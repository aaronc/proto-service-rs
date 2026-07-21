//! Client half of the pipe transport: multiplexes calls over one
//! `Stream<ResponsePacket>` / `Sink<RequestPacket>` pair, correlating them by `req_id`.

use std::{
    pin::pin,
    sync::{
        Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use alloc::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use futures_channel::{mpsc, oneshot};
use futures_util::{Sink, SinkExt, Stream, StreamExt, future};
use proto_service::{
    CallEnd, MetadataMap, SendError, Status,
    client::{
        CallEndFut, Cancel, ClientConnection, RawRequestFrame, RawRequestSink, RawResponseFrame,
        RawResponseStream,
    },
};

use crate::{
    packet::{RequestPacket, ResponsePacket, request_packet, response_packet},
    util::{from_packet_metadata, from_packet_status, to_packet_metadata},
};

/// Opens calls on a [`PipeConnection`].
///
/// Cheap to clone; every clone shares one connection, and calls are multiplexed on it
/// by `req_id`. Hence `&self`: any number of concurrent calls may be in flight, and a
/// call outlives the client that opened it.
#[derive(Clone)]
pub struct PipeClient {
    inner: Arc<Inner>,
}

/// Owns one bidirectional pipe and drives it.
///
/// Nothing is read or written until [`run`](Self::run) is polled, so mint clients with
/// [`new_client`](Self::new_client) first and then hand the connection to an executor
/// -- or `select!` it into an existing loop. Either way the client side needs no
/// spawner of its own.
pub struct PipeConnection<In, Out> {
    inner: Arc<Inner>,
    inbound: In,
    outbound: Out,
    outbound_rx: mpsc::UnboundedReceiver<RequestPacket>,
}

struct Inner {
    next_req_id: AtomicU64,
    sessions: Mutex<BTreeMap<u64, Session>>,
    outbound: mpsc::UnboundedSender<RequestPacket>,
}

/// The delivery end of a live call, held by the run loop.
///
/// `Streaming` is unbounded because the run loop routes frames for every call on the
/// connection: blocking on one call's channel would stall all the others.
enum Session {
    Single(oneshot::Sender<CallEnd>),
    Streaming(mpsc::UnboundedSender<RawResponseFrame>),
}

impl<In, Out> PipeConnection<In, Out> {
    /// Wraps one pipe. Performs no I/O; see [`run`](Self::run).
    pub fn new(inbound: In, outbound: Out) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::unbounded();
        Self {
            inner: Arc::new(Inner {
                next_req_id: AtomicU64::new(1),
                sessions: Mutex::new(BTreeMap::new()),
                outbound: outbound_tx,
            }),
            inbound,
            outbound,
            outbound_rx,
        }
    }

    /// A client for opening calls on this connection.
    pub fn new_client(&self) -> PipeClient {
        PipeClient {
            inner: self.inner.clone(),
        }
    }
}

impl PipeClient {
    fn cancel_handle_for(&self, req_id: u64) -> Arc<dyn Cancel> {
        Arc::new(PipeCancelHandle {
            req_id,
            inner: self.inner.clone(),
        })
    }

    fn new_frame_sink(
        &self,
        req_id: u64,
    ) -> impl Sink<RawRequestFrame, Error = SendError> + Send + 'static {
        self.inner
            .outbound
            .clone()
            .sink_map_err(|_| SendError)
            .with(move |frame| {
                future::ready(Ok::<_, SendError>(frame_request_packet(req_id, frame)))
            })
    }

    fn new_call_end_fut(&self, rx: oneshot::Receiver<CallEnd>, req_id: u64) -> CallEndFut {
        CallEndFut::new(
            async move {
                rx.await
                    .unwrap_or_else(|_| CallEnd::error(Status::unavailable("connection closed")))
            },
            self.cancel_handle_for(req_id),
        )
    }
}

impl ClientConnection for PipeClient {
    fn unary(&self, method: &str, headers: MetadataMap, request: Bytes) -> CallEndFut {
        let (call_end_tx, call_end_rx) = oneshot::channel();
        let req_id = self
            .inner
            .begin(method, headers, Some(request), Session::Single(call_end_tx));
        self.new_call_end_fut(call_end_rx, req_id)
    }

    fn server_stream(
        &self,
        method: &str,
        headers: MetadataMap,
        request: Bytes,
    ) -> RawResponseStream {
        let (response_tx, response_rx) = mpsc::unbounded();
        let req_id = self.inner.begin(
            method,
            headers,
            Some(request),
            Session::Streaming(response_tx),
        );
        RawResponseStream::new(response_rx, self.cancel_handle_for(req_id))
    }

    fn client_stream(&self, method: &str, headers: MetadataMap) -> (RawRequestSink, CallEndFut) {
        let (call_end_tx, call_end_rx) = oneshot::channel();
        let req_id = self
            .inner
            .begin(method, headers, None, Session::Single(call_end_tx));
        (
            RawRequestSink::new(self.new_frame_sink(req_id), self.cancel_handle_for(req_id)),
            self.new_call_end_fut(call_end_rx, req_id),
        )
    }

    fn bidi(&self, method: &str, headers: MetadataMap) -> (RawRequestSink, RawResponseStream) {
        let (response_tx, response_rx) = mpsc::unbounded();
        let req_id = self
            .inner
            .begin(method, headers, None, Session::Streaming(response_tx));
        (
            RawRequestSink::new(self.new_frame_sink(req_id), self.cancel_handle_for(req_id)),
            RawResponseStream::new(response_rx, self.cancel_handle_for(req_id)),
        )
    }
}

impl Session {
    fn deliver_call_end(self, end: CallEnd) {
        match self {
            Session::Single(tx) => {
                let _ = tx.send(end);
            }
            Session::Streaming(tx) => {
                let _ = tx.unbounded_send(RawResponseFrame::CallEnd(end));
            }
        }
    }

    fn deliver_frame(&self, frame: RawResponseFrame) {
        if let Session::Streaming(tx) = self {
            let _ = tx.unbounded_send(frame);
        }
    }
}

impl Inner {
    fn begin(
        &self,
        method: &str,
        headers: MetadataMap,
        single_request: Option<Bytes>,
        session: Session,
    ) -> u64 {
        let req_id = self.next_req_id.fetch_add(1, Ordering::Relaxed);
        // Insert the session before we send the begin packet so we can listen for responses right away
        self.lock_sessions().insert(req_id, session);
        let sent = self.outbound.unbounded_send(RequestPacket {
            req_id,
            frame: Some(request_packet::Frame::Begin(request_packet::Begin {
                method: method.into(),
                headers: Some(to_packet_metadata(&headers)),
                single_request: single_request.map(|r| r.into()),
            })),
        });
        // Remove the session and deliver an error if the send failed
        if sent.is_err()
            && let Some(session) = self.remove_session(req_id)
        {
            session.deliver_call_end(CallEnd::error(Status::unavailable("connection closed")));
        }
        req_id
    }

    fn lock_sessions(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, Session>> {
        self.sessions.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn remove_session(&self, req_id: u64) -> Option<Session> {
        self.lock_sessions().remove(&req_id)
    }

    fn deliver_frame(&self, req_id: u64, frame: RawResponseFrame) {
        if let Some(session) = self.lock_sessions().get(&req_id) {
            session.deliver_frame(frame);
        }
    }
}

struct PipeCancelHandle {
    req_id: u64,
    inner: Arc<Inner>,
}

impl Cancel for PipeCancelHandle {
    /// Removing the session is what makes this idempotent, as `Cancel` requires: the run
    /// loop removes it when `Close` arrives, so a later drop finds nothing and does
    /// nothing. Both halves of a bidi call hold one of these and cancel independently.
    ///
    /// Delivering `CANCELLED` gives whichever half the caller still holds a terminal
    /// frame, rather than a stream that simply goes silent.
    fn cancel(&self) {
        let Some(session) = self.inner.remove_session(self.req_id) else {
            return;
        };
        let _ = self.inner.outbound.unbounded_send(RequestPacket {
            req_id: self.req_id,
            frame: Some(request_packet::Frame::Cancel(request_packet::Cancel {})),
        });
        session.deliver_call_end(CallEnd::error(Status::cancelled("cancelled by client")));
    }
}

impl<In, Out> PipeConnection<In, Out>
where
    In: Stream<Item = ResponsePacket> + Unpin,
    Out: Sink<RequestPacket> + Unpin,
{
    /// Drives the connection: routes inbound packets to their calls, writes queued
    /// outbound packets to the pipe.
    ///
    /// The two directions are driven concurrently rather than in turn: a pipe with
    /// finite buffers deadlocks if we stop reading while blocked on a write.
    ///
    /// Returns once the inbound half ends, the outbound half errors, or the last client
    /// and in-flight call are dropped -- the run loop holds only a [`Weak`] reference,
    /// so the outbound queue's senders outlive it exactly as long as somebody can still
    /// open or cancel a call. Every call still live is then failed with `UNAVAILABLE`.
    /// That is the only place a call is resolved by connection teardown, which is why no
    /// call handle ever has to cope with its delivery channel vanishing.
    pub async fn run(self) {
        let Self {
            inner,
            mut inbound,
            outbound,
            outbound_rx,
        } = self;
        // Must drop the strong ref, not just shadow it: the outbound queue's senders live
        // in `Inner`, so holding one here would keep `outbound_rx` open forever.
        let weak = Arc::downgrade(&inner);
        drop(inner);
        let inner = weak;

        let read = async {
            while let Some(packet) = inbound.next().await {
                let Some(inner) = inner.upgrade() else { break };
                route_response(&inner, packet);
            }
        };
        let write = outbound_rx.map(Ok).forward(outbound);

        let _ = future::select(pin!(read), pin!(write)).await;

        if let Some(inner) = inner.upgrade() {
            let sessions = core::mem::take(&mut *inner.lock_sessions());
            for (_, session) in sessions {
                session.deliver_call_end(CallEnd::error(Status::unavailable("connection closed")));
            }
        }
    }
}

fn route_response(inner: &Inner, packet: ResponsePacket) {
    let Some(frame) = packet.frame else {
        return;
    };
    match frame {
        response_packet::Frame::Close(close) => {
            if let Some(session) = inner.remove_session(packet.req_id) {
                session.deliver_call_end(close_to_call_end(close));
            }
        }
        response_packet::Frame::Headers(headers) => inner.deliver_frame(
            packet.req_id,
            RawResponseFrame::Headers(from_packet_metadata(headers.metadata.unwrap_or_default())),
        ),
        response_packet::Frame::Message(message) => inner.deliver_frame(
            packet.req_id,
            RawResponseFrame::Message(message.body.into()),
        ),
    }
}

fn close_to_call_end(close: response_packet::Close) -> CallEnd {
    CallEnd {
        status: from_packet_status(close.status.unwrap_or_default()),
        trailers: from_packet_metadata(close.trailers.unwrap_or_default()),
        single_response: close.single_response.map(Bytes::from),
        single_headers: close.single_headers.map(from_packet_metadata),
    }
}

fn frame_request_packet(req_id: u64, frame: RawRequestFrame) -> RequestPacket {
    let frame = match frame {
        RawRequestFrame::Message(body) => {
            request_packet::Frame::Message(request_packet::Message { body: body.into() })
        }
        RawRequestFrame::Done => request_packet::Frame::DoneSending(request_packet::DoneSending {}),
    };
    RequestPacket {
        req_id,
        frame: Some(frame),
    }
}
