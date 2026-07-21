use core::future;
use core::ops::ControlFlow;
use core::pin::Pin;
use std::panic::AssertUnwindSafe;

use alloc::sync::Arc;
use alloc::{boxed::Box, collections::btree_map::BTreeMap, vec::Vec};
use bytes::Bytes;
use futures_channel::mpsc::{self, Receiver, Sender, UnboundedSender};
use futures_util::future::{AbortHandle, Abortable};
use futures_util::{FutureExt, SinkExt, StreamExt, stream};
use proto_service::server::{Call, RawResponseFrame, RequestPayload, ServiceRouter};
use proto_service::{CallEnd, SendError, Sink, Status, Stream};

use crate::packet::{self, RequestPacket};
use crate::packet::{ResponsePacket, request_packet::Frame};
use crate::spawner::Spawner;
use crate::util::{from_packet_metadata, to_packet_metadata};

/// Dispatches RPC calls over bidirectional pipes. Built once, then serves many
/// connections concurrently against a shared [`ServiceRouter`].
pub struct PipeServer<S> {
    router: Arc<ServiceRouter>,
    spawner: S,
}

impl<S: Spawner> PipeServer<S> {
    /// The `router` is shared (`Arc`) so the same registrations can back other
    /// transports; handlers run on `spawner`.
    pub fn new(router: Arc<ServiceRouter>, spawner: S) -> Self {
        Self { router, spawner }
    }

    /// Serves one connection to completion: returns when `inbound` ends (the pipe
    /// closed) or the client commits a protocol violation, aborting in-flight handlers.
    ///
    /// Client-streaming request messages are buffered without bound (so the run loop
    /// never blocks and stays responsive to Cancel). A pipe serves a single client, so
    /// there is no cross-tenant starvation; but guarding against an *untrusted* client
    /// that floods `inbound` to exhaust memory is the caller's responsibility -- it owns
    /// the wire and can meter, rate-limit, or drop the connection. This transport does not.
    pub async fn serve<In, Out>(&self, inbound: In, outbound: Out)
    where
        In: Stream<Item = RequestPacket> + Unpin,
        Out: Sink<ResponsePacket> + Send + 'static,
    {
        // The funnel is bounded so handlers get response backpressure from a slow
        // `outbound` (blocking a handler is fine; only the run loop must never block).
        const FUNNEL_BUFFER: usize = 32;
        let (funnel_tx, funnel_rx) = mpsc::channel::<ResponsePacket>(FUNNEL_BUFFER);
        let (done_tx, done_rx) = mpsc::channel::<u64>(FUNNEL_BUFFER);
        // Signals the run loop that the writer (hence `outbound`) is gone, so the loop
        // stops reading inbound rather than spawning handlers whose responses vanish.
        let (mut writer_gone_tx, writer_gone_rx) = mpsc::channel::<()>(1);

        // The writer task is the sole owner of `outbound`; every call's frames reach the
        // pipe only through the funnel. It ends when all funnel senders drop (the
        // connection tore down) or the sink errors.
        let outbound = Box::pin(outbound);
        self.spawner.spawn(Box::pin(async move {
            let _ = funnel_rx
                .map(Ok::<_, <Out as Sink<ResponsePacket>>::Error>)
                .forward(outbound)
                .await;
            let _ = writer_gone_tx.try_send(());
        }));

        let mut conn = Connection {
            router: &self.router,
            spawner: &self.spawner,
            sessions: BTreeMap::new(),
            highest_id: 0,
            funnel_tx,
            done_tx,
        };
        conn.run(inbound, done_rx, writer_gone_rx).await;
    }
}

/// Per-connection state. Created by [`PipeServer::serve`]; never constructed directly.
struct Connection<'a, S> {
    router: &'a ServiceRouter,
    spawner: &'a S,
    sessions: BTreeMap<u64, Session>,
    /// Highest req_id seen in a Begin. Ids must be non-zero, strictly increasing and
    /// never reused; `<=` rejects zero, reuse of a live id, and reuse of a closed id.
    highest_id: u64,
    funnel_tx: Sender<ResponsePacket>,
    /// A handler sends its req_id here when it finishes, so `run` drops the session.
    done_tx: Sender<u64>,
}

struct Session {
    client_tx: Option<UnboundedSender<Bytes>>,
    abort: AbortHandle,
}

/// What the run loop wakes on: an inbound packet, the inbound stream ending, a finished
/// handler, or the writer (hence `outbound`) going away.
enum Event {
    Packet(RequestPacket),
    InboundClosed,
    Done(u64),
    WriterGone,
}

impl<S: Spawner> Connection<'_, S> {
    /// Drives one connection: interleaves inbound packets with handler-completion
    /// notifications, dropping each session as its handler finishes. Returns when the
    /// pipe closes, the writer (hence `outbound`) dies, or the client commits a protocol
    /// violation. In-flight handlers are aborted on drop (see the `Drop` impl).
    async fn run(
        &mut self,
        inbound: impl Stream<Item = RequestPacket> + Unpin,
        done_rx: Receiver<u64>,
        writer_gone_rx: Receiver<()>,
    ) {
        // `stream::select` only ends once every branch ends, but done_rx/writer_gone_rx
        // never do (their senders live for the whole connection). So append a sentinel to
        // inbound and break on it, rather than relying on select to terminate.
        let inbound = inbound
            .map(Event::Packet)
            .chain(stream::once(future::ready(Event::InboundClosed)));
        let mut events = stream::select(
            stream::select(inbound, done_rx.map(Event::Done)),
            writer_gone_rx.map(|()| Event::WriterGone),
        );
        while let Some(event) = events.next().await {
            match event {
                Event::Packet(packet) => {
                    if self.handle_packet(packet).is_break() {
                        break;
                    }
                }
                // Handler finished; drop its session. Idempotent -- Cancel may have
                // already removed it (the Close-vs-Cancel race).
                Event::Done(req_id) => {
                    self.sessions.remove(&req_id);
                }
                // Pipe closed, or outbound is gone: the connection is over. `Drop` aborts
                // any handlers still in flight.
                Event::InboundClosed | Event::WriterGone => break,
            }
        }
    }

    /// Handles a single packet; `Break` shuts the whole connection down (a protocol
    /// violation). Never blocks -- the run loop must stay responsive to Cancel.
    fn handle_packet(&mut self, req_packet: RequestPacket) -> ControlFlow<()> {
        let req_id = req_packet.req_id;
        let Some(frame) = req_packet.frame else {
            // Absent frame, e.g. a variant from a newer client that prost decoded as
            // None. Forward-compatible: ignore it.
            return ControlFlow::Continue(());
        };
        match frame {
            Frame::Begin(begin) => {
                // Non-monotonic id, protocol violation so kill the
                // connection rather than risk reusing a call ID.
                // <= also rejects zero IDs naturally.
                if req_id <= self.highest_id {
                    return ControlFlow::Break(());
                }
                self.highest_id = req_id;

                let Some((service_name, method_name)) = parse_method(&begin.method) else {
                    self.send_close(req_id, Status::invalid_argument("malformed method name"));
                    return ControlFlow::Continue(());
                };
                let Some(service) = self.router.resolve(service_name) else {
                    self.send_close(req_id, Status::unimplemented("unknown service"));
                    return ControlFlow::Continue(());
                };
                let Some(method_desc) = service.describe_method(method_name) else {
                    self.send_close(req_id, Status::unimplemented("unknown method"));
                    return ControlFlow::Continue(());
                };
                let (req_payload, client_tx) = if method_desc.client_streaming {
                    // Unbounded on purpose. The run loop feeds this with `unbounded_send`,
                    // which never blocks, so the loop stays responsive to Cancel however
                    // fast the client streams -- a bounded channel would force us to either
                    // block the loop (starving Cancel) or drop/kill the call (spuriously
                    // failing a merely-fast client). A pipe carries exactly one client, so
                    // this can't starve other tenants; bounding an *untrusted* client's
                    // memory is the caller's job -- it owns the wire and can meter,
                    // rate-limit, or tear down `inbound`. See `serve`.
                    let (tx, rx) = mpsc::unbounded::<Bytes>();
                    (RequestPayload::Streaming(Box::pin(rx)), Some(tx))
                } else {
                    let Some(req) = begin.single_request else {
                        self.send_close(
                            req_id,
                            Status::invalid_argument("missing single_request payload"),
                        );
                        return ControlFlow::Continue(());
                    };
                    (RequestPayload::Single(req.into()), None)
                };

                // Sink adapter to send a response frame as a response packet
                let streaming_response = if method_desc.server_streaming {
                    let sink = self.funnel_tx.clone().sink_map_err(|_| SendError).with(
                        move |frame: RawResponseFrame| {
                            future::ready(Ok::<_, SendError>(response_packet(req_id, frame)))
                        },
                    );
                    Some(Box::pin(sink)
                        as Pin<
                            Box<dyn Sink<RawResponseFrame, Error = SendError> + Send>,
                        >)
                } else {
                    None
                };

                let call = Call {
                    method_name: method_name.into(),
                    extensions: Default::default(),
                    headers: from_packet_metadata(begin.headers.unwrap_or_default()),
                    req_payload,
                    streaming_response,
                };

                let mut close_tx = self.funnel_tx.clone();
                let mut done_tx = self.done_tx.clone();
                let handler_fut = service.handle(call);

                // Wrap the handler so Cancel can abort it
                let (abort, abort_reg) = AbortHandle::new_pair();
                let task = async move {
                    // A panicking handler must still Close, or the client waits forever.
                    // AssertUnwindSafe: the future is consumed by the panic, so nothing
                    // observes its broken state afterwards. (Moot under panic=abort.)
                    let call_end = match AssertUnwindSafe(handler_fut).catch_unwind().await {
                        Ok(call_end) => call_end,
                        Err(_) => CallEnd::error(Status::internal("handler panicked")),
                    };
                    let _ = close_tx.send(close_packet(req_id, call_end)).await;
                    // Tell the run loop to drop this session. Skipped if aborted (Cancel
                    // already removed it).
                    let _ = done_tx.send(req_id).await;
                };
                self.spawner.spawn(Box::pin(async move {
                    let _ = Abortable::new(task, abort_reg).await;
                }));
                self.sessions.insert(req_id, Session { client_tx, abort });
            }
            Frame::Message(message) => {
                // Unbounded send never blocks, so the run loop stays responsive to Cancel.
                let violation = match self.sessions.get(&req_id) {
                    // Unknown req_id: benign race with a call already closed. Discard.
                    None => false,
                    Some(session) => match &session.client_tx {
                        // Live call that accepts no message now -- a single-request
                        // method, or the client already sent DoneSending. A detectable
                        // client bug, not a benign race.
                        None => true,
                        // Handler can receive a client message
                        Some(client_tx) => {
                            // Err means the handler dropped its receiver; discard.
                            let _ = client_tx.unbounded_send(message.body.into());
                            false
                        }
                    },
                };
                if violation {
                    self.kill(
                        req_id,
                        Status::invalid_argument("unexpected request message"),
                    );
                }
            }
            Frame::DoneSending(_) => {
                // Unknown req_id: benign race; nothing to close.
                let Some(session) = self.sessions.get_mut(&req_id) else {
                    return ControlFlow::Continue(());
                };
                // Dropping the sender ends the handler's request stream (rx yields None).
                session.client_tx = None;
            }
            Frame::Cancel(_) => {
                // Abort the handler and drop its state. Frames already in the funnel may
                // still flush (allowed by the protocol). No Close is sent after Cancel.
                // Cancel for an unknown/closed req_id is a no-op.
                if let Some(session) = self.sessions.remove(&req_id) {
                    session.abort.abort();
                }
            }
        }
        ControlFlow::Continue(())
    }

    /// Sends a trailers-only Close for a call no handler will complete. Spawned so the
    /// run loop never blocks on the bounded funnel (which would starve Cancel).
    fn send_close(&self, req_id: u64, status: Status) {
        let mut funnel = self.funnel_tx.clone();
        self.spawner.spawn(Box::pin(async move {
            let _ = funnel
                .send(close_packet(req_id, CallEnd::error(status)))
                .await;
        }));
    }

    /// Aborts a live session and gives it a terminal `status` (server-initiated kill on a
    /// per-call protocol violation).
    fn kill(&mut self, req_id: u64, status: Status) {
        if let Some(session) = self.sessions.remove(&req_id) {
            session.abort.abort();
        }
        self.send_close(req_id, status);
    }
}

impl<S> Drop for Connection<'_, S> {
    /// Abort every in-flight handler, whether `run` returned normally or the `serve`
    /// future was dropped mid-flight.
    fn drop(&mut self) {
        for (_, session) in core::mem::take(&mut self.sessions) {
            session.abort.abort();
        }
    }
}

// split /foo.Bar/Baz -> (foo.Bar, Baz)
fn parse_method(method: &str) -> Option<(&str, &str)> {
    method.strip_prefix('/')?.split_once('/')
}

/// Stamp a non-terminal response frame with its call id for the funnel.
fn response_packet(req_id: u64, frame: RawResponseFrame) -> ResponsePacket {
    let frame = match frame {
        RawResponseFrame::Headers(md) => {
            packet::response_packet::Frame::Headers(packet::response_packet::Headers {
                metadata: Some(to_packet_metadata(&md)),
            })
        }
        RawResponseFrame::Message(body) => {
            packet::response_packet::Frame::Message(packet::response_packet::Message {
                body: body.into(),
            })
        }
    };
    ResponsePacket {
        req_id,
        frame: Some(frame),
    }
}

/// Build the terminal Close packet from CallEnd.
fn close_packet(req_id: u64, end: CallEnd) -> ResponsePacket {
    let close = packet::response_packet::Close {
        status: Some(to_packet_status(end.status)),
        trailers: Some(to_packet_metadata(&end.trailers)),
        single_response: end.single_response.map(Into::into),
        single_headers: end.single_headers.as_ref().map(to_packet_metadata),
    };
    ResponsePacket {
        req_id,
        frame: Some(packet::response_packet::Frame::Close(close)),
    }
}

fn to_packet_status(status: Status) -> packet::Status {
    packet::Status {
        code: i32::from(status.code()),
        message: status.message().into(),
        details: Vec::new(),
    }
}
