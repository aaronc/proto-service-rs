use alloc::boxed::Box;
use alloc::sync::Arc;
use bytes::Bytes;
use core::pin::Pin;
use core::task::{Context, Poll, ready};
use futures_core::Stream;
use futures_sink::Sink;

use crate::{CallEnd, MetadataMap, SendError};

/// Aborts a call, telling the peer to stop and failing the caller's local state.
///
/// Invoked from `Drop`, so it must not block, await, or wait on a bounded queue --
/// a transport whose outbound path can fill up will silently fail to signal the
/// peer and leak the call there.
///
/// Must be idempotent. Both halves of a [`ClientConnection::bidi`] call cancel
/// independently, and each is unaware of the other's terminal state, so `cancel`
/// can be called twice for one call. Cancelling a call that has already ended is a
/// no-op.
///
/// When a call is cancelled while the caller still holds its [`RawResponseStream`]
/// or [`CallEndFut`], the implementation must deliver a terminal [`CallEnd`] to it.
/// Otherwise the response half ends without a status and the caller cannot tell a
/// cancelled call from a broken transport.
pub trait Cancel: Send + Sync {
    fn cancel(&self);
}

/// A client -> server frame after the call has been opened.
pub enum RawRequestFrame {
    /// An encoded request message.
    Message(Bytes),
    /// The send half is closed; the response direction stays open. Not an abort --
    /// dropping a [`RawRequestSink`] without sending this cancels the call.
    Done,
}

/// A server -> client frame.
pub enum RawResponseFrame {
    /// Leading response metadata. At most once, before any message.
    Headers(MetadataMap),
    /// An encoded response message.
    Message(Bytes),
    /// Terminal frame; nothing follows.
    CallEnd(CallEnd),
}

/// Opens calls on one connection. Calls are multiplexed, so this takes `&self` and
/// the handles it returns own their state rather than borrowing the connection.
///
/// Each method sends the call's opening frame -- method, headers, and for
/// single-request shapes the payload -- before returning. It must not wait for the
/// caller to poll the returned handles: the server may legally respond before the
/// client sends its first message.
///
/// None of these return a `Result`. A call that cannot start ends like any other,
/// with a [`CallEnd`] carrying the status.
pub trait ClientConnection: Send + Sync {
    fn unary(&self, method: &str, headers: MetadataMap, request: Bytes) -> CallEndFut;

    fn server_stream(
        &self,
        method: &str,
        headers: MetadataMap,
        request: Bytes,
    ) -> RawResponseStream;

    fn client_stream(&self, method: &str, headers: MetadataMap) -> (RawRequestSink, CallEndFut);

    fn bidi(&self, method: &str, headers: MetadataMap) -> (RawRequestSink, RawResponseStream);
}

/// The send half of a streaming-request call.
///
/// Sending [`RawRequestFrame::Done`] half-closes the send direction. Dropping this
/// without having sent it cancels the call -- there is no other way to distinguish
/// a client that finished from one that died.
///
/// `poll_close` only closes the sink; it does not half-close the call. Send `Done`.
pub struct RawRequestSink {
    sink: Pin<Box<dyn Sink<RawRequestFrame, Error = SendError> + Send>>,
    cancel: Arc<dyn Cancel>,
    done: bool,
}

impl RawRequestSink {
    pub fn new(
        sink: impl Sink<RawRequestFrame, Error = SendError> + Send + 'static,
        cancel: Arc<dyn Cancel>,
    ) -> Self {
        Self {
            sink: Box::pin(sink),
            cancel,
            done: false,
        }
    }
}

impl Sink<RawRequestFrame> for RawRequestSink {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        self.get_mut().sink.as_mut().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, frame: RawRequestFrame) -> Result<(), SendError> {
        let this = self.get_mut();
        if matches!(frame, RawRequestFrame::Done) {
            this.done = true;
        }
        this.sink.as_mut().start_send(frame)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        self.get_mut().sink.as_mut().poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        self.get_mut().sink.as_mut().poll_close(cx)
    }
}

impl Drop for RawRequestSink {
    fn drop(&mut self) {
        if !self.done {
            self.cancel.cancel();
        }
    }
}

/// The receive half of a streaming-response call.
///
/// Dropping this before [`RawResponseFrame::CallEnd`] has been yielded cancels the
/// call.
pub struct RawResponseStream {
    stream: Pin<Box<dyn Stream<Item = RawResponseFrame> + Send>>,
    cancel: Arc<dyn Cancel>,
    terminated: bool,
}

impl RawResponseStream {
    pub fn new(
        stream: impl Stream<Item = RawResponseFrame> + Send + 'static,
        cancel: Arc<dyn Cancel>,
    ) -> Self {
        Self {
            stream: Box::pin(stream),
            cancel,
            terminated: false,
        }
    }
}

impl Stream for RawResponseStream {
    type Item = RawResponseFrame;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<RawResponseFrame>> {
        let this = self.get_mut();
        let frame = ready!(this.stream.as_mut().poll_next(cx));
        if matches!(frame, Some(RawResponseFrame::CallEnd(_))) {
            this.terminated = true;
        }
        Poll::Ready(frame)
    }
}

impl Drop for RawResponseStream {
    fn drop(&mut self) {
        if !self.terminated {
            self.cancel.cancel();
        }
    }
}

/// The response of a single-response call.
///
/// Dropping this before it resolves cancels the call, so racing it against a
/// timeout cancels on the wire for free.
pub struct CallEndFut {
    fut: Pin<Box<dyn Future<Output = CallEnd> + Send>>,
    cancel: Arc<dyn Cancel>,
    resolved: bool,
}

impl CallEndFut {
    pub fn new(
        fut: impl Future<Output = CallEnd> + Send + 'static,
        cancel: Arc<dyn Cancel>,
    ) -> Self {
        Self {
            fut: Box::pin(fut),
            cancel,
            resolved: false,
        }
    }
}

impl Future for CallEndFut {
    type Output = CallEnd;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<CallEnd> {
        let this = self.get_mut();
        let end = ready!(this.fut.as_mut().poll(cx));
        this.resolved = true;
        Poll::Ready(end)
    }
}

impl Drop for CallEndFut {
    fn drop(&mut self) {
        if !self.resolved {
            self.cancel.cancel();
        }
    }
}

//// WIP design for types that codegen will actually use
// pub struct RequestSink<T> {
//     tx: RawRequestSink,
// }

// impl<T> RequestSink<T> {
//     pub async fn send(&self, message: T) -> Result<(), SendError> {
//         todo!()
//     }

//     pub async fn done(self) {
//         todo!()
//     }
// }

// pub struct ResponseStream<T> {
//     rx: RawRequestStream,
// }

// impl<T> ResponseStream<T> {
//     pub async fn start(&self) -> Result<(MetadataMap, MessageResponseStream<T>), Status> {
//         todo!()
//     }
// }

// pub struct MessageResponseStream<T> {}

// impl<T> ResponseStream<T> {
//     pub async fn next() -> Result<T, Status> {
//         todo!()
//     }

//     fn trailers(&self) -> Option<&MetadataMap> {
//         todo!()
//     }
// }

// pub enum ResponseFrame<T> {
//     Message(T),
//     Done(Result<MetadataMap, Status>),
// }

// pub enum ResponseFrame<T> {
//     Headers(MetadataMap),
//     Message(Bytes),
//     Done(Result<MetadataMap, Status>),
// }
