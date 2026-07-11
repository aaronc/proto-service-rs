use alloc::boxed::Box;
use alloc::sync::Arc;
use bytes::Bytes;
use core::pin::Pin;
use core::task::{Context, Poll, ready};
use futures_core::Stream;
use futures_sink::Sink;
use futures_util::{SinkExt, StreamExt, stream};

use crate::{CallEnd, Code, MetadataMap, Response, SendError, Status};

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

/// A type-erased [`ClientConnection`] handle held by generated clients.
///
/// Cloning is cheap: clones share the underlying connection.
#[derive(Clone)]
pub struct Connection(Arc<dyn ClientConnection>);

impl Connection {
    pub fn new(conn: impl ClientConnection + 'static) -> Self {
        Self(Arc::new(conn))
    }

    pub fn unary(&self, method: &str, headers: MetadataMap, request: Bytes) -> CallEndFut {
        self.0.unary(method, headers, request)
    }

    pub fn server_stream(
        &self,
        method: &str,
        headers: MetadataMap,
        request: Bytes,
    ) -> RawResponseStream {
        self.0.server_stream(method, headers, request)
    }

    pub fn client_stream(&self, method: &str, headers: MetadataMap) -> (RawRequestSink, CallEndFut) {
        self.0.client_stream(method, headers)
    }

    pub fn bidi(&self, method: &str, headers: MetadataMap) -> (RawRequestSink, RawResponseStream) {
        self.0.bidi(method, headers)
    }
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

/// The send half of a streaming-request call, as generated code hands it to callers.
///
/// Implements [`Sink<T>`], so `SinkExt::send`, `send_all` and `forward` all work.
/// Closing it half-closes the call; dropping it without closing cancels the call.
pub struct RequestSink<T> {
    tx: RawRequestSink,
    encode: Box<dyn FnMut(T) -> Bytes + Send>,
    done: bool,
}

impl<T: 'static> RequestSink<T> {
    pub fn new(tx: RawRequestSink, encode: impl FnMut(T) -> Bytes + Send + 'static) -> Self {
        Self {
            tx,
            encode: Box::new(encode),
            done: false,
        }
    }

    /// Half-closes the call, consuming the sink so nothing can be sent afterwards.
    ///
    /// Equivalent to `SinkExt::close`, which is also what makes the subsequent drop a
    /// clean finish rather than a cancellation. An error here only means the call had
    /// already ended; its status arrives on the response half.
    pub async fn done(mut self) -> Result<(), SendError> {
        SinkExt::close(&mut self).await
    }
}

impl<T: 'static> Sink<T> for RequestSink<T> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        Pin::new(&mut self.get_mut().tx).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, message: T) -> Result<(), SendError> {
        let this = self.get_mut();
        let body = (this.encode)(message);
        Pin::new(&mut this.tx).start_send(RawRequestFrame::Message(body))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        Pin::new(&mut self.get_mut().tx).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        let this = self.get_mut();
        if !this.done {
            ready!(Pin::new(&mut this.tx).poll_ready(cx))?;
            Pin::new(&mut this.tx).start_send(RawRequestFrame::Done)?;
            this.done = true;
        }
        Pin::new(&mut this.tx).poll_close(cx)
    }
}

/// The send half of a client-streaming call, paired with its single response.
///
/// Implements [`Sink<T>`] for sending request messages; [`finish`](Self::finish)
/// half-closes the send direction and awaits the one response. Dropping this before
/// `finish` cancels the call.
pub struct ClientStream<T, U> {
    sink: RequestSink<T>,
    response: CallEndFut,
    decode: Box<dyn FnMut(Bytes) -> Result<U, Status> + Send>,
}

impl<T: 'static, U: 'static> ClientStream<T, U> {
    pub fn new(
        sink: RequestSink<T>,
        response: CallEndFut,
        decode: impl FnMut(Bytes) -> Result<U, Status> + Send + 'static,
    ) -> Self {
        Self {
            sink,
            response,
            decode: Box::new(decode),
        }
    }

    /// Half-closes the send direction and awaits the single response.
    ///
    /// A send error while closing only means the call had already ended; its status
    /// arrives with the response.
    pub async fn finish(self) -> Result<Response<U>, Status> {
        let Self {
            sink,
            response,
            mut decode,
        } = self;
        let _ = sink.done().await;
        let end = response.await;
        if end.status.code() != Code::Ok {
            return Err(end.status);
        }
        let bytes = end
            .single_response
            .ok_or_else(|| Status::internal("unable to decode response message"))?;
        Ok(Response {
            headers: end.single_headers.unwrap_or_default(),
            message: decode(bytes)?,
            trailers: end.trailers,
        })
    }
}

impl<T: 'static, U> Sink<T> for ClientStream<T, U> {
    type Error = SendError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        Pin::new(&mut self.get_mut().sink).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, message: T) -> Result<(), SendError> {
        Pin::new(&mut self.get_mut().sink).start_send(message)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        Pin::new(&mut self.get_mut().sink).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        Pin::new(&mut self.get_mut().sink).poll_close(cx)
    }
}

/// The messages of a streaming response, as generated code hands them to callers.
///
/// Implements [`Stream<Item = Result<T, Status>>`]. A non-OK terminal frame surfaces as
/// the final `Err`; an OK one ends the stream. Leading metadata is read before this is
/// handed over, so [`headers`](Self::headers) is populated up front; [`trailers`] fill in
/// once the terminal frame arrives.
///
/// Dropping this before it terminates cancels the call.
///
/// [`trailers`]: Self::trailers
pub struct ResponseStream<T> {
    headers: MetadataMap,
    trailers: Option<MetadataMap>,
    frames: Pin<Box<dyn Stream<Item = RawResponseFrame> + Send>>,
    decode: Box<dyn FnMut(Bytes) -> Result<T, Status> + Send>,
    done: bool,
}

impl<T: 'static> ResponseStream<T> {
    /// Reads the response head, then yields the decoded messages.
    ///
    /// Awaits whichever of `Headers`, the first message, or the terminal frame arrives
    /// first. Returns `Err` if the call ended non-OK before any message -- the common
    /// early rejection, so `?` catches `UNIMPLEMENTED` and friends before the caller
    /// loops. Any frame consumed to find the head is put back at the front of the stream.
    ///
    /// An absent `Headers` frame yields empty metadata; on the wire the two are the same.
    pub async fn read(
        mut rx: RawResponseStream,
        decode: impl FnMut(Bytes) -> Result<T, Status> + Send + 'static,
    ) -> Result<Self, Status> {
        let (headers, prefix) = match rx.next().await {
            Some(RawResponseFrame::Headers(headers)) => (headers, None),
            Some(message @ RawResponseFrame::Message(_)) => (MetadataMap::new(), Some(message)),
            Some(RawResponseFrame::CallEnd(end)) if end.status.code() != Code::Ok => {
                return Err(end.status);
            }
            Some(end @ RawResponseFrame::CallEnd(_)) => (MetadataMap::new(), Some(end)),
            None => return Err(Status::internal("response ended without a status")),
        };
        Ok(Self {
            headers,
            trailers: None,
            frames: Box::pin(stream::iter(prefix).chain(rx)),
            decode: Box::new(decode),
            done: false,
        })
    }

    /// Leading metadata, read before the first message.
    pub fn headers(&self) -> &MetadataMap {
        &self.headers
    }

    /// Trailing metadata, once the call has ended.
    pub fn trailers(&self) -> Option<&MetadataMap> {
        self.trailers.as_ref()
    }
}

impl<T> Stream for ResponseStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<T, Status>>> {
        let this = self.get_mut();
        loop {
            if this.done {
                return Poll::Ready(None);
            }
            match ready!(this.frames.as_mut().poll_next(cx)) {
                // At most one Headers frame, before any message; already captured in `read`.
                Some(RawResponseFrame::Headers(_)) => {}
                Some(RawResponseFrame::Message(body)) => {
                    let message = (this.decode)(body);
                    if message.is_err() {
                        this.done = true;
                    }
                    return Poll::Ready(Some(message));
                }
                Some(RawResponseFrame::CallEnd(end)) => {
                    this.done = true;
                    this.trailers = Some(end.trailers);
                    return Poll::Ready(match end.status.code() {
                        Code::Ok => None,
                        _ => Some(Err(end.status)),
                    });
                }
                // The transport always delivers a terminal frame first, so a bare end is
                // just end-of-stream.
                None => {
                    this.done = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}
