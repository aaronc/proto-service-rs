use alloc::boxed::Box;
use core::pin::Pin;
use core::task::{Context, Poll, ready};

use bytes::Bytes;
use futures_sink::Sink;
use futures_util::SinkExt;

use crate::client::{CallEndFut, RawRequestFrame, RawRequestSink};
use crate::{Response, SendError, Status};

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
            decode,
        } = self;
        let _ = sink.done().await;
        response.await.into_response(decode)
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
