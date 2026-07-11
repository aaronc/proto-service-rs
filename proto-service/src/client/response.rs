use alloc::boxed::Box;
use core::pin::Pin;
use core::task::{Context, Poll, ready};

use bytes::Bytes;
use futures_core::Stream;
use futures_util::{StreamExt, stream};

use crate::client::{RawResponseFrame, RawResponseStream};
use crate::{Code, MetadataMap, Status};

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
