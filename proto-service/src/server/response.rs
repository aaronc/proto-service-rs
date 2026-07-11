use alloc::boxed::Box;
use core::pin::Pin;

use bytes::Bytes;
use futures_sink::Sink;
use futures_util::SinkExt;

use crate::{Code, MetadataMap, SendError, Status, server::RawResponseFrame};

/// A frame on a [`StreamingResponse`]: leading metadata, then messages.
pub enum ResponseFrame<T> {
    /// Leading metadata; at most once, before any message.
    Headers(MetadataMap),
    /// A response message.
    Message(T),
}

/// Error returned by [`StreamingResponse::send_headers`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SendHeadersError {
    /// The receiving end has been dropped or closed ([`SendError`]).
    #[error("sending on a closed channel")]
    Closed,
    /// A frame was already sent; headers must be the first frame.
    #[error("headers must be the first frame")]
    AlreadyOpened,
}

impl From<SendHeadersError> for Status {
    fn from(err: SendHeadersError) -> Self {
        match err {
            SendHeadersError::Closed => Code::Cancelled.into(),
            SendHeadersError::AlreadyOpened => {
                Status::internal("response headers must be sent before any message")
            }
        }
    }
}

/// The send side of a server-streaming / bidi response.
pub struct StreamingResponse<T> {
    /// Sink for response frames: headers (at most once, first) then messages.
    tx: Pin<Box<dyn Sink<ResponseFrame<T>, Error = SendError> + Send>>,
    /// Whether any frame has been sent; headers are only legal before this.
    opened: bool,
}

impl<T> StreamingResponse<T> {
    /// Builds a typed streaming response over a raw frame sink, encoding each message
    /// with `encode`; headers pass through unchanged.
    pub fn new<F>(
        sink: Pin<Box<dyn Sink<RawResponseFrame, Error = SendError> + Send>>,
        mut encode: F,
    ) -> Self
    where
        T: Send + 'static,
        F: FnMut(T) -> Bytes + Send + 'static,
    {
        Self {
            tx: Box::pin(sink.with(move |frame: ResponseFrame<T>| {
                core::future::ready(Ok::<_, SendError>(match frame {
                    ResponseFrame::Headers(headers) => RawResponseFrame::Headers(headers),
                    ResponseFrame::Message(message) => RawResponseFrame::Message(encode(message)),
                }))
            })),
            opened: false,
        }
    }

    /// Sends leading metadata; must precede any message.
    pub async fn send_headers(&mut self, headers: MetadataMap) -> Result<(), SendHeadersError> {
        if self.opened {
            return Err(SendHeadersError::AlreadyOpened);
        }
        self.tx
            .send(ResponseFrame::Headers(headers))
            .await
            .map_err(|_| SendHeadersError::Closed)?;
        // Mark opened after sending
        self.opened = true;
        Ok(())
    }

    /// Sends one response message.
    pub async fn send_message(&mut self, message: T) -> Result<(), SendError> {
        self.tx.send(ResponseFrame::Message(message)).await?;
        self.opened = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    extern crate std;

    use super::*;
    use futures::StreamExt;
    use futures::channel::mpsc;
    use futures::executor::block_on;
    use std::vec::Vec;

    /// A transport adapter maps each native sink error into our concrete SendError,
    /// then erases the sink into the owned, boxed form `new` expects.
    fn streaming_response(
        capacity: usize,
    ) -> (StreamingResponse<i32>, mpsc::Receiver<RawResponseFrame>) {
        let (tx, rx) = mpsc::channel::<RawResponseFrame>(capacity);
        let resp = StreamingResponse::new(Box::pin(tx.sink_map_err(|_| SendError)), |m: i32| {
            Bytes::copy_from_slice(&m.to_be_bytes())
        });
        (resp, rx)
    }

    #[test]
    fn helpers_send_frames_to_owned_boxed_sink() {
        let (mut resp, rx) = streaming_response(4);
        block_on(async {
            resp.send_headers(MetadataMap::new()).await.unwrap();
            resp.send_message(1).await.unwrap();
            resp.send_message(2).await.unwrap();
        });
        drop(resp);

        let frames: Vec<RawResponseFrame> = block_on(rx.collect());
        assert_eq!(frames.len(), 3);
        assert!(matches!(frames[0], RawResponseFrame::Headers(_)));
        let expect = |frame: &RawResponseFrame, n: i32| matches!(frame, RawResponseFrame::Message(b) if b.as_ref() == n.to_be_bytes());
        assert!(expect(&frames[1], 1));
        assert!(expect(&frames[2], 2));
    }

    #[test]
    fn headers_after_message_or_headers_rejected() {
        let (mut resp, _rx) = streaming_response(4);
        block_on(async {
            resp.send_message(1).await.unwrap();
            assert_eq!(
                resp.send_headers(MetadataMap::new()).await,
                Err(SendHeadersError::AlreadyOpened)
            );
        });

        let (mut resp, _rx) = streaming_response(4);
        block_on(async {
            resp.send_headers(MetadataMap::new()).await.unwrap();
            assert_eq!(
                resp.send_headers(MetadataMap::new()).await,
                Err(SendHeadersError::AlreadyOpened)
            );
        });
    }

    #[test]
    fn send_error_converts_to_cancelled_status() {
        let status: Status = SendError.into();
        assert_eq!(status.code(), Code::Cancelled);
    }
}
