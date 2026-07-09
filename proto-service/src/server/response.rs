use alloc::boxed::Box;
use core::fmt;
use core::pin::Pin;

use bytes::Bytes;
use futures_sink::Sink;
use futures_util::SinkExt;

use crate::server::RawResponseFrame;
use crate::{Code, MetadataMap, Status};

/// A unary response: a single message with leading and trailing metadata.
pub struct Response<T> {
    /// Metadata sent before the message.
    pub headers: MetadataMap,
    /// The response message.
    pub message: T,
    /// Metadata sent after the message.
    pub trailers: MetadataMap,
}

impl<T> Response<T> {
    /// Wraps `message` with empty leading and trailing metadata.
    pub fn new(message: T) -> Self {
        Self {
            headers: MetadataMap::new(),
            message,
            trailers: MetadataMap::new(),
        }
    }
}

/// Error returned by a response [`Sink`] once its receiving end has been dropped
/// or closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sending on a closed channel")
    }
}

impl core::error::Error for SendError {}

impl From<SendError> for Status {
    fn from(_: SendError) -> Self {
        Code::Cancelled.into()
    }
}

/// A frame on a [`StreamingResponse`]: leading metadata, then messages.
pub enum ResponseFrame<T> {
    /// Leading metadata; at most once, before any message.
    Headers(MetadataMap),
    /// A response message.
    Message(T),
}

/// The send side of a server-streaming / bidi response.
pub struct StreamingResponse<T> {
    /// Sink for response frames: headers (at most once, first) then messages.
    pub tx: Pin<Box<dyn Sink<ResponseFrame<T>, Error = SendError> + Send>>,
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
        }
    }

    /// Sends leading metadata; must precede any message.
    pub async fn send_headers(&mut self, headers: MetadataMap) -> Result<(), SendError> {
        self.tx.send(ResponseFrame::Headers(headers)).await
    }

    /// Sends one response message.
    pub async fn send_message(&mut self, message: T) -> Result<(), SendError> {
        self.tx.send(ResponseFrame::Message(message)).await
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

    #[test]
    fn helpers_send_frames_to_owned_boxed_sink() {
        let (tx, rx) = mpsc::channel::<ResponseFrame<i32>>(4);

        // A transport adapter maps each native sink error into our concrete
        // SendError, then erases the sink into the owned, boxed form.
        let mut resp = StreamingResponse {
            tx: Box::pin(tx.sink_map_err(|_| SendError)),
        };
        block_on(async {
            resp.send_headers(MetadataMap::new()).await.unwrap();
            resp.send_message(1).await.unwrap();
            resp.send_message(2).await.unwrap();
        });
        drop(resp);

        let frames: Vec<ResponseFrame<i32>> = block_on(rx.collect());
        assert_eq!(frames.len(), 3);
        assert!(matches!(frames[0], ResponseFrame::Headers(_)));
        assert!(matches!(frames[1], ResponseFrame::Message(1)));
        assert!(matches!(frames[2], ResponseFrame::Message(2)));
    }

    #[test]
    fn send_error_converts_to_cancelled_status() {
        let status: Status = SendError.into();
        assert_eq!(status.code(), Code::Cancelled);
    }
}
