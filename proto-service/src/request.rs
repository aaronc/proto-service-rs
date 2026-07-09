use alloc::boxed::Box;
use core::pin::Pin;

use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;

use crate::{Extensions, MetadataMap, Status};

/// A unary request: a single message with its metadata.
pub struct Request<T> {
    /// Request metadata, received before the message.
    pub headers: MetadataMap,
    /// Values attached to the request by the transport or middleware.
    pub extensions: Extensions,
    /// The request message.
    pub message: T,
}

/// The receive side of a client-streaming / bidi request.
pub struct StreamingRequest<T> {
    /// Request metadata, received before the stream.
    pub headers: MetadataMap,
    /// Values attached to the request by the transport or middleware.
    pub extensions: Extensions,
    /// Stream of request messages: each item is a decoded message, or a
    /// [`Status`] if that message failed to decode.
    pub rx: Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>,
}

impl<T> StreamingRequest<T> {
    /// Builds a typed streaming request over a raw byte stream, decoding each message
    /// with `decode`.
    pub fn new<F>(
        headers: MetadataMap,
        extensions: Extensions,
        stream: Pin<Box<dyn Stream<Item = Bytes> + Send>>,
        decode: F,
    ) -> Self
    where
        T: Send + 'static,
        F: FnMut(Bytes) -> Result<T, Status> + Send + 'static,
    {
        Self {
            headers,
            extensions,
            rx: Box::pin(stream.map(decode)),
        }
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
    fn drains_owned_boxed_stream() {
        let (mut tx, rx) = mpsc::channel::<crate::Result<i32>>(4);
        tx.try_send(Ok(1)).unwrap();
        tx.try_send(Ok(2)).unwrap();
        drop(tx);

        // Erase a concrete channel receiver into the owned, boxed Stream.
        let mut req = StreamingRequest {
            headers: MetadataMap::new(),
            extensions: Extensions::new(),
            rx: Box::pin(rx),
        };
        let got: Vec<i32> = block_on(async {
            let mut v = Vec::new();
            while let Some(item) = req.rx.next().await {
                v.push(item.unwrap());
            }
            v
        });
        assert_eq!(got, std::vec![1, 2]);
    }
}
