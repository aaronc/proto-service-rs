use alloc::boxed::Box;
use bytes::Bytes;
use core::pin::Pin;
use futures_core::Stream;
use futures_sink::Sink;

use crate::{
    MetadataMap,
    server::{CallEnd, SendError},
};

pub type RawRequestSink = Pin<Box<dyn Sink<RawRequestFrame, Error = SendError> + Send>>;

pub enum RawRequestFrame {
    Message(Bytes),
    Done,
}

pub type CallEndFut = Pin<Box<dyn Future<Output = CallEnd> + Send>>;
pub type RawResponseStream = Pin<Box<dyn Stream<Item = RawResponseFrame> + Send>>;

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

pub enum RawResponseFrame {
    Headers(MetadataMap),
    Message(Bytes),
    CallEnd(CallEnd),
}

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
