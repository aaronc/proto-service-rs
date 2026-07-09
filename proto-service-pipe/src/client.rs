use alloc::{collections::BTreeMap, sync::Arc};

use futures_channel::mpsc::unbounded;
use futures_util::lock::Mutex;
use proto_service::client::{ClientConnection, RawResponseFrame};

use crate::packet::{RequestPacket, ResponsePacket};

use futures_sink::SinkExt;

pub struct PipeClient {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    last_req_id: u64,
    sessions: BTreeMap<u64, Session>,
    tx: Pin<Box<dyn Sink<RequestPacket> + Send>>,
    rx: Pin<Box<dyn Stream<Item = ResponsePacket>>>,
}

enum Session {
    Unary(Pin<Box<dyn Sink<Item = CallEnd>>>),
    // rx: Pin<Box<dyn Stream<Item = RawResponseFrame>>>,
}

impl ClientConnection for PipeClient {
    fn unary(
        &self,
        method: &str,
        headers: proto_service::MetadataMap,
        request: bytes::Bytes,
    ) -> proto_service::client::CallEndFut {
        let req_id = self.last_req_id + 1;
        self.last_req_id += 1;
        let (tx, rx) = unbounded();
        let session = Session::Unary(tx);
        Box::pin(async {
            self.tx
                .send(RequestPacket {
                    req_id,
                    frame: todo!(),
                })
                .await;
        })
    }

    fn server_stream(
        &self,
        method: &str,
        headers: proto_service::MetadataMap,
        request: bytes::Bytes,
    ) -> proto_service::client::RawResponseStream {
        todo!()
    }

    fn client_stream(
        &self,
        method: &str,
        headers: proto_service::MetadataMap,
    ) -> (
        proto_service::client::RawRequestSink,
        proto_service::client::CallEndFut,
    ) {
        todo!()
    }

    fn bidi(
        &self,
        method: &str,
        headers: proto_service::MetadataMap,
    ) -> (
        proto_service::client::RawRequestSink,
        proto_service::client::RawResponseStream,
    ) {
        todo!()
    }
}
