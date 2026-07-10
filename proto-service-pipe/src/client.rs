use std::{
    pin::Pin,
    sync::{Mutex, PoisonError},
};

use alloc::{collections::BTreeMap, sync::Arc};

use futures_util::{Sink, SinkExt, Stream};
use proto_service::{
    CallEnd, SendError,
    client::{ClientConnection, RawResponseFrame},
};

use crate::{
    packet::{RequestPacket, ResponsePacket, metadata, request_packet},
    util::to_packet_metadata,
};

pub struct PipeClient {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    last_req_id: u64,
    sessions: BTreeMap<u64, Session>,
    funnel: futures_channel::mpsc::Sender<RequestPacket>,
    tx: Pin<Box<dyn Sink<RequestPacket, Error = SendError> + Send>>,
    rx: Pin<Box<dyn Stream<Item = ResponsePacket> + Send>>,
}

struct Session {
    res_tx: SessionTx,
}

enum SessionTx {
    Single(futures_channel::oneshot::Sender<CallEnd>),
    Streaming(futures_channel::mpsc::Sender<RawResponseFrame>),
}

impl ClientConnection for PipeClient {
    fn unary(
        &self,
        method: &str,
        headers: proto_service::MetadataMap,
        request: bytes::Bytes,
    ) -> proto_service::client::CallEndFut {
        let mut inner = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let req_id = inner.last_req_id + 1;
        inner.last_req_id += 1;
        let (call_end_tx, call_end_rx) = futures_channel::oneshot::channel();
        let session = Session {
            res_tx: SessionTx::Single(call_end_tx),
        };
        inner.sessions.insert(req_id, session);
        let mut req_tx = inner.funnel.clone();
        let packet = RequestPacket {
            req_id,
            frame: Some(request_packet::Frame::Begin(request_packet::Begin {
                method: method.into(),
                headers: Some(to_packet_metadata(&headers)),
                single_request: Some(request.into()),
            })),
        };
        Box::pin(async move {
            // TODO handle send error
            let _ = req_tx.send(packet).await;
            call_end_rx.await.unwrap()
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
