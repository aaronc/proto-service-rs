use std::{
    pin::Pin,
    sync::{Mutex, PoisonError, atomic::AtomicU64},
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
    inner: Arc<Inner>,
}

struct Inner {
    next_req_id: AtomicU64,
    sessions: Mutex<BTreeMap<u64, Session>>,
    outbound: futures_channel::mpsc::UnboundedSender<RequestPacket>,
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
        let req_id = self
            .inner
            .next_req_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed); // TODO what ordering?
        let (call_end_tx, call_end_rx) = futures_channel::oneshot::channel();
        let session = Session {
            res_tx: SessionTx::Single(call_end_tx),
        };
        {
            let mut sessions = self
                .inner
                .sessions
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            sessions.insert(req_id, session);
        }
        let mut req_tx = self.inner.outbound.clone();
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
