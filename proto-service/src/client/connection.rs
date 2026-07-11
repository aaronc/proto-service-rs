use alloc::sync::Arc;

use bytes::Bytes;

use crate::MetadataMap;
use crate::client::{CallEndFut, RawRequestSink, RawResponseStream};

/// Opens calls on one connection. Calls are multiplexed, so this takes `&self` and
/// the handles it returns own their state rather than borrowing the connection.
///
/// Each method sends the call's opening frame -- method, headers, and for
/// single-request shapes the payload -- before returning. It must not wait for the
/// caller to poll the returned handles: the server may legally respond before the
/// client sends its first message.
///
/// None of these return a `Result`. A call that cannot start ends like any other,
/// with a [`CallEnd`](crate::CallEnd) carrying the status.
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
