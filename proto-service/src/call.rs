use bytes::Bytes;

use crate::{Code, MetadataMap, Status};

/// How the call ended: what [`Service::handle`] resolves to, mirroring the wire's
/// terminal Close frame.
pub struct CallEnd {
    /// Metadata sent after the response stream ends.
    pub trailers: MetadataMap,
    /// Final status of the call.
    pub status: Status,
    /// The entire encoded response for single-response methods. Mutually exclusive
    /// with sending on `streaming_response`; must be `None` when `status` is not OK.
    pub single_response: Option<Bytes>,
    /// Leading metadata for single-response methods; streaming methods send
    /// [`RawResponseFrame::Headers`] instead.
    pub single_headers: Option<MetadataMap>,
}

impl CallEnd {
    /// Terminal end carrying only a status: empty trailers, no response body or headers.
    pub fn error(status: Status) -> Self {
        Self {
            trailers: MetadataMap::new(),
            status,
            single_response: None,
            single_headers: None,
        }
    }

    /// Successful single-response end: leading headers, the encoded response, and trailers.
    pub fn single(headers: MetadataMap, response: Bytes, trailers: MetadataMap) -> Self {
        Self {
            trailers,
            status: Status::ok(""),
            single_response: Some(response),
            single_headers: Some(headers),
        }
    }

    /// Successful streaming-response end: only trailers (messages went out on the sink).
    pub fn streaming(trailers: MetadataMap) -> Self {
        Self {
            trailers,
            status: Status::ok(""),
            single_response: None,
            single_headers: None,
        }
    }
}

/// Error returned by a response [`Sink`] once its receiving end has been dropped
/// or closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("sending on a closed channel")]
pub struct SendError;

impl From<SendError> for Status {
    fn from(_: SendError) -> Self {
        Code::Cancelled.into()
    }
}
