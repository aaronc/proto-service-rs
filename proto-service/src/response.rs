use crate::MetadataMap;

/// A unary response: a single message with leading and trailing metadata.
///
/// Shared by both sides: returned by a server handler and received by a client.
pub struct Response<T> {
    /// Leading metadata, before the message.
    pub headers: MetadataMap,
    /// The response message.
    pub message: T,
    /// Trailing metadata, after the message.
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
