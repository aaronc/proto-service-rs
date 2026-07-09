use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use bytes::Bytes;
use core::future::Future;
use core::pin::Pin;

use futures_core::Stream;
use futures_sink::Sink;

use crate::{CallEnd, Extensions, MetadataMap, SendError, Status};

/// A service the transport dispatches requests to.
///
/// Generated code implements this: `handle` matches on `call.method_name`, decodes
/// the request bytes, runs the method, and encodes the response bytes.
pub trait Service: Send + Sync {
    /// Fully-qualified proto service name, e.g. `"pkg.Service"`.
    const SERVICE_NAME: &'static str;

    /// Dispatches one call to completion.
    fn handle(&self, call: Call) -> impl Future<Output = CallEnd> + Send;

    /// Shape of `method_name`, or `None` if unknown — the transport then responds
    /// UNIMPLEMENTED without calling [`Self::handle`]. Must agree with `handle`'s own
    /// method dispatch.
    fn describe_method(&self, method_name: &str) -> Option<MethodDescriptor>;
}

pub struct MethodDescriptor {
    pub client_streaming: bool,
    pub server_streaming: bool,
}

/// One call as handed to a [`Service`]: the type-erased dispatch surface. Channels
/// carry encoded bytes; method dispatch decodes and encodes on top.
pub struct Call {
    /// Method being invoked, e.g. `"Echo"`.
    pub method_name: String,
    /// Values attached to the request by the transport or middleware.
    pub extensions: Extensions,
    /// Request metadata.
    pub headers: MetadataMap,
    /// Encoded request payload.
    pub req_payload: RequestPayload,
    /// Sink for the response's non-terminal frames; `Some` iff the method's
    /// [`MethodDescriptor`] says `server_streaming`. Single-response methods return
    /// everything through [`CallEnd`] instead.
    pub streaming_response: Option<Pin<Box<dyn Sink<RawResponseFrame, Error = SendError> + Send>>>,
}

pub enum RequestPayload {
    Single(Bytes),
    Streaming(Pin<Box<dyn Stream<Item = Bytes> + Send>>),
}

impl RequestPayload {
    /// The single encoded request for a single-request method, or an error [`CallEnd`]
    /// if the call carries a request stream instead.
    pub fn into_single(self) -> Result<Bytes, CallEnd> {
        match self {
            Self::Single(bytes) => Ok(bytes),
            Self::Streaming(_) => Err(CallEnd::error(Status::invalid_argument(
                "expected single request",
            ))),
        }
    }

    /// The request stream for a streaming-request method, or an error [`CallEnd`] if the
    /// call carries a single request instead.
    pub fn into_streaming(self) -> Result<Pin<Box<dyn Stream<Item = Bytes> + Send>>, CallEnd> {
        match self {
            Self::Streaming(stream) => Ok(stream),
            Self::Single(_) => Err(CallEnd::error(Status::invalid_argument(
                "expected streaming request",
            ))),
        }
    }
}

/// A non-terminal response frame carrying encoded bytes; the terminal frame is the
/// returned [`CallEnd`]. The typed, generic form is [`crate::ResponseFrame`].
pub enum RawResponseFrame {
    /// Leading response metadata. At most once, only as the first frame.
    Headers(MetadataMap),
    /// An encoded streaming response message.
    Message(Bytes),
}

pub trait DynService: Send + Sync {
    fn service_name(&self) -> &'static str;
    fn handle(
        self: Arc<Self>,
        call: Call,
    ) -> Pin<Box<dyn Future<Output = CallEnd> + Send + 'static>>;
    fn describe_method(&self, method_name: &str) -> Option<MethodDescriptor>;
}

impl<T: Service + 'static> DynService for T {
    fn service_name(&self) -> &'static str {
        T::SERVICE_NAME
    }

    fn handle(
        self: Arc<Self>,
        call: Call,
    ) -> Pin<Box<dyn Future<Output = CallEnd> + Send + 'static>> {
        Box::pin(async move { Service::handle(&*self, call).await })
    }

    fn describe_method(&self, method_name: &str) -> Option<MethodDescriptor> {
        Service::describe_method(self, method_name)
    }
}
