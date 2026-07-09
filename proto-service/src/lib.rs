#![no_std]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

mod extensions;
mod metadata;
mod request;
mod response;
mod router;
mod service;
mod status;

pub use extensions::*;
pub use metadata::*;
pub use request::*;
pub use response::*;
pub use router::*;
pub use service::*;
pub use status::*;

// Re-exported so the channel types can be named/constructed, and their
// combinators used, without a separate dependency on futures.
pub use bytes::Bytes;
pub use futures_core::Stream;
pub use futures_sink::Sink;
pub use futures_util::{SinkExt, StreamExt};

pub type Result<T> = core::result::Result<T, Status>;
