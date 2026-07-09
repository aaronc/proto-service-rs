#![no_std]

extern crate alloc;

#[cfg(feature = "std")]
extern crate std;

mod call;
pub mod client;
mod extensions;
mod metadata;
pub mod server;
mod status;

pub use call::*;
pub use extensions::*;
pub use metadata::*;
pub use status::*;

// Re-exported so the channel types can be named/constructed, and their
// combinators used, without a separate dependency on futures.
pub use bytes::Bytes;
pub use futures_core::Stream;
pub use futures_sink::Sink;
pub use futures_util::{SinkExt, StreamExt};

pub type Result<T> = core::result::Result<T, Status>;
