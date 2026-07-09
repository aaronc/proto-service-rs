// Note: not no_std: futures-channel's mpsc and catch_unwind both require std,
// but in the future we could maybe have an alternate build config.

extern crate alloc;

pub mod client;
pub mod spawner;
pub mod transport;

/// Wire types generated from `proto/proto_pipe/v1/packet.proto`.
pub mod packet {
    include!(concat!(env!("OUT_DIR"), "/proto_pipe.v1.rs"));
}
