use core::pin::Pin;

/// Runs handler tasks on some executor. Fire-and-forget: completion flows back through
/// the response funnel, not a join handle, so implementations need only spawn (e.g.
/// `tokio::spawn`, `wasm_bindgen_futures::spawn_local`).
pub trait Spawner {
    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send + 'static>>);
}

/// Spawns onto the ambient Tokio runtime. `serve` must therefore run within a runtime
/// context; works on both the current-thread and multi-thread runtimes.
#[cfg(feature = "tokio")]
#[derive(Clone, Copy, Default)]
pub struct TokioSpawner;

#[cfg(feature = "tokio")]
impl Spawner for TokioSpawner {
    fn spawn(&self, fut: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        tokio::spawn(fut);
    }
}
