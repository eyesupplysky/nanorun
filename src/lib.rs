//! nanorun — a from-scratch async runtime in Rust.
//!
//! Reference implementation in the spirit of `mio` plus a stripped-down
//! `tokio`. Per-task overhead is measured in nanoseconds and the runtime
//! is designed so hot-path costs stay in that range.
//!
//! Read top-down: this file, then [`executor`], then [`reactor`], then
//! [`time`]. Tasks live in [`task`], the user-facing entry point lives
//! in [`runtime`], and all raw OS calls are isolated to [`sys`]. Internal
//! waker plumbing lives in `crate::waker` (not part of the public API).

#![warn(missing_docs)]

pub mod executor;
#[cfg(any(target_os = "linux", target_os = "windows"))]
pub mod net;
pub mod reactor;
pub mod runtime;
pub mod sys;
pub mod task;
pub mod time;
pub(crate) mod waker;

/// Re-export of [`executor::block_on`] for the common case.
pub use executor::block_on;

/// Re-export of [`executor::Handle`] for the common case.
pub use executor::Handle;

/// Re-export of [`runtime::Runtime`] for the common case.
pub use runtime::Runtime;

/// Re-export of [`task::JoinHandle`] for the common case.
pub use task::JoinHandle;

/// Re-export of [`task::yield_now`] for the common case.
pub use task::yield_now;

/// Spawn `future` onto the current thread's runtime.
///
/// Reads the per-worker [`Handle`] thread-local installed by the
/// runtime; only callable from inside a future polled by a nanorun
/// worker thread. Panics otherwise.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: core::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    Handle::current().spawn(future)
}
