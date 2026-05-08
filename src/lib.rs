//! nanorun — a from-scratch async runtime in Rust.
//!
//! Reference implementation in the spirit of `mio` plus a stripped-down
//! `tokio`. Per-task overhead is measured in nanoseconds and the runtime
//! is designed so hot-path costs stay in that range.
//!
//! Read top-down: this file, then [`executor`], then [`reactor`], then
//! [`time`]. Tasks live in [`task`], the user-facing entry point lives
//! in [`runtime`], and all raw OS calls are isolated to [`sys`].

#![warn(missing_docs)]

pub mod executor;
#[cfg(target_os = "linux")]
pub mod net;
pub mod reactor;
pub mod runtime;
pub mod sys;
pub mod task;
pub mod time;
pub mod waker;

/// Re-export of [`executor::block_on`] for the common case.
pub use executor::block_on;

/// Re-export of [`runtime::Runtime`] for the common case.
pub use runtime::Runtime;

/// Re-export of [`task::JoinHandle`] for the common case.
pub use task::JoinHandle;
