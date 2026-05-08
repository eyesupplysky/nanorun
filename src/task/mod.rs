//! Task abstraction: spawned futures and their handles.
//!
//! A task is a future that the executor owns and polls. The user-facing
//! handle returned by spawning is a [`JoinHandle`]. The internal layout
//! lives in the `raw` submodule (crate-private). Cooperative yield is
//! exposed at [`yield_now`].

mod join;
pub(crate) mod raw;
mod yield_now;

pub use join::JoinHandle;
pub use yield_now::yield_now;

pub(crate) use raw::{spawn_raw, Schedule, TaskRef};
