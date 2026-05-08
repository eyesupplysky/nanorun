//! Task abstraction: spawned futures and their handles.
//!
//! A task is a future that the executor owns and polls. The user-facing
//! handle returned by spawning is a [`JoinHandle`]. The internal layout
//! lives in the [`raw`] submodule.

mod join;
pub(crate) mod raw;

pub use join::JoinHandle;

pub(crate) use raw::{spawn_raw, Schedule, TaskRef};
