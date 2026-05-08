//! User-facing handle for awaiting a spawned task's output.

use core::marker::PhantomData;

/// Handle returned from spawning a task. Awaiting it yields the task's output.
pub struct JoinHandle<T>(PhantomData<T>);
