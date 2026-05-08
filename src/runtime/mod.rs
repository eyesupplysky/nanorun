//! User-facing entry point: a runtime composes executor, reactor, and timers.
//!
//! Most users construct a [`Runtime`] and call [`Runtime::block_on`] or
//! [`Runtime::spawn`]. The free function [`crate::block_on`] is a
//! convenience wrapper that builds a one-shot single-worker runtime,
//! drives `f` to completion, and tears the runtime down on return.

pub(crate) mod context;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Wake;
use std::thread::{self, Thread};

use crate::executor::multi::Multi;
use crate::executor::Handle;
use crate::task::JoinHandle;

/// Composed runtime: multi-worker executor + shared reactor.
pub struct Runtime {
    multi: Multi,
}

impl Runtime {
    /// Construct a runtime sized to [`std::thread::available_parallelism`],
    /// falling back to a single worker if the OS reports an error.
    #[must_use]
    pub fn new() -> Self {
        let workers = thread::available_parallelism().map_or(1, NonZeroUsize::get);
        Self::with_workers(workers)
    }

    /// Construct a runtime with `worker_count` worker threads.
    #[must_use]
    pub fn with_workers(worker_count: usize) -> Self {
        Self {
            multi: Multi::new(worker_count),
        }
    }

    /// Spawn `future` onto this runtime; returns a [`JoinHandle`] for its output.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.multi.spawner().spawn(future)
    }

    /// A cheap-clone [`Handle`] to this runtime, usable from any thread.
    ///
    /// Use this to spawn from off-runtime threads, or move a clone into a
    /// future to spawn from inside a task without an `Arc<Runtime>`.
    #[must_use]
    pub fn handle(&self) -> Handle {
        self.multi.handle()
    }

    /// Drive `f` to completion on this runtime.
    ///
    /// # Example
    ///
    /// ```
    /// use nanorun::Runtime;
    ///
    /// let rt = Runtime::new();
    /// let value = rt.block_on(async { 40 + 2 });
    /// assert_eq!(value, 42);
    /// ```
    pub fn block_on<F>(&self, f: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = self.spawn(f);
        block_on_handle(handle)
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on_handle<T>(mut handle: JoinHandle<T>) -> T
where
    T: Send + 'static,
{
    assert!(
        !context::is_worker(),
        "Runtime::block_on cannot be called from inside a nanorun runtime worker thread",
    );
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match Pin::new(&mut handle).poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => thread::park(),
        }
    }
}
