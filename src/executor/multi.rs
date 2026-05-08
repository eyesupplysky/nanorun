//! Multi-threaded executor driver.
//!
//! N worker threads compete over runnable tasks via per-worker LIFO
//! local queues, a shared injector, and work-stealing across peers.
//! Wakers schedule tasks through the [`Spawner`], which pushes the
//! ref onto the injector and conditionally fires the reactor wake.
//!
//! # Layout
//!
//! - [`Shared`] — `Arc`-shared state: injector queue, per-worker slots,
//!   shutdown signal, round-robin unpark cursor, driver-token + parked
//!   flag.
//! - [`worker::WorkerSlot`] — per-worker state (sibling submodule):
//!   local LIFO `VecDeque` and the `OnceLock<Thread>` populated when the
//!   worker thread starts.
//! - [`Spawner`] — internal facade that implements [`Schedule`].
//! - [`Handle`] — public, cheap-clone wrapper exposing `spawn` to user
//!   code; obtainable via [`Runtime::handle`](crate::Runtime::handle) or
//!   [`Handle::current`] from inside a spawned task.
//! - [`Multi`] — owns the spawned worker threads and the shared state.
//!
//! # Scheduling
//!
//! Wakes and spawns both push onto the **injector** (global queue).
//! Workers prefer their **local** queue (LIFO) for cache locality, fall
//! back to the injector, and finally **steal halves** from a randomly
//! chosen peer's local queue (FIFO from the peer's front). Every
//! [`POLL_BUDGET`] ticks the worker checks the injector first regardless,
//! to keep the global queue from starving while local work churns.
//!
//! # Parking and the reactor
//!
//! Idle workers fall into one of two paths:
//!
//! - **Driver path:** if no worker currently holds the driver token,
//!   the idle worker takes it via CAS and calls
//!   [`Reactor::poll`](crate::reactor::Reactor::poll) with no timeout.
//!   That call blocks the worker in `epoll_wait` (Linux) or a condvar
//!   wait (fallback) until either an fd becomes ready or
//!   [`ReactorHandle::wake`](crate::reactor::ReactorHandle::wake)
//!   fires.
//! - **Park path:** workers that lost the CAS call [`thread::park`].
//!
//! `Spawner::schedule` unconditionally pushes onto the injector and
//! unparks one worker (the unpark token is idempotent and cheap). It
//! fires `ReactorHandle::wake` only when [`Shared::driver_parked`]
//! observes a driver currently inside `Reactor::poll`. The
//! park-side/schedule-side memory ordering that makes this gate
//! lost-wakeup-free is documented in [`worker::idle`].

mod worker;

#[cfg(test)]
mod tests;

use core::future::Future;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle as ThreadJoinHandle};

use crate::reactor::Reactor;
use crate::runtime::context::{DriverGuard, Guard, HandleGuard, WorkerMarker};
use crate::task::{spawn_raw, JoinHandle, Schedule, TaskRef};
use crate::time::Driver;

use worker::{idle, steal, WorkerSlot};

/// Per-tick budget before a worker prefers the injector over its local queue.
/// 61 is tokio's number — large enough for cache-friendly LIFO bursts,
/// small enough that the global queue cannot starve.
const POLL_BUDGET: u64 = 61;

/// Per-runtime shared state. Lives behind an [`Arc`].
///
/// Fields are module-private; the [`worker`] submodule accesses them
/// through Rust's parent-to-child visibility rule.
pub(crate) struct Shared {
    injector: Mutex<VecDeque<TaskRef>>,
    workers: Vec<WorkerSlot>,
    next_unpark: AtomicU64,
    shutdown: AtomicBool,
    reactor: Reactor,
    driver: Driver,
    driver_held: AtomicBool,
    driver_parked: AtomicBool,
}

/// Schedule facade implementing [`Schedule`] over a [`Shared`] runtime.
#[derive(Clone)]
pub(crate) struct Spawner {
    shared: Arc<Shared>,
}

impl Schedule for Spawner {
    fn schedule(&self, task: TaskRef) {
        if self.shared.shutdown.load(Ordering::Acquire) {
            // Runtime is shutting down: drop the task instead of
            // queuing. This is the cycle-breaker for tasks whose
            // wakers fire after [`Multi::drop`] has joined the
            // workers — without this drop, the task allocation
            // (which owns an `Arc<Shared>` via its `Spawner`) would
            // keep `Shared` alive forever.
            drop(task);
            return;
        }
        push_global(&self.shared, task);
        wake_driver_if_parked(&self.shared);
        unpark_one(&self.shared);
    }
}

impl Spawner {
    /// Spawn `future` onto this runtime; returns a [`JoinHandle`] for its output.
    pub(crate) fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (queue_ref, join_ref) = spawn_raw(future, self.clone());
        push_global(&self.shared, queue_ref);
        wake_driver_if_parked(&self.shared);
        unpark_one(&self.shared);
        JoinHandle::new(join_ref)
    }
}

/// Public, cheap-clone handle to a runtime. Obtain one via
/// [`Runtime::handle`](crate::Runtime::handle) on any thread, or
/// [`Handle::current`] from inside a spawned task.
#[derive(Clone)]
pub struct Handle {
    spawner: Spawner,
}

impl Handle {
    /// Return the current thread's [`Handle`], panicking if none is installed.
    ///
    /// Only callable from inside a future polled by a nanorun worker thread.
    #[must_use]
    pub fn current() -> Self {
        crate::runtime::context::current_handle().expect(
            "Handle::current() called outside a nanorun runtime worker thread; \
             use Runtime::handle on any thread, or call this only from inside a spawned task",
        )
    }

    /// Return the current thread's [`Handle`], or `None` if none is installed.
    #[must_use]
    pub fn try_current() -> Option<Self> {
        crate::runtime::context::current_handle()
    }

    /// Spawn `future` onto the runtime; returns a [`JoinHandle`] for its output.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawner.spawn(future)
    }
}

/// Owns the worker threads. Drop joins them after signalling shutdown.
pub(crate) struct Multi {
    shared: Arc<Shared>,
    threads: Vec<ThreadJoinHandle<()>>,
}

impl Multi {
    /// Build a runtime with `worker_count` worker threads.
    pub(crate) fn new(worker_count: usize) -> Self {
        assert!(worker_count >= 1, "Multi requires at least one worker");
        let reactor = Reactor::new().expect("reactor::new");
        let driver = Driver::new(reactor.handle());
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            workers.push(WorkerSlot::new());
        }
        let shared = Arc::new(Shared {
            injector: Mutex::new(VecDeque::new()),
            workers,
            next_unpark: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            reactor,
            driver,
            driver_held: AtomicBool::new(false),
            driver_parked: AtomicBool::new(false),
        });
        let mut threads = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let s = Arc::clone(&shared);
            let h = thread::Builder::new()
                .name(format!("nanorun-worker-{i}"))
                .spawn(move || run_worker(&s, i))
                .expect("spawn worker thread");
            threads.push(h);
        }
        Self { shared, threads }
    }

    /// Cheap handle for spawning onto this runtime.
    pub(crate) fn spawner(&self) -> Spawner {
        Spawner {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Public-API [`Handle`] to this runtime.
    pub(crate) fn handle(&self) -> Handle {
        Handle {
            spawner: self.spawner(),
        }
    }
}

impl Drop for Multi {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Release);
        // Break out the driver if any is parked in `reactor.poll`. The
        // shutdown wake is unconditional: we do not consult
        // `driver_parked` because we want to fire the eventfd whether
        // the flag was visible to us or not.
        self.shared
            .reactor
            .handle()
            .wake()
            .expect("reactor wake from shutdown");
        for w in &self.shared.workers {
            if let Some(th) = w.thread.get() {
                th.unpark();
            }
        }
        for h in core::mem::take(&mut self.threads) {
            h.join().expect("worker thread panicked");
        }
        // Workers are gone. Drain queues and the timer driver to break
        // the `Arc<Shared>` ↔ task cycle: each `TaskRef` we drop and each
        // `Waker` the driver holds releases a refcount on a task header,
        // which transitively keeps the runtime alive. Without these
        // drops, `Arc<Shared>` would live until every outstanding waker
        // fires (or never, if it never fires).
        let _guard = Guard::install(&self.shared.reactor);
        self.shared
            .injector
            .lock()
            .expect("injector poisoned")
            .clear();
        for w in &self.shared.workers {
            w.local.lock().expect("local queue poisoned").clear();
        }
        self.shared.driver.clear();
    }
}

fn push_global(shared: &Shared, task: TaskRef) {
    shared
        .injector
        .lock()
        .expect("injector poisoned")
        .push_back(task);
}

fn pop_global(shared: &Shared) -> Option<TaskRef> {
    shared
        .injector
        .lock()
        .expect("injector poisoned")
        .pop_front()
}

fn pop_local(shared: &Shared, id: usize) -> Option<TaskRef> {
    shared.workers[id]
        .local
        .lock()
        .expect("local queue poisoned")
        .pop_back()
}

fn wake_driver_if_parked(shared: &Shared) {
    if shared.driver_parked.load(Ordering::SeqCst) {
        shared
            .reactor
            .handle()
            .wake()
            .expect("reactor wake from schedule");
    }
}

fn unpark_one(shared: &Shared) {
    let n = shared.workers.len();
    if n == 0 {
        return;
    }
    let raw = shared.next_unpark.fetch_add(1, Ordering::Relaxed);
    // `raw % n` lies in `0..n` where `n: usize`, so the result fits in usize on every target.
    #[allow(clippy::cast_possible_truncation)]
    let idx = (raw % n as u64) as usize;
    if let Some(th) = shared.workers[idx].thread.get() {
        th.unpark();
    }
}

fn run_worker(shared: &Arc<Shared>, my_id: usize) {
    shared.workers[my_id]
        .thread
        .set(thread::current())
        .expect("worker thread set twice");

    // Four RAII guards bracket the run loop. They are installed in the
    // order WorkerMarker → HandleGuard → reactor Guard → DriverGuard,
    // so on worker exit they drop in reverse: DriverGuard first, then
    // reactor Guard (which owns the raw Reactor pointer), then
    // HandleGuard, then WorkerMarker.
    let _worker_marker = WorkerMarker::enter();

    // Install the runtime's [`Handle`] as this worker's thread-local
    // current handle. `Handle::current()` and `nanorun::spawn` read this
    // slot.
    let _handle_guard = HandleGuard::install(Handle {
        spawner: Spawner {
            shared: Arc::clone(shared),
        },
    });

    // Install the runtime's reactor as this worker's thread-local
    // current reactor. Tasks polled on this thread call
    // `runtime::context::with_current` to register fds against it.
    let _reactor_guard = Guard::install(&shared.reactor);

    // Install the runtime's timer driver as this worker's thread-local
    // current driver. `crate::time::sleep` futures register their
    // wakers via `runtime::context::with_current_driver`.
    let _driver_guard = DriverGuard::install(&shared.driver);

    let mut tick: u64 = 0;
    let mut rng: u64 = (my_id as u64)
        .wrapping_add(1)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15);

    loop {
        if shared.shutdown.load(Ordering::Acquire) {
            return;
        }

        let task = if tick % POLL_BUDGET == 0 {
            pop_global(shared).or_else(|| pop_local(shared, my_id))
        } else {
            pop_local(shared, my_id).or_else(|| pop_global(shared))
        };

        let task = match task {
            Some(t) => t,
            None => {
                if let Some(t) = steal(shared, my_id, &mut rng) {
                    t
                } else {
                    idle(shared, my_id);
                    continue;
                }
            }
        };

        task.poll();
        tick = tick.wrapping_add(1);
    }
}
