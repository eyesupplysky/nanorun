//! Multi-threaded executor driver.
//!
//! N worker threads compete over runnable tasks via per-worker LIFO
//! local queues, a shared injector, and work-stealing across peers.
//! Wakers schedule tasks through the [`Spawner`], which pushes the
//! ref onto the injector and unparks one worker.
//!
//! # Layout
//!
//! - [`Shared`] — `Arc`-shared state: injector queue, per-worker slots,
//!   shutdown signal, round-robin unpark cursor.
//! - [`WorkerSlot`] — per-worker state: local LIFO `VecDeque` and the
//!   `OnceLock<Thread>` populated when the worker thread starts.
//! - [`Spawner`] — facade that implements [`Schedule`] and exposes
//!   `spawn` to user code.
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
//!   fires. On return, the worker releases the token and re-enters
//!   the run loop.
//! - **Park path:** workers that lost the CAS call [`thread::park`].
//!
//! Every [`Spawner::schedule`] both fires `ReactorHandle::wake` (in case
//! the driver is parked in `epoll_wait`) and unparks one worker (in
//! case any are in `thread::park`). The two paths are independent; the
//! double-fire is idempotent and cheap.
//!
//! The classic "set parked, re-check, then park" race is avoided
//! because [`thread::park`] respects the unpark token deposited by any
//! prior schedule.

use core::future::Future;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle as ThreadJoinHandle, Thread};

use crate::reactor::Reactor;
use crate::runtime::context::Guard;
use crate::task::{spawn_raw, JoinHandle, Schedule, TaskRef};

/// Per-tick budget before a worker prefers the injector over its local queue.
/// 61 is tokio's number — large enough for cache-friendly LIFO bursts,
/// small enough that the global queue cannot starve.
const POLL_BUDGET: u64 = 61;

struct WorkerSlot {
    thread: OnceLock<Thread>,
    local: Mutex<VecDeque<TaskRef>>,
}

/// Per-runtime shared state. Lives behind an [`Arc`].
pub(crate) struct Shared {
    injector: Mutex<VecDeque<TaskRef>>,
    workers: Vec<WorkerSlot>,
    next_unpark: AtomicU64,
    shutdown: AtomicBool,
    reactor: Reactor,
    driver_held: AtomicBool,
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
        // Wake whichever idle worker is reachable: the driver via the
        // reactor handle, any thread-parked worker via unpark.
        self.shared
            .reactor
            .handle()
            .wake()
            .expect("reactor wake from schedule");
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
        self.shared
            .reactor
            .handle()
            .wake()
            .expect("reactor wake from spawn");
        unpark_one(&self.shared);
        JoinHandle::new(join_ref)
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
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            workers.push(WorkerSlot {
                thread: OnceLock::new(),
                local: Mutex::new(VecDeque::new()),
            });
        }
        let shared = Arc::new(Shared {
            injector: Mutex::new(VecDeque::new()),
            workers,
            next_unpark: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            reactor,
            driver_held: AtomicBool::new(false),
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

}

impl Drop for Multi {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::Release);
        // Break out the driver if any is parked in `reactor.poll`.
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
        // Workers are gone. Drain queues to break the
        // `Arc<Shared>` ↔ task cycle: each `TaskRef` we drop releases
        // its task's `Spawner`, which holds an `Arc<Shared>`. Without
        // this drain, the runtime's allocation lives until every
        // outstanding waker fires (or never, if it never fires).
        let _guard = Guard::install(&self.shared.reactor);
        self.shared
            .injector
            .lock()
            .expect("injector poisoned")
            .clear();
        for w in &self.shared.workers {
            w.local.lock().expect("local queue poisoned").clear();
        }
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

fn unpark_one(shared: &Shared) {
    let n = shared.workers.len();
    if n == 0 {
        return;
    }
    let raw = shared.next_unpark.fetch_add(1, Ordering::Relaxed);
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

    // Install the runtime's reactor as this worker's thread-local
    // current reactor. Tasks polled on this thread call
    // `runtime::context::with_current` to register fds against it.
    let _guard = Guard::install(&shared.reactor);

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
            None => match steal(shared, my_id, &mut rng) {
                Some(t) => t,
                None => {
                    idle(shared, my_id);
                    continue;
                }
            },
        };

        task.poll();
        tick = tick.wrapping_add(1);
    }
}

fn idle(shared: &Shared, my_id: usize) {
    if shared.shutdown.load(Ordering::Acquire) {
        return;
    }
    if has_work(shared, my_id) {
        return;
    }
    if shared
        .driver_held
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
    {
        // We are the driver. Block on the reactor until something fires.
        // Spurious returns are safe — the run loop re-checks queues.
        shared.reactor.poll(None).expect("reactor poll");
        shared.driver_held.store(false, Ordering::Release);
        return;
    }
    // Someone else is driving; sleep until unparked.
    thread::park();
}

fn steal(shared: &Shared, my_id: usize, rng: &mut u64) -> Option<TaskRef> {
    let n = shared.workers.len();
    if n <= 1 {
        return None;
    }
    let start = (xorshift64(rng) as usize) % n;
    for i in 0..n {
        let victim = (start + i) % n;
        if victim == my_id {
            continue;
        }
        if let Some(t) = steal_from(shared, victim, my_id) {
            return Some(t);
        }
    }
    None
}

fn steal_from(shared: &Shared, victim: usize, my_id: usize) -> Option<TaskRef> {
    let mut victim_q = shared.workers[victim]
        .local
        .lock()
        .expect("victim local poisoned");
    let total = victim_q.len();
    if total == 0 {
        return None;
    }
    let take = total.div_ceil(2);
    let mut stolen: VecDeque<TaskRef> = victim_q.drain(..take).collect();
    drop(victim_q);

    let first = stolen.pop_front();
    if !stolen.is_empty() {
        let mut my_q = shared.workers[my_id]
            .local
            .lock()
            .expect("local queue poisoned");
        my_q.extend(stolen);
    }
    first
}

fn has_work(shared: &Shared, my_id: usize) -> bool {
    if !shared.workers[my_id]
        .local
        .lock()
        .expect("local queue poisoned")
        .is_empty()
    {
        return true;
    }
    if !shared
        .injector
        .lock()
        .expect("injector poisoned")
        .is_empty()
    {
        return true;
    }
    for (i, w) in shared.workers.iter().enumerate() {
        if i == my_id {
            continue;
        }
        if !w.local.lock().expect("victim local poisoned").is_empty() {
            return true;
        }
    }
    false
}

fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    if x == 0 {
        x = 0xDEAD_BEEF_CAFE_F00D;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::pin::Pin;
    use core::task::{Context, Poll, Waker};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomOrd};
    use std::sync::Arc;
    use std::task::Wake;
    use std::time::{Duration, Instant};

    /// Block the calling thread on `handle`'s completion using a thread-park waker.
    fn block_on_handle<T: Send + 'static>(mut handle: JoinHandle<T>) -> T {
        struct ThreadWaker(Thread);
        impl Wake for ThreadWaker {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
        let mut cx = Context::from_waker(&waker);
        loop {
            match Pin::new(&mut handle).poll(&mut cx) {
                Poll::Ready(v) => return v,
                Poll::Pending => thread::park(),
            }
        }
    }

    #[test]
    fn ready_future_completes() {
        let multi = Multi::new(2);
        let h = multi.spawner().spawn(async { 42_u32 });
        let v = block_on_handle(h);
        drop(multi);
        assert_eq!(v, 42);
    }

    #[test]
    fn many_tasks_all_complete() {
        let multi = Multi::new(4);
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..1000 {
            let c = Arc::clone(&counter);
            handles.push(multi.spawner().spawn(async move {
                c.fetch_add(1, AtomOrd::SeqCst);
            }));
        }
        for h in handles {
            block_on_handle(h);
        }
        drop(multi);
        assert_eq!(counter.load(AtomOrd::SeqCst), 1000);
    }

    #[test]
    fn yielding_task_progresses() {
        struct YieldN(usize);
        impl Future for YieldN {
            type Output = usize;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
                if self.0 == 0 {
                    Poll::Ready(0)
                } else {
                    self.0 -= 1;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
        let multi = Multi::new(2);
        let h = multi.spawner().spawn(YieldN(50));
        let v = block_on_handle(h);
        drop(multi);
        assert_eq!(v, 0);
    }

    #[test]
    fn cross_worker_wake_delivers() {
        // A task parks on a custom waker, then another thread wakes it via that waker.
        let multi = Multi::new(2);
        let signal = Arc::new(Mutex::new((false, None::<Waker>)));

        let s = Arc::clone(&signal);
        struct Park {
            slot: Arc<Mutex<(bool, Option<Waker>)>>,
        }
        impl Future for Park {
            type Output = u32;
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
                let mut g = self.slot.lock().expect("park slot poisoned");
                if g.0 {
                    Poll::Ready(7)
                } else {
                    g.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }

        let h = multi.spawner().spawn(Park { slot: s });

        // Sleep briefly to ensure the task has parked.
        thread::sleep(Duration::from_millis(20));
        {
            let mut g = signal.lock().expect("park slot poisoned");
            g.0 = true;
            if let Some(w) = g.1.take() {
                w.wake();
            }
        }

        let started = Instant::now();
        let v = block_on_handle(h);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(v, 7);
        drop(multi);
    }

    #[test]
    fn shutdown_joins_all_workers() {
        let multi = Multi::new(4);
        let started = Instant::now();
        drop(multi);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "shutdown should not stall on idle workers"
        );
    }
}
