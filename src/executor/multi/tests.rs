//! Unit tests for the multi-worker executor.

use super::*;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::atomic::{AtomicUsize, Ordering as AtomOrd};
use std::sync::Arc;
use std::task::Wake;
use std::thread::Thread;
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

    let multi = Multi::new(2);
    let signal = Arc::new(Mutex::new((false, None::<Waker>)));

    let s = Arc::clone(&signal);
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
