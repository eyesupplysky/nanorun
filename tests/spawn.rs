//! Integration tests for `Runtime::spawn` and `JoinHandle` (M3).
//!
//! These tests exercise the public spawn API as an external user would.
//! They cover: spawn-then-await, throughput across many tasks, cross-task
//! wakes, JoinHandle drop-detach, multiple coexisting runtimes, runtime
//! drop while tasks are queued, and nested spawn through an
//! `Arc<Runtime>` handle.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use nanorun::Runtime;

#[test]
fn spawn_completes_and_yields_value() {
    let rt = Runtime::with_workers(2);
    let h = rt.spawn(async { 42_u32 });
    let v = rt.block_on(async move { h.await });
    assert_eq!(v, 42);
}

#[test]
fn many_spawned_tasks_all_complete() {
    let rt = Runtime::with_workers(4);
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let c = Arc::clone(&counter);
        handles.push(rt.spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        }));
    }
    rt.block_on(async move {
        for h in handles {
            h.await;
        }
    });
    assert_eq!(counter.load(Ordering::SeqCst), 1000);
}

#[test]
fn join_handle_drop_detaches_task() {
    let rt = Runtime::with_workers(1);
    let signal = Arc::new(AtomicUsize::new(0));
    let s = Arc::clone(&signal);
    let h = rt.spawn(async move {
        s.fetch_add(1, Ordering::SeqCst);
        99_u32
    });
    drop(h);
    // Give the worker a chance to poll the detached task.
    thread::sleep(Duration::from_millis(50));
    drop(rt);
    assert_eq!(signal.load(Ordering::SeqCst), 1);
}

#[test]
fn nested_spawn_via_arc_runtime() {
    let rt = Arc::new(Runtime::with_workers(2));
    let rt_inner = Arc::clone(&rt);
    let h = rt.spawn(async move {
        let inner = rt_inner.spawn(async { 7_u32 });
        inner.await
    });
    let v = rt.block_on(async move { h.await });
    assert_eq!(v, 7);
}

#[test]
fn cross_task_signal_via_waker() {
    struct Wait {
        slot: Arc<Mutex<Option<Waker>>>,
        signal: Arc<AtomicUsize>,
    }
    impl Future for Wait {
        type Output = u32;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
            if self.signal.load(Ordering::Acquire) == 1 {
                Poll::Ready(11)
            } else {
                *self.slot.lock().expect("waker slot poisoned") = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }

    let rt = Runtime::with_workers(2);
    let slot: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
    let signal: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

    let waiter = rt.spawn({
        let slot = Arc::clone(&slot);
        let signal = Arc::clone(&signal);
        async move { Wait { slot, signal }.await }
    });

    let signaller = rt.spawn({
        let slot = Arc::clone(&slot);
        let signal = Arc::clone(&signal);
        async move {
            // Brief pause so the waiter parks first. M4 will replace
            // this with `nanorun::time::sleep`.
            thread::sleep(Duration::from_millis(20));
            signal.store(1, Ordering::Release);
            if let Some(w) = slot.lock().expect("waker slot poisoned").take() {
                w.wake();
            }
        }
    });

    let v = rt.block_on(async move {
        signaller.await;
        waiter.await
    });
    assert_eq!(v, 11);
}

#[test]
fn multiple_runtimes_are_independent() {
    let rt1 = Runtime::with_workers(1);
    let rt2 = Runtime::with_workers(1);
    let h1 = rt1.spawn(async { "a" });
    let h2 = rt2.spawn(async { "b" });
    let v1 = rt1.block_on(async move { h1.await });
    let v2 = rt2.block_on(async move { h2.await });
    assert_eq!(v1, "a");
    assert_eq!(v2, "b");
}

#[test]
fn runtime_drop_with_queued_tasks_does_not_hang() {
    struct YieldOnce(bool);
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    let rt = Runtime::with_workers(1);
    for _ in 0..200 {
        rt.spawn(async { YieldOnce(false).await });
    }
    let started = Instant::now();
    drop(rt);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "runtime drop took {:?}, expected sub-second",
        started.elapsed()
    );
}

#[test]
fn yielding_tasks_make_progress_under_contention() {
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

    let rt = Runtime::with_workers(4);
    let mut handles = Vec::with_capacity(50);
    for _ in 0..50 {
        handles.push(rt.spawn(YieldN(20)));
    }
    rt.block_on(async move {
        for h in handles {
            assert_eq!(h.await, 0);
        }
    });
}
