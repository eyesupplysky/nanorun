//! Integration tests for `Runtime::spawn`, `JoinHandle`, and `Handle` (M3).
//!
//! These tests exercise the public spawn API as an external user would.
//! They cover: spawn-then-await, throughput across many tasks, cross-task
//! wakes, [`JoinHandle`] drop-detach, multiple coexisting runtimes,
//! runtime drop while tasks are queued, nested spawn via a captured
//! [`Handle`], nested spawn via [`nanorun::spawn`] (reads the per-worker
//! thread-local), and cross-thread [`Handle`] clone.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll, Waker};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use nanorun::{Handle, Runtime};

#[test]
fn spawn_completes_and_yields_value() {
    let rt = Runtime::with_workers(2);
    let h = rt.spawn(async { 42_u32 });
    let v = rt.block_on(h);
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
fn nested_spawn_via_handle() {
    let rt = Runtime::with_workers(2);
    let handle = rt.handle();
    let h = rt.spawn(async move {
        let inner = handle.spawn(async { 7_u32 });
        inner.await
    });
    let v = rt.block_on(h);
    assert_eq!(v, 7);
}

#[test]
fn nested_spawn_via_current() {
    // Inside a spawned task, `nanorun::spawn` reads the per-worker
    // thread-local installed by the runtime. No captured handle needed.
    let rt = Runtime::with_workers(2);
    let h = rt.spawn(async move {
        let inner = nanorun::spawn(async { 13_u32 });
        inner.await
    });
    let v = rt.block_on(h);
    assert_eq!(v, 13);
}

#[test]
fn handle_clone_across_threads() {
    // `Handle: Clone + Send + Sync`. Move a clone to a non-runtime thread,
    // spawn from there, then await on the runtime.
    let rt = Runtime::with_workers(2);
    let off_thread_handle = rt.handle();
    let join = thread::spawn(move || off_thread_handle.spawn(async { 17_u32 }));
    let task = join.join().expect("off-runtime thread panicked");
    let v = rt.block_on(task);
    assert_eq!(v, 17);
}

#[test]
#[should_panic(expected = "outside a nanorun runtime worker thread")]
fn handle_current_outside_runtime_panics() {
    // Main test thread is not a nanorun worker; `Handle::current()` panics.
    let _ = Handle::current();
}

#[test]
fn block_on_inside_worker_panics() {
    use std::any::Any;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    // The spawned task catches its own panic so the worker thread does
    // not unwind out of `run_worker`; the catch result rides out via the
    // JoinHandle output.
    let rt = Arc::new(Runtime::with_workers(1));
    let rt_for_task = Arc::clone(&rt);

    let h = rt.spawn(async move {
        catch_unwind(AssertUnwindSafe(|| {
            rt_for_task.block_on(async { 0_u32 });
        }))
    });

    let result: Result<(), Box<dyn Any + Send>> = rt.block_on(h);
    let payload = result.expect_err("Runtime::block_on inside a worker should panic");
    let msg = if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        String::from("<unknown panic payload>")
    };
    assert!(
        msg.contains("block_on") && msg.contains("worker"),
        "expected block_on/worker panic message, got: {msg}",
    );
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
        Wait { slot, signal }
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
    let v1 = rt1.block_on(h1);
    let v2 = rt2.block_on(h2);
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
        rt.spawn(YieldOnce(false));
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
    let rt = Runtime::with_workers(4);
    let mut handles = Vec::with_capacity(50);
    for _ in 0..50 {
        handles.push(rt.spawn(async {
            for _ in 0..20 {
                nanorun::yield_now().await;
            }
            0_usize
        }));
    }
    rt.block_on(async move {
        for h in handles {
            assert_eq!(h.await, 0);
        }
    });
}
