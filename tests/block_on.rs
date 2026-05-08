//! Integration tests for the entry-point future drivers.
//!
//! These tests link against `nanorun` as an external user would. They
//! cover: the no-park ready path, the cross-thread wake path, ordinary
//! async syntax, and the `Runtime::block_on` passthrough contract.
//! Since M3, both the free `block_on` fn and `Runtime::block_on` route
//! through the multi-worker executor.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

/// A future that resolves to `42` on the first poll without ever returning `Pending`.
struct Now;

impl core::future::Future for Now {
    type Output = u32;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(42)
    }
}

#[test]
fn ready_immediately_does_not_park() {
    let value = nanorun::block_on(Now);
    assert_eq!(value, 42);
}

/// A future that returns `Pending` once, parks the waker into a shared
/// slot, then resolves on the next poll.
struct WakeOnce {
    polled: AtomicBool,
    slot: Arc<Mutex<Option<Waker>>>,
}

impl core::future::Future for WakeOnce {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.polled.swap(true, Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            *self.slot.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[test]
fn pending_then_woken_by_other_thread() {
    let slot: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
    let slot_for_waker = Arc::clone(&slot);

    let sleep_ms = 20;
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(sleep_ms));
        if let Some(waker) = slot_for_waker.lock().unwrap().take() {
            waker.wake();
            return;
        }
    });

    let started = Instant::now();
    nanorun::block_on(WakeOnce {
        polled: AtomicBool::new(false),
        slot,
    });

    // Loose lower bound: we must have actually parked at least once,
    // otherwise the executor was busy-looping.
    assert!(
        started.elapsed() >= Duration::from_millis(sleep_ms),
        "block_on returned in {:?}, expected at least {}ms",
        started.elapsed(),
        sleep_ms
    );
}

#[test]
fn async_block_returns_value() {
    let sum = nanorun::block_on(async { 1 + 2 });
    assert_eq!(sum, 3);
}

#[test]
fn runtime_block_on_matches_free_function() {
    let rt = nanorun::Runtime::new();
    let via_runtime = rt.block_on(async { "ok" });
    let via_free = nanorun::block_on(async { "ok" });
    assert_eq!(via_runtime, via_free);
}
