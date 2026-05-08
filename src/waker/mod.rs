//! Waker plumbing: hand-rolled `RawWakerVTable` over a thread handle.
//!
//! The single-threaded executor parks its own thread when the future
//! returns `Pending`. The waker therefore needs to do exactly one thing
//! when fired: unpark that thread. The data pointer of the [`RawWaker`]
//! is an `Arc<Thread>` round-tripped through `Arc::into_raw`.
//!
//! When M3 introduces multi-threaded scheduling and `spawn`, this module
//! gets replaced by a `Schedule`-trait-driven waker that targets a task
//! header instead of a thread.

use core::task::{RawWaker, RawWakerVTable, Waker};
use std::sync::Arc;
use std::thread::Thread;

/// Build a [`Waker`] that unparks `thread` when fired.
pub(crate) fn waker_for(thread: Thread) -> Waker {
    let arc = Arc::new(thread);
    let raw = RawWaker::new(Arc::into_raw(arc).cast::<()>(), &VTABLE);
    // SAFETY: `raw` was just built from a vtable whose contracts are
    // upheld by `clone_raw`, `wake_raw`, `wake_by_ref_raw`, and
    // `drop_raw` below.
    unsafe { Waker::from_raw(raw) }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, wake_raw, wake_by_ref_raw, drop_raw);

unsafe fn clone_raw(data: *const ()) -> RawWaker {
    // SAFETY: `data` originates from `Arc::<Thread>::into_raw` per the
    // module invariant; `increment_strong_count` is the documented way
    // to clone without round-tripping through `Arc::from_raw`.
    unsafe { Arc::<Thread>::increment_strong_count(data.cast::<Thread>()) };
    RawWaker::new(data, &VTABLE)
}

unsafe fn wake_raw(data: *const ()) {
    // SAFETY: `data` originates from `Arc::<Thread>::into_raw`; we are
    // taking back the strong count that the producing `into_raw` left
    // behind (or that `clone_raw` added).
    let arc = unsafe { Arc::<Thread>::from_raw(data.cast::<Thread>()) };
    arc.unpark();
}

unsafe fn wake_by_ref_raw(data: *const ()) {
    // SAFETY: `data` originates from `Arc::<Thread>::into_raw`; we cast
    // it to a shared reference for the duration of the `unpark` call
    // and never drop the underlying allocation.
    let thread = unsafe { &*data.cast::<Thread>() };
    thread.unpark();
}

unsafe fn drop_raw(data: *const ()) {
    // SAFETY: `data` originates from `Arc::<Thread>::into_raw`; reclaim
    // the strong count and let the `Arc` drop run.
    drop(unsafe { Arc::<Thread>::from_raw(data.cast::<Thread>()) });
}
