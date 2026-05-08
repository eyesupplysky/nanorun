//! Waker plumbing: hand-rolled `RawWakerVTable` over a `ReactorHandle`.
//!
//! When fired, the waker pokes the reactor's cross-thread wake channel
//! (an eventfd on Linux, a condvar elsewhere). The executor blocked in
//! `Reactor::poll` returns and the future is polled again. The data
//! pointer of the [`RawWaker`] is an `Arc<ReactorHandle>` round-tripped
//! through `Arc::into_raw`.
//!
//! When M3 introduces multi-threaded scheduling and `spawn`, this module
//! gets replaced by a `Schedule`-trait-driven waker that targets a task
//! header instead of the executor thread.

use core::task::{RawWaker, RawWakerVTable, Waker};
use std::sync::Arc;

use crate::reactor::ReactorHandle;

/// Build a [`Waker`] that wakes `handle`'s reactor when fired.
pub(crate) fn waker_for(handle: ReactorHandle) -> Waker {
    let arc = Arc::new(handle);
    let raw = RawWaker::new(Arc::into_raw(arc).cast::<()>(), &VTABLE);
    // SAFETY: `raw` was just built from a vtable whose contracts are
    // upheld by `clone_raw`, `wake_raw`, `wake_by_ref_raw`, and
    // `drop_raw` below.
    unsafe { Waker::from_raw(raw) }
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, wake_raw, wake_by_ref_raw, drop_raw);

unsafe fn clone_raw(data: *const ()) -> RawWaker {
    // SAFETY: `data` originates from `Arc::<ReactorHandle>::into_raw` per
    // the module invariant; `increment_strong_count` is the documented
    // way to clone without round-tripping through `Arc::from_raw`.
    unsafe {
        Arc::<ReactorHandle>::increment_strong_count(data.cast::<ReactorHandle>());
    }
    RawWaker::new(data, &VTABLE)
}

unsafe fn wake_raw(data: *const ()) {
    // SAFETY: `data` originates from `Arc::<ReactorHandle>::into_raw`;
    // we are taking back the strong count that the producing `into_raw`
    // left behind (or that `clone_raw` added).
    let arc = unsafe { Arc::<ReactorHandle>::from_raw(data.cast::<ReactorHandle>()) };
    arc.wake().expect("reactor wake");
}

unsafe fn wake_by_ref_raw(data: *const ()) {
    // SAFETY: `data` originates from `Arc::<ReactorHandle>::into_raw`; we
    // cast it to a shared reference for the duration of the `wake` call
    // and never drop the underlying allocation.
    let handle = unsafe { &*data.cast::<ReactorHandle>() };
    handle.wake().expect("reactor wake");
}

unsafe fn drop_raw(data: *const ()) {
    // SAFETY: `data` originates from `Arc::<ReactorHandle>::into_raw`;
    // reclaim the strong count and let the `Arc` drop run.
    drop(unsafe { Arc::<ReactorHandle>::from_raw(data.cast::<ReactorHandle>()) });
}
