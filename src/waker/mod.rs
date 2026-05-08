//! Waker plumbing: hand-rolled `RawWakerVTable` over a task header pointer.
//!
//! The data pointer of every [`Waker`] this crate produces is a thin
//! `*const Header` originating from [`TaskRef::into_raw`]. Cloning the
//! waker bumps the task's strong refcount via the header's vtable;
//! `wake()` invokes the same vtable's `schedule` entry, which sets the
//! task's `NOTIFIED` bit and (if previously idle) pushes the task back
//! onto a runqueue.

use core::ptr::NonNull;
use core::task::{RawWaker, RawWakerVTable, Waker};

use crate::task::raw::{Header, TaskRef};

/// Build a [`Waker`] whose `wake()` re-schedules `task`.
pub(crate) fn waker_for_task(task: TaskRef) -> Waker {
    let ptr = task.into_raw();
    let raw = RawWaker::new(ptr.as_ptr().cast::<()>(), &TASK_VTABLE);
    // SAFETY: the task vtable upholds the `RawWakerVTable` contracts;
    // see invariants on the four fns below.
    unsafe { Waker::from_raw(raw) }
}

static TASK_VTABLE: RawWakerVTable =
    RawWakerVTable::new(task_clone, task_wake, task_wake_by_ref, task_drop);

unsafe fn task_clone(data: *const ()) -> RawWaker {
    // SAFETY: the module invariant on `waker_for_task` guarantees `data`
    // is a header pointer; the vtable's `clone_ref` bumps the count.
    let ptr = unsafe { NonNull::new_unchecked(data.cast::<Header>().cast_mut()) };
    // SAFETY: same as above.
    unsafe {
        (ptr.as_ref().vtable.clone_ref)(ptr);
    }
    RawWaker::new(data, &TASK_VTABLE)
}

unsafe fn task_wake(data: *const ()) {
    let ptr = unsafe { NonNull::new_unchecked(data.cast::<Header>().cast_mut()) };
    // SAFETY: invariant on `waker_for_task`; schedule consumes the ref.
    unsafe {
        (ptr.as_ref().vtable.schedule)(ptr);
    }
}

unsafe fn task_wake_by_ref(data: *const ()) {
    let ptr = unsafe { NonNull::new_unchecked(data.cast::<Header>().cast_mut()) };
    // SAFETY: bump the count, then hand the bumped ref to `schedule`.
    unsafe {
        (ptr.as_ref().vtable.clone_ref)(ptr);
        (ptr.as_ref().vtable.schedule)(ptr);
    }
}

unsafe fn task_drop(data: *const ()) {
    let ptr = unsafe { NonNull::new_unchecked(data.cast::<Header>().cast_mut()) };
    // SAFETY: reclaim the strong count via the vtable.
    unsafe {
        (ptr.as_ref().vtable.drop_ref)(ptr);
    }
}
