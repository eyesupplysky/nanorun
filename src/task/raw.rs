//! Raw task layout: header, future storage, and vtable.
//!
//! A spawned task is a single heap allocation laid out as
//! `RawTask<F, S>` with `#[repr(C)]` and `Header` at offset 0. The
//! header carries the state bits, the per-monomorphization vtable, and
//! the `JoinHandle` waker slot. The future and the schedule callback
//! live alongside it. The runtime, the worker queues, and the
//! `RawWaker` data pointer all hold a thin [`*const Header`] and recover
//! the typed allocation through the vtable.
//!
//! Reference counting rides on `Arc<RawTask<F, S>>`. The vtable's
//! [`Vtable::clone_ref`] / [`Vtable::drop_ref`] entries call
//! `Arc::increment_strong_count` / `Arc::decrement_strong_count` against
//! the typed pointer recovered from the thin header pointer, so the
//! code that holds erased refs (worker queues, wakers) does not need
//! to know `F` or `S` to bump or release refs.
//!
//! # Lifecycle
//!
//! State bits in [`Header::state`] form a small state machine. A task
//! is born `NOTIFIED` (it was just spawned, so it is in some run
//! queue). The worker that pops it transitions `NOTIFIED → RUNNING`
//! and clears `NOTIFIED` before polling. Wakes that fire while the
//! task is `RUNNING` re-set `NOTIFIED` but do not enqueue; when poll
//! returns `Pending` the worker checks `NOTIFIED` and reschedules
//! itself. When poll returns `Ready` the worker writes the output into
//! the [`Stage`] cell, sets `COMPLETE`, and wakes the `JoinHandle` if
//! one is parked.

use core::cell::UnsafeCell;
use core::future::Future;
use core::mem::{self, ManuallyDrop, MaybeUninit};
use core::pin::Pin;
use core::ptr::NonNull;
use core::task::{Context, Poll, Waker};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Task is queued for poll. Set on spawn and on every wake from idle.
pub(crate) const NOTIFIED: u64 = 1 << 0;
/// A worker is currently polling this task. Mutually exclusive with NOTIFIED+queued.
pub(crate) const RUNNING: u64 = 1 << 1;
/// Future has produced its output and the cell holds `Stage::Ready` (or `Stage::Empty` if dropped).
pub(crate) const COMPLETE: u64 = 1 << 2;
/// A `JoinHandle` is alive and may want the output. Cleared on `JoinHandle` drop.
pub(crate) const JOIN_INTEREST: u64 = 1 << 3;
/// `join_waker` slot is occupied. Set by `JoinHandle::poll`, cleared by the completing worker.
pub(crate) const JOIN_WAKER: u64 = 1 << 4;

/// Per-monomorphization function table that lets erased `Header` pointers reach typed task ops.
pub(crate) struct Vtable {
    /// Poll the future. Consumes one strong ref.
    pub(crate) poll: unsafe fn(NonNull<Header>),
    /// Notify and (if previously idle) schedule. Consumes one strong ref.
    pub(crate) schedule: unsafe fn(NonNull<Header>),
    /// Bump the strong refcount.
    pub(crate) clone_ref: unsafe fn(NonNull<Header>),
    /// Decrement and possibly free.
    pub(crate) drop_ref: unsafe fn(NonNull<Header>),
    /// If `COMPLETE`, move the output into `dst` and return true. Caller must size `dst` for `F::Output`.
    pub(crate) try_take_output: unsafe fn(NonNull<Header>, *mut ()) -> bool,
    /// `JoinHandle` is being dropped. Clear `JOIN_INTEREST`; if already `COMPLETE`, drop the held output.
    pub(crate) drop_join_interest: unsafe fn(NonNull<Header>),
}

/// Type-erased task header. Always at offset 0 of the task allocation.
#[repr(C)]
pub(crate) struct Header {
    pub(crate) state: AtomicU64,
    pub(crate) vtable: &'static Vtable,
    pub(crate) join_waker: Mutex<Option<Waker>>,
}

/// Pluggable scheduler for `spawn`. Implementors push the task into a run queue.
pub(crate) trait Schedule: Send + Sync + 'static {
    /// Enqueue `task` for polling. Called from waker fires.
    fn schedule(&self, task: TaskRef);
}

enum Stage<F: Future> {
    Pending(F),
    Ready(F::Output),
    Empty,
}

#[repr(C)]
struct RawTask<F: Future, S: Schedule> {
    header: Header,
    schedule: S,
    cell: UnsafeCell<Stage<F>>,
}

// SAFETY: the cell is serialized by the `RUNNING`/`COMPLETE` state bits;
// see invariant on `Header::state`.
unsafe impl<F, S> Send for RawTask<F, S>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
}
// SAFETY: same as `Send` — concurrent access is serialized by the state bits.
unsafe impl<F, S> Sync for RawTask<F, S>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
}

/// Type-erased ref-counted handle to a task. Equivalent to `Arc<RawTask<F, S>>`
/// with the `(F, S)` parameters erased through the header's vtable.
pub(crate) struct TaskRef {
    ptr: NonNull<Header>,
}

// SAFETY: the underlying allocation is `Send + Sync` (see RawTask impls);
// the ref itself is just a refcounted pointer.
unsafe impl Send for TaskRef {}
// SAFETY: see Send impl.
unsafe impl Sync for TaskRef {}

impl TaskRef {
    /// Borrow the header without affecting the refcount.
    pub(crate) fn header(&self) -> &Header {
        // SAFETY: the invariant on `ptr` guarantees a live allocation.
        unsafe { self.ptr.as_ref() }
    }

    /// Consume this ref and poll the task. The vtable takes ownership of the strong count.
    pub(crate) fn poll(self) {
        let ptr = self.ptr;
        let vtable = self.header().vtable;
        mem::forget(self);
        // SAFETY: `ptr` was a valid header ref that we just transferred ownership of.
        unsafe {
            (vtable.poll)(ptr);
        }
    }

    /// Consume into a raw header pointer without dropping the ref. The
    /// caller is responsible for either re-binding it as a `TaskRef`
    /// (e.g. inside the waker vtable) or releasing it via the header's
    /// `drop_ref`.
    pub(crate) fn into_raw(self) -> NonNull<Header> {
        let ptr = self.ptr;
        mem::forget(self);
        ptr
    }
}

impl Clone for TaskRef {
    fn clone(&self) -> Self {
        // SAFETY: `ptr` is live; vtable bumps the strong count.
        unsafe {
            (self.header().vtable.clone_ref)(self.ptr);
        }
        Self { ptr: self.ptr }
    }
}

impl Drop for TaskRef {
    fn drop(&mut self) {
        // SAFETY: `ptr` is live; vtable releases one strong count.
        unsafe {
            (self.header().vtable.drop_ref)(self.ptr);
        }
    }
}

/// Allocate a fresh task and return its run-queue ref and join ref.
pub(crate) fn spawn_raw<F, S>(future: F, schedule: S) -> (TaskRef, TaskRef)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    let task = Arc::new(RawTask::<F, S> {
        header: Header {
            state: AtomicU64::new(NOTIFIED | JOIN_INTEREST),
            vtable: vtable::<F, S>(),
            join_waker: Mutex::new(None),
        },
        schedule,
        cell: UnsafeCell::new(Stage::Pending(future)),
    });

    // Bump to refcount 2: one for the run-queue, one for the JoinHandle.
    let ptr = Arc::into_raw(task);
    // SAFETY: `ptr` is a fresh `Arc::into_raw` pointer; bumping the count
    // is the documented way to clone without round-tripping.
    unsafe {
        Arc::<RawTask<F, S>>::increment_strong_count(ptr);
    }

    let header_ptr =
        // SAFETY: `RawTask<F, S>` is `#[repr(C)]` with `header: Header` first;
        // the cast yields a valid header pointer.
        unsafe { NonNull::new_unchecked(ptr as *mut Header) };

    let queue_ref = TaskRef { ptr: header_ptr };
    let join_ref = TaskRef { ptr: header_ptr };
    (queue_ref, join_ref)
}

const fn vtable<F, S>() -> &'static Vtable
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    &Vtable {
        poll: poll_fn::<F, S>,
        schedule: schedule_fn::<F, S>,
        clone_ref: clone_ref_fn::<F, S>,
        drop_ref: drop_ref_fn::<F, S>,
        try_take_output: try_take_output_fn::<F, S>,
        drop_join_interest: drop_join_interest_fn::<F, S>,
    }
}

unsafe fn typed<F: Future, S: Schedule>(ptr: NonNull<Header>) -> *const RawTask<F, S> {
    // SAFETY: the invariant on the vtable callers guarantees `ptr` points
    // at the header of a `RawTask<F, S>` allocation; with `#[repr(C)]`
    // the header lives at offset 0.
    ptr.as_ptr().cast::<RawTask<F, S>>()
}

// `state` (header bits) and `stage` (output cell) are intentionally distinct names for distinct concepts.
#[allow(clippy::similar_names)]
unsafe fn poll_fn<F, S>(ptr: NonNull<Header>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: caller transferred one strong ref to us via the vtable contract.
    let arc = unsafe { Arc::<RawTask<F, S>>::from_raw(typed::<F, S>(ptr)) };
    let header = &arc.header;

    // Acquire the RUNNING bit, clear NOTIFIED. Bail if another worker
    // already holds RUNNING (should be unreachable under correct
    // scheduling) or if the task is already COMPLETE.
    let mut state = header.state.load(Ordering::Acquire);
    loop {
        if state & RUNNING != 0 || state & COMPLETE != 0 {
            return;
        }
        let new = (state | RUNNING) & !NOTIFIED;
        match header
            .state
            .compare_exchange_weak(state, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => break,
            Err(actual) => state = actual,
        }
    }

    // Build a waker that holds its own ref. Bump the count on behalf
    // of the waker, then construct the TaskRef from the bumped ref.
    // SAFETY: we already hold a ref via `arc`; bumping is sound.
    unsafe {
        Arc::<RawTask<F, S>>::increment_strong_count(typed::<F, S>(ptr));
    }
    let waker = crate::waker::waker_for_task(TaskRef { ptr });
    let mut cx = Context::from_waker(&waker);

    // Poll the future. The cell is touched only here while RUNNING is held.
    // SAFETY: the `RUNNING` bit serializes access to the cell.
    let result = unsafe {
        let stage = &mut *arc.cell.get();
        match stage {
            Stage::Pending(fut) => {
                // SAFETY: the future is heap-pinned for the rest of its life.
                Pin::new_unchecked(fut).poll(&mut cx)
            }
            Stage::Ready(_) | Stage::Empty => unreachable!("polled task with no pending future"),
        }
    };

    match result {
        Poll::Ready(out) => {
            // Move the output in. Cell is exclusive (we hold RUNNING).
            // SAFETY: see above.
            unsafe {
                *arc.cell.get() = Stage::Ready(out);
            }

            // Set COMPLETE, clear RUNNING. Read JOIN bits in the prior state.
            let prev = header.state.fetch_or(COMPLETE, Ordering::AcqRel);
            let merged = prev | COMPLETE;
            header.state.fetch_and(!RUNNING, Ordering::Release);

            if merged & JOIN_INTEREST == 0 {
                // No JoinHandle alive — drop the output now.
                // SAFETY: we still hold the only access to the cell:
                // RUNNING was just cleared but COMPLETE blocks future polls,
                // and JOIN_INTEREST=0 means no JoinHandle::poll path either.
                unsafe {
                    *arc.cell.get() = Stage::Empty;
                }
            } else if merged & JOIN_WAKER != 0 {
                let waker = header
                    .join_waker
                    .lock()
                    .expect("join waker poisoned")
                    .take();
                if let Some(w) = waker {
                    w.wake();
                }
            }
        }
        Poll::Pending => {
            // Clear RUNNING. If NOTIFIED was set during poll, reschedule.
            let prev = header.state.fetch_and(!RUNNING, Ordering::AcqRel);
            if prev & NOTIFIED != 0 {
                // Bump ref for the reschedule and hand it to the scheduler.
                // SAFETY: we still hold a ref; bumping is sound.
                unsafe {
                    Arc::<RawTask<F, S>>::increment_strong_count(typed::<F, S>(ptr));
                }
                arc.schedule.schedule(TaskRef { ptr });
            }
        }
    }
    // arc drops, releasing the poll-call's ref.
}

unsafe fn schedule_fn<F, S>(ptr: NonNull<Header>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: caller transferred one strong ref via the vtable contract.
    let arc = unsafe { Arc::<RawTask<F, S>>::from_raw(typed::<F, S>(ptr)) };
    let header = &arc.header;

    let prev = header.state.fetch_or(NOTIFIED, Ordering::AcqRel);
    if prev & (RUNNING | NOTIFIED | COMPLETE) == 0 {
        // Was idle: schedule. Forget the arc to keep the ref alive for the queue.
        let arc = ManuallyDrop::new(arc);
        arc.schedule.schedule(TaskRef { ptr });
    }
    // else: arc drops, releasing the ref.
}

unsafe fn clone_ref_fn<F, S>(ptr: NonNull<Header>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: caller asserts a live allocation.
    unsafe {
        Arc::<RawTask<F, S>>::increment_strong_count(typed::<F, S>(ptr));
    }
}

unsafe fn drop_ref_fn<F, S>(ptr: NonNull<Header>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: caller asserts a live allocation we own a ref to.
    unsafe {
        Arc::<RawTask<F, S>>::decrement_strong_count(typed::<F, S>(ptr));
    }
}

unsafe fn try_take_output_fn<F, S>(ptr: NonNull<Header>, dst: *mut ()) -> bool
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: caller asserts a live allocation; we only borrow.
    let task = unsafe { &*typed::<F, S>(ptr) };

    if task.header.state.load(Ordering::Acquire) & COMPLETE == 0 {
        return false;
    }

    // SAFETY: COMPLETE is set; the cell write happens-before our load.
    // No other thread will touch the cell while COMPLETE is set and the
    // sole `JoinHandle` is the only caller of this fn.
    let stage = unsafe { &mut *task.cell.get() };
    match mem::replace(stage, Stage::Empty) {
        Stage::Ready(out) => {
            // SAFETY: caller sized `dst` for `F::Output`.
            unsafe {
                std::ptr::write(dst.cast::<F::Output>(), out);
            }
            true
        }
        Stage::Pending(_) | Stage::Empty => false,
    }
}

unsafe fn drop_join_interest_fn<F, S>(ptr: NonNull<Header>)
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
{
    // SAFETY: caller asserts a live allocation; we only borrow.
    let task = unsafe { &*typed::<F, S>(ptr) };

    let prev = task
        .header
        .state
        .fetch_and(!JOIN_INTEREST, Ordering::AcqRel);
    if prev & COMPLETE != 0 {
        // Output sitting in the cell needs to drop now.
        // SAFETY: COMPLETE is set, no worker touches the cell, and we
        // are the sole JoinHandle.
        unsafe {
            *task.cell.get() = Stage::Empty;
        }
    }
}

/// Helper used by `JoinHandle::poll` to extract the output via the vtable.
///
/// Returns `Some(out)` when the future has completed and the output had
/// not yet been taken, `None` otherwise.
pub(crate) fn try_take_output<T>(header: &Header) -> Option<T> {
    let mut slot: MaybeUninit<T> = MaybeUninit::uninit();
    // SAFETY: the vtable's `try_take_output` writes a `T` into `slot`
    // exactly when it returns true; the header is borrowed for the call.
    let ok = unsafe {
        let ptr = NonNull::from(header);
        (header.vtable.try_take_output)(ptr, slot.as_mut_ptr().cast::<()>())
    };
    if ok {
        // SAFETY: the vtable initialized `slot` on success.
        Some(unsafe { slot.assume_init() })
    } else {
        None
    }
}

/// Helper used by `JoinHandle::drop` to release join interest before the ref drops.
pub(crate) fn drop_join_interest(header: &Header) {
    // SAFETY: header is borrowed; vtable contract is upheld.
    unsafe {
        let ptr = NonNull::from(header);
        (header.vtable.drop_join_interest)(ptr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex as StdMutex;
    use std::task::Wake;

    use crate::task::JoinHandle;

    /// A scheduler that pushes notified tasks onto a shared `Vec`.
    /// Tests drive the queue by popping and calling `TaskRef::poll`.
    #[derive(Clone)]
    struct TestSchedule(Arc<StdMutex<Vec<TaskRef>>>);

    impl Schedule for TestSchedule {
        fn schedule(&self, task: TaskRef) {
            self.0.lock().expect("test queue poisoned").push(task);
        }
    }

    fn drain(queue: &StdMutex<Vec<TaskRef>>) {
        loop {
            let next = queue.lock().expect("test queue poisoned").pop();
            match next {
                Some(t) => t.poll(),
                None => return,
            }
        }
    }

    /// Counts how many times its `wake` is called.
    struct CountWaker(AtomicUsize);
    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn ready_future_completes_in_one_poll() {
        let queue = Arc::new(StdMutex::new(Vec::<TaskRef>::new()));
        let sched = TestSchedule(queue.clone());

        let (queued, join) = spawn_raw(async { 42_u32 }, sched);
        queue.lock().expect("queue").push(queued);

        drain(&queue);

        let header = join.header();
        assert!(header.state.load(Ordering::Acquire) & COMPLETE != 0);
        assert_eq!(try_take_output::<u32>(header), Some(42));
    }

    #[test]
    fn pending_then_self_wake_reschedules() {
        struct YieldOnce(bool);
        impl Future for YieldOnce {
            type Output = u32;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
                if self.0 {
                    Poll::Ready(7)
                } else {
                    self.0 = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }

        let queue = Arc::new(StdMutex::new(Vec::<TaskRef>::new()));
        let sched = TestSchedule(queue.clone());

        let (queued, join) = spawn_raw(YieldOnce(false), sched);
        queue.lock().expect("queue").push(queued);

        drain(&queue);

        assert!(join.header().state.load(Ordering::Acquire) & COMPLETE != 0);
        assert_eq!(try_take_output::<u32>(join.header()), Some(7));
    }

    #[test]
    fn join_handle_delivers_output_via_waker() {
        let queue = Arc::new(StdMutex::new(Vec::<TaskRef>::new()));
        let sched = TestSchedule(queue.clone());

        let (queued, join_ref) = spawn_raw(async { 99_u32 }, sched);
        queue.lock().expect("queue").push(queued);

        let mut handle: JoinHandle<u32> = JoinHandle::new(join_ref);

        let count = Arc::new(CountWaker(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);

        // Task hasn't run yet — should park.
        match Pin::new(&mut handle).poll(&mut cx) {
            Poll::Pending => {}
            Poll::Ready(_) => panic!("unexpected early completion"),
        }

        drain(&queue);
        assert!(
            count.0.load(Ordering::SeqCst) >= 1,
            "join waker fires once on completion"
        );

        match Pin::new(&mut handle).poll(&mut cx) {
            Poll::Ready(v) => assert_eq!(v, 99),
            Poll::Pending => panic!("expected ready after drain"),
        }
    }

    #[test]
    fn dropping_join_handle_detaches_task() {
        let queue = Arc::new(StdMutex::new(Vec::<TaskRef>::new()));
        let sched = TestSchedule(queue.clone());

        let (queued, join_ref) = spawn_raw(async { 1_u32 }, sched);
        queue.lock().expect("queue").push(queued);

        let handle: JoinHandle<u32> = JoinHandle::new(join_ref);
        drop(handle);

        // Drain — task should still complete, output should be dropped.
        drain(&queue);
        // No assertion on output: with JOIN_INTEREST cleared, the cell
        // is empty. The point is no leak, no panic — exercised by the
        // refcount machinery dropping the allocation.
    }
}
