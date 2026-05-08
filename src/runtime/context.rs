//! Thread-local runtime context: current reactor, current [`Handle`],
//! and a worker-thread marker.
//!
//! Worker threads install three RAII guards on entry: a [`WorkerMarker`]
//! flag (so [`Runtime::block_on`](crate::Runtime::block_on) can detect
//! and reject nested calls), a [`HandleGuard`], and a reactor [`Guard`].
//! User-side I/O types call [`with_current`] to register interest with
//! the running reactor; user code spawning from inside a task calls
//! [`crate::Handle::current`], which reads the handle slot. Each guard's
//! lifetime brackets its borrow, so the values stored in the
//! thread-locals are always live when read.

use core::cell::{Cell, RefCell};
use core::marker::PhantomData;
use core::ptr::NonNull;

use crate::executor::Handle;
use crate::reactor::Reactor;

thread_local! {
    static CTX: Cell<Option<NonNull<Reactor>>> = const { Cell::new(None) };
    static HANDLE: RefCell<Option<Handle>> = const { RefCell::new(None) };
    static IS_WORKER: Cell<bool> = const { Cell::new(false) };
}

/// RAII guard that installs `&'a Reactor` into the current thread's slot.
pub(crate) struct Guard<'a> {
    _borrow: PhantomData<&'a Reactor>,
}

impl<'a> Guard<'a> {
    /// Install `reactor` for the duration of the guard. Panics on nested install.
    pub(crate) fn install(reactor: &'a Reactor) -> Self {
        CTX.with(|c| {
            assert!(
                c.get().is_none(),
                "nanorun runtime already installed on this thread; nested block_on is unsupported",
            );
            c.set(Some(NonNull::from(reactor)));
        });
        Self {
            _borrow: PhantomData,
        }
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        CTX.with(|c| c.set(None));
    }
}

/// Run `f` against the reactor installed on this thread.
///
/// Panics if no [`Guard`] is currently installed.
#[allow(dead_code)] // wired by Phase 4 (TcpStream)
pub(crate) fn with_current<R>(f: impl FnOnce(&Reactor) -> R) -> R {
    try_with_current(f).expect(
        "no nanorun runtime installed on this thread; \
         call this from inside Runtime::block_on or nanorun::block_on",
    )
}

/// Run `f` against the reactor installed on this thread, if any.
///
/// Returns `None` when no [`Guard`] is currently installed — used by
/// [`Drop`] paths that may execute after `block_on` has already returned.
#[allow(dead_code)] // wired by Phase 4 (TcpStream::Drop)
pub(crate) fn try_with_current<R>(f: impl FnOnce(&Reactor) -> R) -> Option<R> {
    let ptr = CTX.with(Cell::get)?;
    // SAFETY: the live Guard whose `install` set this slot also holds an
    // active borrow of the same Reactor; the borrow checker therefore
    // prevents the Reactor from being dropped or moved while the slot is
    // populated. The reference handed to `f` cannot outlive this call.
    Some(unsafe { f(ptr.as_ref()) })
}

/// RAII guard installing a [`Handle`] into the current thread's slot.
pub(crate) struct HandleGuard {
    _priv: (),
}

impl HandleGuard {
    /// Install `handle` for the duration of the guard. Panics on nested install.
    pub(crate) fn install(handle: Handle) -> Self {
        HANDLE.with(|cell| {
            let mut slot = cell.borrow_mut();
            assert!(
                slot.is_none(),
                "nanorun handle already installed on this thread",
            );
            *slot = Some(handle);
        });
        Self { _priv: () }
    }
}

impl Drop for HandleGuard {
    fn drop(&mut self) {
        HANDLE.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }
}

/// Return a clone of the current thread's [`Handle`], if any is installed.
pub(crate) fn current_handle() -> Option<Handle> {
    HANDLE.with(|cell| cell.borrow().as_ref().cloned())
}

/// RAII guard that flags the current thread as a runtime worker.
pub(crate) struct WorkerMarker {
    _priv: (),
}

impl WorkerMarker {
    /// Mark the current thread as a worker for the duration of the guard. Panics on nested install.
    pub(crate) fn enter() -> Self {
        IS_WORKER.with(|cell| {
            assert!(
                !cell.get(),
                "nanorun WorkerMarker already installed on this thread",
            );
            cell.set(true);
        });
        Self { _priv: () }
    }
}

impl Drop for WorkerMarker {
    fn drop(&mut self) {
        IS_WORKER.with(|cell| cell.set(false));
    }
}

/// Return `true` if the current thread is inside an active [`WorkerMarker`] bracket.
pub(crate) fn is_worker() -> bool {
    IS_WORKER.with(Cell::get)
}
