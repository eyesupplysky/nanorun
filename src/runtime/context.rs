//! Thread-local "current reactor" plumbing.
//!
//! [`crate::executor::single::run`] installs a [`Guard`] before polling
//! the user future; user-side I/O types (the M2 [`crate::net`] module on
//! Linux) call [`with_current`] to register interest with the running
//! reactor. The guard's lifetime brackets the borrow, so the raw pointer
//! stored in the thread-local is always live when read.

use core::cell::Cell;
use core::marker::PhantomData;
use core::ptr::NonNull;

use crate::reactor::Reactor;

thread_local! {
    static CTX: Cell<Option<NonNull<Reactor>>> = const { Cell::new(None) };
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
