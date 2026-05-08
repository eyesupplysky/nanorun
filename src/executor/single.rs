//! Single-threaded executor driver (M1 target).
//!
//! Polls a single future on the calling thread, parking when the future
//! returns `Pending` and waking via the hand-rolled vtable in
//! [`crate::waker`].

use core::future::Future;

/// Drive a future to completion on the calling thread.
pub(crate) fn run<F: Future>(_f: F) -> F::Output {
    unimplemented!("M1: single-threaded executor driver")
}
