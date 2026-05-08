//! Executor: drives futures to completion.
//!
//! The `single` submodule hosts the single-threaded driver (M1). The
//! `multi` submodule reserves the slot for the multi-worker driver (M3).

mod multi;
mod single;

use core::future::Future;

/// Drive a future to completion on the current thread.
///
/// # Example
///
/// ```
/// let value = nanorun::block_on(async { 1 + 2 });
/// assert_eq!(value, 3);
/// ```
pub fn block_on<F: Future>(f: F) -> F::Output {
    single::run(f)
}
