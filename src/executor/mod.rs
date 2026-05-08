//! Executor: drives futures to completion.
//!
//! The [`multi`] submodule is the multi-worker driver that owns a
//! shared reactor and round-robins runnable tasks across worker
//! threads. The free function [`block_on`] wraps a one-shot
//! single-worker [`crate::Runtime`] for users who just want to drive a
//! future to completion without explicitly managing the runtime.

pub(crate) mod multi;

use core::future::Future;

/// Drive a future to completion on a one-shot single-worker runtime.
///
/// # Example
///
/// ```
/// let value = nanorun::block_on(async { 1 + 2 });
/// assert_eq!(value, 3);
/// ```
pub fn block_on<F>(f: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    crate::runtime::Runtime::with_workers(1).block_on(f)
}
