//! User-facing entry point: a runtime composes executor, reactor, and timers.
//!
//! Most users construct a [`Runtime`] and call [`Runtime::block_on`].
//! The free function [`crate::executor::block_on`] is the lower-level
//! escape hatch and uses the same per-call reactor.

pub(crate) mod context;

use core::future::Future;

/// Composed runtime: executor + reactor + timer wheel.
pub struct Runtime;

impl Runtime {
    /// Construct a runtime with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Drive `f` to completion on this runtime.
    ///
    /// # Example
    ///
    /// ```
    /// use nanorun::Runtime;
    ///
    /// let rt = Runtime::new();
    /// let value = rt.block_on(async { 40 + 2 });
    /// assert_eq!(value, 42);
    /// ```
    pub fn block_on<F: Future>(&self, f: F) -> F::Output {
        crate::executor::block_on(f)
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}
