//! Timers: `sleep`, `timeout`, hierarchical wheel dispatch.
//!
//! The `wheel` submodule holds the timer-wheel implementation (M4). The
//! public surface here is what user code awaits.

mod wheel;

use core::future::{pending, Future};
use core::time::Duration;

/// Sleep for at least `_dur` before resolving.
pub fn sleep(_dur: Duration) -> impl Future<Output = ()> {
    pending::<()>()
}
