//! Timers: `sleep`, `timeout`, hierarchical wheel dispatch.
//!
//! The public surface is what user code awaits. A crate-private `driver`
//! submodule owns the per-runtime timer state; a `wheel` submodule will
//! hold the hierarchical wheel data structure (slice 3).

mod driver;
mod wheel;

pub(crate) use driver::Driver;
use driver::EntryId;

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::time::Instant;

use crate::runtime::context::{try_with_current_driver, with_current_driver};

/// Sleep for at least `dur` before resolving.
///
/// # Example
///
/// ```
/// # use core::time::Duration;
/// # use std::time::Instant;
/// let start = Instant::now();
/// nanorun::block_on(async {
///     nanorun::time::sleep(Duration::from_millis(10)).await;
/// });
/// assert!(start.elapsed() >= Duration::from_millis(10));
/// ```
pub fn sleep(dur: Duration) -> impl Future<Output = ()> {
    Sleep {
        deadline: Instant::now() + dur,
        entry_id: None,
    }
}

struct Sleep {
    deadline: Instant,
    entry_id: Option<EntryId>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            return Poll::Ready(());
        }
        if self.entry_id.is_none() {
            // `register` returns `None` when the deadline was already
            // past at the moment the wheel inspected it — in that case
            // it has fired the waker synchronously, and the next poll
            // will see `Instant::now() >= deadline` and resolve.
            self.entry_id = with_current_driver(|d| d.register(self.deadline, cx.waker().clone()));
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(id) = self.entry_id {
            let _ = try_with_current_driver(|d| d.cancel(id));
        }
    }
}

/// Run `future` with a deadline `dur` from now.
///
/// Resolves to `Ok(value)` if the inner future completes in time, or
/// `Err(Elapsed)` if the deadline fires first. The future is polled at
/// least once before the deadline is checked, so a synchronously-ready
/// future always wins (even with `dur = 0`).
///
/// # Example
///
/// ```
/// # use core::time::Duration;
/// // The inner future completes in time.
/// let ok = nanorun::block_on(async {
///     nanorun::time::timeout(Duration::from_secs(1), async { 42 }).await
/// });
/// assert_eq!(ok, Ok(42));
///
/// // The inner future never completes — the deadline wins.
/// let err = nanorun::block_on(async {
///     nanorun::time::timeout(
///         Duration::from_millis(10),
///         nanorun::time::sleep(Duration::from_secs(60)),
///     )
///     .await
/// });
/// assert!(err.is_err());
/// ```
pub fn timeout<F: Future>(dur: Duration, future: F) -> Timeout<F> {
    Timeout {
        future,
        sleep: Sleep {
            deadline: Instant::now() + dur,
            entry_id: None,
        },
    }
}

/// Future returned by [`timeout`].
pub struct Timeout<F> {
    future: F,
    sleep: Sleep,
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: hand-rolled structural pin projection. We never move
        // out of `this.future` for the lifetime of the pin — only call
        // `Pin::new_unchecked(&mut this.future)`, which keeps the
        // address fixed. `sleep` is `Unpin` (no `!Unpin` field), so the
        // `Pin::new` projection is safe without an `unsafe` block.
        let this = unsafe { self.get_unchecked_mut() };
        let future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(v) = future.poll(cx) {
            return Poll::Ready(Ok(v));
        }
        let sleep = Pin::new(&mut this.sleep);
        if sleep.poll(cx).is_ready() {
            return Poll::Ready(Err(Elapsed(())));
        }
        Poll::Pending
    }
}

/// Error returned by [`timeout`] when the deadline fires before the
/// inner future completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed(());

impl core::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("deadline elapsed")
    }
}

impl std::error::Error for Elapsed {}
