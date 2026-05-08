//! Cooperative yield primitive.
//!
//! `yield_now().await` returns `Pending` once, schedules the current
//! waker, and resolves to `()` on the next poll. Use it inside CPU-bound
//! loops on a worker thread to give peers a chance to run without
//! blocking.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

/// Cooperative yield: return `Pending` once and re-schedule, then `Ready(())` on the next poll.
pub fn yield_now() -> impl Future<Output = ()> {
    YieldNow { polled: false }
}

struct YieldNow {
    polled: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ptr;
    use core::task::{RawWaker, RawWakerVTable, Waker};

    /// Construct a Waker whose four vtable entries are no-ops.
    ///
    /// The data pointer is never dereferenced; the clone path returns a
    /// fresh raw waker pointing at the same vtable.
    fn noop_waker() -> Waker {
        static VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        // SAFETY: every vtable entry is a no-op and never dereferences `data`.
        unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) }
    }

    #[test]
    fn pending_then_ready() {
        let mut fut = core::pin::pin!(yield_now());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Pending));
        assert!(matches!(fut.as_mut().poll(&mut cx), Poll::Ready(())));
    }
}
