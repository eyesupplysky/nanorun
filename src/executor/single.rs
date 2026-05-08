//! Single-threaded executor driver (M1).
//!
//! The future is stack-pinned with [`core::pin::pin!`] and polled in a
//! loop. When `poll` returns `Pending`, the calling thread parks; the
//! waker (built in [`crate::waker`]) holds an `Arc<Thread>` to that
//! same thread and calls `unpark` to schedule the next iteration.

use core::future::Future;
use core::task::{Context, Poll};
use std::thread;

/// Drive a future to completion on the calling thread.
pub(crate) fn run<F: Future>(f: F) -> F::Output {
    let mut fut = core::pin::pin!(f);
    let waker = crate::waker::waker_for(thread::current());
    let mut cx = Context::from_waker(&waker);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}
