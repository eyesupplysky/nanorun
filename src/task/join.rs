//! User-facing handle for awaiting a spawned task's output.

use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::atomic::Ordering;

use crate::task::raw::{drop_join_interest, try_take_output, TaskRef, COMPLETE, JOIN_WAKER};

/// Handle returned from spawning a task. Awaiting it yields the task's output.
///
/// Dropping a `JoinHandle` without awaiting detaches the task: it will
/// continue to run to completion, but its output is dropped instead of
/// being delivered.
pub struct JoinHandle<T> {
    raw: Option<TaskRef>,
    _phantom: PhantomData<T>,
}

// `JoinHandle` is unconditionally `Unpin` — its only state is a refcounted
// pointer and a phantom marker. Pin projection through `Pin::deref_mut`
// in `Future::poll` relies on this.
impl<T> core::marker::Unpin for JoinHandle<T> {}

impl<T> JoinHandle<T> {
    pub(crate) fn new(raw: TaskRef) -> Self {
        Self {
            raw: Some(raw),
            _phantom: PhantomData,
        }
    }

    fn take_ready(&mut self) -> T {
        let header = self
            .raw
            .as_ref()
            .expect("JoinHandle polled after completion")
            .header();
        match try_take_output::<T>(header) {
            Some(v) => {
                // Once we've taken the output, drop the join ref so the
                // task allocation can be freed; future polls panic.
                self.raw = None;
                v
            }
            None => panic!("JoinHandle polled after output was already taken"),
        }
    }
}

impl<T> Future for JoinHandle<T>
where
    T: Send + 'static,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        // Fast path: task already finished.
        {
            let header = self
                .raw
                .as_ref()
                .expect("JoinHandle polled after completion")
                .header();
            if header.state.load(Ordering::Acquire) & COMPLETE != 0 {
                return Poll::Ready(self.take_ready());
            }
        }

        // Slow path: park. Install the waker, set JOIN_WAKER, then
        // re-check COMPLETE to close the race against the worker that
        // may complete the task between our first check and our
        // registration.
        {
            let header = self
                .raw
                .as_ref()
                .expect("JoinHandle polled after completion")
                .header();
            {
                let mut slot = header.join_waker.lock().expect("join waker poisoned");
                *slot = Some(cx.waker().clone());
            }
            header.state.fetch_or(JOIN_WAKER, Ordering::Release);

            if header.state.load(Ordering::Acquire) & COMPLETE != 0 {
                return Poll::Ready(self.take_ready());
            }
        }

        Poll::Pending
    }
}

impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if let Some(raw) = self.raw.as_ref() {
            drop_join_interest(raw.header());
        }
        // `self.raw` drops here, releasing this handle's strong ref.
    }
}
