//! Single-threaded executor driver (M1+M2).
//!
//! The future is stack-pinned with [`core::pin::pin!`] and polled in a
//! loop. When `poll` returns `Pending`, the executor blocks in
//! [`crate::reactor::Reactor::poll`] until either a registered fd is
//! ready or a cross-thread wake fires the reactor handle. The waker
//! (built in [`crate::waker`]) holds an `Arc<ReactorHandle>` and pokes
//! the same reactor instance.

use core::future::Future;
use core::task::{Context, Poll};

use crate::reactor::Reactor;
use crate::runtime::context::Guard;

/// Drive a future to completion on the calling thread.
pub(crate) fn run<F: Future>(f: F) -> F::Output {
    let reactor = Reactor::new().expect("reactor::new");
    let _guard = Guard::install(&reactor);
    let waker = crate::waker::waker_for(reactor.handle());
    let mut cx = Context::from_waker(&waker);
    let mut fut = core::pin::pin!(f);
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => reactor.poll(None).expect("reactor::poll"),
        }
    }
}
