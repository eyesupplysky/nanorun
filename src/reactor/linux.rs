//! Linux reactor backend: `epoll` for fd readiness, `eventfd` for cross-thread wakeups.
//!
//! The kernel-side `epoll_data` carries a `u64` token: token 0 is the
//! self-wake eventfd; non-zero tokens are slab keys + 1 (see the
//! `reactor-token` pattern documented on [`super::Reactor`]).

use core::task::Waker;
use std::io;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Mutex;
use std::time::Duration;

use super::slab::Slab;
use super::{Direction, Interest, Token};
use crate::sys::linux::{epoll, eventfd};

const SELF_WAKE_TOKEN: u64 = 0;

/// Linux backend state. One per [`super::Reactor`]; held behind `Arc<Inner>`.
pub(super) struct Inner {
    slots: Mutex<Slab>,
    epfd: OwnedFd,
    waker_fd: OwnedFd,
}

impl Inner {
    pub(super) fn new() -> io::Result<Self> {
        let epfd = epoll::create()?;
        let waker_fd = eventfd::create()?;
        epoll::add(
            epfd.as_fd(),
            waker_fd.as_fd(),
            SELF_WAKE_TOKEN,
            epoll::READABLE,
        )?;
        Ok(Self {
            slots: Mutex::new(Slab::new()),
            epfd,
            waker_fd,
        })
    }

    pub(super) fn poll(&self, timeout: Option<Duration>) -> io::Result<()> {
        let timeout_ms: i32 = match timeout {
            None => -1,
            Some(d) => i32::try_from(d.as_millis()).unwrap_or(i32::MAX),
        };
        let mut events = [epoll::Event { token: 0, ready: 0 }; epoll::MAX_EVENTS_PER_WAIT];
        let n = epoll::wait(self.epfd.as_fd(), &mut events, timeout_ms)?;
        if n == 0 {
            return Ok(());
        }
        let mut to_wake: Vec<Waker> = Vec::with_capacity(n * 2);
        {
            let mut slots = self.slots.lock().expect("reactor slab poisoned");
            for ev in &events[..n] {
                if ev.token == SELF_WAKE_TOKEN {
                    eventfd::drain(self.waker_fd.as_fd())?;
                    continue;
                }
                let Ok(key) = usize::try_from(ev.token - 1) else {
                    continue;
                };
                let Some(slot) = slots.get_mut(key) else {
                    continue;
                };
                let err_or_hup = ev.ready & (epoll::ERROR | epoll::HANGUP) != 0;
                let readable = ev.ready & epoll::READABLE != 0 || err_or_hup;
                let writable = ev.ready & epoll::WRITABLE != 0 || err_or_hup;
                if readable {
                    if let Some(w) = slot.read.take() {
                        to_wake.push(w);
                    }
                }
                if writable {
                    if let Some(w) = slot.write.take() {
                        to_wake.push(w);
                    }
                }
            }
        }
        for w in to_wake {
            w.wake();
        }
        Ok(())
    }

    pub(super) fn wake(&self) -> io::Result<()> {
        eventfd::write(self.waker_fd.as_fd())
    }
}

#[allow(dead_code)] // wired by Phase 4 (TcpStream)
impl Inner {
    pub(super) fn register(&self, fd: BorrowedFd<'_>, interest: Interest) -> io::Result<Token> {
        let mut events: u32 = 0;
        if interest.read {
            events |= epoll::READABLE;
        }
        if interest.write {
            events |= epoll::WRITABLE;
        }
        let key = self.slots.lock().expect("reactor slab poisoned").insert();
        let token = u64::try_from(key)
            .expect("slab key fits in u64")
            .checked_add(1)
            .expect("slab key under u64::MAX");
        if let Err(e) = epoll::add(self.epfd.as_fd(), fd, token, events) {
            self.slots
                .lock()
                .expect("reactor slab poisoned")
                .remove(key);
            return Err(e);
        }
        Ok(Token(token))
    }

    pub(super) fn set_waker(&self, token: Token, direction: Direction, waker: &Waker) {
        let Some(raw) = token.0.checked_sub(1) else {
            return; // token 0 is reserved
        };
        let Ok(key) = usize::try_from(raw) else {
            return;
        };
        let mut slots = self.slots.lock().expect("reactor slab poisoned");
        if let Some(slot) = slots.get_mut(key) {
            match direction {
                Direction::Read => slot.read = Some(waker.clone()),
                Direction::Write => slot.write = Some(waker.clone()),
            }
        }
    }

    pub(super) fn deregister(&self, fd: BorrowedFd<'_>, token: Token) -> io::Result<()> {
        let Some(raw) = token.0.checked_sub(1) else {
            return Ok(());
        };
        let Ok(key) = usize::try_from(raw) else {
            return Ok(());
        };
        epoll::delete(self.epfd.as_fd(), fd)?;
        self.slots
            .lock()
            .expect("reactor slab poisoned")
            .remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reactor::Reactor;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::task::Wake;

    struct Flag(AtomicBool);
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn flag() -> (Arc<Flag>, Waker) {
        let f = Arc::new(Flag(AtomicBool::new(false)));
        let w = Waker::from(Arc::clone(&f));
        (f, w)
    }

    #[test]
    fn registered_fd_readiness_drives_waker() {
        let r = Reactor::new().expect("reactor");
        let efd = eventfd::create().expect("eventfd");
        let token = r.register(efd.as_fd(), Interest::READ).expect("register");

        let (flag, waker) = flag();
        r.set_waker(token, Direction::Read, &waker);

        eventfd::write(efd.as_fd()).expect("write");
        r.poll(Some(Duration::from_millis(200))).expect("poll");

        assert!(
            flag.0.load(Ordering::SeqCst),
            "waker should have been fired"
        );

        r.deregister(efd.as_fd(), token).expect("deregister");
    }

    #[test]
    fn waker_is_consumed_on_fire_not_re_armed() {
        let r = Reactor::new().expect("reactor");
        let efd = eventfd::create().expect("eventfd");
        let token = r.register(efd.as_fd(), Interest::READ).expect("register");

        let (flag, waker) = flag();
        r.set_waker(token, Direction::Read, &waker);
        eventfd::write(efd.as_fd()).expect("write");
        r.poll(Some(Duration::from_millis(200))).expect("poll");
        assert!(flag.0.load(Ordering::SeqCst));

        // Reset the flag. Without re-arming the waker, a second readiness
        // event must NOT fire it again — the slot was cleared.
        flag.0.store(false, Ordering::SeqCst);
        // Drain the eventfd we wrote earlier so the next write is a fresh edge.
        eventfd::drain(efd.as_fd()).expect("drain");
        eventfd::write(efd.as_fd()).expect("write");
        r.poll(Some(Duration::from_millis(50))).expect("poll");
        assert!(
            !flag.0.load(Ordering::SeqCst),
            "waker must not re-fire without set_waker"
        );

        r.deregister(efd.as_fd(), token).expect("deregister");
    }
}
