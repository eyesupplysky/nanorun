//! Reactor: registers fd interest, parks the executor, wakes on readiness.
//!
//! The abstraction shape (pluggable trait vs. cfg-gated module) is
//! deliberately undecided until M5 lands the Windows backend. M2 ships
//! one struct, with the Linux backend (epoll + eventfd) and a non-Linux
//! fallback (Mutex + Condvar) cfg-gated within this file. Do not split
//! into `linux.rs` / `windows.rs` before M5.

#[cfg(target_os = "linux")]
mod slab;

use std::io;
use std::sync::Arc;
use std::time::Duration;

#[cfg(target_os = "linux")]
use core::task::Waker;
#[cfg(target_os = "linux")]
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
#[cfg(target_os = "linux")]
use std::sync::Mutex;

#[cfg(not(target_os = "linux"))]
use std::sync::{Condvar, Mutex};

#[cfg(target_os = "linux")]
use slab::Slab;

#[cfg(target_os = "linux")]
const SELF_WAKE_TOKEN: u64 = 0;

/// Stable handle returned by [`Reactor::register`].
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Token(pub(crate) u64);

/// Wakeup direction, used by [`Reactor::set_waker`].
#[allow(dead_code)] // wired by Phase 4 (TcpStream)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

/// Bitset of directions to register interest for.
#[allow(dead_code)] // wired by Phase 4 (TcpStream)
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Interest {
    pub(crate) read: bool,
    pub(crate) write: bool,
}

#[allow(dead_code)]
impl Interest {
    pub(crate) const READ: Self = Self {
        read: true,
        write: false,
    };
    pub(crate) const WRITE: Self = Self {
        read: false,
        write: true,
    };
    pub(crate) const READ_WRITE: Self = Self {
        read: true,
        write: true,
    };
}

struct Inner {
    #[cfg(target_os = "linux")]
    slots: Mutex<Slab>,
    #[cfg(target_os = "linux")]
    epfd: OwnedFd,
    #[cfg(target_os = "linux")]
    waker_fd: OwnedFd,

    #[cfg(not(target_os = "linux"))]
    notify: Mutex<bool>,
    #[cfg(not(target_os = "linux"))]
    cvar: Condvar,
}

/// Per-runtime reactor. Owned by the executor; one per `Runtime::block_on` call.
pub(crate) struct Reactor {
    inner: Arc<Inner>,
}

/// Cross-thread wake handle. Cheap to clone, safe to send across threads.
#[derive(Clone)]
pub(crate) struct ReactorHandle {
    inner: Arc<Inner>,
}

impl Reactor {
    /// Construct a fresh reactor.
    #[allow(clippy::unnecessary_wraps)] // Linux branch is fallible
    pub(crate) fn new() -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            use crate::sys::linux::{epoll, eventfd};
            let epfd = epoll::create()?;
            let waker_fd = eventfd::create()?;
            epoll::add(
                epfd.as_fd(),
                waker_fd.as_fd(),
                SELF_WAKE_TOKEN,
                epoll::READABLE,
            )?;
            Ok(Self {
                inner: Arc::new(Inner {
                    slots: Mutex::new(Slab::new()),
                    epfd,
                    waker_fd,
                }),
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(Self {
                inner: Arc::new(Inner {
                    notify: Mutex::new(false),
                    cvar: Condvar::new(),
                }),
            })
        }
    }

    /// Cross-thread wake handle.
    pub(crate) fn handle(&self) -> ReactorHandle {
        ReactorHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Block until a registered fd is ready or [`ReactorHandle::wake`] fires.
    #[allow(clippy::unnecessary_wraps)] // Linux branch is fallible
    pub(crate) fn poll(&self, timeout: Option<Duration>) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            self.poll_linux(timeout)
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.poll_fallback(timeout);
            Ok(())
        }
    }

    #[cfg(target_os = "linux")]
    fn poll_linux(&self, timeout: Option<Duration>) -> io::Result<()> {
        use crate::sys::linux::{epoll, eventfd};
        let timeout_ms: i32 = match timeout {
            None => -1,
            Some(d) => i32::try_from(d.as_millis()).unwrap_or(i32::MAX),
        };
        let mut events = [epoll::Event { token: 0, ready: 0 }; epoll::MAX_EVENTS_PER_WAIT];
        let n = epoll::wait(self.inner.epfd.as_fd(), &mut events, timeout_ms)?;
        if n == 0 {
            return Ok(());
        }
        let mut to_wake: Vec<Waker> = Vec::with_capacity(n * 2);
        {
            let mut slots = self.inner.slots.lock().expect("reactor slab poisoned");
            for ev in &events[..n] {
                if ev.token == SELF_WAKE_TOKEN {
                    eventfd::drain(self.inner.waker_fd.as_fd())?;
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

    #[cfg(not(target_os = "linux"))]
    fn poll_fallback(&self, timeout: Option<Duration>) {
        let mut notified = self.inner.notify.lock().expect("reactor notify poisoned");
        if !*notified {
            notified = match timeout {
                Some(d) => {
                    self.inner
                        .cvar
                        .wait_timeout(notified, d)
                        .expect("reactor cvar")
                        .0
                }
                None => self.inner.cvar.wait(notified).expect("reactor cvar"),
            };
        }
        *notified = false;
    }
}

#[cfg(target_os = "linux")]
#[allow(dead_code)] // wired by Phase 4 (TcpStream)
impl Reactor {
    /// Register `fd` for readiness notifications.
    pub(crate) fn register(&self, fd: BorrowedFd<'_>, interest: Interest) -> io::Result<Token> {
        use crate::sys::linux::epoll;
        let mut events: u32 = 0;
        if interest.read {
            events |= epoll::READABLE;
        }
        if interest.write {
            events |= epoll::WRITABLE;
        }
        let key = self
            .inner
            .slots
            .lock()
            .expect("reactor slab poisoned")
            .insert();
        let token = u64::try_from(key)
            .expect("slab key fits in u64")
            .checked_add(1)
            .expect("slab key under u64::MAX");
        if let Err(e) = epoll::add(self.inner.epfd.as_fd(), fd, token, events) {
            self.inner
                .slots
                .lock()
                .expect("reactor slab poisoned")
                .remove(key);
            return Err(e);
        }
        Ok(Token(token))
    }

    /// Replace the waker stored for `(token, direction)`.
    pub(crate) fn set_waker(&self, token: Token, direction: Direction, waker: &Waker) {
        let Some(raw) = token.0.checked_sub(1) else {
            return; // token 0 is reserved
        };
        let Ok(key) = usize::try_from(raw) else {
            return;
        };
        let mut slots = self.inner.slots.lock().expect("reactor slab poisoned");
        if let Some(slot) = slots.get_mut(key) {
            match direction {
                Direction::Read => slot.read = Some(waker.clone()),
                Direction::Write => slot.write = Some(waker.clone()),
            }
        }
    }

    /// Remove the registration for `(fd, token)`.
    pub(crate) fn deregister(&self, fd: BorrowedFd<'_>, token: Token) -> io::Result<()> {
        use crate::sys::linux::epoll;
        let Some(raw) = token.0.checked_sub(1) else {
            return Ok(());
        };
        let Ok(key) = usize::try_from(raw) else {
            return Ok(());
        };
        epoll::delete(self.inner.epfd.as_fd(), fd)?;
        self.inner
            .slots
            .lock()
            .expect("reactor slab poisoned")
            .remove(key);
        Ok(())
    }
}

impl ReactorHandle {
    /// Wake the executor blocked in [`Reactor::poll`].
    #[allow(clippy::unnecessary_wraps)] // Linux branch is fallible
    pub(crate) fn wake(&self) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            crate::sys::linux::eventfd::write(self.inner.waker_fd.as_fd())
        }
        #[cfg(not(target_os = "linux"))]
        {
            *self.inner.notify.lock().expect("reactor notify poisoned") = true;
            self.inner.cvar.notify_one();
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_os = "linux")]
    use std::sync::atomic::{AtomicBool, Ordering};
    #[cfg(target_os = "linux")]
    use std::task::Wake;
    use std::time::Instant;

    #[cfg(target_os = "linux")]
    struct Flag(AtomicBool);
    #[cfg(target_os = "linux")]
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[cfg(target_os = "linux")]
    fn flag() -> (Arc<Flag>, Waker) {
        let f = Arc::new(Flag(AtomicBool::new(false)));
        let w = Waker::from(Arc::clone(&f));
        (f, w)
    }

    #[test]
    fn handle_wake_breaks_blocking_poll() {
        let r = Reactor::new().expect("reactor");
        let h = r.handle();
        let started = Instant::now();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            h.wake().expect("wake");
        });
        r.poll(None).expect("poll");
        t.join().expect("thread");
        assert!(started.elapsed() >= Duration::from_millis(15));
    }

    #[test]
    fn handle_wake_already_pending_returns_immediately() {
        let r = Reactor::new().expect("reactor");
        r.handle().wake().expect("wake");
        let started = Instant::now();
        r.poll(Some(Duration::from_secs(5))).expect("poll");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "poll should return immediately when wake is pending"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn registered_fd_readiness_drives_waker() {
        use crate::sys::linux::eventfd;
        use std::os::fd::AsFd;

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

    #[cfg(target_os = "linux")]
    #[test]
    fn waker_is_consumed_on_fire_not_re_armed() {
        use crate::sys::linux::eventfd;
        use std::os::fd::AsFd;

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
