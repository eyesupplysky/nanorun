//! Reactor: registers fd interest, parks the executor, wakes on readiness.
//!
//! Backend choice is cfg-gated. The Linux backend (epoll + eventfd) lives
//! in `linux.rs`; the Windows backend (IOCP, soon plus AFD-poll) lives
//! in `windows.rs`; the fallback (`Mutex` + `Condvar`) for any other
//! target lives in `fallback.rs`.
//!
//! Cross-platform types (`Token`, `Direction`, `Interest`, `Reactor`,
//! `ReactorHandle`) live in this file. Each backend module exposes one
//! `pub(super) struct Inner` with `new`/`poll`/`wake`, and — where the
//! platform supports real I/O — `register`/`set_waker`/`deregister`.

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux::Inner;

#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
use windows::Inner;

#[cfg(any(target_os = "linux", target_os = "windows"))]
mod slab;

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
mod fallback;
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
use fallback::Inner;

/// Cross-platform borrowed I/O source: `BorrowedFd` on Linux, `BorrowedSocket` on Windows.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub(crate) type IoSource<'a> = std::os::fd::BorrowedFd<'a>;
#[cfg(target_os = "windows")]
#[allow(dead_code)]
pub(crate) type IoSource<'a> = std::os::windows::io::BorrowedSocket<'a>;

/// Cross-platform owned I/O source: `OwnedFd` on Linux, `OwnedSocket` on Windows.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
pub(crate) type OwnedIoSource = std::os::fd::OwnedFd;
#[cfg(target_os = "windows")]
#[allow(dead_code)]
pub(crate) type OwnedIoSource = std::os::windows::io::OwnedSocket;

/// Borrow an [`OwnedIoSource`] as an [`IoSource`].
///
/// On Linux this is `AsFd::as_fd`; on Windows this is
/// `AsSocket::as_socket`. Lets `net::tcp` share one body across both
/// platforms while the underlying std types differ.
#[cfg(any(target_os = "linux", target_os = "windows"))]
#[allow(dead_code)]
pub(crate) trait AsIoSource {
    /// Borrow this owned I/O source as the platform's borrowed type.
    fn as_io_source(&self) -> IoSource<'_>;
}

#[cfg(target_os = "linux")]
impl AsIoSource for std::os::fd::OwnedFd {
    fn as_io_source(&self) -> IoSource<'_> {
        std::os::fd::AsFd::as_fd(self)
    }
}

#[cfg(target_os = "windows")]
impl AsIoSource for std::os::windows::io::OwnedSocket {
    fn as_io_source(&self) -> IoSource<'_> {
        std::os::windows::io::AsSocket::as_socket(self)
    }
}

use std::io;
use std::sync::Arc;
use std::time::Duration;

#[cfg(any(target_os = "linux", target_os = "windows"))]
use core::task::Waker;

/// Stable handle returned by [`Reactor::register`].
#[cfg(any(target_os = "linux", target_os = "windows"))]
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
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(Inner::new()?),
        })
    }

    /// Cross-thread wake handle.
    pub(crate) fn handle(&self) -> ReactorHandle {
        ReactorHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Block until a registered fd is ready or [`ReactorHandle::wake`] fires.
    pub(crate) fn poll(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.inner.poll(timeout)
    }
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
#[allow(dead_code)] // wired by Phase 4 (TcpStream / Windows TcpStream)
impl Reactor {
    /// Register `source` for readiness notifications.
    pub(crate) fn register(&self, source: IoSource<'_>, interest: Interest) -> io::Result<Token> {
        self.inner.register(source, interest)
    }

    /// Replace the waker stored for `(token, direction)`.
    pub(crate) fn set_waker(&self, token: Token, direction: Direction, waker: &Waker) {
        self.inner.set_waker(token, direction, waker);
    }

    /// Remove the registration for `(source, token)`.
    pub(crate) fn deregister(&self, source: IoSource<'_>, token: Token) -> io::Result<()> {
        self.inner.deregister(source, token)
    }
}

impl ReactorHandle {
    /// Wake the executor blocked in [`Reactor::poll`].
    pub(crate) fn wake(&self) -> io::Result<()> {
        self.inner.wake()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

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
}
