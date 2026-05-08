//! `epoll_create1`, `epoll_ctl`, `epoll_wait` thin wrappers.
//!
//! Tokens are arbitrary `u64` values stored in the kernel-side
//! `epoll_data` and returned in [`Event`] when readiness fires.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

/// One readiness event returned by [`wait`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Event {
    /// Token registered with [`add`].
    pub(crate) token: u64,
    /// Bitset of `EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP`.
    pub(crate) ready: u32,
}

/// Readable interest flag.
#[allow(clippy::cast_sign_loss)]
pub(crate) const READABLE: u32 = libc::EPOLLIN as u32;
/// Writable interest flag.
#[allow(clippy::cast_sign_loss)]
pub(crate) const WRITABLE: u32 = libc::EPOLLOUT as u32;
/// Error flag (always reported by the kernel even without explicit interest).
#[allow(clippy::cast_sign_loss)]
pub(crate) const ERROR: u32 = libc::EPOLLERR as u32;
/// Hangup flag (always reported by the kernel even without explicit interest).
#[allow(clippy::cast_sign_loss)]
pub(crate) const HANGUP: u32 = libc::EPOLLHUP as u32;

/// Create a new epoll instance.
pub(crate) fn create() -> io::Result<OwnedFd> {
    // SAFETY: epoll_create1 has no preconditions.
    let raw = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: kernel returned a fresh fd that we now own.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Register `fd` with the epoll set, tagging readiness with `token`.
pub(crate) fn add(
    epfd: BorrowedFd<'_>,
    fd: BorrowedFd<'_>,
    token: u64,
    events: u32,
) -> io::Result<()> {
    ctl(epfd, fd, libc::EPOLL_CTL_ADD, token, events)
}

/// Remove `fd` from the epoll set.
pub(crate) fn delete(epfd: BorrowedFd<'_>, fd: BorrowedFd<'_>) -> io::Result<()> {
    ctl(epfd, fd, libc::EPOLL_CTL_DEL, 0, 0)
}

fn ctl(
    epfd: BorrowedFd<'_>,
    fd: BorrowedFd<'_>,
    op: libc::c_int,
    token: u64,
    events: u32,
) -> io::Result<()> {
    let mut ev = libc::epoll_event { events, u64: token };
    let ev_ptr: *mut libc::epoll_event = if op == libc::EPOLL_CTL_DEL {
        core::ptr::null_mut()
    } else {
        &mut ev
    };
    // SAFETY: `epfd` and `fd` are valid borrowed file descriptors for the
    // duration of the call; `ev_ptr` is non-null for ADD/MOD and null for
    // DEL, both of which the kernel accepts.
    let rc = unsafe { libc::epoll_ctl(epfd.as_raw_fd(), op, fd.as_raw_fd(), ev_ptr) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Maximum events filled per [`wait`] call.
pub(crate) const MAX_EVENTS_PER_WAIT: usize = 64;

/// Wait for readiness; fills `out` with up to `out.len().min(64)` ready entries.
pub(crate) fn wait(epfd: BorrowedFd<'_>, out: &mut [Event], timeout_ms: i32) -> io::Result<usize> {
    if out.is_empty() {
        return Ok(0);
    }
    let cap = out.len().min(MAX_EVENTS_PER_WAIT);
    let mut buf = [libc::epoll_event { events: 0, u64: 0 }; MAX_EVENTS_PER_WAIT];
    // SAFETY: `epfd` is a valid borrowed epoll fd; `buf` is a local array of
    // `cap` initialised entries; the kernel writes at most `cap` entries.
    let rc = unsafe {
        libc::epoll_wait(
            epfd.as_raw_fd(),
            buf.as_mut_ptr(),
            i32::try_from(cap).unwrap_or(i32::MAX),
            timeout_ms,
        )
    };
    if rc < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            return Ok(0);
        }
        return Err(err);
    }
    #[allow(clippy::cast_sign_loss)]
    let count = rc as usize;
    for (i, slot) in out.iter_mut().take(count).enumerate() {
        // Reading by value through a packed field is sound; taking a
        // reference to one would not be.
        let token = buf[i].u64;
        let ready = buf[i].events;
        *slot = Event { token, ready };
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sys::linux::eventfd;
    use std::os::fd::AsFd;

    #[test]
    fn create_succeeds() {
        let epfd = create().expect("epoll_create1");
        drop(epfd);
    }

    #[test]
    fn wait_returns_eventfd_readiness() {
        let epfd = create().expect("epoll_create1");
        let evt = eventfd::create().expect("eventfd");
        add(epfd.as_fd(), evt.as_fd(), 7, READABLE).expect("add");

        // No events yet — short timeout returns 0.
        let mut out = [Event { token: 0, ready: 0 }; 4];
        let n = wait(epfd.as_fd(), &mut out, 0).expect("wait");
        assert_eq!(n, 0);

        // Fire the eventfd, then expect one event with our token.
        eventfd::write(evt.as_fd()).expect("eventfd_write");
        let n = wait(epfd.as_fd(), &mut out, 100).expect("wait");
        assert_eq!(n, 1);
        assert_eq!(out[0].token, 7);
        assert!(out[0].ready & READABLE != 0);
    }

    #[test]
    fn delete_removes_fd_from_set() {
        let epfd = create().expect("epoll_create1");
        let evt = eventfd::create().expect("eventfd");
        add(epfd.as_fd(), evt.as_fd(), 1, READABLE).expect("add");
        delete(epfd.as_fd(), evt.as_fd()).expect("delete");

        eventfd::write(evt.as_fd()).expect("eventfd_write");
        let mut out = [Event { token: 0, ready: 0 }; 4];
        let n = wait(epfd.as_fd(), &mut out, 0).expect("wait");
        assert_eq!(n, 0);
    }
}
