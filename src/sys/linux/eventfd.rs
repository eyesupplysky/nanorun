//! `eventfd` thin wrappers — used for cross-thread reactor wakeup.
//!
//! All eventfds in nanorun are created `EFD_NONBLOCK | EFD_CLOEXEC`:
//! readers never block (drain returns `EAGAIN` when empty) and child
//! processes do not inherit the fd.

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

/// Create a new eventfd in non-blocking, close-on-exec mode.
pub(crate) fn create() -> io::Result<OwnedFd> {
    // SAFETY: eventfd has no preconditions; flags are valid kernel constants.
    let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: kernel returned a fresh fd that we now own.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Wake any reader blocked on `fd`. Idempotent under concurrent writes.
pub(crate) fn write(fd: BorrowedFd<'_>) -> io::Result<()> {
    let val: u64 = 1;
    let buf: *const u64 = &val;
    // SAFETY: `fd` is a valid borrowed file descriptor; `buf` points to an
    // 8-byte stack value valid for the duration of the call. eventfd writes
    // are atomic at the 8-byte granularity.
    let rc = unsafe { libc::write(fd.as_raw_fd(), buf.cast::<libc::c_void>(), 8) };
    if rc < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Ok(());
        }
        return Err(err);
    }
    Ok(())
}

/// Drain pending wakes.
pub(crate) fn drain(fd: BorrowedFd<'_>) -> io::Result<()> {
    let mut val: u64 = 0;
    let buf: *mut u64 = &mut val;
    // SAFETY: `fd` is a valid borrowed file descriptor; `buf` points to an
    // 8-byte stack slot the kernel writes into.
    let rc = unsafe { libc::read(fd.as_raw_fd(), buf.cast::<libc::c_void>(), 8) };
    if rc < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Ok(());
        }
        return Err(err);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;

    #[test]
    fn create_succeeds() {
        let fd = create().expect("eventfd");
        drop(fd);
    }

    #[test]
    fn write_then_drain_succeeds() {
        let fd = create().expect("eventfd");
        write(fd.as_fd()).expect("write");
        write(fd.as_fd()).expect("write again — coalesces");
        drain(fd.as_fd()).expect("drain");
        // Second drain should also succeed (EAGAIN treated as Ok).
        drain(fd.as_fd()).expect("drain when empty");
    }
}
