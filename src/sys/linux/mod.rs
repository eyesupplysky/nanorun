//! Linux-specific syscall wrappers.
//!
//! Each submodule contains thin `libc` calls returning [`std::io::Result`].
//! Errors are mapped via [`std::io::Error::last_os_error`] at the call site.

pub(crate) mod epoll;
pub(crate) mod eventfd;
pub(crate) mod socket;
