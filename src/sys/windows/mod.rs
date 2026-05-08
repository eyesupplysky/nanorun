//! Windows-specific syscall wrappers.
//!
//! Each submodule contains thin `windows-sys` calls returning [`std::io::Result`].
//! Errors are mapped via [`std::io::Error::from_raw_os_error`] using
//! `WSAGetLastError` (winsock calls) or [`std::io::Error::last_os_error`]
//! (other Win32 calls) at the call site.

pub(crate) mod afd;
pub(crate) mod iocp;
pub(crate) mod socket;
