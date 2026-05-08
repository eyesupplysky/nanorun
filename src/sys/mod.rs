//! OS shims: the only place raw `libc` / `windows-sys` calls may live.
//!
//! Backends register their syscall wrappers here so the rest of the
//! crate stays platform-agnostic. Empty until M2 needs the first
//! `epoll_create1` call.

#[allow(dead_code)]
pub(crate) fn placeholder() {}
