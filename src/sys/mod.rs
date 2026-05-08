//! OS shims: the only place raw `libc` / `windows-sys` calls may live.
//!
//! Backends register their syscall wrappers here so the rest of the
//! crate stays platform-agnostic. The Linux backend lives under
//! `linux`; other targets fall back to the in-process notifier built
//! into `crate::reactor` until M5 lands real IOCP support.


#[cfg(target_os = "linux")]
pub(crate) mod linux;
