//! OS shims: the only place raw `libc` / `windows-sys` calls may live.
//!
//! Backends register their syscall wrappers here so the rest of the
//! crate stays platform-agnostic. The Linux backend lives under
//! `linux`; the Windows backend lives under `windows` (M5: socket
//! wrappers landed in slice 2, AFD-poll lands in slice 3). Other
//! targets have no real-I/O backend and fall back to the in-process
//! notifier built into `crate::reactor::fallback`.


#[cfg(target_os = "linux")]
pub(crate) mod linux;

#[cfg(target_os = "windows")]
pub(crate) mod windows;
