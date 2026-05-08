//! IOCP (I/O Completion Port) thin wrappers — the Windows reactor's parking primitive.
//!
//! In M5 slice 3a the IOCP carries only cross-thread wake completions
//! (posted via [`post`] from another thread, drained by [`wait`] in the
//! reactor's poll loop). Slice 3b layers AFD-poll completions on top of
//! the same IOCP for fd-readiness dispatch.

use core::mem::MaybeUninit;
use core::ptr::null_mut;
use std::io;
use std::os::windows::io::{AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::time::Duration;

use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, PostQueuedCompletionStatus, OVERLAPPED,
    OVERLAPPED_ENTRY,
};

/// Maximum entries pulled per [`wait`] call (matches the Linux reactor's epoll batch size).
pub(crate) const MAX_ENTRIES_PER_WAIT: usize = 64;

// `INFINITE` for `dwMilliseconds`. Pass `u32::MAX` to mean "no timeout".
const INFINITE: u32 = u32::MAX;

// `WAIT_TIMEOUT` is `u32 = 258`; `io::Error::raw_os_error()` returns `i32`.
// 258 fits both representations; the cast is value-preserving.
#[allow(clippy::cast_possible_wrap)]
const WAIT_TIMEOUT_I32: i32 = WAIT_TIMEOUT as i32;

// `HANDLE` is `isize` in windows-sys ≥ 0.46. The bitwise value is the
// kernel handle; the sign reinterpretation is harmless for handles
// produced by the kernel.
#[allow(clippy::cast_possible_wrap)]
fn handle_to_raw(h: BorrowedHandle<'_>) -> HANDLE {
    h.as_raw_handle() as HANDLE
}

/// Create a fresh IOCP with no associated file handle.
pub(crate) fn create() -> io::Result<OwnedHandle> {
    // SAFETY: passing INVALID_HANDLE_VALUE + null + 0 + 0 is the documented
    // creation pattern for a new IOCP not bound to any file.
    let raw = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, null_handle(), 0, 0) };
    if raw == null_handle() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: kernel returned a fresh HANDLE that we now own.
    Ok(unsafe { OwnedHandle::from_raw_handle(raw as RawHandle) })
}

/// Post a completion to `iocp`; the waiting thread receives `completion_key` via [`Entry::completion_key`].
pub(crate) fn post(iocp: BorrowedHandle<'_>, completion_key: usize) -> io::Result<()> {
    // SAFETY: `iocp` is a valid borrowed IOCP handle; null overlapped is the documented "no associated I/O" form.
    let rc = unsafe {
        PostQueuedCompletionStatus(
            handle_to_raw(iocp),
            0,
            completion_key,
            null_mut::<OVERLAPPED>(),
        )
    };
    if rc == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// One completion drained by [`wait`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Entry {
    /// Completion key — caller-defined identifier set at attach or post time.
    #[allow(dead_code)] // dispatch keys off `overlapped` ptr; key kept for diagnostics
    pub(crate) completion_key: usize,
    /// Overlapped pointer associated with the completion (null for posts).
    #[allow(dead_code)] // wired by slice 3b (AFD-poll dispatch)
    pub(crate) overlapped: *mut OVERLAPPED,
}

impl Entry {
    /// Zero-initialised entry suitable for filling slot arrays passed to [`wait`].
    pub(crate) const ZERO: Self = Self {
        completion_key: 0,
        overlapped: null_mut(),
    };
}

/// Drain ready completions; fills `out` with up to `out.len().min(MAX_ENTRIES_PER_WAIT)` entries.
pub(crate) fn wait(
    iocp: BorrowedHandle<'_>,
    out: &mut [Entry],
    timeout: Option<Duration>,
) -> io::Result<usize> {
    let timeout_ms: u32 = match timeout {
        None => INFINITE,
        Some(d) => u32::try_from(d.as_millis()).unwrap_or(INFINITE - 1),
    };
    let cap = out.len().min(MAX_ENTRIES_PER_WAIT);
    let count = u32::try_from(cap).unwrap_or(u32::MAX);
    let mut entries: [MaybeUninit<OVERLAPPED_ENTRY>; MAX_ENTRIES_PER_WAIT] =
        [const { MaybeUninit::uninit() }; MAX_ENTRIES_PER_WAIT];
    let mut removed: u32 = 0;
    // SAFETY: `iocp` valid; `entries` is uninit storage of capacity `count`;
    // `removed` is a stack u32 the kernel writes.
    let rc = unsafe {
        GetQueuedCompletionStatusEx(
            handle_to_raw(iocp),
            entries.as_mut_ptr().cast::<OVERLAPPED_ENTRY>(),
            count,
            &mut removed,
            timeout_ms,
            0,
        )
    };
    if rc == 0 {
        let e = io::Error::last_os_error();
        if e.raw_os_error() == Some(WAIT_TIMEOUT_I32) {
            return Ok(0);
        }
        return Err(e);
    }
    let n = usize::try_from(removed).unwrap_or(0).min(cap);
    for (i, slot) in out.iter_mut().take(n).enumerate() {
        // SAFETY: GetQueuedCompletionStatusEx initialised `entries[..removed]`.
        let raw = unsafe { entries[i].assume_init() };
        *slot = Entry {
            completion_key: raw.lpCompletionKey,
            overlapped: raw.lpOverlapped,
        };
    }
    Ok(n)
}

// `HANDLE` is `isize`; null-handle constant.
const fn null_handle() -> HANDLE {
    0
}
