//! AFD (Ancillary Function Driver) thin wrappers — the kernel mechanism behind Winsock readiness.
//!
//! The interface is undocumented Win32: we open a handle to `\\Device\\Afd`
//! via `NtCreateFile`, associate it with the reactor's IOCP, and submit
//! `IOCTL_AFD_POLL` IOCTLs through `NtDeviceIoControlFile`. Each pending
//! IOCTL completes on the IOCP when one of the requested events fires on
//! the associated socket; the completion's `lpOverlapped` is the
//! [`IoStatusBlock`] pointer we passed in, which lets the reactor recover
//! the per-socket state.
//!
//! This module deliberately mirrors the wepoll/mio approach. The constants
//! and structure layouts come from the Windows DDK headers; they are not
//! part of the public Win32 SDK.

use core::ffi::c_void;
use core::mem;
use core::ptr::{null, null_mut};
use std::io;
use std::os::windows::io::{AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle, RawHandle};

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
use windows_sys::Win32::System::IO::CreateIoCompletionPort;

/// Completion key set when associating the AFD handle with the IOCP.
/// The Windows reactor uses this to distinguish AFD-poll completions
/// (key = `AFD_COMPLETION_KEY`) from cross-thread wakes (key = `0`).
pub(crate) const AFD_COMPLETION_KEY: usize = 1;

/// `STATUS_PENDING` — IOCTL queued, completion will arrive via IOCP.
pub(crate) const STATUS_PENDING: i32 = 0x103;
/// `STATUS_CANCELLED` — IOCTL was cancelled by [`cancel`].
#[allow(clippy::cast_possible_wrap)]
#[allow(dead_code)] // surfaced by slice 4 when checking completion status
pub(crate) const STATUS_CANCELLED: i32 = 0xC000_0120_u32 as i32;
/// `STATUS_NOT_FOUND` — `NtCancelIoFileEx` could not find the IRP (already completed).
#[allow(clippy::cast_possible_wrap)]
pub(crate) const STATUS_NOT_FOUND: i32 = 0xC000_0225_u32 as i32;

const STATUS_SUCCESS: i32 = 0;

const SYNCHRONIZE: u32 = 0x0010_0000;
const FILE_OPEN: u32 = 1;
const FILE_SHARE_READ: u32 = 0x1;
const FILE_SHARE_WRITE: u32 = 0x2;

const IOCTL_AFD_POLL: u32 = 0x0001_2024;

/// AFD poll-event flags.
pub(crate) const POLL_RECEIVE: u32 = 0x0001;
/// OOB receive readiness — folded into "readable".
pub(crate) const POLL_RECEIVE_EXPEDITED: u32 = 0x0002;
/// Send readiness — "writable".
pub(crate) const POLL_SEND: u32 = 0x0004;
/// Graceful peer disconnect — folded into "readable".
pub(crate) const POLL_DISCONNECT: u32 = 0x0008;
/// Abortive peer disconnect — folded into "readable".
pub(crate) const POLL_ABORT: u32 = 0x0010;
/// Local close (the socket handle was closed). Surfaced for completeness.
#[allow(dead_code)] // not in READ/WRITE_EVENTS — reserved for diagnostic use
pub(crate) const POLL_LOCAL_CLOSE: u32 = 0x0020;
/// Connect-complete — folded into "writable" for connecting client sockets.
pub(crate) const POLL_CONNECT: u32 = 0x0040;
/// Accept-ready — folded into "readable" for listening sockets.
pub(crate) const POLL_ACCEPT: u32 = 0x0080;
/// Connect-failed — folded into both "readable" and "writable" so the future re-syscalls and observes the error.
pub(crate) const POLL_CONNECT_FAIL: u32 = 0x0100;

/// Mask of AFD-poll bits we treat as "readable" readiness.
pub(crate) const READ_EVENTS: u32 = POLL_RECEIVE
    | POLL_RECEIVE_EXPEDITED
    | POLL_DISCONNECT
    | POLL_ABORT
    | POLL_ACCEPT
    | POLL_CONNECT_FAIL;

/// Mask of AFD-poll bits we treat as "writable" readiness.
pub(crate) const WRITE_EVENTS: u32 = POLL_SEND | POLL_CONNECT | POLL_CONNECT_FAIL;

// read as NTSTATUS via [`status`]; `information` is reserved for the kernel.
/// `IO_STATUS_BLOCK` as understood by the NT executive.
#[repr(C)]
pub(crate) struct IoStatusBlock {
    /// C union { NTSTATUS Status; PVOID Pointer; } — read low 32 bits as NTSTATUS.
    pub(crate) status_or_pointer: usize,
    /// Bytes-transferred / handle-of-target on completion.
    pub(crate) information: usize,
}

impl IoStatusBlock {
    /// Zero-initialised IOSB suitable for handing to `NtDeviceIoControlFile`.
    pub(crate) const ZERO: Self = Self {
        status_or_pointer: 0,
        information: 0,
    };

    /// Read the NTSTATUS slot of the union.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    #[allow(dead_code)] // surfaced by slice 4 for AFD-poll error reporting
    pub(crate) fn status(&self) -> i32 {
        // On x64 the union is 8 bytes; NTSTATUS occupies the low 4. The
        // cast preserves the bit pattern.
        self.status_or_pointer as i32
    }
}

#[repr(C)]
struct UnicodeString {
    length: u16,
    maximum_length: u16,
    buffer: *mut u16,
}

#[repr(C)]
struct ObjectAttributes {
    length: u32,
    root_directory: HANDLE,
    object_name: *const UnicodeString,
    attributes: u32,
    security_descriptor: *mut c_void,
    security_quality_of_service: *mut c_void,
}

/// One handle's worth of poll input/output, as the AFD driver expects.
#[repr(C)]
pub(crate) struct AfdPollHandleInfo {
    /// Base socket handle (must come from `socket::base_socket`).
    pub(crate) handle: HANDLE,
    /// Input: requested events. Output: events that fired.
    pub(crate) events: u32,
    /// Output: NTSTATUS for this handle's poll outcome.
    pub(crate) status: i32,
}

/// Poll request: what events to watch for, on which handles.
#[repr(C)]
pub(crate) struct AfdPollInfo {
    /// Relative timeout in 100ns units (negative). `i64::MAX` ≈ "infinite".
    pub(crate) timeout: i64,
    /// Number of valid entries in `handles` (always 1 for nanorun).
    pub(crate) number_of_handles: u32,
    /// Reserved; keep zero.
    pub(crate) exclusive: u32,
    /// One handle's worth of input/output. nanorun watches one socket per IOCTL.
    pub(crate) handles: [AfdPollHandleInfo; 1],
}

impl AfdPollInfo {
    /// Zero-initialised poll info; caller fills `timeout`, `number_of_handles`, and `handles[0]`.
    pub(crate) const ZERO: Self = Self {
        timeout: 0,
        number_of_handles: 0,
        exclusive: 0,
        handles: [AfdPollHandleInfo {
            handle: 0,
            events: 0,
            status: 0,
        }],
    };
}

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtCreateFile(
        FileHandle: *mut HANDLE,
        DesiredAccess: u32,
        ObjectAttributes: *const ObjectAttributes,
        IoStatusBlock: *mut IoStatusBlock,
        AllocationSize: *const i64,
        FileAttributes: u32,
        ShareAccess: u32,
        CreateDisposition: u32,
        CreateOptions: u32,
        EaBuffer: *const c_void,
        EaLength: u32,
    ) -> i32;

    fn NtDeviceIoControlFile(
        FileHandle: HANDLE,
        Event: HANDLE,
        ApcRoutine: *const c_void,
        ApcContext: *const c_void,
        IoStatusBlock: *mut IoStatusBlock,
        IoControlCode: u32,
        InputBuffer: *const c_void,
        InputBufferLength: u32,
        OutputBuffer: *mut c_void,
        OutputBufferLength: u32,
    ) -> i32;

    fn NtCancelIoFileEx(
        FileHandle: HANDLE,
        IoRequestToCancel: *mut IoStatusBlock,
        IoStatusBlock: *mut IoStatusBlock,
    ) -> i32;

    fn RtlNtStatusToDosError(Status: i32) -> u32;
}

// AFD requires the helper handle to live under `\Device\Afd\<name>` —
// opening the bare device path does not produce a handle the driver
// will accept for IOCTL_AFD_POLL. The name ("Nanorun" here) is
// arbitrary; wepoll uses "Wepoll", mio uses "Mio".
const AFD_DEVICE_NAME_LEN: usize = 19;
const AFD_DEVICE_NAME: [u16; AFD_DEVICE_NAME_LEN] = [
    0x005C, 0x0044, 0x0065, 0x0076, 0x0069, 0x0063, 0x0065, // \Device
    0x005C, 0x0041, 0x0066, 0x0064, // \Afd
    0x005C, 0x004E, 0x0061, 0x006E, 0x006F, 0x0072, 0x0075, 0x006E, // \Nanorun
];

// `HANDLE` is `isize`; the bitwise value is a kernel handle.
#[allow(clippy::cast_possible_wrap)]
fn handle_to_raw(h: BorrowedHandle<'_>) -> HANDLE {
    h.as_raw_handle() as HANDLE
}

fn nt_status_to_io_error(status: i32) -> io::Error {
    // SAFETY: RtlNtStatusToDosError is FFI-pure.
    let win32 = unsafe { RtlNtStatusToDosError(status) };
    #[allow(clippy::cast_possible_wrap)]
    io::Error::from_raw_os_error(win32 as i32)
}

/// Open `\\Device\\Afd` and associate it with `iocp`.
pub(crate) fn open(iocp: BorrowedHandle<'_>) -> io::Result<OwnedHandle> {
    #[allow(clippy::cast_possible_truncation)]
    let name_bytes = (AFD_DEVICE_NAME_LEN * mem::size_of::<u16>()) as u16;
    let name = UnicodeString {
        length: name_bytes,
        maximum_length: name_bytes,
        buffer: AFD_DEVICE_NAME.as_ptr().cast_mut(),
    };
    #[allow(clippy::cast_possible_truncation)]
    let attrs = ObjectAttributes {
        length: mem::size_of::<ObjectAttributes>() as u32,
        root_directory: 0,
        object_name: &name,
        attributes: 0,
        security_descriptor: null_mut(),
        security_quality_of_service: null_mut(),
    };
    let mut iosb = IoStatusBlock::ZERO;
    let mut handle: HANDLE = 0;
    // SAFETY: all pointers reference live stack data; the kernel writes
    // `handle` and `iosb`. CreateDisposition=FILE_OPEN means "open existing".
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            SYNCHRONIZE,
            &attrs,
            &mut iosb,
            null(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            FILE_OPEN,
            0,
            null(),
            0,
        )
    };
    if status != STATUS_SUCCESS {
        return Err(nt_status_to_io_error(status));
    }
    // SAFETY: kernel returned a fresh HANDLE that we now own.
    let owned = unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) };
    // Associate with IOCP so AFD-poll completions land there.
    // SAFETY: `handle` is the just-opened AFD handle; `iocp` is borrowed live.
    let assoc =
        unsafe { CreateIoCompletionPort(handle, handle_to_raw(iocp), AFD_COMPLETION_KEY, 0) };
    if assoc == 0 {
        // Drop `owned` to close the AFD handle before returning the error.
        return Err(io::Error::last_os_error());
    }
    Ok(owned)
}

/// Submit one `IOCTL_AFD_POLL` on `afd`. Returns Ok when queued (`STATUS_PENDING`) or completed synchronously.
pub(crate) fn submit(
    afd: BorrowedHandle<'_>,
    iosb: &mut IoStatusBlock,
    info: &mut AfdPollInfo,
) -> io::Result<()> {
    iosb.status_or_pointer = STATUS_PENDING as usize;
    iosb.information = 0;
    #[allow(clippy::cast_possible_truncation)]
    let info_size = mem::size_of::<AfdPollInfo>() as u32;
    // SAFETY: `afd` is a valid AFD handle associated with the reactor IOCP;
    // `iosb` and `info` are exclusively borrowed; the kernel writes both
    // asynchronously, but the caller guarantees they remain live until
    // the completion arrives on the IOCP.
    let status = unsafe {
        NtDeviceIoControlFile(
            handle_to_raw(afd),
            0,
            null(),
            null(),
            iosb,
            IOCTL_AFD_POLL,
            (info as *mut AfdPollInfo).cast::<c_void>(),
            info_size,
            (info as *mut AfdPollInfo).cast::<c_void>(),
            info_size,
        )
    };
    if status == STATUS_SUCCESS || status == STATUS_PENDING {
        return Ok(());
    }
    Err(nt_status_to_io_error(status))
}

/// Cancel the IOCTL whose `iosb` matches.
pub(crate) fn cancel(afd: BorrowedHandle<'_>, iosb: &mut IoStatusBlock) -> io::Result<()> {
    let mut out_iosb = IoStatusBlock::ZERO;
    // SAFETY: `afd` is a valid handle; `iosb` identifies the in-flight IRP;
    // `out_iosb` is a stack slot the kernel writes.
    let status = unsafe { NtCancelIoFileEx(handle_to_raw(afd), iosb, &mut out_iosb) };
    if status == STATUS_SUCCESS || status == STATUS_NOT_FOUND {
        return Ok(());
    }
    Err(nt_status_to_io_error(status))
}

/// Best-effort handle close, used in error paths where `OwnedHandle` isn't yet owned.
#[allow(dead_code)]
pub(crate) fn close_raw(handle: HANDLE) {
    // SAFETY: caller asserts `handle` is a valid kernel handle they own.
    unsafe { CloseHandle(handle) };
}
