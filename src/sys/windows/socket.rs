//! `WSAStartup`/`WSASocketW`/`bind`/`listen`/`accept`/`connect`/`send`/`recv` thin wrappers.
//!
//! All sockets are created with `WSA_FLAG_OVERLAPPED |
//! WSA_FLAG_NO_HANDLE_INHERIT` and made non-blocking via
//! `ioctlsocket(FIONBIO, 1)` immediately after creation. Address
//! conversion goes through the windows-sys `SOCKADDR_IN` / `SOCKADDR_IN6`
//! shapes; we never leak `SOCKADDR_STORAGE` out of this module.
//!
//! Winsock requires a process-global `WSAStartup` before any socket
//! operation. [`stream_socket`] performs that init lazily via a
//! [`std::sync::OnceLock`].

use core::mem::{self, MaybeUninit};
use core::ptr::{addr_of, addr_of_mut, null};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::windows::io::{AsRawSocket, BorrowedSocket, FromRawSocket, OwnedSocket, RawSocket};
use std::sync::OnceLock;

use windows_sys::Win32::Foundation::{SetHandleInformation, HANDLE, HANDLE_FLAG_INHERIT};
use windows_sys::Win32::Networking::WinSock::{
    self as ws, AF_INET, AF_INET6, FIONBIO, IN6_ADDR, IN6_ADDR_0, INVALID_SOCKET, IN_ADDR,
    IN_ADDR_0, IPPROTO_TCP, SIO_BASE_HANDLE, SIO_BSP_HANDLE_POLL, SOCKADDR, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_IN6_0, SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOCK_STREAM, SOL_SOCKET,
    SO_ERROR, WSADATA, WSAEWOULDBLOCK, WSA_FLAG_NO_HANDLE_INHERIT, WSA_FLAG_OVERLAPPED,
};

/// Outcome of a non-blocking [`connect`] call.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectStatus {
    /// `connect` completed synchronously (rare on non-blocking sockets).
    Done,
    /// `connect` returned `WSAEWOULDBLOCK`; caller must wait for write
    /// readiness and check `SO_ERROR`.
    InProgress,
}

fn ensure_wsa_started() -> io::Result<()> {
    static INIT: OnceLock<i32> = OnceLock::new();
    let rc = INIT.get_or_init(|| {
        // SAFETY: WSADATA is a plain POD struct; zero-init is valid.
        let mut data: WSADATA = unsafe { mem::zeroed() };
        // SAFETY: `&mut data` is a valid pointer for the call.
        unsafe { ws::WSAStartup(0x0202, &mut data) }
    });
    if *rc != 0 {
        return Err(io::Error::from_raw_os_error(*rc));
    }
    Ok(())
}

fn wsa_err() -> io::Error {
    // SAFETY: WSAGetLastError is FFI-pure; reads thread-local state.
    let code = unsafe { ws::WSAGetLastError() };
    io::Error::from_raw_os_error(code)
}

// `RawSocket` is `u64`, `SOCKET` is `usize`. On the 64-bit Windows targets
// nanorun supports, the cast is identity; we do not target 32-bit Windows.
#[allow(clippy::cast_possible_truncation)]
fn as_socket(s: BorrowedSocket<'_>) -> SOCKET {
    s.as_raw_socket() as SOCKET
}

// `SOCKET` is `usize`, `HANDLE` is `isize` (windows-sys ≥ 0.46). The
// bitwise value is the kernel handle; the sign reinterpretation is
// harmless for handles produced by winsock.
#[allow(clippy::cast_possible_wrap)]
fn socket_as_handle(raw: SOCKET) -> HANDLE {
    raw as HANDLE
}

fn set_nonblocking(raw: SOCKET) -> io::Result<()> {
    let mut nbio: u32 = 1;
    // SAFETY: `nbio` is a stack u32 the kernel reads as the FIONBIO arg.
    let rc = unsafe { ws::ioctlsocket(raw, FIONBIO, &mut nbio) };
    if rc == SOCKET_ERROR {
        return Err(wsa_err());
    }
    Ok(())
}

/// Create a non-blocking, non-inheritable stream socket of the same family as `addr`.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn stream_socket(addr: SocketAddr) -> io::Result<OwnedSocket> {
    ensure_wsa_started()?;
    let domain: i32 = match addr {
        SocketAddr::V4(_) => i32::from(AF_INET),
        SocketAddr::V6(_) => i32::from(AF_INET6),
    };
    // SAFETY: domain/type/protocol are valid winsock constants; null
    // protocol-info / zero group match the standard creation pattern.
    let raw = unsafe {
        ws::WSASocketW(
            domain,
            SOCK_STREAM,
            IPPROTO_TCP,
            null(),
            0,
            WSA_FLAG_OVERLAPPED | WSA_FLAG_NO_HANDLE_INHERIT,
        )
    };
    if raw == INVALID_SOCKET {
        return Err(wsa_err());
    }
    // SAFETY: kernel returned a fresh socket that we now own.
    let owned = unsafe { OwnedSocket::from_raw_socket(raw as RawSocket) };
    set_nonblocking(raw)?;
    Ok(owned)
}

/// Bind `socket` to `addr`.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn bind(socket: BorrowedSocket<'_>, addr: SocketAddr) -> io::Result<()> {
    let (storage, len) = sockaddr_storage_from(addr);
    // SAFETY: `storage` is a valid sockaddr of length `len`; `socket` is borrowed live.
    let rc = unsafe { ws::bind(as_socket(socket), addr_of!(storage).cast::<SOCKADDR>(), len) };
    if rc == SOCKET_ERROR {
        return Err(wsa_err());
    }
    Ok(())
}

/// Mark `socket` as a passive listening socket.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn listen(socket: BorrowedSocket<'_>, backlog: i32) -> io::Result<()> {
    // SAFETY: `socket` is a valid borrowed socket.
    let rc = unsafe { ws::listen(as_socket(socket), backlog) };
    if rc == SOCKET_ERROR {
        return Err(wsa_err());
    }
    Ok(())
}

/// Accept one pending connection. Returns `Err(WouldBlock)` if none ready.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn accept(socket: BorrowedSocket<'_>) -> io::Result<(OwnedSocket, SocketAddr)> {
    let mut storage: MaybeUninit<SOCKADDR_STORAGE> = MaybeUninit::uninit();
    let storage_size = mem::size_of::<SOCKADDR_STORAGE>();
    let mut len: i32 = i32::try_from(storage_size).unwrap_or(i32::MAX);
    // SAFETY: `storage` is a valid uninitialised SOCKADDR_STORAGE; `len`
    // is its capacity. winsock writes at most `len` bytes and updates
    // `len` to the actual length.
    let raw = unsafe {
        ws::accept(
            as_socket(socket),
            storage.as_mut_ptr().cast::<SOCKADDR>(),
            addr_of_mut!(len),
        )
    };
    if raw == INVALID_SOCKET {
        return Err(wsa_err());
    }
    // SAFETY: kernel returned a fresh socket that we now own.
    let owned = unsafe { OwnedSocket::from_raw_socket(raw as RawSocket) };
    // SAFETY: `raw` is a valid Win32 kernel object handle; clearing the
    // inherit flag matches Linux's accept4(SOCK_CLOEXEC) contract.
    let rc = unsafe { SetHandleInformation(socket_as_handle(raw), HANDLE_FLAG_INHERIT, 0) };
    if rc == 0 {
        return Err(io::Error::last_os_error());
    }
    set_nonblocking(raw)?;
    // SAFETY: accept initialised the prefix of `storage` matching `len`.
    let addr = unsafe { sockaddr_to_socketaddr(storage.as_ptr())? };
    Ok((owned, addr))
}

/// Initiate a non-blocking connect to `addr`.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn connect(socket: BorrowedSocket<'_>, addr: SocketAddr) -> io::Result<ConnectStatus> {
    let (storage, len) = sockaddr_storage_from(addr);
    // SAFETY: `storage` is a valid sockaddr of length `len`; `socket` is borrowed live.
    let rc = unsafe { ws::connect(as_socket(socket), addr_of!(storage).cast::<SOCKADDR>(), len) };
    if rc == SOCKET_ERROR {
        // SAFETY: WSAGetLastError is FFI-pure; reads thread-local state.
        let code = unsafe { ws::WSAGetLastError() };
        if code == WSAEWOULDBLOCK {
            return Ok(ConnectStatus::InProgress);
        }
        return Err(io::Error::from_raw_os_error(code));
    }
    Ok(ConnectStatus::Done)
}

/// Send up to `buf.len()` bytes. Returns `Err(WouldBlock)` when the kernel would block.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn send(socket: BorrowedSocket<'_>, buf: &[u8]) -> io::Result<usize> {
    let len = i32::try_from(buf.len()).unwrap_or(i32::MAX);
    // SAFETY: `socket` is a valid borrowed socket; `buf` is a valid byte slice.
    let rc = unsafe { ws::send(as_socket(socket), buf.as_ptr(), len, 0) };
    if rc == SOCKET_ERROR {
        return Err(wsa_err());
    }
    Ok(usize::try_from(rc).expect("non-negative send return"))
}

/// Receive up to `buf.len()` bytes. Returns `Err(WouldBlock)` when the kernel would block.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn recv(socket: BorrowedSocket<'_>, buf: &mut [u8]) -> io::Result<usize> {
    let len = i32::try_from(buf.len()).unwrap_or(i32::MAX);
    // SAFETY: `socket` is a valid borrowed socket; `buf` is a valid mutable byte slice.
    let rc = unsafe { ws::recv(as_socket(socket), buf.as_mut_ptr(), len, 0) };
    if rc == SOCKET_ERROR {
        return Err(wsa_err());
    }
    Ok(usize::try_from(rc).expect("non-negative recv return"))
}

/// Read pending socket-level error, clearing it from the kernel.
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn so_error(socket: BorrowedSocket<'_>) -> io::Result<i32> {
    let mut err: i32 = 0;
    let mut len: i32 = i32::try_from(mem::size_of_val(&err)).expect("c_int fits i32");
    // SAFETY: `socket` valid; `err`/`len` are stack slots the kernel writes.
    let rc = unsafe {
        ws::getsockopt(
            as_socket(socket),
            SOL_SOCKET,
            SO_ERROR,
            addr_of_mut!(err).cast::<u8>(),
            addr_of_mut!(len),
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_err());
    }
    Ok(err)
}

/// Resolve the base (bottom-of-LSP-stack) socket handle for `socket`.
///
/// Tries `SIO_BASE_HANDLE` first; if the kernel rejects it (Windows 10
/// 1903+ disables it for security, returning `WSAEINVAL`), falls back
/// to iterative `SIO_BSP_HANDLE_POLL` until the chain stops changing.
/// This is the wepoll/mio approach for AFD-poll on modern Windows.
#[allow(dead_code)] // wired by slice 3b (Windows reactor::register)
pub(crate) fn base_socket(socket: BorrowedSocket<'_>) -> io::Result<SOCKET> {
    let raw = as_socket(socket);
    if let Some(base) = try_base_socket(raw, SIO_BASE_HANDLE)? {
        return Ok(base);
    }
    let mut base = raw;
    loop {
        match try_base_socket(base, SIO_BSP_HANDLE_POLL)? {
            Some(next) if next != base => base = next,
            _ => break,
        }
    }
    if base == 0 || base == INVALID_SOCKET {
        return Err(io::Error::other(
            "base socket lookup returned invalid handle",
        ));
    }
    Ok(base)
}

/// Issue one `WSAIoctl(code)` and return the resolved base socket.
/// Returns `Ok(None)` if the kernel rejects the IOCTL (`WSAEINVAL` /
/// `WSAEOPNOTSUPP`) — the caller falls back to a different IOCTL.
fn try_base_socket(socket: SOCKET, code: u32) -> io::Result<Option<SOCKET>> {
    let mut base: SOCKET = 0;
    let mut bytes_returned: u32 = 0;
    #[allow(clippy::cast_possible_truncation)]
    let out_size = mem::size_of::<SOCKET>() as u32;
    // SAFETY: `socket` is a valid socket; `base` and `bytes_returned`
    // are stack slots the kernel writes; null overlapped/routine forces
    // synchronous behaviour for the queried IOCTLs.
    let rc = unsafe {
        ws::WSAIoctl(
            socket,
            code,
            core::ptr::null(),
            0,
            addr_of_mut!(base).cast::<core::ffi::c_void>(),
            out_size,
            addr_of_mut!(bytes_returned),
            core::ptr::null_mut(),
            None,
        )
    };
    if rc == SOCKET_ERROR {
        // SAFETY: WSAGetLastError is FFI-pure.
        let code = unsafe { ws::WSAGetLastError() };
        if code == ws::WSAEINVAL || code == ws::WSAEOPNOTSUPP {
            return Ok(None);
        }
        return Err(io::Error::from_raw_os_error(code));
    }
    Ok(Some(base))
}

/// Read the local address of `socket` (post-bind for listeners, peer-known for connected streams).
#[allow(dead_code)] // wired by Phase 4 (Windows TcpStream)
pub(crate) fn local_addr(socket: BorrowedSocket<'_>) -> io::Result<SocketAddr> {
    let mut storage: MaybeUninit<SOCKADDR_STORAGE> = MaybeUninit::uninit();
    let storage_size = mem::size_of::<SOCKADDR_STORAGE>();
    let mut len: i32 = i32::try_from(storage_size).unwrap_or(i32::MAX);
    // SAFETY: `socket` valid; `storage`/`len` are stack slots the kernel fills.
    let rc = unsafe {
        ws::getsockname(
            as_socket(socket),
            storage.as_mut_ptr().cast::<SOCKADDR>(),
            addr_of_mut!(len),
        )
    };
    if rc == SOCKET_ERROR {
        return Err(wsa_err());
    }
    // SAFETY: kernel populated `storage` to `len` bytes.
    unsafe { sockaddr_to_socketaddr(storage.as_ptr()) }
}

fn sockaddr_storage_from(addr: SocketAddr) -> (SOCKADDR_STORAGE, i32) {
    // SAFETY: SOCKADDR_STORAGE is a plain POD struct; zero is valid.
    let mut storage: SOCKADDR_STORAGE = unsafe { mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(v4) => write_sockaddr_in(&mut storage, v4),
        SocketAddr::V6(v6) => write_sockaddr_in6(&mut storage, v6),
    };
    (storage, len)
}

fn write_sockaddr_in(storage: &mut SOCKADDR_STORAGE, addr: SocketAddrV4) -> i32 {
    // SAFETY: SOCKADDR_IN is POD; zero-init is valid.
    let mut sin: SOCKADDR_IN = unsafe { mem::zeroed() };
    sin.sin_family = AF_INET;
    sin.sin_port = addr.port().to_be();
    sin.sin_addr = IN_ADDR {
        S_un: IN_ADDR_0 {
            S_addr: u32::from_ne_bytes(addr.ip().octets()),
        },
    };
    // SAFETY: SOCKADDR_IN fits within SOCKADDR_STORAGE; bytewise copy is sound.
    unsafe {
        core::ptr::copy_nonoverlapping(
            addr_of!(sin).cast::<u8>(),
            (storage as *mut SOCKADDR_STORAGE).cast::<u8>(),
            mem::size_of::<SOCKADDR_IN>(),
        );
    }
    i32::try_from(mem::size_of::<SOCKADDR_IN>()).expect("SOCKADDR_IN fits i32")
}

fn write_sockaddr_in6(storage: &mut SOCKADDR_STORAGE, addr: SocketAddrV6) -> i32 {
    // SAFETY: SOCKADDR_IN6 is POD; zero-init is valid.
    let mut sin6: SOCKADDR_IN6 = unsafe { mem::zeroed() };
    sin6.sin6_family = AF_INET6;
    sin6.sin6_port = addr.port().to_be();
    sin6.sin6_flowinfo = addr.flowinfo();
    sin6.sin6_addr = IN6_ADDR {
        u: IN6_ADDR_0 {
            Byte: addr.ip().octets(),
        },
    };
    sin6.Anonymous = SOCKADDR_IN6_0 {
        sin6_scope_id: addr.scope_id(),
    };
    // SAFETY: SOCKADDR_IN6 fits within SOCKADDR_STORAGE; bytewise copy is sound.
    unsafe {
        core::ptr::copy_nonoverlapping(
            addr_of!(sin6).cast::<u8>(),
            (storage as *mut SOCKADDR_STORAGE).cast::<u8>(),
            mem::size_of::<SOCKADDR_IN6>(),
        );
    }
    i32::try_from(mem::size_of::<SOCKADDR_IN6>()).expect("SOCKADDR_IN6 fits i32")
}

unsafe fn sockaddr_to_socketaddr(storage: *const SOCKADDR_STORAGE) -> io::Result<SocketAddr> {
    // SAFETY: caller guarantees `storage` is initialised and points at a
    // SOCKADDR_STORAGE; reading ss_family is sound.
    let family = unsafe { (*storage).ss_family };
    if family == AF_INET {
        // SAFETY: AF_INET => SOCKADDR_IN prefix.
        let sin = unsafe { *storage.cast::<SOCKADDR_IN>() };
        // SAFETY: union access — IN_ADDR_0::S_addr is the canonical u32 view
        // populated by `write_sockaddr_in` and the kernel.
        let s_addr = unsafe { sin.sin_addr.S_un.S_addr };
        let octets = u32::to_ne_bytes(s_addr);
        let ip = Ipv4Addr::from(octets);
        let port = u16::from_be(sin.sin_port);
        Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
    } else if family == AF_INET6 {
        // SAFETY: AF_INET6 => SOCKADDR_IN6 prefix.
        let sin6 = unsafe { *storage.cast::<SOCKADDR_IN6>() };
        // SAFETY: union access — IN6_ADDR_0::Byte is the canonical [u8;16] view.
        let octets = unsafe { sin6.sin6_addr.u.Byte };
        let ip = Ipv6Addr::from(octets);
        let port = u16::from_be(sin6.sin6_port);
        // SAFETY: union access — sin6_scope_id is the canonical u32 view.
        let scope_id = unsafe { sin6.Anonymous.sin6_scope_id };
        Ok(SocketAddr::V6(SocketAddrV6::new(
            ip,
            port,
            sin6.sin6_flowinfo,
            scope_id,
        )))
    } else {
        Err(io::Error::other(format!(
            "unknown socket address family: {family}"
        )))
    }
}
