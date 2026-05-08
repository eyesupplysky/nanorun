//! `socket`/`bind`/`listen`/`accept4`/`connect`/`send`/`recv` thin wrappers.
//!
//! All sockets are created `SOCK_NONBLOCK | SOCK_CLOEXEC`. Address
//! conversion goes through the libc `sockaddr_in` / `sockaddr_in6`
//! shapes; we never leak `libc::sockaddr_storage` out of this module.

use core::mem::{self, MaybeUninit};
use core::ptr::{addr_of, addr_of_mut};
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};

/// Outcome of a non-blocking [`connect`] call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectStatus {
    /// `connect` completed synchronously (rare on non-blocking sockets).
    Done,
    /// `connect` returned `EINPROGRESS`; caller must wait for write readiness
    /// and check `SO_ERROR`.
    InProgress,
}

/// Create a non-blocking, CLOEXEC stream socket of the same family as `addr`.
pub(crate) fn stream_socket(addr: SocketAddr) -> io::Result<OwnedFd> {
    let domain = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    // SAFETY: domain/type/protocol are valid kernel constants.
    let raw = unsafe {
        libc::socket(
            domain,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: kernel returned a fresh fd that we now own.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// Bind `fd` to `addr`.
pub(crate) fn bind(fd: BorrowedFd<'_>, addr: SocketAddr) -> io::Result<()> {
    let (storage, len) = sockaddr_storage_from(addr);
    // SAFETY: `storage` is a valid sockaddr of length `len`; `fd` is borrowed live.
    let rc = unsafe {
        libc::bind(
            fd.as_raw_fd(),
            addr_of!(storage).cast::<libc::sockaddr>(),
            len,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Mark `fd` as a passive listening socket.
pub(crate) fn listen(fd: BorrowedFd<'_>, backlog: i32) -> io::Result<()> {
    // SAFETY: `fd` is a valid borrowed file descriptor.
    let rc = unsafe { libc::listen(fd.as_raw_fd(), backlog) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Accept one pending connection. Returns `Err(WouldBlock)` if none ready.
pub(crate) fn accept(fd: BorrowedFd<'_>) -> io::Result<(OwnedFd, SocketAddr)> {
    let mut storage: MaybeUninit<libc::sockaddr_storage> = MaybeUninit::uninit();
    let storage_size = mem::size_of::<libc::sockaddr_storage>();
    let mut len: libc::socklen_t = u32::try_from(storage_size).unwrap_or(u32::MAX);
    // SAFETY: `storage` is a valid uninitialised sockaddr_storage; `len`
    // is its capacity. The kernel writes at most `*len` bytes and updates
    // `len` to the actual length.
    let raw = unsafe {
        libc::accept4(
            fd.as_raw_fd(),
            storage.as_mut_ptr().cast::<libc::sockaddr>(),
            addr_of_mut!(len),
            libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
        )
    };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `accept4` initialised the prefix of `storage` matching `len`.
    let addr = unsafe { sockaddr_to_socketaddr(storage.as_ptr())? };
    // SAFETY: kernel returned a fresh fd that we now own.
    Ok((unsafe { OwnedFd::from_raw_fd(raw) }, addr))
}

/// Initiate a non-blocking connect to `addr`.
pub(crate) fn connect(fd: BorrowedFd<'_>, addr: SocketAddr) -> io::Result<ConnectStatus> {
    let (storage, len) = sockaddr_storage_from(addr);
    // SAFETY: `storage` is a valid sockaddr of length `len`; `fd` is borrowed live.
    let rc = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            addr_of!(storage).cast::<libc::sockaddr>(),
            len,
        )
    };
    if rc < 0 {
        let e = io::Error::last_os_error();
        if e.raw_os_error() == Some(libc::EINPROGRESS) {
            return Ok(ConnectStatus::InProgress);
        }
        return Err(e);
    }
    Ok(ConnectStatus::Done)
}

/// Send up to `buf.len()` bytes. Returns `Err(WouldBlock)` when the kernel would block.
pub(crate) fn send(fd: BorrowedFd<'_>, buf: &[u8]) -> io::Result<usize> {
    // SAFETY: `fd` is a valid borrowed fd; `buf` is a valid byte slice.
    let rc = unsafe {
        libc::send(
            fd.as_raw_fd(),
            buf.as_ptr().cast::<libc::c_void>(),
            buf.len(),
            libc::MSG_NOSIGNAL,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(usize::try_from(rc).expect("non-negative send return"))
}

/// Receive up to `buf.len()` bytes. Returns `Err(WouldBlock)` when the kernel would block.
pub(crate) fn recv(fd: BorrowedFd<'_>, buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: `fd` is a valid borrowed fd; `buf` is a valid mutable byte slice.
    let rc = unsafe {
        libc::recv(
            fd.as_raw_fd(),
            buf.as_mut_ptr().cast::<libc::c_void>(),
            buf.len(),
            0,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(usize::try_from(rc).expect("non-negative recv return"))
}

/// Read pending socket-level error, clearing it from the kernel.
pub(crate) fn so_error(fd: BorrowedFd<'_>) -> io::Result<i32> {
    let mut err: libc::c_int = 0;
    let mut len: libc::socklen_t = u32::try_from(mem::size_of_val(&err)).expect("c_int fits u32");
    // SAFETY: `fd` is valid; `err`/`len` are stack slots the kernel writes.
    let rc = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            addr_of_mut!(err).cast::<libc::c_void>(),
            addr_of_mut!(len),
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(err)
}

/// Read the local address of `fd` (post-bind for listeners, peer-known for connected streams).
pub(crate) fn local_addr(fd: BorrowedFd<'_>) -> io::Result<SocketAddr> {
    let mut storage: MaybeUninit<libc::sockaddr_storage> = MaybeUninit::uninit();
    let storage_size = mem::size_of::<libc::sockaddr_storage>();
    let mut len: libc::socklen_t = u32::try_from(storage_size).unwrap_or(u32::MAX);
    // SAFETY: `fd` valid; `storage`/`len` stack slots the kernel fills.
    let rc = unsafe {
        libc::getsockname(
            fd.as_raw_fd(),
            storage.as_mut_ptr().cast::<libc::sockaddr>(),
            addr_of_mut!(len),
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: kernel populated `storage` to `len` bytes.
    unsafe { sockaddr_to_socketaddr(storage.as_ptr()) }
}

#[allow(clippy::cast_possible_truncation)] // sin_family/sin6_family fit u16 by spec
fn sockaddr_storage_from(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: sockaddr_storage is a plain POD struct; zero is a valid init.
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let len = match addr {
        SocketAddr::V4(v4) => write_sockaddr_in(&mut storage, v4),
        SocketAddr::V6(v6) => write_sockaddr_in6(&mut storage, v6),
    };
    (storage, len)
}

fn write_sockaddr_in(storage: &mut libc::sockaddr_storage, addr: SocketAddrV4) -> libc::socklen_t {
    let sin = libc::sockaddr_in {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: addr.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(addr.ip().octets()),
        },
        sin_zero: [0; 8],
    };
    // SAFETY: sockaddr_in fits within sockaddr_storage; bytewise copy is sound.
    unsafe {
        core::ptr::copy_nonoverlapping(
            addr_of!(sin).cast::<u8>(),
            (storage as *mut libc::sockaddr_storage).cast::<u8>(),
            mem::size_of::<libc::sockaddr_in>(),
        );
    }
    u32::try_from(mem::size_of::<libc::sockaddr_in>()).expect("sockaddr_in fits u32")
}

fn write_sockaddr_in6(storage: &mut libc::sockaddr_storage, addr: SocketAddrV6) -> libc::socklen_t {
    let sin6 = libc::sockaddr_in6 {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        sin6_family: libc::AF_INET6 as libc::sa_family_t,
        sin6_port: addr.port().to_be(),
        sin6_flowinfo: addr.flowinfo(),
        sin6_addr: libc::in6_addr {
            s6_addr: addr.ip().octets(),
        },
        sin6_scope_id: addr.scope_id(),
    };
    // SAFETY: sockaddr_in6 fits within sockaddr_storage; bytewise copy is sound.
    unsafe {
        core::ptr::copy_nonoverlapping(
            addr_of!(sin6).cast::<u8>(),
            (storage as *mut libc::sockaddr_storage).cast::<u8>(),
            mem::size_of::<libc::sockaddr_in6>(),
        );
    }
    u32::try_from(mem::size_of::<libc::sockaddr_in6>()).expect("sockaddr_in6 fits u32")
}

unsafe fn sockaddr_to_socketaddr(storage: *const libc::sockaddr_storage) -> io::Result<SocketAddr> {
    // SAFETY: caller guarantees `storage` is initialised and points at a
    // sockaddr_storage; reading ss_family is sound.
    let family = i32::from(unsafe { (*storage).ss_family });
    match family {
        libc::AF_INET => {
            // SAFETY: the kernel reported AF_INET, so `storage` actually
            // holds a sockaddr_in in its prefix.
            let sin = unsafe { *storage.cast::<libc::sockaddr_in>() };
            let octets = u32::to_ne_bytes(sin.sin_addr.s_addr);
            let ip = Ipv4Addr::from(octets);
            let port = u16::from_be(sin.sin_port);
            Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        libc::AF_INET6 => {
            // SAFETY: AF_INET6 ⇒ sockaddr_in6 prefix.
            let sin6 = unsafe { *storage.cast::<libc::sockaddr_in6>() };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                port,
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        other => Err(io::Error::other(format!(
            "unknown socket address family: {other}"
        ))),
    }
}
