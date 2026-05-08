//! Async `TcpListener` and `TcpStream` over the epoll reactor.
//!
//! Each socket is created `SOCK_NONBLOCK | SOCK_CLOEXEC` and registered
//! with the current runtime's reactor on construction. I/O methods loop
//! on the underlying syscall: a `WouldBlock` error parks the future in
//! the reactor (level-triggered, so a re-poll after readiness fires is
//! always sufficient) and any other error propagates.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsFd, OwnedFd};

use crate::reactor::{Direction, Interest, Token};
use crate::runtime::context::{try_with_current, with_current};
use crate::sys::linux::socket as sys;


/// Asynchronous TCP listener.
#[derive(Debug)]
pub struct TcpListener {
    fd: OwnedFd,
    token: Token,
}

/// Asynchronous TCP stream.
#[derive(Debug)]
pub struct TcpStream {
    fd: OwnedFd,
    token: Token,
}

impl TcpListener {
    /// Bind a listening socket to `addr`.
    ///
    /// Synchronous because there is no I/O to wait for — only `socket`,
    /// `bind`, `listen`, and reactor registration. The function still
    /// returns `io::Result` because each step can fail.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let fd = sys::stream_socket(addr)?;
        sys::bind(fd.as_fd(), addr)?;
        sys::listen(fd.as_fd(), 1024)?;
        let token = with_current(|r| r.register(fd.as_fd(), Interest::READ))?;
        Ok(Self { fd, token })
    }

    /// The bound local address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        sys::local_addr(self.fd.as_fd())
    }

    /// Wait for and accept one incoming connection.
    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        loop {
            match sys::accept(self.fd.as_fd()) {
                Ok((fd, addr)) => {
                    let token = with_current(|r| r.register(fd.as_fd(), Interest::READ_WRITE))?;
                    return Ok((TcpStream { fd, token }, addr));
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            ReadyFor::new(self.token, Direction::Read).await;
        }
    }
}

impl TcpStream {
    /// Open a TCP connection to `addr`.
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let fd = sys::stream_socket(addr)?;
        let status = sys::connect(fd.as_fd(), addr)?;
        let token = with_current(|r| r.register(fd.as_fd(), Interest::READ_WRITE))?;
        let stream = Self { fd, token };
        if let sys::ConnectStatus::InProgress = status {
            ReadyFor::new(stream.token, Direction::Write).await;
            let err = sys::so_error(stream.fd.as_fd())?;
            if err != 0 {
                return Err(io::Error::from_raw_os_error(err));
            }
        }
        Ok(stream)
    }

    /// Read up to `buf.len()` bytes. Returns `Ok(0)` on a graceful peer close.
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match sys::recv(self.fd.as_fd(), buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            ReadyFor::new(self.token, Direction::Read).await;
        }
    }

    /// Write up to `buf.len()` bytes. May write fewer; the caller drives the loop.
    pub async fn write(&self, buf: &[u8]) -> io::Result<usize> {
        loop {
            match sys::send(self.fd.as_fd(), buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
            ReadyFor::new(self.token, Direction::Write).await;
        }
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        deregister_best_effort(self.fd.as_fd(), self.token);
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        deregister_best_effort(self.fd.as_fd(), self.token);
    }
}

// Best-effort deregister called from Drop. If the runtime is no longer
// installed on this thread (e.g. block_on already returned), there is
// no reactor to touch — closing the OwnedFd via Drop is sufficient,
// and the kernel removes any leftover epoll registration when the fd
// closes. The Result inside is intentionally discarded: Drop cannot
// propagate, and a failed deregister does not invalidate the close.
fn deregister_best_effort(fd: std::os::fd::BorrowedFd<'_>, token: Token) {
    try_with_current(|r| {
        let _ = r.deregister(fd, token);
    });
}

struct ReadyFor {
    token: Token,
    dir: Direction,
    polled: bool,
}

impl ReadyFor {
    fn new(token: Token, dir: Direction) -> Self {
        Self {
            token,
            dir,
            polled: false,
        }
    }
}

impl Future for ReadyFor {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            with_current(|r| r.set_waker(self.token, self.dir, cx.waker()));
            Poll::Pending
        }
    }
}
