//! Async networking primitives.
//!
//! M2 ships TCP only, Linux only. The Windows backend lands at M5
//! alongside the IOCP reactor. The interface modelled here mirrors
//! `std::net` so the async-vs-sync diff is just the `await`.

mod tcp;

pub use tcp::{TcpListener, TcpStream};
