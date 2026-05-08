//! Echo-server integration test (M2, Linux-only).
//!
//! Two `block_on`s on two threads — the server binds on `127.0.0.1:0`,
//! accepts one connection, reads exactly N bytes, writes them back, and
//! exits. The client connects, writes the same N bytes, reads them
//! back, and asserts the round-trip matches. `spawn` lands at M3, so
//! we use `std::thread` for the server side.

#![cfg(target_os = "linux")]

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use nanorun::net::{TcpListener, TcpStream};

const PAYLOAD: &[u8] = b"hello, nanorun!";

async fn read_exact(s: &TcpStream, buf: &mut [u8]) -> io::Result<()> {
    let mut got = 0;
    while got < buf.len() {
        let n = s.read(&mut buf[got..]).await?;
        if n == 0 {
            return Err(io::Error::other("unexpected EOF before exact read"));
        }
        got += n;
    }
    Ok(())
}

async fn write_all(s: &TcpStream, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let n = s.write(buf).await?;
        if n == 0 {
            return Err(io::Error::other("write returned 0"));
        }
        buf = &buf[n..];
    }
    Ok(())
}

#[test]
fn echo_round_trip_over_loopback() {
    let bound: Arc<(Mutex<Option<SocketAddr>>, Condvar)> =
        Arc::new((Mutex::new(None), Condvar::new()));
    let bound_for_server = Arc::clone(&bound);

    let server = thread::spawn(move || {
        nanorun::block_on(async move {
            let any: SocketAddr = "127.0.0.1:0".parse().expect("parse");
            let listener = TcpListener::bind(any).expect("bind");
            let local = listener.local_addr().expect("local_addr");

            // Hand the bound address to the client thread.
            {
                let (lock, cvar) = &*bound_for_server;
                *lock.lock().expect("server addr slot") = Some(local);
                cvar.notify_all();
            }

            let (stream, _peer) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; PAYLOAD.len()];
            read_exact(&stream, &mut buf).await.expect("server read");
            write_all(&stream, &buf).await.expect("server write");
        });
    });

    let server_addr = {
        let (lock, cvar) = &*bound;
        let mut guard = lock.lock().expect("client addr slot");
        while guard.is_none() {
            guard = cvar
                .wait_timeout(guard, Duration::from_secs(5))
                .expect("cvar wait")
                .0;
            assert!(guard.is_some(), "server failed to bind within timeout");
        }
        guard.expect("addr present")
    };

    nanorun::block_on(async move {
        let stream = TcpStream::connect(server_addr).await.expect("connect");
        write_all(&stream, PAYLOAD).await.expect("client write");
        let mut buf = vec![0u8; PAYLOAD.len()];
        read_exact(&stream, &mut buf).await.expect("client read");
        assert_eq!(&buf[..], PAYLOAD);
    });

    server.join().expect("server thread");
}

#[test]
fn connect_to_unbound_port_fails() {
    nanorun::block_on(async {
        // Pick an arbitrary unlikely-to-be-bound port. ECONNREFUSED is the
        // expected outcome for a non-listening loopback target.
        let addr: SocketAddr = "127.0.0.1:1".parse().expect("parse");
        let result = TcpStream::connect(addr).await;
        assert!(result.is_err(), "connect to :1 should fail; got {result:?}");
    });
}
