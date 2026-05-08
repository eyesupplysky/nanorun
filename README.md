# nanorun

A from-scratch async runtime in Rust, written as a clean reference implementation in the spirit of [`mio`](https://github.com/tokio-rs/mio) plus a stripped-down [`tokio`](https://github.com/tokio-rs/tokio).

## Status

Pre-1.0. Currently at **M2**: an epoll-backed reactor parks the executor when futures return `Pending`, and `nanorun::net::TcpListener` / `TcpStream` provide async TCP on Linux. Cross-thread wakeups go through an `eventfd` registered in the reactor. No `spawn` and no timers yet.

## Example

```rust
let value = nanorun::block_on(async { 1 + 2 });
assert_eq!(value, 3);
```

Or via a `Runtime`:

```rust
use nanorun::Runtime;

let rt = Runtime::new();
let value = rt.block_on(async { 40 + 2 });
assert_eq!(value, 42);
```

## Milestones

- [x] **M1** — Single-threaded executor: `Future` polling loop, hand-written `Waker` + `RawWakerVTable`, `block_on`-style entry point. No I/O.
- [x] **M2** — Reactor (Linux first): `epoll`-backed reactor that registers fd interest, parks tasks, wakes on readiness. `TcpListener` + `TcpStream` async wrappers. Echo server.
- [ ] **M3** — Multi-threaded executor: per-worker schedulers, fairness, `spawn` returning a `JoinHandle`.
- [ ] **M4** — Timers: hierarchical timer wheel, `sleep` and `timeout`. Sub-microsecond timer dispatch overhead.
- [ ] **M5** — Windows reactor: IOCP backend behind the same trait the epoll backend uses. CI runs the echo-server test on both OSes.
- [ ] **M6** — Polish: bench suite (per-task overhead in ns, spawn throughput, syscall round-trip), public docs (architecture, lifecycle of a task, picking your reactor).

## Design notes

The runtime targets readability first. The M2 surface composes three pieces:

- A hand-rolled `RawWakerVTable` whose data pointer is an `Arc<ReactorHandle>`. `wake` writes `1u64` to the reactor's permanent `eventfd`, breaking the executor out of `epoll_wait`.
- A reactor (`src/reactor/`) that owns one `epoll` fd plus the self-wake `eventfd` and a slab of registered wakers keyed by token.
- A polling loop (`src/executor/single.rs`) that stack-pins the future via `core::pin::pin!`, polls it once, and on `Pending` blocks in `Reactor::poll` until either fd readiness fires registered wakers or a cross-thread wake fires the eventfd.

Async TCP types (`nanorun::net::TcpListener`, `nanorun::net::TcpStream`) are non-blocking sockets registered with the current reactor via a thread-local context guard installed by `block_on`. Each I/O method is an `async fn` that loops the underlying syscall: a `WouldBlock` error parks the future on level-triggered readiness; everything else propagates.

Architectural docs (lifecycle of a task, reactor backend selection) ship at M6.

## Example: echo over TCP (Linux)

```rust,no_run
use nanorun::net::{TcpListener, TcpStream};

nanorun::block_on(async {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    // Drive both sides on separate threads — `spawn` lands at M3.
    std::thread::spawn(move || nanorun::block_on(async move {
        let (s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 5];
        s.read(&mut buf).await.unwrap();
        s.write(&buf).await.unwrap();
    }));
    let s = TcpStream::connect(addr).await.unwrap();
    s.write(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    s.read(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");
});
```

## Requirements

- Rust **stable**, MSRV **1.75**.
- No external runtime dependencies (`tokio`, `mio`, `smol` are not pulled in). The Linux build uses `libc` for direct kernel calls.
- Cross-platform: Linux, macOS, Windows. CI runs all three. M2 networking is Linux-only; macOS and Windows get a thread-park fallback that supports the cross-thread waker contract until M5 lands native IOCP / kqueue.

## License

MIT. See [LICENSE](LICENSE).
