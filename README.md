# nanorun

A from-scratch async runtime in Rust, written as a clean reference implementation in the spirit of [`mio`](https://github.com/tokio-rs/mio) plus a stripped-down [`tokio`](https://github.com/tokio-rs/tokio).

## Status

Pre-1.0. Currently at **M1**: a single-threaded `block_on` driven by a hand-rolled `RawWakerVTable` over `std::thread::park` / `Thread::unpark`. No I/O, no `spawn`, no timers yet.

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
- [ ] **M2** — Reactor (Linux first): `epoll`-backed reactor that registers fd interest, parks tasks, wakes on readiness. `TcpListener` + `TcpStream` async wrappers. Echo server.
- [ ] **M3** — Multi-threaded executor: per-worker schedulers, fairness, `spawn` returning a `JoinHandle`.
- [ ] **M4** — Timers: hierarchical timer wheel, `sleep` and `timeout`. Sub-microsecond timer dispatch overhead.
- [ ] **M5** — Windows reactor: IOCP backend behind the same trait the epoll backend uses. CI runs the echo-server test on both OSes.
- [ ] **M6** — Polish: bench suite (per-task overhead in ns, spawn throughput, syscall round-trip), public docs (architecture, lifecycle of a task, picking your reactor).

## Design notes

The runtime targets readability first. The current M1 surface is two small files: a hand-rolled `RawWakerVTable` whose data pointer is an `Arc<std::thread::Thread>`, and a polling loop that stack-pins the future via `core::pin::pin!` and calls `thread::park` on `Pending`. No external dependencies, no syscalls, no allocation in the hot path beyond the one `Arc` clone per `block_on` call.

Architectural docs (lifecycle of a task, reactor backend selection) ship at M6.

## Requirements

- Rust **stable**, MSRV **1.75**.
- No external runtime dependencies (`tokio`, `mio`, `smol` are not pulled in).
- Cross-platform: Linux, macOS, Windows. CI runs all three.

## License

MIT. See [LICENSE](LICENSE).
