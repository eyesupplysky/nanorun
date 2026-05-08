# nanorun

A from-scratch async runtime in Rust, written as a clean reference implementation in the spirit of [`mio`](https://github.com/tokio-rs/mio) plus a stripped-down [`tokio`](https://github.com/tokio-rs/tokio).

Hand-rolled wakers, a work-stealing multi-worker executor, a hierarchical timer wheel, and per-OS reactors (epoll on Linux, IOCP + AFD-poll on Windows) — every layer written for readability first.

## Features

- **Multi-threaded executor.** Per-worker LIFO local queues, a shared injector, work-stealing across peers, periodic injector-fairness checks, idle workers parking through the reactor.
- **`Runtime` + `Handle` + `spawn` + `JoinHandle`.** Construct a runtime, spawn `Send + 'static` futures from any thread, await them through a join handle.
- **Async TCP.** `TcpListener` and `TcpStream` over the running reactor on Linux and Windows.
- **Timers.** `sleep` and `timeout` backed by a hierarchical timer wheel (6 levels × 64 slots × 1 ms; ~2.2-year range, O(1) insert).
- **Cooperative yield.** `yield_now` for CPU-bound loops on a worker.

No external runtime dependencies — `tokio`, `mio`, `smol` are not pulled in. Linux uses `libc`; Windows uses `windows-sys`. Nothing else.

The API is pre-1.0 and may change.

## Examples

### Block on a future

```rust
let value = nanorun::block_on(async { 1 + 2 });
assert_eq!(value, 3);
```

Or via a `Runtime` you keep around:

```rust
use nanorun::Runtime;

let rt = Runtime::new();
let value = rt.block_on(async { 40 + 2 });
assert_eq!(value, 42);
```

`Runtime::new()` sizes itself to [`std::thread::available_parallelism`]; use `Runtime::with_workers(n)` to pin the worker count.

### Spawn

```rust
let rt = nanorun::Runtime::new();
rt.block_on(async {
    let h1 = nanorun::spawn(async { 21 + 21 });
    let h2 = nanorun::spawn(async { 100 - 58 });
    assert_eq!(h1.await + h2.await, 84);
});
```

`nanorun::spawn` reads the per-worker `Handle` thread-local installed by the runtime, so it must be called from inside a future polled by the runtime. To spawn from an off-runtime thread, clone a `Handle` first:

```rust
let rt = nanorun::Runtime::new();
let handle = rt.handle();
let h = handle.spawn(async { 7 * 6 });
assert_eq!(rt.block_on(async { h.await }), 42);
```

### Sleep and timeout

```rust
use core::time::Duration;

nanorun::block_on(async {
    nanorun::time::sleep(Duration::from_millis(10)).await;

    let ok = nanorun::time::timeout(
        Duration::from_secs(1),
        async { 42 },
    ).await;
    assert_eq!(ok, Ok(42));

    let err = nanorun::time::timeout(
        Duration::from_millis(10),
        nanorun::time::sleep(Duration::from_secs(60)),
    ).await;
    assert!(err.is_err());
});
```

### Echo over TCP

```rust,no_run
use nanorun::net::{TcpListener, TcpStream};

nanorun::block_on(async {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();

    let server = nanorun::spawn(async move {
        let (s, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 5];
        s.read(&mut buf).await.unwrap();
        s.write(&buf).await.unwrap();
    });

    let s = TcpStream::connect(addr).await.unwrap();
    s.write(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    s.read(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    server.await;
});
```

## Architecture

The runtime is layered top-down. Each layer is a few hundred lines and worth reading in order: `lib.rs` → `executor/` → `reactor/` → `time/` → `task/`.

### Executor (`src/executor/`)

A multi-worker driver. Spawns *N* worker threads (default: `available_parallelism`) that compete over runnable tasks via three queue layers:

1. **Per-worker local queue** — LIFO `VecDeque`, hot in cache, scanned first.
2. **Shared injector** — global queue. Every wake/spawn pushes here.
3. **Work-stealing** — when both above are empty, steal half of a randomly chosen peer's local queue (FIFO from the peer's front).

Every `POLL_BUDGET` ticks (61, matching tokio) a worker checks the injector first regardless of local work, so the global queue can't starve while local work churns.

When a worker finds nothing to do, exactly one of them takes the **driver token** via CAS and parks in `Reactor::poll`. The others `thread::park`. Wakes both push onto the injector and unpark a worker; they only fire `ReactorHandle::wake` when the driver is currently parked. The SeqCst fence pair around the `driver_parked` flag closes the lost-wakeup window.

### Reactor (`src/reactor/`)

Per-OS backends behind a small platform-agnostic surface (`Token`, `Direction`, `Interest`, `Reactor`, `ReactorHandle`):

- **Linux** — `epoll` for fd readiness, `eventfd` for cross-thread wakeups. The kernel-side token is a `u64`: token `0` is the self-wake eventfd, non-zero tokens are slab keys + 1.
- **Windows** — IOCP for parking + cross-thread posts; AFD-poll IOCTLs for socket readiness. Each registered socket carries a stable `Box<WindowsSlotState>` whose first field is the `IoStatusBlock` the kernel returns in `lpOverlapped`, recovered to a slab key via `repr(C)` layout. AFD-poll is one-shot, so the dispatch loop re-arms after every completion to give epoll-style level-triggered semantics.
- **Fallback** — for any other target, an in-process `Mutex` + `Condvar` notifier that supports cross-thread wakeups but no real I/O. `net` is gated off these targets.

### Timer driver (`src/time/`)

One driver lives per runtime. Workers install a thread-local pointer to it; `sleep` futures register their wakers through that pointer. After each `Reactor::poll` return the driver-worker advances the wheel and fires every elapsed waker.

The wheel is the canonical Varghese-Lauck (1987) hashed-and-hierarchical design: **6 levels × 64 slots × 1 ms tick**, total range ≈ 2.18 years, O(1) insert. On each tick that crosses a `64^k` boundary, level `k` cascades — the now-current slot is drained and each entry re-inserted, landing it in a lower level. `timeout` composes a `Sleep` with the inner future and polls the inner first on every wake, so a future that becomes ready on the same wake as the deadline still resolves to `Ok`.

### Task layout (`src/task/raw.rs`)

A spawned task is a single heap allocation laid out as `RawTask<F, S>` with `#[repr(C)]` and a `Header` at offset `0`. The header carries the lock-free state machine, a per-monomorphization vtable, and the `JoinHandle` waker slot. The future and schedule callback live alongside it.

Worker queues, `RawWaker` data pointers, and `JoinHandle` all hold a thin `*const Header` and recover the typed allocation through the vtable's six entries (`poll`, `schedule`, `clone_ref`, `drop_ref`, `try_take_output`, `drop_join_interest`). Reference counting rides on `Arc<RawTask<F, S>>`; the type-erased call sites bump and release strong counts via `clone_ref` / `drop_ref` without ever knowing `F` or `S`.

State bits (`NOTIFIED`, `RUNNING`, `COMPLETE`, `JOIN_INTEREST`, `JOIN_WAKER`) form a small AcqRel state machine that serializes the unlocked output `Stage` cell. A task is born `NOTIFIED`; the worker that pops it transitions `NOTIFIED → RUNNING` and clears `NOTIFIED` before polling. Wakes that fire while `RUNNING` re-set `NOTIFIED` so the worker reschedules itself when poll returns `Pending`. On `Ready`, the worker writes the output into the `Stage` cell, sets `COMPLETE`, and wakes the parked `JoinHandle`.

### Waker

The `RawWaker` data pointer of every waker the runtime produces is a thin `*const Header` from `TaskRef::into_raw`. Cloning bumps the task's strong refcount via the header's vtable; `wake()` invokes the same vtable's `schedule` entry, which sets `NOTIFIED` and (if previously idle) pushes the task onto the injector.

### `sys`

The only place raw `libc` / `windows-sys` calls live. The Linux backend lives under `sys/linux/` (`epoll`, `eventfd`, socket wrappers); the Windows backend lives under `sys/windows/` (`socket`, `iocp`, `afd`). Adding a new backend means writing one `sys/<os>/` directory and one `reactor/<os>.rs` — nothing else in the crate needs to know.

## Requirements

- Rust **stable**, MSRV **1.75**.
- No external runtime dependencies (`tokio`, `mio`, `smol` are not pulled in). Linux uses `libc`; Windows uses `windows-sys`.
- Cross-platform: Linux and Windows have native fd-readiness reactors; macOS and other targets get an in-process fallback (cross-thread wakeups only, no async I/O).

## License

MIT. See [LICENSE](LICENSE).
