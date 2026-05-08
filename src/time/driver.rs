//! Timer driver.
//!
//! One driver lives per runtime, owned by
//! [`crate::executor::multi::Shared`]. Workers install a thread-local
//! pointer via [`crate::runtime::context::DriverGuard`]; user-side
//! [`crate::time::sleep`] futures register their wakers through that
//! pointer. The driver-worker advances the wheel after each
//! [`crate::reactor::Reactor::poll`] return, firing every waker whose
//! deadline has elapsed.
//!
//! # Backing store
//!
//! Slice 1 used a `BTreeMap<Instant, Vec<Waker>>`. Slice 3 (this file,
//! current) backs the driver with the hierarchical timer wheel in the
//! sibling `wheel` module, preserving the public `Driver` surface.
//!
//! # Tick translation
//!
//! The wheel works in `u64` ticks of [`TICK`] each. The driver records
//! an [`Instant`] epoch at construction and converts:
//!
//! - `Instant -> tick` for **deadlines**: ceiling division — the
//!   resulting tick is never earlier than the requested instant, so a
//!   sleep never fires before its requested duration.
//! - `Instant -> tick` for **now**: floor division — the cursor is
//!   only advanced when real time has fully passed each 1ms tick.
//! - `tick -> Instant` for the next-deadline report: exact (`epoch +
//!   tick * TICK`).

use core::task::Waker;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use crate::reactor::ReactorHandle;

use super::wheel::{Entry, Wheel};

/// Wheel tick granularity in nanoseconds (1ms). Sub-millisecond sleeps round up.
const TICK_NS: u128 = 1_000_000;

/// Stable handle returned by [`Driver::register`]; the caller passes it
/// back to [`Driver::cancel`] to drop the waker without firing.
pub(crate) type EntryId = u64;

/// Per-runtime timer driver. `Send + Sync` via the inner `Mutex`.
pub(crate) struct Driver {
    inner: Mutex<Inner>,
    reactor_handle: ReactorHandle,
    epoch: Instant,
}

struct Inner {
    wheel: Wheel,
    wakers: HashMap<EntryId, Waker>,
    next_id: EntryId,
}

impl Driver {
    /// Construct a driver tied to `reactor_handle`. The handle is used by
    /// [`Driver::register`] to interrupt the driver-worker when a new
    /// (potentially earlier) deadline is enqueued.
    pub(crate) fn new(reactor_handle: ReactorHandle) -> Self {
        Self {
            inner: Mutex::new(Inner {
                wheel: Wheel::new(),
                wakers: HashMap::new(),
                next_id: 0,
            }),
            reactor_handle,
            epoch: Instant::now(),
        }
    }

    /// Register `waker` to fire at `deadline`. Returns the [`EntryId`]
    /// the caller should pass to [`Driver::cancel`] on early drop.
    ///
    /// Wakes the reactor unconditionally on a successful registration so
    /// the driver-worker re-computes its next timeout. The wake is cheap
    /// (a single eventfd write on Linux) and idempotent under concurrent
    /// calls.
    pub(crate) fn register(&self, deadline: Instant, waker: Waker) -> Option<EntryId> {
        let deadline_tick = instant_to_tick_ceil(self.epoch, deadline);
        // Either we placed the entry and have an `EntryId` to return, or
        // the deadline was already past and we own the waker again so it
        // can be fired outside the lock.
        let outcome: Result<EntryId, Waker> = {
            let mut inner = self.inner.lock().expect("driver poisoned");
            let id = inner.next_id;
            inner.next_id = inner.next_id.wrapping_add(1);
            match inner.wheel.insert(Entry {
                id,
                deadline: deadline_tick,
            }) {
                Ok(()) => {
                    inner.wakers.insert(id, waker);
                    Ok(id)
                }
                Err(_) => Err(waker),
            }
        };
        match outcome {
            Ok(id) => {
                let _ = self.reactor_handle.wake();
                Some(id)
            }
            Err(w) => {
                w.wake();
                None
            }
        }
    }

    /// Drop the waker for `id` without firing. The wheel still holds the
    /// id; on slot drain it will be looked up, found absent from the
    /// waker map, and discarded silently.
    ///
    /// Idempotent: calling `cancel` after the entry has already fired
    /// (and been removed by [`Driver::advance`]) is a cheap no-op.
    pub(crate) fn cancel(&self, id: EntryId) {
        self.inner
            .lock()
            .expect("driver poisoned")
            .wakers
            .remove(&id);
    }

    /// Earliest pending deadline, or `None` if no timers are registered.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        let tick = self
            .inner
            .lock()
            .expect("driver poisoned")
            .wheel
            .next_tick()?;
        Some(tick_to_instant(self.epoch, tick))
    }

    /// Fire every waker whose deadline is `<= now`. Returns the number fired.
    ///
    /// Cancelled entries (whose id is no longer in the waker map) are
    /// drained from the wheel and silently discarded.
    pub(crate) fn advance(&self, now: Instant) -> usize {
        let now_tick = instant_to_tick_floor(self.epoch, now);
        let expired: Vec<Waker> = {
            let mut inner = self.inner.lock().expect("driver poisoned");
            let drained = inner.wheel.advance_to(now_tick);
            let mut wakers = Vec::with_capacity(drained.len());
            for entry in drained {
                if let Some(w) = inner.wakers.remove(&entry.id) {
                    wakers.push(w);
                }
            }
            wakers
        };
        let n = expired.len();
        for w in expired {
            w.wake();
        }
        n
    }

    /// Drop every pending registration without firing.
    ///
    /// Called from [`crate::executor::multi::Multi::drop`] to break the
    /// `Arc<Shared>` ↔ task cycle: each stored `Waker` holds a refcount
    /// on a task header, which transitively keeps the runtime alive.
    pub(crate) fn clear(&self) {
        let mut inner = self.inner.lock().expect("driver poisoned");
        inner.wheel.clear();
        inner.wakers.clear();
    }
}

/// Ceil-divide nanoseconds-since-epoch by `TICK_NS` so the resulting tick
/// is never earlier than `deadline`.
fn instant_to_tick_ceil(epoch: Instant, deadline: Instant) -> u64 {
    let nanos = deadline.saturating_duration_since(epoch).as_nanos();
    let ticks = nanos.div_ceil(TICK_NS);
    u64::try_from(ticks).unwrap_or(u64::MAX)
}

/// Floor-divide nanoseconds-since-epoch by `TICK_NS` so the cursor only
/// moves to a tick once real time has crossed it.
fn instant_to_tick_floor(epoch: Instant, now: Instant) -> u64 {
    let nanos = now.saturating_duration_since(epoch).as_nanos();
    let ticks = nanos / TICK_NS;
    u64::try_from(ticks).unwrap_or(u64::MAX)
}

/// Exact reverse: `epoch + tick * TICK`.
fn tick_to_instant(epoch: Instant, tick: u64) -> Instant {
    epoch + Duration::from_millis(tick)
}
