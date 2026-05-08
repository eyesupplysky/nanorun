//! Worker run-loop helpers: per-worker slot, idle (driver-token + park),
//! work-stealing, and the queue-emptiness probe.
//!
//! These functions are split out of `multi.rs` so that file stays under
//! the 500-line CLAUDE.md cap and the wake-gating memory-ordering
//! comment in [`idle`] is easy to find.

use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Mutex, OnceLock};
use std::thread::{self, Thread};
use std::time::Instant;

use crate::task::TaskRef;

use super::Shared;

pub(super) struct WorkerSlot {
    pub(super) thread: OnceLock<Thread>,
    pub(super) local: Mutex<VecDeque<TaskRef>>,
}

impl WorkerSlot {
    pub(super) fn new() -> Self {
        Self {
            thread: OnceLock::new(),
            local: Mutex::new(VecDeque::new()),
        }
    }
}

pub(super) fn idle(shared: &Shared, my_id: usize) {
    if shared.shutdown.load(Ordering::Acquire) {
        return;
    }
    if has_work(shared, my_id) {
        return;
    }
    if shared
        .driver_held
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
    {
        // We hold the driver token. Mark ourselves as parked BEFORE
        // the final re-check, then enter `Reactor::poll`.
        //
        // # Memory ordering
        //
        // The store/load on `driver_parked` here is the Dekker-style
        // half of the lost-wakeup race fix. It is paired with the
        // SeqCst load in `Spawner::schedule`. The two sides:
        //
        //   schedule:                       park (here):
        //     push_global(task)               store(driver_parked, true)
        //     load(driver_parked)             has_work() == load(queues)
        //
        // SeqCst on both sides imposes a single total order across the
        // two memory locations (`driver_parked` and the queue
        // contents). Either schedule observes `driver_parked=true` and
        // rings the eventfd, or the park side's re-check observes the
        // task that schedule just pushed. AcqRel would be insufficient
        // because each location's release/acquire only orders accesses
        // to the *same* location.
        shared.driver_parked.store(true, Ordering::SeqCst);
        if has_work(shared, my_id) {
            // A scheduler raced us between the CAS and the parked-flag
            // set. Skip the reactor poll entirely.
            shared.driver_parked.store(false, Ordering::SeqCst);
            shared.driver_held.store(false, Ordering::Release);
            return;
        }
        // Bound the reactor poll by the next timer deadline. `None`
        // (no pending timers) blocks until I/O wake or shutdown wake;
        // a deadline already in the past produces a zero timeout, so
        // the reactor returns immediately and we fall through to
        // `advance`.
        let now = Instant::now();
        let timeout = shared
            .driver
            .next_deadline()
            .map(|d| d.saturating_duration_since(now));
        shared.reactor.poll(timeout).expect("reactor poll");
        shared.driver_parked.store(false, Ordering::SeqCst);
        shared.driver_held.store(false, Ordering::Release);
        // Fire any timers that have expired during the poll. Wakers
        // re-enter `Spawner::schedule` and push their tasks onto the
        // injector — the next loop iteration will pick them up.
        shared.driver.advance(Instant::now());
        return;
    }
    // Someone else is driving; sleep until unparked. The classic
    // "set parked, re-check, then park" race is avoided because
    // `thread::park` respects the unpark token deposited by any prior
    // schedule.
    thread::park();
}

pub(super) fn steal(shared: &Shared, my_id: usize, rng: &mut u64) -> Option<TaskRef> {
    let n = shared.workers.len();
    if n <= 1 {
        return None;
    }
    // Random index for the steal start point; truncating the high bits of the rng output is intentional.
    #[allow(clippy::cast_possible_truncation)]
    let start = (xorshift64(rng) as usize) % n;
    for i in 0..n {
        let victim = (start + i) % n;
        if victim == my_id {
            continue;
        }
        if let Some(t) = steal_from(shared, victim, my_id) {
            return Some(t);
        }
    }
    None
}

fn steal_from(shared: &Shared, victim: usize, my_id: usize) -> Option<TaskRef> {
    let mut victim_q = shared.workers[victim]
        .local
        .lock()
        .expect("victim local poisoned");
    let total = victim_q.len();
    if total == 0 {
        return None;
    }
    let take = total.div_ceil(2);
    let mut stolen: VecDeque<TaskRef> = victim_q.drain(..take).collect();
    drop(victim_q);

    let first = stolen.pop_front();
    if !stolen.is_empty() {
        let mut my_q = shared.workers[my_id]
            .local
            .lock()
            .expect("local queue poisoned");
        my_q.extend(stolen);
    }
    first
}

fn has_work(shared: &Shared, my_id: usize) -> bool {
    if !shared.workers[my_id]
        .local
        .lock()
        .expect("local queue poisoned")
        .is_empty()
    {
        return true;
    }
    if !shared
        .injector
        .lock()
        .expect("injector poisoned")
        .is_empty()
    {
        return true;
    }
    for (i, w) in shared.workers.iter().enumerate() {
        if i == my_id {
            continue;
        }
        if !w.local.lock().expect("victim local poisoned").is_empty() {
            return true;
        }
    }
    false
}

fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    if x == 0 {
        x = 0xDEAD_BEEF_CAFE_F00D;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}
