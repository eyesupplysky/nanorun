//! Hierarchical timer wheel.
//!
//! Geometry: **6 levels × 64 slots × 1ms base tick**. Total range
//! `64^6 - 1 = 68_719_476_735` ticks ≈ 2.18 years. Implements the
//! canonical Varghese-Lauck hashed-and-hierarchical wheel from
//! "Hashed and Hierarchical Timer Wheels" (1987).
//!
//! # Algorithm
//!
//! An entry with absolute deadline `d` and current cursor `c` lives at
//! level `k = floor(log64(d - c))` and slot `(d >> (6*k)) & 63`. Time
//! advances tick-by-tick. On each tick at which the cursor crosses a
//! `64^k` boundary, level `k` cascades — the now-current slot is drained
//! and each entry re-inserted, which lands it in a lower level (since
//! its remaining delta has shrunk).
//!
//! # Performance shape
//!
//! - `insert`: O(1).
//! - `advance_to`: per-tick loop, cheap when the wheel is empty
//!   (short-circuit), O(64 + cascaded entries) per tick otherwise. Slice
//!   6 may add bitmap-driven slot-skipping for large empty stretches.
//! - `next_tick`: walks all entries — O(N). Used once per worker idle
//!   pass to bound the reactor poll timeout. Slice 6 may cache.
//!
//! # Range clamping
//!
//! Deadlines beyond `cursor + MAX_TICKS - 1` are clamped at insert time
//! so the wheel's level invariants always hold. Practically, this only
//! bites timers scheduled more than ~2.2 years in the future.

const LEVELS: usize = 6;
const SLOTS_PER_LEVEL: usize = 64;
const LEVEL_BITS: u64 = 6; // log2(SLOTS_PER_LEVEL)
const SLOT_MASK: u64 = SLOTS_PER_LEVEL as u64 - 1;
const MAX_TICKS: u64 = 1u64 << (LEVEL_BITS * LEVELS as u64); // 64^6

pub(crate) struct Entry {
    pub(crate) id: u64,
    pub(crate) deadline: u64,
}

pub(crate) struct Wheel {
    levels: [Level; LEVELS],
    cursor: u64,
    count: usize,
}

struct Level {
    slots: [Vec<Entry>; SLOTS_PER_LEVEL],
    /// Bit `i` set ⇔ `slots[i]` is non-empty. Slice 6 will use this for
    /// fast slot-skipping; for now it just keeps `next_tick` honest.
    occupied: u64,
}

impl Wheel {
    pub(crate) fn new() -> Self {
        Self {
            levels: core::array::from_fn(|_| Level::new()),
            cursor: 0,
            count: 0,
        }
    }

    /// Insert `entry`. Caller must drain `Err`-returned entries themselves
    /// (the wheel cannot place a "fire-now" entry into a slot that the
    /// next `advance` would visit, because slots are indexed by deadline,
    /// not by recency).
    pub(crate) fn insert(&mut self, entry: Entry) -> Result<(), Entry> {
        if entry.deadline <= self.cursor {
            return Err(entry);
        }
        let mut entry = entry;
        // Clamp deadlines past the wheel's representable range. The
        // entry will fire at the clamped tick rather than its requested
        // deadline.
        let max_deadline = self.cursor + MAX_TICKS - 1;
        if entry.deadline > max_deadline {
            entry.deadline = max_deadline;
        }
        let delta = entry.deadline - self.cursor;
        let k = level_for(delta);
        let slot = ((entry.deadline >> (LEVEL_BITS * k as u64)) & SLOT_MASK) as usize;
        self.levels[k].insert(slot, entry);
        self.count += 1;
        Ok(())
    }

    /// Earliest pending deadline (in ticks), or `None` if empty.
    ///
    /// Walks every entry (O(count)). Consumed once per worker idle pass
    /// — when the wheel is empty (the common runtime-idle case) this
    /// short-circuits via the `count == 0` check.
    pub(crate) fn next_tick(&self) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let mut min: Option<u64> = None;
        for level in &self.levels {
            if level.occupied == 0 {
                continue;
            }
            for slot in &level.slots {
                for entry in slot {
                    min = Some(min.map_or(entry.deadline, |m| m.min(entry.deadline)));
                }
            }
        }
        min
    }

    /// Advance cursor to `now`; return every entry whose deadline is `<= now`.
    pub(crate) fn advance_to(&mut self, now: u64) -> Vec<Entry> {
        let mut expired = Vec::new();
        if now <= self.cursor {
            return expired;
        }
        if self.count == 0 {
            self.cursor = now;
            return expired;
        }
        while self.cursor < now {
            self.advance_one(&mut expired);
            if self.count == 0 {
                // No more pending entries; skip directly to the target.
                self.cursor = now;
                break;
            }
        }
        expired
    }

    /// Drop every pending entry without firing.
    pub(crate) fn clear(&mut self) {
        for level in &mut self.levels {
            for slot in &mut level.slots {
                slot.clear();
            }
            level.occupied = 0;
        }
        self.count = 0;
    }

    /// Advance cursor by exactly one tick, draining and cascading as needed.
    ///
    /// Cascade order is **top-down**: when the cursor crosses a `64^k`
    /// boundary, level `k` is processed before level `k-1`. This is
    /// required for correctness — entries cascading down from level `k`
    /// must land in their lower-level slots *before* lower-level
    /// cascades drain those slots.
    fn advance_one(&mut self, expired: &mut Vec<Entry>) {
        self.cursor += 1;
        for k in (1..LEVELS).rev() {
            let period = 1u64 << (LEVEL_BITS * k as u64);
            if self.cursor % period == 0 {
                let slot = ((self.cursor >> (LEVEL_BITS * k as u64)) & SLOT_MASK) as usize;
                let drained = self.levels[k].drain_slot(slot);
                self.count -= drained.len();
                for entry in drained {
                    match self.insert(entry) {
                        Ok(()) => {}
                        // Cascaded entry whose deadline equals the new
                        // cursor expires now.
                        Err(e) => expired.push(e),
                    }
                }
            }
        }
        let slot = (self.cursor & SLOT_MASK) as usize;
        let drained = self.levels[0].drain_slot(slot);
        self.count -= drained.len();
        expired.extend(drained);
    }
}

impl Level {
    fn new() -> Self {
        Self {
            slots: core::array::from_fn(|_| Vec::new()),
            occupied: 0,
        }
    }

    fn insert(&mut self, slot: usize, entry: Entry) {
        debug_assert!(slot < SLOTS_PER_LEVEL);
        self.slots[slot].push(entry);
        self.occupied |= 1u64 << slot;
    }

    fn drain_slot(&mut self, slot: usize) -> Vec<Entry> {
        debug_assert!(slot < SLOTS_PER_LEVEL);
        let drained: Vec<Entry> = self.slots[slot].drain(..).collect();
        if self.slots[slot].is_empty() {
            self.occupied &= !(1u64 << slot);
        }
        drained
    }
}

/// Smallest `k` such that `delta < 64^(k+1)`, clamped to `LEVELS - 1`.
fn level_for(delta: u64) -> usize {
    debug_assert!(delta > 0);
    // `ilog2` returns `u32`, widening to `usize` is lossless on every
    // supported target. The literal `6` is `LEVEL_BITS` — kept inline
    // to avoid a `u64 as usize` truncation cast.
    let highest_bit = delta.ilog2() as usize;
    (highest_bit / 6).min(LEVELS - 1)
}
