//! Minimal free-list slab for waker storage.
//!
//! Hands out stable `usize` keys; freed slots are reused via a singly-
//! linked free list embedded in the vacant entries themselves. No
//! generation counters in M2 — registration and deregistration are
//! strictly paired by [`crate::reactor`] callers.

use core::task::Waker;

/// Per-fd waker storage. Read and write directions are independent.
#[derive(Default)]
pub(crate) struct Slot {
    pub(crate) read: Option<Waker>,
    pub(crate) write: Option<Waker>,
}

enum Entry {
    Vacant(usize),
    Occupied(Slot),
}

const SENTINEL: usize = usize::MAX;

/// Append-only with O(1) reuse via the embedded free list.
pub(crate) struct Slab {
    entries: Vec<Entry>,
    next_free: usize,
}

impl Slab {
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_free: SENTINEL,
        }
    }

    /// Insert an empty slot and return its key.
    pub(crate) fn insert(&mut self) -> usize {
        if self.next_free == SENTINEL {
            let key = self.entries.len();
            self.entries.push(Entry::Occupied(Slot::default()));
            key
        } else {
            let key = self.next_free;
            let next = match &self.entries[key] {
                Entry::Vacant(n) => *n,
                Entry::Occupied(_) => unreachable!("free list pointed at occupied slot"),
            };
            self.next_free = next;
            self.entries[key] = Entry::Occupied(Slot::default());
            key
        }
    }

    /// Remove the slot at `key`. No-op if `key` is out of range or already vacant.
    pub(crate) fn remove(&mut self, key: usize) {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        if matches!(entry, Entry::Vacant(_)) {
            return;
        }
        *entry = Entry::Vacant(self.next_free);
        self.next_free = key;
    }

    /// Mutable access to the slot at `key`, if occupied.
    pub(crate) fn get_mut(&mut self, key: usize) -> Option<&mut Slot> {
        match self.entries.get_mut(key)? {
            Entry::Occupied(s) => Some(s),
            Entry::Vacant(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_remove_then_reuse() {
        let mut s = Slab::new();
        let a = s.insert();
        let b = s.insert();
        assert_ne!(a, b);
        s.remove(a);
        let c = s.insert();
        assert_eq!(c, a, "freed slot should be reused first");
    }

    #[test]
    fn remove_out_of_range_is_noop() {
        let mut s = Slab::new();
        s.remove(42);
    }

    #[test]
    fn get_mut_after_remove_returns_none() {
        let mut s = Slab::new();
        let k = s.insert();
        s.remove(k);
        assert!(s.get_mut(k).is_none());
    }
}
