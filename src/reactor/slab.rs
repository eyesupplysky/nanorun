//! Minimal free-list slab for waker storage.
//!
//! Hands out stable `usize` keys; freed slots are reused via a singly-
//! linked free list embedded in the vacant entries themselves. No
//! generation counters in M2 — registration and deregistration are
//! strictly paired by [`crate::reactor`] callers.
//!
//! Shared by the Linux and Windows reactor backends. On Windows each
//! slot also owns a `Box<WindowsSlotState>` whose stable address is the
//! `IoStatusBlock` pointer the IOCP returns; recovery from completion
//! to slot uses `repr(C)` layout (iosb at offset 0).

use core::task::Waker;

#[cfg(target_os = "windows")]
use crate::sys::windows::afd::{AfdPollInfo, IoStatusBlock};
#[cfg(target_os = "windows")]
use windows_sys::Win32::Networking::WinSock::SOCKET;

/// Per-socket Windows AFD-poll state. Owned via `Box` so its address is stable.
#[cfg(target_os = "windows")]
#[repr(C)]
pub(crate) struct WindowsSlotState {
    /// IOCP returns a pointer here in `lpOverlapped`. MUST stay at offset 0.
    pub(crate) iosb: IoStatusBlock,
    /// Input/output for `IOCTL_AFD_POLL`.
    pub(crate) info: AfdPollInfo,
    /// Bottom-of-stack base socket handle (bypasses any LSP layered over the user socket).
    pub(crate) base_socket: SOCKET,
    /// Index into the parent slab; recovered via the offset-0 cast.
    pub(crate) slab_key: usize,
    /// AFD event mask requested at registration time; replayed on every re-submit to preserve level-triggered semantics.
    pub(crate) requested_events: u32,
}

/// Per-fd waker storage. Read and write directions are independent.
#[derive(Default)]
pub(crate) struct Slot {
    pub(crate) read: Option<Waker>,
    pub(crate) write: Option<Waker>,
    /// Windows AFD-poll state — None until [`crate::reactor::Reactor::register`] populates it.
    #[cfg(target_os = "windows")]
    pub(crate) windows: Option<Box<WindowsSlotState>>,
    /// Set by [`crate::reactor::Reactor::deregister`]; the next IOCP completion that touches this slot frees it.
    #[cfg(target_os = "windows")]
    pub(crate) cancelled: bool,
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
