//! Windows reactor backend — IOCP for parking, AFD-poll for fd readiness.
//!
//! `Inner::new` opens the IOCP and the AFD device handle; the AFD handle
//! is associated with the IOCP under [`afd::AFD_COMPLETION_KEY`] so its
//! IOCTL completions land alongside cross-thread wake posts on the same
//! port. Every registered socket carries a stable `Box<WindowsSlotState>`
//! whose first field is the [`afd::IoStatusBlock`] passed to
//! `IOCTL_AFD_POLL`; the IOCP delivers that same pointer in
//! `lpOverlapped`, which the dispatch loop casts back via `repr(C)` to
//! recover the slab key.
//!
//! AFD-poll IOCTLs are one-shot. To preserve epoll-style level-triggered
//! semantics the dispatch loop re-submits a fresh IOCTL with the same
//! event mask after every completion.

use core::task::Waker;
use std::io;
use std::os::windows::io::{AsHandle, OwnedHandle};
use std::sync::Mutex;
use std::time::Duration;

use windows_sys::Win32::Foundation::HANDLE;

use super::slab::{Slab, WindowsSlotState};
use super::{Direction, Interest, IoSource, Token};
use crate::sys::windows::{afd, iocp, socket as sys_socket};

/// Completion key used by [`Inner::wake`] to mark cross-thread wakeup posts.
const SELF_WAKE_KEY: usize = 0;

/// Windows backend state. One per [`super::Reactor`]; held behind `Arc<Inner>`.
pub(super) struct Inner {
    iocp: OwnedHandle,
    afd: OwnedHandle,
    slots: Mutex<Slab>,
}

// `SOCKET` is `usize`, `HANDLE` is `isize`. The bitwise value is the
// same kernel handle; sign reinterpretation is harmless.
#[allow(clippy::cast_possible_wrap)]
fn socket_to_handle(s: windows_sys::Win32::Networking::WinSock::SOCKET) -> HANDLE {
    s as HANDLE
}

impl Inner {
    pub(super) fn new() -> io::Result<Self> {
        let iocp = iocp::create()?;
        let afd = afd::open(iocp.as_handle())?;
        Ok(Self {
            iocp,
            afd,
            slots: Mutex::new(Slab::new()),
        })
    }

    pub(super) fn poll(&self, timeout: Option<Duration>) -> io::Result<()> {
        let mut entries = [iocp::Entry::ZERO; iocp::MAX_ENTRIES_PER_WAIT];
        let n = iocp::wait(self.iocp.as_handle(), &mut entries, timeout)?;
        if n == 0 {
            return Ok(());
        }
        let mut to_wake: Vec<Waker> = Vec::with_capacity(n * 2);
        let mut to_resubmit: Vec<usize> = Vec::with_capacity(n);

        // Phase 1: dispatch completions, collect wakers and re-submit candidates.
        {
            let mut slots = self.slots.lock().expect("reactor slab poisoned");
            for entry in &entries[..n] {
                if entry.overlapped.is_null() {
                    continue; // self-wake post
                }
                // SAFETY: AFD-poll completions deliver the IoStatusBlock
                // pointer we passed in; that pointer is the first field
                // of a Box-backed WindowsSlotState (`repr(C)`, iosb at
                // offset 0). The Box is alive for as long as the slot
                // owns it; we hold the slab lock here so nothing else
                // can drop it.
                let state_ptr = entry.overlapped.cast::<WindowsSlotState>();
                let slab_key = unsafe { (*state_ptr).slab_key };
                let Some(slot) = slots.get_mut(slab_key) else {
                    continue; // slot already removed
                };
                if slot.cancelled {
                    // Cancellation completion — drop the box and free the slot.
                    slot.windows = None;
                    slots.remove(slab_key);
                    continue;
                }
                let Some(state) = slot.windows.as_ref() else {
                    continue;
                };
                let events = state.info.handles[0].events;
                if events & afd::READ_EVENTS != 0 {
                    if let Some(w) = slot.read.take() {
                        to_wake.push(w);
                    }
                }
                if events & afd::WRITE_EVENTS != 0 {
                    if let Some(w) = slot.write.take() {
                        to_wake.push(w);
                    }
                }
                to_resubmit.push(slab_key);
            }
        }

        // Phase 2: fire wakers (no slab lock held).
        for w in to_wake.drain(..) {
            w.wake();
        }

        // Phase 3: re-submit IOCTLs to preserve level-triggered semantics.
        let mut failed_wakers: Vec<Waker> = Vec::new();
        {
            let mut slots = self.slots.lock().expect("reactor slab poisoned");
            for key in to_resubmit {
                let Some(slot) = slots.get_mut(key) else {
                    continue;
                };
                if slot.cancelled {
                    slot.windows = None;
                    slots.remove(key);
                    continue;
                }
                let Some(state) = slot.windows.as_mut() else {
                    continue;
                };
                state.info.handles[0].events = state.requested_events;
                state.info.handles[0].status = 0;
                if afd::submit(self.afd.as_handle(), &mut state.iosb, &mut state.info).is_err() {
                    // Re-submit failed — orphan the slot. Wake any
                    // waiting future so it re-polls and observes the
                    // real syscall error.
                    if let Some(w) = slot.read.take() {
                        failed_wakers.push(w);
                    }
                    if let Some(w) = slot.write.take() {
                        failed_wakers.push(w);
                    }
                    slot.windows = None;
                    slots.remove(key);
                }
            }
        }

        // Phase 4: fire wakers from re-submit failures.
        for w in failed_wakers {
            w.wake();
        }

        Ok(())
    }

    pub(super) fn wake(&self) -> io::Result<()> {
        iocp::post(self.iocp.as_handle(), SELF_WAKE_KEY)
    }

    pub(super) fn register(&self, source: IoSource<'_>, interest: Interest) -> io::Result<Token> {
        let base = sys_socket::base_socket(source)?;
        let mut events: u32 = 0;
        if interest.read {
            events |= afd::READ_EVENTS;
        }
        if interest.write {
            events |= afd::WRITE_EVENTS;
        }

        let mut slots = self.slots.lock().expect("reactor slab poisoned");
        let key = slots.insert();

        let mut state = Box::new(WindowsSlotState {
            iosb: afd::IoStatusBlock::ZERO,
            info: afd::AfdPollInfo::ZERO,
            base_socket: base,
            slab_key: key,
            requested_events: events,
        });
        state.info.timeout = i64::MAX;
        state.info.number_of_handles = 1;
        state.info.handles[0].handle = socket_to_handle(base);
        state.info.handles[0].events = events;
        state.info.handles[0].status = 0;

        if let Err(e) = afd::submit(self.afd.as_handle(), &mut state.iosb, &mut state.info) {
            slots.remove(key);
            return Err(e);
        }

        let slot = slots
            .get_mut(key)
            .expect("just-inserted slot must be occupied");
        slot.windows = Some(state);

        let token = u64::try_from(key)
            .expect("slab key fits u64")
            .checked_add(1)
            .expect("slab key under u64::MAX");
        Ok(Token(token))
    }

    pub(super) fn set_waker(&self, token: Token, direction: Direction, waker: &Waker) {
        let Some(raw) = token.0.checked_sub(1) else {
            return;
        };
        let Ok(key) = usize::try_from(raw) else {
            return;
        };
        let mut slots = self.slots.lock().expect("reactor slab poisoned");
        if let Some(slot) = slots.get_mut(key) {
            if slot.cancelled {
                return;
            }
            match direction {
                Direction::Read => slot.read = Some(waker.clone()),
                Direction::Write => slot.write = Some(waker.clone()),
            }
        }
    }

    pub(super) fn deregister(&self, _source: IoSource<'_>, token: Token) -> io::Result<()> {
        let Some(raw) = token.0.checked_sub(1) else {
            return Ok(());
        };
        let Ok(key) = usize::try_from(raw) else {
            return Ok(());
        };
        let mut slots = self.slots.lock().expect("reactor slab poisoned");
        let Some(slot) = slots.get_mut(key) else {
            return Ok(());
        };
        // Issue cancel BEFORE marking cancelled. The Box stays alive
        // until the IOCP delivers the cancellation completion — that
        // completion's Phase-1 dispatch frees it.
        let cancel_result = match slot.windows.as_mut() {
            Some(state) => afd::cancel(self.afd.as_handle(), &mut state.iosb),
            None => Ok(()),
        };
        slot.cancelled = true;
        slot.read = None;
        slot.write = None;
        cancel_result
    }
}
