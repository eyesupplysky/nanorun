//! Fallback reactor: in-process `Mutex` + `Condvar` parking primitive.
//!
//! Used on non-Linux, non-Windows targets (e.g. macOS until a kqueue
//! backend lands, or any tier-3 target). Supports cross-thread wakeups
//! but no real fd-readiness — `net` types are gated off these targets.

use std::io;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// Fallback backend state. One per [`super::Reactor`]; held behind `Arc<Inner>`.
pub(super) struct Inner {
    notify: Mutex<bool>,
    cvar: Condvar,
}

// Each method returns `io::Result<()>` to share its signature with the
// Linux backend; on this backend the operations are infallible, hence
// the allows.
#[allow(clippy::unnecessary_wraps)]
impl Inner {
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self {
            notify: Mutex::new(false),
            cvar: Condvar::new(),
        })
    }

    pub(super) fn poll(&self, timeout: Option<Duration>) -> io::Result<()> {
        let mut notified = self.notify.lock().expect("reactor notify poisoned");
        if !*notified {
            notified = match timeout {
                Some(d) => self.cvar.wait_timeout(notified, d).expect("reactor cvar").0,
                None => self.cvar.wait(notified).expect("reactor cvar"),
            };
        }
        *notified = false;
        Ok(())
    }

    pub(super) fn wake(&self) -> io::Result<()> {
        *self.notify.lock().expect("reactor notify poisoned") = true;
        self.cvar.notify_one();
        Ok(())
    }
}
