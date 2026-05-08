//! Multi-threaded executor driver (M3 target).
//!
//! Per-worker schedulers and `spawn`. Architecture (work-stealing vs.
//! fixed-affinity) is decided at M3 — this file is a slot-reservation.

#[allow(dead_code)]
pub(crate) fn placeholder() {}
