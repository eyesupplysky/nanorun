//! Waker plumbing: hand-rolled `RawWakerVTable` construction.
//!
//! Lean toward a hand-rolled vtable for transparency in M1. The
//! `std::task::Wake` trait remains an option; if we adopt it later this
//! module splits into `vtable.rs` + `arc.rs`.

#[allow(dead_code)]
pub(crate) fn placeholder() {}
