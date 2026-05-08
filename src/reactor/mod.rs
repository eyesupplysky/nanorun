//! Reactor: registers fd / handle interest, parks tasks, wakes on readiness.
//!
//! The abstraction shape (pluggable trait vs. `cfg`-gated module) is
//! deliberately undecided until M5 lands the Windows backend. Do not
//! introduce a `Reactor` trait or split into `linux.rs` / `windows.rs`
//! before then.

#[allow(dead_code)]
pub(crate) fn placeholder() {}
