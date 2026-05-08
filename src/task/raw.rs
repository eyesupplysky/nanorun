//! Raw task layout: header, future storage, and vtable.
//!
//! The header carries reference counts, state bits, and the waker
//! vtable pointer. The future is stored inline after the header.

/// Raw task storage. Layout is unstable until M1 commits to a header design.
#[allow(dead_code)]
pub(crate) struct RawTask;
