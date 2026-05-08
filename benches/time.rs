#![allow(missing_docs)] // criterion macros expand to undocumented items

//! Microbenchmarks for `nanorun::time`.
//!
//! Two cases break the per-call cost into components:
//!
//! - `sleep_zero` — `sleep(Duration::ZERO).await`. The deadline is in
//!   the past at first poll, so the future returns `Ready` immediately
//!   and never touches the wheel. This isolates the
//!   `block_on` + spawn + poll + return baseline.
//! - `sleep_1ms` — `sleep(Duration::from_millis(1)).await`. The future
//!   registers with the timer driver, parks the worker on the reactor
//!   for ~1ms, fires on advance, re-polls, and resolves. This measures
//!   the full register → advance → dispatch round-trip.
//!
//! Headline dispatch overhead = `sleep_1ms - 1ms - sleep_zero`.

use core::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_time(c: &mut Criterion) {
    let rt = nanorun::Runtime::new();

    c.bench_function("sleep_zero", |b| {
        b.iter(|| {
            rt.block_on(async {
                nanorun::time::sleep(Duration::ZERO).await;
            });
        });
    });

    c.bench_function("sleep_1ms", |b| {
        b.iter(|| {
            rt.block_on(async {
                nanorun::time::sleep(Duration::from_millis(1)).await;
            });
        });
    });
}

criterion_group!(benches, bench_time);
criterion_main!(benches);
