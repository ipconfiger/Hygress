//! Allocation-count regression guard for the zero-copy hot path
//! (`docs/research/zero-copy-plan.md` §1.3 / 2.4 / 3.3).
//!
//! Run:
//!   cargo test -p hygress-gateway --test alloc_guard -- --test-threads=1
//!   cargo test -p hygress-gateway --release --test alloc_guard -- --test-threads=1
//!
//! Isolation: this is a DEDICATED integration-test binary → its own process →
//! its OWN `#[global_allocator]`, so other `cargo test` binaries cannot pollute
//! the counters. Within this binary, every test takes a binary-wide `Mutex`, so
//! even under the default threaded test harness (no `--test-threads=1`) only one
//! test runs at a time — a fresh `measure()` window can never observe another
//! test's allocations or be reset under a concurrent measurement.
//!
//! Budgets (input = 512KiB):
//!   * `extract_model` / skip-strings        -> O(model string),  < 16 KiB
//!   * `apply_with_current` identity/None     -> O(1),            < 16 KiB
//!   * `rewrite_json_model` real change       -> ~1x body,        < 1.5 x body
//!   * `UsageSnapshot::feed` (SSE steady)     -> O(tail sliver),  < 16 KiB
//!   * linear ratio                            allocs(1MiB) < 4 * allocs(256KiB)
//!   * wall-time ratio (release only)          t(1MiB) < 6 * t(256KiB)

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use bytes::Bytes;
use hygress_core::prelude::{ModelMapping, UsageSchema, UsageSnapshot};
use hygress_gateway::body::{extract_model, rewrite_json_model};
use hygress_gateway::pipeline::model_mapper::apply_with_current;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const BODY_256K: usize = 256 * KIB;
const BODY_512K: usize = 512 * KIB;
const BUDGET_KB16: u64 = 16 * KIB as u64;

// ---------------------------------------------------------------------------
// Counting global allocator + binary-wide serialization
// ---------------------------------------------------------------------------

/// Serializes every test in this binary so a measurement window never races
/// with another test (required to stay correct without `--test-threads=1`).
static TEST_LOCK: Mutex<()> = Mutex::new(());

static MEASURING: AtomicBool = AtomicBool::new(false);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            // Charge only the growth delta; the original allocation is already
            // counted (keeps capacity growth visible without double counting).
            if new_size > layout.size() {
                ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            }
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Run `f` under the measurement gate; return the bytes it allocated.
/// Inputs must be built BEFORE the gate so their setup cost is not counted.
fn measure(f: impl FnOnce()) -> u64 {
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    f();
    MEASURING.store(false, Ordering::Relaxed);
    ALLOC_BYTES.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn chat_body(content_bytes: usize, model: &str) -> Bytes {
    // A large `messages[0].content` string is the "skipped" value the scanner
    // must validate-and-advance without materializing (B3).
    let content = "a".repeat(content_bytes);
    let s = format!(
        "{{\"model\":\"{model}\",\"messages\":[{{\"role\":\"user\",\"content\":\"{content}\"}}],\"stream\":true}}"
    );
    Bytes::from(s)
}

fn sse_stream(total: usize, chunk: usize) -> Vec<Bytes> {
    // Content-only SSE data lines (no `usage` token): the M5 prefilter skips
    // the DOM parse, so feed cost must be O(tail sliver), not O(body).
    let line = "data: {\"content\":\"hello\"}\n\n";
    let mut out = Vec::new();
    let mut remaining = total;
    while remaining >= line.len() {
        let take = remaining.min(chunk);
        let rep = line.repeat(take / line.len());
        let pushed = rep.len();
        if pushed == 0 {
            break;
        }
        out.push(Bytes::from(rep));
        remaining -= pushed;
    }
    out
}

// ---------------------------------------------------------------------------
// Request-side budgets (2.4: request-direction avoidable copies = 0)
// ---------------------------------------------------------------------------

#[test]
fn extract_model_512k_is_o_model_string_not_o_body() {
    let _g = TEST_LOCK.lock().unwrap();
    let body = chat_body(BODY_512K, "org-1/llama-3-8b");
    let mut value: Option<String> = None;
    let allocated = measure(|| {
        value = extract_model(&body, Some("application/json"), "model");
    });
    assert_eq!(value.as_deref(), Some("org-1/llama-3-8b"));
    assert!(
        allocated < BUDGET_KB16,
        "extract_model allocated {allocated} bytes for a 512KiB body (budget < {BUDGET_KB16})"
    );
}

#[test]
fn rewrite_real_change_allocates_about_one_body() {
    let _g = TEST_LOCK.lock().unwrap();
    let body = chat_body(BODY_512K, "org-1/llama-3-8b");
    let mut out: Option<Bytes> = None;
    let allocated = measure(|| {
        out = rewrite_json_model(&body, "model", "mapped-name");
    });
    let out = out.expect("real rewrite must produce a spliced body");
    assert_eq!(
        extract_model(&out, Some("application/json"), "model").as_deref(),
        Some("mapped-name")
    );
    assert!(
        (allocated as usize) < (BODY_512K * 3 / 2),
        "real rewrite allocated {allocated} bytes (expected ~1x body < 768KiB)"
    );
    assert!(
        (allocated as usize) > BODY_512K / 4,
        "real rewrite should have spliced a fresh buffer (~1x body), got {allocated}"
    );
}

#[test]
fn identity_and_none_mapping_short_circuit_allocates_little_512k() {
    let _g = TEST_LOCK.lock().unwrap();
    // Identity mapping: current == mapped → body reused, no scan, no splice.
    let body = chat_body(BODY_512K, "same");
    let mapping = ModelMapping::single("a.static", "same");
    let mut identity_body = body.clone();
    let allocated_identity = measure(|| {
        let changed = apply_with_current(
            &mapping,
            "a.static",
            &mut identity_body,
            "application/json",
            Some("same"),
        );
        assert!(!changed, "identity mapping must not rewrite");
    });
    assert_eq!(identity_body.len(), body.len(), "identity body must be reused");
    assert!(
        allocated_identity < BUDGET_KB16,
        "identity short-circuit allocated {allocated_identity} bytes (budget < {BUDGET_KB16})"
    );

    // None current (missing/non-string/malformed) → skip the scan entirely.
    let mut none_body = body.clone();
    let allocated_none = measure(|| {
        let changed = apply_with_current(
            &mapping,
            "a.static",
            &mut none_body,
            "application/json",
            None,
        );
        assert!(!changed, "None current must not rewrite");
    });
    assert!(
        allocated_none < BUDGET_KB16,
        "None-current skip allocated {allocated_none} bytes (budget < {BUDGET_KB16})"
    );
}

// ---------------------------------------------------------------------------
// Response-side budget (3.3: SSE per-chunk copy ≤ partial-tail sliver)
// ---------------------------------------------------------------------------

#[test]
fn usage_feed_sse_512k_is_o_tail_not_o_body() {
    let _g = TEST_LOCK.lock().unwrap();
    let mut snap = UsageSnapshot::new(UsageSchema::OpenAi);
    // Prime: the first chunk flips Unknown → Sse and performs the acknowledged
    // one-time transition copy (excluded from the gate).
    snap.feed(b"data: {\"content\":\"prime\"}\n\n");
    let chunks = sse_stream(BODY_512K, 4 * KIB);
    assert!(!chunks.is_empty());
    let allocated = measure(|| {
        for chunk in &chunks {
            snap.feed(chunk.as_ref());
        }
    });
    assert!(
        allocated < BUDGET_KB16,
        "SSE feed allocated {allocated} bytes for a 512KiB stream (budget < {BUDGET_KB16})"
    );
}

// ---------------------------------------------------------------------------
// Ratio guards
// ---------------------------------------------------------------------------

#[test]
fn allocs_linear_in_body_size() {
    let _g = TEST_LOCK.lock().unwrap();
    let small = chat_body(BODY_256K, "org-1/llama-3-8b");
    let large = chat_body(MIB, "org-1/llama-3-8b");
    let alloc_small = measure(|| {
        let _ = extract_model(&small, Some("application/json"), "model");
    });
    let alloc_large = measure(|| {
        let _ = extract_model(&large, Some("application/json"), "model");
    });
    assert!(
        alloc_large < 4 * alloc_small.max(1),
        "allocations not linear: 256KiB={alloc_small}, 1MiB={alloc_large}"
    );
}

/// Wall-time scan linearity. Strict under release (stable timing); relaxed in
/// debug so the guard is not flaky on slow debug runners.
#[cfg(not(debug_assertions))]
#[test]
fn scan_wall_time_is_linear_release() {
    let _g = TEST_LOCK.lock().unwrap();
    assert_scan_ratio(6.0);
}

#[cfg(debug_assertions)]
#[test]
fn scan_wall_time_is_bounded_debug() {
    let _g = TEST_LOCK.lock().unwrap();
    // Debug builds have predictable-but-slower code; only catch gross superlinear
    // blowups (the old O(n²) path was ~44,000x at 512KiB, far past any ratio).
    assert_scan_ratio(64.0);
}

fn assert_scan_ratio(ratio: f64) {
    const ITER: u32 = 8;
    let t = |bytes: usize| {
        let body = chat_body(bytes, "org-1/llama-3-8b");
        let start = Instant::now();
        for _ in 0..ITER {
            let _ = extract_model(&body, Some("application/json"), "model");
        }
        start.elapsed().as_secs_f64()
    };
    let t_small = t(BODY_256K);
    let t_large = t(MIB);
    assert!(
        t_large < ratio * t_small.max(1e-9),
        "scan time not linear: 256KiB={t_small:.6}s x{ITER}, 1MiB={t_large:.6}s x{ITER} (ratio {ratio})"
    );
}
