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
//!
//! AM-6 (header materialization, `am6_*` tests): the pure header stages —
//! `prepare` end-to-end, `build_outbound` (per-candidate deep copy), and the
//! dial pair materialization (`HeaderMap::into_pairs`, drain vs shared clone).
//! AM-6b replaced the per-candidate clone-then-mutate `HeaderMap` with the lazy
//! `OutboundHeaders` overlay (`build_outbound` records a delta only; the base
//! entries are materialized once at the dial drain or the provider-branch
//! `materialize`). The ceilings below are intentionally GENEROUS (the
//! coordinator tightens them after the first real run); each test prints
//! `AM6 measured ...` / `AM6b measured ...` with the bytes + allocation count
//! for that pass.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use bytes::Bytes;
use hygress_core::prelude::{
    ConfigData, Destination, HeaderMap, ModelMapping, ModelRouterConfig, ModelRouterSettings,
    PathPred, Registry, RouteKind, RouteRule, RouteTable, SharedConfig, UsageSchema, UsageSnapshot,
};
use hygress_gateway::body::{extract_model, rewrite_json_model};
use hygress_gateway::context::{Scheme, hdr};
use hygress_gateway::pipeline::model_mapper::apply_with_current;
use hygress_gateway::pipeline::{self, PipelineCtx};
use hygress_gateway::{
    CandidateTarget, InboundRequest, OutboundRequest, PreparedRequest, SharedConfigHandle,
};

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
/// AM-6: allocation COUNT (one event per alloc/alloc_zeroed/realloc while
/// measuring) — lets the AM-6 benches print the "small-allocations per request"
/// figure the charter tracks, alongside the byte total.
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if MEASURING.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
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
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
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
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    f();
    MEASURING.store(false, Ordering::Relaxed);
    ALLOC_BYTES.load(Ordering::Relaxed)
}

/// Run `f` under the gate; return (allocated bytes, allocation count).
fn measure_counted(f: impl FnOnce()) -> (u64, u64) {
    ALLOC_BYTES.store(0, Ordering::Relaxed);
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    f();
    MEASURING.store(false, Ordering::Relaxed);
    (
        ALLOC_BYTES.load(Ordering::Relaxed),
        ALLOC_COUNT.load(Ordering::Relaxed),
    )
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

// ---------------------------------------------------------------------------
// AM-6: header-materialization allocation accounting for the PURE header
// stages (counting allocator; fixtures built BEFORE each gate).
// ---------------------------------------------------------------------------

/// A registry-resolved candidate fixture (non-provider: no key-swap).
fn candidate(service_name: &str) -> CandidateTarget {
    CandidateTarget {
        service: format!("{service_name}:80"),
        service_name: service_name.to_string(),
        address: "10.0.0.5:8081".into(),
        proxied: false,
        scheme: Scheme::Http,
        proxy: None,
    }
}

/// The config/table/shared/SWRR fixtures a `prepare` needs (a Main route keyed
/// `org1/llama-3-8b` over one registry destination, body-driven model
/// resolution on `/v1/chat/completions`). Built once per test, BEFORE the gate.
fn model_route_env() -> (ConfigData, RouteTable, SharedConfigHandle, ModelRouterConfig) {
    let data = ConfigData {
        routes: vec![RouteRule::new(
            "org1/llama-3-8b",
            RouteKind::Main,
            vec![PathPred::new(".*")],
            vec![Destination::new("model-1-10.static:80")],
        )
        .expect("route fixture")],
        registries: vec![
            Registry::new("model-1-10.static:80", "10.0.0.5:8081").expect("registry fixture"),
        ],
        model_router: ModelRouterSettings {
            enable_on_path_suffix: vec!["/v1/chat/completions".into()],
            ..Default::default()
        },
        ..ConfigData::default()
    };
    let router = ModelRouterConfig::from_settings(&data.model_router);
    let table = RouteTable::rebuild(&data).expect("route-table fixture");
    let shared = SharedConfigHandle::new(SharedConfig::new(data.clone()).expect("shared fixture"));
    (data, table, shared, router)
}

/// A realistic model-route inbound: ~13 headers (host / content-length /
/// content-type / authorization / x-higress-llm-model / x-organization-id /
/// cookie / ...) plus the mirrored `:path`, and a JSON chat body whose model
/// matches the route key (R-5 identity: prepare does NOT splice the body).
fn realistic_model_route_inbound() -> InboundRequest {
    let body = br#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}],"stream":true}"#;
    let mut headers = HeaderMap::new();
    for (name, value) in [
        ("host", "llm.gpustack.local"),
        ("content-type", "application/json"),
        ("authorization", "Bearer sk-client-0123456789abcdef"),
        ("user-agent", "curl/8.4.0"),
        ("accept", "application/json"),
        ("x-real-ip", "203.0.113.7"),
        ("x-forwarded-for", "203.0.113.7"),
        ("x-request-id", "req-0123456789abcdef"),
        ("x-higress-llm-model", "org1/llama-3-8b"),
        ("x-organization-id", "org-42"),
        ("cookie", "session=abc123"),
    ] {
        headers.insert(name, value);
    }
    headers.insert("content-length", body.len().to_string());
    // `read_headers` mirrors `:path` into the map (the transformer backstops it).
    headers.insert(hdr::PATH, "/v1/chat/completions");
    InboundRequest {
        method: "POST".into(),
        path: "/v1/chat/completions".into(),
        query: String::new(),
        headers,
        body: Bytes::from_static(body),
        content_type: "application/json".into(),
        client_ip: "203.0.113.7".into(),
        host: "llm.gpustack.local".into(),
    }
}

#[test]
fn am6_prepare_model_route_end_to_end() {
    let _g = TEST_LOCK.lock().unwrap();
    // Fixtures built BEFORE the gate.
    let inbound = realistic_model_route_inbound();
    let (data, table, shared, router) = model_route_env();
    let ctx = PipelineCtx {
        data: &data,
        table: &table,
        config: &shared,
        router: &router,
    };
    let mut prepared: Option<PreparedRequest> = None;
    let (bytes, allocs) = measure_counted(|| {
        prepared = Some(pipeline::prepare(&inbound, &ctx).expect("fixture must route"));
    });
    eprintln!(
        "AM6 measured prepare model-route end-to-end: {bytes} bytes / {allocs} allocs"
    );
    let p = prepared.expect("prepare succeeded");
    assert!(p.route.is_model_route);
    assert_eq!(p.base_headers.get(hdr::LLM_MODEL), Some("org1/llama-3-8b"));
    assert!(
        bytes < 6 * KIB as u64,
        "AM6 prepare allocated {bytes} bytes (measured 3018/91; ceiling < 6 KiB)"
    );
}

#[test]
fn am6_build_outbound_single_candidate() {
    let _g = TEST_LOCK.lock().unwrap();
    let inbound = realistic_model_route_inbound();
    let (data, table, shared, router) = model_route_env();
    let ctx = PipelineCtx {
        data: &data,
        table: &table,
        config: &shared,
        router: &router,
    };
    // Fixture: one prepared request (its own prepare is NOT measured).
    let p = pipeline::prepare(&inbound, &ctx).expect("fixture routes");
    let c = candidate("model-1-10.static");
    let mut out: Option<OutboundRequest> = None;
    let (bytes, allocs) = measure_counted(|| {
        out = Some(pipeline::build_outbound(
            "POST",
            &p,
            &c,
            &HeaderMap::new(),
            &[],
        ));
    });
    eprintln!(
        "AM6b measured build_outbound 1 candidate (registry, no token swap): {bytes} bytes / {allocs} allocs"
    );
    let out = out.expect("outbound built");
    assert_eq!(
        out.headers.get(hdr::MODEL_INSTANCE_OUT),
        Some("model-1-10.static")
    );
    assert_eq!(out.headers.get(hdr::LLM_MODEL), Some("org1/llama-3-8b"));
    assert_eq!(out.headers.get("content-length"), None, "hop-by-hop stripped");
    assert!(
        bytes < 2 * KIB as u64,
        "AM6b build_outbound (1 candidate) allocated {bytes} bytes (AM6b measured 1071/27; ceiling < 2 KiB)"
    );
}

#[test]
fn am6_build_outbound_with_ext_auth_writeback() {
    let _g = TEST_LOCK.lock().unwrap();
    let inbound = realistic_model_route_inbound();
    let (data, table, shared, router) = model_route_env();
    let ctx = PipelineCtx {
        data: &data,
        table: &table,
        config: &shared,
        router: &router,
    };
    let p = pipeline::prepare(&inbound, &ctx).expect("fixture routes");
    let c = candidate("model-1-10.static");
    // Realistic ext-auth write-back (an allowed ai-route verdict replaces the
    // client credential + adds cookie / consumer / auth-cache).
    let wb = HeaderMap::from_iter([
        (hdr::AUTHORIZATION, "Bearer reg-token-abcdefgh".to_string()),
        (hdr::COOKIE, "session=writeme".to_string()),
        (hdr::MSE_CONSUMER, "ak.gpustack-7".to_string()),
        (hdr::AUTH_CACHE, "jwt-cache-value".to_string()),
    ]);
    let mut out: Option<OutboundRequest> = None;
    let (bytes, allocs) = measure_counted(|| {
        out = Some(pipeline::build_outbound("POST", &p, &c, &wb, &[]));
    });
    eprintln!(
        "AM6b measured build_outbound 1 candidate + ext-auth write-back: {bytes} bytes / {allocs} allocs"
    );
    let out = out.expect("outbound built");
    // The write-back REPLACED the client key (exactly one Authorization).
    assert_eq!(
        out.headers.get(hdr::AUTHORIZATION),
        Some("Bearer reg-token-abcdefgh")
    );
    assert_eq!(out.headers.count(hdr::AUTHORIZATION), 1);
    assert_eq!(out.headers.get(hdr::MSE_CONSUMER), Some("ak.gpustack-7"));
    assert!(
        bytes < 4 * KIB as u64,
        "AM6b build_outbound + write-back allocated {bytes} bytes (AM6b measured 1417/39; ceiling < 4 KiB)"
    );
}

#[test]
fn am6_build_outbound_three_candidates_in_a_row() {
    let _g = TEST_LOCK.lock().unwrap();
    let inbound = realistic_model_route_inbound();
    let (data, table, shared, router) = model_route_env();
    let ctx = PipelineCtx {
        data: &data,
        table: &table,
        config: &shared,
        router: &router,
    };
    let p = pipeline::prepare(&inbound, &ctx).expect("fixture routes");
    // The SAME prepared request against 3 candidates in a row (failover shape).
    let cs = [
        candidate("model-1-10.static"),
        candidate("model-1-11.static"),
        candidate("model-1-12.static"),
    ];
    let mut built: usize = 0;
    let (bytes, allocs) = measure_counted(|| {
        for cand in &cs {
            let o = pipeline::build_outbound("POST", &p, cand, &HeaderMap::new(), &[]);
            std::hint::black_box(&o.headers);
            built += 1;
        }
    });
    assert_eq!(built, 3);
    eprintln!(
        "AM6b measured build_outbound 3 candidates in a row: {bytes} bytes / {allocs} allocs"
    );
    assert!(
        bytes < 4 * KIB as u64,
        "AM6b build_outbound (3 candidates) allocated {bytes} bytes (AM6b measured 3213/81; ceiling < 4 KiB)"
    );
}

#[test]
fn am6_dial_overlay_drain_and_materialize() {
    let _g = TEST_LOCK.lock().unwrap();
    let inbound = realistic_model_route_inbound();
    let (data, table, shared, router) = model_route_env();
    let ctx = PipelineCtx {
        data: &data,
        table: &table,
        config: &shared,
        router: &router,
    };
    let p = pipeline::prepare(&inbound, &ctx).expect("fixture routes");

    // AM-6b direct-dial drain: `outbound.headers` is a LAZY OVERLAY over the
    // (shared) `prepared.base_headers`. `into_pairs` emits the base entries
    // (cloned exactly once HERE — the AM-6 per-candidate deep copy moved out of
    // `build_outbound` to this one drain of the actually-dialed candidate) and
    // moves the candidate's delta strings.
    let out = pipeline::build_outbound(
        "POST",
        &p,
        &candidate("model-1-10.static"),
        &HeaderMap::new(),
        &[],
    );
    let mut pairs: Vec<(String, String)> = Vec::new();
    let (bytes, allocs) = measure_counted(|| {
        pairs = out.headers.into_pairs();
    });
    eprintln!(
        "AM6b measured dial pairs (overlay over shared base): {bytes} bytes / {allocs} allocs"
    );
    assert!(
        pairs
            .iter()
            .any(|(n, v)| n == "x-gpustack-model-instance" && v == "model-1-10.static")
    );
    assert!(
        pairs.iter().any(|(n, _)| n == ":path"),
        "the pseudo header stays in the overlay; the dial fold drops it"
    );
    assert!(
        pairs.iter().any(|(n, _)| n == "content-type"),
        "content-type stays in the overlay; DIAL_SKIP drops it at the fold"
    );
    assert!(
        !pairs.iter().any(|(n, _)| n == "content-length"),
        "content-length was hop-by-hop stripped in build_outbound"
    );
    assert!(
        !pairs.iter().any(|(n, _)| n == "host"),
        "host was hop-by-hop stripped in build_outbound (re-set from outbound.host)"
    );
    assert!(
        bytes < 2 * KIB as u64,
        "AM6b overlay dial drain allocated {bytes} bytes (AM6b measured 1012/25; ceiling < 2 KiB)"
    );

    // AM-6b provider branch: the frozen ProviderClient needs a FULL
    // CoreHeaderMap — the overlay is materialized once (ONE base deep copy,
    // paid only when a provider candidate is actually dialed).
    let out2 = pipeline::build_outbound(
        "POST",
        &p,
        &candidate("model-1-10.static"),
        &HeaderMap::new(),
        &[],
    );
    let mut map: Option<hygress_core::prelude::HeaderMap> = None;
    let (bytes_m, allocs_m) = measure_counted(|| {
        map = Some(out2.headers.materialize());
    });
    eprintln!(
        "AM6b measured provider-branch materialize: {bytes_m} bytes / {allocs_m} allocs"
    );
    let map = map.expect("materialized map");
    assert_eq!(
        map.get(hdr::MODEL_INSTANCE_OUT),
        Some("model-1-10.static")
    );
    assert_eq!(map.get("content-length"), None, "hop-by-hop stripped");
    assert_eq!(map.get("host"), None, "hop-by-hop stripped");
    assert_eq!(map.get(hdr::LLM_MODEL), Some("org1/llama-3-8b"));
    assert!(
        bytes_m < 4 * KIB as u64,
        "AM6b materialize allocated {bytes_m} bytes (AM6b measured 1696/50; ceiling < 4 KiB)"
    );

    // AM-6 budget kept: the materialized map is EXCLUSIVELY owned, so its drain
    // (`HeaderMap::into_pairs`) still MOVES every String (the old 672 B / 1
    // alloc exclusive-drain norm — unchanged semantics for full-map dials).
    let mut pairs_m: Vec<(String, String)> = Vec::new();
    let (bytes_x, allocs_x) = measure_counted(|| {
        pairs_m = map.into_pairs();
    });
    eprintln!(
        "AM6b measured dial pairs (exclusive materialized map drain): {bytes_x} bytes / {allocs_x} allocs"
    );
    assert!(
        pairs_m
            .iter()
            .any(|(n, v)| n == "x-gpustack-model-instance" && v == "model-1-10.static")
    );
    assert!(
        bytes_x < 2 * KIB as u64,
        "AM6b exclusive-map drain allocated {bytes_x} bytes (AM-6 measured 672 / 1 alloc; ceiling < 2 KiB)"
    );
}

#[test]
fn am8_p4_absent_remove_does_not_deep_copy() {
    let _g = TEST_LOCK.lock().unwrap();
    // A realistic inbound-size map (clone + two absent strip removes, P4).
    let mut h = HeaderMap::new();
    for (i, n) in [
        "host",
        "content-type",
        "authorization",
        "x-higress-llm-model",
        "cookie",
        "x-mse-consumer",
        "x-forwarded-for",
        "accept",
    ]
    .iter()
    .enumerate()
    {
        h.insert(n, format!("value-{i}"));
    }
    let shared = h.clone();
    let (b_absent, c_absent) = measure_counted(|| {
        let mut m = shared.clone(); // O(1) Arc bump, no copy
        m.remove("x-gpustack-auth-token"); // ① absent name -> NO deep copy (P4)
        m.remove("x-gpustack-model-instance"); // ① absent name -> NO deep copy
    });
    eprintln!("AM8 measured absent-removes (clone + 2 misses): {b_absent} bytes / {c_absent} allocs");
    assert!(
        c_absent <= 4 && b_absent < 4 * KIB as u64,
        "absent strip removes must not deep-copy the shared map: {b_absent}B / {c_absent} allocs"
    );

    // A PRESENT remove must still pay exactly ONE COW deep copy (semantics kept).
    let (b_present, c_present) = measure_counted(|| {
        let mut m = shared.clone();
        m.remove("authorization");
    });
    eprintln!("AM8 measured present-remove: {b_present} bytes / {c_present} allocs");
    assert!(
        c_present > c_absent && b_present > b_absent,
        "a present remove must trigger the one deep copy: {b_present}B / {c_present} allocs"
    );
}
