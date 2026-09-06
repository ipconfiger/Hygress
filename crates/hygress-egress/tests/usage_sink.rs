//! Integration tests for `usage_sink::GpustackSink` against a **real** local HTTP server.
//!
//! Verifies the exact wire contract of the `POST /v2/usage/gateway-metrics` report
//! (plugin-contract-pin.md §2.8 / §5.1): the `X-GPUStack-Auth-Token` header, the `Content-Type`,
//! and a body that is exactly the 17-field `ModelUsageMetrics` form. Also verifies the bounded
//! retry-then-drop (never spin) delivery behavior.

#[path = "common/mod.rs"]
mod common;

use hygress_core::usage::ModelUsageMetrics;
use hygress_egress::usage_sink::GpustackSink;
use serde_json::Value;

/// The 17-field `ModelUsageMetrics` wire form: 9 always-present scalars + 8
/// `Option` fields; a real flush always stamps started_at/completed_at, so 11
/// fields are present in practice plus the 6 attribution fields (G5-unified).
fn sample_metric() -> ModelUsageMetrics {
    ModelUsageMetrics {
        model: "org1/llama-3-8b".into(),
        input_token: 10,
        output_token: 5,
        total_token: 15,
        input_cached_token: 3,
        request_count: 1,
        completed: true,
        output_chunk_count: 12,
        request_content_bytes: 320,
        started_at: Some(1_700_000_000_000),
        completed_at: Some(1_700_000_003_000),
        user_id: Some(7),
        model_id: Some(42),
        model_route_id: Some(5),
        provider_id: Some(9),
        access_key: Some("key123".into()),
        organization_id: Some("org1".into()),
    }
}

const PINNED_FIELDS: [&str; 17] = [
    "model",
    "input_token",
    "output_token",
    "total_token",
    "input_cached_token",
    "request_count",
    "completed",
    "output_chunk_count",
    "request_content_bytes",
    "started_at",
    "completed_at",
    "user_id",
    "model_id",
    "model_route_id",
    "provider_id",
    "access_key",
    "organization_id",
];

async fn start_sink(server: &common::TestServer, token: &str) -> GpustackSink {
    let endpoint = format!("{}/v2/usage/gateway-metrics", server.base_url());
    // `None` drop hook: these tests only pin the wire/retry behavior (ORA3-M4 on_drop is covered
    // by the sink's unit tests, which hold a counting closure).
    GpustackSink::new(&endpoint, reqwest::Client::new(), token.to_string(), None)
}

#[tokio::test]
async fn push_posts_exact_17_field_body_with_correct_auth_header() {
    let server = common::TestServer::spawn().await;
    server.set_status(200);
    let token = hygress_egress::token::derive_gateway_token(b"secret");
    let sink = start_sink(&server, &token).await;

    let metric = sample_metric();
    sink.push(&metric).await.expect("push must succeed");

    let reqs = server.wait_for(1).await;
    let got = &reqs[0];

    // Method + path (pin §5.1).
    assert_eq!(got.method, "POST");
    assert_eq!(got.target, "/v2/usage/gateway-metrics");

    // Auth header (the derived gateway token, pin §5.1).
    assert_eq!(got.header("x-gpustack-auth-token"), Some(token.as_str()));
    // JSON content type.
    assert_eq!(got.header("content-type"), Some("application/json"));

    // Body == the exact 17-field wire form of the metric.
    let body: Value = serde_json::from_slice(&got.body).expect("body is valid JSON");
    let expected: Value = serde_json::to_value(&metric).unwrap();
    assert_eq!(
        body, expected,
        "wire body must equal the serialized ModelUsageMetrics"
    );

    // Exactly the 17 pinned fields (no server-side-only fields, no extras).
    let keys: std::collections::BTreeSet<&str> = body
        .as_object()
        .unwrap()
        .keys()
        .map(|s| s.as_str())
        .collect();
    let mut expected_keys: Vec<&str> = PINNED_FIELDS.to_vec();
    expected_keys.sort_unstable();
    let got_keys: Vec<&str> = keys.into_iter().collect::<Vec<_>>();
    assert_eq!(
        got_keys, expected_keys,
        "wire field set must be exactly the 17 pinned fields"
    );
    for forbidden in ["operation", "cluster_id", "provider_name", "provider_type"] {
        assert!(
            !body.as_object().unwrap().contains_key(forbidden),
            "{forbidden} must not be on the wire"
        );
    }
    // The `completed` flag must be present and true (we observed a usage chunk).
    assert_eq!(body["completed"], true);
    server.shutdown();
}

#[tokio::test]
async fn push_multiple_metrics_delivered_in_order() {
    let server = common::TestServer::spawn().await;
    server.set_status(200);
    let token = hygress_egress::token::derive_gateway_token(b"secret");
    let sink = start_sink(&server, &token).await;

    let m1 = sample_metric();
    let mut m2 = sample_metric();
    m2.model_route_id = Some(999);
    sink.push(&m1).await.unwrap();
    sink.push(&m2).await.unwrap();

    let reqs = server.wait_for(2).await;
    assert_eq!(reqs.len(), 2);
    let b1: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    let b2: Value = serde_json::from_slice(&reqs[1].body).unwrap();
    // Order preserved: m1 (model_route_id=5) then m2 (model_route_id=999).
    assert_eq!(b1["model_route_id"], 5);
    assert_eq!(b2["model_route_id"], 999);
    server.shutdown();
}

#[tokio::test]
async fn retries_bounded_times_then_drops_without_spinning() {
    // A 500 endpoint: each attempt is delivered and answered 500; the flusher retries a bounded
    // number of times (3) then drops the metric with a log — it must NOT spin forever.
    let server = common::TestServer::spawn().await;
    server.set_status(500);
    let token = hygress_egress::token::derive_gateway_token(b"secret");
    let sink = start_sink(&server, &token).await;

    sink.push(&sample_metric()).await.unwrap();

    // Wait for the 3 attempts to be observed.
    let _ = server.wait_for(3).await;

    // Give it time (backoffs are tiny): if it were spinning, the count would keep growing.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let n = server.count();
    assert!(
        (1..=3).contains(&n),
        "retries must be finite (<= 3 attempts), got {n}"
    );

    // Proving it stopped: the count is stable.
    let n_before = server.count();
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        server.count(),
        n_before,
        "flusher must stop (no infinite retries), count changed: {n_before} -> {}",
        server.count()
    );
    server.shutdown();
}

#[tokio::test]
async fn push_is_fire_and_forget_and_nonblocking() {
    // Push enqueues and returns Ok immediately (fire-and-forget), even under a blackhole endpoint
    // where the flusher will keep failing — it never blocks the caller or spins.
    let server = common::TestServer::spawn().await;
    server.set_blackhole(true);
    let token = hygress_egress::token::derive_gateway_token(b"secret");
    let sink = start_sink(&server, &token).await;

    let res = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sink.push(&sample_metric()),
    )
    .await;
    assert!(res.is_ok(), "push must not block");
    assert!(res.unwrap().is_ok(), "push returns Ok (fire-and-forget)");
    server.shutdown();
}
