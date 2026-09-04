//! Integration tests for `forward_auth::Client` against a **real** local HTTP server.
//!
//! These verify the exact wire contract of ext-auth forward-auth (plugin-contract-pin.md
//! §2.1 / §5.3): the outbound `GET /token-auth` header allowlist + `X-GPUStack-Auth-Token`
//! injection, the 2xx write-back header parsing, the 4xx rejection verdict, and the FAIL_OPEN
//! behavior on 5xx and on timeout.

#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use http::HeaderMap;
use hygress_egress::forward_auth::{Client, ForwardAuthRequest};

fn client() -> reqwest::Client {
    // No global timeout; per-request timeouts are set on the egress Client.
    reqwest::Client::new()
}

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(
            http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            http::HeaderValue::from_bytes(v.as_bytes()).unwrap(),
        );
    }
    h
}

/// The six allowlisted inbound headers (pin §5.3) + the derived token to inject.
const ALLOWLIST_IN: &[(&str, &str)] = &[
    ("x-real-ip", "203.0.113.7"),
    ("x-forwarded-for", "203.0.113.7, 10.0.0.1"),
    ("x-higress-llm-model", "org1/llama-3-8b"),
    ("x-api-key", "sk-client-abc"),
    ("cookie", "session=abc; path=/"),
    ("x-gpustack-auth-cache", "cache-jwt-123"),
];

#[tokio::test]
async fn token_auth_get_path_and_forwards_allowlist_and_injects_token() {
    let server = common::TestServer::spawn().await;
    // 200 with the write-back headers.
    server.set_headers(vec![
        ("X-Mse-Consumer", "sk-abc.gpustack-7"),
        ("Authorization", "Bearer reg-token-1"),
        ("cookie", "dummy=dummy"),
        ("x-gpustack-auth-cache", "jwt-cache-abc"),
    ]);

    let token = hygress_egress::token::derive_gateway_token(b"secret");
    let c = Client::new(&server.base_url(), client()).with_auth_token(token.clone());

    // Inbound: the 6 allowlisted headers + two that must NOT be forwarded.
    let mut inbound = headers(ALLOWLIST_IN);
    inbound.insert(
        http::HeaderName::from_bytes("x-foo".as_bytes()).unwrap(),
        http::HeaderValue::from_static("bar"),
    );
    inbound.insert(
        http::HeaderName::from_bytes("x-gpustack-model-instance".as_bytes()).unwrap(),
        http::HeaderValue::from_static("forged-9-9.static"),
    );
    let req = ForwardAuthRequest::new(inbound);

    let verdict = c.authenticate(&req).await.unwrap();
    let v = verdict.expect("2xx must yield a verdict");

    // 2xx → authenticated, with the four write-back fields parsed from the response headers.
    assert!(v.authenticated);
    assert_eq!(v.consumer.as_deref(), Some("sk-abc.gpustack-7"));
    assert_eq!(v.authorization.as_deref(), Some("Bearer reg-token-1"));
    assert_eq!(v.set_cookie.as_deref(), Some("dummy=dummy"));
    assert_eq!(v.auth_cache.as_deref(), Some("jwt-cache-abc"));

    // The outbound request was a GET /token-auth.
    let server_reqs = server.wait_for(1).await;
    let got = &server_reqs[0];
    assert_eq!(got.method, "GET");
    assert_eq!(got.target, "/token-auth");

    // Exactly the six allowlisted inbound headers were forwarded.
    for (k, val) in ALLOWLIST_IN {
        assert_eq!(got.header(k), Some(*val), "must forward {k}");
    }
    // The derived token was injected (the gateway's own value, not forwarded from the request).
    assert_eq!(got.header("x-gpustack-auth-token"), Some(token.as_str()));
    // Non-allowlisted inbound headers were NOT forwarded.
    assert!(!got.has_header("x-foo"), "x-foo must not be forwarded");
    assert!(
        !got.has_header("x-gpustack-model-instance"),
        "x-gpustack-model-instance must not be forwarded (inbound-spoofable)"
    );

    server.shutdown();
}

#[tokio::test]
async fn client_forged_token_is_not_forwarded() {
    // Even if a client somehow set x-gpustack-auth-token inbound, the gateway must replace it
    // with its own injected value (never forward the client's).
    let server = common::TestServer::spawn().await;
    let token = hygress_egress::token::derive_gateway_token(b"mytoken");
    let c = Client::new(&server.base_url(), client()).with_auth_token(token.clone());

    let mut inbound = headers(ALLOWLIST_IN);
    inbound.insert(
        http::HeaderName::from_bytes("x-gpustack-auth-token".as_bytes()).unwrap(),
        http::HeaderValue::from_static("client-forged-token"),
    );

    let _ = c
        .authenticate(&ForwardAuthRequest::new(inbound))
        .await
        .unwrap();
    let got = &server.wait_for(1).await[0];
    // The injected token is exactly one and equals the gateway's derived value.
    assert_eq!(got.header("x-gpustack-auth-token"), Some(token.as_str()));
    // The client's forged value is not present.
    assert!(!got.headers.iter().any(|(_, v)| v == "client-forged-token"));
    server.shutdown();
}

#[tokio::test]
async fn no_token_configured_means_no_injection() {
    // A Client built without `with_auth_token` does not inject X-GPUStack-Auth-Token.
    let server = common::TestServer::spawn().await;
    let c = Client::new(&server.base_url(), client());
    let _ = c
        .authenticate(&ForwardAuthRequest::new(headers(ALLOWLIST_IN)))
        .await
        .unwrap();
    let got = &server.wait_for(1).await[0];
    assert!(!got.has_header("x-gpustack-auth-token"));
    server.shutdown();
}

#[tokio::test]
async fn four_xx_returns_authenticated_false() {
    let server = common::TestServer::spawn().await;
    server.set_status(401); // Unauthorized → a real rejection (not fail-open).

    let c = Client::new(&server.base_url(), client());
    let v = c
        .authenticate(&ForwardAuthRequest::new(headers(ALLOWLIST_IN)))
        .await
        .unwrap()
        .expect("4xx must still yield a verdict (a real result, not fail-open)");
    assert!(!v.authenticated);
    server.shutdown();
}

#[tokio::test]
async fn five_xx_is_fail_open_none() {
    let server = common::TestServer::spawn().await;
    server.set_status(503); // 5xx → FAIL_OPEN.

    let c = Client::new(&server.base_url(), client());
    let v = c
        .authenticate(&ForwardAuthRequest::new(headers(ALLOWLIST_IN)))
        .await
        .unwrap();
    assert!(v.is_none(), "5xx must be FAIL_OPEN (None)");
    server.shutdown();
}

#[tokio::test]
async fn transport_error_is_fail_open_none() {
    // Point at a closed port → connection refused (transport error) → FAIL_OPEN.
    let c = Client::new("http://127.0.0.1:1", client()).with_timeout(Duration::from_secs(2));
    let v = c
        .authenticate(&ForwardAuthRequest::new(headers(ALLOWLIST_IN)))
        .await
        .unwrap();
    assert!(v.is_none(), "transport error must be FAIL_OPEN (None)");
}

#[tokio::test]
async fn timeout_is_fail_open_none() {
    // A server that holds the connection open (never responds) + a short client timeout.
    let server = common::TestServer::spawn().await;
    server.set_blackhole(true);
    let c = Client::new(&server.base_url(), client()).with_timeout(Duration::from_millis(250));
    let v = c
        .authenticate(&ForwardAuthRequest::new(headers(ALLOWLIST_IN)))
        .await
        .unwrap();
    assert!(v.is_none(), "timeout must be FAIL_OPEN (None)");
    server.shutdown();
}

#[tokio::test]
async fn slow_500_over_timeout_still_fail_open() {
    // Delay longer than the timeout → the read times out (FAIL_OPEN), even though a 500 would
    // arrive just after.
    let server = common::TestServer::spawn().await;
    server.set_status(503);
    server.set_delay_ms(3000);
    let c = Client::new(&server.base_url(), client()).with_timeout(Duration::from_millis(200));
    let v = c
        .authenticate(&ForwardAuthRequest::new(headers(ALLOWLIST_IN)))
        .await
        .unwrap();
    assert!(v.is_none(), "timeout must be FAIL_OPEN (None)");
    server.shutdown();
}
