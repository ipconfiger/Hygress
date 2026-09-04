//! **Real** end-to-end integration test for the terminate-mode data plane.
//!
//! Only built under `--features integrations` (see `[[test]] required-features` in
//! `Cargo.toml`) — it drives the actual Pingora [`HygressProxy`] against real local
//! HTTP/1.1 servers and asserts the frozen stage-①–⑭ semantics end to end:
//!
//! (a) model-route forward — path + `X-GPUStack-Model-Instance` +
//!     `X-GPUStack-Route-Name` + the ext-auth `Authorization` key swap reach the
//!     upstream, and the body `model` is preserved;
//! (b) mirror pass-through — a request with no `x-higress-llm-model` is forwarded
//!     verbatim to the mirror target;
//! (c) a real `401` from `/token-auth` short-circuits the request (401, upstream
//!     never contacted);
//! (d) an SSE response's `usage` object is pushed to `/v2/usage/gateway-metrics` as a
//!     `completed=true` 17-field row with the right attribution;
//! (e) a `503` from the model-route upstream triggers the bounded fallback
//!     re-dispatch to the linked Fallback route.
//!
//! Test doubles (real local TCP/HTTP servers) live in this `tests/` file only — the
//! implementation crates use no mocks.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hygress_core::prelude::{
    ConfigData, Destination, FallbackLink, ModelRouterSettings, OutboundProxy, PathPred,
    ProviderToken, Registry, RouteKind, RouteRule, SharedConfig,
};
use hygress_egress::forward_auth;
use hygress_egress::provider::ProviderClient;
use hygress_egress::token::derive_gateway_token;
use hygress_egress::usage_sink::GpustackSink;
use hygress_gateway::context::{GatewayState, SharedConfigHandle};
use hygress_gateway::metrics::Metrics;
use hygress_gateway::pipe::HygressProxy;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Test double: a real local HTTP/1.1 server that records every request and replies
// with a configurable status + headers + body.
// ---------------------------------------------------------------------------

/// One captured request (target = the request target/path).
#[derive(Clone, Debug)]
struct Rec {
    method: String,
    target: String,
    /// `(lowercased-name, value)` pairs, in wire order.
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Rec {
    fn header(&self, name: &str) -> Option<&str> {
        let n = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.as_str() == n)
            .map(|(_, v)| v.as_str())
    }
}

struct ServerState {
    recorded: Mutex<Vec<Rec>>,
    status: AtomicU16,
    resp_headers: Mutex<Vec<(String, String)>>,
    body: Mutex<Vec<u8>>,
    shutdown: Notify,
}

struct TestServer {
    addr: SocketAddr,
    state: Arc<ServerState>,
}

impl TestServer {
    async fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind 127.0.0.1:0");
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(ServerState {
            recorded: Mutex::new(Vec::new()),
            status: AtomicU16::new(200),
            resp_headers: Mutex::new(Vec::new()),
            body: Mutex::new(Vec::new()),
            shutdown: Notify::new(),
        });
        let accept_state = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = accept_state.shutdown.notified() => break,
                    res = listener.accept() => {
                        let (sock, _peer) = match res {
                            Ok(x) => x,
                            Err(_) => continue,
                        };
                        let st = accept_state.clone();
                        tokio::spawn(handle_connection(sock, st));
                    }
                }
            }
        });
        Self { addr, state }
    }

    fn addr_str(&self) -> String {
        self.addr.to_string()
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Set the response (status + headers + body) for subsequent requests.
    fn set_response(&self, status: u16, headers: Vec<(String, String)>, body: Vec<u8>) {
        self.state.status.store(status, Ordering::SeqCst);
        *self.state.resp_headers.lock().unwrap() = headers;
        *self.state.body.lock().unwrap() = body;
    }

    /// Wait (bounded) until at least `n` requests were recorded; return them.
    async fn wait_for(&self, n: usize) -> Vec<Rec> {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let len = self.state.recorded.lock().unwrap().len();
            if len >= n {
                return self.state.recorded.lock().unwrap().clone();
            }
            if Instant::now() > deadline {
                let got = self.state.recorded.lock().unwrap().clone();
                panic!("timed out waiting for {n} request(s), got {got:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn count(&self) -> usize {
        self.state.recorded.lock().unwrap().len()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.state.shutdown.notify_waiters();
    }
}

async fn handle_connection(mut sock: TcpStream, state: Arc<ServerState>) {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 8192];
    // Read until the header block completes (`\r\n\r\n`).
    loop {
        match sock.read(&mut tmp).await {
            Ok(0) => return,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => return,
        }
        if let Some(e) = find(&buf, b"\r\n\r\n") {
            if e + 4 <= buf.len() {
                break;
            }
        }
        if buf.len() > (1 << 20) {
            return;
        }
    }
    let header_end = find(&buf, b"\r\n\r\n").unwrap() + 4;
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let (method, target, headers) = parse_head(&head);
    let content_length = headers
        .iter()
        .find(|(k, _)| k.as_str() == "content-length")
        .map(|(_, v)| v.parse::<usize>().unwrap_or(0))
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        match sock.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&tmp[..n]),
            Err(_) => break,
        }
    }
    if body.len() > content_length {
        body.truncate(content_length);
    }
    state.recorded.lock().unwrap().push(Rec { method, target, headers, body });

    let status = state.status.load(Ordering::SeqCst);
    let resp_headers = state.resp_headers.lock().unwrap().clone();
    let resp_body = state.body.lock().unwrap().clone();
    let mut resp = String::new();
    resp.push_str(&format!("HTTP/1.1 {} {}\r\n", status, reason(status)));
    for (k, v) in &resp_headers {
        resp.push_str(&format!("{k}: {v}\r\n"));
    }
    resp.push_str(&format!("Content-Length: {}\r\n", resp_body.len()));
    resp.push_str("Connection: close\r\n");
    resp.push_str("\r\n");
    let _ = sock.write_all(resp.as_bytes()).await;
    let _ = sock.write_all(&resp_body).await;
    let _ = sock.flush().await;
    let _ = sock.shutdown().await;
}

fn reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        401 => "Unauthorized",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

fn parse_head(head: &str) -> (String, String, Vec<(String, String)>) {
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.splitn(3, ' ');
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some(idx) = line.find(':') {
            headers.push((line[..idx].trim().to_ascii_lowercase(), line[idx + 1..].trim().to_string()));
        }
    }
    (method, target, headers)
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if hay.len() < needle.len() {
        return None;
    }
    let last = hay.len() - needle.len();
    (0..=last).find(|&i| &hay[i..i + needle.len()] == needle)
}

// ---------------------------------------------------------------------------
// Config / state / gateway bootstrapping
// ---------------------------------------------------------------------------

/// One model-route (Main) + one Fallback route + one mirror, with destinations
/// pointing at the three local upstream servers (address `host:port`).
///
/// The model instance registry uses the real GPUStack grammar
/// `model-<model_id>-<instance_id>.<type>` (contract-pin §4.4) so the usage
/// attribution (`model_id`) parses end to end.
fn build_data(model_upstream: &str, mirror_upstream: &str, fallback_upstream: &str) -> ConfigData {
    let model_route = RouteRule::new(
        "org1/llama-3-8b",
        RouteKind::Main,
        vec![PathPred::new(".*")],
        vec![Destination::new("model-1-10.static:80")],
    )
    .unwrap()
    .with_ingress_name("higress-system/ai-route-route-1.internal")
    .with_fallback(FallbackLink::new("ai-route-route-5.internal"));
    let fallback_route = RouteRule::new(
        "ai-route-route-5.internal",
        RouteKind::Fallback,
        vec![PathPred::new(".*")],
        vec![Destination::new("fallback-5.static:80")],
    )
    .unwrap()
    // The real GPUStack fallback ingress name: `ai-route-route-<id>.fallback.
    // internal` (D7 — it is auth-scoped; the test's auth server answers both
    // hops with 200, and it is the source of `model_route_id` attribution).
    .with_ingress_name("higress-system/ai-route-route-5.fallback.internal");
    let mirror_route = RouteRule::new(
        "gpustack",
        RouteKind::Mirror,
        vec![PathPred::new("/")],
        vec![Destination::new("gpustack.static:80")],
    )
    .unwrap();
    ConfigData {
        routes: vec![model_route, fallback_route, mirror_route],
        registries: vec![
            Registry::new("model-1-10.static:80", model_upstream).unwrap(),
            Registry::new("fallback-5.static:80", fallback_upstream).unwrap(),
            Registry::new("gpustack.static:80", mirror_upstream).unwrap(),
        ],
        ..Default::default()
    }
}

fn build_state(
    data: ConfigData,
    auth_url: &str,
    usage_url: &str,
    http: reqwest::Client,
    token: String,
) -> Arc<GatewayState> {
    let shared = SharedConfig::new(data).expect("config is valid");
    Arc::new(GatewayState {
        config: Arc::new(SharedConfigHandle::new(shared)),
        auth: Some(Arc::new(
            forward_auth::Client::new(auth_url, http.clone()).with_auth_token(token.clone()),
        )),
        sink: Some(Arc::new(GpustackSink::new(
            usage_url,
            http.clone(),
            token.clone(),
        ))),
        upstream: Arc::new(ProviderClient),
        metrics: Arc::new(Metrics::new()),
    })
}

/// Boot a real Pingora terminate-mode server on an ephemeral `127.0.0.1` port on a
/// bare std thread (Pingora `run_forever` owns its own runtime — it cannot run
/// inside a `#[tokio::test]` runtime). Returns the gateway base URL.
async fn spawn_gateway(state: Arc<GatewayState>) -> String {
    let tmp = TcpListener::bind("127.0.0.1:0").await.expect("bind 127.0.0.1:0");
    let port = tmp.local_addr().unwrap().port();
    drop(tmp);
    let addr = format!("127.0.0.1:{port}");
    let proxy = HygressProxy::new(state);
    std::thread::spawn(move || match proxy.new_server(&addr) {
        Ok(server) => server.run_forever(), // blocking
        Err(e) => eprintln!("gateway failed to start listener: {e}"),
    });
    // Wait for the listener to accept.
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base).await;
    base
}

async fn wait_ready(base: &str) {
    let addr = base.trim_start_matches("http://");
    for _ in 0..200 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("gateway did not become ready at {base}");
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

#[tokio::test]
async fn model_route_forwards_model_instance_route_and_auth() {
    let model_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let fallback = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let _usage = TestServer::spawn().await;

    // ext-auth: 200 (allow) with the write-back header set.
    auth.set_response(
        200,
        vec![
            ("X-Mse-Consumer".into(), "ak123.gpustack-7".into()),
            ("Authorization".into(), "Bearer reg-token".into()),
            ("x-gpustack-auth-cache".into(), "jwt-cache".into()),
        ],
        b"ok".to_vec(),
    );
    model_upstream.set_response(
        200,
        vec![("content-type".into(), "application/json".into())],
        br#"{"id":"1"}"#.to_vec(),
    );

    let data = build_data(&model_upstream.addr_str(), &mirror.addr_str(), &fallback.addr_str());
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", _usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    // The client presents its OWN key upstream must never see.
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/llama-3-8b")
        .header("authorization", "Bearer sk-client")
        .header("content-type", "application/json")
        .body(r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let reqs = model_upstream.wait_for(1).await;
    let req = &reqs[0];
    assert_eq!(req.method, "POST");
    assert_eq!(req.target, "/v1/chat/completions");
    // ⑨ set-instance / route-name headers reach the upstream.
    assert_eq!(req.header("x-gpustack-model-instance"), Some("model-1-10.static"));
    assert_eq!(
        req.header("x-gpustack-route-name"),
        Some("higress-system/ai-route-route-1.internal")
    );
    // B4: the auth write-back REPLACES the client's Authorization — the
    // upstream sees exactly ONE Authorization (= the registration token); the
    // client key must not leak (appending would leave both visible).
    let auths: Vec<&str> = req
        .headers
        .iter()
        .filter(|(k, _)| k.as_str() == "authorization")
        .map(|(_, v)| v.as_str())
        .collect();
    assert_eq!(auths, vec!["Bearer reg-token"], "exactly one Authorization: {auths:?}");
    // The body model is preserved (no model-mapper mapping for this target).
    let body = String::from_utf8_lossy(&req.body);
    assert!(body.contains("\"model\":\"org1/llama-3-8b\""), "body was {body}");
}

#[tokio::test]
async fn mirror_passes_through() {
    let model_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let fallback = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let usage = TestServer::spawn().await;

    mirror.set_response(
        200,
        vec![("content-type".into(), "text/plain".into())],
        b"mirror-response".to_vec(),
    );

    let data = build_data(&model_upstream.addr_str(), &mirror.addr_str(), &fallback.addr_str());
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    // No `x-higress-llm-model` → mirror catch-all, verbatim forward. A
    // client-forged instance header must also be stripped (stage ①).
    let resp = reqwest::Client::new()
        .get(format!("{gw}/some/other/path"))
        .header("x-gpustack-model-instance", "forged")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "mirror-response");

    let reqs = mirror.wait_for(1).await;
    assert_eq!(reqs[0].method, "GET");
    assert_eq!(reqs[0].target, "/some/other/path");
    // NB6: mirror / passthrough traffic carries NO instance / route-name
    // headers (they identify a concrete model worker instance; the mirror is
    // the GPUStack server itself).
    assert_eq!(reqs[0].header("x-gpustack-model-instance"), None, "mirror must not carry X-GPUStack-Model-Instance");
    assert_eq!(reqs[0].header("x-gpustack-route-name"), None, "mirror must not carry X-GPUStack-Route-Name");
    // The model-route upstream must NOT be contacted.
    assert_eq!(model_upstream.count(), 0);
}

#[tokio::test]
async fn bad_key_is_401() {
    let model_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let fallback = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let usage = TestServer::spawn().await;

    // ext-auth: a real 401 (a genuine rejection, not a fail-open 5xx/transport).
    auth.set_response(401, vec![], b"denied".to_vec());

    let data = build_data(&model_upstream.addr_str(), &mirror.addr_str(), &fallback.addr_str());
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/llama-3-8b")
        .header("content-type", "application/json")
        .body(r#"{"model":"org1/llama-3-8b"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "a real 401 from /token-auth must short-circuit");
    // The upstream is never contacted (the pipeline stops at stage ⑤).
    assert_eq!(model_upstream.count(), 0);
    // NB7: no usage row is reported for an auth-denied request (no upstream
    // was reached). The sink is fire-and-forget, so allow it a moment.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(usage.count(), 0, "auth-denied must not report usage");
}

#[tokio::test]
async fn sse_usage_is_pushed_completed() {
    let model_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let fallback = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let usage = TestServer::spawn().await;

    auth.set_response(
        200,
        vec![("X-Mse-Consumer".into(), "ak1.gpustack-7".into())],
        b"ok".to_vec(),
    );
    // An SSE stream whose final event carries the `usage` object.
    let sse = b"data: {\"choices\":[{\"delta\":{\"content\":\"H\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15,\"prompt_tokens_details\":{\"cached_tokens\":3}}}\n\ndata: [DONE]\n\n";
    model_upstream.set_response(
        200,
        vec![("content-type".into(), "text/event-stream".into())],
        sse.to_vec(),
    );

    let data = build_data(&model_upstream.addr_str(), &mirror.addr_str(), &fallback.addr_str());
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/llama-3-8b")
        .header("content-type", "application/json")
        .header("X-Organization-Id", "org1")
        .body(r#"{"model":"org1/llama-3-8b","stream":true}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // ⑫ the usage record is pushed to the sink as a completed 17-field row.
    let reqs = usage.wait_for(1).await;
    let body = String::from_utf8_lossy(&reqs[0].body).to_string();
    assert_eq!(reqs[0].target, "/v2/usage/gateway-metrics");
    assert!(body.contains("\"completed\":true"), "body was {body}");
    assert!(body.contains("\"model\":\"org1/llama-3-8b\""), "body was {body}");
    assert!(body.contains("\"model_route_id\":1"), "body was {body}");
    // B1: `model_id` is non-null and correct — parsed from the selected
    // destination service name `model-1-10.static` (`model-<mid>-<iid>`).
    // Without it the GPUStack server drops EVERY row.
    assert!(body.contains("\"model_id\":1"), "body was {body}");
    // A model-instance destination carries no provider id (omitempty — absent).
    assert!(!body.contains("\"provider_id\":"), "body was {body}");
    assert!(body.contains("\"user_id\":7"), "body was {body}");
    assert!(body.contains("\"input_token\":10"), "body was {body}");
    assert!(body.contains("\"output_token\":5"), "body was {body}");
    assert!(body.contains("\"total_token\":15"), "body was {body}");
    assert!(body.contains("\"input_cached_token\":3"), "body was {body}");
    assert!(body.contains("\"organization_id\":\"org1\""), "body was {body}");
}

#[tokio::test]
async fn upstream_503_falls_back() {
    let model_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let fallback = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let usage = TestServer::spawn().await;

    // The model-route upstream returns 503 (a single candidate → no retry, straight to ⑭).
    model_upstream.set_response(503, vec![], b"unavailable".to_vec());
    // The linked Fallback route's upstream succeeds.
    fallback.set_response(
        200,
        vec![("content-type".into(), "text/plain".into())],
        b"fallback-response".to_vec(),
    );
    auth.set_response(
        200,
        vec![("X-Mse-Consumer".into(), "ak1.gpustack-7".into())],
        b"ok".to_vec(),
    );

    let data = build_data(&model_upstream.addr_str(), &mirror.addr_str(), &fallback.addr_str());
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/llama-3-8b")
        .header("content-type", "application/json")
        .body(r#"{"model":"org1/llama-3-8b"}"#)
        .send()
        .await
        .unwrap();
    // The fallback hop succeeds → the client sees the fallback upstream's 200.
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "fallback-response");
    // The fallback target received the (restored) original path.
    let reqs = fallback.wait_for(1).await;
    assert_eq!(reqs[0].target, "/v1/chat/completions");
    // The model-route upstream was tried once (its 503 triggered the fallback).
    assert_eq!(model_upstream.count(), 1);
    // D7: the fallback hop's usage row parses `model_route_id` from the
    // fallback route name `.../ai-route-route-5.fallback.internal` (the `.
    // fallback` suffix used to break the parse → model_route_id None).
    let reqs = usage.wait_for(1).await;
    let body = String::from_utf8_lossy(&reqs[0].body).to_string();
    assert!(body.contains("\"model_route_id\":5"), "fallback usage row: {body}");
    assert!(body.contains("\"model\":\"org1/llama-3-8b\""), "fallback usage row: {body}");
}

#[tokio::test]
async fn model_router_settings_from_snapshot_arm_routing() {
    // B2: the gateway builds the stage-② model-router config from the
    // **current snapshot** (`ConfigData.model_router`). If it kept using
    // `ModelRouterConfig::default()` (empty `enableOnPathSuffix`, empty
    // `aliasNameMapping`), neither request below could reach the model route.
    let model_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let _usage = TestServer::spawn().await;

    model_upstream.set_response(
        200,
        vec![("content-type".into(), "application/json".into())],
        br#"{"id":"1"}"#.to_vec(),
    );
    auth.set_response(200, vec![("X-Mse-Consumer".into(), "none".into())], b"ok".to_vec());

    let model_route = RouteRule::new(
        "org1/llama-3-8b",
        RouteKind::Main,
        vec![PathPred::new(".*")],
        vec![Destination::new("model-1-10.static:80")],
    )
    .unwrap()
    .with_ingress_name("higress-system/ai-route-route-1.internal");
    let mirror_route = RouteRule::new(
        "gpustack",
        RouteKind::Mirror,
        vec![PathPred::new("/")],
        vec![Destination::new("gpustack.static:80")],
    )
    .unwrap();
    let data = ConfigData {
        routes: vec![model_route, mirror_route],
        registries: vec![
            Registry::new("model-1-10.static:80", model_upstream.addr_str()).unwrap(),
            Registry::new("gpustack.static:80", mirror.addr_str()).unwrap(),
        ],
        // The `gpustack-model-router` defaultConfig as GPUStack writes it
        // (contract-pin §2.3).
        model_router: ModelRouterSettings {
            prefix: "/model/proxy/".into(),
            target_header: "x-higress-llm-model".into(),
            enable_on_path_suffix: vec!["/v1/chat/completions".into()],
            alias_name_mapping: {
                let mut m = std::collections::BTreeMap::new();
                m.insert("7".to_string(), "org1/llama-3-8b".to_string());
                m
            },
            max_body_bytes: Some(1024 * 1024),
        },
        ..Default::default()
    };
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", _usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;
    let client = reqwest::Client::new();

    // (a) BODY-DRIVEN: `enableOnPathSuffix` (from the snapshot) arms
    //     /v1/chat/completions — the model is read from the body.
    let resp = client
        .post(format!("{gw}/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "body-driven must reach the model route");

    // (b) PATH ALIAS: /model/proxy/7/... resolves `aliasNameMapping["7"]` and
    //     rewrites the body `model` to the alias value.
    let resp = client
        .post(format!("{gw}/model/proxy/7/v1/chat/completions"))
        .header("content-type", "application/json")
        .body(r#"{"model":"client-alias","messages":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "alias must reach the model route");

    let reqs = model_upstream.wait_for(2).await;
    assert_eq!(reqs[0].target, "/v1/chat/completions");
    assert_eq!(reqs[1].target, "/model/proxy/7/v1/chat/completions");
    let body = String::from_utf8_lossy(&reqs[1].body);
    assert!(body.contains("\"model\":\"org1/llama-3-8b\""), "alias body rewrite: {body}");
    // Neither request fell through to the mirror.
    assert_eq!(mirror.count(), 0);
}

#[tokio::test]
async fn terminal_non2xx_reports_incomplete_usage() {
    // NB7: a terminal non-2xx that **reached an upstream** still reports usage:
    // `completed=false`, zero tokens, `request_content_bytes` set, full
    // attribution. (Not for auth-denied / 404-no-route / transport failure.)
    let model_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let usage = TestServer::spawn().await;

    model_upstream.set_response(500, vec![], br#"{"error":"boom"}"#.to_vec());
    auth.set_response(
        200,
        vec![("X-Mse-Consumer".into(), "ak1.gpustack-7".into())],
        b"ok".to_vec(),
    );

    // A model route WITHOUT a fallback link: the 500 is terminal.
    let model_route = RouteRule::new(
        "org1/llama-3-8b",
        RouteKind::Main,
        vec![PathPred::new(".*")],
        vec![Destination::new("model-1-10.static:80")],
    )
    .unwrap()
    .with_ingress_name("higress-system/ai-route-route-1.internal");
    let mirror_route = RouteRule::new(
        "gpustack",
        RouteKind::Mirror,
        vec![PathPred::new("/")],
        vec![Destination::new("gpustack.static:80")],
    )
    .unwrap();
    let data = ConfigData {
        routes: vec![model_route, mirror_route],
        registries: vec![
            Registry::new("model-1-10.static:80", model_upstream.addr_str()).unwrap(),
            Registry::new("gpustack.static:80", mirror.addr_str()).unwrap(),
        ],
        ..Default::default()
    };
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    let body = r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}]}"#;
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/llama-3-8b")
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    // The client sees the upstream's 500 verbatim.
    assert_eq!(resp.status(), 500);

    let reqs = usage.wait_for(1).await;
    let ub = String::from_utf8_lossy(&reqs[0].body).to_string();
    assert!(reqs[0].target == "/v2/usage/gateway-metrics");
    assert!(ub.contains("\"completed\":false"), "row: {ub}");
    assert!(ub.contains("\"input_token\":0"), "row: {ub}");
    assert!(ub.contains("\"output_token\":0"), "row: {ub}");
    assert!(ub.contains("\"total_token\":0"), "row: {ub}");
    assert!(
        ub.contains(&format!("\"request_content_bytes\":{}", body.len())),
        "row: {ub}"
    );
    // B1 attribution: model_id from `model-1-10.static`, route id from the
    // ingress name.
    assert!(ub.contains("\"model_id\":1"), "row: {ub}");
    assert!(ub.contains("\"model_route_id\":1"), "row: {ub}");
    assert!(ub.contains("\"user_id\":7"), "row: {ub}");
}

#[tokio::test]
async fn proxied_target_routes_through_outbound_proxy() {
    // D8: a `proxy`-kind registry routes the request **through the outbound
    // forward proxy** (HTTP-proxy semantics: the proxy receives the absolute
    // origin URI). The true origin is never dialed directly. The destination
    // is `provider-9.proxy` → the usage row carries `provider_id` (B1).
    let mirror = TestServer::spawn().await;
    let proxy = TestServer::spawn().await; // the outbound forward proxy
    let auth = TestServer::spawn().await;
    let usage = TestServer::spawn().await;

    proxy.set_response(
        200,
        vec![("content-type".into(), "text/plain".into())],
        b"via-proxy".to_vec(),
    );
    auth.set_response(
        200,
        vec![("X-Mse-Consumer".into(), "ak1.gpustack-7".into())],
        b"ok".to_vec(),
    );

    let model_route = RouteRule::new(
        "org1/gpt-4o",
        RouteKind::Main,
        vec![PathPred::new(".*")],
        vec![Destination::new("provider-9.proxy:443")],
    )
    .unwrap()
    .with_ingress_name("higress-system/ai-route-route-2.internal");
    let mirror_route = RouteRule::new(
        "gpustack",
        RouteKind::Mirror,
        vec![PathPred::new("/")],
        vec![Destination::new("gpustack.static:80")],
    )
    .unwrap();
    let data = ConfigData {
        routes: vec![model_route, mirror_route],
        registries: vec![
            Registry::new("provider-9.proxy:443", "api.upstream.example.com")
                .unwrap()
                .with_proxy_ref("egress-a"),
            Registry::new("gpustack.static:80", mirror.addr_str()).unwrap(),
        ],
        proxies: vec![OutboundProxy::new("egress-a", "127.0.0.1", proxy.addr.port())],
        ..Default::default()
    };
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/gpt-4o")
        .header("content-type", "application/json")
        .body(r#"{"model":"org1/gpt-4o","messages":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "via-proxy");

    // The outbound proxy received the request in absolute form (HTTP proxy
    // semantics); the origin host was only ever referenced, never dialed.
    let reqs = proxy.wait_for(1).await;
    assert_eq!(reqs[0].method, "POST");
    assert_eq!(
        reqs[0].target,
        "http://api.upstream.example.com:443/v1/chat/completions"
    );

    // B1 + D8: the usage row is attributed to the PROVIDER (provider_id=9, no
    // model_id — `provider-9.proxy` carries no model id).
    let reqs = usage.wait_for(1).await;
    let ub = String::from_utf8_lossy(&reqs[0].body).to_string();
    assert!(ub.contains("\"provider_id\":9"), "row: {ub}");
    assert!(!ub.contains("\"model_id\":"), "row: {ub}");
    assert!(ub.contains("\"model_route_id\":2"), "row: {ub}");
}

#[tokio::test]
async fn https_proxied_target_tunnels_connect() {
    // D8 https: the endpoint's scheme is derived from the registry domain
    // (`https://...`). Reached through the outbound proxy, the gateway opens a
    // forward-proxy `CONNECT` tunnel to the origin host:port. The test proxy
    // records the CONNECT (proving the scheme + proxy plumbing) but cannot
    // complete the TLS handshake → transport failure → 502.
    let mirror = TestServer::spawn().await;
    let proxy = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let _usage = TestServer::spawn().await;

    proxy.set_response(200, vec![], Vec::new()); // CONNECT "established"
    auth.set_response(200, vec![("X-Mse-Consumer".into(), "none".into())], b"ok".to_vec());

    let model_route = RouteRule::new(
        "org1/tls-model",
        RouteKind::Main,
        vec![PathPred::new(".*")],
        vec![Destination::new("provider-8.proxy:443")],
    )
    .unwrap()
    .with_ingress_name("higress-system/ai-route-route-3.internal");
    let mirror_route = RouteRule::new(
        "gpustack",
        RouteKind::Mirror,
        vec![PathPred::new("/")],
        vec![Destination::new("gpustack.static:80")],
    )
    .unwrap();
    let data = ConfigData {
        routes: vec![model_route, mirror_route],
        registries: vec![
            // `https://` domain → Scheme::Https; address is scheme-stripped.
            Registry::new("provider-8.proxy:443", "https://api.tls.example.com")
                .unwrap()
                .with_proxy_ref("egress-tls"),
            Registry::new("gpustack.static:80", mirror.addr_str()).unwrap(),
        ],
        proxies: vec![OutboundProxy::new("egress-tls", "127.0.0.1", proxy.addr.port())],
        ..Default::default()
    };
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", _usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/tls-model")
        .header("content-type", "application/json")
        .body(r#"{"model":"org1/tls-model","messages":[]}"#)
        .send()
        .await
        .unwrap();
    // The test proxy cannot complete the TLS handshake (it is not a TLS
    // server) → transport failure → 502.
    assert_eq!(resp.status(), 502);

    // The proxy saw a `CONNECT` tunnellisation for the https origin.
    let reqs = proxy.wait_for(1).await;
    assert_eq!(reqs[0].method, "CONNECT");
    assert_eq!(reqs[0].target, "api.tls.example.com:443");
}

#[tokio::test]
async fn provider_destination_swaps_api_token() {
    // D6 / §7 ai-proxy: a `provider-<id>.<type>`-destined request swaps the
    // outbound `Authorization` to the provider's `apiToken` — the provider upstream
    // sees the provider key, NEVER the client/registration key. The live
    // `ProviderClient` build runs on the data-plane forward path.
    let provider_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let _usage = TestServer::spawn().await;

    // ext-auth: allow, with the registration-token write-back (which the swap must
    // override for a provider destination).
    auth.set_response(
        200,
        vec![
            ("X-Mse-Consumer".into(), "ak1.gpustack-7".into()),
            ("Authorization".into(), "Bearer reg-token".into()),
        ],
        b"ok".to_vec(),
    );
    provider_upstream.set_response(
        200,
        vec![("content-type".into(), "application/json".into())],
        br#"{"id":"1"}"#.to_vec(),
    );

    // A Main route to a PROVIDER destination `provider-7.static:443`, plus the
    // per-destination `apiTokens`.
    let model_route = RouteRule::new(
        "org1/gpt-4o",
        RouteKind::Main,
        vec![PathPred::new(".*")],
        vec![Destination::new("provider-7.static:443")],
    )
    .unwrap()
    .with_ingress_name("higress-system/ai-route-route-1.internal");
    let mirror_route = RouteRule::new(
        "gpustack",
        RouteKind::Mirror,
        vec![PathPred::new("/")],
        vec![Destination::new("gpustack.static:80")],
    )
    .unwrap();
    let data = ConfigData {
        routes: vec![model_route, mirror_route],
        registries: vec![
            Registry::new("provider-7.static:443", provider_upstream.addr_str()).unwrap(),
            Registry::new("gpustack.static:80", mirror.addr_str()).unwrap(),
        ],
        // D6 / §7: the provider's `apiTokens`, keyed by the destination `name.type`
        // (no port).
        provider_tokens: vec![ProviderToken {
            service: "provider-7.static".into(),
            ingress_scope: None, // applies to every ingress selecting this provider
            api_tokens: vec!["sk-provider-7-token".into()],
        }],
        ..Default::default()
    };
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", _usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    // The client presents its OWN key the provider must never see.
    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/gpt-4o")
        .header("authorization", "Bearer sk-client")
        .header("content-type", "application/json")
        .body(r#"{"model":"org1/gpt-4o","messages":[{"role":"user","content":"hi"}]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The provider upstream saw the PROVIDER's `apiToken` as the Authorization,
    // exactly once — never the registration write-back nor the client key.
    let reqs = provider_upstream.wait_for(1).await;
    let req = &reqs[0];
    let auths: Vec<&str> = req
        .headers
        .iter()
        .filter(|(k, _)| k.as_str() == "authorization")
        .map(|(_, v)| v.as_str())
        .collect();
    assert_eq!(
        auths,
        vec!["Bearer sk-provider-7-token"],
        "provider Authorization must be the apiToken: {auths:?}"
    );
    // The instance / route-name headers are still set (model-route traffic).
    assert_eq!(req.header("x-gpustack-model-instance"), Some("provider-7.static"));
    assert_eq!(
        req.header("x-gpustack-route-name"),
        Some("higress-system/ai-route-route-1.internal")
    );
}

#[tokio::test]
async fn provider_ingress_scoped_token_wins_over_global() {
    // D6 / §7 keying: when the route's ingress matches a per-ingress
    // (`ingress_scope`) token, the scoped token wins over the global one for the
    // same provider destination.
    let provider_upstream = TestServer::spawn().await;
    let mirror = TestServer::spawn().await;
    let auth = TestServer::spawn().await;
    let _usage = TestServer::spawn().await;

    auth.set_response(
        200,
        vec![("X-Mse-Consumer".into(), "ak1.gpustack-7".into())],
        b"ok".to_vec(),
    );
    provider_upstream.set_response(
        200,
        vec![("content-type".into(), "application/json".into())],
        br#"{"id":"1"}"#.to_vec(),
    );

    let model_route = RouteRule::new(
        "org1/gpt-4o",
        RouteKind::Main,
        vec![PathPred::new(".*")],
        vec![Destination::new("provider-7.static:443")],
    )
    .unwrap()
    .with_ingress_name("higress-system/ai-route-route-1.internal");
    let mirror_route = RouteRule::new(
        "gpustack",
        RouteKind::Mirror,
        vec![PathPred::new("/")],
        vec![Destination::new("gpustack.static:80")],
    )
    .unwrap();
    let data = ConfigData {
        routes: vec![model_route, mirror_route],
        registries: vec![
            Registry::new("provider-7.static:443", provider_upstream.addr_str()).unwrap(),
            Registry::new("gpustack.static:80", mirror.addr_str()).unwrap(),
        ],
        // A GLOBAL token + an ingress-SCOPED token for the same provider; the
        // route's ingress (`ai-route-route-1.internal`) matches the scoped scope.
        provider_tokens: vec![
            ProviderToken {
                service: "provider-7.static".into(),
                ingress_scope: None,
                api_tokens: vec!["sk-provider-7-global".into()],
            },
            ProviderToken {
                service: "provider-7.static".into(),
                ingress_scope: Some("ai-route-route-1.internal".into()),
                api_tokens: vec!["sk-provider-7-scoped".into()],
            },
        ],
        ..Default::default()
    };
    let http = reqwest::Client::new();
    let token = derive_gateway_token(b"test-secret");
    let state = build_state(
        data,
        &auth.base_url(),
        &format!("{}/v2/usage/gateway-metrics", _usage.base_url()),
        http,
        token,
    );
    let gw = spawn_gateway(state).await;

    let resp = reqwest::Client::new()
        .post(format!("{gw}/v1/chat/completions"))
        .header("x-higress-llm-model", "org1/gpt-4o")
        .header("authorization", "Bearer sk-client")
        .header("content-type", "application/json")
        .body(r#"{"model":"org1/gpt-4o","messages":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The ingress-scoped token wins over the global one for this route.
    let reqs = provider_upstream.wait_for(1).await;
    let auths: Vec<&str> = reqs[0]
        .headers
        .iter()
        .filter(|(k, _)| k.as_str() == "authorization")
        .map(|(_, v)| v.as_str())
        .collect();
    assert_eq!(auths, vec!["Bearer sk-provider-7-scoped"]);
}
