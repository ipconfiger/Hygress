//! `GpustackSink` — out-of-band usage push to `POST /v2/usage/gateway-metrics` (native
//! equivalent of the `gpustack-token-usage` plugin; design §7; plugin-contract-pin §2.8 / §5.1).
//!
//! The sink serializes a complete [`hygress_core::usage::ModelUsageMetrics`] (the exact 17-field
//! wire form, including the `completed` flag and the omitempty attribution fields) and POSTs it with
//! `X-GPUStack-Auth-Token` = the derived gateway token. Delivery is **fire-and-forget**:
//!
//! - [`GpustackSink::push`] enqueues the serialized payload onto a bounded in-memory channel and
//!   returns `Ok(())` immediately — it never blocks and never spins. When the buffer is full (or the
//!   flusher is gone) the metric is dropped (log + the `on_drop` callback passed to
//!   [`GpustackSink::new`], when one is given).
//! - One background flusher (spawned per [`GpustackSink::new`]) drains the channel and POSTs each
//!   payload. On a **transient** failure (transport error, `429`, 5xx) it retries a bounded number
//!   of times with a small backoff and then **drops the metric** (log + `on_drop`) — it never
//!   retries forever. A deterministic 4xx (e.g. a 401 from a bad token, 400/404) is **not** retried
//!   at all (MINOR-4): the metric is dropped after the first attempt.
//! - **Graceful close drains the queue** (ORA3-M4): the only sender is the sink itself, so dropping
//!   the last [`GpustackSink`] closes the channel; the flusher keeps flushing the rows still queued
//!   (each through the normal bounded retry path) before exiting. If the backend is down at
//!   shutdown, the existing per-row `POST_TIMEOUT`/`MAX_ATTEMPTS` budget bounds the drain.
//!
//! No mock in impl: the flusher performs a real `reqwest` `POST`. Test doubles (a real local HTTP
//! server) are confined to `tests/`.
//!
//! # Scope gating (caller's responsibility)
//!
//! The pin scopes the token-usage report to **model-route traffic only** (mirror / GPUStack-self
//! traffic must NOT be reported, else it double-counts against the server's `record_model_usage`).
//! This crate therefore performs **no** scope gating — the caller (the gateway data plane) must only
//! call [`GpustackSink::push`] for model-route requests. See [`GpustackSink::push`].

use std::sync::Arc;
use std::time::Duration;

use http::header;
use hygress_core::usage::ModelUsageMetrics;
use tokio::sync::mpsc;

use crate::forward_auth::GATEWAY_AUTH_TOKEN_HEADER;
use crate::Result;

/// In-memory buffer capacity (fire-and-forget). When full, new metrics are dropped (with a log)
/// rather than blocking the caller.
const BUFFER_SIZE: usize = 1024;
/// Total POST attempts per metric (initial try + bounded retries). The flusher never spins forever.
const MAX_ATTEMPTS: u32 = 3;
/// Backoff before attempts 2 and 3 (kept tiny so a slow/failed endpoint does not stall the queue).
const BACKOFFS: [Duration; 2] = [Duration::from_millis(50), Duration::from_millis(100)];
/// Per-POST overall timeout (R-8): the flusher must never block forever on an
/// endpoint that accepts but never answers (the shared client only has a
/// connect timeout).
const POST_TIMEOUT: Duration = Duration::from_secs(30);

/// Out-of-band sink for `ModelUsageMetrics` → `POST {endpoint}`.
#[derive(Clone)]
pub struct GpustackSink {
    /// Full URL to `POST` to (e.g. `http://127.0.0.1:8080/v2/usage/gateway-metrics`).
    endpoint: String,
    /// The derived `X-GPUStack-Auth-Token` value.
    token: String,
    /// Enqueue handle into the flusher's queue (the flusher itself holds the `reqwest` client).
    tx: mpsc::Sender<Vec<u8>>,
    /// Invoked once per **dropped** usage row (bounded queue full / flusher gone / final push
    /// failure) so the caller can count the loss (ORA3-M4); `None` keeps drops log-only.
    on_drop: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl std::fmt::Debug for GpustackSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual impl: the `dyn Fn` drop hook is not `Debug` (and should not end up in logs anyway).
        // G4/O11: the derived credential is NEVER printed — a `{:?}` of the sink in logs must not
        // leak the `X-GPUStack-Auth-Token` value.
        f.debug_struct("GpustackSink")
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .field("on_drop", &self.on_drop.as_ref().map(|_| "<closure>"))
            .finish()
    }
}

impl GpustackSink {
    /// Create a sink that POSTs `ModelUsageMetrics` to `endpoint` with `X-GPUStack-Auth-Token` =
    /// `token`. **Spawns one background flusher** (so this must be called within a tokio runtime;
    /// the gateway and the tests always are).
    ///
    /// `token` must be the **derived** gateway token — see
    /// [`crate::token::derive_gateway_token`] over the `jwt_secret_key` resolved by
    /// [`crate::token::resolve_jwt_key`]. Using a raw/absent key here would make every report 401.
    ///
    /// `on_drop` (ORA3-M4): invoked once per **dropped** usage row — the bounded queue was full, the
    /// flusher task was gone, or a payload failed after its retry budget. `None` keeps the historical
    /// log-only drops. The caller (gateway bootstrap) wires this to its
    /// `hygress_usage_push_dropped_total` counter.
    pub fn new(
        endpoint: &str,
        http: reqwest::Client,
        token: String,
        on_drop: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Self {
        let endpoint = endpoint.to_string();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(BUFFER_SIZE);

        // One flusher per sink. Captures its own copies of the client/endpoint/token + the receiver
        // and a clone of the drop hook (for the final-push-failure drop site inside the task).
        let flusher_http = http.clone();
        let flusher_endpoint = endpoint.clone();
        let flusher_token = token.clone();
        let flusher_on_drop = on_drop.clone();
        tokio::spawn(async move {
            // Steady state + graceful-close drain in one loop. `rx.recv()` returns the next queued
            // payload and only yields `None` once the channel is closed (the last sender — the sink
            // itself — was dropped) AND its buffer is empty, so every row accepted before the close
            // is still flushed here with the normal bounded retry path (POST_TIMEOUT/MAX_ATTEMPTS).
            // Best-effort by construction: if the backend is down at shutdown, each remaining row
            // exhausts its retry budget (invoking `on_drop`) and the task returns.
            while let Some(payload) = rx.recv().await {
                Self::post_with_retry(
                    &flusher_http,
                    &flusher_endpoint,
                    &flusher_token,
                    &payload,
                    &flusher_on_drop,
                )
                .await;
            }
            tracing::debug!("usage flusher: channel closed and queue drained; flusher exiting");
        });

        Self {
            endpoint,
            token,
            tx,
            on_drop,
        }
    }

    /// The endpoint this sink POSTs to (for logging/verification by the caller).
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The derived `X-GPUStack-Auth-Token` value this sink authenticates with.
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Enqueue one usage metric for fire-and-forget delivery (see module docs).
    ///
    /// Returns `Ok(())` immediately without blocking (fire-and-forget contract). A full buffer or a
    /// gone flusher drops the metric (logged; the [`GpustackSink::new`] `on_drop` hook fires) rather
    /// than returning an error.
    ///
    /// # Scope
    /// Only call this for **model-route** traffic — the pin restricts the report to model routes.
    /// The caller is responsible for gating (this crate does not).
    pub async fn push(&self, m: &ModelUsageMetrics) -> Result<()> {
        // Serialize once to the exact 17-field wire form (the core type's serde is the source of
        // truth; `ModelUsageMetrics` is fixed-shape so this cannot fail in practice).
        let payload = serde_json::to_vec(m)?;
        match self.tx.try_send(payload) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("usage push queue full; dropping metric (fire-and-forget)");
                Self::notify_drop(&self.on_drop);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("usage flusher gone; dropping metric");
                Self::notify_drop(&self.on_drop);
                Ok(())
            }
        }
    }

    /// Fire the [`GpustackSink::new`] `on_drop` hook once (no-op when `None`).
    fn notify_drop(on_drop: &Option<Arc<dyn Fn() + Send + Sync>>) {
        if let Some(f) = on_drop {
            f();
        }
    }

    /// POST one serialized payload with bounded retry, then drop (log + `on_drop`) on final failure.
    ///
    /// Retry policy (MINOR-4): only **transient** failures are retried — transport errors
    /// (connect refused / DNS / timeout), `429 Too Many Requests` and 5xx. Deterministic 4xx
    /// client errors (401 bad token, 400, 404, …) are **not** retried: retrying cannot turn them
    /// into a success, so the metric is dropped after the first attempt.
    ///
    /// `on_drop` fires exactly once when the metric is given up (either a non-retryable failure or
    /// the retry budget exhausted) — every drop site is observable, not log-only (ORA3-M4).
    async fn post_with_retry(
        http: &reqwest::Client,
        endpoint: &str,
        token: &str,
        payload: &[u8],
        on_drop: &Option<Arc<dyn Fn() + Send + Sync>>,
    ) {
        for attempt in 1..=MAX_ATTEMPTS {
            // Backoff before every retry (not the first try).
            if attempt > 1 {
                tokio::time::sleep(BACKOFFS[(attempt - 2) as usize]).await;
            }
            match Self::post_once(http, endpoint, token, payload).await {
                Ok(()) => return,
                Err(e) => {
                    if !Self::retryable(&e) {
                        tracing::warn!(
                            "usage push to {endpoint} failed with non-retryable {e} on attempt {attempt}; dropping metric"
                        );
                        Self::notify_drop(on_drop);
                        return;
                    }
                    tracing::warn!(
                        "usage push to {endpoint} attempt {attempt}/{MAX_ATTEMPTS} failed: {e}"
                    );
                }
            }
        }
        tracing::warn!(
            "usage push to {endpoint} failed after {MAX_ATTEMPTS} attempts; dropping metric"
        );
        Self::notify_drop(on_drop);
    }

    /// Whether a failed POST is worth retrying. Transport errors (connect/refused/DNS/timeout) and
    /// the transient HTTP statuses `429` + 5xx are; every other status (all 4xx — e.g. a 401 from a
    /// wrong/missing token, 400, 404) is deterministic and must fail fast (MINOR-4).
    fn retryable(e: &PostError) -> bool {
        match e {
            PostError::Transport(_) => true,
            PostError::Status(s) => {
                *s == http::StatusCode::TOO_MANY_REQUESTS || s.is_server_error()
            }
        }
    }

    /// A single real `POST {endpoint}` with the auth token and a JSON body.
    ///
    /// Returns `Ok(())` on a 2xx response; `Err` on a transport error or a non-2xx status (which
    /// the caller classifies via [`Self::retryable`]).
    async fn post_once(
        http: &reqwest::Client,
        endpoint: &str,
        token: &str,
        payload: &[u8],
    ) -> std::result::Result<(), PostError> {
        // The token is a hex token (from `derive_gateway_token`) — always a valid header value. If
        // (unexpectedly) it is not, log and send without it rather than panicking.
        let mut request = http
            .post(endpoint)
            .header(header::CONTENT_TYPE, "application/json")
            .timeout(POST_TIMEOUT) // R-8: bounded per-POST (see module const).
            .body(payload.to_vec());
        match http::HeaderValue::from_str(token) {
            Ok(t) => request = request.header(GATEWAY_AUTH_TOKEN_HEADER, t),
            Err(_) => {
                tracing::warn!("invalid X-GPUStack-Auth-Token header value; sending without it")
            }
        }

        let resp = request.send().await.map_err(PostError::Transport)?;
        let status = resp.status();
        if status.is_success() {
            // Drain the (small) response body so the connection is released cleanly.
            let _ = resp.bytes().await;
            Ok(())
        } else {
            // Surface the status as an error so the caller can classify it (retry 429/5xx only).
            Err(PostError::Status(status))
        }
    }
}

/// A failure to deliver one usage payload: a transport error or a non-2xx HTTP status.
#[derive(Debug)]
enum PostError {
    /// A `reqwest` transport/build error (connect refused, DNS, timeout, …).
    Transport(reqwest::Error),
    /// The endpoint responded with a non-2xx status.
    Status(http::StatusCode),
}

impl std::fmt::Display for PostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PostError::Transport(e) => write!(f, "transport error: {e}"),
            PostError::Status(status) => write!(f, "HTTP {status} from usage endpoint"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::usage::{ModelUsageMetrics, Operation};

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

    /// The 17-field wire form the sink must send is exactly the core type's serde output.
    #[test]
    fn payload_is_the_pinned_17_fields() {
        let json = serde_json::to_value(sample_metric()).unwrap();
        let keys: std::collections::BTreeSet<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(|s| s.as_str())
            .collect();
        let expected: std::collections::BTreeSet<&str> = [
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
        ]
        .into_iter()
        .collect();
        assert_eq!(keys, expected);
        assert_eq!(json["completed"], true, "completed flag must be present");
    }

    /// `push` is fire-and-forget: it never blocks and returns `Ok` even with the sink's flusher
    /// consuming — and the `Operation` vocabulary (server-side, not on the wire) does not leak in.
    #[tokio::test]
    async fn push_returns_ok_and_is_nonblocking() {
        // We cannot spin up a server in a unit test without the integration harness; here we only
        // verify the enqueue path returns Ok and does not hang. The real POST is covered in tests/.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let sink = GpustackSink::new(
            "http://127.0.0.1:1/v2/usage/gateway-metrics",
            http,
            "tok".into(),
            None,
        );
        // A short runtime sleep is enough for the (failing) flusher to try once without hanging.
        let res = tokio::time::timeout(Duration::from_secs(2), sink.push(&sample_metric())).await;
        assert!(res.is_ok(), "push must not block");
    }

    /// Guard: the four server-side-only fields are NOT serialized into the payload.
    #[test]
    fn server_only_fields_are_absent() {
        let json = serde_json::to_value(sample_metric()).unwrap();
        for forbidden in ["operation", "cluster_id", "provider_name", "provider_type"] {
            assert!(
                !json.as_object().unwrap().contains_key(forbidden),
                "{forbidden} must not be on the wire"
            );
        }
        // And `operation` is a real core value we might be tempted to leak — prove absence.
        let _ = Operation::ChatCompletion.as_str();
    }

    // ----- MINOR-4: retry policy (real local HTTP server; no mocks) -----
    //
    // `post_with_retry` must only retry TRANSIENT failures: transport errors, `429` and 5xx.
    // Deterministic 4xx client errors (401 bad token, 400/404, ...) must fail fast after the
    // first attempt — retrying them cannot succeed and only adds useless load/backoff.

    /// Minimal real HTTP/1.1 server: answers every request with one fixed status and counts the
    /// requests it receives (a test double, allowed in `#[cfg(test)]` per the crate lib docs).
    struct StubStatusServer {
        addr: std::net::SocketAddr,
        count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        stop: std::sync::Arc<tokio::sync::Notify>,
    }

    impl StubStatusServer {
        async fn spawn(status: u16) -> Self {
            use tokio::net::TcpListener;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let stop = std::sync::Arc::new(tokio::sync::Notify::new());
            let (accept_count, accept_stop) = (count.clone(), stop.clone());
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = accept_stop.notified() => break,
                        res = listener.accept() => {
                            let (sock, _) = match res { Ok(x) => x, Err(_) => continue };
                            let (c, s) = (accept_count.clone(), accept_stop.clone());
                            tokio::spawn(async move { serve_one(sock, status, c, s).await });
                        }
                    }
                }
            });
            Self { addr, count, stop }
        }

        fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        fn count(&self) -> usize {
            self.count.load(std::sync::atomic::Ordering::SeqCst)
        }

        async fn wait_until(&self, n: usize) {
            let deadline = std::time::Instant::now() + Duration::from_secs(8);
            while self.count() < n {
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for {n} request(s), got {}",
                    self.count()
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        fn shutdown(&self) {
            self.stop.notify_waiters();
        }
    }

    impl Drop for StubStatusServer {
        fn drop(&mut self) {
            self.shutdown();
        }
    }

    async fn serve_one(
        mut sock: tokio::net::TcpStream,
        status: u16,
        count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        stop: std::sync::Arc<tokio::sync::Notify>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Read the request head, then the body (Content-Length), then respond — one request per
        // connection (`Connection: close`). Reading the full body keeps the client's write from
        // being RST-dropped mid-flight.
        let mut buf: Vec<u8> = Vec::new();
        let mut tmp = [0u8; 2048];
        loop {
            match sock.read(&mut tmp).await {
                Ok(0) => return,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(_) => return,
            }
            if find_bytes(&buf, b"\r\n\r\n").is_some() {
                break;
            }
            if buf.len() > 1 << 20 {
                return;
            }
        }
        let head_end = find_bytes(&buf, b"\r\n\r\n").unwrap() + 4;
        let head = String::from_utf8_lossy(&buf[..head_end]);
        let content_length = head
            .lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                (k.trim().eq_ignore_ascii_case("content-length"))
                    .then(|| v.trim().parse::<usize>().unwrap_or(0))
            })
            .unwrap_or(0);
        while buf.len() < head_end + content_length {
            match sock.read(&mut tmp).await {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(_) => break,
            }
        }
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let reason = match status {
            401 => "Unauthorized",
            404 => "Not Found",
            429 => "Too Many Requests",
            503 => "Service Unavailable",
            _ => "Status",
        };
        let resp =
            format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let _ = sock.write_all(resp.as_bytes()).await;
        let _ = sock.flush().await;
        let _ = sock.shutdown().await;
        let _ = stop;
    }

    fn find_bytes(hay: &[u8], needle: &[u8]) -> Option<usize> {
        if needle.is_empty() || hay.len() < needle.len() {
            return None;
        }
        let last = hay.len() - needle.len();
        (0..=last).find(|&i| &hay[i..i + needle.len()] == needle)
    }

    /// Endpoint of a stub server with the real sink URL path.
    fn endpoint(server: &StubStatusServer) -> String {
        format!("{}/v2/usage/gateway-metrics", server.base_url())
    }

    #[tokio::test]
    async fn http_401_is_not_retried_drops_after_one_attempt() {
        // A 401 (bad/missing token) is deterministic: retrying cannot turn it into a success,
        // so the metric must be dropped after exactly ONE attempt (no backoff, no retries).
        let server = StubStatusServer::spawn(401).await;
        GpustackSink::post_with_retry(
            &reqwest::Client::new(),
            &endpoint(&server),
            "tok",
            b"{}",
            &None,
        )
        .await;
        server.wait_until(1).await;
        // Covers the 50ms + 100ms backoffs attempts 2/3 would have taken.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            server.count(),
            1,
            "a 401 must not be retried (deterministic client error)"
        );
    }

    #[tokio::test]
    async fn http_404_is_not_retried_drops_after_one_attempt() {
        // Same rule for any other 4xx: a wrong endpoint (404) will never start succeeding.
        let server = StubStatusServer::spawn(404).await;
        GpustackSink::post_with_retry(
            &reqwest::Client::new(),
            &endpoint(&server),
            "tok",
            b"{}",
            &None,
        )
        .await;
        server.wait_until(1).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(server.count(), 1, "a 404 must not be retried");
    }

    #[tokio::test]
    async fn http_503_is_retried_up_to_max_attempts_then_dropped() {
        // A transient server error IS retried: all 3 bounded attempts are made, then the metric is
        // dropped (never spins).
        let server = StubStatusServer::spawn(503).await;
        GpustackSink::post_with_retry(
            &reqwest::Client::new(),
            &endpoint(&server),
            "tok",
            b"{}",
            &None,
        )
        .await;
        server.wait_until(3).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            server.count(),
            3,
            "a 503 is transient: retried the full MAX_ATTEMPTS (3) then dropped"
        );
    }

    #[tokio::test]
    async fn http_429_is_retried_then_dropped_after_max_attempts() {
        // 429 (rate-limited) is transient: it must be retried (all 3 attempts) like a 5xx.
        let server = StubStatusServer::spawn(429).await;
        GpustackSink::post_with_retry(
            &reqwest::Client::new(),
            &endpoint(&server),
            "tok",
            b"{}",
            &None,
        )
        .await;
        server.wait_until(3).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            server.count(),
            3,
            "a 429 must be retried (transient rate limit), up to MAX_ATTEMPTS"
        );
    }

    // ----- ORA3-M4: drops are observable (on_drop), not log-only -----

    /// A counting `on_drop` closure (increments the shared counter each time a row is dropped).
    fn counting_on_drop(
        counter: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> Option<Arc<dyn Fn() + Send + Sync>> {
        let counter = counter.clone();
        Some(Arc::new(move || {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }))
    }

    /// A TCP listener that accepts connections and never answers: the flusher's in-flight POST
    /// stays pending (bounded by `POST_TIMEOUT`), which lets the bounded queue actually fill.
    async fn spawn_tarpit() -> std::net::SocketAddr {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    continue;
                };
                tokio::spawn(async move {
                    // Read and discard whatever the client sends; never write a response.
                    let mut buf = [0u8; 2048];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn queue_full_push_drop_invokes_on_drop() {
        // A full bounded (1024) queue drops the metric — `on_drop` must fire so the gateway can
        // count the loss instead of it being log-only.
        let addr = spawn_tarpit().await; // flusher's first POST hangs → the queue really fills
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sink = GpustackSink::new(
            &format!("http://{addr}/v2/usage/gateway-metrics"),
            reqwest::Client::new(),
            "tok".into(),
            counting_on_drop(&dropped),
        );
        // Burst beyond capacity: pushes are immediate `try_send`s, so every overflow row beyond
        // the buffer (queue full) is dropped and must fire `on_drop` once.
        for _ in 0..(BUFFER_SIZE + 20) {
            sink.push(&sample_metric()).await.unwrap();
        }
        assert!(
            dropped.load(std::sync::atomic::Ordering::SeqCst) >= 20,
            "every queue-full push drop must invoke on_drop (got {} fires)",
            dropped.load(std::sync::atomic::Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn final_push_failure_invokes_on_drop_once() {
        // After the bounded retry budget is exhausted the metric is dropped inside the flusher —
        // `on_drop` must fire exactly once for that row.
        let server = StubStatusServer::spawn(503).await;
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        GpustackSink::post_with_retry(
            &reqwest::Client::new(),
            &endpoint(&server),
            "tok",
            b"{}",
            &counting_on_drop(&dropped),
        )
        .await;
        server.wait_until(3).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the exhausted-retries drop must invoke on_drop exactly once"
        );
    }

    #[tokio::test]
    async fn non_retryable_push_failure_invokes_on_drop_once() {
        // A deterministic 4xx is dropped after the first attempt — that drop is observable too.
        let server = StubStatusServer::spawn(401).await;
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        GpustackSink::post_with_retry(
            &reqwest::Client::new(),
            &endpoint(&server),
            "tok",
            b"{}",
            &counting_on_drop(&dropped),
        )
        .await;
        server.wait_until(1).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            dropped.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the non-retryable drop must invoke on_drop exactly once"
        );
    }

    #[tokio::test]
    async fn dropping_last_sink_handle_drains_queued_rows_to_backend() {
        // ORA3-M4 graceful-close drain: rows accepted into the bounded queue are NOT abandoned
        // when the last sender (the sink itself) is dropped. `rx.recv()` only returns `None` once
        // the channel is closed AND its buffer is empty, so the flusher keeps flushing every
        // queued row (normal retry path) after the close before exiting. Pin that property: on the
        // (current-thread) test runtime the flusher is not polled during the burst below — pushes
        // are immediate `try_send`s — so the rows are genuinely still queued when the sink drops.
        let server = StubStatusServer::spawn(200).await;
        let sink = GpustackSink::new(
            &endpoint(&server),
            reqwest::Client::new(),
            "tok".into(),
            None,
        );
        const ROWS: usize = 24;
        for _ in 0..ROWS {
            sink.push(&sample_metric())
                .await
                .expect("push must not fail");
        }
        drop(sink); // last sender gone → channel closes → flusher must drain the queue.
        server.wait_until(ROWS).await;
        assert_eq!(
            server.count(),
            ROWS,
            "rows queued at close must be flushed to the backend, not dropped"
        );
        server.shutdown();
    }
}
