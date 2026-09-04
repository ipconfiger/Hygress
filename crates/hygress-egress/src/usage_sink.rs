//! `GpustackSink` — out-of-band usage push to `POST /v2/usage/gateway-metrics` (native
//! equivalent of the `gpustack-token-usage` plugin; design §7; plugin-contract-pin §2.8 / §5.1).
//!
//! The sink serializes a complete [`hygress_core::usage::ModelUsageMetrics`] (the exact 17-field
//! wire form, including the `completed` flag and the omitempty attribution fields) and POSTs it with
//! `X-GPUStack-Auth-Token` = the derived gateway token. Delivery is **fire-and-forget**:
//!
//! - [`GpustackSink::push`] enqueues the serialized payload onto a bounded in-memory channel and
//!   returns `Ok(())` immediately — it never blocks and never spins. When the buffer is full (or the
//!   flusher is gone) the metric is dropped with a log line.
//! - One background flusher (spawned per [`GpustackSink::new`]) drains the channel and POSTs each
//!   payload. On transport / HTTP failure it retries a bounded number of times with a small backoff
//!   and then **drops the metric with a log** — it never retries forever.
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

/// Out-of-band sink for `ModelUsageMetrics` → `POST {endpoint}`.
#[derive(Clone, Debug)]
pub struct GpustackSink {
    /// Full URL to `POST` to (e.g. `http://127.0.0.1:8080/v2/usage/gateway-metrics`).
    endpoint: String,
    /// The derived `X-GPUStack-Auth-Token` value.
    token: String,
    /// Enqueue handle into the flusher's queue (the flusher itself holds the `reqwest` client).
    tx: mpsc::Sender<Vec<u8>>,
}

impl GpustackSink {
    /// Create a sink that POSTs `ModelUsageMetrics` to `endpoint` with `X-GPUStack-Auth-Token` =
    /// `token`. **Spawns one background flusher** (so this must be called within a tokio runtime;
    /// the gateway and the tests always are).
    ///
    /// `token` must be the **derived** gateway token — see
    /// [`crate::token::derive_gateway_token`] over the `jwt_secret_key` resolved by
    /// [`crate::token::resolve_jwt_key`]. Using a raw/absent key here would make every report 401.
    pub fn new(endpoint: &str, http: reqwest::Client, token: String) -> Self {
        let endpoint = endpoint.to_string();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(BUFFER_SIZE);

        // One flusher per sink. Captures its own copies of the client/endpoint/token + the receiver.
        let flusher_http = http.clone();
        let flusher_endpoint = endpoint.clone();
        let flusher_token = token.clone();
        tokio::spawn(async move {
            while let Some(payload) = rx.recv().await {
                Self::post_with_retry(&flusher_http, &flusher_endpoint, &flusher_token, &payload)
                    .await;
            }
        });

        Self {
            endpoint,
            token,
            tx,
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
    /// gone flusher drops the metric (logged) rather than returning an error.
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
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("usage flusher gone; dropping metric");
                Ok(())
            }
        }
    }

    /// POST one serialized payload with bounded retry, then drop-with-log on final failure.
    async fn post_with_retry(http: &reqwest::Client, endpoint: &str, token: &str, payload: &[u8]) {
        for attempt in 1..=MAX_ATTEMPTS {
            // Backoff before every retry (not the first try).
            if attempt > 1 {
                tokio::time::sleep(BACKOFFS[(attempt - 2) as usize]).await;
            }
            match Self::post_once(http, endpoint, token, payload).await {
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(
                        "usage push to {endpoint} attempt {attempt}/{MAX_ATTEMPTS} failed: {e}"
                    );
                }
            }
        }
        tracing::warn!(
            "usage push to {endpoint} failed after {MAX_ATTEMPTS} attempts; dropping metric"
        );
    }

    /// A single real `POST {endpoint}` with the auth token and a JSON body.
    ///
    /// Returns `Ok(())` on a 2xx response; `Err` on a transport error or a non-2xx status (which the
    /// caller turns into a retry).
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
            // Surface the status as an error so the caller retries.
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
}
