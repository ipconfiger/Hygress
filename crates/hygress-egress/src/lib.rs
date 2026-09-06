//! hygress-egress — out-of-band HTTP clients to GPUStack (design §7 plugins, §2.1.3 bypass contract).
//!
//! This crate is the **out-of-band** counterpart to the data plane: four clients that talk to
//! upstream services (the GPUStack server and the LLM guardrail service) from outside the proxy's
//! request cycle (they never proxy traffic themselves):
//!
//! - [`token`] — derive the gateway auth token (HMAC-SHA256 of `jwt_secret_key` over
//!   `gateway-metrics-push`) and resolve `jwt_secret_key` (env → `{data_dir}/jwt_secret_key` file →
//!   fail-fast). No mock in impl: real crypto; the key is never invented.
//! - [`forward_auth`] — [`forward_auth::Client`]: **GET** `/token-auth`; forwards ONLY the pin §5.3
//!   allowlist (`X-Real-IP`/`X-Forwarded-For`/`x-higress-llm-model`/`x-api-key`/`cookie`/
//!   `x-gpustack-auth-cache`), injects `X-GPUStack-Auth-Token`; reads back `X-Mse-Consumer`/
//!   `Authorization`/`cookie`/`AUTH_CACHE_HEADER`; 30s timeout; transport errors/5xx →
//!   `Ok(None)` ("auth service unavailable" — the gateway decides reject-vs-fail-open per its
//!   `HYGRESS_EXT_AUTH_FAIL_MODE`, R-12). No mock in impl: transport failures are genuine
//!   connect/read errors; 4xx+ = real result.
//! - [`usage_sink`] — [`usage_sink::GpustackSink`]: `POST /v2/usage/gateway-metrics` with
//!   `X-GPUStack-Auth-Token` = the derived token; serializes the exact 17-field
//!   `ModelUsageMetrics` (incl. `completed`); scope = model-route traffic only (the caller's job).
//!   Fire-and-forget: `push` enqueues and returns `Ok`; a background flusher retries briefly then
//!   drops (never spins). No mock in impl: real HTTP POST.
//! - [`provider`] — [`provider::ProviderClient`]: builds the outbound upstream request (path rewrite
//!   `rewrite-target`/`$1$3`, `Authorization: Bearer` key swap, `Host` override, per-destination
//!   model-mapping application for JSON/multipart bodies, forward-safe header copy). Pure enough to
//!   unit test: no I/O of its own (the gateway dials the upstream).
//! - [`guardrail`] — [`guardrail::GuardrailClient`]: the LLM guardrail verdict client (design
//!   §4.4 B4b). `POST {base_url}` with `{"text": …}`; parses a lenient [`guardrail::GuardVerdict`]
//!   (`{blocked, reason}`). New vs `forward_auth`: a `Semaphore` bounds concurrency and a `DashMap`
//!   caches verdicts by (whitespace-normalized) text for a TTL. **This client only reports**: a 2xx
//!   → `Ok(Some(verdict))`; a 4xx/5xx or transport failure → `Err`. It does NOT fail-open/closed —
//!   that decision is the gateway's `fail_mode` (D-14; not-configured = pass-through).
//!
//! Tests may spin up **real** local HTTP servers (hand-rolled `tokio` `TcpListener` in `tests/` or
//! `#[cfg(test)]`) — test doubles are allowed in test code ONLY, never in implementation code.
//!
//! # Cross-crate contract
//!
//! The public API of the four modules is the frozen inter-crate contract the `hygress-gateway`
//! data-plane lane consumes. Do not rename the pinned types/functions:
//! `token::{derive_gateway_token, resolve_jwt_key}`,
//! `forward_auth::{Client, ForwardAuthRequest, ForwardAuthVerdict}`,
//! `usage_sink::GpustackSink`, `provider::ProviderClient`,
//! `guardrail::{GuardrailClient, GuardVerdict}`.

#![warn(missing_docs)]

pub mod forward_auth;
pub mod guardrail;
pub mod provider;
pub mod token;
pub mod usage_sink;


/// Test-only helper to install the ring crypto provider once (M4: this build
/// compiles reqwest with `rustls-no-provider`; the gateway binary installs the
/// provider in `main`, unit tests install it here before building clients).
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::Once;

    pub(crate) fn install_ring_provider() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }
}

use thiserror::Error as ThisError;

/// Crate error for the fallible out-of-band operations (key resolution, request building).
///
/// Transport/HTTP failures that the contract treats as **fail-open** (forward-auth) or
/// **drop-with-log** (usage push) are NOT surfaced here — they are absorbed by the client and
/// reported via tracing. `Error` is otherwise returned for genuinely unrecoverable/programming
/// conditions (missing/empty key, malformed base URL) **and** for a failed guardrail verdict call
/// ([`Error::GuardrailCall`]): there the failure is *reported* (not absorbed) so the gateway can
/// apply its `fail_mode` (D-14).
#[derive(Debug, ThisError)]
pub enum Error {
    /// The `jwt_secret_key` has no source (neither env nor the `{data_dir}/jwt_secret_key` file).
    /// Per design §9 the gateway must fail-fast at startup rather than silently degrade.
    #[error("jwt_secret_key not found (set env GPUSTACK_JWT_SECRET_KEY or create {data_dir}/jwt_secret_key)")]
    JwtKeyNotFound {
        /// The `data_dir` probed (and therefore the file path that was missing).
        data_dir: String,
    },
    /// A key source was present but resolved to an empty value (an empty secret is meaningless).
    #[error("jwt_secret_key is empty")]
    JwtKeyEmpty,
    /// A base URL could not be parsed (programming/configuration error).
    #[error("invalid base URL: {0}")]
    InvalidUrl(String),
    /// Serializing the usage payload failed (cannot happen for the fixed `ModelUsageMetrics`
    /// shape; surfaced for completeness).
    #[error("serializing usage payload: {0}")]
    Serialize(#[from] serde_json::Error),
    /// The guardrail LLM verdict call failed — a transport error (connect/DNS/timeout) or a
    /// non-2xx HTTP status (or a malformed 2xx body).
    ///
    /// Per design §4.4 B4b / D-14, **how the caller reacts** (fail-open vs fail-closed) is the
    /// gateway's job (its `fail_mode`): this crate only reports that the call failed and does NOT
    /// imply either direction (it does not fail-open like forward-auth, nor fail-closed itself).
    /// The message carries the cause for logging/diagnostics.
    #[error("guardrail verdict call failed: {0}")]
    GuardrailCall(String),
}

/// Crate result alias (keeps the pinned `Result<T, _>` signatures short and unambiguous).
pub type Result<T> = std::result::Result<T, Error>;
