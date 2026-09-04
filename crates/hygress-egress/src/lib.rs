//! hygress-egress — out-of-band HTTP clients to GPUStack (design §7 plugins, §2.1.3 bypass contract).
//!
//! This crate is the **out-of-band** counterpart to the data plane: three clients that talk to the
//! GPUStack server from outside the proxy's request cycle (they never proxy traffic themselves):
//!
//! - [`token`] — derive the gateway auth token (HMAC-SHA256 of `jwt_secret_key` over
//!   `gateway-metrics-push`) and resolve `jwt_secret_key` (env → `{data_dir}/jwt_secret_key` file →
//!   fail-fast). No mock in impl: real crypto; the key is never invented.
//! - [`forward_auth`] — [`forward_auth::Client`]: **GET** `/token-auth`; forwards ONLY the pin §5.3
//!   allowlist (`X-Real-IP`/`X-Forwarded-For`/`x-higress-llm-model`/`x-api-key`/`cookie`/
//!   `x-gpustack-auth-cache`), injects `X-GPUStack-Auth-Token`; reads back `X-Mse-Consumer`/
//!   `Authorization`/`cookie`/`AUTH_CACHE_HEADER`; 30s timeout; FAIL_OPEN on transport errors/5xx
//!   (returns `None`). No mock in impl: transport failures = fail-open, 4xx+ = real result.
//! - [`usage_sink`] — [`usage_sink::GpustackSink`]: `POST /v2/usage/gateway-metrics` with
//!   `X-GPUStack-Auth-Token` = the derived token; serializes the exact 17-field
//!   `ModelUsageMetrics` (incl. `completed`); scope = model-route traffic only (the caller's job).
//!   Fire-and-forget: `push` enqueues and returns `Ok`; a background flusher retries briefly then
//!   drops (never spins). No mock in impl: real HTTP POST.
//! - [`provider`] — [`provider::ProviderClient`]: builds the outbound upstream request (path rewrite
//!   `rewrite-target`/`$1$3`, `Authorization: Bearer` key swap, `Host` override, per-destination
//!   model-mapping application for JSON/multipart bodies, forward-safe header copy). Pure enough to
//!   unit test: no I/O of its own (the gateway dials the upstream).
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
//! `usage_sink::GpustackSink`, `provider::ProviderClient`.

pub mod forward_auth;
pub mod provider;
pub mod token;
pub mod usage_sink;

use thiserror::Error as ThisError;

/// Crate error for the fallible out-of-band operations (key resolution, request building).
///
/// Transport/HTTP failures that the contract treats as **fail-open** (forward-auth) or
/// **drop-with-log** (usage push) are NOT surfaced here — they are absorbed by the client and
/// reported via tracing. `Error` is returned only for genuinely unrecoverable/programming
/// conditions (missing/empty key, malformed base URL).
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
}

/// Crate result alias (keeps the pinned `Result<T, _>` signatures short and unambiguous).
pub type Result<T> = std::result::Result<T, Error>;
