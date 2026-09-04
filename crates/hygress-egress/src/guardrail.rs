//! LLM guardrail verdict client (design §4.4 B4b) — the out-of-band "call the guardrail/audit
//! service and get a verdict" half of the safety-guardrail feature.
//!
//! This is the **egress-side owner** of the LLM verdict (the gateway data-plane lane builds the
//! client and applies the verdict + `fail_mode`). The skeleton mirrors [`crate::forward_auth`]
//! (real `reqwest` + per-request timeout + response parsing + transport-failure-as-`Err`), with two
//! additions called out by the design:
//!
//! - **Concurrency bound** — a `tokio::sync::Semaphore` (`max_concurrency`) wraps the request phase
//!   so a burst of prompts cannot fan out unbounded calls to the (slow) LLM service.
//! - **Verdict cache** — a `DashMap<String, (GuardVerdict, Instant)>` keyed by the
//!   whitespace-normalized text; a TTL-bounded hit returns the cached verdict without any request.
//!
//! # Error semantics (deliberately NOT fail-open / fail-closed)
//!
//! Per design §4.4 B4b / D-14, **how a failure is handled is the gateway's `fail_mode` decision**
//! (default `closed` = reject; only when the guardrail is *enabled and* the call fails; not-configured
//! = pass-through). This crate's job is strictly to **call the service and report the outcome**:
//!
//! | outcome                              | result                            |
//! |--------------------------------------|-----------------------------------|
//! | 2xx + parseable `{blocked, reason}`  | `Ok(Some(verdict))`               |
//! | 2xx + empty body (no verdict)        | `Ok(None)`                        |
//! | 3xx/4xx/5xx (any non-2xx)            | `Err(Error::GuardrailCall)`       |
//! | transport error (connect/DNS/timeout)| `Err(Error::GuardrailCall)`       |
//! | 2xx + malformed (non-empty) body     | `Err(Error::GuardrailCall)`       |
//!
//! It shares **no** code path with `forward_auth`'s FAIL_OPEN (design §4.4: "不回归 A7").
//!
//! No mock in impl: the client performs a real `reqwest` `POST`. Test doubles (a real local HTTP
//! server) are confined to `tests/`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use http::header;
use serde::Deserialize;
use tokio::sync::Semaphore;

use crate::{Error, Result};

/// A guardrail verdict, parsed **leniently** from the service's response body.
///
/// The canonical wire form is `{ "blocked": bool, "reason": string }`. Because the exact field
/// names are per-service configurable (design §4.4 B4b), parsing is lenient: both fields default
/// when absent, and a small set of common alternative names is accepted via `#[serde(alias)]`.
///
/// `Default` (`blocked=false`, `reason=""`) is the "not blocked" verdict — the safe shape when a
/// service returns an object without a verdict (e.g. `{}`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct GuardVerdict {
    /// `true` if the guardrail service blocked the text.
    #[serde(default, alias = "is_blocked", alias = "block")]
    pub blocked: bool,
    /// Human-readable reason from the service; empty when the service gives none.
    #[serde(default, alias = "message", alias = "verdict")]
    pub reason: String,
}

/// LLM guardrail verdict client (design §4.4 B4b).
///
/// `Clone`-cheap: the `reqwest` client and the `Semaphore`/`DashMap` are `Arc`-shared, so many
/// clones (e.g. one per request handler) share the same concurrency bound and cache.
#[derive(Clone, Debug)]
pub struct GuardrailClient {
    /// Guardrail-service base URL (scheme + authority, optional path), trailing slashes trimmed.
    /// This is the full URL `POST`ed to (the path, if any, is part of `base_url`).
    base_url: String,
    /// The `reqwest` client that performs the real HTTP call.
    http: reqwest::Client,
    /// Per-request timeout (applied to each `POST`, like `forward_auth`).
    timeout: Duration,
    /// Concurrency bound for the request phase (clamped to a minimum of 1 — a zero-permit
    /// semaphore would deadlock).
    semaphore: Arc<Semaphore>,
    /// Verdict cache: whitespace-normalized text → `(verdict, expiry)`.
    cache: Arc<DashMap<String, (GuardVerdict, Instant)>>,
    /// Time-to-live for a cached verdict.
    cache_ttl: Duration,
}

impl GuardrailClient {
    /// Build a client that `POST`s `{"text": …}` to `base_url`.
    ///
    /// `base_url` is the full endpoint (trailing slashes are trimmed so a path component joins
    /// cleanly). `max_concurrency` bounds how many verdict calls are in flight at once (clamped to
    /// ≥ 1); `cache_ttl` is how long a verdict is cached for the same (normalized) text.
    pub fn new(
        base_url: impl Into<String>,
        http: reqwest::Client,
        timeout: Duration,
        max_concurrency: usize,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            timeout,
            // A 0-permit semaphore would hang `acquire` forever; clamp to at least 1.
            semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
            cache: Arc::new(DashMap::new()),
            cache_ttl,
        }
    }

    /// The full URL this client `POST`s to (base URL with trailing slashes trimmed).
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Classify `text` and return the guardrail verdict (see the module docs for the full
    /// status→result mapping).
    ///
    /// A TTL-bounded cache hit returns the cached verdict **without any request**. Otherwise a real
    /// `POST` is issued under the concurrency bound, and a 2xx verdict is cached for `cache_ttl`.
    /// A 4xx/5xx or transport failure is returned as `Err` (the caller applies `fail_mode`).
    pub async fn classify(&self, text: &str) -> Result<Option<GuardVerdict>> {
        let key = normalize(text);

        // 1. Cache lookup: a live (unexpired) entry returns immediately — no request is made.
        if let Some(entry) = self.cache.get(&key) {
            let (verdict, expiry) = entry.value();
            if Instant::now() < *expiry {
                return Ok(Some(verdict.clone()));
            }
            // Expired: drop it and fall through to a fresh call.
            drop(entry);
            self.cache.remove(&key);
        }

        // 2. Acquire the concurrency permit for the request phase only (released when `permit`
        //    is dropped at the end of the block — i.e. once the POST completes).
        let result = {
            let _permit = self.semaphore.acquire().await;
            self.post(text).await
        };

        // 3. Cache only a real verdict (the cache stores `GuardVerdict`, not `Option`). A `None`
        //    (no verdict) or an `Err` is not cached.
        if let Ok(Some(ref verdict)) = result {
            self.cache.insert(key, (verdict.clone(), Instant::now() + self.cache_ttl));
        }

        result
    }

    /// One real `POST {base_url}` with `{"text": …}` (a JSON body + `Content-Type`).
    ///
    /// Returns `Ok(Some(verdict))` for a 2xx with a parseable body, `Ok(None)` for a 2xx with an
    /// empty body (no verdict), and `Err` for a non-2xx status, a transport error, or a malformed
    /// (non-empty) 2xx body.
    async fn post(&self, text: &str) -> Result<Option<GuardVerdict>> {
        let url = self.base_url.clone();
        let payload = serde_json::to_vec(&serde_json::json!({ "text": text }))
            .expect("serializing {text: <&str>} cannot fail");

        let response = self
            .http
            .post(&url)
            .header(header::CONTENT_TYPE, "application/json")
            .timeout(self.timeout)
            .body(payload)
            .send()
            .await
            .map_err(|e| {
                tracing::warn!("guardrail transport error to {url}: {e}");
                Error::GuardrailCall(format!("transport: {e}"))
            })?;

        let status = response.status();
        if !status.is_success() {
            // Drain the (small) body so the connection is released cleanly, then report the status.
            let _ = response.bytes().await;
            tracing::warn!("guardrail {status} from {url}");
            return Err(Error::GuardrailCall(format!("HTTP {status} from guardrail endpoint")));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| Error::GuardrailCall(format!("reading response body: {e}")))?;

        // A 2xx with an empty body carries no verdict (e.g. 204) → `Ok(None)`.
        if bytes.is_empty() {
            return Ok(None);
        }

        // Lenient parse of `{blocked, reason}`; a non-empty body that is not a valid verdict object
        // is a real failure (the gateway applies `fail_mode`).
        match serde_json::from_slice::<GuardVerdict>(&bytes) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                tracing::warn!("guardrail malformed 2xx body from {url}: {e}");
                Err(Error::GuardrailCall(format!("malformed verdict body: {e}")))
            }
        }
    }
}

/// Normalize the cache key: collapse every whitespace run (spaces/tabs/newlines) to a single space
/// and trim the ends, so `  hello   world\nfoo ` keys the same as `hello world foo`.
fn normalize(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- base URL joining -----

    #[test]
    fn base_url_trims_trailing_slash() {
        let c = GuardrailClient::new(
            "http://127.0.0.1:8080",
            reqwest::Client::new(),
            Duration::from_secs(1),
            4,
            Duration::from_secs(5),
        );
        assert_eq!(c.base_url(), "http://127.0.0.1:8080");

        let c = GuardrailClient::new(
            "http://127.0.0.1:8080///",
            reqwest::Client::new(),
            Duration::from_secs(1),
            4,
            Duration::from_secs(5),
        );
        assert_eq!(c.base_url(), "http://127.0.0.1:8080");

        // A path component is preserved (only trailing slashes trimmed).
        let c = GuardrailClient::new(
            "http://127.0.0.1:8080/v1/classify/",
            reqwest::Client::new(),
            Duration::from_secs(1),
            4,
            Duration::from_secs(5),
        );
        assert_eq!(c.base_url(), "http://127.0.0.1:8080/v1/classify");
    }

    // ----- cache-key normalization -----

    #[test]
    fn normalize_collapses_and_trims_whitespace() {
        assert_eq!(normalize("  hello   world "), "hello world");
        assert_eq!(normalize("a\t\tb\n\nc  d"), "a b c d");
        assert_eq!(normalize("   "), "");
        assert_eq!(normalize("single"), "single");
        assert_eq!(normalize(""), "");
    }

    // ----- GuardVerdict lenient serde -----

    #[test]
    fn verdict_parses_canonical_shape() {
        let v: GuardVerdict =
            serde_json::from_str(r#"{"blocked": true, "reason": "injection detected"}"#).unwrap();
        assert!(v.blocked);
        assert_eq!(v.reason, "injection detected");
    }

    #[test]
    fn verdict_defaults_missing_fields() {
        // `{}` → the "not blocked" default verdict (both fields default).
        let v: GuardVerdict = serde_json::from_str(r#"{}"#).unwrap();
        assert!(!v.blocked);
        assert_eq!(v.reason, "");

        // Partial: only `blocked` present.
        let v: GuardVerdict = serde_json::from_str(r#"{"blocked": true}"#).unwrap();
        assert!(v.blocked);
        assert_eq!(v.reason, "");
    }

    #[test]
    fn verdict_accepts_alias_field_names() {
        // Common per-service alternative names are accepted.
        let v: GuardVerdict = serde_json::from_str(r#"{"is_blocked": true, "message": "pi"}"#)
            .unwrap();
        assert!(v.blocked);
        assert_eq!(v.reason, "pi");

        let v: GuardVerdict = serde_json::from_str(r#"{"block": false, "verdict": "ok"}"#).unwrap();
        assert!(!v.blocked);
        assert_eq!(v.reason, "ok");
    }

    // ----- error formatting -----

    #[test]
    fn guardrail_call_error_displays_cause() {
        let e = Error::GuardrailCall("HTTP 503 from guardrail endpoint".to_string());
        assert_eq!(
            e.to_string(),
            "guardrail verdict call failed: HTTP 503 from guardrail endpoint"
        );
    }
}
