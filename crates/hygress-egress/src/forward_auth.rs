//! ext-auth forward-auth client (`GET /token-auth`) — native equivalent of the
//! `gpustack-llm-ext-auth` plugin (design §7; plugin-contract-pin §2.1 / §5.3).
//!
//! The gateway calls the GPUStack server's `/token-auth` **out-of-band** (forward-auth) before
//! proxying a model-route request. Per the pin:
//!
//! - **Outbound** (`GET /token-auth`): forward ONLY the seven allowlisted inbound headers
//!   (`authorization`, `X-Real-IP`, `X-Forwarded-For`, `x-higress-llm-model`, `x-api-key`, `cookie`,
//!   `x-gpustack-auth-cache`) and **inject** `X-GPUStack-Auth-Token` = the derived gateway token.
//!   The client-supplied `X-GPUStack-Auth-Token` is NOT forwarded (inbound-spoofable; it is
//!   stripped by the transformer and replaced by the gateway's own injected value).
//! - **Write-back** (response → request), parsed from **headers only** (the body is never read):
//!   `X-Mse-Consumer` → consumer, `Authorization` → authorization, `cookie` → set_cookie,
//!   `x-gpustack-auth-cache` (`AUTH_CACHE_HEADER`) → auth_cache.
//! - **Availability** (R-12): this client is **verdict-returning**, never policy-deciding.
//!   A transport error or a 5xx response → `Ok(None)` — "no verdict: the auth service is
//!   unavailable" — and the **gateway** turns that into a decision per its configured
//!   `HYGRESS_EXT_AUTH_FAIL_MODE` (default: deny, 403, matching GPUStack/Higress
//!   `failure_mode_allow=false`; the legacy fail-open is an explicit opt-out there). The egress
//!   contract only reports the availability gap. A 2xx → `Ok(Some(VERDICT authenticated=true))`;
//!   any other status (3xx/4xx) → `Ok(Some(VERDICT authenticated=false))` (a real rejection).
//! - **Timeout**: 30 s overall by default; `HIGRESS_EXT_AUTH_TIMEOUT_MS` (ms, read once at
//!   [`Client::new`]) overrides it; [`Client::with_timeout`] overrides both per client.
//!
//! No mock in impl: real HTTP via `reqwest`; transport failures are genuine connect/read errors that
//! map to the no-verdict behavior above (the gateway applies its fail mode to the gap).

use std::time::Duration;

use http::header::{self, HeaderValue};
use http::HeaderMap;

use crate::Result;

/// Default forward-auth timeout in seconds (30).
///
/// Applied when `HIGRESS_EXT_AUTH_TIMEOUT_MS` is unset, empty, or not a strictly positive integer
/// of milliseconds; also the value a per-client [`Client::with_timeout`] starts from.
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Env var (ms, `u64`, > 0) that overrides the forward-auth request timeout at [`Client::new`]
/// time. GPUStack's ext-auth plugin writes this env and defaults it to 30000 (plugin-contract-pin
/// §5.3). ORA3-M6: previously the knob was documented but never read — `Client::new` now honors it.
const EXT_AUTH_TIMEOUT_MS_ENV: &str = "HIGRESS_EXT_AUTH_TIMEOUT_MS";

/// The auth-service path (ext-auth `endpoint.path`).
pub const TOKEN_AUTH_PATH: &str = "/token-auth";

/// `AUTH_CACHE_HEADER` — the cache JWT the auth service returns (and which the next hop forwards).
/// Value: `x-gpustack-auth-cache` (GPUStack `security.py:92`).
pub const AUTH_CACHE_HEADER: &str = "x-gpustack-auth-cache";

/// The gateway-derived auth token header injected into the `/token-auth` request.
pub const GATEWAY_AUTH_TOKEN_HEADER: &str = "x-gpustack-auth-token";

/// The consumer-attribution header the auth service returns.
pub const X_MSE_CONSUMER: &str = "x-mse-consumer";

/// Resolve the overall request timeout from the optional `HIGRESS_EXT_AUTH_TIMEOUT_MS` value (ms).
///
/// `None` (env unset), or a value that is not a strictly positive integer of milliseconds, keeps
/// the [`DEFAULT_TIMEOUT_SECS`] default — an invalid knob is logged (warn) rather than silently
/// producing an instant or unlimited request timeout. `0` is rejected for the same reason.
fn ext_auth_timeout_from_env(value: Option<&str>) -> Duration {
    let default = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
    match value {
        None => default,
        Some(raw) => match raw.trim().parse::<u64>() {
            Ok(ms) if ms > 0 => Duration::from_millis(ms),
            _ => {
                tracing::warn!(
                    "ignoring invalid {EXT_AUTH_TIMEOUT_MS_ENV}={raw:?}: expected a positive integer \
                     of milliseconds; using the {DEFAULT_TIMEOUT_SECS}s default"
                );
                default
            }
        },
    }
}

/// Inbound (already-transformed) request to authenticate.
///
/// `headers` is the post-transformer inbound header set. Only the allowlist is picked
/// out of it for the outbound `/token-auth` GET (the rest is dropped — forward-auth forwards a
/// fixed, minimal set).
#[derive(Clone, Debug, Default)]
pub struct ForwardAuthRequest {
    /// Inbound request headers (the 7 allowlisted headers are read from here).
    pub headers: HeaderMap,
}

impl ForwardAuthRequest {
    /// Build a request from the inbound (already-transformed) request header set.
    pub fn new(headers: HeaderMap) -> Self {
        Self { headers }
    }
}

/// The forward-auth decision, parsed from the auth-service **response headers** (body unused).
///
/// `authenticated` distinguishes a 2xx success (request may proceed with the write-back headers)
/// from a 4xx rejection (request must be rejected). A transport error / 5xx produces `None` —
/// "no verdict": the egress client could not authenticate, and the gateway applies its configured
/// fail mode to the missing verdict — never a `ForwardAuthVerdict`.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ForwardAuthVerdict {
    /// `true` for a 2xx auth success; `false` for a 4xx/3xx rejection.
    pub authenticated: bool,
    /// `X-Mse-Consumer` (e.g. `access_key.gpustack-<user>` or the `none` sentinel).
    pub consumer: Option<String>,
    /// `Authorization` (e.g. `Bearer <registration_token>`).
    pub authorization: Option<String>,
    /// The `cookie` header the auth service returns (the dummy cookie), or `Set-Cookie` fallback.
    pub set_cookie: Option<String>,
    /// `x-gpustack-auth-cache` — the 5-min cache JWT to write back (and forward next time).
    pub auth_cache: Option<String>,
}

// O11: manual `Debug` — the auth-service write-back carries credentials
// (`authorization` = `Bearer <registration_token>`, `auth_cache` = a JWT);
// those two are redacted so a `{:?}` in logs cannot leak them.
impl std::fmt::Debug for ForwardAuthVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForwardAuthVerdict")
            .field("authenticated", &self.authenticated)
            .field("consumer", &self.consumer)
            .field("authorization", &self.authorization.as_ref().map(|_| "<redacted>"))
            .field("set_cookie", &self.set_cookie)
            .field("auth_cache", &self.auth_cache.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Out-of-band forward-auth client — `GET {base_url}/token-auth` with header pass-through and
/// write-back (design §7 ext-auth row).
#[derive(Clone)]
pub struct Client {
    /// Auth-service base URL (scheme + authority), e.g. `http://127.0.0.1:8080`.
    base_url: String,
    /// The `reqwest` client that performs the real HTTP call.
    http: reqwest::Client,
    /// The derived `X-GPUStack-Auth-Token` to inject (None → do not inject).
    auth_token: Option<String>,
    /// Overall request timeout — `HIGRESS_EXT_AUTH_TIMEOUT_MS` (ms) when set and valid, else the
    /// 30 s default; [`with_timeout`](Self::with_timeout) overrides it for this client.
    timeout: Duration,
}

// O11: manual `Debug` — the derived `X-GPUStack-Auth-Token` is redacted so a
// `{:?}` of the client in logs cannot leak the credential.
impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("base_url", &self.base_url)
            .field("timeout", &self.timeout)
            .field("auth_token", &self.auth_token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl Client {
    /// Build a client that `GET`s `{base_url}/token-auth`.
    ///
    /// Trailing slashes on `base_url` are trimmed so the path joins cleanly. The derived gateway
    /// token is set separately via [`with_auth_token`](Self::with_auth_token) (the call has the
    /// resolved `jwt_secret_key` in hand); a client built without it does not inject the token.
    ///
    /// The overall request timeout is read from `HIGRESS_EXT_AUTH_TIMEOUT_MS` (ms) at this point —
    /// see `ext_auth_timeout_from_env` — falling back to the 30 s default; a later
    /// [`with_timeout`](Self::with_timeout) call overrides it.
    pub fn new(base_url: &str, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
            auth_token: None,
            timeout: ext_auth_timeout_from_env(
                std::env::var(EXT_AUTH_TIMEOUT_MS_ENV).ok().as_deref(),
            ),
        }
    }

    /// Inject the derived `X-GPUStack-Auth-Token` into the `/token-auth` request.
    #[must_use]
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Override the overall request timeout for this client. Takes precedence over the
    /// [`Client::new`] default (which already honors `HIGRESS_EXT_AUTH_TIMEOUT_MS`).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The full `/token-auth` URL this client calls.
    pub fn token_auth_url(&self) -> String {
        format!("{}{}", self.base_url, TOKEN_AUTH_PATH)
    }

    /// Perform forward-auth and return the parsed verdict (see struct docs + module docs for the
    /// full status→result mapping). The response **body is never read** (header-only contract).
    ///
    /// Returns `Ok(None)` when the auth service produced no verdict (transport error or 5xx — the
    /// gateway then applies its configured fail mode), `Ok(Some(v))` for every parsed verdict.
    pub async fn authenticate(
        &self,
        req: &ForwardAuthRequest,
    ) -> Result<Option<ForwardAuthVerdict>> {
        let url = self.token_auth_url();
        let mut builder = self.http.get(&url).timeout(self.timeout);

        // Forward ONLY the allowlist (pin §5.3). Each is copied only if it is present inbound.
        for name in ALLOWLIST {
            if let Some(value) = req.headers.get(name) {
                builder = builder.header(name, value.clone());
            }
        }
        // Inject the derived gateway token (NOT forwarded from the inbound request — it is the
        // gateway's own value, computed from the resolved jwt_secret_key).
        if let Some(token) = &self.auth_token {
            if let Ok(value) = HeaderValue::from_str(token) {
                builder = builder.header(GATEWAY_AUTH_TOKEN_HEADER, value);
            }
        }

        // Transport error (connect refused, DNS, timeout, ...): no verdict — the auth service is
        // unavailable. The gateway decides what the missing verdict means (default: deny, 403).
        // O6: DEBUG, not warn — this recurs at request rate while the auth service is down and
        // would flood the log; the outcome is counted by the gateway on
        // `hygress_auth_decisions_total{result="auth_service_unavailable_*"}`.
        let response = match builder.send().await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::debug!(
                    "forward-auth transport error to {url}: {e}; auth service unavailable — \
                     returning no verdict (the gateway applies its configured fail mode)"
                );
                return Ok(None);
            }
        };

        let status = response.status();
        if status.is_server_error() {
            // 5xx: the auth service answered but is unhealthy — same no-verdict contract as a
            // transport error; the gateway applies its configured fail mode to the gap.
            // O6: DEBUG at request rate (see the transport branch above); the metric carries it.
            tracing::debug!(
                "forward-auth {status} from {url}: auth service unavailable — returning no \
                 verdict (the gateway applies its configured fail mode)"
            );
            return Ok(None);
        }

        // 2xx → authenticated; 3xx/4xx → a real rejection (authenticated=false), still a verdict.
        let authenticated = status.is_success();
        let mut verdict = ForwardAuthVerdict {
            authenticated,
            ..ForwardAuthVerdict::default()
        };
        let headers = response.headers();

        if let Some(v) = headers.get(X_MSE_CONSUMER) {
            verdict.consumer = to_header_string(v);
        }
        if let Some(v) = headers.get(header::AUTHORIZATION) {
            verdict.authorization = to_header_string(v);
        }
        // GPUStack emits a literal `cookie` header (token.py `"cookie": "dummy=dummy"`; the
        // ext-auth `allowed_upstream_headers` lists `cookie`). Read `cookie` first; fall back to
        // `Set-Cookie` for other auth services. (Pin §5.3 write-back lists the dummy cookie.)
        if let Some(v) = headers
            .get(header::COOKIE)
            .or_else(|| headers.get(header::SET_COOKIE))
        {
            verdict.set_cookie = to_header_string(v);
        }
        if let Some(v) = headers.get(AUTH_CACHE_HEADER) {
            verdict.auth_cache = to_header_string(v);
        }

        Ok(Some(verdict))
    }
}

/// The allowlist of inbound headers forwarded to `/token-auth` (pin §5.3 outbound list).
///
/// Seven entries; order does not matter (HTTP headers are a set). `X-GPUStack-Auth-Token` is NOT here
/// — it is injected separately, not forwarded from the request.
///
/// `authorization` is forwarded iff present inbound — deliberately NOT gated on the GPUStack
/// `spec.extra[].allowed_headers` list (which omits it): the real Higress wasm ext-auth plugin
/// (`extensions/ext-auth/main.go`) explicitly `ExtractFromHeader(authorization)` + `Set`s it onto
/// the forward-auth request regardless, which is what lets AUTHED models authenticate via the
/// client's `Bearer` apiKey. Hygress mirrors that behavior.
const ALLOWLIST: [&str; 7] = [
    "authorization",
    "x-real-ip",
    "x-forwarded-for",
    "x-higress-llm-model",
    "x-api-key",
    "cookie",
    AUTH_CACHE_HEADER,
];

/// Decode a header value to a `String`.
///
/// HTTP header values may carry arbitrary (non-UTF-8) bytes. The write-back fields
/// (`Authorization` / `cookie` / `X-Mse-Consumer` / `AUTH_CACHE_HEADER`) are model strings that
/// the gateway re-emits as header values (see the write-back path), so a non-UTF-8 value cannot
/// be written back and must be dropped. MINOR-14: the drop is **logged**, not silent — in
/// practice this path is inert because GPUStack auth values are always ASCII, but a silent empty
/// write-back would be very hard to diagnose (a request would suddenly lose its credential).
fn to_header_string(v: &HeaderValue) -> Option<String> {
    match v.to_str() {
        Ok(s) => Some(s.to_string()),
        Err(_) => {
            tracing::warn!(
                "forward-auth response carries a non-UTF-8 header value; dropping the write-back value (write-back headers must be valid UTF-8 to be re-emitted)"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_auth_url_joins_path_with_trailing_slash_trimmed() { crate::test_support::install_ring_provider(); 
        let http = reqwest::Client::new();
        assert_eq!(
            Client::new("http://127.0.0.1:8080", http.clone()).token_auth_url(),
            "http://127.0.0.1:8080/token-auth"
        );
        assert_eq!(
            Client::new("http://127.0.0.1:8080/", http).token_auth_url(),
            "http://127.0.0.1:8080/token-auth"
        );
    }

    #[test]
    fn header_string_roundtrip() { crate::test_support::install_ring_provider(); 
        let v = HeaderValue::from_static("dummy=dummy");
        assert_eq!(to_header_string(&v), Some("dummy=dummy".to_string()));
    }

    #[test]
    fn non_utf8_header_value_is_dropped_not_misdecoded() { crate::test_support::install_ring_provider(); 
        // MINOR-14: a write-back header with non-UTF-8 bytes cannot be re-emitted as a model
        // string — the parser returns None (the drop is logged, never silent), and it must NOT
        // produce replacement-character garbage that would corrupt the credential.
        let v = HeaderValue::from_bytes(b"Bearer \xff\xfetoken").unwrap();
        assert_eq!(
            to_header_string(&v),
            None,
            "non-UTF-8 write-back must be dropped"
        );
        // A normal ASCII write-back still decodes cleanly.
        let ok = HeaderValue::from_static("Bearer ascii-token");
        assert_eq!(
            to_header_string(&ok),
            Some("Bearer ascii-token".to_string())
        );
    }

    // ----- ORA3-M6: HIGRESS_EXT_AUTH_TIMEOUT_MS is a real knob, not a dangling one -----

    /// `Client::new` must actually read the env var: a valid ms value wins at construction, and an
    /// unset var keeps the 30 s default.
    #[test]
    fn client_reads_higress_ext_auth_timeout_ms_at_construction() { crate::test_support::install_ring_provider(); 
        let http = reqwest::Client::new();
        std::env::set_var(EXT_AUTH_TIMEOUT_MS_ENV, "1500");
        let client = Client::new("http://127.0.0.1:8080", http.clone());
        assert_eq!(
            client.timeout,
            Duration::from_millis(1500),
            "a valid HIGRESS_EXT_AUTH_TIMEOUT_MS must be applied"
        );
        std::env::remove_var(EXT_AUTH_TIMEOUT_MS_ENV);
        let client = Client::new("http://127.0.0.1:8080", http);
        assert_eq!(
            client.timeout,
            Duration::from_secs(DEFAULT_TIMEOUT_SECS),
            "unset HIGRESS_EXT_AUTH_TIMEOUT_MS keeps the 30 s default"
        );
    }

    /// Invalid values (garbage / empty / whitespace / negative / zero) must keep the default rather
    /// than silently producing an instant or unlimited request timeout.
    #[test]
    fn invalid_timeout_env_value_keeps_the_default() { crate::test_support::install_ring_provider(); 
        let default = Duration::from_secs(DEFAULT_TIMEOUT_SECS);
        for bad in ["", "abc", "1.5s", "-1", "0", "   "] {
            assert_eq!(
                ext_auth_timeout_from_env(Some(bad)),
                default,
                "invalid value {bad:?} must keep the default"
            );
        }
        // Unset → default; whitespace around a valid number is tolerated and ms are honored.
        assert_eq!(ext_auth_timeout_from_env(None), default);
        assert_eq!(
            ext_auth_timeout_from_env(Some(" 3000 ")),
            Duration::from_millis(3000)
        );
    }
}

#[cfg(test)]
mod allowlist_tests {
    use super::*;

    /// The wasm ext-auth plugin always forwards the client `authorization` (Bearer) to
    /// `/token-auth`, regardless of the GPUStack `allowed_headers` config — this is what lets
    /// AUTHED models authenticate via the client API key. Pin that Hygress mirrors it.
    #[test]
    fn authorization_is_forwarded() { crate::test_support::install_ring_provider(); 
        assert!(
            ALLOWLIST.contains(&"authorization"),
            "client Authorization must be forwarded to /token-auth (wasm ext-auth behavior)"
        );
    }
}
