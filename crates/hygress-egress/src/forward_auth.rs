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
//! - **FAIL_OPEN**: transport error or a 5xx response → `Ok(None)` (pass-through, no verdict). A
//!   2xx → `Ok(Some(VERDICT authenticated=true))`; any other status (3xx/4xx) →
//!   `Ok(Some(VERDICT authenticated=false))` (a real rejection, not a fail-open).
//! - **Timeout**: 30 s overall (`HIGRESS_EXT_AUTH_TIMEOUT_MS` default).
//!
//! No mock in impl: real HTTP via `reqwest`; transport failures are genuine connect/read errors that
//! map to the contract's fail-open behavior.

use std::time::Duration;

use http::header::{self, HeaderValue};
use http::HeaderMap;

use crate::Result;

/// Default forward-auth timeout (30 s, `HIGRESS_EXT_AUTH_TIMEOUT_MS`).
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// The auth-service path (ext-auth `endpoint.path`).
pub const TOKEN_AUTH_PATH: &str = "/token-auth";

/// `AUTH_CACHE_HEADER` — the cache JWT the auth service returns (and which the next hop forwards).
/// Value: `x-gpustack-auth-cache` (GPUStack `security.py:92`).
pub const AUTH_CACHE_HEADER: &str = "x-gpustack-auth-cache";

/// The gateway-derived auth token header injected into the `/token-auth` request.
pub const GATEWAY_AUTH_TOKEN_HEADER: &str = "x-gpustack-auth-token";

/// The consumer-attribution header the auth service returns.
pub const X_MSE_CONSUMER: &str = "x-mse-consumer";

/// Inbound (already-transformed) request to authenticate.
///
/// `headers` is the post-transformer inbound header set. Only the [allowlist][ALLOWLIST] is picked
/// out of it for the outbound `/token-auth` GET (the rest is dropped — forward-auth forwards a
/// fixed, minimal set).
#[derive(Clone, Debug, Default)]
pub struct ForwardAuthRequest {
    /// Inbound request headers (the 7 allowlisted headers are read from here).
    pub headers: HeaderMap,
}

impl ForwardAuthRequest {
    pub fn new(headers: HeaderMap) -> Self {
        Self { headers }
    }
}

/// The forward-auth decision, parsed from the auth-service **response headers** (body unused).
///
/// `authenticated` distinguishes a 2xx success (request may proceed with the write-back headers)
/// from a 4xx rejection (request must be rejected). A transport error / 5xx produces `None`
/// (FAIL_OPEN at the caller), not a `ForwardAuthVerdict`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
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

/// Out-of-band forward-auth client — `GET {base_url}/token-auth` with header pass-through and
/// write-back (design §7 ext-auth row).
#[derive(Clone, Debug)]
pub struct Client {
    /// Auth-service base URL (scheme + authority), e.g. `http://127.0.0.1:8080`.
    base_url: String,
    /// The `reqwest` client that performs the real HTTP call.
    http: reqwest::Client,
    /// The derived `X-GPUStack-Auth-Token` to inject (None → do not inject).
    auth_token: Option<String>,
    /// Overall request timeout (default 30 s).
    timeout: Duration,
}

impl Client {
    /// Build a client that `GET`s `{base_url}/token-auth`.
    ///
    /// Trailing slashes on `base_url` are trimmed so the path joins cleanly. The derived gateway
    /// token is set separately via [`with_auth_token`](Self::with_auth_token) (the call has the
    /// resolved `jwt_secret_key` in hand); a client built without it does not inject the token.
    pub fn new(base_url: &str, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
            auth_token: None,
            timeout: Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        }
    }

    /// Inject the derived `X-GPUStack-Auth-Token` into the `/token-auth` request.
    #[must_use]
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set the overall request timeout (default is [`DEFAULT_TIMEOUT_SECS`] = 30 s).
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
    /// Returns `Ok(None)` for FAIL_OPEN (transport error or 5xx), `Ok(Some(v))` otherwise.
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

        // Transport error (connect refused, DNS, timeout, ...) → FAIL_OPEN (pass through).
        let response = match builder.send().await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!("forward-auth transport error to {url}: {e} (FAIL_OPEN)");
                return Ok(None);
            }
        };

        let status = response.status();
        if status.is_server_error() {
            tracing::warn!("forward-auth {status} from {url} (FAIL_OPEN)");
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

/// Decode a header value to a `String` (lossy fallback when it is not valid UTF-8).
fn to_header_string(v: &HeaderValue) -> Option<String> {
    Some(v.to_str().ok()?.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_auth_url_joins_path_with_trailing_slash_trimmed() {
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
    fn header_string_roundtrip() {
        let v = HeaderValue::from_static("dummy=dummy");
        assert_eq!(to_header_string(&v), Some("dummy=dummy".to_string()));
    }
}

#[cfg(test)]
mod allowlist_tests {
    use super::*;

    /// The wasm ext-auth plugin always forwards the client `authorization` (Bearer) to
    /// `/token-auth`, regardless of the GPUStack `allowed_headers` config — this is what lets
    /// AUTHED models authenticate via the client API key. Pin that Hygress mirrors it.
    #[test]
    fn authorization_is_forwarded() {
        assert!(
            ALLOWLIST.contains(&"authorization"),
            "client Authorization must be forwarded to /token-auth (wasm ext-auth behavior)"
        );
    }
}
