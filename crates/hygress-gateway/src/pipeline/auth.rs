//! ⑤ ext-auth (`gpustack-llm-ext-auth`) equivalent — **scope is pure**; the
//! forward-auth callsite lives under the `integrations` feature (default).
//!
//! The **frozen security invariant** (design §9 / pin §2.3): ext-auth scope is
//! the **origin ingress name prefix** `ai-route-route-` — *never* a path prefix
//! (a path prefix would open a FAIL_OPEN hole). A matched route requires auth
//! iff its origin ingress name (optional `gateway_namespace/` prefix stripped)
//! starts with `ai-route-route-`. Mirror (`gpustack`) and non-GPUStack routes
//! are never authenticated. [`required`] (pure) encodes exactly this via the
//! core [`hygress_core::AuthScope`]; the actual `GET /token-auth` request is
//! [`authenticate`] (`integrations`-gated, egress `forward_auth::Client`).
//!
//! Write-back / auth-service failure (R-12): on a real `401` (a genuine
//! rejection) the pipe short-circuits (401). When `/token-auth` is
//! unreachable or answers 5xx, the outcome is
//! [`AuthOutcome::AuthServiceUnavailable`]; the pipe then rejects (403,
//! default — matching GPUStack/Higress `failure_mode_allow=false`) or
//! fail-opens (env `HYGRESS_EXT_AUTH_FAIL_MODE=open`) per its configured
//! `auth_fail_closed` mode.

use hygress_core::prelude::{HeaderMap, RouteRule};

#[cfg(feature = "integrations")]
use crate::context::hdr;

/// Pure scope decision: does the matched route require ext-auth?
///
/// Delegates to the core `RouteRule::requires_auth` (origin ingress name
/// prefix `ai-route-route-`, ns prefix stripped). A mirror / non-GPUStack route
/// returns `false`.
pub fn required(route: &RouteRule) -> bool {
    route.requires_auth()
}

/// The outcome of a forward-auth exchange (drives 401 vs. proceed). Pure type —
/// the egress glue that produces it is `integrations`-gated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthOutcome {
    /// Proceed (authenticated). `write_back` carries the response headers to
    /// apply to the outbound request.
    Allowed {
        write_back: HeaderMap,
    },
    /// A real (non-fail-open) denial → the pipe responds 401.
    Denied,
    /// The auth service could not be reached / answered 5xx (the egress
    /// client returned "no verdict"). How the pipe reacts is the configured
    /// `auth_fail_closed` mode (R-12): `true` → reject (default, matches the
    /// GPUStack/Higress `failure_mode_allow=false` behavior: 403), `false`
    /// → legacy fail-open (proceed without write-back).
    AuthServiceUnavailable,
}

/// Build the outbound write-back header set from a forward-auth verdict.
///
/// `integrations`-gated: consumes the frozen
/// `hygress_egress::forward_auth::ForwardAuthVerdict`.
#[cfg(feature = "integrations")]
pub fn write_back(v: &hygress_egress::forward_auth::ForwardAuthVerdict) -> HeaderMap {
    let mut h = HeaderMap::new();
    // The frozen egress verdict fields are `Option<String>` (absent when the auth
    // service did not return them); treat `None` / empty as "no write-back".
    if let Some(c) = v.consumer.as_deref().filter(|s| !s.is_empty()) {
        h.insert(hdr::MSE_CONSUMER, c.to_string());
    }
    if let Some(a) = v.authorization.as_deref().filter(|s| !s.is_empty()) {
        h.insert(hdr::AUTHORIZATION, a.to_string());
    }
    if let Some(c) = v.set_cookie.as_deref().filter(|s| !s.is_empty()) {
        h.insert(hdr::COOKIE, c.to_string());
    }
    if let Some(a) = v.auth_cache.as_deref().filter(|s| !s.is_empty()) {
        h.insert(hdr::AUTH_CACHE, a.to_string());
    }
    h
}

/// The forward-auth exchange (stage ⑤). `integrations`-gated: consumes the
/// frozen egress contract
/// `hygress_egress::forward_auth::{Client, ForwardAuthRequest}`.
///
/// - `Ok(Some(v))` with `v.authenticated` → `Allowed { write_back }`.
/// - `Ok(Some(v))` with `!v.authenticated` → `Denied` (real 401).
/// - `Ok(None)` (the auth service was unreachable / answered 5xx — no verdict)
///   → `AuthServiceUnavailable`.
/// - `Err(..)` (transport/parse failure) → `AuthServiceUnavailable`.
///
/// How `AuthServiceUnavailable` is resolved is the **pipe's** configured
/// `auth_fail_closed` mode (R-12): default fail-closed → 403; explicit
/// `HYGRESS_EXT_AUTH_FAIL_MODE=open` → legacy fail-open (proceed without
/// write-back). Neither arm of this function ever fail-opens by itself.
#[cfg(feature = "integrations")]
pub async fn authenticate(
    client: &hygress_egress::forward_auth::Client,
    headers: &HeaderMap,
) -> AuthOutcome {
    use tracing::warn;

    // The core `HeaderMap` is a flat string set; the egress contract takes a
    // `http::HeaderMap` (typed names/values). Rebuild it, dropping any entry that
    // is not a valid HTTP header (defensive: the transformer already normalized
    // the set, so this should be a lossless copy in practice).
    let mut http_headers: http::HeaderMap = http::HeaderMap::new();
    for name in headers.names() {
        for value in headers.get_all(name) {
            if let (Ok(n), Ok(v)) = (
                http::HeaderName::from_bytes(name.as_bytes()),
                http::HeaderValue::from_bytes(value.as_bytes()),
            ) {
                http_headers.append(n, v);
            }
        }
    }

    let req = hygress_egress::forward_auth::ForwardAuthRequest {
        headers: http_headers,
    };
    match client.authenticate(&req).await {
        Ok(Some(v)) => {
            if v.authenticated {
                AuthOutcome::Allowed {
                    write_back: write_back(&v),
                }
            } else {
                AuthOutcome::Denied
            }
        }
        Ok(None) => AuthOutcome::AuthServiceUnavailable,
        Err(e) => {
            warn!(error = %e, "forward-auth error; auth service unavailable (mode decided by the pipe)");
            AuthOutcome::AuthServiceUnavailable
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::prelude::{Destination, PathPred, RouteKind};

    fn main_route(ingress: &str) -> RouteRule {
        RouteRule::new("k", RouteKind::Main, vec![PathPred::new("/")], vec![Destination::new("a.static:80")])
            .unwrap()
            .with_ingress_name(ingress)
    }

    #[test]
    fn scoped_ingress_requires_auth() {
        assert!(required(&main_route("higress-system/ai-route-route-5.internal")));
        // Bare (no ns) form still scoped.
        assert!(required(&main_route("ai-route-route-5.internal")));
    }

    #[test]
    fn non_scoped_or_mirror_never_auths() {
        assert!(!required(&main_route("gpustack")));
        assert!(!required(&main_route("higress-system/ai-route-model-3")));
        let mirror = RouteRule::new(
            "gpustack",
            RouteKind::Mirror,
            vec![PathPred::new("/")],
            vec![Destination::new("gpustack.dns:30080")],
        )
        .unwrap()
        .with_ingress_name("higress-system/ai-route-route-9.internal");
        assert!(!required(&mirror));
    }
}
