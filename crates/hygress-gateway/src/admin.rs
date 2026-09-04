//! Admin HTTP service (design §13): a Pingora [`ServeHttp`] app on its own
//! plain-TCP port (`HYGRESS_ADMIN_ADDR`, default `127.0.0.1:8081`) sharing the
//! proxy's Tokio runtime (no axum, no second runtime).
//!
//! ## Surface
//!
//! - `GET /healthz`        — process/registry liveness (open; no token).
//! - `GET /metrics`        — self-hosted prometheus exposition ([`Metrics`]) (open).
//! - `POST /reload`        — trigger a config reload (token-gated, fail-closed).
//! - `GET /stats/usage`    — token/request usage stats (token-gated, fail-closed).
//!
//! ## Token gate (design §13.3)
//!
//! `/healthz` + `/metrics` stay open (healthchecks); `/reload` + `/stats/usage`
//! require `Authorization: Bearer <HYGRESS_ADMIN_TOKEN>` and fail **closed**
//! (denied) when no token is configured. The routing decision is a **pure**
//! function ([`AdminState::route`]) over `(method, path, headers)` so it is
//! unit-testable without a running server; the [`ServeHttp`] impl is a thin
//! wrapper that copies the request, routes, and writes the response.

use std::sync::Arc;

use async_trait::async_trait;
use http::{header, Response, StatusCode};
use pingora_core::apps::http_app::ServeHttp;
use pingora_core::protocols::http::ServerSession;

use hygress_core::transform::HeaderMap;

use crate::metrics::Metrics;

/// Shared admin state. Cheap to `Arc`-clone.
#[derive(Clone)]
pub struct AdminState {
    /// Central metrics handle (scraped by `/metrics` and `/stats/usage`).
    pub metrics: Arc<Metrics>,
    /// Admin bearer token. `None` ⇒ the gated endpoints deny (fail-closed).
    pub admin_token: Option<String>,
    /// Optional reload hook. Returns `true` on success (new policy swapped),
    /// `false` on failure (last-known-good kept). `None` ⇒ `/reload` reports
    /// 501 (reload not wired).
    pub reloader: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
}

impl AdminState {
    /// Build admin state.
    pub fn new(
        metrics: Arc<Metrics>,
        admin_token: Option<String>,
        reloader: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    ) -> Self {
        Self {
            metrics,
            admin_token,
            reloader,
        }
    }

    /// Extract the `Authorization: Bearer <token>` value, if present.
    pub fn bearer_token(headers: &HeaderMap) -> Option<&str> {
        headers
            .get("authorization")
            .and_then(|s| s.strip_prefix("Bearer ").or_else(|| s.strip_prefix("bearer ")))
    }

    /// Token gate: `true` when the request's bearer matches the configured
    /// token. Fail-closed when no token is configured.
    pub fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(token) = &self.admin_token else {
            return false;
        };
        Self::bearer_token(headers) == Some(token.as_str())
    }

    /// Pure routing decision: maps `(method, path, headers)` to a response
    /// (status / content-type / body). No I/O — fully unit-testable.
    pub fn route(&self, method: &str, path: &str, headers: &HeaderMap) -> AdminResp {
        match (method, path) {
            ("GET", "/healthz") => AdminResp::new(200, "text/plain; charset=utf-8", "ok\n"),
            ("GET", "/metrics") => AdminResp::new(
                200,
                "text/plain; version=0.0.4; charset=utf-8",
                self.metrics.encode(),
            ),
            ("POST", "/reload") => {
                if !self.authorized(headers) {
                    return AdminResp::json(401, "unauthorized", "missing or invalid admin token");
                }
                match &self.reloader {
                    Some(reload) => {
                        if reload() {
                            AdminResp::json(200, "ok", "policy reloaded")
                        } else {
                            AdminResp::json(
                                500,
                                "reload_failed",
                                "reload failed; last-known-good policy retained",
                            )
                        }
                    }
                    None => AdminResp::json(501, "not_implemented", "reload is not wired"),
                }
            }
            ("GET", "/stats/usage") => {
                if !self.authorized(headers) {
                    return AdminResp::json(401, "unauthorized", "missing or invalid admin token");
                }
                AdminResp::new(
                    200,
                    "text/plain; version=0.0.4; charset=utf-8",
                    self.metrics.encode(),
                )
            }
            _ => AdminResp::json(404, "not_found", "unknown path"),
        }
    }
}

/// One admin response (status + content-type + body), produced by the pure
/// [`AdminState::route`].
#[derive(Clone, Debug)]
pub struct AdminResp {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
}

impl AdminResp {
    pub fn new(status: u16, content_type: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type,
            body: body.into(),
        }
    }
    pub fn json(status: u16, reason: &str, message: &str) -> Self {
        Self::new(
            status,
            "application/json; charset=utf-8",
            format!("{{\"reason\":\"{reason}\",\"message\":\"{message}\"}}"),
        )
    }
}

/// The Pingora `ServeHttp` app: thin wrapper over [`AdminState::route`].
pub struct AdminService {
    state: Arc<AdminState>,
}

impl AdminService {
    pub fn new(state: Arc<AdminState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ServeHttp for AdminService {
    async fn response(&self, session: &mut ServerSession) -> Response<Vec<u8>> {
        let method = session.req_header().method.as_str().to_string();
        let path = session.req_header().uri.path().to_string();
        let mut headers = HeaderMap::new();
        for (name, value) in session.req_header().headers.iter() {
            if let Ok(v) = value.to_str() {
                headers.append(name.as_str(), v.to_string());
            }
        }

        let resp = self.state.route(&method, &path, &headers);
        let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut out = Response::new(resp.body.into_bytes());
        *out.status_mut() = status;
        if let Ok(ct) = resp.content_type.parse() {
            out.headers_mut().insert(header::CONTENT_TYPE, ct);
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Tests — the pure routing decision (no server, no I/O).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn state(token: Option<&str>) -> AdminState {
        AdminState::new(Arc::new(Metrics::new()), token.map(str::to_string), None)
    }

    fn h() -> HeaderMap {
        HeaderMap::new()
    }

    #[test]
    fn healthz_open() {
        let s = state(None);
        let r = s.route("GET", "/healthz", &h());
        assert_eq!(r.status, 200);
        assert_eq!(r.body, "ok\n");
    }

    #[test]
    fn metrics_open_and_exposes_prometheus() {
        let s = state(None);
        // Seed one sample so the family has a child series and renders.
        s.metrics.record_request(200, "model_route");
        let r = s.route("GET", "/metrics", &h());
        assert_eq!(r.status, 200);
        assert!(r.content_type.contains("text/plain"));
        assert!(r.body.contains("hygress_requests_total"));
    }

    #[test]
    fn reload_open_when_token_unset_fails_closed() {
        let s = state(None);
        let r = s.route("POST", "/reload", &h());
        assert_eq!(r.status, 401);
    }

    #[test]
    fn reload_requires_matching_token() {
        let s = state(Some("secret"));
        let mut hh = h();
        hh.insert("authorization", "Bearer wrong");
        let r = s.route("POST", "/reload", &hh);
        assert_eq!(r.status, 401);

        let mut ok = h();
        ok.insert("authorization", "Bearer secret");
        // No reloader wired → 501 once authenticated.
        let r2 = s.route("POST", "/reload", &ok);
        assert_eq!(r2.status, 501);
    }

    #[test]
    fn reload_invokes_reloader() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c2 = counter.clone();
        let reloader: Arc<dyn Fn() -> bool + Send + Sync> =
            Arc::new(move || {
                c2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                true
            });
        let s = AdminState::new(
            Arc::new(Metrics::new()),
            Some("secret".to_string()),
            Some(reloader),
        );
        let mut ok = h();
        ok.insert("authorization", "Bearer secret");
        let r = s.route("POST", "/reload", &ok);
        assert_eq!(r.status, 200);
        assert!(r.body.contains("policy reloaded"));
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn reload_failure_reports_500() {
        let reloader: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(|| false);
        let s = AdminState::new(
            Arc::new(Metrics::new()),
            Some("secret".to_string()),
            Some(reloader),
        );
        let mut ok = h();
        ok.insert("authorization", "Bearer secret");
        let r = s.route("POST", "/reload", &ok);
        assert_eq!(r.status, 500);
        assert!(r.body.contains("last-known-good"));
    }

    #[test]
    fn stats_usage_gated() {
        let s = state(None);
        assert_eq!(s.route("GET", "/stats/usage", &h()).status, 401);

        let s = state(Some("tk"));
        // Seed one sample so the tokens family renders.
        s.metrics.record_tokens("prompt", 12);
        let mut ok = h();
        ok.insert("authorization", "Bearer tk");
        let r = s.route("GET", "/stats/usage", &ok);
        assert_eq!(r.status, 200);
        assert!(r.body.contains("hygress_tokens_total"));
    }

    #[test]
    fn unknown_path_404() {
        let s = state(None);
        assert_eq!(s.route("GET", "/nope", &h()).status, 404);
        assert_eq!(s.route("DELETE", "/healthz", &h()).status, 404);
    }
}
