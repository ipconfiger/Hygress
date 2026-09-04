//! 15020 pilot-agent metrics shallow-compat service (design §11.1).
//!
//! The Higress/Istio sidecar contract expects a pilot-agent on `15020` exposing
//! `GET /stats/prometheus` (prometheus text) and `GET /stats` (JSON). This is a
//! **shallow** compat endpoint: it is a thin, read-only view over the same
//! [`Metrics`] registry the data plane records into — no separate stats store,
//! no envoy-internal semantics. It lets an operator scrape the gateway exactly
//! where the sidecar contract says, without the data plane binding 15020 itself
//! (the data plane binds only `GATEWAY_HTTP_PORT`/`GATEWAY_TLS_PORT`; the port
//! discipline in design §11 keeps 15010/15012/8888/15051/9876 free).
//!
//! Same split as [`crate::admin`]: the routing decision is a **pure**
//! [`StatsState::route`] (unit-tested), the [`ServeHttp`] impl is a thin wrapper.

use std::sync::Arc;

use async_trait::async_trait;
use http::{header, Response, StatusCode};
use pingora_core::apps::http_app::ServeHttp;
use pingora_core::protocols::http::ServerSession;

use crate::metrics::Metrics;

/// Shared state for the 15020 stats service (a read-only view of [`Metrics`]).
#[derive(Clone)]
pub struct StatsState {
    pub metrics: Arc<Metrics>,
}

impl StatsState {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }

    /// Pure routing decision for the shallow-compat surface.
    pub fn route(&self, method: &str, path: &str) -> StatsResp {
        match (method, path) {
            ("GET", "/stats/prometheus") => StatsResp::new(
                200,
                "text/plain; version=0.0.4; charset=utf-8",
                self.metrics.encode(),
            ),
            ("GET", "/stats") => StatsResp::new(
                200,
                "application/json; charset=utf-8",
                "{\"state\":\"LIVE\"}\n",
            ),
            _ => StatsResp::new(404, "application/json; charset=utf-8", "{\"error\":\"not_found\"}\n"),
        }
    }
}

/// One stats response (status + content-type + body).
#[derive(Clone, Debug)]
pub struct StatsResp {
    pub status: u16,
    pub content_type: &'static str,
    pub body: String,
}

impl StatsResp {
    pub fn new(status: u16, content_type: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type,
            body: body.into(),
        }
    }
}

/// The Pingora `ServeHttp` app: thin wrapper over [`StatsState::route`].
pub struct StatsService {
    state: Arc<StatsState>,
}

impl StatsService {
    pub fn new(state: Arc<StatsState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ServeHttp for StatsService {
    async fn response(&self, session: &mut ServerSession) -> Response<Vec<u8>> {
        let method = session.req_header().method.as_str().to_string();
        let path = session.req_header().uri.path().to_string();
        let resp = self.state.route(&method, &path);
        let status = StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut out = Response::new(resp.body.into_bytes());
        *out.status_mut() = status;
        if let Ok(ct) = resp.content_type.parse() {
            out.headers_mut().insert(header::CONTENT_TYPE, ct);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_prometheus_exposes_metrics() {
        let s = StatsState::new(Arc::new(Metrics::new()));
        // Seed one sample so the family has a child series and renders.
        s.metrics.record_request(200, "model_route");
        let r = s.route("GET", "/stats/prometheus");
        assert_eq!(r.status, 200);
        assert!(r.content_type.contains("text/plain"));
        assert!(r.body.contains("hygress_requests_total"));
    }

    #[test]
    fn stats_json_is_shallow_live() {
        let s = StatsState::new(Arc::new(Metrics::new()));
        let r = s.route("GET", "/stats");
        assert_eq!(r.status, 200);
        assert!(r.body.contains("LIVE"));
    }

    #[test]
    fn unknown_stats_path_404() {
        let s = StatsState::new(Arc::new(Metrics::new()));
        assert_eq!(s.route("GET", "/stats/unknown").status, 404);
        assert_eq!(s.route("POST", "/stats/prometheus").status, 404);
    }
}
