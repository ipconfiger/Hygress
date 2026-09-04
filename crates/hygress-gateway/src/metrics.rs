//! Prometheus metrics (design §10). A single [`Metrics`] holds one
//! [`Registry`]; the admin `/metrics` (and the 15020 shallow-compat `/stats/
//! prometheus`) scrape it. Counters/histograms are real — no stubs.

use std::sync::Arc;

use prometheus::core::Collector;
use prometheus::{HistogramVec, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder};

/// Central metrics handle. All recording methods are cheap (lazy label vec);
/// clone the handle (`Arc<Metrics>`) per request.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Registry,
    requests_total: IntCounterVec,
    request_duration: HistogramVec,
    tokens_total: IntCounterVec,
    ttft: HistogramVec,
    retries_total: IntCounter,
    upstream_errors_total: IntCounter,
    fallback_total: IntCounter,
    auth_decisions: IntCounterVec,
    active_requests: IntGauge,
}

impl Metrics {
    /// Register every metric family with `prefix = "hygress_"`.
    pub fn new() -> Self {
        let registry = Registry::new();
        let requests_total = IntCounterVec::new(
            prometheus::Opts::new("hygress_requests_total", "Requests by status and kind."),
            &["status", "kind"],
        )
        .expect("requests_total");
        let request_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "hygress_request_duration_seconds",
                "End-to-end request latency.",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
            ]),
            &["kind"],
        )
        .expect("request_duration");
        let tokens_total = IntCounterVec::new(
            prometheus::Opts::new(
                "hygress_tokens_total",
                "Tokens observed (prompt/completion/cached).",
            ),
            &["direction"],
        )
        .expect("tokens_total");
        let ttft = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "hygress_ttft_seconds",
                "Time to first response chunk.",
            )
            .buckets(vec![
                0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
            &["kind"],
        )
        .expect("ttft");
        let retries_total =
            IntCounter::new("hygress_retries_total", "Failover retries across candidates.")
                .expect("retries_total");
        let upstream_errors_total =
            IntCounter::new("hygress_upstream_errors_total", "Upstream attempt failures.")
                .expect("upstream_errors");
        let fallback_total =
            IntCounter::new("hygress_fallback_total", "Fallback redirects taken.")
                .expect("fallback_total");
        let auth_decisions = IntCounterVec::new(
            prometheus::Opts::new("hygress_auth_decisions_total", "Auth decisions."),
            &["result"],
        )
        .expect("auth_decisions");
        let active_requests =
            IntGauge::new("hygress_active_requests", "In-flight requests.")
                .expect("active_requests");

        let collectors: Vec<Box<dyn Collector>> = vec![
            Box::new(requests_total.clone()),
            Box::new(request_duration.clone()),
            Box::new(tokens_total.clone()),
            Box::new(ttft.clone()),
            Box::new(retries_total.clone()),
            Box::new(upstream_errors_total.clone()),
            Box::new(fallback_total.clone()),
            Box::new(auth_decisions.clone()),
            Box::new(active_requests.clone()),
        ];
        for c in collectors {
            registry.register(c).expect("metric registration");
        }

        Self {
            inner: Arc::new(Inner {
                registry,
                requests_total,
                request_duration,
                tokens_total,
                ttft,
                retries_total,
                upstream_errors_total,
                fallback_total,
                auth_decisions,
                active_requests,
            }),
        }
    }

    pub fn record_request(&self, status: u16, kind: &str) {
        self.inner
            .requests_total
            .with_label_values(&[&status.to_string(), kind])
            .inc();
    }

    pub fn record_request_duration(&self, kind: &str, secs: f64) {
        self.inner
            .request_duration
            .with_label_values(&[kind])
            .observe(secs);
    }

    pub fn record_tokens(&self, direction: &str, n: u64) {
        self.inner
            .tokens_total
            .with_label_values(&[direction])
            .inc_by(n);
    }

    pub fn record_ttft(&self, kind: &str, secs: f64) {
        self.inner.ttft.with_label_values(&[kind]).observe(secs);
    }

    pub fn record_retry(&self) {
        self.inner.retries_total.inc();
    }

    pub fn record_upstream_error(&self) {
        self.inner.upstream_errors_total.inc();
    }

    pub fn record_fallback(&self) {
        self.inner.fallback_total.inc();
    }

    pub fn record_auth(&self, result: &str) {
        self.inner
            .auth_decisions
            .with_label_values(&[result])
            .inc();
    }

    pub fn active_requests_inc(&self) {
        self.inner.active_requests.inc();
    }

    pub fn active_requests_dec(&self) {
        self.inner.active_requests.dec();
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn encode(&self) -> String {
        let metric_families = self.inner.registry.gather();
        TextEncoder::new()
            .encode_to_string(&metric_families)
            .unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_emits_registered_families() {
        let m = Metrics::new();
        m.record_request(200, "model_route");
        m.record_tokens("prompt", 12);
        m.record_fallback();
        let out = m.encode();
        assert!(out.contains("hygress_requests_total"));
        assert!(out.contains("hygress_tokens_total"));
        assert!(out.contains("hygress_fallback_total"));
    }
}
