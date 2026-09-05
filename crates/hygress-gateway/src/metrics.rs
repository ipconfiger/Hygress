//! Prometheus metrics (design §10). A single [`Metrics`] holds one
//! [`Registry`]; the admin `/metrics` (and the 15020 shallow-compat `/stats/
//! prometheus`) scrape it. Counters/histograms are real — no stubs.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use prometheus::core::{Collector, Desc};
use prometheus::proto;
use prometheus::{HistogramVec, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder};

/// A custom prometheus [`Collector`] that publishes the two core control-plane
/// snapshot counters ([`hygress_core::SharedConfig::snapshot_reject_total`] /
/// `snapshot_skipped_total`, R-4) at scrape time. The counters live on the
/// core `SharedConfig` (incremented by the adapter's store path without any
/// prometheus dependency); this collector only *reads* the atomics.
pub struct ConfigSnapshotCollector {
    shared: Arc<hygress_core::SharedConfig>,
    reject: Desc,
    skipped: Desc,
}

const REJECT_NAME: &str = "hygress_config_reject_total";
const REJECT_HELP: &str = "Control-plane snapshots rejected as a whole (structural, keep-last-known-good).";
const SKIPPED_NAME: &str = "hygress_config_object_skipped_total";
const SKIPPED_HELP: &str = "Control-plane objects skipped by per-object validation.";

fn snapshot_desc(name: &str, help: &str) -> Desc {
    Desc::new(
        name.to_string(),
        help.to_string(),
        Vec::new(),
        std::collections::HashMap::new(),
    )
    .expect("config counter desc")
}

impl ConfigSnapshotCollector {
    pub fn new(shared: Arc<hygress_core::SharedConfig>) -> Self {
        Self {
            shared,
            reject: snapshot_desc(REJECT_NAME, REJECT_HELP),
            skipped: snapshot_desc(SKIPPED_NAME, SKIPPED_HELP),
        }
    }
}

impl Collector for ConfigSnapshotCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.reject, &self.skipped]
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let reject = self.shared.snapshot_reject_total.load(Ordering::Relaxed);
        let skipped = self.shared.snapshot_skipped_total.load(Ordering::Relaxed);
        vec![
            counter_family(REJECT_NAME, REJECT_HELP, reject),
            counter_family(SKIPPED_NAME, SKIPPED_HELP, skipped),
        ]
    }
}

fn counter_family(name: &str, help: &str, value: u64) -> proto::MetricFamily {
    let mut f = proto::MetricFamily::default();
    f.set_name(name.to_string());
    f.set_help(help.to_string());
    let mut metric = proto::Metric::default();
    let mut counter = proto::Counter::default();
    counter.set_value(value as f64);
    metric.set_counter(counter);
    f.set_metric(vec![metric]);
    f
}

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
    // Extension stages (design §4): rate limiting / quota / routing policy /
    // guardrail counters.
    rate_limit_denied: IntCounterVec,
    quota_denied: IntCounter,
    quota_soft_exceed: IntCounter,
    policy_applied: IntCounterVec,
    guardrail_blocked: IntCounterVec,
    // R-11 (C3): TLS rotation detection (0.8 = no hot reload → restart needed).
    tls_cert_change_detected: IntCounter,
    tls_cert_requires_restart: IntCounter,
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
        let rate_limit_denied = IntCounterVec::new(
            prometheus::Opts::new(
                "hygress_rate_limit_denied_total",
                "Rate-limit denials by dimension (ip/consumer).",
            ),
            &["dimension"],
        )
        .expect("rate_limit_denied");
        let quota_denied = IntCounter::new(
            "hygress_quota_denied_total",
            "Token-quota hard-limit denials (429).",
        )
        .expect("quota_denied");
        let quota_soft_exceed = IntCounter::new(
            "hygress_quota_soft_exceed_total",
            "Token-quota soft-limit exceeds (allowed, warning bit).",
        )
        .expect("quota_soft_exceed");
        let policy_applied = IntCounterVec::new(
            prometheus::Opts::new(
                "hygress_policy_applied_total",
                "Routing-policy application outcomes (applied true/false).",
            ),
            &["applied"],
        )
        .expect("policy_applied");
        let guardrail_blocked = IntCounterVec::new(
            prometheus::Opts::new(
                "hygress_guardrail_blocked_total",
                "Guardrail blocks by side (in/out).",
            ),
            &["side"],
        )
        .expect("guardrail_blocked");
        // R-11 (C3): TLS rotation detection counters.
        let tls_cert_change_detected = IntCounter::new(
            "hygress_tls_cert_change_detected_total",
            "Control-plane TLS content fingerprint changes detected at runtime.",
        )
        .expect("tls_cert_change_detected");
        let tls_cert_requires_restart = IntCounter::new(
            "hygress_tls_cert_requires_restart_total",
            "TLS rotation events that require a container restart (pingora 0.8 has no hot reload).",
        )
        .expect("tls_cert_requires_restart");

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
            Box::new(rate_limit_denied.clone()),
            Box::new(quota_denied.clone()),
            Box::new(quota_soft_exceed.clone()),
            Box::new(policy_applied.clone()),
            Box::new(guardrail_blocked.clone()),
            Box::new(tls_cert_change_detected.clone()),
            Box::new(tls_cert_requires_restart.clone()),
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
                rate_limit_denied,
                quota_denied,
                quota_soft_exceed,
                policy_applied,
                guardrail_blocked,
                tls_cert_change_detected,
                tls_cert_requires_restart,
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

    /// A rate-limit denial (design §4.1): `dimension` is `ip` or `consumer`.
    pub fn record_rate_limit_denied(&self, dimension: &str) {
        self.inner
            .rate_limit_denied
            .with_label_values(&[dimension])
            .inc();
    }

    /// A token-quota hard-limit denial (429, design §4.2).
    pub fn record_quota_denied(&self) {
        self.inner.quota_denied.inc();
    }

    /// A token-quota soft-limit exceed (allowed; the warning bit, design §4.2).
    pub fn record_quota_soft_exceed(&self) {
        self.inner.quota_soft_exceed.inc();
    }

    /// A routing-policy application outcome (design §4.3 / D-2): `applied`
    /// `true` when a policy action took effect, `false` when the policy was
    /// present but none of its actions did (e.g. an `override_route` miss →
    /// runtime fallback).
    pub fn record_policy_applied(&self, applied: bool) {
        self.inner
            .policy_applied
            .with_label_values(&[if applied { "true" } else { "false" }])
            .inc();
    }

    /// A guardrail block (design §4.4): `side` is `in` (request side) or `out`
    /// (response side / B4c).
    pub fn record_guardrail_blocked(&self, side: &str) {
        self.inner
            .guardrail_blocked
            .with_label_values(&[side])
            .inc();
    }

    /// R-11 (C3): a TLS content fingerprint change was detected at runtime.
    pub fn record_tls_cert_change_detected(&self) {
        self.inner.tls_cert_change_detected.inc();
    }

    /// R-11 (C3): a TLS rotation event requiring a container restart (pingora
    /// 0.8 has no hot reload).
    pub fn record_tls_cert_requires_restart(&self) {
        self.inner.tls_cert_requires_restart.inc();
    }

    /// Register an additional (custom) collector into this instance's registry
    /// (R-4: the config reject/skip counters from core). A duplicate / invalid
    /// registration is ignored (logged via the returned error, if any).
    pub fn add_collector(&self, collector: Box<dyn Collector>) {
        if let Err(e) = self.inner.registry.register(collector) {
            tracing::warn!("registering config snapshot collector: {e}");
        }
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

    #[test]
    fn config_snapshot_collector_exposes_core_counters() {
        use hygress_core::prelude::{Destination, PathPred, RouteKind, RouteRule};
        // Bump the core counters through real store calls.
        let shared = Arc::new(hygress_core::SharedConfig::new(hygress_core::ConfigData::default()).unwrap());
        // Per-object skips (bad empty-key route dropped, good kept).
        let skipped = shared
            .store(hygress_core::ConfigData {
                routes: vec![
                    RouteRule {
                        key: String::new(),
                        ..RouteRule::new(
                            "m",
                            RouteKind::Main,
                            vec![PathPred::new("/")],
                            vec![Destination::new("a.static:80")],
                        )
                        .unwrap()
                    },
                    RouteRule::new(
                        "good",
                        RouteKind::Main,
                        vec![PathPred::new("/")],
                        vec![Destination::new("b.static:80")],
                    )
                    .unwrap(),
                ],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(skipped, 1);
        // Structural rejection bumps the reject counter.
        let structural = hygress_core::ConfigData {
            routes: vec![RouteRule::new(
                "bad",
                RouteKind::Main,
                vec![PathPred::new("([unclosed")],
                vec![Destination::new("a.static:80")],
            )
            .unwrap()],
            ..Default::default()
        };
        assert!(shared.store(structural).is_err());

        let m = Metrics::new();
        m.add_collector(Box::new(ConfigSnapshotCollector::new(shared.clone())));
        let out = m.encode();
        assert!(out.contains("hygress_config_reject_total"));
        assert!(out.contains("hygress_config_object_skipped_total"));
        // The prometheus text lines carry the values: NAME 1 / NAME 2 pattern
        // (unlabeled counters: "NAME 1" or "NAME 2").
        assert!(out.lines().any(|l| l.starts_with("hygress_config_reject_total") && l.ends_with('1')));
        assert!(out.lines().any(|l| l.starts_with("hygress_config_object_skipped_total") && l.ends_with('1')));
    }
}
