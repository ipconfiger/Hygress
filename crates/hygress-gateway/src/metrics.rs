//! Prometheus metrics (design §10). A single [`Metrics`] holds one
//! [`Registry`]; the admin `/metrics` (and the 15020 shallow-compat `/stats/
//! prometheus`) scrape it. Counters/histograms are real — no stubs.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use prometheus::core::{Collector, Desc};
use prometheus::proto;
use prometheus::{HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry, TextEncoder};

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
const REJECT_HELP: &str =
    "Control-plane snapshots rejected as a whole (structural, keep-last-known-good).";
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

/// The `kind` label dictionary shared by `hygress_requests_total` and
/// `hygress_request_duration_seconds` (AM-5):
/// - `model_route` — a request whose terminal came from a complete upstream
///   dispatch over a model route (2xx stream end, forwarded final non-2xx,
///   total transport failure, guardrail-cut stream);
/// - `mirror` — the same, for a mirror / passthrough route;
/// - [`KIND_SHORT_CIRCUIT`] — a terminal 4xx/5xx the gateway itself generated
///   **before** a complete upstream dispatch (rate-limit 429, auth 401 /
///   fail-closed 403, quota 429, guardrail 403, no-route 404, registry 503,
///   body 413 / read-abort 400). Previously these short-circuit paths only
///   bumped their dedicated counters and were absent from the request-level
///   totals.
pub(crate) const KIND_SHORT_CIRCUIT: &str = "short_circuit";

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
    fallback_exhausted_total: IntCounter,
    usage_push_dropped_total: IntCounter,
    /// Rows handed to the usage sink for delivery, split by whether the
    /// upstream reported a canonical usage object (G2); the drop counter above
    /// subtracts rows that never reached GPUStack.
    usage_pushed_total: IntCounterVec,
    // ORA3-MAJ-1: control-plane health — watcher errors by kind/class, new
    // snapshot stores, and the last-store staleness gauge (the round's only
    // MAJOR: control-plane death / permanent degradation was otherwise a black
    // box behind rate-limited logs).
    control_watch_error_total: IntCounterVec,
    control_snapshot_store_total: IntCounter,
    control_last_store_timestamp_seconds: IntGauge,
    /// O3: liveness heartbeat — stamped after EVERY successful reconcile pass
    /// (including fingerprint no-op rounds), unlike the content-change store
    /// gauge above. `time() - this > ~3×poll_interval` means the controller
    /// loop is stuck or dead, independent of whether anything changed.
    control_last_sync_timestamp_seconds: IntGauge,
    /// O4: control-plane reconcile failure episodes by class (`list` /
    /// `rejected`). Counts episodes (the adapter's warn-once latch fires the
    /// hook once per outage), not ~1s-tick repeats.
    control_reconcile_error_total: IntCounterVec,
    /// O9: static build provenance gauge (`hygress_build_info{version} = 1`).
    /// Registered once at construction (no per-event record — see `Metrics::new`).
    /// O5: policy reload attempts by outcome (admin `/reload` + the 30s mtime
    /// poll both flow through `PolicyHandle::reload_from`'s observer).
    policy_reload_total: IntCounterVec,
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
            prometheus::Opts::new(
                "hygress_requests_total",
                "Requests by status and kind. Kind: model_route/mirror (complete upstream dispatch) or short_circuit (gateway-generated terminal 4xx/5xx before a complete upstream dispatch).",
            ),
            &["status", "kind"],
        )
        .expect("requests_total");
        let request_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "hygress_request_duration_seconds",
                "End-to-end request latency. Kind: model_route/mirror/short_circuit (see hygress_requests_total).",
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
            prometheus::HistogramOpts::new("hygress_ttft_seconds", "Time to first response chunk.")
                .buckets(vec![
                    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
                ]),
            &["kind"],
        )
        .expect("ttft");
        let retries_total = IntCounter::new(
            "hygress_retries_total",
            "Failover retries across candidates.",
        )
        .expect("retries_total");
        let upstream_errors_total = IntCounter::new(
            "hygress_upstream_errors_total",
            "Upstream attempt failures.",
        )
        .expect("upstream_errors");
        let fallback_total = IntCounter::new("hygress_fallback_total", "Fallback redirects taken.")
            .expect("fallback_total");
        let fallback_exhausted_total = IntCounter::new(
            "hygress_fallback_exhausted_total",
            "Fallback chains that terminated without a successful hop (budget exhausted or chain end without dispatch).",
        )
        .expect("fallback_exhausted_total");
        let usage_push_dropped_total = IntCounter::new(
            "hygress_usage_push_dropped_total",
            "Usage rows dropped without reaching the usage sink (queue-full / sink task gone / final push failure).",
        )
        .expect("usage_push_dropped_total");
        let usage_pushed_total = IntCounterVec::new(
            prometheus::Opts::new(
                "hygress_usage_pushed_total",
                "Usage rows handed to the GPUStack usage sink, by whether the upstream reported a canonical usage object (completed=\"true\") or the row relies on the GPUStack server's byte/chunk estimation (completed=\"false\"). Rows that never reach GPUStack are counted separately by hygress_usage_push_dropped_total.",
            ),
            &["completed"],
        )
        .expect("usage_pushed_total");
        // ORA3-MAJ-1: control-plane observability families (see `Inner`).
        let control_watch_error_total = IntCounterVec::new(
            prometheus::Opts::new(
                "hygress_control_watch_error_total",
                "Control-plane watcher errors by kind and class (permanent: watch unsupported by the apiserver, convergence degraded to the tick; transient: recoverable watch failure).",
            ),
            &["kind", "class"],
        )
        .expect("control_watch_error_total");
        let control_snapshot_store_total = IntCounter::new(
            "hygress_control_snapshot_store_total",
            "Control-plane snapshots successfully stored (new content).",
        )
        .expect("control_snapshot_store_total");
        let control_last_store_timestamp_seconds = IntGauge::new(
            "hygress_control_last_store_timestamp_seconds",
            "Unix time (seconds) of the last successful control-plane snapshot store; 0 before the first store.",
        )
        .expect("control_last_store_timestamp_seconds");
        let control_last_sync_timestamp_seconds = IntGauge::new(
            "hygress_control_last_sync_timestamp_seconds",
            "Unix time (seconds) of the last successful control-plane reconcile pass (any outcome, including no-op fingerprint rounds); 0 before the first pass. Distinguishes a stalled/dead controller from a quiet healthy cluster — hygress_control_last_store_timestamp_seconds only advances on content changes.",
        )
        .expect("control_last_sync_timestamp_seconds");
        let control_reconcile_error_total = IntCounterVec::new(
            prometheus::Opts::new(
                "hygress_control_reconcile_error_total",
                "Control-plane reconcile failure episodes by class (list: snapshot LIST/transport failure; rejected: structurally rejected snapshot). Counts episodes (the adapter's warn-once latch fires once per outage), not per-tick repeats.",
            ),
            &["class"],
        )
        .expect("control_reconcile_error_total");
        // O9: static build provenance — always `1` for the compiled version.
        let build_info = IntGaugeVec::new(
            prometheus::Opts::new(
                "hygress_build_info",
                "Static build provenance: 1 for the compiled gateway version.",
            ),
            &["version"],
        )
        .expect("build_info");
        build_info
            .with_label_values(&[env!("CARGO_PKG_VERSION")])
            .set(1);
        // O5: policy reload outcome (success = swapped; failure = last-known-good kept).
        let policy_reload_total = IntCounterVec::new(
            prometheus::Opts::new(
                "hygress_policy_reload_total",
                "Policy reload attempts by outcome — success (new policy swapped) or failure (last-known-good kept). Covers the admin POST /reload and the 30s mtime-poll reload; unchanged no-op ticks are not counted.",
            ),
            &["result"],
        )
        .expect("policy_reload_total");
        let auth_decisions = IntCounterVec::new(
            prometheus::Opts::new("hygress_auth_decisions_total", "Auth decisions."),
            &["result"],
        )
        .expect("auth_decisions");
        let active_requests = IntGauge::new("hygress_active_requests", "In-flight requests.")
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
            Box::new(fallback_exhausted_total.clone()),
            Box::new(usage_push_dropped_total.clone()),
            Box::new(usage_pushed_total.clone()),
            Box::new(control_watch_error_total.clone()),
            Box::new(control_snapshot_store_total.clone()),
            Box::new(control_last_store_timestamp_seconds.clone()),
            Box::new(control_last_sync_timestamp_seconds.clone()),
            Box::new(control_reconcile_error_total.clone()),
            Box::new(build_info.clone()),
            Box::new(policy_reload_total.clone()),
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
                fallback_exhausted_total,
                usage_push_dropped_total,
                usage_pushed_total,
                control_watch_error_total,
                control_snapshot_store_total,
                control_last_store_timestamp_seconds,
                control_last_sync_timestamp_seconds,
                control_reconcile_error_total,
                policy_reload_total,
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

    /// AM-5: account a **gateway-generated terminal short-circuit** — a 4xx/5xx
    /// the gateway writes before a complete upstream dispatch (rate-limit 429,
    /// auth 401 / fail-closed 403, quota 429, guardrail 403, no-route 404,
    /// registry 503, body 413 / read-abort 400) — under the fixed
    /// `KIND_SHORT_CIRCUIT` kind. The `requests_total` count and the
    /// `request_duration` latency are recorded together so **every** written
    /// downstream terminal keeps the request-level totals complete (the
    /// dedicated counters — `auth_decisions` / `rate_limit_denied` /
    /// `quota_denied` / `guardrail_blocked` — are kept for the classification
    /// dimension; they are not request-level totals).
    pub fn record_short_circuit(&self, status: u16, secs: f64) {
        self.record_request(status, KIND_SHORT_CIRCUIT);
        self.record_request_duration(KIND_SHORT_CIRCUIT, secs);
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

    /// ORA3-M3: a fallback chain ended without a successful hop (10-hop budget
    /// exhausted, or the chain terminated with no dispatch). Distinct from
    /// [`Metrics::record_fallback`], which counts *armed* redirect hops.
    pub fn record_fallback_exhausted(&self) {
        self.inner.fallback_exhausted_total.inc();
    }

    /// ORA3-M4: a usage row was dropped before the usage sink accepted it
    /// (bounded queue full / sink task gone / final push failure). The egress
    /// sink invokes this through the `on_drop` callback wired in bootstrap.
    pub fn record_usage_push_dropped(&self) {
        self.inner.usage_push_dropped_total.inc();
    }

    /// G2: a usage row was handed to the sink for delivery. `completed` mirrors
    /// the row's `completed` flag — `true` = the upstream reported a canonical
    /// usage object (exact metering), `false` = the row carries zero tokens and
    /// the GPUStack server applies its byte/chunk estimation fallback. Rows
    /// that never reach GPUStack are subtracted by
    /// [`Metrics::record_usage_push_dropped`].
    pub fn record_usage_pushed(&self, completed: bool) {
        self.inner
            .usage_pushed_total
            .with_label_values(&[if completed { "true" } else { "false" }])
            .inc();
    }

    /// ORA3-MAJ-1: a control-plane watcher error was classified. `kind` is the
    /// watched resource kind (configmap / ingress / secret / …); `class` is
    /// `permanent` (watch unsupported by the apiserver → convergence degraded
    /// to the poll tick / POLL_INTERVAL) or `transient` (recoverable watch
    /// failure).
    pub fn record_control_watch_error(&self, kind: &str, class: &str) {
        self.inner
            .control_watch_error_total
            .with_label_values(&[kind, class])
            .inc();
    }

    /// ORA3-MAJ-1: a new control-plane snapshot was successfully stored.
    pub fn record_control_snapshot_store(&self) {
        self.inner.control_snapshot_store_total.inc();
    }

    /// ORA3-MAJ-1: stamp the last-store staleness gauge with the current wall
    /// clock (unix seconds). Kept separate from the store counter so bootstrap
    /// wires both from the single adapter `on_snapshot_store` hook.
    pub fn record_control_last_store_timestamp(&self) {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.inner.control_last_store_timestamp_seconds.set(secs);
    }

    /// O3: stamp the control-plane liveness heartbeat (unix seconds). Wired to
    /// the adapter `on_sync_ok` hook — fired after EVERY successful reconcile
    /// pass, including fingerprint no-op rounds — so a quiet healthy cluster
    /// keeps this gauge fresh (unlike the content-change store gauge above).
    pub fn record_control_sync(&self) {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.inner.control_last_sync_timestamp_seconds.set(secs);
    }

    /// O4: a control-plane reconcile failure episode. `class` is `list`
    /// (snapshot LIST/transport failure) or `rejected` (structurally rejected
    /// snapshot); the adapter fires once per outage episode (warn-once latch).
    pub fn record_control_reconcile_error(&self, class: &str) {
        self.inner
            .control_reconcile_error_total
            .with_label_values(&[class])
            .inc();
    }

    /// O5: a policy reload attempt finished. `ok` = the swap happened (new
    /// policy live) vs `false` = last-known-good kept (missing/malformed file).
    pub fn record_policy_reload(&self, ok: bool) {
        self.inner
            .policy_reload_total
            .with_label_values(&[if ok { "success" } else { "failure" }])
            .inc();
    }

    pub fn record_auth(&self, result: &str) {
        self.inner.auth_decisions.with_label_values(&[result]).inc();
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

    /// AM-5: `record_short_circuit` must land the request-level count AND the
    /// duration under the fixed `short_circuit` kind, for every short-circuit
    /// status — the accounting the pipe's short-circuit exits converge on.
    #[test]
    fn record_short_circuit_uses_the_fixed_kind() {
        let m = Metrics::new();
        m.record_short_circuit(429, 0.125);
        m.record_short_circuit(403, 0.5);
        m.record_short_circuit(413, 0.0625);
        let out = m.encode();
        // Counts: one `hygress_requests_total` per (status, kind) pair, under
        // kind="short_circuit" (never under a reason-slug kind).
        for status in ["429", "403", "413"] {
            let present = out.lines().any(|l| {
                l.starts_with("hygress_requests_total{")
                    && l.contains(&format!("status=\"{status}\""))
                    && l.contains("kind=\"short_circuit\"")
                    && l.ends_with(" 1")
            });
            assert!(
                present,
                "requests_total for status {status} kind short_circuit missing:\n{out}"
            );
        }
        // The duration histogram carries the same fixed kind label.
        assert!(
            out.lines()
                .any(|l| l
                    .starts_with("hygress_request_duration_seconds_bucket{kind=\"short_circuit\"")),
            "no short_circuit duration buckets in:\n{out}"
        );
    }

    #[test]
    fn config_snapshot_collector_exposes_core_counters() {
        use hygress_core::prelude::{Destination, PathPred, RouteKind, RouteRule};
        // Bump the core counters through real store calls.
        let shared =
            Arc::new(hygress_core::SharedConfig::new(hygress_core::ConfigData::default()).unwrap());
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
        assert!(out
            .lines()
            .any(|l| l.starts_with("hygress_config_reject_total") && l.ends_with('1')));
        assert!(out
            .lines()
            .any(|l| l.starts_with("hygress_config_object_skipped_total") && l.ends_with('1')));
    }

    /// ORA3-MAJ-1: the control-plane health families record per (kind, class)
    /// watcher errors, count new snapshot stores, and stamp the last-store
    /// staleness gauge — all on `/metrics` like the other real counters.
    #[test]
    fn control_plane_metrics_record_and_render() {
        let m = Metrics::new();
        // Two permanent configmap watcher errors + one transient ingress error.
        m.record_control_watch_error("configmap", "permanent");
        m.record_control_watch_error("configmap", "permanent");
        m.record_control_watch_error("ingress", "transient");
        // Two new-snapshot stores; the last one stamps the staleness gauge.
        m.record_control_snapshot_store();
        m.record_control_snapshot_store();
        m.record_control_last_store_timestamp();

        let out = m.encode();
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_control_watch_error_total{")
                    && l.contains("kind=\"configmap\"")
                    && l.contains("class=\"permanent\"")
                    && l.ends_with(" 2")
            }),
            "permanent configmap watch errors (x2) missing:\n{out}"
        );
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_control_watch_error_total{")
                    && l.contains("kind=\"ingress\"")
                    && l.contains("class=\"transient\"")
                    && l.ends_with(" 1")
            }),
            "transient ingress watch error missing:\n{out}"
        );
        assert!(
            out.lines()
                .any(|l| l.starts_with("hygress_control_snapshot_store_total") && l.ends_with(" 2")),
            "snapshot store counter (x2) missing:\n{out}"
        );
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_control_last_store_timestamp_seconds")
                    && l.split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse::<f64>().ok())
                        .map(|v| v > 0.0)
                        .unwrap_or(false)
            }),
            "last-store timestamp gauge must be set to a positive unix time:\n{out}"
        );
    }

    /// G2/O3/O4/O9: the metering-quality split, the control-plane liveness
    /// heartbeat + reconcile-failure counters and the build-info gauge all
    /// render on `/metrics`.
    #[test]
    fn usage_pushed_and_control_liveness_families_render() {
        let m = Metrics::new();
        m.record_usage_pushed(true);
        m.record_usage_pushed(false);
        m.record_usage_pushed(true);
        m.record_control_reconcile_error("list");
        m.record_control_sync();
        m.record_policy_reload(true);
        m.record_policy_reload(false);
        let out = m.encode();
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_usage_pushed_total{")
                    && l.contains("completed=\"true\"")
                    && l.ends_with(" 2")
            }),
            "completed=true pushed rows (x2) missing:\n{out}"
        );
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_usage_pushed_total{")
                    && l.contains("completed=\"false\"")
                    && l.ends_with(" 1")
            }),
            "completed=false pushed row missing:\n{out}"
        );
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_control_reconcile_error_total{")
                    && l.contains("class=\"list\"")
                    && l.ends_with(" 1")
            }),
            "reconcile list-failure episode missing:\n{out}"
        );
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_control_last_sync_timestamp_seconds")
                    && l.split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse::<f64>().ok())
                        .map(|v| v > 0.0)
                        .unwrap_or(false)
            }),
            "last-sync heartbeat must be set to a positive unix time:\n{out}"
        );
        assert!(
            out.lines()
                .any(|l| l.starts_with("hygress_build_info{") && l.ends_with(" 1")),
            "build-info gauge missing:\n{out}"
        );
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_policy_reload_total{")
                    && l.contains("result=\"success\"")
                    && l.ends_with(" 1")
            }),
            "policy reload success missing:\n{out}"
        );
        assert!(
            out.lines().any(|l| {
                l.starts_with("hygress_policy_reload_total{")
                    && l.contains("result=\"failure\"")
                    && l.ends_with(" 1")
            }),
            "policy reload failure missing:\n{out}"
        );
    }
}
