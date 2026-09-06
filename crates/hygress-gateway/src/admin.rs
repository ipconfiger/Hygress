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
    /// The control-plane snapshot holder for `GET /config` (R-4 / C4).
    /// `None` ⇒ `/config` reports 503 (not wired).
    pub shared: Option<Arc<hygress_core::SharedConfig>>,
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
            shared: None,
        }
    }

    /// Attach the control-plane snapshot holder for `GET /config` (R-4 / C4).
    pub fn with_config_shared(mut self, shared: Arc<hygress_core::SharedConfig>) -> Self {
        self.shared = Some(shared);
        self
    }

    /// Extract the `Authorization: Bearer <token>` value, if present.
    pub fn bearer_token(headers: &HeaderMap) -> Option<&str> {
        headers.get("authorization").and_then(|s| {
            s.strip_prefix("Bearer ")
                .or_else(|| s.strip_prefix("bearer "))
        })
    }

    /// Token gate: `true` when the request's bearer matches the configured
    /// token. Fail-closed when no token is configured.
    pub fn authorized(&self, headers: &HeaderMap) -> bool {
        let Some(token) = &self.admin_token else {
            return false;
        };
        let Some(provided) = Self::bearer_token(headers) else {
            return false;
        };
        // Compare in constant time (m5): a plain `Option<&str> ==` short-circuits on the first
        // differing byte, leaking how many leading bytes matched through response timing.
        Self::tokens_eq(provided.as_bytes(), token.as_bytes())
    }

    /// Constant-time equality over equal-length byte slices.
    ///
    /// Every byte of both inputs is read and folded into one accumulator, so a mismatch at any
    /// position is not distinguishable by timing. The length check is the only early exit — a
    /// bearer token's length is not secret, and comparing lengths first is the mainstream constant-
    /// time pattern (this crate has no `subtle` dependency; the manual fold is the whole compare).
    fn tokens_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        // XOR-fold every byte pair: any difference sets at least one bit of `acc`.
        a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
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
                            // ORA3-M2: a false reload is NEVER reported as a
                            // success. The loader logs the precise cause (missing
                            // file / malformed file); the message below stays
                            // honest for both LKG cases.
                            AdminResp::json(
                                500,
                                "reload_failed",
                                "reload failed; no new policy applied — keeping the last-known-good policy (the built-in all-pass default when none was ever loaded)",
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
            ("GET", "/config") => {
                // C4 (R-4): introspect the current control-plane snapshot
                // (redacted summary). Token-gated, fail-closed like /reload.
                if !self.authorized(headers) {
                    return AdminResp::json(401, "unauthorized", "missing or invalid admin token");
                }
                match &self.shared {
                    Some(shared) => AdminResp::new(
                        200,
                        "application/json; charset=utf-8",
                        config_summary_json(shared),
                    ),
                    None => {
                        AdminResp::json(503, "config_unavailable", "config introspection not wired")
                    }
                }
            }
            _ => AdminResp::json(404, "not_found", "unknown path"),
        }
    }
}

/// Redacted JSON summary of the current control-plane snapshot (GET /config).
///
/// **Never** includes: provider `api_tokens`, TLS `key_pem`/`cert_pem` payloads
/// (TLS hosts are shown as a sha256 fingerprint of the cert only), or the raw
/// WasmPlugin spec (which carried the derived `X-GPUStack-Auth-Token`).
fn config_summary_json(shared: &hygress_core::SharedConfig) -> String {
    use serde_json::json;
    let data = shared.load();
    let routes: Vec<serde_json::Value> = data
        .routes
        .iter()
        .map(|r| {
            let kind = match r.kind {
                hygress_core::RouteKind::Main => "main",
                hygress_core::RouteKind::Fallback => "fallback",
                hygress_core::RouteKind::Mirror => "mirror",
            };
            json!({
                "kind": kind,
                "key": r.key,
                "ingress_name": r.ingress_name,
                "path_predicates": r.path_predicates.len(),
                "destinations": r.destinations.len(),
                "auth": r.auth_scope.enabled,
                "fallback": r.fallback.as_ref().map(|f| f.target_key.as_str()),
            })
        })
        .collect();
    let registries: Vec<serde_json::Value> = data
        .registries
        .iter()
        .map(|reg| json!({ "id": reg.id, "domain": reg.domain, "port": reg.port }))
        .collect();
    let proxies: Vec<serde_json::Value> = data
        .proxies
        .iter()
        .map(|p| {
            json!({ "name": p.name, "server_address": p.server_address, "server_port": p.server_port })
        })
        .collect();
    let tls: Vec<serde_json::Value> = data
        .tls
        .hosts
        .iter()
        .map(|h| {
            // sha256 fingerprint of the cert PEM — the key/cert payloads are
            // never exposed.
            let fp = sha256_short(&h.cert_pem);
            json!({ "host": h.host, "is_default": h.is_default, "cert_sha256_prefix12": fp })
        })
        .collect();
    let features: Vec<serde_json::Value> = data
        .features
        .iter()
        .map(|f| {
            json!({
                "plugin": f.plugin,
                "phase": f.phase,
                "priority": f.priority,
                "fail_open": f.fail_open,
                "default_config_disable": f.default_config_disable,
            })
        })
        .collect();
    let provider_tokens: Vec<serde_json::Value> = data
        .provider_tokens
        .iter()
        .map(|t| json!({ "service": t.service, "ingress_scope": t.ingress_scope }))
        .collect();
    let body = json!({
        "routes": routes,
        "registries": registries,
        "proxies": proxies,
        "tls_hosts": tls,
        "features": features,
        "provider_tokens": provider_tokens,
        "timing": {
            "downstream_idle_timeout_secs": data.timing.downstream_idle_timeout_secs,
            "upstream_idle_timeout_secs": data.timing.upstream_idle_timeout_secs,
        },
        "model_router": {
            "prefix": data.model_router.prefix,
            "target_header": data.model_router.target_header,
            "alias_count": data.model_router.alias_name_mapping.len(),
            "suffix_count": data.model_router.enable_on_path_suffix.len(),
        },
        "snapshot_counters": {
            "reject_total": shared.snapshot_reject_total.load(std::sync::atomic::Ordering::Relaxed),
            "object_skipped_total": shared.snapshot_skipped_total.load(std::sync::atomic::Ordering::Relaxed),
        },
    });
    serde_json::to_string(&body).unwrap_or_else(|_| r#"{"error":"serialize"}"#.to_string())
}

/// First 12 hex chars of the sha256 of `bytes` (a short, non-reversible
/// content fingerprint for `GET /config` TLS redaction).
fn sha256_short(bytes: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes.as_bytes());
    hex::encode(hasher.finalize())[..12].to_string()
}

/// One admin response (status + content-type + body), produced by the pure
/// [`AdminState::route`].
#[derive(Clone, Debug)]
pub struct AdminResp {
    /// The HTTP status code of the response.
    pub status: u16,
    /// The `Content-Type` header value of the body.
    pub content_type: &'static str,
    /// The serialized response body (JSON for the structured endpoints).
    pub body: String,
}

impl AdminResp {
    /// Build a response from raw status / content-type / body.
    pub fn new(status: u16, content_type: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type,
            body: body.into(),
        }
    }
    /// Build a JSON error response: `{"reason": <reason>, "message": <message>}`
    /// with `application/json; charset=utf-8` content-type.
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
    /// Wrap the shared admin state (token + metrics + reload channels).
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
        // R-8 (P1 nitpick): frame the fixed-size response so the writer is not
        // close-delimited (keeps the admin connection reusable).
        if let Ok(cl) = http::header::HeaderValue::from_str(&out.body().len().to_string()) {
            out.headers_mut().insert(header::CONTENT_LENGTH, cl);
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

    // ----- (m5) constant-time token comparison semantics -----

    #[test]
    fn constant_time_token_eq_exact_semantics() {
        // Pins the compare helper: same length + same bytes → true; same length + any differing
        // byte → false; different length → false (length is not secret for an admin bearer, so
        // exiting early there is acceptable and matches mainstream constant-time compares).
        let s = state(Some("secret")); // configured token: 6 bytes
        let mut ok = h();
        ok.insert("authorization", "Bearer secret");
        assert!(s.authorized(&ok), "exact token must authorize");

        // Equal length (6), every byte wrong / one byte wrong → false.
        let mut all_wrong = h();
        all_wrong.insert("authorization", "Bearer xxxxxx");
        assert!(
            !s.authorized(&all_wrong),
            "equal-length wrong token must fail"
        );
        let mut one_wrong = h();
        one_wrong.insert("authorization", "Bearer secerX");
        assert!(!s.authorized(&one_wrong), "single-byte mismatch must fail");

        // Different lengths → false.
        let mut short = h();
        short.insert("authorization", "Bearer secre");
        assert!(!s.authorized(&short), "shorter token must fail");
        let mut long = h();
        long.insert("authorization", "Bearer secrets");
        assert!(!s.authorized(&long), "longer token must fail");

        // The helper itself (length-check + XOR fold) is unit-visible.
        assert!(AdminState::tokens_eq(b"secret", b"secret"));
        assert!(!AdminState::tokens_eq(b"secret", b"secrex"));
        assert!(!AdminState::tokens_eq(b"secret", b"secre"));
    }

    #[test]
    fn bearer_token_prefix_handling() {
        // `Bearer` / `bearer` prefixes are both accepted and the gate still compares exactly
        // (trailing whitespace is part of the value and must NOT match).
        let s = state(Some("tk"));
        let mut lower = h();
        lower.insert("authorization", "bearer tk");
        assert!(s.authorized(&lower), "lowercase bearer prefix accepted");

        let mut trailing = h();
        trailing.insert("authorization", "Bearer tk ");
        assert!(
            !s.authorized(&trailing),
            "trailing whitespace must not match (exact token compare)"
        );
    }

    #[test]
    fn reload_invokes_reloader() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c2 = counter.clone();
        let reloader: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || {
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
        // ORA3-M2: the message is explicit that NOTHING was applied.
        assert!(r.body.contains("no new policy applied"));
    }

    /// ORA3-M2: a real `POST /reload` through the policy handle — when the file
    /// disappears after a successful load, admin reports 500 and the real
    /// policy survives (no silent downgrade to the all-pass default).
    #[test]
    fn reload_missing_policy_file_reports_500_and_keeps_lkg() {
        use crate::policy_loader::PolicyHandle;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static N: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "hygress-admin-reload-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("policy.yaml");
        std::fs::write(&path, "version: 1\n").unwrap();

        let handle = Arc::new(PolicyHandle::new(path.to_string_lossy().into_owned()));
        // Load a real policy (one route), then delete the file.
        std::thread::sleep(std::time::Duration::from_millis(15)); // distinct mtime
        std::fs::write(
            &path,
            "version: 1\nroutes:\n  - name_glob: \"ai-route-route-*\"\n    limits:\n      consumer: { rps: 5, burst: 10 }\n",
        )
        .unwrap();
        assert!(handle.reload());
        assert_eq!(handle.shared().routes.len(), 1);

        let h2 = handle.clone();
        let reloader: Arc<dyn Fn() -> bool + Send + Sync> = Arc::new(move || h2.reload());
        let s = AdminState::new(
            Arc::new(Metrics::new()),
            Some("secret".to_string()),
            Some(reloader),
        );
        let mut ok = h();
        ok.insert("authorization", "Bearer secret");

        std::fs::remove_file(&path).unwrap();
        let r = s.route("POST", "/reload", &ok);
        assert_eq!(r.status, 500, "a missing policy file must never report 200");
        assert!(r.body.contains("no new policy applied"));
        // The loaded policy (1 route) survived the failed reload.
        assert_eq!(handle.shared().routes.len(), 1, "LKG must be kept");
        std::fs::remove_dir_all(&dir).ok();
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

    // ----- C4 / R-4: GET /config (token-gated, redacted) -----

    fn state_with_shared(token: Option<&str>) -> AdminState {
        use hygress_core::prelude::{ProviderToken, TlsConfig, TlsHost};
        let data = hygress_core::ConfigData {
            provider_tokens: vec![ProviderToken {
                service: "provider-1.proxy".into(),
                ingress_scope: None,
                api_tokens: vec!["sk-TOP-SECRET".into()],
            }],
            tls: TlsConfig {
                hosts: vec![TlsHost {
                    host: "api.example.com".into(),
                    is_default: true,
                    cert_pem: "-----BEGIN CERTIFICATE-----\nSECRETCERT\n-----END CERTIFICATE-----"
                        .into(),
                    key_pem: "-----BEGIN PRIVATE KEY-----\nSECRETKEY\n-----END PRIVATE KEY-----"
                        .into(),
                }],
            },
            ..Default::default()
        };
        let shared = hygress_core::SharedConfig::new(data).unwrap();
        AdminState::new(Arc::new(Metrics::new()), token.map(str::to_string), None)
            .with_config_shared(Arc::new(shared))
    }

    #[test]
    fn config_is_token_gated() {
        let s = state_with_shared(None);
        assert_eq!(s.route("GET", "/config", &h()).status, 401);
        let s2 = state_with_shared(Some("tk"));
        let mut ok = h();
        ok.insert("authorization", "Bearer tk");
        let r = s2.route("GET", "/config", &ok);
        assert_eq!(r.status, 200);
        assert!(r.content_type.contains("application/json"));
        assert!(r.body.contains("\"routes\""));
        assert!(r.body.contains("\"tls_hosts\""));
    }

    #[test]
    fn config_redacts_secrets() {
        let s = state_with_shared(Some("tk"));
        let mut ok = h();
        ok.insert("authorization", "Bearer tk");
        let body = s.route("GET", "/config", &ok).body;
        // Secrets must not leak into the dump.
        assert!(!body.contains("sk-TOP-SECRET"));
        assert!(!body.contains("SECRETKEY"));
        assert!(!body.contains("SECRETCERT"));
        assert!(!body.contains("-----BEGIN"));
        // The TLS fingerprint field IS present (sha256 short).
        assert!(body.contains("cert_sha256_prefix12"));
        assert!(body.contains("api.example.com"));
    }

    #[test]
    fn config_unwired_reports_503() {
        let s = state(Some("tk"));
        let mut ok = h();
        ok.insert("authorization", "Bearer tk");
        assert_eq!(s.route("GET", "/config", &ok).status, 503);
    }
}
