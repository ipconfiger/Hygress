//! Gateway process configuration — env parsing (design §11 / §9).
//!
//! Pure: [`GatewayConfig::parse`] takes a key→value lookup closure so tests can
//! drive it with a fixed map (no process-env mutation). [`GatewayConfig::from_env`]
//! binds it to `std::env::var`.
//!
//! Ports (design §11.1 port discipline): the data plane binds
//! `GATEWAY_HTTP_PORT`/`GATEWAY_TLS_PORT`; 15020 is the pilot-agent metrics
//! shallow-compat endpoint; admin is `HYGRESS_ADMIN_ADDR` (default 8081). Hygress
//! NEVER binds 9876 / 15010 / 15012 / 8888 / 15051.

use std::time::Duration;

/// Parsed gateway configuration.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Data-plane HTTP port (`GATEWAY_HTTP_PORT`, default 80).
    pub http_port: u16,
    /// Data-plane HTTPS / TLS port (`GATEWAY_TLS_PORT`, default 443).
    pub tls_port: u16,
    /// GPUStack server `api_port` the process waits on for readiness
    /// (`GPUSTACK_API_PORT`, default 80). The data plane binds only after this
    /// is ready (design §11.2).
    pub gpustack_api_port: u16,
    /// Admin listener address (`HYGRESS_ADMIN_ADDR`, default `127.0.0.1:8081`).
    pub admin_addr: String,
    /// Admin bearer token (`HYGRESS_ADMIN_TOKEN`). `None` ⇒ `/reload` &
    /// `/stats/usage` deny (fail-closed); `/healthz` & `/metrics` stay open.
    pub admin_token: Option<String>,
    /// GPUStack `jwt_secret_key` (`GPUSTACK_JWT_SECRET_KEY`). `None` ⇒ the egress
    /// sink falls back to `{data_dir}/jwt_secret_key`, then fail-fast (design §9).
    pub jwt_secret_key: Option<String>,
    /// GPUStack data dir (`GPUSTACK_DATA_DIR`, default empty).
    pub data_dir: String,
    /// Ingress class / gateway namespace (`GATEWAY_NAMESPACE`, default
    /// `higress-system`).
    pub gateway_namespace: String,
    /// Control-plane poll interval (`POLL_INTERVAL`, default 1s).
    pub poll_interval: Duration,
    /// Pilot-agent metrics shallow-compat port (`GATEWAY_PILOT_AGENT_METRICS_PORT`,
    /// default 15020).
    pub pilot_agent_metrics_port: u16,
    /// Topology-B flag (`HYGRESS_TOPOLOGY_B`). When set, bootstrap seeds the
    /// `higress` IngressClass (GPUStack never creates it; external `is_supported_higress`
    /// probes it by name — design §5 / §13 topology-B).
    pub topology_b: bool,
    /// Max wait for the GPUStack API (`GPUSTACK_API_PORT`) readiness probe
    /// (`HYGRESS_API_READY_TIMEOUT`, seconds or `300s`/`5000ms`, default 30s).
    /// Fail-fast after this — aligned with design §11.2. Operators in a slow-starting
    /// container (GPUStack re-boots + runs the gateway init) should widen it.
    pub api_ready_timeout: Duration,
    /// Max wait for the adapter's FIRST snapshot before binding the data plane
    /// (`HYGRESS_SNAPSHOT_TIMEOUT`, seconds or `300s`/`5000ms`, default 60s).
    pub snapshot_timeout: Duration,
    /// Policy file path (design §2.1 / D-7; `HYGRESS_POLICY_PATH`, default
    /// `/etc/hygress/policy.yaml`). A missing file is the all-pass default; a
    /// malformed file keeps the last-known-good (warn).
    pub policy_path: String,
    /// Token-quota estimate divisor (design §4.2 / D-13; `HYGRESS_QUOTA_K`,
    /// default 4): `est = ceil(request_content_bytes / K)`. Clamped to ≥ 1.
    pub quota_k: u64,
    /// The LLM guardrail verdict service URL (design §4.4 B4b / D-14;
    /// `HYGRESS_GUARDRAIL_URL`). `None` ⇒ the LLM guardrail is not configured
    /// (pass-through; `fail_mode` never applies — D-14).
    pub guardrail_url: Option<String>,
    /// ext-auth failure mode (R-12): when `/token-auth` is unreachable or
    /// answers 5xx, reject (403) when `true` (default — matches the
    /// GPUStack/Higress `failure_mode_allow=false` baseline) or fail-open when
    /// `false`. Env: `HYGRESS_EXT_AUTH_FAIL_MODE` = `closed` (default) |
    /// `open`.
    pub ext_auth_fail_closed: bool,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            http_port: 80,
            tls_port: 443,
            gpustack_api_port: 80,
            admin_addr: "127.0.0.1:8081".to_string(),
            admin_token: None,
            jwt_secret_key: None,
            data_dir: String::new(),
            gateway_namespace: "higress-system".to_string(),
            poll_interval: Duration::from_secs(1),
            pilot_agent_metrics_port: 15020,
            topology_b: false,
            api_ready_timeout: Duration::from_secs(30),
            snapshot_timeout: Duration::from_secs(60),
            policy_path: "/etc/hygress/policy.yaml".to_string(),
            quota_k: 4,
            guardrail_url: None,
            ext_auth_fail_closed: true,
        }
    }
}

impl GatewayConfig {
    /// Read from the process environment.
    pub fn from_env() -> Self {
        Self::parse(|k| std::env::var(k).ok())
    }

    /// Parse from an arbitrary key lookup (pure; testable).
    ///
    /// Every numeric / duration / bool / enum env that fails to parse logs a
    /// warning naming the key and the attempted value and keeps its documented
    /// default (ORA3-M1: no silent env fallback).
    pub fn parse(get: impl Fn(&str) -> Option<String>) -> Self {
        let mut c = Self::default();

        if let Some(v) = clean(get("GATEWAY_HTTP_PORT")) {
            c.http_port = match v.parse::<u16>() {
                Ok(p) => p,
                Err(_) => warn_unparsable("GATEWAY_HTTP_PORT", &v, c.http_port),
            };
        }
        if let Some(v) = clean(get("GATEWAY_TLS_PORT")) {
            c.tls_port = match v.parse::<u16>() {
                Ok(p) => p,
                Err(_) => warn_unparsable("GATEWAY_TLS_PORT", &v, c.tls_port),
            };
        }
        if let Some(v) = clean(get("GPUSTACK_API_PORT")) {
            c.gpustack_api_port = match v.parse::<u16>() {
                Ok(p) => p,
                Err(_) => warn_unparsable("GPUSTACK_API_PORT", &v, c.gpustack_api_port),
            };
        }
        if let Some(v) = clean(get("HYGRESS_ADMIN_ADDR")) {
            c.admin_addr = v;
        }
        if let Some(v) = clean(get("HYGRESS_ADMIN_TOKEN")) {
            c.admin_token = Some(v);
        }
        if let Some(v) = clean(get("GPUSTACK_JWT_SECRET_KEY")) {
            c.jwt_secret_key = Some(v);
        }
        if let Some(v) = clean(get("GPUSTACK_DATA_DIR")) {
            c.data_dir = v;
        }
        if let Some(v) = clean(get("GATEWAY_NAMESPACE")) {
            c.gateway_namespace = v;
        }
        if let Some(v) = clean(get("POLL_INTERVAL")) {
            c.poll_interval = parse_duration_env("POLL_INTERVAL", &v, c.poll_interval);
        }
        if let Some(v) = clean(get("GATEWAY_PILOT_AGENT_METRICS_PORT")) {
            c.pilot_agent_metrics_port = match v.parse::<u16>() {
                Ok(p) => p,
                Err(_) => warn_unparsable(
                    "GATEWAY_PILOT_AGENT_METRICS_PORT",
                    &v,
                    c.pilot_agent_metrics_port,
                ),
            };
        }
        if let Some(v) = clean(get("HYGRESS_TOPOLOGY_B")) {
            c.topology_b = parse_bool_env("HYGRESS_TOPOLOGY_B", &v);
        }
        if let Some(v) = clean(get("HYGRESS_API_READY_TIMEOUT")) {
            c.api_ready_timeout =
                parse_duration_env("HYGRESS_API_READY_TIMEOUT", &v, c.api_ready_timeout);
        }
        if let Some(v) = clean(get("HYGRESS_SNAPSHOT_TIMEOUT")) {
            c.snapshot_timeout =
                parse_duration_env("HYGRESS_SNAPSHOT_TIMEOUT", &v, c.snapshot_timeout);
        }
        if let Some(v) = clean(get("HYGRESS_POLICY_PATH")) {
            c.policy_path = v;
        }
        if let Some(v) = clean(get("HYGRESS_QUOTA_K")) {
            c.quota_k = match v.parse::<u64>() {
                Ok(k) => k.max(1),
                Err(_) => warn_unparsable("HYGRESS_QUOTA_K", &v, c.quota_k),
            };
        }
        if let Some(v) = clean(get("HYGRESS_GUARDRAIL_URL")) {
            c.guardrail_url = Some(v);
        }
        if let Some(v) = clean(get("HYGRESS_EXT_AUTH_FAIL_MODE")) {
            // `open` → fail-open; `closed` → fail-closed (the safe default
            // matching the GPUStack/Higress baseline). Anything else is a typo:
            // warn (ORA3-M1) and stay fail-closed — never silent, never
            // misinterpreted as `open`.
            match v.to_ascii_lowercase().as_str() {
                "open" => c.ext_auth_fail_closed = false,
                "closed" => c.ext_auth_fail_closed = true,
                _ => {
                    warn_unparsable("HYGRESS_EXT_AUTH_FAIL_MODE", &v, c.ext_auth_fail_closed);
                }
            }
        }
        c
    }

    /// The data-plane HTTP bind address (`0.0.0.0:<http_port>`).
    pub fn http_bind(&self) -> String {
        format!("0.0.0.0:{}", self.http_port)
    }
}

/// Trim + treat blank as absent.
fn clean(v: Option<String>) -> Option<String> {
    let v = v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())?;
    Some(v)
}

/// ORA3-M1: an env value failed to parse — log a warning naming the key, the
/// attempted value and the applied default, then return the default. This is
/// the single warn channel for every silent-env-fallback fix in [`parse`].
fn warn_unparsable<T: std::fmt::Debug>(key: &str, value: &str, default: T) -> T {
    tracing::warn!(
        "{}; using default {:?}",
        unparsable_env_msg(key, value),
        default
    );
    default
}

/// The ORA3-M1 warning prefix naming the env key and the attempted value
/// (pure, so the message contract is unit-testable without a tracing
/// subscriber).
fn unparsable_env_msg(key: &str, value: &str) -> String {
    format!("env {key}={value:?} unparsable")
}

/// Accept `true`/`1`/`yes`/`on` as `true` and `false`/`0`/`no`/`off` as
/// `false` (case-insensitive). Any other value is malformed: warn (ORA3-M1)
/// and fall back to `false` (the field default) — never a silent misinterpret
/// of a typo like `HYGRESS_TOPOLOGY_B=treu`.
fn parse_bool_env(key: &str, v: &str) -> bool {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => warn_unparsable(key, v, false),
    }
}

/// Accept a bare integer (seconds) or a `1s` / `500ms` duration string for the
/// env `key`. A malformed value logs a warning (ORA3-M1) and falls back to
/// `default` — that key's documented default — instead of the old silent 1s.
fn parse_duration_env(key: &str, v: &str, default: Duration) -> Duration {
    let s = v.trim().to_ascii_lowercase();
    if let Some(ms) = s.strip_suffix("ms") {
        return match ms.parse::<u64>() {
            Ok(n) => Duration::from_millis(n),
            Err(_) => warn_unparsable(key, v, default),
        };
    }
    if let Some(secs) = s.strip_suffix('s') {
        return match secs.parse::<u64>() {
            Ok(n) => Duration::from_secs(n),
            Err(_) => warn_unparsable(key, v, default),
        };
    }
    match s.parse::<u64>() {
        Ok(n) => Duration::from_secs(n),
        Err(_) => warn_unparsable(key, v, default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn parse(map: &[(&str, &str)]) -> GatewayConfig {
        let m: HashMap<&str, &str> = map.iter().cloned().collect();
        GatewayConfig::parse(|k| m.get(k).copied().map(String::from))
    }

    #[test]
    fn defaults() {
        let c = parse(&[]);
        assert_eq!(c.http_port, 80);
        assert_eq!(c.tls_port, 443);
        assert_eq!(c.gpustack_api_port, 80);
        assert_eq!(c.admin_addr, "127.0.0.1:8081");
        assert_eq!(c.admin_token, None);
        assert_eq!(c.gateway_namespace, "higress-system");
        assert_eq!(c.poll_interval, Duration::from_secs(1));
        assert_eq!(c.pilot_agent_metrics_port, 15020);
        assert_eq!(c.api_ready_timeout, Duration::from_secs(30));
        assert_eq!(c.snapshot_timeout, Duration::from_secs(60));
        assert_eq!(c.policy_path, "/etc/hygress/policy.yaml");
        assert_eq!(c.quota_k, 4);
        assert_eq!(c.guardrail_url, None);
    }

    #[test]
    fn overrides() {
        let c = parse(&[
            ("GATEWAY_HTTP_PORT", "9000"),
            ("GATEWAY_TLS_PORT", "9443"),
            ("GPUSTACK_API_PORT", "8080"),
            ("HYGRESS_ADMIN_ADDR", "127.0.0.1:9081"),
            ("HYGRESS_ADMIN_TOKEN", "secret"),
            ("GPUSTACK_JWT_SECRET_KEY", "jwt-secret"),
            ("GPUSTACK_DATA_DIR", "/data"),
            ("GATEWAY_NAMESPACE", "custom-ns"),
            ("POLL_INTERVAL", "2s"),
            ("GATEWAY_PILOT_AGENT_METRICS_PORT", "15099"),
            ("HYGRESS_API_READY_TIMEOUT", "300"),
            ("HYGRESS_SNAPSHOT_TIMEOUT", "300s"),
            ("HYGRESS_POLICY_PATH", "/etc/hygress/custom-policy.yaml"),
            ("HYGRESS_QUOTA_K", "8"),
            ("HYGRESS_GUARDRAIL_URL", "http://127.0.0.1:9090/v1/classify"),
        ]);
        assert_eq!(c.http_port, 9000);
        assert_eq!(c.tls_port, 9443);
        assert_eq!(c.gpustack_api_port, 8080);
        assert_eq!(c.admin_addr, "127.0.0.1:9081");
        assert_eq!(c.admin_token.as_deref(), Some("secret"));
        assert_eq!(c.jwt_secret_key.as_deref(), Some("jwt-secret"));
        assert_eq!(c.data_dir, "/data");
        assert_eq!(c.gateway_namespace, "custom-ns");
        assert_eq!(c.poll_interval, Duration::from_secs(2));
        assert_eq!(c.pilot_agent_metrics_port, 15099);
        assert_eq!(c.api_ready_timeout, Duration::from_secs(300));
        assert_eq!(c.snapshot_timeout, Duration::from_secs(300));
        assert_eq!(c.policy_path, "/etc/hygress/custom-policy.yaml");
        assert_eq!(c.quota_k, 8);
        assert_eq!(
            c.guardrail_url.as_deref(),
            Some("http://127.0.0.1:9090/v1/classify")
        );
    }

    #[test]
    fn poll_interval_forms() {
        assert_eq!(
            parse(&[("POLL_INTERVAL", "3")]).poll_interval,
            Duration::from_secs(3)
        );
        assert_eq!(
            parse(&[("POLL_INTERVAL", "1500ms")]).poll_interval,
            Duration::from_millis(1500)
        );
        assert_eq!(
            parse(&[("POLL_INTERVAL", "0bad")]).poll_interval,
            Duration::from_secs(1)
        );
    }

    #[test]
    fn blank_is_ignored() {
        let c = parse(&[("GATEWAY_HTTP_PORT", "  "), ("HYGRESS_ADMIN_TOKEN", "")]);
        assert_eq!(c.http_port, 80);
        assert_eq!(c.admin_token, None);
    }

    #[test]
    fn bad_port_falls_back_to_default() {
        let c = parse(&[("GATEWAY_HTTP_PORT", "not-a-port")]);
        assert_eq!(c.http_port, 80);
    }

    #[test]
    fn quota_k_bad_or_zero_falls_back() {
        // A non-numeric K keeps the default (4); a zero K is clamped to 1.
        assert_eq!(parse(&[("HYGRESS_QUOTA_K", "not-a-number")]).quota_k, 4);
        assert_eq!(parse(&[("HYGRESS_QUOTA_K", "0")]).quota_k, 1);
        assert_eq!(parse(&[("HYGRESS_QUOTA_K", "7")]).quota_k, 7);
    }

    #[test]
    fn topology_b_flag() {
        assert!(!parse(&[]).topology_b);
        assert!(parse(&[("HYGRESS_TOPOLOGY_B", "true")]).topology_b);
        assert!(parse(&[("HYGRESS_TOPOLOGY_B", "1")]).topology_b);
        assert!(parse(&[("HYGRESS_TOPOLOGY_B", "YES")]).topology_b);
        assert!(!parse(&[("HYGRESS_TOPOLOGY_B", "no")]).topology_b);
        assert!(!parse(&[("HYGRESS_TOPOLOGY_B", "")]).topology_b);
    }

    #[test]
    fn ext_auth_fail_mode_env() {
        // R-12: default fail-closed (matches GPUStack/Higress
        // `failure_mode_allow=false`); `open` flips to fail-open; `closed`
        // stays fail-closed; anything else is a typo — warned (ORA3-M1) and
        // kept fail-closed (the safe default).
        assert!(parse(&[]).ext_auth_fail_closed, "default must be closed");
        assert!(!parse(&[("HYGRESS_EXT_AUTH_FAIL_MODE", "open")]).ext_auth_fail_closed);
        assert!(!parse(&[("HYGRESS_EXT_AUTH_FAIL_MODE", "OPEN")]).ext_auth_fail_closed);
        assert!(parse(&[("HYGRESS_EXT_AUTH_FAIL_MODE", "closed")]).ext_auth_fail_closed);
        assert!(
            parse(&[("HYGRESS_EXT_AUTH_FAIL_MODE", "bogus")]).ext_auth_fail_closed,
            "unknown value stays closed"
        );
    }

    // ----- ORA3-M1: malformed env values warn and keep their OWN default -----

    #[test]
    fn malformed_numeric_env_keeps_each_default() {
        // Every invalid numeric env silently kept its default before ORA3-M1;
        // it still does (fail-safe), now with a warn naming key + value.
        let c = parse(&[
            ("GATEWAY_HTTP_PORT", "not-a-port"),
            ("GATEWAY_TLS_PORT", "abc"),
            ("GPUSTACK_API_PORT", "12x"),
            ("GATEWAY_PILOT_AGENT_METRICS_PORT", "no"),
        ]);
        assert_eq!(c.http_port, 80);
        assert_eq!(c.tls_port, 443);
        assert_eq!(c.gpustack_api_port, 80);
        assert_eq!(c.pilot_agent_metrics_port, 15020);
    }

    #[test]
    fn malformed_duration_env_keeps_each_keys_default() {
        // ORA3-M1: previously ANY malformed duration silently collapsed to 1s
        // (a typo'd HYGRESS_SNAPSHOT_TIMEOUT shrank the boot budget to 1s!).
        // Each malformed value now warns and keeps that key's documented
        // default (poll 1s / api-ready 30s / snapshot 60s).
        assert_eq!(
            parse(&[("POLL_INTERVAL", "0bad")]).poll_interval,
            Duration::from_secs(1)
        );
        assert_eq!(
            parse(&[("HYGRESS_API_READY_TIMEOUT", "forever")]).api_ready_timeout,
            Duration::from_secs(30)
        );
        assert_eq!(
            parse(&[("HYGRESS_SNAPSHOT_TIMEOUT", "garbage")]).snapshot_timeout,
            Duration::from_secs(60)
        );
        // Valid forms are unaffected (bare seconds / ms / s suffixes).
        assert_eq!(
            parse(&[("HYGRESS_API_READY_TIMEOUT", "5")]).api_ready_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(
            parse(&[("HYGRESS_SNAPSHOT_TIMEOUT", "2500ms")]).snapshot_timeout,
            Duration::from_millis(2500)
        );
    }

    #[test]
    fn malformed_bool_env_warns_and_keeps_default() {
        // `treu` (a real-world typo the audit found) must not silently seed
        // topology B; the falsy forms stay silent/valid.
        assert!(!parse(&[("HYGRESS_TOPOLOGY_B", "treu")]).topology_b);
        assert!(!parse(&[("HYGRESS_TOPOLOGY_B", "false")]).topology_b);
        assert!(!parse(&[("HYGRESS_TOPOLOGY_B", "off")]).topology_b);
    }

    /// The warn message itself names the env key and the attempted value, so an
    /// operator can find the typo (ORA3-M1 message contract; the tracing event
    /// is dropped in unit tests without a subscriber, so the pure prefix is
    /// what gets pinned here).
    #[test]
    fn warn_message_names_key_and_value() {
        let msg = unparsable_env_msg("HYGRESS_TOPOLOGY_B", "treu");
        assert!(
            msg.contains("HYGRESS_TOPOLOGY_B"),
            "must name the key: {msg}"
        );
        assert!(msg.contains("treu"), "must echo the attempted value: {msg}");
        let msg = unparsable_env_msg("POLL_INTERVAL", "0bad");
        assert!(
            msg.contains("POLL_INTERVAL") && msg.contains("0bad"),
            "{msg}"
        );
    }
}
