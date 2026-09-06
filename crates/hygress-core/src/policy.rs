//! Pure policy config types (design §3).
//!
//! [`PolicyConfig`] is the serde-shaped, **zero-I/O** policy snapshot the
//! gateway loads from `policy.yaml` (the `HYGRESS_POLICY_PATH` file, design
//! §2.1 / D-7). `Default` is **all-pass** (no limits, no quota, no guardrail,
//! no routes), so a missing or empty policy file is a no-op (design §7:
//! missing file = default pass + warn).
//!
//! # Route key (D-12)
//!
//! Routes are keyed by the **bare ingress name** (namespace-stripped), matched
//! as a glob (`*` = any sequence, e.g. `ai-route-route-*`).
//! [`PolicyConfig::for_ingress`] returns the **last** matching route (later
//! config wins). Merging a matched route's fields over the `global` defaults is
//! the **gateway's** job — core only selects the last/most-specific matching
//! spec (the design keeps merge semantics out of the pure core).
//!
//! # Schema notes
//!
//! - Rate limiting has **no** `window` field (D-6): a token bucket is defined by
//!   `rps` (fill rate) + `burst` (capacity) only.
//! - Quota `window_secs` **is** real (fixed-window reset semantics, D-6).
//! - The LLM guardrail's `service`/`path` are held by the **gateway** (egress
//!   side); core stores **no** URL (D-14).

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// The whole policy file (`/etc/hygress/policy.yaml`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PolicyConfig {
    /// Schema version (design §3: `version: 1`). Defaults to 1.
    #[serde(default = "default_policy_version")]
    pub version: u32,
    /// Global (default) policies, applied to every ingress unless a route
    /// overrides them. `Default` = all pass.
    #[serde(default)]
    pub global: GlobalPolicy,
    /// Per-route policies, keyed by bare ingress name glob (D-12).
    #[serde(default)]
    pub routes: Vec<RoutePolicySpec>,
}

fn default_policy_version() -> u32 {
    1
}

impl Default for PolicyConfig {
    /// All-pass: version 1, no global policies, no routes.
    fn default() -> Self {
        Self {
            version: default_policy_version(),
            global: GlobalPolicy::default(),
            routes: Vec::new(),
        }
    }
}

impl PolicyConfig {
    /// Select the route policy for a **bare** ingress name (D-12).
    ///
    /// A route matches when its `name_glob` glob-matches the bare ingress name
    /// (`*` matches any, possibly empty, sequence). When multiple routes match,
    /// the **last** one in `routes` wins (later config takes precedence).
    /// Returns `None` when no route matches (the gateway then falls back to
    /// `global`).
    pub fn for_ingress(&self, bare_ingress: &str) -> Option<&RoutePolicySpec> {
        let mut hit: Option<&RoutePolicySpec> = None;
        for r in &self.routes {
            if glob_match(&r.name_glob, bare_ingress) {
                hit = Some(r);
            }
        }
        hit
    }
}

/// Global (default) policy section.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GlobalPolicy {
    /// Per-IP / per-consumer token-bucket rate limits (design §4.1).
    #[serde(default)]
    pub limits: Option<LimitsSpec>,
    /// Token-quota limits (fixed window, design §4.2).
    #[serde(default)]
    pub quota: Option<QuotaSpec>,
    /// Guardrail policy (static rules / LLM verdict, design §4.4).
    #[serde(default)]
    pub guardrail: Option<GuardrailSpec>,
}

// ---------------------------------------------------------------------------
// Rate limiting (D-6: no window)
// ---------------------------------------------------------------------------

/// Token-bucket limits, keyed by ip and/or consumer (design §4.1).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LimitsSpec {
    /// Per-IP bucket (key = client ip; an empty key skips the dimension, D-9).
    #[serde(default)]
    pub ip: Option<TokenBucketSpec>,
    /// Per-consumer bucket (key = `X-Mse-Consumer`; `none`/absent skips, D-10).
    #[serde(default)]
    pub consumer: Option<TokenBucketSpec>,
}

/// One token-bucket: `rps` (fill rate, tokens/sec) + `burst` (capacity).
///
/// There is **no** `window` field (D-6): the limiter is a pure token bucket.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TokenBucketSpec {
    /// Sustained rate in tokens per second.
    pub rps: f64,
    /// Burst capacity (maximum tokens).
    pub burst: u64,
}

// ---------------------------------------------------------------------------
// Token quota (fixed window — real semantics, D-6)
// ---------------------------------------------------------------------------

/// Token-quota limits (design §4.2).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QuotaSpec {
    /// Per-(consumer, model) fixed-window token budget.
    #[serde(default)]
    pub by_model_tokens: Option<LimitWindowSpec>,
}

/// One fixed-window token budget.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LimitWindowSpec {
    /// Window length in seconds (fixed-window reset boundary).
    pub window_secs: u64,
    /// Soft limit (warn / optional degrade); `None` = no soft limit.
    #[serde(default)]
    pub soft: Option<u64>,
    /// Hard limit (reject, 429); `None` = no hard limit.
    #[serde(default)]
    pub hard: Option<u64>,
}

// ---------------------------------------------------------------------------
// Guardrail (design §4.4 / D-14)
// ---------------------------------------------------------------------------

/// Guardrail section (design §4.4).
///
/// `Default` = `fail_mode: Closed`, no static rules, no LLM. An **unconfigured**
/// guardrail (no `llm`, no `static_rules`) is a pass-through (D-14): `fail_mode`
/// only takes effect when the guardrail is enabled *and* its external call
/// fails.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GuardrailSpec {
    /// Behavior when the (enabled) guardrail's external verdict fails (D-14).
    /// Default `Closed` (reject — the safe direction).
    #[serde(default)]
    pub fail_mode: GuardrailFailMode,
    /// B4a static rules (regex).
    #[serde(default)]
    pub static_rules: Vec<StaticRuleSpec>,
    /// B4b LLM verdict client settings (service/path held by the gateway, D-14).
    #[serde(default)]
    pub llm: Option<LlmGuardSpec>,
}

/// Guardrail failure mode when the (enabled) external verdict fails (D-14).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailFailMode {
    /// Reject the request (fail-closed). Default.
    #[default]
    Closed,
    /// Allow the request (fail-open).
    Open,
}

/// One static guardrail rule (B4a).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaticRuleSpec {
    /// Rule name (reported in [`crate::guardrail::GuardDecision::hit_name`]).
    pub name: String,
    /// The regex to match (compiled by the core; compile errors surface as
    /// [`crate::error::Error`]).
    pub regex: String,
    /// The action on a hit (v1: only [`GuardAction::Block`]).
    pub action: GuardAction,
}

/// The action a guardrail rule takes on a hit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardAction {
    /// Block the request / cut the stream.
    Block,
}

/// B4b LLM guardrail verdict client settings.
///
/// The verdict service `service`/`path` are held by the **gateway** (the egress
/// `GuardrailClient`); core stores **no** URL (D-14).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmGuardSpec {
    /// `sync` (block until the verdict) or `async` (collect without blocking).
    #[serde(default)]
    pub mode: LlmGuardMode,
    /// Per-request verdict timeout in milliseconds.
    #[serde(default = "default_llm_timeout_ms")]
    pub timeout_ms: u64,
    /// Sustained RPS cap for verdict calls.
    #[serde(default = "default_llm_max_rps")]
    pub max_rps: u32,
    /// Verdict cache TTL in seconds.
    #[serde(default = "default_llm_cache_ttl_secs")]
    pub cache_ttl_secs: u64,
    /// Policy when the verdict call errors (D-14). Default `Reject`.
    #[serde(default)]
    pub on_error: LlmOnError,
}

impl Default for LlmGuardSpec {
    fn default() -> Self {
        Self {
            mode: LlmGuardMode::default(),
            timeout_ms: default_llm_timeout_ms(),
            max_rps: default_llm_max_rps(),
            cache_ttl_secs: default_llm_cache_ttl_secs(),
            on_error: LlmOnError::default(),
        }
    }
}

fn default_llm_timeout_ms() -> u64 {
    3000
}
fn default_llm_max_rps() -> u32 {
    5
}
fn default_llm_cache_ttl_secs() -> u64 {
    300
}

/// LLM verdict mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmGuardMode {
    /// Block the request until the verdict returns (or times out). Default.
    #[default]
    Sync,
    /// Do not block; the verdict is collected asynchronously.
    Async,
}

/// LLM verdict error policy (D-14).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LlmOnError {
    /// Reject on error (fail-closed). Default.
    #[default]
    Reject,
    /// Allow on error (fail-open).
    Allow,
}

// ---------------------------------------------------------------------------
// Route policy (per-ingress override; design §4.3 / D-2 / D-12)
// ---------------------------------------------------------------------------

/// One per-route policy, keyed by bare ingress name glob (D-12).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RoutePolicySpec {
    /// Bare ingress name glob (e.g. `ai-route-route-*`, `*`).
    pub name_glob: String,
    /// Route-level rate limits (override `global.limits`).
    #[serde(default)]
    pub limits: Option<LimitsSpec>,
    /// Route-level token quota (override `global.quota`).
    #[serde(default)]
    pub quota: Option<QuotaSpec>,
    /// Route-level routing-policy actions.
    #[serde(default)]
    pub policy: Option<RoutePolicyActions>,
    /// Route-level guardrail (inherit + append over `global.guardrail`).
    #[serde(default)]
    pub guardrail: Option<GuardrailSpec>,
}

/// Per-route routing-policy actions (design §4.3 / D-2).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RoutePolicyActions {
    /// Replace `prepared.candidates` with this target (`name.type:port`). The
    /// target must exist in the registry, else the gateway falls back to the
    /// original routing at runtime (D-2).
    #[serde(default)]
    pub override_route: Option<String>,
    /// Filter/pin candidate services by a `name.type` glob (e.g. `provider-8.*`).
    /// There is no "region" dimension in the data model (D-2).
    #[serde(default)]
    pub pin_provider_svc_pattern: Option<String>,
    /// Headers to add (applied in order).
    #[serde(default)]
    pub header_add: Vec<(String, String)>,
    /// Header names to remove.
    #[serde(default)]
    pub header_del: Vec<String>,
    /// Per-request timeout override (milliseconds).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Retry count override.
    #[serde(default)]
    pub retries: Option<u32>,
}

// ---------------------------------------------------------------------------
// Glob matching (`*` only)
// ---------------------------------------------------------------------------

/// Glob-match `pattern` against `input` where `*` matches any (possibly empty)
/// sequence of characters and every other character matches literally.
///
/// This is the D-12 route-key glob and the D-2 provider-service glob. Only `*`
/// is special (no `?` / character classes), matching the documented patterns
/// (`*`, `ai-route-route-*`, `provider-8.*`).
pub(crate) fn glob_match(pattern: &str, input: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = input.chars().collect();
    let (mut pi, mut si) = (0usize, 0usize);
    let (mut star_pi, mut star_si) = (usize::MAX, 0usize);
    while si < s.len() {
        if pi < p.len() && (p[pi] == '*' || p[pi] == s[si]) {
            if p[pi] == '*' {
                star_pi = pi;
                star_si = si;
                pi += 1;
            } else {
                pi += 1;
                si += 1;
            }
        } else if star_pi != usize::MAX {
            // Backtrack: extend the last `*`'s match by one input char, retry.
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }
    // Any trailing `*` (or run of them) matches the empty remainder.
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- Default = all pass -----

    #[test]
    fn default_is_all_pass() {
        let c = PolicyConfig::default();
        assert_eq!(c.version, 1);
        assert!(c.global.limits.is_none());
        assert!(c.global.quota.is_none());
        assert!(c.global.guardrail.is_none());
        assert!(c.routes.is_empty());
        // No route matches any ingress.
        assert!(c.for_ingress("ai-route-route-1.internal").is_none());
    }

    #[test]
    fn serde_empty_defaults() {
        // `{}` -> version 1, no global, no routes (all pass).
        let c: PolicyConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(c, PolicyConfig::default());
    }

    #[test]
    fn serde_full_round_trip() {
        let c = PolicyConfig {
            version: 1,
            global: GlobalPolicy {
                limits: Some(LimitsSpec {
                    ip: Some(TokenBucketSpec { rps: 20.0, burst: 40 }),
                    consumer: Some(TokenBucketSpec { rps: 100.0, burst: 200 }),
                }),
                quota: Some(QuotaSpec {
                    by_model_tokens: Some(LimitWindowSpec {
                        window_secs: 86400,
                        soft: None,
                        hard: Some(1_000_000),
                    }),
                }),
                guardrail: Some(GuardrailSpec {
                    fail_mode: GuardrailFailMode::Open,
                    static_rules: vec![StaticRuleSpec {
                        name: "prompt-inject".into(),
                        regex: "(?i)ignore previous instruction".into(),
                        action: GuardAction::Block,
                    }],
                    llm: Some(LlmGuardSpec {
                        mode: LlmGuardMode::Async,
                        timeout_ms: 5000,
                        max_rps: 3,
                        cache_ttl_secs: 60,
                        on_error: LlmOnError::Allow,
                    }),
                }),
            },
            routes: vec![RoutePolicySpec {
                name_glob: "ai-route-route-*".into(),
                limits: Some(LimitsSpec {
                    ip: None,
                    consumer: Some(TokenBucketSpec { rps: 5.0, burst: 10 }),
                }),
                quota: None,
                policy: Some(RoutePolicyActions {
                    override_route: Some("model-8-6.static:80".into()),
                    pin_provider_svc_pattern: Some("provider-8.*".into()),
                    header_add: vec![("x-canary".into(), "true".into())],
                    header_del: vec!["x-internal".into()],
                    timeout_ms: Some(30_000),
                    retries: Some(2),
                }),
                guardrail: None,
            }],
        };
        let v = serde_json::to_value(&c).unwrap();
        let back: PolicyConfig = serde_json::from_value(v).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn serde_guardrail_fail_mode_defaults_closed() {
        // A guardrail section with no `fail_mode` -> Closed (D-14 safe default).
        let g: GuardrailSpec = serde_json::from_str("{}").unwrap();
        assert_eq!(g.fail_mode, GuardrailFailMode::Closed);
        assert!(g.static_rules.is_empty());
        assert!(g.llm.is_none());

        let g2: GuardrailSpec =
            serde_json::from_str(r#"{"fail_mode":"open"}"#).unwrap();
        assert_eq!(g2.fail_mode, GuardrailFailMode::Open);
    }

    #[test]
    fn serde_llm_spec_defaults() {
        // `llm: {}` -> documented defaults (no service/path stored in core).
        let g: GuardrailSpec = serde_json::from_str(r#"{"llm":{}}"#).unwrap();
        let llm = g.llm.unwrap();
        assert_eq!(llm, LlmGuardSpec::default());
        assert_eq!(llm.mode, LlmGuardMode::Sync);
        assert_eq!(llm.timeout_ms, 3000);
        assert_eq!(llm.max_rps, 5);
        assert_eq!(llm.cache_ttl_secs, 300);
        assert_eq!(llm.on_error, LlmOnError::Reject);
    }

    // ----- for_ingress (D-12) -----

    #[test]
    fn for_ingress_star_matches_any() {
        let c = PolicyConfig {
            routes: vec![RoutePolicySpec {
                name_glob: "*".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(c.for_ingress("ai-route-route-1.internal").is_some());
        assert!(c.for_ingress("anything-else").is_some());
        assert!(c.for_ingress("").is_some());
    }

    #[test]
    fn for_ingress_prefix_wildcard() {
        let c = PolicyConfig {
            routes: vec![RoutePolicySpec {
                name_glob: "ai-route-route-*".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(c.for_ingress("ai-route-route-1.internal").is_some());
        assert!(c.for_ingress("ai-route-route-42.internal").is_some());
        assert!(c.for_ingress("other-route").is_none());
        assert!(c.for_ingress("ai-route-route-").is_some()); // `*` matches empty
    }

    #[test]
    fn for_ingress_last_match_wins() {
        // Two routes both match; the later one (index 1) takes precedence.
        let c = PolicyConfig {
            routes: vec![
                RoutePolicySpec {
                    name_glob: "*".into(),
                    limits: Some(LimitsSpec {
                        ip: Some(TokenBucketSpec { rps: 1.0, burst: 1 }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                RoutePolicySpec {
                    name_glob: "ai-route-route-*".into(),
                    limits: Some(LimitsSpec {
                        consumer: Some(TokenBucketSpec { rps: 5.0, burst: 10 }),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let hit = c.for_ingress("ai-route-route-9.internal").unwrap();
        // The second (later) route won: it has a consumer limit, no ip limit.
        assert!(hit.limits.as_ref().unwrap().consumer.is_some());
        assert!(hit.limits.as_ref().unwrap().ip.is_none());
    }

    #[test]
    fn for_ingress_no_match_returns_none() {
        let c = PolicyConfig {
            routes: vec![RoutePolicySpec {
                name_glob: "only-this".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(c.for_ingress("ai-route-route-1.internal").is_none());
    }

    #[test]
    fn for_ingress_exposes_route_fields() {
        // A matching route's own fields are what `for_ingress` exposes (the
        // gateway merges them over `global`; core only selects the spec).
        let c = PolicyConfig {
            global: GlobalPolicy {
                limits: Some(LimitsSpec {
                    ip: Some(TokenBucketSpec { rps: 20.0, burst: 40 }),
                    ..Default::default()
                }),
                ..Default::default()
            },
            routes: vec![RoutePolicySpec {
                name_glob: "ai-route-route-*".into(),
                limits: Some(LimitsSpec {
                    consumer: Some(TokenBucketSpec { rps: 5.0, burst: 10 }),
                    ..Default::default()
                }),
                quota: Some(QuotaSpec {
                    by_model_tokens: Some(LimitWindowSpec {
                        window_secs: 86400,
                        hard: Some(50_000),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }],
            ..Default::default()
        };
        let hit = c.for_ingress("ai-route-route-7.internal").unwrap();
        // The route spec carries the route's own limits/quota.
        assert!(hit.limits.as_ref().unwrap().consumer.is_some());
        assert!(hit.limits.as_ref().unwrap().ip.is_none());
        assert_eq!(
            hit.quota
                .as_ref()
                .unwrap()
                .by_model_tokens
                .as_ref()
                .unwrap()
                .hard,
            Some(50_000)
        );
    }

    // ----- glob_match -----

    #[test]
    fn glob_match_basics() {
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exac"));
        assert!(!glob_match("exact", "exactx"));
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("a*c", "ac")); // `*` matches empty
        assert!(glob_match("a*c", "aXc"));
        assert!(!glob_match("a*c", "aXX"));
        assert!(glob_match("a**b", "ab")); // run of stars == one star
        assert!(glob_match("ai-route-route-*", "ai-route-route-1.internal"));
        assert!(!glob_match("ai-route-route-*", "ai-route-route"));
        assert!(glob_match("provider-8.*", "provider-8.proxy"));
        assert!(!glob_match("provider-8.*", "provider-9.proxy"));
    }
}
