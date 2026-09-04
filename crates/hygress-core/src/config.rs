//! `ConfigData` snapshot, `RouteTable` index, config validation, and the
//! lock-free runtime holder (`SharedConfig`) — design §5.3 / §6.2 / §8.
//!
//! [`ConfigData`] is the pure, `Clone`-able control-plane snapshot produced
//! by the adapter from the managed CRDs. It is validated with
//! [`ConfigData::sanitize`] (per-object skip-and-report: malformed routes /
//! registries are dropped and the rest kept; structural failures reject the
//! whole snapshot) and stored whole into [`SharedConfig`]
//! (`ArcSwap<ConfigData>` + per-`route/destination-group` SWRR state in a
//! `DashMap`), so data-plane workers read it lock-free and a 1s poll diff
//! takes effect on the next request without restart (design D6).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use regex::RegexBuilder;
use serde::{Deserialize, Serialize};

use crate::destination::ServiceType;
use crate::error::Error;
use crate::matcher::RouteMatch;
use crate::registry::{OutboundProxy, Registry};
use crate::route::{FallbackLink, RouteKind, RouteRule};
use crate::swrr::SwrrState;

// ---------------------------------------------------------------------------
// Snapshot data
// ---------------------------------------------------------------------------

/// Control-plane snapshot (design §5.3 internal model).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ConfigData {
    /// Route rules (Main / Fallback / Mirror).
    pub routes: Vec<RouteRule>,
    /// `McpBridge.spec.registries`.
    pub registries: Vec<Registry>,
    /// `McpBridge.spec.proxies` (provider egress).
    pub proxies: Vec<OutboundProxy>,
    /// Per-WasmPlugin feature configs (the 8 plugin equivalents;
    /// `defaultConfigDisable` is recorded and treated as immutable).
    pub features: Vec<GatewayFeatureConfig>,
    /// TLS certificates (`Secret gpustack-tls-*` / `-default`).
    pub tls: TlsConfig,
    /// Timeouts / limits (`ConfigMap higress-config`; seed 1800/10, patched
    /// to 3 by `ensure_gateway_timeout`).
    pub timing: TimingConfig,
    /// `gpustack-model-router` (generic-proxy-router) `defaultConfig` — the hot-reloadable
    /// model-resolver settings (plugin-contract-pin §2.3). Additive: existing snapshots
    /// without the field deserialize to the defaults.
    #[serde(default)]
    pub model_router: ModelRouterSettings,
    /// Per-destination provider `apiTokens` (the `gpustack-ai-proxy` WasmPlugin
    /// `defaultConfig` `providers` + `matchRules`; design D6 / §7 ai-proxy). For a
    /// `provider-<id>.<type>` destination the live outbound path swaps `Authorization`
    /// to one of these tokens so the request reaches the provider with the **provider's**
    /// key (not the client/registration key). Additive: existing snapshots without the
    /// field deserialize to empty.
    #[serde(default)]
    pub provider_tokens: Vec<ProviderToken>,
}

impl ConfigData {
    /// Per-object validation issues (malformed routes / registries).
    ///
    /// These do **not** reject the whole snapshot — use
    /// [`ConfigData::sanitize`] to drop the offending objects and keep the
    /// rest. See [`ConfigData::sanitize`] for the skip-and-report contract.
    pub fn validate(&self) -> Vec<ValidationError> {
        self.sanitize().issues
    }

    /// Validate and drop offending objects, returning the accepted subset.
    ///
    /// Contract:
    /// - **per-object** issues (a route with an empty key / no destination /
    ///   a bad endpoint / a bad weight sum / a mirror that is authed / an
    ///   unknown fallback target / an auth-enabled route with an empty scope
    ///   root; a registry missing a required field / an unknown proxy ref; a
    ///   duplicate route key) DROP that single object and keep the rest;
    /// - **structural** failures (a path predicate that is not a valid regex)
    ///   are surfaced when a [`RouteTable`] is built from the accepted set —
    ///   they reject the whole snapshot.
    ///
    /// Fallbacks are derived from [`RouteRule::fallback`] (the canonical
    /// form; see [`ConfigData::fallbacks`]); a route whose fallback target
    /// references no Fallback route is dropped with its fallback issue.
    pub fn sanitize(&self) -> SanitizeResult {
        let mut issues: Vec<ValidationError> = Vec::new();

        // ---- routes: per-object skip-and-report ----
        let mut accepted_routes: Vec<RouteRule> = Vec::new();
        let mut seen_keys: BTreeSet<(String, String)> = BTreeSet::new();
        for route in &self.routes {
            let label = if route.key.is_empty() {
                "route (empty key)".to_string()
            } else {
                format!("route '{}'", route.key)
            };
            let mut route_issues: Vec<ValidationError> = Vec::new();

            if route.key.is_empty() {
                route_issues.push(ValidationError::new(format!(
                    "{label}: route key must be non-empty"
                )));
            } else if !seen_keys.insert((format!("{:?}", route.kind), route.key.clone())) {
                route_issues.push(ValidationError::new(format!(
                    "{label}: duplicate route key"
                )));
            }
            if route.kind == RouteKind::Mirror && route.auth_scope.enabled {
                route_issues.push(ValidationError::new(format!(
                    "{label}: mirror route must not have auth enabled"
                )));
            }
            if route.destinations.is_empty() {
                route_issues.push(ValidationError::new(format!(
                    "{label}: has no destinations"
                )));
            }
            for d in &route.destinations {
                if let Err(e) = d.service_ref() {
                    route_issues.push(ValidationError::new(format!(
                        "{label}: bad endpoint '{}': {e}",
                        d.service
                    )));
                }
            }
            let weighted = route.destinations.iter().any(|d| d.percent.is_some());
            if weighted {
                let sum: u32 = route.destinations.iter().map(|d| d.weight()).sum();
                if sum != 100 {
                    route_issues.push(ValidationError::new(format!(
                        "{label}: destination weights sum to {sum}, expected 100"
                    )));
                }
            }
            if let Some(fl) = &route.fallback {
                if !self.routes.iter().any(|r| r.kind == RouteKind::Fallback && r.key == fl.target_key) {
                    route_issues.push(ValidationError::new(format!(
                        "{label}: fallback target '{}' not found in routes",
                        fl.target_key
                    )));
                }
            }
            if route.kind != RouteKind::Mirror
                && route.auth_scope.enabled
                && route.auth_scope.scope_root.is_empty()
            {
                route_issues.push(ValidationError::new(format!(
                    "{label}: auth enabled but scope_root is empty"
                )));
            }

            if route_issues.is_empty() {
                accepted_routes.push(route.clone());
            } else {
                issues.extend(route_issues);
            }
        }

        // ---- registries: per-object skip-and-report ----
        let mut accepted_registries: Vec<Registry> = Vec::new();
        for reg in &self.registries {
            let label = format!("registry '{}'", reg.id);
            let mut reg_issues: Vec<ValidationError> = Vec::new();
            if reg.id.is_empty() || reg.domain.is_empty() {
                reg_issues.push(ValidationError::new(format!(
                    "{label}: id and domain must be non-empty"
                )));
            }
            match reg.kind {
                ServiceType::Dns if reg.port.is_none() => {
                    reg_issues.push(ValidationError::new(format!(
                        "{label}: dns registry requires a port"
                    )));
                }
                ServiceType::Proxy => match &reg.proxy_ref {
                    None => {
                        reg_issues.push(ValidationError::new(format!(
                            "{label}: proxy registry requires proxy_ref"
                        )));
                    }
                    Some(p) if !self.proxies.iter().any(|x| &x.name == p) => {
                        reg_issues.push(ValidationError::new(format!(
                            "{label}: references unknown outbound proxy '{p}'"
                        )));
                    }
                    Some(_) => {}
                },
                _ => {}
            }
            if reg_issues.is_empty() {
                accepted_registries.push(reg.clone());
            } else {
                issues.extend(reg_issues);
            }
        }

        // ---- provider tokens: per-object skip-and-report (D6 / §7) ----
        let mut accepted_provider_tokens: Vec<ProviderToken> = Vec::new();
        for pt in &self.provider_tokens {
            let label = if pt.service.is_empty() {
                "provider token (empty service)".to_string()
            } else {
                format!("provider token '{}'", pt.service)
            };
            let mut pt_issues: Vec<ValidationError> = Vec::new();
            if !is_valid_service_id(&pt.service) {
                pt_issues.push(ValidationError::new(format!(
                    "{label}: service must be a valid `name.type` id (alphanumeric, '-', '_', '.')"
                )));
            }
            if pt.api_tokens.is_empty() {
                pt_issues.push(ValidationError::new(format!(
                    "{label}: has no apiTokens"
                )));
            }
            if pt_issues.is_empty() {
                accepted_provider_tokens.push(pt.clone());
            } else {
                issues.extend(pt_issues);
            }
        }

        issues.sort_by(|a, b| a.message.cmp(&b.message));

        let accepted = ConfigData {
            routes: accepted_routes,
            registries: accepted_registries,
            proxies: self.proxies.clone(),
            features: self.features.clone(),
            tls: self.tls.clone(),
            timing: self.timing.clone(),
            model_router: self.model_router.clone(),
            provider_tokens: accepted_provider_tokens,
        };

        SanitizeResult { accepted, issues }
    }

    /// Fallback specs **derived** from the canonical
    /// [`RouteRule::fallback`] links (design: one canonical form, the rest is
    /// a view). A route without a fallback link contributes nothing.
    pub fn fallbacks(&self) -> Vec<FallbackSpec> {
        self.routes
            .iter()
            .filter_map(FallbackSpec::from_route)
            .collect()
    }

    /// Resolve the provider API token (bearer) for a (selected destination
    /// `service`, originating `ingress`) pair (design D6 / §7 ai-proxy). See
    /// [`provider_bearer`] for the selection semantics.
    pub fn provider_token(&self, service: &str, ingress_name: &str) -> Option<&str> {
        provider_bearer(&self.provider_tokens, service, ingress_name)
    }
}

/// The provider key-swap selection (design D6 / §7 ai-proxy).
///
/// Given the per-destination [`ProviderToken`] list, a selected destination
/// `service` (the `name.type`, **no port**, e.g. `provider-1.proxy`) and the
/// originating `ingress` name, return the bearer (the first `api_token`) of the
/// matching [`ProviderToken`]. An **ingress-scoped** token
/// ([`ProviderToken::ingress_scope`] = `Some`) wins over a global one (= `None`)
/// when its scope matches the ingress name; otherwise the first global token is
/// used. Ingress comparison uses the **bare** name (last path segment) so `name`
/// and `<ns>/name` forms match. Returns `None` when no token matches (the caller
/// then keeps the existing `Authorization` write-back).
pub fn provider_bearer<'a>(
    tokens: &'a [ProviderToken],
    service: &str,
    ingress_name: &str,
) -> Option<&'a str> {
    let ingress_bare = ingress_name.rsplit('/').next().unwrap_or(ingress_name);
    let mut global: Option<&ProviderToken> = None;
    for pt in tokens {
        if pt.service != service || pt.api_tokens.is_empty() {
            continue;
        }
        match &pt.ingress_scope {
            Some(scope) => {
                let scope_bare = scope.rsplit('/').next().unwrap_or(scope);
                if !scope_bare.is_empty() && scope_bare == ingress_bare {
                    return pt.api_tokens.first().map(String::as_str);
                }
            }
            None => {
                if global.is_none() {
                    global = Some(pt);
                }
            }
        }
    }
    global.and_then(|pt| pt.api_tokens.first().map(String::as_str))
}

/// The skip-and-report result of [`ConfigData::sanitize`].
#[derive(Clone, Debug, Default)]
pub struct SanitizeResult {
    /// Snapshot with offending objects dropped (good objects kept).
    pub accepted: ConfigData,
    /// Per-object issues that caused an object to be dropped.
    pub issues: Vec<ValidationError>,
}

/// One validation finding (deterministically ordered by message).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationError {
    pub message: String,
}

impl ValidationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

// ---------------------------------------------------------------------------
// Section types
// ---------------------------------------------------------------------------

/// EnvoyFilter `custom_response` fallback spec (design §5.3 / §6.1 ⑭).
///
/// This is a **derived view** of the canonical [`RouteRule::fallback`]
/// ([`FallbackLink`]) link — it is not an independent source of truth. See
/// [`ConfigData::fallbacks`] and [`FallbackSpec::from_route`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FallbackSpec {
    /// Main route key (the value placed into `x-higress-fallback-from`).
    pub route_key: String,
    /// Origin (main) ingress name this fallback redirects from (ns-qualified).
    #[serde(default)]
    pub main_ingress_name: String,
    /// Fallback route key (the key of the linked Fallback route).
    pub target_key: String,
    /// Max internal redirects (default 10).
    #[serde(default = "default_max_redirects")]
    pub max_redirects: u32,
    /// `use_original_request_body` (default true).
    #[serde(default = "default_true")]
    pub use_original_body: bool,
    /// `use_original_uri` (default true).
    #[serde(default = "default_true")]
    pub use_original_uri: bool,
}

impl FallbackSpec {
    /// Derive a [`FallbackSpec`] from the canonical fallback link on a route.
    /// `None` when the route has no fallback link.
    pub fn from_route(route: &RouteRule) -> Option<Self> {
        let link: &FallbackLink = route.fallback.as_ref()?;
        Some(Self {
            route_key: route.key.clone(),
            main_ingress_name: if link.main_ingress_name.is_empty() {
                route.ingress_name.clone()
            } else {
                link.main_ingress_name.clone()
            },
            target_key: link.target_key.clone(),
            max_redirects: link.max_redirects,
            use_original_body: link.use_original_request,
            use_original_uri: link.use_original_request,
        })
    }
}

fn default_max_redirects() -> u32 {
    10
}

fn default_true() -> bool {
    true
}

/// One WasmPlugin-equivalent feature config (plugin name + phase + priority +
/// immutable `defaultConfigDisable`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayFeatureConfig {
    /// WasmPlugin resource name (e.g. `gpustack-model-router`).
    pub plugin: String,
    /// Phase (`AUTHN` / `AUTHZ` / `STATS`).
    pub phase: String,
    /// Priority within the phase (higher runs first).
    pub priority: i32,
    /// `failStrategy: FAIL_OPEN`.
    #[serde(default)]
    pub fail_open: bool,
    /// `defaultConfigDisable` — recorded and immutable after creation.
    #[serde(default)]
    pub default_config_disable: bool,
    /// The plugin `defaultConfig` / match-rule payload (opaque here).
    #[serde(default)]
    pub config: serde_json::Value,
}

/// TLS certificate table (`Secret gpustack-tls-<host>` / `-default`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsConfig {
    pub hosts: Vec<TlsHost>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsHost {
    /// SNI host (or the host string of the `gpustack-tls-<host>` secret).
    pub host: String,
    /// `true` for the `gpustack-tls-default` fallback cert.
    #[serde(default)]
    pub is_default: bool,
    /// `tls.crt` PEM.
    pub cert_pem: String,
    /// `tls.key` PEM (plaintext in memory only).
    pub key_pem: String,
}

/// Timeouts / limits from `ConfigMap higress-config` (design §2.1.2: seed
/// `downstream.idleTimeout=1800, upstream.idleTimeout=10`;
/// `ensure_gateway_timeout` rewrites upstream to env-driven default 3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimingConfig {
    #[serde(default = "default_downstream_idle")]
    pub downstream_idle_timeout_secs: u64,
    #[serde(default = "default_upstream_idle")]
    pub upstream_idle_timeout_secs: u64,
    pub max_request_headers_kb: Option<u64>,
}

fn default_downstream_idle() -> u64 {
    1800
}

fn default_upstream_idle() -> u64 {
    10
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            downstream_idle_timeout_secs: default_downstream_idle(),
            upstream_idle_timeout_secs: default_upstream_idle(),
            max_request_headers_kb: None,
        }
    }
}

/// The `gpustack-model-router` (generic-proxy-router) `defaultConfig` — the
/// hot-reloadable model-resolver settings (plugin-contract-pin §2.3).
///
/// GPUStack always writes a NON-EMPTY `enableOnPathSuffix` (openai + anthropic
/// routes) and hot-updates `aliasNameMapping` per route; only `aliasNameMapping`
/// and `maxBodyBytes` survive init-time diffs (the router reconciler only mutates
/// `aliasNameMapping`, and `defaultConfigDisable` is never flipped).
///
/// Serde field names mirror the WasmPlugin `defaultConfig` **wire keys** (camelCase):
/// `prefix`, `targetHeader`, `enableOnPathSuffix`, `aliasNameMapping`, `maxBodyBytes`.
/// The other Wasm config keys (`modelKey`, `autoRouting*`, ...) are ignored on
/// deserialization; absent keys fall back to the defaults below.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRouterSettings {
    /// Path prefix that arms path-driven (alias) mode — default `/model/proxy/`.
    #[serde(default = "default_model_router_prefix", rename = "prefix")]
    pub prefix: String,
    /// Header the resolved model is written to — default `x-higress-llm-model`.
    #[serde(default = "default_target_header", rename = "targetHeader")]
    pub target_header: String,
    /// Request paths (full prefixes) that arm body-driven mode. GPUStack always writes this
    /// NON-EMPTY; an empty list means body-driven mode is disabled.
    #[serde(default, rename = "enableOnPathSuffix")]
    pub enable_on_path_suffix: Vec<String>,
    /// `aliasNameMapping` — `str(route_id)` → effective model name; hot-updated per route.
    #[serde(default, rename = "aliasNameMapping")]
    pub alias_name_mapping: BTreeMap<String, String>,
    /// Body read cap in bytes (the plugin's `maxBodyBytes`; a positive integer).
    #[serde(default, rename = "maxBodyBytes")]
    pub max_body_bytes: Option<usize>,
}

fn default_model_router_prefix() -> String {
    "/model/proxy/".to_string()
}

fn default_target_header() -> String {
    "x-higress-llm-model".to_string()
}

impl Default for ModelRouterSettings {
    fn default() -> Self {
        Self {
            prefix: default_model_router_prefix(),
            target_header: default_target_header(),
            enable_on_path_suffix: Vec::new(),
            alias_name_mapping: BTreeMap::new(),
            max_body_bytes: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Provider tokens (the ai-proxy key-swap source; design D6 / §7)
// ---------------------------------------------------------------------------

/// One per-destination provider `apiTokens` entry, parsed from the
/// `gpustack-ai-proxy` WasmPlugin (design D6 / §7 ai-proxy).
///
/// GPUStack writes a `defaultConfig.providers[]` (each carrying an `id` and a
/// `apiTokens[]` list) plus `matchRules[]` that pin the **active** provider
/// (`config.activeProviderId`) to a `service` (the `name.type` destination,
/// **no port**) and, optionally, to an `ingress` scope. The adapter flattens
/// that into one [`ProviderToken`] per (`service`, `ingress-scope`) pair so the
/// data plane can, for a `provider-<id>.<type>` destination, swap the outbound
/// `Authorization` to the provider's key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderToken {
    /// The destination service `name.type` (**no port**) the tokens apply to,
    /// e.g. `provider-1.proxy`. Must be a valid `name.type` service id.
    #[serde(default)]
    pub service: String,
    /// An optional ingress scope (a bare ingress name, `name` or `<ns>/name`)
    /// that narrows the tokens to one ingress/route. `None` means the tokens
    /// apply to **every** ingress that selects `service`.
    #[serde(default)]
    pub ingress_scope: Option<String>,
    /// The provider `apiTokens` (at least one). The first entry is the active
    /// bearer the gateway swaps into `Authorization`.
    #[serde(default)]
    pub api_tokens: Vec<String>,
}

/// True when `service` is a well-formed `name.type` service id: non-empty and
/// composed only of alphanumeric characters, `-`, `_`, and `.` (the registry
/// grammar, design §4.4) — e.g. `provider-1.proxy` / `model-1-10.static`.
fn is_valid_service_id(service: &str) -> bool {
    !service.is_empty()
        && service
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

// ---------------------------------------------------------------------------
// RouteTable (runtime index)
// ---------------------------------------------------------------------------

/// Length of the leading literal (non-regex) portion of a predicate pattern,
/// after removing empty capture groups. Used to rank predicates **within** a
/// single matched route (longest anchor chosen for rewrite capture).
pub fn literal_anchor_len(pattern: &str) -> usize {
    let normalized = strip_empty_groups(pattern);
    normalized
        .chars()
        .take_while(|c| !is_regex_metachar(*c))
        .count()
}

fn is_regex_metachar(c: char) -> bool {
    matches!(
        c,
        '$' | '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\'
    )
}

/// Remove `()` / `(?:)` empty groups so `()/path...` anchors at `/path`.
fn strip_empty_groups(pattern: &str) -> String {
    let mut s = pattern.replace("(?:)", "");
    loop {
        let next = s.replace("()", "");
        if next == s {
            return next;
        }
        s = next;
    }
}

/// Wrap a path predicate so it only matches the **entire** path (full match).
///
/// Higress `ImplementationSpecific` path matching and GPUStack's
/// `regex_prefixes` are whole-path: the regex must consume the whole path.
/// Wrapping in `^(?:...)` + `$` enforces full-match regardless of the
/// predicate's own shape (a pattern with no trailing `.*` therefore does not
/// match a `/…/extra` suffix).
fn full_match_pattern(pattern: &str) -> String {
    format!("^(?:{})$", pattern)
}

/// Lock-free runtime route index built from a [`ConfigData`].
///
/// Owns a copy of the routes (snapshot semantics) plus compiled full-match
/// path predicates and two **separate** exact-key indexes:
/// - [`RouteTable::by_main_key`] — Main routes only (initial requests);
/// - [`RouteTable::by_fallback_key`] — Fallback routes only (fallback redirects).
///
/// The two key spaces are physically separated so a Fallback rule can never
/// be selected by an initial request (and vice versa).
pub struct RouteTable {
    routes: Vec<RouteRule>,
    /// Exact-key index over Main routes (initial requests only).
    by_main_key: BTreeMap<String, Vec<usize>>,
    /// Exact-key index over Fallback routes (fallback redirects only).
    by_fallback_key: BTreeMap<String, Vec<usize>>,
    /// Compiled full-match predicates per route (index-aligned with `routes`).
    regexes: Vec<Vec<regex::Regex>>,
    /// Per-route predicate anchor lengths (for within-route ranking).
    anchors: Vec<Vec<usize>>,
    /// First `Mirror` route, if any (the only path-based catch-all).
    mirror_index: Option<usize>,
}

impl RouteTable {
    /// Build the runtime index for a snapshot.
    ///
    /// Fails with [`Error::Parse`] when a path predicate is not a valid
    /// regex (a structural failure that rejects the whole snapshot).
    pub fn rebuild(data: &ConfigData) -> Result<Self, Error> {
        let routes = data.routes.clone();
        let mut by_main_key: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut by_fallback_key: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut regexes: Vec<Vec<regex::Regex>> = Vec::with_capacity(routes.len());
        let mut anchors: Vec<Vec<usize>> = Vec::with_capacity(routes.len());
        let mut mirror_index: Option<usize> = None;

        for (r, route) in routes.iter().enumerate() {
            // Separate Main / Fallback key spaces (type-safe: a Fallback rule
            // is never reachable from the initial-request index).
            if !route.key.is_empty() {
                match route.kind {
                    RouteKind::Main => {
                        by_main_key.entry(route.key.clone()).or_default().push(r);
                    }
                    RouteKind::Fallback => {
                        by_fallback_key.entry(route.key.clone()).or_default().push(r);
                    }
                    RouteKind::Mirror => {}
                }
            }
            if route.kind == RouteKind::Mirror && mirror_index.is_none() {
                mirror_index = Some(r);
            }

            let mut compiled = Vec::with_capacity(route.path_predicates.len());
            let mut anchor_lens = Vec::with_capacity(route.path_predicates.len());
            for p in &route.path_predicates {
                let re = RegexBuilder::new(&full_match_pattern(&p.regex))
                    .case_insensitive(p.ignore_case)
                    .build()
                    .map_err(|e| {
                        Error::parse(format!(
                            "route '{}': invalid path regex '{}': {e}",
                            route.key, p.regex
                        ))
                    })?;
                anchor_lens.push(literal_anchor_len(&p.regex));
                compiled.push(re);
            }
            regexes.push(compiled);
            anchors.push(anchor_lens);
        }

        Ok(RouteTable {
            routes,
            by_main_key,
            by_fallback_key,
            regexes,
            anchors,
            mirror_index,
        })
    }

    /// Choose the predicate **within** route `r` that full-matches `path`:
    /// longest literal anchor wins (ties -> first), for rewrite capture. For
    /// a Mirror route (the catch-all) the longest-anchor predicate is chosen
    /// without a path test.
    fn best_predicate(&self, r: usize, path: &str) -> Option<usize> {
        let is_mirror = self.routes[r].kind == RouteKind::Mirror;
        let mut best: Option<(usize, usize)> = None; // (anchor, pred_index)
        for (pi, re) in self.regexes[r].iter().enumerate() {
            let matched = if is_mirror {
                true // mirror is the catch-all (its `/` prefix matches everything)
            } else {
                re.is_match(path)
            };
            if matched {
                let anchor = self.anchors[r][pi];
                if best.is_none_or(|(ba, _)| anchor > ba) {
                    best = Some((anchor, pi));
                }
            }
        }
        best.map(|(_, pi)| pi)
    }

    /// **Initial request**: match a **Main** route by exact
    /// `x-higress-llm-model` key (AND its full-match path predicate), else the
    /// mirror. A Fallback route is never selectable here.
    pub fn find_match(&self, model_key: Option<&str>, path: &str) -> Option<RouteMatch> {
        if let Some(k) = model_key {
            if let Some(idxs) = self.by_main_key.get(k) {
                for &r in idxs {
                    if let Some(pred) = self.best_predicate(r, path) {
                        return Some(RouteMatch {
                            index: r,
                            matched_by: crate::matcher::MatchKind::HeaderExact,
                            matched_predicate: Some(pred),
                        });
                    }
                }
            }
        }

        // Mirror catch-all (the only path-based last resort).
        self.mirror_match(path)
    }

    /// **Fallback redirect**: match a **Fallback** route by exact
    /// `x-higress-fallback-from` key (AND its full-match path predicate), else
    /// the mirror. A Main route is never selectable here.
    pub fn find_match_fallback(&self, fallback_from: Option<&str>, path: &str) -> Option<RouteMatch> {
        if let Some(k) = fallback_from {
            if let Some(idxs) = self.by_fallback_key.get(k) {
                for &r in idxs {
                    if let Some(pred) = self.best_predicate(r, path) {
                        return Some(RouteMatch {
                            index: r,
                            matched_by: crate::matcher::MatchKind::FallbackExact,
                            matched_predicate: Some(pred),
                        });
                    }
                }
            }
        }

        // Mirror catch-all (the only path-based last resort).
        self.mirror_match(path)
    }

    /// The mirror route, if any (unconditional catch-all; no path test).
    /// `matched_predicate` records the longest-anchor predicate for rewrite
    /// capture (first, in the rare no-predicate case).
    fn mirror_match(&self, _path: &str) -> Option<RouteMatch> {
        self.mirror_index.map(|m| RouteMatch {
            index: m,
            matched_by: crate::matcher::MatchKind::Mirror,
            matched_predicate: if self.regexes[m].is_empty() {
                None
            } else {
                // Non-empty slice -> max exists.
                let best_pi = (0..self.regexes[m].len())
                    .max_by_key(|i| self.anchors[m][*i])
                    .unwrap();
                Some(best_pi)
            },
        })
    }

    /// Route at `index`.
    pub fn route(&self, index: usize) -> &RouteRule {
        &self.routes[index]
    }

    /// All routes, in snapshot order.
    pub fn routes(&self) -> &[RouteRule] {
        &self.routes
    }

    /// The mirror route index, if any.
    pub fn mirror_index(&self) -> Option<usize> {
        self.mirror_index
    }

    /// The Main-route index for an `x-higress-llm-model` value.
    pub fn main_route_indices(&self, model_key: &str) -> &[usize] {
        self.by_main_key
            .get(model_key)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// The Fallback-route index for an `x-higress-fallback-from` value.
    pub fn fallback_route_indices(&self, fallback_from: &str) -> &[usize] {
        self.by_fallback_key
            .get(fallback_from)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

// ---------------------------------------------------------------------------
// SharedConfig (lock-free runtime holder)
// ---------------------------------------------------------------------------

/// Deterministic (FNV-1a 64-bit, hex) digest of a **sorted, deduped** set of
/// destination service ids — a stable route-group identity that is
/// independent of destination ordering and of run (no per-process-random
/// hashing, so keys survive restarts and re-indexing across polls).
fn group_digest(ids: &[String]) -> String {
    let set: BTreeSet<&str> = ids.iter().map(|s| s.as_str()).collect();
    let mut hash: u64 = 0xcbf29ce484222325; // FNV-1a 64 offset basis
    for id in set.iter() {
        for b in id.as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x00000100_000001B3); // FNV-1a 64 prime
        }
        // Separator between ids so ("ab","c") != ("a","bc").
        hash ^= 0x2d;
        hash = hash.wrapping_mul(0x00000100_000001B3);
    }
    format!("{hash:016x}")
}

/// Stable route-group identity: `(route key, digest of sorted destination
/// service ids)`. One shared SWRR state per group.
fn group_key(route_key: &str, destination_services: &[String]) -> (String, String) {
    (route_key.to_string(), group_digest(destination_services))
}

/// The set of valid route-group keys for a snapshot (for stale-state pruning).
fn valid_group_keys(data: &ConfigData) -> BTreeSet<(String, String)> {
    data.routes
        .iter()
        .map(|r| {
            let svcs: Vec<String> = r.destinations.iter().map(|d| d.service.clone()).collect();
            group_key(&r.key, &svcs)
        })
        .collect()
}

/// Runtime shared state: `ArcSwap<ConfigData>` snapshot + per-route-group
/// SWRR state (design §6.2 / §8).
///
/// The SWRR key is a **route group**: `(route key, digest of the sorted
/// destination service ids)`. One [`SwrrState`] (holding `current_weights`
/// across every candidate) is shared by all requests to that route, so the
/// weighted selection stays smooth and Nginx-deterministic across workers.
#[derive(Debug)]
pub struct SharedConfig {
    data: ArcSwap<ConfigData>,
    swrr_states: DashMap<(String, String), SwrrState>,
}

impl SharedConfig {
    /// Create the holder from a snapshot.
    ///
    /// Per-object issues are dropped (good objects kept). The whole snapshot
    /// is rejected (`Err`) only for a **structural** failure (a path predicate
    /// that is not a valid regex). Callers wanting the per-object issues use
    /// [`ConfigData::sanitize`].
    pub fn new(data: ConfigData) -> Result<Self, Vec<ValidationError>> {
        let SanitizeResult {
            accepted,
            issues,
        } = data.sanitize();
        if let Err(e) = RouteTable::rebuild(&accepted) {
            let mut all = issues;
            all.push(ValidationError::new(format!("structural: {e}")));
            return Err(all);
        }
        Ok(Self {
            data: ArcSwap::from_pointee(accepted),
            swrr_states: DashMap::new(),
        })
    }

    /// Atomically swap in a (sanitized) snapshot (poll diff hot-reload).
    ///
    /// Per-object issues are dropped (good objects kept); the whole snapshot
    /// is rejected only for a structural failure. Stale SWRR state entries for
    /// routes / destination groups no longer present in the new snapshot are
    /// pruned before the swap.
    pub fn store(&self, data: ConfigData) -> Result<(), Vec<ValidationError>> {
        let SanitizeResult {
            accepted,
            issues,
        } = data.sanitize();
        if let Err(e) = RouteTable::rebuild(&accepted) {
            let mut all = issues;
            all.push(ValidationError::new(format!("structural: {e}")));
            return Err(all);
        }
        // Prune stale SWRR state for removed routes / destination groups.
        let valid = valid_group_keys(&accepted);
        self.swrr_states
            .retain(|k, _| valid.contains(k));
        self.data.store(Arc::new(accepted));
        Ok(())
    }

    /// The current snapshot (lock-free read).
    pub fn load(&self) -> Arc<ConfigData> {
        self.data.load_full()
    }

    /// Borrow (creating on first use) the shared SWRR state for one
    /// **route group** identified by `(route key, sorted destination service
    /// ids)`.
    pub fn swrr_group_state(
        &self,
        route_key: &str,
        destination_services: &[String],
    ) -> dashmap::mapref::entry::Entry<'_, (String, String), SwrrState> {
        let key = group_key(route_key, destination_services);
        self.swrr_states.entry(key)
    }

    /// The current SWRR state for a route group, if one exists (read-only).
    pub fn swrr_group_state_ref(
        &self,
        route_key: &str,
        destination_services: &[String],
    ) -> Option<dashmap::mapref::one::Ref<'_, (String, String), SwrrState>> {
        let key = group_key(route_key, destination_services);
        self.swrr_states.get(&key)
    }

    /// Rebuild the route table for the current snapshot (call after
    /// `store` when the gateway wants a fresh index).
    pub fn route_table(&self) -> Result<RouteTable, Error> {
        RouteTable::rebuild(&self.data.load_full())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::{Destination, ServiceType};
    use crate::route::{PathPred, RouteKind, RouteRule};

    fn main_route(key: &str) -> RouteRule {
        RouteRule::new(
            key,
            RouteKind::Main,
            vec![PathPred::new("/(v1)()(/chat/completions|/embeddings)")],
            vec![Destination::new("model-1-10.static:80")],
        )
        .unwrap()
    }

    #[test]
    fn rebuild_and_route_access() {
        let data = ConfigData {
            routes: vec![main_route("m1"), main_route("m2")],
            ..Default::default()
        };
        let t = RouteTable::rebuild(&data).unwrap();
        assert_eq!(t.routes().len(), 2);
        assert_eq!(t.route(0).key, "m1");
        assert_eq!(t.mirror_index(), None);
    }

    #[test]
    fn rebuild_rejects_invalid_regex() {
        let data = ConfigData {
            routes: vec![RouteRule::new(
                "m",
                RouteKind::Main,
                vec![PathPred::new("([unclosed")],
                vec![Destination::new("a.static:80")],
            )
            .unwrap()],
            ..Default::default()
        };
        assert!(matches!(RouteTable::rebuild(&data), Err(Error::Parse(_))));
    }

    #[test]
    fn main_fallback_key_spaces_are_separate() {
        let data = ConfigData {
            routes: vec![
                // Same key "k" on a Main and a Fallback route: the indexes
                // must not bleed into each other.
                RouteRule::new(
                    "k",
                    RouteKind::Main,
                    vec![PathPred::new("/(v1)()(/chat/completions)")],
                    vec![Destination::new("a.static:80")],
                )
                .unwrap(),
                RouteRule::new(
                    "k",
                    RouteKind::Fallback,
                    vec![PathPred::new("/(v1)()(/chat/completions)")],
                    vec![Destination::new("b.static:80")],
                )
                .unwrap(),
            ],
            ..Default::default()
        };
        let t = RouteTable::rebuild(&data).unwrap();
        // Initial requests can only see the Main; fallback only the Fallback.
        let m = t.find_match(Some("k"), "/v1/chat/completions").unwrap();
        assert_eq!(m.matched_by, crate::matcher::MatchKind::HeaderExact);
        let f = t.find_match_fallback(Some("k"), "/v1/chat/completions").unwrap();
        assert_eq!(f.matched_by, crate::matcher::MatchKind::FallbackExact);
        assert_ne!(m.index, f.index);
    }

    #[test]
    fn full_match_anchoring_rejects_suffix() {
        // /(v1)()(/chat/completions) full-matches /v1/chat/completions but not
        // /v1/chat/completions/extra (no trailing .* in the real pattern).
        let data = ConfigData {
            routes: vec![RouteRule::new(
                "m",
                RouteKind::Main,
                vec![PathPred::new("/(v1)()(/chat/completions)")],
                vec![Destination::new("a.static:80")],
            )
            .unwrap()],
            ..Default::default()
        };
        let t = RouteTable::rebuild(&data).unwrap();
        assert!(t.find_match(Some("m"), "/v1/chat/completions").is_some());
        assert!(t.find_match(Some("m"), "/v1/chat/completions/extra").is_none());
        assert!(t.find_match(Some("m"), "/prefix/v1/chat/completions").is_none());
    }

    #[test]
    fn literal_anchor_len_basics() {
        assert_eq!(literal_anchor_len("()chat/completions(/|$)(.*)"), 16);
        assert_eq!(literal_anchor_len("/()model/proxy/\\d+(/|$)(.*)"), 13);
        assert_eq!(literal_anchor_len("/"), 1);
        assert_eq!(literal_anchor_len("()v1/messages(/|$)(.*)"), 11);
        // A leading metachar ('.') yields no anchor.
        assert_eq!(literal_anchor_len("().a(/|$)(.*)"), 0);
        // An empty non-capturing group is stripped; content groups are not.
        assert_eq!(literal_anchor_len("(?:)abc(/)"), 3);
        assert_eq!(literal_anchor_len("(?:x)abc(/)"), 0);
    }

    // ----- validation / sanitize -----

    #[test]
    fn valid_snapshot_has_no_issues() {
        let data = ConfigData {
            routes: vec![
                main_route("m1"),
                RouteRule::new(
                    "gpustack",
                    RouteKind::Mirror,
                    vec![PathPred::new("/")],
                    vec![Destination::new("gpustack.dns:30080")],
                )
                .unwrap(),
            ],
            registries: vec![
                Registry::new("model-1-10.static:80", "10.0.0.5:8081").unwrap(),
                Registry::new("gpustack.dns:30080", "127.0.0.1")
                    .unwrap()
                    .with_port(30080),
            ],
            proxies: vec![],
            ..Default::default()
        };
        assert!(data.validate().is_empty());
        let sr = data.sanitize();
        assert!(sr.issues.is_empty());
        assert_eq!(sr.accepted.routes.len(), 2);
    }

    #[test]
    fn validation_empty_key() {
        let data = ConfigData {
            routes: vec![RouteRule {
                key: String::new(),
                ..main_route("ignored")
            }],
            ..Default::default()
        };
        let issues = data.validate();
        assert!(issues
            .iter()
            .any(|i| i.message.contains("key must be non-empty")));
        // Per-object: the empty-key route is dropped, others kept.
        let sr = data.sanitize();
        assert_eq!(sr.accepted.routes.len(), 0);
    }

    #[test]
    fn validation_bad_endpoint() {
        let bad = RouteRule {
            destinations: vec![Destination::new("svc.unknown:80")],
            ..main_route("m1")
        };
        let good = main_route("m2");
        let data = ConfigData {
            routes: vec![bad, good],
            ..Default::default()
        };
        let issues = data.validate();
        assert!(issues.iter().any(|i| i.message.contains("bad endpoint")));
        // The good route survives while the bad one is dropped.
        let sr = data.sanitize();
        assert_eq!(sr.accepted.routes.len(), 1);
        assert_eq!(sr.accepted.routes[0].key, "m2");
    }

    #[test]
    fn validation_weight_sum() {
        let r = RouteRule {
            destinations: vec![
                Destination::with_percent(60, "a.static:80"),
                Destination::with_percent(30, "b.static:80"),
            ],
            ..main_route("m1")
        };
        let data = ConfigData {
            routes: vec![r],
            ..Default::default()
        };
        let issues = data.validate();
        assert!(issues.iter().any(|i| i.message.contains("sum to 90")));

        // 50/50 is fine.
        let r2 = RouteRule {
            destinations: vec![
                Destination::with_percent(50, "a.static:80"),
                Destination::with_percent(50, "b.static:80"),
            ],
            ..main_route("m1")
        };
        let ok = ConfigData {
            routes: vec![r2],
            ..Default::default()
        };
        assert!(ok.validate().is_empty());
    }

    #[test]
    fn validation_mirror_auth_and_duplicate_keys() {
        let mut mirror = RouteRule::new(
            "gpustack",
            RouteKind::Mirror,
            vec![PathPred::new("/")],
            vec![Destination::new("gpustack.dns:30080")],
        )
        .unwrap();
        mirror.auth_scope.enabled = true;
        let data = ConfigData {
            routes: vec![mirror, main_route("m1"), main_route("m1")],
            ..Default::default()
        };
        let issues = data.validate();
        assert!(issues.iter().any(|i| i.message.contains("mirror")));
        assert!(issues
            .iter()
            .any(|i| i.message.contains("duplicate route key")));
        // Both the authed mirror and the duplicate are dropped; the first m1
        // is kept -> 1 accepted route.
        let sr = data.sanitize();
        assert_eq!(sr.accepted.routes.len(), 1);
        assert_eq!(sr.accepted.routes[0].key, "m1");
    }

    #[test]
    fn validation_fallback_unknown_target() {
        // Fallbacks are derived from RouteRule.fallback; a link whose target
        // (main ingress name = Fallback route key) matches no route is dropped.
        let r = main_route("m1").with_fallback(crate::route::FallbackLink::new("ghost"));
        let good = main_route("m2");
        let data = ConfigData {
            routes: vec![r, good],
            ..Default::default()
        };
        let issues = data.validate();
        assert!(issues.iter().any(|i| i.message.contains("ghost")));
        // The offending route is dropped; the good route is kept.
        let sr = data.sanitize();
        assert_eq!(sr.accepted.routes.len(), 1);
        assert_eq!(sr.accepted.routes[0].key, "m2");
    }

    #[test]
    fn validation_registry_rules() {
        let data = ConfigData {
            registries: vec![
                // dns without port
                Registry {
                    id: "w.dns".into(),
                    kind: ServiceType::Dns,
                    domain: "10.0.0.1".into(),
                    port: None,
                    proxy_ref: None,
                },
                // proxy without ref
                Registry::new("p.proxy:443", "api.example.com").unwrap(),
                // proxy with unknown ref
                Registry::new("q.proxy:443", "api.example.com")
                    .unwrap()
                    .with_proxy_ref("ghost"),
                // a good one
                Registry::new("ok.static:80", "10.0.0.9:80").unwrap(),
            ],
            ..Default::default()
        };
        let issues = data.validate();
        assert!(issues
            .iter()
            .any(|i| i.message.contains("dns registry requires a port")));
        assert!(issues
            .iter()
            .any(|i| i.message.contains("requires proxy_ref")));
        assert!(issues
            .iter()
            .any(|i| i.message.contains("unknown outbound proxy")));
        // Only the good registry survives.
        let sr = data.sanitize();
        assert_eq!(sr.accepted.registries.len(), 1);
        assert_eq!(sr.accepted.registries[0].id, "ok.static");
    }

    #[test]
    fn fallback_spec_derived_from_route_link() {
        // The canonical form is RouteRule.fallback; FallbackSpec is a view.
        let link = crate::route::FallbackLink::new("ai-route-route-5.internal")
            .with_main_ingress_name("higress-system/ai-route-route-5.internal");
        let main = main_route("m1").with_fallback(link);
        let data = ConfigData {
            routes: vec![main],
            ..Default::default()
        };
        let specs = data.fallbacks();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].route_key, "m1");
        assert_eq!(specs[0].target_key, "ai-route-route-5.internal");
        assert_eq!(
            specs[0].main_ingress_name,
            "higress-system/ai-route-route-5.internal"
        );
        assert_eq!(specs[0].max_redirects, 10);
        assert!(specs[0].use_original_body && specs[0].use_original_uri);
    }

    // ----- SharedConfig -----

    #[test]
    fn shared_config_drops_bad_objects_and_keeps_good() {
        // Per-object issue (empty-key route) is dropped, others kept, and new
        // succeeds (no structural rejection).
        let mixed = ConfigData {
            routes: vec![
                RouteRule {
                    key: String::new(),
                    ..main_route("bad")
                },
                main_route("good1"),
                main_route("good2"),
            ],
            ..Default::default()
        };
        let sc = SharedConfig::new(mixed.clone()).unwrap();
        assert_eq!(sc.load().routes.len(), 2); // bad dropped

        // Structural failure (invalid regex) rejects the whole snapshot.
        let structural = ConfigData {
            routes: vec![RouteRule::new(
                "m",
                RouteKind::Main,
                vec![PathPred::new("([unclosed")],
                vec![Destination::new("a.static:80")],
            )
            .unwrap()],
            ..Default::default()
        };
        assert!(SharedConfig::new(structural).is_err());

        // Store a mixed snapshot: good objects kept, bad dropped, then the
        // current snapshot reflects only the good ones.
        assert!(
            sc.store(ConfigData {
                routes: vec![
                    RouteRule {
                        key: String::new(),
                        ..main_route("bad2")
                    },
                    main_route("a"),
                    main_route("b"),
                ],
                ..Default::default()
            })
            .is_ok()
        );
        assert_eq!(sc.load().routes.len(), 2);
        // Storing an all-good (even empty) snapshot is fine.
        assert!(sc.store(ConfigData::default()).is_ok());
        assert_eq!(sc.load().routes.len(), 0);
    }

    #[test]
    fn shared_config_swrr_group_state_shared() {
        let sc = SharedConfig::new(ConfigData {
            routes: vec![main_route("m1")],
            ..Default::default()
        })
        .unwrap();
        let services = vec!["model-1-10.static:80".to_string()];
        // Order-insensitive: the same (route, dest set) resolves to one state.
        sc.swrr_group_state("m1", &services)
            .or_default()
            .current_weights
            .insert("model-1-10.static:80".to_string(), 5);
        assert_eq!(
            sc.swrr_group_state_ref("m1", &services)
                .unwrap()
                .current_weights
                .get("model-1-10.static:80"),
            Some(&5)
        );
        // A different route key is a different group (no bleed).
        assert!(sc.swrr_group_state_ref("m2", &services).is_none());
    }

    #[test]
    fn shared_config_multi_candidate_weighted_sequence() {
        // One shared SwrrState for the (route, dest-group); weights 5/1/1
        // produce the Nginx-deterministic smooth sequence through the shared
        // config.
        let route = RouteRule::new(
            "m1",
            RouteKind::Main,
            vec![PathPred::new("/(v1)()(/chat/completions)")],
            vec![
                Destination::with_percent(5, "a.static:80"),
                Destination::with_percent(1, "b.static:80"),
                Destination::with_percent(1, "c.static:80"),
            ],
        )
        .unwrap();
        let sc = SharedConfig::new(ConfigData {
            routes: vec![route],
            ..Default::default()
        })
        .unwrap();
        let services = vec![
            "a.static:80".to_string(),
            "b.static:80".to_string(),
            "c.static:80".to_string(),
        ];
        let mut candidates = vec![
            crate::swrr::SwrrCandidate::new("a.static:80", 5),
            crate::swrr::SwrrCandidate::new("b.static:80", 1),
            crate::swrr::SwrrCandidate::new("c.static:80", 1),
        ];
        let mut guard = sc.swrr_group_state("m1", &services).or_default();
        let mut seq = Vec::new();
        for _ in 0..7 {
            crate::swrr::order(&mut candidates, &mut guard);
            seq.push(candidates[0].id.clone());
        }
        assert_eq!(
            seq,
            vec![
                "a.static:80",
                "a.static:80",
                "b.static:80",
                "a.static:80",
                "c.static:80",
                "a.static:80",
                "a.static:80"
            ]
        );
    }

    #[test]
    fn shared_config_prunes_stale_swrr_state_on_swap() {
        let sc = SharedConfig::new(ConfigData {
            routes: vec![main_route("m1")],
            ..Default::default()
        })
        .unwrap();
        let services = vec!["model-1-10.static:80".to_string()];
        sc.swrr_group_state("m1", &services).or_default().current_weights.insert(
            "model-1-10.static:80".to_string(),
            7,
        );
        // Swap in a snapshot where "m1" is gone -> its state is pruned.
        assert!(sc
            .store(ConfigData {
                routes: vec![main_route("m2")],
                ..Default::default()
            })
            .is_ok());
        assert!(sc.swrr_group_state_ref("m1", &services).is_none());
        // ... while a surviving group keeps its state.
        let sc2 = SharedConfig::new(ConfigData {
            routes: vec![main_route("m1")],
            ..Default::default()
        })
        .unwrap();
        sc2.swrr_group_state("m1", &services).or_default().current_weights.insert(
            "model-1-10.static:80".to_string(),
            9,
        );
        // Same route + same group survives a same-content store.
        assert!(sc2.store(ConfigData {
            routes: vec![main_route("m1")],
            ..Default::default()
        }).is_ok());
        assert_eq!(
            sc2.swrr_group_state_ref("m1", &services)
                .unwrap()
                .current_weights
                .get("model-1-10.static:80"),
            Some(&9)
        );
    }

    #[test]
    fn shared_config_route_table() {
        let sc = SharedConfig::new(ConfigData {
            routes: vec![main_route("m1")],
            ..Default::default()
        })
        .unwrap();
        let t = sc.route_table().unwrap();
        assert_eq!(t.routes().len(), 1);
    }

    #[test]
    fn timing_config_defaults() {
        let t = TimingConfig::default();
        assert_eq!(t.downstream_idle_timeout_secs, 1800);
        assert_eq!(t.upstream_idle_timeout_secs, 10);
        // serde defaults fill omitted fields (ConfigMap partial form).
        let v: TimingConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(v.upstream_idle_timeout_secs, 10);
        assert_eq!(v.downstream_idle_timeout_secs, 1800);
    }

    #[test]
    fn model_router_settings_defaults() {
        let s = ModelRouterSettings::default();
        assert_eq!(s.prefix, "/model/proxy/");
        assert_eq!(s.target_header, "x-higress-llm-model");
        assert!(s.enable_on_path_suffix.is_empty());
        assert!(s.alias_name_mapping.is_empty());
        assert_eq!(s.max_body_bytes, None);
        // serde defaults fill omitted keys (an empty defaultConfig / `{}`).
        let v: ModelRouterSettings = serde_json::from_str("{}").unwrap();
        assert_eq!(v, ModelRouterSettings::default());
    }

    /// A realistic GPUStack-shaped `gpustack-model-router` `defaultConfig`
    /// (plugin-contract-pin §2.3: `prefix`/`targetHeader`/`enableOnPathSuffix`
    /// non-empty / `aliasNameMapping` / `maxBodyBytes`).
    const MODEL_ROUTER_DEFAULT_CONFIG: &str = r#"{
        "prefix": "/model/proxy/",
        "targetHeader": "x-higress-llm-model",
        "enableOnPathSuffix": [
            "/v1/chat/completions",
            "/v1/completions",
            "/v1/embeddings",
            "/v1/responses",
            "/v1/messages"
        ],
        "aliasNameMapping": { "1": "route-one", "2": "route-two" },
        "maxBodyBytes": 104857600,
        "modelKey": "model",
        "autoRoutingEnabled": false
    }"#;

    #[test]
    fn model_router_settings_serde_round_trip() {
        // Deserialize the real wire shape (camelCase keys; unknown keys ignored).
        let s: ModelRouterSettings = serde_json::from_str(MODEL_ROUTER_DEFAULT_CONFIG).unwrap();
        assert_eq!(s.prefix, "/model/proxy/");
        assert_eq!(s.target_header, "x-higress-llm-model");
        assert_eq!(
            s.enable_on_path_suffix,
            vec![
                "/v1/chat/completions",
                "/v1/completions",
                "/v1/embeddings",
                "/v1/responses",
                "/v1/messages"
            ]
        );
        assert_eq!(
            s.alias_name_mapping.get("1"),
            Some(&"route-one".to_string())
        );
        assert_eq!(
            s.alias_name_mapping.get("2"),
            Some(&"route-two".to_string())
        );
        assert_eq!(s.max_body_bytes, Some(104857600));

        // Round-trip: serialize then re-deserialize yields the same value.
        let serialized = serde_json::to_value(&s).unwrap();
        let round: ModelRouterSettings = serde_json::from_value(serialized).unwrap();
        assert_eq!(s, round);
    }

    #[test]
    fn configdata_builds_with_and_without_model_router() {
        // "With": a non-default model_router is stored, sanitized and read back.
        let with = ConfigData {
            model_router: ModelRouterSettings {
                prefix: "/model/proxy/".into(),
                target_header: "x-higress-llm-model".into(),
                enable_on_path_suffix: vec!["/v1/chat/completions".into()],
                alias_name_mapping: {
                    let mut m = BTreeMap::new();
                    m.insert("1".to_string(), "route-one".to_string());
                    m
                },
                max_body_bytes: Some(1024),
            },
            ..Default::default()
        };
        // No routes -> no structural failure; builds fine.
        let sc = SharedConfig::new(with.clone()).unwrap();
        assert_eq!(sc.load().model_router.max_body_bytes, Some(1024));
        assert_eq!(sc.load().model_router.alias_name_mapping.len(), 1);

        // "Without": an empty ConfigData keeps the default model_router.
        let without = ConfigData::default();
        let sc2 = SharedConfig::new(without).unwrap();
        assert_eq!(sc2.load().model_router, ModelRouterSettings::default());

        // Round-trips the field, and an "old" payload that lacks the `model_router` key
        // still deserializes to the default (additive: existing snapshots keep working).
        let rt: ConfigData = serde_json::from_value(serde_json::to_value(&with).unwrap()).unwrap();
        assert_eq!(rt.model_router.max_body_bytes, Some(1024));
        let mut old = serde_json::to_value(&with).unwrap();
        old.as_object_mut().unwrap().remove("model_router");
        let v: ConfigData = serde_json::from_value(old).unwrap();
        assert_eq!(v.model_router, ModelRouterSettings::default());
    }

    // ----- provider tokens (D6 / §7) -----

    #[test]
    fn provider_token_serde_defaults_and_round_trip() {
        // Absent keys -> field defaults (additive: an "old" snapshot / `{}`).
        // `ProviderToken` is built by the adapter (not deserialized from GPUStack's
        // `ai-proxy` wire directly), so its field names are the internal snapshot
        // form (snake_case).
        let v: ProviderToken = serde_json::from_str(r#"{"service":"provider-1.proxy","api_tokens":["sk-a"]}"#)
            .unwrap();
        assert_eq!(v.service, "provider-1.proxy");
        assert_eq!(v.ingress_scope, None);
        assert_eq!(v.api_tokens, vec!["sk-a".to_string()]);

        // Round-trips a fully-populated entry.
        let p = ProviderToken {
            service: "provider-2.dns".into(),
            ingress_scope: Some("ai-route-route-5.internal".into()),
            api_tokens: vec!["sk-1".into(), "sk-2".into()],
        };
        let rt: ProviderToken =
            serde_json::from_value(serde_json::to_value(&p).unwrap()).unwrap();
        assert_eq!(p, rt);
    }

    #[test]
    fn configdata_builds_with_and_without_provider_tokens() {
        // With: stored, sanitized, read back through the snapshot.
        let with = ConfigData {
            provider_tokens: vec![ProviderToken {
                service: "provider-1.proxy".into(),
                ingress_scope: None,
                api_tokens: vec!["sk-provider-1".into()],
            }],
            ..Default::default()
        };
        let sc = SharedConfig::new(with.clone()).unwrap();
        assert_eq!(sc.load().provider_tokens.len(), 1);

        // Without: an "old" payload lacking the `provider_tokens` key -> empty.
        let mut old = serde_json::to_value(&with).unwrap();
        old.as_object_mut().unwrap().remove("provider_tokens");
        let v: ConfigData = serde_json::from_value(old).unwrap();
        assert!(v.provider_tokens.is_empty());
    }

    #[test]
    fn provider_token_validation_drops_bad_entries() {
        let data = ConfigData {
            provider_tokens: vec![
                // good
                ProviderToken {
                    service: "provider-9.proxy".into(),
                    ingress_scope: None,
                    api_tokens: vec!["sk-9".into()],
                },
                // bad: empty service
                ProviderToken {
                    service: String::new(),
                    ingress_scope: None,
                    api_tokens: vec!["sk-x".into()],
                },
                // bad: non-service characters in the id
                ProviderToken {
                    service: "provider/7!proxy".into(),
                    ingress_scope: None,
                    api_tokens: vec!["sk-y".into()],
                },
                // bad: no tokens
                ProviderToken {
                    service: "provider-8.dns".into(),
                    ingress_scope: None,
                    api_tokens: Vec::new(),
                },
            ],
            ..Default::default()
        };
        let sr = data.sanitize();
        // Exactly the good entry survives; the three bad ones are dropped w/ issues.
        assert_eq!(sr.accepted.provider_tokens.len(), 1);
        assert_eq!(sr.accepted.provider_tokens[0].service, "provider-9.proxy");
        assert_eq!(sr.issues.len(), 3);
        assert!(sr.issues.iter().any(|i| i.message.contains("service must be a valid")));
        assert!(sr.issues.iter().any(|i| i.message.contains("has no apiTokens")));
    }

    #[test]
    fn provider_token_resolves_by_service_and_ingress() {
        let data = ConfigData {
            provider_tokens: vec![
                // global (no ingress scope)
                ProviderToken {
                    service: "provider-1.proxy".into(),
                    ingress_scope: None,
                    api_tokens: vec!["global-key".into()],
                },
                // ingress-scoped, wins when the ingress matches
                ProviderToken {
                    service: "provider-1.proxy".into(),
                    ingress_scope: Some("higress-system/ai-route-route-5.internal".into()),
                    api_tokens: vec!["scoped-key".into()],
                },
                // a different service
                ProviderToken {
                    service: "provider-2.dns".into(),
                    ingress_scope: None,
                    api_tokens: vec!["prov2-key".into()],
                },
            ],
            ..Default::default()
        };
        // Scoped match (ns-qualified ingress): the scoped token wins.
        assert_eq!(
            data.provider_token("provider-1.proxy", "higress-system/ai-route-route-5.internal"),
            Some("scoped-key")
        );
        // Same service, ingress without a matching scope -> global token.
        assert_eq!(
            data.provider_token("provider-1.proxy", "higress-system/ai-route-route-7.internal"),
            Some("global-key")
        );
        // Bare ingress name matches the ns-qualified scope (last segment).
        assert_eq!(
            data.provider_token("provider-1.proxy", "ai-route-route-5.internal"),
            Some("scoped-key")
        );
        // Different service.
        assert_eq!(
            data.provider_token("provider-2.dns", "higress-system/ai-route-route-7.internal"),
            Some("prov2-key")
        );
        // Unknown service -> None.
        assert_eq!(data.provider_token("provider-3.dns", "x"), None);
        // Empty snapshot -> always None.
        assert_eq!(ConfigData::default().provider_token("provider-1.proxy", "x"), None);
    }

    #[test]
    fn fallback_spec_defaults() {
        let f: FallbackSpec =
            serde_json::from_str("{\"route_key\":\"a\",\"target_key\":\"b\"}").unwrap();
        assert_eq!(f.max_redirects, 10);
        assert!(f.use_original_body && f.use_original_uri);
        // main_ingress_name is optional in the wire form (defaults to empty).
        assert_eq!(f.main_ingress_name, "");
    }

    impl Registry {
        fn with_port(mut self, port: u16) -> Self {
            self.port = Some(port);
            self
        }
    }
}
