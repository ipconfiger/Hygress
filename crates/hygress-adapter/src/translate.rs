//! Pure translation of GPUStack-written Higress CRD JSON into `hygress_core` types.
//!
//! Every function here is **pure**: it operates over a [`serde_json::Value`] (the exact wire
//! shape of a k8s object as it arrives from the apiserver) plus small metadata and returns the
//! corresponding [`hygress_core`] types. There is **no cluster access and no I/O**, so the whole
//! translation is fully unit-testable with real recorded fixture JSON (see the `tests` module),
//! independent of any running kube-apiserver.
//!
//! Translation contract (docs/design.md §5.3; docs/research/plugin-contract-pin.md §2/§4):
//! - `Ingress` `ai-route-route-<id>.internal`     → [`RouteKind::Main`]
//! - `Ingress` `ai-route-route-<id>.fallback...`  → [`RouteKind::Fallback`]
//! - `Ingress` `gpustack` (mirror, no `pct%`)     → [`RouteKind::Mirror`]
//!   (legacy `ai-route-model-*` are ignore/cleanup-only)
//! - `McpBridge default` `spec.registries[]`      → [`Registry`] (static/dns/proxy/tunnel)
//! - `McpBridge default` `spec.proxies[]`         → [`OutboundProxy`]
//! - `WasmPlugin gpustack-model-mapper` per-rule `matchRules` → [`hygress_core::model_mapping::ModelMapping`] (keyed by
//!   `name.type`, merged into the route whose ingress scope matches)
//! - `EnvoyFilter ai-route-route-<id>`            → [`FallbackLink`] (4xx/5xx redirect)
//! - `Secret gpustack-tls-*`                       → [`TlsHost`] (`tls.crt`/`tls.key`, base64)
//! - `ConfigMap higress-config`                    → [`TimingConfig`] (`idleTimeout` keys)
//!
//! Managed-object filter: only objects with the label
//! [`MANAGED_LABEL`] = `gpustack.ai/managed=true` are consumed; the unmanaged global
//! custom-response `EnvoyFilter` is ignored because it lacks the label.

use std::collections::BTreeSet;

use base64::Engine;
use serde_json::{Map, Value};

use hygress_core::{
    parse_destinations, ConfigData, Error, FallbackLink, GatewayFeatureConfig, ModelRouterSettings,
    OutboundProxy, PathPred, ProviderToken, Registry, RouteKind, RouteRule, RuleSource, RetryPolicy,
    TlsConfig, TlsHost, TimingConfig,
};

/// GPUStack's managed-object label (design §2.1.1). The seed global custom-response
/// EnvoyFilter deliberately carries **no** such label, so the label-selector list never
/// expects to encounter it.
pub const MANAGED_LABEL: (&str, &str) = ("gpustack.ai/managed", "true");

/// Default mirror ingress name (`GATEWAY_MIRROR_INGRESS_NAME`, design §2.1.1 / §4.3).
pub const MIRROR_NAME: &str = "gpustack";

/// The single McpBridge GPUStack writes (`default_mcp_bridge_name`).
pub const MCP_BRIDGE_NAME: &str = "default";

/// The gateway `ConfigMap` carrying timeout/limit settings.
pub const HIGRESS_CONFIG_MAP: &str = "higress-config";

/// Prefix of the TLS `Secret`s the data plane consumes.
pub const TLS_SECRET_PREFIX: &str = "gpustack-tls-";

/// `higress.io/*` ingress annotations (design §2.1.2 / plugin-contract-pin §3.2).
mod annot {
    pub const DESTINATION: &str = "higress.io/destination";
    pub const REWRITE_TARGET: &str = "higress.io/rewrite-target";
    pub const IGNORE_PATH_CASE: &str = "higress.io/ignore-path-case";
    pub const PROXY_NEXT_UPSTREAM: &str = "higress.io/proxy-next-upstream";
    pub const PROXY_NEXT_UPSTREAM_TRIES: &str = "higress.io/proxy-next-upstream-tries";
    /// `higress.io/exact-match-header-x-higress-llm-model` — core model-route match.
    pub const EXACT_MATCH_LLM_MODEL: &str = "higress.io/exact-match-header-x-higress-llm-model";
    /// `higress.io/exact-match-header-x-higress-fallback-from` — fallback ingress matcher.
    pub const EXACT_MATCH_FALLBACK_FROM: &str =
        "higress.io/exact-match-header-x-higress-fallback-from";
}

/// WasmPlugin resource name that carries per-destination `modelMapping` rules.
const MODEL_MAPPER_NAME: &str = "gpustack-model-mapper";

/// WasmPlugin resource name of the generic-proxy-router (the model resolver;
/// plugin-contract-pin §2.3). Its `defaultConfig` is translated into the typed
/// [`ModelRouterSettings`] (hot-reloadable).
const MODEL_ROUTER_NAME: &str = "gpustack-model-router";

/// WasmPlugin resource name of the `ai-proxy` plugin (provider egress key-swap;
/// design D6 / §7). Its `defaultConfig.providers[]` + `matchRules[]` are
/// flattened into the per-destination [`ProviderToken`] list.
const AI_PROXY_NAME: &str = "gpustack-ai-proxy";

/// The `x-higress-fallback-from` header injected by the fallback EnvoyFilter redirect.
const FALLBACK_FROM_VALUE_HEADER: &str = "x-higress-fallback-from";

/// The Envoy custom-response filter key inside a fallback EnvoyFilter's `typed_per_filter_config`.
const CUSTOM_RESPONSE_FILTER: &str = "envoy.filters.http.custom_response";

/// Managed WasmPlugin resource names Hygress consumes (pin §1; typed native
/// equivalents). A managed plugin OUTSIDE this set is a future/unknown plugin
/// whose behavior Hygress does not reproduce — surfaced once per pass (R-10 /
/// C1, fail-open).
const KNOWN_WASM_PLUGINS: &[&str] = &[
    "gpustack-llm-ext-auth",
    "gpustack-ai-statistics",
    "gpustack-model-router",
    "gpustack-ai-proxy",
    "gpustack-set-model-pre-route",
    "gpustack-model-mapper",
    "gpustack-header-transformer",
    "gpustack-token-usage",
];

/// R-10 / C1: warn (once per object) about `defaultConfig` / rule keys Hygress
/// does not consume — GPUStack upgrade drift becomes discoverable instead of
/// silently ignored. Fail-open: never rejects, never changes behavior.
fn warn_unknown_keys(value: &Value, known: &[&str], what: &str) {
    let Some(obj) = value.as_object() else {
        return;
    };
    let unknown: Vec<String> = obj
        .keys()
        .filter(|k| !known.contains(&k.as_str()))
        .cloned()
        .collect();
    if !unknown.is_empty() {
        tracing::warn!(
            what,
            unknown_keys = ?unknown,
            "unconsumed config keys (possible GPUStack upgrade drift; C1) — native typed equivalent is authoritative"
        );
    }
}

// ---------------------------------------------------------------------------
// Object model (input to the pure translation)
// ---------------------------------------------------------------------------

/// The kind of a k8s object being translated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ObjectKind {
    /// A Higress `McpBridge` CRD object (`networking.higress.io/v1`).
    McpBridge,
    /// A Higress `WasmPlugin` CRD object (`extensions.higress.io/v1alpha1`).
    WasmPlugin,
    /// An Istio `EnvoyFilter` CRD object (`networking.istio.io/v1alpha3`).
    EnvoyFilter,
    /// A `networking.k8s.io/v1` `Ingress` object (the GPUStack routes).
    Ingress,
    /// A `core/v1` `Secret` object (the data-plane TLS secrets).
    Secret,
    /// A `core/v1` `ConfigMap` object (the gateway `higress-config` timing map).
    ConfigMap,
}

/// One k8s object as read from the cluster: its identity (`name`/`namespace`/`uid`/
/// `resourceVersion`) plus the raw wire JSON. This is the unit the kube layer hands to the
/// pure translation in [`build_config_data`].
#[derive(Clone, Debug)]
pub struct Object {
    /// The k8s kind of the object (selects the translation below).
    pub kind: ObjectKind,
    /// `metadata.name` of the object.
    pub name: String,
    /// `metadata.namespace` of the object.
    pub namespace: String,
    /// `metadata.uid` of the object (provenance; may be empty when absent).
    pub uid: String,
    /// `metadata.resourceVersion`, parsed as a `u64` (provenance; 0 when absent).
    pub resource_version: u64,
    /// The full object as it appears on the wire (e.g. `{apiVersion, kind, metadata, spec}`).
    pub value: Value,
}

impl Object {
    /// Convenience constructor.
    pub fn new(
        kind: ObjectKind,
        name: impl Into<String>,
        namespace: impl Into<String>,
        uid: impl Into<String>,
        resource_version: u64,
        value: Value,
    ) -> Self {
        Self {
            kind,
            name: name.into(),
            namespace: namespace.into(),
            uid: uid.into(),
            resource_version,
            value,
        }
    }

    /// `true` when the object carries the GPUStack managed label.
    pub fn is_managed(&self) -> bool {
        let labels = self
            .value
            .get("metadata")
            .and_then(|m| m.get("labels"))
            .and_then(|l| l.as_object());
        match labels {
            Some(labels) => labels.get(MANAGED_LABEL.0) == Some(&Value::String(MANAGED_LABEL.1.to_string())),
            None => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Generic JSON helpers
// ---------------------------------------------------------------------------

/// Read an optional string annotation from `metadata.annotations`.
fn annotation(value: &Value, key: &str) -> Option<String> {
    value
        .get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get(key))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Read a `metadata.resourceVersion` as a `u64` (0 when absent/non-numeric).
pub fn resource_version_of(value: &Value) -> u64 {
    value
        .get("metadata")
        .and_then(|m| m.get("resourceVersion"))
        .and_then(|rv| rv.as_str().and_then(|s| s.parse().ok()).or_else(|| rv.as_u64()))
        .unwrap_or(0)
}

/// Read `metadata.uid` (empty when absent).
pub fn uid_of(value: &Value) -> String {
    value
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

/// Read `metadata.name` (empty when absent).
pub fn name_of(value: &Value) -> String {
    value
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

// ---------------------------------------------------------------------------
// Ingress → RouteRule
// ---------------------------------------------------------------------------

/// Classify an ingress name into a route kind.
///
/// Returns `None` for names that are not a managed GPUStack route: this deliberately covers the
/// legacy `ai-route-model-<id>` forms (cleanup-only, never expected in the list) and any
/// unmanaged ingress whose shape we do not translate.
pub fn classify_ingress_name(name: &str, mirror_name: &str) -> Option<RouteKind> {
    // Fallback: `<main>.fallback.internal`.
    if name.ends_with(".fallback.internal") {
        return Some(RouteKind::Fallback);
    }
    // Main: `ai-route-route-<id>.internal`.
    if name.starts_with("ai-route-route-") && name.ends_with(".internal") {
        return Some(RouteKind::Main);
    }
    // Mirror: the GPUStack self ingress (e.g. `gpustack`), no `pct%` destinations.
    if name == mirror_name {
        return Some(RouteKind::Mirror);
    }
    // `ai-route-model-<id>` and anything else: ignore (legacy/cleanup-only / not ours).
    None
}

/// Collect the ingress path regex predicates (from `spec.rules[*].http.paths[*]`).
///
/// GPUStack may add a `host` rule that is a deep copy of the default rule; predicates are
/// de-duplicated by their path string (order preserved) so the two do not double up. The
/// `ignore_case` flag is uniform per ingress (`higress.io/ignore-path-case`).
fn ingress_path_predicates(value: &Value) -> Vec<PathPred> {
    let ignore_case = annotation(value, annot::IGNORE_PATH_CASE).as_deref() == Some("true");
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut preds = Vec::new();
    if let Some(rules) = value.get("spec").and_then(|s| s.get("rules")).and_then(|r| r.as_array()) {
        for rule in rules {
            let Some(paths) = rule
                .get("http")
                .and_then(|h| h.get("paths"))
                .and_then(|p| p.as_array())
            else {
                continue;
            };
            for path in paths {
                let Some(p) = path.get("path").and_then(|v| v.as_str()) else {
                    continue;
                };
                if p.is_empty() {
                    continue;
                }
                if seen.insert(p.to_string()) {
                    preds.push(PathPred {
                        regex: p.to_string(),
                        ignore_case,
                    });
                }
            }
        }
    }
    preds
}

/// Translate one managed ingress into a [`RouteRule`].
///
/// `kind` is expected to be the value of
/// [`classify_ingress_name`] for `obj.name`; the match key is taken from the corresponding
/// `higress.io/exact-match-header-*` annotation (Main: `x-higress-llm-model`, Fallback:
/// `x-higress-fallback-from`). The `ingress_name` provenance identity is stored ns-qualified as
/// `<namespace>/<name>` (design §6.2 / §9).
pub fn ingress_to_route(
    obj: &Object,
    kind: RouteKind,
    gateway_namespace: &str,
) -> Result<RouteRule, Error> {
    let value = &obj.value;

    let key = match kind {
        RouteKind::Main => annotation(value, annot::EXACT_MATCH_LLM_MODEL)
            .ok_or_else(|| {
                Error::invalid(format!(
                    "ingress '{}' (Main) is missing the {} annotation",
                    obj.name,
                    annot::EXACT_MATCH_LLM_MODEL
                ))
            })?,
        RouteKind::Fallback => annotation(value, annot::EXACT_MATCH_FALLBACK_FROM)
            .ok_or_else(|| {
                Error::invalid(format!(
                    "ingress '{}' (Fallback) is missing the {} annotation",
                    obj.name,
                    annot::EXACT_MATCH_FALLBACK_FROM
                ))
            })?,
        // Mirror: the key is the mirror ingress name itself.
        RouteKind::Mirror => obj.name.clone(),
    };

    let destination_block = annotation(value, annot::DESTINATION)
        .ok_or_else(|| Error::invalid(format!("ingress '{}' has no destination annotation", obj.name)))?;
    let destinations = parse_destinations(&destination_block)?;
    if destinations.is_empty() {
        return Err(Error::invalid(format!(
            "ingress '{}' has an empty destination list",
            obj.name
        )));
    }

    // `higress.io/rewrite-target` is carried through as the raw capture template
    // (`with_rewrite_target` builds the `PathRewriter`).
    let rewrite = annotation(value, annot::REWRITE_TARGET);

    let retry = RetryPolicy::parse(
        annotation(value, annot::PROXY_NEXT_UPSTREAM).as_deref(),
        annotation(value, annot::PROXY_NEXT_UPSTREAM_TRIES).as_deref(),
    )
    .policy;

    // Origin identity as GPUStack writes it (design §6.2/§9). A namespaced
    // object is `<namespace>/<name>`; the namespace falls back to
    // `gateway_namespace` when the object does not carry one (should not happen
    // for namespaced resources).
    let namespace = if obj.namespace.is_empty() {
        gateway_namespace
    } else {
        obj.namespace.as_str()
    };
    // D9 (pin §5.2): the **embedded** case — the ingress lives in the gateway's
    // own namespace — is recorded **bare** (`ai-route-route-<id>.internal`); a
    // name in a *distinct* namespace is ns-qualified (`<ns>/<name>`). This is
    // the value the data plane places into `X-GPUStack-Route-Name`.
    let ingress_name = if namespace.is_empty() || namespace == gateway_namespace {
        obj.name.clone()
    } else {
        format!("{namespace}/{}", obj.name)
    };

    let rule = RouteRule::new(key, kind, ingress_path_predicates(value), destinations)?
        .with_ingress_name(ingress_name.clone())
        .with_retry(retry)
        .with_source(
            RuleSource::new(obj.uid.clone(), obj.resource_version)
                .with_ingress_name(ingress_name),
        );

    let rule = match rewrite {
        Some(r) => rule.with_rewrite_target(r),
        None => rule,
    };
    rule.validate()?;
    Ok(rule)
}

// ---------------------------------------------------------------------------
// McpBridge → Registry / OutboundProxy
// ---------------------------------------------------------------------------

/// Translate one `McpBridge` into its `spec.registries` (as [`Registry`]) and
/// `spec.proxies` (as [`OutboundProxy`]).
///
/// A registry `name`/`type` pair forms the `name.type[:port]` service id (matching
/// `McpBridgeRegistry.get_service_name[_with_port]()`), which the core parses into the `id`,
/// `kind` and `port`. A `proxy` registry references an outbound proxy by `proxyName`.
pub fn mcpbridge_to_registries(obj: &Object) -> Result<(Vec<Registry>, Vec<OutboundProxy>), Error> {
    let spec = obj.value.get("spec").cloned().unwrap_or(Value::Null);

    let mut proxies = Vec::new();
    if let Some(list) = spec.get("proxies").and_then(|p| p.as_array()) {
        for p in list {
            let name = p.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if name.is_empty() {
                continue;
            }
            let server_address = p
                .get("serverAddress")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let server_port = p.get("serverPort").and_then(|v| v.as_u64()).unwrap_or(80) as u16;
            let connect_timeout_ms = p.get("connectTimeout").and_then(|v| v.as_u64());
            let listener_port = p.get("listenerPort").and_then(|v| v.as_u64()).map(|v| v as u16);
            let kind = p.get("type").and_then(|v| v.as_str()).map(str::to_string);
            let mut proxy = OutboundProxy::new(name, server_address, server_port);
            if let Some(ms) = connect_timeout_ms {
                proxy = proxy.with_timeout((ms / 1000).max(1) as u32);
            }
            if let Some(k) = kind {
                proxy = proxy.with_kind(k);
            }
            // `listenerPort` is not an `OutboundProxy` builder field; attach it
            // directly (field retained for wire fidelity; no data-plane consumer
            // — R-9④).
            proxies.push(with_listener_port(proxy, listener_port));
        }
    }

    let mut registries = Vec::new();
    if let Some(list) = spec.get("registries").and_then(|r| r.as_array()) {
        for r in list {
            let rname = r.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let rtype = r.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if rname.is_empty() || rtype.is_empty() {
                continue;
            }
            let domain = r
                .get("domain")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let port = r.get("port").and_then(|v| v.as_u64()).map(|v| v as u16);

            // name.type[:port] — the service id the registries/matchRules key off.
            let service = match port {
                Some(p) => format!("{}.{}:{}", rname, rtype, p),
                None => format!("{}.{}", rname, rtype),
            };
            let mut reg = Registry::new(&service, domain)?;
            if let Some(p) = r.get("proxyName").and_then(|v| v.as_str()) {
                reg = reg.with_proxy_ref(p);
            }
            registries.push(reg);
        }
    }

    Ok((registries, proxies))
}

/// Attach a `listenerPort` to an [`OutboundProxy`] (not exposed as a builder in core).
fn with_listener_port(mut proxy: OutboundProxy, port: Option<u16>) -> OutboundProxy {
    proxy.listener_port = port;
    proxy
}

// ---------------------------------------------------------------------------
// WasmPlugin → gateway feature config + model-mapping rules
// ---------------------------------------------------------------------------

/// Translate one `WasmPlugin` into a [`GatewayFeatureConfig`] (the plugin
/// name/phase/priority/immutable `defaultConfigDisable`). The raw
/// `defaultConfig` spec is NOT retained (R-9① — it can carry provider
/// `apiTokens` / the derived gateway token, and nothing consumes it: typed
/// equivalents are implemented natively).
pub fn wasmplugin_to_feature(obj: &Object) -> GatewayFeatureConfig {
    let spec = obj.value.get("spec").cloned().unwrap_or(Value::Null);
    let phase = spec.get("phase").and_then(|v| v.as_str()).unwrap_or("UNSPECIFIED_PHASE");
    let priority = spec.get("priority").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let fail_open = spec
        .get("failStrategy")
        .and_then(|v| v.as_str())
        .map(|s| s == "FAIL_OPEN")
        .unwrap_or(false);
    let default_config_disable = spec
        .get("defaultConfigDisable")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    GatewayFeatureConfig {
        plugin: obj.name.clone(),
        phase: phase.to_string(),
        priority,
        fail_open,
        default_config_disable,
    }
}

/// One model-mapper match rule, as emitted by GPUStack's
/// `gpustack-model-mapper` plugin: an `ingress` scope (one or more ingress names) plus a list of
/// `service` names (each `name.type`, **no port**) and the outbound `model` name to rewrite to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelMappingRule {
    /// Ingress names this rule applies to (may be ns-prefixed).
    pub ingress: Vec<String>,
    /// `name.type` service keys (no port).
    pub services: Vec<String>,
    /// The outbound body model name to rewrite to.
    pub model: String,
}

/// Extract the per-destination model-mapping rules from a `gpustack-model-mapper`
/// `WasmPlugin`'s `spec.matchRules`.
///
/// Each rule's `config.modelMapping` is `{ <route_name>: <effective_model_name> }` (a single
/// pair); the rule applies to every `service` in `rule.service`. For any other plugin (or an
/// empty `matchRules` list) this returns an empty vector.
pub fn wasmplugin_model_mapping(obj: &Object) -> Vec<ModelMappingRule> {
    if obj.name != MODEL_MAPPER_NAME {
        return Vec::new();
    }
    let Some(rules) = obj
        .value
        .get("spec")
        .and_then(|s| s.get("matchRules"))
        .and_then(|r| r.as_array())
    else {
        return Vec::new();
    };

    let mut out = Vec::new();
    for rule in rules {
        // R-10 / C1: warn on unknown rule keys (GPUStack upgrade drift).
        warn_unknown_keys(
            rule,
            &["config", "service", "ingress", "domain", "configDisable"],
            "gpustack-model-mapper matchRule",
        );
        let Some(model) = rule
            .get("config")
            .and_then(|c| c.get("modelMapping"))
            .and_then(|m| m.as_object())
            .and_then(|o| o.values().next())
            .and_then(|v| v.as_str())
        else {
            continue;
        };
        let model = model.to_string();
        if model.is_empty() {
            continue;
        }
        let services = rule
            .get("service")
            .and_then(|s| s.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let ingress = rule
            .get("ingress")
            .and_then(|s| s.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if services.is_empty() {
            continue;
        }
        out.push(ModelMappingRule {
            ingress,
            services,
            model,
        });
    }
    out
}

/// Translate the `gpustack-model-router` WasmPlugin's `spec.defaultConfig` into a typed
/// [`ModelRouterSettings`] (fix B2; plugin-contract-pin §2.3).
///
/// Returns `None` for any plugin other than [`MODEL_ROUTER_NAME`]; for the model-router it
/// returns `Some` (the parsed `defaultConfig`, or the core defaults when the `defaultConfig`
/// is absent / malformed). Parsing the **real** wire keys (`prefix`, `targetHeader`,
/// `enableOnPathSuffix`, `aliasNameMapping`, `maxBodyBytes`) is done by the serde derive on
/// [`ModelRouterSettings`] (unknown keys such as `modelKey` / `autoRouting*` are ignored).
/// Callers achieve "last-wins across puids" by overwriting the accumulated value with the
/// last model-router object seen in the object list.
fn wasmplugin_model_router(obj: &Object) -> Option<ModelRouterSettings> {
    if obj.name != MODEL_ROUTER_NAME {
        return None;
    }
    let default_config = obj.value.get("spec").and_then(|s| s.get("defaultConfig"));
    Some(translate_model_router_config(default_config))
}

/// Parse a `defaultConfig` [`Value`] into [`ModelRouterSettings`].
///
/// An absent / non-object / unparseable `defaultConfig` yields the core defaults (the plugin
/// is present but carries no usable config) rather than an error, so a malformed object can
/// never reject the snapshot — at most it resets the field to the defaults.
fn translate_model_router_config(default_config: Option<&Value>) -> ModelRouterSettings {
    let Some(cfg) = default_config else {
        return ModelRouterSettings::default();
    };
    if !cfg.is_object() {
        return ModelRouterSettings::default();
    }
    // R-10 / C1: keys beyond the typed-consumed set (prefix/targetHeader/
    // enableOnPathSuffix/aliasNameMapping/maxBodyBytes) are not consumed —
    // GPUStack contract fields such as modelKey/autoRouting* included — so a
    // GPUStack upgrade writing new behaviour here becomes visible.
    warn_unknown_keys(
        cfg,
        &[
            "prefix",
            "targetHeader",
            "enableOnPathSuffix",
            "aliasNameMapping",
            "maxBodyBytes",
        ],
        "gpustack-model-router defaultConfig",
    );
    match serde_json::from_value::<ModelRouterSettings>(cfg.clone()) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("model-router: malformed defaultConfig, using defaults: {e}");
            ModelRouterSettings::default()
        }
    }
}

/// Extract the per-destination provider [`ProviderToken`] list from a
/// `gpustack-ai-proxy` WasmPlugin (design D6 / §7).
///
/// GPUStack's `gpustack-ai-proxy` spec carries:
/// - `defaultConfig.providers[]` — each an `id` + `apiTokens[]` (plus `type` /
///   `baseUrl` / `failover` / `retryOnFailure`); and
/// - `matchRules[]` — each pinning `config.activeProviderId` (the active
///   provider's `id`) to a `service` (the `name.type` destination, **no port**)
///   and, optionally, to an `ingress` scope (bare ingress name(s)).
///
/// This flattens the (`activeProviderId` → `apiTokens`) reference into one
/// [`ProviderToken`] per (`service`[, `ingress`]) pair so the data plane can,
/// for a `provider-<id>.<type>` destination, swap the outbound `Authorization`
/// to the provider's key. Returns an empty vector for any other plugin (or an
/// empty `matchRules`). Callers achieve "last-wins across puids" by overwriting
/// the accumulated list with the last ai-proxy object seen.
pub fn wasmplugin_ai_proxy(obj: &Object) -> Vec<ProviderToken> {
    if obj.name != AI_PROXY_NAME {
        return Vec::new();
    }
    let Some(spec) = obj.value.get("spec") else {
        return Vec::new();
    };
    // R-10 / C1: the ai-proxy defaultConfig carries only `providers` today.
    if let Some(dc) = spec.get("defaultConfig") {
        warn_unknown_keys(dc, &["providers"], "gpustack-ai-proxy defaultConfig");
    }

    // `defaultConfig.providers[]`: provider `id` -> `apiTokens` (list).
    let mut provider_tokens: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    if let Some(providers) = spec
        .get("defaultConfig")
        .and_then(|d| d.get("providers"))
        .and_then(|p| p.as_array())
    {
        for p in providers {
            let id = p.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if id.is_empty() {
                continue;
            }
            let tokens = p
                .get("apiTokens")
                .and_then(|a| a.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            provider_tokens.insert(id, tokens);
        }
    }

    // `matchRules[]`: `config.activeProviderId` + `service[]` + `ingress[]`.
    let mut out = Vec::new();
    if let Some(rules) = spec.get("matchRules").and_then(|r| r.as_array()) {
        for rule in rules {
            // R-10 / C1.
            warn_unknown_keys(
                rule,
                &["config", "service", "ingress", "domain", "configDisable"],
                "gpustack-ai-proxy matchRule",
            );
            let active = rule
                .get("config")
                .and_then(|c| c.get("activeProviderId"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // The active provider must carry at least one apiToken (a provider
            // with an empty token list cannot authenticate upstream).
            let Some(tokens) = provider_tokens.get(&active).filter(|t| !t.is_empty()) else {
                continue;
            };
            let services = rule
                .get("service")
                .and_then(|s| s.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let ingress = rule
                .get("ingress")
                .and_then(|s| s.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for service in services {
                if service.is_empty() {
                    continue;
                }
                if ingress.is_empty() {
                    // No ingress scope: the token applies to every ingress that
                    // selects this service (global).
                    out.push(ProviderToken {
                        service,
                        ingress_scope: None,
                        api_tokens: tokens.clone(),
                    });
                } else {
                    // One token per (service, ingress) scope — a rule may cover
                    // multiple ingresses.
                    for ing in &ingress {
                        out.push(ProviderToken {
                            service: service.clone(),
                            ingress_scope: Some(ing.clone()),
                            api_tokens: tokens.clone(),
                        });
                    }
                }
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// EnvoyFilter → FallbackLink derivation
// ---------------------------------------------------------------------------

/// The parts of a fallback `EnvoyFilter` a main route needs to derive its [`FallbackLink`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvoyFilterFallback {
    /// The main ingress name the filter is scoped to (`metadata.name`, bare form).
    pub ingress_name: String,
    /// The value injected into `x-higress-fallback-from` on the 4xx/5xx redirect (= the
    /// main ingress name; the key of the linked Fallback route).
    pub fallback_from: String,
    /// `max_internal_redirects`.
    pub max_redirects: u32,
    /// `use_original_request_body && use_original_request_uri`.
    pub use_original_request: bool,
}

/// Derive the [`EnvoyFilterFallback`] from a fallback EnvoyFilter's
/// `spec.configPatches[].patch.value`.
///
/// Returns `None` when the object is not a 4xx/5xx custom-response redirect filter (e.g. the
/// unmanaged global custom-response EnvoyFilter, or a filter missing the redirect policy).
pub fn envoyfilter_fallback(obj: &Object) -> Option<EnvoyFilterFallback> {
    let value = &obj.value;
    let ingress_name = obj.name.clone();

    // Walk configPatches to find the custom_response redirect policy value.
    let mut max_redirects: u32 = 10;
    let mut use_body = true;
    let mut use_uri = true;
    let mut redirect_policy: Option<&Map<String, Value>> = None;

    if let Some(patches) = value
        .get("spec")
        .and_then(|s| s.get("configPatches"))
        .and_then(|p| p.as_array())
    {
        for patch in patches {
            let Some(typed) = patch
                .get("patch")
                .and_then(|p| p.get("value"))
                .and_then(|v| v.get("typed_per_filter_config"))
                .and_then(|c| c.get(CUSTOM_RESPONSE_FILTER))
                .and_then(|c| c.get("value"))
            else {
                continue;
            };
            let Some(matcher_list) = typed
                .get("custom_response_matcher")
                .and_then(|m| m.get("matcher_list"))
                .and_then(|m| m.get("matchers"))
                .and_then(|m| m.as_array())
            else {
                continue;
            };
            for matcher in matcher_list {
                let Some(policy) = matcher
                    .get("on_match")
                    .and_then(|a| a.get("action"))
                    .and_then(|a| a.get("typed_config"))
                    .and_then(|a| a.get("value"))
                else {
                    continue;
                };
                // NB4: a non-object `typed_config.value` must never panic the poll loop —
                // skip this matcher (per-object skip-and-issue) instead of `.expect()`.
                let Some(policy_obj) = policy.as_object() else {
                    tracing::warn!(
                        envoyfilter = %ingress_name,
                        "skipping EnvoyFilter matcher: typed_config.value is not an object"
                    );
                    continue;
                };
                max_redirects = policy_obj
                    .get("max_internal_redirects")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10) as u32;
                use_body = policy_obj
                    .get("use_original_request_body")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                use_uri = policy_obj
                    .get("use_original_request_uri")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);
                redirect_policy = Some(policy_obj);
                break;
            }
        }
    }

    let policy = redirect_policy?;

    // The redirect injects x-higress-fallback-from = <main ingress name>.
    // Owned `String` so the borrow on `ingress_name` (the `unwrap_or` fallback) has ended before
    // `ingress_name` is moved into the struct literal below.
    let fallback_from: String = policy
        .get("request_headers_to_add")
        .or_else(|| policy.get("response_headers_to_add"))
        .and_then(|h| h.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|h| h.get("header").and_then(|h| h.get("key")).and_then(|k| k.as_str())
                    == Some(FALLBACK_FROM_VALUE_HEADER))
                .and_then(|h| h.get("header").and_then(|h| h.get("value")).and_then(|v| v.as_str()))
        })
        .unwrap_or(ingress_name.as_str())
        .to_string();

    Some(EnvoyFilterFallback {
        ingress_name,
        fallback_from,
        max_redirects,
        use_original_request: use_body && use_uri,
    })
}

// ---------------------------------------------------------------------------
// Secret → TLS host
// ---------------------------------------------------------------------------

/// Translate a `gpustack-tls-*` `Secret` into a [`TlsHost`].
///
/// Returns `None` for secret names outside the `gpustack-tls-` prefix (i.e. not a data-plane
/// TLS secret). The `tls.crt`/`tls.key` data fields are base64-decoded to PEM. `is_default`
/// is set for the `gpustack-tls-default` fallback cert.
pub fn secret_to_tls_host(obj: &Object) -> Option<TlsHost> {
    if !obj.name.starts_with(TLS_SECRET_PREFIX) {
        return None;
    }
    let host = obj.name.trim_start_matches(TLS_SECRET_PREFIX).to_string();
    let is_default = obj.name == format!("{TLS_SECRET_PREFIX}default");

    let value = &obj.value;
    let data = value.get("data").and_then(|d| d.as_object());
    let string_data = value.get("stringData").and_then(|d| d.as_object());

    let decode = |key: &str| -> Option<String> {
        if let Some(d) = string_data.and_then(|m| m.get(key)).and_then(|v| v.as_str()) {
            return Some(d.to_string());
        }
        let b64 = data.and_then(|m| m.get(key)).and_then(|v| v.as_str())?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .ok()?;
        String::from_utf8(bytes).ok()
    };

    let cert = decode("tls.crt")?;
    let key = decode("tls.key")?;

    Some(TlsHost {
        host,
        is_default,
        cert_pem: cert,
        key_pem: key,
    })
}

// ---------------------------------------------------------------------------
// ConfigMap → timing
// ---------------------------------------------------------------------------

/// Translate the `higress-config` `ConfigMap` into a [`TimingConfig`] (fix D5).
///
/// GPUStack stores the gateway's Higress/Envoy settings as a **single YAML document** under
/// `data["higress"]` (NOT as flat ConfigMap keys). `ensure_gateway_timeout` reads that blob,
/// rewrites `downstream.idleTimeout` (env `GPUSTACK_PROXY_TIMEOUT_SECONDS`, default 1800) and
/// `upstream.idleTimeout` (env `GPUSTACK_PROXY_UPSTREAM_IDLE_TIMEOUT_SECONDS`, **default 3**)
/// in place while preserving the rest — so `maxRequestHeadersKb` lives at
/// `downstream.maxRequestHeadersKb` **inside** the document. We therefore parse the `higress`
/// document as YAML to pull `downstream.idleTimeout`, `upstream.idleTimeout` and
/// `downstream.maxRequestHeadersKb`.
///
/// **Patched-labeled nuance:** the values are *nested* (under `downstream`/`upstream`), and the
/// upstream idle timeout is frequently rewritten from the seed (10) to 3 by
/// `ensure_gateway_timeout` — so the effective value is what the *patched* blob says, not the
/// seed. Reading flat `downstream.idleTimeout`-style keys misses the real `higress` document
/// entirely (this is the bug being fixed).
///
/// **Robustness fallback:** when the `higress` document is absent (or unparseable), the flat
/// keys `downstream.idleTimeout` / `upstream.idleTimeout` / `maxRequestHeadersKb` are honoured,
/// and any absent value falls back to the core default. Returns `None` for `ConfigMap`s that
/// carry no `data` object (by name is checked by the caller).
pub fn configmap_to_timing(obj: &Object) -> Option<TimingConfig> {
    let data = obj.value.get("data").and_then(|d| d.as_object())?;
    let mut cfg = TimingConfig::default();
    // R-10 / C1: only the `higress` YAML document (and the flat timeout
    // fallbacks below) are consumed; other data keys are envoy-tuning keys that
    // Hygress does not reproduce — surface them once.
    let flat_known = ["higress", "downstream.idleTimeout", "upstream.idleTimeout", "maxRequestHeadersKb"];
    let unknown_flat: Vec<String> = data
        .keys()
        .filter(|k| !flat_known.contains(&k.as_str()))
        .cloned()
        .collect();
    if !unknown_flat.is_empty() {
        tracing::warn!(
            what = "higress-config data",
            unknown_keys = ?unknown_flat,
            "unconsumed ConfigMap keys (C1) — only the timeout document is honored"
        );
    }

    // ---- preferred source: the `higress` YAML document (the real GPUStack shape) ----
    let (yml_down, yml_up, yml_max): (Option<u64>, Option<u64>, Option<u64>) =
        match data.get("higress").and_then(|v| v.as_str()) {
            Some(document) => match serde_yaml::from_str::<serde_yaml::Value>(document) {
                Ok(m) => {
                    // (R-10/C1 for the flat map is emitted above; the YAML
                    // document is serde_yaml::Value — its extra sections
                    // (mesh/tracing/gzip/…) are covered by the timing
                    // not-enforced warning at bind.)
                    (
                        m.get("downstream")
                            .and_then(|d| d.get("idleTimeout"))
                            .and_then(|v| v.as_u64()),
                        m.get("upstream")
                            .and_then(|u| u.get("idleTimeout"))
                            .and_then(|v| v.as_u64()),
                        m.get("downstream")
                            .and_then(|d| d.get("maxRequestHeadersKb"))
                            .and_then(|v| v.as_u64()),
                    )
                }
                Err(e) => {
                    tracing::warn!("higress-config: could not parse 'higress' YAML: {e}");
                    (None, None, None)
                }
            },
            None => (None, None, None),
        };

    // ---- fallback: flat ConfigMap keys (older / simplified seed variants) ----
    let to_secs = |key: &str| -> Option<u64> {
        data.get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
    };

    if let Some(v) = yml_down.or(to_secs("downstream.idleTimeout")) {
        cfg.downstream_idle_timeout_secs = v;
    }
    if let Some(v) = yml_up.or(to_secs("upstream.idleTimeout")) {
        cfg.upstream_idle_timeout_secs = v;
    }
    if let Some(v) = yml_max.or(to_secs("maxRequestHeadersKb")) {
        cfg.max_request_headers_kb = Some(v);
    }
    Some(cfg)
}

// ---------------------------------------------------------------------------
// Assembly
// ---------------------------------------------------------------------------

/// Assemble a [`ConfigData`] snapshot from the set of managed k8s objects.
///
/// This is the pure, total entrypoint: it translates every object, drops (and logs) any
/// individual object that fails to translate, then wires the cross-object relationships:
/// - the per-destination `model_mapping` is merged into each route whose ingress scope a
///   model-mapper rule covers;
/// - each Main route gets a [`FallbackLink`] when a corresponding Fallback route exists; the
///   link's `max_redirects` / `use_original_request` are taken from the main route's
///   EnvoyFilter when present (defaults otherwise).
///
/// The returned snapshot is the raw translation (it is still passed through
/// [`hygress_core::SharedConfig::store`] which performs the final per-object sanitize and
/// structural validation), so a bad route never rejects the whole snapshot.
pub fn build_config_data(objects: &[Object], gateway_namespace: &str, mirror_name: &str) -> ConfigData {
    let mut routes: Vec<RouteRule> = Vec::new();
    let mut registries: Vec<Registry> = Vec::new();
    let mut proxies: Vec<OutboundProxy> = Vec::new();
    let mut features: Vec<GatewayFeatureConfig> = Vec::new();
    let mut tls_hosts: Vec<TlsHost> = Vec::new();
    let mut timing = TimingConfig::default();
    let mut model_router = ModelRouterSettings::default();
    let mut provider_tokens: Vec<ProviderToken> = Vec::new();

    // ---- per-object translation (skip-and-log on failure) ----
    for obj in objects {
        match obj.kind {
            ObjectKind::Ingress => match classify_ingress_name(&obj.name, mirror_name) {
                Some(kind) => match ingress_to_route(obj, kind, gateway_namespace) {
                    Ok(route) => routes.push(route),
                    Err(e) => tracing::warn!(ingress = %obj.name, "dropping ingress on translation: {e}"),
                },
                None => {
                    // Legacy / unmanaged ingress shape: ignored by design.
                    tracing::trace!(ingress = %obj.name, "ignoring non-model-route ingress");
                }
            },
            ObjectKind::McpBridge => match mcpbridge_to_registries(obj) {
                Ok((regs, proxs)) => {
                    registries.extend(regs);
                    proxies.extend(proxs);
                }
                Err(e) => tracing::warn!(bridge = %obj.name, "dropping mcpbridge on translation: {e}"),
            },
            ObjectKind::WasmPlugin => {
                // R-10 / C1: a MANAGED plugin outside the known set is one
                // Hygress does not reproduce natively — surface it (fail-open).
                if !KNOWN_WASM_PLUGINS.contains(&obj.name.as_str()) && obj.is_managed() {
                    tracing::warn!(
                        plugin = %obj.name,
                        "managed WasmPlugin is not reproduced natively by Hygress (C1); its behavior will differ from GPUStack"
                    );
                }
                features.push(wasmplugin_to_feature(obj));
                // The model-router (generic-proxy-router) `defaultConfig` is typed and
                // hot-reloadable; last-wins across puids (plugin absent -> default).
                if let Some(mr) = wasmplugin_model_router(obj) {
                    model_router = mr;
                }
                // The ai-proxy provider `apiTokens` (D6 / §7): last-wins across
                // puids; absent plugin -> empty. Only the ai-proxy object
                // contributes (others are ignored, not reset).
                if obj.name == AI_PROXY_NAME {
                    provider_tokens = wasmplugin_ai_proxy(obj);
                }
            }
            ObjectKind::Secret => {
                if let Some(host) = secret_to_tls_host(obj) {
                    tls_hosts.push(host);
                }
            }
            ObjectKind::ConfigMap => {
                // Only the gateway timeout map contributes to `timing` (by name).
                if obj.name == HIGRESS_CONFIG_MAP {
                    if let Some(t) = configmap_to_timing(obj) {
                        timing = t;
                    }
                }
            }
            ObjectKind::EnvoyFilter => {} // handled below (fallback wiring)
        }
    }

    // ---- model-mapping merge (per-destination, keyed by name.type) ----
    let mut mapping_rules: Vec<ModelMappingRule> = Vec::new();
    for obj in objects.iter().filter(|o| o.kind == ObjectKind::WasmPlugin) {
        mapping_rules.extend(wasmplugin_model_mapping(obj));
    }
    if !mapping_rules.is_empty() {
        merge_model_mapping(&mut routes, &mapping_rules);
    }

    // ---- fallback wiring (EnvoyFilter / Fallback route -> Main route) ----
    wire_fallbacks(objects, &mut routes);

    ConfigData {
        routes,
        registries,
        proxies,
        features,
        tls: TlsConfig {
            hosts: tls_hosts,
        },
        timing,
        model_router,
        provider_tokens,
    }
}

/// Normalize a possibly ns-prefixed ingress reference to its bare name (last path segment).
fn bare_ingress_name(ref_: &str) -> &str {
    ref_.rsplit('/').next().unwrap_or(ref_)
}

/// Merge model-mapper rules into the routes whose ingress scope matches (by bare ingress name).
fn merge_model_mapping(routes: &mut [RouteRule], rules: &[ModelMappingRule]) {
    for route in routes.iter_mut() {
        let route_bare = bare_ingress_name(&route.ingress_name);
        for rule in rules {
            // A rule covers this route when any of its ingress names match this route's
            // bare name (handles both `name` and `<ns>/name` forms).
            if !rule.ingress.iter().any(|ing| bare_ingress_name(ing) == route_bare) {
                continue;
            }
            for service in &rule.services {
                // Only keep the first rule for a given service (matches core first-match-wins).
                if route.model_mapping.lookup(service).is_none() {
                    route.model_mapping
                        .rules
                        .push((service.clone(), rule.model.clone()));
                }
            }
        }
    }
}

/// For each Main route, attach a [`FallbackLink`] when a corresponding Fallback route exists.
///
/// The Fallback route's `key` equals the main ingress name (the value
/// `x-higress-fallback-from` carries). When the main route's EnvoyFilter is present its
/// `max_redirects` / `use_original_request` are used; otherwise the core defaults (10 / true)
/// apply, matching GPUStack's `get_ingress_fallback_envoyfilter`.
fn wire_fallbacks(objects: &[Object], routes: &mut [RouteRule]) {
    // Fallback route keys (the main ingress name each fallback is keyed to). Cloned to an owned
    // `String` set so this holds no borrow on `routes` (which is iterated mutably below).
    let fallback_keys: BTreeSet<String> = routes
        .iter()
        .filter(|r| r.kind == RouteKind::Fallback)
        .map(|r| r.key.clone())
        .collect();

    // EnvoyFilter params keyed by the main (bare) ingress name.
    let mut envoy_by_main: std::collections::BTreeMap<String, EnvoyFilterFallback> =
        std::collections::BTreeMap::new();
    for obj in objects.iter().filter(|o| o.kind == ObjectKind::EnvoyFilter) {
        if let Some(fb) = envoyfilter_fallback(obj) {
            envoy_by_main.insert(fb.ingress_name.clone(), fb);
        }
    }

    for route in routes.iter_mut().filter(|r| r.kind == RouteKind::Main) {
        let main_bare = bare_ingress_name(&route.ingress_name);
        if !fallback_keys.contains(main_bare) {
            continue;
        }
        let (max_redirects, use_original) = match envoy_by_main.get(main_bare) {
            Some(fb) => (fb.max_redirects, fb.use_original_request),
            // A fallback route without its EnvoyFilter: use the core defaults.
            None => (10, true),
        };
        let link = FallbackLink {
            target_key: main_bare.to_string(),
            main_ingress_name: route.ingress_name.clone(),
            max_redirects,
            use_original_request: use_original,
        };
        route.fallback = Some(link);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A helper to build an [`Object`] from a JSON value with a kind + name.
    fn obj(
        kind: ObjectKind,
        name: &str,
        ns: &str,
        uid: &str,
        rv: u64,
        value: Value,
    ) -> Object {
        Object {
            kind,
            name: name.to_string(),
            namespace: ns.to_string(),
            uid: uid.to_string(),
            resource_version: rv,
            value,
        }
    }

    /// `labels` map as JSON with the managed label set.
    fn managed_labels() -> Value {
        json!({ "gpustack.ai/managed": "true" })
    }

    // A representative weighted main ingress mirroring generate_model_ingress: two static
    // destinations with Hamilton percents, the real regex_prefixes path shapes, rewrite +
    // retry annotations and the core x-higress-llm-model matcher.
    fn main_ingress() -> Value {
        json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {
                "name": "ai-route-route-5.internal",
                "namespace": "higress-system",
                "uid": "uid-main-5",
                "resourceVersion": "1001",
                "labels": managed_labels(),
                "annotations": {
                    "higress.io/destination": "60% model-5-12.static:80\n40% model-5-13.static:80",
                    "higress.io/rewrite-target": "/$1$3",
                    "higress.io/ignore-path-case": "true",
                    "higress.io/proxy-next-upstream": "error,timeout,http_503,http_502,non_idempotent",
                    "higress.io/proxy-next-upstream-tries": "2",
                    "higress.io/exact-match-header-x-higress-llm-model": "org1/llama-3-8b"
                }
            },
            "spec": {
                "ingressClassName": "higress",
                "rules": [
                    {
                        "http": {
                            "paths": [
                                { "path": "/(v1)()(/chat/completions|/embeddings|/responses)", "pathType": "ImplementationSpecific" },
                                { "path": "/()model/proxy/\\d+(/|$)(.*)", "pathType": "ImplementationSpecific" }
                            ]
                        }
                    }
                ]
            }
        })
    }

    /// A fallback ingress mirroring the extra `x-higress-fallback-from` matcher.
    fn fallback_ingress() -> Value {
        json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {
                "name": "ai-route-route-5.fallback.internal",
                "namespace": "higress-system",
                "uid": "uid-fallback-5",
                "resourceVersion": "1002",
                "labels": managed_labels(),
                "annotations": {
                    "higress.io/destination": "100% model-5-20.static:80",
                    "higress.io/rewrite-target": "/$1$3",
                    "higress.io/ignore-path-case": "true",
                    "higress.io/exact-match-header-x-higress-fallback-from": "ai-route-route-5.internal"
                }
            },
            "spec": {
                "ingressClassName": "higress",
                "rules": [
                    {
                        "http": {
                            "paths": [
                                { "path": "/(v1)()(/chat/completions|/embeddings|/responses)", "pathType": "ImplementationSpecific" }
                            ]
                        }
                    }
                ]
            }
        })
    }

    /// The `gpustack` mirror ingress: single no-`pct%` destination, `/` prefix, ignore-path-case false.
    fn mirror_ingress() -> Value {
        json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {
                "name": "gpustack",
                "namespace": "higress-system",
                "uid": "uid-mirror",
                "resourceVersion": "1003",
                "labels": managed_labels(),
                "annotations": {
                    "higress.io/destination": "gpustack.static:80",
                    "higress.io/ignore-path-case": "false"
                }
            },
            "spec": {
                "ingressClassName": "higress",
                "rules": [
                    { "http": { "paths": [ { "path": "/", "pathType": "Prefix" } ] } }
                ]
            }
        })
    }

    /// The real `default` McpBridge shape: GPUStack creates it WITHOUT the
    /// `gpustack.ai/managed=true` label (verified against the live baseline), so it
    /// must translate regardless of the managed label (unlike route-scoped objects).
    fn mcpbridge() -> Value {
        json!({
            "apiVersion": "networking.higress.io/v1",
            "kind": "McpBridge",
            "metadata": {
                "name": "default",
                "namespace": "higress-system",
                "uid": "uid-bridge",
                "resourceVersion": "2001"
            },
            "spec": {
                "registries": [
                    { "name": "gpustack", "type": "static", "domain": "127.0.0.1:8080", "port": 80, "protocol": "http" },
                    { "name": "model-5-12", "type": "static", "domain": "10.0.0.5:8081", "port": 80 },
                    { "name": "provider-1", "type": "proxy", "domain": "api.example.com", "port": 443, "proxyName": "provider-1-proxy" },
                    { "name": "model-1-2", "type": "dns", "domain": "worker.example.com", "port": 30080 }
                ],
                "proxies": [
                    {
                        "name": "provider-1-proxy",
                        "serverAddress": "proxy.internal",
                        "serverPort": 3128,
                        "type": "HTTPS",
                        "connectTimeout": 5000
                    }
                ]
            }
        })
    }

    /// A `gpustack-model-mapper` WasmPlugin with per-destination matchRules (name.type, no port).
    fn model_mapper_plugin() -> Value {
        json!({
            "apiVersion": "extensions.higress.io/v1alpha1",
            "kind": "WasmPlugin",
            "metadata": {
                "name": "gpustack-model-mapper",
                "namespace": "higress-system",
                "uid": "uid-mapper",
                "resourceVersion": "3001",
                "labels": managed_labels()
            },
            "spec": {
                "phase": "AUTHN",
                "priority": 800,
                "failStrategy": "FAIL_OPEN",
                "defaultConfigDisable": false,
                "url": "http://127.0.0.1:8080/plugins/gpustack-model-mapper/1.0.0/plugin.wasm",
                "defaultConfig": { "modelMapping": {} },
                "matchRules": [
                    {
                        "config": { "modelMapping": { "org1/llama-3-8b": "llama-3-8b-instruct" } },
                        "ingress": ["ai-route-route-5.internal", "ai-route-route-5.fallback.internal"],
                        "service": ["model-5-12.static", "model-5-13.static"],
                        "configDisable": false
                    }
                ]
            }
        })
    }

    /// A fallback EnvoyFilter (4xx/5xx custom-response redirect) mirroring
    /// get_ingress_fallback_envoyfilter.
    fn fallback_envoyfilter() -> Value {
        let fallback_value = || {
            json!({
                "request_headers_to_add": [
                    { "append": false, "header": { "key": "x-higress-fallback-from", "value": "ai-route-route-5.internal" } },
                    { "append": false, "header": { "key": "x-gpustack-fallback-path", "value": "%REQ(X_GPUSTACK_ORIGINAL_PATH)%" } }
                ],
                "response_headers_to_add": [
                    { "append": false, "header": { "key": "x-higress-fallback-from", "value": "ai-route-route-5.internal" } }
                ],
                "keep_original_response_code": false,
                "max_internal_redirects": 10,
                "only_redirect_upstream_code": false,
                "use_original_request_body": true,
                "use_original_request_uri": true
            })
        };
        json!({
            "apiVersion": "networking.istio.io/v1alpha3",
            "kind": "EnvoyFilter",
            "metadata": {
                "name": "ai-route-route-5.internal",
                "namespace": "higress-system",
                "uid": "uid-ef-5",
                "resourceVersion": "4001",
                "labels": managed_labels()
            },
            "spec": {
                "configPatches": [
                    {
                        "applyTo": "HTTP_ROUTE",
                        "match": {
                            "context": "GATEWAY",
                            "routeConfiguration": { "vhost": { "route": { "name": "ai-route-route-5.internal" } } }
                        },
                        "patch": {
                            "operation": "MERGE",
                            "value": {
                                "typed_per_filter_config": {
                                    "envoy.filters.http.custom_response": {
                                        "@type": "type.googleapis.com/udpa.type.v1.TypedStruct",
                                        "type_url": "type.googleapis.com/envoy.extensions.filters.http.custom_response.v3.CustomResponse",
                                        "value": {
                                            "custom_response_matcher": {
                                                "matcher_list": {
                                                    "matchers": [
                                                        {
                                                            "on_match": {
                                                                "action": {
                                                                    "typed_config": {
                                                                        "@type": "type.googleapis.com/udpa.type.v1.TypedStruct",
                                                                        "value": fallback_value()
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    ]
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        })
    }

    /// A TLS secret (gpustack-tls-default) with base64 `tls.crt` / `tls.key`.
    fn tls_secret() -> Value {
        // base64 of "CERT-PEM" and "KEY-PEM"
        let cert_b64 = base64::engine::general_purpose::STANDARD.encode(b"CERT-PEM");
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(b"KEY-PEM");
        json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "gpustack-tls-default",
                "namespace": "higress-system",
                "uid": "uid-tls",
                "resourceVersion": "5001",
                "labels": managed_labels()
            },
            "type": "kubernetes.io/tls",
            "data": { "tls.crt": cert_b64, "tls.key": key_b64 }
        })
    }

    /// The `higress-config` ConfigMap (patched upstream idle timeout).
    ///
    /// This is the **flat-key** form (kept as the robustness fallback); the real GPUStack
    /// `ensure_gateway_timeout` writes the YAML `higress` document instead (see
    /// [`higress_configmap_yaml`]).
    fn higress_configmap() -> Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "higress-config",
                "namespace": "higress-system",
                "uid": "uid-cm",
                "resourceVersion": "6001",
                "labels": managed_labels()
            },
            "data": {
                "downstream.idleTimeout": "1800",
                "upstream.idleTimeout": "3",
                "maxRequestHeadersKb": "128"
            }
        })
    }

    /// The REAL GPUStack-shaped `gpustack-model-router` WasmPlugin
    /// (plugin-contract-pin §2.3): `defaultConfig` = `prefix` / `targetHeader` /
    /// non-empty `enableOnPathSuffix` (openai + anthropic routes) / hot-updated
    /// `aliasNameMapping`.
    fn model_router_plugin() -> Value {
        json!({
            "apiVersion": "extensions.higress.io/v1alpha1",
            "kind": "WasmPlugin",
            "metadata": {
                "name": "gpustack-model-router",
                "namespace": "higress-system",
                "uid": "uid-router",
                "resourceVersion": "3201",
                "labels": managed_labels()
            },
            "spec": {
                "phase": "AUTHN",
                "priority": 900,
                "failStrategy": "FAIL_OPEN",
                "defaultConfigDisable": false,
                "url": "http://127.0.0.1:8080/plugins/gpustack-generic-proxy-router/1.0.0/plugin.wasm",
                "matchRules": [],
                "defaultConfig": {
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
                    "maxBodyBytes": 104857600
                }
            }
        })
    }

    /// The REAL GPUStack-shaped `gpustack-ai-proxy` WasmPlugin (design D6 / §7).
    /// It carries `defaultConfig.providers[]`, each with its `id`, `apiTokens`,
    /// and `type`/`baseUrl`/`failover`/`retryOnFailure`, plus `matchRules[]`, each
    /// pinning the active provider (`config.activeProviderId`) to a `service` and,
    /// optionally, an `ingress`. Two providers are declared, with one global and
    /// one ingress-scoped match rule (concrete ids: see the body).
    fn ai_proxy_plugin() -> Value {
        json!({
            "apiVersion": "extensions.higress.io/v1alpha1",
            "kind": "WasmPlugin",
            "metadata": {
                "name": "gpustack-ai-proxy",
                "namespace": "higress-system",
                "uid": "uid-aiproxy",
                "resourceVersion": "3301",
                "labels": managed_labels()
            },
            "spec": {
                "phase": "UNSPECIFIED_PHASE",
                "priority": 100,
                "failStrategy": "FAIL_OPEN",
                "defaultConfigDisable": false,
                "url": "http://127.0.0.1:8080/plugins/ai-proxy/2.0.0/plugin.wasm",
                "matchRules": [
                    {
                        "configDisable": false,
                        "config": { "activeProviderId": "provider-1-101" },
                        "service": ["provider-1.proxy"],
                        "ingress": []
                    },
                    {
                        "configDisable": false,
                        "config": { "activeProviderId": "provider-2-202" },
                        "service": ["provider-2.dns"],
                        "ingress": ["ai-route-route-5.internal"]
                    }
                ],
                "defaultConfig": {
                    "providers": [
                        {
                            "id": "provider-1-101",
                            "apiTokens": ["sk-provider-1-a", "sk-provider-1-b"],
                            "type": "openai",
                            "baseUrl": "https://api.upstream.example.com",
                            "failover": { "enabled": true, "healthCheckModel": "gpt-4o" },
                            "retryOnFailure": { "enabled": false }
                        },
                        {
                            "id": "provider-2-202",
                            "apiTokens": ["sk-provider-2"],
                            "type": "anthropic",
                            "baseUrl": "https://provider2.example.com",
                            "failover": { "enabled": false },
                            "retryOnFailure": { "enabled": false }
                        }
                    ]
                }
            }
        })
    }

    /// A `higress-config` ConfigMap using the real GPUStack shape: a single YAML document
    /// under `data["higress"]` with nested `downstream`/`upstream` (idle timeouts +
    /// `maxRequestHeadersKb`), as rewritten in place by `ensure_gateway_timeout`
    /// (upstream idle timeout patched to the env default 3).
    fn higress_configmap_yaml() -> Value {
        let higress_doc = "\
mcpServer:
  enable: false
  sse_path_suffix: /sse
  redis:
    address: redis-address:6379
    username: \"\"
    password: \"\"
    db: 0
  match_list: []
  servers: []
downstream:
  connectionBufferLimits: 32768
  idleTimeout: 1800
  maxRequestHeadersKb: 60
  routeTimeout: 0
upstream:
  connectionBufferLimits: 10485760
  idleTimeout: 3
";
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "higress-config",
                "namespace": "higress-system",
                "uid": "uid-cm-yaml",
                "resourceVersion": "6101",
                "labels": managed_labels()
            },
            "data": { "higress": higress_doc }
        })
    }

    // ----- classify / mirror name -----

    #[test]
    fn classify_ingress_names() {
        let m = MIRROR_NAME;
        assert_eq!(classify_ingress_name("ai-route-route-5.internal", m), Some(RouteKind::Main));
        assert_eq!(
            classify_ingress_name("ai-route-route-5.fallback.internal", m),
            Some(RouteKind::Fallback)
        );
        assert_eq!(classify_ingress_name("gpustack", m), Some(RouteKind::Mirror));
        // Legacy / unmanaged: ignored.
        assert_eq!(classify_ingress_name("ai-route-model-3", m), None);
        assert_eq!(classify_ingress_name("some-other-ingress", m), None);
    }

    #[test]
    fn mirror_name_is_configurable() {
        assert_eq!(
            classify_ingress_name("gpustack-worker", "gpustack-worker"),
            Some(RouteKind::Mirror)
        );
        assert_eq!(classify_ingress_name("gpustack_worker", "gpustack"), None);
    }

    // ----- main / fallback / mirror route translation -----

    #[test]
    fn translate_main_ingress() {
        let o = obj(ObjectKind::Ingress, "ai-route-route-5.internal", "higress-system", "uid-main-5", 1001, main_ingress());
        let route = ingress_to_route(&o, RouteKind::Main, "higress-system").unwrap();
        assert_eq!(route.key, "org1/llama-3-8b");
        assert_eq!(route.kind, RouteKind::Main);
        // D9: the embedded case (ingress ns == gateway ns) records the origin
        // identity BARE (pin §5.2).
        assert_eq!(route.ingress_name, "ai-route-route-5.internal");
        // weighted destinations both present with percents.
        assert_eq!(route.destinations.len(), 2);
        assert_eq!(route.destinations[0].percent, Some(60));
        assert_eq!(route.destinations[0].service, "model-5-12.static:80");
        assert_eq!(route.destinations[1].service, "model-5-13.static:80");
        // rewrite + retry + paths.
        assert_eq!(route.rewrite_target.as_ref().unwrap().target, "/$1$3");
        assert_eq!(route.retry.tries, 2);
        assert!(route.retry.has(&hygress_core::RetryCond::Error));
        assert_eq!(route.path_predicates.len(), 2);
        assert!(route.path_predicates[0].ignore_case); // ignore-path-case: true
        // auth scope: main is auth-scoped to ai-route-route-.
        assert!(route.requires_auth());
        // source provenance carries the (embedded) bare ingress name.
        assert_eq!(route.sources[0].ingress_name, "ai-route-route-5.internal");
        assert_eq!(route.sources[0].resource_version, 1001);
    }

    #[test]
    fn translate_fallback_ingress_key_is_fallback_from() {
        let o = obj(ObjectKind::Ingress, "ai-route-route-5.fallback.internal", "higress-system", "uid-fallback-5", 1002, fallback_ingress());
        let route = ingress_to_route(&o, RouteKind::Fallback, "higress-system").unwrap();
        assert_eq!(route.kind, RouteKind::Fallback);
        // key = x-higress-fallback-from value = main ingress name.
        assert_eq!(route.key, "ai-route-route-5.internal");
        // D9: embedded case -> bare fallback ingress name.
        assert_eq!(route.ingress_name, "ai-route-route-5.fallback.internal");
        assert_eq!(route.destinations[0].service, "model-5-20.static:80");
        // fallback (non-mirror) is also auth-scoped.
        assert!(route.requires_auth());
    }

    #[test]
    fn translate_mirror_ingress_no_percent_no_auth() {
        let o = obj(ObjectKind::Ingress, "gpustack", "higress-system", "uid-mirror", 1003, mirror_ingress());
        let route = ingress_to_route(&o, RouteKind::Mirror, "higress-system").unwrap();
        assert_eq!(route.kind, RouteKind::Mirror);
        assert_eq!(route.key, "gpustack");
        // no-`pct%` destination (mirror form).
        assert_eq!(route.destinations.len(), 1);
        assert_eq!(route.destinations[0].percent, None);
        assert_eq!(route.destinations[0].weight(), 100);
        assert!(route.destinations[0].service == "gpustack.static:80");
        // mirror must never be authed.
        assert!(!route.auth_scope.enabled);
        assert!(!route.requires_auth());
        // ignore-path-case false -> predicates are case-sensitive.
        assert!(!route.path_predicates[0].ignore_case);
    }

    #[test]
    fn main_ingress_missing_model_header_is_error() {
        let mut v = main_ingress();
        v["metadata"]["annotations"]
            .as_object_mut()
            .unwrap()
            .remove("higress.io/exact-match-header-x-higress-llm-model");
        let o = obj(ObjectKind::Ingress, "ai-route-route-7.internal", "higress-system", "uid-x", 1, v);
        assert!(matches!(
            ingress_to_route(&o, RouteKind::Main, "higress-system"),
            Err(Error::Invalid(_))
        ));
    }

    // ----- McpBridge / registries / proxies -----

    #[test]
    fn mcpbridge_registries_and_proxies() {
        let o = obj(ObjectKind::McpBridge, "default", "higress-system", "uid-bridge", 2001, mcpbridge());
        let (regs, proxies) = mcpbridge_to_registries(&o).unwrap();
        assert_eq!(regs.len(), 4);
        // static: id=gpustack.static, port=80.
        let gp = regs.iter().find(|r| r.id == "gpustack.static").unwrap();
        assert_eq!(gp.port, Some(80));
        assert_eq!(gp.kind, hygress_core::ServiceType::Static);
        // dns: worker with real port.
        let w = regs.iter().find(|r| r.id == "model-1-2.dns").unwrap();
        assert_eq!(w.port, Some(30080));
        // proxy: proxy_ref set.
        let p = regs.iter().find(|r| r.id == "provider-1.proxy").unwrap();
        assert_eq!(p.proxy_ref.as_deref(), Some("provider-1-proxy"));
        // proxy list.
        assert_eq!(proxies.len(), 1);
        assert_eq!(proxies[0].server_address, "proxy.internal");
        assert_eq!(proxies[0].server_port, 3128);
        assert_eq!(proxies[0].connect_timeout_secs, Some(5));
        assert_eq!(proxies[0].kind.as_deref(), Some("HTTPS"));
    }

    #[test]
    fn registry_resolve_via_snapshot() {
        let o = obj(ObjectKind::McpBridge, "default", "higress-system", "uid-bridge", 2001, mcpbridge());
        let (regs, proxies) = mcpbridge_to_registries(&o).unwrap();
        // A static registry resolves to its domain host:port.
        let gp = regs.iter().find(|r| r.id == "gpustack.static").unwrap();
        assert_eq!(
            gp.resolve(&proxies).unwrap(),
            hygress_core::ResolvedTarget::Direct { address: "127.0.0.1:8080".into() }
        );
        // A proxy registry resolves through the outbound proxy.
        let p = regs.iter().find(|r| r.id == "provider-1.proxy").unwrap();
        match p.resolve(&proxies).unwrap() {
            hygress_core::ResolvedTarget::Proxied { address, proxy_name, .. } => {
                assert_eq!(address, "api.example.com:443");
                assert_eq!(proxy_name, "provider-1-proxy");
            }
            other => panic!("expected Proxied, got {other:?}"),
        }
    }

    // ----- model-mapper matchRules -----

    #[test]
    fn model_mapper_rules_extracted_as_name_type_no_port() {
        let o = obj(ObjectKind::WasmPlugin, "gpustack-model-mapper", "higress-system", "uid-mapper", 3001, model_mapper_plugin());
        let rules = wasmplugin_model_mapping(&o);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].model, "llama-3-8b-instruct");
        assert_eq!(rules[0].services, vec!["model-5-12.static", "model-5-13.static"]);
        assert_eq!(
            rules[0].ingress,
            vec!["ai-route-route-5.internal", "ai-route-route-5.fallback.internal"]
        );
    }

    #[test]
    fn other_plugin_has_no_model_mapping() {
        let mut v = model_mapper_plugin();
        v["metadata"]["name"] = json!("gpustack-generic-proxy-router");
        let o = obj(ObjectKind::WasmPlugin, "gpustack-generic-proxy-router", "higress-system", "uid-mapper", 3001, v);
        assert!(wasmplugin_model_mapping(&o).is_empty());
    }

    #[test]
    fn wasmplugin_feature_config() {
        let o = obj(ObjectKind::WasmPlugin, "gpustack-model-mapper", "higress-system", "uid-mapper", 3001, model_mapper_plugin());
        let f = wasmplugin_to_feature(&o);
        assert_eq!(f.plugin, "gpustack-model-mapper");
        assert_eq!(f.phase, "AUTHN");
        assert_eq!(f.priority, 800);
        assert!(f.fail_open);
        assert!(!f.default_config_disable);
        // R-9①: the opaque `defaultConfig` spec is NOT retained (it could carry
        // provider apiTokens / the derived gateway token).
    }

    // ----- model-router (generic-proxy-router) defaultConfig ----

    #[test]
    fn model_router_translates_real_default_config() {
        let o = obj(ObjectKind::WasmPlugin, "gpustack-model-router", "higress-system", "uid-router", 3201, model_router_plugin());
        let s = wasmplugin_model_router(&o).unwrap();
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
        assert_eq!(s.alias_name_mapping.get("1"), Some(&"route-one".to_string()));
        assert_eq!(s.alias_name_mapping.get("2"), Some(&"route-two".to_string()));
        assert_eq!(s.max_body_bytes, Some(104857600));
    }

    #[test]
    fn model_router_absent_gives_none_and_defaults() {
        // A plugin that is not the model-router yields None (the caller keeps the default).
        let mapper = obj(ObjectKind::WasmPlugin, "gpustack-model-mapper", "higress-system", "uid-mapper", 3001, model_mapper_plugin());
        assert!(wasmplugin_model_router(&mapper).is_none());

        // The model-router plugin with no `defaultConfig` yields the core defaults.
        let mut v = model_router_plugin();
        v["spec"].as_object_mut().unwrap().remove("defaultConfig");
        let o = obj(ObjectKind::WasmPlugin, "gpustack-model-router", "higress-system", "uid-router", 3201, v);
        assert_eq!(
            wasmplugin_model_router(&o).unwrap(),
            ModelRouterSettings::default()
        );
    }

    #[test]
    fn build_config_data_wires_model_router() {
        // Present: the real model-router config lands on the snapshot.
        let mut objects = all_objects();
        objects.push(obj(
            ObjectKind::WasmPlugin,
            "gpustack-model-router",
            "higress-system",
            "uid-router",
            3201,
            model_router_plugin(),
        ));
        let data = build_config_data(&objects, "higress-system", MIRROR_NAME);
        assert_eq!(data.model_router.max_body_bytes, Some(104857600));
        assert_eq!(
            data.model_router.enable_on_path_suffix.len(),
            5
        );
        assert_eq!(data.model_router.alias_name_mapping.get("1"), Some(&"route-one".to_string()));

        // Absent: plugin not in the list -> the field stays the core default.
        let data2 = build_config_data(&all_objects(), "higress-system", MIRROR_NAME);
        assert_eq!(data2.model_router, ModelRouterSettings::default());
    }

    #[test]
    fn build_config_data_model_router_last_wins_across_puids() {
        // Two model-router objects: the last one (in object-list order) wins.
        let first = model_router_plugin();
        let mut second = model_router_plugin();
        second["spec"]["defaultConfig"]["aliasNameMapping"] = json!({ "9": "last-route" });
        second["spec"]["defaultConfig"]["maxBodyBytes"] = json!(512);
        let objects = vec![
            obj(ObjectKind::WasmPlugin, "gpustack-model-router", "higress-system", "uid-r1", 1, first),
            obj(ObjectKind::WasmPlugin, "gpustack-model-router", "higress-system", "uid-r2", 2, second),
        ];
        let data = build_config_data(&objects, "higress-system", MIRROR_NAME);
        assert_eq!(data.model_router.max_body_bytes, Some(512));
        assert_eq!(
            data.model_router.alias_name_mapping.get("9"),
            Some(&"last-route".to_string())
        );
        // The first object's alias entry was overwritten (last wins).
        assert!(!data.model_router.alias_name_mapping.contains_key("1"));
    }

    // ----- ai-proxy provider tokens (D6 / §7) -----

    #[test]
    fn ai_proxy_parsers_flatten_providers_and_match_rules() {
        let o = obj(ObjectKind::WasmPlugin, "gpustack-ai-proxy", "higress-system", "uid-aiproxy", 3301, ai_proxy_plugin());
        let tokens = wasmplugin_ai_proxy(&o);
        // Two match rules: one global (provider-1.proxy) + one ingress-scoped
        // (provider-2.dns).
        assert_eq!(tokens.len(), 2);

        // Global (no ingress scope): provider-1.proxy -> first apiToken is the active
        // bearer (the whole token list is retained).
        let global = tokens.iter().find(|t| t.service == "provider-1.proxy").unwrap();
        assert_eq!(global.ingress_scope, None);
        assert_eq!(global.api_tokens, vec!["sk-provider-1-a".to_string(), "sk-provider-1-b".to_string()]);

        // Ingress-scoped: provider-2.dns -> ai-route-route-5.internal.
        let scoped = tokens.iter().find(|t| t.service == "provider-2.dns").unwrap();
        assert_eq!(scoped.ingress_scope.as_deref(), Some("ai-route-route-5.internal"));
        assert_eq!(scoped.api_tokens, vec!["sk-provider-2".to_string()]);
    }

    #[test]
    fn other_plugin_has_no_ai_proxy_tokens() {
        // A non-ai-proxy plugin (model-router) yields no provider tokens.
        let router = obj(ObjectKind::WasmPlugin, "gpustack-model-router", "higress-system", "uid-router", 3201, model_router_plugin());
        assert!(wasmplugin_ai_proxy(&router).is_empty());
    }

    #[test]
    fn build_config_data_wires_ai_proxy_tokens_last_wins() {
        let mut objects = all_objects();
        objects.push(obj(ObjectKind::WasmPlugin, "gpustack-ai-proxy", "higress-system", "uid-aiproxy", 3301, ai_proxy_plugin()));
        let data = build_config_data(&objects, "higress-system", MIRROR_NAME);
        assert_eq!(data.provider_tokens.len(), 2);
        assert!(data.provider_tokens.iter().any(|t| t.service == "provider-1.proxy" && t.ingress_scope.is_none()));
        assert!(data.provider_tokens.iter().any(|t| t.service == "provider-2.dns" && t.ingress_scope == Some("ai-route-route-5.internal".into())));

        // Absent plugin -> empty provider tokens.
        let data2 = build_config_data(&all_objects(), "higress-system", MIRROR_NAME);
        assert!(data2.provider_tokens.is_empty());
    }

    #[test]
    fn build_config_data_ai_proxy_last_wins_across_puids() {
        // Two ai-proxy objects: the last (in object-list order) wins.
        let first = ai_proxy_plugin();
        let mut second = ai_proxy_plugin();
        second["spec"]["matchRules"].as_array_mut().unwrap().clear();
        second["spec"]["matchRules"] = json!([
            {
              "config": { "activeProviderId": "provider-9-999" },
              "service": ["provider-9.proxy"],
              "ingress": []
            }
        ]);
        second["spec"]["defaultConfig"]["providers"] = json!([
            { "id": "provider-9-999", "apiTokens": ["sk-provider-9"] }
        ]);
        let objects = vec![
            obj(ObjectKind::WasmPlugin, "gpustack-ai-proxy", "higress-system", "uid-p1", 1, first),
            obj(ObjectKind::WasmPlugin, "gpustack-ai-proxy", "higress-system", "uid-p2", 2, second),
        ];
        let data = build_config_data(&objects, "higress-system", MIRROR_NAME);
        // Only the second object's rule survives (last wins).
        assert_eq!(data.provider_tokens.len(), 1);
        assert_eq!(data.provider_tokens[0].service, "provider-9.proxy");
        assert_eq!(data.provider_tokens[0].api_tokens, vec!["sk-provider-9".to_string()]);
    }

    // ----- D9: embedded (bare) vs cross-namespace (ns-qualified) -----

    #[test]
    fn ingress_name_embedded_is_bare() {
        // Ingress ns == gateway_ns -> bare (pin §5.2 embedded value).
        let o = obj(ObjectKind::Ingress, "ai-route-route-5.internal", "higress-system", "uid-main-5", 1001, main_ingress());
        let route = ingress_to_route(&o, RouteKind::Main, "higress-system").unwrap();
        assert_eq!(route.ingress_name, "ai-route-route-5.internal");
    }

    #[test]
    fn ingress_name_cross_namespace_is_qualified() {
        // Ingress ns != gateway_ns -> ns-qualified `<ns>/<name>`.
        let mut v = main_ingress();
        v["metadata"]["namespace"] = json!("gpustack-prod");
        let o = obj(ObjectKind::Ingress, "ai-route-route-5.internal", "gpustack-prod", "uid-main-5", 1001, v);
        let route = ingress_to_route(&o, RouteKind::Main, "higress-system").unwrap();
        assert_eq!(route.ingress_name, "gpustack-prod/ai-route-route-5.internal");
    }

    // ----- EnvoyFilter fallback derivation -----

    #[test]
    fn envoyfilter_fallback_derives_link_params() {
        let o = obj(ObjectKind::EnvoyFilter, "ai-route-route-5.internal", "higress-system", "uid-ef-5", 4001, fallback_envoyfilter());
        let fb = envoyfilter_fallback(&o).unwrap();
        assert_eq!(fb.ingress_name, "ai-route-route-5.internal");
        assert_eq!(fb.fallback_from, "ai-route-route-5.internal");
        assert_eq!(fb.max_redirects, 10);
        assert!(fb.use_original_request);
    }

    #[test]
    fn envoyfilter_non_redirect_returns_none() {
        // The unmanaged global custom-response filter has a different shape: no redirect policy
        // (and it lacks the managed label, so it never reaches here in practice).
        let v = json!({
            "apiVersion": "networking.istio.io/v1alpha3",
            "kind": "EnvoyFilter",
            "metadata": { "name": "higress-gateway-global-custom-response", "namespace": "higress-system" },
            "spec": { "configPatches": [ { "applyTo": "HTTP_FILTER", "patch": { "operation": "INSERT_FIRST", "value": { "typed_per_filter_config": { "envoy.filters.http.custom_response": { "value": { "custom_response_matcher": {} } } } } } } ] }
        });
        let o = obj(ObjectKind::EnvoyFilter, "higress-gateway-global-custom-response", "higress-system", "uid-ef", 1, v);
        assert!(envoyfilter_fallback(&o).is_none());
    }

    #[test]
    fn envoyfilter_nonobject_typed_config_value_skips_without_panic() {
        // NB4: a `typed_config.value` that is not a JSON object must be skipped (per-object
        // skip-and-issue), never panic the poll loop.
        let mut v = fallback_envoyfilter();
        // Overwrite the redirect policy's `typed_config.value` with a non-object (a string).
        let matchers = v["spec"]["configPatches"][0]["patch"]["value"]
            ["typed_per_filter_config"]["envoy.filters.http.custom_response"]["value"]
            ["custom_response_matcher"]["matcher_list"]["matchers"]
            .as_array_mut()
            .unwrap();
        matchers[0]["on_match"]["action"]["typed_config"]["value"] = json!("not-an-object");
        let o = obj(ObjectKind::EnvoyFilter, "ai-route-route-5.internal", "higress-system", "uid-ef-5", 4001, v);
        // Must resolve to None (skip) rather than panicking.
        assert!(envoyfilter_fallback(&o).is_none());
    }

    // ----- Secret / TLS -----

    #[test]
    fn tls_secret_decodes_and_marks_default() {
        let o = obj(ObjectKind::Secret, "gpustack-tls-default", "higress-system", "uid-tls", 5001, tls_secret());
        let host = secret_to_tls_host(&o).unwrap();
        assert_eq!(host.host, "default");
        assert!(host.is_default);
        assert_eq!(host.cert_pem, "CERT-PEM");
        assert_eq!(host.key_pem, "KEY-PEM");
    }

    #[test]
    fn tls_secret_named_host() {
        let mut v = tls_secret();
        v["metadata"]["name"] = json!("gpustack-tls-api.example.com");
        let o = obj(ObjectKind::Secret, "gpustack-tls-api.example.com", "higress-system", "uid-tls", 5001, v);
        let host = secret_to_tls_host(&o).unwrap();
        assert_eq!(host.host, "api.example.com");
        assert!(!host.is_default);
    }

    #[test]
    fn non_tls_secret_ignored() {
        let mut v = tls_secret();
        v["metadata"]["name"] = json!("gpustack-other-secret");
        let o = obj(ObjectKind::Secret, "gpustack-other-secret", "higress-system", "uid-x", 5001, v);
        assert!(secret_to_tls_host(&o).is_none());
    }

    // ----- ConfigMap -> timing -----

    #[test]
    fn configmap_timing() {
        let o = obj(ObjectKind::ConfigMap, "higress-config", "higress-system", "uid-cm", 6001, higress_configmap());
        let t = configmap_to_timing(&o).unwrap();
        assert_eq!(t.downstream_idle_timeout_secs, 1800);
        assert_eq!(t.upstream_idle_timeout_secs, 3);
        assert_eq!(t.max_request_headers_kb, Some(128));
    }

    #[test]
    fn configmap_partial_defaults() {
        let v = json!({
            "metadata": { "name": "higress-config" },
            "data": { "upstream.idleTimeout": "3" }
        });
        let o = obj(ObjectKind::ConfigMap, "higress-config", "higress-system", "uid-cm", 6001, v);
        let t = configmap_to_timing(&o).unwrap();
        // absent downstream defaults to the core 1800; upstream overridden to 3.
        assert_eq!(t.downstream_idle_timeout_secs, 1800);
        assert_eq!(t.upstream_idle_timeout_secs, 3);
        assert_eq!(t.max_request_headers_kb, None);
    }

    #[test]
    fn configmap_timing_real_higress_yaml() {
        // The real GPUStack shape: a YAML document under `data["higress"]`, with the idle
        // timeouts nested under `downstream`/`upstream` and `maxRequestHeadersKb` under
        // `downstream`. `ensure_gateway_timeout` patches the upstream timeout to 3.
        let o = obj(ObjectKind::ConfigMap, "higress-config", "higress-system", "uid-cm-yaml", 6101, higress_configmap_yaml());
        let t = configmap_to_timing(&o).unwrap();
        assert_eq!(t.downstream_idle_timeout_secs, 1800);
        assert_eq!(t.upstream_idle_timeout_secs, 3);
        assert_eq!(t.max_request_headers_kb, Some(60));
    }

    #[test]
    fn configmap_timing_higress_yaml_wins_over_flat() {
        // When both the `higress` doc and flat keys are present, the YAML values take
        // precedence (the real shape) and the flat keys are ignored for those values.
        let mut v = higress_configmap_yaml();
        // Add conflicting flat keys that must NOT win.
        v["data"]["downstream.idleTimeout"] = json!("1");
        v["data"]["upstream.idleTimeout"] = json!("2");
        let o = obj(ObjectKind::ConfigMap, "higress-config", "higress-system", "uid-cm-yaml", 6101, v);
        let t = configmap_to_timing(&o).unwrap();
        assert_eq!(t.downstream_idle_timeout_secs, 1800); // from YAML, not the flat `1`
        assert_eq!(t.upstream_idle_timeout_secs, 3); // from YAML, not the flat `2`
        assert_eq!(t.max_request_headers_kb, Some(60));
    }

    #[test]
    fn configmap_timing_unparseable_higress_falls_back_to_flat() {
        // A malformed `higress` doc never panics; we warn and fall back to flat keys.
        let v = json!({
            "metadata": { "name": "higress-config" },
            "data": {
                "higress": "downstream: [not valid yaml ::",
                "upstream.idleTimeout": "7"
            }
        });
        let o = obj(ObjectKind::ConfigMap, "higress-config", "higress-system", "uid-cm", 6001, v);
        let t = configmap_to_timing(&o).unwrap();
        // downstream falls back to the default (no usable source); upstream from the flat key.
        assert_eq!(t.downstream_idle_timeout_secs, 1800);
        assert_eq!(t.upstream_idle_timeout_secs, 7);
    }

    // ----- full snapshot assembly -----

    fn all_objects() -> Vec<Object> {
        vec![
            obj(ObjectKind::Ingress, "ai-route-route-5.internal", "higress-system", "uid-main-5", 1001, main_ingress()),
            obj(ObjectKind::Ingress, "ai-route-route-5.fallback.internal", "higress-system", "uid-fallback-5", 1002, fallback_ingress()),
            obj(ObjectKind::Ingress, "gpustack", "higress-system", "uid-mirror", 1003, mirror_ingress()),
            obj(ObjectKind::McpBridge, "default", "higress-system", "uid-bridge", 2001, mcpbridge()),
            obj(ObjectKind::WasmPlugin, "gpustack-model-mapper", "higress-system", "uid-mapper", 3001, model_mapper_plugin()),
            obj(ObjectKind::EnvoyFilter, "ai-route-route-5.internal", "higress-system", "uid-ef-5", 4001, fallback_envoyfilter()),
            obj(ObjectKind::Secret, "gpustack-tls-default", "higress-system", "uid-tls", 5001, tls_secret()),
            obj(ObjectKind::ConfigMap, "higress-config", "higress-system", "uid-cm", 6001, higress_configmap()),
        ]
    }

    #[test]
    fn build_full_snapshot() {
        let objects = all_objects();
        let data = build_config_data(&objects, "higress-system", MIRROR_NAME);

        assert_eq!(data.routes.len(), 3);
        assert_eq!(data.registries.len(), 4);
        assert_eq!(data.proxies.len(), 1);
        assert_eq!(data.features.len(), 1);
        assert_eq!(data.tls.hosts.len(), 1);
        assert_eq!(data.timing.upstream_idle_timeout_secs, 3);

        // Main route: key, fallback link, and per-destination model mapping merged in.
        let main = data.routes.iter().find(|r| r.kind == RouteKind::Main).unwrap();
        assert_eq!(main.key, "org1/llama-3-8b");
        let link = main.fallback.as_ref().unwrap();
        assert_eq!(link.target_key, "ai-route-route-5.internal");
        // D9: embedded (ns == gateway ns) -> the link's main ingress is bare.
        assert_eq!(link.main_ingress_name, "ai-route-route-5.internal");
        assert_eq!(link.max_redirects, 10);
        assert!(link.use_original_request);
        // model-mapper rules merged: both static destinations map to the effective model.
        assert_eq!(main.model_mapping.lookup("model-5-12.static"), Some("llama-3-8b-instruct"));
        assert_eq!(main.model_mapping.lookup("model-5-13.static"), Some("llama-3-8b-instruct"));

        // Fallback route: keyed by the main ingress name, with its own model mapping merged.
        let fb = data.routes.iter().find(|r| r.kind == RouteKind::Fallback).unwrap();
        assert_eq!(fb.key, "ai-route-route-5.internal");
        assert_eq!(fb.ingress_name, "ai-route-route-5.fallback.internal");
        assert_eq!(fb.model_mapping.lookup("model-5-12.static"), Some("llama-3-8b-instruct"));

        // Mirror: no fallback link, no auth.
        let mirror = data.routes.iter().find(|r| r.kind == RouteKind::Mirror).unwrap();
        assert_eq!(mirror.key, "gpustack");
        assert!(mirror.fallback.is_none());
        assert!(!mirror.requires_auth());

        // Fallback spec is derivable from the main route link.
        let specs = data.fallbacks();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].route_key, "org1/llama-3-8b");
        assert_eq!(specs[0].target_key, "ai-route-route-5.internal");
    }

    #[test]
    fn build_snapshot_ignores_legacy_ingress() {
        // A legacy `ai-route-model-<id>` ingress is ignored (cleanup-only).
        let legacy = json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {
                "name": "ai-route-model-5",
                "namespace": "higress-system",
                "labels": managed_labels(),
                "annotations": { "higress.io/destination": "100% model-5-12.static:80" }
            },
            "spec": { "rules": [ { "http": { "paths": [ { "path": "/", "pathType": "Prefix" } ] } } ] }
        });
        let mut objects = all_objects();
        objects.push(obj(ObjectKind::Ingress, "ai-route-model-5", "higress-system", "uid-legacy", 9999, legacy));
        let data = build_config_data(&objects, "higress-system", MIRROR_NAME);
        // Still 3 routes (the legacy one is dropped, not a route).
        assert_eq!(data.routes.len(), 3);
        assert!(data.routes.iter().all(|r| r.ingress_name != "higress-system/ai-route-model-5"));
    }

    #[test]
    fn bad_route_dropped_not_whole_snapshot() {
        // A main ingress with an unparseable destination (unknown service type) is dropped;
        // the valid mirror + fallback routes survive.
        let bad = json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {
                "name": "ai-route-route-6.internal",
                "namespace": "higress-system",
                "uid": "uid-bad",
                "resourceVersion": "7001",
                "labels": managed_labels(),
                "annotations": {
                    "higress.io/destination": "100% model-6-1.bogus:80",
                    "higress.io/exact-match-header-x-higress-llm-model": "org2/bad-route"
                }
            },
            "spec": { "rules": [ { "http": { "paths": [ { "path": "/", "pathType": "Prefix" } ] } } ] }
        });
        let mut objects = all_objects();
        objects.push(obj(ObjectKind::Ingress, "ai-route-route-6.internal", "higress-system", "uid-bad", 7001, bad));

        // The bad route fails at RouteRule construction (unknown service type) → dropped.
        let data = build_config_data(&objects, "higress-system", MIRROR_NAME);
        assert_eq!(data.routes.len(), 3);
        assert!(data
            .routes
            .iter()
            .all(|r| r.key != "org2/bad-route" && r.ingress_name != "higress-system/ai-route-route-6.internal"));

        // The snapshot still passes the core structural validation (only structural failures
        // reject the whole snapshot); a per-object bad route is a skip, not a reject.
        assert!(hygress_core::SharedConfig::new(data).is_ok());
    }

    #[test]
    fn fallback_link_requires_fallback_route() {
        // Remove the fallback ingress: the main route must not get a fallback link (its
        // EnvoyFilter alone is not enough — the Fallback route key must exist).
        let objects: Vec<Object> = all_objects()
            .into_iter()
            .filter(|o| o.name != "ai-route-route-5.fallback.internal")
            .collect();
        let data = build_config_data(&objects, "higress-system", MIRROR_NAME);
        let main = data.routes.iter().find(|r| r.kind == RouteKind::Main).unwrap();
        assert!(main.fallback.is_none());
        assert!(data.fallbacks().is_empty());
    }

    #[test]
    fn main_destinations_empty_uses_fallback_copies() {
        // GPUStack copies fallback destinations into the main ingress when the main list is
        // empty. We read whatever the main ingress carries — a main with copied fallback
        // destinations translates fine.
        let main = json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": {
                "name": "ai-route-route-8.internal",
                "namespace": "higress-system",
                "uid": "uid-main-8",
                "resourceVersion": "8001",
                "labels": managed_labels(),
                "annotations": {
                    "higress.io/destination": "100% model-8-20.static:80",
                    "higress.io/exact-match-header-x-higress-llm-model": "org8/fallback-model"
                }
            },
            "spec": { "rules": [ { "http": { "paths": [ { "path": "/", "pathType": "Prefix" } ] } } ] }
        });
        let o = obj(ObjectKind::Ingress, "ai-route-route-8.internal", "higress-system", "uid-main-8", 8001, main);
        let route = ingress_to_route(&o, RouteKind::Main, "higress-system").unwrap();
        assert_eq!(route.destinations.len(), 1);
        assert_eq!(route.destinations[0].service, "model-8-20.static:80");
    }

    #[test]
    fn duplicate_main_and_fallback_key_coexist() {
        // Main and Fallback share the same key string (the main ingress name). They live in
        // separate key spaces and both are kept (design §6.2).
        let data = build_config_data(&all_objects(), "higress-system", MIRROR_NAME);
        let main = data.routes.iter().find(|r| r.kind == RouteKind::Main).unwrap();
        // The fallback key equals the main's ingress name; the main's key (model) differs.
        assert_eq!(main.key, "org1/llama-3-8b");
        let fb = data.routes.iter().find(|r| r.kind == RouteKind::Fallback).unwrap();
        assert_eq!(fb.key, "ai-route-route-5.internal");
        // Both present in the table.
        assert!(data.routes.iter().any(|r| r.kind == RouteKind::Main && r.key == "org1/llama-3-8b"));
        assert!(data.routes.iter().any(|r| r.kind == RouteKind::Fallback && r.key == "ai-route-route-5.internal"));
    }

    #[test]
    fn snapshot_roundtrips_through_core_store() {
        // End-to-end: the assembled snapshot stores via the real core SharedConfig and the
        // built RouteTable has the expected Main/Fallback/Mirror layout.
        let data = build_config_data(&all_objects(), "higress-system", MIRROR_NAME);
        let shared = hygress_core::SharedConfig::new(data).unwrap();
        let table = shared.route_table().unwrap();
        assert_eq!(table.routes().len(), 3);
        // Main header match.
        let m = table.find_match(Some("org1/llama-3-8b"), "/v1/chat/completions").unwrap();
        assert_eq!(m.matched_by, hygress_core::MatchKind::HeaderExact);
        // Fallback via fallback-from.
        let f = table
            .find_match_fallback(Some("ai-route-route-5.internal"), "/v1/chat/completions")
            .unwrap();
        assert_eq!(f.matched_by, hygress_core::MatchKind::FallbackExact);
        // Mirror catch-all.
        let mir = table.find_match(None, "/token-auth").unwrap();
        assert_eq!(mir.matched_by, hygress_core::MatchKind::Mirror);
    }

    #[test]
    fn managed_label_filter() {
        let mut v = mirror_ingress();
        // Unmanaged object: no managed label -> not consumed.
        v["metadata"]["labels"] = json!({});
        let o = obj(ObjectKind::Ingress, "gpustack", "higress-system", "uid-mirror", 1003, v);
        assert!(!o.is_managed());
    }
}
