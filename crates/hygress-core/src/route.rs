//! Route rule data model (design §6.2).
//!
//! A [`RouteRule`] is the internal translation of one GPUStack Ingress:
//!
//! - `ai-route-route-<id>.internal`       → `RouteRule(kind = Main)`
//! - `ai-route-route-<id>.fallback...`    → `RouteRule(kind = Fallback)`
//! - `gpustack` (mirror)                  → `RouteRule(kind = Mirror)`
//!
//! The `key` is the exact-match header key: `x-higress-llm-model` for main
//! routes (the model/route name) and `x-higress-fallback-from` for fallback
//! routes (the main ingress name).

use serde::{Deserialize, Serialize};

use crate::destination::Destination;
use crate::error::Error;
use crate::model_mapping::ModelMapping;
use crate::retry::RetryPolicy;

/// Route class (design §5.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteKind {
    /// `ai-route-route-<id>.internal` — the primary model route.
    Main,
    /// `ai-route-route-<id>.fallback.internal` — reached via
    /// `x-higress-fallback-from` internal redirect on 4xx/5xx.
    Fallback,
    /// The `gpustack` mirror ingress — direct pass-through to the GPUStack
    /// server, never authenticated, no percent-weighted destinations.
    Mirror,
}

/// One Ingress path regex predicate (from `spec.rules[].http.paths[]`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathPred {
    /// The regex form of the Ingress path (`ImplementationSpecific`), e.g.
    /// `()/chat/completions(/|$)(.*)`.
    pub regex: String,
    /// `higress.io/ignore-path-case: true`.
    #[serde(default)]
    pub ignore_case: bool,
}

impl PathPred {
    pub fn new(regex: impl Into<String>) -> Self {
        Self {
            regex: regex.into(),
            ignore_case: false,
        }
    }

    /// Same predicate with case-insensitive matching.
    pub fn ignore_case(mut self) -> Self {
        self.ignore_case = true;
        self
    }
}

/// Capture-group path rewriter (`higress.io/rewrite-target`, e.g. `/$1$3`).
///
/// `$$` renders a literal `$`; `$1`..`$9` replace the Nth (1-based) capture
/// group, empty when the group is absent from the match.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRewriter {
    pub target: String,
}

impl PathRewriter {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
        }
    }

    /// Apply the rewrite against the captured groups of a matched path.
    pub fn rewrite(&self, groups: &[impl AsRef<str>]) -> String {
        let chars: Vec<char> = self.target.chars().collect();
        let mut out = String::with_capacity(chars.len());
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            if c == '$' && i + 1 < chars.len() {
                match chars[i + 1] {
                    '$' => {
                        out.push('$');
                        i += 2;
                        continue;
                    }
                    '1'..='9' => {
                        let idx = (chars[i + 1] as u8 - b'1') as usize;
                        if idx < groups.len() {
                            out.push_str(groups[idx].as_ref());
                        }
                        i += 2;
                        continue;
                    }
                    _ => {}
                }
            }
            out.push(c);
            i += 1;
        }
        out
    }
}

/// 4xx/5xx → fallback ingress link (`higress.io` fallback Ingress + the
/// EnvoyFilter `custom_response` redirect, design §2.1.2 / §6.1 ⑭).
///
/// GPUStack semantics (authoritative): the fallback Ingress is a **separate**
/// object named `<main>.fallback.internal` whose exact header matcher is
/// `x-higress-fallback-from = <main ingress name>`. So `target_key` (the
/// Fallback route key) is the **main** ingress's name — it is the value the
/// internal redirect sets in `x-higress-fallback-from`, and it is what a
/// Fallback route's `key` holds. `main_ingress_name` records the origin
/// identity (ns-qualified) that the fallback redirects from; it is the single
/// canonical reference the derived [`crate::config::FallbackSpec`] uses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FallbackLink {
    /// Fallback route key — the value the internal redirect sets in
    /// `x-higress-fallback-from` (= the main ingress's name; the key of the
    /// linked Fallback route).
    pub target_key: String,
    /// The origin (main) ingress name this fallback redirects from,
    /// ns-qualified as GPUStack writes it (e.g.
    /// `higress-system/ai-route-route-5.internal`). Canonical reference for
    /// the derived `FallbackSpec`.
    #[serde(default)]
    pub main_ingress_name: String,
    /// Max internal redirects (EnvoyFilter: 10).
    #[serde(default = "default_max_redirects")]
    pub max_redirects: u32,
    /// `use_original_request_body` / `use_original_uri` (both default true).
    #[serde(default = "default_true")]
    pub use_original_request: bool,
}

impl FallbackLink {
    pub fn new(target_key: impl Into<String>) -> Self {
        let target_key = target_key.into();
        Self {
            // Default: the main ingress name is the fallback route key (bare
            // form). The caller upgrades this to the ns-qualified identity
            // via `with_main_ingress_name` when it knows the namespace.
            main_ingress_name: target_key.clone(),
            target_key,
            max_redirects: default_max_redirects(),
            use_original_request: true,
        }
    }

    /// Set the ns-qualified origin (main) ingress name this fallback links to.
    pub fn with_main_ingress_name(mut self, main_ingress_name: impl Into<String>) -> Self {
        self.main_ingress_name = main_ingress_name.into();
        self
    }
}

fn default_max_redirects() -> u32 {
    10
}

fn default_true() -> bool {
    true
}

/// ext-auth scoping (design §9): scope is the **origin ingress name prefix**
/// `ai-route-route-` — never a path prefix (that would open a FAIL_OPEN hole).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthScope {
    /// `false` for mirror routes (never authenticated).
    pub enabled: bool,
    /// Ingress-name prefix that gates auth (default `ai-route-route-`).
    pub scope_root: String,
}

impl AuthScope {
    /// Default scope root (`ai-route-route-` prefix on the origin ingress
    /// name).
    pub const DEFAULT_SCOPE_ROOT: &'static str = "ai-route-route-";

    /// Build the natural scope for a route kind: Main/Fallback are
    /// auth-scoped to `ai-route-route-`; Mirror is never authed.
    pub fn for_kind(kind: RouteKind) -> Self {
        match kind {
            RouteKind::Mirror => Self {
                enabled: false,
                scope_root: String::new(),
            },
            RouteKind::Main | RouteKind::Fallback => Self {
                enabled: true,
                scope_root: Self::DEFAULT_SCOPE_ROOT.to_string(),
            },
        }
    }

    /// Whether a request hitting a route whose **origin ingress name** is
    /// `origin_ingress` must pass ext-auth.
    ///
    /// The origin ingress name may carry an optional `gateway_namespace/`
    /// prefix (design §9); it is stripped before the prefix check.
    pub fn should_auth(&self, origin_ingress: &str) -> bool {
        if !self.enabled || self.scope_root.is_empty() {
            return false;
        }
        let name = origin_ingress.rsplit('/').next().unwrap_or(origin_ingress);
        name.starts_with(&self.scope_root)
    }
}

/// Provenance of one contributing k8s object (snapshot diff + orphan
/// tolerance, design §6.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleSource {
    /// k8s object `metadata.uid`.
    pub uid: String,
    /// k8s object `metadata.resourceVersion`.
    pub resource_version: u64,
    /// The origin ingress name (ns-qualified, as GPUStack writes it, e.g.
    /// `higress-system/ai-route-route-5.internal`) this object contributed to.
    /// Empty for objects that do not carry an ingress provenance.
    #[serde(default)]
    pub ingress_name: String,
}

impl RuleSource {
    pub fn new(uid: impl Into<String>, resource_version: u64) -> Self {
        Self {
            uid: uid.into(),
            resource_version,
            ingress_name: String::new(),
        }
    }

    /// Attach the origin ingress name this source object contributes to.
    pub fn with_ingress_name(mut self, ingress_name: impl Into<String>) -> Self {
        self.ingress_name = ingress_name.into();
        self
    }
}

/// One routing rule (design §6.2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteRule {
    /// Exact-match key: model/route name (Main) or main ingress name
    /// (Fallback), or the mirror ingress name (Mirror).
    pub key: String,
    /// Origin ingress name **as GPUStack writes it, ns-qualified** (e.g.
    /// `higress-system/ai-route-route-5.internal`). This is the provenance
    /// identity carried by the Ingress object and the reference
    /// [`AuthScope::should_auth`] is consulted with (see
    /// [`RouteRule::requires_auth`]).
    #[serde(default)]
    pub ingress_name: String,
    pub kind: RouteKind,
    /// Path regex predicates from the Ingress paths.
    #[serde(default)]
    pub path_predicates: Vec<PathPred>,
    /// `higress.io/rewrite-target` (e.g. `/$1$3`).
    #[serde(default)]
    pub rewrite_target: Option<PathRewriter>,
    /// Weighted destination list (percent optional — mirror has none).
    pub destinations: Vec<Destination>,
    /// `higress.io/proxy-next-upstream(-tries)` (default: error, timeout,
    /// 503, 502, non_idempotent, 2 tries).
    #[serde(default)]
    pub retry: RetryPolicy,
    /// 4xx/5xx fallback link.
    #[serde(default)]
    pub fallback: Option<FallbackLink>,
    pub auth_scope: AuthScope,
    /// Per-destination (`name.type`) → outbound body model name.
    #[serde(default)]
    pub model_mapping: ModelMapping,
    /// Origin ingress identities (snapshot diff).
    #[serde(default)]
    pub sources: Vec<RuleSource>,
}

impl RouteRule {
    /// Construct and validate a route rule.
    ///
    /// Validation: non-empty `key`; at least one destination (GPUStack copies
    /// fallback destinations into the main Ingress when the main list is
    /// empty, so both forms are non-empty at the CRD level — design §6.1);
    /// every destination parses to a known `name.type[:port]`; mirror routes
    /// carry no auth.
    pub fn new(
        key: impl Into<String>,
        kind: RouteKind,
        path_predicates: Vec<PathPred>,
        destinations: Vec<Destination>,
    ) -> Result<Self, Error> {
        let key = key.into();
        let rule = Self {
            // Default to the bare key; the adapter upgrades this to the
            // ns-qualified form via `with_ingress_name` when it knows the
            // namespace (design §9 origin identity).
            ingress_name: key.clone(),
            key,
            kind,
            path_predicates,
            rewrite_target: None,
            destinations,
            retry: RetryPolicy::default(),
            fallback: None,
            auth_scope: AuthScope::for_kind(kind),
            model_mapping: ModelMapping::default(),
            sources: Vec::new(),
        };
        rule.validate()?;
        Ok(rule)
    }

    /// Set the origin ingress name (ns-qualified as GPUStack writes it).
    pub fn with_ingress_name(mut self, ingress_name: impl Into<String>) -> Self {
        self.ingress_name = ingress_name.into();
        self
    }

    /// Validate invariants (same set enforced by the constructor).
    pub fn validate(&self) -> Result<(), Error> {
        if self.key.is_empty() {
            return Err(Error::invalid("route key must be non-empty"));
        }
        if self.kind == RouteKind::Mirror && self.auth_scope.enabled {
            return Err(Error::invalid(
                "mirror route must not enable auth (mirror is never authenticated)",
            ));
        }
        if self.destinations.is_empty() {
            return Err(Error::invalid("route must have at least one destination"));
        }
        for d in &self.destinations {
            if let Err(e) = d.service_ref() {
                return Err(Error::invalid(format!("destination '{}': {e}", d.service)));
            }
        }
        Ok(())
    }

    /// Attach a rewrite target.
    pub fn with_rewrite_target(mut self, target: impl Into<String>) -> Self {
        self.rewrite_target = Some(PathRewriter::new(target));
        self
    }

    /// Attach a fallback link.
    pub fn with_fallback(mut self, link: FallbackLink) -> Self {
        self.fallback = Some(link);
        self
    }

    /// Replace the retry policy.
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Replace the per-destination model mapping.
    pub fn with_model_mapping(mut self, mapping: ModelMapping) -> Self {
        self.model_mapping = mapping;
        self
    }

    /// Record an origin ingress source.
    pub fn with_source(mut self, source: RuleSource) -> Self {
        self.sources.push(source);
        self
    }

    /// Rewrite the matched path if this rule defines a rewrite target.
    pub fn rewrite_path(&self, groups: &[impl AsRef<str>]) -> Option<String> {
        self.rewrite_target.as_ref().map(|r| r.rewrite(groups))
    }

    /// Should a request for an origin ingress pass ext-auth?
    pub fn auth_required(&self, origin_ingress: &str) -> bool {
        self.auth_scope.should_auth(origin_ingress)
    }

    /// Whether **this rule** requires ext-auth, decided from its stored
    /// [`RouteRule::ingress_name`]. Mirrors of a route (auth disabled) and
    /// any ingress outside the `ai-route-route-` scope return `false`.
    pub fn requires_auth(&self) -> bool {
        self.auth_scope.should_auth(&self.ingress_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn dest(svc: &str) -> Destination {
        Destination::new(svc)
    }

    #[test]
    fn construct_main_route() {
        let r = RouteRule::new(
            "org1/llama-3-8b",
            RouteKind::Main,
            vec![PathPred::new("()/chat/completions(/|$)(.*)")],
            vec![dest("model-1-10.static:80")],
        )
        .unwrap();
        assert_eq!(r.kind, RouteKind::Main);
        assert!(r.auth_scope.enabled);
        assert_eq!(r.auth_scope.scope_root, "ai-route-route-");
        assert_eq!(r.retry, RetryPolicy::default());
    }

    #[test]
    fn empty_key_rejected() {
        let e = RouteRule::new(
            "",
            RouteKind::Main,
            vec![PathPred::new("/")],
            vec![dest("model-1.static:80")],
        )
        .unwrap_err();
        assert!(matches!(e, Error::Invalid(_)));
    }

    #[test]
    fn mirror_route_has_no_auth() {
        let r = RouteRule::new(
            "gpustack",
            RouteKind::Mirror,
            vec![PathPred::new("/")],
            vec![dest("gpustack.dns:30080")],
        )
        .unwrap();
        assert!(!r.auth_scope.enabled);
        assert!(!r.auth_required("ai-route-route-1.internal"));
    }

    #[test]
    fn mirror_with_auth_enabled_rejected() {
        let mut r = RouteRule {
            key: "gpustack".to_string(),
            ingress_name: "gpustack".to_string(),
            kind: RouteKind::Mirror,
            path_predicates: vec![PathPred::new("/")],
            rewrite_target: None,
            destinations: vec![dest("gpustack.dns:30080")],
            retry: RetryPolicy::default(),
            fallback: None,
            auth_scope: AuthScope {
                enabled: true,
                scope_root: "ai-route-route-".into(),
            },
            model_mapping: ModelMapping::default(),
            sources: vec![],
        };
        assert!(r.validate().is_err());
        r.auth_scope.enabled = false;
        assert!(r.validate().is_ok());
    }

    #[test]
    fn empty_destinations_rejected() {
        assert!(RouteRule::new("m", RouteKind::Main, vec![PathPred::new("/")], vec![],).is_err());
    }

    #[test]
    fn bad_destination_service_rejected() {
        assert!(RouteRule::new(
            "m",
            RouteKind::Main,
            vec![PathPred::new("/")],
            vec![dest("svc.unknown:80")],
        )
        .is_err());
        assert!(RouteRule::new(
            "m",
            RouteKind::Main,
            vec![PathPred::new("/")],
            vec![dest("not-a-service")],
        )
        .is_err());
    }

    #[test]
    fn rewrite_target_substitutes_groups() {
        // GPUStack form: pattern /()model/proxy/\d+(/|$)(.*) rewrites to /$1$3.
        // For /model/proxy/5/chat/completions: $1="" (empty group), $2="/",
        // $3="chat/completions" -> "/chat/completions".
        let r = PathRewriter::new("/$1$3");
        assert_eq!(
            r.rewrite(&["", "/", "chat/completions"]),
            "/chat/completions"
        );
        // Missing group renders empty.
        let empty: &[String] = &[];
        assert_eq!(r.rewrite(empty), "/");
        // $$ is a literal $.
        assert_eq!(PathRewriter::new("a$$b").rewrite(empty), "a$b");
        // Higher groups: $3 alone.
        assert_eq!(PathRewriter::new("$3").rewrite(&["", "x", "tail"]), "tail");
        assert_eq!(PathRewriter::new("$1x").rewrite(&["hey"]), "heyx");
    }

    #[test]
    fn rule_rewrite_path() {
        let r = RouteRule::new(
            "m",
            RouteKind::Main,
            vec![],
            vec![dest("model-1.static:80")],
        )
        .unwrap()
        .with_rewrite_target("/$1$3");
        assert_eq!(
            r.rewrite_path(&["", "/", "chat/completions"]),
            Some("/chat/completions".to_string())
        );
        let r2 = RouteRule::new(
            "m",
            RouteKind::Main,
            vec![],
            vec![dest("model-1.static:80")],
        )
        .unwrap();
        let empty: &[String] = &[];
        assert_eq!(r2.rewrite_path(empty), None);
    }

    #[test]
    fn fallback_link_defaults() {
        let f = FallbackLink::new("ai-route-route-1.internal");
        assert_eq!(f.max_redirects, 10);
        assert!(f.use_original_request);
        // serde defaults apply on deserialization.
        let f2: FallbackLink = serde_json::from_str("{\"target_key\":\"x\"}").unwrap();
        assert_eq!(
            f2,
            FallbackLink {
                target_key: "x".to_string(),
                // serde default: main_ingress_name is empty when absent on the wire.
                main_ingress_name: String::new(),
                max_redirects: 10,
                use_original_request: true,
            }
        );
    }

    #[test]
    fn fallback_link_serde_full() {
        let f = FallbackLink {
            target_key: "k".into(),
            main_ingress_name: "ns/k.internal".into(),
            max_redirects: 5,
            use_original_request: false,
        };
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["target_key"], "k");
        assert_eq!(v["main_ingress_name"], "ns/k.internal");
        assert_eq!(v["max_redirects"], 5);
        assert_eq!(v["use_original_request"], false);
    }

    #[test]
    fn auth_scope_prefix_gating() {
        let s = AuthScope::for_kind(RouteKind::Main);
        assert!(s.should_auth("ai-route-route-42.internal"));
        assert!(s.should_auth("higress-system/ai-route-route-42.internal"));
        assert!(!s.should_auth("gpustack"));
        assert!(!s.should_auth("ai-route-model-3")); // legacy pattern must not match
        let mirror = AuthScope::for_kind(RouteKind::Mirror);
        assert!(!mirror.should_auth("ai-route-route-42.internal"));
    }

    #[test]
    fn rule_source_roundtrip() {
        let s = RuleSource::new("uid-1", 4242);
        let v = serde_json::to_value(&s).unwrap();
        assert_eq!(v["uid"], "uid-1");
        assert_eq!(v["resource_version"], 4242);
    }

    // ----- origin ingress name (finding #2) -----

    #[test]
    fn ingress_name_defaults_to_key_and_is_overridable() {
        let r = RouteRule::new(
            "org1/llama-3-8b",
            RouteKind::Main,
            vec![PathPred::new("()/chat/completions(/|$)(.*)")],
            vec![dest("model-1-10.static:80")],
        )
        .unwrap();
        assert_eq!(r.ingress_name, "org1/llama-3-8b");
        // The adapter upgrades to the ns-qualified origin identity GPUStack writes.
        let r2 = r.with_ingress_name("higress-system/ai-route-route-5.internal");
        assert_eq!(r2.ingress_name, "higress-system/ai-route-route-5.internal");
        let v: Value = serde_json::to_value(&r2).unwrap();
        assert_eq!(
            v["ingress_name"],
            Value::String("higress-system/ai-route-route-5.internal".into())
        );
    }

    #[test]
    fn requires_auth_works_off_stored_ingress_name() {
        // Main route: the ns-qualified origin name still resolves to auth.
        let main = RouteRule::new(
            "llama",
            RouteKind::Main,
            vec![PathPred::new("/")],
            vec![dest("model-1-10.static:80")],
        )
        .unwrap()
        .with_ingress_name("higress-system/ai-route-route-5.internal");
        assert!(main.requires_auth());
        // A mirror (auth disabled) never requires auth even with a scoped name.
        let mirror = RouteRule::new(
            "gpustack",
            RouteKind::Mirror,
            vec![PathPred::new("/")],
            vec![dest("gpustack.dns:30080")],
        )
        .unwrap()
        .with_ingress_name("higress-system/ai-route-route-9.internal");
        assert!(!mirror.requires_auth());
    }

    #[test]
    fn fallback_link_references_main_ingress_name() {
        // GPUStack: the fallback Ingress's exact matcher is
        // x-higress-fallback-from = <main ingress name>. target_key (the
        // Fallback route key) equals the main ingress name, and the link also
        // records the ns-qualified origin identity.
        let link = FallbackLink::new("ai-route-route-5.internal")
            .with_main_ingress_name("higress-system/ai-route-route-5.internal");
        assert_eq!(link.target_key, "ai-route-route-5.internal");
        assert_eq!(link.main_ingress_name, "higress-system/ai-route-route-5.internal");
        // `new` seeds main_ingress_name with the (bare) target_key so a link
        // built without the namespace is still consistent.
        let bare = FallbackLink::new("ai-route-route-5.internal");
        assert_eq!(bare.main_ingress_name, "ai-route-route-5.internal");
    }

    #[test]
    fn rule_source_carries_ingress_name() {
        let s = RuleSource::new("uid-1", 4242)
            .with_ingress_name("higress-system/ai-route-route-5.internal");
        assert_eq!(s.ingress_name, "higress-system/ai-route-route-5.internal");
        // serde default keeps old payloads deserialisable.
        let s2: RuleSource = serde_json::from_str(r#"{"uid":"u","resource_version":1}"#).unwrap();
        assert_eq!(s2.ingress_name, "");
    }
}
