//! Policy file loading + hot-reload handle + global/route merge (design §2.1 /
//! §3 / D-7 / D-12).
//!
//! ## Loading (design §2.1 / §7)
//!
//! [`load_policy`] reads `HYGRESS_POLICY_PATH` (default
//! [`DEFAULT_POLICY_PATH`]) as YAML:
//! - **missing file** → `Ok(PolicyConfig::default())` (all-pass, D-7);
//! - **empty / whitespace-only file** → the same all-pass default;
//! - **malformed YAML / schema** → `Err` (the caller keeps the last-known-good
//!   and warns — never a silent degrade).
//!
//! [`PolicyHandle`] owns the live `ArcSwap<PolicyConfig>` (cheap per-request
//! load, no lock) plus the source path and an mtime watermark. Two reload
//! entry points:
//! - [`PolicyHandle::poll`] — the mtime-poll tick, invoked on the gateway's
//!   30s dutycycle (bootstrap; only reloads when the file actually changed);
//! - [`PolicyHandle::reload_from`] — the admin `POST /reload` path (forced).
//!
//! Both keep the last-known-good value (and warn) on failure (design §7:
//! last-known-good semantics). ORA3-M2: a **disappeared** policy file is a
//! failure, never a silent downgrade — [`PolicyHandle::reload_from`] keeps the
//! last-known-good (when one was ever loaded) and reports `false`, and the
//! initial load of an absent file warns once that limits/quota/guardrails are
//! effectively disabled (all-pass).
//!
//! ## Merge (design §3: `global` defaults → `routes` override)
//!
//! Core selects the last-matching route spec ([`PolicyConfig::for_ingress`],
//! D-12); **merging** the matched route's fields over the `global` defaults is
//! the gateway's job ([`merge_policy`]):
//! - `limits` — per-dimension override (the route's `consumer` wins over the
//!   global one; an absent dimension means "no limit for that dimension").
//!   **v1 boundary:** only the `consumer` dimension is route-aware
//!   (`rate_limit_post` runs after `route_match`); the `ip` dimension is
//!   evaluated by `rate_limit_pre` **before** the route is known, so it reads
//!   the **global** `limits.ip` only — a route-level `ip` override is honored
//!   nowhere in v1 (document, do not configure it expecting effect);
//! - `quota` — the route's `by_model_tokens` wins over the global's;
//! - `guardrail` — **inherit + append** the static rules (route rules appended
//!   after the global ones), the route's `llm`/`fail_mode` win when present;
//! - `actions` — route-level only (there is no `global` routing-policy slot).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use hygress_core::prelude::{
    GlobalPolicy, GuardrailSpec, LimitsSpec, PolicyConfig, QuotaSpec, RoutePolicyActions,
    RoutePolicySpec, StaticRuleSet, StaticRuleSpec,
};
use tracing::{error, warn};

/// The default policy file location (design §2.1 / D-7).
pub const DEFAULT_POLICY_PATH: &str = "/etc/hygress/policy.yaml";

/// Load a policy file (see the module docs for the missing/empty/malformed
/// semantics). Pure file I/O + parse — no logging (the handle logs).
pub fn load_policy(path: impl AsRef<Path>) -> Result<PolicyConfig, String> {
    let path = path.as_ref();
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Missing file = empty policy (default all-pass), design §7.
            return Ok(PolicyConfig::default());
        }
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    if bytes.iter().all(|b| b.is_ascii_whitespace()) {
        // An empty file is the all-pass default (not a parse error).
        return Ok(PolicyConfig::default());
    }
    serde_yaml_ng::from_slice(&bytes).map_err(|e| format!("parse {}: {e}", path.display()))
}

/// The live policy holder: an `ArcSwap<PolicyRuntime>` (lock-free per-request
/// load of the config **and** its precomputed merged-policy index) plus the
/// source path, an mtime watermark for the poll, and a "loaded once" flag that
/// drives the ORA3-M2 missing-file reload behaviour.
///
/// The per-request policy cost is one `Arc` load ([`PolicyHandle::shared`] /
/// [`PolicyHandle::merged_for`]); the merge (and static-rule regex compilation)
/// happens once per load/reload in `MergedTable::build` (H3).
///
/// Cheap to `Arc`-clone (all state is `Arc`-shared); the admin `/reload`
/// closure and the poll task share one handle.
#[derive(Clone)]
pub struct PolicyHandle {
    inner: Arc<PolicyInner>,
    /// O5: optional per-attempt observer fired by [`PolicyHandle::reload_from`]
    /// (`true` = swap happened, `false` = last-known-good kept). Lives on the
    /// handle (not `PolicyInner`) so every clone shares the same observer `Arc`
    /// once it is set — bootstrap attaches it immediately after construction,
    /// before any clone is made. `None` (default) keeps reloads log-only.
    reload_observer: Option<Arc<dyn Fn(bool) + Send + Sync>>,
}

/// One consistent, atomically-swapped policy view: the parsed config plus its
/// precomputed merge index.
struct PolicyRuntime {
    config: Arc<PolicyConfig>,
    merged: Arc<MergedTable>,
}

struct PolicyInner {
    state: ArcSwap<PolicyRuntime>,
    path: String,
    /// The last observed file mtime (`None` when the file is absent) — the
    /// poll reloads only when this changes.
    last_mtime: std::sync::Mutex<Option<SystemTime>>,
    /// ORA3-M2: true once a *present* policy file was loaded successfully
    /// (initial load or reload). Distinguishes "keep the last-known-good" from
    /// "nothing real was ever loaded" when the file disappears before /reload.
    loaded_once: AtomicBool,
}

impl PolicyHandle {
    /// Build the handle and perform the **initial** load from `path`.
    ///
    /// A missing file starts as the all-pass default **with a warn** (ORA3-M2:
    /// a missing/mis-mounted policy must not silently disable limits/quota/
    /// guardrails); a malformed file starts as the all-pass default with a warn
    /// too (there is no last-known-good at startup — design §7).
    pub fn new(path: impl Into<String>) -> Self {
        let path = path.into();
        // Present = the path names an existing regular-ish file. A missing file
        // is the built-in all-pass default — warn once so operators see it.
        let present = Path::new(&path).is_file();
        let mut loaded_once = false;
        let initial = match load_policy(&path) {
            Ok(c) => {
                if present {
                    // A real (possibly content-free) policy file was read.
                    loaded_once = true;
                } else {
                    // ORA3-M2 (boot): the loader returned the built-in default
                    // because the file is absent — surface it.
                    warn!(
                        path = %path,
                        "policy file missing at startup; limits/quota/guardrails are effectively disabled (all-pass default)"
                    );
                }
                c
            }
            Err(e) => {
                warn!(
                    path = %path,
                    error = %e,
                    "policy load failed; starting with the default (all-pass) policy"
                );
                PolicyConfig::default()
            }
        };
        let last_mtime = std::fs::metadata(&path)
            .ok()
            .and_then(|m| m.modified().ok());
        Self {
            inner: Arc::new(PolicyInner {
                state: ArcSwap::new(Arc::new(PolicyRuntime::of(initial))),
                path,
                last_mtime: std::sync::Mutex::new(last_mtime),
                loaded_once: AtomicBool::new(loaded_once),
            }),
            reload_observer: None,
        }
    }

    /// O5: attach the per-attempt reload observer (the gateway's metrics
    /// counter). Call immediately after construction, before the handle is
    /// cloned/shared — every later clone shares the same observer `Arc`.
    /// `None` (the default) keeps reloads log-only; unit tests rely on that.
    pub fn set_reload_observer(&mut self, observer: Option<Arc<dyn Fn(bool) + Send + Sync>>) {
        self.reload_observer = observer;
    }

    /// The current policy snapshot (lock-free `Arc` load — cheap per request).
    pub fn shared(&self) -> Arc<PolicyConfig> {
        self.inner.state.load_full().config.clone()
    }

    /// The precomputed effective (merged) policy for one **bare** ingress name
    /// (lock-free `Arc` load — cheap per request). The merge and the static-rule
    /// compilation were done once at load/reload (H3).
    pub fn merged_for(&self, bare_ingress: &str) -> Arc<MergedEntry> {
        let runtime = self.inner.state.load_full();
        runtime.merged.for_ingress(&runtime.config, bare_ingress)
    }

    /// The configured source path.
    pub fn path(&self) -> &str {
        &self.inner.path
    }

    /// Reload (and swap) from `path`.
    ///
    /// Returns `true` when the file was read and parsed and the swap happened.
    /// Returns `false` — **without swapping** — when the file is missing or the
    /// read/parse failed: the last-known-good value is kept and the failure is
    /// logged (design §7). ORA3-M2: a missing file is NEVER reported as a
    /// successful reload, and when a real policy was loaded before it is kept
    /// (no silent downgrade to the built-in all-pass default).
    pub fn reload_from(&self, path: &str) -> bool {
        let result = (|| {
            // ORA3-M2: a disappeared policy file must not silently swap in the
            // all-pass default — keep the last-known-good and report the failure.
            if !Path::new(path).is_file() {
                if self.inner.loaded_once.load(Ordering::SeqCst) {
                    error!(
                        path = %path,
                        "policy file is missing; keeping the last-known-good policy \
                         (limits/quota/guardrails stay in force)"
                    );
                } else {
                    error!(
                        path = %path,
                        "policy file is missing and no policy was ever loaded; \
                         limits/quota/guardrails remain disabled (all-pass default)"
                    );
                }
                return false;
            }
            match load_policy(path) {
                Ok(c) => {
                    self.inner.state.store(Arc::new(PolicyRuntime::of(c)));
                    *self.inner.last_mtime.lock().unwrap() =
                        std::fs::metadata(path).ok().and_then(|m| m.modified().ok());
                    self.inner.loaded_once.store(true, Ordering::SeqCst);
                    true
                }
                Err(e) => {
                    warn!(
                        path = %path,
                        error = %e,
                        "policy reload failed; keeping the last-known-good policy"
                    );
                    false
                }
            }
        })();
        // O5: ONE measured choke point for admin `/reload` and the mtime poll.
        if let Some(obs) = &self.reload_observer {
            obs(result);
        }
        result
    }

    /// Reload from the handle's configured path (the admin `POST /reload`).
    pub fn reload(&self) -> bool {
        self.reload_from(&self.inner.path)
    }

    /// The mtime-poll tick (the gateway invokes it on its 30s dutycycle):
    /// reload **only** when the file's mtime changed.
    ///
    /// A missing file (metadata failure) is a no-op — the handle keeps its
    /// current value (the all-pass default when it never existed, the
    /// last-known-good otherwise).
    pub fn poll(&self) -> bool {
        let mt = match std::fs::metadata(&self.inner.path).and_then(|m| m.modified()) {
            Ok(t) => t,
            Err(_) => return false,
        };
        let changed = *self.inner.last_mtime.lock().unwrap() != Some(mt);
        if !changed {
            return false;
        }
        self.reload()
    }
}

impl PolicyRuntime {
    /// Build one atomically-swapped view: the config + its precomputed merge
    /// index (H3 — the expensive per-ingress merges + regex compilation run
    /// here, not on the request path).
    fn of(config: PolicyConfig) -> Self {
        let merged = Arc::new(MergedTable::build(&config));
        Self {
            config: Arc::new(config),
            merged,
        }
    }
}

// ---------------------------------------------------------------------------
// Merge (design §3: global defaults → matched route override)
// ---------------------------------------------------------------------------

/// The effective (merged) policy for one ingress: the matched route's fields
/// over the `global` defaults (see the module docs for the per-section rules).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MergedPolicy {
    /// Effective rate limits (per-dimension override; `None` = no limits).
    pub limits: Option<LimitsSpec>,
    /// Effective token quota (route `by_model_tokens` wins over global).
    pub quota: Option<QuotaSpec>,
    /// Effective guardrail (static rules = global ++ route; route `llm` /
    /// `fail_mode` win when present).
    pub guardrail: Option<GuardrailSpec>,
    /// The matched route's routing-policy actions (route-level only).
    pub actions: Option<RoutePolicyActions>,
}

/// The bare (namespace-stripped) ingress name — the D-12 policy route key.
/// `higress-system/ai-route-route-1.internal` → `ai-route-route-1.internal`.
pub fn bare_ingress_name(ingress: &str) -> &str {
    ingress.rsplit('/').next().unwrap_or(ingress)
}

/// Merge the matched route (selected by core `for_ingress`, D-12) over the
/// `global` defaults. A non-matching ingress yields the bare global section.
///
/// This is the pure merge used by the (test) direct callers and as the base of
/// the precomputed [`MergedEntry`]; the request path uses
/// [`PolicyHandle::merged_for`] (H3) instead.
pub fn merge_policy(cfg: &PolicyConfig, bare_ingress: &str) -> MergedPolicy {
    merge_spec(&cfg.global, cfg.for_ingress(bare_ingress))
}

/// The pure merge of a (possibly absent) route spec over the `global` defaults.
fn merge_spec(global: &GlobalPolicy, route: Option<&RoutePolicySpec>) -> MergedPolicy {
    match route {
        Some(route) => MergedPolicy {
            limits: merge_limits(global.limits.as_ref(), route.limits.as_ref()),
            quota: merge_quota(global.quota.as_ref(), route.quota.as_ref()),
            guardrail: merge_guardrail(global.guardrail.as_ref(), route.guardrail.as_ref()),
            actions: route.policy.clone(),
        },
        None => MergedPolicy {
            limits: global.limits.clone(),
            quota: global.quota.clone(),
            guardrail: global.guardrail.clone(),
            actions: None,
        },
    }
}

/// A **precomputed** effective policy for one ingress context: the merged spec
/// fields plus the merged static-rule regexes compiled once at load/reload.
///
/// `static_set` is `None` when the effective guardrail has no static rules or a
/// rule failed to compile — the observe / pass-through state (D-14), decided
/// once here instead of per request / per response. The compiled set is
/// `Arc`-shared so the per-response scanner clones a pointer, not the rule vec.
#[derive(Clone, Debug)]
pub struct MergedEntry {
    /// The effective spec fields (`limits` / `quota` / `guardrail` / `actions`).
    pub policy: MergedPolicy,
    /// The compiled effective static rules (observe/pass-through when `None`).
    pub static_set: Option<Arc<StaticRuleSet>>,
}

impl MergedEntry {
    /// Compile a merged policy's static rules once (a compile failure only
    /// disables the static scan — the LLM stage and the rest are unaffected).
    fn compile(policy: MergedPolicy) -> Self {
        let static_set = policy.guardrail.as_ref().and_then(|g| {
            if g.static_rules.is_empty() {
                return None;
            }
            match StaticRuleSet::new(&g.static_rules) {
                Ok(set) => Some(Arc::new(set)),
                Err(e) => {
                    warn!(
                        error = %e,
                        "guardrail static rule compile failed; static scanning disabled"
                    );
                    None
                }
            }
        });
        Self { policy, static_set }
    }
}

/// The precomputed merge index for a whole policy: the global-only section
/// plus one section per route spec, keyed by its `name_glob`.
///
/// The **last** entry for a glob wins (build order = route order), which is
/// exactly `PolicyConfig::for_ingress`'s last-match semantics — a bare ingress
/// that matches a glob resolves to the same precomputed merge the direct merge
/// would produce (H3).
#[derive(Debug)]
struct MergedTable {
    global: Arc<MergedEntry>,
    by_glob: HashMap<String, Arc<MergedEntry>>,
}

impl MergedTable {
    /// Precompute the global-only entry and one entry per route spec (runs once
    /// per load/reload — the per-request policy cost becomes an `Arc` load).
    fn build(cfg: &PolicyConfig) -> Self {
        let global = Arc::new(MergedEntry::compile(merge_spec(&cfg.global, None)));
        let mut by_glob = HashMap::with_capacity(cfg.routes.len());
        for spec in &cfg.routes {
            by_glob.insert(
                spec.name_glob.clone(),
                Arc::new(MergedEntry::compile(merge_spec(&cfg.global, Some(spec)))),
            );
        }
        Self { global, by_glob }
    }

    /// The precomputed entry for `bare_ingress`.
    fn for_ingress(&self, cfg: &PolicyConfig, bare_ingress: &str) -> Arc<MergedEntry> {
        match cfg.for_ingress(bare_ingress) {
            Some(spec) => self
                .by_glob
                .get(&spec.name_glob)
                .cloned()
                .unwrap_or_else(|| self.global.clone()),
            None => self.global.clone(),
        }
    }
}

/// Per-dimension override: a route dimension wins over the global one; an
/// absent route dimension falls back to the global one.
fn merge_limits(global: Option<&LimitsSpec>, route: Option<&LimitsSpec>) -> Option<LimitsSpec> {
    match (global, route) {
        (None, None) => None,
        (Some(g), None) => Some(g.clone()),
        (None, Some(r)) => Some(r.clone()),
        (Some(g), Some(r)) => Some(LimitsSpec {
            ip: r.ip.clone().or_else(|| g.ip.clone()),
            consumer: r.consumer.clone().or_else(|| g.consumer.clone()),
        }),
    }
}

/// The route's `by_model_tokens` wins over the global's.
fn merge_quota(global: Option<&QuotaSpec>, route: Option<&QuotaSpec>) -> Option<QuotaSpec> {
    let by = route
        .and_then(|q| q.by_model_tokens.clone())
        .or_else(|| global.and_then(|q| q.by_model_tokens.clone()));
    by.map(|by_model_tokens| QuotaSpec {
        by_model_tokens: Some(by_model_tokens),
    })
}

/// **Inherit + append** the static rules (route rules after the global ones);
/// the route's `llm` and `fail_mode` win when the route section is present.
fn merge_guardrail(
    global: Option<&GuardrailSpec>,
    route: Option<&GuardrailSpec>,
) -> Option<GuardrailSpec> {
    match (global, route) {
        (None, None) => None,
        (Some(g), None) => Some(g.clone()),
        (None, Some(r)) => Some(r.clone()),
        (Some(g), Some(r)) => {
            let mut static_rules: Vec<StaticRuleSpec> = g.static_rules.clone();
            static_rules.extend(r.static_rules.iter().cloned());
            Some(GuardrailSpec {
                fail_mode: r.fail_mode,
                static_rules,
                llm: r.llm.clone().or_else(|| g.llm.clone()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::prelude::{
        GlobalPolicy, GuardAction, GuardrailFailMode, GuardrailSpec, LimitWindowSpec, LimitsSpec,
        LlmGuardMode, LlmGuardSpec, PolicyConfig, QuotaSpec, RoutePolicyActions, RoutePolicySpec,
        StaticRuleSpec, TokenBucketSpec,
    };

    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    fn tempdir() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("hygress-policy-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn policy_yaml() -> String {
        r#"
version: 1
global:
  limits:
    ip: { rps: 20, burst: 40 }
    consumer: { rps: 100, burst: 200 }
  quota:
    by_model_tokens: { window_secs: 86400, hard: 1000000 }
  guardrail:
    fail_mode: open
    static_rules:
      - { name: global-rule, regex: "global-bad", action: block }
routes:
  - name_glob: "ai-route-route-*"
    limits:
      consumer: { rps: 5, burst: 10 }
    quota:
      by_model_tokens: { window_secs: 86400, soft: 1000, hard: 50000 }
    policy:
      override_route: "model-8-6.static:80"
      pin_provider_svc_pattern: "provider-8.*"
      header_add:
        - [x-canary, "true"]
      header_del:
        - x-internal
      timeout_ms: 30000
      retries: 2
    guardrail:
      fail_mode: closed
      static_rules:
        - { name: route-rule, regex: "route-bad", action: block }
"#
        .to_string()
    }

    // ----- load_policy -----

    #[test]
    fn load_policy_parses_full_file() {
        let dir = tempdir();
        let path = dir.join("policy.yaml");
        std::fs::write(&path, policy_yaml()).unwrap();
        let c = load_policy(&path).unwrap();
        assert_eq!(c.version, 1);
        assert!(c.global.limits.is_some());
        assert_eq!(
            c.global.limits.as_ref().unwrap().ip.as_ref().unwrap().burst,
            40
        );
        assert_eq!(c.routes.len(), 1);
        assert_eq!(c.routes[0].name_glob, "ai-route-route-*");
        // `header_add` is a list of `[name, value]` pairs (serde (String, String)).
        let actions = c.routes[0].policy.as_ref().unwrap();
        assert_eq!(
            actions.header_add,
            vec![("x-canary".to_string(), "true".to_string())]
        );
        assert_eq!(actions.header_del, vec!["x-internal".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// BLOCK-3: a full policy.yaml written with the **§3 spec keys** (the
    /// real serde field names from `hygress-core::policy`) must parse
    /// successfully. This guards against a docs/impl drift where the design
    /// doc uses human-friendly keys (`window: "1d"`) that don't match the
    /// actual serde schema (`window_secs: 86400`).
    #[test]
    fn load_policy_parses_design_doc_section3_keys() {
        let yaml = r#"
version: 1
global:
  limits:
    ip: { rps: 20, burst: 40 }
    consumer: { rps: 100, burst: 200 }
  quota:
    by_model_tokens: { window_secs: 86400, hard: 1000000 }
  guardrail:
    fail_mode: closed
    static_rules:
      - { name: prompt-inject, regex: "(?i)ignore previous instruction", action: block }
    llm: { timeout_ms: 3000, max_rps: 5, cache_ttl_secs: 300, mode: sync, on_error: reject }
routes:
  - name_glob: ai-route-route-*
    limits: { consumer: { rps: 5, burst: 10 } }
    quota: { by_model_tokens: { window_secs: 86400, hard: 50000 } }
    policy:
      override_route: model-8-6.static:80
      pin_provider_svc_pattern: "provider-8.*"
      header_add:
        - [x-canary, "true"]
      timeout_ms: 30000
      retries: 2
    guardrail:
      fail_mode: closed
      static_rules:
        - { name: route-rule, regex: "route-bad", action: block }
"#;
        let dir = tempdir();
        let path = dir.join("policy.yaml");
        std::fs::write(&path, yaml).unwrap();
        let c = load_policy(&path).unwrap();
        assert_eq!(c.version, 1);
        // Global limits.
        let gl = c.global.limits.as_ref().unwrap();
        assert_eq!(gl.ip.as_ref().unwrap().rps, 20.0);
        assert_eq!(gl.consumer.as_ref().unwrap().burst, 200);
        // Global quota.
        let gq = c.global.quota.as_ref().unwrap();
        assert_eq!(gq.by_model_tokens.as_ref().unwrap().window_secs, 86400);
        // Global guardrail LLM.
        let gg = c.global.guardrail.as_ref().unwrap();
        let llm = gg.llm.as_ref().unwrap();
        assert_eq!(llm.timeout_ms, 3000);
        assert_eq!(llm.cache_ttl_secs, 300);
        // Route.
        assert_eq!(c.routes.len(), 1);
        assert_eq!(c.routes[0].name_glob, "ai-route-route-*");
        let actions = c.routes[0].policy.as_ref().unwrap();
        assert_eq!(
            actions.override_route.as_deref(),
            Some("model-8-6.static:80")
        );
        assert_eq!(
            actions.pin_provider_svc_pattern.as_deref(),
            Some("provider-8.*")
        );
        assert_eq!(actions.timeout_ms, Some(30000));
        assert_eq!(actions.retries, Some(2));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_policy_missing_file_is_default_all_pass() {
        let dir = tempdir();
        let path = dir.join("absent.yaml");
        let c = load_policy(&path).unwrap();
        assert_eq!(c, PolicyConfig::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_policy_empty_file_is_default_all_pass() {
        let dir = tempdir();
        let path = dir.join("empty.yaml");
        std::fs::write(&path, "   \n\t\n").unwrap();
        let c = load_policy(&path).unwrap();
        assert_eq!(c, PolicyConfig::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_policy_malformed_file_is_err() {
        let dir = tempdir();
        let path = dir.join("bad.yaml");
        std::fs::write(&path, "global: [not a mapping").unwrap();
        assert!(load_policy(&path).is_err());
        // A schema-level error (wrong type) is also `Err`.
        std::fs::write(&path, "version: not-a-number").unwrap();
        assert!(load_policy(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ----- PolicyHandle -----

    #[test]
    fn handle_missing_file_starts_all_pass() {
        let dir = tempdir();
        let h = PolicyHandle::new(dir.join("absent.yaml").to_string_lossy().into_owned());
        assert_eq!(*h.shared(), PolicyConfig::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn handle_malformed_file_starts_all_pass() {
        let dir = tempdir();
        let path = dir.join("bad.yaml");
        std::fs::write(&path, "version: [broken").unwrap();
        let h = PolicyHandle::new(path.to_string_lossy().into_owned());
        assert_eq!(*h.shared(), PolicyConfig::default());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn handle_reload_hot_updates_and_keeps_on_failure() {
        let dir = tempdir();
        let path = dir.join("policy.yaml");
        std::fs::write(&path, "version: 1\n").unwrap();
        let h = PolicyHandle::new(path.to_string_lossy().into_owned());
        assert_eq!(*h.shared(), PolicyConfig::default());

        // A good reload swaps in the new value.
        std::fs::write(&path, policy_yaml()).unwrap();
        assert!(h.reload());
        assert_eq!(h.shared().routes.len(), 1);

        // A bad reload keeps the last-known-good (and reports false).
        std::fs::write(&path, "global: [broken").unwrap();
        assert!(!h.reload());
        assert_eq!(h.shared().routes.len(), 1, "last-known-good must be kept");

        // ORA3-M2: a reload whose file has *disappeared* is a failure — the
        // last-known-good is kept (never a silent downgrade to all-pass) and
        // the response is false (admin surfaces a non-200).
        std::fs::remove_file(&path).unwrap();
        assert!(!h.reload_from(&path.to_string_lossy()));
        assert_eq!(
            h.shared().routes.len(),
            1,
            "last-known-good must be kept when the file disappears"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn handle_reload_missing_file_without_prior_load_is_an_honest_failure() {
        // ORA3-M2: boot with no policy file (image default) → all-pass + warn;
        // a later /reload while the file is still missing must NOT report a
        // successful reload (the old behaviour returned true + 200 "reloaded").
        let dir = tempdir();
        let absent = dir.join("absent.yaml");
        let h = PolicyHandle::new(absent.to_string_lossy().into_owned());
        assert_eq!(*h.shared(), PolicyConfig::default());
        assert!(
            !h.reload(),
            "missing-file reload must report failure (honest)"
        );
        assert_eq!(
            *h.shared(),
            PolicyConfig::default(),
            "state stays the built-in all-pass"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn handle_reload_after_malformed_boot_then_fixed_then_missing() {
        // Full ORA3-M2 cycle: malformed at boot (all-pass, no LKG) → fixed via
        // /reload (a real policy loads) → the file disappears → /reload keeps
        // the real policy and fails loudly instead of downgrading to all-pass.
        let dir = tempdir();
        let path = dir.join("policy.yaml");
        std::fs::write(&path, "version: [broken").unwrap();
        let h = PolicyHandle::new(path.to_string_lossy().into_owned());
        assert_eq!(*h.shared(), PolicyConfig::default());
        // Still malformed → false (LKG = all-pass, kept).
        assert!(!h.reload());
        // Fixed → a real policy loads.
        std::fs::write(&path, policy_yaml()).unwrap();
        assert!(h.reload());
        assert_eq!(h.shared().routes.len(), 1);
        // Gone → the real policy is kept and reload reports false.
        std::fs::remove_file(&path).unwrap();
        assert!(!h.reload());
        assert_eq!(
            h.shared().routes.len(),
            1,
            "real policy must survive the file disappearing"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn handle_poll_reloads_on_mtime_change_only() {
        let dir = tempdir();
        let path = dir.join("policy.yaml");
        std::fs::write(&path, "version: 1\n").unwrap();
        let h = PolicyHandle::new(path.to_string_lossy().into_owned());

        // No change yet → no reload.
        assert!(!h.poll());

        // A rewrite (new mtime) → reload picks up the new value.
        std::thread::sleep(std::time::Duration::from_millis(15));
        std::fs::write(&path, policy_yaml()).unwrap();
        assert!(h.poll());
        assert_eq!(h.shared().routes.len(), 1);

        // Same file again → no reload.
        assert!(!h.poll());
        std::fs::remove_dir_all(&dir).ok();
    }

    // ----- bare_ingress_name (D-12) -----

    #[test]
    fn bare_name_strips_ns_prefix() {
        assert_eq!(
            bare_ingress_name("higress-system/ai-route-route-1.internal"),
            "ai-route-route-1.internal"
        );
        assert_eq!(
            bare_ingress_name("ai-route-route-1.internal"),
            "ai-route-route-1.internal"
        );
        assert_eq!(bare_ingress_name("gpustack"), "gpustack");
    }

    // ----- merge_policy -----

    fn cfg_with(global: GlobalPolicy, routes: Vec<RoutePolicySpec>) -> PolicyConfig {
        PolicyConfig {
            version: 1,
            global,
            routes,
        }
    }

    fn rule(name: &str, regex: &str) -> StaticRuleSpec {
        StaticRuleSpec {
            name: name.into(),
            regex: regex.into(),
            action: GuardAction::Block,
        }
    }

    #[test]
    fn merge_no_route_matches_uses_global() {
        let c = cfg_with(
            GlobalPolicy {
                limits: Some(LimitsSpec {
                    ip: Some(TokenBucketSpec {
                        rps: 20.0,
                        burst: 40,
                    }),
                    consumer: None,
                }),
                quota: None,
                guardrail: Some(GuardrailSpec {
                    fail_mode: GuardrailFailMode::Open,
                    static_rules: vec![rule("g", "g-bad")],
                    llm: None,
                }),
            },
            vec![RoutePolicySpec {
                name_glob: "only-this".into(),
                ..Default::default()
            }],
        );
        let m = merge_policy(&c, "ai-route-route-9.internal");
        assert_eq!(m.limits.as_ref().unwrap().ip.as_ref().unwrap().burst, 40);
        assert!(m.limits.as_ref().unwrap().consumer.is_none());
        assert!(m.quota.is_none());
        assert_eq!(m.guardrail.as_ref().unwrap().static_rules.len(), 1);
        assert!(m.actions.is_none());
    }

    #[test]
    fn merge_route_overrides_per_dimension() {
        // Global: ip + consumer. Route: consumer only → the merged keeps the
        // global ip and the route consumer (per-dimension override).
        let c = cfg_with(
            GlobalPolicy {
                limits: Some(LimitsSpec {
                    ip: Some(TokenBucketSpec {
                        rps: 20.0,
                        burst: 40,
                    }),
                    consumer: Some(TokenBucketSpec {
                        rps: 100.0,
                        burst: 200,
                    }),
                }),
                quota: Some(QuotaSpec {
                    by_model_tokens: Some(LimitWindowSpec {
                        window_secs: 86400,
                        soft: None,
                        hard: Some(1_000_000),
                    }),
                }),
                guardrail: None,
            },
            vec![RoutePolicySpec {
                name_glob: "ai-route-route-*".into(),
                limits: Some(LimitsSpec {
                    ip: None,
                    consumer: Some(TokenBucketSpec {
                        rps: 5.0,
                        burst: 10,
                    }),
                }),
                quota: Some(QuotaSpec {
                    by_model_tokens: Some(LimitWindowSpec {
                        window_secs: 86400,
                        soft: Some(1000),
                        hard: Some(50_000),
                    }),
                }),
                policy: None,
                guardrail: None,
            }],
        );
        let m = merge_policy(&c, "ai-route-route-1.internal");
        let limits = m.limits.as_ref().unwrap();
        // The global ip survives (the route did not set it) ...
        assert_eq!(limits.ip.as_ref().unwrap().burst, 40);
        // ... but the route consumer overrides the global consumer.
        assert_eq!(limits.consumer.as_ref().unwrap().burst, 10);
        // The route quota wins over the global quota.
        assert_eq!(
            m.quota
                .as_ref()
                .unwrap()
                .by_model_tokens
                .as_ref()
                .unwrap()
                .hard,
            Some(50_000)
        );
        assert_eq!(
            m.quota
                .as_ref()
                .unwrap()
                .by_model_tokens
                .as_ref()
                .unwrap()
                .soft,
            Some(1000)
        );
        // No guardrail anywhere.
        assert!(m.guardrail.is_none());
    }

    #[test]
    fn merge_guardrail_inherits_and_appends_rules() {
        let c = cfg_with(
            GlobalPolicy {
                limits: None,
                quota: None,
                guardrail: Some(GuardrailSpec {
                    fail_mode: GuardrailFailMode::Open,
                    static_rules: vec![rule("g1", "g-bad"), rule("g2", "g-bad-2")],
                    llm: Some(LlmGuardSpec {
                        mode: LlmGuardMode::Async,
                        ..Default::default()
                    }),
                }),
            },
            vec![RoutePolicySpec {
                name_glob: "ai-route-route-*".into(),
                limits: None,
                quota: None,
                guardrail: Some(GuardrailSpec {
                    fail_mode: GuardrailFailMode::Closed,
                    static_rules: vec![rule("r1", "r-bad")],
                    llm: None,
                }),
                policy: Some(RoutePolicyActions {
                    header_add: vec![("x-canary".into(), "true".into())],
                    ..Default::default()
                }),
            }],
        );
        let m = merge_policy(&c, "ai-route-route-7.internal");
        let g = m.guardrail.as_ref().unwrap();
        // Inherit + append: the two global rules first, then the route rule.
        assert_eq!(
            g.static_rules
                .iter()
                .map(|r| r.name.as_str())
                .collect::<Vec<_>>(),
            vec!["g1", "g2", "r1"]
        );
        // The route's fail_mode wins ...
        assert_eq!(g.fail_mode, GuardrailFailMode::Closed);
        // ... but the global llm is inherited (the route set none).
        assert!(g.llm.is_some());
        assert_eq!(g.llm.as_ref().unwrap().mode, LlmGuardMode::Async);
        // The route's actions surface.
        assert!(m.actions.is_some());
    }

    #[test]
    fn merge_route_guardrail_without_global_is_route_only() {
        let c = cfg_with(
            GlobalPolicy::default(),
            vec![RoutePolicySpec {
                name_glob: "*".into(),
                guardrail: Some(GuardrailSpec {
                    static_rules: vec![rule("r1", "r-bad")],
                    ..Default::default()
                }),
                ..Default::default()
            }],
        );
        let m = merge_policy(&c, "anything");
        assert_eq!(m.guardrail.as_ref().unwrap().static_rules.len(), 1);
        // The route section's default fail_mode (Closed) applies.
        assert_eq!(
            m.guardrail.as_ref().unwrap().fail_mode,
            GuardrailFailMode::Closed
        );
    }

    #[test]
    fn merge_empty_policy_is_all_pass() {
        let m = merge_policy(&PolicyConfig::default(), "anything");
        assert_eq!(m, MergedPolicy::default());
    }
}
