//! Ordered header transformation rule engine (native equivalent of the
//! `gpustack-header-transformer` plugin, design §6.1 ①③ / §7).
//!
//! The inbound set strips untrusted headers **before** any auth, renames the
//! legacy `x-gpustack-model` header, restores the original path from the
//! fallback marker, strips any client-forged `x-gpustack-original-path`, and
//! backstops `:path` into `x-gpustack-original-path` for the fallback flow —
//! so the only value of that header the fallback hop can read is the
//! gateway's own (ORA3-M8). The outbound set guarantees the instance /
//! route-name headers survive (deduped, never stripped).
//!
//! [`HeaderMap`] is the header abstraction: keys are stored
//! lowercased (case-insensitive lookups), values are multi-valued.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Case-insensitive multi-value header map (keys lowercased on write).
///
/// Pseudo-headers (e.g. `:path`) are stored under the same map, keyed by
/// their lowercase form (`":path"`).
///
/// The map is **copy-on-write** ([`Arc`]): [`HeaderMap::clone`] is O(1), so
/// the pipeline's clone-then-mutate (inbound strip, per-candidate outbound
/// build, provider forward) never copies header payload bytes until an actual
/// mutation. Lookups use an allocation-free fast path when the name is already
/// ASCII-lowercase (the common case for HTTP header names).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeaderMap {
    map: Arc<HashMap<String, Vec<String>>>,
}

impl HeaderMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// A mutable, uniquely-owned view of the inner map (deep-clones the map
    /// only when other clones still share it).
    fn make_mut(&mut self) -> &mut HashMap<String, Vec<String>> {
        Arc::make_mut(&mut self.map)
    }

    /// The owned lowercased key for `name` (allocates only when `name` is not
    /// already ASCII-lowercase).
    fn key(name: &str) -> String {
        if is_ascii_lowercase(name) {
            name.to_string()
        } else {
            name.to_ascii_lowercase()
        }
    }

    /// The borrowable lookup form for `name`: `&str` when already lowercase
    /// (allocation-free read), an owned lowered `String` otherwise.
    fn lookup<'a>(name: &'a str) -> Cow<'a, str> {
        if is_ascii_lowercase(name) {
            Cow::Borrowed(name)
        } else {
            Cow::Owned(name.to_ascii_lowercase())
        }
    }

    /// Replace all values of `name` with a single `value`.
    pub fn insert(&mut self, name: &str, value: impl Into<String>) {
        self.make_mut().insert(Self::key(name), vec![value.into()]);
    }

    /// Append a value (multi-value semantics).
    pub fn append(&mut self, name: &str, value: impl Into<String>) {
        self.make_mut()
            .entry(Self::key(name))
            .or_default()
            .push(value.into());
    }

    /// First value of `name`, if any.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.map
            .get(Self::lookup(name).as_ref())
            .and_then(|v| v.first())
            .map(String::as_str)
    }

    /// All values of `name` (empty slice when absent).
    pub fn get_all(&self, name: &str) -> &[String] {
        self.map
            .get(Self::lookup(name).as_ref())
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Number of values of `name`.
    pub fn count(&self, name: &str) -> usize {
        self.map
            .get(Self::lookup(name).as_ref())
            .map(|v| v.len())
            .unwrap_or(0)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.map.contains_key(Self::lookup(name).as_ref())
    }

    /// Remove all values of `name`.
    pub fn remove(&mut self, name: &str) {
        self.make_mut().remove(Self::lookup(name).as_ref());
    }

    /// All header names present (lowercase form).
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.map.keys().map(|k| k.as_str())
    }
}

/// `true` when `name` contains no ASCII uppercase byte (the allocation-free
/// guard for the lookup fast path).
fn is_ascii_lowercase(name: &str) -> bool {
    !name.bytes().any(|b| b.is_ascii_uppercase())
}

impl<'a> FromIterator<(&'a str, String)> for HeaderMap {
    fn from_iter<I: IntoIterator<Item = (&'a str, String)>>(iter: I) -> Self {
        let mut m = Self::default();
        for (k, v) in iter {
            m.append(k, v);
        }
        m
    }
}

impl<'a> FromIterator<(&'a str, &'a str)> for HeaderMap {
    fn from_iter<I: IntoIterator<Item = (&'a str, &'a str)>>(iter: I) -> Self {
        let mut m = Self::default();
        for (k, v) in iter {
            m.append(k, v);
        }
        m
    }
}

/// Transformation operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransformOp {
    /// Remove `source`.
    Remove,
    /// Move all `source` values to `dest` (in order), delete `source`.
    Rename,
    /// Collapse `source` to one value per [`RetainMode`].
    Dedupe,
    /// Copy `source` values to `dest` (source kept).
    Backup,
    /// Explicit pass-through: the header is never modified by this rule
    /// (documents "keep" intent for egress).
    Skip,
}

/// Which duplicate to keep.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetainMode {
    RetainFirst,
    RetainLast,
}

/// One ordered transformation rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransformRule {
    pub op: TransformOp,
    pub source: String,
    /// Target for [`TransformOp::Rename`] / [`TransformOp::Backup`].
    pub dest: Option<String>,
    /// Retention mode for [`TransformOp::Dedupe`].
    pub mode: Option<RetainMode>,
}

impl TransformRule {
    fn remove(source: &str) -> Self {
        Self {
            op: TransformOp::Remove,
            source: source.to_string(),
            dest: None,
            mode: None,
        }
    }

    fn rename(source: &str, dest: &str) -> Self {
        Self {
            op: TransformOp::Rename,
            source: source.to_string(),
            dest: Some(dest.to_string()),
            mode: None,
        }
    }

    fn dedupe(source: &str, mode: RetainMode) -> Self {
        Self {
            op: TransformOp::Dedupe,
            source: source.to_string(),
            dest: None,
            mode: Some(mode),
        }
    }

    fn backup(source: &str, dest: &str) -> Self {
        Self {
            op: TransformOp::Backup,
            source: source.to_string(),
            dest: Some(dest.to_string()),
            mode: None,
        }
    }

    fn skip(source: &str) -> Self {
        Self {
            op: TransformOp::Skip,
            source: source.to_string(),
            dest: None,
            mode: None,
        }
    }
}

/// An ordered transformer. Rules execute strictly in list order (the
/// GPUStack `reqRules` order is load-bearing: rename before dedupe before
/// backstop).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transformer {
    rules: Vec<TransformRule>,
}

impl Transformer {
    pub fn new(rules: Vec<TransformRule>) -> Self {
        Self { rules }
    }

    /// The GPUStack **inbound** set (design §6.1 ①③, plugin
    /// `gpustack-header-transformer` `reqRules`):
    ///
    /// 1. remove `x-gpustack-auth-token` (untrusted, stripped before auth)
    /// 2. remove `x-gpustack-model-instance` (untrusted)
    /// 3. remove `x-gpustack-original-path` (untrusted — client-forgeable
    ///    fallback-restore value, ORA3-M8; see the backstop rule below)
    /// 4. rename `x-gpustack-model` → `x-higress-llm-model`
    /// 5. rename `x-gpustack-fallback-path` → `:path`
    /// 6. dedupe `x-gpustack-model` (RETAIN_FIRST) — legacy no-op after rename
    /// 7. dedupe `x-higress-llm-model` (RETAIN_FIRST — existing wins)
    /// 8. dedupe `:path` (RETAIN_LAST — fallback-restored path wins)
    /// 9. backstop `:path` → `x-gpustack-original-path`
    /// 10. remove `x-gpustack-fallback-path`
    pub fn inbound() -> Self {
        Self::new(vec![
            TransformRule::remove("x-gpustack-auth-token"),
            TransformRule::remove("x-gpustack-model-instance"),
            // ORA3-M8: `x-gpustack-original-path` is a GATEWAY-WRITTEN internal
            // header (the rule-9 backstop feeds the fallback restore), so any
            // client-supplied occurrence must be stripped BEFORE the backstop
            // runs — the fallback hop reads the FIRST value (HeaderMap::get),
            // and an inbound value that survived would win over the backstop
            // and steer the restored `:path` of the fallback re-dispatch.
            TransformRule::remove("x-gpustack-original-path"),
            TransformRule::rename("x-gpustack-model", "x-higress-llm-model"),
            TransformRule::rename("x-gpustack-fallback-path", ":path"),
            TransformRule::dedupe("x-gpustack-model", RetainMode::RetainFirst),
            TransformRule::dedupe("x-higress-llm-model", RetainMode::RetainFirst),
            TransformRule::dedupe(":path", RetainMode::RetainLast),
            TransformRule::backup(":path", "x-gpustack-original-path"),
            TransformRule::remove("x-gpustack-fallback-path"),
        ])
    }

    /// The GPUStack **outbound** set: the instance / route-name headers must
    /// survive egress — dedupe to a single value, explicitly kept (never
    /// stripped, design §6.1 ⑩: "transformer 出向不得剥离实例头").
    pub fn outbound() -> Self {
        Self::new(vec![
            TransformRule::dedupe("x-gpustack-model-instance", RetainMode::RetainFirst),
            TransformRule::dedupe("x-gpustack-route-name", RetainMode::RetainFirst),
            TransformRule::skip("x-gpustack-model-instance"),
            TransformRule::skip("x-gpustack-route-name"),
        ])
    }

    pub fn rules(&self) -> &[TransformRule] {
        &self.rules
    }

    /// Apply all rules in order to `headers` (in place).
    pub fn apply(&self, headers: &mut HeaderMap) {
        for rule in &self.rules {
            match rule.op {
                TransformOp::Remove => {
                    headers.remove(&rule.source);
                }
                TransformOp::Rename => {
                    if let Some(dest) = &rule.dest {
                        let values: Vec<String> = headers.get_all(&rule.source).to_vec();
                        for v in values {
                            headers.append(dest, v);
                        }
                        headers.remove(&rule.source);
                    }
                }
                TransformOp::Dedupe => {
                    let values = headers.get_all(&rule.source);
                    if values.len() > 1 {
                        let kept = match rule.mode.unwrap_or(RetainMode::RetainFirst) {
                            RetainMode::RetainFirst => values.first().cloned(),
                            RetainMode::RetainLast => values.last().cloned(),
                        };
                        if let Some(v) = kept {
                            headers.insert(&rule.source, v);
                        } else {
                            headers.remove(&rule.source);
                        }
                    }
                }
                TransformOp::Backup => {
                    if let Some(dest) = &rule.dest {
                        let values: Vec<String> = headers.get_all(&rule.source).to_vec();
                        for v in values {
                            headers.append(dest, v);
                        }
                    }
                }
                TransformOp::Skip => {
                    // Explicit pass-through: no mutation (documents keep
                    // intent for the header).
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- HeaderMap -----

    #[test]
    fn header_map_case_insensitive() {
        let mut h = HeaderMap::new();
        h.insert("X-Foo", "a");
        assert_eq!(h.get("x-foo"), Some("a"));
        assert_eq!(h.get("X-FOO"), Some("a"));
        assert!(h.contains("x-foo"));
        h.remove("X-foo");
        assert!(!h.contains("x-foo"));
    }

    #[test]
    fn header_map_multi_value_and_first_wins() {
        let mut h = HeaderMap::new();
        h.append("set-cookie", "a");
        h.append("SET-COOKIE", "b");
        assert_eq!(h.count("set-cookie"), 2);
        assert_eq!(h.get("set-cookie"), Some("a"));
        assert_eq!(h.get_all("set-cookie"), &["a".to_string(), "b".to_string()]);
        h.insert("set-cookie", "c");
        assert_eq!(h.count("set-cookie"), 1);
        assert_eq!(h.get("set-cookie"), Some("c"));
    }

    // ----- inbound -----

    #[test]
    fn inbound_strips_untrusted_headers() {
        let mut h = HeaderMap::from_iter([
            ("X-GPUStack-Auth-Token", "forged"),
            ("X-GPUStack-Model-Instance", "forged"),
            ("x-higress-llm-model", "my-model"),
            (":path", "/v1/chat/completions"),
        ]);
        Transformer::inbound().apply(&mut h);
        assert!(!h.contains("x-gpustack-auth-token"));
        assert!(!h.contains("x-gpustack-model-instance"));
        // Legit route header survives.
        assert_eq!(h.get("x-higress-llm-model"), Some("my-model"));
        // :path backstopped.
        assert_eq!(
            h.get("x-gpustack-original-path"),
            Some("/v1/chat/completions")
        );
    }

    #[test]
    fn inbound_rename_legacy_model_header() {
        let mut h = HeaderMap::from_iter([("x-gpustack-model", "legacy-model")]);
        Transformer::inbound().apply(&mut h);
        assert!(!h.contains("x-gpustack-model"));
        assert_eq!(h.get("x-higress-llm-model"), Some("legacy-model"));
    }

    #[test]
    fn inbound_rename_retain_first_when_llm_model_prefilled() {
        // Existing x-higress-llm-model wins over the renamed legacy value
        // (design §6.1 ③: RETAIN_FIRST).
        let mut h = HeaderMap::from_iter([
            ("x-higress-llm-model", "prefilled"),
            ("x-gpustack-model", "legacy"),
        ]);
        Transformer::inbound().apply(&mut h);
        assert_eq!(h.count("x-higress-llm-model"), 1);
        assert_eq!(h.get("x-higress-llm-model"), Some("prefilled"));
    }

    #[test]
    fn inbound_dedupe_retains_first_of_renamed_duplicates() {
        // Two legacy values both rename into x-higress-llm-model; the first
        // is retained.
        let mut h = HeaderMap::new();
        h.append("x-gpustack-model", "first");
        h.append("x-gpustack-model", "second");
        Transformer::inbound().apply(&mut h);
        assert_eq!(h.count("x-higress-llm-model"), 1);
        assert_eq!(h.get("x-higress-llm-model"), Some("first"));
    }

    #[test]
    fn inbound_restores_path_from_fallback_marker() {
        // Fallback flow: x-gpustack-fallback-path restores :path and wins the
        // RETAIN_LAST dedupe; the marker is then removed; original-path
        // backstop carries the restored value.
        let mut h = HeaderMap::from_iter([
            (":path", "/v1/chat/completions"),
            ("x-gpustack-fallback-path", "/original/path"),
        ]);
        Transformer::inbound().apply(&mut h);
        assert_eq!(h.get(":path"), Some("/original/path"));
        assert!(!h.contains("x-gpustack-fallback-path"));
        assert_eq!(h.get("x-gpustack-original-path"), Some("/original/path"));
    }

    #[test]
    fn inbound_single_value_path_backstopped() {
        // A single-occurrence :path is untouched by dedupe but the backup
        // still copies it to x-gpustack-original-path.
        let mut h = HeaderMap::from_iter([(":path", "/single".to_string())]);
        Transformer::inbound().apply(&mut h);
        assert_eq!(h.get(":path"), Some("/single"));
        assert_eq!(h.get("x-gpustack-original-path"), Some("/single"));
        assert_eq!(h.count(":path"), 1);
        // No :path at all -> no backstop, nothing added.
        let mut h2 = HeaderMap::new();
        Transformer::inbound().apply(&mut h2);
        assert!(!h2.contains("x-gpustack-original-path"));
        assert!(!h2.contains(":path"));
    }

    #[test]
    fn inbound_strips_client_forged_original_path_before_backstop() {
        // ORA3-M8: `x-gpustack-original-path` is a GATEWAY-WRITTEN internal
        // header (the fallback hop restores `:path` from its FIRST value via
        // HeaderMap::get). A client-supplied value must be stripped by the
        // inbound set BEFORE the `:path` backstop appends, so the only value
        // present at the fallback hop is the gateway's own — otherwise a
        // client could choose the restored path of the fallback re-dispatch
        // (falling to the mirror catch-all when the Fallback predicate cannot
        // match) and have the forged value echoed upstream.
        let mut h = HeaderMap::from_iter([
            (":path", "/v1/chat/completions"),
            ("x-gpustack-original-path", "/attacker/chosen"),
        ]);
        Transformer::inbound().apply(&mut h);
        // The forged value is gone; the backstop appended exactly one value —
        // the gateway's own, so `get()` (first value) reads it.
        assert_eq!(h.count("x-gpustack-original-path"), 1);
        assert_eq!(
            h.get("x-gpustack-original-path"),
            Some("/v1/chat/completions"),
            "the client value must not win over the gateway backstop"
        );

        // Header names are case-insensitive: any casing of the forged header
        // is stripped (keys are stored lowercased).
        let mut h2 = HeaderMap::from_iter([
            (":path", "/v1/embeddings"),
            ("X-GPUStack-Original-Path", "/attacker/chosen"),
        ]);
        Transformer::inbound().apply(&mut h2);
        assert_eq!(h2.count("x-gpustack-original-path"), 1);
        assert_eq!(h2.get("x-gpustack-original-path"), Some("/v1/embeddings"));

        // No `:path` to backstop: a forged value is stripped outright and no
        // header is created (nothing to restore).
        let mut h3 = HeaderMap::from_iter([("x-gpustack-original-path", "/attacker/chosen")]);
        Transformer::inbound().apply(&mut h3);
        assert!(!h3.contains("x-gpustack-original-path"));
    }

    // ----- outbound -----

    #[test]
    fn outbound_keeps_instance_and_route_name() {
        let mut h = HeaderMap::new();
        h.append("X-GPUStack-Model-Instance", "model-1-2.static");
        h.append("x-gpustack-model-instance", "model-9-9.static"); // duplicate
        h.append("X-GPUStack-Route-Name", "org1/llama-route");
        Transformer::outbound().apply(&mut h);
        // Deduped to the retained-first value, never removed.
        assert_eq!(h.get("x-gpustack-model-instance"), Some("model-1-2.static"));
        assert_eq!(h.count("x-gpustack-model-instance"), 1);
        assert_eq!(h.get("x-gpustack-route-name"), Some("org1/llama-route"));
    }

    #[test]
    fn outbound_does_not_strip_llm_model() {
        let mut h = HeaderMap::from_iter([("x-higress-llm-model", "m")]);
        Transformer::outbound().apply(&mut h);
        assert_eq!(h.get("x-higress-llm-model"), Some("m"));
    }

    // ----- individual ops -----

    #[test]
    fn skip_rule_is_noop() {
        let mut h = HeaderMap::from_iter([("x-gpustack-model-instance", "keep")]);
        Transformer::new(vec![TransformRule::skip("x-gpustack-model-instance")]).apply(&mut h);
        assert_eq!(h.get("x-gpustack-model-instance"), Some("keep"));
    }

    #[test]
    fn rename_without_dest_is_noop() {
        let mut h = HeaderMap::from_iter([("a", "v")]);
        let rule = TransformRule {
            op: TransformOp::Rename,
            source: "a".into(),
            dest: None,
            mode: None,
        };
        Transformer::new(vec![rule]).apply(&mut h);
        assert_eq!(h.get("a"), Some("v"));
    }

    #[test]
    fn dedupe_retain_last() {
        let mut h = HeaderMap::new();
        h.append("x", "1");
        h.append("x", "2");
        h.append("x", "3");
        Transformer::new(vec![TransformRule::dedupe("x", RetainMode::RetainLast)]).apply(&mut h);
        assert_eq!(h.get("x"), Some("3"));
        assert_eq!(h.count("x"), 1);
    }

    #[test]
    fn rules_execute_in_order() {
        // remove before rename would change the outcome: with remove first,
        // the rename has nothing to move.
        let mut h = HeaderMap::from_iter([("a", "v")]);
        Transformer::new(vec![
            TransformRule::remove("a"),
            TransformRule::rename("a", "b"),
        ])
        .apply(&mut h);
        assert!(!h.contains("a"));
        assert!(!h.contains("b"));

        let mut h2 = HeaderMap::from_iter([("a", "v")]);
        Transformer::new(vec![
            TransformRule::rename("a", "b"),
            TransformRule::remove("b"),
        ])
        .apply(&mut h2);
        assert!(!h2.contains("a"));
        assert!(!h2.contains("b"));
    }
}
