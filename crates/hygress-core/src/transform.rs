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
//! [`OutboundHeaders`] (AM-6b) is the lazy per-candidate outbound variant: a
//! shared base [`HeaderMap`] plus an in-order mutation delta, so candidates
//! that only add/override/remove a few headers never deep-copy the base.

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
    /// An empty header map.
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

    /// Whether `name` has at least one value.
    pub fn contains(&self, name: &str) -> bool {
        self.map.contains_key(Self::lookup(name).as_ref())
    }

    /// Remove all values of `name`.
    ///
    /// P4: an ABSENT name is a no-op and skips the `make_mut` deep copy — the
    /// inbound ① strip removes client-unforgeable headers that are absent on
    /// virtually every request, so a miss must keep the map shared (only a
    /// real mutation pays the one COW copy).
    pub fn remove(&mut self, name: &str) {
        let key = Self::lookup(name);
        if !self.map.contains_key(key.as_ref()) {
            return;
        }
        self.make_mut().remove(key.as_ref());
    }

    /// All header names present (lowercase form).
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.map.keys().map(|k| k.as_str())
    }

    /// AM-6: consume the map into owned `(name, value)` pairs — one pair per
    /// value, in map iteration order.
    ///
    /// AM-6b: the per-candidate outbound dial no longer drains a per-candidate
    /// `HeaderMap` (it drains the [`OutboundHeaders`] overlay via
    /// [`OutboundHeaders::into_pairs`]); this method serves the maps a full
    /// materialization produced — e.g. [`OutboundHeaders::materialize`] in the
    /// provider branch — plus standalone `HeaderMap`s. When this map is the
    /// **only** reference (the `materialize` norm: an O(1) `base` clone plus
    /// one `make_mut` leave the [`Arc`] exclusive), every `String` is
    /// **moved** out — no per-header clone, no per-name re-allocation. When
    /// clones still share the map, the pairs are deep-cloned and the shared map
    /// is left untouched — byte-identical to the historical clone-then-dial
    /// path. Callers must not use the map after this call (it is consumed).
    pub fn into_pairs(self) -> Vec<(String, String)> {
        // Total value count (the fold emits one pair per value) so the result
        // Vec never re-grows.
        let pair_count: usize = self.map.values().map(|values| values.len()).sum();
        let mut pairs = Vec::with_capacity(pair_count);
        match Arc::try_unwrap(self.map) {
            // Exclusively owned: move the keys + values out.
            Ok(map) => {
                for (name, values) in map {
                    let total = values.len();
                    let mut iter = values.into_iter();
                    // Every value except the LAST gets a cloned name (a
                    // multi-value key needs one name per pair); the last value
                    // moves the name out. The move sits OUTSIDE the prefix loop
                    // so `name` is only ever moved once per map entry.
                    for _ in 0..total.saturating_sub(1) {
                        let value = iter.next().expect("prefix count == values.len() - 1");
                        pairs.push((name.clone(), value));
                    }
                    if let Some(last_value) = iter.next() {
                        pairs.push((name, last_value));
                    }
                }
            }
            // Still shared: deep-clone every pair (the other owners keep the
            // original untouched).
            Err(shared) => {
                for (name, values) in shared.iter() {
                    for value in values {
                        pairs.push((name.clone(), value.clone()));
                    }
                }
            }
        }
        pairs
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

/// The mutation surface the ordered rule engine ([`Transformer::apply`]) needs
/// — implemented by [`HeaderMap`] (a materialized map) and [`OutboundHeaders`]
/// (the lazy overlay) so the engine has exactly ONE implementation and the two
/// can never drift. Hidden: not part of the supported public surface.
#[doc(hidden)]
pub trait HeaderOps {
    /// All current values of `name` (empty when absent).
    fn header_get_all(&self, name: &str) -> &[String];
    /// Replace all values of `name` with `value`.
    fn header_insert(&mut self, name: &str, value: String);
    /// Append a value (multi-value semantics).
    fn header_append(&mut self, name: &str, value: String);
    /// Remove all values of `name`.
    fn header_remove(&mut self, name: &str);
}

impl HeaderOps for HeaderMap {
    fn header_get_all(&self, name: &str) -> &[String] {
        self.get_all(name)
    }
    fn header_insert(&mut self, name: &str, value: String) {
        self.insert(name, value);
    }
    fn header_append(&mut self, name: &str, value: String) {
        self.append(name, value);
    }
    fn header_remove(&mut self, name: &str) {
        self.remove(name);
    }
}

/// AM-6b: a **lazy overlay** over a shared base [`HeaderMap`] (typically
/// `PreparedRequest.base_headers`), replacing the AM-6 clone-then-mutate
/// per-candidate copy with an in-order delta: a candidate that only
/// adds / overrides / removes a handful of headers (the norm: auth write-back,
/// pre-route instance/route-name headers, hop-by-hop strip, provider key swap)
/// **never** deep-copies the base entries — the base stays shared and the
/// deltas are materialized exactly once, at the dial
/// ([`OutboundHeaders::into_pairs`]) or at the egress-contract boundary
/// ([`OutboundHeaders::materialize`]).
///
/// Semantics are content-identical to running the same operation sequence on a
/// cloned [`HeaderMap`]: keys are lowercased on write, `insert` replaces,
/// `append` extends (a first `append` over a live base name inherits the base
/// values; after a `remove` it starts fresh), `remove` deletes, and every read
/// (`get` / `get_all` / `count` / `contains` / `names`) sees the same effective
/// set the materialized map would. The materialized result of an overlay
/// therefore equals exactly what today's clone-then-mutate `HeaderMap` yields
/// for the same op sequence.
///
/// Layout:
/// * `overrides` — a name whose final value list the overlay fully owns (first
///   touched by an `insert` / `append`). A name appears at most once and its
///   list is never empty.
/// * `removed` — names deleted from the **base** (removing an overlay-only
///   name simply leaves `overrides`, recording nothing). `removed` and
///   `overrides` never overlap.
#[derive(Clone, Debug, Default)]
pub struct OutboundHeaders {
    base: HeaderMap,
    overrides: Vec<(String, Vec<String>)>,
    removed: Vec<String>,
}

impl OutboundHeaders {
    /// An empty delta over `base`. O(1): `HeaderMap` is an [`Arc`]-backed
    /// copy-on-write map, so this shares (never copies) the base payload.
    pub fn new(base: HeaderMap) -> Self {
        Self {
            base,
            overrides: Vec::new(),
            removed: Vec::new(),
        }
    }

    /// Replace all values of `name` with `value` — shadows any base entry with
    /// the same (lowercased) name.
    pub fn insert(&mut self, name: &str, value: impl Into<String>) {
        let key = HeaderMap::lookup(name);
        // A re-add after a remove cancels the removal.
        if let Some(pos) = self.removed.iter().position(|n| n == key.as_ref()) {
            self.removed.remove(pos);
        }
        if let Some((_, values)) = self
            .overrides
            .iter_mut()
            .find(|(n, _)| n == key.as_ref())
        {
            values.clear();
            values.push(value.into());
        } else {
            self.overrides.push((HeaderMap::key(name), vec![value.into()]));
        }
    }

    /// Append a value (multi-value semantics). The FIRST `append` over a live
    /// base name inherits the base's current values (a bounded copy of that one
    /// name's list only — the base entries themselves are never copied);
    /// `append` over an overlay-owned name extends the overlay list. A
    /// re-`append` after a [`OutboundHeaders::remove`] starts a FRESH list (map
    /// remove-then-append yields just the appended value — Q1: no base
    /// inheritance through a suppression record).
    pub fn append(&mut self, name: &str, value: impl Into<String>) {
        let key = HeaderMap::lookup(name);
        let was_removed = self.removed.iter().position(|n| n == key.as_ref());
        if let Some(pos) = was_removed {
            self.removed.remove(pos);
        }
        if let Some((_, values)) = self
            .overrides
            .iter_mut()
            .find(|(n, _)| n == key.as_ref())
        {
            values.push(value.into());
        } else if was_removed.is_some() {
            // The name was just un-suppressed: a fresh single-value override
            // (the base list was deleted by the `remove`).
            self.overrides.push((HeaderMap::key(name), vec![value.into()]));
        } else {
            let mut values = self.base.get_all(key.as_ref()).to_vec();
            values.push(value.into());
            self.overrides.push((HeaderMap::key(name), values));
        }
    }

    /// Remove all values of `name` — deletes an overlay entry and suppresses a
    /// base entry of the same name.
    pub fn remove(&mut self, name: &str) {
        let key = HeaderMap::lookup(name);
        if let Some(pos) = self.overrides.iter().position(|(n, _)| n == key.as_ref()) {
            self.overrides.remove(pos);
        }
        if self.base.contains(key.as_ref()) && !self.removed.iter().any(|n| n == key.as_ref()) {
            // Only base-present names need a suppression record (an overlay-only
            // removal already left `overrides` above).
            self.removed.push(key.into_owned());
        }
    }

    /// First value of `name`, if any.
    pub fn get(&self, name: &str) -> Option<&str> {
        let key = HeaderMap::lookup(name);
        if let Some((_, values)) = self.overrides.iter().find(|(n, _)| n == key.as_ref()) {
            return values.first().map(String::as_str);
        }
        if self.removed.iter().any(|n| n == key.as_ref()) {
            return None;
        }
        self.base.get(key.as_ref())
    }

    /// All values of `name` (empty slice when absent).
    pub fn get_all(&self, name: &str) -> &[String] {
        let key = HeaderMap::lookup(name);
        if let Some((_, values)) = self.overrides.iter().find(|(n, _)| n == key.as_ref()) {
            return values.as_slice();
        }
        if self.removed.iter().any(|n| n == key.as_ref()) {
            return &[];
        }
        self.base.get_all(key.as_ref())
    }

    /// Number of values of `name`.
    pub fn count(&self, name: &str) -> usize {
        self.get_all(name).len()
    }

    /// Whether `name` has any value after applying the delta: present when an
    /// overlay entry shadows it, or when it is in the base and not removed.
    pub fn contains(&self, name: &str) -> bool {
        let key = HeaderMap::lookup(name);
        if self.overrides.iter().any(|(n, _)| n == key.as_ref()) {
            return true;
        }
        !self.removed.iter().any(|n| n == key.as_ref()) && self.base.contains(key.as_ref())
    }

    /// All header names present (lowercase form): the base names minus
    /// removed, emitted in base-map order, then the overlay-only names in
    /// first-touch order. A name shadowed by an overlay entry (present in both
    /// base and `overrides`) is emitted ONCE at its base position — matching
    /// [`OutboundHeaders::materialize`] / [`OutboundHeaders::into_pairs`], whose
    /// `names()` the caller must agree with (M2).
    pub fn names(&self) -> impl Iterator<Item = &str> + '_ {
        self.base
            .names()
            .filter(move |name| !self.removed.iter().any(|n| n == *name))
            .chain(
                self.overrides
                    .iter()
                    .filter(move |(n, _)| !self.base.contains(n))
                    .map(|(n, _)| n.as_str()),
            )
    }

    /// AM-6b: the dial materialization — consume the overlay into owned
    /// `(name, value)` pairs, one pair per value. The base entries are cloned
    /// once (the base stays shared with `prepared.base_headers` for the whole
    /// failover loop); the overlay's own strings are **moved**. Order: base
    /// map order (a shadowed base name is emitted at its base position with the
    /// overlay's list), then overlay-only names in first-touch order. The pair
    /// SET and the per-name value order are identical to
    /// [`HeaderMap::into_pairs`] on the clone-then-mutate result; the
    /// name-level order is deterministic here (the reference map's order was
    /// HashMap-hash-dependent and therefore not a stable contract).
    pub fn into_pairs(self) -> Vec<(String, String)> {
        let OutboundHeaders {
            base,
            mut overrides,
            removed,
        } = self;
        // One pass over the base for the exact pair count (the result Vec never
        // re-grows).
        let mut pair_count: usize = 0;
        for (name, base_values) in base.map.iter() {
            if removed.iter().any(|r| r == name) {
                continue;
            }
            pair_count += match overrides.iter().find(|(n, _)| n == name) {
                Some((_, values)) => values.len(),
                None => base_values.len(),
            };
        }
        pair_count += overrides
            .iter()
            .filter(|(n, _)| !base.map.contains_key(n))
            .map(|(_, values)| values.len())
            .sum::<usize>();

        let mut pairs = Vec::with_capacity(pair_count);
        for (name, base_values) in base.map.iter() {
            if removed.iter().any(|r| r == name) {
                continue;
            }
            // A shadowed base name: emit the overlay's list (drain-moved) at the
            // base key's position; the trailing loop then skips it.
            if let Some(idx) = overrides.iter().position(|(n, _)| n == name) {
                let (_, values) = overrides.remove(idx);
                push_name_value_pairs(&mut pairs, name.clone(), values);
                continue;
            }
            for value in base_values {
                pairs.push((name.clone(), value.clone()));
            }
        }
        // The survivors are overlay-only names, in first-touch order.
        for (name, values) in overrides {
            push_name_value_pairs(&mut pairs, name, values);
        }
        pairs
    }

    /// Materialize the overlay into a full [`HeaderMap`] — the egress-contract
    /// boundary (the frozen `ProviderClient` needs a real `CoreHeaderMap`).
    /// This is the ONE base deep copy of the overlay path, paid only when a
    /// provider candidate is actually dialed: `base.clone()` is an O(1) Arc
    /// bump and the first mutation below triggers exactly one `make_mut`.
    /// Content-identical to the AM-6 clone-then-mutate build.
    pub fn materialize(&self) -> HeaderMap {
        let mut m = self.base.clone();
        if !self.removed.is_empty() {
            for name in &self.removed {
                if m.contains(name) {
                    m.remove(name);
                }
            }
        }
        for (name, values) in &self.overrides {
            if let Some((first, rest)) = values.split_first() {
                m.insert(name, first.clone());
                for v in rest {
                    m.append(name, v.clone());
                }
            }
        }
        m
    }
}

impl From<HeaderMap> for OutboundHeaders {
    fn from(base: HeaderMap) -> Self {
        Self::new(base)
    }
}

impl HeaderOps for OutboundHeaders {
    fn header_get_all(&self, name: &str) -> &[String] {
        self.get_all(name)
    }
    fn header_insert(&mut self, name: &str, value: String) {
        self.insert(name, value);
    }
    fn header_append(&mut self, name: &str, value: String) {
        self.append(name, value);
    }
    fn header_remove(&mut self, name: &str) {
        self.remove(name);
    }
}

/// Semantic equality: two overlays are equal when their materialized maps are.
impl PartialEq for OutboundHeaders {
    fn eq(&self, other: &Self) -> bool {
        self.materialize() == other.materialize()
    }
}

impl Eq for OutboundHeaders {}

/// Push `values` into `pairs`, cloning `name` for every value except the LAST
/// (which moves it) — the [`HeaderMap::into_pairs`] drain pattern.
fn push_name_value_pairs(
    pairs: &mut Vec<(String, String)>,
    name: String,
    values: Vec<String>,
) {
    let total = values.len();
    let mut iter = values.into_iter();
    for _ in 0..total.saturating_sub(1) {
        let value = iter.next().expect("prefix count == values.len() - 1");
        pairs.push((name.clone(), value));
    }
    if let Some(value) = iter.next() {
        pairs.push((name, value));
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
    /// Keep the first value.
    RetainFirst,
    /// Keep the last value.
    RetainLast,
}

/// One ordered transformation rule.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransformRule {
    /// The operation to apply.
    pub op: TransformOp,
    /// Source header name the rule operates on.
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
    /// A transformer that executes `rules` strictly in list order.
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

    /// The configured rules, in execution order.
    pub fn rules(&self) -> &[TransformRule] {
        &self.rules
    }

    /// Apply all rules in order to `headers` (in place).
    ///
    /// Generic over [`HeaderOps`]: `H` is a [`HeaderMap`] for the materialized
    /// paths (inbound transform) or an [`OutboundHeaders`] for the lazy
    /// outbound build (AM-6b) — one rule implementation, no drift possible.
    pub fn apply<H: HeaderOps>(&self, headers: &mut H) {
        for rule in &self.rules {
            match rule.op {
                TransformOp::Remove => {
                    headers.header_remove(&rule.source);
                }
                TransformOp::Rename => {
                    if let Some(dest) = &rule.dest {
                        let values: Vec<String> = headers.header_get_all(&rule.source).to_vec();
                        for v in values {
                            headers.header_append(dest, v);
                        }
                        headers.header_remove(&rule.source);
                    }
                }
                TransformOp::Dedupe => {
                    let values = headers.header_get_all(&rule.source);
                    if values.len() > 1 {
                        let kept = match rule.mode.unwrap_or(RetainMode::RetainFirst) {
                            RetainMode::RetainFirst => values.first().cloned(),
                            RetainMode::RetainLast => values.last().cloned(),
                        };
                        if let Some(v) = kept {
                            headers.header_insert(&rule.source, v);
                        } else {
                            headers.header_remove(&rule.source);
                        }
                    }
                }
                TransformOp::Backup => {
                    if let Some(dest) = &rule.dest {
                        let values: Vec<String> = headers.header_get_all(&rule.source).to_vec();
                        for v in values {
                            headers.header_append(dest, v);
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

    // ----- AM-6b: OutboundHeaders (lazy overlay) ≡ clone-then-mutate HeaderMap -----

    /// A realistic base: single-value names + a multi-value `set-cookie` +
    /// duplicate-cased `x-gpustack-route-name` (the transformer-outbound dedupe
    /// input) + the mirrored `:path`.
    fn base_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("Host", "llm.gpustack.local");
        h.insert("content-type", "application/json");
        h.insert("Authorization", "Bearer sk-client");
        h.insert("X-Higress-Llm-Model", "org1/llama-3-8b");
        h.insert("x-organization-id", "org-42");
        h.insert("content-length", "128");
        h.append("set-cookie", "a=1");
        h.append("SET-COOKIE", "b=2");
        h.append("x-gpustack-route-name", "r1");
        h.append("X-GPUStack-Route-Name", "r2");
        h.insert(":path", "/v1/chat/completions");
        h
    }

    /// The op sequence a mutating outbound candidate runs (auth write-back
    /// replace, ⑨ pre-route inserts, multi-value append, hop-by-hop guard
    /// removes) — executed through the SAME [`HeaderOps`] surface on both the
    /// clone-then-mutate [`HeaderMap`] and the [`OutboundHeaders`] overlay.
    fn golden_outbound_sequence(h: &mut impl HeaderOps) {
        // ext-auth write-back REPLACES the client credential / adds cookie.
        h.header_insert("Authorization", "Bearer reg-token".to_string());
        h.header_insert("Cookie", "session=writeme".to_string());
        h.header_insert("X-Mse-Consumer", "ak.gpustack-7".to_string());
        // ⑨ set-instance / route-name.
        h.header_insert("X-GPUStack-Model-Instance", "model-1-10.static".to_string());
        h.header_insert(
            "X-GPUStack-Route-Name",
            "higress-system/ai-route-route-1.internal".to_string(),
        );
        // multi-value append over a base-present name.
        h.header_append("set-cookie", "c=3".to_string());
        // hop-by-hop guard removes: present -> deleted, absent -> no-op.
        h.header_remove("content-length");
        h.header_remove("connection");
        // remove-then-reinsert cycle.
        h.header_remove("x-mse-consumer");
        h.header_insert("X-Mse-Consumer", "ak.gpustack-7".to_string());
        h.header_remove("x-organization-id");
    }

    #[test]
    fn outbound_overlay_matches_clone_then_mutate_golden() {
        let mut reference = base_headers();
        golden_outbound_sequence(&mut reference);
        let mut overlay = OutboundHeaders::new(base_headers());
        golden_outbound_sequence(&mut overlay);
        // The materialized overlay equals the clone-then-mutate HeaderMap.
        assert_eq!(overlay.materialize(), reference);
        // Reads over the overlay equal reads over the reference map.
        assert_eq!(overlay.get("authorization"), reference.get("authorization"));
        assert_eq!(overlay.get("cookie"), reference.get("cookie"));
        assert_eq!(
            overlay.get("x-gpustack-model-instance"),
            reference.get("x-gpustack-model-instance")
        );
        assert_eq!(overlay.count("set-cookie"), reference.count("set-cookie"));
        assert_eq!(overlay.get_all("set-cookie"), reference.get_all("set-cookie"));
        assert_eq!(
            overlay.get("x-organization-id"),
            reference.get("x-organization-id")
        );
        assert!(!overlay.contains("content-length"));
        assert_eq!(overlay.count("x-mse-consumer"), reference.count("x-mse-consumer"));
        assert_eq!(
            overlay.contains("x-mse-consumer"),
            reference.contains("x-mse-consumer")
        );
        assert_eq!(
            overlay.get("content-length"),
            reference.get("content-length")
        );
    }

    #[test]
    fn outbound_overlay_into_pairs_matches_materialized_drain() {
        let mut reference = base_headers();
        golden_outbound_sequence(&mut reference);
        let mut overlay = OutboundHeaders::new(base_headers());
        golden_outbound_sequence(&mut overlay);

        let mut pairs_overlay = overlay.clone().into_pairs();
        let mut pairs_reference = reference.into_pairs();
        // Pair SET + per-name value order are the contract; name-level order is
        // not (the reference drained a HashMap).
        pairs_overlay.sort();
        pairs_reference.sort();
        assert_eq!(pairs_overlay, pairs_reference);
        assert!(
            !pairs_overlay.iter().any(|(n, _)| n == "content-length"),
            "content-length was removed and must not be drained: {pairs_overlay:?}"
        );
    }

    #[test]
    fn outbound_overlay_reads_are_case_insensitive() {
        let mut o = OutboundHeaders::new(base_headers());
        // Base reads with any casing.
        assert_eq!(o.get("AUTHORIZATION"), Some("Bearer sk-client"));
        assert_eq!(o.count("Set-Cookie"), 2);
        // Overlay insert shadows the base under any casing, read under any.
        o.insert("AUTHORIZATION", "Bearer swapped");
        assert_eq!(o.get("authorization"), Some("Bearer swapped"));
        assert_eq!(o.count("AUTHORIZATION"), 1);
        o.remove("Authorization");
        assert_eq!(o.get("authorization"), None);
        assert!(!o.contains("AUTHORIZATION"));
        assert_eq!(o.count("authorization"), 0);
    }

    #[test]
    fn overlay_names_match_materialized_names_including_shadowed() {
        // M2/Q1 regression: a name present in BOTH the base and the overlay
        // deltas (the normal auth-write-back replace / ⑨ pre-route case) was
        // dropped from both `names()` passes and never yielded. `names()` must
        // equal the materialized map's names (each name exactly once).
        let mut reference = base_headers();
        golden_outbound_sequence(&mut reference);
        let mut overlay = OutboundHeaders::new(base_headers());
        golden_outbound_sequence(&mut overlay);

        let mut materialized: Vec<String> = overlay.materialize().names().map(str::to_owned).collect();
        materialized.sort();
        let mut via_overlay: Vec<String> = overlay.names().map(str::to_owned).collect();
        via_overlay.sort();
        assert_eq!(
            via_overlay, materialized,
            "overlay.names() must equal materialize().names() (shadowed names included once)"
        );
        let mut reference_names: Vec<String> = reference.names().map(str::to_owned).collect();
        reference_names.sort();
        assert_eq!(
            via_overlay, reference_names,
            "overlay.names() must equal the clone-then-mutate reference map's names"
        );
        // The specific shadowed names must be present exactly once each.
        assert_eq!(
            via_overlay.iter().filter(|n| *n == "authorization").count(),
            1,
            "shadowed authorization must be yielded exactly once"
        );
    }

    #[test]
    fn overlay_remove_then_append_starts_fresh() {
        // Q1 regression: `append` after `remove` must NOT re-inherit the
        // removed base list through the suppression record (map
        // remove-then-append yields only the appended value).
        let mut reference = base_headers();
        reference.remove("set-cookie");
        reference.append("set-cookie", "c=3");
        let mut overlay = OutboundHeaders::new(base_headers());
        overlay.remove("set-cookie");
        overlay.append("set-cookie", "c=3");

        assert_eq!(overlay.materialize(), reference);
        assert_eq!(overlay.get_all("set-cookie"), reference.get_all("set-cookie"));
        assert_eq!(overlay.count("set-cookie"), 1);
        assert_eq!(overlay.get("set-cookie"), Some("c=3"));
    }

    #[test]
    fn overlay_insert_after_remove_over_base_name_replaces() {
        // ora-6 follow-up (quality O2): `insert` after `remove` over a
        // BASE-present name must cancel the suppression and REPLACE with the
        // single new value (no re-inherited base values) — same map
        // remove-then-insert semantics.
        let mut reference = base_headers();
        reference.remove("authorization");
        reference.insert("authorization", "Bearer swapped");
        let mut overlay = OutboundHeaders::new(base_headers());
        overlay.remove("authorization");
        overlay.insert("authorization", "Bearer swapped");

        assert_eq!(overlay.materialize(), reference);
        assert_eq!(
            overlay.get_all("authorization"),
            reference.get_all("authorization")
        );
        assert_eq!(overlay.count("authorization"), 1);
        assert_eq!(overlay.get("authorization"), Some("Bearer swapped"));
    }

    #[test]
    fn outbound_overlay_transformer_apply_matches_header_map() {
        // The outbound keep rule set over the overlay (the build_outbound path)
        // dedupes the base-carried duplicate route-name exactly as over a map.
        let mut reference = base_headers();
        Transformer::outbound().apply(&mut reference);
        let mut overlay = OutboundHeaders::new(base_headers());
        Transformer::outbound().apply(&mut overlay);
        assert_eq!(overlay.materialize(), reference);
        // The dedupe actually happened (retained first).
        assert_eq!(overlay.count("x-gpustack-route-name"), 1);
        assert_eq!(overlay.get("x-gpustack-route-name"), Some("r1"));
    }

    #[test]
    fn outbound_overlay_equality_is_semantic_not_structural() {
        // Identical content reached by different base/delta splits compares
        // equal (materialized equality), the property PartialEq is defined on.
        let mut a = OutboundHeaders::new(base_headers());
        golden_outbound_sequence(&mut a);
        let mut b = OutboundHeaders::new(base_headers());
        golden_outbound_sequence(&mut b);
        assert_eq!(a, b);
        b.remove("cookie");
        assert_ne!(a, b);
    }
}
