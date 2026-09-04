//! ④' routing-policy override layer (design §4.3 / D-2 / D-12) — the **pure**
//! decoration applied to the matched **Main** route's [`PreparedRequest`]
//! **after** `route_match` (only on the initial dispatch,
//! `redirect_count == 0`; design §5 / D-3).
//!
//! The actions (all from the matched route's `policy:` slot):
//!
//! - **`override_route`** — replace `prepared.candidates` with the single
//!   target selected by core [`RoutePolicyActions::pick_override`] (an exact
//!   `name.type:port` among the candidates). The target is **not** cross-
//!   validated at load time (the policy slot and the CRD slot are independent
//!   `ArcSwap`s — D-2): a miss is a **runtime fallback** to the original
//!   routing (the pipe logs a warn and records `policy_applied=false`), never
//!   a load-time rejection.
//! - **`pin_provider_svc_pattern`** — filter the candidates by a
//!   `name.type` glob (core [`pin_matches`]); when the filter is non-empty it
//!   wins, when it is empty the (possibly override-adjusted) original list is
//!   kept (runtime fallback). There is no "region" dimension in the data
//!   model (D-2).
//! - **`header_add` / `header_del`** — applied to `prepared.base_headers`
//!   (the outbound header set every candidate inherits).
//! - **`timeout_ms` / `retries`** — stored on `prepared`
//!   (`override_timeout_ms` / `override_retries`) and applied by the pipe's
//!   forward stage (per-request reqwest timeout; the route's retry
//!   **conditions** are kept, only `tries` is overridden).
//!
//! Pure: no I/O, no `Session` — unit-testable in isolation.

use hygress_core::prelude::{pin_matches, RoutePolicyActions};

use crate::context::PreparedRequest;

/// The outcome of [`apply`] (drives the pipe's warn / `policy_applied` metric).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PolicyApply {
    /// At least one action took effect (override hit / pin applied / header
    /// add-del / timeout / retries). The pipe records `policy_applied=true`.
    pub applied: bool,
    /// `override_route` was set and matched a candidate (the candidates were
    /// replaced with the single target).
    pub override_hit: bool,
    /// `override_route` was set but matched **no** candidate — the original
    /// routing is kept (runtime fallback, D-2; the pipe warns).
    pub override_miss: bool,
    /// `pin_provider_svc_pattern` filtered the candidates to a non-empty set.
    pub pin_applied: bool,
    /// `pin_provider_svc_pattern` filtered to an **empty** set — the
    /// (possibly override-adjusted) original candidates are kept (runtime
    /// fallback; the pipe warns).
    pub pin_miss: bool,
}

/// Apply the matched route's routing-policy actions to `prepared` (in place).
pub fn apply(prepared: &mut PreparedRequest, actions: &RoutePolicyActions) -> PolicyApply {
    let mut out = PolicyApply::default();

    // 1. override_route: exact `name.type:port` among the candidates (D-2).
    if let Some(_target) = actions.override_route.as_deref() {
        let services: Vec<&str> = prepared.candidates.iter().map(|c| c.service.as_str()).collect();
        if let Some(hit) = actions.pick_override(&services) {
            if let Some(pos) = prepared.candidates.iter().position(|c| c.service == hit) {
                let c = prepared.candidates.remove(pos);
                prepared.selected_service = c.service_name.clone();
                prepared.candidates = vec![c];
                out.override_hit = true;
                out.applied = true;
            } else {
                // `pick_override` only returns a value present in `services` —
                // unreachable; the miss path below covers every real case.
                out.override_miss = true;
            }
        } else {
            // Target not among the candidates → runtime fallback (D-2).
            out.override_miss = true;
        }
    }

    // 2. pin_provider_svc_pattern: filter by `name.type` glob (D-2).
    if let Some(pattern) = actions.pin_provider_svc_pattern.as_deref() {
        let filtered: Vec<_> = prepared
            .candidates
            .iter()
            .filter(|c| pin_matches(pattern, &c.service_name))
            .cloned()
            .collect();
        if !filtered.is_empty() {
            prepared.candidates = filtered;
            if let Some(first) = prepared.candidates.first() {
                prepared.selected_service = first.service_name.clone();
            }
            out.pin_applied = true;
            out.applied = true;
        } else {
            out.pin_miss = true;
        }
    }

    // 3. header_add / header_del on the base (outbound) header set.
    for (name, value) in &actions.header_add {
        prepared.base_headers.insert(name, value);
    }
    for name in &actions.header_del {
        prepared.base_headers.remove(name);
    }
    if !actions.header_add.is_empty() || !actions.header_del.is_empty() {
        out.applied = true;
    }

    // 4. timeout / retries overrides (applied by the pipe's forward stage).
    prepared.override_timeout_ms = actions.timeout_ms;
    prepared.override_retries = actions.retries;
    if actions.timeout_ms.is_some() || actions.retries.is_some() {
        out.applied = true;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{CandidateTarget, RouteInfo, Scheme};
    use hygress_core::prelude::{HeaderMap, MatchKind};

    fn candidate(service: &str, address: &str) -> CandidateTarget {
        let service_name = service.split(':').next().unwrap_or(service).to_string();
        CandidateTarget {
            service: service.to_string(),
            service_name,
            address: address.to_string(),
            proxied: false,
            scheme: Scheme::Http,
            proxy: None,
        }
    }

    fn prepared_with(candidates: Vec<CandidateTarget>) -> PreparedRequest {
        let first = candidates.first().cloned().unwrap_or_else(|| {
            candidate("model-1-10.static:80", "10.0.0.5:8081")
        });
        PreparedRequest {
            candidates,
            route: RouteInfo {
                route_key: "org1/llama-3-8b".into(),
                ingress_name: "higress-system/ai-route-route-1.internal".into(),
                matched_by: MatchKind::HeaderExact,
                is_model_route: true,
                model: "org1/llama-3-8b".into(),
                auth_required: true,
                retry: Default::default(),
                fallback: None,
                matched_predicate: None,
                path_groups: vec![],
            },
            base_headers: HeaderMap::new(),
            upstream_path: "/v1/chat/completions".into(),
            query: String::new(),
            body: bytes::Bytes::new(),
            body_model: None,
            content_type: "application/json".into(),
            model_mapping: Default::default(),
            usage: None,
            selected_service: first.service_name.clone(),
            started_at_ms: 0,
            override_timeout_ms: None,
            override_retries: None,
        }
    }

    // ----- override_route (D-2) -----

    #[test]
    fn override_hit_replaces_candidates_with_single_target() {
        let mut p = prepared_with(vec![
            candidate("model-1-10.static:80", "10.0.0.5:8081"),
            candidate("model-8-6.static:80", "10.0.0.8:8081"),
        ]);
        let actions = RoutePolicyActions {
            override_route: Some("model-8-6.static:80".into()),
            ..Default::default()
        };
        let out = apply(&mut p, &actions);
        assert!(out.override_hit);
        assert!(out.applied);
        assert!(!out.override_miss);
        assert_eq!(p.candidates.len(), 1);
        assert_eq!(p.candidates[0].service, "model-8-6.static:80");
        assert_eq!(p.selected_service, "model-8-6.static");
    }

    #[test]
    fn override_miss_keeps_original_candidates() {
        // The target is not among the candidates → runtime fallback (D-2):
        // the original routing is kept and the miss is reported.
        let mut p = prepared_with(vec![
            candidate("model-1-10.static:80", "10.0.0.5:8081"),
            candidate("model-9-9.static:80", "10.0.0.9:8081"),
        ]);
        let actions = RoutePolicyActions {
            override_route: Some("model-8-6.static:80".into()),
            ..Default::default()
        };
        let out = apply(&mut p, &actions);
        assert!(out.override_miss);
        assert!(!out.override_hit);
        assert!(!out.applied); // no action took effect
        assert_eq!(p.candidates.len(), 2);
        assert_eq!(p.candidates[0].service, "model-1-10.static:80");
    }

    #[test]
    fn override_unset_is_a_no_op() {
        let mut p = prepared_with(vec![candidate("model-1-10.static:80", "10.0.0.5:8081")]);
        let out = apply(&mut p, &RoutePolicyActions::default());
        assert!(!out.override_hit && !out.override_miss);
        assert_eq!(p.candidates.len(), 1);
    }

    // ----- pin_provider_svc_pattern (D-2) -----

    #[test]
    fn pin_filters_candidates_by_service_pattern() {
        let mut p = prepared_with(vec![
            candidate("provider-8.proxy:443", "10.0.0.1:443"),
            candidate("provider-9.dns:443", "10.0.0.2:443"),
            candidate("model-1-10.static:80", "10.0.0.5:8081"),
        ]);
        let actions = RoutePolicyActions {
            pin_provider_svc_pattern: Some("provider-8.*".into()),
            ..Default::default()
        };
        let out = apply(&mut p, &actions);
        assert!(out.pin_applied);
        assert!(out.applied);
        assert_eq!(p.candidates.len(), 1);
        assert_eq!(p.candidates[0].service_name, "provider-8.proxy");
        assert_eq!(p.selected_service, "provider-8.proxy");
    }

    #[test]
    fn pin_empty_filter_keeps_original() {
        // The pattern matches nothing → the original candidates are kept
        // (runtime fallback; a warn is the pipe's job).
        let mut p = prepared_with(vec![
            candidate("provider-9.dns:443", "10.0.0.2:443"),
            candidate("model-1-10.static:80", "10.0.0.5:8081"),
        ]);
        let actions = RoutePolicyActions {
            pin_provider_svc_pattern: Some("provider-8.*".into()),
            ..Default::default()
        };
        let out = apply(&mut p, &actions);
        assert!(out.pin_miss);
        assert!(!out.pin_applied);
        assert_eq!(p.candidates.len(), 2);
    }

    #[test]
    fn override_then_pin_compose() {
        // The override pins a single provider; the pin pattern then keeps it.
        let mut p = prepared_with(vec![
            candidate("provider-8.proxy:443", "10.0.0.1:443"),
            candidate("provider-9.dns:443", "10.0.0.2:443"),
        ]);
        let actions = RoutePolicyActions {
            override_route: Some("provider-8.proxy:443".into()),
            pin_provider_svc_pattern: Some("provider-8.*".into()),
            ..Default::default()
        };
        let out = apply(&mut p, &actions);
        assert!(out.override_hit && out.pin_applied);
        assert_eq!(p.candidates.len(), 1);
        assert_eq!(p.candidates[0].service, "provider-8.proxy:443");
    }

    // ----- header_add / header_del -----

    #[test]
    fn headers_added_and_removed() {
        let mut p = prepared_with(vec![candidate("model-1-10.static:80", "10.0.0.5:8081")]);
        p.base_headers.insert("x-internal", "secret");
        let actions = RoutePolicyActions {
            header_add: vec![("x-canary".into(), "true".into())],
            header_del: vec!["x-internal".into()],
            ..Default::default()
        };
        let out = apply(&mut p, &actions);
        assert!(out.applied);
        assert_eq!(p.base_headers.get("x-canary"), Some("true"));
        assert_eq!(p.base_headers.get("x-internal"), None);
    }

    // ----- timeout / retries -----

    #[test]
    fn timeout_and_retries_stored_on_prepared() {
        let mut p = prepared_with(vec![candidate("model-1-10.static:80", "10.0.0.5:8081")]);
        let actions = RoutePolicyActions {
            timeout_ms: Some(30_000),
            retries: Some(2),
            ..Default::default()
        };
        let out = apply(&mut p, &actions);
        assert!(out.applied);
        assert_eq!(p.override_timeout_ms, Some(30_000));
        assert_eq!(p.override_retries, Some(2));
    }
}
