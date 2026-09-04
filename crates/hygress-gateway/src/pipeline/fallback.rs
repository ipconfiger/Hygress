//! ⑭ 4xx/5xx → fallback — pure. When a route's upstream fails with 4xx/5xx, the
//! gateway does an **internal redirect** to the route's linked Fallback route:
//! it sets `x-higress-fallback-from` to the main ingress name, restores the
//! original path (from the `x-gpustack-original-path` backstop) via
//! `x-gpustack-fallback-path`, and re-matches. The redirect is **bounded to 10**
//! internal hops (the EnvoyFilter `custom_response` `max_redirects`) so a
//! fallback loop cannot spin forever.

use hygress_core::prelude::{FallbackSpec, HeaderMap, RouteMatch, RouteTable};

use crate::context::hdr;

/// A planned fallback hop (pure).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FallbackPlan {
    /// The Fallback route key to match on (`x-higress-fallback-from` value).
    pub target_key: String,
    /// The original path to restore onto `:path` for the hop.
    pub restored_path: String,
    /// The hop budget this spec enforces (the max-10 guard).
    pub max_redirects: u32,
    /// The `x-gpustack-fallback-path` value to set for the redirect.
    pub fallback_path_header: String,
}

/// `true` when `redirect_count` hops have already been taken (guard: no more).
pub fn budget_exhausted(redirect_count: u32, spec: &FallbackSpec) -> bool {
    redirect_count >= spec.max_redirects
}

/// Build the fallback hop plan, or `None` when the hop budget is exhausted.
///
/// `redirect_count` is the number of fallback redirects already taken for this
/// request (0 on the first failure). `original_path` is the pre-rewrite
/// `:path` (the `x-gpustack-original-path` backstop).
pub fn plan(spec: &FallbackSpec, original_path: &str, redirect_count: u32) -> Option<FallbackPlan> {
    if budget_exhausted(redirect_count, spec) {
        return None;
    }
    Some(FallbackPlan {
        target_key: spec.target_key.clone(),
        restored_path: original_path.to_string(),
        max_redirects: spec.max_redirects,
        fallback_path_header: original_path.to_string(),
    })
}

/// Match a fallback redirect by `x-higress-fallback-from` (stage ⑭ re-match).
pub fn match_fallback(table: &RouteTable, headers: &HeaderMap, path: &str) -> Option<RouteMatch> {
    table.find_match_fallback(headers.get(hdr::FALLBACK_FROM), path)
}

/// Read the original (pre-rewrite) path backstop, if present.
pub fn original_path(headers: &HeaderMap) -> Option<String> {
    headers.get(hdr::ORIGINAL_PATH).map(|s| s.to_string())
}

/// Arm the fallback headers for one hop: set `x-higress-fallback-from` to the
/// plan's `target_key` and `x-gpustack-fallback-path` to the restored path.
/// (The inbound transformer then renames `x-gpustack-fallback-path` → `:path`.)
pub fn arm(headers: &mut HeaderMap, plan: &FallbackPlan) {
    headers.insert(hdr::FALLBACK_FROM, &plan.target_key);
    headers.insert(hdr::FALLBACK_PATH, &plan.fallback_path_header);
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::prelude::{ConfigData, Destination, PathPred, RouteKind, RouteRule};

    fn spec() -> FallbackSpec {
        FallbackSpec {
            route_key: "m".into(),
            main_ingress_name: "ns/ai-route-route-5.internal".into(),
            target_key: "ai-route-route-5.internal".into(),
            max_redirects: 10,
            use_original_body: true,
            use_original_uri: true,
        }
    }

    #[test]
    fn plan_within_budget() {
        let p = plan(&spec(), "/original/path", 0).unwrap();
        assert_eq!(p.target_key, "ai-route-route-5.internal");
        assert_eq!(p.restored_path, "/original/path");
        assert_eq!(p.max_redirects, 10);
    }

    #[test]
    fn max_ten_guard_blocks_eleventh_hop() {
        // Hops 0..9 are allowed; the 10th already-taken count blocks the next.
        assert!(plan(&spec(), "/p", 9).is_some());
        assert!(plan(&spec(), "/p", 10).is_none());
        assert!(budget_exhausted(10, &spec()));
        assert!(!budget_exhausted(9, &spec()));
    }

    #[test]
    fn arm_sets_fallback_headers() {
        let p = plan(&spec(), "/orig", 0).unwrap();
        let mut h = HeaderMap::new();
        arm(&mut h, &p);
        assert_eq!(h.get(hdr::FALLBACK_FROM), Some("ai-route-route-5.internal"));
        assert_eq!(h.get(hdr::FALLBACK_PATH), Some("/orig"));
    }

    #[test]
    fn original_path_reads_backstop() {
        let mut h = HeaderMap::new();
        h.insert(hdr::ORIGINAL_PATH, "/v1/chat/completions");
        assert_eq!(original_path(&h), Some("/v1/chat/completions".to_string()));
        assert_eq!(original_path(&HeaderMap::new()), None);
    }

    #[test]
    fn fallback_matches_only_via_fallback_from() {
        let data = ConfigData {
            routes: vec![],
            ..Default::default()
        };
        // No routes → no match (None).
        let t = RouteTable::rebuild(&data).unwrap();
        let mut h = HeaderMap::new();
        h.insert(hdr::FALLBACK_FROM, "x");
        assert!(match_fallback(&t, &h, "/x").is_none());

        // With a Fallback route, match_fallback selects it.
        let data2 = ConfigData {
            routes: vec![RouteRule::new(
                "ai-route-route-5.internal",
                RouteKind::Fallback,
                vec![PathPred::new("/(v1)()(/chat/completions)")],
                vec![Destination::new("b.static:80")],
            )
            .unwrap()],
            ..Default::default()
        };
        let t2 = RouteTable::rebuild(&data2).unwrap();
        // `x-higress-fallback-from` MUST be the target route's key — a wrong
        // key never selects a Fallback route (and a path alone never does).
        h.insert(hdr::FALLBACK_FROM, "wrong-key");
        assert!(match_fallback(&t2, &h, "/v1/chat/completions").is_none());
        // The matching key (the Fallback route's key) selects it.
        h.insert(hdr::FALLBACK_FROM, "ai-route-route-5.internal");
        let m = match_fallback(&t2, &h, "/v1/chat/completions").unwrap();
        assert_eq!(m.matched_by, hygress_core::MatchKind::FallbackExact);
    }
}
