//! Header + path route matching (design §6.2 match order, corrected).
//!
//! The match model is split into **two separate key spaces** that mirror the
//! two Higress Ingresses GPUStack writes (the main `ai-route-route-<id>.internal`
//! and the fallback `ai-route-route-<id>.fallback.internal`):
//!
//! 1. **Main (header-exact)** — an *initial* request is matched to a **Main**
//!    route only when its `x-higress-llm-model` header **exactly equals** the
//!    route's `key` **and** one of that route's **full-match** path predicates
//!    matches the path. There is *no* path-only selection across Main routes:
//!    a header value that names one route never routes to a different route
//!    whose path the request happens to carry.
//! 2. **Mirror (catch-all)** — the `Mirror` route (predicate `/`) is the only
//!    path-based catch-all. Every header-less / unknown-model / non-matching
//!    path request lands here.
//! 3. **Fallback (header-exact)** — *only* during a fallback redirect the
//!    gateway matches on `x-higress-fallback-from` to a **Fallback** route
//!    whose `key` equals the redirect value (the main ingress name). This index
//!    is consulted exclusively through [`match_fallback_by_key`]; an initial
//!    request can never select a Fallback route.
//!
//! Path predicates are **full-match** (anchored) regexes compiled at
//! [`RouteTable::rebuild`]. The longest-literal-anchor ranking is used **only**
//! to choose the predicate *within* an already-matched route (for rewrite
//! capture) — never to pick among routes.

use crate::config::RouteTable;
use crate::transform::HeaderMap;

/// `x-higress-llm-model` — the core header routing key (exact match, Main
/// routes).
pub const LLM_MODEL_HEADER: &str = "x-higress-llm-model";

/// `x-higress-fallback-from` — set by the internal 4xx/5xx redirect to the
/// main ingress name; matches Fallback route keys.
pub const FALLBACK_FROM_HEADER: &str = "x-higress-fallback-from";

/// How a route was matched (match metadata).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchKind {
    /// Main route: exact `x-higress-llm-model` equals the route key AND the
    /// route's full-match path predicate matched the path.
    HeaderExact,
    /// Fallback route: exact `x-higress-fallback-from` equals the route key AND
    /// the route's full-match path predicate matched the path.
    FallbackExact,
    /// The mirror route catch-all (reached only after the header stages miss).
    Mirror,
}

/// A successful route match: the route index in the table plus metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RouteMatch {
    /// Index into [`RouteTable::routes`].
    pub index: usize,
    /// How the route was matched ([`MatchKind`]).
    pub matched_by: MatchKind,
    /// The predicate (within the matched route) that established the match;
    /// `None` if the route has no path predicates.
    pub matched_predicate: Option<usize>,
}

/// Find the route for an **initial** request.
///
/// Reads `x-higress-llm-model` and matches a **Main** route (header-exact AND
/// path) else the mirror. A Fallback route is never selectable here.
pub fn match_route(table: &RouteTable, headers: &HeaderMap, path: &str) -> Option<RouteMatch> {
    let key = headers.get(LLM_MODEL_HEADER);
    match_route_by_key(table, key, path)
}

/// Find the route for an **initial** request given the explicit
/// `x-higress-llm-model` value.
///
/// `None` (header absent) skips the header stage and goes straight to the
/// mirror. A Fallback route is **never** selected by an initial request, even
/// when the header value coincides with a Fallback route's key.
pub fn match_route_by_key(
    table: &RouteTable,
    model_key: Option<&str>,
    path: &str,
) -> Option<RouteMatch> {
    table.find_match(model_key, path)
}

/// Find the route for a **fallback redirect** given the inbound headers.
///
/// Reads `x-higress-fallback-from` (the main ingress name the internal
/// redirect set) and matches a **Fallback** route (header-exact AND path) else
/// the mirror. A Main route is never selected here.
pub fn match_fallback_route(table: &RouteTable, headers: &HeaderMap, path: &str) -> Option<RouteMatch> {
    let fallback_from = headers.get(FALLBACK_FROM_HEADER);
    match_fallback_by_key(table, fallback_from, path)
}

/// Find the route for a **fallback redirect** given the explicit
/// `x-higress-fallback-from` value (the main ingress name).
///
/// Consulted only during fallback redirect attempts; an initial request must
/// not use this. A Main route is never selected here.
pub fn match_fallback_by_key(
    table: &RouteTable,
    fallback_from: Option<&str>,
    path: &str,
) -> Option<RouteMatch> {
    table.find_match_fallback(fallback_from, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConfigData, RouteTable};
    use crate::destination::Destination;
    use crate::route::{PathPred, RouteKind, RouteRule};

    /// Test topology (full-match, real `regex_prefixes`-shaped patterns):
    /// - routes[0] "gpt-4o"               Main,     `/(v1)()(/chat/completions|/embeddings)`
    /// - routes[1] "claude"               Main,     `/(v1)()(/messages)`
    /// - routes[2] "ai-route-route-1.int." Fallback, `/(v1)()(/chat/completions|/embeddings)`
    /// - routes[3] "gpustack"             Mirror,   `/`
    ///
    /// A Fallback route is keyed by the main ingress name (the value
    /// `x-higress-fallback-from` carries).
    fn table() -> RouteTable {
        let data = ConfigData {
            routes: vec![
                RouteRule::new(
                    "gpt-4o",
                    RouteKind::Main,
                    vec![PathPred::new("/(v1)()(/chat/completions|/embeddings)")],
                    vec![Destination::new("model-1-10.static:80")],
                )
                .unwrap(),
                RouteRule::new(
                    "claude",
                    RouteKind::Main,
                    vec![PathPred::new("/(v1)()(/messages)")],
                    vec![Destination::new("model-2-10.static:80")],
                )
                .unwrap(),
                RouteRule::new(
                    "ai-route-route-1.internal",
                    RouteKind::Fallback,
                    vec![PathPred::new("/(v1)()(/chat/completions|/embeddings)")],
                    vec![Destination::new("model-3-10.static:80")],
                )
                .unwrap(),
                RouteRule::new(
                    "gpustack",
                    RouteKind::Mirror,
                    vec![PathPred::new("/")],
                    vec![Destination::new("gpustack.dns:30080")],
                )
                .unwrap(),
            ],
            ..Default::default()
        };
        RouteTable::rebuild(&data).unwrap()
    }

    fn mirror_idx() -> usize {
        3
    }

    // ----- initial: header-exact Main + path -----

    #[test]
    fn exact_header_match_wins() {
        let t = table();
        let m = match_route_by_key(&t, Some("gpt-4o"), "/v1/chat/completions").unwrap();
        assert_eq!(m.index, 0);
        assert_eq!(m.matched_by, MatchKind::HeaderExact);
        assert_eq!(m.matched_predicate, Some(0));
    }

    #[test]
    fn header_less_ai_path_goes_to_mirror() {
        // No x-higress-llm-model: even an obviously-AI path is NOT selected by
        // path alone — it lands on the mirror catch-all.
        let t = table();
        let m = match_route_by_key(&t, None, "/v1/chat/completions").unwrap();
        assert_eq!(m.index, mirror_idx());
        assert_eq!(m.matched_by, MatchKind::Mirror);
    }

    #[test]
    fn unknown_header_goes_to_mirror() {
        let t = table();
        let m = match_route_by_key(&t, Some("nope"), "/v1/chat/completions").unwrap();
        assert_eq!(m.index, mirror_idx());
        assert_eq!(m.matched_by, MatchKind::Mirror);
    }

    #[test]
    fn present_header_nonmatching_path_goes_to_mirror() {
        // Header names gpt-4o, but /v1/messages is not one of gpt-4o's paths:
        // no cross-route path selection, so the mirror.
        let t = table();
        let m = match_route_by_key(&t, Some("gpt-4o"), "/v1/messages").unwrap();
        assert_eq!(m.index, mirror_idx());
    }

    #[test]
    fn client_header_one_route_path_another_route_goes_to_mirror() {
        // A client set x-higress-llm-model to claude's key (route 1) while the
        // path is gpt-4o's: the header selects claude, but claude's path doesn
        // match /v1/chat/completions -> mirror (NOT picked across routes).
        let t = table();
        let m = match_route_by_key(&t, Some("claude"), "/v1/chat/completions").unwrap();
        assert_eq!(m.index, mirror_idx());
    }

    #[test]
    fn suffix_does_not_match_model_route() {
        // Full-match anchoring: /v1/chat/completions/extra is not consumed by
        // the (v1)()(/chat/completions|/embeddings) pattern -> mirror.
        let t = table();
        let m = match_route_by_key(&t, Some("gpt-4o"), "/v1/chat/completions/extra").unwrap();
        assert_eq!(m.index, mirror_idx());
        // And the exact path (no suffix) matches the model route.
        let ok = match_route_by_key(&t, Some("gpt-4o"), "/v1/chat/completions").unwrap();
        assert_eq!(ok.index, 0);
        assert_eq!(ok.matched_by, MatchKind::HeaderExact);
    }

    // ----- initial: never reaches a Fallback route -----

    #[test]
    fn initial_request_never_selects_fallback_route() {
        // The header value equals a Fallback route's key, but an initial
        // request must never be routed to a Fallback rule.
        let t = table();
        let m = match_route_by_key(
            &t,
            Some("ai-route-route-1.internal"),
            "/v1/chat/completions",
        )
        .unwrap();
        assert_eq!(m.index, mirror_idx());
        assert_ne!(m.matched_by, MatchKind::FallbackExact);
    }

    // ----- fallback: only via x-higress-fallback-from -----

    #[test]
    fn fallback_via_fallback_from_header() {
        let t = table();
        let m = match_fallback_by_key(&t, Some("ai-route-route-1.internal"), "/v1/chat/completions")
            .unwrap();
        assert_eq!(m.index, 2);
        assert_eq!(m.matched_by, MatchKind::FallbackExact);
    }

    #[test]
    fn fallback_requires_path_match_too() {
        let t = table();
        // Fallback key matches but the path is not a gpt-4o-style path ->
        // mirror (path AND header).
        let m = match_fallback_by_key(&t, Some("ai-route-route-1.internal"), "/v1/messages").unwrap();
        assert_eq!(m.index, mirror_idx());
        // Unknown fallback-from -> mirror.
        let m2 = match_fallback_by_key(&t, Some("ghost"), "/v1/chat/completions").unwrap();
        assert_eq!(m2.index, mirror_idx());
    }

    // ----- mirror is the last resort (path-based catch-all) -----

    #[test]
    fn mirror_is_last_resort() {
        let t = table();
        let m = match_route_by_key(&t, None, "/token-auth").unwrap();
        assert_eq!(m.index, mirror_idx());
        assert_eq!(m.matched_by, MatchKind::Mirror);
        assert_eq!(m.matched_predicate, Some(0));
    }

    #[test]
    fn nothing_matches_without_mirror() {
        // No mirror present: header-less and header+nonmatching path both miss.
        let data = ConfigData {
            routes: vec![RouteRule::new(
                "m",
                RouteKind::Main,
                vec![PathPred::new("/only/this")],
                vec![Destination::new("a.static:80")],
            )
            .unwrap()],
            ..Default::default()
        };
        let t = RouteTable::rebuild(&data).unwrap();
        assert!(match_route_by_key(&t, None, "/elsewhere").is_none());
        assert!(match_route_by_key(&t, Some("m"), "/elsewhere").is_none());
        // ... but header + matching path still matches.
        let ok = match_route_by_key(&t, Some("m"), "/only/this").unwrap();
        assert_eq!(ok.index, 0);
        assert_eq!(ok.matched_by, MatchKind::HeaderExact);
    }

    #[test]
    fn case_insensitive_path_within_matched_route() {
        // Case-insensitivity lives on the predicate of the *already-matched*
        // route (chosen via the header), not in cross-route selection.
        let data = ConfigData {
            routes: vec![
                RouteRule::new(
                    "m",
                    RouteKind::Main,
                    vec![PathPred::new("/(v1)()(/chat/completions)").ignore_case()],
                    vec![Destination::new("a.static:80")],
                )
                .unwrap(),
                RouteRule::new(
                    "gpustack",
                    RouteKind::Mirror,
                    vec![PathPred::new("/")],
                    vec![Destination::new("gpustack.dns:30080")],
                )
                .unwrap(),
            ],
            ..Default::default()
        };
        let t = RouteTable::rebuild(&data).unwrap();
        // Header selects the route; the (ignore-case) predicate matches.
        let m = match_route_by_key(&t, Some("m"), "/V1/CHAT/COMPLETIONS").unwrap();
        assert_eq!(m.index, 0);
        // A case-SENSITIVE predicate would not match the same path -> mirror.
        let data2 = ConfigData {
            routes: vec![
                RouteRule::new(
                    "m",
                    RouteKind::Main,
                    vec![PathPred::new("/(v1)()(/Chat/Completions)")],
                    vec![Destination::new("a.static:80")],
                )
                .unwrap(),
                RouteRule::new(
                    "gpustack",
                    RouteKind::Mirror,
                    vec![PathPred::new("/")],
                    vec![Destination::new("gpustack.dns:30080")],
                )
                .unwrap(),
            ],
            ..Default::default()
        };
        let t2 = RouteTable::rebuild(&data2).unwrap();
        assert_eq!(
            match_route_by_key(&t2, Some("m"), "/v1/chat/completions").unwrap().matched_by,
            MatchKind::Mirror
        );
    }

    #[test]
    fn header_map_entry_points() {
        let t = table();
        // Initial entry: header present and path matches the named route.
        let mut h = HeaderMap::new();
        h.insert(LLM_MODEL_HEADER, "claude");
        let m = match_route(&t, &h, "/v1/messages").unwrap();
        assert_eq!(m.index, 1);
        assert_eq!(m.matched_by, MatchKind::HeaderExact);

        // Initial entry: header absent -> mirror.
        let h2 = HeaderMap::new();
        let m2 = match_route(&t, &h2, "/v1/messages").unwrap();
        assert_eq!(m2.matched_by, MatchKind::Mirror);

        // Fallback entry: only x-higress-fallback-from is set.
        let mut h3 = HeaderMap::new();
        h3.insert(FALLBACK_FROM_HEADER, "ai-route-route-1.internal");
        let m3 = match_fallback_route(&t, &h3, "/v1/chat/completions").unwrap();
        assert_eq!(m3.index, 2);
        assert_eq!(m3.matched_by, MatchKind::FallbackExact);
    }

    #[test]
    fn within_route_longest_anchor_predicate_wins() {
        // Two predicates in one route: both match; the longest literal anchor
        // is chosen for rewrite capture (still a single header-matched route).
        let data = ConfigData {
            routes: vec![RouteRule::new(
                "m",
                RouteKind::Main,
                vec![
                    PathPred::new("/v1(/|$)(.*)"),
                    PathPred::new("/v1/embeddings(/|$)(.*)"),
                ],
                vec![Destination::new("a.static:80")],
            )
            .unwrap()],
            ..Default::default()
        };
        let t = RouteTable::rebuild(&data).unwrap();
        let m = match_route_by_key(&t, Some("m"), "/v1/embeddings/x").unwrap();
        assert_eq!(m.index, 0);
        assert_eq!(m.matched_by, MatchKind::HeaderExact);
        // The longer-anchored predicate (index 1) is recorded.
        assert_eq!(m.matched_predicate, Some(1));
    }

    #[test]
    fn matched_predicate_recorded() {
        let data = ConfigData {
            routes: vec![RouteRule::new(
                "m",
                RouteKind::Main,
                vec![PathPred::new("/a(/|$)(.*)"), PathPred::new("/b(/|$)(.*)")],
                vec![Destination::new("a.static:80")],
            )
            .unwrap()],
            ..Default::default()
        };
        let t = RouteTable::rebuild(&data).unwrap();
        let m = match_route_by_key(&t, Some("m"), "/b/x").unwrap();
        assert_eq!(m.matched_predicate, Some(1));
    }
}
