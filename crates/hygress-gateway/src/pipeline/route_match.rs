//! ④ route match — pure wrappers over the core `RouteTable` index.
//!
//! - **Initial request**: [`match_initial`] matches a **Main** route by exact
//!   `x-higress-llm-model` (AND its full-match path predicate), else the **mirror**
//!   catch-all. A Fallback route is never selectable here (separate key space).
//! - **Fallback redirect** (stage ⑭): [`match_fallback`] matches a **Fallback**
//!   route by exact `x-higress-fallback-from`; a Main route is never selectable
//!   here.
//!
//! [`capture_groups`] re-derives the matched predicate's capture groups (for a
//! `rewrite-target` such as `/$1$3`). It must mirror the core's exact compiled
//! shape — `^(?:<regex>)$` with the predicate's `ignore_case` — so the groups
//! line up with what [`RouteTable`] matched on.

use hygress_core::prelude::{HeaderMap, PathPred, RouteMatch, RouteTable};

/// Match an **initial** request (exact header + full-match path, else mirror).
pub fn match_initial(table: &RouteTable, headers: &HeaderMap, path: &str) -> Option<RouteMatch> {
    // Reads `x-higress-llm-model` from `headers` and matches a Main route.
    table.find_match(headers.get("x-higress-llm-model"), path)
}

/// Match a **fallback redirect** (by `x-higress-fallback-from`, else mirror).
pub fn match_fallback(table: &RouteTable, headers: &HeaderMap, path: &str) -> Option<RouteMatch> {
    table.find_match_fallback(headers.get("x-higress-fallback-from"), path)
}

/// The 1-based capture groups of `pred` full-matched on `path` (for rewrite).
///
/// Returns an empty vec when the predicate is not a valid regex or does not
/// full-match (the caller treats that as "no rewrite").
pub fn capture_groups(pred: &PathPred, path: &str) -> Vec<String> {
    let pattern = format!("^(?:{})$", pred.regex);
    let Ok(re) = regex::RegexBuilder::new(&pattern)
        .case_insensitive(pred.ignore_case)
        .build()
    else {
        return Vec::new();
    };
    let Some(caps) = re.captures(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    // Skip group 0 (the whole match).
    for g in caps.iter().skip(1) {
        out.push(g.map(|s| s.as_str().to_string()).unwrap_or_default());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::prelude::{ConfigData, Destination, PathPred, RouteKind, RouteRule};

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
                    "ai-route-route-1.internal",
                    RouteKind::Fallback,
                    vec![PathPred::new("/(v1)()(/chat/completions)")],
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

    #[test]
    fn initial_exact_header_and_path() {
        let t = table();
        let m = match_initial(&t, &h_llm("gpt-4o"), "/v1/chat/completions").unwrap();
        assert_eq!(m.matched_by, hygress_core::MatchKind::HeaderExact);
        assert_eq!(m.matched_predicate, Some(0));
    }

    #[test]
    fn initial_no_header_goes_to_mirror() {
        let t = table();
        let m = match_initial(&t, &HeaderMap::new(), "/v1/chat/completions").unwrap();
        assert_eq!(m.matched_by, hygress_core::MatchKind::Mirror);
    }

    #[test]
    fn initial_header_but_nonmatching_path_goes_to_mirror() {
        let t = table();
        let m = match_initial(&t, &h_llm("gpt-4o"), "/v1/messages").unwrap();
        assert_eq!(m.matched_by, hygress_core::MatchKind::Mirror);
    }

    #[test]
    fn fallback_only_via_fallback_from() {
        let t = table();
        let m = match_fallback(&t, &h_fallback("ai-route-route-1.internal"), "/v1/chat/completions")
            .unwrap();
        assert_eq!(m.matched_by, hygress_core::MatchKind::FallbackExact);
    }

    #[test]
    fn capture_groups_rewrite_substitution() {
        // GPUStack /model/proxy form: pattern /()model/proxy/\d+(/|$)(.*) → /$1$3.
        let pred = PathPred::new("/()model/proxy/\\d+(/|$)(.*)");
        let groups = capture_groups(&pred, "/model/proxy/5/chat/completions");
        assert_eq!(groups, vec!["", "/", "chat/completions"]);
        // Missing group renders empty.
        let g2 = capture_groups(&pred, "/model/proxy/5");
        assert_eq!(g2, vec!["", "", ""]);
    }

    #[test]
    fn capture_groups_empty_on_nomatch() {
        let pred = PathPred::new("/only/this");
        assert_eq!(capture_groups(&pred, "/elsewhere"), Vec::<String>::new());
    }

    fn h_llm(v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-higress-llm-model", v);
        h
    }

    fn h_fallback(v: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-higress-fallback-from", v);
        h
    }
}
