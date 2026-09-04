//! Routing-policy action queries + service-name pattern matching (design
//! §4.3 / D-2 / D-12).
//!
//! The pure core provides the **selection** logic the gateway applies at the
//! `routing_policy` stage (after `route_match`, on the matched Main route):
//!
//! - [`pin_matches`] — filter/pin candidate services by a `name.type` glob
//!   (e.g. `provider-8.*`). There is no "region" dimension in the data model
//!   (D-2): the `Registry` only carries `id/kind/domain/port/proxy_ref`, so
//!   pinning is purely by service-name pattern.
//! - [`RoutePolicyActions::pick_override`] — select the `override_route` target
//!   from the resolved candidates. The override is a `name.type:port` that must
//!   **exist** among the candidates; when it does not, `None` is returned so the
//!   gateway falls back to the original routing at runtime (D-2: no load-time
//!   cross-slot validation — the policy and CRD slots are independent ArcSwaps).
//!
//! `header_add` / `header_del` / `timeout_ms` / `retries` are pure data the
//! gateway applies; the core does not mutate headers here.

use crate::policy::{glob_match, RoutePolicyActions};

/// Glob-match a provider **service-name pattern** (e.g. `provider-8.*`) against
/// a service `name.type` (e.g. `provider-8.proxy`). `*` matches any (possibly
/// empty) sequence; all other characters are literal (D-2).
pub fn pin_matches(pattern: &str, service_name: &str) -> bool {
    glob_match(pattern, service_name)
}

impl RoutePolicyActions {
    /// Select the `override_route` target from the resolved candidate services.
    ///
    /// `services` are the candidate `name.type:port` ids. When
    /// [`RoutePolicyActions::override_route`] is set and **exactly equals** one
    /// of `services`, that candidate is returned (the gateway replaces
    /// `prepared.candidates` with it, skipping SWRR order). When it is unset or
    /// matches no candidate, `None` is returned so the gateway falls back to the
    /// original routing at runtime (D-2).
    pub fn pick_override<'a>(&self, services: &[&'a str]) -> Option<&'a str> {
        let target = self.override_route.as_deref()?;
        services.iter().copied().find(|s| *s == target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- pin_matches -----

    #[test]
    fn pin_prefix_wildcard() {
        assert!(pin_matches("provider-8.*", "provider-8.proxy"));
        assert!(pin_matches("provider-8.*", "provider-8.dns"));
        assert!(pin_matches("provider-8.*", "provider-8.static"));
        assert!(!pin_matches("provider-8.*", "provider-9.proxy"));
        assert!(!pin_matches("provider-8.*", "provider-80.proxy"));
    }

    #[test]
    fn pin_exact() {
        assert!(pin_matches("provider-1.proxy", "provider-1.proxy"));
        assert!(!pin_matches("provider-1.proxy", "provider-1.dns"));
    }

    #[test]
    fn pin_star_matches_any() {
        assert!(pin_matches("*", "model-1-10.static"));
        assert!(pin_matches("*", ""));
    }

    #[test]
    fn pin_no_match() {
        assert!(!pin_matches("model-.*", "provider-1.proxy"));
    }

    // ----- pick_override -----

    #[test]
    fn override_hit_selects_candidate() {
        let a = RoutePolicyActions {
            override_route: Some("model-8-6.static:80".to_string()),
            ..Default::default()
        };
        let services = [
            "model-8-6.static:80",
            "model-9-9.static:80",
            "provider-1.proxy:443",
        ];
        assert_eq!(a.pick_override(&services), Some("model-8-6.static:80"));
    }

    #[test]
    fn override_miss_returns_none_for_fallback() {
        // The override target is not among the candidates -> None (the gateway
        // falls back to the original routing at runtime, D-2).
        let a = RoutePolicyActions {
            override_route: Some("model-8-6.static:80".to_string()),
            ..Default::default()
        };
        let services = ["model-9-9.static:80", "provider-1.proxy:443"];
        assert_eq!(a.pick_override(&services), None);
    }

    #[test]
    fn override_unset_returns_none() {
        let a = RoutePolicyActions::default();
        let services = ["model-8-6.static:80"];
        assert_eq!(a.pick_override(&services), None);
    }

    #[test]
    fn override_empty_candidates_returns_none() {
        let a = RoutePolicyActions {
            override_route: Some("model-8-6.static:80".to_string()),
            ..Default::default()
        };
        assert_eq!(a.pick_override(&[]), None);
    }
}
