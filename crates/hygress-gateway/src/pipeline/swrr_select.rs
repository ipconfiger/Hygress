//! ⑦ SWRR weighted selection — pure. Orders a route's destination candidates
//! for one request using the **shared** per-route-group SWRR state from the
//! core `SharedConfig` (so the weighted sequence stays smooth and Nginx-
//! deterministic across workers, design §6.2).
//!
//! The candidate id is the destination `name.type:port` (stable across config
//! re-indexing); the group key is `(route key, digest of sorted service ids)` —
//! exactly the core's route-group identity.

use hygress_core::prelude::{Destination, RouteRule, RouteTable, SwrrCandidate};

use crate::context::SharedConfigHandle;

/// Order `route`'s destinations for one request via the shared SWRR state.
///
/// Returns the destinations in SWRR order (selected = index 0). Zero-weight
/// destinations are retained (they are only reachable via the failover walk).
///
/// This build-from-scratch form is kept for direct callers / tests; the pipe
/// uses [`order_route`], which borrows the route table's precomputed SWRR group
/// key + candidate vec (M7).
pub fn order(config: &SharedConfigHandle, route: &RouteRule) -> Vec<Destination> {
    // Guard: no destinations (should not happen — validated), nothing to order.
    if route.destinations.is_empty() {
        return Vec::new();
    }
    let services: Vec<String> = route.destinations.iter().map(|d| d.service.clone()).collect();

    let mut candidates: Vec<SwrrCandidate> = route
        .destinations
        .iter()
        .map(|d| SwrrCandidate::new(d.service.clone(), d.weight() as i32))
        .collect();

    // One deterministic SWRR round over the shared per-group state.
    let mut guard = config.swrr_group_state(&route.key, &services).or_default();
    hygress_core::swrr_order(&mut candidates, &mut guard);

    // Map candidates back to their destination entries (by stable service id).
    candidates
        .iter()
        .filter_map(|c| {
            route
                .destinations
                .iter()
                .find(|d| d.service == c.id)
                .cloned()
        })
        .collect()
}

/// Order candidates for the route at `index` in `table` (convenience for the
/// pipe).
///
/// M7: the SWRR group key + candidate vec were precomputed into `table` at
/// snapshot build time, and only the tiny per-group weight map lives in the
/// `DashMap` — the per-request path no longer rebuilds the `Vec<String>` /
/// `Vec<SwrrCandidate>` scratch, no longer recomputes the sorted-service FNV
/// digest, and only holds the shard lock for the ordering round itself (an
/// `O(1)` map-back via the precomputed destination index replaces the O(n²)
/// `find`).
pub fn order_route(
    config: &SharedConfigHandle,
    table: &RouteTable,
    index: usize,
) -> Vec<Destination> {
    let route = table.route(index);
    // Guard: no destinations (should not happen — validated), nothing to order.
    if route.destinations.is_empty() {
        return Vec::new();
    }
    // R-7: a single-destination route has nothing to weight — return it without
    // touching the shared SWRR state (no DashMap shard lock on the hot path;
    // concentrated single-instance routes are the GPUStack norm).
    if route.destinations.len() == 1 {
        return vec![route.destinations[0].clone()];
    }
    // The SWRR round reorders its own operating copy of the (small) precomputed
    // candidate vec.
    let mut candidates = table.swrr_candidates(index).to_vec();
    let key = table.swrr_group_key(index);

    // One deterministic SWRR round over the shared per-group state. The group is
    // created on first sight; afterwards only the existing (tiny) weight map is
    // mutated under the shard lock (`get_mut` — no key rebuild).
    match config.swrr_group_state_mut(key) {
        Some(mut guard) => hygress_core::swrr_order(&mut candidates, &mut guard),
        None => {
            let mut guard = config.swrr_group_state_entry(key.clone()).or_default();
            hygress_core::swrr_order(&mut candidates, &mut guard);
        }
    }

    // Map candidates back to their destination entries via the precomputed
    // per-route index (O(1) per candidate).
    candidates
        .iter()
        .filter_map(|c| table.destination_for_service(index, &c.id).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::prelude::{ConfigData, RouteKind, SharedConfig};
    use hygress_core::route::PathPred;

    fn share(data: ConfigData) -> SharedConfigHandle {
        let sc = SharedConfig::new(data).unwrap();
        SharedConfigHandle::new(sc)
    }

    #[test]
    fn weighted_seven_pick_sequence() {
        // Weights 5/1/1 → the classic Nginx smooth 7-pick sequence.
        let route = RouteRule::new(
            "m1",
            RouteKind::Main,
            vec![PathPred::new("/(v1)()(/chat/completions)")],
            vec![
                Destination::with_percent(5, "a.static:80"),
                Destination::with_percent(1, "b.static:80"),
                Destination::with_percent(1, "c.static:80"),
            ],
        )
        .unwrap();
        let cfg = share(ConfigData {
            routes: vec![route.clone()],
            ..Default::default()
        });
        let mut seq = Vec::new();
        for _ in 0..7 {
            seq.push(order(&cfg, &route)[0].service.clone());
        }
        assert_eq!(
            seq,
            vec![
                "a.static:80",
                "a.static:80",
                "b.static:80",
                "a.static:80",
                "c.static:80",
                "a.static:80",
                "a.static:80"
            ]
        );
    }

    #[test]
    fn single_destination_always_first() {
        let route = RouteRule::new(
            "m",
            RouteKind::Main,
            vec![PathPred::new("/(v1)()(/chat/completions)")],
            vec![Destination::new("only.static:80")],
        )
        .unwrap();
        let cfg = share(ConfigData { routes: vec![route.clone()], ..Default::default() });
        assert_eq!(order(&cfg, &route)[0].service, "only.static:80");
    }

    #[test]
    fn state_is_shared_across_calls() {
        // The same (route, dest-group) shares one SWRR state across calls: two
        // independent `order` calls continue the same running sequence.
        let route = RouteRule::new(
            "m",
            RouteKind::Main,
            vec![PathPred::new("/(v1)()(/chat/completions)")],
            vec![
                Destination::with_percent(5, "a.static:80"),
                Destination::with_percent(1, "b.static:80"),
            ],
        )
        .unwrap();
        let cfg = share(ConfigData { routes: vec![route.clone()], ..Default::default() });
        // 5 picks of a 5/1 pair → Nginx smooth sequence: a a a b a.
        let mut s = Vec::new();
        for _ in 0..5 {
            s.push(order(&cfg, &route)[0].service.clone());
        }
        assert_eq!(s, vec!["a.static:80", "a.static:80", "a.static:80", "b.static:80", "a.static:80"]);
    }

    #[test]
    fn different_route_is_different_group() {
        let r1 = RouteRule::new(
            "r1",
            RouteKind::Main,
            vec![PathPred::new("/")],
            vec![Destination::new("a.static:80")],
        )
        .unwrap();
        let r2 = RouteRule::new(
            "r2",
            RouteKind::Main,
            vec![PathPred::new("/")],
            vec![Destination::new("a.static:80")],
        )
        .unwrap();
        let cfg = share(ConfigData { routes: vec![r1.clone(), r2.clone()], ..Default::default() });
        // Distinct route keys → distinct groups (no bleed).
        assert!(cfg.swrr_group_state_ref("r1", &["a.static:80".to_string()]).is_none());
        order(&cfg, &r1);
        order(&cfg, &r2);
        assert!(cfg.swrr_group_state_ref("r1", &["a.static:80".to_string()]).is_some());
        assert!(cfg.swrr_group_state_ref("r2", &["a.static:80".to_string()]).is_some());
    }
}
