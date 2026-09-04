//! Smooth Weighted Round-Robin — Nginx-compatible weighted selection (pure).
//!
//! Ported from dogress2 `hydra-core::swrr`, re-keyed for Hygress usage: the
//! candidates are per-route destination group entries, and the caller owns one
//! [`SwrrState`] per `(route, destination-group)`.
//!
//! ## Purity
//!
//! No I/O, no time, no global state. The caller passes the state by `&mut`;
//! [`order`] performs exactly one deterministic SWRR round.
//!
//! ## Algorithm (Nginx SWRR invariants)
//!
//! For one selection:
//! 1. for each candidate `i`: `current_weight[i] += weight[i]`;
//! 2. `total = Σ weight[i]`;
//! 3. pick the candidate with the **max** `current_weight` (ties broken by
//!    candidate position — first wins, matching Nginx);
//! 4. `current_weight[picked] -= total`;
//! 5. the picked candidate is rotated to the front of the slice (the rest
//!    keep their relative order, so failover can walk the tail by cursor).
//!
//! [`order`] is a no-op on an empty slice and guards `total <= 0` to stay
//! panic-free with degenerate input.

use std::collections::HashMap;

/// A selectable destination candidate.
///
/// `id` should be the destination's stable service identity
/// (``name.type:port``) so state survives config re-indexing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwrrCandidate {
    /// Stable candidate identity (e.g. `model-1-10.static:80`).
    pub id: String,
    /// Weight (from the destination percent). `0` means "selectable only via
    /// failover walk" — the caller is expected to filter `weight > 0` before
    /// calling [`order`]; the guard in [`order`] simply tolerates the
    /// degenerate all-zero set.
    pub weight: i32,
}

impl SwrrCandidate {
    pub fn new(id: impl Into<String>, weight: i32) -> Self {
        Self {
            id: id.into(),
            weight,
        }
    }
}

/// Per-`(route, destination-group)` running SWRR state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SwrrState {
    /// `candidate id` → running `current_weight`.
    pub current_weights: HashMap<String, i32>,
}

/// Perform one Nginx Smooth Weighted Round-Robin step.
///
/// Mutates `state` and reorders `candidates` so the selected candidate is
/// first, preserving the relative order of the remaining candidates (so a
/// single request's failover can walk the tail by incrementing a cursor).
///
/// The caller owns `state`; it must **not** call [`order`] again for the same
/// request (failover advances the cursor instead of re-ordering).
pub fn order(candidates: &mut [SwrrCandidate], state: &mut SwrrState) {
    let total: i32 = candidates.iter().map(|c| c.weight).sum();
    if total <= 0 {
        // Nothing selectable (callers supply weight > 0; guard for direct
        // callers with degenerate input).
        return;
    }

    // Step 1: advance every candidate's running current_weight by its weight.
    // Candidates unseen by the state default to 0.
    for c in candidates.iter() {
        let entry = state.current_weights.entry(c.id.clone()).or_insert(0);
        *entry += c.weight;
    }

    // Steps 2–3: pick the max current_weight, first wins on ties (Nginx).
    let mut picked = 0usize;
    for i in 1..candidates.len() {
        let picked_i = state
            .current_weights
            .get(&candidates[i].id)
            .copied()
            .unwrap_or(0);
        let picked_p = state
            .current_weights
            .get(&candidates[picked].id)
            .copied()
            .unwrap_or(0);
        if picked_i > picked_p {
            picked = i;
        }
    }

    // Step 4: subtract total from the picked candidate.
    if let Some(cw) = state.current_weights.get_mut(&candidates[picked].id) {
        *cw -= total;
    }

    // Step 5: rotate the picked candidate to the front, preserving the
    // relative order of the rest. `[a, b, PICK, c]` -> `[PICK, a, b, c]`.
    candidates[..=picked].rotate_right(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Run `n` selection rounds over a candidate set and return, in order, the
    /// selected candidate ids.
    fn pick_sequence(weights: &[(i32, i32)], n: usize) -> Vec<String> {
        let mut candidates: Vec<SwrrCandidate> = weights
            .iter()
            .map(|(id_suffix, w)| SwrrCandidate::new(format!("c{id_suffix}"), *w))
            .collect();
        let mut state = SwrrState::default();
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            order(&mut candidates, &mut state);
            out.push(candidates[0].id.clone());
        }
        out
    }

    fn count_ids(seq: &[String]) -> HashMap<String, usize> {
        let mut m = HashMap::new();
        for id in seq {
            *m.entry(id.clone()).or_insert(0) += 1;
        }
        m
    }

    #[test]
    fn deterministic_spread_across_weights_5_1_1() {
        // 7 rounds, weights (5,1,1): each candidate gets exactly its weight.
        let seq = pick_sequence(&[(1, 5), (2, 1), (3, 1)], 7);
        let counts = count_ids(&seq);
        assert_eq!(counts.get("c1"), Some(&5));
        assert_eq!(counts.get("c2"), Some(&1));
        assert_eq!(counts.get("c3"), Some(&1));
    }

    #[test]
    fn smooth_sequence_shape_matches_nginx() {
        // Classic Nginx 5/1/1 smooth sequence (state starts empty):
        // c1 c1 c2 c1 c3 c1 c1
        let seq = pick_sequence(&[(1, 5), (2, 1), (3, 1)], 7);
        assert_eq!(
            seq,
            vec!["c1", "c1", "c2", "c1", "c3", "c1", "c1"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn equal_weights_sequence() {
        // Nginx tie-break after rotation: 1:1 yields a,b,b,a per 4-pick
        // block (first-in-array wins ties; the array is re-ordered every
        // pick). The *distribution* is still exact.
        let seq = pick_sequence(&[(1, 1), (2, 1)], 4);
        let counts = count_ids(&seq);
        assert_eq!(counts.get("c1"), Some(&2));
        assert_eq!(counts.get("c2"), Some(&2));
        // The Nginx-deterministic shape of the first 4 picks.
        assert_eq!(
            seq,
            vec!["c1", "c2", "c2", "c1"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn tie_broken_by_position_first_wins() {
        // Two candidates with the same current_weight: the earlier position
        // in the slice wins (Nginx tie-break).
        let mut candidates = vec![
            SwrrCandidate::new("a", 1),
            SwrrCandidate::new("b", 1),
            SwrrCandidate::new("c", 0),
        ];
        let mut state = SwrrState::default();
        order(&mut candidates, &mut state);
        assert_eq!(candidates[0].id, "a");
        // Tail keeps its relative order (c's zero weight stays last-ish).
        assert_eq!(candidates[1].id, "b");
    }

    #[test]
    fn picked_candidate_rotates_to_front() {
        let mut candidates = vec![
            SwrrCandidate::new("a", 1),
            SwrrCandidate::new("b", 3),
            SwrrCandidate::new("c", 1),
        ];
        let mut state = SwrrState::default();
        order(&mut candidates, &mut state);
        // b has the highest first-round current weight.
        assert_eq!(candidates[0].id, "b");
        // Relative order of the rest preserved: a before c.
        assert_eq!(
            &candidates[1..3],
            &[SwrrCandidate::new("a", 1), SwrrCandidate::new("c", 1),]
        );
    }

    #[test]
    fn empty_slice_is_noop() {
        let mut candidates: Vec<SwrrCandidate> = Vec::new();
        let mut state = SwrrState::default();
        order(&mut candidates, &mut state);
        assert!(candidates.is_empty());
        assert!(state.current_weights.is_empty());
    }

    #[test]
    fn all_zero_weights_is_noop() {
        let mut candidates = vec![SwrrCandidate::new("a", 0), SwrrCandidate::new("b", 0)];
        let mut state = SwrrState::default();
        order(&mut candidates, &mut state);
        // No division, no reordering, no state mutation.
        assert_eq!(candidates[0].id, "a");
        assert!(state.current_weights.is_empty());
    }

    #[test]
    fn state_survives_across_calls_and_is_deterministic() {
        // Two independent runs with identical inputs produce identical
        // sequences (pure function of inputs); 12 picks = 2 full SWRR
        // cycles of 6 for weights 3/2/1.
        let a = pick_sequence(&[(1, 3), (2, 2), (3, 1)], 12);
        let b = pick_sequence(&[(1, 3), (2, 2), (3, 1)], 12);
        assert_eq!(a, b);
        let counts = count_ids(&a);
        assert_eq!(counts.get("c1"), Some(&6));
        assert_eq!(counts.get("c2"), Some(&4));
        assert_eq!(counts.get("c3"), Some(&2));
    }

    #[test]
    fn candidates_appear_later_start_from_zero() {
        // A candidate that appears in a later round starts at current_weight 0.
        let mut candidates = vec![SwrrCandidate::new("a", 1), SwrrCandidate::new("b", 1)];
        let mut state = SwrrState::default();
        order(&mut candidates, &mut state); // picks a
        assert_eq!(candidates[0].id, "a");
        // Insert 'c': failover-style set growth (new candidate starts at 0).
        candidates.insert(1, SwrrCandidate::new("c", 1));
        order(&mut candidates, &mut state);
        // a: (1+1)-2=-... recompute: after round1 a cw=0 (1+1-2), b cw=1.
        // round2: a=0+1=1, b=1+1=2, c=0+1=1 -> b wins.
        assert_eq!(candidates[0].id, "b");
    }
}
