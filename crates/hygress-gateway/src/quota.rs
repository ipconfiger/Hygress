//! Token-quota reservation lifecycle (design §4.2 / D-11 / D-13).
//!
//! [`QuotaReservation`] is the **RAII Drop guard** that keeps the
//! two-phase `reserve → commit / release` lifecycle correct on **every**
//! terminal path (the design's terminal matrix, D-11):
//!
//! | terminal path                        | settlement                |
//! |--------------------------------------|---------------------------|
//! | 2xx stream completes                 | `settle(Some(actual))`    |
//! | terminal non-2xx (`report_incomplete_usage`) | guard `Drop` → release |
//! | all candidates transport-failed      | guard `Drop` → release    |
//! | downstream write failed mid-stream   | `settle(None)` + `Drop` (idempotent) |
//! | guardrail cut (in / out)             | `settle(None)` + `Drop` (idempotent) |
//!
//! The guard is created **only** for a request whose [`QuotaEngine::reserve`]
//! returned `Allowed` / `SoftExceed` (a `HardDeny` is a 429 short-circuit with
//! no reservation). Exactly one reservation exists per request (the initial
//! dispatch, `redirect_count == 0`); when the fallback re-dispatch `continue`s
//! to hop 1 the hop-0 guard drops (releasing its estimate) and hop 1 runs
//! without a quota (D-3).
//!
//! Settlement is **idempotent**: an explicit [`QuotaReservation::settle`]
//! marks the guard settled, so the `Drop` is a no-op; an unsettled drop
//! releases the (still in-flight) estimate so no budget leaks (the TTL `gc_stale`
//! is the process-level backstop).

use std::sync::Arc;

use hygress_core::prelude::{LimitWindowSpec, QuotaEngine};

/// A live token-quota reservation for one request (settle on every terminal
/// path; see the module docs for the matrix).
pub struct QuotaReservation {
    engine: Arc<QuotaEngine>,
    /// The `(consumer, model)` budget key (the pre-mapping effective model,
    /// the same value the usage report uses — design §4.2 / BLOCK-3).
    key: (String, String),
    spec: LimitWindowSpec,
    /// The window index at reservation time (the `release` target, D-11).
    widx: u64,
    /// The reserved estimate (`ceil(request_content_bytes / K)`, D-13).
    est: u64,
    /// `true` once [`QuotaReservation::settle`] ran (the `Drop` is a no-op).
    settled: bool,
}

impl QuotaReservation {
    /// Wrap a successful `reserve` (`Allowed` / `SoftExceed`) in the guard.
    pub fn new(
        engine: Arc<QuotaEngine>,
        consumer: String,
        model: String,
        spec: LimitWindowSpec,
        now_ms: u64,
        est: u64,
    ) -> Self {
        let window_ms = spec.window_secs.saturating_mul(1000);
        // A zero-length window (window_ms == 0) is a single infinite window
        // (index 0) — the same formula the core engine uses.
        let widx = now_ms.checked_div(window_ms).unwrap_or(0);
        Self {
            engine,
            key: (consumer, model),
            spec,
            widx,
            est,
            settled: false,
        }
    }

    /// Settle the reservation exactly once:
    /// - `Some(actual)` — the request completed 2xx: **commit** (replace the
    ///   estimate with the actual `total_token`);
    /// - `None` — the request aborted: **release** (the estimate is returned;
    ///   nothing is committed for the abort).
    ///
    /// A second call is a no-op (idempotent), so the `Drop` never double-settles.
    pub fn settle(&mut self, actual: Option<u64>) {
        if self.settled {
            return;
        }
        self.settled = true;
        let now = now_millis();
        match actual {
            Some(actual) => {
                self.engine
                    .commit(now, &self.key.0, &self.key.1, &self.spec, self.est, actual);
            }
            None => {
                self.engine.release(self.widx, &self.key, self.est, 0);
            }
        }
    }

    /// `true` once settled (an explicit `settle` already ran).
    pub fn is_settled(&self) -> bool {
        self.settled
    }

    /// The reserved estimate (for diagnostics/tests).
    pub fn est(&self) -> u64 {
        self.est
    }
}

impl Drop for QuotaReservation {
    /// An unsettled reservation (any early return the guard did not explicitly
    /// settle) releases the in-flight estimate — no budget leak (D-11).
    fn drop(&mut self) {
        if !self.settled {
            self.engine.release(self.widx, &self.key, self.est, 0);
        }
    }
}

/// Unix millis since the epoch (settle-time window computation).
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hygress_core::prelude::QuotaDecision;

    const BIG_WINDOW_SECS: u64 = 10_000_000_000;

    fn spec(window_secs: u64, hard: Option<u64>) -> LimitWindowSpec {
        LimitWindowSpec {
            window_secs,
            soft: None,
            hard,
        }
    }

    // ----- commit (2xx stream completed) -----

    #[test]
    fn settle_some_commits_actual_replacing_estimate() {
        let engine = Arc::new(QuotaEngine::new());
        let s = spec(BIG_WINDOW_SECS, Some(1000));
        assert_eq!(engine.reserve(0, "c", "m", &s, 20), QuotaDecision::Allowed);
        let mut g = QuotaReservation::new(engine.clone(), "c".into(), "m".into(), s, 0, 20);
        g.settle(Some(85)); // the upstream `total_token`
        assert_eq!(engine.usage("c", "m"), Some(85));
        assert!(g.is_settled());
    }

    #[test]
    fn settle_some_actual_less_than_estimate_returns_difference() {
        let engine = Arc::new(QuotaEngine::new());
        let s = spec(BIG_WINDOW_SECS, Some(1000));
        engine.reserve(0, "c", "m", &s, 50);
        let mut g = QuotaReservation::new(engine.clone(), "c".into(), "m".into(), s, 0, 50);
        g.settle(Some(10));
        assert_eq!(engine.usage("c", "m"), Some(10));
    }

    // ----- release (abort paths) -----

    #[test]
    fn settle_none_releases_estimate() {
        let engine = Arc::new(QuotaEngine::new());
        let s = spec(BIG_WINDOW_SECS, Some(1000));
        engine.reserve(0, "c", "m", &s, 30);
        assert_eq!(engine.usage("c", "m"), Some(30));
        let mut g = QuotaReservation::new(engine.clone(), "c".into(), "m".into(), s, 0, 30);
        g.settle(None); // e.g. guardrail cut / downstream write fail
        assert_eq!(engine.usage("c", "m"), Some(0));
        assert!(g.is_settled());
    }

    #[test]
    fn drop_without_settle_releases() {
        let engine = Arc::new(QuotaEngine::new());
        let s = spec(BIG_WINDOW_SECS, Some(1000));
        engine.reserve(0, "c", "m", &s, 40);
        {
            let _g = QuotaReservation::new(engine.clone(), "c".into(), "m".into(), s, 0, 40);
            // dropped unsettled → the in-flight estimate is released.
        }
        assert_eq!(engine.usage("c", "m"), Some(0));
    }

    #[test]
    fn settle_is_idempotent() {
        let engine = Arc::new(QuotaEngine::new());
        let s = spec(BIG_WINDOW_SECS, Some(1000));
        engine.reserve(0, "c", "m", &s, 20);
        let mut g = QuotaReservation::new(engine.clone(), "c".into(), "m".into(), s, 0, 20);
        g.settle(Some(85));
        // A second settle (the Drop path) must not touch the budget again.
        g.settle(None);
        assert_eq!(engine.usage("c", "m"), Some(85));
        drop(g);
        assert_eq!(engine.usage("c", "m"), Some(85));
    }

    // ----- concurrency: per-reservation settle (D-11) -----

    #[test]
    fn concurrent_reservations_settle_independently() {
        let engine = Arc::new(QuotaEngine::new());
        let s = spec(BIG_WINDOW_SECS, Some(1000));
        engine.reserve(0, "c", "m", &s, 300);
        engine.reserve(0, "c", "m", &s, 200);
        let mut g1 = QuotaReservation::new(engine.clone(), "c".into(), "m".into(), s.clone(), 0, 300);
        let mut g2 = QuotaReservation::new(engine.clone(), "c".into(), "m".into(), s, 0, 200);
        // Request 1 commits 250 ...
        g1.settle(Some(250));
        assert_eq!(engine.usage("c", "m"), Some(450));
        // ... request 2 aborts (releases its 200).
        g2.settle(None);
        assert_eq!(engine.usage("c", "m"), Some(250));
    }

    // ----- window index (zero window = infinite) -----

    #[test]
    fn zero_window_is_single_infinite_window() {
        let engine = Arc::new(QuotaEngine::new());
        let s = spec(0, Some(100));
        // A zero-length window is a single infinite window: a reserve 10 days
        // later sees the same budget (60 used; 60+60 > 100 → deny).
        assert_eq!(engine.reserve(0, "c", "m", &s, 60), QuotaDecision::Allowed);
        let late = 10 * 24 * 3600 * 1000u64;
        assert_eq!(engine.reserve(late, "c", "m", &s, 60), QuotaDecision::HardDeny);
        // The guard built at the late timestamp still targets window 0, so its
        // release settles against the same counter.
        let mut g = QuotaReservation::new(engine.clone(), "c".into(), "m".into(), s, late, 40);
        g.settle(None);
        assert_eq!(engine.usage("c", "m"), Some(20));
    }

    // ----- estimate helper parity (D-13) -----

    #[test]
    fn est_key_shape_matches_engine_usage() {
        // The guard's `(consumer, model)` key must index the same engine
        // counter the reserve/commit path used (usage reflects the settle).
        let engine = Arc::new(QuotaEngine::new());
        let s = spec(BIG_WINDOW_SECS, None);
        engine.reserve(0, "ak.gpustack-7", "org1/llama-3-8b", &s, 12);
        let mut g = QuotaReservation::new(
            engine.clone(),
            "ak.gpustack-7".into(),
            "org1/llama-3-8b".into(),
            s,
            0,
            12,
        );
        g.settle(Some(33));
        assert_eq!(engine.usage("ak.gpustack-7", "org1/llama-3-8b"), Some(33));
    }
}
