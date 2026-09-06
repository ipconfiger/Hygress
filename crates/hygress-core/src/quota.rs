//! Token-quota engine (design §4.2 / D-5 / D-11 / D-13).
//!
//! [`QuotaEngine`] enforces per-`(consumer, model)` **fixed-window** token
//! budgets. Time is injected as `now_ms` (deterministic — no system clock).
//!
//! # Two-phase reservation (D-11 / D-13)
//!
//! A request first `reserve`s an **estimate** (`est = ceil(body_bytes / K)`,
//! D-13), then settles it:
//! - on success: `commit` replaces the estimate with the **actual** tokens;
//! - on abort (terminal non-2xx / transport / guardrail / write-fail):
//!   `release` returns the unused portion.
//!
//! # Counters
//!
//! Each window counter tracks:
//! - `used` — the running total **including in-flight estimates** (committed +
//!   reserved). A `reserve` adds the estimate; a `commit`/`release` settles it.
//! - `est` — the in-flight (reserved, not-yet-settled) portion of `used`.
//!
//! The soft/hard decision compares `used + est_tokens` (the projected total
//! after this request) against the limits. A [`QuotaDecision::HardDeny`]
//! records nothing (the estimate is not added), so a denied request does not
//! consume budget.
//!
//! # Memory bound (D-11 TTL)
//!
//! Windows are `window_secs` long; a counter is auto-reset when a later window
//! is observed. Process-level cleanup is **idle-based** [`QuotaEngine::evict_idle`]
//! (run by the gateway's periodic task); [`QuotaEngine::gc_stale`] is the
//! window-based auxiliary (not the runtime backstop — corrected in R-3).
//!
//! # In-flight settle (per-reservation)
//!
//! Both `commit` and `release` take the specific reservation's
//! **estimate**, so concurrent in-flight requests on the same
//! `(consumer, model)` settle independently (the unit is the reserved
//! `est_tokens`, not "settle the whole key"). The v1 RAII guard creates at
//! most one reservation per request and settles it on every terminal path.

use dashmap::DashMap;

use crate::policy::LimitWindowSpec;

/// The outcome of a quota [`QuotaEngine::reserve`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaDecision {
    /// Within both soft and hard limits.
    Allowed,
    /// At/over the soft limit (still allowed; the gateway may warn / degrade).
    SoftExceed,
    /// Over the hard limit (must be rejected, e.g. 429). Nothing is recorded.
    HardDeny,
}

/// Per-`(consumer, model)` fixed-window counter.
#[derive(Clone, Copy, Debug)]
struct WindowCounter {
    /// The window index this counter belongs to (`now_ms / window_ms`).
    window_idx: u64,
    /// Running total including in-flight estimates (committed + reserved).
    used: u64,
    /// The in-flight (reserved, not-yet-settled) portion of `used`.
    est: u64,
    /// The last `now_ms` at which this key was touched (reserve / commit /
    /// release). Used by [`QuotaEngine::evict_idle`] for idle-based eviction.
    last_used_ms: u64,
}

/// Token-quota engine: fixed-window counters per `(consumer, model)`.
#[derive(Default)]
pub struct QuotaEngine {
    counters: DashMap<(String, String), WindowCounter>,
}

impl QuotaEngine {
    /// Create an empty engine.
    pub fn new() -> Self {
        Self::default()
    }

    /// The window index for `now_ms` under `spec` (a zero-length window is a
    /// single infinite window, index 0).
    fn window_idx(now_ms: u64, spec: &LimitWindowSpec) -> u64 {
        let window_ms = spec.window_secs.saturating_mul(1000);
        // A zero-length window (window_ms == 0) yields `None` -> index 0.
        now_ms.checked_div(window_ms).unwrap_or(0)
    }

    /// Reserve `est_tokens` for `(consumer, model)` at `now_ms`.
    ///
    /// The decision compares the projected total (`used + est_tokens`) against
    /// the soft/hard limits. A [`QuotaDecision::HardDeny`] records nothing;
    /// [`QuotaDecision::Allowed`] / [`QuotaDecision::SoftExceed`] add the
    /// estimate to the window (in-flight until `commit` / `release`).
    pub fn reserve(
        &self,
        now_ms: u64,
        consumer: &str,
        model: &str,
        spec: &LimitWindowSpec,
        est_tokens: u64,
    ) -> QuotaDecision {
        let widx = Self::window_idx(now_ms, spec);
        let key = (consumer.to_string(), model.to_string());
        let mut entry = self.counters.entry(key).or_insert(WindowCounter {
            window_idx: widx,
            used: 0,
            est: 0,
            last_used_ms: now_ms,
        });
        // Auto-reset on window crossing (fixed-window semantics).
        if entry.window_idx != widx {
            *entry = WindowCounter {
                window_idx: widx,
                used: 0,
                est: 0,
                last_used_ms: now_ms,
            };
        }
        entry.last_used_ms = now_ms;
        let projected = entry.used.saturating_add(est_tokens);
        let deny = spec.hard.is_some_and(|h| projected > h);
        let decision = if deny {
            QuotaDecision::HardDeny
        } else if spec.soft.is_some_and(|s| projected >= s) {
            QuotaDecision::SoftExceed
        } else {
            QuotaDecision::Allowed
        };
        if !deny {
            entry.used = entry.used.saturating_add(est_tokens);
            entry.est = entry.est.saturating_add(est_tokens);
        }
        decision
    }

    /// Settle a **successful** request at `now_ms`: replace the in-flight
    /// reservation identified by `est_tokens` with `actual_tokens` (the net
    /// recorded is `actual` for that reservation). Per-reservation settle, so
    /// concurrent in-flight requests on the same key settle independently.
    ///
    /// If the window crossed since the reservation, the (stale) reservation
    /// is dropped and `actual` is committed into the current window.
    pub fn commit(
        &self,
        now_ms: u64,
        consumer: &str,
        model: &str,
        spec: &LimitWindowSpec,
        est_tokens: u64,
        actual_tokens: u64,
    ) {
        let widx = Self::window_idx(now_ms, spec);
        let key = (consumer.to_string(), model.to_string());
        let settled = match self.counters.get_mut(&key) {
            Some(mut c) => {
                if c.window_idx == widx {
                    // Settle exactly this reservation: drop its estimate,
                    // capped by the actual in-flight amount so a "phantom"
                    // settle for a reservation that was never recorded (e.g.
                    // after a HardDeny) cannot reduce the held budget.
                    let subtract = est_tokens.min(c.est);
                    c.used = c
                        .used
                        .saturating_sub(subtract)
                        .saturating_add(actual_tokens);
                    c.est = c.est.saturating_sub(subtract);
                } else {
                    // Window crossed since reservation: drop the stale
                    // reservation, commit actual into the current window.
                    c.window_idx = widx;
                    c.used = actual_tokens;
                    c.est = 0;
                }
                c.last_used_ms = now_ms;
                true
            }
            None => false,
        };
        if !settled {
            self.counters.insert(
                key,
                WindowCounter {
                    window_idx: widx,
                    used: actual_tokens,
                    est: 0,
                    last_used_ms: now_ms,
                },
            );
        }
    }

    /// Settle an **aborted** request reserved in `window_idx`: release the
    /// reserved `est_tokens` (the request did not complete, so its consumption
    /// is not committed). Returns `est_tokens - actual_tokens` (the unused
    /// reserved amount, saturated at 0).
    ///
    /// A release also counts as **activity** (NB-3): `last_used_ms` is
    /// refreshed so a long-lived in-flight stream that aborts late is not
    /// idle-evicted before its release lands.
    ///
    /// Time is injected (`now_ms`) for determinism (R-3): this engine never
    /// reads a system clock, matching the module contract.
    pub fn release(
        &self,
        now_ms: u64,
        window_idx: u64,
        key: &(String, String),
        est_tokens: u64,
        actual_tokens: u64,
    ) -> u64 {
        if let Some(mut c) = self.counters.get_mut(key) {
            c.last_used_ms = now_ms;
            if c.window_idx == window_idx {
                c.used = c.used.saturating_sub(est_tokens);
                c.est = c.est.saturating_sub(est_tokens);
            }
        }
        est_tokens.saturating_sub(actual_tokens)
    }

    /// Lazily GC entries whose window is older than `current_window_idx`
    /// (leak prevention, D-11). Returns the number of entries removed.
    ///
    /// Note (R-3): the LIVE process-level cleanup is the idle-based
    /// [`QuotaEngine::evict_idle`] invoked by the gateway's periodic task
    /// (`bootstrap.rs`); `gc_stale` is the window-based auxiliary (its docs
    /// previously claimed it was the runtime backstop — it is not).
    pub fn gc_stale(&self, current_window_idx: u64) -> usize {
        let mut removed = 0usize;
        self.counters.retain(|_, c| {
            if c.window_idx >= current_window_idx {
                true
            } else {
                removed += 1;
                false
            }
        });
        removed
    }

    /// Evict entries whose `last_used_ms` is older than `now_ms - idle_ms`
    /// (idle-based leak prevention, complementing the window-based
    /// [`QuotaEngine::gc_stale`]). Returns the number of entries removed.
    ///
    /// This is the gateway's periodic cleanup: each key's window is
    /// spec-relative, so a single global window index does not adapt; idle
    /// eviction is spec-agnostic and safe for all key shapes.
    pub fn evict_idle(&self, now_ms: u64, idle_ms: u64) -> usize {
        let cutoff = now_ms.saturating_sub(idle_ms);
        let mut removed = 0usize;
        self.counters.retain(|_, c| {
            if c.last_used_ms >= cutoff {
                true
            } else {
                removed += 1;
                false
            }
        });
        removed
    }

    /// The current total (committed + in-flight reserved) for `(consumer,
    /// model)`, or `None` when no counter exists (never reserved or GC'd).
    pub fn usage(&self, consumer: &str, model: &str) -> Option<u64> {
        self.counters
            .get(&(consumer.to_string(), model.to_string()))
            .map(|c| c.used)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(window_secs: u64, soft: Option<u64>, hard: Option<u64>) -> LimitWindowSpec {
        LimitWindowSpec {
            window_secs,
            soft,
            hard,
        }
    }

    const WINDOW_MS_60: u64 = 60 * 1000;

    // ----- reserve decisions -----

    #[test]
    fn reserve_allows_within_hard() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(500));
        assert_eq!(e.reserve(0, "c", "m", &s, 100), QuotaDecision::Allowed);
        assert_eq!(e.usage("c", "m"), Some(100));
    }

    #[test]
    fn reserve_hard_deny_records_nothing() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(500));
        // Fill to the hard limit (allowed: projected 500 <= 500).
        assert_eq!(e.reserve(0, "c", "m", &s, 500), QuotaDecision::Allowed);
        // One more token would exceed hard -> HardDeny, and it is NOT recorded.
        assert_eq!(e.reserve(0, "c", "m", &s, 1), QuotaDecision::HardDeny);
        assert_eq!(e.usage("c", "m"), Some(500)); // unchanged (not 501)
    }

    #[test]
    fn reserve_soft_exceed_still_allowed() {
        let e = QuotaEngine::new();
        let s = spec(60, Some(100), Some(500));
        // Projected 150: >= soft(100), <= hard(500) -> SoftExceed (allowed).
        assert_eq!(e.reserve(0, "c", "m", &s, 150), QuotaDecision::SoftExceed);
        assert_eq!(e.usage("c", "m"), Some(150));
    }

    #[test]
    fn no_soft_no_hard_always_allows() {
        let e = QuotaEngine::new();
        let s = spec(60, None, None);
        assert_eq!(e.reserve(0, "c", "m", &s, 1_000_000), QuotaDecision::Allowed);
    }

    #[test]
    fn cumulative_reserves_track_inflight() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(500));
        // 300 + 200 = 500 (both in-flight) -> allowed (projected 500 <= 500).
        assert_eq!(e.reserve(0, "c", "m", &s, 300), QuotaDecision::Allowed);
        assert_eq!(e.reserve(0, "c", "m", &s, 200), QuotaDecision::Allowed);
        // A third in-flight would push to 600 > 500 -> deny (no over-allow).
        assert_eq!(e.reserve(0, "c", "m", &s, 1), QuotaDecision::HardDeny);
    }

    // ----- commit -----

    #[test]
    fn commit_replaces_estimate_with_actual() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(1000));
        e.reserve(0, "c", "m", &s, 5);
        // Actual (10) replaces the estimate (5): net recorded is 10.
        e.commit(0, "c", "m", &s, 5, 10);
        assert_eq!(e.usage("c", "m"), Some(10));
    }

    #[test]
    fn commit_actual_less_than_estimate_returns_difference() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(1000));
        e.reserve(0, "c", "m", &s, 20);
        // Actual (10) < estimate (20): the over-estimate is returned; net 10.
        e.commit(0, "c", "m", &s, 20, 10);
        assert_eq!(e.usage("c", "m"), Some(10));
    }

    #[test]
    fn concurrent_inflight_reservations_settle_independently() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(1000));
        // Two concurrent in-flight on the same (consumer, model).
        e.reserve(0, "c", "m", &s, 300);
        e.reserve(0, "c", "m", &s, 200);
        assert_eq!(e.usage("c", "m"), Some(500));
        // Settle request #1 (est 300 -> actual 250): net 500 - 300 + 250 = 450.
        e.commit(0, "c", "m", &s, 300, 250);
        assert_eq!(e.usage("c", "m"), Some(450));
        // Settle request #2 (est 200 -> actual 220): net 450 - 200 + 220 = 470.
        e.commit(0, "c", "m", &s, 200, 220);
        assert_eq!(e.usage("c", "m"), Some(470));
    }

    #[test]
    fn hard_deny_does_not_consume_budget() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(500));
        // Commit the full budget (500 committed, nothing in flight).
        assert_eq!(e.reserve(0, "c", "m", &s, 500), QuotaDecision::Allowed);
        e.commit(0, "c", "m", &s, 500, 500);
        assert_eq!(e.usage("c", "m"), Some(500));
        // A subsequent reserve that would exceed hard -> HardDeny, records
        // nothing (no in-flight estimate added).
        assert_eq!(e.reserve(0, "c", "m", &s, 10), QuotaDecision::HardDeny);
        assert_eq!(e.usage("c", "m"), Some(500));
        // Because the denial added no in-flight estimate, settling with
        // est=10/actual=0 does NOT reduce the committed budget (would be 490
        // if the denied reserve had recorded est=10).
        e.commit(0, "c", "m", &s, 10, 0);
        assert_eq!(e.usage("c", "m"), Some(500));
    }

    // ----- release -----

    #[test]
    fn release_returns_unused_difference() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(1000));
        e.reserve(0, "c", "m", &s, 100);
        let key = ("c".to_string(), "m".to_string());
        let widx = QuotaEngine::window_idx(0, &s);
        // Aborted after consuming 40 of the 100 reserved -> returns 60.
        // (now_ms injected: 90_000 refreshes last_used_ms for idle-eviction.)
        let returned = e.release(90_000, widx, &key, 100, 40);
        assert_eq!(returned, 60);
        // The reservation is released (nothing committed for the abort).
        assert_eq!(e.usage("c", "m"), Some(0));
        // Release counts as activity: an idle eviction just after (t=90_001,
        // idle 1s) must NOT remove the key.
        assert_eq!(e.evict_idle(90_001, 1_000), 0);
    }

    #[test]
    fn release_actual_greater_than_estimate_saturates() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(1000));
        e.reserve(0, "c", "m", &s, 10);
        let key = ("c".to_string(), "m".to_string());
        let widx = QuotaEngine::window_idx(0, &s);
        // actual (50) > est (10): the returned difference saturates at 0.
        let returned = e.release(0, widx, &key, 10, 50);
        assert_eq!(returned, 0);
    }

    // ----- window reset -----

    #[test]
    fn window_crossing_resets_counter() {
        let e = QuotaEngine::new();
        let s = spec(60, None, Some(1000));
        // Window 0: reserve 500.
        e.reserve(0, "c", "m", &s, 500);
        // Window 1 (after 60s): reserve 500 again -> fresh window, so 500 not
        // 1000.
        e.reserve(WINDOW_MS_60 + 1, "c", "m", &s, 500);
        assert_eq!(e.usage("c", "m"), Some(500));
    }

    // ----- TTL GC -----

    #[test]
    fn gc_stale_removes_old_window_entries() {
        let e = QuotaEngine::new();
        let s = spec(60, None, None);
        // An entry in window 0.
        e.reserve(0, "old", "m", &s, 10);
        // GC at window 5 removes it (window 0 < 5).
        let removed = e.gc_stale(5);
        assert_eq!(removed, 1);
        assert_eq!(e.usage("old", "m"), None);
        // A fresh reserve in window 5 recreates a counter.
        e.reserve(5 * WINDOW_MS_60 + 1, "new", "m", &s, 10);
        assert_eq!(e.usage("new", "m"), Some(10));
        // GC again: the window-5 entry is current -> kept.
        assert_eq!(e.gc_stale(5), 0);
    }

    // ----- idle eviction (NB-2) -----

    #[test]
    fn evict_idle_removes_stale_and_keeps_active() {
        let e = QuotaEngine::new();
        let s = spec(0, None, Some(1000)); // infinite window (window_secs=0)

        // "active" key: reserved at t=90_000 (recent).
        e.reserve(90_000, "active", "m", &s, 10);
        // "stale" key: reserved at t=10_000 (old).
        e.reserve(10_000, "stale", "m", &s, 10);

        // Evict at t=100_000 with idle_ms=50_000:
        // cutoff = 100_000 - 50_000 = 50_000.
        // "stale" last_used_ms=10_000 < 50_000 → evicted.
        // "active" last_used_ms=90_000 >= 50_000 → kept.
        let removed = e.evict_idle(100_000, 50_000);
        assert_eq!(removed, 1);
        assert_eq!(e.usage("stale", "m"), None, "stale entry must be evicted");
        assert_eq!(e.usage("active", "m"), Some(10), "active entry must be kept");

        // Evict again at t=130_000 (still within 50s of t=90_000):
        // cutoff = 130_000 - 50_000 = 80_000.
        // "active" last_used_ms=90_000 >= 80_000 → kept.
        assert_eq!(e.evict_idle(130_000, 50_000), 0);
        assert_eq!(e.usage("active", "m"), Some(10));

        // Evict at t=200_000: cutoff = 150_000.
        // "active" last_used_ms=90_000 < 150_000 → now evicted.
        assert_eq!(e.evict_idle(200_000, 50_000), 1);
        assert_eq!(e.usage("active", "m"), None);
    }

    #[test]
    fn evict_idle_last_used_ms_refreshed_by_commit() {
        let e = QuotaEngine::new();
        let s = spec(0, None, Some(1000));
        // Reserve at t=0.
        e.reserve(0, "c", "m", &s, 10);
        // Commit at t=90_000 (refreshes last_used_ms).
        e.commit(90_000, "c", "m", &s, 10, 20);

        // Evict at t=100_000 with idle_ms=50_000:
        // cutoff = 50_000. last_used_ms=90_000 >= 50_000 → kept.
        assert_eq!(e.evict_idle(100_000, 50_000), 0);
        assert_eq!(e.usage("c", "m"), Some(20));

        // Evict at t=200_000 with idle_ms=50_000:
        // cutoff = 150_000. last_used_ms=90_000 < 150_000 → evicted.
        assert_eq!(e.evict_idle(200_000, 50_000), 1);
        assert_eq!(e.usage("c", "m"), None);
    }
}
