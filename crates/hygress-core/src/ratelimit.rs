//! Token-bucket rate limiter (design §4.1, D-6/D-9/D-10).
//!
//! [`RatLimiter`] enforces per-key token buckets for the **ip** and
//! **consumer** dimensions. Time is injected as `now_ms` (deterministic — no
//! system clock), so the limiter is fully unit-testable.
//!
//! # Semantics
//!
//! - Each dimension (when configured) is a token bucket with `burst` capacity
//!   and `rps` fill rate. A bucket refills by `elapsed_ms * (rps/1000)` capped
//!   at `burst`, and a `check` consumes one token (allow) or refuses (deny).
//! - **Key skip (D-9/D-10):** an **empty** key (`""`) or an **absent**
//!   dimension is *skipped* (returns allow) — an empty key is **never** shared
//!   as a bucket, so requests without an ip / consumer do not throttle each
//!   other and are never denied by that dimension.
//!
//! The live per-key bucket state lives in a [`DashMap`]; the bucket *parameters*
//! (capacity + rate) come from the [`LimitsSpec`] and are carried as
//! [`Buckets`] templates used to seed each key's live bucket.

use dashmap::DashMap;

use crate::policy::LimitsSpec;

/// A live token bucket: capacity, fill rate, and running state.
///
/// `tokens` is `f64` (sub-token precision); `last` is the last refill time in
/// ms. A fresh bucket starts **full** (`tokens = burst`).
#[derive(Clone, Debug, PartialEq)]
pub struct TokenBucket {
    /// Burst capacity (max tokens).
    burst: u64,
    /// Fill rate in tokens per millisecond (`rps / 1000`).
    fill_per_ms: f64,
    /// Current tokens (sub-token precision).
    tokens: f64,
    /// Last refill time (ms).
    last: u64,
}

impl TokenBucket {
    /// A fresh, full bucket (starts with `burst` tokens, `last = 0`).
    pub fn new(burst: u64, rps: f64) -> Self {
        Self {
            burst,
            fill_per_ms: rps / 1000.0,
            tokens: burst as f64,
            last: 0,
        }
    }

    /// Refill by elapsed time (capped at `burst`) and try to take one token.
    ///
    /// Returns `true` when a token was granted (and consumed), `false` when the
    /// bucket is empty (deny).
    pub fn check(&mut self, now_ms: u64) -> bool {
        self.refill(now_ms);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self, now_ms: u64) {
        // Guard against clock going backwards (deterministic tests only move
        // forward; the `>=` makes a backward tick a no-op).
        if now_ms >= self.last {
            let elapsed = (now_ms - self.last) as f64;
            self.tokens = (self.tokens + elapsed * self.fill_per_ms).min(self.burst as f64);
            self.last = now_ms;
        }
    }

    /// Current token count (for tests / diagnostics).
    pub fn tokens(&self) -> f64 {
        self.tokens
    }
}

/// The enabled bucket dimensions derived from a [`LimitsSpec`].
///
/// `ip` / `consumer` are `Some` only when the corresponding dimension is
/// configured in the spec. These act as **templates** (a fresh, full bucket with
/// the spec's capacity + rate) from which [`RatLimiter`] seeds per-key live
/// buckets.
#[derive(Clone, Debug, PartialEq)]
pub struct Buckets {
    /// The ip-dimension bucket template (`None` = dimension disabled).
    pub ip: Option<TokenBucket>,
    /// The consumer-dimension bucket template (`None` = dimension disabled).
    pub consumer: Option<TokenBucket>,
}

impl Buckets {
    /// Derive the enabled bucket dimensions from a [`LimitsSpec`].
    pub fn from_spec(spec: &LimitsSpec) -> Self {
        Self {
            ip: spec.ip.as_ref().map(|t| TokenBucket::new(t.burst, t.rps)),
            consumer: spec.consumer.as_ref().map(|t| TokenBucket::new(t.burst, t.rps)),
        }
    }
}

/// Rate limiter: per-key token buckets for the ip and consumer dimensions.
///
/// Built from a [`LimitsSpec`]. Tracks live [`TokenBucket`] state per key in a
/// [`DashMap`] (shared across gateway workers). An **empty** key or an **absent**
/// dimension is skipped (returns allow) — design D-9/D-10.
pub struct RatLimiter {
    /// The enabled-dimension templates (capacity + rate).
    templates: Buckets,
    /// Live ip-dimension buckets, keyed by ip.
    ip_buckets: DashMap<String, TokenBucket>,
    /// Live consumer-dimension buckets, keyed by consumer.
    consumer_buckets: DashMap<String, TokenBucket>,
}

impl RatLimiter {
    /// Build a limiter from a [`LimitsSpec`].
    pub fn new(spec: &LimitsSpec) -> Self {
        Self {
            templates: Buckets::from_spec(spec),
            ip_buckets: DashMap::new(),
            consumer_buckets: DashMap::new(),
        }
    }

    /// Check the **ip** dimension for `ip_key` at `now_ms`.
    ///
    /// Returns `true` (allow) when the ip dimension is disabled **or** the key
    /// is empty (skip, D-9), else the token-bucket decision.
    pub fn check_ip(&self, ip_key: &str, now_ms: u64) -> bool {
        self.check_dim(&self.templates.ip, &self.ip_buckets, ip_key, now_ms)
    }

    /// Check the **consumer** dimension for `consumer_key` at `now_ms`.
    ///
    /// Returns `true` (allow) when the consumer dimension is disabled **or** the
    /// key is empty / `none` (skip, D-10), else the token-bucket decision.
    pub fn check_consumer(&self, consumer_key: &str, now_ms: u64) -> bool {
        self.check_dim(&self.templates.consumer, &self.consumer_buckets, consumer_key, now_ms)
    }

    fn check_dim(
        &self,
        template: &Option<TokenBucket>,
        live: &DashMap<String, TokenBucket>,
        key: &str,
        now_ms: u64,
    ) -> bool {
        // Absent dimension -> allow (skip).
        let Some(tpl) = template else {
            return true;
        };
        // Empty key -> skip (never share an empty bucket) -> allow.
        if key.is_empty() {
            return true;
        }
        // Seed a fresh (full) bucket from the template on first use, then check.
        live.entry(key.to_string())
            .or_insert_with(|| tpl.clone())
            .check(now_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{LimitsSpec, TokenBucketSpec};

    fn spec(rps: f64, burst: u64) -> LimitsSpec {
        LimitsSpec {
            ip: Some(TokenBucketSpec { rps, burst }),
            consumer: None,
        }
    }

    // ----- TokenBucket core behavior -----

    #[test]
    fn full_bucket_allows_burst_then_denies() {
        let mut b = TokenBucket::new(3, 1.0);
        // Burst of 3: three allows at the same instant (no refill time), then
        // deny.
        assert!(b.check(0));
        assert!(b.check(0));
        assert!(b.check(0));
        assert!(!b.check(0));
    }

    #[test]
    fn refill_by_elapsed_rate() {
        // rps=10 -> 10 tokens/sec -> 1 token per 100ms.
        let mut b = TokenBucket::new(2, 10.0);
        // Drain the full burst of 2.
        assert!(b.check(0));
        assert!(b.check(0));
        assert!(!b.check(0));
        // After 100ms, ~1 token refilled -> one allow.
        assert!(b.check(100));
        // Nothing more until another 100ms.
        assert!(!b.check(100));
        // After another 100ms (200ms total), one more token.
        assert!(b.check(200));
    }

    #[test]
    fn refill_capped_at_burst() {
        // A long idle period must not exceed the burst capacity.
        let mut b = TokenBucket::new(5, 1000.0);
        // Let a huge amount of time pass; the bucket caps at burst (5).
        assert!(b.check(1_000_000));
        assert!(b.check(1_000_000));
        assert!(b.check(1_000_000));
        assert!(b.check(1_000_000));
        assert!(b.check(1_000_000));
        assert!(!b.check(1_000_000)); // capped at 5
    }

    // ----- RatLimiter: key skip (D-9/D-10) -----

    #[test]
    fn empty_ip_key_skips_allows() {
        let lim = RatLimiter::new(&spec(1.0, 1));
        // Empty ip key -> skip -> allow, even with a tiny bucket.
        assert!(lim.check_ip("", 0));
        assert!(lim.check_ip("", 0));
        // A real key is still limited (proves the empty key didn't share a
        // bucket with it).
        assert!(lim.check_ip("1.2.3.4", 0));
        assert!(!lim.check_ip("1.2.3.4", 0));
    }

    #[test]
    fn distinct_keys_do_not_share_buckets() {
        let lim = RatLimiter::new(&spec(1.0, 1));
        // Two distinct ips each get their own full bucket.
        assert!(lim.check_ip("a", 0));
        assert!(lim.check_ip("b", 0));
        // Each is now drained, but independently.
        assert!(!lim.check_ip("a", 0));
        assert!(!lim.check_ip("b", 0));
    }

    #[test]
    fn disabled_dimension_allows() {
        // Consumer dimension disabled (None) -> always allow.
        let lim = RatLimiter::new(&spec(1.0, 1));
        assert!(lim.check_consumer("anyone", 0));
        assert!(lim.check_consumer("anyone", 0));
    }

    #[test]
    fn empty_consumer_key_skips() {
        let s = LimitsSpec {
            ip: None,
            consumer: Some(TokenBucketSpec { rps: 1.0, burst: 1 }),
        };
        let lim = RatLimiter::new(&s);
        // Empty consumer key (e.g. `none`/absent) -> skip -> allow (D-10).
        assert!(lim.check_consumer("", 0));
        assert!(lim.check_consumer("", 0));
        // A real consumer key is limited.
        assert!(lim.check_consumer("user-1", 0));
        assert!(!lim.check_consumer("user-1", 0));
    }

    // ----- Determinism -----

    #[test]
    fn deterministic_across_instances() {
        // Two independent limiters with identical spec + key sequence produce
        // identical decisions (pure function of inputs + injected time).
        let a = RatLimiter::new(&spec(2.0, 3));
        let b = RatLimiter::new(&spec(2.0, 3));
        let times = [0u64, 0, 0, 0, 100, 100, 250];
        let ra: Vec<bool> = times.iter().map(|&t| a.check_ip("k", t)).collect();
        let rb: Vec<bool> = times.iter().map(|&t| b.check_ip("k", t)).collect();
        assert_eq!(ra, rb);
    }
}
