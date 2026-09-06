//! Retry policy — pure translation of `higress.io/proxy-next-upstream(-tries)`
//! annotation semantics (design §6.4).
//!
//! GPUStack writes `higress.io/proxy-next-upstream: "error,timeout,http_503,
//! http_502,non_idempotent"` and `higress.io/proxy-next-upstream-tries: "2"`
//! on every model Ingress. The default (annotation absent) is exactly that
//! set with `tries = 2`.

use serde::{Deserialize, Serialize};

/// A single retry-on condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryCond {
    /// Transport / connection error (upstream connect failure, reset, ...).
    Error,
    /// Upstream request timed out.
    Timeout,
    /// Any 5xx status.
    Http5xx,
    /// An exact status code (`http_503` → `Status(503)`, bare `503` too).
    Status(u16),
    /// Allow retrying non-idempotent requests — a **modifier**, not a trigger
    /// (nginx semantics). When listed, non-idempotent methods (POST/PUT/PATCH)
    /// may retry on an eligible failure; when absent they are never retried
    /// (see [`RetryPolicy::should_retry`]).
    NonIdempotent,
}

/// Result of parsing the retry annotations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedRetry {
    /// The parsed retry policy (conditions plus tries).
    pub policy: RetryPolicy,
    /// Tokens that were present but not recognized (skipped).
    pub unknown: Vec<String>,
}

/// Retry policy for one route (design §6.2 / §6.4).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Retry-on conditions. An empty list means "never retry".
    pub conditions: Vec<RetryCond>,
    /// Number of retries after the first attempt (Envoy `num_retries`).
    pub tries: u32,
}

impl Default for RetryPolicy {
    /// The GPUStack/Higress default: `error,timeout,http_503,http_502,
    /// non_idempotent` with 2 tries (design §2.1.2 / §6.4).
    fn default() -> Self {
        RetryPolicy {
            conditions: vec![
                RetryCond::Error,
                RetryCond::Timeout,
                RetryCond::Status(503),
                RetryCond::Status(502),
                RetryCond::NonIdempotent,
            ],
            tries: 2,
        }
    }
}

impl RetryPolicy {
    /// Whether a given condition is present.
    pub fn has(&self, cond: &RetryCond) -> bool {
        self.conditions.contains(cond)
    }

    /// Parse `higress.io/proxy-next-upstream` (+ optional
    /// `...-tries`). `None` for the conditions annotation yields the default
    /// condition set; for `tries`, `None` (or an unparsable value) yields 2.
    ///
    /// Recognized tokens (case-insensitive, comma-separated, surrounding
    /// whitespace tolerated):
    /// - `error` → [`RetryCond::Error`]
    /// - `timeout` → [`RetryCond::Timeout`]
    /// - `5xx` → [`RetryCond::Http5xx`]
    /// - `http_<n>` / bare `<n>` (400 <= n <= 599) → [`RetryCond::Status`]
    /// - `non_idempotent` → [`RetryCond::NonIdempotent`]
    ///
    /// Unknown tokens are collected in [`ParsedRetry::unknown`] and skipped.
    pub fn parse(conditions: Option<&str>, tries: Option<&str>) -> ParsedRetry {
        let (conds, unknown) = match conditions {
            None => (RetryPolicy::default().conditions, Vec::new()),
            Some(raw) => {
                let mut out = Vec::new();
                let mut unknown = Vec::new();
                for token in raw.split(',') {
                    let t = token.trim().to_ascii_lowercase();
                    if t.is_empty() {
                        continue;
                    }
                    match parse_cond_token(&t) {
                        Some(c) => out.push(c),
                        None => unknown.push(t),
                    }
                }
                (out, unknown)
            }
        };

        // Cap the retry count (R-1): a runaway annotation cannot exceed 32
        // retries; the failover loop treats `tries` as retries-after-first.
        let tries = match tries {
            None => 2,
            Some(raw) => raw.trim().parse::<u32>().unwrap_or(2).min(32),
        };

        ParsedRetry {
            policy: RetryPolicy {
                conditions: conds,
                tries,
            },
            unknown,
        }
    }

    /// Should a request be retried after this attempt?
    ///
    /// Semantics follow nginx/Higress/Envoy (audit fix R-1):
    /// 1. **Trigger set** — a failure is eligible only when it matches a
    ///    non-modifier condition in the list: [`RetryCond::Error`] on a
    ///    transport failure, [`RetryCond::Timeout`] on a timeout,
    ///    [`RetryCond::Http5xx`] / [`RetryCond::Status`] on a matching
    ///    upstream status. A status that is not listed (e.g. 400/404/429 on
    ///    the default GPUStack set) never triggers a retry.
    /// 2. **`non_idempotent` is a modifier gate, not a trigger** — a request
    ///    whose method is non-idempotent (`POST`/`PUT`/`PATCH`) is retried
    ///    only when the policy lists [`RetryCond::NonIdempotent`]. When the
    ///    gate is absent, such requests are NOT retried even on an eligible
    ///    failure (nginx: non-idempotent requests are not passed to the next
    ///    server once sent, unless `non_idempotent` is enabled).
    ///
    /// Parameters:
    /// - `status` — the upstream HTTP status, if any.
    /// - `transport_error` — connect failure / reset (no HTTP response).
    /// - `timed_out` — the attempt timed out.
    /// - `non_idempotent` — `true` when the request method is non-idempotent
    ///   (e.g. POST without an `Idempotency-Key`); gates retries on the
    ///   presence of [`RetryCond::NonIdempotent`] in the policy.
    pub fn should_retry(
        &self,
        status: Option<u16>,
        transport_error: bool,
        timed_out: bool,
        non_idempotent: bool,
    ) -> bool {
        if self.conditions.is_empty() {
            return false;
        }
        // 1. Trigger set: ignore `NonIdempotent` here (it is a modifier).
        let mut eligible = false;
        for c in &self.conditions {
            let hit = match c {
                RetryCond::Error => transport_error,
                RetryCond::Timeout => timed_out,
                RetryCond::Http5xx => status.is_some_and(|s| (500..=599).contains(&s)),
                RetryCond::Status(code) => status == Some(*code),
                RetryCond::NonIdempotent => false,
            };
            if hit {
                eligible = true;
                break;
            }
        }
        if !eligible {
            return false;
        }
        // 2. Modifier gate (nginx semantics).
        if non_idempotent && !self.conditions.contains(&RetryCond::NonIdempotent) {
            return false;
        }
        true
    }
}

/// Parse one condition token (already lowercased / trimmed).
fn parse_cond_token(t: &str) -> Option<RetryCond> {
    match t {
        "error" => Some(RetryCond::Error),
        "timeout" => Some(RetryCond::Timeout),
        "5xx" => Some(RetryCond::Http5xx),
        "non_idempotent" => Some(RetryCond::NonIdempotent),
        _ => {
            let num = t.strip_prefix("http_").unwrap_or(t);
            num.parse::<u16>()
                .ok()
                .filter(|n| (400..=599).contains(n))
                .map(RetryCond::Status)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_matches_gpustack_annotation() {
        let p = RetryPolicy::default();
        assert_eq!(
            p.conditions,
            vec![
                RetryCond::Error,
                RetryCond::Timeout,
                RetryCond::Status(503),
                RetryCond::Status(502),
                RetryCond::NonIdempotent,
            ]
        );
        assert_eq!(p.tries, 2);
    }

    #[test]
    fn parse_gpustack_annotation() {
        let r = RetryPolicy::parse(
            Some("error,timeout,http_503,http_502,non_idempotent"),
            Some("2"),
        );
        assert_eq!(r.policy, RetryPolicy::default());
        assert!(r.unknown.is_empty());
    }

    #[test]
    fn parse_absent_annotation_yields_default() {
        let r = RetryPolicy::parse(None, None);
        assert_eq!(r.policy, RetryPolicy::default());
    }

    #[test]
    fn parse_handles_spacing_case_and_bare_codes() {
        let r = RetryPolicy::parse(Some(" ERROR , 5xx , 503 , Http_502 "), None);
        assert_eq!(
            r.policy.conditions,
            vec![
                RetryCond::Error,
                RetryCond::Http5xx,
                RetryCond::Status(503),
                RetryCond::Status(502),
            ]
        );
        assert_eq!(r.policy.tries, 2);
        assert!(r.unknown.is_empty());
    }

    #[test]
    fn parse_unknown_tokens_are_collected() {
        let r = RetryPolicy::parse(Some("error,bogus,600"), Some("5"));
        assert_eq!(r.policy.conditions, vec![RetryCond::Error]);
        assert_eq!(r.unknown, vec!["bogus", "600"]);
        assert_eq!(r.policy.tries, 5);
    }

    #[test]
    fn parse_bad_falls_back_to_default_tries() {
        assert_eq!(RetryPolicy::parse(None, Some("x")).policy.tries, 2);
        assert_eq!(RetryPolicy::parse(Some("error"), Some("0")).policy.tries, 0);
    }

    fn matrix() -> (RetryPolicy, RetryPolicy, RetryPolicy) {
        (
            RetryPolicy::default(),
            RetryPolicy {
                conditions: vec![RetryCond::Http5xx],
                tries: 1,
            },
            RetryPolicy {
                conditions: Vec::new(),
                tries: 2,
            },
        )
    }

    #[test]
    fn should_retry_default_matrix() {
        let (default, _, none) = matrix();

        // transport error / timeout (eligible triggers)
        assert!(default.should_retry(None, true, false, false));
        assert!(default.should_retry(None, false, true, false));
        // A non-idempotent request with NO other eligible failure is NOT
        // retried: `non_idempotent` is a modifier, never a trigger (R-1).
        assert!(!default.should_retry(None, false, false, true));
        assert!(!default.should_retry(None, false, false, false));

        // status-based: only listed statuses trigger.
        assert!(default.should_retry(Some(503), false, false, false));
        assert!(default.should_retry(Some(502), false, false, false));
        assert!(!default.should_retry(Some(500), false, false, false)); // not in default set
        assert!(!default.should_retry(Some(429), false, false, false));
        // A 4xx (e.g. 401/404) is never retried on the default GPUStack set.
        assert!(!default.should_retry(Some(400), false, false, true));
        assert!(!default.should_retry(Some(404), false, false, true));

        // empty conditions: never retry
        for (s, e, t, n) in [
            (Some(503u16), true, false, true),
            (Some(502), false, true, true),
            (None, true, true, true),
        ] {
            assert!(!none.should_retry(s, e, t, n));
        }
    }

    #[test]
    fn should_retry_non_idempotent_gate() {
        // Policy WITHOUT NonIdempotent: POST (non-idempotent) must not retry
        // even on an eligible status; an idempotent request still retries.
        let gated = RetryPolicy {
            conditions: vec![RetryCond::Status(503)],
            tries: 1,
        };
        assert!(!gated.should_retry(Some(503), false, false, true), "POST gated");
        assert!(gated.should_retry(Some(503), false, false, false), "GET allowed");
        // Transport failures are also gated for non-idempotent methods.
        let gated2 = RetryPolicy {
            conditions: vec![RetryCond::Error],
            tries: 1,
        };
        assert!(!gated2.should_retry(None, true, false, true));
        assert!(gated2.should_retry(None, true, false, false));

        // Policy WITH NonIdempotent: POST retries on an eligible failure.
        let open = RetryPolicy {
            conditions: vec![RetryCond::Status(503), RetryCond::NonIdempotent],
            tries: 1,
        };
        assert!(open.should_retry(Some(503), false, false, true));
        assert!(!open.should_retry(Some(400), false, false, true), "400 still not listed");
    }

    #[test]
    fn should_retry_timeout_trigger() {
        // Timeout retries only when the attempt timed out (R-1: pipe now
        // reports reqwest timeouts as timed_out=true). A POST is still gated
        // by the absent `non_idempotent` modifier.
        let t = RetryPolicy {
            conditions: vec![RetryCond::Timeout],
            tries: 2,
        };
        assert!(!t.should_retry(None, false, true, true), "POST gated (no non_idempotent)");
        assert!(t.should_retry(None, false, true, false), "GET timed out retries");
        assert!(!t.should_retry(None, false, false, true));
        assert!(!t.should_retry(None, true, false, false), "transport is not timeout");
    }

    #[test]
    fn should_retry_5xx_cond() {
        let (_, fiftyxx, _) = matrix();
        assert!(fiftyxx.should_retry(Some(500), false, false, false));
        assert!(fiftyxx.should_retry(Some(599), false, false, false));
        assert!(!fiftyxx.should_retry(Some(499), false, false, false));
        // A status that triggers a *different* condition (transport error)
        // still does not retry when only Http5xx is configured.
        assert!(!fiftyxx.should_retry(None, true, true, true));
    }
}
