//! Gateway error type — one enum covering every failure the data plane can
//! surface to a client: each `request_filter` / pipeline stage (and, since
//! ORA3-M13, the read-side body failures too) maps to a stable HTTP status +
//! reason slug. Pure, no I/O.

/// Unified data-plane error for the Hygress gateway pipeline.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayError {
    /// The request body exceeds `maxBodyBytes` (413).
    ///
    /// ORA3-M13: this is the ONE oversized-body producer — raised both by the
    /// model-router body-limit check (stage ②, in `crate::pipeline`) and by the
    /// raw downstream body reader (`pipe::read_body`, the former module-local
    /// `BodyReadFailure::TooLarge`). One variant ⇒ one owner of the 413
    /// status + `request_body_too_large` slug.
    #[error("request body too large: {0} bytes (cap {1})")]
    BodyTooLarge(usize, usize),

    /// The downstream request body could not be read to completion (400): the
    /// peer closed / the framing broke mid-body. The buffered bytes are a
    /// **truncated prefix, never a complete request** (AM-3) — the read side
    /// treats this as an abort: the request is short-circuited WITHOUT
    /// dispatch / quota / usage and the connection is closed.
    ///
    /// ORA3-M13: merged from the former module-local `BodyReadFailure::Read`
    /// so the read-side failure classes share the taxonomy with the pipeline
    /// stages (they are raised while reading the body, before `prepare`, and
    /// are consumed in `request_filter`; keeping them on this enum means no
    /// parallel per-variant `status`/`reason` maps drift apart).
    #[error("request body read failed: {detail}")]
    BodyReadAborted {
        /// Description of the read failure (framing error / premature close).
        detail: String,
    },

    /// No route matched and no mirror catch-all exists (404).
    #[error("no route and no mirror for path '{0}'")]
    NoRoute(String),

    /// The registry for a selected destination could not be resolved (503).
    #[error("registry resolve failed: {0}")]
    RegistryResolve(String),

    /// Every candidate destination was exhausted (502/503).
    #[error("all candidates exhausted: {0}")]
    AllCandidatesFailed(String),

    /// A single upstream attempt failed (transport / connect / timeout).
    #[error("upstream attempt failed: {0}")]
    UpstreamAttempt(String),

    /// An outbound (forward-auth / usage) request failed to build / send.
    #[error("egress request failed: {0}")]
    Egress(String),

    /// A downstream write (header / chunk / EOS) failed mid-stream.
    #[error("downstream write failed: {0}")]
    DownstreamWrite(String),

    /// A generic / structural failure.
    #[error("{0}")]
    Other(String),
}

impl GatewayError {
    /// The HTTP status this error short-circuits to.
    pub fn status(&self) -> u16 {
        match self {
            GatewayError::BodyTooLarge(..) => 413,
            // AM-3: a truncated body read is a client/framing failure (400),
            // distinct from the business oversized-body 413.
            GatewayError::BodyReadAborted { .. } => 400,
            GatewayError::NoRoute(_) => 404,
            GatewayError::RegistryResolve(_) => 503,
            GatewayError::AllCandidatesFailed(_) => 502,
            GatewayError::UpstreamAttempt(_) => 502,
            GatewayError::Egress(_) => 502,
            GatewayError::DownstreamWrite(_) => 502,
            GatewayError::Other(_) => 500,
        }
    }

    /// A stable, reason-agnostic slug for metrics + structured error bodies.
    pub fn reason(&self) -> &'static str {
        match self {
            GatewayError::BodyTooLarge(..) => "request_body_too_large",
            GatewayError::BodyReadAborted { .. } => "request_body_read_failed",
            GatewayError::NoRoute(_) => "no_route",
            GatewayError::RegistryResolve(_) => "registry_resolve_failed",
            GatewayError::AllCandidatesFailed(_) => "all_candidates_failed",
            GatewayError::UpstreamAttempt(_) => "upstream_attempt_failed",
            GatewayError::Egress(_) => "egress_failed",
            GatewayError::DownstreamWrite(_) => "downstream_write_failed",
            GatewayError::Other(_) => "other",
        }
    }

    /// A compact, client-safe JSON error body for short-circuits.
    pub fn json_body(&self) -> String {
        format!(
            "{{\"error\":{{\"message\":\"{}\",\"type\":\"proxy_error\",\"reason\":\"{}\"}}}}",
            self.reason(),
            self.reason()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- ORA3-M13: the read-side classes share the single enum -----

    #[test]
    fn read_failure_classes_are_not_conflated() {
        // AM-3/AM-5: the oversized-body business rejection (413) and the
        // truncated-read abort (400) must stay distinct — different statuses,
        // client reason slugs, and abort semantics (only the abort must never
        // dispatch / reserve quota / report usage).
        let too_large = GatewayError::BodyTooLarge(101, 100);
        let aborted = GatewayError::BodyReadAborted {
            detail: "ConnectionClosed: peer prematurely closed".to_string(),
        };
        assert_eq!(too_large.status(), 413);
        assert_eq!(too_large.reason(), "request_body_too_large");
        assert_eq!(aborted.status(), 400);
        assert_eq!(aborted.reason(), "request_body_read_failed");
        // The abort discriminator lives with the variant itself: only the
        // truncated read is `BodyReadAborted` (the oversized-body read was
        // clean); the request_filter matches on this to close the connection
        // and skip dispatch / quota / usage (AM-3).
        assert!(!matches!(too_large, GatewayError::BodyReadAborted { .. }));
        assert!(matches!(aborted, GatewayError::BodyReadAborted { .. }));
        // The Display keeps the log/error message distinguishable.
        assert!(too_large.to_string().contains("too large"));
        assert!(aborted.to_string().contains("peer prematurely closed"));
    }

    #[test]
    fn body_too_large_is_the_single_413_producer_mapping() {
        // ORA3-M13: the model-router body-limit check (stage ②) and the raw
        // read-side cap in `pipe::read_body` both construct THIS variant, so
        // the 413 status + slug are owned here exactly once. Both construction
        // shapes must stay in sync.
        let from_model_router = GatewayError::BodyTooLarge(1024, 1024);
        let from_read_cap = GatewayError::BodyTooLarge(2048, 1024);
        assert_eq!(from_model_router.status(), 413);
        assert_eq!(from_model_router.status(), from_read_cap.status());
        assert_eq!(from_model_router.reason(), "request_body_too_large");
        assert_eq!(from_model_router.reason(), from_read_cap.reason());
    }

    #[test]
    fn every_variant_has_stable_status_and_slug() {
        // A taxonomy invariant: every error maps to a status + slug (no panic,
        // no drift between `status()` and `reason()` arms).
        let cases = [
            (GatewayError::BodyTooLarge(1, 1), 413, "request_body_too_large"),
            (
                GatewayError::BodyReadAborted {
                    detail: "x".into(),
                },
                400,
                "request_body_read_failed",
            ),
            (GatewayError::NoRoute("p".into()), 404, "no_route"),
            (
                GatewayError::RegistryResolve("r".into()),
                503,
                "registry_resolve_failed",
            ),
            (
                GatewayError::AllCandidatesFailed("c".into()),
                502,
                "all_candidates_failed",
            ),
            (
                GatewayError::UpstreamAttempt("u".into()),
                502,
                "upstream_attempt_failed",
            ),
            (GatewayError::Egress("e".into()), 502, "egress_failed"),
            (
                GatewayError::DownstreamWrite("d".into()),
                502,
                "downstream_write_failed",
            ),
            (GatewayError::Other("o".into()), 500, "other"),
        ];
        for (err, status, slug) in cases {
            assert_eq!(err.status(), status, "{err:?}");
            assert_eq!(err.reason(), slug, "{err:?}");
            assert!(err.json_body().contains(slug), "{err:?}");
        }
    }
}
