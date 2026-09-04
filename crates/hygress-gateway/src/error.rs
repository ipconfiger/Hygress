//! Gateway error type — one enum covering every data-plane pipeline stage so a
//! `request_filter` short-circuit can map a failure to a stable HTTP status +
//! reason slug. Pure, no I/O.

/// Unified data-plane error for the Hygress gateway pipeline.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayError {
    /// The request body exceeds `maxBodyBytes` (413).
    #[error("request body too large: {0} bytes (cap {1})")]
    BodyTooLarge(usize, usize),

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
