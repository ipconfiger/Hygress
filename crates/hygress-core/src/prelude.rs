//! `hygress-core` public type surface (re-exports for downstream crates).

pub use crate::config::{
    literal_anchor_len, ConfigData, FallbackSpec, GatewayFeatureConfig, ModelRouterSettings,
    ProviderToken, RouteTable, SanitizeResult, SharedConfig, TimingConfig, TlsConfig, TlsHost,
    ValidationError, provider_bearer,
};
pub use crate::destination::{
    parse_destinations, parse_service_with_port, Destination, ServiceRef, ServiceType,
};
pub use crate::error::Error;
pub use crate::matcher::{
    match_fallback_by_key, match_fallback_route, match_route, match_route_by_key, MatchKind,
    RouteMatch, FALLBACK_FROM_HEADER, LLM_MODEL_HEADER,
};
pub use crate::guardrail::{ChunkScanner, GuardDecision, StaticRuleSet};
pub use crate::model_mapping::ModelMapping;
pub use crate::policy::{
    GlobalPolicy, GuardAction, GuardrailFailMode, GuardrailSpec, LimitsSpec, LlmGuardMode,
    LlmGuardSpec, LlmOnError, LimitWindowSpec, PolicyConfig, QuotaSpec, RoutePolicyActions,
    RoutePolicySpec, StaticRuleSpec, TokenBucketSpec,
};
pub use crate::quota::{QuotaDecision, QuotaEngine};
pub use crate::ratelimit::{Buckets, RatLimiter, TokenBucket};
pub use crate::registry::{OutboundProxy, Registry, ResolvedTarget};
pub use crate::retry::{ParsedRetry, RetryCond, RetryPolicy};
pub use crate::route_policy::pin_matches;
pub use crate::route::{
    AuthScope, FallbackLink, PathPred, PathRewriter, RouteKind, RouteRule, RuleSource,
};
pub use crate::swrr::{order as swrr_order, SwrrCandidate, SwrrState};
pub use crate::transform::{HeaderMap, RetainMode, TransformOp, TransformRule, Transformer};
pub use crate::usage::{
    parse_usage, FlushFields, ModelUsageMetrics, Operation, Usage, UsageSchema, UsageSnapshot,
};
