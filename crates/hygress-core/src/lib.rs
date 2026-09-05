//! hygress-core — pure domain logic for the Hygress GPUStack/Higress-replacement gateway.
//!
//! This crate is the **zero-I/O** contract foundation. All other crates depend on its types.
//! Module layout (implementation lane owns this):
//!
//! - `bytes`        — shared byte/scan utilities (find_subseq / replace_bytes / multipart
//!   part-value locator; one canonical copy for the gateway body + mapping helpers)
//! - `route`        — `RouteRule`, `PathPred`, `Destination`, `FallbackLink`, `AuthScope`, `RuleSource`
//! - `matcher`      — header + path matching predicates (match input -> matched RouteRule)
//! - `registry`     — `Registry` (Static/Dns/Proxy/Tunnel) + `OutboundProxy` + `name.type[:port]` parsing
//! - `model_mapping`— per-destination (`name.type`) -> outbound body model name map application for JSON/multipart
//! - `swrr`         — Nginx SWRR weighted selection (ported from dogress2 hydra-core; pure, no I/O)
//! - `retry`        — `RetryPolicy` translation from `proxy-next-upstream` annotation semantics
//! - `transform`    — ordered header transformer rules (remove/rename/dedupe/backup original-path)
//! - `usage`        — `ModelUsageMetrics` payload + `completed` semantics (pure types)
//! - `config`       — `ConfigData`/`RouteTable` snapshot + validation (arc-swap/dashmap-backed runtime state)
//! - `policy`       — `PolicyConfig` + limits/quota/guardrail/route-policy config types (pure, serde)
//! - `ratelimit`    — `RatLimiter` token-bucket rate limiter (ip/consumer; deterministic `now_ms`)
//! - `quota`        — `QuotaEngine` fixed-window token quota (reserve/commit/release; deterministic `now_ms`)
//! - `route_policy` — routing-policy action queries (`pin_matches`, `pick_override`)
//! - `guardrail`    — `StaticRuleSet` + `ChunkScanner` (static rules, cross-chunk scan)
//! - `error`        — crate error enum
//!
//! Contract constraints (see docs/design.md):
//! - NO I/O anywhere in this crate. Pure/CPU-only.
//! - Match-order (initial request): exact `x-higress-llm-model` header (Main,
//!   AND the route's full-match path predicate) > mirror `/` catch-all. A
//!   Fallback route is only reachable via `x-higress-fallback-from` during a
//!   fallback redirect (separate key space; an initial request can never pick
//!   it). Path predicates are full-match; longest-anchor ranking is used only
//!   to pick the predicate *within* an already-matched route.
//! - `model_mapping` keys: matchRule service = `name.type` (no port); destination annotation = `name.type:port`.
//! - `RetryPolicy` default: error, timeout, 503, 502, non_idempotent, tries=2.
//!
//! TDD: unit tests live alongside modules; tests may use only real data, no mocks.

pub mod bytes;
pub mod config;
pub mod destination;
pub mod error;
pub mod guardrail;
pub mod matcher;
pub mod model_mapping;
pub mod policy;
pub mod prelude;
pub mod quota;
pub mod ratelimit;
pub mod registry;
pub mod retry;
pub mod route;
pub mod route_policy;
pub mod swrr;
pub mod transform;
pub mod usage;

pub use prelude::*;
