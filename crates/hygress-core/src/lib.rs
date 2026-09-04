//! hygress-core — pure domain logic for the Hygress GPUStack/Higress-replacement gateway.
//!
//! This crate is the **zero-I/O** contract foundation. All other crates depend on its types.
//! Module layout (implementation lane owns this):
//!
//! - `route`        — `RouteRule`, `PathPred`, `Destination`, `FallbackLink`, `AuthScope`, `RuleSource`
//! - `matcher`      — header + path matching predicates (match input -> matched RouteRule)
//! - `registry`     — `Registry` (Static/Dns/Proxy/Tunnel) + `OutboundProxy` + `name.type[:port]` parsing
//! - `model_mapping`— per-destination (`name.type`) -> outbound body model name map application for JSON/multipart
//! - `swrr`         — Nginx SWRR weighted selection (ported from dogress2 hydra-core; pure, no I/O)
//! - `retry`        — `RetryPolicy` translation from `proxy-next-upstream` annotation semantics
//! - `transform`    — ordered header transformer rules (remove/rename/dedupe/backup original-path)
//! - `usage`        — `ModelUsageMetrics` payload + `completed` semantics (pure types)
//! - `config`       — `ConfigData`/`RouteTable` snapshot + validation (arc-swap/dashmap-backed runtime state)
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

pub mod config;
pub mod destination;
pub mod error;
pub mod matcher;
pub mod model_mapping;
pub mod prelude;
pub mod registry;
pub mod retry;
pub mod route;
pub mod swrr;
pub mod transform;
pub mod usage;

pub use prelude::*;
