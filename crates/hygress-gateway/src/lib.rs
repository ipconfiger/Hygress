//! hygress-gateway — data plane (design §6) + container main (design §11).
//!
//! Pingora **terminate-mode** proxy: the whole gateway lifecycle runs in
//! `request_filter` and returns `Ok(true)` so Pingora never dials an upstream
//! itself. The decision pipeline (stages ①–⑨, net semantics) is **pure** and
//! lives in [`pipeline`]; the async forward (⑩–⑮) lives in [`pipe`]
//! (`integrations`-gated, consumes the frozen `hygress-egress` /
//! `hygress-adapter` contracts — default feature; see ORA3-M20).
//!
//! Pipeline (design §6.1 net semantics):
//! ① strip untrusted inbound headers · ② model-router (body/alias →
//! `x-higress-llm-model` overwrite) · ③ transformer-in · ④ route match
//! (header + full-match path, mirror `/` catch-all) · ⑤ ext-auth (origin
//! ingress `ai-route-route-` scope, FAIL_OPEN) · ⑥ full-body read (cap→413) ·
//! ⑦ registry resolve → SWRR · ⑧ model-mapper (per-destination) · ⑨ set
//! `X-GPUStack-Model-Instance`/`X-GPUStack-Route-Name` · ⑩ failover + upstream
//! (path rewrite / key swap / Host) · ⑪ stream response (chunk / SSE usage /
//! TTFT / strip hop-by-hop) · ⑫ usage push · ⑬ stats/logging · ⑭ 4xx/5xx
//! fallback (`x-higress-fallback-from` guard, max 10, original-path restore).
//!
//! Modules:
//! - `pipeline` — pure stages (①–⑨ + fallback) with per-stage unit tests (core-only)
//! - `pipe`     — the Pingora `ProxyHttp` terminate-mode impl (`integrations`)
//! - `tls_store`— SNI cert store fed from `Secret gpustack-tls-*` (hot-reload)
//! - `admin`    — admin HTTP (raw method+path router, admin-token gated) + real `/metrics`
//! - `stats`    — 15020 `/stats/prometheus` envoy-style shallow-compat endpoint
//! - `config`   — env parsing (`GATEWAY_*` / `GPUSTACK_*` / `HYGRESS_*`)
//! - `bootstrap`— wiring + readiness + (gated) data-plane / control-plane launch
//! - `main`     — container entry
//!
//! Port discipline (design §11): data plane `GATEWAY_HTTP_PORT`/`tls_port`;
//! 15020 stats; admin 8081. NEVER bind 9876/15010/15012/8888/15051.

pub mod admin;
pub mod body;
pub mod bootstrap;
pub mod config;
pub mod context;
pub mod error;
pub mod metrics;
pub mod pipeline;
#[cfg(feature = "integrations")]
pub mod pipe;
pub mod policy_loader;
pub mod quota;
pub mod response_pipeline;
pub mod stats;
pub mod tls_store;

pub use context::{
    CandidateTarget, InboundRequest, ModelRouterConfig, OutboundRequest, PreparedRequest,
    RouteInfo, SharedConfigHandle, UsageTarget,
};
pub use error::GatewayError;
pub use policy_loader::{MergedPolicy, PolicyHandle, merge_policy};
pub use quota::QuotaReservation;
pub use response_pipeline::ResponsePipeline;
