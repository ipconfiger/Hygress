//! Per-request data + the plan the pure pipeline produces (`prepare`) for the
//! async forward stage to execute. These are plain, `Clone`-able value types —
//! no Pingora `Session`, no I/O — so the whole decision pipeline (stages ①–⑨)
//! is unit-testable in isolation, and the async forward (⑩–⑮) consumes a
//! [`PreparedRequest`].

use std::sync::Arc;

use bytes::Bytes;
use hygress_core::prelude::MatchKind;
use hygress_core::transform::HeaderMap;

/// Wire-header names (contract-pin §3.1). Centralized so the stages and the
/// egress/pipeline modules reference a single source of truth.
pub mod hdr {
    /// Core routing key (exact-match, Main routes).
    pub const LLM_MODEL: &str = "x-higress-llm-model";
    /// Fallback redirect key (the main ingress name).
    pub const FALLBACK_FROM: &str = "x-higress-fallback-from";
    /// Original pre-rewrite `:path` backup (transformer), for fallback restore.
    pub const ORIGINAL_PATH: &str = "x-gpustack-original-path";
    /// Fallback-hop path marker, renamed onto `:path` by transformer-in.
    pub const FALLBACK_PATH: &str = "x-gpustack-fallback-path";
    /// Legacy model header (`x-gpustack-model`), renamed to `LLM_MODEL`.
    pub const LEGACY_MODEL: &str = "x-gpustack-model";
    /// Untrusted inbound: derived gateway auth token (stripped at entry).
    pub const GPUSTACK_AUTH_TOKEN: &str = "x-gpustack-auth-token";
    /// Untrusted inbound: instance-routing header (stripped at entry).
    pub const MODEL_INSTANCE: &str = "x-gpustack-model-instance";
    /// Outbound: selected instance (`name.type`), set by set-header stage.
    pub const MODEL_INSTANCE_OUT: &str = "X-GPUStack-Model-Instance";
    /// Outbound: matched route name, set by set-header stage.
    pub const ROUTE_NAME_OUT: &str = "X-GPUStack-Route-Name";
    /// Forward-auth write-back: consumer identity (`access_key.gpustack-<id>` / `none`).
    pub const MSE_CONSUMER: &str = "X-Mse-Consumer";
    /// Forward-auth write-back: `Bearer <registration_token>`.
    pub const AUTHORIZATION: &str = "Authorization";
    /// Forward-auth write-back / forward: request cookie.
    pub const COOKIE: &str = "cookie";
    /// Forward-auth write-back / forward: 5-min auth cache JWT.
    pub const AUTH_CACHE: &str = "x-gpustack-auth-cache";
    /// Tenant id (token-usage org attribution source).
    pub const ORGANIZATION_ID: &str = "X-Organization-Id";
    /// Client raw IP (forwarded to forward-auth).
    pub const REAL_IP: &str = "X-Real-IP";
    /// Forwarded-for chain (forwarded to forward-auth).
    pub const FORWARDED_FOR: &str = "X-Forwarded-For";
    /// Client API key (forwarded to forward-auth).
    pub const API_KEY: &str = "x-api-key";
    /// The request path pseudo-header (transformer-in reads/writes this).
    pub const PATH: &str = ":path";
}

/// The raw inbound request, built once from the downstream `Session`.
///
/// The body is read **fully** up front (terminate-mode: the model-router,
/// failover replay, and model-mapper all need the whole body, and replay is an
/// O(1) `Bytes` clone). `path` is the original `:path`; `headers` also carries
/// a `:path` entry so `Transformer::inbound()` can back up / restore it.
#[derive(Clone, Debug)]
pub struct InboundRequest {
    pub method: String,
    /// Original `:path` (no query).
    pub path: String,
    /// Query string (empty when absent). Leading `?` excluded.
    pub query: String,
    /// Request headers (case-insensitive `HeaderMap`); includes `:path`.
    pub headers: HeaderMap,
    /// Full downstream body (empty for a bodyless request).
    pub body: Bytes,
    /// `Content-Type` of the request (for model-field extraction / rewrite).
    pub content_type: String,
    /// Client source IP (`X-Real-IP` if present, else the peer address).
    pub client_ip: String,
    pub host: String,
}

impl InboundRequest {
    pub fn path_and_query(&self) -> String {
        if self.query.is_empty() {
            self.path.clone()
        } else {
            format!("{}?{}", self.path, self.query)
        }
    }
}

/// The `gpustack-model-router` (`generic-proxy-router`) configuration — derived
/// once per snapshot in `hygress-core` (so the request path never re-derives /
/// re-clones it, and the value can never go stale). Re-exported here (and via
/// `hygress_gateway::ModelRouterConfig`) to preserve the established path.
pub use hygress_core::prelude::ModelRouterConfig;

/// The stage-② model resolution outcome (contract-pin §2.3 decision tree).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelResolution {
    /// PATH-DRIVEN alias HIT: model = `aliasNameMapping[id]`; the body `model`
    /// field is rewritten to that value (if the body is JSON/multipart).
    PathAlias { model: String },
    /// BODY-DRIVEN: model read from the request body (JSON / multipart) or
    /// auto-routing.
    Body { model: String },
    /// Neither `prefix` nor `enableOnPathSuffix` matched (or the body yielded no
    /// model): pass through — no header write, no body rewrite.
    Passthrough,
}

/// Outbound scheme of a resolved upstream (D8): `http` or `https`.
///
/// Derived from the registry protocol (an `https://` domain ⇒ TLS). The data
/// plane dials with **this** scheme — it never hardcodes `http` (a TLS
/// provider endpoint dialed over plain HTTP gets a garbage response).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Scheme {
    /// Plain HTTP (the default).
    #[default]
    Http,
    /// TLS.
    Https,
}

impl Scheme {
    /// The URL scheme string (`http` / `https`).
    pub fn as_str(self) -> &'static str {
        match self {
            Scheme::Http => "http",
            Scheme::Https => "https",
        }
    }

    /// Parse a scheme string (case-insensitive); `None` for any other value.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "http" => Some(Scheme::Http),
            "https" => Some(Scheme::Https),
            _ => None,
        }
    }
}

impl std::fmt::Display for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A candidate upstream destination in SWRR order (the concrete instance the
/// weighted cluster collapses to, contract-pin §2.5).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CandidateTarget {
    /// The destination service string `name.type:port` (as in the Ingress
    /// `higress.io/destination`).
    pub service: String,
    /// `name.type` (no port) — the model-mapper / set-instance key.
    pub service_name: String,
    /// Resolved connect address `host:port`.
    pub address: String,
    /// Whether the upstream is reached directly or through an outbound proxy.
    pub proxied: bool,
    /// Outbound scheme (D8) — from the registry protocol resolution.
    pub scheme: Scheme,
    /// Outbound forward proxy address (`host:port`) when `proxied` (D8);
    /// the request is routed through it (HTTP-proxy semantics).
    pub proxy: Option<String>,
}

/// The outbound request to send to an upstream (after path rewrite, header
/// set-instance/route-name, model-mapper rewrite, and the auth write-back).
#[derive(Clone, Debug)]
pub struct OutboundRequest {
    pub method: String,
    /// The (possibly rewritten) path + query to send upstream.
    pub path: String,
    /// The upstream `Host` header value (the target host[:port] host part).
    pub host: String,
    /// Outbound headers (already carry the set-instance / route-name / auth
    /// write-back; do NOT include hop-by-hop).
    pub headers: HeaderMap,
    /// The (possibly model-mapper-rewritten) body.
    pub body: Bytes,
    /// Content-type forwarded to the upstream.
    pub content_type: String,
}

/// Routing decision metadata carried to the forward stage (for usage
/// attribution + fallback + logging).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteInfo {
    /// The matched route key (main model-route name / fallback ingress name /
    /// mirror name).
    pub route_key: String,
    /// The origin ingress name (ns-qualified as GPUStack writes it).
    pub ingress_name: String,
    /// How the route was matched.
    pub matched_by: MatchKind,
    /// Model-route traffic (Main/Fallback) gets usage push; mirror does not.
    pub is_model_route: bool,
    /// The effective `x-higress-llm-model` value (the routing key).
    pub model: String,
    /// Whether ext-auth (stage ⑤) applies: the matched route's origin ingress
    /// name (ns prefix stripped) starts with `ai-route-route-` (core
    /// [`hygress_core::AuthScope`] prefix scope).
    pub auth_required: bool,
    /// The route's retry policy (`higress.io/proxy-next-upstream[-tries]`).
    pub retry: hygress_core::RetryPolicy,
    /// The canonical 4xx/5xx fallback link (absent for mirror / no-fallback).
    pub fallback: Option<hygress_core::FallbackSpec>,
    /// The matched predicate index within the route (for rewrite capture).
    pub matched_predicate: Option<usize>,
    /// The route's captured path groups (for `rewrite-target`).
    pub path_groups: Vec<String>,
}

/// Where the usage record (stage ⑬) should go — `None` for mirror / passthrough
/// traffic (sink is model-route only, contract-pin §2.8 / §5.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageTarget {
    /// The routed/effective model name (verbatim into the wire `model` field).
    pub model: String,
    /// The matched route name (`<ns>/ai-route-route-<id>.internal`) — source of
    /// `model_route_id` attribution.
    pub route_name: String,
    /// `X-Mse-Consumer` (from forward-auth write-back) — source of `user_id` /
    /// `access_key` attribution. Empty when fail-open / no auth.
    pub mse_consumer: String,
    /// `X-Organization-Id` (tenant attribution).
    pub organization_id: String,
}

/// The pure plan produced by [`crate::pipeline::prepare`] (stages ①–④, ⑥-cap,
/// ⑦) for the async forward stage to execute. It carries the **base** request
/// state (post inbound-strip / model-overwrite / inbound-transform, pre
/// forward-auth write-back, pre per-candidate ⑧/⑨) plus the SWRR-ordered
/// candidate list. The per-candidate [`pipeline::build_outbound`] (⑧ model-mapper
/// and ⑨ set-instance/route-name plus transformer-outbound and Host) is invoked
/// by the forward stage for the selected candidate and, on failover, for each
/// fallback candidate.
#[derive(Clone, Debug)]
pub struct PreparedRequest {
    /// SWRR-ordered candidates (the selected instance is `candidates[0]`).
    pub candidates: Vec<CandidateTarget>,
    /// Routing metadata (usage / fallback / retry / auth scope / logging).
    pub route: RouteInfo,
    /// Base request headers after ① (strip untrusted) ② (model overwrite)
    /// ③ (inbound transform). Does **not** yet carry the forward-auth write-back
    /// (⑤) or the per-candidate ⑨ instance / route-name headers.
    pub base_headers: HeaderMap,
    /// The routed, possibly `rewrite-target`-rewritten upstream path (no query).
    pub upstream_path: String,
    /// The (query string without the leading `?;` empty when absent).
    pub query: String,
    /// The request body after model-router ② (pre per-candidate model-mapper).
    pub body: Bytes,
    /// The body `model` value as of the end of stage ② (B4). `Some(..)` =
    /// the body carries a top-level string `model` and this is its **current**
    /// value (the value after prepare's own rewrite when one happened);
    /// `None` = the body has no rewritable model (missing / non-string /
    /// malformed) — per-candidate model-mapper then skips its scan entirely.
    pub body_model: Option<String>,
    /// The request `Content-Type` forwarded upstream.
    pub content_type: String,
    /// The matched route's per-destination model mapping (⑧; keyed `name.type`).
    pub model_mapping: hygress_core::ModelMapping,
    /// Usage attribution target (model-route traffic only; `None` for mirror).
    pub usage: Option<UsageTarget>,
    /// The selected candidate's service `name.type` (for usage / metrics).
    pub selected_service: String,
    /// Unix millis at request entry (usage `started_at`).
    pub started_at_ms: u64,
    /// Per-request timeout override from the routing policy (design §4.3,
    /// `RoutePolicyActions::timeout_ms`, milliseconds). Applied per outbound
    /// request via the reqwest `RequestBuilder::timeout` (the shared client
    /// itself has no read timeout — LLM streams are long-lived).
    pub override_timeout_ms: Option<u64>,
    /// Retry-count override from the routing policy (design §4.3,
    /// `RoutePolicyActions::retries`). Replaces the route's retry `tries`;
    /// the route's retry **conditions** are kept.
    pub override_retries: Option<u32>,
}

/// Shared, long-lived gateway state threaded through every request. Cheap to
/// `Arc`-clone per request task (all fields are `Arc` / `Clone`).
///
/// **P5-pending / `integrations`-gated:** the three egress fields hold the
/// frozen-contract client types from `hygress-egress` (`forward_auth::Client`,
/// `usage_sink::GpustackSink`, `provider::ProviderClient`). Those symbols are
/// implemented by the egress lane and do not yet exist in the placeholder
/// crate, so this struct is compiled only under the `integrations` feature.
/// The pure pipeline stages do **not** depend on it — they take explicit
/// inputs (see [`crate::pipeline`]).
#[cfg(feature = "integrations")]
#[derive(Clone)]
pub struct GatewayState {
    /// Hot-reload config centre (`ArcSwap<ConfigData>` + per-route-group SWRR).
    pub config: Arc<SharedConfigHandle>,
    /// The SNI certificate store (fed from `Secret gpustack-tls-*`; R-9⑤ —
    /// snapshot-reflected on bind for a future pingora SNI resolver; pingora
    /// 0.8 serves the default-cert PEM on the listener).
    pub tls: crate::tls_store::SniStore,
    /// Forward-auth client (`GET /token-auth`); `None` when auth is disabled.
    /// Contract: `hygress_egress::forward_auth::{Client, ForwardAuthRequest, ForwardAuthVerdict}`.
    pub auth: Option<std::sync::Arc<hygress_egress::forward_auth::Client>>,
    /// R-12: reject when `/token-auth` is unavailable/5xx (`true`, default,
    /// matches GPUStack/Higress `failure_mode_allow=false`); `false` =
    /// legacy fail-open. Env `HYGRESS_EXT_AUTH_FAIL_MODE`.
    pub auth_fail_closed: bool,
    /// Usage sink (`POST /v2/usage/gateway-metrics`); `None` when disabled.
    /// Contract: `hygress_egress::usage_sink::{GpustackSink, new, push}`.
    pub sink: Option<std::sync::Arc<hygress_egress::usage_sink::GpustackSink>>,
    /// Upstream provider forward client (path rewrite / key swap / host /
    /// scheme / proxy). **The live D6/§7 provider build routes through this
    /// field**: for a `provider-<id>.<type>` destination the data plane
    /// (`send_provider_outbound`) invokes
    /// [`hygress_egress::provider::ProviderClient::build_upstream_request`] on
    /// this instance to apply the key-swap (outbound `Authorization` =
    /// `Bearer <provider apiToken>`), `Host` / scheme / outbound-proxy. This is
    /// a read on the hot path, not dead weight. Contract:
    /// `hygress_egress::provider::{ProviderClient, build_upstream_request}`.
    pub upstream: std::sync::Arc<hygress_egress::provider::ProviderClient>,
    /// Prometheus metrics.
    pub metrics: std::sync::Arc<crate::metrics::Metrics>,
    /// The policy handle (design §2.1): `ArcSwap<PolicyConfig>` + mtime poll +
    /// admin `/reload`. `None` ⇒ no policy (all pass-through; the stage is
    /// disabled, design §7).
    pub policy: Option<std::sync::Arc<crate::policy_loader::PolicyHandle>>,
    /// Live per-key rate-limit token buckets (design §4.1 / D-6 / D-9 / D-10),
    /// keyed `ip:<client-ip>` / `consumer:<consumer>`. The bucket **parameters**
    /// come from the current policy snapshot at seed time (hot-reloadable);
    /// the per-key state (tokens / last) lives here, never in `ConfigData`.
    /// Each entry carries the spec that seeded it (for hot-reload detection)
    /// and the last activity timestamp (for idle eviction).
    pub ratelimit_buckets:
        std::sync::Arc<dashmap::DashMap<String, RateLimitEntry>>,
    /// The token-quota engine (design §4.2): fixed-window budgets per
    /// `(consumer, model)`. In-memory only (D-5: restart = fresh window).
    pub quota: std::sync::Arc<hygress_core::prelude::QuotaEngine>,
    /// The quota estimate divisor K (design §4.2 / D-13; `HYGRESS_QUOTA_K`,
    /// default 4): `est = ceil(request_content_bytes / K)`.
    pub quota_k: u64,
    /// Long-lived egress HTTP client (the LLM guardrail verdict calls, design
    /// §4.4 B4b).
    pub http: reqwest::Client,
    /// The LLM guardrail verdict service URL (`HYGRESS_GUARDRAIL_URL`). `None`
    /// ⇒ the LLM guardrail is not configured (pass-through; D-14).
    pub guardrail_url: Option<String>,
    /// Cached LLM guardrail clients, keyed by their (hot-reloadable)
    /// parameters — one process-wide client (shared concurrency bound +
    /// verdict cache) per distinct configuration.
    pub guardrail_clients: std::sync::Arc<
        dashmap::DashMap<
            GuardrailClientKey,
            std::sync::Arc<hygress_egress::guardrail::GuardrailClient>,
        >,
    >,
}

/// The (hot-reloadable) parameters that define one cached
/// [`hygress_egress::guardrail::GuardrailClient`] (design §4.4 B4b). A policy
/// reload with new parameters builds a new client; the previous one stays
/// cached (bounded by the number of distinct configurations ever seen).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GuardrailClientKey {
    /// The verdict service base URL.
    pub url: String,
    /// Per-request verdict timeout (milliseconds).
    pub timeout_ms: u64,
    /// Sustained RPS cap for verdict calls.
    pub max_rps: u32,
    /// Verdict cache TTL (seconds).
    pub cache_ttl_secs: u64,
}

/// One per-key rate-limit bucket entry (design §4.1). Carries the spec that
/// seeded it (for hot-reload detection: a policy change with different
/// rps/burst resets the bucket) and the last activity timestamp (for idle
/// eviction in the bootstrap periodic task).
#[derive(Debug)]
pub struct RateLimitEntry {
    /// The `rps` of the spec that seeded this bucket.
    pub spec_rps: f64,
    /// The `burst` of the spec that seeded this bucket.
    pub spec_burst: u64,
    /// The last `now_ms` at which this key was checked.
    pub last_active_ms: u64,
    /// The token bucket itself.
    pub bucket: hygress_core::prelude::TokenBucket,
}

/// A thin `Clone` handle over [`hygress_core::SharedConfig`] so [`GatewayState`]
/// can clone it cheaply (core holds it by value; this wraps it in `Arc`).
///
/// Snapshot-derived state — the compiled route table + registry index and the
/// derived [`ModelRouterConfig`] — lives in the core [`hygress_core::Snapshot`],
/// built once per snapshot at store time. The request path needs no per-snapshot
/// cache of its own (and therefore has no address-reuse / ABA hazard).
#[derive(Clone, Debug)]
pub struct SharedConfigHandle {
    pub inner: Arc<hygress_core::SharedConfig>,
}

impl SharedConfigHandle {
    pub fn new(inner: hygress_core::SharedConfig) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// The current snapshot's data + route table (with its registry index) +
    /// derived model-router config, from **one** atomic read (they can never
    /// drift across a hot reload) — the pipe threads it through
    /// [`crate::pipeline::PipelineCtx`].
    pub fn snapshot(&self) -> hygress_core::prelude::Snapshot {
        self.inner.snapshot()
    }
}

/// Dereference to the core [`hygress_core::SharedConfig`] so the pipeline can
/// call its methods (SWRR group state, `route_table`, `load`) directly on the
/// thin clone handle.
impl std::ops::Deref for SharedConfigHandle {
    type Target = hygress_core::SharedConfig;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
