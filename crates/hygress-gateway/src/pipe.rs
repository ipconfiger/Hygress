//! The Pingora **terminate-mode** data plane (design §6.1 ①–⑭, net semantics /
//! §11). All compiled under the `integrations` feature (it consumes the frozen
//! `hygress-egress` / `hygress-adapter` contracts).
//!
//! ## Terminate mode
//!
//! The entire request lifecycle runs inside [`ProxyHttp::request_filter`] and
//! returns `Ok(true)`, so Pingora never dials an upstream itself
//! ([`upstream_peer`] is a trait-required sentinel that is never contacted).
//! This mirrors the validated `dogress2` terminate-mode mechanism:
//!
//! 1. Read the **full** downstream body (terminate-mode: model-router, failover
//!    replay, model-mapper, and usage all need the whole body; replay is an O(1)
//!    `Bytes` clone). Enforce the body cap → 413.
//! 2. Run the **pure** pipeline stages ①–⑦ via [`crate::pipeline::prepare`]
//!    (strip / model-router / transformer-in / route match / SWRR / registry).
//! 3. Stage ⑤ ext-auth (origin-ingress `ai-route-route-` scope, FAIL_OPEN).
//! 4. **Failover loop** (⑩): for each SWRR-ordered candidate, build the outbound
//!    (⑧ model-mapper + ⑨ set-instance/route-name + Host) via
//!    [`crate::pipeline::build_outbound`] and send it over a long-lived
//!    [`reqwest`] client (no read timeout — SSE/LLM are long-lived).
//! 5. **Stream the response back** (⑪) chunk-by-chunk, feeding the usage
//!    accumulator (SSE / non-streaming JSON), capturing TTFT, and stripping
//!    hop-by-hop / encoding headers.
//! 6. **Usage push** (⑫, model-route only) via the egress [`GpustackSink`].
//! 7. **Stats/logging** (⑬) via [`crate::metrics::Metrics`].
//! 8. **4xx/5xx fallback** (⑭): on a final 4xx/5xx (or a total transport
//!    failure) with a fallback link, arm `x-higress-fallback-from` +
//!    `x-gpustack-fallback-path`, restore the original path, and re-dispatch via
//!    the fallback pipeline path (bounded by the `max_redirects` guard).
//!
//! ## Provider forward via the frozen `ProviderClient` (D6 / §7)
//!
//! For a **provider-destined** candidate (`name.type` starts `provider-`), the
//! outbound upstream request is built by the frozen
//! [`crate::context::GatewayState::upstream`] (`hygress_egress::provider::ProviderClient`):
//! it applies the key-swap (`Authorization: Bearer <provider apiToken>`), the
//! `Host` / scheme / outbound-proxy resolution, and the path/query — the same
//! pure model unit-tested in [`crate::pipeline::build_outbound`]. The resulting
//! [`hygress_egress::provider::UpstreamRequest`] (`url` + `headers`) is then
//! **dialed** with a long-lived [`reqwest`] client, and the raw `Bytes` body is
//! forwarded byte-for-byte (no JSON re-encode) while the SSE/LLM response
//! streams back.
//!
//! Non-provider candidates (model instances / mirror) skip the `ProviderClient`
//! and dial the [`crate::pipeline::build_outbound`] result directly over the
//! same long-lived [`reqwest`] client.

use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use hygress_core::prelude::{
    GuardAction, GuardrailFailMode, GuardrailSpec, LlmGuardSpec, LlmGuardMode, LlmOnError,
    MatchKind, QuotaDecision, RouteTable, StaticRuleSet, TokenBucketSpec, UsageSchema,
};
use hygress_core::usage::{FlushFields, UsageSnapshot};
use hygress_core::transform::HeaderMap;
use pingora_core::server::Server;
use pingora_core::server::configuration::Opt;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::Error as PingoraError;
use pingora_core::ErrorType::InternalError;
use pingora_core::Result as PingoraResult;
use pingora_http::ResponseHeader;
use pingora_proxy::{http_proxy_service, ProxyHttp, Session};
use tracing::{debug, warn};

use crate::context::{
    hdr, GatewayState, GuardrailClientKey, InboundRequest, OutboundRequest, PreparedRequest,
};
use crate::error::GatewayError;
use crate::pipeline;
use crate::pipeline::PipelineCtx;
use crate::policy_loader::{MergedEntry, MergedPolicy, bare_ingress_name};
use crate::quota::QuotaReservation;
use crate::response_pipeline::ResponsePipeline;

/// A long-lived, per-process data-plane proxy. Cheap to `Arc`-clone per Pingora
/// worker (all state is `Arc` / `Clone`).
#[derive(Clone)]
pub struct HygressProxy {
    pub state: std::sync::Arc<GatewayState>,
    /// Long-lived upstream client: connect budget, **no** read timeout (SSE).
    pub http: reqwest::Client,
    /// D8: outbound **forward-proxy** clients, keyed by the proxy destination
    /// (`host:port`). Reqwest (0.13) configures proxies at the **client** level
    /// (no per-request `proxy`), so one long-lived client is built lazily per
    /// distinct outbound proxy and shared across workers.
    proxy_clients: std::sync::Arc<dashmap::DashMap<String, reqwest::Client>>,
}

impl HygressProxy {
    /// Build the proxy over the shared state. The `reqwest` client is infallible
    /// to build (a builder failure falls back to a default client).
    #[must_use]
    pub fn new(state: std::sync::Arc<GatewayState>) -> Self {
        let http = Self::upstream_client();
        Self {
            state,
            http,
            proxy_clients: std::sync::Arc::new(dashmap::DashMap::new()),
        }
    }

    /// The shared long-lived upstream client builder (direct + per-proxy).
    fn upstream_client() -> reqwest::Client {
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(128)
            // No overall / read timeout: LLM responses stream for a long time.
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    }

    /// D8: the client to dial a candidate with — the base client for a direct
    /// connect, or a lazily-built (and cached) client that routes through the
    /// candidate's outbound forward proxy (HTTP-proxy semantics).
    fn client_for(&self, candidate: &crate::context::CandidateTarget) -> reqwest::Client {
        match &candidate.proxy {
            None => self.http.clone(),
            Some(proxy) => {
                let entry = self
                    .proxy_clients
                    .entry(proxy.clone())
                    .or_insert_with(|| {
                        let proxy_url = if proxy.contains("://") {
                            proxy.clone()
                        } else {
                            format!("http://{proxy}")
                        };
                        let mut builder = reqwest::Client::builder()
                            .connect_timeout(Duration::from_secs(10))
                            .pool_max_idle_per_host(128);
                        // `all` covers both origin schemes for the egress: `http://` origins
                        // proxy absolute-form; `https://` origins open a `CONNECT` tunnel.
                        // `http`-only would skip the tunnel and dial `https` origins direct.
                        match reqwest::Proxy::all(&proxy_url) {
                            Ok(p) => builder = builder.proxy(p),
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    proxy = %proxy,
                                    "invalid outbound proxy; dialing direct"
                                )
                            }
                        }
                        builder
                            .build()
                            .unwrap_or_else(|_| reqwest::Client::new())
                    });
                entry.value().clone()
            }
        }
    }

    /// Bind a plain-TCP listener on `addr` hosting this proxy (terminating in
    /// Pingora). Used by the integration tests and the container launcher.
    pub fn new_server(self, addr: &str) -> PingoraResult<Server> {
        let mut server = Server::new(Some(Opt::default()))?;
        server.bootstrap();
        let mut service = http_proxy_service(&server.configuration, self);
        service.add_tcp(addr);
        server.add_service(service);
        Ok(server)
    }
}

/// Per-request scratch (terminate-mode: the whole lifecycle happens in one
/// `request_filter`, so there is little cross-phase state to thread). `pub`
/// because it is the (public) `ProxyHttp::CTX` of the `HygressProxy` impl.
#[derive(Default)]
pub struct ReqCtx {
    /// The status finally written downstream (for logging).
    status: u16,
}

/// RAII guard keeping the `active_requests` gauge balanced for the whole
/// request lifetime regardless of how [`HygressProxy::request_filter`] exits:
/// one `inc` on entry, one `dec` whenever the guard is dropped at **any**
/// return point (2xx, 4xx/5xx, transport failure, retry-loop exit, auth-deny,
/// or an early short-circuit). This makes the gauge correct for every path
/// without a `dec` scattered across each branch.
struct ActiveGuard(std::sync::Arc<crate::metrics::Metrics>);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.active_requests_dec();
    }
}

#[async_trait::async_trait]
impl ProxyHttp for HygressProxy {
    type CTX = ReqCtx;

    fn new_ctx(&self) -> Self::CTX {
        ReqCtx::default()
    }

    // -------------------------------------------------------------------
    // request_filter — the full terminate-mode data path (①–⑭).
    // -------------------------------------------------------------------
    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut ReqCtx,
    ) -> PingoraResult<bool>
    where
        Self::CTX: Send + Sync,
    {
        let started = Instant::now();
        let state = self.state.clone();

        // ⑬ account the request as in-flight for its whole lifetime: increment
        // once on entry; the guard decrements whenever this function returns at
        // any point (success / error / short-circuit / retry exit), so the gauge
        // is balanced on every path.
        state.metrics.active_requests_inc();
        let _active_guard = ActiveGuard(state.metrics.clone());

        // A consistent snapshot + derived runtime state, read together in ONE
        // atomic read from the cached `SharedConfig` snapshot: the sanitized
        // data, the compiled route table (with its precomputed registry index),
        // and the derived model-router config — swapped as one unit, so the
        // route match, the registry / mapping lookups, and the model-router
        // set-up never drift (and never read a stale cache) — and the
        // per-request route-table rebuild (H2) is gone (an `Arc` load instead
        // of rebuilding the BTreeMap indexes + recompiling every path
        // predicate).
        //
        // B2: the stage-② model-router settings come from the **current
        // snapshot** (`ConfigData.model_router`, hot-reloadable — contract-pin
        // §2.3). The config is a lock-free `ArcSwap` load; the derived
        // [`ModelRouterConfig`] is built once per snapshot at store time (H2),
        // so a `defaultConfig` update (enableOnPathSuffix / aliasNameMapping /
        // maxBodyBytes / prefix / targetHeader) takes effect on the next
        // request with no restart and no per-request DB read or re-derivation.
        let snapshot = state.config.snapshot();
        let data: &hygress_core::ConfigData = &snapshot.data;
        let table: &RouteTable = &snapshot.table;
        let router: &crate::context::ModelRouterConfig = &snapshot.router;

        // ⑥ inbound header phase (phase 1 of the body read; design §4.1): the
        // headers come first so `rate_limit_pre` can short-circuit **before**
        // the body is read (an early 429 does not drain the body).
        let head = Self::read_headers(session);

        // B2 (design §4.1): `rate_limit_pre` — the **ip** dimension (the global
        // spec: the route is not known before `route_match`), before the body
        // read. Empty client ip skips the dimension (D-9).
        if let Some(extra) = self.rate_limit_pre(&state, &head.client_ip) {
            return short_circuit_typed(
                session,
                429,
                "rate_limit_error",
                "rate limit exceeded",
                &extra,
            )
            .await;
        }

        // ⑥ body phase (phase 2): read the full body up to the cap (413).
        let body = match Self::read_body(
            session,
            &head.method,
            router.max_body_bytes,
            head.content_length,
        )
        .await
        {
            Ok(b) => b,
            Err(e) => {
                state.metrics.record_request(e.status(), e.reason());
                return short_circuit(session, e.status(), e.reason()).await;
            }
        };
        let method = head.method;
        let inbound = InboundRequest {
            method: method.clone(),
            path: head.path,
            query: head.query,
            headers: head.headers,
            body,
            content_type: head.content_type,
            client_ip: head.client_ip,
            host: head.host,
        };

        let pctx = PipelineCtx {
            data,
            table,
            config: &state.config,
            router,
        };

        // B1 (design §4.2 / D-11 / D-13): quota `reserve` — initial dispatch
        // only, model-route traffic only (the `usage` scope: mirror /
        // passthrough never reserve). `est = ceil(request_content_bytes / K)`.
        // The RAII guard is declared **outside** the fallback loop so it
        // survives across hops: hop-0 reserves, hop≥1 skips reserve but the
        // guard lives to the true terminal (2xx commit; every abort release).
        let mut quota: Option<QuotaReservation> = None;

        // ⑭ bounded fallback re-dispatch loop.
        let mut current = inbound;
        let mut redirect_count = 0u32;
        loop {
            let mut prepared = match if redirect_count == 0 {
                pipeline::prepare(&current, &pctx)
            } else {
                pipeline::prepare_fallback(&current, &pctx)
            } {
                Ok(p) => p,
                Err(e) => return short_circuit(session, e.status(), e.reason()).await,
            };

            let kind = if prepared.route.is_model_route {
                "model_route"
            } else {
                "mirror"
            };
            // A POST/PUT/PATCH inference request is non-idempotent (retry gate).
            let non_idempotent =
                matches!(method.as_str(), "POST" | "PUT" | "PATCH");

            // The effective policy for this hop (route key known after
            // `route_match`; design §3 / D-12). `None` when no policy is loaded
            // (all pass-through, design §7). The merged spec + its compiled
            // static rules were precomputed at load/reload (H3) — per hop this
            // is one `Arc` load (`PolicyHandle::merged_for`).
            let merged: Option<std::sync::Arc<MergedEntry>> =
                state
                    .policy
                    .as_ref()
                    .map(|h| h.merged_for(bare_ingress_name(&prepared.route.ingress_name)));

            // ④' routing-policy override layer (design §4.3 / D-2 / D-3):
            // decorate the matched **Main** route only, initial dispatch only
            // (`redirect_count == 0`); Fallback / mirror never apply.
            if redirect_count == 0 && prepared.route.matched_by == MatchKind::HeaderExact {
                if let Some(actions) = merged.as_ref().and_then(|e| e.policy.actions.as_ref()) {
                    let applied = pipeline::routing_policy::apply(&mut prepared, actions);
                    if applied.override_miss {
                        warn!(
                            route = %prepared.route.ingress_name,
                            "override_route target not among the candidates; falling back to the original routing (D-2)"
                        );
                    }
                    if applied.pin_miss {
                        warn!(
                            route = %prepared.route.ingress_name,
                            "pin_provider_svc_pattern matched no candidate; keeping the original candidates (D-2)"
                        );
                    }
                    state.metrics.record_policy_applied(applied.applied);
                }
            }

            // ⑤ ext-auth (only for `ai-route-route-` scoped model routes).
            let mut auth_writeback = HeaderMap::new();
            if prepared.route.auth_required {
                if let Some(client) = state.auth.as_ref() {
                    let outcome =
                        crate::pipeline::auth::authenticate(client, &prepared.base_headers).await;
                    state.metrics.record_auth(match &outcome {
                        crate::pipeline::auth::AuthOutcome::Allowed { .. } => "allowed",
                        crate::pipeline::auth::AuthOutcome::Denied => "denied",
                    });
                    match outcome {
                        crate::pipeline::auth::AuthOutcome::Denied => {
                            return short_circuit(session, 401, "auth_denied").await;
                        }
                        crate::pipeline::auth::AuthOutcome::Allowed { write_back } => {
                            if let Some(v) = write_back.get(hdr::MSE_CONSUMER) {
                                if let Some(ut) = prepared.usage.as_mut() {
                                    ut.mse_consumer = v.to_string();
                                }
                            }
                            auth_writeback = write_back;
                        }
                    }
                }
                // `None` auth client → the scope exists but auth is disabled:
                // proceed (fail-open by configuration).
            }

            // B2 (design §4.1): `rate_limit_post` — the **consumer** dimension
            // (the effective global/route spec), after ext-auth (the consumer
            // comes from the `X-Mse-Consumer` write-back), initial dispatch
            // only (D-3). `none` / absent consumer skips (D-10).
            if redirect_count == 0 {
                let consumer = auth_writeback.get(hdr::MSE_CONSUMER).unwrap_or("").to_string();
                let rate_policy = merged.as_ref().map(|e| &e.policy);
                if let Some(extra) = self.rate_limit_post(&state, rate_policy, &consumer) {
                    return short_circuit_typed(
                        session,
                        429,
                        "rate_limit_error",
                        "rate limit exceeded",
                        &extra,
                    )
                    .await;
                }
            }

            // B1 (design §4.2 / D-11 / D-13): quota `reserve` — initial
            // dispatch only, model-route traffic only (the `usage` scope:
            // mirror / passthrough never reserve). `est =
            // ceil(request_content_bytes / K)`. The guard was declared outside
            // the loop (BLOCK-1): it survives across fallback hops so the
            // true terminal commits/releases exactly once.
            if redirect_count == 0 {
                if let (Some(entry), Some(ut)) = (merged.as_ref(), prepared.usage.as_ref()) {
                    if let Some(spec) =
                        entry.policy.quota.as_ref().and_then(|q| q.by_model_tokens.as_ref())
                    {
                        let est = est_tokens(prepared.body.len(), state.quota_k);
                        let now = now_millis();
                        let decision =
                            state.quota.reserve(now, &ut.mse_consumer, &ut.model, spec, est);
                        match decision {
                            QuotaDecision::HardDeny => {
                                state.metrics.record_quota_denied();
                                return short_circuit_typed(
                                    session,
                                    429,
                                    "quota_limit_error",
                                    "token quota exceeded",
                                    &HeaderMap::new(),
                                )
                                .await;
                            }
                            QuotaDecision::SoftExceed => {
                                state.metrics.record_quota_soft_exceed();
                                quota = Some(QuotaReservation::new(
                                    state.quota.clone(),
                                    ut.mse_consumer.clone(),
                                    ut.model.clone(),
                                    spec.clone(),
                                    now,
                                    est,
                                ));
                            }
                            QuotaDecision::Allowed => {
                                quota = Some(QuotaReservation::new(
                                    state.quota.clone(),
                                    ut.mse_consumer.clone(),
                                    ut.model.clone(),
                                    spec.clone(),
                                    now,
                                    est,
                                ));
                            }
                        }
                    }
                }
            }

            // B4a/B4b (design §4.4 / D-14): `guardrail_in` — once before the
            // candidate loop, initial dispatch only. A hit short-circuits 403
            // `guardrail_blocked`; the quota guard releases on drop (D-11) and
            // a `completed=false` usage row is reported (the terminal matrix).
            // The static rules are the precompiled set from the merged policy
            // (H3) — no per-request regex compilation.
            if redirect_count == 0 {
                if let Some(entry) = merged.as_ref() {
                    if let Some(reason) = self
                        .guardrail_in(
                            &state,
                            entry.policy.guardrail.as_ref(),
                            entry.static_set.as_deref(),
                            &prepared.body,
                        )
                        .await
                    {
                        state.metrics.record_guardrail_blocked("in");
                        self.report_incomplete_usage(&prepared, &prepared.selected_service)
                            .await;
                        return short_circuit_typed(
                            session,
                            403,
                            "guardrail_blocked",
                            &reason,
                            &HeaderMap::new(),
                        )
                        .await;
                    }
                }
            }

            // ⑩ failover loop over the SWRR-ordered candidates.
            // The routing policy may override the retry count (the route's
            // retry **conditions** are kept; design §4.3).
            let retry_policy = match prepared.override_retries {
                Some(tries) => hygress_core::prelude::RetryPolicy {
                    tries,
                    conditions: prepared.route.retry.conditions.clone(),
                },
                None => prepared.route.retry.clone(),
            };
            let mut last: Option<Final> = None;
            // The candidate that produced the terminal non-2xx (NB7 usage
            // attribution: model_id / provider_id parse from its service name —
            // the same value written into X-GPUStack-Model-Instance).
            let mut last_service: Option<String> = None;
            for (i, candidate) in prepared.candidates.iter().enumerate() {
                let is_last = i + 1 == prepared.candidates.len();
                // `data.provider_tokens` feeds the D6/§7 provider key-swap: a
                // `provider-<id>.<type>` destination gets its `Authorization`
                // swapped to the provider `apiToken` (see `build_outbound`).
                let outbound =
                    pipeline::build_outbound(&method, &prepared, candidate, &auth_writeback, &data.provider_tokens);
                match self.send_outbound(&prepared, &outbound, candidate).await {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        if (200..=299).contains(&status) {
                            // ⑪/⑫/⑬ success: stream back + usage + metrics. The
                            // outbound static rules come precompiled from the
                            // merged policy (H3).
                            if let Err(e) = self
                                .stream_back(
                                    session,
                                    ctx,
                                    &prepared,
                                    candidate,
                                    resp,
                                    kind,
                                    started,
                                    &mut quota,
                                    merged.as_ref().and_then(|m| m.static_set.as_ref()),
                                )
                                .await
                            {
                                // A downstream write (or an upstream mid-stream read
                                // failure) after the 2xx header was already sent —
                                // the client may have partial bytes. Failover is
                                // impossible here; close the connection (H1: the
                                // stream is broken mid-flight, so keep-alive is
                                // disabled only here, not on the normal end).
                                warn!(error = %e, "downstream stream write failed; closing");
                                session.as_downstream_mut().set_keepalive(None);
                                // D-11: downstream write-fail abort → release the
                                // quota (the guard drops at the return) + report a
                                // `completed=false` usage row (previously: no usage
                                // at all).
                                self.report_incomplete_usage(&prepared, &candidate.service_name)
                                    .await;
                                return Ok(true);
                            }
                            ctx.status = status;
                            return Ok(true);
                        }
                        // Non-2xx: retry the next candidate when the policy allows.
                        if !is_last
                            && retry_policy.should_retry(Some(status), false, false, non_idempotent)
                        {
                            state.metrics.record_retry();
                            state.metrics.record_upstream_error();
                            debug!(status, candidate = %candidate.service_name, "non-2xx; trying next candidate");
                            continue;
                        }
                        let body = resp.bytes().await.unwrap_or_default();
                        last = Some(Final::Http { status, body });
                        last_service = Some(candidate.service_name.clone());
                        break;
                    }
                    Err(e) => {
                        state.metrics.record_upstream_error();
                        if !is_last
                            && retry_policy.should_retry(None, true, false, non_idempotent)
                        {
                            state.metrics.record_retry();
                            debug!(error = %e, candidate = %candidate.service_name, "transport failure; trying next candidate");
                            continue;
                        }
                        last = Some(Final::Transport { detail: e.to_string() });
                        break;
                    }
                }
            }

            // ⑭ fall back on a final 4xx/5xx (or total transport failure).
            if let Some(spec) = prepared.route.fallback.as_ref() {
                let is_error_status = match &last {
                    Some(Final::Http { status, .. }) => (400..=599).contains(status),
                    Some(Final::Transport { .. }) => true,
                    None => false,
                };
                if is_error_status {
                    let original_path = prepared
                        .base_headers
                        .get(hdr::ORIGINAL_PATH)
                        .unwrap_or(&current.path)
                        .to_string();
                    if let Some(plan) =
                        pipeline::fallback::plan(spec, &original_path, redirect_count)
                    {
                        state.metrics.record_fallback();
                        // Arm the re-dispatch: the next hop matches via
                        // `x-higress-fallback-from` and restores the original path.
                        let next = arm_fallback(&current, &plan);
                        current = next;
                        redirect_count += 1;
                        continue;
                    }
                    // Budget exhausted → fall through to forward the error.
                }
            }

            // Forward the final result (no fallback, or budget exhausted).
            match last {
                Some(Final::Http { status, body }) => {
                    ctx.status = status;
                    state
                        .metrics
                        .record_request(status, kind);
                    state
                        .metrics
                        .record_request_duration(kind, started.elapsed().as_secs_f64());
                    // NB7: a terminal non-2xx that **reached an upstream** still
                    // reports usage — `completed=false` (no usage object
                    // observed), zero tokens, the request content bytes, and the
                    // full attribution (model / model_id / provider_id /
                    // model_route_id / user / org). It never fires for
                    // auth-denied / 404-no-route (short-circuits above the
                    // candidate loop) or a total transport failure (no upstream
                    // was reached).
                    if let Some(service) = &last_service {
                        self.report_incomplete_usage(&prepared, service).await;
                    }
                    // P1: frame the buffered non-2xx body (`content-length`) so
                    // Pingora's body writer is not close-delimited. The body is a
                    // known size here (it was fully read via `resp.bytes()`), so
                    // the helper emits `content-length`.
                    let body_len = body.len() as u64;
                    let mut resp_header = ResponseHeader::build(status, None)?;
                    if let Some((name, value)) = response_framing(status, Some(body_len), None) {
                        let _ = resp_header.append_header(name, value);
                    }
                    session
                        .write_response_header(Box::new(resp_header), false)
                        .await?;
                    session.write_response_body(Some(body), true).await?;
                }
                Some(Final::Transport { detail }) => {
                    warn!(error = %detail, "all candidates failed at the transport layer");
                    let status: u16 = 502;
                    ctx.status = status;
                    state.metrics.record_request(status, kind);
                    state
                        .metrics
                        .record_request_duration(kind, started.elapsed().as_secs_f64());
                    short_circuit(session, 502, "all_candidates_failed").await?;
                }
                None => {
                    let status: u16 = 502;
                    ctx.status = status;
                    state.metrics.record_request(status, kind);
                    state
                        .metrics
                        .record_request_duration(kind, started.elapsed().as_secs_f64());
                    short_circuit(session, 502, "all_candidates_failed").await?;
                }
            }
            return Ok(true);
        }
    }

    // -------------------------------------------------------------------
    // upstream_peer — trait-mandatory sentinel. NEVER dialed: request_filter
    // returns Ok(true), so Pingora never reaches the upstream dial path.
    // -------------------------------------------------------------------
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut ReqCtx,
    ) -> PingoraResult<Box<HttpPeer>> {
        Ok(Box::new(HttpPeer::new("127.0.0.1:0", false, String::new())))
    }
}

/// The inbound **header phase** result (phase 1 of the two-phase read; design
/// §4.1). `:path` is mirrored into `headers` so the transformer-in can
/// backstop it for the fallback restore.
struct InboundHead {
    method: String,
    /// Original `:path` (no query).
    path: String,
    /// Query string (empty when absent; leading `?` excluded).
    query: String,
    host: String,
    content_type: String,
    /// Client source IP (`X-Real-IP` if present, else `X-Forwarded-For`).
    client_ip: String,
    /// The `Content-Length` header when present and a valid integer (B1 — the
    /// body reader pre-reserves this exact size, capped at `max_body`).
    content_length: Option<u64>,
    headers: HeaderMap,
}

impl HygressProxy {
    /// Inbound phase 1: read the request **headers** only (no body).
    ///
    /// Splitting the read into headers-then-body (design §4.1 / M1 refactor)
    /// lets `rate_limit_pre` short-circuit **before** the body is read — an
    /// early 429 does not drain a potentially large request body.
    fn read_headers(session: &Session) -> InboundHead {
        let req = session.req_header();
        let host = req
            .headers
            .get("host")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let client_ip = req
            .headers
            .get("x-real-ip")
            .and_then(|v| v.to_str().ok())
            .or_else(|| {
                // D-9: XFF is a comma-separated list; take the **first** value
                // (the leftmost = the original client, per RFC 7239).
                req.headers
                    .get("x-forwarded-for")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.split(',').next())
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or("")
            .to_string();
        let method = req.method.as_str().to_string();
        let path = req.uri.path().to_string();
        let query = req.uri.query().map(|q| q.to_string()).unwrap_or_default();
        // B1: a valid `Content-Length` lets the body reader pre-reserve the
        // exact size (no geometric growth on the request buffer).
        let content_length = req
            .headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let mut headers = HeaderMap::new();
        for (name, value) in req.headers.iter() {
            if let Ok(v) = value.to_str() {
                headers.append(name.as_str(), v.to_string());
            }
        }
        // Mirror `:path` so transformer-in can backstop / restore it (stage ③⑭).
        headers.insert(hdr::PATH, path.clone());
        InboundHead {
            method,
            path,
            query,
            host,
            content_type,
            client_ip,
            content_length,
            headers,
        }
    }

    /// Inbound phase 2: read the **full** body (POST/PUT/PATCH only) up to the
    /// cap (413 above it).
    async fn read_body(
        session: &mut Session,
        method: &str,
        max_body: usize,
        content_length: Option<u64>,
    ) -> Result<Bytes, GatewayError> {
        let has_body = matches!(method, "POST" | "PUT" | "PATCH");
        // B1: when the peer declared a valid Content-Length, pre-reserve that
        // exact size (capped at max_body) so the buffer grows geometrically
        // free (~½ body copy saved). A bogus/oversized Content-Length only
        // costs a transient allocation (capped and freed at request end) and
        // can never bypass the cap below — the per-chunk `extend_from_slice`
        // + the `> max_body` check are the enforcement.
        let mut buf: Vec<u8> = match content_length {
            Some(len) if has_body => Vec::with_capacity(
                usize::try_from(len).unwrap_or(max_body).min(max_body),
            ),
            _ => Vec::new(),
        };
        if has_body {
            while let Ok(Some(chunk)) = session.as_downstream_mut().read_request_body().await {
                buf.extend_from_slice(&chunk);
                if buf.len() > max_body {
                    session.set_keepalive(None);
                    let _ = session.as_downstream_mut().drain_request_body().await;
                    return Err(GatewayError::BodyTooLarge(buf.len(), max_body));
                }
            }
        }
        Ok(Bytes::from(buf))
    }
}

impl HygressProxy {
    // -------------------------------------------------------------------
    // Extension stages (design §4): rate limiting (B2) / guardrail (B4a/B4b).
    // -------------------------------------------------------------------

    /// Check (or seed + check) a per-key token bucket in the shared
    /// `DashMap` (design §4.1 / D-6 / D-9 / D-10).
    ///
    /// The bucket **parameters** (capacity + rate) come from the current
    /// policy snapshot at seed time; the per-key state (tokens / last refill)
    /// lives in the map. An **empty** key skips the dimension (allow) — an
    /// empty key is never shared as a bucket.
    fn check_bucket(
        buckets: &DashMap<String, crate::context::RateLimitEntry>,
        key: &str,
        spec: &TokenBucketSpec,
        now_ms: u64,
    ) -> bool {
        if key.is_empty() {
            return true;
        }
        let mut entry = buckets
            .entry(key.to_string())
            .or_insert_with(|| crate::context::RateLimitEntry {
                spec_rps: spec.rps,
                spec_burst: spec.burst,
                last_active_ms: now_ms,
                bucket: hygress_core::prelude::TokenBucket::new(spec.burst, spec.rps),
            });
        // Hot-reload detection: if the spec changed since the bucket was
        // seeded (e.g. policy reload with new rps/burst), reset the bucket
        // with the new parameters (BLOCK-2: not retain the old spec).
        if entry.spec_rps != spec.rps || entry.spec_burst != spec.burst {
            entry.spec_rps = spec.rps;
            entry.spec_burst = spec.burst;
            entry.bucket = hygress_core::prelude::TokenBucket::new(spec.burst, spec.rps);
        }
        entry.last_active_ms = now_ms;
        entry.bucket.check(now_ms)
    }

    /// The `Retry-After` value (seconds) for a token-bucket denial: the time
    /// until the next token at the spec's fill rate (minimum 1s).
    fn retry_after(spec: &TokenBucketSpec) -> String {
        if spec.rps > 0.0 {
            let secs = (1.0 / spec.rps).ceil();
            (secs.max(1.0) as u64).to_string()
        } else {
            "1".to_string()
        }
    }

    /// B2 (design §4.1): `rate_limit_pre` — the **ip** dimension (the **global**
    /// spec: the route is not known before `route_match`), evaluated **before**
    /// the body read (early rejection does not drain the body).
    ///
    /// Returns the extra response headers (`Retry-After`) on a denial — the
    /// caller short-circuits 429 `rate_limit_error`. `None` = allow / skip
    /// (no policy, no ip limit, or an empty client ip — D-9).
    fn rate_limit_pre(&self, state: &GatewayState, client_ip: &str) -> Option<HeaderMap> {
        let cfg = state.policy.as_ref()?.shared();
        let spec = cfg.global.limits.as_ref()?.ip.as_ref()?;
        if client_ip.is_empty() {
            return None;
        }
        let now = now_millis();
        let key = format!("ip:{client_ip}");
        if Self::check_bucket(&state.ratelimit_buckets, &key, spec, now) {
            None
        } else {
            state.metrics.record_rate_limit_denied("ip");
            let mut h = HeaderMap::new();
            h.insert("retry-after", Self::retry_after(spec));
            Some(h)
        }
    }

    /// B2 (design §4.1): `rate_limit_post` — the **consumer** dimension (the
    /// effective global/route spec), after ext-auth (the consumer comes from
    /// the `X-Mse-Consumer` write-back).
    ///
    /// `none` / an absent consumer skips the dimension (D-10, fail-open).
    /// Returns the extra headers on a denial (the caller short-circuits 429).
    fn rate_limit_post(
        &self,
        state: &GatewayState,
        merged: Option<&MergedPolicy>,
        consumer: &str,
    ) -> Option<HeaderMap> {
        let spec = merged?.limits.as_ref()?.consumer.as_ref()?;
        let key = if consumer.is_empty() || consumer.eq_ignore_ascii_case("none") {
            String::new()
        } else {
            consumer.to_string()
        };
        if key.is_empty() {
            return None;
        }
        let now = now_millis();
        let bkey = format!("consumer:{key}");
        if Self::check_bucket(&state.ratelimit_buckets, &bkey, spec, now) {
            None
        } else {
            state.metrics.record_rate_limit_denied("consumer");
            let mut h = HeaderMap::new();
            h.insert("retry-after", Self::retry_after(spec));
            Some(h)
        }
    }

    /// B4a/B4b (design §4.4 / D-14): the **request-side** guardrail, run once
    /// before the candidate loop (initial dispatch only). The guarded body is
    /// then carried by the loop — a fallback hop inherits it (D-3); v1 actions
    /// are `Block`-only, so the body is unchanged on a pass.
    ///
    /// - **not configured** (no `guardrail` section) → pass-through (D-14);
    /// - **B4a static rules** (the effective `global ++ route` set): a `Block`
    ///   hit → the block reason (the caller 403s `guardrail_blocked`);
    /// - **B4b LLM verdict** (a `HYGRESS_GUARDRAIL_URL` is required — without
    ///   it the LLM stage is *not configured* → pass-through, D-14):
    ///   - `sync`: `Ok(None)` (no verdict) → pass; `Ok(Some(blocked))` → block;
    ///     `Err` → reject only when **both** knobs agree (`fail_mode` `closed`
    ///     + `on_error` `reject`) — fail-closed (D-14), else fail-open;
    ///   - `async`: the verdict is collected in a spawned task (recorded via
    ///     tracing) and the request proceeds (not on the path).
    async fn guardrail_in(
        &self,
        state: &GatewayState,
        guardrail: Option<&GuardrailSpec>,
        static_set: Option<&StaticRuleSet>,
        body: &[u8],
    ) -> Option<String> {
        let Some(g) = guardrail else {
            return None; // not configured → pass-through (D-14)
        };
        let text = String::from_utf8_lossy(body);

        // B4a: static rules (the effective `global ++ route` set), compiled
        // once into the merged policy at load/reload (H3). `None` = no rules
        // or an uncompilable rule → the static scan is skipped (fail-safe,
        // design §7).
        if let Some(set) = static_set {
            if let Some(hit) = set.evaluate(&text) {
                if hit.action == GuardAction::Block {
                    return Some(format!("static rule '{}'", hit.hit_name));
                }
            }
        }

        // B4b: LLM verdict.
        let llm = g.llm.as_ref()?;
        let Some(url) = state.guardrail_url.as_deref() else {
            // `llm:` configured but no service URL: the LLM stage is not
            // configured → pass-through (D-14: `fail_mode` never applies then).
            return None;
        };
        let client = self.guardrail_client(state, url, llm);
        match llm.mode {
            LlmGuardMode::Sync => match client.classify(&text).await {
                Ok(None) => None, // no verdict → pass
                Ok(Some(v)) => {
                    if v.blocked {
                        Some(if v.reason.is_empty() {
                            "llm verdict: blocked".to_string()
                        } else {
                            v.reason.clone()
                        })
                    } else {
                        None
                    }
                }
                Err(e) => {
                    // D-14: fail-closed only when the guardrail is enabled AND
                    // the call failed, and both knobs agree on reject.
                    let reject = matches!(g.fail_mode, GuardrailFailMode::Closed)
                        && matches!(llm.on_error, LlmOnError::Reject);
                    if reject {
                        warn!(
                            error = %e,
                            "llm guardrail verdict failed; rejecting (fail-closed, D-14)"
                        );
                        Some("llm guardrail verdict failed (fail-closed)".to_string())
                    } else {
                        warn!(error = %e, "llm guardrail verdict failed; allowing (fail-open)");
                        None
                    }
                }
            },
            LlmGuardMode::Async => {
                // Collect the verdict without blocking the request path
                // (record only — the request proceeds regardless).
                let client = client.clone();
                let text = text.into_owned();
                tokio::spawn(async move {
                    match client.classify(&text).await {
                        Ok(Some(v)) => {
                            if v.blocked {
                                warn!(
                                    reason = %v.reason,
                                    "async guardrail verdict: blocked (recorded only)"
                                );
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            warn!(error = %e, "async guardrail verdict failed (recorded only)");
                        }
                    }
                });
                None
            }
        }
    }

    /// The cached LLM guardrail client for the (hot-reloadable) spec — one
    /// process-wide client per distinct parameter set (shared concurrency
    /// bound + verdict cache, design §4.4 B4b).
    fn guardrail_client(
        &self,
        state: &GatewayState,
        url: &str,
        llm: &LlmGuardSpec,
    ) -> std::sync::Arc<hygress_egress::guardrail::GuardrailClient> {
        let key = GuardrailClientKey {
            url: url.to_string(),
            timeout_ms: llm.timeout_ms,
            max_rps: llm.max_rps,
            cache_ttl_secs: llm.cache_ttl_secs,
        };
        state
            .guardrail_clients
            .entry(key)
            .or_insert_with(|| {
                std::sync::Arc::new(hygress_egress::guardrail::GuardrailClient::new(
                    url,
                    state.http.clone(),
                    Duration::from_millis(llm.timeout_ms),
                    llm.max_rps as usize,
                    Duration::from_secs(llm.cache_ttl_secs),
                ))
            })
            .value()
            .clone()
    }
}

/// The D-13 quota estimate: `ceil(request_content_bytes / K)` (`K` ≥ 1).
fn est_tokens(body_bytes: usize, k: u64) -> u64 {
    let k = k.max(1);
    let b = body_bytes as u64;
    b.div_ceil(k)
}

/// The final terminal outcome of the candidate failover loop (for ⑭ fallback
/// and downstream error forwarding).
enum Final {
    /// An upstream answered with a non-2xx status (its body is captured).
    Http { status: u16, body: Bytes },
    /// Every candidate failed at the transport (connect) layer.
    Transport { detail: String },
}

impl HygressProxy {
    /// Send one candidate's outbound request over the long-lived client.
    ///
    /// A **provider-destined** candidate (`name.type` starts `provider-`) is
    /// assembled and dialed via the frozen `ProviderClient` (the live D6/§7
    /// ai-proxy key-swap); any other candidate is dialed directly.
    async fn send_outbound(
        &self,
        prepared: &PreparedRequest,
        outbound: &OutboundRequest,
        candidate: &crate::context::CandidateTarget,
    ) -> Result<reqwest::Response, reqwest::Error> {
        // D6 / §7: a provider-destined upstream is assembled by the frozen
        // ProviderClient, then dialed over the long-lived client.
        if candidate.service_name.starts_with("provider-") {
            return self.send_provider_outbound(prepared, outbound, candidate).await;
        }

        // D8: dial with the candidate's **resolved scheme** (never a
        // hardcoded `http` — a TLS provider endpoint dialed over plain HTTP
        // would get a garbage response) and, for a proxied target, route the
        // request **through the outbound forward proxy** (HTTP-proxy
        // semantics: absolute-form for `http`, `CONNECT` tunnel for `https`).
        let url = format!("{}://{}{}", candidate.scheme.as_str(), candidate.address, outbound.path);
        let method = reqwest::Method::from_bytes(outbound.method.as_bytes())
            .unwrap_or(reqwest::Method::POST);
        let mut req = self.client_for(candidate).request(method, url);
        // Design §4.3: the routing policy's per-request timeout override (the
        // shared client itself has no read timeout — LLM streams are
        // long-lived; the override applies to this request only).
        if let Some(ms) = prepared.override_timeout_ms {
            req = req.timeout(Duration::from_millis(ms));
        }
        let names: Vec<&str> = outbound.headers.names().collect();
        for name in names {
            // Pseudo-headers (the core `:path` marker) are internal and are not
            // valid HTTP request headers — drop them before building the request.
            if name.starts_with(':') {
                continue;
            }
            // `content-type` is set once, explicitly below (it is already
            // forwarded in `outbound.headers` from the inbound copy — skip it so
            // the header is not doubled). Mirrors the provider path.
            if name.eq_ignore_ascii_case("content-type") {
                continue;
            }
            for value in outbound.headers.get_all(name) {
                req = req.header(name, value.clone());
            }
        }
        if !outbound.host.is_empty() {
            req = req.header("host", outbound.host.clone());
        }
        if !outbound.content_type.is_empty() {
            req = req.header("content-type", outbound.content_type.clone());
        }
        req.body(outbound.body.clone()).send().await
    }

    /// D6 / §7: build a provider-destined outbound via the frozen
    /// [`hygress_egress::provider::ProviderClient`] and dial it over the long-lived
    /// client.
    ///
    /// The key-swap is applied in the PURE [`crate::pipeline::build_outbound`]
    /// (unit-tested): a `provider-<id>.<type>` candidate's `Authorization` is already
    /// `Bearer <provider apiToken>` on `outbound.headers`. Here the `ProviderClient`
    /// re-derives the provider upstream (`url` / `Authorization` / `Host` / scheme /
    /// outbound-proxy) from that pre-swapped state and the candidate, and the raw
    /// `Bytes` body (`outbound.body`) is forwarded byte-for-byte (no JSON re-encode
    /// — the "no re-encode on the hot path" invariant).
    async fn send_provider_outbound(
        &self,
        prepared: &PreparedRequest,
        outbound: &OutboundRequest,
        candidate: &crate::context::CandidateTarget,
    ) -> Result<reqwest::Response, reqwest::Error> {
        // Resolved origin (`scheme://host:port`) — the `ProviderClient` base URL. A
        // registry-resolved origin is always well-formed; on the (unreachable)
        // malformed case, surface reqwest's own error (reqwest shares the `url`
        // crate, so it rejects identically to `Url::parse`).
        let base_str = format!("{}://{}", candidate.scheme.as_str(), candidate.address);
        let base = match url::Url::parse(&base_str) {
            Ok(u) => u,
            Err(_) => {
                return Err(
                    self.client_for(candidate)
                        .request(http::Method::GET, base_str)
                        .build()
                        .expect_err(
                            "reqwest and Url share the url crate; a rejected origin must be an invalid URL",
                        ),
                );
            }
        };

        // The key-swapped credential is already on `outbound.headers` (build_outbound).
        // Extract the (stripped) bearer so the ProviderClient re-asserts it on the
        // provider upstream; a non-`Bearer` credential yields "" so the existing
        // header is left verbatim (never re-prefixed as `Bearer <raw>`).
        let api_token = provider_api_token(outbound);

        let method = http::Method::from_bytes(outbound.method.as_bytes())
            .unwrap_or(http::Method::POST);
        let opts = hygress_egress::provider::UpstreamOptions {
            method,
            // `prepared.upstream_path` is already `rewrite-target`-applied; do not
            // re-apply a path rewrite here.
            input_path: prepared.upstream_path.clone(),
            capture_groups: Vec::new(),
            path_rewrite: None,
            api_token,
            host_override: if outbound.host.is_empty() {
                None
            } else {
                Some(outbound.host.clone())
            },
            // The model-mapping was already applied to the body in build_outbound —
            // forward the result raw (no re-encode), so leave the ProviderClient
            // body-building (JSON/multipart-only) uninvolved.
            model_mapping: None,
            destination_service: None,
            // `outbound.headers` already carries the (key-swapped) Authorization +
            // set-instance/route-name + forward-auth write-back; forward them.
            inbound_headers: outbound.headers.clone(),
            // Raw body forwarded separately below (a `None` body = no re-encode).
            body: None,
            query: if prepared.query.is_empty() {
                None
            } else {
                Some(prepared.query.clone())
            },
            scheme: None, // `base` already carries the resolved scheme.
            proxy: candidate.proxy.clone(),
        };
        // The D6/§7 provider build routes through the state's frozen-contract
        // `ProviderClient` instance (`self.state.upstream`): its pure
        // `build_upstream_request` applies the key-swap / `Host` / scheme /
        // outbound-proxy, and the field is genuinely read on the live data path.
        let upstream = self.state.upstream.build_upstream_request(&base, &opts);

        // Dial through the candidate's client (direct, or the outbound forward proxy
        // for a proxied target — D8). The ProviderClient records the provider origin
        // (`url`) + the (key-swapped) headers; the raw body streams byte-for-byte.
        let mut req = self.client_for(candidate).request(upstream.method, upstream.url);
        // Design §4.3: the routing policy's per-request timeout override.
        if let Some(ms) = prepared.override_timeout_ms {
            req = req.timeout(Duration::from_millis(ms));
        }
        for (name, value) in upstream.headers.iter() {
            // `content-type` is set once, explicitly below (it is already forwarded
            // in `upstream.headers` from the inbound copy — skip it so the header
            // is not doubled).
            if name.as_str().eq_ignore_ascii_case("content-type") {
                continue;
            }
            let Ok(v) = value.to_str() else {
                continue;
            };
            req = req.header(name.as_str(), v);
        }
        if !outbound.content_type.is_empty() {
            req = req.header("content-type", outbound.content_type.clone());
        }
        req.body(outbound.body.clone()).send().await
    }
}

/// Instance-form provider build for the frozen-contract
/// [`hygress_egress::provider::ProviderClient`]. The egress type is a **stateless
/// unit struct** whose build is a stateless *associated* function
/// (`ProviderClient::build_upstream_request`, not callable via method syntax). This
/// extension trait gives that build an instance form so the live data plane can
/// call `self.state.upstream.build_upstream_request(...)` — routing **through the held
/// instance** (`&self` is the stored client, a read on the hot path) rather than the
/// bare associated function. It only forwards to the pure build.
trait ProviderBuild {
    fn build_upstream_request(
        &self,
        base: &url::Url,
        opts: &hygress_egress::provider::UpstreamOptions,
    ) -> hygress_egress::provider::UpstreamRequest;
}

impl ProviderBuild for hygress_egress::provider::ProviderClient {
    #[inline]
    fn build_upstream_request(
        &self,
        base: &url::Url,
        opts: &hygress_egress::provider::UpstreamOptions,
    ) -> hygress_egress::provider::UpstreamRequest {
        hygress_egress::provider::ProviderClient::build_upstream_request(base, opts)
    }
}

/// The (stripped) bearer token the frozen
/// [`hygress_egress::provider::ProviderClient`] should re-assert on the provider
/// upstream, taken from the key-swapped `Authorization` already on
/// [`OutboundRequest`].
///
/// When a provider token matched, [`crate::pipeline::build_outbound`] wrote
/// `Authorization: Bearer <provider apiToken>` — we strip the `Bearer ` prefix so
/// the `ProviderClient` re-asserts it unchanged. When **no** token matched (ext-auth
/// FAIL_OPEN + a provider destination with no matching token), the credential is the
/// verbatim inbound `Authorization`, which may be non-`Bearer`; for that case we
/// return an **empty** token so the `ProviderClient` leaves the existing header
/// **verbatim** instead of re-prefixing it as `Bearer <raw>`.
fn provider_api_token(outbound: &OutboundRequest) -> String {
    outbound
        .headers
        .get(crate::context::hdr::AUTHORIZATION)
        .and_then(|v| v.strip_prefix("Bearer ").map(|t| t.to_string()))
        .unwrap_or_default()
}

impl HygressProxy {
    /// ⑪ stream a successful upstream response back to the downstream `Session`,
    /// feeding the usage accumulator, capturing TTFT, and stripping hop-by-hop /
    /// encoding headers. Then ⑫ push the usage record (model-route only) and ⑬
    /// record metrics.
    ///
    /// The **response-side skeleton** (design §2.2 / D-1 / B4c) runs between
    /// `usage.feed(chunk)` and `write_response_body(chunk)`:
    /// the precompiled static-rule set builds the per-response
    /// [`ResponsePipeline`] (observe pass-through when `None`; per-chunk
    /// judgment otherwise). A hit **stops
    /// writing and cuts the downstream** (the 2xx header is already sent — it
    /// cannot be changed), then takes the terminal path: the quota reservation
    /// is released (D-11) and a `completed=false` usage row is reported.
    ///
    /// The 2xx stream-end path **commits** the quota reservation with the
    /// actual `total_token` (D-11 / D-13). `quota` is settled here on the
    /// success and guardrail-cut paths; the `Drop` guard covers the remaining
    /// terminal paths (abort / write-fail) — settlement is idempotent.
    #[allow(clippy::too_many_arguments)] // ⑪ forwards the full prepared + candidate context
    async fn stream_back(
        &self,
        session: &mut Session,
        ctx: &mut ReqCtx,
        prepared: &PreparedRequest,
        candidate: &crate::context::CandidateTarget,
        mut resp: reqwest::Response,
        kind: &str,
        started: Instant,
        quota: &mut Option<QuotaReservation>,
        compiled_static: Option<&std::sync::Arc<StaticRuleSet>>,
    ) -> PingoraResult<()> {
        // Hop-by-hop / connection-negotiated headers are not forwarded. Framing
        // (`content-length`) is handled by `response_framing` below (P1), and
        // `content-encoding` IS forwarded verbatim (P5): the body is forwarded
        // byte-for-byte and reqwest has no gzip feature, so stripping it would
        // hand the client a mislabeled encoded body.
        const SKIP: &[&str] = &[
            "server",
            "via",
            "transfer-encoding",
            "content-length",
            "connection",
        ];
        let status = resp.status().as_u16();
        // The upstream `content-length` is trusted for keep-alive framing when
        // the body passes through unmodified. A streamed upstream (SSE) has no
        // CL → chunked framing below.
        let upstream_cl: Option<u64> = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        // Headers are read up front (immutably) so the borrow of `resp` ends
        // before the chunk loop mutates it via `resp.chunk()`.
        let forwarded: Vec<(String, String)> = resp
            .headers()
            .iter()
            .filter(|(name, _)| !SKIP.iter().any(|s| name.as_str().eq_ignore_ascii_case(s)))
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|v| (name.as_str().to_string(), v.to_string()))
            })
            .collect();

        let mut resp_header = ResponseHeader::build(status, None)?;
        for (name, value) in forwarded {
            let _ = resp_header.append_header(name, value);
        }
        // P1: give the response explicit framing — the upstream content-length
        // when the body is forwarded unmodified, else chunked — so Pingora's
        // body writer is not close-delimited (which would mark the session
        // un-reusable and close the TCP connection after every response).
        if let Some((name, value)) = response_framing(status, None, upstream_cl) {
            let _ = resp_header.append_header(name, value);
        }
        // H1: the normal 2xx stream-end keeps the downstream connection alive
        // (the connection is only cut on a mid-stream break — the B4c guardrail
        // hit below or a downstream write failure in the caller).
        session
            .write_response_header(Box::new(resp_header), false)
            .await?;

        // ⑪ stream body; count SSE usage + TTFT (first chunk); B4c per-chunk
        // guardrail between `usage.feed` and `write_response_body`. The output
        // static rules arrive precompiled (H3) — `from_compiled` Arc-clones the
        // compiled set instead of re-compiling or re-cloning the rules.
        let mut usage = UsageSnapshot::new(UsageSchema::Generic);
        let mut resp_pipeline = ResponsePipeline::from_compiled(compiled_static);
        let mut ttft: Option<f64> = None;
        let mut first = true;
        while let Some(chunk) = resp
            .chunk()
            .await
            .map_err(|e| pingora_err(format!("upstream stream read: {e}")))?
        {
            if first {
                first = false;
                ttft = Some(started.elapsed().as_secs_f64());
            }
            usage.feed(chunk.as_ref());
            // B4c (design §2.2 / §4.4): per-chunk judgment. A hit = stop
            // writing + cut the downstream + terminal path (D-11: release the
            // quota + report a `completed=false` usage row).
            if let Some(hit) = resp_pipeline.on_chunk(chunk.as_ref()) {
                self.state.metrics.record_guardrail_blocked("out");
                // B-11: record the request (the 2xx header was already sent;
                // the stream was cut). The duration covers the partial stream.
                self.state
                    .metrics
                    .record_request(status, kind);
                self.state
                    .metrics
                    .record_request_duration(kind, started.elapsed().as_secs_f64());
                // NB-4: set ctx.status for consistency with the normal
                // stream_end path (line ~1347). The status is the upstream
                // 2xx (the header was already sent to the client).
                ctx.status = status;
                warn!(
                    rule = %hit.hit_name,
                    "output guardrail hit; cutting the downstream stream"
                );
                if let Some(g) = quota {
                    g.settle(None);
                }
                self.report_incomplete_usage(prepared, &candidate.service_name)
                    .await;
                // Cut the downstream connection (no further writes; the
                // connection is not kept alive).
                session.as_downstream_mut().set_keepalive(None);
                return Ok(());
            }
            session.write_response_body(Some(chunk), false).await?;
        }
        session.write_response_body(None, true).await?;
        ctx.status = status;

        // ⑬ metrics.
        let m = self.state.metrics.clone();
        m.record_request(status, kind);
        m.record_request_duration(kind, started.elapsed().as_secs_f64());
        let (input, output, cached) = usage.tokens();
        if input > 0 {
            m.record_tokens("prompt", input);
        }
        if output > 0 {
            m.record_tokens("completion", output);
        }
        if cached > 0 {
            m.record_tokens("cached", cached);
        }
        if let Some(t) = ttft {
            m.record_ttft(kind, t);
        }

        // ⑫ usage push — model-route traffic only (mirror never reports).
        // `prepared` is borrow-only, so the usage target is cloned (it is small).
        if let (Some(sink), Some(ut)) = (self.state.sink.as_ref(), prepared.usage.clone()) {
            // B1: `model_id` / `provider_id` come from the selected destination
            // service name (the value written into X-GPUStack-Model-Instance).
            // Without them the GPUStack server drops EVERY row
            // (`_validate_usage_metric`: `model_id is None and provider_id is
            // None` → skip).
            let fields = usage_fields(&ut, prepared, &candidate.service_name);
            let metrics = usage.flush(&fields);
            // D-11 / D-13: 2xx stream end → commit the quota reservation with
            // the actual `total_token` (the flushed total: the upstream total
            // when it exceeds `input + output`, else the recomputed sum).
            if let Some(g) = quota {
                g.settle(Some(metrics.total_token));
            }
            let _ = sink.push(&metrics).await;
        } else if let Some(g) = quota {
            // Defensive: a reservation without a usage row (mirror traffic
            // never reserves, so this is unreachable in practice) — settle
            // with the observed tokens rather than leaking the estimate.
            g.settle(Some(input.saturating_add(output)));
        }
        Ok(())
    }

    /// NB7: report usage for a terminal non-2xx that **reached an upstream**.
    ///
    /// The request ran to completion (an upstream answered) but carried no
    /// usage object: `completed=false`, zero tokens, `request_content_bytes`
    /// set, full attribution (model / model_id / provider_id / model_route_id /
    /// user / org). Not called for auth-denied / 404-no-route (the pipeline
    /// short-circuits before any upstream) or a total transport failure (no
    /// upstream was reached). Model-route traffic only (`usage` is `None` for
    /// the mirror / passthrough).
    async fn report_incomplete_usage(&self, prepared: &PreparedRequest, service_name: &str) {
        if let (Some(sink), Some(ut)) = (self.state.sink.as_ref(), prepared.usage.clone()) {
            let fields = usage_fields(&ut, prepared, service_name);
            // A fresh, empty snapshot: no usage object observed → `completed`
            // is `false` and every token field is zero.
            let metrics =
                hygress_core::usage::UsageSnapshot::new(hygress_core::usage::UsageSchema::Generic)
                    .flush(&fields);
            let _ = sink.push(&metrics).await;
        }
    }
}

/// Build the usage [`FlushFields`] (the context fields around the accumulated
/// tokens) for one report from the [`UsageTarget`] + prepared request. Shared
/// by the 2xx success push (B1) and the terminal-non-2xx push (NB7) so both
/// carry identical attribution: `model_id` / `provider_id` parsed from the
/// selected service name, `model_route_id` from the route name, `user_id` /
/// `access_key` from the forward-auth write-back consumer, and the
/// `X-Organization-Id` attribution.
fn usage_fields(ut: &crate::context::UsageTarget, prepared: &PreparedRequest, service_name: &str) -> FlushFields {
    let (user_id, access_key) = parse_consumer(&ut.mse_consumer);
    let organization_id = prepared
        .base_headers
        .get(hdr::ORGANIZATION_ID)
        .unwrap_or(&ut.organization_id)
        .to_string();
    let (model_id, provider_id) = parse_instance_ids(service_name);
    FlushFields {
        model: ut.model.clone(),
        user_id,
        access_key,
        model_id,
        provider_id,
        model_route_id: parse_model_route_id(&ut.route_name),
        organization_id: if organization_id.is_empty() {
            None
        } else {
            Some(organization_id)
        },
        started_at_ms: Some(prepared.started_at_ms),
        completed_at_ms: Some(now_millis()),
        request_content_bytes: prepared.body.len() as u64,
        ..Default::default()
    }
}

/// Parse `X-Mse-Consumer` = `<access_key>.gpustack-<user_id>` (or the `none`
/// sentinel) into (`user_id`, `access_key`) for usage attribution.
fn parse_consumer(consumer: &str) -> (Option<u64>, Option<String>) {
    if consumer.is_empty() || consumer.eq_ignore_ascii_case("none") {
        return (None, None);
    }
    if let Some(idx) = consumer.rfind(".gpustack-") {
        let access_key = consumer[..idx].to_string();
        let user_id = consumer[idx + ".gpustack-".len()..].parse::<u64>().ok();
        (user_id, Some(access_key))
    } else {
        // No `.gpustack-` marker: treat the whole value as the access key.
        (None, Some(consumer.to_string()))
    }
}

/// B1: parse `(model_id, provider_id)` from the selected destination service
/// name (`name.type`, no port) — the exact value the gateway writes into
/// `X-GPUStack-Model-Instance` (which the GPUStack server parses back with
/// `get_instance_id_from_header`, regex `^model-\d+-(\d+)(?:-[^.]+)?\..+`).
///
/// GPUStack registry grammar (contract-pin §4.4):
/// - model instance: `model-<model_id>-<instance_id>[-<lora alias>].<type>`
///   → `model_id` from `^model-(\d+)-` (a second segment — the instance id —
///   must follow the model id);
/// - provider: `provider-<provider_id>[-<suffix>].<type>` → `provider_id`
///   from `^provider-(\d+)`.
///
/// An optional ns prefix (`ns/name.type`) and the `<type>` suffix (plus any
/// alias) are ignored. A name without an id yields `None` (the wire field is
/// `omitempty`; e.g. mirror `gpustack` or the test-shaped `model-1.static`).
fn parse_instance_ids(service: &str) -> (Option<i64>, Option<i64>) {
    let name = service.rsplit('/').next().unwrap_or(service);
    let stem = name.split('.').next().unwrap_or(name);
    let mut model_id: Option<i64> = None;
    let mut provider_id: Option<i64> = None;
    if let Some(rest) = stem.strip_prefix("model-") {
        let first = rest.split('-').next().unwrap_or("");
        if !first.is_empty()
            && first.bytes().all(|b| b.is_ascii_digit())
            && rest[first.len()..].starts_with('-')
        {
            model_id = first.parse().ok();
        }
    } else if let Some(rest) = stem.strip_prefix("provider-") {
        let first = rest.split('-').next().unwrap_or("");
        if !first.is_empty() && first.bytes().all(|b| b.is_ascii_digit()) {
            provider_id = first.parse().ok();
        }
    }
    (model_id, provider_id)
}

/// Extract the numeric model-route id from `X-GPUStack-Route-Name`.
///
/// Accepts the main form `<ns>/ai-route-route-<id>.internal` **and** the
/// fallback form `<ns>/ai-route-route-<id>.fallback.internal` (D7): parse the
/// leading digits after `ai-route-route-`, ignoring the remaining suffixes
/// (`.fallback` / `.internal`) and any ns prefix.
fn parse_model_route_id(route_name: &str) -> Option<i64> {
    let name = route_name.rsplit('/').next().unwrap_or(route_name);
    let rest = name.strip_prefix("ai-route-route-")?;
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse::<i64>().ok()
}

/// Unix millis since the epoch (usage `completed_at` / timing).
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Wrap a message into a boxed Pingora [`PingoraError`] (internal-error variant).
fn pingora_err<S: Into<String>>(msg: S) -> Box<PingoraError> {
    PingoraError::explain(InternalError, msg.into())
}

/// Write a short JSON error and return `Ok(true)` (short-circuit the
/// pipeline). The existing calls keep the `proxy_error` shape (D-15
/// compatibility).
async fn short_circuit(session: &mut Session, status: u16, reason: &str) -> PingoraResult<bool> {
    short_circuit_typed(session, status, "proxy_error", reason, &HeaderMap::new()).await
}

/// The **framing** header for a response body of known (`known_len` or a
/// passthrough upstream `content-length`) or unknown (streamed) size (P1).
///
/// Pingora 0.8.1 selects the downstream body writer from the response headers
/// (`init_body_writer_comm`): a `content-length` yields the content-length
/// writer, a `transfer-encoding: chunked` yields the chunked writer, and
/// **neither** yields the close-delimited writer plus `set_keepalive(None)` —
/// which would tear the TCP connection down after every response. So every
/// body-bearing response must carry exactly one of the two.
///
/// Bodyless statuses (1xx / 204 / 304) carry no framing (Pingora emits its
/// own CL-0 framing for these). A known size (the error JSON or a buffered
/// non-2xx body) gets `content-length`; otherwise the streamed body gets
/// `transfer-encoding: chunked` (each `write_response_body` chunk is framed by
/// the chunked writer, so the session stays reusable).
///
/// Never both, and never neither for body-bearing statuses.
fn response_framing(
    status: u16,
    known_len: Option<u64>,
    upstream_cl: Option<u64>,
) -> Option<(&'static str, String)> {
    if (100..200).contains(&status) || status == 204 || status == 304 {
        return None;
    }
    match known_len.or(upstream_cl) {
        Some(len) => Some(("content-length", len.to_string())),
        None => Some(("transfer-encoding", "chunked".to_string())),
    }
}

/// Write a **typed** JSON error with optional extra response headers (design
/// §4.1 / D-15) and return `Ok(true)` (short-circuit the pipeline).
///
/// The body is `{"error":{"message":<message>,"type":<err_type>}}`; `extra`
/// carries e.g. the rate-limit `Retry-After` header.
///
/// H1: the error response is complete, so the downstream connection stays
/// eligible for keep-alive (it is only force-closed on a mid-stream break —
/// the B4c guardrail hit or a downstream write failure). Pingora's own reuse
/// logic still closes the connection when the exchange did not finish cleanly
/// (e.g. an early rejection before the request body was drained).
async fn short_circuit_typed(
    session: &mut Session,
    status: u16,
    err_type: &str,
    message: &str,
    extra: &HeaderMap,
) -> PingoraResult<bool> {
    let body = Bytes::from(format!(
        "{{\"error\":{{\"message\":\"{}\",\"type\":\"{}\"}}}}",
        json_escape(message),
        json_escape(err_type)
    ));
    let mut resp = ResponseHeader::build(status, None)?;
    let _ = resp.insert_header("content-type", "application/json; charset=utf-8");
    let pairs: Vec<(String, String)> = extra
        .names()
        .filter_map(|name| extra.get(name).map(|v| (name.to_string(), v.to_string())))
        .collect();
    for (name, v) in pairs {
        let _ = resp.insert_header(name, v);
    }
    // P1: frame the known-size JSON body (`content-length`) so Pingora's body
    // writer is not close-delimited (which would close the connection after the
    // response). The helper returns None for bodyless statuses (1xx/204/304),
    // which cannot occur here (errors are 4xx/5xx), so framing is always set.
    if let Some((name, value)) = response_framing(status, Some(body.len() as u64), None) {
        let _ = resp.append_header(name, value);
    }
    session.write_response_header(Box::new(resp), false).await?;
    session.write_response_body(Some(body), true).await?;
    Ok(true)
}

/// Minimal JSON string escaping (the values here are slugs / controlled text).
fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Arm a fallback re-dispatch over the **original** inbound request: set
/// `x-higress-fallback-from` (= the Fallback route key) and
/// `x-gpustack-fallback-path` (= the restored original path). The `:path`
/// restored by the transformer-in (stage ③) is the same original path, so the
/// Fallback route's full-match predicate lines up.
fn arm_fallback(current: &InboundRequest, plan: &pipeline::fallback::FallbackPlan) -> InboundRequest {
    let mut next = current.clone();
    // Carry the armed fallback markers through (the inbound transform will fold
    // `x-gpustack-fallback-path` back onto `:path`).
    pipeline::fallback::arm(&mut next.headers, plan);
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- B1: parse_instance_ids (model_id / provider_id attribution) -----

    #[test]
    fn parses_model_instance_id() {
        // `model-<model_id>-<instance_id>.<type>` (contract-pin §4.4).
        assert_eq!(parse_instance_ids("model-1-10.static"), (Some(1), None));
        assert_eq!(parse_instance_ids("model-12-45.dns"), (Some(12), None));
        assert_eq!(parse_instance_ids("model-1-10.proxy"), (Some(1), None));
        assert_eq!(parse_instance_ids("model-1-10.tunnel"), (Some(1), None));
    }

    #[test]
    fn parses_lora_alias_suffix() {
        // LoRA: `model-<mid>-<iid>-l<sha8>.<type>` — the model id is still the
        // first numeric segment.
        assert_eq!(
            parse_instance_ids("model-1-10-labcdef12.static"),
            (Some(1), None)
        );
    }

    #[test]
    fn parses_provider_id() {
        assert_eq!(parse_instance_ids("provider-3.dns"), (None, Some(3)));
        assert_eq!(parse_instance_ids("provider-12.proxy"), (None, Some(12)));
        // The provider egress proxy name (`provider-<id>-proxy`) still yields
        // the provider id.
        assert_eq!(
            parse_instance_ids("provider-3-proxy.dns"),
            (None, Some(3))
        );
    }

    #[test]
    fn handles_ns_prefix() {
        // Defensive: an ns-qualified service (`ns/name.type`) parses the same.
        assert_eq!(
            parse_instance_ids("higress-system/model-1-10.static"),
            (Some(1), None)
        );
        assert_eq!(
            parse_instance_ids("higress-system/provider-3.dns"),
            (None, Some(3))
        );
    }

    #[test]
    fn no_id_yields_none() {
        // `model-1.static` has no instance segment (`^model-(\d+)-` needs a
        // second segment) → None; mirror / cluster names carry no id.
        assert_eq!(parse_instance_ids("model-1.static"), (None, None));
        assert_eq!(parse_instance_ids("gpustack"), (None, None));
        assert_eq!(parse_instance_ids("gpustack.dns:30080"), (None, None));
        assert_eq!(parse_instance_ids("cluster-gateway"), (None, None));
        assert_eq!(parse_instance_ids("fallback-5.static"), (None, None));
        // Non-numeric ids do not parse.
        assert_eq!(parse_instance_ids("model-x-10.static"), (None, None));
        assert_eq!(parse_instance_ids("provider-x.dns"), (None, None));
    }

    // ----- D7: parse_model_route_id (main + fallback route names) -----

    #[test]
    fn parses_main_route_name() {
        assert_eq!(
            parse_model_route_id("higress-system/ai-route-route-1.internal"),
            Some(1)
        );
        // Same-namespace (no ns prefix) form.
        assert_eq!(parse_model_route_id("ai-route-route-42.internal"), Some(42));
    }

    #[test]
    fn parses_fallback_route_name() {
        // D7: the fallback ingress name `...<id>.fallback.internal` yields the
        // same route id (the leading digits after `ai-route-route-`).
        assert_eq!(
            parse_model_route_id("higress-system/ai-route-route-5.fallback.internal"),
            Some(5)
        );
        assert_eq!(
            parse_model_route_id("ai-route-route-5.fallback.internal"),
            Some(5)
        );
    }

    #[test]
    fn route_name_without_id_is_none() {
        assert_eq!(parse_model_route_id("gpustack"), None);
        assert_eq!(parse_model_route_id("higress-system/fallback-5"), None);
        // `ai-route-route-` with no digits (e.g. a malformed name) → None.
        assert_eq!(parse_model_route_id("ai-route-route-.internal"), None);
        // Legacy pattern must not match.
        assert_eq!(parse_model_route_id("higress-system/ai-route-model-3"), None);
    }

    // ----- D6/§7: provider bearer re-assert (provider_api_token) -----

    fn out_with_auth(auth: Option<&str>) -> OutboundRequest {
        let mut headers = HeaderMap::new();
        if let Some(a) = auth {
            headers.insert(hdr::AUTHORIZATION, a);
        }
        OutboundRequest {
            method: "POST".into(),
            path: "/v1/chat/completions".into(),
            host: "provider-1.example.com".into(),
            headers,
            body: bytes::Bytes::new(),
            content_type: "application/json".into(),
        }
    }

    #[test]
    fn bearer_credential_yields_stripped_token() {
        // A `Bearer <token>` credential (provider key-swap or a `Bearer` write-back)
        // yields the stripped token so the ProviderClient re-asserts it unchanged.
        assert_eq!(
            provider_api_token(&out_with_auth(Some("Bearer sk-provider-1"))),
            "sk-provider-1"
        );
    }

    #[test]
    fn non_bearer_inbound_credential_is_not_reordered() {
        // A non-`Bearer` inbound `Authorization` (reachable via ext-auth FAIL_OPEN +
        // a provider destination with no matching token) must NOT be re-prefixed as
        // `Bearer <raw>`: the token is empty so the ProviderClient leaves the existing
        // header verbatim.
        assert_eq!(
            provider_api_token(&out_with_auth(Some("Basic dXNlcjpwYXNz"))),
            ""
        );
        // A scheme-less raw credential behaves the same (never re-prefixed).
        assert_eq!(provider_api_token(&out_with_auth(Some("raw-cred"))), "");
    }

    #[test]
    fn absent_authorization_yields_empty_token() {
        assert_eq!(provider_api_token(&out_with_auth(None)), "");
    }

    // ----- NB-2: check_bucket spec-fingerprint reset -----

    #[test]
    fn check_bucket_resets_on_spec_change() {
        let buckets: DashMap<String, crate::context::RateLimitEntry> =
            DashMap::new();
        // Seed with rps=1, burst=1: first check passes, second is denied.
        let spec1 = TokenBucketSpec { rps: 1.0, burst: 1 };
        assert!(HygressProxy::check_bucket(&buckets, "ip:1.2.3.4", &spec1, 0));
        assert!(!HygressProxy::check_bucket(&buckets, "ip:1.2.3.4", &spec1, 1));

        // Hot-reload: the spec changes to burst=5. The bucket must be reset
        // (not retain the old burst=1), so the next check passes.
        let spec2 = TokenBucketSpec { rps: 1.0, burst: 5 };
        assert!(
            HygressProxy::check_bucket(&buckets, "ip:1.2.3.4", &spec2, 2),
            "after spec change, the reset bucket (burst=5) must allow"
        );
        // The new bucket has burst=5: we can take 4 more tokens.
        for _ in 0..4 {
            assert!(HygressProxy::check_bucket(&buckets, "ip:1.2.3.4", &spec2, 3));
        }
        // Now the bucket is empty (burst=5 exhausted).
        assert!(!HygressProxy::check_bucket(&buckets, "ip:1.2.3.4", &spec2, 4));
    }

    #[test]
    fn check_bucket_empty_key_is_passthrough() {
        let buckets: DashMap<String, crate::context::RateLimitEntry> =
            DashMap::new();
        let spec = TokenBucketSpec { rps: 1.0, burst: 1 };
        // An empty key is always allowed (D-10: never share a "" bucket).
        assert!(HygressProxy::check_bucket(&buckets, "", &spec, 0));
        assert!(HygressProxy::check_bucket(&buckets, "", &spec, 1));
        // No entry was created.
        assert_eq!(buckets.len(), 0);
    }

    // ----- P1: response_framing (explicit body framing for keep-alive) -----

    #[test]
    fn framing_bodyless_statuses_have_none() {
        // 1xx / 204 / 304 carry no body → no framing header (Pingora emits its
        // own CL-0 framing for these).
        for s in [100u16, 101, 102, 199, 204, 304] {
            assert_eq!(response_framing(s, Some(5), None), None, "known_len {s}");
            assert_eq!(response_framing(s, None, Some(5)), None, "upstream_cl {s}");
        }
    }

    #[test]
    fn framing_known_len_is_content_length() {
        let (name, value) = response_framing(404, Some(123), None).unwrap();
        assert_eq!(name, "content-length");
        assert_eq!(value, "123");
    }

    #[test]
    fn framing_unknown_size_is_chunked() {
        // No known size and no upstream CL (a streamed body, e.g. SSE) → chunked.
        let (name, value) = response_framing(200, None, None).unwrap();
        assert_eq!(name, "transfer-encoding");
        assert_eq!(value, "chunked");
    }

    #[test]
    fn framing_upstream_cl_passthrough() {
        // No local known_len, but the upstream supplied a content-length → forward it.
        let (name, value) = response_framing(200, None, Some(77)).unwrap();
        assert_eq!(name, "content-length");
        assert_eq!(value, "77");
        // A local known_len takes precedence over the upstream CL.
        let (_, value) = response_framing(200, Some(5), Some(77)).unwrap();
        assert_eq!(value, "5");
    }

    #[test]
    fn framing_never_both_never_neither_for_body() {
        // For every body-bearing status, framing is present (never neither) and
        // is exactly one of the two framing headers (never both).
        for s in [200u16, 201, 206, 301, 302, 400, 401, 403, 404, 500, 502, 503] {
            for (known, upstream) in [
                (None, None),
                (Some(0), None),
                (Some(10), None),
                (None, Some(10)),
                (Some(10), Some(20)),
            ] {
                let f = response_framing(s, known, upstream)
                    .unwrap_or_else(|| panic!("{s} {known:?} {upstream:?} must be framed"));
                let (name, _) = f;
                assert!(
                    name == "content-length" || name == "transfer-encoding",
                    "{s}: unexpected framing header {name:?}"
                );
            }
        }
    }

    #[test]
    fn short_circuit_error_response_carries_content_length() {
        // Integration-level: the exact body + status a `short_circuit_typed`
        // error produces must yield a **present** `content-length` whose value
        // equals the real on-wire body length (a keep-alive client depends on it
        // to know the response is complete). The body format mirrors
        // `short_circuit_typed` verbatim (same JSON + `json_escape`).
        for (status, err_type, message) in [
            (404u16, "no_route", "no matching route"),
            (401u16, "unauthorized", "bad token"),
            (502u16, "proxy_error", "all_candidates_failed"),
            (429u16, "rate_limited", "too many requests"),
        ] {
            let body = format!(
                "{{\"error\":{{\"message\":\"{}\",\"type\":\"{}\"}}}}",
                json_escape(message),
                json_escape(err_type)
            );
            let (name, value) =
                response_framing(status, Some(body.len() as u64), None)
                    .unwrap_or_else(|| panic!("a {status} error body must be framed"));
            assert_eq!(name, "content-length", "{status}");
            assert!(!value.is_empty(), "{status}: content-length must be present, not absent");
            assert_eq!(
                value,
                body.len().to_string(),
                "{status}: content-length must match the real body length"
            );
        }
    }
}
