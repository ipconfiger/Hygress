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
use hygress_core::prelude::{RouteTable, UsageSchema};
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

use crate::context::{hdr, GatewayState, InboundRequest, OutboundRequest, PreparedRequest};
use crate::error::GatewayError;
use crate::pipeline;
use crate::pipeline::PipelineCtx;

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

        // A consistent config snapshot + runtime index (built from the same
        // snapshot so the route match and the registry lookups never drift).
        //
        // B2: the stage-② model-router settings come from the **current
        // snapshot** (`ConfigData.model_router`, hot-reloadable — contract-pin
        // §2.3). The `ArcSwap` load is cheap and per-request, so a
        // `defaultConfig` update (enableOnPathSuffix / aliasNameMapping /
        // maxBodyBytes / prefix / targetHeader) takes effect on the next
        // request with no restart and no per-request DB read.
        let data = state.config.load();
        let router = crate::context::ModelRouterConfig::from_settings(&data.model_router);

        // ⑥ read the full request up front (body cap → 413; the cap is the
        // snapshot's `maxBodyBytes`).
        let inbound = match Self::read_inbound(session, router.max_body_bytes).await {
            Ok(i) => i,
            Err(e) => {
                self.state
                    .metrics
                    .record_request(e.status(), e.reason());
                return short_circuit(session, e.status(), e.reason()).await;
            }
        };
        let method = inbound.method.clone();

        let table = match RouteTable::rebuild(&data) {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "route table rebuild failed");
                return short_circuit(session, 503, "config_invalid").await;
            }
        };
        let pctx = PipelineCtx {
            data: &data,
            table: &table,
            config: &state.config,
            router: &router,
        };

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

            // ⑩ failover loop over the SWRR-ordered candidates.
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
                            // ⑪/⑫/⑬ success: stream back + usage + metrics.
                            if let Err(e) = self
                                .stream_back(session, ctx, &prepared, candidate, resp, kind, started)
                                .await
                            {
                                // A downstream write failed after the 2xx header was
                                // already sent — the client may have partial bytes.
                                // Failover is impossible here; close the connection.
                                warn!(error = %e, "downstream stream write failed; closing");
                                return Ok(true);
                            }
                            ctx.status = status;
                            return Ok(true);
                        }
                        // Non-2xx: retry the next candidate when the policy allows.
                        if !is_last
                            && prepared.route.retry.should_retry(Some(status), false, false, non_idempotent)
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
                            && prepared.route.retry.should_retry(None, true, false, non_idempotent)
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
                    let resp_header = ResponseHeader::build(status, None)?;
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

impl HygressProxy {
    /// Read the full downstream request into an [`InboundRequest`] (① body read +
    /// cap). `:path` is mirrored into the header map so the transformer-in can
    /// backstop it for the fallback restore.
    async fn read_inbound(session: &mut Session, max_body: usize) -> Result<InboundRequest, GatewayError> {
        // (i) Immutable borrow of the request header to pull out method/path/headers.
        let (method, path, query, host, content_type, client_ip, headers) = {
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
                .or_else(|| req.headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()))
                .unwrap_or("")
                .to_string();
            let method = req.method.as_str().to_string();
            let path = req.uri.path().to_string();
            let query = req.uri.query().map(|q| q.to_string()).unwrap_or_default();
            let mut headers = HeaderMap::new();
            for (name, value) in req.headers.iter() {
                if let Ok(v) = value.to_str() {
                    headers.append(name.as_str(), v.to_string());
                }
            }
            // Mirror `:path` so transformer-in can backstop / restore it (stage ③⑭).
            headers.insert(hdr::PATH, path.clone());
            (method, path, query, host, content_type, client_ip, headers)
        };

        // (ii) Read the full body (POST/PUT/PATCH only) up to the cap.
        let has_body = matches!(method.as_str(), "POST" | "PUT" | "PATCH");
        let mut buf: Vec<u8> = Vec::new();
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

        Ok(InboundRequest {
            method,
            path,
            query,
            headers,
            body: Bytes::from(buf),
            content_type,
            client_ip,
            host,
        })
    }
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
    ) -> PingoraResult<()> {
        const SKIP: &[&str] = &[
            "server",
            "via",
            "transfer-encoding",
            "content-length",
            "connection",
            "content-encoding",
        ];
        let status = resp.status().as_u16();
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
        // LLM / SSE responses are long-lived — do not reuse the downstream conn.
        session.as_downstream_mut().set_keepalive(None);
        session
            .write_response_header(Box::new(resp_header), false)
            .await?;

        // ⑪ stream body; count SSE usage + TTFT (first chunk).
        let mut usage = UsageSnapshot::new(UsageSchema::Generic);
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
            let _ = sink.push(&metrics).await;
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

/// Write a short JSON error and return `Ok(true)` (short-circuit the pipeline).
async fn short_circuit(session: &mut Session, status: u16, reason: &str) -> PingoraResult<bool> {
    let body = Bytes::from(format!(
        "{{\"error\":{{\"message\":\"{reason}\",\"type\":\"proxy_error\"}}}}"
    ));
    session.set_keepalive(None);
    session.respond_error_with_body(status, body).await?;
    Ok(true)
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
}
