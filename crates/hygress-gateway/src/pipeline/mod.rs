//! The pure data-plane pipeline (design §6.1 ①–⑨, net semantics).
//!
//! Every stage is a **pure function** over `hygress-core` types and the gateway's
//! value types ([`InboundRequest`] / [`OutboundRequest`] / …) — no I/O, no Pingora
//! `Session`, no [`crate::context::GatewayState`]. The async forward stage (⑩–⑮,
//! in [`crate::pipe`], `integrations`-gated) consumes a [`prepare`]d
//! [`PreparedRequest`] and builds each per-candidate outbound request via
//! [`build_outbound`] (⑧ model-mapper + ⑨ set-instance/route-name +
//! transformer-outbound + Host).
//!
//! ## Net-semantics ordering vs. this decomposition
//!
//! The Higress net order is ①②③④⑤⑥⑦⑧⑨. The security-critical invariants are:
//! ① (inbound untrusted-strip) and ② (body→`x-higress-llm-model` resolution) must
//! precede ⑤ (ext-auth) so auth sees the resolved model. [`prepare`] executes
//! ①②③④⑦ (plus the ⑥ cap check); the pipe then runs ⑤ (async forward-auth,
//! `integrations`-gated) and ⑧⑨+⑩ via [`build_outbound`]. ⑦ (registry/SWRR) does
//! not consume ⑤'s output, so executing it in `prepare` (before ⑤) yields an
//! identical result while keeping the whole decision path pure and unit-testable.
//!
//! Stages (each a module with its own unit tests, core-only):
//! - [`model_router`]      ②  body/alias model resolution + `x-higress-llm-model` overwrite + cap
//! - [`transformer`]       ①③ inbound untrusted-strip / rename / original-path backstop; outbound keep
//! - [`route_match`]       ④  `x-higress-llm-model` + full-match path → Main route, else mirror
//! - [`auth`]              ⑤  scope = origin ingress name `ai-route-route-` prefix (pure) + FAIL_OPEN glue (P5)
//! - [`registry_resolve`]  ⑦  `name.type:port` → static/dns/proxy/tunnel connect target
//! - [`swrr_select`]       ⑦  Nginx SWRR weighted order over the per-route-group shared state
//! - [`model_mapper`]      ⑧  per-destination (`name.type`) outbound body `model` rewrite
//! - [`set_pre_route_headers`] ⑨ write `X-GPUStack-Model-Instance` + `X-GPUStack-Route-Name`
//! - [`fallback`]          ⑭  `x-higress-fallback-from` match, original-path restore, max-10 guard

use hygress_core::prelude::{
    ConfigData, HeaderMap, ProviderToken, RouteMatch, RouteTable, provider_bearer,
};

use crate::context::{
    CandidateTarget, InboundRequest, ModelRouterConfig, OutboundRequest, PreparedRequest,
    RouteInfo, SharedConfigHandle,
};
use crate::error::GatewayError;

pub mod auth;
pub mod fallback;
pub mod model_mapper;
pub mod model_router;
pub mod registry_resolve;
pub mod route_match;
pub mod set_pre_route_headers;
pub mod swrr_select;
pub mod transformer;

/// Per-request borrow-only context for the pure pipeline (①–⑦). `data` and
/// `table` are built from the **same** snapshot by the caller (the pipe) so the
/// route match and the registry/mapping lookups never drift across a hot reload.
pub struct PipelineCtx<'a> {
    /// The control-plane snapshot (`routes` / `registries` / `proxies` / `features`).
    pub data: &'a ConfigData,
    /// The runtime route index built from `data` (full-match path predicates compiled).
    pub table: &'a RouteTable,
    /// The lock-free config holder (used only for the per-route-group SWRR state).
    pub config: &'a SharedConfigHandle,
    /// The `gpustack-model-router` stage config.
    pub router: &'a ModelRouterConfig,
}

/// Stages ①②③④⑦ (+ ⑥ cap) → a [`PreparedRequest`] for the async forward stage.
///
/// Route is matched by **initial** key (`x-higress-llm-model` → Main, else mirror).
/// Pure: no I/O. ⑤ forward-auth (async, egress) and ⑧⑨ (per-candidate, via
/// [`build_outbound`]) are intentionally excluded and run in the pipe.
pub fn prepare(inbound: &InboundRequest, ctx: &PipelineCtx) -> Result<PreparedRequest, GatewayError> {
    prepare_inner(inbound, ctx, route_match::match_initial)
}

/// Fallback re-dispatch (stage ⑭) — identical to [`prepare`] except the route is matched
/// via **`x-higress-fallback-from`** (a Fallback route, else the mirror) instead of
/// `x-higress-llm-model`. The pipe arms the `x-higress-fallback-from` /
/// `x-gpustack-fallback-path` headers and re-dispatches here on a final 4xx/5xx (or total
/// transport failure) when the selected route has a fallback link. Bounded by the stage-⑭
/// `max_redirects` guard (the pipe tracks the hop count).
pub fn prepare_fallback(
    inbound: &InboundRequest,
    ctx: &PipelineCtx,
) -> Result<PreparedRequest, GatewayError> {
    prepare_inner(inbound, ctx, route_match::match_fallback)
}

/// Shared body (①②③④⑦ + ⑥-cap) parameterized over the route-matching stage.
///
/// Keeping the whole decision path in one pure fn (with only the ④ matcher selected)
/// guarantees the initial and fallback re-dispatch share identical strip / model /
/// transform / registry / SWRR semantics.
fn prepare_inner(
    inbound: &InboundRequest,
    ctx: &PipelineCtx,
    match_route: fn(&RouteTable, &HeaderMap, &str) -> Option<RouteMatch>,
) -> Result<PreparedRequest, GatewayError> {
    use hygress_core::prelude::{FallbackSpec, RouteKind};

    let started_at_ms = now_millis();

    // ① strip untrusted inbound (unforgeable-by-client) headers — before anything else.
    let mut base_headers = inbound.headers.clone();
    base_headers.remove(crate::context::hdr::GPUSTACK_AUTH_TOKEN);
    base_headers.remove(crate::context::hdr::MODEL_INSTANCE);

    // ② model-router: resolve the model + enforce the body cap; OVERWRITE the
    //    configured `targetHeader` (and the body `model` field) when resolved.
    let mut body = inbound.body.clone();
    let mr = model_router::resolve(&inbound.path, &body, &inbound.content_type, ctx.router)?;
    if let Some(model) = &mr.model {
        base_headers.insert(ctx.router.target_header.as_str(), model);
        // The route-match key is canonically `x-higress-llm-model` (the Ingress
        // `higress.io/exact-match-header-x-higress-llm-model`). When the plugin's
        // `targetHeader` differs, keep the canonical key in sync too, so routing
        // follows the resolved model (a client-spoofed header is still
        // overwritten — the resolved value wins, contract-pin §2.3).
        if ctx.router.target_header != crate::context::hdr::LLM_MODEL {
            base_headers.insert(crate::context::hdr::LLM_MODEL, model);
        }
        if let Some(nb) = crate::body::rewrite_model_field(
            &body,
            Some(inbound.content_type.as_str()),
            &ctx.router.model_key,
            model,
        ) {
            body = nb;
        }
    }

    // ③ transformer-in: rename legacy model header, restore fallback path, backstop
    //    `:path` → `x-gpustack-original-path` (all pure core semantics).
    transformer::apply_inbound(&mut base_headers);

    // ④ route match: initial (`x-higress-llm-model`) or fallback (`x-higress-fallback-from`)
    //    exact-key AND full-match path; else the mirror catch-all; `None` → 404.
    let Some(matched) = match_route(ctx.table, &base_headers, &inbound.path) else {
        return Err(GatewayError::NoRoute(inbound.path.clone()));
    };
    let route = ctx.table.route(matched.index);

    // Rewrite capture: the matched predicate's groups → `rewrite-target` (e.g. `/$1$3`).
    let groups = matched
        .matched_predicate
        .and_then(|pi| route.path_predicates.get(pi))
        .map(|pi| route_match::capture_groups(pi, &inbound.path))
        .unwrap_or_default();
    let upstream_path = route.rewrite_path(&groups).unwrap_or_else(|| inbound.path.clone());

    // ⑦ registry resolve + SWRR weighted order over the per-route-group shared state.
    let ordered = swrr_select::order(ctx.config, route);
    let mut candidates = Vec::with_capacity(ordered.len());
    for d in &ordered {
        candidates.push(registry_resolve::resolve_destination(ctx.data, d)?);
    }
    if candidates.is_empty() {
        return Err(GatewayError::AllCandidatesFailed(route.key.clone()));
    }
    let selected_service = candidates[0].service_name.clone();

    let is_model_route = matches!(route.kind, RouteKind::Main | RouteKind::Fallback);
    let effective_model = base_headers
        .get(crate::context::hdr::LLM_MODEL)
        .unwrap_or(route.key.as_str())
        .to_string();

    // Usage attribution is model-route traffic only (mirror / passthrough never reports,
    // design §2.1.3 / pin §2.8). `mse_consumer` is empty here — filled by the pipe after ⑤.
    let usage = is_model_route.then(|| crate::context::UsageTarget {
        model: effective_model.clone(),
        route_name: route.ingress_name.clone(),
        mse_consumer: String::new(),
        organization_id: base_headers
            .get(crate::context::hdr::ORGANIZATION_ID)
            .unwrap_or("")
            .to_string(),
    });

    let route_info = RouteInfo {
        route_key: route.key.clone(),
        ingress_name: route.ingress_name.clone(),
        matched_by: matched.matched_by,
        is_model_route,
        model: effective_model,
        auth_required: auth::required(route),
        retry: route.retry.clone(),
        fallback: FallbackSpec::from_route(route),
        matched_predicate: matched.matched_predicate,
        path_groups: groups,
    };

    Ok(PreparedRequest {
        candidates,
        route: route_info,
        base_headers,
        upstream_path,
        query: inbound.query.clone(),
        body,
        content_type: inbound.content_type.clone(),
        model_mapping: route.model_mapping.clone(),
        usage,
        selected_service,
        started_at_ms,
    })
}

/// Per-candidate stages ⑧ (model-mapper) + ⑨ (set-instance/route-name) + the
/// transformer-outbound keep + hop-by-hop strip + Host/`path` build (⑩ request
/// construction). Pure — called by the pipe for the selected candidate and, on
/// failover, for each fallback candidate (the body is `Bytes`, so re-mapping is
/// an O(1) clone).
pub fn build_outbound(
    method: &str,
    prepared: &PreparedRequest,
    candidate: &CandidateTarget,
    auth_writeback: &HeaderMap,
    provider_tokens: &[ProviderToken],
) -> OutboundRequest {
    // Base headers + the forward-auth write-back (⑤; empty when fail-open /
    // mirror). The verdict's values **REPLACE** the inbound (client) values —
    // the gateway SETS the upstream credentials: `Authorization` =
    // `Bearer <registration_token>`, `cookie`, `X-Mse-Consumer`,
    // `x-gpustack-auth-cache` (contract-pin §2.1 / §5.3: the
    // `allowed_upstream_headers` are written back, not merged). `insert`
    // replaces any pre-existing value so the client's key can never leak
    // upstream (B4: appending would leave the client `Authorization` visible
    // to the worker).
    let mut headers = prepared.base_headers.clone();
    for name in auth_writeback.names().collect::<Vec<&str>>() {
        for value in auth_writeback.get_all(name) {
            headers.insert(name, value.clone());
        }
    }

    // ⑧ model-mapper: rewrite the outbound body `model` field for this candidate's
    //    `name.type` (the selected instance's service identity).
    let mut out_body = prepared.body.clone();
    model_mapper::apply(
        &prepared.model_mapping,
        &candidate.service_name,
        &mut out_body,
        &prepared.content_type,
    );

    // ⑨ set-model-pre-route: the selected instance cluster name + the route
    //    name — **model-route traffic only** (NB6). The mirror `/` and any
    //    non-model passthrough never carry `X-GPUStack-Model-Instance` /
    //    `X-GPUStack-Route-Name`: those identify a concrete model worker
    //    instance (contract-pin §2.5), and GPUStack-self mirror traffic is
    //    served by the server itself (no instance to point at).
    if prepared.route.is_model_route {
        set_pre_route_headers::apply(
            &mut headers,
            &candidate.service_name,
            &prepared.route.ingress_name,
        );
    }

    // Transformer-outbound: dedupe (keep) the instance / route-name headers the
    //    egress must not strip.
    transformer::apply_outbound(&mut headers);

    // Strip hop-by-hop / connection-management headers before forwarding.
    for h in HOP_BY_HOP {
        headers.remove(h);
    }

    // D6 / §7 ai-proxy: a **provider-destined** candidate (`name.type` starts
    // `provider-`) gets its `Authorization` swapped to the provider's
    // `apiToken` (keyed by the candidate's `name.type` service, no port, or a
    // per-ingress match). This **replaces** the ext-auth write-back credential
    // so the request reaches the provider with the provider's key — never the
    // client / registration key. Non-provider candidates keep the write-back
    // behavior unchanged (the swap is a no-op for them). The real
    // [`hygress_egress::provider::ProviderClient`] performs the identical swap on
    // the live (integrations) forward path; this is the pure, unit-tested model.
    if candidate.service_name.starts_with("provider-") {
        if let Some(token) =
            provider_bearer(provider_tokens, &candidate.service_name, &prepared.route.ingress_name)
        {
            headers.insert(crate::context::hdr::AUTHORIZATION, format!("Bearer {token}"));
        }
    }

    let path = if prepared.query.is_empty() {
        prepared.upstream_path.clone()
    } else {
        format!("{}?{}", prepared.upstream_path, prepared.query)
    };

    OutboundRequest {
        method: method.to_string(),
        path,
        host: host_from_address(&candidate.address),
        headers,
        body: out_body,
        content_type: prepared.content_type.clone(),
    }
}

/// Hop-by-hop / connection-management headers never forwarded to the upstream.
pub const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

/// `host[:port]` (or `[v6]:port`) → the `Host` header value (host part).
pub fn host_from_address(address: &str) -> String {
    if let Some(close) = address.find(']') {
        let inner = &address[..close];
        return inner.trim_start_matches('[').to_string();
    }
    address
        .rsplit_once(':')
        .map(|(h, _)| h.to_string())
        .unwrap_or_else(|| address.to_string())
}

/// Unix millis since the epoch (usage `started_at` / timing).
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{hdr, CandidateTarget, Scheme};
    use hygress_core::prelude::MatchKind;

    fn candidate() -> CandidateTarget {
        CandidateTarget {
            service: "model-1-10.static:80".into(),
            service_name: "model-1-10.static".into(),
            address: "10.0.0.5:8081".into(),
            proxied: false,
            scheme: Scheme::Http,
            proxy: None,
        }
    }

    fn prepared(is_model_route: bool, ingress: &str) -> PreparedRequest {
        PreparedRequest {
            candidates: vec![candidate()],
            route: RouteInfo {
                route_key: ingress.into(),
                ingress_name: ingress.into(),
                matched_by: if is_model_route {
                    MatchKind::HeaderExact
                } else {
                    MatchKind::Mirror
                },
                is_model_route,
                model: "org1/llama-3-8b".into(),
                auth_required: is_model_route,
                retry: Default::default(),
                fallback: None,
                matched_predicate: None,
                path_groups: vec![],
            },
            base_headers: HeaderMap::new(),
            upstream_path: "/v1/chat/completions".into(),
            query: String::new(),
            body: bytes::Bytes::new(),
            content_type: "application/json".into(),
            model_mapping: Default::default(),
            usage: None,
            selected_service: "model-1-10.static".into(),
            started_at_ms: 0,
        }
    }

    // ----- B4: the auth write-back REPLACES (never appends) -----

    #[test]
    fn auth_writeback_replaces_client_credentials() {
        let mut p = prepared(true, "higress-system/ai-route-route-1.internal");
        p.base_headers.insert(hdr::AUTHORIZATION, "Bearer sk-client");
        p.base_headers.insert(hdr::COOKIE, "client=1");
        let wb = HeaderMap::from_iter([
            (hdr::AUTHORIZATION, "Bearer reg-token".to_string()),
            (hdr::COOKIE, "dummy=dummy".to_string()),
            (hdr::MSE_CONSUMER, "ak.gpustack-7".to_string()),
            (hdr::AUTH_CACHE, "jwt-cache".to_string()),
        ]);
        let out = build_outbound("POST", &p, &candidate(), &wb, &[]);
        // Exactly one Authorization — the registration token (the client key is gone).
        assert_eq!(out.headers.get(hdr::AUTHORIZATION), Some("Bearer reg-token"));
        assert_eq!(out.headers.count(hdr::AUTHORIZATION), 1);
        // Exactly one cookie — the auth service's dummy cookie.
        assert_eq!(out.headers.get(hdr::COOKIE), Some("dummy=dummy"));
        assert_eq!(out.headers.count(hdr::COOKIE), 1);
        assert_eq!(out.headers.get(hdr::MSE_CONSUMER), Some("ak.gpustack-7"));
        assert_eq!(out.headers.get(hdr::AUTH_CACHE), Some("jwt-cache"));
    }

    #[test]
    fn empty_writeback_keeps_client_headers() {
        // Fail-open (no verdict): the request proceeds with the inbound headers
        // untouched (no replacement happens when the verdict provides no value).
        let mut p = prepared(true, "higress-system/ai-route-route-1.internal");
        p.base_headers.insert(hdr::AUTHORIZATION, "Bearer keep");
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        assert_eq!(out.headers.get(hdr::AUTHORIZATION), Some("Bearer keep"));
        assert_eq!(out.headers.count(hdr::AUTHORIZATION), 1);
    }

    // ----- D6 / §7: provider-destined key-swap -----

    fn provider_candidate() -> CandidateTarget {
        CandidateTarget {
            service: "provider-1.proxy:443".into(),
            service_name: "provider-1.proxy".into(),
            address: "10.0.0.9:443".into(),
            proxied: false,
            scheme: Scheme::Https,
            proxy: None,
        }
    }

    fn provider_tokens_global_and_scoped() -> Vec<ProviderToken> {
        use hygress_core::prelude::ProviderToken;
        vec![
            ProviderToken {
                service: "provider-1.proxy".into(),
                ingress_scope: None,
                api_tokens: vec!["sk-provider-1".into()],
            },
            ProviderToken {
                service: "provider-1.proxy".into(),
                ingress_scope: Some("ai-route-route-7.internal".into()),
                api_tokens: vec!["sk-provider-1-scoped".into()],
            },
        ]
    }

    #[test]
    fn provider_destination_swaps_authorization_to_provider_token() {
        // A provider-destined candidate, no write-back: the provider apiToken
        // becomes the Authorization (the client/registration key never reaches it).
        let p = prepared(true, "higress-system/ai-route-route-1.internal");
        let tokens = provider_tokens_global_and_scoped();
        let out = build_outbound(
            "POST",
            &p,
            &provider_candidate(),
            &HeaderMap::new(),
            &tokens,
        );
        assert_eq!(out.headers.get(hdr::AUTHORIZATION), Some("Bearer sk-provider-1"));
        assert_eq!(out.headers.count(hdr::AUTHORIZATION), 1);
    }

    #[test]
    fn provider_swap_replaces_registration_token() {
        // The ext-auth write-back set the registration token, but a provider
        // destination overrides it with the provider apiToken (exactly one
        // Authorization — the provider key).
        let mut p = prepared(true, "higress-system/ai-route-route-1.internal");
        p.base_headers.insert(hdr::AUTHORIZATION, "Bearer sk-client");
        let tokens = provider_tokens_global_and_scoped();
        let wb = HeaderMap::from_iter([(hdr::AUTHORIZATION, "Bearer reg-token".to_string())]);
        let out = build_outbound("POST", &p, &provider_candidate(), &wb, &tokens);
        assert_eq!(out.headers.get(hdr::AUTHORIZATION), Some("Bearer sk-provider-1"));
        assert_eq!(out.headers.count(hdr::AUTHORIZATION), 1);
    }

    #[test]
    fn provider_swap_prefers_ingress_scoped_token() {
        // The ingress name of the route matches a scoped token → it wins over the
        // global one (per-ingress match).
        let p = prepared(true, "higress-system/ai-route-route-7.internal");
        let tokens = provider_tokens_global_and_scoped();
        let out = build_outbound(
            "POST",
            &p,
            &provider_candidate(),
            &HeaderMap::new(),
            &tokens,
        );
        assert_eq!(out.headers.get(hdr::AUTHORIZATION), Some("Bearer sk-provider-1-scoped"));
    }

    #[test]
    fn non_provider_destination_keeps_writeback() {
        // A model-instance (non-provider) destination is NOT provider-destined:
        // the write-back Authorization is kept unchanged even if provider tokens
        // exist for an unrelated service.
        let p = prepared(true, "higress-system/ai-route-route-1.internal");
        let tokens = provider_tokens_global_and_scoped();
        let wb = HeaderMap::from_iter([(hdr::AUTHORIZATION, "Bearer reg-token".to_string())]);
        let out = build_outbound("POST", &p, &candidate(), &wb, &tokens);
        assert_eq!(out.headers.get(hdr::AUTHORIZATION), Some("Bearer reg-token"));
    }

    #[test]
    fn provider_destination_with_no_matching_token_keeps_writeback() {
        // A provider destination with no matching token (service not in the list)
        // falls back to the write-back credential (no swap).
        let p = prepared(true, "higress-system/ai-route-route-1.internal");
        let other = vec![ProviderToken {
            service: "provider-2.dns".into(),
            ingress_scope: None,
            api_tokens: vec!["sk-provider-2".into()],
        }];
        let wb = HeaderMap::from_iter([(hdr::AUTHORIZATION, "Bearer reg-token".to_string())]);
        let out = build_outbound("POST", &p, &provider_candidate(), &wb, &other);
        assert_eq!(out.headers.get(hdr::AUTHORIZATION), Some("Bearer reg-token"));
    }

    // ----- NB6: instance / route-name headers are model-route only -----

    #[test]
    fn instance_headers_present_for_model_route() {
        let p = prepared(true, "higress-system/ai-route-route-1.internal");
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        assert_eq!(out.headers.get(hdr::MODEL_INSTANCE_OUT), Some("model-1-10.static"));
        assert_eq!(
            out.headers.get(hdr::ROUTE_NAME_OUT),
            Some("higress-system/ai-route-route-1.internal")
        );
    }

    #[test]
    fn instance_headers_absent_for_mirror_and_passthrough() {
        let p = prepared(false, "gpustack");
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        assert_eq!(out.headers.get(hdr::MODEL_INSTANCE_OUT), None);
        assert_eq!(out.headers.get(hdr::ROUTE_NAME_OUT), None);
    }

    // ----- B2: the configured targetHeader receives the resolved model -----

    #[test]
    fn resolved_model_is_written_to_configured_target_header() {
        use crate::context::ModelRouterConfig;
        use bytes::Bytes;
        use hygress_core::prelude::{Destination, PathPred, Registry, RouteKind, RouteRule};

        // A Main route keyed to the model the body will resolve to, and the
        // snapshot's `model_router` settings with a NON-canonical targetHeader.
        let data = ConfigData {
            routes: vec![RouteRule::new(
                "org1/llama-3-8b",
                RouteKind::Main,
                vec![PathPred::new(".*")],
                vec![Destination::new("model-1-10.static:80")],
            )
            .unwrap()],
            registries: vec![Registry::new("model-1-10.static:80", "10.0.0.5:8081").unwrap()],
            model_router: hygress_core::prelude::ModelRouterSettings {
                target_header: "x-custom-model".into(),
                enable_on_path_suffix: vec!["/v1/chat/completions".into()],
                ..Default::default()
            },
            ..ConfigData::default()
        };
        // The snapshot-derived config (B2 glue) carries the custom target header.
        let router = ModelRouterConfig::from_settings(&data.model_router);
        assert_eq!(router.target_header, "x-custom-model");
        assert_eq!(router.enable_on_path_suffix, vec!["/v1/chat/completions"]);

        let table = RouteTable::rebuild(&data).unwrap();
        let shared = SharedConfigHandle::new(
            hygress_core::SharedConfig::new(data.clone()).unwrap(),
        );
        let ctx = PipelineCtx {
            data: &data,
            table: &table,
            config: &shared,
            router: &router,
        };
        let inbound = InboundRequest {
            method: "POST".into(),
            path: "/v1/chat/completions".into(),
            query: String::new(),
            headers: HeaderMap::new(),
            body: Bytes::from(r#"{"model":"org1/llama-3-8b"}"#),
            content_type: "application/json".into(),
            client_ip: String::new(),
            host: String::new(),
        };
        let p = prepare(&inbound, &ctx).unwrap();
        // ② wrote the resolved model to the configured targetHeader ...
        assert_eq!(p.base_headers.get("x-custom-model"), Some("org1/llama-3-8b"));
        // ... and kept the canonical routing key in sync, so ④ matched the
        // Main route (not the mirror).
        assert_eq!(p.base_headers.get(hdr::LLM_MODEL), Some("org1/llama-3-8b"));
        assert!(p.route.is_model_route);
        assert_eq!(p.route.model, "org1/llama-3-8b");
    }
}
