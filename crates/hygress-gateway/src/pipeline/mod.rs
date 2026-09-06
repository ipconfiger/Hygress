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
//! - [`auth`]              ⑤  scope = origin ingress name `ai-route-route-` prefix (pure; the exchange lives in `pipe` under `integrations`)
//! - [`registry_resolve`]  ⑦  `name.type:port` → static/dns/proxy/tunnel connect target
//! - [`swrr_select`]       ⑦  Nginx SWRR weighted order over the per-route-group shared state
//! - [`model_mapper`]      ⑧  per-destination (`name.type`) outbound body `model` rewrite
//! - [`set_pre_route_headers`] ⑨ write `X-GPUStack-Model-Instance` + `X-GPUStack-Route-Name`
//! - [`fallback`]          ⑭  `x-higress-fallback-from` match, original-path restore, max-10 guard

use hygress_core::prelude::{
    provider_bearer, ConfigData, HeaderMap, OutboundHeaders, ProviderToken, RouteMatch, RouteTable,
    Transformer,
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
pub mod routing_policy;
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
pub fn prepare(
    inbound: &InboundRequest,
    ctx: &PipelineCtx,
) -> Result<PreparedRequest, GatewayError> {
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
    // AM-6: the whole mutation phase below is exactly ONE `make_mut` deep copy.
    // `clone` is an O(1) Arc bump; the FIRST mutating call (the remove below)
    // deep-copies while `inbound.headers` still shares the Arc; every later
    // mutation (`remove` / `insert` / the transformer-in rules) runs in place on
    // the now-exclusively-owned map — nothing re-shares `base_headers` between
    // mutations, so no second copy can trigger.
    let mut base_headers = inbound.headers.clone();
    base_headers.remove(crate::context::hdr::GPUSTACK_AUTH_TOKEN);
    base_headers.remove(crate::context::hdr::MODEL_INSTANCE);

    // ② model-router: resolve the model + enforce the body cap; OVERWRITE the
    //    configured `targetHeader` (and the body `model` field) when resolved.
    let mut body = inbound.body.clone();
    let content_type = inbound.content_type.as_str();

    // ORA3-M14: ONE prepare-time no-DOM top-level scan for a well-formed JSON
    // body — replacing up to three overlapping traversals (the model-router's
    // body extraction in ②, R-5's `model_field_equals`, and the
    // rewrite/extract fallback) — and capturing the AM-2 `stream` /
    // `stream_options` flags + closing-brace offset on the same pass. The scan
    // runs ONCE per request over the ORIGINAL body (before any model-value
    // splice), so its model / stream verdicts remain valid for every
    // downstream consumer. Multipart / empty / non-JSON / malformed bodies get
    // `None` here and fall back to the classic per-step scans below
    // (byte-identical to the pre-fusion path).
    let profile = if crate::body::is_json(Some(content_type)) && !body.is_empty() {
        crate::body::scan_top_level_profile(&body, &ctx.router.model_key).ok()
    } else {
        None
    };
    let scanned_model = profile
        .as_ref()
        .and_then(|p| p.model.as_ref().map(|(decoded, _)| decoded.as_str()));

    // ② resolution: when the body was scanned above, the fused entry consumes
    // the scan (cap + path-mode decision only — no body re-scan); otherwise
    // the classic scanning entry keeps its exact behavior for multipart /
    // non-JSON / malformed bodies.
    let mr = if profile.is_some() {
        model_router::resolve_fused(&inbound.path, &body, ctx.router, scanned_model)?
    } else {
        model_router::resolve(&inbound.path, &body, content_type, ctx.router)?
    };

    // B4: the body's model value as of the end of prepare — the carrier for the
    // per-candidate model-mapper identity short-circuit. When prepare rewrote
    // the body, use the resolved value (no re-scan); otherwise reuse the
    // prepare-time scan, `None` covering missing / non-string / malformed
    // (each candidate then skips its body-mapper scan entirely).
    let body_model = if let Some(model) = &mr.model {
        base_headers.insert(ctx.router.target_header.as_str(), model);
        // The route-match key is canonically `x-higress-llm-model` (the Ingress
        // `higress.io/exact-match-header-x-higress-llm-model`). When the plugin's
        // `targetHeader` differs, keep the canonical key in sync too, so routing
        // follows the resolved model (a client-spoofed header is still
        // overwritten — the resolved value wins, contract-pin §2.3).
        if ctx.router.target_header != crate::context::hdr::LLM_MODEL {
            base_headers.insert(crate::context::hdr::LLM_MODEL, model);
        }
        match &profile {
            // Well-formed JSON object (the prepare scan ran): R-5 identity
            // check and the model rewrite are pure compare / offset-splice
            // operations on the scan result — no re-scan.
            Some(p) => match &p.model {
                Some((decoded, span)) if decoded == model => Some(model.clone()),
                Some((_, span)) => {
                    // R-5 rewrite: splice the located value token in place.
                    body = crate::body::splice_json_string_at(&body, *span, model);
                    Some(model.clone()) // rewritten → the body now carries `mr.model`
                }
                // Absent / non-string model member: there is no string token to
                // splice; the classic R-5 chain would no-op and re-extract
                // `None` — skip those re-scans (ORA3-M14).
                None => None,
            },
            // Multipart / non-JSON / empty / malformed body (not scanned): the
            // classic R-5 decision chain, byte-for-byte as before.
            None => {
                let content_type = Some(content_type);
                // R-5 (identity short-circuit): when the body's `model` field
                // already equals the resolved value, do NOT splice — skip the
                // full-body rewrite (alloc + copy) and record the body as
                // carrying `model`.
                if crate::body::model_field_equals(
                    &body,
                    content_type,
                    &ctx.router.model_key,
                    model,
                ) {
                    Some(model.clone())
                } else if let Some(nb) = crate::body::rewrite_model_field(
                    &body,
                    content_type,
                    &ctx.router.model_key,
                    model,
                ) {
                    body = nb;
                    Some(model.clone()) // rewritten → the body now carries `mr.model`
                } else {
                    // Rewrite no-op'd (no string model field to splice): the body
                    // is unchanged, so its current model value is the extracted
                    // one.
                    crate::body::extract_model(&body, content_type, &ctx.router.model_key)
                }
            }
        }
    } else {
        // No resolved model (path/header-driven): the body was not rewritten.
        // Record its model value from the prepare scan when it ran (amortized
        // over every candidate), else extract it as before.
        match &profile {
            Some(p) => p.model.as_ref().map(|(decoded, _)| decoded.clone()),
            None => crate::body::extract_model(
                &body,
                Some(content_type),
                &ctx.router.model_key,
            ),
        }
    };

    // ③ transformer-in: rename legacy model header, restore fallback path, backstop
    //    `:path` → `x-gpustack-original-path` (all pure core semantics).
    transformer::apply_inbound(&mut base_headers);

    // ④ route match: initial (`x-higress-llm-model`) or fallback (`x-higress-fallback-from`)
    //    exact-key AND full-match path; else the mirror catch-all; `None` → 404.
    let Some(matched) = match_route(ctx.table, &base_headers, &inbound.path) else {
        return Err(GatewayError::NoRoute(inbound.path.clone()));
    };
    let route = ctx.table.route(matched.index);

    // Rewrite capture: the matched predicate's groups → `rewrite-target`
    // (e.g. `/$1$3`). R-6: only computed when the route actually defines a
    // rewrite target, and served from the route table's ALREADY-COMPILED
    // regex (no per-request `RegexBuilder::build`).
    let groups = if route.rewrite_target.is_some() {
        matched
            .matched_predicate
            .map(|pi| {
                ctx.table
                    .capture_groups_for(matched.index, pi, &inbound.path)
            })
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    let upstream_path = route
        .rewrite_path(&groups)
        .unwrap_or_else(|| inbound.path.clone());

    // ⑦ registry resolve + SWRR weighted order over the per-route-group shared state.
    // The group key / candidates come precomputed from the (cached) route table
    // and the registry index is read from the SAME table (M7 / M8) — the route
    // match, the candidates, and the registry targets all come from one atomic
    // snapshot read (no cross-snapshot drift window).
    let ordered = swrr_select::order_route(ctx.config, ctx.table, matched.index);
    let registry_index = ctx.table.registry_index();
    let mut candidates = Vec::with_capacity(ordered.len());
    for d in &ordered {
        candidates.push(registry_resolve::resolve_index(registry_index, d)?);
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
        body_model,
        content_type: inbound.content_type.clone(),
        model_mapping: route.model_mapping.clone(),
        usage,
        selected_service,
        started_at_ms,
        // Routing-policy overrides (design §4.3) are applied by the pipe after
        // `route_match` (the pure pipeline cannot know the matched route's
        // policy); they start absent.
        override_timeout_ms: None,
        override_retries: None,
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
    // AM-6b: the candidate's headers are a LAZY OVERLAY over the shared base
    // (`OutboundHeaders::new` is an O(1) Arc bump — the ~14-entry base payload
    // is NEVER deep-copied per candidate). The deltas below are recorded in
    // order and materialized exactly once, at the dial (`into_pairs`) or at the
    // provider egress boundary (`materialize`). Reads (`get` / `contains`) and
    // the materialized result are byte-identical to the AM-6 clone-then-mutate
    // `HeaderMap` for the same operation sequence.
    let mut headers = OutboundHeaders::new(prepared.base_headers.clone());
    // AM-6: iterate `names()` directly (no `Vec<&str>` collect — `auth_writeback`
    // is only ever borrowed immutably here, so there is no borrow conflict to
    // work around).
    for name in auth_writeback.names() {
        for value in auth_writeback.get_all(name) {
            headers.insert(name, value.clone());
        }
    }

    // ⑧ model-mapper: rewrite the outbound body `model` field for this
    //    candidate's `name.type` (the selected instance's service identity).
    //    The body's current model value was recorded once at prepare
    //    (`body_model`, B4): None → skip the per-candidate scan entirely;
    //    identity mapping → reuse the Bytes reference (no scan, no splice);
    //    otherwise one bounded rewrite.
    let mut out_body = prepared.body.clone();
    model_mapper::apply_with_current(
        &prepared.model_mapping,
        &candidate.service_name,
        &mut out_body,
        &prepared.content_type,
        prepared.body_model.as_deref(),
    );

    // AM-2 (pin §2.8): force `stream_options.include_usage` on streaming
    // OpenAI-completions bodies of model-route traffic, so the upstream emits
    // the canonical final usage chunk (ai-proxy parity; Higress #4258/#2524).
    // Single-point injection: both the registry destination and the
    // provider-destined send paths consume `outbound.body`, so this covers
    // every metered upstream. `None` keeps `out_body` untouched (R-5
    // zero-allocation short path); a client-supplied `stream_options` is never
    // overridden.
    //
    // ORA3-M18 (GX-3): this injection is a DOCUMENTED SUPERSET of upstream
    // Higress ai-proxy, which only injects for OpenAI-protocol, non-generic
    // providers (`apiName` in chat/completions|completions). Hygress injects
    // for ALL model-route chat/completions|completions streams, with no
    // per-destination protocol discrimination — GPUStack-managed destinations
    // always speak the OpenAI protocol, so no generic / strict engine (vLLM
    // < 0.4.3 class, which 400s on unknown `stream_options`) is reachable
    // through a model route today (README/equivalence updated by the docs
    // agent). Revisit if generic or non-OpenAI destinations are ever routed
    // through model routes. Deliberately no behavior change.
    //
    // ORA3-M14 (PX-1): prepare's fused scan already validated this JSON top
    // level once per request and produced the stream flags + closing brace
    // (see `crate::body::scan_top_level_profile`); carrying that memo into
    // this per-candidate step needs a memo field on `PreparedRequest`
    // (context lane) — until then this gate re-derives the flags from the
    // candidate's FINAL bytes (post-⑧), which is the byte-exact single point
    // of injection. The profile verdicts are proven equal to this scan
    // (`profile_and_specialized_scanners_agree_*` in body.rs), so adopting the
    // memo later cannot change the outbound bytes.
    if let Some(nb) = crate::body::ensure_stream_include_usage(
        &out_body,
        Some(prepared.content_type.as_str()),
        &prepared.upstream_path,
        prepared.route.is_model_route,
    ) {
        out_body = nb;
    }

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
    //    egress must not strip. AM-6b: runs the CORE rule engine directly over
    //    the overlay (the core `Transformer::apply` is generic over
    //    `HeaderOps`); the gateway `transformer::apply_outbound` wrapper —
    //    `Transformer::outbound().apply(&mut HeaderMap)` — is exactly this for
    //    a materialized map, so the semantics are identical by construction.
    Transformer::outbound().apply(&mut headers);

    // Strip hop-by-hop / connection-management headers before forwarding.
    // AM-6b: the overlay records a removal ONLY when the name is present (base
    // or overlay), so the presence guard keeps an absent-name remove from
    // allocating a suppression record for nothing; when a header IS present the
    // outcome is byte-identical to the AM-6 unguarded remove.
    for h in HOP_BY_HOP {
        if headers.contains(h) {
            headers.remove(h);
        }
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
        if let Some(token) = provider_bearer(
            provider_tokens,
            &candidate.service_name,
            &prepared.route.ingress_name,
        ) {
            headers.insert(
                crate::context::hdr::AUTHORIZATION,
                format!("Bearer {token}"),
            );
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
            body_model: None,
            content_type: "application/json".into(),
            model_mapping: Default::default(),
            usage: None,
            selected_service: "model-1-10.static".into(),
            started_at_ms: 0,
            override_timeout_ms: None,
            override_retries: None,
        }
    }

    // ----- B4: the auth write-back REPLACES (never appends) -----

    #[test]
    fn auth_writeback_replaces_client_credentials() {
        let mut p = prepared(true, "higress-system/ai-route-route-1.internal");
        p.base_headers
            .insert(hdr::AUTHORIZATION, "Bearer sk-client");
        p.base_headers.insert(hdr::COOKIE, "client=1");
        let wb = HeaderMap::from_iter([
            (hdr::AUTHORIZATION, "Bearer reg-token".to_string()),
            (hdr::COOKIE, "dummy=dummy".to_string()),
            (hdr::MSE_CONSUMER, "ak.gpustack-7".to_string()),
            (hdr::AUTH_CACHE, "jwt-cache".to_string()),
        ]);
        let out = build_outbound("POST", &p, &candidate(), &wb, &[]);
        // Exactly one Authorization — the registration token (the client key is gone).
        assert_eq!(
            out.headers.get(hdr::AUTHORIZATION),
            Some("Bearer reg-token")
        );
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
        assert_eq!(
            out.headers.get(hdr::AUTHORIZATION),
            Some("Bearer sk-provider-1")
        );
        assert_eq!(out.headers.count(hdr::AUTHORIZATION), 1);
    }

    #[test]
    fn provider_swap_replaces_registration_token() {
        // The ext-auth write-back set the registration token, but a provider
        // destination overrides it with the provider apiToken (exactly one
        // Authorization — the provider key).
        let mut p = prepared(true, "higress-system/ai-route-route-1.internal");
        p.base_headers
            .insert(hdr::AUTHORIZATION, "Bearer sk-client");
        let tokens = provider_tokens_global_and_scoped();
        let wb = HeaderMap::from_iter([(hdr::AUTHORIZATION, "Bearer reg-token".to_string())]);
        let out = build_outbound("POST", &p, &provider_candidate(), &wb, &tokens);
        assert_eq!(
            out.headers.get(hdr::AUTHORIZATION),
            Some("Bearer sk-provider-1")
        );
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
        assert_eq!(
            out.headers.get(hdr::AUTHORIZATION),
            Some("Bearer sk-provider-1-scoped")
        );
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
        assert_eq!(
            out.headers.get(hdr::AUTHORIZATION),
            Some("Bearer reg-token")
        );
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
        assert_eq!(
            out.headers.get(hdr::AUTHORIZATION),
            Some("Bearer reg-token")
        );
    }

    // ----- NB6: instance / route-name headers are model-route only -----

    #[test]
    fn instance_headers_present_for_model_route() {
        let p = prepared(true, "higress-system/ai-route-route-1.internal");
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        assert_eq!(
            out.headers.get(hdr::MODEL_INSTANCE_OUT),
            Some("model-1-10.static")
        );
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

    // ----- AM-2 (pin §2.8): streaming `include_usage` forced on -----

    #[test]
    fn model_route_stream_body_gets_include_usage_injected() {
        // A metered model-route + OpenAI completions shape + top-level
        // `stream:true` → the outbound body carries the forced option exactly
        // once (both the registry and the provider send paths consume
        // `outbound.body`, so this single injection point covers all).
        let mut p = prepared(true, "higress-system/ai-route-route-1.internal");
        p.body = bytes::Bytes::from(r#"{"model":"org1/llama-3-8b","stream":true}"#);
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        let got = String::from_utf8(out.body.to_vec()).unwrap();
        assert_eq!(
            got,
            r#"{"model":"org1/llama-3-8b","stream":true,"stream_options":{"include_usage":true}}"#
        );
        // Only one `stream_options` object in the whole outbound body.
        assert_eq!(got.matches("\"stream_options\"").count(), 1, "body: {got}");
    }

    #[test]
    fn mirror_body_is_passthrough_never_include_usage_injected() {
        // Non-model (mirror) traffic is forwarded byte-for-byte — usage is not
        // metered there and the target may not understand `stream_options`.
        let mut p = prepared(false, "gpustack");
        let body = bytes::Bytes::from(r#"{"model":"x","stream":true}"#);
        p.body = body.clone();
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        assert_eq!(out.body, body);
        assert_eq!(
            String::from_utf8(out.body.to_vec()).unwrap(),
            r#"{"model":"x","stream":true}"#
        );
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
        let shared =
            SharedConfigHandle::new(hygress_core::SharedConfig::new(data.clone()).unwrap());
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
        assert_eq!(
            p.base_headers.get("x-custom-model"),
            Some("org1/llama-3-8b")
        );
        // ... and kept the canonical routing key in sync, so ④ matched the
        // Main route (not the mirror).
        assert_eq!(p.base_headers.get(hdr::LLM_MODEL), Some("org1/llama-3-8b"));
        assert!(p.route.is_model_route);
        assert_eq!(p.route.model, "org1/llama-3-8b");
    }

    // ----- ORA3-M14: fused prepare-time scan drives R-5 + AM-2 byte-exact -----

    /// Prepare a JSON request against a Main route keyed `route_model`, using
    /// the given `gpustack-model-router` snapshot settings.
    fn prepare_request(
        body: &str,
        path: &str,
        route_model: &str,
        router_settings: hygress_core::prelude::ModelRouterSettings,
    ) -> PreparedRequest {
        use hygress_core::prelude::{Destination, PathPred, Registry, RouteKind, RouteRule};

        let data = ConfigData {
            routes: vec![RouteRule::new(
                route_model,
                RouteKind::Main,
                vec![PathPred::new(".*")],
                vec![Destination::new("model-1-10.static:80")],
            )
            .unwrap()],
            registries: vec![Registry::new("model-1-10.static:80", "10.0.0.5:8081").unwrap()],
            model_router: router_settings,
            ..ConfigData::default()
        };
        let router = ModelRouterConfig::from_settings(&data.model_router);
        let table = RouteTable::rebuild(&data).unwrap();
        let shared =
            SharedConfigHandle::new(hygress_core::SharedConfig::new(data.clone()).unwrap());
        let ctx = PipelineCtx {
            data: &data,
            table: &table,
            config: &shared,
            router: &router,
        };
        let inbound = InboundRequest {
            method: "POST".into(),
            path: path.into(),
            query: String::new(),
            headers: HeaderMap::new(),
            body: bytes::Bytes::from(body.to_string()),
            content_type: "application/json".into(),
            client_ip: String::new(),
            host: String::new(),
        };
        prepare(&inbound, &ctx).unwrap()
    }

    #[test]
    fn fused_prepare_identity_body_then_outbound_injects_stream_options_byte_exact() {
        // The canonical flow: body-driven resolution, body model == resolved
        // model → R-5 identity (prepare does NOT splice), then build_outbound
        // splices `stream_options` exactly once before the closing `}`.
        let p = prepare_request(
            r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
            "/v1/chat/completions",
            "org1/llama-3-8b",
            hygress_core::prelude::ModelRouterSettings {
                enable_on_path_suffix: vec!["/v1/chat/completions".into()],
                ..Default::default()
            },
        );
        assert_eq!(p.body_model.as_deref(), Some("org1/llama-3-8b"));
        // Identity: prepare left the body byte-for-byte untouched.
        assert_eq!(
            String::from_utf8(p.body.to_vec()).unwrap(),
            r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}],"stream":true}"#
        );
        assert!(p.route.is_model_route);
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        let got = String::from_utf8(out.body.to_vec()).unwrap();
        let expected = r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}"#;
        assert_eq!(got, expected);
        assert_eq!(got.matches("\"stream_options\"").count(), 1, "body: {got}");
    }

    #[test]
    fn fused_prepare_rewritten_model_body_still_injects_exactly_once() {
        // Invariant (a): a PATH alias resolves `mapped-model` ≠ the body's
        // `client-model` → prepare's fused scan drives the R-5 SPLICE (offset
        // splice from the profile span, longer value). The AM-2 verdict was
        // computed on the PRE-splice body (a model-value splice cannot flip the
        // stream/stream_options structure) — build_outbound must still inject
        // exactly once, before the closing `}` of the REWRITTEN body.
        let p = prepare_request(
            r#"{"model":"client-model","stream":true}"#,
            "/model/proxy/7/v1/chat/completions",
            "mapped-model",
            hygress_core::prelude::ModelRouterSettings {
                alias_name_mapping: [("7".to_string(), "mapped-model".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        );
        assert_eq!(p.body_model.as_deref(), Some("mapped-model"));
        let ub = String::from_utf8(p.body.to_vec()).unwrap();
        assert!(ub.contains(r#""model":"mapped-model""#), "spliced body: {ub}");
        assert!(!ub.contains("client-model"), "spliced body: {ub}");
        assert!(p.route.is_model_route);
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        let got = String::from_utf8(out.body.to_vec()).unwrap();
        assert_eq!(
            got,
            r#"{"model":"mapped-model","stream":true,"stream_options":{"include_usage":true}}"#
        );
        assert_eq!(got.matches("\"stream_options\"").count(), 1, "body: {got}");
    }

    #[test]
    fn explicit_client_stream_options_survive_prepare_rewrite_uninjected() {
        // Invariants (a)+(b): a client that explicitly sent `stream_options`
        // keeps its own preference even when prepare's alias-driven model
        // rewrite changes the body — the pre-splice scan flagged
        // `has_stream_options`, so build_outbound never overrides/duplicates it.
        let p = prepare_request(
            r#"{"model":"client-model","stream":true,"stream_options":{"include_usage":false}}"#,
            "/model/proxy/7/v1/chat/completions",
            "mapped-model",
            hygress_core::prelude::ModelRouterSettings {
                alias_name_mapping: [("7".to_string(), "mapped-model".to_string())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            },
        );
        let out = build_outbound("POST", &p, &candidate(), &HeaderMap::new(), &[]);
        let got = String::from_utf8(out.body.to_vec()).unwrap();
        assert_eq!(
            got,
            r#"{"model":"mapped-model","stream":true,"stream_options":{"include_usage":false}}"#
        );
        assert_eq!(got.matches("\"stream_options\"").count(), 1, "body: {got}");
    }
}
