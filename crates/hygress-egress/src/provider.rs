//! `ProviderClient` — build the outbound **upstream** request for an LLM/provider call
//! (design §6.1 ⑨⑩ / §7 ai-proxy). This is the native equivalent of the provider-proxy portion of
//! the data plane: after ext-auth, routing, registry resolution, SWRR selection, and model-mapper
//! have run, the gateway must dial the selected upstream. `ProviderClient` assembles that outbound
//! request **purely** (no I/O of its own — the gateway holds and dials the resulting
//! [`UpstreamRequest`]):
//!
//! - **Path rewrite** — `higress.io/rewrite-target` (e.g. `/$1$3`) evaluated against the matched
//!   path predicate's capture groups (via the core [`PathRewriter`]), else the inbound path passthrough.
//! - **Key swap** — set `Authorization: Bearer <api_token>` (the provider's key, replacing the inbound one).
//! - **Host override** — set the outbound `Host` header + the URL host.
//! - **Model mapping** — apply the per-destination `ModelMapping` (key = selected `name.type`) to a
//!   JSON or basic-multipart body (via the core `ModelMapping`).
//! - **Header copy** — copy the forward-safe inbound headers (skipping pseudo-headers and `Host`,
//!   which is set explicitly).

use http::header;
use http::{HeaderMap, HeaderValue, Method};
use hygress_core::prelude::{HeaderMap as CoreHeaderMap, ModelMapping, PathRewriter};
use serde_json::Value;
use url::Url;

/// The assembled outbound upstream request the gateway dials.
#[derive(Clone, Debug)]
pub struct UpstreamRequest {
    /// Destination URL (scheme + host from `base`, path from the rewrite, optional query).
    pub url: Url,
    /// HTTP method.
    pub method: Method,
    /// Outbound headers (inbound-forwarded + `Authorization` key swap + `Host` + `Content-Type`).
    pub headers: HeaderMap,
    /// The outbound body bytes (empty when there is no body).
    pub body: Vec<u8>,
    /// When the upstream is reached through an outbound proxy, the proxy server's
    /// `host:port`. The dialer connects through it; `url` remains the upstream origin.
    /// `None` for a direct connection.
    pub proxy: Option<String>,
}

/// A provider outbound request body.
#[derive(Clone, Debug)]
pub enum Body {
    /// A JSON request body (the usual OpenAI/Anthropic inference form).
    Json(Value),
    /// A basic multipart form body (the model-router multipart form) and its boundary.
    Multipart { bytes: Vec<u8>, boundary: String },
}

/// Parameters for building one upstream request (design §6.1 ⑨⑩).
#[derive(Clone, Debug, Default)]
pub struct UpstreamOptions {
    /// HTTP method (default `GET`; inference is normally `POST`).
    pub method: Method,
    /// The original inbound request path (**path only, no query**), e.g. `/v1/chat/completions`.
    pub input_path: String,
    /// Capture groups from the matched path predicate (`$1`..`$9` for the rewriter).
    pub capture_groups: Vec<String>,
    /// `higress.io/rewrite-target` (e.g. `/$1$3`), if the route defines one.
    pub path_rewrite: Option<PathRewriter>,
    /// The provider API token — swapped into `Authorization: Bearer …`.
    pub api_token: String,
    /// Outbound `Host` header override (and URL host).
    pub host_override: Option<String>,
    /// Per-destination model mapping (keys are `name.type`, **no port**).
    pub model_mapping: Option<ModelMapping>,
    /// The selected destination service name `name.type` (no port) — the model-mapping key.
    pub destination_service: Option<String>,
    /// Inbound (already-transformed, forward-safe) headers to copy into the outbound request.
    pub inbound_headers: CoreHeaderMap,
    /// The outbound body (JSON or multipart), if any.
    pub body: Option<Body>,
    /// An optional query string (with or without a leading `?`) preserved onto the outbound URL.
    pub query: Option<String>,
    /// The upstream scheme to dial (`"http"` / `"https"`). `None` keeps the `base` URL's
    /// scheme. The gateway derives it from the resolved port (443 → `https`, else `http`).
    pub scheme: Option<String>,
    /// When the upstream is reached through an outbound proxy, the proxy server's
    /// `host:port` (recorded on [`UpstreamRequest::proxy`] for the dialer).
    pub proxy: Option<String>,
}

/// Pure upstream-request builder (no I/O — the gateway holds/dials the result).
#[derive(Clone, Copy, Debug, Default)]
pub struct ProviderClient;

impl ProviderClient {
    /// Build the outbound upstream request for a destination (design §6.1 ⑨⑩).
    ///
    /// `base` supplies scheme + authority (the resolved upstream origin, e.g.
    /// `http://10.0.0.5:8081`); the path, method, headers, and body come from `opts`.
    pub fn build_upstream_request(base: &Url, opts: &UpstreamOptions) -> UpstreamRequest {
        // 1. URL: path from the rewrite (or inbound passthrough) + optional query.
        let new_path = match &opts.path_rewrite {
            Some(rw) => rw.rewrite(&opts.capture_groups),
            None => opts.input_path.clone(),
        };
        let mut url = base.clone();
        url.set_path(&new_path);
        if let Some(q) = &opts.query {
            let q = q.trim().trim_start_matches('?');
            if !q.is_empty() {
                url.set_query(Some(q));
            }
        }
        // 2. Host override (URL authority + explicit Header, set below).
        // MINOR-12: `set_host` fails (invalid host chars, e.g. a space or `_`) — never silently
        // ignore the failure: log it and keep the `base` host so the request still goes out (the
        // dialer would otherwise talk to the wrong origin with no trace).
        if let Some(host) = &opts.host_override {
            if url.set_host(Some(host)).is_err() {
                tracing::warn!(
                    "provider: cannot set outbound URL host to {host:?} on base {base}; keeping the base host (request may reach the wrong origin)"
                );
            }
        }
        // 2b. Scheme override (e.g. dial an `https` upstream through a proxy); `None`
        //     keeps the `base` URL's scheme. `set_scheme` only touches the scheme, so it is
        //     independent of the path / host / query set above. As with the host, a failure
        //     (an invalid scheme string) must not be silent.
        if let Some(scheme) = &opts.scheme {
            if url.set_scheme(scheme).is_err() {
                tracing::warn!(
                    "provider: cannot set outbound URL scheme to {scheme:?} on base {base}; keeping the base scheme"
                );
            }
        }

        // 3. Headers: forward-safe inbound copy, then key swap + Host override.
        let mut headers = forward_inbound_headers(&opts.inbound_headers);
        if !opts.api_token.is_empty() {
            if let Ok(v) = HeaderValue::from_str(&format!("Bearer {}", opts.api_token)) {
                headers.insert(header::AUTHORIZATION, v);
            }
        }
        if let Some(host) = &opts.host_override {
            if let Ok(v) = HeaderValue::from_str(host) {
                headers.insert(header::HOST, v);
            }
        }

        // 4. Body + per-destination model rewrite (JSON or multipart via the core ModelMapping).
        let body = build_body(opts, &mut headers);

        UpstreamRequest {
            url,
            method: opts.method.clone(),
            headers,
            body,
            proxy: opts.proxy.clone(),
        }
    }
}

/// Copy the forward-safe inbound headers, dropping pseudo-headers (`:path`, …) and `Host`
/// (which is set explicitly by the builder). Invalid header values are skipped, not fatal.
fn forward_inbound_headers(inbound: &CoreHeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for name in inbound.names() {
        if name.starts_with(':') || name.eq_ignore_ascii_case("host") {
            continue;
        }
        // `append`/`insert` require an owned `HeaderName` (or `&'static str`); build one
        // from the (already-valid) inbound name so the borrow of `inbound` does not escape.
        let Ok(name) = http::HeaderName::from_bytes(name.as_bytes()) else {
            continue; // not a valid header name (defensive; shouldn't happen for inbound headers)
        };
        for value in inbound.get_all(name.as_str()) {
            if let Ok(v) = HeaderValue::from_bytes(value.as_bytes()) {
                out.append(name.clone(), v);
            }
        }
    }
    out
}

/// Build the outbound body, applying the per-destination model rewrite when a mapping + service
/// are provided, and setting `Content-Type` when the caller did not.
///
/// A mapping lookup miss (the selected destination has no rule) is a **normal** no-op — the body is
/// forwarded unchanged. But when the destination IS mapped and the rewrite still cannot be applied
/// (non-object body, missing or non-string `model` field / no `name="model"` multipart part) the
/// client-supplied model alias would reach the upstream unmapped — MINOR-12: log that instead of
/// silently forwarding it.
fn build_body(opts: &UpstreamOptions, headers: &mut HeaderMap) -> Vec<u8> {
    match &opts.body {
        None => Vec::new(),
        Some(Body::Json(value)) => {
            let mut value = value.clone();
            let mapped = opts
                .model_mapping
                .as_ref()
                .zip(opts.destination_service.as_ref())
                .filter(|(m, svc)| m.lookup(svc).is_some());
            if let Some((m, svc)) = mapped {
                if !m.apply_json(svc, &mut value) {
                    tracing::warn!(
                        "provider: model mapping for destination '{svc}' could not be applied to the JSON body (no rewritable top-level string `model`); forwarding the client-supplied value"
                    );
                }
            }
            if !headers.contains_key(header::CONTENT_TYPE) {
                headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
            }
            serde_json::to_vec(&value).unwrap_or_default()
        }
        Some(Body::Multipart { bytes, boundary }) => {
            let mut bytes = bytes.clone();
            let mapped = opts
                .model_mapping
                .as_ref()
                .zip(opts.destination_service.as_ref())
                .filter(|(m, svc)| m.lookup(svc).is_some());
            if let Some((m, svc)) = mapped {
                if !m.apply_multipart(svc, &mut bytes, boundary) {
                    tracing::warn!(
                        "provider: model mapping for destination '{svc}' could not be applied to the multipart body (no `name=\"model\"` part); forwarding the client-supplied value"
                    );
                }
            }
            if !headers.contains_key(header::CONTENT_TYPE) {
                if let Ok(v) =
                    HeaderValue::from_str(&format!("multipart/form-data; boundary={boundary}"))
                {
                    headers.insert(header::CONTENT_TYPE, v);
                }
            }
            bytes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header;
    use hygress_core::prelude::HeaderMap as CoreHeaderMap;
    use serde_json::json;
    use url::Url;

    fn base(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    /// Convenience wrapper for [`ProviderClient::build_upstream_request`] (associated function).
    #[inline]
    fn build(s: &str, o: &UpstreamOptions) -> UpstreamRequest {
        ProviderClient::build_upstream_request(&base(s), o)
    }

    fn opt(mut o: UpstreamOptions) -> UpstreamOptions {
        o.input_path = "/v1/chat/completions".into();
        o.method = Method::POST;
        o
    }

    // ----- path rewrite with capture-group substitution (pin: /$1$3) -----

    #[test]
    fn path_rewrite_substitutes_capture_groups() {
        // GPUStack rewrite-target "/$1$3" against pattern ()model/proxy/\d+(/|$)(.*)
        // for /model/proxy/5/chat/completions -> groups ["", "/", "chat/completions"] -> "/chat/completions".
        let o = opt(UpstreamOptions {
            input_path: "/model/proxy/5/chat/completions".into(),
            capture_groups: vec!["".into(), "/".into(), "chat/completions".into()],
            path_rewrite: Some(PathRewriter::new("/$1$3")),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(req.url.to_string(), "http://10.0.0.5:8081/chat/completions");
    }

    #[test]
    fn no_rewrite_keeps_inbound_path() {
        let o = opt(UpstreamOptions::default());
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(
            req.url.to_string(),
            "http://10.0.0.5:8081/v1/chat/completions"
        );
    }

    #[test]
    fn query_is_preserved_onto_outbound_url() {
        let o = opt(UpstreamOptions {
            query: Some("stream=1".into()),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(
            req.url.to_string(),
            "http://10.0.0.5:8081/v1/chat/completions?stream=1"
        );
    }

    // ----- Authorization bearer key swap + Host override -----

    #[test]
    fn key_swap_sets_authorization_bearer() {
        let o = opt(UpstreamOptions {
            api_token: "sk-provider-1".into(),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(
            req.headers.get(header::AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer sk-provider-1"))
        );
    }

    #[test]
    fn key_swap_overrides_inbound_authorization() {
        // The inbound (client/previous-hops) Authorization must be replaced by the provider key.
        let mut inbound = CoreHeaderMap::new();
        inbound.insert(header::AUTHORIZATION.as_str(), "Bearer client-secret");
        let o = opt(UpstreamOptions {
            api_token: "sk-provider-1".into(),
            inbound_headers: inbound,
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(
            req.headers.get(header::AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer sk-provider-1"))
        );
        assert_eq!(
            req.headers
                .get_all(header::AUTHORIZATION)
                .into_iter()
                .count(),
            1
        );
    }

    #[test]
    fn empty_token_keeps_inbound_authorization() {
        let mut inbound = CoreHeaderMap::new();
        inbound.insert(header::AUTHORIZATION.as_str(), "Bearer keep");
        let o = opt(UpstreamOptions {
            api_token: String::new(),
            inbound_headers: inbound,
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(
            req.headers.get(header::AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer keep"))
        );
    }

    #[test]
    fn host_override_sets_header_and_url_host() {
        let o = opt(UpstreamOptions {
            host_override: Some("provider-1.example.com".into()),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(
            req.headers.get(header::HOST),
            Some(&HeaderValue::from_static("provider-1.example.com"))
        );
        assert_eq!(req.url.host_str(), Some("provider-1.example.com"));
        // Port preserved from base (set_host only changes the host part).
        assert_eq!(req.url.port(), Some(8081));
    }

    // ----- scheme + proxy (D8) -----

    #[test]
    fn https_scheme_overrides_base_url() {
        // The gateway derives `https` from a `:443` upstream and overrides the
        // `base` URL's `http` scheme (e.g. a provider egress reached over TLS).
        // A non-default port keeps the full URL round-trip unambiguous (the `url`
        // crate omits the default port `443` for `https`).
        let o = opt(UpstreamOptions {
            scheme: Some("https".into()),
            ..UpstreamOptions::default()
        });
        let req = build("http://api.example.com:8443", &o);
        assert_eq!(req.url.scheme(), "https");
        assert_eq!(
            req.url.to_string(),
            "https://api.example.com:8443/v1/chat/completions"
        );
        // And `https` + explicit 443 round-trips without the redundant port.
        let req2 = build("http://api.example.com:443", &o);
        assert_eq!(req2.url.scheme(), "https");
        assert_eq!(req2.url.host_str(), Some("api.example.com"));
    }

    #[test]
    fn no_scheme_keeps_base_scheme() {
        // Default (`scheme: None`) preserves the `base` URL's scheme.
        let o = opt(UpstreamOptions::default());
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(req.url.scheme(), "http");
        assert!(req.proxy.is_none());
    }

    #[test]
    fn outbound_proxy_is_recorded_on_result() {
        // A `proxy` (outbound `host:port`) is recorded on the result so the gateway
        // dialer can route through it; the `url` still points at the upstream origin.
        let o = opt(UpstreamOptions {
            scheme: Some("https".into()),
            proxy: Some("proxy.internal:3128".into()),
            ..UpstreamOptions::default()
        });
        let req = build("http://api.example.com:443", &o);
        assert_eq!(req.proxy.as_deref(), Some("proxy.internal:3128"));
        assert_eq!(req.url.host_str(), Some("api.example.com"));
        assert_eq!(req.url.scheme(), "https");
    }

    // ----- per-destination model mapping: JSON + multipart (core ModelMapping) -----

    #[test]
    fn json_body_model_rewrite() {
        let o = opt(UpstreamOptions {
            model_mapping: Some(ModelMapping::single(
                "model-1-10.static",
                "llama-3-8b-instruct",
            )),
            destination_service: Some("model-1-10.static".into()),
            body: Some(Body::Json(json!({
                "model": "org1/llama-3-8b:adapter",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": true
            }))),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        let v: Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(v["model"], json!("llama-3-8b-instruct"));
        // Other fields untouched.
        assert_eq!(v["stream"], json!(true));
        assert_eq!(v["messages"][0]["role"], json!("user"));
        assert_eq!(
            req.headers.get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );
    }

    #[test]
    fn json_body_unmapped_service_is_noop() {
        // The selected service has no mapping -> the body is unchanged.
        let o = opt(UpstreamOptions {
            model_mapping: Some(ModelMapping::single("other.static", "x")),
            destination_service: Some("model-1-10.static".into()),
            body: Some(Body::Json(json!({"model": "keep"}))),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        let v: Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(v["model"], json!("keep"));
    }

    fn mp_body(model_value: &str) -> Vec<u8> {
        format!(
            "--B\r\n\
             Content-Disposition: form-data; name=\"model\"\r\n\
             \r\n\
             {model_value}\r\n\
             --B\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"f.bin\"\r\n\
             Content-Type: application/octet-stream\r\n\
             \r\n\
             XYZ\r\n\
             --B--\r\n"
        )
        .into_bytes()
    }

    #[test]
    fn multipart_body_model_rewrite() {
        let o = opt(UpstreamOptions {
            model_mapping: Some(ModelMapping::single(
                "model-1-10.static",
                "llama-3-8b-instruct",
            )),
            destination_service: Some("model-1-10.static".into()),
            body: Some(Body::Multipart {
                bytes: mp_body("org1/llama-3-8b:adapter"),
                boundary: "B".into(),
            }),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        let s = String::from_utf8(req.body.clone()).unwrap();
        assert!(s.contains("name=\"model\"\r\n\r\nllama-3-8b-instruct\r\n"));
        // The other part is untouched.
        assert!(s.contains("XYZ"));
        assert!(s.ends_with("--B--\r\n"));
        assert_eq!(
            req.headers.get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("multipart/form-data; boundary=B"))
        );
    }

    #[test]
    fn multipart_body_unmapped_service_is_noop() {
        let o = opt(UpstreamOptions {
            model_mapping: Some(ModelMapping::single("a.static", "x")),
            destination_service: Some("b.static".into()),
            body: Some(Body::Multipart {
                bytes: mp_body("keep"),
                boundary: "B".into(),
            }),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        assert!(String::from_utf8(req.body).unwrap().contains("keep"));
    }

    // ----- forward-safe header copy -----

    #[test]
    fn inbound_headers_are_forwarded_but_host_and_pseudos_are_not() {
        let mut inbound = CoreHeaderMap::new();
        inbound.insert("x-higress-llm-model", "llama-3-8b");
        inbound.insert("x-request-id", "abc-123");
        inbound.insert(":path", "/v1/chat/completions"); // pseudo-header: must NOT be forwarded
        inbound.insert("host", "inbound.example.com"); // must NOT be forwarded
        let o = opt(UpstreamOptions {
            host_override: Some("provider-1.example.com".into()),
            inbound_headers: inbound,
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        assert_eq!(
            req.headers.get("x-higress-llm-model"),
            Some(&HeaderValue::from_static("llama-3-8b"))
        );
        assert_eq!(
            req.headers.get("x-request-id"),
            Some(&HeaderValue::from_static("abc-123"))
        );
        // Pseudo-header never lands in an http::HeaderMap (it would be invalid); assert absence.
        assert_eq!(
            req.headers.get("host"),
            Some(&HeaderValue::from_static("provider-1.example.com"))
        );
    }

    #[test]
    fn no_body_gives_empty_body() {
        let o = opt(UpstreamOptions::default());
        let req = build("http://10.0.0.5:8081", &o);
        assert!(req.body.is_empty());
        assert_eq!(req.method, Method::POST);
    }

    // ----- MINOR-12: URL rewrite / mapping failures are logged, never silent -----

    #[test]
    fn invalid_host_override_keeps_base_host() {
        // A host value the `url` crate rejects (whitespace is not a valid domain character) cannot
        // be set on the URL authority. Previously `let _ = url.set_host(...)` swallowed that
        // silently; the builder now logs it (MINOR-12) and falls back to the `base` host so the
        // dialable URL stays valid.
        let o = opt(UpstreamOptions {
            host_override: Some("bad host.example".into()),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        // The URL authority keeps the base host (the invalid override is not dialable)...
        assert_eq!(req.url.host_str(), Some("10.0.0.5"));
        assert_eq!(
            req.url.to_string(),
            "http://10.0.0.5:8081/v1/chat/completions"
        );
        // ... while the Host *header* still carries the override (an interior space IS a valid
        // header value). The request would dial `10.0.0.5` while claiming `bad host.example` —
        // the exact divergence MINOR-12 now logs instead of silently ignoring.
        assert_eq!(
            req.headers.get(header::HOST),
            Some(&HeaderValue::from_static("bad host.example"))
        );
    }

    #[test]
    fn mapped_destination_with_unrewritable_model_forwards_unchanged() {
        // The destination IS mapped, but the body has no rewritable top-level string `model`
        // (here a number): the rewrite cannot apply. The builder logs it (MINOR-12) and forwards
        // the body unchanged — never a silent partial rewrite.
        let o = opt(UpstreamOptions {
            model_mapping: Some(ModelMapping::single("model-1-10.static", "llama-3-8b-instruct")),
            destination_service: Some("model-1-10.static".into()),
            body: Some(Body::Json(json!({"model": 5, "messages": []}))),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.0.0.5:8081", &o);
        let v: Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(v["model"], json!(5), "non-string model is not rewritten");
    }

    #[test]
    fn combined_full_outbound_request() {
        let mut inbound = CoreHeaderMap::new();
        inbound.insert("x-higress-llm-model", "llama-3-8b");
        let o = opt(UpstreamOptions {
            input_path: "/model/proxy/7/v1/chat/completions".into(),
            capture_groups: vec!["".into(), "/".into(), "v1/chat/completions".into()],
            path_rewrite: Some(PathRewriter::new("/$1$3")),
            api_token: "sk-provider-42".into(),
            host_override: Some("api.upstream.com".into()),
            model_mapping: Some(ModelMapping::single("provider-1.dns", "gpt-4o")),
            destination_service: Some("provider-1.dns".into()),
            body: Some(Body::Json(json!({"model": "client-alias", "messages": []}))),
            inbound_headers: inbound,
            query: Some("stream=true".into()),
            ..UpstreamOptions::default()
        });
        let req = build("http://10.1.2.3:8443", &o);
        assert_eq!(
            req.url.to_string(),
            "http://api.upstream.com:8443/v1/chat/completions?stream=true"
        );
        assert_eq!(req.method, Method::POST);
        assert_eq!(
            req.headers.get(header::AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer sk-provider-42"))
        );
        assert_eq!(
            req.headers.get(header::HOST),
            Some(&HeaderValue::from_static("api.upstream.com"))
        );
        let v: Value = serde_json::from_slice(&req.body).unwrap();
        assert_eq!(v["model"], json!("gpt-4o"));
        assert_eq!(
            req.headers.get("x-higress-llm-model"),
            Some(&HeaderValue::from_static("llama-3-8b"))
        );
    }
}
