//! ② `gpustack-model-router` (generic-proxy-router) equivalent — pure.
//!
//! Resolves the effective model from the request **path** (alias) or **body**
//! (JSON / basic multipart), per the frozen decision tree (pin §2.3):
//!
//! 1. `:path` starts with `prefix` (`/model/proxy/`) → **PATH-DRIVEN (alias)**:
//!    id = first path segment after the prefix.
//!    - id ∈ `aliasNameMapping` → **HIT**: model = `aliasNameMapping[id]`.
//!    - id ∉ mapping → **fall through to BODY-DRIVEN**.
//! 2. `enableOnPathSuffix` match (and no prefix hit) → **BODY-DRIVEN**:
//!    model = the body `model` field (JSON) or `model` part (multipart).
//! 3. neither → **pass through** (no header write, no body rewrite).
//!
//! The resolved model **overwrites** `x-higress-llm-model` (pin §2.3: the plugin
//! always sets `targetHeader` with the resolved value; a client cannot spoof the
//! routed model by pre-setting the header). Both the header write and the body
//! `model`-field rewrite are applied by [`crate::pipeline::prepare`] from the
//! returned [`ModelResolve`].
//!
//! `maxBodyBytes` is a hard cap on the buffered request body → 413 above it.
//! Auto-routing (`autoRoutingRules` / `defaultModel`) is outside v1 scope (the
//! in-cluster instances are OpenAI-compatible; no rules configured by GPUStack).

use bytes::Bytes;

use crate::context::ModelResolution;
use crate::context::ModelRouterConfig;
use crate::error::GatewayError;

/// The resolution outcome of ②. `model` is the value to **overwrite**
/// `x-higress-llm-model` with and (when the body is JSON/multipart) rewrite into
/// the body `model` field; `None` for pass-through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelResolve {
    pub resolution: ModelResolution,
    pub model: Option<String>,
}

/// The stage-② path-mode decision (contract-pin §2.3 decision tree), computed
/// from the path + alias table **before** any body work: an alias hit resolves
/// without touching the body; body-driven mode is the only one that reads the
/// body `model`.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PathMode {
    /// PATH-DRIVEN alias HIT: model = `aliasNameMapping[id]`.
    AliasHit(String),
    /// BODY-DRIVEN: model = the body `model` field (JSON) / part (multipart).
    BodyDriven,
    /// Neither `prefix` nor `enableOnPathSuffix` matched → pass through.
    Passthrough,
}

/// The `gpustack-model-router` decision tree restricted to `path`/`cfg`.
fn path_mode(path: &str, cfg: &ModelRouterConfig) -> PathMode {
    // (1) PATH-DRIVEN (alias): `:path` starts with `prefix`.
    if let Some(rest) = path.strip_prefix(&cfg.prefix) {
        let id = rest.split('/').next().unwrap_or("");
        if !id.is_empty() {
            if let Some(model) = cfg.alias_name_mapping.get(id) {
                return PathMode::AliasHit(model.clone());
            }
        }
        // path matched the prefix but the alias id was missed → fall through to
        // body-driven mode.
        return PathMode::BodyDriven;
    }
    // (2) BODY-DRIVEN when `enableOnPathSuffix` matches (and there was no
    //     prefix hit).
    if cfg
        .enable_on_path_suffix
        .iter()
        .any(|s| path.ends_with(s.as_str()))
    {
        return PathMode::BodyDriven;
    }
    // (3) Neither → pass through.
    PathMode::Passthrough
}

/// Combine the path mode with the resolved body model (only `BodyDriven`
/// consumes it).
fn from_mode(mode: PathMode, body_model: Option<String>) -> ModelResolve {
    match mode {
        PathMode::AliasHit(model) => ModelResolve {
            resolution: ModelResolution::PathAlias { model: model.clone() },
            model: Some(model),
        },
        PathMode::BodyDriven => from_body(body_model),
        PathMode::Passthrough => ModelResolve {
            resolution: ModelResolution::Passthrough,
            model: None,
        },
    }
}

/// Resolve the model for `path` over `body`. Pure; enforces the body cap.
///
/// `prefix_hit` + alias-miss, or an `enableOnPathSuffix` match, arm BODY-DRIVEN
/// (the body is read from the already-buffered `body`). The body is scanned
/// only when body-driven mode is actually engaged; [`resolve_fused`] is the
/// entry the pipe's prepare uses to avoid even that scan (ORA3-M14).
pub fn resolve(
    path: &str,
    body: &Bytes,
    content_type: &str,
    cfg: &ModelRouterConfig,
) -> Result<ModelResolve, GatewayError> {
    // ⑥ cap: a buffered body above the hard cap is rejected (413). This runs for
    // every request (terminate-mode buffers the full body up front).
    if body.len() > cfg.max_body_bytes {
        return Err(GatewayError::BodyTooLarge(body.len(), cfg.max_body_bytes));
    }
    let mode = path_mode(path, cfg);
    let body_model = if matches!(mode, PathMode::BodyDriven) {
        extract_model(body, content_type, cfg)
    } else {
        None
    };
    Ok(from_mode(mode, body_model))
}

/// Fused ② entry for the pipe's prepare (ORA3-M14): body cap + path-mode
/// decision only — the body-driven model value is supplied by prepare's single
/// prepare-time top-level scan (`body_model`), so resolve does NOT re-scan the
/// body. `body_model = None` covers a missing / non-string / malformed body,
/// exactly what the extraction scan would return, so the outcome is identical
/// to [`resolve`] on the same inputs.
pub(crate) fn resolve_fused(
    path: &str,
    body: &Bytes,
    cfg: &ModelRouterConfig,
    body_model: Option<&str>,
) -> Result<ModelResolve, GatewayError> {
    if body.len() > cfg.max_body_bytes {
        return Err(GatewayError::BodyTooLarge(body.len(), cfg.max_body_bytes));
    }
    let mode = path_mode(path, cfg);
    Ok(from_mode(mode, body_model.map(str::to_string)))
}

/// Read the body `model` field (JSON `modelKey` / multipart `model` part).
fn extract_model(body: &Bytes, content_type: &str, cfg: &ModelRouterConfig) -> Option<String> {
    if content_type.is_empty() && body.is_empty() {
        return None;
    }
    let ct = if content_type.is_empty() {
        None
    } else {
        Some(content_type)
    };
    crate::body::extract_model(body, ct, &cfg.model_key)
}

/// BODY-DRIVEN → a `model` value, else pass-through (no header write).
fn from_body(model: Option<String>) -> ModelResolve {
    match model {
        Some(m) => ModelResolve {
            resolution: ModelResolution::Body { model: m.clone() },
            model: Some(m),
        },
        None => ModelResolve {
            resolution: ModelResolution::Passthrough,
            model: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::context::ModelRouterConfig;

    const JSON: &str = "application/json";
    const MP: &str = "multipart/form-data; boundary=B";

    fn cfg() -> ModelRouterConfig {
        ModelRouterConfig {
            enable_on_path_suffix: vec!["/v1/chat/completions".to_string()],
            ..Default::default()
        }
    }

    fn with_alias(alias: &[(&str, &str)]) -> ModelRouterConfig {
        let mut c = cfg();
        for (k, v) in alias {
            c.alias_name_mapping.insert(k.to_string(), v.to_string());
        }
        c
    }

    #[test]
    fn path_alias_hit_overwrites_model() {
        let c = with_alias(&[("5", "org1/llama-3-8b")]);
        let body = Bytes::from(r#"{"model":"org1/llama-3-8b"}"#);
        let r = resolve("/model/proxy/5/v1/chat/completions", &body, JSON, &c).unwrap();
        assert_eq!(r.model.as_deref(), Some("org1/llama-3-8b"));
        assert_eq!(
            r.resolution,
            ModelResolution::PathAlias {
                model: "org1/llama-3-8b".to_string()
            }
        );
    }

    #[test]
    fn path_alias_miss_falls_through_to_body() {
        let c = with_alias(&[("5", "org1/llama")]);
        let body = Bytes::from(r#"{"model":"org2/gpt-4o"}"#);
        // id=99 not in the mapping → body-driven reads the body model.
        let r = resolve("/model/proxy/99/v1/chat/completions", &body, JSON, &c).unwrap();
        assert_eq!(r.model.as_deref(), Some("org2/gpt-4o"));
        matches!(r.resolution, ModelResolution::Body { .. });
    }

    #[test]
    fn body_driven_on_enable_suffix() {
        let c = cfg();
        let body = Bytes::from(r#"{"model":"gpt-4o","messages":[]}"#);
        let r = resolve("/v1/chat/completions", &body, JSON, &c).unwrap();
        assert_eq!(r.model.as_deref(), Some("gpt-4o"));
        matches!(r.resolution, ModelResolution::Body { .. });
    }

    #[test]
    fn body_driven_no_model_is_passthrough() {
        let c = cfg();
        let body = Bytes::from(r#"{"messages":[]}"#); // no `model` field
        let r = resolve("/v1/chat/completions", &body, JSON, &c).unwrap();
        assert!(r.model.is_none());
        assert_eq!(r.resolution, ModelResolution::Passthrough);
    }

    #[test]
    fn neither_prefix_nor_suffix_is_passthrough() {
        let c = cfg();
        let body = Bytes::from(r#"{"model":"gpt-4o"}"#);
        // /v1/messages is not the enable suffix and has no proxy prefix.
        let r = resolve("/v1/messages", &body, JSON, &c).unwrap();
        assert_eq!(r.resolution, ModelResolution::Passthrough);
        assert!(r.model.is_none());
    }

    #[test]
    fn multipart_body_driven_model() {
        let c = cfg();
        let body = Bytes::from(
            "--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\norg1/llama\r\n--B--\r\n",
        );
        let r = resolve("/v1/chat/completions", &body, MP, &c).unwrap();
        assert_eq!(r.model.as_deref(), Some("org1/llama"));
    }

    #[test]
    fn body_over_max_bytes_is_413() {
        let c = cfg();
        let big = bytes::Bytes::from(vec![b'x'; 1024]); // > default 8MiB? no; craft a cap
        let mut c2 = c;
        c2.max_body_bytes = 10;
        let r = resolve("/v1/chat/completions", &big, JSON, &c2);
        assert!(matches!(r, Err(GatewayError::BodyTooLarge(1024, 10))));
    }

    #[test]
    fn empty_body_within_cap_is_passthrough() {
        let c = cfg();
        let r = resolve("/v1/chat/completions", &Bytes::new(), JSON, &c).unwrap();
        assert_eq!(r.resolution, ModelResolution::Passthrough);
    }

    #[test]
    fn alias_empty_is_ignored() {
        let c = with_alias(&[("", "x")]);
        let r = resolve("/model/proxy//v1/chat/completions", &Bytes::new(), JSON, &c).unwrap();
        // id is empty → no alias hit → body-driven (empty body) → passthrough.
        assert_eq!(r.resolution, ModelResolution::Passthrough);
    }

    // ----- ORA3-M14: resolve_fused (prepare's no-rescan entry) parity -----

    /// Assert `resolve` and `resolve_fused` (fed the body model extracted by
    /// prepare's single scan) agree on every path shape.
    fn assert_fused_parity(path: &str, body: &Bytes, cfg: &ModelRouterConfig) {
        let scanned = crate::body::extract_model(body, Some(JSON), "model");
        let expect = resolve(path, body, JSON, cfg).unwrap();
        let got = resolve_fused(path, body, cfg, scanned.as_deref()).unwrap();
        assert_eq!(
            got, expect,
            "resolve_fused diverged from resolve for path {path} body {body:?}"
        );
    }

    #[test]
    fn fused_resolve_matches_resolve_across_path_shapes() {
        let c_alias = with_alias(&[("5", "org1/llama-3-8b")]);
        let c = cfg();
        let cases: &[(&str, &[u8], &ModelRouterConfig)] = &[
            // Alias hit — the body model is irrelevant (never scanned).
            (
                "/model/proxy/5/v1/chat/completions",
                br#"{"model":"org2/gpt-4o"}"#,
                &c_alias,
            ),
            // Prefix hit + alias miss → body-driven.
            (
                "/model/proxy/99/v1/chat/completions",
                br#"{"model":"org2/gpt-4o"}"#,
                &c_alias,
            ),
            // Suffix hit → body-driven, model present / absent / non-string.
            ("/v1/chat/completions", br#"{"model":"gpt-4o"}"#, &c),
            ("/v1/chat/completions", br#"{"messages":[]}"#, &c),
            ("/v1/chat/completions", br#"{"model":5}"#, &c),
            // Malformed body in body-driven mode → None → passthrough.
            ("/v1/chat/completions", b"{broken", &c),
            // Neither prefix nor suffix → passthrough even with a model body.
            ("/v1/messages", br#"{"model":"gpt-4o"}"#, &c),
        ];
        for (path, body, cfg) in cases {
            let body = Bytes::copy_from_slice(body);
            assert_fused_parity(path, &body, cfg);
        }
    }

    #[test]
    fn fused_resolve_still_enforces_the_body_cap() {
        let mut c = cfg();
        c.max_body_bytes = 10;
        let big = bytes::Bytes::from(vec![b'x'; 1024]);
        let r = resolve_fused("/v1/chat/completions", &big, &c, None);
        assert!(matches!(r, Err(GatewayError::BodyTooLarge(1024, 10))));
    }
}
