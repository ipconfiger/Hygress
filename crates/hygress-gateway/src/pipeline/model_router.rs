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

/// Resolve the model for `path` over `body`. Pure; enforces the body cap.
///
/// `prefix_hit` + alias-miss, or an `enableOnPathSuffix` match, arm BODY-DRIVEN
/// (the body is read from the already-buffered `body`).
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

    // (1) PATH-DRIVEN (alias): `:path` starts with `prefix`.
    if let Some(rest) = path.strip_prefix(&cfg.prefix) {
        let id = rest.split('/').next().unwrap_or("");
        if !id.is_empty() {
            if let Some(model) = cfg.alias_name_mapping.get(id) {
                return Ok(ModelResolve {
                    resolution: ModelResolution::PathAlias {
                        model: model.clone(),
                    },
                    model: Some(model.clone()),
                });
            }
        }
        // path matched the prefix but the alias id was missed → fall through to
        // body-driven mode.
        return Ok(from_body(extract_model(body, content_type, cfg)));
    }

    // (2) BODY-DRIVEN when `enableOnPathSuffix` matches (and there was no prefix
    //     hit).
    if cfg
        .enable_on_path_suffix
        .iter()
        .any(|s| path.ends_with(s.as_str()))
    {
        return Ok(from_body(extract_model(body, content_type, cfg)));
    }

    // (3) Neither → pass through.
    Ok(ModelResolve {
        resolution: ModelResolution::Passthrough,
        model: None,
    })
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
}
