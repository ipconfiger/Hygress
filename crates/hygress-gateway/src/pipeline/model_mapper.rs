//! ⑧ `gpustack-model-mapper` equivalent — pure. Applies the matched route's
//! per-destination model mapping to the **outbound** body `model` field for the
//! selected (or failed-over-to) candidate's `name.type` (no port).
//!
//! The mapping key is `name.type` (the matchRule service form), distinct from
//! the destination `name.type:port` form — the selected candidate's port is
//! dropped before lookup (design §6.2). Only a top-level string `model` (JSON)
//! or the `model` multipart part is rewritten; anything else is left as-is.

use bytes::Bytes;
use hygress_core::prelude::ModelMapping;

/// Rewrite the outbound body `model` field for `service_name`.
///
/// Returns `true` when the body was actually rewritten. `service_name` is the
/// selected candidate's `name.type` (e.g. `model-1-10.static`).
pub fn apply(
    mapping: &ModelMapping,
    service_name: &str,
    body: &mut Bytes,
    content_type: &str,
) -> bool {
    if body.is_empty() {
        return false;
    }
    if crate::body::is_json(Some(content_type)) {
        let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(body) else {
            return false;
        };
        if mapping.apply_json(service_name, &mut v) {
            return match serde_json::to_vec(&v) {
                Ok(b) => {
                    *body = Bytes::from(b);
                    true
                }
                Err(_) => false,
            };
        }
        return false;
    }
    if let Some(boundary) = crate::body::parse_boundary(Some(content_type)) {
        let mut buf = body.to_vec();
        if mapping.apply_multipart(service_name, &mut buf, &boundary) {
            *body = Bytes::from(buf);
            return true;
        }
        return false;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const JSON: &str = "application/json";
    const MP: &str = "multipart/form-data; boundary=B";

    #[test]
    fn json_rewrites_mapped_service_model() {
        let m = ModelMapping::single("model-1-10.static", "llama-3-8b-instruct");
        let mut body = Bytes::from(r#"{"model":"org1/llama:adapter","stream":true}"#);
        assert!(apply(&m, "model-1-10.static", &mut body, JSON));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["model"], json!("llama-3-8b-instruct"));
        assert_eq!(v["stream"], json!(true));
    }

    #[test]
    fn unmapped_service_is_noop() {
        let m = ModelMapping::single("a.static", "x");
        let mut body = Bytes::from(r#"{"model":"keep"}"#);
        assert!(!apply(&m, "b.static", &mut body, JSON));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["model"], json!("keep"));
    }

    #[test]
    fn empty_mapping_is_noop() {
        let m = ModelMapping::new();
        let mut body = Bytes::from(r#"{"model":"keep"}"#);
        assert!(!apply(&m, "a.static", &mut body, JSON));
    }

    #[test]
    fn non_string_model_is_noop() {
        let m = ModelMapping::single("a.static", "x");
        let mut body = Bytes::from(r#"{"model":5}"#);
        assert!(!apply(&m, "a.static", &mut body, JSON));
    }

    #[test]
    fn multipart_rewrites_model_part() {
        let m = ModelMapping::single("model-1-10.static", "mapped-name");
        let mut body = Bytes::from(
            "--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\norg/llama\r\n\
             --B\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nXYZ\r\n--B--\r\n",
        );
        assert!(apply(&m, "model-1-10.static", &mut body, MP));
        let s = String::from_utf8(body.to_vec()).unwrap();
        assert!(s.contains("name=\"model\"\r\n\r\nmapped-name\r\n"));
        assert!(s.contains("XYZ"));
    }

    #[test]
    fn non_json_non_multipart_is_noop() {
        let m = ModelMapping::single("a.static", "x");
        let mut body = Bytes::from("text body");
        assert!(!apply(&m, "a.static", &mut body, "text/plain"));
    }

    #[test]
    fn empty_body_is_noop() {
        let m = ModelMapping::single("a.static", "x");
        let mut body = Bytes::new();
        assert!(!apply(&m, "a.static", &mut body, JSON));
    }
}
