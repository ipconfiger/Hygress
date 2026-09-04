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

/// Rewrite the outbound body `model` field for `service_name` (convenience
/// form: re-derives the body's current model by scanning — used by tests /
/// direct callers). The hot path is [`apply_with_current`], which carries the
/// body model recorded once at prepare (B4).
///
/// Returns `true` when the body was actually rewritten. `service_name` is the
/// selected candidate's `name.type` (e.g. `model-1-10.static`).
pub fn apply(
    mapping: &ModelMapping,
    service_name: &str,
    body: &mut Bytes,
    content_type: &str,
) -> bool {
    let current = crate::body::extract_model(body, Some(content_type), "model");
    apply_with_current(mapping, service_name, body, content_type, current.as_deref())
}

/// Rewrite the outbound body `model` field for `service_name`, given the body's
/// **current** model value (`current_model`) recorded once at prepare (B4).
///
/// Returns `true` only when the body was actually rewritten.
///
/// - empty body → `false`;
/// - no mapping entry for `service_name` → `false`;
/// - `current_model == None` (missing / non-string / malformed body) → `false`
///   **without scanning** — the per-candidate body scan (incl. on malformed
///   bodies) is skipped entirely;
/// - `mapped == current_model` (identity mapping) → `false` with the body
///   **reused as-is** (the caller keeps `prepared.body`, which already carries
///   the right model): no second scan, no splice. ⚠ Values are compared, never
///   spans — any span from the prepare scan is invalid after prepare's own
///   rewrite.
/// - otherwise → one bounded rewrite (validate-and-skip scan + byte splice via
///   [`crate::body::rewrite_json_model`], or the multipart form-part splice).
pub fn apply_with_current(
    mapping: &ModelMapping,
    service_name: &str,
    body: &mut Bytes,
    content_type: &str,
    current_model: Option<&str>,
) -> bool {
    // Guard: nothing to rewrite for an empty body.
    if body.is_empty() {
        return false;
    }
    let Some(mapped) = mapping.lookup(service_name) else {
        return false;
    };
    let Some(current) = current_model else {
        return false;
    };
    if current == mapped {
        // Identity mapping: reuse the body Bytes reference (O(1)) — no scan,
        // no splice.
        return false;
    }
    if crate::body::is_json(Some(content_type)) {
        if let Some(nb) = crate::body::rewrite_json_model(body, "model", mapped) {
            *body = nb;
            return true;
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

    // ----- B4: apply_with_current short-circuit (zero-copy plan §2.3) -----

    #[test]
    fn b4_identity_mapping_reuses_body_no_rewrite() {
        // Identity: the body already carries the mapped model -> false, body
        // untouched (no scan, no splice — the caller keeps the Bytes ref).
        let m = ModelMapping::single("a.static", "same");
        let mut body = Bytes::from(r#"{"model":"same","stream":true}"#);
        assert!(!apply_with_current(&m, "a.static", &mut body, JSON, Some("same")));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["model"], json!("same"));
        assert_eq!(v["stream"], json!(true));
    }

    #[test]
    fn b4_none_current_skips_scan_even_with_mapping() {
        // current_model None (missing/non-string/malformed body): skip the body
        // mapper entirely — the candidate never scans.
        let m = ModelMapping::single("a.static", "x");
        let mut body = Bytes::from(r#"{"model":"irrelevant","stream":true}"#);
        assert!(!apply_with_current(&m, "a.static", &mut body, JSON, None));
    }

    #[test]
    fn b4_absent_model_fields_skip_without_scan() {
        // No top-level string model in the body -> prepared.body_model is None
        // -> apply_with_current returns false without scanning (previously the
        // per-candidate rewrite scanned + found nothing).
        let m = ModelMapping::single("a.static", "x");
        for body in [
            Bytes::from(r#"{"model":5}"#),      // non-string model
            Bytes::from(r#"{"messages":[]}"#),  // missing model
            Bytes::from(r#"{broken"#),          // malformed
        ] {
            let mut b = body;
            assert!(!apply_with_current(&m, "a.static", &mut b, JSON, None));
        }
    }

    #[test]
    fn b4_real_rewrite_when_current_differs() {
        let m = ModelMapping::single("a.static", "mapped");
        let mut body = Bytes::from(r#"{"model":"org1/llama","stream":true}"#);
        assert!(apply_with_current(&m, "a.static", &mut body, JSON, Some("org1/llama")));
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["model"], json!("mapped"));
    }

    #[test]
    fn b4_multipart_identity_and_rewrite() {
        let mk = |model: &str| {
            Bytes::from(format!(
                "--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\n{model}\r\n\
                 --B--\r\n"
            ))
        };
        // Identity multipart -> false, body untouched (the model part already
        // carries the mapped value).
        let m = ModelMapping::single("a.static", "org/llama");
        let mut body = mk("org/llama");
        assert!(!apply_with_current(&m, "a.static", &mut body, MP, Some("org/llama")));
        assert!(String::from_utf8_lossy(&body).contains("org/llama"));

        // Non-identity multipart -> real rewrite of the model part.
        let m2 = ModelMapping::single("a.static", "mapped-name");
        let mut body = mk("org/llama");
        assert!(apply_with_current(&m2, "a.static", &mut body, MP, Some("org/llama")));
        let s = String::from_utf8_lossy(&body);
        assert!(s.contains("mapped-name"));
        assert!(!s.contains("org/llama"));
    }
}
