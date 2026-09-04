//! Pure request/response body helpers for the model-router (stage ②) and
//! model-mapper (stage ⑧) equivalents: extract the `model` field from a JSON or
//! (basic multipart) body, rewrite it to a resolved / mapped value, and parse a
//! multipart boundary off the `Content-Type`.
//!
//! No I/O, no allocation beyond what serde / the returned strings need. The
//! multipart handling is intentionally a small, robust parser — sufficient for
//! the GPUStack model-router / model-mapper form (single `model` text part),
//! mirroring `hygress_core::model_mapping`'s parser.

use bytes::Bytes;
use serde_json::Value;

/// Parse the multipart `boundary=` parameter off a `Content-Type` value, if any.
///
/// Returns `None` when the header is not multipart or carries no boundary.
pub fn parse_boundary(content_type: Option<&str>) -> Option<String> {
    let ct = content_type?;
    let lower = ct.to_ascii_lowercase();
    if !lower.contains("multipart/form-data") {
        return None;
    }
    for part in ct.split(';') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("boundary=") {
            let v = v.trim();
            let v = v.strip_prefix('"').and_then(|s| s.strip_suffix('"')).unwrap_or(v);
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// `true` when the request `Content-Type` marks a JSON body.
pub fn is_json(content_type: Option<&str>) -> bool {
    content_type
        .is_some_and(|ct| ct.to_ascii_lowercase().contains("application/json"))
}

/// Extract the top-level `model` field from a **JSON** body, or the value of the
/// first `model` form part from a **basic multipart** body. `None` for other
/// content types, malformed bodies, or a missing / non-string `model`.
pub fn extract_model(body: &Bytes, content_type: Option<&str>, model_key: &str) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    if is_json(content_type) {
        let v: Value = serde_json::from_slice(body).ok()?;
        return v
            .get(model_key)
            .and_then(|m| m.as_str())
            .map(|s| s.to_string());
    }
    if let Some(boundary) = parse_boundary(content_type) {
        return extract_multipart_model(body, &boundary);
    }
    None
}

/// Find the value of the first `name="model"` part in a basic multipart body.
pub fn extract_multipart_model(body: &Bytes, boundary: &str) -> Option<String> {
    let marker = format!("--{boundary}");
    let marker = marker.as_bytes();
    let slice = body.as_ref();

    let mut search_from = 0usize;
    while search_from <= slice.len() {
        let start = find_subseq(slice, marker, search_from)?;
        let after = start + marker.len();
        // Terminator boundary (`--boundary--`) ends the body.
        if slice.get(after..after + 2) == Some(b"--") {
            return None;
        }
        let next = find_subseq(slice, marker, after).unwrap_or(slice.len());
        let part = &slice[after..next];
        if let Some(sep) = find_subseq(part, b"\r\n\r\n", 0) {
            let header = &part[..sep];
            if contains_field(header, "model") {
                let value_start = after + sep + 4;
                let value_end = if part.ends_with(b"\r\n") {
                    next - 2
                } else {
                    next
                };
                if value_start <= value_end {
                    return Some(String::from_utf8_lossy(&slice[value_start..value_end]).into_owned());
                }
            }
        }
        search_from = next;
    }
    None
}

/// Rewrite the top-level `model` field of a **JSON** body to `value`, returning
/// the new body (unmodified on parse failure / non-object / missing field).
///
/// The rewrite replaces the field's value in place and re-serializes; unknown
/// fields and ordering are otherwise preserved by serde_json's object map.
pub fn rewrite_json_model(body: &Bytes, model_key: &str, value: &str) -> Option<Bytes> {
    let mut v: Value = serde_json::from_slice(body).ok()?;
    let Some(val) = v.as_object_mut()?.get_mut(model_key) else {
        // Missing `model` field → no-op (mirrors model_mapping::apply_json).
        return None;
    };
    if !val.is_string() {
        // Only a top-level string `model` field is touched; a non-string value
        // is left untouched (mirrors model_mapping::apply_json).
        return None;
    }
    *val = Value::String(value.to_string());
    serde_json::to_vec(&v).ok().map(Bytes::from)
}

/// Rewrite the value of the first `name="model"` part of a **basic multipart**
/// body to `value`. Returns `None` when there is no matching part.
pub fn rewrite_multipart_model(body: &Bytes, boundary: &str, value: &str) -> Option<Bytes> {
    let marker = format!("--{boundary}");
    let marker = marker.as_bytes();
    let slice = body.as_ref();
    let mut out = vec![0u8; body.len()];
    out.copy_from_slice(slice);

    let mut search_from = 0usize;
    while search_from <= out.len() {
        let Some(start) = find_subseq(&out, marker, search_from) else {
            break;
        };
        let after = start + marker.len();
        if out.get(after..after + 2) == Some(b"--") {
            break;
        }
        let next = find_subseq(&out, marker, after).unwrap_or(out.len());
        let part = &out[after..next];
        if let Some(sep) = find_subseq(part, b"\r\n\r\n", 0) {
            let header = &part[..sep];
            if contains_field(header, "model") {
                let value_start = after + sep + 4;
                let value_end = if part.ends_with(b"\r\n") {
                    next - 2
                } else {
                    next
                };
                if value_start <= value_end {
                    replace_bytes(&mut out, value_start, value_end, value.as_bytes());
                    return Some(Bytes::from(out));
                }
            }
        }
        search_from = next;
    }
    None
}

/// Dispatch `extract_model` / `rewrite_model` on the `Content-Type`.
pub fn rewrite_model_field(
    body: &Bytes,
    content_type: Option<&str>,
    model_key: &str,
    value: &str,
) -> Option<Bytes> {
    if is_json(content_type) {
        rewrite_json_model(body, model_key, value)
    } else if let Some(boundary) = parse_boundary(content_type) {
        rewrite_multipart_model(body, &boundary, value)
    } else {
        None
    }
}

/// `true` when a multipart part header block carries `name="model"`.
fn contains_field(header: &[u8], field: &str) -> bool {
    let needle = format!("name=\"{field}\"");
    find_subseq(header, needle.as_bytes(), 0).is_some()
}

/// Replace `hay[start..end]` with `new` (growing or shrinking the vec).
fn replace_bytes(hay: &mut Vec<u8>, start: usize, end: usize, new: &[u8]) {
    debug_assert!(start <= end, "replace_bytes: start > end");
    let tail: Vec<u8> = hay[end..].to_vec();
    hay.truncate(start);
    hay.extend_from_slice(new);
    hay.extend_from_slice(&tail);
}

/// Naive byte-subsequence search (small control-plane bodies; no `memchr` dep).
fn find_subseq(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() {
        return Some(from);
    }
    if hay.len() < from + needle.len() {
        return None;
    }
    let last = hay.len() - needle.len();
    let mut i = from.min(last);
    while i <= last {
        if &hay[i..i + needle.len()] == needle {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const MODEL: &str = "application/json";

    #[test]
    fn boundary_parsing() {
        assert_eq!(
            parse_boundary(Some("multipart/form-data; boundary=XYZ-123")),
            Some("XYZ-123".to_string())
        );
        assert_eq!(
            parse_boundary(Some("Multipart/Form-Data; boundary=\"quoted b\"")),
            Some("quoted b".to_string())
        );
        assert_eq!(parse_boundary(Some("application/json")), None);
        assert_eq!(parse_boundary(Some("multipart/form-data")), None);
        assert_eq!(parse_boundary(None), None);
    }

    #[test]
    fn is_json_case_insensitive() {
        assert!(is_json(Some("Application/JSON; charset=utf-8")));
        assert!(!is_json(Some("text/event-stream")));
        assert!(!is_json(None));
    }

    #[test]
    fn extract_json_model_top_level_only() {
        let body = Bytes::from(
            r#"{"model":"org-1/llama-3-8b","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
        );
        assert_eq!(
            extract_model(&body, Some(MODEL), "model"),
            Some("org-1/llama-3-8b".to_string())
        );
        // A nested "model" (not top-level) is not the model field.
        let nested = Bytes::from(r#"{"meta":{"model":"x"},"model":"top"}"#);
        assert_eq!(
            extract_model(&nested, Some(MODEL), "model"),
            Some("top".to_string())
        );
        // Non-string model -> None.
        let num = Bytes::from(r#"{"model":5}"#);
        assert_eq!(extract_model(&num, Some(MODEL), "model"), None);
        // Malformed JSON -> None (never panics).
        let bad = Bytes::copy_from_slice(br#"{broken"#);
        assert_eq!(extract_model(&bad, Some(MODEL), "model"), None);
        // Empty -> None.
        assert_eq!(extract_model(&Bytes::new(), Some(MODEL), "model"), None);
    }

    #[test]
    fn extract_model_custom_key() {
        let body = Bytes::from(r#"{"llm":"gpt-4o"}"#);
        assert_eq!(extract_model(&body, Some(MODEL), "llm"), Some("gpt-4o".to_string()));
        assert_eq!(extract_model(&body, Some(MODEL), "model"), None);
    }

    fn multipart(model_value: &str) -> Bytes {
        Bytes::from(
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
                 --B--\r\n",
            ),
        )
    }

    const MP: &str = "multipart/form-data; boundary=B";

    #[test]
    fn extract_multipart_model() {
        let body = multipart("org-1/llama");
        assert_eq!(
            extract_model(&body, Some(MP), "model"),
            Some("org-1/llama".to_string())
        );
        // No model part -> None.
        let no_model = Bytes::from(
            "--B\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nXYZ\r\n--B--\r\n",
        );
        assert_eq!(extract_model(&no_model, Some(MP), "model"), None);
    }

    #[test]
    fn rewrite_json_model_replaces_and_preserves() {
        let body = Bytes::from(
            r#"{"model":"old","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
        );
        let out = rewrite_model_field(&body, Some(MODEL), "model", "new-model").unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["model"], json!("new-model"));
        assert_eq!(v["stream"], json!(true));
        assert_eq!(v["messages"][0]["role"], json!("user"));
    }

    #[test]
    fn rewrite_json_model_non_string_is_noop() {
        let body = Bytes::from(r#"{"model":5}"#);
        assert!(rewrite_model_field(&body, Some(MODEL), "model", "x").is_none());
    }

    #[test]
    fn rewrite_multipart_model_only_model_part() {
        let body = multipart("org-1/llama");
        let out = rewrite_multipart_model(&body, "B", "mapped-name").unwrap();
        let s = String::from_utf8(out.to_vec()).unwrap();
        assert!(s.contains("name=\"model\"\r\n\r\nmapped-name\r\n"));
        assert!(s.contains("XYZ")); // other part untouched
        assert!(s.ends_with("--B--\r\n"));
    }

    #[test]
    fn extract_on_non_model_body_types_is_none() {
        let plain = Bytes::from("not a model body");
        assert_eq!(extract_model(&plain, Some("text/plain"), "model"), None);
        assert_eq!(extract_model(&plain, None, "model"), None);
    }
}
