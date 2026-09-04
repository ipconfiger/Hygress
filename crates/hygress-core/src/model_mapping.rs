//! Per-destination model name mapping (native equivalent of the
//! `gpustack-model-mapper` plugin, design §5.3 / §6.2).
//!
//! Key format is fixed by the contract and the two key spaces must not be
//! mixed (design §6.2): the mapping is keyed by the matchRule service name
//! `name.type` (**no port**), resolved from the SWRR-selected destination's
//! `name.type:port` by dropping the port.
//!
//! Application is a pure body mutation: the outbound `model` field of the
//! request body (JSON object or basic multipart form) is replaced with the
//! mapped model name for the selected destination.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `name.type` service → outbound body model name.
///
/// `rules` is an ordered list of `(service, model)` pairs; the **first**
/// entry for a given service wins (duplicates are a config smell validated at
/// the config level).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelMapping {
    pub rules: Vec<(String, String)>,
}

impl ModelMapping {
    pub fn new() -> Self {
        Self::default()
    }

    /// A mapping with a single rule.
    pub fn single(service: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            rules: vec![(service.into(), model.into())],
        }
    }

    /// Add a rule (appended; first match for a service wins on lookup).
    pub fn with_rule(mut self, service: impl Into<String>, model: impl Into<String>) -> Self {
        self.rules.push((service.into(), model.into()));
        self
    }

    /// Look up the outbound model name for a `name.type` service key.
    pub fn lookup(&self, service: &str) -> Option<&str> {
        self.rules
            .iter()
            .find(|(svc, _)| svc.as_str() == service)
            .map(|(_, model)| model.as_str())
    }

    /// Replace the `model` field of a JSON request body for `service`.
    ///
    /// Returns `true` when the body was rewritten. Only a top-level string
    /// `model` field is touched; anything else (non-object body, missing or
    /// non-string field, unmapped service) is left unchanged.
    pub fn apply_json(&self, service: &str, body: &mut Value) -> bool {
        let Some(model) = self.lookup(service) else {
            return false;
        };
        let Some(obj) = body.as_object_mut() else {
            return false;
        };
        let Some(val) = obj.get_mut("model") else {
            return false;
        };
        if val.is_string() {
            *val = Value::String(model.to_string());
            true
        } else {
            false
        }
    }

    /// Replace the `model` form field of a **basic** multipart request body.
    ///
    /// A part is matched when its headers contain `name="model"` (exact).
    /// Only the value section of the first matching part is rewritten; part
    /// headers and all other parts are untouched. Returns `true` on rewrite.
    ///
    /// This is intentionally a small, robust parser — sufficient for the
    /// GPUStack model-router multipart form (single `model` text part).
    pub fn apply_multipart(&self, service: &str, body: &mut Vec<u8>, boundary: &str) -> bool {
        let Some(model) = self.lookup(service) else {
            return false;
        };
        let marker = format!("--{boundary}");
        let marker = marker.as_bytes();

        let mut search_from = 0;
        while search_from <= body.len() {
            let Some(start) = find_subseq(body, marker, search_from) else {
                break;
            };
            let after = start + marker.len();
            // Terminator boundary (`--boundary--`) ends the body.
            if body.get(after..after + 2) == Some(b"--") {
                break;
            }
            let next = find_subseq(body, marker, after).unwrap_or(body.len());
            let part = &body[after..next];
            if let Some(sep) = find_subseq(part, b"\r\n\r\n", 0) {
                let header = &part[..sep];
                if header_contains_model_field(header) {
                    let value_start = after + sep + 4;
                    let value_end = if part.ends_with(b"\r\n") {
                        next - 2
                    } else {
                        next
                    };
                    if value_start <= value_end {
                        replace_bytes(body, value_start, value_end, model.as_bytes());
                        return true;
                    }
                }
            }
            search_from = next;
        }
        false
    }
}

/// `true` when a multipart part header block carries a `name="model"` field.
fn header_contains_model_field(header: &[u8]) -> bool {
    let needle = b"name=\"model\"";
    find_subseq(header, needle, 0).is_some()
}

/// Replace `hay[start..end]` with `new` (growing or shrinking the vec).
fn replace_bytes(hay: &mut Vec<u8>, start: usize, end: usize, new: &[u8]) {
    debug_assert!(start <= end, "replace_bytes: start > end");
    let tail: Vec<u8> = hay[end..].to_vec();
    hay.truncate(start);
    hay.extend_from_slice(new);
    hay.extend_from_slice(&tail);
}

/// Naive byte-subsequence search (sufficient for the small control-plane
/// bodies this crate touches; no `memchr` dependency allowed).
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

    #[test]
    fn lookup_by_service_key() {
        let m = ModelMapping::new()
            .with_rule("model-1-10.static", "llama-3-8b-instruct")
            .with_rule("provider-1.dns", "gpt-4o");
        assert_eq!(m.lookup("model-1-10.static"), Some("llama-3-8b-instruct"));
        assert_eq!(m.lookup("provider-1.dns"), Some("gpt-4o"));
        assert_eq!(m.lookup("unknown.static"), None);
    }

    #[test]
    fn lookup_first_rule_wins_on_duplicate_service() {
        let m = ModelMapping::new()
            .with_rule("a.static", "first")
            .with_rule("a.static", "second");
        assert_eq!(m.lookup("a.static"), Some("first"));
    }

    #[test]
    fn apply_json_rewrites_model_field() {
        let m = ModelMapping::single("model-1-10.static", "llama-3-8b-instruct");
        let mut body = json!({
            "model": "org1/llama-3-8b:adapter",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true
        });
        assert!(m.apply_json("model-1-10.static", &mut body));
        assert_eq!(body["model"], json!("llama-3-8b-instruct"));
        // Other fields untouched.
        assert_eq!(body["stream"], json!(true));
        assert_eq!(body["messages"][0]["role"], json!("user"));
    }

    #[test]
    fn apply_json_unmapped_service_is_noop() {
        let m = ModelMapping::single("a.static", "x");
        let mut body = json!({"model": "keep"});
        assert!(!m.apply_json("b.static", &mut body));
        assert_eq!(body["model"], json!("keep"));
    }

    #[test]
    fn apply_json_non_object_or_non_string_model_is_noop() {
        let m = ModelMapping::single("a.static", "x");
        let mut arr = json!([1, 2]);
        assert!(!m.apply_json("a.static", &mut arr));
        let mut num = json!({"model": 5});
        assert!(!m.apply_json("a.static", &mut num));
        let mut missing = json!({"prompt": "x"});
        assert!(!m.apply_json("a.static", &mut missing));
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
    fn apply_multipart_rewrites_model_part() {
        let m = ModelMapping::single("model-1-10.static", "llama-3-8b-instruct");
        let mut body = mp_body("org1/llama-3-8b:adapter");
        assert!(m.apply_multipart("model-1-10.static", &mut body, "B"));
        let s = String::from_utf8(body).unwrap();
        assert!(s.contains("name=\"model\"\r\n\r\nllama-3-8b-instruct\r\n"));
        // Other part value untouched.
        assert!(s.contains("XYZ"));
        // Structure intact.
        assert!(s.ends_with("--B--\r\n"));
    }

    #[test]
    fn apply_multipart_unmapped_or_missing_model_is_noop() {
        let m = ModelMapping::single("a.static", "x");
        let mut body = mp_body("keep");
        assert!(!m.apply_multipart("other.static", &mut body, "B"));
        assert!(String::from_utf8(body).unwrap().contains("keep"));

        let no_model =
            b"--B\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nXYZ\r\n--B--\r\n";
        let mut body2 = no_model.to_vec();
        assert!(!m.apply_multipart("a.static", &mut body2, "B"));
        assert_eq!(body2, no_model.to_vec());
    }
}
