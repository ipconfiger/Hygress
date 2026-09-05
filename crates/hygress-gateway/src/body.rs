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
// `Value` is only referenced by the test module (via `super::*`) — the
// production model-router / model-mapper path is a bounded targeted scan.
#[cfg(test)]
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
        // Bounded targeted scan of the top-level object only (H4): the JSON
        // contract is a top-level string `model`, so the full document never
        // needs to be materialized into a `serde_json::Value` DOM.
        return match scan_top_level_value(body, model_key) {
            Ok(Some(v)) => v.decoded,
            _ => None,
        };
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
/// The rewrite splices the bytes of the quoted string token in place (H4) —
/// no `serde_json::Value` DOM round-trip, so the rest of the document is
/// preserved byte-for-byte (whitespace and field ordering included).
pub fn rewrite_json_model(body: &Bytes, model_key: &str, value: &str) -> Option<Bytes> {
    match scan_top_level_value(body, model_key) {
        Ok(Some(v)) if v.decoded.is_some() => {
            let (start, end) = v.span;
            // The value token includes its quotes; replace the whole token.
            let encoded = encode_json_string(value);
            let mut out = Vec::with_capacity(body.len() - (end - start) + encoded.len());
            out.extend_from_slice(&body[..start]);
            out.extend_from_slice(encoded.as_bytes());
            out.extend_from_slice(&body[end..]);
            Some(Bytes::from(out))
        }
        // Missing `model` field, malformed body, or a non-string value → no-op
        // (mirrors model_mapping::apply_json).
        _ => None,
    }
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

/// `true` when the body's top-level `model` value already equals `value`
/// (R-5 identity short-circuit): the stage-② overwrite can skip the full-body
/// splice when the resolved value is already present. One bounded scan; no
/// output allocation.
pub fn model_field_equals(
    body: &Bytes,
    content_type: Option<&str>,
    model_key: &str,
    value: &str,
) -> bool {
    if body.is_empty() {
        return false;
    }
    if is_json(content_type) {
        match scan_top_level_value(body, model_key) {
            Ok(Some(v)) => v.decoded.as_deref() == Some(value),
            _ => false,
        }
    } else if let Some(boundary) = parse_boundary(content_type) {
        extract_multipart_model(body, &boundary).as_deref() == Some(value)
    } else {
        false
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

// ---------------------------------------------------------------------------
// Bounded top-level JSON scanner (H4)
// ---------------------------------------------------------------------------
//
// The model-router / model-mapper contract is a **top-level string** `model`
// field, so extracting or rewriting it never needs a full `serde_json::Value`
// DOM. These helpers tokenize the top-level object only: keys are decoded, the
// target value is decoded if it is a string, and every other value is skipped
// token-by-token without allocating (recursion bounded by [`MAX_JSON_DEPTH`],
// matching serde_json's own limit, so a hostile deeply-nested document cannot
// overflow the stack — malformed input yields `None`, exactly like the DOM
// parse it replaces).

/// serde_json's default recursion limit (a deeper document is treated as
/// malformed here, matching the previous `from_slice` behavior).
const MAX_JSON_DEPTH: usize = 128;

/// The located top-level value for the scanned key (last occurrence wins,
/// matching serde_json's map semantics for duplicate keys).
struct TopLevelValue {
    /// Byte span `[start, end)` of the value token (end exclusive).
    span: (usize, usize),
    /// The decoded string when the value is a JSON string (`None` for a
    /// non-string value — still "present", just not rewritable).
    decoded: Option<String>,
}

/// Scan the **top-level object** of `body` for `key`, skipping every other
/// value without materializing the document.
///
/// Returns `Ok(None)` when the key is absent, `Ok(Some(..))` for the last
/// occurrence (duplicate keys: last wins, like `serde_json::Map`), and `Err(())`
/// when the body is not a well-formed JSON object.
fn scan_top_level_value(body: &[u8], key: &str) -> Result<Option<TopLevelValue>, ()> {
    let mut pos = skip_ws(body, 0);
    if body.get(pos) != Some(&b'{') {
        return Err(());
    }
    pos += 1;
    let mut last: Option<TopLevelValue> = None;
    loop {
        pos = skip_ws(body, pos);
        match body.get(pos) {
            Some(b'}') => {
                // Trailing content must be whitespace-only (like
                // `serde_json::from_slice`); any other trailing byte makes the
                // document malformed for the model-router contract.
                if skip_ws(body, pos + 1) != body.len() {
                    return Err(());
                }
                return Ok(last);
            }
            Some(b',') => {
                let after = skip_ws(body, pos + 1);
                if body.get(after) == Some(&b'}') {
                    return Err(()); // trailing comma (serde rejects)
                }
                pos = after;
            }
            Some(b'"') => {
                let (k, after_key) = parse_json_string(body, pos).ok_or(())?;
                pos = skip_ws(body, after_key);
                if body.get(pos) != Some(&b':') {
                    return Err(());
                }
                pos = skip_ws(body, pos + 1);
                if k == key {
                    last = if body.get(pos) == Some(&b'"') {
                        let (decoded, end) = parse_json_string(body, pos).ok_or(())?;
                        Some(TopLevelValue {
                            span: (pos, end),
                            decoded: Some(decoded),
                        })
                    } else {
                        Some(TopLevelValue {
                            span: (pos, pos),
                            decoded: None,
                        })
                    };
                }
                pos = skip_json_value(body, pos, 0).ok_or(())?;
            }
            _ => return Err(()),
        }
    }
}

/// Decode a JSON string token at `from` (must point at `"`), returning the
/// decoded value and the position **after** the closing quote. `None` on a
/// malformed / unterminated string.
///
/// Every byte is handled in O(1) (`< 0x80` → push directly, multi-byte UTF-8 →
/// a bounded ≤4-byte window), so the scan stays near-linear and never validates
/// the whole remaining slice for an ordinary byte.
fn parse_json_string(b: &[u8], from: usize) -> Option<(String, usize)> {
    if b.get(from) != Some(&b'"') {
        return None;
    }
    let mut out = String::new();
    let mut i = from + 1;
    loop {
        let (next, ch) = json_string_char(b, i).ok()?;
        match ch {
            Some(c) => out.push(c),
            None => return Some((out, next)), // closing quote
        }
        i = next;
    }
}

/// Validate-and-advance a JSON string token starting at `from` (must point at
/// `"`), **without materializing** the decoded `String` (B3) — the skip path
/// for non-target keys/values. Returns the position after the closing quote.
///
/// Validation is identical to [`parse_json_string`] (same escape table, control
/// chars, bounded multi-byte UTF-8, surrogate pairing), so a malformed token in
/// a **skipped** string still rejects the document exactly like serde — the
/// skip path is validate-and-advance, never merely-advance.
fn skip_json_string(b: &[u8], from: usize) -> Option<usize> {
    if b.get(from) != Some(&b'"') {
        return None;
    }
    let mut i = from + 1;
    loop {
        let (next, ch) = json_string_char(b, i).ok()?;
        match ch {
            Some(_) => i = next,
            None => return Some(next), // closing quote
        }
    }
}

/// Decode the character beginning at `i` inside a JSON string token.
///
/// Returns `(next_i, Some(ch))` for an ordinary character / escape to emit,
/// `(next_i, None)` for the closing quote (the caller returns), and `Err(())`
/// for malformed input (bad escape, control char, lone surrogate, invalid
/// UTF-8). Surrogate-pair escapes are combined into the scalar here.
fn json_string_char(b: &[u8], i: usize) -> Result<(usize, Option<char>), ()> {
    let c = *b.get(i).ok_or(())?;
    match c {
        b'"' => Ok((i + 1, None)),
        b'\\' => {
            let esc = *b.get(i + 1).ok_or(())?;
            match esc {
                b'"' => Ok((i + 2, Some('"'))),
                b'\\' => Ok((i + 2, Some('\\'))),
                b'/' => Ok((i + 2, Some('/'))),
                b'b' => Ok((i + 2, Some('\u{0008}'))),
                b'f' => Ok((i + 2, Some('\u{000C}'))),
                b'n' => Ok((i + 2, Some('\n'))),
                b'r' => Ok((i + 2, Some('\r'))),
                b't' => Ok((i + 2, Some('\t'))),
                b'u' => {
                    let cp = parse_hex4(b, i + 2).ok_or(())?;
                    if (0xD800..=0xDBFF).contains(&cp) {
                        // High surrogate: the next escape MUST be a low
                        // surrogate.
                        if b.get(i + 6) != Some(&b'\\') || b.get(i + 7) != Some(&b'u') {
                            return Err(()); // lone leading surrogate
                        }
                        let low = parse_hex4(b, i + 8).ok_or(())?;
                        if !(0xDC00..=0xDFFF).contains(&low) {
                            return Err(());
                        }
                        let scalar = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                        let ch = char::from_u32(scalar).ok_or(())?;
                        // Total escape: `\u`(2) + 4 + `\u`(2) + 4 = 12 bytes.
                        Ok((i + 12, Some(ch)))
                    } else if (0xDC00..=0xDFFF).contains(&cp) {
                        Err(()) // lone low surrogate
                    } else {
                        let ch = char::from_u32(cp).ok_or(())?;
                        Ok((i + 6, Some(ch))) // total escape: `\u`(2) + 4 = 6 bytes.
                    }
                }
                _ => Err(()),
            }
        }
        c if c < 0x20 => Err(()),
        c if c < 0x80 => Ok((i + 1, Some(c as char))),
        _ => {
            // A multi-byte UTF-8 char: decode from a bounded ≤4-byte window.
            let (ch, len) = decode_utf8(b, i).ok_or(())?;
            Ok((i + len, Some(ch)))
        }
    }
}

/// Parse exactly 4 hex digits at `from` (the `XXXX` of `\uXXXX`).
fn parse_hex4(b: &[u8], from: usize) -> Option<u32> {
    let h = b.get(from..from + 4)?;
    let mut v: u32 = 0;
    for &d in h {
        v = (v << 4) | hex_digit(d)?;
    }
    Some(v)
}

/// The numeric value of one hex nibble byte.
fn hex_digit(d: u8) -> Option<u32> {
    match d {
        b'0'..=b'9' => Some((d - b'0') as u32),
        b'a'..=b'f' => Some((d - b'a' + 10) as u32),
        b'A'..=b'F' => Some((d - b'A' + 10) as u32),
        _ => None,
    }
}

/// Decode one multi-byte UTF-8 char starting at `i` from a **bounded** ≤4-byte
/// window (never the whole remaining slice). `None` for an invalid sequence.
fn decode_utf8(b: &[u8], i: usize) -> Option<(char, usize)> {
    let c0 = *b.get(i)?;
    let (cp, len) = if c0 < 0xE0 {
        // 2-byte: 110xxxxx 10xxxxxx
        let c1 = *b.get(i + 1)?;
        if c0 < 0xC2 || c1 & 0xC0 != 0x80 {
            return None;
        }
        (((c0 as u32 & 0x1F) << 6) | (c1 as u32 & 0x3F), 2)
    } else if c0 < 0xF0 {
        // 3-byte: 1110xxxx 10xxxxxx 10xxxxxx (with overlong / surrogate
        // rejection).
        let c1 = *b.get(i + 1)?;
        let c2 = *b.get(i + 2)?;
        if (c0 == 0xE0 && c1 < 0xA0)
            || (c0 == 0xED && c1 >= 0xA0)
            || c1 & 0xC0 != 0x80
            || c2 & 0xC0 != 0x80
        {
            return None;
        }
        (
            ((c0 as u32 & 0x0F) << 12) | ((c1 as u32 & 0x3F) << 6) | (c2 as u32 & 0x3F),
            3,
        )
    } else if c0 < 0xF5 {
        // 4-byte: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx (with overlong / too-large
        // rejection).
        let c1 = *b.get(i + 1)?;
        let c2 = *b.get(i + 2)?;
        let c3 = *b.get(i + 3)?;
        if (c0 == 0xF0 && c1 < 0x90)
            || (c0 == 0xF4 && c1 >= 0x90)
            || c1 & 0xC0 != 0x80
            || c2 & 0xC0 != 0x80
            || c3 & 0xC0 != 0x80
        {
            return None;
        }
        (
            ((c0 as u32 & 0x07) << 18)
                | ((c1 as u32 & 0x3F) << 12)
                | ((c2 as u32 & 0x3F) << 6)
                | (c3 as u32 & 0x3F),
            4,
        )
    } else {
        return None;
    };
    char::from_u32(cp).map(|ch| (ch, len))
}

/// Skip one complete JSON value starting at `i`, returning the position after
/// it (no allocation; recursion bounded by [`MAX_JSON_DEPTH`]).
fn skip_json_value(b: &[u8], i: usize, depth: usize) -> Option<usize> {
    if depth > MAX_JSON_DEPTH {
        return None;
    }
    let p = skip_ws(b, i);
    match *b.get(p)? {
        b'"' => skip_json_string(b, p),
        b'{' => {
            let mut q = skip_ws(b, p + 1);
            if b.get(q) == Some(&b'}') {
                return Some(q + 1);
            }
            loop {
                let after_key = skip_json_string(b, q)?;
                let mut q2 = skip_ws(b, after_key);
                if b.get(q2)? != &b':' {
                    return None;
                }
                q2 = skip_ws(b, q2 + 1);
                q2 = skip_json_value(b, q2, depth + 1)?;
                q2 = skip_ws(b, q2);
                match b.get(q2)? {
                    b',' => q = skip_ws(b, q2 + 1),
                    b'}' => return Some(q2 + 1),
                    _ => return None,
                }
            }
        }
        b'[' => {
            let mut q = skip_ws(b, p + 1);
            if b.get(q) == Some(&b']') {
                return Some(q + 1);
            }
            loop {
                q = skip_json_value(b, q, depth + 1)?;
                q = skip_ws(b, q);
                match b.get(q)? {
                    b',' => q = skip_ws(b, q + 1),
                    b']' => return Some(q + 1),
                    _ => return None,
                }
            }
        }
        b't' if b.get(p..p + 4) == Some(b"true") => Some(p + 4),
        b'f' if b.get(p..p + 5) == Some(b"false") => Some(p + 5),
        b'n' if b.get(p..p + 4) == Some(b"null") => Some(p + 4),
        c if c.is_ascii_digit() || c == b'-' => skip_json_number(b, p),
        _ => None,
    }
}

/// Skip a JSON number per serde's grammar:
/// `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?` — a leading `0` must not be
/// followed by a digit, and a fraction / exponent needs at least one digit.
///
/// Returns `None` when the bytes do not form a valid number, so a malformed
/// number in an unrelated field rejects the whole document exactly like
/// `serde_json::from_slice` (a model must not be extracted from JSON serde
/// would refuse).
fn skip_json_number(b: &[u8], i: usize) -> Option<usize> {
    let start = i;
    let mut p = i;
    // Optional leading minus.
    if b.get(p) == Some(&b'-') {
        p += 1;
    }
    // Integer part: `0` or `[1-9][0-9]*` (no leading zero).
    match *b.get(p)? {
        b'0' => {
            p += 1;
            if b.get(p).is_some_and(|c| c.is_ascii_digit()) {
                return None; // serde rejects "01"
            }
        }
        c if c.is_ascii_digit() => {
            while b.get(p).is_some_and(|c| c.is_ascii_digit()) {
                p += 1;
            }
        }
        _ => return None,
    }
    // Optional fraction: `.` + at least one digit.
    if b.get(p) == Some(&b'.') {
        p += 1;
        if !b.get(p).is_some_and(|c| c.is_ascii_digit()) {
            return None;
        }
        while b.get(p).is_some_and(|c| c.is_ascii_digit()) {
            p += 1;
        }
    }
    // Optional exponent: `e`/`E`, optional sign, at least one digit.
    if matches!(b.get(p), Some(b'e') | Some(b'E')) {
        p += 1;
        if matches!(b.get(p), Some(b'+') | Some(b'-')) {
            p += 1;
        }
        if !b.get(p).is_some_and(|c| c.is_ascii_digit()) {
            return None;
        }
        while b.get(p).is_some_and(|c| c.is_ascii_digit()) {
            p += 1;
        }
    }
    if p == start {
        None
    } else {
        Some(p)
    }
}

/// Advance past JSON insignificant whitespace.
fn skip_ws(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    i
}

/// JSON-encode `s` as a complete string token (quotes included), matching
/// serde_json's emission: `"`/`\` escaped, control chars < 0x20 as `\n\r\t\b\f`
/// or `\u00xx`, and non-ASCII left raw.
fn encode_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
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

    // -------------------------------------------------------------------
    // H4 scanner parity tests (bounded targeted JSON scan vs serde DOM).
    // -------------------------------------------------------------------

    /// Parse `s` with the full DOM and return the top-level `model` string, or
    /// the sentinel for "not a top-level string or document invalid" so a
    /// parity assertion can tell absent-from-invalid apart.
    fn serde_model(s: &str) -> Option<String> {
        let v: Value = serde_json::from_str(s).ok()?;
        v.get("model").and_then(|m| m.as_str()).map(|x| x.to_string())
    }

    #[test]
    fn escaped_quotes_and_backslashes_decode_like_serde() {
        // `\"` and `\\` inside the model string both decode and rewrite.
        for src in [
            r#"{"model":"a\"b"}"#,   // model = a"b
            r#"{"model":"a\\b"}"#,   // model = a\b
            r#"{"model":"a\"b\\c\"d"}"#, // mixed
        ] {
            let got = extract_model(&Bytes::from(src), Some(MODEL), "model");
            assert_eq!(got, serde_model(src), "extract parity for {src}");
            assert!(got.is_some());

            let out = rewrite_model_field(&Bytes::from(src), Some(MODEL), "model", "x")
                .expect("rewrite must succeed");
            let v: Value = serde_json::from_slice(&out).expect("rewritten body is valid JSON");
            assert_eq!(v["model"], json!("x"), "rewrite parity for {src}");
        }
    }

    #[test]
    fn unicode_escapes_match_serde_including_surrogates() {
        // BMP escapes.
        let bmp = r#"{"model":"\u00e9clair"}"#; // é
        assert_eq!(
            extract_model(&Bytes::from(bmp), Some(MODEL), "model"),
            Some("éclair".to_string())
        );
        assert_eq!(extract_model(&Bytes::from(bmp), Some(MODEL), "model"), serde_model(bmp));

        // Non-BMP via a surrogate pair: "\uD83D\uDE00" == U+1F600 😀, and a
        // real rocket "\uD83D\uDE80" == U+1F680 🚀. Both must decode to the
        // same scalar serde produces.
        let grin = r#"{"model":"\uD83D\uDE00"}"#;
        assert_eq!(
            extract_model(&Bytes::from(grin), Some(MODEL), "model"),
            Some("😀".to_string())
        );
        assert_eq!(extract_model(&Bytes::from(grin), Some(MODEL), "model"), serde_model(grin));

        let rocket = r#"{"model":"\uD83D\uDE80"}"#;
        assert_eq!(
            extract_model(&Bytes::from(rocket), Some(MODEL), "model"),
            Some("🚀".to_string())
        );
        assert_eq!(extract_model(&Bytes::from(rocket), Some(MODEL), "model"), serde_model(rocket));

        // Rewrite a body whose model is a surrogate pair (the splice must
        // target the right token and keep the document valid).
        let out = rewrite_model_field(&Bytes::from(rocket), Some(MODEL), "model", "x").unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["model"], json!("x"));

        // A lone high surrogate is invalid JSON (serde rejects it) -> None.
        let lone = r#"{"model":"\uD83D"}"#;
        assert_eq!(extract_model(&Bytes::from(lone), Some(MODEL), "model"), None);
        assert_eq!(serde_model(lone), None);

        // Raw (multi-byte) unicode decodes as-is.
        let raw = "{\"model\":\"模型-🚀\"}";
        assert_eq!(
            extract_model(&Bytes::from(raw), Some(MODEL), "model"),
            Some("模型-🚀".to_string())
        );
        assert_eq!(extract_model(&Bytes::from(raw), Some(MODEL), "model"), serde_model(raw));
    }

    #[test]
    fn nested_model_keys_are_ignored() {
        // Nested under an object / array is not the top-level model field.
        let nested_obj = Bytes::from(r#"{"meta":{"model":"nested"},"model":"top"}"#);
        assert_eq!(extract_model(&nested_obj, Some(MODEL), "model"), Some("top".to_string()));
        let only_nested = Bytes::from(r#"{"meta":{"model":"nested"}}"#);
        assert_eq!(extract_model(&only_nested, Some(MODEL), "model"), None);
        let in_array = Bytes::from(r#"{"list":[{"model":"x"}]}"#);
        assert_eq!(extract_model(&in_array, Some(MODEL), "model"), None);
    }

    #[test]
    fn duplicate_model_keys_are_last_wins() {
        let dup = r#"{"model":"first","model":"second"}"#;
        assert_eq!(
            extract_model(&Bytes::from(dup), Some(MODEL), "model"),
            Some("second".to_string())
        );
        assert_eq!(extract_model(&Bytes::from(dup), Some(MODEL), "model"), serde_model(dup));

        // The rewrite replaces the LAST occurrence (serde `Map` semantics).
        let out = rewrite_model_field(&Bytes::from(dup), Some(MODEL), "model", "x").unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["model"], json!("x"));
    }

    #[test]
    fn non_string_and_missing_model_are_none() {
        for src in [
            r#"{"model":5}"#,
            r#"{"model":null}"#,
            r#"{"model":true}"#,
            r#"{"model":{"nested":"x"}}"#,
            r#"{"model":[1,2]}"#,
            r#"{"messages":[]}"#,
            "{}",
        ] {
            let got = extract_model(&Bytes::from(src), Some(MODEL), "model");
            assert_eq!(got, serde_model(src), "parity for {src}");
            assert_eq!(got, None);
        }
    }

    #[test]
    fn malformed_documents_are_rejected_like_serde() {
        // Trailing garbage after the object, trailing commas, bad numbers,
        // unclosed string/object — the scanner must NOT extract a model serde
        // would refuse the document for.
        for src in [
            r#"{"model":"x"} garbage"#,
            r#"{"model":"x"}{"a":1}"#,
            r#"{"model":"x",}"#,
            r#"{"bad":1.2.3,"model":"x"}"#,
            r#"{"bad":01,"model":"x"}"#,
            r#"{"bad":1e,"model":"x"}"#,
            r#"{"bad":-,"model":"x"}"#,
            r#"{"model":"x""#,
            r#"{"model":"x", "y":}"#,
        ] {
            let got = extract_model(&Bytes::from(src), Some(MODEL), "model");
            assert_eq!(got, serde_model(src), "parity for {src}");
            assert_eq!(got, None, "must reject {src}");
        }
    }

    #[test]
    fn malformed_numbers_inside_values_are_rejected_like_serde() {
        // A malformed number anywhere in the document must reject the whole
        // document (the model must not be extracted from JSON serde refuses).
        for src in [
            r#"{"bad":1.2.3,"model":"x"}"#,
            r#"{"bad":01,"model":"x"}"#,
            r#"{"bad":-,"model":"x"}"#,
            r#"{"bad":1e,"model":"x"}"#,
            r#"{"bad":[1.], "model":"x"}"#,
            r#"{"bad":1e-,"model":"x"}"#,
        ] {
            assert_eq!(
                extract_model(&Bytes::from(src), Some(MODEL), "model"),
                serde_model(src),
                "number parity for {src}"
            );
            assert_eq!(extract_model(&Bytes::from(src), Some(MODEL), "model"), None);
        }
        // Valid numbers still scan fine (model extracted).
        let ok = r#"{"bad":-0.5e+2,"model":"ok"}"#;
        assert_eq!(extract_model(&Bytes::from(ok), Some(MODEL), "model"), Some("ok".to_string()));
    }

    #[test]
    fn skip_path_validates_bad_escape_in_skipped_string() {
        // B3: the skip path is validate-and-advance. A bad escape inside a
        // NON-target (skipped) string must reject the whole document — serde
        // refuses it, so the model must not be extracted.
        let src = r#"{"bad":"\q","model":"x"}"#;
        assert_eq!(extract_model(&Bytes::from(src), Some(MODEL), "model"), None);
        assert_eq!(serde_model(src), None);

        // The same holds for an escape inside a skipped NESTED value.
        let nested = r#"{"meta":{"note":"\q"},"model":"x"}"#;
        assert_eq!(extract_model(&Bytes::from(nested), Some(MODEL), "model"), None);
        assert_eq!(serde_model(nested), None);
    }

    #[test]
    fn skip_path_validates_invalid_utf8_in_skipped_string() {
        // B3: an invalid UTF-8 byte inside a skipped string must reject the
        // document (validate-and-advance, not merely-advance) — a bare
        // "advance without validation" skip would have accepted it. The oracle
        // is a byte-level serde parse (an invalid-UTF-8 doc is not a `&str`).
        let bytes = {
            let mut b = Vec::from(br#"{"bad":"ok"#);
            b.push(0xFF); // invalid UTF-8 inside a SKIPPED string value
            b.extend_from_slice(br#","model":"x"}"#);
            b
        };
        let body = Bytes::from(bytes);
        assert_eq!(extract_model(&body, Some(MODEL), "model"), None);
        let serde_ok = serde_json::from_slice::<Value>(&body).is_err();
        assert!(serde_ok, "serde must also reject the invalid-UTF-8 document");
    }

    #[test]
    fn skip_path_accepts_valid_multibyte_utf8_in_skipped_string() {
        // B3: valid multi-byte UTF-8 inside a skipped string is accepted and
        // discarded (the model extraction still succeeds, matching serde).
        let src = "{\"bad\":\"café-🚀 中文\",\"model\":\"x\"}";
        assert_eq!(extract_model(&Bytes::from(src), Some(MODEL), "model"), Some("x".to_string()));
        assert_eq!(serde_model(src), Some("x".to_string()));

        // Escapes (incl. surrogate pairs) inside a skipped string are also
        // validated-and-advanced:
        let esc = r#"{"bad":"\uD83D\uDE80 \n \t \\","model":"x"}"#;
        assert_eq!(extract_model(&Bytes::from(esc), Some(MODEL), "model"), Some("x".to_string()));
        assert_eq!(serde_model(esc), Some("x".to_string()));
    }

    #[test]
    fn large_valid_body_scans_in_linear_time() {
        // Regression guard for the O(n²) bug: every ordinary ASCII byte in a
        // skipped string used to validate the entire remaining slice
        // (`from_utf8(&b[i..])`), making a 64KB body ~59ms and 512KB ~2.3s.
        // The linear path is tens of microseconds, so these generous bounds
        // catch the quadratic regression with large headroom while staying
        // robust on slow CI.
        let content = vec![b'a'; 64 * 1024];
        let body_64k = Bytes::from(make_chat_body(&content));
        let t0 = std::time::Instant::now();
        assert_eq!(
            extract_model(&body_64k, Some(MODEL), "model"),
            Some("org-1/llama-3-8b".to_string())
        );
        let d64 = t0.elapsed();

        let content = vec![b'a'; 256 * 1024];
        let body_256k = Bytes::from(make_chat_body(&content));
        let t0 = std::time::Instant::now();
        assert_eq!(
            extract_model(&body_256k, Some(MODEL), "model"),
            Some("org-1/llama-3-8b".to_string())
        );
        let d256 = t0.elapsed();

        assert!(
            d64.as_secs_f64() < 0.008,
            "64KB scan took {:?} — expected linear (µs-range); O(n²) regression?",
            d64
        );
        assert!(
            d256.as_secs_f64() < 0.032,
            "256KB scan took {:?} — expected ~4x of the 64KB linear time; O(n²) regression?",
            d256
        );
    }

    /// A chat-style JSON body with `content` bytes inside the user message.
    fn make_chat_body(content: &[u8]) -> String {
        // {"model":"...","messages":[{"role":"user","content":"<content>"}],"stream":true}
        let mut body = String::from(
            "{\"model\":\"org-1/llama-3-8b\",\"messages\":[{\"role\":\"user\",\"content\":\"",
        );
        for &b in content {
            body.push(b as char);
        }
        body.push_str("\"}],\"stream\":true}");
        body
    }
}

