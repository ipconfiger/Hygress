//! Pure request/response body helpers for the model-router (stage ②) and
//! model-mapper (stage ⑧) equivalents: extract the `model` field from a JSON or
//! (basic multipart) body, rewrite it to a resolved / mapped value, and parse a
//! multipart boundary off the `Content-Type`.
//!
//! No I/O, no allocation beyond what serde / the returned strings need. The
//! multipart handling shares the canonical `hygress_core::bytes` part-value
//! locator (the same one `hygress_core::model_mapping` uses), so the two
//! crates cannot silently diverge on boundary semantics (ORA3-M10). The JSON
//! top-level scans all run through ONE member-loop state machine
//! ([`top_level_members`], ORA3-M11), with `serde`-rule handling in a single
//! place.

use bytes::Bytes;
use hygress_core::bytes::{first_form_value_span, replace_bytes};
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
///
/// Thin wrapper over the shared [`first_form_value_span`] locator (the merged
/// multipart part scanner, ORA3-M10): same boundary / terminator /
/// line-ending semantics the historic per-crate loops implemented.
pub fn extract_multipart_model(body: &Bytes, boundary: &str) -> Option<String> {
    let (start, end) = first_form_value_span(body.as_ref(), boundary, "model")?;
    Some(String::from_utf8_lossy(&body[start..end]).into_owned())
}

/// Rewrite the top-level `model` field of a **JSON** body to `value`, returning
/// the new body (unmodified on parse failure / non-object / missing field).
///
/// The rewrite splices the bytes of the quoted string token in place (H4) —
/// no `serde_json::Value` DOM round-trip, so the rest of the document is
/// preserved byte-for-byte (whitespace and field ordering included).
pub fn rewrite_json_model(body: &Bytes, model_key: &str, value: &str) -> Option<Bytes> {
    match scan_top_level_value(body, model_key) {
        Ok(Some(v)) if v.decoded.is_some() => Some(splice_json_string_at(body, v.span, value)),
        // Missing `model` field, malformed body, or a non-string value → no-op
        // (mirrors model_mapping::apply_json).
        _ => None,
    }
}

/// Rewrite the value of the first `name="model"` part of a **basic multipart**
/// body to `value`. Returns `None` when there is no matching part.
pub fn rewrite_multipart_model(body: &Bytes, boundary: &str, value: &str) -> Option<Bytes> {
    let mut out = body.to_vec();
    let (start, end) = first_form_value_span(&out, boundary, "model")?;
    replace_bytes(&mut out, start, end, value.as_bytes());
    Some(Bytes::from(out))
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

/// AM-2 (pin §2.8) streaming-metering gate: force `stream_options.include_usage`
/// on OpenAI completions-style **streaming** outbound bodies of model-route
/// traffic so the upstream emits the canonical final usage chunk (the Higress
/// ai-proxy baseline injects the same option — upstream PR #4258 provides the
/// `disableStreamUsageStats` opt-out and #2524 restricts the injection to the
/// OpenAI chat/completions family).
///
/// Returns `Some(new_body)` only when EVERY gate passes:
/// 1. `is_model_route` — mirror / non-model passthrough bodies are never
///    rewritten: they are not metered, and their upstream may be an engine
///    that does not understand `stream_options` (an older vLLM 400s on the
///    unknown parameter — the reason #4258 exists).
/// 2. JSON `content_type` with a non-empty body.
/// 3. the outbound path is an OpenAI completions shape — ends with
///    `/chat/completions` or `/completions` (covers `/v1` and `/v1-openai`
///    prefixes; excludes embeddings/images/audio/… so no "Unknown parameter"
///    400 is risked on endpoints that never stream usage).
/// 4. the top-level `stream` field is the literal `true` (missing / `false` /
///    a string / nested are all left untouched — only a top-level boolean true
///    selects the streaming meter path).
/// 5. the top-level object has **no** `stream_options` key. A client that sent
///    one keeps its own explicit preference (respecting the #4147-style
///    clients that deliberately ask for a usage-less stream). Residual
///    divergence vs. the wasm baseline is limited to exactly those explicit
///    clients; the mainstream OpenAI-SDK shape — `stream_options` absent — is
///    what this fixes.
///
/// `None` = no rewrite (the caller keeps its `Bytes` reference — the R-5
/// zero-allocation short path). All decisions precede the single splice, which
/// scans the **top-level object only** (H4 — no `serde_json::Value` DOM, same
/// validate-and-advance semantics as the model-key scan) and inserts the new
/// member immediately before the object's closing `}`, preserving every other
/// byte (whitespace included).
pub fn ensure_stream_include_usage(
    body: &Bytes,
    content_type: Option<&str>,
    upstream_path: &str,
    is_model_route: bool,
) -> Option<Bytes> {
    // (1) Metered traffic only — mirror / non-model passthrough never injects.
    if !is_model_route {
        return None;
    }
    // (2) JSON with a non-empty body.
    if body.is_empty() || !is_json(content_type) {
        return None;
    }
    // (3) OpenAI completions shape only (see #2524's chat/completions scoping).
    if !(upstream_path.ends_with("/chat/completions") || upstream_path.ends_with("/completions")) {
        return None;
    }
    // (4)+(5) One bounded top-level scan: `stream == literal true`, no
    // `stream_options` key, and the closing-`}` splice point. A body that is
    // not a well-formed JSON object scans `Err(())` → no injection.
    let (stream_true, has_stream_options, closing_brace) =
        scan_top_level_stream(body.as_ref()).ok()?;
    if !stream_true || has_stream_options {
        return None;
    }
    // Splice before the closing `}`. The object is non-empty here (a top-level
    // `stream` member exists), so the leading comma is always correct; all
    // whitespace around `}` is preserved byte-for-byte.
    let mut out = Vec::with_capacity(body.len() + STREAM_OPTIONS_INJECTION.len());
    out.extend_from_slice(&body[..closing_brace]);
    out.extend_from_slice(STREAM_OPTIONS_INJECTION);
    out.extend_from_slice(&body[closing_brace..]);
    Some(Bytes::from(out))
}

/// The member spliced before the top-level object's closing `}` (AM-2).
const STREAM_OPTIONS_INJECTION: &[u8] = b",\"stream_options\":{\"include_usage\":true}";

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

/// The one top-level member-loop state machine behind every scan in this
/// module (ORA3-M11): ws / comma / string-key / colon / skip-value / close —
/// the exact validate-and-advance semantics `serde_json::from_slice` applies
/// to a JSON object, driven per member through a `visit` callback.
///
/// For each top-level member the **decoded** key (`String`, escapes resolved —
/// one allocation per top-level key, the historic cost) and the byte offset of
/// its value token (post-colon whitespace skipped) are passed to `visit`. The
/// visitor returns:
/// - `Ok(Some(end))` — it fully validated the value token itself, which ends
///   at `end` (lets a target string be decoded once instead of parsed and then
///   skipped again);
/// - `Ok(None)` — the iterator must validate-and-advance the value with
///   [`skip_json_value`] (the no-alloc skip path — bad escapes / numbers /
///   nesting anywhere still reject the document);
/// - `Err(())` — abort the scan as malformed.
///
/// Returns the byte offset of the object's closing `}` (the AM-2 splice
/// point). `Err(())` when `body` is not a well-formed JSON object whose
/// trailing content is whitespace-only (same strictness as
/// `serde_json::from_slice`). Recursion is bounded by [`MAX_JSON_DEPTH`] via
/// [`skip_json_value`]; all indexing is bounds-checked `get()` — never panics.
fn top_level_members(
    body: &[u8],
    mut visit: impl FnMut(&str, usize) -> Result<Option<usize>, ()>,
) -> Result<usize, ()> {
    let mut pos = skip_ws(body, 0);
    if body.get(pos) != Some(&b'{') {
        return Err(());
    }
    pos += 1;
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
                return Ok(pos);
            }
            Some(b',') => {
                let after = skip_ws(body, pos + 1);
                if body.get(after) == Some(&b'}') {
                    return Err(()); // trailing comma (serde rejects)
                }
                pos = after;
            }
            Some(b'"') => {
                let (key, after_key) = parse_json_string(body, pos).ok_or(())?;
                pos = skip_ws(body, after_key);
                if body.get(pos) != Some(&b':') {
                    return Err(());
                }
                pos = skip_ws(body, pos + 1);
                match visit(&key, pos)? {
                    Some(end) => pos = end,
                    None => pos = skip_json_value(body, pos, 0).ok_or(())?,
                }
            }
            _ => return Err(()),
        }
    }
}

/// Scan the **top-level object** of `body` for `key`, skipping every other
/// value without materializing the document.
///
/// Returns `Ok(None)` when the key is absent, `Ok(Some(..))` for the last
/// occurrence (duplicate keys: last wins, like `serde_json::Map`), and `Err(())`
/// when the body is not a well-formed JSON object.
fn scan_top_level_value(body: &[u8], key: &str) -> Result<Option<TopLevelValue>, ()> {
    let mut last: Option<TopLevelValue> = None;
    top_level_members(body, |k, value_start| {
        if k != key {
            return Ok(None);
        }
        if body.get(value_start) == Some(&b'"') {
            // The located value is decoded here (the equality/rewrite callers
            // need the `String`); returning `Some(end)` avoids the iterator
            // re-skipping the same token.
            let (decoded, end) = parse_json_string(body, value_start).ok_or(())?;
            last = Some(TopLevelValue {
                span: (value_start, end),
                decoded: Some(decoded),
            });
            Ok(Some(end))
        } else {
            // Present but not a JSON string — still "present", just not
            // rewritable; the iterator validates and advances the value.
            last = Some(TopLevelValue {
                span: (value_start, value_start),
                decoded: None,
            });
            Ok(None)
        }
    })?;
    Ok(last)
}

/// Scan the **top-level object** of `body` for the AM-2 streaming-metering
/// gate, returning `(stream_true, has_stream_options, closing_brace)`:
/// - `stream_true` — the top-level `stream` field's value is the **literal**
///   `true` (a string, a nested object, a number, or `false` are all `false`;
///   duplicate keys follow serde last-wins like [`scan_top_level_value`]);
/// - `has_stream_options` — a top-level `stream_options` field exists with any
///   value (presence alone is an explicit client control);
/// - `closing_brace` — the byte offset of the object's closing `}` — the
///   splice point: the comma-key member is inserted right before it so every
///   other byte (whitespace included) is preserved.
///
/// `Err(())` when the body is not a well-formed JSON object (same strict
/// validate-and-advance semantics as [`scan_top_level_value`]: bad escapes,
/// malformed numbers, trailing content, etc. all reject) — such bodies are
/// never injected into.
fn scan_top_level_stream(body: &[u8]) -> Result<(bool, bool, usize), ()> {
    let mut stream_true = false;
    let mut has_stream_options = false;
    let closing_brace = top_level_members(body, |k, value_start| {
        if k == "stream" {
            // Only a top-level literal `true` gates; the value token is still
            // fully validated below (validate-and-advance).
            stream_true = body.get(value_start..value_start + 4) == Some(b"true");
        } else if k == "stream_options" {
            // Presence alone counts (any value, incl. `null`/`false`): the
            // client explicitly controls usage reporting.
            has_stream_options = true;
        }
        Ok(None)
    })?;
    Ok((stream_true, has_stream_options, closing_brace))
}

/// The fused single-pass view of a well-formed **top-level JSON object**
/// (ORA3-M14): everything the pipeline needs from the request body in one
/// bounded top-level scan instead of several overlapping ones.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonObjectProfile {
    /// The decoded value + quote-inclusive byte span of the **last** top-level
    /// `model_key` member when it is a JSON string (serde last-wins; span is
    /// the splice target for a model rewrite). `None` for an absent /
    /// non-string (or duplicate-then-non-string) target — not rewritable.
    pub model: Option<(String, (usize, usize))>,
    /// The top-level `stream` value is the literal `true` (AM-2 gate 4).
    pub stream_true: bool,
    /// A top-level `stream_options` member exists with any value (AM-2 gate 5).
    pub has_stream_options: bool,
    /// Byte offset of the object's closing `}` — the AM-2 splice point.
    pub closing_brace: usize,
}

/// Fused single-pass top-level scan (ORA3-M14): gathers the `model_key`
/// value + quote-inclusive span, the AM-2 `stream` / `stream_options` flags,
/// and the closing-`}` offset in ONE validate-and-advance pass.
///
/// `Ok(profile)` only for a well-formed JSON object; `Err(())` otherwise —
/// exactly the verdicts of [`scan_top_level_value`] /
/// [`scan_top_level_stream`] on the same bytes (see the agreement tests).
///
/// Note the verdict is computed on the **original** body: a later model-value
/// splice (byte-identical member content, only the string token changes)
/// cannot flip the `stream` / `stream_options` structure, so a caller that
/// splices after this scan keeps these flags valid.
#[allow(clippy::result_unit_err)] // internal scanner convention: `Err` = structurally invalid body
pub fn scan_top_level_profile(
    body: &[u8],
    model_key: &str,
) -> Result<JsonObjectProfile, ()> {
    let mut model: Option<(String, (usize, usize))> = None;
    let mut stream_true = false;
    let mut has_stream_options = false;
    let closing_brace = top_level_members(body, |k, value_start| {
        if k == model_key {
            if body.get(value_start) == Some(&b'"') {
                let (decoded, end) = parse_json_string(body, value_start).ok_or(())?;
                model = Some((decoded, (value_start, end)));
                Ok(Some(end))
            } else {
                // A non-string member (or a duplicate key whose last value is
                // non-string): last-wins, not a rewritable string.
                model = None;
                Ok(None)
            }
        } else if k == "stream" {
            stream_true = body.get(value_start..value_start + 4) == Some(b"true");
            Ok(None)
        } else if k == "stream_options" {
            has_stream_options = true;
            Ok(None)
        } else {
            Ok(None)
        }
    })?;
    Ok(JsonObjectProfile {
        model,
        stream_true,
        has_stream_options,
        closing_brace,
    })
}

/// Splice the JSON string-value token at quote-inclusive `span` (from a
/// [`JsonObjectProfile`] / [`TopLevelValue`]) with the JSON encoding of
/// `value` — the offset form of [`rewrite_json_model`] for callers that
/// already hold the located token from the fused prepare-time scan
/// (ORA3-M14: no re-scan, no DOM; every other byte is preserved).
pub fn splice_json_string_at(body: &[u8], span: (usize, usize), value: &str) -> Bytes {
    let (start, end) = span;
    let encoded = encode_json_string(value);
    let mut out = Vec::with_capacity(body.len() - (end - start) + encoded.len());
    out.extend_from_slice(&body[..start]);
    out.extend_from_slice(encoded.as_bytes());
    out.extend_from_slice(&body[end..]);
    Bytes::from(out)
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

    // -------------------------------------------------------------------
    // AM-2: `stream_options.include_usage` forced-on injection gate
    // -------------------------------------------------------------------

    /// A canonical streaming OpenAI chat body the client sends WITHOUT
    /// `stream_options` (the mainstream OpenAI-SDK default shape).
    const STREAM_BODY: &str =
        r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}],"stream":true}"#;

    /// Convenience: run the gate with the canonical metered setup (JSON
    /// content-type, `/v1/chat/completions`, model route).
    fn inject(body: &str) -> Option<Bytes> {
        ensure_stream_include_usage(
            &Bytes::copy_from_slice(body.as_bytes()),
            Some(MODEL),
            "/v1/chat/completions",
            true,
        )
    }

    /// The exact splice the injection appends before the closing `}`.
    const INJECT: &str = r#","stream_options":{"include_usage":true}"#;

    #[test]
    fn inject_include_usage_for_stream_chat_byte_exact() {
        let out = inject(STREAM_BODY).expect("a stream:true chat body must be injected");
        let got = String::from_utf8(out.to_vec()).unwrap();
        // Byte-exact: the original body with the member spliced before the
        // top-level closing `}` (trailing `}` closes the object, and the
        // `}` inside `messages` did not confuse the locator).
        let expected = r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}"#;
        assert_eq!(got, expected);
        // Exactly one injection.
        assert_eq!(got.matches("\"stream_options\"").count(), 1, "body: {got}");
        // The result is well-formed and carries the forced option.
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["stream"], json!(true));
        assert_eq!(v["stream_options"]["include_usage"], json!(true));
        assert_eq!(v["messages"][0]["content"], json!("hi"));
    }

    #[test]
    fn inject_preserves_whitespace_byte_for_byte() {
        // Trailing whitespace AFTER the object close stays at the end.
        let body = "{\"stream\":true}\n  \t";
        let out = inject(body).expect("must inject");
        let got = String::from_utf8(out.to_vec()).unwrap();
        assert_eq!(got, format!("{{\"stream\":true{INJECT}}}\n  \t"));

        // Whitespace BETWEEN the last member and the `}` is also kept (the
        // comma lands right before `}`, valid JSON either way).
        let pretty = "{\n  \"model\": \"m\",\n  \"stream\": true\n}\n";
        let out = inject(pretty).expect("must inject");
        let got = String::from_utf8(out.to_vec()).unwrap();
        assert_eq!(
            got,
            "{\n  \"model\": \"m\",\n  \"stream\": true\n,\"stream_options\":{\"include_usage\":true}}\n"
        );
    }

    #[test]
    fn already_explicit_stream_options_is_not_overridden() {
        // The client explicitly controls usage reporting (any value, incl.
        // `false` / `null`) — the gateway never overrides or duplicates it.
        for body in [
            r#"{"stream":true,"stream_options":{"include_usage":true}}"#,
            r#"{"stream":true,"stream_options":{"include_usage":false}}"#,
            r#"{"stream_options":{},"stream":true}"#,
            r#"{"stream":true,"stream_options":null}"#,
        ] {
            assert_eq!(inject(body), None, "must not touch {body}");
        }
    }

    #[test]
    fn no_injection_unless_stream_is_top_level_literal_true() {
        // Missing / false / string / nested / numeric / empty-object bodies are
        // all untouched; a duplicate `stream` key follows serde last-wins.
        for body in [
            r#"{"model":"m"}"#,
            r#"{"stream":false}"#,
            r#"{"stream":"true"}"#,
            r#"{"stream":true,"stream":false}"#,
            r#"{"stream":{"inner":true}}"#,
            r#"{"stream":[true]}"#,
            r#"{"stream":1}"#,
            "{}",
        ] {
            assert_eq!(inject(body), None, "must not inject into {body}");
        }
    }

    #[test]
    fn path_gate_limits_injection_to_completions_endpoints() {
        // /v1-openai prefix and bare /completions inject too ...
        for path in [
            "/v1/chat/completions",
            "/v1-openai/chat/completions",
            "/v1/completions",
            "/completions",
        ] {
            let out = ensure_stream_include_usage(
                &Bytes::from(r#"{"stream":true}"#),
                Some(MODEL),
                path,
                true,
            );
            assert!(out.is_some(), "must inject on {path}");
            assert_eq!(
                out.unwrap(),
                Bytes::from(format!("{{\"stream\":true{INJECT}}}")),
                "injection bytes on {path}"
            );
        }
        // ... while every other streaming endpoint (embeddings / images /
        // audio / anything that is not a completions shape) never injects.
        for path in [
            "/v1/embeddings",
            "/v1/images/generations",
            "/v1/audio/speech",
            "/v1/responses",
            "/other/chat/completionsX",
            "/v1/chat/completions/extra",
            "/",
            "",
        ] {
            assert_eq!(
                ensure_stream_include_usage(
                    &Bytes::from(r#"{"stream":true}"#),
                    Some(MODEL),
                    path,
                    true
                ),
                None,
                "must not inject on {path:?}"
            );
        }
    }

    #[test]
    fn non_json_content_type_empty_body_and_non_model_route_are_none() {
        let stream_body = Bytes::from(r#"{"stream":true}"#);
        // Non-JSON content types (multipart / SSE / none).
        assert_eq!(
            ensure_stream_include_usage(&stream_body, Some(MP), "/v1/chat/completions", true),
            None
        );
        assert_eq!(
            ensure_stream_include_usage(
                &stream_body,
                Some("text/event-stream"),
                "/v1/chat/completions",
                true
            ),
            None
        );
        assert_eq!(
            ensure_stream_include_usage(&stream_body, None, "/v1/chat/completions", true),
            None
        );
        // Empty body.
        assert_eq!(
            ensure_stream_include_usage(&Bytes::new(), Some(MODEL), "/v1/chat/completions", true),
            None
        );
        // Mirror / non-model passthrough: even a perfect stream body is untouched.
        assert_eq!(
            ensure_stream_include_usage(&stream_body, Some(MODEL), "/v1/chat/completions", false),
            None
        );
    }

    #[test]
    fn malformed_or_non_object_bodies_never_inject() {
        for body in [
            "{broken",
            r#"{"stream":true"#,          // unterminated object
            r#"{"stream":true} garbage"#, // trailing content
            r#"{"stream":true,}"#,        // trailing comma
            r#"{"stream":"\q"}"#,         // bad escape in a skipped string
            r#"{"bad":01,"stream":true}"#, // bad number
            r#"{"stream" true}"#,         // missing colon
            r#"{"stream":true}{"a":1}"#,  // trailing object
            r#"[{"stream":true}]"#,       // top-level array (not an object)
            "null",
            "42",
        ] {
            assert_eq!(inject(body), None, "must never inject into {body}");
        }
    }

    #[test]
    fn braces_and_escaped_quotes_inside_strings_do_not_fool_the_locator() {
        // The messages content contains a brace-object, a stray `}`, and
        // escaped quotes — the scanner must only honor the TOP-LEVEL closing
        // `}` and splice there, byte-faithfully.
        let body = r#"{"model":"m","messages":[{"role":"user","content":"say {\"a\":1} then } he said \"hi\""}],"stream":true}"#;
        let out = inject(body).expect("must inject");
        let got = String::from_utf8(out.to_vec()).unwrap();
        // body[..] minus its final `}` + the injection + the `}` it closed.
        let expected = format!("{}{INJECT}}}", &body[..body.len() - 1]);
        assert_eq!(got, expected);
        assert_eq!(got.matches("\"stream_options\"").count(), 1, "body: {got}");
        // Round-trips with the content (incl. its braces/quotes) intact.
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["stream_options"]["include_usage"], json!(true));
        assert_eq!(v["messages"][0]["content"], json!(r#"say {"a":1} then } he said "hi""#));
    }

    #[test]
    fn escaped_stream_key_still_counts_as_the_stream_field() {
        // Keys are decoded like serde (escapes included): `"\u0073tream"` IS
        // the `stream` key — parity with the model-key scanner.
        let body = r#"{"\u0073tream":true}"#;
        let out = inject(body).expect("escaped key must still gate");
        assert_eq!(
            String::from_utf8(out.to_vec()).unwrap(),
            format!(r#"{{"\u0073tream":true{INJECT}}}"#)
        );
    }

    // -------------------------------------------------------------------
    // ORA3-M11/M14: the fused profile scan agrees with the two scanners it
    // consolidates, and its model span drives byte-identical rewrites.
    // -------------------------------------------------------------------

    /// Run every scanner over `src` and return the profile view for assertions.
    fn profile_of(src: &str, key: &str) -> JsonObjectProfile {
        scan_top_level_profile(src.as_bytes(), key).expect("fixture must be a valid object")
    }

    #[test]
    fn profile_and_specialized_scanners_agree_on_valid_objects() {
        // The single-pass profile scan must reproduce, on the same fixtures,
        // the exact verdicts of scan_top_level_value (model) and
        // scan_top_level_stream (AM-2 gate + closing brace) — this is the
        // ORA3-M11 "both scanners agree" proof.
        let valid: &[&str] = &[
            "{}",
            r#"{"model":"m"}"#,
            r#"{"model":"org1/llama-3-8b","messages":[{"role":"user","content":"hi"}],"stream":true}"#,
            r#"{"stream":true,"model":"m"}"#,
            r#"{"\u0073tream":true}"#,
            r#"{"stream":true,"stream_options":{"include_usage":false}}"#,
            r#"{"stream_options":{},"stream":true}"#,
            r#"{"stream":true,"stream_options":null}"#,
            r#"{"model":5}"#,
            r#"{"model":"a\"b\\c"}"#,
            "{\"model\":\"模型-🚀\",\"stream\":false}",
            r#"{"model":"dup","model":"dup2"}"#,
            r#"{"model":"dup","model":7}"#,
            r#"{"model":7,"model":"dup"}"#,
            r#"{"stream":true,"stream":false}"#,
            r#"{"stream":false,"stream":true}"#,
            "{\"model\":\"m\"} \n\t ",
            r#"{"a":[1,{"b":{"model":"nested"}}],"model":"top"}"#,
        ];
        for src in valid {
            let b = src.as_bytes();
            let profile = profile_of(src, "model");
            // model value: last-wins decoded string, None for absent/non-string.
            let expected_model = match scan_top_level_value(b, "model").unwrap() {
                Some(v) => v.decoded,
                None => None,
            };
            assert_eq!(
                profile.model.as_ref().map(|(m, _)| m.clone()),
                expected_model,
                "model mismatch for {src}"
            );
            // AM-2 flags + closing brace match scan_top_level_stream exactly.
            let (stream_true, has_stream_options, closing) =
                scan_top_level_stream(b).expect("valid object");
            assert_eq!(profile.stream_true, stream_true, "stream mismatch for {src}");
            assert_eq!(
                profile.has_stream_options, has_stream_options,
                "stream_options mismatch for {src}"
            );
            assert_eq!(profile.closing_brace, closing, "closing brace for {src}");
            // The profile's model span (when present) rewrites byte-identically
            // to the re-scanning rewrite fn.
            if let Some((_, span)) = &profile.model {
                let spliced = splice_json_string_at(b, *span, "X");
                let via_fn =
                    rewrite_json_model(&Bytes::copy_from_slice(b), "model", "X").expect("rewrite");
                assert_eq!(spliced, via_fn, "span splice mismatch for {src}");
                assert!(
                    serde_json::from_slice::<Value>(&spliced).is_ok(),
                    "spliced body must stay valid for {src}"
                );
            }
        }
    }

    #[test]
    fn profile_custom_model_key_and_agreement_on_corpus() {
        // A custom model key is honored by the profile exactly like the
        // specialized scan.
        let body = r#"{"llm":"gpt-4o","stream":true}"#;
        let p = profile_of(body, "llm");
        assert_eq!(p.model.as_ref().map(|(m, _)| m.as_str()), Some("gpt-4o"));
        assert!(p.stream_true && !p.has_stream_options);
        let p2 = profile_of(body, "model");
        assert_eq!(p2.model, None);
    }

    #[test]
    fn profile_and_scanners_reject_malformed_bodies_together() {
        // Both scanners and the profile must Err on exactly the same inputs.
        let invalid: &[&str] = &[
            "{broken",
            "",
            r#"{"model":"x",}"#,
            r#"{"stream":true"#,
            r#"{"stream":true} garbage"#,
            r#"{"stream":true}{"a":1}"#,
            r#"{"bad":"\q","model":"x"}"#,
            r#"{"bad":01,"model":"x"}"#,
            r#"{"stream" true}"#,
            "null",
            "42",
            r#"[{"stream":true}]"#,
        ];
        for src in invalid {
            assert!(
                scan_top_level_profile(src.as_bytes(), "model").is_err(),
                "profile must reject {src:?}"
            );
            assert!(
                scan_top_level_stream(src.as_bytes()).is_err(),
                "stream scan must reject {src:?}"
            );
            assert!(
                scan_top_level_value(src.as_bytes(), "model").is_err(),
                "value scan must reject {src:?}"
            );
        }
    }
}

