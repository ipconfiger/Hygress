//! Shared byte / scan utilities (ORA3-M10).
//!
//! One canonical home for the small byte helpers that used to be triplicated
//! across the crates (`body.rs`, `model_mapping.rs`, `usage.rs` each carried a
//! private `find_subseq` / `replace_bytes` / multipart `name="model"`-header
//! matcher) and for the basic-multipart part scanner those files share. All of
//! them are byte-for-byte ports of the historic per-crate copies — same
//! case-sensitivity (exact byte match, no case folding), same boundary /
//! line-ending semantics — so behavior is preserved by construction.
//!
//! Pure: operates on `&[u8]` / `&mut Vec<u8>` only; no I/O, no `bytes` dep.

/// Byte-subsequence search backed by `memchr`'s SIMD `memmem` (P6 — the
/// per-chunk SSE newline / `"usage"` prefilter / multipart-header hot path
/// previously ran a naive byte loop).
///
/// Returns the first index `i >= from` with `hay[i..i + needle.len()] ==
/// needle`, or `None`. An empty needle matches at `from` when `from` is in
/// range (`from <= hay.len()`); this is the union of the historic per-crate
/// behaviors (each caller only ever passes an in-range `from` with a non-empty
/// needle, so the corner rule is unobservable in practice).
pub fn find_subseq(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() {
        return (from <= hay.len()).then_some(from);
    }
    if hay.len() < from + needle.len() {
        return None;
    }
    memchr::memmem::find(&hay[from..], needle).map(|i| i + from)
}

/// Replace `hay[start..end]` with `new` (growing or shrinking the vec in
/// place). `start`/`end` are absolute offsets into `hay` (end exclusive).
pub fn replace_bytes(hay: &mut Vec<u8>, start: usize, end: usize, new: &[u8]) {
    debug_assert!(start <= end, "replace_bytes: start > end");
    let tail: Vec<u8> = hay[end..].to_vec();
    hay.truncate(start);
    hay.extend_from_slice(new);
    hay.extend_from_slice(&tail);
}

/// `true` when a multipart part header block carries `name="<field>"`.
///
/// The match is the **exact byte form** the GPUStack model forms use
/// (`name="model"`); kept case-sensitive byte-for-byte from the historic
/// per-crate scanners (no case folding — do not "fix" without auditing both
/// multipart scanners in `hygress-gateway::body` and `model_mapping`).
pub fn contains_form_field(header: &[u8], field: &str) -> bool {
    let needle = format!("name=\"{field}\"");
    find_subseq(header, needle.as_bytes(), 0).is_some()
}

/// Locate the value byte span `(start, end)` (end exclusive) of the **first**
/// multipart part whose header block carries `name="<field>"`.
///
/// This is the merged form of the two historic multipart part scanners
/// (the `model`-part loops in `hygress-gateway::body` and `model_mapping`),
/// extracted verbatim so all three call sites share one boundary /
/// line-ending implementation instead of silently diverging:
/// - parts are delimited by the literal `--<boundary>` marker, and a
///   `--<boundary>--` terminator ends the body;
/// - the part value is everything after the first `\r\n\r\n` header separator
///   up to the next marker, with one trailing `\r\n` stripped (a final part
///   without a trailing newline keeps its bytes);
/// - a part whose value region would be empty / malformed (`start > end`) is
///   skipped and the scan continues with the next part;
/// - `None` when the body holds no `--<boundary>` part (or the terminator
///   comes first), or no part header matches `name="<field>"`.
///
/// Callers splice or read `body[start..end]` themselves; this function never
/// allocates the value.
pub fn first_form_value_span(body: &[u8], boundary: &str, field: &str) -> Option<(usize, usize)> {
    let marker = format!("--{boundary}");
    let marker = marker.as_bytes();
    let mut search_from = 0usize;
    while search_from <= body.len() {
        let start = find_subseq(body, marker, search_from)?;
        let after = start + marker.len();
        // Terminator boundary (`--boundary--`) ends the body.
        if body.get(after..after + 2) == Some(b"--") {
            return None;
        }
        let next = find_subseq(body, marker, after).unwrap_or(body.len());
        let part = &body[after..next];
        if let Some(sep) = find_subseq(part, b"\r\n\r\n", 0) {
            let header = &part[..sep];
            if contains_form_field(header, field) {
                let value_start = after + sep + 4;
                let value_end = if part.ends_with(b"\r\n") {
                    next - 2
                } else {
                    next
                };
                if value_start <= value_end {
                    return Some((value_start, value_end));
                }
            }
        }
        search_from = next;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- find_subseq -----

    #[test]
    fn find_subseq_basic_and_from_offset() {
        assert_eq!(find_subseq(b"ababa", b"aba", 0), Some(0));
        assert_eq!(find_subseq(b"ababa", b"aba", 1), Some(2));
        assert_eq!(find_subseq(b"hello", b"x", 0), None);
        assert_eq!(find_subseq(b"hello", b"ell", 0), Some(1));
        // `from` past the last possible start → None.
        assert_eq!(find_subseq(b"hello", b"ll", 4), None);
        assert_eq!(find_subseq(b"hello", b"o", 5), None);
        // Overlapping needles are fine (naive scan).
        assert_eq!(find_subseq(b"aaaa", b"aa", 0), Some(0));
        assert_eq!(find_subseq(b"aaaa", b"aa", 2), Some(2));
    }

    #[test]
    fn find_subseq_empty_needle() {
        assert_eq!(find_subseq(b"abc", b"", 2), Some(2));
        assert_eq!(find_subseq(b"abc", b"", 3), Some(3));
        // Out-of-range from: no match (union of the historic copies).
        assert_eq!(find_subseq(b"abc", b"", 4), None);
    }

    // ----- replace_bytes -----

    #[test]
    fn replace_bytes_grows_shrinks_and_equal() {
        let mut v = b"abXcde".to_vec();
        replace_bytes(&mut v, 2, 3, b"Y");
        assert_eq!(v, b"abYcde");
        // Growing replacement.
        replace_bytes(&mut v, 2, 3, b"longer");
        assert_eq!(v, b"ablongercde");
        // Shrinking (replace the tail).
        let tail = v.len();
        replace_bytes(&mut v, 2, tail, b"");
        assert_eq!(v, b"ab");
        // Equal length.
        replace_bytes(&mut v, 0, 2, b"ab");
        assert_eq!(v, b"ab");
        // Empty replacement at the end.
        let mut w = b"xy".to_vec();
        replace_bytes(&mut w, 0, 0, b"z");
        assert_eq!(w, b"zxy");
    }

    // ----- contains_form_field / multipart header matching -----

    #[test]
    fn contains_form_field_is_byte_exact() {
        assert!(contains_form_field(b"Content-Disposition: form-data; name=\"model\"", "model"));
        assert!(contains_form_field(
            b"Content-Disposition: form-data; name=\"model\"; filename=\"f\"",
            "model"
        ));
        assert!(!contains_form_field(b"Content-Disposition: form-data; name=\"file\"", "model"));
        assert!(!contains_form_field(b"name=\"MODEL\"", "model")); // case-sensitive (historic)
        assert!(!contains_form_field(b"", "model"));
    }

    // The canonical multipart fixture used by the historic scanners' tests
    // (model part first, then a file part, then the terminator).
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
    fn first_form_value_span_finds_the_model_part_value() {
        let body = mp_body("org-1/llama");
        let (start, end) = first_form_value_span(&body, "B", "model").unwrap();
        // The span covers exactly the part value, minus its trailing CRLF.
        assert_eq!(&body[start..end], b"org-1/llama");
        assert!(body[end..].starts_with(b"\r\n--B\r\nContent-Disposition: form-data; name=\"file\""));

        // A different real field ("file" part is present) yields ITS part value,
        // not the model part's.
        let (fstart, fend) = first_form_value_span(&body, "B", "file").unwrap();
        assert_eq!(&body[fstart..fend], b"XYZ");
        assert_eq!(first_form_value_span(&body, "B", "nonexistent"), None);
    }

    #[test]
    fn first_form_value_span_handles_no_model_part_and_terminator() {
        let no_model =
            b"--B\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nXYZ\r\n--B--\r\n";
        assert_eq!(first_form_value_span(no_model, "B", "model"), None);
        // Terminator only (no real part): no span.
        assert_eq!(first_form_value_span(b"--B--\r\n", "B", "model"), None);
        assert_eq!(first_form_value_span(b"", "B", "model"), None);
        // Unknown boundary never matches.
        let body = mp_body("x");
        assert_eq!(first_form_value_span(&body, "ZZ", "model"), None);
    }

    #[test]
    fn first_form_value_span_value_span_equivalence_with_slice_rewrite() {
        // The span is exactly what a byte-splice rewrite needs: replacing
        // body[start..end] with a new value and re-slicing yields the value.
        let body = mp_body("org1/llama-3-8b:adapter");
        let (start, end) = first_form_value_span(&body, "B", "model").unwrap();
        let mut replaced = body.clone();
        replace_bytes(&mut replaced, start, end, b"mapped-name");
        let s = String::from_utf8(replaced).unwrap();
        assert!(s.contains("name=\"model\"\r\n\r\nmapped-name\r\n"));
        assert!(s.contains("XYZ")); // other part value untouched
        assert!(s.ends_with("--B--\r\n"));
    }

    #[test]
    fn first_form_value_span_skips_malformed_part_and_keeps_looking() {
        // A part whose header has no `\r\n\r\n` separator is not a valid part;
        // the scanner moves on to the next marker instead of failing.
        let body = b"--B\r\nname=\"model\"\r\n--B\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nval\r\n--B--\r\n";
        let (start, end) = first_form_value_span(body, "B", "model").unwrap();
        assert_eq!(&body[start..end], b"val");
    }

    // ----- T4 edge pins: needle == haystack / longer needle -----

    #[test]
    fn find_subseq_edge_pins_needle_equals_or_is_longer_than_haystack() {
        // needle == haystack: the only valid window is the whole haystack, so the
        // match is found at index 0 (for an in-range `from`).
        assert_eq!(find_subseq(b"abc", b"abc", 0), Some(0));
        assert_eq!(find_subseq(b"x", b"x", 0), Some(0));
        assert_eq!(find_subseq(b"", b"", 0), Some(0));
        // needle strictly longer than haystack: no window can ever fit -> None,
        // even with `from == 0` (impl: `hay.len() < from + needle.len()` short-
        // circuits before the scan loop).
        assert_eq!(find_subseq(b"abc", b"abcd", 0), None);
        assert_eq!(find_subseq(b"ab", b"abc", 0), None);
        assert_eq!(find_subseq(b"", b"a", 0), None); // empty haystack
        // Empty needle stays as currently documented: `Some(from)` iff
        // `from <= hay.len()` (out-of-range -> None, pinned by
        // find_subseq_empty_needle above).
        assert_eq!(find_subseq(b"abc", b"", 0), Some(0));
        assert_eq!(find_subseq(b"abc", b"", 3), Some(3));
    }

    // ----- T5 multipart locator edge pins -----

    #[test]
    fn first_form_value_span_present_but_empty_model_value_yields_empty_span() {
        // mp_body("") puts the model part FIRST with an empty value: its value
        // line is just the trailing "\r\n" (i.e. header separator directly
        // followed by CRLF, then the next marker).
        let body = mp_body("");
        let (start, end) = first_form_value_span(&body, "B", "model").unwrap();
        // Real behavior: a present-but-empty model part yields a ZERO-LENGTH span
        // (start == end -> `Some("")`). `None` is reserved for an absent part /
        // terminator-first body (see first_form_value_span_handles_...), not for
        // an empty value.
        assert_eq!(start, end);
        assert_eq!(&body[start..end], b"");
        // The zero-length span sits on the empty value line's lone CRLF.
        assert!(body[start..].starts_with(b"\r\n--B\r\nContent-Disposition: form-data; name=\"file\""));
        // Downstream routing consequence: callers that splice `body[start..end]`
        // see `Some("")` — the request routes with an EMPTY model string rather
        // than being treated as having no model part; only the `None` case means
        // "no model".
    }

    #[test]
    fn first_form_value_span_byte_search_hits_name_model_inside_other_param() {
        // Crafted EARLIER part: its *filename* parameter value contains the
        // literal bytes `name="model"` (here inside `filename="name="model".txt"`)
        // while its real field is name="file". The locator is a documented byte
        // search (contains_form_field -> find_subseq), NOT a header tokenizer:
        // no Content-Disposition parameter grammar is parsed, so this earlier
        // part's header is reported as carrying the model field and the genuine
        // name="model" part below it is never reached.
        let body = b"--B\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"name=\"model\".txt\"\r\n\
             \r\n\
             EARLY\r\n\
             --B\r\n\
             Content-Disposition: form-data; name=\"model\"\r\n\
             \r\n\
             REAL\r\n\
             --B--\r\n";
        let (start, end) = first_form_value_span(body, "B", "model").unwrap();
        // Real result: the FIRST header whose bytes contain `name="model"` wins —
        // the earlier part's value, not the later genuine model part's.
        assert_eq!(&body[start..end], b"EARLY");
    }

    #[test]
    fn first_form_value_span_quote_escaped_filename_is_not_a_false_positive() {
        // Control for the byte-search caveat above: when the same idea is carried
        // with RFC-style backslash-escaped quotes (wire bytes contain
        // `name=\"model\"`), the contiguous literal bytes `name="model"` are NOT
        // present, so the matcher does not fire on the earlier part and the
        // genuine model part wins. The matcher never interprets quoting/escaping.
        let body = b"--B\r\n\
             Content-Disposition: form-data; name=\"file\"; filename=\"name=\\\"model\\\".txt\"\r\n\
             \r\n\
             EARLY\r\n\
             --B\r\n\
             Content-Disposition: form-data; name=\"model\"\r\n\
             \r\n\
             REAL\r\n\
             --B--\r\n";
        let (start, end) = first_form_value_span(body, "B", "model").unwrap();
        assert_eq!(&body[start..end], b"REAL");
    }
}
