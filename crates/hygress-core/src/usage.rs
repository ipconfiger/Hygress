//! Usage metrics wire types + pure per-chunk usage aggregation
//! (native equivalent of the `gpustack-token-usage` plugin accumulator,
//! design §2.1.3 / §7).
//!
//! [`ModelUsageMetrics`] serializes to the **exact 17-field** JSON the plugin
//! POSTs to `POST /v2/usage/gateway-metrics` (plugin-contract-pin.md §2.8 /
//! §5.1): 11 always-present fields + 6 `Option` fields that serialize
//! `omitempty`-absent when `None` (never `null`). The four server-side-only
//! fields (`operation`, `cluster_id`, `provider_name`, `provider_type`) are
//! **NOT** on the wire — the server owns/assigns them — so they are not
//! members of [`ModelUsageMetrics`]; their internal values live in
//! [`FlushFields`] only.
//!
//! [`UsageSnapshot`] is a pure byte→state aggregator: feed it response
//! chunks (SSE or non-streaming JSON), it absorbs `usage` objects
//! (last-wins, matching OpenAI final-chunk / Anthropic cumulative semantics,
//! including OpenAI `prompt_tokens_details.cached_tokens`, Anthropic
//! `cache_read_input_tokens`, and the **upstream** `total_tokens`), and
//! [`UsageSnapshot::flush`] yields a `completed = true` record when a usage
//! object was observed. The flushed `total_token` honors the upstream total
//! when it exceeds `input + output` (the server's
//! `metrics_collector._resolve_usage_tokens` reconciliation).
//!
//! # `data:` handling (TPACKET-safe)
//!
//! SSE `data:` markers are detected only at **line starts** (index 0 or
//! immediately after `\n`) and each data line is counted **exactly once**,
//! only when it is newline-terminated. An incomplete trailing line is
//! buffered and reassembled across [`feed`] calls, so arbitrary packet
//! fragmentation never double-counts. A usage object is absorbed only when it
//! is the **top-level** `"usage"` field of the SSE event's JSON payload.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One `POST /v2/usage/gateway-metrics` payload (wire form).
///
/// Field names are the GPUStack server's snake_case wire names
/// (`input_token`, `output_token`, `total_token`, `input_cached_token`, ...).
/// `started_at` / `completed_at` are Unix **millis**; a missing or `0` value
/// is treated as absent downstream.
///
/// # Wire pin (plugin-contract-pin.md §2.8 / §5.1)
///
/// This type serializes to **exactly** the 17-field `ModelUsageMetrics` JSON
/// the `gpustack-token-usage` plugin emits: 9 always-present scalar fields
/// (`model`, `input_token`, `output_token`, `total_token`, `input_cached_token`,
/// `request_count`, `completed`, `output_chunk_count`, `request_content_bytes`)
/// plus 8 `Option` fields that serialize absent when `None` (never `null`):
/// `started_at`, `completed_at` (the server maps absent/0 → None per pin §2.8)
/// and the 6 true attribution fields `user_id`, `model_id`, `model_route_id`,
/// `provider_id`, `access_key`, `organization_id`.
///
/// The four server-side-only fields — `operation`, `cluster_id`,
/// `provider_name`, `provider_type` — are deliberately **not** members here.
/// The server owns them (it assigns `operation` itself; the plugin's Go
/// `json` tags prove none are sent), so no wire bytes exist for them
/// (contract pin §2.8, §6-resolution). Any needed internal values for those
/// stay in [`FlushFields`] and are dropped at [`UsageSnapshot::flush`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsageMetrics {
    /// The **routed/effective** model name (may be a LoRA route name). This is
    /// the exact string the server reconciles against
    /// (`model.name == metric.model`, or for LoRA
    /// `metric.model == route_name[model_route_id]`), taken verbatim from
    /// [`FlushFields::model`] at [`UsageSnapshot::flush`].
    pub model: String,
    pub input_token: u64,
    pub output_token: u64,
    pub total_token: u64,
    pub input_cached_token: u64,
    pub request_count: u64,
    /// `true` iff the canonical usage chunk was observed before the response
    /// ended. When `false` the server falls back to byte/chunk estimation.
    pub completed: bool,
    pub output_chunk_count: u64,
    pub request_content_bytes: u64,
    /// Unix millis (request entry); `None` = absent on the wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    /// Unix millis (report dispatch); `None` = absent on the wire.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<u64>,
    /// `omitempty`: absent on the wire when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<u64>,
    /// `omitempty`: absent on the wire when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<i64>,
    /// `omitempty`: absent on the wire when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_route_id: Option<i64>,
    /// `omitempty`: absent on the wire when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<i64>,
    /// `omitempty`: absent on the wire when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    /// Tenant id from `X-Organization-Id`; `omitempty` when `None`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<String>,
}

/// Inference operation vocabulary — the **exact** constant set of the GPUStack
/// server `OperationEnum` (authoritative `server/schemas/model_usage.py`).
///
/// Note the server's (intentional) spelling `AUDIO_TRANSCRIPTION =
/// "audit_transcription"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Completion,
    ChatCompletion,
    Embedding,
    Rerank,
    ImageGeneration,
    AudioSpeech,
    /// Server string is `audit_transcription` (not `audio_transcription`).
    AuditTranscription,
}

impl Operation {
    /// The full server vocabulary (7 entries), in declaration order.
    pub const ALL: [Operation; 7] = [
        Operation::Completion,
        Operation::ChatCompletion,
        Operation::Embedding,
        Operation::Rerank,
        Operation::ImageGeneration,
        Operation::AudioSpeech,
        Operation::AuditTranscription,
    ];

    /// The exact wire string, matching the server `OperationEnum` values.
    pub const fn as_str(self) -> &'static str {
        match self {
            Operation::Completion => "completion",
            Operation::ChatCompletion => "chat_completion",
            Operation::Embedding => "embedding",
            Operation::Rerank => "rerank",
            Operation::ImageGeneration => "image_generation",
            Operation::AudioSpeech => "audio_speech",
            Operation::AuditTranscription => "audit_transcription",
        }
    }

    /// Parse a wire string into an [`Operation`]; `None` for unknown values.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|op| op.as_str() == s)
    }
}

/// Provider usage schema family driving field extraction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsageSchema {
    /// OpenAI-compatible (`prompt_tokens` / `completion_tokens` /
    /// `prompt_tokens_details.cached_tokens`, plus the generic
    /// `input_tokens` / `output_tokens` aliases).
    OpenAi,
    /// Anthropic (`input_tokens` / `output_tokens` /
    /// `cache_read_input_tokens`; values are final/cumulative).
    Anthropic,
    /// Unknown provider — accepts the union of both field families.
    Generic,
}

/// Normalized usage fields extracted from one `usage` object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    /// Upstream-reported total (the `total_tokens` / `total_token` field).
    /// Preferred over `input + output` in [`UsageSnapshot::flush`] when it
    /// exceeds the recomputed sum.
    pub total_tokens: Option<u64>,
    /// Prompt-cache hit tokens (subset of input; mirrors OpenAI
    /// `cached_tokens` / Anthropic `cache_read_input_tokens`).
    pub cache_hit_tokens: Option<u64>,
}

/// Extract normalized usage fields from a `usage` JSON **object**.
///
/// Unknown fields are ignored; missing fields are `None`. Returns `None` when
/// the payload is not a JSON object.
fn fields_from(obj: &Value, schema: UsageSchema) -> Option<Usage> {
    let o = obj.as_object()?;
    let u64 = |key: &str| -> Option<u64> { o.get(key).and_then(|x| x.as_u64()) };
    let total = || u64("total_tokens").or_else(|| u64("total_token"));
    Some(match schema {
        UsageSchema::OpenAi => {
            let cached = o
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|x| x.as_u64())
                .or_else(|| u64("cached_tokens"));
            Usage {
                input_tokens: u64("prompt_tokens").or_else(|| u64("input_tokens")),
                output_tokens: u64("completion_tokens").or_else(|| u64("output_tokens")),
                total_tokens: total(),
                cache_hit_tokens: cached,
            }
        }
        UsageSchema::Anthropic => Usage {
            input_tokens: u64("input_tokens"),
            output_tokens: u64("output_tokens"),
            total_tokens: total(),
            cache_hit_tokens: u64("cache_read_input_tokens"),
        },
        UsageSchema::Generic => Usage {
            input_tokens: u64("prompt_tokens").or_else(|| u64("input_tokens")),
            output_tokens: u64("completion_tokens").or_else(|| u64("output_tokens")),
            total_tokens: total(),
            cache_hit_tokens: u64("cache_read_input_tokens")
                .or_else(|| u64("cached_tokens"))
                .or_else(|| {
                    o.get("prompt_tokens_details")
                        .and_then(|d| d.get("cached_tokens"))
                        .and_then(|x| x.as_u64())
                }),
        },
    })
}

/// Parse one bare `usage` JSON object per `schema`.
///
/// Unknown fields are ignored; missing fields are `None`. Returns `None`
/// when the payload is not a JSON object.
pub fn parse_usage(json: &[u8], schema: UsageSchema) -> Option<Usage> {
    let v: Value = serde_json::from_slice(json).ok()?;
    fields_from(&v, schema)
}

/// Context fields supplied by the caller at flush time (identity, timing,
/// body size). Token fields come from the accumulated snapshot.
///
/// # Internal classification (not forwarded to the wire)
///
/// `model` is the **routed/effective** model name (may be a LoRA route name);
/// [`UsageSnapshot::flush`] copies it verbatim into [`ModelUsageMetrics::model`]
/// — the caller must pass the effective name, not a client-supplied alias.
///
/// `operation`, `cluster_id`, `provider_name`, and `provider_type` are accepted
/// here as **internal classification inputs only** (server-side vocabulary for
/// attribution/log bookkeeping). They are **never** forwarded onto the wire:
/// the GPUStack server assigns `operation` itself and owns `cluster_id`,
/// `provider_name`, `provider_type`, and the token-usage plugin does not emit
/// any of them (contract pin §2.8). [`UsageSnapshot::flush`] therefore drops
/// them when building [`ModelUsageMetrics`]. Keeping them on this *separate*
/// internal type (rather than the wire struct) is what the pin prescribes.
#[derive(Clone, Debug, Default)]
pub struct FlushFields {
    /// The routed/effective model name (may be a LoRA route name); copied
    /// verbatim into [`ModelUsageMetrics::model`] at flush.
    pub model: String,
    pub user_id: Option<u64>,
    pub model_id: Option<i64>,
    pub model_route_id: Option<i64>,
    /// Internal classification only (NOT sent on the wire; see struct docs).
    pub cluster_id: Option<i64>,
    pub provider_id: Option<i64>,
    /// Internal classification only (NOT sent on the wire; see struct docs).
    pub provider_name: Option<String>,
    /// Internal classification only (NOT sent on the wire; see struct docs).
    pub provider_type: Option<String>,
    pub access_key: Option<String>,
    /// Internal classification only (NOT sent on the wire; see struct docs).
    pub operation: Option<String>,
    pub organization_id: Option<String>,
    /// Unix millis at request entry.
    pub started_at_ms: Option<u64>,
    /// Unix millis at report dispatch.
    pub completed_at_ms: Option<u64>,
    pub request_content_bytes: u64,
    /// Override the chunk count computed by the snapshot (e.g. when the
    /// caller counted content bytes itself).
    pub output_chunk_count: Option<u64>,
}

/// Frame of a response we are accumulating.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    /// Not yet classified: keep reassembling fragments and re-check each feed.
    Unknown,
    /// A `data:` event stream (further classification: SSE).
    Sse,
    /// A single non-streaming JSON body (already consumed).
    Json,
}

/// Pure per-response usage accumulator.
///
/// Feed response chunks in order via [`feed`]; when a usage object is seen
/// the fields are adopted **last-wins** (OpenAI's final chunk is
/// authoritative; Anthropic `message_delta` overrides `message_start`).
/// `completed` in the flushed record is `true` iff at least one usage object
/// was observed.
#[derive(Clone, Debug)]
pub struct UsageSnapshot {
    schema: UsageSchema,
    input_token: Option<u64>,
    output_token: Option<u64>,
    total_token: Option<u64>,
    input_cached_token: Option<u64>,
    /// SSE `data:` payloads with non-`[DONE]` content (non-streaming JSON
    /// counts as 1), counted exactly once each.
    output_chunk_count: u64,
    /// Whether any usage object was absorbed.
    seen_any: bool,
    /// Incomplete trailing data carried across chunk boundaries.
    tail: Vec<u8>,
    mode: Mode,
    /// For the non-streaming JSON body: counted exactly once, even when it
    /// arrives across several `feed` calls.
    json_counted: bool,
}

/// Locate the `usage` object within a parsed **payload object** (the
/// top-level JSON of the SSE event / non-streaming body).
///
/// We parse the payload as a proper JSON object (rather than scanning raw
/// bytes for `"usage"`) so a usage nested under an *unrelated* key is ignored.
/// The accepted locations are the documented ones: the top-level `usage`
/// field (OpenAI final chunk, Anthropic `message_delta`, generic) and
/// Anthropic's `message_start` nesting `message.usage`.
fn usage_from_payload(value: &Value) -> Option<&Value> {
    value
        .get("usage")
        .or_else(|| value.get("message").and_then(|m| m.get("usage")))
}

impl UsageSnapshot {
    pub fn new(schema: UsageSchema) -> Self {
        Self {
            schema,
            input_token: None,
            output_token: None,
            total_token: None,
            input_cached_token: None,
            output_chunk_count: 0,
            seen_any: false,
            tail: Vec::new(),
            mode: Mode::Unknown,
            json_counted: false,
        }
    }

    /// Consume one response chunk. Returns `true` when at least one usage
    /// object was absorbed from this chunk.
    ///
    /// Never panics on malformed input; reassembles a `data:` line (or the
    /// whole non-streaming body) split across chunks via the tail buffer.
    pub fn feed(&mut self, chunk: &[u8]) -> bool {
        let mut buf = std::mem::take(&mut self.tail);
        buf.extend_from_slice(chunk);
        if buf.is_empty() {
            return false;
        }

        match self.mode {
            Mode::Unknown => {
                if has_anchored_data(&buf) {
                    self.mode = Mode::Sse;
                    self.process_sse(&buf)
                } else if let Ok(value) = serde_json::from_slice::<Value>(&buf) {
                    if value.is_object() {
                        self.mode = Mode::Json;
                        self.finish_json(&value)
                    } else {
                        // Valid JSON but not an object: not a usage body.
                        self.tail = buf;
                        false
                    }
                } else {
                    // Incomplete (fragmented) prefix: hold for reassembly.
                    self.tail = buf;
                    false
                }
            }
            Mode::Sse => self.process_sse(&buf),
            // The single non-streaming body is already consumed; ignore any
            // trailing bytes.
            Mode::Json => {
                self.tail.clear();
                false
            }
        }
    }

    /// `true` iff a canonical usage chunk was observed (drives `completed`).
    pub fn complete(&self) -> bool {
        self.seen_any
    }

    /// Accumulated tokens so far (all-zero when nothing observed).
    pub fn tokens(&self) -> (u64, u64, u64) {
        (
            self.input_token.unwrap_or(0),
            self.output_token.unwrap_or(0),
            self.input_cached_token.unwrap_or(0),
        )
    }

    pub fn output_chunks(&self) -> u64 {
        self.output_chunk_count
    }

    /// Build the final usage record. `completed` is `true` iff a usage
    /// object was observed. `total_token` prefers the upstream total when it
    /// exceeds `input + output` (server reconciliation), else recomputes.
    ///
    /// `model` is taken verbatim from `f.model` — the caller must supply the
    /// **routed/effective** model name (may be a LoRA route name). The four
    /// server-side-only fields on `FlushFields` (`cluster_id`,
    /// `provider_name`, `provider_type`, `operation`) are intentionally NOT
    /// copied into the returned wire struct: the server owns/assigns them and
    /// the plugin does not send them (contract pin §2.8).
    pub fn flush(&self, f: &FlushFields) -> ModelUsageMetrics {
        let input = self.input_token.unwrap_or(0);
        let output = self.output_token.unwrap_or(0);
        let recomputed = input + output;
        // Prefer the upstream total when it exceeds the recomputed sum
        // (metrics_collector: `total_token > input + output`).
        let total = match self.total_token {
            Some(t) if t > recomputed => t,
            _ => recomputed,
        };
        ModelUsageMetrics {
            model: f.model.clone(),
            input_token: input,
            output_token: output,
            total_token: total,
            input_cached_token: self.input_cached_token.unwrap_or(0),
            request_count: 1,
            completed: self.seen_any,
            output_chunk_count: f.output_chunk_count.unwrap_or(self.output_chunk_count),
            request_content_bytes: f.request_content_bytes,
            started_at: f.started_at_ms,
            completed_at: f.completed_at_ms,
            user_id: f.user_id,
            model_id: f.model_id,
            model_route_id: f.model_route_id,
            provider_id: f.provider_id,
            access_key: f.access_key.clone(),
            organization_id: f.organization_id.clone(),
        }
    }

    /// Process an SSE buffer: count + absorb each **newline-terminated**
    /// anchored `data:` line exactly once; buffer the incomplete trailing
    /// line for the next `feed`.
    fn process_sse(&mut self, buf: &[u8]) -> bool {
        let mut found = false;
        let mut pos = 0;
        while pos < buf.len() {
            let Some(nl) = find_subseq(buf, b"\n", pos) else {
                break;
            };
            let line = &buf[pos..nl];
            if self.process_sse_line(line) {
                found = true;
            }
            pos = nl + 1;
        }
        // Incomplete trailing line (no newline yet) -> reassemble on next feed.
        self.tail = buf[pos..].to_vec();
        found
    }

    /// Handle one full (newline-terminated) SSE line. Only anchored `data:`
    /// lines count, and only once.
    fn process_sse_line(&mut self, line: &[u8]) -> bool {
        // `data:` must be at the line start (index 0 within this line).
        if !line.starts_with(b"data:") {
            return false;
        }
        let payload = trim_ascii(&line[b"data:".len()..]);
        if payload.is_empty() || payload == b"[DONE]" {
            return false;
        }
        // Count this data line exactly once (it is newline-terminated).
        self.output_chunk_count += 1;
        // The usage object must come from the parsed top-level payload JSON.
        if let Ok(value) = serde_json::from_slice::<Value>(payload) {
            if let Some(usage) = usage_from_payload(&value) {
                if self.absorb_value(usage) {
                    return true;
                }
            }
        }
        false
    }

    /// Consume a (complete) non-streaming JSON body: count once, absorb the
    /// top-level `"usage"` field if present.
    fn finish_json(&mut self, value: &Value) -> bool {
        if !self.json_counted {
            self.output_chunk_count += 1;
            self.json_counted = true;
            self.tail.clear();
        }
        match usage_from_payload(value) {
            Some(usage) => self.absorb_value(usage),
            None => false,
        }
    }

    /// Absorb one usage object (last-wins per field, incl. upstream total).
    fn absorb_value(&mut self, value: &Value) -> bool {
        let Some(u) = fields_from(value, self.schema) else {
            return false;
        };
        if let Some(v) = u.input_tokens {
            self.input_token = Some(v);
        }
        if let Some(v) = u.output_tokens {
            self.output_token = Some(v);
        }
        if let Some(v) = u.total_tokens {
            self.total_token = Some(v);
        }
        if let Some(v) = u.cache_hit_tokens {
            self.input_cached_token = Some(v);
        }
        self.seen_any = true;
        true
    }
}

/// `true` when `buf` contains an anchored `data:` marker — at index 0 or
/// immediately after a `\n`. Used to classify a response as SSE.
fn has_anchored_data(buf: &[u8]) -> bool {
    if buf.starts_with(b"data:") {
        return true;
    }
    let mut pos = 0;
    while pos < buf.len() {
        match buf[pos] {
            b'\n' => {
                if buf.len() >= pos + 1 + 5 && &buf[pos + 1..pos + 6] == b"data:" {
                    return true;
                }
                pos += 1;
            }
            _ => pos += 1,
        }
    }
    false
}

fn trim_ascii(b: &[u8]) -> &[u8] {
    let mut start = 0;
    let mut end = b.len();
    while start < end && b[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && b[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &b[start..end]
}

/// Naive byte-subsequence search (no memchr dependency in this crate).
fn find_subseq(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() {
        return (from..=hay.len()).find(|_| true);
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
    use std::collections::BTreeSet;

    // ----- wire format (17-field pin, plugin-contract-pin.md §2.8/§5.1) -----

    /// The exact 17 wire field names (11 required + 6 omitempty).
    const PINNED_WIRE_FIELDS: [&str; 17] = [
        // 11 always-present
        "model",
        "input_token",
        "output_token",
        "total_token",
        "input_cached_token",
        "request_count",
        "completed",
        "output_chunk_count",
        "request_content_bytes",
        "started_at",
        "completed_at",
        // 6 omitempty
        "user_id",
        "model_id",
        "model_route_id",
        "provider_id",
        "access_key",
        "organization_id",
    ];

    /// The 4 server-side-only fields the plugin does NOT send on the wire.
    const SERVER_ONLY_FIELDS: [&str; 4] =
        ["operation", "cluster_id", "provider_name", "provider_type"];

    #[test]
    fn wire_json_emits_exactly_the_17_pinned_fields() {
        // All 17 pinned fields present: 11 required + 6 omitempty (all Some).
        let m = ModelUsageMetrics {
            // Routed/effective model name (a LoRA route name is allowed).
            model: "org1/llama-3-8b".into(),
            input_token: 10,
            output_token: 5,
            total_token: 15,
            input_cached_token: 3,
            request_count: 1,
            completed: true,
            output_chunk_count: 12,
            request_content_bytes: 320,
            started_at: Some(1_700_000_000_000),
            completed_at: Some(1_700_000_003_000),
            user_id: Some(7),
            model_id: Some(42),
            model_route_id: Some(5),
            provider_id: Some(9),
            access_key: Some("key123".into()),
            organization_id: Some("org1".into()),
        };
        let v: Value = serde_json::to_value(&m).unwrap();
        let obj = v.as_object().unwrap();

        // Exactly 17 keys, and the key set equals the 17 pinned names.
        let keys: BTreeSet<&str> = obj.keys().map(|s| s.as_str()).collect();
        let expected: BTreeSet<&str> = PINNED_WIRE_FIELDS.into_iter().collect();
        assert_eq!(
            obj.len(),
            17,
            "wire must be exactly 17 fields, got {}",
            obj.len()
        );
        assert_eq!(keys, expected, "wire field set must equal the 17 pinned names");

        // The 4 server-side-only fields must be ABSENT on the wire.
        for forbidden in SERVER_ONLY_FIELDS {
            assert!(
                !obj.contains_key(forbidden),
                "server-only field `{forbidden}` must not appear on the wire"
            );
        }

        assert_eq!(v["input_token"], json!(10));
        assert_eq!(v["completed"], json!(true));
        // Round-trip (the 4 server-only fields are not part of the type at all).
        let back: ModelUsageMetrics = serde_json::from_value(v).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn none_options_are_absent_not_null() {
        // When every Option is None, only the 9 always-present scalar fields
        // serialize; the 8 Option fields are *absent* (never `null`).
        let m = ModelUsageMetrics {
            model: "m".into(),
            input_token: 0,
            output_token: 0,
            total_token: 0,
            input_cached_token: 0,
            request_count: 1,
            completed: false,
            output_chunk_count: 0,
            request_content_bytes: 0,
            started_at: None,
            completed_at: None,
            user_id: None,
            model_id: None,
            model_route_id: None,
            provider_id: None,
            access_key: None,
            organization_id: None,
        };
        let v: Value = serde_json::to_value(&m).unwrap();
        let obj = v.as_object().unwrap();

        // 9 always-present (non-Option) fields: model, the 6 token/request
        // counters, completed, and request_content_bytes.
        assert_eq!(
            obj.len(),
            9,
            "with all Options None only the 9 scalar fields serialize, got {}",
            obj.len()
        );

        // Every Option field must be absent (not present as `null`).
        for absent in [
            "started_at",
            "completed_at",
            "user_id",
            "model_id",
            "model_route_id",
            "provider_id",
            "access_key",
            "organization_id",
        ] {
            assert!(
                !obj.contains_key(absent),
                "field `{absent}` must be absent (not null) when None"
            );
        }
        // No field may serialize to `null`.
        for (k, val) in obj {
            assert!(!val.is_null(), "field `{k}` must never serialize to null");
        }
    }

    #[test]
    fn flush_drops_server_only_fields_from_wire() {
        // The caller may still supply the internal classification values on
        // FlushFields, but the flushed wire record must not contain them.
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\n");
        let m = s.flush(&FlushFields {
            model: "org1/llama-3-8b-lora".into(),
            user_id: Some(7),
            model_id: Some(42),
            model_route_id: Some(5),
            cluster_id: Some(3),
            provider_id: Some(9),
            provider_name: Some("openai".into()),
            provider_type: Some("openai".into()),
            access_key: Some("key123".into()),
            operation: Some(Operation::ChatCompletion.as_str().to_string()),
            organization_id: Some("org1".into()),
            request_content_bytes: 128,
            started_at_ms: Some(1000),
            completed_at_ms: Some(2000),
            // Leave override as None -> use the snapshot's computed chunk count.
            ..Default::default()
        });

        // Effective model name flows through verbatim.
        assert_eq!(m.model, "org1/llama-3-8b-lora");
        // The 6 omitempty fields the caller supplied are on the wire...
        assert_eq!(m.user_id, Some(7));
        assert_eq!(m.model_route_id, Some(5));
        assert_eq!(m.provider_id, Some(9));
        assert_eq!(m.access_key, Some("key123".into()));
        assert_eq!(m.organization_id, Some("org1".into()));
        // ...and the flushed wire JSON still emits exactly the 17 fields
        // (the 4 server-only are dropped).
        let v: Value = serde_json::to_value(&m).unwrap();
        let keys: BTreeSet<&str> = v.as_object().unwrap().keys().map(|s| s.as_str()).collect();
        let expected: BTreeSet<&str> = PINNED_WIRE_FIELDS.into_iter().collect();
        assert_eq!(keys, expected);
    }

    // ----- Operation vocabulary (finding #6) -----

    #[test]
    fn operation_constants_match_server_enum() {
        assert_eq!(Operation::Completion.as_str(), "completion");
        assert_eq!(Operation::ChatCompletion.as_str(), "chat_completion");
        assert_eq!(Operation::Embedding.as_str(), "embedding");
        assert_eq!(Operation::Rerank.as_str(), "rerank");
        assert_eq!(Operation::ImageGeneration.as_str(), "image_generation");
        assert_eq!(Operation::AudioSpeech.as_str(), "audio_speech");
        // Server spelling (intentional typo in GPUStack): audit_transcription.
        assert_eq!(Operation::AuditTranscription.as_str(), "audit_transcription");
        assert_eq!(Operation::ALL.len(), 7);
    }

    #[test]
    fn operation_parse_roundtrip() {
        for op in Operation::ALL {
            assert_eq!(Operation::parse(op.as_str()), Some(op));
        }
        assert_eq!(Operation::parse("chat_completions"), None);
        assert_eq!(Operation::parse("bogus"), None);
    }

    // ----- parse_usage -----

    #[test]
    fn parse_usage_openai() {
        let u = parse_usage(
            br#"{"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15,
                  "prompt_tokens_details": {"cached_tokens": 3}}"#,
            UsageSchema::OpenAi,
        )
        .unwrap();
        assert_eq!(
            u,
            Usage {
                input_tokens: Some(10),
                output_tokens: Some(5),
                total_tokens: Some(15),
                cache_hit_tokens: Some(3),
            }
        );
    }

    #[test]
    fn parse_usage_openai_alias_fields() {
        let u = parse_usage(
            br#"{"input_tokens": 4, "output_tokens": 2, "cached_tokens": 1}"#,
            UsageSchema::OpenAi,
        )
        .unwrap();
        assert_eq!(u.input_tokens, Some(4));
        assert_eq!(u.output_tokens, Some(2));
        assert_eq!(u.cache_hit_tokens, Some(1));
        assert_eq!(u.total_tokens, None);
    }

    #[test]
    fn parse_usage_anthropic() {
        let u = parse_usage(
            br#"{"input_tokens": 20, "output_tokens": 7, "cache_read_input_tokens": 4}"#,
            UsageSchema::Anthropic,
        )
        .unwrap();
        assert_eq!(
            u,
            Usage {
                input_tokens: Some(20),
                output_tokens: Some(7),
                total_tokens: None,
                cache_hit_tokens: Some(4),
            }
        );
    }

    #[test]
    fn parse_usage_rejects_non_object() {
        assert!(parse_usage(br#"42"#, UsageSchema::OpenAi).is_none());
        assert!(parse_usage(br#"[1]"#, UsageSchema::OpenAi).is_none());
        assert!(parse_usage(b"", UsageSchema::OpenAi).is_none());
    }

    // ----- UsageSnapshot: OpenAI SSE -----

    #[test]
    fn openai_sse_accumulation_and_completed_true() {
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        // Content chunks (no usage).
        assert!(!s.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"H\"}}]}\n\n"));
        assert!(!s.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"i\"}}]}\n\n"));
        // Final chunk with usage (a single SSE data: line, as on the wire).
        assert!(s.feed(
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":40,\"total_tokens\":140,\"prompt_tokens_details\":{\"cached_tokens\":12}}}\n\n"
        ));
        // [DONE] terminates the stream but carries no usage.
        assert!(!s.feed(b"data: [DONE]\n\n"));

        assert!(s.complete());
        let (in_tok, out_tok, cached) = s.tokens();
        assert_eq!((in_tok, out_tok, cached), (100, 40, 12));
        // 3 content chunks ([DONE] excluded).
        assert_eq!(s.output_chunks(), 3);

        let m = s.flush(&FlushFields {
            model: "gpt-4o".into(),
            model_id: Some(1),
            model_route_id: Some(2),
            provider_id: Some(3),
            access_key: Some("ak".into()),
            operation: Some(Operation::ChatCompletion.as_str().to_string()),
            request_content_bytes: 512,
            started_at_ms: Some(1000),
            completed_at_ms: Some(2000),
            ..Default::default()
        });
        assert!(m.completed);
        assert_eq!(m.input_token, 100);
        assert_eq!(m.output_token, 40);
        assert_eq!(m.total_token, 140);
        assert_eq!(m.input_cached_token, 12);
        assert_eq!(m.output_chunk_count, 3);
        assert_eq!(m.request_content_bytes, 512);
        assert_eq!(m.model_route_id, Some(2));
        assert_eq!(m.request_count, 1);
    }

    // ----- Upstream total reconciliation (finding #3) -----

    #[test]
    fn flush_prefers_upstream_total_when_greater() {
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        // Upstream reports a total larger than the broken-out input+output
        // (e.g. reasoning/tool tokens not split out client-side).
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":99}}\n\n");
        let m = s.flush(&FlushFields {
            request_content_bytes: 0,
            ..Default::default()
        });
        assert_eq!(m.input_token, 10);
        assert_eq!(m.output_token, 5);
        assert_eq!(m.total_token, 99); // upstream total preferred (99 > 15)
    }

    #[test]
    fn flush_recomputes_when_upstream_total_absent_or_smaller() {
        // Absent total -> recompute input+output.
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n");
        let m = s.flush(&FlushFields {
            request_content_bytes: 0,
            ..Default::default()
        });
        assert_eq!(m.total_token, 10);

        // Total present but smaller than the sum -> recompute.
        let mut s2 = UsageSnapshot::new(UsageSchema::OpenAi);
        s2.feed(b"data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":5}}\n\n");
        let m2 = s2.flush(&FlushFields {
            request_content_bytes: 0,
            ..Default::default()
        });
        assert_eq!(m2.total_token, 10);
    }

    // ----- Anthropic (last-wins, cache, total) -----

    #[test]
    fn anthropic_sse_last_wins_and_cache_read_tokens() {
        let mut s = UsageSnapshot::new(UsageSchema::Anthropic);
        // message_start carries the final input_tokens and an initial
        // output_tokens; message_delta carries cumulative output_tokens and
        // cache_read_input_tokens. feed() returns true when a usage object
        // is absorbed.
        assert!(s.feed(
            b"event: message_start\ndata: {\"message\":{\"usage\":{\"input_tokens\":20,\"output_tokens\":1}}}\n\n"
        ));
        assert_eq!(s.tokens().0, 20);
        s.feed(
            b"data: {\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":8,\"cache_read_input_tokens\":6}}\n\n",
        );
        let m = s.flush(&FlushFields {
            model: "claude".into(),
            ..Default::default()
        });
        assert!(m.completed);
        assert_eq!(m.input_token, 20);
        assert_eq!(m.output_token, 8); // message_delta overrides message_start's 1
        assert_eq!(m.input_cached_token, 6);
        // No upstream total -> recomputed as input+output.
        assert_eq!(m.total_token, 28);
    }

    #[test]
    fn no_usage_means_completed_false() {
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n");
        s.feed(b"data: [DONE]\n\n");
        assert!(!s.complete());
        let m = s.flush(&FlushFields {
            model: "m".into(),
            request_content_bytes: 99,
            ..Default::default()
        });
        assert!(!m.completed);
        assert_eq!(m.input_token, 0);
        assert_eq!(m.output_chunk_count, 1);
        assert_eq!(m.request_content_bytes, 99);
    }

    #[test]
    fn non_streaming_json_usage() {
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(
            b"{\"id\":\"cmpl-1\",\"choices\":[],\"usage\":
              {\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10}}",
        );
        let m = s.flush(&FlushFields {
            model: "m".into(),
            ..Default::default()
        });
        assert!(m.completed);
        assert_eq!((m.input_token, m.output_token, m.total_token), (7, 3, 10));
        assert_eq!(m.output_chunk_count, 1);
    }

    #[test]
    fn usage_split_across_chunk_boundary() {
        let full =
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":4}}\n\n";
        let (a, b) = full.split_at(25);
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(a);
        s.feed(b);
        let m = s.flush(&FlushFields {
            model: "m".into(),
            ..Default::default()
        });
        assert!(m.completed);
        assert_eq!((m.input_token, m.output_token), (11, 4));
    }

    // ----- SSE fragmentation: count exactly once, never double (finding #4) -----

    #[test]
    fn sse_data_line_fragmented_across_feed_is_counted_once() {
        // A single data line split across two feeds is counted exactly once —
        // only when its newline terminates the line.
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        let line = b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}";
        let half = line.len() / 2;
        s.feed(&line[..half]);
        // Not yet newline-terminated -> not counted.
        assert_eq!(s.output_chunks(), 0);
        let mut rest = line[half..].to_vec();
        rest.push(b'\n');
        s.feed(&rest);
        // Counted exactly once, and the usage was absorbed.
        assert_eq!(s.output_chunks(), 1);
        assert!(s.complete());
        assert_eq!(s.tokens(), (3, 2, 0));
    }

    #[test]
    fn sse_no_double_count_incomplete_trailing_line() {
        // A data line that is incomplete when the chunk ends must not be
        // counted until its newline arrives in a later feed (the classic
        // double-count regression).
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(b"data: {\"a\":1}\ndata: {\"b\":2");
        // Only the first complete line is counted.
        assert_eq!(s.output_chunks(), 1);
        s.feed(b":99}\n");
        // The second line is now complete -> counted once (total 2).
        assert_eq!(s.output_chunks(), 2);
    }

    #[test]
    fn sse_mixed_fragmentation_counts_each_line_once() {
        // Line 1 complete, line 2's start held, then line 2 completed + an
        // empty line: each data line counted exactly once.
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(b"data: {\"x\":1}\nda");
        assert_eq!(s.output_chunks(), 1);
        s.feed(b"ta: {\"y\":2}\n\n");
        assert_eq!(s.output_chunks(), 2);
        // A trailing [DONE] on its own line is not counted.
        s.feed(b"data: [DONE]\n");
        assert_eq!(s.output_chunks(), 2);
    }

    #[test]
    fn malformed_chunks_never_panic() {
        let mut s = UsageSnapshot::new(UsageSchema::Generic);
        s.feed(b"data: {broken json\n\n");
        s.feed(b"not sse at all");
        s.feed(b"data: {\"choices\"}\n\n");
        s.feed(b"");
        assert!(!s.complete());
    }

    #[test]
    fn generic_schema_accepts_both_families() {
        let mut s = UsageSnapshot::new(UsageSchema::Generic);
        s.feed(
            b"data: {\"usage\":{\"input_tokens\":5,\"output_tokens\":2,\"cache_read_input_tokens\":1}}\n\n",
        );
        assert!(s.complete());
        assert_eq!(s.tokens(), (5, 2, 1));
    }

    #[test]
    fn top_level_usage_only_nested_ignored() {
        // A `"usage"` key that is not the top-level field of the payload must
        // not be absorbed.
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(
            b"data: {\"meta\":{\"usage\":{\"prompt_tokens\":50,\"completion_tokens\":40}}}\n\n",
        );
        // The nested usage is not top-level -> no completion, no tokens.
        assert!(!s.complete());
        assert_eq!(s.tokens(), (0, 0, 0));
    }
}
