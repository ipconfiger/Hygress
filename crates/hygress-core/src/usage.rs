//! Usage metrics wire types + pure per-chunk usage aggregation
//! (native equivalent of the `gpustack-token-usage` plugin accumulator,
//! design §2.1.3 / §7).
//!
//! [`ModelUsageMetrics`] serializes to the **exact 17-field** JSON the plugin
//! POSTs to `POST /v2/usage/gateway-metrics` (plugin-contract-pin.md §2.8 /
//! §5.1): 9 always-present scalar fields + 8 `Option` fields that serialize
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
//! buffered and reassembled across [`UsageSnapshot::feed`] calls, so arbitrary packet
//! fragmentation never double-counts. A usage object is absorbed only when it
//! is the **top-level** `"usage"` field of the SSE event's JSON payload.
//!
//! # Boundedness (MINOR-6 / F3)
//!
//! The accumulation is bounded so a hostile / pathological response cannot
//! make the snapshot grow without limit or re-parse its whole buffer on every
//! chunk:
//! - while a response is still **unclassified** (no SSE `data:` seen, no
//!   complete JSON object yet) the buffered tail is capped at
//!   `MAX_TAIL_BYTES`; past it the response is marked `Mode::Oversized`
//!   and the rest is ignored (flush still yields `completed = false`, the
//!   server's byte/chunk-estimation fallback);
//! - full-JSON parse attempts in that state are gated on a possible closing
//!   `}` and limited to `MAX_JSON_PARSE_ATTEMPTS`;
//! - in the SSE state a **single data line** may only grow the persistent tail
//!   up to `MAX_TAIL_BYTES`; an unterminated line beyond that is dropped and
//!   skipped to its `\n` instead of being buffered forever.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bytes::find_subseq;

/// MINOR-6 / F3: byte cap on the buffered-but-unclassified tail and on a single unterminated
/// SSE `data:` line (1 MiB). A realistic usage-bearing frame is far smaller; larger unclassified
/// bodies are dropped to the server's `completed = false` estimation fallback.
const MAX_TAIL_BYTES: usize = 1024 * 1024;
/// Budget of full-DOM JSON parse attempts while the response is still unclassified. Each attempt
/// costs O(tail), so the budget bounds the worst-case re-parse cost of a hostile fragmented body;
/// any real response classifies in a handful of attempts.
const MAX_JSON_PARSE_ATTEMPTS: u32 = 128;

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
/// `started_at`, `completed_at` (always stamped by a real gateway flush — the
/// server maps absent/0 → None per pin §2.8) and the 6 attribution fields
/// `user_id`, `model_id`, `model_route_id`, `provider_id`, `access_key`,
/// `organization_id`. A real flush therefore carries 11 present fields plus the
/// attribution subset — the "11 present + 6 omitempty" shorthand used in the
/// tests below refers to that practical shape (G5-unified wording).
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
    /// Input (prompt) token count.
    pub input_token: u64,
    /// Output (completion) token count.
    pub output_token: u64,
    /// Total token count (upstream-reported total or recomputed input+output).
    pub total_token: u64,
    /// Prompt-cache hit tokens (subset of the input tokens).
    pub input_cached_token: u64,
    /// Number of requests aggregated into this record (1 per flush).
    pub request_count: u64,
    /// `true` iff the canonical usage chunk was observed before the response
    /// ended. When `false` the server falls back to byte/chunk estimation.
    pub completed: bool,
    /// Number of output chunks counted (SSE `data:` payloads with content; a
    /// non-streaming JSON body counts as 1).
    pub output_chunk_count: u64,
    /// Request content size in bytes.
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
    /// Text completion (wire value `completion`).
    Completion,
    /// Chat completion (wire value `chat_completion`).
    ChatCompletion,
    /// Embeddings (wire value `embedding`).
    Embedding,
    /// Rerank (wire value `rerank`).
    Rerank,
    /// Image generation (wire value `image_generation`).
    ImageGeneration,
    /// Audio speech synthesis (wire value `audio_speech`).
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
        Self::ALL.iter().copied().find(|op| op.as_str() == s)
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
    /// Input (prompt) tokens, when reported by the upstream.
    pub input_tokens: Option<u64>,
    /// Output (completion) tokens, when reported by the upstream.
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
    /// User id; `None` = absent.
    pub user_id: Option<u64>,
    /// Model id; `None` = absent.
    pub model_id: Option<i64>,
    /// Model route id; `None` = absent.
    pub model_route_id: Option<i64>,
    /// Internal classification only (NOT sent on the wire; see struct docs).
    pub cluster_id: Option<i64>,
    /// Provider id; `None` = absent.
    pub provider_id: Option<i64>,
    /// Internal classification only (NOT sent on the wire; see struct docs).
    pub provider_name: Option<String>,
    /// Internal classification only (NOT sent on the wire; see struct docs).
    pub provider_type: Option<String>,
    /// Access key used for the request; `None` = absent.
    pub access_key: Option<String>,
    /// Internal classification only (NOT sent on the wire; see struct docs).
    pub operation: Option<String>,
    /// Tenant organization id (from `X-Organization-Id`); `None` = absent.
    pub organization_id: Option<String>,
    /// Unix millis at request entry.
    pub started_at_ms: Option<u64>,
    /// Unix millis at report dispatch.
    pub completed_at_ms: Option<u64>,
    /// Request content size in bytes.
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
    /// The buffered tail exceeded `MAX_TAIL_BYTES` while still unclassified — this is not a
    /// usage-bearing body. The rest of the response is ignored (boundedness, MINOR-6/F3); flush
    /// still works and reports `completed = false`.
    Oversized,
}

/// Pure per-response usage accumulator.
///
/// Feed response chunks in order via [`UsageSnapshot::feed`]; when a usage object is seen
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
    /// Tail length already scanned for an anchored `data:` marker while `mode == Unknown`
    /// (incremental probe so a fragmented response is not re-scanned from index 0 per feed).
    data_probe: usize,
    /// Full-JSON parse attempts made while still `Unknown` (bounded by `MAX_JSON_PARSE_ATTEMPTS`).
    json_parse_attempts: u32,
    /// `true` while an oversized SSE line (past `MAX_TAIL_BYTES`, unterminated) is being
    /// skipped up to its `\n` (single-line unbounded-buffer guard, MINOR-6/F3).
    skip_until_newline: bool,
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
    /// A fresh accumulator that parses response chunks according to `schema`.
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
            data_probe: 0,
            json_parse_attempts: 0,
            skip_until_newline: false,
        }
    }

    /// Consume one response chunk. Returns `true` when at least one usage
    /// object was absorbed from this chunk.
    ///
    /// Never panics on malformed input. In the SSE steady state the complete
    /// lines fully inside `chunk` are processed **in place** (zero-copy); only
    /// a cross-buffer line (a partial tail prefix + the chunk prefix up to the
    /// first newline) is spliced once into the persistent tail, and only the
    /// incomplete trailing sliver is buffered — per-chunk payload copies drop
    /// from ~chunk.len() to ~partial-tail (B2). The first (mode-classifying)
    /// chunk still copies once.
    pub fn feed(&mut self, chunk: &[u8]) -> bool {
        if chunk.is_empty() {
            return false;
        }
        match self.mode {
            Mode::Json | Mode::Oversized => {
                // Already consumed (Json) or deliberately ignored (Oversized — the tail exceeded
                // MAX_TAIL_BYTES before classification): drop any trailing bytes and ignore the
                // rest of the response.
                self.tail.clear();
                false
            }
            Mode::Unknown => {
                // Mode classification needs the accumulated bytes, so this
                // (transition) chunk is appended to the tail and processed
                // there — a one-time full-chunk copy.
                self.tail.extend_from_slice(chunk);
                if probe_anchored_data(&self.tail, &mut self.data_probe) {
                    self.mode = Mode::Sse;
                    let (consumed, found) = self.consume_sse();
                    if consumed > 0 {
                        self.discard_prefix(consumed);
                    }
                    found
                } else if self.tail.len() > MAX_TAIL_BYTES {
                    // MINOR-6/F3: still unclassified after a full MiB — neither an SSE stream nor
                    // a parseable JSON object. Stop buffering/re-parsing it (a hostile
                    // unclassifiable response must not grow the buffer without bound or keep the
                    // snapshot re-parsing the whole tail per feed). Flush still works and reports
                    // `completed = false` (the server's byte-estimation fallback).
                    self.tail.clear();
                    self.mode = Mode::Oversized;
                    false
                } else if json_may_have_completed(&self.tail)
                    && self.json_parse_attempts < MAX_JSON_PARSE_ATTEMPTS
                {
                    // A JSON object can only be parseable once its final `}` has arrived — skip
                    // the O(tail) full-DOM parse on fragments that cannot be complete. The attempt
                    // budget bounds the worst-case cost of a hostile `}`-per-feed stream.
                    self.json_parse_attempts += 1;
                    match serde_json::from_slice::<Value>(&self.tail) {
                        Ok(value) if value.is_object() => {
                            self.mode = Mode::Json;
                            let absorbed = self.finish_json(&value);
                            self.tail.clear();
                            absorbed
                        }
                        // Valid JSON that is not an object (not a usage body) or an incomplete
                        // prefix: keep holding for reassembly.
                        Ok(_) | Err(_) => false,
                    }
                } else {
                    // Incomplete (fragmented) prefix: hold for reassembly.
                    false
                }
            }
            Mode::Sse => self.consume_sse_slice(chunk),
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

    /// Number of output chunks counted so far.
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
        // R-3: saturating — a hostile/malformed upstream value (e.g. u64::MAX)
        // must not overflow/panic on the data plane.
        let recomputed = input.saturating_add(output);
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

    /// Process the buffered tail's SSE lines: count + absorb each
    /// **newline-terminated** anchored `data:` line exactly once; return the
    /// number of bytes consumed (through the last newline — the incomplete
    /// trailing line stays in the persistent buffer) and whether any usage
    /// object was absorbed.
    ///
    /// The buffer is detached for the duration so the line slices never alias
    /// the mutable borrow of `self`; its allocation (capacity) survives the
    /// move and is carried back, so no per-chunk reallocation happens.
    fn consume_sse(&mut self) -> (usize, bool) {
        let buf = std::mem::take(&mut self.tail);
        let mut found = false;
        let mut pos = 0;
        while pos < buf.len() {
            let Some(nl) = find_subseq(&buf, b"\n", pos) else {
                break;
            };
            let line = &buf[pos..nl];
            if self.process_sse_line(line) {
                found = true;
            }
            pos = nl + 1;
        }
        self.tail = buf;
        (pos, found)
    }

    /// Process the SSE `chunk` **in place** (B2): complete lines fully inside
    /// the chunk are each judged from the chunk slice itself (no copy), and at
    /// most ONE cross-buffer line — the pending partial tail + the chunk prefix
    /// up to the first newline — is spliced into the persistent tail and judged
    /// there. The `"usage"` prefilter runs on the **reassembled** cross-buffer
    /// line, so a `"usage"` token split across chunk boundaries is still seen.
    /// The incomplete trailing sliver of the chunk becomes the new tail.
    ///
    /// A single SSE data line may only grow the persistent tail up to
    /// `MAX_TAIL_BYTES` (MINOR-6/F3): an unterminated line past the cap is
    /// dropped (it can never be counted/parsed as a whole) and the stream
    /// skips to the line's `\n` before resuming — an adversarial never-ending
    /// line cannot grow the buffer without bound.
    /// Returns `true` when at least one usage object was absorbed.
    fn consume_sse_slice(&mut self, chunk: &[u8]) -> bool {
        let mut found = false;
        let mut start = 0usize;

        // An oversized unterminated line was dropped in an earlier feed: discard the rest of that
        // line (everything up to its first `\n`), then resume normal line processing.
        if self.skip_until_newline {
            match find_subseq(chunk, b"\n", 0) {
                Some(nl) => {
                    self.skip_until_newline = false;
                    start = nl + 1;
                }
                None => return found, // the whole chunk is still inside the dropped line
            }
        }

        // If a partial (newline-less) tail line is pending, the first line of this chunk completes
        // it — one bounded splice into the persistent tail.
        if !self.tail.is_empty() {
            match find_subseq(chunk, b"\n", start) {
                Some(nl) if self.tail.len().saturating_add(nl - start) > MAX_TAIL_BYTES => {
                    // The reassembled line would exceed the cap: drop it (single-line guard) and
                    // continue right after its `\n` — it is never counted.
                    self.tail.clear();
                    start = nl + 1;
                }
                Some(nl) => {
                    // Detach the partial tail so the joined line never aliases
                    // the mutable borrow of `self`; the buffer allocation is
                    // carried back (capacity reused for future splices).
                    let mut joined = std::mem::take(&mut self.tail);
                    joined.extend_from_slice(&chunk[start..nl]);
                    if self.process_sse_line(&joined) {
                        found = true;
                    }
                    joined.clear();
                    self.tail = joined;
                    start = nl + 1;
                }
                None => {
                    // The whole remainder continues the pending line without a terminator.
                    let sliver = &chunk[start..];
                    if self.tail.len().saturating_add(sliver.len()) > MAX_TAIL_BYTES {
                        // MINOR-6/F3: past the cap the line can never be counted/parsed whole —
                        // drop it and skip to its `\n` instead of buffering without bound.
                        self.tail.clear();
                        self.skip_until_newline = true;
                        return found;
                    }
                    self.tail.extend_from_slice(sliver);
                    return found;
                }
            }
        }

        // Judge complete newline-terminated lines fully inside the chunk, in
        // place (the line slice borrows `chunk`, never `self` — no aliasing).
        let mut pos = start;
        while pos < chunk.len() {
            let Some(nl) = find_subseq(chunk, b"\n", pos) else {
                break;
            };
            let line = &chunk[pos..nl];
            if self.process_sse_line(line) {
                found = true;
            }
            pos = nl + 1;
        }

        // The incomplete trailing sliver becomes the new partial tail (a
        // bounded copy, never the whole chunk; the single-line cap applies).
        if pos < chunk.len() {
            let sliver = &chunk[pos..];
            if self.tail.len().saturating_add(sliver.len()) > MAX_TAIL_BYTES {
                // The line this sliver belongs to is (now) oversized: drop + skip to its `\n`.
                self.tail.clear();
                self.skip_until_newline = true;
            } else {
                self.tail.extend_from_slice(sliver);
            }
        } else {
            self.tail.clear();
        }
        found
    }

    /// Drop the consumed prefix, keeping the incomplete trailing line in the
    /// persistent buffer (no per-chunk reallocation — the capacity is reused).
    fn discard_prefix(&mut self, consumed: usize) {
        if consumed >= self.tail.len() {
            self.tail.clear();
        } else {
            self.tail.copy_within(consumed.., 0);
            self.tail.truncate(self.tail.len() - consumed);
        }
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
        // M5 prefilter: a usage object requires the literal `"usage"` JSON key
        // somewhere in the payload (OpenAI `usage` or Anthropic
        // `message.usage` — both contain the byte token `"usage"`). Skip the
        // full `serde_json::Value` DOM parse for the ~all data lines that carry
        // content only.
        if find_subseq(payload, b"\"usage\"", 0).is_none() {
            return false;
        }
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

/// Incrementally detect an anchored `data:` marker in `buf` (at index 0 or immediately after a
/// `\n`), continuing a previous probe whose frontier is `*probe` (the length of `buf` at the last
/// scan). A marker can only become visible where its bytes were previously incomplete — within the
/// last few bytes of the old buffer or in the new ones — so each scan is O(new bytes + const)
/// rather than O(whole tail): a fragmented hostile response cannot turn classification into O(n²).
/// On return `*probe` is advanced to `buf.len()`.
fn probe_anchored_data(buf: &[u8], probe: &mut usize) -> bool {
    let len = buf.len();
    // A marker at index 0 is anchored without a `\n`; it was only checkable once 5 bytes existed.
    if *probe < 5 && len >= 5 && buf.starts_with(b"data:") {
        *probe = len;
        return true;
    }
    // A `\n`-anchored marker at p needs `\n` at p-1 and bytes p..p+5 present. A window that was
    // incomplete at the last scan had its `\n` within the final 6 bytes of the old buffer, so
    // rescanning from `probe - 6` covers every possibly-new marker (and re-checks a few old ones).
    let Some(last) = len.checked_sub(6) else {
        *probe = len;
        return false;
    };
    let from = probe.saturating_sub(6);
    if from > last {
        *probe = len;
        return false;
    }
    let mut i = from;
    while i <= last {
        if buf[i] == b'\n' && &buf[i + 1..i + 6] == b"data:" {
            *probe = len;
            return true;
        }
        i += 1;
    }
    *probe = len;
    false
}

/// `true` when the trailing non-whitespace byte of `buf` is `}` — the only point at which a JSON
/// object *could* be complete (the gate that keeps the Unknown state from running an O(tail)
/// full-DOM parse on fragments that cannot possibly parse yet).
fn json_may_have_completed(buf: &[u8]) -> bool {
    match buf.iter().rev().find(|b| !b.is_ascii_whitespace()) {
        Some(b) => *b == b'}',
        None => false,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeSet;

    // ----- wire format (17-field pin, plugin-contract-pin.md §2.8/§5.1) -----

    /// The exact 17 wire field names — 9 non-`Option` scalars + 8 `Option`
    /// fields; a real flush always stamps `started_at`/`completed_at`, hence
    /// the practical "11 present + up to 6 attribution" wire shape (G5-unified
    /// wording with the struct docs above).
    const PINNED_WIRE_FIELDS: [&str; 17] = [
        // 9 always-present scalar members
        "model",
        "input_token",
        "output_token",
        "total_token",
        "input_cached_token",
        "request_count",
        "completed",
        "output_chunk_count",
        "request_content_bytes",
        // started_at / completed_at — Option, always stamped by a real flush
        "started_at",
        "completed_at",
        // 6 attribution fields (omitempty)
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
        assert_eq!(
            keys, expected,
            "wire field set must equal the 17 pinned names"
        );

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
        assert_eq!(
            Operation::AuditTranscription.as_str(),
            "audit_transcription"
        );
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

    // ----- ORA3-M9: mid-stream flush (the write-fail terminal contract) -----

    #[test]
    fn flush_after_mid_stream_disconnect_carries_observed_tokens() {
        // The gateway's mid-stream write-fail terminal (ORA3-M9) flushes the
        // LIVE accumulator retained when the downstream died mid-stream — NOT a
        // fresh empty snapshot. The response here ends abruptly right after the
        // usage chunk (no [DONE], no clean end-of-body), exactly like a
        // downstream disconnect: the flushed incomplete row must still carry
        // the tokens absorbed before the break and `completed` must reflect
        // that a usage object was observed.
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        // Content chunk, then the canonical usage chunk — then the stream dies.
        assert!(!s.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"H\"}}]}\n\n"));
        assert!(s.feed(
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":40,\"total_tokens\":140,\"prompt_tokens_details\":{\"cached_tokens\":12}}}\n\n"
        ));
        // Mid-stream: the usage object was observed, but the response never
        // reached a natural end — no [DONE] was fed.
        assert!(s.complete());
        let (in_tok, out_tok, cached) = s.tokens();
        assert_eq!((in_tok, out_tok, cached), (100, 40, 12));

        let m = s.flush(&FlushFields {
            model: "gpt-4o".into(),
            model_id: Some(1),
            model_route_id: Some(2),
            provider_id: Some(3),
            request_content_bytes: 512,
            started_at_ms: Some(1000),
            completed_at_ms: Some(2000),
            ..Default::default()
        });
        assert!(
            m.completed,
            "usage was observed mid-stream; the incomplete row must report completed=true, not empty"
        );
        assert_eq!(m.input_token, 100);
        assert_eq!(m.output_token, 40);
        assert_eq!(m.input_cached_token, 12);
        assert_eq!(m.total_token, 140);
        // Attribution flows through on the incomplete row exactly as on the
        // normal end-of-stream flush.
        assert_eq!(
            (m.model_id, m.model_route_id, m.provider_id),
            (Some(1), Some(2), Some(3))
        );
    }

    #[test]
    fn flush_after_mid_stream_disconnect_without_usage_stays_empty() {
        // No usage object was observed before the break: the incomplete row
        // must stay the historical `completed=false` zero-token row (the
        // GPUStack server falls back to byte/chunk estimation).
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        assert!(!s.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"H\"}}]}\n\n"));
        assert!(!s.feed(b"data: {\"choices\":[{\"delta\":{\"content\":\"i\"}}]}\n\n"));
        // Mid-stream: only content chunks were fed, then the stream died.
        assert!(!s.complete());
        let m = s.flush(&FlushFields {
            model: "gpt-4o".into(),
            request_content_bytes: 512,
            ..Default::default()
        });
        assert!(!m.completed);
        assert_eq!(
            (
                m.input_token,
                m.output_token,
                m.input_cached_token,
                m.total_token
            ),
            (0, 0, 0, 0)
        );
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
        let line =
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}";
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
        s.feed(b"data: {\"meta\":{\"usage\":{\"prompt_tokens\":50,\"completion_tokens\":40}}}\n\n");
        // The nested usage is not top-level -> no completion, no tokens.
        assert!(!s.complete());
        assert_eq!(s.tokens(), (0, 0, 0));
    }

    #[test]
    fn usage_token_split_across_chunk_boundary_is_absorbed() {
        // B2: the `"usage"` prefilter must run on the REASSEMBLED line. Here the
        // literal token is split by the chunk boundary (`"us` + `age"`), so a
        // per-chunk-slice prefilter would miss it — only the cross-buffer line
        // splice sees the contiguous token.
        let full =
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":4}}\n\n";
        // Split inside the `"usage"` token (after the `"us` prefix).
        let split = find_subseq(full, b"\"usage\"", 0).expect("anchor") + 3;
        let (a, b) = full.split_at(split);
        assert!(a.ends_with(b"\"us"));
        assert!(b.starts_with(b"age\""));

        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(a);
        assert!(!s.complete(), "no newline yet -> nothing absorbed");
        s.feed(b);
        assert!(s.complete(), "reassembled line must be seen and absorbed");
        assert_eq!(s.tokens(), (11, 4, 0));
    }

    #[test]
    fn flush_saturates_huge_upstream_tokens() {
        // R-3: a hostile / malformed upstream report with u64::MAX tokens must
        // not overflow (debug panic / release wrap) on the flush recompute.
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        s.feed(
            b"data: {\"usage\":{\"prompt_tokens\":18446744073709551615,\
               \"completion_tokens\":18446744073709551615}}\n\n",
        );
        let m = s.flush(&FlushFields {
            request_content_bytes: 0,
            ..Default::default()
        });
        assert_eq!(m.input_token, u64::MAX);
        assert_eq!(m.output_token, u64::MAX);
        // recomputed = saturating_sum -> still u64::MAX (no wrap, no panic).
        assert_eq!(m.total_token, u64::MAX);
        assert!(m.completed);
    }

    // ----- MINOR-6 / F3: bounded tail, no unbounded single line, no O(n²) re-parse -----

    #[test]
    fn unclassified_tail_over_cap_stops_buffering_and_keeps_flush_semantics() {
        // A response that is neither SSE nor a parseable JSON object must not be buffered /
        // re-parsed without bound: once the unclassified tail passes MAX_TAIL_BYTES the snapshot
        // stops (Mode::Oversized) — the rest of the response is ignored and flush still works,
        // reporting `completed = false` (the server's byte-estimation fallback).
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        // Two 700 KiB junk chunks (no `data:`, no newline, no `}`-ending) push the tail past the
        // 1 MiB cap while still Unknown.
        let junk = vec![b'x'; 700 * 1024];
        s.feed(&junk);
        assert_eq!(s.mode, Mode::Unknown);
        assert_eq!(s.tail.len(), 700 * 1024);
        s.feed(&junk);
        assert_eq!(
            s.mode,
            Mode::Oversized,
            "cap must stop the Unknown accumulation"
        );
        assert!(s.tail.is_empty(), "buffered tail must be dropped");

        // After the cap the rest of the response is ignored (no more buffering / full re-parses)...
        assert!(!s.feed(b"{\"usage\":{\"prompt_tokens\":9,\"completion_tokens\":1}}"));
        assert!(s.tail.is_empty());
        // ... and flush keeps its contract: nothing absorbed -> completed = false, zero tokens.
        assert!(!s.complete());
        let m = s.flush(&FlushFields {
            model: "m".into(),
            request_content_bytes: 0,
            ..Default::default()
        });
        assert!(!m.completed);
        assert_eq!((m.input_token, m.output_token, m.total_token), (0, 0, 0));
    }

    #[test]
    fn oversized_unterminated_sse_line_is_dropped_and_stream_recovers() {
        // SSE single-line guard: an unterminated `data:` line must not grow the persistent tail
        // without bound. Once the pending line passes MAX_TAIL_BYTES it is dropped and the stream
        // skips to its `\n`; following lines are still counted/absorbed normally.
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        // Classify as SSE with a real (small) data line — counted once.
        assert!(!s.feed(b"data: {\"a\":1}\n"));
        assert_eq!(s.output_chunks(), 1);
        assert_eq!(s.mode, Mode::Sse);

        // Keep feeding a never-terminated line: first 600 KiB fits under the cap...
        let big = vec![b'y'; 600 * 1024];
        assert!(!s.feed(&big));
        assert_eq!(s.tail.len(), 600 * 1024);
        // ... the second 600 KiB pushes the pending line past the cap: it is dropped + skipped.
        assert!(!s.feed(&big));
        assert!(s.tail.is_empty(), "oversized line must not stay buffered");
        assert!(
            s.skip_until_newline,
            "remaining oversized line is skipped to its \\n"
        );

        // The giant line's terminator arrives; the line AFTER it is processed normally.
        assert!(s.feed(b"\ndata: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n"));
        assert!(s.complete());
        assert_eq!(s.tokens(), (7, 3, 0));
        // The oversized line was NOT counted; the first small line + the usage line were.
        assert_eq!(s.output_chunks(), 2);
        // The skip state was cleared and the tail is drained.
        assert!(!s.skip_until_newline);
        assert!(s.tail.is_empty());
    }

    #[test]
    fn fragmented_json_with_interior_brace_is_still_classified_at_completion() {
        // The Unknown JSON-parse gate must not skip a *legitimate* completion: a body that
        // contains `}` inside a string fails an early parse attempt, but when the final `}`
        // arrives it must still be classified and absorbed (bounded attempts, not "parse once").
        let body = b"{\"content\":\"}\",\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":4}}";
        let prefix: &[u8] = b"{\"content\":\"}"; // 13 bytes, shared by `body`'s head
        assert_eq!(&body[..prefix.len()], prefix);
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        // Feed an incomplete prefix that ends with `}` (parse attempt fails, attempt budget used)...
        assert!(!s.feed(prefix), "incomplete prefix cannot absorb usage");
        assert_eq!(s.json_parse_attempts, 1, "one failed attempt recorded");
        // ... then the remainder completes the object on the final `}`.
        let rest = &body[prefix.len()..];
        assert!(
            s.feed(rest),
            "completed JSON object with usage must be absorbed"
        );
        assert_eq!(s.tokens(), (11, 4, 0));
        assert_eq!(s.mode, Mode::Json);
        assert!(s.complete());
    }

    #[test]
    fn unknown_mode_parse_attempts_are_budgeted() {
        // The attempt budget bounds repeated full-DOM parses of a hostile `}`-ending fragment
        // stream: the snapshot gives up parsing after MAX_JSON_PARSE_ATTEMPTS failed attempts
        // (each `}`-ending feed still parses fine later only if it completes the object).
        let mut s = UsageSnapshot::new(UsageSchema::OpenAi);
        // A fragment that ends with `}` on EVERY feed but never becomes a complete object: the
        // classic O(n²) re-parse driver.
        let frag = b"{\"a\":\"";
        for i in 0..(MAX_JSON_PARSE_ATTEMPTS + 10) {
            let mut chunk = Vec::new();
            chunk.extend_from_slice(frag);
            chunk.extend_from_slice(format!("x{i}").as_bytes());
            chunk.push(b'}');
            assert!(
                !s.feed(&chunk),
                "never-complete fragments must not absorb usage"
            );
        }
        assert_eq!(
            s.json_parse_attempts, MAX_JSON_PARSE_ATTEMPTS,
            "parse attempts must be budgeted, not unbounded"
        );
        // Still Unknown (never classified, never overflowed the cap), flush is well-defined.
        assert_eq!(s.mode, Mode::Unknown);
        assert!(!s.complete());
        let m = s.flush(&FlushFields {
            request_content_bytes: 0,
            ..Default::default()
        });
        assert!(!m.completed);
    }

    // ----- Ora-5 T2: two DIFFERENT usage objects -> single-flush + last-wins -----

    #[test]
    fn usage_two_objects_in_one_data_line_absorbs_top_level_once() {
        // Ora-5 quality gap T2: a single final SSE `data:` line whose JSON
        // payload carries TWO different usage objects — a top-level `usage`
        // field AND an Anthropic-style nested `message.usage` (the only
        // physically realizable "two usage objects in one payload": JSON
        // cannot hold two `"usage"` keys at the same location). A naive
        // reading of the design ("absorb_value is last-wins per field") might
        // expect the two objects to be merged with the later one winning;
        // the REAL semantics pinned here are precedence + single absorption:
        // `usage_from_payload` picks the top-level `usage` and never consults
        // the `message.usage` fallback when a top-level `usage` exists, and a
        // data line absorbs at most ONE usage object — so there is no "later"
        // object inside a single line for per-field last-wins to span. The
        // flushed record carries the TOP-LEVEL object's values, exactly one
        // record, `completed = true`.
        let mut s = UsageSnapshot::new(UsageSchema::Generic);
        // Top-level usage uses OpenAI-family keys; the nested message.usage
        // uses Anthropic-family keys with DIFFERENT values on every
        // normalized field, so the winner is unambiguous.
        let line = b"data: {\"usage\":{\"prompt_tokens\":120,\"completion_tokens\":55,\
                      \"total_tokens\":190,\"prompt_tokens_details\":{\"cached_tokens\":9}},\
                      \"message\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":4,\
                      \"cache_read_input_tokens\":1}}}\n\n";
        assert!(s.feed(line), "top-level usage object must be absorbed");
        assert!(s.complete());
        assert_eq!(s.output_chunks(), 1, "one data line -> counted exactly once");
        // The nested message.usage (3 / 4 / 1) was NOT absorbed: top-level wins.
        assert_eq!(s.tokens(), (120, 55, 9));

        let m = s.flush(&FlushFields {
            model: "gw-generic".into(),
            request_content_bytes: 0,
            ..Default::default()
        });
        assert!(m.completed);
        assert_eq!((m.input_token, m.output_token), (120, 55));
        assert_eq!(m.input_cached_token, 9);
        assert_eq!(m.total_token, 190); // top-level object's upstream total
        assert_eq!(m.output_chunk_count, 1);
        assert_eq!(m.request_count, 1);
    }

    #[test]
    fn usage_two_objects_across_final_two_chunks_last_wins_single_flush() {
        // Ora-5 quality gap T2: two DIFFERENT usage objects arrive in the
        // stream's final two chunks, each on its own `data:` line, under the
        // Generic (gateway) schema. Per the design (`absorb_value` is
        // last-wins per field), the LATER object must override the earlier
        // one on every normalized field, each usage line counts as its own
        // chunk, and one terminal `flush` yields exactly ONE record carrying
        // the later object's values with `completed = true` — never a merge
        // or a sum of the two objects.
        let mut s = UsageSnapshot::new(UsageSchema::Generic);
        // First usage object: Anthropic-family keys.
        assert!(s.feed(
            b"data: {\"usage\":{\"input_tokens\":100,\"output_tokens\":40,\
              \"total_tokens\":150,\"cache_read_input_tokens\":12}}\n\n"
        ));
        assert!(s.complete());
        assert_eq!(s.tokens(), (100, 40, 12), "first object absorbed");

        // Second usage object: the SAME normalized fields via OpenAI-family
        // keys, with DIFFERENT values on every field.
        assert!(s.feed(
            b"data: {\"usage\":{\"prompt_tokens\":130,\"completion_tokens\":60,\
              \"total_tokens\":210,\"prompt_tokens_details\":{\"cached_tokens\":15}}}\n\n"
        ));
        // Last-wins already at absorption time: every field now reflects the
        // LATER object.
        assert_eq!(s.tokens(), (130, 60, 15));

        let m = s.flush(&FlushFields {
            model: "gw-generic".into(),
            request_content_bytes: 0,
            ..Default::default()
        });
        assert!(m.completed);
        assert_eq!((m.input_token, m.output_token), (130, 60));
        assert_eq!(m.input_cached_token, 15);
        assert_eq!(m.total_token, 210); // later object's total (> 130 + 60) kept
        assert_eq!(m.request_count, 1);
        // Both usage lines were separate events, each counted exactly once.
        assert_eq!(m.output_chunk_count, 2);
    }
}
