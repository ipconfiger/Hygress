#!/usr/bin/env bash
# =============================================================================
# Baseline (unmodified) GPUStack v2 end-to-end validation
#
# Brings up:  server (embedded Higress gateway :80 + API :30080)
#             +     one GPU worker (RTX 4090)
# Then:      creates admin -> API key -> small GGUF model + ModelRoute
#            runs a REAL chat completion THROUGH the embedded gateway (:80),
#            verifies the usage row lands in the GPUStack DB (Postgres),
#            and dumps the real CRD objects the embedded Higress received.
#
# Idempotent: safe to re-run.  `SKIP_UP=1` skips `docker compose up` when the
# containers are already running.
#
# Usage (on the remote host, from /root/gpustack-validation):
#   ./bootstrap.sh            # full flow (compose up + everything)
#   SKIP_UP=1 ./bootstrap.sh  # containers already up; just (re)run the flow
# =============================================================================
set -uo pipefail

BASE=/root/gpustack-validation
cd "$BASE"
FIX="$BASE/fixtures"
mkdir -p "$FIX"

# ---- configuration ----------------------------------------------------------
set -a; source "$BASE/.env"; set +a
PW="$GPUSTACK_BOOTSTRAP_PASSWORD"
TOKEN="$GPUSTACK_TOKEN"

API="http://127.0.0.1:30080"      # management + OpenAI API server
GW="http://127.0.0.1:80"          # embedded Higress gateway
MODEL_NAME="qwen2.5-0.5b-instruct"
HF_REPO="Qwen/Qwen2.5-0.5B-Instruct-GGUF"
HF_FILE="qwen2.5-0.5b-instruct-q4_k_m.gguf"
# ghcr.io large image layers transfer at ~65KB/s from this host (the default
# cuda llama.cpp:server-cuda has a single ~2GB layer; even the CPU :server
# 265MB layer is ~3.9MB/60s) - not fetchable. So we use a GPUStack *Custom*
# backend whose image we BUILD LOCALLY on the GPUStack base (python3.11 + cmake
# + 16 cores), installing llama-cpp-python from PyPI/Aliyun-Mirrors (reachable)
# to serve the GGUF via a minimal OpenAI-compatible server. The A/B compares the
# GATEWAY (usage rows + CRDs), which is independent of CPU vs GPU inference.
# Triple-slash tag [registry]/[namespace]/[repo]:tag so that parse_image yields a
# real registry, making BOTH override pass-throughs (apply_registry_override_to_image
# and the deployer's adjust_image_with_envs) return it unchanged. The deployer's
# default IF_NOT_PRESENT policy then resolves it from the local dockerd store (no pull).
BACKEND_IMG="127.0.0.1:5000/gpustack-serve/gguf:latest"
BACKEND_RUN_COMMAND="-m {{model_path}} --port {{port}} --alias {{model_name}}"
DBCMD=(docker exec gpustack-server psql -U root -h 127.0.0.1 -p 5432 gpustack)
AUTH=(-u "admin:$PW")

log()  { echo "[$(date -u +%H:%M:%S)] $*"; }
die()  { echo "[$(date -u +%H:%M:%S)] FATAL: $*" >&2; exit 1; }

# api METHOD PATH [JSON-BODY]  -> prints body (fails nonzero on HTTP error)
api() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sf -X "$method" -H "Content-Type: application/json" -d "$body" "${AUTH[@]}" "$API$path"
  else
    curl -sf -X "$method" "${AUTH[@]}" "$API$path"
  fi
}

# api_raw METHOD PATH [JSON-BODY] -> prints body regardless of HTTP status
# (so validation errors are visible); caller must validate the response.
api_raw() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -s -X "$method" -H "Content-Type: application/json" -d "$body" "${AUTH[@]}" "$API$path"
  else
    curl -s -X "$method" "${AUTH[@]}" "$API$path"
  fi
}

wait_for() { # desc timeout cmd...
  local desc="$1" timeout="$2"; shift 2
  local start=$(date +%s)
  log "Waiting for: $desc (timeout ${timeout}s)"
  while :; do
    if "$@" >/dev/null 2>&1; then log "OK: $desc"; return 0; fi
    if (( $(date +%s) - start >= timeout )); then
      fail "Timed out waiting for: $desc"; return 1
    fi
    sleep 3
  done
}
fail() { echo "[$(date -u +%H:%M:%S)] ERROR: $*" >&2; }

server_ready() { curl -sf -m 5 "$API/healthz" >/dev/null; }
worker_ready() {
  api GET /v2/workers | jq -e '
    [.items[] | select(
        ((.status.gpu_devices // .gpu_devices // []) | length) > 0
        or ((.gpus // 0) > 0)
    )] | length > 0
  ' >/dev/null 2>&1
}
model_running() {
  api GET "/v2/models/$MODEL_ID/instances" | jq -e '
    [.items[] | select(.state=="running")] | length > 0
  ' >/dev/null 2>&1
}

# =============================================================================
step_up() {
  [[ "${SKIP_UP:-0}" == "1" ]] && { log "SKIP_UP=1: skipping compose up"; return 0; }
  log "Step 1: docker compose up -d"
  docker compose -f "$BASE/compose.yaml" up -d | tee "$FIX/compose_up.log"
}

step_build_image() {
  log "Step 1.5: build local GGUF server image $BACKEND_IMG (skip if present)"
  if docker image inspect "$BACKEND_IMG" >/dev/null 2>&1; then
    log "image $BACKEND_IMG already present; skipping build"
    return 0
  fi
  # Build on the GPUStack base: installs llama-cpp-python (CPU) from PyPI/Aliyun
  # mirror + our serve.py. Compiles ~1-3 min on 16 cores. Build to a plain tag,
  # then re-tag to the "localhost/..." reference GPUStack expects.
  docker build -f "$BASE/Dockerfile.serve" -t gpustack-gguf-serve:build "$BASE" 2>&1 | tee "$FIX/image_build.log" \
    | tail -3
  docker tag gpustack-gguf-serve:build "$BACKEND_IMG"
  docker image inspect "$BACKEND_IMG" >/dev/null 2>&1 || die "image build failed (see fixtures/image_build.log)"
  log "image $BACKEND_IMG built"
}

step_wait_server() {
  log "Step 2: wait for server API (/healthz)"
  wait_for "server /healthz" 240 server_ready || {
    echo "--- server logs (tail) ---" >&2
    docker logs --tail 100 gpustack-server >&2
    die "server did not become ready"
  }
  log "healthz => $(curl -s "$API/healthz")"
}

step_wait_worker() {
  log "Step 3: wait for a GPU worker to register"
  wait_for "GPU worker registered" 240 worker_ready || {
    echo "--- worker logs (tail) ---" >&2
    docker logs --tail 100 gpustack-worker >&2
    die "no GPU worker registered"
  }
  echo "$("api" GET /v2/workers 2>/dev/null | jq -c '.items[] | {name, state, state_message, gpus:(.gpus|map({gpu_type, name, available}))}')" | tee "$FIX/workers.json"
}

step_openapi() {
  log "Step 4: capture API surface (openapi.json)"
  curl -s "$API/openapi.json" > "$FIX/openapi.json" || true
  local sz; sz=$(wc -c < "$FIX/openapi.json"); log "openapi.json bytes: $sz"
}

step_apikey() {
  log "Step 5: ensure API key 'baseline'"
  local keyval
  # reuse existing if present (value is not re-exposed; only creation returns it)
  if curl -sf "${AUTH[@]}" "$API/v2/api-keys?search=baseline" 2>/dev/null \
       | jq -e '.items | length > 0' >/dev/null; then
    fail "API key 'baseline' already exists; reading stored key from fixtures"
    [[ -f "$FIX/api_key.txt" ]] || die "no stored api_key.txt; delete the key and re-run"
    API_KEY="$(cat "$FIX/api_key.txt")"
    log "Reusing stored API key (masked: ${API_KEY:0:8}...)"
    return 0
  fi
  local resp
  resp=$(api_raw POST /v2/api-keys '{"name":"baseline"}')
  API_KEY=$(echo "$resp" | jq -r '.value // .access_key // empty')
  [[ -n "$API_KEY" ]] || die "API key creation failed: $resp"
  echo "$API_KEY" > "$FIX/api_key.txt"
  log "Created API key id=$(echo "$resp" | jq -r .id) (value stored in fixtures/api_key.txt)"
}

step_model() {
  log "Step 6: ensure model + model-route '$MODEL_NAME'"
  # v2.2.3 models require an explicit cluster_id (NOT NULL)
  local cluster_id
  cluster_id=$(curl -sf "${AUTH[@]}" "$API/v2/clusters" 2>/dev/null | jq -r '.items[0].id // empty')
  [[ -n "$cluster_id" ]] || die "no cluster found via /v2/clusters"
  log "using cluster_id=$cluster_id"
  local existing
  existing=$(curl -sf "${AUTH[@]}" "$API/v2/models" 2>/dev/null | jq -r --arg n "$MODEL_NAME" '.items[] | select(.name==$n) | .id' | head -1)
  if [[ -n "$existing" ]]; then
    MODEL_ID="$existing"
    log "Model '$MODEL_NAME' already exists (id=$MODEL_ID); reusing"
  else
    local body resp
    body=$(jq -cn --arg n "$MODEL_NAME" --arg r "$HF_REPO" --arg f "$HF_FILE" --argjson cid "$cluster_id" --arg img "$BACKEND_IMG" --arg rc "$BACKEND_RUN_COMMAND" '{
      name:$n,
      backend:"Custom",
      image_name:$img,
      run_command:$rc,
      replicas:1,
      cluster_id:$cid,
      source:"huggingface",
      huggingface_repo_id:$r,
      huggingface_filename:$f,
      enable_model_route:true
    }')
    resp=$(api_raw POST /v2/models "$body")
    MODEL_ID=$(echo "$resp" | jq -r '.id // empty')
    [[ -n "$MODEL_ID" ]] || die "model create failed: $resp"
    log "Created model (id=$MODEL_ID)"
  fi
  echo "$MODEL_ID" > "$FIX/model_id.txt"

  # Ensure a model route exists (create explicitly if the flag was not honored)
  local route_id
  route_id=$(curl -sf "${AUTH[@]}" "$API/v2/model-routes" 2>/dev/null | jq -r --arg n "$MODEL_NAME" '.items[] | select(.name==$n) | .id' | head -1)
  if [[ -z "$route_id" ]]; then
    log "No model route auto-created; creating via POST /v2/model-routes"
    local rbody rresp
    rbody=$(jq -cn --arg n "$MODEL_NAME" --argjson mid "$MODEL_ID" '{
      name:$n,
      targets:[{model_id:$mid, weight:100}]
    }')
    rresp=$(api_raw POST /v2/model-routes "$rbody")
    rid=$(echo "$rresp" | jq -r '.id // empty')
    [[ -n "$rid" ]] || die "model route create failed: $rresp"
    log "Created model route (id=$rid)"
  else
    log "Model route exists (id=$route_id)"
  fi
}

step_wait_instance() {
  log "Step 7: wait for model instance to be RUNNING (download+pull+load can take minutes)"
  wait_for "model instance running" 900 model_running || {
    echo "--- instance state ---" >&2
    curl -sf "${AUTH[@]}" "$API/v2/models/$MODEL_ID/instances" | jq '.' >&2 || true
    echo "--- worker logs (tail) ---" >&2
    docker logs --tail 120 gpustack-worker >&2
    die "model instance did not reach RUNNING"
  }
  curl -sf "${AUTH[@]}" "$API/v2/models/$MODEL_ID/instances" | jq '.' | tee "$FIX/model_instances.json"
}

step_chat() {
  log "Step 8: run chat completion THROUGH the embedded gateway ($GW/v1/chat/completions)"
  local body
  body=$(jq -cn --arg m "$MODEL_NAME" '{
    model:$m,
    messages:[{role:"system",content:"You are a helpful assistant."},{role:"user",content:"Say hello in exactly one short sentence."}],
    stream:false,
    max_tokens:64
  }')
  log "request: $GW/v1/chat/completions  (Authorization: Bearer <api-key>)"
  log "body:    $body"
  api_key=$(cat "$FIX/api_key.txt")
  curl -s -X POST "$GW/v1/chat/completions" \
     -H "Content-Type: application/json" -H "Authorization: Bearer $api_key" \
     -d "$body" | tee "$FIX/chat_response.json"
  # verify it actually produced content
  jq -e '.choices[0].message.content' "$FIX/chat_response.json" >/dev/null || die "chat completion returned no content"
  log "chat OK => $(jq -r '.choices[0].message.content' "$FIX/chat_response.json" | tr '\n' ' ')"
}

step_usage() {
  log "Step 9: verify usage row in the GPUStack DB (poll; gateway metrics flush is periodic)"
  local start=$(date +%s)
  while :; do
    local n
    n=$("${DBCMD[@]}" -tA -c "select count(*) from model_usage_details where model_name='$MODEL_NAME';" 2>/dev/null | tr -d '[:space:]')
    if [[ -n "$n" && "$n" -gt 0 ]]; then log "found $n usage detail row(s)"; break; fi
    if (( $(date +%s) - start >= 120 )); then fail "no model_usage_details row appeared"; "$DBCMD[@]" -c "\d model_usage_details" >&2 || true; die "usage row not found"; fi
    sleep 3
  done
  log "--- model_usage_details (this model, newest 3) ---"
  echo "=== model_usage_details (per-request) ===" | tee "$FIX/usage_row.txt"
  "${DBCMD[@]}" -c "select id,user_name,model_id,model_name,model_route_id,model_route_name,api_key_name,access_key,prompt_token_count,completion_token_count,prompt_cached_token_count,completed,started_at,completed_at from model_usage_details where model_name='$MODEL_NAME' order by id desc limit 3;" | tee -a "$FIX/usage_row.txt"
  log "--- model_usages (daily aggregate) ---"
  echo "=== model_usages (daily aggregate) ===" | tee -a "$FIX/usage_row.txt"
  "${DBCMD[@]}" -c "select id,user_name,model_id,model_name,date,prompt_token_count,completion_token_count,request_count,api_key_name,model_route_id,model_route_name from model_usages where model_name='$MODEL_NAME' order by id desc limit 3;" | tee -a "$FIX/usage_row.txt"
}

step_crds() {
  log "Step 10: dump real CRD objects from the embedded Higress apiserver"
  docker cp "$BASE/dump_crds.py" gpustack-server:/tmp/dump_crds.py >/dev/null 2>&1
  docker exec gpustack-server python /tmp/dump_crds.py /var/lib/gpustack/higress/kubeconfig > "$FIX/crds.yaml" 2>"$FIX/crds_dump.log"
  log "CRD dump ($(wc -l < "$FIX/crds.yaml") lines) -> fixtures/crds.yaml"
  log "log: $(cat "$FIX/crds_dump.log" 2>/dev/null | tail -3)"
}

step_summary() {
  log "Step 11: capture host/container evidence"
  docker ps -a | tee "$FIX/containers.txt"
  ss -ltn | tee "$FIX/ports.txt"
}

# =============================================================================
main() {
  log "==== GPUStack baseline bootstrap start ===="
  step_up
  step_build_image
  step_wait_server
  step_wait_worker
  step_openapi
  step_apikey
  step_model
  export MODEL_ID
  step_wait_instance
  step_chat
  step_usage
  step_crds
  step_summary
  log "==== DONE ===="
  log "API key:       fixtures/api_key.txt"
  log "chat response: fixtures/chat_response.json"
  log "usage row:     fixtures/usage_row.txt"
  log "CRDs:          fixtures/crds.yaml"
}

# allow running a single step: ./bootstrap.sh <fn>  e.g.  ./bootstrap.sh step_chat
if [[ -n "${1:-}" && "${1:-}" == step_* ]]; then
  # steps that need prior state set globals from fixtures
  if [[ -f "$FIX/api_key.txt" ]]; then API_KEY="$(cat "$FIX/api_key.txt")"; fi
  if [[ -f "$FIX/model_id.txt" ]]; then export MODEL_ID="$(cat "$FIX/model_id.txt")"; fi
  "$1"
else
  main
fi
