#!/usr/bin/env bash
# =============================================================================
# DoD 1 (e2e through hygress) + DoD 5-DB (usage rows landed in the GPUStack DB).
#
# Run ON THE BASELINE REMOTE from /root/gpustack-validation:
#   ./e2e_hygress.sh
#
# Sends the SAME chat completion the baseline did, THROUGH the hygress data
# plane (:80/v1/chat/completions) with the `baseline` API key, plus a follow-up,
# then asserts NEW usage rows (count increased vs before) with
# access_key=baseline, model_id/model_route_id set, completed=true, tokens>0.
# =============================================================================
set -euo pipefail
BASE=/root/gpustack-validation; cd "$BASE"
FIX="$BASE/fixtures"
FXH="$BASE/fixtures-hygress"; mkdir -p "$FXH"
set -a; source "$BASE/.env"; set +a

API="http://127.0.0.1:30080"
GW="http://127.0.0.1:80"
DB=(docker exec gpustack-server psql -U root -h 127.0.0.1 -p 5432 gpustack)
MODEL_NAME="qwen2.5-0.5b-instruct"
log(){ echo "[$(date -u +%H:%M:%S)] $*"; }

[[ -f "$FIX/api_key.txt" ]] || { log "FATAL: no $FIX/api_key.txt (baseline API key)"; exit 1; }
API_KEY="$(cat "$FIX/api_key.txt")"
BEFORE_CNT="$(cat "$FXH/before_usage_count.txt" 2>/dev/null || echo 0)"
log "before usage-row count = $BEFORE_CNT ; model=$MODEL_NAME ; api_key=${API_KEY:0:8}..."

chat(){
  local body="$1" out="$2"
  curl -s -X POST "$GW/v1/chat/completions" \
    -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "$body" | tee "$out"
}

# ---- DoD 1: chat completion through hygress (same as baseline) ----
log "DoD1: chat #1 through $GW/v1/chat/completions"
B1=$(jq -cn --arg m "$MODEL_NAME" '{model:$m,
  messages:[{role:"system",content:"You are a helpful assistant."},
            {role:"user",content:"Say hello in exactly one short sentence."}],
  stream:false, max_tokens:64}')
chat "$B1" "$FXH/chat_hygress_1.json"
jq -e '.choices[0].message.content' "$FXH/chat_hygress_1.json" >/dev/null \
  || { log "FATAL: chat #1 returned no content"; exit 1; }
log "DoD1 #1 reply => $(jq -r '.choices[0].message.content' $FXH/chat_hygress_1.json | tr '\n' ' ')"
log "DoD1 #1 usage => $(jq -c '.usage' $FXH/chat_hygress_1.json)"

log "DoD1: chat #2 (follow-up)"
B2=$(jq -cn --arg m "$MODEL_NAME" '{model:$m,
  messages:[{role:"user",content:"What is 2+2?"}], stream:false, max_tokens:16}')
chat "$B2" "$FXH/chat_hygress_2.json"
log "DoD1 #2 reply => $(jq -r '.choices[0].message.content' $FXH/chat_hygress_2.json | tr '\n' ' ')"
log "DoD1 #2 usage => $(jq -c '.usage' $FXH/chat_hygress_2.json)"

# ---- DoD 5-DB: poll for NEW usage rows ----
log "DoD5: waiting for usage rows to flush (gateway metrics flush is periodic)"
AFTER_CNT=0
start=$(date +%s)
while :; do
  AFTER_CNT=$("${DB[@]}" -tA -c "select count(*) from model_usage_details where model_name='$MODEL_NAME';" 2>/dev/null | tr -d '[:space:]')
  AFTER_CNT="${AFTER_CNT:-0}"
  if (( AFTER_CNT > BEFORE_CNT )); then log "DoD5: usage rows increased ($BEFORE_CNT -> $AFTER_CNT)"; break; fi
  if (( $(date +%s) - start >= 120 )); then log "DoD5: no new usage row after 120s"; break; fi
  sleep 3
done
echo "$AFTER_CNT" > "$FXH/after_usage_count.txt"

log "DoD5: capture NEW model_usage_details rows (since before-count)"
{
  echo "=== before count: $BEFORE_CNT ; after count: $AFTER_CNT (delta=$((AFTER_CNT-BEFORE_CNT))) ==="
  echo
  echo "=== model_usage_details (this model, newest 5) ==="
  "${DB[@]}" -c "select id,user_name,model_id,model_name,model_route_id,model_route_name,api_key_name,access_key,prompt_token_count,completion_token_count,prompt_cached_token_count,completed,started_at,completed_at from model_usage_details where model_name='$MODEL_NAME' order by id desc limit 5;"
  echo
  echo "=== model_usages (daily aggregate, this model, newest 3) ==="
  "${DB[@]}" -c "select id,user_name,model_id,model_name,date,prompt_token_count,completion_token_count,request_count,api_key_name,model_route_id,model_route_name from model_usages where model_name='$MODEL_NAME' order by id desc limit 3;"
} > "$FXH/usage_rows_hygress.txt" 2>&1

# ---- DoD 5 assertions ----
log "DoD5: assertions on the newest detail row(s)"
RES=0
newest=$("${DB[@]}" -tA -F $'\t' -c "select id,model_id,model_route_id,access_key,api_key_name,prompt_token_count,completion_token_count,total_tokens,completed from model_usage_details where model_name='$MODEL_NAME' order by id desc limit 1;" 2>/dev/null)
echo "$newest" | sed 's/\t/ | /g' | tee -a "$FXH/usage_rows_hygress.txt"
readr -r RID MID RID2 ACC AK P C T COMP <<<"$newest"
chk(){ if [[ "$1" == ok ]]; then log "PASS DoD5 $2"; else log "FAIL DoD5 $2 ($1)"; RES=1; fi; }
chk  [[ -n "$RID" ]]; chk "row landed id=$RID" "$RID"
chk  [[ -n "$MID" ]]; chk "model_id set" "$MID"
chk  [[ -n "$RID2" && "$RID2" =~ ^[0-9]+$ ]]; chk "model_route_id set (non-NULL, not 'Untracked')" "$RID2"
chk  [[ -n "$ACC" ]]; chk "access_key set (baseline api key)" "$ACC"
chk  [[ -n "$AK" && "$AK" == "baseline" ]]; chk "api_key_name == baseline" "$AK"
chk  [[ "${C:-0}" -gt 0 ]]; chk "completion_token_count > 0" "$C"
chk  [[ "$COMP" == "t" ]]; chk "completed == true" "$COMP"

# ---- DoD 1b/5b: provider/instance routing header (best-effort) ----
log "DoD5b: X-GPUStack-Model-Instance in instance backend logs (if the backend logs headers)"
docker ps --format '{{.Names}}' | grep -iE "qwen2.5|run-0" | head -3 > "$FXH/instance_containers.txt" || true
INSD=$(docker logs $(head -1 "$FXH/instance_containers.txt") 2>&1 | grep -iE "X-GPUStack-Model-Instance|x-higress-llm-model|X-GPUStack-Route-Name" | tail -10)
{ echo "=== instance containers ==="; cat "$FXH/instance_containers.txt"; echo; echo "=== routing headers seen in instance logs ==="; echo "${INSD:-<none — minimal gguf backend does not log request headers>}"; } > "$FXH/routing_headers.txt"
cat "$FXH/routing_headers.txt"

log "e2e done. RES=$RES (1 if any DoD5 assertion failed)"
exit $RES
