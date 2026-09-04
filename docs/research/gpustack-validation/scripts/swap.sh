#!/usr/bin/env bash
# =============================================================================
# Swap the baseline gpustack-server for the Hygress data plane and validate boot.
#
# Run ON THE BASELINE REMOTE from /root/gpustack-validation:
#   ./swap.sh
#
# Does:
#   1. Snapshot baseline usage count (DB) + container evidence ("before").
#   2. docker rm -f gpustack-server; docker compose -f compose-hygress.yaml up -d
#      gpustack-server  (ONLY the server; worker + server-data volume untouched).
#   3. Wait for readiness (server API :30080/healthz AND gateway :80).
#   4. Capture the full s6 boot log + a ps snapshot.
# =============================================================================
set -uo pipefail
BASE=/root/gpustack-validation
cd "$BASE"
FIX="$BASE/fixtures"
FXH="$BASE/fixtures-hygress"
mkdir -p "$FXH"
set -a; source "$BASE/.env"; set +a

API="http://127.0.0.1:30080"
GW="http://127.0.0.1:80"
DBCMD=(docker exec gpustack-server psql -U root -h 127.0.0.1 -p 5432 gpustack)
MODEL_NAME="qwen2.5-0.5b-instruct"
log(){ echo "[$(date -u +%H:%M:%S)] $*"; }

log "== HYGRESS SWAP =="
log "Step 1: snapshot baseline (before) usage count"
"${DBCMD[@]}" -tA -c "select count(*) from model_usage_details where model_name='$MODEL_NAME';" | tr -d '[:space:]' > "$FXH/before_usage_count.txt" || echo 0 > "$FXH/before_usage_count.txt"
log "before usage-row count: $(cat $FXH/before_usage_count.txt)"
docker ps -a | tee "$FXH/containers_before.txt" >/dev/null
docker exec gpustack-server ps aux 2>/dev/null | grep -E "hygress|envoy|pilot|controller|supercronic" | tee "$FXH/ps_before.txt"

log "Step 2: swap the server container (worker + server-data volume KEPT)"
log "image: gpustack:hygress (was quay.io/gpustack/gpustack:latest)"
command -v gpustack_image >/dev/null 2>&1
docker image inspect gpustack:hygress >/dev/null 2>&1 || { log "FATAL: gpustack:hygress image not present; run ship_to_remote.sh first"; exit 1; }
docker rm -f gpustack-server
docker compose -f compose-hygress.yaml up -d gpustack-server | tee "$FXH/compose_up_hygress.log"

log "Step 3: wait for readiness"
start=$(date +%s)
ok(){ curl -sf -m 5 "$API/healthz" >/dev/null 2>&1; }
while :; do
  if ok; then log "OK: server API /healthz"; break; fi
  (( $(date +%s) - start >= 300 )) && { log "FATAL: server API not ready in 300s"; docker logs --tail 120 gpustack-server; exit 1; }
  sleep 3
done
# gateway :80 ready via hygress mirror passthrough
while :; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -m 5 "$GW/readyz" 2>/dev/null)
  if [[ "$code" == "200" ]]; then log "OK: gateway :80/readyz -> 200 (through hygress mirror)"; break; fi
  if (( $(date +%s) - start >= 420 )); then log "FATAL: gateway :80/readyz not 200 in 420s (10-min lock?)"; break; fi
  sleep 3
done

log "Step 4: capture s6 boot log + ps"
docker logs gpustack-server | tee "$FXH/server_logs.txt" >/dev/null
docker exec gpustack-server ps aux 2>/dev/null | grep -E "hygress|envoy|pilot|controller|supercronic" | tee "$FXH/ps_after.txt"
docker ps | tee "$FXH/containers_after.txt" >/dev/null
log "SWAP COMPLETE. Run: ./dod6.sh ; ./e2e_hygress.sh ; ./dod2_hygress.sh"
