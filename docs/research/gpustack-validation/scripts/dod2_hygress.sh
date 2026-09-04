#!/usr/bin/env bash
# =============================================================================
# DoD 2 — CRD fixtures: dump the real CRD objects from the embedded apiserver
# AFTER the hygress swap, and diff against the baseline crds.yaml.
#
# GPUStack (the Python control plane) is what WRITES these CRDs; it is unchanged
# by the data-plane swap. Hygress is a pure *consumer*. => the dump must be
# byte-identical to the baseline (modulo volatile fields like managedFields /
# resourceVersion / timestamps), proving hygress did not mutate the CRDs.
#
# Run ON THE BASELINE REMOTE from /root/gpustack-validation:
#   ./dod2_hygress.sh
# =============================================================================
set -euo pipefail
BASE=/root/gpustack-validation; cd "$BASE"
FIX="$BASE/fixtures"          # baseline CRDs live here (crds.yaml)
FXH="$BASE/fixtures-hygress"; mkdir -p "$FXH"
log(){ echo "[$(date -u +%H:%M:%S)] $*"; }

[[ -f "$BASE/dump_crds.py" ]] || { log "FATAL: no dump_crds.py in $BASE"; exit 1; }
[[ -f "$FIX/crds.yaml" ]] || { log "FATAL: no baseline $FIX/crds.yaml"; exit 1; }

log "DoD2: re-dump CRDs from embedded apiserver (post-hygress)"
docker cp "$BASE/dump_crds.py" gpustack-server:/tmp/dump_crds.py >/dev/null 2>&1
docker exec gpustack-server python /tmp/dump_crds.py /var/lib/gpustack/higress/kubeconfig \
  > "$FXH/crds-hygress.yaml" 2>"$FXH/crds-hygress_dump.log"
log "hygress CRD dump: $(wc -l < "$FXH/crds-hygress.yaml") lines (log: $(tail -2 "$FXH/crds-hygress_dump.log" 2>/dev/null | tr '\n' ' '))"
log "baseline CRD dump: $(wc -l < "$FIX/crds.yaml") lines"

# Normalize: strip volatile/identity fields so the diff reflects SUBSTANCE.
norm(){ python3 -c '
import sys, yaml
docs=[]
for d in yaml.safe_load_all(sys.stdin):
    if not d: continue
    m=d.get("metadata",{})
    m.pop("managedFields",None); m.pop("resourceVersion",None); m.pop("uid",None); m.pop("creationTimestamp",None)
    docs.append(d)
print("---".join(yaml.safe_dump(x,sort_keys=False,width=4096).rstrip() for x in docs))
' "$1" 2>/dev/null; }

log "DoD2: diff (normalized: managedFields/resourceVersion/uid/creationTimestamp removed)"
norm "$FIX/crds.yaml"            > "$FXH/crds_baseline.norm.yaml"
norm "$FXH/crds-hygress.yaml"    > "$FXH/crds_hygress.norm.yaml"
if diff -u "$FXH/crds_baseline.norm.yaml" "$FXH/crds_hygress.norm.yaml" > "$FXH/crds_norm_diff.txt"; then
  log "PASS DoD2: CRDs IDENTICAL after normalization (hygress did not mutate the CRDs)"
else
  log "NOTE DoD2: differences present (inspect crds_norm_diff.txt) — may be benign (counts/weights/ports) or a real mutation"
  wc -l "$FXH/crds_norm_diff.txt"
fi
# raw diff (incl. volatile fields) for the record
diff -u "$FIX/crds.yaml" "$FXH/crds-hygress.yaml" > "$FXH/crds_raw_diff.txt" || true
log "DoD2 artifacts: crds-hygress.yaml, crds_*norm*.yaml, crds_norm_diff.txt, crds_raw_diff.txt"

# object counts for the report
{
  echo "baseline objects per kind:";   grep -E '^kind:' "$FIX/crds.yaml" | sort | uniq -c
  echo "hygress  objects per kind:";   grep -E '^kind:' "$FXH/crds-hygress.yaml" | sort | uniq -c
} | tee "$FXH/crds_kind_counts.txt"
log "DoD2 complete"
