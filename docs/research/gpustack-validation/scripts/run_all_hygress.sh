#!/usr/bin/env bash
# =============================================================================
# Run the full Hygress A/B on the baseline remote, in order:
#   ship  (build gpustack:hygress) -> swap -> DoD6 -> e2e/usage -> DoD2
#
# Local:    REMOTE=root@host ./run_all_hygress.sh
# (ships artifacts, then runs swap.sh + dod6.sh + e2e_hygress.sh + dod2_hygress.sh
#  on the remote via ssh)
# =============================================================================
set -uo pipefail
S="$(cd "$(dirname "$0")" && pwd)"
[[ -n "${REMOTE:?usage: REMOTE=root@host ./run_all_hygress.sh}" ]] || exit 1

echo "### 1) ship + build gpustack:hygress"
REMOTE="$REMOTE" "$S/ship_to_remote.sh" || { echo "build failed"; exit 1; }

ssh() { command ssh -o StrictHostKeyChecking=no "$@"; }

echo "### 2) swap"
ssh "$REMOTE" "cd /root/gpustack-validation && bash $S/../scripts/swap.sh" 2>/dev/null || \
ssh "$REMOTE" "cd /root/gpustack-validation && ./swap.sh"

echo "### 3) DoD 6"
ssh "$REMOTE" "cd /root/gpustack-validation && ./dod6.sh"

echo "### 4) DoD 1 + DoD 5-DB"
ssh "$REMOTE" "cd /root/gpustack-validation && ./e2e_hygress.sh"

echo "### 5) DoD 2"
ssh "$REMOTE" "cd /root/gpustack-validation && ./dod2_hygress.sh"

echo "### copy evidence back (local)"
rsync -a "$REMOTE:/root/gpustack-validation/fixtures-hygress/" \
        "$S/../fixtures-hygress/"
echo "### DONE -> $S/../fixtures-hygress/"
