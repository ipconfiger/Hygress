#!/usr/bin/env bash
# =============================================================================
# Ship the Hygress artifacts to the baseline remote and build the swap image.
#
# Run LOCALLY from the Hygress repo root:
#   REMOTE=root@<baseline-host> ./ship_to_remote.sh
#
# Layout on the remote (keeps Dockerfile.hygress relative COPY paths resolvable):
#   /root/hygress-deploy/
#     Dockerfile                       <- pack/Dockerfile.hygress
#     pack/hygress-s6/...              <- s6 run scripts (gateway/pilot/controller/supercronic)
#     target/release/hygress           <- the built x86-64 PIE binary
#
# Build:
#   docker build -f /root/hygress-deploy/Dockerfile -t gpustack:hygress /root/hygress-deploy
# =============================================================================
set -euo pipefail
REMOTE="${REMOTE:?usage: REMOTE=root@host ./ship_to_remote.sh}"
REPO="$(cd "$(dirname "$0")" && pwd)/../.."   # scripts/ -> gpustack-validation/ -> repo root
HYGR="$REPO"
DEPLOY=/root/hygress-deploy
echo "Hygress source : $HYGR"
echo "Remote         : $REMOTE"
echo "Remote deploy  : $DEPLOY"

[[ -f "$HYGR/target/release/hygress" ]] || { echo "MISSING binary: $HYGR/target/release/hygress"; exit 1; }
[[ -f "$HYGR/pack/Dockerfile.hygress" ]] || { echo "MISSING Dockerfile: $HYGR/pack/Dockerfile.hygress"; exit 1; }
[[ -d "$HYGR/pack/hygress-s6" ]] || { echo "MISSING pack dir: $HYGR/pack/hygress-s6"; exit 1; }

ssh "$REMOTE" "mkdir -p $DEPLOY/pack $DEPLOY/target/release"

# rsync the pack/ tree (s6 scripts)
rsync -a --delete "$HYGR/pack/" "$REMOTE:$DEPLOY/pack/"
# the binary (single file)
rsync -a "$HYGR/target/release/hygress" "$REMOTE:$DEPLOY/target/release/hygress"
# the Dockerfile (renamed to Dockerfile at deploy root)
scp "$HYGR/pack/Dockerfile.hygress" "$REMOTE:$DEPLOY/Dockerfile"

echo "=== verify remote layout ==="
ssh "$REMOTE" "find $DEPLOY -type f | sort"

echo "=== build image gpustack:hygress ==="
ssh "$REMOTE" "docker build -f $DEPLOY/Dockerfile -t gpustack:hygress $DEPLOY"

echo "=== image verified ==="
ssh "$REMOTE" "docker image inspect gpustack:hygress --format 'id={{.Id}} size={{.Size}} bytes'"

echo "=== smoke: hygress binary loads in image (fail-fast is a PASS) ==="
# Expect a NON-ZERO exit with a readiness/log message (not a missing-symbol crash).
ssh "$REMOTE" 'set +e
  docker run --rm --env GPUSTACK_API_PORT=30080 --env GATEWAY_HTTP_PORT=80 \
    --env HYGRESS_KUBECONFIG=/var/lib/gpustack/higress/kubeconfig \
    gpustack:hygress /usr/local/bin/hygress 2>&1 | tail -25
  echo "exit=${PIPESTATUS[0]}"
'
echo "DONE. Next: ./swap.sh REMOTE=$REMOTE"
