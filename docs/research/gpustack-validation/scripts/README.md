# Hygress A/B — remote swap + validation tooling

These scripts perform the **Hygress gateway swap** on the *running* baseline
(`gpustack-server` + `gpustack-worker`) and record the DoD 1 / 2 / 5-DB / 6 evidence.
They are the "hygress side" counterpart to the baseline `bootstrap.sh`.

## Prereqs
- The Hygress repo (has `target/release/hygress`, `pack/Dockerfile.hygress`, `pack/hygress-s6/`).
- The baseline remote that is **already running** the baseline (has
  `/root/gpustack-validation/{compose.yaml,.env,fixtures/}`).
- Access to the remote as `root` via ssh (the baseline's API key lives in
  `fixtures/api_key.txt` — read-only reuse, never re-created).

## Scripts
| Script | Runs | Purpose |
|---|---|---|
| `ship_to_remote.sh` | local | Ship binary + `pack/` to `/root/hygress-deploy/`, build `gpustack:hygress`, smoke-test the binary loads. |
| `swap.sh` | remote | Snapshot **before** usage count → `docker rm -f gpustack-server` → `docker compose -f compose-hygress.yaml up -d gpustack-server` (server only) → wait readiness → capture s6 boot log. |
| `dod6.sh` | remote | DoD 6 image/port/ops checks (apiserver :18443, :80, :8081, :15020, NO 9876/15010/15012/8888/15051; hygress proc present & envoy/pilot/controller gone; supercronic healthy + no `readinessCheck.*Higress Pilot`; `:80/readyz==200`; no panic; rollback `.dist` artifacts; `gateway/run` is the hygress launcher). |
| `e2e_hygress.sh` | remote | DoD 1 (chat completion through `:80` with the `baseline` key + follow-up) + DoD 5-DB (new usage rows, before/after, access_key/baseline, model_id/model_route_id set, completed=true, tokens>0) + routing-header probe. |
| `dod2_hygress.sh` | remote | DoD 2 re-dump CRDs from the embedded apiserver + normalized & raw diff vs `fixtures/crds.yaml`. |
| `run_all_hygress.sh` | local | Orchestrates ship → swap → dod6 → e2e → dod2, then rsync evidence back. |
| `compose-hygress.yaml` | remote | Baseline compose with **only** the `gpustack-server` image changed to `gpustack:hygress`; worker + server-data volume identical, worker not restarted. |
| `dump_crds.py` | (copied into server) | Reused from the baseline — dumps all Higress/Envoy CRD objects from the embedded apiserver. |

## Evidence layout (on the remote → synced back to `../fixtures-hygress/`)
```
fixtures-hygress/
  before_usage_count.txt  after_usage_count.txt
  containers_before.txt   containers_after.txt
  ps_before.txt           ps_after.txt
  compose_up_hygress.log  server_logs.txt
  chat_hygress_1.json     chat_hygress_2.json
  usage_rows_hygress.txt  routing_headers.txt
  crds-hygress.yaml       crds_*norm*.yaml   crds_norm_diff.txt  crds_raw_diff.txt
  crds_kind_counts.txt
  dod6_results.txt        dod6_ports.txt     dod6_ps.txt         dod6_hygress_log_tail.txt
```

## Manual step-by-step (without run_all)
```
# local
REMOTE=root@<host> ./ship_to_remote.sh
# remote  (cd /root/gpustack-validation)
./swap.sh
./dod6.sh
./e2e_hygress.sh
./dod2_hygress.sh
# local: rsync /root/gpustack-validation/fixtures-hygress/ back
```

## Rollback (data plane only; DB + CRDs + worker untouched)
`docker rm -f gpustack-server && docker compose -f compose.yaml up -d gpustack-server`
(re-runs the pristine `quay.io/gpustack/gpustack:latest` image; the preserved
`/etc/s6-overlay/s6-rc.d.dist/` scripts are what that stock image ships anyway).
