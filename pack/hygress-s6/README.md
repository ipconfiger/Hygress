# Hygress s6-overlay surgery (design §11.1 — GPUStack embedded in-place replacement)

Image-layer s6 scripts that swap the embedded Higress `pilot / controller / gateway`
processes for a single `hygress` process, keeping the lightweight embedded `apiserver`
(strategy 2) and `supercronic`. **Zero Python change.**

## Mechanism (IMPORTANT)
GPUStack's `prepare_s6_overlay` regenerates the s6 enabled-service set from the Python
`gateway_services` list on every start — so an *added* s6 service is never enabled, and
deleting `contents.d` entries does not survive. The surgery therefore **reuses the existing
`gateway` service slot**: `gateway/run` IS the hygress launcher. `pilot/run`/`controller/run`
become long-sleep no-ops (keep the s6 topology + a rollback switch). `apiserver` stays.

## Layout (relative to image `/`)
```
etc/s6-overlay/s6-rc.d/gateway/run        = hygress launcher (wait GPUSTACK_API_PORT → exec hygress; binds after first snapshot)
etc/s6-overlay/s6-rc.d/pilot/run          = long-sleep no-op (NO exit 0 — restart loop)
etc/s6-overlay/s6-rc.d/controller/run     = long-sleep no-op
etc/s6-overlay/s6-rc.d/supercronic/run    = edited: drop `readinessCheck "Higress Pilot" 15010`
```

## Port discipline
- data plane: `GATEWAY_HTTP_PORT` (default 80) / `GATEWAY_HTTPS_PORT` (443)
- admin: `HYGRESS_ADMIN_ADDR` (127.0.0.1:8081); stats: `GATEWAY_PILOT_AGENT_METRICS_PORT` (15020) `/stats/prometheus`
- NEVER binds 9876/15010/15012/8888/15051

## access.log / logrotate / hygress.log
The original `gateway/run` created `${HIGRESS_LOG_DIR}/access.log` for the hourly logrotate
(supercronic-driven). The launcher keeps that contract for the logrotate tooling — it `touch`es
`${HIGRESS_LOG_DIR}/access.log` (kept **empty**: no per-request writes; GPUStack logrotate rotates the
empty file) — and appends the hygress process output to **`${GPUSTACK_DATA_DIR}/log/hygress.log`**
(host-visible under the GPUStack data bind volume, for diagnosis). It does **not** write request logs
into `access.log`.

## Rollback (DoD 6)
`pack/Dockerfile.hygress` copies the surgery scripts; pristine upstream `gateway/pilot/controller/run`
are preserved under `/etc/s6-overlay/s6-rc.d.dist/` in the image. Rollback = copy them back +
restore supercronic's pilot check + rebuild the image (or keep a `s6-rc.d.dist` bind-mount).

## Verification checklist (real GPUStack container)
- [ ] apiserver (18443) + supercronic + `gateway`(=hygress) up; pilot/controller no-ops idle
- [ ] GPUStack `_wait_for_gateway_ready` passes on `GATEWAY_HTTP_PORT`
- [ ] chat completion routes through hygress; usage rows land (model_id/model_route_id/access_key)
- [ ] `:15020/stats/prometheus` + `:8081/metrics` respond; forbidden ports unused (`ss -ltn`)
