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

## Data-plane TLS (:443) & certificate rotation

- **Default-install delta (ORA3-M19)**: Hygress binds `GATEWAY_TLS_PORT` (443) **only when** the control-plane
  snapshot carries a managed `gpustack-tls-*` Secret; GPUStack writes those Secrets **only when launched with
  `--ssl-keyfile`/`--ssl-certfile`**. A default GPUStack install (no `--ssl-*`) therefore has **no :443
  listener** → `https://host:443` = connection refused, whereas the real embedded Higress serves an auto
  self-signed cert page there. Remedy: launch GPUStack with `--ssl-keyfile`/`--ssl-certfile` (Hygress's cert
  name syntax and managed path already match those), or accept plain http on :80.
- **SNI constraint (OX-9)**: pingora 0.8's file-path listener API serves the default/first host's cert PEM for
  **all** SNI names (the per-host SniStore is reflected but not wired to the live listener) → deployments with
  more than one distinct TLS domain get a hostname mismatch beyond the default cert; single-default-cert is a
  documented constraint.
- **Rotation runbook (OX-9)**: a TLS content change in the snapshot is detected at runtime (60s poll; log
  ERROR "TLS certificate content changed in the control-plane snapshot ... a container restart is REQUIRED
  ...", counters `hygress_tls_cert_change_detected_total` / `hygress_tls_cert_requires_restart_total` bump) —
  but pingora reads the listener PEM only at bind time, so a **container restart is REQUIRED** for the new
  certificate to take effect:
  1. Rotate the GPUStack TLS secret (`gpustack-tls-*`; refresh via GPUStack's cert config);
  2. within ~60s observe the ERROR log line and the two counters incrementing (the change was absorbed into
     the snapshot);
  3. restart the hygress container (s6 longrun restart / container restart) — the new cert PEM is picked up
     at the next bind.

## Rollback (DoD 6)
`pack/Dockerfile.hygress` copies the surgery scripts; pristine upstream `gateway/pilot/controller/run`
are preserved under `/etc/s6-overlay/s6-rc.d.dist/` in the image. Rollback = copy them back +
restore supercronic's pilot check + rebuild the image (or keep a `s6-rc.d.dist` bind-mount).

## Verification checklist (real GPUStack container)
- [ ] apiserver (18443) + supercronic + `gateway`(=hygress) up; pilot/controller no-ops idle
- [ ] GPUStack `_wait_for_gateway_ready` passes on `GATEWAY_HTTP_PORT`
- [ ] chat completion routes through hygress; usage rows land (model_id/model_route_id/access_key)
- [ ] `:15020/stats/prometheus` + `:8081/metrics` respond; forbidden ports unused (`ss -ltn`)
