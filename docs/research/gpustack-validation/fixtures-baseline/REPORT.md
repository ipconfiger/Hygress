# GPUStack v2 Baseline Validation (Hygress-replacement A/B — Baseline side)

## Objective
Stand up an **unmodified** GPUStack v2 (server with embedded Higress gateway + one
GPU worker), run a *real* end-to-end chat completion **through the embedded
gateway**, and capture the artifacts needed to compare GPUStack's built-in
gateway behavior against a later **Hygress** replacement:

- the gateway's real **token-usage rows** in the GPUStack DB,
- the real **gateway CRD objects** (WasmPlugins / EnvoyFilter / Ingress / McpBridge)
  that the embedded Higress controller created.

The comparison is about **gateway behavior** (routing, auth, usage accounting,
CRDs) — *not* inference throughput — so the model may serve on CPU (see §4).

## Environment
| Item | Value |
|---|---|
| Host | Ubuntu 24.04 (Aliyun ECS), Docker + nvidia runtime |
| GPU | 1x NVIDIA RTX 4090 (24 GB) |
| GPUStack image | `quay.io/gpustack/gpustack:latest` = **GPUStack v2.2.3** (revision `1cfcbbf`) |
| Layout | `gpustack-server` (embedded Higress gateway :80, API :30080, embedded Postgres :5432) + `gpustack-worker` (owns the GPU) |
| Model | `Qwen/Qwen2.5-0.5B-Instruct-GGUF`, `qwen2.5-0.5b-instruct-q4_k_m.gguf` (~491 MB) |

Network reality that drove the design (measured from the host):
- `docker.io`, `huggingface.co`, `github.com` — **unreachable / throttled**.
- `quay.io`, `ghcr.io` small requests — OK; **large image layers ~65 KB/s** (the
  default `llama.cpp:server-cuda` has a single ~2 GB layer; even the 265 MB CPU
  layer measured **3.9 MB / 60 s**). Unfetchable in reasonable time.
- `hf-mirror.com`, `pypi.org`, `mirrors.aliyun.com` — **reachable and fast**
  (~900 KB/s).

## 1. Deployment (`compose.yaml`)
- Both services: `network_mode: host`, `privileged`, `/var/run/docker.sock`.
- Worker GPU via `deploy.resources.reservations.devices` (nvidia, count: all).
- Worker data dir uses an **identity host bind** `/var/lib/gpustack:/var/lib/gpustack`
  so backend containers bind-mount the host model path; the server uses a separate
  host dir for its DB + embedded gateway state.
- Server `--disable-worker` (server is privileged and would otherwise shadow the
  dedicated worker).

Key env flags (both services):
- `HF_ENDPOINT=https://hf-mirror.com` — model download + scheduler resource-fit.
- `GPUSTACK_SYSTEM_DEFAULT_CONTAINER_REGISTRY=quay.io` — resolves the unprefixed
  `gpustack/runtime:pause` workload helper image to `quay.io/...` (already pulled),
  instead of the unreachable `docker.io`.
- `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT="false"` (worker) — **required**, see §4.

## 2. Model (Custom backend)
Model id=8, `backend: Custom`, `image_name: 127.0.0.1:5000/gpustack-serve/gguf:latest`,
`run_command: -m {{model_path}} --port {{port}} --alias {{model_name}}`,
`enable_model_route: true` (auto-creates the model route).

### 2a. Why a locally-built backend image
GPUStack's Custom backend runs the model container's **ENTRYPOINT** + the rendered
`run_command`. Because the stock llama.cpp images are unfetchable from this host
(§ Environment), the backend image is **built locally** on the GPUStack base:

- `Dockerfile.serve`: `FROM quay.io/gpustack/gpustack:latest` (already local; has
  python3.11 + cmake + 16 cores), `pip install llama-cpp-python==0.3.35` (sdist,
  compiled for CPU from the Aliyun PyPI mirror), `serve.py` (a minimal
  OpenAI-compatible GGUF server), `ENTRYPOINT ["python3","/opt/serve.py"]`.
- `serve.py` implements `/health`, `/v1/models`, `/v1/chat/completions`,
  `/v1/completions` with the Qwen2.5 chat template and returns `usage`
  (prompt/completion tokens). Serves on the port GPUStack assigns; reports the
  model under the `--alias` name so gateway routing matches.

### 2b. Why the image is tagged `127.0.0.1:5000/gpustack-serve/gguf:latest`
Two independent registry pass-throughs must leave a local image untouched:
1. `gpustack.utils.config.apply_registry_override_to_image` — skips images whose
   first path component contains `.` or `:`.
2. the deployer's `adjust_image_with_envs` (uses `parse_image`) — `parse_image`
   treats a **single-slash** first component as a *namespace* (returns
   original_reg=None) and re-prefixes it with the default registry
   (`quay.io/localhost/...`, `quay.io/127.0.0.1/...`) — the failures seen in §4.

A **triple-slash** `[registry]/[namespace]/[repo]:tag` form yields a real
`original_reg`, so *both* pass-throughs return it unchanged. The deployer's default
pull policy is `IfNotPresent`, and it checks the **local dockerd store first**
(`images.get`), so the locally-tagged image is used with **no pull**.

## 3. What actually ran (verification)
- Instance `qwen2.5-0.5b-instruct` (model id=8) reached **`state=running`**,
  backend container `...-run-0` healthy on the assigned port (40027).
- **Real chat completion through the gateway** `POST :80/v1/chat/completions`
  (API key `baseline`), e.g. `"Say hello…"` → `"Hello! It's nice to meet you!"`,
  `usage: {prompt_tokens: 50, completion_tokens: 9, total_tokens: 59}`; and a
  follow-up `"What is 2+2?"` → `"4"`, `usage: {35/1/36}`.
- **Usage recorded in the GPUStack DB** (the gateway's token-usage plugin wrote it):

  `model_usage_details` (per-request):
  ```
  id=1 user=admin model=8 model_route=6
  api_key=baseline access_key=905a9ac6a66554e2
  prompt_tokens=50 completion_tokens=9 cached=0 completed=t
  started_at=08:07:46.041  completed_at=08:07:46.302  (≈261 ms)
  ```
  `model_usages` (daily): `prompt_tokens=50 completion_tokens=9 request_count=1`
  for the same model/route/api-key.

- **Real gateway CRDs dumped** (`crds.yaml`, 16 objects, namespace `higress-system`):
  - 8 **WasmPlugin**: `gpustack-ai-proxy`, `gpustack-ai-statistics`,
    `gpustack-header-transformer`, `gpustack-llm-ext-auth`, `gpustack-model-mapper`
    (phase AUTHN), `gpustack-model-router`, `gpustack-set-model-pre-route`,
    `gpustack-token-usage` — each `url=http://127.0.0.1:30080/wasm-plugins/<name>/<ver>/plugin.wasm`.
  - 1 **EnvoyFilter** (`higress-gateway-global-custom-response`, 1 patch).
  - 2 **Ingress**, 1 **McpBridge** (`networking.higress.io/v1`),
    3 **ConfigMap** (higress-config / higress-https / higress-ca-root-cert), 1 **Service**.

  `gpustack-token-usage` (v1.1.1) is the plugin that produced the usage rows above;
  `gpustack-model-mapper`/`-router` implement the model→instance routing.

## 4. Blockers hit and how they were resolved
1. **Unprefixed `gpustack/runtime:pause` pulled from `docker.io` (unreachable).**
   → `GPUSTACK_SYSTEM_DEFAULT_CONTAINER_REGISTRY=quay.io` (official mirror, pre-pulled).
2. **Backend image unfetchable** (ghcr large-layer throttle ~65 KB/s).
   → Built a local CPU GGUF server image on the GPUStack base from PyPI (reachable);
     the A/B compares the *gateway*, so CPU inference is valid.
3. **Local image got an unwanted `quay.io/` prefix → 401** (`apply_registry_override`
   and the deployer `adjust_image_with_envs` both fire).
   → Triple-slash local tag `127.0.0.1:5000/gpustack-serve/gguf:latest` (see §2b);
     default `IfNotPresent` policy resolves it from the local store.
4. **Mirrored deployment (default true) hid the host-side GGUF**: the instance
   container got a separate empty `/var/lib/gpustack` *volume* instead of a host
   bind, so `{{model_path}}` did not exist in the container ("Model path does not
   exist").
   → `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT="false"` forces
   `ContainerMount(path=…)` → host **bind** of the model dir (verified:
   `Type=bind, Source=Target=/var/lib/gpustack/cache/…`).

## 5. Reproduction
```bash
# on the host, from /root/gpustack-validation  (compose + scripts + .env already there)
./bootstrap.sh                 # full flow: compose up + build image + create model
                               #   + wait instance + chat via :80 + dump usage + dump CRDs
SKIP_UP=1 ./bootstrap.sh       # containers already up; re-run the flow only
```
Idempotent. Steps are individually runnable: `./bootstrap.sh step_chat`, etc.

## 6. Artifacts (`fixtures/`)
| File | Contents |
|---|---|
| `chat_response.json` | gateway chat completion (JSON, with `usage`) |
| `usage_row.txt` | `model_usage_details` + `model_usages` rows |
| `crds.yaml` | 16 real Higress CRD objects (multi-doc YAML) |
| `api_key.txt` | the `baseline` OpenAI API key |
| `openapi.json` | full management/OpenAI API surface |
| `model_instances.json`, `workers.json`, `containers.txt`, `ports.txt` | host/container evidence |

## 7. Notes for the Hygress side of the A/B
- The behavior to reproduce/compare is: **(a)** a `/v1/chat/completions` request to
  the gateway's `:80` with a **per-user API key**, routed to exactly one instance
  by the **model route**; **(b)** token usage written per request
  (`prompt_tokens`/`completion_tokens` via the response `usage`); **(c)** the set of
  gateway objects (WasmPlugins `model-mapper`/`model-router`/`token-usage`/
  `header-transformer`/`llm-ext-auth`, EnvoyFilter, Ingress, McpBridge) that make
  routing + usage accounting work.
- The baseline gateway is **Higress** (embedded). The baseline image/version is
  `quay.io/gpustack/gpustack:latest` (v2.2.3, rev `1cfcbbf`).
