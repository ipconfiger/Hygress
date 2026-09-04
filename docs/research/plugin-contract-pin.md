# GPUStack Higress Plugin Wire-Contract Pin

**Purpose.** Pin the exact wire/data-plane contract of the 9 Higress Wasm plugins that GPUStack
deploys, so the Rust `hygress` implementation (a) emits control-plane CRDs byte-compatible with
what GPUStack reconciles, and (b) reproduces the data-plane semantics (headers, usage push,
forward-auth) that the GPUStack server consumes. This file is the source of truth for the unit-test
fixtures and e2e wire assertions in the Rust implementation.

**Sources (all paths absolute).**
- Control plane (authoritative for CRD phases/priorities/config):
  - `GPUS=/home/alex/Projects/GPUStack/gpustack/gpustack`
  - `GPUS/gateway/__init__.py` (plugin specs + priorities), `GPUS/gateway/utils.py` (header/ingress/registry
    naming), `GPUS/gateway/plugins.py` + extracted `manifest.json` (versions), `GPUS/server/controllers.py`
    (per-route matchRules + ingress building).
- Server wire contract (authoritative for what the server parses):
  - `GPUS/server/metrics_collector.py` (`ModelUsageMetrics`, `_validate_usage_metric`),
    `GPUS/schemas/model_usage.py` (`OperationEnum`), `GPUS/routes/gateway_metrics.py`,
    `GPUS/routes/token.py` (forward-auth response), `GPUS/routes/routes.py` (router mounts),
    `GPUS/api/auth.py` + `GPUS/config/config.py` (auth headers + derived token).
- Plugin binaries (L0 string extraction from the `gpustack-higress-plugins` wheel, v0.2.3.post5):
  - Wasm: `/tmp/opencode/higress_plugins/extracted/gpustack_higress_plugins/plugins/<name>/<ver>/plugin.wasm`
  - Raw string dumps: `/tmp/opencode/higress_plugins/notes/<name>.strings.txt` and
    `/tmp/opencode/wasm_strings/{tu,gr,sh,ea,mm,tr,ap,as,rl}_raw.txt`.
  - Go symbol table (struct defs) extracted from the `.data`/`.rodata` sections; log-message format
    strings recovered from the packed string blocks.

**Evidence conventions.** `[py]` = from a Python source file (line cited). `[wasm:<plugin>]` = a string
recovered from that plugin's Wasm binary. Each claim is tagged with its evidence.

---

## 0. Method

1. Extracted the 9 plugin Wasm binaries (Go→wasm) from the `gpustack-higress-plugins==0.2.3.post5`
   wheel; pulled printable string blocks from the data/rodata sections (Go symbol table + Go string
   literals + log format strings).
2. Read the exact Go `struct { ... }` definitions embedded in the binaries (they survive as
   `main.<Field> <type>` symbol strings) to reconstruct each plugin's config schema.
3. Read the GPUStack control plane (`gateway/__init__.py` etc.) to get the authoritative
   `WasmPluginSpec` (phase, priority, defaultConfig, `url` name/version, `failStrategy`).
4. Read the GPUStack server to get the authoritative *consumer-side* wire contract
   (`ModelUsageMetrics` fields, `OperationEnum`, `X-Mse-Consumer` shape, `/v2/usage/gateway-metrics`,
   `/token-auth`), then cross-checked what the plugins actually emit.
5. Cross-checked the plugin priority/phase ordering against `gateway/__init__.py` and
   `server/controllers.py` (controllers contribute per-route `matchRules`, never plugin priorities —
   all priorities live in `__init__.py`).

**Scope.** MVP server-role, `gateway_mode=embedded|external`. Worker-role asymmetry
(`transformer` + `token-usage` only created when `server_role() != WORKER` [py:__init__:846-848]) is
noted per-plugin.

---

## 1. The 9 plugins (authoritative inventory)

`[manifest.json]` lists 9 plugins; `gateway/__init__.py::initialize_gateway` deploys **8** of them.
`gpustack-rate-limit` is shipped in the wheel but **never created** by GPUStack (not in
`plugin_list` [py:__init__:838-848]); it is an available-but-off slot (the generic-router docstring
references a "rate-limit ~600" slot [py:__init__:556]).

| # | Plugin (wasm image name) | WasmPlugin resource name | Version | Phase | Priority | Roles |
|---|---|---|---|---|---|---|
| 1 | `ext-auth` | `gpustack-llm-ext-auth` | 2.0.0 | AUTHN | 360 | server+worker |
| 2 | `ai-statistics` | `gpustack-ai-statistics` | 2.0.0 | UNSPECIFIED_PHASE | 900 | server+worker |
| 3 | `gpustack-generic-proxy-router` | `gpustack-model-router` | 1.0.0 | AUTHN | 900 | server+worker |
| 4 | `ai-proxy` | `gpustack-ai-proxy` | 2.0.0 | UNSPECIFIED_PHASE | 100 | server+worker |
| 5 | `gpustack-set-header-pre-route` | `gpustack-set-model-pre-route` | 1.0.0 | AUTHN | 90 | server+worker |
| 6 | `gpustack-model-mapper` | `gpustack-model-mapper` | 1.0.0 | AUTHN | 800 | server+worker |
| 7 | `transformer` | `gpustack-header-transformer` | 2.0.0 | AUTHN | 810 | server only |
| 8 | `gpustack-token-usage` | `gpustack-token-usage` | 1.1.0 | UNSPECIFIED_PHASE | 400 | server only |
| 9 | `gpustack-rate-limit` | *(not created)* | 1.0.0 | — | — | off |

All resource names/phases/priorities/versions are from [py:__init__] (`ext_auth_plugin` 294-376,
`ai_statistics_plugin` 379-404, `generic_proxy_router_plugin` 539-615, `ai_proxy_plugin` 645-659,
`model_pre_route_plugin` 407-428, `model_mapper_plugin` 431-442, `transformer_plugin` 471-536,
`token_usage_plugin` 618-642) and [manifest.json]. URLs are
`http://<plugin_server>/<prefix>/<name>/<version>/plugin.wasm` [py:plugins.py:18-21,41-49].
All plugins: `failStrategy="FAIL_OPEN"`, `defaultConfigDisable=False`, `matchRules=[]` at init.

> Note on the numeric ordering: the numbers are the *Wasm filter position* in Envoy. The Rust
> terminate-mode data plane does **not** copy the numbers — it implements the *net-semantic* pipeline
> in §4.2. The numbers matter only for CRD compatibility (what a fixture would contain).

---

## 2. Per-plugin contract

### 2.1 ext-auth (`gpustack-llm-ext-auth`) — forward-auth

**Phase/priority/fail:** AUTHN / 360 / `FAIL_OPEN` [py:__init__:371-375].

**defaultConfig** [py:__init__:358-375]:
```jsonc
{
  "_rules_": [{
    "_match_route_prefix_": ["<NS>/ai-route-route-"],     // NS = cfg.get_namespace() + "/" ONLY if ns != gateway_namespace, else ["ai-route-route-"]
    "http_service": {
      "authorization_request": {
        "allowed_headers": [
          {"exact":"X-Real-IP"},{"exact":"X-Forwarded-For"},{"exact":"x-higress-llm-model"},
          {"exact":"x-api-key"},{"exact":"cookie"},
          {"exact":"x-gpustack-auth-cache"},                // AUTH_CACHE_HEADER
          {"exact":"X-GPUStack-Auth-Token"}                 // GATEWAY_AUTH_TOKEN_HEADER
        ],
        "headers_to_add": { "X-GPUStack-Auth-Token": "<derived_gateway_token>" }   // hex(HMAC-SHA256(jwt_secret_key,"gateway-metrics-push"))
      },
      "authorization_response": {
        "allowed_upstream_headers": [
          {"exact":"X-Mse-Consumer"},{"exact":"Authorization"},
          {"exact":"cookie"},{"exact":"x-gpustack-auth-cache"}
        ]
      },
      "endpoint": { "path":"/token-auth", "request_method":"GET",
                    "service_name":"<registry.get_service_name()>", "service_port":<registry.port> },
      "endpoint_mode": "forward_auth",
      "timeout": 30000   // HIGRESS_EXT_AUTH_TIMEOUT_MS default [py:envs/__init__:37]
    },
    "match_type":"blacklist",
    "match_list":[{"match_rule_path":"/","match_rule_type":"prefix"}]
  }]
}
```

**Scope (AUTHN gate) — pin, do not approximate.** Auth is gated by **Envoy route-name prefix**, not
path. `_match_route_prefix_` = `ai-route-route-` (optionally `<ns>/ai-route-route-` cross-namespace)
[py:test_gateway_plugins.py:137-152]. Rationale [py:__init__:336-356]: matching on route name, not
path, is what prevents a `FAIL_OPEN` security hole (path-prefix auth would let spoofed requests skip
auth). Mirror ingress `gpustack` and any non-`ai-route-route-` route are **never** authenticated.

**Behavior.** GET `/token-auth` on the GPUStack server registry. Forwards the 7 `allowed_headers`,
injects `X-GPUStack-Auth-Token=<derived>`. Writes the 4 `allowed_upstream_headers` back onto the
request: `X-Mse-Consumer`, `Authorization` (= `Bearer <registration_token>`), `cookie`,
`x-gpustack-auth-cache` (5-min JWT) [py:routes/token.py:140-148]. `FAIL_OPEN`: transport error →
pass-through (no verdict).

**Forward-auth wire (server response → what Hygress must reproduce),** [py:routes/token.py:137-148]:
- `X-Mse-Consumer`: `access_key.gpustack-<user.id>` (both parts dot-joined) when an API key is
  present; **literal `"none"`** for public-policy / no-key authentications
  [py:routes/token.py:79-95,146].
- `Authorization`: `Bearer <registration_token>`.
- `x-gpustack-auth-cache`: a JWT (auth cache, TTL 5 min) [py:routes/token.py:140,148].
- `cookie`: dummy cookie [py:routes/token.py (dummy cookie)].

**AMBIGUOUS → RESOLVED.** `realIPHeader` is **NOT** an ext-auth config key. Ext-auth forwards
`X-Real-IP`/`X-Forwarded-For` via `allowed_headers` (see above); it has no `realIPHeader` field.
`realIPHeader` is a **token-usage** config (see §2.8). Ext-auth Wasm config keys
`[wasm:ext-auth]`: `endpoint`, `endpoint_mode` (`forward_auth`|`envoy`), `path`, `path_prefix`,
`request_method`, `service_name`, `service_port`, `service_host`, `allowed_headers`,
`status_on_error`, `match_rule_type`, `match_rule_path`; log-format `"outbound|%d||%s"` (the
auth-call result tag: `|<status>||<route>`).

### 2.2 ai-statistics (`gpustack-ai-statistics`)

**Phase/priority:** UNSPECIFIED_PHASE / 900 / `FAIL_OPEN` [py:__init__:395-403].

**defaultConfig** [py:__init__:382-393]:
```jsonc
{
  "enable_content_types": ["application/json","text/event-stream"],  // GATEWAY_AI_STATISTICS_PLUGIN_CONTENT_TYPES default [py:envs/__init__:181]
  "attributes": [
    { "apply_to_log": true, "apply_to_span": false,
      "key": "consumer", "value": "x-mse-consumer", "value_source": "request_header" }
  ]
}
```
**Behavior.** Shallow statistics/logging (content-type gated); copies the `x-mse-consumer` request
header into a `consumer` log attribute. Shallow-compatible in Hygress (MVP = record + metric).

### 2.3 generic-proxy-router (`gpustack-model-router`) — the model resolver

**Phase/priority:** AUTHN / 900 / `FAIL_OPEN` [py:__init__:598-614]. Resource name reuses the legacy
`gpustack-model-router` slot (in-place swap of Higress's built-in model-router) [py:__init__:591-596].

**defaultConfig** (init) [py:__init__:599-604]:
```jsonc
{
  "prefix": "/model/proxy/",
  "targetHeader": "x-higress-llm-model",
  "enableOnPathSuffix": [/* supported_openai_routes + supported_anthropic_routes; see §6.3 */],
  "aliasNameMapping": { "1": "route-one", "2": "route-two" }  // keys = str(route_id); hot-updated per route
}
```
`aliasNameMapping` and `maxBodyBytes` are the **only** keys preserved across init diffs
(`_GENERIC_PROXY_ROUTER_PRESERVED_KEYS`) [py:__init__:788-816]; the router reconciler only mutates
`aliasNameMapping` [py:utils.py:1397-1446]. `defaultConfigDisable` is **never** flipped
[py:utils.py:1410-1413].

**Full config struct** `[wasm:generic-proxy-router]` (Go symbol `main.*`):
```
prefix          string
targetHeader    string
modelKey        string            // JSON body key for the model field (default "model")
modelToHeader   string
addProviderHeader string
enableOnPathSuffix []string
aliasNameMapping map[string]string
autoRoutingEnabled bool
autoRoutingDefault string         // = defaultModel
autoRoutingRules []{ pattern, model }   // pattern (regex) matched against last user message
maxBodyBytes    int               // "maxBodyBytes must be a positive integer"; Sets body buffer limit
```

**Mode-decision tree (evidence = Wasm log format strings, `[wasm:generic-proxy-router]`, all `%s:`-prefixed):**
Key messages recovered:
- `path %q does not match enableOnPathSuffix (%d entries); passing through`
- `path-driven HIT id=%q`
- `path-driven miss id=%q not in aliasNameMapping (size=%d); falling through to body-driven`
- `body-driven activated: mediaType=%q hasBody=%v prefix=%q aliasCount=%d`
- `body-driven JSON model rewritten: %q` / `body-driven JSON no rewrite needed (resolved=%q == original)`
- `body-driven JSON skipped`
- `body callback fired without body-driven arming; len=%d`
- `header set %s=%q` / `multipart field %q rewritten: %q` / `emitting body unchanged`
- `autoRouting matched rule, last user message=%q` / `autoRouting fell back to defaultModel=%q` /
  `autoRouting hit but no rule matched and no defaultModel configured`
- `failed to compile autoRouting pattern %q: %v` / `skipping invalid autoRouting rule: pattern=%q model=%q`

Resolved decision logic (by `:path`, decided in the request-headers phase; body read only if armed):
1. **`<prefix>` prefix (`/model/proxy/`) → PATH-DRIVEN (alias) mode.** id = first path segment after
   the prefix.
   - id ∈ `aliasNameMapping` → **HIT**: `model = aliasNameMapping[id]`; set `targetHeader`, and
     rewrite the request body's `model` field to that value (JSON or multipart). (`path-driven HIT id=%q`)
   - id ∉ `aliasNameMapping` → **fall through to body-driven** mode. (`path-driven miss id=%q … falling through to body-driven`)
2. **`enableOnPathSuffix` match (and no prefix HIT) → BODY-DRIVEN mode** (armed in headers phase;
   body read in body phase). (`body-driven activated: mediaType=… hasBody=…`)
   - Reads the model from the body (JSON `modelKey` field, or the `model` part in multipart).
   - If `autoRoutingEnabled`: apply `autoRoutingRules` (regex vs. the **last user message** in the
     body). Match → `rule.model`; no match → `autoRoutingDefault`; none configured → skip.
   - Else: use the body's model value. Set `targetHeader` to the resolved model.
   - `body callback fired without body-driven arming` = body phase ran but headers phase did not arm
     it (i.e. path matched neither `prefix`-miss nor `enableOnPathSuffix`) → body not processed.
3. **Neither `prefix` nor `enableOnPathSuffix` → pass through** (no body read, no header write).

**Trigger answer (the AMBIGUOUS "read body vs use header"):**
- The body is read **iff** body-driven mode is **armed** in the headers phase, i.e. `:path` ∈
  `enableOnPathSuffix`, OR `:path` starts with `prefix` **and** the alias id was missed.
- The **path alias** (not a request header) is the model source in the prefix-HIT case; the plugin
  *writes* the body's `model` field to the alias.
- The `targetHeader` (`x-higress-llm-model`) is always **set** by this plugin to the resolved model
  (`header set %s=%q`). It is derived from **body** (body-driven) or **path alias** (prefix-HIT) or
  **autoRouting** — the plugin does **not** consume a client-supplied `x-higress-llm-model` as its
  source; it overwrites it with the resolved value. This fixes the design's open "overwrite vs
  preserve" question: **the plugin overwrites** `x-higress-llm-model` with the body/alias-derived
  model. (Consequence: a client cannot spoof the routed model by pre-setting the header — the resolved
  value wins; the spoof surface is therefore governed by the body/path, consistent with
  design §6.1②/§7 item 2.)

### 2.4 ai-proxy (`gpustack-ai-proxy`)

**Phase/priority:** UNSPECIFIED_PHASE / 100 / `FAIL_OPEN` [py:__init__:647-658]. **`create_only`** at
init (only `url`/`sha256` refreshed; `providers[]`/`matchRules[]` are hot-updated per route/provider
event [py:__init__:864-873, utils.py:1296-1336,1516-1534; controllers.py:943-1010,2843-2856]).

**defaultConfig (init):** `{}`; the controllers then append per-provider:
```jsonc
// defaultConfig.providers[] entry  [py:utils.py:425-446]
{ "id":"provider-<id>", "apiTokens":[...], "type":"openai"|..., 
  "failover":{"enabled":true,"healthCheckModel":"<llm model>"}  // only if >1 token
}
// matchRules[] entry  [py:utils.py:447-456, controllers.py:977-998]
{ "config":{"activeProviderId":"provider-<id>"},
  "service":["<registry.get_service_name()>"],      // name.type (no port)
  "ingress":["<ns>/ai-route-route-<id>.internal", "<ns>/ai-route-route-<id>.fallback.internal"],
  "configDisable":false }
```
**Behavior.** OpenAI-compatible provider proxy: path rewrite, `apiTokens` key-swap into outbound
`Authorization`, provider `type` dispatch, failover. **v1 = OpenAI subset + Anthropic passthrough;
non-OpenAI provider types → graceful passthrough** (no hard error) [design §3 D8, §7].

### 2.5 set-header-pre-route (`gpustack-set-model-pre-route`)

**Phase/priority:** AUTHN / 90 / `FAIL_OPEN` [py:__init__:411-427].

**defaultConfig** [py:__init__:412-417]:
```jsonc
{
  "clusterNameHeader": "X-GPUStack-Model-Instance",   // = router_header_key [py:utils.py:77]
  "routeNameHeader":   "X-GPUStack-Route-Name",
  "enableOnPathSuffix": [/* openai + anthropic routes */],
  "enableOnPathPrefix": ["/model/proxy"]
}
```
**Full config struct** `[wasm:set-header-pre-route]` (Go symbol): `routeNameHeader string;
clusterNameHeader string; enableOnPathSuffix []string; enableOnPathPrefix []string`.
Config-validation message: `one of the routeNameHeader or clusterNameHeader should be configured`.

**Behavior (the `xxxx|xx||xxx` pin).** At the request phase (pre-route), for an enabled path, the
plugin reads the **selected Envoy upstream cluster name** and:
- validates it against the format **`xxxx|xx||xxx`** — i.e. four `|`-separated fields with the third
  empty. Recovered Wasm string: `the cluster_name is not in the right format, expected format
  xxxx|xx||xxx` `[wasm:set-header-pre-route]`. Mapped to the McpBridge registry fields this is
  **`<name>|<type>||<port>`** (e.g. `model-5-12|static||80`; see §5.3 for the registry name grammar).
- writes `clusterNameHeader` (`X-GPUStack-Model-Instance`) = the instance identity derived from that
  cluster name. **This value must be parseable by the server/egress `get_instance_id_from_header`**
  whose regex is `^model-\d+-(\d+)(?:-[^.]+)?\..+` [py:utils.py:1560] — i.e. `model-<model_id>-
  <instance_id>[-<alias>].<type>[:port]`. The same grammar appears as a Wasm cluster matcher
  `^model-\d+-\d+(\.|$)` (and `^provider-\d+(\.|$)`, `^gpustack(-|\.|$)`) in the token-usage binary
  `[wasm:token-usage]`.
- writes `routeNameHeader` (`X-GPUStack-Route-Name`) = the **matched Envoy route name**, which for a
  model-route ingress is `<ns>/ai-route-route-<id>.internal` (same identifier ext-auth matches on and
  the token-usage parses, §2.8).

**Weighted model-route ingress — resolution (Q: "which Envoy cluster name is visible at request
phase").** For an ingress with multiple weighted destinations (`higress.io/destination:
"33% model-5-12.static:80\n34% model-5-13.static:80\n33% …"`, Hamilton-weighted [py:utils.py:870-875]),
Higress resolves the weighted cluster to a **specific member** at route/cluster-selection time. The
`xxxx|xx||xxx` (single-`<model-..-..>`) validation in the set-header plugin **requires** the cluster
name to be a single instance (not an aggregate), and `get_instance_id_from_header` likewise requires
a concrete instance id. **∴ the visible cluster name at request phase is the selected member's
`model-<mid>-<iid>|<type>||<port>`, not a weighted aggregate.** *(Residual AMBIGUOUS: the exact
Higress-internal step at which the weighted cluster collapses to a member is not visible from the
binary; but the net, observable effect — `X-GPUStack-Model-Instance` always carries a concrete
instance — is pinned.)* For the **Rust** impl this is moot: Hygress does its own SWRR selection in the
terminate-mode pipeline, so it *already knows* the chosen instance and sets `X-GPUStack-Model-Instance`
directly (no need to read an Envoy cluster name).

### 2.6 model-mapper (`gpustack-model-mapper`)

**Phase/priority:** AUTHN / 800 / `FAIL_OPEN` [py:__init__:432-442]. **`create_only`** at init;
`matchRules` are hot-updated per route event [py:__init__:864-873].

**defaultConfig (init):** `{ "modelMapping": {} }`.
**Per-rule config** [py:utils.py:1141-1173]:
```jsonc
{
  "config": { "modelMapping": { "<route_name>": "<effective_model_name>" } },
  "ingress": [ "<ns>/ai-route-route-<id>.internal" ]            // + fallback name for fallback rules
  "service": ["<service_name>", ...],                            // name.type (NO port)  [py:utils.py:1157,design §6.2]
  "configDisable": false
}
```
**Behavior.** Per-destination outgoing body `model` rewrite keyed by **service name `name.type`**
(no port — matches the McpBridge registry key form, distinct from the `name.type:port` destination
annotation [py:design §6.2, __init__ comment]). Used to hit LoRA-aliased registries
(`model-<mid>-<iid>-l<sha256[:8]>`, 8-hex suffix [py:utils.py:239-249]) and renamed-provider
destinations. Key form is **fixed and non-mixable**: matchRule `service` = `name.type`;
`higress.io/destination` = `name.type:port` [design §6.2].

### 2.7 transformer (`gpustack-header-transformer`) — **server only**

**Phase/priority:** AUTHN / 810 / `FAIL_OPEN` [py:__init__:526-535]. Created **only** when
`server_role() != WORKER` [py:__init__:846-848].

**defaultConfig (`reqRules`, ordered)** [py:__init__:473-524, transform_header 460-468]:
```
1. remove     X-GPUStack-Auth-Token
2. remove     X-GPUStack-Model-Instance
3. rename     x-gpustack-model       -> x-higress-llm-model
4. rename     x-gpustack-fallback-path -> :path
5. dedupe     x-gpustack-model        (RETAIN_FIRST)
6. dedupe     x-higress-llm-model     (RETAIN_FIRST)
7. dedupe     :path                   (RETAIN_LAST)
8. map        :path                   -> x-gpustack-original-path
9. remove     x-gpustack-fallback-path
```
(`transform_header(operate, *rules)` → `{ "headers":[<rule>], "operate":<op> }`; `HeaderRule` fields:
`key/newKey/oldKey/fromKey/toKey/value/newValue/appendValue/value_type/strategy/host_pattern/
path_pattern` [py:__init__:445-468].)

**Behavior / net effect.**
- **Strip untrusted inbound headers FIRST** (rules 1–2): `X-GPUStack-Auth-Token` and
  `X-GPUStack-Model-Instance` are dropped at request entry so a client can't forge the auth token or
  the instance-routing header [design §6.1①].
- Rename client `x-gpustack-model` → `x-higress-llm-model` (RETAIN_FIRST: a pre-existing
  `x-higress-llm-model` is preserved over the renamed one) [design §6.1③].
- Backup the original `:path` → `x-gpustack-original-path` (rule 8) so the fallback path can restore
  it; `x-gpustack-fallback-path` is renamed to `:path` (rule 4) and then removed (rule 9) on normal
  flow [py:utils.py fallback EnvoyFilter 1256-1263].
- Rule engine is **ordered** and per-operation (`remove|rename|replace|add|append|map|dedupe`) with
  strategy `RETAIN_FIRST|RETAIN_LAST|RETAIN_UNIQUE`.

### 2.8 token-usage (`gpustack-token-usage`) — **server only**

**Phase/priority:** UNSPECIFIED_PHASE / 400 / `FAIL_OPEN` [py:__init__:632-641]. Created **only** when
`server_role() != WORKER` [py:__init__:846-848].

**defaultConfig** [py:__init__:622-630]:
```jsonc
{
  "endpoint": { "path":"/v2/usage/gateway-metrics",
                "service_name":"<registry.get_service_name()>", "service_port":<registry.port> },
  "header_add": { "X-GPUStack-Auth-Token": "<derived_gateway_token>" }
}
```

**Full config struct** `[wasm:token-usage]` (Go symbol): `EnableOnPathSuffix []string`
(+ `enableUsageOnPathSuffix`), `Endpoint *main.EndpointConfig`, `HeaderAdd map[string]string`
(key `header_add`), `ReportClient wrapper.HttpClient`, `RealIPHeader string` (key `realIPHeader`),
`ClusterNameMatchers []*regexp.Regexp` (built from key `additionalClusterNameRegexps`),
`OrganizationIDHeader string` (key `organizationIDHeader`, **default `X-Organization-Id`**),
`MaxResponseBodyBytes int` (key `maxResponseBodyBytes`).

**Behavior.** At response completion, for tracked model-route requests (upstream `cluster_name`
matches `ClusterNameMatchers`), it scans OpenAI/Anthropic/Gemini usage (SSE + non-stream;
`stream_options.include_usage` forced on), then POSTs `/v2/usage/gateway-metrics` with
`X-GPUStack-Auth-Token=<derived>` (HMAC-SHA256 hex of `jwt_secret_key` over `"gateway-metrics-push"`
[py:config.py:359-361]). Wasm log lines: `reportMetrics: reported for route %s, status=%d`,
`reportMetrics: dispatch failed for route %s: %v`, `reportMetrics: cluster %s not tracked`,
`reportMetrics: cluster_name %s does not match expected pattern, skipping`,
`reportMetrics: no cluster_name, skipping`, `reportMetrics: no base metrics, skipping`,
`onStreamingResponseBody: token usage: total=%d, output=%d` `[wasm:token-usage]`.

**EXACT wire schema (the `ModelUsageMetrics` JSON the plugin emits) — pin.** Recovered from the Go
`json:"…"` tags in the token-usage binary `[wasm:token-usage]` (**17 fields**):

Required (always sent):
| field | Go type / meaning |
|---|---|
| `model` | string — request model (the routed/effective model name, e.g. a LoRA route name) |
| `input_token` | int |
| `output_token` | int |
| `total_token` | int |
| `input_cached_token` | int |
| `request_count` | int (default 1) |
| `completed` | bool — true iff a canonical usage chunk was observed before stream end |
| `output_chunk_count` | int |
| `request_content_bytes` | int |
| `started_at` | int — UnixMilli (0/absent ⇒ treated as None server-side [py:metrics_collector:100-110]) |
| `completed_at` | int — UnixMilli |

Optional (`omitempty` — sent only when non-zero/non-empty):
| field | meaning |
|---|---|
| `user_id` | int |
| `model_id` | int |
| `model_route_id` | int |
| `provider_id` | int |
| `access_key` | string |
| `organization_id` | string (source header `X-Organization-Id`) |

**NOT present in the wire payload** (server fields the plugin does *not* send): `operation`,
`cluster_id`, `provider_name`, `provider_type`. *(This corrects design §2.1.3/§15-Q1, which assumed
the payload "must include operation/cluster_id/provider_name/type" — the binary proves it does not.)*

**Attribution (how the plugin derives the omitempty fields):**
- `model_route_id` — parsed from `X-GPUStack-Route-Name` (value `<ns>/ai-route-route-<id>.internal`);
  the binary contains the `ai-route-route-` literal and a `gpustack-route_name` context key
  `[wasm:token-usage]`.
- `user_id`, `access_key` — parsed from `X-Mse-Consumer` = `access_key.gpustack-<user_id>`
  (or `"none"` for no-key) [py:routes/token.py:79-95].
- `organization_id` — from `X-Organization-Id` [py:metrics_collector:95-97].

**`realIPHeader` (RESOLVED).** A **token-usage** config key (not ext-auth). The plugin, on the
outbound report POST, calls `writeRealIPHeader` (sets `realIPHeader` to the client's source IP;
Wasm lines `writeRealIPHeader: failed to get source address: %v`,
`writeRealIPHeader: failed to replace header %s: %v`) and `injectTrustHeaders` (carries the upstream
`cluster_name` as a trust header; `injectTrustHeaders: cluster_name unavailable: %v`).
`[wasm:token-usage]`
**AMBIGUOUS (minor, low impact):** the **default value** of `realIPHeader` is not a recoverable
standalone literal in the binary (no `X-Real-IP` string present in the token-usage Wasm). It only
affects an HTTP header on the metrics POST, **not** the `ModelUsageMetrics` body → no effect on the
usage row landing (the server `report_gateway_metrics` reads only the auth header + body
[py:routes/gateway_metrics.py:16-21]). For wire-equivalence Hygress *may* omit it; if reproduced,
use `X-Real-IP` (the Envoy/Higress convention) and treat it as client-IP only.

### 2.9 rate-limit (`gpustack-rate-limit`)

Shipped in the wheel (v1.0.0) but **not created** by GPUStack [py:__init__:838-848 — absent from
`plugin_list`]. Informational only; not part of the deployed contract. *(The generic-router docstring
notes a "rate-limit ~600" slot [py:__init__:556], but no rate-limit WasmPlugin is emitted.)*

---

## 3. Header / field register

### 3.1 Routing / attribution headers (data plane)

| Header | Set by | Value / grammar | Consumed by | Evidence |
|---|---|---|---|---|
| `x-higress-llm-model` | generic-proxy-router `targetHeader`; transformer renames `x-gpustack-model`→this; **ingress `higress.io/exact-match-header-x-higress-llm-model`** matches on it | the **route (effective) name** of the matched ingress | router match (CRD), ext-auth (forwarded), token-usage | py:__init__:403,487-490; py:utils.py:629; py:utils.py:660-676 |
| `X-GPUStack-Model-Instance` (=`router_header_key`) | set-header-pre-route `clusterNameHeader` | `model-<mid>-<iid>[-<alias>].<type>[:port]` (must match `^model-\d+-(\d+)(?:-[^.]+)?\..+`) | server `get_instance_id_from_header`; fallback/egress | py:utils.py:77,1537-1566 |
| `X-GPUStack-Route-Name` | set-header-pre-route `routeNameHeader` | `<ns>/ai-route-route-<id>.internal` (or no `ns/` same-namespace) | token-usage (`model_route_id`); **no server-side consumer** | py:__init__:414; py:utils.py:210-218; wasm:token-usage |
| `x-gpustack-original-path` | transformer `map :path→` this | original pre-rewrite `:path` | fallback restore | py:utils.py:78; py:__init__:511-517 |
| `x-gpustack-fallback-path` | fallback EnvoyFilter (`%REQ(X_GPUSTACK_ORIGINAL_PATH)%`) | original path for the fallback hop | transformer `rename→:path` | py:utils.py:79; py:utils.py:1256-1263 |
| `x-higress-fallback-from` | fallback ingress annotation (exact-match-header) | the main ingress name | router (fallback route) | py:utils.py:1092 (design §2.1.2) |
| `X-Mse-Consumer` | GPUStack server (forward-auth response) | `access_key.gpustack-<user.id>` or `none` | ext-auth (write-back), token-usage (attribution) | py:routes/token.py:84-95 |
| `Authorization` | forward-auth response | `Bearer <registration_token>` | ext-auth write-back; ai-proxy key-swap | py:routes/token.py:147 |
| `x-gpustack-auth-cache` (=`AUTH_CACHE_HEADER`) | forward-auth response | 5-min JWT | ext-auth write-back + forward next time | py:security.py:92 |
| `X-Real-IP`, `X-Forwarded-For` | client / LB | real client IP | ext-auth `allowed_headers` (forwarded) | py:__init__:307-308 |
| `X-Organization-Id` | client | org id (string) | token-usage `organization_id` | py:metrics_collector:95-97 |

### 3.2 `higress.io/*` ingress annotations (control plane)

[py:utils.py:609-677 `generate_model_ingress`; py:utils.py:91-92 mirror; design §2.1.2]
- `higress.io/destination`: `\n`-joined weighted list `<pct>% <name.type:port>` (Hamilton weights);
  **mirror ingress** targets have **no** `pct%` prefix (parser must accept both).
- `higress.io/rewrite-target`: `/$1$3`.
- `higress.io/ignore-path-case`: `true` (model) / `false` (mirror).
- `higress.io/proxy-next-upstream`: `error,timeout,http_503,http_502,non_idempotent`.
- `higress.io/proxy-next-upstream-tries`: `2`.
- `higress.io/exact-match-header-x-higress-llm-model`: `<route_name>` — **core model-route match**.
- `higress.io/exact-match-header-x-higress-fallback-from`: `<main_ingress_name>` (fallback ingress).

---

## 4. Global ordering, ingress naming, registry/cluster naming

### 4.1 Control-plane CRD priority/phase table (authoritative — from `gateway/__init__.py`; cross-checked vs `server/controllers.py`, which adds per-route `matchRules` but **no** plugin priorities)

Same-phase rule: **higher priority runs earlier**. Net cross-phase invariant used by the data plane:
**generic-proxy-router (AUTHN 900) resolves `x-higress-llm-model` before ext-auth (AUTHN 360) runs**
[design §2.1.2].

| order (in-deck) | plugin (resource) | phase | priority | note |
|---|---|---|---|---|
| 1 | set-model-pre-route | AUTHN | 90 | writes instance + route-name headers |
| 2 | ai-proxy | UNSPECIFIED | 100 | provider proxy |
| 3 | ext-auth | AUTHN | 360 | forward-auth, FAIL_OPEN, route-name scoped |
| 4 | token-usage | UNSPECIFIED | 400 | usage push (server only) |
| 5 | model-mapper | AUTHN | 800 | per-service model rewrite (server+worker) |
| 6 | header-transformer | AUTHN | 810 | (server only) |
| 7 | generic-proxy-router | AUTHN | 900 | body/path model resolver |
| 8 | ai-statistics | UNSPECIFIED | 900 | shallow stats |

> The **phase × priority** tuple is the CRD fixture truth. The numbers do **not** equal the
> pipeline order: within AUTHN the *filter* order is 900→810→800→360→90, but several plugins'
> *data-plane effect* lands at a later logical stage (model-mapper/transformer operate on the outbound
> request; set-header after route+SWRR). Hygress therefore implements the **net-semantic pipeline**
> below, not the numeric order.

### 4.2 Net-semantic data-plane pipeline (what Hygress must implement) [design §6.1, refined above]

```
①  strip untrusted inbound  : X-GPUStack-Auth-Token, X-GPUStack-Model-Instance   (transformer rules 1-2)
②  model resolve           : generic-proxy-router (path-alias / body / autoRouting) -> overwrites x-higress-llm-model
③  transformer-in          : rename x-gpustack-model→x-higress-llm-model (RETAIN_FIRST); backup :path→x-gpustack-original-path
④  route match             : x-higress-llm-model (exact) + path predicate -> RouteRule
⑤  ext-auth                : ONLY if route name starts with (ns/)ai-route-route- ; GET /token-auth ; write-back ; FAIL_OPEN ; 30s
⑥  full-body read (cap→413)
⑦  registry resolve        : static|dns|proxy|tunnel -> target group
⑧  SWRR select             : pick concrete instance  (this is what the Wasm set-header would read from the weighted cluster)
⑨  set-header-equivalent   : write X-GPUStack-Model-Instance (selected instance) + X-GPUStack-Route-Name (route name)
⑩  model-mapper            : per-service outbound body model rewrite (LoRA / renamed provider)
⑪  failover loop           : per-route retry (proxy-next-upstream)
    outbound: path rewrite (rewrite-target /$1$3) + key swap + Host
⑫  stream response         : SSE usage scan (OpenAI/Anthropic/Gemini) ; TTFT ; strip server/via
⑬  token-usage             : POST /v2/usage/gateway-metrics (model-route traffic only) ; complete ModelUsageMetrics
⑭  stats + log + prometheus
⑮  4xx/5xx -> fallback      : x-higress-fallback-from guard (max 10) ; x-gpustack-original-path restore
```

### 4.3 Ingress naming

[py:utils.py:72-73,206-218,221-222; py:envs 177]
- Main model-route: `ai-route-route-<id>.internal`
- Fallback: `ai-route-route-<id>.fallback.internal`
- Legacy (cleanup-only, expect never present): `ai-route-model-<id>`
- Mirror (GPUStack self): `gpustack` (env `GATEWAY_MIRROR_INGRESS_NAME`, default `gpustack`), path `/` Prefix
- `service_namespace_prefix` = `"<ns>/"` when `ns != gateway_namespace`, else `""` — applied to route
  names inside `matchRules.ingress`, ext-auth `_match_route_prefix_`, and ai-proxy `matchRules.ingress`
  [py:utils.py:1141-1173,1149; py:__init__:352-356].

### 4.4 Registry / cluster naming (McpBridge `spec.registries[].name`)

[py:utils.py:196-391,233-250; wasm:token-usage matchers]
- Model instance: `model-<model_id>-<instance_id>` (prefix `model-<mid>-` + instance id)
  [py:utils.py:229-237].
- LoRA alias suffix: `-l<sha256(route_name)[:8]>` → `model-<mid>-<iid>-l<hash8>` [py:utils.py:239-249].
- Provider: `provider-<id>`; provider egress proxy: `provider-<id>-proxy` [py:utils.py:356-361].
- Worker (direct/worker-proxy/tunnel): `cluster-<cluster_id>-worker-<worker_id>` [py:utils.py:225-227].
- Cluster gateway: `cluster-gateway` [py:utils.py:344-353].
- GPUStack server itself: `gpustack` (registry `gpustack.<type>:<port>`; embedded = `static` to
  `127.0.0.1:<api_port>` → `gpustack.static:80`; incluster = `dns` `:<api_port>`) [py:design §2.1.1;
  __init__:120-146].
- Service-name forms: **matchRule `service` = `name.type` (no port)**; **destination annotation =
  `name.type:port`** [py:design §6.2]. `static` ⇒ `domain` already `host:port`, `port=80`; `dns` ⇒
  `domain` bare host + real `port`.
- Envoy upstream cluster name (set-header sees): **`name|type||port`** (the `xxxx|xx||xxx` form)
  [wasm:set-header-pre-route].

---

## 5. Wire-level assertions for Hygress e2e

### 5.1 Usage push (`POST /v2/usage/gateway-metrics`)

**Request contract**
- Method/path: `POST /v2/usage/gateway-metrics` (`versioned_prefix="/v2"` + `/usage` mount;
  `@router.post("/gateway-metrics")`) [py:routes/routes.py:69,441; py:routes/gateway_metrics.py:16].
- Auth header: `X-GPUStack-Auth-Token: <derived>`, where `derived = hex(HMAC-SHA256(jwt_secret_key,
  "gateway-metrics-push"))` [py:api/auth.py:53; py:config.py:359-361]. **Exact header name: `X-GPUStack-Auth-Token`.**
- Body = `ModelUsageMetrics` JSON. **Assert these exact JSON field names** (no `operation`,
  `cluster_id`, `provider_name`, `provider_type`):
  - Required: `model`, `input_token`, `output_token`, `total_token`, `input_cached_token`,
    `request_count`, `completed`, `output_chunk_count`, `request_content_bytes`, `started_at`,
    `completed_at`
  - Optional: `user_id`, `model_id`, `model_route_id`, `provider_id`, `access_key`, `organization_id`
- `operation` **enum (server-side only — the gateway does NOT send it).** If/when a value is needed,
  the only valid strings (pydantic `OperationEnum`) [py:schemas/model_usage.py:11-18] are:
  `completion`, `chat_completion`, `embedding`, `rerank`, `image_generation`, `audio_speech`,
  `audit_transcription` (note: the last enum *value* is the string **`audit_transcription`**, whose
  member name is `AUDIO_TRANSCRIPTION` — a GPUStack-side typo; the wire string is `audit_transcription`).
  For Hygress **send `operation` absent** (wire-equivalent to the Wasm plugin).
- Server-side gates that must be satisfied for the row to land [py:metrics_collector:382-432]:
  - not dropped: `model_id is not None or provider_id is not None` (else `_validate_usage_metric`
    returns False and the row is skipped).
  - `model_route_id` non-NULL (else the row lands in the "Untracked" bucket + throttled warning)
    [py:metrics_collector:216,296-311].
  - `model.name == metric.model` **or** (LoRA) `metric.model == route_name[metric.model_route_id]`
    and `route.created_model_id == metric.model_id` [py:metrics_collector:400-417].
- **e2e assertion (design DoD #5):** a real inference through a model-route produces a
  `model_usages` row with **non-NULL `model_route_id`** and correct `access_key`/`consumer`
  attribution — not merely HTTP 200.

### 5.2 Route-name / model-instance header assertions

- After routing a request on a model-route ingress, **`X-GPUStack-Route-Name`** =
  `<ns>/ai-route-route-<id>.internal` (no `ns/` when same namespace).
  *Assert* the value starts with the matched ingress name and the `<id>` equals the route's DB id.
- **`X-GPUStack-Model-Instance`** = the selected instance, matching `^model-\d+-(\d+)(?:-[^.]+)?\..+`
  (LoRA: `model-<mid>-<iid>-l<hash8>.<type>`). For a provider route it is `provider-<id>.<type>`.
  *Assert* the second numeric segment equals the chosen instance id (what the server's
  `get_instance_id_from_header` returns).
- Stripping: inbound `X-GPUStack-Auth-Token` and `X-GPUStack-Model-Instance` set by the client must
  be **removed** before auth/routing (transformer rules 1–2). *Assert* a client-forged
  `X-GPUStack-Model-Instance` does not change the selected instance.

### 5.3 Ext-auth request/response header contract

**Outbound (to `GET /token-auth`)** — Hygress must forward/inject:
- Forward (from request, allowlist): `X-Real-IP`, `X-Forwarded-For`, `x-higress-llm-model`,
  `x-api-key`, `cookie`, `x-gpustack-auth-cache` (the previous hop's cache JWT).
- Inject: `X-GPUStack-Auth-Token` = derived token.
- Path/method: `GET /token-auth`. Scope: only when route name starts with
  `(ns/)ai-route-route-`; `FAIL_OPEN`; timeout 30 s.

**Write-back (auth response → request)** — Hygress must set from the response:
- `X-Mse-Consumer` = `access_key.gpustack-<user.id>` or literal `none` (no-key/public policy).
- `Authorization` = `Bearer <registration_token>`.
- `cookie` (dummy cookie).
- `x-gpustack-auth-cache` = 5-min JWT.

*Assert:* on a valid key, the request forwarded upstream carries `X-Mse-Consumer=sk-…<ak>.gpustack-<uid>`
and `Authorization: Bearer …`; on the public (no-key) policy, `X-Mse-Consumer=none`; on auth-service
unreachable, the request proceeds (FAIL_OPEN) without an auth verdict.

---

## 6. Residual AMBIGUOUS (unresolvable from the available evidence)

1. **`realIPHeader` default value (token-usage, §2.8).** The Go struct field + config key
   `realIPHeader` exist, but no `X-Real-IP`-style literal is recoverable from the binary. Scope is
   narrow: it only names an HTTP header on the metrics POST (client-IP), not the usage body → no
   effect on row landing. **Marked AMBIGUOUS (low impact); recommend omitting in Hygress or using
   `X-Real-IP`.**
2. **Weighted-cluster collapse step (set-header, §2.5).** The *observable* effect —
   `X-GPUStack-Model-Instance` = a concrete instance for every weighted model-route request — is
   pinned (via the `xxxx|xx||xxx` validation + instance-id regex). The exact Higress-internal step
   where the weighted cluster collapses to a member at the request phase is **not** observable from
   the binary. **AMBIGUOUS (net effect resolved; mechanism not).** Moot for Hygress (self-SWRR).
3. **`operation` wire presence (§5.1).** *Resolved negatively*: the binary `json` tags prove the
   token-usage plugin does **not** emit `operation` (only Go-runtime noise matches). The server
   `OperationEnum` values are documented for completeness, but the gateway path leaves it `None`.

Everything else previously flagged (`operation`, `realIPHeader` ownership, generic-proxy body-vs-header
trigger, cluster names, `X-GPUStack-Route-Name` consumer = token-usage) is **resolved with evidence**
above.
