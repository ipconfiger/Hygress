# Hygress — 基于 Pingora 的 GPUStack 内嵌 Higress 原位替换 AI Gateway 设计方案

- 版本：v1.2（根据 oracle 第二轮审核 APPROVED-WITH-MINOR-FIXES 8.5/10 修订）
- 日期：2026-09-02
- 状态：草案（第 3 轮，终审目标 APPROVED）
- 关联源码：
  - 借鉴/复用对象：`/home/alex/Projects/dogress2`（Pingora AI 网关，代码名 Hydra）
  - 替换目标：GPUStack 内嵌 Higress（`/home/alex/Projects/GPUStack/gpustack`，分析基线 v2.2.x / Higress 2.1.9）

**修订记录**

| 版本 | 修订内容 |
|---|---|
| v1.0 | 初稿 |
| v1.1 | 依 oracle v1 审核（NEEDS-REVISION 6.5/10）修订 9 阻塞项+14 非阻塞项：修正 s6 手术方案与 supercronic 依赖；`jwt_secret_key` 取点由「开放问题」改为设计内定案；usage payload 补全 `ModelUsageMetrics` 完整字段与鉴权头；RouteRule 增加逐目的地 `model_mapping`（LoRA/改名 provider）；修正插件相位/执行顺序叙述；ext-auth 改为**路由名**前缀作用域；策略 2 下删除本地 SQLite；拓扑 B 补 IngressClass 播种；WORKER+external 明确范围之外；L2 估算与 deep-dive 对齐 |
| v1.2 | 依 oracle v2 审核（APPROVED-WITH-MINOR-FIXES 8.5/10）修订：model-router 对预置 `x-higress-llm-model` 的覆盖语义从「事实」改为「L0 待核」并进核对清单；端口排除补 15012；s6 手术补 logrotate/access.log 承接与 no-op 用长 sleep 禁 exit 0；`X-GPUStack-Route-Name` 值格式改为 L0 待核；`model_mapping` 键格式区分 `name.type`(matchRule) 与 `name.type:port`(destination)；token-usage 作用域/双计问题并入 Q1；`X-Mse-Consumer` 公共策略哨兵值 'none' 标注 |
| v1.3 | 依 Gate-1 oracle 代码门禁（NEEDS-REVISION 7/10）+ 契约定案修正：§6.2 匹配语义改为 Higress AND 语义（Main 仅按 header+path 全匹配、Fallback 独立索引仅 fallback 时查询、mirror 为唯一 path 兜底）；RouteRule 增加 `ingress_name`/`main_ingress_name` 与 Main/Fallback 键空间隔离；SWRR 改路由目标组共享状态；usage 保留上游 total_token、`operation` 取服务器 OperationEnum；§2.1.3 usage wire 定为恰 17 字段（operation/cluster_id/provider_name/provider_type 不上送），§15 Q1 定案 |
| v1.5 | **真机 A/B 验证完成**（live GPUStack v2.2.3 + RTX4090 worker + qwen2.5-0.5b-instruct，见 `docs/research/gpustack-validation/REPORT.md` §13）：DoD 1/2/5-DB/6 全部 PASS。真机揭示并修复 7 项实契约/运行时缺口：①no-op pilot/controller 补 `notification-fd:3` 就绪信号（s6-rc `change top` 收敛）；②Dockerfile 去 `--chmod`（legacy builder）+ `.dist` 快照先于覆盖（真回滚原件）；③`GPUSTACK_DATA_DIR` 注入（`DATA_DIR`→jwt 解析路径，原 fail-fast）；④rustls `ring` provider + `install_default()`（首 TLS 握手 panic）；⑤McpBridge `default` 无 managed 标签 → 快照无需标签选择器（registry 非空）；⑥forward-auth 转发 `authorization`（wasm ext-auth 行为，AUTHED 模型 401 根因）；⑦`HYGRESS_API_READY_TIMEOUT`/`HYGRESS_SNAPSHOT_TIMEOUT` 可配（300s） |

---

## 1. 背景与目标

GPUStack 默认把 Higress 作为其 **AI Gateway 数据面**（核心路由、鉴权、用量计量），以 4 个进程
（`apiserver` / `pilot` / `controller` / `gateway(envoy)`）通过 s6-overlay 内嵌在 server 容器中运行。
Higress 体系庞大（Istio + Envoy + Wasm 插件链），与 GPUStack 的耦合是**代码级**的：
GPUStack 的 Python 服务通过 `kubernetes_asyncio` 持续把自身状态翻译成一组 CRD（Ingress / McpBridge /
WasmPlugin / EnvoyFilter / Secret / ConfigMap），由 Higress 控制器消费后下发到 Envoy。

**目标**：实现一个基于 [Pingora](https://github.com/cloudflare/pingora)（Rust）的 AI Gateway——**Hygress**，
借鉴 dogress2（Hydra）已验证的生产级架构，在 GPUStack 中 **与内嵌 Higress 原位（in-place）替换**，
且 **不改动 GPUStack 的任何一行 Python 代码**（零改动替换），实现更小的资源占用、更低的延迟、
更强的可观测性与原生多租户能力。

**成功判据（DoD）**
1. GPUStack `gateway_mode=embedded`（默认路径）下，Hygress 替换 higress 三进程后，官方 e2e 全部通过：
   模型 → 实例 → 推理 → 用量 → fallback。
2. GPUStack `tests/gateway/` 既有单测充当**控制面适配器行为基线 / CRD fixture 生成器**（生成录制 CRD
   快照供 Rust 端断言），零失败。
3. 数据面端口（`GATEWAY_HTTP_PORT`、`tls_port`）与 CRD schema 不变。
4. 单二进制，无 Wasm 运行时；插件等价功能全部用原生 Rust 实现。
5. **用量断言**：e2e 必须验证 `/v2/usage/gateway-metrics` 推送在 GPUStack DB 中**落为真实 usage 行**
   （`model_route_id` 非空 + 用户归属正确），而非仅 HTTP 200。
6. 可回滚：s6 镜像层保留三进程脚本（no-op 而非删除），便于对比回归。

> **DoD 状态（v1.5，真机验证完成）**：DoD 1/2/3/4 MET ✅；DoD 5-DB MET ✅
> （`model_usage_details` 新增行 34/7 与 e2e 响应逐位一致）；DoD 6 MET ✅（端口纪律/进程/回滚/supercronic）。
> 证据：`docs/research/gpustack-validation/`（REPORT §13、hygress.log、CRD dump diff、usage_rows）。
> 注：真机为 v2.2.3 实测兜底；若未来 GPUStack 给 McpBridge 补上 managed 标签或改 ext-auth 转发语义，
> 需按 §13 变更点回归。

---

## 2. 现状分析

### 2.1 GPUStack ↔ Higress 集成契约（必须复刻的外部边界）

GPUStack 是 **纯 k8s-client 控制面**：从不写 Higress 配置文件，只通过 `kubernetes_asyncio`
(CustomObjectsApi / NetworkingV1Api / CoreV1Api) 对嵌入式 kube-apiserver（`127.0.0.1:18443`，
env `APISERVER_PORT`）做 CRUD reconcile。**不存在 Higress 私有 API 契约**——替换面收敛为两块
有限、可枚举的表面：控制面 k8s API 子集 + 数据面语义。

#### 2.1.1 控制面（GPUStack 写、替换网关必须接受）的 CRD 种类

命名空间：`cfg.gateway_namespace`（默认 `higress-system`；embedded/external 时 = `get_namespace()`）。

| 组/版本 | Kind | GPUStack 生成的名称 | 用途 |
|---|---|---|---|
| `networking.higress.io/v1` | `McpBridge` | `default` | 服务目录：registries[] + proxies[] |
| `extensions.higress.io/v1alpha1` | `WasmPlugin` | `gpustack-llm-ext-auth`, `gpustack-ai-statistics`, `gpustack-model-router`, `gpustack-ai-proxy`, `gpustack-set-model-pre-route`, `gpustack-model-mapper`, `gpustack-header-transformer`, `gpustack-token-usage` | 8 段行为 |
| `networking.k8s.io/v1` | `Ingress` | `gpustack`（mirror），`ai-route-route-<id>.internal`（主），`ai-route-route-<id>.fallback.internal`；`ai-route-model-<id>` 系**遗留/清理专用**（`_cleanup_orphaned_gateway_data` 只删不建） | 路由规则来源 |
| `networking.istio.io/v1alpha3` | `EnvoyFilter` | `ai-route-route-<id>.internal` | 4xx/5xx → fallback 内部重定向 |
| `v1` | `Secret` | `gpustack-tls-<host>` / `gpustack-tls-default` | 数据面 TLS 证书 |
| `v1` | `ConfigMap` | `higress-config` | 超时/限制配置（种子 `downstream.idleTimeout=1800, upstream.idleTimeout=10`，`ensure_gateway_timeout` 会重写为 env `GPUSTACK_PROXY_UPSTREAM_IDLE_TIMEOUT_SECONDS` 默认 **3**）—— **Hygress 读 ConfigMap 生效，不硬编码** |
| `networking.k8s.io/v1` | `IngressClass` | `higress` | `is_supported_higress` 探测点（`read_ingress_class(name="higress")`，404 即判不支持；external 模式下探测失败直接 **raise**） |

要点：
- **没有** Higress 的 `Gateway` / `ExtensionPolicy` / `ApiKey` / `Consumer` CRD——鉴权完全委托给
  GPUStack 自己的 `/token-auth`（ext-auth 插件 forward-auth）。
- 所有受管对象都带标签 `gpustack.ai/managed=true`；`match_labels` 只更新 GPUStack 拥有的对象。
- 种子全局 EnvoyFilter `higress-gateway-global-custom-response.yaml`（INSERT_FIRST）**无 managed 标签**
  ——Hygress 的 label-selector list 不应期望遇到它（辅导性说明）。
- 响应形状要求：`apiVersion/kind/metadata.resourceVersion`、list 的 `items: []`、合法的
  `APIResourceList`（启动时 `wait_for_apiserver_ready` ≥60s 内轮询 `get_api_resources` 求 APIServer 就绪）。
- 示例目标寻址：embedded 下 McpBridge registry 是 **`gpustack.static:80`**（`domain=127.0.0.1:<api_port>`）；
  `gpustack.dns:30080` 是 **incluster** 形态。目标串解析器必须接受两种形态。

#### 2.1.2 关键 CRD spec 语义（替换数据面必须遵守）

- **McpBridge** `spec.registries[]`：目标寻址字符串 `name.type:port`（如 `model-5-12.static:80`、
  `provider-1.dns:8443`、`gpustack.static:80`）。`type: static` ⇒ `domain` 已是 `host:port`、`port=80`；
  `type: dns` ⇒ `domain` 为裸 host + 真实 `port`。`proxies[]` 为外部 provider 出向代理。
- **Ingress**（`generate_model_ingress`）：`ingressClassName=higress`；每路径 `backend.resource =
  {apiGroup: networking.higress.io, kind: McpBridge, name: default}`；注解：
  - `higress.io/destination: "<pct>% <svc:port>\n<...>"` —— 换行分隔的加权目标列表（Hamilton 算法
    `hamilton_calculate_weight` 计算百分比）。**mirror ingress 的目标**（`gpustack.<type>`) 无 `pct%` 前缀
    —— 解析器必须两种形式都接受。
  - `higress.io/rewrite-target: /$1$3`
  - `higress.io/ignore-path-case`、`higress.io/proxy-next-upstream[-tries]`
  - `higress.io/exact-match-header-x-higress-llm-model: <route>` —— **核心 header 路由机制**
  - fallback Ingress 增加 `higress.io/exact-match-header-x-higress-fallback-from: <main>`
- **WasmPlugin** 相位/优先级（同一 phase 内 **高 priority 先执行**）：`set-model-pre-route`(AUTHN,90) →
  `ai-proxy`(100) → `ext-auth`(AUTHN,360,`failStrategy: FAIL_OPEN`) → `token-usage`(400) →
  `model-mapper`(AUTHN,800) → `transformer`(AUTHN,810) → `generic-proxy-router`(AUTHN,900) →
  `ai-statistics`(900)。注意相位交叉的净执行序：**generic-proxy-router(900) 在 ext-auth(360) 之前**
  （稍高的 AUTHN priority 先跑），因此 body→`x-higress-llm-model` 定稿发生在鉴权之前 —— §6.1 按此
  **净语义**定义原生流水线，而不是照抄数字。
  - `gpustack-ai-proxy` / `gpustack-model-mapper` 的 `create_only` 只适用于**初始化时的 `spec_replace`**
    （只刷新 `url`/`sha256`）；其 `matchRules`/`providers` 会随路由事件**热更新**
    （`ai_proxy_diff_spec` / `sync_model_route_mapper`）—— 纯 CRD 消费者无需关心，但绝不能「只快照一次
    初始化配置」。
  - `defaultConfigDisable` 运行时**永不翻转**（翻转会重建 Envoy 过滤器链、拆掉长连接）—— 替换实现必须
    视为创建后不可变。
- **EnvoyFilter**（fallback）：HTTP_ROUTE MERGE，4xx/5xx → `custom_response`（最多 10 次内部重定向，
  `use_original_request_body/uri`），注入 `x-higress-fallback-from: <ingress>` 与
  `x-gpustack-fallback-path: %REQ(X_GPUSTACK_ORIGINAL_PATH)%`。

#### 2.1.3 数据面行为（插件链 → 原生等价），端口与旁路

数据面 HTTP 端口 = `cfg.get_gateway_port()`（server 场景 = `cfg.port`，否则 worker 端口）；
HTTPS = `cfg.tls_port`。旁路端口：envoy admin 15000 / health 15021 / metrics 15020（pilot-agent metrics，
**env 可覆盖** `GATEWAY_PILOT_AGENT_METRICS_PORT`）,15090。15020 被 `prerun.py` 的 Prometheus 抓取引用。

**鉴权与用量旁路契约（Hygress 必须调用）**
- `GET /token-auth`（`routes/token.py::server_auth` / worker `/token-auth`）：ext-auth 目标。读
  `x-higress-llm-model`，鉴权（api_key/basic/bearer/cookie/GPUStack token），按 ModelRoute 访问策略
  解析 `registration_token`，返回 `X-Mse-Consumer`（格式 `access_key.gpustack-<user.id>`；
  **公共策略（无 key）鉴权的请求为哨兵值 `'none'`**）、
  `Authorization: Bearer <registration_token>`、`AUTH_CACHE_HEADER` JWT（5 分钟）、dummy cookie。
- `POST /v2/usage/gateway-metrics`（`metrics_collector.flush_gateway_metrics_to_db` 循环消费）：
  头 `X-GPUStack-Auth-Token` = `cfg.get_derived_gateway_token()` = **hex(HMAC-SHA256(jwt_secret_key,
  "gateway-metrics-push"))**。payload = **`ModelUsageMetrics` 恰 17 字段**（已对 wasm 二进制字节级定案，
  见 `docs/research/plugin-contract-pin.md` §2.8/§5.1 —— 修正原先「必含 operation/provider 类字段」的错误
  假设）：
  - **必送 11**：`model`（路由/生效模型名，可为 LoRA 路由名）、`input_token`、`output_token`、
    `total_token`、`input_cached_token`、`request_count`、`completed`、`output_chunk_count`、
    `request_content_bytes`、`started_at`、`completed_at`（UnixMilli）；
  - **omitempty 6**：`user_id`、`model_id`、`model_route_id`、`provider_id`、`access_key`、
    `organization_id`（None 时**省略而非 null**）；
  - **wire 上不存在**：`operation`、`cluster_id`、`provider_name`、`provider_type`（服务器端自派生；
    `operation` 仅在服务器 `OperationEnum` 侧存在：`completion|chat_completion|embedding|rerank|
    image_generation|audio_speech|audit_transcription`，网关不上送）。
  服务端 `_validate_usage_metric` 会丢弃任何 `model_id==None && provider_id==None` 的请求；缺
  `model_route_id` 落入「Untracked」桶并降速告警。归属来源：
  `model_route_id` ← `X-GPUStack-Route-Name`（`<ns>/ai-route-route-<id>.internal`）；
  `user_id`/`access_key` ← `X-Mse-Consumer`（`access_key.gpustack-<user.id>`，公共策略为哨兵 `'none'`）；
  `organization_id` ← `X-Organization-Id`。`completed=false` 时 GPUStack 按字节估算 token —— Hygress
  应尽量送 `completed=true`（SSE 累计 usage 分片，Hydra 已同时解析 OpenAI cached_tokens 与 Anthropic
  cache_read_input_tokens）；`total_token` 优先取上游上报总数（`> input+output` 时服务器按总数对账）。
- Wasm 插件二进制由 GPUStack 自己在 `http://127.0.0.1:{api_port}/{prefix}/...` 提供
  （`gpustack_higress_plugins.server`）；Hygress **不拉取/不执行 Wasm**（本地原生等价），但需在启动期
  与插件 URL/manifest 无关地工作（自身不依赖 plugin server）。

**生命周期/运维**
- 启动：`server.py::initialize_gateway` → `wait_for_apiserver_ready` → `ensure_tls_secret` →
  `ensure_mcp_resources` → `ensure_gateway_timeout`（非 incluster）→ `ensure_ingress_resources`
  (mirror) → `ensure_wasm_plugin`×8；随后 embedded 模式 `_wait_for_gateway_ready` 轮询数据面 HTTP 端口
  （300×2s≈600s）；`_cleanup_orphaned_gateway_data` 清理孤儿 `ai-route-*`（含 legacy `ai-route-model-*`）。
- 事件驱动控制器（leader-only，订阅 DB 事件总线）：`ModelController`→模型实例 registry；
  `ModelProviderController`→provider registry/proxy + ai-proxy 插件；`ModelRouteController._reconcile`→
  `sync_gateway`（mapper + main ingress + fallback ingress + EnvoyFilter + generic-router alias +
  ai-proxy match rules）；`WorkerController`→worker 地址变化更新 registry。全部 `ensure_*` 带
  `@retry(stop_after_attempt(5), wait_fixed(2))`。
- 就绪：Python 等数据面 HTTP 端口；s6 `gateway/run` 等 `GPUSTACK_API_PORT`。s6 longrun 崩溃自重启；
  SIGTERM 停容器。替换进程必须被 s6 同样方式监管，并在重启后重新加载 CRD（GPUStack 会重跑
  `initialize_gateway` 与启动清理）。
- 探测：`utils/platform.py::is_supported_higress(ingress_class="higress")` = 读 IngressClass `higress`，
  成功=>true，404=>false。`detect_gateway_mode` 用它选 incluster / embedded / external（external 缺失
  直接 raise）。
- **worker 角色不对称**：`transformer` + `token-usage` 插件只在 `server_role() != WORKER` 时创建
  （`__init__.py`）；worker 态 registry 名为 `gpustack-worker`；worker 侧 `/token-auth` 走 `worker_auth`
  （registration-token 校验）；mirror ingress 有 worker 变体。**embedded 模式禁止 WORKER 角色**
  （`config.py` raise），MVP 天然安全；`WORKER + external` **明确划出 v1 范围**（§3 D9）。

### 2.2 Hydra（dogress2）可复用资产与差距

Hydra 已被宣称生产就绪（评估 9.2/10：11,056 RPS、p99 4.39ms、65 MiB RSS、~0.3ms 附加延迟），
`dev-docs/HANDOFF.md` 及 6 个 wave 全部完成。

可复用（属性和代码资产）：
1. **Pingora 终止模式**（`hydra-server/src/proxy.rs` `HydraProxy::ProxyHttp`）：整条网关生命周期在
   `request_filter` 内实现，返回 `Ok(true)`，Pingora 从不自己拨上游——全程可控（鉴权、全文读取、
   路由、failover、限流、计量）。这是解决了 GPUStack wasm model-router 首块解析缺陷的结构性方案。
2. **纯核心库 `hydra-core`**：`router.rs`(路由)、`breaker.rs`(纯断路器)、`swrr.rs`(Nginx SWRR 加权)、
   `limit.rs`(滑窗限流)、`sse.rs`(UsageScanner：SSE/JSON usage 零拷贝扫描，含 cached tokens)、
   `extract.rs`(全文 model 提取)、`rewrite.rs`。CI 依赖防火墙保证零 I/O。
3. **热更新配置**：`store.rs::ConfigStore = ArcSwap<ConfigData>` + DashMap 状态——CRD diff → 快照 swap，
   下一个请求即生效，无重启。
4. **`UsageSink` trait**（`sink.rs`，SQLite/ClickHouse 实现，fire-and-forget 缓冲）——GPUStack 计量契约
   的天然插入点（新增 `GpustackSink`）。
5. **TLS**：`tls.rs::HydraCertStore` 动态 SNI 证书，热重载（Secret → 证书映射可直接复用）。
6. **Admin**（`admin/mod.rs`，raw ServeHttp，无框架）+ `/metrics` Prometheus。
7. **multi-tenant 模型**、key 加密存储（AES-256-GCM）、SWRR、breaker、admission、限流。

关键差距（替换契约要求但 Hydra 目前没有）：
- **无 k8s/CRD 消费能力**——没有 kube-rs、没有控制面适配层。
- **无路由规则数据模型**——只有 `(tenant, model) → 静态 provider URL`；没有 header 匹配、
  Ingress 语义（正则/header/rewrite/权重）、动态后端发现。
- **无 registry/discovery**——McpBridge 的 static/dns/proxy/tunnel 四类注册项与动态实例伸缩
  （1s 刷新）不存在。
- **`HttpAuthChecker` 是 POST + 判定**——缺 forward-auth（GET 语义、header 透传/写回、token 注入、
  `FAIL_OPEN`）。
- **无 header transformer / fallback / `X-GPUStack-Model-Instance` / `X-GPUStack-Route-Name`**。
- **无 GPUStack usage sink / `/token-auth` 适配 / 15020 风格指标别名**。
- **路径匹配仅 `is_v1_route` 判定**——无不限 host/path 路由表。

---

## 3. 设计原则与关键决策

| # | 决策 | 理由 |
|---|---|---|
| D1 | **零改动**：GPUStack Python 代码冻结；只允许镜像重打包（s6 脚本替换）与有限 env/config 变更 | 用户核心诉求；GPUStack 与 higress 的耦合是代码级 CRD reconcile，替换=新适配层而非删配置 |
| D2 | **MVP 拓扑 = A（embedded in-place）**：自定义镜像内以**镜像层**处理 s6，不触碰 Python `determine_enabled_services`；替换/禁用脚本见 §11。GPUStack env **完全不变**（embedded 自动探测 18443） | 最小改动面、最快可验证；**同时满足 D1**（`determine_enabled_services` 是 Python，改它会破坏零改动） |
| D3 | **快速验证路径 = 拓扑 B（external 指向）**：`gateway_mode=external` + `gateway_kubeconfig` 指向 Hygress 控制面 + `advertise_address` 指向 Hygress 数据面。**前提：目标集群必须有 IngressClass `higress`（GPUStack 不创建它）——由 Hygress/镜像引导负责播种** | env-only 变更，开发期冒烟/对比回归；播种责任明确归属 |
| D4 | **控制面策略 = 策略 2（保留内嵌 file-storage apiserver，Hygress 用 kube-rs 消费）**：只下掉最重的 3 个进程（pilot/controller/gateway），Hygress 做 controller+data plane。**策略 2 下 Hygress 自身不持久化**——CRD（file-storage apiserver）即持久真相源（GPUStack 是唯一写者；孤儿清理它自己做）。**放弃**策略 1（自研假 apiserver）在 MVP | apiserver 是最轻进程；list/label-selector/resourceVersion 语义免费；无第二持久层（省迁移/发散/恢复）；策略 1（axum+SQLite 假 apiserver 于 18443、写 5 类 CRD API）另列「全量替换」后期 |
| D5 | **数据面 = Pingora 终止模式**（沿用 Hydra 已验证设计） | 全文读取天然优于 Wasm 首块；可控性最高；单二进制无 Wasm 运行时 |
| D6 | **路由核心 = Hydra `ConfigStore`/ArcSwap 快照**：CRD poll diff → 路由表快照 swap，无重启热更新 | 与数据面解耦、天然契合 GPUStack 事件驱动 CRUD |
| D7 | **插件等价 → 原生 Rust 模块**（8 插件行为逐一映射）；**执行顺序按「净语义」而非按插件 priority 数字**（body 定稿先于鉴权；入向剥不可信头最先） | 无 Wasm VM；比 Envoy 链更稳更快；顺序正确性由 §6.1 流水线保证 |
| D8 | **v1 只承诺 OpenAI 兼容子集 + Anthropic 透传**（ai-proxy）；内部实例本就是 OpenAI 格式。非 OpenAI 类型 provider 的失败模式：**优雅透传**（保留原请求）而非硬断 | 收敛范围；避免 DoD#1 e2e 因 provider 类型打挂 |
| D9 | **incluster 模式不承诺**；**`WORKER + external` 明确划出 v1 范围**（worker 态 registry/worker_auth/mirror 变体在 §2.1.3 已描述，作为 L4 扩展） | 收敛工程范围；MVP 仅 server 角色+embedded/external |
| D10 | **worker 直连（direct）/ WORKER-proxy / TUNNEL 三类实例寻址**：MVP 支持 direct + WORKER-proxy；TUNNEL（WebSocket 中继 `worker_websocket_connect_callback`）列 L2+ | 依赖 WebSocket 中继，独立复杂度 |

---

## 4. 总体架构

```
┌────────────────────────────── GPUStack server 容器 ──────────────────────────────┐
│                                                                                  │
│   GPUStack Python (fastapi/uvicorn)           127.0.0.1:{api_port}               │
│     ├─ /token-auth  ────────────────┐          /v2/usage/gateway-metrics ─────┐  │
│     └─ 事件驱动 controller ══CRD══▶ └─┬────────┐     plugin server(wasm, Hygress 不拉取)│
│                                       │        │                               │  │
│  ┌────────────────────────────────────▼────┐   │                               │  │
│  │  apiserver (embedded k8s, 保留)         │   │  127.0.0.1:18443           │  │
│  │  --storage file --file-root-dir ...    │   │  = GPUStack↔Hygress 契约     │  │
│  └──────────────────────────────────────────┘   │  通道(仅 Hygress 读)         │  │
│  ┌──────────────────────────────── HYC ────────────────────────────────────┐ │  │
│  │  Hygress 二进制（s6 服务: hygress, 依赖 GPUSTACK_API_PORT 就绪）         │ │  │
│  │  ┌─ 控制面适配器 gateway-adapter ─────────────────────────────────────┐  │ │  │
│  │  │ kube-rs 启动全量 LIST(managed=true) → 快照 → 1s poll/watch diff   │  │ │  │
│  │  │ → Ingress/McpBridge/WasmPlugin/EnvoyFilter/ConfigMap/Secret       │  │ │  │
│  │  │ → 路由表快照 ArcSwap<ConfigData>（无本地持久化）                    │  │ │  │
│  │  └───────────────────────────────────────────────────────────────────┘  │ │  │
│  │  ┌─ 数据面 (Pingora 终止模式, 首次快照就绪后才监听 80/443)────────────┐  │ │  │
│  │  │ request_filter:                                                │  │ │  │
│  │  │  [剥不可信入向头][model-router 等价内容][ext-auth 等价]           │  │ │  │
│  │  │  全文读取 → 路由引擎(header x-higress-llm-model + path)          │  │ │  │
│  │  │  → registry 解析(static/dns/proxy/tunnel) → SWRR 目标组          │  │ │  │
│  │  │  → [model-mapper 等价: 逐目的地 modelMapping] → failover          │  │ │  │
│  │  │  → 出向(set-model-pre-route 等价头) → [token-usage 等价]          │  │ │  │
│  │  │  → 4xx/5xx fallback（fallback-from 守卫）                         │  │ │  │
│  │  │  上游: 127.0.0.1:{gpustack api}|worker|provider|TUNNEL           │  │ │  │
│  │  └───────────────────────────────────────────────────────────────────┘  │ │  │
│  │  ├─ forward-auth 客户端(/token-auth) ├─ GpustackSink(/v2/usage,HMAC)     │ │  │
│  │  ├─ Admin(mod)/metrics                ├─ TLS(SNI, Secret→cert)           │ │  │
│  │  └─ 15020? /stats/prometheus(浅兼容, 端口即 env)                          │ │  │
│  └───────────────────────────────────────────────────────────────────────────┘ │  │
  │  s6-overlay: apiserver + supercronic(改 run) → hygress                        │  │
  │  (pilot/controller/gateway 镜像层 no-op/替换, 不占 9876/15010/15012/8888/15051)│ │
└───────────────────────────────────────────────────────────────────────────────┘
```

分层职责：
- **控制面适配层（`gateway-adapter`）**：唯一与 k8s/apiserver 打交道的模块。启动 api-resources
  discovery → 全量 LIST（label selector `gpustack.ai/managed=true`）建初始快照 → 1s 轮询（或 watch）
  增量 → diff → `ConfigStore::reload_all()`。**纯只读消费者，不实现 k8s API**（策略 2）；无本地写库。
- **路由引擎（route engine）**：Ingress 注解 → 路由规则，McpBridge registry 解析，SWRR 目标组 + 动态
  刷新 + 逐目的地 model 改写。
- **数据面（Pingora）**：请求周期串联插件等价模块 + 路由 + failover + fallback + 计量。
- **旁路（out-of-band）**：`/token-auth` forward-auth 客户端、`/v2/usage/gateway-metrics` sink、
  `15020 /stats/prometheus`（浅兼容）。

---

## 5. 控制面适配层设计（gateway-adapter）

### 5.1 范围与探针

- 启动序列：`wait_for_apiserver_ready`（GET `/api`，60s 内 5s 重试）→ 逐资源 discovery
  (McpBridge/WasmPlugin/Ingress/EnvoyFilter/IngressClass) → 全量 LIST（label selector）建初始快照 →
  **首快照完成后才绑定数据面 80/443**（避免 GPUStack 300×2s 就绪窗口被慢同步耗尽而在 10 分钟才判定
  失败）→ 进入 1s 轮询（或 kube-rs watch）增量更新 → diff → swap。
- **只读消费，不实现写路径**。策略 1 假 apiserver 时才有写接口（§5.4）。CPU 上无「所有 CRUD 返回
  成功」语义，那属于策略 1。
- 与 GPUStack 生命周期对齐：崩溃重启后全量重 LIST（GPUStack 会重跑 `initialize_gateway` 与孤儿清理，
  Hygress 不必自己删）；心跳用 apiserver 的 ping。

### 5.2 依赖

- `kube-rs`（kube + kube-runtime）+ `k8s-openapi`（`networking.v1` Ingress/IngressClass、
  `core.v1` Secret/ConfigMap）——`networking.higress.io`、`extensions.higress.io`、
  `networking.istio.io` 三类是 **CRD，k8s-openapi 无内置类型**，用 `kube::core::DynamicObject` +
  `Api<DynamicObject>` 泛化消费，配 3 个自定义 GroupVersionResource。
- kubeconfig：embedded 时 **文件** `{data_dir}/higress/kubeconfig`（prerun 写入，
  `https://127.0.0.1:18443`，`insecure-skip-tls-verify:true`，user `higress-admin` 无 token）；
  external 时用户 `gateway_kubeconfig`。
- **拓扑 B 需要播种 IngressClass `higress`**（写入保留的 apiserver 或 external 集群，GPUStack 不创建）；
  embedded（拓扑 A）不检查 IngressClass，无需播种，但播种无副作用、推荐统一做，以备 external 切换。

### 5.3 k8s 对象 → 内部模型映射

| k8s 对象 | 内部模型（`ConfigData` 的一部分） | 关键字段翻译 |
|---|---|---|
| `Ingress ai-route-route-<id>.internal` | `RouteRule(kind=Main)` | `exact-match-header-x-higress-llm-model`→匹配键；`path`(regex)→路径谓词；`rewrite-target`；`destination`(pct% 或无 pct% 列表)；`proxy-next-upstream(-tries)`；`ignore-path-case`；`origin ingress name`（ext-auth 作用域依据） |
| `Ingress ai-route-route-<id>.fallback.internal` | `FallbackRule`（挂 Main） | `fallback-from` matcher；`x-gpustack-fallback-path` |
| `Ingress gpustack`（mirror） | `MirrorRoute("/")` | 直连 GPUStack server（UI/API/`/token-auth`/usage），**不鉴权**；无 `pct%` 目标 |
| `McpBridge default.registries[]` | `Registry`（static/dns/proxy/tunnel） | `domain`+`port`；`proxyName` |
| `McpBridge default.proxies[]` | `OutboundProxy` | provider 出向代理（ai-proxy 用） |
| `WasmPlugin gpustack-model-mapper` | `model_mapping: Map<service_name, model_name>` | per-(ingress,service) matchRules → 逐目的地改写 |
| `WasmPlugin`×其余 | `GatewayFeatureConfig` | 见 §7 逐插件映射；记录 `defaultConfigDisable` 不可变 |
| `EnvoyFilter ai-route-route-<id>` | `FallbackSpec` | 4xx/5xx → fallback，max 10，`use_original_request_body/uri` |
| `Secret gpustack-tls-*` | TLS 证书表（→ `tls.rs` SNI store） | `tls.crt/key`；`gpustack-tls-default` 兜底；管理标签外不碰其他 Secret |
| `ConfigMap higress-config` | 超时/限制配置 | `idleTimeout`（种子 1800/10，patch 后 3）、`maxRequestHeadersKb` 等 |
| **种子全局 custom-response EnvoyFilter** | **忽略**（无 managed 标签） | 适配器 list 时跳过/容忍 |

注：`ai-route-model-<id>` Ingress 不应出现在列表中（仅清理专用），若出现按 GPUStack 语义视为 legacy
忽略即可。

### 5.4 策略 1（后期，可选）：假 apiserver

若想彻底去掉 apiserver 进程：Hygress 内置 axum（或 raw ServeHttp）实现 `127.0.0.1:18443` HTTPS 上的
k8s API 子集：GET `/api`、`/apis/{...}`（APIResourceList）；5 类资源 CRUD + LIST(label selector) +
IngressClass `higress`；其余 404/405。持久化 SQLite（**仅本策略**新增表 `k8s_object`）。响应形状严格对齐
kubernetes_asyncio 期望。估算 2~3 周，置于「全量替换」阶段。§8 的 SQLite 方案仅随本策略启用。

---

## 6. 数据面与路由引擎设计

### 6.1 请求生命周期（Pingora 终止模式）—— 按净语义定义的流水线

沿用 Hydra 的 `request_filter` 串行管道。**顺序依据 Higress 相位净语义 + 安全约束**，而非照抄
priority 数字号：

```
① 剥不可信入向头（最先）  remove X-GPUStack-Auth-Token / X-GPUStack-Model-Instance（入向伪造防护）
② model-router (等价)     (全文/流读取→model; 对客户端预置 x-higress-llm-model 的覆盖或保留待 L0
                          插件源核对定案(§7 核对清单), 不臆测, 按 Higress 实际行为复刻;
                          /model/proxy/<id> 别名→body model 改写; multipart 抽取; maxBodyBytes;
                          路径后缀白名单)
   此时 body-derived model 对后续鉴权/路由可见 = 净语义上 generic-proxy-router(900) 先于 ext-auth(360)
③ transformer-in (等价)  rename x-gpustack-model → x-higress-llm-model（RETAIN_FIRST: 已有则保留）
                          （净语义: 旧头改名在 model-router 定稿之后执行以保障优先级）
④ 路由匹配                header x-higress-llm-model + path 谓词 → RouteRule
⑤ ext-auth (等价)         仅当命中 RouteRule 的 origin ingress 名（含可选 ns 前缀）以 ai-route-route- 开头
                          → forward-auth GET /token-auth（透传/注入头, 写回 X-Mse-Consumer/Authorization/
                          cookie/AUTH_CACHE_HEADER, FAIL_OPEN, 30s 超时）;
                          mirror(gpustack) 及非 GPUStack 路由不鉴权
⑥ 全文读取 (cap→413)      (Hydra 已验证; multipart 单独处理)
⑦ registry 解析           (static|dns|proxy|tunnel → 目标组) → SWRR 加权选后端
⑧ model-mapper (等价)     逐目的地 modelMapping: 按所选 service 名改写出向 body model 字段
                          (LoRA 别名 / 改名 provider 必须命中)
⑨ set-model-pre-route (等价) 写 X-GPUStack-Model-Instance（值必须匹配 get_instance_id_from_header 的
                          regex: model-<mid>-<iid>[-alias].<type>, 即所选实例的 cluster name）
                          + 上游 X-GPUStack-Route-Name（routeNameHeader 等价）
⑩ failover 循环           (复用 breaker+admission; per-route 重试策略 proxy-next-upstream)
   出向: 路径重写(rewrite-target/$1$3) + key 替换 + Host 重写; transformer 出向不得剥离实例头
⑪ 响应流式                (chunk; SSE usage 扫描; TTFT; 去 server/via/... 头)
⑫ token-usage (等价)      POST /v2/usage/gateway-metrics（completed 标志; 完整 ModelUsageMetrics）
⑬ 统计 + 日志 + Prometheus 指标
⑭ 4xx/5xx → fallback      (x-higress-fallback-from 循环守卫 max10; x-gpustack-original-path 备份/
                          x-gpustack-fallback-path 恢复)
```

要点：
- 入向头剥离必须先于 ext-auth（防伪造 `X-GPUStack-Auth-Token`/`X-GPUStack-Model-Instance` 进入鉴权/寻址）。
- **`x-gpustack-original-path` 备份**：transformer 会在请求进入时把 `:path`（+rewrite 前的原始路径）备份
  到此头，供 fallback 流恢复；fallback 时会 rename `x-gpustack-fallback-path → :path`。任一路由生效后
  必须保证出向带去重后的正确 `x-higress-llm-model`。
- **fallback 空目标特例**：GPUStack 在主路由 destinations 为空时会把 fallback 的 destinations **拷进主
  Ingress**（代码 FIXME 行为）——Hygress 由直接消费 CRD 天然获得，**禁止「简化」掉**。
- **fail-open 边界**：ext-auth FAIL_OPEN + WORKER-proxy 目标 ⇒ 请求到达 worker proxy 时可能未携带改写
  后的 `Authorization` ⇒ 401 ⇒ 落入 fallback 链——作为可接受的等效行为在测试中固定。

### 6.2 路由规则数据模型（新增，核心）

```rust
struct RouteRule {
  kind: RouteKind,                     // Main | Fallback | Mirror（键空间类型隔离）
  key: String,                         // Main: 匹配键=x-higress-llm-model(模型名); Fallback: 不参与 Main 索引
  ingress_name: String,                // 源 Ingress 名（ns 限定, 如 higress-system/ai-route-route-5.internal）;
                                       //   供 ext-auth 作用域(ai-route-route- 前缀)与 x-higress-fallback-from 发射
  main_ingress_name: Option<String>,   // Fallback 显式关联的主路由（Fallback 不可作为 Main 命中）
  path_predicates: Vec<PathPred>,      // Ingress path 正则（**全匹配/锚定**, 非 substring）; 仅命中路由内选谓词
  rewrite_target: Option<PathRewriter>,// /$1$3 等捕获组重写
  destinations: Vec<Destination>,      // pct%(可缺省=100) → (Registry, service:port)
  retry: RetryPolicy,                  // proxy-next-upstream(-tries); 错误/超时/5xx/non_idempotent
  fallback: Option<FallbackLink>,      // 4xx/5xx → fallback（单一规范表示, FallbackSpec 由它推导）
  auth_scope: AuthScope,               // 依据 ingress_name 前缀 ai-route-route- 判定（非路径）
  model_mapping: Map<name.type(无端口), String>, // 逐目的地 → 出向 body model 名（model-mapper）
}
struct Registry { id, kind: Static|Dns|Proxy|Tunnel, domain, port, proxy_ref }
```

- **SWRR 状态按“路由目标组”共享**（不按单个服务）：`(route key + 目标组稳定摘要)` 一个共享
  `current_weights`，实现组内加权选择（Hamilton 百分比生效）；快照 swap 时以代际计数清理陈旧组状态。
- **usage**：保留上游 `total_tokens`（embedding/rerank 场景 `total_token > input+output` 时服务器按
  上报总数对账，不得用 input+output 重算覆盖）；`operation` 取服务器 `OperationEnum` 精确枚举：
  `completion | chat_completion | embedding | rerank | image_generation | audio_speech |
  audit_transcription`（注意服务器拼写 `audit_transcription`）。

- 路由输入键（匹配顺序，**遵循 Higress AND 语义**，经 Gate-1 审核修订）：
  1. **阶段 1（Main 路由）**：仅按 `x-higress-llm-model` 精确匹配 Main 路由，且该路由的 path 谓词
     必须**全匹配**（锚定正则，非 substring）——header AND path 同时成立才命中；
  2. **Fallback 路由**：独立的 `x-higress-fallback-from` 索引，**仅**fallback 重定向尝试时查询，初始
     请求永不命中 Fallback 路由；
  3. **Mirror `/`**：唯一基于 path 的兜底 —— 无/未知 `x-higress-llm-model` 的请求落到 mirror
     （直连 GPUStack server 自行鉴权/404/直连模型处理），**绝不**跨 Main 路由按 path 选路；
  4. 最长字面锚点仅用于**已命中路由内部的谓词选择**（rewrite 捕获组），不做跨路由选择。
- `RouteRule` 携带 `ingress_name`（ns 限定，如 `higress-system/ai-route-route-5.internal`），
  Fallback 通过 `main_ingress_name` 显式关联主路由；Main 与 Fallback 键空间类型隔离，Fallback 不可作
  为 Main 命中。
- `model_mapping` 键对齐 GPUStack 规则的 service 名格式，**两类键分开固定、不得混用**：
  matchRule 的 `service` 用 `registry.get_service_name()` = **`name.type`（无端口）**；而
  `higress.io/destination` 注解用 `get_service_name_with_port()` = **`name.type:port`**。适配器在
  解析 model-mapper matchRules 与 destination 时必须各自用对应格式，出向应用模型改写时以 SWRR
  选中的目标（service:port）反查 `name.type` 键。

### 6.3 registry 解析与动态实例寻址

- 目标 = `(registry, service:port)`；`name.type:port` 字符串解析（与
  `McpBridgeRegistry.get_service_name_with_port()` 一致）：
  - `dns` → 域名:端口直连（worker/GPUStack/gpustack 实例）
  - `static` → `host:port`（embedded 的 `gpustack.static:80` = `127.0.0.1:<api_port>`；direct 实例）
  - `proxy` → 经 `OutboundProxy`（provider 出向）
  - `tunnel` → 经 WebSocket 中继（`worker_websocket_connect_callback`，L2+）
- **`X-GPUStack-Model-Instance` / `X-GPUStack-Route-Name` 头**：路由决定后写出（§⑨）；worker 用实例头
  定位实例端口（实例 cluster name 如 `model-1-2.static`，格式匹配 `get_instance_id_from_header` 正则）；
  出向 `X-GPUStack-Route-Name` 的值格式（`{org_id}/{route}` 仅具参考性）**无服务器端消费方**，由外部
  插件源定义——确切格式并入 L0 核对（§15 Q2）；实现期按原生 route id 携带、待核对后对齐，不臆测外部格式。

### 6.4 流量治理（复用 Hydra 原生能力）

- SWRR 加权负载均衡（`hydra-core::swrr`）+ per-(route→dest) 状态。
- 断路器 + 后台探活复活（`breaker_wrap`）。
- 准入队列 / 限流（`limit.rs` 滑窗、`admission.rs`）——GPUStack 契约不需要，保留为增值能力。
- 重试：`RetryPolicy` 直译 `higress.io/proxy-next-upstream(-tries)`（默认 error,timeout,503,502,
  non_idempotent, 2 tries）。

---

## 7. 插件等价模块（8 个 Wasm → 原生 Rust）

| 插件 | Hygress 原生等价 | 关键行为 | 工期估 |
|---|---|---|---|
| `gpustack-model-router`(generic-proxy) | `model_router.rs` | 全文 body 提取 model；**对客户端预置 `x-higress-llm-model` 的覆盖/保留语义待 L0 插件源核对定案**（默认按 Higress 实际行为复刻，不臆测；因 ext-auth 按此头作用域，spoof 面由契约本身决定）；`/model/proxy/<route_id>/` 别名 → 解析 + **改写 body model 字段**（JSON/multipart）；multipart model part 抽取；`maxBodyBytes`；路径后缀白名单；`aliasNameMapping` 仅刷新、`defaultConfigDisable` 不可变 | ~1 wk |
| `gpustack-set-model-pre-route` | 路由钩子 | 选实例 → 出向写 `X-GPUStack-Model-Instance`（格式=`get_instance_id_from_header` 正则可解析）+ `X-GPUStack-Route-Name` | 2-3 d |
| `gpustack-model-mapper` | `model_mapper.rs` | **逐目的地**模型名映射（`model_mapping` keyed by service 名）；**随路由事件热更新**（非 init-only）；LoRA 别名（`l<sha256[:8]>` 注册表）与改名 provider 必须命中 | 3-5 d |
| `gpustack-header-transformer` | `transformer.rs` | **有序规则引擎**：入向 remove `X-GPUStack-Auth-Token`/`X-GPUStack-Model-Instance`；rename `x-gpustack-model`→`x-higress-llm-model`；rename `x-gpustack-fallback-path`→`:path`(fallback)；dedupe(RETAIN_FIRST/LAST)；`:path`→`x-gpustack-original-path` 备份；出向保留实例/route-name 头 | 3-5 d |
| `gpustack-llm-ext-auth` | `forward_auth.rs` | **GET** `/token-auth`；透传 `X-Real-IP/X-Forwarded-For/x-higress-llm-model/x-api-key/cookie/x-gpustack-auth-cache`；注入 `X-GPUStack-Auth-Token`；**写回 `X-Mse-Consumer`/`Authorization`/cookie/`AUTH_CACHE_HEADER`**；30s 超时、`FAIL_OPEN`；复用 Hydra auth 缓存(5min)；**作用域=路由名前缀 `ai-route-route-`** | 3-5 d |
| `gpustack-ai-proxy` | `ai_proxy.rs` | v1 OpenAI 兼容子集 provider（`providers[]/(apiTokens/failover/retryOnFailure)` + `matchRules[]`）+ key 替换/失败转移（复用 provider_client）；**非 OpenAI 类型优雅透传**；claude 透传 | 1-2 wk |
| `gpustack-token-usage` | `gpustack_sink.rs` | `POST /v2/usage/gateway-metrics`，`X-GPUStack-Auth-Token`=HMAC(jwt_secret_key,"gateway-metrics-push")；**wire=恰 17 字段**（必送 11 + omitempty 6：user_id/model_id/model_route_id/provider_id/access_key/organization_id；`operation`/`cluster_id`/`provider_name/provider_type` **不上送**，见 §2.1.3）；归属来自 X-Mse-Consumer + X-GPUStack-Route-Name；`completed=true` 优先；仅 model-route 流量上报 | 3-5 d |
| `gpustack-ai-statistics` | `ai_statistics.rs` | 浅兼容统计型（content-type 门控 `GATEWAY_AI_STATISTICS_PLUGIN_CONTENT_TYPES`）；MVP 记录 + 暴露 metric | 浅 |

**插件源核对清单（启动设计任务的 L0 前置，不得延后）**：
1. 拉取 `gpustack-higress-plugins` PyPI（或 GPUStack 镜像内文件）锁定 token-usage **确切 JSON schema**
   与 model-router multipart 行为、`model_route_id`/`operation` 的确切推导。
2. **model-router 对客户端预置 `x-higress-llm-model` 的覆盖/保留行为**（§6.1② / §7 措辞当前为
   待核 tentative；该语义影响 ext-auth 作用域 = 防 spoof 的关键）。
3. **token-usage 的作用域**：插件 `matchRules=[]` 为全局作用域——需确认它是否会对非 model-route
   流量（无 `x-higress-llm-model`，含 mirror 路由到 GPUStack 自身推理端点）上报；Hygress 的 sink
   **必须同作用域（仅 model-route 流量上报）**，避免与 server 端 `record_model_usage`
   （`api/middlewares.py`）双计（服务端 `_validate_usage_metric` 会**丢弃**缺 model_id/provider_id 的
   报告，可能因此恰好抵消双计，但需以插件源为准）。
4. 确认 `X-GPUStack-Route-Name` 是否被 Hygress 替换的插件之外消费（服务器端已 grep 无消费方；值格式
   由插件源定义，并入 Q2）。
5. 采样 `set-model-pre-route` 的实例选择算法（是否重复加权选择）——若非等价，做一致性说明。

---

## 8. 数据模型与持久化

**策略 2（MVP）下不新增本地持久化**：CRD（file-storage apiserver）= 持久真相源；GPUStack 是唯一写者，
孤儿清理由其自身完成。Hygress 启动 L0 = 全量 LIST → 快照；运行时 = poll diff → ArcSwap swap；
重启 = 重 LIST。无迁移、无发散、无恢复逻辑。

仅**策略 1（假 apiserver，全量替换后期）**启用本地 SQLite（沿用 Hydra sqlx）+ 新表 `k8s_object`
(group,version,plural,namespace,name,body_json,rv)。数据面始终只读 `ConfigStore` 快照（ArcSwap），
保证多 worker 见同一份配置、锁无关。

---

## 9. 认证、安全与 TLS

- **转发鉴权（forward-auth）**：作用域为**路由名前缀** `ai-route-route-`（`_match_route_prefix_` 打的是
  Envoy route name 而非请求 path，`__init__.py:336-356`）——正确实现 = 命中 RouteRule 的 origin ingress
  名（含可选 `gateway_namespace` 前缀）以 `ai-route-route-` 开头才鉴权；mirror `/` 与 GPUStack 自身流量
  永不鉴权。**以 path 前缀判定会导致 FAIL_OPEN 下的安全洞**，禁止。
- `GET` 语义 + header 透传/写回（见 §7 ext-auth）+ 5 分钟缓存（复用 Hydra `AuthCache`/Redis 失效流）、
  `FAIL_OPEN`（GPUStack 契约 `failStrategy: FAIL_OPEN`）+ 30s 超时。
- **`jwt_secret_key` 获取（设计内定案，禁止在实现期才定）**，解析优先级：
  1. env `GPUSTACK_JWT_SECRET_KEY`（with-contenv 可见，分布部署必配）；
  2. 文件 `{data_dir}/jwt_secret_key`（`prepare_jwt_secret_key` 仅在**自动生成**时写入；若操作者经
     config 文件显式提供 key，此文件**不存在或过期**——读到旧值会全部 401、usage 静默丢失）；
  3. 两者皆无 → **启动即 fail-fast**（不静默降级）。
  文档注明「config 文件显式提供 key」场景需用户以 env 注入同值；e2e 必须断言 usage 行真的落地
  （§12）。
- **出向认证**：`X-GPUStack-Auth-Token` 注入（token-auth 签名）、AI-proxy 的 provider `apiTokens`
  替换（复用 `provider_key` AES-256-GCM 静态加密 + 运行时随机 pick）。
- **数据面 TLS**：读 `Secret gpustack-tls-<host>`/`-default`（含种子全局 custom-response EnvoyFilter
  之外的、managed 对象）→ `HydraCertStore` SNI store（热重载）。
- **管理面**：Hygress Admin（`HYGRESS_ADMIN_ADDR`，默认 127.0.0.1:8081，admin-token 门控）——需确认
  与 GPUStack 默认端口无冲突（GPUStack server 默认 80，admin loopback 不冲突）；数据面不暴露管理端口。
- **威胁模型**：数据面只与 GPUStack server（loopback）、worker、provider 通信；不拉取/不执行 Wasm；
  kubeconfig 落在 `{data_dir}/higress/kubeconfig`（文件级权限收敛）。

---

## 10. 可观测性与兼容

- `/metrics`（Prometheus，Hygress 原生，hydra 指标名 + 新指标）。
- **15020 `/stats/prometheus` 浅兼容**：绑定数据面旁路端口（**取 env `GATEWAY_PILOT_AGENT_METRICS_PORT`
  而非硬编码**），输出 envoy 风格指标名子集（或测速降级），满足 `prerun.py` 抓取不报错；Grafana 重画
  列 L3。
- 用量链路：`GpustackSink` → `/v2/usage/gateway-metrics` → GPUStack DB（GPUStack 侧零改动）；
  **e2e 断言真实落行**。
- 审计：沿用 Hydra logging（request/duration/tokens/cached/ttft/retries）。

---

## 11. 部署与运维（embedded 打包）

### 11.1 s6 手术方案（镜像层，绝对不改 Python）

`determine_enabled_services` 是 Python（会重写）且 `prepare_s6_overlay` **每次启动都会删除并重建
`contents.d` 条目**（`prerun.py`）——因此**不能**靠删 image 里 contents.d 实现，必须改 run 脚本本体：
- 保留：`apiserver`（策略 2，18443）+ **`supercronic`**。
- 替换：`gateway/run` → `exec hygress`（注入 `GPUSTACK_GATEWAY_CONFIG`、`EMBEDDED_KUBECONFIG_PATH`
  等既有 env 语义）。**承接 `gateway/run` 的文件副作用**：原脚本创建 `${HIGRESS_LOG_DIR}/access.log`
  并把 `/etc/logrotate.d/higress-logrotate` 指过去，supercronic 每小时跑该 logrotate——`hygress` 的
  run 脚本应变相写 `${HIGRESS_LOG_DIR}/access.log`（兼容加分）或在镜像中和掉该 cron 行，避免启动即
  持续报错。
- no-op 化：`pilot/run`、`controller/run` → 改为**长 sleep**（oneshot/长驻型），**禁止 `exit 0`**
  （s6-supervise 会把瞬间退出当崩溃进入重启循环）；保留脚本文件以维持 s6 拓扑与可回滚；`*-logger`
  管道消费者保持挂接在 no-op 生产者上（无害闲置，一句话说明即可）。
- **改 `supercronic/run`：去掉 `readinessCheck "Higress Pilot" 15010`**（supercronic 是
  `gateway_services` 成员，其 run 脚本读 pilot 15010 就绪；pilot 没了它就永远不 ready，cron 任务
  ——含 postgres 日志清理 `postgres-log-cleanup.sh`——将全停）。supercronic 的 cron 目标保持
  GPUStack 注入项。
- **端口占用**：Hygress 不监听 9876 / 15010 / **15012**（pilot 三端口）/ 8888 / 15051（controller
  专用），仅数据面 (HTTP/HTTPS) + 15020 浅兼容 + admin 8081；`ports_for_services` 启停前会对
  15000/15021/15090/15020/**15012**/18443 做可用性检查（Harmless，Hygress 不与之冲突）。

### 11.2 就绪与生命周期

- 依赖：`hygress` 等 `GPUSTACK_API_PORT` 就绪（对齐原 `gateway/run` readiness）→ 数据面监听
  `GATEWAY_HTTP_PORT`/`tls_port`；Python `_wait_for_gateway_ready` 自然通过。
- **绑定时机**：数据面 80/443 在**首快照 LIST 成功后**才监听（避免慢同步吃掉 GPUStack 的 300×2s
  就绪窗口 → 快速失败而非 10 分钟挂起）。
- s6 longrun 崩溃自重启 → adapter 全量重 LIST → 数据面无感（ArcSwap）。SIGTERM 停容器。
- 镜像：`pack/Dockerfile` 增加 Hygress 构建 stage（借鉴 dogress2 `environment/Dockerfile` debian slim +
  tini）；回滚开关：镜像层保留 `pilot/controller/gateway` 原 run 脚本（另目录），便于对比回归。

---

## 12. 兼容性矩阵与回归验证基线

| 契约面 | Hygress 承诺 | 验证方式 |
|---|---|---|
| 控制面 CRD 读兼容 + 响应形状 | 全量（策略 2 天然满足） | GPUStack `tests/gateway/*` 单测化为 **CRD fixture 生成器**：录制 `ensure_*` 产物 → Rust 端断言解析/翻译一致（渲染「零失败」为「fixture 基线一致」） |
| 数据面路由 | `x-higress-llm-model`/path/`pct%` 权重/`/model/proxy` 别名/mirror `/` | e2e 推理直连 + 回放录制 |
| 鉴权 | `/token-auth`（写回 X-Mse-Consumer/Authorization/cookie，FAIL_OPEN，路由名作用域） | e2e：key/无 key/坏 key/FAIL_OPEN 链路 |
| 用量 | `/v2/usage/gateway-metrics`（完整字段、completed=true） | **e2e 断言 usage 行落地：model_route_id 非空 + access_key 归属正确**（不止 200） |
| model 改写 | 逐目的地 modelMapping（LoRA/改名 provider） | e2e：LoRA 别名路由、provider 改名路由命中正确上游 model |
| fallback | 4xx/5xx max10、fallback-from 守卫、original-path 备份恢复 | e2e：主动 503 → fallback 生效且 loop 接线正确 |
| 内存/延迟 | 单进程 ≤100MB、terminate-mode 低延迟 | 与 Higress 对照压测 |
| 多租户 | 原生（增值，不改 GPUStack 契约） | 内部压测 |

**验证流水线**
1. 单元（Rust）：route engine / forward-auth / transformer / registry 解析 / SWRR / model_mapping 应用，
   用录制 CRD 快照断言。
2. 集成：`docker-compose` GPUStack（官方镜像 + Hygress 替换 s6）→ 拓扑 B（external）冒烟 → 拓扑 A；
   e2e：模型→实例→推理(OpenAI/Anthropic 流式)→用量落行→fallback。
3. 回归：录制 CRD 快照集合在 CI 中回放 + GPUStack 版本升级对齐测试（适配器独立模块 + 版本参数化）。

---

## 13. 分阶段实施计划（估算，与 deep-dive 对齐）

**L0 前置（必做，不晚于 L1 结束）**：拉取 `gpustack-higress-plugins` 并锁定 token-usage wire schema
与 model-router multipart 行为（§7 核对清单）；采样 set-model-pre-route 实例选择；确认
`X-GPUStack-Route-Name` 消费方。

| 阶段 | 内容 | 依赖 | 估算（人周） |
|---|---|---|---|
| L0 | 控制面适配器：kube-rs 启动全量 LIST → 快照 → 1s poll diff → ArcSwap swap；label selector/孤儿容忍；**(拓扑 B) IngressClass 播种**；Response-shape/就绪时序（首快照后绑端口） | D4 | 1–2 |
| L0-2 | **路由引擎（核心）**：RouteRule 数据模型（含 model_mapping）、header+path 匹配、registry(static/dns/proxy/tunnel) 解析、SWRR 目标组、动态 1s 刷新、mirror 直连、rewrite/别名 | L0 | 3–4 |
| L1 | forward-auth(/token-auth, 路由名作用域, cookie 写回)、`GpustackSink`(HMAC jwt key 解析、完整字段、completed)、model-router 等价(multipart/别名/body 改写/maxBodyBytes) | L0-2 | 1–1.5 |
| L2 | transformer 规则、fallback(含空目标特例)、ai-proxy v1(OpenAI 子集+优雅透传)、worker-proxy 寻址、TLS Secret→SNI、model-mapper 逐目的地 | L1 | **4–6**（=deep-dive 逐项之和，不再乐观压缩） |
| L3 | 可观测性：15020(env 端口) /stats/prometheus 浅兼容 + envoy 指标名映射；Grafana 重画（可选） | L2 | ~1 |
| L4 |（可选）集群模式、策略 1 假 apiserver、TUNNEL 完整、`WORKER+external`、incluster 评估 | — | 悬置 |
| 测试 | 单元 + 集成 + e2e 基线（全阶段持续；含 usage 落行断言、LoRA/改名用例） | — | 1–2 |

**MVP（拓扑 B/L2 前可验证 + L0/L0-2/L1 主路径）约 6–9 人周；全量（含 L2-L3/TLS/fallback）约
10–16 人周。**

---

## 14. 风险与缓解

| 风险 | 等级 | 缓解 |
|---|---|---|
| s6 手术破坏 supercronic（pilot 就绪依赖） | 高 | §11.1 精确方案：pilot/controller no-op、gateway→hygress、supercronic 去 pilot check；镜像层回滚开关 |
| `jwt_secret_key` 读旧值 → usage 全部 401 静默丢 | 高 | §9 优先级解析 + 缺失 fail-fast；e2e 断言 usage 落行；文档注明 config-key 场景 |
| `token-usage` wire format 不确定（插件源码外置） | 中 | L0 前置拉包锁定 schema；服务器侧 `ModelUsageMetrics`/_validate 作为基准；e2e 落行断言 |
| **逐目的地 model 改写缺失 → LoRA/改名 provider 打到错误上游 model** | 中 | RouteRule.model_mapping + 出向应用；e2e 覆盖 LoRA/改名用例 |
| **插件执行序错误（body 定稿晚于鉴权/剥头晚于鉴权）** | 中 | §6.1 净语义流水线（剥头最先、body 先于 ext-auth）；顺序单测 |
| **ext-auth 作用域误实现（path 判）→ FAIL_OPEN 安全洞** | 中 | 路由名前缀作用域 + 单测；禁用 path 判定 |
| 拓扑 B 缺 IngressClass → external 启动即 raise | 中 | §5.2 播种责任归属（Hygress/镜像引导），拓扑 B 冒烟覆盖 |
| CRD 注解语义漂移（GPUStack 版本） | 中 | 适配器独立模块 + 版本参数化 + 录制快照回放 CI |
| 流式 completed 语义与 token-usage 插件不一致 | 中 | 对齐 SSE 累计 usage 分片协议 |
| worker-proxy/TUNNEL WebSocket 中继复杂度 | 中 | L2+；MVP direct + WORKER-proxy |
| 与原三进程混跑资源争抢/端口冲突 | 低 | s6 镜像层 no-op 长 sleep + 回滚；不占 9876/15010/15012/8888/15051 |
| 数据面-服务器循环依赖（token-auth 在 server） | 低 | mirror 与 model 路由拆开；鉴权仅 model 路由 |
| `defaultConfigDisable`/`create_only` 语义误翻 | 低 | 只读消费 + 记录；「热更新非 init-only」文档化 |

---

## 15. 开放问题（设计内定案 / 实施期用数据确认）

| # | 问题 | 状态 |
|---|---|---|
| Q1 | token-usage 精确 wire schema 与作用域 | **已定案**（L0 前置完成）：恰 17 字段（§2.1.3），`operation`/`cluster_id`/`provider_name/provider_type` 不上送；模型路由/mirror 双计问题 → sink 仅 model-route 流量上报（`plugin-contract-pin.md` §2.8/§5.1） |
| Q2 | `set-model-pre-route` 的实例选择是否与 Hygress SWRR 加权一致；`X-GPUStack-Route-Name` 是否被插件之外消费 | L0 前置采样确认；不一致则文档化等价性 |
| Q3 | `jwt_secret_key` 取点 | **已定案**（§9）：env → `{data_dir}/jwt_secret_key` → fail-fast |
| Q4 | 出向 header 集：确认 `x-higress-llm-model` 保留出向、`x-gpustack-original-path` 的发射时机（每请求 vs 仅 fallback） | L1 实施时按 transformer 插件源码与合约锁定 |
| Q5 | `WORKER + external` 网关（worker_auth/gpustack-worker registry/worker mirror） | 已定案**排除 v1**（§3 D9），L4 扩展 |
| Q6 | `higress-config` 除 idleTimeout 外（`maxRequestHeadersKb`、http2 窗口等）须遵循项；admin 8081 端口冲突 | L0 实施时核对 ConfigMap 全键；admin 默认 loopback 不冲突 |
| Q7 | 拓扑 B 的 IngressClass 播种与保留 apiserver 的 higress.io/extensions/istio CRD serving | 已定案由 Hygress/镜像引导播种（§5.2）；拓扑 B 冒烟验证 CRD serving 真实可读 |
| Q8 | ai-proxy v1 对非 OpenAI provider 的失败模式 | 已定案**优雅透传**（§3 D8） |

---

## 附：Hydra → Hygress 资产复用对照

| Hydra 资产 | Hygress 用途 |
|---|---|
| `hydra-core`（router/breaker/swrr/limit/sse/extract/rewrite/config） | 核心纯逻辑库，原样/瘦身复用 |
| `proxy.rs` 终止模式 + failover 循环 + 流式响应 + SSE 扫描 | 数据面骨架（超集改造：插件等价流水线） |
| `store.rs` ConfigStore(ArcSwap) + DashMap | 热更新快照机制（路由表挂载点） |
| `sink.rs` UsageSink trait | 新增 `GpustackSink` 实现 |
| `tls.rs` HydraCertStore SNI | TLS Secret → SNI store |
| `http.rs` HttpAuthChecker + AuthCache | forward-auth 复用缓存机制（+GET 模式/写回） |
| `admin/mod.rs` + `/metrics` | 管理面/指标 |
| `cluster/` + `redis/`（feature） | L4 集群扩展骨架 |
