# Hygress vs 原生 Higress —— 控制面能力等价性分析（对照 GPUStack 实际写入面）

> **结论先行**：在 **GPUStack 当前实际写入的控制面 surface 内**，Hygress 的控制面与 Higress（pilot/xDS）**行为等价**（CRD 逐字节一致性 + 真机 e2e 已验证，DoD1-6）；等价性边界是**有意设计**——GPUStack 未动态变更的 WasmPlugin 配置由原生实现按 pin 契约固化，不消费 CRD 内容。差异清单见 §B（无 🔴 阻断项；3 项 🟠 潜在项均绑定"GPUStack 未来演进"而非当前行为）。
>
> 一句话版本：**"GPUStack 所用 surface 内范围性等价（真机验证）；surface 外的 Higress/Istio 通用控制能力不等价，且有明确清单（§B）。"**

**方法**：以 `plugin-contract-pin.md`（GPUStack 源码 + Wasm 二进制逆向的 wire 契约）与 Higress 架构（pilot/xDS 6-controller，见附录 A）为对照基线，逐字段核对 `hygress-adapter/src/translate.rs` 的消费面与 `hygress-core/src/config.rs` 的校验语义，数据面行为对照 `pipe.rs`，动态面对照 `lib.rs`/`snapshot.rs`（WATCH 实现）。

## A. 控制面等价矩阵

### A1. 配置输入面（6 类 managed CRD → translate.rs 消费面）

GPUStack 控制器实际写入的 CRD 种类：Ingress（model/fallback/mirror 路由）、McpBridge `default`（registries/proxies）、WasmPlugin ×8、EnvoyFilter（per-route fallback + 全局 custom-response）、Secret（`gpustack-tls-*`）、ConfigMap `higress-config`。（契约钉 §1：9 插件中 `gpustack-rate-limit` **从未被 GPUStack 创建**。）

| CRD / 字段 | Higress 机制 | Hygress 消费（file:line） | 判定 |
|---|---|---|---|
| Ingress `ai-route-route-<id>.internal`（main） | Ingress Controller → Istio Gateway/VirtualService | `classify_ingress_name` + 6 个 `higress.io/*` 注解（destination/rewrite-target/ignore-path-case/proxy-next-upstream[-tries]/exact-match-header×2）→ `RouteRule`（translate.rs:55-66, 263-329） | **EQUIVALENT**（注解集与 pin §3.2 一致；去重保序；ns 限定名 D9） |
| Ingress `*.fallback.internal` | fallback EnvoyFilter（custom_response + 内部重定向） | `RouteKind::Fallback` + `wire_fallbacks`（translate.rs:1002-1003）→ 4xx/5xx 重派发 + `x-gpustack-fallback-from` 恢复（pipe.rs ⑮） | **EQUIVALENT-WITHIN-SCOPE**（重派发语义等价；custom_response 的**最终错误体形状**不复刻，见 §B-C5） |
| Ingress `gpustack`（mirror） | mirror 透传 | `RouteKind::Mirror`（translate.rs:211-214） | **EQUIVALENT** |
| Ingress legacy `ai-route-model-*` / 非受管 | —（清理期残留） | **按设计忽略**（translate.rs:215, 950-953，trace 日志） | ✅ 有意忽略 |
| McpBridge `spec.registries[]` | McpBridge Controller → Istio ServiceEntry（static/dns/proxy/tunnel） | `mcpbridge_to_registries` → `Registry` 四类全支持（translate.rs:955-960） | **EQUIVALENT** |
| McpBridge `spec.proxies[]` | —（provider egress proxy） | `OutboundProxy`（D8 出向代理） | **EQUIVALENT** |
| WasmPlugin `gpustack-model-router` | xDS 下发；GPUStack 控制器**只热更 `aliasNameMapping`**（pin §2.3, py:utils.py:1397-1446） | `wasmplugin_model_router` → `ModelRouterSettings`（translate.rs:533-543；last-wins across puids） | **EQUIVALENT-WITHIN-SCOPE**：热更字段（aliasNameMapping）已消费；`modelKey`/`autoRouting*`/`maxBodyBytes` 等 **init 静态字段**未消费 → 原生等价实现取自有默认（`model_key` 固定 `"model"`，与 GPUStack init 默认一致） |
| WasmPlugin `gpustack-model-mapper` | xDS 热更 per-route `matchRules`（modelMapping） | `wasmplugin_model_mapping` → `merge_model_mapping`（translate.rs:993-1000，按 `name.type` 键） | **EQUIVALENT**（LoRA 别名/改名的热更主路径） |
| WasmPlugin `gpustack-ai-proxy` | xDS 热更 `providers[]`+`matchRules[]`（per-route activeProviderId） | `wasmplugin_ai_proxy` → `ProviderToken`（translate.rs:972-974；D6 密钥换写） | **EQUIVALENT** |
| WasmPlugin ext-auth / header-transformer / token-usage / ai-statistics（4 个） | xDS 下发 init defaultConfig（init 后 GPUStack 不热更） | **CRD 内容不消费**——行为原生实现：ext-auth→egress forward_auth（派生 token）、transformer→transform.rs 头规则①-⑨、token-usage→usage sink、ai-statistics→metrics（translate.rs:962-963 仅记录为 `GatewayFeatureConfig`） | **EQUIVALENT-WITHIN-SCOPE**（设计决策：行为按 pin 契约原生固化；CRD 内容忽略——见 §B-C1） |
| EnvoyFilter（fallback, managed） | custom_response 重定向 | `wire_fallbacks` → `FallbackLink` | **EQUIVALENT-WITHIN-SCOPE** |
| EnvoyFilter（全局 custom-response，**无 managed label**） | 全局错误响应整形 | **忽略**（label 过滤，translate.rs:22-24） | 🟡 **GAP-C5**（见 §B） |
| Secret `gpustack-tls-*` | Cert Server 签发 + SDS 下发 | `secret_to_tls_host`（tls.crt/tls.key base64）→ TLS SNI 监听 | **EQUIVALENT-WITHIN-SCOPE**（🟠 GAP-C3：单默认证书 + 无热载，见 §B） |
| ConfigMap `higress-config` | ConfigmapMgr → EnvoyFilter（tracing/gzip/mcpServer 等） | **仅** `downstream.idleTimeout`/`upstream.idleTimeout`/`maxRequestHeadersKb` → `TimingConfig`（translate.rs:848-880）；其余键忽略 | **EQUIVALENT-WITHIN-SCOPE**（🟡 GAP-C2：envoy 调优类键不生效） |
| Http2Rpc / Gateway API / VirtualService 等 | Higress Controller 支持 | 不消费（GPUStack embedded 模式不写） | **N/A**（超出 GPUStack 使用面） |

**reject-vs-ignore 语义**（与 xDS NACK 对照）：
- **per-object 错误**（空 key/无 destination/bad endpoint/权重和非法/未知 fallback 目标/duplicate route key）→ **丢弃该对象 + warn，其余生效**（config.rs:64-94 `sanitize`；translate.rs:948/960 skip-and-log）——粒度**细于** Istio 的 per-object NACK。
- **结构性错误**（路径谓词非合法正则）→ **整快照拒绝**、keep last-known-good + warn（config.rs:1488；lib.rs:395-397）——语义对应 xDS NACK-keep-last。**差异**：higress 的 NACK 状态可经 `istioctl proxy-status` 观测；hygress 仅日志（见 §B-C4）。
- **未知字段**：serde 宽松解析 + 未知注解/键忽略（translate.rs:540/555 显式注释）——GPUStack 升级新增字段不会导致拒绝（fail-open），但也**不生效**（§B-C1）。

### A2. 行为/功能面（GPUStack 依赖的数据面行为）

| GPUStack 依赖的能力（pin §4.2 管线 ①-⑮） | Higress 实现 | Hygress 实现 | 判定 |
|---|---|---|---|
| ① 剥离伪造头（Auth-Token/Model-Instance） | transformer 规则 1-2 | transform.rs 原生（管线①） | EQUIVALENT |
| ② 模型解析（path alias / body / autoRouting） | generic-proxy-router Wasm | model-router 原生（body 零拷贝 B1-B4） | EQUIVALENT-WITHIN-SCOPE（autoRouting 字段未消费，见 §B-C1） |
| ③ transformer-in（rename/backup :path） | transformer 规则 3-9 | 原生 | EQUIVALENT |
| ④ 路由匹配（`x-higress-llm-model` exact + path regex） | Envoy RDS | RouteTable（H2 快照缓存 + SWRR） | EQUIVALENT |
| ⑤ ext-auth forward-auth（/token-auth、FAIL_OPEN、30s、7 头转发 + 4 头写回） | ext-auth Wasm | egress forward_auth（token 派生 HMAC 契约一致，e2e 验证） | EQUIVALENT |
| ⑦⑧⑨ registry resolve + SWRR + instance/route 头 | Envoy cluster + set-header Wasm | 原生 SWRR + `X-GPUStack-Model-Instance`/`Route-Name`（`get_instance_id_from_header` 正则一致） | EQUIVALENT（LB 算法 SWRR vs envoy 加权 RR：长期分布等价，短时平滑度不同——🟢） |
| ⑩ model-mapper（LoRA/改名） | model-mapper Wasm | 零拷贝 body model 改写（B1-B4） | EQUIVALENT |
| ⑪ failover（proxy-next-upstream ×tries + path 重写 + key swap） | Envoy retry + fallback EnvoyFilter | 候选循环 + `x-gpustack-fallback-from`（D7） | EQUIVALENT |
| ⑫⑬ usage 落库（17 字段 → `model_usage_details`） | token-usage Wasm → POST /v2/usage | usage sink（B2 SSE 计量；DoD5 34/7 行逐位一致） | EQUIVALENT |
| mirror `/readyz` 透传 | envoy route | mirror route（基准路径） | EQUIVALENT |
| TLS 终止 | SDS + Cert Server | gpustack-tls-* Secret → 单默认证书 SNI | 🟠 GAP-C3 |
| rate-limit（第 9 插件） | GPUStack 未创建（wheel 内闲置） | hygress 自有令牌桶（扩展，policy.yaml 驱动） | **hygress 超集**（GPUStack 未用的槽位反而有原生实现） |

**判定**：GPUStack 数据面契约（pin §4.2 管线 ①-⑮ + §5 wire 断言）**全链路原生覆盖**，DoD1-6 真机验证（CRD 逐字节、usage 34/7 行逐位、端口纪律、回滚）。超出 GPUStack 使用面的 Wasm 能力（§B-C1）是唯一系统性边界。

### A3. 动态控制/生命周期面

| 维度 | Higress（pilot/xDS） | Hygress（WATCH 快照） | 判定 |
|---|---|---|---|
| 配置下发 | MCP-over-xDS `xds://127.0.0.1:15051` + k8s → Envoy LDS/RDS/CDS/EDS/SDS，增量推送（亚秒） | 6 类 WATCH（`kube-runtime` watcher，reconnect/relist 内建）→ 去抖 → 一次全量 LIST+translate+ArcSwap store；rv 指纹幂等短路（rv==0 加固）；30s 安全网 tick | **EQUIVALENT-WITHIN-SCOPE**：变更收敛 ≤1 事件周期（亚秒级 vs xDS 毫秒级，同量级）；无事件时 30s 兜底（实例缩扩容等 k8s 事件必然触发 WATCH，不依赖兜底） |
| 实例 join/scale/delete | EDS 增量 | McpBridge registries 变更 → WATCH 事件 → SWRR 池重建 | EQUIVALENT-WITHIN-SCOPE（全量重建 vs 增量——namespace 规模下 O(对象数) 可忽略；未基准测实例扩缩收敛延迟，🟢 注） |
| 配置校验失败 | NACK（per-object，`proxy-status` 可见） | per-object skip+warn / 结构性整快照拒绝 keep-last-good（lib.rs:395） | **EQUIVALENT-WITHIN-SCOPE**（语义同向；可见性差异见 §B-C4） |
| 启动门控 | envoy 先起、配置后到（空配置期） | **fail-fast**：首快照成功才绑 :80（300s 窗口，bootstrap.rs） | 行为差异（有意）：hygress 不出现"无配置裸奔窗口"；代价是 apiserver 未就绪则网关不就绪 |
| 证书轮换 | Cert Server + SDS 热下发 | Secret WATCH → 快照更新，但 **TLS 证书仅启动时写 PEM 绑定**（bootstrap `write_default_tls_pem` 一次性） | 🟠 GAP-C3：轮换需重启进程 |

### A4. 观测/运维控制面

| 能力 | Higress | Hygress | 判定 |
|---|---|---|---|
| 配置下发状态可见性 | `istioctl proxy-status` / `proxy-config`（NACK/版本可见） | 日志（warn on reject/skip）+ **无**当前生效快照的 introspection 端点 | 🟡 GAP-C4：建议 admin 增 `/config` dump（ArcSwap 快照直接序列化）+ `config_reject_total`/`config_skip_total` 计数器——低成本补齐 |
| 指标 | envoy stats + prometheus（丰富） | `:15020/stats/prometheus` 浅兼容 + `hygress_*` 家族（requests/duration/tokens/ttft/retries/upstream_errors/fallback/auth/rate_limit/quota/policy/guardrail） | EQUIVALENT-WITHIN-SCOPE（GPUStack 消费的指标面已覆盖；envoy 细粒度 cluster-level stats 无对等——🟢） |
| 健康/就绪 | — | `/healthz`（admin）+ `:80/readyz`（镜像路径）+ 绑定门控 | ✅ |
| 热更 | xDS 推送 | CRD WATCH + policy.yaml mtime 1s + admin `/reload`（token 门禁） | ✅（Phase 1.1 后零周期打点） |
| 日志 | envoy/pilot 分散 | 单进程 Rust tracing 集中 | ✅ |

### A5. 端口/契约面

| 契约 | Higress | Hygress | 判定 |
|---|---|---|---|
| 数据面 :80/:443 | envoy | pingora（TLS SNI 单默认证书，🟠 C3） | ✅（DoD3） |
| `:80/readyz` 镜像 | envoy → GPUStack | mirror route → GPUStack（真机 200 双侧） | ✅ |
| admin/stats 端口纪律 | pilot 15010/15012/15051、console 8080 | **永绑定**禁用端口 9876/15010/15012/8888/15051；admin 127.0.0.1:8081、stats 15020 | ✅（DoD6 `ss -ltn` 验证零泄漏） |
| usage 落库 | token-usage Wasm → `/v2/usage/gateway-metrics` | egress sink（17 字段逐位一致，`model_usage_details` 34/7 行 DoD5） | ✅ |
| CRD 只读 | — | 6 类 LIST/WATCH + label selector，**不写任何 CRD**（IngressClass 种子除外，topology-B 显式开启） | ✅ |

## B. 诚实差距清单（按严重度）

| # | 严重度 | 差距 | 证据 | 影响 |
|---|---|---|---|---|
| 🟠 C1 | 潜在（GPUStack 演进才可见） | **未消费的 WasmPlugin 配置字段**：8 个受管 WasmPlugin 中 4 个（ext-auth/header-transformer/token-usage/ai-statistics）的 `defaultConfig` 完全不消费（原生等价实现，按 pin 契约固化）；model-router 仅消费 `aliasNameMapping`（`modelKey`/`autoRouting*`/`maxBodyBytes` 忽略）。GPUStack **当前**不热更这些字段（pin §2.1/2.3/2.7），但 **GPUStack 升级若写入新字段/改 init 配置，hygress 静默忽略**（translate.rs:962-975 仅 trace） | translate.rs:540/555（unknown keys … ignored, never reject）、962-975 | 建议：启动时对已消费 WasmPlugin 的 `defaultConfig` 做**未消费键告警**（warn 一次），并把"GPUStack 版本升级 → 重跑契约 pin 对比"写入手册 |
| 🟠 C2 | 潜在 | **`higress-config` ConfigMap 仅消费 3 个超时键**（idleTimeout×2/maxRequestHeadersKb，translate.rs:848-880）；mesh/tracing/gzip/mcpServer.redis 等 envoy 调优键静默忽略 | translate.rs:848-880 | 建议：未知顶层键 warn 一次；文档明示"仅超时三键生效" |
| 🟠 C3 | 潜在（TLS 轮换/多证书场景） | **TLS 证书启动时一次性写入**（`write_default_tls_pem` 取 default/first），Secret WATCH 会更新快照但**不重载监听证书**；多 `gpustack-tls-*` 主机时仅默认证书服务所有 SNI | bootstrap.rs:226-244 | 单证书 GPUStack 部署无影响；多证书/轮换场景需重启或补 SNI+热载 |
| 🟡 C4 | 运维可见性 | 无 `istioctl proxy-config` 等效物：当前生效快照（路由/注册表/特性/被拒对象）不可 introspect；拒绝/跳过仅 warn 日志、无计数指标 | lib.rs:395-397 | 建议：admin `GET /config` dump + `config_reject_total`/`config_object_skipped` 计数器（成本低，ArcSwap 已有） |
| 🟡 C5 | 行为差异（语义等价内） | 全局 custom-response EnvoyFilter（未受管）被忽略——最终错误的**响应体形状**可能与 envoy 不同（hygress 用自有 JSON 错误形状）；SWRR vs envoy 加权 RR 的短时分布差异；`realIPHeader` 默认值歧义（pin §6.1） | translate.rs:22-24；pin §6 | 低影响：GPUStack 服务端不解析网关错误体；e2e 已验证错误路径功能 |
| 🟢 C6 | N/A | Http2Rpc / Gateway API / Gateway Controller / Cert Server 签发、xDS/MCP 协议本身、多集群——GPUStack embedded 模式不使用（契约钉范围外） | Higress 架构附录 | 记录边界即可 |
| 🟢 C7 | 已消除 | 早期"每请求路由表重建"、close-delimited、单线程、1s 轮询——均已修复（857d21b/5df02f2/815ebd3/493dc21/cf4f6c5），benchmark §6-§11 曲线为证 | benchmark.md | — |

**没有 🔴**：GPUStack **当前实际写入**的每一个控制面变更（路由/注册表/provider 令牌/模型映射/TLS/超时）都被 hygress 消费并产生等价数据面行为，且有真机 DoD（CRD 逐字节一致、usage 逐位一致、429/403/200 矩阵）背书。

## C. 判定（推荐表述）

> **在 GPUStack 当前实际使用的控制面范围内（6 类受管 CRD 的已写字段 × 数据面管线 ①-⑮），Hygress 与内嵌 Higress 控制面行为等价，且已经真机验证（CRD fixture 逐字节一致、e2e usage 落库逐位一致、wrk 同 rig 对比性能持平或反超）。等价性边界是显式设计的：GPUStack 静态初始化的 4 个 WasmPlugin 与 higress-config 的 envoy 调优键由原生实现按 pin 契约固化，不消费 CRD 内容——这换来"无 Wasm 运行时/无 xDS/单二进制"的资源与运维收益（≈23× RSS、13.6k req/s 内核下限），代价是 GPUStack 未来若写入这些字段或引入新 CRD kind，hygress 不会自动跟随（静默忽略 + trace 日志）。动态配置收敛语义等价（WATCH 事件驱动 ≤1 事件周期 vs xDS 推送；失败 keep-last-good 双方一致），仅证书热轮换与配置可见性存在运维级差距（C3/C4）。**

一句话版本：**"GPUStack 所用 surface 内范围性等价（真机验证）；surface 外的 Higress/Istio 通用能力不等价，且有明确清单（§B）。"**

## D. 建议（按优先级，均可小步落地）

1. **C4（低成本高价值）**：admin 增加 `GET /config`（dump 当前 ArcSwap 快照：路由/注册表/provider 令牌脱敏/特性开关/指纹 rv）+ `config_reject_total` / `config_skipped_total` 两个计数器——把"拒绝/跳过"从日志升级为可抓取指标，对齐 istioctl proxy-status 的运维可见性。
2. **C1（防演进漂移）**：对已消费 WasmPlugin 的 `defaultConfig` 增加"未识别键"一次性 warn（启动/变更时），并在 `plugin-contract-pin.md` 增补"GPUStack 升级时需重跑 pin 对比"的检查单条目——把静默忽略变成可发现的漂移。
3. **C3（按需）**：TLS Secret WATCH 已在快照中（rv 变更即重建）——补一个"快照 TLS 指纹变化 → 重写 PEM + pingora 热加载（若支持 conf reload）或输出需重启告警"的闭环；多证书 SNI 作为独立 backlog（当前单默认证书已覆盖 GPUStack 默认部署）。

## 附录 A：Higress 控制面是什么（xDS / istiod / 6 控制器）

（背景：基于高度权威来源浓缩，完整出处见 hygress-vs-higress-report 调研）

- **xDS**：Envoy 从控制面动态发现配置的协议族——LDS（Listener）/ RDS（Route）/ CDS（Cluster）/ EDS（Endpoint）/ SDS（Secret）；经 `DiscoveryRequest`/`DiscoveryResponse` 承载；分 SotW（全量）vs Delta（增量）、每型一条流 vs **ADS 聚合单流**（Istio 用 ADS）；带版本 + **ACK/NACK**（NACK 填 `error_detail` 并回退上一有效配置——被拒配置不生效）。
- **istiod（pilot）**：k8s watch → 合并到内部配置模型 → **按每个代理全量重算 Envoy 视图** → ADS gRPC 推送；防抖默认 `PILOT_DEBOUNCE_AFTER=100ms`/`MAX=10s`。经典行为即"每次变化对全部已连接代理全量重算 + SotW 全量下发"（我们报告所指的"全量 xDS 推送"）；**现代 Istio 默认 `ISTIO_DELTA_XDS=true`**（线上尽量增量下发，但仍是每次全量重算 per-proxy 视图）。
- **Higress**：控制面**扩展自 Istio/pilot**（`higress-group/higress/AGENTS.md`、`docs/architecture.md`）；Higress Core 通过 **MCP（基于 xDS）** 作为 Discovery 配置源（`xds://127.0.0.1:15051`）；6 个控制器（Ingress/Gateway/McpBridge→ServiceEntry/Http2Rpc/WasmPlugin/ConfigmapMgr→EnvoyFilter）；数据面 Pilot Agent 代理 Envoy 的 xDS 请求（UDS）。GPUStack 内嵌该整套（server 容器内 apiserver + pilot + envoy）。w依赖 Istio 1.27 线（go.mod）。
- **与 Hygress 对照**：Hygress 直接 kube WATCH 6 类受管 CRD → 去抖 → **仅 rv 指纹变化时重新翻译全量快照** → ArcSwap 快照供数据面按请求读取；稳态零配置推送、**不引入 Envoy/istiod/xDS**。对照之下 Higress 即便在现代 delta-xDS 下，仍是"事件驱动、面向全部已连接 Envoy、每次全量重算 per-proxy"的控制面→数据面配置分发模型。

## 证据索引

- `plugin-contract-pin.md` §1-§6（9 插件契约/字段/热更面）
- Higress 架构（higress-group/higress `AGENTS.md` / `docs/architecture.md`；istio.io 架构 / pilot-discovery env；envoyproxy.io xDS 协议）
- `translate.rs`（:22-24 label 过滤、:202-217 路由分类、:533-543/:972-974 三插件消费、:848-880 configmap、:940-1003 分发与 skip-and-log、:2097-2150 测试）
- `config.rs`（:64-94 sanitize、:1488 structural reject）、`lib.rs`（:395-397 keep-last-good）
- `snapshot.rs`（WATCH + 指纹 + rv==0 加固）、`bootstrap.rs`（:226-244 TLS 单证书）
- `benchmark.md` §3-§5（DoD 真机证据）、`README.md` §4（部署/回滚）
- 分析者：@oracle（基于仓库 + lib-1 xDS/istiod 调研）
