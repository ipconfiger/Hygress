# Hygress 网关延伸能力审核与落档

> 目的：对 Hygress（GPUStack 内嵌 Higress 原位替换）在**token 配额 / 限流 / 路由策略 / 安全护栏**
> 四类网关延伸需求上做一次深度审核，明确三分类（**已实现 / 需实现 / 不必实现**）与配置来源边界
> （**GPUStack 控制面驱动 vs Hygress 配置文件**），并给出优先级与前置缺口。
> 依据：实际代码核验（crates/ 四层）+ `docs/design.md`（v1.5）+ `docs/research/plugin-contract-pin.md` + 真机验证记录。
> 日期：2026-09-04；状态：**已核验**（基于代码事实，非臆断；个别处标注"需人工确认"）。

## 0. 结论摘要

| 能力 | 分类 | 配置来源 | 优先级 |
|---|---|---|---|
| 路由（model-router/route_match/registry/SWRR/fallback/镜像） | **已实现** | GPUStack 控制面（CRD，只读） | — |
| 认证（ext-auth）/ 头转换（transformer）/ 模型映射 / ai-proxy 令牌交换 | **已实现** | GPUStack 控制面（CRD） | — |
| usage 计量（17 字段上送）/ admin / 15020 / 热重载 / s6 部署 | **已实现** | 环境变量 + CRD | — |
| **Token quota（配额）** | 需实现 | **Hygress 配置文件**（现 GPUStack v2.2.3 无配额 CRD/API） | P1 |
| **限流（Rate limiting）** | 需实现 | **Hygress 配置文件** | P0 |
| **路由策略（Routing policy）覆盖层** | 需实现（部分基础已实现） | **Hygress 配置文件** | P2（先过设计门禁） |
| **安全护栏（Guardrails）** | 需实现（分 3 档） | **Hygress 配置文件** | P3（含响应侧骨架） |
| 分布式限流（Redis/多副本） | **不必实现** | — | — |
| Hygress 播种自有 CRD | **不必实现** | — | — |
| 路由策略做成 GPUStack model-router 平替 | **不必实现** | — | — |
| 配额双写回 GPUStack DB | **不必实现** | — | — |
| 全量 OWASP WAF | **不必实现** | — | — |

---

## A. 已实现能力盘点（IMPLEMENTED）

> 逐项核验自 crates/hygress-gateway/src/pipe.rs、pipeline/{…}.rs、bootstrap.rs、config.rs，
> crates/hygress-core/src/（route/registry/swrr/model_mapping/usage/transform/matcher/retry），
> crates/hygress-egress/src/（forward_auth/provider/token/usage_sink），crates/hygress-adapter/src/（snapshot/translate）。

| # | 能力 | 落点 | 说明 |
|---|---|---|---|
| A1 | **模型路由（model-router）** | `pipeline/model_router.rs` | body→模型派生 + `x-higress-llm-model` 覆盖 + cap |
| A2 | **路由匹配** | `pipeline/route_match.rs` + `core/route.rs` | Main/Fallback/镜像三空间隔离，Higress AND 语义 |
| A3 | **注册表解析** | `pipeline/registry_resolve.rs` + `core/registry.rs` | static/dns/proxy/tunnel → ResolvedTarget |
| A4 | **加权下游选择（SWRR）** | `pipeline/swrr_select.rs` + `core/swrr.rs` | 目标组共享状态 |
| A5 | **回退（fallback）** | `pipeline/fallback.rs` | 有界重定向环（别名字段 `x-higress-fallback-from`） |
| A6 | **镜像（mirror）** | `route_match` + `config.rs`(mirror_name) | 唯一 path 兜底 → GPUStack API；`:80/readyz` |
| A7 | **认证（ext-auth）** | `pipeline/auth.rs` + `egress/forward_auth.rs` | `/token-auth`：7 头转发 + Authorization + 注入派生 token；路由名前缀作用域；FAIL_OPEN |
| A8 | **头转换（transformer）** | `pipeline/transformer.rs` | 入站 strip / set / pre-route |
| A9 | **模型映射（model-mapper）** | `pipeline/model_mapper.rs` + `core/model_mapping.rs` | destination 级模型名映射（LoRA/改名） |
| A10 | **AI 代理令牌交换（ai-proxy）** | `egress/provider.rs` + 管道 | provider-destined 请求令牌替换（D6） |
| A11 | **usage 计量** | `egress/usage_sink.rs` + `core/usage.rs` | `ModelUsageMetrics` 17 字段 → `/v2/usage/gateway-metrics`，丢行闸门、非 2xx 计量 |
| A12 | **TLS/SNI** | `core/config.rs`(TlsHost) + 数据面 | Secret gpustack-tls-* → TLS 终止 |
| A13 | **admin / 可观测** | `bootstrap.rs` | 127.0.0.1:8081（healthz/metrics/reload token 保护）+ 15020 readyz 浅兼容 + tracing |
| A14 | **热重载** | `adapter/snapshot.rs` + `SharedConfig`(ARC-swap) | CRD 快照变更即点即用（零重建） |
| A15 | **部署/回滚** | `pack/Dockerfile.hygress` + `pack/hygress-s6/` | s6 手术 + `.dist` 原件快照 |
| A16 | **错误语义/短回路** | `error.rs`(GatewayError) + `pipe.rs` short_circuit | 统一 `{status, reason-slug}`；前置 fail-fast |

> 对四类延伸需求的直接既有底座：**usage 计量（A11）→ token quota 的记账流**；**SWRR+fallback+条件匹配（A2/A4/A5）→ 路由策略的基础原语**；
> **transformer 规则表（A8）+ retry 核心类型（core/retry.rs）→ 护栏静态规则与超时/重试的现成骨架**。

---

## B. 待实现能力（NEEDS-IMPLEMENTATION）

### B1 Token quota（token 配额）
- **语义**：按 `api_key × model × 窗口`（或总量）限制 token 消耗；超额 429（带 `Retry-After`/余量头）或 403。
- **落点**：core 新增纯类型 `QuotaEngine`（预算表 + reserve→commit/release 两段式，防流式/并发超卖）；管道新增阶段 `quota`（**ext-auth 之后**，拿到 `X-Mse-Consumer` 身份再计量）。
- **依赖既有**：usage 计量流（A11）提供真实 completion_token；鉴权写回（A7）提供 consumer；`core/usage.rs` 的字段可直接复用为扣减依据。
- **配置来源建议：Hygress 配置文件**（`hygress.policy.yaml` 的 `quota:` 段 + 热重载）。理由：GPUStack v2.2.3 **没有**配额 CRD/API（真机核验其 modelroute 仅有 `access_policy` PUBLIC/AUTHED）；配额为租户业务配置，应归运维按文件管控并支持即点生效。若未来 GPUStack 提供配额 API → 走 `egress` 加 `QuotaClient`（镜像 forward_auth 的字节 pin 模式），此时转为**GPUStack 控制面驱动**。

### B2 限流（Rate limiting）
- **语义**：RPS/并发/突发；按 `IP / api_key(consumer) / user / route`；超限 429 + `Retry-After`，错误体对齐 GPUStack OpenAI 风格。
- **落点**：core 新纯类型 `RatLimiter`（令牌桶/滑动窗口，dashmap 原子，单实例）；**两个阶段**：匿名/IP 限流在管道**最前**（inbound strip 后，早拒省算力）；按 key/user 限流在 **ext-auth 后**（consumer 已知）。
- **依赖既有**：`pipeline/registry_resolve`/matcher 原语与阶段注册表；错误短回路（A16）直接给 429。
- **配置来源建议：Hygress 配置文件**（`limit:` 段，路由级 + 全局，支持继承/覆盖）。理由：限流是**网关自有语义**，与 GPUStack 无契约关系；不应占用 GPUStack CRD 面。

### B3 路由策略（Routing policy，覆盖层）
- **语义**：灰度/条件路由（header/path/model_name/region）、模型→provider/region 绑定、策略头注入、超时/重试策略覆盖、目标组覆盖（替代默认 SWRR 顺序）。
- **落点**：core 新 `RoutePolicy` 类型（条件表复用 `core/matcher.rs` 原语）；管道新增阶段 `routing_policy`（**route_match 之前**）；与 SWRR（A4）保持"覆盖层"语义——**只覆盖不重写**，metric 标 `policy_applied` 可溯源。
- **依赖既有**：model-router 派生模型（A1）、registry（A3）、SWRR（A4）、fallback（A5）、core/retry.rs（超时/重试核心类型）。
- **配置来源建议：Hygress 配置文件**（`policy:` 段）。理由：策略是对 GPUStack"事实路由"的扩展覆盖层，走独立文件才能清晰区分"GPUStack 原生路由 vs Hygress 覆盖"且可整体回退，避免与 GPUStack 原生 model-router 的字节契约冲突。
- **门槛**：与 GPUStack 原生路由冲突面最大 → **建议先过 oracle 设计门禁**再实现。

### B4 安全护栏（Guardrails，分三档）
- **B4a 静态可判定规则**（提示注入特征、敏感词、内容类型、体大小、max_tokens 封顶、超时）：
  core 规则表（仿 `core/transform.rs` 规则）+ 请求侧新阶段 `guardrail`（transformer 前后）+ 结构前置拦截。
  配置走**配置文件**（规则表热重载）。
- **B4b LLM 判定**（审核模型/护栏服务外呼）：`egress` 新增 `GuardrailClient`（并发上限、判词缓存、超时降级）；同步拦截或异步旁路；
  配置走**配置文件**（模型/服务/阈值/**失败策略**）。**关键定案：安全方向默认 `fail-closed`**（超时/出错→拒绝并告警），
  与 ext-auth（A7）的 FAIL_OPEN 相反——两者性质不同，必须显式配置。
- **B4c 输出侧护栏**（响应合规/PII 脱敏）：**前置缺口**——见 D-2（无通用响应侧管道）。需在 Pingora upstream 回调上扩展响应体读取/拦截骨架（SSE 流式场景需逐块或缓存策略，需设计定案）。usage 计量（A11）已证明响应体可达（completion_tokens 取自响应），可在此能力上扩展。

---

## C. 不必实现项（NOT-NEEDED）及理由

| 项 | 理由 |
|---|---|
| **分布式限流（Redis/多副本）** | Hygress 是**单二进制数据面**（embedded 单实例定位，无多副本数据面）；分布式协调在 v1 收益≈0、复杂度高。引擎抽象可移植预留即可。 |
| **Hygress 播种自有 CRD** | 内嵌 apiserver 极简（真机验证不支持写 IngressClass，405），无法承载 Hygress 定制 CRD；且违背"零 Python 改动 / 只读控制面"原则 → 延伸配置一律走配置文件。 |
| **把路由策略做成 GPUStack model-router 的平替/改写** | GPUStack 的 model-router 路由是**字节契约事实**（真机验证 16 CRD 逐字节一致）；Hygress 只做覆盖层，语义冲突面最小化。 |
| **配额记账双写回 GPUStack DB** | token quota 直接复用 usage 计量流（A11）即可，无需反向写 GPUStack DB，避免一致性负担。 |
| **全量 OWASP/WAF（完整 CRS 规则集）** | 超范围、收益不聚焦于 AI Gateway 场景；只实现与 LLM 服务相关护栏子集（注入/合规/结构）。 |
| **规则在线学习/动态编译** | 静态规则表 + 文件热重载已满足；在线学习引入不确定性与安全风险，不值得。 |

---

## D. 配置来源边界总表（GPUStack 控制面 vs 配置文件）

> 原则：**与 GPUStack 已有 CRD/计量/鉴权有契约关系的能力 → 由 GPUStack 控制面写入、Hygress 只读消费**；
> **网关自有语义的延伸能力 → Hygress 自有配置文件 + admin `/reload` 热重载**（内嵌 apiserver 不允许 Hygress 播种 CRD）。

| 能力 | 配置来源 | 由谁配置 / 载体 |
|---|---|---|
| 路由 / 认证 / 映射 / 变换 / 回退 / 镜像 / TLS / usage | **GPUStack 控制面（CRD）** | GPUStack 写 Ingress/McpBridge/WasmPlugin/EnvoyFilter/Secret/ConfigMap；Hygress 只读 |
| 端口 / kubeconfig / 数据目录 / 窗口超时 | **环境变量**（s6 `gateway/run` 注入） | 部署者改 compose/env |
| **Token quota** | **Hygress 配置文件**（`quota:`）· 未来可切 QuotaClient→GPUStack | Hygress 侧运维 |
| **限流** | **Hygress 配置文件**（`limit:`） | Hygress 侧运维 |
| **路由策略** | **Hygress 配置文件**（`policy:`） | Hygress 侧运维 |
| **安全护栏** | **Hygress 配置文件**（`guardrail:`：规则/模型/阈值/失败策略） | Hygress 侧运维 |
| admin/metrics/15020/readyz | 固定契约 + 环境变量 | 不可配置面最小化 |

---

## E. 边界与风险（核验结论）

- **E-1（前置缺口）"Hygress 无独立配置文件机制"判断成立**：进程配置仅 `.parse(from_env)`（`config.rs`）+
  CRD 派生 `ConfigData`（adapter）；唯一的 `serde_yaml` 出现在 `translate.rs`（解析 GPUStack 写的 WasmPlugin
  配置文档），**并非 Hygress 自身配置**。→ **四类延伸需求共用的前置工程**：新增 `hygress-policy.yaml`
  加载（`/etc/hygress/`，可选 `HYGRESS_CONFIG` env）→ 并入 `SharedConfig` ARC-swap 热重载 → 复用 admin
  `/reload`（token 保护已存在）。建议**先做这一个共用骨架**，四类能力都在其上生长。
- **E-2（前置缺口）无通用"响应侧管道"**：`pipe.rs` 以 `write_response_body(Some(chunk), false)` 分块转发上游
  响应、`(None, true)` 收尾；usage 计量已从响应体取 completion_tokens（响应体可达），但**没有通用的
  响应体读取/改写/拦截钩子**（护栏 B4c 需要）。→ 需在 Pingora upstream 回调上补响应侧骨架，SSE 流式策略需设计定案。
- **E-3 事实纠错**：无（本审核未发现对既有设计结论的事实性偏差；个别"已实现细节"以代码为准）。
- **E-4 失败模式必须区分领域**：ext-auth 是"接入认证"（FAIL_OPEN 合理）；安全护栏是"安全/合规"（必须
  fail-closed）；两者并存时护栏默认 closed、且配置显式化，避免"放行恶意请求"。

---

## F. 优先级与实施路线

| 序 | 项 | 理由 | 门槛 |
|---|---|---|---|
| P0 | **配置骨架**（policy.yaml + SharedConfig 热重载 + admin reload） | 四类能力的共用前置 | 低 |
| P1 | **限流**（B2） | 自包含、无外呼、纯本地、价值直接 | 低（无需外呼） |
| P2 | **Token quota**（B1） | 复用 usage 计量流；二段式扣减需细致 | 中 |
| P3 | **路由策略覆盖层**（B3） | 价值高但冲突面大，须与 GPUStack 原生路由协同 | **高——先过 oracle 设计门禁** |
| P4 | **安全护栏**（B4a→B4b→B4c） | 最复杂；B4c 依赖响应侧骨架（E-2）；B4b 依赖 egress + fail 策略定案 | 高——B4 的失败策略+SSE 策略先设计定案 |

**贯穿工程约束**：每个能力 = core 纯类型 + 管道新阶段 + 可选 egress 客户端，沿用 TDD、零 mock、
每阶段 metric `{stage,result,route}`；沿用 Gate 门禁流程。v1 不引入 Redis/多副本（见 C）。
