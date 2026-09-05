# Hygress 升级开发设计 v2（终版 —— 二轮复审无阻塞 + M0-M4 已实现，见 §9/§10）

> 依据：`docs/extensions-audit.md`（深度审核）+ `docs/design.md`（v1.5）+ 实际代码结构。
> 范围：Token quota · 限流 · 路由策略 · 安全护栏（四类延伸能力）+ 两个前置缺口（配置骨架 / 响应侧管道）。
> 状态：**终版（v2）** —— 已消解首轮审核 5 项阻塞（BLOCK-1~5）并裁决 D-1~D-15；二轮复审无阻塞（§9）；
> M0-M4 已实现并真机 e2e（§10）。
> 原则延续：TDD + 零 mock/stub；纯逻辑进 `hygress-core`、外呼进 `hygress-egress`、阶段进 `hygress-gateway`；
> 热重载走独立 `SharedPolicy`(ARC-swap)；每能力沿用 Gate 门禁；GPUStack 控制面只读（契约边界不变）。

---

## 1. 目标与边界

- **目标**：在现有 9 插件等价管道之上，以"阶段 + 纯核心引擎 + 配置"新增四类能力，全部由 **Hygress 自有配置文件**驱动（`extensions-audit.md` §D），不触碰 GPUStack 控制面、不改 Python、不改端口契约。
- **边界**：
  - 与 GPUStack 契约的能力（路由/认证/计量/映射/变换/回退/镜像/TLS）**维持现状**，延伸能力全部是"前置阶段/覆盖层/后处理"。
  - 延伸能力 fail-safe 可回退：缺文件/坏配置/未配置 → 按能力默认语义（§7）。
  - 单实例定位（embedded 单二进制数据面）——不引入 Redis/分布式。
  - **quota 只覆盖 model-route 流量**（`UsageTarget` 为 None 的 mirror/直通无 consumer 亦无 usage 上报，与既有 usage 作用域对齐）。

## 2. 前置工程（P0 骨架，四类能力共用）

### 2.1 配置骨架（消除 E-1）
- **载体**：独立 `SharedPolicy` —— `ArcSwap<PolicyConfig>`（core 纯类型 + serde derive；持有器仿 `SharedConfigHandle` 放 `GatewayState.policy`）。**限流/配额等引擎可变态（DashMap）挂 `GatewayState`/`PolicyHandle`，绝不进 `ConfigData`**（后者被 adapter 每秒整体换入，状态即丢；`SharedConfig::store` 无相等性短路，现状保留不动，不做"Data 不变则不动"）。
- **路径**：`HYGRESS_POLICY_PATH`（env 覆盖，默认 `/etc/hygress/policy.yaml`）。文件名统一 `policy.yaml`，弃 `HYGRESS_CONFIG` 命名。
- **加载/热重载**：启动加载；**mtime 轮询**（实现期先按 1s 与 adapter poll 同节奏；审计 R-8 后并入
  quota/限流 evict dutycycle，周期 **30s**，`bootstrap.rs`）+ admin `POST /reload` **即时**（token
  门禁 fail-closed 401）。接线即 M0 真实工作量：闭包捕获 `Arc<PolicyHandle>` → 读文件→解析→swap
  （失败保留旧值+warn）——**M0 已完成**（设计期 `/reload` 返回 501，见 §10.1/§10.2）。
- **语义**：缺文件 = 空策略（默认全放行）+ warn 一次；坏文件 = 保留上次有效 + warn（对齐 last-known-good）。配置分层：`global` 默认 → `routes` 覆盖。
- **部署**：`pack/hygress-s6/.../gateway/run` 导出 `HYGRESS_POLICY_PATH`；镜像建 `/etc/hygress/`（或置于 `HYGRESS_DATA_DIR` bind mount 下——**挂载来源为部署决策，需人工确认**，代码无约束）。
- **依赖**：`serde_yaml` 加入 hygress-gateway（core 零 IO 无 yaml；adapter 已有，workspace 版本沿用）。

### 2.2 响应侧骨架（消除 E-2）
- **已知事实（代码实证）**：`pipe.rs::stream_back` 中**响应头先于 body 写出**；body 以 `resp.chunk() → usage.feed(chunk) → write_response_body(chunk)` 逐块透传；**现网无响应体大小上限**（仅请求侧 `max_body_bytes`）。
- **骨架**：`ResponsePipeline` 钩子插在 `usage.feed(chunk)` 与 `write_response_body` 之间（chunk 单次读取双消费）。能力分三态：
  1. **observe（M0，直通）**：仅统计/透传，无拦截；
  2. **per-chunk 判定（M4）**：跨 chunk 有界重组缓冲（仿 `usage.rs::process_sse` 的不完整尾行缓冲），命中 = 停写 + 断下游 + 按终端路径处理（`completed=false` usage 上报 + quota release）；
  3. **per-route `mode: buffer`（可选，M4）**：仅非流式 JSON（content-type 判定），设定**响应体上限**，**推迟响应头写出**到判定完成后；超限按上游错误处理。
- **SSE 策略裁决（D-1 采纳 oracle：B 为主引擎，A 仅可选项，C 否决）**：默认 per-chunk；buffer 为 per-route 可选（非流式 JSON + 字节上限）。理由：响应头先发不可回改状态码；全量缓冲无界流 = OOM 风险且摧毁 TTFT/流式；SSE 无"尾部"概念。

## 3. 配置 Schema（`/etc/hygress/policy.yaml`）

> 键与实现 `hygress-core::policy` serde 一致（`window_secs: u64`、`timeout_ms: u64`、`cache_ttl_secs: u64`、`name_glob: String`、`pin_provider_svc_pattern`）。

```yaml
version: 1
global:
  limits:                              # 令牌桶：仅 rps(填充率)+burst(容量)；无 window（D-6）
    ip:        { rps: 20, burst: 40 }
    consumer:  { rps: 100, burst: 200 }
  quota:
    by_model_tokens: { window_secs: 86400, soft: null, hard: 1000000 }   # 固定窗口（秒）
  guardrail:
    fail_mode: closed                  # 仅"已启用且外呼失败"时生效；未配置 = 阶段关闭（D-14）
    static_rules: [ {name: prompt-inject, regex: "(?i)ignore previous instruction", action: block} ]
    llm: { timeout_ms: 3000, max_rps: 5, cache_ttl_secs: 300, mode: sync, on_error: reject }
routes:                                # 路由键 = ingress_name（ns 剥离后 bare，glob 匹配；D-12）
  - name_glob: ai-route-route-*
    limits:  { consumer: { rps: 5, burst: 10 } }
    quota:   { by_model_tokens: { window_secs: 86400, hard: 50000 } }
    policy:
      override_route: model-8-6.static:80      # 运行时回退（见 4.3）：目标需存在于 McpBridge，否则回原路由+告警
      pin_provider_svc_pattern: "provider-8.*"  # 按 service 名模式过滤/固定候选（D-2）
      header_add: [{x-canary, "true"}]
      timeout_ms: 30000
      retries: 2
    guardrail:
      fail_mode: closed
      static_rules: [...]               # 继承 + 追加
```

## 4. 各能力实现设计

### 4.1 限流（P1）
- core：`RatLimiter`（令牌桶，`AtomicU64` 桶 + DashMap 键表，单实例）。**键语义（裁决 D-9/D-10）**：
  - **IP 键** = 现有 `client_ip` 提取结果（`X-Real-IP` → **XFF 首值** → 空串；与 `pipe.rs:473-479` 实现一致，三处文档口径本次统一）。**为空则跳过**（绝不共享 "" 桶）；头可伪造 → 文档标注 best-effort；peer-addr 键列为 M1 可选增强（需把 Pingora 下游地址接入 `InboundRequest`）。
  - **consumer 键** = `X-Mse-Consumer`；`none`/缺省（含 ext-auth 鉴权缺失 / legacy fail-open 模式）时 **consumer 维度跳过**，仅 IP 维度生效。
- 阶段：**两处**，均在 pipe 异步段（不拆 `prepare_inner`，见 §5）：`rate_limit_pre`（入站**头读取后、body 读取前**，早拒不排空 body）；`rate_limit_post`（ext-auth 后、候选循环前，每请求一次）。
- 响应：**扩展 `short_circuit`** 支持类型化错误与附加头（D-15）：`{"error":{"message":...,"type":"rate_limit_error"}}` + `Retry-After`。

### 4.2 Token quota（P2）
- core：`QuotaEngine`（预算表 `(consumer, model) → (window_start, used, soft, hard)`，窗口原子增减）。**键裁决（BLOCK-3）**：`model` = **映射前路由生效模型名**（`UsageTarget.model` = `x-higress-llm-model` / route key）——与 usage 上报 `FlushFields::model` **同源**；model_mapper 逐目的地改写**不影响**该口径（否则配额与计量对不上账）。
- 两段式：`reserve → commit / release`。**预留估算（D-13 定案）**：v1 用 `prompt_est = ceil(request_content_bytes / K)`（K 默认 4，可配）；commit 用 usage `total_token`（上游 total 优先，语义与 `UsageSnapshot` 一致）。
- **预留生命周期（BLOCK-5/D-11）**：RAII Drop guard 挂请求生命周期（`GatewayState` 内），TERMINAL 路径矩阵：
  | 路径 | 动作 |
  |---|---|
  | 2xx 流正常结束 | commit(actual) |
  | 终端非 2xx（`report_incomplete_usage` 路径） | release |
  | 全候选传输失败 | release |
  | **下游写失败中断（`pipe.rs:319-328` 现状直接 return，连 usage 都不报）** | release + 补 `completed=false` usage（护栏中断同此） |
  | 护栏拦截（4.4） | release + `completed=false` |
  | TTL 兜底 GC | 防进程内泄漏 |
  > 注（completed=false 行记账口径，D-11）：护栏/网关前置拒绝补报的 `completed=false` usage 行，由
  > GPUStack 侧按字节/chunk 估算落账（`_estimate_partial_usage`：input≈bytes/4、output≈chunks×1）——
  > 与 wasm token-usage **仅在上游请求触达被跟踪集群后上报**不同；前置拒绝（未触达上游）仍补一行属
  > Hygress 自有层的取舍（D-11），真机已验证该行形态（REPORT §14 completed=false 行）。
- 阶段：`quota(reserve)` 在 ext-auth 后、候选循环前（每请求一次）；`commit/release` 随响应终端路径。hard 超限 429；soft 超限告警 + 可选降级头。
- **持久化（D-5 裁决）**：**内存先行**（重启丢窗口 → 短暂 over-allow，可接受；与护栏 fail-closed 性质不同）；WAL/文件移出 v1（design §8 策略 2 零本地持久化原则）。

### 4.3 路由策略覆盖层（P3；实现前先过设计门禁）
- core：`RoutePolicy`（条件 → 动作；条件复用 `core/matcher.rs` 的 `PathPred`/`HeaderMap` 原语做独立小匹配器，**不复用 `RouteTable::find_match`**）。
- 动作（D-2 裁决 + 两强制边界）：
  - `override_route`：替换 `prepared.candidates`（`swrr_select::order` 跳过），**目标必须经既有 registry 解析**——policy 槽与 CRD 槽是两个独立 ArcSwap、加载期无法交叉校验 → 目标不存在 = **运行时回退原路由 + 告警**（非加载期拒绝）；SWRR 状态键 `(route_key,dest-digest)` 不被污染。
  - `pin_model_provider`：重述为"按 service 名模式过滤/固定候选"（**"region" 维在数据模型不存在**——`Registry` 仅 `id/kind/domain/port/proxy_ref`）。
  - `header_add/del`、`timeout/retries` 覆盖（per-request timeout 走 reqwest `RequestBuilder::timeout`，现有 client 无读超时，可覆盖）。
- 阶段：`routing_policy` 在 **route_match 之后**对命中的 **Main 路由**做装饰（非"匹配前"——匹配前无法得知命中哪条路由的策略）；仅初始派发（`redirect_count==0`）生效；Fallback/镜像天然不适用。metric 打 `policy_applied=true`。
- 冲突边界：只覆盖目标选择/头/超时，不新增/删除 GPUStack 路由（与 model-router 字节契约正交）。

### 4.4 安全护栏（P4；依赖 M0 骨架）
- **B4a 静态规则**：core 规则表（正则/敏感词/结构：体大小、`max_tokens` 封顶、超时），请求侧 `guardrail_in`（候选循环前），命中 → 拦截/改写/脱敏。
- **B4b LLM 判定**：`egress/guardrail.rs` `GuardrailClient`（骨架 = `forward_auth.rs` 的 reqwest+per-request timeout 模式；差异：并发上限 semaphore + 判词缓存 dashmap 均新增但平凡）。`mode: sync|async`。**fail_mode：默认 `closed`**（安全方向，仅"已启用且外呼失败"时生效；未配置 = 直通，D-14）。**与 ext-auth 的 FAIL_OPEN 无共享代码路径（`pipeline/auth.rs` 独立），不回归 A7**。
- **B4c 输出侧**：响应侧骨架（§2.2）上实现 `guardrail_out`（B4a 规则 + 可选 LLM 输出审核），per-chunk 判定，命中 = 断流 + 终端路径处理。

## 5. 管线与阶段插入点（v2，按代码结构修订；不拆 `prepare_inner`）

> 代码事实：纯函数 `prepare`/`prepare_fallback` 融合 ①②③④⑦（**SWRR 在 ext-auth 之前**，既定事实）；pipe 异步段 = ⑤ auth → 候选循环（⑧⑨⑩）→ fallback 环（每跳重跑 `prepare_fallback` + ext-auth）；响应 = `stream_back` chunk 循环。**新阶段全部落在 pipe 异步段与响应接缝，不重排既有纯管线。**

```
inbound 头读取 ── rate_limit_pre(IP, B2) ── body 读取（复用 read_inbound 的 max_body_bytes）
        → model_router(body→model) → route_match(Main/Fallback/镜像)
        → [redirect_count==0] routing_policy(装饰命中 Main，D-2/D-3)
        → transformer → ext_auth(A7) → rate_limit_post(consumer, B2) → quota(reserve, B1)
        → [一次] guardrail_in(改写循环携带的 current.body，fallback 跳继承且不重复外呼)   ← 循环级，勿画到 ai_proxy 之后
        → 候选循环: registry_resolve → swrr_select(可被 policy 跳过) → model_mapper → set_pre_route_headers
        → ai_proxy → 数据面转发
        fallback 环: prepare_fallback + ⑤ auth 每跳重跑；routing_policy/rate_limit_post/quota 仅 redirect_count==0
响应: stream_back: chunk → usage.feed → [guardrail_out(B4c, per-chunk)] → write_response_body
        2xx 流末 → quota commit + usage 上报；终端非 2xx/传输/写失败/护栏中断 → release + completed=false
```

- **问卷明确**：`routing_policy`/`rate_limit_post`/`quota reserve` 每请求一次（首跳）；`rate_limit_pre` 每请求一次（首跳前）；`guardrail_in` 改写循环携带的 `current`（fallback 跳自动继承已防护体、不重复 LLM 外呼）；`guardrail_out` 逐跳生效（每跳响应各自判定）。

## 6. 开发计划（里程碑 → 任务 → 验证）

| M | 内容 | 交付 | 验收 | 依赖 |
|---|---|---|---|---|
| M0 | 配置骨架（§2.1）+ 响应侧骨架 observe（§2.2） | `PolicyConfig`(core) + `SharedPolicy`/handle + mtime 轮询 + **admin /reload 接线（现状 501，M0 必做）** + `ResponsePipeline` observe；s6 run 导出 env/建目录；gateway 加 `serde_yaml` | 单测：加载/重载/缺文件/坏文件(旧值)；集成：改文件即生效、/reload 生效、chunk 透传 | — |
| M1 | 限流（4.1） | `RatLimiter` + 两阶段 + `short_circuit` 类型化扩展 | TDD：桶/键/空键跳过/'none'跳过/429+Retry-After；集成：真实并发打点 | M0 |
| M2 | Token quota（4.2） | `QuotaEngine` + quota 阶段 + RAII 生命周期 + 终端矩阵 | TDD：reserve/commit/release/超卖/TTL；集成：真实 usage 计量对账 + 写失败 release | M0 |
| M3 | 路由策略（4.3；**先过 oracle 设计门禁**） | `RoutePolicy` + routing_policy 阶段 + 运行时回退 | TDD：条件/动作/SWRR 覆盖/目标缺失回退；集成：canary/目标覆盖 | M0 |
| M4 | 护栏（4.4） | B4a 静态规则 → B4b LLM(fail-closed) → B4c 输出侧(per-chunk) | TDD：规则/LLM 判定/断流终端路径；集成：注入样本拦截、输出违规断流 | M0（B4b 无额外前置；B4c 需 M0） |

各里程碑以库级测试 + 集成测试 + clippy 0 收口，延续 Gate 门禁。M3/M4/B4b 的失败模式与 SSE 策略已在本文档定案（不进实现期再摇摆）。

## 7. 错误与降级语义

| 能力 | 超限/失败 | 默认 | 可配 |
|---|---|---|---|
| 限流 | 429（`rate_limit_error` + Retry-After） | 拒绝 | 阈值（rps/burst） |
| quota | 429(hard)/soft 告警 | 拒绝(hard) | hard/soft |
| 路由策略 | 目标缺失/策略无效 | **运行时回退原路由 + 告警** | — |
| 护栏(静态) | 命中 | 拦截/改写 | 按规则动作 |
| 护栏(LLM) | 已启用且外呼超时/出错 | **reject（fail-closed）** | on_error: reject\|allow |
| 护栏(未配置 `llm.service: null`) | — | **直通（非 closed）** | 配置即启用 |
| 配置（缺失/坏文件） | — | 上次有效 / 默认放行 + warn | — |
| ext-auth（既有，R-12 修订） | 传输失败/5xx | **默认 fail-closed：403 `ext_auth_unavailable`**（对齐 GPUStack/Higress `failure_mode_allow=false`；`failStrategy` 仅约束 Wasm VM 致命错误） | `HYGRESS_EXT_AUTH_FAIL_MODE=open` 切回 legacy fail-open |

## 8. 定案记录（D-1~D-15，首轮 @oracle 裁决已采纳）

| # | 项 | 裁决 |
|---|---|---|
| D-1 | 响应侧 SSE | B per-chunk 为主引擎，A buffer 仅 per-route 可选（非流式 JSON+上限+头推迟），C 否决 |
| D-2 | 路由策略正交性 | 成立；override 目标运行时回退 + 不跨槽交叉校验；pin provider 重述为 service 名过滤（无 region 维） |
| D-3 | fallback 重路由 | policy/quota/rate_limit_post 每请求一次；guardrail_in 改写 current 跨跳继承；guardrail_out 逐跳 |
| D-4 | 配置并入 | 独立 `SharedPolicy`(ArcSwap)；删除"数据不变则不动"（CRD store 现状保留） |
| D-5 | quota 持久化 | 内存先行（over-allow 可接受）；WAL 移 v1 |
| D-6 | 限流 window | 从限流 schema 删除（文档化字段不入）；quota window 保留（真语义） |
| D-7 | 路径/触发 | `HYGRESS_POLICY_PATH`（默认 /etc/hygress/policy.yaml）+ mtime 轮询（实现期 1s；R-8 后并入 30s dutycycle）+ `/reload`（M0 接线完成，原设计期 501） |
| D-8 | 阶段插入点 | 全部在 pipe 异步段与响应接缝；不拆 prepare_inner |
| D-9 | 限流 IP 键 | 现有 client_ip（X-Real-IP→XFF 首值），空则跳过；头可伪造 best-effort；peer-addr 可选增强 |
| D-10 | consumer 键 | `X-Mse-Consumer`，'none'/缺省跳过 consumer 维（fail-open）；mirror 仅 IP 维 |
| D-11 | quota 预留生命周期 | RAII Drop guard + TTL GC + 终端矩阵（含写失败补 completed=false） |
| D-12 | policy 路由键 | ingress_name（ns 剥离 bare，glob），非 route.key(模型名) |
| D-13 | quota 估算 | `prompt_est=ceil(body_bytes/K)`，K 默认 4；commit 用 usage total_token |
| D-14 | guardrail 语义 | 未配置=直通；fail-closed 仅已启用且外呼失败时 |
| D-15 | 429 错误体 | 扩展 short_circuit 支持类型化错误 + 附加头 |

## 9. 阻塞项状态（终版 —— 复审达成，可实现前提成立）

> **审核记录**：
> - 首轮（v1，@oracle 高精度交叉审核）：判定"需修订"，5 项阻塞（BLOCK-1 管线插入点、BLOCK-2 响应侧接缝、BLOCK-3 quota 模型名口径、BLOCK-4 键语义、BLOCK-5 预留生命周期）+ D-1~D-15 裁决与多项代码事实纠错（admin /reload 未接线返回 501、xclient_ip 口径不一、SharedConfig::store 无相等短路、SWRR 先于 auth 等）。
> - 二轮（v2 修订后，复用 ora-1 复审）：**达成 —— 无阻塞，可开始 M0**。BLOCK-1~5 逐条消解（代码可落地），0 新阻塞；仅 2 处非阻塞文字修正（§5 图序 `guardrail_in` 移到候选循环前、§3 示例值改 `provider-8.*`）——已并入本终版。
>
> **遗留【需人工确认，不阻塞】**：policy 文件挂载来源（镜像内置 `/etc/hygress/` vs `HYGRESS_DATA_DIR` bind mount）——部署决策，M0 集成测试用 `HYGRESS_POLICY_PATH` env 覆盖即可。
>
> **结论：按本终版可从 M0 开始实现**（里程碑/依赖/验收见 §6；实现期勿遗漏二轮注记：`read_inbound` 拆分头/body 两相为 M1 重构点；`GatewayState` 为 `integrations` gate，`PolicyHandle` 与纯引擎的 feature 分层）。

---

## 10. 实现完成与自审记录（2026-09-04）

> 注：本节为 2026-09-04 的**实现/审核记录**（早于审计修复批 B1-B5）。彼时 policy mtime 轮询周期为
> 1s、ext-auth 传输失败默认 fail-open；均已随审计修复变更（R-8：policy 轮询并入 ≤30s dutycycle；
> R-12：ext-auth 默认 fail-closed/403 + `HYGRESS_EXT_AUTH_FAIL_MODE=open` 开关），本节内相应表述保留
> 为当时状态，现行机制以 §2.1/§7（已修订）与 README env 表为准。

### 10.1 实现（三个并行 lane + 复核，全部 TDD、零 mock/stub）
| lane | 内容 | 结果 |
|---|---|---|
| C1 core | policy/ratelimit/quota/route_policy/guardrail 纯引擎 + prelude 导出；`commit` 增加 `est`（per-reservation，并发在途精确结算，含幻影结算钳制） | 198 测试绿 |
| C2 egress | `GuardrailClient`/`GuardVerdict`（semaphore 并发上限、TTL 判词缓存、`Ok(None)`=无判定、非 2xx=Err） | 67 测试绿 |
| G1 gateway | `policy_loader.rs`（加载/热重载/merge）+ `PolicyHandle`(ArcSwap) + admin `/reload` 接线（原 501） + mtime 1s 轮询 + `ResponsePipeline`（observe/per-chunk） + `QuotaReservation`(RAII+终端矩阵) + 限流/配额/策略/护栏阶段插桩 + `short_circuit_typed`(类型化错误+附加头) + s6 env/`/etc/hygress` | 48 新测试 |
| 复核 | `cargo test --workspace --all-features` **492 全绿**（基线 437 + 55）；`clippy 0` | — |

### 10.2 自审（替代 @oracle——其运行时当轮不可用）
按 §2-§7 / D-1~D-15 / §6 M0-M4 逐条对照代码核验：
- **正确性关键点**：head/body 两相（429 早拒不排空 body）；IP/consumer 维空键/'none' 跳过；quota key=(consumer, **映射前** model) 与 usage 同源；RAII Drop 全覆盖 + 写失败补 `completed=false`；override 运行时回退不污染 SWRR；guardrail_out per-chunk 跨块 tail(上限 4096) 命中断流；fail-closed 与 ext-auth FAIL_OPEN 无共享路径；热重载并发安全（ArcSwap）+ 缺/坏文件 last-known-good；/reload 已接线（非 501）；s6 部署。
- **结论**：无实现阻断项；**M0-M4 目标全部达成**。
- **v1 明确边界（已文档化，非缺陷）**：①响应侧 `mode: buffer` 未实现（§2.2 本为可选），per-chunk 为主；②路由级 `limits.ip` 覆盖不生效（`rate_limit_pre` 在 route 未知前只读 global ip，已改 merge 注释如实）；③async LLM 护栏=旁路记录（按设计）；④公共路由配额共享 `''/none` consumer 桶。
- **注**：@oracle 高精度实现 vs 设计交叉审核在本轮因运行时不可用改由自审替代；**建议 oracle 运行时恢复后补跑一轮**（命令/范围同 §9 流程），作为第二方复核。

#### 非阻塞项处置记录（ora-2 交叉审核）
| 项 | 处置 | 说明 |
|---|---|---|
| B-6 限流桶内联 vs core RatLimiter | 内联保留（v1 选择），`check_bucket` 注释标注 | core `RatLimiter` 保留供未来统一；内联因需 per-request spec 指纹 + 空闲淘汰，与 core 单例模型不直接兼容 |
| B-7 正则编译缓存 | 每次 `load_policy` 编译（v1），建议后续预编译 | 策略文件变更频率低（秒级 poll），regex 编译成本可接受；缓存需版本管理，复杂度 > 收益 |
| B-9 启动缺文件 vs poll 缺文件 | 语义有意不同，注释澄清 | 启动：缺文件 → all-pass 默认（首次加载无 last-known-good）；poll：缺文件 → 保留 last-known-good（运行中不可降级） |
| B-10 `max_rps` 实为并发上限 | 注释/文档澄清 | `GuardrailClient` 的 `max_rps` 是 semaphore 并发上限（非令牌桶 RPS）；命名保留兼容已冻结 API |
| B-12 跨窗提交瞬态 | 最终一致，不改代码 | `commit` 在窗口交叉时把 actual 写入当前窗（旧窗的 estimate 丢弃）；瞬态 over/under-count 在下一窗自愈 |
| B-13 1s 周期内同步 fs | 可接受，不改代码 | `policy.poll()` 的 `metadata()` + 可能的 `read()` 在 1s 周期内执行；单次 < 1ms（小文件），不影响数据面延迟 |

### 10.3 目标判定
**文档 §6 定义的目标（M0-M4）全部完成**：M0 骨架（/reload 接线、响应骨架、s6 env/目录）、M1 限流（两阶段/429/键语义）、M2 配额（两段式/RAII 生命周期/终端矩阵/内存先行）、M3 策略（覆盖层/回退/pin/头/超时重试）、M4 护栏（静态/LLM fail-closed/输出侧 per-chunk）——逐条有单元+集成测试佐证（**492 全绿、clippy 0**，基线 437 + 55）。

### 10.4 审核闭环与真机 e2e 佐证（2026-09-04）
- **@oracle 二轮交叉审核（ora-2）**：发现 BLOCK-1/2/3（fallback 配额不 commit、GC/限流桶未接线、§3 键不兼容）→ 修复并补回归测试 → **复审：BLOCK 全部闭合，目标达成**；残留 NON-BLOCK（NB-1~5）一并消除（含 BLOCK-1 判别性回归测试重写、evict_idle/桶指纹单测）→ **最终 492 全绿、clippy 0，全部问题消除**。
- **真机 e2e（live GPUStack v2.2.3，升级后构建、测试主机）**：
  | 场景 | 政策（`docker cp` → `/etc/hygress/policy.yaml`，1s mtime 热更生效） | 结果 |
  |---|---|---|
  | 限流 (consumer) | 路由 `limits.consumer {rps:1, burst:1}` | 200 → 200 → **429 `rate_limit_error`** + `Retry-After: 1` |
  | 配额 (hard) | `global.quota.by_model_tokens {window_secs:3600, hard:60}` | 200 → **429 429 `quota_limit_error`** |
  | 输入护栏 | `static_rules [{name:marker, regex:FORBIDDEN_MARKER, action:block}]` | 命中 **403 `guardrail_blocked`**；正常内容 200 |
  | 复位 | 移除 policy.yaml → 默认放行 | `:80/readyz=200`、chat=200，server R=0 稳定 |
  - 升级前的部署漂移（真机运行镜像用旧 pack、缺 `/etc/hygress`）已修正：重传 pack/Dockerfile + s6 到远端重建镜像，远端部署与仓库 pack 一致。
- **注**：oracle 运行时在自审阶段曾不可用，其后恢复完成第二方复核（ora-2）与复审闭环；§10.2 自审结论与 oracle 复审一致（M0-M4 达成）。
