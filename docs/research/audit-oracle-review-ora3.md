# Hygress Oracle 第二方终审复核报告（ora-3，HEAD dab5177）

> 性质：dev-process §10.6.2 / audit-repair-plan §9 预留的 **oracle 第二方复核**第二次正式补跑。
> 基线：ora-2（docs/research/audit-oracle-review.md，HEAD 19ee0fb，加权 ≈7.9/10，CONDITIONAL-APPROVE，
> MAJOR 5 项）→ B7 修复提交 bc9d5cd（AM-1..AM-5 + MINOR 集）+ watcher 退避 bfb783e → 本轮 ora-3。
> 方式：5 个全新独立子代理（无修复先入立场、互不共享上下文），按 dev-process §5.1 oracle 方法论对
> **代码成熟度 · 代码质量/正确性 · 性能/资源 · 可运维/可观测 · GPUStack 集成保真** 五维做只读高精度审计；
> 主控对全部 1 项 MAJOR 与关键修复声明逐一二次取证（读码核验 file:line）。仅一处真机复核声明（watcher
> 日志节律、real-box metrics）因本轮无真机会话、仅做静态一致性核验，见 §8 局限。
> 结论：**无 BLOCK；判词 CONDITIONAL-APPROVE**（加权 ≈8.0/10，较 ora-2 +0.1；唯一 MAJOR 收口后即可
> APPROVE，条件清单见 §6）。

---

## 1. 五维结论总表

| 维度 | 代理审计 | 评分 | 判词 | 新 MAJOR | 新 MINOR / INFO |
|---|---|---|---|---|---|
| 代码成熟度/架构 | 独立静态（全仓 unwrap/死代码/门控/测试清点） | 8/10 | CONDITIONAL-APPROVE | 0 | 5 / 5 |
| 代码质量/正确性/测试 | 独立精读（5 条不变式抽查全 PASS） | 8/10 | CONDITIONAL-APPROVE | 0 | 6 / 4 |
| 性能/资源效率 | 独立静态（热路径逐级成本清单 + B7 diff 比对） | 7.5/10 | APPROVE-with-minors | 0 | 2 / 6 |
| 可运维/可观测/安全运维 | 独立静态（含运维 runbook 走查） | 7.5/10 | CONDITIONAL-APPROVE | 1 | 7 / 5 |
| GPUStack 集成保真 | 独立静态（对上游 ai-proxy/gpustack 源码逐条佐证） | 9.0/10 | APPROVE-with-notes | 0 | 5 / 4 |
| **加权** | — | **≈8.0/10** | **CONDITIONAL-APPROVE（无 BLOCK）** | **1 项（去重后）** | **20 项去重后** |

较 ora-2（≈7.9）：集成维 8.2→9.0（五项修复经上游源码佐证全部闭环）拉高均值；性能 8→7.5 与可运维 7→7.5
反映 B7 修复核实为真、但新暴露的效率类小问题（PX-1/PX-2）与控制面观测仍是短板。三个独立维度（成熟度/可运维/
集成）不约而同指向控制面可观测性缺口，交叉印证度高。

---

## 2. ora-2 修复声明复核汇总（五维代理一致 + 主控抽证）

| 声明 | 证据（HEAD） | 判定 |
|---|---|---|
| AM-1 IngressClass 门控 | `adapter/lib.rs:197-201`（run() 内仅 flag 时播种）；`reconcile.rs:98` 为 adapter 全 crate **唯一** `api.create`（主控 grep 证实）；接线 `bootstrap.rs:523-529`（seed=config.topology_b，默认 false config.rs:87） | **VERIFIED**（五维一致） |
| AM-2 SSE include_usage 强制 | `body.rs:240-274` 五道门 + `scan_top_level_stream`（body.rs:416-463，无 DOM 有界扫描）；唯一生产调用点 `pipeline/mod.rs:299-306`；显式 include_usage=false 不被覆盖（body.rs:263-265 + 测试 1244-1255）；e2e integration.rs:624/697 | **VERIFIED**；与上游 Higress `normalizeOpenAiRequestBody` 字节/语义对齐（集成代理佐证），仅一处罕见子情形（有 stream_options 无 include_usage 成员）行为更保守 |
| AM-3 截断体非结束 | `read_body -> Result<Option<Bytes>, BodyReadFailure>`（pipe.rs:977-1033）；`is_abort`→`set_keepalive(None)`+400 短路先于管道（pipe.rs:283-299） | **VERIFIED** |
| AM-4 回退目标重检 | `config.rs:221-248` 二遍丢弃引用已删 Fallback 的 Main | **VERIFIED（留边）** → ORA3-M8 |
| AM-5 短路计量 | `metrics.rs:87` KIND_SHORT_CIRCUIT + `record_short_circuit`（286-289）；pipe.rs 恰好 8 个调用点（256/297/344/419/441/470/503/562），全部先于候选循环返回 | **VERIFIED** |
| MINOR 行为集（usage 上限/预算、usage_sink 4xx 不重试、provider URL 改写告警、admin 常数时间比较、percent/和溢出、镜像去重等） | usage.rs:55-59/424-451、usage_sink.rs:132-166、provider.rs:106-123、admin.rs:96-102、config.rs:141-170/196-219 | **VERIFIED**（各自带测试） |
| watcher 退避 bfb783e | `adapter/lib.rs:318-322`（PERMANENT 60s / TRANSIENT 5s / 30s 每 kind 日志限流）；`NoResourceVersion` 归永久类（332-358）；30s FALLBACK_TICK 兜底（lib.rs:86,254） | **VERIFIED**（主控抽证；真机节律未复跑） |
| AM-6 头物化预算 | pipe.rs 头 String 物化 + COW 深拷贝（约 200-400 小分配/请求）仍在 | **按约定继续挂账**，未回退（两维复核） |
| ora-2 文档漂移批（10 文件） | 外部文档已同步；**码内**注释残留 FAIL_OPEN/P5/1s 轮询文案（见 ORA3-M6/M7） | **PARTIAL** → 收口项 |

---

## 3. 去重后 MAJOR（1 项，主控读码二次取证成立）

### ORA3-MAJ-1 控制面健康是黑盒：无陈旧度/退避/代际指标，任务死亡不可观测（3 审计交叉）
- 证据：控制面唯一出口指标是 reject/skip 计数（`metrics.rs:17-73` ConfigSnapshotCollector）；watcher 退避态、
  快照陈旧度、store 成功数、配置代际**只进限流日志、不进 /metrics**（adapter/lib.rs:336-361,393-453）；
  `bootstrap.rs:533` 的 `tokio::spawn(controller.run(...))` JoinHandle 直接丢弃（主控取证），ready 之后
  controller 任务 Err/panic 无人观察（OX-8：全仓无 panic hook，grep `set_hook|take_hook` = 0）。
- 后果：拓扑 A 六路 WATCH 全部永久失败 + 30s 兜底 tick（设计内）；若兜底 LIST 也失败或 controller 死亡，
  网关**静默服务最后一个快照**，/healthz、/metrics、15020 全绿，路由/模型/TLS 变更后永久陈旧、无告警信号。
- 修复（B9.5 批次）：① 可抓取指标：`hygress_config_last_store_timestamp_seconds`、`hygress_config_store_total`、
  生效路由/registry/TLS-host 计数、每 kind watcher 错误计数（含"降级 tick-only"态）、配置代际 gauge（复用
  ConfigSnapshotCollector 模式）；② panic hook（记日志 + exit(1) 触发 s6 重启）或重生 supervisor + liveness
  gauge；③ 启动时一次性记录生效拓扑（watch 驱动 vs 30s tick 驱动，拓扑 A 收敛≈30s）。GX-6 的
  `snapshot_last_store_ts` 建议并入 ①。
- 验收：拓扑 A 真机 kill controller 任务（或断 kubeconfig）后 30s 内出现可告警的陈旧信号；指标在 /metrics 可见。

---

## 4. 去重后 MINOR（20 项，跨维合并后按主题分组）

### A. 配置/启动静默失败（可运维向）
| id | 来源 | 证据 | 问题 → 修复 |
|---|---|---|---|
| ORA3-M1 | MX-1 ∧ OX-6 | gateway/config.rs:105-172；bootstrap.rs:395-401 | 环境变量解析失败一律**静默回退默认值**无告警：`HYGRESS_TOPOLOGY_B=treu`→false（种子消失）、`GATEWAY_TLS_PORT=abc`→443、畸形时长→1s。→ 逐键告警 + 启动时打印（脱敏）有效配置摘要 |
| ORA3-M2 | OX-2 | policy_loader.rs:57-71,110-131,157-174；Dockerfile.hygress:28-29 | 策略文件缺失时 boot **静默全放行**（镜像自带空 /etc/hygress，正常启动即全放行无日志）；`/reload` 缺文件→装入全放行默认且返回 **true**（admin 200"已重载"，admin.rs:120-122）。挂载/拼写错误静默关掉限流/quota/guardrail。→ boot 缺文件 WARN；reload"由有变无"保留 last-known-good 返回 false |
| ORA3-M3 | OX-3 | pipe.rs:681-707 | 回退预算耗尽（10 跳）与链终止无日志/无计数，运维无法区分"回退 k 跳后失败"与"直连失败"。→ `hygress_fallback_exhausted_total` + 一条带 route key/redirect_count 的 warn |
| ORA3-M4 | MX-I3 ∧ OX-7 ∧ GX-9 | usage_sink.rs:109-124,140-156 | usage 行丢弃（队列满/冲刷任务退出/最终失败）仅日志无计数；1024 深队列优雅停机不排空。→ `usage_push_dropped_total` + 停机 best-effort drain |
| ORA3-M5 | OX-5 | policy_loader.rs:16-18,76,181；bootstrap.rs:465-489；design.md:243-285；README:119 | 注释与文档仍写"1s 轮询"，实际唯一调用点跑在 30s 周期；README:119"WATCH ≤1 事件周期生效"对拓扑 A 不成立（实际 ≤30s tick）。→ 注释清扫 + 按拓扑限定生效延迟表述 |

### B. 文案/悬空旋钮（R-12 后残留）
| id | 来源 | 证据 | 问题 → 修复 |
|---|---|---|---|
| ORA3-M6 | MX-3 ∧ Q5 ∧ OX-4 ∧ GX-4 | forward_auth.rs:23-35,138,164,171；pipe.rs:17/lib.rs:14；pipeline/auth.rs:2,56,77 | 三向残留：① forward_auth 日志无条件写"(FAIL_OPEN)"，而 R-12 默认 fail-closed 下实际 403（pipe.rs:432-449）且无日志——排障默认配置 403 只见误导行；② pipe.rs:17/lib.rs:14 模块头仍写 FAIL_OPEN 为默认；③ P5-pending 标记挂在已实现代码上；④ `HIGRESS_EXT_AUTH_TIMEOUT_MS`（GPUStack 真写此 env，fixture timeout:30000）**从未被读**，30s 为硬编码常量。→ 改文案（"auth 不可用，由调用方按策略裁决"）+ 落地 env/CRD timeout 解析或摘除悬空命名 |

### C. 正确性边角
| id | 来源 | 证据 | 问题 → 修复 |
|---|---|---|---|
| ORA3-M7 | MX-2 | core/config.rs:221-248 | AM-4 重检是**固定键快照的单遍扫描而非不动点**：Fallback 自身携带的 fallback 目标在遍中被删时，先前已判的引用 Main 存活 → 快照仍可含悬空回退（dispatch 时 404）。当前配置只到深度 1、可达性低，但与声明的"无悬空"不变式不符。→ 循环至无删（有界）+ 链式 fixture 测试（Main→fb1→fb2(坏)） |
| ORA3-M8 | Q6 | transform.rs:250-262,310-317；pipe.rs:689-693 | 客户端可伪造 `x-gpustack-original-path`：入站剥离集不含它、backstop `Backup` 是 **append**、回退跳 `get()` 取**首个**值 → 客户端值压过 backstop，攻击者可控制回退重派发的 :path（谓词不中时落入 mirror）且该头未被 HOP_BY_HOP 剥离、原样上送。→ 入站先剥离或 backstop 改为覆盖 |
| ORA3-M9 | Q7 ∧ GX-7 | pipe.rs:622-638,1704-1714 | 中途写失败终止（下行断开）两处缺陷同源：① 不记 `record_request`/时长（B4c 截断分支记了）→ `hygress_requests_total` 缺该类终止；② 报 usage 用**全新空快照**——若 usage chunk 已先被上游吸收则 token 丢弃、落 completed=false/0 行 → GPUStack 按估算回填。→ 该终止记指标；写失败路径冲刷**活快照**（completed=seen_any）+ 单测"中途 usage 后断连" |

### D. 结构/质量（去重收敛）
| id | 来源 | 证据 | 问题 → 修复 |
|---|---|---|---|
| ORA3-M10 | Q1 | body.rs:68-164 vs model_mapping.rs:83-123；find_subseq/replace_bytes 三份（body.rs:280-311、model_mapping.rs:124-157、usage.rs:787-803） | body 改写/字节工具跨 crate **三份复制**，两套独立 multipart 扫描器 → 解析行为静默分叉风险（大小写/边界处理）。→ core 抽共用工具，body.rs 收敛为唯一改写库 |
| ORA3-M11 | Q2 | body.rs:346-398 vs 416-463 | 两个顶层 JSON 对象扫描器是同一成员循环状态机、仅 key 分派不同；serde 语法规则要改两处。→ 抽单一成员迭代器 + 每 key 回调 |
| ORA3-M12 | Q3 | pipe.rs:1331-1354,1446-1461,946-949,1571-1576；HOP_BY_HOP mod.rs:365-376 vs SKIP pipe.rs:1549-1555 | 出站头构建两处重复 + 三套互不一致的头部过滤（lossy-drop / 静默丢 / 静默丢）+ 三份剥离清单 → 非 UTF-8 策略无统一文档。→ 单一拷贝助手 + 单一策略 |
| ORA3-M13 | Q4 | pipe.rs:810-850 | 错误体系碎片化：`BodyReadFailure` 以"GatewayError 已 frozen"为由本地私有化（实际无冻结机制），与 `GatewayError::BodyTooLarge`(413) 平行产出 413；`Other(String)` stringly。→ 读侧变体并入单一 enum，撤销"frozen"说法 |

### E. 性能（B7 自身引入的效率小回归）
| id | 来源 | 证据 | 问题 → 修复 |
|---|---|---|---|
| ORA3-M14 | PX-1 | pipeline/mod.rs:299-306；body.rs:416-463；mod.rs:117-145 | 模型路由 JSON body 每请求被遍历至多 **3 次**（extract_model / model_field_equals / AM-2 扫描），stream=true 时再叠一次整 body splice 拷贝；AM-2 扫描按 failover 候选重复执行。均 O(n)、有界（≤8MiB），但无跨阶段共享状态。→ 合一次 prepare 期扫描并返回 (stream_true, has_stream_options, 收尾花括号)，AM-2 门与 model-mapper 共用；每请求一次 |
| ORA3-M15 | PX-2 | pipe.rs:1614；usage.rs:412-455 | 响应侧 usage 分类对**每个非 SSE 2xx JSON body** 在首块写出前内联做整 DOM 解析（TTFT 代价），包括永不报 usage 的 mirror 流。→ 镜像/透传路径跳过累积；非 JSON content-type 预过滤 |

### F. GPUStack 集成保真
| id | 来源 | 证据 | 问题 → 修复 |
|---|---|---|---|
| ORA3-M16 | GX-1 | snapshot.rs:85-89,108-116；lib.rs:243-249 | GPUStack 写 `higress-config`（超时）/`higress-https`/`higress-ca-root-cert` 三个 ConfigMap **无 managed 标签** → 按标签选择器永不列出，`configmap_to_timing`（translate.rs:938-1004）在真机是死代码、R-9③ 绑定告警永不触发；与 equivalence A1:30"已消费"声明矛盾，且若将来从快照值强制超时会在真机静默跑默认值（1800/10）。→ 按名列出 higress-config（McpBridge 先例）或文档显式降级并修正 equivalence |
| ORA3-M17 | GX-2 | config.rs:146-148；README.md:197；pack 无设置 | 拓扑 B 种子仅环境变量、pack/launcher 未接线：GPUStack **external 模式**缺 `higress` IngressClass 会直接 raise（boot 失败），漏设 HYGRESS_TOPOLOGY_B 的部署者得到费解的失败。→ external/拓扑 B 部署文档醒目提示（或 pack env 文档补 flag） |
| ORA3-M18 | GX-3 | pipeline/mod.rs:299-306 vs 上游 ai-proxy | 注入无按目标协议/类型的区分（上游仅 OpenAI 协议非 generic、apiName 在 chat/completion 才注入）；generic 目标接严格引擎（vLLM<0.4.3 类）时 Hygress 多注入 → 基线没有的 400。GPUStack 自身写出的路由（type=openai）不受影响。→ 目标可判协议时 generic 跳过；否则文档化超集 |
| ORA3-M19 | GX-5 | bootstrap.rs:229-240；translate.rs:880-911 | 真实内嵌 Higress 默认安装即 :443 自签证书页；Hygress 仅在有 managed `gpustack-tls-*` Secret（GPUStack **仅 --ssl 配置时才写**）时绑 443 → **默认安装 https://host:443 直接 connection refused**。证书名语法与托管路径完全对齐（ssl 开启场景无问题）。→ 文档化行为差 + 真机默认安装核对；可选：无 Secret 时生成自签兜底证书 |
| ORA3-M20 | MX-4 | gateway/Cargo.toml `default=[]`；lib.rs:42 | `cargo build -p hygress-gateway` 默认产出**数据面完全缺失**的二进制（仅 admin/stats），无任何告警 —— 历史 P5 拆分遗留的静默陷阱，错件可直接被镜像打包。→ 翻转 `default=["integrations"]` 或 bootstrap 检测未开 features 时大声失败 |

---

## 5. 不变式抽查结果（质量代理 5/5 PASS）

| 不变式 | 结果 | 要点证据 |
|---|---|---|
| (i) include_usage 恰一次注入且幂等 | PASS | 唯一生产调用点 mod.rs:299；prepared.body 不可能携带该成员（再注入不可能）+ 门 5；字节级恰一次断言 body.rs:1206-1222、mod.rs:610-641；mirror/透传被排除 body.rs:246-257 |
| (ii) 回退不选刚失败主目标/已删镜像 | PASS | by_fallback_key 只索引 Fallback（config.rs:823-825）；AM-4 二遍删悬挂引用（221-248）；镜像只装第一个（829-831）；预算 fallback.rs:27-46 |
| (iii) usage 解析预算不被块边界绕过 | PASS | 仅 `}` 结尾才尝试且 ≤128 次（usage.rs:433-451）；Unknown 态保留缓冲 ≤1MiB（424-432）；SSE 单行上限 + 跳行（556-646）；锚定数据探测 O(new)（733-762）。注：SSE 稳态对含 usage 的每 data 行做 O(line) DOM —— 每行有界、恶意上游跨流无界（上游受信，仅注记） |
| (iv) 短路路径一致记指标且跳过管道 | PASS | 8 个 record_short_circuit 点全部先于候选循环返回；count+duration 同记（metrics.rs:286-289）。注：中途写失败终止（非短路类）漏记 → ORA3-M9 |
| (v) quota 与请求体同一 Bytes、无 TOCTOU | PASS | 体一次性缓冲（pipe.rs:977-1033）；决策与执行读同一 Bytes（pipe.rs:493）；出站为同缓冲的 clone/splice；唯一决策后差量是 +43B 注入 splice |

---

## 6. 条件清单（收口后放行 APPROVE）

1. **ORA3-MAJ-1**：控制面指标（陈旧度/store/每 kind watcher/代际）+ panic hook + 启动拓扑日志（B9.5 批次 A）。
2. 静默配置批：ORA3-M1（env 告警 + 启动摘要）、ORA3-M2（策略 boot/reload）、ORA3-M3（fallback 耗尽）、ORA3-M4（usage 丢弃计数）。
3. 文案收口批：ORA3-M6（FAIL_OPEN/P5/ext-auth timeout 旋钮）、ORA3-M5（节律 1s→30s + README 拓扑限定）。
4. 正确性批：ORA3-M7（AM-4 不动点 + 链式测试）、ORA3-M8（original-path 剥离/覆盖）、ORA3-M9（中途终止记账 + 活快照冲刷）。
5. 结构批（可并入扩容轮）：ORA3-M10/M11/M12/M13（去重、错误体系、撤"frozen"）。
6. 性能批（可并入扩容轮）：ORA3-M14/M15；AM-6 头物化继续挂账。
7. 集成批：ORA3-M16（higress-config 消费或文档降级）、ORA3-M17（拓扑 B 提示）、ORA3-M18（generic 目标注入）、ORA3-M19（默认 443 文档化 + 真机核对）。
8. 真机补验（无真机会话欠账）：AM-2 真实流式引擎判别、GPUStack UI 建路由→立即推理流（≤30s 窗口）、默认安装 443、拓扑 B 链式回退 fixture。

---

## 7. 五维最突出风险（去重后）

1. 配置陈旧不可告警：watcher/controller 死亡 + last-known-good = 任何 GPUStack 路由/模型/TLS 变更后静默错路由（ORA3-MAJ-1）。
2. 默认安装差异面：443 拒绝连接（ORA3-M19）、拓扑 B 种子缺 env 即 boot raise（ORA3-M17）、higress-config 超时静默不消费（ORA3-M16）——三者都是"换装即遇、文档难查"。
3. 保护策略静默消失：缺策略文件全放行 + reload 假成功（ORA3-M2），叠加 FAIL_OPEN 误导日志（ORA3-M6）排障困难。
4. 计费行丢失窗口：中途断连丢已吸收 token（ORA3-M9）、队列满丢弃不可见（ORA3-M4），仅靠 GPUStack 服务端估算部分回填。
5. 结构债复利：三份 body 工具/双扫描器/三套头过滤（ORA3-M10..M13）+ AM-4 单遍边角（ORA3-M7），下轮改动易踩。

---

## 8. 诚实局限

- 本轮为**纯只读静态审计**（无 cargo 构建、无真机复跑）：性能代理明确未重跑基准（14k req/s 平坦性、快照短路
  p99 −23/−25% 仅判"代码一致"）；可运维代理对真机日志节律（+6 行/~80s）与 /metrics 实况仅静态复核。
- 集成代理的三项"诚实未知"维持：AM-2 在真实 GPUStack 流式引擎上的判别（真机 llama-box 忽略 stream 字段，
  仅桩后端 e2e）、拓扑 A ≤30s 收敛对 UI create→infer 流的敏感度、默认安装 443 行为。
- 各代理打分独立，主控仅合并去重并加权平均；INFO 级发现（约 20 项，见附录 B）未逐条二次取证。
- 交叉去重依赖代理各自证据链一致；凡跨维重合项（ORA3-MAJ-1、M1、M4、M6、M9）已在表中并列来源。

---

## 附录 A：维度新发现明细（保留代理原始编号，供追溯）

- 成熟度：MAJOR 0；MINOR MX-1..MX-5；INFO MX-I1..MX-I5（非 UTF-8 值静默丢弃；体读取限 3 动词；
  usage 丢弃未计数；测试缺口——无并发桶/quota、watcher 退避无单测、TLS 轮换负路径无测试、MSRV 1.89 仅声明
  无构建验证）。
- 质量：MAJOR 0；MINOR Q1..Q6；INFO Q7..Q10（中途终止漏计；413 双产出源；Debug 格式键去重 + 头常量大小写
  混用 + Bearer 大小写；request_filter 565 行单体）。
- 性能：MAJOR 0；MINOR PX-1..PX-2；INFO PX-3..PX-7（128 次 DOM 预算最坏情形；指标 RwLock 子句查找 +
  status.to_string；AM-6 挂账复证；SSE 每 chunk 一次写帧；限流/quota 配置时 DashMap 分片锁串行）。
- 可运维：MAJOR OX-1；MINOR OX-2..OX-8；INFO OX-9..OX-13（TLS 单默认证书 SNI + 轮换需重启无 runbook；
  密钥 0600 后写/临时目录不清理；15020 免认证 0.0.0.0 + ADMIN_ADDR 不校验；reqwest 错误 Display 可能带查询串
  泄漏；结构性拒绝每 30s 永续重试以时间计）。
- 集成：MAJOR 0；MINOR GX-1..GX-5；INFO GX-6..GX-9（30s tick 收敛；中途 usage 空快照；modelToHeader 前向
  兼容；usage 丢弃无计数）。

## 附录 B：INFO 级聚合（按修复成本分档）

- 低成本（并入 B9.5 顺手做）：MX-I1（非 UTF-8 告警复用 forward_auth 模式）、MX-I3/M4（计数已并入 ORA3-M4）、
  OX-10（密钥目录 0700 + 退出清理）、OX-12（出站错误 URL 脱敏）、OX-13（同构拒绝指纹去重 + N 次后降 debug）、
  Q9（Bearer 大小写 + 头常量统一 + Debug 键去重替换）。
- 中成本（并入结构/扩容轮）：Q7/M9（已并入 ORA3-M9）、Q8（去双 413 源）、Q10（request_filter 拆分）、
  MX-I4（并发/退避/TLS 负路径测试补齐）、MX-I5（MSRV 1.89 建验证车道或上调声明）、PX-3（预算 128→~16 或
  字节扫描预算）、PX-4（status 句柄缓存）、PX-6（SSE 有界写合并）、PX-7（策略键分片说明）、OX-9/OX-11
  （TLS/15020 文档注记）、GX-6（last-store 指标已并入 ORA3-MAJ-1）、GX-8（modelToHeader 别名）。

---

*ora-3 审核窗口：HEAD dab5177（bc9d5cd..dab5177 之间为 B7 行为 + watcher 退避 + 文档净化提交）。*
*五代理证据基：四 crate 全量读 + B7 diff（4b05b28..bc9d5cd）+ fixture dump.log + pack/s6 脚本 + README/design/
equivalence 文档 + 上游 Higress ai-proxy / GPUStack GitHub 源码佐证。*
