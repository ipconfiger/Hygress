# Hygress Oracle 第二方终审复核报告（ora-2，HEAD 19ee0fb）

> 性质：dev-process §10.6.2 / audit-repair-plan §9 预留的 **oracle 第二方复核兜底**正式补跑。
> 方式：5 个全新独立子代理（无本仓库修复历史先入立场、互不共享上下文），按 dev-process §5.1 oracle
> 方法论分别对**代码成熟度/架构 · 代码质量/正确性 · 性能/资源 · 可运维/可观测 · GPUStack 集成保真**
> 五个维度做只读高精度交叉审计；主控对全部 MAJOR 逐一二次取证（读码核验 file:line）。
> 结论：**无 BLOCK；判词 CONDITIONAL-APPROVE**（放行现有状态，条件=清除 5 项正确性/运维 MAJOR + 文档同步，
> 见 §4 条件清单）。

---

## 1. 五维结论总表

| 维度 | 代理审计 | 评分 | 判词 | MAJOR | MINOR/NOTE |
|---|---|---|---|---|---|
| 代码成熟度/架构 | 独立实测复跑 | 8/10 | CONDITIONAL-APPROVE | 1 | 9 |
| 代码质量/正确性/测试 | 独立精读（核心引擎+测试实测 206/206） | 8/10 | CONDITIONAL-APPROVE | 2 | 15 |
| 性能/资源效率 | 独立静态（7 项优化声明逐项核验） | 8/10 | CONDITIONAL-APPROVE | 1（效率类） | 8+2 |
| 可运维/可观测/安全运维 | 独立静态（含真实日志 fixture 佐证） | 7/10 | CONDITIONAL-APPROVE | 1 | 14 |
| GPUStack 集成保真 | 独立静态（20 条 pin 契约逐条核对） | 8.2/10 | CONDITIONAL-APPROVE | 2 | 9 |
| **加权** | — | **≈7.9/10** | **CONDITIONAL-APPROVE（无 BLOCK）** | **5 项去重后** | 文档漂移为主 |

三个独立维度（成熟度/集成/可运维）不约而同指向同一处 MAJOR（IngressClass 写点），交叉印证度高。

---

## 2. 去重后 MAJOR 清单（全部经主控读码复核）

### AM-1 唯一 kube 写点在拓扑 A 也无条件执行 —— 与"只读控制面"不变式冲突（3 审计交叉）
- 证据：`crates/hygress-adapter/src/lib.rs:178-181`（`Controller::run` 内无条件 `ensure_ingress_class`）；
  `reconcile.rs:85-99`（`get_opt`→不存在即 `api.create`）。bootstrap.rs:530-533 的 `topology_b` 门控只包住
  预种子 `seed_ingress_class()`（lib.rs:309-337），`run()` 内种子对所有拓扑执行 → 双路径、门控形同虚设。
- nuance（主控取证）：`design.md:299-300` 明言"embedded（拓扑 A）…播种无副作用、**推荐统一做**，以备 external
  切换"——代码行为**符合设计本意**；矛盾在模块注释（lib.rs:15-17、client.rs:12-13 声称"仅 topology B 例外"）与
  实现不符；真机 fixture（operability m11）显示拓扑 A 下内嵌 apiserver 对该 create 返回 **405** → 实际写不成功、
  仅每 boot 一条 warn 噪音。
- 修复（二选一）：① run() 内按 topology_b 门控（Controller 构造注入标志），删 bootstrap 重复预种子，注释与
  README"只读"口径统一；② 保持无条件（贴 design），改为：模块注释改写为"按 design §5.2 统一播种，拓扑 A 下
  通常 405 容忍"，并消除双路径。**建议 ①**（让声明=代码=最强不变式）。
- 验收：非 topology_b 下无任何 apiserver 写（守卫测试/真机日志无 seed 行）。

### AM-2 流式计量不强制 `stream_options.include_usage` → SSE 行 completed=false + 估算，偏离 wasm 基线（集成 MAJOR-1）
- 证据：pin §2.8（plugin-contract-pin.md:394）钉死 wasm"`stream_options.include_usage` forced on"；全仓
  crates 内 grep `include_usage|stream_options` **0 命中**（出站请求体字节透传，pipe.rs:1412-1458 只 feed+透传，
  pipeline/mod.rs:253-345 无注入点）。
- 外部佐证（本轮 web）：[Higress ai-proxy 对 OpenAI `/v1/chat/completions` 流式自动注入 include_usage](https://github.com/alibaba/higress/blob/main/plugins/wasm-go/extensions/ai-proxy/main.go)
  （[PR #4258 提供 disableStreamUsageStats 关闭项](https://github.com/higress-group/higress/pull/4258)、
  [PR #2524 限定仅 OpenAI chat/completions](https://github.com/alibaba/higress/pull/2524)）；GPUStack 服务器侧对
  `completed=false` 且 token 为空的行按字节/chunk 估算（[env 文档](https://docs.gpustack.ai/2.2/environment-variables/)
  `GPUSTACK_USAGE_ESTIMATED_BYTES_PER_INPUT_TOKEN=4` / `..._TOKENS_PER_OUTPUT_CHUNK=1`；[metrics_collector.py
  `_estimate_partial_usage`](https://github.com/gpustack/gpustack/blob/70d0eff7/tests/server/test_metrics_collector.py)
  completed=False → input=bytes/4、output=chunks×1）→ 流式请求若客户端未自设 include_usage，Hygress 上报
  completed=false 空 token 行，GPUStack 落**估算**而非精确 token，与真实 Higress 行内容不一致（计费/报表数据源）。
- 修复：出站对"候选目标 + json + 顶层 `stream==true` + 无 `stream_options` 键"做有界注入
  `{"stream_options":{"include_usage":true}}`（复用 body.rs 改写机制、受 R-5 恒等短路保护；参照上游限定
  chat/completions 路径，避免对不支持接口注入）；补集成断言"SSE 无 include_usage 客户端 → completed=true 精确行"。
  若决策不做：pin §2.8 + equivalence 文档显式降级记录。
- 验收：新增注入单测/集成；现有 SSE 计量用例全部 completed=true。

### AM-3 下游 body 读取 Err 被当"body 结束" → 截断请求照常进管道派发上游（质量 MAJOR-1）
- 证据（主控读码确认）：`pipe.rs:834` `while let Ok(Some(chunk)) = ...read_request_body().await` —— Err（下游
  中断/协议错）退出循环后仍 `Ok(Bytes::from(buf))`（:843）返回半截 body，无 abort 区分。
- 修复：Err 视为 aborted——不派发、短路关闭连接（set_keepalive(None) + 直接返回错误），与 BodyTooLarge 分支同构。
- 验收：新增"客户端发完头即断 → 不派发上游"终端路径集成测试。

### AM-4 fallback 目标存在性在 sanitize 前校验 → 级联悬空（质量 MAJOR-2）
- 证据（主控读码确认）：`config.rs:140-147` 用 `self.routes`（全集）校验 fallback 目标；被引用 Fallback 路由若
  自身校验失败被丢弃（:157-161 仅 accepted_routes 收录无 issue 者），引用它的 Main 路由仍存活 → 悬空 fallback。
- 修复：fallback 目标对 sanitize 后 accepted 集复检（收集后二次校验，报告"引用已丢弃目标"）。
- 验收：构造"目标路由自身损坏 + 主路由引用"fixture → 主路由被拒或悬空被明确拒绝，单测。

### AM-5 指标口径不完整：短路/拒绝路径不计 requests_total/duration（可运维 MAJOR-1）
- 证据：`metrics.rs:110` help 声明 "Requests by status and kind"，但限流 429（pipe.rs:251-260/421-434）、
  auth 401/403（:383-410）、配额 429（:452-462）、护栏 403（:506-516）、no-route 404 / registry 503
  （:313-315）均只记专用 counter、不记 requests_total/duration → 4xx/5xx 错误率面板与 SLO 系统性少计。
- 修复：计数收敛到 `short_circuit_typed` 单一出口统一 record_request(status,kind)+duration；413 的 kind 标签
  语义修正（m1）；404/503 口径在 help/文档写明。
- 验收：短路路径/metrics 集成断言（每类拒绝均有 status 计数）。

### AM-6 请求/响应头 3-4 代物化 + 每请求 2 次全表深拷贝（性能 MAJOR，效率类非正确性）
- 证据：pipe.rs:792-799（第 1 代 String 拷贝）→ pipeline/mod.rs:104-106 + transform.rs:41-43（make_mut 深拷贝①）
  → mod.rs:269-312（深拷贝②）→ pipe.rs:1142-1165（第 2 代 value 物化）；auth.rs:95-105 全头重建为 http map（第 3 代）；
  响应侧 pipe.rs:1378-1393 每头 2×String。14k req/s 量级 ≈ 百万级分配/s。
- 修复（下轮扩容前）：hop-0 消费所有权（strip 前先判存在，fallback 再 clone）；auth 仅构造 allowlist 7 头；
  reqwest 侧传 HeaderValue 引用；评估 COW 语义改造。F2-F10 MINOR 一并（见 §3）。

---

## 3. 主要 MINOR 聚合（跨维度去重，非穷尽）

- **文档/叙事漂移（体量最大、成本最低）**：policy 热载"1s"残留（bootstrap.rs:465-467 注释、policy_loader.rs、
  extensions-design §7、README:233）vs 实际 ≤30s（README:13/audit-fix-report:36 正确面）；README 测试计数
  538 vs 368 分表/对比表、dev-process 368/492 阶段数残留；design.md 头部仍标 v1.2 草案（实际 v1.5）；usage 17 字段
  分组标签打架（9+8 vs 11+6）；大量 "P5-pending/frozen-contract" 过时注释；GPUSTACK_API_PORT 默认 80 vs launcher 30080、
  POLL_INTERVAL/JWT_SECRET_KEY 不在 README env 表；ext-auth FAIL_OPEN 旧表述（design.md:349/452/492、pin §2.1:133-135）
  未随 R-12 更新；forward_auth HIGRESS_EXT_AUTH_TIMEOUT_MS 悬空 knob（代码不读）；15020/readyz 表述不准（stats 仅
  /stats[/prometheus]，/readyz 404）；usage 34/7 vs 34/5 真机跑数漂移（README 未更新为 B5 实测 34/5）。
- **行为小项**：guardrail/前置拒绝行会上报 completed=false usage 行（与 wasm"未触达集群不上报"不同——extensions-design
  D-11 有意设计，需 equivalence 文档注明取舍）；usage_sink 对 401 等 4xx 也重试 3 次（建议 4xx 不重试）+ 队列满丢行
  无计数（建议 usage_push_dropped_total）；provider URL 改写失败静默吞掉（set_host/set_scheme 返回忽略）；
  第 2+ Mirror 路由静默死配置；quota release 跨窗分支清零同键 in-flight est（瞬态，总量自愈）；墙钟补给/窗口索引
  （建议单调时钟仅 wall-clock 记 started_at/completed_at）；admin token 非常量时间比较（建议 subtle）；无 panic hook /
  无 adapter liveness（建议 hook + `snapshot_last_store` gauge 或心跳 counter）；结构坏快照每 30s 重试 → reject 计数
  按时间通胀 + warn 刷屏（建议拒绝指纹退避或降级 debug）；TLS watcher 重写 PEM 目标永不被重读（注释/删除动作）；
  env/私钥 environ 暴露无文档警示 + /tmp 残留旧 pid 私钥目录 + 写-改 0600 小窗口（建议先 0700 目录 + OpenOptions 0600）；
  access.log 仅 touch 无写入（建议删除 touch 或接可选 per-request 结构化日志）；s6 README 与 gateway/run 日志落点漂移
  （实际 `${GPUSTACK_DATA_DIR}/log/hygress.log`）；ROLLBACK.md 声称含 supercronic 原件但 .dist 未含；rustls 双 crypto
  provider（aws-lc 死重，建议 default-features=false + ring）；serde_yaml deprecated；serde_yaml/prometheus/rand 多版本；
  percent 无 0..=100 上界（畸形权重 debug panic 可杀 adapter 任务，建议钳制+checked sum）；usage tail 无字节上限 +
  Unknown 模式累积重 parse（建议 1-4MiB 上限）；auth 401/403 拒绝路径不中继鉴权服务实际状态/body（MINOR-3，Envoy
  [DeniedHttpResponse](https://www.envoyproxy.io/docs/envoy/latest/api-v3/service/auth/v3/external_auth.proto) 语义支持
  透传——equivalence 文档记录或扩展契约）；provider 多 token failover/健康探测未实现（design:453"失败转移"字面超卖，
  改述 v1 范围）；pin 版本 1.1.0 vs 真机 1.1.1 + C1"pin 升级重跑检查单"条目未落地；McpBridge 非 default 桥无 name
  过滤（建议过滤 + C1 同构 warn）；header 值非 UTF-8 write-back 静默丢弃；adapter 注释里 read-err→warn 噪音见 AM-1。
- **测试缺口**：候选层 failover E2E（`!is_last && i<retry_cap`）、timeout 触发重试 E2E（is_timeout 未实证）、
  写失败/断流中断 E2E、max_redirects=10 耗尽 E2E、429/500 不重试判别 E2E、guardrail 4096/30s 直接判别测试、
  并发 store/读测试缺失。

---

## 4. 修复声明独立复核汇总（B1-B6 主张，非采信文档）

| 声明 | 复核 |
|---|---|
| retry 语义（触发集 + non_idempotent 修饰门 + tries 封顶 + timeout 透传） | ✅ 属实（边界：单候选路由即使 tries>0 也不重试——R-7 直通下与 next-upstream-tries 语义有差，MINOR-9，建议文档化或允许单候选重试） |
| adapter 收敛（指纹仅成功推进 / 每唤醒 sync_once / LIST 失败保旧） | ✅ 属实（含 rv==0 全量重建保险） |
| quota 契约（时间全注入 / gc_stale 文档更正 / usage 饱和） | ✅ 属实（core 零时钟实测；saturating 判别性测试实测通过） |
| C4（snapshot 计数 + /config 脱敏） | ✅ 属实（含 `config_redacts_secrets` 回归测试） |
| R-5/R-6/R-7/R-8（恒等短路/捕获组复用/单目的地 SWRR/节律超时缓存上限） | ✅ 属实（均无 O(1)→O(n) 反效果；R-5 在 Body-driven 下冗余第二次 O(body) 扫，F2） |
| R-9（raw spec 移除 / timing 诚实降级 / SniStore 最小接线 / listenerPort / find_subseq） | ✅ 属实（SniStore 功能受限已文档化，0.8 文件式 add_tls 单默认 PEM） |
| R-10/R-11/R-12（C1 漂移告警 / TLS 指纹闭环 / ext-auth 默认 fail-closed） | ✅ 属实（C1 pin 检查单文档条目未落地 → MINOR） |
| R-13（env/MSRV/README/测试数） | ⚠️ 部分属实：538/clippy 0 实测复现；但 README 分表 368、design 头部 v1.2、1s 热载等漂移未收净 |
| B5 真机矩阵 | ⏸ 未复验（无真机环境；仓库 fixture/报告为历史证据链，34/7 vs 34/5 两跑数漂移提示 README 未同步最新） |

---

## 5. 判词与条件

**无 BLOCK → CONDITIONAL-APPROVE（≈7.9/10）。** 放行现有状态（538 测试全绿 / clippy 双模式 0 / 无 feature 编译
通过均由成熟度代理本会话实测复现）；但在以下条件清除前不建议宣布"最终 APPROVE / 上生产"：

1. AM-1：IngressClass 写点收敛（建议 topology_b 门控 + 双路径去重 + 注释/README 统一）；
2. AM-2：SSE include_usage 注入（或 pin/equivalence 显式降级记录）；
3. AM-3：下游 body Err → abort 短路（+集成测试）；
4. AM-4：fallback 目标 sanitize 后复检（+fixture 单测）；
5. AM-5：指标统一出口覆盖全部短路路径（+断言）；
6. AM-6：头物化预算复核（可随下轮扩容）；
7. 文档漂移批量同步（§3 清单：1s/30s、计数、design 版本、FAIL_OPEN 表述、env 表、日志落点、15020 口径等）；
8. 补 §3 测试缺口（failover/timeout/写失败/redirect 耗尽判别 E2E）。

建议组织为 B7 批次（AM-1..5 + 文档 + 判别测试）→ 门禁 → 真机回归（新增 SSE include_usage 与 auth-断流 403 用例）→
终审 APPROVE。

---

## 6. 诚实声明（未验证项）

- 未复跑：真机矩阵、CRD 逐字节一致（需活集群）、14k req/s 基准、MSRV 1.89 实际构建、RUSTSEC/许可审计；
  性能分配计数与 prometheus 锁行为为静态推断。
- 未实证外部运行时事实：vLLM/llama-box 对无 include_usage SSE 是否真不返回 usage（决定 AM-2 现实后果；代码侧
  "pin 声明强制 vs 实现无强制"差异确定）；GPUStack 估算器对 completed=false 行的精确记账已由上游测试佐证（§2 AM-2）。
- wasm 二进制级语义（authorization 转发、model-mapper key 门控、前置拒绝是否产生行）以仓库 pin + 真机记录为据，
  无独立二进制复验。
- 各代理标注的抽查文件（body.rs/translate.rs/pipe.rs 等大文件部分段落）非 100% 逐行精读。
