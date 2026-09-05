# Hygress 审计修复计划与方案（v0.1，待高精度复核）

> 依据：五线深度审计简报（成熟度 7.5 / 质量 7.5 / 性能-见文 / 可运维 6.5 / GPUStack 集成 8.5，全部 file:line 取证）。
> 范围决策（用户）：**档位 C —— 全部实施，含治理项 C1/C3/C4**；验收清单 = 修复逐条验收 + 真机/部署验证矩阵两者。
> 流程：本方案经 **oracle 级高精度交叉复核**（无阻塞判定）→ 分批委派实施（每批后 `cargo build/test/clippy` 门禁）→ 全量测试 → 真机验证 → 清单核销。
> 原则：只修不改外部契约；每个子项给 file:line、方案、验收、风险与"延后/文档化"依据；删除/降级项必须显式记录，不静默省略。

## 0. 批次划分与执行顺序

| 批次 | 内容 | 依赖 | 验收门禁 |
|---|---|---|---|
| B1 正确性/安全 | R-1 重试语义 · R-2 adapter 收敛 · R-3 quota 契约与记账饱和 · R-4 C4 计数+`/config`+issue 告警 | — | 单测+集成+clippy |
| B2 性能/死面 | R-5 prepare②恒等短路 · R-6 capture_groups · R-7 单目的地 SWRR · R-8 evict/usage 超时等小项 · R-9 死载荷/死字段处置（features/timing/listenerPort/SniStore） | B1（无代码耦合，可并行评审） | 单测+alloc_guard |
| B3 治理 | R-10 C1 漂移告警 · R-11 C3 TLS 指纹闭环 · R-12 ext-auth fail 语义对齐决策 · R-13 文档/env/元数据对齐（README/测试数/MSRV/日志重定向） | — | 单测+集成+clippy |
| B4 全量验证 | `cargo test --workspace --all-features` · clippy 双模式 · alloc_guard · e2e integration | B1-B3 | 全绿 |
| B5 真机 | GPUStack v2.2.3 测试机 A/B：s6 就绪/端口纪律/usage 落行/CRD/限流配额护栏 e2e/回滚 | B4 | 矩阵 PASS |
| B6 落档 | 修复验收清单核销 + DoD/真机清单核销 + 提交 | B5 | 报告 |

## 1. B1 —— 正确性/安全批

### R-1 重试子系统语义修复（H-A+R1，一条语义单元）
- 事实（audit）：`hygress-core/src/retry.rs:145-149` 把 `NonIdempotent` 当独立 OR 触发；`crates/hygress-gateway/src/pipe.rs:494-580` failover 循环不读 `tries` 上限、`retries:0` 不生效；`pipe.rs:570` `timed_out` 恒 false → `RetryCond::Timeout` 永不触发。外部基准：nginx/Envoy 中 retry_on 定触发集、num_retries 定上限、`non_idempotent` 是修饰符（已 web 核验）。
- 方案：
  1. `RetryCond` 语义改写：`should_retry` 增加正确分层——先按 status/transport/timed_out 判定是否命中"错误类条件"（error/timeout/5xx/status），命中后**再**受 `NonIdempotent` 门禁约束（`non_idempotent=false` 且请求非幂等 → 不重试）。保持枚举与注解解析不变（外部 CRD 契约不变）。
  2. failover 循环以 `min(剩余候选, tries 语义)` 封顶：`tries` 语义 = 首次尝试后的重试次数（对齐 nginx `proxy_next_upstream_tries`，GPUStack 写 2）。`override_retries:0` ⇒ 不退。
  3. `pipe.rs:570` 传输错误分支把 reqwest `Timeout` 错误以 `timed_out=true` 传入（错误分类：`reqwest::Error::is_timeout()`）；读取超时也归类。
  4. `tries` 解析（`retry.rs:101-104`）设上限（≤ 32）并饱和。
- 验收：单测（non_idempotent gate/401 不重试/tries 封顶/timed_out 触发/retries 0）+ 集成 failover 用例；行为回归：真机 AUTHED 模型 200、503→fallback 仍成立。
- 风险：语义变更会影响现有 e2e 预期（如 401 曾换候选被当等效）；在 B5 真机回归清单中单列"重试矩阵"。
- 注：同时修正 audit 指出的"transport error timed_out=false"与超时错误映射。

### R-2 adapter 控制回路收敛（质量 H1）
- 事实：`crates/hygress-adapter/src/lib.rs:368-370` 在 store 成功前推进指纹；`store` Err（结构拒绝）后下一 tick 指纹短路永不重试；`FALLBACK_TICK`（30s）唤醒后无 dirty 则不同步；LIST 失败不置 dirty。
- 方案：①指纹推进移入 `store Ok` 分支；②定时唤醒无条件 `sync_once`（有 rv 指纹短路保护，安全）；③LIST 失败重置 dirty 并短退避（如 1s 内不再立即重试，记录 warn）。
- 验收：单测（结构拒绝 → 下 tick 重试；指纹仅成功时推进）；模拟：`build_snapshot` Err → 恢复后自愈。

### R-3 quota 契约 + 记账饱和（质量 H2/M5a）
- 事实：`core/src/quota.rs:201-220 release()` 内部读 `SystemTime::now()`（`quota.rs:208-211`）破坏"时间注入"契约（`quota.rs:1-4`）；`gc_stale`（`quota.rs:224-235`）生产零调用但文档（`quota.rs:26-30`、`gateway/src/quota.rs:24-25`）宣称是清理后盾；`core/src/usage.rs:433` `input + output` 非饱和。
- 方案：①`release(now_ms, …)` 增加显式时间参数，gateway `QuotaReservation`（`gateway/src/quota.rs:90,111`）改传 `now_millis()`；②清理职责统一为 `evict_idle`，`gc_stale` 删除或改为 `evict_idle` 内部辅助并修订文档；③`usage.rs:433` 改 `saturating_add`；④bootstrap evict 任务保留（间隔见 R-8）。
- 验收：单测全部改注入时钟；新增跨窗/abort + idle evict 用例；无墙钟引用（grep SystemTime）。

### R-4 C4：配置拒绝/跳过计数 + admin GET /config（治理 C4，docs/research/control-gaps-plan.md Phase 1）
- 事实：`hygress-core/src/config.rs:1050-1095` `SharedConfig::new/store` Ok 路径静默丢弃 per-object issues；全仓无计数、admin 无 `/config`（`gateway/src/admin.rs:77-115`）。
- 方案：
  1. core `SharedConfig` 增 `snapshot_reject_total: AtomicU64`、`snapshot_skipped_total: AtomicU64`（core 不引 prometheus）；`new/store` 成功路径返回 per-object issue 数（签名 `Result<usize, Vec<ValidationError>>` 或等价），消除"静默丢弃"。
  2. adapter `sync_once`：store Err → reject+1（含 warn 内容）；结构通过但 per-object drop → skipped+issue 数。
  3. gateway `Metrics` 增 `~25 行 Collector` 包装两个原子 → `/metrics` 与 `/stats/prometheus` 带出 `hygress_config_reject_total` / `hygress_config_object_skipped_total`。
  4. admin `GET /config`（token 门禁 fail-closed）：当前 ArcSwap 快照结构化摘要（routes 概要/registries/proxies 概要/TLS 仅指纹/特性元数据）；**脱敏**：不输出 provider api_tokens、TLS key_pem（仅 sha256 前 12 hex）、`features[].config` raw spec（含派生令牌者整体省略）。
- 验收：单测（401→200 字段枚举；dump 无明文密钥；reject/skip 计数自增；/metrics 出现两行）；集成。

## 2. B2 —— 性能/死面批

### R-5 prepare② 恒等回写短路（性能 #1）
- 事实：`gateway/src/pipeline/mod.rs:117-140` 无条件 `rewrite_model_field`（`body.rs:108-124` 全量 splice），即使回写值==原值；resolve 已解出值。
- 方案：重写前比较 `resolved == extracted`（复用 resolve 的 decoded 值），恒等则 `body_model = Some(model)` 直接走 B4 短路（不 splice、不二次扫描）；仅在真改写时 splice。
- 验收：alloc_guard 预算（identity 无 body 分配）+ 功能奇偶（改写/非改写/畸形 body）。

### R-6 capture_groups 复用 + 按需（性能 #3/质量 M3/R3）
- 事实：`pipeline/mod.rs:161-167` 无条件捕获 → `route_match.rs:32-49` 每请求重编译；core `RouteTable` 已编译同款（`config.rs:746-758`）不暴露 captures。
- 方案：`RouteTable` 暴露"用已编译正则取捕获组"方法（返回捕获组，生命周期借用），`route_match::capture_groups` 改为接收预编译句柄；且仅当 `route.rewrite_target.is_some()` 才计算。
- 验收：单测捕获组不变；alloc_guard/时间比；无行为回归。

### R-7 SWRR 单目的地直通（性能 #5 之最小改）
- 事实：`gateway/src/pipeline/swrr_select.rs:74-93` 每请求 clone 候选 + DashMap 分片写锁；集中单实例路由纯属浪费。
- 方案：`order_route` 对 `destinations.len()==1` 直接返回该目的地（跳过 SWRR 与锁）；多目的地原路径不变。
- 验收：单测（单目的地恒首/状态不创建）+ 既有加权序列不变。

### R-8 周期/超时/记账小项
- ① bootstrap dutycycle evict 间隔 1s → 30s（空闲阈值 5min 不变；文档化）。
- ② `usage_sink.rs::post_once` 增请求级整体超时（与 forward_auth 30s 对齐；bootstrap 注入 client 仅 connect_timeout）。
- ③ guardrail client 缓存（`egress/src/guardrail.rs:119-147`）增容量上限或与 bootstrap evict 同节奏的过期清理。
- ④ admin 响应带 `content-length`（P1 nitpick，admin.rs:170/stats.rs:89 可选低风险，与 B1-B4 同 PR 内一起做）。
- 验收：单测/集成 + clippy。

### R-9 死载荷/死字段处置（质量 M1/M2/L2 + 成熟度 #2/#3 + 可运维 H3）
- 决策（待 oracle 复核）：
  - ① `ConfigData.features`：移除 opaque `config` 全量 spec（消除 apiTokens 双份明文与 raw spec 泄密面），保留元数据（plugin/phase/priority/fail_open/default_config_disable）；凡引用 features 的测试/集成同步改。若存在依赖其 raw spec 的路径需先确认。
  - ② `failStrategy`（`translate.rs:436-439` → `GatewayFeatureConfig.fail_open`）：作为启动期 WARN 输入（记录"typed 等价映射，插件级开关默认无效"），不改变现行为（GPUStack 恒 FAIL_OPEN/never flip）。ext-auth 行为对齐另列 R-12。
  - ③ `ConfigData.timing`（`translate.rs:869-913`）：实现最小生效语义——数据面无策略覆盖时用 `upstream_idle_timeout_secs` 作为出站默认读超时上限的输入？**风险：可能杀死 SSE 长流**；oracle 判定。若判定不生效则改为：启动/变更日志 + 文档明示"仅记录、未强制"（诚实降级，对照 equivalence C2 建议的 warn-once）。
  - ④ `OutboundProxy.listener_port`/`translate.rs:383` 空语句：删除空语句；字段保留（McpBridge wire 兼容）并标注"数据面无消费方"。
  - ⑤ `SniStore`（`gateway/src/tls_store.rs`）：**接线或删除**。倾向接线到数据面 TLS（当前 `bootstrap.rs:220-234` 一次性 PEM `add_tls`）→ 由 C3（R-11）周期重载 SniMap；若 pingora 0.8 公共 API 不允许（已裁定）则：删除死代码或保留供未来 pingora 升级（PR #599/#832 证据），并修模块 hot-reload 注释 + PEM 0600/退出清理。oracle 在接线/删除两案间定案。
  - ⑥ `gc_stale` 见 R-3；`find_subseq`×4/`now_millis`×5 下沉：**仅做 find_subseq 单义化（空 needle 定变体 A）+ 注释**，不强行重构（改动面 vs 收益）。multipart 三份保留但两处注释互指。
- 验收：grep 证明无 raw spec 滞留敏感字段；无死字段读方误删；clippy。

## 3. B3 —— 治理批

### R-10 C1 配置漂移告警（control-gaps-plan Phase 2）
- 三处解析函数未知键告警（白名单差集）：`translate.rs:556-570`（model-router）、`:475-531`（model-mapper）、`:588+`（ai-proxy）；分发处告警（未知受管 WasmPlugin 名、higress-config 非超时键 @ `configmap_to_timing`）→ 每 pass 聚合 warn（指纹短路防风暴）；pin 文档增"升级重跑 pin 对比"条目。fail-open 语义不变。
- 验收：含未知键 fixture → warn 且不 reject；已知键行为不变（回归测试）。

### R-11 C3 TLS 轮换闭环（control-gaps-plan Phase 3，已裁定 pingora 0.8 无热载）
- 方案：`bootstrap.rs run()` 内 `tokio::spawn` 周期任务（30-60s）：读快照 `tls` 内容指纹（cert+key sha256）→ 与上次写入指纹比对 → 变化则重写 PEM（复用 `write_default_tls_pem` 逻辑，0600）+ `error!` + 计数 `tls_cert_change_detected_total`/`tls_cert_requires_restart_total` + README"轮换需重启容器"。SniStore 若按 R-9⑤ 接线则此处同时 store_config。
- 验收：指纹单测（内容变→变/不变→不变）；真实模拟：快照换证书 → 检测+告警+计数日志。

### R-12 ext-auth fail 语义对齐决策
- 事实：Hygress 硬编码 transport/5xx→FAIL_OPEN（`forward_auth.rs:15-17`、`pipeline/auth.rs:110-119`）；外部证据（GPUStack #6003）显示现行插件 `failure_mode_allow` 默认 false = fail-closed、`failStrategy` 只管 Wasm VM 致命错误。
- 决策点（oracle）：是否把默认改为 fail-closed？影响面：/token-auth 瞬时故障时真机 AUTHED 请求将 401 而非放行——与基线 Higress 行为一致但改变仓库内现有 FAIL_OPEN 契约文档与测试。倾向：**引入配置化**（env `HYGRESS_EXT_AUTH_FAIL_MODE` 默认 `open` 保持现有行为，文档记录与新版 GPUStack 默认的差异与切换方法；README/env 表补项）。若 oracle 判定跟随上游默认 closed 则需全量改测试与真机回归。
- 验收：单测两态；文档。

### R-13 文档/env/元数据对齐（成熟度 #4、质量 H3/M3-相关）
- ① README §4.3 env 表与 `gateway/src/config.rs` 对齐（补 GPUSTACK_JWT_SECRET_KEY/POLL_INTERVAL；统一 GATEWAY_TLS_PORT vs GATEWAY_HTTPS_PORT 命名——保留代码键，pack `gateway/run` 两处 export 对齐；`HYGRESS_KUBECONFIG` launcher 与 `bootstrap.rs:263-273` 的读取逻辑统一为"launcher 导出 KUBECONFIG"或代码读 HYGRESS_KUBECONFIG 二选一）；GPUSTACK_API_PORT 默认值三方核对（config.rs:24 默认 80 的注释语境澄清，launcher 30080 为 GPUStack 实际端口）。
- ② 测试数：README/dev-process/extensions-design/design 的 368/437/492 → 更新为实测数（B4 后填写）。
- ③ workspace `rust-version`：1.83 → ≥1.89（或按依赖实际锁定；rust-toolchain 1.98 不动）。
- ④ 日志重定向：`pack/hygress-s6/.../gateway/run` 的 `>> hygress.log` 保留（真机运维需要），文档注明 docker logs 不可见的取舍。
- ⑤ MSRV 元数据 + 未用依赖清理（gateway serde/rand/hmac/sha2/hex、adapter arc-swap、egress bytes/async-trait、workspace rand）→ cargo machete 或 grep 佐证后删除。
- 验收：grep 对照表一致；cargo 双态编译。

## 4. B4 —— 全量验证门禁
`cargo test --workspace --all-features`（预期 ≥525，新增用例后更新）· `cargo clippy --workspace --all-features -- -D warnings` · `cargo clippy --workspace -- -D warnings`（默认）· `cargo test -p hygress-gateway --test alloc_guard -- --test-threads=1` · `cargo test -p hygress-gateway --test integration`（integrations）。

## 5. B5 —— 真机验证（GPUStack v2.2.3 测试机，凭据按需获取）
复用 `docs/research/gpustack-validation/scripts/`（ship_to_remote/swap/dod6/e2e/dod2/run_all）+ `pack/` 构建。矩阵：
1. s6 启动/就绪（apiserver:18443、gateway=hygress、supercronic 无 pilot 检查、:80/readyz=200）
2. 端口纪律（无 9876/15010/15012/8888/15051）
3. 进程（单一 hygress）
4. DoD1 e2e chat 200 + usage 34/7 落行（model_route_id/access_key）
5. DoD2 CRD 逐字节一致
6. DoD6 回滚件 `.dist`
7. 延伸能力：限流 429 `rate_limit_error`+Retry-After、配额 429 `quota_limit_error`、输入护栏 403 `guardrail_blocked`、复位全放行
8. **修复回归**：重试矩阵（R-1，503→fallback 与候选切换次数）、`/config`（R-4，脱敏 + 计数）、TLS 指纹告警（R-11，若接线则同时验证热载）
9. 稳定性 R=0
> 若凭据不可达：交付"可执行真机套件 + 核对清单"，清单该项标 ⏸ 未执行。

## 6. B6 —— 清单核销与落档
- 清单 A：本计划每子项验收（表格，状态 ✅/⏸）。
- 清单 B：真机/部署矩阵（表格，✅/⏸ + 证据路径）。
- 更新 docs（REPORT/README/dev-process/benchmark 计数、control-gaps-plan 勾选+状态、pin 检查单条目），git 提交（计划→各批→报告分提交）。
- 诚实声明段：未执行/未验证/需实测项单列。

## 7. 复核要求（给 oracle）
- 逐子项判定：方案正确性（对照 file:line 事实）、验收可达性、风险是否可接受、依赖顺序、删除/降级项是否合理（尤其 R-9③ timing、R-9⑤ SniStore 接线 vs 删除、R-12 fail 默认方向）。
- 输出：APPROVE / REQUEST_CHANGES（逐条）+ 无阻塞声明，若判为"无阻塞"才放行 B1。

## 8. 复核期自取证补充（oracle 之外，主控/网络/仓库实证，实施时并入对应批次）
- **B1 附加小项（parse_consumer 边界）**：GPUStack `/token-auth` 返回的 `X-Mse-Consumer` 在"有 user 无 api_key"时为 `gpustack-<user.id>`（无 access_key 前缀，[token.py@557aa4fd](https://github.com/gpustack/gpustack/blob/557aa4fd/gpustack/routes/token.py)）；仓库 `pipe.rs:1519-1531 parse_consumer` 只认 `.gpustack-` 分隔 → 该形态会误判为 access_key="gpustack-7"、user_id=None。实施：补 `gpustack-<uid>` 前缀解析分支 + 单测（低影响但记账正确性）。
- **R-13 外延（ext-auth spec 漂移实据）**：GPUStack trunk（557aa4fd）ext-auth `defaultConfig` 已改为 openai 路由 exact + `/model/proxy` prefix 的 path-blacklist、allowed_headers 改为 X-GPUStack-Real-IP/x-higress-llm-model/cookie（无 failure_mode_allow、无 x-api-key 白名单）——与 recorded v2.2.3（路由名前缀 `_match_route_prefix_`、7 头白名单）不同。落档：升级检查单 + pin 文档"仅钉 v2.2.3 形状"标注，不做行为跟随。
- **failStrategy 语义主源**：Istio WasmPlugin 文档 + Higress e2e #3816 + higress ext-auth main.go `callExtAuthServerErrorHandler` —— `failStrategy` 仅致命错误；业务级 auth 失败由 `failure_mode_allow`(默认 false)/`status_on_error`(403) 决定。R-12 采纳"默认 fail-closed + env 开关 + 文档修订"的依据齐备。


## 9. 高精度复核结论（oracle 子代理超时未收敛 → 主控按 dev-process §10.6.2 方法论自审替代；建议后续补第二方复核）

- 判定：**APPROVE（无阻塞，放行 B1）**。复核依据：R-1..R-13 全部已对照代码 file:line 复核 + 本会话多源外部证据（nginx/Envoy/Higress retry 语义、regex 编译成本、ext-auth failure_mode_allow/failStrategy 主源、GPUStack trunk spec 漂移）。
- 复核中的定案（并入批次）：
  1. R-9③ timing：**诚实降级**——保持解析进 `ConfigData.timing`（观测/未来用），数据面不消费；启动 WARN-once 说明"higress-config 超时仅记录、未强制"；README 明示。不引入可能杀死 SSE 的默认读超时。
  2. R-9⑤ SniStore：**最小接线 + 未来化**——0.8 公共 API 无法接 resolver（外部证据 PR#599/#832 仅新版支持）；实施为 attach 时 `store_config` 同步一次快照（供未来/集成测试），修正模块 hot-reload 注释，PEM 写 0600 并尽量清理；README 注明升级 pingora 后启用 SNI/hot-reload。
  3. R-12：**默认 fail-closed + env 开关 `HYGRESS_EXT_AUTH_FAIL_MODE`（closed|open，默认 closed 对齐 GPUStack/Higress 基线）**；egress `Ok(None)`=无判定契约不变，仅 `pipeline/auth.rs` 映射与 Metrics 记 `fail_closed`/`fail_open`；修订 forward_auth 文档、design/pin/README 相关"FAIL_OPEN"表述；受影响测试更新见 R-12 验收。
  4. R-4 计数接线采用最小签名改动：仅 `store` 成功返回 dropped 计数（`Ok(usize)`），`new` 不变。
  5. R-3：保留 `gc_stale`（仅修文档为"evict_idle 为线上清理、gc_stale 为窗口级辅助"），release 增 `now_ms` 参数。
  6. 批次顺序与验收门禁自洽；B1/B2/B3 无互相耦合（B2 的 R-9 文档项由 B3 统一收口亦可）。
- NB：`SharedConfig::store` 签名变更波及 ~12 测试点（机械适配）；R-12 默认切换影响 egress tests/forward_auth.rs 4 用例语义注释与 pipe 相关集成预期（新增而非修改既有成功路径断言）；真机回归需在 B5 加"auth 服务抖动 → 403（closed）"用例。
