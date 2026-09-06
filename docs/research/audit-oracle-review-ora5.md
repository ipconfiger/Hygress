# Hygress Oracle 修复与收敛复核（ora-5，HEAD 5de0cd4）

> 性质：≥9.5 收口目标的修复轮。ora-4 五维 ≈8.6 加权 APPROVE；本轮回合重新派发五个独立 oracle 深度审核
> （各维度一档，HEAD 26a9696 基线）得到新基线分：成熟度 9.0 / 质量 9.2 / 性能 8.8 / 可运维 8.0 / 集成 9.2，
> 无 BLOCK、无 MAJOR（可运维 7 项 O1-O7 属观测/文档类门禁项）。随后按维度逐项修复（提交
> 79a3d5e → b0fd5ad → b0b1eaf → 1dae6db → 6a49670 → 5de0cd4），修复后交叉编译（x86_64-linux，
> 移除 aws-lc 依赖链）换真机镜像并跑 b101 活体验证。本文记录修复清单、决策与证据；最终收敛分由
> 新一轮收敛复核（ora-6）裁定。

## 1. ora-5 基线与修复映射

| 维度 | ora-5 基线 | 修复（提交） | 状态 |
|---|---|---|---|
| 代码成熟度/架构 | 9.0 | M1 stale 标记清扫 / M2(=Q1) Overlay names+append 契约 + 回归测试 / M4 单 TLS provider（ring-only，aws-lc 出锁）/ M5 未用 rand 直依赖移除 / M6 missing_docs 121 项补齐 + 四 crate 根 `#![warn(missing_docs)]` 强制 / M3 记录决策 / M7 记录接受偏差 | 待收敛复核 |
| 代码质量/正确性 | 9.2 | Q1(=M2) / Q2 auth 文档如实 / Q3 guardrail 断流保留活 accumulator / Q4 read_headers 注释 / T1-T5 12 项边界测试 | 待收敛复核 |
| 性能/资源 | 8.8 | P1 指标 handle 缓存 / P2,R3 AM-2 memo（消除 per-candidate 体扫）/ P5 错误体读取 256KiB 封顶 / P7 限流 borrowed-key 快路径 / P6 记录备忘 | 待收敛复核 |
| 可运维/可观测 | 8.0 | O1 README env 纠错 / O2 pack compose 模板 / O3 last_sync 心跳 / O4 reconcile_error+list 失败计数 / O5 policy_reload_total+observer / O6 日志洪水收敛 + guardrail_error 指标 / O7 operations.md / O9 版本+contract_pin 启动字段 + build_info / O11 Debug 脱敏 / O8 残留注释 / O10 关闭有界丢失文档化 / O12 destination 标签 / O13 controller 任务观察 | 待收敛复核 |
| GPUStack 集成 | 9.2 | G1(=O4) LIST 失败 warn-once / G2 usage_pushed{completed} / G3 4 项 e2e / G4 Debug 脱敏 / G5 wire 注释统一 / G6 启动 contract-pin 行 | 待收敛复核 |

## 2. 关键修复取证（file:line 摘要，详见各提交说明）
- O3 心跳：metrics `control_last_sync_timestamp_seconds`（每次成功 reconcile pass 包括指纹 no-op 刷新）；
  adapter `on_sync_ok`/`on_sync_failure` hooks + `sync_failure_warned` warn-once（G1/O4：LIST 失败/结构性拒绝
  每 episode 一条，成功复位）。
- G2：`usage_pushed_total{completed}` 于 pipe 两推送点。
- O5：`PolicyHandle::set_reload_observer` → `policy_reload_total{result}`（admin /reload 与 30s mtime 轮询
  同一 choke point）。
- R3/M14 memo：`PreparedRequest.am2_memo`（profile 旗标 + closing-brace，prepare 自身 model splice 位移已计入）；
  `build_outbound` 在 ⑧ 保持字节一致（buffer 同一性）时走 `ensure_stream_include_usage_from_memo`（O(1)），
  否则保字节精确重扫；body.rs `Am2Memo`/splice 单点。
- O6：pipe `guardrail_in` 返回 `GuardrailHit::{Block,Unavailable}`，Unavailable → `guardrail_error_total`
  （不再误计 content-block）；forward_auth/guardrail/usage_sink 逐请求 warn 降 debug（计数承载速率）。
- M4：workspace `rustls = { default-features=false, features=[ring,logging,std,tls12] }`；
  `reqwest = { default-features=false, features=[json,http2,rustls-no-provider,stream] }`；
  Cargo.lock 中 aws-lc-rs/aws-lc-sys 消失；ring 由 gateway main 装为进程默认，测试侧 once 安装。
- M6：四 crate 根 `#![warn(missing_docs)]`；`cargo doc -D warnings` 0。
- P1：requests/duration/tokens/ttft label handle 缓存（温路径 = 无锁 mutex 查 + 原子自增）。

## 3. 记录决策（本轮显式决策，替代"未做"）
- M3：`serde_yaml 0.9.34+deprecated` 保留。crates 源为本地离线镜像（rsproxy-sparse 无远端），无法换
  `serde_yaml_ng`；两处解析（策略文件、higress-config YAML 文档）均为受信输入（运维提供 / GPUStack 自身写出），
  风险可接受。升级路径：源可访问后切 `serde_yaml_ng`（API 兼容），并在 pin §7 升级清单加注。
- M7：`request_filter`（pipe.rs ~600 行编排器）记录为**有意的分层编排器**（阶段委派纯 helper、注释充分），
  维持不拆；等价的后续重构价值低。
- P6（memchr SSE 扫描）与 P4（入站 header 惰性化，需 pingora Session 改造）保留为记录在案的后续项
  （perf-tail-plan 已有量化：µs 级 / p99 平）。
- O10：关闭时内存队列（1024 行）在 SIGKILL/panic 下为**有界丢失**（无落盘），已在 docs/operations.md §7
  文档化；优雅停止走 pingora run() 排空。

## 4. 真机验证（b101，镜像 bfcf515 / 提交 6a49670）
- readyz 200 @24s、healthz 200；基线 chat HYGRESS_B101_OK（真实 qwen 实例 usage 35/7/42 completed=true）。
- 新族活体：`build_info{version="0.1.0"}=1`；`control_last_sync` 于安静集群 6s 窗口 +6s 前移（O3 心跳成立，
  与内容 store 区分）；`control_last_store`/`snapshot_store_total` 仅内容变更推进。
- `watch_error{class=permanent,kind=<6>}` 各 1（拓扑 A 预期降级、限速稳定）；`reconcile_error`/guardrail_error
  0 episode；`usage_pushed{completed="true"}=1`、`usage_push_dropped=0`。
- 无 token /reload → 401 fail-closed；policy_reload 家族首触后才现号（单测/e2e 覆盖成功/失败路径）。
- 60s 日志增量 1690B（仅 watcher 限速行）；`snapshot LIST failed` 日志计数 0（G1 无逐 tick 刷屏）。
- 启动行含 `version="0.1.0"` / `contract_pin=…v0.2.3.post5…` / `ext_auth_timeout_ms=30000` / `poll_interval_ms=1000`。
- 终态：readyz 200、容器稳定、默认清场；回滚点 `gpustack:hygress-b100`。

## 5. 门禁（5de0cd4）
- **659 tests**（含 e2e 39：新增 G3 4 项）全绿；clippy 双模式 0；alloc_guard 11/11（release）；
  `cargo doc -D warnings` 0（missing_docs 强制生效）。
- 交叉产物：x86_64-unknown-linux-gnu ELF（ring-only，无 aws-lc 编译链）。

## 6. 待收敛复核（ora-6）
各维度对修复后 HEAD 做收敛复核：基线 MINOR 是否全部关闭/记录、无新 MAJOR、评分 ≥9.5。

## 7. 后续收紧（ora-6 之后的性能收尾）
- **P6 完成**：`hygress_core::bytes::find_subseq` 改由 `memchr` memmem(SIMD) 实现；usage.rs 每 chunk SSE `\n` 拆行热路径改用 `memchr::memchr`（4 处）。语义不变（memmem 首个命中 = 原 naive 语义；空 needle 角规则保留），全部既有 find_subseq/multipart/usage 测试通过。
- **P4 完成（可消除部分）**：core `HeaderMap::remove` 对**不存在的名字**跳过 `make_mut` 深拷贝（入站 ① 剥离的 `x-gpustack-auth-token`/`x-gpustack-model-instance` 在几乎全部请求上缺席 → 不再触发拷贝）。alloc_guard `am8` 计量：clone+2 次 absent remove = **0 bytes / 0 allocs**（修复前每次 remove 都付 ~1193B/26）；present remove 仍恰好一次 COW 深拷贝（1193B/26，语义保留）。配套更新 prepare ① 注释（首个真实变更才付一次拷贝；纯 mirror/直通零拷贝）。仍属架构延后：入站 read-time 全量物化 + 借用式惰性包装（需 pingora Session borrow 贯穿）维持记录在案（µs 级量化，perf-tail-plan Phase 2）。
- **P4 追加（惰性入站头）**：`read_headers` 只提取标量；整表 core `HeaderMap` 物化拆到独立
  `materialize_headers`，在限流 429 / 413 / body 断流等提前终结之后、dispatch 前执行——提前终结的请求
  不再付逐头小写/分配拷贝（早先每请求都物化）。存活请求的物化仍为每请求一次（结构性必需：fallback 回放需
  原样表，auth/⑧ 均读 base），逐 hop 的 COW 仅发生在真实变更时。门禁 661/661 + 39 e2e 全绿。
