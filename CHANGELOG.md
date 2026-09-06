# Changelog

本项目为 GPUStack 内嵌 Higress 的 Rust（Pingora）原位替换 AI Gateway。版本口径与发布记录见
`docs/RELEASE-v0.1.0.md`；变更分类遵循 keep-a-changelog 惯例（类别为中文，便于与仓库文档一致）。

## [Unreleased]

- （无待发布项；残余记录在案：P4 入站头借用式物化属数据流重构延后项，见 `docs/research/audit-oracle-review-ora5.md` §7）

## [v0.1.0] - 2026-09-06（冻结版；tag `v0.1.0`）

首个冻结版：多轮 oracle 审核-修复循环收敛（ora-6 五维 = 成熟度 9.5 / 质量 9.5 / 性能 9.6 / 可运维 9.5 /
GPUStack 集成 9.5，无 BLOCK）。门禁：661 tests（含 39 真实 e2e、12 alloc_guard 预算）、clippy 双模式 0、
`cargo doc -D warnings` 0。真机 b101–b105 复验通过（详见 RELEASE 记录）。

### 新增（Added）
- **控制面可观测性**：`hygress_control_last_sync_timestamp_seconds`（每次成功 reconcile 含 no-op 的心跳）、
  `hygress_control_reconcile_error_total{class=list|rejected}`（outage 片段计数）、
  `hygress_policy_reload_total{result}`（`PolicyHandle` reload 单点 observer，覆盖 admin `/reload` 与 30s mtime 轮询）、
  `hygress_usage_pushed_total{completed}`（精确 vs 服务端估算计量）、`hygress_guardrail_error_total`
  （LLM 护栏服务失败，与内容拦截分离）、`hygress_upstream_errors_total{destination}`、`hygress_build_info{version}`。
- 启动日志：`version=` / `contract_pin=`（升级契约复核指针）/ `ext_auth_timeout_ms=` / `poll_interval_ms=`。
- 运维交付：`docs/operations.md`（指标目录 + PromQL 告警 + 事故 runbook + 升级/重启矩阵 + 日志与关闭语义）、
  `pack/compose-hygress.template.yaml`（参数化 compose 模板）。
- 依赖/工程：ring-only TLS（`rustls` default-features=false + `reqwest` `rustls-no-provider`，移除 aws-lc 编译链）；
  四 crate 根 `#![warn(missing_docs)]`（121 个此前无文档的 pub 项补齐）；直接依赖 `serde_yaml`（deprecated）
  → 维护中 `serde_yaml_ng`；`memchr` 用于 SSE 热路径。
- 测试：12 项边界单测（body splice 字节往返 / 双 usage 快照 last-wins / config 不动点环与终止 / find_subseq 与
  multipart 角例）+ 4 项 AM-2/用量/护栏 e2e + alloc_guard `am8`/`am9` 计量；e2e 累计 35→39。
- 性能：AM-2 注入 memo（消除 per-candidate 顶层体扫，字节等价）；指标 label-handle 缓存（P1）；
  上游非 2xx 错误体 256 KiB 读取封顶（P5）；限流 warm-bucket 借用键快路径（P7）；
  入站 `HeaderMap::remove` 缺席感知（no-op 剥离不再深拷贝；absent 0B/0 alloc 计量）；
  入站头惰性物化（限流/断体短路后）与 SSE 拆行 memchr（P6）。

### 变更（Changed）
- AM-2 `include_usage` 注入决策迁移到 prepare 期 memo（`PreparedRequest.am2_memo` + buffer 同一性守卫），
  出站字节与既有字节精确路径完全一致（wire 零变化）。
- 输出护栏断流：终态改冲刷**活** usage 累计器（真实 `output_chunk_count`/已吸收 token，与断流保留一致）。
- `OutboundHeaders`：`names()` 对 base 覆盖名按位输出一次；`append`/`insert` 在 `remove` 后从新列表开始
  （契约与 clone-then-mutate 等价，附回归测试）。
- guardrail 请求侧判定返回 `GuardrailHit::{Block,Unavailable}`，客户端 403 slug 不变、计数语义分离。

### 修复（Fixed）
- `guardrail_error_total` 在 sync fail-closed 路径的双计（按 ora-6 处方只保留 `guardrail_in` 内一次计数）。
- README 把已接线的 `HIGRESS_EXT_AUTH_TIMEOUT_MS` 误写为悬空 knob；`POLL_INTERVAL` 表述过时（B9.9 ~1s 轮询口径）。
- 文档/注释漂移：P5-pending/placeholder/“30s 安全网”残留标记、rustdoc 破损链接（`cargo doc -D warnings` 全仓零错误）、
  `ForwardAuthVerdict`/ext-auth 文档与代码不符。
- 控制面 LIST/结构性拒绝失败日志 ~1/s 刷屏 → 每 episode 一条 + 指标；watcher 错误日志维持 60s/5s 退避 + 30s/kind 限速。
- 日志洪水：forward-auth/guardrail/usage-sink 逐请求 warn → debug（计数承载速率）。
- e2e/单测覆盖缺口：显式 `include_usage:false` 不被覆盖、usage-less 2xx → `completed=false`、mirror 零用量上报、
  不可达 ext-auth → 403、护栏断流活累计、double-usage 单刷、memo delta≠0 注入等。

### 性能（Performance）
- build_outbound 3 候选 11595B/198 → 3213B/…（AM-6b overlay，早于本版但含于交付口径）；
  指标热路径 label-vec 全局写锁 + 每调用 status String → 缓存句柄 + 原子自增；
  SSE 拆行/预过滤 SIMD；限流每请求二次 key 分配 → 借用键（仅 miss 时 entry）；
  prepare 前短路请求零头表物化；no-op 剥离零 COW 拷贝。

### 移除（Removed）
- `aws-lc-rs`/`aws-lc-sys` 依赖链（单 TLS provider：ring）；未使用的直接依赖 `rand`；直接依赖 `serde_yaml`（deprecated）。

### 安全（Security）
- 密钥脱敏：`GpustackSink`/`forward_auth::{Client, ForwardAuthVerdict}` 的 `Debug` 不再打印 token/authorization/auth_cache。
- fail-closed 语义保持：无 admin token ⇒ `/reload`/`/config`/`/stats/usage` 401；ext-auth 默认 closed（403）；
  panic hook 记录后 exit(1) 由 s6 重启；启动摘要仅布尔化输出密钥存在性。
- 客户端不可伪造头（`x-gpustack-auth-token` 等）入站剥离保持，且缺席时不再引发整表拷贝。

### 文档（Documentation）
- 新增：`docs/RELEASE-v0.1.0.md`（本版发布记录）、`docs/operations.md`、`CHANGELOG.md`、
  `pack/compose-hygress.template.yaml`、`docs/research/audit-oracle-review-ora5.md`（ora-5 修复与决策）、
  `docs/research/audit-fix-checklist.md`（B10 + ora-6 + 固化行）。
- 更新：README 状态摘要/对比表/环境表（env 拼写与收敛口径）；README/design/equivalence 收敛节律 ~1s poll 表述；
  17 字段 usage wire 描述统一；missing-docs/门禁口径。

[Unreleased]: ./docs/RELEASE-v0.1.0.md
[v0.1.0]: https://github.com/ipconfiger/Hygress/releases/tag/v0.1.0
