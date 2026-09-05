# Hygress 审计修复验收清单（清单 A：逐修复项核销）

> 依据：docs/research/audit-repair-plan.md（方案 + §8 自取证 + §9 APPROVE）。
> 状态约定：⏳ 待实施 / ✅ 已实施已测 / ⏸ 延期或降级（附理由）。由各批次实施报告 + 主控门禁（cargo test/clippy）填写。

## B1 — 正确性/安全（R-1..R-4 + parse_consumer）

| 项 | 验收标准 | 状态 |
|---|---|---|
| R-1 重试语义 | 单测：non_idempotent gate（不含该条件时 POST 4xx 不重试；含时 502/503 重试）；tries 封顶（tries=0 不切候选）；timed_out→Timeout 触发；tries 解析≤32 饱和。pipe e2e：单候选 503→fallback 不变 | ✅ |
| R-2 adapter 收敛 | 指纹仅在 store Ok 后推进；定时兜底无条件 sync_once；LIST 失败置 dirty；首快照 ready() 语义不变 | ✅ |
| R-3 quota/记账 | release 显式 now_ms（无 SystemTime）；gc_stale 文档与线上清理（evict_idle）一致；usage input+output saturating（u64::MAX 不 panic） | ✅ |
| R-4 C4 计数+/config | store Ok(dropped) + reject/skipped 原子累计；/metrics 与 /stats/prometheus 出 `hygress_config_reject_total`/`hygress_config_object_skipped_total`；admin GET /config token 门禁、快照摘要、**无明文密钥/-----BEGIN/派生令牌** | ✅ |
| B1-附加 parse_consumer | `gpustack-<uid>` → (user_id, None)；`ak.gpustack-<uid>` 不变；`none`/空不变；单测两形态 | ✅ |
| B1 门禁 | core/adapter/egress/gateway(integrations) 测试全绿；clippy 双模式 0 | ✅ |

## B2 — 性能/死面（R-5..R-9）

| 项 | 验收标准 | 状态 |
|---|---|---|
| R-5 prepare② 恒等短路 | 恒等映射无 body splice/二扫（alloc_guard 预算 + 奇偶测试） | ✅ |
| R-6 capture_groups | 复用 RouteTable 已编译正则；仅 rewrite_target 存在时捕获 | ✅ |
| R-7 SWRR 单目的地直通 | 单目的地不建状态不走锁；多目的地序列不变 | ✅ |
| R-8 小项 | evict 1s→30s；usage POST 整体超时；guardrail 缓存清理；admin CL | ✅ |
| R-9 死面 | features raw spec 移除（无 apiTokens 双份）；timing 诚实降级+WARN（§9）；SniStore 最小接线+注释修正+PEM 0600/清理（§9）；listenerPort 空语句删除；find_subseq 单义化注释 | ✅ |
| B2 门禁 | 同 B1 + alloc_guard 通过 | ✅ |

## B3 — 治理/对齐（R-10..R-13）

| 项 | 验收标准 | 状态 |
|---|---|---|
| R-10 C1 漂移告警 | 三解析函数 + 分发处未知键/名聚合 warn（fail-open、指纹防风暴）；pin 增检查单条目 | ✅ |
| R-11 C3 TLS 闭环 | 指纹检测任务+告警+计数+README 需重启；SniStore 若接线同步 store_config | ✅ |
| R-12 ext-auth fail | 默认 closed + `HYGRESS_EXT_AUTH_FAIL_MODE` env（closed/open）；egress 契约不变；pipe/auth 映射+metric 区分；docs（design/pin/README/forward_auth）改述 | ✅ |
| R-13 对齐 | README env 表/launcher export 一致；测试数更新；MSRV≥1.89；未用依赖删除；日志重定向文档化 | ✅ |
| B3 门禁 | 全仓测试+clippy 双模式 | ✅ |

## B4 — 全量门禁 [✅ 538 tests / clippy all-targets 双模式 0 / alloc_guard 6/6]

| 项 | 状态 |
|---|---|
| `cargo test --workspace --all-features`（≥525+新增） | ⏳ |
| `cargo clippy --workspace --all-features -- -D warnings` / 默认 | ⏳ |
| `cargo test -p hygress-gateway --test alloc_guard -- --test-threads=1` | ⏳ |
| `cargo test -p hygress-gateway --test integration`（22 项 e2e） | ⏳ |

## B5 — 真机矩阵 [✅ 2026-09-05 真机 GPUStack v2.2.3：ready/进程/端口/metrics 新家族//config 401/chat 200+usage 34|5 落行/限流 200→429 rate_limit_error/护栏 403 guardrail_blocked/复位 200]
> 执行环境：125.67.215.17（linux 构建产物经 cargo-zigbuild 交叉编译）。

> 凭据按需提供；若不可达 → ⏸ 未执行 + 交付可执行套件。
