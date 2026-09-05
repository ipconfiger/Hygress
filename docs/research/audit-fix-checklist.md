# Hygress 审计修复验收清单（清单 A：逐修复项核销）

> 依据：docs/research/audit-repair-plan.md（方案 + §8 自取证 + §9 APPROVE）+ docs/research/audit-oracle-review.md（ora-2 第二方终审）。
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

## B4 — 全量门禁 [✅ **587 tests** / clippy all-targets 双模式 0 / alloc_guard 6/6]

| 项 | 状态 |
|---|---|
| `cargo test --workspace --all-features`（587：adapter 50 / core 215 / egress 52+8+10+4 / gateway 206 / integration 35 / doc 1） | ✅ |
| `cargo clippy --workspace --all-features -- -D warnings` / 默认 | ✅ |
| `cargo test -p hygress-gateway --test alloc_guard -- --test-threads=1`（6/6） | ✅ |
| `cargo test -p hygress-gateway --test integration`（35 项 e2e，含 AM-2 SSE 注入 2 项 + 判别缺口 11 项） | ✅ |

## B7 — ora-2 终审 MAJOR/MINOR 修复（audit-oracle-review.md §2/§3，2026-09-05 实施）

| 项 | 验收标准 | 状态 |
|---|---|---|
| AM-2 SSE include_usage | `build_outbound` 单点注入（is_model_route + json + completions 路径 + 顶层 stream=true + 无显式 stream_options）；五条件全在分配前；无 DOM/H4；e2e：不带→上游收到注入且 usage completed=true 精确、带→不双注入；pin §2.8 落差闭合 | ✅ |
| AM-1 IngressClass 门控 | Controller 增 `seed_ingress_class`，run() 内 `if` 门控；预种子方法删除（grep 佐证唯一写点被门控）；注释/README 口径统一；拓扑 A 零 apiserver 写 | ✅ |
| AM-4 fallback 复检 | sanitize 对 accepted 集复检；悬空引用 Main 被移出并报告 issue；判别测试（坏 Fallback+引用 Main→拒；健康对→双收） | ✅ |
| AM-3 body 读 Err | `read_body` 返回区分 Ok(None)/Ok(Some)/TooLarge/Read；读错→400 short_circuit 且**不派发上游**（e2e：半关闭断流→上游 0 次、无 usage 行）；BodyReadFailure 分类不混淆 | ✅ |
| AM-5 指标统一出口 | 8 处短路点统一 `record_short_circuit(status,elapsed)`（kind="short_circuit"）；404/503/401/403/429/413 全覆盖；专用计数器保留；help/kind 词典更新；4 类短路 e2e 计数断言 | ✅ |
| MINOR 行为 7 项 | usage_sink 4xx 不重试（401/404 一次即 drop；503/429 仍 3 次）；provider URL 改写失败 warn；admin bearer 恒定时间比较；sanitize percent 0..=100 + u64 checked 累加（双 u32::MAX 不 panic）；第 2+ Mirror 报告并剔除；usage Unknown 1 MiB 上限 + SSE 单行上限 + parse 预算（消 O(n²)）；forward_auth 非 UTF-8 write-back warn | ✅ |
| 判别测试缺口 | integration 22→35：failover 换候选/tries=0 禁用/timeout 触发重试/500·429 重试集外不换候选但 fallback/max_redirects=10 预算有界/断流不派发/短路计数（翻转断言红验证） | ✅ |
| 文档漂移批量 | 10 文件：1s→30s 口径、design v1.5、FAIL_OPEN→fail-closed 两层语义、pin 补 authorization/版本 1.1.1/§7 升级检查单、env 表、15020、34/7 vs 34/5 按跑次标注、日志落点、ai-proxy 措辞 | ✅ |
| B7 门禁 | 全量 587 tests 全绿 / clippy 双模式 0 / alloc_guard 6/6 / rustfmt 无净新增 | ✅ |

## B5 — 真机矩阵 [✅ 2026-09-05 真机 GPUStack v2.2.3：ready/进程/端口/metrics 新家族//config 401/chat 200+usage 34|5 落行/限流 200→429 rate_limit_error/护栏 403 guardrail_blocked/复位 200]
> 执行环境：125.67.215.17（linux 构建产物经 cargo-zigbuild 交叉编译）。

| 项 | 状态 |
|---|---|
| s6 启动/就绪/进程/端口纪律 | ✅（b5.log） |
| DoD1 e2e chat 200 + usage 34/5 落行 | ✅（b5.log） |
| metrics 新家族（config reject/skipped、tls cert×2）在线 | ✅（b5.log） |
| admin /config 401 fail-closed | ✅（b5.log） |
| 限流 429 `rate_limit_error`+Retry-After / 护栏 403 `guardrail_blocked` / 复位 | ✅（b6/b8.log） |
| **B7 新行为真机回归**（SSE include_usage 注入、AM-3 断流、AM-5 计数、AM-1 零写） | ⏸ 未跑（无真机会话；集成层已覆盖，建议下次真机补） |
> 凭据按需提供；若不可达 → ⏸ 未执行 + 交付可执行套件。
