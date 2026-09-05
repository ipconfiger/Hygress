# Hygress 审计修复执行与验收报告（B1–B4 ✅ / B5 ✅ / B6 落档）

> 时间：2026-09-05（会话内）。方案：`docs/research/audit-repair-plan.md`（§9 APPROVE，oracle 子代理超时后按 dev-process §10.6.2 主控高精度自审替代并记录）。
> 结果：B1–B4 全部落地并提交（`7b3f450`）；**538 测试全绿、clippy `--all-targets` 双 feature 模式 0 警告、alloc_guard 6/6**。
> B5 真机验证：✅ —— 经用户提供 cargo-zigbuild 交叉编译 linux/x86_64 二进制后，真机（GPUStack v2.2.3 + RTX4090）换入新版并通过验证矩阵（详见 §3；远端证据 `/root/hygress-b3/b*.log`）。

## 1. 变更内容（B1–B3，提交 7b3f450）
| 批 | 项 | 摘要（file） |
|---|---|---|
| B1 | R-1 重试语义 | `retry.rs` NonIdempotent→修饰 gate、tries 封顶语义、`timeout` 触发经 reqwest `is_timeout` 透传；`pipe.rs` failover 按 `tries` 次数封顶 |
| B1 | R-2 adapter 收敛 | 指纹仅 store Ok 后推进；每唤醒无条件 sync_once；LIST 失败保留旧指纹待重试（`adapter/src/lib.rs`） |
| B1 | R-3 quota/记账 | `release(now_ms,…)` 全注入；gc_stale 文档更正（线上=evict_idle）；usage `saturating_add`（`quota.rs`×2、`usage.rs`） |
| B1 | R-4 C4 | core `SharedConfig` 计数（store Ok(dropped)+reject/skip 原子）；gateway Metrics Collector 出两行 `/metrics`；admin `GET /config`（token 门禁、脱敏：无 apiTokens/key/cert/raw spec） |
| B1 | 附加 | `parse_consumer` 支持 `gpustack-<uid>` 形态（无 key 用户记账） |
| B2 | R-5 | prepare② 恒等短路 `body::model_field_equals`（同值不 splice） |
| B2 | R-6 | `RouteTable::capture_groups_for` 复用已编译正则；仅 rewrite_target 存在时计算 |
| B2 | R-7 | SWRR 单目的地直通（不建状态/不加锁） |
| B2 | R-8 | policy/evict 周期 1s→30s；usage POST 请求级 30s 超时；guardrail 判词缓存 4096 上限；admin/stats CL framing |
| B2 | R-9 | features raw spec 移除（消除 apiTokens/派生令牌内存双份）；listenerPort 冗余语句清理；timing 诚实降级+warn（不强制，防杀 SSE）；SniStore 最小接线+注释修正+PEM 0600；find_subseq 单义化 |
| B3 | R-10 C1 | 三解析函数+分发处未知键/未知受管插件漂移告警（fail-open、逐 pass 聚合） |
| B3 | R-11 C3 | TLS 内容指纹 60s 轮询：变化→重写 PEM+error+双计数 `hygress_tls_cert_change_detected_total`/`..._requires_restart_total`（0.8 无热载→需重启，README 注明） |
| B3 | R-12 | ext-auth 业务失败默认 **fail-closed(403)**（对齐 GPUStack/Higress `failure_mode_allow=false`）+ `HYGRESS_EXT_AUTH_FAIL_MODE=open` 切回旧 fail-open；指标区分 unavailable_allowed/denied |
| B3 | R-13 | README env 表增补（EXT_AUTH_FAIL_MODE/KUBECONFIG 镜像/GATEWAY_TLS_PORT）；launcher 双名导出（pack/gateway/run）；workspace rust-version 1.83→1.89；测试数 492→538 |

## 2. 门禁证据（本会话实测）
- `cargo test --workspace --all-features` → 各 suite 全绿，合计 **538 passed**（含 22 项 Pingora e2e；alloc_guard 6 项独立通过 `--test-threads=1`）。
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` → 0；`--workspace --all-targets`（默认）→ 0。
- 新增核心单测：retry gate/封顶/超时（10）· usage 饱和· quota 时间注入· config 计数（2）· metrics Collector· admin /config 脱敏（3）· parse_consumer（3+）等。

## 3. B5 真机验证 — ✅（2026-09-05，真机 GPUStack v2.2.3 + RTX4090 worker）
- 解除阻塞：用户提供 `/usr/local/bin/rust_build_linux`（cargo-zigbuild）→ 本机交叉编译 **linux/x86_64 ELF**（`cargo zigbuild --release --target x86_64-unknown-linux-gnu -p hygress-gateway --features integrations`，HOME 指工作区缓存规避 zig 缓存沙箱拒绝）→ 远端起镜像 `gpustack:hygress`（e6eb3458）→ `compose up --force-recreate` 换入。
- 实测结果（证据存远端 `/root/hygress-b3/b{5,6,8}.log`）：
  1. 启动：~15s 内 `readyz=200`；进程：hygress×1，envoy/pilot-discovery×0；端口：80/443?/30080/8081/15020/18443，禁绑 5 端口计数 0。
  2. 新代码在线：`/metrics` 出现 `hygress_config_reject_total` / `hygress_config_object_skipped_total` / `hygress_tls_cert_change_detected_total` / `hygress_tls_cert_requires_restart_total`（均 0，接线证实）；admin `/config` 无 token → **401**（fail-closed 门禁证实）。
  3. e2e chat：`POST :80/v1/chat/completions`（qwen2.5-0.5b-instruct）→ 200，内容恰为 `HYGRESS_B3_OK`，usage 34/5；`model_usage_details` 新行 id=254 = **34/5 completed=t** 逐位一致。
  4. 延伸能力（policy 热载周期已按 R-8 为 ≤30s）：consumer `rps:1/burst:1` → **200 / 429 / 429 `rate_limit_error`**；`FORBIDDEN_B3` 静态规则 → **403 `guardrail_blocked`**；移除 policy → 200 复位。
  5. 备注：真机原镜像已保留 `gpustack:hygress-pre` 可回滚；R-8 将 policy mtime 轮询由 1s 改为 30s（README 措辞已同步）。
- 未做（能力外/无现成场景）：auth-unavailable 403↔open 对比（单元/集成已覆盖 + env 配置测试）、CRD 逐字节 dump（非本次修复范围，基线与修复前镜像已逐字节一致过）。

## 4. 诚实声明
- B5 各 ⏸ 项未在真机确认；旧镜像（修复前）行为不受影响且已恢复。
- 依赖链/发布件、以及"管线级纯内核吞吐/单路由锁"等审计遗留需实测项仍未测（与修复无直接关系）。
- oracle 子代理（Phase0）两次超时未收敛 → 复核由主控高精度自审完成并留档（§9），建议有条件时补第二方复核。
