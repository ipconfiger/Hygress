---
status: in-progress
phase: 1
updated: 2026-09-05
---

# 尾延迟与吞吐收尾修复计划（控制面事件化 / P6 / P3 口径）

## Goal
在已落地（B1-B4 / P1 / P2 / P4）基础上，把周期性 p99 尾部残余（~300ms 级）与剩余可证实开销降到最小：①控制面由"每 1s 轮询 + 全量 LIST/JSON decode"改为**事件驱动（kube WATCH）**，②数据面 `read_headers` 每请求 String 分配削减（P6），③benchmark rig 增加**纯网关内核隔离口径**（P3）。全部经 @oracle 放行后实施，以同 rig wrk A/B + alloc_guard + 22 Pingora e2e 验证后落档。

## Context & Decisions
| Decision | Rationale | Source |
|----------|-----------|--------|
| Phase 1 主方案 = kube WATCH 事件驱动替代 1s 周期 LIST | 根因是轮询架构本身（每 1s 进行 6 类 LIST + 全量 JSON decode，控制面运行时 CPU 与数据面争抢 → 残余 ~300ms 尾部）；WATCH 令稳态零开销、变化即时生效；kube 4.2 已依赖 `runtime` feature（watcher 可用） | `ref:ora-2` P4 复审 Notes / 预判 |
| Phase 1 廉价回退 = 轮询间隔 1s → 5s（保留 rv 指纹短路） | 若 WATCH 的 reconnect / 流式资源管理被评审判为风险过高则退而求其次；配置热更仍秒级 | `ref:ora-2` |
| Phase 2 = P6 read_headers 每请求 String 克隆削减 | 数据面自身每请求固定 CPU；P1/P2/P4 落地后具备量化前提 | `ref:ora-2` P6 |
| Phase 3 = P3 纯网关内核基准口径 | 现有 `:80/readyz` 混入共享镜像上游（§3 口径缺口）；需一个无上游的纯内核测量点才能分离"网关到底多快" | `ref:ora-2` P3 |
| 计划须先经 @oracle 评审 PASS（放行）后才开启实施 | 团队流程：计划 → 评审 → 放行 → 实施 | 用户指令 |

## Phase 1: 控制面尾仓治理 [IN PROGRESS]
- [x] 0.0 计划评审放行：@oracle APPROVE（无 🔴；🟠1/🟠2 由评审给出必采纳决策并已并入下述实现设计）
- [ ] **1.1 kube WATCH 事件驱动**（`crates/hygress-adapter`，评审钉死设计）：
  - 保留**首轮 `sync_once`**（首快照 = 首次成功 6 类 LIST + store，bind-ready 语义不变）
  - 之后 6 类资源各一个 `kube::runtime::watcher`（3 个 CRD 走现有 `Api<DynamicObject>`，McpBridge `Config::default()`、其余 `Config::default().labels(MANAGED_SELECTOR)`；reconnect/relist/resourceVersion-continuation 由 kube-runtime 内部处理）
  - 每个 `Ok(_event)` 置**共享 dirty 标志 + `Notify`**；主循环 `select!{ shutdown, dirty.notified(), sleep(fallback_tick) }` → `if dirty.swap(false) { sync_once }`
  - **保留 30-60s 低频 fallback tick**（watch 流安全网，非 1s）
  - **保留 rv 指纹短路**（事件突发去抖的幂等护栏，非冗余）+ 已有 rv==0 加固
  - watcher `Err` 项 → 记日志 + keep last-known-good + **继续观察，绝不退出流任务** ← CURRENT
- [ ] 1.2 廉价回退（仅当 1.1 受阻）：**纯配置改动零 diff**——`POLL_INTERVAL=5s`（config.rs:132 已环境可解析），配合 rv 指纹；非代码任务
- [ ] 1.3 验证：同 rig wrk c16/c64 p99（目标：剔除 6×LIST decode 后残余尾收敛）+ 配置热更 e2e（CRD/Secret 改动 ≤ 一个事件周期生效）+ adapter/translate 测试全绿

## Phase 2: 数据面分配削减（P6） [PENDING]
- [ ] 2.1 先量化（评审注：`read_headers(&Session)` 主体是 `session.req_header()` 的纯变换——先抽取 `fn build_inbound_head(req: &RequestHeader) -> InboundHead` 纯函数便于 alloc_guard 无 socket 可测，再上"临时计数"量化；若占比可忽略则 P6 如实降级）
- [ ] 2.2 验证：alloc_guard 新增 `build_inbound_head` 预算断言 + 同 rig wrk A/B（c16/c64 p50/p99/吞吐）

## Phase 3: 纯网关内核基准口径（P3） [PENDING]
- [ ] 3.1 在 rig 上加**网关自服务 `/healthz`**（admin/stats 是 pingora ServeHttp，不经过 `request_filter` 管线 = 纯 accept+parse+respond 的**内核下限**，非"真代理路径"；用 `/healthz` 而非 `/metrics`——后者 O(families) 编码劣选）
- [ ] 3.2 若需 envoy 内核对比：临时静态 loopback sink + 一条测试路由（不改生产数据面），两侧同法；**任何 "hygress 内核 vs envoy 内核" 结论必须出自 3.2**

## Phase 4: 端到端验证与归档 [PENDING]
- [ ] 4.1 全套：`cargo build/test` 两 feature 模式 + 22 Pingora e2e + clippy 双模式 0 + alloc_guard
- [ ] 4.2 盒子 wrk A/B（P3 口径 + `:80/readyz`）：记录 p99/p50/吞吐，目标残余尾进一步收敛；对比基准曲线（§8/§9）
- [ ] 4.3 落档 benchmark.md 新章节 + 证据 + 本计划状态更新，提交

## Notes
- 2026-09-05: 上一轮（P4 re-bench）负载工具 `-t4 -c16` / `-t8 -c64`（4/8 客户端线程）；hygress 数据面线程 = vCPU16（每服务 16 × 3 服务 + 控制面 ≈ 68 线程/进程，实测 `ps -eLf`）→ `ref:ora-2`、实测
- 2026-09-05: 残余 ~300ms 尾嫌疑 = 每 1s 6 类 LIST + 全量 JSON decode（控制面 CPU，与数据面争抢）与共享镜像上游抖动；Phase 1 针对前者 → `ref:ora-2`
- 2026-09-05: 全部实现项保持"不加不经评审的单点改"：先 1.1 落、再按评审回退/并行 P6、P3 为测量口径改动
