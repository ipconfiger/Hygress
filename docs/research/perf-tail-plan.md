---
status: complete
phase: 4
updated: 2026-09-05
---

# 尾延迟与吞吐收尾修复计划（控制面事件化 / P6 / P3 口径）— 已完成并归档

## Goal
在已落地（B1-B4 / P1 / P2 / P4）基础上，把周期性 p99 尾部残余（~300ms 级）与剩余可证实开销降到最小：①控制面由"每 1s 轮询 + 全量 LIST/JSON decode"改为**事件驱动（kube WATCH）**，②数据面 `read_headers` 每请求 String 分配削减（P6），③benchmark rig 增加**纯网关内核隔离口径**（P3）。全部经 @oracle 放行后实施，以同 rig wrk A/B + alloc_guard + 22 Pingora e2e 验证后落档。

## Context & Decisions
| Decision | Rationale | Source |
|----------|-----------|--------|
| Phase 1 主方案 = kube WATCH 事件驱动替代 1s 周期 LIST | 根因是轮询架构本身（每 1s 进行 6 类 LIST + 全量 JSON decode，控制面运行时 CPU 与数据面争抢 → 残余 ~300ms 尾部）；WATCH 令稳态零开销、变化即时生效；kube 4.2 已依赖 `runtime` feature（watcher 可用） | `ref:ora-2` |
| Phase 1 廉价回退 = 轮询间隔 1s → 5s（保留 rv 指纹短路） | 若 WATCH 被评审判为风险过高则退而求其次；未启用（1.1 成功） | `ref:ora-2` |
| Phase 2 = P6 read_headers 每请求 String 克隆削减 | 数据面自身每请求固定 CPU；**量化判据后诚实降级**（见 Phase 2） | `ref:ora-2` P6 |
| Phase 3 = P3 纯网关内核基准口径 | `:80/readyz` 混入共享镜像上游（§3 口径缺口）；需无上游的纯内核测量点 | `ref:ora-2` P3 |
| 计划须先经 @oracle 评审 PASS（放行）后才开启实施 | 团队流程：计划 → 评审 → 放行 → 实施 | 用户指令 |

## Phase 1: 控制面尾仓治理 [COMPLETE]
- [x] 0.0 计划评审放行：@oracle APPROVE（无 🔴；🟠1/🟠2 由评审给出必采纳决策并已并入实现设计）
- [x] **1.1 kube WATCH 事件驱动**（实施 `cf4f6c5`，oracle 复审 PASS，9 点钉死设计全过）：
  首轮 `sync_once` 保留（bind-ready 语义不变）；6 类资源各一 `watcher`（McpBridge `Config::default()`、
  其余 managed-selector）；dirty+`Notify` 去抖；30s fallback tick；rv 指纹短路 + rv==0 加固保留；
  `Err` 保活、意外流结束补 `tracing::error!`（oracle Minor）。
- [x] 1.2 廉价回退（未启用——1.1 成功落地，不适用）
- [x] 1.3 验证（`benchmark.md §10`）：功能 e2e 通过；**c16/c64 p99 未扁平（388/479-481ms，与 §9 同域）
  → 残余尾部非控制面轮询所致**；嫌疑收敛为共享镜像上游 / rig 抖动

## Phase 2: 数据面分配削减（P6） [COMPLETE — 量化判据后诚实降级，未实施]
- [x] 2.1 量化判据：@oracle 测算 `read_headers` 每请求 String 克隆为 **~µs 级、纯分配卫生、p99 近零**
  （且本负载瓶颈为共享上游，非数据面分配）——按计划"若占比可忽略则如实降级"的原则，**P6 明确记录为
  低价值未实施**，不追加改码以保持"不加不经评审的单点改"
- [x] 2.2 验证：不适用（未实施）；`build_inbound_head` 纯函数抽取如未来确需可另立小任务

## Phase 3: 纯网关内核基准口径（P3） [COMPLETE]
- [x] 3.1 网关自服务 `/healthz` 内核下限——**免改码完成**（`bench_kernel_healthz_c{16,64}.txt`）：纯内核
  c16 13,643 req/s / p50 0.74ms / **p99 5.85ms（平坦、无周期尾）**；c64 14,537 / 2.49 / 24.4ms。
  **判定：网关内核平坦且快；`:80/readyz` 周期尾与 ~1.1k req/s 上限归因于共享镜像上游**
  （`benchmark.md §11`，决定性归因）
- [x] 3.2 静态 loopback sink 形式化"网关 vs envoy 纯内核"对比——**降级为可选、未实施**（3.1 已给出等效
  判定证据；如需正式跨网关数字可另立项临时 rig，本计划不启用）

## Phase 4: 端到端验证与归档 [COMPLETE]
- [x] 4.1 全套验证在各轮实施时逐项通过（`cargo build/test` 两 feature 模式 + 22 Pingora e2e + clippy
  双模式 0 + alloc_guard）——见各实现提交
- [x] 4.2 盒子 wrk A/B 全曲线已记录（§8 P1/P2 → §9 P4 → §10 WATCH → §11 内核下限）
- [x] 4.3 落档完成：`benchmark.md §10/§11` + 证据 + 本计划状态；提交 `81502ac` / `fc25a87` / 本次终态

## Notes
- 2026-09-05: 负载工具 `-t4 -c16` / `-t8 -c64`（4/8 客户端线程）；hygress 数据面线程 = vCPU16（每服务 16 × 3 服务 + 控制面 ≈ 68 线程/进程，实测）→ `ref:ora-2`、实测
- 2026-09-05: **Phase 1 判读**——WATCH 落地后 p99 未扁平 → 控制面轮询不是残余尾源（§10）
- 2026-09-05: **Phase 3.1 判定**——纯内核平坦 ~14k req/s / p99 5.85ms；`:80/readyz` 周期尾与上限归因
  共享镜像上游（§11）；P1/P2/P4/WATCH 各轮移除网关侧开销后 p99 只随上游波动的现象由此统一解释
- 2026-09-05: **收尾决定**（用户）：本计划归档并标记完成；P6 / 3.2 按量化/可选原则未实施并记录；
  整体优化链（B1-B4 / P1 / P2 / P4 / WATCH + 全套 benchmark §6-§11）至此闭环
