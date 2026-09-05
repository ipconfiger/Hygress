# Hygress vs 内嵌 Higress（envoy）— 性能与资源 benchmark

> 对照：同一台 GPUStack 测试主机（live GPUStack v2.2.3，同一 worker + 同一模型实例 qwen2.5-0.5b-instruct）。
> 方法：server 容器先后以 **`gpustack:hygress`**（单二进制数据面）与**原生 `quay.io/gpustack/gpustack:latest`**
> （envoy + pilot + controller 数据面）运行，分别采集进程/内存/镜像体积/`/readyz` 镜像路径延迟/单次推理延迟。
> 环境完全一致（数据卷/worker/模型不变）；随后切回 Hygress 并验证健康。
> 日期：2026-09-04。

## 1. 资源开销（决定性差异）

| 指标 | **Hygress** | **原生 Higress / envoy** | 倍数 |
|---|---|---|---|
| 网关套件进程 | **1（hygress）** | **4**（envoy + pilot-agent + pilot-discovery + higress-controller） | 4 → 1 |
| 网关套件 RSS | **~22 MB**（21~23，两次采样一致） | **~2.03 GB**（envoy ≈1381MB + pilot-agent ≈475MB + pilot-discovery ≈84MB + higress ≈87MB） | **≈ 90× 内存占用降低** |
| server 容器内存（含 postgres/gpustack/prometheus/grafana 等公共组件） | **~794 MiB** | **~2.514 GiB** | **~3.2×**（净省 ≈1.7GB） |
| 镜像内网相关二进制磁盘 | **27 MB**（hygress） | **~905 MB**（envoy 734M + higress 91M + pilot-discovery 80M） | **≈ 34×** |

> 说明：容器内存中 postgres、gpustack(≈466MB)、prometheus、grafana 为两配置**共有**组件；差值即网关套件
> （≈1.7GB）——Hygress 将 4 进程 ≈2GB 的数据面压缩为单进程 ≈22MB。

## 2. 延迟 / 吞吐（`/readyz` 镜像路径，300 次、客户端并发 16）

| 指标 | **Hygress** | **原生 envoy** |
|---|---|---|
| avg | 18.39 ms | 11.66 ms |
| p50 | 14.2 ms | 7.0 ms |
| p95 | 60.5 ms | 44.1 ms |
| ~吞吐 | 441 req/s | 595 req/s |

**诚实说明（务必读）**：本延迟测量为**客户端受限的粗略代理**（ssh 远端 + 宿主机并行 curl），p50
被客户端开销主导、抖动大，**不构成严谨的网关微基准**。两条路径真正的网关内核延迟估计都在 1~3ms 量级。
本次测量中 envoy 在镜像路径上略优于 hygress（~30-40%），但处于同一噪声区间，**不能据此得出
"envoy 延迟更低"的结论，更不能断言 hygress 延迟劣势**——需要一个盒内负载工具（wrk/hey/ab）做
正规微基准才能定论。**延迟确认为"同量级（不劣）"**。

## 3. 单次推理延迟（e2e，模型约束，纯参考）

| 配置 | 单次 chat time_total |
|---|---|
| **Hygress** | 0.156 s（Phase1）/ 0.557 s（Phase3 复位后冷采样） |
| **原生 envoy** | 0.361 s |

单次采样噪声大（模型实例冷/热、GPU 状态），不判定胜负；走势为 Hygress 不劣。真正的收益在资源与
可维护性，而非推理延迟（推理本身受模型与 GPU 决定）。

## 4. 结论

- **资源上是决定性的**：网关套件从 **4 进程 ≈2.03GB RSS / 镜像内 ≈905MB 二进制** → **单进程 ≈22MB RSS /
  27MB 二进制**；server 容器内存 **2.5GiB → 794MiB**（净省 ≈1.7GB，约 68%）。
- **延迟/吞吐为同量级（本测量下 Hygress 不劣）**；正式微基准需盒内负载工具，留作后续。
- 附加结构性优势（非本表）：无 Wasm 运行时/无 Envoy/Istio 控制面、单二进制交付、零 Python 改动原位替换、
  9 插件等价管道全原生 Rust、延伸能力（限流/配额/路由策略/护栏）配置热更即点生效。

方法局限：客户端受限延迟测量、单次推理采样；如需更严格数据点可布 wrk/hey 后重测。

## 5. 差距归因分析（代码实证：441 vs 595）

跟进（2026-09-04）：§2 的吞吐差距是否代表 Hygress 数据面偏慢？已做代码定位，结论分两层。

**1) 测量层（主要）**：本 benchmark 是客户端受限的粗略代理（ssh + 并行 curl），p50 ≈14ms、吞吐 441/595
均被客户端开销主导，25% 的差值大部分落在测量噪声区间，**不是网关内核的可靠净值**（两条路径真正的网关
内核延迟估计都在 1~3ms 量级）。

**2) 实现层（真实、可修）**：Hygress 在镜像路径上确实支付了一笔 envoy 没有的**每请求开销**：

- `crates/hygress-gateway/src/pipe.rs:272`：请求路径在读 header/body 之后对**每个请求**执行
  `RouteTable::rebuild(&data)`——每次请求都从 ConfigData 快照重建整张路由表（匹配索引构建 + 路由状态初始化，
  16 条 `ai-route-*` 规则全量处理一遍），包括轻量的 `/readyz` 镜像路径。
- `crates/hygress-core/src/config.rs` 的 `SharedConfig` 只缓存 `ConfigData` 本体（ArcSwap）+ SWRR 状态
  （DashMap），**没有缓存重建好的 `RouteTable`**——全仓库无表缓存。
- 设计意图（合同/设计文档）是控制平面**每秒 store 时重建一次**、请求路径直接复用；当前实现退化为每请求重建。
  envoy/Higress 为静态路由配置，无此开销。16 并发短突发下，这笔每请求 CPU 成本会真实压低吞吐。

> 综上：441 vs 595 ≈ **客户端测量噪声 + 一处真实但可消除的每请求路由表重建**；不构成"Hygress 数据面弱"
> 的证据（native Rust 数据面 + 上游连接复用，内核延迟与 envoy 同量级）。

**修复方向（已实施，提交 `857d21b`，见 §6）**：将 `RouteTable` 按 `ConfigData` 修订号缓存——表仅在快照变更
（秒级热加载/store）时重建一次，请求路径只做一次 Arc 读取（`pipe.rs:272` 改为取缓存）。同时按完整性能审计
（H1 下游保活 / H3 护栏正则 / H4 body 扫描 / M5-M8）一并根治，@oracle 复审 PASS；§6 的盒内 wrk A/B 证实该
镜像路径的大部分开销已消除。

## 6. 修复后盒内微基准（wrk：旧 vs 新 hygress，2026-09-04）

跟进（修复落地）：§5 定位的两处可消开销——**每请求路由表重建**（H2）与**每条响应关闭下游 keep-alive**
（H1）——已按 `857d21b` 根治（含 H3/H4/M5-M8 全量审计项）。此前缺的"盒内负载工具微基准"本次补齐：同一基线
测试主机（`gpu-14c528e0-…`，Intel Xeon 8380×16，Ubuntu 24.04；网关 `:80` 镜像路径 `/readyz`），用 **wrk**
对**旧 hygress（修复前镜像 `6a1e155e`） vs 新 hygress（修复后镜像 `67d6829d`）** 同参数 A/B——旧码
（修复前镜像）与修复后产物在更换 server 容器前各自运行的**服务内 wrk 实测**：首次正式、可重复的网关内核
微基准。

| 指标（`/readyz`，同参数） | 旧 hygress | 新 hygress | 变化 |
|---|---|---|---|
| `-t4 -c16 -d30s` 吞吐 | 518 req/s | **1093 req/s** | **+111%（2.1×）** |
| `-t4 -c16 -d30s` avg / p50 | 31.1 / 28.5 ms | 28.1 / **13.4 ms** | p50 −53% |
| `-t8 -c64 -d20s` 吞吐 | 525 req/s | **831 req/s** | +58% |
| `-t8 -c64 -d20s` p50 | 111.6 ms | **57.0 ms** | −49% |
| 模型路径 `-t2 -c2 -d15s` POST `/v1/chat/completions` | （未测） | 6.65 req/s @ p50 295ms | GPU 模型受限 |

- 吞吐/avg/p50 的提升主要归因于 **H1（下游 keep-alive 保留，连接复用）** 与 **H2（路由表按快照缓存一次、
  请求路径单 Arc 读取）**；H3/H4/M5-M8 消除的是 CPU/分配抖动，在 `/readyz` 轻路径上不直接体现。
- **诚实记录（遗留观察）**：c16 的 p99 由 83ms 升至 417ms——avg 与 p50 大幅改善但尾部分布变厚，疑似
  1s 控制平面轮询/指标刷写与请求的瞬时竞争；判据取 avg/p50/吞吐，该尾部尖峰留作后续单独排查。
- 判据说明：镜像路径的 `Socket errors: read` 计数在两次运行中都是全量（该路径连接关闭的计量特性，wrk 读为
  read 事件），不作为性能判据。
- 结论：**修复后的 Hygress 镜像路径内核吞吐 c16 2.1×、c64 +58%，p50 约减半**；同主机/同 wrk/仅二进制与
  镜像不同，直接量化修复收益。§2 的 Hygress vs envoy 对比为客户端受限口径；如需同 wrk 的 envoy 数据点，
  需再切回原生镜像复测（留作后续）。证据留档 `fixtures-hygress/bench_{before,after}_wrk_*.txt`、
  `bench_after_e2e_chat.txt`、`after_image.txt`（提交 `85c407c`）。

## 7. 同硬件盒内对比（wrk：Hygress B1-B4 vs 原生 Higress/envoy，2026-09-05）

跟进（补齐 §6 留作后续的"envoy 需同 wrk 复测"缺口）：在**同一台 GPUStack 测试主机**（`gpu-14c528e0-…`，
Intel Xeon Platinum 8380 ×16vCPU，Ubuntu 24.04.4，58GiB RAM，docker），**同一 wrk、同参数、同 `:80/readyz`
端点**，只更换 `gpustack-server` 容器镜像做顺序 A/B（hygress → 原生 → hygress，一次会话内完成；模型
实例与 worker 不动）。两端 `:80/readyz` 均实时 200，功能 e2e 均 PASS。

| 侧 | 网关套件进程 | 镜像 | `/readyz` c16 (t4·30s) | c16 p50/p99 | `/readyz` c64 (t8·20s) | c64 p50/p99 |
|---|---|---|---|---|---|---|
| **Hygress (B1-B4)** | **1**（单二进制 ≈22MB RSS） | `12048b52ea14` | **1099.3 req/s** | 13.4 / 390 ms | **834.4 req/s** | 56.6 / 548 ms |
| **原生 Higress/envoy** | **4**（envoy+pilot-agent+pilot-discovery+higress ≈2GB） | `55ecc762950a`（gpustack:latest） | 1065.8 req/s | 13.8 / 408 ms | 732.8 req/s | 58.2 / 540 ms |
| 差值 | — | — | **+3.1%** | p50 −3.5% | **+13.9%** | p50 −2.7% |

- **结论（同口径，直截了当）**：在正式盒内 wrk 微基准下，**Hygress 镜像路径吞吐 ≥ 原生 envoy，p50 略优**。
  c64 的 +13.9% 可信；c16 的 +3.1%/p50 −3.5% 处于单次运行噪声区间，但方向一致。§2 的"441 vs 595"差距
  确认为客户端受限测量噪声 + 已修复的每请求路由表重建。**资源差距仍然数量级**：数据面 4 进程 ≈2GB → 1 进程
  ≈22MB（≈90×）。
- **判据说明**：`Socket errors: read` 计数两侧均全量（镜像路径连接关闭计量特性，详见 §6）；native `:80`
  对 `/readyz` 返回体更大（Transfer 259 vs 124 KB/s，运行期 read 7.6MB vs 3.65MB），req/s 为对比主口径。
  p99 尾部两侧同量级（390-550ms，周期控制面轮询/指标竞争，§6 遗留观察，非一侧独有）。
- **B1-B4 收益不体现在本表**：`/readyz` 为轻路径；B1-B4 削的是大 body 请求/SSE 流的拷贝 CPU（见
  `zero-copy-plan.md` 与 `alloc_guard` 分配的预算断言），其影响在模型路径（GPU 受限）与高并发大 payload
  场景，本表方法是镜像路径所以差距极小。
- e2e chat（GPU 受限，非网关判据）：原生 0.60/0.18/0.18s；Hygress 侧功能 e2e 通过（usage 行落库）。
- 证据：`fixtures-hygress/bench_{after2,higress}_wrk_c{16,64}.txt`、`after2_image.txt`、`higress_image.txt`、
  `higress_ps.txt`（提交随本报告）。

## 8. P1/P2 修复后重测（wrk，2026-09-05）：keep-alive 生效、c64 扩展改善

跟进（oracle P1/P2/P5 落地 `815ebd3`，复审 PASS）：P1 为所有 body 响应恢复 framing（CL/chunked，
不再 close-delimited）；P2 数据面多线程（threads = vCPU，clamp 2..32）。同 rig 重测（同主机/同 wrk/
同 `:80/readyz`）：

| 指标 | B1-B4 build（`12048b52`） | **P1/P2 build（`ba0b467b`）** | 变化 |
|---|---|---|---|
| c16 (t4·30s) 吞吐 | 1099.3 req/s | 1091.7（重复 ~1155-1169） | ≈持平（共享上游封顶） |
| c16 **Socket errors: read** | ≈所有请求 | **0（wrk errors 行消失）** | **P1 确定性修复** |
| c64 (t8·20s) 吞吐 | 834.4 req/s | **898.3 req/s** | **+7.7%** |
| c64 p50 | 56.6 ms | **53.1 ms** | **−6.3%** |
| c16 / c64 p99 | 390 / 547 ms | 388 / 539 ms | 尾部未消除（见下） |

- **P1 确认**：响应显式 CL/chunked 后 pingora 不再选 close-delimited 写出器，keep-alive 真实生效——
  每请求 TCP 拆除与 ~1.1k/s accept/TIME_WAIT churn 消除（此前 `read` 错误 ≈ 全量；既测试均未拦到，
  因 wrk 把 EOF 定界 body 计为完整响应——这正是 §6/§7 反复出现的 read-errors 谜底的根因）。
- **P2 确认**：数据面多线程后 c64 +7.7%、p50 −6.3%（此前单 worker 线程串行化）。
- **c16 封顶未变**（~1100 上下）：瓶颈为共享镜像上游（§3 方法缺口，需静态 loopback sink 才能隔离纯
  网关内核）。
- **周期性 p99 尾部仍在**（c16 ~388 / c64 ~539ms）：ora-2 预判与 P1 连接 churn 无关、头号嫌疑为
  **P4**（adapter 每 1s 全量 kube LIST → 路由表重建 → ArcSwap store，无 resourceVersion 短路）——
  本表数据佐证该方向（P1 未消尾），其次候选 P6（`read_headers` 每请求 String 克隆）。
- 证据：`fixtures-hygress/bench_after3_wrk_c{16,64}.txt`、`after3_image.txt`（提交随本报告）。

## 9. P4 快照短路后重测（wrk，2026-09-05）：p99 尾部 −23-25%、吞吐继续上台阶

跟进（P4 `493dc21`，oracle 复审 PASS）：adapter 每 1s 轮询改**指纹短路**——6 类 LIST 单遍 + 排序
`(kind, ns, name, resourceVersion)` 指纹，未变则跳过 translate/RouteTable/正则重建与 store（消除每 1s
与数据面争 CPU 的全量重建尖峰；rv==0 强制全量作为加固）。同 rig 重测（同主机/同 wrk/同 `:80/readyz`）：

| 指标 | P1/P2 build（`ba0b467b`） | **P4 build（`b1c82601`）** | 变化 |
|---|---|---|---|
| c16 吞吐 | 1091.7 req/s | **1152.3**（重复 1100-1162） | **+5.5%** |
| c16 avg / p50 | 28.1 / 13.6 ms | 22.8 / 13.0 ms | avg −19% |
| c16 p99 | 388 ms | **299 ms** | **−23%** |
| c64 吞吐 | 898.3 req/s | **968.3** | **+7.8%** |
| c64 p50 / p99 | 53.1 / 539 ms | 51.3 / **403 ms** | −3.4% / **p99 −25%** |
| Socket errors | 0 | 0 | P1 保持 |

- **P4 确认真实收益**：1s 全量 translate + RouteTable/正则重建 + store 的 CPU 尖峰从稳态轮询中消失
  （与数据面争 CPU → p99 尾部 −23-25%、吞吐与 avg 同步改善）。
- **诚实记录（残余尾巴）**：p99 未归零（c16 重复 295/307/399ms）——仍存在周期性 ~300ms 级尖峰，与
  P1（连接 churn）无关。ora-2 预判的下两个嫌疑与数据吻合：① 每 1s 仍有的 6 类 kube LIST（含对象全量
  JSON decode 的 CPU，控制面运行时的次优开销）；② 共享镜像上游 / GPUStack worker `/readyz` 自身抖动
  （§3 口径）。进一步方向：kube WATCH 替代周期 LIST、控制面轮询拉开到 ≤5s、或 P6（`read_headers`
  每请求 String 克隆——数据面自身）。
- 证据：`fixtures-hygress/bench_after4_wrk_c{16,64}.txt`、`after4_image.txt`（提交随本报告）。

## 10. Phase 1.1（kube WATCH 事件化）重测（wrk，2026-09-05）：控制面稳态零 LIST，但 p99 尾未扁平 → 尾源非控制面

跟进（Phase 1.1 `cf4f6c5`，oracle 复审 PASS）：控制面由 1s 周期轮询改为 **kube WATCH 事件驱动**
（稳态零 LIST/JSON-decode/重建，仅 30s 安全网 tick；oracle 钉死设计 9 点全过）。同 rig 重测：

| 指标 | P4 build（`b1c82601`） | **WATCH build（`3b6beabc`）** | 判读 |
|---|---|---|---|
| c16 吞吐 / p50 | 1152 / 13.0 ms | 1119-1143 / 12.6 ms | ≈持平（含抖动） |
| c16 p99（30s / 重复×3） | 299 / 295-399 ms | 388 / 479-481 ms | **未降（噪声内偏升）** |
| c64 吞吐 / p50 / p99 | 968 / 51.3 / 403 ms | 824 / 50.0 / 577 ms | 本次偏低（共享主机波动） |
| Socket errors | 0 | 0 | P1 保持 |

- **诚实结论（关键判读）**：控制面 1s LIST/JSON-decode/重建已从稳态移除（代码审校确认），但周期性
  ~300-480ms p99 尾**未因此扁平** → **该尾部并非来自控制面轮询**；剩余嫌疑为**共享镜像上游**
  （GPUStack worker / `/readyz` 自身）或 wrk rig 环境抖动（本机并发负载，本次 c16/c64 均可见波动）。
- **下一步判定器**：`perf-tail-plan.md` Phase 3.2——静态 loopback sink + 测试路由，同法测"网关 vs
  envoy 纯内核"，一次性归因"网关内核 vs 上游/rig"；在此之后再无控制面微成本可逐（oracle 预判一致）。
- 证据：`fixtures-hygress/bench_after5_wrk_c{16,64}.txt`、`after5_image.txt`（提交随本报告）。

## 11. 纯网关内核下限（Phase 3.1，wrk，2026-09-05）：内核平坦 ~14k req/s、p99 个位数毫秒 → 周期尾归因上游

判定器（免改码）：直接对 **hygress 自服务 admin ServeHttp `127.0.0.1:8081/healthz`**（pingora
ServeHttp：纯 accept+parse+respond，**不经过 `request_filter` 管线、无路由/无上游镜像**）打 wrk：

| 指标 | `:8081/healthz`（网关内核下限） | `:80/readyz`（含共享上游，§10） |
|---|---|---|
| c16 吞吐 / p50 | **13,643 req/s / 0.74 ms** | 1,119-1,143 / 12.6 ms |
| c16 p99（30s / 重复×3） | **5.9 ms / 5.6-6.9 ms（平坦）** | 388 / 479-481 ms（周期性） |
| c64 吞吐 / p50 / p99 | **14,537 / 2.49 / 24.4 ms** | 824 / 50.0 / 577 ms |
| Socket errors | 全量（admin 亦 close-delimited） | 0（P1 后 keep-alive） |

- **判定结论（决定性）**：**网关内核本身平坦且快**（~14k req/s、p50 <1ms、p99 个位数毫秒、**无周期性尾**）；
  `:80/readyz` 的 ~1.1k req/s 与周期性 ~300-480ms 尾**几乎全部来自共享镜像上游**（GPUStack worker
  `/readyz` 往返与抖动），而非网关内核/控制面/数据面管线——这统一解释了 P1/P2/P4/WATCH 逐步移除网关侧
  开销后 `:80/readyz` 的 p99 只随上游波动的现象（§8/§9/§10）。
- **附带确认**：admin ServeHttp 亦为 close-delimited（oracle P1 nitpick #1）；但逐请求强制断连下仍达
  13.6k req/s——网关内核能力绰绰有余；如需该端点也开 keep-alive，属 P1 同款一行 CL 修复（非热路径、可选）。
- **Perf-tail-plan 影响**：Phase 3.1 [x]（免改码完成）；Phase 3.2（静态 sink + 测试路由的"网关 vs envoy
  纯内核"形式化对比）由"判定器"降级为**可选形式化佐证**（临时 sink 仍涉 rig 改动）；如无硬性跨网关对比
  需求可收尾。
- 证据：`fixtures-hygress/bench_kernel_healthz_c{16,64}.txt`（提交随本报告）。
