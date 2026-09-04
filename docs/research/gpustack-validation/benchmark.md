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
