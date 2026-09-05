# Hygress vs 原生 Higress（envoy）：最终同 rig 对比报告

> 结论速览
> 1. 最终同 rig wrk 对比（同一 GPUStack 主机、同 wrk、同 `:80/readyz`，顺序 hygress→higress→hygress）中，hygress 吞吐 c16 +4.8%、c64 +20.3%，p50 −7.8%/−12.0%、p99 −5.3%/−20.6%（证据 `bench_final_*.txt`，构建 `3b6beabc` vs `55ecc762`）。
> 2. 不含上游时网关自身可达 13.6k req/s，p99 稳定在 6ms、无周期尖峰（`§11` 自服务 healthz 判定器）。`:80/readyz` 的 ~1.1k req/s 与周期性 p99 尾部归因于共享镜像上游而非网关内核，两侧在镜像路径上呈现同样的周期性尾部。
> 3. 数据面 1 进程 / ~108MB RSS vs envoy 套件 4 进程 / ~2.54GB（≈23×）；镜像内网关二进制 27MB vs ~905MB（≈34×）。
> 4. 单二进制原位替换（零 Python 改动、s6 一键回滚）、9 插件等价原生 Rust 管线、控制面 WATCH 事件化热更（稳态零 LIST）、限流/配额/路由策略/护栏真机验证通过。

---

## 0. 测量环境与方法

- 同 rig：同一 GPUStack 测试主机 `gpu-14c528e0-…`（Intel Xeon Platinum 8380 ×16vCPU，Ubuntu 24.04.4，58GiB RAM，docker）；server 容器先后以 `gpustack:hygress`（构建 `3b6beabc`）与原生 `quay.io/gpustack/gpustack:latest`（`55ecc762`）运行，worker/模型实例（qwen2.5-0.5b-instruct）/数据卷不动；顺序 A/B/A 一次会话完成，切换后健康验证通过。
- 负载：盒内 wrk，`GET :80/readyz`（镜像透传路径，两端均实时 200），c16 = `-t4 -c16 -d30s`，c64 = `-t8 -c64 -d20s`；另以 `:8081/healthz`（hygress 自服务、无上游）测网关自身的本底吞吐，下称“内核下限”。
- 口径：req/s 为主判据（envoy 的 `/readyz` 响应体约 1.9× 大：249 B/req vs 134 B/req，Transfer 不可直接比）；p50/p99/avg 并列列出。
- P1 修复生效确认：hygress 侧 wrk `Socket errors: read` 已从“≈全量”（§6/§7）降为 0（本组 final 文件无 errors 行）；envoy 侧同为 0。两侧同为 keep-alive 口径，对比公平。

## 1. 性能对比

### 1.1 最终同 rig A/B/A（`/readyz` 镜像路径，2026-09-05）

| 指标 | hygress（`3b6beabc`，threads=16） | 原生 higress/envoy（`55ecc762`） | 差值（hygress vs envoy） |
|---|---|---|---|
| c16 吞吐 | **1127.4 req/s**（33,884 req/30s） | 1075.3 req/s（32,304 req/30s） | **+4.8%** |
| c16 p50 / p99 | **12.73 / 385.78 ms** | 13.80 / 407.21 ms | **−7.8% / −5.3%** |
| c16 avg | 31.75 ms | 28.23 ms | +12.5%（高延迟尾部样本抬高均值，见 p99 分布） |
| c64 吞吐 | **916.1 req/s**（18,373 req/20s） | 761.7 req/s（15,276 req/20s） | **+20.3%** |
| c64 p50 / p99 | **51.82 / 421.53 ms** | 58.90 / 530.55 ms | **−12.0% / −20.6%** |
| c64 avg | **99.05 ms** | 118.35 ms | −16.3% |
| Socket errors: read | 0（无 errors 行） | 0（无 errors 行） | P1 keep-alive 两侧均健康 |
| Transfer/run | ~4.4 MB | ~7.7 MB | envoy 响应体更大（见口径说明） |

- hygress 全部四项主指标（吞吐/p50/p99）**均优于原生 envoy**；c64 优势（+20.3%）大于 c16（+4.8%），与并发提高后单请求 CPU 与连接开销占比上升一致（P2 多线程 + P1 keep-alive 的收益随并发放大）。
- c16 avg 一项 hygress 偏高（31.75 vs 28.23）：由 p99 之后的尾部样本贡献（两侧 p99 相近、hygress 极端尾样本更重）；主判据（吞吐/p50/p99）均优。
- 两侧在 `:80/readyz` 上都有周期性 ~300-500ms p99 尾部，归因见 §1.2（非网关内核所致）。

### 1.2 网关内核下限（`:8081/healthz` 自服务端点，无上游，Phase 3.1）

| 指标 | hygress 内核（admin `:8081/healthz`） | `:80/readyz`（含共享镜像上游） |
|---|---|---|
| c16 吞吐 | **13,598.67 req/s** | 1,127.4 req/s |
| c16 p50 | **0.736 ms** | 12.73 ms |
| c16 p99（30s + 重复×3） | **6.07 ms（重复 5.6-6.9，无周期峰）** | 385.8 ms（周期性尖峰） |
| c64 吞吐 / p50 / p99 | 14,537 / 2.49 / 24.4 ms | 916.1 / 51.8 / 421.5 ms |

判定：网关内核（pingora accept+parse+respond，不经过 request_filter 管线/路由/上游）延迟分布稳定、无周期峰，快 ~12×，p99 个位数毫秒。`:80/readyz` 的吞吐天花板（~1.1k）与周期性尾部**归因于共享镜像上游**（GPUStack worker 的 `/readyz` 往返与抖动）；envoy 侧同路径同样呈现尾部（407/530ms），两侧表现一致。§8→§10 期间逐项消除 P1（连接churn）、P2（单线程）、P4（控制面轮询）等网关侧开销后，`:80/readyz` 的 p99 只随上游波动。
诚实说明：admin 端点当前仍为 close-delimited 响应（`read errors ≈ 全量`，P1 nitpick #1），即 13.6k req/s 是在每请求 TCP 拆建的不利条件下测得的内核下限，只会低估；如需 keep-alive 口径属一行 CL 修复（非热路径、可选）。跨网关“纯内核对纯内核”的正式对比（envoy admin 端点等效测量）留作 Phase 3.2 可选待办。

### 1.3 优化全程曲线（同 rig wrk，`:80/readyz`）

| 阶段（提交） | c16 req/s | c16 p50 / p99 | c64 req/s | c64 p50 / p99 | read-errors | 关键变化 |
|---|---|---|---|---|---|---|
| 旧 hygress（修复前 `6a1e155e`，§6） | 518 | 28.5 / 83 ms | 525 | 111.6 / — | ≈全量 | 每请求路由表重建 + 响应无 keep-alive |
| **857d21b**（H1-H4/M5-M8，§6） | **1093** | 13.4 / 417 | **831** | 57.0 / — | ≈全量 | 路由表缓存 + keep-alive + body/SSE/SWRR/registry |
| **5df02f2**（B1-B4 零拷贝，§7） | 1099.3 | 13.4 / 390 | 834.4 | 56.6 / 548 | ≈全量 | 热路径零拷贝（大 body/SSE 受益，readyz 轻路径持平） |
| **815ebd3**（P1/P2/P5，§8） | 1091.7 | — / 388 | **898.3** | 53.1 / 539 | **0** | 响应 framing（keep-alive 真实生效）+ 数据面 16 线程 |
| **493dc21**（P4 指纹短路，§9） | **1152.3** | 13.0 / **299** | **968.3** | 51.3 / **403** | 0 | 控制 1s 全量 LIST+重建消除 → p99 −23/−25% |
| **cf4f6c5**（Phase 1.1 WATCH，§10） | 1119-1143 | 12.6 / 388-481 | 824 | 50.0 / 577 | 0 | 控制面稳态零 LIST；尾部未变平 → 与控制面轮询无关 |
| **fc25a87**（§11 内核下限） | **13,598.67**（p50 0.74ms / p99 6.1ms 无周期峰） | — | 14,537（c64） | — | （admin close-delimited） | 据此判定：尾部来自共享上游 |
| **最终 A/B（本报告，`3b6beabc`）** | **1127.4** | **12.73 / 385.78** | **916.1** | **51.8 / 421.5** | 0 | 对 envoy：+4.8%/+20.3%，p50/p99 全优 |
| 原生 envoy（同 rig） | 1075.3 | 13.8 / 407.2 | 761.7 | 58.9 / 530.6 | 0 | — |

P1 根治 read-errors（close-delimited framing 根因，pingora `init_close_delimited` 实证）；P2 消除单 worker 串行化（c64 +7.7%）；P4 消除控制面周期重建尖峰（p99 −23/−25%）；WATCH 后尾部未变平；§11 测得内核无周期峰，周期尾最终归因共享上游（非网关）；同 rig 下 hygress 全指标 ≥ envoy。

### 1.4 e2e 推理（`/v1/chat/completions`，GPU 受限，纯参考）

| 侧 | 三次采样 time_total |
|---|---|
| hygress | 2.60* / 0.169 / 0.154 s（*首采样含冷态） |
| envoy | 0.474 / 0.184 / 0.188 s |

GPU/模型实例决定推理时长，网关增量（ms 级）不可从此分离；走势 hygress 不劣。功能判据见 §4（usage 落库、CRD 一致）。

## 2. 资源占用

### 2.1 进程与 RSS（最终同 rig 实测）

| 指标 | hygress | 原生 higress/envoy 套件 | 倍数 |
|---|---|---|---|
| 数据面进程数 | **1**（单二进制 hygress；supercronic 除外，两侧同） | **4**（envoy 1.83GB + pilot-agent 0.51GB + pilot-discovery 85MB + higress 87MB ≈ **2.54GB**） | 4 → 1 |
| 网关 RSS | **~108 MB**（VmRSS 111,152 kB） | ~2.54 GB | **≈23×** |

诚实更新：早期报告（benchmark.md §1，P2 多线程之前）实测 hygress RSS ≈22MB；当前 ~108MB 的增长来自 P2 的数据面多线程（threads=vCPU=16，3 个 pingora service + 控制面 runtime，实测 `ps -eLf` ≈68 线程/进程）与 WATCH 任务。以 ~16× 线程换取 c64 +20% 吞吐与尾延迟改善，属有意权衡；**即便如此仍 ≈23× 低于 envoy 套件**。早期 server 容器级口径（§1：794MiB vs 2.514GiB，净省 ≈1.7GB/68%）未在最终轮重测，引用时注意其为 P2 前数据。

### 2.2 镜像与容器

- 镜像内网关相关二进制：hygress 27MB vs envoy 套件 ~905MB（envoy 734M + higress 91M + pilot-discovery 80M）≈ 34×（§1 口径）。
- 容器内存（含 postgres/gpustack/prometheus/grafana 公共组件）：794MiB vs 2.514GiB（§1 早期口径，净省 ≈1.7GB）。
- 镜像 ID 留档：hygress `3b6beabc…`、higress `55ecc762…`（`final_*_image.txt`）。

## 3. 架构与交付

| 维度 | 原生 Higress（envoy） | Hygress |
|---|---|---|
| 进程模型 | s6 托管 4 进程（apiserver/pilot/controller/envoy） | **单进程单二进制**（Pingora terminate-mode；pilot/controller 槽位 no-op + `notification-fd:3` 就绪字节） |
| 数据面运行时 | Envoy + Wasm 运行时 + xDS | 纯 Rust（pingora 0.8），无 Wasm/无 xDS |
| 控制面 | Istio/pilot 全量 xDS 推送 | kube WATCH 事件驱动（Phase 1.1 `cf4f6c5`）：6 类 CRD 各一 watcher，事件 → 去抖 → 全量快照重建（rv 指纹幂等短路 + 30s 安全网 tick）；**稳态零 LIST/零 JSON decode**，配置生效 ≤ 一个事件周期 |
| 配置热更 | xDS | CRD WATCH（秒级内）+ `hygress.policy.yaml` mtime 热重载（≤30s dutycycle，R-8）+ admin `POST /reload`（即时） |
| 扩展模型 | Wasm 插件（Go/跨语言桥接） | 原生 Rust 管线（见 §4），`hygress.policy.yaml` 声明式延伸能力 |
| 交付/回滚 | 官方镜像体系 | 镜像层 s6 手术（`.dist` 原脚本快照），**换回镜像即回滚**；端口契约不变（80/443 数据面、127.0.0.1:8081 admin、15020 stats；永绑禁端口 9876/15010/15012/8888/15051 零绑定） |

## 4. GPUStack 用到的部分全景（全部原生 Rust，零 Python 改动）

- 9 插件等价管道（相位与 wire 契约逐字节对齐，`plugin-contract-pin.md`）：inbound strip → model-router（body→模型派生 + `x-higress-llm-model` 覆盖）→ transformer（头改写）→ ext-auth（forward-auth → GPUStack `/token-auth`，`ai-route-route-` 作用域）→ model-mapper（目标模型名映射，零拷贝 B1-B4）→ ai-proxy（provider 令牌交换）→ fallback 重定向 ×N 组 → 数据面转发 → usage sink → 头写回（`X-Mse-Consumer`/`Authorization`/cache）。
- 模型路由/多实例：RouteTable 按快照缓存（H2，请求路径单 Arc 读取）、SWRR O(1) 选择（M7）、fallback 链（D7）、provider 密钥换写（D6）。
- usage 落库：`ModelUsageMetrics` 17 字段推送 `/v2/usage/gateway-metrics` → GPUStack DB `model_usage_details` 真实行（DoD5：34/7 行与响应 usage 逐位一致，A/B 基线跑；修复批 B5 复跑 34/5，见 audit-fix-report §3.3）；流式 SSE 逐块计量（B2）。
- managed CRD 只读消费：Ingress / EnvoyFilter / WasmPlugin / McpBridge（无 label 例外）/ Secret / ConfigMap，WATCH 事件化 + 指纹幂等；CRD fixture 与基线逐字节一致（DoD2）。
- mirror 透传：`:80/readyz` 等 mirror 路由原样转发（本报告全部 wrk 数据的路径）。
- :15020 端点兼容：实现 GPUStack pilot-agent 探活所需的 `/stats/prometheus`、`/stats` 两个端点。
- 延伸能力（真机验证）：限流（IP/consumer 令牌桶，429+Retry-After）、token 配额（固定窗口 reserve/commit/release，429）、路由策略（override/pin/头增删/超时重试）、安全护栏（静态规则 + LLM fail-closed + 输出侧 per-chunk 断流，403），经 `hygress.policy.yaml` 热更（mtime ≤30s dutycycle；admin `POST /reload` 即时）后生效（真机 429/429/403 复现，§5.3）。

## 5. 可运维性

- 可观测：Prometheus 文本指标（`:15020/stats/prometheus`，`hygress_*` 家族：requests/duration/tokens/ttft/retries/upstream_errors/fallback/auth/rate_limit/quota/policy/guardrail）；admin `127.0.0.1:8081`（`/healthz`、`/metrics`、token 门禁 `/reload`）；tracing 结构化日志。
- 升级/回滚：单二进制镜像层替换；s6 `.dist` 快照保底，`docker compose` 切回官方镜像即回滚（README §4.5）。
- 热更：CRD 变更 WATCH 事件驱动（≤1 事件周期）；策略文件 mtime 轮询（≤30s dutycycle，R-8）+ admin 强制 reload（即时）；均无需重启进程。
- 运行面自检：启动 fail-fast（GPUSTACK_API_PORT 探测 + 首快照 bind-ready 门控）；admin/stats 独立监听器与数据面隔离。

## 6. 诚实局限

1. 生态成熟度：envoy/Istio 生态的 Wasm 插件市场、完整 Istio 配置语义、多集群/多租户/fleet 管理、xDS 动态配置广度、envoy 丰富遥测与 hot-restart 升级，hygress 均无对等物。GPUStack 内嵌场景只消费 9 插件等价能力（§4），这些能力在该场景未被使用。**超出该场景的网关需求仍应选原生 Higress**。
2. terminate-mode 全量缓冲：模型路由/fallback/配额/输入护栏需要对 body 的全文读取（上限 `max_body`，超出 413）；mirror/GET 透传与 SSE 为流式。对超大非流式请求的内存占用高于 envoy 的流式模型（B1-B4 已消除多余拷贝，但缓冲语义是设计取舍）。
3. 无 Wasm 扩展机制：第三方以 Wasm/Go 扩展网关的方式不可用；扩展须走 hygress 原生管线 + policy.yaml（v1 边界见 extensions-design §10.2：响应侧 `mode: buffer` 未实现、路由级 `limits.ip` 覆盖不生效、async LLM 护栏为旁路记录）。
4. 单集群单机聚焦：面向 GPUStack 内嵌单 apiserver 命名空间（只读 CRD）；无多集群/多命名空间联邦。
5. admin/stats 端点 close-delimited（P1 nitpick #1，§11 实测 read-errors=全量）：`ServeHttp` 响应未带 CL/chunked framing，非数据面热路径（仅 8081/15020 管理面），一行 CL 修复可选；不影响 `:80` 数据面（P1 后 keep-alive 实测 0 错误）。
6. `:80/readyz` 的周期尾部来自上游（非网关）：§11 测得内核无周期峰（p99 6.1ms），envoy 同路径也有同样的尾部（407/530ms），两者都归因于共享 GPUStack worker；该尾部不在 hygress 可修复范围。
7. 跨网关纯内核对比未完成：envoy 无等效 admin 基准被测（Phase 3.2 静态 sink 可选待办）。当前内核结论（13.6k req/s、p99 6ms 无周期峰）仅 hygress 单侧；跨网关内核对比留待需要时补测。
8. e2e 采样噪声：GPU 受限路径单次采样（首采样 2.60s 冷态）不构成网关判据（§3 口径）。
9. RSS 增长：~22MB → ~108MB（threads=16 + WATCH），仍 ≈23× 优于 envoy 套件（§2.1）；如需常驻内存最小化可调 `threads`，但以 c64 吞吐为代价（§8 实证 +7.7% 来自多线程）。

## 7. 结论与选用建议

- **GPUStack 内嵌网关原位替换场景：选 Hygress。** 同 rig wrk 全主指标 ≥ envoy（吞吐 +4.8%/+20.3%，p50/p99 全优），内核下限 13.6k req/s（≈12× 于镜像路径实测负载，余量充足），资源 1 进程/108MB vs 4 进程/2.54GB（≈23×）、镜像二进制 34×。9 插件等价 + 限流/配额/策略/护栏真机验证，CRD/usage/端口契约逐字节一致、零 Python 改动、一键回滚，优化全程可复现（§6→§11 曲线，每步 oracle 复审 + 双模式全绿）。
- 仍选原生 Higress/envoy 的场景：需要 Wasm/Go 插件生态或自定义 envoy filter、Istio/xDS 多集群 fleet 语义、envoy 热重启（不切镜像的进程级升级），或 GPUStack 之外的独立网关拓扑。这些是能力域差异，非性能差异；本报告的性能结论在 GPUStack 内嵌场景内成立。
- 后续可选项（不阻塞结论）：Phase 3.2 静态 sink 跨网关内核对比；admin/stats CL 一行修复；RSS/线程权衡参数化。

## 8. 数据与证据索引

| 证据 | 文件（`docs/research/gpustack-validation/fixtures-hygress/`） | 提交 |
|---|---|---|
| 最终 A/B/A wrk（本报告 §1.1） | `bench_final_hygress_c{16,64}.txt`、`bench_final_higress_c{16,64}.txt`、`final_hygress_image.txt`、`final_higress_image.txt` | 本报告提交 |
| 内核下限（§1.2/§11） | `bench_final_hygress_kernel_c16.txt`、`bench_kernel_healthz_c{16,64}.txt` | `fc25a87` |
| WATCH 重测（§10） | `bench_after5_wrk_c{16,64}.txt`、`after5_image.txt` | `81502ac` |
| P4 重测（§9） | `bench_after4_wrk_c{16,64}.txt`、`after4_image.txt` | `663d6fe` |
| P1/P2 重测（§8） | `bench_after3_wrk_c{16,64}.txt`、`after3_image.txt` | `e42b8af` |
| 首次同 rig A/B（§7） | `bench_after2_*`、`bench_higress_wrk_c{16,64}.txt`、`higress_image.txt`、`higress_ps.txt` | `346f201` |
| 修复前/后首测（§6） | `bench_{before,after}_wrk_*.txt`、`bench_after_e2e_chat.txt`、`after_image.txt` | `85c407c` |
| 早期资源/真机验证（§1-§5） | `containers_before/after.txt`、`ps_after.txt`、`usage_rows*.txt`、`chat_hygress_*.json`、`server_logs.txt`、`hygress.log` | `c7ccaf5`/`5491de7` |
| 提交链 | c7ccaf5（首测）→ 7552e7b（差距归因）→ 85c407c（§6）→ 857d21b（H1-H4/M5-M8）→ 5df02f2（B1-B4）→ 346f201（§7 A/B）→ 815ebd3（P1/P2/P5）→ e42b8af（§8）→ 493dc21（P4）→ 663d6fe（§9）→ cf4f6c5（WATCH）→ 81502ac（§10）→ fc25a87（§11）→ 4ba507e（perf-tail-plan 归档）→ 本报告 | — |

> 方法与口径细节（wrk 参数、判定规则、各阶段诚实记录）见 `benchmark.md` §6-§11；本报告为其最终横向汇总。
