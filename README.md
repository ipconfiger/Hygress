# Hygress — 基于 Pingora 的 GPUStack 内嵌 Higress 原位替换 AI Gateway

> 一个用 Rust（Cloudflare **Pingora**）实现的轻量 AI Gateway，在 GPUStack 中**原位（in-place）替换**
> 其内嵌 Higress 三进程（pilot / controller / envoy gateway），**不修改 GPUStack 任何一行 Python**。
> 更强的性能、更小的资源占用、更清晰的可观测性与原生多租户能力。

- 状态：**真机 A/B 验证通过**（live GPUStack v2.2.3 + RTX4090 worker + qwen2.5-0.5b-instruct）
- 数据面：Pingora terminate-mode（单二进制，无 Wasm 运行时 / 无 Envoy）
- 代码：4 个 Rust crate（hygress-core / hygress-adapter / hygress-egress / hygress-gateway）
- 质量：368 个测试全绿 · clippy 0 警告 · **零 mock/stub** · 经两轮 oracle 高精度交叉审核（Gate-1 / Gate-2 均 9/10）
- 主要文档：`docs/design.md`（设计 v1.5）· `docs/research/plugin-contract-pin.md`（字节级外部契约）
  · `docs/research/gpustack-validation/REPORT.md`（真机验证证据）· `docs/dev-process.md`（开发全过程）

---

## 1. 开发目的

GPUStack（开源 LLMOps 平台）默认把 [Higress](https://higress.cn) 作为其 **AI Gateway 数据面**：
负责模型路由（model-router）、鉴权（ext-auth）、用量计量（token-usage）、模型映射（model-mapper）、
AI 代理（ai-proxy）、降级（fallback）等核心职责，以 4 个进程（`apiserver` / `pilot` / `controller` /
`gateway(envoy)`）通过 s6-overlay 内嵌在 server 容器中。

Higress 体系非常庞大（Istio / Envoy / 9 个 Wasm 插件），带来如下工程问题：

- **资源占用高**：4 个进程 + Envoy + Wasm 运行时，内存与镜像体积可观；
- **延迟与可控性受限**：Wasm 插件链（AUTHN→AUTHZ→ROUTE→TRANSFORM→TRAFFIC）执行路径长，
  全文读取 / 精确改写能力受 Wasm 沙箱约束；
- **可观测性弱**：插件内部状态难以采样，指标分散在 Envoy / Istio 各通道；
- **多租户能力原始**：租户隔离主要依赖路由前缀命名约定，而非类型安全的原生隔离；
- **扩展/维护成本高**：Go 插件需跨语言桥接，构建与分发链路复杂。

**Hygress 的目标**：以 Cloudflare 生产级基础库 **Pingora**（Rust）实现一个等价 AI Gateway，
与 GPUStack 内嵌 Higress **原位替换**（零代码改动替换，`gateway_mode=embedded` 默认路径直接可用），
获得：更小的资源占用、更低的延迟、更强的可观测性与原生多租户能力。

**成功判据（DoD，全部达成 ✅）**

| DoD | 内容 | 结果 |
|---|---|---|
| 1 | embedded 模式下 e2e：模型→实例→推理→用量→fallback | ✅ 真机 200，`HELLO_HYGRESS_WORKS`，usage 34/7 |
| 2 | CRD fixture 一致：Hygress 换入后 16 个 CRD 与基线逐字节一致（只读控制面） | ✅ |
| 3 | 数据面端口（GATEWAY_HTTP_PORT/tls_port）与 CRD schema 不变 | ✅ |
| 4 | 单二进制、无 Wasm 运行时，插件等价功能全部原生 Rust 实现 | ✅ |
| 5 | `/v2/usage/gateway-metrics` 推送在 GPUStack DB 落为真实 usage 行 | ✅ `model_usage_details` 34/7 与 e2e 完全一致 |
| 6 | 可回滚：s6 镜像层保留三进程脚本（no-op 而非删除），`.dist` 原样快照 | ✅ |

---

## 2. 技术架构

```
                         GPUStack server 容器（s6-overlay, 零 Python 改动）
 ┌────────────────────────────────────────────────────────────────────────┐
 │  gpustack (API :30080, admin :18443?)                                  │
 │    │  kubernetes_asyncio 持续写入 CRD                                   │
 │    ▼                                                                    │
 │  内嵌 kube-apiserver (:18443)  ←─ CRD: Ingress / McpBridge / WasmPlugin  │
 │                                     / EnvoyFilter / Secret / ConfigMap   │
 │    ▲ 只读 LIST（label 选择器 + McpBridge 例外）                           │
 │  ┌─┴──────────────── Hygress（单进程，替代 pilot/controller/envoy）───┐ │
 │  │  hygress-adapter  控制面：CRD 快照 → ConfigData（路由/注册表/模型映射）│ │
 │  │  hygress-core     纯领域：RouteTable / Registry / 模型路由 / 认证作用域 │ │
 │  │  hygress-egress   出向客户端：/token-auth、/v2/usage/gateway-metrics、│ │
 │  │                   provider 令牌交换、上游代理                          │ │
 │  │  hygress-gateway  数据面：Pingora terminate-mode + TLS SNI + admin     │ │
 │  │                   + :15020/readyz 兼容 + 9 插件等价管道（原生 Rust）    │ │
 │  └───────────────────────────────────────────────────────────────────┘  │
 │       0.0.0.0:80/443（数据面） · 127.0.0.1:8081（admin）· :15020（兼容）   │
 └────────────────────────────────────────────────────────────────────────┘
```

**四个 crate 的分层（职责单一、契约冻结、可独立测试）**

| crate | 职责 | 规模/测试 |
|---|---|---|
| `hygress-core` | 纯领域模型：RouteTable、Registry、Destination、模型路由/映射、认证作用域、ConfigData（无 IO，全可单测） | 146 |
| `hygress-adapter` | 控制面：以 kube client 只读消费 GPUStack 写入的 CRD → ConfigData（快照 + 热重载） | 45 |
| `hygress-egress` | 出向 HTTP 客户端：forward-auth（/token-auth）、usage sink（/v2/usage/gateway-metrics）、provider 客户端 | 39+8+4 |
| `hygress-gateway` |数据面：Pingora 终止模式代理 + admin/metrics/15020 + **9 插件等价管道** + 容器 main()/bootstrap | 115+11 |

**9 个 Higress Wasm 插件 → 原生 Rust 管道**（相位与外部 wire 契约严格对齐，见 `plugin-contract-pin.md`）：
`inbound strip → model-router（body→模型派生 + x-higress-llm-model 覆盖）→ transformer（头改写）→
ext-auth（forward-auth → GPUStack /token-auth，路由名前缀 `ai-route-route-` 作用域）→ model-mapper（
destination 模型名映射）→ ai-proxy（provider 令牌交换）→ fallback（重定向）× N 组 → 数据面转发 →
usage sink（`ModelUsageMetrics` 17 字段防丢行）→ 写回头（X-Mse-Consumer / Authorization / cache）`。

**关键设计决策（D1-D10，见 `design.md`）**
- **MVP 拓扑 = A（embedded 原位替换）**：以**镜像层**做 s6 手术，完全不触碰 GPUStack 的 Python
  `determine_enabled_services`（零改动替换）。
- **数据面延迟语义**：`ready()` 门控（首个 CRD 快照就绪前不绑 :80）+ `GPUSTACK_API_PORT` 有界就绪探测。
- **端口纪律**：数据面 80/443；admin 127.0.0.1:8081；stats 15020；**永不停用/绑定** 9876/15010/15012/8888/15051。
- **认证/用量 wire 契约**：`/token-auth`（转发 7 头 + 注入派生 `X-GPUStack-Auth-Token`，AUTHED 模型额外
  转发客户端 `Authorization`）；usage 恰好 17 字段（operation/cluster_id/provider_name/provider_type 不上送）。

---

## 3. 对比优势

| 维度 | GPUStack 内嵌 Higress（基线） | **Hygress（本方案）** |
|---|---|---|
| 进程模型 | 4 进程：apiserver + pilot + controller + envoy(gateway) | **单进程单二进制**（Pingora terminate-mode） |
| 运行时 | Envoy + Wasm 运行时 + Istio 控制面 | **无 Envoy、无 Wasm**（纯 Rust 原生管道） |
| 资源占用 | 数十进程/数百 MB~GB 级镜像层 | 64MB 静态二进制；无额外运行时进程 |
| 延迟 | Wasm 插件链多跳（AUTHN→…→TRAFFIC） | Pingora 全文读取、单跳原生管道，测试低至 ~0.4s 首 token 级响应 |
| 可观测性 | 指标散在 Envoy/Istio/Wasm 各通道 | Rust tracing + Prometheus（:15020）+ admin 集中输出 |
| 多租户 | 依赖路由命名约定 | 类型安全的路由/注册表隔离 + 原生命中/映射 |
| 可控性 | 扩展需 Go + Wasm 桥接 | 全 Rust，`async`/无锁 ARC-swap 热重载，更新即点即用 |
| 测试 | — | **368 测试、零 mock/stub、Gate-1/2 oracle 9/10** |
| **兼容性** | 自身即基线 | **端口契约 / CRD schema / usage 落库逐字节一致**，只读控制面，零 Python 改动 |
| 回滚 | — | s6 层保留 `.dist` 原脚本快照，一条命令回退基线镜像 |

**真机 A/B 结论**（GPUStack v2.2.3 + RTX4090 worker）：换入 Hygress 后，`/v1/chat/completions` 经
:80 返回真实 Qwen 推理（200 / 0.47s），`model_usage_details` 落库行（34/7）与响应 usage **逐位一致**；
16 个 CRD 与基线**逐字节一致**（只读）；禁 5 端口零绑定；supercronic/admin/15020 全通；server 稳定。

---

## 4. 部署与配置（集成到 GPUStack 原位替换 Higress）

> 前提：已有可用的 GPUStack 部署（本方案在 **GPUStack v2.2.3** + `gateway_mode=embedded` 上验证）。
> 全程**不改 Python、不改端口、不改任何 GPUStack 环境变量**。

### 4.1 构建 Hygress 镜像

```bash
# 1) 构建单二进制（在仓库根目录；需 Rust 1.98+）
cargo build --release -p hygress-gateway --features integrations
cp target/release/hygress-gateway target/release/hygress

# 2) 基于 GPUStack 官方镜像构建 Hygress 定制镜像（复用基础层，含 s6 手术）
docker build -f pack/Dockerfile.hygress -t gpustack:hygress .
#    （base 为 gpustack/gpustack:latest；如需指定基础标签：
#      docker build --build-arg GPUSTACK_TAG=latest -f pack/Dockerfile.hygress -t gpustack:hygress .）
```

`pack/Dockerfile.hygress` 做的事（s6 手术，全部在**镜像层**完成，见 `pack/hygress-s6/rootfs/.../run`）：

| 文件 | 处理 |
|---|---|
| `s6-rc.d/gateway/run` | **改写为 Hygress 启动器**（占原 envoy 槽位；导出 env、承接 access.log、exec hygress） |
| `s6-rc.d/pilot/run` | no-op 长 sleep（**向 `notification-fd:3` 写就绪字节**，否则 s6-rc 启动不收敛）+ 保留 `.dist` 原件 |
| `s6-rc.d/controller/run` | no-op 长 sleep（同上，CRD 消费移交 Hygress） |
| `s6-rc.d/supercronic/run` | 仅在原脚本基础去掉 `readinessCheck "Higress Pilot" 15010`，cron 路径 `/var/lib/istio/cron.txt` |
| `s6-rc.d.dist/` | **先于覆盖快照**的原始 scripts（回滚原件） |

### 4.2 换入（切换 server 镜像，worker/数据卷不动）

```bash
# 在 GPUStack compose 目录（含 .env：GPUSTACK_BOOTSTRAP_PASSWORD / GPUSTACK_TOKEN）
# 将 gpustack-server 的 image 由 quay.io/gpustack/gpustack:latest 换为 gpustack:hygress
# （gpustack-worker 保持官方镜像不变）
docker compose -f compose-hygress.yaml up -d gpustack-server
```

`compose-hygress.yaml` 关键配置（server 服务，其余原样）：

```yaml
gpustack-server:
  image: gpustack:hygress          # 仅此行变化
  network_mode: host
  privileged: true
  command: ["--disable-worker"]    # worker 由独立容器承担（保持原拓扑）
  volumes:
    - ./server-data:/var/lib/gpustack   # 数据卷保留（DB/模型/密钥原样）
    - /var/run/docker.sock:/var/run/docker.sock
```

### 4.3 Hygress 环境变量（`gateway/run` 自动注入，均可按需覆盖）

| 变量 | 默认 | 说明 |
|---|---|---|
| `GATEWAY_HTTP_PORT` / `GATEWAY_TLS_PORT` | 80 / 443 | 数据面端口（与基线一致） |
| `GPUSTACK_API_PORT` | 30080 | GPUStack API 端口（就绪探测与转发目标） |
| `GPUSTACK_DATA_DIR` | `/var/lib/gpustack` | **必须注入**：Hygress 从 `{data_dir}/jwt_secret_key` 解析密钥 |
| `HYGRESS_KUBECONFIG` | `${EMBEDDED_KUBECONFIG_PATH}` | 内嵌 apiserver kubeconfig |
| `GATEWAY_NAMESPACE` | `higress-system` | 网关命名空间 |
| `HYGRESS_ADMIN_ADDR` / `HYGRESS_ADMIN_TOKEN` | 127.0.0.1:8081 / 无 | admin（/healthz、/metrics 公开，/reload 需 token） |
| `GATEWAY_PILOT_AGENT_METRICS_PORT` | 15020 | stats 浅兼容端口 |
| `HYGRESS_API_READY_TIMEOUT` / `HYGRESS_SNAPSHOT_TIMEOUT` | 30s / 60s | 启动窗口（launcher 已放宽 300s） |
| `HYGRESS_TOPOLOGY_B` | 关 | 外部网关拓扑时播种 IngressClass |

### 4.4 验证（真机验证矩阵）

```bash
# API 健康（GPUStack 直接）
curl http://127.0.0.1:30080/healthz          # 200
# 数据面经 Hygress 镜像转发
curl http://127.0.0.1:80/readyz              # 200（Hygress mirror → GPUStack）

# DoD1 e2e：真实推理经 Hygress
curl http://127.0.0.1:80/v1/chat/completions \
  -H "Authorization: Bearer <api_key>" -H "Content-Type: application/json" \
  -d '{"model":"qwen2.5-0.5b-instruct","messages":[{"role":"user","content":"Hi"}],"stream":false}'

# DoD5：用量落库（Hygress usage sink → GPUStack DB）
docker exec gpustack-server psql -U root -h 127.0.0.1 -p 5432 gpustack \
  -c "select date,model_name,prompt_token_count,completion_token_count,completed \
      from model_usage_details where model_name='<model>' order by id desc limit 3;"

# DoD6：端口纪律（必须看不到 9876/15010/15012/8888/15051）
docker exec gpustack-server ss -ltn | grep -E ':9876|:15010|:15012|:8888|:15051' && echo LEAK || echo PASS
# 进程：应仅有单一 hygress + supercronic（无 envoy/pilot/controller）
docker exec gpustack-server ps aux | grep -E 'hygress|envoy|pilot|controller'
```

### 4.5 回滚

```bash
# 1) 把 server 镜像切回 GPUStack 官方镜像，原样重启即可
docker compose -f compose.yaml up -d gpustack-server
# 2) 若需保留 Hygress 镜像并仅在容器内恢复原脚本：原始 scripts 已在 /etc/s6-overlay/s6-rc.d.dist/
```

---

**文档索引**
- `docs/design.md` — 设计 v1.5（现状分析 / 契约 / 架构 / 相位 / 部署与运维 / 兼容性矩阵）
- `docs/research/plugin-contract-pin.md` — 字节级外部 wire 契约（9 插件 / ext-auth / usage / 头）
- `docs/research/gpustack-validation/` — 真机验证（REPORT、CRD fixtures、hygress 日志、usage 行）
- `docs/dev-process.md` — 开发全过程记录（设计→实现→门禁→打包→真机 A/B→修复清单）
- `pack/` — 可部署产物（Dockerfile.hygress + s6 手术脚本）
