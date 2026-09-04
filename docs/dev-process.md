# Hygress 开发全过程记录

> 记录自初始调研到真机 A/B 验证通过的完整开发历程、决策、质量门禁与真机发现的修复清单。
> 本文件是过程性档案；权威的设计/契约/验证结论以 `docs/design.md`（v1.5）、
> `docs/research/plugin-contract-pin.md`、`docs/research/gpustack-validation/REPORT.md` 为准。

- 项目：Hygress —— 基于 Pingora 的 GPUStack 内嵌 Higress 原位替换 AI Gateway
- 周期：2026-09-02 → 2026-09-03
- 终态：**368 测试全绿 / clippy 0 / 零 mock-stub / Gate-1·Gate-2 oracle 9/10 / 真机 DoD 1·2·5-DB·6 全 PASS**

---

## 1. 起点与目标

GPUStack（LLMOps 平台）把 Higress 作为内嵌 AI Gateway 数据面：4 进程（apiserver/pilot/controller/envoy）
+ 9 个 Wasm 插件（model-router / ext-auth / token-usage / model-mapper / ai-proxy / transformer /
set-model-pre-route / fallback 等），通过 s6-overlay 内嵌于 server 容器。Higress 在 GPUStack 里依赖
**代码级耦合**：GPUStack 的 Python 服务用 `kubernetes_asyncio` 持续把自身状态翻译成 CRD
（Ingress / McpBridge / WasmPlugin / EnvoyFilter / Secret / ConfigMap），Higress 控制器消费后下发 Envoy。

**目标**：用 Rust（Cloudflare Pingora）实现等价 AI Gateway——**Hygress**，与 Higress **原位替换**，
且**不改 GPUStack 一行 Python**（零改动替换），获得更小资源、更低延迟、更强可观测性、原生多租户。

借用此前 dogress2（Hydra：Pingora AI 网关）已验证的生产架构作为干线参考。

---

## 2. 阶段总览

| 阶段 | 内容 | 产出 |
|---|---|---|
| 1 调研 | Higress 插件链 / GPUStack 控制面 / wire 契约 | `plugin-contract-pin.md`（字节级） |
| 2 设计 | v1.0 → v1.4（oracle 三轮设计审核） | `design.md` |
| 3 实现 | TDD 并行 lane：4 crate | 368 测试、零 mock |
| 4 门禁 | Gate-1（核心契约）→ 修复 → Gate-2（实现 vs 设计交叉审计，三轮） | 9/10 APPROVED |
| 5 打包 | s6 手术镜像层方案 | `pack/Dockerfile.hygress` + `pack/hygress-s6/` |
| 6 真机 | 基线部署 → A/B 换入 → 7 项真机缺口修复 → 全 DoD | REPORT + fixtures |

---

## 3. 调研：把"语义契约"落到"字节契约"

初期按 Higress 文档与 GPUStack 源码建立插件语义，在实现前用两轮深度调研（exp-3/exp-4）把**每个外部边界钉死到字节级**：

- GPUStack 控制面权威：`gpustack/gateway/__init__.py`（9 插件的 name/phase/priority/版本/spec 形状）、
  `gateway/utils.py`、`gateway/plugins.py`；
- 服务器权威（消费侧 wire）：`routes/token.py`（`/token-auth` 响应头）、`server/metrics_collector.py`
  （usage 落库字段）、`api/auth.py`（派生网关 token）、`config/config.py`（端口/网关模式）；
- Higress 插件实现权威：`extensions/wasm-go/extensions/{transformer,model-router,model-mapper,ext-auth}/main.go`。

关键定案（全部进 `plugin-contract-pin.md`）：
- **usage 恰好 17 字段上传**（`ModelUsageMetrics`），`operation`/`cluster_id`/`provider_name`/`provider_type`
  由服务器归类，网关**不上送**；丢行闸门（model_id/provider_id）在实现期抓出；
- **ext-auth 作用域 = 路由名前缀 `ai-route-route-`**（绝非路径前缀，防 FAIL_OPEN 洞）；
  `/token-auth` 转发 7 头 + 注入派生 token；`X-Mse-Consumer` 公共策略哨兵 `'none'`；
- **route-name 裸/ns 双形式**（embedded 同命名空间时为裸名）——实现期 D9 抓出；
- **model-mapper 键格式**：matchRule 用 `name.type`（无端口）、destination 用 `name.type:port`。

> 教训：**"功能等价"必须落到"字节等价"**——若不钉死字节，真机集成必然被服务端的隐性收紧拦下。

---

## 4. 设计：三轮 oracle 审核（v1.0 → v1.4）

- **v1.0** 初稿 → oracle NEEDS-REVISION（6.5/10）：9 阻塞 + 14 非阻塞修订
  （s6 手术方案、supercronic 依赖、jwt 取点定案、usage 完整字段、ext-auth 路由名作用域、
  策略 2 删本地 SQLite、拓扑 B 播种 IngressClass、插件相位修正）。
- **v1.2** oracle APPROVED-WITH-MINOR-FIXES（8.5/10）：端口排除 15012、logrotate/access.log 承接、
  no-op 用长 sleep 禁 exit 0、route-name 格式 L0 待核、`X-Mse-Consumer 'none'` 哨兵。
- **v1.3** 依 Gate-1 代码门禁 + 契约定案：§6.2 匹配语义改为 **Higress AND 语义**（Main 仅 header+path
  全匹配、Fallback 独立索引、mirror 唯一 path 兜底）、RouteRule 增加 `ingress_name`/`main_ingress_name`
  与 Main/Fallback 键空间隔离、SWRR 共享状态、usage 保留上游 total_token、`operation` 取服务器枚举。
- **v1.4** 实现落地 + Gate-2 三轮交叉审计（6.5→8.5→9/10）：B1-B4（usage 补 model_id/provider_id、
  model-router 热接线、真实启动序列、鉴权写回改**替换**防客户端 key 泄漏）+ D5-D10。
- **v1.5** 真机 A/B 完成后更新（DoD 全 PASS + 7 项真机变更点，见 §7）。

核心决策（D 系列）：**MVP 拓扑 A = embedded 原位替换**（镜像层 s6 手术，不碰 Python）；数据面
Pingora 终止模式；`ready()` 门控（首快照前不绑 :80）+ 有界就绪探测（快速失败而非 10 分钟挂起）；
端口纪律；v1 仅承诺 OpenAI 兼容子集 + Anthropic 透传（ai-proxy），非 OpenAI provider 优雅透传。

---

## 5. 实现：TDD 并行 lane，零 mock/stub

按 crate 分解为并行实现 lane（各 lane TDD、契约冻结、库存/stub 全部避免）：

| crate | 实现要点 | 测试 |
|---|---|---|
| `hygress-core` | RouteTable/Registry/Destination/模型路由映射/认证作用域/ConfigData——纯领域、无 IO | 146 |
| `hygress-adapter` | kube 只读 CRD 消费 → ConfigData；快照 + 热重载；命名空间/标签选择器 | 45 |
| `hygress-egress` | `/token-auth` forward-auth、`/v2/usage/gateway-metrics` sink、provider 客户端、令牌派生 | 39+8+4 |
| `hygress-gateway` | Pingora 终止模式、admin/metrics/15020(readyz)/TLS-SNI、9 插件等价管道、bootstrap/main | 115+11 |

**质量约定**：一切走真实 IO（无 mock）；覆盖错误路径（fail-fast 语义、FAIL_OPEN、非 2xx 用量）；
e2e 集成测试用真实 Pingora + 真实本地上游/认证/用量服务。最终 **368 测试 / clippy 0 / 零 mock-stub**。

### 5.1 质量门禁（oracle 高精度交叉审计）

- **Gate-1 核心契约**：6.5 → 9/10 通过（补 main/Fallback 键空间、SWRR、17 字段 usage 等）。
- **Gate-2 实现 vs 设计交叉审计**：三轮 6.5 → 8.5 → **APPROVED 9/10**；消除 B1-B4 与 D1-D10 全部问题，
  含 ai-proxy provider 令牌交换（D6）、裸/ns route-name（D9）、死状态清理。
> 门禁原则：每个修复后重新过 oracle 直到干净；测试必须是"真实 GPUStack 可用"的证据链。

---

## 6. 打包：s6 手术（镜象层，零 Python 改动）

`pack/`：
- `Dockerfile.hygress`（builder 兼容）：基于 `gpustack/gpustack:latest`；**先快照 `s6-rc.d.dist/` 原始
  `gateway/pilot/controller` run 脚本（回滚原件，先于覆盖）**；再覆盖 s6 脚本；最后 `chmod`。
- `pack/hygress-s6/rootfs/etc/s6-overlay/s6-rc.d/`：
  - `gateway/run` → **Hygress 启动器**（源 base.sh/$GPUSTACK_GATEWAY_CONFIG/default-variables.sh；
    导出 `GPUSTACK_DATA_DIR`、kubeconfig、端口等；承接 access.log；exec hygress）；
  - `pilot/run` / `controller/run` → no-op 长 sleep（**向 `notification-fd:3` 写就绪字节**；
    status 保持 up，禁 exit 0 防重启风暴）；
  - `supercronic/run` → 去 `readinessCheck "Higress Pilot" 15010`，cron 路径 `/var/lib/istio/cron.txt`。

---

## 7. 真机验证（GPUStack v2.2.3，host TEST_HOST:33006）

### 7.1 基线部署（fix-17）
真实 GPUStack（quay.io/gpustack/gpustack:latest）+ 独立 worker（RTX4090）+ qwen2.5-0.5b-instruct + api key
"baseline"；环境限制（ghcr 阻塞 → 本地 CPU llama-cpp GGUF serve 镜像；`GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT="false"`）。
录制基线：真实对话响应 / usage 行 / 16 个真实 CRD → `gpustack-validation/fixtures-baseline/`。

### 7.2 换入与 7 项真机缺口（fix-18 定界 → 现场修复）
fix-18 曾误判"远端不可达"（试错主机）并留下 s6 boot 阻滞的**精确定界**；随后由主控在真实基线 host
直接完成换入，逐项修复真机暴露的 7 个实契约/运行时缺口：

| # | 真机缺口 | 根因 | 修复 |
|---|---|---|---|
| 1 | s6-rc `change top` 永不收敛，主进程不启动 | no-op pilot/controller 未在 `notification-fd:3` 发就绪 | no-op 脚本 `printf '\n' >&3` + `exec sleep infinity` |
| 2 | 远端构建失败 `--chmod requires BuildKit` | legacy builder | Dockerfile 改 COPY + RUN chmod；`.dist` 快照先于覆盖 |
| 3 | API 起来 ~15s 后 fail-fast：jwt_secret_key 找不到 | Rust 读 `GPUSTACK_DATA_DIR`，容器只有 `DATA_DIR` | `gateway/run` 导出 `GPUSTACK_DATA_DIR` |
| 4 | 连接 apiserver :18443 首握手 panic（rustls CryptoProvider） | reqwest `rustls` 未选中 provider | 固定 `ring` feature + `install_default()` in main |
| 5 | `registry_resolve_failed` + mirror 503 | GPUStack 的 `default` McpBridge **无 managed 标签**，被标签选择器滤掉 → registries 空 | `snapshot.rs` 对 McpBridge 用无标签选择器 LIST |
| 6 | AUTHED 模型 401 `auth_denied` | forward-auth 未转发客户端 `authorization`（wasm ext-auth 总转发） | allowlist 补 `authorization` |
| 7 | 慢容器启动触发 fail-fast | 30s/60s 有界窗口 | `HYGRESS_API_READY_TIMEOUT`/`HYGRESS_SNAPSHOT_TIMEOUT` 可配（launcher 300s） |

> 教训：这 7 项全部是**只有真机才能暴露**的隐性契约（s6 语义、GPUStack 标签习惯、wasm 插件
> 与配置声明不一致等）——单元/集成测试无法覆盖，验证计划的"真实环境证据链"设想得到兑现。

### 7.3 最终 DoD（全部 PASS）
- **DoD 1** e2e：`POST :80/v1/chat/completions` → 200 / "HELLO_HYGRESS_WORKS" / usage 34/7 / 0.47s；
- **DoD 2** CRD 换入后 16/16 与基线一致（规范化后逐字节），无新增/缺失，只读证实；
- **DoD 5-DB** usage `model_usage_details` 2→3，新行 34/7 与响应逐位一致；
- **DoD 6** 端口纪律（无 9876/15010/15012/8888/15051）、单 hygress 进程（无 envoy/pilot/controller）、
  supercronic 正常、`.dist` 回滚件齐、`/readyz`=200、server 稳定（R=0）。

---

## 8. 终态与产物索引

```
├── README.md                        # 导读（目的/架构/对比/部署）
├── docs/
│   ├── design.md                    # 设计 v1.5（现状/契约/架构/相位/部署/兼容性矩阵）
│   ├── dev-process.md              # 本文件（开发全过程）
│   └── research/
│       ├── plugin-contract-pin.md   # 字节级外部 wire 契约（权威）
│       └── gpustack-validation/     # 真机验证证据（REPORT/fixtures/scripts/logs）
├── crates/
│   ├── hygress-core/                # 纯领域（146 测试）
│   ├── hygress-adapter/             # CRD 控制面（45）
│   ├── hygress-egress/              # 出向客户端（39+8+4）
│   └── hygress-gateway/             # Pingora 数据面 + 管道 + main（115+11）
├── pack/
│   ├── Dockerfile.hygress           # Hygress 定制镜像（s6 手术）
│   └── hygress-s6/rootfs/etc/s6-overlay/s6-rc.d/  # gateway/pilot/controller/supercronic run
└── target/release/hygress           # 单二进制（hygress-gateway 复制产物）
```

**验证/质量复核命令**
```bash
cargo test --workspace --all-features   # 368 tests
cargo clippy --workspace --all-features -- -D warnings   # 0 warnings
cargo build --release -p hygress-gateway --features integrations
```

---

## 9. 经验与后续

**经验**
1. 字节级契约先行（pin 文档）——把所有外部边界的"应然"在编码前钉死；
2. TDD + 真实 IO：错误路径也要真实覆盖（fail-fast/FAIL_OPEN/丢行闸门）；
3. 双 oracle 门禁（Gate-1 契约、Gate-2 实现 vs 设计）显著提升正确性；
4. 真机验证不可省：7 项隐性契约只能在真实 GPUStack/s6 环境暴露（本阶段最大收益）。

**后续方向（未实施）**
- 更多真实 provider（非 OpenAI 类型）的 ai-proxy 端到端；
- 延迟/资源占用的量化基准（对比基线 Higress 进程数/内存/首 token 延迟）；
- 多租户隔离的专项验证与文档化；
- 回滚演练脚本化（`ROLLBACK.md` 已随 pack 提供）。
