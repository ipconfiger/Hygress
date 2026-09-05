---
status: in-progress
phase: 1
updated: 2026-09-05  (rev 2: 按 ora-2 门禁评审 B1-B4 修订)
---

# 控制面治理修复计划（C4 / C1 / C3）

## Goal
落地 `control-plane-equivalence.md` 建议 D 的三项治理修复：① **C4**：admin `GET /config` 内省当前生效快照（provider/注册表/TLS 私钥/特性 raw spec 脱敏）+ 配置拒绝/跳过两个计数器（对齐 `istioctl proxy-status` 的配置可见性）；② **C1**：三类配置漂移告警（受管 WasmPlugin 未知 defaultConfig 键 / 未知受管插件资源名 / higress-config 非超时键）+ pin 契约升级检查单（把静默忽略变成可发现的漂移）；③ **C3**：TLS 证书轮换闭环（内容指纹变化检测 → 重写 PEM + `error!` 告警 + 计数 + 文档化"需重启"；**已裁定 pingora 0.8 无热载**）。三项目均经 @oracle 高精度评审判定"可实施"后才开启实施；入选全部实现后以全套验证 + 盒子复测落档。

## Context & Decisions
| Decision | Rationale | Source |
|----------|-----------|--------|
| C4 = `GET /config`（token 门禁 fail-closed，见 B2 脱敏清单）+ `config_reject_total` / `config_object_skipped_total` 双计数 | 计数**落点 hygress-core `SharedConfigHandle`**（adapter 与 gateway 双方持有同一 Arc，规避跨 crate 方向依赖）；gateway `Metrics` 侧以 ~25 行 `Collector` 包装读原子值 → `/metrics` 自动带出 | `ref:ora-2` 评审 B1 |
| C1 = 三类漂移面（未知 defaultConfig 键 @ 解析函数白名单差集 / 未知受管插件资源名 @ 分发处 / higress-config 非超时键 @ configmap_to_timing）→ 每次 translate pass 聚合 warn 一次（fail-open，不拒绝） | 覆盖评审 B4 补全的三类静默点；指纹短路保证无日志风暴 | `ref:ora-2` 评审 B4 |
| C3 = 内容指纹变化 → 重写 PEM + `error!` + `tls_cert_change_detected_total`/`tls_cert_requires_restart_total` + README"轮换需重启容器"；**圆括号内删除热载分支** | 已实测 pingora 0.8.1：`add_tls(addr, cert_path, key_path)` 仅 listener 构建期可用（listeners/mod.rs:262、services/listening.rs:141/146）；listeners/ 全目录无 reload/CertResolver 动态证书 surface → **热载不可行** | `ref:ora-2` 评审 B3 + 已裁决 |
| /config 脱敏范围 = provider `api_tokens` + `TlsHost.key_pem` + `features[].config`（raw spec，含派生网关令牌 `X-GPUStack-Auth-Token`）；Registry 无凭据字段无需处理 | 防凭据泄露；admin token ≠ gateway token，raw spec 原样输出＝提权面 | `ref:ora-2` 评审 B2 |
| C1/C3/C4 均不改数据面热路径公共 API、不动 egress 契约；计数器为 `AtomicU64`（非数据面争用） | 治理/可观测类改动 | — |

## Phase 1: C4 —— admin 配置内省 + 拒绝/跳过计数 [IN PROGRESS]
- [ ] **1.1 `GET /config`（`crates/hygress-gateway/src/admin.rs`）**
  - token 门禁沿用 `authorized()`（**实为 :68-73**，fail-closed：无 token → 401）；`route()` 纯函数（:77）增 `("GET", "/config")` 分支；`AdminResp::json` 现成
  - `AdminState` 现无快照句柄 → `DataState::new`（同持 `shared` 与 `metrics`）给 `AdminState` 注入 `shared: SharedConfigHandle` 或 dump 闭包（仿既有 `reloader: Option<Arc<dyn Fn…>>` 模式，admin.rs:42）
  - 输出为当前 ArcSwap 快照结构化 JSON：路由规则概要（kind/name/path/priority）、注册表/出向代理概要、特性开关元数据、TLS 主机（仅指纹）、快照指纹 rv
  - **脱敏（B2）**：`provider_tokens[].api_tokens` → 仅 `len()` 或 `"***"`；`tls.hosts[].key_pem` → 仅 sha256 前 12 hex 指纹或 `"***"`；`features[].config`（raw spec，**含 `gpustack-llm-ext-auth` `defaultConfig.headers_to_add["X-GPUStack-Auth-Token"]` 派生网关令牌**）→ 整体省略或仅出 `plugin/phase/priority/defaultConfigDisable` 元数据，不得原样输出；`Registry`（registry.rs:27-40 无凭据字段）`domain` 可原样输出 ← CURRENT
- [ ] **1.2 双计数器（B1 接线方案）**
  - 落点 **hygress-core**：`SharedConfigHandle`（config.rs）增 `pub snapshot_reject_total: AtomicU64` + `pub snapshot_skipped_total: AtomicU64`（纯原子，core 不引入 prometheus 依赖）
  - increment 站点：结构性拒绝 = adapter `lib.rs` `sync_once`（现 :395-397，`store` 返回 `Err` 时 +1）；per-object skip = **先修现存静默点**——`SharedConfig::new/store`（config.rs:1049-1085）`Ok` 路径现**丢弃** `sanitize().issues`，最小改法选**返回值方案**：`new/store` 的 `Ok` 携带 `issues.len()`（core 无 tracing 依赖，不选 core 内 warn）→ adapter 统一计数 + warn
  - gateway 暴露：`Metrics` 增 ~25 行 `prometheus::core::Collector` 包装（读两个 AtomicU64），在 `DataState::new`（bootstrap.rs，`ds.shared` 与 `ds.metrics` 同点可得）注册进现有 Registry → `/metrics` 与 `/stats/prometheus` 自动带出（`hygress_config_reject_total` / `hygress_config_object_skipped_total`）
- [ ] 1.3 测试（add-only）：`/config` 鉴权（无 token → 401；有 token → 200 + 快照字段枚举）；**脱敏断言：dump 不含 provider/注册表 secret 明文、不含 `X-GPUStack-Auth-Token` 值、不含 `-----BEGIN` 私钥**；构造坏路由（test 已有 `rebuild_rejects_invalid_regex` 形态）→ reject+1、skip 对象 → skipped+1；`/metrics` 出现两行新计数

## Phase 2: C1 —— 三类配置漂移告警 [PENDING]
- [ ] **2.1 `translate.rs` 三处解析函数的未知 defaultConfig 键告警（B4 修正锚点至解析函数定义处，白名单差集）**
  - model-router `translate_model_router_config`（**translate.rs:556-570**）：已知键集 = `prefix/targetHeader/enableOnPathSuffix/aliasNameMapping/maxBodyBytes`——**注意 `modelKey`/`autoRouting*` 为 GPUStack 契约存在但 hygress 未消费字段**（config.rs:554 注释 + pin §2.3"reconciler 只热更 aliasNameMapping"，正是 C1 要抓的漂移）；未知键收集 = 已知键白名单差集（手写 `Value::get` 链，非 serde `deny_unknown_fields`）
  - model-mapper `wasmplugin_model_mapping`（**:475-531**）；ai-proxy `wasmplugin_ai_proxy`（**:588+**）
  - **2.1b 分发处告警**：`build_config_data` 分发处（translate.rs:962-989）：未知受管 WasmPlugin 资源名（`wasmplugin_to_feature` 照单全收养为 feature，数据面只行为消费 3 个 → GPUStack 新增第 9/10 插件时静默无效）+ `higress-config` ConfigMap 非超时键（`configmap_to_timing` 只读 3 键，:866-869）
  - warn 语义：每次 translate pass 一条聚合 warn（列键名），指纹短路保证仅真实变更触发；**保持 fail-open**（不 reject、不影响已知键消费）
- [ ] 2.2 `docs/research/plugin-contract-pin.md` 增补检查单条目：「GPUStack 升级后重跑 pin 对比（新字段/新插件 → C1 告警即刻可见 + 等价性复核）」
- [ ] 2.3 测试（add-only）：含未知 `defaultConfig` 键的 WasmPlugin fixture → 断言 warn 触发且不 reject、已知键行为不变；未知受管插件名 / higress-config 未知键 → 分发处告警触发

## Phase 3: C3 —— TLS 证书轮换闭环（已裁定无热载）[PENDING]
- [ ] **3.1 机制落点（B3 修正）**：bootstrap.rs `run()` 内（attach_data_plane 之后、run_forever 之前）`tokio::spawn` 独立任务：每 30-60s 读 `ds.shared.load().tls` 内容指纹（cert_pem+key_pem 的 sha256）→ 与上次写入指纹比对 → 变化则重写 PEM（复用 `write_default_tls_pem` 逻辑 :245-257）+ `error!` 告警；快照已由 WATCH 秒级刷新，bootstrap 侧轮询仅增 ≤一周期检测延迟（证书轮换为分钟级运维操作，可接受）；**不让 adapter 持有 PEM 路径/回调**（跨 crate 反向依赖）；启动后记录初始指纹
- [ ] **3.2 变化分支（无热载分支）**：重写 PEM + `tracing::error!` 响亮告警 + `Metrics` 计数 `tls_cert_change_detected_total` / `tls_cert_requires_restart_total` + README/运维文档注明"证书轮换需重启 server 容器"（运维文档附：pingora 0.8.1 单默认证书限制——bootstrap.rs:226-229 注释，多 `gpustack-tls-*` 主机 SNI 也只服务默认证书，为已知边界）
- [ ] 3.3 测试（add-only）：TlsConfig 内容指纹函数单测（内容变化 → 指纹变化 → 检测触发）；以"检测 + 告警 + 计数 + 文档化需重启"为验收（热载已裁定不可行）

## Phase 4: 端到端验证与落档 [PENDING]
- [ ] 4.1 `cargo build/test` 两 feature 模式 + 22 Pingora e2e + clippy 双模式 0 + alloc_guard 通过
- [ ] 4.2 部署到盒子（ship + swap）：`/config` 真机 200 + **响应体 grep 不到派生令牌与 `-----BEGIN` 私钥（脱敏 e2e）**；新计数器在 `/metrics` 出现；C1 告警日志；TLS 指纹分支日志
- [ ] 4.3 更新 `control-plane-equivalence.md`（C1/C3/C4 标记已实施）+ 本计划落档 + 提交

## Notes
- 2026-09-05（rev2）: **@oracle 门禁评审终裁 = REQUEST CHANGES 已全部纳入**：B1 计数器落 core + Collector 接线；B2 脱敏补 key_pem/features[].config；B3 热载不可行（listeners/mod.rs:262、listening.rs:141 构建期 `add_tls`；listeners/ 无 reload/CertResolver surface；原 configuration/mod.rs:199 注释实为 load_from_yaml，已弃用该引证）→ 落点=轮询指纹检测+告警+计数+文档化重启；B4 三类漂移面 + 解析函数定义处锚点（:556-570/:475-531/:588+/:962-989/:866-869） → `ref:ora-2 评审`
- 2026-09-05: `/config` 为敏感面：admin token ≠ gateway 令牌（`X-GPUStack-Auth-Token` 派生网关令牌经 ext-auth raw spec 存在），脱敏 + token 门禁与 `/stats/usage` 同级 → `ref:ora-2`
- 2026-09-05: 计数器为 `AtomicU64` 原子自增（非数据面争用）；改动治理/可观测类，不动数据面热路径与 egress 契约
- 2026-09-05 minor（随实施顺手，不阻塞）：①`ProviderToken.api_tokens`/`TlsHost.key_pem` 的 Debug derive 会经 `{:?}` 泄钥 → 手写脱敏 Debug 或 `#[serde(skip_serializing)]` 等价处理（独立小提交）；②watcher 流结束（理论不可达）静默退化 30s 兜底处加 `error!`；③修订后的本计划引证以 listeners/mod.rs:262、listening.rs:141 为准
- 2026-09-05: 本计划须**先经 @oracle 高精度评审判定"可实施"** 才开启实施；判定不通过则按阻塞清单修订后循环再审，直至放行（当前轮次：第 1 轮 REQUEST CHANGES → rev2 修订完成）
