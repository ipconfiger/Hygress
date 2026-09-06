# Hygress 运维手册（监控 / 告警 / runbook）

> 对象：以 Hygress（Rust/Pingora）原位替换 GPUStack 内嵌 Higress 的部署。本文只讲
> **运行期观测与处置**；配置语义见 `README.md` §4 环境变量表，TLS 轮换细纲见
> `pack/hygress-s6/README.md`，镜像构建/换入/回滚见 `README.md` §4 与
> `pack/hygress-s6/ROLLBACK.md`。
>
> 术语速览：**数据面** = :80/:443 转发；**控制面** = 只读 CRD 消费（拓扑 A：GPUStack
> 内嵌 apiserver，~1s 轮询为主；拓扑 B：external 集群，WATCH 事件 + 轮询兜底）；
> **快照 store** = 成功解析出**新内容**的控制面快照；**reconcile pass** = 任一轮
> 收敛尝试（含无变化指纹轮）。`hygress-*` 指标与下方 PromQL 全部以
> `hygress_` 为前缀。

## 1. 健康语义

| 信号 | 位置/门禁 | 语义 | 能/不能说明什么 |
|---|---|---|---|
| `GET /healthz` | 127.0.0.1:8081，开放 | 静态 `200 ok` | **仅进程 + admin 监听存活**。不反映控制面新鲜度、数据面、策略状态 |
| `GET /metrics` | 127.0.0.1:8081，开放 | Prometheus 文本 | 进程起来即可抓（数据面 bind-ready 之前也能抓，此时 `control_last_store=0`） |
| `POST /reload` | 127.0.0.1:8081，token | 策略热重载 | 缺/错 `HYGRESS_ADMIN_TOKEN` ⇒ **401 fail-closed**；失败 ⇒ **500 `reload_failed`，LKG 保留** |
| `GET /config` | 127.0.0.1:8081，token | **脱敏**快照摘要 | 核对当前生效路由/策略快照（新路由迟迟不生效时的第一排查点） |
| `GET /stats/usage` | 127.0.0.1:8081，token | 指标文本（同 /metrics 编码） | 给只读审计/抓取用的门禁出口 |
| `GET /readyz` | 数据面 :80，开放 | **GPUStack 自身应用镜像通道** | GPUStack 应用健康，**与 Hygress 快照无关** |
| `:15020/stats/prometheus`、`/stats` | 0.0.0.0:15020，开放 | 指标文本 / JSON 浅视图 | Istio-shallow 兼容出口（绑 0.0.0.0、无鉴权，勿暴露公网） |

数据面绑定门（bind-ready）：

- :80/:443 只在**首个控制面快照成功 store** 后绑定（`Controller::ready()` 门）。
- 首快照有界 fail-fast：超 `HYGRESS_SNAPSHOT_TIMEOUT`（二进制默认 60s；s6 launcher
  默认放宽 300s）仍未拿到首个可用快照 ⇒ 非零退出 → s6 重启。
- 另有 `GPUSTACK_API_PORT` 就绪探测 `HYGRESS_API_READY_TIMEOUT`（二进制默认 30s /
  launcher 300s）在控制面初始化前执行。
- 推论：**bind-ready 前抓 `:8081/metrics` 看到 `hygress_control_last_store_timestamp_seconds=0`
  属正常启动态**；持续不前进才是问题（见 §4）。

控制面“心跳 vs 内容”区分（排查“快照陈旧”的关键）：

| 指标 | 何时前进 | 停滞含义 |
|---|---|---|
| `hygress_control_last_sync_timestamp_seconds` | **每次成功 reconcile pass**（含无变化 no-op 指纹轮），拓扑 A ≈ 每 ~1s tick | 控制器卡死 / 崩溃 / LIST 持续失败（退避中）——真正的“控制面死了”信号 |
| `hygress_control_last_store_timestamp_seconds` | 仅**内容变化**的快照 store | 集群静默健康（无变更）时本就该停滞——**不是故障** |
| `hygress_control_reconcile_error_total` | 按 **outage 片段**（非每 tick）计 LIST/拒绝失败 | 与上面互补：pass 失败但进程活着时，此计数 + warn-once 日志是主证据 |

健康判断口诀：`/healthz` 绿 + `last_sync` 新鲜 + `last_store` 随时间前进 = 端到端健康；
`/readyz` 绿 **不能**替代其中任何一项。

## 2. 指标目录

抓取：`:8081/metrics`（loopback，同机/容器内）与 `:15020/stats/prometheus`（0.0.0.0）
输出**同一注册表**。示例 scrape：

```yaml
scrape_configs:
  - job_name: hygress
    metrics_path: /stats/prometheus
    static_configs:
      - targets: ["127.0.0.1:15020"]   # 仅内网可达处暴露；bridge 部署则映射 15020 后抓取
        labels: { instance: "gpustack-server" }
```

| 指标（`hygress_` 前缀省略） | 关键 label | 语义 | 典型告警/排查用法 |
|---|---|---|---|
| `requests_total` | `status`, `kind` | 请求按状态计数。`kind=model_route`/`mirror` = 完整上游分发；`short_circuit` = 网关自身终结的 4xx/5xx | 按 `kind` 拆“网关自身 4xx/5xx”与真实上游结果；`short_circuit` 突增查限流/配额/护栏/鉴权/路由 miss |
| `request_duration_seconds` | `kind` | 端到端请求延迟直方图 | 90/99 分位基线；kind 细分同上 |
| `ttft_seconds` | `kind` | 首块（TTFT）直方图 | 流式首 token 体验；突增查上游/provider 首跳 |
| `tokens_total` | `direction=prompt\|completion\|cached` | 观测到的 token 量 | 用量趋势；与 `usage_pushed_total` 对照（cached 计入 prompt 侧命中） |
| `active_requests` | — | 在飞请求 gauge | 容量/背压；配合 duration 算并发 |
| `retries_total` | — | 候选间故障转移重试 | 单后端抖动时上升；持续增长查该上游 |
| `upstream_errors_total` | — | 上游尝试失败数 | 与重试/fallback 对照定位坏上游 |
| `fallback_total` | — | 执行的 fallback 重定向数 | fallback 链被频繁使用 = 主用模型异常 |
| `fallback_exhausted_total` | — | 预算耗尽 / 链到末端仍无成功跳 | **P1 告警候选**：用户请求最终失败 |
| `auth_decisions_total` | `result` | `allowed` / `denied` / `auth_service_unavailable_denied`（closed）/ `auth_service_unavailable_allowed`（open） | 后者正则告警 = `/token-auth` 不可达且 fail-closed ⇒ 全网 403 |
| `rate_limit_denied_total` | `dimension=ip\|consumer` | 限流 429 拒绝（令牌桶） | 按维度拆毛刺来源；配合 `Retry-After` |
| `quota_denied_total` | — | token 配额 hard 超限 429 | 某 consumer×model 触顶 |
| `quota_soft_exceed_total` | — | soft 超限（放行 + 警告位） | 配额估算是否过紧的前兆 |
| `guardrail_blocked_total` | `side=in\|out` | 护栏拦截（入向 403；出向跨块断流） | 提示注入/敏感词命中率；`side=out` 断流同时产生 `completed=false` usage |
| `policy_applied_total` | `applied=true\|false` | 路由策略覆盖应用结果 | `override_route` 目标运行时 miss ⇒ 回退原路由 + `applied=false` |
| `usage_pushed_total` | `completed=true\|false` | 交给 usage sink 的行数。`true` = 上游给了规范 usage；`false` = 依赖 GPUStack 服务端字节/分块估算（CPU `serve.py` 等后端属正常） | “缺 usage 行”时的第一对照指标（见 §4） |
| `usage_push_dropped_total` | — | 到不了 sink 的行（队列满 / flusher 消失 / 最终推送失败） | **>0 即告警**：sink 侧（GPUStack API）不可达或过载 |
| `control_watch_error_total` | `kind`, `class=permanent\|transient` | watcher 错误。`permanent`（如内嵌 apiserver 不支持 watch → 降级轮询）；`transient`（可恢复） | 拓扑 A 出现 `permanent` 是**预期降级**非故障；拓扑 B `transient` 增长 = watch 通道不稳（查网络/负载） |
| `control_snapshot_store_total` | — | 成功 store（新内容）的快照数 | store 频率过低看是否“配置不生效”（配错 label 选择器/命名空间） |
| `control_last_store_timestamp_seconds` | — | 最近一次内容 store 时间；首 store 前为 0 | 见 §1 心跳/内容区分 |
| `control_last_sync_timestamp_seconds` | — | 最近一次成功 reconcile pass（含 no-op）；首 pass 前为 0 | **Liveness 主告警源**：停滞 = 控制器死了 |
| `control_reconcile_error_total` | `class=list\|rejected` | 失败**片段**计数（warn-once latch，非每 tick）：`list` = LIST/传输失败；`rejected` = 结构性拒绝（LKG 保留） | `increase(...[5m])>0` 告警；`rejected` 常见于 GPUStack 写出异常对象 |
| `config_reject_total` | — | 整个快照被结构性拒绝（keep-LKG） | 与 `reconcile_error{class=rejected}` 同源视角 |
| `config_object_skipped_total` | — | 逐对象校验被跳过的对象数 | 部分对象不生效时先看这里（跳过原因在日志） |
| `policy_reload_total` | `result=success\|failure` | 策略重载尝试（admin `/reload` + 30s mtime 轮询；无变化 tick 不计） | `failure` 增长 = 策略文件坏/路径丢 ⇒ 现网跑的是 **LKG** |
| `build_info` | `version` | 静态构建溯源 = 1 | 核对运行的二进制版本（多版本混跑排查） |
| `tls_cert_change_detected_total` | — | 运行时检出 TLS 内容指纹变化（~60s 巡检） | 证书轮换后应 +1（同时有 ERROR 日志） |
| `tls_cert_requires_restart_total` | — | 已检出、**需重启容器**才生效的轮换事件 | **>0 即告警 + 重启**（pingora 0.8 无热载） |

## 3. 告警规则（starter PromQL）

> 均以 `hygress_` 完整名为准；`time()` 比对适合 15s scrape 间隔。阈值按部署规模调。

```promql
# —— P1：控制面心跳断（拓扑 A 为主。B 有 WATCH 事件驱动，pass 更密，误报低）——
time() - hygress_control_last_sync_timestamp_seconds > 90

# —— P1：控制面 reconcile 失败片段（LIST 故障或快照被拒，LKG 保留 = 静默陈旧）——
increase(hygress_control_reconcile_error_total[5m]) > 0

# —— P1：usage 行到不了 sink（丢行 = GPUStack 用量账缺失）——
increase(hygress_usage_push_dropped_total[5m]) > 0

# —— P2：策略热重载失败（现网跑的是上次有效 / 从未成功 = 内置全放行默认）——
increase(hygress_policy_reload_total{result="failure"}[5m]) > 0

# —— P2：fallback 链耗尽（模型请求最终无可用上游）——
increase(hygress_fallback_exhausted_total[5m]) > 0

# —— P1：TLS 轮换已检出但未重启（现网证书与 Secret 不一致）——
hygress_tls_cert_requires_restart_total > 0

# —— P1：ext-auth 不可用 + fail-closed ⇒ 全网 403（见 §4 排查）——
increase(hygress_auth_decisions_total{result=~"auth_service_unavailable.*"}[5m]) > 0

# —— P2 附加：整快照拒绝（结构性问题，可能伴随 403/路由缺失）——
increase(hygress_config_reject_total[5m]) > 0
# —— P2 附加：上游故障侧信号——
increase(hygress_upstream_errors_total[5m]) > 5
```

告警收敛提示：`control_last_sync` 断 90s + `control_reconcile_error_total{class="list"}` 上升
→ 判控制器故障；只有 `last_sync` 停滞而无 error 增长 → 查进程是否 panic 循环（s6 重启、
`build_info` 时间戳、panic 日志），或控制面线程被阻塞。

## 4. 事故排查表（symptom → check → action）

| 症状 | 检查 | 处置 |
|---|---|---|
| 建路由/模型后**迟迟不生效**（预期拓扑 A ≈ **~2s**：1s poll + 收敛；拓扑 B 走 WATCH 事件 + 轮询兜底） | `control_last_store` 是否前进；`GET /config`（token）摘要是否含新路由；`config_object_skipped_total` | 快照已 store 但配置缺 → 查对象 label/命名空间选择器与 `skipped` 日志；快照未 store → 见下行 |
| 快照陈旧（`:80/readyz` 200 但流量/配置是旧的） | `:80/readyz` 是 **GPUStack 镜像通道**，与 Hygress 无关；看 `last_sync`（活否）与 `last_store`（内容否）、`reconcile_error_total`、`config_reject_total` | `reject/list` 增长 → LKG 保留是**预期保护**，根因在 LIST 失败或对象结构；无增长 → 等下一 tick（≤~1s），仍陈旧则看日志 warn-once 片段与 watcher 状态 |
| WATCH 热循环（曾现；已修复 = **60s permanent / 5s transient 退避 + 30s/kind 日志限速**，指标佐证） | `control_watch_error_total{kind,class}` 增长形态；日志同 kind 每 ≤30s 一条 | `permanent`：拓扑 A（内嵌 apiserver 无 watch）属**预期降级**，收敛靠轮询，无需处置；拓扑 B 反复 `transient` → 查 apiserver 网络/负载/RBAC |
| usage 行缺失（`model_usage_details` 无新行 / 比预期少） | `usage_pushed_total{completed}` 拆分：`completed=false` 属**服务端估算或 CPU serve.py 类后端**（正常路径）；`completed=true` 该有而没有 → 上游没回 usage；`usage_push_dropped_total` >0 → sink 侧问题 | dropped 增长 → 查 GPUStack API（`/v2/usage/gateway-metrics` 目标）可达性/限流；两侧都平 → 查 1024 行队列与进程生命周期（SIGKILL/panic 丢队列内未推，无计数——见 §7） |
| 突发 429 `rate_limit_error` | `rate_limit_denied_total{dimension}` 拆分 ip/consumer；对照 policy 的 `limits`（令牌桶 rps/burst） | 合法毛刺：调 policy 或等 `Retry-After`；`consumer` 维度 429 常见于共享 token 挤爆桶 |
| 突发 403 | `guardrail_blocked_total{side}`（护栏）；`auth_decisions_total{result}`：`denied`（凭据被 GPUStack 拒）vs `auth_service_unavailable_denied`（**closed** 模式下 `/token-auth` 不可达/5xx ⇒ 全网 403） | unavailable 形态 → 查 GPUStack `/token-auth` 与 forward-auth 超时 `HIGRESS_EXT_AUTH_TIMEOUT_MS`（默认 30000ms；**注意拼写是 `HIGRESS_` 前缀**）；恢复 ext-auth 即自愈；如需容忍短暂不可用可评估 `HYGRESS_EXT_AUTH_FAIL_MODE=open`（**不要**默认环境开） |
| `POST /reload` 返回 500 `reload_failed` | `policy_reload_total{result="failure"}`；响应体明文 | **LKG 保留 = 无副作用**（从未成功加载则为内置全放行默认）。改对 policy.yaml（键名与 `hygress-core::policy` serde 一致）后重试；mtime 轮询同路径自动恢复 |
| `https://host:443` connection refused | 默认 GPUStack 安装**无 `gpustack-tls-*` Secret ⇒ 数据面无 :443 监听**（真实内嵌 Higress 会呈自签页——已知差异） | 需要 TLS：给 GPUStack 配 `--ssl-keyfile/--ssl-certfile` → Secret 出现 → **重启 Hygress 容器**后 :443 按新证书绑定；证书轮换同理需重启（§5） |
| `/healthz` 200 但控制面不动 | 见 §1：healthz 只证进程；看 `last_sync` / `reconcile_error` / panic 日志 | 进程 panic → s6 自动重启（日志含 panic 定位 + exit(1)）；反复 panic 循环 → 带上日志与 `build_info{version}` 复盘 |
| 端口纪律回归检查 | `ss -ltn` 不应见 9876/15010/15012/8888/15051 | 见到即配置回归，对照 `pack/hygress-s6/README.md` 端口清单 |

## 5. 升级 / 重启矩阵

| 变更类型 | 生效方式 | 操作 | 说明 |
|---|---|---|---|
| env / 二进制 / 镜像变更 | **重启容器** | 重建 `IMAGE_TAG` 镜像 → `docker compose -f compose-hygress.yaml up -d` | env 在启动时解析一次；启动摘要日志回显生效值（§6）核对 |
| 策略文件 `/etc/hygress/policy.yaml` | **热**：mtime ≤30s 轮询，或 `POST /reload` 即时（需 token） | 改文件（或替换挂载卷内文件） | 无重启；坏文件 → LKG 保留（§4 的 500 行） |
| CRD 变更（建模型/路由/Secret 等） | **热**：拓扑 A ~1s poll（route→live ≈2s）；拓扑 B WATCH 事件 + poll 兜底 | GPUStack 侧正常操作即可 | 无重启；只读控制面零写入 |
| TLS 证书轮换（`gpustack-tls-*`） | **检测热、生效需重启** | ~60s 巡检检出（`tls_cert_change_detected_total` + ERROR 日志）→ 重启容器 | pingora 0.8 仅在绑定时读 PEM；runbook：`pack/hygress-s6/README.md` |
| GPUStack 大版本升级 / 回滚 | 停机窗口重启 | 保留 `DATA_DIR_HOST_PATH` 数据卷；回滚 = `IMAGE_TAG` 切回旧标签或官方镜像 `GPU_STACK_IMAGE` | worker 与数据卷不动（基线拓扑）；原内嵌 Higress 脚本在镜像 `/etc/s6-overlay/s6-rc.d.dist/` |

## 6. 日志

- **级别与通道**：tracing 级别由 `RUST_LOG` 控制，默认 `info`（`EnvFilter` 兜底）。日志走
  stdout/stderr。
- **落盘**：s6 launcher（`pack/hygress-s6/rootfs/etc/s6-overlay/s6-rc.d/gateway/run`）把
  Hygress 输出 **append 到 `${GPUSTACK_DATA_DIR}/log/hygress.log`**（数据卷内，宿主机
  路径 `DATA_DIR_HOST_PATH/log/hygress.log`，便于诊断与采集）；原 Higress `access.log`
  槽位仅 `touch` 保持**空文件**兼容 GPUStack logrotate（Hygress 不写逐请求日志）。
  容器内 `docker exec gpustack-server tail -f /var/lib/gpustack/log/hygress.log` 或宿主
  机直接 tail 数据卷该文件均可。轮转由部署侧日志采集 / 外层 logrotate 负责。
- **启动摘要**：bootstrap 一条 `info` 打印生效配置（可 grep `hygress-gateway bootstrap`）：
  `version`、`contract_pin`、`http_port`、`tls_port`、`admin`、`admin_token_set`（bool，
  永远不是明文 token）、`stats_port`、`quota_k`、`topology_b`、`policy_path`、
  `ext_auth_fail_mode`、`ext_auth_timeout_ms`、`poll_interval_ms`。升级后先对这条日志再谈
  其他——env 拼写/注入问题在这里现形。
- **限速与退避语义**（读日志时避免误判“刷屏=风暴”）：
  - watcher 错误**退避门控**：permanent 60s / transient 5s 重试；**日志按 kind 限速 30s**
    一条，指标才是全集。
  - 快照 LIST/reconcile 失败 **warn-once per outage 片段**（latch 到下一次成功 pass 复位）：
    一段故障只有一条 warn，不要用行数估故障时长，用 `reconcile_error_total` 的片段计数。
  - 逐请求出站失败：egress 层 **warn 每次尝试**（区别于控制面限速）。
  - TLS 内容变更：~60s 巡检 ERROR “TLS certificate content changed … container restart is
    REQUIRED”，伴随两个计数器。
  - panic：ERROR 带 `location` + `payload`，随后 `exit(1)` → s6 重启进程；**不是**静默
    死亡，/healthz 与指标都会因进程重启而重置。
- **ext-auth 超时**：forward-auth 整体超时默认 30s（`HIGRESS_EXT_AUTH_TIMEOUT_MS`），
  启动摘要 `ext_auth_timeout_ms` 回显；注意前缀拼写（§4）。

## 7. 关闭行为（优雅停止与有界丢失）

- **优雅停止**（SIGTERM，如 `docker compose stop` / s6 longrun 停止）：pingora `run()`
  进入停止流程，usage flusher 在 stop 期间**排空队列**（channel 关闭后逐行走常规有界重试，
  受 per-POST 超时与最大尝试次数约束；后端不可达时由该预算兜底，不无限挂起）。
- **有界丢失（文档化局限）**：usage 队列是 **1024 行内存有界队列**；进程被
  **SIGKILL / panic（exit(1)）** 打断时，队列内未推行的行**直接丢失且无计数**
  （`usage_push_dropped_total` 不覆盖该路径）。因此：
  - 优先用优雅停止（`docker compose stop`、s6 正常 stop）做维护；
  - 强杀后如需对账，以 GPUStack 侧 `model_usage_details` 与 `usage_pushed_total`
    （已交给 sink 的行）之差估算损失窗口，不要依赖 Hygress 侧计数器补账。

## 8. 快速开始指针

- 环境变量全表与默认值：`README.md` §4.3（尤其 `HYGRESS_ADMIN_TOKEN` 生产注入、
  `HYGRESS_TOPOLOGY_B` 仅 external 置 true、`HIGRESS_EXT_AUTH_TIMEOUT_MS` 前缀拼写）。
- 一键部署模板：`pack/compose-hygress.template.yaml`（`cp` → 填 `${...}` →
  `docker compose up -d`；回滚 = 换 `IMAGE_TAG`；默认无 `--ssl-*` ⇒ 无 :443）。
- TLS：绑定条件 / SNI 约束 / 证书轮换 runbook → `pack/hygress-s6/README.md`。
- 回滚：`pack/hygress-s6/ROLLBACK.md`（`s6-rc.d.dist/` 原脚本 + supercronic 恢复）。
