# Hygress 审计修复验收清单（清单 A：逐修复项核销）

> 依据：docs/research/audit-repair-plan.md（方案 + §8 自取证 + §9 APPROVE）+ docs/research/audit-oracle-review.md（ora-2 第二方终审）。
> 状态约定：⏳ 待实施 / ✅ 已实施已测 / ⏸ 延期或降级（附理由）。由各批次实施报告 + 主控门禁（cargo test/clippy）填写。

## B1 — 正确性/安全（R-1..R-4 + parse_consumer）

| 项 | 验收标准 | 状态 |
|---|---|---|
| R-1 重试语义 | 单测：non_idempotent gate（不含该条件时 POST 4xx 不重试；含时 502/503 重试）；tries 封顶（tries=0 不切候选）；timed_out→Timeout 触发；tries 解析≤32 饱和。pipe e2e：单候选 503→fallback 不变 | ✅ |
| R-2 adapter 收敛 | 指纹仅在 store Ok 后推进；定时兜底无条件 sync_once；LIST 失败置 dirty；首快照 ready() 语义不变 | ✅ |
| R-3 quota/记账 | release 显式 now_ms（无 SystemTime）；gc_stale 文档与线上清理（evict_idle）一致；usage input+output saturating（u64::MAX 不 panic） | ✅ |
| R-4 C4 计数+/config | store Ok(dropped) + reject/skipped 原子累计；/metrics 与 /stats/prometheus 出 `hygress_config_reject_total`/`hygress_config_object_skipped_total`；admin GET /config token 门禁、快照摘要、**无明文密钥/-----BEGIN/派生令牌** | ✅ |
| B1-附加 parse_consumer | `gpustack-<uid>` → (user_id, None)；`ak.gpustack-<uid>` 不变；`none`/空不变；单测两形态 | ✅ |
| B1 门禁 | core/adapter/egress/gateway(integrations) 测试全绿；clippy 双模式 0 | ✅ |

## B2 — 性能/死面（R-5..R-9）

| 项 | 验收标准 | 状态 |
|---|---|---|
| R-5 prepare② 恒等短路 | 恒等映射无 body splice/二扫（alloc_guard 预算 + 奇偶测试） | ✅ |
| R-6 capture_groups | 复用 RouteTable 已编译正则；仅 rewrite_target 存在时捕获 | ✅ |
| R-7 SWRR 单目的地直通 | 单目的地不建状态不走锁；多目的地序列不变 | ✅ |
| R-8 小项 | evict 1s→30s；usage POST 整体超时；guardrail 缓存清理；admin CL | ✅ |
| R-9 死面 | features raw spec 移除（无 apiTokens 双份）；timing 诚实降级+WARN（§9）；SniStore 最小接线+注释修正+PEM 0600/清理（§9）；listenerPort 空语句删除；find_subseq 单义化注释 | ✅ |
| B2 门禁 | 同 B1 + alloc_guard 通过 | ✅ |

## B3 — 治理/对齐（R-10..R-13）

| 项 | 验收标准 | 状态 |
|---|---|---|
| R-10 C1 漂移告警 | 三解析函数 + 分发处未知键/名聚合 warn（fail-open、指纹防风暴）；pin 增检查单条目 | ✅ |
| R-11 C3 TLS 闭环 | 指纹检测任务+告警+计数+README 需重启；SniStore 若接线同步 store_config | ✅ |
| R-12 ext-auth fail | 默认 closed + `HYGRESS_EXT_AUTH_FAIL_MODE` env（closed/open）；egress 契约不变；pipe/auth 映射+metric 区分；docs（design/pin/README/forward_auth）改述 | ✅ |
| R-13 对齐 | README env 表/launcher export 一致；测试数更新；MSRV≥1.89；未用依赖删除；日志重定向文档化 | ✅ |
| B3 门禁 | 全仓测试+clippy 双模式 | ✅ |

## B4 — 全量门禁 [✅ **587 tests** / clippy all-targets 双模式 0 / alloc_guard 6/6]

| 项 | 状态 |
|---|---|
| `cargo test --workspace --all-features`（587：adapter 50 / core 215 / egress 52+8+10+4 / gateway 206 / integration 35 / doc 1） | ✅ |
| `cargo clippy --workspace --all-features -- -D warnings` / 默认 | ✅ |
| `cargo test -p hygress-gateway --test alloc_guard -- --test-threads=1`（6/6） | ✅ |
| `cargo test -p hygress-gateway --test integration`（35 项 e2e，含 AM-2 SSE 注入 2 项 + 判别缺口 11 项） | ✅ |

## B7 — ora-2 终审 MAJOR/MINOR 修复（audit-oracle-review.md §2/§3，2026-09-05 实施）

| 项 | 验收标准 | 状态 |
|---|---|---|
| AM-2 SSE include_usage | `build_outbound` 单点注入（is_model_route + json + completions 路径 + 顶层 stream=true + 无显式 stream_options）；五条件全在分配前；无 DOM/H4；e2e：不带→上游收到注入且 usage completed=true 精确、带→不双注入；pin §2.8 落差闭合 | ✅ |
| AM-1 IngressClass 门控 | Controller 增 `seed_ingress_class`，run() 内 `if` 门控；预种子方法删除（grep 佐证唯一写点被门控）；注释/README 口径统一；拓扑 A 零 apiserver 写 | ✅ |
| AM-4 fallback 复检 | sanitize 对 accepted 集复检；悬空引用 Main 被移出并报告 issue；判别测试（坏 Fallback+引用 Main→拒；健康对→双收） | ✅ |
| AM-3 body 读 Err | `read_body` 返回区分 Ok(None)/Ok(Some)/TooLarge/Read；读错→400 short_circuit 且**不派发上游**（e2e：半关闭断流→上游 0 次、无 usage 行）；BodyReadFailure 分类不混淆 | ✅ |
| AM-5 指标统一出口 | 8 处短路点统一 `record_short_circuit(status,elapsed)`（kind="short_circuit"）；404/503/401/403/429/413 全覆盖；专用计数器保留；help/kind 词典更新；4 类短路 e2e 计数断言 | ✅ |
| MINOR 行为 7 项 | usage_sink 4xx 不重试（401/404 一次即 drop；503/429 仍 3 次）；provider URL 改写失败 warn；admin bearer 恒定时间比较；sanitize percent 0..=100 + u64 checked 累加（双 u32::MAX 不 panic）；第 2+ Mirror 报告并剔除；usage Unknown 1 MiB 上限 + SSE 单行上限 + parse 预算（消 O(n²)）；forward_auth 非 UTF-8 write-back warn | ✅ |
| 判别测试缺口 | integration 22→35：failover 换候选/tries=0 禁用/timeout 触发重试/500·429 重试集外不换候选但 fallback/max_redirects=10 预算有界/断流不派发/短路计数（翻转断言红验证） | ✅ |
| 文档漂移批量 | 10 文件：1s→30s 口径、design v1.5、FAIL_OPEN→fail-closed 两层语义、pin 补 authorization/版本 1.1.1/§7 升级检查单、env 表、15020、34/7 vs 34/5 按跑次标注、日志落点、ai-proxy 措辞 | ✅ |
| B7 门禁 | 全量 587 tests 全绿 / clippy 双模式 0 / alloc_guard 6/6 / rustfmt 无净新增 | ✅ |

## B5 — 真机矩阵 [✅ 2026-09-05 真机 GPUStack v2.2.3：ready/进程/端口/metrics 新家族//config 401/chat 200+usage 34|5 落行/限流 200→429 rate_limit_error/护栏 403 guardrail_blocked/复位 200]
> 执行环境：125.67.215.17（linux 构建产物经 cargo-zigbuild 交叉编译）。

| 项 | 状态 |
|---|---|
| s6 启动/就绪/进程/端口纪律 | ✅（b5.log） |
| DoD1 e2e chat 200 + usage 34/5 落行 | ✅（b5.log） |
| metrics 新家族（config reject/skipped、tls cert×2）在线 | ✅（b5.log） |
| admin /config 401 fail-closed | ✅（b5.log） |
| 限流 429 `rate_limit_error`+Retry-After / 护栏 403 `guardrail_blocked` / 复位 | ✅（b6/b8.log） |
| **B7 新行为真机回归**（2026-09-05 b9，镜像 fa37e12a，远端 `/root/hygress-b3/b9.log`+`b9-verdict.txt`） | ✅ |
| ↳ AM-1 拓扑 A 零写 | ✅ 容器启动 12:42:21 后 hygress.log **零** IngressClass seed 行（最后 seed 11:20:09 属旧启动） |
| ↳ AM-2 SSE include_usage | ✅ 真机 3/3 请求通过（显式 true/false/裸 stream）：不覆盖显式选项、无双注入；usage 行 270/271/272 全 completed=t、28/4 逐位一致。SSE-chunk 判别在该机 **N/A**（CPU llama-box 后端非流式），注入证据由集成层 35 e2e 提供 |
| ↳ AM-3 断流不派发 | ✅ 半关闭 → HTTP 400 + Connection: close；metrics `short_circuit status=400` 0→1；无新 usage 行（未派发） |
| ↳ AM-5 短路计数 | ✅ `requests_total{kind="short_circuit"}` 401×1/403×1/429×2（+400×1）；auth_decisions denied=1；guardrail in=1；rate_limit consumer=2 |
| ↳ 基线回归 | ✅ readyz=200@2s、hygress×1/envoy·pilot×0、端口 80/30080/8081/15020/18443、chat 200 `HYGRESS_B7_OK` 34/5、限流 200/429/429、护栏 403、复位 200；回滚点 `gpustack:hygress-b5`(e6eb3458)/`hygress-pre`(3b6beabc) |

## B7b — 遗留观察项处理（2026-09-05）
| 项 | 结论 | 状态 |
|---|---|---|
| 观察项 1：AM-2 SSE-chunk 真机判别 | 根因：该机后端为 GPUStack 测试用**手写最小 OpenAI 兼容服务器**（`serve.py`，llama-cpp-python CPU），`_chat` 忽略 `stream` 字段、恒非流式单块返回——非流式是**测试替身特性**，与 Hygress 无关；真实 llama-box/vLLM 才流式。SSE-chunk 判别证据由集成层 35 e2e 承担（假上游断言收到注入体）；如需真机强判别须流式后端（llama-box/vLLM）环境 | ✅（文档固化，无需改代码或 serve.py） |
| 观察项 2：embedded apiserver WATCH 刷屏 | 根因：embedded apiserver LIST 无 resourceVersion → 6 类 watcher 均 `NoResourceVersion`，kube-runtime 无自退避 → 热循环（实测 ~2000 行/s、日志 17.6GB）。修复：`spawn_watcher` Err 分支**分类退避**（永久错误 60s / 瞬时 5s）+ **30s 日志限速**（adapter lib.rs）；收敛仍由 30s 安全网 tick 保证（R-2，每唤醒无条件 sync_once + 指纹短路） | ✅（587 tests / clippy 双模式 0；待真机验证日志降噪，见 equivalence §A3 注） |
> 凭据按需提供；若不可达 → ⏸ 未执行 + 交付可执行套件。

## B9.5 — ora-3 终审修复批（audit-oracle-review-ora3.md §3/§4/§6，2026 实施）

> ora-3 五维加权 ≈8.0/10 CONDITIONAL-APPROVE，唯一 MAJOR=ORA3-MAJ-1（控制面健康黑盒）。本批收口
> §6 条件清单第 1-4 组 + 部分第 5/7 组；结构/性能批（ORA3-M10..M15）与 AM-6 按约定并入扩容轮。

| id | 修复 | 状态 |
|---|---|---|
| ORA3-MAJ-1 | 控制面可观测：metrics.rs 新增 `hygress_control_watch_error_total{kind,class}`/`snapshot_store_total`/`last_store_timestamp_seconds`；adapter 依赖无关 `ControllerHooks`（on_watch_error/on_snapshot_store，Fn 回调）；bootstrap 接线 + `install_panic_hook()`（记录后 exit(1)，s6 重启）+ 启动收敛模式日志 | ✅ 单测 3（含 hook 接线）+ 指标渲染测试 |
| ORA3-M1 | env 解析失败逐键 `warn!`（config.rs `warn_unparsable`/`parse_duration_env`/`parse_bool_env`；畸形时长回落该键文档默认而非旧 1s）；bootstrap 启动脱敏配置摘要（token 仅布尔） | ✅ 单测 4 |
| ORA3-M2 | 策略文件缺失 boot `warn!` 一次；reload 遇缺失=失败（有 last-known-good 则保留并返回 false，永不静默换全放行）；admin /reload 500 文案诚实（两情形） | ✅ 单测 3（loader 2 + admin e2e 1） |
| ORA3-M3 | fallback 链无成功跳终止（预算耗尽/链尽）→ `record_fallback_exhausted` + warn（pipe.rs，redirect_count>0 守卫，不双计） | ✅ 依赖既有 max_ten_guard 单测 |
| ORA3-M4 | usage 行丢弃可计数：egress `GpustackSink::new` 增 `on_drop` 尾参（三丢弃点+4xx 点触发）；bootstrap 接 `hygress_usage_push_dropped_total`；flusher 通道关闭即排空（recv 语义 + 文档）；shutdown drain 由 pingora run() 优雅停止期主 runtime 存活自然保证（run_forever `-> !`，无死代码） | ✅ egress 单测 4 + drain 回归；gateway 集成编译就位 |
| ORA3-M5 | 码内注释 1s→30s（policy_loader/bootstrap/adapter lib/snapshot）；README/design.md 收敛节律按拓扑限定（docs 代理） | ✅ |
| ORA3-M6 | FAIL_OPEN 残留收口：forward_auth 文案改"无裁决，网关按 fail mode 裁决"；`HIGRESS_EXT_AUTH_TIMEOUT_MS` 实际接线（Client::new 读 env，非法回落 30s）；pipe.rs:17 头改 fail-closed 默认 + 拒绝分支 debug 日志；P5 标记清除 | ✅ egress 单测 2 |
| ORA3-M7 | AM-4 重检改不动点循环（每遍重算 accepted 集，N+1 界有证明）；链式悬空全解链 | ✅ 单测 2（含失败于旧代码的级联用例） |
| ORA3-M8 | 入站剥离 `x-gpustack-original-path`（transform 规则 3，先于 backstop 追加；防客户端伪造 :path） | ✅ 单测 1（含混合大小写/无 :path 用例） |
| ORA3-M9 | 中途写失败终止记账（镜像 B4c 截断分支：record_request+duration，kind 不新增）+ `report_incomplete_usage` 冲刷活快照（retained 透传，观测到 usage → completed=true 带 token；未观测保持空行语义） | ✅ core 单测 2 |
| ORA3-M16 | higress-config 不消费=文档化降级（equivalence A1:30 EQUIVALENT→NOT-CONSUMED；snapshot.rs/bootstrap 注释更正；R-9③ warn 保留待未来 managed 源） | ✅ 文档+注释 |
| ORA3-M17 | topology-B 种子仅 env：README/design 醒目提示（external raise 行为 + 需 HYGRESS_TOPOLOGY_B=true 或预建类） | ✅ 文档 |
| ORA3-M18 | AM-2 注入超集=文档化（代码注释 + README；不改变行为，GPUStack 目标均 OpenAI 协议） | ✅ 注释+文档 |
| ORA3-M19/OX-9/OX-11 | 默认安装 :443 差异、SNI 单默认证书、轮换需重启 runbook、15020/ADMIN_ADDR 注记 → pack/README + README 块 | ✅ 文档 |
| ORA3-M20 | gateway `default = ["integrations"]`（普通 build 即含数据面）；无 integrations 分支 error!；feature 保留供 --no-default-features 拆分 | ✅ 双模式编译 |
| 协调项 | integration.rs 构造补第 4 参；metrics.rs 契约计数器预置；bootstrap 死代码/Result.filter/多余括号修复 | ✅ |

门禁（B9.5 后）：**608 tests**（35 integration e2e 全绿）/ clippy `--workspace --all-targets --all-features` 0 /
`-p hygress-gateway --no-default-features` 0 / 无新依赖（Cargo.lock 未动）。注：本批实施后仓库在 rustfmt
默认（max_width=100）下非全量排版干净（历史风格更宽、未强制 fmt），未做全仓格式化以免噪音入批。

### B9.5 真机验证 ✅（镜像 f85cb78e，远端 /root/hygress-b3/b95.log + b95-verdict.txt；回滚点 gpustack:hygress-b7）
- readyz=200@~18s；hygress=1 / envoy·pilot=0；chat 基线 200 `HYGRESS_B95_OK`（usage 35/6）；末次 readyz=200，server 稳定。
- **ORA3-MAJ-1 活体成立**：`hygress_control_watch_error_total{class="permanent",kind=<6 类>}` 1→2（60s 永久退避周期递增）；
  `hygress_control_snapshot_store_total` 1→2；`last_store_timestamp_seconds` 前移 ~31s（30s tick 活体）；
  `fallback_exhausted_total` 0 / `usage_push_dropped_total` 0（真实 chat 后仍 0 = 行未丢）。
- tick-only 收敛标记 ×6（每 kind 各一次，限速门）；env unparsable=0（ORA3-M1 无误报）；启动脱敏摘要行就位
  （topology_b=false / admin_token_set=false / ext_auth_fail_mode=closed / ext_auth_timeout_ms=30000）。
- watcher 节律：70s 日志增量 +1690B（≈6 行，对比修复前 ~2000 行/s）——退避+限速持续成立。
- ORA3-M2 `/reload` 运行时探测：SKIP——容器未设 HYGRESS_ADMIN_TOKEN，fail-closed 下 /reload 按设计拒绝；
  缺文件保 LKG 返回 false 的 500 行为由单测 + 35 e2e 覆盖（本机无法免 token 触发）。

### B9.6 — 扩容轮结构批 ORA3-M10..M15 + 真机补验（提交 beaf83c；镜像 75802cca；远端 /root/hygress-b3/{b96*.log,b96-verdict.txt}）
| id | 修复 | 验证 |
|---|---|---|
| ORA3-M10 | 字节/multipart 工具跨 crate 三份复制收敛为 `hygress-core::bytes`（find_subseq/replace_bytes/contains_form_field/first_form_value_span）；两个 multipart 扫描器合并为一个定位器，body.rs/model_mapping.rs/usage.rs 变薄包装 | ✅ 单测 8（含等价性/畸形 part 跳过） |
| ORA3-M11 | 重复顶层 JSON 扫描器合一为 `top_level_members` 无 DOM 成员迭代器（深度上限 128、serde 精确语法），scan_top_level_value/scan_top_level_stream 建于其上 | ✅ 单测 3（双扫描器与 profile 一致性/畸形拒绝一致） |
| ORA3-M14 | 三次体扫→一次：`scan_top_level_profile` 融合(model 值+span, stream, has_stream_options, 收尾花括号)；prepare 驱动 resolve_fused/R-5 恒等与改写；AM-2 保持逐候选字节精确（注释说明 profile 已就绪待 PreparedRequest 承载）；不预注入 prepared.body（保 quota/request_content_bytes 不变式） | ✅ 单测 5（fused==legacy、字节精确、显式 stream_options 不被改写） |
| ORA3-M12 | 出站头构建统一 `copy_headers_excluding` + `utf8_header_value`（非 UTF-8 一律 drop+warn，不静默不 lossy）；响应方向单一 `RESPONSE_STRIP`，请求方向用 pipeline::HOP_BY_HOP；四处拷贝点统一 | ✅ 单测 3（含 HOP_BY_HOP 漂移守卫） |
| ORA3-M13 | 撤"frozen"说法：GatewayError 增 `BodyReadAborted`(400/slug)，`BodyTooLarge` 成为唯一 413 产出源；模块私有 BodyReadFailure 删除，截断→400+close 不派发语义字节保持 | ✅ error.rs 单测 3 + 既有截断 e2e |
| ORA3-M15 | 响应侧 usage 仅在 `prepared.usage.is_some()`（mirror 无）且 content-type 含 json/event-stream 时 feed（文本/octet/缺省跳过 DOM）；SSE/JSON 模型路由路径不变 | ✅ 单测 1 + 既有 usage e2e 全绿 |

门禁（beaf83c 后）：**630 tests** / clippy 双模式 0 / Cargo.lock 未动。主控修复：bytes 测试双借用、
HeaderValue::from_static→from_str、result_unit_err allow、dial 点 FnMut→收集 Vec 折叠、multipart "file" 断言纠错。

### B9.6 真机验证 ✅（镜像 75802cca，rollback gpustack:hygress-b95）
- readyz=200@~14s；基线 chat 200 HYGRESS_B96_OK；MAJ-1 控制面指标活体；usage_push_dropped=0；fallback_exhausted=0。
- **AM-2 真实流式引擎判别（关闭 ora-3/ora-4 诚实未知）**：GPUStack OpenAI Provider 路由（custom base_url=
  host mock SSE 服务器 18080，经 /v2/model-providers + /v2/model-routes 全 API 创建）→ 真机证明
  case A 裸 stream=true → 上游实收 include_usage=true（注入生效）→ 客户端 5 行 SSE + usage chunk + [DONE]；
  case B 显式 include_usage=false → 上游实收 false → 无 usage chunk。判据基于上游实收日志 + 原始 SSE 抓包。
- **ORA3-M2 /reload 运行时探测（关闭单测外欠账）**：compose 注入 HYGRESS_ADMIN_TOKEN 后实测
  缺文件→500（诚实文案）、有效文件→200、/config 无泄露、错 token→401；随后移除 token 恢复默认
  （无 token /reload→401 fail-closed 复原）。
- 清理复原：mock provider/route 删除、ssemock 停止、compose 还原（token 移除）、末次 chat OK + readyz=200。
- b96.log "A-usage=0" 为脚本引号转义假象（原始 /tmp/A.sse 含 usage chunk）——记录在 verdict 以免误读。

### B9.7 — AM-6 头物化专项批（提交 2bfbc20；镜像 4b8b5401；远端 /root/hygress-b3/b97*.log + b97-verdict.txt）
| 项 | 落地 | 证据 |
|---|---|---|
| alloc_guard 计量先行 | `measure_counted`（bytes+alloc 事件数）+ 5 个 `am6_*` 测试覆盖 prepare / build_outbound(1候选/写回/3候选) / dial 物化 | 见下实测 |
| HeaderMap::into_pairs | core transform.rs:129 新增消费式导出：独占(Arc::try_unwrap)→**移动**键值 0 额外 String 分配；共享→深拷（与历史一致） | 语义/derive/公共面不变（additive） |
| direct dial drain | `send_outbound` 按值收 `OutboundRequest`（headers 在 dial 后确证无消费者）；单趟 fold 跳过 ':'/DIAL_SKIP，host/content-type/body 移动不拷贝；删除两个中间 Vec | 行为：同集合同相对顺序、':path' 永不外发 |
| 拷贝次数收敛 | prepare_inner 已恰一次 make_mut（注释固化）；HOP_BY_HOP 改 contains 守卫（无变更候选保持共享 base 免空深拷）；去掉多余 Vec<&str> collect | build_outbound 单测语义不变 |
| **实测（release alloc_guard）** | prepare 3018B/91；build_outbound 1候选 3865B/66、+写回 4019B/77、3候选 11595B/198；**dial drain(独占) 672B/1 alloc**（原 ~1.9-2.2KiB/~30 → 削减 ~70%）；共享路径 1090B/29 | 预算已收紧（×2 余量）双 profile 复跑 11/11 |
| 真机冒烟 ✅ | 镜像 4b8b5401：readyz 200@28s、3×chat 200（走新 drain 路径）、usage_push_dropped=0、无 token /reload→401、server 稳定 | b97-verdict.txt |

诚实边界：逐候选一次 make_mut 深拷保留（候选 map 须与存活 base 分叉——语义必需，为最大残余成本）；provider dial 的 drain 未做（frozen egress 契约取 &outbound + 头读取）；shared-map 无变更候选多 2 个瞬时对（:path/content-type 克隆即弃）——独占常态无影响；read_headers 入站物化未动（非纯阶段）；AM-2 per-candidate 复核依赖 profile 随 PreparedRequest（M14 注记，独立后续）。

### B9.8 — AM-6b overlay 头（提交 3da7dd2；镜像 800ec8a8；远端 b98*.log + b98-verdict.txt）+ 诚实未知收尾
| 项 | 落地 | 实测（release alloc_guard） |
|---|---|---|
| OutboundHeaders overlay | core transform.rs：共享 base + overrides/removed delta；读/meterialize 与 clone-then-mutate 内容一致（golden 等价测试 6）；`HeaderOps` 泛型化 Transformer::apply（outbound keep 共用同一引擎）；build_outbound 只记 delta（O(1) Arc） | build_outbound 1候选 **3865→1071B**（−72%）、+写回 **4019→1417B**（−65%）、3候选 **11595→3213B**（−72%） |
| dial/materialize 时机 | direct dial 经 overlay into_pairs 单次（base 只在实 dial 候选克隆一次）；provider 分支 dial 前 materialize（全量拷贝仅 provider 实际拨号时付一次，provider 罕见） | overlay drain 1012B/25；provider materialize 1696B/50；独占 drain 仍 672B/1 |
| 门禁 | 640 tests / clippy 双模式 0 / alloc_guard 双 profile（预算按 AM6b 实测收紧 ×2 左右） | — |
| 真机冒烟 ✅ | 镜像 800ec8a8：readyz 200@20s、3×chat 200 HYGRESS_B98_OK、usage_push_dropped=0、无 token /reload→401、server 稳定 | b98-verdict.txt |
| **诚实未知收尾（P1/P2，b98-unknowns-verdict.txt）** | P1 建路由→Hygress 生效：**实测 17s**（≤文档 30s tick 界）——UI/API 建后立即推理可短暂 404 属预期，已量化；P2 默认安装（无 --ssl）:443 = connection refused，监听仅 80/30080/8081/15020——与文档化差异一致 | — |

诚实边界：wire 上 header 名级顺序变为确定性 base→delta（原 HashMap 随机序；HTTP 头序非契约，语义=多重集+同名内序保留）；provider 每次拨号付一次 materialize 深拷（可接受）；HeaderMap::into_pairs 现仅测试/独占 drain 使用（保留 pub）；prepare 的 3018B/91 不变（结构必需）。

### B9.9 — P1 控制面收敛修复（提交 8c183fe；镜像 bc3eb452；远端 b99*.log + b99-verdict.txt）
| 项 | 落地 | 真机实测 |
|---|---|---|
| 收敛节奏 30s tick → ~1s poll | adapter `CONVERGE_MIN_TICK`=1s，tick=`max(poll_interval,1s)`（POLL_INTERVAL env 默认 1000ms 可调）；指纹短路保稳态零 store；watch 事件仍为健康态快路径；策略 mtime 30s dutycycle 独立保留（非热路径） | **P1 建路由→生效 17s → 2s**（对齐真实 Higress pilot 1s LIST 轮询平价） |
| 文案同步 | 启动收敛模式日志/注释/README/design/equivalence 拓扑 A 表述改 ~1s poll（audit/checklist 历史记录保留原样） | 60s 日志增量 +1690B（仅 watch 限速行；1s 轮询静默） |
| 门禁 | 640 tests / clippy 双模式 0 | chat 基线 200 HYGRESS_B99_OK；store_total 按变更推进 |

### B10 — ora-5 修复批（≥9.5 收口轮，79a3d5e→5de0cd4→80db787；详见 docs/research/audit-oracle-review-ora5.md）
| 组 | 落地 |
|---|---|
| 观测/控制面（O3/O4/O5/O9/G1/G2/G6 + P1） | last_sync 心跳、reconcile_error{list/rejected} episode 计数、policy_reload observer、usage_pushed{completed}、build_info + 启动 version/contract_pin、指标 handle 缓存、LIST 失败 warn-once |
| 性能（R3/P2/P5/P7） | AM-2 memo（per-candidate 体扫消除，字节等价）、错误体 256KiB 封顶、限流 borrowed-key |
| 质量（Q1-Q4, T1-T5, G3） | OutboundHeaders names/append 契约修复、auth 文档、guardrail 活 accumulator、12 单元 + 4 e2e 边界测试 |
| 日志/可观测 O6/O11/O12/O13 | guardrail_error_total（服务失败与内容拦截分离）、逐请求 warn 降 debug、Debug 脱敏、upstream_errors{destination}、controller 任务观察 |
| 依赖/文档（M4/M5/M6/M3、O1/O2/O7/O8/O10） | ring-only TLS（aws-lc 出锁）、rand 清理、missing_docs 121 项 + 四 crate 强制、serde_yaml 离线决策、README/compose/operations.md |
| 门禁/真机 | **659 tests** / clippy 双模式 0 / alloc_guard 11/11 / cargo doc -D warnings 0；真机 b101（镜像 bfcf515）：新族活体 + 心跳前移 + 0 丢行 + 0 LIST 刷屏 + 401 fail-closed；回滚 gpustack:hygress-b100 |

状态：ora-6 收敛复核（五维 ≥9.5 判定）进行中。
