# Hygress Oracle 收敛复核简报（ora-4，HEAD 91172d3）

> 性质：ora-3 后 B9.5 修复批（含真机验证 f85cb78e）的**收敛复核**。目的：验证各维度缺口收敛，作为
> 审核-修复循环的收口判定依据（ora-3 全部 MAJOR 与条件清单第 1-4 组闭环、无新 MAJOR、无 BLOCK、评分不低于 ora-3）。
> 方式：先并行 5 维度独立复核（超时中断）→ 单代理前台收敛复核兜底（限定时间、逐项 file:line 取证）+ 主控抽证。
> 结论：**APPROVE；加权 ≈8.6/10（ora-3 ≈8.0）；循环可收口**。

## 1. 五维结论总表

| 维度 | ora-3 分 | ora-4 分 | 判词 | ora-3 项闭环 | 新 MAJOR |
|---|---|---|---|---|---|
| 代码成熟度/架构 | 8.0 | **8.5** | APPROVE | MAJ-1 ✅ / M1 ✅ / M20 ✅ | 0 |
| 代码质量/正确性 | 8.0 | **8.5** | APPROVE | M6/M7/M8/M9 ✅ | 0 |
| 性能/资源 | 7.5 | **8.0** | APPROVE | M14/M15/AM-6 按期挂账、无回退 | 0 |
| 可运维/可观测 | 7.5 | **8.5** | APPROVE | MAJ-1 ✅ / M1..M5 ✅ | 0 |
| GPUStack 集成 | 9.0 | **9.0** | APPROVE | M16..M19 ✅（doc-only 决策） | 0 |
| **加权** | ≈8.0 | **≈8.6** | **APPROVE（无 BLOCK）** | — | **0** |

## 2. 关键取证（file:line，代理 + 主控双重核实）

- **ORA3-MAJ-1**：metrics.rs 注册 191-208（watch_error_total / snapshot_store_total / last_store_timestamp_seconds
  193/200/204）、record 372-407、渲染断言测试 622-645；adapter `ControllerHooks` 99-109、`Controller::new` hooks 参
  160、调用点 387（watch_error）/495（snapshot_store）；bootstrap 接线 665-681、`install_panic_hook` 462-482
  （error! + exit(1) → s6 重启）、收敛模式日志 699-711、启动脱敏摘要 493-513。
- **ORA3-M6**：egress 全仓 grep 无 FAIL_OPEN 日志串；`HIGRESS_EXT_AUTH_TIMEOUT_MS` 为真旋钮（const 44、
  resolver 59、`Client::new` env read 149、测试 337-356：1500ms 生效 + 非法回落 30s）。
- **ORA3-M7**：config.rs 不动点循环 `0..=initial_len`（250-251，bound 证明 244-247）+ 级联链式测试 1763-1867 +
  健康链对照（不受扰）。
- **ORA3-M8**：transform.rs 规则序 remove original-path（264）先于 backstop backup（270）；伪造头测试 455-492。
- **ORA3-M9**：stream_back 携 `&mut Option<UsageSnapshot>`（1614）；保留仅在 Err/终末路径（1684/1726/1734）——
  **逐 chunk 稳态零新增**；写失败终末 record_request+duration（656-660）+ 活快照冲刷（668-673、
  report_incomplete_usage 1802-1824）；其余调用点 566/784/1715 传 None；usage.rs 中段冲刷测试 1158-1225。
- **ORA3-M1..M5**：gateway config.rs warn_unparsable 逐键；policy_loader `loaded_once` + boot 缺文件 warn +
  reload 缺文件保 LKG 返 false；admin /reload 500 诚实文案 + e2e；M3 守卫 `redirect_count>0`（758-764）；
  usage_sink `on_drop` 尾参（92-96）+ 三丢弃点 + drain-on-close；bootstrap 565-570 接线
  `record_usage_push_dropped`；integration.rs:345-350 第 4 参 None。
- **AM-2 超集**：pipeline/mod.rs 305-314 注释化声明（deliberately no behavior change），注入点逻辑零 delta。
- 门禁：608 tests / clippy 双模式 0 / Cargo.toml:22 `default=["integrations"]`（记录在案）。
- 真机交叉一致（f85cb78e）：watch_error 6 类 1→2 ↔ adapter 387 + metrics 389；store 1→2 ↔ 495/396；
  last-store +31s ↔ 407；tick-only ×6 ↔ 收敛模式日志；env unparsable=0 ↔ 仅解析错误时 warn；watcher
  +1690B/70s ↔ 退避+限速接线不变。

## 3. 各维最强残余风险（均无 MAJOR）
- 成熟度：无（进程级 panic hook 有意 exit(1)）。
- 质量：无；仅 INFO 级文档残留（pipeline/mod.rs:25 模块清单 "(P5)" 措辞、egress guardrail.rs:28 注释引
  FAIL_OPEN——非日志；pipe.rs:1276 "(fail-open)" 为 guardrail on_error 放行语义，非 ext-auth 残留）。
- 性能：无；M9 保留为错误路径指针赋值，控制面指标仅在 adapter 后台循环（387/495），不在数据面。
- 可运维：无；/reload 缺文件 500 的真机免 token 探测未演练（单测 + e2e 覆盖，checklist 如实记录）——非阻塞。
- 集成：无；M16-M19 零 wire 变化已核；AM-4 不动点只影响悬空链畸形配置。

## 4. 诚实局限
- 文档散文（README/design/equivalence/pack 的 M5/M16-M19 部分）未逐句独立重读——依据 checklist/fix-report 逐项
  ✅ 声明 + 代码侧零行为改动佐证。
- 本复核未运行测试/构建（按收敛轮约定，门禁证据采信 608/clippy 0 记录）。
- 三项诚实未知仍列为 open（真实流式引擎 AM-2 判别、拓扑 A ≤30s 对 UI 流敏感度、默认安装 443）——fix-report §8。

## 5. 结论
no new MAJOR across dimensions: **yes**；no BLOCK: **yes**；gaps converged: **yes**。加权 ≈8.6/10；
ora-3 全部 MAJOR 与条件清单第 1-4 组代码层闭环且与真机佐证一致，判词 APPROVE——多轮审核-修复循环收口。
