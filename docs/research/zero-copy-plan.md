---
status: in-progress
phase: 1
updated: 2026-09-04
---

# Zero-Copy 改进方案（热路径用户态拷贝最小化）

## Goal
在不改变 terminate-mode / TLS 终止 / 检视-改写-核算语义的前提下，把请求-响应热路径上**可避免的用户态 payload 拷贝/分配**从 ~2.5-3.5× body 降至 ≈0（仅剩 2 次 TLS 转换 + 条件性 1 次模型 splice），并附带分配计数与 wrk 双路验证，作为可在下一迭代直接移交 @coder 实施的最终方案。

## Context & Decisions
| Decision | Rationale | Source |
|----------|-----------|--------|
| 不追求内核级零拷贝（splice/sendfile/MSG_ZEROCOPY/io_uring） | TLS 终止（rustls 用户态 AES-GCM）+ terminate-mode 检视需明文，内核路径永远碰不到明文；MSG_ZEROCOPY 对 ≤512KB 体开销>收益；io_uring 只省 syscall 不省拷贝 | `ref:ora-1`, `ref:lib-1`(F15-F19，外部研究产物未入库，事实表存于会话) |
| 只做用户态 Bytes 引用式拷贝最小化 | pingora/hyper/reqwest 均为 Bytes-frame 语义，可达"每方向 payload 只碰一次" | `ref:ora-1` Part 2, `ref:lib-1`(F1/F3/F8/F12) |
| 请求侧必须整 body 缓冲（terminate-mode） | 模型字段提取 + guardrail/配额 + 改写需要完整明文；"零拷贝"在此指"单次捕获 + 引用复用"，非避免缓冲 | `ref:ora-1`, `ref:lib-1`(F6/F13) |
| SSE 响应侧是唯一有意义的"流式零拷贝"位置 | pingora 透传 chunk（write_response_body(Option<Bytes>)）本身 0 再缓冲；B2 仅消除 feed 累积拷贝 | `ref:ora-1`(S1), `ref:lib-1`(F3/F21) |
| 恒等映射短路（B4）为最高性价比项 | GPUStack 默认路由多为恒等 model 映射；免第二次全量扫描+splice；畸形 body 还免 N 次逐候选 apply | `ref:ora-1`(R4) |
| B5（非流式响应 DOM→有界扫描）延后 | 非流式响应占比低、中等回归风险，仅在有边界解析模板后做 | `ref:ora-1`(S3) |
| 头部每候选重编码（B-nit）保持现状 | reqwest/hyper 边界强制一次转换，payload ~KB，收益低 | `ref:ora-1`(R5/R8) |
| 3.2 guardrail 原位处理改为"度量后门控" | guardrail 规则无分隔符，匹配可跨 buffer 边界，需重叠窗口方案、风险更高；无静态规则时零收益 → 先出 3.1，4.3 度量证明是热点再实施 | `ref:ora-1`(S2), 本轮计划评审 |

## Phase 1: 现状基线 & 验证脚手架 [IN PROGRESS]
- [x] 1.1 热路径拷贝/分配清点（oracle 审计，见 §Context）→ `ref:ora-1`
- [x] 1.2 内核零拷贝不可行性研究 → `ref:lib-1`
- [ ] **1.3 建立"分配计数"回归护栏（隔离 + 分目标预算 + 比率）** ← CURRENT
  - 计数型全局分配器（`#[cfg(test)]` 门控，记录 `malloc`/`realloc` 的累计字节，不拦截，纯计数器）——不引入 `unsafe` 之外的全局改写；`dhat` 有相同的跨测试干扰问题，不能单独解决隔离。
  - **隔离（规范）**：护栏放在**专用集成测试目标** `crates/hygress-gateway/tests/alloc_guard.rs`（独立进程 = 独立全局分配器，并行 `cargo test` 的其他测试不会干扰计数），验收命令 `cargo test -p hygress-gateway --test alloc_guard -- --test-threads=1`（写入 §4.2）。备选方案（若集成目标受限）：线程 ID 键控计数器，仅归因测量线程的分配。断言一律用"预算上限"而非"≈ body 全局相等"（一次整 body 拷贝会远超 KB 级预算）。
  - **分目标预算**（512KB 输入时）：`extract_model`/`skip` = O(model 字符串)（断言 < 16KB，不得 O(body)）；`rewrite_json_model` = 恒等短路时 < 16KB（无 body 拷贝）、真实改写时 ≈1× body；`UsageSnapshot::feed` SSE = O(tail)（断言 < 16KB + 尾行残片，不得 O(body)）。
  - **线性比率护栏**：`allocs(1MB) < 4× allocs(256KB)`（对增长类回归，噪声下稳健）。
- [ ] 1.4 复用 H4 serde-oracle 金丝雀（body.rs 现有 `#[test]`，只增不改），并把 **B3 跳过路径奇偶用例**并入：
  - 被跳过（非目标）字符串内的坏转义：`{"bad":"\q","model":"x"}` → None（serde 拒绝）
  - 被跳过字符串内的非法 UTF-8 字节 → None
  - 被跳过字符串内的合法多字节 UTF-8 → 接受（校验并前进、丢弃）
  - 明确断言：**跳过路径必须"校验并前进"（validate-and-advance），不是仅前进**

## Phase 2: 请求侧 [PENDING]
- [ ] 2.1 **B1 read_body 容量预预留**（`crates/hygress-gateway/src/pipe.rs:743-760`）：存在合法 `Content-Length` 时 `Vec::with_capacity(min(len, max_body))` 后逐 chunk 读取，消除几何增长（收益 = ~½ body 拷贝；逐 chunk `extend_from_slice` 本身仍拷贝，连续累积是设计使然，保留）。
  - **注意**：`Content-Length` 已被 `max_body` 截断上限约束——如 `Content-Length: 8388608` 但实际 100B = 瞬态 8MiB 分配（上限内、请求末释放，可接受）；**不要**加第二层"聪明"上限破坏该保证。
- [ ] 2.2 **B3 skip_json_string 无物化**（`crates/hygress-gateway/src/body.rs` 扫描器 `parse_json_string` 及相邻 skip 路径）：对非目标 key / 非目标字符串值走"校验并前进"路径（validate-and-advance，不构建 `String`，且不丢失任何 JSON 文法校验——区分并实现独立 `skip_json_string`）；目标 key 仍解码返回。时序护栏在 1.3 的 alloc_guard harness 内实现，用**比率形式** `t(1MB) < 4-6× t(256KB)`（不设固定 1MB 绝对边界）；**release profile 下执行以保证时序稳定，debug 下放宽比率**。函数名以代码为准：`rewrite_json_model`（body.rs）。
- [ ] 2.3 **B4 恒等映射短路**（`crates/hygress-gateway/src/pipeline/mod.rs:104-127` + `pipeline/model_mapper.rs`）：
  - 载体 = `PreparedRequest` 新增字段（如 `body_model: Option<String>`），语义"prepare 改写后的当前 body 模型值"：改写 → `Some(mr.model)`；未改写 → 提取值（`None` 覆盖缺失/非字符串/畸形 → 每个候选完全跳过 apply，也省掉畸形 body 的 N 次扫描）。
  - 短路条件：`decoded == model` 则逐候选 O(1) 引用复用 body（跳过 `rewrite_json_model` 的第二次扫描+splice）。
  - ⚠ **陷阱**：prepare 的扫描 **span 在 prepare 自身改写后失效**——候选短路必须比较**值**，不得复用 span；span 仅用于首次真实改写。
- [ ] 2.4 更新 1.3 护栏断言（B1/B3/B4 落地后"请求方向可避免拷贝 = 0"）

## Phase 3: 响应侧 [PENDING]
- [ ] 3.1 **B2 `UsageSnapshot::feed` 原位行扫描**（`crates/hygress-core/src/usage.rs:356-360,488`）：在流入 chunk 上定位最后 `\n`；**跨 buffer 的行**（尾部前缀 + chunk 后缀）仍需一次**有界的按行拼接**（拷贝量 = 每条从 ~chunk.len() 降到 ~partial-tail，即所需收益）；`"usage"` 预过滤器（usage.rs:511）必须在**重组后的行**上跑（否则跨 chunk 的 `usage` token 被跳过）；计数仍在预过滤器之前（现有语义，不改）。补充"usage 跨 chunk 分片"测试。
- [ ] 3.3 更新 1.3 护栏断言（SSE: 每 chunk 拷贝 ≤ 尾部残片）
- [ ] 3.2 **S2 guardrail 原位处理 —— 度量后门控（延后实施）**：guardrail 规则无分隔符，匹配可跨 tail/chunk 边界；当前代码（guardrail.rs:117-124）在截断前对整 buffer 求值。原位变体需**重叠窗口**（tail ∪ chunk 前 MAX_TAIL 字节），仅配置静态规则时有收益、风险更高。**排序调整**：本迭代只出 3.1；4.3 度量显示 guardrail 拷贝是热点 → 再按重叠窗口设计 + 保留跨 chunk 匹配测试实现；否则跳过。
- [ ] 3.4 **B5（延后项，不在本迭代）**：非流式响应 usage 提取从全量 DOM 改有界 top-level 扫描——仅记录设计待办。

## Phase 4: 端到端验证 [PENDING]
- [x] 4.1 `cargo test --workspace` 与 `--features integrations`（含 22 Pingora e2e）全绿；clippy 双模式 0 warning；无现测改动（只增不改）
- [x] 4.2 分配计数护栏通过（1.3 命令 `cargo test -p hygress-gateway --test alloc_guard -- --test-threads=1`；及 2.4/3.3 断言更新）
- [ ] 4.3 复用 boxed wrk A/B rig（`85c407c` 方法）：/readyz c16/c64 与 e2e chat 各一条，记录 p50/p99/吞吐与每万请求 RSS delta（修复前 = 当前 HEAD）；**顺带判定 3.2 是否成热点**
- [ ] 4.4 可选 `perf stat -e cache-misses` 请求路径对比（若盒子可跑）

## Phase 5: 移交 & 归档 [PENDING]
- [x] 5.1 本方案获 @oracle 复核通过（PASS / 无 Critical/Major）→ 2026-09-04 round 3 = **APPROVE** `ref:ora-1`
- [x] 5.2 @coder/@orchestrator 依据本方案实施（B1-B4 + alloc_guard 栏护，工作树完成）
- [x] 5.3 @oracle 代码复审（§5.3，两轮重审含 B4 热路径接线修复）→ **PASS**；提交完成；benchmark 归档随 hygress-vs-higress 对比进行

## Notes
- 2026-09-04: 基线清点结论——body 驱动请求现有 ~2.5-3.5× 可避免 memcpy/alloc（R1 R3 R4 S1 S2），实现 B1-B4 后收敛到 2 次 A 类 TLS + 条件性 splice → `ref:ora-1`
- 2026-09-04: 任何对 body 的"再序列化"路径（rewrite 逐候选重跑）均已确认不存在；provider 路径 `body: None` 无 JSON 重编码，候选 body 为 Bytes 纯引用 → `ref:ora-1`
- 2026-09-04: hyper 直写依赖 rustls 的 vectored-write 支持；如剖析显示 Flatten 拷贝成为热点，可另立任务验证 `write_vectored` 毛利（当前不阻塞）→ `ref:lib-1`(F11)
- 2026-09-04: 内网可信上游去 TLS（UDS/明文 HTTP）是删一整轮密码学而非一份 memcpy 的更高价值动作，需安全评审另立议题 → `ref:lib-1`(F23/F24)
- 2026-09-04: 计划评审 round 1 = REQUEST_CHANGES；已解决：1.3 护栏"隔离+预算+比率"设计、2.3 `body_model` 载体与"值非 span"陷阱、3.1 跨 buffer 行重组与预过滤器顺序、3.2 改为度量后门控、B3 跳过路径奇偶用例与命名/锚点修正、Content-Length 瞬态分配说明、`ref:lib-1` 未入库标注
- 2026-09-04: 计划评审 round 2 = REQUEST_CHANGES（1 阻断 + 2 笔误）；已解决：1.3 隔离规范为专用集成测试目标 `tests/alloc_guard.rs` + `--test-threads=1`（备选线程 ID 键控计数器）、2.2 锚点修正为 `src/body.rs`、时序护栏归入 1.3 harness 并注明 release/debug 配置、命名统一 `rewrite_json_model`
- 2026-09-04: 计划评审 round 3 = **APPROVE**（全部锚点核对 HEAD、护栏可信、无新问题）；方案**进入可实施状态**，待 §5.2 移交 @coder
