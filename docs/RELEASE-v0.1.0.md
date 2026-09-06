# Hygress v0.1.0（冻结发布记录）

**tag**：`v0.1.0`（annotated）  **代码 HEAD**：`d564b21`
**包版本**：全部四 crate `0.1.0`（`CARGO_PKG_VERSION`；启动日志 `version=`、指标 `hygress_build_info{version}` 同步）
**性质**：多轮 oracle 审核-修复循环收敛后的冻结点（ora-6 五维 ≥9.5，见 docs/research/audit-oracle-review-ora5.md + audit-fix-checklist.md）。

## 1. 收敛结论（ora-6，独立收敛复核）
| 维度 | 收敛分 | 判词 |
|---|---|---|
| 代码成熟度/架构 | 9.5 | M1-M7 关闭取证；无新发现 |
| 代码质量/正确性 | 9.5 | Q1-Q4 + T1-T5 全关闭；memo 字节等价 |
| 性能/资源 | 9.6 | memo/P1/P5/P7/P6/P4（可消除部分）关闭 |
| 可运维/可观测 | 9.5 | O1-O13 全关闭（双计按处方修复后复查） |
| GPUStack 集成 | 9.5 | G1-G7 + AM-2 关闭；无 wire 回归 |

## 2. 质量门禁（HEAD `d564b21`）
- `cargo test --workspace --all-features`：**661 项全绿**（含 39 项真实 e2e、12 项 alloc_guard 分配预算；无 flaky/ignore）
- `cargo clippy`：`--workspace --all-targets --all-features` 与 `-p hygress-gateway --all-targets --no-default-features` 双模式 **0 警告**
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`：**0 错误**（四 crate `#![warn(missing_docs)]`）
- 工具链：rust-toolchain.toml 钉 1.98；workspace `rust-version = "1.89"`

## 3. 本轮主要演进（提交区间摘要，新→旧）
- **性能收尾**：P6 memchr（`6ed955a` 前）+ P4 absent-aware COW remove（am8：absent 0B/0 alloc；`6ed955a`）+
  P4 惰性入站头（限流/断体短路后物化；`d564b21`）
- **ora-6 收口**：guardrail_error_total 双计修复（`b384a85`）→ 五维收敛记录（`05a5183`）
- **M3**：serde_yaml → serde_yaml_ng（`35b5b39`）
- **ora-5 修复批**：观测族（心跳/失败计数/重载/usage_pushed/build_info）、AM-2 memo、P1 指标缓存、O6 日志收敛、
  Debug 脱敏、ring-only TLS、missing_docs 强制、运维文档、12 单测 + 4 e2e（`79a3d5e`…`1dae6db`）
- 更早基线见 audit-fix-checklist（B1-B10）/ dev-process / ora-1..ora-4 记录

## 4. 真机验证（125.67.215.17，GPUStack v2.2.3）
| 轮 | 镜像（sha 前缀） | 内容 | 判据 |
|---|---|---|---|
| b101 | bfcf515 | ora-5 观测族活体 + 心跳/0丢行/0刷屏 | /root/hygress-b3/b101-verdict.txt |
| b102 | cb1f8eeb | O12/O13 后基线 | b102-verdict.txt |
| b103 | c069d7ab | serde_yaml_ng 构建基线 | b103-verdict.txt |
| b104 | 4a3ded8 | P4-1/P6 | b104-verdict.txt |
| b105 | cf978c | P4 惰性入站（最终镜像） | b105-verdict.txt |
回滚 tag 链（真机 docker images）：`gpustack:hygress-b100/-b101/-b102/-b103/-b104`；容器默认态清场保持。

## 5. 部署/回滚口径
- 部署：README §4（交叉构建 `cargo zigbuild --release -p hygress-gateway --target x86_64-unknown-linux-gnu`；
  `pack/Dockerfile.hygress` 定制镜像；`pack/compose-hygress.template.yaml` 参数化 compose 模板）
- 回滚：README §4.5（切回官方镜像）或换回任一 `gpustack:hygress-b*` 回滚镜像
- 运维/告警/runbook：`docs/operations.md`；升级契约复核：boot 日志 `contract_pin` 指向 plugin-contract-pin.md §7

## 6. 记录在案的残余（非本版阻塞）
- P4 结构残余：存活请求一次入站物化 + 每真实变更 hop 一次 COW（pristine 供 fallback 回放 / auth/⑧ 读 prepared.base）——
  属数据流设计约束，µs 级量化（perf-tail-plan Phase 2；ora-5 §7）
- 引擎约束（非代码缺口）：CPU `serve.py` 忽略 stream / llama-cpp-python 0.3.35 不发 usage chunk → `completed=false`
  估算行由 GPUStack 服务端字节/分块估算承担；真流式引擎（llama-box）AM-2 判别已在真机实证（b100）
- 依赖：锁内 `serde_yaml` 仅剩 pingora-core 自身传递依赖（非本项目直接引用）
