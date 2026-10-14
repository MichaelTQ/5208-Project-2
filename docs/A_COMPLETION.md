# A 工作完成与交接状态

已完成 A 可独立执行的工作：来源与环境、正式清洗/质量、跨年与站点描述性分析、统一运行入口与交接说明、完整报告素材，以及窗口交叉检查工具和样例。用户已确认 B/C 尚未交付，先完成 A 交给他们。真实联调在回交后执行，不把准备工作的 PASS 当成 B/C 模块通过。

| 工作 | 交付位置 |
|---|---|
| 数据/质量 | data/clean/observations、artifacts/stage2、docs/DATA_CONTRACT_V1.md |
| 跨年/站点结果 | artifacts/stage3/tables、figures；docs/STAGE_3_ANALYSIS.md |
| 完整 A 报告 | docs/REPORT_A_FULL.md、REPORT_A_MERGE_GUIDE.md；artifacts/stage5 |
| B 窗口检查包 | docs/WINDOW_CHECK_GUIDE.md、data/handoff/stage6_window_checks；artifacts/stage6 |
| 运行与交接 | README.md、docs/RUN_GUIDE.md、HANDOFF_B.md、MODULE_INTEGRATION.md |
| 最新文件清单 | artifacts/stage4/handoff_manifest.json、storage_inventory.json |
| 最终就绪状态 | artifacts/A_FINAL_STATUS.json、A_FINAL_CHECK_REPORT.md（执行 finalize_A 后生成） |

最终状态为 READY_FOR_B，含以下区分：A1–A5 完成；A6 的独立核验工具/准备完成；B/C 真正的标签、特征拟合、模型加载/预测及端到端检查为 PENDING_DELIVERY。没有模型效果结果，也没有声称整个项目已完成。

## 本轮新增的运行顺序

```bash
bash scripts/run.sh scripts/run_pipeline.py --mode verify
bash scripts/run.sh tests/test_stage4_runtime.py
bash scripts/run.sh scripts/check_portability.py
bash scripts/run.sh scripts/finalize_A.py
```

verify 已扩展到 Stage 5/6，在 Stage 3 后重建报告/夹具并运行其检查。finalize_A 刷新交接清单、核验核心与可选原始 CSV、更新 A4 验收后生成最终就绪状态。未更改代码/配置/数据的独立检查不需反复重跑；更新后使用上述顺序避免旧证据。

仅更新报告可用 `--mode report`；仅重建独立窗口检查包可用 `--mode window-check`。它们会使旧交接验收/最终状态失效，需要重新核验再交付。生产缓存规则和完整重建命令见 RUN_GUIDE。

## 真正回交后 A 还需要做什么

- B：用自己的实现导出两组核验查询；A 核对结果和实际全量窗口代码，审核历史特征/训练拟合范围。
- C：回交已保存模型、独立加载预测命令和例子；A 按真实接口复现。
- 将 B/C 的实际命令接入 README，并运行整个项目，记录真实结果。

这些是依赖外部代码的交叉审核，不由 A 预先虚构 B/C 结果或替他们建模。Lecture 5/6 不需要提供。最终 ZIP 在用户明确要求时创建。
