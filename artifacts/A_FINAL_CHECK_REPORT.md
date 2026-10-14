# A 最终交接检查

状态：**READY_FOR_B**。检查时间：2026-10-11T19:03:42+08:00。

A 当前可独立执行的工作已完成；Stage 1–6 对应范围全部有验收证据。B/C 尚未交付，实际联调为 PENDING_DELIVERY，不代表模型或全项目已经验收。

| 工作包 | 状态 | 交付 |
|---|---|---|
| A1 来源/环境 | 完成 | 八年来源、原始数据清单、锁定环境 |
| A2 清洗/质量 | 完成 | 正式 Parquet、账本、覆盖/缺口/站点信息 |
| A3 跨年/站点分析 | 完成 | 16 份原分析表、六组 PNG/PDF |
| A4 运行/交接 | 完成 | 统一入口、运行指南、文件清单及摘要 |
| A5 报告 | 完成 | 完整英文正文、图注、站点稳定性、B 合并指南 |
| A6 独立核验准备 | 完成 | 21 合成/15 真实查询、15 已知答案测试、Spark 对照、导出检查接口 |
| A6 B/C 实际联调 | 待代码回交 | 窗口/特征拟合审核、模型加载预测、整个项目运行 |

核心文件 445 个，4,249,058,892 字节（4.249 GB）；可选原始 CSV 8,047,044,701 字节（8.047 GB）。这是未压缩文件容量；清单/验收补充文件无循环摘要，按 handoff_policy 一并交接。

所有核心与八个原始 CSV 的大小和 SHA 已核对；正式数据 48,538,625 行/96 文件，既有六组图表保持不变。只验收 A 已有数据流程和本轮新增材料，没有重新下载全量数据或在 B/C 电脑上验证。

## B 先读

1. README.md、docs/RUN_GUIDE.md 和 docs/DATA_CONTRACT_V1.md。
2. docs/HANDOFF_B.md、docs/MODULE_INTEGRATION.md。
3. docs/REPORT_A_FULL.md 与 docs/REPORT_A_MERGE_GUIDE.md。
4. docs/WINDOW_CHECK_GUIDE.md，使用自己的实现回交两组诊断导出。

## 证据

各阶段 artifacts/stageN/final_verification.json 与检查报告；最新文件清单 artifacts/stage4/handoff_manifest.json。阶段状态分别按实际范围解释，尤其 Stage 6 是独立核验准备。

Lecture 5/6 不需要提供。当前未创建 ZIP、未向组员发消息；用户明确要求打包后，按这份已核验清单整理交接包。
