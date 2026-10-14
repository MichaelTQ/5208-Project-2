# Stage 6 检查报告

检查结果：**PASS（A 独立核验准备）**。时间：2026-10-11T18:52:52+08:00。

8 项检查通过；15 项已知答案/失败边界测试；21 个合成和 15 个真实查询，与独立 Spark 精确时间连接逐字段一致。

6 类故意错误（>=q、缺测补零、包含 t、跨 split、舍入金额、浮点金额导出）经用户接口检查均返回非零/FAIL；正确导出通过。该正确导出来自 A 参考夹具，并不是 B 的代码运行证据。

真实窄表与正式样本逐键逐值一致，覆盖全部八年；正式数据 96 个文件与既有六组 PNG/PDF 指纹不变。

- PASS：fixture_inputs_and_source_sample_fingerprints_current
- PASS：fifteen_known_answer_precision_split_and_failure_tests
- PASS：independent_spark_exact_joins_all_36_queries_match_oracle
- PASS：future_six_rows_can_skip_a_required_timestamp_demonstrated
- PASS：real_fixtures_exact_formal_sample_and_all_eight_years
- PASS：actual_export_cli_accepts_correct_and_rejects_six_error_classes
- PASS：formal_observations_and_six_figure_pairs_unchanged
- PASS：cross_check_scope_and_real_external_integration_pending

## 后续真实联调

用户已确认 B/C 尚未交付。本阶段检查的是 A 的工具与夹具，不是其实际标签/特征/模型。收到 B 导出后按 docs/WINDOW_CHECK_GUIDE.md 验收，再检查训练拟合范围与 C 独立加载预测；最终按 README 跑整个项目。

A 的数据、描述性分析、报告及交叉检查准备可先交接给 B。Lecture 5/6 不需要提供；未创建 ZIP。
