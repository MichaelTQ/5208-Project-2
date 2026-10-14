# Stage 4 检查报告

检查时间：2026-10-11T19:03:41+08:00。结果：**PASS（A4 当前独立范围）**。

12 项独立检查、8 项失败/完整性边界测试通过。

## 已验证

- 统一 verify 入口顺序执行 16 步，返回码全部为 0；本轮实际用时 142.02 秒。
- Stage 1：八年原始 CSV 摘要与 5 项完整性测试；Stage 2：全量数据 8 项验收与 9 项边界测试；Stage 3：11 项验收与 6 项分析边界测试均通过。
- 正式真实样本 16,712 行，31 个站点年汇总；原始记录摘要与逐键雨量、独立 Decimal 汇总、所有字段 Parquet 往返与汇总 CSV 一致。
- 当前本机环境与 Spark 实际计算通过；项目外启动及含空格临时迁移项目通过。迁移复用本机环境。
- 运行中失败/脚本缺失会停止并标 FAIL；缺文件、同大小篡改、重复/越界清单条目被拒绝，可选原始 CSV 缺少不误报核心失败。
- 核心文件完整枚举并逐个 SHA-256 检查；八个可选原始 CSV 也已核验。

## 交接容量（未压缩）

| 内容 | 文件数 | 字节数 | GiB |
|---|---:|---:|---:|
| 核心文件 | 445 | 4,249,058,892 | 3.957 |
| 可选八年原始 CSV | 8 | 8,047,044,701 | 7.494 |

清单/容量/验收文件为不互相循环摘要的补充文件，随核心文件交接；容量表不含这些小型补充文件，也不是 ZIP 大小。正式 Parquet 为 96 文件、4,223,822,379 字节。排除机器环境、缓存、Spark 临时文件、parsed/grouped 和分析检查点。

## 检查项

- PASS：stage1_stage2_stage3_acceptance_pass
- PASS：ordered_unified_verification_current_script_fingerprints
- PASS：current_environment_and_actual_sgt_decimal_computation
- PASS：real_sample_source_values_independent_amounts_and_roundtrips
- PASS：relocated_sample_space_path_external_cwd_same_machine
- PASS：eight_failure_and_integrity_boundary_tests
- PASS：all_mode_previews_ordered_and_do_not_mutate_evidence
- PASS：complete_core_and_optional_raw_inventory_all_sha256_verified
- PASS：formal_parquet_96_files_match_independent_stage2_fingerprints
- PASS：six_png_pdf_figures_and_source_tables_in_delivery
- PASS：documentation_links_and_confirmed_lecture_decision
- PASS：external_module_status_separate_from_A_acceptance

## 尚需实际外部代码的工作

B/C 在本目录尚未交付：当前没有验收标签/特征/基线或模型训练/加载/预测。后续按 docs/MODULE_INTEGRATION.md 接入并做 A6 联调；不得将本次 A4 PASS 当作整个项目 PASS。

本次执行的是已有成果复核；完整重建顺序已提供，但未在本轮重新下载、全量清洗或声称新安装验证。Lecture 5/6 不需要提供；未创建 ZIP，也未向组员发送消息。

## 证据与复查

- verify_run.json 和 runs/：真实步骤、代码摘要、返回码、耗时及日志。
- preflight.json、sample_verification.json、portability_check.json、boundary_test_results.json。
- handoff_manifest.json、storage_inventory.json、manifest_verification.json。

```bash
bash scripts/run.sh scripts/check_stage4.py
```

代码/文档/数据或证据更新后先重建清单并运行 verify_handoff，再验收。
