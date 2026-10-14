# A 的复现与交接运行指南

## 第一次接收 A 的数据

所有路径均从项目自身位置解析；目录可以含空格。正式输入是 `data/clean/observations/`，契约为 `docs/DATA_CONTRACT_V1.md`。`docs/DATA_CONTRACT.md` 是 Stage 1 旧开发样本说明，不能据此解释正式样本。

先用系统 Python 3.11（不需要 Spark）检查交接文件，再安装锁定环境：

```bash
python3 scripts/verify_handoff.py
bash scripts/setup.sh
bash scripts/run.sh scripts/run_pipeline.py --mode sample
```

`verify_handoff.py` 检查核心清单的大小和 SHA-256；不是下载新数据，也不证明来自发布方的真实性。默认命令不要求原始 CSV；只有同时收到八个原始 CSV 时才加 `--include-raw`，加上后会要求这些文件齐全并核对摘要，缺少任何一个都会失败。不要复制 A 的 `.venv`、`.runtime`；先按 `docs/ENVIRONMENT.md` 安装 uv，再运行 setup 建立锁定环境。macOS ARM64 已实际测试；Linux/WSL 等平台仅提供安装方法，尚未实际验收。

## 六种统一入口

```bash
# 当前机器的 Python、包、Java、Spark、时区和计算检查
bash scripts/run.sh scripts/run_pipeline.py --mode preflight

# 真实正式样本：读取、独立核对、聚合、保存和重读
bash scripts/run.sh scripts/run_pipeline.py --mode sample

# 复核已有 A 的成果；需要原始 CSV、正式数据和分析检查点
bash scripts/run.sh scripts/run_pipeline.py --mode verify

# 只列出重建顺序，不执行、不更改输出
bash scripts/run.sh scripts/run_pipeline.py --mode rebuild --dry-run

# 更新完整英文报告与报告核验
bash scripts/run.sh scripts/run_pipeline.py --mode report

# 重建窗口诊断夹具，运行边界测试、Spark 对照与检查
bash scripts/run.sh scripts/run_pipeline.py --mode window-check
```

不指定模式时执行 sample。真实样本流程不生成标签、特征或预测；结果位于 `artifacts/stage4/smoke/station_year_summary.csv` 和 `sample_verification.json`。样本中人为选取的站点日之间有间隔，不用该样本汇总冒充完整覆盖率或年雨量。

项目外启动示例（把路径换成自己的项目位置）：

```bash
bash "/path/with spaces/5208 Project 2/scripts/run.sh" scripts/run_pipeline.py --mode sample
```

命令顺序固定在 `config/pipeline.json`。日志与耗时位于 `artifacts/stage4/runs/<本轮编号>/`，最新模式记录是 `<模式>_run.json`。失败时立即停止，不继续生成下游报告；运行记录标 FAIL。真实运行会清除旧 Stage 4 验收和 A 最终就绪状态，需要重新核验再交接。manifest 的摘要会因数据、代码、报告或运行证据更新而变化，这是正常的版本变化。

## 从原始 CSV 重建 A 的完整成果

在新目录安装环境后执行：

```bash
bash scripts/run.sh scripts/run_pipeline.py --mode rebuild
```

fetch_data 使用已有官方数据清单和文件；缺少原始 CSV 时会请求官方来源，需要网络。已有 CSV 不能随意换成新发布版本而仍使用旧摘要。网络失败保留 `.part`，不会当作完成数据。

统一入口要求 `data/stage2/parsed` 与 `grouped` 不存在，防止静默复用旧规则的缓存。在已有项目上重新做全量处理时，可先将整个 `data/stage2` **移动到项目以外的备份目录**，再运行 rebuild。请保留备份直至新验收通过；脚本不自动删除它。移动路径由使用者自行选择。单阶段故障恢复仍可按 README 的分步命令复用确认未变化的成功检查点；如果原始数据、配置或清洗代码改变，不可复用这些缓存。

rebuild 写入现有输出目录，会更新 A 的正式数据、质量结果、分析表、图和报告。它只重建 A 的工作，不会调用未交付的 B/C 模块。Stage 4 本轮实际运行的是 verify，未宣称重新下载并重新清洗了全量数据。

## 输入、输出与先后依赖

| 步骤 | 必需输入 | 主要输出 | 下游 |
|---|---|---|---|
| 获取与样本提取 | 官方元信息/API、config/project.json | 八年 CSV、来源清单、未清洗连续样本 | Stage 1 核验、ingest |
| Stage 1 Spark/检查 | 原始样本、八年 CSV、锁定环境 | 环境/完整性证据 | ingest |
| ingest | 已核验原始 CSV、Stage 2 配置 | parsed 检查点、诊断 | clean |
| clean | parsed、清洗规则 | 正式年月 Parquet、隔离表、账本 | quality、分析、B/C |
| quality | 正式观测、清洗账本 | 覆盖率、缺口、站点变体、正式真实样本 | Stage 2 核验、support probe |
| support probe | 正式观测、年度覆盖与站点变体 | 日历共同支持、固定站点清单 | analyze |
| analyze | 正式观测、共同支持 | 年度/站点/共同支持统计 CSV、分析检查点 | figures、Stage 3 核验 |
| figures | 小型分析 CSV | 六组 PNG/PDF、数据来源指纹 | 报告 |
| Stage 1–3 独立核验 | 上述数据、边界测试结果 | 数据/质量/描述性分析 PASS、阶段报告 | Stage 5/6 |
| Stage 5 完整报告 | Stage 2/3 表、六组图、环境/耗时证据 | 英文正文、站点排名稳定性、来源事实与合并指南 | B 总报告 |
| Stage 6 窗口检查包 | 正式真实样本、窗口配置、既有图指纹 | 合成/真实查询、参考答案、Spark 对照、导出验收工具 | B 实际实现审核 |
| 核心交接清单核验 | 核心文件、handoff_manifest.json | 文件大小/SHA 复核 | B/C 读取与开发 |

verify 会重跑各阶段验收和报告生成，但不重建缺少的分析面板。若只收到核心交接文件，可先从正式数据运行 Stage 3 的 support_probe、analyze、figures 来恢复面板；Stage 2 验收及 Stage 1 验收还需要可选原始 CSV。接收正式数据、运行 sample 不需要 `data/stage2` 和 `data/analysis`。

## A 最终就绪检查

```bash
bash scripts/run.sh scripts/run_pipeline.py --mode verify
bash scripts/run.sh tests/test_stage4_runtime.py
bash scripts/run.sh scripts/check_portability.py
bash scripts/run.sh scripts/finalize_A.py
```

finalize_A 依次重建清单、核验核心与可选原始 CSV、验收 A4，再写入 A 最终状态。READY_FOR_B 表示 A 可独立执行的工作完成；B/C 实际联调保持 PENDING_DELIVERY。在只收到核心文件的机器上，使用前面的清单检查和 sample 即可开始开发，不能直接声称全量验收通过。详见 A_COMPLETION.md。

## 耗时与磁盘

本机首次 Stage 4 sample 用时 10.08 秒。verify 的逐步实际耗时见 `artifacts/stage4/verify_run.json`，它与下载/全量清洗/分析生产时间不同，不能相加后称为原始数据至模型的端到端时间。Stage 2/3 实际生产耗时记录在其完成 JSON 中；网络下载时长和其他电脑运行时长不据此推算。

本轮文件容量以 `artifacts/stage4/storage_inventory.json` 为准：核心包包括约 4.22 GB 正式 Parquet；可选原始 CSV 约 8.05 GB。还需预留 parsed/grouped、Spark shuffle 和 Stage 3 面板空间。不要只按最终包大小准备重建磁盘；本机已有 parsed/grouped 约 9 GB，临时占用取决于阶段和并行设置。该统计是未压缩文件大小，不是 ZIP 压缩后大小。

## 常见问题

| 情况 | 检查与处理 |
|---|---|
| 找不到项目 Python | 在项目中运行 setup.sh；不要移动旧 .venv |
| Java 不存在或不是 17 | 检查 JAVA_HOME；macOS ARM64 可用 setup，其他系统安装 Temurin 17 |
| 包版本不一致 | 按 requirements.lock.txt 安装，运行 preflight 验证 |
| Spark 无法启动或内存不足 | 检查本机回环通信与剩余内存；4 GiB 堆并不等于机器总内存需求。参数变化须记录，不能沿用 A 的环境 PASS |
| 原始 CSV 或分析检查点不存在，verify 失败 | 使用 sample；若要复核全部 A 结果，取得原始 CSV 并按表恢复检查点 |
| 重建因旧缓存停止 | 先备份/移动 data/stage2，或者按未变化规则的单阶段恢复说明执行 |
| 交接摘要不一致 | 检查缺文件/解压/版本差异，不以删除清单条目绕过；A 更新后重建清单并重新验收 |
| NativeCodeLoader 提示 | 本机 Spark 使用内建 Java 实现完成计算；只有后续实际异常/非零返回码才是本次失败证据 |

## B/C 接入

当前没有 B/C 可执行命令。A 已提供 `docs/WINDOW_CHECK_GUIDE.md` 和独立窗口查询包。B 请按该接口运行自己的代码，并按 `docs/MODULE_INTEGRATION.md` 回交真实代码、输入/输出说明、依赖、已验证的命令与运行证据。A 收到后加入入口和 README，核对未来六个精确槽位、阈值严格比较、缺口、切分和训练拟合范围，以及 C 模型加载预测。

ZIP 等用户后续指示创建；本阶段的清单与空间说明只用于交接准备。Lecture 5/6 不需要提供。
