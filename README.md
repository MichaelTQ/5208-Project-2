# Singapore Rainfall Project — A

项目使用 Apache Spark 处理 2017–2024 新加坡五分钟降雨数据。A 的独立工作覆盖 Stage 1–6：来源与环境、正式清洗、跨年/站点分析、复现与交接、完整英文报告，以及窗口交叉检查工具。最终状态与证据见 [A 完成说明](docs/A_COMPLETION.md)。B/C 代码尚未交付，实际标签/特征/模型联调及整个项目验收待回交后执行。

## 接收交接文件后的快速开始

```bash
python3 scripts/verify_handoff.py
bash scripts/setup.sh
bash scripts/run.sh scripts/run_pipeline.py --mode sample
```

第一个命令只需 Python 标准库，核验核心文件大小和 SHA-256。环境安装后，sample 用 16,712 条正式真实记录跑通读取、汇总、保存与重读，约十秒；不会生成标签或模型。核心包不需要原始 CSV 即可供 B/C 读取正式观测。完整 A 验收另需原始 CSV 与可重建的分析面板。详见 [运行指南](docs/RUN_GUIDE.md)。

正式输入：`data/clean/observations/`，48,538,625 条、96 个 Parquet；字段依据 [正式数据契约 v1.0](docs/DATA_CONTRACT_V1.md)。

## 统一入口

```bash
bash scripts/run.sh scripts/run_pipeline.py --mode preflight
bash scripts/run.sh scripts/run_pipeline.py --mode sample
bash scripts/run.sh scripts/run_pipeline.py --mode verify
bash scripts/run.sh scripts/run_pipeline.py --mode rebuild --dry-run
bash scripts/run.sh scripts/run_pipeline.py --mode report
bash scripts/run.sh scripts/run_pipeline.py --mode window-check
```

verify 按 16 步复核已有 A 成果，包括 Stage 5 报告与 Stage 6 独立窗口检查；rebuild 从原始数据重建 A 流程，去掉 `--dry-run` 后才执行。report 只更新完整报告；window-check 只重建并验收窗口检查包。为防止复用旧规则缓存，rebuild 要求先备份/移走 `data/stage2/parsed` 与 `grouped`；入口不自动删除文件。模式/顺序在 `config/pipeline.json`，每次真实运行记录返回码、日志、代码摘要与耗时，失败即停止。从其他目录启动时用项目 run.sh 的绝对路径。

逐步输入输出、实际运行时间、空间需求及错误排查见 [运行指南](docs/RUN_GUIDE.md)；本轮证据见 [Stage 4 检查报告](artifacts/stage4/STAGE_4_CHECK_REPORT.md)。

## 先读这些文件

- docs/STAGE_1_PLAN.md：执行步骤与验收标准。
- docs/ENVIRONMENT.md：三人统一版本、本机实际配置与安装方法。
- docs/DATA_CONTRACT.md：Stage 1 未清洗开发样本的旧接口，保留历史，不代替正式 v1.0。
- artifacts/stage1/STAGE_1_CHECK_REPORT.md：执行结果与检查证据。
- docs/STAGE_2_PLAN.md：Stage 2 详细执行步骤。
- artifacts/stage2/STAGE_2_CHECK_REPORT.md：Stage 2 检查与实际结果。
- docs/DATA_CONTRACT_V1.md、CLEANING_RULES.md、HANDOFF_B.md：正式输入、规则和 B 交接事项。
- docs/PROJECT_DECISIONS.md：用户已确认的约定，含 Lecture 5/6 不需要提供。
- docs/STAGE_3_PLAN.md、STAGE_3_ANALYSIS.md、REPORT_SECTION_A.md：Stage 3 步骤、中文分析与英文 A 报告文字。
- artifacts/stage3/STAGE_3_CHECK_REPORT.md：Stage 3 验收结果；figures/ 提供六组 PNG/PDF。
- docs/STAGE_4_PLAN.md、RUN_GUIDE.md、MODULE_INTEGRATION.md：A4 执行步骤、统一运行说明、B/C 回交要求。
- artifacts/stage4/handoff_manifest.json、storage_inventory.json：核心与可选原始数据清单、未压缩大小和排除目录。
- docs/STAGE_5_PLAN.md、REPORT_A_FULL.md、REPORT_A_MERGE_GUIDE.md：完整英文 A 正文、六幅图注及 B 合并指南。
- artifacts/stage5/STAGE_5_CHECK_REPORT.md：报告、站点排名稳定性与事实来源核验。
- docs/STAGE_6_PLAN.md、WINDOW_CHECK_GUIDE.md：独立窗口工具、查询夹具和 B 诊断导出格式。
- artifacts/stage6/STAGE_6_CHECK_REPORT.md：15 项边界测试、36 查询 Spark 对照及六类故意错误拒绝证据。
- docs/A_COMPLETION.md：最终就绪状态生成方法，以及收到 B/C 后的真实联调边界。

## 环境

Python 3.11.14、Java 17、Spark 3.5.8。直接依赖在 requirements.txt，全部 Python 依赖版本在 requirements.lock.txt。安装方法见 docs/ENVIRONMENT.md。

```bash
bash scripts/setup.sh
```

## Stage 1 完整执行顺序

```bash
bash scripts/run.sh scripts/fetch_data.py
bash scripts/run.sh scripts/extract_samples.py
bash scripts/run.sh scripts/verify_spark.py
bash scripts/run.sh tests/test_integrity.py
bash scripts/run.sh scripts/check_stage1.py
```

fetch_data.py 使用官方元信息、datastore 与下载接口。API 请求至少间隔 13 秒，遇错误有限重试；实际 CSV 下载由官方 HTTPS 链接提供。完整下载保存为 data/raw/rainfall_YEAR.csv，中断文件后缀 .part；不会把 .part 当作可交付原始数据。

data/data_manifest.csv 包含八年来源、目录大小、实际导出大小、官方记录总数、实际行数和本地 SHA-256。官方目录大小不一定等于 API 导出大小；验收以导出 HTTP 总长度和官方逻辑记录数为准。SHA-256 是本地完整性留档，不能声称为官方发布的 CSV 校验值。

extract_samples.py 流式扫描完整 CSV 统计记录数，再提取连续站点日，内存使用不随原始数据总量增长。verify_spark.py 用 Spark 读 CSV、解析 SGT 时间、写/读 Parquet 并比较全部样本记录。check_stage1.py 独立重算八个文件的 SHA-256，检查数据清单及其它证据。

## Stage 2 完整执行顺序

```bash
bash scripts/run.sh scripts/stage2_ingest.py
bash scripts/run.sh scripts/stage2_clean.py
bash scripts/run.sh scripts/stage2_quality.py
bash scripts/run.sh tests/test_stage2_spark.py
bash scripts/run.sh scripts/check_stage2.py
bash scripts/run.sh scripts/write_stage2_report.py
```

ingest 使用 Stage 1 文件指纹核验输入，逐年保存全量解析检查点；clean 按规范键去重/隔离，写正式 Parquet；quality 生成站点、缺口、覆盖率和真实交接样例；之后进行边界回归、独立验收并生成报告。全量记录处理均由 Spark 执行，不将原始数据加载进 pandas。小型元信息和汇总表才在 Python 中读取。

中断后可复用成功的解析/分组检查点。若修改输入、config/stage2.json 或清洗程序，必须先移走旧的 data/stage2 检查点和旧的完成标记，再从 ingest 重新执行；不能把旧缓存当成新规则的结果。各下游步骤只在上游完成后运行，检查启动时清除旧 PASS 与报告，本轮成功后才生成新的证据。

正式观测唯一键为 station_id + observation_time，雨量 Decimal(20,9)，缺测不补零。时间戳区间终点解释仍为研究约定，具体限制见正式接口与检查报告。

## Stage 3 完整执行顺序

```bash
bash scripts/run.sh scripts/stage3_support_probe.py
bash scripts/run.sh scripts/stage3_analyze.py
bash scripts/run.sh scripts/stage3_figures.py
bash scripts/run.sh tests/test_stage3_spark.py
bash scripts/run.sh scripts/check_stage3.py
bash scripts/run.sh scripts/write_stage3_report.py
```

分析参数固定在 config/stage3.json。20 个描述性站点要求八年日历覆盖率均至少 80% 且单一有效坐标组合。主比较采用各站自身八年共用日历时刻并等站点加权；全部站点联合共用支持仅 3,976 个键、覆盖五个月，因此只用于局部站点差异与支持敏感性。全年观测累计不简单按覆盖率放大，不解释为全国年雨量或长期气候趋势。详细结果见 docs/STAGE_3_ANALYSIS.md。

全量计算由 Spark 完成；Matplotlib 只读取小型汇总 CSV 作图。近似湿分位数为 percentile_approx，accuracy=10000；独立检查对所有有湿观测的站点年验证经验秩边界，并重建两个匹配面板与正式数据比较。读数金额仍精确以 Decimal 核对。中间匹配数据可从正式输入重建；分析只用于描述，不自动为 B/C 挑训练站点。

## 目录

- config/：版本、Spark 初始参数、年度数据 ID 和样本站点。
- scripts/：环境、下载、样本与验证入口。
- data/metadata/：官方元信息、官方记录总数、下载记录。
- data/raw/：八个完整原始 CSV。
- data/samples/raw/：每年四站点连续日期样本。
- data/samples/parquet/：未经清洗的解析样本。
- data/clean/observations/：正式规范观测，按年/月分区。
- data/quarantine/：无效与冲突原始记录。
- data/stage2/：可重建解析/分组检查点，不默认放入交接 ZIP。
- data/handoff/：正式真实样例及可查看 CSV。
- data/analysis/stage3/：年度汇总、共用日历支持和匹配观测检查点。
- artifacts/environment/：实际环境与 Java 来源/校验记录。
- artifacts/stage1/：样本、Spark、最终验收证据。
- artifacts/stage2/：全量质量、站点、覆盖率、缺口、数据指纹与验收证据。
- artifacts/stage3/：分析表、六组 PNG/PDF、支持诊断及独立验收证据。
- artifacts/stage4/：环境/真实样本复核、统一入口运行日志、交接清单和验收报告。
- artifacts/stage5/：报告事实、图注、站点年排名/相关性、来源指纹及报告验收。
- artifacts/stage6/：合成/真实夹具清单、Spark 精确槽位对照、错误拒绝与工具验收。
- docs/：计划、字段契约、环境和 AI 记录。
- tests/：完整性、清洗/分析边界及交接运行失败路径的必要测试。

## Stage 5/6 与最终交接检查

完整英文报告位于 [REPORT_A_FULL.md](docs/REPORT_A_FULL.md)，B 按 [合并指南](docs/REPORT_A_MERGE_GUIDE.md) 将其并入总报告。它保留统计分母、共同时间支持、缺测与描述性范围，不包含 B/C 尚未产生的结果。

窗口工具的 21 个合成查询与 15 个真实查询是小型诊断夹具，不是生产训练标签。标准库参考计算与 Spark 精确时间连接逐字段比较；B 按 [窗口检查指南](docs/WINDOW_CHECK_GUIDE.md) 导出自己实现的答案后才能验收实际模块。

```bash
bash scripts/run.sh scripts/run_pipeline.py --mode verify
bash scripts/run.sh tests/test_stage4_runtime.py
bash scripts/run.sh scripts/check_portability.py
bash scripts/run.sh scripts/finalize_A.py
```

上述顺序更新本机证据，并由 finalize_A 顺序刷新文件清单、核验核心/八年原始 CSV、验收 A4，最终生成 artifacts/A_FINAL_STATUS.json 和 A_FINAL_CHECK_REPORT.md。任何交付代码、文档、数据或运行证据更新后，都应重新核验；不要沿用旧清单。check_portability 在同一台机器将小样本项目移到含空格的临时路径，显式复用本机环境，不等于 B/C 电脑或全新安装已通过。清单主文件与验收报告互相引用，避免循环摘要，这些补充文件按 handoff_policy 一并交接。

## 后续模块

Stage 3 已完成 A 的跨年与站点比较，Stage 5 完成完整报告，Stage 6 提供独立核验工具。后续由 B 使用 v1.0 输入契约开发标签与特征，明确时间区间约定并检查窗口完整性；A 在收到 B 的实现后使用现有检查包核对实际窗口、特征拟合范围并整合说明。

B 后续提供标签、特征与基线命令，C 提供训练、评估和加载预测命令，由 A 整合此 README；当前均为 NOT_DELIVERED。需回交的资料及 A 后续验收步骤见 [模块接入说明](docs/MODULE_INTEGRATION.md)。原分工建议训练 2017–2022、验证 2023、测试 2024。

## 交接

最终按用户后续指示打包 ZIP，交付代码、文档、质量与分析结果、正式数据和样例。清单分核心包与八年可选原始 CSV；原始 CSV 是否随 ZIP，按后续交接要求落实。排除 .venv、.runtime、.cache、可重建 parsed/grouped/analysis 中间结果和 Spark 临时文件。容量是未压缩大小，不预估 ZIP 压缩率。当前不创建 ZIP。

用户已明确 Lecture 5/6 不需要提供，不再列为待办或前置条件，已记录在 docs/PROJECT_DECISIONS.md。覆盖率口径已固定并检查；时间戳区间起止和更新字段语义保持注明假设的方式说明。
