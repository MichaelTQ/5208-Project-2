# A → B：正式数据、分析与运行交接说明

## 先看什么

1. `artifacts/stage2/STAGE_2_CHECK_REPORT.md`：执行结果和验收证据。
2. `docs/DATA_CONTRACT_V1.md`：正式输入字段、时间约定、覆盖率口径。
3. `docs/CLEANING_RULES.md`：去重、隔离、极端值与未知语义。
4. `docs/ENVIRONMENT.md`、`requirements.lock.txt`：由 A 决定、B/C 参照的环境。
5. `docs/RUN_GUIDE.md`：交接清单核验、正式样本运行、复核与重建的区别。
6. `docs/MODULE_INTEGRATION.md`：B/C 需回交给 A 的接口、依赖、命令和证据。
7. `docs/REPORT_A_FULL.md`、`docs/REPORT_A_MERGE_GUIDE.md`：完整英文 A 正文、六幅图注及总报告合并方法。
8. `docs/WINDOW_CHECK_GUIDE.md`：21 合成/15 真实查询、答案字段与实际导出检查命令。
9. `docs/A_COMPLETION.md`、`artifacts/A_FINAL_CHECK_REPORT.md`：A 完成范围、最终就绪证据与后续实际联调事项。

## 输入与运行

从 `data/clean/observations/` 读正式数据，先用 `data/handoff/stage2_sample/` 检查接口。不要把 Stage 1 未清洗样本和 Stage 2 正式样例混用。项目脚本用 `bash scripts/run.sh <你的脚本.py>` 启动；脚本会设置项目 Python 和 Java。Spark 时区必须显式为 Asia/Singapore。

接收文件后先运行 `python3 scripts/verify_handoff.py`（只需标准库），再安装环境并运行 `bash scripts/run.sh scripts/run_pipeline.py --mode sample`。核心清单包括正式数据和样本，不默认附八年原始 CSV。小样本流程检查正式字段、雨量/原始记录摘要、站点年汇总及保存重读，不生成未来标签。

在分组/窗口中按 station_id 排序 observation_time。source_year 是来源年度，训练切分应使用规范时间。除模型向量需要外，累计与阈值比较保留 Decimal，防止先舍入影响边界。

## B/C 需要落实的窗口规则

目前采用区间终点研究约定：在预测时点 t，未来 30 分钟由 t+5、t+10、t+15、t+20、t+25、t+30 六个五分钟读数组成。不包含 t，不能只取“未来六行”而忽略中间缺口。若六个槽位缺任一条，标签标为不可用，不能用零补齐。历史特征同样检查所需时间范围的槽位完整性。

“超过阈值 q”按累计雨量严格大于 q 定义；等于 q 不算超过。累计与阈值比较先保留十进制精度，由 B 实现并针对等于/略高于阈值的边界验证。

这是供 B/C 落实的接口约定，不是 A 已生成的标签或特征。时间起止若得到新官方/课堂证据，应统一修订，避免平移五分钟改变任务含义。

Stage 6 已将这些规则落实为独立标准库参考工具和 Spark 对照案例，见 `data/handoff/stage6_window_checks/`。B 使用自己的实现处理同一输入/查询，并按 WINDOW_CHECK_GUIDE 导出诊断答案；A 的夹具参考答案不能替代 B 实际代码的输出证据。示例历史为包含 t 的 12 槽，只用于核验，可按 B 的真实特征范围调整；不得把示例硬编码为唯一模型配置。

训练/验证/测试拟按 2017–2022 / 2023 / 2024 划分；整段未来标签窗口必须留在所属集合内。先划分时间，再拟合特征缩放、填补、编码和选择。更新时间字段不是已确认的首次可用性，不作为默认输入特征。年度覆盖率与全期站点变体表供质量解释，不能利用 2024 测试表现挑选训练站点或调参。

缺口详表 `artifacts/stage2/gaps/`，日/月/年覆盖率与站点元信息见同目录。元信息组合的首次/末次出现不证明期间一直使用该位置；站点运行期未获确认。极端标记暂保留，不默认剔除或截尾。

## 交付边界

A 已提供规范数据、质量与覆盖率结果、字段契约和真实边界样例。B 负责年内变化、标签/历史特征/阈值扩展与基线；C 负责主模型、验证、评估与加载预测。A 后续结合 B 的实现核对窗口边界和集成说明。

完整 A 报告优先使用 `docs/REPORT_A_FULL.md`，Stage 3 的 REPORT_SECTION_A.md 保留为较短阶段摘要，不要将两份重复叠加。完整稿已包含跨年、共同支持、站点差异/排名稳定性、质量与局限；B 并入年内变化和基线，C 并入主模型结果，再由 B 整理最终 PDF。不得提前填写尚未运行的模型结论或人工审核声明。

Stage 3 已提供 A 的跨年/站点分析与六组图表，见 docs/STAGE_3_ANALYSIS.md 和 docs/REPORT_SECTION_A.md。20 站描述性 cohort 使用了全期覆盖情况，包括 2024，因此不能直接作为模型训练选择。主年度比较采用站点自身八年共同时间；站点之间联合共同时间只覆盖五个月。年度观测平均值、五分钟湿比例与湿分位数不能冒充未来 30 分钟标签的分布。Lecture 5/6 不需要提供。

用户将另外指示最终 ZIP 交接给 B；当前不创建 ZIP。最终排除 .venv、.runtime、.cache、Spark 临时目录和可重建 parsed/grouped 中间结果。全量正式 Parquet、清单、质量证据、代码和样例应保留；原始 CSV 是否随包或另行提供再按用户交接范围落实。

Stage 4 已提供统一入口、核心/可选原始 CSV 文件清单及复现说明。B/C 在此目录尚未交付代码，A 当前只验收自己的流程。`--mode verify` 是现有 A 成果的复核，需要原始 CSV 与 Stage 3 分析面板；这些面板可从正式数据重建。接收核心文件并开始 B 的开发只需清单检查和 sample 模式。
