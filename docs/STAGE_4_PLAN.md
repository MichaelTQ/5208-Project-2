# Stage 4：A 的运行入口、README 与交接准备

## 范围与验收边界

本阶段落实原工作包 A4。环境由 A 确定，B/C 参照；Lecture 5/6 不需要提供。当前只有 A 的代码，B/C 实际联调在收到其交付后进行。不得把 A 的复现通过写成模型或全项目已通过。最终 ZIP 由用户后续明确要求时创建。本阶段不生成标签、特征或模型，也不代替 A6 对 B 的窗口实现作验收。

## 详细执行步骤

1. 核对 Stage 1–3 PASS 和现有依赖。README 以 DATA_CONTRACT_V1.md 为正式输入依据，旧 DATA_CONTRACT.md 只描述 Stage 1 未清洗样本。
2. 将已有 A 命令串成 config/pipeline.json 与 scripts/run_pipeline.py：preflight、sample、verify、rebuild 四种模式。支持只预览命令；每一步顺序执行、保存时间/返回码/日志，遇失败立即停止，失败运行不得留下该次 PASS。
3. 增加环境预检：当前 Python、锁定包、Java 17、Spark 3.5.8、SGT、本地四线程、实际 4 GiB 堆及简单 Decimal 计算；记录本机与磁盘。错误给出安装入口，不擅自更改 B/C 的系统。
4. 真实小样本流程只读 data/handoff/stage2_sample：检查正式字段、唯一键、五分钟网格、雨量精度；按站点年汇总，写 Parquet/CSV 并重读。以独立读取 CSV 后用 Python Decimal/SGT 计算的汇总核对 Spark 结果，不将缺测补零，也不虚构标签。
5. 从项目之外的目录调用统一入口，验证路径含空格仍可启动。用 verify 模式重新运行 A 的环境、样本、Stage 1–3 检查及报告生成，记录本轮真实耗时。verify 不重跑清洗/分析生产流程，依赖已有输入和中间分析面板；rebuild 列出从原始数据开始的重建顺序。
6. 制作交接清单：相对路径、文件大小、SHA-256、分类与纳入理由。核心包含代码/文档/依赖、正式 Parquet、隔离表、样本、来源登记、质量结果、分析表与图。原始 CSV 为单列可选组；机器环境、缓存、Spark 临时文件、解析/分组/分析检查点排除。清单并非 ZIP。
7. 提供仅依赖 Python 标准库的交接清单验证器，核验所有核心文件，拒绝越界路径、缺文件、大小/摘要变化与重复项；可选核验原始 CSV。
8. 更新 README 与运行指南：安装、快速样本、复核/重建区别、逐步输入输出、实际耗时、空间统计、缓存失效处理、错误排查。B/C 只列明待接入，不写不存在的执行命令。
9. 制作 B/C 模块回交资料模板：已测试命令、输入/输出字段、额外依赖、split/窗口/阈值、训练拟合范围、模型保存/加载/预测样例、运行证据。A 后续据此整合 README；当前外部集成状态为 NOT_DELIVERED。
10. 检查失败中止/路径含空格/清单篡改防护；核对原正式数据指纹、六组图表、文档链接与排除规则。生成 Stage 4 独立验收报告，说明本次是已有 A 结果的复核，未宣称重新下载或在 B/C 电脑上运行。

## 输出

- config/pipeline.json、scripts/run_pipeline.py、preflight.py、smoke_sample.py：统一复现入口。
- scripts/build_handoff_manifest.py、verify_handoff.py：无 ZIP 的交接清单与验证。
- docs/RUN_GUIDE.md、MODULE_INTEGRATION.md、更新后的 README/HANDOFF_B/ENVIRONMENT。
- artifacts/stage4/：当前环境、小样本证据、运行记录、核心/可选原始数据清单及验收报告。

Stage 4 的 PASS 仅表示 A4 当前可独立执行部分完成。B/C 代码接入、模型加载、预测及标签窗口联调仍须在真实代码到位后验收。
