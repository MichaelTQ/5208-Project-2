# 给 B：未来 30 分钟窗口检查包

这个包是 A 的独立交叉检查工具。它用少量已知答案检验 B 的窗口实现，不生产全量训练标签，也不决定 B 的模型特征。B/C 尚未交付，因此当前通过的是检查工具本身；不是 B/C 模块已通过。Lecture 5/6 不需要提供。

## 使用哪些文件

`data/handoff/stage6_window_checks/` 提供 synthetic/real 两组 observations.csv、queries.csv、expected.json。合成样例覆盖 21 种边界查询；真实样例从正式交接样本的 16,712 条观测中选择，覆盖八年、正值/零值、缺口和跨午夜。真实样本人为选择了站点日，缺口可能是样本截断，也可能是源数据缺口；它只检查当前输入的可用性，不能用其缺口比例描述全数据。

观测字段：station_id、observation_time（带时区的规范时间）、rainfall_mm（精确十进制）。查询字段：query_id、station_id、prediction_time、q_mm、history_slots、case。rainfall_mm 与 q_mm 均为毫米。

## 核验口径

- 在研究约定的区间终点语义下，目标是 t+5、10、15、20、25、30 分钟六个精确时间槽，排除 t。未来缺一个槽，累计和标签保持 null。简单未来六行可能跳过缺口；Spark 的 rowsBetween 按行位置确定范围，使用时必须另核验实际时间。[Spark 3.5.8 rowsBetween](https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.Window.rowsBetween.html)
- 超过 q 指累计严格大于 q；等于 q 为 0。金额字符串保留十进制，不先取整或转浮点。无雨六槽完整时为有效零累计，缺测不等于零。
- 采用 train=[2017-01-01,2023-01-01)、validation=[2023-01-01,2024-01-01)、test=[2024-01-01,2025-01-01)，均为 SGT。t 和所有未来目标时间都在同一集合，才可作为该 split 的标签。普通跨日/闰日/同一训练集合内跨年允许。
- label_usable 只检查完整目标与 split；历史完整性单独报告。夹具以含 t 的过去 12 个五分钟槽作为历史检查例子，不是 A 指定 B 必须采用的特征。预测时刻 t 已观测是历史信息研究假设，不能据此证明实时上报可用。
- 历史可跨到前一个 split，因为这些是过去信息；不得通过未来信息或在 validation/test 上拟合转换。这个导出检查器不能单凭窗口输出证明特征拟合没有泄漏，真实代码回交后还需审核。

配置在 config/window_audit.json。核验器最多读 100,000 条观测、5,000 个查询，history_slots 支持 1–2,016。这个上限控制小样本检查的规模，不是模型历史范围限制；更大的实际开发数据仍由 Spark 处理。

## B 怎样运行

1. 把夹具观测输入自己的窗口逻辑，以 queries.csv 指定的站点、t、q 和示例历史槽位导出核验结果。可以写一个字段适配器；不要把 expected.json 当作自己的实现输出。
2. 导出 JSON 数组，每个 query_id 一条。字段和类型依照 expected.json：金额为字符串；可用性为 JSON true/false；label_exceeds_q 为整数 0/1 或 null；缺槽列表为按示例顺序、带 +08:00 的时间字符串。
3. 使用 A 的标准库检查器，与自己的导出比较：

```bash
python3 scripts/window_audit.py \
  --observations data/handoff/stage6_window_checks/synthetic_observations.csv \
  --queries data/handoff/stage6_window_checks/synthetic_queries.csv \
  --actual /path/to/B_synthetic_export.json \
  --output /path/to/B_synthetic_audit.json
```

real 组同样运行，替换对应文件名。0 返回码/PASS 表示这些指定查询与独立 oracle 一致；不代表全量标签、所有特征或模型已经验收。输出为 FAIL 时查看 mismatches 中 query_id、字段、期望与实际值，修复后重跑。

不提供 --actual 时，只生成 REFERENCE_ONLY 的 A 参考诊断，不能将其作为 B 通过的证据。duplicate query_id、少/多查询、非法/重复观测、非网格/无时区时间、非法金额会报错。检查器不对源观测写入或补齐。

## 输出字段解释

| 字段 | 意义 |
|---|---|
| split | t 所在集合；研究范围之外为 null |
| anchor_observed | 当前输入是否有 t 的读数；与目标完整性分开 |
| future_complete、missing_future_times | 六个精确目标槽的完整性与缺槽列表（时间升序） |
| future_sum_mm | 六槽都存在时的诊断累计；若跨 split 仍可显示该诊断，但不能作为所属集合标签 |
| future_within_split、label_usable | 目标全部同 split；标签可用需同时完整 |
| label_exceeds_q | 可用标签时严格 >q，其他为 null |
| history_slots、history_complete、missing_history_times | 示例历史槽完整性；缺槽按 t 向过去的顺序 |
| example_evaluation_usable | 该示例同时满足目标与历史完整性，不能替代实际 B 特征可用规则 |

## A 已做和后续要做的检查

A 已用标准库 exact-time/Decimal oracle 与独立 Spark 时间 join 对两组查询逐字段核对，并对“未来六行”“包含 t”“>=q”“缺测补零”“跨 split 保留标签”“舍入/浮点金额”等故意错误输出确认失败。已知答案的边界测试另有运行记录。

B 回交时请提供：两组真实实现导出/检查结果、自己的全量标签与特征运行命令、各列的历史范围/可用规则、split 与转换拟合证据。C 需交回模型存储、独立加载及预测样例。A 再检查真实代码和 README 端到端执行；这些工作在当前尚未交付状态下没有被宣称已通过。
