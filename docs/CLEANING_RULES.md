# Stage 2 清洗规则、依据与研究约定

## 来源依据

数据来源为 [data.gov.sg 降雨集合 2279](https://data.gov.sg/collections/2279/view)，年度来源与文件指纹见 `data/data_manifest.csv`。官方 [历史 2017 年数据页](https://data.gov.sg/datasets/d_1990a5a1aeaf3dd243cf4dae294a61c4/view) 和 [实时降雨说明](https://data.gov.sg/datasets/d_6580738cdd7db79374ed3152159fbd69/view) 提供粒度和注意事项；实时说明指出读数按五分钟更新、可能出现技术性缺口，并可能事后修订。查阅日期：2026-10-10。

官方资料没有在本次查阅中明确历史 timestamp 的区间起止含义或更新字段的首次可用性。`interval_semantics` 因此明确为研究约定，不表示确认了实时信息可用性。

解析使用 Spark 3.5.8 的 [try_to_timestamp](https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_to_timestamp.html)；连续性使用按站点、时间排序的 [窗口函数](https://spark.apache.org/docs/3.5.8/sql-ref-syntax-qry-select-window.html)。处理参数冻结在 `config/stage2.json`。

## 时间和数值

接受精确到秒的 ISO `+08:00` 时间字符串。先解析为时间点，再检查五分钟网格：余数 0 保持，余数 299 向前 +1 秒，其余隔离。不得对所有时间向下取整。原始 timestamp、解析时间、调整量和内容哈希始终保留。

雨量为 Decimal(20,9)。负值、不能解析或非有限值、超过九位且多余位有非零数字、非 mm 单位、非目标读数类型全部隔离。多余位全是零可精确表示。CSV 错行、空站点、不支持的时间格式/偏移同样隔离。隔离字段 `invalid_reasons` 提供原因数组。

元信息错误作为质量标记，不能直接把真实雨量改成零。原始日期不用于覆盖观测时间。坐标使用全局有效经纬度范围检查，位置比较键为八位小数；原始坐标字符串不变。

## 唯一键与重复

候选数据按 station_id + observation_time 分组。完全相同原始 12 字段及来源年度使用相同 raw_hash。每组保存条数、不同原始内容数、调整过的原始条数和雨量金额。

如果同键雨量不同，或存在两个不同的有效位置比较键，则整组进入 `data/quarantine/conflicts/`，不相加或平均。某条缺失坐标和另一条有效坐标不构成已知的位置矛盾，但保留相应标记。

兼容组确定性选一条：优先未调整的整五分钟记录，然后原始 timestamp 字符串，再 raw_hash。其它字段变化只表示兼容原始变体，不自动解释成修订。该选择不追求“最新修订”，也不证明所选版本在预测时已存在。

## 标记

quality_flags 可能包括：timestamp_shifted_forward_one_second、invalid_coordinates、missing_station_name、update_time_unparsed、reading_update_time_unparsed、update_before_observation、reading_update_before_observation、source_date_mismatch、source_year_mismatch、extreme_rainfall_candidate、compatible_duplicate_group、compatible_raw_variant_group。标记描述被选代表记录及兼容组；其它原始变体在 parsed 中保留供审计。

大于等于 20 mm/5min 只作为预先固定的人工复核筛查阈值。全部有效大值保留，输出 extreme_candidates.csv 与前后 15 分钟 extreme_temporal_context.csv。这个阈值不表示物理上限，也不是从未来年度分布学习得到的删除规则。核查单位、数值和时间邻域不能代替仪器记录或气象专家核验。

名称、设备 ID、坐标变体按原始站点 ID 报告。有效坐标间最大距离大于等于 100m 只提示疑似位置变化；不可单凭此断言迁站。最早/最晚出现不是连续有效期，不同 ID 不因名称相似而合并。

## 缺测与统计守恒

不插值、不补零、不生成虚构观测。相邻有效时间差为 delta 秒时，内部缺失槽位为 delta/300 - 1。跨年度连续计算；长缺口定义为**缺失槽位持续至少 60 分钟**，不是相邻时间差达到 60 分钟。

日历覆盖率与 observed-span 覆盖率分别报告。完整日历分母包括未出现前/末次之后的未知状态；span 分母涵盖首次至末次之间的内部缺口。日历期间按网格时间归属，不能自动视作降雨区间归属。

记录账本：原始 = 无效隔离 + 冲突隔离 + 正式唯一记录 + 兼容冗余记录。雨量账本使用十进制精确求和：原始可精确解析总量 = 无效/冲突雨量 + 正式雨量 + 被去重记录的雨量。去重金额只用于审计，不能累加进气象总量。若出现不可解析或过精度原值，必须另报不可精确计量记录，不能把舍入后金额伪称原始精确总量。

## 已确认的资料范围与限制

用户已明确 Lecture 5/6 不需要提供，不再列为待办或前置条件。历史数据修订和更新时间语义仍按注明假设的方式说明。Stage 2 的 PASS 只代表约定下的数据工程检查通过。
