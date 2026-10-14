# A → B/C：正式观测接口 v1.0

## 读取入口

正式观测：`data/clean/observations/`（Parquet，按 observation_year / observation_month 分区）。真实小样例：`data/handoff/stage2_sample/`；对应可查看 CSV 为 `data/handoff/stage2_sample.csv`。Stage 1 的 `data/samples/parquet/` 是未清洗开发样本，不再作为正式标签输入。

共同环境由 A 固定：Python 3.11.14、Java 17、PySpark 3.5.8。必须设置 Spark session timeZone 为 `Asia/Singapore`。Stage 2 使用 local[4]、driver 4g、64 个 shuffle 分区。安装与启动入口见 ENVIRONMENT.md 和 README。Parquet 的 TimestampType 保存时间点，SGT 显示设置由读取方落实。

唯一键为 **station_id + observation_time**。每行是一个有效的五分钟累计降雨观测，单位 mm；正式表没有填补缺测记录。rainfall_mm 使用 **Decimal(20,9)**，保留源值九位以内的小数精度，不能先舍入再比较阈值。机器学习向量可另外转换 double，但精确累计和阈值边界建议保留 Decimal。

## 时间语义与限制

- timestamp 是完整保留的原始字符串；raw_observation_time 是其解析时间点。
- observation_time 是规范时间：整五分钟保持不变；恰好提前一秒的记录向前移动一秒；其它偏移隔离。time_adjust_seconds 只有 0 或 1。
- 为研究统一接口，暂约定每条读数属于 `(observation_time - 5 分钟, observation_time]`。interval_start_assumed、interval_end_assumed 和 interval_semantics 明确记录这一约定。
- **这个区间起止约定尚未得到官方明确确认**；数据质量验收不会将它升级为官方事实。保持注明假设的接口即可继续；若以后出现明确来源证据，再统一修订及验收。用户已明确 Lecture 5/6 不需要提供。
- parsed_update_time / parsed_reading_update_time 只用于追溯和质量检查。它们是否代表首次上报、后续修订或批次更新未确认，不能当成预测时已经可知的特征。
- 官方说明历史读数可能事后修订。因此现有历史数据可以做注明假设的历史实验，不能直接证明真实线上效果或当时实际可用的信息。

## 字段字典

完整机器可读 Spark schema：`artifacts/stage2/clean_schema.json`。

| 字段 | 类型 | 含义 |
|---|---|---|
| station_id | string | 原始站点 ID；不按相同名称合并 |
| observation_time | timestamp | SGT 约定下的规范五分钟网格时间；唯一键的一部分 |
| rainfall_mm | decimal(20,9) | 五分钟累计雨量；非负、有效、单位 mm |
| timestamp / date | string | 原始观测时间 / 原始日期，完整保留 |
| reading_value / reading_unit / reading_type | string | 原始雨量值 / 单位 / 读数类型；有效类型为 TB1 Rainfall 5 Minute Total F |
| station_name / station_device_id | string | 原始站名与设备 ID；变化不等于可确认的迁站或换设备事件 |
| location_longitude / location_latitude | string | 原始经纬度字符串 |
| longitude / latitude | double | 解析后的经纬度；无效坐标会标记，当前数据无该问题 |
| location_key | string | 有效坐标保留八位小数形成的位置比较键；不改变原始坐标 |
| update_timestamp / reading_update_timestamp | string | 原始更新字段；语义与首次可用性未确认 |
| raw_observation_time | timestamp | 未对齐的原始观测时间点 |
| parsed_update_time / parsed_reading_update_time | timestamp | 原始更新字段的解析值 |
| update_delay_seconds / reading_update_delay_seconds | long | 各更新字段减原始观测时间；不是经过确认的实时延迟指标 |
| source_year | int | 输入文件所属年度；与规范分区年份含义不同 |
| raw_hash | string | 原始 12 字段及 source_year 的 SHA-256；不同于 CSV 文件 SHA-256 |
| grid_remainder_seconds | long | 原始时间点对 300 秒取模；正式记录仅 0 或 299 |
| time_adjust_seconds | int | 原始到规范时间的调整秒数，0 或 +1 |
| source_record_count | long | 同站同规范时间有效候选组的原始记录条数 |
| distinct_raw_records | long | 该组不同 raw_hash 数 |
| adjusted_source_records | long | 该组发生 +1 秒调整的原始记录数 |
| exact_duplicate_rows_removed | long | 组条数减不同原始内容数 |
| compatible_variant_rows_removed | long | 不同原始内容数减一；仅用于兼容组 |
| quality_flags | array<string> | 质量标记；取值说明见 CLEANING_RULES.md |
| interval_start_assumed / interval_end_assumed | timestamp | 研究约定的五分钟区间边界 |
| interval_semantics | string | research_convention_end_timestamp_not_officially_confirmed |
| observation_year / observation_month | int | 规范 SGT 时间的年份 / 月份，同时为分区键 |
| observation_date | date | 规范 SGT 观测日期 |

## 读取示例

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[4]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.session.timeZone", "Asia/Singapore")
         .getOrCreate())
observations = spark.read.parquet("data/clean/observations")
observations.select("station_id", "observation_time", "rainfall_mm", "quality_flags").show()
```

## 覆盖率与站点资料

- 日覆盖率 Parquet：`artifacts/stage2/coverage_daily/`；月/年汇总 CSV：`coverage_monthly.csv`、`coverage_annual.csv`。
- 日历分母使用 SGT 半开区间 `[期间起点, 下一期间起点)` 的网格时间，每天 288，2020/2024 年各 105,408。这里计算的是记录覆盖率；区间终点约定使午夜读数的雨量区间跨越上一日。后续气象总量按哪一日归属须单独说明，不能混淆两种口径。
- span_expected_slots 只计算首次至末次有效观测（包含两端）的网格槽位。span_coverage = observed_slots / span_expected_slots；分母为零时是 null。
- 首次出现之前和末次出现之后记为 outside_observed_span_unknown_slots；并不等于已确认未投运/停运。
- observed_slots=0 表示没有有效观测；对应 rainfall_sum_mm 为 null，不能解释为零降雨。wet_slots=0 是观测到的有雨记录数，不证明缺失期间无雨。
- 全部站点与全部期间均输出，包括没有观测的站点年。原始/清洗元信息组合的最早/最晚出现不证明中间持续使用同一配置。
- 八年覆盖率和位置变化汇总仅用于质量与分析。B/C 不应依据未来测试年度质量或气候结果自动选择训练样本、特征或调参。
