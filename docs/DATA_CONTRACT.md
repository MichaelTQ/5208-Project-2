# A → B/C：Stage 1 样本接口 v0

Stage 2 已完成。当前正式输入以 `docs/DATA_CONTRACT_V1.md` 为准；本文件保留 Stage 1 开发接口及当时未解决事项，不能替代 v1.0。

本文件是开发接口草案。样本没有对齐、去重或填补，不是正式清洗数据。未来 30 分钟标签和历史特征不能以本阶段的未确认时间假设作为最终依据。

## 来源与粒度

每条原始记录是一个站点的一次五分钟累计降雨读数。单位 mm，样本 reading_type 为 TB1 Rainfall 5 Minute Total F。官方说明时间为 SGT/ISO 8601；时间戳代表区间起点还是终点尚待查明。

原始 CSV 位于 data/raw；连续样本位于 data/samples/raw；Spark 解析后的开发用 Parquet 位于 data/samples/parquet。样本按四个候选站点 ID 选择每年首个观测日、末个观测日、首个有雨日的全部记录。某个候选 ID 在这些日期没有记录时，保留其缺失，并在检查报告列出当年样本实际站点。重叠日期只选择一次；原始数据中的重复行仍然保留。候选 ID 不代表八年稳定运行的站点，站点运行历史需在 Stage 2 全量核查。

首末日保留跨年边界的真实观测和缺口。该样本只用于接口与读写验证，不能用于评估模型、估计气候分布或推断全量质量。

## 字段字典

| 字段 | Stage 1 类型 | 含义/约束 |
|---|---|---|
| date | string | 原始日期，完整保留；不能代替 observation_time |
| timestamp | string | 原始观测时间及 +08:00 偏移，完整保留 |
| update_timestamp | string | 原始更新字段；具体语义、实时可用性未确认 |
| station_id | string | 原始站点 ID，不根据名称擅自合并 ID |
| station_name | string | 原始名称，可能跨期变化 |
| station_device_id | string | 原始设备 ID |
| location_longitude | string | 原始经度字符串 |
| location_latitude | string | 原始纬度字符串 |
| reading_update_timestamp | string | 原始读数更新字段；不直接假定为首次上报时间 |
| reading_value | string | 原始数值字符串；保留原精度 |
| reading_type | string | 原始读数类型 |
| reading_unit | string | 原始单位，当前样本为 mm |
| source_year | integer | 来源年度文件，作为 Parquet 分区字段 |
| observation_time | Spark TimestampType | 带时区字符串解析到时间点；session timeZone 必须为 Asia/Singapore |
| parsed_update_time | Spark TimestampType | update_timestamp 解析值，不用于替代观测时间 |
| parsed_reading_update_time | Spark TimestampType | reading_update_timestamp 解析值 |
| rainfall_mm | double | reading_value 解析值；未四舍五入、未填零 |
| longitude / latitude | double | 解析后的经纬度 |

Spark TimestampType 内部保存时间点，显示时受 session timeZone 影响；Parquet 不携带可替代时区约定的显示设置，B/C 必须显式设置 Asia/Singapore。原始 timestamp 字符串用于追溯。

## 读取示例

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "Asia/Singapore")
sample = spark.read.parquet("data/samples/parquet")
sample.select("station_id", "timestamp", "observation_time", "rainfall_mm").show()
```

## Stage 2 需要确认的接口事项

- timestamp 对五分钟区间的起止含义；2017 年秒数偏移的标准化规则。
- 完全重复和同站同时间冲突的处理规则；对齐后是否新增冲突。
- 更新字段与历史回测的信息可用性；历史修订不能自动当成 t 时已经可知。
- 正式质量标记、有效记录条件、站点位置历史及覆盖率分母。
- 正式观测表的唯一键目标：station_id + 经确认的标准化 observation_time；原始样本不承诺唯一。
- 特征、标签、阈值扩展、训练/验证/测试切分由 B/C 负责。

建议时间划分仍为训练 2017–2022、验证 2023、测试 2024，须由建模模块落实并排除跨集合的未来标签窗口。首版清洗接口冻结后再向 B 交接正式数据。
