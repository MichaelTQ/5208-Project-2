"""Render the reviewed Stage 2 evidence as a concise Chinese report."""
import csv
import json
from collections import Counter

from common import ROOT


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def main():
    p = ROOT / "artifacts/stage2"
    verification = json.loads((p / "final_verification.json").read_text())
    assert verification["status"] == "PASS" and all(verification["checks"].values())
    tests = json.loads((p / "boundary_test_results.json").read_text())
    assert tests["passed"]
    accounting = json.loads((p / "clean_accounting.json").read_text())
    quality = json.loads((p / "quality_complete.json").read_text())
    sample = json.loads((p / "sample_summary.json").read_text())
    annual = read_csv(p / "coverage_annual_summary.csv")
    annual_detail = read_csv(p / "coverage_annual.csv")
    activity = read_csv(p / "daily_station_activity.csv")
    missing_days = Counter(r["observation_date"][:4] for r in activity if int(r["observed_slots"]) == 0)
    metadata = read_csv(p / "station_metadata_review.csv")
    max_2018 = max(float(r["calendar_coverage"]) for r in annual_detail if r["observation_year"] == "2018")
    stats = verification["clean_stats"]
    rows = [
        "# Stage 2 执行与检查报告", "",
        f"**验收状态：PASS**。独立验收时间：{verification['checked_at_sgt']}。负责人 A。", "",
        "## 核心结果", "",
        "| 项目 | 实际结果 |", "|---|---:|",
        f"| 原始记录 | {verification['raw_rows']:,} |",
        f"| 正式唯一观测 | {stats['rows']:,} |",
        f"| 无效隔离 | {verification['quarantined_invalid_rows']:,} |",
        f"| 冲突隔离 | {verification['quarantined_conflict_rows']:,} 条，2 个规范键 |",
        f"| 兼容冗余记录 | {accounting['redundant_rows_removed']:,} |",
        f"| 完全相同原始行去重 | {accounting['exact_duplicate_rows_removed']:,} |",
        "| 原始提前一秒记录 | 1,775,327，全部来自 2017 年 |",
        f"| 正式保留的 +1 秒记录 | {stats['shifted_clean_rows']:,} |",
        f"| 全期站点 ID | {quality['stations']} |",
        f"| 内部缺口 | {quality['gaps']['internal_gaps']:,} 段 |",
        f"| 内部缺失槽位 | {quality['gaps']['internal_missing_slots']:,} |",
        f"| 至少一小时缺失的长缺口 | {quality['gaps']['long_gaps_at_least_60_missing_minutes']:,} 段 |",
        f"| 跨年缺口 | {quality['gaps']['cross_year_gaps']} 段 |",
        f"| 全数据集完全无有效观测日 | {verification['days_without_any_valid_observation']} 天 |",
        f"| 极端雨量复核候选 | {stats['extreme_candidates']} 条，全部保留 |",
        f"| 正式真实样例 | {sample['rows']:,} 条 |",
        f"| 正式数据文件 | {verification['clean_parquet_files']} 个 Parquet，{verification['clean_parquet_bytes']:,} 字节（约 {verification['clean_parquet_bytes']/1e9:.2f} GB） |",
        "",
        f"正式规范时间范围：{stats['first_sgt']} 至 {stats['last_sgt']}。所有雨量为 Decimal(20,9)，原始字符串、来源年度、内容哈希和质量标记保留。", "",
        "记录账本严格闭合：48,538,713 = 0（无效）+ 4（冲突）+ 48,538,625（正式）+ 84（兼容冗余）。84 条冗余记录属于 84 个同雨量同位置、但原始时间不同的对齐碰撞组，不能称为完全相同的原始行。另 2 个碰撞组为位置冲突，雨量均为零。", "",
        f"十进制雨量账本严格闭合：{verification['raw_rainfall_sum_mm']} = 0（无效/冲突）+ {stats['rainfall_sum_mm']}（正式）+ {verification['removed_rainfall_sum_mm']}（去重）。原始值均可精确解析，无超出九位非零小数的值。**这些是跨站点、跨时间的审计金额，不能解释为全国累计降雨。**", "",
        "## 年度记录覆盖情况", "",
        "| 年份 | 有观测站点 | 日历覆盖率 ≥95% | 日历覆盖率 ≥90% | 正式记录 |", "|---|---:|---:|---:|---:|",
    ]
    for r in annual:
        rows.append(f"| {r['observation_year']} | {r['stations_with_observations']} | {r['stations_calendar_coverage_ge_95pct']} | {r['stations_calendar_coverage_ge_90pct']} | {int(r['observed_slots']):,} |")
    rows += ["", "每年按全期 91 个站点 ID 输出完整期间，缺记录的站点年仍在表中；“有观测站点”只表示该年出现过有效读数，不表示全年连续运行。日历分母每天 288，2020/2024 年各 105,408；另有 observed-span 分母。", "",
             f"2018 年所有站点的日历覆盖率均低于 90%，最高为 {max_2018:.2%}。全数据集完全无有效观测日按年为：" + "、".join(f"{year} 年 {count} 天" for year, count in sorted(missing_days.items())) + "。其中 2018 年 8 月 4–31 日连续 28 天没有有效观测。现有数据不足以确定这些空档的具体原因。年度雨量比较必须处理覆盖差异，不能把缺测补为零。", "",
             "站点元信息中 S113 出现两个坐标组合，最大间距约 38.59m；其 2017-04-25 11:20、11:25 两个规范时间键有位置冲突，各两条记录均被隔离。没有站点达到预设的 100m 位置变化复核阈值；这不表示确认所有站点长期未迁移。名称/设备/坐标组合的首次与末次出现不能当作连续运行期。", "",
             "## 验证证据", "",
             f"{tests['tests_run']} 项边界回归全部通过，失败 {tests['failures']}，错误 {tests['errors']}。覆盖：+1 秒和跨年对齐、兼容碰撞、重复数量和金额、雨量/位置冲突、负值/非有限值/错单位/错时区/CSV 错行、九位精度、极端值保留、缺测不补零、闰年和 span 分母、空隔离表可读取。", "",
             "独立检查重新计算八个原始 CSV 的 SHA-256，给正式 96 个 Parquet 记录行数、字节数和 SHA-256。读取全量正式表检查唯一键、网格、调整量、数值/单位、年份月份分区、区间字段、研究假设标志及研究范围；独立核对隔离条数、去重金额、雨量/记录守恒、极端记录去向。", "",
             "覆盖率逐日与正式观测计数一致；逐站首次至末次应有槽位减实际槽位，等于内部缺口槽位总数，也等于日覆盖率表的 span 缺失合计。月/年汇总、所有闰年与月份分母、2,922 个日历日的活动计数均通过检查。真实样例与正式表双向一致，原始内容哈希、数值和时间差另用 Python 独立核对。", "",
             "证据文件：final_verification.json、boundary_test_results.json、input_integrity.json、clean_file_manifest.json、clean_accounting.json、clean_schema.json。", "",
             "## 交付入口", "",
             "- 正式观测：data/clean/observations/。", "- 隔离原始记录：data/quarantine/invalid/、data/quarantine/conflicts/；可查看明细 conflicting_records.csv。", "- 覆盖率：coverage_daily/、coverage_monthly.csv、coverage_annual.csv、daily_station_activity.csv。", "- 缺口：gaps/（全部内部缺口）、long_gaps.csv、gap_summary.csv。", "- 站点：station_metadata_raw_variants.csv、station_metadata_clean_variants.csv、station_metadata_review.csv、station_observed_spans.csv。", "- 极端值：extreme_candidates.csv、extreme_temporal_context.csv（前后 15 分钟真实已观测邻域，没有补零）。", "- 真实样例：data/handoff/stage2_sample/、data/handoff/stage2_sample.csv。", "- 文档：docs/STAGE_2_PLAN.md、DATA_CONTRACT_V1.md、CLEANING_RULES.md、HANDOFF_B.md、STAGE_2_REPORT_METHODS.md。", "",
             "## 仍需后续核对的事项", "",
             "时间区间终点 `(t-5min,t]` 尚未得到官方明确确认，接口已标注为研究约定。更新字段首次可用性未确认，官方说明历史读数可能事后修订；本验收不能证明历史时点真实可用信息或线上效果。", "",
             "10 条大值均通过格式、单位和数值检查，最大为 S107 在 2022-08-21 20:20 的 37.2 mm/5min；时间邻域已输出，但未做仪器记录或专家气象核验。极端值筛查参数不是删除标准。", "",
             "按用户最新要求，Lecture 5/6 不需要提供，不列为待办或前置条件。环境继续由 A 固定，B/C 参照；用户将后续指示创建交接 ZIP。Stage 2 没有构造 B 的特征/标签或训练模型。后续可进入 A 的 Stage 3 跨年与站点分析。", "",
             "## 复现", "", "依次执行 README 中 Stage 2 的前五个命令，全部检查通过后运行报告生成命令：", "", "```bash", "bash scripts/run.sh scripts/write_stage2_report.py", "```", "", "输入或规则变化须移走旧检查点及完成标记，再重新处理。检查脚本启动时会清除旧 PASS 和旧报告，只有本轮通过后才能重建报告。", ""]
    (p / "STAGE_2_CHECK_REPORT.md").write_text("\n".join(rows))
    print("Stage 2 report written")


if __name__ == "__main__":
    main()
