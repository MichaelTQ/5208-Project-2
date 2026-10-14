"""Generate the analysis narrative and handoff evidence from verified tables."""
import csv
import json
from decimal import Decimal
from common import ROOT


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def main():
    p, tables = ROOT / "artifacts/stage3", ROOT / "artifacts/stage3/tables"
    verification = json.loads((p / "final_verification.json").read_text())
    assert verification["status"] == "PASS" and all(verification["checks"].values())
    complete = json.loads((p / "analysis_complete.json").read_text())
    probe = json.loads((p / "support_probe.json").read_text())
    annual = read_csv(tables / "station_matched_annual.csv")
    joint = read_csv(tables / "global_matched_annual.csv")
    network = read_csv(tables / "network_year.csv")
    fixed = read_csv(tables / "cohort_year.csv")
    station = sorted(read_csv(tables / "global_matched_pooled.csv"), key=lambda r:float(r["rainfall_rate_mm_per_5min"]), reverse=True)
    top, bottom = station[0], station[-1]
    meta = {r["station_id"]:r for r in read_csv(tables / "cohort_metadata.csv")}
    ranked = sorted(annual, key=lambda r:float(r["equal_station_rainfall_rate"]), reverse=True)
    high, low = ranked[0], ranked[-1]
    total_extreme = sum(Decimal(r["extreme_rainfall_sum_mm"]) for r in network)
    max_extreme_share = max(float(r["extreme_rainfall_sum_mm"])/float(r["rainfall_sum_mm"])*100 for r in network)
    primary_extreme_change = max(abs(float(r["equal_station_rainfall_rate"])-float(r["equal_station_rate_without_extremes"]))/float(r["equal_station_rainfall_rate"])*100 for r in annual)
    no_extreme_rank = [r["observation_year"] for r in sorted(annual, key=lambda r:float(r["equal_station_rate_without_extremes"]), reverse=True)]
    assert no_extreme_rank == [r["observation_year"] for r in ranked]
    support_counts = [r["count"] for r in probe["station_support"]]
    september = next(r["count"] for r in probe["global_months"] if r["month"] == "09") / probe["global_matched_slots"] * 100
    rows = ["# Stage 3：A 的跨年与站点分析", "", "## 已完成的工作", "",
            "使用 Stage 2 正式五分钟观测，完成全网络与站点年汇总、固定站点集合、八年匹配日历时间比较、局部共同支持下的站点差异、极端敏感性及六组 PNG/PDF 图表。全量处理采用 Spark；没有补零、估填完整全年雨量或修改正式数据。", "",
            "用户已明确 Lecture 5/6 不需要提供；这一决定已写入 PROJECT_DECISIONS.md 并更新原有文档，不构成任何前置条件。", "",
            "## 比较方法与分母", "",
            "湿观测定义为 rainfall_mm > 0。观测雨量平均值 = 有效读数之和 / 有效五分钟槽位；湿比例 = 正值槽位 / 有效槽位。观测累计只代表存在数据的时间段，不叫完整年降雨量。没有观测的站点年计数为零，雨量和比例为 null；已观测零降雨与缺失不同。", "",
            "全网络按记录汇总时，年份间的站点数量和时间支持会变化，因此只作背景诊断。固定站点集合依据八年日历覆盖率均至少 80% 且单一有效坐标组合选取，不依据雨量排序选择。门槛敏感性分别得到：70% 为 30 个、75% 为 26 个、80% 为 20 个、90% 与 95% 均为空。这个集合仅供描述性分析，不是 B/C 的训练站点推荐。", "",
            "年度主比较对每个站点取八年都出现的 MM-dd HH:mm 日历键，排除 2 月 29 日，再分别求年平均并对 20 个站点等权平均。全量年度汇总仍包含闰日。相同日历键是跨年相同月份、日期、时刻，不是同一次天气事件。站点之间可用的日历键不同；这个方法控制站内支持变化，不能保证站间支持相同。", "",
            f"每个站点的共同槽位为 {min(support_counts):,}–{max(support_counts):,}，相当于非闰年五分钟网格的 {min(support_counts)/105120:.1%}–{max(support_counts)/105120:.1%}。20 站合计每年 1,318,195 条，八年共 {complete['matched_rows']:,} 条。各站支持覆盖 10–12 个月，月份权重不均衡，故主比较仍是匹配样本上的描述，不是全年气候估计。", "",
            f"进一步取全部 20 站与八年共同支持，仅剩 {probe['global_matched_slots']:,} 个日历键（每站每年占全年网格 {probe['global_matched_slots']/105120:.2%}），分布于 2、5、7、9、10 月，其中 {september:.1f}% 位于 9 月。未满足预设“至少 1,000 槽位且覆盖 12 月”的年度主比较条件，因此只用于局部站点比较与选择敏感性说明。", "",
            "雨量总和保留 Decimal 精度；近似湿读数 p50/p90/p99 使用 Spark percentile_approx，accuracy=10000。它控制近似秩误差，不是雨量值的置信区间，也不是缺测不确定性的界限。依据：[Spark 3.5.8 官方文档](https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html)。", "",
            "## 主匹配比较结果", "", "| 年份 | 等站点平均 mm/有效五分钟槽位 | 等站点湿比例 | 每年观测数 |", "|---|---:|---:|---:|"]
    for r in annual:
        rows.append(f"| {r['observation_year']} | {float(r['equal_station_rainfall_rate']):.6f} | {float(r['equal_station_wet_rate'])*100:.3f}% | {int(r['observed_slots']):,} |")
    rows += ["", f"在这 20 站各自固定的八年共同时间支持上，{high['observation_year']} 年平均雨量最高（{float(high['equal_station_rainfall_rate']):.6f} mm/槽位），{low['observation_year']} 年最低（{float(low['equal_station_rainfall_rate']):.6f} mm/槽位）。这一句必须保留“在所选支持上”，不能改写成全国最湿/最干年或长期气候趋势。", "",
             "不同口径最高年份不同：全网络按记录为 2022；固定 20 站但时间未匹配为 2021；每站自身八年匹配支持为 2023；五个月联合支持为 2022。联合局部支持中 2023 反而最低，显示不能将局部季节样本推广为全年结论。2019 在前三种较宽时间口径中最低，但这种一致性也不是完整全国年雨量的证明。", "",
             "![不同观测支持下的年度比较](../artifacts/stage3/figures/02_annual_support_comparison.png)", "",
             "## 站点差异", "",
             f"站点之间采用相同的 3,976 个日历键 × 8 年，每站 {int(top['observed_slots']):,} 条真实观测。在这个限定五个月的样本中，{top['station_id']}（{meta[top['station_id']]['station_name']}）平均最高，{float(top['rainfall_rate_mm_per_5min']):.6f} mm/槽位；{bottom['station_id']}（{meta[bottom['station_id']]['station_name']}）最低，{float(bottom['rainfall_rate_mm_per_5min']):.6f} mm/槽位。该排序不代表全年或整个新加坡的空间雨量分布。", "",
             "较宽但站点时间支持不同的各站自身匹配样本中，S33 排第一；联合支持中第一为 S08。这种差异也说明跨站排名必须附支持范围。经纬度图只显示采样点，没有插值、行政区域汇总或面积权重。元信息单一坐标组合只是当前数据表现，不证明八年没有未记录的迁站。", "",
             "![同一局部支持下的站点差异](../artifacts/stage3/figures/04_station_differences.png)", "",
             "## 湿读数分布与极端敏感性", "",
             "全网络有效观测的湿比例约为 1.94%–3.20%；条件于正值的五分钟 p99 约为 6.6–7.2 mm。这是五分钟读数的分布，不能直接当成未来 30 分钟累计标签的比例或分位数。不同站点的读数精度及网络构成可能影响严格大于零的湿比例，报告明确保留这个定义。", "",
             f">=20 mm 候选在全网络为 10 条，审计金额合计 {total_extreme} mm；其年度观测雨量份额最高约 {max_extreme_share:.4f}%。敏感性统计排除这些观测时同时减少分母，不改成零。主匹配面板只包含其中 1 条（2021 年 S33 的 26.4 mm），排除后等站点平均值最大相对变化约 {primary_extreme_change:.4f}%，八年排序不变。这只说明当前统计排序对这组固定筛查读数不敏感，不能证明大值气象真实性。", "",
             "## 可以保留的结论与局限", "",
             "可以保留：所选支持上的年际平均雨量和湿比例不同；支持变化会改变最高年份和站点排序；缺测与站点构成必须进入比较解释。不能保留：把所有站点雨量相加当全国降雨、将观测累计除以覆盖率当真实全年总量、从八年数据宣称长期趋势或因果机制、用地图散点推出全国连续降雨面。", "",
             "时间端点、更新字段与历史修订沿用 Stage 2 的注明假设。日历归属按规范时间戳；午夜读数的累计区间可能跨上一日。这不影响本阶段明确的时间键定义，但不能无说明地换成气象日总量定义。非随机缺测、仪器校准和未记录站点变化没有被本阶段恢复。", "",
             "## 文件与图表", "",
             "- all_station_year.csv / network_year.csv：91×8 站点年和全网络背景统计。", "- cohort_station_year.csv / cohort_year.csv：固定 20 站、未匹配时间的诊断统计。", "- station_matched_year.csv / station_matched_annual.csv：年度主比较。", "- global_matched_year.csv / global_matched_pooled.csv：五个月局部联合支持的年度及站点统计。", "- station_support_counts.csv / station_support_months.csv / global_support_months.csv：支持量及其月份分布。", "",
             "图表目录 artifacts/stage3/figures/ 中提供六组 PNG 和矢量 PDF：01 覆盖率、02 年度不同支持比较、03 支持分布、04 站点差异、05 站点位置、06 条件湿读数分位数。图中的月分布是数据可用性，不替代 B 的年内天气分析。", "",
             "## 复现与验收", "",
             f"独立验收 PASS（{verification['checked_at_sgt']}）；{verification['boundary_tests']} 项人工数据边界测试通过。正式 Parquet 指纹未变；728 个站点年、8 个网络年度与 Stage 2 记录/雨量账本一致；两个匹配面板的支持与原始观测已独立重建核对；站点年湿分位数通过经验秩界限检查。具体证据见 artifacts/stage3/STAGE_3_CHECK_REPORT.md。", "",
             "完整运行顺序见 README 的 Stage 3 部分。B/C 不使用本阶段看过 2024 的描述性站点集合直接做训练选择；模型选择及转换拟合应仅由训练期落实。用户后续要求时再创建交接 ZIP。", ""]
    (ROOT / "docs/STAGE_3_ANALYSIS.md").write_text("\n".join(rows))
    findings = {"primary_highest_year": int(high["observation_year"]), "primary_lowest_year": int(low["observation_year"]), "joint_top_station": top["station_id"], "joint_bottom_station": bottom["station_id"], "global_september_share_pct": september, "max_network_extreme_rain_share_pct": max_extreme_share, "max_primary_rate_change_without_extremes_pct": primary_extreme_change, "primary_rank_unchanged_without_extremes": True}
    from stage2_common import write_json
    write_json(p / "findings.json", findings)
    # The following outputs are maintained from the same verified evidence.
    write_english_section(ROOT, complete, high, low, top, bottom, september, primary_extreme_change)
    write_check_report(p, verification, complete)
    print("Chinese analysis, English report section and Stage 3 check report written")


def write_english_section(root, complete, high, low, top, bottom, september, extreme_change):
    text = f"""# A: data preparation and cross-year/station comparisons

The project uses 2017–2024 five-minute station rainfall records from data.gov.sg. Apache Spark 3.5.8 performs full-data processing in the Asia/Singapore time zone. Raw values and source timestamps are retained; rainfall amounts use Decimal(20,9). Exact grid timestamps remain unchanged and records one second before a five-minute boundary move forward by one second. Compatible records are reduced to one deterministic representative; conflicting rainfall or coordinates are quarantined. No missing rainfall is imputed as zero. The final dataset contains 48,538,625 unique observations across 91 station IDs.

Availability varies substantially across stations and years. Calendar coverage uses 288 timestamp slots per day; observed-span coverage is reported separately. All stations have less than 90% calendar coverage in 2018, and the dataset has 63 entire days without valid observations across the study. These gaps prevent a direct interpretation of observed sums as complete annual rainfall.

A descriptive cohort of 20 stations was selected using at least 80% calendar coverage in every year and one recorded coordinate combination. Selection does not depend on rainfall ranking. For within-station annual comparisons, we retained month-day/time keys observed in all eight years, excluding February 29. Each station has 50,641–75,843 such keys; the combined panel contains {complete['matched_rows']:,} observations. Annual station means and wet fractions are averaged with equal station weights. This controls within-station calendar support, although support differs across stations and covers only 10–12 months with uneven weights.

On this matched support, {high['observation_year']} has the highest mean observed rainfall rate ({float(high['equal_station_rainfall_rate']):.6f} mm per five-minute slot) and {low['observation_year']} the lowest ({float(low['equal_station_rainfall_rate']):.6f}). Rankings are sensitive to the observation support: the unbalanced network peaks in 2022, the fixed cohort at its available times in 2021, and the station-specific matched panel in 2023. These results describe the selected observations; they do not establish complete national annual totals or a long-term climate trend.

Requiring identical calendar support across all 20 stations and eight years leaves only 3,976 keys in five months; {september:.1f}% are in September. This fails the pre-specified twelve-month condition for the primary annual comparison. On this restricted spatial panel, each station has 31,808 readings. {top['station_id']} has the highest observed mean rate and {bottom['station_id']} the lowest, but these rankings cannot be generalized to all seasons. The coordinate figure shows station points, without interpolation or area weighting.

Wet readings are defined strictly as rainfall greater than zero. Conditional wet p50/p90/p99 use Spark percentile_approx with accuracy 10,000; the reported approximation concerns rank, not missing-data uncertainty. Ten readings at least 20 mm per five minutes remain in the official cleaned data. Excluding these readings solely for sensitivity diagnostics reduces both sum and observation count. Only one occurs in the primary matched panel; the largest relative change in the equal-station mean is {extreme_change:.4f}%, and its year ranking is unchanged.

The interval-end interpretation (t−5 min, t] is an explicitly recorded research convention, not an officially confirmed timestamp definition. Historical revisions and update-field availability are unresolved, so results do not establish operational information availability. Independent checks verify source fingerprints, exact count/amount reconciliation, both matched panels against source observations, weighting and denominator rules, and station-year wet-quantile rank bounds. The descriptive cohort uses all years and must not be adopted automatically as a forecasting training selection.

Suggested figures: 01_calendar_coverage, 02_annual_support_comparison, 03_calendar_support_distribution, 04_station_differences, 05_station_locations, and 06_wet_reading_quantiles. Files and numerical evidence are in artifacts/stage3/. Intra-year weather analysis and model development belong to B and C.
"""
    (root / "docs/REPORT_SECTION_A.md").write_text(text)


def write_check_report(p, v, complete):
    rows = ["# Stage 3 执行与检查报告", "", f"**状态：PASS**。验收时间：{v['checked_at_sgt']}。", "",
            f"完成 728 个站点年、8 个全网络年度、20 站描述性 cohort、{v['matched_rows']:,} 条站内八年匹配观测与 {v['global_rows']:,} 条局部联合观测。生成 6 张 PNG 和 6 张矢量 PDF，中文分析与英文 A 报告文字。", "",
            "## 必需检查", "", "| 检查 | 结果 |", "|---|---|"]
    descriptions = {"all_station_years_and_network_reconcile_with_stage2": "728 个站点年及网络年度与 Stage 2 精确记录/雨量/湿槽位闭合", "observed_denominators_exact_amounts_and_extreme_sensitivity": "缺测分母、精确金额与排除极端时的分母变化", "coverage_only_cohort_and_threshold_sensitivity": "按覆盖率/坐标的站点集合及五个门槛", "eight_year_calendar_support_complete_unique_and_source_exact": "逐站八年支持独立重建、唯一性、非闰日及逐条来源一致", "global_calendar_support_and_panel_membership": "联合共同键与每键 20×8 面板完整性", "panel_amounts_rates_and_equal_station_weighting": "两个面板的数量/雨量及等站点加权独立重算", "restricted_global_support_not_used_as_full_year_primary": "五个月联合样本未用作全年主比较", "all_station_year_wet_quantiles_within_empirical_rank_bounds": "所有有湿观测站点年的 p50/p90/p99 经验秩界限", "stage2_parquet_unchanged": "96 个正式 Parquet 的大小与 SHA-256 未改变", "six_png_pdf_figures_and_their_source_fingerprints": "六组 PNG/PDF 可读取及绘图 CSV 指纹", "six_analytical_boundary_tests": "六项人工数据边界回归"}
    for key, value in v["checks"].items():
        rows.append(f"| {descriptions[key]} | {'PASS' if value else 'FAIL'} |")
    rows += ["", f"人工测试 {v['boundary_tests']} 项，全部通过，覆盖等站点/记录权重差异、缺测不补零、排除极端后的分母、九位小数、条件湿分位数和闰年不完整键。", "",
             "分位数校验采用 empirical count(<q) 与 count(<=q) 对目标秩的夹逼，容差为 N/10000 加一个离散秩位置。雨量总和以 Decimal 精确比较；展示比例/平均值只因 Spark 除法和浮点显示采用 1e-12 数值容差，不将其用于雨量守恒。", "",
             "六张图已由 Codex 逐张进行视觉检查，修正了位置图相邻标签与支持分布注释位置；图中的数据源和解释保存于 figure_manifest.json。月份图只报告可用性，不是季节雨量结论。这不表示组员已完成人工审核。", "",
             "## 核心限制", "", "年度主比较是 20 站自身固定时间支持上的等权平均，站间支持仍不同。全部站点共用键只有 3,976，集中五个月，70.7% 位于九月，故不作为全年主比较。不同支持下最高年份不同；只在明确样本上描述年度/站点差异，不宣称全国完整年雨量或气候趋势。", "",
             "Lecture 5/6 按用户要求不需要提供，相关项目文档已更新。时间端点及历史修订保持注明假设。正式数据未改；本阶段未生成 B 的特征/标签或模型，未创建 ZIP。", "",
             "## 交付", "", "执行步骤：docs/STAGE_3_PLAN.md。分析结果：docs/STAGE_3_ANALYSIS.md。英文 A 部分：docs/REPORT_SECTION_A.md。全表：artifacts/stage3/tables/。图表：artifacts/stage3/figures/。检查证据：final_verification.json、boundary_test_results.json、input_verification.json、figure_manifest.json、findings.json。", ""]
    (p / "STAGE_3_CHECK_REPORT.md").write_text("\n".join(rows))


if __name__ == "__main__":
    main()
