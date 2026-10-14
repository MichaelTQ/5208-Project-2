"""Independent report/rank checks using direct comparison and statistics.correlation."""
import csv
import json
import re
import statistics
from decimal import Decimal

from common import ROOT, now_sgt, save_json, sha256_file


def load(path):
    return json.loads((ROOT / path).read_text())


def table(path):
    with (ROOT / path).open(newline="") as handle:
        return list(csv.DictReader(handle))


def main():
    out = ROOT / "artifacts/stage5"
    for name in ["final_verification.json", "STAGE_5_CHECK_REPORT.md"]:
        (out / name).unlink(missing_ok=True)
    checks = {}
    for r in load("artifacts/stage5/input_manifest.json")["files"]:
        assert sha256_file(ROOT / r["path"]) == r["sha256"], r["path"]
    checks["report_input_fingerprints_current"] = True
    for r in load("artifacts/stage5/report_complete.json")["outputs"]:
        assert sha256_file(ROOT / r["path"]) == r["sha256"]
    checks["report_output_fingerprints_current"] = True
    facts = load("artifacts/stage5/report_facts.json")
    s1 = load("artifacts/stage1/final_verification.json")
    s2 = load("artifacts/stage2/final_verification.json")
    assert facts["raw_rows"] == s1["total_raw_rows"] == facts["clean_rows"] + facts["compatible_rows_removed"] + facts["conflict_rows"] + facts["invalid_rows"]
    assert facts["raw_bytes"] == s1["total_raw_bytes"] and facts["clean_bytes"] == s2["clean_parquet_bytes"]
    assert Decimal(facts["raw_audit_amount_mm"]) == Decimal(facts["clean_audit_amount_mm"]) + Decimal(facts["removed_audit_amount_mm"])
    assert facts["clean_adjusted_rows"] == s2["clean_stats"]["shifted_clean_rows"]
    assert facts["raw_adjusted_rows"] == sum(r["one_second_offset_rows"] for r in load("artifacts/stage2/ingest_complete.json")["profiles"])
    assert facts["days_without_any_valid_observation"] == s2["days_without_any_valid_observation"]
    assert facts["gaps"] == load("artifacts/stage2/gap_totals.json")
    checks["source_cleaning_exact_ledgers_and_quality_claims"] = True
    annual = table("artifacts/stage3/tables/station_matched_annual.csv")
    assert facts["annual_primary_values"] == annual
    report = (ROOT / "docs/REPORT_A_FULL.md").read_text()
    for r in annual:
        line = f"| {r['observation_year']} | {Decimal(r['equal_station_rainfall_rate']):.6f} | {Decimal(r['equal_station_wet_rate'])*100:.3f}% | {int(r['observed_slots']):,} |"
        assert line in report
    checks["every_report_annual_row_matches_verified_table"] = True
    original = table("artifacts/stage3/tables/global_matched_year.csv")
    source = {(int(r["observation_year"]), r["station_id"]): Decimal(r["rainfall_sum_mm"]) for r in original}
    assert len(source) == 160
    expected_ranks = {}
    for (y, s), amount in source.items():
        others = [v for (yy, ss), v in source.items() if yy == y]
        # Descending average rank from counts, independently of the sorted grouping.
        expected_ranks[(y, s)] = sum(v > amount for v in others) + (sum(v == amount for v in others) + 1) / 2
    rank_rows = table("artifacts/stage5/station_rank_by_year.csv")
    assert len(rank_rows) == 160
    for r in rank_rows:
        key = int(r["observation_year"]), r["station_id"]
        assert float(r["rank_desc_average_ties"]) == expected_ranks[key]
        assert Decimal(r["rainfall_sum_mm"]) == source[key]
        assert Decimal(r["rainfall_rate_mm_per_5min"]) == source[key] / Decimal(3976)
    checks["all_160_ranks_independently_recomputed_with_ties"] = True
    ids = sorted({s for y, s in source})
    summaries = table("artifacts/stage5/station_rank_summary.csv")
    assert len(summaries) == 20
    for r in summaries:
        v = [expected_ranks[(y, r["station_id"])] for y in range(2017, 2025)]
        assert float(r["min_rank"]) == min(v) and float(r["max_rank"]) == max(v)
        assert float(r["mean_rank"]) == statistics.mean(v)
        assert int(r["years_at_rank_one"]) == sum(x == 1 for x in v)
    correlations = table("artifacts/stage5/rank_correlations.csv")
    assert len(correlations) == 28
    for r in correlations:
        a = [expected_ranks[(int(r["year_1"]), s)] for s in ids]
        b = [expected_ranks[(int(r["year_2"]), s)] for s in ids]
        assert abs(float(r["spearman_rank_correlation"]) - statistics.correlation(a, b)) < 1e-12
    assert abs(facts["rank_correlation_range"][0] - min(float(r["spearman_rank_correlation"]) for r in correlations)) < 1e-12
    assert abs(facts["rank_correlation_range"][1] - max(float(r["spearman_rank_correlation"]) for r in correlations)) < 1e-12
    checks["rank_ranges_and_28_correlations_independently_verified"] = True
    timing = facts["timings"]
    assert timing["clean_job_seconds"] == load("artifacts/stage2/clean_complete.json")["elapsed_seconds"]
    assert timing["quality_job_seconds"] == load("artifacts/stage2/quality_complete.json")["elapsed_seconds"]
    assert timing["analysis_job_seconds"] == load("artifacts/stage3/analysis_complete.json")["elapsed_seconds"]
    assert timing["annual_parse_job_seconds"] == sum(r["elapsed_seconds"] for r in load("artifacts/stage2/ingest_complete.json")["profiles"])
    checks["job_timings_trace_to_actual_records_not_end_to_end"] = True
    captions = table("artifacts/stage5/figure_captions.csv")
    assert len(captions) == 6 and report.count("![Figure A") == 6
    for r in captions:
        assert (ROOT / r["png"]).is_file() and (ROOT / r["pdf"]).is_file()
        assert r["caption_en"] in report
    for doc in ["docs/REPORT_A_FULL.md", "docs/REPORT_A_MERGE_GUIDE.md"]:
        for link in re.findall(r"\]\(([^)]+)\)", (ROOT / doc).read_text()):
            if "://" not in link:
                assert ((ROOT / doc).parent / link.split("#")[0]).is_file(), (doc, link)
    for r in table("artifacts/stage5/claim_evidence.csv"):
        assert all((ROOT / p.strip()).is_file() for p in r["source"].split(";"))
    checks["six_figures_captions_and_local_evidence_links_resolve"] = True
    assert len(re.findall(r"^## [1-6]\. ", report, re.M)) == 6
    for phrase in ["five months", "research convention", "not publisher-issued", "not rainfall over Singapore", "training", "not been supplied or tested", "not claims of human"]:
        assert phrase in report, phrase
    checks["scope_support_assumptions_and_AI_statement_explicit"] = True
    result = {"status": "PASS", "checked_at_sgt": now_sgt(), "checks": checks, "rank_rows": 160, "rank_correlations": 28, "figures": 6, "report_words": len(report.split()), "external_BC_status": "NOT_DELIVERED"}
    save_json(out / "final_verification.json", result)
    lines = ["# Stage 5 检查报告", "", f"结果：**PASS**。检查时间：{result['checked_at_sgt']}。", "", f"{len(checks)} 项检查通过。英文合并稿六部分、六张既有图及图注；160 行逐年站点排名、20 行范围表、28 对秩相关均独立重算。", "", "正文数值对应已验收源表；逐项耗时对应真实生产记录，不作为项目总耗时。正式观测和既有图表未修改。", ""]
    lines += [f"- PASS：{name}" for name in checks]
    lines += ["", "完整正文：docs/REPORT_A_FULL.md；B 合并说明：docs/REPORT_A_MERGE_GUIDE.md。B/C 年内分析、标签、模型及最终 PDF 仍由他们完成。本报告没有虚构其结果；Lecture 5/6 不需要提供。", ""]
    (out / "STAGE_5_CHECK_REPORT.md").write_text("\n".join(lines))
    print(f"Stage 5 PASS: {len(checks)} report/rank checks, six sections and six figures")


if __name__ == "__main__":
    main()
