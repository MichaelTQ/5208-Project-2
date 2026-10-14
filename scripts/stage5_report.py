"""Assemble A's report from accepted evidence and small aggregate tables."""
import csv
import json
import math
from collections import defaultdict
from decimal import Decimal

from common import ROOT, config, now_sgt, save_json, sha256_file


def read_json(path):
    return json.loads((ROOT / path).read_text())


def rows(path):
    with (ROOT / path).open(newline="") as handle:
        return list(csv.DictReader(handle))


def export(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(data[0]))
        writer.writeheader()
        writer.writerows(data)


def average_ranks(values):
    ordered = sorted(values, key=lambda s: (-values[s], s))
    result, i = {}, 0
    while i < len(ordered):
        j = i + 1
        while j < len(ordered) and values[ordered[j]] == values[ordered[i]]:
            j += 1
        for s in ordered[i:j]:
            result[s] = (i + 1 + j) / 2
        i = j
    return result


def main():
    out = ROOT / "artifacts/stage5"
    out.mkdir(parents=True, exist_ok=True)
    (out / "final_verification.json").unlink(missing_ok=True)
    (out / "STAGE_5_CHECK_REPORT.md").unlink(missing_ok=True)
    inputs = ["artifacts/stage1/final_verification.json", "artifacts/stage2/final_verification.json", "artifacts/stage2/clean_accounting.json", "artifacts/stage2/gap_totals.json", "artifacts/stage2/ingest_complete.json", "artifacts/stage2/clean_complete.json", "artifacts/stage2/quality_complete.json", "artifacts/stage3/final_verification.json", "artifacts/stage3/analysis_complete.json", "artifacts/stage3/findings.json", "artifacts/stage3/figure_manifest.json", "artifacts/stage3/tables/station_matched_annual.csv", "artifacts/stage3/tables/global_matched_year.csv", "artifacts/stage3/tables/global_matched_pooled.csv", "artifacts/stage3/tables/cohort_metadata.csv", "artifacts/stage3/tables/station_support_counts.csv", "artifacts/environment/environment.json", "config/project.json", "config/stage2.json", "config/stage3.json"]
    save_json(out / "input_manifest.json", {"created_at_sgt": now_sgt(), "files": [{"path": p, "sha256": sha256_file(ROOT / p)} for p in inputs]})
    s1, s2, s3 = [read_json(f"artifacts/stage{s}/final_verification.json") for s in [1, 2, 3]]
    assert all(s["status"] == "PASS" for s in [s1, s2, s3])
    accounting = read_json("artifacts/stage2/clean_accounting.json")
    gaps = read_json("artifacts/stage2/gap_totals.json")
    env = read_json("artifacts/environment/environment.json")
    findings = read_json("artifacts/stage3/findings.json")
    annual = rows("artifacts/stage3/tables/station_matched_annual.csv")
    global_year = rows("artifacts/stage3/tables/global_matched_year.csv")
    pooled = rows("artifacts/stage3/tables/global_matched_pooled.csv")
    metadata = {r["station_id"]: r for r in rows("artifacts/stage3/tables/cohort_metadata.csv")}
    by_year = defaultdict(dict)
    for r in global_year:
        assert int(r["observed_slots"]) == 3976
        by_year[int(r["observation_year"])][r["station_id"]] = Decimal(r["rainfall_sum_mm"])
    ranks = {y: average_ranks(v) for y, v in by_year.items()}
    ids = sorted(next(iter(ranks.values())))
    assert len(ids) == 20 and set(by_year) == set(range(2017, 2025))
    long = [{"station_id": s, "observation_year": y, "rainfall_sum_mm": str(by_year[y][s]), "rainfall_rate_mm_per_5min": str(by_year[y][s] / Decimal(3976)), "rank_desc_average_ties": ranks[y][s], "observed_slots": 3976, "support": "joint_calendar_keys_five_months"} for y in sorted(ranks) for s in ids]
    export(out / "station_rank_by_year.csv", long)
    summaries = [{"station_id": s, "station_name": metadata[s]["station_name"], "min_rank": min(ranks[y][s] for y in ranks), "max_rank": max(ranks[y][s] for y in ranks), "mean_rank": sum(ranks[y][s] for y in ranks) / 8, "years_at_rank_one": sum(ranks[y][s] == 1 for y in ranks), "support": "joint_calendar_keys_five_months"} for s in ids]
    export(out / "station_rank_summary.csv", summaries)
    correlations = []
    years = sorted(ranks)
    for i, y1 in enumerate(years):
        for y2 in years[i + 1:]:
            x, y = [ranks[y1][s] for s in ids], [ranks[y2][s] for s in ids]
            mx, my = sum(x) / 20, sum(y) / 20
            numerator = sum((a - mx) * (b - my) for a, b in zip(x, y))
            denominator = math.sqrt(sum((a - mx)**2 for a in x) * sum((b - my)**2 for b in y))
            correlations.append({"year_1": y1, "year_2": y2, "stations": 20, "spearman_rank_correlation": numerator / denominator, "support": "joint_calendar_keys_five_months"})
    export(out / "rank_correlations.csv", correlations)
    winners = {str(y): sorted(s for s in ids if ranks[y][s] == min(ranks[y].values())) for y in years}
    r_min, r_max = min(r["spearman_rank_correlation"] for r in correlations), max(r["spearman_rank_correlation"] for r in correlations)
    s08 = next(r for r in summaries if r["station_id"] == "S08")
    durations = {"annual_parse_job_seconds": sum(p["elapsed_seconds"] for p in read_json("artifacts/stage2/ingest_complete.json")["profiles"]), "clean_job_seconds": read_json("artifacts/stage2/clean_complete.json")["elapsed_seconds"], "quality_job_seconds": read_json("artifacts/stage2/quality_complete.json")["elapsed_seconds"], "analysis_job_seconds": read_json("artifacts/stage3/analysis_complete.json")["elapsed_seconds"]}
    facts = {"raw_rows": s1["total_raw_rows"], "raw_bytes": s1["total_raw_bytes"], "clean_rows": s2["clean_stats"]["rows"], "clean_files": s2["clean_parquet_files"], "clean_bytes": s2["clean_parquet_bytes"], "stations": 91, "compatible_rows_removed": accounting["compatible_variant_rows_removed"], "conflict_rows": accounting["conflicting_rows"], "invalid_rows": accounting["invalid_rows"], "raw_adjusted_rows": sum(p["one_second_offset_rows"] for p in read_json("artifacts/stage2/ingest_complete.json")["profiles"]), "clean_adjusted_rows": s2["clean_stats"]["shifted_clean_rows"], "raw_audit_amount_mm": accounting["parsed_rainfall_sum_mm"], "clean_audit_amount_mm": accounting["clean_rainfall_sum_mm"], "removed_audit_amount_mm": accounting["redundant_rainfall_sum_mm"], "days_without_any_valid_observation": s2["days_without_any_valid_observation"], "gaps": gaps, "annual_primary_values": annual, "findings": findings, "station_rank_first_by_year": winners, "rank_correlation_range": [r_min, r_max], "S08_rank_range": [s08["min_rank"], s08["max_rank"]], "timings": durations}
    save_json(out / "report_facts.json", facts)
    captions = [
        ("01_calendar_coverage", "Calendar coverage of 91 station IDs in 2017–2024. Denominators are 105,120 five-minute timestamp slots per non-leap year and 105,408 in 2020/2024. Zero observed coverage means no valid observations, not zero rainfall."),
        ("02_annual_support_comparison", "Mean observed rainfall per valid five-minute slot and wet fraction under changing network, fixed cohort, station-specific matched calendar and restricted joint support. Primary station-specific comparison uses 20 equal-weighted stations and 1,318,195 observations per year. Joint support covers five months only; neither panel estimates complete national annual rainfall."),
        ("03_calendar_support_distribution", "Availability of eight-year shared calendar positions by month. Each station has its own shared positions; the joint panel has 3,976 positions in February, May, July, September and October, with 70.7% in September. This figure describes observation support, not a monthly rainfall climatology."),
        ("04_station_differences", "Station mean rainfall and wet fraction on the same 3,976 calendar positions in each of eight years: 31,808 readings per station. S08 and S24 are the highest and lowest pooled mean rates on this restricted five-month support; ranking is not a full-year spatial result."),
        ("05_station_locations", "Longitude/latitude of the descriptive cohort, coloured by observed mean rainfall on restricted joint support. Markers are station measurements; no interpolation or spatial area weighting is applied."),
        ("06_wet_reading_quantiles", "Network conditional wet-reading p50/p90/p99 by year, using strictly positive five-minute amounts and percentile_approx with accuracy 10,000. Approximation concerns rank; these are not thirty-minute future-label quantiles and not estimates of missing-data uncertainty.")]
    export(out / "figure_captions.csv", [{"figure_id": name, "png": f"artifacts/stage3/figures/{name}.png", "pdf": f"artifacts/stage3/figures/{name}.pdf", "caption_en": caption} for name, caption in captions])
    annual_lines = ["| Year | Equal-station mean mm / observed 5-min slot | Wet fraction | Observations |", "|---|---:|---:|---:|"]
    annual_lines += [f"| {r['observation_year']} | {Decimal(r['equal_station_rainfall_rate']):.6f} | {Decimal(r['equal_station_wet_rate'])*100:.3f}% | {int(r['observed_slots']):,} |" for r in annual]
    top = max(pooled, key=lambda r: Decimal(r["rainfall_sum_mm"]))
    bottom = min(pooled, key=lambda r: Decimal(r["rainfall_sum_mm"]))
    sources = "\n".join(f"- [{year} annual source](https://data.gov.sg/datasets/{dataset}/view)" for year, dataset in config()["datasets"].items())
    text = f"""# A report contribution: data, quality and cross-year/station analysis

## 1. Data source and dataset overview

We use the 2017–2024 annual files from NEA's Historical Rainfall across Singapore collection on data.gov.sg. The publisher specifies ISO 8601 timestamps in Singapore Time and cautions that records can be missing and do not receive the quality-control procedures used for official climate records. Our analysis therefore describes this historical dataset rather than replacing official climate statistics. [NEA collection and disclaimer](https://data.gov.sg/collections/2279/view)

The frozen local snapshot comprises {facts['raw_rows']:,} CSV records ({facts['raw_bytes']:,} bytes), with twelve source fields covering observation/update timestamps, station identifiers and names, coordinates, rainfall amount, type and unit. Each annual file has a dataset ID, download record, actual row count, size and locally computed SHA-256. Local digests support reproducibility; they are not publisher-issued CSV hashes. The actual downloaded export sizes differ from the catalogue's displayed sizes, so we audit downloaded bytes and logical records separately. The source links are listed at the end of this contribution.

After cleaning, the formal data contain {facts['clean_rows']:,} unique station/time observations across 91 recorded station IDs, in {facts['clean_files']} Parquet files partitioned by SGT observation year and month ({facts['clean_bytes']:,} bytes). The unique key is station_id plus observation_time. Original strings, original timestamps, source year, adjustment amount and source-record digest remain available for tracing decisions. Field definitions are provided in [the formal v1.0 contract](DATA_CONTRACT_V1.md).

## 2. Computing environment and reproducibility

Full-data parsing, cleaning, joins, coverage calculations and statistical aggregation use Apache Spark/PySpark 3.5.8. The local machine runs macOS 26.1 ARM64 with ten physical/logical CPU cores and 16 GiB RAM. Python is 3.11.14; the tested JVM is Eclipse Temurin 17.0.20.1+1. Spark runs as local[4] with a verified 4 GiB JVM maximum heap, Asia/Singapore session time zone, and adaptive query execution enabled. Stage 1 uses 16 shuffle partitions; full-data Stage 2/3 operations use 64. There are no remote worker nodes. Python dependencies are pinned; the relevant packages include NumPy 1.26.4, pandas 2.2.3, PyArrow 17.0.0 and Matplotlib 3.9.2.

Only bounded samples and small aggregate tables are read into Python memory for independent checking or plotting. No full raw or cleaned observation table is loaded into pandas. Formal rainfall amounts use Decimal(20,9); exact sums and boundary comparisons are maintained before any conversion required for plots or model vectors.

Recorded production timings on this machine are {durations['annual_parse_job_seconds']:.2f} seconds summed across the eight annual parse jobs, {durations['clean_job_seconds']:.2f} seconds for cleaning, {durations['quality_job_seconds']:.2f} seconds for quality outputs, and {durations['analysis_job_seconds']:.2f} seconds for the Stage 3 analysis job. These measure different jobs, include their recorded execution conditions, and exclude some input hashing, downloading, support probing, plotting and independent checks. They must not be combined into an end-to-end runtime or compared directly with model-training time. Installation and the ordered raw-to-figure workflow are in [README](../README.md) and [the run guide](RUN_GUIDE.md). The environment was tested on this Mac; portability checks relocate the sample on the same machine and do not establish successful installation on B/C's computers.

## 3. Data cleaning and quality assessment

Time normalization preserves exact five-minute timestamps. Records exactly one second before a boundary move forward by one second, with the original time and adjustment retained; other offsets are subject to quarantine. All {facts['raw_adjusted_rows']:,} such source offsets occur in 2017, and {facts['clean_adjusted_rows']:,} adjusted canonical rows remain. This rule is based on the observed timestamp pattern, not indiscriminate rounding.

At each normalized station/time key, compatible source variants are reduced to one deterministic representative. The data have no completely identical duplicates under the retained source-field definition. We remove {facts['compatible_rows_removed']} compatible redundant rows and quarantine {facts['conflict_rows']} rows in two coordinate-conflict keys for S113; the coordinates differ by about 38.59 metres, while both readings are zero. The observed invalid-record quarantine has {facts['invalid_rows']} rows. No missing timestamps are generated, interpolated or set to zero, and large rain readings are retained with diagnostic flags.

The record ledger is {facts['raw_rows']:,} = {facts['clean_rows']:,} + 84 redundant + 4 quarantined records. The exact cross-station audit amount is {facts['raw_audit_amount_mm']} = {facts['clean_audit_amount_mm']} + {facts['removed_audit_amount_mm']} + 0 quarantined mm. This accounting sum tracks record transformations across gauges; it is not rainfall over Singapore and has no area-average interpretation.

Calendar coverage uses 288 timestamp slots per day, 105,120 per non-leap year and 105,408 in 2020/2024. Observed-span coverage is separately defined between a station's first and last available observation, inclusive. Outside that span, the station's operating status is unknown: first/last appearance does not prove commissioning or retirement. Station name/device/location combinations are reported, without inferring an unrecorded operational history.

There are {gaps['internal_gaps']:,} internal gaps totalling {gaps['internal_missing_slots']:,} missing five-minute slots, including {gaps['cross_year_gaps']} gaps across a year boundary and {gaps['long_gaps_at_least_60_missing_minutes']:,} gaps with at least sixty missing minutes. Across the full dataset, {facts['days_without_any_valid_observation']} complete SGT timestamp dates have no valid readings; 39 are in 2018, including 4–31 August. Every station's 2018 calendar coverage is below 90%. An observed zero is a dry reading; a nonexistent record has unknown rainfall. An unobserved station-year has zero observations and null rainfall/rate, rather than an inferred zero-rainfall year.

![Figure A1](../artifacts/stage3/figures/01_calendar_coverage.png)

**Figure A1.** {captions[0][1]}

## 4. Interannual rainfall patterns

We first report all available network records, then distinguish station composition from time support. A descriptive cohort requires calendar coverage of at least 80% in each of the eight years and one recorded coordinate combination. This yields twenty stations. Threshold sensitivity yields thirty stations at 70%, twenty-six at 75%, twenty at 80%, and none at 90% or 95%. The cohort rule uses coverage, not rainfall ranking; it remains a descriptive all-years selection and is not a forecasting training-station rule.

For the primary comparison, each station retains month-day/time keys observed in all eight years, excluding 29 February. Each station has 50,641–75,843 shared positions; the combined panel has 1,318,195 readings per year and 10,545,560 over all years. Station-year observed mean rainfall and wet fractions are averaged with equal station weights. Wet means strictly greater than zero. Each station's support is fixed across years, but supports differ across stations and cover only ten to twelve months with uneven month weights. Matching controls the selected calendar positions; it does not recover randomly or systematically missing rain.

{chr(10).join(annual_lines)}

On this selected station-specific matched support, 2023 has the highest observed mean rate (0.028935 mm per five-minute slot) and 2019 the lowest (0.017900). The highest year changes with support: 2022 for the unbalanced network, 2021 for the fixed cohort at available times, and 2023 for the primary matched panel. A restricted joint panel described below peaks in 2022 and has its lowest rate in 2023. These differences caution against treating available-data totals or means as complete national annual rainfall. No long-term climate trend or causal attribution is estimated.

![Figure A2](../artifacts/stage3/figures/02_annual_support_comparison.png)

**Figure A2.** {captions[1][1]}

![Figure A3](../artifacts/stage3/figures/03_calendar_support_distribution.png)

**Figure A3.** {captions[2][1]}

## 5. Differences across monitoring stations

Joint support across all twenty stations and all eight years contains 3,976 calendar positions in February, May, July, September and October; 70.7% lie in September. It fails the pre-specified twelve-month condition for the primary annual comparison and is used only as a restricted spatial diagnostic. Each station has 31,808 joint-support readings across eight years. {top['station_id']} ({metadata[top['station_id']]['station_name']}) has the highest pooled observed rate ({Decimal(top['rainfall_rate_mm_per_5min']):.6f} mm per slot), and {bottom['station_id']} ({metadata[bottom['station_id']]['station_name']}) the lowest ({Decimal(bottom['rainfall_rate_mm_per_5min']):.6f}). Under wider but station-specific supports, S33 instead ranks highest. Both the calendar restriction and equal exposure must accompany any ranking.

The supplementary rank tables compare all twenty stations on the identical 3,976 positions per year, with average ranks for tied amounts. Across the 28 year pairs, Spearman rank correlation ranges from {r_min:.3f} to {r_max:.3f}. S08's individual-year rank ranges from {s08['min_rank']:g} to {s08['max_rank']:g}; pooled first place is therefore not an invariant annual rank. The annual first-place stations are: {'; '.join(y + ': ' + ', '.join(s) for y, s in winners.items())}. These are descriptive checks on the five-month support; no significance test, full-season rank stability or spatial causal explanation is claimed. Detailed tables are in artifacts/stage5/station_rank_by_year.csv, station_rank_summary.csv and rank_correlations.csv.

![Figure A4](../artifacts/stage3/figures/04_station_differences.png)

**Figure A4.** {captions[3][1]}

![Figure A5](../artifacts/stage3/figures/05_station_locations.png)

**Figure A5.** {captions[4][1]}

Conditional wet p50/p90/p99 use percentile_approx with accuracy 10,000. The approximation concerns rank; it is not a confidence interval or a bound on missing-data uncertainty. [Spark 3.5.8 percentile_approx documentation](https://spark.apache.org/docs/3.5.8/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html) Network wet fractions are approximately 1.94%–3.20%, and conditional five-minute p99 values approximately 6.6–7.2 mm; neither describes the future thirty-minute target.

Ten readings at least 20 mm per five minutes remain in the cleaned data. They total 244.8 mm as a diagnostic record sum, with a maximum annual audit-amount share of {findings['max_network_extreme_rain_share_pct']:.4f}%. Exclusion is a sensitivity diagnostic only and reduces both sum and observation count. The primary panel includes just one of these readings, S33's 26.4 mm in 2021. Excluding it changes the equal-station rate by at most {findings['max_primary_rate_change_without_extremes_pct']:.4f}% and does not change the year ordering. This does not establish the meteorological validity of extreme readings.

![Figure A6](../artifacts/stage3/figures/06_wet_reading_quantiles.png)

**Figure A6.** {captions[5][1]}

## 6. Limitations, validation and AI use

The interval-end interpretation (t−5 min, t] remains an explicitly stated research convention, not an officially established historical timestamp definition. Timestamp-date coverage and a meteorological daily rainfall total are different constructions, particularly at midnight. Update fields are not confirmed first-availability times; historical revisions cannot be replayed from this snapshot. Offline results cannot alone establish operational information availability or live prediction performance.

Nonrandom missingness, instrument calibration and unrecorded station changes remain unresolved. The eight-year observation window and selected supports cannot identify a long-term climate trend. Gauges have no spatial area weights, so station millimetres are not summed or interpolated into national rainfall. No observed sums are simply divided by coverage to impute full annual totals. The descriptive cohort uses 2024 availability and must not be automatically adopted for model selection. B/C must fit preprocessing and selection only within their training data.

Independent acceptance checks reconcile source fingerprints and exact count/amount ledgers, coverage denominators and per-station gap totals, formal sample values, both matched panels against actual observations, equal-station weighting and empirical wet-quantile rank bounds. Stage 1 has five source-integrity tests; Stage 2 has nine cleaning-boundary tests and eight acceptance checks; Stage 3 has six analytical-boundary tests and eleven acceptance checks. Stage 4 verifies A's ordered run entry, actual environment, real sample read/write flow and delivery inventory. These are automated Codex-assisted checks. B's label/features implementation and C's actual model loading/prediction have not been supplied or tested at the time of this contribution.

**AI-use statement for the group to review:** Codex/ChatGPT assisted with understanding the project and responsibility split, locating official sources, drafting processing/checking scripts and documentation, executing local Spark analyses, and generating report text and Matplotlib figures. Automated checks and Codex visual inspection were recorded. They are not claims of human meteorological validation or independent group-member review. The final submitted statement should add only the human review and edits actually performed by the group, and the actual model-development assistance used by B/C. [AI activity record](AI_USAGE.md)

## Source and evidence links

{sources}

- [Source manifest](../data/data_manifest.csv), [Stage 2 quality report](../artifacts/stage2/STAGE_2_CHECK_REPORT.md), [Stage 3 analysis](STAGE_3_ANALYSIS.md).
- [Figure captions](../artifacts/stage5/figure_captions.csv), [report facts](../artifacts/stage5/report_facts.json), [B merge guide](REPORT_A_MERGE_GUIDE.md).

This is A's report contribution. Within-year rainfall analysis, threshold-target distributions, baselines, main model/evaluation and final PDF assembly belong to B/C and should be merged only from their actual delivered results.
"""
    (ROOT / "docs/REPORT_A_FULL.md").write_text(text)
    evidence = [{"claim": "Source records and bytes", "source": "artifacts/stage1/final_verification.json", "fields": "total_raw_rows; total_raw_bytes"}, {"claim": "Cleaning record/amount ledgers", "source": "artifacts/stage2/clean_accounting.json", "fields": "raw_rows; clean_keys; redundant_rainfall_sum_mm; conflicting_rows"}, {"claim": "Coverage and gaps", "source": "artifacts/stage2/final_verification.json; artifacts/stage2/gap_totals.json", "fields": "days_without_any_valid_observation; internal_gaps; internal_missing_slots"}, {"claim": "Hardware and software", "source": "artifacts/environment/environment.json; config/stage2.json", "fields": "hardware; packages; spark_mode; shuffle_partitions"}, {"claim": "Primary annual means/wet fractions", "source": "artifacts/stage3/tables/station_matched_annual.csv", "fields": "equal_station_rainfall_rate; equal_station_wet_rate; observed_slots"}, {"claim": "Spatial pooled rates and annual ranks", "source": "artifacts/stage3/tables/global_matched_year.csv; artifacts/stage3/tables/global_matched_pooled.csv", "fields": "rainfall_sum_mm; observed_slots; Stage 5 rank tables"}, {"claim": "Extreme sensitivity", "source": "artifacts/stage3/findings.json", "fields": "max_network_extreme_rain_share_pct; max_primary_rate_change_without_extremes_pct"}]
    export(out / "claim_evidence.csv", evidence)
    save_json(out / "report_complete.json", {"completed_at_sgt": now_sgt(), "scope": "A report only; B/C models not delivered", "outputs": [{"path": p.relative_to(ROOT).as_posix(), "sha256": sha256_file(p)} for p in [ROOT / "docs/REPORT_A_FULL.md", *sorted(out.glob("*.csv")), out / "report_facts.json"]]})
    print(f"A report assembled: six sections, six captions, 160 station-year ranks, 28 rank correlations ({r_min:.3f} to {r_max:.3f})")


if __name__ == "__main__":
    main()
