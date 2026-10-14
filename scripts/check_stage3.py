"""Independent numerical, calendar-panel and artifact acceptance checks."""
import csv
import json
import math
import time
from decimal import Decimal

from PIL import Image
from pyspark.sql import functions as F
from common import ROOT, now_sgt, sha256_file
from stage2_common import spark_session, write_json


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def keyed(rows):
    return {(r["station_id"], int(r["observation_year"])): r for r in rows}


def close(a, b, tolerance=1e-12):
    assert abs(float(a) - float(b)) <= tolerance, (a, b)


def verify_rates(r):
    n, wet, zero = int(r["observed_slots"]), int(r["wet_slots"]), int(r["zero_slots"])
    assert n == wet + zero
    if n == 0:
        assert r["rainfall_sum_mm"] == r["wet_rate"] == r["rainfall_rate_mm_per_5min"] == ""
        return
    rain = Decimal(r["rainfall_sum_mm"])
    assert rain >= 0 and 0 <= wet <= n
    close(r["wet_rate"], wet / n)
    close(r["rainfall_rate_mm_per_5min"], rain / n)
    count, amount = int(r["extreme_slots"]), Decimal(r["extreme_rainfall_sum_mm"])
    assert int(r["non_extreme_slots"]) == n - count
    assert Decimal(r["non_extreme_rainfall_sum_mm"]) == rain - amount
    if n > count:
        close(r["rainfall_rate_without_extremes"], (rain - amount) / (n - count))
        close(r["wet_rate_without_extremes"], (wet - count) / (n - count))
    if wet > 0:
        values = [Decimal(r[c]) for c in ["wet_p50_mm", "wet_p90_mm", "wet_p99_mm", "max_rainfall_mm"]]
        assert 0 < values[0] <= values[1] <= values[2] <= values[3]
    else:
        assert not r["wet_p50_mm"] and not r["wet_p90_mm"] and not r["wet_p99_mm"]


def main():
    started = time.monotonic()
    p, tables, data = ROOT / "artifacts/stage3", ROOT / "artifacts/stage3/tables", ROOT / "data/analysis/stage3"
    (p / "final_verification.json").unlink(missing_ok=True)
    (p / "STAGE_3_CHECK_REPORT.md").unlink(missing_ok=True)
    cfg = json.loads((ROOT / "config/stage3.json").read_text())
    complete = json.loads((p / "analysis_complete.json").read_text())
    tests = json.loads((p / "boundary_test_results.json").read_text())
    assert tests["passed"]
    checks = {}
    all_stats = keyed(read_csv(tables / "all_station_year.csv"))
    baseline = keyed(read_csv(ROOT / "artifacts/stage2/coverage_annual.csv"))
    assert set(all_stats) == set(baseline) and len(all_stats) == 728
    for key, r in all_stats.items():
        verify_rates(r)
        b = baseline[key]
        assert int(r["observed_slots"]) == int(b["observed_slots"])
        assert int(r["wet_slots"]) == int(b["wet_slots"])
        assert (Decimal(r["rainfall_sum_mm"]) if r["rainfall_sum_mm"] else None) == (Decimal(b["rainfall_sum_mm"]) if b["rainfall_sum_mm"] else None)
        close(r["calendar_coverage"], b["calendar_coverage"])
    network = read_csv(tables / "network_year.csv")
    for r in network:
        verify_rates(r)
        year = int(r["observation_year"])
        yearly = [v for (s, y), v in all_stats.items() if y == year]
        assert sum(int(v["observed_slots"]) for v in yearly) == int(r["observed_slots"])
        assert sum(Decimal(v["rainfall_sum_mm"]) for v in yearly if v["rainfall_sum_mm"]) == Decimal(r["rainfall_sum_mm"])
        assert sum(int(v["wet_slots"]) for v in yearly) == int(r["wet_slots"])
    s2 = json.loads((ROOT / "artifacts/stage2/final_verification.json").read_text())
    assert sum(int(r["observed_slots"]) for r in network) == s2["clean_stats"]["rows"]
    assert sum(Decimal(r["rainfall_sum_mm"]) for r in network) == Decimal(s2["clean_stats"]["rainfall_sum_mm"])
    checks["all_station_years_and_network_reconcile_with_stage2"] = True
    checks["observed_denominators_exact_amounts_and_extreme_sensitivity"] = True
    metadata = {r["station_id"]: r for r in read_csv(ROOT / "artifacts/stage2/station_metadata_review.csv")}
    expected_cohort = sorted(s for s, m in metadata.items() if int(m["coordinate_variants"]) == 1 and all(float(baseline[(s, year)]["calendar_coverage"]) >= cfg["minimum_calendar_coverage_each_year"] for year in cfg["years"]))
    assert expected_cohort == complete["cohort"]
    for r in read_csv(tables / "cohort_threshold_sensitivity.csv"):
        threshold = float(r["calendar_coverage_threshold"])
        expected = sorted(s for s, m in metadata.items() if int(m["coordinate_variants"]) == 1 and all(float(baseline[(s, year)]["calendar_coverage"]) >= threshold for year in cfg["years"]))
        assert int(r["station_count"]) == len(expected) and r["station_ids"].split(";") == (expected or [""])
    checks["coverage_only_cohort_and_threshold_sensitivity"] = True
    print("Station/network accounting, rates and cohort independently verified", flush=True)
    spark = spark_session("stage3-independent-check")
    try:
        original = (spark.read.parquet(str(ROOT / "data/clean/observations")).filter(F.col("station_id").isin(expected_cohort))
                    .select("station_id", "observation_time", "observation_year", "rainfall_mm")
                    .filter(F.date_format("observation_time", "MM-dd") != "02-29")
                    .withColumn("calendar_key", F.date_format("observation_time", "MM-dd HH:mm")))
        # Stage 2 guaranteed unique station/time; a calendar key occurs once per year.
        expected_support = original.groupBy("station_id", "calendar_key").agg(F.count("*").alias("n"), F.min("observation_year").alias("lo"), F.max("observation_year").alias("hi")).filter((F.col("n") == 8) & (F.col("lo") == 2017) & (F.col("hi") == 2024)).select("station_id", "calendar_key")
        support = spark.read.parquet(str(data / "station_calendar_support"))
        assert support.exceptAll(expected_support).limit(1).count() == 0 and expected_support.exceptAll(support).limit(1).count() == 0
        matched = spark.read.parquet(str(data / "station_matched_observations"))
        assert matched.count() == complete["matched_rows"]
        assert matched.groupBy("station_id", "calendar_key").agg(F.count("*").alias("n"), F.countDistinct("observation_year").alias("years")).filter((F.col("n") != 8) | (F.col("years") != 8)).limit(1).count() == 0
        assert matched.select("station_id", "calendar_key").distinct().exceptAll(support).limit(1).count() == 0
        assert matched.filter((F.date_format("observation_time", "MM-dd HH:mm") != F.col("calendar_key")) | (F.substring("calendar_key", 1, 5) == "02-29")).limit(1).count() == 0
        columns = ["station_id", "observation_time", "observation_year", "rainfall_mm", "calendar_key"]
        assert matched.join(original, columns, "left_anti").limit(1).count() == 0
        global_keys = spark.read.parquet(str(data / "global_calendar_support"))
        expected_global = support.groupBy("calendar_key").count().filter(F.col("count") == len(expected_cohort)).select("calendar_key")
        assert global_keys.exceptAll(expected_global).limit(1).count() == 0 and expected_global.exceptAll(global_keys).limit(1).count() == 0
        globally = spark.read.parquet(str(data / "global_matched_observations"))
        global_count = global_keys.count()
        assert globally.count() == global_count * 8 * len(expected_cohort) == complete["global_rows"]
        assert globally.join(matched, columns, "left_anti").limit(1).count() == 0
        assert globally.select("calendar_key").distinct().exceptAll(global_keys).limit(1).count() == 0
        global_combinations = globally.groupBy("calendar_key").agg(F.count("*").alias("n"), F.countDistinct(F.struct("station_id", "observation_year")).alias("unique_station_years"))
        assert global_combinations.filter((F.col("n") != 8 * len(expected_cohort)) | (F.col("unique_station_years") != 8 * len(expected_cohort))).limit(1).count() == 0
        checks["eight_year_calendar_support_complete_unique_and_source_exact"] = True
        checks["global_calendar_support_and_panel_membership"] = True
        print("Both calendar panels independently reconstructed and source-checked", flush=True)

        def panel_check(frame, filename):
            actual = {(r.station_id, r.observation_year): r for r in frame.groupBy("station_id", "observation_year").agg(F.count("*").alias("n"), F.sum("rainfall_mm").alias("rain"), F.sum((F.col("rainfall_mm") > 0).cast("long")).alias("wet"), F.sum((F.col("rainfall_mm") >= 20).cast("long")).alias("extreme"), F.sum(F.when(F.col("rainfall_mm") >= 20, F.col("rainfall_mm")).otherwise(0)).alias("extreme_rain")).collect()}
            rows = keyed(read_csv(tables / filename))
            assert set(actual) == set(rows) and len(actual) == len(expected_cohort) * 8
            for key, r in rows.items():
                verify_rates(r)
                a = actual[key]
                assert int(r["observed_slots"]) == a.n and Decimal(r["rainfall_sum_mm"]) == a.rain and int(r["wet_slots"]) == a.wet
                assert int(r["extreme_slots"]) == a.extreme and Decimal(r["extreme_rainfall_sum_mm"]) == a.extreme_rain
            return rows
        panel_check(matched, "station_matched_year.csv")
        panel_check(globally, "global_matched_year.csv")
        for source, annual_file in [("cohort_station_year.csv", "cohort_year.csv"), ("station_matched_year.csv", "station_matched_annual.csv"), ("global_matched_year.csv", "global_matched_annual.csv")]:
            station_rows = read_csv(tables / source)
            for r in read_csv(tables / annual_file):
                selected = [s for s in station_rows if s["observation_year"] == r["observation_year"]]
                assert len(selected) == int(r["stations"]) == len(expected_cohort)
                assert sum(int(s["observed_slots"]) for s in selected) == int(r["observed_slots"])
                for field, target in [("rainfall_rate_mm_per_5min", "equal_station_rainfall_rate"), ("wet_rate", "equal_station_wet_rate"), ("rainfall_sum_mm", "mean_station_observed_sum_mm"), ("rainfall_rate_without_extremes", "equal_station_rate_without_extremes")]:
                    close(sum(Decimal(s[field]) for s in selected) / len(selected), r[target])
        checks["panel_amounts_rates_and_equal_station_weighting"] = True
        assert complete["primary_comparison"] == "station_specific_matched" and complete["global_support_month_count"] < cfg["minimum_months_in_global_support"]
        checks["restricted_global_support_not_used_as_full_year_primary"] = True

        # Verify each station-year wet quantile against its empirical rank interval.
        quantiles = spark.read.parquet(str(data / "station_year_summary")).filter(F.col("wet_slots") > 0).select("station_id", "observation_year", "wet_slots", F.explode(F.array(*[F.struct(F.lit(prob).alias("prob"), F.col(col).cast("decimal(20,9)").alias("value")) for prob, col in [(.5, "wet_p50_mm"), (.9, "wet_p90_mm"), (.99, "wet_p99_mm")]])).alias("q"))
        wet = spark.read.parquet(str(ROOT / "data/clean/observations")).filter(F.col("rainfall_mm") > 0).select("station_id", "observation_year", "rainfall_mm")
        rank = wet.join(F.broadcast(quantiles), ["station_id", "observation_year"]).groupBy("station_id", "observation_year", "wet_slots", "q").agg(F.sum((F.col("rainfall_mm") < F.col("q.value")).cast("long")).alias("below"), F.sum((F.col("rainfall_mm") <= F.col("q.value")).cast("long")).alias("at_most"))
        error = F.col("wet_slots") / cfg["wet_quantile_accuracy"] + 1
        target = F.col("wet_slots") * F.col("q.prob")
        bad_rank = rank.filter((F.col("below") > target + error) | (F.col("at_most") < target - error)).count()
        assert bad_rank == 0, f"{bad_rank} approximate quantiles outside rank tolerance"
        checks["all_station_year_wet_quantiles_within_empirical_rank_bounds"] = True
        print("Aggregates and wet-quantile rank bounds verified", flush=True)
    finally:
        spark.stop()

    for r in json.loads((ROOT / "artifacts/stage2/clean_file_manifest.json").read_text()):
        path = ROOT / r["path"]
        assert path.stat().st_size == r["bytes"] and sha256_file(path) == r["sha256"]
    checks["stage2_parquet_unchanged"] = True
    figure_manifest = json.loads((p / "figure_manifest.json").read_text())
    assert len(figure_manifest) == 6
    for figure in figure_manifest:
        for source in figure["data_sources"]:
            assert sha256_file(ROOT / source["path"]) == source["sha256"]
        with Image.open(ROOT / figure["png"]) as im:
            im.verify()
        assert (ROOT / figure["pdf"]).read_bytes().startswith(b"%PDF-")
    checks["six_png_pdf_figures_and_their_source_fingerprints"] = True
    checks["six_analytical_boundary_tests"] = True
    write_json(p / "final_verification.json", {"status": "PASS", "checked_at_sgt": now_sgt(), "elapsed_seconds": round(time.monotonic() - started, 2), "checks": checks, "cohort_stations": len(expected_cohort), "matched_rows": complete["matched_rows"], "global_rows": complete["global_rows"], "global_calendar_keys": global_count, "figures": len(figure_manifest), "boundary_tests": tests["tests_run"]})
    print("Stage 3 independent acceptance: PASS", flush=True)


if __name__ == "__main__":
    main()
