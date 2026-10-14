"""Full Spark annual/station summaries on explicit observed calendar support."""
import csv
import json
import time
from pyspark.sql import functions as F
from common import ROOT, now_sgt, sha256_file
from stage2_common import export_csv, spark_session, write_json


def summarize(df, keys, cfg):
    threshold = cfg["extreme_sensitivity_threshold_mm_per_5min"]
    result = df.groupBy(*keys).agg(
        F.count("*").alias("observed_slots"), F.sum("rainfall_mm").alias("rainfall_sum_mm"),
        F.sum((F.col("rainfall_mm") > 0).cast("long")).alias("wet_slots"),
        F.sum((F.col("rainfall_mm") == 0).cast("long")).alias("zero_slots"),
        F.max("rainfall_mm").alias("max_rainfall_mm"),
        F.percentile_approx(F.when(F.col("rainfall_mm") > 0, F.col("rainfall_mm")), cfg["wet_quantile_probabilities"], cfg["wet_quantile_accuracy"]).alias("wet_quantiles"),
        F.sum((F.col("rainfall_mm") >= threshold).cast("long")).alias("extreme_slots"),
        F.sum(F.when(F.col("rainfall_mm") >= threshold, F.col("rainfall_mm")).otherwise(0)).alias("extreme_rainfall_sum_mm"),
        F.min("observation_time").alias("first_observation"), F.max("observation_time").alias("last_observation"),
        F.countDistinct(F.month("observation_time")).alias("months_with_observations"),
    )
    return add_rates(result).withColumn("wet_p50_mm", F.col("wet_quantiles")[0]).withColumn("wet_p90_mm", F.col("wet_quantiles")[1]).withColumn("wet_p99_mm", F.col("wet_quantiles")[2]).drop("wet_quantiles")


def add_rates(df):
    return (df.withColumn("wet_rate", F.col("wet_slots") / F.col("observed_slots"))
            .withColumn("rainfall_rate_mm_per_5min", F.col("rainfall_sum_mm") / F.col("observed_slots").cast("decimal(10,0)"))
            .withColumn("non_extreme_slots", F.col("observed_slots") - F.col("extreme_slots"))
            .withColumn("non_extreme_rainfall_sum_mm", F.col("rainfall_sum_mm") - F.col("extreme_rainfall_sum_mm"))
            .withColumn("rainfall_rate_without_extremes", F.when(F.col("non_extreme_slots") > 0, F.col("non_extreme_rainfall_sum_mm") / F.col("non_extreme_slots").cast("decimal(10,0)")))
            .withColumn("wet_rate_without_extremes", F.when(F.col("non_extreme_slots") > 0, (F.col("wet_slots") - F.col("extreme_slots")) / F.col("non_extreme_slots"))))


def annual_equal_weight(station_year, cohort_label):
    return (station_year.groupBy("observation_year").agg(
        F.count("*").alias("stations"), F.sum("observed_slots").alias("observed_slots"),
        F.min("observed_slots").alias("min_station_slots"), F.max("observed_slots").alias("max_station_slots"),
        F.avg("rainfall_rate_mm_per_5min").alias("equal_station_rainfall_rate"), F.avg("wet_rate").alias("equal_station_wet_rate"),
        F.avg("rainfall_sum_mm").alias("mean_station_observed_sum_mm"),
        F.avg("rainfall_rate_without_extremes").alias("equal_station_rate_without_extremes"),
        F.avg("wet_rate_without_extremes").alias("equal_station_wet_rate_without_extremes"),
        F.sum("extreme_slots").alias("extreme_slots"), F.sum("extreme_rainfall_sum_mm").alias("extreme_rainfall_sum_mm"),
        F.min("months_with_observations").alias("min_months_per_station"),
    ).withColumn("support_type", F.lit(cohort_label)))


def main():
    started = time.monotonic()
    cfg = json.loads((ROOT / "config/stage3.json").read_text())
    p, tables, data = ROOT / "artifacts/stage3", ROOT / "artifacts/stage3/tables", ROOT / "data/analysis/stage3"
    p.mkdir(parents=True, exist_ok=True)
    s2 = json.loads((ROOT / "artifacts/stage2/final_verification.json").read_text())
    assert s2["status"] == "PASS"
    inventory = json.loads((ROOT / "artifacts/stage2/clean_file_manifest.json").read_text())
    for row in inventory:
        path = ROOT / row["path"]
        assert path.stat().st_size == row["bytes"] and sha256_file(path) == row["sha256"]
    write_json(p / "input_verification.json", {"checked_at_sgt": now_sgt(), "files": len(inventory), "rows": sum(r["rows"] for r in inventory), "all_sha256_match": True, "stage2_verification_sha256": sha256_file(ROOT / "artifacts/stage2/final_verification.json"), "configuration": cfg})
    probe = json.loads((p / "support_probe.json").read_text())
    cohort = probe["cohort"]
    primary_global = probe["global_matched_slots"] >= cfg["minimum_global_matched_slots_for_primary_comparison"] and len(probe["global_months"]) >= cfg["minimum_months_in_global_support"]
    spark = spark_session("stage3-annual-station-analysis")
    try:
        obs = spark.read.parquet(str(ROOT / "data/clean/observations")).select("station_id", "observation_time", "observation_year", "rainfall_mm")
        stats_path = data / "station_year_summary"
        summarize(obs, ["station_id", "observation_year"], cfg).write.mode("overwrite").parquet(str(stats_path))
        summaries = spark.read.parquet(str(stats_path))
        coverage = spark.read.option("header", True).option("inferSchema", True).csv(str(ROOT / "artifacts/stage2/coverage_annual.csv")).select("station_id", "observation_year", "calendar_expected_slots", "calendar_missing_slots", "calendar_coverage", "span_expected_slots", "span_missing_slots", "span_coverage", "outside_observed_span_unknown_slots")
        combined = coverage.join(summaries, ["station_id", "observation_year"], "left").fillna(0, subset=["observed_slots", "wet_slots", "zero_slots", "extreme_slots", "non_extreme_slots"])
        export_csv(combined.orderBy("station_id", "observation_year"), tables / "all_station_year.csv")
        export_csv(summarize(obs, ["observation_year"], cfg).orderBy("observation_year"), tables / "network_year.csv")
        cohort_summary = summaries.filter(F.col("station_id").isin(cohort))
        export_csv(combined.filter(F.col("station_id").isin(cohort)).orderBy("station_id", "observation_year"), tables / "cohort_station_year.csv")
        export_csv(annual_equal_weight(cohort_summary, "cohort_observed_calendar_varies").orderBy("observation_year"), tables / "cohort_year.csv")
        print("Full-network and fixed-cohort annual summaries complete", flush=True)

        keyed = obs.filter(F.col("station_id").isin(cohort)).filter(F.date_format("observation_time", "MM-dd") != "02-29").withColumn("calendar_key", F.date_format("observation_time", cfg["calendar_key_format"]))
        station_support = spark.read.parquet(str(data / "station_calendar_support"))
        matched = keyed.join(station_support, ["station_id", "calendar_key"], "inner")
        matched.repartition(32, "observation_year").write.mode("overwrite").partitionBy("observation_year").parquet(str(data / "station_matched_observations"))
        matched = spark.read.parquet(str(data / "station_matched_observations"))
        matched_summary = summarize(matched, ["station_id", "observation_year"], cfg)
        matched_summary.write.mode("overwrite").parquet(str(data / "station_matched_year"))
        matched_summary = spark.read.parquet(str(data / "station_matched_year"))
        export_csv(matched_summary.orderBy("station_id", "observation_year"), tables / "station_matched_year.csv")
        export_csv(annual_equal_weight(matched_summary, "station_specific_calendar_matched").orderBy("observation_year"), tables / "station_matched_annual.csv")
        export_csv(summarize(matched, ["station_id"], cfg).orderBy("station_id"), tables / "station_matched_pooled.csv")
        print("Station-specific eight-year matched support summarized", flush=True)

        global_support = spark.read.parquet(str(data / "global_calendar_support"))
        globally_matched = matched.join(F.broadcast(global_support), "calendar_key", "inner")
        globally_matched.repartition(8, "observation_year").write.mode("overwrite").partitionBy("observation_year").parquet(str(data / "global_matched_observations"))
        globally_matched = spark.read.parquet(str(data / "global_matched_observations"))
        global_summary = summarize(globally_matched, ["station_id", "observation_year"], cfg)
        export_csv(global_summary.orderBy("station_id", "observation_year"), tables / "global_matched_year.csv")
        export_csv(annual_equal_weight(global_summary, "global_calendar_matched_partial_months").orderBy("observation_year"), tables / "global_matched_annual.csv")
        export_csv(summarize(globally_matched, ["station_id"], cfg).orderBy("station_id"), tables / "global_matched_pooled.csv")
        export_csv(globally_matched.withColumn("month", F.substring("calendar_key", 1, 2)).groupBy("month", "observation_year").agg(F.count("*").alias("rows")), tables / "global_panel_month_year.csv")
        meta = spark.read.option("header", True).option("inferSchema", True).csv(str(ROOT / "artifacts/stage2/station_metadata_clean_variants.csv")).filter(F.col("station_id").isin(cohort)).select("station_id", "station_name", "station_device_id", "longitude", "latitude", "location_key").distinct()
        export_csv(meta.orderBy("station_id"), tables / "cohort_metadata.csv")
        sensitivity = [{"calendar_coverage_threshold": t, "station_count": len(ids), "station_ids": ";".join(ids)} for t, ids in probe["threshold_sensitivity"].items()]
        with (tables / "cohort_threshold_sensitivity.csv").open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(sensitivity[0])); writer.writeheader(); writer.writerows(sensitivity)
        write_json(p / "analysis_complete.json", {"completed_at_sgt": now_sgt(), "elapsed_seconds": round(time.monotonic() - started, 2), "configuration": cfg, "cohort": cohort, "primary_comparison": "global_matched" if primary_global else "station_specific_matched", "global_support_is_annual_representative": False, "global_support_month_count": len(probe["global_months"]), "global_slots_per_station_year": probe["global_matched_slots"], "matched_rows": matched.count(), "global_rows": globally_matched.count()})
        print("Stage 3 analysis tables complete", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
