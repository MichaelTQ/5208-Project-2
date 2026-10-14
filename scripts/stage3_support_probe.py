"""Measure exact calendar support before choosing any rainfall comparison."""
import csv
import json
from pyspark.sql import functions as F
from common import ROOT
from stage2_common import export_csv, spark_session, write_json


def select_cohort(threshold):
    annual = list(csv.DictReader((ROOT / "artifacts/stage2/coverage_annual.csv").open()))
    metadata = {r["station_id"]: r for r in csv.DictReader((ROOT / "artifacts/stage2/station_metadata_review.csv").open())}
    return [s for s in sorted(metadata) if sum(r["station_id"] == s and float(r["calendar_coverage"]) >= threshold for r in annual) == 8 and int(metadata[s]["coordinate_variants"]) == 1]


def matched_support(obs, years):
    return (obs.filter(F.col("observation_year").isin(years)).groupBy("station_id", "calendar_key")
            .agg(F.countDistinct("observation_year").alias("years_present"))
            .filter(F.col("years_present") == len(years)).select("station_id", "calendar_key"))


def main():
    cfg = json.loads((ROOT / "config/stage3.json").read_text())
    cohort = select_cohort(cfg["minimum_calendar_coverage_each_year"])
    assert cohort
    spark = spark_session("stage3-calendar-support-probe")
    try:
        obs = spark.read.parquet(str(ROOT / "data/clean/observations")).filter(F.col("station_id").isin(cohort)).filter(F.date_format("observation_time", "MM-dd") != "02-29").select("station_id", "observation_year", F.date_format("observation_time", cfg["calendar_key_format"]).alias("calendar_key"))
        support = matched_support(obs, cfg["years"])
        support.write.mode("overwrite").parquet(str(ROOT / "data/analysis/stage3/station_calendar_support"))
        support = spark.read.parquet(str(ROOT / "data/analysis/stage3/station_calendar_support"))
        global_keys = support.groupBy("calendar_key").count().filter(F.col("count") == len(cohort)).select("calendar_key")
        global_keys.write.mode("overwrite").parquet(str(ROOT / "data/analysis/stage3/global_calendar_support"))
        global_keys = spark.read.parquet(str(ROOT / "data/analysis/stage3/global_calendar_support"))
        station_counts = support.groupBy("station_id").count().orderBy("station_id")
        export_csv(station_counts, ROOT / "artifacts/stage3/tables/station_support_counts.csv")
        export_csv(support.withColumn("month", F.substring("calendar_key", 1, 2)).groupBy("station_id", "month").count().orderBy("station_id", "month"), ROOT / "artifacts/stage3/tables/station_support_months.csv")
        months = global_keys.withColumn("month", F.substring("calendar_key", 1, 2)).groupBy("month").count().orderBy("month")
        export_csv(months, ROOT / "artifacts/stage3/tables/global_support_months.csv")
        result = {"cohort": cohort, "global_matched_slots": global_keys.count(), "global_months": months.collect(), "station_support": station_counts.collect(), "threshold_sensitivity": {str(t): select_cohort(t) for t in cfg["coverage_threshold_sensitivity"]}}
        write_json(ROOT / "artifacts/stage3/support_probe.json", result)
        print(f"Cohort {len(cohort)} stations; global shared calendar slots {result['global_matched_slots']:,}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
