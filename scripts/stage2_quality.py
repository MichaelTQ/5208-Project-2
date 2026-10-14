"""Station history, coverage, gaps and the small A-to-B handoff sample."""
import csv
import math
import time
from collections import defaultdict
from datetime import date

from pyspark.sql import Window, functions as F, types as T

from common import ROOT, RAW_COLUMNS, now_sgt
from stage2_common import export_csv, parse_raw, spark_session, stage2_config, write_json


def span_slots(day_start, first, last):
    """Count grid timestamps in a day intersected with the inclusive observed span."""
    lower = F.greatest(F.unix_timestamp(day_start), F.unix_timestamp(first))
    upper = F.least(F.unix_timestamp(day_start) + 86400, F.unix_timestamp(last) + 300)
    return F.when(first.isNotNull(), (F.greatest(F.lit(0), upper - lower) / 300).cast("long")).otherwise(F.lit(0).cast("long"))


def haversine(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    value = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371000 * 2 * math.asin(min(1, math.sqrt(value)))


def metadata_review(variants_path, output):
    stations = defaultdict(lambda: {"names": set(), "devices": set(), "locations": set()})
    with variants_path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            station = stations[row["station_id"]]
            station["names"].add(row["station_name"])
            station["devices"].add(row["station_device_id"])
            if row["location_key"]:
                station["locations"].add(tuple(map(float, row["location_key"].split(","))))
    rows = []
    for station_id, station in sorted(stations.items()):
        locations = sorted(station["locations"])
        distance = max((haversine(a, b) for i, a in enumerate(locations) for b in locations[i + 1:]), default=0)
        rows.append({"station_id": station_id, "name_variants": len(station["names"]), "device_variants": len(station["devices"]), "coordinate_variants": len(locations), "max_location_separation_m": round(distance, 3), "location_change_review": distance >= stage2_config()["location_change_review_distance_m"]})
    with output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    return rows


def coverage_period(daily, column):
    grouped = daily.groupBy("station_id", column).agg(
        F.sum("observed_slots").alias("observed_slots"), F.sum("calendar_expected_slots").alias("calendar_expected_slots"),
        F.sum("span_expected_slots").alias("span_expected_slots"), F.sum("rainfall_sum_mm").alias("rainfall_sum_mm"),
        F.sum("wet_slots").alias("wet_slots"), F.min("first_observation").alias("first_observation"), F.max("last_observation").alias("last_observation"),
    )
    return (grouped.withColumn("calendar_missing_slots", F.col("calendar_expected_slots") - F.col("observed_slots"))
            .withColumn("span_missing_slots", F.col("span_expected_slots") - F.col("observed_slots"))
            .withColumn("outside_observed_span_unknown_slots", F.col("calendar_expected_slots") - F.col("span_expected_slots"))
            .withColumn("calendar_coverage", F.col("observed_slots") / F.col("calendar_expected_slots"))
            .withColumn("span_coverage", F.when(F.col("span_expected_slots") > 0, F.col("observed_slots") / F.col("span_expected_slots"))))


def main():
    started = time.monotonic()
    artifacts = ROOT / "artifacts/stage2"
    assert (artifacts / "clean_complete.json").exists(), "Complete cleaning first"
    spark = spark_session("rainfall-stage2-quality")
    try:
        parsed = spark.read.parquet(str(ROOT / "data/stage2/parsed"))
        clean = spark.read.parquet(str(ROOT / "data/clean/observations"))
        export_csv(spark.read.parquet(str(ROOT / "data/quarantine/conflicts")).orderBy("station_id", "observation_time"), artifacts / "conflicting_records.csv")
        raw_variants = parsed.filter(F.col("station_id").isNotNull()).groupBy("station_id", "station_name", "station_device_id", "location_key", "longitude", "latitude").agg(
            F.count("*").alias("raw_records"), F.min("raw_observation_time").alias("first_appearance"), F.max("raw_observation_time").alias("last_appearance"))
        export_csv(raw_variants.orderBy("station_id", "first_appearance"), artifacts / "station_metadata_raw_variants.csv")
        clean_variants = clean.groupBy("station_id", "station_name", "station_device_id", "location_key", "longitude", "latitude").agg(
            F.count("*").alias("clean_records"), F.min("observation_time").alias("first_appearance"), F.max("observation_time").alias("last_appearance"))
        export_csv(clean_variants.orderBy("station_id", "first_appearance"), artifacts / "station_metadata_clean_variants.csv")
        reviews = metadata_review(artifacts / "station_metadata_clean_variants.csv", artifacts / "station_metadata_review.csv")
        spans = clean.groupBy("station_id").agg(F.min("observation_time").alias("station_first_observation"), F.max("observation_time").alias("station_last_observation"), F.count("*").alias("station_observed_slots"))
        export_csv(spans.orderBy("station_id"), artifacts / "station_observed_spans.csv")
        print(f"Station metadata exported: {len(reviews)} clean station IDs", flush=True)

        intervals = (clean.select("station_id", "observation_time", "rainfall_mm")
                     .withColumn("previous_observation", F.lag("observation_time").over(Window.partitionBy("station_id").orderBy("observation_time")))
                     .withColumn("elapsed_seconds", F.unix_timestamp("observation_time") - F.unix_timestamp("previous_observation")))
        gaps = (intervals.filter(F.col("elapsed_seconds") > 300)
                .withColumn("first_missing_time", F.expr("previous_observation + INTERVAL 5 MINUTES"))
                .withColumn("last_missing_time", F.expr("observation_time - INTERVAL 5 MINUTES"))
                .withColumn("missing_slots", (F.col("elapsed_seconds") / 300 - 1).cast("long"))
                .withColumn("missing_duration_minutes", F.col("missing_slots") * 5)
                .withColumn("crosses_year_boundary", F.year("previous_observation") != F.year("observation_time")))
        gaps.write.mode("overwrite").parquet(str(artifacts / "gaps"))
        gaps = spark.read.parquet(str(artifacts / "gaps"))
        export_csv(gaps.filter(F.col("missing_duration_minutes") >= stage2_config()["long_gap_threshold_minutes"]).orderBy("station_id", "observation_time"), artifacts / "long_gaps.csv")
        gap_summary = gaps.groupBy("station_id").agg(F.count("*").alias("internal_gap_count"), F.sum("missing_slots").alias("internal_missing_slots"), F.max("missing_duration_minutes").alias("longest_internal_gap_minutes"), F.sum(F.col("crosses_year_boundary").cast("long")).alias("cross_year_gap_count"))
        export_csv(gap_summary.orderBy("station_id"), artifacts / "gap_summary.csv")
        gap_totals = gaps.agg(F.count("*").alias("internal_gaps"), F.sum("missing_slots").alias("internal_missing_slots"), F.sum(F.col("crosses_year_boundary").cast("long")).alias("cross_year_gaps"), F.sum((F.col("missing_duration_minutes") >= 60).cast("long")).alias("long_gaps_at_least_60_missing_minutes")).first().asDict()
        write_json(artifacts / "gap_totals.json", gap_totals)
        print(f"Internal gaps written: {gap_totals['internal_gaps']:,}", flush=True)

        cfg = stage2_config()
        stations = parsed.filter(F.col("station_id").isNotNull() & (F.trim("station_id") != "")).select("station_id").distinct()
        end_date = date.fromisoformat(cfg["study_end_exclusive_sgt"][:10])
        dates = spark.sql(f"SELECT explode(sequence(to_date('{cfg['study_start_sgt'][:10]}'), date_sub(to_date('{end_date.isoformat()}'),1), interval 1 day)) AS observation_date")
        daily_actual = clean.groupBy("station_id", "observation_date").agg(F.count("*").alias("observed_slots"), F.sum("rainfall_mm").alias("rainfall_sum_mm"), F.sum((F.col("rainfall_mm") > 0).cast("long")).alias("wet_slots"), F.min("observation_time").alias("first_observation"), F.max("observation_time").alias("last_observation"))
        daily = (stations.crossJoin(F.broadcast(dates)).join(daily_actual, ["station_id", "observation_date"], "left").join(spans, "station_id", "left")
                 .fillna(0, subset=["observed_slots", "wet_slots"])
                 .withColumn("calendar_expected_slots", F.lit(288).cast("long"))
                 .withColumn("span_expected_slots", span_slots(F.col("observation_date").cast("timestamp"), F.col("station_first_observation"), F.col("station_last_observation")))
                 .withColumn("calendar_missing_slots", F.col("calendar_expected_slots") - F.col("observed_slots"))
                 .withColumn("span_missing_slots", F.col("span_expected_slots") - F.col("observed_slots"))
                 .withColumn("outside_observed_span_unknown_slots", F.col("calendar_expected_slots") - F.col("span_expected_slots"))
                 .withColumn("calendar_coverage", F.col("observed_slots") / F.col("calendar_expected_slots"))
                 .withColumn("span_coverage", F.when(F.col("span_expected_slots") > 0, F.col("observed_slots") / F.col("span_expected_slots")))
                 .withColumn("observation_year", F.year("observation_date"))
                 .withColumn("observation_year_month", F.date_format("observation_date", "yyyy-MM")))
        daily.write.mode("overwrite").partitionBy("observation_year").parquet(str(artifacts / "coverage_daily"))
        daily = spark.read.parquet(str(artifacts / "coverage_daily"))
        export_csv(daily.groupBy("observation_date").agg(F.sum("observed_slots").alias("observed_slots"), F.sum((F.col("observed_slots") > 0).cast("long")).alias("stations_with_observations")).orderBy("observation_date"), artifacts / "daily_station_activity.csv")
        export_csv(coverage_period(daily, "observation_year_month").orderBy("station_id", "observation_year_month"), artifacts / "coverage_monthly.csv")
        annual = coverage_period(daily, "observation_year")
        export_csv(annual.orderBy("station_id", "observation_year"), artifacts / "coverage_annual.csv")
        export_csv(annual.groupBy("observation_year").agg(F.count("*").alias("all_station_ids"), F.sum((F.col("observed_slots") > 0).cast("long")).alias("stations_with_observations"), F.sum((F.col("calendar_coverage") >= .95).cast("long")).alias("stations_calendar_coverage_ge_95pct"), F.sum((F.col("calendar_coverage") >= .90).cast("long")).alias("stations_calendar_coverage_ge_90pct"), F.sum("observed_slots").alias("observed_slots")).orderBy("observation_year"), artifacts / "coverage_annual_summary.csv")
        print("Daily, monthly and annual coverage complete", flush=True)

        export_csv(clean.select("source_year", F.explode("quality_flags").alias("flag")).groupBy("source_year", "flag").count().orderBy("source_year", "flag"), artifacts / "clean_quality_flag_counts.csv")
        export_csv(clean.filter(F.array_contains("quality_flags", "extreme_rainfall_candidate")).orderBy(F.desc("rainfall_mm"), "station_id", "observation_time"), artifacts / "extreme_candidates.csv")
        extremes = clean.filter(F.array_contains("quality_flags", "extreme_rainfall_candidate")).select(F.col("station_id").alias("event_station_id"), F.col("observation_time").alias("event_time"), F.col("rainfall_mm").alias("event_rainfall_mm"))
        neighbors = clean.select("station_id", F.col("observation_time").alias("neighbor_time"), F.col("rainfall_mm").alias("neighbor_rainfall_mm"))
        context = neighbors.join(F.broadcast(extremes), (F.col("station_id") == F.col("event_station_id")) & (F.abs(F.unix_timestamp("neighbor_time") - F.unix_timestamp("event_time")) <= 900), "inner").withColumn("offset_minutes", (F.unix_timestamp("neighbor_time") - F.unix_timestamp("event_time")) / 60)
        export_csv(context.orderBy("event_station_id", "event_time", "neighbor_time"), artifacts / "extreme_temporal_context.csv")
        export_csv(clean.filter("observation_month = 1 OR observation_month = 12").groupBy("station_id", "observation_year", "observation_month").agg(F.min("observation_time").alias("first_observation"), F.max("observation_time").alias("last_observation"), F.count("*").alias("rows")).orderBy("station_id", "observation_year", "observation_month"), artifacts / "year_boundary_observations.csv")

        schema = T.StructType([T.StructField(c, T.StringType(), True) for c in RAW_COLUMNS] + [T.StructField("_corrupt_record", T.StringType(), True)])
        keys = None
        for path in sorted((ROOT / "data/samples/raw").glob("rainfall_*_sample.csv")):
            year = int(path.name.split("_")[1])
            raw_sample = spark.read.schema(schema).option("header", True).csv(str(path)).withColumn("source_year", F.lit(year))
            annual_keys = parse_raw(raw_sample).filter("is_valid_candidate").select("station_id", "observation_time")
            keys = annual_keys if keys is None else keys.unionByName(annual_keys)
        sample = clean.join(F.broadcast(keys.distinct()), ["station_id", "observation_time"], "inner")
        sample.coalesce(8).write.mode("overwrite").partitionBy("observation_year").parquet(str(ROOT / "data/handoff/stage2_sample"))
        sample = spark.read.parquet(str(ROOT / "data/handoff/stage2_sample"))
        export_csv(sample.orderBy("station_id", "observation_time"), ROOT / "data/handoff/stage2_sample.csv")
        write_json(artifacts / "clean_schema.json", clean.schema.jsonValue())
        write_json(artifacts / "sample_summary.json", {"rows": sample.count(), "by_year": sample.groupBy("observation_year").count().orderBy("observation_year").collect(), "stations": [r.station_id for r in sample.select("station_id").distinct().orderBy("station_id").collect()]})
        write_json(artifacts / "quality_complete.json", {"completed_at_sgt": now_sgt(), "elapsed_seconds": round(time.monotonic() - started, 2), "stations": len(reviews), "location_review_station_ids": [r["station_id"] for r in reviews if r["location_change_review"]], "gaps": gap_totals})
        print("Stage 2 quality tables and handoff sample complete", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
