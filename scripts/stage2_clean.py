"""Create canonical observations, quarantines and exact accounting evidence."""
import json
import time

from pyspark.sql import functions as F

from common import ROOT, now_sgt
from stage2_common import aggregate_candidates, canonical_observations, export_csv, spark_session, write_json


def main():
    started = time.monotonic()
    artifacts = ROOT / "artifacts/stage2"
    assert (artifacts / "ingest_complete.json").exists(), "Complete ingestion first"
    spark = spark_session("rainfall-stage2-clean")
    try:
        parsed = spark.read.parquet(str(ROOT / "data/stage2/parsed"))
        invalid = parsed.filter(~F.col("is_valid_candidate"))
        # Keep source_year in the schema; unpartitioned writes also create a
        # readable schema-bearing file when the quarantine has zero records.
        invalid.write.mode("overwrite").parquet(str(ROOT / "data/quarantine/invalid"))
        print("Invalid-record quarantine written", flush=True)
        group_path = ROOT / "data/stage2/grouped"
        if not (group_path / "_SUCCESS").exists():
            aggregate_candidates(parsed.filter("is_valid_candidate")).write.mode("overwrite").parquet(str(group_path))
        grouped = spark.read.parquet(str(group_path))
        group_stats = grouped.agg(
            F.count("*").alias("candidate_keys"), F.sum("source_record_count").alias("candidate_rows"),
            F.sum(F.when(F.col("conflict"), F.col("source_record_count")).otherwise(0)).alias("conflicting_rows"),
            F.sum(F.col("conflict").cast("long")).alias("conflicting_keys"),
            F.sum(F.when(~F.col("conflict"), 1).otherwise(0)).alias("clean_keys"),
            F.sum(F.when(~F.col("conflict"), F.col("source_record_count") - 1).otherwise(0)).alias("redundant_rows_removed"),
            F.sum(F.when(~F.col("conflict"), F.col("source_record_count") - F.col("distinct_raw_records")).otherwise(0)).alias("exact_duplicate_rows_removed"),
            F.sum(F.when(~F.col("conflict"), F.col("distinct_raw_records") - 1).otherwise(0)).alias("compatible_variant_rows_removed"),
            F.sum("source_rainfall_sum_mm").alias("candidate_rainfall_sum_mm"),
            F.sum(F.when(F.col("conflict"), F.col("source_rainfall_sum_mm")).otherwise(0)).alias("conflicting_rainfall_sum_mm"),
            F.sum(F.when(~F.col("conflict"), F.col("representative.rainfall_mm")).otherwise(0)).alias("clean_rainfall_sum_mm"),
            F.sum(F.when(~F.col("conflict"), F.col("source_rainfall_sum_mm") - F.col("representative.rainfall_mm")).otherwise(0)).alias("redundant_rainfall_sum_mm"),
            F.max("source_record_count").alias("max_source_records_per_key"),
        ).first().asDict()
        write_json(artifacts / "group_accounting.json", group_stats)
        conflict_keys = grouped.filter("conflict").select("station_id", "observation_time", "min_rainfall_mm", "max_rainfall_mm", "min_location_key", "max_location_key")
        conflicts = parsed.filter("is_valid_candidate").join(conflict_keys, ["station_id", "observation_time"], "inner")
        conflicts.write.mode("overwrite").parquet(str(ROOT / "data/quarantine/conflicts"))
        export_csv(grouped.filter(F.col("source_record_count") > 1).groupBy("conflict", "source_record_count", "distinct_raw_records", "adjusted_source_records").count().orderBy("conflict", "source_record_count"), artifacts / "duplicate_group_distribution.csv")
        export_csv(conflict_keys.orderBy("station_id", "observation_time"), artifacts / "conflicting_keys.csv")
        print(f"Groups resolved: {group_stats['clean_keys']:,} clean keys; {group_stats['conflicting_keys']:,} conflicting keys", flush=True)
        clean = canonical_observations(grouped)
        clean.repartition(96, "observation_year", "observation_month").write.mode("overwrite").partitionBy("observation_year", "observation_month").parquet(str(ROOT / "data/clean/observations"))
        raw_stats = parsed.agg(
            F.count("*").alias("raw_rows"), F.sum("rainfall_mm").alias("parsed_rainfall_sum_mm"),
            F.sum(F.col("rainfall_mm").isNull().cast("long")).alias("unparseable_rainfall_rows"),
            F.sum(F.array_contains("invalid_reasons", "excess_rainfall_precision").cast("long")).alias("excess_precision_rows"),
        ).first().asDict()
        invalid_stats = invalid.agg(F.count("*").alias("invalid_rows"), F.sum("rainfall_mm").alias("invalid_parsed_rainfall_sum_mm")).first().asDict()
        accounting = {**raw_stats, **invalid_stats, **group_stats}
        accounting["invalid_parsed_rainfall_sum_mm"] = accounting["invalid_parsed_rainfall_sum_mm"] or 0
        accounting["row_ledger_balanced"] = raw_stats["raw_rows"] == invalid_stats["invalid_rows"] + group_stats["conflicting_rows"] + group_stats["clean_keys"] + group_stats["redundant_rows_removed"]
        accounting["decimal_ledger_balanced"] = raw_stats["parsed_rainfall_sum_mm"] == accounting["invalid_parsed_rainfall_sum_mm"] + group_stats["conflicting_rainfall_sum_mm"] + group_stats["clean_rainfall_sum_mm"] + group_stats["redundant_rainfall_sum_mm"]
        accounting["exact_original_numeric_ledger_available"] = raw_stats["unparseable_rainfall_rows"] == 0 and raw_stats["excess_precision_rows"] == 0
        assert accounting["row_ledger_balanced"] and accounting["decimal_ledger_balanced"]
        write_json(artifacts / "clean_accounting.json", accounting)
        write_json(artifacts / "clean_complete.json", {"completed_at_sgt": now_sgt(), "elapsed_seconds": round(time.monotonic() - started, 2), "accounting": accounting})
        print("Canonical Parquet and exact accounting complete", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
