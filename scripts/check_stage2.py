"""Independent acceptance checks against the final artifacts, not raw cleaning code."""
import calendar
import csv
import hashlib
import json
import time
from datetime import datetime
from decimal import Decimal

import pyarrow.parquet as pq
from pyspark.sql import functions as F

from common import ROOT, RAW_COLUMNS, now_sgt, sha256_file
from stage2_common import spark_session, write_json


def csv_rows(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def main():
    started = time.monotonic()
    artifacts = ROOT / "artifacts/stage2"
    (artifacts / "final_verification.json").unlink(missing_ok=True)
    (artifacts / "STAGE_2_CHECK_REPORT.md").unlink(missing_ok=True)
    checks = {}
    for filename in ["ingest_complete.json", "clean_complete.json", "quality_complete.json", "boundary_test_results.json"]:
        assert (artifacts / filename).exists(), filename
    assert json.loads((artifacts / "boundary_test_results.json").read_text())["passed"]
    manifest = csv_rows(ROOT / "data/data_manifest.csv")
    for row in manifest:
        path = ROOT / row["local_path"]
        assert path.stat().st_size == int(row["size_bytes"]) and sha256_file(path) == row["sha256"]
    checks["eight_original_files_unchanged"] = True
    inventory = []
    for path in sorted((ROOT / "data/clean/observations").rglob("*.parquet")):
        inventory.append({"path": str(path.relative_to(ROOT)), "bytes": path.stat().st_size, "rows": pq.ParquetFile(path).metadata.num_rows, "sha256": sha256_file(path)})
    write_json(artifacts / "clean_file_manifest.json", inventory)
    print("Original CSV and clean Parquet fingerprints verified", flush=True)
    spark = spark_session("rainfall-stage2-independent-check")
    try:
        clean = spark.read.parquet(str(ROOT / "data/clean/observations"))
        invalid = spark.read.parquet(str(ROOT / "data/quarantine/invalid"))
        conflicts = spark.read.parquet(str(ROOT / "data/quarantine/conflicts"))
        cfg = json.loads((ROOT / "config/stage2.json").read_text())
        bad = (F.col("station_id").isNull() | (F.trim("station_id") == "") | F.col("observation_time").isNull() | F.col("rainfall_mm").isNull() | (F.col("rainfall_mm") < 0)
               | (F.pmod(F.unix_timestamp("observation_time"), F.lit(300)) != 0)
               | (~F.col("time_adjust_seconds").isin(0, 1))
               | (F.unix_timestamp("observation_time") - F.unix_timestamp("raw_observation_time") != F.col("time_adjust_seconds"))
               | (F.col("reading_value").cast("decimal(20,9)") != F.col("rainfall_mm"))
               | (F.col("reading_unit") != "mm") | (F.col("reading_type") != "TB1 Rainfall 5 Minute Total F")
               | (F.col("observation_year") != F.year("observation_time")) | (F.col("observation_month") != F.month("observation_time"))
               | (F.col("interval_end_assumed") != F.col("observation_time"))
               | (F.unix_timestamp("interval_end_assumed") - F.unix_timestamp("interval_start_assumed") != 300)
               | (F.col("interval_semantics") != cfg["interval_semantics"])
               | (F.col("observation_time") < F.to_timestamp(F.lit(cfg["study_start_sgt"])))
               | (F.col("observation_time") >= F.to_timestamp(F.lit(cfg["study_end_exclusive_sgt"]))))
        stats = clean.agg(F.count("*").alias("rows"), F.sum("rainfall_mm").alias("rainfall_sum_mm"), F.sum("source_record_count").alias("represented_source_rows"), F.sum("exact_duplicate_rows_removed").alias("exact_removed"), F.sum("compatible_variant_rows_removed").alias("variant_removed"), F.sum(F.coalesce(bad, F.lit(True)).cast("long")).alias("invalid_final_rows"), F.sum(F.array_contains("quality_flags", "extreme_rainfall_candidate").cast("long")).alias("extreme_candidates"), F.sum((F.col("time_adjust_seconds") == 1).cast("long")).alias("shifted_clean_rows"), F.min(F.date_format("observation_time", "yyyy-MM-dd HH:mm:ssXXX")).alias("first_sgt"), F.max(F.date_format("observation_time", "yyyy-MM-dd HH:mm:ssXXX")).alias("last_sgt")).first().asDict()
        assert stats["invalid_final_rows"] == 0
        assert str(clean.schema["rainfall_mm"].dataType) == "DecimalType(20,9)"
        assert sum(r["rows"] for r in inventory) == stats["rows"]
        duplicate_keys = clean.groupBy("station_id", "observation_time").count().filter(F.col("count") > 1).limit(1).count()
        assert duplicate_keys == 0
        checks["full_final_table_unique_valid_grid_and_precision"] = True
        print(f"Final table independently checked: {stats['rows']:,} unique valid keys", flush=True)

        inv = invalid.agg(F.count("*").alias("n"), F.sum("rainfall_mm").alias("rain")).first()
        con = conflicts.agg(F.count("*").alias("n"), F.sum("rainfall_mm").alias("rain")).first()
        acc = json.loads((artifacts / "clean_accounting.json").read_text())
        raw_count = sum(int(r["row_count"]) for r in manifest)
        assert raw_count == inv.n + con.n + stats["rows"] + stats["exact_removed"] + stats["variant_removed"]
        assert raw_count == inv.n + con.n + stats["represented_source_rows"]
        assert stats["rows"] == acc["clean_keys"] and inv.n == acc["invalid_rows"] and con.n == acc["conflicting_rows"]
        raw_rain = sum(Decimal(p["parsed_rainfall_sum_mm"]) for p in json.loads((artifacts / "ingest_complete.json").read_text())["profiles"])
        # Removed rows carry the group's common value; this independently derives their amount.
        # The bounded integer multiplicity fits decimal(10,0). Multiplying by
        # an uncast long can force Spark to reduce a nine-digit decimal scale.
        assert clean.agg(F.max("source_record_count").alias("n")).first().n < 10**10
        removed_rain = clean.agg(F.sum(F.col("rainfall_mm") * (F.col("source_record_count") - 1).cast("decimal(10,0)")).alias("rain")).first().rain or Decimal(0)
        assert raw_rain == (inv.rain or Decimal(0)) + (con.rain or Decimal(0)) + stats["rainfall_sum_mm"] + removed_rain
        assert acc["exact_original_numeric_ledger_available"], "Unparseable/excess-precision source amounts require a separate numeric ledger"
        raw_extremes = sum(p["extreme_candidates"] for p in json.loads((artifacts / "ingest_complete.json").read_text())["profiles"])
        assert raw_extremes == stats["extreme_candidates"] + invalid.filter(F.col("rainfall_mm") >= 20).count() + conflicts.filter(F.col("rainfall_mm") >= 20).count() + clean.filter(F.col("rainfall_mm") >= 20).agg(F.sum(F.col("source_record_count") - 1).alias("n")).first().n
        checks["raw_row_and_exact_decimal_rainfall_ledgers_balanced"] = True
        checks["extreme_values_accounted_for"] = True

        daily = spark.read.parquet(str(artifacts / "coverage_daily"))
        actual = clean.groupBy("station_id", "observation_date").count()
        actual_mismatch = daily.join(actual, ["station_id", "observation_date"], "left").filter(F.col("observed_slots") != F.coalesce(F.col("count"), F.lit(0))).limit(1).count()
        assert actual_mismatch == 0
        coverage_bad = daily.filter((F.col("calendar_expected_slots") != 288) | ~F.col("calendar_coverage").between(0, 1) | ((F.col("span_expected_slots") > 0) & ~F.col("span_coverage").between(0, 1)) | (F.col("span_missing_slots") < 0) | (F.col("calendar_missing_slots") < 0)).limit(1).count()
        assert coverage_bad == 0
        spans = clean.groupBy("station_id").agg(F.min("observation_time").alias("first"), F.max("observation_time").alias("last"), F.count("*").alias("observed"))
        spans = spans.withColumn("expected", ((F.unix_timestamp("last") - F.unix_timestamp("first")) / 300 + 1).cast("long"))
        gaps = spark.read.parquet(str(artifacts / "gaps"))
        gap_sum = gaps.groupBy("station_id").agg(F.sum("missing_slots").alias("gap_missing"))
        coverage_sum = daily.groupBy("station_id").agg(F.sum("observed_slots").alias("daily_observed"), F.sum("span_expected_slots").alias("daily_expected"), F.sum("span_missing_slots").alias("daily_missing"))
        joined = spans.join(gap_sum, "station_id", "left").fillna(0, subset=["gap_missing"]).join(coverage_sum, "station_id")
        assert joined.filter((F.col("expected") - F.col("observed") != F.col("gap_missing")) | (F.col("expected") != F.col("daily_expected")) | (F.col("observed") != F.col("daily_observed")) | (F.col("gap_missing") != F.col("daily_missing"))).limit(1).count() == 0
        annual = csv_rows(artifacts / "coverage_annual.csv")
        monthly = csv_rows(artifacts / "coverage_monthly.csv")
        for row in annual:
            year = int(row["observation_year"])
            assert int(row["calendar_expected_slots"]) == (366 if calendar.isleap(year) else 365) * 288
            subset = [r for r in monthly if r["station_id"] == row["station_id"] and r["observation_year_month"].startswith(str(year))]
            assert len(subset) == 12
            for field in ["observed_slots", "calendar_expected_slots", "span_expected_slots", "calendar_missing_slots", "span_missing_slots"]:
                assert sum(int(r[field]) for r in subset) == int(row[field])
        for row in monthly:
            year, month = map(int, row["observation_year_month"].split("-"))
            assert int(row["calendar_expected_slots"]) == calendar.monthrange(year, month)[1] * 288
        assert sum(int(r["observed_slots"]) for r in annual) == stats["rows"]
        activity = csv_rows(artifacts / "daily_station_activity.csv")
        assert len(activity) == 2922 and sum(int(r["observed_slots"]) for r in activity) == stats["rows"]
        checks["daily_monthly_annual_coverage_and_leap_years"] = True
        checks["per_station_internal_gaps_equal_observed_span_missing_slots"] = True
        print("Coverage and internal gaps independently reconciled", flush=True)

        sample = spark.read.parquet(str(ROOT / "data/handoff/stage2_sample"))
        keys = sample.select("station_id", "observation_time")
        matching = clean.join(F.broadcast(keys), ["station_id", "observation_time"]).select(*sample.columns)
        assert sample.exceptAll(matching).limit(1).count() == 0 and matching.exceptAll(sample).limit(1).count() == 0
        assert sample.count() == json.loads((artifacts / "sample_summary.json").read_text())["rows"]
        for row in csv_rows(ROOT / "data/handoff/stage2_sample.csv"):
            content = {k: row[k] if row[k] != "" else None for k in RAW_COLUMNS}
            content["source_year"] = int(row["source_year"])
            digest = hashlib.sha256(json.dumps(content, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
            assert digest == row["raw_hash"]
            original = datetime.fromisoformat(row["timestamp"])
            normalized = datetime.fromisoformat(row["observation_time"])
            assert (normalized - original).total_seconds() == int(row["time_adjust_seconds"])
            assert Decimal(row["reading_value"]) == Decimal(row["rainfall_mm"])
        checks["handoff_sample_matches_final_table_and_original_values"] = True
        checks["synthetic_boundary_regressions"] = True
        write_json(artifacts / "final_verification.json", {"status": "PASS", "checked_at_sgt": now_sgt(), "elapsed_seconds": round(time.monotonic() - started, 2), "checks": checks, "clean_stats": stats, "clean_parquet_files": len(inventory), "clean_parquet_bytes": sum(r["bytes"] for r in inventory), "raw_rows": raw_count, "raw_rainfall_sum_mm": raw_rain, "removed_rainfall_sum_mm": removed_rain, "quarantined_invalid_rows": inv.n, "quarantined_conflict_rows": con.n, "days_without_any_valid_observation": sum(int(r["observed_slots"]) == 0 for r in activity)})
        print("Stage 2 independent acceptance: PASS", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
