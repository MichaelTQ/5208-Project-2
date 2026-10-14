"""Full-data Spark parse and diagnostic stage; checkpoint each annual result."""
import csv
import json
import time

from pyspark.sql import Observation, functions as F, types as T

from common import ROOT, RAW_COLUMNS, config, now_sgt, sha256_file
from stage2_common import export_csv, parse_raw, spark_session, write_json


def main():
    artifacts = ROOT / "artifacts/stage2"
    artifacts.mkdir(parents=True, exist_ok=True)
    with (ROOT / "data/data_manifest.csv").open(newline="") as handle:
        manifest = list(csv.DictReader(handle))
    integrity = []
    for row in manifest:
        path = ROOT / row["local_path"]
        assert path.stat().st_size == int(row["size_bytes"])
        assert sha256_file(path) == row["sha256"]
        integrity.append({"year": int(row["year"]), "sha256": row["sha256"], "verified": True})
    write_json(artifacts / "input_integrity.json", {"checked_at_sgt": now_sgt(), "files": integrity})
    spark = spark_session("rainfall-stage2-full-ingest")
    try:
        profiles = []
        for row in manifest:
            year = int(row["year"])
            output = ROOT / f"data/stage2/parsed/source_year={year}"
            profile_path = artifacts / f"ingest_{year}.json"
            if (output / "_SUCCESS").exists() and profile_path.exists():
                profiles.append(json.loads(profile_path.read_text()))
                print(f"{year}: reuse completed parse checkpoint", flush=True)
                continue
            started = time.monotonic()
            schema = T.StructType([T.StructField(c, T.StringType(), True) for c in RAW_COLUMNS] + [T.StructField("_corrupt_record", T.StringType(), True)])
            raw = spark.read.schema(schema).option("header", True).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").csv(str(ROOT / row["local_path"]))
            parsed = parse_raw(raw.withColumn("source_year", F.lit(year)))
            observation = Observation(f"year-{year}")
            parsed = parsed.observe(observation,
                F.count("*").alias("raw_rows"),
                F.sum((~F.col("is_valid_candidate")).cast("long")).alias("invalid_rows"),
                F.sum((F.col("time_adjust_seconds") == 1).cast("long")).alias("one_second_offset_rows"),
                F.sum("rainfall_mm").alias("parsed_rainfall_sum_mm"),
                F.min("rainfall_mm").alias("min_rainfall_mm"), F.max("rainfall_mm").alias("max_rainfall_mm"),
                F.sum(F.array_contains("quality_flags", "extreme_rainfall_candidate").cast("long")).alias("extreme_candidates"),
                F.min("update_delay_seconds").alias("min_update_delay_seconds"), F.max("update_delay_seconds").alias("max_update_delay_seconds"),
                F.min("reading_update_delay_seconds").alias("min_reading_update_delay_seconds"), F.max("reading_update_delay_seconds").alias("max_reading_update_delay_seconds"),
            )
            parsed.drop("source_year").write.mode("overwrite").parquet(str(output))
            profile = {"source_year": year, **observation.get, "elapsed_seconds": round(time.monotonic() - started, 2)}
            assert profile["raw_rows"] == int(row["row_count"])
            write_json(profile_path, profile)
            profiles.append(profile)
            print(f"{year}: parsed {profile['raw_rows']:,}; invalid candidates {profile['invalid_rows']:,}; one-second offsets {profile['one_second_offset_rows']:,}", flush=True)
        parsed = spark.read.parquet(str(ROOT / "data/stage2/parsed"))
        export_csv(parsed.groupBy("source_year", "grid_remainder_seconds").count().orderBy("source_year", "grid_remainder_seconds"), artifacts / "time_grid_offsets.csv")
        export_csv(parsed.select("source_year", F.explode("invalid_reasons").alias("reason")).groupBy("source_year", "reason").count().orderBy("source_year", "reason"), artifacts / "invalid_reason_counts.csv")
        export_csv(parsed.select("source_year", F.explode("quality_flags").alias("flag")).groupBy("source_year", "flag").count().orderBy("source_year", "flag"), artifacts / "raw_quality_flag_counts.csv")
        export_csv(parsed.groupBy("source_year", "reading_type", "reading_unit").count().orderBy("source_year"), artifacts / "reading_type_unit_counts.csv")
        write_json(artifacts / "ingest_complete.json", {"completed_at_sgt": now_sgt(), "profiles": profiles, "total_raw_rows": sum(p["raw_rows"] for p in profiles)})
        print("Full parse and diagnostics complete", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
