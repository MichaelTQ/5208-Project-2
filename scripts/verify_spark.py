"""Verify real CSV ingestion, time semantics preservation and Parquet round-trip."""
import csv
import importlib.metadata
import os
import platform
import shutil
import subprocess
import sys
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T

from common import ROOT, RAW_COLUMNS, config, now_sgt, save_json


def main():
    cfg = config()
    java_version = subprocess.run([str(os.environ["JAVA_HOME"] + "/bin/java"), "-version"], capture_output=True, text=True, check=True).stderr
    if not java_version.startswith('openjdk version "17.'):
        raise RuntimeError("Java 17 is required")
    if platform.python_version() != cfg["python_version"]:
        raise RuntimeError("Unexpected Python version")
    spark = (SparkSession.builder.appName("rainfall-stage1-verification")
             .master(cfg["spark_master"])
             .config("spark.driver.memory", cfg["driver_memory"])
             .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
             .config("spark.sql.session.timeZone", cfg["timezone"])
             .config("spark.ui.enabled", "false")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        assert spark.version == cfg["spark_version"]
        jvm_max_memory = spark._jvm.java.lang.Runtime.getRuntime().maxMemory()
        assert jvm_max_memory == 4 * 1024**3, jvm_max_memory
        frames = []
        expected_rows = 0
        for year in cfg["datasets"]:
            path = ROOT / f"data/samples/raw/rainfall_{year}_sample.csv"
            with path.open(newline="") as handle:
                reader = csv.reader(handle)
                assert next(reader) == RAW_COLUMNS
                expected_rows += sum(1 for _ in reader)
            schema = T.StructType([T.StructField(name, T.StringType(), True) for name in RAW_COLUMNS] + [T.StructField("_corrupt_record", T.StringType(), True)])
            raw = spark.read.schema(schema).option("header", True).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").csv(str(path))
            frames.append(raw.withColumn("source_year", F.lit(int(year))))
        raw = frames[0]
        for frame in frames[1:]:
            raw = raw.unionByName(frame)
        df = (raw.withColumn("observation_time", F.to_timestamp("timestamp"))
              .withColumn("parsed_update_time", F.to_timestamp("update_timestamp"))
              .withColumn("parsed_reading_update_time", F.to_timestamp("reading_update_timestamp"))
              .withColumn("rainfall_mm", F.col("reading_value").cast("double"))
              .withColumn("longitude", F.col("location_longitude").cast("double"))
              .withColumn("latitude", F.col("location_latitude").cast("double"))
              .cache())
        total = df.count()
        assert total == expected_rows
        checks = df.agg(
            F.sum(F.col("_corrupt_record").isNotNull().cast("int")).alias("malformed_csv"),
            F.sum(F.col("observation_time").isNull().cast("int")).alias("invalid_observation_time"),
            F.sum(F.col("parsed_update_time").isNull().cast("int")).alias("invalid_update_time"),
            F.sum(F.col("parsed_reading_update_time").isNull().cast("int")).alias("invalid_reading_update_time"),
            F.sum((F.col("rainfall_mm").isNull() | F.isnan("rainfall_mm")).cast("int")).alias("invalid_rainfall"),
            F.sum((F.col("rainfall_mm") < 0).cast("int")).alias("negative_rainfall"),
            F.sum((F.col("rainfall_mm") == 0).cast("int")).alias("zero_rainfall"),
            F.sum((F.col("rainfall_mm") > 0).cast("int")).alias("positive_rainfall"),
            F.sum((F.col("longitude").isNull() | F.col("latitude").isNull()).cast("int")).alias("invalid_coordinates"),
            F.sum((F.col("station_id").isNull() | (F.trim("station_id") == "")).cast("int")).alias("missing_station_id"),
            F.sum((F.to_date("observation_time") != F.to_date("date")).cast("int")).alias("date_observation_mismatch"),
        ).first().asDict()
        year_counts = [row.asDict() for row in df.groupBy("source_year").count().orderBy("source_year").collect()]
        seconds = [row.asDict() for row in df.groupBy("source_year", F.second("observation_time").alias("second")).count().orderBy("source_year", "second").collect()]
        stations = sorted(row[0] for row in df.select("station_id").distinct().collect())
        units = sorted(row[0] for row in df.select("reading_unit").distinct().collect())
        reading_types = sorted(row[0] for row in df.select("reading_type").distinct().collect())
        duplicate_keys = df.groupBy("station_id", "observation_time").count().filter("count > 1").count()
        time_examples = []
        for year in cfg["datasets"]:
            row = df.filter(F.col("source_year") == int(year)).select("timestamp", F.unix_timestamp("observation_time").alias("epoch"), F.date_format("observation_time", "yyyy-MM-dd HH:mm:ss").alias("sgt_display")).first()
            expected_epoch = int(datetime.fromisoformat(row["timestamp"]).timestamp())
            assert expected_epoch == row["epoch"]
            assert row["sgt_display"] == datetime.fromisoformat(row["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
            time_examples.append({**row.asDict(), "python_epoch": expected_epoch, "year": int(year)})
        output = ROOT / "data/samples/parquet"
        df.drop("_corrupt_record").write.mode("overwrite").partitionBy("source_year").parquet(str(output))
        reread = spark.read.parquet(str(output))
        comparable = df.drop("_corrupt_record")
        reread = reread.select(*comparable.columns)
        assert reread.count() == total
        assert comparable.exceptAll(reread).limit(1).count() == 0
        assert reread.exceptAll(comparable).limit(1).count() == 0
        assertions = {
            "row_count_preserved": True,
            "parquet_multiset_equal": True,
            "timezone_epoch_and_display_verified_all_years": True,
            "all_eight_years_present": len(year_counts) == 8,
            "all_four_selected_stations_present": stations == sorted(cfg["sample_stations"]),
            "rain_and_dry_examples_present": checks["positive_rainfall"] > 0 and checks["zero_rainfall"] > 0,
            "2017_second_offset_preserved": any(r["source_year"] == 2017 and r["second"] != 0 for r in seconds),
            "sample_has_no_parse_failures": all(checks[k] == 0 for k in ["malformed_csv", "invalid_observation_time", "invalid_update_time", "invalid_reading_update_time", "invalid_rainfall", "invalid_coordinates", "missing_station_id"]),
            "sample_units_are_mm": units == ["mm"],
            "sample_reading_type_is_5_minute_total": reading_types == ["TB1 Rainfall 5 Minute Total F"],
        }
        save_json(ROOT / "artifacts/stage1/spark_verification.json", {
            "checked_at_sgt": now_sgt(), "scope": "Development sample only, not full-data quality assessment",
            "rows": total, "sample_checks": checks, "year_counts": year_counts,
            "seconds_by_year": seconds, "stations": stations, "units": units,
            "reading_types": reading_types, "duplicate_station_time_keys": duplicate_keys,
            "time_examples": time_examples, "assertions": assertions,
            "schema": comparable.schema.jsonValue(),
        })
        versions = {p: importlib.metadata.version(p) for p in ["pyspark", "py4j", "numpy", "pandas", "pyarrow", "matplotlib"]}
        hardware = {}
        for key in ["hw.physicalcpu", "hw.logicalcpu", "hw.memsize"]:
            process = subprocess.run(["sysctl", "-n", key], capture_output=True, text=True)
            hardware[key] = int(process.stdout.strip()) if process.returncode == 0 else "unavailable_in_current_sandbox"
        save_json(ROOT / "artifacts/environment/environment.json", {
            "recorded_at_sgt": now_sgt(), "os": platform.platform(), "architecture": platform.machine(),
            "python_version": platform.python_version(), "java_version_output": java_version,
            "packages": versions, "hardware": hardware, "disk_free_bytes": shutil.disk_usage(ROOT).free,
            "spark_mode": cfg["spark_master"], "spark_driver_memory": cfg["driver_memory"],
            "actual_jvm_max_heap_bytes": jvm_max_memory,
            "spark_shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
            "spark_timezone": spark.conf.get("spark.sql.session.timeZone"),
            "spark_aqe_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
            "cluster_nodes": 1, "note": "Local mode: one driver process, four local execution threads; no remote executor nodes.",
        })
        assert all(assertions.values()), assertions
        print(f"Spark verified {total:,} real sample records; Parquet round-trip and timezone checks passed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
