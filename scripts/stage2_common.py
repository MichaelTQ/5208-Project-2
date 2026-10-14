"""Spark helpers shared by Stage 2 production and independent verification."""
import csv
import json
import shutil
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T

from common import ROOT, RAW_COLUMNS, config


def stage2_config():
    return json.loads((ROOT / "config/stage2.json").read_text())


def spark_session(name):
    cfg, s2 = config(), stage2_config()
    spark = (SparkSession.builder.appName(name).master(cfg["spark_master"])
             .config("spark.driver.memory", cfg["driver_memory"])
             .config("spark.sql.shuffle.partitions", s2["shuffle_partitions"])
             .config("spark.sql.session.timeZone", cfg["timezone"])
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.csv.parser.columnPruning.enabled", "false")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .config("spark.sql.files.maxRecordsPerFile", s2["max_records_per_file"])
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def serializable(value):
    if hasattr(value, "asDict"):
        return serializable(value.asDict())
    if isinstance(value, dict):
        return {k: serializable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [serializable(v) for v in value]
    if isinstance(value, (Decimal, datetime, date)):
        return str(value)
    return value


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(serializable(value), ensure_ascii=False, indent=2) + "\n")


def export_csv(df, path):
    """Export small/aggregated tables without collecting full observations."""
    path = Path(path)
    directory = path.with_name(path.name + ".spark-output")
    for field in df.schema:
        if isinstance(field.dataType, T.TimestampType):
            df = df.withColumn(field.name, F.date_format(field.name, "yyyy-MM-dd'T'HH:mm:ssXXX"))
        elif isinstance(field.dataType, (T.ArrayType, T.StructType, T.MapType)):
            df = df.withColumn(field.name, F.to_json(field.name))
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(directory))
    part = next(directory.glob("part-*.csv"))
    path.parent.mkdir(parents=True, exist_ok=True)
    part.replace(path)
    shutil.rmtree(directory)


def flags(*pairs):
    return F.filter(F.array(*[F.when(condition, F.lit(label)) for condition, label in pairs]), lambda x: x.isNotNull())


def parse_raw(df):
    s2 = stage2_config()
    decimal_type = T.DecimalType(s2["rainfall_decimal_precision"], s2["rainfall_decimal_scale"])
    df = (df.withColumn("raw_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in RAW_COLUMNS], "source_year"), options={"ignoreNullFields": "false"}), 256))
          .withColumn("raw_observation_time", F.try_to_timestamp("timestamp"))
          .withColumn("parsed_update_time", F.try_to_timestamp("update_timestamp"))
          .withColumn("parsed_reading_update_time", F.try_to_timestamp("reading_update_timestamp"))
          .withColumn("rainfall_mm", F.col("reading_value").cast(decimal_type))
          .withColumn("rainfall_as_double", F.col("reading_value").cast("double"))
          .withColumn("longitude", F.col("location_longitude").cast("double"))
          .withColumn("latitude", F.col("location_latitude").cast("double"))
          .withColumn("grid_remainder_seconds", F.pmod(F.unix_timestamp("raw_observation_time"), F.lit(300)))
          .withColumn("time_adjust_seconds", F.when(F.col("grid_remainder_seconds") == 0, 0).when(F.col("grid_remainder_seconds") == 299, 1))
          .withColumn("observation_time", F.expr("timestamp_seconds(unix_timestamp(raw_observation_time) + time_adjust_seconds)")))
    timestamp_shape = F.col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+08:00$")
    numeric_shape = F.col("reading_value").rlike(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
    excess_precision = F.regexp_extract("reading_value", r"\.\d{9}(\d+)$", 1).rlike("[1-9]")
    invalid_rain = (F.col("rainfall_mm").isNull() | F.col("rainfall_as_double").isNull() | F.isnan("rainfall_as_double") | (F.abs("rainfall_as_double") == float("inf")) | ~F.coalesce(numeric_shape, F.lit(False)))
    coordinates_valid = F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180)
    df = df.withColumn("invalid_reasons", flags(
        (F.col("_corrupt_record").isNotNull(), "malformed_csv"),
        (F.col("station_id").isNull() | (F.trim("station_id") == ""), "missing_station_id"),
        (F.col("raw_observation_time").isNull(), "invalid_observation_time"),
        (~F.coalesce(timestamp_shape, F.lit(False)), "unexpected_timestamp_format_or_timezone"),
        (F.col("raw_observation_time").isNotNull() & F.col("time_adjust_seconds").isNull(), "unsupported_time_offset"),
        (invalid_rain, "invalid_or_nonfinite_rainfall"),
        (F.coalesce(excess_precision, F.lit(False)), "excess_rainfall_precision"),
        (F.col("rainfall_mm") < 0, "negative_rainfall"),
        (~F.coalesce(F.col("reading_unit") == "mm", F.lit(False)), "unexpected_unit"),
        (~F.coalesce(F.col("reading_type") == "TB1 Rainfall 5 Minute Total F", F.lit(False)), "unexpected_reading_type"),
    )).withColumn("quality_flags", flags(
        (F.col("time_adjust_seconds") == 1, "timestamp_shifted_forward_one_second"),
        (~F.coalesce(coordinates_valid, F.lit(False)), "invalid_coordinates"),
        (F.col("station_name").isNull() | (F.trim("station_name") == ""), "missing_station_name"),
        (F.col("parsed_update_time").isNull(), "update_time_unparsed"),
        (F.col("parsed_reading_update_time").isNull(), "reading_update_time_unparsed"),
        (F.col("parsed_update_time") < F.col("raw_observation_time"), "update_before_observation"),
        (F.col("parsed_reading_update_time") < F.col("raw_observation_time"), "reading_update_before_observation"),
        (F.to_date("date") != F.to_date("raw_observation_time"), "source_date_mismatch"),
        (F.year("raw_observation_time") != F.col("source_year"), "source_year_mismatch"),
        (F.col("rainfall_mm") >= s2["extreme_review_threshold_mm_per_5min"], "extreme_rainfall_candidate"),
    )).withColumn("is_valid_candidate", F.size("invalid_reasons") == 0)
    return (df.withColumn("location_key", F.when(coordinates_valid, F.concat_ws(",", F.col("latitude").cast("decimal(12,8)").cast("string"), F.col("longitude").cast("decimal(12,8)").cast("string"))))
            .withColumn("update_delay_seconds", F.unix_timestamp("parsed_update_time") - F.unix_timestamp("raw_observation_time"))
            .withColumn("reading_update_delay_seconds", F.unix_timestamp("parsed_reading_update_time") - F.unix_timestamp("raw_observation_time")))


def aggregate_candidates(df):
    """One deterministic representative; all conflicting groups are quarantined."""
    payload_columns = [c for c in df.columns if c not in {"station_id", "observation_time"}]
    payload = F.struct("time_adjust_seconds", "timestamp", "raw_hash", *[F.col(c) for c in payload_columns if c not in {"time_adjust_seconds", "timestamp", "raw_hash"}])
    grouped = df.groupBy("station_id", "observation_time").agg(
        F.count("*").alias("source_record_count"), F.countDistinct("raw_hash").alias("distinct_raw_records"),
        F.min("rainfall_mm").alias("min_rainfall_mm"), F.max("rainfall_mm").alias("max_rainfall_mm"),
        F.sum("rainfall_mm").alias("source_rainfall_sum_mm"),
        F.min("location_key").alias("min_location_key"), F.max("location_key").alias("max_location_key"),
        F.sum((F.col("time_adjust_seconds") == 1).cast("long")).alias("adjusted_source_records"),
        F.min(payload).alias("representative"),
    )
    return grouped.withColumn("conflict", (F.col("min_rainfall_mm") != F.col("max_rainfall_mm")) | (~F.col("min_location_key").eqNullSafe(F.col("max_location_key"))))


def canonical_observations(grouped):
    df = grouped.filter(~F.col("conflict"))
    fields = [f.name for f in df.schema["representative"].dataType.fields]
    df = df.select("station_id", "observation_time", "source_record_count", "distinct_raw_records", "adjusted_source_records", *[F.col(f"representative.{c}").alias(c) for c in fields])
    return (df.withColumn("exact_duplicate_rows_removed", F.col("source_record_count") - F.col("distinct_raw_records"))
            .withColumn("compatible_variant_rows_removed", F.col("distinct_raw_records") - 1)
            .withColumn("quality_flags", F.concat("quality_flags", flags(
                (F.col("source_record_count") > 1, "compatible_duplicate_group"),
                (F.col("distinct_raw_records") > 1, "compatible_raw_variant_group"),
            )))
            .withColumn("interval_start_assumed", F.expr("observation_time - INTERVAL 5 MINUTES"))
            .withColumn("interval_end_assumed", F.col("observation_time"))
            .withColumn("interval_semantics", F.lit(stage2_config()["interval_semantics"]))
            .withColumn("observation_year", F.year("observation_time"))
            .withColumn("observation_month", F.month("observation_time"))
            .withColumn("observation_date", F.to_date("observation_time"))
            .drop("_corrupt_record", "rainfall_as_double", "is_valid_candidate", "invalid_reasons"))
