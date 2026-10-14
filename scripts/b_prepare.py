"""Build B's compact station-time table and training-set threshold baselines."""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from b_windows import THRESHOLDS_MM, add_thresholds, add_windows


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", action="store_true", help="Use A's small handoff sample")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("rainfall-B-training-table")
             .master("local[4]").config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "64")
             .config("spark.sql.session.timeZone", "Asia/Singapore").getOrCreate())
    try:
        source = ROOT / ("data/handoff/stage2_sample" if args.sample else "data/clean/observations")
        output = ROOT / ("data/b_sample" if args.sample else "data/b_model")
        obs = spark.read.parquet(str(source)).select("station_id", "observation_time", "rainfall_mm")
        examples = add_windows(obs).filter("label_usable AND features_usable")
        # Save one row per station-time. C can use add_thresholds() during training.
        examples.write.mode("overwrite").partitionBy("split").parquet(str(output / "examples"))
        examples = spark.read.parquet(str(output / "examples"))
        expanded = add_thresholds(examples)
        summary = (expanded.groupBy("split", "q_mm")
                   .agg(F.count("*").alias("examples"), F.sum("label").alias("positive"))
                   .withColumn("positive_rate", F.col("positive") / F.col("examples"))
                   .orderBy("split", "q_mm"))
        summary.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output / "label_counts"))
        train_rates = (summary.filter(F.col("split") == "train")
                       .select("q_mm", F.col("positive_rate").alias("baseline_probability")))
        train_rates.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output / "baseline"))
        # Brier score of the constant training-rate baseline on each split.
        evaluated = (summary.join(train_rates, "q_mm")
                     .withColumn("brier_score",
                                 F.col("positive_rate") * (1 - F.col("baseline_probability")) ** 2 +
                                 (1 - F.col("positive_rate")) * F.col("baseline_probability") ** 2)
                     .orderBy("split", "q_mm"))
        evaluated.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output / "baseline_evaluation"))
        print(f"Saved examples to {output / 'examples'}")
        print("Thresholds (mm):", ", ".join(THRESHOLDS_MM))
        summary.show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
