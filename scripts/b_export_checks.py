"""Export A's small window queries using Spark exact-time joins.

This is only a diagnostic adapter. The full training table uses b_windows.py.
"""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]
CHECKS = ROOT / "data/handoff/stage6_window_checks"


def split_name(t):
    return (F.when((t >= F.lit("2017-01-01").cast("timestamp")) &
                   (t < F.lit("2023-01-01").cast("timestamp")), "train")
            .when((t >= F.lit("2023-01-01").cast("timestamp")) &
                  (t < F.lit("2024-01-01").cast("timestamp")), "validation")
            .when((t >= F.lit("2024-01-01").cast("timestamp")) &
                  (t < F.lit("2025-01-01").cast("timestamp")), "test"))


def export(spark, kind):
    observations = (spark.read.option("header", True).csv(str(CHECKS / f"{kind}_observations.csv"))
                    .select("station_id", F.to_timestamp("observation_time").alias("obs_time"),
                            F.col("rainfall_mm").cast("decimal(20,9)").alias("rain_mm")))
    queries = (spark.read.option("header", True).csv(str(CHECKS / f"{kind}_queries.csv"))
               .select("query_id", "station_id", F.to_timestamp("prediction_time").alias("t"),
                       F.col("q_mm").cast("decimal(20,9)").alias("q_mm"),
                       F.col("history_slots").cast("integer").alias("history_slots")))
    # The supplied checks use the same 12-slot history as our model features.
    assert all(row.history_slots == 12 for row in queries.select("history_slots").distinct().collect())

    result = queries
    for k in range(-11, 7):
        name = f"r_{'m' + str(-k) if k < 0 else 'p' + str(k)}"
        at = F.expr(f"t + INTERVAL {k * 5} MINUTES")
        lookup = observations.select(F.col("station_id").alias("lookup_station"),
                                     F.col("obs_time").alias("lookup_time"),
                                     F.col("rain_mm").alias(name))
        result = (result.join(lookup, (F.col("station_id") == F.col("lookup_station")) &
                              (at == F.col("lookup_time")), "left")
                  .drop("lookup_station", "lookup_time"))

    future = [F.col(f"r_p{k}") for k in range(1, 7)]
    history = [F.col("r_p0")] + [F.col(f"r_m{k}") for k in range(1, 12)]
    result = (result.withColumn("future_complete", F.lit(True))
              .withColumn("history_complete", F.lit(True)))
    for value in future:
        result = result.withColumn("future_complete", F.col("future_complete") & value.isNotNull())
    for value in history:
        result = result.withColumn("history_complete", F.col("history_complete") & value.isNotNull())

    zero = F.lit("0").cast("decimal(30,9)")
    total = zero
    for value in future:
        total = total + F.coalesce(value, zero)
    result = result.withColumn("future_sum_mm", F.when(F.col("future_complete"), total))
    result = result.withColumn("split", split_name(F.col("t")))
    end = (F.when(F.col("split") == "train", F.lit("2023-01-01").cast("timestamp"))
           .when(F.col("split") == "validation", F.lit("2024-01-01").cast("timestamp"))
           .when(F.col("split") == "test", F.lit("2025-01-01").cast("timestamp")))
    within = F.coalesce((F.col("t") + F.expr("INTERVAL 30 MINUTES")) < end, F.lit(False))
    result = (result.withColumn("future_within_split", within)
              .withColumn("label_usable", F.col("future_complete") & within)
              .withColumn("label_exceeds_q", F.when(F.col("label_usable"),
                          (F.col("future_sum_mm") > F.col("q_mm")).cast("integer"))))

    fmt = "yyyy-MM-dd'T'HH:mm:ssXXX"
    missing_future = [F.when(F.col(f"r_p{k}").isNull(),
                             F.date_format(F.expr(f"t + INTERVAL {5 * k} MINUTES"), fmt))
                      for k in range(1, 7)]
    missing_history = [F.when(F.col("r_p0").isNull(), F.date_format("t", fmt))]
    missing_history += [F.when(F.col(f"r_m{k}").isNull(),
                               F.date_format(F.expr(f"t - INTERVAL {5 * k} MINUTES"), fmt))
                        for k in range(1, 12)]
    result = (result.withColumn("missing_future_times",
                                F.filter(F.array(*missing_future), lambda x: x.isNotNull()))
              .withColumn("missing_history_times",
                          F.filter(F.array(*missing_history), lambda x: x.isNotNull()))
              .withColumn("prediction_time_text", F.date_format("t", fmt))
              .select("query_id", "station_id", "prediction_time_text", "q_mm", "split", "r_p0",
                      "future_complete", "missing_future_times", "future_within_split",
                      "label_usable", "future_sum_mm", "label_exceeds_q", "history_slots",
                      "history_complete", "missing_history_times"))

    answers = []
    for r in result.collect():
        answers.append({
            "query_id": r.query_id, "station_id": r.station_id,
            "prediction_time": r.prediction_time_text,
            "q_mm": format(r.q_mm, ".9f"), "split": r.split,
            "anchor_observed": r.r_p0 is not None,
            "future_complete": r.future_complete,
            "missing_future_times": r.missing_future_times,
            "future_within_split": r.future_within_split,
            "label_usable": r.label_usable,
            "future_sum_mm": format(r.future_sum_mm, ".9f") if r.future_sum_mm is not None else None,
            "label_exceeds_q": r.label_exceeds_q,
            "history_slots": r.history_slots,
            "history_complete": r.history_complete,
            "missing_history_times": r.missing_history_times,
            "example_evaluation_usable": r.label_usable and r.history_complete,
        })
    out = ROOT / "artifacts/b/audit"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{kind}_actual.json"
    path.write_text(json.dumps(answers, indent=2) + "\n")
    print(f"Wrote {len(answers)} {kind} answers to {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=["synthetic", "real"])
    args = parser.parse_args()
    spark = (SparkSession.builder.master("local[4]").appName("B-window-audit-export")
             .config("spark.sql.session.timeZone", "Asia/Singapore").getOrCreate())
    try:
        export(spark, args.kind)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
