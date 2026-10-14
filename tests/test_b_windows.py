"""Compare B's real window code with A's known-answer edge cases."""

import json
import sys
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from b_windows import add_thresholds, add_windows  # noqa: E402


def main():
    spark = (SparkSession.builder.master("local[4]").appName("B-window-test")
             .config("spark.sql.session.timeZone", "Asia/Singapore").getOrCreate())
    try:
        folder = ROOT / "data/handoff/stage6_window_checks"
        total = 0
        for kind in ("synthetic", "real"):
            obs = (spark.read.option("header", True).csv(str(folder / f"{kind}_observations.csv"))
                   .select("station_id", F.to_timestamp("observation_time").alias("observation_time"),
                           F.col("rainfall_mm").cast("decimal(20,9)").alias("rainfall_mm")))
            queries = (spark.read.option("header", True).csv(str(folder / f"{kind}_queries.csv"))
                       .select("query_id", "station_id",
                               F.to_timestamp("prediction_time").alias("observation_time"),
                               F.col("q_mm").cast("decimal(20,9)").alias("q_mm")))
            actual = (queries.join(add_windows(obs), ["station_id", "observation_time"], "left")
                      .withColumn("label", F.when(F.col("label_usable"),
                                  (F.col("future_30_mm") > F.col("q_mm")).cast("integer")))
                      .select("query_id", "station_id", "future_complete", "future_30_mm",
                              "label_usable", "features_usable", "split", "label")
                      .collect())
            expected = {r["query_id"]: r for r in json.loads((folder / f"{kind}_expected.json").read_text())}
            assert len(actual) == len(expected)
            for row in actual:
                e = expected[row.query_id]
                if not e["anchor_observed"]:
                    assert row.future_complete is None
                    continue
                assert row.future_complete == e["future_complete"], row.query_id
                assert row.label_usable == e["label_usable"], row.query_id
                assert row.features_usable == e["history_complete"], row.query_id
                assert row.split == e["split"], row.query_id
                assert row.label == e["label_exceeds_q"], row.query_id
                if e["future_sum_mm"] is None:
                    assert row.future_30_mm is None, row.query_id
                else:
                    assert row.future_30_mm == Decimal(e["future_sum_mm"]), row.query_id
                total += 1
        print(f"PASS: {total} observed-anchor window queries match A's known answers")
        for bad in ([], ["-1"], ["NaN"], ["0.0000000001"]):
            try:
                add_thresholds(None, bad)
            except ValueError:
                pass
            else:
                raise AssertionError(f"Invalid threshold accepted: {bad}")
        print("PASS: empty, negative, nonfinite and over-precise thresholds rejected")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
