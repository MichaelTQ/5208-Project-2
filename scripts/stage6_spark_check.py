"""Compute diagnostic fixture results by exact Spark joins, independently of oracle."""
import csv
import json
from pyspark.sql import functions as F, types as T, Window

from common import ROOT, now_sgt, save_json, sha256_file
from stage2_common import spark_session
from window_audit import compare_answers


def main():
    out = ROOT / "artifacts/stage6"
    target = out / "spark_verification.json"
    target.unlink(missing_ok=True)
    cfg = json.loads((ROOT / "config/window_audit.json").read_text())
    fixture = ROOT / "data/handoff/stage6_window_checks"
    spark = spark_session("A-independent-window-fixture-check")
    evidence, comparisons = {}, []
    try:
        def split_expr(column):
            result = F.lit(None).cast("string")
            for s in cfg["splits"]:
                result = F.when((column >= F.to_timestamp(F.lit(s["start"]))) & (column < F.to_timestamp(F.lit(s["end_exclusive"]))), F.lit(s["name"])).otherwise(result)
            return result

        for prefix in ["synthetic", "real"]:
            raw = spark.read.option("header", True).csv(str(fixture / f"{prefix}_observations.csv"))
            obs = raw.select("station_id", F.to_timestamp("observation_time").alias("observation_time"), F.col("rainfall_mm").cast("decimal(20,9)").alias("rainfall_mm"))
            q = spark.read.option("header", True).csv(str(fixture / f"{prefix}_queries.csv")).select("query_id", "station_id", F.to_timestamp("prediction_time").alias("t"), F.col("q_mm").cast("decimal(20,9)").alias("q"), F.col("history_slots").cast("int").alias("h"))
            future_slots = q.withColumn("offset", F.explode(F.sequence(F.lit(1), F.lit(6)))).withColumn("expected_time", F.expr("timestamp_seconds(unix_timestamp(t) + offset * 300)"))
            future = future_slots.join(obs, (future_slots.station_id == obs.station_id) & (future_slots.expected_time == obs.observation_time), "left").select(future_slots["*"], obs.rainfall_mm)
            future = future.withColumn("same_split", (split_expr(F.col("t")) == split_expr(F.col("expected_time"))) & split_expr(F.col("t")).isNotNull())
            grouped = future.groupBy("query_id").agg(F.count("rainfall_mm").alias("observed"), F.sum("rainfall_mm").alias("partial_sum"), F.min(F.coalesce(F.col("same_split"), F.lit(False)).cast("int")).alias("within"), F.sort_array(F.collect_list(F.when(F.col("rainfall_mm").isNull(), F.date_format("expected_time", "yyyy-MM-dd'T'HH:mm:ssXXX")))).alias("missing_future"))
            history_slots = q.withColumn("offset", F.explode(F.sequence(F.lit(0), F.col("h") - 1))).withColumn("expected_time", F.expr("timestamp_seconds(unix_timestamp(t) - offset * 300)"))
            history = history_slots.join(obs, (history_slots.station_id == obs.station_id) & (history_slots.expected_time == obs.observation_time), "left").select(history_slots["*"], obs.rainfall_mm)
            historical = history.groupBy("query_id").agg(F.count("rainfall_mm").alias("history_observed"), F.sort_array(F.collect_list(F.when(F.col("rainfall_mm").isNull(), F.date_format("expected_time", "yyyy-MM-dd'T'HH:mm:ssXXX"))), asc=False).alias("missing_history"))
            anchors = q.join(obs, (q.station_id == obs.station_id) & (q.t == obs.observation_time), "left").select("query_id", obs.rainfall_mm.isNotNull().alias("anchor_observed"))
            joined = q.join(grouped, "query_id").join(historical, "query_id").join(anchors, "query_id").withColumn("split", split_expr(F.col("t"))).withColumn("time_str", F.date_format("t", "yyyy-MM-dd'T'HH:mm:ssXXX"))
            actual = []
            for r in joined.orderBy("query_id").collect():
                complete = r.observed == 6
                usable = complete and r.within == 1
                history_complete = r.history_observed == r.h
                actual.append({"query_id": r.query_id, "station_id": r.station_id, "prediction_time": r.time_str, "q_mm": format(r.q, ".9f"), "split": r.split, "anchor_observed": r.anchor_observed, "future_complete": complete, "missing_future_times": r.missing_future, "future_within_split": r.within == 1, "label_usable": usable, "future_sum_mm": format(r.partial_sum, ".9f") if complete else None, "label_exceeds_q": int(r.partial_sum > r.q) if usable else None, "history_slots": r.h, "history_complete": history_complete, "missing_history_times": r.missing_history, "example_evaluation_usable": usable and history_complete})
            expected = json.loads((fixture / f"{prefix}_expected.json").read_text())
            mismatches = compare_answers(expected, actual)
            assert not mismatches, mismatches[:3]
            save_json(out / f"{prefix}_spark_answers.json", actual)
            evidence[prefix] = {"queries": len(actual), "fields_per_query": len(actual[0]), "all_exact_join_answers_match_oracle": True}
            comparisons.append({"scope": prefix, "mismatches": len(mismatches)})
            if prefix == "synthetic":
                # A deliberately wrong next-six-rows construction counts the gap case.
                w = Window.partitionBy("station_id").orderBy("observation_time").rowsBetween(1, 6)
                bad = obs.withColumn("next_rows", F.count("rainfall_mm").over(w)).withColumn("next_row_sum", F.sum("rainfall_mm").over(w))
                gap = bad.filter((F.col("station_id") == "SYN_GAP_SIX_ROWS") & (F.col("observation_time") == F.to_timestamp(F.lit("2022-03-10T12:00:00+08:00")))).first()
                assert gap.next_rows == 6 and str(gap.next_row_sum) == "1.200000000"
                evidence["six_rows_counterexample"] = {"counted_rows": gap.next_rows, "wrong_sum_mm": str(gap.next_row_sum), "oracle_future_complete": False, "missing_slot": "2022-03-10T12:15:00+08:00", "sixth_later_row_is_t_plus_35_minutes": True}
    finally:
        spark.stop()
    save_json(target, {"status": "PASS", "checked_at_sgt": now_sgt(), "scope": "A fixture/oracle cross-check only; B/C implementation not delivered", "evidence": evidence, "script_sha256": sha256_file(ROOT / "scripts/stage6_spark_check.py"), "oracle_sha256": sha256_file(ROOT / "scripts/window_audit.py"), "config_sha256": sha256_file(ROOT / "config/window_audit.json")})
    print("Independent Spark exact-time joins PASS: all synthetic/real answers match; next-six-rows bug demonstrated")


if __name__ == "__main__":
    main()
