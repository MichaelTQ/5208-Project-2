"""A small real-data read/aggregate/write/read workflow; no labels or features."""
import csv
import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from common import ROOT, now_sgt, save_json, sha256_file
from stage2_common import spark_session, export_csv
from pyspark.sql import functions as F


def main():
    out = ROOT / "artifacts/stage4/smoke"
    out.mkdir(parents=True, exist_ok=True)
    target = ROOT / "artifacts/stage4/sample_verification.json"
    target.unlink(missing_ok=True)
    csv_path = ROOT / "data/handoff/stage2_sample.csv"
    oracle = defaultdict(lambda: {"observed_slots": 0, "wet_slots": 0, "rainfall_sum_mm": Decimal(0)})
    expected_keys = set()
    expected_values = {}
    with csv_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))  # The explicitly bounded 16,712-row sample only.
    for r in rows:
        dt = datetime.fromisoformat(r["observation_time"])
        assert dt.utcoffset().total_seconds() == 8 * 3600
        year = dt.astimezone(ZoneInfo("Asia/Singapore")).year
        assert year == int(r["observation_year"]) and int(dt.timestamp()) % 300 == 0
        key = r["station_id"], int(dt.timestamp())
        assert key not in expected_keys
        expected_keys.add(key)
        rain = Decimal(r["rainfall_mm"])
        assert rain == Decimal(r["reading_value"]) and rain >= 0
        expected_values[key] = rain, r["raw_hash"]
        s = oracle[(r["station_id"], year)]
        s["observed_slots"] += 1
        s["wet_slots"] += int(rain > 0)
        s["rainfall_sum_mm"] += rain
    spark = spark_session("A-real-handoff-sample-smoke")
    checks = {}
    try:
        sample = spark.read.parquet(str(ROOT / "data/handoff/stage2_sample"))
        fields = json.loads((ROOT / "artifacts/stage2/clean_schema.json").read_text())
        # Partition field order differs; types/names must remain the formal contract.
        actual_schema = {f.name: f.dataType.jsonValue() for f in sample.schema}
        formal_schema = {f["name"]: f["type"] for f in fields["fields"]}
        assert actual_schema == formal_schema
        assert str(sample.schema["rainfall_mm"].dataType) == "DecimalType(20,9)"
        actual_keys = {(r.station_id, r.epoch) for r in sample.select("station_id", F.unix_timestamp("observation_time").alias("epoch")).collect()}
        assert actual_keys == expected_keys and sample.count() == len(rows)
        actual_values = {(r.station_id, r.epoch): (r.rainfall_mm, r.raw_hash) for r in sample.select("station_id", F.unix_timestamp("observation_time").alias("epoch"), "rainfall_mm", "raw_hash").collect()}
        assert actual_values == expected_values
        checks["formal_schema_unique_grid_and_csv_keys"] = True
        summary = sample.groupBy("station_id", "observation_year").agg(F.count("*").alias("observed_slots"), F.sum((F.col("rainfall_mm") > 0).cast("long")).alias("wet_slots"), F.sum("rainfall_mm").alias("rainfall_sum_mm"))
        actual = {(r.station_id, r.observation_year): {k: r[k] for k in ["observed_slots", "wet_slots", "rainfall_sum_mm"]} for r in summary.collect()}
        assert actual == dict(oracle)
        checks["independent_decimal_csv_aggregation"] = True
        sample.write.mode("overwrite").parquet(str(out / "observations"))
        reloaded = spark.read.parquet(str(out / "observations")).select(*sample.columns)
        assert sample.exceptAll(reloaded).limit(1).count() == 0 and reloaded.exceptAll(sample).limit(1).count() == 0
        checks["all_fields_parquet_roundtrip"] = True
        export_csv(summary.orderBy("station_id", "observation_year"), out / "station_year_summary.csv")
        with (out / "station_year_summary.csv").open(newline="") as handle:
            exported = {(r["station_id"], int(r["observation_year"])): {"observed_slots": int(r["observed_slots"]), "wet_slots": int(r["wet_slots"]), "rainfall_sum_mm": Decimal(r["rainfall_sum_mm"])} for r in csv.DictReader(handle)}
        assert exported == dict(oracle)
        assert set(y for s, y in oracle) == set(range(2017, 2025))
        assert 0 < sum(x["wet_slots"] for x in oracle.values()) < len(rows)
        checks["csv_roundtrip_eight_years_wet_and_dry"] = True
    finally:
        spark.stop()
    save_json(target, {"status": "PASS", "checked_at_sgt": now_sgt(), "scope": "A handoff sample only; not representative full data; no model pipeline", "rows": len(rows), "station_year_groups": len(oracle), "source_csv_sha256": sha256_file(csv_path), "summary_csv_sha256": sha256_file(out / "station_year_summary.csv"), "rainfall_sum_mm": str(sum((x["rainfall_sum_mm"] for x in oracle.values()), Decimal(0))), "checks": checks})
    print(f"Real handoff sample PASS: {len(rows):,} rows, {len(oracle)} station-year groups", flush=True)


if __name__ == "__main__":
    main()
