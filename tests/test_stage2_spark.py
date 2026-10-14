"""Synthetic boundary regressions for the actual Stage 2 cleaning rules."""
import sys
import tempfile
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from pyspark.sql import functions as F, types as T
from common import RAW_COLUMNS, ROOT
from stage2_common import aggregate_candidates, canonical_observations, parse_raw, spark_session, write_json
from stage2_quality import span_slots


class Stage2BoundaryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session("rainfall-stage2-boundary-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def raw(self, *overrides):
        base = dict(date="2020-01-01", timestamp="2020-01-01T00:05:00+08:00", update_timestamp="2020-01-01T00:10:00+08:00", station_id="S_TEST", station_name="Test", station_device_id="D_TEST", location_longitude="103.8", location_latitude="1.3", reading_update_timestamp="2020-01-01T00:10:00+08:00", reading_value="0.2", reading_type="TB1 Rainfall 5 Minute Total F", reading_unit="mm", _corrupt_record=None, source_year=2020)
        schema = T.StructType([T.StructField(c, T.StringType(), True) for c in RAW_COLUMNS + ["_corrupt_record"]] + [T.StructField("source_year", T.IntegerType(), False)])
        return parse_raw(self.spark.createDataFrame([{**base, **o} for o in overrides], schema))

    def test_one_second_alignment_and_cross_year(self):
        rows = self.raw({}, {"timestamp": "2019-12-31T23:59:59+08:00", "source_year": 2019}, {"timestamp": "2020-01-01T00:04:58+08:00"})
        result = rows.select("time_adjust_seconds", "is_valid_candidate", F.date_format("observation_time", "yyyy-MM-dd HH:mm:ss").alias("normalized")).collect()
        self.assertEqual((result[0].time_adjust_seconds, result[0].is_valid_candidate), (0, True))
        self.assertEqual((result[1].time_adjust_seconds, result[1].normalized), (1, "2020-01-01 00:00:00"))
        self.assertFalse(result[2].is_valid_candidate)

    def test_duplicate_collision_and_exact_rainfall_ledger(self):
        parsed = self.raw({}, {}, {"timestamp": "2020-01-01T00:04:59+08:00"})
        groups = aggregate_candidates(parsed)
        row = canonical_observations(groups).first()
        self.assertEqual(row.source_record_count, 3)
        self.assertEqual(row.exact_duplicate_rows_removed, 1)
        self.assertEqual(row.compatible_variant_rows_removed, 1)
        self.assertEqual(row.time_adjust_seconds, 0)
        self.assertEqual(row.rainfall_mm, Decimal("0.2"))
        group = groups.first()
        self.assertEqual(group.source_rainfall_sum_mm, Decimal("0.6"))
        self.assertEqual(group.source_rainfall_sum_mm - row.rainfall_mm, Decimal("0.4"))

    def test_conflicting_values_and_locations_quarantine_all(self):
        values = aggregate_candidates(self.raw({}, {"reading_value": "0.4"}))
        self.assertTrue(values.first().conflict)
        self.assertEqual(canonical_observations(values).count(), 0)
        locations = aggregate_candidates(self.raw({}, {"location_latitude": "1.4"}))
        self.assertTrue(locations.first().conflict)
        self.assertEqual(canonical_observations(locations).count(), 0)

    def test_invalid_rainfall_and_metadata_flags(self):
        parsed = self.raw(*[{"reading_value": v} for v in [None, "NaN", "Infinity", "-0.2", "not-a-number", "0.1234567891"]], {"reading_unit": "cm"}, {"timestamp": "2020-01-01T00:05:00Z"}, {"_corrupt_record": "bad CSV"}, {"station_id": ""})
        self.assertEqual(parsed.filter("is_valid_candidate").count(), 0)
        metadata = self.raw({"location_latitude": None, "update_timestamp": "bad"}).first()
        self.assertTrue(metadata.is_valid_candidate)
        self.assertIn("invalid_coordinates", metadata.quality_flags)
        self.assertIn("update_time_unparsed", metadata.quality_flags)

    def test_precision_and_extreme_retention(self):
        parsed = self.raw({"reading_value": "0.123456789"}, {"timestamp": "2020-01-01T00:10:00+08:00", "reading_value": "0.1234567890"}, {"timestamp": "2020-01-01T00:15:00+08:00", "reading_value": "25"})
        clean = canonical_observations(aggregate_candidates(parsed))
        self.assertEqual(clean.count(), 3)
        values = [r.rainfall_mm for r in clean.orderBy("observation_time").collect()]
        self.assertEqual(values, [Decimal("0.123456789"), Decimal("0.123456789"), Decimal("25")])
        self.assertEqual(clean.filter(F.array_contains("quality_flags", "extreme_rainfall_candidate")).count(), 1)

    def test_missing_slot_is_not_a_zero_reading(self):
        clean = canonical_observations(aggregate_candidates(self.raw({}, {"timestamp": "2020-01-01T00:15:00+08:00", "reading_value": "0"})))
        epochs = [r.epoch for r in clean.select(F.unix_timestamp("observation_time").alias("epoch")).orderBy("epoch").collect()]
        self.assertEqual(len(epochs), 2)
        self.assertEqual((epochs[1] - epochs[0]) // 300 - 1, 1)
        self.assertEqual(clean.filter(F.col("rainfall_mm") == 0).count(), 1)

    def test_daily_span_and_leap_day_denominators(self):
        frame = self.spark.sql("SELECT timestamp'2020-02-29 00:00:00' AS d, timestamp'2020-02-29 00:05:00' AS first, timestamp'2020-02-29 23:55:00' AS last")
        self.assertEqual(frame.select(span_slots(F.col("d"), F.col("first"), F.col("last")).alias("n")).first().n, 287)
        dates = self.spark.sql("SELECT explode(sequence(date'2020-01-01',date'2020-12-31', interval 1 day)) AS d")
        self.assertEqual(dates.count() * 288, 105408)
        outside = self.spark.sql("SELECT timestamp'2020-03-01 00:00:00' AS d, timestamp'2020-02-29 00:05:00' AS first, timestamp'2020-02-29 23:55:00' AS last")
        self.assertEqual(outside.select(span_slots(F.col("d"), F.col("first"), F.col("last")).alias("n")).first().n, 0)

    def test_empty_quarantine_preserves_readable_schema(self):
        empty = self.raw({}).filter("NOT is_valid_candidate")
        with tempfile.TemporaryDirectory(prefix="rainfall-stage2-empty-") as directory:
            empty.write.mode("overwrite").parquet(directory)
            restored = self.spark.read.parquet(directory)
            self.assertEqual(restored.count(), 0)
            self.assertEqual(restored.schema.simpleString(), empty.schema.simpleString())

    def test_duplicate_rainfall_accounting_preserves_nine_decimal_digits(self):
        frame = self.spark.sql("SELECT cast('0.123456789' as decimal(20,9)) AS rain, cast(3 as bigint) AS source_count")
        amount = frame.select((F.col("rain") * (F.col("source_count") - 1).cast("decimal(10,0)")).alias("removed")).first().removed
        self.assertEqual(amount, Decimal("0.246913578"))


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(Stage2BoundaryTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    write_json(ROOT / "artifacts/stage2/boundary_test_results.json", {"tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors), "passed": result.wasSuccessful()})
    sys.exit(0 if result.wasSuccessful() else 1)
