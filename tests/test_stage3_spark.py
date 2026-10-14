"""Known numeric examples verify analytical denominators and calendar support."""
import sys
import unittest
from decimal import Decimal
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from pyspark.sql import functions as F
from common import ROOT
from stage2_common import spark_session, write_json
from stage3_analyze import summarize, annual_equal_weight
from stage3_support_probe import matched_support


class Stage3Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session("stage3-boundary-tests")
        cls.cfg = {"extreme_sensitivity_threshold_mm_per_5min": 20, "wet_quantile_probabilities": [.5, .9, .99], "wet_quantile_accuracy": 10000}

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def observations(self, rows):
        df = self.spark.createDataFrame(rows, ["station_id", "time", "rain"])
        return df.select("station_id", F.to_timestamp("time").alias("observation_time"), F.col("rain").cast("decimal(20,9)").alias("rainfall_mm")).withColumn("observation_year", F.year("observation_time"))

    def test_equal_station_weights_are_not_record_weights(self):
        obs = self.observations([("A", "2020-01-01 00:00:00", "0"), ("A", "2020-01-01 00:05:00", "0"), ("B", "2020-01-01 00:00:00", "6")])
        stations = summarize(obs, ["station_id", "observation_year"], self.cfg)
        self.assertEqual(annual_equal_weight(stations, "test").first().equal_station_rainfall_rate, Decimal("3"))
        self.assertEqual(summarize(obs, ["observation_year"], self.cfg).first().rainfall_rate_mm_per_5min, Decimal("2"))

    def test_missing_is_not_added_to_observed_denominator(self):
        obs = self.observations([("A", "2020-01-01 00:00:00", "0"), ("A", "2020-01-01 00:10:00", "1")])
        stats = summarize(obs, ["station_id"], self.cfg).first()
        self.assertEqual(stats.observed_slots, 2)
        self.assertEqual(stats.rainfall_rate_mm_per_5min, Decimal("0.5"))
        self.assertEqual(stats.wet_rate, .5)

    def test_extreme_exclusion_changes_the_denominator(self):
        obs = self.observations([("A", "2020-01-01 00:00:00", "0"), ("A", "2020-01-01 00:05:00", "1"), ("A", "2020-01-01 00:10:00", "25")])
        stats = summarize(obs, ["station_id"], self.cfg).first()
        self.assertEqual(stats.rainfall_sum_mm, Decimal("26"))
        self.assertEqual(stats.non_extreme_slots, 2)
        self.assertEqual(stats.rainfall_rate_without_extremes, Decimal("0.5"))
        self.assertEqual(stats.wet_rate_without_extremes, .5)
        self.assertEqual(obs.count(), 3)

    def test_nine_digit_precision_is_preserved(self):
        obs = self.observations([("A", "2020-01-01 00:00:00", "0.123456789"), ("A", "2020-01-01 00:05:00", "0.123456789")])
        stats = summarize(obs, ["station_id"], self.cfg).first()
        self.assertEqual(stats.rainfall_sum_mm, Decimal("0.246913578"))
        self.assertEqual(stats.rainfall_rate_mm_per_5min, Decimal("0.123456789"))

    def test_wet_quantiles_exclude_zero_and_dry_groups_remain_null(self):
        obs = self.observations([("A", "2020-01-01 00:00:00", "0"), ("A", "2020-01-01 00:05:00", "1"), ("A", "2020-01-01 00:10:00", "2"), ("A", "2020-01-01 00:15:00", "3"), ("B", "2020-01-01 00:00:00", "0")])
        stats = {r.station_id: r for r in summarize(obs, ["station_id"], self.cfg).collect()}
        self.assertEqual(stats["A"].wet_p50_mm, Decimal("2"))
        self.assertEqual(stats["A"].wet_p99_mm, Decimal("3"))
        self.assertIsNone(stats["B"].wet_p50_mm)
        self.assertEqual(stats["B"].rainfall_sum_mm, Decimal("0"))

    def test_leap_day_and_incomplete_calendar_keys(self):
        obs = self.observations([("A", "2020-02-28 00:00:00", "1"), ("A", "2021-02-28 00:00:00", "2"), ("A", "2020-02-29 00:00:00", "9"), ("A", "2020-03-01 00:00:00", "3"), ("B", "2020-02-28 00:00:00", "1"), ("B", "2021-02-28 00:00:00", "2")])
        keyed = obs.filter(F.date_format("observation_time", "MM-dd") != "02-29").withColumn("calendar_key", F.date_format("observation_time", "MM-dd HH:mm"))
        support = matched_support(keyed, [2020, 2021])
        self.assertEqual({(r.station_id, r.calendar_key) for r in support.collect()}, {("A", "02-28 00:00"), ("B", "02-28 00:00")})
        self.assertEqual(keyed.join(support, ["station_id", "calendar_key"]).count(), 4)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Stage3Tests))
    write_json(ROOT / "artifacts/stage3/boundary_test_results.json", {"tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors), "passed": result.wasSuccessful()})
    sys.exit(0 if result.wasSuccessful() else 1)
