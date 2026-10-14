"""Known-answer cases for the independent A cross-check oracle and export CLI."""
import copy
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from common import ROOT, save_json, sha256_file
from window_audit import amount, compare_answers, expected_answer, grid_time, load_observations, oracle_answers


class WindowAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = ROOT / "data/handoff/stage6_window_checks"
        cls.answers = oracle_answers(cls.fixture / "synthetic_observations.csv", cls.fixture / "synthetic_queries.csv")
        cls.by_id = {r["query_id"]: r for r in cls.answers}

    def test_strict_greater_equal_and_one_decimal_step(self):
        self.assertEqual(self.by_id["SYN_EQUAL"]["future_sum_mm"], "1.200000000")
        self.assertEqual(self.by_id["SYN_EQUAL"]["label_exceeds_q"], 0)
        self.assertEqual(self.by_id["SYN_ABOVE"]["label_exceeds_q"], 1)
        self.assertEqual(self.by_id["SYN_BELOW"]["label_exceeds_q"], 0)

    def test_observed_zero_is_valid_dry_target(self):
        r = self.by_id["SYN_DRY"]
        self.assertTrue(r["future_complete"] and r["label_usable"])
        self.assertEqual(r["future_sum_mm"], "0.000000000")
        self.assertEqual(r["label_exceeds_q"], 0)

    def test_missing_slot_with_six_later_rows_is_not_a_label(self):
        r = self.by_id["SYN_GAP_SIX_ROWS"]
        self.assertFalse(r["future_complete"])
        self.assertEqual(r["missing_future_times"], ["2022-03-10T12:15:00+08:00"])
        self.assertIsNone(r["future_sum_mm"])
        self.assertIsNone(r["label_exceeds_q"])
        index = load_observations(self.fixture / "synthetic_observations.csv")
        t = grid_time(r["prediction_time"])
        later = sorted((time, rain) for (s, time), rain in index.items() if s == r["station_id"] and time > t)
        self.assertGreaterEqual(len(later), 6)
        self.assertEqual(later[5][0], t + timedelta(minutes=35))

    def test_history_gap_does_not_impute_or_erase_complete_target(self):
        r = self.by_id["SYN_HISTORY_GAP"]
        self.assertTrue(r["label_usable"])
        self.assertFalse(r["history_complete"] or r["example_evaluation_usable"])
        self.assertEqual(r["missing_history_times"], ["2022-03-10T11:50:00+08:00"])

    def test_t_reading_is_excluded(self):
        r = self.by_id["SYN_EXCLUDE_T"]
        self.assertEqual(r["future_sum_mm"], "0.000000000")
        self.assertEqual(r["label_exceeds_q"], 0)

    def test_nine_decimal_precision_sum(self):
        r = self.by_id["SYN_NINE_DECIMALS"]
        self.assertEqual(r["future_sum_mm"], "0.740740734")
        self.assertEqual(r["label_exceeds_q"], 1)

    def test_cross_day_leap_day_and_training_year_are_allowed(self):
        for qid in ["SYN_MIDNIGHT", "SYN_LEAP_DAY", "SYN_TRAIN_YEAR"]:
            with self.subTest(qid=qid):
                self.assertTrue(self.by_id[qid]["future_complete"] and self.by_id[qid]["label_usable"])

    def test_exact_split_boundary_and_study_end(self):
        self.assertTrue(self.by_id["SYN_TRAIN_LAST_OK"]["label_usable"])
        for qid in ["SYN_TRAIN_CROSS", "SYN_VALIDATION_CROSS", "SYN_TEST_END", "SYN_OUTSIDE_SPLIT"]:
            with self.subTest(qid=qid):
                r = self.by_id[qid]
                self.assertTrue(r["future_complete"])
                self.assertFalse(r["future_within_split"] or r["label_usable"])
                self.assertIsNone(r["label_exceeds_q"])

    def test_historical_values_before_validation_are_permitted(self):
        r = self.by_id["SYN_VAL_HISTORY_TRAIN"]
        self.assertEqual(r["split"], "validation")
        self.assertTrue(r["history_complete"] and r["label_usable"])

    def test_missing_anchor_and_end_of_data(self):
        r = self.by_id["SYN_NO_ANCHOR"]
        self.assertFalse(r["anchor_observed"] or r["history_complete"])
        self.assertTrue(r["label_usable"])
        self.assertFalse(self.by_id["SYN_LAST_RECORD"]["future_complete"])
        self.assertEqual(len(self.by_id["SYN_LAST_RECORD"]["missing_future_times"]), 6)

    def test_station_isolation_and_timezone_conversion(self):
        self.assertEqual(self.by_id["SYN_STATION_A"]["label_exceeds_q"], 0)
        self.assertEqual(self.by_id["SYN_STATION_B"]["future_sum_mm"], "6.000000000")
        self.assertEqual(grid_time("2022-03-10T04:00:00+00:00").isoformat(), "2022-03-10T12:00:00+08:00")

    def test_invalid_inputs_and_duplicate_observations_rejected(self):
        for value in ["-1", "NaN", "Infinity", "0.0000000001", "100000000000"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                amount(value)
        for value in ["2022-03-10T12:00:00", "2022-03-10T12:04:59+08:00"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                grid_time(value)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "duplicate.csv"
            p.write_text("station_id,observation_time,rainfall_mm\nA,2022-01-01T00:00:00+08:00,0\nA,2022-01-01T00:00:00+08:00,0\n")
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                load_observations(p)

    def test_export_mutations_and_float_rejected(self):
        for query, field, value in [("SYN_EQUAL", "label_exceeds_q", 1), ("SYN_GAP_SIX_ROWS", "future_sum_mm", "0.000000000"), ("SYN_EXCLUDE_T", "future_sum_mm", "37.200000000"), ("SYN_TRAIN_CROSS", "label_usable", True), ("SYN_NINE_DECIMALS", "future_sum_mm", "0.740741000"), ("SYN_EQUAL", "future_sum_mm", 1.2)]:
            with self.subTest(query=query, field=field):
                actual = copy.deepcopy(self.answers)
                next(r for r in actual if r["query_id"] == query)[field] = value
                self.assertTrue(compare_answers(self.answers, actual))
        self.assertEqual(compare_answers(self.answers, self.answers), [])

    def test_query_ids_must_match_and_cannot_repeat(self):
        self.assertTrue(compare_answers(self.answers, self.answers[:-1]))
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            compare_answers(self.answers, self.answers + [self.answers[0]])
        for malformed in [[None], [1], [{"query_id": None}], [{"query_id": []}], [{}]]:
            with self.subTest(malformed=malformed), self.assertRaises(ValueError):
                compare_answers(self.answers, malformed)

    def test_cli_failure_replaces_old_pass_and_returns_nonzero(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            actual = copy.deepcopy(self.answers)
            actual[0]["label_exceeds_q"] = 1
            (root / "actual.json").write_text(json.dumps(actual))
            output = root / "audit.json"
            output.write_text('{"status":"PASS"}')
            process = subprocess.run([sys.executable, str(ROOT / "scripts/window_audit.py"), "--observations", str(self.fixture / "synthetic_observations.csv"), "--queries", str(self.fixture / "synthetic_queries.csv"), "--actual", str(root / "actual.json"), "--output", str(output)], capture_output=True, text=True)
            self.assertEqual(process.returncode, 1)
            self.assertEqual(json.loads(output.read_text())["status"], "FAIL")
            (root / "actual.json").write_text("[null]")
            output.write_text('{"status":"PASS"}')
            malformed = subprocess.run([sys.executable, str(ROOT / "scripts/window_audit.py"), "--observations", str(self.fixture / "synthetic_observations.csv"), "--queries", str(self.fixture / "synthetic_queries.csv"), "--actual", str(root / "actual.json"), "--output", str(output)], capture_output=True, text=True)
            self.assertEqual(malformed.returncode, 1)
            self.assertEqual(json.loads(output.read_text())["status"], "FAIL")
            source = root / "observations.csv"
            source.write_bytes((self.fixture / "synthetic_observations.csv").read_bytes())
            original = source.read_bytes()
            protected = subprocess.run([sys.executable, str(ROOT / "scripts/window_audit.py"), "--observations", str(source), "--queries", str(self.fixture / "synthetic_queries.csv"), "--output", str(source)], capture_output=True, text=True)
            self.assertNotEqual(protected.returncode, 0)
            self.assertEqual(source.read_bytes(), original)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(WindowAuditTests))
    save_json(ROOT / "artifacts/stage6/boundary_test_results.json", {"passed": result.wasSuccessful(), "tests_run": result.testsRun, "failures": len(result.failures), "errors": len(result.errors), "test_script_sha256": sha256_file(Path(__file__))})
    sys.exit(0 if result.wasSuccessful() else 1)
