"""Meaningful failure-path tests for delivery integrity and ordered execution."""
import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from common import ROOT, sha256_file, save_json
from run_pipeline import execute_steps
from verify_handoff import verify_manifest


class Stage4Tests(unittest.TestCase):
    def payload(self, root):
        p = root / "observations.parquet"
        p.write_bytes(b"formal")
        return {"core_files": [{"path": p.name, "bytes": p.stat().st_size, "sha256": sha256_file(p)}], "optional_raw_files": [{"path": "raw.csv", "bytes": 1, "sha256": "0" * 64}]}

    def test_optional_raw_absence_does_not_fail_core(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            m = self.payload(root)
            self.assertEqual(verify_manifest(root, m)["files_verified"], 1)
            with self.assertRaisesRegex(RuntimeError, "Missing file"):
                verify_manifest(root, m, include_raw=True)
            with self.assertRaisesRegex(RuntimeError, "Empty core inventory"):
                verify_manifest(root, {"core_files": []})

    def test_same_size_tamper_and_missing_file_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            m = self.payload(root)
            (root / "observations.parquet").write_bytes(b"tamper")
            with self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"):
                verify_manifest(root, m)
            (root / "observations.parquet").unlink()
            with self.assertRaisesRegex(RuntimeError, "Missing file"):
                verify_manifest(root, m)

    def test_duplicate_and_traversal_entries_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            m = self.payload(root)
            m["core_files"].append(dict(m["core_files"][0]))
            with self.assertRaisesRegex(RuntimeError, "Duplicate path"):
                verify_manifest(root, m)
            for p in ["../secret", "/etc/hosts", "C:\\outside", "safe/../../outside"]:
                with self.subTest(path=p):
                    m["core_files"] = [{"path": p, "bytes": 0, "sha256": "0" * 64}]
                    with self.assertRaisesRegex(RuntimeError, "Unsafe path"):
                        verify_manifest(root, m)

    def test_symlink_outside_root_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            outer = Path(d)
            root = outer / "project"
            root.mkdir()
            source = outer / "elsewhere"
            source.write_bytes(b"formal")
            (root / "observations.parquet").symlink_to(source)
            m = {"core_files": [{"path": "observations.parquet", "bytes": 6, "sha256": sha256_file(source)}]}
            with self.assertRaisesRegex(RuntimeError, "escapes root"):
                verify_manifest(root, m)

    def test_failure_stops_before_downstream_and_records_nonzero(self):
        with tempfile.TemporaryDirectory(prefix="A pipeline space ") as d:
            root = Path(d)
            (root / "failed.py").write_text("raise SystemExit(7)\n")
            (root / "later.py").write_text("from pathlib import Path\nPath('should_not_exist').write_text('ran')\n")
            with patch("run_pipeline.ROOT", root), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                code, result = execute_steps(["failed.py", "later.py"], root / "run", "test")
            self.assertEqual(code, 7)
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(len(result["steps"]), 1)
            self.assertFalse((root / "should_not_exist").exists())
            self.assertEqual(json.loads((root / "run/run.json").read_text())["status"], "FAIL")

    def test_child_runs_with_project_cwd_and_space_path(self):
        with tempfile.TemporaryDirectory(prefix="A pipeline space ") as d:
            root = Path(d)
            (root / "success.py").write_text("from pathlib import Path\nassert Path.cwd().name.startswith('A pipeline space ')\nPath('output').write_text('ok')\n")
            with patch("run_pipeline.ROOT", root), contextlib.redirect_stdout(io.StringIO()):
                code, result = execute_steps(["success.py"], root / "run", "test")
            self.assertEqual(code, 0)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual((root / "output").read_text(), "ok")

    def test_missing_script_is_fail_not_running(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            with patch("run_pipeline.ROOT", root), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                code, result = execute_steps(["not_delivered.py"], root / "run", "test")
            self.assertEqual(code, 1)
            self.assertEqual(result["status"], "FAIL")
            self.assertEqual(json.loads((root / "run/run.json").read_text())["status"], "FAIL")

    def test_bootstrap_missing_environment_reports_actionable_error(self):
        with tempfile.TemporaryDirectory(prefix="A no env ") as d:
            root = Path(d)
            (root / "scripts").mkdir()
            (root / "scripts/run.sh").write_bytes((ROOT / "scripts/run.sh").read_bytes())
            result = subprocess.run(["bash", str(root / "scripts/run.sh"), "-c", "print('should not run')"], cwd="/private/tmp", capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("setup.sh", result.stderr)
            self.assertNotIn("should not run", result.stdout)


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Stage4Tests))
    save_json(ROOT / "artifacts/stage4/boundary_test_results.json", {"tests_run": result.testsRun, "passed": result.wasSuccessful(), "failures": len(result.failures), "errors": len(result.errors), "test_script_sha256": sha256_file(Path(__file__))})
    sys.exit(0 if result.wasSuccessful() else 1)
