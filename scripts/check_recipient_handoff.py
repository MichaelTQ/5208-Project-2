"""Exercise only the listed core payload in a temporary recipient directory.

This checks data/package independence, not a fresh environment installation.
The local Python/Java runtime is explicitly reused.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from common import ROOT, now_sgt, save_json, sha256_file
from verify_handoff import verify_manifest


def main():
    target = ROOT / "artifacts/stage4/recipient_review.json"
    manifest_path = ROOT / "artifacts/stage4/handoff_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    source = verify_manifest(ROOT, manifest, include_raw=True)
    steps = []
    with tempfile.TemporaryDirectory(prefix="A recipient core only ") as folder:
        recipient = Path(folder) / "B project with spaces"
        for row in manifest["core_files"]:
            dest = recipient / row["path"]
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / row["path"], dest)
        policy = json.loads((ROOT / "config/handoff_policy.json").read_text())
        for name in policy["unhashed_companions"]:
            if not (ROOT / name).is_file():
                continue  # Final seals may be absent while refreshing a delivery.
            dest = recipient / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, dest)
        assert all(not (recipient / p).exists() for p in ["data/raw", "data/stage2", "data/analysis", ".runtime", ".cache", ".venv"])
        copied = verify_manifest(recipient, manifest)
        target.unlink(missing_ok=True)
        (recipient / ".venv").symlink_to(ROOT / ".venv", target_is_directory=True)
        env = dict(os.environ)
        env["JAVA_HOME"] = os.environ.get("JAVA_HOME", str(ROOT / ".runtime/java/jdk-17.0.20.1+1/Contents/Home"))
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        log = []

        def run(name, command):
            result = subprocess.run(command, cwd=folder, env=env, capture_output=True, text=True)
            log.extend([name, result.stdout, result.stderr])
            (ROOT / "artifacts/stage4/recipient_review.log").write_text("\n".join(log))
            assert result.returncode == 0, f"{name} failed; see recipient_review.log"
            steps.append({"name": name, "return_code": result.returncode})

        run("recipient standard-library core integrity", [sys.executable, str(recipient / "scripts/verify_handoff.py")])
        run("recipient sample entry from external working directory", ["bash", str(recipient / "scripts/run.sh"), "scripts/run_pipeline.py", "--mode", "sample"])
        sample = json.loads((recipient / "artifacts/stage4/sample_verification.json").read_text())
        assert sample["status"] == "PASS" and sample["rows"] == 16712
        fixture = recipient / "data/handoff/stage6_window_checks"
        for prefix in ["synthetic", "real"]:
            output = Path(folder) / f"{prefix}_audit.json"
            run(f"recipient {prefix} diagnostic export interface", [sys.executable, str(recipient / "scripts/window_audit.py"), "--observations", str(fixture / f"{prefix}_observations.csv"), "--queries", str(fixture / f"{prefix}_queries.csv"), "--actual", str(recipient / f"artifacts/stage6/{prefix}_spark_answers.json"), "--output", str(output)])
            assert json.loads(output.read_text())["status"] == "PASS"
        reader = Path(folder) / "read_formal.py"
        reader.write_text("from pyspark.sql import SparkSession, functions as F\nfrom decimal import Decimal\nspark=(SparkSession.builder.master('local[4]').config('spark.driver.memory','4g').config('spark.sql.session.timeZone','Asia/Singapore').getOrCreate())\ntry:\n    data=spark.read.parquet('data/clean/observations')\n    row=data.agg(F.count('*').alias('n'),F.sum('rainfall_mm').alias('mm'),F.countDistinct('station_id').alias('stations')).first()\n    assert row.n==48538625 and row.mm==Decimal('1228210.429440000') and row.stations==91, row\n    print('Formal recipient data PASS:', row)\nfinally:\n    spark.stop()\n")
        run("recipient full formal table independent count and Decimal sum", ["bash", str(recipient / "scripts/run.sh"), str(reader)])
    save_json(target, {"status": "PASS", "checked_at_sgt": now_sgt(), "scope": "core-only recipient copy; same-machine runtime explicitly reused; not B/C hardware or fresh installation", "review_script_sha256": sha256_file(Path(__file__)), "reviewed_manifest_sha256": sha256_file(manifest_path), "source_integrity": source, "recipient_core_integrity": copied, "excluded_production_caches_and_raw": True, "sample_rows": 16712, "formal_rows": 48538625, "formal_station_ids": 91, "formal_rainfall_audit_sum_mm": "1228210.429440000", "window_queries": 36, "window_actual_source": "A independent Spark fixture answers, not B implementation", "steps": steps})
    print("Recipient core-only review PASS: full formal data, sample entry and 36 diagnostic queries")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        save_json(ROOT / "artifacts/stage4/recipient_review.json", {"status": "FAIL", "checked_at_sgt": now_sgt(), "error": str(error)})
        raise
