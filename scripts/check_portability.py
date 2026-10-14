"""Relocate only the sample payload; reuse this machine's runtime explicitly."""
import json
import os
import shutil
import subprocess
import tempfile

from pathlib import Path
from common import ROOT, now_sgt, save_json, sha256_file


def main():
    target = ROOT / "artifacts/stage4/portability_check.json"
    target.unlink(missing_ok=True)
    with tempfile.TemporaryDirectory(prefix="A relocated space ") as d:
        relocated = Path(d) / "Project A with spaces"
        relocated.mkdir()
        for name in ["scripts", "config", "data/handoff/stage2_sample"]:
            shutil.copytree(ROOT / name, relocated / name, ignore=shutil.ignore_patterns("__pycache__"))
        for name in ["requirements.lock.txt", "data/handoff/stage2_sample.csv", "artifacts/stage2/clean_schema.json"]:
            dst = relocated / name
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, dst)
        (relocated / ".venv").symlink_to(ROOT / ".venv", target_is_directory=True)
        env = dict(os.environ)
        env["JAVA_HOME"] = os.environ.get("JAVA_HOME", str(ROOT / ".runtime/java/jdk-17.0.20.1+1/Contents/Home"))
        result = subprocess.run(["bash", str(relocated / "scripts/run.sh"), "scripts/run_pipeline.py", "--mode", "sample"], cwd="/private/tmp", env=env, capture_output=True, text=True)
        log = ROOT / "artifacts/stage4/portability_check.log"
        log.write_text(result.stdout + result.stderr)
        assert result.returncode == 0, f"Relocated sample failed; see {log}"
        sample = json.loads((relocated / "artifacts/stage4/sample_verification.json").read_text())
        run = json.loads((relocated / "artifacts/stage4/sample_run.json").read_text())
        assert sample["status"] == run["status"] == "PASS" and sample["rows"] == 16712
        assert (relocated / "artifacts/stage4/smoke/station_year_summary.csv").exists()
    save_json(target, {"status": "PASS", "checked_at_sgt": now_sgt(), "scope": "Relocated sample payload, path with spaces, external cwd, same macOS ARM64 machine; runtime reused explicitly, not a fresh install or B/C machine test", "sample_rows": sample["rows"], "elapsed_seconds": run["elapsed_seconds"], "script_fingerprints": {**{r["script"]: r["script_sha256"] for r in run["steps"]}, **{p: sha256_file(ROOT / p) for p in ["scripts/run_pipeline.py", "scripts/run.sh", "config/pipeline.json"]}}})
    print("Relocated sample PASS: project path with spaces; output created in relocated project")


if __name__ == "__main__":
    main()
