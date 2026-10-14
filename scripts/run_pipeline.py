"""Run A's documented steps sequentially, preserving failure and timing evidence."""
import argparse
import json
import subprocess
import sys
import time
import uuid

from common import ROOT, now_sgt, save_json, sha256_file


def execute_steps(steps, run_dir, mode):
    run_dir.mkdir(parents=True, exist_ok=True)
    record = {"status": "RUNNING", "scope": "A only", "mode": mode, "started_at_sgt": now_sgt(), "steps": []}
    started = time.monotonic()
    save_json(run_dir / "run.json", record)
    for index, path in enumerate(steps, 1):
        print(f"[{index}/{len(steps)}] {path}", flush=True)
        step_started = time.monotonic()
        try:
            script_sha = sha256_file(ROOT / path)
        except OSError as error:
            record["steps"].append({"script": path, "return_code": 1, "elapsed_seconds": 0, "error": str(error)})
            record.update(status="FAIL", finished_at_sgt=now_sgt(), elapsed_seconds=round(time.monotonic() - started, 2))
            save_json(run_dir / "run.json", record)
            print(f"Cannot open {path}: {error}", file=sys.stderr)
            return 1, record
        log = run_dir / f"{index:02d}_{path.rsplit('/', 1)[-1]}.log"
        with log.open("w") as handle:
            process = subprocess.Popen([sys.executable, str(ROOT / path)], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            try:
                for line in process.stdout:
                    handle.write(line)
                    handle.flush()
                    print(line, end="", flush=True)
                code = process.wait()
            except BaseException:
                process.terminate()
                process.wait()
                record["status"] = "INTERRUPTED"
                save_json(run_dir / "run.json", record)
                raise
        record["steps"].append({"script": path, "script_sha256": script_sha, "return_code": code, "elapsed_seconds": round(time.monotonic() - step_started, 2), "log": log.name})
        if code:
            record["status"] = "FAIL"
            record["finished_at_sgt"] = now_sgt()
            record["elapsed_seconds"] = round(time.monotonic() - started, 2)
            save_json(run_dir / "run.json", record)
            print(f"Stopped at {path}; see {log}", file=sys.stderr)
            return code, record
        save_json(run_dir / "run.json", record)
    record.update(status="PASS", finished_at_sgt=now_sgt(), elapsed_seconds=round(time.monotonic() - started, 2))
    save_json(run_dir / "run.json", record)
    return 0, record


def main():
    cfg = json.loads((ROOT / "config/pipeline.json").read_text())
    parser = argparse.ArgumentParser(description="A environment, sample, existing-result verification or raw-to-figures rebuild")
    parser.add_argument("--mode", choices=cfg["modes"], default="sample")
    parser.add_argument("--dry-run", action="store_true", help="print ordered scripts without executing or changing results")
    args = parser.parse_args()
    steps = cfg["modes"][args.mode]
    if args.dry_run:
        for index, path in enumerate(steps, 1):
            print(f"{index:02d}. {path}")
        return 0
    if args.mode == "rebuild" and any((ROOT / "data/stage2" / name).exists() for name in ["parsed", "grouped"]):
        parser.error("Fresh rebuild requires moving data/stage2/parsed and grouped out of data/stage2 first. Existing checkpoints may reflect old rules. See docs/RUN_GUIDE.md; no files were deleted.")
    target = ROOT / f"artifacts/stage4/{args.mode}_run.json"
    target.unlink(missing_ok=True)
    # Any new execution invalidates the old Stage 4 seal until checked again.
    for name in ["final_verification.json", "STAGE_4_CHECK_REPORT.md"]:
        (ROOT / "artifacts/stage4" / name).unlink(missing_ok=True)
    for name in ["A_FINAL_STATUS.json", "A_FINAL_CHECK_REPORT.md"]:
        (ROOT / "artifacts" / name).unlink(missing_ok=True)
    run_dir = ROOT / "artifacts/stage4/runs" / (time.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8])
    code, record = execute_steps(steps, run_dir, args.mode)
    record["run_directory"] = str(run_dir.relative_to(ROOT))
    save_json(target, record)
    print(f"A {args.mode}: {record['status']} ({record['elapsed_seconds']}s)", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
