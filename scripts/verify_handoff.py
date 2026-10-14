"""Verify a transferred file inventory using Python's standard library only."""
import argparse
import json
import sys
from pathlib import Path, PurePosixPath

from common import ROOT, now_sgt, save_json, sha256_file


def verify_manifest(root, manifest, include_raw=False):
    root = Path(root).resolve()
    if not manifest["core_files"]:
        raise RuntimeError("Empty core inventory cannot be verified")
    entries = list(manifest["core_files"])
    if include_raw:
        entries += manifest["optional_raw_files"]
    seen, errors, size = set(), [], 0
    for r in entries:
        relative = r["path"]
        p = PurePosixPath(relative)
        if p.is_absolute() or ".." in p.parts or "\\" in relative or ":" in relative or not p.parts:
            errors.append(f"Unsafe path: {relative}")
            continue
        if relative in seen:
            errors.append(f"Duplicate path: {relative}")
            continue
        seen.add(relative)
        path = root / relative
        if not path.resolve().is_relative_to(root):
            errors.append(f"Path escapes root: {relative}")
        elif not path.is_file():
            errors.append(f"Missing file: {relative}")
        elif path.stat().st_size != r["bytes"]:
            errors.append(f"Size mismatch: {relative}")
        elif sha256_file(path) != r["sha256"]:
            errors.append(f"SHA-256 mismatch: {relative}")
        else:
            size += r["bytes"]
    if errors:
        raise RuntimeError("\n".join(errors))
    return {"files_verified": len(entries), "bytes_verified": size, "optional_raw_verified": include_raw}


def main():
    parser = argparse.ArgumentParser(description="Check listed core payload files; no Spark/environment installation required")
    parser.add_argument("--include-raw", action="store_true")
    parser.add_argument("--manifest", type=Path, default=ROOT / "artifacts/stage4/handoff_manifest.json")
    args = parser.parse_args()
    target = ROOT / "artifacts/stage4/manifest_verification.json"
    target.unlink(missing_ok=True)
    try:
        manifest = json.loads(args.manifest.read_text())
        evidence = verify_manifest(ROOT, manifest, args.include_raw)
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        for name in ["final_verification.json", "STAGE_4_CHECK_REPORT.md"]:
            (ROOT / "artifacts/stage4" / name).unlink(missing_ok=True)
        for name in ["A_FINAL_STATUS.json", "A_FINAL_CHECK_REPORT.md"]:
            (ROOT / "artifacts" / name).unlink(missing_ok=True)
        print(f"Handoff verification FAIL: {error}", file=sys.stderr)
        return 1
    save_json(target, {"status": "PASS", "checked_at_sgt": now_sgt(), "manifest_sha256": sha256_file(args.manifest), **evidence})
    print(f"Handoff inventory PASS: {evidence['files_verified']} files, {evidence['bytes_verified']:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
