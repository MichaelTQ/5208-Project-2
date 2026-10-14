"""Describe the future payload and excluded storage without creating an archive."""
import json
from pathlib import Path

from common import ROOT, now_sgt, save_json, sha256_file


def under(path, prefix):
    return path == prefix or path.startswith(prefix + "/")


def selected(path, policy):
    if any(under(path, x) for x in policy["excluded_roots"]):
        return False
    if path in policy["unhashed_companions"] or "__pycache__" in Path(path).parts:
        return False
    return Path(path).name not in policy["ignored_file_names"] and not any(path.endswith(x) for x in policy["ignored_suffixes"])


def entries_for(roots, policy):
    files = {}
    for name in roots:
        start = ROOT / name
        if not start.exists():
            raise RuntimeError(f"Required handoff root missing: {name}")
        for p in ([start] if start.is_file() else start.rglob("*")):
            if not p.is_file():
                continue
            path = p.relative_to(ROOT).as_posix()
            if not selected(path, policy):
                continue
            if p.is_symlink() or not p.resolve().is_relative_to(ROOT):
                raise RuntimeError(f"Handoff payload cannot reference external/symlink files: {path}")
            files[path] = {"path": path, "bytes": p.stat().st_size, "sha256": sha256_file(p)}
    return [files[name] for name in sorted(files)]


def main():
    out = ROOT / "artifacts/stage4"
    out.mkdir(parents=True, exist_ok=True)
    for name in ["final_verification.json", "STAGE_4_CHECK_REPORT.md", "manifest_verification.json"]:
        (out / name).unlink(missing_ok=True)
    for name in ["A_FINAL_STATUS.json", "A_FINAL_CHECK_REPORT.md"]:
        (ROOT / "artifacts" / name).unlink(missing_ok=True)
    for stage in [1, 2, 3]:
        assert json.loads((ROOT / f"artifacts/stage{stage}/final_verification.json").read_text())["status"] == "PASS"
    policy = json.loads((ROOT / "config/handoff_policy.json").read_text())
    core = entries_for(policy["core_roots"], policy)
    raw = entries_for([policy["optional_raw_root"]], policy)
    assert len(raw) == 8 and all(r["path"].endswith(".csv") for r in raw)
    groups = {}
    for r in core:
        path = r["path"]
        group = next((x for x in ["data/clean", "data/handoff", "data/quarantine", "artifacts/stage2", "artifacts/stage3"] if under(path, x)), "code_docs_and_other_evidence")
        value = groups.setdefault(group, {"files": 0, "bytes": 0})
        value["files"] += 1
        value["bytes"] += r["bytes"]
    excluded = []
    for name in policy["excluded_roots"]:
        p = ROOT / name
        if p.exists() and name not in [".git", ".agents", ".codex"]:
            files = [x for x in p.rglob("*") if x.is_file() and not x.is_symlink()]
            excluded.append({"path": name, "files": len(files), "bytes": sum(x.stat().st_size for x in files), "reason": "machine environment, temporary/cache or rebuildable intermediate"})
    manifest = {"schema_version": 1, "created_at_sgt": now_sgt(), "scope": "A payload inventory only; no ZIP or B/C module acceptance", "core_files": core, "optional_raw_files": raw, "unhashed_companions": policy["unhashed_companions"], "notes": policy["notes"]}
    save_json(out / "handoff_manifest.json", manifest)
    save_json(out / "storage_inventory.json", {"created_at_sgt": now_sgt(), "core_files": len(core), "core_bytes": sum(r["bytes"] for r in core), "optional_raw_files": len(raw), "optional_raw_bytes": sum(r["bytes"] for r in raw), "core_groups": groups, "excluded_storage": excluded, "uncompressed_bytes_not_zip_estimate": True})
    print(f"Handoff inventory created: {len(core)} core files ({sum(r['bytes'] for r in core):,} bytes); 8 optional raw CSV; no ZIP")


if __name__ == "__main__":
    main()
