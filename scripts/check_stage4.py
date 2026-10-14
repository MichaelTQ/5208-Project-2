"""Acceptance of A4; never marks undelivered B/C modules as passing."""
import json
import re
import subprocess
import sys
from pathlib import Path

from common import ROOT, config, now_sgt, save_json, sha256_file
from verify_handoff import verify_manifest


def read(relative):
    return json.loads((ROOT / relative).read_text())


def main():
    out = ROOT / "artifacts/stage4"
    for name in ["final_verification.json", "STAGE_4_CHECK_REPORT.md"]:
        (out / name).unlink(missing_ok=True)
    checks = {}
    stages = {s: read(f"artifacts/stage{s}/final_verification.json") for s in [1, 2, 3]}
    assert all(r["status"] == "PASS" for r in stages.values())
    assert stages[1]["total_raw_rows"] == 48538713 and stages[2]["clean_stats"]["rows"] == 48538625
    assert stages[3]["figures"] == 6
    checks["stage1_stage2_stage3_acceptance_pass"] = True
    pipeline = read("config/pipeline.json")
    run = read("artifacts/stage4/verify_run.json")
    assert run["status"] == "PASS" and run["scope"] == "A only"
    assert [r["script"] for r in run["steps"]] == pipeline["modes"]["verify"]
    for r in run["steps"]:
        assert r["return_code"] == 0 and sha256_file(ROOT / r["script"]) == r["script_sha256"]
        assert (ROOT / run["run_directory"] / r["log"]).is_file()
    checks["ordered_unified_verification_current_script_fingerprints"] = True
    environment = read("artifacts/stage4/preflight.json")
    cfg = config()
    assert environment["status"] == "PASS" and environment["python_version"] == cfg["python_version"]
    assert environment["spark_version"] == cfg["spark_version"] and environment["spark_timezone"] == cfg["timezone"]
    assert environment["actual_jvm_max_heap_bytes"] == 4 * 1024**3 and environment["arithmetic_and_sgt_checks"]
    checks["current_environment_and_actual_sgt_decimal_computation"] = True
    sample = read("artifacts/stage4/sample_verification.json")
    assert sample["status"] == "PASS" and sample["rows"] == 16712 and all(sample["checks"].values())
    assert sample["source_csv_sha256"] == sha256_file(ROOT / "data/handoff/stage2_sample.csv")
    assert sample["summary_csv_sha256"] == sha256_file(out / "smoke/station_year_summary.csv")
    checks["real_sample_source_values_independent_amounts_and_roundtrips"] = True
    portability = read("artifacts/stage4/portability_check.json")
    assert portability["status"] == "PASS" and portability["sample_rows"] == sample["rows"]
    for name, digest in portability["script_fingerprints"].items():
        assert sha256_file(ROOT / name) == digest
    checks["relocated_sample_space_path_external_cwd_same_machine"] = True
    tests = read("artifacts/stage4/boundary_test_results.json")
    assert tests["passed"] and tests["tests_run"] == 8
    assert tests["test_script_sha256"] == sha256_file(ROOT / "tests/test_stage4_runtime.py")
    checks["eight_failure_and_integrity_boundary_tests"] = True

    # Preview every mode from outside the project; it must not rewrite evidence.
    before = {name: sha256_file(out / name) for name in ["verify_run.json", "preflight.json", "sample_verification.json"]}
    for mode, steps in pipeline["modes"].items():
        result = subprocess.run(["bash", str(ROOT / "scripts/run.sh"), "scripts/run_pipeline.py", "--mode", mode, "--dry-run"], cwd="/private/tmp", capture_output=True, text=True)
        assert result.returncode == 0
        assert [line.split(". ", 1)[1] for line in result.stdout.splitlines()] == steps
        assert all((ROOT / s).is_file() for s in steps)
    assert before == {name: sha256_file(out / name) for name in before}
    checks["all_mode_previews_ordered_and_do_not_mutate_evidence"] = True

    manifest = read("artifacts/stage4/handoff_manifest.json")
    evidence = verify_manifest(ROOT, manifest, include_raw=True)
    previous = read("artifacts/stage4/manifest_verification.json")
    assert previous["status"] == "PASS" and previous["optional_raw_verified"]
    assert previous["manifest_sha256"] == sha256_file(out / "handoff_manifest.json")
    policy = read("config/handoff_policy.json")
    actual = {r["path"] for r in manifest["core_files"]}
    # Independently enumerate the policy's inputs, without using the builder's helper.
    expected = set()
    for name in policy["core_roots"]:
        start = ROOT / name
        for p in ([start] if start.is_file() else start.rglob("*")):
            if not p.is_file():
                continue
            rel = p.relative_to(ROOT).as_posix()
            if rel in policy["unhashed_companions"] or "__pycache__" in p.parts or p.name in policy["ignored_file_names"] or any(rel.endswith(x) for x in policy["ignored_suffixes"]):
                continue
            if any(rel == prefix or rel.startswith(prefix + "/") for prefix in policy["excluded_roots"]):
                continue
            expected.add(rel)
    assert actual == expected
    assert len(manifest["optional_raw_files"]) == 8
    assert not any(p.startswith("data/raw/") for p in actual)
    storage = read("artifacts/stage4/storage_inventory.json")
    assert storage["core_files"] == len(actual) and storage["core_bytes"] == sum(r["bytes"] for r in manifest["core_files"])
    assert storage["optional_raw_bytes"] == stages[1]["total_raw_bytes"]
    checks["complete_core_and_optional_raw_inventory_all_sha256_verified"] = True
    files = {r["path"]: r for r in manifest["core_files"]}
    parquet = read("artifacts/stage2/clean_file_manifest.json")
    assert len(parquet) == 96
    for r in parquet:
        assert r["bytes"] == files[r["path"]]["bytes"] and r["sha256"] == files[r["path"]]["sha256"]
    assert sum(r["rows"] for r in parquet) == 48538625
    assert sum(r["bytes"] for r in parquet) == 4223822379
    checks["formal_parquet_96_files_match_independent_stage2_fingerprints"] = True
    figures = read("artifacts/stage3/figure_manifest.json")
    assert len(figures) == 6
    for f in figures:
        assert f["png"] in files and f["pdf"] in files
        for source in f["data_sources"]:
            assert files[source["path"]]["sha256"] == source["sha256"]
    checks["six_png_pdf_figures_and_source_tables_in_delivery"] = True
    for doc in ["README.md", "docs/RUN_GUIDE.md", "docs/MODULE_INTEGRATION.md", "docs/HANDOFF_B.md", "docs/STAGE_4_PLAN.md"]:
        text = (ROOT / doc).read_text()
        assert "Lecture 5/6" in text and "不需要" in text
        for link in re.findall(r"\]\(([^)]+)\)", text):
            if "://" in link or link.startswith("#"):
                continue
            path = (ROOT / doc).parent / link.split("#")[0]
            assert path.is_file() or path == out / "STAGE_4_CHECK_REPORT.md", (doc, link)
    checks["documentation_links_and_confirmed_lecture_decision"] = True
    assert set(pipeline["external_modules"]) == {"B", "C"}
    assert all(m["status"] == "NOT_DELIVERED" for m in pipeline["external_modules"].values())
    checks["external_module_status_separate_from_A_acceptance"] = True

    result = {"status": "PASS", "scope": "A4 reproducibility and handoff readiness only", "checked_at_sgt": now_sgt(), "checks": checks, "verify_seconds": run["elapsed_seconds"], "sample_rows": sample["rows"], "boundary_tests": tests["tests_run"], "core_files": storage["core_files"], "core_bytes": storage["core_bytes"], "optional_raw_bytes": storage["optional_raw_bytes"], "manifest_sha256": sha256_file(out / "handoff_manifest.json"), "external_integration_status": "NOT_DELIVERED", "limitations": ["verify checked existing A artifacts, not a new full raw-to-output rebuild", "same-machine relocation reused A runtime; B/C machines not tested", "B/C labels/features/model loading not tested without delivered code", "ZIP not created; raw CSV inclusion to be decided later"]}
    save_json(out / "final_verification.json", result)
    lines = ["# Stage 4 检查报告", "", f"检查时间：{result['checked_at_sgt']}。结果：**PASS（A4 当前独立范围）**。", "", f"{len(checks)} 项独立检查、{tests['tests_run']} 项失败/完整性边界测试通过。", "", "## 已验证", "", f"- 统一 verify 入口顺序执行 {len(run['steps'])} 步，返回码全部为 0；本轮实际用时 {run['elapsed_seconds']:.2f} 秒。", "- Stage 1：八年原始 CSV 摘要与 5 项完整性测试；Stage 2：全量数据 8 项验收与 9 项边界测试；Stage 3：11 项验收与 6 项分析边界测试均通过。", f"- 正式真实样本 {sample['rows']:,} 行，{sample['station_year_groups']} 个站点年汇总；原始记录摘要与逐键雨量、独立 Decimal 汇总、所有字段 Parquet 往返与汇总 CSV 一致。", "- 当前本机环境与 Spark 实际计算通过；项目外启动及含空格临时迁移项目通过。迁移复用本机环境。", "- 运行中失败/脚本缺失会停止并标 FAIL；缺文件、同大小篡改、重复/越界清单条目被拒绝，可选原始 CSV 缺少不误报核心失败。", "- 核心文件完整枚举并逐个 SHA-256 检查；八个可选原始 CSV 也已核验。", "", "## 交接容量（未压缩）", "", "| 内容 | 文件数 | 字节数 | GiB |", "|---|---:|---:|---:|", f"| 核心文件 | {storage['core_files']} | {storage['core_bytes']:,} | {storage['core_bytes']/1024**3:.3f} |", f"| 可选八年原始 CSV | 8 | {storage['optional_raw_bytes']:,} | {storage['optional_raw_bytes']/1024**3:.3f} |", "", "清单/容量/验收文件为不互相循环摘要的补充文件，随核心文件交接；容量表不含这些小型补充文件，也不是 ZIP 大小。正式 Parquet 为 96 文件、4,223,822,379 字节。排除机器环境、缓存、Spark 临时文件、parsed/grouped 和分析检查点。", "", "## 检查项", ""]
    lines.extend(f"- PASS：{name}" for name in checks)
    lines += ["", "## 尚需实际外部代码的工作", "", "B/C 在本目录尚未交付：当前没有验收标签/特征/基线或模型训练/加载/预测。后续按 docs/MODULE_INTEGRATION.md 接入并做 A6 联调；不得将本次 A4 PASS 当作整个项目 PASS。", "", "本次执行的是已有成果复核；完整重建顺序已提供，但未在本轮重新下载、全量清洗或声称新安装验证。Lecture 5/6 不需要提供；未创建 ZIP，也未向组员发送消息。", "", "## 证据与复查", "", "- verify_run.json 和 runs/：真实步骤、代码摘要、返回码、耗时及日志。", "- preflight.json、sample_verification.json、portability_check.json、boundary_test_results.json。", "- handoff_manifest.json、storage_inventory.json、manifest_verification.json。", "", "```bash", "bash scripts/run.sh scripts/check_stage4.py", "```", "", "代码/文档/数据或证据更新后先重建清单并运行 verify_handoff，再验收。", ""]
    (out / "STAGE_4_CHECK_REPORT.md").write_text("\n".join(lines))
    print(f"Stage 4 A4 acceptance PASS: {len(checks)} checks; {tests['tests_run']} boundary tests; external B/C NOT_DELIVERED")


if __name__ == "__main__":
    main()
