"""Verify A's bounded cross-check kit; external implementation status stays pending."""
import copy
import csv
import json
import re
import subprocess
import sys
import tempfile

from pathlib import Path
from common import ROOT, now_sgt, save_json, sha256_file
from window_audit import oracle_answers, compare_answers


def load(path):
    return json.loads((ROOT / path).read_text())


def main():
    out = ROOT / "artifacts/stage6"
    out.mkdir(parents=True, exist_ok=True)
    for name in ["final_verification.json", "STAGE_6_CHECK_REPORT.md"]:
        (out / name).unlink(missing_ok=True)
    checks = {}
    manifest = load("artifacts/stage6/fixtures_manifest.json")
    assert manifest["generator_sha256"] == sha256_file(ROOT / "scripts/stage6_fixtures.py")
    fixture = ROOT / "data/handoff/stage6_window_checks"
    for r in manifest["files"]:
        assert sha256_file(ROOT / r["path"]) == r["sha256"]
    assert sha256_file(ROOT / "data/handoff/stage2_sample.csv") == manifest["real_source_csv_sha256"]
    checks["fixture_inputs_and_source_sample_fingerprints_current"] = True
    tests = load("artifacts/stage6/boundary_test_results.json")
    assert tests["passed"] and tests["tests_run"] == 15
    assert tests["test_script_sha256"] == sha256_file(ROOT / "tests/test_window_audit.py")
    checks["fifteen_known_answer_precision_split_and_failure_tests"] = True
    spark = load("artifacts/stage6/spark_verification.json")
    assert spark["status"] == "PASS"
    assert spark["script_sha256"] == sha256_file(ROOT / "scripts/stage6_spark_check.py")
    assert spark["oracle_sha256"] == sha256_file(ROOT / "scripts/window_audit.py")
    assert spark["config_sha256"] == sha256_file(ROOT / "config/window_audit.json")
    for prefix in ["synthetic", "real"]:
        expected = oracle_answers(fixture / f"{prefix}_observations.csv", fixture / f"{prefix}_queries.csv")
        assert compare_answers(expected, load(f"data/handoff/stage6_window_checks/{prefix}_expected.json")) == []
        assert compare_answers(expected, load(f"artifacts/stage6/{prefix}_spark_answers.json")) == []
    assert spark["evidence"]["synthetic"]["queries"] == 21 and spark["evidence"]["real"]["queries"] == 15
    checks["independent_spark_exact_joins_all_36_queries_match_oracle"] = True
    assert spark["evidence"]["six_rows_counterexample"]["counted_rows"] == 6
    assert spark["evidence"]["six_rows_counterexample"]["oracle_future_complete"] is False
    checks["future_six_rows_can_skip_a_required_timestamp_demonstrated"] = True
    with (ROOT / "data/handoff/stage2_sample.csv").open(newline="") as handle:
        source = {(r["station_id"], r["observation_time"]): r["rainfall_mm"] for r in csv.DictReader(handle)}
    with (fixture / "real_observations.csv").open(newline="") as handle:
        real = {(r["station_id"], r["observation_time"]): r["rainfall_mm"] for r in csv.DictReader(handle)}
    assert real == source and len(real) == 16712
    with (fixture / "real_queries.csv").open(newline="") as handle:
        queries = list(csv.DictReader(handle))
    assert set(q["prediction_time"][:4] for q in queries) == set(str(y) for y in range(2017, 2025))
    checks["real_fixtures_exact_formal_sample_and_all_eight_years"] = True

    # Exercise the user-facing CLI on correct and intentionally wrong exported answers.
    expected = load("data/handoff/stage6_window_checks/synthetic_expected.json")
    mutations = [("SYN_EQUAL", "label_exceeds_q", 1, "greater_or_equal"), ("SYN_GAP_SIX_ROWS", "future_sum_mm", "0.000000000", "missing_as_zero"), ("SYN_EXCLUDE_T", "future_sum_mm", "37.200000000", "include_t"), ("SYN_TRAIN_CROSS", "label_usable", True, "split_crossing"), ("SYN_NINE_DECIMALS", "future_sum_mm", "0.740741000", "rounded_amount"), ("SYN_EQUAL", "future_sum_mm", 1.2, "float_output")]
    mutation_evidence = []
    with tempfile.TemporaryDirectory(prefix="A window CLI ") as d:
        folder = Path(d)
        actual, report = folder / "actual.json", folder / "audit.json"
        command = [sys.executable, str(ROOT / "scripts/window_audit.py"), "--observations", str(fixture / "synthetic_observations.csv"), "--queries", str(fixture / "synthetic_queries.csv"), "--actual", str(actual), "--output", str(report)]
        actual.write_text(json.dumps(expected))
        good = subprocess.run(command, cwd="/private/tmp", capture_output=True, text=True)
        assert good.returncode == 0 and json.loads(report.read_text())["status"] == "PASS"
        for q, field, value, description in mutations:
            wrong = copy.deepcopy(expected)
            next(r for r in wrong if r["query_id"] == q)[field] = value
            actual.write_text(json.dumps(wrong))
            result = subprocess.run(command, cwd="/private/tmp", capture_output=True, text=True)
            audit = json.loads(report.read_text())
            assert result.returncode != 0 and audit["status"] == "FAIL" and audit["mismatches"]
            mutation_evidence.append({"error": description, "query_id": q, "return_code": result.returncode, "mismatch_count": len(audit["mismatches"]), "rejected": True})
    save_json(out / "mutation_checks.json", mutation_evidence)
    checks["actual_export_cli_accepts_correct_and_rejects_six_error_classes"] = True
    for r in load("artifacts/stage2/clean_file_manifest.json"):
        assert sha256_file(ROOT / r["path"]) == r["sha256"]
    # Capture the current production figures before auditing. This also works for a
    # fresh rebuild where no historical handoff seal exists yet.
    figure_files = manifest["preserved_figure_files"]
    assert len([r for r in figure_files if r["path"].endswith(".png")]) == 6
    assert len([r for r in figure_files if r["path"].endswith(".pdf")]) == 6
    assert all(sha256_file(ROOT / r["path"]) == r["sha256"] for r in figure_files)
    checks["formal_observations_and_six_figure_pairs_unchanged"] = True
    guide = (ROOT / "docs/WINDOW_CHECK_GUIDE.md").read_text()
    for phrase in ["REFERENCE_ONLY", "尚未交付", "不是 B/C 模块已通过", "不需要提供", "不生产全量训练标签", "历史完整性单独"]:
        assert phrase in guide
    pipeline = load("config/pipeline.json")
    assert all(v["status"] == "NOT_DELIVERED" for v in pipeline["external_modules"].values())
    checks["cross_check_scope_and_real_external_integration_pending"] = True
    result = {"status": "PASS", "scope": "A6 independent audit preparation only", "checked_at_sgt": now_sgt(), "checks": checks, "known_answer_tests": tests["tests_run"], "synthetic_queries": 21, "real_queries": 15, "mutation_error_classes": len(mutation_evidence), "external_BC_implementation": "NOT_DELIVERED", "required_later": ["B actual window exports and full label/feature code", "training-only preprocessing fit review", "C saved-model load and prediction", "full project run after B/C delivery"]}
    save_json(out / "final_verification.json", result)
    lines = ["# Stage 6 检查报告", "", f"检查结果：**PASS（A 独立核验准备）**。时间：{result['checked_at_sgt']}。", "", f"{len(checks)} 项检查通过；15 项已知答案/失败边界测试；21 个合成和 15 个真实查询，与独立 Spark 精确时间连接逐字段一致。", "", "6 类故意错误（>=q、缺测补零、包含 t、跨 split、舍入金额、浮点金额导出）经用户接口检查均返回非零/FAIL；正确导出通过。该正确导出来自 A 参考夹具，并不是 B 的代码运行证据。", "", "真实窄表与正式样本逐键逐值一致，覆盖全部八年；正式数据 96 个文件与既有六组 PNG/PDF 指纹不变。", ""]
    lines += [f"- PASS：{k}" for k in checks]
    lines += ["", "## 后续真实联调", "", "用户已确认 B/C 尚未交付。本阶段检查的是 A 的工具与夹具，不是其实际标签/特征/模型。收到 B 导出后按 docs/WINDOW_CHECK_GUIDE.md 验收，再检查训练拟合范围与 C 独立加载预测；最终按 README 跑整个项目。", "", "A 的数据、描述性分析、报告及交叉检查准备可先交接给 B。Lecture 5/6 不需要提供；未创建 ZIP。", ""]
    (out / "STAGE_6_CHECK_REPORT.md").write_text("\n".join(lines))
    print(f"Stage 6 A preparation PASS: {len(checks)} checks, 15 tests, 36 fixture queries; B/C NOT_DELIVERED")


if __name__ == "__main__":
    main()
