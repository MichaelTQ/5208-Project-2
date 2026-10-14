"""Refresh the payload seal and summarize completed A work without creating ZIP."""
import json
import subprocess
import sys

from common import ROOT, now_sgt, save_json, sha256_file


def main():
    for name in ["A_FINAL_STATUS.json", "A_FINAL_CHECK_REPORT.md"]:
        (ROOT / "artifacts" / name).unlink(missing_ok=True)
    for s in [1, 2, 3, 5, 6]:
        assert json.loads((ROOT / f"artifacts/stage{s}/final_verification.json").read_text())["status"] == "PASS"
    # Sequential mutations are deliberate; stop immediately if any seal/check fails.
    commands = [["scripts/build_handoff_manifest.py"], ["scripts/verify_handoff.py", "--include-raw"], ["scripts/check_stage4.py"]]
    for args in commands:
        print(f"Final A check: {args[0]}", flush=True)
        subprocess.run([sys.executable, str(ROOT / args[0]), *args[1:]], cwd=ROOT, check=True)
    stages = {str(s): json.loads((ROOT / f"artifacts/stage{s}/final_verification.json").read_text()) for s in range(1, 7)}
    assert all(s["status"] == "PASS" for s in stages.values())
    policy = json.loads((ROOT / "config/handoff_policy.json").read_text())
    manifest = json.loads((ROOT / "artifacts/stage4/handoff_manifest.json").read_text())
    payload = {r["path"]: r for r in manifest["core_files"]}
    required = ["docs/REPORT_A_FULL.md", "docs/REPORT_A_MERGE_GUIDE.md", "docs/WINDOW_CHECK_GUIDE.md", "docs/A_COMPLETION.md", "scripts/window_audit.py", "data/handoff/stage6_window_checks/synthetic_expected.json", "data/handoff/stage6_window_checks/real_expected.json", "artifacts/stage5/final_verification.json", "artifacts/stage6/final_verification.json"]
    assert all(name in payload for name in required)
    assert all(payload[name]["sha256"] == sha256_file(ROOT / name) for name in required)
    pipeline = json.loads((ROOT / "config/pipeline.json").read_text())
    assert all(x["status"] == "NOT_DELIVERED" for x in pipeline["external_modules"].values())
    default_scopes = {"1": "A1 source/environment", "2": "A2 cleaning/quality", "3": "A3 descriptive analysis", "5": "A5 report contribution"}
    stage_summary = {}
    for stage, evidence in stages.items():
        stage_summary[stage] = {"status": evidence["status"], "scope": evidence.get("scope", default_scopes.get(stage)), "checks": len(evidence["checks"]) if "checks" in evidence else None}
        if "unit_tests_run" in evidence:
            stage_summary[stage].update(unit_tests=evidence["unit_tests_run"], spark_assertions=len(evidence["spark_assertions"]))
    result = {"status": "READY_FOR_B", "checked_at_sgt": now_sgt(), "A_independent_work_complete": True, "whole_project_accepted": False, "external_BC_status": "NOT_DELIVERED", "work_packages": {"A1_data_environment": "COMPLETE", "A2_cleaning_quality": "COMPLETE", "A3_cross_year_station_analysis": "COMPLETE", "A4_readme_run_handoff": "COMPLETE", "A5_report_contribution": "COMPLETE", "A6_independent_cross_check_kit": "COMPLETE", "A6_actual_BC_integration": "PENDING_DELIVERY"}, "stage_checks": stage_summary, "core_files": stages["4"]["core_files"], "core_bytes": stages["4"]["core_bytes"], "optional_raw_bytes": stages["4"]["optional_raw_bytes"], "handoff_manifest_sha256": sha256_file(ROOT / "artifacts/stage4/handoff_manifest.json"), "zip_status": "NOT_CREATED_AWAITING_USER_REQUEST", "required_later": stages["6"]["required_later"], "verification_seal_files": policy["unhashed_companions"]}
    save_json(ROOT / "artifacts/A_FINAL_STATUS.json", result)
    lines = ["# A 最终交接检查", "", f"状态：**READY_FOR_B**。检查时间：{result['checked_at_sgt']}。", "", "A 当前可独立执行的工作已完成；Stage 1–6 对应范围全部有验收证据。B/C 尚未交付，实际联调为 PENDING_DELIVERY，不代表模型或全项目已经验收。", "", "| 工作包 | 状态 | 交付 |", "|---|---|---|", "| A1 来源/环境 | 完成 | 八年来源、原始数据清单、锁定环境 |", "| A2 清洗/质量 | 完成 | 正式 Parquet、账本、覆盖/缺口/站点信息 |", "| A3 跨年/站点分析 | 完成 | 16 份原分析表、六组 PNG/PDF |", "| A4 运行/交接 | 完成 | 统一入口、运行指南、文件清单及摘要 |", "| A5 报告 | 完成 | 完整英文正文、图注、站点稳定性、B 合并指南 |", "| A6 独立核验准备 | 完成 | 21 合成/15 真实查询、15 已知答案测试、Spark 对照、导出检查接口 |", "| A6 B/C 实际联调 | 待代码回交 | 窗口/特征拟合审核、模型加载预测、整个项目运行 |", "", f"核心文件 {result['core_files']} 个，{result['core_bytes']:,} 字节（{result['core_bytes']/10**9:.3f} GB）；可选原始 CSV {result['optional_raw_bytes']:,} 字节（{result['optional_raw_bytes']/10**9:.3f} GB）。这是未压缩文件容量；清单/验收补充文件无循环摘要，按 handoff_policy 一并交接。", "", "所有核心与八个原始 CSV 的大小和 SHA 已核对；正式数据 48,538,625 行/96 文件，既有六组图表保持不变。只验收 A 已有数据流程和本轮新增材料，没有重新下载全量数据或在 B/C 电脑上验证。", "", "## B 先读", "", "1. README.md、docs/RUN_GUIDE.md 和 docs/DATA_CONTRACT_V1.md。", "2. docs/HANDOFF_B.md、docs/MODULE_INTEGRATION.md。", "3. docs/REPORT_A_FULL.md 与 docs/REPORT_A_MERGE_GUIDE.md。", "4. docs/WINDOW_CHECK_GUIDE.md，使用自己的实现回交两组诊断导出。", "", "## 证据", "", "各阶段 artifacts/stageN/final_verification.json 与检查报告；最新文件清单 artifacts/stage4/handoff_manifest.json。阶段状态分别按实际范围解释，尤其 Stage 6 是独立核验准备。", "", "Lecture 5/6 不需要提供。当前未创建 ZIP、未向组员发消息；用户明确要求打包后，按这份已核验清单整理交接包。", ""]
    (ROOT / "artifacts/A_FINAL_CHECK_REPORT.md").write_text("\n".join(lines))
    print(f"A independent work READY_FOR_B: {result['core_files']} core files; actual B/C integration pending; no ZIP")


if __name__ == "__main__":
    main()
