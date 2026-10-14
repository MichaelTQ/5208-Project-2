"""Independent final artifact audit; recompute original-file checksums."""
import csv
import io
import json
import unittest
from collections import defaultdict
from datetime import datetime

from common import ROOT, RAW_COLUMNS, config, now_sgt, save_json, sha256_file


def main():
    cfg = config()
    with (ROOT / "data/data_manifest.csv").open(newline="") as handle:
        manifest = list(csv.DictReader(handle))
    assert len(manifest) == 8
    assert sorted(row["year"] for row in manifest) == sorted(cfg["datasets"])
    audited = []
    sample_rows = []
    for record in manifest:
        year = record["year"]
        path = ROOT / record["local_path"]
        metadata = json.loads((ROOT / record["metadata_path"]).read_text())["data"]
        download = json.loads((ROOT / f"data/metadata/download_{year}.json").read_text())
        official = json.loads((ROOT / f"data/metadata/rows_{year}.json").read_text())
        profile = json.loads((ROOT / f"artifacts/stage1/raw_profile_{year}.json").read_text())
        assert record["dataset_id"] == cfg["datasets"][year] == metadata["datasetId"]
        assert record["status"] == "downloaded"
        assert path.stat().st_size == int(record["size_bytes"]) == download["export_size_bytes"]
        digest = sha256_file(path)
        assert digest == record["sha256"] == download["sha256"]
        assert int(record["row_count"]) == profile["row_count"] == download["row_count"] == official["result"]["total"]
        assert profile["malformed_width_records"] == 0
        with path.open(newline="", encoding="utf-8-sig") as handle:
            assert next(csv.reader(handle)) == RAW_COLUMNS
        sample_path = ROOT / profile["sample_path"]
        assert sha256_file(sample_path) == profile["sample_sha256"]
        with sample_path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == profile["sample_rows"]
        observed_dates = {r["timestamp"][:10] for r in rows}
        assert set(profile["selected_dates"]) == observed_dates
        actual_stations = {r["station_id"] for r in rows}
        assert actual_stations <= set(cfg["sample_stations"])
        assert len(actual_stations) >= 2
        sample_rows.extend((int(year), r) for r in rows)
        audited.append({"year": int(year), "raw_rows": profile["row_count"], "size_bytes": path.stat().st_size,
                        "catalogue_size_bytes": metadata["datasetSize"], "sha256_rechecked": True,
                        "sample_rows": len(rows), "first_timestamp": profile["first_timestamp_lexical"],
                        "last_timestamp": profile["last_timestamp_lexical"],
                        "sample_station_ids": sorted(actual_stations),
                        "candidate_stations_absent_from_sample": sorted(set(cfg["sample_stations"]) - actual_stations)})
    assert not list((ROOT / "data/raw").glob("*.part"))
    spark = json.loads((ROOT / "artifacts/stage1/spark_verification.json").read_text())
    assert all(spark["assertions"].values())
    assert spark["rows"] == len(sample_rows)
    environment = json.loads((ROOT / "artifacts/environment/environment.json").read_text())
    assert environment["python_version"] == cfg["python_version"]
    assert environment["packages"]["pyspark"] == cfg["spark_version"]
    assert environment["spark_timezone"] == cfg["timezone"]
    assert environment["actual_jvm_max_heap_bytes"] == 4 * 1024**3
    java = json.loads((ROOT / "artifacts/environment/java_install.json").read_text())
    assert java["publisher_sha256_verified"] is True
    installed = environment["packages"]
    lock = dict(line.split("==", 1) for line in (ROOT / "requirements.lock.txt").read_text().splitlines() if "==" in line)
    assert all(lock[name] == version for name, version in installed.items())
    boundary_times = defaultdict(list)
    for year, row in sample_rows:
        dt = datetime.fromisoformat(row["timestamp"])
        if dt.strftime("%m-%d") in ("01-01", "12-31"):
            boundary_times[(year, row["station_id"], dt.strftime("%m-%d"))].append(dt)
    boundaries = []
    for year in range(2018, 2025):
        for station in cfg["sample_stations"]:
            previous = boundary_times.get((year - 1, station, "12-31"), [])
            following = boundary_times.get((year, station, "01-01"), [])
            gap = (min(following) - max(previous)).total_seconds() if previous and following else None
            boundaries.append({"boundary_year": year, "station_id": station,
                               "previous_last_timestamp": max(previous).isoformat() if previous else None,
                               "next_first_timestamp": min(following).isoformat() if following else None,
                               "gap_seconds": gap, "five_minute_continuity": gap == 300})
    save_json(ROOT / "artifacts/stage1/year_boundary_examples.json", boundaries)
    tests = unittest.defaultTestLoader.discover(str(ROOT / "tests"), pattern="test_integrity.py")
    output = io.StringIO()
    tested = unittest.TextTestRunner(stream=output, verbosity=2).run(tests)
    (ROOT / "artifacts/stage1/test_results.txt").write_text(output.getvalue())
    assert tested.wasSuccessful()
    for name in ["README.md", "docs/STAGE_1_PLAN.md", "docs/ENVIRONMENT.md", "docs/DATA_CONTRACT.md", "docs/AI_USAGE.md"]:
        assert (ROOT / name).is_file()
    report = {
        "checked_at_sgt": now_sgt(), "status": "PASS", "annual_files": audited,
        "total_raw_rows": sum(row["raw_rows"] for row in audited),
        "total_raw_bytes": sum(row["size_bytes"] for row in audited),
        "total_sample_rows": len(sample_rows), "unit_tests_run": tested.testsRun,
        "unit_tests_passed": tested.wasSuccessful(), "spark_assertions": spark["assertions"],
        "boundary_examples": len(boundaries),
        "continuous_five_minute_boundaries": sum(r["five_minute_continuity"] for r in boundaries),
        "limitations": [
            "五分钟区间的起止语义和更新字段含义未确认；本阶段不构造标签。",
            "样本结果不能代表全量缺失、异常、重复或站点历史质量。",
            "首末日开发样本中的真实缺口予以保留，不填零。",
            "仅本机 macOS ARM64 环境得到实际验证；Lecture 5/6 不需要提供。",
        ],
    }
    save_json(ROOT / "artifacts/stage1/final_verification.json", report)
    lines = ["# Stage 1 检查报告", "", f"检查时间：{report['checked_at_sgt']}。结果：**PASS**。", "",
             "## 已完成", "",
             f"- 八年完整原始 CSV：{report['total_raw_rows']:,} 条逻辑记录，共 {report['total_raw_bytes']:,} 字节（{report['total_raw_bytes']/1024**3:.2f} GiB）。",
             f"- 连续真实样本：{report['total_sample_rows']:,} 条，整体覆盖八年及四个候选站点；逐年实际站点见下表。缺少的站点观测不补造。",
             "- 八个文件重新计算 SHA-256，与下载清单一致；实际行数与官方 datastore 总数一致。",
             "- 目录大小与 API 导出大小分别保存。完整性按 HTTP 导出总字节数和官方行数验收。",
             "- Python 3.11.14、Temurin Java 17、Spark 3.5.8 启动和实际计算通过。JVM 最大堆为 4 GiB。",
             "- 样本 CSV 读取、三个时间字段解析、SGT 显示与 epoch 核对、Parquet 保存/重读及记录多重集合一致性均通过。",
             f"- {tested.testsRun} 项必要的完整性逻辑测试通过。", "",
             "## 年度文件核对", "", "| 年份 | 官方/实际行数 | 实际导出字节数 | 样本行数 | 样本实际站点 |", "|---|---:|---:|---:|---|"]
    lines.extend(f"| {r['year']} | {r['raw_rows']:,} | {r['size_bytes']:,} | {r['sample_rows']:,} | {', '.join(r['sample_station_ids'])} |" for r in audited)
    lines += ["", "## 样本检查结果", "", f"- 零降雨：{spark['sample_checks']['zero_rainfall']:,} 条；正降雨：{spark['sample_checks']['positive_rainfall']:,} 条。",
              f"- 观测时间解析失败：{spark['sample_checks']['invalid_observation_time']}；数值解析失败：{spark['sample_checks']['invalid_rainfall']}。",
              f"- 同站点同时间重复键：{spark['duplicate_station_time_keys']}（原样保留）。",
              "- 2017 非零秒时间戳原样保留，未擅自取整。",
              f"- {len(boundaries)} 个站点跨年边界案例中，{report['continuous_five_minute_boundaries']} 个相邻观测间隔恰好为五分钟；其余见 year_boundary_examples.json，不补齐、不填零。",
              "", "## 尚待下一阶段确认", ""]
    lines.extend(f"- {item}" for item in report["limitations"])
    lines += ["", "## 复查入口", "", "```bash", "bash scripts/run.sh scripts/check_stage1.py", "```", "",
              "来源清单：data/data_manifest.csv；环境：artifacts/environment/environment.json；Spark 证据：spark_verification.json；完整验收证据：final_verification.json。", ""]
    (ROOT / "artifacts/stage1/STAGE_1_CHECK_REPORT.md").write_text("\n".join(lines))
    print(f"Stage 1 PASS: {report['total_raw_rows']:,} original records, {len(sample_rows):,} sample records, {tested.testsRun} tests.")


if __name__ == "__main__":
    main()
