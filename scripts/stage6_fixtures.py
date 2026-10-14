"""Build bounded synthetic and real audit fixtures; not production training labels."""
import csv
import json
from datetime import datetime, timedelta
from decimal import Decimal

from common import ROOT, now_sgt, save_json, sha256_file
from window_audit import grid_time, oracle_answers, load_observations


def write_csv(path, data, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        w = csv.DictWriter(handle, fieldnames=columns)
        w.writeheader()
        w.writerows(data)


def main():
    out = ROOT / "data/handoff/stage6_window_checks"
    out.mkdir(parents=True, exist_ok=True)
    observations, queries = {}, []

    def add(name, anchor, rain="0.2", missing=(), q="1.2", values=None):
        t = grid_time(anchor)
        for k in range(-12, 10):
            if k in missing:
                continue
            value = (values or {}).get(k, rain)
            observations[(name, t + timedelta(minutes=5 * k))] = value
        queries.append({"query_id": name, "station_id": name, "prediction_time": t.isoformat(), "q_mm": q, "history_slots": 12, "case": name})

    add("SYN_EQUAL", "2022-03-10T12:00:00+08:00")
    add("SYN_ABOVE", "2022-03-10T12:00:00+08:00", q="1.199999999")
    add("SYN_BELOW", "2022-03-10T12:00:00+08:00", q="1.200000001")
    add("SYN_DRY", "2022-03-10T12:00:00+08:00", rain="0", q="0")
    add("SYN_GAP_SIX_ROWS", "2022-03-10T12:00:00+08:00", missing=(3,), q="0.5")
    add("SYN_HISTORY_GAP", "2022-03-10T12:00:00+08:00", missing=(-2,), q="0.5")
    add("SYN_NO_ANCHOR", "2022-03-10T12:00:00+08:00", missing=(0,), q="0.5")
    add("SYN_EXCLUDE_T", "2022-03-10T12:00:00+08:00", rain="0", values={0: "37.2"}, q="0")
    add("SYN_NINE_DECIMALS", "2022-03-10T12:00:00+08:00", rain="0.123456789", q="0.740740733")
    add("SYN_MIDNIGHT", "2022-03-10T23:45:00+08:00", q="0.5")
    add("SYN_LEAP_DAY", "2020-02-28T23:45:00+08:00", q="0.5")
    add("SYN_TRAIN_YEAR", "2020-12-31T23:45:00+08:00", q="0.5")
    add("SYN_TRAIN_LAST_OK", "2022-12-31T23:25:00+08:00", q="0.5")
    add("SYN_TRAIN_CROSS", "2022-12-31T23:30:00+08:00", q="0.5")
    add("SYN_VALIDATION_CROSS", "2023-12-31T23:30:00+08:00", q="0.5")
    add("SYN_TEST_END", "2024-12-31T23:30:00+08:00", q="0.5")
    add("SYN_VAL_HISTORY_TRAIN", "2023-01-01T00:00:00+08:00", q="0.5")
    add("SYN_LAST_RECORD", "2024-07-15T12:00:00+08:00", missing=tuple(range(1, 10)), q="0")
    add("SYN_STATION_A", "2022-03-10T12:00:00+08:00", rain="0", q="0")
    add("SYN_STATION_B", "2022-03-10T12:00:00+08:00", rain="1", q="0")
    add("SYN_OUTSIDE_SPLIT", "2016-12-31T12:00:00+08:00", q="0.5")
    synthetic_rows = [{"station_id": s, "observation_time": t.isoformat(), "rainfall_mm": v} for (s, t), v in sorted(observations.items())]
    write_csv(out / "synthetic_observations.csv", synthetic_rows, ["station_id", "observation_time", "rainfall_mm"])
    write_csv(out / "synthetic_queries.csv", queries, list(queries[0]))
    expected = oracle_answers(out / "synthetic_observations.csv", out / "synthetic_queries.csv")
    save_json(out / "synthetic_expected.json", expected)

    sample = ROOT / "data/handoff/stage2_sample.csv"
    with sample.open(newline="") as handle:
        original = list(csv.DictReader(handle))
    real_rows = [{k: r[k] for k in ["station_id", "observation_time", "rainfall_mm"]} for r in original]
    write_csv(out / "real_observations.csv", real_rows, ["station_id", "observation_time", "rainfall_mm"])
    index = load_observations(out / "real_observations.csv")
    real_queries, selected = [], set()

    def pick(case, predicate):
        for s, t in sorted(index):
            if (s, t) in selected:
                continue
            future = [index.get((s, t + timedelta(minutes=5 * k))) for k in range(1, 7)]
            history = [index.get((s, t - timedelta(minutes=5 * k))) for k in range(12)]
            if predicate(t, future, history):
                selected.add((s, t))
                q = sum(future, Decimal(0)) if all(v is not None for v in future) else Decimal(0)
                qid = f"REAL_{len(real_queries)+1:02d}_{case}"
                real_queries.append({"query_id": qid, "station_id": s, "prediction_time": t.isoformat(), "q_mm": str(q), "history_slots": 12, "case": case})
                if q > 0:
                    real_queries.append({"query_id": qid + "_BELOW_Q", "station_id": s, "prediction_time": t.isoformat(), "q_mm": str(q - Decimal("0.000000001")), "history_slots": 12, "case": case + "_strict_boundary"})
                return
        raise RuntimeError(f"Real sample does not contain requested case: {case}")

    for year in range(2017, 2025):
        pick(f"year_{year}_complete", lambda t, f, h, y=year: t.year == y and all(v is not None for v in f + h))
    pick("wet_complete", lambda t, f, h: all(v is not None for v in f + h) and sum(f) > 0)
    pick("dry_complete", lambda t, f, h: all(v is not None for v in f + h) and sum(f) == 0)
    pick("missing_future", lambda t, f, h: any(v is None for v in f))
    pick("missing_history", lambda t, f, h: all(v is not None for v in f) and any(v is None for v in h))
    pick("cross_midnight", lambda t, f, h: all(v is not None for v in f) and (t + timedelta(minutes=30)).date() != t.date())
    write_csv(out / "real_queries.csv", real_queries, list(real_queries[0]))
    real_expected = oracle_answers(out / "real_observations.csv", out / "real_queries.csv")
    save_json(out / "real_expected.json", real_expected)
    artifact = ROOT / "artifacts/stage6"
    artifact.mkdir(parents=True, exist_ok=True)
    figures = json.loads((ROOT / "artifacts/stage3/figure_manifest.json").read_text())
    figure_paths = sorted({f[k] for f in figures for k in ["png", "pdf"]})
    assert len(figure_paths) == 12
    preserved = [{"path": p, "sha256": sha256_file(ROOT / p)} for p in figure_paths]
    save_json(artifact / "fixtures_manifest.json", {"created_at_sgt": now_sgt(), "scope": "synthetic and bounded real sample audit fixtures, not full labels", "generator_sha256": sha256_file(ROOT / "scripts/stage6_fixtures.py"), "synthetic_queries": len(queries), "real_queries": len(real_queries), "real_source_rows": len(original), "real_source_csv_sha256": sha256_file(sample), "preserved_figure_files": preserved, "files": [{"path": p.relative_to(ROOT).as_posix(), "sha256": sha256_file(p)} for p in [*sorted(out.glob("*.csv")), *sorted(out.glob("*.json")), ROOT / "config/window_audit.json"]]})
    print(f"Window fixtures: {len(queries)} synthetic, {len(real_queries)} real queries; {len(original):,} real rows")


if __name__ == "__main__":
    main()
