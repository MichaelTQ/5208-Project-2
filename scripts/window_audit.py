"""Independent exact-time/Decimal oracle for bounded B window audit exports."""
import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

from common import ROOT, now_sgt, save_json, sha256_file

TZ = ZoneInfo("Asia/Singapore")


def audit_config():
    return json.loads((ROOT / "config/window_audit.json").read_text())


def grid_time(value):
    dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("Timezone offset required")
    dt = dt.astimezone(TZ)
    if dt.minute % 5 or dt.second or dt.microsecond:
        raise ValueError("Timestamp is not on the five-minute grid")
    return dt


def amount(value):
    try:
        d = Decimal(str(value))
        if not d.is_finite() or d < 0 or d >= Decimal("100000000000") or d != d.quantize(Decimal("0.000000001")):
            raise ValueError("Nonnegative Decimal(20,9) amount required")
    except InvalidOperation as error:
        raise ValueError("Invalid decimal amount") from error
    return d


def load_observations(path):
    index = {}
    with Path(path).open(newline="") as handle:
        for r in csv.DictReader(handle):
            s = r["station_id"]
            if not s or s != s.strip():
                raise ValueError("Invalid station_id")
            key = s, grid_time(r["observation_time"])
            if key in index:
                raise ValueError(f"Duplicate observation key: {key}")
            index[key] = amount(r["rainfall_mm"])
            if len(index) > 100000:
                raise ValueError("This checker is for bounded fixtures (at most 100,000 observations)")
    return index


def split_at(t, cfg):
    for s in cfg["splits"]:
        if grid_time(s["start"]) <= t < grid_time(s["end_exclusive"]):
            return s["name"]
    return None


def expected_answer(index, query, cfg=None):
    cfg = cfg or audit_config()
    s = query["station_id"]
    if not s or s != s.strip():
        raise ValueError("Invalid query station_id")
    t, q = grid_time(query["prediction_time"]), amount(query["q_mm"])
    h = int(query.get("history_slots", cfg["example_history_slots_including_t"]))
    if not 1 <= h <= 2016:
        raise ValueError("Audit history_slots must be 1..2016 (bounded checker, not a model restriction)")
    step = timedelta(seconds=cfg["interval_seconds"])
    future = [t + k * step for k in range(1, cfg["future_slots"] + 1)]
    history = [t - k * step for k in range(h)]
    missing_future = [x.isoformat() for x in future if (s, x) not in index]
    missing_history = [x.isoformat() for x in history if (s, x) not in index]
    complete = not missing_future
    name = split_at(t, cfg)
    within = name is not None and all(split_at(x, cfg) == name for x in future)
    usable = complete and within
    total = sum((index[(s, x)] for x in future), Decimal(0)) if complete else None
    return {"query_id": query["query_id"], "station_id": s, "prediction_time": t.isoformat(), "q_mm": format(q, ".9f"), "split": name, "anchor_observed": (s, t) in index, "future_complete": complete, "missing_future_times": missing_future, "future_within_split": within, "label_usable": usable, "future_sum_mm": format(total, ".9f") if total is not None else None, "label_exceeds_q": int(total > q) if usable else None, "history_slots": h, "history_complete": not missing_history, "missing_history_times": missing_history, "example_evaluation_usable": usable and not missing_history}


def oracle_answers(observations, queries, cfg=None):
    index = load_observations(observations)
    with Path(queries).open(newline="") as handle:
        qs = list(csv.DictReader(handle))
    ids = [q["query_id"] for q in qs]
    if not ids or any(not x for x in ids) or len(ids) != len(set(ids)) or len(ids) > 5000:
        raise ValueError("Nonempty unique query_id values required; at most 5,000 bounded queries")
    return [expected_answer(index, q, cfg) for q in qs]


def compare_answers(expected, actual):
    if not isinstance(actual, list):
        raise ValueError("Audit export must be a JSON list")
    if any(not isinstance(r, dict) for r in actual):
        raise ValueError("Every audit export row must be a JSON object")
    ids = [r.get("query_id") for r in actual]
    if any(not isinstance(qid, str) or not qid for qid in ids):
        raise ValueError("Every audit export row requires a nonempty string query_id")
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate query_id in audit export")
    e, a = {r["query_id"]: r for r in expected}, {r["query_id"]: r for r in actual}
    mismatches = []
    if e.keys() != a.keys():
        mismatches.append({"field": "query_id_set", "missing": sorted(e.keys() - a.keys()), "extra": sorted(a.keys() - e.keys())})
    for query_id in e.keys() & a.keys():
        for field, value in e[query_id].items():
            if field not in a[query_id]:
                mismatches.append({"query_id": query_id, "field": field, "error": "missing field"})
                continue
            found = a[query_id][field]
            if field in ["q_mm", "future_sum_mm"] and value is not None:
                # Outputs must retain decimals as strings: do not silently accept floats.
                good = isinstance(found, str) and Decimal(found).is_finite() and Decimal(found) == Decimal(value)
            else:
                good = type(found) is type(value) and found == value
            if not good:
                mismatches.append({"query_id": query_id, "field": field, "expected": value, "actual": found})
    return mismatches


def main():
    parser = argparse.ArgumentParser(description="A exact-slot oracle; bounded diagnostic queries only")
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--actual", type=Path, help="B's JSON export; omission only writes A reference answers")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    inputs = [args.observations, args.queries, args.actual, ROOT / "config/window_audit.json"]
    if args.output.resolve() in [p.resolve() for p in inputs if p is not None]:
        parser.error("Output cannot overwrite an audit input")
    args.output.unlink(missing_ok=True)
    try:
        expected = oracle_answers(args.observations, args.queries)
        mismatches = compare_answers(expected, json.loads(args.actual.read_text())) if args.actual else []
        result = {"status": ("FAIL" if mismatches else "PASS") if args.actual else "REFERENCE_ONLY", "scope": "bounded window diagnostic; no validation of preprocessing fit or C model", "checked_at_sgt": now_sgt(), "queries": len(expected), "inputs": [{"path": str(p), "sha256": sha256_file(p)} for p in [args.observations, args.queries, ROOT / "config/window_audit.json"]], "actual_sha256": sha256_file(args.actual) if args.actual else None, "mismatches": mismatches, "expected_answers": expected if not args.actual else None}
        save_json(args.output, result)
        print(f"Window audit {result['status']}: {len(expected)} queries; {len(mismatches)} mismatches")
        return 1 if mismatches else 0
    except (OSError, ValueError, KeyError, InvalidOperation, TypeError) as error:
        save_json(args.output, {"status": "FAIL", "checked_at_sgt": now_sgt(), "error": str(error)})
        print(f"Window audit FAIL: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
