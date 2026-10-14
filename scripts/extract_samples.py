"""Count logical CSV records and take complete station-day slices, without cleaning."""
import argparse
import csv
import json
import time

from common import ROOT, RAW_COLUMNS, config, now_sgt, save_json, sha256_file


def extract_year(year, stations):
    path = ROOT / f"data/raw/rainfall_{year}.csv"
    started = time.monotonic()
    count = 0
    malformed = 0
    minimum = None
    maximum = None
    first_wet_day = None
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        if header != RAW_COLUMNS:
            raise RuntimeError(f"Unexpected original CSV header in {year}: {header}")
        for row in reader:
            count += 1
            if len(row) != len(header):
                malformed += 1
                continue
            timestamp = row[1]
            if timestamp:
                minimum = timestamp if minimum is None else min(minimum, timestamp)
                maximum = timestamp if maximum is None else max(maximum, timestamp)
            if row[3] in stations:
                try:
                    wet = float(row[9]) > 0
                except ValueError:
                    wet = False
                if wet:
                    day = timestamp[:10]
                    first_wet_day = day if first_wet_day is None else min(first_wet_day, day)
    if not minimum or not maximum or not first_wet_day:
        raise RuntimeError(f"Could not establish sample days for {year}")
    selected_days = sorted({minimum[:10], maximum[:10], first_wet_day})
    sample_path = ROOT / f"data/samples/raw/rainfall_{year}_sample.csv"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    sample_count = 0
    with path.open(newline="", encoding="utf-8-sig") as handle, sample_path.open("w", newline="") as output:
        reader = csv.reader(handle)
        next(reader)
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(RAW_COLUMNS)
        for row in reader:
            if len(row) == len(RAW_COLUMNS) and row[3] in stations and row[1][:10] in selected_days:
                writer.writerow(row)
                sample_count += 1
    download_path = ROOT / f"data/metadata/download_{year}.json"
    download_record = json.loads(download_path.read_text())
    if count != download_record["official_row_count"]:
        raise RuntimeError(f"Official row count mismatch for {year}: {count} != {download_record['official_row_count']}")
    download_record.update({"row_count": count, "header_verified": True})
    save_json(download_path, download_record)
    result = {
        "year": year, "row_count": count, "malformed_width_records": malformed,
        "first_timestamp_lexical": minimum, "last_timestamp_lexical": maximum,
        "selected_station_ids": sorted(stations), "selected_dates": selected_days,
        "first_wet_date_for_selected_stations": first_wet_day,
        "sample_path": str(sample_path.relative_to(ROOT)),
        "sample_rows": sample_count, "sample_sha256": sha256_file(sample_path),
        "elapsed_seconds": round(time.monotonic() - started, 2),
        "scope": "Full-file logical record count/header/width; other quality checks are sample-only.",
    }
    save_json(ROOT / f"artifacts/stage1/raw_profile_{year}.json", result)
    print(f"{year}: counted {count:,} records; selected {sample_count:,} sample records", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait-for-downloads", action="store_true")
    args = parser.parse_args()
    cfg = config()
    profiles = []
    for value in cfg["datasets"]:
        year = int(value)
        path = ROOT / f"data/raw/rainfall_{year}.csv"
        record_path = ROOT / f"data/metadata/download_{year}.json"
        deadline = time.monotonic() + 7200
        while not (path.exists() and record_path.exists()):
            if not args.wait_for_downloads or time.monotonic() > deadline:
                raise RuntimeError(f"Complete verified raw CSV not available: {year}")
            time.sleep(5)
        profiles.append(extract_year(year, set(cfg["sample_stations"])))
    save_json(ROOT / "artifacts/stage1/sample_selection.json", {
        "generated_at_sgt": now_sgt(),
        "method": "All original records for four station IDs on first/last observed dates and earliest wet date in each year; overlapping dates are included once.",
        "raw_columns_unchanged": True, "profiles": profiles,
        "warning": "Development fixtures only; weather-biased selection is not a modelling sample or a representative climate sample.",
    })
    # Wait for the downloader's final manifest before adding logical row counts.
    while not (ROOT / "artifacts/stage1/downloads_complete.json").exists():
        if not args.wait_for_downloads:
            raise RuntimeError("Download manifest has not been finalized")
        time.sleep(2)
    manifest_path = ROOT / "data/data_manifest.csv"
    with manifest_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    by_year = {str(p["year"]): p for p in profiles}
    for row in rows:
        row["row_count"] = by_year[row["year"]]["row_count"]
    with manifest_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
