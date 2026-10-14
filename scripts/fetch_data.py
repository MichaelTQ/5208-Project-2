"""Fetch official metadata and complete annual CSVs with resumable downloads."""
import argparse
import csv
import json
import re
import time

from common import ROOT, RAW_COLUMNS, PublicAPI, config, download, export_size, now_sgt, save_json


def ordered_columns(metadata):
    columns = metadata["columnMetadata"]
    return [columns["map"][key] for key in columns["order"]]


def download_link(api, dataset_id):
    base = f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}"
    api.get(base + "/initiate-download")
    for _ in range(30):
        data = api.get(base + "/poll-download")["data"]
        if data.get("url"):
            if not data["url"].startswith("https://"):
                raise RuntimeError("Download URL is not HTTPS")
            return data["url"]
        time.sleep(15)
    raise RuntimeError("Download preparation did not complete")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata-only", action="store_true")
    args = parser.parse_args()
    api = PublicAPI()
    cfg = config()
    metadata_dir = ROOT / "data/metadata"
    collection_path = metadata_dir / "collection_2279.json"
    if not collection_path.exists():
        save_json(collection_path, api.get("https://api-production.data.gov.sg/v2/public/api/collections/2279/metadata"))
    collection = json.loads(collection_path.read_text())["data"]["collectionMetadata"]
    assert all(d in collection["childDatasets"] for d in cfg["datasets"].values())
    manifest = []
    for year, dataset_id in cfg["datasets"].items():
        metadata_path = metadata_dir / f"rainfall_{year}.json"
        if not metadata_path.exists():
            obj = api.get(f"https://api-production.data.gov.sg/v2/public/api/datasets/{dataset_id}/metadata")
            save_json(metadata_path, obj)
        metadata = json.loads(metadata_path.read_text())["data"]
        assert metadata["datasetId"] == dataset_id
        assert re.search(rf"\({year}\)", metadata["name"])
        assert metadata["format"] == "CSV"
        assert ordered_columns(metadata) == RAW_COLUMNS
        rows_path = metadata_dir / f"rows_{year}.json"
        if not rows_path.exists():
            save_json(rows_path, api.get(f"https://data.gov.sg/api/action/datastore_search?resource_id={dataset_id}&limit=1"))
        official_rows = json.loads(rows_path.read_text())
        assert official_rows["success"] is True
        official_row_count = official_rows["result"]["total"]
        path = ROOT / f"data/raw/rainfall_{year}.csv"
        record_path = metadata_dir / f"download_{year}.json"
        if not args.metadata_only:
            print(f"{year}: downloading complete original CSV", flush=True)
            if path.exists() and record_path.exists():
                old = json.loads(record_path.read_text())
                details = download("", path, old["size_bytes"], old["sha256"])
            else:
                url = download_link(api, dataset_id)
                actual_export_size = export_size(url, ROOT / f"artifacts/stage1/download_{year}.headers")
                details = download(url, path, actual_export_size)
            previous = json.loads(record_path.read_text()) if record_path.exists() else {}
            previous.update({"downloaded_at_sgt": previous.get("downloaded_at_sgt", now_sgt()), **details,
                             "official_row_count": official_row_count,
                             "catalogue_size_bytes": metadata["datasetSize"],
                             "export_size_bytes": details["size_bytes"],
                             "integrity": "HTTP export length verified; local SHA-256 recorded; logical row count checked by extract_samples.py"})
            save_json(record_path, previous)
            print(f"{year}: verified {details['size_bytes']:,} bytes", flush=True)
        saved = json.loads(record_path.read_text()) if record_path.exists() else {}
        manifest.append({
            "year": int(year), "dataset_id": dataset_id,
            "source_url": f"https://data.gov.sg/datasets/{dataset_id}/view",
            "metadata_path": str(metadata_path.relative_to(ROOT)),
            "metadata_last_updated": metadata["lastUpdatedAt"],
            "catalogue_coverage_start": metadata["coverageStart"],
            "catalogue_coverage_end": metadata["coverageEnd"],
            "catalogue_size_bytes": metadata["datasetSize"],
            "official_row_count": official_row_count,
            "local_path": str(path.relative_to(ROOT)),
            "status": "downloaded" if path.exists() and saved else "metadata_verified",
            "downloaded_at_sgt": saved.get("downloaded_at_sgt", ""),
            "size_bytes": saved.get("size_bytes", ""),
            "sha256": saved.get("sha256", ""),
            "row_count": saved.get("row_count", ""),
        })
        dest = ROOT / "data/data_manifest.csv"
        with dest.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=manifest[0].keys())
            writer.writeheader()
            writer.writerows(manifest)
    print("Eight annual sources verified.", flush=True)
    if not args.metadata_only:
        save_json(ROOT / "artifacts/stage1/downloads_complete.json", {"completed_at_sgt": now_sgt(), "annual_files": 8})


if __name__ == "__main__":
    main()
