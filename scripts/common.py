"""Small standard-library helpers; HTTPS verification remains enabled."""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
RAW_COLUMNS = [
    "date", "timestamp", "update_timestamp", "station_id", "station_name",
    "station_device_id", "location_longitude", "location_latitude",
    "reading_update_timestamp", "reading_value", "reading_type", "reading_unit",
]


def now_sgt():
    return datetime.now(ZoneInfo("Asia/Singapore")).isoformat(timespec="seconds")


def config():
    return json.loads((ROOT / "config/project.json").read_text())


def save_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    tmp.replace(path)


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class PublicAPI:
    """Keep API calls below the documented unauthenticated 5 requests/minute."""
    def __init__(self, interval=13):
        self.interval = interval
        self.last_call = 0.0

    def get(self, url):
        for attempt in range(4):
            time.sleep(max(0, self.interval - (time.monotonic() - self.last_call)))
            self.last_call = time.monotonic()
            result = subprocess.run([
                "curl", "--fail", "--silent", "--show-error", "--location",
                "--connect-timeout", "20", "--max-time", "120", url,
            ], capture_output=True, text=True)
            if result.returncode == 0:
                value = json.loads(result.stdout)
                if isinstance(value, dict) and value.get("code", 0) != 0:
                    raise RuntimeError(f"API returned an error: {value.get('errorMsg')}")
                return value
            if attempt == 3:
                raise RuntimeError(f"HTTPS request failed: {result.stderr.strip()}")
            time.sleep(15 * (attempt + 1))


def download(url, destination, expected_size=None, expected_sha=None):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    if not destination.exists() and not (part.exists() and expected_size is not None and part.stat().st_size == expected_size):
        process = subprocess.run([
            "curl", "--fail", "--silent", "--show-error", "--location",
            "--retry", "4", "--retry-delay", "5", "--connect-timeout", "30",
            "--continue-at", "-", "--output", str(part), url,
        ], capture_output=True, text=True)
        if process.returncode:
            raise RuntimeError(f"Download failed for {destination.name}: {process.stderr.strip()}")
    if not destination.exists():
        if expected_size is not None and part.stat().st_size != expected_size:
            raise RuntimeError(f"Download size mismatch: {destination.name}")
        if expected_sha and sha256_file(part) != expected_sha:
            raise RuntimeError(f"Download checksum mismatch: {destination.name}")
        part.replace(destination)
    if expected_size is not None and destination.stat().st_size != expected_size:
        raise RuntimeError(f"Existing file size mismatch: {destination.name}")
    checksum = sha256_file(destination)
    if expected_sha and checksum != expected_sha:
        raise RuntimeError(f"Existing file checksum mismatch: {destination.name}")
    return {"size_bytes": destination.stat().st_size, "sha256": checksum}


def export_size(url, header_path):
    """GET Range is used because official signed GetObject links reject HEAD."""
    header_path = Path(header_path)
    header_path.parent.mkdir(parents=True, exist_ok=True)
    process = subprocess.run([
        "curl", "--fail", "--silent", "--show-error", "--location", "--range", "0-0",
        "--max-filesize", "1048576", "--connect-timeout", "30", "--max-time", "120",
        "--dump-header", str(header_path), "--output", "/dev/null", url,
    ], capture_output=True, text=True)
    if process.returncode not in (0, 63):
        raise RuntimeError(f"CSV export size probe failed: {process.stderr.strip()}")
    headers = header_path.read_text()
    ranges = re.findall(r"^content-range:\s*bytes\s+\d+-\d+/(\d+)", headers, re.I | re.M)
    if ranges:
        return int(ranges[-1])
    sizes = re.findall(r"^content-length:\s*(\d+)", headers, re.I | re.M)
    if sizes and int(sizes[-1]) > 1:
        return int(sizes[-1])
    raise RuntimeError("Official export did not provide a total byte count")
