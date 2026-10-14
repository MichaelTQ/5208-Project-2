"""Check the current machine, rather than trusting A's old environment record."""
import importlib.metadata
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from decimal import Decimal

from common import ROOT, config, now_sgt, save_json


def main():
    target = ROOT / "artifacts/stage4/preflight.json"
    target.unlink(missing_ok=True)
    cfg = config()
    if platform.python_version() != cfg["python_version"]:
        raise RuntimeError("Python version mismatch; run bash scripts/setup.sh")
    lock = dict(line.split("==", 1) for line in (ROOT / "requirements.lock.txt").read_text().splitlines() if "==" in line)
    installed = {name: importlib.metadata.version(name) for name in lock}
    differences = {name: [version, installed[name]] for name, version in lock.items() if installed[name] != version}
    if differences:
        raise RuntimeError(f"Dependency mismatch: {differences}; run bash scripts/setup.sh")
    java = shutil.which("java")
    if not java:
        raise RuntimeError("Java missing; install Temurin 17 and set JAVA_HOME. See docs/ENVIRONMENT.md")
    result = subprocess.run([java, "-version"], capture_output=True, text=True, check=True)
    java_text = result.stderr + result.stdout
    match = re.search(r'version "(\d+)[^\"]*"', java_text)
    if not match or int(match.group(1)) != cfg["java_major"]:
        raise RuntimeError("Java 17 required; check JAVA_HOME. See docs/ENVIRONMENT.md")
    from pyspark.sql import functions as F
    from stage2_common import spark_session
    spark = spark_session("A-current-environment-preflight")
    try:
        assert spark.version == cfg["spark_version"]
        assert spark.sparkContext.master == cfg["spark_master"]
        assert spark.conf.get("spark.sql.session.timeZone") == cfg["timezone"]
        heap = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()
        assert heap == 4 * 1024**3
        assert spark.range(10).agg(F.sum("id")).first()[0] == 45
        row = spark.sql("SELECT unix_timestamp(to_timestamp('2023-01-01T00:00:00+08:00')) AS epoch, CAST('0.123456789' AS DECIMAL(20,9)) + CAST('0.000000001' AS DECIMAL(20,9)) AS amount").first()
        assert row.epoch == 1672502400 and row.amount == Decimal("0.123456790")
        evidence = {"spark_version": spark.version, "spark_timezone": spark.conf.get("spark.sql.session.timeZone"), "spark_master": spark.sparkContext.master, "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"), "actual_jvm_max_heap_bytes": heap, "arithmetic_and_sgt_checks": True}
    finally:
        spark.stop()
    save_json(target, {"status": "PASS", "checked_at_sgt": now_sgt(), "python_version": platform.python_version(), "python_executable": sys.executable, "platform": platform.platform(), "machine": platform.machine(), "cpu_count": os.cpu_count(), "disk_free_bytes": shutil.disk_usage(ROOT).free, "java_executable": java, "java_version_output": java_text.strip(), "packages": installed, **evidence})
    print("Current environment PASS: Python 3.11.14, Java 17, Spark 3.5.8, SGT, 4 GiB heap", flush=True)


if __name__ == "__main__":
    main()
