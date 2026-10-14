"""Install the official pinned macOS ARM64 Temurin archive in this project."""
import platform
import tarfile

from common import ROOT, download, now_sgt, save_json

if platform.system() != "Darwin" or platform.machine() != "arm64":
    raise SystemExit("This helper targets macOS ARM64. Other hosts: install Temurin Java 17; see docs/ENVIRONMENT.md.")

url = "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1/OpenJDK17U-jdk_aarch64_mac_hotspot_17.0.20.1_1.tar.gz"
checksum = "196d13ba5f10414bef7f6a05a9b3f00edacb18ebacef2b99485db9e2ee18f0e8"
archive = ROOT / ".runtime/downloads/temurin17.tar.gz"
result = download(url, archive, 185851019, checksum)
target = ROOT / ".runtime/java"
target.mkdir(parents=True, exist_ok=True)
java_home = target / "jdk-17.0.20.1+1/Contents/Home"
if not (java_home / "bin/java").exists():
    with tarfile.open(archive) as handle:
        handle.extractall(target, filter="data")
save_json(ROOT / "artifacts/environment/java_install.json", {
    "retrieved_at_sgt": now_sgt(), "vendor": "Eclipse Temurin",
    "release": "17.0.20.1+1", "url": url, **result,
    "java_home": str(java_home), "publisher_sha256_verified": True,
})
print("Java 17 archive verified and installed inside the project.", flush=True)
