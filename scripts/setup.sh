#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"
if ! command -v uv >/dev/null 2>&1; then
  echo "Install uv from https://docs.astral.sh/uv/getting-started/installation/ and run this script again."
  exit 1
fi
export UV_PYTHON_INSTALL_DIR="$PROJECT_ROOT/.runtime/python"
export UV_CACHE_DIR="$PROJECT_ROOT/.cache/uv"
uv python install 3.11.14 --no-bin --native-tls --no-config
PROJECT_PYTHON="$(uv python find --managed-python 3.11.14 --no-config)"
if [[ ! -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
  uv venv .venv --python "$PROJECT_PYTHON" --no-config
fi
uv pip install --python .venv/bin/python --requirements requirements.lock.txt --native-tls --no-config --quiet
if [[ "$(uname -s)" == "Darwin" && "$(uname -m)" == "arm64" ]]; then
  "$PROJECT_PYTHON" scripts/install_java.py
else
  echo "Install Eclipse Temurin Java 17 and set JAVA_HOME; see docs/ENVIRONMENT.md."
fi
echo "Project Python environment is ready."
