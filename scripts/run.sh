#!/usr/bin/env bash
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -z "${JAVA_HOME:-}" ]]; then
  export JAVA_HOME="$PROJECT_ROOT/.runtime/java/jdk-17.0.20.1+1/Contents/Home"
fi
if [[ ! -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
  echo "Project Python is missing. Run bash scripts/setup.sh; see docs/ENVIRONMENT.md." >&2
  exit 1
fi
if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
  echo "Java is missing at JAVA_HOME. Install Temurin 17 or run setup.sh on macOS ARM64; see docs/ENVIRONMENT.md." >&2
  exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PROJECT_ROOT/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJECT_ROOT/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1
export PYTHONDONTWRITEBYTECODE=1
mkdir -p "$PROJECT_ROOT/artifacts/spark-local"
export SPARK_LOCAL_DIRS="$PROJECT_ROOT/artifacts/spark-local"
cd "$PROJECT_ROOT"
exec "$PROJECT_ROOT/.venv/bin/python" "$@"
