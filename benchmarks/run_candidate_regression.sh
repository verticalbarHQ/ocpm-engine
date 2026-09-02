#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 BASELINE_SOURCE OUTPUT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANDIDATE_SOURCE="$(cd "${SCRIPT_DIR}/.." && pwd)"
BASELINE_SOURCE="$(cd "$1" && pwd)"
OUTPUT="$2"
if [[ "${OUTPUT}" != /* ]]; then
  OUTPUT="${CANDIDATE_SOURCE}/${OUTPUT}"
fi

RUN_ROOT="$(mktemp -d)"
cleanup() {
  find "${RUN_ROOT}" -depth -delete
}
trap cleanup EXIT

python "${SCRIPT_DIR}/build_candidate_worker.py" \
  --engine-source "${BASELINE_SOURCE}" \
  --build-root "${RUN_ROOT}/baseline-build" \
  --output "${RUN_ROOT}/baseline-worker"
python "${SCRIPT_DIR}/build_candidate_worker.py" \
  --engine-source "${CANDIDATE_SOURCE}" \
  --build-root "${RUN_ROOT}/candidate-build" \
  --output "${RUN_ROOT}/candidate-worker"
python "${SCRIPT_DIR}/candidate_regression.py" \
  --baseline-source "${BASELINE_SOURCE}" \
  --candidate-source "${CANDIDATE_SOURCE}" \
  --baseline-worker "${RUN_ROOT}/baseline-worker" \
  --candidate-worker "${RUN_ROOT}/candidate-worker" \
  --manifest "${SCRIPT_DIR}/fixtures/candidate-gate-engine.json" \
  --output "${OUTPUT}"
