#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
compose="$root/benchmarks/public/docker-compose.yml"
pg_ocpm_source="${PG_OCPM_SOURCE:-}"
pg_ocpm_repository="${PG_OCPM_REPOSITORY:-}"

if [[ -z "$pg_ocpm_source" ]]; then
    sibling="$(cd "$root/.." && pwd)/pg_ocpm"
    checkout="$root/.benchmarks/pg_ocpm-0.5.0"
    if [[ -f "$sibling/pg_ocpm.control" ]]; then
        pg_ocpm_source="$sibling"
    else
        if [[ -z "$pg_ocpm_repository" ]]; then
            echo "Set PG_OCPM_SOURCE to a local pg_ocpm checkout or" >&2
            echo "set PG_OCPM_REPOSITORY to its public Git repository URL." >&2
            exit 2
        fi
        if [[ ! -d "$checkout/.git" ]]; then
            mkdir -p "$root/.benchmarks"
            git clone --branch v0.5.0 --depth 1 \
                "$pg_ocpm_repository" "$checkout"
        fi
        pg_ocpm_source="$checkout"
    fi
fi

export PG_OCPM_SOURCE="$pg_ocpm_source"
mkdir -p "$root/.benchmarks/public-data"

cleanup() {
    docker compose -f "$compose" down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker compose -f "$compose" down --volumes --remove-orphans
docker compose -f "$compose" build
docker compose -f "$compose" up -d --wait
docker compose -f "$compose" exec -T benchmark \
    python benchmarks/public_fixture.py \
    --baseline-host postgres_vanilla \
    --extension-host postgres_ocpm \
    --baseline-db ocel_benchmark \
    --extension-db ocel_benchmark \
    --data-dir /data \
    --output /results/public-prepare.json
docker compose -f "$compose" exec -T benchmark \
    python benchmarks/public_common_pm.py \
    --baseline-host postgres_vanilla \
    --extension-host postgres_ocpm \
    --baseline-db ocel_benchmark \
    --extension-db ocel_benchmark \
    --output /results/public-common-pm-0.4.0.json \
    "$@"

docker compose -f "$compose" exec -T postgres_vanilla \
    psql -U postgres -d ocel_benchmark \
    < "$root/benchmarks/public/prepare_pm4py_baseline.sql"
docker compose -f "$compose" exec -T benchmark \
    python benchmarks/sap_pm4py_three_way.py \
    --baseline-host postgres_vanilla \
    --extension-host postgres_ocpm \
    --database ocel_benchmark \
    --output /results/sap-pm4py-three-way.json \
    --report /results/sap-pm4py-three-way.md

echo "result: $root/.benchmarks/public-common-pm-0.4.0.json"
echo "result: $root/.benchmarks/sap-pm4py-three-way.json"
