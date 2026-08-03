#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
compose="$root/benchmarks/ecosystem/docker-compose.yml"
pg_ocpm_root="${PG_OCPM_SOURCE:-$(cd "$root/../pg_ocpm" && pwd)}"
pair="all"
smoke=false
keep=false

while (( $# > 0 )); do
    case "$1" in
        --pair)
            shift
            pair="${1:-}"
            ;;
        --smoke) smoke=true ;;
        --keep) keep=true ;;
        *)
            echo "unknown argument: $1" >&2
            exit 2
            ;;
    esac
    shift
done

if [[ "$pair" != "all" && "$pair" != "rust4pm" && "$pair" != "ocpa" ]]; then
    echo "--pair must be all, rust4pm, or ocpa" >&2
    exit 2
fi
if [[ ! -f "$pg_ocpm_root/pg_ocpm.control" ]]; then
    echo "pg_ocpm source not found: $pg_ocpm_root" >&2
    exit 2
fi

engine_revision="$(git -C "$root" rev-parse HEAD)"
pg_ocpm_revision="$(git -C "$pg_ocpm_root" rev-parse HEAD)"
engine_clean=false
pg_ocpm_clean=false
if [[ -z "$(git -C "$root" status --porcelain --untracked-files=all)" ]]; then
    engine_clean=true
fi
if [[ -z "$(git -C "$pg_ocpm_root" status --porcelain --untracked-files=all)" ]]; then
    pg_ocpm_clean=true
fi

export PG_OCPM_SOURCE="$pg_ocpm_root"
export OCPM_ENGINE_SOURCE_REVISION="$engine_revision"
export OCPM_PG_OCPM_SOURCE_REVISION="$pg_ocpm_revision"
mkdir -p "$root/.benchmarks/ecosystem-data"

cleanup() {
    if [[ "$keep" == false ]]; then
        docker compose -f "$compose" down --remove-orphans >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

services=(postgres_ocpm engine)
if [[ "$pair" == "all" || "$pair" == "ocpa" ]]; then
    services+=(ocpa)
fi
if [[ "$pair" == "all" || "$pair" == "rust4pm" ]]; then
    services+=(rust4pm)
fi

docker compose -f "$compose" down --volumes --remove-orphans
docker compose -f "$compose" build "${services[@]}"
docker compose -f "$compose" up -d --wait postgres_vanilla "${services[@]}"

engine_container="$(docker compose -f "$compose" ps -q engine)"
engine_image="$(docker inspect --format '{{.Image}}' "$engine_container")"
ocpa_image=""
rust4pm_image=""
if [[ "$pair" == "all" || "$pair" == "ocpa" ]]; then
    ocpa_image="$(docker inspect --format '{{.Image}}' "$(docker compose -f "$compose" ps -q ocpa)")"
fi
if [[ "$pair" == "all" || "$pair" == "rust4pm" ]]; then
    rust4pm_image="$(docker inspect --format '{{.Image}}' "$(docker compose -f "$compose" ps -q rust4pm)")"
fi

engine_exec() {
    docker compose -f "$compose" exec -T \
        -e OCPM_ENGINE_IMAGE_ID="$engine_image" \
        -e OCPM_ENGINE_SOURCE_TREE_CLEAN="$engine_clean" \
        -e OCPM_PG_OCPM_SOURCE_TREE_CLEAN="$pg_ocpm_clean" \
        engine "$@"
}

ocpa_exec() {
    docker compose -f "$compose" exec -T \
        -e OCPM_OCPA_IMAGE_ID="$ocpa_image" \
        -e OCPM_ENGINE_SOURCE_TREE_CLEAN="$engine_clean" \
        ocpa "$@"
}

rust4pm_exec() {
    docker compose -f "$compose" exec -T \
        -e OCPM_RUST4PM_IMAGE_ID="$rust4pm_image" \
        -e OCPM_ENGINE_SOURCE_TREE_CLEAN="$engine_clean" \
        rust4pm "$@"
}

if [[ "$smoke" == true ]]; then
    benchmark_args=(
        --warmups 0
        --runs 1
        --latency-epochs 1
        --concurrency 1
        --concurrency-epochs 1
        --concurrency-min-seconds 0.01
        --concurrency-requests 1
    )
else
    benchmark_args=(
        --warmups 10
        --runs 30
        --latency-epochs 3
        --concurrency 1,2,4,8
        --concurrency-epochs 3
        --concurrency-min-seconds 5
        --concurrency-requests 32
    )
fi

suffix="1.0.0"
if [[ "$smoke" == true ]]; then
    suffix="smoke"
fi

run_pair() {
    local competitor="$1"
    local dataset="$2"
    local prepare_result="/results/ecosystem-prepare-${dataset}.json"
    local manifest="/results/ecosystem-manifest-${dataset}.json"
    local engine_output="/results/ecosystem-engine-${dataset}.json"
    local competitor_output="/results/ecosystem-${competitor}-${dataset}.json"

    engine_exec python -m benchmarks.ecosystem.fixture \
        --dataset "$dataset" \
        --baseline-host postgres_vanilla \
        --extension-host postgres_ocpm \
        --data-dir /data \
        --output "$prepare_result"
    engine_exec python -m benchmarks.ecosystem.engine_arm \
        --make-manifest \
        --prepare-result "$prepare_result" \
        --manifest "$manifest"

    engine_exec python -m benchmarks.ecosystem.engine_arm \
        --manifest "$manifest" \
        --datasets "$dataset" \
        --output "$engine_output" \
        "${benchmark_args[@]}"

    if [[ "$competitor" == "ocpa" ]]; then
        ocpa_exec python -m benchmarks.ecosystem.ocpa_arm \
            --manifest "$manifest" \
            --data-dir /data \
            --datasets "$dataset" \
            --output "$competitor_output" \
            "${benchmark_args[@]}"
    else
        rust4pm_exec /usr/local/bin/rust4pm-ecosystem-benchmark \
            --manifest "$manifest" \
            --data-dir /data \
            --datasets "$dataset" \
            --output "$competitor_output" \
            "${benchmark_args[@]}"
    fi

    engine_exec python -m benchmarks.ecosystem.merge_report \
        --manifest "$manifest" \
        --engine "$engine_output" \
        --competitor "$competitor_output" \
        --competitor-name "$competitor" \
        --output "/results/ecosystem-${competitor}-vs-pg-ocpm-engine-${suffix}.json" \
        --report "/results/ecosystem-${competitor}-vs-pg-ocpm-engine-${suffix}.md"
}

if [[ "$pair" == "all" || "$pair" == "rust4pm" ]]; then
    run_pair rust4pm rust4pm_p2p
fi
if [[ "$pair" == "all" || "$pair" == "ocpa" ]]; then
    run_pair ocpa ocpa_running_example
fi

echo "ecosystem benchmark completed; artifacts are under $root/.benchmarks"
