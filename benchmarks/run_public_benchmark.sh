#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
compose="$root/benchmarks/public/docker-compose.yml"
python="${PYTHON:-python3}"
pg_ocpm_source="${PG_OCPM_SOURCE:-}"
pg_ocpm_repository="${PG_OCPM_REPOSITORY:-}"
concurrency_only=false

if [[ "${1:-}" == "--concurrency-only" ]]; then
    concurrency_only=true
    shift
fi

if [[ "$concurrency_only" == true ]]; then
    if (( $# > 0 )); then
        echo "Concurrency-only mode does not accept additional arguments." >&2
        exit 2
    fi
    for artifact in \
        "$root/.benchmarks/public-common-pm-0.5.0.json" \
        "$root/.benchmarks/sap-pm4py-three-way-0.5.0.json"; do
        if [[ ! -f "$artifact" ]]; then
            echo "Concurrency-only mode requires existing artifact: $artifact" >&2
            exit 2
        fi
    done
fi

if [[ -z "$pg_ocpm_source" ]]; then
    sibling="$(cd "$root/.." && pwd)/pg_ocpm"
    checkout="$root/.benchmarks/pg_ocpm-0.6.0"
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
            git clone --branch v0.6.0 --depth 1 \
                "$pg_ocpm_repository" "$checkout"
        fi
        pg_ocpm_source="$checkout"
    fi
fi

pg_ocpm_source="$(cd "$pg_ocpm_source" && pwd)"
export PG_OCPM_SOURCE="$pg_ocpm_source"
mkdir -p "$root/.benchmarks/public-data"

engine_revision="$(git -C "$root" rev-parse --verify HEAD)"
pg_ocpm_revision="$(git -C "$pg_ocpm_source" rev-parse --verify HEAD)"
export OCPM_ENGINE_SOURCE_REVISION="$engine_revision"
export OCPM_PG_OCPM_SOURCE_REVISION="$pg_ocpm_revision"
if [[ -z "$(git -C "$root" status --porcelain --untracked-files=all)" ]]; then
    engine_tree_clean=true
else
    engine_tree_clean=false
fi
if [[ -z "$(git -C "$pg_ocpm_source" status --porcelain --untracked-files=all)" ]]; then
    pg_ocpm_tree_clean=true
else
    pg_ocpm_tree_clean=false
fi
if [[ "${OCPM_ALLOW_DIRTY_BENCHMARK:-false}" != true ]] \
    && { [[ "$engine_tree_clean" != true ]] || [[ "$pg_ocpm_tree_clean" != true ]]; }; then
    echo "public release benchmarks require clean ocpm-engine and pg_ocpm sources" >&2
    echo "set OCPM_ALLOW_DIRTY_BENCHMARK=true only for an unpublished preview" >&2
    exit 2
fi
docker_daemon_id="$(docker info --format '{{.ID}}')"
benchmark_host_id="sha256:$(
    printf '%s' "$docker_daemon_id" \
        | "$python" -c 'import hashlib,sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())'
)"

cleanup() {
    docker compose -f "$compose" down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker compose -f "$compose" down --volumes --remove-orphans
docker compose -f "$compose" build
docker compose -f "$compose" up -d --wait
client_container="$(docker compose -f "$compose" ps -q benchmark)"
vanilla_container="$(docker compose -f "$compose" ps -q postgres_vanilla)"
pg_ocpm_container="$(docker compose -f "$compose" ps -q postgres_ocpm)"
client_image_id="$(docker inspect --format '{{.Image}}' "$client_container")"
vanilla_image_id="$(docker inspect --format '{{.Image}}' "$vanilla_container")"
pg_ocpm_image_id="$(docker inspect --format '{{.Image}}' "$pg_ocpm_container")"
client_image_revision="$(
    docker image inspect \
        --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
        "$client_image_id"
)"
pg_ocpm_image_revision="$(
    docker image inspect \
        --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
        "$pg_ocpm_image_id"
)"
if [[ "$client_image_revision" != "$engine_revision" ]] \
    || [[ "$pg_ocpm_image_revision" != "$pg_ocpm_revision" ]]; then
    echo "built Docker image source labels do not match checked-out revisions" >&2
    exit 2
fi

benchmark_exec() {
    docker compose -f "$compose" exec -T \
        -e OCPM_BENCHMARK_HOST_ID="$benchmark_host_id" \
        -e OCPM_ENGINE_SOURCE_REVISION="$engine_revision" \
        -e OCPM_ENGINE_SOURCE_TREE_CLEAN="$engine_tree_clean" \
        -e OCPM_PG_OCPM_SOURCE_REVISION="$pg_ocpm_revision" \
        -e OCPM_PG_OCPM_SOURCE_TREE_CLEAN="$pg_ocpm_tree_clean" \
        -e OCPM_CLIENT_IMAGE_ID="$client_image_id" \
        -e OCPM_VANILLA_DATABASE_IMAGE_ID="$vanilla_image_id" \
        -e OCPM_PG_OCPM_DATABASE_IMAGE_ID="$pg_ocpm_image_id" \
        benchmark "$@"
}

benchmark_exec \
    python benchmarks/public_fixture.py \
    --baseline-host postgres_vanilla \
    --extension-host postgres_ocpm \
    --baseline-db ocel_benchmark \
    --extension-db ocel_benchmark \
    --data-dir /data \
    --output /results/public-prepare.json
if [[ "$concurrency_only" == true ]]; then
    benchmark_exec \
        python benchmarks/public_common_pm.py \
        --baseline-host postgres_vanilla \
        --extension-host postgres_ocpm \
        --baseline-db ocel_benchmark \
        --extension-db ocel_benchmark \
        --output /results/public-common-pm-0.5.0.json \
        --concurrency-only
else
    benchmark_exec \
        python benchmarks/public_common_pm.py \
        --baseline-host postgres_vanilla \
        --extension-host postgres_ocpm \
        --baseline-db ocel_benchmark \
        --extension-db ocel_benchmark \
        --output /results/public-common-pm-0.5.0.json \
        --warmups 10 \
        --runs 30 \
        "$@"
fi

docker compose -f "$compose" exec -T postgres_vanilla \
    psql -U postgres -d ocel_benchmark \
    < "$root/benchmarks/public/prepare_pm4py_baseline.sql"
if [[ "$concurrency_only" == true ]]; then
    benchmark_exec \
        python benchmarks/sap_pm4py_three_way.py \
        --baseline-host postgres_vanilla \
        --extension-host postgres_ocpm \
        --database ocel_benchmark \
        --output /results/sap-pm4py-three-way-0.5.0.json \
        --report /results/sap-pm4py-three-way-0.5.0.md \
        --concurrency-only
else
    benchmark_exec \
        python benchmarks/sap_pm4py_three_way.py \
        --baseline-host postgres_vanilla \
        --extension-host postgres_ocpm \
        --database ocel_benchmark \
        --output /results/sap-pm4py-three-way-0.5.0.json \
        --report /results/sap-pm4py-three-way-0.5.0.md \
        --warmups 10 \
        --runs 30
fi

public_result="$root/.benchmarks/public-common-pm-0.5.0.json"
sap_result="$root/.benchmarks/sap-pm4py-three-way-0.5.0.json"

if [[ "$concurrency_only" == true ]]; then
    "$python" "$root/benchmarks/check_public_result.py" \
        "$public_result" --preview
    "$python" "$root/benchmarks/check_sap_pm4py_result.py" \
        "$sap_result" --preview
    "$python" "$root/benchmarks/check_public_provenance_pair.py" \
        --common "$public_result" --sap "$sap_result" --preview
    echo "Validated schema-3 concurrency preview; published docs/results unchanged."
fi

echo "staged result: $public_result"
echo "staged result: $sap_result"
