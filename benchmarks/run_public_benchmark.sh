#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
compose="$root/benchmarks/public/docker-compose.yml"
python="${PYTHON:-python3}"
pg_ocpm_repository="${PG_OCPM_SOURCE:-}"
pg_ocpm_remote="${PG_OCPM_REPOSITORY:-}"
concurrency_only=false
sap_only=false
engine_release="${OCPM_ENGINE_RELEASE:-0.8.0}"
pg_ocpm_release="${OCPM_PG_OCPM_RELEASE:-0.8.0}"
engine_revision="${OCPM_ENGINE_PRODUCT_REVISION:-f5a95ecd6b8a1f184f8ffed2371980ef419beaab}"
pg_ocpm_revision="${OCPM_PG_OCPM_PRODUCT_REVISION:-0e15ab10f8ec87518b9e822072028fb3eda3879c}"
engine_read_path="${OCPM_ENGINE_READ_PATH:-aggregate}"
public_result_name="public-common-pm-${engine_release}.json"
sap_result_name="${OCPM_SAP_RESULT_NAME:-sap-pm4py-three-way-${engine_release}.json}"
sap_report_name="${OCPM_SAP_REPORT_NAME:-sap-pm4py-three-way-${engine_release}.md}"
release_bridge="$root/.benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json"
sources="$root/.benchmarks/public-sources"
engine_source="$sources/ocpm-engine-${engine_release}-${engine_revision:0:12}"
pg_ocpm_source="$sources/pg_ocpm-${pg_ocpm_release}-${pg_ocpm_revision:0:12}"

while (( $# > 0 )); do
    case "$1" in
        --concurrency-only) concurrency_only=true ;;
        --sap-only) sap_only=true ;;
        *)
            echo "Public benchmark runs do not accept additional arguments: $1" >&2
            exit 2
            ;;
    esac
    shift
done

if [[ "$concurrency_only" == true ]]; then
    artifacts=("$root/.benchmarks/$sap_result_name")
    if [[ "$sap_only" == false ]]; then
        artifacts+=("$root/.benchmarks/$public_result_name")
    fi
    for artifact in "${artifacts[@]}"; do
        if [[ ! -f "$artifact" ]]; then
            echo "Concurrency-only mode requires existing artifact: $artifact" >&2
            exit 2
        fi
    done
fi

if [[ "$sap_only" == false && ! -f "$release_bridge" ]]; then
    echo "public preview validation requires the staged matched-release bridge:" >&2
    echo "  $release_bridge" >&2
    echo "run: make perf-sap-release-bridge-preview" >&2
    exit 2
fi

controller_revision="$(git -C "$root" rev-parse --verify HEAD)"

canonical_dir() {
    (cd "$1" && pwd -P)
}

git_common_dir() {
    local checkout="$1"
    local common
    common="$(git -C "$checkout" rev-parse --git-common-dir)"
    if [[ "$common" = /* ]]; then
        canonical_dir "$common"
    else
        canonical_dir "$checkout/$common"
    fi
}

verify_controller_checkout() {
    local expected_revision="$1"
    local expected_root actual_root actual_revision
    expected_root="$(canonical_dir "$root")"
    actual_root="$(canonical_dir "$(git -C "$root" rev-parse --show-toplevel)")"
    actual_revision="$(git -C "$root" rev-parse --verify HEAD)"
    if [[ "$actual_root" != "$expected_root" ]]; then
        echo "controller path is not its Git worktree root: $root" >&2
        exit 2
    fi
    if [[ "$actual_revision" != "$expected_revision" ]]; then
        echo "controller revision changed: $actual_revision; expected $expected_revision" >&2
        exit 2
    fi
    if [[ -n "$(git -C "$root" status --porcelain --untracked-files=all)" ]]; then
        echo "public benchmarks require a clean current controller checkout: $root" >&2
        exit 2
    fi
}

verify_controller_checkout "$controller_revision"

require_commit() {
    local repository="$1"
    local revision="$2"
    local label="$3"
    if ! git -C "$repository" cat-file -e "${revision}^{commit}" 2>/dev/null; then
        echo "$label revision is absent from $repository: $revision" >&2
        exit 2
    fi
}

ensure_worktree() {
    local repository="$1"
    local revision="$2"
    local target="$3"
    local label="$4"
    require_commit "$repository" "$revision" "$label"
    if [[ -e "$target" ]]; then
        if [[ ! -d "$target" ]]; then
            echo "$label source path exists but is not a directory: $target" >&2
            exit 2
        fi
        if ! git -C "$target" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
            echo "$label source path exists but is not a Git worktree: $target" >&2
            exit 2
        fi
    else
        mkdir -p "$(dirname "$target")"
        git -C "$repository" worktree add --detach "$target" "$revision"
    fi

    local repository_common target_common target_path target_top actual
    repository_common="$(git_common_dir "$repository")"
    target_common="$(git_common_dir "$target")"
    target_path="$(canonical_dir "$target")"
    target_top="$(canonical_dir "$(git -C "$target" rev-parse --show-toplevel)")"
    if [[ "$target_top" != "$target_path" ]]; then
        echo "$label source path is inside another worktree: $target" >&2
        exit 2
    fi
    if [[ "$target_common" != "$repository_common" ]]; then
        echo "$label worktree is not associated with the requested repository: $target" >&2
        exit 2
    fi
    if git -C "$target" symbolic-ref -q HEAD >/dev/null; then
        echo "$label source must be a detached worktree: $target" >&2
        exit 2
    fi
    actual="$(git -C "$target" rev-parse --verify HEAD)"
    if [[ "$actual" != "$revision" ]]; then
        echo "$label worktree has $actual; expected $revision: $target" >&2
        exit 2
    fi
    if [[ -n "$(git -C "$target" status --porcelain --untracked-files=all)" ]]; then
        echo "$label worktree must be clean: $target" >&2
        exit 2
    fi
}

if [[ -z "$pg_ocpm_repository" ]]; then
    sibling="$(cd "$root/.." && pwd)/pg_ocpm"
    checkout="$root/.benchmarks/pg_ocpm-repository-${pg_ocpm_release}"
    if [[ -f "$sibling/pg_ocpm.control" ]]; then
        pg_ocpm_repository="$sibling"
    else
        if [[ -z "$pg_ocpm_remote" ]]; then
            echo "Set PG_OCPM_SOURCE to a local pg_ocpm checkout or" >&2
            echo "set PG_OCPM_REPOSITORY to its public Git repository URL." >&2
            exit 2
        fi
        if [[ ! -d "$checkout/.git" ]]; then
            mkdir -p "$root/.benchmarks"
            git clone --branch "v${pg_ocpm_release}" --depth 1 \
                "$pg_ocpm_remote" "$checkout"
        fi
        pg_ocpm_repository="$checkout"
    fi
fi

pg_ocpm_repository="$(cd "$pg_ocpm_repository" && pwd)"
ensure_worktree \
    "$root" "$engine_revision" "$engine_source" "ocpm-engine ${engine_release}"
ensure_worktree \
    "$pg_ocpm_repository" "$pg_ocpm_revision" "$pg_ocpm_source" \
    "pg_ocpm ${pg_ocpm_release}"

export PG_OCPM_SOURCE="$pg_ocpm_source"
export OCPM_ENGINE_PRODUCT_SOURCE="$engine_source"
mkdir -p "$root/.benchmarks/public-data"

export OCPM_CONTROLLER_SOURCE_REVISION="$controller_revision"
export OCPM_ENGINE_SOURCE_REVISION="$engine_revision"
export OCPM_PG_OCPM_SOURCE_REVISION="$pg_ocpm_revision"
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
verify_controller_checkout "$controller_revision"
ensure_worktree \
    "$root" "$engine_revision" "$engine_source" "ocpm-engine ${engine_release}"
ensure_worktree \
    "$pg_ocpm_repository" "$pg_ocpm_revision" "$pg_ocpm_source" \
    "pg_ocpm ${pg_ocpm_release}"
controller_tree_clean=true
engine_tree_clean=true
pg_ocpm_tree_clean=true
docker compose -f "$compose" up -d --wait
client_container="$(docker compose -f "$compose" ps -q benchmark)"
vanilla_container="$(docker compose -f "$compose" ps -q postgres_vanilla)"
pg_ocpm_container="$(docker compose -f "$compose" ps -q postgres_ocpm)"
client_image_id="$(docker inspect --format '{{.Image}}' "$client_container")"
vanilla_image_id="$(docker inspect --format '{{.Image}}' "$vanilla_container")"
pg_ocpm_image_id="$(docker inspect --format '{{.Image}}' "$pg_ocpm_container")"
client_controller_revision="$(
    docker image inspect \
        --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
        "$client_image_id"
)"
client_engine_revision="$(
    docker image inspect \
        --format '{{index .Config.Labels "org.opencontainers.image.ocpm-engine.revision"}}' \
        "$client_image_id"
)"
pg_ocpm_image_revision="$(
    docker image inspect \
        --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
        "$pg_ocpm_image_id"
)"
if [[ "$client_controller_revision" != "$controller_revision" ]] \
    || [[ "$client_engine_revision" != "$engine_revision" ]] \
    || [[ "$pg_ocpm_image_revision" != "$pg_ocpm_revision" ]]; then
    echo "built Docker image labels do not match locked product and controller revisions" >&2
    exit 2
fi

benchmark_exec() {
    docker compose -f "$compose" exec -T \
        -e OCPM_BENCHMARK_HOST_ID="$benchmark_host_id" \
        -e OCPM_CONTROLLER_SOURCE_REVISION="$controller_revision" \
        -e OCPM_CONTROLLER_SOURCE_TREE_CLEAN="$controller_tree_clean" \
        -e OCPM_ENGINE_SOURCE_REVISION="$engine_revision" \
        -e OCPM_ENGINE_SOURCE_TREE_CLEAN="$engine_tree_clean" \
        -e OCPM_PG_OCPM_SOURCE_REVISION="$pg_ocpm_revision" \
        -e OCPM_PG_OCPM_SOURCE_TREE_CLEAN="$pg_ocpm_tree_clean" \
        -e OCPM_ENGINE_READ_PATH="$engine_read_path" \
        -e OCPM_CLIENT_IMAGE_ID="$client_image_id" \
        -e OCPM_VANILLA_DATABASE_IMAGE_ID="$vanilla_image_id" \
        -e OCPM_PG_OCPM_DATABASE_IMAGE_ID="$pg_ocpm_image_id" \
        benchmark "$@"
}

benchmark_exec env OCPM_EXPECTED_ENGINE_RELEASE="$engine_release" python -c '
from importlib import metadata
import os
from pathlib import Path
import sys

sys.path.insert(0, "/workspace/benchmarks")
import ocpm_engine

path = Path(ocpm_engine.__file__).resolve()
version = metadata.version("ocpm-engine")
if "site-packages" not in path.parts or Path("/workspace") in path.parents:
    raise SystemExit(f"ocpm_engine resolved outside site-packages: {path}")
if version != os.environ["OCPM_EXPECTED_ENGINE_RELEASE"]:
    raise SystemExit(f"unexpected ocpm-engine version: {version}")
'

benchmark_exec \
    python benchmarks/public_fixture.py \
    --baseline-host postgres_vanilla \
    --extension-host postgres_ocpm \
    --baseline-db ocel_benchmark \
    --extension-db ocel_benchmark \
    --data-dir /data \
    --output /results/public-prepare.json
if [[ "$sap_only" == true ]]; then
    :
elif [[ "$concurrency_only" == true ]]; then
    benchmark_exec \
        python benchmarks/public_common_pm.py \
        --baseline-host postgres_vanilla \
        --extension-host postgres_ocpm \
        --baseline-db ocel_benchmark \
        --extension-db ocel_benchmark \
        --output "/results/$public_result_name" \
        --concurrency-only
else
    benchmark_exec \
        python benchmarks/public_common_pm.py \
        --baseline-host postgres_vanilla \
        --extension-host postgres_ocpm \
        --baseline-db ocel_benchmark \
        --extension-db ocel_benchmark \
        --output "/results/$public_result_name" \
        --warmups 10 \
        --runs 30 \
        --latency-epochs 3
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
        --output "/results/$sap_result_name" \
        --report "/results/$sap_report_name" \
        --concurrency-only
else
    benchmark_exec \
        python benchmarks/sap_pm4py_three_way.py \
        --baseline-host postgres_vanilla \
        --extension-host postgres_ocpm \
        --database ocel_benchmark \
        --output "/results/$sap_result_name" \
        --report "/results/$sap_report_name" \
        --warmups 10 \
        --runs 30 \
        --latency-epochs 3
fi

sap_result="$root/.benchmarks/$sap_result_name"

sap_check=(
    "$python" "$root/benchmarks/check_sap_pm4py_result.py"
    "$sap_result" --preview
    --expected-ocpm-engine-version "$engine_release"
    --expected-pg-ocpm-version "$pg_ocpm_release"
)
if [[ "$sap_only" == false ]]; then
    public_result="$root/.benchmarks/$public_result_name"
    "$python" "$root/benchmarks/check_public_result.py" \
        "$public_result" --release-bridge "$release_bridge" --preview
    "${sap_check[@]}" --release-bridge "$release_bridge"
    "$python" "$root/benchmarks/check_public_provenance_pair.py" \
        --common "$public_result" --sap "$sap_result" --preview
else
    "${sap_check[@]}"
fi
echo "Validated staged schema-5 preview; published docs/results unchanged."

if [[ "$sap_only" == false ]]; then
    echo "staged result: $public_result"
fi
echo "staged result: $sap_result"
