#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
compose="$root/benchmarks/bridge/docker-compose.yml"
python="${PYTHON:-python3}"
pg_ocpm_repository="${PG_OCPM_SOURCE:-$(cd "$root/.." && pwd)/pg_ocpm}"
sources="$root/.benchmarks/sap-release-bridge-sources"
result="$root/.benchmarks/sap-release-bridge-0.4.0-to-0.6.0.json"
postgres_base_image="postgres:16@sha256:33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"

prior_engine_revision="8427c36aa16da11b04ba642672df096d6f21e156"
current_engine_revision="c44e9341ced643e0b777a18d7b0d26a43127caa0"
prior_pg_ocpm_revision="e72c5ffc281a1f1019d07aef8ad479217823e4f2"
current_pg_ocpm_revision="279d81b3db0a0ae7470bf90824f1fbba9d188e70"

if [[ "${1:-}" != "--preview" ]] || (( $# != 1 )); then
    echo "usage: $0 --preview" >&2
    echo "The release bridge always stages ignored evidence for review." >&2
    exit 2
fi

if [[ ! -d "$pg_ocpm_repository" ]]; then
    echo "pg_ocpm checkout not found: $pg_ocpm_repository" >&2
    echo "Set PG_OCPM_SOURCE to a checkout containing both locked revisions." >&2
    exit 2
fi
pg_ocpm_repository="$(cd "$pg_ocpm_repository" && pwd)"

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
        if ! git -C "$target" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
            echo "$label source path exists but is not a Git worktree: $target" >&2
            exit 2
        fi
        local actual
        actual="$(git -C "$target" rev-parse --verify HEAD)"
        if [[ "$actual" != "$revision" ]]; then
            echo "$label worktree has $actual; expected $revision: $target" >&2
            exit 2
        fi
    else
        mkdir -p "$(dirname "$target")"
        git -C "$repository" worktree add --detach "$target" "$revision"
    fi
    if [[ -n "$(git -C "$target" status --porcelain --untracked-files=all)" ]]; then
        echo "$label worktree must be clean: $target" >&2
        exit 2
    fi
}

prior_engine_source="$sources/engine-0.4.0"
current_engine_source="$sources/engine-0.6.0"
prior_pg_ocpm_source="$sources/pg_ocpm-0.5.0"
current_pg_ocpm_source="$sources/pg_ocpm-0.7.0"

ensure_worktree \
    "$root" "$prior_engine_revision" "$prior_engine_source" "prior ocpm-engine"
ensure_worktree \
    "$root" "$current_engine_revision" "$current_engine_source" \
    "current ocpm-engine"
ensure_worktree \
    "$pg_ocpm_repository" "$prior_pg_ocpm_revision" "$prior_pg_ocpm_source" \
    "prior pg_ocpm"
ensure_worktree \
    "$pg_ocpm_repository" "$current_pg_ocpm_revision" "$current_pg_ocpm_source" \
    "current pg_ocpm"

export PRIOR_ENGINE_SOURCE="$prior_engine_source"
export CURRENT_ENGINE_SOURCE="$current_engine_source"
export PRIOR_PG_OCPM_SOURCE="$prior_pg_ocpm_source"
export CURRENT_PG_OCPM_SOURCE="$current_pg_ocpm_source"
export PRIOR_ENGINE_REVISION="$prior_engine_revision"
export CURRENT_ENGINE_REVISION="$current_engine_revision"
export PRIOR_PG_OCPM_REVISION="$prior_pg_ocpm_revision"
export CURRENT_PG_OCPM_REVISION="$current_pg_ocpm_revision"

controller_revision="$(git -C "$root" rev-parse --verify HEAD)"
export CONTROLLER_REVISION="$controller_revision"
if [[ -z "$(git -C "$root" status --porcelain --untracked-files=all)" ]]; then
    controller_tree_clean=true
else
    controller_tree_clean=false
fi

file_sha256() {
    "$python" -c \
        'import hashlib,pathlib,sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' \
        "$1"
}

prior_loader_sha256="$(
    file_sha256 "$prior_engine_source/benchmarks/public_fixture.py"
)"
current_loader_sha256="$(
    file_sha256 "$current_engine_source/benchmarks/public_fixture.py"
)"

docker_daemon_id="$(docker info --format '{{.ID}}')"
benchmark_host_id="sha256:$(
    printf '%s' "$docker_daemon_id" \
        | "$python" -c \
            'import hashlib,sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())'
)"

cleanup() {
    docker compose -f "$compose" down --volumes --remove-orphans \
        >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

mkdir -p "$root/.benchmarks/public-data"
docker compose -f "$compose" down --volumes --remove-orphans
docker compose -f "$compose" build
docker compose -f "$compose" up -d --wait

client_container="$(docker compose -f "$compose" ps -q bridge)"
vanilla_container="$(docker compose -f "$compose" ps -q postgres_vanilla)"
prior_container="$(docker compose -f "$compose" ps -q postgres_prior)"
current_container="$(docker compose -f "$compose" ps -q postgres_current)"

client_image_id="$(docker inspect --format '{{.Image}}' "$client_container")"
vanilla_image_id="$(docker inspect --format '{{.Image}}' "$vanilla_container")"
prior_image_id="$(docker inspect --format '{{.Image}}' "$prior_container")"
current_image_id="$(docker inspect --format '{{.Image}}' "$current_container")"

image_label() {
    docker image inspect --format "{{index .Config.Labels \"$2\"}}" "$1"
}

if [[ "$(image_label "$client_image_id" org.opencontainers.image.revision)" \
        != "$controller_revision" ]] \
    || [[ "$(image_label "$client_image_id" \
        org.opencontainers.image.bridge.prior-revision)" \
        != "$prior_engine_revision" ]] \
    || [[ "$(image_label "$client_image_id" \
        org.opencontainers.image.bridge.current-revision)" \
        != "$current_engine_revision" ]] \
    || [[ "$(image_label "$prior_image_id" org.opencontainers.image.revision)" \
        != "$prior_pg_ocpm_revision" ]] \
    || [[ "$(image_label "$current_image_id" org.opencontainers.image.revision)" \
        != "$current_pg_ocpm_revision" ]] \
    || [[ "$(image_label "$prior_image_id" org.opencontainers.image.base.name)" \
        != "$postgres_base_image" ]] \
    || [[ "$(image_label "$current_image_id" org.opencontainers.image.base.name)" \
        != "$postgres_base_image" ]]; then
    echo "built bridge image labels do not match the locked source revisions" >&2
    exit 2
fi

bridge_exec() {
    docker compose -f "$compose" exec -T \
        -e OCPM_BENCHMARK_HOST_ID="$benchmark_host_id" \
        -e OCPM_CLIENT_IMAGE_ID="$client_image_id" \
        -e OCPM_VANILLA_DATABASE_IMAGE_ID="$vanilla_image_id" \
        -e OCPM_PRIOR_DATABASE_IMAGE_ID="$prior_image_id" \
        -e OCPM_CURRENT_DATABASE_IMAGE_ID="$current_image_id" \
        -e OCPM_CONTROLLER_SOURCE_REVISION="$controller_revision" \
        -e OCPM_CONTROLLER_SOURCE_TREE_CLEAN="$controller_tree_clean" \
        -e OCPM_PRIOR_ENGINE_SOURCE_REVISION="$prior_engine_revision" \
        -e OCPM_PRIOR_ENGINE_SOURCE_TREE_CLEAN=true \
        -e OCPM_PRIOR_PG_OCPM_SOURCE_REVISION="$prior_pg_ocpm_revision" \
        -e OCPM_PRIOR_PG_OCPM_SOURCE_TREE_CLEAN=true \
        -e OCPM_CURRENT_ENGINE_SOURCE_REVISION="$current_engine_revision" \
        -e OCPM_CURRENT_ENGINE_SOURCE_TREE_CLEAN=true \
        -e OCPM_CURRENT_PG_OCPM_SOURCE_REVISION="$current_pg_ocpm_revision" \
        -e OCPM_CURRENT_PG_OCPM_SOURCE_TREE_CLEAN=true \
        -e OCPM_PRIOR_LOADER_SHA256="$prior_loader_sha256" \
        -e OCPM_CURRENT_LOADER_SHA256="$current_loader_sha256" \
        -e OCPM_POSTGRES_BASE_IMAGE="$postgres_base_image" \
        bridge "$@"
}

# Load current first, then prior. Each historical loader rebuilds the vanilla
# database from the same checksum-pinned SAP source; the second pass therefore
# leaves the oracle in the exact state paired with the prior arm.
bridge_exec \
    python /sources/engine-current/benchmarks/public_fixture.py \
    --baseline-host postgres_vanilla \
    --extension-host postgres_current \
    --baseline-db ocel_benchmark \
    --extension-db ocel_benchmark \
    --data-dir /data \
    --output /results/sap-release-bridge-current-prepare.json

bridge_exec \
    python /sources/engine-prior/benchmarks/public_fixture.py \
    --baseline-host postgres_vanilla \
    --extension-host postgres_prior \
    --baseline-db ocel_benchmark \
    --extension-db ocel_benchmark \
    --data-dir /data \
    --output /results/sap-release-bridge-prior-prepare.json

bridge_exec \
    python benchmarks/sap_release_regression.py \
    --oracle-host postgres_vanilla \
    --prior-host postgres_prior \
    --current-host postgres_current \
    --database ocel_benchmark \
    --output /results/sap-release-bridge-0.4.0-to-0.6.0.json

"$python" "$root/benchmarks/check_sap_release_regression.py" \
    "$result" --preview

echo "Validated staged SAP release bridge preview; published docs/results unchanged."
echo "staged result: $result"
