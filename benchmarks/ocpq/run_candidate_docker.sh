#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: OCPM_DATABASE_URL=... $0 OCPQ_REFERENCE_JSON OUTPUT_JSON" >&2
  exit 2
fi
: "${OCPM_DATABASE_URL:?OCPM_DATABASE_URL is required}"

reference="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
output_dir="$(cd "$(dirname "$2")" && pwd)"
output_name="$(basename "$2")"
image="${OCPM_CANDIDATE_IMAGE:-ocpm-engine:ocpq-candidate}"
network="${OCPM_DOCKER_NETWORK:-bridge}"
database_image="${OCPM_DATABASE_IMAGE:-unspecified}"

docker_daemon_id="$(docker info --format '{{.ID}}')"
benchmark_host_id="sha256:$(
  printf '%s' "$docker_daemon_id" \
    | python3 -c 'import hashlib,sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())'
)"
reference_host_id="$(jq --exit-status --raw-output '.environment.benchmark_host_id' "$reference")"
if [[ "$benchmark_host_id" != "$reference_host_id" ]]; then
  echo "candidate and OCPQ reference were not run by the same Docker daemon" >&2
  exit 1
fi

if [[ -n "${OCPM_SOURCE_REVISION:-}" ]]; then
  source_revision="$OCPM_SOURCE_REVISION"
else
  source_revision="$(git rev-parse --verify HEAD 2>/dev/null || true)"
  source_revision="${source_revision:-working-tree-preview}"
fi
if [[ -n "${OCPM_SOURCE_TREE_CLEAN:-}" ]]; then
  source_tree_clean="$OCPM_SOURCE_TREE_CLEAN"
elif git diff --quiet --ignore-submodules -- \
  && git diff --cached --quiet --ignore-submodules -- \
  && [[ -z "$(git ls-files --others --exclude-standard)" ]]; then
  source_tree_clean=true
else
  source_tree_clean=false
fi

docker build \
  --file benchmarks/ocpq/Dockerfile.candidate \
  --build-arg SOURCE_REVISION="$source_revision" \
  --tag "$image" \
  .

candidate_image_id="${OCPM_CANDIDATE_IMAGE_ID:-$(
  docker image inspect --format '{{.Id}}' "$image"
)}"
candidate_image_revision="$(
  docker image inspect \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
    "$image"
)"
if [[ "$candidate_image_revision" != "$source_revision" ]]; then
  echo "candidate image source label does not match the benchmark revision" >&2
  exit 1
fi
database_image_id="${OCPM_DATABASE_IMAGE_ID:-}"
if [[ -z "$database_image_id" && "$database_image" != "unspecified" ]]; then
  database_image_id="$(
    docker image inspect --format '{{.Id}}' "$database_image" 2>/dev/null || true
  )"
fi
database_image_id="${database_image_id:-unspecified}"
pg_ocpm_source_revision="${OCPM_PG_OCPM_SOURCE_REVISION:-working-tree-preview}"
pg_ocpm_source_tree_clean="${OCPM_PG_OCPM_SOURCE_TREE_CLEAN:-false}"
if [[ "$database_image" != "unspecified" ]]; then
  database_image_revision="$(
    docker image inspect \
      --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
      "$database_image"
  )"
  if [[ "$database_image_revision" != "$pg_ocpm_source_revision" ]]; then
    echo "database image source label does not match the pg_ocpm revision" >&2
    exit 1
  fi
fi

arguments=(
  --reference /benchmark/reference.json
  --output "/output/$output_name"
)
mounts=(
  --volume "$reference:/benchmark/reference.json:ro"
  --volume "$output_dir:/output"
)
docker run --rm \
  --network "$network" \
  --env OCPM_DATABASE_URL \
  --env OCPM_SOURCE_REVISION="$source_revision" \
  --env OCPM_SOURCE_TREE_CLEAN="$source_tree_clean" \
  --env OCPM_CANDIDATE_IMAGE="$image" \
  --env OCPM_CANDIDATE_IMAGE_ID="$candidate_image_id" \
  --env OCPM_DATABASE_IMAGE="$database_image" \
  --env OCPM_DATABASE_IMAGE_ID="$database_image_id" \
  --env OCPM_BENCHMARK_HOST_ID="$benchmark_host_id" \
  --env OCPM_PG_OCPM_SOURCE_REVISION="$pg_ocpm_source_revision" \
  --env OCPM_PG_OCPM_SOURCE_TREE_CLEAN="$pg_ocpm_source_tree_clean" \
  "${mounts[@]}" \
  "$image" \
  "${arguments[@]}"
