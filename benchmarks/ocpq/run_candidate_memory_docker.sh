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

mounts=(--volume "$reference:/benchmark/reference.json:ro")
q5_arguments=()
if [[ -n "${OCPM_Q5_SQL_FILE:-}" ]]; then
  q5_sql="$(cd "$(dirname "$OCPM_Q5_SQL_FILE")" && pwd)/$(basename "$OCPM_Q5_SQL_FILE")"
  mounts+=(--volume "$q5_sql:/benchmark/q5.sql:ro")
  q5_arguments+=(--q5-sql-file /benchmark/q5.sql)
fi

for query in Q1 Q2 Q3 Q4 Q5 Q6 Q7; do
  expected_rows="$(jq --exit-status --raw-output ".queries.${query}.canonical_output.rows" "$reference")"
  if [[ -n "${OCPM_Q5_SQL_FILE:-}" ]]; then
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
      --env TOKIO_WORKER_THREADS=1 \
      "${mounts[@]}" \
      "$image" \
      --reference /benchmark/reference.json \
      --output - \
      --memory-only-query "$query" \
      --expected-rows "$expected_rows" \
      "${q5_arguments[@]}"
  else
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
      --env TOKIO_WORKER_THREADS=1 \
      "${mounts[@]}" \
      "$image" \
      --reference /benchmark/reference.json \
      --output - \
      --memory-only-query "$query" \
      --expected-rows "$expected_rows"
  fi
done | jq --slurp '
  .[0] as $first
  | if length != 7 or any(.[];
      .release != $first.release
      or .environment != $first.environment
      or .reference_source != $first.reference_source
      or .semantic_parity != true
    ) then
    error("memory diagnostics did not produce one consistent Q1-Q7 set")
  else . end
  | .[0] as $first
  | {
    schema_version: 3,
    generated_at_unix_ms: (now * 1000 | floor),
    release: $first.release,
    environment: $first.environment,
    reference_source: $first.reference_source,
    mode: "fresh-container-per-query-peak-rss-diagnostic",
    fresh_container_per_query: true,
    summary: {
      all_queries_exact: all(.[]; .semantic_parity == true),
      maximum_owned_rows_bytes: (map(.owned_rows_bytes) | max),
      maximum_peak_over_baseline_rss_bytes:
        (map(.peak_over_baseline_rss_bytes) | max)
    },
    queries: (
      map(
        del(.release, .environment, .reference_source)
        | {key: .query, value: .}
      )
      | from_entries
    )
  }
' > "$output_dir/$output_name"
