#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: OCPM_DATABASE_URL=... $0 STRICT_OCPQ_REFERENCE_JSON OUTPUT_JSON" >&2
  exit 2
fi
: "${OCPM_DATABASE_URL:?OCPM_DATABASE_URL is required}"

for command in docker git jq shasum; do
  command -v "$command" >/dev/null || {
    echo "$command is required" >&2
    exit 2
  }
done

repo="$(git rev-parse --show-toplevel)"
reference="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
output_dir="$(cd "$(dirname "$2")" && pwd)"
output="$output_dir/$(basename "$2")"
image="${OCPM_CANDIDATE_IMAGE:-ocpm-engine:ocpq-strict-publication-preview}"
network="${OCPM_DOCKER_NETWORK:-bridge}"
database_container="${OCPM_DATABASE_CONTAINER:-ocpq-strict-nodes}"
temporary="$(mktemp -d "${TMPDIR:-/tmp}/ocpq-strict-gates.XXXXXX")"
cleanup() {
  status=$?
  if [[ $status -eq 0 ]]; then
    rm -rf "$temporary"
  else
    echo "strict gate failed; raw diagnostics retained at $temporary" >&2
  fi
  return "$status"
}
trap cleanup EXIT

reference_host_id="$(jq --exit-status --raw-output '.environment.benchmark_host_id' "$reference")"
docker_daemon_id="$(docker info --format '{{.ID}}')"
benchmark_host_id="sha256:$(
  printf '%s' "$docker_daemon_id" | shasum -a 256 | awk '{print $1}'
)"
if [[ "$benchmark_host_id" != "$reference_host_id" ]]; then
  echo "candidate and strict OCPQ reference were not run by the same Docker daemon" >&2
  exit 1
fi

source_revision="$(git -C "$repo" rev-parse --verify HEAD)"
if git -C "$repo" diff --quiet --ignore-submodules -- \
  && git -C "$repo" diff --cached --quiet --ignore-submodules -- \
  && [[ -z "$(git -C "$repo" ls-files --others --exclude-standard)" ]]; then
  source_tree_clean=true
else
  source_tree_clean=false
fi
reference_revision="$(jq --exit-status --raw-output '.environment.source_revision' "$reference")"
reference_clean="$(
  jq --raw-output '
    if (.environment.source_tree_clean | type) == "boolean" then
      .environment.source_tree_clean
    else
      error("reference environment source_tree_clean must be a JSON boolean")
    end
  ' "$reference"
)"
if [[ "$source_revision" != "$reference_revision" || "$source_tree_clean" != "$reference_clean" ]]; then
  echo "candidate and strict OCPQ reference revision/clean-state provenance differs" >&2
  exit 1
fi

source_tree_id="$(bash "$repo/benchmarks/ocpq/source_tree_id.sh" "$repo")"

candidate_runner_sha256="$({
  shasum -a 256 \
    "$repo/benchmarks/ocpq/strict_candidate_benchmark.rs" \
    "$repo/benchmarks/ocpq/strict_resource_support.rs" \
    "$repo/benchmarks/ocpq/source_tree_id.sh" \
    "$repo/benchmarks/ocpq/Dockerfile.candidate"
} | shasum -a 256 | awk '{print $1}')"

docker build \
  --file "$repo/benchmarks/ocpq/Dockerfile.candidate" \
  --build-arg SOURCE_REVISION="$source_revision" \
  --tag "$image" \
  "$repo"
candidate_image_id="$(docker image inspect --format '{{.Id}}' "$image")"
candidate_image_revision="$(
  docker image inspect \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
    "$image"
)"
if [[ "$candidate_image_revision" != "$source_revision" ]]; then
  echo "candidate image revision label does not match the source revision" >&2
  exit 1
fi

database_image="${OCPM_DATABASE_IMAGE:-$(
  docker inspect --format '{{.Config.Image}}' "$database_container"
)}"
database_image_id="${OCPM_DATABASE_IMAGE_ID:-$(
  docker inspect --format '{{.Image}}' "$database_container"
)}"
database_image_revision="$(
  docker image inspect \
    --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
    "$database_image_id" 2>/dev/null || true
)"
pg_ocpm_source_revision="${OCPM_PG_OCPM_SOURCE_REVISION:-$database_image_revision}"
pg_ocpm_source_revision="${pg_ocpm_source_revision:-unknown}"
pg_ocpm_source_tree_clean="${OCPM_PG_OCPM_SOURCE_TREE_CLEAN:-false}"
case "$pg_ocpm_source_tree_clean" in
  true|false) ;;
  *)
    echo "OCPM_PG_OCPM_SOURCE_TREE_CLEAN must be true or false" >&2
    exit 2
    ;;
esac
if [[ "$database_image_revision" == "$pg_ocpm_source_revision" \
  && "$pg_ocpm_source_revision" != "unknown" \
  && "$source_tree_clean" == true \
  && "$reference_clean" == true \
  && "$pg_ocpm_source_tree_clean" == true ]]; then
  provenance_complete=true
else
  provenance_complete=false
  echo "warning: release provenance is incomplete; artifact will not be publication-ready" >&2
fi

common_docker_arguments=(
  --rm
  --network "$network"
  --env OCPM_DATABASE_URL
  --env OCPM_BENCHMARK_HOST_ID="$benchmark_host_id"
  --env OCPM_SOURCE_REVISION="$source_revision"
  --env OCPM_SOURCE_TREE_CLEAN="$source_tree_clean"
  --env OCPM_SOURCE_TREE_ID="$source_tree_id"
  --env OCPM_CANDIDATE_RUNNER_SHA256="$candidate_runner_sha256"
  --env OCPM_CANDIDATE_IMAGE="$image"
  --env OCPM_CANDIDATE_IMAGE_ID="$candidate_image_id"
  --env OCPM_DATABASE_IMAGE="$database_image"
  --env OCPM_DATABASE_IMAGE_ID="$database_image_id"
  --env OCPM_PG_OCPM_SOURCE_REVISION="$pg_ocpm_source_revision"
  --env OCPM_PG_OCPM_SOURCE_TREE_CLEAN="$pg_ocpm_source_tree_clean"
  --env OCPM_PROVENANCE_COMPLETE="$provenance_complete"
  --volume "$reference:/benchmark/reference.json:ro"
  --volume "$temporary:/output"
)

for query in Q1 Q2 Q3 Q4 Q5 Q6 Q7; do
  echo "strict latency: $query in a fresh container (0 warmups, 10 runs)" >&2
  process_run_id="sha256:$(
    printf 'latency:%s:%s:%s:%s' "$query" "$RANDOM" "$$" "$SECONDS" \
      | shasum -a 256 | awk '{print $1}'
  )"
  docker run \
    "${common_docker_arguments[@]}" \
    --env TOKIO_WORKER_THREADS=1 \
    --env OCPM_PROCESS_RUN_ID="$process_run_id" \
    "$image" \
    --reference /benchmark/reference.json \
    --output "/output/latency-$query.json" \
    --query "$query"
done

for query in Q1 Q2 Q3 Q4 Q5 Q6 Q7; do
  echo "strict memory: $query in a fresh container" >&2
  process_run_id="sha256:$(
    printf 'memory:%s:%s:%s:%s' "$query" "$RANDOM" "$$" "$SECONDS" \
      | shasum -a 256 | awk '{print $1}'
  )"
  docker run \
    "${common_docker_arguments[@]}" \
    --env TOKIO_WORKER_THREADS=1 \
    --env OCPM_PROCESS_RUN_ID="$process_run_id" \
    "$image" \
    --reference /benchmark/reference.json \
    --output "/output/memory-$query.json" \
    --memory-only-query "$query"
done

echo "strict resources: storage and 1/4/8/16-client concurrency" >&2
resource_process_run_id="sha256:$(
  printf 'resources:%s:%s:%s' "$RANDOM" "$$" "$SECONDS" \
    | shasum -a 256 | awk '{print $1}'
)"
# Three client runtime workers leave CPU headroom for the co-located PostgreSQL
# server while still scaling the asynchronous request path at 16 clients.
docker run \
  "${common_docker_arguments[@]}" \
  --env TOKIO_WORKER_THREADS="${OCPM_CONCURRENCY_WORKER_THREADS:-3}" \
  --env OCPM_PROCESS_RUN_ID="$resource_process_run_id" \
  "$image" \
  --reference /benchmark/reference.json \
  --output /output/resources.json \
  --resource-gates

jq --slurp '.' "$temporary"/latency-Q{1,2,3,4,5,6,7}.json > "$temporary/latency.json"
jq --slurp '.' "$temporary"/memory-Q{1,2,3,4,5,6,7}.json > "$temporary/memory.json"

maximum_total_serving_bytes="${OCPM_MAX_TOTAL_SERVING_BYTES:-134217728}"
maximum_index_bytes="${OCPM_MAX_INDEX_BYTES:-16777216}"
maximum_binding_summary_bytes="${OCPM_MAX_BINDING_SUMMARY_BYTES:-8388608}"

jq --null-input \
  -L "$repo/benchmarks/ocpq" \
  --slurpfile latency "$temporary/latency.json" \
  --slurpfile memory "$temporary/memory.json" \
  --slurpfile resources "$temporary/resources.json" \
  --argjson maximum_total_serving_bytes "$maximum_total_serving_bytes" \
  --argjson maximum_index_bytes "$maximum_index_bytes" \
  --argjson maximum_binding_summary_bytes "$maximum_binding_summary_bytes" '
  include "strict_publication_readiness";
  def geometric_mean(values):
    values | map(log) | (add / length) | exp;
  ($latency[0]) as $latency_runs
  | ($memory[0]) as $memory_runs
  | ($resources[0]) as $resource
  | ($latency_runs[0]) as $first
  | (reduce $latency_runs[] as $run ({}; . + $run.queries)) as $queries
  | (reduce $memory_runs[] as $run ({};
      . + {($run.query): ($run | del(
        .schema_version,
        .generated_at_unix_ms,
        .reference_schema_version,
        .reference_artifact_sha256,
        .reference_source,
        .reference_environment,
        .release,
        .environment,
        .query
      ))}
    )) as $memory_by_query
  | if ($latency_runs | length) != 7
      or ($memory_runs | length) != 7
      or ($queries | keys) != ["Q1","Q2","Q3","Q4","Q5","Q6","Q7"]
      or ($memory_by_query | keys) != ["Q1","Q2","Q3","Q4","Q5","Q6","Q7"]
      or any($latency_runs[];
        (.selected_queries | length) != 1
        or .schema_version != 2
        or .reference_artifact_sha256 != $first.reference_artifact_sha256
        or .reference_source != $first.reference_source
        or .reference_environment != $first.reference_environment
        or .release != $first.release
        or (.environment | del(
          .tokio_worker_threads_env,
          .process_run_id,
          .container_hostname,
          .client_process_id
        )) != ($first.environment | del(
          .tokio_worker_threads_env,
          .process_run_id,
          .container_hostname,
          .client_process_id
        ))
        or .summary.every_query_and_node_exact != true
      )
      or any($memory_runs[];
        .reference_artifact_sha256 != $first.reference_artifact_sha256
        or .reference_source != $first.reference_source
        or .reference_environment != $first.reference_environment
        or .release != $first.release
        or (.environment | del(
          .tokio_worker_threads_env,
          .process_run_id,
          .container_hostname,
          .client_process_id
        )) != ($first.environment | del(
          .tokio_worker_threads_env,
          .process_run_id,
          .container_hostname,
          .client_process_id
        ))
        or .every_node_exact != true
      )
      or $resource.reference_artifact_sha256 != $first.reference_artifact_sha256
      or $resource.reference_source != $first.reference_source
      or $resource.reference_environment != $first.reference_environment
      or $resource.release != $first.release
      or ($resource.environment | del(
        .tokio_worker_threads_env,
        .process_run_id,
        .container_hostname,
        .client_process_id
      )) != ($first.environment | del(
        .tokio_worker_threads_env,
        .process_run_id,
        .container_hostname,
        .client_process_id
      ))
      or $resource.every_concurrency_level_pre_and_post_node_exact != true
      or ([$latency_runs[].environment.process_run_id] | unique | length) != 7
      or ([$latency_runs[].environment.container_hostname] | unique | length) != 7
      or $resource.storage.result_cache_rows != 0
      or $resource.storage.request_result_cache_enabled != false
    then error("strict publication evidence is incomplete or inconsistent")
    else . end
  | (geometric_mean([$queries[].reference_ocpq_mean_ms])) as $reference_geomean
  | (geometric_mean([$queries[].candidate_mean_ms])) as $candidate_geomean
  | (geometric_mean([$queries[].speedup_vs_reference_ocpq])) as $speedup_geomean
  | ([$queries[].speedup_vs_reference_ocpq] | min) as $minimum_speedup
  | ($resource.storage.total_serving_bytes <= $maximum_total_serving_bytes
      and $resource.storage.index_bytes <= $maximum_index_bytes
      and $resource.storage.binding_summary_bytes <= $maximum_binding_summary_bytes
    ) as $storage_within_limits
  | ($minimum_speedup >= 5 and $speedup_geomean >= 10) as $latency_targets_met
  | strict_publication_readiness(
      $latency_targets_met;
      $storage_within_limits;
      $first.environment;
      $resource.environment;
      $first.reference_environment;
      $resource.every_concurrency_level_pre_and_post_node_exact;
      all($memory_runs[]; .every_node_exact == true)
    ) as $readiness
  | {
      schema_version: 1,
      artifact_kind: "strict-all-node-ocpq-publication-gates",
      generated_at_unix_ms: (now * 1000 | floor),
      fresh_container_per_query: true,
      latency_processes: (
        reduce $latency_runs[] as $run ({};
          . + {($run.selected_queries[0]): {
            container_id: $run.environment.container_hostname,
            process_start_id: $run.environment.process_run_id,
            client_process_id: $run.environment.client_process_id
          }}
        )
      ),
      publication_status: {
        ready: $readiness.ready,
        latency_targets_met: $latency_targets_met,
        minimum_query_speedup_required: 5,
        geometric_mean_speedup_required: 10,
        every_latency_query_and_node_exact: true,
        every_memory_query_and_node_exact: true,
        every_concurrency_level_pre_and_post_node_exact:
          $resource.every_concurrency_level_pre_and_post_node_exact,
        storage_within_limits: $storage_within_limits,
        provenance_complete: $readiness.provenance_complete
      },
      release: $first.release,
      environment: {
        latency_and_memory: $first.environment,
        concurrency: $resource.environment
      },
      reference_schema_version: $first.reference_schema_version,
      reference_artifact_sha256: $first.reference_artifact_sha256,
      reference_source: $first.reference_source,
      reference_environment: $first.reference_environment,
      method: {
        latency: $first.method,
        latency_fresh_container_per_query: true,
        latency_fresh_container_count: 7,
        latency_fresh_processes: (
          $latency_runs
          | map({
              query: .selected_queries[0],
              process_run_id: .environment.process_run_id,
              container_hostname: .environment.container_hostname,
              client_process_id: .environment.client_process_id
            })
        ),
        memory: $memory_runs[0] | {
          mode,
          measurement_boundary,
          correctness_boundary
        },
        resources: $resource.method
      },
      summary: {
        reference_ocpq_geometric_mean_ms: $reference_geomean,
        candidate_geometric_mean_ms: $candidate_geomean,
        speedup_geometric_mean: $speedup_geomean,
        minimum_query_speedup: $minimum_speedup,
        maximum_client_peak_over_baseline_rss_bytes:
          ([$memory_runs[].peak_over_baseline_rss_bytes] | max),
        maximum_client_peak_over_baseline_vmhwm_bytes:
          ([$memory_runs[].peak_over_baseline_vmhwm_bytes] | max),
        maximum_owned_tree_allocated_bytes:
          ([$memory_runs[].owned_tree_allocated_bytes] | max)
      },
      storage_limits: {
        maximum_total_serving_bytes: $maximum_total_serving_bytes,
        maximum_index_bytes: $maximum_index_bytes,
        maximum_binding_summary_bytes: $maximum_binding_summary_bytes
      },
      storage: $resource.storage,
      queries: $queries,
      memory: $memory_by_query,
      concurrency: $resource.concurrency
    }
' > "$output"

jq --exit-status '.publication_status' "$output"
echo "strict publication preview written to $output" >&2
