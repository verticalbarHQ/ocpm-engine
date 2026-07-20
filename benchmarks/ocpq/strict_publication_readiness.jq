# Publication evidence uses native JSON booleans. Shell callers normalize the
# corresponding environment inputs to the lowercase literals true or false
# before the benchmark processes serialize them.
def require_json_boolean($value; $name):
  if ($value | type) == "boolean" then
    $value
  else
    error("\($name) must be a JSON boolean")
  end;

def strict_publication_readiness(
  $latency_targets_met;
  $storage_within_limits;
  $latency_environment;
  $resource_environment;
  $reference_environment;
  $concurrency_exact;
  $memory_exact
):
  (require_json_boolean(
    $latency_targets_met;
    "latency_targets_met"
  )) as $latency_ok
  | (require_json_boolean(
      $storage_within_limits;
      "storage_within_limits"
    )) as $storage_ok
  | (require_json_boolean(
      $latency_environment.provenance_complete;
      "latency environment provenance_complete"
    )) as $latency_provenance
  | (require_json_boolean(
      $resource_environment.provenance_complete;
      "resource environment provenance_complete"
    )) as $resource_provenance
  | (require_json_boolean(
      $latency_environment.source_tree_clean;
      "latency environment source_tree_clean"
    )) as $latency_source_clean
  | (require_json_boolean(
      $resource_environment.source_tree_clean;
      "resource environment source_tree_clean"
    )) as $resource_source_clean
  | (require_json_boolean(
      $latency_environment.pg_ocpm_source_tree_clean;
      "latency environment pg_ocpm_source_tree_clean"
    )) as $latency_pg_clean
  | (require_json_boolean(
      $resource_environment.pg_ocpm_source_tree_clean;
      "resource environment pg_ocpm_source_tree_clean"
    )) as $resource_pg_clean
  | (require_json_boolean(
      $reference_environment.source_tree_clean;
      "reference environment source_tree_clean"
    )) as $reference_source_clean
  | (require_json_boolean(
      $concurrency_exact;
      "every_concurrency_level_pre_and_post_node_exact"
    )) as $concurrency_ok
  | (require_json_boolean(
      $memory_exact;
      "every_memory_query_and_node_exact"
    )) as $memory_ok
  | (
      $latency_provenance
      and $resource_provenance
      and $latency_source_clean
      and $resource_source_clean
      and $latency_pg_clean
      and $resource_pg_clean
      and $reference_source_clean
    ) as $provenance_complete
  | {
      provenance_complete: $provenance_complete,
      ready: (
        $latency_ok
        and $storage_ok
        and $provenance_complete
        and $concurrency_ok
        and $memory_ok
      )
    };
