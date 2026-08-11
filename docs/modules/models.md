# `ocpm_engine.models`

Public request and query-plan models shared by the compatibility API. All
models are frozen dataclasses; request models also accept plain dictionaries
through `from_mapping(...)`, with ISO-8601 strings (including a trailing
`Z`) parsed as datetimes.

## Endpoints

`Endpoint` enumerates the generic request shapes: `process_map`,
`variant_list`, `timeline`, `case_throughput`, `edge_info`, `case_list`,
`entire_process_map`, `dynamic_dfg`, `event_log_summary`, `lifecycle_dfg`,
`lifecycle_variants`, `edge_features`.

## Generic requests

```python
ProcessMiningRequest(
    endpoint: Endpoint | str,
    backbone_type: str | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    timeline_period: str = "week",
    status: str | None = None,
    variants: tuple[str, ...] = (),
    network: NetworkFilter = NetworkFilter(),
    source_activity: str | None = None,
    target_activity: str | None = None,
    edge_context: str | None = None,
    limit: int = 20,
    offset: int = 0,
    transitive_closure: bool | None = None,
)
```

`NetworkFilter(activities, edges, min_execution_time, max_execution_time)`
narrows process-map shapes; its `from_mapping` reads the API's
`network_filter` dictionary.

## Dynamic DFG requests

```python
DynamicDfgRequest(
    backbone_type: str,
    from_date: datetime,
    to_date: datetime,
    filter: DynamicFilter = DynamicFilter(),
)
```

`DynamicFilter` holds composable exact case predicates. Every tuple is
conjunctive; included predicates must exist, excluded predicates must not,
and multiple statuses are alternatives:

| Field | Meaning |
|---|---|
| `statuses` | Case status alternatives |
| `included_activities` / `excluded_activities` | Activities the case must / must not contain |
| `min_case_execution_time` / `max_case_execution_time` | Case duration bounds (seconds) |
| `included_event_attributes` / `excluded_event_attributes` | `EventAttributeFilter(key, value)` — `actor` and `context` address the first-class event columns, any other key the JSON attribute map |
| `included_related_object_types` / `excluded_related_object_types` | Required / forbidden related object types |
| `included_edges` / `excluded_edges` | `EdgeFilter(source, target, min_execution_time, max_execution_time)` transitions |

```python
from ocpm_engine import DynamicDfgRequest

request = DynamicDfgRequest.from_mapping(
    {
        "backbone_type": "Order",
        "from_date": "2026-01-01T00:00:00Z",
        "to_date": "2026-02-01T00:00:00Z",
        "filter": {
            "statuses": ["complete"],
            "activities": {"include": ["Order:Approved"]},
            "event_attributes": {"include": [{"key": "region", "value": "west"}]},
            "edges": {
                "include": [
                    {
                        "source": "Order:Created",
                        "target": "Order:Approved",
                        "max_execution_time": 86400,
                    }
                ]
            },
        },
    }
)
```

## Event-log and lifecycle requests

```python
EventLogRequest(object_type: str, windows: tuple[EventLogWindow, ...])
EventLogWindow(from_date: datetime, to_date: datetime)

LifecycleDfgRequest(object_types, windows, source_activities=(),
                    target_activities=(), minimum_total_frequency=1)

LifecycleVariantRequest(object_types, windows, statuses=(),
                        variant_hashes=(), include_activities=(),
                        exclude_activities=(), minimum_total_frequency=1)

EdgeFeatureRequest(from_date, to_date, slow_threshold=inf,
                   source_object_types=(), target_object_types=(),
                   source_activities=(), target_activities=(),
                   edge_types=(), contexts=(), minimum_frequency=1)
```

## Plans and executions

| Type | Fields |
|---|---|
| `QueryPlan` | `endpoint`, `sql`, `params`, `strategy` |
| `LifecycleDfgExecution` | `strategy`, `database_rows`, `expanded_event_rows`, `counts` |
| `WindowedDfgCount` | `object_type`, `source`, `target`, `frequencies` |
| `LifecycleVariantExecution` | `strategy`, `database_rows`, `counts` |
| `WindowedVariantCount` | `object_type`, `path_hash`, `activity_path`, `frequencies` |
| `EdgeFeatureExecution` | `strategy`, `database_rows`, `features` |
| `EdgeFeature` | `source`, `target`, `source_object_type`, `target_object_type`, `edge_type`, `frequency`, `mean_duration`, `minimum_duration`, `maximum_duration`, `standard_deviation`, `slow_count`, `slow_rate` |

`frequencies` tuples are aligned to the request's windows, zero-filled where
a path or edge is absent from a window.

## Capabilities and coverage

```python
PgOcpmCapabilities(
    version,
    event_log_rows,
    event_log_batches,
    event_log_window_batches,
    lifecycle_dfg_window_counts=False,
    lifecycle_variant_window_counts=False,
)
```

Convenience properties: `factorized_event_export`,
`factorized_multi_window_export`, `lifecycle_dfg_pushdown`,
`lifecycle_variant_pushdown`.

```python
BindingIndexCoverage(
    refreshed_at,
    source_watermark,
    event_identity_complete,
    object_types,
    activities,
    event_activities,
    neighbors,
    relations,
)
```

Observable binding-index declarations and dataset refresh markers, with
membership helpers `covers_object_type(...)`, `covers_activity(...)`,
`covers_event_activity(...)`, `covers_neighbor(...)`, and
`covers_relation(...)` over `BindingNeighborCoverage` and
`BindingRelationCoverage` entries.
