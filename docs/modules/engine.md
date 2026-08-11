# `ocpm_engine.engine`

Translate API request shapes into parameterized
[pg_ocpm](https://github.com/verticalbarHQ/pg_ocpm) queries. Works with any
psycopg2-style cursor (`execute`, `fetchone`, `fetchall`); SQL and parameter
values are always returned separately, and every `build_*` method has a
matching `execute_*` method.

## `OcpmEngine`

```python
OcpmEngine(dataset_id: int, tenant_id: int, *, wide_window_days: int = 30)
```

Build and execute process-mining read paths over an installed pg_ocpm.
`dataset_id` must be positive and `tenant_id` non-negative;
`wide_window_days` controls when a request counts as wide-window for
strategy selection.

### Environment inspection

```python
OcpmEngine.verify_pg_ocpm(cursor) -> str
```
Fail early unless pg_ocpm is installed; returns its version. Call at
application startup.

```python
OcpmEngine.inspect_pg_ocpm(cursor) -> PgOcpmCapabilities
```
Detect callable extension features and retain an exact fallback; pass the
result to the `execute_*` methods that accept `capabilities`.

```python
inspect_binding_index(cursor) -> BindingIndexCoverage
```
Read exact declared binding coverage and dataset refresh markers without
assuming freshness. Select a binding-index plan only after its required
declaration is present.

### Generic requests

```python
build(request: ProcessMiningRequest) -> QueryPlan
execute(cursor, request: ProcessMiningRequest) -> Any
```

`QueryPlan` carries `endpoint`, `sql`, `params`, and the selected
`strategy`.

```python
from datetime import UTC, datetime, timedelta

from ocpm_engine import OcpmEngine, ProcessMiningRequest

engine = OcpmEngine(dataset_id=42, tenant_id=7)
request = ProcessMiningRequest(
    endpoint="process_map",
    backbone_type="Order",
    from_date=datetime.now(UTC) - timedelta(days=7),
    to_date=datetime.now(UTC),
    status="complete",
)

plan = engine.build(request)
cursor.execute(plan.sql, plan.params)
payload = cursor.fetchone()[0]

# or, from an existing API dictionary:
payload = engine.execute(cursor, ProcessMiningRequest.from_mapping(request_body))
```

### Dynamic filters

```python
build_dynamic_dfg(request: DynamicDfgRequest) -> QueryPlan
execute_dynamic_dfg(cursor, request_or_plan) -> dict
build_dynamic_case_ids(request: DynamicDfgRequest) -> QueryPlan
```

One uniform filter contract compiles to an exact filtered DFG;
`execute_dynamic_dfg` applies the shared Rust ranking kernel to the result
rows. `build_dynamic_case_ids` exposes the same selection as an ordered
case-ID projection for correctness checks, pagination seeds, and downstream
analysis. See [`ocpm_engine.models`](models.md) for the
`DynamicDfgRequest` filter semantics.

### Event-log summaries

```python
build_event_log_summary(request: EventLogRequest,
                        capabilities: PgOcpmCapabilities) -> QueryPlan
execute_event_log_summary(cursor, request_or_plan, *,
                          capabilities=None) -> EventLogExecution
```

Chooses the factorized pg_ocpm 0.9 export or the exact 0.8 compatibility
path, and reports transfer and expansion costs on the returned
`EventLogExecution` (`strategy`, `database_rows`, `expanded_event_rows`,
`summaries`). Requests larger than 256 windows are split transparently and
the global window order is preserved.

```python
from ocpm_engine import EventLogRequest, EventLogWindow

capabilities = engine.inspect_pg_ocpm(cursor)
request = EventLogRequest(
    object_type="Order",
    windows=(
        EventLogWindow(training_start, training_end),
        EventLogWindow(test_start, test_end),
    ),
)
execution = engine.execute_event_log_summary(cursor, request, capabilities=capabilities)
assert execution.expanded_event_rows == 0  # factorized 0.9 path
training, test = execution.summaries
```

### Lifecycle pushdowns

```python
build_lifecycle_dfg(request: LifecycleDfgRequest) -> QueryPlan
execute_lifecycle_dfg(cursor, request, *, capabilities=None) -> LifecycleDfgExecution
build_lifecycle_variants(request: LifecycleVariantRequest,
                         capabilities: PgOcpmCapabilities) -> QueryPlan
execute_lifecycle_variants(cursor, request, *,
                           capabilities=None) -> LifecycleVariantExecution
```

Bounded exact DFG and complete-variant pushdowns over finalized lifecycle
paths, with lossless fallbacks on older extension versions. Results carry
exact aligned per-window frequencies (zero-filled where absent).

### Edge features

```python
build_edge_features(request: EdgeFeatureRequest) -> QueryPlan
execute_edge_features(cursor, request) -> EdgeFeatureExecution
```

Selective exact edge-feature aggregates (frequency, mean/min/max duration,
sample deviation, slow count and rate) streamed without reconstructing event
logs.

## `score_dynamic_dfg_rows`

```python
score_dynamic_dfg_rows(rows: list[tuple]) -> dict
```

Canonicalize dynamic-DFG count rows and rank bottlenecks with the Rust
kernel; used by `execute_dynamic_dfg` and callable directly on rows fetched
elsewhere. Raises if the rows are empty or their selected counts are
inconsistent.

## Server-side cursors

For large remote results, use a driver-level server-side cursor so the
client driver does not buffer every factorized row. With psycopg2 this
requires an explicit transaction:

```python
connection.autocommit = False
with connection.cursor(name="ocpm_event_batches") as cursor:
    cursor.itersize = 64
    execution = engine.execute_event_log_summary(
        cursor, request, capabilities=capabilities
    )
connection.commit()
```
