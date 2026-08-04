# API reference

Every public name below is importable from the top-level `ocpm_engine`
package. The modules listed are where each name is defined; algorithms run in
the native Rust extension (`ocpm_engine._native`), and the Python layer stays
a thin typed facade. Current version: 1.1.0.

## `ocpm_engine.standalone` — file-based analysis

Source-neutral facade over the standalone Rust engine. No database or
dataframe runtime is required.

### `StandaloneEngine`

| Constructor | Input |
|---|---|
| `StandaloneEngine(canonical_log)` | Canonical log as a JSON object |
| `StandaloneEngine.from_ocel2_json(value)` | OCEL 2.0 JSON path, string, or bytes |
| `StandaloneEngine.from_xes(value)` | XES path, string, or bytes |
| `StandaloneEngine.from_sqlite(path)` | OCEL 2.0 SQLite file |
| `StandaloneEngine.from_duckdb_parquet(source)` | Existing deployment-supplied DuckDB catalog over immutable Parquet snapshots |

| Method | Purpose |
|---|---|
| `provider_name()`, `capabilities()` | Report the active provider and its capability surface |
| `append(batch)` | Validate a bounded columnar batch and advance the source watermark atomically |
| `profile(view=None)` | Activity profile for a view (object types, time bounds) |
| `query(request)` | Typed filtering and binding queries |
| `discover(request)` | DFG, OC-DFG, Alpha, process-tree, Petri-net, OCPN, and declarative discovery |
| `conformance(request)` | Conformance checking against a discovered or supplied model |
| `enhance(request)` | Model enhancement (frequencies, durations, bottlenecks) |
| `fit_prediction(request)`, `predict(request)`, `evaluate_prediction(...)` | Prediction with temporal holdout evaluation |
| `execution_summary(request)` | Execution statistics for a request without materializing results |
| `canonical_json(view=None)`, `ocel2_json(view=None)`, `xes(object_type, view=None)` | Serialize a view to canonical JSON, OCEL 2.0 JSON, or XES |
| `write_sqlite(path, view=None)`, `write_parquet_snapshot(...)` | Persist a view to OCEL SQLite or a Parquet snapshot |
| `explain(view, capability)` | Report the selected provider boundary for a capability |

All requests and responses are versioned JSON objects (plain Python
dictionaries); the native layer releases the GIL during algorithm work.

```python
from ocpm_engine import StandaloneEngine

engine = StandaloneEngine.from_ocel2_json("events.json")

profile = engine.profile({"object_types": ["Order"]})
model = engine.discover(
    {
        "view": {"object_types": ["Order"]},
        "algorithm": "object_centric_dfg",
    }
)
```

### `serialize_model(artifact, format="json")`

Serializes a discovery artifact to an interchange format string.

## `ocpm_engine.engine` — PostgreSQL compatibility API

Query planning and execution against an installed
[pg_ocpm](https://github.com/verticalbarHQ/pg_ocpm). Works with any
psycopg2-style cursor; SQL and parameter values are always returned
separately.

### `OcpmEngine(dataset_id, tenant_id, *, wide_window_days=30)`

| Method | Purpose |
|---|---|
| `verify_pg_ocpm(cursor)` | Fail early unless pg_ocpm is installed; returns its version |
| `inspect_pg_ocpm(cursor)` | Negotiate the installed capability surface (`PgOcpmCapabilities`) |
| `inspect_binding_index(cursor)` | Exact binding-index coverage and dataset refresh markers |
| `build(request)` / `execute(cursor, request)` | Plan or run a `ProcessMiningRequest` |
| `build_dynamic_dfg(request)` / `execute_dynamic_dfg(cursor, plan)` | Composable dynamic filters compiled to exact DFG queries |
| `build_dynamic_case_ids(request)` | The same dynamic selection as an ordered case-ID projection |
| `build_event_log_summary(...)` / `execute_event_log_summary(...)` | Factorized event-log summaries (compact 0.9 path with exact 0.8 fallback) |
| `build_lifecycle_dfg(request)` / `execute_lifecycle_dfg(...)` | Windowed lifecycle DFG counts |
| `build_lifecycle_variants(...)` / `execute_lifecycle_variants(...)` | Windowed lifecycle variant counts |
| `build_edge_features(request)` / `execute_edge_features(...)` | Edge-feature sufficient statistics |

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

plan = engine.build(request)          # plan.sql, plan.params
cursor.execute(plan.sql, plan.params)
payload = cursor.fetchone()[0]
```

Existing API dictionaries translate directly:

```python
request = ProcessMiningRequest.from_mapping(request_body)
payload = engine.execute(cursor, request)
```

Native event-log algorithms negotiate capabilities first, then keep the
result factorized end to end:

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
execution = engine.execute_event_log_summary(
    cursor, request, capabilities=capabilities
)
training, test = execution.summaries
```

### `score_dynamic_dfg_rows(rows)`

Scores dynamic-DFG result rows into a response dictionary (edges, counts,
bottleneck order) without a database round trip.

## `ocpm_engine.analytics` — scoring compact aggregates

Model construction and scoring directly from sufficient-statistic rows; no
event-level data is needed.

| Function | Returns |
|---|---|
| `dfg_conformance(rows, *, coverage=0.95)` | `ConformanceScore(fitness, conforming, deviations)` |
| `variant_conformance(variants, train_counts, test_counts, *, coverage=0.95)` | `(fitness, conforming, deviations, test_total, model_variants)` |
| `next_activity(rows)` | `PredictionScore` (accuracy, correct, test_total, predictions) |
| `bottleneck_order(frequencies, mean_durations)` | Transition indexes ordered by bottleneck severity |
| `frequency_drift(labels, baseline_counts, current_counts, *, top_n=10)` | `DriftScore` with per-label `DriftContributor` entries |

`rows` are `TransitionCount(source, target, edge_type, train_count,
test_count)` records.

```python
from ocpm_engine import TransitionCount, dfg_conformance

rows = [
    TransitionCount("Create", "Approve", "directly_follows", 900, 95),
    TransitionCount("Create", "Reject", "directly_follows", 100, 5),
]
result = dfg_conformance(rows, coverage=0.95)
print(result.fitness, result.deviations)
```

## `ocpm_engine.event_batches` — factorized event-batch summaries

Native summarization of pg_ocpm 0.9+ factorized event batches into exact
per-window summaries without expanding event rows in Python.

| Function | Purpose |
|---|---|
| `summarize_event_batch_rows(rows)` | One `EventLogSummary` from factorized batch rows |
| `summarize_event_window_batch_rows(rows, ...)` | Aligned summaries across training/test/drift windows |

`EventLogSummary` carries `case_count`, `event_count`, `payload_bytes`, and
exact `variants`, `dfg`, and `activities` tuples (`EventVariantCount`,
`EventDfgEdge`, `EventActivityCount`).

```python
from ocpm_engine import summarize_event_batch_rows

cursor.execute(sql, params)                # ocpm.event_log_batches(...)
summary = summarize_event_batch_rows(cursor.fetchall())
print(summary.case_count, summary.variants[0])
```

## `ocpm_engine.bindings` — binding-capsule decoding

Native decoding for compact pg_ocpm binding-result capsules.

| Function | Purpose |
|---|---|
| `binding_capsule_info(capsule)` | Header inspection without full decode (`BindingCapsuleInfo`) |
| `decode_binding_capsule(capsule)` | Full decode to `BindingRow` tuples |
| `decode_binding_pair_groups(capsule)` | Grouped decode to `BindingPairGroup` tuples |

```python
from ocpm_engine import decode_binding_capsule

cursor.execute(sql, params)                # ocpm.binding_ids(...) capsule
rows = decode_binding_capsule(cursor.fetchone()[0])
```

## `ocpm_engine.models` — request and result models

Typed request contracts, execution results, and capability reports shared by
the compatibility API:

- Requests: `ProcessMiningRequest`, `DynamicDfgRequest`, `DynamicFilter`,
  `EdgeFilter`, `EventAttributeFilter`, `NetworkFilter`, `EventLogRequest`,
  `EventLogWindow`, `LifecycleDfgRequest`, `LifecycleVariantRequest`,
  `EdgeFeatureRequest`, `Endpoint`
- Plans and executions: `QueryPlan` (`sql`, `params`), `EventLogExecution`,
  `LifecycleDfgExecution`, `LifecycleVariantExecution`,
  `EdgeFeatureExecution`, `WindowedDfgCount`, `WindowedVariantCount`,
  `EdgeFeature`
- Capabilities and coverage: `PgOcpmCapabilities`, `BindingIndexCoverage`,
  `BindingNeighborCoverage`, `BindingRelationCoverage`

Request models accept plain dictionaries via `from_mapping(...)`.

## `ocpm_engine.queries` and `ocpm_engine.dynamic_queries` — SQL compilation

Low-level parameterized SQL generation, normally reached through
`OcpmEngine`:

- `process_map_sql(*, filtered_network, transitive_closure)` returns the
  parameterized process-map SQL for the requested shape.
- `compile_dynamic_dfg(request, *, dataset_id, tenant_id, projection)`
  returns `(sql, params, strategy)` for a `DynamicDfgRequest`, where
  `strategy` names the selected dynamic execution path.
