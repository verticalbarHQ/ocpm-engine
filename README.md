# ocpm-engine

`ocpm-engine` 1.1 is a standalone, Rust-first object-centric process-mining
engine. It can load canonical or OCEL JSON, XES, CSV, and SQLite data directly,
then query, discover, check, enhance, predict, and serialize models without a
database or Python dataframe runtime.

`pg_ocpm >= 1.0.0` is an optional provider. When present, the engine pushes
selective scans and sufficient-statistic aggregation into PostgreSQL while the
same source-neutral Rust kernels construct and score models. The legacy
PostgreSQL planner remains available for `pg_ocpm >= 0.8.0` during the 1.x
compatibility window.
Legacy PostgreSQL planner compatibility: Required extension version: `pg_ocpm >= 0.8.0`.

Current engine version: **1.1.0**. See the [release notes](CHANGELOG.md) and
[academic implementation provenance](docs/academic-implementation-provenance.md).

## Native capabilities

- typed object-centric filtering and binding queries;
- DFG, OC-DFG, Alpha, process-tree, Petri-net, OCPN, and declarative discovery;
- frequency coverage, token replay, bounded alignment, OCPN, and declarative
  conformance;
- process maps, timelines, histograms, performance, rework, organizational,
  bottleneck, window-comparison, and drift enhancement;
- next-activity, remaining-time, and outcome prediction with temporal holdout
  evaluation;
- canonical JSON, DOT, PNML, SVG, OCEL JSON, XES, CSV, and SQLite I/O; and
- Python 3.11+ stable-ABI bindings that release the GIL during native work.

OCEL 2.0 XML is intentionally not claimed in 1.0. The detailed XML interchange
syntax is not defined by the peer-reviewed sources currently admitted by this
project's clean-room policy. It will remain deferred until an eligible source
or an explicit interoperability-policy change exists.

The `ocpm-postgres` crate provides an asynchronous adapter for activity profiles
and single-window and multi-window DFG/variant counts. It also retrieves and
decodes generic binding-result capsules for cardinality, required-activity,
eventually-follows, actor, delay, and related-object pair operations. Pair
results remain factorized, expose borrowed pair groups and materialized columns,
and expand only through an exact-size lazy iterator when rows are genuinely
needed.
Multi-window requests retrieve aligned training, test, comparison-period, or
drift statistics in one database request.

With `pg_ocpm >= 0.10.0`, exact lifecycle DFG requests use a compact native
multi-window aggregate over finalized case variants. The engine requests only
the sufficient statistics needed for DFG conformance and prediction, while
complete-variant and edge-duration analyses use their own compact aggregates.
Only analyses that genuinely require event-level detail retain the factorized
event path.
Requests can span multiple object types and arbitrary aligned windows; the
engine chunks more than 256 windows to bound database working memory.

For algorithms that genuinely require individual events, `ocpm-postgres`
detects the installed callable surface. With `pg_ocpm >= 0.9.0`, it decodes
factorized single- or multi-window activity-path bucket rows, whose packed
vectors can represent multiple cases, directly into incremental Rust summaries
through bounded cursor chunks without constructing a whole-result Python list,
event rows, or Python dataframes. With
`pg_ocpm 0.8.x`, it preserves the exact asynchronous event-row stream as a
compatibility fallback. Aggregate-native algorithms continue to use sufficient
statistics when those are the smaller input.

Event-log window boundaries are inclusive, and a case is selected only when its
complete lifecycle is contained in the window. Consequently, two adjacent
nonoverlapping timestamp windows require a one-microsecond gap between the
first window's inclusive end and the second window's inclusive start.

The existing Python query planner remains available for these request shapes:

- Dynamically filtered directly-follows graphs with composable status,
  activity existence/nonexistence, case-duration, event-attribute,
  related-object, and edge-duration predicates
- Filtered process maps with date, case status, variant, activity, case-duration,
  and edge-duration filters
- Variant distribution
- Case timeline
- Case-duration histogram
- Selected-edge duration histogram, including context filtering
- Paginated case detail hydration
- Whole-dataset process map

The planner uses compact `pg_ocpm` case, event, edge, and adjacency structures.
It selects bounded one-hop traversal for narrow windows, transitive closure for
wide or unbounded windows, and exact boundary reconstruction for wide variant
queries.

The legacy 0.10 APIs also expose lifecycle DFG counts, complete lifecycle variants,
and filtered edge-duration features as general sufficient-statistic requests.
They accept caller-selected datasets, tenants, object/activity filters, and
arbitrary aligned windows; requests larger than 256 windows are transparently
chunked and zero-filled back into the original order. Rich event-level analyses
continue to use the factorized event stream.

The clean 1.0 Docker evidence is split by compatible workload and dataset:

- [strict OCPQ Q1-Q7](docs/ocpq-performance.md), including all-node exactness,
  same-host latency, concurrency, storage, memory, and provenance gates;
- [four-way OCPQ comparison](docs/ocpq-1.0-four-way-comparison.md) across OCPQ,
  vanilla PostgreSQL plus fixed PM4Py, `pg_ocpm` plus the same PM4Py evaluator,
  and `pg_ocpm` plus `ocpm-engine`;
- [SAP O2C/P2P three-way comparison](docs/sap-pm4py-three-way-performance.md)
  across vanilla PostgreSQL plus PM4Py, `pg_ocpm` plus unchanged PM4Py, and
  `pg_ocpm` plus `ocpm-engine`; and
- separate same-dataset common-workload pairs for
  [Rust4PM](docs/rust4pm-vs-pg-ocpm-engine.md) and
  [OCPA](docs/ocpa-vs-pg-ocpm-engine.md).

The strict 1.0 OCPQ result is 16.137x faster than OCPQ 0.6.7 by same-host
geometric-mean latency with exact parity at every tree node. Across eight SAP
workloads, the engine is 55.529x faster than vanilla PostgreSQL plus PM4Py,
while `pg_ocpm` plus the fixed PM4Py evaluator is 1.338x faster. The Rust4PM
pair is publication-ready; the OCPA pair remains descriptive because its
documented native importer failed on the unchanged upstream example.

The benchmark systems are isolated black-box arms. Product algorithms are
independently implemented from the peer-reviewed sources in the
[academic provenance ledger](docs/academic-implementation-provenance.md).
The benchmark reports state the timing, memory, storage, dataset, and dynamic-
query interpretation boundaries; fixed-workload ratios are not generalized to
unmeasured algorithms.

The 0.10 root cause, general API boundaries, resource model, and peer-reviewed
database/process-mining basis remain documented in
[pg_ocpm 0.10 general aggregation design](docs/pg-ocpm-0.10-general-aggregation.md).
For the detailed application read-path design and code references, see
[Application query performance improvements](docs/technical-performance-improvements.md).
For the open-source capability survey, license boundary, research review, and
database-versus-engine placement decisions, see
[Process-mining capability map](docs/process-mining-capability-map.md).
The preliminary release, patent, and ICPM evidence assessment is in
[Open-source, patent, and ICPM assessment](docs/open-source-patent-icpm-assessment.md).
The implementation-ready strategy for a provider-independent DuckDB module
over local and S3 Parquet is in the
[DuckDB Parquet provider specification](docs/duckdb-parquet-provider-spec.md).

## Install

```sh
python -m pip install .
```

For local development:

```sh
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
```

See [the benchmark guide](benchmarks/README.md) for the current Docker commands,
artifact promotion rules, exact-answer checks, and release gates. Preview and
dirty-tree artifacts cannot satisfy a publication check.

## Use

The standalone facade accepts versioned JSON requests and returns ordinary
Python dictionaries while algorithms run in Rust:

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

DuckDB Parquet is an optional provider. It dynamically links to a compatible
deployment-supplied DuckDB 1.5 client and opens an existing caller-selected
catalog; the wheel does not bundle DuckDB and the provider does not create or
operate a database service. The `existing` path must already exist; the engine
never creates that catalog implicitly:

```python
engine = StandaloneEngine.from_duckdb_parquet(
    {
        "database": {
            "kind": "existing",
            "path": "/catalog/analytics.duckdb",
            "read_only": True,
        },
        "location": {"kind": "local", "root": "/data/ocel-parquet"},
        "snapshot": {"kind": "current", "pointer": "CURRENT"},
        "layout": {"kind": "canonical_v1"},
        "cache": {"kind": "direct"},
        "options": {
            "memory_budget_bytes": 536_870_912,
            "result_cache_bytes": 67_108_864,
            "materialize_execution_relation": True,
        },
    }
)
```

Set `result_cache_bytes` to zero for cache-disabled operation and set
`materialize_execution_relation` to false to minimize connection-open work and
DuckDB-managed resident memory. S3 uses the same API with a structured `s3://`
location and deployment credential-chain references.

`append()` validates a bounded columnar batch and advances the source watermark
only after the complete batch succeeds. `explain()` reports the selected
provider boundary without exposing provider-specific expressions.

### Compatibility API

Compact aggregate rows can be scored directly:

```python
from ocpm_engine import TransitionCount, dfg_conformance

rows = [
    TransitionCount("Create", "Approve", "directly_follows", 900, 95),
    TransitionCount("Create", "Reject", "directly_follows", 100, 5),
]
result = dfg_conformance(rows, coverage=0.95)
```

The compatibility query-planning API works with a psycopg2-style cursor and
returns parameterized SQL separately from its values:

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
```

Existing API dictionaries can be translated directly:

```python
request = ProcessMiningRequest.from_mapping(request_body)
payload = engine.execute(cursor, request)
```

Dynamic filters use one source-neutral request contract. Included tuple
members are conjunctive, excluded members must not occur, and multiple status
values are alternatives:

```python
from ocpm_engine import DynamicDfgRequest

request = DynamicDfgRequest.from_mapping(
    {
        "backbone_type": "Order",
        "from_date": "2026-01-01T00:00:00Z",
        "to_date": "2026-02-01T00:00:00Z",
        "filter": {
            "statuses": ["complete"],
            "activities": {
                "include": ["Order:Approved"],
                "exclude": ["Order:Rejected"],
            },
            "event_attributes": {
                "include": [{"key": "region", "value": "west"}],
            },
            "related_object_types": {"include": ["Invoice"]},
            "edges": {
                "include": [
                    {
                        "source": "Order:Created",
                        "target": "Order:Approved",
                        "min_execution_time": 0,
                        "max_execution_time": 86400,
                    }
                ]
            },
        },
    }
)
plan = engine.build_dynamic_dfg(request)
answer = engine.execute_dynamic_dfg(cursor, plan)
```

`build_dynamic_case_ids(request)` exposes the same selection as an ordered ID
projection for correctness checks, pagination seeds, and downstream analysis.

At application startup, verify that the dependency is present:

```python
version = engine.verify_pg_ocpm(cursor)
```

For native event-log algorithms, inspect capabilities and let the planner
select the compact 0.9 path or the exact 0.8 fallback:

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

assert execution.expanded_event_rows == 0  # pg_ocpm 0.9 factorized path
training, test = execution.summaries
```

For a large remote result, use a driver-level server-side cursor so the Python
database driver does not buffer every factorized row during `execute()`. With
psycopg2 this requires an explicit transaction:

```python
connection.autocommit = False
with connection.cursor(name="ocpm_event_batches") as cursor:
    cursor.itersize = 64
    execution = engine.execute_event_log_summary(
        cursor, request, capabilities=capabilities
    )
connection.commit()
```

`pg_ocpm 0.9` bounds PostgreSQL backend memory with a `work_mem`-backed
tuplestore, while the named cursor bounds client-driver buffering. PostgreSQL's
materialized set-returning-function contract still completes the compact
server result before returning its first row, so this path improves peak memory
and transfer shape but does not claim streaming time-to-first-row. Both engine
adapters transparently split requests larger than 256 windows into bounded
extension calls and preserve the original global window order.

Binding-index declarations are observable rather than inferred from a version
number. `inspect_binding_index(cursor)` returns exact coverage and dataset
refresh markers; callers should select a binding-index plan only after its
required object, activity, event, neighbor, or relation declaration is present.

## Integration boundary

Source ingestion belongs outside this package. A loader maps source events,
objects, directly-follows edges, adjacency, and case summaries into the
normalized `ocpm` schema. `ocpm-engine` begins at the read path after
`ocpm.finish_load(...)` succeeds.

Application-only response decoration, authorization, labels, and external
record URLs should remain in the API service. New server-side primitives belong
in `pg_ocpm` only when they are useful across OCPM workloads.

## Project status and licensing

The dependency graph is locked and license-gated; see
[third-party notices](THIRD_PARTY_NOTICES.md). This repository currently grants
no open-source license. The
[prior-art and ICPM roadmap](docs/prior-art-and-icpm-roadmap.md) separates known
prior work from candidate research contributions and required evidence.
