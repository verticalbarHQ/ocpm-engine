# ocpm-engine

`ocpm-engine` is a Rust-first process-mining companion library for `pg_ocpm`.
PostgreSQL performs selective capsule scans and sufficient-statistic
aggregation; deterministic Rust kernels construct and score models without
transferring event tables into Python.

This package does not install another PostgreSQL extension or create database
objects. PostgreSQL must already have `pg_ocpm` installed and the target dataset
must already be finalized with `ocpm.finish_load(...)`.

Required extension version: `pg_ocpm >= 0.8.0`. See the
[release notes](CHANGELOG.md) for every library version.

## Native analytics

- frequency-covered DFG and complete-variant conformance;
- deterministic next-activity models;
- stable bottleneck ranking;
- explainable DFG, variant, or activity-frequency drift using bounded
  Jensen-Shannon divergence; and
- Python 3.11+ stable-ABI bindings that release the GIL during native work.

The `ocpm-postgres` crate provides an asynchronous adapter for activity profiles
and single-window and multi-window DFG/variant counts. It also retrieves and
decodes generic binding-result capsules for cardinality, required-activity,
eventually-follows, actor, delay, and related-object pair operations. Pair
results remain factorized, expose borrowed pair groups and materialized columns,
and expand only through an exact-size lazy iterator when rows are genuinely
needed.
Multi-window requests retrieve aligned training, test, comparison-period, or
drift statistics in one database request.

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

For the public SAP O2C/P2P release benchmark, including latency, storage,
concurrency, correctness gates, and published context, see
[Public common-process-mining performance](docs/public-common-pm-performance.md).
For the three-way comparison of lightly indexed PostgreSQL with PM4Py,
`pg_ocpm` with PM4Py, and `pg_ocpm` with ocpm-engine, see
[SAP PM4Py three-way performance](docs/sap-pm4py-three-way-performance.md).
For the clean-commit 0.9 Docker comparison across OCPQ, SAP O2C, and SAP P2P,
including all four requested arms, latency, concurrency, storage, memory, and
dynamic-query expectations, see the
[full 0.9 benchmark](docs/ocpm-engine-0.9-full-benchmark.md). The implementation
details and planner boundaries are summarized in
[ocpm-engine 0.9 performance evidence](docs/ocpm-engine-0.9-performance.md).
For the OCPQ data evaluated across OCPQ, vanilla PostgreSQL plus PM4Py,
`pg_ocpm` plus PM4Py, and `pg_ocpm` plus the engine, see the
[four-way 0.9 comparison](docs/ocpq-0.9-four-way-comparison.md).
The clean 0.9 strict OCPQ Q1-Q7 result uses zero warmups, ten same-host measured
runs per query, and exact duplicate-preserving parity for every node. It is
15.108x faster than OCPQ by geometric mean with a 7.473x minimum query
speedup.
For the detailed application read-path design and code references, see
[Application query performance improvements](docs/technical-performance-improvements.md).
For the open-source capability survey, license boundary, research review, and
database-versus-engine placement decisions, see
[Process-mining capability map](docs/process-mining-capability-map.md).

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

First run `make perf-sap-release-bridge-preview`, then run both self-contained
current-versus-vanilla SAP benchmarks with `make perf-public` and validate the
three staged artifacts with `make perf-public-preview-check`. After review and
explicit promotion, validate committed artifacts with
`make perf-public-release-check`. See
[the benchmark guide](benchmarks/README.md) for the exact methodology.

## Use

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
