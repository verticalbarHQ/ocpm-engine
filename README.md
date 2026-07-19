# ocpm-engine

`ocpm-engine` is a Rust-first process-mining companion library for `pg_ocpm`.
PostgreSQL performs selective capsule scans and sufficient-statistic
aggregation; deterministic Rust kernels construct and score models without
transferring event tables into Python.

This package does not install another PostgreSQL extension or create database
objects. PostgreSQL must already have `pg_ocpm` installed and the target dataset
must already be finalized with `ocpm.finish_load(...)`.

Required extension version: `pg_ocpm >= 0.5.0`. See the
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
results remain factorized and expand through an exact-size lazy iterator.
Multi-window requests retrieve aligned training, test, comparison-period, or
drift statistics in one database request.

The existing Python query planner remains available for these request shapes:

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
For the seven-query comparison with the results published by OCPQ, see
[Published OCPQ comparison](docs/ocpq-performance.md).
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

Run both self-contained public SAP benchmarks with `make perf-public` and
validate the committed results with `make perf-public-check`. See
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

At application startup, verify that the dependency is present:

```python
version = engine.verify_pg_ocpm(cursor)
```

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
