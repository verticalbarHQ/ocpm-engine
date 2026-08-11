# Getting started

This walkthrough goes from an empty environment to a discovered, conformance-
checked object-centric model. For installation options (PyPI community wheel,
certified enterprise wheel, source builds, and the external DuckDB requirement), see the
[installation guide](https://github.com/verticalbarHQ/ocpm-engine/blob/main/INSTALL.md).

## 1. Install

```sh
python3.11 -m venv .venv
. .venv/bin/activate
pip install --only-binary=:all: ocpm-engine==1.1.0
```

Or, for development, clone the repository and run `pip install -e '.[dev]'`.

## 2. Analyze a file, no database required

The standalone facade accepts versioned JSON requests and returns ordinary
Python dictionaries while algorithms run in Rust:

```python
from ocpm_engine import StandaloneEngine

engine = StandaloneEngine.from_ocel2_json("events.json")

# Activity profile for one object type
profile = engine.profile({"object_types": ["Order"]})

# Object-centric DFG discovery
model = engine.discover(
    {
        "view": {"object_types": ["Order"]},
        "algorithm": "object_centric_dfg",
    }
)
```

XES and SQLite loaders follow the same pattern (`from_xes`, `from_sqlite`);
the full constructor and method surface is in the
[standalone module reference](modules/standalone.md).

## 3. Score conformance from compact aggregates

Aggregate rows can be scored directly without event-level data:

```python
from ocpm_engine import TransitionCount, dfg_conformance

rows = [
    TransitionCount("Create", "Approve", "directly_follows", 900, 95),
    TransitionCount("Create", "Reject", "directly_follows", 100, 5),
]
result = dfg_conformance(rows, coverage=0.95)
```

## 4. Connect a provider

With [`pg_ocpm`](https://github.com/verticalbarHQ/pg_ocpm) installed in
PostgreSQL, the engine negotiates the installed capability surface and pushes
selective scans and sufficient-statistic aggregation into the database:

```python
from ocpm_engine import OcpmEngine, EventLogRequest, EventLogWindow

engine = OcpmEngine(dataset_id=42, tenant_id=7)
engine.verify_pg_ocpm(cursor)
capabilities = engine.inspect_pg_ocpm(cursor)

request = EventLogRequest(
    object_type="Order",
    windows=(
        EventLogWindow(training_start, training_end),
        EventLogWindow(test_start, test_end),
    ),
)
execution = engine.execute_event_log_summary(cursor, request, capabilities=capabilities)
```

An existing DuckDB catalog over Parquet snapshots works through
`StandaloneEngine.from_duckdb_parquet(...)`; see the
[README](https://github.com/verticalbarHQ/ocpm-engine#use) for the full
configuration shape and the server-side-cursor pattern for large results.

## Where to go next

- [OCPQ four-way benchmark](ocpq-1.0-four-way-comparison.md) and the other
  benchmark reports for what the engine does to end-to-end latency
- [1.0.0 specification](ocpm-engine-1.0.0-spec.md) for the full engine design
- [DuckDB Parquet provider spec](duckdb-parquet-provider-spec.md) for the
  analytical provider architecture
- [Academic implementation provenance](academic-implementation-provenance.md)
  for the peer-reviewed basis of every algorithm
