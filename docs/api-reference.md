# Module overview

Every public name is importable from the top-level `ocpm_engine` package
(`from ocpm_engine import StandaloneEngine, dfg_conformance, ...`); the two
SQL-compilation modules are imported by module path. Algorithms run in the
native Rust extension (`ocpm_engine._native`) with the GIL released, and the
Python layer stays a thin typed facade. Current version: 1.1.0.

Each module page carries the full API and function specs with example
usages:

| Module | Purpose |
|---|---|
| [`ocpm_engine.standalone`](modules/standalone.md) | File-based analysis: `StandaloneEngine` over OCEL JSON, XES, SQLite, or DuckDB Parquet, plus `serialize_model` |
| [`ocpm_engine.engine`](modules/engine.md) | PostgreSQL compatibility API: `OcpmEngine` query planning and execution over pg_ocpm |
| [`ocpm_engine.analytics`](modules/analytics.md) | Conformance, prediction, bottleneck, and drift scoring from compact aggregate rows |
| [`ocpm_engine.event_batches`](modules/event_batches.md) | Native summarization of factorized pg_ocpm event batches |
| [`ocpm_engine.bindings`](modules/bindings.md) | Native decoding of compact binding-result capsules |
| [`ocpm_engine.models`](modules/models.md) | Request contracts, query plans, executions, and capability reports |
| [`ocpm_engine.queries`](modules/queries.md) | Parameterized SQL for process-mining response shapes |
| [`ocpm_engine.dynamic_queries`](modules/dynamic_queries.md) | Compilation of composable dynamic filters into exact DFG queries |

A typical standalone flow touches one module; a typical PostgreSQL flow
composes four:

```python
from ocpm_engine import (
    EventLogRequest,     # models: request contract
    EventLogWindow,
    OcpmEngine,          # engine: planning and execution
    dfg_conformance,     # analytics: scoring
    TransitionCount,
)

engine = OcpmEngine(dataset_id=42, tenant_id=7)
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
training, test = execution.summaries   # event_batches: EventLogSummary

test_index = {(e.source, e.target): e.frequency for e in test.dfg}
rows = [
    TransitionCount(e.source, e.target, "directly_follows",
                    e.frequency, test_index.get((e.source, e.target), 0))
    for e in training.dfg
]
score = dfg_conformance(rows, coverage=0.95)
```
