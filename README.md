# ocpm-engine

`ocpm-engine` is Vertical Bar's application query layer for
[`pg_ocpm`](https://github.com/verticalbarHQ/pg_ocpm). It translates the process
mining requests used by the API into parameterized SQL over the normalized
`ocpm` schema.

This package does not install another PostgreSQL extension or create database
objects. PostgreSQL must already have `pg_ocpm` installed and the target dataset
must already be finalized with `ocpm.finish_load(...)`.

Required extension version: `pg_ocpm >= 0.2.0`.

## Supported request shapes

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

## Install

```sh
pip install git+https://github.com/verticalbarHQ/ocpm-engine.git
```

For local development:

```sh
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
```

## Use

The package has no database-driver dependency. It works with a psycopg2-style
cursor and returns parameterized SQL separately from its values.

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

## Ownership

Copyright © 2026 [Vertical Bar](https://vertical.bar). All rights reserved.
