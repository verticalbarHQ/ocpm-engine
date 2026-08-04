# `ocpm_engine.queries`

Parameterized SQL for process-mining response shapes. The module holds the
static SQL used by [`OcpmEngine`](engine.md) — case-window and case-bucket
variant queries, timelines, throughput, edge info, and process maps — all
addressing pg_ocpm serving relations with named `%(param)s` placeholders and
values kept separate.

## `process_map_sql`

```python
process_map_sql(*, filtered_network: bool, transitive_closure: bool) -> str
```

Return the parameterized process-map SQL for the requested shape:

- `filtered_network` — apply the request's `NetworkFilter` (activities,
  edges, execution-time range) inside the query.
- `transitive_closure` — include indirect (transitively closed) edges
  rather than only directly-follows pairs.

The returned SQL expects the standard parameter dictionary produced by
`OcpmEngine.build(...)`; prefer building through
[`OcpmEngine`](engine.md#generic-requests), which selects the right shape
and strategy for the request.
