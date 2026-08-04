# `ocpm_engine.dynamic_queries`

Compile composable dynamic filters into exact pg_ocpm DFG queries. This is
the compilation layer under
[`OcpmEngine.build_dynamic_dfg`](engine.md#dynamic-filters); use it directly
only when you need the SQL without an engine instance.

## `compile_dynamic_dfg`

```python
compile_dynamic_dfg(request: DynamicDfgRequest, *,
                    dataset_id: int, tenant_id: int,
                    projection: Projection)
    -> tuple[str, dict[str, Any], str]
```

Return SQL, parameters, and the selected dynamic execution strategy for a
[`DynamicDfgRequest`](models.md#dynamic-dfg-requests).

- `projection` — `Projection = Literal["case_ids", "dfg"]`. `"dfg"`
  produces the filtered directly-follows counts; `"case_ids"` produces the
  same selection as an ordered case-ID projection.
- The returned strategy string names the chosen execution path; the SQL
  addresses pg_ocpm serving relations (`ocpm.case_bucket`,
  `ocpm.event_chunk`, `ocpm.edge_bucket`) with named `%(param)s`
  placeholders.

Filter semantics match the request contract: included tuple members are
conjunctive, excluded members must not occur, and multiple statuses are
alternatives.

```python
from ocpm_engine import DynamicDfgRequest
from ocpm_engine.dynamic_queries import compile_dynamic_dfg

request = DynamicDfgRequest.from_mapping(
    {
        "backbone_type": "Order",
        "from_date": "2026-01-01T00:00:00Z",
        "to_date": "2026-02-01T00:00:00Z",
        "filter": {"statuses": ["complete"]},
    }
)
sql, params, strategy = compile_dynamic_dfg(
    request, dataset_id=42, tenant_id=7, projection="dfg"
)
cursor.execute(sql, params)
```
