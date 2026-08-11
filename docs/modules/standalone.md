# `ocpm_engine.standalone`

Source-neutral Python facade over the standalone Rust engine. Public request
and result values are ordinary mappings following the versioned 1.0 JSON
contracts; this module only handles Python-to-JSON conversion, and algorithms
execute in Rust with the GIL released.

## `StandaloneEngine`

Run OCPM operations from files or canonical in-memory data.

### Constructors

```python
StandaloneEngine(canonical_log: Mapping[str, Any])
StandaloneEngine.from_ocel2_json(value: str | bytes | Path)
StandaloneEngine.from_xes(value: str | bytes | Path)
StandaloneEngine.from_sqlite(path: str | Path)
StandaloneEngine.from_duckdb_parquet(source: Mapping[str, Any])
```

- `canonical_log` — a canonical log as a JSON object.
- `from_ocel2_json` / `from_xes` — accept a `Path` to read, or the document
  itself as `str`/`bytes`.
- `from_sqlite` — path to an OCEL 2.0 SQLite file.
- `from_duckdb_parquet` — opens local or S3 Parquet through a
  deployment-supplied DuckDB installation; the `source` object selects the
  existing catalog, location, snapshot, layout, cache mode, and options. The
  engine never creates the catalog implicitly.

```python
from ocpm_engine import StandaloneEngine

engine = StandaloneEngine.from_ocel2_json("events.json")
```

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

### Properties

| Property | Type | Purpose |
|---|---|---|
| `provider_name` | `str` | Name of the active provider |
| `capabilities` | `list[str]` | Capability surface of the active provider |

### Methods

Every request-taking method accepts a versioned JSON object (a plain mapping)
and returns a plain dictionary.

```python
append(batch: Mapping) -> None
```
Atomically validate and append a canonical columnar batch; the source
watermark advances only after the complete batch succeeds.

```python
profile(view: Mapping | None = None) -> dict
```
Activity profile for a view (object types, time bounds).

```python
query(request: Mapping) -> dict
```
Typed filtering and binding queries.

```python
discover(request: Mapping) -> dict
```
DFG, OC-DFG, Alpha, process-tree, Petri-net, OCPN, and declarative discovery.

```python
conformance(request: Mapping) -> dict
enhance(request: Mapping) -> dict
bottlenecks(request: Mapping) -> dict
```
Conformance checking and model enhancement (frequencies, durations,
bottlenecks). `bottlenecks` runs the full provider-neutral tail, synchronization,
waiting-cause, queue, drift, spectrum, and cascade suite described in the
[bottleneck analysis contract](../bottleneck-analysis.md).

```python
fit_gnn_bottlenecks(request: Mapping) -> dict
score_gnn_bottlenecks(request: Mapping, artifact: Mapping) -> dict
gnn_bottlenecks(request: Mapping) -> dict
```
The optional CPU graph module can fit a portable artifact, score one, or fit
and score in one provider scan. It uses the same accelerated transition
projection for local, DuckDB, and PostgreSQL data, with all graph semantics in
the engine. See [graph-aware bottleneck detection](../bottleneck-analysis.md#graph-aware-bottleneck-detection).

```python
fit_prediction(request: Mapping) -> dict
predict(request: Mapping) -> dict
evaluate_prediction(view: Mapping, target: str, *,
                    holdout_fraction: float = 0.2,
                    parameters: Mapping | None = None) -> dict
```
Prediction: fit a model, predict, or run a temporal holdout evaluation for
`target` on the selected view. Core prediction accepts `sequential` and
`tabular` feature encodings. Predictive Graph/GNN tasks use the separate
optional `ocpm_engine.gnn` backend protocol; graph-aware bottleneck detection
uses the built-in optional CPU module above.

```python
execution_summary(request: Mapping) -> dict
```
Exact compact lifecycle, variant, DFG, and activity statistics without
materializing results.

```python
canonical_json(view: Mapping | None = None) -> dict
ocel2_json(view: Mapping | None = None) -> dict
xes(object_type: str, view: Mapping | None = None) -> str
write_sqlite(path: str | Path, view: Mapping | None = None) -> None
write_parquet_snapshot(root: str | Path, version: str,
                       view: Mapping | None = None) -> dict
```
Serialization: export a view as canonical JSON, OCEL 2.0 JSON, or XES (one
object type per XES document); persist to OCEL SQLite; or write a new
immutable canonical Parquet snapshot and `CURRENT` pointer.

```python
explain(view: Mapping, capability: str) -> dict
```
Report the selected provider boundary for a capability without exposing
provider-specific expressions.

### Example

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

## `serialize_model`

```python
serialize_model(artifact: Mapping, format: str = "json") -> str
```

Serialize a 1.0 model artifact as JSON, DOT, PNML, or SVG.
