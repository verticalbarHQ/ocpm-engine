# Bottleneck analysis

`Engine::bottlenecks` and `StandaloneEngine.bottlenecks` are the default exact
entry points for bottleneck analysis. The request and result contract is the
same for canonical in-memory logs, local files, DuckDB Parquet, and PostgreSQL.
The optional `gnn_bottlenecks` entry point adds explicitly probabilistic,
graph-context risk signals without changing the exact result.

Providers do not implement analytical policy. They may accelerate an exact
projection of adjacent object-level transitions, including lifecycle and event
attributes. `ocpm-engine` applies all thresholds, attribution, statistical
tests, multiple-testing correction, rankings, and warnings. If a provider
cannot preserve a requested predicate, the operation uses the exact canonical
fallback.

## Included analyses

| Result field | Method | Required evidence |
|---|---|---|
| `signals` | Median, p90, p95, configurable tail mean, Tukey or explicit slow threshold, Wilson interval, and total excess-duration impact | Adjacent events on an object trace |
| `synchronization` | Readiness spread and lagging-object attribution at events shared by multiple object types | A shared event with a predecessor on at least two object traces |
| `waiting_causes` | Non-overlapping batching, contention, prioritization, explicit unavailability, and residual attribution | Paired lifecycle `start`/`complete` events and a resource attribute |
| `resource_pressure` | Recent arrival/throughput rate, interval-union utilization, queue depth, wait/service time, and queue-growth flag | Paired lifecycle events and a resource attribute |
| `changes` | Edge-localized two-sample KS statistic, latency/impact deltas, and Benjamini-Hochberg q-values | `comparison_view` plus current `view` |
| `patterns` | Batch rate, FIFO-overtaking rate, and interarrival burstiness | Lifecycle and resource evidence |
| `cascades` | Recursive resource-blocking chains with cycle/depth guards | Overlapping service and wait intervals on a resource |
| `hypotheses` | Temporal precedence, probability raising, risk difference/ratio, and corrected significance | Caller-declared hypotheses |

The result is observational. A supported temporal hypothesis is not presented
as proof of causality. Resource unavailability is never inferred from gaps: it
is attributed only when the caller supplies explicit availability intervals.
Missing lifecycle or resource evidence produces warnings and empty dependent
sections while transition-based analyses remain available.

## Performance and concurrency

Let `T` be projected transitions and `L` paired lifecycle instances. Exact
quantiles retain and sort each edge distribution, giving `O(T log T)` time and
`O(T)` worst-case retained memory. Synchronization grouping is `O(T log T)`.
Queue timelines, FIFO inversions, prioritization lookups, and temporal
hypothesis ranges use sorted indexes, interval prefix sums, and Fenwick trees,
giving `O(L log L)` preprocessing plus output work. Provider projections avoid
hydrating unrelated canonical entities, and DuckDB emits one object execution
at a time before the compact observations enter the kernel.

Blocking-cascade selection is exact and partitioned by resource. Its worst case
is `O(sum(L_resource²))` when many service intervals overlap on the same
resource; typical single-capacity resources prune this naturally. This bound
is reported explicitly and is a target for a future exact interval index.

Every invocation owns its analytical state. There is no global model cache or
kernel lock, so concurrent calls depend only on provider concurrency limits.
DuckDB uses its configured independent-connection pool; PostgreSQL uses the
caller's connection scheduling.

## Python example

```python
result = engine.bottlenecks(
    {
        "semantic_version": "1.0",
        "view": {"object_types": ["Order"]},
        "minimum_support": 20,
        "tail_quantile": 0.95,
        "resource_attribute": "org:resource",
    }
)

for signal in result["signals"][:10]:
    print(
        signal["source_activity"], signal["target_activity"], signal["impact_seconds"]
    )
```

The default ordering uses total excess-duration impact, then p95, affected
rate, and support. This prevents one extreme observation from automatically
outranking a broadly harmful delay.

## Graph-aware bottleneck detection

The optional `ocpm-gnn` module includes a deterministic CPU implementation of
a two-layer GraphSAGE-style mean-aggregation classifier. Each canonical
transition observation is a node. Bounded links connect adjacent transitions
on an object and transitions sharing an event, preserving object and
synchronization context without moving graph semantics into PostgreSQL or
DuckDB.

The model uses cyclical source-time features, lifecycle presence, graph degree,
and signed feature hashes for object type, activities, transition identity, and
resource context. It deliberately excludes the transition's target duration
from input features. Duration is used only for temporally partitioned training
labels and observed excess-duration impact, preventing trivial target leakage.
Thresholds come from the training partition only: an explicit threshold when
provided, otherwise the configured edge-local quantile with a global fallback.

```python
result = engine.gnn_bottlenecks(
    {
        "semantic_version": "1.0",
        "view": {"object_types": ["Order"]},
        "leading_object_type": "Order",
        "minimum_support": 20,
        "validation_fraction": 0.2,
        "maximum_nodes": 100_000,
        "maximum_neighbors": 32,
    }
)
```

`fit_gnn_bottlenecks` returns a content-hashed portable model artifact;
`score_gnn_bottlenecks` applies it to another compatible view. Model fitting is
deterministic for a fixed request and seed. Temporal holdout loss, accuracy,
and AUC (when both classes exist) are reported. The network is probabilistic,
so `diagnostics.exact` is `false`, and its associative risk score is never
presented as a causal effect.

Runtime is `O(Epochs * (T * F * H + T * H² + A * H))`, where `F` is the
feature width, `H` the hidden width, and `A <= T * maximum_neighbors` the
bounded message-passing arcs. Working memory is `O(T * (F + H +
maximum_neighbors))`. `maximum_nodes` and `maximum_neighbors` fail closed
before unbounded graph growth. The implementation adds no tensor runtime or
database dependency.

Other predictive GNN tasks remain behind the `GnnBackend` protocol. The engine
does not silently reinterpret `feature_encoding="graph"` as a tabular model.

## Academic basis

The clean-room implementation is derived from the peer-reviewed definitions
listed in [Academic implementation provenance](academic-implementation-provenance.md),
including object-centric performance, waiting-cause decomposition, queue
mining, performance spectra, batch detection, explainable drift, temporal
causal hypotheses, and recursive blocking analysis. No third-party
process-mining implementation source or fixture is used.
