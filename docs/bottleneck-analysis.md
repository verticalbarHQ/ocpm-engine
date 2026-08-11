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

## Algorithm catalog

`bottlenecks` runs ten deterministic detection and diagnostic methods in one
request. `gnn_bottlenecks` is an eleventh, explicitly probabilistic method.
Each method is data-source neutral: local files, PostgreSQL, and DuckDB supply
the same canonical observations to the same Rust implementation.

| Algorithm | Output | Exactness | High-level approach | Required evidence |
|---|---|---|---|---|
| Impact-weighted tail-latency ranking | `signals` | Deterministic | Groups adjacent-event duration by object type and activity edge; computes median, p90, p95, configurable tail mean, and a robust Tukey or caller-supplied slow threshold. It ranks total excess-duration impact before p95, affected rate, and support, and reports a Wilson interval for the affected rate. | Adjacent events on an object trace |
| Object synchronization delay attribution | `synchronization` | Deterministic | At events shared by multiple object types, compares predecessor timestamps, measures each type's readiness lag from the latest arrival, and counts the lagging object type. | A shared event with a predecessor on at least two object traces |
| Evidence-gated waiting-time decomposition | `waiting_causes` | Deterministic | Pairs lifecycle starts and completions, then assigns each wait once, in order, to temporal batching, overlapping resource contention, FIFO overtaking or prioritization, explicit calendar unavailability, and finally residual delay. | Paired lifecycle `start`/`complete` events and a resource attribute; calendars only for unavailability |
| Queue and resource-pressure detection | `resource_pressure` | Deterministic | Uses a sweep-line queue maximum, merged busy-interval utilization, recent arrival/throughput rates, backlog change, and wait/service distributions. It flags pressure when arrivals exceed throughput or the sampled backlog grows. | Paired lifecycle events and a resource attribute |
| Temporal batch detection | `patterns.batch_rate` | Deterministic | Forms non-overlapping runs whose adjacent start times remain within `batch_tolerance_seconds`, then reports the share of instances in runs meeting `minimum_batch_size`. | Lifecycle and resource evidence |
| FIFO-overtaking detection | `patterns.overtaking_rate` | Deterministic | Orders executions by start time and uses inversion counting over readiness order to measure how often later-ready work starts ahead of earlier-ready work. | Lifecycle and resource evidence |
| Interarrival burstiness detection | `patterns.burstiness` | Deterministic | Computes the bounded burstiness index `(standard_deviation - mean) / (standard_deviation + mean)` over consecutive readiness intervals. | Lifecycle and resource evidence |
| Localized bottleneck-drift detection | `changes` | Deterministic statistical test | Compares baseline and current duration distributions per object-centric edge with a two-sample Kolmogorov-Smirnov statistic, then applies Benjamini-Hochberg correction across tested edges and reports median, p95, and excess-impact deltas. | `comparison_view` plus current `view` |
| Recursive blocking-cascade detection | `cascades` | Deterministic | Selects the greatest overlapping blocker for each waiting execution, follows the resource-blocking chain, and aggregates repeated activity/resource paths with cycle and depth guards. | Overlapping service and wait intervals on a resource |
| Temporal probability-raising hypotheses | `hypotheses` | Deterministic statistical test | Evaluates caller-declared cause/effect windows as exposed and unexposed contingency tables; reports risk difference, risk ratio, chi-square significance, and Benjamini-Hochberg q-values. A supported result remains observational and is not causal proof. | Caller-declared hypotheses and enough exposed and unexposed observations |
| Bounded graph-aware risk detector | separate `gnn_bottlenecks` result | Probabilistic | Builds a transition graph linked by object adjacency and shared events, applies two learned GraphSAGE-style mean-aggregation layers, and reports risk using leakage-safe hashed/context features and temporal holdout diagnostics. | Canonical transitions; bounded by `maximum_nodes` and `maximum_neighbors` |

The methods draw on the peer-reviewed work mapped in
[Academic implementation provenance](academic-implementation-provenance.md):
object-centric performance and synchronization, waiting-cause decomposition,
queue mining, performance-spectrum and batch analysis, explainable drift,
temporal causal-hypothesis testing, recursive blocking analysis, object-centric
event graphs, and inductive neighborhood aggregation. The implementation is
independently authored from those papers rather than from another
process-mining library.

## Output and evidence behavior

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
