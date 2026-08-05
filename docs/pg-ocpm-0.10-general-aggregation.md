# pg_ocpm 0.10 general aggregation design

## Outcome

The DFG concurrency deficit was caused by an execution-granularity mismatch,
not by the Rust scoring kernel. The 0.9 engine transferred 126 factorized batch
rows (105,384 bytes), decoded 10,284 events, and then reduced them to ten DFG
edges for every request. The score itself took only about 0.008 ms.

Version 0.10 moves the reusable, exact reduction to `pg_ocpm` and returns the
ten sufficient-statistic rows. The same architecture is available for complete
variant frequencies and existing filtered edge-duration features. These are
public, parameterized APIs rather than branches in the benchmark:

- `ocpm.lifecycle_dfg_window_counts` accepts one or more object types,
  arbitrary aligned lifecycle windows, source and target activity filters, and
  a minimum total frequency.
- `ocpm.lifecycle_variant_window_counts` returns complete activity paths and
  aligned frequencies with object-type, status, variant, included-activity,
  excluded-activity, and minimum-frequency filters.
- `OcpmEngine.execute_edge_features` exposes `ocpm.edge_feature_aggregates`
  with event-time, object-type, activity, edge-type, context, slow-threshold,
  and minimum-frequency filters.
- The Python planner and asynchronous Rust PostgreSQL adapter expose the
  lifecycle DFG path. The Python planner also exposes lifecycle variants and
  edge features. Capability probes select exact older-version fallbacks.

## Generality and resource bounds

No OCPQ, Rust4PM, OCPA, SAP, activity-name, expected-answer, or fixed-window
condition exists in the extension or engine implementation. The new extension
release adds no table, index, materialized DFG, result cache, or background
worker, so serving storage is unchanged.

Each extension call accepts at most 256 windows. Larger requests are chunked
inside one statement snapshot and stitched into their original aligned order.
Per-chunk calls retain every positive row; caller-selected minimum-frequency
thresholds are applied only after all chunks have been aligned, so sparse
frequencies cannot disappear at a chunk boundary.
The PostgreSQL executor owns scans, grouping, and spill behavior through
`work_mem`; the extension retains only bounded native states and compact output
rows. Engine cursors are consumed incrementally and validate window dimensions,
negative counts, duplicate chunks, and malformed paths before returning data.

The optimization applies when an algorithm consumes DFG frequencies, complete
variant frequencies, or aggregate edge features. Algorithms requiring event
attributes, case identities, full timestamp sequences, or other event-level
data continue to use the factorized event stream. Arbitrary dynamic queries are
therefore expected to benefit only when their required sufficient statistic is
covered; the Rust4PM ratio is not a claim about every process-mining query.

## Research basis

The design was developed from database and process-mining principles without
reading or reusing competitor implementation code:

1. Dijkman et al., *Enabling efficient process mining on large data sets:
   realizing an in-database process mining operator*, Distributed and Parallel
   Databases 38 (2020), DOI 10.1007/s10619-019-07270-1. The paper formalizes
   directly-follows as a database operator and motivates applying relational
   filtering and aggregation before extracting a log. That maps to the native
   lifecycle DFG operator and exact filter pushdown.
2. Boncz, Zukowski, and Nes, *MonetDB/X100: Hyper-Pipelining Query Execution*,
   CIDR 2005. Its vectorized execution argument maps to scanning finalized
   PostgreSQL arrays in native loops and aggregating aligned count vectors
   instead of interpreting one Python object per event.
3. Neumann, *Efficiently Compiling Efficient Query Plans for Modern Hardware*,
   PVLDB 4(9), 2011, DOI 10.14778/2002938.2002940. Its focus on data-local
   pipelines and avoiding materialization maps to producing the requested
   sufficient statistic directly rather than constructing an intermediate log.
4. Leis et al., *Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation
   Framework for the Many-Core Age*, SIGMOD 2014,
   DOI 10.1145/2588555.2610507. Its bounded-work scheduling principle maps to
   the 256-window chunks and persistent, independently schedulable PostgreSQL
   connections used by concurrent requests.
5. Olteanu and Zavodny, *Factorised Representations of Query Results: Size
   Bounds and Readability*, ICDT 2012, DOI 10.1145/2274576.2274607. Its compact
   representation principle maps to grouping identical finalized lifecycle
   variants once, carrying a frequency vector, and expanding adjacent edges
   only within that shared path.

These papers are architectural inspiration, not evidence that the specific
implementation or performance result is novel or patentable.

## Validation contract

All comparisons run in Docker from pinned sources. The benchmark uses the same
upstream dataset and four fixed algorithms for both arms, randomizes latency
epochs, reports p50/p95 separately from throughput, and fails closed unless
every serial and concurrency answer has the same canonical SHA-256 digest.

The final full Rust4PM validation passed all four answer cells. DFG throughput
changed from 0.551x-0.681x Rust4PM in 0.9 to 1.537x-1.684x at 1/2/4/8 workers
in 0.10. DFG p50 fell from 1.393 ms to 0.414 ms, and the request changed from
10,284 reconstructed events to ten aggregate rows. The generalized variant and
edge APIs also passed their exact-answer cells and reconstructed zero events
for every workload. Across all four workloads, the 0.10 engine was 1.497x to
3.305x faster than Rust4PM at p50, with a 2.510x geometric mean.

The strict OCPQ rerun also retained exact every-node parity for Q1-Q7. Its
geometric-mean speedup was 16.482x, its minimum query speedup was 8.260x, its
maximum client peak over baseline was 6,299,648 bytes, and the serving schema
occupied 114,974,720 bytes with the request-result cache disabled. These
numbers are a development preview; the published 1.0 benchmark reports
supersede them.

## References

- <https://link.springer.com/article/10.1007/s10619-019-07270-1>
- <https://www.cidrdb.org/cidr2005/papers/P19.pdf>
- <https://www.vldb.org/pvldb/vol4/p539-neumann.pdf>
- <https://www-db.in.tum.de/~leis/papers/morsels.pdf>
- <https://ora.ox.ac.uk/objects/uuid%3A90fcc934-1ee0-40e5-9039-9ee4aee1e824>
