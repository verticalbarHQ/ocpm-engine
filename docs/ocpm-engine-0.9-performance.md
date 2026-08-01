# ocpm-engine 0.9 performance evidence

`ocpm-engine 0.9.0` adds a general factorized-event read path for
`pg_ocpm 0.9.0`. PostgreSQL returns compact activity-path bucket rows with packed
vectors for multiple cases, Rust validates and decodes those rows, and
the Python adapter feeds a persistent native builder in bounded 64-row cursor
chunks. Incremental native builders compute DFGs,
variants, next-activity inputs, and bottleneck statistics without creating
Python event rows or dataframes. The adapter discovers this capability from the
installed SQL surface and retains an exact row-stream fallback for
`pg_ocpm 0.8.x`.

The implementation contains no OCPQ query identifiers, dataset names, or
fixed-answer caches. Aggregate-native algorithms still use sufficient
statistics when those are smaller than event batches. The new event-summary
API and its factorized benchmark arm use event batches. Existing dynamic
filtered queries keep their exact row/bucket plans and now preselect eligible
cases before event or lifecycle expansion when edge predicates are present.

The PostgreSQL export reads source buckets through a bounded SPI portal and
stores compact output in a `work_mem`-backed tuplestore. The engine consumes it
through a named, 64-row server-side cursor in the benchmark and documents the
same production integration pattern. This bounds backend and client memory,
but PostgreSQL materialize mode still means the compact server result is
completed before the first row is returned; 0.9 does not claim streaming
time-to-first-row.

## SAP O2C and P2P three-way preview

The unchanged SAP IDES O2C and P2P OCEL 2.0 datasets were run through three
Docker-isolated paths:

1. lightly indexed PostgreSQL plus the pinned PM4Py environment;
2. `pg_ocpm 0.9.0` plus the same PM4Py environment; and
3. `pg_ocpm 0.9.0` plus `ocpm-engine 0.9.0` using factorized event batches.

All eight workload comparisons passed exact three-way semantic checks. The
protocol used 10 warmups, three randomized serial epochs of 30 measured rounds,
three concurrency epochs at 1, 2, 4, and 8 workers, and fresh-process memory
probes.

### Latency

| Dataset / workload | Vanilla PG + PM4Py p50 | pg_ocpm + PM4Py p50 | pg_ocpm + engine p50 | Engine vs vanilla | Engine vs pg_ocpm + PM4Py |
|---|---:|---:|---:|---:|---:|
| O2C DFG conformance | 222.42 ms | 162.97 ms | 15.52 ms | 14.33x | 10.50x |
| O2C variant conformance | 205.05 ms | 144.49 ms | 19.77 ms | 10.37x | 7.31x |
| O2C next activity | 222.89 ms | 162.31 ms | 15.66 ms | 14.23x | 10.36x |
| O2C bottleneck ranking | 306.40 ms | 233.12 ms | 23.92 ms | 12.81x | 9.74x |
| P2P DFG conformance | 194.46 ms | 152.01 ms | 3.58 ms | 54.30x | 42.45x |
| P2P variant conformance | 185.34 ms | 143.33 ms | 3.56 ms | 52.03x | 40.24x |
| P2P next activity | 193.40 ms | 151.53 ms | 3.52 ms | 54.97x | 43.08x |
| P2P bottleneck ranking | 199.77 ms | 162.28 ms | 3.39 ms | 58.91x | 47.86x |

Across all eight workloads, the geometric-mean speedup is **26.56x** versus
vanilla PostgreSQL plus PM4Py and **20.16x** versus `pg_ocpm` plus PM4Py. The
dataset-level engine-versus-vanilla geometric means are **12.83x** for O2C and
**55.00x** for P2P.

Every factorized engine sample reported zero expanded event rows. The two-window
O2C workloads decoded 822 factorized database rows containing 47,899 logical events in
432,704 payload bytes; the corresponding P2P workloads decoded 58 factorized
database rows with
42,015 logical events in 709,752 bytes. These counters make accidental fallback
to row materialization observable.

### Concurrency

DFG throughput scaled while the engine avoided PM4Py/Pandas event
materialization and performed aggregation in native Rust. The benchmark driver
still uses Python worker processes to issue concurrent requests for all three
paths.

| Dataset / workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + engine |
|---|---:|---:|---:|
| O2C / 1 | 4.66 req/s | 6.42 req/s | 69.97 req/s |
| O2C / 2 | 7.93 req/s | 11.20 req/s | 131.51 req/s |
| O2C / 4 | 11.58 req/s | 16.43 req/s | 182.15 req/s |
| O2C / 8 | 16.23 req/s | 24.87 req/s | 271.83 req/s |
| P2P / 1 | 5.19 req/s | 6.70 req/s | 310.34 req/s |
| P2P / 2 | 9.20 req/s | 12.29 req/s | 571.52 req/s |
| P2P / 4 | 13.70 req/s | 17.93 req/s | 827.58 req/s |
| P2P / 8 | 19.93 req/s | 26.87 req/s | 1,129.11 req/s |

At eight workers, engine p95 latency was 35.52 ms on O2C and 8.07 ms on P2P.

### Memory and storage

Fresh-process peak RSS was 48.7–53.1 MiB for the O2C engine workloads and
40.9–41.4 MiB for P2P. The PM4Py paths used 186.0–202.4 MiB on O2C and
181.1–184.9 MiB on P2P. Incremental memory above the native client baseline was
12.0–16.4 MiB on O2C and 4.2–4.7 MiB on P2P.

The database serving representation is shared by both `pg_ocpm` client paths:

| Footprint | Size |
|---|---:|
| Vanilla relational OCEL | 125.3 MiB |
| pg_ocpm serving schema | 102.3 MiB |
| PM4Py dependency closure | 464.6 MiB |
| ocpm-engine dependency closure | 0.9 MiB |

The `pg_ocpm` schema is 18.3% smaller than the benchmark's lightly indexed
relational representation. The dependency sizes are client installation
footprints and are not added to PostgreSQL storage.

## OCPQ Q1-Q7 strict preview

The OCPQ latency/correctness diagnostic exercises factorized binding capsules
rather than the new event-batch API, so it is an independent no-regression
guard. A corrected OCPQ
0.6.7 reference and the 0.9 candidate were run on the same Docker daemon from
the same harness revision and dirty-tree state. Each query used a fresh Docker
container, zero warmups, and ten direct all-node measurements. Every
duplicate-preserving node manifest, situation count, violation count, and
canonical hash matched exactly.

| Query | Corrected OCPQ mean | pg_ocpm 0.9 + engine 0.9 mean | Speedup | Exact nodes |
|---|---:|---:|---:|---:|
| Q1 | 26.33 ms | 1.63 ms | 16.19x | yes |
| Q2 | 43.12 ms | 2.92 ms | 14.76x | yes |
| Q3 | 19.22 ms | 1.91 ms | 10.07x | yes |
| Q4 | 44.41 ms | 2.43 ms | 18.30x | yes |
| Q5 | 50.01 ms | 4.61 ms | 10.85x | yes |
| Q6 | 82.95 ms | 1.95 ms | 42.58x | yes |
| Q7 | 66.54 ms | 1.88 ms | 35.42x | yes |

The geometric-mean speedup is **18.42x**. Fresh-process peak memory above the
native-client baseline ranged from 0.79 MiB to 6.03 MiB. Median throughput was
472.00 requests/s at one client, 1,457.53 at four, 2,427.76 at eight, and
4,448.28 at sixteen; median p95 latency was 4.52, 5.41, 6.52, and 6.97 ms,
respectively. Throughput CV stayed below 0.7%, and exact pre/post result checks
passed at every level. The three-worker asynchronous client runtime leaves CPU
headroom for the co-located PostgreSQL server; the worker count is recorded in
the artifact and remains configurable rather than being part of query logic. The
PostgreSQL serving schema occupied 109.65 MiB, including 5.87 MiB of binding
summaries.

These values are local release
preview evidence, not promoted publication results: the candidate working tree
was dirty and the locally built `pg_ocpm` image does not carry complete source
provenance. The committed 0.8 publication result remains the externally
reproducible OCPQ claim. The same data's four-way comparison against vanilla
PostgreSQL plus PM4Py and `pg_ocpm` plus PM4Py is documented in
[OCPQ data: four-way 0.9 comparison](ocpq-0.9-four-way-comparison.md).

## What to expect from dynamic queries

The fixed OCPQ speedup should not be projected directly onto arbitrary dynamic
queries. Dynamic latency depends on predicate selectivity, relationship fanout,
requested output size, cache state, and whether a complete index covers the
requested operation. Version 0.9 improves the general path in three ways:

- selective case filters are applied before expensive event and lifecycle
  expansion when edge predicates are also present;
- exact binding-index coverage is exposed per object, activity, event,
  neighbor, and relation lookup so callers can fail closed before choosing an
  indexed path; and
- capability inspection is separate from execution telemetry, which reports
  the chosen strategy, database rows, logical events, expanded rows, and packed
  payload bytes.

The expected result is a broad reduction in transfer, allocation, and Python
work for event-oriented analytics, plus better plans for selective dynamic
queries. Small result-set lookups may see OCPQ-like improvements; wide,
low-selectivity queries that must return many rows remain bounded by necessary
database and transfer work. No benchmark-specific query plan or dataset-specific
threshold is used.

## Reproduce

The Docker commands and artifact checker are documented in the
[benchmark guide](../benchmarks/README.md). The ignored local evidence artifacts
used for this release preview are:

- `.benchmarks/sap-pm4py-three-way-factorized-0.9.0.json`;
- `.benchmarks/ocpq-reproduced-strict-all-node-0.9-preview.json`; and
- `.benchmarks/ocpq-bpic2017-pg_ocpm-0.9.0-ocpm-engine-0.9.0-strict-preview.json`.

The strict checker independently recomputes every raw latency, memory, storage,
concurrency, correctness, and speedup claim. This preview passes all those gates
and reports only the provenance blockers described above:

```sh
python3 benchmarks/check_ocpq_result.py --preview \
  --reference .benchmarks/ocpq-reproduced-strict-all-node-0.9-preview.json \
  --candidate .benchmarks/ocpq-bpic2017-pg_ocpm-0.9.0-ocpm-engine-0.9.0-strict-preview.json \
  --expected-reference-sha256 ae317e94d0cb8a7cb74786aa71a7f1792eb16c245e577729995640056c4beb9a \
  --expected-candidate-sha256 ffcaa60383bb52945ea2f0053add72b1ca20264bbcf0dfcc05ed3de5c67c45ba \
  --expected-pg-ocpm-version 0.9.0 \
  --expected-ocpm-engine-version 0.9.0
```
