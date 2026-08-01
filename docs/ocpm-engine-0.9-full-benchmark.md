# pg_ocpm and ocpm-engine 0.9 full benchmark

## Result

The clean 0.9 Docker runs establish two separate outcomes:

- with PM4Py 2.7.23.3 fixed, `pg_ocpm 0.9.0` improves the geometric-mean
  end-to-end SAP O2C/P2P latency by **1.340x** over vanilla PostgreSQL; and
- `pg_ocpm 0.9.0` plus `ocpm-engine 0.9.0` improves it by **25.181x** over
  vanilla PostgreSQL plus PM4Py and **18.793x** over `pg_ocpm` plus PM4Py.

On the complete OCPQ Q1-Q7 fixture, the strict native path is **15.108x**
faster than OCPQ by geometric mean, with a **7.473x** minimum query speedup.
All 13 nodes and all 380,083 duplicate-preserving situations match exactly.

These gains do not come from saved query results. Every timed request executes
the database path, result caches are disabled, and exact canonical answers are
checked throughout latency and concurrency measurement.

## Benchmark matrix

| Fixed dataset | OCPQ | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|---:|
| BPIC 2017 OCPQ Q1-Q7 | yes | descriptive evaluator | descriptive evaluator | strict native evaluator |
| SAP IDES O2C | N/A | yes | yes | yes |
| SAP IDES P2P | N/A | yes | yes | yes |

OCPQ Q1-Q7 cannot be transferred unchanged to the fixed SAP fixtures. O2C and
P2P contain zero object-to-object relations, while Q4, Q5, and Q7 require
them. Fabricating links or dropping queries would change the data or workload,
so the report keeps the complete OCPQ suite on its BPIC-derived fixture and
uses four fixed process-mining workloads on SAP.

## OCPQ Q1-Q7 latency

All values are means from the upstream zero-warmup, ten-run protocol, with one
fresh Docker process per query. The two PM4Py columns use one fixed explicit
Pandas evaluator over PM4Py's resident OCEL because PM4Py does not implement
OCPQ evaluation trees. They are a verified descriptive comparison, not a
claim that the evaluator is native PM4Py functionality.

| Query | OCPQ | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + engine | Engine vs OCPQ |
|---|---:|---:|---:|---:|---:|
| Q1 | 19.437 ms | 31.622 ms | 25.179 ms | 1.339 ms | 14.521x |
| Q2 | 35.504 ms | 62.826 ms | 47.400 ms | 2.747 ms | 12.926x |
| Q3 | 14.001 ms | 29.649 ms | 24.540 ms | 1.874 ms | 7.473x |
| Q4 | 35.269 ms | 84.317 ms | 76.914 ms | 2.325 ms | 15.171x |
| Q5 | 43.461 ms | 145.028 ms | 141.304 ms | 5.302 ms | 8.197x |
| Q6 | 68.128 ms | 23.563 ms | 16.742 ms | 1.926 ms | 35.372x |
| Q7 | 54.708 ms | 95.556 ms | 77.233 ms | 1.878 ms | 29.125x |
| **Geometric mean** | **34.242 ms** | **55.499 ms** | **45.626 ms** | **2.266 ms** | **15.108x** |

The engine geometric-mean speedups are **24.487x** versus vanilla PostgreSQL
plus PM4Py and **20.131x** versus `pg_ocpm` plus PM4Py. The fixed
`pg_ocpm` export improves the PM4Py evaluator by **1.216x** on this fixture.

The strict native artifact independently passes latency, memory, storage,
provenance, and every-node correctness gates. The four-way verifier returns
`verified_descriptive_preview`: the PM4Py artifacts omit a shared host
identifier and use a different evaluator boundary from OCPQ. The table is
therefore useful comparative evidence but is not promoted to the strict
publication claim.

## SAP O2C and P2P latency

The SAP timer includes database extraction, client materialization, model
construction, and scoring. Every arm used 10 warmups followed by three
randomized epochs of 30 samples, for 90 measured samples per workload and arm.

| Dataset / workload | Vanilla PG + PM4Py p50 | pg_ocpm + PM4Py p50 | pg_ocpm + engine p50 | Engine vs vanilla |
|---|---:|---:|---:|---:|
| O2C DFG conformance | 156.304 ms | 110.575 ms | 10.741 ms | 14.552x |
| O2C variant conformance | 143.434 ms | 99.224 ms | 13.516 ms | 10.612x |
| O2C next activity | 153.818 ms | 109.925 ms | 10.797 ms | 14.246x |
| O2C bottleneck ranking | 213.977 ms | 161.960 ms | 16.311 ms | 13.119x |
| P2P DFG conformance | 130.476 ms | 101.023 ms | 2.691 ms | 48.486x |
| P2P variant conformance | 123.439 ms | 93.859 ms | 2.682 ms | 46.025x |
| P2P next activity | 129.475 ms | 99.443 ms | 2.677 ms | 48.366x |
| P2P bottleneck ranking | 132.478 ms | 106.532 ms | 2.553 ms | 51.891x |

The dataset-level geometric means are:

| Dataset | pg_ocpm + PM4Py vs vanilla | Engine vs vanilla | Engine vs pg_ocpm + PM4Py |
|---|---:|---:|---:|
| SAP O2C | 1.394x | 13.034x | 9.349x |
| SAP P2P | 1.288x | 48.648x | 37.776x |
| **Combined** | **1.340x** | **25.181x** | **18.793x** |

Every accepted serial sample reproduced the same canonical answer. Factorized
engine samples reported zero expanded event rows, making accidental fallback
to row materialization observable.

## Concurrency

SAP concurrency used persistent database connections in isolated client
processes, three epochs at 1, 2, 4, and 8 workers, at least five seconds per
epoch, and an exact answer check for every request. The table reports median
epoch QPS and p95 latency for DFG conformance.

| Dataset / workers | Vanilla QPS / p95 | pg_ocpm + PM4Py QPS / p95 | pg_ocpm + engine QPS / p95 |
|---|---:|---:|---:|
| O2C / 1 | 6.584 / 157.811 ms | 9.173 / 114.231 ms | 104.200 / 10.304 ms |
| O2C / 2 | 11.294 / 185.080 ms | 16.543 / 129.046 ms | 194.538 / 11.187 ms |
| O2C / 4 | 19.131 / 219.149 ms | 27.908 / 151.285 ms | 339.567 / 12.708 ms |
| O2C / 8 | 26.745 / 308.134 ms | 45.269 / 183.025 ms | 528.938 / 16.543 ms |
| P2P / 1 | 7.736 / 134.122 ms | 10.151 / 102.382 ms | 441.078 / 2.489 ms |
| P2P / 2 | 13.608 / 153.993 ms | 18.606 / 112.462 ms | 844.071 / 2.588 ms |
| P2P / 4 | 23.535 / 178.594 ms | 31.919 / 130.623 ms | 1,473.987 / 3.025 ms |
| P2P / 8 | 36.013 / 237.364 ms | 50.592 / 162.041 ms | 2,288.608 / 3.833 ms |

The strict OCPQ engine path was also tested at 1, 4, 8, and 16 clients. Median
throughput rose from 470.975 to 4,405.857 requests/s while p95 rose from 4.521
to 7.039 ms. Every client passed full Q1-Q7 canonical checks before and after
each timed epoch.

## Memory and storage

Fresh-process SAP probes include imports and one complete request. Maximum
incremental peak RSS across the four workloads was:

| Dataset | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + engine |
|---|---:|---:|---:|
| SAP O2C | 165.516 MiB | 165.637 MiB | 9.113 MiB |
| SAP P2P | 148.094 MiB | 148.094 MiB | 3.430 MiB |

PM4Py expands either PostgreSQL source into the same Pandas-heavy object graph,
so the extension alone cannot remove that client-memory floor. The native
builder operates on compact factorized batches and avoids Python event rows.

| Footprint | Size |
|---|---:|
| SAP vanilla relational OCEL | 125.266 MiB |
| SAP shared pg_ocpm serving schema | 102.320 MiB |
| OCPQ vanilla benchmark relations | 474.695 MiB |
| OCPQ pg_ocpm serving schema | 109.680 MiB |
| PM4Py dependency closure | 464.550 MiB |
| ocpm-engine dependency closure | 0.784 MiB |

On SAP, `pg_ocpm` is 18.3% smaller than the lightly indexed relational schema.
On the OCPQ fixture it is 76.9% smaller. Version 0.9 reuses existing serving
relations and adds no table, materialized view, index, or request-result cache.
The strict OCPQ candidate used 109.680 MiB of serving storage, had zero result
cache rows, and peaked at 6.078 MiB above native-client baseline and 3.994 MiB
of owned result-tree allocation.

## Why the native engine is fast

The improvement is architectural rather than a query-ID shortcut:

- PostgreSQL uses indexed summaries and transfers factorized binding capsules
  for OCPQ-shaped queries, or pruned activity-path batches instead of expanded
  event tuples for event-log consumers;
- Rust validates and decodes borrowed column groups, then aggregates without
  constructing Python rows or Pandas dataframes;
- persistent prepared statements, database connections, and native builders
  remove repeated setup; and
- exact binding coverage lets the planner choose indexed, summary, or
  factorized paths without hard-coding a benchmark name.

No implementation branch contains OCPQ query identifiers, SAP dataset names,
expected counts, expected hashes, or cached answers. The same factorized API
is exercised by four different SAP workloads and by arbitrary time-window
requests. Requests with more than 256 windows are transparently sliced within
one SQL statement so they retain one PostgreSQL snapshot.

Dynamic-query speedups will vary with predicate selectivity, relation fanout,
output cardinality, and index coverage. Selective and aggregation-heavy
queries should benefit most. Wide queries that must return large owned result
sets remain bounded by necessary PostgreSQL, transfer, and allocation work.

One executor limitation remains explicit: `pg_ocpm` uses a `work_mem`-backed
materialized SRF, which bounds backend memory and can spill, but PostgreSQL
finishes the compact result before returning the first row. Version 0.9 does
not claim streaming time-to-first-row.

## Fixed versions and evidence

- `pg_ocpm`: `7b201978d00ff4014ffc536ed7f391493b707a76`
- `ocpm-engine`: `45de4da07a88e9c722a5ac9dcd5e154aa38bae8f`
- PM4Py: 2.7.23.3
- PostgreSQL: 16.14
- OCPQ: 0.6.7 at `80457e561edd7bb9e142d959dd7e0f96e6b03f2f`
- OCPQ evaluation sources: `846dd4eb9f8600ae42355968453a9412ea4759c2`

Artifact SHA-256 values:

- SAP O2C/P2P: `da0d79d83cde5a7966b5bd2ec2335658d0366fa2af813ef868d3a09e04eda850`
- strict OCPQ reference: `39894339697421834a652620406152c87a92b08831c3a68dc3f30acd6dc77964`
- strict pg_ocpm + engine: `317af340bff890551c1dfcafe0e0fc8777ade938865173362d56d20073222f1e`
- vanilla PostgreSQL + PM4Py: `12ae7b81f6b67d075b7d491f29fde4e2868f87c8c31828400ee2489c8e83e036`
- pg_ocpm + PM4Py: `a460e2145d8966def16fa5672e4e9f186572d83226ec1a39ea5b68b382be721a`
- fixed PM4Py evaluator: `a431ed3ac827fb86011ddda60c33aeaa9d304a7836800168750cb923ef925486`

The strict OCPQ reference, strict native candidate, and SAP three-arm artifact
are published under `docs/results/`. The two descriptive PM4Py OCPQ artifacts
remain ignored staging evidence because they are not strict publication
artifacts. Reproduction and fail-closed verification commands are documented
in [`benchmarks/README.md`](../benchmarks/README.md).
