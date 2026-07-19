# Published OCPQ comparison

This report compares `pg_ocpm 0.5.0` plus `ocpm-engine 0.4.0` with the ten raw
OCPQ timings published by the OCPQ authors. There is no vanilla PostgreSQL arm
in this comparison.

The workload is Q1-Q7 from the public `aarkue/ocpq-eval` repository at commit
`846dd4eb9f8600ae42355968453a9412ea4759c2`, using its BPIC 2017-derived OCEL
2.0 dataset. OCPQ is version 0.6.7. Every candidate result passed a complete
row expansion and exact row-count, violation/value, and canonical SHA-256 gate.

## Latency

| Query | Published OCPQ | pg_ocpm + ocpm-engine | Ratio vs published OCPQ | Correct |
|---|---:|---:|---:|---:|
| Q1: application submitted exactly once | 37.427 ms | 0.427 ms | 87.591x | yes |
| Q2: offer returned after creation | 57.868 ms | 0.923 ms | 62.700x | yes |
| Q3: returned event has exactly one offer | 22.864 ms | 0.729 ms | 31.368x | yes |
| Q4: associated offer accepted after application | 67.105 ms | 0.750 ms | 89.530x | yes |
| Q5: accepting resource creates every offer | 68.541 ms | 1.950 ms | 35.143x | yes |
| Q6: maximum creation-to-acceptance delay | 84.442 ms | 0.380 ms | 222.204x | yes |
| Q7: offer pairs and creation events | 79.853 ms | 1.241 ms | 64.341x | yes |

Geometric-mean latency is **55.069 ms** for published OCPQ and **0.794 ms** for
the candidate, a directional **69.394x ratio**. The minimum per-query ratio is
**31.368x**, so all seven queries clear the 10x release gate.

Published OCPQ and candidate timings were recorded on different hosts. These
ratios are therefore cross-environment comparisons, not controlled same-host
speedup claims. The pinned local OCPQ Docker reproduction is included in the
benchmark package to validate the source, queries, and timing extraction.

## Timing boundary and correctness

The OCPQ source timings include `tree.evaluate` and construction of its
in-memory `EvaluationResultWithCount` structures. Candidate timing includes:

1. PostgreSQL execution;
2. fetching one binary result capsule over the database connection; and
3. native Rust decoding into an in-memory binding structure.

Complete SQL expansion is intentionally outside the timed region. It is a
correctness gate, not the serving representation. The Rust structure exposes
an exact-size iterator, reuses dictionary labels, and keeps related-object pair
groups factorized until a consumer iterates them. This preserves the output
without forcing eager allocation of every Cartesian pair.

## Concurrency and backend memory

The mixed workload cycles through Q1-Q7 and includes native decoding. Each
worker sends 1,000 requests over a persistent connection.

| Clients | Throughput | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|
| 1 | 1,028 qps | 0.838 ms | 1.945 ms | 2.038 ms |
| 4 | 3,581 qps | 0.937 ms | 2.204 ms | 2.536 ms |
| 8 | 6,133 qps | 1.138 ms | 2.569 ms | 3.141 ms |
| 16 | 4,804 qps | 2.930 ms | 7.004 ms | 9.513 ms |

The best measured throughput is at eight clients; 16 clients oversubscribe the
available execution path and increase queueing. PostgreSQL reports about
**1.9 MiB** of backend memory contexts after each request. That is not a peak
RSS measurement and excludes shared buffers and operating-system page cache.
The native decoder releases the Python GIL, so independent requests can decode
in parallel.

## Storage

The candidate serving schema occupies **119.6 MiB**, including **13.6 MiB** of
indexes. The workload-declared binding summaries occupy **3.8 MiB** total and
use only their three primary-key B-trees. The OCPQ evaluation does not publish
an equivalent serving-storage measurement, so this report does not fabricate a
storage ratio.

Binding indexes are opt-in and workload-declared. Existing datasets that do not
request them keep the tables empty, avoiding an all-neighbor expansion. The
benchmark declares two object types, five activities, and one typed neighbor
relationship. These are inputs to generic index and operator APIs, not compiled
query identifiers or dataset-specific code paths.

## Reproduce

The separate pinned OCPQ image, public dataset preparation, candidate runner,
raw result, and release gate are in [`benchmarks/ocpq`](../benchmarks/ocpq) and
[`results/ocpq-bpic2017-0.4.0.json`](results/ocpq-bpic2017-0.4.0.json).
