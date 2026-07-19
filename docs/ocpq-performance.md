# Published and reproduced OCPQ comparison

This report compares `pg_ocpm 0.5.0` plus `ocpm-engine 0.4.0` with both the ten
raw OCPQ timings published by the OCPQ authors and a pinned same-host
reproduction of OCPQ 0.6.7. There is no vanilla PostgreSQL arm in this
comparison.

The workload is Q1-Q7 from the public `aarkue/ocpq-eval` repository at commit
`846dd4eb9f8600ae42355968453a9412ea4759c2`, using its BPIC 2017-derived OCEL
2.0 dataset. OCPQ is version 0.6.7. Every candidate result passed a complete
row expansion and exact row-count, violation/value, and canonical SHA-256 gate.

## Latency

| Query | Published OCPQ | Reproduced OCPQ | pg_ocpm + ocpm-engine | Ratio vs published OCPQ | Correct |
|---|---:|---:|---:|---:|---:|
| Q1: application submitted exactly once | 37.427 ms | 18.095 ms | 0.435 ms | 86.047x | yes |
| Q2: offer returned after creation | 57.868 ms | 28.496 ms | 0.937 ms | 61.733x | yes |
| Q3: returned event has exactly one offer | 22.864 ms | 12.413 ms | 0.737 ms | 31.002x | yes |
| Q4: associated offer accepted after application | 67.105 ms | 29.781 ms | 0.754 ms | 88.942x | yes |
| Q5: accepting resource creates every offer | 68.541 ms | 34.001 ms | 1.929 ms | 35.530x | yes |
| Q6: maximum creation-to-acceptance delay | 84.442 ms | 62.339 ms | 0.376 ms | 224.400x | yes |
| Q7: offer pairs and creation events | 79.853 ms | 46.481 ms | 1.242 ms | 64.273x | yes |

Geometric-mean latency is **55.069 ms** for published OCPQ, **29.354 ms** for
reproduced OCPQ, and **0.797 ms** for the candidate. The candidate is **69.078x**
lower-latency than the published reference and **36.822x** lower-latency than
the same-host reproduction by geometric mean. The minimum ratio versus the
published reference is **31.002x**, so all seven queries clear the 10x release
gate.

Published OCPQ and candidate timings were recorded on different hosts, so the
published ratio remains cross-environment. The reproduced OCPQ column was
measured on the candidate host from the pinned source and lockfile, with a fresh
container for every query. It provides a same-host reproduction reference while
retaining the published result for source traceability.

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
| 1 | 1,021 qps | 0.834 ms | 1.955 ms | 2.067 ms |
| 4 | 3,528 qps | 0.945 ms | 2.306 ms | 2.574 ms |
| 8 | 5,816 qps | 1.178 ms | 2.800 ms | 3.252 ms |
| 16 | 5,428 qps | 2.598 ms | 6.134 ms | 8.354 ms |

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
raw results, and release gate are in [`benchmarks/ocpq`](../benchmarks/ocpq),
[`results/ocpq-reproduced-0.6.7.json`](results/ocpq-reproduced-0.6.7.json), and
[`results/ocpq-bpic2017-0.4.0.json`](results/ocpq-bpic2017-0.4.0.json).
