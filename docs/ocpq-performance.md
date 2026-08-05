# Strict OCPQ Q1-Q7 benchmark

`pg_ocpm 1.0.0` plus `ocpm-engine 1.0.0` passed the strict Q1-Q7 publication
protocol against OCPQ 0.6.7 with exact duplicate-preserving parity for every
evaluation-tree node. The same-host geometric-mean speedup is **16.137x** and
the minimum individual-query speedup is **9.522x**.

| Query | OCPQ mean | pg_ocpm + engine mean | Speedup | Exact nodes |
|---|---:|---:|---:|---:|
| Q1 | 32.517 ms | 2.158 ms | 15.070x | yes |
| Q2 | 46.192 ms | 3.589 ms | 12.870x | yes |
| Q3 | 25.962 ms | 2.490 ms | 10.427x | yes |
| Q4 | 49.905 ms | 3.444 ms | 14.491x | yes |
| Q5 | 53.537 ms | 5.623 ms | 9.522x | yes |
| Q6 | 105.727 ms | 2.813 ms | 37.586x | yes |
| Q7 | 86.676 ms | 3.190 ms | 27.167x | yes |
| **Geometric mean** | **51.453 ms** | **3.189 ms** | **16.137x** | **yes** |

Both arms used zero warmups, ten measured runs per query, and a fresh container
for each query. The candidate timer includes prepared PostgreSQL execution,
fetch, native decoding, and complete owned materialization of every node.
External-ID canonicalization, sorting, serialization, hashing, and comparison
are excluded from both timers but required for every accepted sample.

The 1/4/8/16-client concurrency sweep reached **2,124.6 req/s** at 16 clients
with **15.661 ms p95**. Maximum client RSS above baseline was **6.25 MiB**;
serving storage was **109.91 MiB**. Every pre- and post-epoch node check passed,
the result cache was disabled, and source, image, database, host, and clean-tree
provenance were complete.

## Same-time 0.9/1.0 concurrency diagnostic

The absolute 1.0 p95 was recorded while unrelated host processes were consuming
several CPU cores. To distinguish product regression from host load, the
byte-identical 0.9 and 1.0 candidate runners were executed back-to-back on the
same active Docker host, with their corresponding extension images and the
unchanged exactness/concurrency protocol.

| Clients | 0.9 QPS | 1.0 QPS | 1.0/0.9 | 0.9 p95 | 1.0 p95 |
|---:|---:|---:|---:|---:|---:|
| 1 | 272.7 | 277.3 | 1.017x | 7.689 ms | 7.422 ms |
| 4 | 891.5 | 883.4 | 0.991x | 8.910 ms | 8.788 ms |
| 8 | 1,439.8 | 1,620.7 | 1.126x | 11.028 ms | 9.659 ms |
| 16 | 2,023.8 | 2,301.6 | 1.137x | 16.390 ms | 14.481 ms |

All pre/post answers remained exact. This diagnostic shows no 1.0 concurrency
regression, but it is not substituted for the clean 1.0 publication artifact.
The checker retains exact raw-latency reconstruction, at most 15% epoch-QPS
variation, at least 5x 16:1 scaling, and a portable p95/p50 tail-amplification
limit instead of treating an unreserved host as an absolute-latency SLA.

The raw diagnostic artifacts are `ocpq-concurrency-ab-pg-0.9.0.json` and
`ocpq-concurrency-ab-pg-1.0.0.json` in the repository's `docs/results/`
directory.

See [the four-way comparison](ocpq-1.0-four-way-comparison.md) for the two
PM4Py arms and the interpretation boundary.
