# OCPQ Q1-Q7 benchmark

`pg_ocpm 0.8.0` plus `ocpm-engine 0.8.0` completed the strict Q1-Q7
publication protocol with exact duplicate-preserving parity for every
evaluation-tree node. The geometric-mean speedup over the same-host OCPQ 0.6.7
reproduction is **14.02x**; the minimum individual-query speedup is **6.51x**.

## Latency and exact output

Both engines used zero warmups and ten measured evaluations per query in a
fresh container. Means are the primary upstream-compatible comparison metric.
Candidate p50 and nearest-rank p95 are included to make the ten-sample
distribution visible; with ten samples, p95 is the maximum observation.

| Query | Published OCPQ mean | Reproduced OCPQ mean | pg_ocpm + engine mean | Candidate p50 / p95 | Speedup | All-node situations |
|---|---:|---:|---:|---:|---:|---:|
| Q1 | 37.43 ms | 20.44 ms | 1.50 ms | 1.03 / 5.78 ms | 13.63x | 51,932 |
| Q2 | 57.87 ms | 32.76 ms | 2.87 ms | 2.50 / 5.99 ms | 11.42x | 66,300 |
| Q3 | 22.86 ms | 13.40 ms | 2.06 ms | 1.47 / 7.32 ms | 6.51x | 46,610 |
| Q4 | 67.11 ms | 32.98 ms | 2.50 ms | 1.99 / 7.00 ms | 13.21x | 48,737 |
| Q5 | 68.54 ms | 38.75 ms | 5.28 ms | 4.61 / 11.22 ms | 7.35x | 74,504 |
| Q6 | 84.44 ms | 62.57 ms | 1.66 ms | 1.56 / 2.55 ms | 37.63x | 17,229 |
| Q7 | 79.85 ms | 47.87 ms | 1.67 ms | 1.55 / 2.53 ms | 28.72x | 74,771 |

Published OCPQ means are source context from a different machine. No ratio is
calculated from them. Every speedup above uses the corrected same-host OCPQ
reproduction.

## Concurrency

Each client holds one persistent PostgreSQL connection and prepared Q1-Q7
request set. Every level contains three epochs of at least five seconds and 32
requests per client, with exact all-node parity before and after each epoch.

| Clients | Median throughput | p50 | p95 | p99 | Throughput CV | Requests |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 505.98 req/s | 1.53 ms | 4.26 ms | 4.34 ms | 1.54% | 7,520 |
| 4 | 1,519.21 req/s | 2.20 ms | 5.08 ms | 6.26 ms | 1.08% | 22,787 |
| 8 | 2,642.76 req/s | 2.65 ms | 5.84 ms | 7.18 ms | 0.61% | 39,584 |
| 16 | 4,223.27 req/s | 3.29 ms | 7.55 ms | 9.91 ms | 1.20% | 62,994 |

## Storage and memory

| Serving footprint | Size |
|---|---:|
| Total `ocpm` relations | 109.7 MiB |
| Indexes | 9.8 MiB |
| Binding summaries | 5.9 MiB |

The maximum fresh-process client peak above baseline was 6.1 MiB, and the
largest fully owned decoded tree was 4.0 MiB. The request-result cache was
disabled and empty.

## Evidence

- [Same-host OCPQ reference](results/ocpq-reproduced-strict-all-node-0.6.7.json)
- [pg_ocpm 0.8.0 + ocpm-engine 0.8.0 result](results/ocpq-bpic2017-pg_ocpm-0.8.0-ocpm-engine-0.8.0.json)
- [Reproduction and gate instructions](../benchmarks/ocpq/README.md)

The corrected protocol compares pinned OCPQ 0.6.7 with `pg_ocpm` plus
`ocpm-engine` on the same Docker host. Correctness covers every object/event
binding, exact violation, typed label, node manifest, and canonical hash. The
timed boundary includes complete owned all-node materialization. The committed
checker pins both evidence-file SHA-256 digests and independently recomputes
every latency, correctness, storage, memory, and concurrency gate.
