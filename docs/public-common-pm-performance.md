# Public common-process-mining benchmark

On the checksum-pinned SAP IDES O2C and P2P OCEL 2.0 logs, `pg_ocpm 0.8.0`
plus `ocpm-engine 0.8.0` achieved a **45.03x geometric-mean speedup** over
indexed relational OCEL in vanilla PostgreSQL plus independent Python kernels.
All 18 workload/dataset pairs were exact, and the minimum speedup was
**20.28x**.

## Serial latency

| Dataset | Workload | Vanilla PostgreSQL p50 | pg_ocpm + engine p50 | Speedup |
|---|---|---:|---:|---:|
| SAP O2C | DFG conformance | 36.19 ms | 0.89 ms | 40.61x |
| SAP O2C | Variant conformance | 24.11 ms | 1.19 ms | 20.28x |
| SAP O2C | Next-activity prediction | 35.96 ms | 0.87 ms | 41.57x |
| SAP O2C | DFG frequency drift | 36.06 ms | 0.92 ms | 39.37x |
| SAP O2C | Repeated-transition rework | 37.95 ms | 1.71 ms | 22.18x |
| SAP O2C | Bottleneck ranking | 39.53 ms | 1.67 ms | 23.72x |
| SAP O2C | Bottleneck prediction | 94.03 ms | 2.44 ms | 38.49x |
| SAP O2C | Edge-duration time series | 246.47 ms | 2.20 ms | 112.29x |
| SAP O2C | Activity profile | 46.68 ms | 2.18 ms | 21.38x |
| SAP P2P | DFG conformance | 15.23 ms | 0.23 ms | 66.22x |
| SAP P2P | Variant conformance | 27.12 ms | 0.31 ms | 86.35x |
| SAP P2P | Next-activity prediction | 15.16 ms | 0.23 ms | 67.08x |
| SAP P2P | DFG frequency drift | 15.25 ms | 0.25 ms | 60.05x |
| SAP P2P | Repeated-transition rework | 17.25 ms | 0.58 ms | 29.79x |
| SAP P2P | Bottleneck ranking | 15.72 ms | 0.37 ms | 42.94x |
| SAP P2P | Bottleneck prediction | 33.32 ms | 0.54 ms | 61.37x |
| SAP P2P | Edge-duration time series | 104.20 ms | 1.64 ms | 63.73x |
| SAP P2P | Activity profile | 29.10 ms | 0.38 ms | 76.79x |

Each arm used 10 warmups followed by three epochs of 30 randomized measured
rounds. The JSON evidence retains every integer-nanosecond sample, realized arm
order, per-epoch p50/p95 range, and exact-answer hash.

## Concurrency

| Workload | Workers | Vanilla throughput | pg_ocpm + engine throughput | Ratio | Vanilla / engine p95 |
|---|---:|---:|---:|---:|---:|
| DFG conformance | 1 | 27.53 req/s | 1,354.43 req/s | 49.21x | 37.06 / 0.78 ms |
| DFG conformance | 4 | 93.21 req/s | 4,969.20 req/s | 53.31x | 46.50 / 0.92 ms |
| DFG conformance | 8 | 150.79 req/s | 5,913.30 req/s | 39.22x | 56.54 / 2.64 ms |
| DFG conformance | 16 | 276.54 req/s | 3,968.20 req/s | 14.35x | 60.38 / 9.73 ms |
| DFG frequency drift | 1 | 27.38 req/s | 1,203.15 req/s | 43.94x | 37.24 / 0.87 ms |
| DFG frequency drift | 4 | 91.71 req/s | 4,412.67 req/s | 48.11x | 47.30 / 1.02 ms |
| DFG frequency drift | 8 | 151.18 req/s | 3,817.13 req/s | 25.25x | 56.48 / 4.50 ms |
| DFG frequency drift | 16 | 280.04 req/s | 3,431.01 req/s | 12.25x | 59.81 / 11.33 ms |

Each engine/worker arm used three independent epochs, one persistent connection
per prestarted worker, an exact warmup from every worker, at least five seconds,
and at least 32 requests per worker in every epoch.

## Storage

| Representation | Total | Indexes |
|---|---:|---:|
| Vanilla relational OCEL | 173.3 MiB | 113.9 MiB |
| pg_ocpm serving schema | 102.4 MiB | 19.8 MiB |

The pg_ocpm representation uses 40.9% less total serving storage and 82.6%
less index storage for this benchmark.

## Evidence

The source is Zenodo DOI `10.5281/zenodo.8261133`, licensed CC BY 4.0. See the
[complete schema-5 evidence](results/public-common-pm-0.8.0.json) and
[benchmark guide](../benchmarks/README.md). `make perf-public-release-check`
recomputes the committed digest, workload results, sample summaries,
concurrency metrics, correctness, storage, and source/image provenance.
