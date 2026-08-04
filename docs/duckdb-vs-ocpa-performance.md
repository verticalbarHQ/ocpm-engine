# OCPA, pg_ocpm, and DuckDB Parquet benchmark

## Result

Publication gate: **descriptive only**.

All 4 workload answers matched exactly on `ocpa_running_example`. Relative to OCPA, the p50 geometric-mean speedups were 70.443x for pg_ocpm + ocpm-engine and 339.186x for DuckDB Parquet + ocpm-engine with its bounded exact-result cache. With that cache disabled, the DuckDB p50 geometric-mean ratio over OCPA was 0.360x.

## Exactness

| Workload | Exact | Answer SHA-256 |
|---|---:|---|
| dfg_conformance_95pct | yes | `6d1597bda1ee4df597f7d0d7c2dcaafdd32552c0290ea422bef92594373cd118` |
| variant_conformance_95pct | yes | `80cd775e0a756938d2ce7f6c48bed2fb26d3c4a933b64ba1723e9612bac740bd` |
| next_activity_prediction | yes | `0d11e08bea5189f4b9bd8d8b63a71840edef2dc8b0a3d120c9a08d1d20e5739d` |
| edge_bottleneck_ranking | yes | `34ecb123e6a6270bca1ed9f302917df1347966c0562f4998a71c7740e3dc1528` |

## Steady-state latency

| Workload | OCPA p50 | pg_ocpm p50 | DuckDB cached p50 | DuckDB cache-off p50 | OCPA p95 | pg_ocpm p95 | DuckDB cached p95 | DuckDB cache-off p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 14.937 ms | 0.166 ms | 0.050 ms | 51.287 ms | 92.127 ms | 0.351 ms | 0.140 ms | 74.388 ms |
| variant_conformance_95pct | 16.298 ms | 0.254 ms | 0.090 ms | 52.533 ms | 103.303 ms | 0.494 ms | 0.377 ms | 79.737 ms |
| next_activity_prediction | 14.478 ms | 0.159 ms | 0.047 ms | 53.129 ms | 103.887 ms | 0.310 ms | 0.218 ms | 84.533 ms |
| edge_bottleneck_ranking | 19.062 ms | 0.407 ms | 0.024 ms | 28.062 ms | 109.580 ms | 0.773 ms | 0.062 ms | 51.395 ms |

## DFG concurrency

| Workers | OCPA QPS | pg_ocpm QPS | DuckDB QPS | OCPA p95 | pg_ocpm p95 | DuckDB p95 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 42.767 | 4094.640 | 21801.953 | 106.393 ms | 0.507 ms | 0.052 ms |
| 2 | 76.834 | 7524.812 | 31974.716 | 125.108 ms | 0.495 ms | 0.115 ms |
| 4 | 126.086 | 11389.756 | 56095.598 | 137.536 ms | 0.674 ms | 0.126 ms |
| 8 | 181.418 | 14585.516 | 93149.413 | 165.188 ms | 1.156 ms | 0.184 ms |

## Memory

| Arm | Maximum incremental peak | Maximum process RSS |
|---|---:|---:|
| OCPA | 0.02 MiB | 184.79 MiB |
| pg_ocpm + ocpm-engine | 0.00 MiB | 54.69 MiB |
| DuckDB Parquet + ocpm-engine | 0.00 MiB | 162.18 MiB |

## Storage reported by each arm

| Arm | Source SQLite | Canonical Parquet snapshot |
|---|---:|---:|
| OCPA | 3.57 MiB | N/A |
| pg_ocpm + ocpm-engine | N/A | N/A |
| DuckDB Parquet + ocpm-engine | 3.57 MiB | 0.58 MiB |

## DuckDB setup cost

Snapshot conversion: 440.603 ms. Existing-catalog connection open and optional relation materialization: 85.645 ms.

## Interpretation boundaries

- Import, snapshot conversion, process startup, and connection startup are excluded from steady-state latency and reported by each arm.
- Concurrency uses each architecture's normal scalable service model; memory is reported per arm and is not normalized into an artificial single runtime.
- The results apply to the declared fixed workloads and do not imply the same ratio for arbitrary dynamic queries.
- DuckDB warm-cache and cache-disabled measurements are both published; only the warm-cache state is used in the primary three-arm latency table and concurrency run.
- OCPA is descriptive rather than publication-ready because its documented importer failed on its unchanged upstream example; the disclosed setup adapter is retained in the raw evidence.
