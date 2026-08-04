# Rust4PM, pg_ocpm, and DuckDB Parquet benchmark

## Result

Publication gate: **passed**.

All 4 workload answers matched exactly on `rust4pm_p2p`. Relative to Rust4PM, the p50 geometric-mean speedups were 2.990x for pg_ocpm + ocpm-engine and 12.164x for DuckDB Parquet + ocpm-engine with its bounded exact-result cache. With that cache disabled, the DuckDB p50 geometric-mean ratio over Rust4PM was 0.056x.

## Exactness

| Workload | Exact | Answer SHA-256 |
|---|---:|---|
| dfg_conformance_95pct | yes | `60ab48203c1517581d04b17d4b025b33dab3a5567f2a2f88a20d8d3e3ff8c7d9` |
| variant_conformance_95pct | yes | `1c911590ca4d2aed672b2b8e0b0680713dcda71dbc6e20ef89eac80bcf8227f1` |
| next_activity_prediction | yes | `a8ea57fdec0c3e86ea487f5a71ef024e099f3bfae3e69d2dae790808e314c8b8` |
| edge_bottleneck_ranking | yes | `4de113819ac516dd65201c03400cf132dc50f02a66cc0e25eebc632165e6b444` |

## Steady-state latency

| Workload | Rust4PM p50 | pg_ocpm p50 | DuckDB cached p50 | DuckDB cache-off p50 | Rust4PM p95 | pg_ocpm p95 | DuckDB cached p95 | DuckDB cache-off p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 3.172 ms | 1.275 ms | 0.206 ms | 70.858 ms | 5.077 ms | 2.003 ms | 0.592 ms | 131.405 ms |
| variant_conformance_95pct | 4.724 ms | 2.047 ms | 1.095 ms | 75.510 ms | 7.078 ms | 2.483 ms | 1.541 ms | 123.609 ms |
| next_activity_prediction | 5.218 ms | 1.269 ms | 0.356 ms | 81.442 ms | 9.207 ms | 2.525 ms | 1.581 ms | 131.082 ms |
| edge_bottleneck_ranking | 3.193 ms | 0.943 ms | 0.142 ms | 59.837 ms | 5.347 ms | 1.543 ms | 0.257 ms | 321.943 ms |

## DFG concurrency

| Workers | Rust4PM QPS | pg_ocpm QPS | DuckDB QPS | Rust4PM p95 | pg_ocpm p95 | DuckDB p95 |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 721.098 | 595.611 | 2570.990 | 2.132 ms | 2.972 ms | 0.774 ms |
| 2 | 1188.170 | 1739.294 | 5191.818 | 2.311 ms | 1.779 ms | 0.744 ms |
| 4 | 2178.317 | 2465.894 | 8962.395 | 2.680 ms | 3.035 ms | 0.858 ms |
| 8 | 3266.529 | 3079.040 | 13029.387 | 4.234 ms | 7.626 ms | 1.683 ms |

## Memory

| Arm | Maximum incremental peak | Maximum process RSS |
|---|---:|---:|
| Rust4PM | 0.00 MiB | 53.18 MiB |
| pg_ocpm + ocpm-engine | 0.02 MiB | 52.76 MiB |
| DuckDB Parquet + ocpm-engine | 0.00 MiB | 149.62 MiB |

## Storage reported by each arm

| Arm | Source SQLite | Canonical Parquet snapshot |
|---|---:|---:|
| Rust4PM | 13.11 MiB | N/A |
| pg_ocpm + ocpm-engine | N/A | N/A |
| DuckDB Parquet + ocpm-engine | 13.11 MiB | 0.81 MiB |

## DuckDB setup cost

Snapshot conversion: 1345.529 ms. Existing-catalog connection open and optional relation materialization: 147.584 ms.

## Interpretation boundaries

- This common-workload suite is separate from strict OCPQ Q1-Q7.
- Import, snapshot conversion, process startup, and connection startup are excluded from steady-state latency and reported by each arm.
- Concurrency uses each architecture's normal scalable service model; memory is reported per arm and is not normalized into an artificial single runtime.
- The results apply to the declared fixed workloads and do not imply the same ratio for arbitrary dynamic queries.
- DuckDB warm-cache and cache-disabled measurements are both published; only the warm-cache state is used in the primary three-arm latency table and concurrency run.
