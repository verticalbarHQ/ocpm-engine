# SAP O2C and P2P four-way benchmark

Publication gate: **passed**.

All reported latency cells passed the exact-answer hash gate.

## sap_o2c

| Workload | Vanilla PG + PM4Py p50 | pg_ocpm + PM4Py p50 | pg_ocpm + engine p50 | DuckDB cached p50 | DuckDB cache-off p50 |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 280.323 ms | 193.566 ms | 28.580 ms | 6.329 ms | 149.577 ms |
| variant_conformance_95pct | 233.713 ms | 156.203 ms | 23.908 ms | 14.488 ms | 179.633 ms |
| next_activity_prediction | 245.179 ms | 172.776 ms | 26.525 ms | 4.463 ms | 159.512 ms |
| edge_bottleneck_ranking | 378.055 ms | 283.246 ms | 3.324 ms | 7.574 ms | 120.334 ms |

| Workers | Vanilla QPS | pg_ocpm + PM4Py QPS | pg_ocpm + engine QPS | DuckDB QPS |
|---:|---:|---:|---:|---:|
| 1 | 3.533 | 5.316 | 34.278 | 193.196 |
| 2 | 6.976 | 10.162 | 69.297 | 359.597 |
| 4 | 11.682 | 18.045 | 127.135 | 579.955 |
| 8 | 18.882 | 29.721 | 203.274 | 854.774 |

| Arm | Maximum incremental peak | Maximum process RSS |
|---|---:|---:|
| Vanilla PG + PM4Py | 165.61 MiB | 202.67 MiB |
| pg_ocpm + PM4Py | 165.86 MiB | 202.95 MiB |
| pg_ocpm + ocpm-engine | 6.41 MiB | 43.48 MiB |
| DuckDB + ocpm-engine cached | 2.54 MiB | 225.26 MiB |

DuckDB snapshot: 3.00 MiB. Snapshot conversion: 7718.047 ms. Connection open/materialization: 228.233 ms.

## sap_p2p

| Workload | Vanilla PG + PM4Py p50 | pg_ocpm + PM4Py p50 | pg_ocpm + engine p50 | DuckDB cached p50 | DuckDB cache-off p50 |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 219.123 ms | 170.184 ms | 1.175 ms | 0.108 ms | 177.113 ms |
| variant_conformance_95pct | 183.593 ms | 142.461 ms | 1.331 ms | 0.240 ms | 191.138 ms |
| next_activity_prediction | 198.042 ms | 158.372 ms | 1.168 ms | 0.089 ms | 219.047 ms |
| edge_bottleneck_ranking | 231.930 ms | 192.856 ms | 1.128 ms | 0.071 ms | 151.575 ms |

| Workers | Vanilla QPS | pg_ocpm + PM4Py QPS | pg_ocpm + engine QPS | DuckDB QPS |
|---:|---:|---:|---:|---:|
| 1 | 4.761 | 5.429 | 1526.188 | 9213.433 |
| 2 | 8.711 | 10.094 | 2864.689 | 15812.760 |
| 4 | 14.472 | 18.722 | 5754.221 | 26045.296 |
| 8 | 24.032 | 31.655 | 10438.325 | 35197.294 |

| Arm | Maximum incremental peak | Maximum process RSS |
|---|---:|---:|
| Vanilla PG + PM4Py | 148.16 MiB | 185.23 MiB |
| pg_ocpm + PM4Py | 148.17 MiB | 185.23 MiB |
| pg_ocpm + ocpm-engine | 2.14 MiB | 39.20 MiB |
| DuckDB + ocpm-engine cached | 0.02 MiB | 213.05 MiB |

DuckDB snapshot: 2.10 MiB. Snapshot conversion: 4222.361 ms. Connection open/materialization: 153.987 ms.

## Storage footprint

| Representation | Bytes | MiB |
|---|---:|---:|
| Vanilla PostgreSQL | 131350528 | 125.27 |
| Shared pg_ocpm | 107503616 | 102.52 |
| DuckDB canonical Parquet snapshots | 5355505 | 5.11 |
| Source OCEL SQLite files | 42868736 | 40.88 |

## Interpretation boundaries

- The three PostgreSQL/PM4Py arms reuse the fixed, exact 1.0.0 artifact because this change does not modify pg_ocpm or the PostgreSQL provider path.
- DuckDB cached and cache-disabled latency are separate columns; concurrency uses the normal bounded cache configuration.
- Snapshot conversion and connection-local relation construction are outside request latency and reported separately.
- The composite is publication-ready only when the accepted baseline and the current DuckDB arm both record clean committed source trees.
