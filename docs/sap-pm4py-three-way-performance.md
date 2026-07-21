# SAP O2C and P2P three-way process-mining benchmark

The official SAP IDES OCEL 2.0 logs are compared through three paths: PM4Py over lightly indexed relational PostgreSQL, PM4Py over `pg_ocpm` event chunks, and ocpm-engine over `pg_ocpm` sufficient statistics. Every accepted sample passed exact three-way semantic comparison.

## sap_o2c

Source: **98,350 events**, **107,767 objects**, and **236,265 event-object links**. Backbone: **MATERIAL**, with **4,198 cases** (**3,987 train**, **139 test**).

| Workload | Vanilla PG + PM4Py p50/p95 (epoch range) | pg_ocpm + PM4Py p50/p95 (epoch range) | pg_ocpm + ocpm-engine p50/p95 (epoch range) | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 136.46 / 141.76 ms (138.31-143.90) | 99.80 / 102.09 ms (101.54-102.30) | 12.45 / 12.70 ms (12.64-12.73) | 1.37x | 10.96x |
| variant_conformance_95pct | 125.91 / 133.44 ms (129.79-133.93) | 89.01 / 92.90 ms (91.07-93.62) | 15.96 / 16.28 ms (16.26-16.29) | 1.42x | 7.89x |
| next_activity_prediction | 136.29 / 141.61 ms (139.92-142.44) | 99.43 / 105.27 ms (102.22-107.43) | 12.39 / 12.65 ms (12.56-13.07) | 1.37x | 11.00x |
| edge_bottleneck_ranking | 191.32 / 195.99 ms (195.71-196.42) | 144.03 / 150.87 ms (148.02-151.97) | 3.21 / 3.32 ms (3.31-3.33) | 1.33x | 59.64x |

Geometric-mean speedup versus vanilla: **1.37x** for pg_ocpm + PM4Py and **15.43x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 7.46 req/s | 10.10 req/s | 90.94 req/s | 12.19x |
| 2 | 13.27 req/s | 19.19 req/s | 168.10 req/s | 12.67x |
| 4 | 22.61 req/s | 33.91 req/s | 295.93 req/s | 13.09x |
| 8 | 32.31 req/s | 53.98 req/s | 480.81 req/s | 14.88x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 190.5 MiB | 189.7 MiB | 37.5 MiB |
| variant_conformance_95pct | 185.6 MiB | 185.8 MiB | 40.2 MiB |
| next_activity_prediction | 189.1 MiB | 190.3 MiB | 37.5 MiB |
| edge_bottleneck_ranking | 202.3 MiB | 202.3 MiB | 37.5 MiB |

## sap_p2p

Source: **24,854 events**, **74,489 objects**, and **105,039 event-object links**. Backbone: **EBELN_EBELP**, with **31,143 cases** (**24,915 train**, **6,221 test**).

| Workload | Vanilla PG + PM4Py p50/p95 (epoch range) | pg_ocpm + PM4Py p50/p95 (epoch range) | pg_ocpm + ocpm-engine p50/p95 (epoch range) | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 119.10 / 122.02 ms (120.91-123.06) | 95.80 / 98.61 ms (97.67-108.15) | 24.72 / 26.30 ms (24.91-26.37) | 1.24x | 4.82x |
| variant_conformance_95pct | 113.23 / 116.13 ms (114.22-129.81) | 90.13 / 92.02 ms (91.37-92.74) | 0.49 / 0.56 ms (0.54-0.56) | 1.26x | 231.09x |
| next_activity_prediction | 117.23 / 120.46 ms (119.67-122.81) | 94.39 / 98.67 ms (96.39-99.60) | 24.59 / 25.06 ms (24.93-25.13) | 1.24x | 4.77x |
| edge_bottleneck_ranking | 121.71 / 126.08 ms (124.64-133.04) | 100.91 / 104.23 ms (103.33-107.51) | 0.80 / 0.89 ms (0.87-0.94) | 1.21x | 151.38x |

Geometric-mean speedup versus vanilla: **1.24x** for pg_ocpm + PM4Py and **29.94x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 8.61 req/s | 10.99 req/s | 41.51 req/s | 4.82x |
| 2 | 15.46 req/s | 20.73 req/s | 76.14 req/s | 4.93x |
| 4 | 26.96 req/s | 37.44 req/s | 130.27 req/s | 4.83x |
| 8 | 40.05 req/s | 57.89 req/s | 221.90 req/s | 5.54x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 181.7 MiB | 181.7 MiB | 37.5 MiB |
| variant_conformance_95pct | 180.9 MiB | 180.9 MiB | 37.5 MiB |
| next_activity_prediction | 181.7 MiB | 181.8 MiB | 37.5 MiB |
| edge_bottleneck_ranking | 184.7 MiB | 184.7 MiB | 37.5 MiB |

## Shared storage and client footprint

| Representation | Total | Indexes |
|---|---:|---:|
| Vanilla relational OCEL with one workload secondary B-tree | 125.3 MiB | 65.9 MiB |
| pg_ocpm serving schema | 102.3 MiB | 19.7 MiB |

The vanilla index total also includes primary-key and uniqueness indexes required for relational integrity. Only `ocel_e2o_object` is retained as a workload-specific secondary index.

| Client | Package only | Dependency closure |
|---|---:|---:|
| PM4Py | 16.6 MiB | 464.6 MiB |
| ocpm-engine | 0.6 MiB | 0.6 MiB |

## Methodology

- 10 warmups and 3 serial epochs of 30 randomized measured rounds per latency comparison.
- Serial p50 and p95 use all retained nanosecond samples; each p95 is shown with the minimum-to-maximum range of the three epoch p95s.
- Latency includes database extraction, client materialization, model construction, and scoring.
- Train and test partitions contain complete cases only; cases spanning the temporal boundary are excluded from both partitions.
- Concurrency uses three independently prestarted epochs per engine and level, one persistent PostgreSQL connection per worker, an exact warmup from every worker PID, and at least five seconds plus 32 requests per worker in every epoch. QPS and p50/p95/p99 are medians of epoch metrics.
- Peak RSS uses a fresh process for each dataset, workload, and engine path.
- Source: Zenodo DOI `10.5281/zenodo.8261133`, CC BY 4.0.
- PM4Py package licensing must be evaluated separately before product integration; installed metadata is retained in JSON.
