# SAP O2C and P2P three-way process-mining benchmark

The official SAP IDES OCEL 2.0 logs are compared through three paths: PM4Py over lightly indexed relational PostgreSQL, PM4Py over `pg_ocpm` event chunks, and ocpm-engine over `pg_ocpm` sufficient statistics. Every accepted sample passed exact three-way semantic comparison.

## sap_o2c

Source: **98,350 events**, **107,767 objects**, and **236,265 event-object links**. Backbone: **MATERIAL**, with **4,198 cases** (**3,987 train**, **139 test**).

| Workload | Vanilla PG + PM4Py p50/p95 (epoch range) | pg_ocpm + PM4Py p50/p95 (epoch range) | pg_ocpm + ocpm-engine p50/p95 (epoch range) | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 280.32 / 326.63 ms (324.85-328.84) | 193.57 / 220.21 ms (215.57-233.11) | 28.58 / 32.35 ms (32.19-33.55) | 1.45x | 9.81x |
| variant_conformance_95pct | 233.71 / 271.84 ms (245.98-277.25) | 156.20 / 176.71 ms (168.81-178.22) | 23.91 / 27.06 ms (25.08-27.78) | 1.50x | 9.78x |
| next_activity_prediction | 245.18 / 283.48 ms (265.13-286.13) | 172.78 / 196.77 ms (191.56-197.82) | 26.52 / 29.11 ms (28.18-29.48) | 1.42x | 9.24x |
| edge_bottleneck_ranking | 378.06 / 486.29 ms (433.42-497.88) | 283.25 / 362.62 ms (306.90-405.08) | 3.32 / 4.03 ms (3.60-4.28) | 1.33x | 113.73x |

Geometric-mean speedup versus vanilla: **1.42x** for pg_ocpm + PM4Py and **17.82x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 3.53 req/s | 5.32 req/s | 34.28 req/s | 9.70x |
| 2 | 6.98 req/s | 10.16 req/s | 69.30 req/s | 9.93x |
| 4 | 11.68 req/s | 18.05 req/s | 127.14 req/s | 10.88x |
| 8 | 18.88 req/s | 29.72 req/s | 203.27 req/s | 10.77x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 191.1 MiB | 190.2 MiB | 39.2 MiB |
| variant_conformance_95pct | 186.3 MiB | 186.3 MiB | 43.5 MiB |
| next_activity_prediction | 190.8 MiB | 190.5 MiB | 39.2 MiB |
| edge_bottleneck_ranking | 202.7 MiB | 202.9 MiB | 39.0 MiB |

## sap_p2p

Source: **24,854 events**, **74,489 objects**, and **105,039 event-object links**. Backbone: **EBELN_EBELP**, with **31,143 cases** (**24,915 train**, **6,221 test**).

| Workload | Vanilla PG + PM4Py p50/p95 (epoch range) | pg_ocpm + PM4Py p50/p95 (epoch range) | pg_ocpm + ocpm-engine p50/p95 (epoch range) | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 219.12 / 264.64 ms (262.73-270.34) | 170.18 / 213.13 ms (211.87-224.50) | 1.18 / 1.72 ms (1.42-1.87) | 1.29x | 186.49x |
| variant_conformance_95pct | 183.59 / 221.22 ms (207.48-222.41) | 142.46 / 172.32 ms (162.72-178.25) | 1.33 / 1.99 ms (1.53-2.06) | 1.29x | 137.94x |
| next_activity_prediction | 198.04 / 250.62 ms (204.98-257.65) | 158.37 / 197.17 ms (163.97-198.08) | 1.17 / 1.55 ms (1.33-1.66) | 1.25x | 169.56x |
| edge_bottleneck_ranking | 231.93 / 268.69 ms (263.94-279.25) | 192.86 / 232.86 ms (220.04-233.39) | 1.13 / 1.50 ms (1.35-1.54) | 1.20x | 205.61x |

Geometric-mean speedup versus vanilla: **1.26x** for pg_ocpm + PM4Py and **173.05x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 4.76 req/s | 5.43 req/s | 1,526.19 req/s | 320.56x |
| 2 | 8.71 req/s | 10.09 req/s | 2,864.69 req/s | 328.86x |
| 4 | 14.47 req/s | 18.72 req/s | 5,754.22 req/s | 397.61x |
| 8 | 24.03 req/s | 31.66 req/s | 10,438.33 req/s | 434.35x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 182.3 MiB | 182.7 MiB | 39.2 MiB |
| variant_conformance_95pct | 181.5 MiB | 181.5 MiB | 39.0 MiB |
| next_activity_prediction | 182.3 MiB | 182.6 MiB | 39.2 MiB |
| edge_bottleneck_ranking | 185.2 MiB | 185.2 MiB | 39.0 MiB |

## Shared storage and client footprint

| Representation | Total | Indexes |
|---|---:|---:|
| Vanilla relational OCEL with one workload secondary B-tree | 125.3 MiB | 65.9 MiB |
| pg_ocpm serving schema | 102.5 MiB | 19.9 MiB |

The vanilla index total also includes primary-key and uniqueness indexes required for relational integrity. Only `ocel_e2o_object` is retained as a workload-specific secondary index.

| Client | Package only | Dependency closure |
|---|---:|---:|
| PM4Py | 16.6 MiB | 464.6 MiB |
| ocpm-engine | 3.9 MiB | 3.9 MiB |

## Methodology

- 10 warmups and 3 serial epochs of 30 randomized measured rounds per latency comparison.
- Serial p50 and p95 use all retained nanosecond samples; each p95 is shown with the minimum-to-maximum range of the three epoch p95s.
- Latency includes database extraction, client materialization, model construction, and scoring.
- Train and test partitions contain complete cases only; cases spanning the temporal boundary are excluded from both partitions.
- Concurrency uses three independently prestarted epochs per engine and level, one persistent PostgreSQL connection per worker, an exact warmup from every worker PID, and at least five seconds plus 32 requests per worker in every epoch. QPS and p50/p95/p99 are medians of epoch metrics.
- Peak RSS uses a fresh process for each dataset, workload, and engine path.
- Source: Zenodo DOI `10.5281/zenodo.8261133`, CC BY 4.0.
- PM4Py package licensing must be evaluated separately before product integration; installed metadata is retained in JSON.

## Clean-room boundary

PM4Py is a fixed, isolated benchmark dependency and an exact-answer oracle; it is not a product dependency or an implementation source. The `pg_ocpm` and `ocpm-engine` algorithms are independently authored from the peer-reviewed papers listed in [academic implementation provenance](academic-implementation-provenance.md).
