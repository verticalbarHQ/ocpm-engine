# SAP O2C and P2P three-way process-mining benchmark

The official SAP IDES OCEL 2.0 logs are compared through three paths: PM4Py over lightly indexed relational PostgreSQL, PM4Py over `pg_ocpm` event chunks, and ocpm-engine over `pg_ocpm` sufficient statistics. Every accepted sample passed exact three-way semantic comparison.

## sap_o2c

Source: **98,350 events**, **107,767 objects**, and **236,265 event-object links**. Backbone: **MATERIAL**, with **4,198 cases** (**3,987 train**, **139 test**).

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 139.02 ms | 101.35 ms | 12.07 ms | 1.37x | 11.52x |
| variant_conformance_95pct | 129.17 ms | 91.31 ms | 16.11 ms | 1.42x | 8.02x |
| next_activity_prediction | 140.71 ms | 103.16 ms | 12.06 ms | 1.36x | 11.67x |
| edge_bottleneck_ranking | 196.87 ms | 148.15 ms | 3.35 ms | 1.33x | 58.70x |

Geometric-mean speedup versus vanilla: **1.37x** for pg_ocpm + PM4Py and **15.86x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 7.13 req/s | 8.99 req/s | 81.23 req/s | 11.39x |
| 2 | 12.29 req/s | 18.65 req/s | 154.74 req/s | 12.59x |
| 4 | 22.55 req/s | 33.55 req/s | 260.54 req/s | 11.56x |
| 8 | 32.02 req/s | 56.43 req/s | 394.38 req/s | 12.32x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 189.4 MiB | 188.7 MiB | 36.4 MiB |
| variant_conformance_95pct | 184.8 MiB | 184.9 MiB | 39.1 MiB |
| next_activity_prediction | 188.7 MiB | 189.4 MiB | 36.4 MiB |
| edge_bottleneck_ranking | 201.1 MiB | 201.4 MiB | 36.4 MiB |

## sap_p2p

Source: **24,854 events**, **74,489 objects**, and **105,039 event-object links**. Backbone: **EBELN_EBELP**, with **31,143 cases** (**24,915 train**, **6,221 test**).

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 123.22 ms | 96.80 ms | 25.05 ms | 1.27x | 4.92x |
| variant_conformance_95pct | 116.46 ms | 90.13 ms | 0.71 ms | 1.29x | 164.73x |
| next_activity_prediction | 122.02 ms | 95.52 ms | 25.10 ms | 1.28x | 4.86x |
| edge_bottleneck_ranking | 127.31 ms | 103.81 ms | 1.01 ms | 1.23x | 125.92x |

Geometric-mean speedup versus vanilla: **1.27x** for pg_ocpm + PM4Py and **26.54x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 8.03 req/s | 9.60 req/s | 39.75 req/s | 4.95x |
| 2 | 14.71 req/s | 18.25 req/s | 69.00 req/s | 4.69x |
| 4 | 26.51 req/s | 33.34 req/s | 125.10 req/s | 4.72x |
| 8 | 36.84 req/s | 60.63 req/s | 222.36 req/s | 6.04x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 180.8 MiB | 180.8 MiB | 36.3 MiB |
| variant_conformance_95pct | 180.0 MiB | 180.1 MiB | 36.4 MiB |
| next_activity_prediction | 180.8 MiB | 180.8 MiB | 36.4 MiB |
| edge_bottleneck_ranking | 183.9 MiB | 183.7 MiB | 36.4 MiB |

## Shared storage and client footprint

| Representation | Total | Indexes |
|---|---:|---:|
| Vanilla relational OCEL with one workload secondary B-tree | 125.3 MiB | 65.9 MiB |
| pg_ocpm serving schema | 102.2 MiB | 19.7 MiB |

The vanilla index total also includes primary-key and uniqueness indexes required for relational integrity. Only `ocel_e2o_object` is retained as a workload-specific secondary index.

| Client | Package only | Dependency closure |
|---|---:|---:|
| PM4Py | 16.6 MiB | 460.6 MiB |
| ocpm-engine | 0.6 MiB | 0.6 MiB |

## Methodology

- 1 warmup and 5 randomized measured runs per latency comparison.
- Latency includes database extraction, client materialization, model construction, and scoring.
- Train and test partitions contain complete cases only; cases spanning the temporal boundary are excluded from both partitions.
- Concurrency uses prestarted, warmed process workers with one PostgreSQL connection per worker.
- Peak RSS uses a fresh process for each dataset, workload, and engine path.
- Source: Zenodo DOI `10.5281/zenodo.8261133`, CC BY 4.0.
- PM4Py package licensing must be evaluated separately before product integration; installed metadata is retained in JSON.
