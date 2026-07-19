# SAP O2C and P2P three-way process-mining benchmark

The official SAP IDES OCEL 2.0 logs are compared through three paths: PM4Py over lightly indexed relational PostgreSQL, PM4Py over `pg_ocpm` event chunks, and ocpm-engine over `pg_ocpm` sufficient statistics. Every accepted sample passed exact three-way semantic comparison.

## sap_o2c

Source: **98,350 events**, **107,767 objects**, and **236,265 event-object links**. Backbone: **MATERIAL**, with **4,198 cases** (**3,987 train**, **139 test**).

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 142.59 ms | 104.21 ms | 12.06 ms | 1.37x | 11.83x |
| variant_conformance_95pct | 130.37 ms | 92.30 ms | 16.33 ms | 1.41x | 7.98x |
| next_activity_prediction | 140.01 ms | 101.93 ms | 11.81 ms | 1.37x | 11.86x |
| edge_bottleneck_ranking | 193.57 ms | 147.81 ms | 3.33 ms | 1.31x | 58.20x |

Geometric-mean speedup versus vanilla: **1.37x** for pg_ocpm + PM4Py and **15.97x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 7.19 req/s | 9.54 req/s | 81.36 req/s | 11.32x |
| 2 | 12.75 req/s | 18.48 req/s | 154.70 req/s | 12.14x |
| 4 | 23.08 req/s | 32.20 req/s | 265.21 req/s | 11.49x |
| 8 | 32.63 req/s | 55.01 req/s | 501.48 req/s | 15.37x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 189.4 MiB | 189.4 MiB | 36.4 MiB |
| variant_conformance_95pct | 184.8 MiB | 184.8 MiB | 39.2 MiB |
| next_activity_prediction | 189.4 MiB | 189.4 MiB | 36.4 MiB |
| edge_bottleneck_ranking | 201.4 MiB | 201.2 MiB | 36.4 MiB |

## sap_p2p

Source: **24,854 events**, **74,489 objects**, and **105,039 event-object links**. Backbone: **EBELN_EBELP**, with **31,143 cases** (**24,915 train**, **6,221 test**).

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 126.30 ms | 98.57 ms | 25.20 ms | 1.28x | 5.01x |
| variant_conformance_95pct | 120.36 ms | 92.72 ms | 0.70 ms | 1.30x | 171.45x |
| next_activity_prediction | 122.92 ms | 95.85 ms | 24.86 ms | 1.28x | 4.95x |
| edge_bottleneck_ranking | 127.01 ms | 102.77 ms | 0.93 ms | 1.24x | 136.28x |

Geometric-mean speedup versus vanilla: **1.27x** for pg_ocpm + PM4Py and **27.58x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 8.00 req/s | 10.19 req/s | 40.10 req/s | 5.01x |
| 2 | 15.17 req/s | 19.89 req/s | 78.07 req/s | 5.15x |
| 4 | 25.96 req/s | 35.64 req/s | 122.92 req/s | 4.74x |
| 8 | 36.17 req/s | 59.74 req/s | 228.55 req/s | 6.32x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 180.8 MiB | 180.8 MiB | 36.4 MiB |
| variant_conformance_95pct | 180.0 MiB | 180.1 MiB | 36.5 MiB |
| next_activity_prediction | 180.8 MiB | 180.8 MiB | 36.4 MiB |
| edge_bottleneck_ranking | 183.8 MiB | 183.7 MiB | 36.4 MiB |

## Shared storage and client footprint

| Representation | Total | Indexes |
|---|---:|---:|
| Vanilla relational OCEL with one workload secondary B-tree | 125.3 MiB | 65.9 MiB |
| pg_ocpm serving schema | 102.3 MiB | 19.7 MiB |

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
