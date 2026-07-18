# SAP O2C and P2P three-way process-mining benchmark

The official SAP IDES OCEL 2.0 logs are compared through three paths: PM4Py over lightly indexed relational PostgreSQL, PM4Py over `pg_ocpm` event chunks, and ocpm-engine over `pg_ocpm` sufficient statistics. Every accepted sample passed exact three-way semantic comparison.

## sap_o2c

Source: **98,350 events**, **107,767 objects**, and **236,265 event-object links**. Backbone: **MATERIAL**, with **4,198 cases** (**3,987 train**, **139 test**).

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 137.19 ms | 103.64 ms | 11.81 ms | 1.32x | 11.62x |
| variant_conformance_95pct | 126.40 ms | 90.01 ms | 15.56 ms | 1.40x | 8.12x |
| next_activity_prediction | 139.57 ms | 101.25 ms | 11.80 ms | 1.38x | 11.82x |
| edge_bottleneck_ranking | 190.41 ms | 146.01 ms | 3.10 ms | 1.30x | 61.32x |

Geometric-mean speedup versus vanilla: **1.35x** for pg_ocpm + PM4Py and **16.18x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 7.22 req/s | 9.72 req/s | 85.48 req/s | 11.84x |
| 2 | 13.03 req/s | 17.84 req/s | 153.62 req/s | 11.79x |
| 4 | 23.27 req/s | 34.52 req/s | 290.81 req/s | 12.50x |
| 8 | 32.29 req/s | 55.65 req/s | 485.63 req/s | 15.04x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 189.6 MiB | 189.6 MiB | 36.3 MiB |
| variant_conformance_95pct | 185.0 MiB | 184.8 MiB | 39.1 MiB |
| next_activity_prediction | 188.8 MiB | 188.5 MiB | 36.4 MiB |
| edge_bottleneck_ranking | 201.3 MiB | 201.6 MiB | 36.3 MiB |

## sap_p2p

Source: **24,854 events**, **74,489 objects**, and **105,039 event-object links**. Backbone: **EBELN_EBELP**, with **31,143 cases** (**24,915 train**, **6,221 test**).

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | pg_ocpm PM4Py vs vanilla | Engine vs vanilla |
|---|---:|---:|---:|---:|---:|
| dfg_conformance_95pct | 122.86 ms | 96.07 ms | 25.15 ms | 1.28x | 4.88x |
| variant_conformance_95pct | 119.90 ms | 91.00 ms | 0.70 ms | 1.32x | 171.53x |
| next_activity_prediction | 122.18 ms | 95.92 ms | 25.24 ms | 1.27x | 4.84x |
| edge_bottleneck_ranking | 130.97 ms | 102.76 ms | 0.94 ms | 1.27x | 139.62x |

Geometric-mean speedup versus vanilla: **1.29x** for pg_ocpm + PM4Py and **27.43x** for pg_ocpm + ocpm-engine. Correctness: **4/4**.

### DFG concurrency

| Workers | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine | Engine/vanilla |
|---:|---:|---:|---:|---:|
| 1 | 8.05 req/s | 10.32 req/s | 40.34 req/s | 5.01x |
| 2 | 14.88 req/s | 20.17 req/s | 75.58 req/s | 5.08x |
| 4 | 27.18 req/s | 37.27 req/s | 125.88 req/s | 4.63x |
| 8 | 37.35 req/s | 60.39 req/s | 222.28 req/s | 5.95x |

### Isolated peak RSS

| Workload | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + ocpm-engine |
|---|---:|---:|---:|
| dfg_conformance_95pct | 181.0 MiB | 181.0 MiB | 36.3 MiB |
| variant_conformance_95pct | 180.2 MiB | 180.2 MiB | 36.4 MiB |
| next_activity_prediction | 181.0 MiB | 181.0 MiB | 36.3 MiB |
| edge_bottleneck_ranking | 183.9 MiB | 183.9 MiB | 36.4 MiB |

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
- The client package footprint reflects a clean 0.2.0 wheel rebuilt from the
  current package metadata; the metadata-only rename does not alter measured
  workload execution paths.
- Source: Zenodo DOI `10.5281/zenodo.8261133`, CC BY 4.0.
- PM4Py package licensing must be evaluated separately before product integration; installed metadata is retained in JSON.
