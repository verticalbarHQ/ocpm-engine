# Rust4PM vs pg_ocpm + ocpm-engine 1.0

## Result

All 4 answer cells passed exact equality. On `rust4pm_p2p` and the four fixed workloads, pg_ocpm + ocpm-engine had a 2.160x geometric-mean p50 speedup over Rust4PM.

This is the separate `ecosystem-common-pm` suite. It does not add Rust4PM or OCPA cells to the strict OCPQ Q1-Q7 benchmark.

Publication status: ready.

The engine had higher DFG-conformance throughput at every measured concurrency level and delivered 1.287x to 1.542x Rust4PM throughput.

## Fixed contract

- Dataset: Procure-To-Payment (P2P) Object-centric Event Log in OCEL 2.0 Standard (`10.5281/zenodo.8412920`), the upstream Rust4PM corpus, SHA-256 `0017c34aeecdcb7712004d4364b11b372f2cc1a9cf2639ffe295f95a0df1ee74`.
- Source license/terms: CC BY 4.0.
- Backbone: `goods receipt`, selected by the fixed rule: maximum count of object lifecycles containing at least two events; then maximum event-object link count; then lexical object-type name.
- Workloads: 95% DFG conformance, 95% variant conformance, next-activity prediction, and edge bottleneck ranking.
- Split: lifecycle-containment 80/20 windows, identical for both arms.
- Event order: event timestamp, then external event ID.
- Invalid O2O rows excluded from PostgreSQL normalization: 2028; O2O is outside the fixed workloads.
- Latency: 10 warmups, 3 epochs of 30 measured requests, monotonic nanosecond clock.
- Concurrency: DFG conformance at 1/2/4/8 workers, 3 epochs, at least 5 seconds and 32 requests per worker.
- Publication gate: exact canonical answer equality for preflight, every serial sample, and every concurrency request.

## Correctness

| Dataset | Workload | Exact | Answer SHA-256 |
|---|---|---:|---|
| rust4pm_p2p | dfg_conformance_95pct | yes | `60ab48203c1517581d04b17d4b025b33dab3a5567f2a2f88a20d8d3e3ff8c7d9` |
| rust4pm_p2p | variant_conformance_95pct | yes | `1c911590ca4d2aed672b2b8e0b0680713dcda71dbc6e20ef89eac80bcf8227f1` |
| rust4pm_p2p | next_activity_prediction | yes | `a8ea57fdec0c3e86ea487f5a71ef024e099f3bfae3e69d2dae790808e314c8b8` |
| rust4pm_p2p | edge_bottleneck_ranking | yes | `4de113819ac516dd65201c03400cf132dc50f02a66cc0e25eebc632165e6b444` |

## Steady-state latency

| Dataset | Workload | Rust4PM p50 | Engine p50 | Engine speedup | Rust4PM p95 | Engine p95 |
|---|---|---:|---:|---:|---:|---:|
| rust4pm_p2p | dfg_conformance_95pct | 2.049 ms | 0.797 ms | 2.571x | 3.396 ms | 1.227 ms |
| rust4pm_p2p | variant_conformance_95pct | 2.175 ms | 1.658 ms | 1.312x | 3.507 ms | 2.019 ms |
| rust4pm_p2p | next_activity_prediction | 1.926 ms | 0.785 ms | 2.454x | 3.125 ms | 1.000 ms |
| rust4pm_p2p | edge_bottleneck_ranking | 1.816 ms | 0.691 ms | 2.628x | 3.234 ms | 0.965 ms |

## Concurrency: DFG conformance

| Dataset | Workers | Rust4PM QPS | Engine QPS | Engine throughput ratio | Rust4PM p95 | Engine p95 |
|---|---:|---:|---:|---:|---:|---:|
| rust4pm_p2p | 1 | 894.949 | 1162.355 | 1.299x | 1.539 ms | 1.145 ms |
| rust4pm_p2p | 2 | 1948.365 | 2506.724 | 1.287x | 1.311 ms | 1.044 ms |
| rust4pm_p2p | 4 | 3366.676 | 5053.932 | 1.501x | 1.468 ms | 1.005 ms |
| rust4pm_p2p | 8 | 6195.718 | 9555.875 | 1.542x | 1.637 ms | 1.077 ms |

## Import, resident memory, and storage

| Dataset | Rust4PM native importer | Model load | Rust4PM resident after load | Source SQLite |
|---|---|---:|---:|---:|
| rust4pm_p2p | pass | 0.105 s | 51.6 MiB | 13.1 MiB |

The pg_ocpm schema uses 9.7 MiB for this loaded dataset. The competitor source-file sizes above are immutable input storage and do not include resident model expansion. Package and binary sizes are retained in the JSON artifact.

Concurrency memory is reported in each raw epoch. OCPA reports summed worker RSS/PSS; Rust4PM reports the shared threaded process RSS/PSS; the engine artifact reports isolated client-worker RSS and keeps PostgreSQL storage/environment separately.
The report therefore does not claim a total-deployment memory win: PostgreSQL server memory is not added to the engine client RSS, while the in-process competitor arms include their loaded model.

## Native importer capability

- rust4pm_p2p: documented importer succeeded.

The measured competitor arm uses the project's public native data model. Its per-dataset adapter field records whether that model was created by the documented importer or the disclosed setup repair. Neither route precomputes a DFG, variant table, score, expected answer, or benchmark-specific index.

## Interpretation boundaries

- This is a shared workload benchmark, not OCPQ Q1-Q7. The strict OCPQ benchmark remains a separate artifact.
- Steady-state latency excludes one-time OCEL import, PostgreSQL fixture preparation, process startup, and worker startup; those costs and resident memory are reported separately.
- Each ecosystem uses its normal scalable service model: Rust4PM shares an immutable log across threads, OCPA uses preloaded forked processes, and ocpm-engine uses process workers with persistent PostgreSQL connections. Concurrency memory is therefore architectural, not a same-runtime microbenchmark.
- Each pair uses that competitor project's own upstream OCEL 2.0 dataset. The Rust4PM and OCPA reports therefore must not be compared as if they used the same input data.
- Native import is probed on the unchanged source and reported separately from steady-state execution. If an upstream importer fails, a disclosed benchmark-owned setup adapter may construct the project's public native model; such a result is not a native-import performance comparison.
- The competitor arms traverse each project's public native OCEL model and execute independently implemented versions of the four fixed common algorithms. They do not measure Rust4PM's Alpha+++ pipeline, OCPA's complete algorithm catalog, or a paper-specific end-to-end benchmark.
- The source contains 2028 O2O rows referencing objects absent from its object table. PostgreSQL loading excludes those impossible relations; none of the four event-object lifecycle workloads uses O2O.

The exact numbers describe these fixed analytical workloads and service models. They do not imply the same ratio for arbitrary dynamic OCEL queries, discovery algorithms, conformance techniques, or workloads that use attributes omitted by the common contract.

## Clean-room boundary

Rust4PM is compiled only inside the pinned benchmark image and exercised through its released public interface. No Rust4PM source was inspected, copied, translated, or used to choose product algorithms. Product implementation choices come only from the peer-reviewed papers listed in [academic implementation provenance](academic-implementation-provenance.md).
