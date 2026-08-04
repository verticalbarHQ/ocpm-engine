# OCPA vs pg_ocpm + ocpm-engine 1.0

## Result

All 4 answer cells passed exact equality. On `ocpa_running_example` and the four fixed workloads, pg_ocpm + ocpm-engine had a 84.013x geometric-mean p50 speedup over OCPA.

Publication status: not ready (OCPA native import did not succeed).

The OCPA query result is adapter-assisted because OCPA's documented native importer fails on this unchanged upstream file. It is not an OCPA native-import performance result.

## Fixed contract

- Dataset: OCPA OCEL 2.0 running example (`ocpm/ocpa@de056e0203a3fa4a9bbc19a95e001eada323074a:sample_logs/ocel2/sqlite/running-example.sqlite`), the upstream OCPA corpus, SHA-256 `019202ee793cbd71c80636ca10d78f9701e83f3696ca818c52cc76f38d2bd38d`.
- Source license/terms: OCPA 1.3.4 wheel contains GPL-3.0; dataset-specific terms are not stated separately.
- Backbone: `orders`, selected by the fixed rule: maximum count of object lifecycles containing at least two events; then maximum event-object link count; then lexical object-type name.
- Workloads: 95% DFG conformance, 95% variant conformance, next-activity prediction, and edge bottleneck ranking.
- Split: lifecycle-containment 80/20 windows, identical for both arms.
- Event order: event timestamp, then external event ID.
- Invalid O2O rows excluded from PostgreSQL normalization: 0; O2O is outside the fixed workloads.
- Latency: 10 warmups, 3 epochs of 30 measured requests, monotonic nanosecond clock.
- Concurrency: DFG conformance at 1/2/4/8 workers, 3 epochs, at least 5 seconds and 32 requests per worker.
- Publication gate: exact canonical answer equality for preflight, every serial sample, and every concurrency request.

## Correctness

| Dataset | Workload | Exact | Answer SHA-256 |
|---|---|---:|---|
| ocpa_running_example | dfg_conformance_95pct | yes | `6d1597bda1ee4df597f7d0d7c2dcaafdd32552c0290ea422bef92594373cd118` |
| ocpa_running_example | variant_conformance_95pct | yes | `80cd775e0a756938d2ce7f6c48bed2fb26d3c4a933b64ba1723e9612bac740bd` |
| ocpa_running_example | next_activity_prediction | yes | `0d11e08bea5189f4b9bd8d8b63a71840edef2dc8b0a3d120c9a08d1d20e5739d` |
| ocpa_running_example | edge_bottleneck_ranking | yes | `34ecb123e6a6270bca1ed9f302917df1347966c0562f4998a71c7740e3dc1528` |

## Steady-state latency

| Dataset | Workload | OCPA p50 | Engine p50 | Engine speedup | OCPA p95 | Engine p95 |
|---|---|---:|---:|---:|---:|---:|
| ocpa_running_example | dfg_conformance_95pct | 11.316 ms | 0.112 ms | 101.036x | 91.486 ms | 0.228 ms |
| ocpa_running_example | variant_conformance_95pct | 13.804 ms | 0.171 ms | 80.725x | 99.971 ms | 0.277 ms |
| ocpa_running_example | next_activity_prediction | 10.760 ms | 0.107 ms | 100.561x | 94.966 ms | 0.126 ms |
| ocpa_running_example | edge_bottleneck_ranking | 15.975 ms | 0.263 ms | 60.741x | 100.318 ms | 0.525 ms |

## Concurrency: DFG conformance

| Dataset | Workers | OCPA QPS | Engine QPS | Engine throughput ratio | OCPA p95 | Engine p95 |
|---|---:|---:|---:|---:|---:|---:|
| ocpa_running_example | 1 | 60.659 | 6487.222 | 106.946x | 94.673 ms | 0.296 ms |
| ocpa_running_example | 2 | 110.238 | 12982.173 | 117.765x | 103.317 ms | 0.266 ms |
| ocpa_running_example | 4 | 195.678 | 21397.852 | 109.352x | 111.922 ms | 0.306 ms |
| ocpa_running_example | 8 | 333.734 | 36872.227 | 110.484x | 122.830 ms | 0.343 ms |

## Import, resident memory, and storage

| Dataset | OCPA native importer | Model load | OCPA resident after load | Source SQLite |
|---|---|---:|---:|---:|
| ocpa_running_example | fail: ValueError | 0.974 s | 185.5 MiB | 3.6 MiB |

The pg_ocpm schema uses 8.9 MiB for this loaded dataset. The competitor source-file sizes above are immutable input storage and do not include resident model expansion. Package and binary sizes are retained in the JSON artifact.

Concurrency memory is reported in each raw epoch. OCPA reports summed worker RSS/PSS; Rust4PM reports the shared threaded process RSS/PSS; the engine artifact reports isolated client-worker RSS and keeps PostgreSQL storage/environment separately.
The report therefore does not claim a total-deployment memory win: PostgreSQL server memory is not added to the engine client RSS, while the in-process competitor arms include their loaded model.

## Native importer capability

- ocpa_running_example: documented importer failed with `ValueError`: `ValueError: Sample larger than population or is negative`

The measured competitor arm uses the project's public native data model. Its per-dataset adapter field records whether that model was created by the documented importer or the disclosed setup repair. Neither route precomputes a DFG, variant table, score, expected answer, or benchmark-specific index.

## Interpretation boundaries

- Steady-state latency excludes one-time OCEL import, PostgreSQL fixture preparation, process startup, and worker startup; those costs and resident memory are reported separately.
- Each ecosystem uses its normal scalable service model: Rust4PM shares an immutable log across threads, OCPA uses preloaded forked processes, and ocpm-engine uses process workers with persistent PostgreSQL connections. Concurrency memory is therefore architectural, not a same-runtime microbenchmark.
- Each pair uses that competitor project's own upstream OCEL 2.0 dataset. The Rust4PM and OCPA reports therefore must not be compared as if they used the same input data.
- Native import is probed on the unchanged source and reported separately from steady-state execution. If an upstream importer fails, a disclosed benchmark-owned setup adapter may construct the project's public native model; such a result is not a native-import performance comparison.
- The competitor arms traverse each project's public native OCEL model and execute independently implemented versions of the four fixed common algorithms. They do not measure Rust4PM's Alpha+++ pipeline, OCPA's complete algorithm catalog, or a paper-specific end-to-end benchmark.

The exact numbers describe these fixed analytical workloads and service models. They do not imply the same ratio for arbitrary dynamic OCEL queries, discovery algorithms, conformance techniques, or workloads that use attributes omitted by the common contract.

## Clean-room boundary

OCPA is installed only in the pinned benchmark image and exercised through its released public interface. No OCPA source was inspected, copied, translated, or used to choose product algorithms. Product implementation choices come only from the peer-reviewed papers listed in [academic implementation provenance](academic-implementation-provenance.md).
