# OCPQ data: four-way 1.0 comparison

The fixed BPIC 2017-derived OCPQ Q1-Q7 fixture was evaluated in Docker through
OCPQ 0.6.7, vanilla PostgreSQL 16 plus PM4Py 2.7.23.3, `pg_ocpm 1.0.0` plus
the same PM4Py evaluator, and `pg_ocpm 1.0.0` plus `ocpm-engine 1.0.0`.
Every arm reproduced all 13 nodes and all 380,083 duplicate-preserving
situations exactly.

| Query | OCPQ | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + engine |
|---|---:|---:|---:|---:|
| Q1 | 32.517 ms | 114.170 ms | 62.418 ms | 2.158 ms |
| Q2 | 46.192 ms | 157.132 ms | 104.876 ms | 3.589 ms |
| Q3 | 25.962 ms | 86.210 ms | 50.969 ms | 2.490 ms |
| Q4 | 49.905 ms | 195.558 ms | 149.940 ms | 3.444 ms |
| Q5 | 53.537 ms | 334.657 ms | 293.769 ms | 5.623 ms |
| Q6 | 105.727 ms | 56.782 ms | 37.887 ms | 2.813 ms |
| Q7 | 86.676 ms | 172.291 ms | 128.010 ms | 3.190 ms |
| **Geometric mean** | **51.453 ms** | **138.754 ms** | **95.278 ms** | **3.189 ms** |

The native engine is **16.137x** faster than OCPQ, **43.516x** faster than
vanilla PostgreSQL plus PM4Py, and **29.881x** faster than `pg_ocpm` plus
PM4Py by geometric mean. `pg_ocpm` improves the fixed PM4Py arm by **1.456x**.

## Strict native-path resources

The OCPQ-versus-native artifact passed every publication gate. Every latency,
memory, and concurrency answer matched every OCPQ node exactly.

| Clients | Throughput | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|
| 1 | 284.5 req/s | 2.971 ms | 7.372 ms | 8.409 ms |
| 4 | 892.4 req/s | 3.880 ms | 8.961 ms | 10.720 ms |
| 8 | 1,506.4 req/s | 4.513 ms | 10.847 ms | 14.346 ms |
| 16 | 2,124.6 req/s | 6.361 ms | 15.661 ms | 21.197 ms |

- Maximum client peak above baseline RSS: **6.25 MiB**.
- Maximum owned result-tree allocation: **3.99 MiB**.
- Serving storage: **109.91 MiB**, including **9.97 MiB** of indexes and
  **5.87 MiB** of binding summaries.
- Request result cache: disabled, with zero cached rows.

## Interpretation boundary

PM4Py does not implement OCPQ evaluation trees. The two PM4Py arms therefore
use the same explicit Pandas evaluator over a complete resident PM4Py OCEL.
The four-way checker labels this table `verified_descriptive_preview` because
the PM4Py artifacts omit the strict cross-arm host identifier and use a
different evaluator boundary. The independent OCPQ-versus-native artifact is
publication-ready and supplies the strict latency, memory, storage,
concurrency, correctness, image, and revision gates.

These results do not imply the same ratio for arbitrary dynamic OCEL queries.
The SAP and ecosystem suites separately test general provider aggregates,
prediction, conformance, bottleneck analysis, and concurrency on different
datasets.

## Clean-room boundary

OCPQ and PM4Py are isolated benchmark arms and exact-output oracles only. No
competitor source was inspected, copied, translated, or used to select product
algorithms. Product implementation choices come only from the peer-reviewed
papers listed in [academic implementation provenance](academic-implementation-provenance.md).

## Evidence pins

- `pg_ocpm`: `44e725f0fd7bf29beab18f143c21303e148386e4`
- `ocpm-engine`: `891c7de2857c0d60125d976caa753fb3a9e2521f`
- OCPQ: `80457e561edd7bb9e142d959dd7e0f96e6b03f2f`
- strict OCPQ reference SHA-256: `1c8d68b117ecc530637772e5c70c22ee039b52e7043caa68de7d497905c87611`
- strict native artifact SHA-256: `3dcac48e5070187f281d89ff093b791dd09df8d2bf111a1f5350f1c415081c0d`
- vanilla PG + PM4Py SHA-256: `2adb7ac8041a05dab797f013d6bcf0f8eb0fad9644c0a376a02f4117661cf6d5`
- `pg_ocpm` + PM4Py SHA-256: `12c2a8e88a654afe1d092dc132de59a3cd4fa2b74b72bff901eb203e6eaec30c`
- PM4Py evaluator SHA-256: `a431ed3ac827fb86011ddda60c33aeaa9d304a7836800168750cb923ef925486`
