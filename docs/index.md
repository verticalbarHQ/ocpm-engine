# ocpm-engine

`ocpm-engine` is a standalone, Rust-first object-centric process-mining engine.
It loads canonical or OCEL JSON, XES, CSV, and SQLite data directly, then
queries, discovers, checks conformance, enhances, predicts, and serializes
models without a database or Python dataframe runtime, exposing Python 3.11+
stable-ABI bindings that release the GIL during native work.

- **Standalone**: typed filtering and binding queries; DFG, OC-DFG, Alpha,
  process-tree, Petri-net, OCPN, and declarative discovery; conformance,
  enhancement, drift, and prediction with temporal holdout evaluation.
- **PostgreSQL provider**: with
  [pg_ocpm](https://github.com/verticalbarHQ/pg_ocpm), selective scans and
  exact sufficient-statistic aggregation run inside PostgreSQL while the same
  source-neutral Rust kernels construct and score models.
- **DuckDB Parquet provider**: opens an existing deployment-supplied DuckDB
  catalog over immutable local or S3 Parquet snapshots; DuckDB is never
  bundled or embedded.
- **Exactness first**: optimized and fallback paths are tested for canonical
  equality, and benchmark evidence passes exact-answer hash gates before
  publication.

## Why ocpm-engine is different

Typical process-mining stacks materialize a full event log in a Python
dataframe runtime before any analysis runs, and every query pays that
hydration cost again. `ocpm-engine` is built around the opposite contract:

- **Exact, or it does not ship.** Optimized and fallback paths are tested
  for canonical equality, and every published benchmark latency passes an
  exact-answer hash gate against the competing implementation. There is no
  sampling or approximation mode.
- **Statistics move, events stay put.** Sufficient statistics and
  factorized batches cross the wire instead of event rows, so conformance,
  prediction, bottleneck, and drift scoring run in megabytes of incremental
  memory where dataframe stacks need hundreds.
- **Source-neutral kernels.** The same Rust kernels serve local files,
  PostgreSQL pushdown through pg_ocpm, and deployment-supplied DuckDB
  Parquet catalogs, so results are identical across providers and the
  engine never bundles or operates a database service.
- **Built for serving, not scripts.** Capability negotiation, parameterized
  SQL planning, bounded working memory, tested concurrency, and stable-ABI
  wheels that release the GIL make it an application read path rather than
  a notebook dependency.

## Benchmarks at a glance

Every latency cell behind these headlines reproduced the competing arm's
answers exactly; publication-gate strictness and full caveats are on each
page. DuckDB figures marked cached use the bounded exact-result cache, and
cache-off latencies are reported on each page.

| Comparison | Headline result |
|---|---|
| [OCPQ Q1-Q7, strict protocol](ocpq-performance.md) | 16.1x geometric-mean speedup over OCPQ 0.6.7, minimum 9.5x per query, exact node parity, 2,124 req/s at 16 clients in under 7 MiB of client peak memory |
| [OCPQ fixture, four-way](ocpq-1.0-four-way-comparison.md) | 3.2 ms geometric-mean latency vs 51.5 ms for OCPQ and 138.8 ms for vanilla PostgreSQL plus PM4Py (43.5x) |
| [SAP O2C/P2P, four-way with DuckDB](duckdb-sap-pm4py-four-way-performance.md) | 9x to 206x faster p50 than vanilla PostgreSQL plus PM4Py across conformance, prediction, and bottleneck workloads, with about 6 MiB of incremental memory instead of about 166 MiB; the cached DuckDB arm reaches 35,197 QPS at 8 workers |
| [Rust4PM](duckdb-vs-rust4pm-performance.md) | 3.0x p50 geometric mean through pg_ocpm and 12.2x through cached DuckDB Parquet, exact on all workloads |
| [OCPA](duckdb-vs-ocpa-performance.md) | 70x p50 geometric mean through pg_ocpm and 339x through cached DuckDB Parquet (descriptive gate) |

## Start here

- [Installation guide](https://github.com/verticalbarHQ/ocpm-engine/blob/main/INSTALL.md):
  private wheel registry, source builds, DuckDB requirement, and PostgreSQL
  setup with pg_ocpm
- [Module reference](api-reference.md): every public module with API and
  function specs and example usages
- [README](https://github.com/verticalbarHQ/ocpm-engine#readme): API examples
  for the standalone facade, compatibility API, and dynamic filters

## License

Copyright 2026 [Vertical Bar, Inc.](https://vertical.bar) Licensed under the
Apache License, Version 2.0.
