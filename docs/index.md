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

## Start here

- [Installation guide](https://github.com/verticalbarHQ/ocpm-engine/blob/main/INSTALL.md):
  private wheel registry, source builds, DuckDB requirement, and PostgreSQL
  setup with pg_ocpm
- [API reference](api-reference.md): every public module and function with
  example usages
- [README](https://github.com/verticalbarHQ/ocpm-engine#readme): API examples
  for the standalone facade, compatibility API, and dynamic filters
- Benchmarks: start with the
  [OCPQ four-way comparison](ocpq-1.0-four-way-comparison.md) and the
  [SAP O2C/P2P four-way comparison](duckdb-sap-pm4py-four-way-performance.md)

## License

Copyright 2026 [Vertical Bar, Inc.](https://vertical.bar) Licensed under the
Apache License, Version 2.0.
