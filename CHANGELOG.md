# Release notes

All notable changes to `ocpm-engine` are documented here. Dates use ISO 8601.

## Unreleased

## 0.4.0 - 2026-07-18

- Added generic native decoding for `pg_ocpm 0.5.0` binding-result capsules,
  including dictionary-coded labels and lazy factorized related-object pairs.
- Added asynchronous PostgreSQL adapters for cardinality, required-activity,
  eventually-follows, actor-equality, maximum-delay, and pair-binding
  operations.
- Added Python capsule metadata and row-decoding APIs that release the GIL
  during native decoding.
- Raised the minimum supported extension version to `pg_ocpm 0.5.0`.
- Added a reproducible public OCPQ comparison with published and pinned
  same-host OCPQ results, correctness, latency, concurrency, memory, and
  storage gates.

## 0.3.0 - 2026-07-18

- Added bounded, explainable Jensen-Shannon drift scoring for any labeled DFG,
  variant, or activity-frequency distribution, with native Rust execution and
  GIL-releasing Python bindings.
- Added an asynchronous adapter for the storage-neutral `pg_ocpm 0.4.0`
  activity profile and raised the minimum supported extension version.
- Added a licensed-project capability survey, recent-research roadmap, and
  placement rationale for database versus middleware algorithms.
- Expanded the public SAP benchmark with correctness-gated activity-profile
  and DFG-drift workloads, concurrency, and storage accounting.

- Prevented test-only transitions with zero training frequency from creating
  next-activity model predictions.
- Added a correctness-gated SAP O2C/P2P comparison of vanilla PostgreSQL with
  PM4Py, `pg_ocpm` with PM4Py, and `pg_ocpm` with ocpm-engine.
- Removed data-source and application-specific benchmark artifacts and naming
  from the public engine; public comparisons retain the vanilla PostgreSQL arm.
- Renamed project distribution metadata to `ocpm-engine` and removed
  organization-specific authorship, URLs, and public-facing prose.

## 0.2.0 - 2026-07-18

- Added a Rust 2024 core for deterministic DFG conformance, variant
  conformance, next-activity prediction, and bottleneck ranking.
- Added stable-ABI Python 3.11+ bindings while retaining the existing Python
  request-planning API.
- Added an asynchronous PostgreSQL adapter for compact `pg_ocpm 0.3.0`
  aggregate results.
- Added aligned multi-window DFG and variant adapters for one-request model
  training, evaluation, period comparison, and drift analysis.
- Raised the minimum supported extension version to `pg_ocpm 0.3.0`.
- Added public-data latency, storage, and concurrency regression harnesses.
- Added mandatory dated release notes and a CI guard that rejects package,
  Cargo, native-module, lockfile, or changelog version drift.

## 0.1.0 - 2026-07-17

- Added parameterized query planning for common process-mining read paths over
  `pg_ocpm`.
