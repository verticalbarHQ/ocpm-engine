# Release notes

All notable changes to `ocpm-engine` are documented here. Dates use ISO 8601.

## 0.5.0 - 2026-07-19

- Prepared the corrected public SAP O2C/P2P and OCPQ protocols for clean
  `ocpm-engine 0.5.0` and `pg_ocpm 0.6.0` release measurements.
- Added reusable prepared binding queries that validate a single non-null
  `bytea` result, reuse the PostgreSQL plan, decode compact capsules, and
  expose the encoded transfer size.
- Removed the intermediate owned `bytea` copy from prepared binding queries;
  callers can consume PostgreSQL's borrowed payload synchronously while the
  existing decoded-capsule API remains source compatible.
- Added decoding for three numeric identifiers plus a violation bit and a
  generic adapter for workload-declared, directed universal-equality relation
  constraints in `pg_ocpm 0.6.0`.
- Replaced the invalid OCPQ comparison with a same-host Q1-Q7 harness that uses
  OCPQ's evaluation boundary, performs complete owned-row materialization,
  requires exact external-ID output parity, and records latency, storage,
  fresh-process memory, and 1/4/8/16-client concurrency.
- Added repeatable public OCPQ and SAP release gates.
- Raised every public headline latency comparison to ten warmups and 30
  randomized, exactness-checked samples while retaining the historical p50
  values only as regression baselines.
- Hardened public Python concurrency gates with persistent per-worker
  connections, verified worker warmups, three duration-bounded epochs,
  per-worker request floors, exact request parity, and concurrency-only artifact
  refreshes that preserve latency, storage, and memory evidence.
- Removed public common-PM artifacts produced by the obsolete short-request
  concurrency protocol. The 0.4 fixture, p50 latency, and storage regression
  limits now live in a compact baseline that contains no concurrency fields.
- Replaced the full historical SAP PM4Py staging result with a compact 0.4
  regression baseline containing only comparable p50 latency, isolated RSS,
  fixture/environment identity, and index/total storage evidence; raw samples
  and obsolete concurrency measurements are not retained.
- Raised the minimum supported extension version to `pg_ocpm 0.6.0`.

## 0.4.0 - 2026-07-18

- Added generic native decoding for `pg_ocpm 0.5.0` binding-result capsules,
  including dictionary-coded labels and lazy factorized related-object pairs.
- Added asynchronous PostgreSQL adapters for cardinality, required-activity,
  eventually-follows, actor-equality, maximum-delay, and pair-binding
  operations.
- Added Python capsule metadata and row-decoding APIs that release the GIL
  during native decoding.
- Raised the minimum supported extension version to `pg_ocpm 0.5.0`.
- Added the initial OCPQ benchmark integration, superseded by the corrected
  measurement and exact-output harness in 0.5.0.

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
