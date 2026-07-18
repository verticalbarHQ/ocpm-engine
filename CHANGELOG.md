# Release notes

All notable changes to `ocpm-engine` are documented here. Dates use ISO 8601.

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

- Added parameterized query planning for the Vertical Bar process-mining read
  paths over `pg_ocpm`.
- Added Goodr correctness-gated performance regression suites.
