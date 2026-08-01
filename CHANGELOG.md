# Release notes

All notable changes to `ocpm-engine` are documented here. Dates use ISO 8601.

## 0.9.0 - 2026-07-31

- Added strict native decoding for `pg_ocpm 0.9.0` factorized event batches,
  preserving activity paths and packed case/timestamp vectors instead of
  expanding one PostgreSQL row and one Python object per event.
- Added incremental event-log summaries for variants, directly-follows
  frequency and mean duration, and activity case/occurrence/start/end
  frequencies. The Rust kernel operates on borrowed per-case views and releases
  Python's GIL during decoding and aggregation.
- Added capability-aware single-window and aligned multi-window adapters in
  Rust and Python. `pg_ocpm 0.9.0` uses one compact batch scan while
  `pg_ocpm 0.8.0` retains an exact ordered event-row compatibility fallback.
- Chunked requests larger than 256 windows transparently in both adapters while
  preserving the original global window order.
- Added transfer and expansion telemetry so callers can observe database row
  counts, packed payload bytes, and whether event rows were materialized.
- Made the Python event adapter consume DB-API cursors incrementally and feed
  compact rows to a persistent native accumulator in bounded 64-row chunks,
  avoiding whole-result Python lists and duplicate decoded-batch retention.
- Exposed borrowed materialized-column and factorized pair-group views for
  binding capsules, avoiding Cartesian row expansion when an algorithm can
  consume the encoded grouping directly.
- Added exact binding-index coverage inspection for object, activity, event,
  neighbor, and declared relation paths. Missing or malformed metadata fails
  closed so callers cannot mistake an undeclared path for covered.
- Pruned event-attribute decoding to base cases and materialized non-edge
  dynamic filters before lifecycle reconstruction, reducing work for selective
  dynamic queries without adding dataset- or benchmark-specific plans.
- Extended the SAP O2C/P2P three-way harness with a selectable factorized
  engine read path while retaining the aggregate-native path for workloads
  already expressible as sufficient statistics.

## 0.8.0 - 2026-07-20

- Published checksum-pinned SAP O2C/P2P current-versus-vanilla and strict OCPQ
  Q1-Q7 evidence for `pg_ocpm 0.8.0` plus `ocpm-engine 0.8.0`; private
  same-host release evidence remains ignored.
- Added the public `DynamicFilter` and `DynamicDfgRequest` contract so status,
  activity existence/nonexistence, case duration, arbitrary event attributes,
  related-object types, and directly-follows duration predicates compose
  through one engine API.
- Added exact included and excluded predicates for event attributes, related
  object types, and directly-follows edges. Multiple predicates compose with
  materialized set intersection and exclusion rather than endpoint-specific
  query fragments.
- Added a dual dynamic DFG strategy: ordinary filters aggregate finalized edge
  buckets, while edge predicates reuse `pg_ocpm`'s native event stream for both
  case selection and directly-follows aggregation.
- Materialized the bounded edge expansion once before joining dynamic case
  sets, preventing PostgreSQL row-estimation errors from selecting a nested
  loop that repeatedly decodes the same buckets.
- Added case-ID query plans and canonical DFG execution/scoring so callers and
  regression suites can verify exact selected-case parity independently of
  aggregate results.
- Raised the minimum supported extension version to `pg_ocpm 0.8.0`.

## 0.7.0 - 2026-07-20

- Added `PreparedEventLogQuery`, an asynchronous adapter over the storage-neutral
  native event stream introduced by `pg_ocpm 0.8.0`.
- Exposed PostgreSQL's `RowStream` directly so event-consuming algorithms can
  apply backpressure without allocating a complete event log in middleware.
- Kept aggregate-native DFG, variant, conformance, prediction, bottleneck, and
  drift paths unchanged; they continue to use bounded sufficient statistics
  and retain compatibility with `pg_ocpm 0.7.0`.

## 0.6.0 - 2026-07-19

- Replaced the withdrawn root-only OCPQ comparison with a strict all-node
  Q1-Q7 harness using OCPQ's primary zero-warmup, ten-evaluation protocol.
- Added prepared binding-tree requests that fetch, decode, and fully own every
  node from one PostgreSQL round trip, including duplicate bindings, exact
  violation reasons, and typed labels.
- Added generic two- through five-ID binding schemas for the complete
  `pg_ocpm 0.7.0` relation, temporal-pair, neighbor, and event-universe outputs.
- Added fresh-container-per-query latency, fresh-process peak-memory,
  relation-level storage, empty-result-cache, and exact pre/post 1/4/8/16-client
  concurrency evidence with fail-closed source and image provenance. Every
  concurrency request retains its integer-nanosecond latency and deterministic
  client/request/query identity so the checker independently recomputes tail
  percentiles.
- Encoded high-volume SAP concurrency samples losslessly as little-endian
  unsigned 64-bit integers with zlib and base64, and retained exact-answer hash
  histograms, so public artifacts remain practical without dropping evidence.
- Corrected the even-sample p50 estimator to average the two middle samples and
  labels ten-sample nearest-rank p95 as the maximum observation.
- Replaced single-batch SAP tail latency with three 30-round epochs, retaining
  every integer-nanosecond sample and realized arm-order code so release
  checkers can recompute pooled p50/p95 and report the epoch-p95 range.
- Added a matched SAP release bridge for `pg_ocpm 0.5.0` plus `ocpm-engine
  0.4.0` versus `pg_ocpm 0.7.0` plus `ocpm-engine 0.6.0`. The bridge uses
  version-isolated workers, an untimed vanilla oracle, exactly counterbalanced
  3x30 latency order, four fresh-process RSS samples per arm, duration-bounded
  concurrency epochs, and maintenance-stabilized relation-level storage across
  the native common-PM and PM4Py execution paths.
- Split public SAP benchmark provenance between the current clean controller
  checkout and clean, revision-locked `ocpm-engine 0.6.0` and `pg_ocpm 0.7.0`
  product worktrees, preventing later harness changes from silently changing
  the measured product.
- Removed the historical two-warmup, nine-sample SAP latency, memory, and
  storage values. Compact committed files retain only source, fixture, input,
  and answer contracts; all release non-regression gates use the matched bridge.
- Removed the obsolete root-only runner and certification paths so withdrawn
  artifacts cannot satisfy current preview or release gates.
- Raised the minimum supported extension version to `pg_ocpm 0.7.0`.

## 0.5.0 - 2026-07-19

- Prepared public SAP O2C/P2P release protocols and an initial OCPQ protocol;
  the latter was subsequently withdrawn because it materialized only root
  output while OCPQ materialized every evaluation-tree node.
- Added reusable prepared binding queries that validate a single non-null
  `bytea` result, reuse the PostgreSQL plan, decode compact capsules, and
  expose the encoded transfer size.
- Removed the intermediate owned `bytea` copy from prepared binding queries;
  callers can consume PostgreSQL's borrowed payload synchronously while the
  existing decoded-capsule API remains source compatible.
- Added decoding for three numeric identifiers plus a violation bit and a
  generic adapter for workload-declared, directed universal-equality relation
  constraints in `pg_ocpm 0.6.0`.
- Added an initial same-host OCPQ Q1-Q7 harness, superseded by the strict
  all-node protocol in 0.6.0.
- Added repeatable public SAP release gates; strict OCPQ gates arrive in 0.6.0.
- Raised SAP headline latency comparisons to ten warmups and 30 randomized,
  exactness-checked samples. The historical p50 values introduced here were
  subsequently withdrawn in 0.6.0.
- Hardened public Python concurrency gates with persistent per-worker
  connections, verified worker warmups, three duration-bounded epochs,
  per-worker request floors, exact request parity, and concurrency-only artifact
  refreshes that preserve latency, storage, and memory evidence.
- Removed public common-PM artifacts produced by the obsolete short-request
  concurrency protocol. The compact 0.4 contract introduced here was reduced
  to source, fixture, and workload identity in 0.6.0.
- Replaced the full historical SAP PM4Py staging result with a compact 0.4
  contract. Its performance values were subsequently removed in 0.6.0, leaving
  only source counts, fixture identity, exact answers, and input shapes.
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
- Added the initial OCPQ benchmark integration, later withdrawn and replaced by
  the strict all-node measurement and exact-output harness in 0.6.0.

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
