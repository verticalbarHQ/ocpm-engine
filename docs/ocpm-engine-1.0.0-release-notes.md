# ocpm-engine 1.0.0 release notes

Version 1.0 makes `ocpm-engine` a standalone object-centric process-mining
library. PostgreSQL is optional. When `pg_ocpm 1.0.0` is available, the same
typed requests can use data-local scans and sufficient-statistic pushdown.

## Main additions

- Source-neutral local and PostgreSQL providers with capability negotiation.
- Canonical events, objects, E2O/O2O relations, attribute history, typed views,
  content hashes, source watermarks, and atomic bounded append batches.
- Typed object-centric queries; leading-object and connected executions.
- DFG/OC-DFG, Alpha, process-tree, Petri-net, OCPN, and declarative discovery.
- Frequency, replay, bounded alignment, OCPN, declarative, and constraint
  conformance.
- Process-map, timeline, histogram, lifecycle-performance, organizational,
  bottleneck, comparison-window, and drift enhancement.
- Next-activity, remaining-time, and outcome prediction with temporal holdout
  evaluation and feature-provenance metadata.
- Canonical/OCEL JSON, CSV, XES, and SQLite I/O; JSON, DOT, PNML, and SVG model
  serialization; Rust and Python entry points.
- Capability-aware routing is the default for release benchmarks and service
  integrations; legacy expanded SQL remains diagnostic-only.

## Clean-room and compatibility boundary

All algorithm modules name their peer-reviewed DOI basis. No implementation is
ported, translated, or inferred from OCPQ, Rust4PM, OCPA, PM4Py, ProM, or other
process-mining source code. Those systems are isolated black-box benchmark arms.

OCEL 2.0 XML is not claimed by this release. Its detailed interchange syntax is
outside the currently admitted peer-reviewed evidence, so support is deferred
rather than reconstructed from another implementation.

The older PostgreSQL planner remains supported through the 1.x compatibility
window. New applications should use `StandaloneEngine` and attach the optional
PostgreSQL provider when data-local execution is desired.

## Release validation and benchmark evidence

The 1.0 implementation passed the complete Rust workspace suite, 236 installed
Python tests with one intentional live-database skip, and the PostgreSQL 16
clean-install and full upgrade chain through 1.0.0. The release benchmarks run
in isolated, pinned Docker environments and require exact canonical answers
before latency or throughput results are accepted.

- Strict OCPQ Q1-Q7: every evaluation-tree node matched OCPQ 0.6.7 exactly.
  `pg_ocpm 1.0.0` plus `ocpm-engine 1.0.0` was 16.137x faster by same-host
  geometric-mean latency, with a 9.522x minimum per-query speedup.
- SAP O2C/P2P: all eight common-process-mining answers matched the fixed PM4Py
  oracle. The engine was 55.529x faster than vanilla PostgreSQL plus PM4Py by
  geometric-mean p50; `pg_ocpm` plus the unchanged PM4Py evaluator was 1.338x
  faster than vanilla.
- Rust4PM pair: all four common-workload answers matched exactly. The engine
  was 2.160x faster by geometric-mean p50 and delivered 1.287x to 1.542x DFG
  throughput at 1/2/4/8 workers on Rust4PM's P2P corpus.
- OCPA pair: all four common-workload answers matched and the descriptive
  steady-state result was 84.013x faster by geometric-mean p50. This pair is
  not publication-ready because OCPA 1.3.4's documented native importer failed
  on its unchanged upstream example; the disclosed adapter-assisted result is
  not a native-import comparison.

These fixed-workload ratios are not predictions for arbitrary dynamic queries,
different discovery/conformance algorithms, or end-to-end deployment memory.
PostgreSQL server memory is reported separately from client RSS. See the
[strict OCPQ report](ocpq-performance.md),
[four-way OCPQ comparison](ocpq-1.0-four-way-comparison.md),
[SAP report](sap-pm4py-three-way-performance.md),
[Rust4PM pair](rust4pm-vs-pg-ocpm-engine.md), and
[OCPA pair](ocpa-vs-pg-ocpm-engine.md) for the complete boundaries.
