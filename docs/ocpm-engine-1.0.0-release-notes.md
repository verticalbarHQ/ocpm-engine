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

## Validation required for publication

Release claims require a clean revision, installed-wheel tests, exact-answer
OCPQ gates, separate Rust4PM and OCPA same-dataset pairs, and SAP O2C/P2P
three-way Docker results covering latency, concurrency, peak memory, and
storage. Preview artifacts from dirty trees must remain labeled preview.
