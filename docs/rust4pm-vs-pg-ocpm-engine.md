# Rust4PM versus pg_ocpm + ocpm-engine 0.10

Run date: 2026-08-03. Status: validated local full run, not publication-ready
because the ocpm-engine source tree contained the benchmark implementation as
uncommitted changes.

## Result

All four workload answers were exactly equal. On Rust4PM's upstream OCEL 2.0
P2P corpus, pg_ocpm + ocpm-engine had a 2.510x geometric-mean p50 serial
latency speedup. It also delivered 1.537x to 1.684x Rust4PM's
DFG-conformance throughput at every tested concurrency level.

This is an `ecosystem-common-pm` pair, not an extension of the strict OCPQ
Q1-Q7 benchmark. The Rust arm traverses Rust4PM's public `SlimLinkedOCEL` and
uses an independently implemented version of the four fixed common algorithms;
it does not reproduce Rust4PM's published Alpha+++ pipeline.

## Fixed input and protocol

- Input: Procure-To-Payment OCEL 2.0 corpus, DOI
  [10.5281/zenodo.8412920](https://doi.org/10.5281/zenodo.8412920), CC BY 4.0.
- SHA-256: `0017c34aeecdcb7712004d4364b11b372f2cc1a9cf2639ffe295f95a0df1ee74`.
- Size: 14,671 events, 9,543 objects, and a 13.1 MiB source SQLite file.
- Backbone: `goods receipt`, chosen by the suite's fixed data-dependent rule.
- Workloads: 95% DFG conformance, 95% variant conformance, next-activity
  prediction, and edge bottleneck ranking.
- Split: identical lifecycle-containment 80/20 windows; timestamp and external
  event ID define deterministic order.
- Serial timing: 10 warmups and three epochs of 30 measured requests.
- Concurrency: three DFG epochs at 1, 2, 4, and 8 workers, each lasting at least
  five seconds and completing at least 32 requests per worker.
- Correctness: canonical exact equality before timing, on every serial sample,
  and on every concurrency request.

The input contains 2,028 O2O rows that refer to absent objects. Both arms omit
those impossible relations; none of the four event-object lifecycle workloads
uses O2O.

## Steady-state latency

| Workload | Rust4PM p50 | Engine p50 | Engine speedup | Rust4PM p95 | Engine p95 |
|---|---:|---:|---:|---:|---:|
| DFG conformance | 1.134 ms | 0.414 ms | 2.739x | 2.037 ms | 0.559 ms |
| Variant conformance | 1.256 ms | 0.839 ms | 1.497x | 1.654 ms | 1.143 ms |
| Next-activity prediction | 1.139 ms | 0.389 ms | 2.928x | 1.503 ms | 0.485 ms |
| Edge bottleneck ranking | 1.094 ms | 0.331 ms | 3.305x | 1.357 ms | 0.449 ms |

## DFG concurrency

| Workers | Rust4PM QPS | Engine QPS | Engine/Rust4PM | Rust4PM p95 | Engine p95 |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,486.637 | 2,504.236 | 1.684x | 0.854 ms | 0.472 ms |
| 2 | 2,889.815 | 4,698.232 | 1.626x | 0.800 ms | 0.502 ms |
| 4 | 5,126.888 | 8,160.255 | 1.592x | 0.885 ms | 0.577 ms |
| 8 | 9,305.604 | 14,300.843 | 1.537x | 0.993 ms | 0.656 ms |

## Import, memory, and storage

Rust4PM's documented importer succeeded. Model load took 0.074 seconds and its
resident size after load was 55.0 MiB RSS. The engine client used 39.8 MiB RSS;
the pg_ocpm schema occupied 9.5 MiB, compared with 13.1 MiB for the immutable
source SQLite file.

The engine client PSS and PostgreSQL schema storage are recorded separately in
the JSON artifact. PostgreSQL server memory was not added to engine client
memory, so this report makes no total-deployment memory-win claim.

## Interpretation

The former concurrency deficit came from reconstructing 10,284 events in the
client for a result containing ten DFG edges. Version 0.10 instead requests an
exact lifecycle-aware sufficient statistic from pg_ocpm. This is a public,
parameterized multi-window API, also used by next-activity prediction; variant
and edge workloads use separate general aggregate APIs. These numbers still do
not predict arbitrary dynamic queries, other discovery or conformance
algorithms, or workloads requiring attributes outside those contracts.

Reproduce with `make perf-ecosystem-rust4pm`. The machine-readable result is
written to `.benchmarks/ecosystem-rust4pm-vs-pg-ocpm-engine-0.10.0.json`.
