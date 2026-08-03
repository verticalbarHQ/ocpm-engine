# OCPA versus pg_ocpm + ocpm-engine 0.10

Run date: 2026-08-03. Status: validated local full run, not publication-ready.
The ocpm-engine source tree contained the benchmark implementation as
uncommitted changes, and OCPA's documented importer did not succeed.

## Result

All four workload answers were exactly equal. On OCPA's upstream OCEL 2.0
running example, pg_ocpm + ocpm-engine had a 73.651x geometric-mean p50 serial
latency speedup and 116.151x to 128.205x its DFG throughput.

This result is adapter-assisted. OCPA 1.3.4's documented SQLite importer fails
on the unchanged upstream file with `ValueError: Sample larger than population
or is negative`. The untimed benchmark setup therefore constructs OCPA's public
`ObjectCentricEventLog` without precomputing DFGs, variants, scores, answers, or
benchmark-specific indexes. This is a valid comparison of the four
steady-state computations over OCPA's model, but it is not a native-import
performance result and does not satisfy the publication gate.

This is an `ecosystem-common-pm` pair, not an extension of the strict OCPQ
Q1-Q7 benchmark. The OCPA arm traverses its public object-centric log and uses
the benchmark-defined versions of the four fixed common algorithms; it does
not measure OCPA's complete algorithm catalog.

## Fixed input and protocol

- Input: OCPA's checked-in OCEL 2.0
  `sample_logs/ocel2/sqlite/running-example.sqlite` at commit
  `de056e0203a3fa4a9bbc19a95e001eada323074a`.
- Terms: the OCPA 1.3.4 wheel contains GPL-3.0; no separate dataset terms were
  found, so the benchmark downloads but does not redistribute the input.
- SHA-256: `019202ee793cbd71c80636ca10d78f9701e83f3696ca818c52cc76f38d2bd38d`.
- Size: 18,211 events, 10,011 objects, and a 3.6 MiB source SQLite file.
- Backbone: `orders`, chosen by the suite's fixed data-dependent rule.
- Workloads, lifecycle split, ordering, timing, concurrency, and exactness
  gates are identical to the Rust4PM pair.

## Steady-state latency

| Workload | OCPA p50 | Engine p50 | Engine speedup | OCPA p95 | Engine p95 |
|---|---:|---:|---:|---:|---:|
| DFG conformance | 7.515 ms | 0.084 ms | 89.464x | 47.942 ms | 0.137 ms |
| Variant conformance | 8.538 ms | 0.123 ms | 69.415x | 48.802 ms | 0.152 ms |
| Next-activity prediction | 7.611 ms | 0.086 ms | 88.500x | 52.651 ms | 0.122 ms |
| Edge bottleneck ranking | 10.119 ms | 0.189 ms | 53.540x | 51.842 ms | 0.208 ms |

## DFG concurrency

| Workers | OCPA QPS | Engine QPS | Engine/OCPA | OCPA p95 | Engine p95 |
|---:|---:|---:|---:|---:|---:|
| 1 | 98.725 | 11,467.056 | 116.151x | 49.759 ms | 0.096 ms |
| 2 | 168.777 | 21,638.042 | 128.205x | 66.072 ms | 0.110 ms |
| 4 | 268.652 | 33,004.633 | 122.853x | 84.073 ms | 0.176 ms |
| 8 | 483.518 | 57,896.002 | 119.739x | 93.835 ms | 0.191 ms |

## Import, memory, and storage

The documented OCPA importer failed before model construction. The disclosed
adapter loaded the public native model in 0.739 seconds; peak RSS during load
was 203.8 MiB. The engine client used 43.2 MiB RSS and the pg_ocpm schema
occupied 8.6 MiB, compared with 3.6 MiB
for the immutable source SQLite file, so pg_ocpm did not win source storage on
this small input.

The engine client PSS and PostgreSQL schema storage are recorded separately in
the JSON artifact. PostgreSQL server memory was not added to engine client
memory, so this report makes no total-deployment memory-win claim.

## Interpretation

The large gap is credible for these four repeated, preloaded analytical
computations, but it cannot be generalized to all OCPA algorithms or arbitrary
dynamic queries. It also cannot be presented as a clean native OCPA end-to-end
result until upstream import succeeds without the adapter.

Reproduce with `make perf-ecosystem-ocpa`. The machine-readable result is
written to `.benchmarks/ecosystem-ocpa-vs-pg-ocpm-engine-0.10.0.json`.
