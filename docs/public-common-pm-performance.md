# Public common-process-mining performance

This report records the `pg_ocpm 0.4.0` and `ocpm-engine 0.3.0` release run on
the public SAP IDES O2C and P2P logs. All 18 workload/dataset pairs passed exact
canonical-output comparison before their latency samples were accepted. The
geometric-mean p50 speedup was **33.146x** and the slowest pair was **17.183x**,
so both release gates exceeded 10x.

The machine-readable result is
[`results/public-common-pm-0.3.0.json`](results/public-common-pm-0.3.0.json)
(payload SHA-256
`17779a957f25d899270e81827e54e07f9e3498d6fcc9070ec949bbe1704ba319`).

## What was compared

The baseline is vanilla PostgreSQL 16 with a normalized relational OCEL schema,
seven appropriate B-tree indexes, and independent Python reference kernels.
It reconstructs lifecycles, directly-follows relations, variants, and duration
features at request time. The Python code follows ordinary dataframe/library
composition semantics, but **does not import or copy PM4Py**. It should be read
as a PostgreSQL-plus-Python baseline, not as a measured PM4Py release.

The candidate uses the same source facts, normalized once into `pg_ocpm` compact
serving capsules. Native PostgreSQL aggregates return counts and sufficient
statistics; `ocpm-engine` builds or scores the small model in Rust through a
stable-ABI Python binding.

```mermaid
flowchart LR
    A[OCEL events and objects] --> B{Serving path}
    B -->|baseline| C[Indexed relational OCEL]
    C --> D[Window functions and row groups]
    D --> E[Python reference kernel]
    B -->|candidate| F[pg_ocpm compact capsules]
    F --> G[Native multi-window and feature aggregates]
    G --> H[Small sufficient-statistic vectors]
    H --> I[ocpm-engine Rust kernel]
    E --> J[Canonical result]
    I --> J
```

The public source is Alessandro Berti's *Collection of Object-Centric Event
Logs*, DOI [10.5281/zenodo.8261133](https://doi.org/10.5281/zenodo.8261133),
licensed CC BY 4.0. The fixture verifies both archive and extracted SQLite
SHA-256 digests. The selected O2C backbone contains 98,350 events and 236,265
event-object links; P2P contains 24,854 events and 105,039 event-object links.

## Latency

Values are warm-cache p50 milliseconds from nine measured runs after two
warmups. Engine order is randomized per pair with seed `20260718`. Latency
includes database extraction/aggregation and model construction or scoring.

| Dataset | Workload | Vanilla PG + Python | pg_ocpm + Rust | Speedup |
|---|---|---:|---:|---:|
| SAP O2C | DFG frequency conformance, 95% | 38.078 | 1.160 | 32.826x |
| SAP O2C | Variant frequency conformance, 95% | 26.651 | 1.551 | 17.183x |
| SAP O2C | Next-activity prediction | 38.295 | 1.203 | 31.833x |
| SAP O2C | DFG frequency drift | 38.228 | 1.197 | 31.937x |
| SAP O2C | Repeated-transition rework | 40.520 | 2.056 | 19.708x |
| SAP O2C | Edge bottleneck ranking | 42.819 | 1.957 | 21.880x |
| SAP O2C | Edge bottleneck prediction | 98.337 | 2.773 | 35.462x |
| SAP O2C | Monthly edge-duration series | 251.292 | 2.377 | 105.718x |
| SAP O2C | Activity profile | 48.742 | 2.624 | 18.575x |
| SAP P2P | DFG frequency conformance, 95% | 15.695 | 0.472 | 33.252x |
| SAP P2P | Variant frequency conformance, 95% | 28.400 | 0.531 | 53.484x |
| SAP P2P | Next-activity prediction | 15.990 | 0.464 | 34.461x |
| SAP P2P | DFG frequency drift | 15.946 | 0.510 | 31.267x |
| SAP P2P | Repeated-transition rework | 18.008 | 0.865 | 20.818x |
| SAP P2P | Edge bottleneck ranking | 16.285 | 0.552 | 29.502x |
| SAP P2P | Edge bottleneck prediction | 34.630 | 0.832 | 41.623x |
| SAP P2P | Monthly edge-duration series | 105.969 | 1.813 | 58.450x |
| SAP P2P | Activity profile | 30.184 | 0.638 | 47.310x |

The step change comes from changing the unit of exchange. The baseline sends
or groups event/case rows after rebuilding order. The candidate scans aligned
capsule arrays in C and transfers only small count or sufficient-statistic
vectors. `ocpm.window_cardinalities(...)` evaluates all requested train/test or
period windows in one aggregate state. `ocpm.activity_profile(...)` counts
activity multiplicity and start/end membership directly inside prebuilt case
capsules, without materializing one SQL row per path position. PyO3 releases
the Python GIL while deterministic Rust kernels rank, select, and score the
vectors, including Jensen-Shannon frequency drift.

## Storage

The measurement sums PostgreSQL heap, index, and TOAST relation bytes after the
same facts are loaded and analyzed. The baseline retains only relational OCEL;
the candidate retains the `ocpm` serving representation after staging rows are
removed.

| Representation | Heap | Indexes | TOAST | Total |
|---|---:|---:|---:|---:|
| Vanilla relational OCEL | 59.2 MiB | 113.9 MiB | <0.1 MiB | 173.3 MiB |
| pg_ocpm serving schema | 50.6 MiB | 19.7 MiB | 31.8 MiB | 102.3 MiB |

`pg_ocpm` used **41.0% less total space** and **82.7% less index space** in this
fixture. Its TOAST use is intentional: cold exact payload vectors are stored
out of line, while a small set of leading-key/time indexes prunes capsules.

## Concurrency

Concurrency replays the O2C DFG-conformance request 32 times at each worker
level. Each task opens a connection. Per-request latency starts after connection
setup; aggregate throughput wall time includes connection setup and teardown.
That makes throughput intentionally more conservative than a production pool.

| Workers | Vanilla p50 / p95 | Candidate p50 / p95 | Vanilla QPS | Candidate QPS |
|---:|---:|---:|---:|---:|
| 1 | 40.643 / 42.109 ms | 1.576 / 1.917 ms | 22.931 | 250.137 |
| 4 | 42.795 / 46.251 ms | 1.780 / 2.322 ms | 85.348 | 638.456 |
| 8 | 49.416 / 53.441 ms | 1.863 / 2.910 ms | 136.448 | 712.671 |
| 16 | 55.107 / 58.191 ms | 2.832 / 13.303 ms | 215.092 | 753.549 |

At 16 workers, candidate p50 remained **19.5x lower** and throughput was
**3.50x higher**. Throughput gains are smaller than latency gains because both
paths include connection churn and the candidate approaches the machine's
available database/client capacity sooner.

The same sweep was run for DFG frequency drift, including Rust scoring:

| Workers | Vanilla p50 / p95 | Candidate p50 / p95 | Vanilla QPS | Candidate QPS |
|---:|---:|---:|---:|---:|
| 1 | 40.730 / 42.869 ms | 1.680 / 2.061 ms | 22.877 | 244.571 |
| 4 | 42.939 / 46.447 ms | 1.833 / 2.022 ms | 83.988 | 624.353 |
| 8 | 60.626 / 82.046 ms | 1.964 / 3.039 ms | 109.010 | 772.035 |
| 16 | 56.370 / 61.024 ms | 2.682 / 4.484 ms | 217.988 | 798.915 |

At 16 workers, the drift path sustained **3.67x higher throughput** with a
**21.0x lower p50**.

## Reproduce

From a checkout of `ocpm-engine`:

```sh
make perf-public
make perf-public-check
```

The runner obtains a sibling `pg_ocpm` checkout when available or clones tag
`v0.4.0`, builds the Rust wheel and both PostgreSQL images, checksum-verifies
the public data, recreates both databases, runs the benchmark, and stops all
containers on exit. The committed-result checker independently verifies its
payload digest, workload count, correctness flags, and per-workload 10x gate.

The recorded environment was PostgreSQL 16.14 on Linux/aarch64 with 18 logical
CPUs visible to Docker, `shared_buffers=1GB`, `effective_cache_size=4GB`,
`work_mem=16MB`, JIT disabled, and the same database settings on both sides.

## Published context, not cross-study comparison

Küsters and van der Aalst's
[*Developing a High-Performance Process Mining Library with Java and Python
Bindings in Rust*](https://arxiv.org/abs/2401.14149) already establishes Rust
plus language bindings as process-mining prior art. Its Alpha+++ experiments
report, for one BPI 2020 DD configuration, 0.6761 s for Python, 0.5925 s for
Java, 0.0675 s for single-threaded Rust, and 0.0120 s for parallel Rust. Those
numbers concern a different algorithm, log representation, machine, and timing
boundary and are not comparable to this suite.

The [OCPQ paper](https://arxiv.org/abs/2506.11541) evaluates a specialized
object-centric query engine against SQLite, Neo4j, and DuckDB and reports
performance comparable to DuckDB and better than the other two for its seven
query workloads. OCPQ's query language, dataset, baselines, and measurement
boundary differ from this common-PM suite. Its compact bindings, early filters,
and specialized execution reinforce the architectural direction, but no OCPQ
implementation was copied or linked here.

[PM4Py's paper](https://arxiv.org/abs/1905.06169) supplies ecosystem context.
The current PM4Py repository is AGPL-3.0/commercial; it was intentionally not
used as source code or a dependency. A future publication-quality study should
add a separately installed, version-pinned PM4Py baseline on identical
algorithms, inputs, hardware, and timing boundaries rather than infer PM4Py
performance from this independent Python reference implementation.

## Claim boundaries

- DFG and variant conformance are deterministic frequency-coverage models, not
  Petri-net token replay or optimal alignments.
- Frequency drift is population drift over DFG edge counts, not automatic
  concept-drift localization or root-cause attribution.
- Next-activity and bottleneck prediction are transparent deterministic
  baselines. The benchmark measures execution, not predictive superiority.
- Results are medium-size, warm-cache, single-machine measurements. They do not
  establish distributed scale, ingest throughput, or universal superiority.
- Exact output equality is established for the implemented workloads. It does
  not establish equivalence for algorithms outside this suite.
- The architecture and results may support a research contribution, but
  novelty requires a formal prior-art review and independent replication.
