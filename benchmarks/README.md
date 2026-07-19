# Performance regression suites

The public suite also includes the BPIC 2017-derived Q1-Q7 comparison with a
same-host OCPQ reproduction. The corrected protocol uses zero warmups and ten
measured runs per query on both sides and requires duplicate-preserving parity
for every evaluation-tree node, not only the root. Author-published timings are
source context only; no cross-host ratio is calculated. See
[`ocpq/README.md`](ocpq/README.md). Strict outputs remain ignored previews until
their latency, memory, storage, concurrency, correctness, and provenance gates
pass and the complete result has been reviewed.

All local checker targets require Python 3.11 or newer. They use
`.venv/bin/python` when that environment exists, otherwise `python3`. Override
the interpreter explicitly when needed, for example
`make PYTHON=/path/to/python3.11 perf-public-preview-check`.

## Public SAP O2C/P2P preview and release gates

`public_fixture.py` downloads and checksum-verifies the CC BY 4.0 SAP IDES O2C
and P2P logs from Zenodo DOI `10.5281/zenodo.8261133`. It creates two clean
PostgreSQL 16 databases containing the same facts: indexed relational OCEL in
vanilla PostgreSQL and the compact `pg_ocpm` serving schema.

`public_common_pm.py` covers DFG and variant conformance, next-activity
prediction, DFG frequency drift, repeated-transition rework, bottleneck ranking
and prediction, edge-duration time series, and filtered activity profiles. A
candidate sample is timed only after its canonical answer equals the independent
Python reference answer. Both the geometric mean and every individual workload
must be at least 10x faster across all 18 dataset/workload pairs.

Run the complete Docker-isolated path with:

```sh
export PG_OCPM_SOURCE=/path/to/pg_ocpm
make perf-public
make perf-public-preview-check
```

`PG_OCPM_SOURCE` must identify a local `pg_ocpm` checkout. Alternatively, set
`PG_OCPM_REPOSITORY` to a public Git URL and the runner will clone tag `v0.7.0`
into the ignored benchmark workspace.

The runner builds a release-mode stable-ABI Rust wheel, recreates both database
volumes, records three serial-latency epochs of 30 randomized measured rounds
after ten warmups, runs 1/4/8/16 worker concurrency sweeps for DFG conformance
and drift, writes
`.benchmarks/public-common-pm-0.6.0.json` and
`.benchmarks/sap-pm4py-three-way-0.6.0.json`, and stops its containers on exit.
The preview check validates both ignored artifacts without changing
`docs/results/`. `check_public_result.py` validates exact public fixtures and
settings, correctness flags, workload count, 10x latency gates, the stable
concurrency protocol, and 1% storage ceiling without requiring Docker. The
historical p50 guard permits 10% or 0.10 ms, whichever is larger, so sub-ms
results are not rejected solely because of timer and scheduling jitter.
Every serial arm retains all 90 positive integer nanosecond samples and the
realized per-round arm-order codes. The release checker independently recomputes
the pooled p50 and nearest-rank p95, every epoch summary, and the epoch-p95
median and range. Reports show pooled p95 with the minimum-to-maximum range of
the three epoch p95s, so a tail claim cannot hide an isolated noisy epoch.

Latency and storage regressions use the compact
[`public-common-pm-0.4.0-regression-baseline.json`](../docs/results/public-common-pm-0.4.0-regression-baseline.json).
It retains only the matching source, environment, latency method, fixtures,
per-workload p50 values, and index/total storage values from the valid 0.4 run.
The obsolete 0.2 and 0.3 artifacts and the full 0.4 artifact were removed; no
historical concurrency result is retained or accepted as regression evidence.

Publication is an explicit second step. After reviewing the staged JSON, copy
the accepted artifacts into `docs/results/`, update the expected payload digest
pins and report links, and run:

```sh
make perf-public-release-check
```

After both the SAP and OCPQ suites have published artifacts,
`make perf-release-check` runs all release checks together. Release checks read
only committed-path artifacts and enforce their pinned file or payload digests;
they do not accept ignored preview evidence.

Concurrency uses one persistent PostgreSQL connection per prestarted worker.
Each engine/level arm runs three independent epochs; every epoch must include an
exact warmup from every worker, last at least five seconds, and complete at
least 32 measured requests per worker. QPS and p50/p95/p99 are medians of the
three epoch metrics. Engine order rotates between epochs. To replace only the
concurrency sections of an existing, digest-valid artifact while preserving its
latency, storage, and fixture evidence, run:

```sh
make perf-public-concurrency
```

The target rebuilds the checksum-verified databases so the common-PM and PM4Py
index policies remain independent, then updates only concurrency in both
ignored `.benchmarks/` staging artifacts. It runs the epoch-level checkers in
preview mode after both artifacts finish. The published `docs/results/`
artifacts are intentionally unchanged until the staged results have been
reviewed, promoted, and their digest pins updated.

See [the public performance report](../docs/public-common-pm-performance.md) for
the recorded environment, results, limitations, and published comparison
context.

## SAP PM4Py three-way comparison

`sap_pm4py_three_way.py` uses the same checksum-verified O2C/P2P databases to
compare three complete request paths:

1. PM4Py over relational PostgreSQL with integrity indexes and one
   workload-specific secondary B-tree.
2. PM4Py over `pg_ocpm` event chunks.
3. ocpm-engine over compact `pg_ocpm` aggregate rows.

The benchmark covers DFG and variant conformance, next-activity prediction,
and bottleneck ranking. It records three randomized serial-latency epochs with
raw nanosecond samples and realized order codes, 1/2/4/8-worker DFG concurrency
using the same three-epoch duration and per-worker request floors, isolated peak
RSS, database storage, and client dependency footprints. Exact three-way
canonical answers are required before samples are accepted.

The relational index reduction is applied only after the main public benchmark
finishes, so the two suites remain independent. The recorded report is
[SAP PM4Py three-way performance](../docs/sap-pm4py-three-way-performance.md),
and `check_sap_pm4py_result.py` validates its committed JSON payload, source
counts, fixture settings, exact answer hashes, 10% p50 tolerance, 1% storage
ceiling, bounded isolated memory, and every retained concurrency epoch.
Concurrency protocol versions are structurally gated rather than compared
across incompatible timing boundaries. To refresh concurrency without rerunning
latency, storage, or memory, use `--concurrency-only` with the existing `--output`
and `--report` paths, or run `make perf-public-concurrency` for the complete
clean-database sequence. Run `make perf-public-preview-check` to revalidate the
staged artifacts without rerunning Docker. Published release artifacts and their
latency and storage gates retain pinned payload digests.

Historical SAP regressions use the compact
[`sap-pm4py-three-way-0.4.0-regression-baseline.json`](../docs/results/sap-pm4py-three-way-0.4.0-regression-baseline.json).
It retains only comparable source, fixture, environment, method, answer/input
shape, p50 latency, isolated incremental/total RSS, and index/total storage
values. It contains no raw samples, historical concurrency measurements,
generated summaries, or dependency-footprint claims.
