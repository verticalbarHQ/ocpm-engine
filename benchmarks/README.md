# Performance regression suites

The public suite also includes the BPIC 2017-derived Q1-Q7 comparison with a
same-host OCPQ reproduction. The corrected protocol uses zero warmups and ten
measured runs per query on both sides and requires duplicate-preserving parity
for every evaluation-tree node, not only the root. Author-published timings are
source context only; no cross-host ratio is calculated. See
[`ocpq/README.md`](ocpq/README.md). The reviewed 0.8.0 artifacts passed every
latency, memory, storage, concurrency, correctness, and provenance gate and are
published under `docs/results/`.

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
make perf-sap-release-bridge-preview
make perf-public
make perf-public-preview-check
```

`PG_OCPM_SOURCE` must identify a local `pg_ocpm` repository containing the
locked `0.8.0` commit. Alternatively, set `PG_OCPM_REPOSITORY` to a public Git
URL and the runner will clone tag `v0.8.0` into the ignored benchmark workspace.
In either case, the measured database image is built from a clean detached
worktree at revision `0e15ab10f8ec87518b9e822072028fb3eda3879c`.

The runner builds the release-mode stable-ABI Rust wheel from a clean detached
`ocpm-engine` worktree at revision
`f5a95ecd6b8a1f184f8ffed2371980ef419beaab`. The benchmark controller comes
from the current clean checkout instead of the release worktree. Named Docker
build contexts keep those sources separate, the runtime image contains only the
current benchmark harness plus the installed release wheel, and schema-5
provenance records both revisions and cleanliness states independently. The
runner then recreates both database volumes, records three serial-latency epochs
of 30 randomized measured rounds after ten warmups, runs 1/4/8/16 worker
concurrency sweeps for DFG conformance and drift, writes
`.benchmarks/public-common-pm-0.8.0.json` and
`.benchmarks/sap-pm4py-three-way-0.8.0.json`, and stops its containers on exit.
The preview check validates both ignored artifacts without changing
`docs/results/`. `check_public_result.py` validates exact public fixtures and
settings, correctness flags, workload count, 10x latency gates, and the stable
concurrency protocol without requiring Docker. Product latency, memory,
concurrency, and storage non-regression are checked by the private same-host
gate described below before public artifacts are staged.
Every serial arm retains all 90 positive integer nanosecond samples and the
realized per-round arm-order codes. The release checker independently recomputes
the pooled p50 and nearest-rank p95, every epoch summary, and the epoch-p95
median and range. Reports show pooled p95 with the minimum-to-maximum range of
the three epoch p95s, so a tail claim cannot hide an isolated noisy epoch.

The compact
[`public-common-pm-0.4.0-regression-baseline.json`](../docs/results/public-common-pm-0.4.0-regression-baseline.json).
retains only source, fixture, and workload identity. Historical latency,
memory, storage, and concurrency values were removed and cannot satisfy a
release gate.

### Private SAP release regression gate

`sap_release_regression.py` compares the current locked release with the last
accepted release on the same checksum-pinned SAP fixture and host. Vanilla
PostgreSQL is an untimed correctness oracle. This artifact is ignored and is
used only to decide whether a public current-versus-vanilla run may proceed.

Every workload uses ten warmup rounds with five executions in each order and
three measured epochs of 30 rounds with exactly 15 executions in each order.
The artifact retains every positive integer-nanosecond sample, every answer
hash, and every realized order, plus first-position and second-position latency
summaries for each arm. Four fresh-process RSS samples per arm use an exact 2/2
execution-order schedule. Four duration-bounded concurrency epochs at every
worker level retain raw controller round-trip and worker-internal samples. The
independent checker recomputes every latency, RSS, QPS, p50, p95, p99, order,
and correctness claim before applying the matched-release non-inferiority gates.

Concurrency timings are retained losslessly as little-endian unsigned 64-bit
integers compressed with zlib and encoded with base64. Each worker also records
an exact-answer SHA-256 histogram whose count must equal its decoded timing
count. The checker implements its own strict decoder and recomputes all metrics;
this representation reduces artifact size without using a sketch or dropping a
request.

Autovacuum is disabled in the release-bridge containers. Both `pg_ocpm` arms
receive an explicit database-wide `VACUUM (ANALYZE)` before the structural
storage snapshot. The artifact records each relation's main, FSM, VM, index,
TOAST, and TOAST-index bytes plus maintenance state. A second post-workload
snapshot is diagnostic only. This prevents relation age or background
maintenance from being reported as product storage growth.

The runner requires a local `pg_ocpm` repository containing both locked
revisions (the sibling `../pg_ocpm` by default, or `PG_OCPM_SOURCE`). It creates
and verifies clean detached worktrees for all four measured releases under the
ignored `.benchmarks/` directory, then builds fresh isolated database volumes.
Execute the ignored preview path with:

```sh
make perf-sap-release-bridge-preview
make perf-sap-release-bridge-preview-check
```

The ignored preview is written to
`.benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json`. The bridge is deliberately
separate from the two current-versus-vanilla public result artifacts and is
never copied into `docs/results/`.

Publication is an explicit second step. After reviewing the staged public JSON,
copy only the current-versus-vanilla artifacts into `docs/results/`, update the
expected payload digest pins and report links, and run:

```sh
make perf-public-release-check
```

`make perf-release-check` runs the published SAP and OCPQ checks together.
Release checks read only committed public artifacts and enforce their pinned
file or payload digests; they do not accept ignored preview evidence.

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
ignored schema-5 `.benchmarks/` staging artifacts. Older schema artifacts must
be replaced by a complete run because they do not distinguish product and
controller source provenance. The target runs the epoch-level checkers in
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
counts, fixture settings, exact answer hashes, current-path performance gates,
and every retained concurrency epoch. Matched-release latency, memory,
concurrency, and storage gates come from `sap_release_regression.py`.
Concurrency protocol versions are structurally gated rather than compared
across incompatible timing boundaries. To refresh concurrency without rerunning
latency, storage, or memory, use `--concurrency-only` with the existing `--output`
and `--report` paths, or run `make perf-public-concurrency` for the complete
clean-database sequence. Run `make perf-public-preview-check` to revalidate the
staged artifacts without rerunning Docker. Published release artifacts and their
latency and storage gates retain pinned payload digests.

The compact
[`sap-pm4py-three-way-0.4.0-regression-baseline.json`](../docs/results/sap-pm4py-three-way-0.4.0-regression-baseline.json).
retains only source counts, fixture identity, exact answer hashes, and input
shapes. It contains no historical latency, memory, storage, concurrency,
environment, method, generated-summary, or dependency-footprint values.
