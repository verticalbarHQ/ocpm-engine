# Performance regression suites

The public suite also includes the BPIC 2017-derived Q1-Q7 comparison with both
published and same-host reproduced OCPQ. See [`ocpq/README.md`](ocpq/README.md).
Validate its committed artifacts with `make perf-ocpq-check`.

## Public SAP O2C/P2P release gate

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
make perf-public-check
```

`PG_OCPM_SOURCE` must identify a local `pg_ocpm` checkout. Alternatively, set
`PG_OCPM_REPOSITORY` to a public Git URL and the runner will clone tag `v0.4.0`
into the ignored benchmark workspace.

The runner builds a release-mode stable-ABI Rust wheel, recreates both database
volumes, records nine randomized measured runs after two warmups, runs 1/4/8/16
worker concurrency sweeps for DFG conformance and drift, writes
`.benchmarks/public-common-pm-0.3.0.json`, and stops its containers on exit.
`check_public_result.py` validates the committed payload digest, correctness
flags, workload count, and 10x gates without requiring Docker.

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
and bottleneck ranking. It records randomized warm latency, 1/2/4/8-worker DFG
concurrency, isolated peak RSS, database storage, and client dependency
footprints. Exact three-way canonical answers are required before samples are
accepted.

The relational index reduction is applied only after the main public benchmark
finishes, so the two suites remain independent. The recorded report is
[SAP PM4Py three-way performance](../docs/sap-pm4py-three-way-performance.md),
and `check_sap_pm4py_result.py` validates its committed JSON payload.
