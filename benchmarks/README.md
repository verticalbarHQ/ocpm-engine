# Performance regression suites

## Public SAP O2C/P2P release gate

`public_fixture.py` downloads and checksum-verifies the CC BY 4.0 SAP IDES O2C
and P2P logs from Zenodo DOI `10.5281/zenodo.8261133`. It creates two clean
PostgreSQL 16 databases containing the same facts: indexed relational OCEL in
vanilla PostgreSQL and the compact `pg_ocpm` serving schema.

`public_common_pm.py` covers DFG and variant conformance, next-activity
prediction, repeated-transition rework, bottleneck ranking and prediction, and
edge-duration time series. A candidate sample is timed only after its canonical
answer equals the independent Python reference answer. Both the geometric mean
and every individual workload must be at least 10x faster.

Run the complete Docker-isolated path with:

```sh
make perf-public
make perf-public-check
```

The runner builds a release-mode stable-ABI Rust wheel, recreates both database
volumes, records nine randomized measured runs after two warmups, runs 1/4/8/16
worker concurrency sweeps, writes `.benchmarks/public-common-pm-0.2.0.json`, and
stops its containers on exit. `check_public_result.py` validates the committed
payload digest, correctness flags, workload count, and 10x gates without
requiring Docker.

See [the public performance report](../docs/public-common-pm-performance.md) for
the recorded environment, results, limitations, and published comparison
context.

## Application regression fixtures

This directory contains two correctness-gated, three-way benchmark suites:

- `goodr_verticalbar_three_way.py` replays the query shapes recovered from
  `verticalbar-mvp` and `verticalbar-app`.
- `goodr_common_pm_three_way.py` covers bottleneck analysis, rework, SLA
  breaches, DFG and variant conformance, and two temporal prediction baselines.

Both suites compare an index-light relational PostgreSQL representation, the
current Vertical Bar relational schema and indexes, and `pg_ocpm`. Every timed
case must return the same canonical answer from all three engines before its
latency is eligible for comparison. Each measured iteration randomizes engine
order.

## Run

The reproducible fixture and Docker services are maintained in the parent
Dendrites benchmark workspace. From this repository, run:

```sh
make perf-goodr
```

This starts the fixture services if necessary, writes fresh JSON under the
ignored `.benchmarks/` directory, then checks both runs against the committed
correctness, p50 latency, and storage baselines. The default regression budget
is 25 percent or 1 ms, whichever is larger; storage may grow by at most 5
percent. Override the benchmark sample sizes with `WARMUPS`, `RUNS`, and
`TIMEOUT_SECONDS` make variables.

Stop the fixture services after a run with `make perf-goodr-stop`.

The common-PM prediction cases use a temporal 80/20 train/test split. Model
construction and held-out scoring are both inside the timed query. They are
deterministic SQL baselines intended to exercise process-mining access paths,
not claims about production model quality.

The conformance cases measure frequency-covered directly-follows and complete
variant conformance. They do not implement Petri-net token replay or alignment.

Committed baselines and detailed methodology live in `docs/results/` and the
two performance comparison documents in `docs/`.
