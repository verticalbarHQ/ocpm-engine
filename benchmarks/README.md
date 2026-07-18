# Goodr performance regression suites

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
