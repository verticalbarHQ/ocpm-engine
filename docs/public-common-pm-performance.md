# Public common-process-mining benchmark status

No `pg_ocpm 0.6.0` plus `ocpm-engine 0.5.0` results are published in this
source revision. Earlier staging values predated the final per-sample exactness,
source-provenance, dependency-lock, and concurrency-performance gates and were
removed rather than promoted.

The clean release run uses the checksum-pinned SAP IDES O2C and P2P OCEL 2.0
logs. It compares indexed relational OCEL in vanilla PostgreSQL plus independent
Python reference kernels with compact `pg_ocpm` aggregates plus Rust kernels.
All 18 workload/dataset pairs must pass canonical output equality for every
timed sample, the per-workload and geometric-mean 10x latency gates, three
duration-bounded concurrency epochs, a 10x candidate-throughput ratio, storage
ceilings, and clean source/image provenance.

Run `make perf-public`, inspect the ignored staging artifacts, and then run
`make perf-public-preview-check`. Results will be shown for review before any
artifact is copied into `docs/results/`.
