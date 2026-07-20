# Public common-process-mining benchmark status

No `pg_ocpm 0.7.0` plus `ocpm-engine 0.6.0` results are published in this
source revision. Earlier staging values predated the final per-sample exactness,
source-provenance, dependency-lock, and concurrency-performance gates and were
removed rather than promoted.

The clean release run uses the checksum-pinned SAP IDES O2C and P2P OCEL 2.0
logs. It compares indexed relational OCEL in vanilla PostgreSQL plus independent
Python reference kernels with compact `pg_ocpm` aggregates plus Rust kernels.
All 18 workload/dataset pairs must pass canonical output equality for every
timed sample. Serial latency uses three epochs of 30 randomized rounds per arm,
retains all 90 integer-nanosecond samples and realized arm-order codes, and
reports pooled p50/p95 together with the range of the three epoch p95s. The
checker independently recomputes that evidence before applying the
per-workload and geometric-mean 10x latency gates, three duration-bounded
concurrency epochs, a 10x candidate-throughput ratio, storage ceilings, and
clean source/image provenance. Schema-5 evidence identifies the locked
`ocpm-engine 0.6.0` product source, current benchmark controller source, and
locked `pg_ocpm 0.7.0` source independently.

Run `make perf-sap-release-bridge-preview`, then `make perf-public`. Inspect the
ignored staging artifacts and run `make perf-public-preview-check`. Results will
be shown for review before any artifact is copied into `docs/results/`.
