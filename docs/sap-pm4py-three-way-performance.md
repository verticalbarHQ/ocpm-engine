# SAP O2C and P2P three-way benchmark status

No `pg_ocpm 0.7.0` plus `ocpm-engine 0.6.0` PM4Py comparison is published in
this source revision. Earlier staging values predated the final per-sample
exactness, source-provenance, hash-locked dependency, concurrency-performance,
and total peak-RSS gates. The full staging artifact was removed rather than
promoted. The compact historical file retains only source counts, fixture
identity, exact answers, and input shapes; all performance values were removed.

The clean run compares three complete request paths on the checksum-pinned SAP
IDES O2C and P2P OCEL 2.0 logs:

1. lightly indexed vanilla PostgreSQL plus PM4Py;
2. `pg_ocpm` plus PM4Py; and
3. `pg_ocpm` plus `ocpm-engine`.

Every timed sample must return the same canonical answer. Serial latency uses
three epochs of 30 randomized rounds per arm and retains all 90 integer-
nanosecond samples plus realized arm-order codes. The report shows pooled p95
with the range of the three epoch p95s, and the release checker independently
recomputes all pooled and epoch metrics. It also requires three duration-bounded
concurrency epochs, at least a 3x engine-throughput ratio at every worker level,
bounded total and incremental peak RSS, storage regression limits, clean
source/image provenance, and exact benchmark package versions.

Run `make perf-sap-release-bridge-preview`, then `make perf-public`. Inspect the
ignored staging artifacts and generated reports, then run
`make perf-public-preview-check`. Results will be shown for review before any
artifact is copied into `docs/results/`.
