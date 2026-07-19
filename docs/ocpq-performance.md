# OCPQ benchmark status

The earlier OCPQ result was withdrawn because its timing protocol did not
support a valid same-host comparison. No replacement numbers are published in
this source revision.

The replacement protocol compares pinned OCPQ 0.6.7 with `pg_ocpm 0.6.0` plus
`ocpm-engine 0.5.0` on the same Docker host. It requires exact Q1-Q7 root-row
multiset parity, 10 warmups and 30 measured runs per query, immutable source and
image provenance, full owned-row materialization, stable duration-bounded
concurrency epochs, complete retained serving-storage accounting, and
fresh-process peak-memory measurements.

The clean reference, candidate, and memory artifacts must pass
`make perf-ocpq-release-check` before this document is populated. Results will
be shown for review before any evidence commit or push.
