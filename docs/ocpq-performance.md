# OCPQ benchmark status

The earlier OCPQ result was withdrawn because its timing protocol did not
support a valid same-host comparison. No replacement numbers are published in
this source revision.

The replacement protocol compares pinned OCPQ 0.6.7 with `pg_ocpm` plus
`ocpm-engine` on the same Docker host. Both sides use zero warmups and ten
measured runs per query, with a fresh container for each query. Correctness is
duplicate-preserving and covers every evaluation-tree node, every object/event
binding, exact violations, typed labels, node manifests, and canonical hashes.
The timed boundary includes complete owned all-node materialization. Immutable
source and image provenance, stable duration-bounded concurrency epochs,
complete retained serving-storage accounting, and fresh-process peak-memory
measurements are separate required gates. Concurrency evidence retains every
integer-nanosecond request latency under a deterministic client, request, and
query identity; the release checker recomputes p50, p95, and p99 from those raw
samples before evaluating the tail-latency gate.

The clean strict reference and combined candidate resource artifact must pass
the publication gates described in [`benchmarks/ocpq/README.md`](../benchmarks/ocpq/README.md)
before this document is populated. Results will be shown for review before any
evidence commit or push.
