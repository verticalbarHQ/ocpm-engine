# Ecosystem common-PM benchmark

This suite is deliberately separate from the strict OCPQ Q1-Q7 benchmark. It
compares the same fixed analytical workloads in two independent pairs, using
the dataset native to each upstream project:

- Rust4PM versus pg_ocpm + ocpm-engine on Rust4PM's OCEL 2.0 P2P test corpus
- OCPA versus pg_ocpm + ocpm-engine on the OCEL 2.0 running example named in
  OCPA's import documentation

Every arm uses the source SQLite files with the checksums recorded in the
generated manifest. The common layer fixes lifecycle-containment windows,
scoring, tie breaking, output canonicalization, latency sampling, concurrency
levels, and exact-answer gates.

The competitor arms traverse the public native OCEL models supplied by
Rust4PM and OCPA, then run independently implemented versions of the four fixed
common algorithms. This keeps the question identical across each pair, but it
is not a reproduction of Rust4PM's Alpha+++ evaluation or a measurement of
OCPA's entire algorithm catalog.

Each documented OCEL 2.0 SQLite importer is probed on the unmodified source and
its outcome is reported. Rust4PM loads through its importer. OCPA 1.3.4's
importer currently fails on its own running example before it constructs a
model; that arm uses a disclosed, untimed benchmark adapter to construct OCPA's
public native object-centric data model without precomputing any measured
answer. A failed native-import probe prevents publication-ready status. The
pg_ocpm fixture is built from the same checksum-verified SQLite bytes. The
object type is selected by a fixed rule: the type with the greatest number of
multi-event lifecycles, then the greatest E2O link count, then lexical name.
This yields `goods receipt` for Rust4PM P2P and `orders` for the OCPA example.
Import time and resident memory remain visible but are excluded from
steady-state query latency.

Run both full pair benchmarks:

```bash
benchmarks/run_ecosystem_benchmark.sh
```

Run one pair or a non-publication smoke pass:

```bash
benchmarks/run_ecosystem_benchmark.sh --pair rust4pm
benchmarks/run_ecosystem_benchmark.sh --pair ocpa
benchmarks/run_ecosystem_benchmark.sh --smoke
```

The runner uses pinned, isolated Docker images and resets PostgreSQL before
each pair. Full runs
use 10 warmups, three 30-sample latency epochs, and three concurrency epochs at
1/2/4/8 workers. Generated JSON and Markdown artifacts are written under
`.benchmarks/`. A dirty pg_ocpm or ocpm-engine source tree is recorded and
prevents the merged artifact from claiming publication readiness. Because each
pair uses a different upstream dataset, the two reports should not be compared
as a cross-project ranking.
