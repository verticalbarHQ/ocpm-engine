# OCPQ data: four-way 0.9 comparison

This release preview runs the BPIC 2017 OCPQ Q1-Q7 data through four fixed
execution paths:

1. OCPQ 0.6.7;
2. vanilla PostgreSQL plus PM4Py 2.7.23.3;
3. `pg_ocpm 0.9.0` plus the same PM4Py 2.7.23.3 environment; and
4. `pg_ocpm 0.9.0` plus `ocpm-engine 0.9.0`.

All paths produced exact duplicate-preserving results for every evaluation-tree
node. Each value is the mean of ten zero-warmup evaluations, with one fresh
Docker process per query. The two PM4Py paths were captured together; OCPQ and
the 0.9 engine were rerun later the same day under the corrected harness. Stable
dataset and query pins match across all four arms, but the PM4Py artifacts do
not record the cross-arm host identifier needed to prove same-host execution.
This is verified descriptive engineering evidence, not a promoted publication
artifact.

| Query | OCPQ | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + engine |
|---|---:|---:|---:|---:|
| Q1 | 26.33 ms | 49.71 ms | 39.84 ms | 1.63 ms |
| Q2 | 43.12 ms | 103.41 ms | 77.69 ms | 2.92 ms |
| Q3 | 19.22 ms | 48.23 ms | 38.97 ms | 1.91 ms |
| Q4 | 44.41 ms | 128.37 ms | 102.64 ms | 2.43 ms |
| Q5 | 50.01 ms | 219.30 ms | 209.66 ms | 4.61 ms |
| Q6 | 82.95 ms | 39.48 ms | 29.38 ms | 1.95 ms |
| Q7 | 66.54 ms | 143.74 ms | 118.12 ms | 1.88 ms |
| Geometric mean | **42.90 ms** | **87.61 ms** | **70.90 ms** | **2.33 ms** |

On the geometric mean, the 0.9 Rust path is **18.42x** faster than OCPQ,
**37.62x** faster than vanilla PostgreSQL plus PM4Py, and **30.44x** faster than
`pg_ocpm` plus PM4Py.

## Timing boundaries

The results do not hide Python initialization inside only one path:

- OCPQ measures its resident imported/linked data structures and excludes
  import, link, canonicalization, and hashing.
- Both PM4Py paths measure a fixed Pandas evaluator over a resident PM4Py OCEL
  through complete Python row materialization. Database extraction and PM4Py
  OCEL construction are excluded.
- The engine path includes one prepared PostgreSQL binding-capsule query, fetch,
  strict OCPB decode, and complete owned materialization of every result node.
  Only external-ID canonicalization, serialization, hashing, and comparison are
  outside the clock.

The engine therefore carries a more inclusive database boundary in this
comparison. The table should not be interpreted as measuring database load
time or PM4Py dataframe construction; the SAP benchmark covers those end-to-end
costs.

## Why Q6 differs

Q6 is a favorable shape for the two PM4Py evaluators, which beat OCPQ on that
individual query. The engine still completes the database-backed path in
1.95 ms. Reporting all seven queries prevents the geometric mean from hiding
that workload-specific difference.

## Correctness and provenance

The comparison checks external IDs, duplicates, typed labels, violation
reasons, per-node row counts, compact canonical JSON sizes, and SHA-256 hashes.
The four paths agree across 380,083 all-node situations.

The new 0.9 engine artifact records the exact source-tree fingerprint, runner
hash, image IDs, per-query process IDs, database version, and
reference-to-engine same-host/same-harness checks. The independent four-way
checker pins all four artifacts and the PM4Py runner by SHA-256, recomputes raw
sample means and ratios, and compares every node fingerprint across all arms.
It remains a preview because the working trees were dirty, the locally built
`pg_ocpm` image has incomplete source provenance, and PM4Py cross-arm host
identity is absent. The authoritative
release-mode OCPQ evidence remains the committed 0.8 result in
[OCPQ Q1-Q7 benchmark](ocpq-performance.md).

## Verify the local evidence

The PM4Py evaluator is currently owned by the adjacent `pg_ocpm` source tree at
`../pg_ocpm/benchmarks/ocpq_pm4py.py`. The verifier requires its digest as an
explicit input so that this cross-repository dependency cannot silently drift.

```sh
python3 benchmarks/check_ocpq_four_way.py \
  .benchmarks/ocpq-reproduced-strict-all-node-0.9-preview.json \
  .benchmarks/ocpq-vanilla-pg-pm4py-preview.json \
  .benchmarks/ocpq-pg09-pm4py-preview.json \
  .benchmarks/ocpq-bpic2017-pg_ocpm-0.9.0-ocpm-engine-0.9.0-strict-preview.json \
  --reference-sha256 ae317e94d0cb8a7cb74786aa71a7f1792eb16c245e577729995640056c4beb9a \
  --vanilla-sha256 fed69bc64ffcc7a358beab14212c5e2b704839e4c7c502c949ff8e5c7a4988a2 \
  --pg-pm4py-sha256 0cd71afb74f13a06d79306c27606c8d53603a61738336eac327fd4d7271bd88b \
  --engine-sha256 ffcaa60383bb52945ea2f0053add72b1ca20264bbcf0dfcc05ed3de5c67c45ba \
  --pm4py-runner ../pg_ocpm/benchmarks/ocpq_pm4py.py \
  --pm4py-runner-sha256 b9e8eec943a6afab60b481760e478c0df3bcec42aa1b344b3e176dfc5dd1cc2e
```

## Local artifacts

- `.benchmarks/ocpq-reproduced-strict-all-node-0.9-preview.json`
- `.benchmarks/ocpq-vanilla-pg-pm4py-preview.json`
- `.benchmarks/ocpq-pg09-pm4py-preview.json`
- `.benchmarks/ocpq-bpic2017-pg_ocpm-0.9.0-ocpm-engine-0.9.0-strict-preview.json`
