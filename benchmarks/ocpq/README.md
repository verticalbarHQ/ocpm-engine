# OCPQ comparison benchmark

This directory contains the correctness-gated, same-host comparison between
OCPQ 0.6.7 and `pg_ocpm` plus `ocpm-engine`. The workload is Q1-Q7 from the
public BPIC 2017-derived OCEL 2.0 evaluation dataset.

The previous root-only comparison is obsolete. The strict protocol measures
and verifies the complete result tree for each query. The reviewed 0.8.0
artifacts passed the reference, candidate, memory, storage, concurrency, and
provenance gates and are published under `docs/results/`.

The timings published by the OCPQ authors are retained as source context only.
They were collected on a different host, so neither the runner nor the report
derives a speedup from them. Comparative ratios use only the pinned same-host
OCPQ reproduction.

## Source pins

- `aarkue/ocpq-eval`:
  `846dd4eb9f8600ae42355968453a9412ea4759c2`
- OCPQ 0.6.7:
  `80457e561edd7bb9e142d959dd7e0f96e6b03f2f`
- SQLite SHA-256:
  `02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7`
- Canonical Q1-Q7 tree/result-file manifest SHA-256:
  `387aeb31398d86ef8e7b15393649cbabe75be56185fd67d27021744025873466`
- Corrected OCPQ image: `ocpq:0.6.7-corrected-harness`

The pinned OCPQ and evaluation repositories do not declare a software license
at these revisions. The optional local reproducer does not redistribute their
source or binary, but public release of the reproducer should wait for explicit
upstream permission. See [`THIRD_PARTY_NOTICES.md`](../../THIRD_PARTY_NOTICES.md).

The OCPQ lockfile no longer resolves on its historical Rust 1.76 image because
transitive dependency metadata raised the minimum Rust version. The Dockerfile
uses Rust 1.86 while preserving the pinned OCPQ source and lockfile.

## Strict output contract

For each query, the reference and candidate must produce the same ordered list
of evaluation-tree nodes. Every node is compared as a duplicate-preserving
multiset of canonical situations containing:

- every bound object and event variable, normalized to the same external IDs;
- every typed label value;
- the exact violation reason, including null for non-violations; and
- the exact node manifest, logical-row count, and canonical SHA-256 digest.

Q6 additionally verifies its typed duration label and the duration recomputed
from the child node. A root-only hash or row count cannot satisfy this contract.

## Build the same-host reference

Obtain the public Git LFS dataset and run each query in a fresh container:

```sh
git clone https://github.com/aarkue/ocpq-eval.git .benchmarks/ocpq-eval
git -C .benchmarks/ocpq-eval checkout \
  846dd4eb9f8600ae42355968453a9412ea4759c2
git -C .benchmarks/ocpq-eval lfs pull --include=bpic2017.sqlite.zip
mkdir -p .benchmarks/ocpq-data
unzip .benchmarks/ocpq-eval/bpic2017.sqlite.zip \
  -d .benchmarks/ocpq-data

docker build -f benchmarks/ocpq/Dockerfile.ocpq \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t ocpq:0.6.7-corrected-harness .

uv run --extra benchmark python benchmarks/ocpq/run_local_ocpq.py \
  --image ocpq:0.6.7-corrected-harness \
  --sqlite .benchmarks/ocpq-data/bpic2017.sqlite \
  --eval .benchmarks/ocpq-eval \
  --warmups 0 \
  --runs 10 \
  --output .benchmarks/ocpq-reproduced-strict-all-node-preview.json
```

The reference uses exactly zero warmups and ten measured runs per query, with a
fresh OCPQ container for each query. Its timer includes `tree.evaluate` plus
construction and collection of every node's `EvaluationResultWithCount` into
owned output. Import, linking, external-ID canonicalization, sorting, JSON
serialization, and hashing remain outside the query timer.

## Run the strict candidate gates

Load the same SQLite fixture into a clean PostgreSQL instance built from the
candidate `pg_ocpm` source, including `ocpm.finish_load(...)` and
`ocpm.rebuild_binding_index(...)`. Those preprocessing operations are recorded
but excluded from request latency. Then run:

```sh
export OCPM_DATABASE_URL='postgres://postgres:pg@postgres_ocpq_pg_ocpm/postgres'
export OCPM_DATABASE_CONTAINER='postgres_ocpq_pg_ocpm'
export OCPM_DOCKER_NETWORK='pg-ocpm-ocpq-bench'
export OCPM_DATABASE_IMAGE='pg_ocpm:release-candidate'
export OCPM_PG_OCPM_SOURCE_REVISION="$(git -C ../pg_ocpm rev-parse HEAD)"
export OCPM_PG_OCPM_SOURCE_TREE_CLEAN=true

benchmarks/ocpq/run_strict_publication_gates.sh \
  .benchmarks/ocpq-reproduced-strict-all-node-preview.json \
  .benchmarks/ocpq-strict-publication-preview.json
```

Clean-state environment values accept only the lowercase literals `true` and
`false`. The generated evidence uses native JSON booleans; quoted strings such
as `"true"` are rejected. A claimed `provenance_complete: true` never overrides
an explicit dirty engine, reference, or `pg_ocpm` source-tree flag.

The latency arm starts a fresh single-worker candidate container for each query
and uses exactly zero warmups and ten measured runs. The timer covers the
prepared PostgreSQL request, fetching all node capsules, native decoding, and
complete owned materialization of every node. External-ID canonicalization,
sorting, JSON serialization, and hashing occur outside the timer, but exact
all-node parity is required for every measured run.

The same command also records fresh-process peak memory per query, complete
serving storage, and 1/4/8/16-client concurrency. Each concurrency client uses
a persistent prepared connection, rotates through Q1-Q7, and proves exact
all-node parity before and after every timed epoch. Every request retains a
positive integer-nanosecond latency under its client ID and zero-based request
ID; this identity determines the Q1-Q7 rotation, allowing the release checker
to reconstruct query counts and recompute p50, p95, and p99 independently.

## Publication gates

The strict artifact is not publication-ready unless all of these conditions
hold:

- exact duplicate-preserving parity for every node of Q1-Q7;
- zero warmups and ten same-host measured runs per query on both engines;
- at least 5x same-host speedup for every query and at least 10x geometric-mean
  speedup;
- no ratio derived from author-published cross-host timings;
- no more than 128 MiB serving storage, 16 MiB indexes, or 8 MiB binding data;
- fresh-process memory evidence while the complete owned tree remains live;
- complete, parity-checked 1/4/8/16-client sweeps with three epochs per level,
  at least five seconds and 32 requests per client in each epoch, no more than
  15% epoch-throughput coefficient of variation, at least 5x 16:1 scaling, and
  median p95 no more than 4x median p50 at each level; and
- matching source-tree, Docker-host, candidate-image, and database-image
  provenance.

The tail gate uses p95/p50 amplification rather than an absolute millisecond
ceiling because the benchmark records the Docker host but does not reserve or
standardize its CPU capacity. Absolute latency remains fully reported; the
scale-independent gate detects unstable tails without accepting or rejecting a
release because of unrelated host load.

See [`docs/ocpq-performance.md`](../../docs/ocpq-performance.md) for the
published results. Preview artifacts remain ignored and must be shown for
review before any replacement result is committed or pushed.
