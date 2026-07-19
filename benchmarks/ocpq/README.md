# OCPQ comparison benchmark

This benchmark compares `pg_ocpm` plus `ocpm-engine` with the ten raw OCPQ
timings published by the OCPQ authors. It does not use a vanilla PostgreSQL
comparison arm.

The workload is the public BPIC 2017-derived OCEL 2.0 dataset and Q1-Q7 query
trees from `aarkue/ocpq-eval` commit
`846dd4eb9f8600ae42355968453a9412ea4759c2`. OCPQ is pinned to version 0.6.7,
commit `80457e561edd7bb9e142d959dd7e0f96e6b03f2f`. The dataset ZIP SHA-256 is
`a5f422c72b0a911bd64383079f9faebfc247e3e5a217f30705ff9969e8547f2b`.

The OCPQ v0.6.7 lockfile no longer resolves on its historical Rust 1.76 image
because transitive dependency metadata raised its minimum Rust version. The
Dockerfile uses Rust 1.86 while retaining the exact OCPQ source commit and
lockfile.

## Pinned OCPQ environment

Clone the evaluation repository at the pinned commit and obtain its Git LFS
dataset, then build the separate OCPQ image:

```sh
git clone https://github.com/aarkue/ocpq-eval.git .benchmarks/ocpq-eval
git -C .benchmarks/ocpq-eval checkout 846dd4eb9f8600ae42355968453a9412ea4759c2
git -C .benchmarks/ocpq-eval lfs pull --include=bpic2017.sqlite.zip
unzip .benchmarks/ocpq-eval/bpic2017.sqlite.zip -d .benchmarks/ocpq-data
docker build -f benchmarks/ocpq/Dockerfile.ocpq -t ocpq:0.6.7-benchmark .
python benchmarks/ocpq/run_local_ocpq.py \
  --sqlite .benchmarks/ocpq-data/bpic2017.sqlite \
  --eval .benchmarks/ocpq-eval \
  --output .benchmarks/ocpq-local.json
```

Each OCPQ container imports and links the OCEL before executing ten measured
evaluations. Import/link time is outside the reported query samples, matching
the authors' published boundary.

## pg_ocpm plus ocpm-engine environment

Start a clean PostgreSQL 16 container containing `pg_ocpm 0.5.0`. Use
`prepare.py` to normalize the same SQLite file and build only the object types,
activities, and typed-neighbor summaries required by Q1-Q7. Then run:

```sh
python benchmarks/ocpq/prepare.py \
  --sqlite .benchmarks/ocpq-data/bpic2017.sqlite \
  --host postgres_ocpm
python benchmarks/ocpq/benchmark.py \
  --host postgres_ocpm \
  --output docs/results/ocpq-bpic2017-0.4.0.json
```

The candidate timing includes PostgreSQL execution, fetching one binary result
capsule, and native Rust decoding into an in-memory binding structure. Complete
row expansion is performed outside the timed region and must match the exact
row count, violation/value totals, and canonical SHA-256 fingerprint before a
sample is accepted. Related-object pair groups remain factorized and expand
through a lazy exact-size iterator.

Published OCPQ timings are cross-environment references. The separately pinned
local OCPQ run validates the workload and timing extraction, but the release
comparison table uses the author-published results as requested.
