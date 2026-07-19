# OCPQ comparison benchmark

This directory contains the correctness-gated, same-host comparison between
OCPQ 0.6.7 and `pg_ocpm 0.6.0` plus `ocpm-engine 0.5.0`. The workload is Q1-Q7
from the public BPIC 2017-derived OCEL 2.0 evaluation dataset.

The ten timings published by the OCPQ authors are preserved as source context
only. They were collected on a different host, so neither the runner, checker,
nor report calculates a ratio from them. Every reported speedup uses the pinned
same-host OCPQ reproduction.

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
  --warmups 10 \
  --runs 30 \
  --output .benchmarks/ocpq-reproduced-0.6.7-preview.json
```

The reference timer includes `tree.evaluate` and construction and collection
of every node's `EvaluationResultWithCount`. Import, linking, external-ID
canonicalization, sorting, JSON serialization, and hashing are reported or
performed outside the query timer.

A regenerated reference has new same-host samples and a new artifact digest.
Pin that reference first, then begin the release candidate run from a clean
checkout so its recorded source revision describes the code actually built.

## Clean candidate rerun

Run the following from a clean `ocpm-engine` checkout using the pinned reference
artifact. It installs the loader dependency, explicitly builds the
sibling `pg_ocpm` source as a PostgreSQL 16 image, creates the network only when
needed, waits for database readiness, and removes the benchmark container and
any network it created on completion or exit.

```bash
uv sync --extra benchmark

PG_OCPM_SOURCE="${PG_OCPM_SOURCE:-../pg_ocpm}"
OCPM_CONTAINER="postgres_ocpq_pg_ocpm"
export OCPM_DOCKER_NETWORK="pg-ocpm-ocpq-bench"
export OCPM_DATABASE_IMAGE="pg_ocpm:0.6.0"
export OCPM_CANDIDATE_IMAGE="ocpm-engine:ocpq-candidate"
export OCPM_DATABASE_URL="postgres://postgres:pg@${OCPM_CONTAINER}/postgres"
export OCPM_SOURCE_REVISION="$(git rev-parse HEAD)"

if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then
  echo "release reruns require a clean ocpm-engine checkout" >&2
  exit 1
fi
export OCPM_SOURCE_TREE_CLEAN=true

if [[ -n "$(git -C "$PG_OCPM_SOURCE" status --porcelain --untracked-files=all)" ]]; then
  echo "release reruns require a clean pg_ocpm checkout" >&2
  exit 1
fi
export OCPM_PG_OCPM_SOURCE_REVISION="$(git -C "$PG_OCPM_SOURCE" rev-parse HEAD)"
export OCPM_PG_OCPM_SOURCE_TREE_CLEAN=true

network_created=false
cleanup() {
  docker rm --force "$OCPM_CONTAINER" >/dev/null 2>&1 || true
  if [[ "$network_created" == true ]]; then
    docker network rm "$OCPM_DOCKER_NETWORK" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

docker build --build-arg PG_MAJOR=16 \
  --build-arg SOURCE_REVISION="$OCPM_PG_OCPM_SOURCE_REVISION" \
  --tag "$OCPM_DATABASE_IMAGE" "$PG_OCPM_SOURCE"
docker rm --force "$OCPM_CONTAINER" >/dev/null 2>&1 || true
if ! docker network inspect "$OCPM_DOCKER_NETWORK" >/dev/null 2>&1; then
  docker network create "$OCPM_DOCKER_NETWORK" >/dev/null
  network_created=true
fi
docker run --detach --name "$OCPM_CONTAINER" \
  --network "$OCPM_DOCKER_NETWORK" \
  --publish 55436:5432 \
  --env POSTGRES_PASSWORD=pg \
  "$OCPM_DATABASE_IMAGE" >/dev/null

ready=false
for attempt in $(seq 1 60); do
  if docker exec "$OCPM_CONTAINER" \
    pg_isready --username postgres --dbname postgres >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "PostgreSQL did not become ready within 60 seconds" >&2
  exit 1
fi

uv run --extra benchmark python benchmarks/ocpq/prepare.py \
  --sqlite .benchmarks/ocpq-data/bpic2017.sqlite \
  --host localhost \
  --port 55436

benchmarks/ocpq/run_candidate_docker.sh \
  .benchmarks/ocpq-reproduced-0.6.7-preview.json \
  .benchmarks/ocpq-bpic2017-0.5.0-host-provenance-preview.json

benchmarks/ocpq/run_candidate_memory_docker.sh \
  .benchmarks/ocpq-reproduced-0.6.7-preview.json \
  .benchmarks/ocpq-bpic2017-0.5.0-host-provenance-memory-preview.json

candidate_sha="$(
  shasum -a 256 .benchmarks/ocpq-bpic2017-0.5.0-host-provenance-preview.json | awk '{print $1}'
)"
memory_sha="$(
  shasum -a 256 .benchmarks/ocpq-bpic2017-0.5.0-host-provenance-memory-preview.json | awk '{print $1}'
)"
reference_sha="$(
  shasum -a 256 .benchmarks/ocpq-reproduced-0.6.7-preview.json | awk '{print $1}'
)"
uv run --extra benchmark python benchmarks/check_ocpq_result.py \
  --reference .benchmarks/ocpq-reproduced-0.6.7-preview.json \
  --candidate .benchmarks/ocpq-bpic2017-0.5.0-host-provenance-preview.json \
  --memory .benchmarks/ocpq-bpic2017-0.5.0-host-provenance-memory-preview.json \
  --expected-reference-sha256 "$reference_sha" \
  --expected-candidate-sha256 "$candidate_sha" \
  --expected-memory-sha256 "$memory_sha"

cleanup
trap - EXIT INT TERM
```

The loader preserves directed object-object relations and builds only the
generic object, activity, typed-neighbor, and relation summaries needed by
Q1-Q7. Loading, `ocpm.finish_load`, and `ocpm.rebuild_binding_index` are
preprocessing costs and are not part of lookup latency.

The candidate timer covers a persistent prepared PostgreSQL query, fetching one
binary capsule, native Rust decoding, and complete expansion into owned logical
root rows. Every warmup and measured query is canonicalized and checked exactly
after its timer. In each concurrency epoch, every client performs one exact
canonical Q1-Q7 check before entering the timed barrier and another after all
timed requests finish. Timed requests enforce the reference logical-row count
and persist per-client and per-query counts; they do not canonicalize and hash
each response inside the serving clock.

The normal run is fixed at ten warmups and 30 measured samples. Its concurrency
sweep uses levels 1/4/8/16, persistent prepared connections, and three epochs
per level. Every epoch runs for at least five seconds and completes at least 32
mixed Q1-Q7 requests per client. The memory runner starts one
fresh, single-worker container per query. It samples Linux RSS and VmHWM while
decoded rows remain live, then performs exact canonical parity after the sample
so the external-ID maps do not contaminate the memory boundary.

For an intentionally dirty working-tree run, regenerate the reference, set
`OCPM_SOURCE_REVISION` to the current 40-hex `HEAD`, set both source-tree-clean
flags to `false`, and pass `--allow-preview` plus all three explicitly computed
SHA-256 values to the checker. Preview mode never bypasses artifact digest
verification. The default checker mode requires matching clean harness
provenance on the OCPQ reference and candidate, a clean 40-hex `pg_ocpm`
revision, one hashed Docker-daemon identity, and immutable reference,
candidate, and database Docker image IDs.

## Release gates

The checker rejects an artifact unless all of these conditions hold:

- exact Q1-Q7 row-multiset, duplicate, violation/value, and SHA-256 parity;
- exactly ten warmups and 30 same-host samples per query;
- at least 10x same-host speedup for every query (with 5x as the hard design
  floor) and a 10x geometric-mean target;
- no speedup or ratio derived from author-published cross-host timings;
- no more than 128 MiB serving storage, 16 MiB indexes, or 8 MiB binding data;
- no more than 8 MiB fresh-process peak RSS over baseline;
- complete, parity-checked 1/4/8/16-client sweeps with three five-second epochs,
  at least 32 requests per client per epoch, throughput CV no greater than 15%,
  16-to-1 median-throughput scaling of at least 5x, and median epoch p95 latency
  below 10 ms at every level;
- exact Q1-Q7 aggregate counts, per-client request counts, and pre/post parity
  evidence for every concurrency epoch;
- clean 40-hex source provenance and immutable candidate and database image IDs
  in default release mode.

See [`docs/ocpq-performance.md`](../../docs/ocpq-performance.md) for the recorded
results and limitations.
