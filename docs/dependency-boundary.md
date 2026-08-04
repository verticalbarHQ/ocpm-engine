# Production and benchmark dependency boundary

`ocpm-engine` and `pg_ocpm` implement their production algorithms independently
from peer-reviewed publications. OCPA, Rust4PM, and PM4Py are comparison
systems, not implementation sources. OCPQ is named only in archived benchmark
results; no runnable OCPQ benchmark integration is supplied.

The production boundary is intentionally strict:

- the `ocpm-engine` wheel has no Python runtime dependencies;
- the production Rust workspace and `pg_ocpm` extension do not depend on,
  import, link, vendor, or fetch any comparison project;
- PM4Py and OCPA are installed only inside disposable, pinned benchmark client
  images;
- Rust4PM is an exact-version registry dependency only of its disposable
  benchmark adapter; and
- the archived OCPQ result contains no associated runner, checker, adapter,
  image builder, dataset loader, or reconstruction instructions.

Benchmark requirements and locks live below `benchmarks/` and are never
exported as package extras. `scripts/check-core-dependency-boundary.py` enforces
this distinction in CI by inspecting production manifests, production imports,
and root lockfiles.

Algorithm changes remain governed by
[`academic-implementation-provenance.md`](academic-implementation-provenance.md).
An equivalent dependency gate in `pg_ocpm` prevents reference projects from
entering extension includes, link directives, extension dependencies, or
vendored paths.
