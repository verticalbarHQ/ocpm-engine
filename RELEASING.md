# Releasing ocpm-engine

Every version change must include a dated `CHANGELOG.md` section in the same
commit. The section records user-visible API, compatibility, correctness, and
benchmark changes. `scripts/check-release-notes.py` rejects unsynchronized
Python, Cargo, native-module, or release-note versions.

Before publishing a version:

1. Synchronize `pyproject.toml`, the Cargo workspace, and native module version.
2. Add the dated release notes before changing the package version.
3. Run `make dependency-boundary-check`, `make license-check`, then run the
   Rust, wheel, Python, dependency-license, and committed public-result checks. Reproduce the public
   benchmark when execution paths change.
4. Commit the release atomically and tag the verified commit `vX.Y.Z`.

Private Python artifacts are built by the `Private wheel distribution`
workflow. Run it without publishing first and inspect every platform wheel,
manifest, checksum, SBOM, installed-wheel smoke test, and provenance result.
Publishing requires a signed tag at the workflow commit, an exact confirmation
string, and a short-lived GitHub OIDC session scoped to one private
CodeArtifact repository. The uploader refuses PyPI and TestPyPI URLs and never
builds a source distribution.

Private registry access controls the supported delivery channel, provenance,
and customer entitlement. It does not narrow the Apache-2.0 rights attached to
the core wheel. Proprietary additions or alternative commercial licenses must
be documented and packaged separately from the unmodified Apache core.

The wheel excludes Rust/C sources, Cargo metadata, tests, benchmarks, and the
DuckDB client library. The current compatibility façade remains Python source
and is reported in the artifact manifest. Binary-only delivery is an artifact
format, not a restriction on the Apache-2.0 source rights. Separately licensed
components must remain outside the public core. See
[`docs/private-distribution.md`](docs/private-distribution.md).
