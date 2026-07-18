# Releasing ocpm-engine

Every version change must include a dated `CHANGELOG.md` section in the same
commit. The section records user-visible API, compatibility, correctness, and
benchmark changes. `scripts/check-release-notes.py` rejects unsynchronized
Python, Cargo, native-module, or release-note versions.

Before publishing a version:

1. Synchronize `pyproject.toml`, the Cargo workspace, and native module version.
2. Add the dated release notes before changing the package version.
3. Run the Rust, wheel, Python, dependency-license, and committed public-result
   checks, then reproduce the public benchmark when execution paths change.
4. Commit the release atomically and tag the verified commit `vX.Y.Z`.
