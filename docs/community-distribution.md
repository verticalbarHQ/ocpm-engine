# Community wheel distribution

Public `ocpm-engine` releases provide source archives on GitHub and native
Python wheels through PyPI. The artifacts contain the same Apache-2.0 core
available in the public repository. No commercial license or customer
entitlement is required.

## Artifact contract

- Package: `ocpm-engine`
- Python: 3.11 or newer through PyO3's `abi3-py311` stable ABI
- Platforms: Linux `x86_64`, Linux `aarch64`, and macOS Apple Silicon
- DuckDB: deployment-supplied DuckDB 1.5.5 shared client, dynamically linked
- Source: public signed tag and GitHub-generated source archive

The wheel never embeds, downloads, installs, or operates DuckDB. Install and
verify the deployment-managed DuckDB client before importing `ocpm_engine`.

## Release contract

The `Community wheel release` workflow runs only from a reviewed signed
`vX.Y.Z` tag. It:

1. builds and audits each supported platform wheel;
2. rejects Rust, C, and C++ sources, Cargo metadata, tests, benchmarks, static
   archives, bundled DuckDB binaries, and pure-Python wheels;
3. smoke-tests the installed wheel against the external DuckDB client;
4. emits SHA-256 checksums, a platform audit manifest, an SPDX SBOM, and build
   provenance;
5. attaches those artifacts to the matching public GitHub release; and
6. publishes only the audited wheels to PyPI through trusted publishing.

The workflow must be dispatched against the signed tag, with the same tag in
the `release_tag` input and the exact confirmation value
`publish-community-ocpm-engine`. It refuses publication when the tag, package
version, workflow commit, repository visibility, or GitHub release state does
not match.

Before the first release, a project owner must reserve `ocpm-engine` on PyPI and
configure the GitHub `pypi` environment as a trusted publisher for
`.github/workflows/community-wheel.yml`. No long-lived PyPI token is used.

## Installation

After installing the supported DuckDB shared client:

```sh
python -m pip install --only-binary=:all: ocpm-engine==1.1.0
python -c 'import ocpm_engine; print(ocpm_engine.__version__)'
```

## Support boundary

Community artifacts are provided under Apache-2.0 without support or warranty.
Certified builds, supported update channels, managed services, service levels,
warranty, and indemnification are separate paid offerings described in the
[commercial offering boundary](https://github.com/verticalbarHQ/ocpm-engine/blob/main/COMMERCIAL.md).
