# Private wheel distribution

Vertical Bar, Inc. distributes supported `ocpm-engine` builds to customers as
a native wheel from a private PEP 503-compatible index. The core is licensed
under Apache-2.0. The supported release process never builds or uploads a source
distribution and explicitly refuses public PyPI endpoints.

## Artifact contract

- Package name: `ocpm-engine`
- Python: 3.11 or newer through PyO3's `abi3-py311` stable ABI
- Platforms: a separate native wheel per operating system and architecture
- DuckDB: dynamically linked deployment dependency; never embedded in a wheel
- Registry: private AWS CodeArtifact or another access-controlled PEP 503 index
- npm: not used unless a separate supported Node SDK is introduced

The current release matrix covers Linux `x86_64`, Linux `aarch64`, and macOS
Apple Silicon. Windows and Intel macOS wheels are not produced until those
platforms have native build, linkage-audit, and installed-wheel coverage.

The current DuckDB-enabled Linux wheel is intentionally a platform wheel, not a
self-contained manylinux wheel. The deployment must provide the exact supported
DuckDB 1.5 client in its normal dynamic-loader path before importing
`ocpm_engine`. This keeps DuckDB outside the product artifact and preserves the
existing-catalog boundary.

## Customer installation

Authenticate with a short-lived registry credential. For AWS CodeArtifact:

```sh
aws codeartifact login \
  --tool pip \
  --domain YOUR_DOMAIN \
  --repository YOUR_REPOSITORY

python -m pip install --only-binary=:all: ocpm-engine==1.1.0
```

Use the private index as the only index for this installation. Do not use an
`--extra-index-url` configuration that can resolve the same package name from a
public registry. Install the deployment-managed DuckDB 1.5.5 shared client and
verify its checksum before importing the DuckDB provider.

The package must fail clearly when `libduckdb` is absent or incompatible; it
must never download, compile, install, or silently substitute DuckDB.

## Building without publishing

Set the deployment-supplied DuckDB header and library locations, then build:

```sh
export DUCKDB_INCLUDE_DIR=/opt/duckdb/include
export DUCKDB_LIB_DIR=/opt/duckdb/lib
python -m pip install 'maturin==1.14.1'
make PYTHON=python DIST_DIR="$PWD/dist" private-wheel
```

The builder uses `maturin build`, never `maturin sdist`. The wheel audit rejects
Rust/C/C++ sources, Cargo manifests and locks, tests, benchmarks, static
archives, bundled DuckDB binaries, and pure-Python wheels. It records the wheel
hash, native module, external DuckDB linkage, and visible Python façade in a
machine-readable manifest.
The strict linkage inspection runs on the wheel's build platform. Publishing
revalidates the wheel payload and requires the matching platform manifest,
bound to the wheel by SHA-256, before any upload.

The Docker-isolated Linux build selects the checksum-pinned external DuckDB
client for the requested architecture and exports only the wheel and manifest:

```sh
docker buildx build \
  -f packaging/wheel/Dockerfile \
  --target artifact \
  --output type=local,dest=dist/linux .
```

## Publishing

The `Private wheel distribution` GitHub workflow is build-only by default. A
publish run requires all of the following:

1. A reviewed signed tag resolving to the workflow commit.
2. The exact `publish-private-ocpm-engine` confirmation input.
3. A GitHub OIDC role restricted to the one CodeArtifact domain/repository.
4. Repository variables `AWS_REGION`, `AWS_CODEARTIFACT_DOMAIN`, and
   `AWS_CODEARTIFACT_REPOSITORY`.
5. Secret `AWS_CODEARTIFACT_PUBLISH_ROLE_ARN` and, when used, variable
   `AWS_CODEARTIFACT_DOMAIN_OWNER`.

The uploader accepts only explicit HTTPS repository URLs and refuses PyPI and
TestPyPI. CI attaches SHA-256 checksums, an SBOM, and build provenance to the
private release artifacts.

## Source and artifact boundary

The wheel contains compiled Rust kernels and does not contain the Rust source
tree. The Apache-2.0 source remains available through the source release. The
wheel still contains the Python compatibility façade required by the current
public API. Wheel recipients can inspect that Python. The artifact
manifest reports those files and their aggregate size so the boundary cannot be
mistaken for complete source concealment.

Separately developed proprietary components should remain outside the public
core and outside its source release. Do not replace `.py` files with bytecode
as a security control; bytecode is reversible and introduces
interpreter-version coupling.

Compiled native modules can also be reverse engineered. A managed API remains
the strongest option for customers who do not require on-premises execution.
