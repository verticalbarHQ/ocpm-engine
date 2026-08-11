# Installing ocpm-engine

`ocpm-engine` is a standalone, Rust-first object-centric process-mining engine
with Python 3.11+ stable-ABI bindings. It runs against in-memory event data,
OCEL JSON, XES, CSV, and SQLite without any database, and can optionally push
work into PostgreSQL (via the [`pg_ocpm`](https://github.com/verticalbarHQ/pg_ocpm)
extension) or an existing DuckDB catalog over Parquet snapshots.

Current version: **1.1.0**.

Choose one installation path:

1. [PyPI community wheel](#install-the-community-wheel-from-pypi)
2. [Certified enterprise wheel](#install-a-certified-enterprise-wheel)
3. [Build from source](#build-from-source)

Then, if you plan to use a database provider:

- [Using ocpm-engine with PostgreSQL (pg_ocpm)](#using-ocpm-engine-with-postgresql-pg_ocpm)
- [Using the DuckDB Parquet provider](#using-the-duckdb-parquet-provider)

## Requirements

- Python 3.11 or newer (wheels use PyO3's `abi3-py311` stable ABI)
- Supported platforms: Linux `x86_64`, Linux `aarch64`, macOS Apple Silicon
- For source builds: Rust 1.85.1 or newer and `maturin==1.14.1`
- For the DuckDB provider: a deployment-supplied DuckDB **1.5** client library
  (`ocpm-engine` never bundles, downloads, or embeds DuckDB)

## Install the community wheel from PyPI

Install the deployment-managed DuckDB 1.5.5 shared client in the normal dynamic
loader path first. The wheel dynamically links to that client and never bundles,
downloads, or installs DuckDB. Then install the signed-tag community release:

```sh
python -m pip install --only-binary=:all: ocpm-engine==1.1.0
```

Public wheel controls, checksums, provenance, and supported platforms are
documented in [docs/community-distribution.md](docs/community-distribution.md).

## Install a certified enterprise wheel

Certified builds are served from a private PEP 503-compatible index with
supported update access and commercial assurance. Authenticate with a
short-lived registry credential. For AWS CodeArtifact:

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
public registry.

Verify:

```python
from ocpm_engine import StandaloneEngine
```

## Build from source

For local development:

```sh
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
pytest
```

To build a release-style native wheel without publishing, supply the
deployment-managed DuckDB header and library locations:

```sh
export DUCKDB_INCLUDE_DIR=/opt/duckdb/include
export DUCKDB_LIB_DIR=/opt/duckdb/lib
python -m pip install 'maturin==1.14.1'
make PYTHON=python DIST_DIR="$PWD/dist" private-wheel
```

The Docker-isolated Linux build selects the checksum-pinned external DuckDB
client for the requested architecture and exports only the wheel and manifest:

```sh
docker buildx build \
  -f packaging/wheel/Dockerfile \
  --target artifact \
  --output type=local,dest=dist/linux .
```

## Using ocpm-engine with PostgreSQL (pg_ocpm)

If your event data lives in PostgreSQL, install the
[`pg_ocpm`](https://github.com/verticalbarHQ/pg_ocpm) extension on the server.
`pg_ocpm` stores the normalized `ocpm` schema and computes exact factorized
batches and sufficient statistics next to the data; `ocpm-engine` negotiates
the installed capability surface and consumes those results directly.

Version pairing:

| ocpm-engine | pg_ocpm | Path |
| --- | --- | --- |
| >= 1.0 | >= 1.0.0 | Native provider: selective scans and sufficient-statistic aggregation |
| >= 1.0 | 0.9.x | Factorized single- and multi-window activity-path batches |
| >= 1.0 | 0.8.x | Exact asynchronous event-row stream (compatibility fallback) |

Install steps:

1. Install `pg_ocpm` on the PostgreSQL server by following the
   [pg_ocpm installation guide](https://github.com/verticalbarHQ/pg_ocpm/blob/main/INSTALL.md)
   (prebuilt package, Docker image, or source build; PostgreSQL 13–18).
2. Enable it in your database:

   ```sql
   CREATE EXTENSION pg_ocpm;
   SELECT ocpm.version();
   ```

3. Load normalized facts and finalize the dataset (`ocpm.finish_load(...)`);
   see the [pg_ocpm data model](https://github.com/verticalbarHQ/pg_ocpm#data-model).
4. From Python, verify the dependency at application startup and let the
   planner select the best installed path:

   ```python
   from ocpm_engine import OcpmEngine

   engine = OcpmEngine(dataset_id=42, tenant_id=7)
   version = engine.verify_pg_ocpm(cursor)
   capabilities = engine.inspect_pg_ocpm(cursor)
   ```

For large remote results, use a driver-level server-side cursor so the Python
database driver does not buffer every factorized row; see the README's
[Use section](README.md#use) for the psycopg2 example.

## Using the DuckDB Parquet provider

The DuckDB provider dynamically links to a compatible deployment-supplied
DuckDB 1.5 client and opens an existing caller-selected catalog. Install the
deployment-managed DuckDB 1.5.5 shared client in the normal dynamic-loader path
and verify its checksum before importing the provider. The package fails
clearly when `libduckdb` is absent or incompatible; it never downloads,
compiles, installs, or silently substitutes DuckDB, and it never creates the
catalog implicitly.

```python
from ocpm_engine import StandaloneEngine

engine = StandaloneEngine.from_duckdb_parquet(
    {
        "database": {
            "kind": "existing",
            "path": "/catalog/analytics.duckdb",
            "read_only": True,
        },
        "location": {"kind": "local", "root": "/data/ocel-parquet"},
        "snapshot": {"kind": "current", "pointer": "CURRENT"},
        "layout": {"kind": "canonical_v1"},
        "cache": {"kind": "direct"},
    }
)
```

S3 uses the same API with a structured `s3://` location and deployment
credential-chain references.

## Standalone quick start

No database is required for file-based analysis:

```python
from ocpm_engine import StandaloneEngine

engine = StandaloneEngine.from_ocel2_json("events.json")
profile = engine.profile({"object_types": ["Order"]})
model = engine.discover(
    {"view": {"object_types": ["Order"]}, "algorithm": "object_centric_dfg"}
)
```

## Troubleshooting

- `pip` tries to build from source or resolves an unexpected package: pass
  `--only-binary=:all:` and confirm that the selected public or private index
  contains a wheel for your platform.
- Import fails mentioning `libduckdb`: the DuckDB provider requires the
  deployment-supplied DuckDB 1.5 client on the dynamic-loader path; the wheel
  intentionally does not bundle it.
- `verify_pg_ocpm` fails: confirm `CREATE EXTENSION pg_ocpm` ran in the
  database you are connected to and that `SELECT ocpm.version();` reports a
  supported version (see the pairing table above).
- Python older than 3.11: unsupported; wheels target the `abi3-py311` stable
  ABI and the package requires `python >= 3.11`.
