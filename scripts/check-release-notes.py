#!/usr/bin/env python3
"""Fail when the package version has no matching release-note heading."""

from __future__ import annotations

import pathlib
import re

root = pathlib.Path(__file__).resolve().parents[1]
pyproject = (root / "pyproject.toml").read_text()
match = re.search(r'^version = "([^"]+)"$', pyproject, re.MULTILINE)
if match is None:
    raise SystemExit("could not read project version from pyproject.toml")
version = match.group(1)
changelog = (root / "CHANGELOG.md").read_text()
if not re.search(
    rf"^## {re.escape(version)} - \d{{4}}-\d{{2}}-\d{{2}}$",
    changelog,
    re.MULTILINE,
):
    raise SystemExit(f"CHANGELOG.md has no release notes for ocpm-engine {version}")

cargo = (root / "Cargo.toml").read_text()
cargo_match = re.search(
    r'^\[workspace\.package\]\s+version = "([^"]+)"',
    cargo,
    re.MULTILINE,
)
if cargo_match is None or cargo_match.group(1) != version:
    raise SystemExit("Cargo workspace version does not match pyproject.toml")

native = (root / "crates/ocpm-python/src/lib.rs").read_text()
if f'module.add("__version__", "{version}")?' not in native:
    raise SystemExit("native Python module version does not match pyproject.toml")

lock = (root / "uv.lock").read_text()
lock_match = re.search(
    r'\[\[package\]\]\s+name = "ocpm-engine"\s+version = "([^"]+)"',
    lock,
    re.MULTILINE,
)
if lock_match is None or lock_match.group(1) != version:
    raise SystemExit("uv.lock package version does not match pyproject.toml")

citation = (root / "CITATION.cff").read_text()
citation_match = re.search(r"^version:\s*([^\s]+)$", citation, re.MULTILINE)
if citation_match is None or citation_match.group(1) != version:
    raise SystemExit("CITATION.cff version does not match pyproject.toml")

cargo_lock = (root / "Cargo.lock").read_text()
for package in ("ocpm-core", "ocpm-postgres", "ocpm-python"):
    package_match = re.search(
        rf'^name = "{re.escape(package)}"\nversion = "([^"]+)"$',
        cargo_lock,
        re.MULTILINE,
    )
    if package_match is None or package_match.group(1) != version:
        raise SystemExit(f"Cargo.lock {package} version does not match pyproject.toml")

engine = (root / "src/ocpm_engine/engine.py").read_text()
minimum_match = re.search(
    r"^_MINIMUM_PG_OCPM_VERSION = \((\d+), (\d+), (\d+)\)$",
    engine,
    re.MULTILINE,
)
if minimum_match is None:
    raise SystemExit("could not read the minimum pg_ocpm version")
minimum_pg_ocpm = ".".join(minimum_match.groups())
readme = (root / "README.md").read_text()
if f"Required extension version: `pg_ocpm >= {minimum_pg_ocpm}`." not in readme:
    raise SystemExit("README.md minimum pg_ocpm version is stale")
if f"pg_ocpm {minimum_pg_ocpm} or later" not in engine:
    raise SystemExit("runtime pg_ocpm compatibility error is stale")

for heading in re.findall(r"^## ([0-9]+\.[0-9]+\.[0-9]+) -", changelog, re.MULTILINE):
    body = re.search(
        rf"^## {re.escape(heading)} - [^\n]+\n(?P<body>.*?)(?=^## |\Z)",
        changelog,
        re.MULTILINE | re.DOTALL,
    )
    if body is None or not re.search(r"^- ", body.group("body"), re.MULTILINE):
        raise SystemExit(f"release notes for {heading} contain no change entries")

benchmark_lock = (root / "benchmarks/public/requirements.lock").read_text()
for requirement in (
    "pm4py==2.7.23.3",
    "psutil==7.2.2",
    "psycopg2-binary==2.9.12",
):
    if not re.search(
        rf"^{re.escape(requirement)}(?:\s|\\)", benchmark_lock, re.MULTILINE
    ):
        raise SystemExit(f"public benchmark lock is missing {requirement}")
if "--hash=sha256:" not in benchmark_lock:
    raise SystemExit("public benchmark dependency lock contains no hashes")

benchmark_dockerfile = (root / "benchmarks/public/Dockerfile.client").read_text()
if "maturin==1.9.6" not in benchmark_dockerfile:
    raise SystemExit("public benchmark maturin version is not pinned")
if "--require-hashes" not in benchmark_dockerfile:
    raise SystemExit("public benchmark does not enforce dependency hashes")
if (
    len(re.findall(r"^FROM .+@sha256:[0-9a-f]{64}", benchmark_dockerfile, re.MULTILINE))
    != 2
):
    raise SystemExit("public benchmark base images are not digest-pinned")
print(f"release notes present for ocpm-engine {version}")
