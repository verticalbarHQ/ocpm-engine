#!/usr/bin/env python3
"""Fail when the package version has no matching release-note heading."""

from __future__ import annotations

import pathlib
import re
import tomllib

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

benchmark_requirements = (
    "pm4py==2.7.23.3",
    "psutil==7.2.2",
    "psycopg2-binary==2.9.12",
)
benchmark_input = (root / "benchmarks/public/requirements.in").read_text()
input_requirements = {
    line.strip()
    for line in benchmark_input.splitlines()
    if line.strip() and not line.lstrip().startswith("#")
}
if input_requirements != set(benchmark_requirements):
    raise SystemExit("public benchmark direct requirements are not minimal and exact")

benchmark_lock = (root / "benchmarks/public/requirements.lock").read_text()
for requirement in benchmark_requirements:
    if not re.search(
        rf"^{re.escape(requirement)}(?:\s|\\)", benchmark_lock, re.MULTILINE
    ):
        raise SystemExit(f"public benchmark lock is missing {requirement}")
if "--hash=sha256:" not in benchmark_lock:
    raise SystemExit("public benchmark dependency lock contains no hashes")

project_metadata = tomllib.loads(pyproject)
project_dependencies = project_metadata["project"].get("dependencies", [])
project_extras = project_metadata["project"].get("optional-dependencies", {})
if project_dependencies:
    raise SystemExit("ocpm-engine must not declare Python runtime dependencies")
if "benchmark" in project_extras:
    raise SystemExit("benchmark packages must not be exported as a package extra")

uv_metadata = tomllib.loads(lock)
forbidden_root_packages = {"ocpa", "pm4py", "process-mining", "rust4pm"}
resolved_root_packages = {
    package["name"].lower() for package in uv_metadata.get("package", [])
}
unexpected_root_packages = forbidden_root_packages & resolved_root_packages
if unexpected_root_packages:
    unexpected = ", ".join(sorted(unexpected_root_packages))
    raise SystemExit(f"root uv.lock resolves benchmark-only packages: {unexpected}")

for removed_benchmark_path in (
    "benchmarks/ocpq",
    "benchmarks/check_ocpq_four_way.py",
    "benchmarks/check_ocpq_result.py",
):
    if (root / removed_benchmark_path).exists():
        raise SystemExit(
            f"archival OCPQ results must not regain benchmark code: "
            f"{removed_benchmark_path}"
        )

rust4pm_manifest = (root / "benchmarks/ecosystem/rust4pm/Cargo.toml").read_text()
if 'process_mining = { version = "=0.6.0"' not in rust4pm_manifest:
    raise SystemExit("Rust4PM benchmark dependency is not exact-version pinned")
if "git =" in rust4pm_manifest:
    raise SystemExit("Rust4PM benchmark must install from the registry, not Git")

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

ocpa_input = (root / "benchmarks/ecosystem/requirements.ocpa.in").read_text()
ocpa_requirements = {
    line.strip()
    for line in ocpa_input.splitlines()
    if line.strip() and not line.lstrip().startswith("#")
}
if ocpa_requirements != {"ocpa==1.3.4"}:
    raise SystemExit("OCPA benchmark input must contain only the exact OCPA pin")
ocpa_lock = (root / "benchmarks/ecosystem/requirements.ocpa.lock").read_text()
if "--hash=sha256:" not in ocpa_lock:
    raise SystemExit("OCPA benchmark dependency lock contains no hashes")
ocpa_dockerfile = (root / "benchmarks/ecosystem/Dockerfile.ocpa").read_text()
if "--require-hashes" not in ocpa_dockerfile:
    raise SystemExit("OCPA benchmark does not enforce dependency hashes")
if "COPY benchmarks ./benchmarks" in ocpa_dockerfile:
    raise SystemExit("OCPA benchmark image copies files outside its minimal adapter")
print(f"release notes present for ocpm-engine {version}")
