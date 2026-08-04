#!/usr/bin/env python3
"""Fail closed when a private ocpm-engine wheel exposes forbidden payloads."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import tempfile
import zipfile


class WheelVerificationError(RuntimeError):
    pass


FORBIDDEN_SUFFIXES = {
    ".a",
    ".c",
    ".cc",
    ".cpp",
    ".h",
    ".hpp",
    ".lock",
    ".o",
    ".rs",
    ".toml",
}
FORBIDDEN_PARTS = {".git", "benchmarks", "crates", "tests"}


def _metadata_value(text: str, key: str) -> str | None:
    prefix = f"{key}: "
    return next(
        (line[len(prefix) :] for line in text.splitlines() if line.startswith(prefix)),
        None,
    )


def _native_dependencies(native_path: pathlib.Path) -> str:
    if shutil.which("readelf"):
        result = subprocess.run(
            ["readelf", "-d", str(native_path)], capture_output=True, text=True
        )
        if result.returncode == 0:
            return result.stdout
    if shutil.which("otool"):
        result = subprocess.run(
            ["otool", "-L", str(native_path)], capture_output=True, text=True
        )
        if result.returncode == 0:
            return result.stdout
    return ""


def verify_wheel(
    path: pathlib.Path, *, require_external_duckdb: bool = True
) -> dict[str, object]:
    if path.suffix != ".whl" or not path.is_file():
        raise WheelVerificationError(f"not a wheel: {path}")
    if re.search(r"-(?:py3|py2\.py3)-none-any\.whl$", path.name):
        raise WheelVerificationError("ocpm-engine must be a native wheel")

    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        forbidden = [
            name
            for name in names
            if pathlib.PurePosixPath(name).suffix.lower() in FORBIDDEN_SUFFIXES
            or FORBIDDEN_PARTS.intersection(pathlib.PurePosixPath(name).parts)
        ]
        if forbidden:
            raise WheelVerificationError(
                f"wheel contains implementation/build source: {forbidden}"
            )

        embedded_duckdb = [
            name
            for name in names
            if "libduckdb" in name.lower() or "duckdb.dll" in name.lower()
        ]
        if embedded_duckdb:
            raise WheelVerificationError(f"wheel bundles DuckDB: {embedded_duckdb}")

        native = [
            name
            for name in names
            if name.startswith("ocpm_engine/_native")
            and pathlib.PurePosixPath(name).suffix.lower() in {".so", ".pyd", ".dylib"}
        ]
        if len(native) != 1:
            raise WheelVerificationError(f"expected one native module, found {native}")

        metadata_names = [
            name for name in names if name.endswith(".dist-info/METADATA")
        ]
        wheel_names = [name for name in names if name.endswith(".dist-info/WHEEL")]
        if len(metadata_names) != 1 or len(wheel_names) != 1:
            raise WheelVerificationError("wheel metadata is incomplete or ambiguous")
        metadata = archive.read(metadata_names[0]).decode("utf-8")
        wheel_metadata = archive.read(wheel_names[0]).decode("utf-8")
        if _metadata_value(metadata, "Name") != "ocpm-engine":
            raise WheelVerificationError("unexpected package name")
        if _metadata_value(metadata, "License-Expression") != "Apache-2.0":
            raise WheelVerificationError(
                "wheel is missing the Apache-2.0 license expression"
            )
        license_basenames = {
            pathlib.PurePosixPath(name).name
            for name in names
            if ".dist-info/licenses/" in name
        }
        required_license_files = {
            "LICENSE",
            "NOTICE",
            "COPYRIGHT.md",
            "THIRD_PARTY_NOTICES.md",
            "THIRD_PARTY_LICENSES.html",
        }
        missing_license_files = required_license_files - license_basenames
        if missing_license_files:
            raise WheelVerificationError(
                "wheel is missing license files: "
                + ", ".join(sorted(missing_license_files))
            )
        if "Root-Is-Purelib: false" not in wheel_metadata:
            raise WheelVerificationError(
                "wheel incorrectly declares a pure-Python payload"
            )

        python_sources = [name for name in names if name.endswith(".py")]
        python_source_bytes = sum(
            archive.getinfo(name).file_size for name in python_sources
        )
        native_name = native[0]
        with tempfile.TemporaryDirectory(prefix="ocpm-wheel-audit-") as temp_dir:
            native_path = pathlib.Path(
                temp_dir, pathlib.PurePosixPath(native_name).name
            )
            native_path.write_bytes(archive.read(native_name))
            dependencies = _native_dependencies(native_path)
        if require_external_duckdb and not dependencies:
            raise WheelVerificationError(
                "could not inspect the native module's dynamic dependencies"
            )
        if require_external_duckdb and "duckdb" not in dependencies.lower():
            raise WheelVerificationError(
                "native module does not declare external DuckDB linkage"
            )

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "artifact": path.name,
        "sha256": digest,
        "bytes": path.stat().st_size,
        "native_module": native_name,
        "external_duckdb_dependency_verified": bool(
            dependencies and "duckdb" in dependencies.lower()
        ),
        "python_facade_files": python_sources,
        "python_facade_bytes": python_source_bytes,
        "prohibited_source_files": [],
        "bundled_duckdb_files": [],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=pathlib.Path)
    parser.add_argument("--manifest", type=pathlib.Path)
    parser.add_argument("--allow-unverified-duckdb-link", action="store_true")
    args = parser.parse_args()
    result = verify_wheel(
        args.wheel,
        require_external_duckdb=not args.allow_unverified_duckdb_link,
    )
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.manifest:
        args.manifest.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
