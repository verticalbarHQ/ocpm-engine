#!/usr/bin/env python3
"""Upload wheel-only artifacts while refusing public Python registries."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import urllib.parse

PUBLIC_HOSTS = {
    "pypi.org",
    "test.pypi.org",
    "upload.pypi.org",
    "test.pypi.org",
    "files.pythonhosted.org",
}


def validate_private_repository_url(value: str) -> str:
    parsed = urllib.parse.urlparse(value)
    hostname = (parsed.hostname or "").lower().rstrip(".")
    if parsed.scheme != "https" or not hostname:
        raise ValueError("private repository URL must use HTTPS")
    if hostname in PUBLIC_HOSTS or hostname.endswith(".pypi.org"):
        raise ValueError(f"public Python registry is prohibited: {hostname}")
    return value.rstrip("/") + "/"


def discover_wheels(directory: pathlib.Path) -> list[pathlib.Path]:
    source_archives = [
        path
        for path in directory.iterdir()
        if path.is_file()
        and path.name.endswith((".tar.gz", ".tar.bz2", ".tar.xz", ".tgz", ".zip"))
    ]
    if source_archives:
        raise ValueError(f"source archives are prohibited: {source_archives}")
    wheels = sorted(directory.glob("*.whl"))
    if not wheels:
        raise ValueError(f"no wheels found in {directory}")
    return wheels


def validate_build_manifest(wheel: pathlib.Path) -> pathlib.Path:
    manifest_path = wheel.with_name(f"{wheel.stem}.manifest.json")
    if not manifest_path.is_file():
        raise ValueError(f"missing platform audit manifest for {wheel.name}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid platform audit manifest: {manifest_path}") from exc

    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    expected = {
        "artifact": wheel.name,
        "sha256": digest,
        "external_duckdb_dependency_verified": True,
        "prohibited_source_files": [],
        "bundled_duckdb_files": [],
    }
    mismatches = {
        key: {"expected": value, "actual": manifest.get(key)}
        for key, value in expected.items()
        if manifest.get(key) != value
    }
    if mismatches:
        raise ValueError(
            f"platform audit manifest mismatch for {wheel.name}: {mismatches}"
        )
    return manifest_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=pathlib.Path)
    parser.add_argument("--repository-url", required=True)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()

    repository_url = validate_private_repository_url(args.repository_url)
    wheels = discover_wheels(args.directory)
    for wheel in wheels:
        validate_build_manifest(wheel)
        subprocess.run(
            [
                sys.executable,
                str(pathlib.Path(__file__).with_name("verify-wheel.py")),
                str(wheel),
                "--allow-unverified-duckdb-link",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    if args.check_only:
        print(
            f"private wheel upload validated: {repository_url} ({len(wheels)} wheels)"
        )
        return 0

    if not os.environ.get("TWINE_USERNAME") or not os.environ.get("TWINE_PASSWORD"):
        raise ValueError("TWINE_USERNAME and TWINE_PASSWORD are required")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "twine",
            "upload",
            "--non-interactive",
            "--repository-url",
            repository_url,
            *map(str, wheels),
        ],
        check=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
