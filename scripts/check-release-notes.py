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
    r'\[\[package\]\]\s+name = "verticalbar-ocpm-engine"\s+version = "([^"]+)"',
    lock,
    re.MULTILINE,
)
if lock_match is None or lock_match.group(1) != version:
    raise SystemExit("uv.lock package version does not match pyproject.toml")

for heading in re.findall(r"^## ([0-9]+\.[0-9]+\.[0-9]+) -", changelog, re.MULTILINE):
    body = re.search(
        rf"^## {re.escape(heading)} - [^\n]+\n(?P<body>.*?)(?=^## |\Z)",
        changelog,
        re.MULTILINE | re.DOTALL,
    )
    if body is None or not re.search(r"^- ", body.group("body"), re.MULTILINE):
        raise SystemExit(f"release notes for {heading} contain no change entries")
print(f"release notes present for ocpm-engine {version}")
