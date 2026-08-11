#!/usr/bin/env python3
"""Fail closed when the Apache-2.0 release contract becomes inconsistent."""

from __future__ import annotations

import hashlib
import pathlib
import re
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]
OWNER = "Vertical Bar, Inc."


def require(path: pathlib.Path, pattern: str, message: str) -> None:
    if not path.is_file() or re.search(pattern, path.read_text(), re.MULTILINE) is None:
        raise SystemExit(f"licensing check failed: {message}: {path.relative_to(ROOT)}")


def main() -> None:
    require(ROOT / "LICENSE", r"^\s*Apache License\s*$", "missing Apache license")
    require(
        ROOT / "LICENSE", r"^\s*Version 2\.0, January 2004\s*$", "wrong license version"
    )
    require(
        ROOT / "NOTICE", rf"Copyright 2026 {re.escape(OWNER)}", "wrong NOTICE owner"
    )
    require(
        ROOT / "COPYRIGHT.md", r"Apache License, Version 2\.0", "stale copyright file"
    )
    require(
        ROOT / "CLA.md", r"commercial, and proprietary", "CLA lacks relicensing grant"
    )
    require(
        ROOT / "CLA.md",
        r"contributions to `ocpm-engine`",
        "CLA names the wrong project",
    )
    require(
        ROOT / "COMMERCIAL.md",
        r"No commercial license is required",
        "missing Apache commercial-use clarification",
    )
    require(ROOT / "TRADEMARKS.md", r"Fair use", "missing trademark policy")
    require(
        ROOT / "CODE_OF_CONDUCT.md",
        r"Reporting and enforcement",
        "missing conduct policy",
    )
    require(ROOT / "GOVERNANCE.md", r"Vertical Bar, Inc\.", "missing governance")

    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    project = pyproject["project"]
    if project.get("license") != "Apache-2.0":
        raise SystemExit("licensing check failed: Python license is not Apache-2.0")
    required_files = {
        "LICENSE",
        "NOTICE",
        "COPYRIGHT.md",
        "THIRD_PARTY_NOTICES.md",
        "THIRD_PARTY_LICENSES.html",
    }
    if set(project.get("license-files", [])) != required_files:
        raise SystemExit(
            "licensing check failed: Python license-files set is incomplete"
        )

    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]
    if workspace.get("license") != "Apache-2.0":
        raise SystemExit(
            "licensing check failed: Cargo workspace license is not Apache-2.0"
        )
    for path in sorted((ROOT / "crates").glob("*/Cargo.toml")):
        package = tomllib.loads(path.read_text())["package"]
        if package.get("license") != {"workspace": True}:
            raise SystemExit(
                f"licensing check failed: {path.relative_to(ROOT)} does not inherit "
                "the workspace license"
            )

    require(
        ROOT / "THIRD_PARTY_LICENSES.html",
        r"<h1>Third-Party Licenses</h1>",
        "generated third-party license bundle is missing",
    )
    digest = hashlib.sha256()
    for name in ("Cargo.lock", "about.toml", "about.hbs"):
        digest.update((ROOT / name).read_bytes())
    recorded_digest = (ROOT / "THIRD_PARTY_LICENSES_INPUT.sha256").read_text().strip()
    if recorded_digest != digest.hexdigest():
        raise SystemExit(
            "licensing check failed: THIRD_PARTY_LICENSES.html is stale; run "
            "scripts/generate-third-party-licenses.py with cargo-about 0.8.4"
        )
    release_paths = [
        ROOT / "README.md",
        ROOT / "COPYRIGHT.md",
        ROOT / "pyproject.toml",
        ROOT / "Cargo.toml",
    ]
    for path in release_paths:
        text = path.read_text().lower()
        for phrase in (
            "licenseref-proprietary",
            "all rights reserved",
            "repository currently grants no open-source license",
            "separately licensed commercial editions",
        ):
            if phrase in text:
                raise SystemExit(
                    f"licensing check failed: stale proprietary term {phrase!r} "
                    f"in {path.relative_to(ROOT)}"
                )
    print(
        "licensing check passed: Apache-2.0 metadata, notices, dependency "
        "bundle, and CLA"
    )


if __name__ == "__main__":
    main()
