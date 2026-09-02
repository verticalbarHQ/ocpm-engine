#!/usr/bin/env python3
"""Regenerate the runtime dependency license bundle with cargo-about 0.8.4."""

from __future__ import annotations

import hashlib
import pathlib
import shutil
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
EXPECTED_VERSION = "cargo-about 0.8.4"


def input_paths() -> list[pathlib.Path]:
    return [
        ROOT / "Cargo.lock",
        ROOT / "Cargo.toml",
        ROOT / "about.toml",
        ROOT / "about.hbs",
        *sorted((ROOT / "crates").glob("*/Cargo.toml")),
    ]


def input_digest() -> str:
    digest = hashlib.sha256()
    for path in input_paths():
        digest.update(path.relative_to(ROOT).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def main() -> None:
    version = subprocess.run(
        ["cargo", "about", "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if version != EXPECTED_VERSION:
        raise SystemExit(f"{EXPECTED_VERSION} is required, found {version!r}")

    with tempfile.TemporaryDirectory(prefix="ocpm-licenses-") as temp_dir:
        generated = pathlib.Path(temp_dir, "THIRD_PARTY_LICENSES.html")
        with generated.open("wb") as output:
            subprocess.run(
                ["cargo", "about", "generate", "about.hbs"],
                cwd=ROOT,
                check=True,
                stdout=output,
            )
        rendered = generated.read_text(encoding="utf-8")
        generated.write_text(
            "\n".join(line.rstrip() for line in rendered.splitlines()).rstrip() + "\n",
            encoding="utf-8",
        )
        # The generator is also run from a Docker build with the repository on
        # a bind mount; `/tmp` and the workspace may be different filesystems.
        shutil.copyfile(generated, ROOT / "THIRD_PARTY_LICENSES.html")
    (ROOT / "THIRD_PARTY_LICENSES_INPUT.sha256").write_text(
        input_digest() + "\n",
        encoding="ascii",
    )
    print("regenerated THIRD_PARTY_LICENSES.html")


if __name__ == "__main__":
    main()
