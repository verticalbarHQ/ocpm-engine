#!/usr/bin/env python3
"""Build the matched-gate worker against one ocpm-engine source tree."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine-source", required=True, type=Path)
    parser.add_argument("--build-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    source = args.engine_source.resolve()
    core = source / "crates/ocpm-core"
    engine = source / "crates/ocpm-engine"
    if not core.is_dir() or not engine.is_dir():
        raise SystemExit("engine source does not contain the required crates")
    build_root = args.build_root.resolve()
    source_dir = build_root / "src"
    source_dir.mkdir(parents=True, exist_ok=True)
    manifest = f"""[package]
name = "ocpm-candidate-worker"
version = "0.0.0"
edition = "2024"
rust-version = "1.85.1"
publish = false

[dependencies]
ocpm-core = {{ path = {json.dumps(str(core))} }}
ocpm-engine = {{ path = {json.dumps(str(engine))} }}
serde_json = "1.0.149"

[profile.release]
codegen-units = 1
lto = "fat"
panic = "abort"
strip = "symbols"

[workspace]
"""
    (build_root / "Cargo.toml").write_text(manifest)
    shutil.copyfile(
        Path(__file__).with_name("candidate_worker") / "main.rs",
        source_dir / "main.rs",
    )
    subprocess.run(
        [
            "cargo",
            "+1.85.1",
            "generate-lockfile",
            "--manifest-path",
            str(build_root / "Cargo.toml"),
        ],
        check=True,
    )
    subprocess.run(
        [
            "cargo",
            "+1.85.1",
            "build",
            "--release",
            "--locked",
            "--manifest-path",
            str(build_root / "Cargo.toml"),
        ],
        check=True,
    )
    built = build_root / "target/release/ocpm-candidate-worker"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(built, args.output)
    args.output.chmod(0o755)
    print(args.output)


if __name__ == "__main__":
    main()
