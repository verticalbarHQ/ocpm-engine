#!/usr/bin/env python3
"""Build the matched-gate worker against one ocpm-engine source tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any


def canonical_payload(value: dict[str, Any]) -> bytes:
    unsigned = dict(value)
    unsigned.pop("payload_sha256", None)
    return json.dumps(unsigned, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(repository: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repository), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def source_provenance(repository: Path) -> dict[str, Any]:
    root = Path(git(repository, "rev-parse", "--show-toplevel"))
    status = git(root, "status", "--porcelain", "--untracked-files=all")
    tracked = subprocess.run(
        ["git", "-C", str(root), "diff", "--binary", "HEAD"],
        check=True,
        capture_output=True,
    ).stdout
    untracked = []
    for relative in git(
        root, "ls-files", "--others", "--exclude-standard"
    ).splitlines():
        path = root / relative
        untracked.append((relative, file_sha256(path) if path.is_file() else None))
    tree_material = tracked + json.dumps(
        untracked, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return {
        "revision": git(root, "rev-parse", "HEAD"),
        "tree_clean": not bool(status),
        "tree_sha256": hashlib.sha256(tree_material).hexdigest(),
    }


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
    provenance = {
        "schema_version": 1,
        "artifact_type": "ocpm_engine_candidate_worker",
        "source": source_provenance(source),
        "inputs": {
            "builder_sha256": file_sha256(Path(__file__)),
            "worker_source_sha256": file_sha256(
                Path(__file__).with_name("candidate_worker") / "main.rs"
            ),
        },
        "worker_sha256": file_sha256(args.output),
    }
    provenance["payload_sha256"] = hashlib.sha256(
        canonical_payload(provenance)
    ).hexdigest()
    provenance_path = args.output.with_name(args.output.name + ".provenance.json")
    provenance_path.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
