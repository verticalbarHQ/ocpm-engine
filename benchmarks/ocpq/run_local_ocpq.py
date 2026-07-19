#!/usr/bin/env python3
"""Run the pinned OCPQ CLI against all seven public evaluation trees."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

TIMINGS = re.compile(r"Evaluation time: \[(?P<values>[^]]+)]")
EXPECTED_EVAL_COMMIT = "846dd4eb9f8600ae42355968453a9412ea4759c2"
EXPECTED_DATASET_SHA256 = (
    "02ac333a2c194b5a411cb8527dd64b4845e5110752d2ffddb531e48ce97556d7"
)


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="ocpq:0.6.7-public-repro")
    parser.add_argument("--sqlite", required=True, type=Path)
    parser.add_argument("--eval", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    args = arguments()
    eval_commit = subprocess.run(
        ["git", "-C", str(args.eval.resolve()), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if eval_commit != EXPECTED_EVAL_COMMIT:
        raise RuntimeError(f"unexpected OCPQ evaluation commit: {eval_commit}")
    dataset_sha256 = sha256(args.sqlite)
    if dataset_sha256 != EXPECTED_DATASET_SHA256:
        raise RuntimeError(f"unexpected OCPQ SQLite SHA-256: {dataset_sha256}")
    result = {}
    for index in range(1, 8):
        completed = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{args.sqlite.resolve()}:/benchmark/bpic2017.sqlite:ro",
                "-v",
                f"{args.eval.resolve()}:/queries:ro",
                args.image,
                "--ocel",
                "/benchmark/bpic2017.sqlite",
                "--bbox-tree",
                f"/queries/Q{index}/ocpq-tree.json",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        match = TIMINGS.search(completed.stdout)
        if match is None:
            raise RuntimeError(f"Q{index}: OCPQ timing output not found")
        samples_ms = [
            float(value.strip()) * 1000 for value in match.group("values").split(",")
        ]
        if len(samples_ms) != 10:
            raise RuntimeError(f"Q{index}: expected ten OCPQ samples")
        result[f"Q{index}"] = {
            "runs_ms": samples_ms,
            "mean_ms": sum(samples_ms) / len(samples_ms),
        }
    artifact = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "ocpq_eval_commit": eval_commit,
            "ocpq_version": "0.6.7",
            "ocpq_commit": "80457e561edd7bb9e142d959dd7e0f96e6b03f2f",
            "docker_image": args.image,
            "dataset_sqlite_sha256": dataset_sha256,
        },
        "method": {
            "samples_per_query": 10,
            "timing_boundary": (
                "OCPQ evaluate_box_tree performance loop after OCEL import and linking"
            ),
            "fresh_container_per_query": True,
        },
        "environment": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "logical_cpus_visible": os.cpu_count(),
        },
        "queries": result,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2) + "\n")


if __name__ == "__main__":
    main()
