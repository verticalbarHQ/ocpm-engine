#!/usr/bin/env python3
"""Run the pinned OCPQ CLI against all seven public evaluation trees."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

TIMINGS = re.compile(r"Evaluation time: \[(?P<values>[^]]+)]")


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="ocpq:0.6.7-benchmark")
    parser.add_argument("--sqlite", required=True, type=Path)
    parser.add_argument("--eval", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> None:
    args = arguments()
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
        result[f"Q{index}"] = {
            "runs_ms": samples_ms,
            "mean_ms": sum(samples_ms) / len(samples_ms),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
