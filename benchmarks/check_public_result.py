"""Validate the committed public benchmark release gate and payload digest."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "result",
        nargs="?",
        default="docs/results/public-common-pm-0.3.0.json",
    )
    return parser.parse_args()


def main() -> None:
    path = Path(parse_args().result)
    result = json.loads(path.read_text())
    recorded = result.pop("payload_sha256", None)
    encoded = json.dumps(result, indent=2, default=str) + "\n"
    computed = hashlib.sha256(encoded.encode()).hexdigest()
    if recorded != computed:
        raise SystemExit(
            f"payload digest mismatch: recorded={recorded!r}, computed={computed}"
        )
    summary = result["summary"]
    workloads = [
        workload for dataset in result["datasets"] for workload in dataset["workloads"]
    ]
    if summary["correct_workloads"] != summary["total_workloads"]:
        raise SystemExit("not every public workload passed its correctness gate")
    if len(workloads) != summary["total_workloads"]:
        raise SystemExit("summary workload count does not match result rows")
    if not all(workload["correct"] for workload in workloads):
        raise SystemExit("at least one public workload is marked incorrect")
    minimum = min(workload["speedup"] for workload in workloads)
    if minimum < 10.0 or summary["geometric_mean_speedup"] < 10.0:
        raise SystemExit(
            "10x gate failed: every workload and the geometric mean must pass"
        )
    if not summary["target_met"]:
        raise SystemExit("public result target_met flag is false")
    print(
        f"public benchmark verified: {len(workloads)} correct workloads, "
        f"minimum {minimum:.3f}x"
    )


if __name__ == "__main__":
    main()
