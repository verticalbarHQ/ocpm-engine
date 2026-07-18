"""Validate a fresh Goodr benchmark against a committed baseline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

ENGINES = ("vanilla_postgres", "verticalbar_optimized", "pg_ocpm")
DATASET_COUNTS = ("events", "edges", "cases")


def load_result(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _case_map(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {case["scenario"]: case for case in result["cases"]}


def validate_regression(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
    *,
    max_regression: float = 0.25,
    absolute_ms: float = 1.0,
    max_storage_growth: float = 0.05,
) -> list[str]:
    """Return human-readable failures; an empty list means the run passed."""
    failures: list[str] = []

    for key in DATASET_COUNTS:
        expected = baseline.get("dataset", {}).get(key)
        actual = candidate.get("dataset", {}).get(key)
        if expected != actual:
            failures.append(f"dataset.{key}: expected {expected}, got {actual}")

    baseline_cases = _case_map(baseline)
    candidate_cases = _case_map(candidate)
    if baseline_cases.keys() != candidate_cases.keys():
        missing = sorted(baseline_cases.keys() - candidate_cases.keys())
        extra = sorted(candidate_cases.keys() - baseline_cases.keys())
        failures.append(f"scenario set changed: missing={missing}, extra={extra}")

    for scenario in sorted(baseline_cases.keys() & candidate_cases.keys()):
        old_case = baseline_cases[scenario]
        new_case = candidate_cases[scenario]
        if not new_case.get("agreement"):
            failures.append(f"{scenario}: correctness gate failed")
            continue
        for engine in ENGINES:
            old_ms = old_case.get(engine, {}).get("p50_ms")
            new_ms = new_case.get(engine, {}).get("p50_ms")
            if not isinstance(old_ms, (int, float)) or not isinstance(
                new_ms, (int, float)
            ):
                failures.append(f"{scenario}/{engine}: missing p50_ms")
                continue
            limit = max(old_ms * (1.0 + max_regression), old_ms + absolute_ms)
            if new_ms > limit:
                failures.append(
                    f"{scenario}/{engine}: p50 {new_ms:.3f} ms exceeds "
                    f"{limit:.3f} ms (baseline {old_ms:.3f} ms)"
                )

    old_storage = baseline.get("storage_and_index_usage", {})
    new_storage = candidate.get("storage_and_index_usage", {})
    for engine in ENGINES:
        old_engine_storage = old_storage.get(engine, {})
        new_engine_storage = new_storage.get(engine, {})
        old_bytes = old_engine_storage.get("totals", old_engine_storage).get(
            "total_bytes"
        )
        new_bytes = new_engine_storage.get("totals", new_engine_storage).get(
            "total_bytes"
        )
        if not isinstance(old_bytes, int) or not isinstance(new_bytes, int):
            failures.append(f"{engine}: missing total storage bytes")
            continue
        limit = int(old_bytes * (1.0 + max_storage_growth))
        if new_bytes > limit:
            failures.append(
                f"{engine}: storage {new_bytes} bytes exceeds {limit} bytes "
                f"(baseline {old_bytes} bytes)"
            )

    return failures


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--max-regression", type=float, default=0.25)
    parser.add_argument("--absolute-ms", type=float, default=1.0)
    parser.add_argument("--max-storage-growth", type=float, default=0.05)
    args = parser.parse_args()

    failures = validate_regression(
        load_result(args.baseline),
        load_result(args.candidate),
        max_regression=args.max_regression,
        absolute_ms=args.absolute_ms,
        max_storage_growth=args.max_storage_growth,
    )
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        raise SystemExit(1)
    print(f"PASS: {args.candidate} matches correctness, performance, and storage gates")


if __name__ == "__main__":
    main()
