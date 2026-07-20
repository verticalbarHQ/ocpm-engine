#!/usr/bin/env python3
"""Require the two public SAP artifacts to come from one exact benchmark run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from benchmark_provenance import (
        PUBLIC_BENCHMARK_SCHEMA_VERSION,
        validate_recorded_public_provenance,
    )
except ModuleNotFoundError:  # imported as a package module in tests
    from benchmarks.benchmark_provenance import (
        PUBLIC_BENCHMARK_SCHEMA_VERSION,
        validate_recorded_public_provenance,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--common",
        type=Path,
        default=Path("docs/results/public-common-pm-0.6.0.json"),
    )
    parser.add_argument(
        "--sap",
        type=Path,
        default=Path("docs/results/sap-pm4py-three-way-0.6.0.json"),
    )
    parser.add_argument("--preview", action="store_true")
    return parser.parse_args()


def load(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"could not read {path}: {error}") from error
    if not isinstance(value, dict):
        raise SystemExit(f"{path} must contain one JSON object")
    return value


def main() -> None:
    args = parse_args()
    common = load(args.common)
    sap = load(args.sap)
    if (
        common.get("schema_version") != PUBLIC_BENCHMARK_SCHEMA_VERSION
        or sap.get("schema_version") != PUBLIC_BENCHMARK_SCHEMA_VERSION
    ):
        raise SystemExit(
            "paired public artifacts must use schema version "
            f"{PUBLIC_BENCHMARK_SCHEMA_VERSION}"
        )
    try:
        common_provenance = validate_recorded_public_provenance(
            common.get("provenance"), allow_dirty=args.preview
        )
        sap_provenance = validate_recorded_public_provenance(
            sap.get("provenance"), allow_dirty=args.preview
        )
    except ValueError as error:
        raise SystemExit(str(error)) from error
    if common_provenance != sap_provenance:
        raise SystemExit("public SAP artifacts do not share exact provenance")
    print("public SAP artifacts share exact source, host, and image provenance")


if __name__ == "__main__":
    main()
