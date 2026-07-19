"""Validate the committed SAP PM4Py three-way benchmark artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ENGINES = (
    "vanilla_pg_pm4py",
    "pg_ocpm_pm4py",
    "pg_ocpm_ocpm_engine",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "result",
        nargs="?",
        default="docs/results/sap-pm4py-three-way-2026-07-18.json",
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

    if result["source"]["datasets"] != ["sap_o2c", "sap_p2p"]:
        raise SystemExit("expected SAP O2C and P2P datasets")
    if result["environment"]["client"]["ocpm_engine_version"] != "0.4.0":
        raise SystemExit("expected ocpm-engine 0.4.0")
    if result["environment"]["database"]["pg_ocpm"]["pg_ocpm_version"] != "0.5.0":
        raise SystemExit("expected pg_ocpm 0.5.0")
    datasets = result["datasets"]
    latency = [row for dataset in datasets for row in dataset["latency"]]
    summary = result["summary"]
    if summary["correct_workloads"] != summary["total_workloads"]:
        raise SystemExit("not every SAP workload passed its correctness gate")
    if len(latency) != summary["total_workloads"] or len(latency) != 8:
        raise SystemExit("expected eight SAP latency comparisons")
    if not all(row["correct"] for row in latency):
        raise SystemExit("at least one SAP latency comparison is incorrect")
    if not all(
        row["pg_ocpm_ocpm_engine"]["p50_ms"] < row["vanilla_pg_pm4py"]["p50_ms"]
        for row in latency
    ):
        raise SystemExit("ocpm-engine did not beat vanilla PostgreSQL on every row")

    for dataset in datasets:
        for engine in ENGINES:
            if not all(
                level["correct"] for level in dataset["concurrency"][engine].values()
            ):
                raise SystemExit("concurrency correctness gate failed")
        for workload in dataset["memory"].values():
            hashes = {workload[engine]["answer_sha256"] for engine in ENGINES}
            if len(hashes) != 1:
                raise SystemExit("isolated-memory answer hashes do not agree")

    secondary = [
        index
        for index in result["storage"]["vanilla_pg_pm4py"]["indexes"]
        if index["definition"].startswith("CREATE INDEX")
        and "UNIQUE" not in index["definition"]
    ]
    if [index["name"] for index in secondary] != ["ocel_e2o_object"]:
        raise SystemExit("vanilla light-index policy changed")
    print(
        "SAP PM4Py benchmark verified: "
        f"{len(latency)} correct workloads, payload {computed[:12]}"
    )


if __name__ == "__main__":
    main()
