"""Join the fixed SAP PostgreSQL/PM4Py baseline with the DuckDB provider arm."""

# Markdown tables and retained benchmark-method prose intentionally exceed the
# source line-length limit in a few generated report rows.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.ecosystem.common import WORKLOADS, answer_sha256

BASELINE_ARMS = (
    "vanilla_pg_pm4py",
    "pg_ocpm_pm4py",
    "pg_ocpm_ocpm_engine",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    return parser.parse_args()


def by_dataset(value: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["dataset"]: row for row in value["datasets"]}


def build(args: argparse.Namespace) -> dict[str, Any]:
    baseline = json.loads(Path(args.baseline).read_text())
    duckdb = json.loads(Path(args.duckdb).read_text())
    if duckdb.get("arm") != "duckdb_parquet_ocpm_engine":
        raise SystemExit("DuckDB artifact has the wrong arm")
    baseline_datasets = by_dataset(baseline)
    duckdb_datasets = by_dataset(duckdb)
    if set(baseline_datasets) != set(duckdb_datasets):
        raise SystemExit("SAP artifacts contain different datasets")

    merged = []
    for name in sorted(baseline_datasets):
        base = baseline_datasets[name]
        lake = duckdb_datasets[name]
        latency_by_workload = {row["workload"]: row for row in base["latency"]}
        latency = []
        for workload in WORKLOADS:
            base_hashes = {
                arm: base["memory"][workload][arm]["answer_sha256"]
                for arm in BASELINE_ARMS
            }
            cached = lake["serial"][workload]
            cache_off = lake["serial_without_result_cache"][workload]
            cached_hash = answer_sha256(cached["answer"])
            cache_off_hash = answer_sha256(cache_off["answer"])
            hashes = {
                **base_hashes,
                "duckdb_parquet_ocpm_engine": cached_hash,
                "duckdb_parquet_ocpm_engine_cache_off": cache_off_hash,
            }
            if len(set(hashes.values())) != 1:
                raise SystemExit(f"{name}/{workload}: exact-answer gate failed")
            if (
                cached_hash != cached["answer_sha256"]
                or cache_off_hash != cache_off["answer_sha256"]
            ):
                raise SystemExit(f"{name}/{workload}: DuckDB stored hash is invalid")
            base_latency = latency_by_workload[workload]
            latency.append(
                {
                    "workload": workload,
                    "answer_sha256": cached_hash,
                    "arms": {
                        arm: {
                            "p50_ms": base_latency[arm]["p50_ms"],
                            "p95_ms": base_latency[arm]["p95_ms"],
                        }
                        for arm in BASELINE_ARMS
                    }
                    | {
                        "duckdb_parquet_ocpm_engine": {
                            "p50_ms": cached["p50_ms"],
                            "p95_ms": cached["p95_ms"],
                        },
                        "duckdb_parquet_ocpm_engine_cache_off": {
                            "p50_ms": cache_off["p50_ms"],
                            "p95_ms": cache_off["p95_ms"],
                        },
                    },
                }
            )

        concurrency = []
        for level in sorted(
            set(base["concurrency"]["levels"]) & set(lake["concurrency"]),
            key=int,
        ):
            base_rows = {arm: base["concurrency"][arm][level] for arm in BASELINE_ARMS}
            lake_row = lake["concurrency"][level]
            if (
                not all(row["correct"] for row in base_rows.values())
                or not lake_row["correct"]
            ):
                raise SystemExit(f"{name}/x{level}: concurrency correctness failed")
            concurrency.append(
                {
                    "workers": int(level),
                    "arms": {
                        arm: {
                            "throughput_qps": row["throughput_qps"],
                            "p95_ms": row["p95_ms"],
                        }
                        for arm, row in base_rows.items()
                    }
                    | {
                        "duckdb_parquet_ocpm_engine": {
                            "throughput_qps": lake_row["throughput_qps"],
                            "p95_ms": lake_row["p95_ms"],
                        }
                    },
                }
            )

        incremental_memory = {
            arm: max(
                base["memory"][workload][arm]["incremental_peak_bytes"]
                for workload in WORKLOADS
            )
            for arm in BASELINE_ARMS
        }
        incremental_memory["duckdb_parquet_ocpm_engine"] = max(
            row["incremental_peak_bytes"] for row in lake["memory"].values()
        )
        peak_memory = {
            arm: max(
                base["memory"][workload][arm]["peak_rss_bytes"]
                for workload in WORKLOADS
            )
            for arm in BASELINE_ARMS
        }
        peak_memory["duckdb_parquet_ocpm_engine"] = max(
            row["peak_rss_bytes"] for row in lake["memory"].values()
        )
        merged.append(
            {
                "dataset": name,
                "latency": latency,
                "concurrency": concurrency,
                "maximum_incremental_peak_bytes": incremental_memory,
                "maximum_peak_rss_bytes": peak_memory,
                "duckdb_snapshot_bytes": lake["snapshot_bytes"],
                "duckdb_connection_open_ms": lake["connection_open_ms"],
                "duckdb_snapshot_conversion_ms": lake["snapshot_conversion_ms"],
            }
        )

    storage = {
        "vanilla_pg_pm4py_total_bytes": baseline["storage"]["vanilla_pg_pm4py"][
            "total_bytes"
        ],
        "shared_pg_ocpm_total_bytes": baseline["storage"]["shared_pg_ocpm"][
            "total_bytes"
        ],
        "duckdb_snapshot_total_bytes": sum(
            row["duckdb_snapshot_bytes"] for row in merged
        ),
        "source_sqlite_total_bytes": sum(
            row["source_sqlite_bytes"] for row in duckdb["datasets"]
        ),
    }
    baseline_provenance = baseline.get("provenance", {})
    baseline_clean = all(
        baseline_provenance.get(field) is True
        for field in (
            "controller_source_tree_clean",
            "ocpm_engine_source_tree_clean",
            "pg_ocpm_source_tree_clean",
        )
    )
    duckdb_clean = duckdb.get("implementation", {}).get("source_tree_clean") is True
    publication_ready = baseline_clean and duckdb_clean
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "suite": "sap-common-pm-four-way",
        "publication_ready": publication_ready,
        "datasets": merged,
        "storage": storage,
        "baseline": baseline,
        "duckdb": duckdb,
        "limitations": [
            "The three PostgreSQL/PM4Py arms reuse the fixed, exact 1.0.0 artifact because this change does not modify pg_ocpm or the PostgreSQL provider path.",
            "DuckDB cached and cache-disabled latency are separate columns; concurrency uses the normal bounded cache configuration.",
            "Snapshot conversion and connection-local relation construction are outside request latency and reported separately.",
            "The composite is publication-ready only when the accepted baseline and the current DuckDB arm both record clean committed source trees.",
        ],
    }


def render(value: dict[str, Any]) -> str:
    labels = {
        "vanilla_pg_pm4py": "Vanilla PG + PM4Py",
        "pg_ocpm_pm4py": "pg_ocpm + PM4Py",
        "pg_ocpm_ocpm_engine": "pg_ocpm + ocpm-engine",
        "duckdb_parquet_ocpm_engine": "DuckDB + ocpm-engine cached",
        "duckdb_parquet_ocpm_engine_cache_off": "DuckDB + ocpm-engine cache-off",
    }
    lines = [
        "# SAP O2C and P2P four-way benchmark",
        "",
        (
            "Publication gate: **passed**."
            if value["publication_ready"]
            else "Publication gate: **descriptive only**."
        ),
        "",
        "All reported latency cells passed the exact-answer hash gate.",
    ]
    for dataset in value["datasets"]:
        lines.extend(
            [
                "",
                f"## {dataset['dataset']}",
                "",
                "| Workload | Vanilla PG + PM4Py p50 | pg_ocpm + PM4Py p50 | pg_ocpm + engine p50 | DuckDB cached p50 | DuckDB cache-off p50 |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for row in dataset["latency"]:
            arms = row["arms"]
            lines.append(
                f"| {row['workload']} | {arms['vanilla_pg_pm4py']['p50_ms']:.3f} ms | "
                f"{arms['pg_ocpm_pm4py']['p50_ms']:.3f} ms | "
                f"{arms['pg_ocpm_ocpm_engine']['p50_ms']:.3f} ms | "
                f"{arms['duckdb_parquet_ocpm_engine']['p50_ms']:.3f} ms | "
                f"{arms['duckdb_parquet_ocpm_engine_cache_off']['p50_ms']:.3f} ms |"
            )
        lines.extend(
            [
                "",
                "| Workers | Vanilla QPS | pg_ocpm + PM4Py QPS | pg_ocpm + engine QPS | DuckDB QPS |",
                "|---:|---:|---:|---:|---:|",
            ]
        )
        for row in dataset["concurrency"]:
            arms = row["arms"]
            lines.append(
                f"| {row['workers']} | {arms['vanilla_pg_pm4py']['throughput_qps']:.3f} | "
                f"{arms['pg_ocpm_pm4py']['throughput_qps']:.3f} | "
                f"{arms['pg_ocpm_ocpm_engine']['throughput_qps']:.3f} | "
                f"{arms['duckdb_parquet_ocpm_engine']['throughput_qps']:.3f} |"
            )
        lines.extend(
            [
                "",
                "| Arm | Maximum incremental peak | Maximum process RSS |",
                "|---|---:|---:|",
            ]
        )
        for arm, count in dataset["maximum_incremental_peak_bytes"].items():
            peak = dataset["maximum_peak_rss_bytes"][arm]
            lines.append(
                f"| {labels[arm]} | {count / 1048576:.2f} MiB | "
                f"{peak / 1048576:.2f} MiB |"
            )
        lines.extend(
            [
                "",
                f"DuckDB snapshot: {dataset['duckdb_snapshot_bytes'] / 1048576:.2f} MiB. "
                f"Snapshot conversion: {dataset['duckdb_snapshot_conversion_ms']:.3f} ms. "
                f"Connection open/materialization: {dataset['duckdb_connection_open_ms']:.3f} ms.",
            ]
        )
    storage = value["storage"]
    lines.extend(
        [
            "",
            "## Storage footprint",
            "",
            "| Representation | Bytes | MiB |",
            "|---|---:|---:|",
            f"| Vanilla PostgreSQL | {storage['vanilla_pg_pm4py_total_bytes']} | {storage['vanilla_pg_pm4py_total_bytes'] / 1048576:.2f} |",
            f"| Shared pg_ocpm | {storage['shared_pg_ocpm_total_bytes']} | {storage['shared_pg_ocpm_total_bytes'] / 1048576:.2f} |",
            f"| DuckDB canonical Parquet snapshots | {storage['duckdb_snapshot_total_bytes']} | {storage['duckdb_snapshot_total_bytes'] / 1048576:.2f} |",
            f"| Source OCEL SQLite files | {storage['source_sqlite_total_bytes']} | {storage['source_sqlite_total_bytes'] / 1048576:.2f} |",
        ]
    )
    lines.extend(["", "## Interpretation boundaries", ""])
    lines.extend(f"- {item}" for item in value["limitations"])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    arguments = parse_args()
    result = build(arguments)
    Path(arguments.output).write_text(json.dumps(result, indent=2) + "\n")
    Path(arguments.report).write_text(render(result))
    print(f"wrote {arguments.output}", flush=True)
    print(f"wrote {arguments.report}", flush=True)
