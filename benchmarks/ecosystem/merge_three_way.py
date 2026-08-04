"""Merge competitor, pg_ocpm, and DuckDB Parquet ecosystem benchmark arms."""

# Markdown tables and retained benchmark-method prose intentionally exceed the
# source line-length limit in a few generated report rows.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.ecosystem.common import WORKLOADS, answer_sha256, canonical


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--postgres", required=True)
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--competitor", required=True)
    parser.add_argument("--competitor-name", choices=("rust4pm", "ocpa"), required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    return parser.parse_args()


def load(path: str, expected: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text())
    if value["arm"] != expected:
        raise SystemExit(f"{path}: expected {expected}, found {value['arm']}")
    return value


def dataset(value: dict[str, Any]) -> dict[str, Any]:
    if len(value["datasets"]) != 1:
        raise SystemExit("three-way report requires exactly one dataset per arm")
    return value["datasets"][0]


def build(args: argparse.Namespace) -> dict[str, Any]:
    manifest = json.loads(Path(args.manifest).read_text())
    arms = {
        args.competitor_name: load(args.competitor, args.competitor_name),
        "pg_ocpm_ocpm_engine": load(args.postgres, "pg_ocpm_ocpm_engine"),
        "duckdb_parquet_ocpm_engine": load(args.duckdb, "duckdb_parquet_ocpm_engine"),
    }
    datasets = {name: dataset(value) for name, value in arms.items()}
    names = {value["dataset"] for value in datasets.values()}
    if len(names) != 1:
        raise SystemExit("three-way arms use different datasets")

    exactness = []
    latency = []
    for workload in WORKLOADS:
        answers = {
            name: value["serial"][workload]["answer"]
            for name, value in datasets.items()
        }
        hashes = {name: answer_sha256(value) for name, value in answers.items()}
        duckdb_uncached = datasets["duckdb_parquet_ocpm_engine"][
            "serial_without_result_cache"
        ][workload]
        uncached_hash = answer_sha256(duckdb_uncached["answer"])
        if uncached_hash != duckdb_uncached["answer_sha256"]:
            raise SystemExit(
                f"duckdb cache-off/{workload}: stored answer hash is invalid"
            )
        hashes["duckdb_parquet_ocpm_engine_cache_off"] = uncached_hash
        for name, value in datasets.items():
            if hashes[name] != value["serial"][workload]["answer_sha256"]:
                raise SystemExit(f"{name}/{workload}: stored answer hash is invalid")
        exact = (
            len({canonical(value) for value in answers.values()}) == 1
            and uncached_hash == hashes["duckdb_parquet_ocpm_engine"]
        )
        exactness.append(
            {
                "workload": workload,
                "exact": exact,
                "answer_sha256": next(iter(hashes.values())) if exact else None,
                "arm_sha256": hashes,
            }
        )
        latency.append(
            {
                "workload": workload,
                "arms": {
                    name: {
                        "p50_ms": value["serial"][workload]["p50_ms"],
                        "p95_ms": value["serial"][workload]["p95_ms"],
                    }
                    for name, value in datasets.items()
                },
                "duckdb_without_result_cache": {
                    "p50_ms": datasets["duckdb_parquet_ocpm_engine"][
                        "serial_without_result_cache"
                    ][workload]["p50_ms"],
                    "p95_ms": datasets["duckdb_parquet_ocpm_engine"][
                        "serial_without_result_cache"
                    ][workload]["p95_ms"],
                },
            }
        )
    if not all(value["exact"] for value in exactness):
        failures = ", ".join(
            value["workload"] for value in exactness if not value["exact"]
        )
        raise SystemExit(f"three-way exact-answer gate failed: {failures}")

    levels = set.intersection(
        *(set(value["concurrency"]) for value in datasets.values())
    )
    concurrency = []
    for level in sorted(levels, key=int):
        rows = {name: value["concurrency"][level] for name, value in datasets.items()}
        if not all(value["correct"] for value in rows.values()):
            raise SystemExit(f"concurrency exactness failed at x{level}")
        concurrency.append(
            {
                "workers": int(level),
                "arms": {
                    name: {
                        "throughput_qps": value["throughput_qps"],
                        "p95_ms": value["p95_ms"],
                    }
                    for name, value in rows.items()
                },
            }
        )

    competitor = args.competitor_name
    speedups = {}
    for name in ("pg_ocpm_ocpm_engine", "duckdb_parquet_ocpm_engine"):
        values = [
            row["arms"][competitor]["p50_ms"]
            / max(row["arms"][name]["p50_ms"], 0.000001)
            for row in latency
        ]
        speedups[name] = round(statistics.geometric_mean(values), 3)
    duckdb_uncached_speedup = round(
        statistics.geometric_mean(
            row["arms"][competitor]["p50_ms"]
            / max(row["duckdb_without_result_cache"]["p50_ms"], 0.000001)
            for row in latency
        ),
        3,
    )
    memory = {
        name: {
            "maximum_incremental_peak_bytes": max(
                row["incremental_peak_bytes"] for row in value["memory"].values()
            ),
            "maximum_peak_rss_bytes": max(
                row["peak_rss_bytes"] for row in value["memory"].values()
            ),
        }
        for name, value in datasets.items()
    }
    storage = {
        name: {
            key: value[key]
            for key in ("source_sqlite_bytes", "snapshot_bytes")
            if key in value
        }
        for name, value in datasets.items()
    }
    competitor_dataset = datasets[args.competitor_name]
    competitor_clean = bool(
        arms[args.competitor_name]["implementation"].get("controller_source_tree_clean")
    )
    native_import = competitor_dataset.get("native_import_probe", {})
    clean = (
        competitor_clean
        and bool(native_import.get("success"))
        and bool(arms["pg_ocpm_ocpm_engine"]["implementation"].get("source_tree_clean"))
        and bool(
            arms["pg_ocpm_ocpm_engine"]["implementation"].get(
                "pg_ocpm_source_tree_clean"
            )
        )
        and bool(
            arms["duckdb_parquet_ocpm_engine"]["implementation"].get(
                "source_tree_clean"
            )
        )
    )
    limitations = [
        "Import, snapshot conversion, process startup, and connection startup are excluded from steady-state latency and reported by each arm.",
        "Concurrency uses each architecture's normal scalable service model; memory is reported per arm and is not normalized into an artificial single runtime.",
        "The results apply to the declared fixed workloads and do not imply the same ratio for arbitrary dynamic queries.",
        "DuckDB warm-cache and cache-disabled measurements are both published; only the warm-cache state is used in the primary three-arm latency table and concurrency run.",
    ]
    if not native_import.get("success"):
        competitor_label = "OCPA" if args.competitor_name == "ocpa" else "Rust4PM"
        limitations.append(
            f"{competitor_label} is descriptive rather than publication-ready because its documented importer failed on its unchanged upstream example; the disclosed setup adapter is retained in the raw evidence."
        )
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "suite": "ecosystem-common-pm-three-way",
        "dataset": next(iter(names)),
        "competitor": competitor,
        "publication_ready": clean,
        "exactness": exactness,
        "latency": latency,
        "concurrency": concurrency,
        "memory": memory,
        "storage": storage,
        "duckdb_setup": {
            "snapshot_conversion_ms": datasets["duckdb_parquet_ocpm_engine"][
                "snapshot_conversion_ms"
            ],
            "connection_open_ms": datasets["duckdb_parquet_ocpm_engine"][
                "connection_open_ms"
            ],
        },
        "summary": {
            "exact_answer_cells": len(exactness),
            "p50_geometric_mean_speedup_over_competitor": speedups,
            "duckdb_without_result_cache_p50_geometric_mean_speedup_over_competitor": duckdb_uncached_speedup,
        },
        "manifest": manifest,
        "arms": arms,
        "limitations": limitations,
    }


def render(value: dict[str, Any]) -> str:
    competitor = value["competitor"]
    display = "Rust4PM" if competitor == "rust4pm" else "OCPA"
    labels = {
        competitor: display,
        "pg_ocpm_ocpm_engine": "pg_ocpm + ocpm-engine",
        "duckdb_parquet_ocpm_engine": "DuckDB Parquet + ocpm-engine",
    }
    lines = [
        f"# {display}, pg_ocpm, and DuckDB Parquet benchmark",
        "",
        "## Result",
        "",
        (
            "Publication gate: **passed**."
            if value["publication_ready"]
            else "Publication gate: **descriptive only**."
        ),
        "",
        (
            f"All {value['summary']['exact_answer_cells']} workload answers matched exactly "
            f"on `{value['dataset']}`. Relative to {display}, the p50 geometric-mean "
            f"speedups were {value['summary']['p50_geometric_mean_speedup_over_competitor']['pg_ocpm_ocpm_engine']:.3f}x "
            "for pg_ocpm + ocpm-engine and "
            f"{value['summary']['p50_geometric_mean_speedup_over_competitor']['duckdb_parquet_ocpm_engine']:.3f}x "
            "for DuckDB Parquet + ocpm-engine with its bounded exact-result cache. "
            "With that cache disabled, the DuckDB p50 geometric-mean ratio over "
            f"{display} was {value['summary']['duckdb_without_result_cache_p50_geometric_mean_speedup_over_competitor']:.3f}x."
        ),
        "",
        "## Exactness",
        "",
        "| Workload | Exact | Answer SHA-256 |",
        "|---|---:|---|",
    ]
    for row in value["exactness"]:
        lines.append(f"| {row['workload']} | yes | `{row['answer_sha256']}` |")
    lines.extend(
        [
            "",
            "## Steady-state latency",
            "",
            f"| Workload | {display} p50 | pg_ocpm p50 | DuckDB cached p50 | DuckDB cache-off p50 | {display} p95 | pg_ocpm p95 | DuckDB cached p95 | DuckDB cache-off p95 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in value["latency"]:
        arms = row["arms"]
        uncached = row["duckdb_without_result_cache"]
        lines.append(
            f"| {row['workload']} | {arms[competitor]['p50_ms']:.3f} ms | "
            f"{arms['pg_ocpm_ocpm_engine']['p50_ms']:.3f} ms | "
            f"{arms['duckdb_parquet_ocpm_engine']['p50_ms']:.3f} ms | "
            f"{uncached['p50_ms']:.3f} ms | "
            f"{arms[competitor]['p95_ms']:.3f} ms | "
            f"{arms['pg_ocpm_ocpm_engine']['p95_ms']:.3f} ms | "
            f"{arms['duckdb_parquet_ocpm_engine']['p95_ms']:.3f} ms | "
            f"{uncached['p95_ms']:.3f} ms |"
        )
    lines.extend(
        [
            "",
            "## DFG concurrency",
            "",
            f"| Workers | {display} QPS | pg_ocpm QPS | DuckDB QPS | {display} p95 | pg_ocpm p95 | DuckDB p95 |",
            "|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in value["concurrency"]:
        arms = row["arms"]
        lines.append(
            f"| {row['workers']} | {arms[competitor]['throughput_qps']:.3f} | "
            f"{arms['pg_ocpm_ocpm_engine']['throughput_qps']:.3f} | "
            f"{arms['duckdb_parquet_ocpm_engine']['throughput_qps']:.3f} | "
            f"{arms[competitor]['p95_ms']:.3f} ms | "
            f"{arms['pg_ocpm_ocpm_engine']['p95_ms']:.3f} ms | "
            f"{arms['duckdb_parquet_ocpm_engine']['p95_ms']:.3f} ms |"
        )
    lines.extend(
        [
            "",
            "## Memory",
            "",
            "| Arm | Maximum incremental peak | Maximum process RSS |",
            "|---|---:|---:|",
        ]
    )
    for name, stats in value["memory"].items():
        lines.append(
            f"| {labels[name]} | {stats['maximum_incremental_peak_bytes'] / 1048576:.2f} MiB | "
            f"{stats['maximum_peak_rss_bytes'] / 1048576:.2f} MiB |"
        )
    lines.extend(["", "## Storage reported by each arm", ""])
    lines.extend(
        [
            "| Arm | Source SQLite | Canonical Parquet snapshot |",
            "|---|---:|---:|",
        ]
    )
    for name, stats in value["storage"].items():
        source = (
            f"{stats['source_sqlite_bytes'] / 1048576:.2f} MiB"
            if "source_sqlite_bytes" in stats
            else "N/A"
        )
        snapshot = (
            f"{stats['snapshot_bytes'] / 1048576:.2f} MiB"
            if "snapshot_bytes" in stats
            else "N/A"
        )
        lines.append(f"| {labels[name]} | {source} | {snapshot} |")
    setup = value["duckdb_setup"]
    lines.extend(
        [
            "",
            "## DuckDB setup cost",
            "",
            f"Snapshot conversion: {setup['snapshot_conversion_ms']:.3f} ms. "
            f"Existing-catalog connection open and optional relation materialization: {setup['connection_open_ms']:.3f} ms.",
        ]
    )
    lines.extend(["", "## Interpretation boundaries", ""])
    lines.extend(f"- {item}" for item in value["limitations"])
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    arguments = parse_args()
    result = build(arguments)
    Path(arguments.output).write_text(json.dumps(result, indent=2, default=str) + "\n")
    Path(arguments.report).write_text(render(result))
    print(f"wrote {arguments.output}", flush=True)
    print(f"wrote {arguments.report}", flush=True)
