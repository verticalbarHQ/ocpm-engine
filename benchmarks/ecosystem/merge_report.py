"""Merge isolated ecosystem arms, enforce exactness, and render pair reports."""

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
    parser.add_argument("--manifest", default="/results/ecosystem-manifest.json")
    parser.add_argument("--engine", action="append", required=True)
    parser.add_argument("--competitor", action="append", required=True)
    parser.add_argument("--competitor-name", choices=("rust4pm", "ocpa"), required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    return parser.parse_args()


def load_arm(paths: list[str], expected_arm: str) -> dict[str, Any]:
    payloads = [json.loads(Path(path).read_text()) for path in paths]
    if any(payload["arm"] != expected_arm for payload in payloads):
        raise SystemExit(f"expected only {expected_arm} arm artifacts")
    datasets = []
    seen = set()
    for payload in payloads:
        for dataset in payload["datasets"]:
            if dataset["dataset"] in seen:
                raise SystemExit(
                    f"duplicate {expected_arm} dataset {dataset['dataset']}"
                )
            seen.add(dataset["dataset"])
            datasets.append(dataset)
    datasets.sort(key=lambda row: row["dataset"])
    base = dict(payloads[0])
    base["datasets"] = datasets
    base["isolated_artifacts"] = paths
    return base


def dataset_map(arm: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["dataset"]: row for row in arm["datasets"]}


def exactness_rows(
    engine: dict[str, Any], competitor: dict[str, Any]
) -> list[dict[str, Any]]:
    engine_datasets = dataset_map(engine)
    competitor_datasets = dataset_map(competitor)
    if set(engine_datasets) != set(competitor_datasets):
        raise SystemExit("pair arms do not contain the same datasets")
    rows = []
    for dataset in sorted(engine_datasets):
        for workload in WORKLOADS:
            engine_value = engine_datasets[dataset]["serial"][workload]
            competitor_value = competitor_datasets[dataset]["serial"][workload]
            engine_answer = engine_value["answer"]
            competitor_answer = competitor_value["answer"]
            exact = canonical(engine_answer) == canonical(competitor_answer)
            engine_hash = answer_sha256(engine_answer)
            competitor_hash = answer_sha256(competitor_answer)
            if engine_hash != engine_value["answer_sha256"]:
                raise SystemExit(f"{dataset}/{workload}: invalid stored engine hash")
            if competitor_hash != competitor_value["answer_sha256"]:
                raise SystemExit(
                    f"{dataset}/{workload}: invalid stored competitor hash"
                )
            rows.append(
                {
                    "dataset": dataset,
                    "workload": workload,
                    "exact": exact,
                    "answer_sha256": engine_hash if exact else None,
                    "engine_sha256": engine_hash,
                    "competitor_sha256": competitor_hash,
                }
            )
    failures = [row for row in rows if not row["exact"]]
    if failures:
        labels = ", ".join(f"{row['dataset']}/{row['workload']}" for row in failures)
        raise SystemExit(f"exact-answer publication gate failed: {labels}")
    return rows


def latency_rows(
    engine: dict[str, Any], competitor: dict[str, Any]
) -> list[dict[str, Any]]:
    rows = []
    engine_datasets = dataset_map(engine)
    competitor_datasets = dataset_map(competitor)
    for dataset in sorted(engine_datasets):
        for workload in WORKLOADS:
            left = competitor_datasets[dataset]["serial"][workload]
            right = engine_datasets[dataset]["serial"][workload]
            rows.append(
                {
                    "dataset": dataset,
                    "workload": workload,
                    "competitor_p50_ms": left["p50_ms"],
                    "competitor_p95_ms": left["p95_ms"],
                    "engine_p50_ms": right["p50_ms"],
                    "engine_p95_ms": right["p95_ms"],
                    "engine_speedup_p50": round(
                        left["p50_ms"] / max(right["p50_ms"], 0.000001), 3
                    ),
                    "exact": True,
                }
            )
    return rows


def concurrency_rows(
    engine: dict[str, Any], competitor: dict[str, Any]
) -> list[dict[str, Any]]:
    rows = []
    engine_datasets = dataset_map(engine)
    competitor_datasets = dataset_map(competitor)
    for dataset in sorted(engine_datasets):
        engine_levels = engine_datasets[dataset]["concurrency"]
        competitor_levels = competitor_datasets[dataset]["concurrency"]
        if set(engine_levels) != set(competitor_levels):
            raise SystemExit(f"{dataset}: concurrency levels differ")
        for level in sorted(engine_levels, key=int):
            left = competitor_levels[level]
            right = engine_levels[level]
            if not (left["correct"] and right["correct"]):
                raise SystemExit(f"{dataset}/x{level}: concurrency correctness failed")
            rows.append(
                {
                    "dataset": dataset,
                    "workers": int(level),
                    "competitor_qps": left["throughput_qps"],
                    "competitor_p95_ms": left["p95_ms"],
                    "engine_qps": right["throughput_qps"],
                    "engine_p95_ms": right["p95_ms"],
                    "engine_throughput_ratio": round(
                        right["throughput_qps"] / max(left["throughput_qps"], 0.000001),
                        3,
                    ),
                    "exact": True,
                }
            )
    return rows


def geometric_mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return statistics.geometric_mean(values)


def build_result(
    manifest: dict[str, Any],
    engine: dict[str, Any],
    competitor: dict[str, Any],
    competitor_name: str,
) -> dict[str, Any]:
    exactness = exactness_rows(engine, competitor)
    latency = latency_rows(engine, competitor)
    concurrency = concurrency_rows(engine, competitor)
    engine_clean = bool(engine["implementation"].get("source_tree_clean"))
    pg_ocpm_clean = bool(engine["implementation"].get("pg_ocpm_source_tree_clean"))
    native_importers_succeeded = all(
        row["native_import_probe"]["success"] for row in competitor["datasets"]
    )
    limitations = [
        (
            "This is a shared workload benchmark, not OCPQ Q1-Q7. The strict "
            "OCPQ benchmark remains a separate artifact."
        ),
        (
            "Steady-state latency excludes one-time OCEL import, PostgreSQL "
            "fixture preparation, process startup, and worker startup; those "
            "costs and resident memory are reported separately."
        ),
        (
            "Each ecosystem uses its normal scalable service model: Rust4PM "
            "shares an immutable log across threads, OCPA uses preloaded forked "
            "processes, and ocpm-engine uses process workers with persistent "
            "PostgreSQL connections. Concurrency memory is therefore "
            "architectural, not a same-runtime microbenchmark."
        ),
        (
            "Each pair uses that competitor project's own upstream OCEL 2.0 "
            "dataset. The Rust4PM and OCPA reports therefore must not be "
            "compared as if they used the same input data."
        ),
        (
            "Native import is probed on the unchanged source and reported "
            "separately from steady-state execution. If an upstream importer "
            "fails, a disclosed benchmark-owned setup adapter may construct the "
            "project's public native model; such a result is not a native-import "
            "performance comparison."
        ),
        (
            "The competitor arms traverse each project's public native OCEL model "
            "and execute independently implemented versions of the four fixed "
            "common algorithms. They do not measure Rust4PM's Alpha+++ pipeline, "
            "OCPA's complete algorithm catalog, or a paper-specific end-to-end "
            "benchmark."
        ),
    ]
    ignored_o2o = sum(
        int(row["source_counts"].get("ignored_orphan_object_relations", 0))
        for row in manifest["datasets"]
    )
    if ignored_o2o:
        limitations.append(
            f"The source contains {ignored_o2o} O2O rows referencing objects absent "
            "from its object table. PostgreSQL loading excludes those impossible "
            "relations; none of the four event-object lifecycle workloads uses O2O."
        )
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "suite": "ecosystem-common-pm",
        "pair": f"{competitor_name}_vs_pg_ocpm_ocpm_engine",
        "publication_ready": (
            engine_clean and pg_ocpm_clean and native_importers_succeeded
        ),
        "strict_ocpq_suite_modified": False,
        "contract": manifest["contract"],
        "source": manifest["source"],
        "datasets": manifest["datasets"],
        "exactness": exactness,
        "latency": latency,
        "concurrency": concurrency,
        "summary": {
            "exact_answer_cells": sum(row["exact"] for row in exactness),
            "exact_answer_cells_expected": len(exactness),
            "engine_p50_geometric_mean_speedup": round(
                geometric_mean([row["engine_speedup_p50"] for row in latency]), 3
            ),
            "engine_p50_minimum_speedup": min(
                row["engine_speedup_p50"] for row in latency
            ),
            "engine_p50_maximum_speedup": max(
                row["engine_speedup_p50"] for row in latency
            ),
        },
        "publication_gate": {
            "exact_answers": True,
            "engine_source_tree_clean": engine_clean,
            "pg_ocpm_source_tree_clean": pg_ocpm_clean,
            "competitor_native_importers": native_importers_succeeded,
        },
        "arms": {
            competitor_name: competitor,
            "pg_ocpm_ocpm_engine": engine,
        },
        "limitations": limitations,
    }


def human_bytes(value: int | float) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size:.1f} {unit}"
        size /= 1024
    raise AssertionError


def render_report(result: dict[str, Any], competitor_name: str) -> str:
    display = "Rust4PM" if competitor_name == "rust4pm" else "OCPA"
    competitor = result["arms"][competitor_name]
    engine = result["arms"]["pg_ocpm_ocpm_engine"]
    competitor_datasets = dataset_map(competitor)
    dataset_entry = result["datasets"][0]
    fixture = dataset_entry["fixture"]
    source = result["source"]
    ignored_o2o = dataset_entry["source_counts"].get(
        "ignored_orphan_object_relations", 0
    )
    gate = result["publication_gate"]
    gate_failures = [
        label
        for condition, label in (
            (gate["engine_source_tree_clean"], "ocpm-engine source tree is dirty"),
            (gate["pg_ocpm_source_tree_clean"], "pg_ocpm source tree is dirty"),
            (
                gate["competitor_native_importers"],
                f"{display} native import did not succeed",
            ),
        )
        if not condition
    ]
    lines = [
        f"# {display} vs pg_ocpm + ocpm-engine 1.0",
        "",
        "## Result",
        "",
        (
            f"All {result['summary']['exact_answer_cells']} answer cells passed exact "
            f"equality. On `{dataset_entry['name']}` and the four fixed workloads, "
            "pg_ocpm + ocpm-engine had a "
            f"{result['summary']['engine_p50_geometric_mean_speedup']:.3f}x "
            f"geometric-mean p50 speedup over {display}."
        ),
        "",
        (
            "This is the separate `ecosystem-common-pm` suite. It does not add "
            "Rust4PM or OCPA cells to the strict OCPQ Q1-Q7 benchmark."
        ),
        "",
        (
            "Publication status: "
            + (
                "ready."
                if result["publication_ready"]
                else "not ready (" + "; ".join(gate_failures) + ")."
            )
        ),
        "",
    ]
    if competitor_name == "rust4pm":
        ratios = [row["engine_throughput_ratio"] for row in result["concurrency"]]
        if min(ratios) >= 1:
            concurrency_summary = (
                "The engine had higher DFG-conformance throughput at every "
                "measured concurrency level and delivered "
                f"{min(ratios):.3f}x to {max(ratios):.3f}x Rust4PM throughput."
            )
        elif max(ratios) < 1:
            concurrency_summary = (
                "Rust4PM had higher DFG-conformance throughput at every "
                "measured concurrency level; the engine delivered "
                f"{min(ratios):.3f}x to {max(ratios):.3f}x Rust4PM throughput."
            )
        else:
            concurrency_summary = (
                "DFG-conformance throughput leadership varied by concurrency; "
                "the engine delivered "
                f"{min(ratios):.3f}x to {max(ratios):.3f}x Rust4PM throughput."
            )
        lines.extend(
            [
                concurrency_summary,
                "",
            ]
        )
    elif not gate["competitor_native_importers"]:
        lines.extend(
            [
                (
                    "The OCPA query result is adapter-assisted because OCPA's "
                    "documented native importer fails on this unchanged upstream "
                    "file. It is not an OCPA native-import performance result."
                ),
                "",
            ]
        )
    lines.extend(
        [
            "## Fixed contract",
            "",
            (
                f"- Dataset: {source['title']} (`{source['identifier']}`), the "
                f"upstream {display} corpus, SHA-256 "
                f"`{dataset_entry['sqlite_sha256']}`."
            ),
            f"- Source license/terms: {source['license']}.",
            (
                f"- Backbone: `{fixture['object_type']}`, selected by the fixed "
                f"rule: {result['contract']['object_type_selection']['rule']}."
            ),
            (
                "- Workloads: 95% DFG conformance, 95% variant conformance, "
                "next-activity prediction, and edge bottleneck ranking."
            ),
            ("- Split: lifecycle-containment 80/20 windows, identical for both arms."),
            f"- Event order: {result['contract']['event_order']}.",
            (
                "- Invalid O2O rows excluded from PostgreSQL normalization: "
                f"{ignored_o2o}; O2O is outside the fixed workloads."
            ),
            (
                "- Latency: 10 warmups, 3 epochs of 30 measured requests, "
                "monotonic nanosecond clock."
            ),
            (
                "- Concurrency: DFG conformance at 1/2/4/8 workers, 3 epochs, at "
                "least 5 seconds and 32 requests per worker."
            ),
            (
                "- Publication gate: exact canonical answer equality for preflight, "
                "every serial sample, and every concurrency request."
            ),
            "",
            "## Correctness",
            "",
            "| Dataset | Workload | Exact | Answer SHA-256 |",
            "|---|---|---:|---|",
        ]
    )
    for row in result["exactness"]:
        lines.append(
            f"| {row['dataset']} | {row['workload']} | yes | `{row['answer_sha256']}` |"
        )
    lines.extend(
        [
            "",
            "## Steady-state latency",
            "",
            (
                f"| Dataset | Workload | {display} p50 | Engine p50 | "
                f"Engine speedup | {display} p95 | Engine p95 |"
            ),
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in result["latency"]:
        lines.append(
            f"| {row['dataset']} | {row['workload']} | "
            f"{row['competitor_p50_ms']:.3f} ms | "
            f"{row['engine_p50_ms']:.3f} ms | {row['engine_speedup_p50']:.3f}x | "
            f"{row['competitor_p95_ms']:.3f} ms | {row['engine_p95_ms']:.3f} ms |"
        )
    lines.extend(
        [
            "",
            "## Concurrency: DFG conformance",
            "",
            (
                f"| Dataset | Workers | {display} QPS | Engine QPS | Engine "
                f"throughput ratio | {display} p95 | Engine p95 |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in result["concurrency"]:
        lines.append(
            f"| {row['dataset']} | {row['workers']} | {row['competitor_qps']:.3f} | "
            f"{row['engine_qps']:.3f} | {row['engine_throughput_ratio']:.3f}x | "
            f"{row['competitor_p95_ms']:.3f} ms | {row['engine_p95_ms']:.3f} ms |"
        )
    lines.extend(
        [
            "",
            "## Import, resident memory, and storage",
            "",
            (
                f"| Dataset | {display} native importer | Model load | "
                f"{display} resident after load | Source SQLite |"
            ),
            "|---|---|---:|---:|---:|",
        ]
    )
    for dataset in sorted(competitor_datasets):
        row = competitor_datasets[dataset]
        probe = row["native_import_probe"]
        adapter = row["adapter"]
        load = adapter.get("load", row.get("load", {}))
        after = load.get(
            "rss_after_bytes", row.get("load", {}).get("rss_after_bytes", 0)
        )
        lines.append(
            f"| {dataset} | "
            f"{'pass' if probe['success'] else 'fail: ' + probe['error_type']} | "
            f"{load['seconds']:.3f} s | {human_bytes(after)} | "
            f"{human_bytes(row['source_sqlite_bytes'])} |"
        )
    pg_storage = engine["storage"]["pg_ocpm_schema"]["total_bytes"]
    lines.extend(
        [
            "",
            (
                f"The pg_ocpm schema uses {human_bytes(pg_storage)} for this loaded "
                "dataset. The competitor source-file sizes above are immutable input "
                "storage and do not include resident model expansion. Package and "
                "binary sizes are retained in the JSON artifact."
            ),
            "",
            "Concurrency memory is reported in each raw epoch. OCPA reports summed "
            "worker RSS/PSS; Rust4PM reports the shared threaded process RSS/PSS; "
            "the engine artifact reports isolated "
            "client-worker RSS and keeps PostgreSQL storage/environment separately.",
            "The report therefore does not claim a total-deployment memory win: "
            "PostgreSQL server memory is not added to the engine client RSS, while "
            "the in-process competitor arms include their loaded model.",
            "",
            "## Native importer capability",
            "",
        ]
    )
    for dataset in sorted(competitor_datasets):
        probe = competitor_datasets[dataset]["native_import_probe"]
        if probe["success"]:
            lines.append(f"- {dataset}: documented importer succeeded.")
        else:
            lines.append(
                f"- {dataset}: documented importer failed with "
                f"`{probe['error_type']}`: "
                f"`{probe['error']}`"
            )
    lines.extend(
        [
            "",
            (
                "The measured competitor arm uses the project's public native data "
                "model. Its per-dataset adapter field records whether that model was "
                "created by the documented importer or the disclosed setup repair. "
                "Neither route precomputes a DFG, variant table, score, expected "
                "answer, or benchmark-specific index."
            ),
            "",
            "## Interpretation boundaries",
            "",
        ]
    )
    lines.extend(f"- {limitation}" for limitation in result["limitations"])
    lines.extend(
        [
            "",
            "The exact numbers describe these fixed analytical workloads and service "
            "models. They do not imply the same ratio for arbitrary dynamic OCEL "
            "queries, discovery algorithms, conformance techniques, "
            "or workloads that use attributes omitted by the common contract.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    engine = load_arm(args.engine, "pg_ocpm_ocpm_engine")
    competitor = load_arm(args.competitor, args.competitor_name)
    result = build_result(manifest, engine, competitor, args.competitor_name)
    output = Path(args.output)
    report = Path(args.report)
    output.parent.mkdir(parents=True, exist_ok=True)
    report.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, default=str) + "\n")
    report.write_text(render_report(result, args.competitor_name))
    print(f"wrote {output}", flush=True)
    print(f"wrote {report}", flush=True)


if __name__ == "__main__":
    main()
