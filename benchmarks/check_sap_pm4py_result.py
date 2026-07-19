"""Validate the SAP PM4Py artifact and historical regression gates."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
from pathlib import Path
from typing import Any

try:
    from benchmark_provenance import validate_recorded_public_provenance
except ModuleNotFoundError:  # loaded through importlib by unit tests
    from benchmarks.benchmark_provenance import validate_recorded_public_provenance

ROOT = Path(__file__).resolve().parents[1]
PRIOR_ARTIFACT = ROOT / "docs/results/sap-pm4py-three-way-2026-07-18.json"
EXPECTED_PAYLOAD_SHA256 = (
    "c96f53261f7c02c5d563dad51a2c875561e8c47ee114fe1b4016636aae50c9c2"
)
EXPECTED_PRIOR_PAYLOAD_SHA256 = (
    "3e432c3c3a26d20159f4a008ead90dbc3ba222716b9d21f095d110f7ab029562"
)
EXPECTED_SOURCE = {
    "title": "Collection of Object-Centric Event Logs (OCEL 2.0 relational SQLite)",
    "doi": "10.5281/zenodo.8261133",
    "license": "CC BY 4.0",
    "datasets": ["sap_o2c", "sap_p2p"],
}
EXPECTED_DATASETS = ("sap_o2c", "sap_p2p")
EXPECTED_WORKLOADS = (
    "dfg_conformance_95pct",
    "variant_conformance_95pct",
    "next_activity_prediction",
    "edge_bottleneck_ranking",
)
EXPECTED_LEVELS = ("1", "2", "4", "8")
ENGINES = (
    "vanilla_pg_pm4py",
    "pg_ocpm_pm4py",
    "pg_ocpm_ocpm_engine",
)
LATENCY_CEILING = 1.10
EXPECTED_CONCURRENCY_EPOCHS = 3
MINIMUM_CONCURRENCY_SECONDS = 5.0
MINIMUM_REQUESTS_PER_WORKER = 32
STORAGE_CEILING = 1.01
MEMORY_CEILING = 1.10
MEMORY_PAGE_SLACK = 64 * 1024
MAX_ENGINE_INCREMENTAL_PEAK_BYTES = 8 * 1024 * 1024
MAX_ENGINE_PEAK_RSS_BYTES = 64 * 1024 * 1024
MAX_PM4PY_PEAK_RSS_BYTES = 256 * 1024 * 1024
MAXIMUM_CONCURRENCY_THROUGHPUT_CV = 0.15
MINIMUM_ENGINE_THROUGHPUT_RATIO = 3.0
MAXIMUM_ENGINE_CONCURRENCY_P95_MS = 50.0
EXPECTED_LATENCY_WARMUPS = 10
EXPECTED_LATENCY_RUNS = 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "result",
        nargs="?",
        default="docs/results/sap-pm4py-three-way-0.5.0.json",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=PRIOR_ARTIFACT,
        help="prior committed release artifact used only for regression gates",
    )
    parser.add_argument(
        "--expected-payload-sha256",
        help="explicit expected current-artifact digest",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help=(
            "validate a staged, self-digested artifact without requiring the "
            "published digest pin"
        ),
    )
    return parser.parse_args()


def fail(message: str) -> None:
    raise SystemExit(message)


def load_verified(path: Path, expected_digest: str | None = None) -> dict[str, Any]:
    result = json.loads(path.read_text())
    recorded = result.get("payload_sha256")
    unsigned = dict(result)
    unsigned.pop("payload_sha256", None)
    encoded = json.dumps(unsigned, indent=2, default=str) + "\n"
    computed = hashlib.sha256(encoded.encode()).hexdigest()
    if recorded != computed:
        fail(
            f"{path}: payload digest mismatch: "
            f"recorded={recorded!r}, computed={computed}"
        )
    if expected_digest is not None and recorded != expected_digest:
        fail(
            f"{path}: unexpected payload digest: "
            f"recorded={recorded!r}, expected={expected_digest}"
        )
    return result


def dataset_map(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    datasets = result.get("datasets", [])
    names = tuple(item["dataset"] for item in datasets)
    if names != EXPECTED_DATASETS:
        fail("expected SAP O2C and P2P datasets in stable order")
    return {item["dataset"]: item for item in datasets}


def comparable_environment(result: dict[str, Any]) -> dict[str, Any]:
    environment = json.loads(json.dumps(result["environment"]))
    environment["client"].pop("ocpm_engine_version", None)
    environment["database"]["pg_ocpm"].pop("pg_ocpm_version", None)
    return environment


def latency_map(dataset: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = dataset["latency"]
    names = tuple(row["workload"] for row in rows)
    if names != EXPECTED_WORKLOADS:
        fail(f"{dataset['dataset']}: latency workload set or ordering changed")
    return {row["workload"]: row for row in rows}


def latency_method(method: dict[str, Any]) -> dict[str, Any]:
    stable = dict(method)
    stable.pop("concurrency", None)
    stable.pop("concurrency_model", None)
    stable.pop("warmups", None)
    stable.pop("measured_runs", None)
    return stable


def validate_latency_sample_counts(method: dict[str, Any]) -> None:
    if (
        method.get("warmups") != EXPECTED_LATENCY_WARMUPS
        or method.get("measured_runs") != EXPECTED_LATENCY_RUNS
    ):
        fail(
            "SAP release latency protocol requires exactly "
            f"{EXPECTED_LATENCY_WARMUPS} warmups and "
            f"{EXPECTED_LATENCY_RUNS} measured runs"
        )


def validate_first_execution_counts(
    dataset: str,
    workload: str,
    counts: dict[str, Any],
    measured_runs: int,
) -> None:
    if (
        set(counts) != set(ENGINES)
        or any(type(value) is not int or value < 0 for value in counts.values())
        or sum(counts.values()) != measured_runs
    ):
        fail(f"{dataset}/{workload}: randomized execution counts changed")


def validate_concurrency_protocol(result: dict[str, Any]) -> None:
    protocol = result["method"].get("concurrency", {})
    if (
        protocol.get("epochs_per_engine_level") != EXPECTED_CONCURRENCY_EPOCHS
        or protocol.get("minimum_epoch_seconds") != MINIMUM_CONCURRENCY_SECONDS
        or protocol.get("minimum_requests_per_worker_per_epoch")
        != MINIMUM_REQUESTS_PER_WORKER
        or "persistent PostgreSQL connection"
        not in protocol.get("connection_model", "")
        or "every worker PID" not in protocol.get("warmup_gate", "")
        or "every measured request" not in protocol.get("correctness_gate", "")
        or "median epoch QPS" not in protocol.get("aggregation", "")
    ):
        fail("SAP concurrency protocol is not the stable three-epoch contract")


def validate_concurrency(
    dataset_name: str,
    dataset_index: int,
    concurrency: dict[str, Any],
    expected_hash: str,
) -> None:
    if concurrency.get("workload") != "dfg_conformance_95pct":
        fail(f"{dataset_name}: concurrency workload changed")
    if tuple(concurrency.get("levels", [])) != EXPECTED_LEVELS:
        fail(f"{dataset_name}: concurrency levels changed")
    orders = concurrency.get("epoch_arm_orders", {})
    if tuple(orders) != EXPECTED_LEVELS:
        fail(f"{dataset_name}: epoch arm-order levels changed")
    for level_index, level in enumerate(EXPECTED_LEVELS):
        level_orders = orders[level]
        if len(level_orders) != EXPECTED_CONCURRENCY_EPOCHS:
            fail(f"{dataset_name}/x{level}: expected three epoch arm orders")
        for epoch_index, order in enumerate(level_orders):
            offset = (dataset_index + level_index + epoch_index) % len(ENGINES)
            expected_order = ENGINES[offset:] + ENGINES[:offset]
            if tuple(order) != expected_order:
                fail(f"{dataset_name}/x{level}: arm rotation changed")

        workers = int(level)
        for engine in ENGINES:
            if tuple(concurrency.get(engine, {})) != EXPECTED_LEVELS:
                fail(f"{dataset_name}/{engine}: concurrency levels changed")
            aggregate = concurrency[engine][level]
            epochs = aggregate.get("epochs", [])
            if (
                aggregate.get("workers") != workers
                or aggregate.get("epoch_count") != EXPECTED_CONCURRENCY_EPOCHS
                or len(epochs) != EXPECTED_CONCURRENCY_EPOCHS
                or aggregate.get("correct") is not True
            ):
                fail(f"{dataset_name}/{engine}/x{level}: invalid epoch aggregate")
            for epoch_index, epoch in enumerate(epochs):
                order = level_orders[epoch_index]
                worker_ids = epoch.get("worker_ids", [])
                request_counts = epoch.get("worker_request_counts", [])
                if (
                    epoch.get("epoch") != epoch_index + 1
                    or epoch.get("arm_position") != order.index(engine) + 1
                    or epoch.get("correct") is not True
                    or epoch.get("warmed_worker_count") != workers
                    or len(worker_ids) != workers
                    or len(set(worker_ids)) != workers
                    or len(request_counts) != workers
                    or min(request_counts, default=0) < MINIMUM_REQUESTS_PER_WORKER
                    or epoch.get("requests") != sum(request_counts)
                    or epoch.get("wall_ms", 0) < MINIMUM_CONCURRENCY_SECONDS * 1000
                    or epoch.get("answer_sha256") != expected_hash
                ):
                    fail(
                        f"{dataset_name}/{engine}/x{level}: invalid epoch "
                        f"{epoch_index + 1}"
                    )
                expected_qps = epoch["requests"] * 1000 / epoch["wall_ms"]
                if not math.isclose(
                    epoch.get("throughput_qps", 0), expected_qps, abs_tol=0.01
                ):
                    fail(f"{dataset_name}/{engine}/x{level}: inconsistent epoch QPS")
                if not (
                    0
                    <= epoch.get("minimum_ms", -1)
                    <= epoch.get("p50_ms", -1)
                    <= epoch.get("p95_ms", -1)
                    <= epoch.get("p99_ms", -1)
                    <= epoch.get("maximum_ms", -1)
                ):
                    fail(f"{dataset_name}/{engine}/x{level}: invalid epoch latency")
            aggregates = {
                "throughput_qps": round(
                    statistics.median(epoch["throughput_qps"] for epoch in epochs), 3
                ),
                "p50_ms": round(
                    statistics.median(epoch["p50_ms"] for epoch in epochs), 3
                ),
                "p95_ms": round(
                    statistics.median(epoch["p95_ms"] for epoch in epochs), 3
                ),
                "p99_ms": round(
                    statistics.median(epoch["p99_ms"] for epoch in epochs), 3
                ),
            }
            if any(aggregate.get(key) != value for key, value in aggregates.items()):
                fail(f"{dataset_name}/{engine}/x{level}: aggregate is not epoch median")
            if aggregate.get("requests") != sum(epoch["requests"] for epoch in epochs):
                fail(
                    f"{dataset_name}/{engine}/x{level}: aggregate request count changed"
                )
            qps = [epoch["throughput_qps"] for epoch in epochs]
            if statistics.pstdev(qps) / statistics.fmean(qps) > (
                MAXIMUM_CONCURRENCY_THROUGHPUT_CV
            ):
                fail(f"{dataset_name}/{engine}/x{level}: unstable epoch throughput")
        vanilla = concurrency["vanilla_pg_pm4py"][level]
        engine = concurrency["pg_ocpm_ocpm_engine"][level]
        if engine["throughput_qps"] < (
            vanilla["throughput_qps"] * MINIMUM_ENGINE_THROUGHPUT_RATIO
        ):
            fail(f"{dataset_name}/x{level}: engine throughput ratio fell below 3x")
        if engine["p95_ms"] > MAXIMUM_ENGINE_CONCURRENCY_P95_MS:
            fail(f"{dataset_name}/x{level}: engine p95 exceeded 50 ms")


def validate_contract(
    result: dict[str, Any], baseline: dict[str, Any], *, allow_dirty: bool
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    if result.get("schema_version") != 3:
        fail("unexpected SAP benchmark schema version")
    section_times = result.get("section_generated_at", {})
    if (
        set(section_times) != {"latency_storage_and_memory", "concurrency"}
        or section_times.get("concurrency") != result.get("generated_at")
        or not isinstance(section_times.get("latency_storage_and_memory"), str)
    ):
        fail("SAP section-level generation provenance is invalid")
    if result.get("source") != EXPECTED_SOURCE:
        fail("SAP source metadata changed")
    if result.get("source") != baseline.get("source"):
        fail("SAP source differs from the prior committed fixture")
    if latency_method(result.get("method", {})) != latency_method(
        baseline.get("method", {})
    ):
        fail("SAP measurement boundary or settings changed")
    validate_latency_sample_counts(result.get("method", {}))
    validate_concurrency_protocol(result)
    if result["environment"]["client"].get("ocpm_engine_version") != "0.5.0":
        fail("expected ocpm-engine 0.5.0")
    if result["environment"]["database"]["pg_ocpm"].get("pg_ocpm_version") != "0.6.0":
        fail("expected pg_ocpm 0.6.0")
    if comparable_environment(result) != comparable_environment(baseline):
        fail("SAP host, PostgreSQL, PM4Py, or database settings changed")
    try:
        validate_recorded_public_provenance(
            result.get("provenance"), allow_dirty=allow_dirty
        )
    except ValueError as error:
        fail(str(error))

    datasets = dataset_map(result)
    prior_datasets = dataset_map(baseline)
    latency_rows = 0
    for dataset_index, (name, dataset) in enumerate(datasets.items()):
        prior_dataset = prior_datasets[name]
        if dataset["source_counts"] != prior_dataset["source_counts"]:
            fail(f"{name}: source row counts changed")
        if dataset["fixture"] != prior_dataset["fixture"]:
            fail(f"{name}: fixture partition or filter settings changed")
        rows = latency_map(dataset)
        prior_rows = latency_map(prior_dataset)
        for workload, row in rows.items():
            latency_rows += 1
            prior_row = prior_rows[workload]
            if row.get("correct") is not True:
                fail(f"{name}/{workload}: exact latency correctness gate failed")
            if row.get("answer_sha256") != prior_row.get("answer_sha256"):
                fail(f"{name}/{workload}: canonical answer hash changed")
            validate_first_execution_counts(
                name,
                workload,
                row.get("first_execution_counts", {}),
                result["method"]["measured_runs"],
            )
            for engine in ENGINES:
                metrics = row[engine]
                if metrics.get("runs") != result["method"]["measured_runs"]:
                    fail(f"{name}/{workload}/{engine}: run count changed")
                if metrics.get("exact_samples") != result["method"]["measured_runs"]:
                    fail(
                        f"{name}/{workload}/{engine}: not every timed sample "
                        "passed exactness"
                    )
                minimum = metrics.get("minimum_ms", 0)
                p50 = metrics.get("p50_ms", 0)
                p95 = metrics.get("p95_ms", 0)
                if not (0 < minimum <= p50 <= p95):
                    fail(f"{name}/{workload}/{engine}: invalid latency metrics")
                if metrics.get("input") != prior_row[engine].get("input"):
                    fail(f"{name}/{workload}/{engine}: timed input shape changed")
            speedups = row["speedups"]
            expected = {
                "pg_ocpm_pm4py_vs_vanilla": round(
                    row["vanilla_pg_pm4py"]["p50_ms"] / row["pg_ocpm_pm4py"]["p50_ms"],
                    3,
                ),
                "pg_ocpm_ocpm_engine_vs_vanilla": round(
                    row["vanilla_pg_pm4py"]["p50_ms"]
                    / row["pg_ocpm_ocpm_engine"]["p50_ms"],
                    3,
                ),
                "pg_ocpm_ocpm_engine_vs_pg_ocpm_pm4py": round(
                    row["pg_ocpm_pm4py"]["p50_ms"]
                    / row["pg_ocpm_ocpm_engine"]["p50_ms"],
                    3,
                ),
            }
            if speedups != expected:
                fail(f"{name}/{workload}: inconsistent speedups")

        validate_concurrency(
            name,
            dataset_index,
            dataset["concurrency"],
            rows["dfg_conformance_95pct"]["answer_sha256"],
        )
        for workload, memory in dataset["memory"].items():
            if workload not in EXPECTED_WORKLOADS:
                fail(f"{name}: unexpected memory workload {workload}")
            hashes = {memory[engine]["answer_sha256"] for engine in ENGINES}
            if hashes != {rows[workload]["answer_sha256"]}:
                fail(f"{name}/{workload}: isolated-memory answer hashes disagree")
            for engine in ENGINES:
                metrics = memory[engine]
                baseline_rss = metrics.get("baseline_rss_bytes", -1)
                peak_rss = metrics.get("peak_rss_bytes", -1)
                incremental = metrics.get("incremental_peak_bytes", -1)
                if (
                    baseline_rss <= 0
                    or peak_rss < baseline_rss
                    or incremental != peak_rss - baseline_rss
                ):
                    fail(f"{name}/{workload}/{engine}: invalid RSS accounting")
                prior_input = prior_dataset["memory"][workload][engine].get("input")
                if metrics.get("input") != prior_input:
                    fail(f"{name}/{workload}/{engine}: memory input shape changed")
        if tuple(dataset["memory"]) != EXPECTED_WORKLOADS:
            fail(f"{name}: memory workload set or ordering changed")

    summary = result["summary"]
    if latency_rows != 8 or summary.get("total_workloads") != 8:
        fail("expected eight SAP latency comparisons")
    if summary.get("correct_workloads") != 8:
        fail("not every SAP workload passed exact correctness")
    all_rows = [row for dataset in datasets.values() for row in dataset["latency"]]
    for summary_key, speedup_key in (
        (
            "geometric_mean_pg_ocpm_pm4py_speedup_vs_vanilla",
            "pg_ocpm_pm4py_vs_vanilla",
        ),
        (
            "geometric_mean_ocpm_engine_speedup_vs_vanilla",
            "pg_ocpm_ocpm_engine_vs_vanilla",
        ),
        (
            "geometric_mean_ocpm_engine_speedup_vs_pg_ocpm_pm4py",
            "pg_ocpm_ocpm_engine_vs_pg_ocpm_pm4py",
        ),
    ):
        values = [row["speedups"][speedup_key] for row in all_rows]
        geometric = math.exp(sum(math.log(value) for value in values) / len(values))
        if not math.isclose(summary[summary_key], geometric, abs_tol=0.001):
            fail(f"inconsistent SAP summary metric: {summary_key}")
    if not all(
        row["pg_ocpm_ocpm_engine"]["p50_ms"] < row["vanilla_pg_pm4py"]["p50_ms"]
        for row in all_rows
    ):
        fail("ocpm-engine did not beat vanilla PostgreSQL on every SAP workload")

    secondary = [
        index
        for index in result["storage"]["vanilla_pg_pm4py"]["indexes"]
        if index["definition"].startswith("CREATE INDEX")
        and "UNIQUE" not in index["definition"]
    ]
    if [index["name"] for index in secondary] != ["ocel_e2o_object"]:
        fail("vanilla light-index policy changed")
    packages = result["storage"]["client_packages"]
    if packages["pm4py"].get("version") != "2.7.23.3":
        fail("PM4Py version changed")
    if packages["ocpm_engine"].get("version") != "0.5.0":
        fail("ocpm-engine package version changed")
    if (
        packages["pm4py"].get("additional_database_bytes") != 0
        or packages["ocpm_engine"].get("additional_database_bytes") != 0
    ):
        fail("unexpected client-specific database storage")
    return datasets, prior_datasets


def validate_regressions(
    result: dict[str, Any],
    baseline: dict[str, Any],
    datasets: dict[str, dict[str, Any]],
    prior_datasets: dict[str, dict[str, Any]],
) -> None:
    for name, dataset in datasets.items():
        prior_dataset = prior_datasets[name]
        rows = latency_map(dataset)
        prior_rows = latency_map(prior_dataset)
        for workload, row in rows.items():
            for engine in ENGINES:
                current = row[engine]["p50_ms"]
                prior = prior_rows[workload][engine]["p50_ms"]
                if current > prior * LATENCY_CEILING:
                    fail(f"{name}/{workload}/{engine}: p50 regression")

        for workload, memory in dataset["memory"].items():
            prior_memory = prior_dataset["memory"][workload]
            for engine in ENGINES:
                current = memory[engine]["incremental_peak_bytes"]
                prior = prior_memory[engine]["incremental_peak_bytes"]
                allowed = max(
                    math.ceil(prior * MEMORY_CEILING),
                    prior + MEMORY_PAGE_SLACK,
                )
                if current > allowed:
                    fail(f"{name}/{workload}/{engine}: memory regression")
                if (
                    engine == "pg_ocpm_ocpm_engine"
                    and current > MAX_ENGINE_INCREMENTAL_PEAK_BYTES
                ):
                    fail(f"{name}/{workload}: engine memory bound exceeded")
                current_peak = memory[engine]["peak_rss_bytes"]
                prior_peak = prior_memory[engine]["peak_rss_bytes"]
                peak_allowed = max(
                    math.ceil(prior_peak * MEMORY_CEILING),
                    prior_peak + MEMORY_PAGE_SLACK,
                )
                if current_peak > peak_allowed:
                    fail(f"{name}/{workload}/{engine}: total peak RSS regression")
                absolute_peak = (
                    MAX_ENGINE_PEAK_RSS_BYTES
                    if engine == "pg_ocpm_ocpm_engine"
                    else MAX_PM4PY_PEAK_RSS_BYTES
                )
                if current_peak > absolute_peak:
                    fail(f"{name}/{workload}/{engine}: total peak RSS bound exceeded")

    for representation in ("vanilla_pg_pm4py", "shared_pg_ocpm"):
        for metric in ("index_bytes", "total_bytes"):
            current = result["storage"][representation][metric]
            prior = baseline["storage"][representation][metric]
            if current > prior * STORAGE_CEILING:
                fail(f"{representation}/{metric}: storage regression")

    old_indexes = {
        item["name"]: (item["table"], item["definition"])
        for item in baseline["storage"]["shared_pg_ocpm"]["indexes"]
    }
    new_indexes = {
        item["name"]: (item["table"], item["definition"])
        for item in result["storage"]["shared_pg_ocpm"]["indexes"]
    }
    if set(old_indexes) - set(new_indexes):
        fail("an existing pg_ocpm index disappeared")
    if {
        name: new_indexes[name]
        for name in old_indexes
        if new_indexes[name] != old_indexes[name]
    }:
        fail("an existing pg_ocpm index definition changed")
    if set(new_indexes) - set(old_indexes) != {"binding_relation_summary_pkey"}:
        fail("unexpected pg_ocpm index additions")


def main() -> None:
    args = parse_args()
    if args.preview and args.expected_payload_sha256:
        fail("--preview and --expected-payload-sha256 are mutually exclusive")
    expected_digest = (
        None
        if args.preview
        else args.expected_payload_sha256 or EXPECTED_PAYLOAD_SHA256
    )
    result = load_verified(Path(args.result), expected_digest)
    baseline = load_verified(args.baseline, EXPECTED_PRIOR_PAYLOAD_SHA256)
    datasets, prior_datasets = validate_contract(
        result, baseline, allow_dirty=args.preview
    )
    validate_regressions(result, baseline, datasets, prior_datasets)
    latency = [row for dataset in datasets.values() for row in dataset["latency"]]
    print(
        "SAP PM4Py benchmark verified: "
        f"{len(latency)} exact workloads; latency, three-epoch concurrency, "
        "storage, and memory gates passed"
    )


if __name__ == "__main__":
    main()
