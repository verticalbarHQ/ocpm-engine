"""Validate SAP PM4Py evidence against a compact regression baseline."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import statistics
from pathlib import Path
from typing import Any

try:
    from benchmark_provenance import (
        PUBLIC_BENCHMARK_SCHEMA_VERSION,
        validate_recorded_public_provenance,
    )
except ModuleNotFoundError:  # loaded through importlib by unit tests
    from benchmarks.benchmark_provenance import (
        PUBLIC_BENCHMARK_SCHEMA_VERSION,
        validate_recorded_public_provenance,
    )

try:
    import check_sap_release_regression as release_regression_checker
except ModuleNotFoundError:  # loaded through importlib by unit tests
    from benchmarks import check_sap_release_regression as release_regression_checker

ROOT = Path(__file__).resolve().parents[1]
REGRESSION_BASELINE = (
    ROOT / "docs/results/sap-pm4py-three-way-0.4.0-regression-baseline.json"
)
RELEASE_BRIDGE = ROOT / ".benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json"
EXPECTED_PAYLOAD_SHA256 = (
    "8c086a36c590291624df9258bceae0067316737a8b058f8e597caac3d9710a7d"
)
EXPECTED_BASELINE_PAYLOAD_SHA256 = (
    "6530be1e72e249d022111f76283a4cb1393df726c0d413948b3689799c4e337b"
)
CURRENT_RELEASE = {"ocpm_engine": "1.0.0", "pg_ocpm": "1.0.0"}
BASELINE_RELEASE = {"ocpm_engine": "0.4.0", "pg_ocpm": "0.5.0"}
BASELINE_ARTIFACT_TYPE = "sap_pm4py_latency_memory_storage_regression_baseline"
EXPECTED_SOURCE = {
    "title": "Collection of Object-Centric Event Logs (OCEL 2.0 relational SQLite)",
    "doi": "10.5281/zenodo.8261133",
    "license": "CC BY 4.0",
    "datasets": ["sap_o2c", "sap_p2p"],
}
EXPECTED_DATASETS = ("sap_o2c", "sap_p2p")
EXPECTED_FIXTURE_FIELDS = {
    "dataset_name",
    "baseline_dataset_id",
    "ocpm_dataset_id",
    "tenant_id",
    "object_type",
    "from_time",
    "train_to",
    "test_from",
    "to_time",
    "cases",
    "train_cases",
    "test_cases",
}
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
EXPECTED_INPUT_FIELDS = {
    "vanilla_pg_pm4py": {
        "source",
        "event_rows",
        "dataframe_bytes",
        "aggregate_rows",
    },
    "pg_ocpm_pm4py": {
        "source",
        "event_rows",
        "dataframe_bytes",
        "aggregate_rows",
    },
    "pg_ocpm_ocpm_engine": {"aggregate_rows"},
}
FACTORIZED_ENGINE_INPUT_FIELDS = {
    "source",
    "strategy",
    "batch_rows",
    "event_rows",
    "expanded_event_rows",
    "payload_bytes",
    "aggregate_rows",
}
PROVIDER_AGGREGATE_INPUT_FIELDS = {
    "source",
    "strategy",
    "database_rows",
    "expanded_event_rows",
    "aggregate_rows",
}
PROVIDER_AGGREGATE_CONTRACTS = {
    "dfg_conformance_95pct": (
        "pg_ocpm_lifecycle_dfg_window_counts",
        "native lifecycle DFG window aggregate",
    ),
    "variant_conformance_95pct": (
        "pg_ocpm_lifecycle_variant_window_counts",
        "native lifecycle variant window aggregate",
    ),
    "next_activity_prediction": (
        "pg_ocpm_lifecycle_dfg_window_counts",
        "native lifecycle DFG window aggregate",
    ),
    "edge_bottleneck_ranking": (
        "pg_ocpm_edge_feature_aggregates",
        "native filtered edge feature aggregate",
    ),
}
EXPECTED_CONCURRENCY_EPOCHS = 3
MINIMUM_CONCURRENCY_SECONDS = 5.0
MINIMUM_REQUESTS_PER_WORKER = 32
MAXIMUM_CONCURRENCY_THROUGHPUT_CV = 0.15
MINIMUM_ENGINE_THROUGHPUT_RATIO = 3.0
MAXIMUM_ENGINE_CONCURRENCY_P95_MS = 50.0
EXPECTED_LATENCY_WARMUPS = 10
EXPECTED_LATENCY_EPOCHS = 3
EXPECTED_LATENCY_SAMPLES_PER_EPOCH = 30
EXPECTED_LATENCY_RUNS = EXPECTED_LATENCY_EPOCHS * EXPECTED_LATENCY_SAMPLES_PER_EPOCH
EXPECTED_RESULT_FIELDS = {
    "schema_version",
    "generated_at",
    "source",
    "environment",
    "provenance",
    "method",
    "summary",
    "datasets",
    "storage",
    "section_generated_at",
    "payload_sha256",
}
EXPECTED_DATASET_FIELDS = {
    "dataset",
    "source_counts",
    "fixture",
    "summary",
    "latency",
    "concurrency",
    "memory",
}
EXPECTED_LATENCY_FIELDS = {
    "workload",
    "correct",
    "vanilla_pg_pm4py",
    "pg_ocpm_pm4py",
    "pg_ocpm_ocpm_engine",
    "speedups",
    "first_execution_counts",
    "serial_epochs",
    "answer_sha256",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "result",
        nargs="?",
        default="docs/results/sap-pm4py-three-way-1.0.0.json",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=REGRESSION_BASELINE,
        help=(
            "compact committed source, fixture, input, and answer contract; "
            "historical performance values are not regression gates"
        ),
    )
    parser.add_argument(
        "--expected-payload-sha256",
        help="explicit expected current-artifact digest",
    )
    parser.add_argument(
        "--release-bridge",
        type=Path,
        help=(
            "optional ignored same-host non-regression artifact; required by "
            "the preview workflow and never published"
        ),
    )
    parser.add_argument(
        "--expected-release-bridge-sha256",
        help="explicit expected release-bridge payload digest",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help=(
            "validate a staged, self-digested artifact without requiring the "
            "published digest pin"
        ),
    )
    parser.add_argument(
        "--expected-ocpm-engine-version",
        default=CURRENT_RELEASE["ocpm_engine"],
        help="expected installed ocpm-engine package version",
    )
    parser.add_argument(
        "--expected-pg-ocpm-version",
        default=CURRENT_RELEASE["pg_ocpm"],
        help="expected installed pg_ocpm extension version",
    )
    return parser.parse_args()


def fail(message: str) -> None:
    raise SystemExit(message)


def validate_provider_aggregate_input(
    name: str,
    workload: str,
    input_value: Any,
    prior_input: dict[str, Any],
) -> None:
    """Validate the public provider API without binding 1.0 to legacy SQL input."""

    expected_source, expected_strategy = PROVIDER_AGGREGATE_CONTRACTS[workload]
    if (
        not isinstance(input_value, dict)
        or set(input_value) != PROVIDER_AGGREGATE_INPUT_FIELDS
        or input_value.get("source") != expected_source
        or input_value.get("strategy") != expected_strategy
        or type(input_value.get("database_rows")) is not int
        or input_value["database_rows"] <= 0
        or input_value.get("database_rows") != input_value.get("aggregate_rows")
        or input_value.get("expanded_event_rows") != 0
        or input_value.get("aggregate_rows") != prior_input.get("aggregate_rows")
    ):
        fail(f"{name}/{workload}/pg_ocpm_ocpm_engine: invalid provider aggregate input")


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


def validate_artifact_shape(result: dict[str, Any]) -> None:
    """Reject fields outside the schema-5 producer's stable structural rows."""

    if set(result) != EXPECTED_RESULT_FIELDS:
        fail("SAP schema-5 top-level fields changed")
    datasets = result.get("datasets")
    if not isinstance(datasets, list):
        fail("SAP schema-5 datasets must be a list")
    for dataset in datasets:
        if not isinstance(dataset, dict) or set(dataset) != EXPECTED_DATASET_FIELDS:
            fail("SAP schema-5 dataset fields changed")
        latency = dataset.get("latency")
        if not isinstance(latency, list):
            fail("SAP schema-5 latency rows must be a list")
        if any(
            not isinstance(row, dict) or set(row) != EXPECTED_LATENCY_FIELDS
            for row in latency
        ):
            fail("SAP schema-5 latency row fields changed")


def baseline_dataset_map(
    baseline: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    datasets = baseline.get("datasets", [])
    names = tuple(item.get("dataset") for item in datasets)
    if names != EXPECTED_DATASETS:
        fail("expected SAP O2C and P2P regression datasets in stable order")
    return {item["dataset"]: item for item in datasets}


def baseline_workload_map(dataset: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = dataset.get("workloads", [])
    names = tuple(row.get("workload") for row in rows)
    if names != EXPECTED_WORKLOADS:
        fail(f"{dataset['dataset']}: regression workload set or ordering changed")
    return {row["workload"]: row for row in rows}


def latency_map(dataset: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = dataset["latency"]
    names = tuple(row["workload"] for row in rows)
    if names != EXPECTED_WORKLOADS:
        fail(f"{dataset['dataset']}: latency workload set or ordering changed")
    return {row["workload"]: row for row in rows}


def validate_latency_sample_counts(method: dict[str, Any]) -> None:
    expected_orders = [list(order) for order in itertools.permutations(ENGINES)]
    expected_serial = {
        "epochs": EXPECTED_LATENCY_EPOCHS,
        "samples_per_epoch": EXPECTED_LATENCY_SAMPLES_PER_EPOCH,
        "total_samples_per_arm": EXPECTED_LATENCY_RUNS,
        "warmups": EXPECTED_LATENCY_WARMUPS,
        "clock": "time.perf_counter_ns",
        "percentile": "nearest-rank",
        "aggregation": (
            "pooled p50/p95 with retained per-epoch p50/p95/min/max and "
            "epoch p95 median/range"
        ),
        "raw_evidence": (
            "positive integer nanosecond samples and realized randomized "
            "arm-order codes"
        ),
        "arm_order_dictionary": expected_orders,
    }
    if (
        method.get("warmups") != EXPECTED_LATENCY_WARMUPS
        or method.get("measured_runs") != EXPECTED_LATENCY_RUNS
        or method.get("serial_latency") != expected_serial
    ):
        fail(
            "SAP release latency protocol requires 10 warmups and three "
            "epochs of 30 retained nanosecond samples per arm"
        )


def validate_regression_baseline(baseline: dict[str, Any]) -> None:
    expected_top_level = {
        "schema_version",
        "artifact_type",
        "release",
        "source",
        "datasets",
        "payload_sha256",
    }
    if set(baseline) != expected_top_level:
        fail("SAP regression baseline contains unexpected fields")
    if baseline.get("schema_version") != 1:
        fail("unexpected SAP regression baseline schema version")
    if baseline.get("artifact_type") != BASELINE_ARTIFACT_TYPE:
        fail("unexpected SAP regression baseline artifact type")
    if baseline.get("release") != BASELINE_RELEASE:
        fail("unexpected SAP regression baseline release versions")
    if baseline.get("source") != EXPECTED_SOURCE:
        fail("SAP regression baseline source metadata changed")

    datasets = baseline_dataset_map(baseline)
    for dataset in datasets.values():
        if set(dataset) != {"dataset", "source_counts", "fixture", "workloads"}:
            fail("SAP regression baseline dataset contains unexpected fields")
        if set(dataset.get("source_counts", {})) != {
            "events",
            "objects",
            "event_object_links",
            "object_object_links",
        }:
            fail(f"{dataset['dataset']}: regression source counts changed")
        if set(dataset.get("fixture", {})) != EXPECTED_FIXTURE_FIELDS:
            fail(f"{dataset['dataset']}: regression fixture fields changed")
        rows = baseline_workload_map(dataset)
        for row in rows.values():
            if set(row) != {
                "workload",
                "answer_sha256",
                "input",
            }:
                fail("SAP regression workload contains unexpected fields")
            answer_hash = row.get("answer_sha256")
            if (
                not isinstance(answer_hash, str)
                or len(answer_hash) != 64
                or any(character not in "0123456789abcdef" for character in answer_hash)
            ):
                fail("SAP regression baseline contains an invalid answer hash")
            if set(row.get("input", {})) != set(ENGINES):
                fail("SAP regression baseline input engines changed")
            if any(
                set(row["input"].get(engine, {})) != fields
                for engine, fields in EXPECTED_INPUT_FIELDS.items()
            ):
                fail("SAP regression baseline input fields changed")


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


def serial_metrics_ns(samples_ns: list[int]) -> dict[str, Any]:
    ordered = sorted(samples_ns)
    p95_index = math.ceil(len(ordered) * 0.95) - 1
    return {
        "p50_ms": round(statistics.median(ordered) / 1_000_000, 3),
        "p95_ms": round(ordered[p95_index] / 1_000_000, 3),
        "minimum_ms": round(ordered[0] / 1_000_000, 3),
        "maximum_ms": round(ordered[-1] / 1_000_000, 3),
        "runs": len(ordered),
    }


def validate_serial_latency_row(
    dataset: str,
    workload: str,
    row: dict[str, Any],
) -> None:
    label = f"{dataset}/{workload}"
    order_dictionary = [list(order) for order in itertools.permutations(ENGINES)]
    epochs = row.get("serial_epochs", [])
    if len(epochs) != EXPECTED_LATENCY_EPOCHS:
        fail(f"{label}: expected three retained serial-latency epochs")

    pooled = {engine: [] for engine in ENGINES}
    epoch_p95 = {engine: [] for engine in ENGINES}
    decoded_first_counts = {engine: 0 for engine in ENGINES}
    epoch_metric_fields = {
        "p50_ms",
        "p95_ms",
        "minimum_ms",
        "maximum_ms",
        "runs",
        "samples_ns",
    }
    for epoch_index, epoch in enumerate(epochs, start=1):
        if set(epoch) != {"epoch", "order_codes", "arms"}:
            fail(f"{label}: serial epoch contains unexpected fields")
        codes = epoch.get("order_codes", [])
        if (
            epoch.get("epoch") != epoch_index
            or len(codes) != EXPECTED_LATENCY_SAMPLES_PER_EPOCH
            or any(
                type(code) is not int or not 0 <= code < len(order_dictionary)
                for code in codes
            )
        ):
            fail(f"{label}: invalid retained serial arm-order codes")
        for code in codes:
            decoded_first_counts[order_dictionary[code][0]] += 1

        arms = epoch.get("arms", {})
        if set(arms) != set(ENGINES):
            fail(f"{label}: serial epoch arm set changed")
        for engine in ENGINES:
            evidence = arms[engine]
            if set(evidence) != epoch_metric_fields:
                fail(f"{label}/{engine}: serial epoch fields changed")
            samples = evidence.get("samples_ns", [])
            if len(samples) != EXPECTED_LATENCY_SAMPLES_PER_EPOCH or any(
                type(sample) is not int or sample <= 0 for sample in samples
            ):
                fail(f"{label}/{engine}: invalid raw nanosecond samples")
            recomputed = serial_metrics_ns(samples)
            if any(evidence.get(key) != value for key, value in recomputed.items()):
                fail(f"{label}/{engine}: serial epoch metrics do not match raw samples")
            pooled[engine].extend(samples)
            epoch_p95[engine].append(recomputed["p95_ms"])

    counts = row.get("first_execution_counts", {})
    validate_first_execution_counts(dataset, workload, counts, EXPECTED_LATENCY_RUNS)
    if any(value <= 0 for value in counts.values()) or counts != decoded_first_counts:
        fail(f"{label}: randomized serial execution counts changed")

    aggregate_fields = {
        "p50_ms",
        "p95_ms",
        "minimum_ms",
        "maximum_ms",
        "runs",
        "exact_samples",
        "epoch_count",
        "epoch_p95_median_ms",
        "epoch_p95_range_ms",
        "input",
    }
    for engine in ENGINES:
        evidence = row.get(engine, {})
        if set(evidence) != aggregate_fields:
            fail(f"{label}/{engine}: pooled serial fields changed")
        recomputed = serial_metrics_ns(pooled[engine])
        p95_values = epoch_p95[engine]
        expected = {
            **recomputed,
            "exact_samples": EXPECTED_LATENCY_RUNS,
            "epoch_count": EXPECTED_LATENCY_EPOCHS,
            "epoch_p95_median_ms": round(statistics.median(p95_values), 3),
            "epoch_p95_range_ms": [min(p95_values), max(p95_values)],
        }
        if any(evidence.get(key) != value for key, value in expected.items()):
            fail(f"{label}/{engine}: pooled metrics do not match raw samples")


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
    result: dict[str, Any],
    baseline: dict[str, Any],
    *,
    allow_dirty: bool,
    current_release: dict[str, str] = CURRENT_RELEASE,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    if not allow_dirty and current_release != CURRENT_RELEASE:
        fail("release validation uses only the published release pair")
    validate_artifact_shape(result)
    if result.get("schema_version") != PUBLIC_BENCHMARK_SCHEMA_VERSION:
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
        fail("SAP source differs from the compact regression baseline")
    validate_latency_sample_counts(result.get("method", {}))
    validate_concurrency_protocol(result)
    if (
        result["environment"]["client"].get("ocpm_engine_version")
        != current_release["ocpm_engine"]
    ):
        fail(f"expected ocpm-engine {current_release['ocpm_engine']}")
    if (
        result["environment"]["database"]["pg_ocpm"].get("pg_ocpm_version")
        != current_release["pg_ocpm"]
    ):
        fail(f"expected pg_ocpm {current_release['pg_ocpm']}")
    try:
        validate_recorded_public_provenance(
            result.get("provenance"), allow_dirty=allow_dirty
        )
    except ValueError as error:
        fail(str(error))

    datasets = dataset_map(result)
    prior_datasets = baseline_dataset_map(baseline)
    latency_rows = 0
    for dataset_index, (name, dataset) in enumerate(datasets.items()):
        prior_dataset = prior_datasets[name]
        if dataset["source_counts"] != prior_dataset["source_counts"]:
            fail(f"{name}: source row counts changed")
        if dataset["fixture"] != prior_dataset["fixture"]:
            fail(f"{name}: fixture partition or filter settings changed")
        rows = latency_map(dataset)
        prior_rows = baseline_workload_map(prior_dataset)
        for workload, row in rows.items():
            latency_rows += 1
            prior_row = prior_rows[workload]
            if row.get("correct") is not True:
                fail(f"{name}/{workload}: exact latency correctness gate failed")
            if row.get("answer_sha256") != prior_row.get("answer_sha256"):
                fail(f"{name}/{workload}: canonical answer hash changed")
            validate_serial_latency_row(name, workload, row)
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
                input_value = metrics.get("input")
                if (
                    engine == "pg_ocpm_ocpm_engine"
                    and result["method"].get("ocpm_engine_read_path") == "factorized"
                ):
                    if (
                        not isinstance(input_value, dict)
                        or set(input_value) != FACTORIZED_ENGINE_INPUT_FIELDS
                        or input_value.get("source") != "pg_ocpm_event_batches"
                        or input_value.get("strategy")
                        not in {
                            "factorized event batch",
                            "factorized multi-window event batch",
                        }
                        or type(input_value.get("batch_rows")) is not int
                        or input_value["batch_rows"] <= 0
                        or input_value.get("event_rows")
                        != prior_row["input"]["vanilla_pg_pm4py"]["event_rows"]
                        or input_value.get("expanded_event_rows") != 0
                        or type(input_value.get("payload_bytes")) is not int
                        or input_value["payload_bytes"] <= 0
                        or input_value.get("aggregate_rows")
                        != prior_row["input"][engine]["aggregate_rows"]
                    ):
                        fail(f"{name}/{workload}/{engine}: invalid factorized input")
                elif (
                    engine == "pg_ocpm_ocpm_engine"
                    and result["method"].get("ocpm_engine_read_path") == "auto"
                ):
                    validate_provider_aggregate_input(
                        name,
                        workload,
                        input_value,
                        prior_row["input"][engine],
                    )
                elif input_value != prior_row["input"].get(engine):
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
                if metrics.get("input") != rows[workload][engine].get("input"):
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
    if packages["ocpm_engine"].get("version") != current_release["ocpm_engine"]:
        fail("ocpm-engine package version changed")
    if (
        packages["pm4py"].get("additional_database_bytes") != 0
        or packages["ocpm_engine"].get("additional_database_bytes") != 0
    ):
        fail("unexpected client-specific database storage")
    return datasets, prior_datasets


def validate_regressions(
    result: dict[str, Any],
    bridge: dict[str, Any],
    *,
    allow_dirty: bool,
) -> None:
    release_regression_checker.validate_for_public_pm4py(
        result,
        bridge,
        allow_dirty=allow_dirty,
    )


def resolve_expected_release(
    *, preview: bool, ocpm_engine: str, pg_ocpm: str
) -> dict[str, str]:
    requested = {"ocpm_engine": ocpm_engine, "pg_ocpm": pg_ocpm}
    if not preview and requested != CURRENT_RELEASE:
        fail("release validation uses only the published release pair")
    return requested


def main() -> None:
    args = parse_args()
    if args.preview and (
        args.expected_payload_sha256 or args.expected_release_bridge_sha256
    ):
        fail("--preview and explicit expected digests are mutually exclusive")
    expected_digest = (
        None
        if args.preview
        else args.expected_payload_sha256 or EXPECTED_PAYLOAD_SHA256
    )
    if args.release_bridge is None and args.expected_release_bridge_sha256:
        fail("--expected-release-bridge-sha256 requires --release-bridge")
    result = load_verified(Path(args.result), expected_digest)
    baseline = load_verified(args.baseline, EXPECTED_BASELINE_PAYLOAD_SHA256)
    validate_regression_baseline(baseline)
    current_release = resolve_expected_release(
        preview=args.preview,
        ocpm_engine=args.expected_ocpm_engine_version,
        pg_ocpm=args.expected_pg_ocpm_version,
    )
    datasets, _ = validate_contract(
        result,
        baseline,
        allow_dirty=args.preview,
        current_release=current_release,
    )
    bridge_checked = False
    if args.release_bridge is not None:
        bridge_digest = None if args.preview else args.expected_release_bridge_sha256
        if not args.preview and bridge_digest is None:
            fail("an explicitly supplied release bridge requires a pinned digest")
        bridge = release_regression_checker.load_verified(
            args.release_bridge,
            bridge_digest,
        )
        validate_regressions(result, bridge, allow_dirty=args.preview)
        bridge_checked = True
    latency = [row for dataset in datasets.values() for row in dataset["latency"]]
    suffix = "; private same-host non-regression gates passed" if bridge_checked else ""
    print(
        "SAP PM4Py benchmark verified: "
        f"{len(latency)} exact current-versus-vanilla workloads{suffix}"
    )


if __name__ == "__main__":
    main()
