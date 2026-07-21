"""Validate public common-PM evidence against a compact regression baseline."""

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
    ROOT / "docs/results/public-common-pm-0.4.0-regression-baseline.json"
)
RELEASE_BRIDGE = ROOT / ".benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json"
EXPECTED_PAYLOAD_SHA256 = (
    "8a64067bdb7b0c40a54256f29d4172b3a528677ed0d28dae3552e178a54d87ea"
)
EXPECTED_BASELINE_PAYLOAD_SHA256 = (
    "bff5956359d45f070905408081dcb466b9d7ec7bb5115271cc0d8347a5c9a60a"
)
CURRENT_RELEASE = {"ocpm_engine": "0.8.0", "pg_ocpm": "0.8.0"}
BASELINE_RELEASE = {"ocpm_engine": "0.4.0", "pg_ocpm": "0.5.0"}
BASELINE_ARTIFACT_TYPE = "public_common_pm_latency_storage_regression_baseline"
EXPECTED_SOURCE = {
    "title": "Collection of Object-Centric Event Logs (SAP IDES O2C and P2P)",
    "doi": "10.5281/zenodo.8261133",
    "license": "CC BY 4.0",
}
EXPECTED_DATASETS = ("sap_o2c", "sap_p2p")
EXPECTED_FIXTURE_FIELDS = {
    "name",
    "baseline_dataset_id",
    "extension_dataset_id",
    "object_type",
    "from_time",
    "train_to",
    "test_from",
    "to_time",
    "source_activity",
    "target_activity",
    "slow_threshold",
}
EXPECTED_WORKLOADS = (
    "dfg_conformance_95pct",
    "variant_conformance_95pct",
    "next_activity_prediction",
    "dfg_frequency_drift",
    "repeated_transition_rework",
    "edge_bottleneck_ranking",
    "edge_bottleneck_prediction",
    "edge_duration_time_series",
    "activity_profile",
)
EXPECTED_CONCURRENCY_LEVELS = ("1", "4", "8", "16")
EXPECTED_CONCURRENCY_ENGINES = ("vanilla_postgres_python", "pg_ocpm_rust")
EXPECTED_CONCURRENCY_EPOCHS = 3
MINIMUM_CONCURRENCY_SECONDS = 5.0
MINIMUM_REQUESTS_PER_WORKER = 32
MAXIMUM_CONCURRENCY_THROUGHPUT_CV = 0.15
MINIMUM_CANDIDATE_THROUGHPUT_RATIO = 10.0
MAXIMUM_CANDIDATE_CONCURRENCY_P95_MS = 15.0
EXPECTED_LATENCY_WARMUPS = 10
EXPECTED_LATENCY_EPOCHS = 3
EXPECTED_LATENCY_SAMPLES_PER_EPOCH = 30
EXPECTED_LATENCY_RUNS = EXPECTED_LATENCY_EPOCHS * EXPECTED_LATENCY_SAMPLES_PER_EPOCH
EXPECTED_RESULT_FIELDS = {
    "schema_version",
    "generated_at",
    "release",
    "source",
    "environment",
    "provenance",
    "method",
    "summary",
    "datasets",
    "storage",
    "concurrency",
    "drift_concurrency",
    "section_generated_at",
    "payload_sha256",
}
EXPECTED_DATASET_FIELDS = {"fixture", "workloads"}
EXPECTED_WORKLOAD_FIELDS = {
    "workload",
    "vanilla_postgres_python",
    "pg_ocpm_rust",
    "speedup",
    "correct",
    "first_execution_counts",
    "serial_epochs",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "result",
        nargs="?",
        default="docs/results/public-common-pm-0.8.0.json",
    )
    parser.add_argument(
        "--regression-baseline",
        "--baseline",
        dest="baseline",
        type=Path,
        default=REGRESSION_BASELINE,
        help=(
            "compact committed source and fixture contract; historical "
            "performance values are not regression gates"
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


def workload_map(result: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for dataset in result["datasets"]:
        dataset_name = dataset["fixture"]["name"]
        for workload in dataset["workloads"]:
            key = (dataset_name, workload["workload"])
            if key in rows:
                fail(f"duplicate workload row: {key}")
            rows[key] = workload
    return rows


def validate_artifact_shape(result: dict[str, Any]) -> None:
    """Reject fields outside the schema-5 producer's stable structural rows."""

    if set(result) != EXPECTED_RESULT_FIELDS:
        fail("public schema-5 top-level fields changed")
    datasets = result.get("datasets")
    if not isinstance(datasets, list):
        fail("public schema-5 datasets must be a list")
    for dataset in datasets:
        if not isinstance(dataset, dict) or set(dataset) != EXPECTED_DATASET_FIELDS:
            fail("public schema-5 dataset fields changed")
        workloads = dataset.get("workloads")
        if not isinstance(workloads, list):
            fail("public schema-5 workloads must be a list")
        if any(
            not isinstance(workload, dict) or set(workload) != EXPECTED_WORKLOAD_FIELDS
            for workload in workloads
        ):
            fail("public schema-5 workload fields changed")


def validate_latency_sample_counts(method: dict[str, Any]) -> None:
    expected_orders = [
        list(order) for order in itertools.permutations(EXPECTED_CONCURRENCY_ENGINES)
    ]
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
            "public release latency protocol requires 10 warmups and three "
            "epochs of 30 retained nanosecond samples per arm"
        )


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
    order_dictionary = [
        list(order) for order in itertools.permutations(EXPECTED_CONCURRENCY_ENGINES)
    ]
    epochs = row.get("serial_epochs", [])
    if len(epochs) != EXPECTED_LATENCY_EPOCHS:
        fail(f"{label}: expected three retained serial-latency epochs")

    pooled = {engine: [] for engine in EXPECTED_CONCURRENCY_ENGINES}
    epoch_p95 = {engine: [] for engine in EXPECTED_CONCURRENCY_ENGINES}
    decoded_first_counts = {engine: 0 for engine in EXPECTED_CONCURRENCY_ENGINES}
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
        if set(arms) != set(EXPECTED_CONCURRENCY_ENGINES):
            fail(f"{label}: serial epoch arm set changed")
        for engine in EXPECTED_CONCURRENCY_ENGINES:
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
    if (
        set(counts) != set(EXPECTED_CONCURRENCY_ENGINES)
        or any(type(value) is not int or value <= 0 for value in counts.values())
        or counts != decoded_first_counts
    ):
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
    }
    for engine in EXPECTED_CONCURRENCY_ENGINES:
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
        if evidence != expected:
            fail(f"{label}/{engine}: pooled metrics do not match raw samples")


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
        fail("public regression baseline contains unexpected fields")
    if baseline.get("schema_version") != 1:
        fail("unexpected public regression baseline schema version")
    if baseline.get("artifact_type") != BASELINE_ARTIFACT_TYPE:
        fail("unexpected public regression baseline artifact type")
    if baseline.get("release") != BASELINE_RELEASE:
        fail("unexpected public regression baseline release versions")
    if baseline.get("source") != EXPECTED_SOURCE:
        fail("public regression baseline source metadata changed")
    datasets = baseline.get("datasets", [])
    if (
        tuple(item.get("fixture", {}).get("name") for item in datasets)
        != EXPECTED_DATASETS
    ):
        fail("public regression baseline dataset order changed")
    for dataset in datasets:
        if set(dataset) != {"fixture", "workloads"}:
            fail("public regression baseline dataset contains unexpected fields")
        if set(dataset.get("fixture", {})) != EXPECTED_FIXTURE_FIELDS:
            fail("public regression baseline fixture contains unexpected fields")
        workloads = dataset.get("workloads", [])
        if tuple(item.get("workload") for item in workloads) != EXPECTED_WORKLOADS:
            fail("public regression baseline workload order changed")
        if any(set(workload) != {"workload"} for workload in workloads):
            fail("public regression baseline workload contains unexpected fields")


def validate_concurrency_protocol(result: dict[str, Any]) -> None:
    protocol = result["method"].get("concurrency", {})
    if (
        protocol.get("epochs_per_engine_level") != EXPECTED_CONCURRENCY_EPOCHS
        or protocol.get("minimum_epoch_seconds") != MINIMUM_CONCURRENCY_SECONDS
        or protocol.get("minimum_requests_per_worker_per_epoch")
        != MINIMUM_REQUESTS_PER_WORKER
        or "persistent PostgreSQL connection"
        not in protocol.get("connection_model", "")
        or "every worker" not in protocol.get("warmup_gate", "")
        or "every measured request" not in protocol.get("correctness_gate", "")
        or "median epoch QPS" not in protocol.get("aggregation", "")
    ):
        fail("public concurrency protocol is not the stable three-epoch contract")


def validate_concurrency_section(section_name: str, section: dict[str, Any]) -> None:
    expected_workload = (
        "dfg_frequency_drift"
        if section_name == "drift_concurrency"
        else "dfg_conformance_95pct"
    )
    if (
        section.get("fixture") != "sap_o2c"
        or section.get("workload") != expected_workload
    ):
        fail(f"{section_name}: fixture or workload changed")
    if tuple(section.get("levels", [])) != EXPECTED_CONCURRENCY_LEVELS:
        fail(f"{section_name}: concurrency levels changed")
    orders = section.get("epoch_arm_orders", {})
    if tuple(orders) != EXPECTED_CONCURRENCY_LEVELS:
        fail(f"{section_name}: epoch arm-order levels changed")
    drift_offset = int(section_name == "drift_concurrency")
    hashes = set()
    for level_index, level in enumerate(EXPECTED_CONCURRENCY_LEVELS):
        level_orders = orders[level]
        if len(level_orders) != EXPECTED_CONCURRENCY_EPOCHS:
            fail(f"{section_name}/x{level}: expected three epoch arm orders")
        for epoch_index, order in enumerate(level_orders):
            offset = (level_index + epoch_index + drift_offset) % len(
                EXPECTED_CONCURRENCY_ENGINES
            )
            expected_order = (
                EXPECTED_CONCURRENCY_ENGINES[offset:]
                + EXPECTED_CONCURRENCY_ENGINES[:offset]
            )
            if tuple(order) != expected_order:
                fail(f"{section_name}/x{level}: arm rotation changed")

        workers = int(level)
        for engine in EXPECTED_CONCURRENCY_ENGINES:
            if tuple(section.get(engine, {})) != EXPECTED_CONCURRENCY_LEVELS:
                fail(f"{section_name}/{engine}: concurrency levels changed")
            aggregate = section[engine][level]
            epochs = aggregate.get("epochs", [])
            if (
                aggregate.get("workers") != workers
                or aggregate.get("epoch_count") != EXPECTED_CONCURRENCY_EPOCHS
                or len(epochs) != EXPECTED_CONCURRENCY_EPOCHS
                or aggregate.get("correct") is not True
            ):
                fail(f"{section_name}/{engine}/x{level}: invalid epoch aggregate")
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
                ):
                    fail(
                        f"{section_name}/{engine}/x{level}: invalid epoch "
                        f"{epoch_index + 1}"
                    )
                expected_qps = epoch["requests"] * 1000 / epoch["wall_ms"]
                if not math.isclose(
                    epoch.get("throughput_qps", 0), expected_qps, abs_tol=0.01
                ):
                    fail(f"{section_name}/{engine}/x{level}: inconsistent epoch QPS")
                if not (
                    0
                    <= epoch.get("minimum_ms", -1)
                    <= epoch.get("p50_ms", -1)
                    <= epoch.get("p95_ms", -1)
                    <= epoch.get("p99_ms", -1)
                    <= epoch.get("maximum_ms", -1)
                ):
                    fail(f"{section_name}/{engine}/x{level}: invalid epoch latency")
                hashes.add(epoch.get("answer_sha256"))
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
                fail(f"{section_name}/{engine}/x{level}: aggregate is not epoch median")
            if aggregate.get("requests") != sum(epoch["requests"] for epoch in epochs):
                fail(
                    f"{section_name}/{engine}/x{level}: aggregate request count changed"
                )
            qps = [epoch["throughput_qps"] for epoch in epochs]
            if statistics.pstdev(qps) / statistics.fmean(qps) > (
                MAXIMUM_CONCURRENCY_THROUGHPUT_CV
            ):
                fail(f"{section_name}/{engine}/x{level}: unstable epoch throughput")
        baseline = section["vanilla_postgres_python"][level]
        candidate = section["pg_ocpm_rust"][level]
        if candidate["throughput_qps"] < (
            baseline["throughput_qps"] * MINIMUM_CANDIDATE_THROUGHPUT_RATIO
        ):
            fail(f"{section_name}/x{level}: candidate throughput ratio fell below 10x")
        if candidate["p95_ms"] > MAXIMUM_CANDIDATE_CONCURRENCY_P95_MS:
            fail(f"{section_name}/x{level}: candidate p95 exceeded 15 ms")
    if None in hashes or len(hashes) != 1:
        fail(f"{section_name}: exact answer hashes differ across arms or epochs")


def validate_contract(
    result: dict[str, Any], baseline: dict[str, Any], *, allow_dirty: bool
) -> dict[tuple[str, str], dict[str, Any]]:
    validate_artifact_shape(result)
    if result.get("schema_version") != PUBLIC_BENCHMARK_SCHEMA_VERSION:
        fail("unexpected public benchmark schema version")
    if result.get("release") != CURRENT_RELEASE:
        fail("unexpected public benchmark release versions")
    section_times = result.get("section_generated_at", {})
    if (
        set(section_times) != {"latency_and_storage", "concurrency"}
        or section_times.get("concurrency") != result.get("generated_at")
        or not isinstance(section_times.get("latency_and_storage"), str)
    ):
        fail("public section-level generation provenance is invalid")
    if result.get("source") != EXPECTED_SOURCE:
        fail("public dataset source metadata changed")
    if result.get("source") != baseline.get("source"):
        fail("public dataset source differs from the prior committed fixture")
    try:
        validate_recorded_public_provenance(
            result.get("provenance"), allow_dirty=allow_dirty
        )
    except ValueError as error:
        fail(str(error))
    validate_latency_sample_counts(result.get("method", {}))
    validate_concurrency_protocol(result)

    datasets = result.get("datasets", [])
    baseline_datasets = {item["fixture"]["name"]: item for item in baseline["datasets"]}
    if tuple(item["fixture"]["name"] for item in datasets) != EXPECTED_DATASETS:
        fail("expected SAP O2C and P2P datasets in stable order")
    for dataset in datasets:
        name = dataset["fixture"]["name"]
        if dataset["fixture"] != baseline_datasets[name]["fixture"]:
            fail(f"{name}: fixture partition or filter settings changed")
        names = tuple(item["workload"] for item in dataset["workloads"])
        if names != EXPECTED_WORKLOADS:
            fail(f"{name}: workload set or ordering changed")
        for row in dataset["workloads"]:
            if row.get("correct") is not True:
                fail(f"{name}/{row['workload']}: correctness gate failed")
            validate_serial_latency_row(name, row["workload"], row)
            for engine in ("vanilla_postgres_python", "pg_ocpm_rust"):
                metrics = row[engine]
                if metrics.get("runs") != result["method"]["measured_runs"]:
                    fail(f"{name}/{row['workload']}/{engine}: run count changed")
                if metrics.get("exact_samples") != result["method"]["measured_runs"]:
                    fail(
                        f"{name}/{row['workload']}/{engine}: not every timed "
                        "sample passed exactness"
                    )
                minimum = metrics.get("minimum_ms", 0)
                p50 = metrics.get("p50_ms", 0)
                p95 = metrics.get("p95_ms", 0)
                if not (0 < minimum <= p50 <= p95):
                    fail(f"{name}/{row['workload']}/{engine}: invalid latency metrics")
            expected_speedup = round(
                row["vanilla_postgres_python"]["p50_ms"]
                / row["pg_ocpm_rust"]["p50_ms"],
                3,
            )
            if not math.isclose(row["speedup"], expected_speedup, abs_tol=0.001):
                fail(f"{name}/{row['workload']}: inconsistent speedup")

    rows = workload_map(result)
    summary = result["summary"]
    if len(rows) != len(EXPECTED_DATASETS) * len(EXPECTED_WORKLOADS):
        fail("unexpected public workload count")
    if summary["correct_workloads"] != len(rows):
        fail("not every public workload passed its correctness gate")
    if summary["total_workloads"] != len(rows):
        fail("summary workload count does not match result rows")
    speedups = [row["speedup"] for row in rows.values()]
    minimum = min(speedups)
    geometric = math.exp(sum(math.log(value) for value in speedups) / len(speedups))
    if not math.isclose(summary["minimum_speedup"], minimum, abs_tol=0.001):
        fail("summary minimum speedup is inconsistent")
    if not math.isclose(summary["geometric_mean_speedup"], geometric, abs_tol=0.001):
        fail("summary geometric-mean speedup is inconsistent")
    if minimum < 10.0 or geometric < 10.0:
        fail("10x gate failed: every workload and geometric mean must pass")
    if summary.get("target_speedup") != 10.0 or summary.get("target_met") is not True:
        fail("public 10x target metadata is inconsistent")
    validate_concurrency_section("concurrency", result["concurrency"])
    validate_concurrency_section("drift_concurrency", result["drift_concurrency"])
    return rows


def validate_regressions(
    result: dict[str, Any],
    bridge: dict[str, Any],
    *,
    allow_dirty: bool,
) -> None:
    release_regression_checker.validate_for_public_common(
        result,
        bridge,
        allow_dirty=allow_dirty,
    )


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
    rows = validate_contract(result, baseline, allow_dirty=args.preview)
    bridge_checked = False
    if args.release_bridge is not None:
        bridge_digest = (
            None if args.preview else args.expected_release_bridge_sha256
        )
        if not args.preview and bridge_digest is None:
            fail("an explicitly supplied release bridge requires a pinned digest")
        bridge = release_regression_checker.load_verified(
            args.release_bridge,
            bridge_digest,
        )
        validate_regressions(result, bridge, allow_dirty=args.preview)
        bridge_checked = True
    minimum = min(row["speedup"] for row in rows.values())
    suffix = (
        "; private same-host non-regression gates passed"
        if bridge_checked
        else ""
    )
    print(
        f"public benchmark verified: {len(rows)} exact workloads, "
        f"minimum {minimum:.3f}x versus vanilla PostgreSQL{suffix}"
    )


if __name__ == "__main__":
    main()
