"""Matched SAP release regression benchmark for pg_ocpm plus ocpm-engine.

The benchmark compares two locked product releases on the same host with one
shared, current controller.  It covers the native common-PM kernels and the
native arm of the PM4Py comparison using identical fixtures, timing scopes,
warmups, samples, fresh-process memory measurements, and persistent-worker
concurrency epochs.  Vanilla PostgreSQL plus the independent Python/PM4Py path
is used only as an untimed correctness oracle.
"""

from __future__ import annotations

import argparse
import base64
import math
import os
import platform
import random
import statistics
import struct
import sys
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

try:
    import sap_pm4py_three_way as pm4py_benchmark
    import sap_release_bridge as bridge
except ModuleNotFoundError:  # imported as benchmarks.sap_release_regression
    from benchmarks import sap_pm4py_three_way as pm4py_benchmark
    from benchmarks import sap_release_bridge as bridge


SCHEMA_VERSION = 2
ARTIFACT_TYPE = "sap_release_bridge"
SUITES = ("common_pm", "pm4py")
COMMON_WORKLOADS = bridge.WORKLOAD_NAMES
PM4PY_WORKLOADS = tuple(pm4py_benchmark.WORKLOADS)
WORKLOADS = {
    "common_pm": COMMON_WORKLOADS,
    "pm4py": PM4PY_WORKLOADS,
}
EXECUTION_PATHS = {
    "common_pm": ("pg_ocpm_rust",),
    "pm4py": ("pg_ocpm_pm4py", "pg_ocpm_ocpm_engine"),
}
EXPECTED_DATASETS = bridge.EXPECTED_DATASETS

WARMUP_ROUNDS = bridge.WARMUP_ROUNDS
LATENCY_EPOCHS = bridge.LATENCY_EPOCHS
SAMPLES_PER_EPOCH = bridge.SAMPLES_PER_EPOCH
MEMORY_SAMPLES_PER_ARM = 4
CONCURRENCY_SPECS = {
    "common_pm": {
        "workloads": ("dfg_conformance_95pct", "dfg_frequency_drift"),
        "levels": (1, 4, 8, 16),
    },
    "pm4py": {
        "workloads": ("dfg_conformance_95pct",),
        "levels": (1, 2, 4, 8),
    },
}
CONCURRENCY_EPOCHS = 4
CONCURRENCY_MIN_SECONDS = 5.0
CONCURRENCY_MIN_REQUESTS_PER_WORKER = 32
RANDOM_SEED = bridge.RANDOM_SEED
ORDER_DICTIONARY = bridge.ORDER_DICTIONARY
LATENCY_CEILING = 1.10
LATENCY_ABSOLUTE_SLACK_NS = 100_000
MEMORY_CEILING = 1.10
MEMORY_ABSOLUTE_SLACK_BYTES = 64 * 1024
CONCURRENCY_CEILING = 1.10
CONCURRENCY_ABSOLUTE_P95_SLACK_MS = 0.10
MAXIMUM_CONCURRENCY_THROUGHPUT_CV = 0.15
STORAGE_CEILING = 1.01
MAX_ENGINE_INCREMENTAL_PEAK_BYTES = 8 * 1024 * 1024
MAX_ENGINE_PEAK_RSS_BYTES = 64 * 1024 * 1024
MAX_PM4PY_PEAK_RSS_BYTES = 256 * 1024 * 1024
SAMPLE_ENCODING = "u64le+zlib+base64-v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle-host", default="postgres_vanilla")
    parser.add_argument("--prior-host", default="postgres_prior")
    parser.add_argument("--current-host", default="postgres_current")
    parser.add_argument("--database")
    parser.add_argument("--oracle-db", default="ocel_benchmark")
    parser.add_argument("--prior-db", default="ocel_benchmark")
    parser.add_argument("--current-db", default="ocel_benchmark")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument(
        "--controller-engine-path",
        default=os.environ.get("OCPM_CURRENT_ENGINE_PATH", "/opt/engine-current"),
    )
    parser.add_argument(
        "--prior-engine-path",
        default=os.environ.get("OCPM_PRIOR_ENGINE_PATH", "/opt/engine-prior"),
    )
    parser.add_argument(
        "--current-engine-path",
        default=os.environ.get("OCPM_CURRENT_ENGINE_PATH", "/opt/engine-current"),
    )
    parser.add_argument("--worker-python", default=sys.executable)
    parser.add_argument(
        "--output",
        default=("/results/sap-release-bridge-0.4.0-to-0.6.0.json"),
    )
    args = parser.parse_args()
    if args.database:
        args.oracle_db = args.database
        args.prior_db = args.database
        args.current_db = args.database
    if args.timeout_seconds < 1:
        parser.error("--timeout-seconds must be positive")
    return args


def _fixture_payload(suite: str, fixture: Any) -> dict[str, Any]:
    if suite == "common_pm":
        return bridge.fixture_payload(fixture)
    if suite == "pm4py":
        return pm4py_benchmark.serialize_fixture(fixture)
    raise ValueError(f"unsupported suite: {suite}")


def _logical_pm4py_fixture(prior: Any, current: Any) -> dict[str, Any]:
    prior_payload = pm4py_benchmark.serialize_fixture(prior)
    current_payload = pm4py_benchmark.serialize_fixture(current)
    if prior_payload != current_payload:
        raise RuntimeError(f"PM4Py fixture mismatch for {prior.dataset_name}")
    return prior_payload


def discover_pm4py_fixtures(
    oracle: Any,
    prior_database: Any,
    current_database: Any,
) -> list[tuple[Any, Any]]:
    fixture_args = SimpleNamespace(train_fraction=0.8)
    pairs = []
    for dataset_name in EXPECTED_DATASETS:
        prior = pm4py_benchmark.discover_fixture(
            prior_database.connection,
            oracle.connection,
            fixture_args,
            dataset_name,
        )
        current = pm4py_benchmark.discover_fixture(
            current_database.connection,
            oracle.connection,
            fixture_args,
            dataset_name,
        )
        _logical_pm4py_fixture(prior, current)
        pairs.append((prior, current))
    return pairs


def oracle_answer(
    suite: str,
    common_pm: Any,
    oracle: Any,
    fixture: Any,
    workload: str,
) -> Any:
    if suite == "common_pm":
        return bridge.oracle_workload(common_pm, oracle, fixture, workload)
    if suite == "pm4py":
        return pm4py_benchmark.run_pm4py(
            oracle.connection,
            fixture,
            workload,
            "vanilla_pg",
        )["answer"]
    raise ValueError(f"unsupported suite: {suite}")


def execute(
    worker: bridge.BridgeWorker,
    suite: str,
    execution_path: str,
    workload: str,
    fixture: dict[str, Any],
    *,
    timed: bool,
) -> dict[str, Any]:
    result = worker.request(
        "run",
        suite=suite,
        execution_path=execution_path,
        workload=workload,
        fixture=fixture,
        timed=timed,
    )
    if not isinstance(result, dict):
        raise RuntimeError(f"{worker.arm} returned a non-object run result")
    if result.get("execution_path") != execution_path:
        raise RuntimeError(f"{worker.arm} returned the wrong execution path")
    if not isinstance(result.get("input"), dict):
        raise RuntimeError(f"{worker.arm} returned invalid input evidence")
    return result


def require_exact(
    arm: str,
    suite: str,
    workload: str,
    phase: str,
    result: dict[str, Any],
    oracle_sha256: str,
) -> None:
    if result.get("answer_sha256") != oracle_sha256:
        raise AssertionError(
            f"{arm} {suite}/{workload} answer mismatch during {phase}: "
            f"{result.get('answer_sha256')} != {oracle_sha256}"
        )


def benchmark_latency(
    *,
    suite: str,
    execution_path: str,
    common_pm: Any,
    oracle: Any,
    oracle_fixture: Any,
    arm_fixtures: dict[str, Any],
    workers: dict[str, bridge.BridgeWorker],
    workload: str,
    rng: random.Random,
) -> dict[str, Any]:
    answer = oracle_answer(suite, common_pm, oracle, oracle_fixture, workload)
    answer_sha256 = bridge.canonical_sha256(answer)
    payloads = {
        arm: _fixture_payload(suite, fixture) for arm, fixture in arm_fixtures.items()
    }

    for arm in ("prior", "current"):
        result = execute(
            workers[arm],
            suite,
            execution_path,
            workload,
            payloads[arm],
            timed=False,
        )
        require_exact(arm, suite, workload, "preflight", result, answer_sha256)
        if result.get("elapsed_ns") is not None:
            raise RuntimeError(f"{arm} preflight unexpectedly reported latency")

    warmup_order_codes = bridge.balanced_order_codes(WARMUP_ROUNDS // 2, rng)
    for round_index, order_code in enumerate(warmup_order_codes, start=1):
        for arm in ORDER_DICTIONARY[order_code]:
            result = execute(
                workers[arm],
                suite,
                execution_path,
                workload,
                payloads[arm],
                timed=False,
            )
            require_exact(
                arm,
                suite,
                workload,
                f"warmup {round_index}",
                result,
                answer_sha256,
            )

    serial_epochs = []
    first_execution_counts = {"prior": 0, "current": 0}
    for epoch_index in range(LATENCY_EPOCHS):
        order_codes = bridge.balanced_order_codes(SAMPLES_PER_EPOCH // 2, rng)
        samples = {"prior": [], "current": []}
        hashes = {"prior": [], "current": []}
        for round_index, order_code in enumerate(order_codes, start=1):
            order = ORDER_DICTIONARY[order_code]
            first_execution_counts[order[0]] += 1
            for arm in order:
                result = execute(
                    workers[arm],
                    suite,
                    execution_path,
                    workload,
                    payloads[arm],
                    timed=True,
                )
                require_exact(
                    arm,
                    suite,
                    workload,
                    f"epoch {epoch_index + 1} round {round_index}",
                    result,
                    answer_sha256,
                )
                elapsed_ns = result.get("elapsed_ns")
                if type(elapsed_ns) is not int or elapsed_ns <= 0:
                    raise RuntimeError(
                        f"{arm} returned invalid elapsed_ns: {elapsed_ns!r}"
                    )
                samples[arm].append(elapsed_ns)
                hashes[arm].append(result["answer_sha256"])
        serial_epochs.append(
            {
                "epoch": epoch_index + 1,
                "order_codes": order_codes,
                "arms": {
                    arm: {
                        **bridge.serial_metrics(samples[arm]),
                        "samples_ns": samples[arm],
                        "answer_sha256s": hashes[arm],
                    }
                    for arm in ("prior", "current")
                },
            }
        )

    prior = bridge.aggregate_arm(serial_epochs, "prior")
    current = bridge.aggregate_arm(serial_epochs, "current")
    prior_samples = [
        sample
        for epoch in serial_epochs
        for sample in epoch["arms"]["prior"]["samples_ns"]
    ]
    current_samples = [
        sample
        for epoch in serial_epochs
        for sample in epoch["arms"]["current"]["samples_ns"]
    ]
    prior_median = statistics.median(prior_samples)
    current_median = statistics.median(current_samples)
    non_regressed = current_median <= max(
        prior_median * LATENCY_CEILING,
        prior_median + LATENCY_ABSOLUTE_SLACK_NS,
    )
    input_evidence = {
        arm: execute(
            workers[arm],
            suite,
            execution_path,
            workload,
            payloads[arm],
            timed=False,
        ).get("input", {})
        for arm in ("prior", "current")
    }
    if bridge.canonical(input_evidence["prior"]) != bridge.canonical(
        input_evidence["current"]
    ):
        raise AssertionError(
            f"{suite}/{execution_path}/{workload} input evidence differs by release"
        )
    return {
        "workload": workload,
        "execution_path": execution_path,
        "oracle_answer_sha256": answer_sha256,
        "input_evidence": input_evidence,
        "correct": True,
        "prior": prior,
        "current": current,
        "p50_ratio_prior_over_current": round(prior_median / current_median, 3),
        "non_regressed": non_regressed,
        "first_execution_counts": first_execution_counts,
        "warmup_order_codes": warmup_order_codes,
        "serial_epochs": serial_epochs,
    }


def worker_factory(
    args: argparse.Namespace,
    worker_path: Path,
    arm: str,
    suite: str,
):
    values = {
        "prior": {
            "host": args.prior_host,
            "database": args.prior_db,
            "engine_path": args.prior_engine_path,
            "engine_version": bridge.PRIOR_ENGINE_VERSION,
            "pg_ocpm_version": bridge.PRIOR_PG_OCPM_VERSION,
        },
        "current": {
            "host": args.current_host,
            "database": args.current_db,
            "engine_path": args.current_engine_path,
            "engine_version": bridge.CURRENT_ENGINE_VERSION,
            "pg_ocpm_version": bridge.CURRENT_PG_OCPM_VERSION,
        },
    }[arm]
    return bridge.BridgeWorker(
        arm=arm,
        timeout_seconds=args.timeout_seconds,
        worker_python=args.worker_python,
        worker_path=worker_path,
        suite=suite,
        **values,
    )


def _memory_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "samples": samples,
        "exact_samples": len(samples),
        "median_baseline_rss_bytes": int(
            statistics.median(item["baseline_rss_bytes"] for item in samples)
        ),
        "median_peak_rss_bytes": int(
            statistics.median(item["peak_rss_bytes"] for item in samples)
        ),
        "maximum_peak_rss_bytes": max(item["peak_rss_bytes"] for item in samples),
        "median_incremental_peak_bytes": int(
            statistics.median(item["incremental_peak_bytes"] for item in samples)
        ),
        "maximum_incremental_peak_bytes": max(
            item["incremental_peak_bytes"] for item in samples
        ),
    }


def benchmark_memory(
    *,
    args: argparse.Namespace,
    worker_path: Path,
    suite: str,
    execution_path: str,
    workload: str,
    fixtures: dict[str, Any],
    oracle_sha256: str,
) -> dict[str, Any]:
    samples: dict[str, list[dict[str, Any]]] = {"prior": [], "current": []}
    order_codes = []
    for sample_index in range(MEMORY_SAMPLES_PER_ARM):
        order_code = sample_index % 2
        order_codes.append(order_code)
        fresh = {
            arm: worker_factory(args, worker_path, arm, suite)
            for arm in ("prior", "current")
        }
        try:
            for position, arm in enumerate(ORDER_DICTIONARY[order_code], start=1):
                result = fresh[arm].request(
                    "memory_run",
                    suite=suite,
                    execution_path=execution_path,
                    workload=workload,
                    fixture=_fixture_payload(suite, fixtures[arm]),
                )
                if not isinstance(result, dict):
                    raise RuntimeError(f"{arm} returned invalid memory evidence")
                if result.get("execution_path") != execution_path:
                    raise RuntimeError(f"{arm} memory worker used the wrong path")
                if not isinstance(result.get("input"), dict):
                    raise RuntimeError(f"{arm} returned invalid memory input evidence")
                require_exact(
                    arm,
                    suite,
                    workload,
                    f"memory sample {sample_index + 1}",
                    result,
                    oracle_sha256,
                )
                sample = {
                    "sample": sample_index + 1,
                    "arm_position": position,
                    **result,
                }
                samples[arm].append(sample)
        finally:
            for worker in fresh.values():
                worker.close()
    for sample_index in range(MEMORY_SAMPLES_PER_ARM):
        if bridge.canonical(
            samples["prior"][sample_index]["input"]
        ) != bridge.canonical(samples["current"][sample_index]["input"]):
            raise AssertionError(
                f"{suite}/{execution_path}/{workload} memory input differs by release"
            )
    worker_pids = [
        sample["worker_pid"] for arm in ("prior", "current") for sample in samples[arm]
    ]
    if any(type(pid) is not int or pid <= 0 for pid in worker_pids) or len(
        set(worker_pids)
    ) != len(worker_pids):
        raise RuntimeError("memory samples did not use unique fresh worker processes")
    prior = _memory_metrics(samples["prior"])
    current = _memory_metrics(samples["current"])

    def allowed(prior_value: int) -> int:
        return max(
            math.ceil(prior_value * MEMORY_CEILING),
            prior_value + MEMORY_ABSOLUTE_SLACK_BYTES,
        )

    compared_metrics = (
        "median_peak_rss_bytes",
        "maximum_peak_rss_bytes",
        "median_incremental_peak_bytes",
        "maximum_incremental_peak_bytes",
    )
    relative_non_regressed = all(
        current[metric] <= allowed(prior[metric]) for metric in compared_metrics
    )
    absolute_non_regressed = current["maximum_peak_rss_bytes"] <= (
        MAX_PM4PY_PEAK_RSS_BYTES
        if execution_path == "pg_ocpm_pm4py"
        else MAX_ENGINE_PEAK_RSS_BYTES
    ) and (
        execution_path == "pg_ocpm_pm4py"
        or current["maximum_incremental_peak_bytes"]
        <= MAX_ENGINE_INCREMENTAL_PEAK_BYTES
    )
    return {
        "workload": workload,
        "execution_path": execution_path,
        "oracle_answer_sha256": oracle_sha256,
        "order_codes": order_codes,
        "prior": prior,
        "current": current,
        "relative_non_regressed": relative_non_regressed,
        "absolute_bound_met": absolute_non_regressed,
        "non_regressed": relative_non_regressed and absolute_non_regressed,
    }


def _percentile_ns(samples: list[int], percentile: float) -> int:
    return int(bridge.percentile(samples, percentile))


def _concurrency_metrics_ns(samples: list[int]) -> dict[str, Any]:
    metrics = bridge.serial_metrics(samples)
    return {
        **metrics,
        "p99_ms": round(_percentile_ns(samples, 0.99) / 1_000_000, 3),
    }


def encode_u64_samples(samples: list[int]) -> dict[str, Any]:
    if not samples or any(type(sample) is not int or sample <= 0 for sample in samples):
        raise ValueError("concurrency samples must be positive integers")
    packed = struct.pack(f"<{len(samples)}Q", *samples)
    compressed = zlib.compress(packed, level=9)
    return {
        "encoding": SAMPLE_ENCODING,
        "count": len(samples),
        "data": base64.b64encode(compressed).decode("ascii"),
    }


def run_concurrency_epoch(
    *,
    pool: list[bridge.BridgeWorker],
    suite: str,
    execution_path: str,
    workload: str,
    fixture: dict[str, Any],
    oracle_sha256: str,
) -> dict[str, Any]:
    ready = threading.Barrier(len(pool) + 1)
    start = threading.Event()
    deadline = {"value": 0.0}

    def run_worker(worker: bridge.BridgeWorker) -> dict[str, Any]:
        warm = execute(
            worker,
            suite,
            execution_path,
            workload,
            fixture,
            timed=False,
        )
        require_exact(
            worker.arm, suite, workload, "concurrency warmup", warm, oracle_sha256
        )
        ready.wait(timeout=30)
        if not start.wait(timeout=30):
            raise RuntimeError("concurrency start signal timed out")
        roundtrip_samples = []
        internal_samples = []
        answer_sha256_counts: dict[str, int] = {}
        while (
            len(roundtrip_samples) < CONCURRENCY_MIN_REQUESTS_PER_WORKER
            or time.perf_counter() < deadline["value"]
        ):
            started_ns = time.perf_counter_ns()
            result = execute(
                worker,
                suite,
                execution_path,
                workload,
                fixture,
                timed=True,
            )
            roundtrip_ns = time.perf_counter_ns() - started_ns
            require_exact(
                worker.arm,
                suite,
                workload,
                "concurrency measured request",
                result,
                oracle_sha256,
            )
            internal_ns = result.get("elapsed_ns")
            if type(internal_ns) is not int or internal_ns <= 0:
                raise RuntimeError(
                    "concurrency worker returned invalid internal timing"
                )
            roundtrip_samples.append(roundtrip_ns)
            internal_samples.append(internal_ns)
            answer_sha256 = result["answer_sha256"]
            answer_sha256_counts[answer_sha256] = (
                answer_sha256_counts.get(answer_sha256, 0) + 1
            )
        return {
            "worker_pid": worker.process.pid,
            "roundtrip_samples_ns": roundtrip_samples,
            "internal_samples_ns": internal_samples,
            "answer_sha256_counts": dict(sorted(answer_sha256_counts.items())),
        }

    with ThreadPoolExecutor(max_workers=len(pool)) as executor:
        futures = [executor.submit(run_worker, worker) for worker in pool]
        ready.wait(timeout=30)
        started_ns = time.perf_counter_ns()
        deadline["value"] = time.perf_counter() + CONCURRENCY_MIN_SECONDS
        start.set()
        worker_results = [future.result() for future in futures]
        wall_ns = time.perf_counter_ns() - started_ns

    worker_pids = [item["worker_pid"] for item in worker_results]
    if len(set(worker_pids)) != len(pool):
        raise RuntimeError("concurrency pool did not use one process per worker")
    roundtrip = [
        value for item in worker_results for value in item["roundtrip_samples_ns"]
    ]
    internal = [
        value for item in worker_results for value in item["internal_samples_ns"]
    ]
    request_counts = [len(item["roundtrip_samples_ns"]) for item in worker_results]
    if min(request_counts) < CONCURRENCY_MIN_REQUESTS_PER_WORKER:
        raise RuntimeError("concurrency request floor was not met")
    if wall_ns < int(CONCURRENCY_MIN_SECONDS * 1_000_000_000):
        raise RuntimeError("concurrency duration floor was not met")
    encoded_workers = [
        {
            "worker_pid": item["worker_pid"],
            "roundtrip_samples_ns": encode_u64_samples(item["roundtrip_samples_ns"]),
            "internal_samples_ns": encode_u64_samples(item["internal_samples_ns"]),
            "answer_sha256_counts": item["answer_sha256_counts"],
        }
        for item in worker_results
    ]
    return {
        "requests": len(roundtrip),
        "wall_ns": wall_ns,
        "throughput_qps": round(len(roundtrip) / (wall_ns / 1_000_000_000), 3),
        "roundtrip": _concurrency_metrics_ns(roundtrip),
        "worker_internal": _concurrency_metrics_ns(internal),
        "worker_count": len(pool),
        "worker_pids": sorted(worker_pids),
        "worker_request_counts": sorted(request_counts),
        "workers": encoded_workers,
        "answer_sha256": oracle_sha256,
        "correct": True,
    }


def _aggregate_concurrency(epochs: list[dict[str, Any]]) -> dict[str, Any]:
    throughput_values = [epoch["throughput_qps"] for epoch in epochs]
    throughput_mean = statistics.mean(throughput_values)
    return {
        "epoch_count": len(epochs),
        "requests": sum(epoch["requests"] for epoch in epochs),
        "throughput_qps": round(statistics.median(throughput_values), 3),
        "throughput_cv": round(
            (
                statistics.pstdev(throughput_values) / throughput_mean
                if throughput_mean and len(throughput_values) > 1
                else 0.0
            ),
            6,
        ),
        "p50_ms": round(
            statistics.median(epoch["roundtrip"]["p50_ms"] for epoch in epochs),
            3,
        ),
        "p95_ms": round(
            statistics.median(epoch["roundtrip"]["p95_ms"] for epoch in epochs),
            3,
        ),
        "p99_ms": round(
            statistics.median(epoch["roundtrip"]["p99_ms"] for epoch in epochs),
            3,
        ),
        "minimum_requests_per_worker": min(
            min(epoch["worker_request_counts"]) for epoch in epochs
        ),
        "correct": all(epoch["correct"] for epoch in epochs),
        "epochs": epochs,
    }


def benchmark_concurrency(
    *,
    args: argparse.Namespace,
    worker_path: Path,
    suite: str,
    execution_path: str,
    workload: str,
    fixture: dict[str, Any],
    oracle_sha256: str,
    schedule_offset: int,
) -> dict[str, Any]:
    levels: dict[str, Any] = {}
    for level_index, worker_count in enumerate(CONCURRENCY_SPECS[suite]["levels"]):
        pools = {
            arm: [
                worker_factory(args, worker_path, arm, suite)
                for _ in range(worker_count)
            ]
            for arm in ("prior", "current")
        }
        epochs = {"prior": [], "current": []}
        epoch_arm_orders = []
        try:
            for epoch_index in range(CONCURRENCY_EPOCHS):
                order_code = (schedule_offset + level_index + epoch_index) % 2
                order = ORDER_DICTIONARY[order_code]
                epoch_arm_orders.append(list(order))
                for position, arm in enumerate(order, start=1):
                    epoch = run_concurrency_epoch(
                        pool=pools[arm],
                        suite=suite,
                        execution_path=execution_path,
                        workload=workload,
                        fixture=fixture[arm],
                        oracle_sha256=oracle_sha256,
                    )
                    epoch.update(
                        {
                            "epoch": epoch_index + 1,
                            "arm_position": position,
                        }
                    )
                    epochs[arm].append(epoch)
        finally:
            for pool in pools.values():
                for worker in pool:
                    worker.close()
        prior = _aggregate_concurrency(epochs["prior"])
        current = _aggregate_concurrency(epochs["current"])
        qps_non_regressed = (
            current["throughput_qps"] >= prior["throughput_qps"] / CONCURRENCY_CEILING
        )
        p95_non_regressed = current["p95_ms"] <= max(
            prior["p95_ms"] * CONCURRENCY_CEILING,
            prior["p95_ms"] + CONCURRENCY_ABSOLUTE_P95_SLACK_MS,
        )
        stable = (
            prior["throughput_cv"] <= MAXIMUM_CONCURRENCY_THROUGHPUT_CV
            and current["throughput_cv"] <= MAXIMUM_CONCURRENCY_THROUGHPUT_CV
        )
        levels[str(worker_count)] = {
            "epoch_arm_orders": epoch_arm_orders,
            "prior": prior,
            "current": current,
            "qps_ratio_current_over_prior": round(
                current["throughput_qps"] / prior["throughput_qps"], 3
            ),
            "qps_non_regressed": qps_non_regressed,
            "p95_non_regressed": p95_non_regressed,
            "stable": stable,
            "non_regressed": qps_non_regressed and p95_non_regressed and stable,
        }
    return {
        "workload": workload,
        "execution_path": execution_path,
        "oracle_answer_sha256": oracle_sha256,
        "levels": levels,
    }


def capture_provenance(
    controller_path: Path,
    worker_path: Path,
    common_pm_path: Path,
    pm4py_path: Path,
) -> dict[str, Any]:
    provenance = bridge.capture_provenance(
        controller_path,
        worker_path,
        common_pm_path,
    )
    common_digest = provenance["harness_sha256"].pop("workload")
    provenance["harness_sha256"].update(
        {
            "support": bridge.file_sha256(Path(bridge.__file__).resolve()),
            "common_pm": common_digest,
            "pm4py": bridge.file_sha256(pm4py_path),
            "requirements_lock": bridge.file_sha256(
                controller_path.parent / "public" / "requirements.lock"
            ),
        }
    )
    provenance["postgres_base_image"] = bridge.required_environment(
        "OCPM_POSTGRES_BASE_IMAGE"
    )
    return provenance


def validate_worker_identity(
    identity: dict[str, Any],
    provenance: dict[str, Any],
    arm: str,
    suite: str,
) -> None:
    if identity.get("suite") != suite:
        raise RuntimeError(f"{arm} worker loaded the wrong suite")
    expected = provenance["harness_sha256"][suite]
    if identity.get("workload_sha256") != expected:
        raise RuntimeError(f"{arm} worker loaded a different {suite} harness")


def _vacuum_and_storage(worker: bridge.BridgeWorker) -> dict[str, Any]:
    vacuum = worker.request("vacuum_analyze")
    if not isinstance(vacuum, dict) or vacuum.get("completed") is not True:
        raise RuntimeError(f"{worker.arm} VACUUM (ANALYZE) did not complete")
    storage = worker.request("storage_details")
    if not isinstance(storage, dict):
        raise RuntimeError(f"{worker.arm} returned invalid detailed storage")
    return storage


def host_fingerprint() -> dict[str, Any]:
    return {
        "benchmark_host_id": bridge.required_image_id("OCPM_BENCHMARK_HOST_ID"),
        "client_image_id": bridge.required_image_id("OCPM_CLIENT_IMAGE_ID"),
        "vanilla_database_image_id": bridge.required_image_id(
            "OCPM_VANILLA_DATABASE_IMAGE_ID"
        ),
        "prior_database_image_id": bridge.required_image_id(
            "OCPM_PRIOR_DATABASE_IMAGE_ID"
        ),
        "current_database_image_id": bridge.required_image_id(
            "OCPM_CURRENT_DATABASE_IMAGE_ID"
        ),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpus_visible": os.cpu_count(),
    }


def database_environment(common_pm: Any, database: Any) -> dict[str, Any]:
    environment = common_pm.database_environment(database)
    environment["autovacuum"] = str(
        database.one("SELECT current_setting('autovacuum')", {})
    )
    return environment


def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    common_pm = bridge.load_common_pm(args.controller_engine_path)
    controller_path = Path(__file__).resolve()
    worker_path = controller_path.with_name("sap_release_bridge_worker.py")
    common_pm_path = Path(common_pm.__file__).resolve()
    pm4py_path = Path(pm4py_benchmark.__file__).resolve()
    provenance = capture_provenance(
        controller_path, worker_path, common_pm_path, pm4py_path
    )
    starting_host_fingerprint = host_fingerprint()

    oracle = common_pm.Database(args.oracle_host, args.oracle_db, args.timeout_seconds)
    prior_database = common_pm.Database(
        args.prior_host, args.prior_db, args.timeout_seconds
    )
    current_database = common_pm.Database(
        args.current_host, args.current_db, args.timeout_seconds
    )
    workers: dict[str, dict[str, bridge.BridgeWorker]] = {}
    try:
        fixtures = {
            "common_pm": bridge.discover_paired_fixtures(
                common_pm, oracle, prior_database, current_database
            ),
            "pm4py": discover_pm4py_fixtures(oracle, prior_database, current_database),
        }
        workers = {
            suite: {
                arm: worker_factory(args, worker_path, arm, suite)
                for arm in ("prior", "current")
            }
            for suite in SUITES
        }
        for suite, suite_workers in workers.items():
            for arm, worker in suite_workers.items():
                validate_worker_identity(worker.identity, provenance, arm, suite)
                if worker.identity["database_environment"].get("autovacuum") != "off":
                    raise RuntimeError(f"{suite}/{arm} autovacuum must be disabled")

        # Autovacuum is disabled in the bridge containers.  Stabilize both
        # arms explicitly before the structural snapshot so relation age cannot
        # masquerade as a release storage regression.
        storage_before = {
            arm: _vacuum_and_storage(workers["common_pm"][arm])
            for arm in ("prior", "current")
        }

        rng = random.Random(RANDOM_SEED)
        suites = {}
        latency_ratios = []
        latency_gates = []
        memory_gates = []
        concurrency_gates = []
        for suite_index, suite in enumerate(SUITES):
            dataset_results = []
            for dataset_index, (prior_fixture, current_fixture) in enumerate(
                fixtures[suite]
            ):
                dataset_name = (
                    prior_fixture.name
                    if suite == "common_pm"
                    else prior_fixture.dataset_name
                )
                print(f"bridging {suite} {dataset_name}", flush=True)
                fixture_map = {
                    "prior": prior_fixture,
                    "current": current_fixture,
                }
                workloads = []
                memory = []
                oracle_hashes = {}
                for execution_path in EXECUTION_PATHS[suite]:
                    for workload in WORKLOADS[suite]:
                        row = benchmark_latency(
                            suite=suite,
                            execution_path=execution_path,
                            common_pm=common_pm,
                            oracle=oracle,
                            oracle_fixture=prior_fixture,
                            arm_fixtures=fixture_map,
                            workers=workers[suite],
                            workload=workload,
                            rng=rng,
                        )
                        workloads.append(row)
                        latency_ratios.append(row["p50_ratio_prior_over_current"])
                        latency_gates.append(row["non_regressed"])
                        oracle_hashes[(execution_path, workload)] = row[
                            "oracle_answer_sha256"
                        ]
                        print(
                            f"  latency {execution_path}/{workload}: "
                            f"{row['p50_ratio_prior_over_current']:.3f}x",
                            flush=True,
                        )
                        memory_row = benchmark_memory(
                            args=args,
                            worker_path=worker_path,
                            suite=suite,
                            execution_path=execution_path,
                            workload=workload,
                            fixtures=fixture_map,
                            oracle_sha256=row["oracle_answer_sha256"],
                        )
                        memory.append(memory_row)
                        memory_gates.append(memory_row["non_regressed"])

                serialized_fixture = (
                    bridge.logical_fixture_payload(prior_fixture, current_fixture)
                    if suite == "common_pm"
                    else _logical_pm4py_fixture(prior_fixture, current_fixture)
                )
                fixture_payloads = {
                    arm: _fixture_payload(suite, fixture)
                    for arm, fixture in fixture_map.items()
                }
                concurrency = []
                for path_index, execution_path in enumerate(EXECUTION_PATHS[suite]):
                    for workload_index, workload in enumerate(
                        CONCURRENCY_SPECS[suite]["workloads"]
                    ):
                        concurrency_row = benchmark_concurrency(
                            args=args,
                            worker_path=worker_path,
                            suite=suite,
                            execution_path=execution_path,
                            workload=workload,
                            fixture=fixture_payloads,
                            oracle_sha256=oracle_hashes[(execution_path, workload)],
                            schedule_offset=(
                                suite_index
                                + dataset_index
                                + path_index
                                + workload_index
                            ),
                        )
                        concurrency.append(concurrency_row)
                        concurrency_gates.extend(
                            level["non_regressed"]
                            for level in concurrency_row["levels"].values()
                        )
                dataset_results.append(
                    {
                        "dataset": dataset_name,
                        "fixture": serialized_fixture,
                        "latency": workloads,
                        "memory": memory,
                        "concurrency": concurrency,
                    }
                )
            suites[suite] = {"datasets": dataset_results}

        storage_after = {
            arm: workers["common_pm"][arm].request("storage_details")
            for arm in ("prior", "current")
        }
        identities = {
            suite: {arm: worker.identity for arm, worker in suite_workers.items()}
            for suite, suite_workers in workers.items()
        }
        total_workloads = sum(
            len(EXPECTED_DATASETS) * len(WORKLOADS[suite]) * len(EXECUTION_PATHS[suite])
            for suite in SUITES
        )
        storage_gates = []
        for metric in ("index_bytes", "total_bytes"):
            prior_value = storage_before["prior"][metric]
            current_value = storage_before["current"][metric]
            storage_gates.append(current_value <= prior_value * STORAGE_CEILING)
        ending_host_fingerprint = host_fingerprint()
        if ending_host_fingerprint != starting_host_fingerprint:
            raise RuntimeError("host fingerprint changed during the release bridge")
        database_environments = {
            "vanilla_postgres": database_environment(common_pm, oracle),
            "prior_postgres": identities["common_pm"]["prior"]["database_environment"],
            "current_postgres": identities["common_pm"]["current"][
                "database_environment"
            ],
        }
        if (
            len(
                {
                    bridge.canonical(environment)
                    for environment in database_environments.values()
                }
            )
            != 1
        ):
            raise RuntimeError("PostgreSQL release-bridge settings differ")
        result = {
            "schema_version": SCHEMA_VERSION,
            "artifact_type": ARTIFACT_TYPE,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": bridge.public_source(),
            "releases": {
                "prior": {
                    "ocpm_engine": bridge.PRIOR_ENGINE_VERSION,
                    "pg_ocpm": bridge.PRIOR_PG_OCPM_VERSION,
                    "ocpm_engine_revision": bridge.PRIOR_ENGINE_REVISION,
                    "pg_ocpm_revision": bridge.PRIOR_PG_OCPM_REVISION,
                    "worker_ocpm_engine": identities["common_pm"]["prior"][
                        "ocpm_engine"
                    ],
                    "worker_pg_ocpm": identities["common_pm"]["prior"]["pg_ocpm"],
                },
                "current": {
                    "ocpm_engine": bridge.CURRENT_ENGINE_VERSION,
                    "pg_ocpm": bridge.CURRENT_PG_OCPM_VERSION,
                    "ocpm_engine_revision": bridge.CURRENT_ENGINE_REVISION,
                    "pg_ocpm_revision": bridge.CURRENT_PG_OCPM_REVISION,
                    "worker_ocpm_engine": identities["common_pm"]["current"][
                        "ocpm_engine"
                    ],
                    "worker_pg_ocpm": identities["common_pm"]["current"]["pg_ocpm"],
                },
            },
            "environment": {
                "client": {
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "machine": platform.machine(),
                    "logical_cpus_visible": os.cpu_count(),
                },
                **database_environments,
                "worker_suites": identities,
                "host_fingerprints": {
                    "start": starting_host_fingerprint,
                    "end": ending_host_fingerprint,
                },
            },
            "provenance": provenance,
            "method": {
                "latency": {
                    "warmup_rounds": WARMUP_ROUNDS,
                    "epochs": LATENCY_EPOCHS,
                    "samples_per_epoch": SAMPLES_PER_EPOCH,
                    "total_samples_per_arm": (LATENCY_EPOCHS * SAMPLES_PER_EPOCH),
                    "clock": "time.perf_counter_ns",
                    "order_dictionary": [list(order) for order in ORDER_DICTIONARY],
                    "timing_scope": (
                        "pg_ocpm extraction and aggregation plus model construction "
                        "and native scoring; JSONL IPC and hashing excluded"
                    ),
                },
                "memory": {
                    "samples_per_arm": MEMORY_SAMPLES_PER_ARM,
                    "model": (
                        "fresh release-isolated process per sample; total baseline, "
                        "peak RSS, and incremental peak retained at 1 ms sampling"
                    ),
                },
                "concurrency": {
                    "suite_specs": {
                        suite: {
                            "workloads": list(CONCURRENCY_SPECS[suite]["workloads"]),
                            "levels": list(CONCURRENCY_SPECS[suite]["levels"]),
                            "execution_paths": list(EXECUTION_PATHS[suite]),
                        }
                        for suite in SUITES
                    },
                    "epochs_per_arm_level": CONCURRENCY_EPOCHS,
                    "minimum_epoch_seconds": CONCURRENCY_MIN_SECONDS,
                    "minimum_requests_per_worker": (
                        CONCURRENCY_MIN_REQUESTS_PER_WORKER
                    ),
                    "sample_encoding": SAMPLE_ENCODING,
                    "connection_model": (
                        "prestarted release-isolated processes with one persistent "
                        "PostgreSQL connection each"
                    ),
                    "timing_scope": (
                        "controller-to-worker round trip for throughput and latency; "
                        "worker-internal database plus native execution also retained"
                    ),
                },
                "storage": {
                    "autovacuum": "disabled during bridge",
                    "structural_snapshot": (
                        "database-wide VACUUM (ANALYZE) on each arm immediately "
                        "before relation/fork enumeration"
                    ),
                    "post_workload_snapshot": "diagnostic only",
                },
                "non_regression_thresholds": {
                    "latency": {
                        "relative_ceiling": LATENCY_CEILING,
                        "absolute_slack_ns": LATENCY_ABSOLUTE_SLACK_NS,
                    },
                    "memory": {
                        "relative_ceiling": MEMORY_CEILING,
                        "absolute_slack_bytes": MEMORY_ABSOLUTE_SLACK_BYTES,
                        "engine_maximum_incremental_peak_bytes": (
                            MAX_ENGINE_INCREMENTAL_PEAK_BYTES
                        ),
                        "engine_maximum_peak_rss_bytes": MAX_ENGINE_PEAK_RSS_BYTES,
                        "pm4py_maximum_peak_rss_bytes": MAX_PM4PY_PEAK_RSS_BYTES,
                    },
                    "concurrency": {
                        "relative_ceiling": CONCURRENCY_CEILING,
                        "absolute_p95_slack_ms": (CONCURRENCY_ABSOLUTE_P95_SLACK_MS),
                        "maximum_throughput_cv": (MAXIMUM_CONCURRENCY_THROUGHPUT_CV),
                    },
                    "storage": {"relative_ceiling": STORAGE_CEILING},
                },
                "correctness_gate": (
                    "every latency, memory, and concurrency answer SHA-256 equals "
                    "the untimed vanilla PostgreSQL reference implementation"
                ),
                "random_seed": RANDOM_SEED,
                "result_cache_used": False,
            },
            "suites": suites,
            "storage": {
                "structural_before_workloads": storage_before,
                "diagnostic_after_workloads": storage_after,
            },
            "summary": {
                "total_latency_workloads": total_workloads,
                "correct_latency_workloads": total_workloads,
                "minimum_p50_ratio_prior_over_current": round(min(latency_ratios), 3),
                "latency_non_regressed": sum(latency_gates),
                "memory_non_regressed": sum(memory_gates),
                "concurrency_levels_non_regressed": sum(concurrency_gates),
                "storage_non_regressed": sum(storage_gates),
                "target_met": all(
                    latency_gates + memory_gates + concurrency_gates + storage_gates
                ),
            },
        }
        output = Path(args.output)
        written = bridge.write_artifact(output, result)
        print(f"wrote {output}", flush=True)
        return written
    finally:
        for suite_workers in workers.values():
            for worker in suite_workers.values():
                worker.close()
        oracle.close()
        prior_database.close()
        current_database.close()


if __name__ == "__main__":
    benchmark(parse_args())
