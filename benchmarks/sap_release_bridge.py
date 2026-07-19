"""Counterbalanced SAP common-PM bridge between two locked releases.

This controller treats vanilla PostgreSQL plus the existing independent Python
kernels as an untimed correctness oracle.  It starts one persistent subprocess
per release and sends work over JSONL.  Each subprocess owns its PostgreSQL
connection and times the complete pg_ocpm extraction plus native scoring path,
so process startup, IPC, serialization, and answer hashing are excluded.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import math
import os
import platform
import random
import re
import statistics
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRIOR_ENGINE_VERSION = "0.4.0"
PRIOR_PG_OCPM_VERSION = "0.5.0"
CURRENT_ENGINE_VERSION = "0.6.0"
CURRENT_PG_OCPM_VERSION = "0.7.0"

PRIOR_ENGINE_REVISION = "8427c36aa16da11b04ba642672df096d6f21e156"
PRIOR_PG_OCPM_REVISION = "e72c5ffc281a1f1019d07aef8ad479217823e4f2"
CURRENT_ENGINE_REVISION = "c44e9341ced643e0b777a18d7b0d26a43127caa0"
CURRENT_PG_OCPM_REVISION = "279d81b3db0a0ae7470bf90824f1fbba9d188e70"

WARMUP_ROUNDS = 10
LATENCY_EPOCHS = 3
SAMPLES_PER_EPOCH = 30
RANDOM_SEED = 20260718
ORDER_DICTIONARY = (("prior", "current"), ("current", "prior"))
WORKLOAD_NAMES = (
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
EXPECTED_DATASETS = ("sap_o2c", "sap_p2p")

_REVISION = re.compile(r"[0-9a-f]{40}")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")


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
        help="isolated current wheel used only to import the shared workload module",
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
        default="/results/sap-common-pm-release-bridge-0.4.0-to-0.6.0.json",
    )
    args = parser.parse_args()
    if args.database:
        args.oracle_db = args.database
        args.prior_db = args.database
        args.current_db = args.database
    if args.timeout_seconds < 1:
        parser.error("--timeout-seconds must be positive")
    return args


def load_common_pm(engine_path: str):
    if engine_path:
        sys.path.insert(0, engine_path)
    return importlib.import_module("public_common_pm")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for bridge provenance")
    return value


def required_revision(name: str) -> str:
    value = required_environment(name)
    if _REVISION.fullmatch(value) is None:
        raise RuntimeError(f"{name} must be an exact lowercase Git revision")
    return value


def required_digest(name: str) -> str:
    value = required_environment(name)
    if _DIGEST.fullmatch(value) is None:
        raise RuntimeError(f"{name} must be a lowercase SHA-256 digest")
    return value


def required_image_id(name: str) -> str:
    value = required_environment(name)
    if _IMAGE_ID.fullmatch(value) is None:
        raise RuntimeError(f"{name} must be an immutable Docker image ID")
    return value


def required_boolean(name: str) -> bool:
    value = required_environment(name).lower()
    if value in {"1", "true"}:
        return True
    if value in {"0", "false"}:
        return False
    raise RuntimeError(f"{name} must be true, false, 1, or 0")


def capture_provenance(
    controller_path: Path,
    worker_path: Path,
    workload_path: Path,
) -> dict[str, Any]:
    provenance = {
        "benchmark_host_id": required_image_id("OCPM_BENCHMARK_HOST_ID"),
        "client_image_id": required_image_id("OCPM_CLIENT_IMAGE_ID"),
        "vanilla_database_image_id": required_image_id(
            "OCPM_VANILLA_DATABASE_IMAGE_ID"
        ),
        "prior_database_image_id": required_image_id("OCPM_PRIOR_DATABASE_IMAGE_ID"),
        "current_database_image_id": required_image_id(
            "OCPM_CURRENT_DATABASE_IMAGE_ID"
        ),
        "controller_source_revision": required_revision(
            "OCPM_CONTROLLER_SOURCE_REVISION"
        ),
        "controller_source_tree_clean": required_boolean(
            "OCPM_CONTROLLER_SOURCE_TREE_CLEAN"
        ),
        "prior_engine_source_revision": required_revision(
            "OCPM_PRIOR_ENGINE_SOURCE_REVISION"
        ),
        "prior_engine_source_tree_clean": required_boolean(
            "OCPM_PRIOR_ENGINE_SOURCE_TREE_CLEAN"
        ),
        "prior_pg_ocpm_source_revision": required_revision(
            "OCPM_PRIOR_PG_OCPM_SOURCE_REVISION"
        ),
        "prior_pg_ocpm_source_tree_clean": required_boolean(
            "OCPM_PRIOR_PG_OCPM_SOURCE_TREE_CLEAN"
        ),
        "current_engine_source_revision": required_revision(
            "OCPM_CURRENT_ENGINE_SOURCE_REVISION"
        ),
        "current_engine_source_tree_clean": required_boolean(
            "OCPM_CURRENT_ENGINE_SOURCE_TREE_CLEAN"
        ),
        "current_pg_ocpm_source_revision": required_revision(
            "OCPM_CURRENT_PG_OCPM_SOURCE_REVISION"
        ),
        "current_pg_ocpm_source_tree_clean": required_boolean(
            "OCPM_CURRENT_PG_OCPM_SOURCE_TREE_CLEAN"
        ),
        "harness_sha256": {
            "controller": file_sha256(controller_path),
            "worker": file_sha256(worker_path),
            "workload": file_sha256(workload_path),
        },
        "loader_sha256": {
            "prior": required_digest("OCPM_PRIOR_LOADER_SHA256"),
            "current": required_digest("OCPM_CURRENT_LOADER_SHA256"),
        },
    }
    locked_revisions = {
        "prior_engine_source_revision": PRIOR_ENGINE_REVISION,
        "prior_pg_ocpm_source_revision": PRIOR_PG_OCPM_REVISION,
        "current_engine_source_revision": CURRENT_ENGINE_REVISION,
        "current_pg_ocpm_source_revision": CURRENT_PG_OCPM_REVISION,
    }
    for field, expected in locked_revisions.items():
        if provenance[field] != expected:
            raise RuntimeError(
                f"{field} is {provenance[field]}; locked bridge requires {expected}"
            )
    return provenance


class BridgeWorker:
    def __init__(
        self,
        *,
        arm: str,
        host: str,
        database: str,
        engine_path: str,
        engine_version: str,
        pg_ocpm_version: str,
        timeout_seconds: int,
        worker_python: str,
        worker_path: Path,
    ):
        environment = dict(os.environ)
        environment["PYTHONPATH"] = engine_path
        environment["PYTHONNOUSERSITE"] = "1"
        environment.pop("PYTHONHOME", None)
        command = [
            worker_python,
            "-u",
            str(worker_path),
            "--arm",
            arm,
            "--host",
            host,
            "--database",
            database,
            "--timeout-seconds",
            str(timeout_seconds),
            "--expected-engine-version",
            engine_version,
            "--expected-pg-ocpm-version",
            pg_ocpm_version,
        ]
        self.arm = arm
        self.request_id = 0
        self.process = subprocess.Popen(
            command,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        try:
            self.identity = self.request("hello")
        except Exception:
            self.close()
            raise

    def request(self, operation: str, **values: Any) -> Any:
        if self.process.stdin is None or self.process.stdout is None:
            raise RuntimeError(f"{self.arm} worker pipes are unavailable")
        self.request_id += 1
        request = {"id": self.request_id, "op": operation, **values}
        self.process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            status = self.process.poll()
            raise RuntimeError(
                f"{self.arm} worker closed its protocol stream (exit={status})"
            )
        response = json.loads(line)
        if not isinstance(response, dict) or response.get("id") != self.request_id:
            raise RuntimeError(f"{self.arm} worker returned an invalid response")
        if response.get("ok") is not True:
            raise RuntimeError(
                f"{self.arm} worker failed: {response.get('error', 'unknown error')}"
            )
        return response.get("result")

    def execute(self, workload: str, fixture: dict[str, Any], *, timed: bool) -> dict:
        result = self.request(
            "run",
            workload=workload,
            fixture=fixture,
            timed=timed,
        )
        if not isinstance(result, dict):
            raise RuntimeError(f"{self.arm} worker returned a non-object result")
        return result

    def storage(self) -> dict[str, int]:
        result = self.request("storage")
        if not isinstance(result, dict):
            raise RuntimeError(f"{self.arm} worker returned invalid storage")
        return {str(key): int(value) for key, value in result.items()}

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                self.request("shutdown")
            except Exception:
                self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        if self.process.stdin is not None:
            self.process.stdin.close()
        if self.process.stdout is not None:
            self.process.stdout.close()


def fixture_payload(fixture: Any) -> dict[str, Any]:
    result = asdict(fixture)
    for key, value in tuple(result.items()):
        if isinstance(value, datetime):
            result[key] = value.isoformat()
    return result


def logical_fixture_payload(prior: Any, current: Any) -> dict[str, Any]:
    prior_payload = json.loads(json.dumps(asdict(prior), default=str))
    current_payload = json.loads(json.dumps(asdict(current), default=str))
    if prior_payload != current_payload:
        raise RuntimeError(f"logical fixture mismatch for {prior.name}")
    return prior_payload


def oracle_workload(common_pm: Any, database: Any, fixture: Any, workload: str) -> Any:
    if workload == "dfg_conformance_95pct":
        return common_pm.reference_dfg(
            common_pm.transition_rows(common_pm.base_pair_counts(database, fixture))
        )
    if workload == "variant_conformance_95pct":
        return common_pm.variant_result(
            common_pm.base_variant_counts(database, fixture), False
        )
    if workload == "next_activity_prediction":
        return common_pm.reference_next(
            common_pm.transition_rows(common_pm.base_pair_counts(database, fixture))
        )
    if workload == "dfg_frequency_drift":
        return common_pm.reference_drift(
            common_pm.transition_rows(common_pm.base_pair_counts(database, fixture))
        )
    if workload == "repeated_transition_rework":
        return common_pm.normalize_rows(
            database.rows(
                common_pm.REWORK_BASE,
                common_pm.params(fixture, False),
            )
        )
    if workload == "edge_bottleneck_ranking":
        return common_pm.bottleneck(database, fixture, False)
    if workload == "edge_bottleneck_prediction":
        return common_pm.edge_prediction(database, fixture, False)
    if workload == "edge_duration_time_series":
        return common_pm.duration_series(database, fixture, False)
    if workload == "activity_profile":
        return common_pm.activity_profile(database, fixture, False)
    raise ValueError(f"unsupported workload: {workload}")


def balanced_order_codes(per_order: int, rng: random.Random) -> list[int]:
    codes = [0] * per_order + [1] * per_order
    rng.shuffle(codes)
    return codes


def percentile(samples: list[int], percentile_value: float) -> int:
    ordered = sorted(samples)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * percentile_value) - 1)
    return ordered[index]


def serial_metrics(samples_ns: list[int]) -> dict[str, Any]:
    invalid = any(type(value) is not int or value <= 0 for value in samples_ns)
    if not samples_ns or invalid:
        raise RuntimeError("serial samples must be positive integer nanoseconds")
    ordered = sorted(samples_ns)
    return {
        "p50_ms": round(statistics.median(ordered) / 1_000_000, 3),
        "p95_ms": round(percentile(ordered, 0.95) / 1_000_000, 3),
        "minimum_ms": round(ordered[0] / 1_000_000, 3),
        "maximum_ms": round(ordered[-1] / 1_000_000, 3),
        "runs": len(ordered),
    }


def aggregate_arm(serial_epochs: list[dict[str, Any]], arm: str) -> dict[str, Any]:
    samples = [
        sample for epoch in serial_epochs for sample in epoch["arms"][arm]["samples_ns"]
    ]
    epoch_p95 = [epoch["arms"][arm]["p95_ms"] for epoch in serial_epochs]
    position_samples = {"first": [], "second": []}
    for epoch in serial_epochs:
        for order_code, sample in zip(
            epoch["order_codes"],
            epoch["arms"][arm]["samples_ns"],
        ):
            position = "first" if ORDER_DICTIONARY[order_code][0] == arm else "second"
            position_samples[position].append(sample)
    return {
        **serial_metrics(samples),
        "exact_samples": len(samples),
        "epoch_count": len(serial_epochs),
        "epoch_p95_median_ms": round(statistics.median(epoch_p95), 3),
        "epoch_p95_range_ms": [min(epoch_p95), max(epoch_p95)],
        "position_stratified": {
            position: {
                **serial_metrics(values),
                "exact_samples": len(values),
            }
            for position, values in position_samples.items()
        },
    }


def require_exact_answer(
    arm: str,
    workload: str,
    phase: str,
    result: dict[str, Any],
    oracle_sha256: str,
) -> None:
    if result.get("answer_sha256") != oracle_sha256:
        raise AssertionError(
            f"{arm} {workload} answer mismatch during {phase}: "
            f"{result.get('answer_sha256')} != {oracle_sha256}"
        )


def benchmark_workload(
    common_pm: Any,
    oracle_database: Any,
    oracle_fixture: Any,
    arm_fixtures: dict[str, Any],
    workers: dict[str, BridgeWorker],
    workload: str,
    rng: random.Random,
) -> tuple[dict[str, Any], bool]:
    oracle_answer = oracle_workload(
        common_pm,
        oracle_database,
        oracle_fixture,
        workload,
    )
    oracle_sha256 = canonical_sha256(oracle_answer)
    payloads = {arm: fixture_payload(fixture) for arm, fixture in arm_fixtures.items()}

    for arm in ("prior", "current"):
        result = workers[arm].execute(workload, payloads[arm], timed=False)
        require_exact_answer(arm, workload, "preflight", result, oracle_sha256)
        if result.get("elapsed_ns") is not None:
            raise RuntimeError(f"{arm} untimed preflight unexpectedly reported latency")

    warmup_order_codes = balanced_order_codes(WARMUP_ROUNDS // 2, rng)
    for round_index, order_code in enumerate(warmup_order_codes, start=1):
        for arm in ORDER_DICTIONARY[order_code]:
            result = workers[arm].execute(workload, payloads[arm], timed=False)
            require_exact_answer(
                arm,
                workload,
                f"warmup {round_index}",
                result,
                oracle_sha256,
            )
            if result.get("elapsed_ns") is not None:
                raise RuntimeError(f"{arm} warmup unexpectedly reported latency")

    first_execution_counts = {"prior": 0, "current": 0}
    serial_epochs = []
    for epoch_index in range(LATENCY_EPOCHS):
        order_codes = balanced_order_codes(SAMPLES_PER_EPOCH // 2, rng)
        epoch_samples = {"prior": [], "current": []}
        epoch_hashes = {"prior": [], "current": []}
        for round_index, order_code in enumerate(order_codes, start=1):
            order = ORDER_DICTIONARY[order_code]
            first_execution_counts[order[0]] += 1
            for arm in order:
                result = workers[arm].execute(workload, payloads[arm], timed=True)
                require_exact_answer(
                    arm,
                    workload,
                    f"epoch {epoch_index + 1} round {round_index}",
                    result,
                    oracle_sha256,
                )
                elapsed_ns = result.get("elapsed_ns")
                if type(elapsed_ns) is not int or elapsed_ns <= 0:
                    raise RuntimeError(
                        f"{arm} returned invalid elapsed_ns: {elapsed_ns!r}"
                    )
                epoch_samples[arm].append(elapsed_ns)
                epoch_hashes[arm].append(result["answer_sha256"])
        serial_epochs.append(
            {
                "epoch": epoch_index + 1,
                "order_codes": order_codes,
                "arms": {
                    arm: {
                        **serial_metrics(epoch_samples[arm]),
                        "samples_ns": epoch_samples[arm],
                        "answer_sha256s": epoch_hashes[arm],
                    }
                    for arm in ("prior", "current")
                },
            }
        )

    prior = aggregate_arm(serial_epochs, "prior")
    current = aggregate_arm(serial_epochs, "current")
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
    prior_median_ns = statistics.median(prior_samples)
    current_median_ns = statistics.median(current_samples)
    non_regressed = current_median_ns <= max(
        prior_median_ns * 1.10,
        prior_median_ns + 100_000,
    )
    row = {
        "workload": workload,
        "oracle_answer_sha256": oracle_sha256,
        "correct": True,
        "prior": prior,
        "current": current,
        "p50_ratio_prior_over_current": round(prior_median_ns / current_median_ns, 3),
        "first_execution_counts": first_execution_counts,
        "warmup_order_codes": warmup_order_codes,
        "serial_epochs": serial_epochs,
    }
    return row, non_regressed


def discover_paired_fixtures(
    common_pm: Any,
    oracle: Any,
    prior_database: Any,
    current_database: Any,
) -> list[tuple[Any, Any]]:
    prior = sorted(
        common_pm.discover_fixtures(oracle, prior_database),
        key=lambda fixture: fixture.name,
    )
    current = sorted(
        common_pm.discover_fixtures(oracle, current_database),
        key=lambda fixture: fixture.name,
    )
    if tuple(fixture.name for fixture in prior) != EXPECTED_DATASETS:
        raise RuntimeError("prior fixture does not contain exactly SAP O2C and P2P")
    if tuple(fixture.name for fixture in current) != EXPECTED_DATASETS:
        raise RuntimeError("current fixture does not contain exactly SAP O2C and P2P")
    for prior_fixture, current_fixture in zip(prior, current):
        logical_fixture_payload(prior_fixture, current_fixture)
    return list(zip(prior, current))


def public_source() -> dict[str, Any]:
    return {
        "title": "Collection of Object-Centric Event Logs (SAP IDES O2C and P2P)",
        "doi": "10.5281/zenodo.8261133",
        "license": "CC BY 4.0",
    }


def write_artifact(path: Path, result: dict[str, Any]) -> dict[str, Any]:
    unsigned = dict(result)
    unsigned.pop("payload_sha256", None)
    encoded = json.dumps(unsigned, indent=2, default=str) + "\n"
    result = {
        **unsigned,
        "payload_sha256": hashlib.sha256(encoded.encode()).hexdigest(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(result, indent=2, default=str) + "\n")
    temporary.replace(path)
    return result


def benchmark(args: argparse.Namespace) -> dict[str, Any]:
    common_pm = load_common_pm(args.controller_engine_path)
    controller_path = Path(__file__).resolve()
    worker_path = controller_path.with_name("sap_release_bridge_worker.py")
    workload_path = Path(common_pm.__file__).resolve()
    provenance = capture_provenance(controller_path, worker_path, workload_path)
    oracle = common_pm.Database(args.oracle_host, args.oracle_db, args.timeout_seconds)
    prior_metadata = common_pm.Database(
        args.prior_host, args.prior_db, args.timeout_seconds
    )
    current_metadata = common_pm.Database(
        args.current_host, args.current_db, args.timeout_seconds
    )
    workers: dict[str, BridgeWorker] = {}
    try:
        fixtures = discover_paired_fixtures(
            common_pm,
            oracle,
            prior_metadata,
            current_metadata,
        )
        prior_metadata.close()
        current_metadata.close()
        prior_metadata = None
        current_metadata = None

        workers["prior"] = BridgeWorker(
            arm="prior",
            host=args.prior_host,
            database=args.prior_db,
            engine_path=args.prior_engine_path,
            engine_version=PRIOR_ENGINE_VERSION,
            pg_ocpm_version=PRIOR_PG_OCPM_VERSION,
            timeout_seconds=args.timeout_seconds,
            worker_python=args.worker_python,
            worker_path=worker_path,
        )
        workers["current"] = BridgeWorker(
            arm="current",
            host=args.current_host,
            database=args.current_db,
            engine_path=args.current_engine_path,
            engine_version=CURRENT_ENGINE_VERSION,
            pg_ocpm_version=CURRENT_PG_OCPM_VERSION,
            timeout_seconds=args.timeout_seconds,
            worker_python=args.worker_python,
            worker_path=worker_path,
        )
        for arm in ("prior", "current"):
            identity = workers[arm].identity
            if (
                identity.get("workload_sha256")
                != provenance["harness_sha256"]["workload"]
            ):
                raise RuntimeError(f"{arm} worker loaded a different workload module")

        rng = random.Random(RANDOM_SEED)
        datasets = []
        ratios = []
        non_regressed_workloads = 0
        for prior_fixture, current_fixture in fixtures:
            print(f"bridging {prior_fixture.name}", flush=True)
            workloads = []
            for workload in WORKLOAD_NAMES:
                row, non_regressed = benchmark_workload(
                    common_pm,
                    oracle,
                    prior_fixture,
                    {"prior": prior_fixture, "current": current_fixture},
                    workers,
                    workload,
                    rng,
                )
                workloads.append(row)
                ratios.append(row["p50_ratio_prior_over_current"])
                non_regressed_workloads += int(non_regressed)
                print(
                    f"  {workload}: {row['p50_ratio_prior_over_current']:.3f}x",
                    flush=True,
                )
            datasets.append(
                {
                    "fixture": logical_fixture_payload(
                        prior_fixture,
                        current_fixture,
                    ),
                    "workloads": workloads,
                }
            )

        prior_identity = workers["prior"].identity
        current_identity = workers["current"].identity
        total_workloads = len(EXPECTED_DATASETS) * len(WORKLOAD_NAMES)
        result = {
            "schema_version": 1,
            "artifact_type": "sap_common_pm_release_bridge",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": public_source(),
            "releases": {
                "prior": {
                    "ocpm_engine": PRIOR_ENGINE_VERSION,
                    "pg_ocpm": PRIOR_PG_OCPM_VERSION,
                    "ocpm_engine_revision": PRIOR_ENGINE_REVISION,
                    "pg_ocpm_revision": PRIOR_PG_OCPM_REVISION,
                    "worker_ocpm_engine": prior_identity["ocpm_engine"],
                    "worker_pg_ocpm": prior_identity["pg_ocpm"],
                },
                "current": {
                    "ocpm_engine": CURRENT_ENGINE_VERSION,
                    "pg_ocpm": CURRENT_PG_OCPM_VERSION,
                    "ocpm_engine_revision": CURRENT_ENGINE_REVISION,
                    "pg_ocpm_revision": CURRENT_PG_OCPM_REVISION,
                    "worker_ocpm_engine": current_identity["ocpm_engine"],
                    "worker_pg_ocpm": current_identity["pg_ocpm"],
                },
            },
            "environment": {
                "client": {
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "machine": platform.machine(),
                    "logical_cpus_visible": os.cpu_count(),
                },
                "vanilla_postgres": common_pm.database_environment(oracle),
                "prior_postgres": prior_identity["database_environment"],
                "current_postgres": current_identity["database_environment"],
            },
            "provenance": provenance,
            "method": {
                "warmup_rounds": WARMUP_ROUNDS,
                "latency_epochs": LATENCY_EPOCHS,
                "samples_per_epoch": SAMPLES_PER_EPOCH,
                "total_samples_per_arm": LATENCY_EPOCHS * SAMPLES_PER_EPOCH,
                "clock": "time.perf_counter_ns",
                "random_seed": RANDOM_SEED,
                "order_dictionary": [list(order) for order in ORDER_DICTIONARY],
                "warmup_order_counts": {"0": 5, "1": 5},
                "epoch_order_counts": {"0": 15, "1": 15},
                "timing_scope": (
                    "pg_ocpm database extraction and aggregation plus model "
                    "construction and native scoring"
                ),
                "ipc_excluded": True,
                "oracle": (
                    "untimed indexed vanilla PostgreSQL extraction plus independent "
                    "Python reference kernels"
                ),
                "correctness_gate": (
                    "every preflight, warmup, and measured canonical answer SHA-256 "
                    "must equal the untimed vanilla oracle"
                ),
                "storage": (
                    "sum of PostgreSQL heap, index, and TOAST relation bytes in the "
                    "ocpm schema"
                ),
            },
            "datasets": datasets,
            "storage": {
                "prior_pg_ocpm": workers["prior"].storage(),
                "current_pg_ocpm": workers["current"].storage(),
            },
            "summary": {
                "total_workloads": total_workloads,
                "correct_workloads": total_workloads,
                "non_regressed_workloads": non_regressed_workloads,
                "minimum_p50_ratio_prior_over_current": round(min(ratios), 3),
                "target_met": non_regressed_workloads == total_workloads,
            },
        }
        target = Path(args.output)
        written = write_artifact(target, result)
        print(f"wrote {target}", flush=True)
        return written
    finally:
        for worker in workers.values():
            worker.close()
        oracle.close()
        if prior_metadata is not None:
            prior_metadata.close()
        if current_metadata is not None:
            current_metadata.close()


if __name__ == "__main__":
    benchmark(parse_args())
