"""Shared support for the unified matched SAP release bridge."""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import os
import random
import re
import statistics
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime
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
        suite: str = "common_pm",
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
            "--suite",
            suite,
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
