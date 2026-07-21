#!/usr/bin/env python3
"""Independently validate the unified matched SAP release bridge artifact."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import math
import re
import statistics
import struct
import zlib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULT = ROOT / "docs/results/sap-release-bridge-0.6.0-to-0.8.0.json"

SCHEMA_VERSION = 2
ARTIFACT_TYPE = "sap_release_bridge"
ARMS = ("prior", "current")
ORDER_DICTIONARY = (("prior", "current"), ("current", "prior"))
SUITES = ("common_pm", "pm4py")
DATASETS = ("sap_o2c", "sap_p2p")
WORKLOADS = {
    "common_pm": (
        "dfg_conformance_95pct",
        "variant_conformance_95pct",
        "next_activity_prediction",
        "dfg_frequency_drift",
        "repeated_transition_rework",
        "edge_bottleneck_ranking",
        "edge_bottleneck_prediction",
        "edge_duration_time_series",
        "activity_profile",
    ),
    "pm4py": (
        "dfg_conformance_95pct",
        "variant_conformance_95pct",
        "next_activity_prediction",
        "edge_bottleneck_ranking",
    ),
}
EXECUTION_PATHS = {
    "common_pm": ("pg_ocpm_rust",),
    "pm4py": ("pg_ocpm_pm4py", "pg_ocpm_ocpm_engine"),
}
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

EXPECTED_SOURCE = {
    "title": "Collection of Object-Centric Event Logs (SAP IDES O2C and P2P)",
    "doi": "10.5281/zenodo.8261133",
    "license": "CC BY 4.0",
}
EXPECTED_RELEASES = {
    "prior": {
        "ocpm_engine": "0.6.0",
        "pg_ocpm": "0.7.0",
        "ocpm_engine_revision": "c44e9341ced643e0b777a18d7b0d26a43127caa0",
        "pg_ocpm_revision": "279d81b3db0a0ae7470bf90824f1fbba9d188e70",
    },
    "current": {
        "ocpm_engine": "0.8.0",
        "pg_ocpm": "0.8.0",
        "ocpm_engine_revision": "f5a95ecd6b8a1f184f8ffed2371980ef419beaab",
        "pg_ocpm_revision": "0e15ab10f8ec87518b9e822072028fb3eda3879c",
    },
}
EXPECTED_HARNESS_SHA256 = {
    "controller": "e267b33f958448f116f6e56c9b0c1250d931a54eb4a983921da45c120809315a",
    "support": "62bd9edc72dce3f18fdd2bade8e5b50b41a2418c0e4131e4534de92cd1f68e3b",
    "worker": "bf2772136b1f8f2e0580e4533ce7556c13c57343aed99301657b2fb0f0ac31ce",
    "common_pm": "bd61720203fb027339d1f5b1f147be18b5b0525702e51285a4019bcbb7c83180",
    "pm4py": "f3c6dc87fb40cb9a7b73519f83f558849b919d729ec65f0f16f65e089121c7b1",
    "requirements_lock": (
        "659c44221d2a473777901318dcfd7ac23443beb10d57642e337b2a03132bbb54"
    ),
}
EXPECTED_LOADER_SHA256 = {
    "prior": "7221622a43d79e0cc58e8567a852b17c45455323ba3108185ac7824780033c4e",
    "current": "7221622a43d79e0cc58e8567a852b17c45455323ba3108185ac7824780033c4e",
}
EXPECTED_POSTGRES_BASE_IMAGE = (
    "postgres:16@sha256:"
    "33f923b05f64ca54ac4401c01126a6b92afe839a0aa0a52bc5aeb5cc958e5f20"
)

LATENCY_EPOCHS = 3
LATENCY_SAMPLES_PER_EPOCH = 30
LATENCY_TOTAL_SAMPLES = LATENCY_EPOCHS * LATENCY_SAMPLES_PER_EPOCH
LATENCY_WARMUPS = 10
LATENCY_CEILING = 1.10
LATENCY_ABSOLUTE_SLACK_NS = 100_000
MEMORY_SAMPLES_PER_ARM = 4
MEMORY_CEILING = 1.10
MEMORY_PAGE_SLACK = 64 * 1024
MAX_NATIVE_INCREMENTAL_BYTES = 8 * 1024 * 1024
MAX_NATIVE_TOTAL_BYTES = 64 * 1024 * 1024
MAX_PM4PY_TOTAL_BYTES = 256 * 1024 * 1024
CONCURRENCY_EPOCHS = 4
CONCURRENCY_MIN_SECONDS = 5.0
CONCURRENCY_MIN_WALL_NS = int(CONCURRENCY_MIN_SECONDS * 1_000_000_000)
CONCURRENCY_MIN_REQUESTS_PER_WORKER = 32
CONCURRENCY_SAMPLE_ENCODING = "u64le+zlib+base64-v1"
MAX_CONCURRENCY_QPS_CV = 0.15
CONCURRENCY_QPS_CEILING = 1.10
CONCURRENCY_P95_CEILING = 1.10
CONCURRENCY_P95_ABSOLUTE_SLACK_NS = 100_000
STORAGE_CEILING_NUMERATOR = 101
STORAGE_CEILING_DENOMINATOR = 100
RANDOM_SEED = 20260718

HEX_64 = re.compile(r"[0-9a-f]{64}")
REVISION = re.compile(r"[0-9a-f]{40}")
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", nargs="?", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--expected-payload-sha256")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="allow only a dirty controller tree; measured release trees stay clean",
    )
    return parser.parse_args()


def fail(message: str) -> None:
    raise SystemExit(message)


def _keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected:
        fail(f"{label}: fields changed")
    return value


def _positive_int(value: object) -> bool:
    return type(value) is int and value > 0


def _nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _hex64(value: object) -> bool:
    return isinstance(value, str) and HEX_64.fullmatch(value) is not None


def load_verified(path: Path, expected_digest: str | None = None) -> dict[str, Any]:
    try:
        result = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"could not read {path}: {error}")
    if not isinstance(result, dict):
        fail(f"{path}: expected one JSON object")
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


def _nearest_rank(samples: list[int], percentile: float) -> int:
    ordered = sorted(samples)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * percentile) - 1)]


def _serial_metrics_ns(samples: list[int]) -> dict[str, Any]:
    if not samples or any(not _positive_int(value) for value in samples):
        fail("raw timing samples must be positive integer nanoseconds")
    ordered = sorted(samples)
    return {
        "p50_ms": round(statistics.median(ordered) / 1_000_000, 3),
        "p95_ms": round(_nearest_rank(ordered, 0.95) / 1_000_000, 3),
        "minimum_ms": round(ordered[0] / 1_000_000, 3),
        "maximum_ms": round(ordered[-1] / 1_000_000, 3),
        "runs": len(ordered),
    }


def _concurrency_metrics_ns(samples: list[int]) -> dict[str, Any]:
    return {
        **_serial_metrics_ns(samples),
        "p99_ms": round(_nearest_rank(samples, 0.99) / 1_000_000, 3),
    }


def _latency_aggregate(
    epochs: list[dict[str, Any]], arm: str
) -> tuple[dict[str, Any], list[int]]:
    pooled = [sample for epoch in epochs for sample in epoch["arms"][arm]["samples_ns"]]
    epoch_p95 = [epoch["arms"][arm]["p95_ms"] for epoch in epochs]
    positions: dict[str, list[int]] = {"first": [], "second": []}
    for epoch in epochs:
        for code, sample in zip(epoch["order_codes"], epoch["arms"][arm]["samples_ns"]):
            position = "first" if ORDER_DICTIONARY[code][0] == arm else "second"
            positions[position].append(sample)
    result = {
        **_serial_metrics_ns(pooled),
        "exact_samples": len(pooled),
        "epoch_count": len(epochs),
        "epoch_p95_median_ms": round(statistics.median(epoch_p95), 3),
        "epoch_p95_range_ms": [min(epoch_p95), max(epoch_p95)],
        "position_stratified": {
            position: {
                **_serial_metrics_ns(values),
                "exact_samples": len(values),
            }
            for position, values in positions.items()
        },
    }
    return result, pooled


def validate_source(source: object) -> None:
    if source != EXPECTED_SOURCE:
        fail("SAP source metadata changed")


def validate_releases(releases: object) -> None:
    value = _keys(releases, set(ARMS), "releases")
    fields = {
        "ocpm_engine",
        "pg_ocpm",
        "ocpm_engine_revision",
        "pg_ocpm_revision",
        "worker_ocpm_engine",
        "worker_pg_ocpm",
    }
    for arm in ARMS:
        release = _keys(value[arm], fields, f"releases/{arm}")
        for field, expected in EXPECTED_RELEASES[arm].items():
            if release.get(field) != expected:
                fail(f"releases/{arm}: unexpected {field}")
        if release["worker_ocpm_engine"] != release["ocpm_engine"]:
            fail(f"releases/{arm}: worker ocpm-engine version mismatch")
        if release["worker_pg_ocpm"] != release["pg_ocpm"]:
            fail(f"releases/{arm}: worker pg_ocpm version mismatch")


def validate_environment(environment: object, provenance: dict[str, Any]) -> None:
    value = _keys(
        environment,
        {
            "client",
            "vanilla_postgres",
            "prior_postgres",
            "current_postgres",
            "worker_suites",
            "host_fingerprints",
        },
        "environment",
    )
    client = _keys(
        value["client"],
        {"python", "platform", "machine", "logical_cpus_visible"},
        "environment/client",
    )
    if any(
        not isinstance(client[field], str) or not client[field]
        for field in ("python", "platform", "machine")
    ) or not _positive_int(client["logical_cpus_visible"]):
        fail("environment/client: invalid host identity")
    database_fields = {
        "postgres_version",
        "shared_buffers",
        "effective_cache_size",
        "work_mem",
        "maintenance_work_mem",
        "max_parallel_workers_per_gather",
        "random_page_cost",
        "jit",
        "autovacuum",
    }
    databases = []
    for name in ("vanilla_postgres", "prior_postgres", "current_postgres"):
        database = _keys(value[name], database_fields, f"environment/{name}")
        if any(not isinstance(item, str) or not item for item in database.values()):
            fail(f"environment/{name}: invalid database setting")
        databases.append(database)
    if not (databases[0] == databases[1] == databases[2]):
        fail("PostgreSQL versions or settings differ across release arms")
    if databases[0]["autovacuum"] != "off":
        fail("environment: autovacuum must be disabled during the bridge")

    worker_fields = {
        "arm",
        "ocpm_engine",
        "native_ocpm_engine",
        "pg_ocpm",
        "pg_extension",
        "suite",
        "python",
        "pm4py",
        "psutil",
        "executable",
        "package_path",
        "database_environment",
        "workload_sha256",
    }
    worker_suites = _keys(value["worker_suites"], set(SUITES), "worker_suites")
    identities: list[dict[str, Any]] = []
    package_paths: dict[str, set[str]] = {arm: set() for arm in ARMS}
    for suite in SUITES:
        suite_workers = _keys(worker_suites[suite], set(ARMS), f"worker_suites/{suite}")
        for arm in ARMS:
            identity = _keys(
                suite_workers[arm],
                worker_fields,
                f"worker_suites/{suite}/{arm}",
            )
            release = EXPECTED_RELEASES[arm]
            if (
                identity["arm"] != arm
                or identity["suite"] != suite
                or identity["ocpm_engine"] != release["ocpm_engine"]
                or identity["native_ocpm_engine"] != release["ocpm_engine"]
                or identity["pg_ocpm"] != release["pg_ocpm"]
                or identity["pg_extension"] != release["pg_ocpm"]
                or identity["database_environment"] != value[f"{arm}_postgres"]
                or identity["workload_sha256"] != EXPECTED_HARNESS_SHA256[suite]
            ):
                fail(f"worker_suites/{suite}/{arm}: release identity changed")
            if any(
                not isinstance(identity[field], str) or not identity[field]
                for field in (
                    "python",
                    "pm4py",
                    "psutil",
                    "executable",
                    "package_path",
                )
            ):
                fail(f"worker_suites/{suite}/{arm}: invalid runtime identity")
            if not identity["package_path"].endswith("/ocpm_engine/__init__.py"):
                fail(f"worker_suites/{suite}/{arm}: invalid package path")
            package_paths[arm].add(identity["package_path"])
            identities.append(identity)
    for field in ("python", "pm4py", "psutil", "executable"):
        if len({identity[field] for identity in identities}) != 1:
            fail(f"worker_suites: {field} differs across measured workers")
    if client["python"] != identities[0]["python"]:
        fail("worker_suites: controller and worker Python versions differ")
    if any(len(paths) != 1 for paths in package_paths.values()) or (
        package_paths["prior"] == package_paths["current"]
    ):
        fail("worker_suites: release package isolation is invalid")

    fingerprints = _keys(
        value["host_fingerprints"], {"start", "end"}, "host_fingerprints"
    )
    fingerprint_fields = {
        "benchmark_host_id",
        "client_image_id",
        "vanilla_database_image_id",
        "prior_database_image_id",
        "current_database_image_id",
        "python",
        "platform",
        "machine",
        "logical_cpus_visible",
    }
    start = _keys(fingerprints["start"], fingerprint_fields, "host_fingerprints/start")
    end = _keys(fingerprints["end"], fingerprint_fields, "host_fingerprints/end")
    if start != end:
        fail("host fingerprint changed during the release bridge")
    for field in (
        "benchmark_host_id",
        "client_image_id",
        "vanilla_database_image_id",
        "prior_database_image_id",
        "current_database_image_id",
    ):
        if start[field] != provenance[field]:
            fail(f"host_fingerprints: {field} differs from provenance")
    for field in ("python", "platform", "machine", "logical_cpus_visible"):
        if start[field] != client[field]:
            fail(f"host_fingerprints: {field} differs from client environment")


def validate_provenance(provenance: object, *, allow_dirty: bool) -> None:
    fields = {
        "benchmark_host_id",
        "client_image_id",
        "vanilla_database_image_id",
        "prior_database_image_id",
        "current_database_image_id",
        "controller_source_revision",
        "controller_source_tree_clean",
        "prior_engine_source_revision",
        "prior_engine_source_tree_clean",
        "prior_pg_ocpm_source_revision",
        "prior_pg_ocpm_source_tree_clean",
        "current_engine_source_revision",
        "current_engine_source_tree_clean",
        "current_pg_ocpm_source_revision",
        "current_pg_ocpm_source_tree_clean",
        "harness_sha256",
        "loader_sha256",
        "postgres_base_image",
    }
    value = _keys(provenance, fields, "provenance")
    for field in (
        "benchmark_host_id",
        "client_image_id",
        "vanilla_database_image_id",
        "prior_database_image_id",
        "current_database_image_id",
    ):
        if (
            not isinstance(value[field], str)
            or IMAGE_ID.fullmatch(value[field]) is None
        ):
            fail(f"provenance: invalid image identity {field}")
    if (
        not isinstance(value["controller_source_revision"], str)
        or REVISION.fullmatch(value["controller_source_revision"]) is None
    ):
        fail("provenance: invalid controller revision")
    revision_fields = {
        "prior_engine_source_revision": EXPECTED_RELEASES["prior"][
            "ocpm_engine_revision"
        ],
        "prior_pg_ocpm_source_revision": EXPECTED_RELEASES["prior"]["pg_ocpm_revision"],
        "current_engine_source_revision": EXPECTED_RELEASES["current"][
            "ocpm_engine_revision"
        ],
        "current_pg_ocpm_source_revision": EXPECTED_RELEASES["current"][
            "pg_ocpm_revision"
        ],
    }
    for field, expected in revision_fields.items():
        if value[field] != expected:
            fail(f"provenance: unexpected {field}")
    measured_cleanliness = (
        "prior_engine_source_tree_clean",
        "prior_pg_ocpm_source_tree_clean",
        "current_engine_source_tree_clean",
        "current_pg_ocpm_source_tree_clean",
    )
    if any(value[field] is not True for field in measured_cleanliness):
        fail("provenance: measured release source trees must be clean")
    controller_clean = value["controller_source_tree_clean"]
    if type(controller_clean) is not bool:
        fail("provenance: controller cleanliness must be boolean")
    if not allow_dirty and controller_clean is not True:
        fail("provenance: release verification requires a clean controller")
    if value["harness_sha256"] != EXPECTED_HARNESS_SHA256:
        fail("provenance: unexpected harness source hashes")
    if value["loader_sha256"] != EXPECTED_LOADER_SHA256:
        fail("provenance: unexpected loader source hashes")
    if value["postgres_base_image"] != EXPECTED_POSTGRES_BASE_IMAGE:
        fail("provenance: PostgreSQL base image changed")


def validate_method(method: object) -> None:
    value = _keys(
        method,
        {
            "latency",
            "memory",
            "concurrency",
            "storage",
            "non_regression_thresholds",
            "correctness_gate",
            "random_seed",
            "result_cache_used",
        },
        "method",
    )
    latency = _keys(
        value["latency"],
        {
            "warmup_rounds",
            "epochs",
            "samples_per_epoch",
            "total_samples_per_arm",
            "clock",
            "order_dictionary",
            "timing_scope",
        },
        "method/latency",
    )
    expected_latency = {
        "warmup_rounds": LATENCY_WARMUPS,
        "epochs": LATENCY_EPOCHS,
        "samples_per_epoch": LATENCY_SAMPLES_PER_EPOCH,
        "total_samples_per_arm": LATENCY_TOTAL_SAMPLES,
        "clock": "time.perf_counter_ns",
        "order_dictionary": [list(order) for order in ORDER_DICTIONARY],
    }
    if any(
        latency.get(field) != expected for field, expected in expected_latency.items()
    ):
        fail("method/latency: protocol changed")
    if not isinstance(latency["timing_scope"], str) or not latency["timing_scope"]:
        fail("method/latency: timing scope missing")
    memory = _keys(value["memory"], {"samples_per_arm", "model"}, "method/memory")
    if memory["samples_per_arm"] != MEMORY_SAMPLES_PER_ARM or not isinstance(
        memory["model"], str
    ):
        fail("method/memory: protocol changed")
    concurrency = _keys(
        value["concurrency"],
        {
            "suite_specs",
            "epochs_per_arm_level",
            "minimum_epoch_seconds",
            "minimum_requests_per_worker",
            "connection_model",
            "timing_scope",
            "sample_encoding",
        },
        "method/concurrency",
    )
    expected_specs = {
        suite: {
            "workloads": list(CONCURRENCY_SPECS[suite]["workloads"]),
            "levels": list(CONCURRENCY_SPECS[suite]["levels"]),
            "execution_paths": list(EXECUTION_PATHS[suite]),
        }
        for suite in SUITES
    }
    if (
        concurrency["suite_specs"] != expected_specs
        or concurrency["epochs_per_arm_level"] != CONCURRENCY_EPOCHS
        or concurrency["minimum_epoch_seconds"] != CONCURRENCY_MIN_SECONDS
        or concurrency["minimum_requests_per_worker"]
        != CONCURRENCY_MIN_REQUESTS_PER_WORKER
        or concurrency["sample_encoding"] != CONCURRENCY_SAMPLE_ENCODING
        or any(
            not isinstance(concurrency[field], str) or not concurrency[field]
            for field in ("connection_model", "timing_scope")
        )
    ):
        fail("method/concurrency: protocol changed")
    storage = _keys(
        value["storage"],
        {"autovacuum", "structural_snapshot", "post_workload_snapshot"},
        "method/storage",
    )
    if storage["autovacuum"] != "disabled during bridge" or any(
        not isinstance(storage[field], str) or not storage[field]
        for field in ("structural_snapshot", "post_workload_snapshot")
    ):
        fail("method/storage: protocol changed")
    thresholds = _keys(
        value["non_regression_thresholds"],
        {"latency", "memory", "concurrency", "storage"},
        "method/non_regression_thresholds",
    )
    expected_thresholds = {
        "latency": {
            "relative_ceiling": LATENCY_CEILING,
            "absolute_slack_ns": LATENCY_ABSOLUTE_SLACK_NS,
        },
        "memory": {
            "relative_ceiling": MEMORY_CEILING,
            "absolute_slack_bytes": MEMORY_PAGE_SLACK,
            "engine_maximum_incremental_peak_bytes": MAX_NATIVE_INCREMENTAL_BYTES,
            "engine_maximum_peak_rss_bytes": MAX_NATIVE_TOTAL_BYTES,
            "pm4py_maximum_peak_rss_bytes": MAX_PM4PY_TOTAL_BYTES,
        },
        "concurrency": {
            "relative_ceiling": CONCURRENCY_QPS_CEILING,
            "absolute_p95_slack_ms": (CONCURRENCY_P95_ABSOLUTE_SLACK_NS / 1_000_000),
            "maximum_throughput_cv": MAX_CONCURRENCY_QPS_CV,
        },
        "storage": {
            "relative_ceiling": (
                STORAGE_CEILING_NUMERATOR / STORAGE_CEILING_DENOMINATOR
            )
        },
    }
    if thresholds != expected_thresholds:
        fail("method/non_regression_thresholds: contract changed")
    if (
        not isinstance(value["correctness_gate"], str)
        or not value["correctness_gate"]
        or value["random_seed"] != RANDOM_SEED
        or value["result_cache_used"] is not False
    ):
        fail("method: correctness, seed, or cache contract changed")


def validate_latency_row(
    row: object,
    *,
    label: str,
    expected_path: str,
    expected_workload: str,
) -> tuple[float, dict[str, Any]]:
    fields = {
        "workload",
        "execution_path",
        "oracle_answer_sha256",
        "input_evidence",
        "correct",
        "prior",
        "current",
        "p50_ratio_prior_over_current",
        "non_regressed",
        "first_execution_counts",
        "warmup_order_codes",
        "serial_epochs",
    }
    value = _keys(row, fields, label)
    if (
        value["workload"] != expected_workload
        or value["execution_path"] != expected_path
    ):
        fail(f"{label}: workload or execution path changed")
    oracle_hash = value["oracle_answer_sha256"]
    if not _hex64(oracle_hash) or value["correct"] is not True:
        fail(f"{label}: correctness evidence invalid")
    inputs = _keys(value["input_evidence"], set(ARMS), f"{label}/input_evidence")
    if (
        inputs["prior"] != inputs["current"]
        or any(not isinstance(inputs[arm], dict) for arm in ARMS)
        or not inputs["current"]
    ):
        fail(f"{label}: release input evidence differs")
    warmups = value["warmup_order_codes"]
    if (
        not isinstance(warmups, list)
        or len(warmups) != LATENCY_WARMUPS
        or warmups.count(0) != 5
        or warmups.count(1) != 5
        or any(code not in (0, 1) for code in warmups)
    ):
        fail(f"{label}: warmup order is not exactly 5/5")
    if value["first_execution_counts"] != {"prior": 45, "current": 45}:
        fail(f"{label}: measured order is not exactly counterbalanced")
    epochs = value["serial_epochs"]
    if not isinstance(epochs, list) or len(epochs) != LATENCY_EPOCHS:
        fail(f"{label}: expected three latency epochs")
    epoch_fields = {
        "p50_ms",
        "p95_ms",
        "minimum_ms",
        "maximum_ms",
        "runs",
        "samples_ns",
        "answer_sha256s",
    }
    decoded_first = {arm: 0 for arm in ARMS}
    for epoch_number, epoch_raw in enumerate(epochs, start=1):
        epoch = _keys(
            epoch_raw, {"epoch", "order_codes", "arms"}, f"{label}/epoch-{epoch_number}"
        )
        codes = epoch["order_codes"]
        if (
            epoch["epoch"] != epoch_number
            or not isinstance(codes, list)
            or len(codes) != LATENCY_SAMPLES_PER_EPOCH
            or codes.count(0) != 15
            or codes.count(1) != 15
            or any(code not in (0, 1) for code in codes)
        ):
            fail(f"{label}/epoch-{epoch_number}: order is not exactly 15/15")
        for code in codes:
            decoded_first[ORDER_DICTIONARY[code][0]] += 1
        arms = _keys(epoch["arms"], set(ARMS), f"{label}/epoch-{epoch_number}/arms")
        for arm in ARMS:
            evidence = _keys(
                arms[arm], epoch_fields, f"{label}/epoch-{epoch_number}/{arm}"
            )
            samples = evidence["samples_ns"]
            hashes = evidence["answer_sha256s"]
            if (
                not isinstance(samples, list)
                or len(samples) != LATENCY_SAMPLES_PER_EPOCH
                or any(not _positive_int(sample) for sample in samples)
            ):
                fail(f"{label}/epoch-{epoch_number}/{arm}: invalid raw latency")
            if (
                not isinstance(hashes, list)
                or len(hashes) != LATENCY_SAMPLES_PER_EPOCH
                or any(item != oracle_hash for item in hashes)
            ):
                fail(f"{label}/epoch-{epoch_number}/{arm}: answer hash mismatch")
            if evidence != {
                **_serial_metrics_ns(samples),
                "samples_ns": samples,
                "answer_sha256s": hashes,
            }:
                fail(
                    f"{label}/epoch-{epoch_number}/{arm}: metrics differ from "
                    "raw samples"
                )
    if decoded_first != value["first_execution_counts"]:
        fail(f"{label}: retained orders differ from first-execution counts")
    pooled: dict[str, list[int]] = {}
    for arm in ARMS:
        expected, pooled[arm] = _latency_aggregate(epochs, arm)
        if value[arm] != expected:
            fail(f"{label}/{arm}: aggregate differs from raw latency")
    prior_median = statistics.median(pooled["prior"])
    current_median = statistics.median(pooled["current"])
    ratio = round(prior_median / current_median, 3)
    if value["p50_ratio_prior_over_current"] != ratio:
        fail(f"{label}: p50 ratio differs from raw latency")
    non_regressed = current_median <= max(
        prior_median * LATENCY_CEILING,
        prior_median + LATENCY_ABSOLUTE_SLACK_NS,
    )
    if value["non_regressed"] is not non_regressed:
        fail(f"{label}: non-regression flag differs from raw latency")
    if not non_regressed:
        fail(f"{label}: matched-release latency regression")
    return ratio, inputs


def _memory_aggregate(samples: list[dict[str, Any]]) -> dict[str, Any]:
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


def _memory_allowed(prior: int) -> int:
    return max(math.ceil(prior * MEMORY_CEILING), prior + MEMORY_PAGE_SLACK)


def validate_memory_row(
    row: object,
    *,
    label: str,
    expected_path: str,
    expected_workload: str,
    latency_row: dict[str, Any],
) -> None:
    value = _keys(
        row,
        {
            "workload",
            "execution_path",
            "oracle_answer_sha256",
            "order_codes",
            "prior",
            "current",
            "relative_non_regressed",
            "absolute_bound_met",
            "non_regressed",
        },
        label,
    )
    if (
        value["workload"] != expected_workload
        or value["execution_path"] != expected_path
        or value["oracle_answer_sha256"] != latency_row["oracle_answer_sha256"]
    ):
        fail(f"{label}: workload, path, or oracle differs from latency evidence")
    order_codes = value["order_codes"]
    if (
        not isinstance(order_codes, list)
        or len(order_codes) != MEMORY_SAMPLES_PER_ARM
        or order_codes.count(0) != 2
        or order_codes.count(1) != 2
        or any(code not in (0, 1) for code in order_codes)
    ):
        fail(f"{label}: memory order is not exactly 2/2")
    sample_fields = {
        "sample",
        "arm_position",
        "worker_pid",
        "answer_sha256",
        "execution_path",
        "input",
        "baseline_rss_bytes",
        "peak_rss_bytes",
        "incremental_peak_bytes",
        "elapsed_ns",
    }
    all_pids: list[int] = []
    for arm in ARMS:
        aggregate = _keys(
            value[arm],
            {
                "samples",
                "exact_samples",
                "median_baseline_rss_bytes",
                "median_peak_rss_bytes",
                "maximum_peak_rss_bytes",
                "median_incremental_peak_bytes",
                "maximum_incremental_peak_bytes",
            },
            f"{label}/{arm}",
        )
        samples = aggregate["samples"]
        if not isinstance(samples, list) or len(samples) != MEMORY_SAMPLES_PER_ARM:
            fail(f"{label}/{arm}: expected four fresh memory samples")
        for index, sample_raw in enumerate(samples, start=1):
            sample = _keys(sample_raw, sample_fields, f"{label}/{arm}/sample-{index}")
            expected_position = ORDER_DICTIONARY[order_codes[index - 1]].index(arm) + 1
            if sample["sample"] != index or sample["arm_position"] != expected_position:
                fail(f"{label}/{arm}/sample-{index}: order annotation mismatch")
            pid = sample["worker_pid"]
            baseline = sample["baseline_rss_bytes"]
            peak = sample["peak_rss_bytes"]
            incremental = sample["incremental_peak_bytes"]
            if not _positive_int(pid):
                fail(f"{label}/{arm}/sample-{index}: invalid worker PID")
            all_pids.append(pid)
            if (
                not _positive_int(baseline)
                or not _positive_int(peak)
                or peak < baseline
                or not _nonnegative_int(incremental)
                or incremental != peak - baseline
                or not _positive_int(sample["elapsed_ns"])
            ):
                fail(f"{label}/{arm}/sample-{index}: invalid RSS arithmetic")
            if (
                sample["answer_sha256"] != value["oracle_answer_sha256"]
                or sample["execution_path"] != expected_path
                or sample["input"] != latency_row["input_evidence"][arm]
            ):
                fail(f"{label}/{arm}/sample-{index}: answer or input mismatch")
        if aggregate != _memory_aggregate(samples):
            fail(f"{label}/{arm}: memory aggregate differs from raw samples")
    if len(set(all_pids)) != len(all_pids):
        fail(f"{label}: memory samples did not use unique fresh process PIDs")
    prior = value["prior"]
    current = value["current"]
    compared_metrics = (
        "median_peak_rss_bytes",
        "maximum_peak_rss_bytes",
        "median_incremental_peak_bytes",
        "maximum_incremental_peak_bytes",
    )
    relative_non_regressed = all(
        current[metric] <= _memory_allowed(prior[metric]) for metric in compared_metrics
    )
    if expected_path in ("pg_ocpm_rust", "pg_ocpm_ocpm_engine"):
        absolute_bound_met = (
            current["maximum_incremental_peak_bytes"] <= MAX_NATIVE_INCREMENTAL_BYTES
            and current["maximum_peak_rss_bytes"] <= MAX_NATIVE_TOTAL_BYTES
        )
    else:
        absolute_bound_met = current["maximum_peak_rss_bytes"] <= MAX_PM4PY_TOTAL_BYTES
    expected_flags = {
        "relative_non_regressed": relative_non_regressed,
        "absolute_bound_met": absolute_bound_met,
        "non_regressed": relative_non_regressed and absolute_bound_met,
    }
    if any(value[field] is not expected for field, expected in expected_flags.items()):
        fail(f"{label}: memory non-regression flags differ from raw evidence")
    if not relative_non_regressed:
        fail(f"{label}: matched-release memory regression")
    if not absolute_bound_met:
        fail(f"{label}: absolute RSS bound exceeded")


def _expected_concurrency_orders(
    suite_index: int,
    dataset_index: int,
    path_index: int,
    workload_index: int,
    level_index: int,
) -> list[list[str]]:
    offset = suite_index + dataset_index + path_index + workload_index + level_index
    return [
        list(ORDER_DICTIONARY[(offset + epoch_index) % 2])
        for epoch_index in range(CONCURRENCY_EPOCHS)
    ]


def _decode_concurrency_samples(value: object, label: str) -> list[int]:
    encoded = _keys(value, {"encoding", "count", "data"}, label)
    count = encoded["count"]
    data = encoded["data"]
    if encoded["encoding"] != CONCURRENCY_SAMPLE_ENCODING:
        fail(f"{label}: sample encoding changed")
    if not _positive_int(count) or not isinstance(data, str) or not data:
        fail(f"{label}: invalid encoded sample metadata")
    try:
        compressed = base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError):
        fail(f"{label}: invalid base64 sample payload")
    if base64.b64encode(compressed).decode("ascii") != data:
        fail(f"{label}: non-canonical base64 sample payload")
    expected_bytes = count * 8
    decompressor = zlib.decompressobj()
    try:
        raw = decompressor.decompress(compressed, expected_bytes + 1)
    except (OverflowError, ValueError, zlib.error):
        fail(f"{label}: invalid zlib sample payload")
    if len(raw) > expected_bytes or decompressor.unconsumed_tail:
        fail(f"{label}: decoded sample count differs from metadata")
    try:
        raw += decompressor.flush()
    except zlib.error:
        fail(f"{label}: invalid zlib sample payload")
    if not decompressor.eof or decompressor.unused_data or decompressor.unconsumed_tail:
        fail(f"{label}: incomplete or trailing compressed sample data")
    if len(raw) != expected_bytes:
        fail(f"{label}: decoded sample count differs from metadata")
    samples = [item[0] for item in struct.iter_unpack("<Q", raw)]
    if len(samples) != count or any(not _positive_int(sample) for sample in samples):
        fail(f"{label}: decoded samples must be positive unsigned 64-bit integers")
    return samples


def _validate_concurrency_epoch(
    epoch_raw: object,
    *,
    label: str,
    arm: str,
    epoch_number: int,
    expected_position: int,
    worker_count: int,
    oracle_hash: str,
) -> tuple[dict[str, Any], float, int]:
    fields = {
        "requests",
        "wall_ns",
        "throughput_qps",
        "roundtrip",
        "worker_internal",
        "worker_count",
        "worker_pids",
        "worker_request_counts",
        "workers",
        "answer_sha256",
        "correct",
        "epoch",
        "arm_position",
    }
    epoch = _keys(epoch_raw, fields, label)
    if (
        epoch["epoch"] != epoch_number
        or epoch["arm_position"] != expected_position
        or epoch["worker_count"] != worker_count
        or epoch["answer_sha256"] != oracle_hash
        or epoch["correct"] is not True
    ):
        fail(f"{label}: epoch identity or correctness changed")
    workers = epoch["workers"]
    if not isinstance(workers, list) or len(workers) != worker_count:
        fail(f"{label}: worker evidence count changed")
    raw_roundtrip: list[int] = []
    raw_internal: list[int] = []
    pids: list[int] = []
    counts: list[int] = []
    worker_fields = {
        "worker_pid",
        "roundtrip_samples_ns",
        "internal_samples_ns",
        "answer_sha256_counts",
    }
    for worker_index, worker_raw in enumerate(workers, start=1):
        worker = _keys(worker_raw, worker_fields, f"{label}/worker-{worker_index}")
        pid = worker["worker_pid"]
        roundtrip = _decode_concurrency_samples(
            worker["roundtrip_samples_ns"],
            f"{label}/worker-{worker_index}/roundtrip_samples_ns",
        )
        internal = _decode_concurrency_samples(
            worker["internal_samples_ns"],
            f"{label}/worker-{worker_index}/internal_samples_ns",
        )
        hash_counts = worker["answer_sha256_counts"]
        if not _positive_int(pid):
            fail(f"{label}/worker-{worker_index}: invalid PID")
        if len(roundtrip) < CONCURRENCY_MIN_REQUESTS_PER_WORKER or len(internal) != len(
            roundtrip
        ):
            fail(f"{label}/worker-{worker_index}: invalid raw request evidence")
        if (
            not isinstance(hash_counts, dict)
            or set(hash_counts) != {oracle_hash}
            or type(hash_counts[oracle_hash]) is not int
            or hash_counts[oracle_hash] != len(roundtrip)
        ):
            fail(f"{label}/worker-{worker_index}: invalid answer hash histogram")
        pids.append(pid)
        counts.append(len(roundtrip))
        raw_roundtrip.extend(roundtrip)
        raw_internal.extend(internal)
    if len(set(pids)) != worker_count:
        fail(f"{label}: concurrency workers are not unique processes")
    if epoch["worker_pids"] != sorted(pids):
        fail(f"{label}: worker PID summary differs from raw evidence")
    if epoch["worker_request_counts"] != sorted(counts):
        fail(f"{label}: worker request counts differ from raw evidence")
    if epoch["requests"] != len(raw_roundtrip):
        fail(f"{label}: request total differs from raw evidence")
    wall_ns = epoch["wall_ns"]
    if not _positive_int(wall_ns) or wall_ns < CONCURRENCY_MIN_WALL_NS:
        fail(f"{label}: concurrency duration floor not met")
    exact_qps = len(raw_roundtrip) / (wall_ns / 1_000_000_000)
    if epoch["throughput_qps"] != round(exact_qps, 3):
        fail(f"{label}: QPS differs from requests and wall time")
    expected_roundtrip = _concurrency_metrics_ns(raw_roundtrip)
    expected_internal = _concurrency_metrics_ns(raw_internal)
    if epoch["roundtrip"] != expected_roundtrip:
        fail(f"{label}: round-trip metrics differ from raw samples")
    if epoch["worker_internal"] != expected_internal:
        fail(f"{label}: internal metrics differ from raw samples")
    return epoch, exact_qps, _nearest_rank(raw_roundtrip, 0.95)


def _concurrency_aggregate(epochs: list[dict[str, Any]]) -> dict[str, Any]:
    throughput_values = [epoch["throughput_qps"] for epoch in epochs]
    throughput_mean = statistics.mean(throughput_values)
    return {
        "epoch_count": len(epochs),
        "requests": sum(epoch["requests"] for epoch in epochs),
        "throughput_qps": round(statistics.median(throughput_values), 3),
        "throughput_cv": round(
            statistics.pstdev(throughput_values) / throughput_mean, 6
        ),
        "p50_ms": round(
            statistics.median(epoch["roundtrip"]["p50_ms"] for epoch in epochs), 3
        ),
        "p95_ms": round(
            statistics.median(epoch["roundtrip"]["p95_ms"] for epoch in epochs), 3
        ),
        "p99_ms": round(
            statistics.median(epoch["roundtrip"]["p99_ms"] for epoch in epochs), 3
        ),
        "minimum_requests_per_worker": min(
            min(epoch["worker_request_counts"]) for epoch in epochs
        ),
        "correct": all(epoch["correct"] for epoch in epochs),
        "epochs": epochs,
    }


def validate_concurrency_row(
    row: object,
    *,
    label: str,
    suite: str,
    suite_index: int,
    dataset_index: int,
    path_index: int,
    workload_index: int,
    expected_path: str,
    expected_workload: str,
    oracle_hash: str,
) -> None:
    value = _keys(
        row,
        {"workload", "execution_path", "oracle_answer_sha256", "levels"},
        label,
    )
    if (
        value["workload"] != expected_workload
        or value["execution_path"] != expected_path
        or value["oracle_answer_sha256"] != oracle_hash
    ):
        fail(f"{label}: workload, path, or oracle changed")
    expected_levels = CONCURRENCY_SPECS[suite]["levels"]
    levels = _keys(
        value["levels"], {str(level) for level in expected_levels}, f"{label}/levels"
    )
    for level_index, worker_count in enumerate(expected_levels):
        level_label = f"{label}/x{worker_count}"
        level = _keys(
            levels[str(worker_count)],
            {
                "epoch_arm_orders",
                "prior",
                "current",
                "qps_ratio_current_over_prior",
                "qps_non_regressed",
                "p95_non_regressed",
                "stable",
                "non_regressed",
            },
            level_label,
        )
        expected_orders = _expected_concurrency_orders(
            suite_index,
            dataset_index,
            path_index,
            workload_index,
            level_index,
        )
        if level["epoch_arm_orders"] != expected_orders:
            fail(f"{level_label}: release order is not the locked 2/2 schedule")
        for arm in ARMS:
            aggregate = _keys(
                level[arm],
                {
                    "epoch_count",
                    "requests",
                    "throughput_qps",
                    "throughput_cv",
                    "p50_ms",
                    "p95_ms",
                    "p99_ms",
                    "minimum_requests_per_worker",
                    "correct",
                    "epochs",
                },
                f"{level_label}/{arm}",
            )
            epochs = aggregate["epochs"]
            if not isinstance(epochs, list) or len(epochs) != CONCURRENCY_EPOCHS:
                fail(f"{level_label}/{arm}: expected four concurrency epochs")
            for epoch_index, epoch_raw in enumerate(epochs):
                order = expected_orders[epoch_index]
                _validate_concurrency_epoch(
                    epoch_raw,
                    label=f"{level_label}/{arm}/epoch-{epoch_index + 1}",
                    arm=arm,
                    epoch_number=epoch_index + 1,
                    expected_position=order.index(arm) + 1,
                    worker_count=worker_count,
                    oracle_hash=oracle_hash,
                )
            if aggregate != _concurrency_aggregate(epochs):
                fail(f"{level_label}/{arm}: aggregate differs from raw epochs")
        prior = level["prior"]
        current = level["current"]
        qps_ratio = round(current["throughput_qps"] / prior["throughput_qps"], 3)
        qps_non_regressed = (
            current["throughput_qps"]
            >= prior["throughput_qps"] / CONCURRENCY_QPS_CEILING
        )
        p95_non_regressed = current["p95_ms"] <= max(
            prior["p95_ms"] * CONCURRENCY_P95_CEILING,
            prior["p95_ms"] + CONCURRENCY_P95_ABSOLUTE_SLACK_NS / 1_000_000,
        )
        stable = (
            prior["throughput_cv"] <= MAX_CONCURRENCY_QPS_CV
            and current["throughput_cv"] <= MAX_CONCURRENCY_QPS_CV
        )
        flags = {
            "qps_non_regressed": qps_non_regressed,
            "p95_non_regressed": p95_non_regressed,
            "stable": stable,
            "non_regressed": qps_non_regressed and p95_non_regressed and stable,
        }
        if level["qps_ratio_current_over_prior"] != qps_ratio:
            fail(f"{level_label}: QPS ratio differs from raw epochs")
        if any(level[field] is not expected for field, expected in flags.items()):
            fail(f"{level_label}: concurrency gate flags differ from raw evidence")
        if not stable:
            fail(f"{level_label}: unstable epoch throughput")
        if not qps_non_regressed:
            fail(f"{level_label}: matched-release throughput regression")
        if not p95_non_regressed:
            fail(f"{level_label}: matched-release concurrency p95 regression")


def validate_suite(
    suite_raw: object,
    *,
    suite: str,
    suite_index: int,
) -> tuple[list[float], int, int]:
    suite_value = _keys(suite_raw, {"datasets"}, f"suites/{suite}")
    datasets = suite_value["datasets"]
    if (
        not isinstance(datasets, list)
        or tuple(item.get("dataset") for item in datasets if isinstance(item, dict))
        != DATASETS
    ):
        fail(f"suites/{suite}: dataset set or order changed")
    ratios: list[float] = []
    memory_rows = 0
    concurrency_cells = 0
    expected_latency_order = [
        (path, workload)
        for path in EXECUTION_PATHS[suite]
        for workload in WORKLOADS[suite]
    ]
    expected_concurrency_order = [
        (path, workload)
        for path in EXECUTION_PATHS[suite]
        for workload in CONCURRENCY_SPECS[suite]["workloads"]
    ]
    for dataset_index, dataset_raw in enumerate(datasets):
        dataset_name = DATASETS[dataset_index]
        label = f"suites/{suite}/{dataset_name}"
        dataset = _keys(
            dataset_raw,
            {"dataset", "fixture", "latency", "memory", "concurrency"},
            label,
        )
        fixture = dataset["fixture"]
        fixture_name_field = "name" if suite == "common_pm" else "dataset_name"
        if (
            dataset["dataset"] != dataset_name
            or not isinstance(fixture, dict)
            or fixture.get(fixture_name_field) != dataset_name
        ):
            fail(f"{label}: fixture identity changed")
        latency_rows = dataset["latency"]
        if (
            not isinstance(latency_rows, list)
            or [
                (row.get("execution_path"), row.get("workload"))
                for row in latency_rows
                if isinstance(row, dict)
            ]
            != expected_latency_order
        ):
            fail(f"{label}: latency path/workload set or order changed")
        latency_map: dict[tuple[str, str], dict[str, Any]] = {}
        for row_index, (path, workload) in enumerate(expected_latency_order):
            row_label = f"{label}/latency/{path}/{workload}"
            ratio, _inputs = validate_latency_row(
                latency_rows[row_index],
                label=row_label,
                expected_path=path,
                expected_workload=workload,
            )
            ratios.append(ratio)
            latency_map[(path, workload)] = latency_rows[row_index]
        memory = dataset["memory"]
        if (
            not isinstance(memory, list)
            or [
                (row.get("execution_path"), row.get("workload"))
                for row in memory
                if isinstance(row, dict)
            ]
            != expected_latency_order
        ):
            fail(f"{label}: memory path/workload set or order changed")
        for row_index, (path, workload) in enumerate(expected_latency_order):
            validate_memory_row(
                memory[row_index],
                label=f"{label}/memory/{path}/{workload}",
                expected_path=path,
                expected_workload=workload,
                latency_row=latency_map[(path, workload)],
            )
            memory_rows += 1
        concurrency = dataset["concurrency"]
        if (
            not isinstance(concurrency, list)
            or [
                (row.get("execution_path"), row.get("workload"))
                for row in concurrency
                if isinstance(row, dict)
            ]
            != expected_concurrency_order
        ):
            fail(f"{label}: concurrency path/workload set or order changed")
        for row_index, (path, workload) in enumerate(expected_concurrency_order):
            path_index = EXECUTION_PATHS[suite].index(path)
            workload_index = CONCURRENCY_SPECS[suite]["workloads"].index(workload)
            validate_concurrency_row(
                concurrency[row_index],
                label=f"{label}/concurrency/{path}/{workload}",
                suite=suite,
                suite_index=suite_index,
                dataset_index=dataset_index,
                path_index=path_index,
                workload_index=workload_index,
                expected_path=path,
                expected_workload=workload,
                oracle_hash=latency_map[(path, workload)]["oracle_answer_sha256"],
            )
            concurrency_cells += len(CONCURRENCY_SPECS[suite]["levels"])
    return ratios, memory_rows, concurrency_cells


def _validate_storage_snapshot(snapshot_raw: object, label: str) -> dict[str, Any]:
    snapshot = _keys(
        snapshot_raw,
        {
            "schema",
            "heap_bytes",
            "index_bytes",
            "toast_bytes",
            "total_bytes",
            "other_fork_bytes",
            "relations",
            "indexes",
            "toast_indexes",
        },
        label,
    )
    if snapshot["schema"] != "ocpm" or any(
        not _nonnegative_int(snapshot[field])
        for field in (
            "heap_bytes",
            "index_bytes",
            "toast_bytes",
            "total_bytes",
            "other_fork_bytes",
        )
    ):
        fail(f"{label}: invalid schema totals")
    relations = snapshot["relations"]
    if not isinstance(relations, list) or not relations:
        fail(f"{label}: relation inventory is empty")
    relation_fields = {
        "name",
        "kind",
        "heap_bytes",
        "main_fsm_bytes",
        "main_vm_bytes",
        "index_bytes",
        "toast_bytes",
        "total_bytes",
        "toast",
        "maintenance",
    }
    relation_map: dict[str, dict[str, Any]] = {}
    for index, relation_raw in enumerate(relations):
        relation = _keys(relation_raw, relation_fields, f"{label}/relation-{index + 1}")
        name = relation["name"]
        if (
            not isinstance(name, str)
            or not name
            or name in relation_map
            or relation["kind"] not in ("r", "m", "p")
            or any(
                not _nonnegative_int(relation[field])
                for field in (
                    "heap_bytes",
                    "main_fsm_bytes",
                    "main_vm_bytes",
                    "index_bytes",
                    "toast_bytes",
                    "total_bytes",
                )
            )
            or relation["total_bytes"]
            != (
                relation["heap_bytes"]
                + relation["main_fsm_bytes"]
                + relation["main_vm_bytes"]
                + relation["index_bytes"]
                + relation["toast_bytes"]
            )
        ):
            fail(f"{label}: invalid relation inventory")
        toast = relation["toast"]
        if toast is None:
            if relation["toast_bytes"] != 0:
                fail(f"{label}/{name}: missing TOAST detail for nonzero bytes")
        else:
            toast = _keys(
                toast,
                {
                    "name",
                    "main_bytes",
                    "fsm_bytes",
                    "vm_bytes",
                    "index_bytes",
                    "total_bytes",
                },
                f"{label}/{name}/toast",
            )
            if (
                not isinstance(toast["name"], str)
                or not toast["name"]
                or any(
                    not _nonnegative_int(toast[field])
                    for field in (
                        "main_bytes",
                        "fsm_bytes",
                        "vm_bytes",
                        "index_bytes",
                        "total_bytes",
                    )
                )
                or toast["total_bytes"]
                != (
                    toast["main_bytes"]
                    + toast["fsm_bytes"]
                    + toast["vm_bytes"]
                    + toast["index_bytes"]
                )
                or toast["total_bytes"] != relation["toast_bytes"]
            ):
                fail(f"{label}/{name}: invalid TOAST accounting")
        maintenance = _keys(
            relation["maintenance"],
            {
                "last_vacuum",
                "last_autovacuum",
                "vacuum_count",
                "autovacuum_count",
            },
            f"{label}/{name}/maintenance",
        )
        if any(
            maintenance[field] is not None
            and (not isinstance(maintenance[field], str) or not maintenance[field])
            for field in ("last_vacuum", "last_autovacuum")
        ) or any(
            not _nonnegative_int(maintenance[field])
            for field in ("vacuum_count", "autovacuum_count")
        ):
            fail(f"{label}/{name}: invalid maintenance evidence")
        relation_map[name] = relation
    if list(relation_map) != sorted(relation_map):
        fail(f"{label}: relation inventory is not stably ordered")
    for field in ("heap_bytes", "index_bytes", "toast_bytes", "total_bytes"):
        if snapshot[field] != sum(relation[field] for relation in relations):
            fail(f"{label}: {field} differs from relation inventory")
    expected_other = sum(
        relation["main_fsm_bytes"] + relation["main_vm_bytes"] for relation in relations
    )
    if snapshot["other_fork_bytes"] != expected_other:
        fail(f"{label}: other_fork_bytes differs from relation inventory")
    indexes = snapshot["indexes"]
    if not isinstance(indexes, list):
        fail(f"{label}: index inventory is not a list")
    index_fields = {"table", "name", "definition", "bytes"}
    seen_names: set[str] = set()
    per_table: dict[str, int] = {name: 0 for name in relation_map}
    ordering: list[tuple[str, str]] = []
    for index, index_raw in enumerate(indexes):
        item = _keys(index_raw, index_fields, f"{label}/index-{index + 1}")
        table = item["table"]
        name = item["name"]
        if (
            table not in relation_map
            or not isinstance(name, str)
            or not name
            or name in seen_names
            or not isinstance(item["definition"], str)
            or not item["definition"].startswith("CREATE")
            or not _nonnegative_int(item["bytes"])
        ):
            fail(f"{label}: invalid index inventory")
        seen_names.add(name)
        per_table[table] += item["bytes"]
        ordering.append((table, name))
    if ordering != sorted(ordering):
        fail(f"{label}: index inventory is not stably ordered")
    if any(
        per_table[name] != relation_map[name]["index_bytes"] for name in relation_map
    ):
        fail(f"{label}: index bytes differ from owning relation totals")

    toast_indexes = snapshot["toast_indexes"]
    if not isinstance(toast_indexes, list):
        fail(f"{label}: TOAST index inventory is not a list")
    toast_index_fields = {"table", "toast_table", "name", "bytes"}
    seen_toast_names: set[str] = set()
    per_table_toast: dict[str, int] = {name: 0 for name in relation_map}
    toast_ordering: list[tuple[str, str]] = []
    for index, index_raw in enumerate(toast_indexes):
        item = _keys(index_raw, toast_index_fields, f"{label}/toast-index-{index + 1}")
        table = item["table"]
        name = item["name"]
        owner = relation_map.get(table)
        toast = None if owner is None else owner["toast"]
        if (
            toast is None
            or item["toast_table"] != toast["name"]
            or not isinstance(name, str)
            or not name
            or name in seen_toast_names
            or not _nonnegative_int(item["bytes"])
        ):
            fail(f"{label}: invalid TOAST index inventory")
        seen_toast_names.add(name)
        per_table_toast[table] += item["bytes"]
        toast_ordering.append((table, name))
    if toast_ordering != sorted(toast_ordering):
        fail(f"{label}: TOAST index inventory is not stably ordered")
    if any(
        per_table_toast[name]
        != (0 if relation["toast"] is None else relation["toast"]["index_bytes"])
        for name, relation in relation_map.items()
    ):
        fail(f"{label}: TOAST index bytes differ from owning TOAST totals")
    return snapshot


def _storage_structure(
    snapshot: dict[str, Any],
) -> tuple[
    list[tuple[str, str, str | None]],
    list[tuple[str, str, str]],
    list[tuple[str, str, str]],
]:
    return (
        [
            (
                row["name"],
                row["kind"],
                None if row["toast"] is None else row["toast"]["name"],
            )
            for row in snapshot["relations"]
        ],
        [(row["table"], row["name"], row["definition"]) for row in snapshot["indexes"]],
        [
            (row["table"], row["toast_table"], row["name"])
            for row in snapshot["toast_indexes"]
        ],
    )


def validate_storage(storage: object) -> int:
    value = _keys(
        storage,
        {"structural_before_workloads", "diagnostic_after_workloads"},
        "storage",
    )
    before = _keys(
        value["structural_before_workloads"], set(ARMS), "storage/structural_before"
    )
    after = _keys(
        value["diagnostic_after_workloads"], set(ARMS), "storage/diagnostic_after"
    )
    checked_before: dict[str, dict[str, Any]] = {}
    for arm in ARMS:
        checked_before[arm] = _validate_storage_snapshot(
            before[arm], f"storage/structural_before/{arm}"
        )
        checked_after = _validate_storage_snapshot(
            after[arm], f"storage/diagnostic_after/{arm}"
        )
        if _storage_structure(checked_before[arm]) != _storage_structure(checked_after):
            fail(f"storage/{arm}: relation or index structure changed during workloads")
    for metric in ("index_bytes", "total_bytes"):
        prior = checked_before["prior"][metric]
        current = checked_before["current"][metric]
        if current * STORAGE_CEILING_DENOMINATOR > prior * STORAGE_CEILING_NUMERATOR:
            fail(f"storage: matched-release {metric} regression")
    return 2


def validate_contract(result: dict[str, Any], *, allow_dirty: bool) -> dict[str, int]:
    top_fields = {
        "schema_version",
        "artifact_type",
        "generated_at",
        "source",
        "releases",
        "environment",
        "provenance",
        "method",
        "suites",
        "storage",
        "summary",
        "payload_sha256",
    }
    if set(result) != top_fields:
        fail("artifact fields changed")
    if result.get("schema_version") != SCHEMA_VERSION:
        fail("unexpected release bridge schema version")
    if result.get("artifact_type") != ARTIFACT_TYPE:
        fail("unexpected release bridge artifact type")
    if not isinstance(result.get("generated_at"), str) or not result["generated_at"]:
        fail("release bridge generation timestamp missing")
    validate_source(result.get("source"))
    validate_releases(result.get("releases"))
    provenance = result.get("provenance")
    validate_provenance(provenance, allow_dirty=allow_dirty)
    validate_environment(result.get("environment"), provenance)
    validate_method(result.get("method"))
    suites = _keys(result.get("suites"), set(SUITES), "suites")
    ratios: list[float] = []
    memory_rows = 0
    concurrency_cells = 0
    for suite_index, suite in enumerate(SUITES):
        suite_ratios, suite_memory, suite_concurrency = validate_suite(
            suites[suite], suite=suite, suite_index=suite_index
        )
        ratios.extend(suite_ratios)
        memory_rows += suite_memory
        concurrency_cells += suite_concurrency
    storage_non_regressed = validate_storage(result.get("storage"))
    expected_latency = sum(
        len(DATASETS) * len(WORKLOADS[suite]) * len(EXECUTION_PATHS[suite])
        for suite in SUITES
    )
    summary = _keys(
        result.get("summary"),
        {
            "total_latency_workloads",
            "correct_latency_workloads",
            "minimum_p50_ratio_prior_over_current",
            "latency_non_regressed",
            "memory_non_regressed",
            "concurrency_levels_non_regressed",
            "storage_non_regressed",
            "target_met",
        },
        "summary",
    )
    expected_summary = {
        "total_latency_workloads": expected_latency,
        "correct_latency_workloads": expected_latency,
        "minimum_p50_ratio_prior_over_current": round(min(ratios), 3),
        "latency_non_regressed": expected_latency,
        "memory_non_regressed": memory_rows,
        "concurrency_levels_non_regressed": concurrency_cells,
        "storage_non_regressed": storage_non_regressed,
        "target_met": True,
    }
    if summary != expected_summary:
        fail("summary differs from independently recomputed evidence")
    return {
        "latency_rows": expected_latency,
        "memory_rows": memory_rows,
        "concurrency_cells": concurrency_cells,
    }


def _validate_public_current_provenance(
    public_result: dict[str, Any], bridge_result: dict[str, Any], *, allow_dirty: bool
) -> None:
    public_provenance = public_result.get("provenance")
    if not isinstance(public_provenance, dict):
        fail("public result provenance is missing")
    bridge_provenance = bridge_result["provenance"]
    controller_revision = public_provenance.get("controller_source_revision")
    if (
        not isinstance(controller_revision, str)
        or REVISION.fullmatch(controller_revision) is None
    ):
        fail("public result controller revision is invalid")
    if controller_revision != bridge_provenance["controller_source_revision"]:
        fail("public result and bridge controller revisions differ")
    for public_field, bridge_field in (
        ("ocpm_engine_source_revision", "current_engine_source_revision"),
        ("pg_ocpm_source_revision", "current_pg_ocpm_source_revision"),
    ):
        if public_provenance.get(public_field) != bridge_provenance[bridge_field]:
            fail("public result and bridge current release revisions differ")
    for field in (
        "controller_source_tree_clean",
        "ocpm_engine_source_tree_clean",
        "pg_ocpm_source_tree_clean",
    ):
        clean = public_provenance.get(field)
        if type(clean) is not bool:
            fail("public result source cleanliness evidence is invalid")
        if not allow_dirty and clean is not True:
            fail("release verification requires clean public result sources")


def _bridge_latency_rows(
    bridge_result: dict[str, Any], suite: str
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]]]:
    datasets = bridge_result["suites"][suite]["datasets"]
    fixtures = {dataset["dataset"]: dataset["fixture"] for dataset in datasets}
    rows = {
        (dataset["dataset"], row["execution_path"], row["workload"]): row
        for dataset in datasets
        for row in dataset["latency"]
    }
    return fixtures, rows


def _public_common_concurrency_hash(
    public_result: dict[str, Any], section_name: str
) -> tuple[str, str, str]:
    section = public_result.get(section_name)
    if not isinstance(section, dict):
        fail(f"public common-PM {section_name} evidence is missing")
    dataset = section.get("fixture")
    workload = section.get("workload")
    levels = section.get("levels")
    candidate = section.get("pg_ocpm_rust")
    if (
        dataset not in DATASETS
        or workload not in WORKLOADS["common_pm"]
        or levels != [str(level) for level in CONCURRENCY_SPECS["common_pm"]["levels"]]
        or not isinstance(candidate, dict)
        or set(candidate) != set(levels)
    ):
        fail(f"public common-PM {section_name} correspondence changed")
    hashes: set[object] = set()
    for level in levels:
        aggregate = candidate[level]
        if not isinstance(aggregate, dict):
            fail(f"public common-PM {section_name} correspondence changed")
        epochs = aggregate.get("epochs")
        if (
            not isinstance(epochs, list)
            or not epochs
            or any(not isinstance(epoch, dict) for epoch in epochs)
        ):
            fail(f"public common-PM {section_name} answer evidence is missing")
        hashes.update(epoch.get("answer_sha256") for epoch in epochs)
    if len(hashes) != 1 or not _hex64(next(iter(hashes), None)):
        fail(f"public common-PM {section_name} answer hashes are invalid")
    return dataset, workload, hashes.pop()


def validate_for_public_common(
    public_result: dict[str, Any],
    bridge_result: dict[str, Any],
    *,
    allow_dirty: bool,
) -> None:
    """Bind a public common-PM artifact to the checked current release arm."""

    validate_contract(bridge_result, allow_dirty=allow_dirty)
    if public_result.get("release") != {
        "ocpm_engine": EXPECTED_RELEASES["current"]["ocpm_engine"],
        "pg_ocpm": EXPECTED_RELEASES["current"]["pg_ocpm"],
    }:
        fail("public common-PM current release versions changed")
    if public_result.get("source") != EXPECTED_SOURCE:
        fail("public common-PM and bridge SAP sources differ")
    _validate_public_current_provenance(
        public_result, bridge_result, allow_dirty=allow_dirty
    )
    bridge_fixtures, bridge_rows = _bridge_latency_rows(bridge_result, "common_pm")
    public_datasets = public_result.get("datasets")
    if (
        not isinstance(public_datasets, list)
        or tuple(
            dataset.get("fixture", {}).get("name")
            for dataset in public_datasets
            if isinstance(dataset, dict)
        )
        != DATASETS
    ):
        fail("public common-PM SAP dataset set or order changed")
    for dataset in public_datasets:
        name = dataset["fixture"]["name"]
        if dataset["fixture"] != bridge_fixtures[name]:
            fail(f"{name}: public common-PM and bridge fixtures differ")
        public_rows = dataset.get("workloads")
        if (
            not isinstance(public_rows, list)
            or any(not isinstance(row, dict) for row in public_rows)
            or tuple(row.get("workload") for row in public_rows)
            != WORKLOADS["common_pm"]
        ):
            fail(f"{name}: public common-PM workload correspondence changed")
        for public_row in public_rows:
            workload = public_row["workload"]
            bridge_row = bridge_rows.get((name, "pg_ocpm_rust", workload))
            if (
                bridge_row is None
                or public_row.get("correct") is not True
                or bridge_row["correct"] is not True
                or bridge_row["non_regressed"] is not True
                or not isinstance(public_row.get("pg_ocpm_rust"), dict)
            ):
                fail(
                    f"{name}/{workload}: public current-path row does not match bridge"
                )
    for section_name in ("concurrency", "drift_concurrency"):
        dataset, workload, answer_hash = _public_common_concurrency_hash(
            public_result, section_name
        )
        bridge_hash = bridge_rows[(dataset, "pg_ocpm_rust", workload)][
            "oracle_answer_sha256"
        ]
        if answer_hash != bridge_hash:
            fail(f"public common-PM {section_name} answer differs from bridge")


def validate_for_public_pm4py(
    public_result: dict[str, Any],
    bridge_result: dict[str, Any],
    *,
    allow_dirty: bool,
) -> None:
    """Bind a public PM4Py artifact to both checked current execution paths."""

    validate_contract(bridge_result, allow_dirty=allow_dirty)
    client = public_result.get("environment", {}).get("client", {})
    database = public_result.get("environment", {}).get("database", {})
    if (
        client.get("ocpm_engine_version") != EXPECTED_RELEASES["current"]["ocpm_engine"]
        or database.get("pg_ocpm", {}).get("pg_ocpm_version")
        != EXPECTED_RELEASES["current"]["pg_ocpm"]
    ):
        fail("public PM4Py current release versions changed")
    source = public_result.get("source")
    if (
        not isinstance(source, dict)
        or source.get("doi") != EXPECTED_SOURCE["doi"]
        or source.get("license") != EXPECTED_SOURCE["license"]
        or source.get("datasets") != list(DATASETS)
    ):
        fail("public PM4Py and bridge SAP sources differ")
    _validate_public_current_provenance(
        public_result, bridge_result, allow_dirty=allow_dirty
    )
    bridge_fixtures, bridge_rows = _bridge_latency_rows(bridge_result, "pm4py")
    public_datasets = public_result.get("datasets")
    if (
        not isinstance(public_datasets, list)
        or tuple(
            dataset.get("dataset")
            for dataset in public_datasets
            if isinstance(dataset, dict)
        )
        != DATASETS
    ):
        fail("public PM4Py SAP dataset set or order changed")
    for dataset in public_datasets:
        name = dataset["dataset"]
        if dataset.get("fixture") != bridge_fixtures[name]:
            fail(f"{name}: public PM4Py and bridge fixtures differ")
        public_rows = dataset.get("latency")
        if (
            not isinstance(public_rows, list)
            or any(not isinstance(row, dict) for row in public_rows)
            or tuple(row.get("workload") for row in public_rows) != WORKLOADS["pm4py"]
        ):
            fail(f"{name}: public PM4Py workload correspondence changed")
        memory = dataset.get("memory")
        if not isinstance(memory, dict) or tuple(memory) != WORKLOADS["pm4py"]:
            fail(f"{name}: public PM4Py memory correspondence changed")
        for public_row in public_rows:
            workload = public_row["workload"]
            answer_hash = public_row.get("answer_sha256")
            if public_row.get("correct") is not True or not _hex64(answer_hash):
                fail(f"{name}/{workload}: public PM4Py correctness evidence is invalid")
            for path in EXECUTION_PATHS["pm4py"]:
                bridge_row = bridge_rows.get((name, path, workload))
                public_metrics = public_row.get(path)
                workload_memory = memory[workload]
                public_memory = (
                    workload_memory.get(path)
                    if isinstance(workload_memory, dict)
                    else None
                )
                if (
                    bridge_row is None
                    or not isinstance(public_metrics, dict)
                    or not isinstance(public_memory, dict)
                    or bridge_row["oracle_answer_sha256"] != answer_hash
                    or bridge_row["input_evidence"]["current"]
                    != public_metrics.get("input")
                    or public_memory.get("answer_sha256") != answer_hash
                    or public_memory.get("input") != public_metrics.get("input")
                ):
                    fail(
                        f"{name}/{path}/{workload}: public current-path evidence "
                        "differs from bridge"
                    )


def main() -> None:
    args = parse_args()
    if args.preview and args.expected_payload_sha256:
        fail("--preview and --expected-payload-sha256 are mutually exclusive")
    result = load_verified(args.result, args.expected_payload_sha256)
    counts = validate_contract(result, allow_dirty=args.preview)
    print(
        "SAP unified release bridge verified: "
        f"{counts['latency_rows']} latency rows, "
        f"{counts['memory_rows']} fresh-memory rows, and "
        f"{counts['concurrency_cells']} concurrency level cells; "
        "correctness, performance, storage, provenance, and digest gates passed"
    )


if __name__ == "__main__":
    main()
