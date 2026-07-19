from __future__ import annotations

import copy
import json
import statistics
from pathlib import Path

import pytest

from benchmarks import check_sap_release_bridge as checker

ROOT = Path(__file__).resolve().parents[1]


def aggregate(epochs: list[dict], arm: str) -> dict:
    samples = [
        sample for epoch in epochs for sample in epoch["arms"][arm]["samples_ns"]
    ]
    p95_values = [epoch["arms"][arm]["p95_ms"] for epoch in epochs]
    position_samples = {"first": [], "second": []}
    for epoch in epochs:
        for order_code, sample in zip(
            epoch["order_codes"], epoch["arms"][arm]["samples_ns"]
        ):
            position = (
                "first" if checker.ORDER_DICTIONARY[order_code][0] == arm else "second"
            )
            position_samples[position].append(sample)
    return {
        **checker.serial_metrics_ns(samples),
        "exact_samples": 90,
        "epoch_count": 3,
        "epoch_p95_median_ms": round(statistics.median(p95_values), 3),
        "epoch_p95_range_ms": [min(p95_values), max(p95_values)],
        "position_stratified": {
            position: {
                **checker.serial_metrics_ns(samples),
                "exact_samples": len(samples),
            }
            for position, samples in position_samples.items()
        },
    }


def make_row(workload: str, prior_ns: int = 1_000_000, current_ns: int = 1_050_000):
    answer = "a" * 64
    epochs = []
    for epoch_number in range(1, 4):
        arms = {}
        for arm, value in (("prior", prior_ns), ("current", current_ns)):
            samples = [value] * 30
            arms[arm] = {
                **checker.serial_metrics_ns(samples),
                "samples_ns": samples,
                "answer_sha256s": [answer] * 30,
            }
        epochs.append(
            {
                "epoch": epoch_number,
                "order_codes": [0, 1] * 15,
                "arms": arms,
            }
        )
    ratio = round(prior_ns / current_ns, 3)
    return {
        "workload": workload,
        "oracle_answer_sha256": answer,
        "correct": True,
        "prior": aggregate(epochs, "prior"),
        "current": aggregate(epochs, "current"),
        "p50_ratio_prior_over_current": ratio,
        "first_execution_counts": {"prior": 45, "current": 45},
        "warmup_order_codes": [0, 1] * 5,
        "serial_epochs": epochs,
    }


def make_artifact(prior_ns: int = 1_000_000, current_ns: int = 1_050_000):
    fixtures = json.loads(checker.FIXTURE_BASELINE.read_text())["datasets"]
    datasets = []
    ratios = []
    for fixture_row in fixtures:
        workloads = [
            make_row(workload, prior_ns, current_ns)
            for workload in checker.EXPECTED_WORKLOADS
        ]
        ratios.extend(row["p50_ratio_prior_over_current"] for row in workloads)
        datasets.append({"fixture": fixture_row["fixture"], "workloads": workloads})
    database = {
        "postgres_version": "16.14",
        "shared_buffers": "1GB",
        "effective_cache_size": "4GB",
        "work_mem": "16MB",
        "maintenance_work_mem": "1GB",
        "max_parallel_workers_per_gather": "4",
        "random_page_cost": "1.1",
        "jit": "off",
    }
    releases = {
        arm: {
            **release,
            "worker_ocpm_engine": release["ocpm_engine"],
            "worker_pg_ocpm": release["pg_ocpm"],
        }
        for arm, release in checker.EXPECTED_RELEASES.items()
    }
    artifact = {
        "schema_version": 1,
        "artifact_type": checker.ARTIFACT_TYPE,
        "generated_at": "2026-07-19T00:00:00+00:00",
        "source": checker.EXPECTED_SOURCE,
        "releases": releases,
        "environment": {
            "client": {
                "python": "3.11.15",
                "platform": "Linux-test",
                "machine": "aarch64",
                "logical_cpus_visible": 18,
            },
            "vanilla_postgres": database,
            "prior_postgres": database,
            "current_postgres": database,
        },
        "provenance": {
            "benchmark_host_id": "sha256:" + "1" * 64,
            "client_image_id": "sha256:" + "2" * 64,
            "vanilla_database_image_id": "sha256:" + "3" * 64,
            "prior_database_image_id": "sha256:" + "4" * 64,
            "current_database_image_id": "sha256:" + "5" * 64,
            "controller_source_revision": "b" * 40,
            "controller_source_tree_clean": True,
            "prior_engine_source_revision": checker.EXPECTED_RELEASES["prior"][
                "ocpm_engine_revision"
            ],
            "prior_engine_source_tree_clean": True,
            "prior_pg_ocpm_source_revision": checker.EXPECTED_RELEASES["prior"][
                "pg_ocpm_revision"
            ],
            "prior_pg_ocpm_source_tree_clean": True,
            "current_engine_source_revision": checker.EXPECTED_RELEASES["current"][
                "ocpm_engine_revision"
            ],
            "current_engine_source_tree_clean": True,
            "current_pg_ocpm_source_revision": checker.EXPECTED_RELEASES["current"][
                "pg_ocpm_revision"
            ],
            "current_pg_ocpm_source_tree_clean": True,
            "harness_sha256": checker.EXPECTED_HARNESS_SHA256,
            "loader_sha256": checker.EXPECTED_LOADER_SHA256,
        },
        "method": {
            "warmup_rounds": 10,
            "latency_epochs": 3,
            "samples_per_epoch": 30,
            "total_samples_per_arm": 90,
            "clock": "time.perf_counter_ns",
            "random_seed": 20260718,
            "order_dictionary": [["prior", "current"], ["current", "prior"]],
            "warmup_order_counts": {"0": 5, "1": 5},
            "epoch_order_counts": {"0": 15, "1": 15},
            "timing_scope": "database extraction plus construction and scoring",
            "ipc_excluded": True,
            "oracle": "untimed vanilla PostgreSQL reference",
            "correctness_gate": "every answer hash equals the oracle",
            "storage": "PostgreSQL schema bytes",
        },
        "datasets": datasets,
        "storage": {
            "prior_pg_ocpm": {
                "heap_bytes": 40_000_000,
                "index_bytes": 20_000_000,
                "toast_bytes": 30_000_000,
                "total_bytes": 100_000_000,
            },
            "current_pg_ocpm": {
                "heap_bytes": 40_000_000,
                "index_bytes": 20_000_000,
                "toast_bytes": 30_000_000,
                "total_bytes": 100_000_000,
            },
        },
        "summary": {
            "total_workloads": 18,
            "correct_workloads": 18,
            "non_regressed_workloads": 18,
            "minimum_p50_ratio_prior_over_current": min(ratios),
            "target_met": True,
        },
        "payload_sha256": "0" * 64,
    }
    return artifact


def refresh_row(row: dict) -> None:
    for arm in checker.ARMS:
        for epoch in row["serial_epochs"]:
            evidence = epoch["arms"][arm]
            metrics = checker.serial_metrics_ns(evidence["samples_ns"])
            evidence.update(metrics)
        row[arm] = aggregate(row["serial_epochs"], arm)
    prior = statistics.median(
        sample
        for epoch in row["serial_epochs"]
        for sample in epoch["arms"]["prior"]["samples_ns"]
    )
    current = statistics.median(
        sample
        for epoch in row["serial_epochs"]
        for sample in epoch["arms"]["current"]["samples_ns"]
    )
    row["p50_ratio_prior_over_current"] = round(prior / current, 3)


def refresh_summary(artifact: dict) -> None:
    ratios = [
        row["p50_ratio_prior_over_current"]
        for dataset in artifact["datasets"]
        for row in dataset["workloads"]
    ]
    artifact["summary"]["minimum_p50_ratio_prior_over_current"] = min(ratios)


def test_valid_counterbalanced_bridge_passes() -> None:
    rows = checker.validate_contract(make_artifact(), allow_dirty=False)
    assert len(rows) == 18


def test_bridge_rejects_sixteen_fourteen_epoch() -> None:
    artifact = make_artifact()
    artifact["datasets"][0]["workloads"][0]["serial_epochs"][0]["order_codes"] = [
        0
    ] * 16 + [1] * 14
    with pytest.raises(SystemExit, match="not exactly 15/15"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_bridge_rejects_forged_aggregate_metric() -> None:
    artifact = make_artifact()
    artifact["datasets"][0]["workloads"][0]["current"]["p50_ms"] += 0.001
    with pytest.raises(SystemExit, match="aggregate metrics"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_bridge_rejects_forged_position_stratification() -> None:
    artifact = make_artifact()
    artifact["datasets"][0]["workloads"][0]["current"]["position_stratified"]["first"][
        "p50_ms"
    ] += 0.001
    with pytest.raises(SystemExit, match="aggregate metrics"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_bridge_rejects_invalid_raw_latency() -> None:
    artifact = make_artifact()
    artifact["datasets"][0]["workloads"][0]["serial_epochs"][0]["arms"]["current"][
        "samples_ns"
    ][0] = 0
    with pytest.raises(SystemExit, match="invalid raw nanosecond"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_bridge_rejects_answer_hash_tampering() -> None:
    artifact = make_artifact()
    artifact["datasets"][0]["workloads"][0]["serial_epochs"][0]["arms"]["current"][
        "answer_sha256s"
    ][0] = "b" * 64
    with pytest.raises(SystemExit, match="differs from the oracle"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_bridge_rejects_wrong_source_revision() -> None:
    artifact = make_artifact()
    artifact["provenance"]["prior_pg_ocpm_source_revision"] = "0" * 40
    with pytest.raises(SystemExit, match="unexpected bridge source revision"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_bridge_rejects_wrong_harness_hash() -> None:
    artifact = make_artifact()
    artifact["provenance"]["harness_sha256"] = {
        **checker.EXPECTED_HARNESS_SHA256,
        "worker": "0" * 64,
    }
    with pytest.raises(SystemExit, match="unexpected bridge harness source hashes"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_preview_still_rejects_dirty_measured_arm() -> None:
    artifact = make_artifact()
    artifact["provenance"]["current_engine_source_tree_clean"] = False
    with pytest.raises(SystemExit, match="measured-arm source trees must be clean"):
        checker.validate_contract(artifact, allow_dirty=True)


def test_preview_permits_only_dirty_controller() -> None:
    artifact = make_artifact()
    artifact["provenance"]["controller_source_tree_clean"] = False
    checker.validate_contract(artifact, allow_dirty=True)
    with pytest.raises(SystemExit, match="clean controller"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_bridge_rejects_fixture_change() -> None:
    artifact = make_artifact()
    artifact["datasets"][0]["fixture"]["object_type"] = "changed"
    with pytest.raises(SystemExit, match="differs from the committed SAP fixture"):
        checker.validate_contract(artifact, allow_dirty=False)


def test_latency_threshold_accepts_boundary_and_rejects_one_ns_above() -> None:
    at_boundary = make_artifact(prior_ns=1_000_000, current_ns=1_100_000)
    checker.validate_contract(at_boundary, allow_dirty=False)

    above = make_artifact(prior_ns=1_000_000, current_ns=1_100_001)
    with pytest.raises(SystemExit, match="18 regressed SAP workloads"):
        checker.validate_contract(above, allow_dirty=False)


def test_storage_threshold_accepts_boundary_and_rejects_one_byte_above() -> None:
    at_boundary = make_artifact()
    at_boundary["storage"]["current_pg_ocpm"]["total_bytes"] = 101_000_000
    checker.validate_contract(at_boundary, allow_dirty=False)

    above = copy.deepcopy(at_boundary)
    above["storage"]["current_pg_ocpm"]["total_bytes"] += 1
    with pytest.raises(SystemExit, match="storage regression"):
        checker.validate_contract(above, allow_dirty=False)
