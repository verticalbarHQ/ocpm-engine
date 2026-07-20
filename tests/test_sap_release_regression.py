from __future__ import annotations

import copy
import hashlib
import itertools
import json
from pathlib import Path
from typing import Any, Callable

import pytest

from benchmarks import check_sap_release_regression as checker

ANSWER_HASH = "a" * 64
IMAGE_ID = "sha256:" + "b" * 64
_PIDS: itertools.count[int]


def sign(payload: dict[str, Any]) -> dict[str, Any]:
    payload.pop("payload_sha256", None)
    encoded = json.dumps(payload, indent=2, default=str) + "\n"
    payload["payload_sha256"] = hashlib.sha256(encoded.encode()).hexdigest()
    return payload


def latency_row(path: str, workload: str) -> dict[str, Any]:
    epochs = []
    for epoch_number in range(1, checker.LATENCY_EPOCHS + 1):
        order_codes = [0] * 15 + [1] * 15
        arms = {}
        for arm in checker.ARMS:
            samples = [1_000_000] * checker.LATENCY_SAMPLES_PER_EPOCH
            hashes = [ANSWER_HASH] * checker.LATENCY_SAMPLES_PER_EPOCH
            arms[arm] = {
                **checker._serial_metrics_ns(samples),
                "samples_ns": samples,
                "answer_sha256s": hashes,
            }
        epochs.append({"epoch": epoch_number, "order_codes": order_codes, "arms": arms})
    prior, prior_samples = checker._latency_aggregate(epochs, "prior")
    current, current_samples = checker._latency_aggregate(epochs, "current")
    input_shape = {"execution_path": path, "aggregate_rows": 1}
    return {
        "workload": workload,
        "execution_path": path,
        "oracle_answer_sha256": ANSWER_HASH,
        "input_evidence": {
            "prior": copy.deepcopy(input_shape),
            "current": copy.deepcopy(input_shape),
        },
        "correct": True,
        "prior": prior,
        "current": current,
        "p50_ratio_prior_over_current": round(
            checker.statistics.median(prior_samples)
            / checker.statistics.median(current_samples),
            3,
        ),
        "non_regressed": True,
        "first_execution_counts": {"prior": 45, "current": 45},
        "warmup_order_codes": [0] * 5 + [1] * 5,
        "serial_epochs": epochs,
    }


def memory_row(path: str, workload: str, latency: dict[str, Any]) -> dict[str, Any]:
    order_codes = [0, 1, 0, 1]
    arms: dict[str, dict[str, Any]] = {}
    for arm in checker.ARMS:
        samples = []
        for sample_number, code in enumerate(order_codes, start=1):
            baseline = 40 * 1024 * 1024
            peak = 41 * 1024 * 1024
            samples.append(
                {
                    "sample": sample_number,
                    "arm_position": checker.ORDER_DICTIONARY[code].index(arm) + 1,
                    "worker_pid": next(_PIDS),
                    "answer_sha256": ANSWER_HASH,
                    "execution_path": path,
                    "input": copy.deepcopy(latency["input_evidence"][arm]),
                    "baseline_rss_bytes": baseline,
                    "peak_rss_bytes": peak,
                    "incremental_peak_bytes": peak - baseline,
                    "elapsed_ns": 1_000_000,
                }
            )
        arms[arm] = checker._memory_aggregate(samples)
    return {
        "workload": workload,
        "execution_path": path,
        "oracle_answer_sha256": ANSWER_HASH,
        "order_codes": order_codes,
        **arms,
        "relative_non_regressed": True,
        "absolute_bound_met": True,
        "non_regressed": True,
    }


def concurrency_epoch(
    *, worker_count: int, epoch_number: int, arm_position: int
) -> dict[str, Any]:
    workers = []
    for _ in range(worker_count):
        workers.append(
            {
                "worker_pid": next(_PIDS),
                "roundtrip_samples_ns": [10_000_000]
                * checker.CONCURRENCY_MIN_REQUESTS_PER_WORKER,
                "internal_samples_ns": [9_000_000]
                * checker.CONCURRENCY_MIN_REQUESTS_PER_WORKER,
                "answer_sha256s": [ANSWER_HASH]
                * checker.CONCURRENCY_MIN_REQUESTS_PER_WORKER,
            }
        )
    roundtrip = [
        sample for worker in workers for sample in worker["roundtrip_samples_ns"]
    ]
    internal = [
        sample for worker in workers for sample in worker["internal_samples_ns"]
    ]
    wall_ns = checker.CONCURRENCY_MIN_WALL_NS
    requests = len(roundtrip)
    counts = [len(worker["roundtrip_samples_ns"]) for worker in workers]
    pids = [worker["worker_pid"] for worker in workers]
    return {
        "requests": requests,
        "wall_ns": wall_ns,
        "throughput_qps": round(requests / (wall_ns / 1_000_000_000), 3),
        "roundtrip": checker._concurrency_metrics_ns(roundtrip),
        "worker_internal": checker._concurrency_metrics_ns(internal),
        "worker_count": worker_count,
        "worker_pids": sorted(pids),
        "worker_request_counts": sorted(counts),
        "workers": workers,
        "answer_sha256": ANSWER_HASH,
        "correct": True,
        "epoch": epoch_number,
        "arm_position": arm_position,
    }


def concurrency_row(
    *,
    suite: str,
    suite_index: int,
    dataset_index: int,
    path: str,
    path_index: int,
    workload: str,
    workload_index: int,
) -> dict[str, Any]:
    levels = {}
    for level_index, worker_count in enumerate(
        checker.CONCURRENCY_SPECS[suite]["levels"]
    ):
        orders = checker._expected_concurrency_orders(
            suite_index,
            dataset_index,
            path_index,
            workload_index,
            level_index,
        )
        arms = {}
        for arm in checker.ARMS:
            epochs = [
                concurrency_epoch(
                    worker_count=worker_count,
                    epoch_number=epoch_index + 1,
                    arm_position=order.index(arm) + 1,
                )
                for epoch_index, order in enumerate(orders)
            ]
            arms[arm] = checker._concurrency_aggregate(epochs)
        levels[str(worker_count)] = {
            "epoch_arm_orders": orders,
            **arms,
            "qps_ratio_current_over_prior": 1.0,
            "qps_non_regressed": True,
            "p95_non_regressed": True,
            "stable": True,
            "non_regressed": True,
        }
    return {
        "workload": workload,
        "execution_path": path,
        "oracle_answer_sha256": ANSWER_HASH,
        "levels": levels,
    }


def storage_snapshot(total_bytes: int = 100_000_000) -> dict[str, Any]:
    heap = 50_000_000
    indexes = 10_000_000
    toast = 10_000_000
    fsm = 20_000_000
    vm = total_bytes - heap - indexes - toast - fsm
    return {
        "schema": "ocpm",
        "heap_bytes": heap,
        "index_bytes": indexes,
        "toast_bytes": toast,
        "total_bytes": total_bytes,
        "other_fork_bytes": fsm + vm,
        "relations": [
            {
                "name": "event",
                "kind": "r",
                "heap_bytes": heap,
                "main_fsm_bytes": fsm,
                "main_vm_bytes": vm,
                "index_bytes": indexes,
                "toast_bytes": toast,
                "total_bytes": total_bytes,
                "toast": {
                    "name": "pg_toast_1234",
                    "main_bytes": 8_000_000,
                    "fsm_bytes": 0,
                    "vm_bytes": 0,
                    "index_bytes": 2_000_000,
                    "total_bytes": toast,
                },
                "maintenance": {
                    "last_vacuum": "2026-07-19T00:00:00+00:00",
                    "last_autovacuum": None,
                    "vacuum_count": 1,
                    "autovacuum_count": 0,
                },
            }
        ],
        "indexes": [
            {
                "table": "event",
                "name": "event_pkey",
                "definition": "CREATE UNIQUE INDEX event_pkey ON ocpm.event (id)",
                "bytes": indexes,
            }
        ],
        "toast_indexes": [
            {
                "table": "event",
                "toast_table": "pg_toast_1234",
                "name": "pg_toast_1234_index",
                "bytes": 2_000_000,
            }
        ],
    }


def method() -> dict[str, Any]:
    return {
        "latency": {
            "warmup_rounds": checker.LATENCY_WARMUPS,
            "epochs": checker.LATENCY_EPOCHS,
            "samples_per_epoch": checker.LATENCY_SAMPLES_PER_EPOCH,
            "total_samples_per_arm": checker.LATENCY_TOTAL_SAMPLES,
            "clock": "time.perf_counter_ns",
            "order_dictionary": [list(order) for order in checker.ORDER_DICTIONARY],
            "timing_scope": "database and native execution; IPC excluded",
        },
        "memory": {
            "samples_per_arm": checker.MEMORY_SAMPLES_PER_ARM,
            "model": "fresh release-isolated process per sample",
        },
        "concurrency": {
            "suite_specs": {
                suite: {
                    "workloads": list(checker.CONCURRENCY_SPECS[suite]["workloads"]),
                    "levels": list(checker.CONCURRENCY_SPECS[suite]["levels"]),
                    "execution_paths": list(checker.EXECUTION_PATHS[suite]),
                }
                for suite in checker.SUITES
            },
            "epochs_per_arm_level": checker.CONCURRENCY_EPOCHS,
            "minimum_epoch_seconds": checker.CONCURRENCY_MIN_SECONDS,
            "minimum_requests_per_worker": (
                checker.CONCURRENCY_MIN_REQUESTS_PER_WORKER
            ),
            "connection_model": "one persistent connection per worker",
            "timing_scope": "controller round trip and worker internal",
        },
        "storage": {
            "autovacuum": "disabled during bridge",
            "structural_snapshot": "VACUUM ANALYZE before relation enumeration",
            "post_workload_snapshot": "diagnostic only",
        },
        "non_regression_thresholds": {
            "latency": {
                "relative_ceiling": checker.LATENCY_CEILING,
                "absolute_slack_ns": checker.LATENCY_ABSOLUTE_SLACK_NS,
            },
            "memory": {
                "relative_ceiling": checker.MEMORY_CEILING,
                "absolute_slack_bytes": checker.MEMORY_PAGE_SLACK,
                "engine_maximum_incremental_peak_bytes": (
                    checker.MAX_NATIVE_INCREMENTAL_BYTES
                ),
                "engine_maximum_peak_rss_bytes": checker.MAX_NATIVE_TOTAL_BYTES,
                "pm4py_maximum_peak_rss_bytes": checker.MAX_PM4PY_TOTAL_BYTES,
            },
            "concurrency": {
                "relative_ceiling": checker.CONCURRENCY_QPS_CEILING,
                "absolute_p95_slack_ms": (
                    checker.CONCURRENCY_P95_ABSOLUTE_SLACK_NS / 1_000_000
                ),
                "maximum_throughput_cv": checker.MAX_CONCURRENCY_QPS_CV,
            },
            "storage": {
                "relative_ceiling": (
                    checker.STORAGE_CEILING_NUMERATOR
                    / checker.STORAGE_CEILING_DENOMINATOR
                )
            },
        },
        "correctness_gate": "every answer matches the untimed vanilla oracle",
        "random_seed": checker.RANDOM_SEED,
        "result_cache_used": False,
    }


def artifact(*, controller_clean: bool = True) -> dict[str, Any]:
    global _PIDS
    _PIDS = itertools.count(10_000)
    suites = {}
    ratios = []
    for suite_index, suite in enumerate(checker.SUITES):
        datasets = []
        latency_order = [
            (path, workload)
            for path in checker.EXECUTION_PATHS[suite]
            for workload in checker.WORKLOADS[suite]
        ]
        concurrency_order = [
            (path, workload)
            for path in checker.EXECUTION_PATHS[suite]
            for workload in checker.CONCURRENCY_SPECS[suite]["workloads"]
        ]
        for dataset_index, dataset_name in enumerate(checker.DATASETS):
            latency = [latency_row(path, workload) for path, workload in latency_order]
            latency_map = {
                (row["execution_path"], row["workload"]): row for row in latency
            }
            memory = [
                memory_row(path, workload, latency_map[(path, workload)])
                for path, workload in latency_order
            ]
            concurrency = []
            for path, workload in concurrency_order:
                concurrency.append(
                    concurrency_row(
                        suite=suite,
                        suite_index=suite_index,
                        dataset_index=dataset_index,
                        path=path,
                        path_index=checker.EXECUTION_PATHS[suite].index(path),
                        workload=workload,
                        workload_index=checker.CONCURRENCY_SPECS[suite][
                            "workloads"
                        ].index(workload),
                    )
                )
            ratios.extend(row["p50_ratio_prior_over_current"] for row in latency)
            fixture_key = "name" if suite == "common_pm" else "dataset_name"
            datasets.append(
                {
                    "dataset": dataset_name,
                    "fixture": {fixture_key: dataset_name},
                    "latency": latency,
                    "memory": memory,
                    "concurrency": concurrency,
                }
            )
        suites[suite] = {"datasets": datasets}
    database = {
        "postgres_version": "16.14",
        "shared_buffers": "1GB",
        "effective_cache_size": "4GB",
        "work_mem": "16MB",
        "maintenance_work_mem": "1GB",
        "max_parallel_workers_per_gather": "4",
        "random_page_cost": "1.1",
        "jit": "off",
        "autovacuum": "off",
    }
    releases = {}
    for arm in checker.ARMS:
        release = copy.deepcopy(checker.EXPECTED_RELEASES[arm])
        release["worker_ocpm_engine"] = release["ocpm_engine"]
        release["worker_pg_ocpm"] = release["pg_ocpm"]
        releases[arm] = release
    prior_storage = storage_snapshot()
    current_storage = storage_snapshot()
    client = {
        "python": "3.11.15",
        "platform": "Linux-test",
        "machine": "aarch64",
        "logical_cpus_visible": 18,
    }
    host_fingerprint = {
        "benchmark_host_id": IMAGE_ID,
        "client_image_id": IMAGE_ID,
        "vanilla_database_image_id": IMAGE_ID,
        "prior_database_image_id": IMAGE_ID,
        "current_database_image_id": IMAGE_ID,
        **client,
    }
    worker_suites = {
        suite: {
            arm: {
                "arm": arm,
                "ocpm_engine": checker.EXPECTED_RELEASES[arm]["ocpm_engine"],
                "native_ocpm_engine": checker.EXPECTED_RELEASES[arm]["ocpm_engine"],
                "pg_ocpm": checker.EXPECTED_RELEASES[arm]["pg_ocpm"],
                "pg_extension": checker.EXPECTED_RELEASES[arm]["pg_ocpm"],
                "suite": suite,
                "python": client["python"],
                "pm4py": "2.7.23.3",
                "psutil": "7.0.0",
                "executable": "/usr/local/bin/python",
                "package_path": f"/opt/engine-{arm}/ocpm_engine/__init__.py",
                "database_environment": copy.deepcopy(database),
                "workload_sha256": checker.EXPECTED_HARNESS_SHA256[suite],
            }
            for arm in checker.ARMS
        }
        for suite in checker.SUITES
    }
    result = {
        "schema_version": checker.SCHEMA_VERSION,
        "artifact_type": checker.ARTIFACT_TYPE,
        "generated_at": "2026-07-19T00:00:00+00:00",
        "source": copy.deepcopy(checker.EXPECTED_SOURCE),
        "releases": releases,
        "environment": {
            "client": client,
            "vanilla_postgres": copy.deepcopy(database),
            "prior_postgres": copy.deepcopy(database),
            "current_postgres": copy.deepcopy(database),
            "worker_suites": worker_suites,
            "host_fingerprints": {
                "start": copy.deepcopy(host_fingerprint),
                "end": copy.deepcopy(host_fingerprint),
            },
        },
        "provenance": {
            "benchmark_host_id": IMAGE_ID,
            "client_image_id": IMAGE_ID,
            "vanilla_database_image_id": IMAGE_ID,
            "prior_database_image_id": IMAGE_ID,
            "current_database_image_id": IMAGE_ID,
            "controller_source_revision": "c" * 40,
            "controller_source_tree_clean": controller_clean,
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
            "harness_sha256": copy.deepcopy(checker.EXPECTED_HARNESS_SHA256),
            "loader_sha256": copy.deepcopy(checker.EXPECTED_LOADER_SHA256),
            "postgres_base_image": checker.EXPECTED_POSTGRES_BASE_IMAGE,
        },
        "method": method(),
        "suites": suites,
        "storage": {
            "structural_before_workloads": {
                "prior": prior_storage,
                "current": current_storage,
            },
            "diagnostic_after_workloads": {
                "prior": copy.deepcopy(prior_storage),
                "current": copy.deepcopy(current_storage),
            },
        },
        "summary": {
            "total_latency_workloads": 34,
            "correct_latency_workloads": 34,
            "minimum_p50_ratio_prior_over_current": min(ratios),
            "latency_non_regressed": 34,
            "memory_non_regressed": 34,
            "concurrency_levels_non_regressed": 32,
            "storage_non_regressed": 2,
            "target_met": True,
        },
    }
    return sign(result)


def first_latency(result: dict[str, Any]) -> dict[str, Any]:
    return result["suites"]["common_pm"]["datasets"][0]["latency"][0]


def first_memory(result: dict[str, Any]) -> dict[str, Any]:
    return result["suites"]["common_pm"]["datasets"][0]["memory"][0]


def first_concurrency(result: dict[str, Any]) -> dict[str, Any]:
    return result["suites"]["common_pm"]["datasets"][0]["concurrency"][0]


def public_provenance() -> dict[str, Any]:
    return {
        "benchmark_host_id": IMAGE_ID,
        "ocpm_engine_source_revision": checker.EXPECTED_RELEASES["current"][
            "ocpm_engine_revision"
        ],
        "ocpm_engine_source_tree_clean": True,
        "pg_ocpm_source_revision": checker.EXPECTED_RELEASES["current"][
            "pg_ocpm_revision"
        ],
        "pg_ocpm_source_tree_clean": True,
        "client_image_id": IMAGE_ID,
        "vanilla_database_image_id": IMAGE_ID,
        "pg_ocpm_database_image_id": IMAGE_ID,
    }


def public_common_result(bridge: dict[str, Any]) -> dict[str, Any]:
    bridge_datasets = bridge["suites"]["common_pm"]["datasets"]
    datasets = []
    for dataset in bridge_datasets:
        datasets.append(
            {
                "fixture": copy.deepcopy(dataset["fixture"]),
                "workloads": [
                    {
                        "workload": row["workload"],
                        "correct": True,
                        "pg_ocpm_rust": {"p50_ms": row["current"]["p50_ms"]},
                    }
                    for row in dataset["latency"]
                ],
            }
        )
    latency_hashes = {
        row["workload"]: row["oracle_answer_sha256"]
        for row in bridge_datasets[0]["latency"]
    }

    def concurrency_section(workload: str) -> dict[str, Any]:
        levels = [
            str(level) for level in checker.CONCURRENCY_SPECS["common_pm"]["levels"]
        ]
        return {
            "fixture": checker.DATASETS[0],
            "workload": workload,
            "levels": levels,
            "pg_ocpm_rust": {
                level: {
                    "epochs": [
                        {"answer_sha256": latency_hashes[workload], "correct": True}
                    ]
                }
                for level in levels
            },
        }

    return {
        "release": {
            "ocpm_engine": checker.EXPECTED_RELEASES["current"]["ocpm_engine"],
            "pg_ocpm": checker.EXPECTED_RELEASES["current"]["pg_ocpm"],
        },
        "source": copy.deepcopy(checker.EXPECTED_SOURCE),
        "provenance": public_provenance(),
        "datasets": datasets,
        "concurrency": concurrency_section("dfg_conformance_95pct"),
        "drift_concurrency": concurrency_section("dfg_frequency_drift"),
    }


def public_pm4py_result(bridge: dict[str, Any]) -> dict[str, Any]:
    datasets = []
    for bridge_dataset in bridge["suites"]["pm4py"]["datasets"]:
        latency_by_workload: dict[str, dict[str, Any]] = {}
        for row in bridge_dataset["latency"]:
            workload_row = latency_by_workload.setdefault(
                row["workload"],
                {
                    "workload": row["workload"],
                    "correct": True,
                    "answer_sha256": row["oracle_answer_sha256"],
                },
            )
            workload_row[row["execution_path"]] = {
                "input": copy.deepcopy(row["input_evidence"]["current"])
            }
        latency = [latency_by_workload[name] for name in checker.WORKLOADS["pm4py"]]
        memory = {
            row["workload"]: {
                path: {
                    "answer_sha256": row["answer_sha256"],
                    "input": copy.deepcopy(row[path]["input"]),
                }
                for path in checker.EXECUTION_PATHS["pm4py"]
            }
            for row in latency
        }
        datasets.append(
            {
                "dataset": bridge_dataset["dataset"],
                "fixture": copy.deepcopy(bridge_dataset["fixture"]),
                "latency": latency,
                "memory": memory,
            }
        )
    return {
        "source": {
            "title": "Collection of Object-Centric Event Logs",
            "doi": checker.EXPECTED_SOURCE["doi"],
            "license": checker.EXPECTED_SOURCE["license"],
            "datasets": list(checker.DATASETS),
        },
        "environment": {
            "client": {
                "ocpm_engine_version": checker.EXPECTED_RELEASES["current"][
                    "ocpm_engine"
                ]
            },
            "database": {
                "pg_ocpm": {
                    "pg_ocpm_version": checker.EXPECTED_RELEASES["current"]["pg_ocpm"]
                }
            },
        },
        "provenance": public_provenance(),
        "datasets": datasets,
    }


def refresh_latency_arm(row: dict[str, Any], arm: str) -> None:
    aggregate, samples = checker._latency_aggregate(row["serial_epochs"], arm)
    row[arm] = aggregate
    prior_samples = checker._latency_aggregate(row["serial_epochs"], "prior")[1]
    current_samples = checker._latency_aggregate(row["serial_epochs"], "current")[1]
    row["p50_ratio_prior_over_current"] = round(
        checker.statistics.median(prior_samples)
        / checker.statistics.median(current_samples),
        3,
    )
    prior_median = checker.statistics.median(prior_samples)
    current_median = checker.statistics.median(current_samples)
    row["non_regressed"] = current_median <= max(
        prior_median * checker.LATENCY_CEILING,
        prior_median + checker.LATENCY_ABSOLUTE_SLACK_NS,
    )
    assert samples


def refresh_memory_arm(row: dict[str, Any], arm: str) -> None:
    row[arm] = checker._memory_aggregate(row[arm]["samples"])
    row["relative_non_regressed"] = all(
        row["current"][metric] <= checker._memory_allowed(row["prior"][metric])
        for metric in (
            "median_peak_rss_bytes",
            "maximum_peak_rss_bytes",
            "median_incremental_peak_bytes",
            "maximum_incremental_peak_bytes",
        )
    )
    if row["execution_path"] == "pg_ocpm_pm4py":
        row["absolute_bound_met"] = (
            row["current"]["maximum_peak_rss_bytes"] <= checker.MAX_PM4PY_TOTAL_BYTES
        )
    else:
        row["absolute_bound_met"] = (
            row["current"]["maximum_peak_rss_bytes"] <= checker.MAX_NATIVE_TOTAL_BYTES
            and row["current"]["maximum_incremental_peak_bytes"]
            <= checker.MAX_NATIVE_INCREMENTAL_BYTES
        )
    row["non_regressed"] = row["relative_non_regressed"] and row["absolute_bound_met"]


def refresh_concurrency_epoch(epoch: dict[str, Any]) -> None:
    roundtrip = [
        sample
        for worker in epoch["workers"]
        for sample in worker["roundtrip_samples_ns"]
    ]
    internal = [
        sample
        for worker in epoch["workers"]
        for sample in worker["internal_samples_ns"]
    ]
    epoch["requests"] = len(roundtrip)
    epoch["worker_request_counts"] = sorted(
        len(worker["roundtrip_samples_ns"]) for worker in epoch["workers"]
    )
    epoch["throughput_qps"] = round(
        len(roundtrip) / (epoch["wall_ns"] / 1_000_000_000), 3
    )
    epoch["roundtrip"] = checker._concurrency_metrics_ns(roundtrip)
    epoch["worker_internal"] = checker._concurrency_metrics_ns(internal)


def refresh_concurrency_arm(level: dict[str, Any], arm: str) -> None:
    level[arm] = checker._concurrency_aggregate(level[arm]["epochs"])
    prior = level["prior"]
    current = level["current"]
    level["qps_ratio_current_over_prior"] = round(
        current["throughput_qps"] / prior["throughput_qps"], 3
    )
    level["qps_non_regressed"] = (
        current["throughput_qps"]
        >= prior["throughput_qps"] / checker.CONCURRENCY_QPS_CEILING
    )
    level["p95_non_regressed"] = current["p95_ms"] <= max(
        prior["p95_ms"] * checker.CONCURRENCY_P95_CEILING,
        prior["p95_ms"] + checker.CONCURRENCY_P95_ABSOLUTE_SLACK_NS / 1_000_000,
    )
    level["stable"] = (
        prior["throughput_cv"] <= checker.MAX_CONCURRENCY_QPS_CV
        and current["throughput_cv"] <= checker.MAX_CONCURRENCY_QPS_CV
    )
    level["non_regressed"] = (
        level["qps_non_regressed"] and level["p95_non_regressed"] and level["stable"]
    )


def test_valid_unified_artifact_passes() -> None:
    counts = checker.validate_contract(artifact(), allow_dirty=False)
    assert counts == {
        "latency_rows": 34,
        "memory_rows": 34,
        "concurrency_cells": 32,
    }


def test_preview_allows_only_dirty_controller() -> None:
    dirty_controller = artifact(controller_clean=False)
    checker.validate_contract(dirty_controller, allow_dirty=True)
    with pytest.raises(SystemExit, match="clean controller"):
        checker.validate_contract(dirty_controller, allow_dirty=False)

    dirty_arm = artifact()
    dirty_arm["provenance"]["current_engine_source_tree_clean"] = False
    with pytest.raises(SystemExit, match="measured release source trees"):
        checker.validate_contract(dirty_arm, allow_dirty=True)


def test_self_digest_and_optional_pin_are_independently_checked(tmp_path: Path) -> None:
    result = artifact()
    path = tmp_path / "bridge.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    assert checker.load_verified(path, result["payload_sha256"]) == result
    with pytest.raises(SystemExit, match="unexpected payload digest"):
        checker.load_verified(path, "0" * 64)
    result["summary"]["correct_latency_workloads"] = 0
    path.write_text(json.dumps(result, indent=2) + "\n")
    with pytest.raises(SystemExit, match="payload digest mismatch"):
        checker.load_verified(path)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.__setitem__("schema_version", 1), "schema version"),
        (
            lambda value: value.__setitem__("artifact_type", "wrong"),
            "artifact type",
        ),
        (
            lambda value: value["releases"]["current"].__setitem__("pg_ocpm", "9.9.9"),
            "unexpected pg_ocpm",
        ),
        (
            lambda value: value["provenance"]["harness_sha256"].__setitem__(
                "worker", "0" * 64
            ),
            "harness source hashes",
        ),
        (
            lambda value: value["environment"]["current_postgres"].__setitem__(
                "jit", "on"
            ),
            "settings differ",
        ),
    ],
)
def test_locked_release_host_and_harness_contracts(
    mutation: Callable[[dict[str, Any]], None], message: str
) -> None:
    result = artifact()
    mutation(result)
    with pytest.raises(SystemExit, match=message):
        checker.validate_contract(result, allow_dirty=False)


def test_exact_suite_dataset_path_and_workload_sets() -> None:
    result = artifact()
    result["suites"]["pm4py"]["datasets"][0]["latency"].pop()
    with pytest.raises(SystemExit, match="latency path/workload set"):
        checker.validate_contract(result, allow_dirty=False)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda row: row["serial_epochs"][0]["arms"]["current"][
                "samples_ns"
            ].__setitem__(0, 2_000_000),
            "metrics differ from raw samples",
        ),
        (
            lambda row: row["serial_epochs"][0]["arms"]["current"][
                "answer_sha256s"
            ].__setitem__(0, "0" * 64),
            "answer hash mismatch",
        ),
        (
            lambda row: row["serial_epochs"][0]["order_codes"].__setitem__(0, 1),
            "order is not exactly 15/15",
        ),
        (
            lambda row: row["current"].__setitem__("p50_ms", 999.0),
            "aggregate differs from raw latency",
        ),
    ],
)
def test_latency_raw_evidence_is_recomputed(
    mutation: Callable[[dict[str, Any]], None], message: str
) -> None:
    result = artifact()
    mutation(first_latency(result))
    with pytest.raises(SystemExit, match=message):
        checker.validate_contract(result, allow_dirty=False)


def test_latency_non_regression_uses_unrounded_samples() -> None:
    result = artifact()
    row = first_latency(result)
    for epoch in row["serial_epochs"]:
        samples = [1_100_001] * checker.LATENCY_SAMPLES_PER_EPOCH
        evidence = epoch["arms"]["current"]
        evidence.update(checker._serial_metrics_ns(samples))
        evidence["samples_ns"] = samples
    refresh_latency_arm(row, "current")
    with pytest.raises(SystemExit, match="latency regression"):
        checker.validate_contract(result, allow_dirty=False)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda row: row["current"]["samples"][1].__setitem__(
                "worker_pid", row["current"]["samples"][0]["worker_pid"]
            ),
            "unique fresh process PIDs",
        ),
        (
            lambda row: row["current"]["samples"][0].__setitem__(
                "incremental_peak_bytes", 1
            ),
            "invalid RSS arithmetic",
        ),
        (
            lambda row: row["current"].__setitem__("median_peak_rss_bytes", 1),
            "aggregate differs",
        ),
        (
            lambda row: row["order_codes"].__setitem__(0, 1),
            "not exactly 2/2",
        ),
    ],
)
def test_memory_freshness_arithmetic_and_aggregates(
    mutation: Callable[[dict[str, Any]], None], message: str
) -> None:
    result = artifact()
    mutation(first_memory(result))
    with pytest.raises(SystemExit, match=message):
        checker.validate_contract(result, allow_dirty=False)


def test_memory_relative_and_absolute_bounds() -> None:
    result = artifact()
    row = first_memory(result)
    for sample in row["current"]["samples"]:
        sample["peak_rss_bytes"] = 70 * 1024 * 1024
        sample["incremental_peak_bytes"] = (
            sample["peak_rss_bytes"] - sample["baseline_rss_bytes"]
        )
    refresh_memory_arm(row, "current")
    with pytest.raises(SystemExit, match="matched-release .* regression"):
        checker.validate_contract(result, allow_dirty=False)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda level: level["current"]["epochs"][0]["workers"][0][
                "roundtrip_samples_ns"
            ].__setitem__(0, 20_000_000),
            "round-trip metrics differ",
        ),
        (
            lambda level: level["current"]["epochs"][0].__setitem__(
                "wall_ns", checker.CONCURRENCY_MIN_WALL_NS - 1
            ),
            "duration floor",
        ),
        (
            lambda level: level["current"]["epochs"][0].__setitem__(
                "throughput_qps", 999.0
            ),
            "QPS differs",
        ),
        (
            lambda level: level["current"]["epochs"][0]["workers"][0][
                "answer_sha256s"
            ].__setitem__(0, "0" * 64),
            "raw request evidence",
        ),
        (
            lambda level: level["epoch_arm_orders"].reverse(),
            "locked 2/2 schedule",
        ),
    ],
)
def test_concurrency_raw_protocol_is_recomputed(
    mutation: Callable[[dict[str, Any]], None], message: str
) -> None:
    result = artifact()
    level = first_concurrency(result)["levels"]["1"]
    mutation(level)
    with pytest.raises(SystemExit, match=message):
        checker.validate_contract(result, allow_dirty=False)


def test_concurrency_qps_and_p95_non_regression() -> None:
    qps_result = artifact()
    qps_level = first_concurrency(qps_result)["levels"]["1"]
    for epoch in qps_level["current"]["epochs"]:
        epoch["wall_ns"] = 6_000_000_000
        refresh_concurrency_epoch(epoch)
    refresh_concurrency_arm(qps_level, "current")
    with pytest.raises(SystemExit, match="throughput regression"):
        checker.validate_contract(qps_result, allow_dirty=False)

    p95_result = artifact()
    p95_level = first_concurrency(p95_result)["levels"]["1"]
    for epoch in p95_level["current"]["epochs"]:
        for worker in epoch["workers"]:
            worker["roundtrip_samples_ns"] = [11_001_000] * len(
                worker["roundtrip_samples_ns"]
            )
        refresh_concurrency_epoch(epoch)
    refresh_concurrency_arm(p95_level, "current")
    with pytest.raises(SystemExit, match="concurrency p95 regression"):
        checker.validate_contract(p95_result, allow_dirty=False)


def test_storage_structure_and_regression_are_recomputed() -> None:
    totals = artifact()
    totals["storage"]["structural_before_workloads"]["current"]["total_bytes"] += 1
    with pytest.raises(SystemExit, match="differs from relation inventory"):
        checker.validate_contract(totals, allow_dirty=False)

    ownership = artifact()
    ownership["storage"]["structural_before_workloads"]["current"]["indexes"][0][
        "table"
    ] = "missing"
    with pytest.raises(SystemExit, match="invalid index inventory"):
        checker.validate_contract(ownership, allow_dirty=False)

    regression = artifact()
    current = regression["storage"]["structural_before_workloads"]["current"]
    current["total_bytes"] = 101_000_001
    current["relations"][0]["total_bytes"] = 101_000_001
    current["relations"][0]["main_fsm_bytes"] += 1_000_001
    current["other_fork_bytes"] += 1_000_001
    with pytest.raises(SystemExit, match="total_bytes regression"):
        checker.validate_contract(regression, allow_dirty=False)

    changed_after = artifact()
    changed_after["storage"]["diagnostic_after_workloads"]["current"]["relations"][0][
        "name"
    ] = "changed"
    changed_after["storage"]["diagnostic_after_workloads"]["current"]["indexes"][0][
        "table"
    ] = "changed"
    changed_after["storage"]["diagnostic_after_workloads"]["current"]["toast_indexes"][
        0
    ]["table"] = "changed"
    with pytest.raises(SystemExit, match="structure changed"):
        checker.validate_contract(changed_after, allow_dirty=False)


def test_public_common_helper_binds_current_bridge_evidence() -> None:
    bridge = artifact()
    public = public_common_result(bridge)
    checker.validate_for_public_common(public, bridge, allow_dirty=False)

    public["drift_concurrency"]["pg_ocpm_rust"]["1"]["epochs"][0]["answer_sha256"] = (
        "0" * 64
    )
    with pytest.raises(SystemExit, match="answer hashes are invalid"):
        checker.validate_for_public_common(public, bridge, allow_dirty=False)


def test_public_pm4py_helper_binds_hashes_inputs_and_versions() -> None:
    bridge = artifact()
    public = public_pm4py_result(bridge)
    checker.validate_for_public_pm4py(public, bridge, allow_dirty=False)

    public["datasets"][0]["latency"][0]["pg_ocpm_ocpm_engine"]["input"] = {
        "changed": True
    }
    with pytest.raises(SystemExit, match="current-path evidence differs"):
        checker.validate_for_public_pm4py(public, bridge, allow_dirty=False)

    wrong_version = public_pm4py_result(bridge)
    wrong_version["environment"]["client"]["ocpm_engine_version"] = "9.9.9"
    with pytest.raises(SystemExit, match="current release versions"):
        checker.validate_for_public_pm4py(wrong_version, bridge, allow_dirty=False)
