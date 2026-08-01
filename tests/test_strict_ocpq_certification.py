from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "strict_ocpq_checker_test",
    ROOT / "benchmarks/check_ocpq_result.py",
)
CHECKER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(CHECKER)

REFERENCE_DIGEST = "a" * 64
SOURCE_REVISION = "1" * 40
TREE_ID = "git-tree:" + "2" * 40
RUNNER_DIGEST = "3" * 64
HOST_ID = "sha256:" + "4" * 64
REFERENCE_IMAGE_ID = "sha256:" + "5" * 64
CANDIDATE_IMAGE_ID = "sha256:" + "6" * 64
DATABASE_IMAGE_ID = "sha256:" + "7" * 64
PG_REVISION = "8" * 40


def canonicalization() -> dict:
    return {
        "scope": "every query-tree node and every materialized situation",
        "identifiers": "OCEL external object and event IDs",
        "variables": "object/event variable indexes retained in every row",
        "labels": "label names and exact serialized OCPQ label value types retained",
        "violation": (
            "exact serialized OCPQ ViolationReason retained; null means satisfied"
        ),
        "q6": (
            "root max_dur typed label retained exactly and independently checked "
            "against the maximum child duration in integer microseconds"
        ),
        "ordering": "lexicographic compact-JSON row order; duplicates retained",
        "row_shape": [
            "[[object_variable_index, external_object_id], ...]",
            "[[event_variable_index, external_event_id], ...]",
            "[[label_name, typed_label_value], ...]",
            "serialized_violation_reason_or_null",
        ],
        "expected_node_manifests": CHECKER.expected_manifests(),
    }


def reference_fixture(*, clean: bool = True) -> dict:
    queries = {}
    for index, name in enumerate(CHECKER.QUERY_NAMES, start=1):
        runs = [100.0 + index] * CHECKER.RUNS
        published = [150.0 + index] * 10
        queries[name] = {
            "author_published_runs_ms": published,
            "author_published_mean_ms": published[0],
            "tree_parse_ms": 1.0,
            "import_ms": 2.0,
            "link_ms": 3.0,
            "runs_ms": runs,
            "mean_ms": runs[0],
            "p50_ms": runs[0],
            "p95_ms": runs[0],
            "all_node_situations": sum(
                item["situation_count"] for item in CHECKER.expected_fingerprints(name)
            ),
            "root_node": 0,
            "q6_root_label": (
                copy.deepcopy(CHECKER.EXPECTED_Q6_ROOT_LABEL) if name == "Q6" else None
            ),
            "q6_duration_microseconds": (
                CHECKER.EXPECTED_Q6_DURATION_MICROSECONDS if name == "Q6" else None
            ),
            "nodes": copy.deepcopy(CHECKER.expected_fingerprints(name)),
        }
    return {
        "schema_version": 4,
        "generated_at": "2026-07-19T12:00:00+00:00",
        "source": {
            **copy.deepcopy(CHECKER.EXPECTED_SOURCE),
            "docker_image_id": REFERENCE_IMAGE_ID,
        },
        "environment": {
            "benchmark_host_id": HOST_ID,
            "source_revision": SOURCE_REVISION,
            "source_tree_clean": clean,
            "platform": "test-linux",
            "machine": "aarch64",
            "logical_cpus_visible": 16,
        },
        "method": {
            "warmups_per_query": 0,
            "measured_runs_per_query": 10,
            "fresh_container_per_query": True,
            "import_and_link_timed_separately": True,
            "author_published_samples_per_query": 10,
            "author_published_results_are_cross_host_only": True,
            "upstream_backend_measure_performance_iterations": 10,
            "upstream_backend_warmups": 0,
            "timing_boundary": CHECKER.REFERENCE_TIMING_BOUNDARY,
            "correctness_boundary": CHECKER.REFERENCE_CORRECTNESS_BOUNDARY,
            "canonicalization": canonicalization(),
        },
        "queries": queries,
    }


def environment(*, concurrency: bool, clean: bool = True) -> dict:
    suffix = "concurrency" if concurrency else "latency"
    return {
        "benchmark_host_id": HOST_ID,
        "source_revision": SOURCE_REVISION,
        "source_tree_clean": clean,
        "source_tree_id": TREE_ID,
        "candidate_runner_sha256": RUNNER_DIGEST,
        "candidate_image": "ocpm-engine:test",
        "candidate_image_id": CANDIDATE_IMAGE_ID,
        "database_image": "pg_ocpm:test",
        "database_image_id": DATABASE_IMAGE_ID,
        "pg_ocpm_source_revision": PG_REVISION,
        "pg_ocpm_source_tree_clean": clean,
        "postgres_server_version": "16.1",
        "postgres_server_version_num": 160001,
        "postgres_jit": "off",
        "client_os": "linux",
        "client_arch": "aarch64",
        "client_logical_cpus_visible": 16,
        "tokio_worker_threads_env": "16" if concurrency else "1",
        "process_run_id": f"process-{suffix}",
        "container_hostname": f"container-{suffix}",
        "client_process_id": 1,
        "same_docker_host_as_reference": True,
        "same_harness_revision_state_as_reference": True,
        "provenance_complete": clean,
    }


def concurrency_level(clients: int, throughput: float) -> dict:
    wall_ms = 5_000.0
    request_count = int(throughput * wall_ms / 1000.0)
    assert request_count % clients == 0
    client_count = request_count // clients
    assert client_count >= CHECKER.MINIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
    assert client_count <= CHECKER.MAXIMUM_CONCURRENCY_REQUESTS_PER_CLIENT
    p95_rank = CHECKER.math.ceil(request_count * 0.95)
    p99_rank = CHECKER.math.ceil(request_count * 0.99)
    raw_latencies = (
        [1_000_000] * (p95_rank - 1)
        + [2_000_000] * (p99_rank - p95_rank)
        + [3_000_000] * (request_count - p99_rank + 1)
    )
    raw_by_client = {
        str(client_id): raw_latencies[
            client_id * client_count : (client_id + 1) * client_count
        ]
        for client_id in range(clients)
    }
    query_counts = {name: 0 for name in CHECKER.QUERY_NAMES}
    for client_id in range(clients):
        for request_id in range(client_count):
            name = CHECKER.QUERY_NAMES[
                (client_id + request_id) % len(CHECKER.QUERY_NAMES)
            ]
            query_counts[name] += 1
    epochs = []
    for epoch in range(1, 4):
        epochs.append(
            {
                "epoch": epoch,
                "client_ids": list(range(clients)),
                "pre_epoch_exact_query_checks_per_client": 7,
                "post_epoch_exact_query_checks_per_client": 7,
                "request_count": request_count,
                "client_request_counts": {
                    str(index): client_count for index in range(clients)
                },
                "query_request_counts": copy.deepcopy(query_counts),
                "client_request_latencies_ns": copy.deepcopy(raw_by_client),
                "wall_time_ms": wall_ms,
                "throughput_requests_per_second": throughput,
                "latency_p50_ms": 1.0,
                "latency_p95_ms": 2.0,
                "latency_p99_ms": 3.0,
                "every_pre_and_post_node_exact": True,
            }
        )
    return {
        "clients": clients,
        "epoch_count": 3,
        "minimum_requests_per_client": 32,
        "maximum_requests_per_client": 250_000,
        "minimum_wall_time_ms": 5_000.0,
        "total_request_count": request_count * 3,
        "total_query_request_counts": {
            name: count * 3 for name, count in query_counts.items()
        },
        "median_epoch_throughput_requests_per_second": throughput,
        "minimum_epoch_throughput_requests_per_second": throughput,
        "maximum_epoch_throughput_requests_per_second": throughput,
        "epoch_throughput_cv": 0.0,
        "median_epoch_latency_p50_ms": 1.0,
        "median_epoch_latency_p95_ms": 2.0,
        "median_epoch_latency_p99_ms": 3.0,
        "epochs": epochs,
        "every_pre_and_post_node_exact": True,
    }


def memory_fixture(name: str, capsule_bytes: int, nodes: list[dict]) -> dict:
    baseline_rss = 10 * 1024 * 1024
    baseline_hwm = 11 * 1024 * 1024
    after_rss = 14 * 1024 * 1024
    after_hwm = 15 * 1024 * 1024
    return {
        "mode": CHECKER.MEMORY_MODE,
        "measurement_boundary": CHECKER.MEMORY_MEASUREMENT_BOUNDARY,
        "correctness_boundary": CHECKER.MEMORY_CORRECTNESS_BOUNDARY,
        "all_node_situations": sum(item["situation_count"] for item in nodes),
        "node_situation_counts": [item["situation_count"] for item in nodes],
        "capsule_bytes": capsule_bytes,
        "owned_tree_allocated_bytes": 2 * 1024 * 1024,
        "baseline_rss_bytes": baseline_rss,
        "baseline_vmhwm_bytes": baseline_hwm,
        "after_rss_bytes": after_rss,
        "after_vmhwm_bytes": after_hwm,
        "rss_delta_bytes": after_rss - baseline_rss,
        "vmhwm_delta_bytes": after_hwm - baseline_hwm,
        "peak_over_baseline_rss_bytes": after_hwm - baseline_rss,
        "peak_over_baseline_vmhwm_bytes": after_hwm - baseline_hwm,
        "postgres_backend_memory_scope": CHECKER.BACKEND_MEMORY_SCOPE,
        "postgres_backend_baseline_bytes": 1_000_000,
        "postgres_backend_after_bytes": 1_100_000,
        "postgres_backend_retained_delta_bytes": 100_000,
        "nodes": copy.deepcopy(nodes),
        "every_node_exact": True,
    }


def refresh_latency_summary(candidate: dict, reference: dict) -> None:
    reference_means = []
    candidate_means = []
    speedups = []
    for name in CHECKER.QUERY_NAMES:
        query = candidate["queries"][name]
        reference_mean = reference["queries"][name]["mean_ms"]
        candidate_mean = sum(query["runs_ms"]) / len(query["runs_ms"])
        speedup = reference_mean / candidate_mean
        query["reference_ocpq_mean_ms"] = reference_mean
        query["candidate_mean_ms"] = candidate_mean
        query["candidate_p50_ms"] = CHECKER.conventional_median(query["runs_ms"])
        query["candidate_p95_ms"] = CHECKER.nearest_rank(query["runs_ms"], 0.95)
        query["speedup_vs_reference_ocpq"] = speedup
        reference_means.append(reference_mean)
        candidate_means.append(candidate_mean)
        speedups.append(speedup)
    candidate["summary"].update(
        {
            "reference_ocpq_geometric_mean_ms": CHECKER.geometric_mean(reference_means),
            "candidate_geometric_mean_ms": CHECKER.geometric_mean(candidate_means),
            "speedup_geometric_mean": CHECKER.geometric_mean(speedups),
            "minimum_query_speedup": min(speedups),
        }
    )


def candidate_fixture(reference: dict, *, clean: bool = True) -> dict:
    queries = {}
    memories = {}
    processes = {}
    nested_processes = []
    for index, name in enumerate(CHECKER.QUERY_NAMES, start=1):
        reference_query = reference["queries"][name]
        run = reference_query["mean_ms"] / 12.0
        capsule_bytes = 10_000 + index
        query = {
            "reference_ocpq_mean_ms": reference_query["mean_ms"],
            "candidate_mean_ms": run,
            "candidate_p50_ms": run,
            "candidate_p95_ms": run,
            "speedup_vs_reference_ocpq": 12.0,
            "runs_ms": [run] * 10,
            "all_node_situations": reference_query["all_node_situations"],
            "capsule_bytes": capsule_bytes,
            "nodes": copy.deepcopy(reference_query["nodes"]),
            "every_node_exact": True,
        }
        if name == "Q6":
            query["q6_root_label"] = copy.deepcopy(reference_query["q6_root_label"])
            query["q6_duration_microseconds"] = reference_query[
                "q6_duration_microseconds"
            ]
        queries[name] = query
        memories[name] = memory_fixture(name, capsule_bytes, reference_query["nodes"])
        process = {
            "container_id": f"container-{index}",
            "process_start_id": f"process-{index}",
            "client_process_id": 1,
        }
        processes[name] = process
        nested_processes.append(
            {
                "query": name,
                "process_run_id": process["process_start_id"],
                "container_hostname": process["container_id"],
                "client_process_id": process["client_process_id"],
            }
        )
    binding_total = 4 * 1024 * 1024
    event_total = 8 * 1024 * 1024
    cache_total = 8 * 1024
    storage_total = binding_total + event_total + cache_total
    storage_indexes = 3 * 1024 * 1024
    candidate = {
        "schema_version": 1,
        "artifact_kind": "strict-all-node-ocpq-publication-gates",
        "generated_at_unix_ms": 1,
        "publication_status": {
            "ready": clean,
            "latency_targets_met": True,
            "minimum_query_speedup_required": 5,
            "geometric_mean_speedup_required": 10,
            "every_latency_query_and_node_exact": True,
            "every_memory_query_and_node_exact": True,
            "every_concurrency_level_pre_and_post_node_exact": True,
            "storage_within_limits": True,
            "provenance_complete": clean,
        },
        "release": copy.deepcopy(CHECKER.EXPECTED_RELEASE),
        "environment": {
            "latency_and_memory": environment(concurrency=False, clean=clean),
            "concurrency": environment(concurrency=True, clean=clean),
        },
        "reference_schema_version": 4,
        "reference_artifact_sha256": REFERENCE_DIGEST,
        "reference_source": copy.deepcopy(reference["source"]),
        "reference_environment": copy.deepcopy(reference["environment"]),
        "fresh_container_per_query": True,
        "latency_processes": processes,
        "method": {
            "latency": {
                "warmups_per_query": 0,
                "measured_runs_per_query": 10,
                "timing_boundary": CHECKER.CANDIDATE_TIMING_BOUNDARY,
                "correctness_boundary": CHECKER.CANDIDATE_CORRECTNESS_BOUNDARY,
                "query_protocol": CHECKER.CANDIDATE_QUERY_PROTOCOL,
                "process_scope": CHECKER.CANDIDATE_PROCESS_SCOPE,
                "p50_estimator": CHECKER.CANDIDATE_P50_ESTIMATOR,
                "p95_estimator": CHECKER.CANDIDATE_P95_ESTIMATOR,
            },
            "latency_fresh_container_per_query": True,
            "latency_fresh_container_count": 7,
            "latency_fresh_processes": nested_processes,
            "memory": {
                "mode": CHECKER.MEMORY_MODE,
                "measurement_boundary": CHECKER.MEMORY_MEASUREMENT_BOUNDARY,
                "correctness_boundary": CHECKER.MEMORY_CORRECTNESS_BOUNDARY,
            },
            "resources": {
                "storage_boundary": CHECKER.STORAGE_BOUNDARY,
                "concurrency_boundary": CHECKER.CONCURRENCY_BOUNDARY,
                "concurrency_protocol": CHECKER.CONCURRENCY_PROTOCOL,
                "concurrency_raw_evidence": CHECKER.CONCURRENCY_RAW_EVIDENCE,
                "correctness_boundary": CHECKER.RESOURCE_CORRECTNESS_BOUNDARY,
                "latency_p50_estimator": CHECKER.RESOURCE_P50_ESTIMATOR,
                "latency_p95_p99_estimator": CHECKER.RESOURCE_TAIL_ESTIMATOR,
            },
        },
        "summary": {
            "maximum_client_peak_over_baseline_rss_bytes": 5 * 1024 * 1024,
            "maximum_client_peak_over_baseline_vmhwm_bytes": 4 * 1024 * 1024,
            "maximum_owned_tree_allocated_bytes": 2 * 1024 * 1024,
        },
        "storage_limits": {
            "maximum_total_serving_bytes": 128 * 1024 * 1024,
            "maximum_index_bytes": 16 * 1024 * 1024,
            "maximum_binding_summary_bytes": 8 * 1024 * 1024,
        },
        "storage": {
            "scope": CHECKER.STORAGE_SCOPE,
            "database_bytes_diagnostic": 20 * 1024 * 1024,
            "total_serving_bytes": storage_total,
            "index_bytes": storage_indexes,
            "heap_toast_fsm_vm_bytes": storage_total - storage_indexes,
            "binding_summary_bytes": binding_total,
            "result_cache_rows": 0,
            "request_result_cache_enabled": False,
            "relations": {
                "binding_activity": {
                    "total_bytes": binding_total,
                    "index_bytes": 1 * 1024 * 1024,
                    "heap_toast_fsm_vm_bytes": 3 * 1024 * 1024,
                },
                "event": {
                    "total_bytes": event_total,
                    "index_bytes": 2 * 1024 * 1024,
                    "heap_toast_fsm_vm_bytes": 6 * 1024 * 1024,
                },
                "result_cache": {
                    "total_bytes": cache_total,
                    "index_bytes": 0,
                    "heap_toast_fsm_vm_bytes": cache_total,
                },
            },
        },
        "queries": queries,
        "memory": memories,
        "concurrency": {
            str(clients): concurrency_level(clients, clients * 100.0)
            for clients in CHECKER.CONCURRENCY_LEVELS
        },
    }
    refresh_latency_summary(candidate, reference)
    return candidate


def certify(reference: dict, candidate: dict, *, release: bool = True) -> dict:
    return CHECKER.certify(
        reference,
        candidate,
        reference_digest=REFERENCE_DIGEST,
        release=release,
    )


def test_valid_release_artifact_certifies() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)

    result = certify(reference, candidate)

    assert result["publication_ready"] is True
    assert result["every_query_and_node_exact"] is True
    assert result["fresh_process_scope_complete"] is True
    assert result["no_result_cache"] is True


def test_preview_forces_publication_not_ready_when_evidence_is_missing() -> None:
    reference = reference_fixture(clean=False)
    candidate = candidate_fixture(reference, clean=False)
    del candidate["fresh_container_per_query"]
    del candidate["latency_processes"]
    del candidate["storage"]["result_cache_rows"]
    del candidate["storage"]["request_result_cache_enabled"]

    result = certify(reference, candidate, release=False)

    assert result["publication_ready"] is False
    assert result["every_query_and_node_exact"] is True
    assert result["fresh_process_scope_complete"] is False
    assert result["no_result_cache"] is False
    assert "preview mode" in result["blocking_evidence"]


def test_preview_accepts_an_explicit_unpublished_release_pair() -> None:
    reference = reference_fixture(clean=False)
    candidate = candidate_fixture(reference, clean=False)
    candidate["release"] = {"pg_ocpm": "0.9.0", "ocpm_engine": "0.9.0"}

    result = CHECKER.certify(
        reference,
        candidate,
        reference_digest=REFERENCE_DIGEST,
        release=False,
        expected_release=candidate["release"],
    )

    assert result["publication_ready"] is False
    assert result["every_query_and_node_exact"] is True


def test_release_api_rejects_an_unpublished_release_pair() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    unpublished = {"pg_ocpm": "0.9.0", "ocpm_engine": "0.9.0"}
    candidate["release"] = unpublished

    with pytest.raises(
        CHECKER.CertificationError,
        match="only the published release pair",
    ):
        CHECKER.certify(
            reference,
            candidate,
            reference_digest=REFERENCE_DIGEST,
            release=True,
            expected_release=unpublished,
        )


def test_async_concurrency_does_not_require_one_runtime_thread_per_client() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    candidate["environment"]["concurrency"]["tokio_worker_threads_env"] = "3"

    result = certify(reference, candidate)

    assert result["concurrency_16_to_1_scaling"] == 16.0


def test_rejects_zero_concurrency_runtime_threads() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    candidate["environment"]["concurrency"]["tokio_worker_threads_env"] = "0"

    with pytest.raises(CHECKER.CertificationError, match="worker-thread provenance"):
        certify(reference, candidate)


def test_preview_rejects_dirty_artifact_marked_publication_ready() -> None:
    reference = reference_fixture(clean=False)
    candidate = candidate_fixture(reference, clean=False)
    candidate["publication_status"]["ready"] = True
    candidate["publication_status"]["provenance_complete"] = True

    with pytest.raises(
        CHECKER.CertificationError,
        match="publication status is inconsistent: provenance_complete",
    ):
        certify(reference, candidate, release=False)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda reference, _: reference.update(schema_version=3), "schema version 4"),
        (
            lambda reference, _: reference["method"].update(warmups_per_query=10),
            "invalid warmups_per_query",
        ),
        (
            lambda reference, _: reference["queries"]["Q1"]["nodes"].pop(),
            "pinned strict every-node",
        ),
        (
            lambda _, candidate: candidate["queries"]["Q1"]["nodes"].pop(),
            "one or more result nodes",
        ),
        (
            lambda _, candidate: candidate["queries"]["Q1"].update(
                candidate_p50_ms=0.1
            ),
            "conventional median",
        ),
        (
            lambda _, candidate: candidate["storage"].update(result_cache_rows=1),
            "result-cache rows",
        ),
        (
            lambda _, candidate: candidate["latency_processes"]["Q2"].update(
                container_id=candidate["latency_processes"]["Q1"]["container_id"]
            ),
            "seven unique containers",
        ),
    ],
)
def test_rejects_adversarial_correctness_protocol_and_cache_mutations(
    mutate, message: str
) -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    mutate(reference, candidate)

    with pytest.raises(CHECKER.CertificationError, match=message):
        certify(reference, candidate)


def test_rejects_query_below_five_x() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    name = "Q3"
    candidate["queries"][name]["runs_ms"] = [
        reference["queries"][name]["mean_ms"] / 4.0
    ] * 10
    refresh_latency_summary(candidate, reference)

    with pytest.raises(CHECKER.CertificationError, match="below 5.0x"):
        certify(reference, candidate)


def test_rejects_geometric_mean_below_ten_x() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    for name in CHECKER.QUERY_NAMES:
        candidate["queries"][name]["runs_ms"] = [
            reference["queries"][name]["mean_ms"] / 6.0
        ] * 10
    refresh_latency_summary(candidate, reference)

    with pytest.raises(CHECKER.CertificationError, match="geometric-mean speedup"):
        certify(reference, candidate)


def test_rejects_client_vmhwm_over_eight_mib() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    memory = candidate["memory"]["Q1"]
    memory["after_vmhwm_bytes"] = memory["baseline_vmhwm_bytes"] + 9 * 1024 * 1024
    memory["vmhwm_delta_bytes"] = 9 * 1024 * 1024
    memory["peak_over_baseline_vmhwm_bytes"] = 9 * 1024 * 1024
    memory["peak_over_baseline_rss_bytes"] = (
        memory["after_vmhwm_bytes"] - memory["baseline_rss_bytes"]
    )

    with pytest.raises(CHECKER.CertificationError, match="VmHWM increase"):
        certify(reference, candidate)


def recalculate_storage(candidate: dict) -> None:
    storage = candidate["storage"]
    relations = storage["relations"]
    storage["total_serving_bytes"] = sum(
        relation["total_bytes"] for relation in relations.values()
    )
    storage["index_bytes"] = sum(
        relation["index_bytes"] for relation in relations.values()
    )
    storage["heap_toast_fsm_vm_bytes"] = (
        storage["total_serving_bytes"] - storage["index_bytes"]
    )
    storage["binding_summary_bytes"] = sum(
        relation["total_bytes"]
        for name, relation in relations.items()
        if name.startswith("binding_")
    )
    storage["database_bytes_diagnostic"] = storage["total_serving_bytes"] + 1024


@pytest.mark.parametrize(
    ("relation_name", "total_mib", "index_mib", "message"),
    [
        ("event", 130, 2, "total serving storage exceeds"),
        ("event", 30, 20, "serving indexes exceed"),
        ("binding_activity", 9, 1, "binding summaries exceed"),
    ],
)
def test_rejects_storage_ceiling_violations(
    relation_name: str, total_mib: int, index_mib: int, message: str
) -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    relation = candidate["storage"]["relations"][relation_name]
    relation["total_bytes"] = total_mib * 1024 * 1024
    relation["index_bytes"] = index_mib * 1024 * 1024
    relation["heap_toast_fsm_vm_bytes"] = (
        relation["total_bytes"] - relation["index_bytes"]
    )
    recalculate_storage(candidate)

    with pytest.raises(CHECKER.CertificationError, match=message):
        certify(reference, candidate)


def test_rejects_sixteen_to_one_scaling_below_five_x() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    candidate["concurrency"]["16"] = concurrency_level(16, 102.4)

    with pytest.raises(CHECKER.CertificationError, match="scaling.*below 5x"):
        certify(reference, candidate)


def test_rejects_concurrency_throughput_cv_above_fifteen_percent() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    reports = [concurrency_level(4, value) for value in (400.0, 600.0, 800.0)]
    report = copy.deepcopy(reports[0])
    report["epochs"] = [copy.deepcopy(source["epochs"][0]) for source in reports]
    for epoch_number, epoch in enumerate(report["epochs"], start=1):
        epoch["epoch"] = epoch_number
    throughputs = [400.0, 600.0, 800.0]
    report["median_epoch_throughput_requests_per_second"] = 600.0
    report["minimum_epoch_throughput_requests_per_second"] = 400.0
    report["maximum_epoch_throughput_requests_per_second"] = 800.0
    report["epoch_throughput_cv"] = CHECKER.statistics.pstdev(
        throughputs
    ) / CHECKER.statistics.fmean(throughputs)
    report["total_request_count"] = sum(
        epoch["request_count"] for epoch in report["epochs"]
    )
    report["total_query_request_counts"] = {
        name: sum(epoch["query_request_counts"][name] for epoch in report["epochs"])
        for name in CHECKER.QUERY_NAMES
    }
    candidate["concurrency"]["4"] = report

    with pytest.raises(CHECKER.CertificationError, match="throughput CV"):
        certify(reference, candidate)


def test_rejects_concurrency_median_p95_at_ten_ms() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    report = candidate["concurrency"]["16"]
    for epoch in report["epochs"]:
        for latencies in epoch["client_request_latencies_ns"].values():
            latencies[:] = [10_000_000] * len(latencies)
        epoch["latency_p50_ms"] = 10.0
        epoch["latency_p95_ms"] = 10.0
        epoch["latency_p99_ms"] = 10.0
    report["median_epoch_latency_p50_ms"] = 10.0
    report["median_epoch_latency_p95_ms"] = 10.0
    report["median_epoch_latency_p99_ms"] = 10.0

    with pytest.raises(CHECKER.CertificationError, match="not below 10 ms"):
        certify(reference, candidate)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("latency_p50_ms", 1.1, "raw p50 is inconsistent"),
        ("latency_p95_ms", 2.1, "raw p95 is inconsistent"),
        ("latency_p99_ms", 3.1, "raw p99 is inconsistent"),
    ],
)
def test_rejects_concurrency_percentile_not_derived_from_raw_evidence(
    field: str, value: float, message: str
) -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    candidate["concurrency"]["4"]["epochs"][0][field] = value

    with pytest.raises(CHECKER.CertificationError, match=message):
        certify(reference, candidate)


def test_rejects_missing_or_misidentified_raw_concurrency_clients() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    raw = candidate["concurrency"]["4"]["epochs"][0]["client_request_latencies_ns"]
    raw["client-0"] = raw.pop("0")

    with pytest.raises(
        CHECKER.CertificationError, match="raw latency evidence must contain exactly"
    ):
        certify(reference, candidate)


def test_rejects_raw_concurrency_request_count_mismatch() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    candidate["concurrency"]["4"]["epochs"][0]["client_request_latencies_ns"]["0"].pop()

    with pytest.raises(CHECKER.CertificationError, match="raw latency count differs"):
        certify(reference, candidate)


def test_rejects_raw_concurrency_query_schedule_mismatch() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    counts = candidate["concurrency"]["4"]["epochs"][0]["query_request_counts"]
    counts["Q1"] -= 1
    counts["Q2"] += 1

    with pytest.raises(CHECKER.CertificationError, match="raw Q1-Q7 schedule differs"):
        certify(reference, candidate)


@pytest.mark.parametrize(
    "latency_ns",
    [0, -1, 1.0, True, 5_000_000_001],
)
def test_rejects_non_positive_non_integer_or_impossible_raw_latency(
    latency_ns: object,
) -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    candidate["concurrency"]["1"]["epochs"][0]["client_request_latencies_ns"]["0"][
        0
    ] = latency_ns

    with pytest.raises(CHECKER.CertificationError, match="latency_ns is invalid"):
        certify(reference, candidate)


def test_rejects_raw_concurrency_evidence_above_per_client_size_bound() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    candidate["concurrency"]["1"]["epochs"][0]["client_request_counts"]["0"] = (
        CHECKER.MAXIMUM_CONCURRENCY_REQUESTS_PER_CLIENT + 1
    )

    with pytest.raises(
        CHECKER.CertificationError, match="out-of-bounds client request count"
    ):
        certify(reference, candidate)


def test_release_rejects_incomplete_provenance() -> None:
    reference = reference_fixture()
    candidate = candidate_fixture(reference)
    for environment_value in candidate["environment"].values():
        environment_value["pg_ocpm_source_revision"] = "unknown"
        environment_value["pg_ocpm_source_tree_clean"] = False
        environment_value["provenance_complete"] = False
    candidate["publication_status"]["provenance_complete"] = False
    candidate["publication_status"]["ready"] = False

    with pytest.raises(CHECKER.CertificationError, match="release provenance"):
        certify(reference, candidate)
