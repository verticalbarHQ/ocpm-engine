from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks import sap_release_regression as regression


class FakeWorker:
    def __init__(
        self,
        arm: str,
        answer_sha256: str,
        *,
        pid: int = 100,
        input_value: dict[str, Any] | None = None,
    ):
        self.arm = arm
        self.answer_sha256 = answer_sha256
        self.process = SimpleNamespace(pid=pid)
        self.input_value = input_value or {"rows": 7}
        self.calls = 0

    def request(self, operation: str, **values: Any) -> dict[str, Any]:
        assert operation == "run"
        self.calls += 1
        return {
            "answer_sha256": self.answer_sha256,
            "execution_path": values["execution_path"],
            "input": self.input_value,
            "elapsed_ns": 1_000 + self.calls if values["timed"] else None,
        }


def test_latency_bridge_retains_exact_counterbalanced_raw_samples(monkeypatch) -> None:
    answer = {"answer": "same"}
    digest = regression.bridge.canonical_sha256(answer)
    workers = {
        "prior": FakeWorker("prior", digest),
        "current": FakeWorker("current", digest),
    }
    monkeypatch.setattr(regression, "oracle_answer", lambda *args, **kwargs: answer)
    monkeypatch.setattr(regression, "_fixture_payload", lambda suite, value: value)

    row = regression.benchmark_latency(
        suite="common_pm",
        execution_path="pg_ocpm_rust",
        common_pm=object(),
        oracle=object(),
        oracle_fixture={"fixture": "same"},
        arm_fixtures={"prior": {"id": 1}, "current": {"id": 1}},
        workers=workers,
        workload="dfg_conformance_95pct",
        rng=regression.random.Random(7),
    )

    assert row["correct"] is True
    assert row["oracle_answer_sha256"] == digest
    assert row["warmup_order_codes"].count(0) == 5
    assert row["warmup_order_codes"].count(1) == 5
    assert row["first_execution_counts"] == {"prior": 45, "current": 45}
    assert len(row["serial_epochs"]) == 3
    for epoch in row["serial_epochs"]:
        assert epoch["order_codes"].count(0) == 15
        assert epoch["order_codes"].count(1) == 15
        for arm in ("prior", "current"):
            assert len(epoch["arms"][arm]["samples_ns"]) == 30
            assert set(epoch["arms"][arm]["answer_sha256s"]) == {digest}


def test_latency_bridge_rejects_release_input_shape_change(monkeypatch) -> None:
    answer = ["same"]
    digest = regression.bridge.canonical_sha256(answer)
    workers = {
        "prior": FakeWorker("prior", digest, input_value={"rows": 7}),
        "current": FakeWorker("current", digest, input_value={"rows": 8}),
    }
    monkeypatch.setattr(regression, "oracle_answer", lambda *args, **kwargs: answer)
    monkeypatch.setattr(regression, "_fixture_payload", lambda suite, value: value)

    with pytest.raises(AssertionError, match="input evidence differs"):
        regression.benchmark_latency(
            suite="pm4py",
            execution_path="pg_ocpm_ocpm_engine",
            common_pm=object(),
            oracle=object(),
            oracle_fixture={"fixture": "same"},
            arm_fixtures={"prior": {"id": 1}, "current": {"id": 1}},
            workers=workers,
            workload="next_activity_prediction",
            rng=regression.random.Random(7),
        )


def test_memory_metrics_recompute_medians_and_maxima() -> None:
    samples = [
        {
            "baseline_rss_bytes": 100,
            "peak_rss_bytes": 140,
            "incremental_peak_bytes": 40,
        },
        {
            "baseline_rss_bytes": 110,
            "peak_rss_bytes": 150,
            "incremental_peak_bytes": 40,
        },
        {
            "baseline_rss_bytes": 120,
            "peak_rss_bytes": 180,
            "incremental_peak_bytes": 60,
        },
        {
            "baseline_rss_bytes": 130,
            "peak_rss_bytes": 190,
            "incremental_peak_bytes": 60,
        },
    ]

    metrics = regression._memory_metrics(samples)

    assert metrics["exact_samples"] == 4
    assert metrics["median_baseline_rss_bytes"] == 115
    assert metrics["median_peak_rss_bytes"] == 165
    assert metrics["maximum_peak_rss_bytes"] == 190
    assert metrics["median_incremental_peak_bytes"] == 50
    assert metrics["maximum_incremental_peak_bytes"] == 60


def test_concurrency_epoch_retains_raw_roundtrip_and_internal_samples(
    monkeypatch,
) -> None:
    digest = "a" * 64
    pool = [
        FakeWorker("prior", digest, pid=101),
        FakeWorker("prior", digest, pid=102),
    ]
    monkeypatch.setattr(regression, "CONCURRENCY_MIN_SECONDS", 0.001)
    monkeypatch.setattr(regression, "CONCURRENCY_MIN_REQUESTS_PER_WORKER", 2)

    epoch = regression.run_concurrency_epoch(
        pool=pool,
        suite="common_pm",
        execution_path="pg_ocpm_rust",
        workload="dfg_conformance_95pct",
        fixture={"id": 1},
        oracle_sha256=digest,
    )

    assert epoch["correct"] is True
    assert epoch["worker_count"] == 2
    assert epoch["worker_pids"] == [101, 102]
    assert epoch["requests"] >= 4
    assert epoch["wall_ns"] >= 1_000_000
    assert epoch["throughput_qps"] > 0
    assert epoch["roundtrip"]["runs"] == epoch["requests"]
    assert epoch["worker_internal"]["runs"] == epoch["requests"]
    assert (
        sum(len(item["answer_sha256s"]) for item in epoch["workers"])
        == epoch["requests"]
    )
    assert all(
        answer == digest
        for worker in epoch["workers"]
        for answer in worker["answer_sha256s"]
    )


def test_concurrency_aggregate_uses_population_cv() -> None:
    epochs = [
        {
            "requests": 100,
            "throughput_qps": qps,
            "roundtrip": {"p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0},
            "worker_request_counts": [32],
            "correct": True,
        }
        for qps in (90.0, 100.0, 100.0, 110.0)
    ]

    aggregate = regression._aggregate_concurrency(epochs)

    assert aggregate["throughput_qps"] == 100.0
    assert aggregate["throughput_cv"] == 0.070711
    assert aggregate["epoch_count"] == 4
    assert aggregate["requests"] == 400


def test_worker_identity_is_suite_and_harness_specific() -> None:
    provenance = {
        "harness_sha256": {
            "common_pm": "a" * 64,
            "pm4py": "b" * 64,
        }
    }
    regression.validate_worker_identity(
        {"suite": "pm4py", "workload_sha256": "b" * 64},
        provenance,
        "prior",
        "pm4py",
    )
    with pytest.raises(RuntimeError, match="wrong suite"):
        regression.validate_worker_identity(
            {"suite": "common_pm", "workload_sha256": "a" * 64},
            provenance,
            "prior",
            "pm4py",
        )


def test_provenance_hashes_the_shared_bridge_support(monkeypatch) -> None:
    paths: list[Path] = []

    monkeypatch.setattr(
        regression.bridge,
        "capture_provenance",
        lambda controller, worker, workload: {
            "harness_sha256": {
                "controller": "a" * 64,
                "worker": "b" * 64,
                "workload": "c" * 64,
            }
        },
    )

    def file_sha256(path: Path) -> str:
        paths.append(path)
        return "d" * 64

    monkeypatch.setattr(regression.bridge, "file_sha256", file_sha256)
    monkeypatch.setattr(
        regression.bridge,
        "required_environment",
        lambda name: "postgres:16@sha256:" + "e" * 64,
    )

    result = regression.capture_provenance(
        Path("controller.py"),
        Path("worker.py"),
        Path("common.py"),
        Path("pm4py.py"),
    )

    assert result["harness_sha256"]["support"] == "d" * 64
    assert Path(regression.bridge.__file__).resolve() in paths
