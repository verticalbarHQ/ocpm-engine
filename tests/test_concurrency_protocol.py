from __future__ import annotations

import importlib.util
import multiprocessing
import random
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]

if importlib.util.find_spec("psutil") is None:
    sys.modules["psutil"] = ModuleType("psutil")
if importlib.util.find_spec("psycopg2") is None:
    sys.modules["psycopg2"] = ModuleType("psycopg2")


def load_module(name: str, relative_path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


PUBLIC = load_module(
    "benchmark_public_common_pm_test", "benchmarks/public_common_pm.py"
)
SAP = load_module("benchmark_sap_pm4py_test", "benchmarks/sap_pm4py_three_way.py")
PUBLIC_CHECK = load_module(
    "check_public_result_test", "benchmarks/check_public_result.py"
)
SAP_CHECK = load_module(
    "check_sap_pm4py_result_test", "benchmarks/check_sap_pm4py_result.py"
)
_SPAWN_SYNC: dict[str, object] = {}


def spawn_initializer(ready, start, value) -> None:
    _SPAWN_SYNC.update({"ready": ready, "start": start, "value": value})


def spawn_worker(_: int) -> float:
    _SPAWN_SYNC["ready"].wait(timeout=10)
    assert _SPAWN_SYNC["start"].wait(timeout=10)
    return _SPAWN_SYNC["value"].value


def protocol_args() -> SimpleNamespace:
    return SimpleNamespace(
        concurrency_epochs=3,
        concurrency_min_seconds=5.0,
        concurrency_requests=32,
    )


def epoch(number: int, qps: float, p50: float) -> dict:
    return {
        "epoch": number,
        "arm_position": 1,
        "requests": 64,
        "wall_ms": 5000.0,
        "throughput_qps": qps,
        "p50_ms": p50,
        "p95_ms": p50 + 1,
        "p99_ms": p50 + 2,
        "minimum_ms": p50 - 1,
        "maximum_ms": p50 + 3,
        "warmed_worker_count": 2,
        "worker_ids": [101, 102],
        "worker_request_counts": [32, 32],
        "answer_sha256": "0" * 64,
        "correct": True,
    }


def checked_epoch(
    number: int, arm_position: int, workers: int, request_multiplier: int = 1
) -> dict:
    requests_per_worker = 32 * request_multiplier
    requests = workers * requests_per_worker
    return {
        "epoch": number,
        "arm_position": arm_position,
        "requests": requests,
        "wall_ms": 5000.0,
        "throughput_qps": round(requests / 5.0, 3),
        "p50_ms": 1.0,
        "p95_ms": 2.0,
        "p99_ms": 3.0,
        "minimum_ms": 0.5,
        "maximum_ms": 4.0,
        "warmed_worker_count": workers,
        "worker_ids": list(range(100, 100 + workers)),
        "worker_request_counts": [requests_per_worker] * workers,
        "answer_sha256": "0" * 64,
        "correct": True,
    }


def checked_concurrency(
    module,
    engines: tuple[str, ...],
    levels: tuple[str, ...],
    offset_base: int,
) -> dict:
    orders = {}
    per_engine = {engine: {} for engine in engines}
    for level_index, level in enumerate(levels):
        workers = int(level)
        orders[level] = []
        epochs = {engine: [] for engine in engines}
        for epoch_index in range(3):
            offset = (offset_base + level_index + epoch_index) % len(engines)
            order = engines[offset:] + engines[:offset]
            orders[level].append(list(order))
            for engine in engines:
                request_multiplier = {
                    "pg_ocpm_rust": 12,
                    "pg_ocpm_ocpm_engine": 4,
                }.get(engine, 1)
                epochs[engine].append(
                    checked_epoch(
                        epoch_index + 1,
                        order.index(engine) + 1,
                        workers,
                        request_multiplier,
                    )
                )
        for engine in engines:
            per_engine[engine][level] = module.aggregate_concurrency_epochs(
                workers, epochs[engine]
            )
    return {
        "levels": list(levels),
        "epoch_arm_orders": orders,
        **per_engine,
    }


def test_public_and_sap_protocols_share_release_floors() -> None:
    for module in (PUBLIC, SAP):
        method = module.concurrency_method(protocol_args())
        assert method["epochs_per_engine_level"] == 3
        assert method["minimum_epoch_seconds"] == 5.0
        assert method["minimum_requests_per_worker_per_epoch"] == 32
        assert "persistent PostgreSQL connection" in method["connection_model"]


@pytest.mark.parametrize(
    ("checker", "historical_warmups", "historical_runs"),
    ((PUBLIC_CHECK, 2, 9), (SAP_CHECK, 1, 5)),
)
def test_release_latency_protocol_requires_ten_warmups_and_thirty_runs(
    checker,
    historical_warmups,
    historical_runs,
) -> None:
    current = {"warmups": 10, "measured_runs": 30, "latency_scope": "same"}
    historical = {
        "warmups": historical_warmups,
        "measured_runs": historical_runs,
        "latency_scope": "same",
    }

    checker.validate_latency_sample_counts(current)
    assert checker.latency_method(current) == checker.latency_method(historical)
    with pytest.raises(SystemExit, match="10 warmups and 30 measured runs"):
        checker.validate_latency_sample_counts(historical)


@pytest.mark.parametrize("runner", (PUBLIC, SAP))
def test_release_latency_runner_defaults_are_ten_and_thirty(
    runner, monkeypatch
) -> None:
    monkeypatch.setattr(sys, "argv", ["benchmark"])
    args = runner.parse_args()

    assert args.warmups == 10
    assert args.runs == 30


def test_sap_first_execution_counts_require_thirty_nonnegative_integers() -> None:
    valid = {
        "vanilla_pg_pm4py": 10,
        "pg_ocpm_pm4py": 9,
        "pg_ocpm_ocpm_engine": 11,
    }
    SAP_CHECK.validate_first_execution_counts("sap_o2c", "dfg", valid, 30)

    for invalid in (
        {**valid, "pg_ocpm_ocpm_engine": -1},
        {**valid, "pg_ocpm_ocpm_engine": True},
        {**valid, "pg_ocpm_ocpm_engine": 5},
    ):
        with pytest.raises(SystemExit, match="randomized execution counts"):
            SAP_CHECK.validate_first_execution_counts("sap_o2c", "dfg", invalid, 30)


def test_public_latency_rejects_an_early_incorrect_timed_sample() -> None:
    baseline_values = iter((["exact"], ["wrong"], ["exact"]))
    extension_values = iter((["exact"], ["exact"], ["exact"]))
    with pytest.raises(AssertionError, match="measured"):
        PUBLIC.timed_pair(
            lambda: next(baseline_values),
            lambda: next(extension_values),
            warmups=0,
            runs=2,
            rng=random.Random(1),
        )


def test_sap_latency_rejects_an_early_incorrect_timed_sample() -> None:
    values = {
        "vanilla_pg_pm4py": iter(("exact", "wrong", "exact")),
        "pg_ocpm_pm4py": iter(("exact", "exact", "exact")),
        "pg_ocpm_ocpm_engine": iter(("exact", "exact", "exact")),
    }

    def call(name: str):
        return lambda: {"answer": next(values[name]), "input": {"rows": 1}}

    calls = {name: call(name) for name in SAP.ENGINES}
    with pytest.raises(AssertionError, match="measured"):
        SAP.timed_comparison(calls, 0, 2, random.Random(1))


def test_epoch_aggregation_uses_medians_and_retains_evidence() -> None:
    epochs = [epoch(1, 10.0, 3.0), epoch(2, 30.0, 1.0), epoch(3, 20.0, 2.0)]
    for module in (PUBLIC, SAP):
        aggregate = module.aggregate_concurrency_epochs(2, epochs)
        assert aggregate["epoch_count"] == 3
        assert aggregate["throughput_qps"] == 20.0
        assert aggregate["p50_ms"] == 2.0
        assert aggregate["requests"] == 192
        assert aggregate["minimum_epoch_wall_ms"] == 5000.0
        assert aggregate["minimum_requests_per_worker"] == 32
        assert aggregate["epochs"] == epochs


def test_concurrency_only_preserves_non_concurrency_evidence() -> None:
    public_before = {
        "schema_version": 1,
        "generated_at": "before",
        "method": {"measured_runs": 9, "concurrency_model": "legacy"},
        "datasets": [{"fixture": {"name": "sap_o2c"}, "workloads": [1]}],
        "storage": {"total_bytes": 123},
        "concurrency": {"legacy": True},
        "payload_sha256": "old",
    }
    public_after = {
        **public_before,
        "schema_version": 2,
        "generated_at": "after",
        "section_generated_at": {"concurrency": "after"},
        "method": {
            "measured_runs": 9,
            "concurrency": {"epochs_per_engine_level": 3},
        },
        "concurrency": {"epochs": [1, 2, 3]},
        "drift_concurrency": {"epochs": [1, 2, 3]},
        "payload_sha256": "new",
    }
    assert PUBLIC.preserved_concurrency_only_payload(
        public_before
    ) == PUBLIC.preserved_concurrency_only_payload(public_after)

    sap_before = {
        "schema_version": 1,
        "generated_at": "before",
        "method": {"measured_runs": 5, "concurrency_model": "legacy"},
        "datasets": [
            {
                "dataset": "sap_o2c",
                "latency": [1],
                "memory": {"rss": 456},
                "concurrency": {"legacy": True},
            }
        ],
        "storage": {"total_bytes": 789},
        "payload_sha256": "old",
    }
    sap_after = {
        **sap_before,
        "schema_version": 2,
        "generated_at": "after",
        "section_generated_at": {"concurrency": "after"},
        "method": {
            "measured_runs": 5,
            "concurrency": {"epochs_per_engine_level": 3},
        },
        "datasets": [
            {
                **sap_before["datasets"][0],
                "concurrency": {"epochs": [1, 2, 3]},
            }
        ],
        "payload_sha256": "new",
    }
    assert SAP.preserved_concurrency_only_payload(
        sap_before
    ) == SAP.preserved_concurrency_only_payload(sap_after)


def test_spawn_context_shares_epoch_barrier_event_and_deadline() -> None:
    context = multiprocessing.get_context("spawn")
    ready = context.Barrier(3)
    start = context.Event()
    value = context.Value("d", 0.0)
    with ProcessPoolExecutor(
        max_workers=2,
        mp_context=context,
        initializer=spawn_initializer,
        initargs=(ready, start, value),
    ) as pool:
        futures = [pool.submit(spawn_worker, slot) for slot in range(2)]
        ready.wait(timeout=10)
        with value.get_lock():
            value.value = 5.0
        start.set()
        assert [future.result(timeout=10) for future in futures] == [5.0, 5.0]


def test_public_epoch_uses_every_persistent_thread_worker(monkeypatch) -> None:
    opened = []

    class FakeDatabase:
        def __init__(self, *_args, **_kwargs) -> None:
            opened.append(self)

        def close(self) -> None:
            pass

    monkeypatch.setattr(PUBLIC, "Database", FakeDatabase)
    monkeypatch.setattr(PUBLIC, "base_pair_counts", lambda *_args: [])
    monkeypatch.setattr(PUBLIC, "reference_dfg", lambda _rows: {"exact": True})
    args = SimpleNamespace(
        baseline_host="baseline",
        extension_host="extension",
        baseline_db="db",
        extension_db="db",
        timeout_seconds=1,
        concurrency_requests=2,
        concurrency_min_seconds=0.01,
    )
    result = PUBLIC.run_concurrency_epoch(
        args,
        fixture=None,
        expected=PUBLIC.canonical({"exact": True}),
        extension=False,
        drift=False,
        workers=2,
    )
    assert len(opened) == 2
    assert result["warmed_worker_count"] == 2
    assert len(result["worker_ids"]) == 2
    assert min(result["worker_request_counts"]) >= 2
    assert result["wall_ms"] >= 10
    assert result["correct"] is True


def test_public_checker_accepts_only_full_epoch_evidence() -> None:
    section = {
        "fixture": "sap_o2c",
        "workload": "dfg_conformance_95pct",
        **checked_concurrency(
            PUBLIC,
            PUBLIC_CHECK.EXPECTED_CONCURRENCY_ENGINES,
            PUBLIC_CHECK.EXPECTED_CONCURRENCY_LEVELS,
            0,
        ),
    }
    PUBLIC_CHECK.validate_concurrency_section("concurrency", section)
    section["pg_ocpm_rust"]["16"]["epochs"][0]["wall_ms"] = 4999.0
    try:
        PUBLIC_CHECK.validate_concurrency_section("concurrency", section)
    except SystemExit:
        pass
    else:  # pragma: no cover - assertion branch
        raise AssertionError("short concurrency epoch was accepted")


def test_sap_checker_accepts_rotated_full_epoch_evidence() -> None:
    section = {
        "workload": "dfg_conformance_95pct",
        **checked_concurrency(SAP, SAP_CHECK.ENGINES, SAP_CHECK.EXPECTED_LEVELS, 0),
    }
    SAP_CHECK.validate_concurrency("sap_o2c", 0, section, "0" * 64)
