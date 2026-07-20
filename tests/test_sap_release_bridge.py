from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from benchmarks import sap_release_bridge_worker as bridge_worker


@dataclass(frozen=True)
class TinyFixture:
    name: str
    from_time: datetime


def tiny_fixture_payload() -> dict[str, Any]:
    return {
        "name": "tiny",
        "from_time": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
    }


def protocol_worker(suite: str, harness: Any) -> bridge_worker.Worker:
    worker = bridge_worker.Worker.__new__(bridge_worker.Worker)
    worker.suite = suite
    worker.harness = harness
    worker.fixture_type = TinyFixture
    worker.workload_names = ("dfg_conformance_95pct",)
    worker.database = SimpleNamespace(connection=object())
    return worker


def test_worker_common_suite_keeps_default_execution_path(monkeypatch) -> None:
    harness = SimpleNamespace(canonical=bridge_worker.common_pm.canonical)
    worker = protocol_worker("common_pm", harness)
    monkeypatch.setattr(
        bridge_worker,
        "execute_workload",
        lambda database, fixture, workload: {
            "fixture": fixture.name,
            "workload": workload,
        },
    )

    result = worker.run(
        {
            "workload": "dfg_conformance_95pct",
            "fixture": tiny_fixture_payload(),
            "timed": False,
        }
    )

    answer = {"fixture": "tiny", "workload": "dfg_conformance_95pct"}
    assert result == {
        "answer_sha256": hashlib.sha256(
            bridge_worker.common_pm.canonical(answer).encode()
        ).hexdigest(),
        "execution_path": "pg_ocpm_rust",
        "input": {"execution_path": "pg_ocpm_rust"},
        "elapsed_ns": None,
    }


def test_worker_pm4py_suite_requires_and_dispatches_execution_path() -> None:
    calls = []

    def run_pm4py(connection, fixture, workload, source):
        calls.append(("pm4py", connection, fixture.name, workload, source))
        return {"answer": ["same"], "input": {"aggregate_rows": 7}}

    def run_ocpm_engine(connection, fixture, workload):
        calls.append(("engine", connection, fixture.name, workload))
        return {"answer": ["same"], "input": {"aggregate_rows": 7}}

    harness = SimpleNamespace(
        canonical=bridge_worker.common_pm.canonical,
        run_pm4py=run_pm4py,
        run_ocpm_engine=run_ocpm_engine,
    )
    worker = protocol_worker("pm4py", harness)
    base_request = {
        "workload": "dfg_conformance_95pct",
        "fixture": tiny_fixture_payload(),
        "timed": False,
    }

    with pytest.raises(ValueError, match="pm4py execution_path"):
        worker.run(base_request)
    pm4py_result = worker.run({**base_request, "execution_path": "pg_ocpm_pm4py"})
    engine_result = worker.run(
        {**base_request, "execution_path": "pg_ocpm_ocpm_engine"}
    )

    assert pm4py_result["answer_sha256"] == engine_result["answer_sha256"]
    assert pm4py_result["input"] == engine_result["input"] == {"aggregate_rows": 7}
    assert [call[0] for call in calls] == ["pm4py", "engine"]


def test_worker_memory_run_returns_exact_hash_and_rss(monkeypatch) -> None:
    harness = SimpleNamespace(
        canonical=bridge_worker.common_pm.canonical,
        run_pm4py=lambda connection, fixture, workload, source: {
            "answer": {"correct": True},
            "input": {"aggregate_rows": 3},
        },
        run_ocpm_engine=lambda connection, fixture, workload: None,
    )
    worker = protocol_worker("pm4py", harness)
    monkeypatch.setattr(bridge_worker, "process_rss_bytes", lambda: 8192)

    result = worker.memory_run(
        {
            "workload": "dfg_conformance_95pct",
            "fixture": tiny_fixture_payload(),
            "execution_path": "pg_ocpm_pm4py",
        }
    )

    expected = hashlib.sha256(
        bridge_worker.common_pm.canonical({"correct": True}).encode()
    ).hexdigest()
    assert result["answer_sha256"] == expected
    assert result["execution_path"] == "pg_ocpm_pm4py"
    assert result["input"] == {"aggregate_rows": 3}
    assert result["baseline_rss_bytes"] == 8192
    assert result["peak_rss_bytes"] == 8192
    assert result["incremental_peak_bytes"] == 0
    assert result["elapsed_ns"] > 0
    assert result["worker_pid"] > 0


def test_worker_storage_details_reports_relation_and_index_levels() -> None:
    class Database:
        def rows(self, sql, params):
            assert params == ("ocpm",)
            if "FROM pg_class class" in sql:
                return [
                    (
                        "edge",
                        "r",
                        100,
                        8,
                        0,
                        40,
                        20,
                        168,
                        "pg_toast_1",
                        8,
                        0,
                        0,
                        12,
                        None,
                        None,
                        1,
                        0,
                    ),
                    (
                        "event",
                        "r",
                        200,
                        0,
                        8,
                        60,
                        0,
                        268,
                        None,
                        0,
                        0,
                        0,
                        0,
                        None,
                        None,
                        1,
                        0,
                    ),
                ]
            if "FROM pg_indexes" in sql:
                return [("edge", "edge_pkey", "CREATE UNIQUE INDEX", 40)]
            if "FROM pg_class owner" in sql:
                return [("edge", "pg_toast_1", "pg_toast_1_index", 12)]
            raise AssertionError(sql)

    worker = bridge_worker.Worker.__new__(bridge_worker.Worker)
    worker.database = Database()

    result = worker.storage_details()

    assert result["schema"] == "ocpm"
    assert result["heap_bytes"] == 300
    assert result["index_bytes"] == 100
    assert result["toast_bytes"] == 20
    assert result["total_bytes"] == 436
    assert result["other_fork_bytes"] == 16
    assert result["relations"][0]["name"] == "edge"
    assert result["relations"][0]["main_fsm_bytes"] == 8
    assert result["relations"][0]["toast"]["index_bytes"] == 12
    assert result["indexes"] == [
        {
            "table": "edge",
            "name": "edge_pkey",
            "definition": "CREATE UNIQUE INDEX",
            "bytes": 40,
        }
    ]
    assert result["toast_indexes"] == [
        {
            "table": "edge",
            "toast_table": "pg_toast_1",
            "name": "pg_toast_1_index",
            "bytes": 12,
        }
    ]


def test_worker_vacuum_analyze_requires_autocommit() -> None:
    statements = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute(self, statement):
            statements.append(statement)

    connection = SimpleNamespace(autocommit=True, cursor=Cursor)
    worker = bridge_worker.Worker.__new__(bridge_worker.Worker)
    worker.database = SimpleNamespace(connection=connection)

    assert worker.vacuum_analyze() == {"completed": True}
    assert statements == ["VACUUM (ANALYZE)"]

    connection.autocommit = False
    with pytest.raises(RuntimeError, match="autocommit"):
        worker.vacuum_analyze()
