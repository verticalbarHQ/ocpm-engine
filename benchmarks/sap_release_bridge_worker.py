"""Persistent release-isolated worker for the SAP common-PM bridge.

The controller starts one copy of this program for each release with an
arm-specific ``PYTHONPATH``.  Requests and responses use one JSON object per
line.  Only database extraction, model construction, and scoring are inside
the worker's timer; JSONL IPC and canonical answer hashing are deliberately
outside it.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import sys
import threading
import time
import traceback
from dataclasses import fields
from datetime import datetime
from importlib import metadata
from pathlib import Path
from typing import Any, Callable

try:
    import public_common_pm as common_pm
except ModuleNotFoundError:  # imported as benchmarks.sap_release_bridge_worker
    from benchmarks import public_common_pm as common_pm

COMMON_PM_WORKLOAD_NAMES = (
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
PM4PY_WORKLOAD_NAMES = (
    "dfg_conformance_95pct",
    "variant_conformance_95pct",
    "next_activity_prediction",
    "edge_bottleneck_ranking",
)
PM4PY_EXECUTION_PATHS = ("pg_ocpm_pm4py", "pg_ocpm_ocpm_engine")
COMMON_PM_EXECUTION_PATH = "pg_ocpm_rust"
WORKLOAD_NAMES = COMMON_PM_WORKLOAD_NAMES
SUITE_NAMES = ("common_pm", "pm4py")

_TIME_FIELDS = {"from_time", "train_to", "test_from", "to_time"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", required=True, choices=("prior", "current"))
    parser.add_argument("--host", required=True)
    parser.add_argument("--database", default="ocel_benchmark")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--expected-engine-version", required=True)
    parser.add_argument("--expected-pg-ocpm-version", required=True)
    parser.add_argument("--suite", choices=SUITE_NAMES, default="common_pm")
    return parser.parse_args()


def canonical_sha256(
    value: Any,
    canonicalizer: Callable[[Any], str] = common_pm.canonical,
) -> str:
    return hashlib.sha256(canonicalizer(value).encode()).hexdigest()


def fixture_from_payload(
    payload: object,
    fixture_type: type[Any] = common_pm.Fixture,
) -> Any:
    if not isinstance(payload, dict):
        raise TypeError("fixture must be a JSON object")
    expected = {field.name for field in fields(fixture_type)}
    if set(payload) != expected:
        raise ValueError(
            "fixture fields changed: "
            f"expected {sorted(expected)}, received {sorted(payload)}"
        )
    values = dict(payload)
    for name in _TIME_FIELDS & expected:
        value = values[name]
        if not isinstance(value, str):
            raise TypeError(f"fixture {name} must be an ISO-8601 string")
        values[name] = datetime.fromisoformat(value)
    return fixture_type(**values)


def execute_workload(
    database: common_pm.Database,
    fixture: common_pm.Fixture,
    workload: str,
) -> Any:
    """Execute the pg_ocpm plus native-kernel side of a public workload."""

    if workload == "dfg_conformance_95pct":
        return common_pm.native_dfg(
            common_pm.transition_rows(common_pm.ext_pair_counts(database, fixture))
        )
    if workload == "variant_conformance_95pct":
        return common_pm.variant_result(
            common_pm.ext_variant_counts(database, fixture), True
        )
    if workload == "next_activity_prediction":
        return common_pm.native_next(
            common_pm.transition_rows(common_pm.ext_pair_counts(database, fixture))
        )
    if workload == "dfg_frequency_drift":
        return common_pm.native_drift(
            common_pm.transition_rows(common_pm.ext_pair_counts(database, fixture))
        )
    if workload == "repeated_transition_rework":
        return common_pm.normalize_rows(
            database.rows(
                common_pm.REWORK_EXT,
                common_pm.params(fixture, True),
            )
        )
    if workload == "edge_bottleneck_ranking":
        return common_pm.bottleneck(database, fixture, True)
    if workload == "edge_bottleneck_prediction":
        return common_pm.edge_prediction(database, fixture, True)
    if workload == "edge_duration_time_series":
        return common_pm.duration_series(database, fixture, True)
    if workload == "activity_profile":
        return common_pm.activity_profile(database, fixture, True)
    raise ValueError(f"unsupported workload: {workload}")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_pm4py_harness() -> Any:
    try:
        return importlib.import_module("sap_pm4py_three_way")
    except ModuleNotFoundError:
        return importlib.import_module("benchmarks.sap_pm4py_three_way")


def process_rss_bytes() -> int:
    psutil = importlib.import_module("psutil")
    return int(psutil.Process().memory_info().rss)


class Worker:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.suite = getattr(args, "suite", "common_pm")
        if self.suite == "common_pm":
            self.harness = common_pm
            self.fixture_type = common_pm.Fixture
            self.workload_names = COMMON_PM_WORKLOAD_NAMES
        elif self.suite == "pm4py":
            self.harness = load_pm4py_harness()
            self.fixture_type = self.harness.Fixture
            self.workload_names = tuple(self.harness.WORKLOADS)
            if self.workload_names != PM4PY_WORKLOAD_NAMES:
                raise RuntimeError(
                    "sap_pm4py_three_way workload contract changed: "
                    f"expected {PM4PY_WORKLOAD_NAMES}, received {self.workload_names}"
                )
        else:
            raise ValueError(f"unsupported suite: {self.suite!r}")
        self.database = common_pm.Database(
            args.host,
            args.database,
            args.timeout_seconds,
        )
        self.engine_version = metadata.version("ocpm-engine")
        native = importlib.import_module("ocpm_engine._native")
        self.native_version = str(getattr(native, "__version__", ""))
        self.extension_version = str(
            self.database.one(
                "SELECT extversion FROM pg_extension WHERE extname='pg_ocpm'",
                {},
            )
        )
        self.pg_ocpm_version = str(self.database.one("SELECT ocpm.version()", {}))
        if self.engine_version != args.expected_engine_version:
            raise RuntimeError(
                f"{args.arm} worker loaded ocpm-engine {self.engine_version}; "
                f"expected {args.expected_engine_version}"
            )
        if self.native_version != args.expected_engine_version:
            raise RuntimeError(
                f"{args.arm} worker loaded native module {self.native_version}; "
                f"expected {args.expected_engine_version}"
            )
        if self.extension_version != args.expected_pg_ocpm_version:
            raise RuntimeError(
                f"{args.arm} database extension is {self.extension_version}; "
                f"expected {args.expected_pg_ocpm_version}"
            )
        if self.pg_ocpm_version != args.expected_pg_ocpm_version:
            raise RuntimeError(
                f"{args.arm} ocpm.version() returned {self.pg_ocpm_version}; "
                f"expected {args.expected_pg_ocpm_version}"
            )

    def hello(self) -> dict[str, Any]:
        database_environment = common_pm.database_environment(self.database)
        database_environment["autovacuum"] = str(
            self.database.one("SELECT current_setting('autovacuum')", {})
        )
        return {
            "arm": self.args.arm,
            "ocpm_engine": self.engine_version,
            "native_ocpm_engine": self.native_version,
            "pg_ocpm": self.pg_ocpm_version,
            "pg_extension": self.extension_version,
            "suite": self.suite,
            "python": sys.version.split()[0],
            "pm4py": metadata.version("pm4py"),
            "psutil": metadata.version("psutil"),
            "executable": sys.executable,
            "package_path": str(importlib.import_module("ocpm_engine").__file__),
            "database_environment": database_environment,
            "workload_sha256": file_sha256(Path(self.harness.__file__).resolve()),
        }

    def request_workload(self, request: dict[str, Any]) -> tuple[str, Any, str]:
        workload = request.get("workload")
        if workload not in self.workload_names:
            raise ValueError(f"unsupported workload: {workload!r}")
        fixture = fixture_from_payload(request.get("fixture"), self.fixture_type)
        execution_path = request.get("execution_path")
        if self.suite == "common_pm":
            if execution_path is None:
                execution_path = COMMON_PM_EXECUTION_PATH
            if execution_path != COMMON_PM_EXECUTION_PATH:
                raise ValueError(
                    f"common_pm execution_path must be {COMMON_PM_EXECUTION_PATH!r}"
                )
        elif execution_path not in PM4PY_EXECUTION_PATHS:
            raise ValueError(
                "pm4py execution_path must be one of "
                f"{PM4PY_EXECUTION_PATHS}; received {execution_path!r}"
            )
        return str(workload), fixture, str(execution_path)

    def execute(
        self,
        fixture: Any,
        workload: str,
        execution_path: str,
    ) -> dict[str, Any]:
        if self.suite == "common_pm":
            return {
                "answer": execute_workload(self.database, fixture, workload),
                "input": {"execution_path": COMMON_PM_EXECUTION_PATH},
            }
        if execution_path == "pg_ocpm_pm4py":
            result = self.harness.run_pm4py(
                self.database.connection,
                fixture,
                workload,
                "pg_ocpm",
            )
        else:
            result = self.harness.run_ocpm_engine(
                self.database.connection,
                fixture,
                workload,
            )
        if (
            not isinstance(result, dict)
            or "answer" not in result
            or not isinstance(result.get("input"), dict)
        ):
            raise RuntimeError("sap_pm4py_three_way returned an invalid result")
        return result

    def answer_sha256(self, answer: Any) -> str:
        return canonical_sha256(answer, self.harness.canonical)

    def run(self, request: dict[str, Any]) -> dict[str, Any]:
        workload, fixture, execution_path = self.request_workload(request)
        timed = request.get("timed")
        if type(timed) is not bool:
            raise TypeError("timed must be a JSON boolean")
        if timed:
            started_ns = time.perf_counter_ns()
            result = self.execute(fixture, workload, execution_path)
            elapsed_ns = time.perf_counter_ns() - started_ns
            if elapsed_ns <= 0:
                raise RuntimeError("time.perf_counter_ns did not advance")
        else:
            result = self.execute(fixture, workload, execution_path)
            elapsed_ns = None
        return {
            "answer_sha256": self.answer_sha256(result["answer"]),
            "execution_path": execution_path,
            "input": result["input"],
            "elapsed_ns": elapsed_ns,
        }

    def memory_run(self, request: dict[str, Any]) -> dict[str, Any]:
        workload, fixture, execution_path = self.request_workload(request)
        baseline_rss = process_rss_bytes()
        peak_rss = baseline_rss
        stop = threading.Event()

        def sample_rss() -> None:
            nonlocal peak_rss
            while not stop.wait(0.001):
                peak_rss = max(peak_rss, process_rss_bytes())

        monitor = threading.Thread(target=sample_rss, daemon=True)
        monitor.start()
        started_ns = time.perf_counter_ns()
        try:
            result = self.execute(fixture, workload, execution_path)
            elapsed_ns = time.perf_counter_ns() - started_ns
        finally:
            peak_rss = max(peak_rss, process_rss_bytes())
            stop.set()
            monitor.join()
        if elapsed_ns <= 0:
            raise RuntimeError("time.perf_counter_ns did not advance")
        return {
            "worker_pid": os.getpid(),
            "answer_sha256": self.answer_sha256(result["answer"]),
            "execution_path": execution_path,
            "input": result["input"],
            "baseline_rss_bytes": baseline_rss,
            "peak_rss_bytes": peak_rss,
            "incremental_peak_bytes": max(0, peak_rss - baseline_rss),
            "elapsed_ns": elapsed_ns,
        }

    def storage(self) -> dict[str, int]:
        return common_pm.schema_storage(self.database, "ocpm")

    def storage_details(self) -> dict[str, Any]:
        relation_rows = self.database.rows(
            """
            SELECT class.relname,class.relkind,
                   pg_relation_size(class.oid,'main')::bigint,
                   pg_relation_size(class.oid,'fsm')::bigint,
                   pg_relation_size(class.oid,'vm')::bigint,
                   pg_indexes_size(class.oid)::bigint,
                   CASE WHEN class.reltoastrelid=0 THEN 0
                        ELSE pg_total_relation_size(class.reltoastrelid) END::bigint,
                   pg_total_relation_size(class.oid)::bigint,
                   CASE WHEN class.reltoastrelid=0 THEN NULL
                        ELSE toast.relname END,
                   CASE WHEN class.reltoastrelid=0 THEN 0
                        ELSE pg_relation_size(class.reltoastrelid,'main') END::bigint,
                   CASE WHEN class.reltoastrelid=0 THEN 0
                        ELSE pg_relation_size(class.reltoastrelid,'fsm') END::bigint,
                   CASE WHEN class.reltoastrelid=0 THEN 0
                        ELSE pg_relation_size(class.reltoastrelid,'vm') END::bigint,
                   CASE WHEN class.reltoastrelid=0 THEN 0
                        ELSE pg_indexes_size(class.reltoastrelid) END::bigint,
                   stats.last_vacuum,stats.last_autovacuum,
                   coalesce(stats.vacuum_count,0)::bigint,
                   coalesce(stats.autovacuum_count,0)::bigint
            FROM pg_class class
            JOIN pg_namespace namespace ON namespace.oid=class.relnamespace
            LEFT JOIN pg_class toast ON toast.oid=class.reltoastrelid
            LEFT JOIN pg_stat_all_tables stats ON stats.relid=class.oid
            WHERE namespace.nspname=%s AND class.relkind IN ('r','m','p')
            ORDER BY class.relname
            """,
            ("ocpm",),
        )
        relations = [
            {
                "name": str(name),
                "kind": str(kind),
                "heap_bytes": int(main),
                "main_fsm_bytes": int(fsm),
                "main_vm_bytes": int(vm),
                "index_bytes": int(indexes),
                "toast_bytes": int(toast_total),
                "total_bytes": int(total),
                "toast": (
                    None
                    if toast_name is None
                    else {
                        "name": str(toast_name),
                        "main_bytes": int(toast_main),
                        "fsm_bytes": int(toast_fsm),
                        "vm_bytes": int(toast_vm),
                        "index_bytes": int(toast_indexes),
                        "total_bytes": int(toast_total),
                    }
                ),
                "maintenance": {
                    "last_vacuum": (
                        None if last_vacuum is None else last_vacuum.isoformat()
                    ),
                    "last_autovacuum": (
                        None if last_autovacuum is None else last_autovacuum.isoformat()
                    ),
                    "vacuum_count": int(vacuum_count),
                    "autovacuum_count": int(autovacuum_count),
                },
            }
            for (
                name,
                kind,
                main,
                fsm,
                vm,
                indexes,
                toast_total,
                total,
                toast_name,
                toast_main,
                toast_fsm,
                toast_vm,
                toast_indexes,
                last_vacuum,
                last_autovacuum,
                vacuum_count,
                autovacuum_count,
            ) in relation_rows
        ]
        index_rows = self.database.rows(
            """
            SELECT tablename,indexname,indexdef,
                   pg_relation_size(
                       (quote_ident(schemaname)||'.'||quote_ident(indexname))::regclass
                   )::bigint
            FROM pg_indexes
            WHERE schemaname=%s
            ORDER BY tablename,indexname
            """,
            ("ocpm",),
        )
        toast_index_rows = self.database.rows(
            """
            SELECT owner.relname,toast.relname,index_class.relname,
                   pg_relation_size(index_class.oid)::bigint
            FROM pg_class owner
            JOIN pg_namespace namespace ON namespace.oid=owner.relnamespace
            JOIN pg_class toast ON toast.oid=owner.reltoastrelid
            JOIN pg_index toast_index ON toast_index.indrelid=toast.oid
            JOIN pg_class index_class ON index_class.oid=toast_index.indexrelid
            WHERE namespace.nspname=%s AND owner.relkind IN ('r','m','p')
            ORDER BY owner.relname,index_class.relname
            """,
            ("ocpm",),
        )
        return {
            "schema": "ocpm",
            "heap_bytes": sum(row["heap_bytes"] for row in relations),
            "index_bytes": sum(row["index_bytes"] for row in relations),
            "toast_bytes": sum(row["toast_bytes"] for row in relations),
            "total_bytes": sum(row["total_bytes"] for row in relations),
            "other_fork_bytes": sum(
                row["total_bytes"]
                - row["heap_bytes"]
                - row["index_bytes"]
                - row["toast_bytes"]
                for row in relations
            ),
            "relations": relations,
            "indexes": [
                {
                    "table": str(table),
                    "name": str(name),
                    "definition": str(definition),
                    "bytes": int(size),
                }
                for table, name, definition, size in index_rows
            ],
            "toast_indexes": [
                {
                    "table": str(table),
                    "toast_table": str(toast_table),
                    "name": str(name),
                    "bytes": int(size),
                }
                for table, toast_table, name, size in toast_index_rows
            ],
        }

    def vacuum_analyze(self) -> dict[str, bool]:
        if self.database.connection.autocommit is not True:
            raise RuntimeError("VACUUM (ANALYZE) requires an autocommit connection")
        with self.database.connection.cursor() as cursor:
            cursor.execute("VACUUM (ANALYZE)")
        return {"completed": True}

    def close(self) -> None:
        self.database.close()


def emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def serve(worker: Worker) -> None:
    for raw_line in sys.stdin:
        request_id: object = None
        try:
            request = json.loads(raw_line)
            if not isinstance(request, dict):
                raise TypeError("request must be a JSON object")
            request_id = request.get("id")
            operation = request.get("op")
            if operation == "hello":
                result = worker.hello()
            elif operation == "run":
                result = worker.run(request)
            elif operation == "storage":
                result = worker.storage()
            elif operation == "storage_details":
                result = worker.storage_details()
            elif operation == "memory_run":
                result = worker.memory_run(request)
            elif operation == "vacuum_analyze":
                result = worker.vacuum_analyze()
            elif operation == "shutdown":
                emit({"id": request_id, "ok": True, "result": {"closed": True}})
                return
            else:
                raise ValueError(f"unsupported operation: {operation!r}")
            emit({"id": request_id, "ok": True, "result": result})
        except Exception as error:  # keep protocol errors machine-readable
            traceback.print_exc(file=sys.stderr)
            emit(
                {
                    "id": request_id,
                    "ok": False,
                    "error": f"{type(error).__name__}: {error}",
                }
            )


def main() -> None:
    worker = Worker(parse_args())
    try:
        serve(worker)
    finally:
        worker.close()


if __name__ == "__main__":
    main()
