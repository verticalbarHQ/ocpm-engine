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
import sys
import time
import traceback
from dataclasses import fields
from datetime import datetime
from importlib import metadata
from pathlib import Path
from typing import Any

import public_common_pm as common_pm

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

_TIME_FIELDS = {"from_time", "train_to", "test_from", "to_time"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", required=True, choices=("prior", "current"))
    parser.add_argument("--host", required=True)
    parser.add_argument("--database", default="ocel_benchmark")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--expected-engine-version", required=True)
    parser.add_argument("--expected-pg-ocpm-version", required=True)
    return parser.parse_args()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(common_pm.canonical(value).encode()).hexdigest()


def fixture_from_payload(payload: object) -> common_pm.Fixture:
    if not isinstance(payload, dict):
        raise TypeError("fixture must be a JSON object")
    expected = {field.name for field in fields(common_pm.Fixture)}
    if set(payload) != expected:
        raise ValueError(
            "fixture fields changed: "
            f"expected {sorted(expected)}, received {sorted(payload)}"
        )
    values = dict(payload)
    for name in _TIME_FIELDS:
        value = values[name]
        if not isinstance(value, str):
            raise TypeError(f"fixture {name} must be an ISO-8601 string")
        values[name] = datetime.fromisoformat(value)
    return common_pm.Fixture(**values)


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


class Worker:
    def __init__(self, args: argparse.Namespace):
        self.args = args
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
        return {
            "arm": self.args.arm,
            "ocpm_engine": self.engine_version,
            "native_ocpm_engine": self.native_version,
            "pg_ocpm": self.pg_ocpm_version,
            "pg_extension": self.extension_version,
            "python": sys.version.split()[0],
            "executable": sys.executable,
            "package_path": str(importlib.import_module("ocpm_engine").__file__),
            "database_environment": common_pm.database_environment(self.database),
            "workload_sha256": file_sha256(Path(common_pm.__file__).resolve()),
        }

    def run(self, request: dict[str, Any]) -> dict[str, Any]:
        workload = request.get("workload")
        if workload not in WORKLOAD_NAMES:
            raise ValueError(f"unsupported workload: {workload!r}")
        fixture = fixture_from_payload(request.get("fixture"))
        timed = request.get("timed")
        if type(timed) is not bool:
            raise TypeError("timed must be a JSON boolean")
        if timed:
            started_ns = time.perf_counter_ns()
            answer = execute_workload(self.database, fixture, workload)
            elapsed_ns = time.perf_counter_ns() - started_ns
            if elapsed_ns <= 0:
                raise RuntimeError("time.perf_counter_ns did not advance")
        else:
            answer = execute_workload(self.database, fixture, workload)
            elapsed_ns = None
        return {
            "answer_sha256": canonical_sha256(answer),
            "elapsed_ns": elapsed_ns,
        }

    def storage(self) -> dict[str, int]:
        return common_pm.schema_storage(self.database, "ocpm")

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
