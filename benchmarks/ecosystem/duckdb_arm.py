"""Benchmark the DuckDB Parquet + ocpm-engine arm on ecosystem-native data."""

from __future__ import annotations

import argparse
import ctypes
import json
import multiprocessing as mp
import os
import platform
import shutil
import statistics
import threading
import time
from datetime import datetime, timedelta, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

import psutil

from benchmarks.ecosystem.common import (
    WORKLOADS,
    Fixture,
    canonical,
    canonical_variant,
    dfg_score_from_counts,
    latency_metrics,
    measure_serial,
    next_score_from_counts,
    variant_score_from_counts,
)

ARM = "duckdb_parquet_ocpm_engine"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--snapshot-dir", default="/results/duckdb-snapshots")
    parser.add_argument("--output", required=True)
    parser.add_argument("--datasets", default="")
    parser.add_argument("--rebuild-snapshot", action="store_true")
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--latency-epochs", type=int, default=3)
    parser.add_argument("--concurrency", default="1,2,4,8")
    parser.add_argument("--concurrency-epochs", type=int, default=3)
    parser.add_argument("--concurrency-min-seconds", type=float, default=5.0)
    parser.add_argument("--concurrency-requests", type=int, default=32)
    return parser.parse_args()


def epoch_nanos(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch
    return (
        delta.days * 86_400_000_000_000
        + delta.seconds * 1_000_000_000
        + delta.microseconds * 1_000
    )


def view(start: datetime, end: datetime, object_type: str) -> dict[str, Any]:
    return {
        "start": {"epoch_nanos_utc": epoch_nanos(start)},
        "end": {"epoch_nanos_utc": epoch_nanos(end)},
        "object_types": [object_type],
    }


def source_configuration(
    root: Path,
    *,
    pool_size: int = 1,
    read_only: bool = True,
    result_cache_bytes: int = 67_108_864,
    materialize_execution_relation: bool = True,
) -> dict[str, Any]:
    return {
        "database": {
            "kind": "existing",
            "path": str(root / "ocpm-engine.duckdb"),
            "read_only": read_only,
        },
        "location": {"kind": "local", "root": str(root)},
        "snapshot": {"kind": "current", "pointer": "CURRENT"},
        "layout": {"kind": "canonical_v1"},
        "cache": {"kind": "direct"},
        "validation": "balanced",
        "options": {
            "memory_budget_bytes": 536_870_912,
            "max_parallelism": max(1, min(4, os.cpu_count() or 1)),
            "connection_pool_size": pool_size,
            "max_temp_bytes": 4_294_967_296,
            "result_cache_bytes": result_cache_bytes,
            "cache_canonical_fallback": True,
            "materialize_execution_relation": materialize_execution_relation,
            "extension_policy": "preinstalled",
        },
    }


def provision_catalog_with_deployment_client(path: Path) -> None:
    """Create the benchmark catalog outside ocpm-engine via libduckdb's C API."""

    library = ctypes.CDLL(os.environ.get("OCPM_DUCKDB_LIBRARY", "libduckdb.so"))
    library.duckdb_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
    library.duckdb_open.restype = ctypes.c_int
    library.duckdb_close.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    database = ctypes.c_void_p()
    state = library.duckdb_open(os.fsencode(path), ctypes.byref(database))
    if state != 0:
        raise RuntimeError(f"deployment DuckDB client could not create {path}")
    library.duckdb_close(ctypes.byref(database))


def ensure_snapshot(source_sqlite: Path, root: Path, version: str) -> dict[str, Any]:
    from ocpm_engine import StandaloneEngine

    current = root / "CURRENT"
    conversion_ms = 0.0
    if not current.exists():
        started = time.perf_counter_ns()
        root.mkdir(parents=True, exist_ok=True)
        importer = StandaloneEngine.from_sqlite(str(source_sqlite))
        importer.write_parquet_snapshot(root, version)
        conversion_ms = (time.perf_counter_ns() - started) / 1_000_000
    database_path = root / "ocpm-engine.duckdb"
    if not database_path.exists():
        provision_catalog_with_deployment_client(database_path)
    source = source_configuration(root, read_only=True)
    opened = time.perf_counter_ns()
    engine = StandaloneEngine.from_duckdb_parquet(source)
    open_ms = (time.perf_counter_ns() - opened) / 1_000_000
    return {
        "engine": engine,
        "source": source,
        "snapshot_conversion_ms": conversion_ms,
        "connection_open_ms": open_ms,
    }


def summary(engine, fixture: Fixture, start: datetime, end: datetime) -> dict[str, Any]:
    request = {
        "view": view(start, end, fixture.object_type),
        "leading_object_type": fixture.object_type,
        "complete_lifecycle": True,
    }
    return engine.execution_summary(request)


def dfg_counts(value: dict[str, Any]) -> dict[tuple[str, str], int]:
    return {
        (str(row["source"]), str(row["target"])): int(row["frequency"])
        for row in value["dfg"]
    }


def variant_counts(value: dict[str, Any]) -> dict[str, int]:
    return {
        canonical_variant(row["activity_path"]): int(row["frequency"])
        for row in value["variants"]
    }


def run_workload(engine, fixture: Fixture, workload: str) -> dict[str, Any]:
    if workload == "edge_bottleneck_ranking":
        full = summary(engine, fixture, fixture.from_time, fixture.to_time)
        answer = [
            [
                str(row["source"]),
                str(row["target"]),
                int(row["frequency"]),
                round(float(row["mean_duration_seconds"]), 6),
            ]
            for row in full["dfg"]
        ]
        answer.sort(key=lambda row: (-row[3], -row[2], row[0], row[1]))
        return {
            "answer": answer,
            "input": {
                "source": "duckdb_parquet_execution_summary",
                "selected_cases": int(full["case_count"]),
                "event_rows": int(full["event_count"]),
                "aggregate_rows": len(answer),
            },
        }

    # DatasetView uses an exclusive upper bound; the fixture's train boundary is
    # inclusive and stored at microsecond precision.
    train_end = fixture.train_to + timedelta(microseconds=1)
    train = summary(engine, fixture, fixture.from_time, train_end)
    test = summary(engine, fixture, fixture.test_from, fixture.to_time)
    if workload == "dfg_conformance_95pct":
        answer = dfg_score_from_counts(dfg_counts(train), dfg_counts(test))
        aggregate_rows = len(set(dfg_counts(train)) | set(dfg_counts(test)))
    elif workload == "next_activity_prediction":
        answer = next_score_from_counts(dfg_counts(train), dfg_counts(test))
        aggregate_rows = len(set(dfg_counts(train)) | set(dfg_counts(test)))
    elif workload == "variant_conformance_95pct":
        answer = variant_score_from_counts(variant_counts(train), variant_counts(test))
        aggregate_rows = len(set(variant_counts(train)) | set(variant_counts(test)))
    else:
        raise ValueError(workload)
    return {
        "answer": answer,
        "input": {
            "source": "duckdb_parquet_execution_summary",
            "selected_cases": int(train["case_count"]) + int(test["case_count"]),
            "event_rows": int(train["event_count"]) + int(test["event_count"]),
            "aggregate_rows": aggregate_rows,
        },
    }


def sample_peak_rss(call) -> tuple[dict[str, Any], dict[str, int]]:
    process = psutil.Process()
    baseline = int(process.memory_info().rss)
    peak = baseline
    stop = threading.Event()

    def monitor() -> None:
        nonlocal peak
        while not stop.wait(0.001):
            peak = max(peak, int(process.memory_info().rss))

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    try:
        value = call()
    finally:
        peak = max(peak, int(process.memory_info().rss))
        stop.set()
        thread.join()
    return value, {
        "baseline_rss_bytes": baseline,
        "peak_rss_bytes": peak,
        "incremental_peak_bytes": max(0, peak - baseline),
    }


def concurrency_worker(
    source: dict[str, Any],
    fixture_dict: dict[str, Any],
    expected: str,
    start: Any,
    ready: Any,
    output: Any,
    minimum_seconds: float,
    minimum_requests: int,
) -> None:
    from ocpm_engine import StandaloneEngine

    engine = StandaloneEngine.from_duckdb_parquet(source)
    fixture = Fixture.from_dict(fixture_dict)
    ready.put(True)
    start.wait()
    samples = []
    correct = True
    started = time.perf_counter()
    while len(samples) < minimum_requests or time.perf_counter() - started < minimum_seconds:
        request_started = time.perf_counter_ns()
        value = run_workload(engine, fixture, "dfg_conformance_95pct")
        samples.append(time.perf_counter_ns() - request_started)
        correct = correct and canonical(value["answer"]) == expected
    elapsed = time.perf_counter() - started
    process = psutil.Process()
    memory = process.memory_full_info()
    output.put(
        {
            "samples_ns": samples,
            "elapsed_seconds": elapsed,
            "correct": correct,
            "rss_bytes": int(memory.rss),
            "pss_bytes": int(getattr(memory, "pss", 0)),
        }
    )


def concurrency_epoch(
    source: dict[str, Any],
    fixture: Fixture,
    expected: str,
    workers: int,
    minimum_seconds: float,
    minimum_requests: int,
) -> dict[str, Any]:
    context = mp.get_context("spawn")
    start = context.Event()
    ready = context.Queue()
    output = context.Queue()
    processes = [
        context.Process(
            target=concurrency_worker,
            args=(
                source,
                fixture.to_dict(),
                expected,
                start,
                ready,
                output,
                minimum_seconds,
                minimum_requests,
            ),
        )
        for _ in range(workers)
    ]
    for process in processes:
        process.start()
    for _ in processes:
        ready.get(timeout=180)
    wall_started = time.perf_counter()
    start.set()
    values = [output.get(timeout=max(180, int(minimum_seconds * 20))) for _ in processes]
    wall_elapsed = time.perf_counter() - wall_started
    for process in processes:
        process.join(timeout=30)
        if process.exitcode != 0:
            raise RuntimeError(f"DuckDB concurrency worker exited {process.exitcode}")
    samples = [sample for value in values for sample in value["samples_ns"]]
    return {
        "correct": all(value["correct"] for value in values),
        "requests": len(samples),
        "elapsed_seconds": wall_elapsed,
        "throughput_qps": len(samples) / wall_elapsed,
        **latency_metrics(samples),
        "summed_worker_rss_bytes": sum(value["rss_bytes"] for value in values),
        "summed_worker_pss_bytes": sum(value["pss_bytes"] for value in values),
    }


def aggregate_epochs(workers: int, epochs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "workers": workers,
        "correct": all(epoch["correct"] for epoch in epochs),
        "throughput_qps": statistics.median(epoch["throughput_qps"] for epoch in epochs),
        "p50_ms": statistics.median(epoch["p50_ms"] for epoch in epochs),
        "p95_ms": statistics.median(epoch["p95_ms"] for epoch in epochs),
        "epochs": epochs,
    }


def directory_bytes(path: Path) -> int:
    return sum(value.stat().st_size for value in path.rglob("*") if value.is_file())


def run(args: argparse.Namespace) -> None:
    from ocpm_engine import StandaloneEngine

    manifest = json.loads(Path(args.manifest).read_text())
    selected = {value for value in args.datasets.split(",") if value}
    datasets = []
    process = psutil.Process()
    baseline_rss = int(process.memory_info().rss)
    for entry in manifest["datasets"]:
        if selected and entry["name"] not in selected:
            continue
        fixture = Fixture.from_dict(entry["fixture"])
        source_sqlite = Path(args.data_dir) / entry["filename"]
        root = Path(args.snapshot_dir) / entry["sqlite_sha256"]
        if args.rebuild_snapshot and root.exists():
            snapshot_root = Path(args.snapshot_dir).resolve()
            resolved_root = root.resolve()
            if resolved_root.parent != snapshot_root:
                raise RuntimeError("refusing to rebuild a snapshot outside snapshot-dir")
            shutil.rmtree(resolved_root)
        prepared = ensure_snapshot(source_sqlite, root, entry["sqlite_sha256"][:16])
        engine = prepared["engine"]
        source = prepared["source"]

        def call(workload: str) -> dict[str, Any]:
            return run_workload(engine, fixture, workload)

        serial = measure_serial(
            call,
            warmups=args.warmups,
            runs=args.runs,
            epochs=args.latency_epochs,
        )
        uncached_source = source_configuration(root, result_cache_bytes=0)
        uncached_opened = time.perf_counter_ns()
        uncached_engine = StandaloneEngine.from_duckdb_parquet(uncached_source)
        uncached_connection_open_ms = (
            time.perf_counter_ns() - uncached_opened
        ) / 1_000_000
        serial_without_result_cache = measure_serial(
            lambda workload: run_workload(uncached_engine, fixture, workload),
            warmups=args.warmups,
            runs=args.runs,
            epochs=args.latency_epochs,
        )
        del uncached_engine
        expected = canonical(serial["dfg_conformance_95pct"]["answer"])
        concurrency = {}
        for workers in [int(value) for value in args.concurrency.split(",") if value]:
            epochs = [
                concurrency_epoch(
                    source,
                    fixture,
                    expected,
                    workers,
                    args.concurrency_min_seconds,
                    args.concurrency_requests,
                )
                for _ in range(args.concurrency_epochs)
            ]
            concurrency[str(workers)] = aggregate_epochs(workers, epochs)
        memory = {}
        for workload in WORKLOADS:
            value, stats = sample_peak_rss(lambda workload=workload: call(workload))
            stats["answer_sha256"] = serial[workload]["answer_sha256"]
            stats["input"] = value["input"]
            memory[workload] = stats
        datasets.append(
            {
                "dataset": entry["name"],
                "fixture": fixture.to_dict(),
                "serial": serial,
                "serial_without_result_cache": serial_without_result_cache,
                "concurrency": concurrency,
                "memory": memory,
                "source_sqlite_bytes": source_sqlite.stat().st_size,
                "snapshot_bytes": directory_bytes(root),
                "snapshot_conversion_ms": prepared["snapshot_conversion_ms"],
                "connection_open_ms": prepared["connection_open_ms"],
                "cache_disabled_connection_open_ms": uncached_connection_open_ms,
                "snapshot_rebuilt": args.rebuild_snapshot,
            }
        )
    result = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "arm": ARM,
        "implementation": {
            "ocpm_engine_version": metadata.version("ocpm-engine"),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "source_revision": os.environ.get("OCPM_ENGINE_SOURCE_REVISION"),
            "source_tree_clean": os.environ.get("OCPM_ENGINE_SOURCE_TREE_CLEAN") == "true",
            "image_id": os.environ.get("OCPM_ENGINE_IMAGE_ID"),
        },
        "method": {
            "read_path": (
                "one source-neutral DuckDB aggregate plan over immutable canonical "
                "Parquet with bounded aggregate transfer"
            ),
            "process_model": (
                "one deployment-linked DuckDB connection pool per process worker"
            ),
            "data_import": "OCEL SQLite to canonical Parquet conversion outside request timing",
            "latency_states": {
                "serial": "warm bounded exact-result cache",
                "serial_without_result_cache": (
                    "warm DuckDB and filesystem state with exact-result cache disabled"
                ),
            },
        },
        "client_memory_baseline_bytes": baseline_rss,
        "client_memory_after_connection_bytes": int(process.memory_info().rss),
        "datasets": datasets,
    }
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2, default=str) + "\n")
    print(f"wrote {target}", flush=True)


if __name__ == "__main__":
    run(parse_args())
