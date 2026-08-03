"""Benchmark OCPA against the fixed ecosystem-common-PM contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
import platform
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

import psutil

from benchmarks.ecosystem.common import (
    WORKLOADS,
    Fixture,
    canonical,
    latency_metrics,
    measure_serial,
    score_lifecycles,
)

ARM = "ocpa"
_MODEL = None
_CONCURRENCY_CONTROL: dict[str, Any] = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default="/results/ecosystem-manifest.json")
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--output", default="/results/ecosystem-ocpa-arm.json")
    parser.add_argument(
        "--datasets",
        default="",
        help="comma-separated manifest dataset names; default all",
    )
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--latency-epochs", type=int, default=3)
    parser.add_argument("--concurrency", default="1,2,4,8")
    parser.add_argument("--concurrency-epochs", type=int, default=3)
    parser.add_argument("--concurrency-min-seconds", type=float, default=5.0)
    parser.add_argument("--concurrency-requests", type=int, default=32)
    parser.add_argument("--native-probe", action="store_true")
    parser.add_argument("--native-probe-file")
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def native_probe(path: str) -> None:
    """Exercise OCPA's documented OCEL 2.0 SQLite importer without adapters."""

    from ocpa.objects.log.importer.ocel2.sqlite import factory

    started = time.perf_counter()
    value = factory.apply(path)
    print(
        json.dumps(
            {
                "success": True,
                "elapsed_s": round(time.perf_counter() - started, 6),
                "events": len(value.obj.raw.events),
                "objects": len(value.obj.raw.objects),
            }
        )
    )


def run_native_probe(path: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "benchmarks.ecosystem.ocpa_arm",
        "--native-probe",
        "--native-probe-file",
        str(path),
    ]
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=180,
        )
        payload = json.loads(completed.stdout.splitlines()[-1])
        return {"documented_importer": True, **payload}
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip().splitlines()
        return {
            "documented_importer": True,
            "success": False,
            "elapsed_s": round(time.perf_counter() - started, 6),
            "error_type": stderr[-1].split(":", 1)[0] if stderr else "unknown",
            "error": stderr[-1] if stderr else "importer exited nonzero",
        }
    except subprocess.TimeoutExpired:
        return {
            "documented_importer": True,
            "success": False,
            "elapsed_s": round(time.perf_counter() - started, 6),
            "error_type": "TimeoutExpired",
            "error": "documented importer exceeded 180 seconds",
        }


def load_ocpa_native(path: Path):
    """Load through OCPA's documented OCEL 2.0 SQLite importer."""

    from ocpa.objects.log.importer.ocel2.sqlite import factory

    imported = factory.apply(str(path))
    model = imported.obj
    telemetry = {
        "adapter": "ocpa_documented_ocel2_sqlite_importer",
        "events": len(model.raw.events),
        "objects": len(model.raw.objects),
        "object_types": len(model.meta.obj_types),
        "activities": len(model.meta.act_attr),
    }
    return model, telemetry


def load_ocpa_model_adapter(path: Path):
    """Build OCPA's public native model when its OCEL2 importer is broken.

    OCPA 1.3.4's documented SQLite importer misaligns event-id-indexed relation
    rows with a RangeIndex and then eagerly samples three objects for a debug
    message. On the project's own running example that leaves the object map
    empty and raises ``ValueError``. This benchmark-owned setup adapter repairs
    only that untimed ingestion step; all measured requests execute against
    OCPA's public ``ObjectCentricEventLog`` implementation.
    """

    import pandas as pd
    from ocpa.objects.log.converter.versions import df_to_ocel

    with sqlite3.connect(path) as connection:
        event_df = pd.read_sql_query(
            "SELECT ocel_id AS event_id, ocel_type AS event_activity FROM event",
            connection,
        )
        event_maps = connection.execute(
            "SELECT ocel_type_map FROM event_map_type ORDER BY ocel_type_map"
        ).fetchall()
        timestamps: dict[str, str] = {}
        for (mapped_type,) in event_maps:
            table = str(mapped_type).replace('"', '""')
            for event_id, event_time in connection.execute(
                f'SELECT ocel_id, ocel_time FROM "event_{table}"'
            ):
                timestamps[str(event_id)] = str(event_time)
        event_df["event_timestamp"] = pd.to_datetime(
            event_df["event_id"].map(timestamps), utc=True
        )
        if event_df["event_timestamp"].isna().any():
            raise ValueError("OCPA adapter found events without timestamps")

        object_types = [
            str(row[0])
            for row in connection.execute(
                "SELECT DISTINCT ocel_type FROM object ORDER BY ocel_type"
            )
        ]
        unsupported = [name for name in object_types if not name.isidentifier()]
        if unsupported:
            raise ValueError(
                "OCPA dataframe converter cannot preserve non-identifier object "
                f"types: {unsupported}"
            )
        for object_type in object_types:
            event_df[object_type] = [set() for _ in range(len(event_df))]

        event_positions = {
            str(event_id): index for index, event_id in enumerate(event_df["event_id"])
        }
        links = connection.execute(
            """
            SELECT eo.ocel_event_id, eo.ocel_object_id, o.ocel_type
            FROM event_object AS eo
            JOIN object AS o ON o.ocel_id = eo.ocel_object_id
            ORDER BY eo.ocel_event_id, o.ocel_type, eo.ocel_object_id
            """
        )
        link_count = 0
        for event_id, object_id, object_type in links:
            event_df.at[event_positions[str(event_id)], str(object_type)].add(
                str(object_id)
            )
            link_count += 1

    model = df_to_ocel.apply(event_df)
    telemetry = {
        "adapter": "benchmark_ocel2_sqlite_to_ocpa_public_model",
        "reason": (
            "OCPA 1.3.4 documented importer fails on its upstream example before "
            "constructing a model"
        ),
        "events": len(model.raw.events),
        "objects": len(model.raw.objects),
        "object_types": len(model.meta.obj_types),
        "event_object_links": link_count,
    }
    return model, telemetry


def lifecycle_rows(model, fixture: Fixture) -> list[list[tuple[str, datetime]]]:
    return [
        [
            (event.act, event.time)
            for event in sorted(
                model.sequence[object_id], key=lambda event: (event.time, event.id)
            )
        ]
        for object_id in model.ot_objects[fixture.object_type]
    ]


def run_ocpa(model, fixture: Fixture, workload: str) -> dict[str, Any]:
    lifecycles = lifecycle_rows(model, fixture)
    result = score_lifecycles(lifecycles, fixture, workload)
    result["input"].update(
        {
            "source": "ocpa.ObjectCentricEventLog",
            "native_object_type_cases": len(model.ot_objects[fixture.object_type]),
        }
    )
    return result


def sample_peak_rss(call) -> tuple[Any, dict[str, int]]:
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
    full = process.memory_full_info()
    return value, {
        "baseline_rss_bytes": baseline,
        "peak_rss_bytes": peak,
        "incremental_peak_bytes": max(0, peak - baseline),
        "resident_pss_bytes": int(getattr(full, "pss", 0)),
    }


def _concurrency_worker(_: int) -> dict[str, Any]:
    control = _CONCURRENCY_CONTROL
    expected = control["expected"]
    error = None
    try:
        warm = run_ocpa(_MODEL, control["fixture"], "dfg_conformance_95pct")
        if canonical(warm["answer"]) != expected:
            error = "warmup correctness mismatch"
    except Exception as exc:  # pragma: no cover - live Docker path
        error = f"warmup failed: {type(exc).__name__}: {exc}"
    try:
        control["barrier"].wait(timeout=30)
    except threading.BrokenBarrierError:
        return {"error": error or "startup barrier failed"}
    if not control["start"].wait(timeout=30):
        return {"error": error or "start signal timed out"}
    if error:
        return {"error": error}

    samples_ns = []
    while (
        len(samples_ns) < control["minimum_requests"]
        or time.perf_counter() < control["deadline"].value
    ):
        started_ns = time.perf_counter_ns()
        value = run_ocpa(_MODEL, control["fixture"], "dfg_conformance_95pct")
        samples_ns.append(time.perf_counter_ns() - started_ns)
        if canonical(value["answer"]) != expected:
            return {"error": "measured correctness mismatch"}
    process = psutil.Process()
    full = process.memory_full_info()
    return {
        "pid": os.getpid(),
        "samples_ns": samples_ns,
        "rss_bytes": int(process.memory_info().rss),
        "pss_bytes": int(getattr(full, "pss", 0)),
    }


def concurrency_epoch(
    fixture: Fixture,
    expected: str,
    workers: int,
    minimum_seconds: float,
    minimum_requests: int,
) -> dict[str, Any]:
    global _CONCURRENCY_CONTROL
    context = multiprocessing.get_context("fork")
    _CONCURRENCY_CONTROL = {
        "fixture": fixture,
        "expected": expected,
        "minimum_requests": minimum_requests,
        "barrier": context.Barrier(workers + 1),
        "start": context.Event(),
        "deadline": context.Value("d", 0.0),
    }
    with ProcessPoolExecutor(max_workers=workers, mp_context=context) as pool:
        futures = [pool.submit(_concurrency_worker, slot) for slot in range(workers)]
        _CONCURRENCY_CONTROL["barrier"].wait(timeout=30)
        started = time.perf_counter()
        with _CONCURRENCY_CONTROL["deadline"].get_lock():
            _CONCURRENCY_CONTROL["deadline"].value = started + minimum_seconds
        _CONCURRENCY_CONTROL["start"].set()
        worker_results = [future.result() for future in futures]
        wall = time.perf_counter() - started
    errors = [row["error"] for row in worker_results if "error" in row]
    if errors:
        raise AssertionError("; ".join(errors))
    samples_ns = [sample for row in worker_results for sample in row["samples_ns"]]
    metrics = latency_metrics(samples_ns)
    return {
        **metrics,
        "requests": len(samples_ns),
        "wall_ms": round(wall * 1000, 3),
        "throughput_qps": round(len(samples_ns) / wall, 3),
        "worker_request_counts": sorted(
            len(row["samples_ns"]) for row in worker_results
        ),
        "worker_rss_bytes_sum": sum(row["rss_bytes"] for row in worker_results),
        "worker_pss_bytes_sum": sum(row["pss_bytes"] for row in worker_results),
        "worker_pids": sorted(row["pid"] for row in worker_results),
        "correct": True,
    }


def aggregate_epochs(workers: int, epochs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "workers": workers,
        "epoch_count": len(epochs),
        "requests": sum(epoch["requests"] for epoch in epochs),
        "throughput_qps": round(
            statistics.median(epoch["throughput_qps"] for epoch in epochs), 3
        ),
        "p50_ms": round(statistics.median(epoch["p50_ms"] for epoch in epochs), 3),
        "p95_ms": round(statistics.median(epoch["p95_ms"] for epoch in epochs), 3),
        "minimum_worker_pss_bytes_sum": min(
            epoch["worker_pss_bytes_sum"] for epoch in epochs
        ),
        "maximum_worker_rss_bytes_sum": max(
            epoch["worker_rss_bytes_sum"] for epoch in epochs
        ),
        "correct": all(epoch["correct"] for epoch in epochs),
        "epochs": epochs,
    }


def package_bytes(distribution_name: str) -> int:
    distribution = metadata.distribution(distribution_name)
    total = 0
    for file in distribution.files or ():
        path = distribution.locate_file(file)
        if path.is_file():
            total += path.stat().st_size
    return total


def run(args: argparse.Namespace) -> None:
    global _MODEL
    manifest = json.loads(Path(args.manifest).read_text())
    requested_datasets = {value for value in args.datasets.split(",") if value}
    manifest_datasets = {entry["name"] for entry in manifest["datasets"]}
    if not requested_datasets:
        requested_datasets = manifest_datasets
    unknown = requested_datasets - manifest_datasets
    if unknown:
        raise SystemExit(f"unknown datasets: {', '.join(sorted(unknown))}")
    datasets = []
    process = psutil.Process()
    process_baseline_rss = int(process.memory_info().rss)
    for entry in manifest["datasets"]:
        if entry["name"] not in requested_datasets:
            continue
        fixture = Fixture.from_dict(entry["fixture"])
        path = Path(args.data_dir) / entry["filename"]
        actual_hash = sha256_file(path)
        if actual_hash != entry["sqlite_sha256"]:
            raise SystemExit(
                f"{fixture.dataset_name}: source hash {actual_hash} != manifest hash"
            )
        native_import = run_native_probe(path)
        before_load = int(process.memory_info().rss)
        started = time.perf_counter()
        load_model = (
            load_ocpa_native if native_import["success"] else load_ocpa_model_adapter
        )
        (_MODEL, adapter), load_memory = sample_peak_rss(lambda: load_model(path))
        load_seconds = time.perf_counter() - started
        after_load = int(process.memory_info().rss)
        print(f"ocpa serial {fixture.dataset_name}", flush=True)

        def call(workload: str) -> dict[str, Any]:
            return run_ocpa(_MODEL, fixture, workload)

        serial = measure_serial(
            call,
            warmups=args.warmups,
            runs=args.runs,
            epochs=args.latency_epochs,
        )
        memory = {}
        for workload in WORKLOADS:
            value, stats = sample_peak_rss(lambda w=workload: call(w))
            stats["answer_sha256"] = serial[workload]["answer_sha256"]
            stats["input"] = value["input"]
            memory[workload] = stats
        expected = canonical(serial["dfg_conformance_95pct"]["answer"])
        concurrency = {}
        for workers in [int(v) for v in args.concurrency.split(",") if v]:
            epochs = []
            for epoch_index in range(args.concurrency_epochs):
                print(
                    f"  ocpa concurrency {fixture.dataset_name} x{workers} "
                    f"epoch {epoch_index + 1}/{args.concurrency_epochs}",
                    flush=True,
                )
                value = concurrency_epoch(
                    fixture,
                    expected,
                    workers,
                    args.concurrency_min_seconds,
                    args.concurrency_requests,
                )
                value["epoch"] = epoch_index + 1
                epochs.append(value)
            concurrency[str(workers)] = aggregate_epochs(workers, epochs)
        datasets.append(
            {
                "dataset": fixture.dataset_name,
                "fixture": fixture.to_dict(),
                "source_sqlite_bytes": path.stat().st_size,
                "source_sqlite_sha256": actual_hash,
                "native_import_probe": native_import,
                "adapter": adapter,
                "load": {
                    "seconds": round(load_seconds, 6),
                    "rss_before_bytes": before_load,
                    "rss_after_bytes": after_load,
                    **load_memory,
                },
                "serial": serial,
                "concurrency": concurrency,
                "memory": memory,
            }
        )
        _MODEL = None

    output = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "arm": ARM,
        "implementation": {
            "ocpa_version": metadata.version("ocpa"),
            "ocpa_wheel_sha256": (
                "52e8208d5ef8633060b905441498aa8b53e0322bf7a310c5a5deb49500da2934"
            ),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "runtime_versions": {
                package: metadata.version(package)
                for package in (
                    "pm4py",
                    "pandas",
                    "numpy",
                    "networkx",
                    "scipy",
                    "psutil",
                )
            },
            "image_id": os.environ.get("OCPM_OCPA_IMAGE_ID"),
            "controller_source_tree_clean": os.environ.get(
                "OCPM_ENGINE_SOURCE_TREE_CLEAN"
            )
            == "true",
        },
        "method": {
            "data_model": "ocpa.objects.log.variants.obj.ObjectCentricEventLog",
            "adapter": (
                "documented importer when functional; otherwise a disclosed "
                "benchmark-owned OCEL2-to-public-OCPA-model setup adapter"
            ),
            "process_model": (
                "preloaded read-only OCPA model with forked process workers"
            ),
            "native_importer": (
                "probed on the unmodified upstream source and reported separately"
            ),
        },
        "process_baseline_rss_bytes": process_baseline_rss,
        "storage": {
            "ocpa_package_bytes": package_bytes("ocpa"),
            "pandas_package_bytes": package_bytes("pandas"),
            "networkx_package_bytes": package_bytes("networkx"),
        },
        "datasets": datasets,
    }
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(output, indent=2, default=str) + "\n")
    print(f"wrote {target}", flush=True)


def main() -> None:
    args = parse_args()
    if args.native_probe:
        if not args.native_probe_file:
            raise SystemExit("--native-probe requires --native-probe-file")
        native_probe(args.native_probe_file)
    else:
        run(args)


if __name__ == "__main__":
    main()
