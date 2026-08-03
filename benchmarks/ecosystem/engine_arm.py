"""Benchmark the pg_ocpm + ocpm-engine arm of the ecosystem suite."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import threading
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psutil

from benchmarks import sap_pm4py_three_way as sap
from benchmarks.ecosystem.common import WORKLOADS, Fixture, canonical, measure_serial

ARM = "pg_ocpm_ocpm_engine"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-host", default="postgres_vanilla")
    parser.add_argument("--extension-host", default="postgres_ocpm")
    parser.add_argument("--database", default="ocel_benchmark")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--manifest", default="/results/ecosystem-manifest.json")
    parser.add_argument("--prepare-result", default="/results/public-prepare.json")
    parser.add_argument("--output", default="/results/ecosystem-engine-arm.json")
    parser.add_argument(
        "--datasets",
        default="",
        help="comma-separated manifest dataset names; default all",
    )
    parser.add_argument("--make-manifest", action="store_true")
    parser.add_argument("--warmups", type=int, default=10)
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--latency-epochs", type=int, default=3)
    parser.add_argument("--concurrency", default="1,2,4,8")
    parser.add_argument("--concurrency-epochs", type=int, default=3)
    parser.add_argument("--concurrency-min-seconds", type=float, default=5.0)
    parser.add_argument("--concurrency-requests", type=int, default=32)
    return parser.parse_args()


def sap_args(args: argparse.Namespace) -> SimpleNamespace:
    return SimpleNamespace(
        baseline_host=args.baseline_host,
        extension_host=args.extension_host,
        database=args.database,
        timeout_seconds=args.timeout_seconds,
        train_fraction=0.8,
        concurrency=args.concurrency,
        concurrency_epochs=args.concurrency_epochs,
        concurrency_min_seconds=args.concurrency_min_seconds,
        concurrency_requests=args.concurrency_requests,
    )


def discover_fixture(
    extension, baseline, settings: SimpleNamespace, dataset_row: dict[str, Any]
) -> Fixture:
    dataset_name = dataset_row["name"]
    object_type = dataset_row["object_type_selection"]["selected"]
    row = sap.query_rows(
        extension,
        sap.FIXTURE_SQL,
        {
            "dataset_name": dataset_name,
            "object_type": object_type,
            "train_fraction": settings.train_fraction,
        },
    )[0]
    baseline_row = sap.query_rows(
        baseline,
        "SELECT dataset_id FROM ocel.dataset WHERE dataset_name=%s",
        (dataset_name,),
    )
    if not baseline_row:
        raise SystemExit(f"missing baseline dataset {dataset_name}")
    if int(row[7]) == 0 or int(row[8]) == 0:
        raise SystemExit(f"{dataset_name}: empty train or test partition")
    return Fixture(
        dataset_name=dataset_name,
        baseline_dataset_id=int(baseline_row[0][0]),
        ocpm_dataset_id=int(row[0]),
        tenant_id=int(row[1]),
        object_type=object_type,
        from_time=row[2],
        train_to=row[3],
        test_from=row[4],
        to_time=row[5],
        cases=int(row[6]),
        train_cases=int(row[7]),
        test_cases=int(row[8]),
    )


def make_manifest(args: argparse.Namespace) -> None:
    prepare = json.loads(Path(args.prepare_result).read_text())
    if len(prepare["datasets"]) != 1:
        raise SystemExit("ecosystem fixture must contain exactly one dataset")
    prepare_row = prepare["datasets"][0]
    baseline = sap.connect(args.baseline_host, args.database, args.timeout_seconds)
    extension = sap.connect(args.extension_host, args.database, args.timeout_seconds)
    settings = sap_args(args)
    try:
        fixture = discover_fixture(extension, baseline, settings, prepare_row)
    finally:
        baseline.close()
        extension.close()

    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contract": {
            "suite": "ecosystem-common-pm",
            "train_fraction": 0.8,
            "workloads": list(WORKLOADS),
            "timing_boundary": (
                "steady-state request only; OCEL import, PostgreSQL fixture load, "
                "process startup, connection startup, and worker startup excluded"
            ),
            "correctness": "exact canonical answer equality on every measured request",
            "object_type_selection": prepare_row["object_type_selection"],
            "event_order": prepare_row["event_order"],
            "latency": "10 warmups; 3 epochs x 30 samples; monotonic nanosecond clock",
            "concurrency": (
                "DFG conformance; 3 epochs; at least 5 seconds and 32 requests "
                "per worker at 1/2/4/8 workers"
            ),
        },
        "source": prepare["source"],
        "datasets": [
            {
                "name": fixture.dataset_name,
                "filename": prepare_row["filename"],
                "sqlite_sha256": prepare_row["sqlite_sha256"],
                "source_url": prepare_row["source_url"],
                "source_sqlite_bytes": prepare_row["source_sqlite_bytes"],
                "source_counts": prepare_row["counts"],
                "object_type_selection": prepare_row["object_type_selection"],
                "fixture": fixture.to_dict(),
            }
        ],
        "fixture_prepare": prepare,
    }
    target = Path(args.manifest)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"wrote {target}", flush=True)


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
    full = process.memory_full_info()
    return value, {
        "baseline_rss_bytes": baseline,
        "peak_rss_bytes": peak,
        "incremental_peak_bytes": max(0, peak - baseline),
        "resident_pss_bytes": int(getattr(full, "pss", 0)),
    }


def memory_measurements(call, serial: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {}
    for workload in WORKLOADS:
        value, stats = sample_peak_rss(lambda workload=workload: call(workload))
        stats["answer_sha256"] = serial[workload]["answer_sha256"]
        stats["input"] = value["input"]
        result[workload] = stats
    return result


def concurrency_measurements(
    args: argparse.Namespace, fixture: Fixture, expected: str
) -> dict[str, Any]:
    settings = sap_args(args)
    levels = [int(value) for value in args.concurrency.split(",") if value]
    result = {}
    for workers in levels:
        epochs = []
        for epoch_index in range(args.concurrency_epochs):
            print(
                f"  engine concurrency {fixture.dataset_name} x{workers} "
                f"epoch {epoch_index + 1}/{args.concurrency_epochs}",
                flush=True,
            )
            value = sap.run_concurrency_epoch(settings, fixture, ARM, expected, workers)
            value["epoch"] = epoch_index + 1
            epochs.append(value)
        result[str(workers)] = sap.aggregate_concurrency_epochs(workers, epochs)
    return result


def command_version(command: list[str]) -> str | None:
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def run(args: argparse.Namespace) -> None:
    manifest = json.loads(Path(args.manifest).read_text())
    requested_datasets = {value for value in args.datasets.split(",") if value}
    manifest_datasets = {entry["name"] for entry in manifest["datasets"]}
    if not requested_datasets:
        requested_datasets = manifest_datasets
    unknown = requested_datasets - manifest_datasets
    if unknown:
        raise SystemExit(f"unknown datasets: {', '.join(sorted(unknown))}")
    baseline = sap.connect(args.baseline_host, args.database, args.timeout_seconds)
    extension = sap.connect(args.extension_host, args.database, args.timeout_seconds)
    process = psutil.Process()
    baseline_rss = int(process.memory_info().rss)
    datasets = []
    try:
        for entry in manifest["datasets"]:
            if entry["name"] not in requested_datasets:
                continue
            fixture = Fixture.from_dict(entry["fixture"])
            sap_fixture = sap.Fixture(**fixture.__dict__)
            prepared_engine = sap.prepare_ocpm_engine(extension, sap_fixture)
            print(f"engine serial {fixture.dataset_name}", flush=True)

            def call(workload: str) -> dict[str, Any]:
                return sap.run_ocpm_engine(
                    extension,
                    sap_fixture,
                    workload,
                    prepared=prepared_engine,
                )

            serial = measure_serial(
                call,
                warmups=args.warmups,
                runs=args.runs,
                epochs=args.latency_epochs,
            )
            expected = canonical(serial["dfg_conformance_95pct"]["answer"])
            concurrency = concurrency_measurements(args, fixture, expected)
            print(f"engine memory {fixture.dataset_name}", flush=True)
            memory = memory_measurements(call, serial)
            datasets.append(
                {
                    "dataset": fixture.dataset_name,
                    "fixture": fixture.to_dict(),
                    "serial": serial,
                    "concurrency": concurrency,
                    "memory": memory,
                }
            )
        database_storage = sap.schema_storage(extension, "ocpm")
        database_environment = sap.database_environment(extension, True)
    finally:
        baseline.close()
        extension.close()

    output = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "arm": ARM,
        "implementation": {
            "pg_ocpm_version": database_environment["pg_ocpm_version"],
            "ocpm_engine_version": metadata.version("ocpm-engine"),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "source_revision": os.environ.get("OCPM_ENGINE_SOURCE_REVISION"),
            "source_tree_clean": os.environ.get("OCPM_ENGINE_SOURCE_TREE_CLEAN")
            == "true",
            "pg_ocpm_source_revision": os.environ.get("OCPM_PG_OCPM_SOURCE_REVISION"),
            "pg_ocpm_source_tree_clean": os.environ.get(
                "OCPM_PG_OCPM_SOURCE_TREE_CLEAN"
            )
            == "true",
            "image_id": os.environ.get("OCPM_ENGINE_IMAGE_ID"),
        },
        "method": {
            "read_path": (
                "capability-aware sufficient-statistic pushdown for DFG workloads; "
                "factorized event batches for event-level workloads"
            ),
            "process_model": "one persistent PostgreSQL connection per process worker",
            "data_import": "PostgreSQL fixture preparation reported separately",
        },
        "client_memory_after_connection_bytes": int(process.memory_info().rss),
        "client_memory_baseline_bytes": baseline_rss,
        "storage": {
            "pg_ocpm_schema": database_storage,
            "packages": sap.package_storage()["ocpm_engine"],
        },
        "database_environment": database_environment,
        "datasets": datasets,
    }
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(output, indent=2, default=str) + "\n")
    print(f"wrote {target}", flush=True)


def main() -> None:
    args = parse_args()
    os.environ["OCPM_ENGINE_READ_PATH"] = "auto"
    if args.make_manifest:
        make_manifest(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
