#!/usr/bin/env python3
"""Execute a same-host baseline-versus-candidate worker regression gate.

Workers are one-shot executables. They receive one JSON object on stdin and must
return one JSON object on stdout with `input`, `answer`, and `storage_bytes`.
The controller, not the worker, measures wall time and process RSS.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from benchmarks.check_candidate_regression import (
        CONCURRENCY_CEILING,
        LATENCY_ABSOLUTE_SLACK_NS,
        LATENCY_CEILING,
        MEMORY_ABSOLUTE_SLACK_BYTES,
        MEMORY_CEILING,
        STORAGE_CEILING,
        payload_sha256,
        validate,
    )
except ModuleNotFoundError:
    from check_candidate_regression import (
        CONCURRENCY_CEILING,
        LATENCY_ABSOLUTE_SLACK_NS,
        LATENCY_CEILING,
        MEMORY_ABSOLUTE_SLACK_BYTES,
        MEMORY_CEILING,
        STORAGE_CEILING,
        payload_sha256,
        validate,
    )

SCHEMA_VERSION = 1
ARTIFACT_TYPE = "ocpm_engine_candidate_regression"
RANDOM_SEED = 1729


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-source", required=True, type=Path)
    parser.add_argument("--candidate-source", required=True, type=Path)
    parser.add_argument("--baseline-worker", required=True, type=Path)
    parser.add_argument("--candidate-worker", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--warmups", type=int, default=4)
    parser.add_argument("--latency-epochs", type=int, default=3)
    parser.add_argument("--samples-per-epoch", type=int, default=20)
    parser.add_argument("--memory-samples", type=int, default=4)
    parser.add_argument("--concurrency-levels", default="1,2,4")
    parser.add_argument("--concurrency-epochs", type=int, default=5)
    parser.add_argument("--concurrency-requests-per-worker", type=int, default=8)
    parser.add_argument("--concurrency-min-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--allow-dirty-controller", action="store_true")
    parser.add_argument("--allow-dirty-candidate", action="store_true")
    parser.add_argument("--allow-unverified-workers", action="store_true")
    args = parser.parse_args()
    positive = (
        "warmups",
        "latency_epochs",
        "samples_per_epoch",
        "memory_samples",
        "concurrency_epochs",
        "concurrency_requests_per_worker",
    )
    if any(getattr(args, field) < 1 for field in positive):
        parser.error("all sample and epoch counts must be positive")
    try:
        args.concurrency_levels = tuple(
            int(value) for value in args.concurrency_levels.split(",")
        )
    except ValueError:
        parser.error("--concurrency-levels must be comma-separated integers")
    if not args.concurrency_levels or any(
        value < 1 for value in args.concurrency_levels
    ):
        parser.error("concurrency levels must be positive")
    if args.timeout_seconds <= 0 or args.concurrency_min_seconds <= 0:
        parser.error("timeout and concurrency duration must be positive")
    return args


def canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def git(repository: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repository), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def source_provenance(repository: Path) -> dict[str, Any]:
    root = Path(git(repository, "rev-parse", "--show-toplevel"))
    status = git(root, "status", "--porcelain", "--untracked-files=all")
    tracked = subprocess.run(
        ["git", "-C", str(root), "diff", "--binary", "HEAD"],
        check=True,
        capture_output=True,
    ).stdout
    untracked = []
    for relative in git(
        root, "ls-files", "--others", "--exclude-standard"
    ).splitlines():
        path = root / relative
        untracked.append((relative, file_sha256(path) if path.is_file() else None))
    tree_material = tracked + canonical(untracked)
    return {
        "revision": git(root, "rev-parse", "HEAD"),
        "tree_clean": not bool(status),
        "tree_sha256": sha256_bytes(tree_material),
    }


def worker_provenance(
    worker: Path, source: dict[str, Any], *, allow_unverified: bool
) -> dict[str, Any] | None:
    path = worker.with_name(worker.name + ".provenance.json")
    if not path.is_file():
        if allow_unverified:
            return None
        raise SystemExit(f"worker provenance is missing: {path}")
    value = json.loads(path.read_text())
    if value.get("payload_sha256") != payload_sha256(value):
        raise SystemExit(f"worker provenance digest mismatch: {path}")
    if value.get("source") != source:
        raise SystemExit(f"worker provenance source mismatch: {path}")
    if value.get("worker_sha256") != file_sha256(worker):
        raise SystemExit(f"worker provenance executable mismatch: {path}")
    return value


def _rss_bytes(pid: int) -> int:
    status = Path(f"/proc/{pid}/status")
    if status.exists():
        try:
            lines = status.read_text().splitlines()
        except (FileNotFoundError, ProcessLookupError):
            # Short-lived workers can exit between the existence check and the
            # read. Once gone, they no longer contribute to the sampled RSS.
            return 0
        for line in lines:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
        return 0
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        capture_output=True,
        text=True,
    )
    value = result.stdout.strip()
    return int(value) * 1024 if value.isdigit() else 0


def invoke(worker: Path, request: dict[str, Any], timeout: float) -> dict[str, Any]:
    started = time.perf_counter_ns()
    process = subprocess.Popen(
        [str(worker)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    peak = {"bytes": 0}
    done = threading.Event()

    def sample() -> None:
        while not done.is_set():
            peak["bytes"] = max(peak["bytes"], _rss_bytes(process.pid))
            done.wait(0.002)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    try:
        stdout, stderr = process.communicate(
            json.dumps(request, sort_keys=True) + "\n", timeout=timeout
        )
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise RuntimeError(f"worker timed out after {timeout} seconds") from None
    finally:
        done.set()
        sampler.join(timeout=1)
    elapsed_ns = time.perf_counter_ns() - started
    if process.returncode != 0:
        raise RuntimeError(
            f"worker exited {process.returncode}; stderr_bytes={len(stderr.encode())}"
        )
    try:
        result = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"worker returned invalid JSON: {error}") from None
    if not isinstance(result, dict) or set(result) != {
        "input",
        "answer",
        "storage_bytes",
    }:
        raise RuntimeError("worker response fields changed")
    if type(result["storage_bytes"]) is not int or result["storage_bytes"] < 0:
        raise RuntimeError("worker returned invalid storage_bytes")
    return {
        **result,
        "elapsed_ns": elapsed_ns,
        "peak_rss_bytes": peak["bytes"],
        "worker_pid": process.pid,
    }


def serial_metrics(samples: list[int]) -> dict[str, Any]:
    ordered = sorted(samples)
    rank = min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)
    return {
        "samples_ns": samples,
        "p50_ns": int(statistics.median(ordered)),
        "p95_ns": ordered[rank],
    }


def balanced_order_codes(rounds: int, rng: random.Random) -> list[int]:
    values = [index % 2 for index in range(rounds)]
    rng.shuffle(values)
    return values


def require_same_answer(
    name: str, baseline: dict[str, Any], candidate: dict[str, Any]
) -> tuple[str, str]:
    baseline_input = sha256_bytes(canonical(baseline["input"]))
    candidate_input = sha256_bytes(canonical(candidate["input"]))
    if baseline_input != candidate_input:
        raise RuntimeError(f"{name}: worker input evidence differs between arms")
    baseline_answer = sha256_bytes(canonical(baseline["answer"]))
    candidate_answer = sha256_bytes(canonical(candidate["answer"]))
    if baseline_answer != candidate_answer:
        raise RuntimeError(f"{name}: exact answer differs between arms")
    return baseline_input, baseline_answer


def run_concurrency(
    worker: Path,
    request: dict[str, Any],
    timeout: float,
    workers: int,
    requests_per_worker: int,
    minimum_seconds: float,
) -> dict[str, Any]:
    minimum_requests = workers * requests_per_worker
    started = time.perf_counter_ns()
    rows = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        while (
            len(rows) < minimum_requests
            or time.perf_counter_ns() - started < minimum_seconds * 1_000_000_000
        ):
            rows.extend(
                pool.map(lambda _: invoke(worker, request, timeout), range(workers))
            )
    wall_ns = time.perf_counter_ns() - started
    total = len(rows)
    samples = [row["elapsed_ns"] for row in rows]
    return {
        "requests": total,
        "wall_ns": wall_ns,
        "throughput_qps": round(total / (wall_ns / 1_000_000_000), 6),
        **serial_metrics(samples),
        "answer_sha256s": [sha256_bytes(canonical(row["answer"])) for row in rows],
    }


def benchmark_workload(
    workload: dict[str, Any],
    workers: dict[str, Path],
    args: argparse.Namespace,
    rng: random.Random,
) -> dict[str, Any]:
    name = workload["name"]
    request = {"workload": name, "payload": workload["payload"]}
    preflight = {
        arm: invoke(worker, request, args.timeout_seconds)
        for arm, worker in workers.items()
    }
    input_sha256, answer_sha256 = require_same_answer(
        name, preflight["baseline"], preflight["candidate"]
    )
    for _ in range(args.warmups):
        for arm in ("baseline", "candidate"):
            result = invoke(workers[arm], request, args.timeout_seconds)
            if sha256_bytes(canonical(result["answer"])) != answer_sha256:
                raise RuntimeError(f"{name}: warmup answer changed")

    epochs = []
    for epoch in range(1, args.latency_epochs + 1):
        samples = {"baseline": [], "candidate": []}
        order_codes = balanced_order_codes(args.samples_per_epoch, rng)
        for code in order_codes:
            order = (
                ("baseline", "candidate") if code == 0 else ("candidate", "baseline")
            )
            for arm in order:
                result = invoke(workers[arm], request, args.timeout_seconds)
                if sha256_bytes(canonical(result["answer"])) != answer_sha256:
                    raise RuntimeError(f"{name}: measured answer changed")
                samples[arm].append(result["elapsed_ns"])
        epochs.append(
            {
                "epoch": epoch,
                "order_codes": order_codes,
                "arms": {
                    arm: serial_metrics(values) for arm, values in samples.items()
                },
            }
        )

    memory = {"order_codes": [], "arms": {"baseline": [], "candidate": []}}
    for sample in range(args.memory_samples):
        code = sample % 2
        memory["order_codes"].append(code)
        order = ("baseline", "candidate") if code == 0 else ("candidate", "baseline")
        for arm in order:
            result = invoke(workers[arm], request, args.timeout_seconds)
            if sha256_bytes(canonical(result["answer"])) != answer_sha256:
                raise RuntimeError(f"{name}: memory answer changed")
            memory["arms"][arm].append(
                {
                    "peak_rss_bytes": result["peak_rss_bytes"],
                    "worker_pid": result["worker_pid"],
                }
            )

    concurrency = []
    for level in args.concurrency_levels:
        arm_epochs = {"baseline": [], "candidate": []}
        for epoch in range(args.concurrency_epochs):
            order = (
                ("baseline", "candidate")
                if epoch % 2 == 0
                else ("candidate", "baseline")
            )
            for arm in order:
                result = run_concurrency(
                    workers[arm],
                    request,
                    args.timeout_seconds,
                    level,
                    args.concurrency_requests_per_worker,
                    args.concurrency_min_seconds,
                )
                if any(value != answer_sha256 for value in result["answer_sha256s"]):
                    raise RuntimeError(f"{name}: concurrency answer changed")
                arm_epochs[arm].append(result)
        concurrency.append({"workers": level, "arms": arm_epochs})

    return {
        "name": name,
        "input_sha256": input_sha256,
        "answer_sha256": answer_sha256,
        "storage_bytes": {
            arm: preflight[arm]["storage_bytes"] for arm in ("baseline", "candidate")
        },
        "latency_epochs": epochs,
        "memory": memory,
        "concurrency": concurrency,
    }


def load_manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or set(value) != {"schema_version", "workloads"}:
        raise RuntimeError("candidate manifest fields changed")
    if value["schema_version"] != 1 or not isinstance(value["workloads"], list):
        raise RuntimeError("invalid candidate manifest")
    for workload in value["workloads"]:
        if not isinstance(workload, dict) or set(workload) != {"name", "payload"}:
            raise RuntimeError("candidate workload fields changed")
        if not isinstance(workload["name"], str) or not workload["name"]:
            raise RuntimeError("candidate workload name is invalid")
    return value


def main() -> None:
    args = parse_args()
    manifest = load_manifest(args.manifest)
    controller = source_provenance(Path(__file__).resolve().parents[1])
    baseline = source_provenance(args.baseline_source)
    candidate = source_provenance(args.candidate_source)
    if not baseline["tree_clean"]:
        raise SystemExit("baseline source must be clean")
    if not controller["tree_clean"] and not args.allow_dirty_controller:
        raise SystemExit("controller source must be clean")
    if not candidate["tree_clean"] and not args.allow_dirty_candidate:
        raise SystemExit("candidate source must be clean")
    workers = {
        "baseline": args.baseline_worker.resolve(),
        "candidate": args.candidate_worker.resolve(),
    }
    if any(
        not path.is_file() or not os.access(path, os.X_OK) for path in workers.values()
    ):
        raise SystemExit("worker paths must be executable files")
    sources = {"baseline": baseline, "candidate": candidate}
    worker_provenances = {
        arm: worker_provenance(
            worker,
            sources[arm],
            allow_unverified=args.allow_unverified_workers,
        )
        for arm, worker in workers.items()
    }
    rng = random.Random(RANDOM_SEED)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "controller": controller,
        "fixture": {
            "manifest_sha256": file_sha256(args.manifest),
            "workload_count": len(manifest["workloads"]),
        },
        "settings": {
            "random_seed": RANDOM_SEED,
            "warmups": args.warmups,
            "latency_epochs": args.latency_epochs,
            "samples_per_epoch": args.samples_per_epoch,
            "memory_samples": args.memory_samples,
            "concurrency_levels": list(args.concurrency_levels),
            "concurrency_epochs": args.concurrency_epochs,
            "concurrency_requests_per_worker": args.concurrency_requests_per_worker,
            "concurrency_min_seconds": args.concurrency_min_seconds,
            "latency_ceiling": LATENCY_CEILING,
            "latency_absolute_slack_ns": LATENCY_ABSOLUTE_SLACK_NS,
            "memory_ceiling": MEMORY_CEILING,
            "memory_absolute_slack_bytes": MEMORY_ABSOLUTE_SLACK_BYTES,
            "concurrency_ceiling": CONCURRENCY_CEILING,
            "storage_ceiling": STORAGE_CEILING,
        },
        "arms": {
            "baseline": {
                **baseline,
                "worker_sha256": file_sha256(workers["baseline"]),
                "worker_provenance": worker_provenances["baseline"],
            },
            "candidate": {
                **candidate,
                "worker_sha256": file_sha256(workers["candidate"]),
                "worker_provenance": worker_provenances["candidate"],
            },
        },
        "workloads": [
            benchmark_workload(workload, workers, args, rng)
            for workload in manifest["workloads"]
        ],
    }
    artifact["payload_sha256"] = payload_sha256(artifact)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    validate(
        artifact,
        allow_dirty_controller=args.allow_dirty_controller,
        allow_dirty_candidate=args.allow_dirty_candidate,
        allow_unverified_workers=args.allow_unverified_workers,
    )
    print(f"candidate regression gate passed: {args.output}")


if __name__ == "__main__":
    main()
