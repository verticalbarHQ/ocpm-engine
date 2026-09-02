#!/usr/bin/env python3
"""Independently validate an executed ocpm-engine candidate regression artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import subprocess
from pathlib import Path
from typing import Any

LATENCY_CEILING = 1.10
LATENCY_ABSOLUTE_SLACK_NS = 100_000
MEMORY_CEILING = 1.10
MEMORY_ABSOLUTE_SLACK_BYTES = 64 * 1024
CONCURRENCY_CEILING = 1.10
CONCURRENCY_ABSOLUTE_SLACK_NS = 100_000
STORAGE_CEILING = 1.01
MAX_CONCURRENCY_QPS_CV = 0.15


def canonical_payload(value: dict[str, Any]) -> bytes:
    unsigned = dict(value)
    unsigned.pop("payload_sha256", None)
    return json.dumps(unsigned, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def payload_sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_payload(value)).hexdigest()


def fail(message: str) -> None:
    raise ValueError(message)


def _positive(values: object, label: str) -> list[int]:
    if (
        not isinstance(values, list)
        or not values
        or any(type(value) is not int or value <= 0 for value in values)
    ):
        fail(f"{label}: samples must be positive integers")
    return values


def _p95(values: list[int]) -> int:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * 0.95) - 1)]


def _check_serial(value: object, label: str) -> list[int]:
    required = {"samples_ns", "p50_ns", "p95_ns"}
    if not isinstance(value, dict) or not required.issubset(value):
        fail(f"{label}: serial fields changed")
    samples = _positive(value["samples_ns"], label)
    if value["p50_ns"] != int(statistics.median(samples)) or value["p95_ns"] != _p95(
        samples
    ):
        fail(f"{label}: serial summary mismatch")
    return samples


def _allowed(prior: int, ceiling: float, slack: int) -> int:
    return max(math.ceil(prior * ceiling), prior + slack)


def _check_worker_provenance(
    arm: dict[str, Any], label: str, *, allow_unverified_workers: bool
) -> None:
    provenance = arm.get("worker_provenance")
    if provenance is None and allow_unverified_workers:
        return
    required = {
        "schema_version",
        "artifact_type",
        "source",
        "inputs",
        "worker_sha256",
        "payload_sha256",
    }
    if not isinstance(provenance, dict) or set(provenance) != required:
        fail(f"{label}: worker provenance fields changed")
    if (
        provenance["schema_version"] != 1
        or provenance["artifact_type"] != "ocpm_engine_candidate_worker"
        or provenance["payload_sha256"] != payload_sha256(provenance)
    ):
        fail(f"{label}: invalid worker provenance")
    source = {name: arm.get(name) for name in ("revision", "tree_clean", "tree_sha256")}
    if provenance["source"] != source:
        fail(f"{label}: worker source does not match declared source")
    if provenance["worker_sha256"] != arm.get("worker_sha256"):
        fail(f"{label}: worker digest does not match provenance")
    inputs = provenance["inputs"]
    if not isinstance(inputs, dict) or set(inputs) != {
        "builder_sha256",
        "worker_source_sha256",
    }:
        fail(f"{label}: worker build inputs changed")
    if any(
        not isinstance(digest, str) or len(digest) != 64 for digest in inputs.values()
    ):
        fail(f"{label}: invalid worker build input digest")


def validate(
    value: dict[str, Any],
    *,
    allow_dirty_controller: bool,
    allow_dirty_candidate: bool,
    allow_unverified_workers: bool = False,
    expected_controller_revision: str | None = None,
    expected_baseline_revision: str | None = None,
    expected_candidate_revision: str | None = None,
) -> None:
    required = {
        "schema_version",
        "artifact_type",
        "generated_at",
        "controller",
        "fixture",
        "settings",
        "arms",
        "workloads",
        "payload_sha256",
    }
    if set(value) != required:
        fail("top-level fields changed")
    if (
        value["schema_version"] != 1
        or value["artifact_type"] != "ocpm_engine_candidate_regression"
    ):
        fail("unsupported candidate artifact")
    if value["payload_sha256"] != payload_sha256(value):
        fail("payload digest mismatch")
    settings = value["settings"]
    expected_limits = {
        "latency_ceiling": LATENCY_CEILING,
        "latency_absolute_slack_ns": LATENCY_ABSOLUTE_SLACK_NS,
        "memory_ceiling": MEMORY_CEILING,
        "memory_absolute_slack_bytes": MEMORY_ABSOLUTE_SLACK_BYTES,
        "concurrency_ceiling": CONCURRENCY_CEILING,
        "storage_ceiling": STORAGE_CEILING,
    }
    for name, expected in expected_limits.items():
        if settings.get(name) != expected:
            fail(f"settings/{name}: threshold changed")
    controller = value["controller"]
    arms = value["arms"]
    if (
        expected_controller_revision is not None
        and controller.get("revision") != expected_controller_revision
    ):
        fail("controller revision does not match current checkout")
    if (
        expected_baseline_revision is not None
        and arms.get("baseline", {}).get("revision") != expected_baseline_revision
    ):
        fail("baseline revision does not match checked-out baseline")
    if (
        expected_candidate_revision is not None
        and arms.get("candidate", {}).get("revision") != expected_candidate_revision
    ):
        fail("candidate revision does not match current checkout")
    if not controller.get("tree_clean") and not allow_dirty_controller:
        fail("controller source is dirty")
    if not arms.get("baseline", {}).get("tree_clean"):
        fail("baseline source is dirty")
    if not arms.get("candidate", {}).get("tree_clean") and not allow_dirty_candidate:
        fail("candidate source is dirty")
    for arm in ("baseline", "candidate"):
        _check_worker_provenance(
            arms.get(arm, {}), arm, allow_unverified_workers=allow_unverified_workers
        )
    if value["fixture"].get("workload_count") != len(value["workloads"]):
        fail("fixture workload count mismatch")
    if not value["workloads"]:
        fail("artifact has no workloads")

    for workload in value["workloads"]:
        name = workload.get("name", "unknown")
        for digest_field in ("input_sha256", "answer_sha256"):
            digest = workload.get(digest_field)
            if not isinstance(digest, str) or len(digest) != 64:
                fail(f"{name}: invalid {digest_field}")
        all_baseline = []
        all_candidate = []
        epochs = workload.get("latency_epochs")
        if not isinstance(epochs, list) or len(epochs) != settings["latency_epochs"]:
            fail(f"{name}: latency epoch count mismatch")
        for epoch in epochs:
            if len(epoch.get("order_codes", [])) != settings["samples_per_epoch"]:
                fail(f"{name}: latency order count mismatch")
            if any(code not in (0, 1) for code in epoch["order_codes"]):
                fail(f"{name}: invalid latency order code")
            all_baseline.extend(
                _check_serial(epoch["arms"]["baseline"], f"{name}/baseline")
            )
            all_candidate.extend(
                _check_serial(epoch["arms"]["candidate"], f"{name}/candidate")
            )
        baseline_p50 = int(statistics.median(all_baseline))
        candidate_p50 = int(statistics.median(all_candidate))
        if candidate_p50 > _allowed(
            baseline_p50, LATENCY_CEILING, LATENCY_ABSOLUTE_SLACK_NS
        ):
            fail(f"{name}: latency regression")

        memory = workload.get("memory")
        if len(memory.get("order_codes", [])) != settings["memory_samples"]:
            fail(f"{name}: memory order count mismatch")
        peaks = {}
        for arm in ("baseline", "candidate"):
            samples = memory["arms"][arm]
            if len(samples) != settings["memory_samples"]:
                fail(f"{name}: memory sample count mismatch")
            pids = [sample.get("worker_pid") for sample in samples]
            if any(type(pid) is not int or pid <= 0 for pid in pids) or len(
                set(pids)
            ) != len(pids):
                fail(f"{name}: memory samples require fresh workers")
            values = [sample.get("peak_rss_bytes") for sample in samples]
            if any(type(item) is not int or item <= 0 for item in values):
                fail(f"{name}: invalid RSS sample")
            peaks[arm] = int(statistics.median(values))
        if peaks["candidate"] > _allowed(
            peaks["baseline"], MEMORY_CEILING, MEMORY_ABSOLUTE_SLACK_BYTES
        ):
            fail(f"{name}: memory regression")

        storage = workload.get("storage_bytes")
        if any(
            type(storage.get(arm)) is not int or storage[arm] < 0
            for arm in ("baseline", "candidate")
        ):
            fail(f"{name}: invalid storage evidence")
        if storage["candidate"] > math.ceil(storage["baseline"] * STORAGE_CEILING):
            fail(f"{name}: storage regression")

        concurrency = workload.get("concurrency")
        if [row.get("workers") for row in concurrency] != settings[
            "concurrency_levels"
        ]:
            fail(f"{name}: concurrency levels mismatch")
        for row in concurrency:
            aggregates = {}
            for arm in ("baseline", "candidate"):
                arm_epochs = row["arms"][arm]
                if len(arm_epochs) != settings["concurrency_epochs"]:
                    fail(f"{name}: concurrency epoch count mismatch")
                qps = []
                p95 = []
                for epoch in arm_epochs:
                    samples = _check_serial(epoch, f"{name}/{arm}/concurrency")
                    if epoch.get("requests") != len(samples):
                        fail(f"{name}: concurrency request count mismatch")
                    if epoch["requests"] < (
                        row["workers"] * settings["concurrency_requests_per_worker"]
                    ):
                        fail(f"{name}: concurrency request floor not met")
                    wall_ns = epoch.get("wall_ns")
                    if type(wall_ns) is not int or wall_ns < int(
                        settings["concurrency_min_seconds"] * 1_000_000_000
                    ):
                        fail(f"{name}: concurrency duration floor not met")
                    if len(epoch.get("answer_sha256s", [])) != len(samples) or any(
                        digest != workload["answer_sha256"]
                        for digest in epoch["answer_sha256s"]
                    ):
                        fail(f"{name}: concurrency exact-answer mismatch")
                    throughput = epoch.get("throughput_qps")
                    if not isinstance(throughput, (int, float)) or throughput <= 0:
                        fail(f"{name}: invalid concurrency throughput")
                    expected_throughput = round(
                        epoch["requests"] / (wall_ns / 1_000_000_000), 6
                    )
                    if throughput != expected_throughput:
                        fail(f"{name}: concurrency throughput summary mismatch")
                    qps.append(float(throughput))
                    p95.append(epoch["p95_ns"])
                mean = statistics.mean(qps)
                cv = statistics.pstdev(qps) / mean if len(qps) > 1 else 0.0
                if cv > MAX_CONCURRENCY_QPS_CV:
                    fail(f"{name}: unstable {arm} concurrency throughput")
                aggregates[arm] = {
                    "qps": statistics.median(qps),
                    "p95": int(statistics.median(p95)),
                }
            if (
                aggregates["candidate"]["qps"]
                < aggregates["baseline"]["qps"] / CONCURRENCY_CEILING
            ):
                fail(f"{name}: concurrency throughput regression")
            if aggregates["candidate"]["p95"] > _allowed(
                aggregates["baseline"]["p95"],
                CONCURRENCY_CEILING,
                CONCURRENCY_ABSOLUTE_SLACK_NS,
            ):
                fail(f"{name}: concurrency p95 regression")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    root = Path(__file__).resolve().parents[1]
    parser.add_argument("--controller-source", type=Path, default=root)
    parser.add_argument("--baseline-source", required=True, type=Path)
    parser.add_argument("--candidate-source", type=Path, default=root)
    parser.add_argument("--allow-dirty-controller", action="store_true")
    parser.add_argument("--allow-dirty-candidate", action="store_true")
    parser.add_argument("--allow-unverified-workers", action="store_true")
    args = parser.parse_args()
    value = json.loads(args.artifact.read_text())

    def revision(source: Path) -> str:
        return subprocess.run(
            ["git", "-C", str(source), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    validate(
        value,
        allow_dirty_controller=args.allow_dirty_controller,
        allow_dirty_candidate=args.allow_dirty_candidate,
        allow_unverified_workers=args.allow_unverified_workers,
        expected_controller_revision=revision(args.controller_source),
        expected_baseline_revision=revision(args.baseline_source),
        expected_candidate_revision=revision(args.candidate_source),
    )
    print(f"candidate regression artifact valid: {args.artifact}")


if __name__ == "__main__":
    main()
