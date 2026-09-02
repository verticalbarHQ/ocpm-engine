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
EXPECTED_GATE_SETTINGS = {
    "random_seed": 1729,
    "warmups": 4,
    "latency_epochs": 3,
    "samples_per_epoch": 20,
    "memory_samples": 4,
    "concurrency_levels": [1, 2, 4],
    "concurrency_epochs": 5,
    "concurrency_requests_per_worker": 8,
    "concurrency_min_seconds": 5.0,
}


def canonical_payload(value: dict[str, Any]) -> bytes:
    unsigned = dict(value)
    unsigned.pop("payload_sha256", None)
    return json.dumps(unsigned, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def payload_sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_payload(value)).hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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
    return {
        "revision": git(root, "rev-parse", "HEAD"),
        "tree_clean": not bool(status),
        "tree_sha256": hashlib.sha256(tracked + canonical(untracked)).hexdigest(),
    }


def manifest_expectations(path: Path) -> tuple[str, list[dict[str, str]]]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or set(value) != {"schema_version", "workloads"}:
        fail("candidate manifest fields changed")
    workloads = value["workloads"]
    if value["schema_version"] != 1 or not isinstance(workloads, list) or not workloads:
        fail("invalid candidate manifest")
    expected = []
    names = set()
    for workload in workloads:
        if not isinstance(workload, dict) or set(workload) != {
            "name",
            "payload",
            "expected_input",
        }:
            fail("candidate workload fields changed")
        name = workload["name"]
        if not isinstance(name, str) or not name or name in names:
            fail("candidate workload name is invalid or duplicated")
        names.add(name)
        expected.append(
            {
                "name": name,
                "input_sha256": hashlib.sha256(
                    canonical(workload["expected_input"])
                ).hexdigest(),
            }
        )
    return file_sha256(path), expected


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
    arm: dict[str, Any],
    label: str,
    *,
    allow_unverified_workers: bool,
    expected_source_lock_sha256: str | None,
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
        "source_lock_sha256",
        "worker_source_sha256",
    }:
        fail(f"{label}: worker build inputs changed")
    if any(
        not isinstance(digest, str) or len(digest) != 64 for digest in inputs.values()
    ):
        fail(f"{label}: invalid worker build input digest")
    if (
        expected_source_lock_sha256 is not None
        and inputs["source_lock_sha256"] != expected_source_lock_sha256
    ):
        fail(f"{label}: worker lock does not match checked-out source")


def validate(
    value: dict[str, Any],
    *,
    allow_dirty_controller: bool,
    allow_dirty_candidate: bool,
    allow_unverified_workers: bool = False,
    expected_controller_revision: str | None = None,
    expected_baseline_revision: str | None = None,
    expected_candidate_revision: str | None = None,
    expected_controller_source: dict[str, Any] | None = None,
    expected_baseline_source: dict[str, Any] | None = None,
    expected_candidate_source: dict[str, Any] | None = None,
    expected_baseline_lock_sha256: str | None = None,
    expected_candidate_lock_sha256: str | None = None,
    expected_manifest_sha256: str | None = None,
    expected_workloads: list[dict[str, str]] | None = None,
    expected_gate_settings: dict[str, Any] | None = None,
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
    if expected_gate_settings is not None:
        for name, expected in expected_gate_settings.items():
            if settings.get(name) != expected:
                fail(f"settings/{name}: gate setting changed")
    concurrency_levels = settings.get("concurrency_levels")
    if (
        not isinstance(concurrency_levels, list)
        or not concurrency_levels
        or any(type(level) is not int or level < 1 for level in concurrency_levels)
        or len(set(concurrency_levels)) != len(concurrency_levels)
    ):
        fail("settings/concurrency_levels: levels must be nonempty unique positives")
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
    if (
        expected_controller_source is not None
        and controller != expected_controller_source
    ):
        fail("controller checkout state does not match artifact")
    for arm, expected in (
        ("baseline", expected_baseline_source),
        ("candidate", expected_candidate_source),
    ):
        identity = {
            name: arms.get(arm, {}).get(name)
            for name in ("revision", "tree_clean", "tree_sha256")
        }
        if expected is not None and identity != expected:
            fail(f"{arm} checkout state does not match artifact")
    if not controller.get("tree_clean") and not allow_dirty_controller:
        fail("controller source is dirty")
    if not arms.get("baseline", {}).get("tree_clean"):
        fail("baseline source is dirty")
    if not arms.get("candidate", {}).get("tree_clean") and not allow_dirty_candidate:
        fail("candidate source is dirty")
    for arm in ("baseline", "candidate"):
        _check_worker_provenance(
            arms.get(arm, {}),
            arm,
            allow_unverified_workers=allow_unverified_workers,
            expected_source_lock_sha256={
                "baseline": expected_baseline_lock_sha256,
                "candidate": expected_candidate_lock_sha256,
            }[arm],
        )
    fixture = value["fixture"]
    if not isinstance(fixture, dict) or set(fixture) != {
        "manifest_sha256",
        "workload_count",
    }:
        fail("fixture fields changed")
    if fixture.get("workload_count") != len(value["workloads"]):
        fail("fixture workload count mismatch")
    if not value["workloads"]:
        fail("artifact has no workloads")
    if (
        expected_manifest_sha256 is not None
        and fixture.get("manifest_sha256") != expected_manifest_sha256
    ):
        fail("fixture manifest does not match the gate manifest")
    if expected_workloads is not None:
        actual_workloads = [
            {"name": workload.get("name"), "input_sha256": workload.get("input_sha256")}
            for workload in value["workloads"]
        ]
        if actual_workloads != expected_workloads:
            fail("workload inputs do not match the gate manifest")

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
            for arm, collected in (
                ("baseline", all_baseline),
                ("candidate", all_candidate),
            ):
                samples = _check_serial(epoch["arms"][arm], f"{name}/{arm}")
                if len(samples) != settings["samples_per_epoch"]:
                    fail(f"{name}: latency sample count mismatch")
                collected.extend(samples)
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
        if not isinstance(concurrency, list) or not concurrency:
            fail(f"{name}: concurrency evidence is empty")
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
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--allow-dirty-controller", action="store_true")
    parser.add_argument("--allow-dirty-candidate", action="store_true")
    parser.add_argument("--allow-unverified-workers", action="store_true")
    args = parser.parse_args()
    value = json.loads(args.artifact.read_text())

    controller_source = source_provenance(args.controller_source)
    baseline_source = source_provenance(args.baseline_source)
    candidate_source = source_provenance(args.candidate_source)
    manifest_sha256, expected_workloads = manifest_expectations(args.manifest)

    validate(
        value,
        allow_dirty_controller=args.allow_dirty_controller,
        allow_dirty_candidate=args.allow_dirty_candidate,
        allow_unverified_workers=args.allow_unverified_workers,
        expected_controller_revision=controller_source["revision"],
        expected_baseline_revision=baseline_source["revision"],
        expected_candidate_revision=candidate_source["revision"],
        expected_controller_source=controller_source,
        expected_baseline_source=baseline_source,
        expected_candidate_source=candidate_source,
        expected_baseline_lock_sha256=file_sha256(args.baseline_source / "Cargo.lock"),
        expected_candidate_lock_sha256=file_sha256(
            args.candidate_source / "Cargo.lock"
        ),
        expected_manifest_sha256=manifest_sha256,
        expected_workloads=expected_workloads,
        expected_gate_settings=EXPECTED_GATE_SETTINGS,
    )
    print(f"candidate regression artifact valid: {args.artifact}")


if __name__ == "__main__":
    main()
