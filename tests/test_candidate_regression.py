from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.check_candidate_regression import payload_sha256, validate

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "tests/fixtures/candidate_gate_worker.py"
MANIFEST = ROOT / "benchmarks/fixtures/candidate-gate-smoke.json"


@pytest.fixture(scope="module")
def artifact(tmp_path_factory: pytest.TempPathFactory) -> dict:
    temporary = tmp_path_factory.mktemp("candidate-gate")
    output = temporary / "result.json"
    baseline_source = temporary / "baseline-source"
    subprocess.run(
        ["git", "clone", "--quiet", "--no-hardlinks", str(ROOT), str(baseline_source)],
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "benchmarks/candidate_regression.py"),
            "--baseline-source",
            str(baseline_source),
            "--candidate-source",
            str(ROOT),
            "--baseline-worker",
            str(WORKER),
            "--candidate-worker",
            str(WORKER),
            "--manifest",
            str(MANIFEST),
            "--output",
            str(output),
            "--warmups",
            "1",
            "--latency-epochs",
            "2",
            "--samples-per-epoch",
            "6",
            "--memory-samples",
            "2",
            "--concurrency-levels",
            "1,2",
            "--concurrency-epochs",
            "3",
            "--concurrency-requests-per-worker",
            "4",
            "--concurrency-min-seconds",
            "0.5",
            "--allow-dirty-controller",
            "--allow-dirty-candidate",
        ],
        cwd=ROOT,
        check=True,
    )
    return json.loads(output.read_text())


def resign(value: dict) -> dict:
    value["payload_sha256"] = payload_sha256(value)
    return value


def test_executed_noop_candidate_passes(artifact: dict) -> None:
    validate(artifact, allow_dirty_controller=True, allow_dirty_candidate=True)
    assert (
        artifact["arms"]["baseline"]["worker_sha256"]
        == artifact["arms"]["candidate"]["worker_sha256"]
    )
    assert artifact["workloads"][0]["answer_sha256"]


def test_checker_rejects_an_answer_mismatch(artifact: dict) -> None:
    changed = copy.deepcopy(artifact)
    changed["workloads"][0]["concurrency"][0]["arms"]["candidate"][0]["answer_sha256s"][
        0
    ] = "0" * 64
    with pytest.raises(ValueError, match="exact-answer mismatch"):
        validate(
            resign(changed),
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
        )


def test_checker_rejects_a_historical_payload_without_workloads(artifact: dict) -> None:
    changed = copy.deepcopy(artifact)
    changed["workloads"] = []
    changed["fixture"]["workload_count"] = 0
    with pytest.raises(ValueError, match="no workloads"):
        validate(
            resign(changed),
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
        )


def test_checker_rejects_latency_regression(artifact: dict) -> None:
    changed = copy.deepcopy(artifact)
    for epoch in changed["workloads"][0]["latency_epochs"]:
        serial = epoch["arms"]["candidate"]
        serial["samples_ns"] = [value * 100 for value in serial["samples_ns"]]
        ordered = sorted(serial["samples_ns"])
        serial["p50_ns"] = int((ordered[2] + ordered[3]) / 2)
        serial["p95_ns"] = ordered[-1]
    with pytest.raises(ValueError, match="latency regression"):
        validate(
            resign(changed),
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
        )


def test_checker_rejects_dirty_baseline(artifact: dict) -> None:
    changed = copy.deepcopy(artifact)
    changed["arms"]["baseline"]["tree_clean"] = False
    with pytest.raises(ValueError, match="baseline source is dirty"):
        validate(
            resign(changed),
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
        )
