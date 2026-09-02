from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

import benchmarks.candidate_regression as candidate_regression
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
            "--allow-unverified-workers",
        ],
        cwd=ROOT,
        check=True,
    )
    return json.loads(output.read_text())


def resign(value: dict) -> dict:
    value["payload_sha256"] = payload_sha256(value)
    return value


def test_rss_sampling_tolerates_a_worker_exiting_during_proc_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class VanishedStatus:
        def exists(self) -> bool:
            return True

        def read_text(self) -> str:
            raise ProcessLookupError

    monkeypatch.setattr(candidate_regression, "Path", lambda _value: VanishedStatus())
    assert candidate_regression._rss_bytes(1) == 0


def with_worker_provenance(value: dict) -> dict:
    changed = copy.deepcopy(value)
    for arm_name in ("baseline", "candidate"):
        arm = changed["arms"][arm_name]
        provenance = {
            "schema_version": 1,
            "artifact_type": "ocpm_engine_candidate_worker",
            "source": {
                name: arm[name] for name in ("revision", "tree_clean", "tree_sha256")
            },
            "inputs": {
                "builder_sha256": "1" * 64,
                "source_lock_sha256": "3" * 64,
                "worker_source_sha256": "2" * 64,
            },
            "worker_sha256": arm["worker_sha256"],
        }
        provenance["payload_sha256"] = payload_sha256(provenance)
        arm["worker_provenance"] = provenance
    return resign(changed)


def test_executed_noop_candidate_passes(artifact: dict) -> None:
    validate(
        artifact,
        allow_dirty_controller=True,
        allow_dirty_candidate=True,
        allow_unverified_workers=True,
    )
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
            allow_unverified_workers=True,
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
            allow_unverified_workers=True,
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
            allow_unverified_workers=True,
        )


def test_checker_rejects_dirty_baseline(artifact: dict) -> None:
    changed = copy.deepcopy(artifact)
    changed["arms"]["baseline"]["tree_clean"] = False
    with pytest.raises(ValueError, match="baseline source is dirty"):
        validate(
            resign(changed),
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
            allow_unverified_workers=True,
        )


def test_checker_rejects_unverified_workers_by_default(artifact: dict) -> None:
    with pytest.raises(ValueError, match="worker provenance fields changed"):
        validate(
            artifact,
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
        )


def test_checker_rejects_another_candidate_revision(artifact: dict) -> None:
    with pytest.raises(ValueError, match="candidate revision does not match"):
        validate(
            artifact,
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
            allow_unverified_workers=True,
            expected_candidate_revision="0" * 40,
        )


def test_checker_rejects_another_baseline_revision(artifact: dict) -> None:
    with pytest.raises(ValueError, match="baseline revision does not match"):
        validate(
            artifact,
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
            allow_unverified_workers=True,
            expected_baseline_revision="0" * 40,
        )


def test_checker_rejects_dirty_same_revision_checkout(artifact: dict) -> None:
    expected = {
        name: artifact["controller"][name]
        for name in ("revision", "tree_clean", "tree_sha256")
    }
    expected["tree_clean"] = False
    expected["tree_sha256"] = "4" * 64
    with pytest.raises(ValueError, match="controller checkout state does not match"):
        validate(
            artifact,
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
            allow_unverified_workers=True,
            expected_controller_source=expected,
        )


def test_checker_rejects_worker_built_with_another_lock(artifact: dict) -> None:
    changed = with_worker_provenance(artifact)
    with pytest.raises(ValueError, match="worker lock does not match"):
        validate(
            changed,
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
            expected_baseline_lock_sha256="0" * 64,
        )


def test_checker_recomputes_concurrency_throughput(artifact: dict) -> None:
    changed = copy.deepcopy(artifact)
    changed["workloads"][0]["concurrency"][0]["arms"]["candidate"][0][
        "throughput_qps"
    ] *= 2
    with pytest.raises(ValueError, match="throughput summary mismatch"):
        validate(
            resign(changed),
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
            allow_unverified_workers=True,
        )


def test_checker_accepts_workers_bound_to_each_source(artifact: dict) -> None:
    validate(
        with_worker_provenance(artifact),
        allow_dirty_controller=True,
        allow_dirty_candidate=True,
    )


def test_checker_rejects_a_worker_bound_to_the_other_source(artifact: dict) -> None:
    changed = with_worker_provenance(artifact)
    changed["arms"]["candidate"]["revision"] = "3" * 40
    changed["arms"]["candidate"]["worker_provenance"] = copy.deepcopy(
        changed["arms"]["baseline"]["worker_provenance"]
    )
    with pytest.raises(ValueError, match="worker source does not match"):
        validate(
            resign(changed),
            allow_dirty_controller=True,
            allow_dirty_candidate=True,
        )
