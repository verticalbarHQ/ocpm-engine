from __future__ import annotations

import copy
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_sap_regression_baseline_test",
    ROOT / "benchmarks/check_sap_pm4py_result.py",
)
assert SPEC is not None and SPEC.loader is not None
CHECKER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = CHECKER
SPEC.loader.exec_module(CHECKER)


def load_baseline() -> dict:
    return CHECKER.load_verified(
        CHECKER.REGRESSION_BASELINE,
        CHECKER.EXPECTED_BASELINE_PAYLOAD_SHA256,
    )


def test_compact_sap_baseline_retains_input_and_answer_contract() -> None:
    baseline = load_baseline()
    CHECKER.validate_regression_baseline(baseline)

    assert set(baseline) == {
        "schema_version",
        "artifact_type",
        "release",
        "source",
        "datasets",
        "payload_sha256",
    }
    assert CHECKER.RELEASE_BRIDGE == (
        ROOT / "docs/results/sap-release-bridge-0.4.0-to-0.6.0.json"
    )
    assert baseline["source"] == CHECKER.EXPECTED_SOURCE
    for dataset in baseline["datasets"]:
        assert set(dataset) == {"dataset", "source_counts", "fixture", "workloads"}
        assert set(dataset["source_counts"]) == {
            "events",
            "objects",
            "event_object_links",
            "object_object_links",
        }
        assert set(dataset["fixture"]) == CHECKER.EXPECTED_FIXTURE_FIELDS
        for workload in dataset["workloads"]:
            assert set(workload) == {"workload", "answer_sha256", "input"}
            assert len(workload["answer_sha256"]) == 64
            assert set(workload["input"]) == set(CHECKER.ENGINES)
            assert all(
                set(workload["input"][engine]) == fields
                for engine, fields in CHECKER.EXPECTED_INPUT_FIELDS.items()
            )


@pytest.mark.parametrize(
    "field",
    (
        "environment",
        "method",
        "storage",
        "concurrency",
        "generated_at",
        "provenance",
        "summary",
    ),
)
def test_sap_baseline_contract_rejects_historical_top_level_fields(
    field: str,
) -> None:
    baseline = copy.deepcopy(load_baseline())
    baseline[field] = {"historical": 1}

    with pytest.raises(SystemExit, match="unexpected fields"):
        CHECKER.validate_regression_baseline(baseline)


@pytest.mark.parametrize("field", ("p50_ms", "memory", "latency", "samples"))
def test_sap_baseline_contract_rejects_historical_workload_fields(
    field: str,
) -> None:
    baseline = copy.deepcopy(load_baseline())
    workload = baseline["datasets"][0]["workloads"][0]
    workload[field] = {"historical": 1}

    with pytest.raises(SystemExit, match="workload contains unexpected fields"):
        CHECKER.validate_regression_baseline(baseline)


def test_sap_baseline_contract_rejects_extra_input_fields() -> None:
    baseline = copy.deepcopy(load_baseline())
    baseline["datasets"][0]["workloads"][0]["input"]["pg_ocpm_ocpm_engine"][
        "p50_ms"
    ] = 1.0

    with pytest.raises(SystemExit, match="input fields changed"):
        CHECKER.validate_regression_baseline(baseline)


def test_matched_bridge_is_the_release_regression_gate(monkeypatch) -> None:
    result = {"artifact_type": "sap-pm4py-test"}
    bridge = {"artifact_type": "matched-release-test"}
    checked = []

    def validate_bridge(public_result, bridge_result, *, allow_dirty):
        checked.append((public_result, bridge_result, allow_dirty))

    monkeypatch.setattr(
        CHECKER.release_regression_checker,
        "validate_for_public_pm4py",
        validate_bridge,
    )
    CHECKER.validate_regressions(result, bridge, allow_dirty=True)

    assert checked == [(result, bridge, True)]
