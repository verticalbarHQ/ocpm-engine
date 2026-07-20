from __future__ import annotations

import copy
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_public_regression_baseline_test",
    ROOT / "benchmarks/check_public_result.py",
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


def test_compact_public_baseline_retains_source_and_fixture_contract() -> None:
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
    assert (
        tuple(dataset["fixture"]["name"] for dataset in baseline["datasets"])
        == CHECKER.EXPECTED_DATASETS
    )
    for dataset in baseline["datasets"]:
        assert set(dataset) == {"fixture", "workloads"}
        assert set(dataset["fixture"]) == CHECKER.EXPECTED_FIXTURE_FIELDS
        assert (
            tuple(row["workload"] for row in dataset["workloads"])
            == CHECKER.EXPECTED_WORKLOADS
        )
        assert all(set(row) == {"workload"} for row in dataset["workloads"])


@pytest.mark.parametrize(
    "field",
    ("environment", "method", "storage", "concurrency", "drift_concurrency"),
)
def test_public_baseline_contract_rejects_historical_top_level_fields(
    field: str,
) -> None:
    baseline = copy.deepcopy(load_baseline())
    baseline[field] = {"historical": 1}

    with pytest.raises(SystemExit, match="unexpected fields"):
        CHECKER.validate_regression_baseline(baseline)


@pytest.mark.parametrize(
    "field",
    ("p50_ms", "memory", "vanilla_postgres_python", "pg_ocpm_rust"),
)
def test_public_baseline_contract_rejects_historical_workload_fields(
    field: str,
) -> None:
    baseline = copy.deepcopy(load_baseline())
    baseline["datasets"][0]["workloads"][0][field] = {"historical": 1}

    with pytest.raises(SystemExit, match="workload contains unexpected fields"):
        CHECKER.validate_regression_baseline(baseline)


def test_matched_bridge_is_the_release_regression_gate(monkeypatch) -> None:
    result = {"artifact_type": "public-common-test"}
    checked = []

    def validate_bridge(public_result, bridge, *, allow_dirty):
        checked.append((public_result, bridge, allow_dirty))

    monkeypatch.setattr(
        CHECKER.release_regression_checker,
        "validate_for_public_common",
        validate_bridge,
    )
    bridge = {"artifact_type": "matched-release-test"}
    CHECKER.validate_regressions(
        result,
        bridge,
        allow_dirty=True,
    )

    assert checked == [(result, bridge, True)]
