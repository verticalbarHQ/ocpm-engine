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


def test_compact_public_baseline_has_only_latency_and_storage_evidence() -> None:
    baseline = load_baseline()
    CHECKER.validate_regression_baseline(baseline)

    assert "concurrency" not in baseline
    assert "drift_concurrency" not in baseline
    assert "concurrency" not in baseline["method"]
    assert "concurrency_model" not in baseline["method"]
    for dataset in baseline["datasets"]:
        for workload in dataset["workloads"]:
            assert set(workload["vanilla_postgres_python"]) == {"p50_ms"}
            assert set(workload["pg_ocpm_rust"]) == {"p50_ms"}
    for storage in baseline["storage"].values():
        assert set(storage) == {"index_bytes", "total_bytes"}


def test_public_baseline_contract_rejects_historical_concurrency() -> None:
    baseline = copy.deepcopy(load_baseline())
    baseline["concurrency"] = {"requests": 32}

    with pytest.raises(SystemExit, match="unexpected fields"):
        CHECKER.validate_regression_baseline(baseline)


def test_public_baseline_contract_rejects_extra_latency_metrics() -> None:
    baseline = copy.deepcopy(load_baseline())
    baseline["datasets"][0]["workloads"][0]["pg_ocpm_rust"]["p95_ms"] = 1.2

    with pytest.raises(SystemExit, match="non-p50 latency fields"):
        CHECKER.validate_regression_baseline(baseline)
