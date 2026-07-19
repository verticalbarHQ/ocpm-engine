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


def test_compact_sap_baseline_has_only_regression_evidence() -> None:
    baseline = load_baseline()
    CHECKER.validate_regression_baseline(baseline)

    assert "concurrency" not in baseline
    assert "generated_at" not in baseline
    assert "provenance" not in baseline
    assert "summary" not in baseline
    assert "warmups" not in baseline["method"]
    assert "measured_runs" not in baseline["method"]
    for dataset in baseline["datasets"]:
        assert "memory" not in dataset
        assert "latency" not in dataset
        for workload in dataset["workloads"]:
            assert set(workload["p50_ms"]) == set(CHECKER.ENGINES)
            for metrics in workload["memory"].values():
                assert set(metrics) == {
                    "peak_rss_bytes",
                    "incremental_peak_bytes",
                }
    for storage in baseline["storage"].values():
        assert set(storage) == {"index_bytes", "total_bytes"}


def test_sap_baseline_contract_rejects_historical_concurrency() -> None:
    baseline = copy.deepcopy(load_baseline())
    baseline["concurrency"] = {"requests": 32}

    with pytest.raises(SystemExit, match="unexpected fields"):
        CHECKER.validate_regression_baseline(baseline)


def test_sap_baseline_contract_rejects_extra_memory_metrics() -> None:
    baseline = copy.deepcopy(load_baseline())
    baseline["datasets"][0]["workloads"][0]["memory"]["pg_ocpm_ocpm_engine"][
        "elapsed_ms"
    ] = 1.0

    with pytest.raises(SystemExit, match="non-regression memory fields"):
        CHECKER.validate_regression_baseline(baseline)
