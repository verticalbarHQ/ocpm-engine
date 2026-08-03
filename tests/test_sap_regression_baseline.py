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


def sap_schema5_shape() -> dict:
    result = {field: None for field in CHECKER.EXPECTED_RESULT_FIELDS}
    latency = {field: None for field in CHECKER.EXPECTED_LATENCY_FIELDS}
    dataset = {field: None for field in CHECKER.EXPECTED_DATASET_FIELDS}
    dataset["latency"] = [latency]
    result["datasets"] = [dataset]
    return result


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
        ROOT / ".benchmarks/sap-release-bridge-0.6.0-to-0.8.0.json"
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


def test_sap_schema5_shape_matches_current_producer() -> None:
    assert CHECKER.EXPECTED_RESULT_FIELDS == {
        "schema_version",
        "generated_at",
        "source",
        "environment",
        "provenance",
        "method",
        "summary",
        "datasets",
        "storage",
        "section_generated_at",
        "payload_sha256",
    }
    assert CHECKER.EXPECTED_DATASET_FIELDS == {
        "dataset",
        "source_counts",
        "fixture",
        "summary",
        "latency",
        "concurrency",
        "memory",
    }
    assert CHECKER.EXPECTED_LATENCY_FIELDS == {
        "workload",
        "correct",
        "vanilla_pg_pm4py",
        "pg_ocpm_pm4py",
        "pg_ocpm_ocpm_engine",
        "speedups",
        "first_execution_counts",
        "serial_epochs",
        "answer_sha256",
    }
    CHECKER.validate_artifact_shape(sap_schema5_shape())


@pytest.mark.parametrize(
    ("level", "message"),
    (
        ("top_level", "top-level fields changed"),
        ("dataset", "dataset fields changed"),
        ("latency", "latency row fields changed"),
    ),
)
def test_sap_schema5_contract_rejects_unknown_fields(level: str, message: str) -> None:
    result = sap_schema5_shape()
    targets = {
        "top_level": result,
        "dataset": result["datasets"][0],
        "latency": result["datasets"][0]["latency"][0],
    }
    targets[level]["unexpected_schema5_field"] = True

    with pytest.raises(SystemExit, match=message):
        CHECKER.validate_contract(result, {}, allow_dirty=True)


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


@pytest.mark.parametrize(
    ("workload", "source", "strategy", "rows"),
    (
        (
            "dfg_conformance_95pct",
            "pg_ocpm_lifecycle_dfg_window_counts",
            "native lifecycle DFG window aggregate",
            59,
        ),
        (
            "variant_conformance_95pct",
            "pg_ocpm_lifecycle_variant_window_counts",
            "native lifecycle variant window aggregate",
            798,
        ),
        (
            "next_activity_prediction",
            "pg_ocpm_lifecycle_dfg_window_counts",
            "native lifecycle DFG window aggregate",
            59,
        ),
        (
            "edge_bottleneck_ranking",
            "pg_ocpm_edge_feature_aggregates",
            "native filtered edge feature aggregate",
            60,
        ),
    ),
)
def test_provider_aggregate_input_contract(
    workload: str,
    source: str,
    strategy: str,
    rows: int,
) -> None:
    CHECKER.validate_provider_aggregate_input(
        "sap_o2c",
        workload,
        {
            "source": source,
            "strategy": strategy,
            "database_rows": rows,
            "expanded_event_rows": 0,
            "aggregate_rows": rows,
        },
        {"aggregate_rows": rows},
    )


def test_provider_aggregate_input_rejects_expanded_rows() -> None:
    with pytest.raises(SystemExit, match="invalid provider aggregate input"):
        CHECKER.validate_provider_aggregate_input(
            "sap_o2c",
            "dfg_conformance_95pct",
            {
                "source": "pg_ocpm_lifecycle_dfg_window_counts",
                "strategy": "native lifecycle DFG window aggregate",
                "database_rows": 59,
                "expanded_event_rows": 1,
                "aggregate_rows": 59,
            },
            {"aggregate_rows": 59},
        )


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
