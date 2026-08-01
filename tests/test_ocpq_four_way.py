from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "ocpq_four_way_checker", ROOT / "benchmarks/check_ocpq_four_way.py"
)
CHECKER = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(CHECKER)


def node() -> dict:
    return {
        "node_index": 0,
        "object_variables": [0],
        "event_variables": [],
        "label_names": [],
        "situation_count": 1,
        "situation_violated_count": 0,
        "violation_reason_counts": {},
        "canonical_json_bytes": 2,
        "canonical_sha256": "a" * 64,
    }


def artifacts() -> tuple[dict, dict, dict, dict]:
    reference_queries = {
        query: {"runs_ms": [40.0] * 10, "mean_ms": 40.0, "nodes": [node()]}
        for query in CHECKER.QUERIES
    }
    reference = {
        "source": copy.deepcopy(CHECKER.STABLE_SOURCE),
        "queries": reference_queries,
    }

    def pm4py(backend: str, duration: float) -> dict:
        source = {
            "backend": backend,
            "dataset": "bpic2017-ocpq",
            "pm4py_version": "2.7.23.3",
            "pg_ocpm_version": "0.9.0" if backend == "pg_ocpm" else None,
            "reference_source": copy.deepcopy(CHECKER.STABLE_SOURCE),
            "reference_artifact_sha256": "b" * 64,
        }
        if backend == "pg_ocpm":
            source.update(
                pg_ocpm_source_revision="working-tree-preview",
                pg_ocpm_source_tree_clean=False,
            )
        return {
            "source": source,
            "method": {
                "warmups_per_query": 0,
                "measured_runs_per_query": 10,
                "query_protocol": "one fresh Docker process per query",
            },
            "summary": {"every_query_and_node_exact": True},
            "queries": {
                query: {
                    "runs_ms": [duration] * 10,
                    "mean_ms": duration,
                    "every_node_exact": True,
                    "nodes": [node()],
                }
                for query in CHECKER.QUERIES
            },
        }

    engine = {
        "reference_artifact_sha256": "b" * 64,
        "reference_source": copy.deepcopy(CHECKER.STABLE_SOURCE),
        "fresh_container_per_query": True,
        "release": {"pg_ocpm": "0.9.0", "ocpm_engine": "0.9.0"},
        "queries": {
            query: {
                "runs_ms": [2.0] * 10,
                "candidate_mean_ms": 2.0,
                "reference_ocpq_mean_ms": 40.0,
                "every_node_exact": True,
                "nodes": [node()],
            }
            for query in CHECKER.QUERIES
        },
    }
    return reference, pm4py("vanilla_pg", 80.0), pm4py("pg_ocpm", 60.0), engine


def test_certify_recomputes_ratios_and_keeps_preview_label() -> None:
    result = CHECKER.certify(*artifacts(), reference_sha256="b" * 64)

    assert result["every_query_and_node_exact"] is True
    assert result["publication_ready"] is False
    assert result["engine_speedups"] == pytest.approx(
        {
            "vs_ocpq": 20.0,
            "vs_vanilla_pg_pm4py": 40.0,
            "vs_pg_ocpm_pm4py": 30.0,
        }
    )


def test_certify_rejects_one_node_mismatch() -> None:
    reference, vanilla, pg_pm4py, engine = artifacts()
    pg_pm4py["queries"]["Q7"]["nodes"][0]["canonical_sha256"] = "c" * 64

    with pytest.raises(CHECKER.FourWayError, match="node fingerprint mismatch"):
        CHECKER.certify(
            reference,
            vanilla,
            pg_pm4py,
            engine,
            reference_sha256="b" * 64,
        )


def test_certify_rejects_pm4py_reference_digest_drift() -> None:
    reference, vanilla, pg_pm4py, engine = artifacts()
    vanilla["source"]["reference_artifact_sha256"] = "c" * 64

    with pytest.raises(CHECKER.FourWayError, match="reference digest mismatch"):
        CHECKER.certify(
            reference,
            vanilla,
            pg_pm4py,
            engine,
            reference_sha256="b" * 64,
        )


def test_verified_runner_digest_still_keeps_four_way_result_descriptive() -> None:
    reference, vanilla, pg_pm4py, engine = artifacts()
    for artifact in (vanilla, pg_pm4py):
        artifact["source"]["pm4py_runner_sha256"] = "d" * 64

    result = CHECKER.certify(
        reference,
        vanilla,
        pg_pm4py,
        engine,
        reference_sha256="b" * 64,
        pm4py_runner_sha256="d" * 64,
    )

    assert result["publication_ready"] is False
    assert any(
        "descriptive-only" in blocker for blocker in result["publication_blockers"]
    )


@pytest.mark.parametrize(
    ("artifact_index", "version", "message"),
    [
        (1, "0.9.0", "vanilla: pg_ocpm provenance must be absent"),
        (2, "0.8.0", "pg_pm4py: pg_ocpm version mismatch"),
    ],
)
def test_certify_rejects_backend_version_drift(
    artifact_index: int, version: str, message: str
) -> None:
    values = list(artifacts())
    values[artifact_index]["source"]["pg_ocpm_version"] = version

    with pytest.raises(CHECKER.FourWayError, match=message):
        CHECKER.certify(*values, reference_sha256="b" * 64)


@pytest.mark.parametrize("invalid", [True, float("inf"), float("nan"), 0.0])
def test_certify_rejects_invalid_timing_samples(invalid: object) -> None:
    reference, vanilla, pg_pm4py, engine = artifacts()
    vanilla["queries"]["Q1"]["runs_ms"][0] = invalid

    with pytest.raises(CHECKER.FourWayError, match="positive measured samples"):
        CHECKER.certify(
            reference,
            vanilla,
            pg_pm4py,
            engine,
            reference_sha256="b" * 64,
        )
