from __future__ import annotations

from datetime import datetime, timezone

import pytest

from benchmarks.ecosystem.common import (
    WORKLOADS,
    Fixture,
    answer_sha256,
    score_lifecycles,
)
from benchmarks.ecosystem.fixture import select_object_type
from benchmarks.ecosystem.merge_report import exactness_rows


def moment(day: int, hour: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, tzinfo=timezone.utc)


@pytest.fixture
def fixture() -> Fixture:
    return Fixture(
        dataset_name="tiny",
        baseline_dataset_id=1,
        ocpm_dataset_id=2,
        tenant_id=1,
        object_type="order",
        from_time=moment(1),
        train_to=moment(4, 23),
        test_from=moment(5),
        to_time=moment(8, 23),
        cases=4,
        train_cases=2,
        test_cases=2,
    )


@pytest.fixture
def lifecycles() -> list[list[tuple[str, datetime]]]:
    return [
        [("A", moment(1)), ("B", moment(2))],
        [("A", moment(3)), ("C", moment(4))],
        [("A", moment(5)), ("B", moment(6))],
        [("A", moment(7)), ("D", moment(8))],
    ]


def test_common_scores_have_fixed_window_and_tie_semantics(
    fixture: Fixture, lifecycles: list[list[tuple[str, datetime]]]
) -> None:
    dfg = score_lifecycles(lifecycles, fixture, "dfg_conformance_95pct")
    assert dfg["answer"] == {
        "fitness": 0.5,
        "conforming": 1,
        "deviations": 1,
        "test_total": 2,
        "model": [["A", "B", "directly_follows"], ["A", "C", "directly_follows"]],
    }

    prediction = score_lifecycles(lifecycles, fixture, "next_activity_prediction")
    assert prediction["answer"]["accuracy"] == 0.5
    assert prediction["answer"]["predictions"] == [["A", "directly_follows", "B"]]

    variants = score_lifecycles(lifecycles, fixture, "variant_conformance_95pct")
    assert variants["answer"]["fitness"] == 0.5
    assert variants["input"] == {
        "selected_cases": 4,
        "event_rows": 8,
        "aggregate_rows": 3,
    }

    bottlenecks = score_lifecycles(lifecycles, fixture, "edge_bottleneck_ranking")
    assert bottlenecks["answer"] == [
        ["A", "B", 2, 86400.0],
        ["A", "C", 1, 86400.0],
        ["A", "D", 1, 86400.0],
    ]


def test_object_type_selection_uses_predeclared_tie_breakers() -> None:
    dataset = {
        "objects": [(1, "o1", "zeta"), (2, "o2", "alpha")],
        "event_objects": [
            (1, 1, "zeta", ""),
            (2, 1, "zeta", ""),
            (3, 2, "alpha", ""),
            (4, 2, "alpha", ""),
        ],
    }
    selected, candidates = select_object_type(dataset)
    assert selected == "alpha"
    assert [row["object_type"] for row in candidates] == ["alpha", "zeta"]


def arm(name: str, answers: dict[str, object]) -> dict[str, object]:
    return {
        "arm": name,
        "datasets": [
            {
                "dataset": "tiny",
                "serial": {
                    workload: {
                        "answer": answers[workload],
                        "answer_sha256": answer_sha256(answers[workload]),
                    }
                    for workload in WORKLOADS
                },
            }
        ],
    }


def test_pair_merge_rejects_a_single_nonidentical_answer() -> None:
    expected = {workload: {"workload": workload, "value": 1} for workload in WORKLOADS}
    candidate = dict(expected)
    candidate["next_activity_prediction"] = {
        "workload": "next_activity_prediction",
        "value": 2,
    }

    with pytest.raises(SystemExit, match="exact-answer publication gate failed"):
        exactness_rows(
            arm("pg_ocpm_ocpm_engine", expected),
            arm("ocpa", candidate),
        )
