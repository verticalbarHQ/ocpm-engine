from copy import deepcopy

from benchmarks.check_regression import validate_regression


def result(p50_ms: float = 10.0, total_bytes: int = 1_000) -> dict:
    engine_result = {"p50_ms": p50_ms}
    storage_result = {"totals": {"total_bytes": total_bytes}}
    return {
        "dataset": {"events": 10, "edges": 9, "cases": 2},
        "cases": [
            {
                "scenario": "example",
                "agreement": True,
                "vanilla_postgres": deepcopy(engine_result),
                "verticalbar_optimized": deepcopy(engine_result),
                "pg_ocpm": deepcopy(engine_result),
            }
        ],
        "storage_and_index_usage": {
            "vanilla_postgres": deepcopy(storage_result),
            "verticalbar_optimized": deepcopy(storage_result),
            "pg_ocpm": deepcopy(storage_result),
        },
    }


def test_matching_result_passes() -> None:
    assert validate_regression(result(), result()) == []


def test_correctness_regression_fails() -> None:
    candidate = result()
    candidate["cases"][0]["agreement"] = False
    assert "correctness gate failed" in validate_regression(result(), candidate)[0]


def test_latency_regression_fails() -> None:
    candidate = result(p50_ms=13.0)
    failures = validate_regression(result(), candidate)
    assert len([failure for failure in failures if "p50" in failure]) == 3


def test_storage_regression_fails() -> None:
    candidate = result(total_bytes=1_100)
    failures = validate_regression(result(), candidate)
    assert len([failure for failure in failures if "storage" in failure]) == 3
