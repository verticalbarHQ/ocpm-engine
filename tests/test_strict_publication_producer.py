from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE_DIRECTORY = ROOT / "benchmarks/ocpq"
JQ = shutil.which("jq")

FILTER = """
include "strict_publication_readiness";
strict_publication_readiness(
  .latency_targets_met;
  .storage_within_limits;
  .latency_environment;
  .resource_environment;
  .reference_environment;
  .concurrency_exact;
  .memory_exact
)
"""


def environment(*, engine_clean: bool, pg_clean: bool, claimed: bool) -> dict:
    return {
        "provenance_complete": claimed,
        "source_tree_clean": engine_clean,
        "pg_ocpm_source_tree_clean": pg_clean,
    }


def payload(
    *, engine_clean: bool, reference_clean: bool, pg_clean: bool, claimed: bool
) -> dict:
    return {
        "latency_targets_met": True,
        "storage_within_limits": True,
        "latency_environment": environment(
            engine_clean=engine_clean,
            pg_clean=pg_clean,
            claimed=claimed,
        ),
        "resource_environment": environment(
            engine_clean=engine_clean,
            pg_clean=pg_clean,
            claimed=claimed,
        ),
        "reference_environment": {"source_tree_clean": reference_clean},
        "concurrency_exact": True,
        "memory_exact": True,
    }


def aggregate(value: dict, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [JQ or "jq", "--compact-output", "-L", str(MODULE_DIRECTORY), FILTER],
        input=json.dumps(value),
        capture_output=True,
        check=check,
        text=True,
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(
            payload(
                engine_clean=False,
                reference_clean=False,
                pg_clean=True,
                claimed=False,
            ),
            False,
            id="dirty-engine-and-reference",
        ),
        pytest.param(
            payload(
                engine_clean=True,
                reference_clean=True,
                pg_clean=False,
                claimed=False,
            ),
            False,
            id="dirty-pg-ocpm",
        ),
        pytest.param(
            payload(
                engine_clean=True,
                reference_clean=True,
                pg_clean=True,
                claimed=True,
            ),
            True,
            id="all-clean",
        ),
        pytest.param(
            payload(
                engine_clean=False,
                reference_clean=False,
                pg_clean=True,
                claimed=True,
            ),
            False,
            id="forged-provenance-with-dirty-flags",
        ),
    ],
)
def test_producer_aggregates_cleanliness_into_readiness(
    value: dict, expected: bool
) -> None:
    result = json.loads(aggregate(value).stdout)

    assert result == {
        "provenance_complete": expected,
        "ready": expected,
    }


def test_producer_rejects_quoted_boolean() -> None:
    value = payload(
        engine_clean=True,
        reference_clean=True,
        pg_clean=True,
        claimed=True,
    )
    value["latency_environment"]["source_tree_clean"] = "true"

    result = aggregate(value, check=False)

    assert result.returncode != 0
    assert "source_tree_clean must be a JSON boolean" in result.stderr


def test_runner_uses_the_tested_readiness_filter() -> None:
    runner = (MODULE_DIRECTORY / "run_strict_publication_gates.sh").read_text()

    assert 'include "strict_publication_readiness";' in runner
    assert "| strict_publication_readiness(" in runner
