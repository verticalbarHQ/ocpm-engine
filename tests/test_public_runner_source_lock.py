from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "benchmarks/run_public_benchmark.sh"


@pytest.mark.parametrize(
    "arguments",
    (
        ("--output", "/tmp/wrong.json"),
        ("--extension-host", "wrong-host"),
        ("--concurrency-only", "--output", "/tmp/wrong.json"),
    ),
)
def test_public_runner_rejects_every_trailing_argument(
    arguments: tuple[str, ...],
) -> None:
    completed = subprocess.run(
        [str(RUNNER), *arguments],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "do not accept additional arguments" in completed.stderr


def test_public_runner_checks_the_workload_import_path() -> None:
    source = RUNNER.read_text()

    benchmark_path = 'sys.path.insert(0, "/workspace/benchmarks")'
    assert benchmark_path in source
    assert source.index(benchmark_path) < source.index("import ocpm_engine")


def test_public_runner_requires_exact_associated_worktree_roots() -> None:
    source = RUNNER.read_text()

    assert "rev-parse --show-toplevel" in source
    assert "rev-parse --git-common-dir" in source
    assert '[[ "$target_top" != "$target_path" ]]' in source
    assert '[[ "$target_common" != "$repository_common" ]]' in source
    assert "symbolic-ref -q HEAD" in source


def test_public_runner_rechecks_every_source_after_image_build() -> None:
    source = RUNNER.read_text()
    build = source.index('docker compose -f "$compose" build')
    up = source.index('docker compose -f "$compose" up -d --wait')
    post_build = source[build:up]

    assert 'verify_controller_checkout "$controller_revision"' in post_build
    assert post_build.count("ensure_worktree") == 2
    for clean_assignment in (
        "controller_tree_clean=true",
        "engine_tree_clean=true",
        "pg_ocpm_tree_clean=true",
    ):
        assert clean_assignment in post_build
        assert post_build.index('verify_controller_checkout "$controller_revision"') < (
            post_build.index(clean_assignment)
        )
