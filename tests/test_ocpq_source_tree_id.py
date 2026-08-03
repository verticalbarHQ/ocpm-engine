from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
FINGERPRINT = ROOT / "benchmarks/ocpq/source_tree_id.sh"
pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git is required")


def _run(*arguments: str, cwd: Path) -> str:
    return subprocess.run(
        arguments,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _fingerprint(repository: Path) -> str:
    value = _run("bash", str(FINGERPRINT), str(repository), cwd=repository)
    assert value.startswith("git-tree:")
    return value


def test_source_tree_id_captures_every_build_context_change(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _run("git", "init", "--quiet", cwd=repository)
    _run("git", "config", "user.email", "benchmark@example.invalid", cwd=repository)
    _run("git", "config", "user.name", "Benchmark Test", cwd=repository)
    (repository / ".gitignore").write_text("ignored\n")
    (repository / "tracked.txt").write_text("base\n")
    (repository / "deleted.txt").write_text("delete me\n")
    _run("git", "add", ".", cwd=repository)
    _run("git", "commit", "--quiet", "-m", "base", cwd=repository)
    baseline = _fingerprint(repository)

    (repository / "tracked.txt").write_text("staged only\n")
    _run("git", "add", "tracked.txt", cwd=repository)
    staged = _fingerprint(repository)
    assert staged != baseline

    (repository / "tracked.txt").write_text("staged plus unstaged\n")
    unstaged = _fingerprint(repository)
    assert unstaged not in {baseline, staged}

    (repository / "new.txt").write_text("untracked\n")
    untracked = _fingerprint(repository)
    assert untracked not in {baseline, staged, unstaged}

    (repository / "deleted.txt").unlink()
    deleted = _fingerprint(repository)
    assert deleted not in {baseline, staged, unstaged, untracked}

    (repository / "ignored").write_text("not in Docker context\n")
    assert _fingerprint(repository) == deleted
