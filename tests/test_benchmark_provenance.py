from __future__ import annotations

import pytest

from benchmarks.benchmark_provenance import (
    public_benchmark_provenance,
    validate_recorded_public_provenance,
)


def valid_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        "OCPM_BENCHMARK_HOST_ID": "sha256:" + "1" * 64,
        "OCPM_CONTROLLER_SOURCE_REVISION": "2" * 40,
        "OCPM_CONTROLLER_SOURCE_TREE_CLEAN": "true",
        "OCPM_ENGINE_SOURCE_REVISION": "3" * 40,
        "OCPM_ENGINE_SOURCE_TREE_CLEAN": "true",
        "OCPM_PG_OCPM_SOURCE_REVISION": "4" * 40,
        "OCPM_PG_OCPM_SOURCE_TREE_CLEAN": "1",
        "OCPM_CLIENT_IMAGE_ID": "sha256:" + "5" * 64,
        "OCPM_VANILLA_DATABASE_IMAGE_ID": "sha256:" + "6" * 64,
        "OCPM_PG_OCPM_DATABASE_IMAGE_ID": "sha256:" + "7" * 64,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_public_benchmark_provenance(monkeypatch: pytest.MonkeyPatch) -> None:
    valid_environment(monkeypatch)
    result = public_benchmark_provenance()
    assert result["controller_source_tree_clean"] is True
    assert result["ocpm_engine_source_tree_clean"] is True
    assert result["pg_ocpm_source_tree_clean"] is True
    assert result["client_image_id"] == "sha256:" + "5" * 64
    assert validate_recorded_public_provenance(result, allow_dirty=False) == result


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("OCPM_CONTROLLER_SOURCE_REVISION", "main"),
        ("OCPM_ENGINE_SOURCE_REVISION", "main"),
        ("OCPM_CLIENT_IMAGE_ID", "latest"),
        ("OCPM_PG_OCPM_SOURCE_TREE_CLEAN", "maybe"),
    ],
)
def test_public_benchmark_provenance_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    valid_environment(monkeypatch)
    monkeypatch.setenv(name, value)
    with pytest.raises(RuntimeError):
        public_benchmark_provenance()


@pytest.mark.parametrize(
    "field",
    [
        "controller_source_tree_clean",
        "ocpm_engine_source_tree_clean",
        "pg_ocpm_source_tree_clean",
    ],
)
def test_release_provenance_rejects_dirty_tree(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    valid_environment(monkeypatch)
    result = public_benchmark_provenance()
    result[field] = False
    assert validate_recorded_public_provenance(result, allow_dirty=True) == result
    with pytest.raises(ValueError, match="clean source trees"):
        validate_recorded_public_provenance(result, allow_dirty=False)
