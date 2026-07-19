"""Strict, shared provenance capture for the public Docker benchmarks."""

from __future__ import annotations

import os
import re

_REVISION = re.compile(r"[0-9a-f]{40}")
_SHA256_ID = re.compile(r"sha256:[0-9a-f]{64}")
_FIELDS = {
    "benchmark_host_id",
    "ocpm_engine_source_revision",
    "ocpm_engine_source_tree_clean",
    "pg_ocpm_source_revision",
    "pg_ocpm_source_tree_clean",
    "client_image_id",
    "vanilla_database_image_id",
    "pg_ocpm_database_image_id",
}


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for benchmark provenance")
    return value


def _boolean(name: str) -> bool:
    value = _required(name).lower()
    if value in {"1", "true"}:
        return True
    if value in {"0", "false"}:
        return False
    raise RuntimeError(f"{name} must be true, false, 1, or 0")


def _revision(name: str) -> str:
    value = _required(name)
    if _REVISION.fullmatch(value) is None:
        raise RuntimeError(f"{name} must be an exact lowercase Git revision")
    return value


def _image_id(name: str) -> str:
    value = _required(name)
    if _SHA256_ID.fullmatch(value) is None:
        raise RuntimeError(f"{name} must be an immutable Docker image ID")
    return value


def public_benchmark_provenance() -> dict[str, object]:
    """Read the host, source, and built-image identity supplied by the runner."""

    return {
        "benchmark_host_id": _image_id("OCPM_BENCHMARK_HOST_ID"),
        "ocpm_engine_source_revision": _revision("OCPM_ENGINE_SOURCE_REVISION"),
        "ocpm_engine_source_tree_clean": _boolean("OCPM_ENGINE_SOURCE_TREE_CLEAN"),
        "pg_ocpm_source_revision": _revision("OCPM_PG_OCPM_SOURCE_REVISION"),
        "pg_ocpm_source_tree_clean": _boolean("OCPM_PG_OCPM_SOURCE_TREE_CLEAN"),
        "client_image_id": _image_id("OCPM_CLIENT_IMAGE_ID"),
        "vanilla_database_image_id": _image_id("OCPM_VANILLA_DATABASE_IMAGE_ID"),
        "pg_ocpm_database_image_id": _image_id("OCPM_PG_OCPM_DATABASE_IMAGE_ID"),
    }


def validate_recorded_public_provenance(
    value: object, *, allow_dirty: bool
) -> dict[str, object]:
    """Validate an artifact's recorded provenance independently of the runner."""

    if not isinstance(value, dict) or set(value) != _FIELDS:
        raise ValueError("public benchmark provenance fields changed")
    for field in ("ocpm_engine_source_revision", "pg_ocpm_source_revision"):
        revision = value.get(field)
        if not isinstance(revision, str) or _REVISION.fullmatch(revision) is None:
            raise ValueError(f"invalid provenance revision: {field}")
    for field in (
        "benchmark_host_id",
        "client_image_id",
        "vanilla_database_image_id",
        "pg_ocpm_database_image_id",
    ):
        image_id = value.get(field)
        if not isinstance(image_id, str) or _SHA256_ID.fullmatch(image_id) is None:
            raise ValueError(f"invalid provenance image ID: {field}")
    for field in (
        "ocpm_engine_source_tree_clean",
        "pg_ocpm_source_tree_clean",
    ):
        clean = value.get(field)
        if type(clean) is not bool:
            raise ValueError(f"invalid provenance cleanliness flag: {field}")
        if not allow_dirty and clean is not True:
            raise ValueError("release artifacts require clean source trees")
    return value
