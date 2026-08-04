from __future__ import annotations

import hashlib
import importlib.util
import json
import pathlib
import zipfile

import pytest

ROOT = pathlib.Path(__file__).parents[1]


def load_script(name: str):
    path = ROOT / "scripts" / name
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


publish = load_script("publish-private-wheel.py")
verify = load_script("verify-wheel.py")


@pytest.mark.parametrize(
    "url",
    [
        "https://upload.pypi.org/legacy/",
        "https://test.pypi.org/legacy/",
        "http://packages.example.test/simple/",
    ],
)
def test_public_or_insecure_registries_are_rejected(url: str) -> None:
    with pytest.raises(ValueError):
        publish.validate_private_repository_url(url)


def test_private_registry_is_accepted() -> None:
    assert (
        publish.validate_private_repository_url(
            "https://example-123.d.codeartifact.us-west-2.amazonaws.com/pypi/ocpm"
        )
        == "https://example-123.d.codeartifact.us-west-2.amazonaws.com/pypi/ocpm/"
    )


def test_source_archives_are_rejected(tmp_path: pathlib.Path) -> None:
    (tmp_path / "ocpm-engine-1.1.0.tar.gz").write_bytes(b"source")
    with pytest.raises(ValueError, match="source archives"):
        publish.discover_wheels(tmp_path)


def test_embedded_duckdb_is_rejected(tmp_path: pathlib.Path) -> None:
    wheel = tmp_path / "ocpm_engine-1.1.0-cp311-abi3-linux_x86_64.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("ocpm_engine/_native.abi3.so", b"native")
        archive.writestr("ocpm_engine/libduckdb.so", b"duckdb")
    with pytest.raises(verify.WheelVerificationError, match="bundles DuckDB"):
        verify.verify_wheel(wheel, require_external_duckdb=False)


def test_uninspectable_native_dependency_is_rejected(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wheel = tmp_path / "ocpm_engine-1.1.0-cp311-abi3-linux_x86_64.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("ocpm_engine/_native.abi3.so", b"native")
        archive.writestr(
            "ocpm_engine-1.1.0.dist-info/METADATA",
            "Name: ocpm-engine\nLicense-Expression: Apache-2.0\n",
        )
        archive.writestr(
            "ocpm_engine-1.1.0.dist-info/WHEEL", "Root-Is-Purelib: false\n"
        )
        for filename in (
            "LICENSE",
            "NOTICE",
            "COPYRIGHT.md",
            "THIRD_PARTY_NOTICES.md",
            "THIRD_PARTY_LICENSES.html",
        ):
            archive.writestr(
                f"ocpm_engine-1.1.0.dist-info/licenses/{filename}", "notice\n"
            )
    monkeypatch.setattr(verify, "_native_dependencies", lambda _: "")
    with pytest.raises(verify.WheelVerificationError, match="could not inspect"):
        verify.verify_wheel(wheel)


def test_platform_manifest_is_bound_to_wheel_hash(tmp_path: pathlib.Path) -> None:
    wheel = tmp_path / "ocpm_engine-1.1.0-cp311-abi3-linux_x86_64.whl"
    wheel.write_bytes(b"wheel")
    manifest = {
        "artifact": wheel.name,
        "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "external_duckdb_dependency_verified": True,
        "prohibited_source_files": [],
        "bundled_duckdb_files": [],
    }
    wheel.with_name(f"{wheel.stem}.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    assert publish.validate_build_manifest(wheel).is_file()

    wheel.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="manifest mismatch"):
        publish.validate_build_manifest(wheel)
