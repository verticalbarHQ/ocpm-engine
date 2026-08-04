#!/usr/bin/env python3
"""Reject benchmark-reference dependencies from production ocpm-engine code."""

from __future__ import annotations

import ast
import pathlib
import re
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]
FORBIDDEN = {
    "ocedeclare",
    "ocedeclare_shared",
    "ocpa",
    "ocpq",
    "pm4py",
    "process_mining",
    "rust4pm",
}
DEPENDENCY_TABLES = {
    "build_dependencies",
    "dependencies",
    "dev_dependencies",
}
FORBIDDEN_REPOSITORIES = (
    "aarkue/ocpq",
    "aarkue/rust4pm",
    "ocpm/ocpa",
    "process-intelligence-solutions/pm4py",
)


def normalized(value: str) -> str:
    return re.sub(r"[-_.]+", "_", value).lower()


def dependency_tables(value: object, path: tuple[str, ...] = ()):
    if not isinstance(value, dict):
        return
    for key, child in value.items():
        child_path = (*path, key)
        if normalized(key) in DEPENDENCY_TABLES and isinstance(child, dict):
            yield child_path, child
        yield from dependency_tables(child, child_path)


def dependency_name(requirement: str) -> str | None:
    match = re.match(r"\s*([A-Za-z0-9_.-]+)", requirement)
    return normalized(match.group(1)) if match else None


def audit_cargo_manifest(path: pathlib.Path, failures: list[str]) -> None:
    raw = path.read_text()
    document = tomllib.loads(raw)
    for table_path, dependencies in dependency_tables(document):
        for name, specification in dependencies.items():
            if normalized(name) in FORBIDDEN:
                failures.append(
                    f"{path.relative_to(ROOT)}:{'.'.join(table_path)} "
                    f"declares forbidden dependency {name!r}"
                )
            rendered = str(specification).lower()
            for repository in FORBIDDEN_REPOSITORIES:
                if repository in rendered:
                    failures.append(
                        f"{path.relative_to(ROOT)}:{'.'.join(table_path)} "
                        f"references forbidden repository {repository!r}"
                    )


def audit_python_manifest(failures: list[str]) -> None:
    path = ROOT / "pyproject.toml"
    document = tomllib.loads(path.read_text())
    project = document.get("project", {})
    groups = {"dependencies": project.get("dependencies", [])}
    groups.update(project.get("optional-dependencies", {}))
    for group, requirements in groups.items():
        for requirement in requirements:
            name = dependency_name(requirement)
            if name in FORBIDDEN:
                failures.append(
                    f"pyproject.toml:{group} declares forbidden dependency "
                    f"{requirement!r}"
                )


def audit_lockfiles(failures: list[str]) -> None:
    for filename in ("Cargo.lock", "uv.lock"):
        path = ROOT / filename
        document = tomllib.loads(path.read_text())
        for package in document.get("package", []):
            name = normalized(str(package.get("name", "")))
            if name in FORBIDDEN:
                failures.append(f"{filename} resolves forbidden package {name!r}")
            source = str(package.get("source", "")).lower()
            for repository in FORBIDDEN_REPOSITORIES:
                if repository in source:
                    failures.append(
                        f"{filename} resolves forbidden repository {repository!r}"
                    )


def imported_modules(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module.split(".", 1)[0])
    return {normalized(module) for module in modules}


def audit_production_sources(failures: list[str]) -> None:
    for path in (ROOT / "src/ocpm_engine").rglob("*.py"):
        forbidden = imported_modules(path) & FORBIDDEN
        for module in sorted(forbidden):
            failures.append(
                f"{path.relative_to(ROOT)} imports forbidden module {module!r}"
            )

    import_pattern = re.compile(
        r"(?m)^\s*(?:pub\s+)?(?:use|extern\s+crate)\s+"
        r"(ocedeclare(?:_shared)?|ocpa|ocpq|pm4py|process_mining|rust4pm)\b"
    )
    for path in (ROOT / "crates").glob("*/src/**/*.rs"):
        for match in import_pattern.finditer(path.read_text()):
            failures.append(
                f"{path.relative_to(ROOT)} imports forbidden crate {match.group(1)!r}"
            )


def audit_vendoring(failures: list[str]) -> None:
    for path in (ROOT / ".gitmodules", ROOT / "vendor", ROOT / "third_party"):
        if path.exists():
            failures.append(
                f"{path.relative_to(ROOT)} violates the no-vendored-reference boundary"
            )

    for path in (ROOT / "benchmarks").rglob("*"):
        if "ocpq" in path.name.lower():
            failures.append(
                f"{path.relative_to(ROOT)} violates the archival-results-only "
                "OCPQ boundary"
            )


def main() -> None:
    failures: list[str] = []
    audit_python_manifest(failures)
    for path in [ROOT / "Cargo.toml", *sorted((ROOT / "crates").glob("*/Cargo.toml"))]:
        audit_cargo_manifest(path, failures)
    audit_lockfiles(failures)
    audit_production_sources(failures)
    audit_vendoring(failures)
    if failures:
        raise SystemExit("core dependency boundary failed:\n- " + "\n- ".join(failures))
    print(
        "core dependency boundary passed: comparison packages are benchmark-only; "
        "OCPQ is archival-result-only"
    )


if __name__ == "__main__":
    main()
