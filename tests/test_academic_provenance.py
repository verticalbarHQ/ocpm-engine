from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ALGORITHM_MODULES = {
    "crates/ocpm-provider/src/lib.rs": "10.1007/978-3-030-85082-1_16",
    "crates/ocpm-local/src/lib.rs": "10.1109/ICPM57379.2022.9980730",
    "crates/ocpm-query/src/lib.rs": "10.1007/978-3-031-92474-3_23",
    "crates/ocpm-discovery/src/lib.rs": "10.1007/s10009-022-00668-w",
    "crates/ocpm-conformance/src/lib.rs": "10.1109/EDOC.2011.12",
    "crates/ocpm-enhancement/src/lib.rs": "10.1007/s10009-022-00668-w",
    "crates/ocpm-prediction/src/lib.rs": "10.1145/3331449",
    "crates/ocpm-io/src/lib.rs": "10.1007/978-3-030-85082-1_16",
}

ADDITIONAL_REQUIRED_DOIS = {
    "crates/ocpm-enhancement/src/lib.rs": (
        "10.1016/j.is.2025.102584",
        "10.1109/18.61115",
    ),
    "crates/ocpm-io/src/lib.rs": ("10.1007/978-3-642-17722-4_5",),
}


def test_every_algorithm_module_records_peer_reviewed_provenance() -> None:
    for relative_path, doi in ALGORITHM_MODULES.items():
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "PROVENANCE" in source, relative_path
        assert doi in source, relative_path
    for relative_path, dois in ADDITIONAL_REQUIRED_DOIS.items():
        source = (ROOT / relative_path).read_text(encoding="utf-8")
        for doi in dois:
            assert doi in source, relative_path


def test_algorithm_modules_do_not_reference_comparison_libraries() -> None:
    forbidden = ("ocpq", "rust4pm", "ocpa", "pm4py", "prom")
    for relative_path in ALGORITHM_MODULES:
        source = (ROOT / relative_path).read_text(encoding="utf-8").lower()
        for name in forbidden:
            assert name not in source, f"{relative_path} references {name}"
