#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "$0")/.." && pwd)"
python_bin="${PYTHON:-python3}"
dist_dir="${DIST_DIR:-$repo_root/dist}"

"$python_bin" -c 'import sys; assert sys.version_info >= (3, 11), sys.version'
"$python_bin" -c \
    'import importlib.metadata; assert importlib.metadata.version("maturin") == "1.14.1"' \
    >/dev/null 2>&1 || {
    echo "maturin 1.14.1 is required" >&2
    exit 2
}
test -n "${DUCKDB_INCLUDE_DIR:-}" && test -f "$DUCKDB_INCLUDE_DIR/duckdb.h" || {
    echo "DUCKDB_INCLUDE_DIR must contain the deployment-supplied DuckDB 1.5.5 header" >&2
    exit 2
}
test -n "${DUCKDB_LIB_DIR:-}" || {
    echo "DUCKDB_LIB_DIR must identify the deployment-supplied DuckDB 1.5.5 library" >&2
    exit 2
}
find "$DUCKDB_LIB_DIR" -maxdepth 1 -type f \
    \( -name 'libduckdb.so' -o -name 'libduckdb.dylib' -o -name 'duckdb.dll' \) \
    | grep -q . || { echo "DUCKDB_LIB_DIR contains no shared DuckDB client" >&2; exit 2; }

build_dir="$(mktemp -d "${TMPDIR:-/tmp}/ocpm-private-wheel.XXXXXX")"
cleanup() {
    find "$build_dir" -mindepth 1 -delete
    rmdir "$build_dir"
}
trap cleanup EXIT
mkdir -p "$dist_dir"

export LIBRARY_PATH="$DUCKDB_LIB_DIR${LIBRARY_PATH:+:$LIBRARY_PATH}"
export LD_LIBRARY_PATH="$DUCKDB_LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export DYLD_LIBRARY_PATH="$DUCKDB_LIB_DIR${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"

"$python_bin" -m maturin build \
    --manifest-path "$repo_root/crates/ocpm-python/Cargo.toml" \
    --interpreter "$python_bin" \
    --release \
    --locked \
    --strip \
    --auditwheel skip \
    --out "$build_dir"

shopt -s nullglob
wheels=("$build_dir"/*.whl)
test "${#wheels[@]}" -eq 1 || { echo "expected exactly one wheel" >&2; exit 1; }
wheel="${wheels[0]}"
manifest="$build_dir/$(basename "${wheel%.whl}").manifest.json"
"$python_bin" "$repo_root/scripts/verify-wheel.py" "$wheel" --manifest "$manifest"
cp "$wheel" "$manifest" "$dist_dir/"
printf '%s\n' "$dist_dir/$(basename "$wheel")"
