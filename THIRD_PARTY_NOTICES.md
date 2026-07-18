# Third-party software

The shipped Rust dependency graph is locked by `Cargo.lock`. The release uses
crates under permissive SPDX choices including MIT, Apache-2.0, Unicode-3.0,
Unlicense, Zlib, and BSL-1.0. `cargo deny check` validates the
resolved graph against `deny.toml`; dependencies with an alternative license
expression are accepted only through an allowed permissive choice.

The policy reports the two target-specific `wasi` ABI crates as a duplicate
warning through Tokio and `whoami`; both are locked transitive platform
dependencies, not two runtime implementations selected on the release target.

The Python wheel has no Python runtime dependency. PyO3, Serde, thiserror,
Tokio, and rust-postgres are used through their MIT or Apache-2.0 options.
Maturin is an MIT/Apache-2.0 build tool. The optional public benchmark installs
`psycopg2-binary`, `psutil`, and PM4Py 2.7.23.3 in its disposable client image;
none is bundled in the library wheel. The installed PM4Py distribution reports
AGPL-3.0 licensing and is used only as an external comparison implementation.

The public benchmark data is Alessandro Berti's *Collection of Object-Centric
Event Logs*, DOI [10.5281/zenodo.8261133](https://doi.org/10.5281/zenodo.8261133),
licensed CC BY 4.0. The benchmark records the downloaded archive and extracted
SQLite SHA-256 digests and preserves source attribution in its result.

PM4Py source is not copied into this repository and PM4Py is not a library
runtime dependency. The ocpm-engine kernels remain independently implemented;
the disposable benchmark imports PM4Py to establish output equivalence and
end-to-end timing for the comparison paths.

This notice summarizes the resolved release graph; the license text supplied
by each dependency remains authoritative.
