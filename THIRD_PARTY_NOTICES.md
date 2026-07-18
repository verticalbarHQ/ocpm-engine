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
`psycopg2-binary` in its disposable client image; it is not bundled in the
library wheel.

The public benchmark data is Alessandro Berti's *Collection of Object-Centric
Event Logs*, DOI [10.5281/zenodo.8261133](https://doi.org/10.5281/zenodo.8261133),
licensed CC BY 4.0. The benchmark records the downloaded archive and extracted
SQLite SHA-256 digests and preserves source attribution in its result.

PM4Py is not a source or runtime dependency. The baseline kernels in this
repository are an independent implementation used solely to establish output
equivalence and timing against vanilla PostgreSQL.

This notice summarizes the resolved release graph; the license text supplied
by each dependency remains authoritative.
