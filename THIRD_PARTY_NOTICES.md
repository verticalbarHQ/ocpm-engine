# Third-party software

`ocpm-engine` itself is licensed under Apache-2.0. The machine-generated
[`THIRD_PARTY_LICENSES.html`](THIRD_PARTY_LICENSES.html) accompanies binary
distributions and carries the available copyright and license texts for the
locked runtime Rust dependency graph. Regenerate and review it whenever
`Cargo.lock` changes. Use `cargo-about 0.8.4` and
`scripts/generate-third-party-licenses.py`; the licensing check binds the
generated bundle to the lockfile, configuration, and template.

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
`psycopg2-binary`, `psutil`, and PM4Py 2.7.23.3 from a separate, hash-locked
benchmark requirements file into its disposable client image; none is declared
by or bundled in the library package. The installed PM4Py distribution reports
AGPL-3.0 licensing and is used only as an external comparison implementation.

The optional DuckDB provider links dynamically to a deployment-supplied DuckDB
client. DuckDB is MIT-licensed and is not embedded in the wheel. The SQLite
interchange provider uses the `rusqlite` bundled build; SQLite is dedicated to
the public domain. These upstream terms are not replaced by this project's
Apache-2.0 license.

The public benchmark data is Alessandro Berti's *Collection of Object-Centric
Event Logs*, DOI [10.5281/zenodo.8261133](https://doi.org/10.5281/zenodo.8261133),
licensed CC BY 4.0. The benchmark records the downloaded archive and extracted
SQLite SHA-256 digests and preserves source attribution in its result.

PM4Py source is not copied into this repository and PM4Py is not a library
runtime dependency. The ocpm-engine kernels remain independently implemented;
the disposable benchmark imports PM4Py to establish output equivalence and
end-to-end timing for the comparison paths.

## Archived OCPQ result

The documentation retains previously published OCPQ comparison results as
historical evidence. This repository supplies no OCPQ software, binary, input
data, benchmark adapter, runner, checker, image builder, dataset loader, or
reproduction instructions. OCPQ is not a runtime, build, development, or
benchmark dependency of the repository or library.

## Ecosystem benchmark-only references

The optional ecosystem benchmark installs the exact published Rust4PM
`process_mining` crate version 0.6.0 from the Rust package registry and compiles
only this repository's black-box adapter in a disposable container. Commit
`b4c06f323fca55cf57eaf44ac25b46ea7c448cb4` is retained solely as upstream
release provenance.
Rust4PM declares Apache-2.0 OR MIT licensing. Its selected OCEL 2.0 P2P input is
the upstream test-data record DOI
[10.5281/zenodo.8412920](https://doi.org/10.5281/zenodo.8412920), licensed CC BY
4.0. Neither Rust4PM source nor its benchmark binary is bundled with the
ocpm-engine wheel.

The optional OCPA arm installs the checksum-verified OCPA 1.3.4 wheel in a
disposable container. Although its package metadata reports `MIT`, the wheel's
included `LICENSE.txt` is GPL-3.0; this project therefore treats OCPA 1.3.4 as
GPL-3.0. OCPA is not linked into or bundled with the ocpm-engine wheel. The
benchmark uses the exact `running-example.sqlite` named in OCPA's OCEL 2.0
documentation at commit `de056e0203a3fa4a9bbc19a95e001eada323074a`. No
dataset-specific license statement was found, so the benchmark downloads the
file into the ignored local artifact directory and does not redistribute it.
