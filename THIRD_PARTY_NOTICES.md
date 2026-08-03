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

## OCPQ benchmark-only reference

The optional OCPQ comparison checks out the following exact upstream revisions:

- [aarkue/OCPQ](https://github.com/aarkue/OCPQ/tree/80457e561edd7bb9e142d959dd7e0f96e6b03f2f)
  at commit `80457e561edd7bb9e142d959dd7e0f96e6b03f2f`, corresponding to
  the evaluated 0.6.7 release; and
- [aarkue/ocpq-eval](https://github.com/aarkue/ocpq-eval/tree/846dd4eb9f8600ae42355968453a9412ea4759c2)
  at commit `846dd4eb9f8600ae42355968453a9412ea4759c2` for the Q1-Q7
  definitions, source measurements, and evaluation input.

The comparison compiles the pinned OCPQ backend into a disposable local
reference container. That container and the OCPQ source or binaries are not
distributed with the ocpm-engine library or wheel. They are not runtime or
build dependencies of the shipped library.

At those pinned commits, neither upstream repository contains a `LICENSE`,
`COPYING`, or `NOTICE` file. The OCPQ Rust package manifests do not declare a
software license (one Tauri manifest has an empty license field), and the
ocpq-eval README does not state a repository license. This repository therefore
does not infer a license grant from public GitHub availability or assign a
license to either upstream repository. Anyone reproducing the optional
reference build must independently confirm that their use is permitted. The
associated paper is Aaron Küsters and Wil M. P. van der Aalst,
[*OCPQ: Object-Centric Process Querying & Constraints*](https://doi.org/10.48550/arXiv.2506.11541).

The ocpq-eval README says that its evaluation OCEL is based on three public 4TU
records:

- Boudewijn van Dongen, *BPI Challenge 2017*, Version 1,
  [DOI 10.4121/uuid:5f3067df-f10b-45da-b98b-86ae4c7a310b](https://data.4tu.nl/datasets/34c3f44b-3101-4ea9-8281-e38905c68b8d/1),
  whose authoritative record points to the 4TU legacy terms of use rather than
  a Creative Commons license;
- Dirk Fahland and Stefan Esser, *Event Graph of BPI Challenge 2017*, Version 1,
  [DOI 10.4121/14169584.v1](https://data.4tu.nl/datasets/5c9717a0-4c22-4b78-a3ad-d2234208bfd7/1),
  licensed CC BY 4.0; and
- Shahrzad Khayatbashi, Olaf Hartig, and Amin Jalali,
  *BPI Challenge 2017 (OCEL)*, Version 1,
  [DOI 10.4121/6889ca3f-97cf-459a-b630-3b0b0d8664b5.v1](https://data.4tu.nl/datasets/6889ca3f-97cf-459a-b630-3b0b0d8664b5/1),
  licensed CC BY 4.0.

Those terms apply to the identified 4TU deposits; this notice does not assume
that any one of them licenses the distinct `bpic2017.sqlite.zip` committed to
ocpq-eval. Its pinned Git LFS object is SHA-256
`a5f422c72b0a911bd64383079f9faebfc247e3e5a217f30705ff9969e8547f2b`
(193,471,345 bytes), but that exact derived archive has no separate license
declaration or provenance manifest at the pinned revision. The benchmark
obtains it in the ignored local benchmark workspace and does not redistribute
it. This repository makes no license claim for that archive.

This notice summarizes the resolved release graph and optional benchmark
inputs. License texts and metadata supplied by each upstream source remain
authoritative; where an upstream source supplies none, this notice does not
create or imply a license.

## Ecosystem benchmark-only references

The optional ecosystem benchmark compiles Rust4PM 0.6.0 at commit
`b4c06f323fca55cf57eaf44ac25b46ea7c448cb4` in a disposable container.
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
