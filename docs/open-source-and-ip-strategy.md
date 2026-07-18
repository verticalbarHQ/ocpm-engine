# Open-source and IP strategy

This is an engineering and community strategy, not legal advice. Counsel should
review the code, ownership chain, patent position, contributor terms, and final
license text before either repository is described as open source.

## Current state

Both repositories currently say “all rights reserved” and grant no software
license. Public source availability alone is not an open-source license. This
state preserves options while the release, patent, and contribution decisions
are made, but it does not permit outside reuse and will suppress adoption and
contributions.

The resolved `ocpm-engine` Rust graph uses permissive license choices and is
checked by `cargo-deny`. The wheel has no Python runtime dependency. The public
benchmark uses CC BY 4.0 data with attribution and verified source hashes.
PM4Py is not copied or shipped as a runtime dependency; one disposable public
benchmark image installs its AGPL-licensed distribution for external
comparison. The ocpm-engine kernels are independently implemented. `pg_ocpm`
uses PostgreSQL's public extension API and has no bundled third-party source.

## The three objectives pull in different directions

| Choice | Community reach | Competitor reciprocity | Proprietary embedding | Relicensing control |
|---|---|---|---|---|
| Apache-2.0 | Highest | Low | Easy | Low without a CLA |
| MPL-2.0 | High | Modified files stay open | Easy in a larger work | Medium with a CLA |
| AGPL-3.0 | Medium | Strong, including network use | Requires commercial license | High with a CLA |
| Source-available only | Low | Contract-defined | Contract-defined | High |

[Apache's license guidance](https://www.apache.org/legal/apply-license) explains
how Apache-2.0 notices are applied. Mozilla's
[MPL 2.0 FAQ](https://www.mozilla.org/en-US/MPL/2.0/FAQ/) describes MPL as
file-level copyleft and explains combining MPL files with differently licensed
code. The GNU license page explains that
[AGPL adds a network source-offer requirement](https://www.gnu.org/licenses/).

## Recommended licensing shape

Subject to counsel, the best fit for the stated objectives is:

- **`pg_ocpm`: AGPL-3.0-or-later plus a paid commercial license.** The extension
  is the server-side performance core and the clearest place for strong
  reciprocity. A competitor operating a modified network service would face
  the AGPL obligations or need a commercial agreement.
- **`ocpm-engine`: MPL-2.0 plus a commercial license.** File-level copyleft is
  friendlier to Python, Rust, notebook, and commercial application adoption,
  while modifications to covered library files remain shareable.
- **One contributor agreement covering both repositories.** It should grant
  the project owner enough rights to distribute contributions under the public
  and commercial licenses, make enforcement practical, and include a patent
  grant. Counsel should decide whether that is a copyright license or
  assignment.

This combination maximizes reach at the integration layer while keeping the
database acceleration core reciprocal. It does not prevent clean-room
competition, patent around the project, or guarantee enforceability. Apache-2.0
for both projects remains the better choice if academic adoption and downstream
packaging are valued above reciprocity.

Do not simply add AGPL text without reviewing how the extension, PostgreSQL,
drivers, hosted database access, and commercial distribution are packaged.
Dual licensing also requires the project owner to retain the necessary rights
in every contribution.

## Contribution controls

Use both provenance and rights controls:

1. Require `Signed-off-by` lines under the
   [Developer Certificate of Origin](https://bestpractices.linuxfoundation.org/ip/contribution-mechanisms-dco.html).
   DCO records contributor provenance; it does not itself grant broad
   relicensing rights.
2. Require an individual or corporate contributor agreement before merge.
   The [Apache CLA guidance](https://www.apache.org/licenses/contributor-agreements.html)
   is a useful operational reference, but the project owner's agreement must
   match its dual-license plan.
3. Protect `main`: pull request only, two reviews for native/SQL storage code,
   CODEOWNERS review, signed commits, linear history, passing compatibility,
   license, correctness, and benchmark-result checks. GitHub documents these
   controls in its
   [protected-branch guidance](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches).
4. Reject generated or copied code without source and license provenance.
   Require contributors to identify papers, repositories, model output, and
   patents that materially informed an implementation.
5. Publish `SECURITY.md`, a private vulnerability intake path, release signing,
   an embargo policy, and supported-version windows before general adoption.
6. Keep trademarks separate from the code license. Publish a trademark policy
   for the company and project names.

## Patent and disclosure gate

If patent protection matters, stop before the public release or conference
submission and obtain counsel's filing decision. The
[USPTO provisional-application guidance](https://www.uspto.gov/patents/basics/apply/provisional-application)
notes a US inventor-disclosure grace period but warns that pre-filing public
disclosure may preclude foreign rights. Under
[EPC Article 54](https://www.epo.org/en/legal/epc/2020/a54.html), material made
available to the public before filing is part of the state of the art, subject
to narrow exceptions.

Practical sequence:

1. Create a dated invention disclosure mapping inventors, claims, source
   commits, benchmark evidence, and known prior art.
2. Have counsel perform ownership and patentability review.
3. File before a public Git tag, preprint, demo video, abstract, benchmark blog,
   or detailed architecture talk if protection is desired.
4. Only then apply the selected public licenses and accept outside code.

## Community visibility plan

- Cut signed semantic-version tags and GitHub releases; every version already
  has mandatory dated release notes and a CI release-note gate.
- Archive release artifacts and benchmark results in Zenodo to obtain a citable
  DOI, and maintain `CITATION.cff` with the eventual paper/preprint metadata.
- Publish the Docker-isolated public artifact, exact data hashes, environment,
  output digest, negative results, and ablations. Invite independent reruns on
  x86-64 and cloud PostgreSQL.
- Engage the OCEL/OCPM community with schema and interoperability proposals,
  not performance marketing alone. Track the
  [OCEL 2.0 specification](https://www.ocel-standard.org/specification/overview/).
- Submit a research paper or tool/demo artifact only after the prior-art and
  claim plan is complete. A permissive client SDK can be added later if it
  materially lowers adoption friction.
- Establish public roadmap, good-first issues, contributor office hours, and a
  lightweight maintainer governance document before soliciting contributions.

## Decision checklist

- [ ] Confirm all employee/contractor assignments to the project owner.
- [ ] Decide whether patent filing precedes disclosure.
- [ ] Obtain written license compatibility and dual-license advice.
- [ ] Choose AGPL/MPL or the Apache alternative.
- [ ] Adopt CLA, DCO, governance, security, trademark, and enforcement policies.
- [ ] Add authoritative license files and SPDX package metadata.
- [ ] Treat any public commit, tag, preprint, talk, or benchmark publication as
  a disclosure and have counsel assess the remaining protection options.

Until those boxes are complete, the repositories should continue to state that
no license is granted and should not accept code contributions.
