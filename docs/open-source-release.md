# First open-source release strategy

The working tree is licensed under Apache-2.0. The current private repository
history is not the public release artifact.

## Required release order

1. Complete the patent filing decision, inventorship audit, and chain-of-title
   review before any new public disclosure.
2. Obtain counsel approval of the Apache-2.0 application, NOTICE, CLA, and
   third-party notices.
3. Review the complete source tree for credentials, customer information,
   private legal material, removed benchmark harnesses, generated artifacts,
   and incompatible third-party material.
4. Create a new public repository with a clean root commit from the approved
   source snapshot. Do not change the current private repository visibility in
   place and do not copy its Git history.
5. Run dependency-boundary, licensing, build, test, upgrade, security,
   package-content, SBOM, and provenance gates on that exact root commit.
6. Sign and publish a source archive and supported binary packages carrying
   LICENSE, NOTICE, copyright, and third-party notices.

## Dual-licensing boundary

The Apache-2.0 grant for a published core version is irrevocable subject to its
terms. A private registry can control the supported delivery channel, update
entitlement, and commercial services, but it cannot retract recipients'
Apache-2.0 rights in that core.

Vertical Bar, Inc. may offer the same company-owned code and CLA-covered
contributions under proprietary terms. Separately developed proprietary
connectors, hosted operations, policy, support, or enterprise modules should
remain in distinct packages and repositories with explicit interfaces and
license metadata.

## Contribution launch

Before accepting an outside pull request, configure a private CLA register,
conduct-reporting route, security-reporting route, branch protection, required
CI, and a documented maintainer decision. Signed CLAs and personal addresses
must never be stored in the public repository.
