# pg_ocpm and ocpm-engine: open-source, patent, and ICPM assessment

Assessment date: 2026-08-02.

This is a technical and commercial assessment, not a legal opinion, patentability
opinion, freedom-to-operate opinion, or valuation. Patent counsel should review
the claims, ownership, inventorship, and disclosure history before publication.

## Decision

| Question | Assessment | Decision |
|---|---|---|
| Do the projects add community value? | Yes. A source-neutral PostgreSQL serving layer for exact object-centric filtering and compact analytics, paired with a small Rust model layer, fills a real infrastructure gap. | Continue both projects. |
| Are they ready to call open source today? | No. Both repositories expressly reserve all rights and contain no `LICENSE` file. | File or decline patent protection first, then add a license and release process. |
| Should the company pursue a patent? | A broad claim to object-centric storage, process graphs, Rust, SQL pushdown, compression, or pre-aggregation is unlikely to be defensible as novel. A narrow co-designed execution claim may be worth preserving. | Commission a claims-chart search and file a detailed provisional only if counsel finds a claim covering the combined capsule and multi-window execution method. Do not fund broad international prosecution based only on the current benchmark. |
| Is an ICPM 2027 paper justified? | Yes, if it is an execution-model and systems paper with causal ablations, exact semantics, scale, cold-cache, concurrent ingest, and independent reproduction. Benchmark headlines alone are insufficient. | Target the research track and a demonstrator, but freeze claims and evaluation before drafting. |

## Community value

The strongest contribution is not a new DFG, variant, conformance, prediction,
or bottleneck algorithm. Those are established process-mining operations. The
useful contribution is an execution boundary:

1. `pg_ocpm` converts normalized OCEL facts into exact, versioned event, case,
   edge, and adjacency capsules behind ordinary PostgreSQL relations and
   transactions.
2. PostgreSQL applies tenant, object, time, activity, attribute, duration, and
   relationship predicates before transferring data.
3. Compact sufficient statistics or factorized lifecycle batches cross the
   database boundary.
4. `ocpm-engine` performs deterministic model construction and scoring in Rust,
   releases the Python GIL, and adds no database-side model state.

This matters to the community because it offers a deployment path between
row-oriented SQL and full event-log hydration. It complements PM4Py, OCPA,
Rust4PM, and OCPQ rather than replacing their full algorithm catalog. It also
keeps PostgreSQL backup, replication, MVCC, tenancy, and SQL composition in the
architecture instead of introducing another proprietary event store.

The projects will be materially more useful after they add source-neutral OCEL
2.0 import examples, stable schema/API compatibility guarantees, packaged
PostgreSQL binaries, a minimal end-to-end tutorial, and at least one integration
that calls an existing process-mining library.

## Preliminary novelty boundary

The surrounding prior art is substantial:

- [OCPQ](https://arxiv.org/abs/2506.11541) already establishes expressive
  object-centric querying with a specialized high-performance Rust backend,
  compact bindings, early filtering, and database comparisons.
- [SQL Queries for Declarative Process Mining on Event Logs of Relational
  Databases](https://arxiv.org/abs/1512.00196) establishes direct process mining
  over relational event data using SQL.
- [Factorized Databases](https://arxiv.org/abs/1104.0867) establishes the broad
  principle of retaining repeated relational results in factorized form.
- [Rust4PM](https://arxiv.org/abs/2401.14149) establishes a high-performance Rust
  process-mining core with Python and Java bindings.
- US 12,265,558 claims object networks, ordered traces, transition derivation,
  and process-graph construction for object-centric process mining.
- EP 4,369,179 claims an object-centric process-mining data model with object
  and event types, object-to-object and event-to-object relationships, generated
  extraction commands, and relational storage.
- PostgreSQL arrays, TOAST, native aggregates, pre-aggregation, MVCC, WAL,
  cursor-based retrieval, pruning indexes, and materialized serving structures
  are established database techniques.

Consequently, the following are not credible standalone novelty claims:

- storing OCEL data in PostgreSQL;
- deriving directly-follows edges or process graphs;
- using Rust for process mining or Python bindings;
- pushing filters or aggregates into a database;
- compressing identifiers or timestamps into arrays; or
- precomputing case, edge, variant, or duration summaries.

The candidate invention is narrower and must be tested as a combination:

> An exact, source-neutral OCEL serving method that finalizes relational event,
> object, edge, and adjacency facts into versioned, duplicate-preserving
> compressed lifecycle capsules inside an MVCC database; evaluates multiple
> aligned lifecycle-containment windows in a single bounded native aggregate;
> returns either sufficient statistics or factorized batches without event-row
> materialization; and delegates deterministic model construction to an
> independently scalable engine through an observable capability contract.

Even that combination is only a hypothesis. The preliminary search did not
establish novelty or non-obviousness, and it did not cover every jurisdiction,
non-patent publication, unpublished application, continuation, or claim
construction.

## Patent recommendation

Preserve the option, but use a strict go/no-go gate.

File a provisional before an open-source release, paper, public benchmark,
conference talk, customer disclosure without an NDA, or offer for sale if all
of these conditions hold:

- counsel identifies at least one claim materially narrower than the prior art
  above but broad enough to cover an independent implementation;
- the claimed behavior is externally detectable from SQL/API behavior, storage
  layout, binaries, or reproducible performance, rather than being an invisible
  implementation detail;
- the company expects licensing, enterprise diligence, fundraising, or an
  acquisition path where the asset will matter; and
- the founders will fund prosecution only after customer and ecosystem evidence
  confirms commercial relevance.

The provisional should contain the current and alternative capsule layouts,
finalization and invalidation flows, exact window semantics, duplicate and tie
handling, capability negotiation, single- and multi-window algorithms,
database/engine boundary, failure behavior, concurrency model, diagrams, and
benchmark evidence. A thin marketing description will not preserve useful
support for later claims.

Do not proceed beyond the provisional if the best claim reduces to “use a
PostgreSQL extension for faster process mining,” if infringement cannot be
detected, or if the company intends to grant every user an unrestricted patent
license with no proprietary commercial layer. In that case, a defensive
publication may create more ecosystem value at lower cost.

[USPTO guidance](https://www.uspto.gov/patents/basics/apply/provisional-application)
states that a provisional must provide a sufficient written description and
must be followed by a corresponding nonprovisional within 12 months to retain
its benefit. The USPTO also warns that an inventor's pre-filing public
disclosure may preserve only a limited US grace period and may preclude foreign
rights. [WIPO guidance](https://www.wipo.int/en/web/patents/protection) likewise
warns that pre-filing public disclosure can destroy novelty. The release order
therefore matters.

## Likely company value

A well-scoped patent can add option value, defensive leverage, and a concrete
technical asset in diligence. It does not by itself establish product value.
The larger company-value drivers will be adoption, production reliability,
managed-service revenue, integrations, and evidence that the architecture
reduces customers' infrastructure cost or query latency.

The best commercial structure is likely:

- open-source `pg_ocpm` and `ocpm-engine` core under Apache-2.0, which includes
  an explicit contributor patent grant and patent-retaliation provision;
- proprietary or separately licensed managed operations, enterprise connectors,
  governance, observability, workload management, and support; and
- a patent used defensively and against non-compliant proprietary copying, not
  as a threat to compliant community users.

Before choosing Apache-2.0, counsel must reconcile the intended patent strategy
with the license's patent grant. If dual licensing is planned, contributor
ownership and inbound rights must support it.

## Open-source release gates

The code is promising but is not open source merely because it is hosted in a
Git repository. Before a public release:

1. Complete the patent go/no-go and first filing.
2. Confirm company ownership, all inventors, employee/contractor assignments,
   generated-code provenance, and third-party dataset/software rights.
3. Add `LICENSE`, `NOTICE`, consistent Vertical Bar copyright notices, and
   SPDX identifiers. Resolve the current `ocpm-engine contributors` versus
   Vertical Bar ownership wording.
4. Adopt a DCO or CLA aligned with any dual-license plan.
5. Publish a private security address, supported versions, response targets,
   and tenant-isolation threat model.
6. Produce signed source and binary releases, checksums, SBOMs, upgrade tests,
   and PostgreSQL 13-18 packages.
7. Make a one-command OCEL 2.0 tutorial and a compatibility matrix for
   `pg_ocpm` and `ocpm-engine` versions.
8. Keep GPL/AGPL comparison tools in disposable benchmark images and out of the
   distributed permissive runtime.

## ICPM 2027 evidence plan

The official conference site lists ICPM 2027 in Rende, Italy, on February
8-12, 2027, but no research-paper deadline was confirmed in this review. The
schedule is therefore feasible only with an immediate research freeze.

The paper should make one bounded claim: the co-designed database/engine
execution boundary reduces tuple materialization, network transfer, and model
memory while retaining exact object-centric semantics and ordinary PostgreSQL
operations.

Required evidence:

- causal ablations from row-wise OCEL through persisted edges, capsules, native
  scans, multi-window aggregation, and Rust scoring;
- identical-answer tests for every timed request;
- upstream-native Rust4PM and OCPA datasets as separate ecosystem pairs, plus
  OCPQ and SAP O2C/P2P where semantics are compatible;
- selective and unselective dynamic filters, overlapping windows, and unseen
  query combinations;
- 1x to at least 100x scaling, warm and cold cache, memory-constrained runs,
  and ARM64/x86-64 reproduction;
- 1, 4, 8, 16, and 32 or more clients through a realistic connection pool;
- concurrent ingest/query, finalization cost, WAL, refresh, crash recovery,
  backup/restore, and upgrade behavior;
- total, heap, index, and TOAST storage, load time, peak RSS/PSS, CPU time,
  bytes read/transferred, p50/p95, and confidence intervals; and
- an independent reproduction from a clean commit and immutable containers.

The current fixed-workload benchmarks are useful validation, not yet sufficient
paper evidence. In particular, the OCPA pair is adapter-assisted because OCPA
1.3.4's documented SQLite importer fails on its own checked-in running example;
that limitation must remain prominent. Rust4PM's published evaluation measures
XES import and Alpha+++ on case-centric logs, so the separate OCEL 2.0 pair is a
new shared-workload comparison rather than a reproduction of the paper's
algorithm benchmark.

## Recommended order

1. Freeze external disclosure and inventory any disclosures already made.
2. Give patent counsel this claim chart, the source, architecture diagrams, and
   benchmark artifacts; decide provisional versus defensive publication.
3. File the provisional if it passes the gate.
4. Choose the license and contribution model.
5. Publish an alpha release with the ecosystem benchmark and known limitations.
6. Run the research ablations and independent reproduction.
7. Draft the ICPM paper only after the causal results are stable.
