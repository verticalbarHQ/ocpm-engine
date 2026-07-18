# Prior art and ICPM research roadmap

## Claim discipline

The current evidence supports a high-performance engineering contribution. It
does **not** yet support claims such as “first,” “unique,” “unparalleled,” or
“state of the art.” Novelty is decided against the complete prior art, not by a
benchmark win over one implementation. The paper should separate reusable
architectural contributions from established algorithms and validate each with
an ablation.

## Known prior art

- Rust process-mining cores with Java and Python bindings are established by
  Küsters and van der Aalst,
  [*Developing a High-Performance Process Mining Library with Java and Python
  Bindings in Rust*](https://arxiv.org/abs/2401.14149). Language choice and
  bindings are therefore implementation decisions, not novelty claims.
- [OCPQ](https://arxiv.org/abs/2506.11541) provides expressive object-centric
  querying with a specialized high-performance Rust backend, compact bindings,
  early filtering, and comparisons to SQLite, Neo4j, and DuckDB. Specialized
  object-centric query execution is not new.
- [PM4Py](https://arxiv.org/abs/1905.06169) and ProM establish broad process
  mining library baselines. DFG discovery, variants, frequency filtering,
  rework counts, bottleneck statistics, and next-activity baselines are not new
  algorithms here.
- [PM4Py-GPU](https://arxiv.org/abs/2204.04898) is relevant prior art for
  columnar and parallel process-mining execution.
- [OPerA](https://arxiv.org/abs/2204.10662) and
  [HOEG](https://arxiv.org/abs/2404.05316) are relevant to object-centric
  predictive representations and graph learning. The current deterministic
  feature baseline should not be positioned as a novel prediction model.
- PostgreSQL arrays, TOAST, MVCC, native aggregates, expression pruning, and
  pre-aggregation are established database techniques. A patent or paper claim
  cannot be the use of any one of them in isolation.

No implementation from these works was copied or linked. Their concepts define
the comparison and novelty boundary.

## Candidate contribution hypotheses

These are hypotheses for literature and patent review, not claims:

1. **Versioned OCPM serving capsules inside PostgreSQL.** Exact, aligned event,
   edge, case, and adjacency vectors live behind a small relational pruning
   surface, preserving ordinary PostgreSQL transactions, backup, tenancy, and
   SQL composition while removing tuples from analytical inner loops.
2. **Native multi-window sufficient-statistic pushdown.** One C aggregate state
   evaluates arbitrary aligned training, test, comparison, or drift windows
   directly over capsule vectors and returns fixed-size count arrays. This
   avoids rescans, row multiplication, and model-layer event transfer.
3. **A deliberately narrow database/library boundary.** PostgreSQL owns exact
   filter semantics and reusable statistics; deterministic Rust owns model
   construction/scoring; Python receives final compact results through a
   GIL-releasing stable ABI. The contribution would be the co-design and cost
   boundary, not Rust bindings alone.
4. **Storage-performance co-optimization.** The representation uses 41.0% less
   total space and 82.7% less index space than the indexed relational OCEL
   baseline in the public fixture while lowering all 14 measured p50 latencies
   by at least 16.884x.
5. **Correctness-gated reproducible evaluation.** Every timed pair must produce
   the same canonical answer, the release artifact carries a digest, and
   latency, index/heap/TOAST storage, and concurrency use one containerized
   methodology.

The strongest paper story is likely the combined execution model and measured
tradeoff, not any individual process-mining algorithm.

## Required ablations

The current release benchmark compares end-to-end systems. A research paper
needs causal evidence:

| Ablation | Question |
|---|---|
| Relational OCEL vs persisted row-wise edges | Value of pre-derivation alone |
| Row-wise edges vs array capsules | Value of tuple elimination and TOAST layout |
| Capsule SQL unnest vs native C scan | Value of the native inner loop |
| Two one-window calls vs native multi-window state | Value of shared scans |
| Aggregate rows in Python vs Rust | Value of native model kernels/GIL release |
| Full indexes vs minimal pruning indexes | Storage, write, and concurrency tradeoff |
| Warm vs cold cache | Dependence on memory residency |
| 1, 4, 8, 16, 32+ pooled clients | Scaling and saturation point |

Report p50, p95, confidence intervals, CPU time, allocations, bytes read, WAL
during load/update, load cost, and total/index/TOAST bytes. Use at least 30
independent measured repetitions or justify a robust alternative. Pre-register
the exact result semantics and exclusion rules before the final run.

## Baselines needed for a credible submission

- Vanilla PostgreSQL with tuned relational OCEL and identical output semantics.
- Version-pinned PM4Py executed directly, not a “PM4Py-like” inference.
- DuckDB or another columnar analytical baseline on the same derived facts.
- OCPQ where its query semantics overlap; otherwise explain the mismatch rather
  than manufacture a comparison.
- A row-wise PostgreSQL materialization baseline to isolate capsules from
  precomputation.
- On the engine side, pure Python, single-thread Rust, and parallel Rust where
  the workload is large enough to justify parallelism.

Published numbers may be shown in a separate context table only. Cross-paper
speedups with different data, algorithms, hardware, or timing boundaries must
never be ranked as if they were the same experiment.

## Scale and robustness plan

1. Add larger public OCEL logs and deterministic scale factors from 1x through
   at least 100x while preserving event/object distributions.
2. Test selective and unselective dynamic filters: activity existence and
   nonexistence, actor/amount/location attributes, inter-activity duration,
   object type, edge type, context, and overlapping time windows.
3. Add incremental refresh, concurrent ingest/query, crash recovery, upgrade,
   replica, and backup/restore experiments. The current result measures static
   serving, not write scalability.
4. Run x86-64 and ARM64, local NVMe and managed PostgreSQL, and memory-constrained
   configurations. Publish every environment and negative result.
5. Commission an independent reproduction before submission.

## ICPM 2027 path

The official conference series currently lists
[ICPM 2027 in Rende, Italy, February 8–12, 2027](https://icpmconference.org/conferences/).
As of 2026-07-18, an official 2027 call and deadlines were not located, so the
team should monitor the conference site rather than plan around an assumed
submission date.

Suggested artifact sequence:

1. **Now:** invention disclosure, ownership audit, prior-art search, and license
   decision before broader technical disclosure.
2. **Research freeze:** formal semantics for each API, ablations, direct PM4Py
   and analytical-database baselines, scale/concurrency experiments, and an
   independent reproduction.
3. **Preprint/paper:** describe the capsule and sufficient-statistic execution
   model, provide proofs or invariants for exact filtering, and make bounded
   claims supported by the ablations.
4. **Demo artifact:** one-command public fixture, signed releases, short video,
   notebook/dashboard, anonymous reviewer access if the track requires it, and
   a complete artifact appendix.

Do not submit an abstract centered only on the 35.927x headline. Reviewers will
reasonably ask which component caused it, whether precomputation is charged,
whether outputs are equivalent, how it compares with current tools, and whether
the result survives scale and cold-cache conditions. The roadmap above is the
minimum evidence needed to answer those questions credibly.
