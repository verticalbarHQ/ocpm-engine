# Academic implementation provenance for ocpm-engine 1.0

Status: normative clean-room engineering policy  
Snapshot date: 2026-08-10

## Source boundary

Implementation logic in `ocpm-engine` and `pg_ocpm` may be derived only from
formal definitions, equations, and algorithms in the peer-reviewed publications
listed below. Serialization code is independently authored against the data
model justified by those papers and round-trip fixtures owned by this project.
No non-peer-reviewed format description may be used to fill a semantic gap.

The project does not inspect, translate, port, decompile, or adapt source code
from OCPQ, Rust4PM, OCPA, PM4Py, ProM, or other process-mining libraries.
Those systems are used only as separately built black-box benchmark arms and,
where semantics match, independent output oracles. Public API documentation is
not an implementation source. It may be consulted only by benchmark adapters
that invoke a released black-box package, never by an algorithm module.

Each algorithm module must include a `PROVENANCE` constant containing the DOI,
the definitions implemented, deliberate deviations, and fixture IDs. A pull
request adding or changing an algorithm is incomplete without its provenance
entry and independently authored tests.

## Peer-reviewed foundation

| Capability | Peer-reviewed basis | Implementation boundary |
|---|---|---|
| OCEL canonical entities and relations | Ghahfarokhi et al., “OCEL: A Standard for Object-Centric Event Logs,” SIMPDA 2021, [doi:10.1007/978-3-030-85082-1_16](https://doi.org/10.1007/978-3-030-85082-1_16) | Events, objects, qualified relations, attributes, lossless exchange model |
| XES event-log exchange | Verbeek et al., “XES, XESame, and ProM 6,” CAiSE Forum 2010 selected papers, [doi:10.1007/978-3-642-17722-4_5](https://doi.org/10.1007/978-3-642-17722-4_5) | Trace/event exchange, concepts, typed attributes, and extensions |
| Object-centric case/execution and graph variants | Adams et al., “Defining Cases and Variants for Object-Centric Event Data,” ICPM 2022, [doi:10.1109/ICPM57379.2022.9980730](https://doi.org/10.1109/ICPM57379.2022.9980730) | Process-execution extraction and labeled graph-isomorphism semantics |
| DFG and OC-DFG | Berti and van der Aalst, “OC-PM: analyzing object-centric event logs and process models,” STTT 2023, [doi:10.1007/s10009-022-00668-w](https://doi.org/10.1007/s10009-022-00668-w) | Per-object directly-follows, node/edge frequencies, object-type annotations |
| Alpha discovery | van der Aalst et al., “Workflow Mining: Discovering Process Models from Event Logs,” IEEE TKDE 2004, [doi:10.1109/TKDE.2004.47](https://doi.org/10.1109/TKDE.2004.47) | Footprint relations and maximal place-pair construction |
| Inductive process-tree discovery | Leemans, Fahland, and van der Aalst, “Discovering Block-Structured Process Models from Event Logs: A Constructive Approach,” PETRI NETS 2013, [doi:10.1007/978-3-642-38697-8_17](https://doi.org/10.1007/978-3-642-38697-8_17) | Recursive sequence, exclusive, parallel, and loop cuts with fall-through |
| Object-centric Petri-net discovery | van der Aalst and Berti, “Discovering Object-Centric Petri Nets,” Fundamenta Informaticae 2020, [doi:10.3233/FI-2020-1946](https://doi.org/10.3233/FI-2020-1946) | Per-type flattening, discovery, transition merge, typed and variable arcs |
| Alignment conformance | Adriansyah et al., “Conformance Checking Using Cost-Based Fitness Analysis,” EDOC 2011, [doi:10.1109/EDOC.2011.12](https://doi.org/10.1109/EDOC.2011.12) | Synchronous/log/model moves and minimum-cost search |
| Decomposed exact alignments | van der Aalst et al., “Recomposing conformance,” Information Sciences 2018, [doi:10.1016/j.ins.2018.07.026](https://doi.org/10.1016/j.ins.2018.07.026) | Safe decomposition and exact recomposition conditions |
| Object-centric DFG conformance/performance | Park, Adams, and van der Aalst, “Conformance Checking and Performance Analysis Using Object-Centric Directly-Follows Graphs,” BPM Forum 2024, [doi:10.1007/978-3-031-70418-5_11](https://doi.org/10.1007/978-3-031-70418-5_11) | OC-DFG diagnostics and object-centric performance measures |
| Object-centric query trees and bindings | Küsters and van der Aalst, “OCPQ: Object-Centric Process Querying and Constraints,” RCIS 2025, [doi:10.1007/978-3-031-92474-3_23](https://doi.org/10.1007/978-3-031-92474-3_23) | Nested binding queries, cardinalities, labels, constraint violations |
| Object-centric declarative discovery | Küsters and van der Aalst, “OC-DECLARE,” BPM 2025, [doi:10.1007/978-3-032-02867-9_11](https://doi.org/10.1007/978-3-032-02867-9_11) | Object-centric declarative templates and synchronization |
| Alternative object-centric declarative discovery | Goossens et al., “Discovery of Object-Centric Declarative Models,” ICPM 2024, [doi:10.1109/ICPM63005.2024.10680680](https://doi.org/10.1109/ICPM63005.2024.10680680) | Independent declarative constraint validation and comparison |
| Predictive targets and evaluation | Verenich et al., “Survey and Cross-benchmark Comparison of Remaining Time Prediction Methods,” ACM TIST 2019, [doi:10.1145/3331449](https://doi.org/10.1145/3331449) | Leakage-safe prefixes, remaining-time targets, comparative metrics |
| Outcome prediction | Di Francescomarino et al., “Clustering-Based Predictive Process Monitoring,” IEEE TSC 2019, [doi:10.1109/TSC.2016.2645153](https://doi.org/10.1109/TSC.2016.2645153) | Prefix/data encodings and probabilistic outcome prediction |
| Predictive-process definitions | Ceravolo et al., “Predictive process monitoring: concepts, challenges, and future research directions,” Process Science 2024, [doi:10.1007/s44311-024-00002-4](https://doi.org/10.1007/s44311-024-00002-4) | Next-activity, outcome/risk, time, calibration, and evaluation scope |
| Frequency drift | Yeshchenko et al., “Comprehensive concept drift characterization in process mining,” Information Systems 2026, [doi:10.1016/j.is.2025.102584](https://doi.org/10.1016/j.is.2025.102584) | Windowed behavior comparison and localized drift contributions |
| Jensen-Shannon divergence | Lin, “Divergence Measures Based on the Shannon Entropy,” IEEE Transactions on Information Theory 1991, [doi:10.1109/18.61115](https://doi.org/10.1109/18.61115) | Symmetric bounded distribution divergence used by drift scoring |
| Object-centric performance operations | Adams et al., “OPerA: Object-Centric Performance Analysis,” ICPM 2022, [doi:10.1007/978-3-031-17995-2_20](https://doi.org/10.1007/978-3-031-17995-2_20) | Object readiness, synchronization, pooling, lagging, and flow-time interpretation |
| Waiting-cause decomposition | Lashkevich et al., “Why am I waiting? Data-driven analysis of waiting times in business processes,” Information Systems 2024, [doi:10.1016/j.is.2024.102434](https://doi.org/10.1016/j.is.2024.102434) | Evidence-gated, non-overlapping batching, prioritization, contention, unavailability, and residual attribution |
| Queue mining and delay prediction | Senderovich et al., “Queue mining for delay prediction in multi-class service processes,” Information Systems 2015, [doi:10.1016/j.is.2015.03.010](https://doi.org/10.1016/j.is.2015.03.010) | Arrival, service, waiting, queue, throughput, and capacity-pressure measures |
| Performance spectrum | Denisov et al., “The Performance Spectrum Miner: Visual Analytics for Fine-Grained Performance Analysis of Processes,” BPM 2018, [doi:10.1007/978-3-319-98648-7_9](https://doi.org/10.1007/978-3-319-98648-7_9) | Relative timing, overtaking, and congestion-pattern measures |
| Batch detection | Martin et al., “Batch Processing: Definition and Event Log Identification,” BPM Workshops 2019, [doi:10.1007/978-3-030-37453-2_15](https://doi.org/10.1007/978-3-030-37453-2_15) | Configurable temporal batch clusters and batch prevalence |
| Explainable object-centric drift | Adams et al., “Detecting and explaining object-centric process drift,” Information Systems 2023, [doi:10.1016/j.is.2023.102177](https://doi.org/10.1016/j.is.2023.102177) | Edge-localized distribution change and object-type attribution |
| Temporal causal hypotheses | Koorn et al., “AITIA-PM: A framework for analyzing causality in process mining,” Engineering Applications of Artificial Intelligence 2024, [doi:10.1016/j.engappai.2023.107145](https://doi.org/10.1016/j.engappai.2023.107145) | Caller-declared temporal precedence and probability-raising hypothesis tests; no causal-proof claim |
| Recursive blocking analysis | García-Bañuelos et al., “Discovering and analyzing blocking cascades in cargo processes,” Process Science 2026, [doi:10.1007/s44311-026-00038-8](https://doi.org/10.1007/s44311-026-00038-8) | Domain-neutral resource-overlap chains with cycle and depth guards |
| Object-centric event GNN representation | Bernard and Andritsos, “HOEG: A New Approach for Object-Centric Predictive Process Monitoring,” BPM 2024, [doi:10.1007/978-3-031-61057-8_14](https://doi.org/10.1007/978-3-031-61057-8_14) | Optional graph module, object/event context, and heterogeneous categorical features; this project uses transition nodes and documents that deliberate representation choice |
| Inductive neighborhood aggregation | Hamilton, Ying, and Leskovec, “Inductive Representation Learning on Large Graphs,” NeurIPS 2017, [paper and reviews](https://proceedings.neurips.cc/paper/2017/hash/5dd9db5e033da9c6fb5ba83c7a7ebea9-Abstract.html) | Two learned mean-aggregation layers over bounded neighborhoods; deterministic feature hashing supports unseen categorical values |
| GNN empirical comparison | Weinzierl et al., “Graph Neural Networks for Predictive Process Monitoring: A Comparative Study,” ICPM Workshops 2023, [doi:10.1007/978-3-031-50974-2_39](https://doi.org/10.1007/978-3-031-50974-2_39) | Separate optional GNN module, temporal holdout diagnostics, and explicit probabilistic result selection |

## Evidence-limited interoperability

The engine implements canonical JSON, the admitted OCEL JSON data model, CSV,
XES, and OCEL SQLite through independently authored parsers and writers. It does
not claim OCEL 2.0 XML support in 1.0 because the detailed XML syntax available
to the project is not itself a peer-reviewed algorithm or data-model source.
This is a deliberate evidence boundary, not a compatibility shortcut.

## Database and runtime engineering basis

| Technique | Peer-reviewed basis | Use here |
|---|---|---|
| Vectorized execution | Boncz, Zukowski, and Nes, “MonetDB/X100: Hyper-Pipelining Query Execution,” CIDR 2005 | Bounded column batches and low-branch inner loops |
| Late materialization | Abadi, Madden, and Hachem, “Column-Stores vs. Row-Stores,” SIGMOD 2008, [doi:10.1145/1376616.1376712](https://doi.org/10.1145/1376616.1376712) | Compact IDs/statistics before strings and full event hydration |
| Morsel-driven concurrency | Leis et al., “Morsel-Driven Parallelism,” SIGMOD 2014, [doi:10.1145/2588555.2610507](https://doi.org/10.1145/2588555.2610507) | Bounded independent work units and work stealing outside PostgreSQL backends |
| Factorized intermediate results | Olteanu and Závodný, “Factorised Representations of Query Results,” TODS 2015, [doi:10.1145/2656335](https://doi.org/10.1145/2656335) | Binding groups and relationship results without Cartesian expansion |

## Module gate

Each implementation module records:

```text
paper DOI
paper section/definition/algorithm
implemented input and output semantics
implementation choices not fixed by the paper
known unsupported conditions
independent fixture IDs
black-box comparison workloads, if any
```

Reviewers reject a change when it cites an upstream repository, source file,
package implementation, generated binding, or copied test fixture as an
algorithm source. Benchmark adapters may call documented public APIs, but the
algorithm under test stays inside its separately built container and does not
enter this repository's implementation dependency graph.
