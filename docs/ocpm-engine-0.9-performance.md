# ocpm-engine 0.9 performance evidence

`ocpm-engine 0.9.0` adds the native consumer for `pg_ocpm 0.9.0` factorized
event batches. PostgreSQL returns compact activity-path bucket rows with packed
vectors, Rust validates and decodes borrowed column groups, and persistent
native builders compute DFG, variant, next-activity, and bottleneck results
without Python event rows or Pandas dataframes.

The final clean-commit Docker evidence is published in the
[full 0.9 benchmark](ocpm-engine-0.9-full-benchmark.md). Across eight exact SAP
O2C/P2P workload comparisons, the native path is **25.181x** faster than
vanilla PostgreSQL plus PM4Py and **18.793x** faster than `pg_ocpm` plus PM4Py
by geometric mean. On strict OCPQ Q1-Q7 it is **15.108x** faster than OCPQ,
with every node and situation exact.

## General architecture

The implementation contains no OCPQ query identifiers, dataset names,
expected counts, expected hashes, or fixed-answer caches. Aggregate-native
algorithms use sufficient statistics when they are smaller than event batches;
event-oriented algorithms use the factorized export. Existing dynamic filtered
queries keep exact row/bucket plans and preselect eligible cases before event
or lifecycle expansion when predicates allow it.

The adapter discovers the installed PostgreSQL capability at runtime:

- `pg_ocpm >= 0.9.0`: factorized event batches;
- `pg_ocpm 0.8.x`: exact row-stream fallback; and
- summary/index plans: selected only when their declared coverage matches the
  requested binding dimensions.

Requests with more than 256 time windows are sliced through one SQL statement
using lateral calls of at most 256 windows, preserving one transaction snapshot
and global result ordinals. Both the Rust and Python adapters validate equal,
nonempty start/end arrays before execution.

## Memory and first-row behavior

The PostgreSQL export reads source buckets through a bounded SPI portal and
stores compact output in a `work_mem`-backed tuplestore. A named server-side
cursor bounds client buffering. PostgreSQL materialized-SRF semantics still
complete the compact server result before returning its first row, so 0.9 does
not claim streaming time-to-first-row.

In the SAP fresh-process probes, maximum incremental peak RSS was 9.113 MiB on
O2C and 3.430 MiB on P2P, versus 165.637 and 148.094 MiB for `pg_ocpm` plus
PM4Py. The improvement comes from removing expanded Python/Pandas structures,
not from relaxing answer materialization or correctness.

## Dynamic-query expectations

The fixed OCPQ speedup must not be projected unchanged onto arbitrary dynamic
queries. Latency depends on selectivity, relationship fanout, returned result
size, cache state, and available index coverage. Version 0.9 generally reduces
tuple transfer, allocation, Python work, and redundant setup; wide queries that
must return large owned trees remain bounded by required database and transfer
work.

Execution telemetry reports the chosen strategy, database rows, logical
events, expanded rows, and packed payload bytes. This makes a fallback or
unexpectedly wide plan observable in production rather than hiding it behind a
benchmark headline.

## Evidence pins

- `pg_ocpm`: `7b201978d00ff4014ffc536ed7f391493b707a76`
- `ocpm-engine`: `45de4da07a88e9c722a5ac9dcd5e154aa38bae8f`
- PM4Py: 2.7.23.3
- SAP artifact SHA-256: `da0d79d83cde5a7966b5bd2ec2335658d0366fa2af813ef868d3a09e04eda850`
- strict OCPQ candidate SHA-256: `317af340bff890551c1dfcafe0e0fc8777ade938865173362d56d20073222f1e`

See the [benchmark guide](../benchmarks/README.md) for Docker reproduction and
fail-closed verification commands.
