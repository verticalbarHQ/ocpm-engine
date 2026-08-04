# OCPQ data: four-way 0.9 comparison

The fixed BPIC 2017-derived OCPQ Q1-Q7 fixture was evaluated through four
Dockerized arms:

1. OCPQ 0.6.7;
2. vanilla PostgreSQL 16 plus PM4Py 2.7.23.3;
3. `pg_ocpm 0.9.0` plus the same PM4Py environment; and
4. `pg_ocpm 0.9.0` plus `ocpm-engine 0.9.0`.

Every arm reproduced all 13 result nodes and all 380,083
duplicate-preserving situations exactly.

| Query | OCPQ | Vanilla PG + PM4Py | pg_ocpm + PM4Py | pg_ocpm + engine |
|---|---:|---:|---:|---:|
| Q1 | 19.437 ms | 31.622 ms | 25.179 ms | 1.339 ms |
| Q2 | 35.504 ms | 62.826 ms | 47.400 ms | 2.747 ms |
| Q3 | 14.001 ms | 29.649 ms | 24.540 ms | 1.874 ms |
| Q4 | 35.269 ms | 84.317 ms | 76.914 ms | 2.325 ms |
| Q5 | 43.461 ms | 145.028 ms | 141.304 ms | 5.302 ms |
| Q6 | 68.128 ms | 23.563 ms | 16.742 ms | 1.926 ms |
| Q7 | 54.708 ms | 95.556 ms | 77.233 ms | 1.878 ms |
| **Geometric mean** | **34.242 ms** | **55.499 ms** | **45.626 ms** | **2.266 ms** |

The native path is **15.108x** faster than OCPQ, **24.487x** faster than
vanilla PostgreSQL plus PM4Py, and **20.131x** faster than `pg_ocpm` plus
PM4Py by geometric mean. `pg_ocpm` improves the fixed PM4Py arm by **1.216x**.

## Interpretation boundary

PM4Py does not implement OCPQ evaluation trees. The two PM4Py arms therefore
use one explicit Pandas 3.0.3 evaluator over the complete resident PM4Py OCEL.
The evaluator performs real joins, constraints, violation construction, and
duplicate-preserving row materialization. It contains no expected answers or
result shortcuts, but it is not OCPQ's native evaluator.

The four-way checker returns `verified_descriptive_preview`. Exactness and
artifact hashes pass, but the PM4Py artifacts omit the strict cross-arm host
identifier and have a different evaluator boundary. The independent OCPQ
versus native-engine artifact passes all strict publication gates. See the
[full benchmark](ocpm-engine-0.9-full-benchmark.md) for concurrency, memory,
storage, SAP generalization, and dynamic-query limits.

## Archival status

This report and its published result artifacts are retained as historical
evidence only. The repository no longer supplies the OCPQ benchmark runner,
checker, adapter, dataset loader, image builder, or reproduction commands.
