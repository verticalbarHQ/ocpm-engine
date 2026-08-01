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

## Verify

```bash
python3 benchmarks/check_ocpq_four_way.py \
  .benchmarks/ocpq-reproduced-strict-all-node-0.9-final.json \
  .benchmarks/ocpq-vanilla-pg-pm4py-0.9-final.json \
  .benchmarks/ocpq-pg_ocpm-0.9-pm4py-final.json \
  .benchmarks/ocpq-bpic2017-pg_ocpm-0.9.0-ocpm-engine-0.9.0-final.json \
  --reference-sha256 39894339697421834a652620406152c87a92b08831c3a68dc3f30acd6dc77964 \
  --vanilla-sha256 12ae7b81f6b67d075b7d491f29fde4e2868f87c8c31828400ee2489c8e83e036 \
  --pg-pm4py-sha256 a460e2145d8966def16fa5672e4e9f186572d83226ec1a39ea5b68b382be721a \
  --engine-sha256 317af340bff890551c1dfcafe0e0fc8777ade938865173362d56d20073222f1e \
  --pm4py-runner ../pg_ocpm/benchmarks/ocpq_pm4py.py \
  --pm4py-runner-sha256 a431ed3ac827fb86011ddda60c33aeaa9d304a7836800168750cb923ef925486
```

The strict release verification is:

```bash
python3 benchmarks/check_ocpq_result.py
```
