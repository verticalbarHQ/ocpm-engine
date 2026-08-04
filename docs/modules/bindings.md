# `ocpm_engine.bindings`

Native decoding helpers for compact pg_ocpm binding-result capsules
(`ocpm.binding_ids`, `ocpm.binding_neighbor_pairs`, relation functions, and
the other binding primitives). Decoding in Rust avoids tuple-at-a-time SQL
materialization; `ocpm.binding_capsule_rows(...)` remains available when a
SQL row representation is required instead.

## Functions

```python
binding_capsule_info(capsule: bytes) -> BindingCapsuleInfo
```
Header inspection without a full decode: the capsule `schema` version, its
`row_count`, and whether it is `factorized`.

```python
decode_binding_capsule(capsule: bytes) -> tuple[BindingRow, ...]
```
Full decode to rows. Each `BindingRow` carries the bound `ids` tuple plus
optional `label`, `violated`, and `value` members depending on the producing
primitive.

```python
decode_binding_pair_groups(capsule: bytes) -> tuple[BindingPairGroup, ...]
```
Decode factorized groups without expanding their Cartesian products. Each
`BindingPairGroup` carries one `source`, its `targets`, and their `events`;
`expanded_row_count` reports the size the expanded form would have.

## Types

| Type | Fields |
|---|---|
| `BindingCapsuleInfo` | `schema`, `row_count`, `factorized` |
| `BindingRow` | `ids`, `label`, `violated`, `value` |
| `BindingPairGroup` | `source`, `targets`, `events`, `expanded_row_count` (property) |

## Example

```python
from ocpm_engine import binding_capsule_info, decode_binding_capsule

cursor.execute(sql, params)          # any ocpm.binding_* capsule result
capsule = cursor.fetchone()[0]

info = binding_capsule_info(capsule)
print(info.schema, info.row_count, info.factorized)

for row in decode_binding_capsule(capsule):
    print(row.ids, row.violated)
```
