"""Native decoding helpers for compact pg_ocpm binding-result capsules."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BindingCapsuleInfo:
    schema: int
    row_count: int
    factorized: bool


@dataclass(frozen=True, slots=True)
class BindingRow:
    ids: tuple[int, ...]
    label: str | None
    violated: bool | None
    value: float | None


@dataclass(frozen=True, slots=True)
class BindingPairGroup:
    source: int
    targets: tuple[int, ...]
    events: tuple[int, ...]

    @property
    def expanded_row_count(self) -> int:
        return len(self.targets) ** 2


def binding_capsule_info(capsule: bytes) -> BindingCapsuleInfo:
    from . import _native

    schema, row_count, factorized = _native.binding_capsule_info(capsule)
    return BindingCapsuleInfo(schema, row_count, factorized)


def decode_binding_capsule(capsule: bytes) -> tuple[BindingRow, ...]:
    from . import _native

    return tuple(
        BindingRow(tuple(ids), label, violated, value)
        for ids, label, violated, value in _native.decode_binding_capsule(capsule)
    )


def decode_binding_pair_groups(capsule: bytes) -> tuple[BindingPairGroup, ...]:
    """Decode factorized groups without expanding their Cartesian products."""

    from . import _native

    return tuple(
        BindingPairGroup(source, tuple(targets), tuple(events))
        for source, targets, events in _native.decode_binding_pair_groups(capsule)
    )
