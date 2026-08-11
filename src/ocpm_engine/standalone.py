"""Source-neutral Python facade over the standalone Rust engine."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

try:
    from ._native import StandaloneEngine as _NativeStandaloneEngine
except ImportError:  # Helpful message for an unbuilt source checkout.
    _NativeStandaloneEngine = None


JsonObject = Mapping[str, Any]


def _extension():
    if _NativeStandaloneEngine is None:
        raise RuntimeError(
            "ocpm-engine native module is not built; install a wheel or run "
            "`maturin develop`"
        )
    return _NativeStandaloneEngine


def _encode(value: JsonObject) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _decode(value: str) -> dict[str, Any] | list[Any]:
    decoded = json.loads(value)
    if not isinstance(decoded, (dict, list)):
        raise RuntimeError("native engine returned a non-container JSON result")
    return decoded


class StandaloneEngine:
    """Run OCPM operations from files or canonical in-memory data.

    Public request and result values are ordinary mappings following the
    versioned 1.0 JSON contracts. Algorithms execute in Rust; this class only
    handles Python-to-JSON conversion.
    """

    def __init__(self, canonical_log: JsonObject) -> None:
        self._native = _extension()(_encode(canonical_log))

    @classmethod
    def from_ocel2_json(cls, value: str | bytes | Path) -> StandaloneEngine:
        instance = cls.__new__(cls)
        if isinstance(value, Path):
            payload = value.read_text(encoding="utf-8")
        elif isinstance(value, bytes):
            payload = value.decode("utf-8")
        else:
            payload = value
        instance._native = _extension().from_ocel2_json(payload)
        return instance

    @classmethod
    def from_xes(cls, value: str | bytes | Path) -> StandaloneEngine:
        instance = cls.__new__(cls)
        if isinstance(value, Path):
            payload = value.read_text(encoding="utf-8")
        elif isinstance(value, bytes):
            payload = value.decode("utf-8")
        else:
            payload = value
        instance._native = _extension().from_xes(payload)
        return instance

    @classmethod
    def from_sqlite(cls, path: str | Path) -> StandaloneEngine:
        instance = cls.__new__(cls)
        instance._native = _extension().from_sqlite(str(path))
        return instance

    @classmethod
    def from_duckdb_parquet(cls, source: JsonObject) -> StandaloneEngine:
        """Open local or S3 Parquet through a supplied DuckDB installation."""

        instance = cls.__new__(cls)
        instance._native = _extension().from_duckdb_parquet(_encode(source))
        return instance

    @property
    def provider_name(self) -> str:
        return self._native.provider_name()

    @property
    def capabilities(self) -> list[str]:
        result = _decode(self._native.capabilities_json())
        if not isinstance(result, list):
            raise RuntimeError("native engine returned invalid capabilities")
        return [str(item) for item in result]

    def append(self, batch: JsonObject) -> None:
        """Atomically validate and append a canonical batch."""

        self._native.append_json(_encode(batch))

    def profile(self, view: JsonObject | None = None) -> dict[str, Any]:
        return self._result(self._native.profile_json(_encode(view or {})))

    def query(self, request: JsonObject) -> dict[str, Any]:
        return self._result(self._native.query_json(_encode(request)))

    def canonical_json(self, view: JsonObject | None = None) -> dict[str, Any]:
        return self._result(self._native.canonical_json(_encode(view or {})))

    def ocel2_json(self, view: JsonObject | None = None) -> dict[str, Any]:
        return self._result(self._native.ocel2_json(_encode(view or {})))

    def xes(self, object_type: str, view: JsonObject | None = None) -> str:
        return str(self._native.xes(_encode(view or {}), object_type))

    def write_sqlite(self, path: str | Path, view: JsonObject | None = None) -> None:
        self._native.write_sqlite(_encode(view or {}), str(path))

    def write_parquet_snapshot(
        self,
        root: str | Path,
        version: str,
        view: JsonObject | None = None,
    ) -> dict[str, Any]:
        """Write a new immutable canonical Parquet snapshot and CURRENT pointer."""

        return self._result(
            self._native.write_parquet_snapshot(_encode(view or {}), str(root), version)
        )

    def execution_summary(self, request: JsonObject) -> dict[str, Any]:
        """Return exact compact lifecycle, variant, DFG, and activity statistics."""

        return self._result(self._native.execution_summary_json(_encode(request)))

    def discover(self, request: JsonObject) -> dict[str, Any]:
        return self._result(self._native.discover_json(_encode(request)))

    def conformance(self, request: JsonObject) -> dict[str, Any]:
        return self._result(self._native.conformance_json(_encode(request)))

    def enhance(self, request: JsonObject) -> dict[str, Any]:
        return self._result(self._native.enhance_json(_encode(request)))

    def bottlenecks(self, request: JsonObject) -> dict[str, Any]:
        """Run provider-neutral latency, queue, drift, and cause analysis."""

        return self._result(self._native.bottlenecks_json(_encode(request)))

    def fit_gnn_bottlenecks(self, request: JsonObject) -> dict[str, Any]:
        """Fit the optional CPU graph-aware bottleneck detector."""

        return self._result(self._native.fit_gnn_bottlenecks_json(_encode(request)))

    def score_gnn_bottlenecks(
        self,
        request: JsonObject,
        artifact: JsonObject,
    ) -> dict[str, Any]:
        """Score graph-aware bottleneck risk with a portable artifact."""

        return self._result(
            self._native.score_gnn_bottlenecks_json(
                _encode(request),
                _encode(artifact),
            )
        )

    def gnn_bottlenecks(self, request: JsonObject) -> dict[str, Any]:
        """Fit and score graph-aware bottleneck risk in one provider scan."""

        return self._result(self._native.gnn_bottlenecks_json(_encode(request)))

    def fit_prediction(self, request: JsonObject) -> dict[str, Any]:
        return self._result(self._native.fit_prediction_json(_encode(request)))

    def predict(self, request: JsonObject) -> dict[str, Any]:
        return self._result(self._native.predict_json(_encode(request)))

    def evaluate_prediction(
        self,
        view: JsonObject,
        target: str,
        *,
        holdout_fraction: float = 0.2,
        parameters: JsonObject | None = None,
    ) -> dict[str, Any]:
        return self._result(
            self._native.evaluate_prediction_json(
                _encode(view),
                _encode(target),
                holdout_fraction,
                _encode(parameters or {}),
            )
        )

    def explain(self, view: JsonObject, capability: str) -> dict[str, Any]:
        return self._result(
            self._native.explain_json(_encode(view), _encode(capability))
        )

    @staticmethod
    def _result(value: str) -> dict[str, Any]:
        result = _decode(value)
        if not isinstance(result, dict):
            raise RuntimeError("native engine returned an invalid result")
        return result


def serialize_model(artifact: JsonObject, format: str = "json") -> str:
    """Serialize a 1.0 model artifact as JSON, DOT, PNML, or SVG."""

    if _NativeStandaloneEngine is None:
        _extension()
    from . import _native

    return str(_native.serialize_model(_encode(artifact), format))
