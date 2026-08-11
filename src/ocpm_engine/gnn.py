"""Optional graph-neural-network interfaces.

``StandaloneEngine.gnn_bottlenecks`` provides the built-in deterministic CPU
detector without installing a tensor runtime. This protocol remains available
for separately packaged predictive GNN models such as next-activity prediction.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class GnnBackend(Protocol):
    """Contract implemented by an independently distributed GNN backend."""

    @property
    def name(self) -> str: ...

    def fit(self, engine: Any, request: dict[str, Any]) -> dict[str, Any]: ...

    def predict(
        self,
        engine: Any,
        request: dict[str, Any],
        artifact: dict[str, Any],
    ) -> dict[str, Any]: ...


def require_backend(backend: GnnBackend | None) -> GnnBackend:
    """Validate an explicit optional backend without importing ML packages."""

    if backend is None or not isinstance(backend, GnnBackend):
        raise RuntimeError(
            "this predictive GNN task requires an explicit ocpm-engine backend"
        )
    return backend
