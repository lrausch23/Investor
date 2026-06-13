from __future__ import annotations

from typing import Any, Callable, TypeVar

T = TypeVar("T")

_REGISTRY: dict[str, dict[str, type[Any]]] = {
    "signal": {},
    "exposure": {},
    "override": {},
    "allocation": {},
    "rebalance": {},
}


def register_layer(kind: str, key: str) -> Callable[[type[T]], type[T]]:
    normalized_kind = _normalize_kind(kind)
    normalized_key = _normalize_key(key)

    def decorator(cls: type[T]) -> type[T]:
        _REGISTRY.setdefault(normalized_kind, {})[normalized_key] = cls
        return cls

    return decorator


def build(kind: str, key: str | None, params: dict[str, Any] | None = None) -> Any:
    normalized_kind = _normalize_kind(kind)
    if key is None:
        return None
    normalized_key = _normalize_key(key)
    bucket = _REGISTRY.setdefault(normalized_kind, {})
    cls = bucket.get(normalized_key)
    if cls is None:
        options = ", ".join(sorted(bucket)) or "none"
        raise KeyError(f"Unknown {normalized_kind} layer '{key}'. Available: {options}")
    return cls(**dict(params or {}))


def available_layers(kind: str | None = None) -> dict[str, list[str]] | list[str]:
    if kind is not None:
        return sorted(_REGISTRY.setdefault(_normalize_kind(kind), {}))
    return {bucket: sorted(values) for bucket, values in sorted(_REGISTRY.items())}


def _normalize_kind(kind: str) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized not in _REGISTRY:
        raise KeyError(f"Unknown layer kind '{kind}'. Available kinds: {', '.join(sorted(_REGISTRY))}")
    return normalized


def _normalize_key(key: str) -> str:
    normalized = str(key or "").strip()
    if not normalized:
        raise KeyError("Layer key must be non-empty.")
    return normalized
