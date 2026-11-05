"""
Resolve WASB bundle classes.

The provider classpath is
``airflow.providers.microsoft.azure.bundles.wasb``. When that module defines
``WasbDagBundle`` or ``WasbArchiveDagBundle``, those classes are used.
Otherwise this package exports the vendored implementations, which follow the
same constructor and staging shape.
"""

from __future__ import annotations

import importlib

PROVIDER_MODULE = "airflow.providers.microsoft.azure.bundles.wasb"

_resolved: dict[str, tuple[type, str]] = {}


def _provider_class(name: str) -> type | None:
    try:
        module = importlib.import_module(PROVIDER_MODULE)
    except ImportError:
        return None
    candidate = getattr(module, name, None)
    if isinstance(candidate, type):
        return candidate
    return None


def _resolve(name: str) -> tuple[type, str]:
    cached = _resolved.get(name)
    if cached is not None:
        return cached
    provider = _provider_class(name)
    if provider is not None:
        resolved = (provider, f"{PROVIDER_MODULE}.{name}")
    else:
        vendored = importlib.import_module("wasb_bundle.wasb")
        resolved = (getattr(vendored, name), f"wasb_bundle.{name}")
    _resolved[name] = resolved
    return resolved


WasbDagBundle, WASB_DAG_BUNDLE_CLASSPATH = _resolve("WasbDagBundle")
WasbArchiveDagBundle, WASB_ARCHIVE_DAG_BUNDLE_CLASSPATH = _resolve("WasbArchiveDagBundle")

__all__ = [
    "WASB_ARCHIVE_DAG_BUNDLE_CLASSPATH",
    "WASB_DAG_BUNDLE_CLASSPATH",
    "WasbArchiveDagBundle",
    "WasbDagBundle",
]
