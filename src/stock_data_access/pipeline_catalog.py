"""Shared data-pipeline catalog.

This module is the single source of truth for ``data_pipelines_catalog.yaml``:
the declarative spec of every data pipeline (id, dependency DAG, trigger flags,
freshness/coverage monitoring config, and data contracts). It is bundled as
package data so every consumer image (quant-data-engine executor, quantFinance
monitoring API, …) carries an identical copy — no runtime file mount or host
sync required.

Resolution order (so local overrides still work):
  1. ``DATA_PIPELINES_CATALOG`` env var, if set and the file exists.
  2. The packaged ``catalogs/data_pipelines_catalog.yaml``.

Consumers should import ``load_pipeline_catalog`` / ``catalog_path`` instead of
reading a hard-coded path.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from importlib.resources import files as _ir_files  # Python 3.9+
except Exception:  # pragma: no cover - very old runtimes
    _ir_files = None  # type: ignore

CATALOG_ENV_VAR = "DATA_PIPELINES_CATALOG"
_PACKAGE_RESOURCE = ("stock_data_access.catalogs", "data_pipelines_catalog.yaml")


def _packaged_path() -> Optional[Path]:
    """Filesystem path to the packaged catalog, or None if unavailable."""
    if _ir_files is not None:
        try:
            res = _ir_files(_PACKAGE_RESOURCE[0]).joinpath(_PACKAGE_RESOURCE[1])
            p = Path(str(res))
            if p.is_file():
                return p
        except Exception:
            pass
    # Fallback for editable/source layouts where importlib.resources misses.
    local = Path(__file__).resolve().parent / "catalogs" / _PACKAGE_RESOURCE[1]
    return local if local.is_file() else None


def catalog_path(override: Optional[str] = None) -> Path:
    """Return the resolved catalog path (env override > packaged copy).

    Raises FileNotFoundError when neither an override nor the packaged file is
    available.
    """
    if override:
        p = Path(override)
        if p.is_file():
            return p
        raise FileNotFoundError(f"catalog override not found: {override}")

    env_path = os.getenv(CATALOG_ENV_VAR, "").strip()
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p

    packaged = _packaged_path()
    if packaged is not None:
        return packaged
    raise FileNotFoundError(
        "data_pipelines_catalog.yaml not found (no env override and no packaged copy)"
    )


def load_pipeline_catalog(override: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return the raw list of pipeline entries from the catalog.

    Each entry is the plain dict as authored in YAML (id, label, depends_on,
    contract, freshness, …). Parsing into typed objects (e.g. contracts) stays
    with each consumer, keeping this library free of consumer-specific models.
    """
    import yaml  # local import so importers that only need catalog_path avoid it

    p = catalog_path(override)
    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    pipelines = data.get("pipelines") if isinstance(data, dict) else data
    if isinstance(pipelines, dict):
        pipelines = list(pipelines.values())
    return [e for e in (pipelines or []) if isinstance(e, dict) and e.get("id")]


__all__ = ["CATALOG_ENV_VAR", "catalog_path", "load_pipeline_catalog"]
