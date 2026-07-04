"""Point-in-time index constituent snapshot helpers.

Resolves a single ``index_constituents`` snapshot and returns the symbol list for
that snapshot — never a per-symbol historical union (which is what a
``$group by symbol`` pipeline would produce).

Snapshots are keyed by ``(weight_trade_date, update_date)``. Legacy documents that
predate the ``weight_trade_date`` field are still supported: resolution falls back
to ``update_date`` so callers never silently get an empty universe.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


def normalize_yyyymmdd(value: Any) -> str:
    text = str(value or "").replace("-", "").strip()
    return text[:8] if len(text) >= 8 and text[:8].isdigit() else ""


@dataclass(frozen=True)
class IndexConstituentSnapshot:
    index_code: str
    weight_trade_date: str
    update_date: str
    symbols: Tuple[str, ...]


def _resolve_marker(
    coll: Any,
    index_code: str,
    *,
    as_of_date: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Find the single snapshot marker doc to use (modern or legacy schema)."""
    projection = {"weight_trade_date": 1, "update_date": 1, "_id": 0}
    as_of = normalize_yyyymmdd(as_of_date) if as_of_date else ""

    if as_of:
        # Prefer modern docs (with weight_trade_date) at/on-before the as-of date.
        doc = coll.find_one(
            {"index_code": index_code, "weight_trade_date": {"$lte": as_of}},
            projection,
            sort=[("weight_trade_date", -1), ("update_date", -1)],
        )
        if doc:
            return doc
        # Fallback: legacy docs lacking weight_trade_date, keyed by update_date only.
        return coll.find_one(
            {"index_code": index_code, "update_date": {"$lte": as_of}},
            projection,
            sort=[("update_date", -1)],
        )

    # Latest snapshot: modern docs sort by weight_trade_date; legacy fall back to update_date.
    doc = coll.find_one(
        {"index_code": index_code},
        projection,
        sort=[("weight_trade_date", -1), ("update_date", -1)],
    )
    if doc and normalize_yyyymmdd(doc.get("weight_trade_date")):
        return doc
    # Either no doc, or the top doc has no weight_trade_date -> resolve by update_date.
    legacy = coll.find_one(
        {"index_code": index_code},
        projection,
        sort=[("update_date", -1)],
    )
    return legacy or doc


def _marker_filter(index_code: str, marker: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Build a snapshot filter using only the key fields present on the marker."""
    wt = normalize_yyyymmdd(marker.get("weight_trade_date"))
    ud = normalize_yyyymmdd(marker.get("update_date"))
    if not wt and not ud:
        return None
    query: Dict[str, Any] = {"index_code": index_code}
    if wt:
        query["weight_trade_date"] = wt
    if ud:
        query["update_date"] = ud
    return query


def resolve_snapshot_keys(
    coll: Any,
    index_code: str,
    *,
    as_of_date: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """Return ``(weight_trade_date, update_date)`` for the snapshot to use.

    Either element may be empty string for legacy docs that only carry one of the
    two keys; ``(None, None)`` means no snapshot was found.
    """
    marker = _resolve_marker(coll, index_code, as_of_date=as_of_date)
    if not marker:
        return None, None
    wt = normalize_yyyymmdd(marker.get("weight_trade_date"))
    ud = normalize_yyyymmdd(marker.get("update_date"))
    if not wt and not ud:
        return None, None
    return (wt or None), (ud or None)


def _symbols_for_filter(coll: Any, query: Dict[str, Any]) -> List[str]:
    try:
        raw = coll.distinct("symbol", query)
    except Exception:
        raw = [row.get("symbol") for row in coll.find(query, {"symbol": 1, "_id": 0})]

    out: List[str] = []
    seen: Set[str] = set()
    for item in raw:
        sym = str(item or "").strip()
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return sorted(out)


def snapshot_symbols_for_keys(
    coll: Any,
    index_code: str,
    *,
    weight_trade_date: Optional[str],
    update_date: Optional[str],
) -> List[str]:
    query = _marker_filter(
        index_code,
        {"weight_trade_date": weight_trade_date, "update_date": update_date},
    )
    if query is None:
        return []
    return _symbols_for_filter(coll, query)


def load_snapshot(
    coll: Any,
    index_code: str,
    *,
    as_of_date: Optional[str] = None,
) -> Optional[IndexConstituentSnapshot]:
    marker = _resolve_marker(coll, index_code, as_of_date=as_of_date)
    if not marker:
        return None
    query = _marker_filter(index_code, marker)
    if query is None:
        return None
    symbols = _symbols_for_filter(coll, query)
    return IndexConstituentSnapshot(
        index_code=index_code,
        weight_trade_date=normalize_yyyymmdd(marker.get("weight_trade_date")),
        update_date=normalize_yyyymmdd(marker.get("update_date")),
        symbols=tuple(symbols),
    )


def latest_snapshot_symbols(coll: Any, index_code: str) -> List[str]:
    snapshot = load_snapshot(coll, index_code)
    return list(snapshot.symbols) if snapshot else []


def asof_snapshot_symbols(coll: Any, index_code: str, as_of_date: str) -> List[str]:
    snapshot = load_snapshot(coll, index_code, as_of_date=as_of_date)
    return list(snapshot.symbols) if snapshot else []


def build_membership_map(
    coll: Any,
    index_codes: Sequence[str],
    *,
    as_of_date: Optional[str] = None,
) -> Dict[str, List[str]]:
    """symbol -> sorted list of index_code labels for one as-of date (or latest)."""
    membership: Dict[str, Set[str]] = {}
    for ic in index_codes:
        code = str(ic or "").strip()
        if not code:
            continue
        symbols = asof_snapshot_symbols(coll, code, as_of_date) if as_of_date else latest_snapshot_symbols(coll, code)
        for symbol in symbols:
            membership.setdefault(symbol, set()).add(code)
    return {symbol: sorted(labels) for symbol, labels in membership.items()}


def snapshot_symbol_filter(
    coll: Any,
    index_code: str,
    *,
    as_of_date: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return the Mongo filter identifying the resolved snapshot (or None)."""
    marker = _resolve_marker(coll, index_code, as_of_date=as_of_date)
    if not marker:
        return None
    return _marker_filter(index_code, marker)


__all__ = [
    "IndexConstituentSnapshot",
    "asof_snapshot_symbols",
    "build_membership_map",
    "latest_snapshot_symbols",
    "load_snapshot",
    "normalize_yyyymmdd",
    "resolve_snapshot_keys",
    "snapshot_symbol_filter",
    "snapshot_symbols_for_keys",
]
