"""Mongo query helpers for ``finance.stock_scores`` index scope.

``stock_scores`` is unique on ``(symbol, score_date)``. ``index_codes`` is the
point-in-time wide-index membership tag for that day, not the scoring universe.
A date always has rows with ``[]`` / missing ``index_codes`` (in the shard-window
union, not a member that day). Callers that mean "this index's board" must
filter; callers that mean "this symbol's score that day" pass ``None`` explicitly.

``index_code`` is a required positional argument so "forgot to pass" and
"intentionally unscoped" do not look the same.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence, Union

IndexScope = Union[str, Sequence[str], None]


def score_scope_filter(index_code: IndexScope) -> Dict[str, Any]:
    """Return a Mongo filter fragment that scopes ``stock_scores`` to an index.

    * ``None`` — explicit full collection (no ``index_codes`` predicate).
    * non-empty ``str`` — ``{"index_codes": code}`` (array-contains).
    * non-empty sequence — ``{"index_codes": {"$in": codes}}`` (any-of).

    Does not match missing/``null`` ``index_codes`` when a code is given;
    Mongo equality / ``$in`` on an array field also does not match ``null``.
    """
    if index_code is None:
        return {}
    if isinstance(index_code, str):
        code = index_code.strip()
        if not code:
            raise ValueError("index_code must be a non-empty string, or None for an unscoped read")
        return {"index_codes": code}
    if not isinstance(index_code, (list, tuple, set)):
        # FastAPI Query() defaults leak through direct Python calls in tests.
        return {}
    codes = []
    seen = set()
    for raw in index_code:
        code = str(raw or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    if not codes:
        raise ValueError("index_code sequence must contain a non-empty code, or pass None")
    if len(codes) == 1:
        return {"index_codes": codes[0]}
    return {"index_codes": {"$in": codes}}


def merge_score_scope(query: Optional[Mapping[str, Any]], index_code: IndexScope) -> Dict[str, Any]:
    """Copy ``query`` and apply :func:`score_scope_filter`."""
    merged: Dict[str, Any] = dict(query or {})
    merged.update(score_scope_filter(index_code))
    return merged


def apply_score_scope(query: MutableMapping[str, Any], index_code: IndexScope) -> MutableMapping[str, Any]:
    """In-place variant of :func:`merge_score_scope`."""
    query.update(score_scope_filter(index_code))
    return query


def tagged_index_codes_filter() -> Dict[str, Any]:
    """Rows whose ``index_codes`` has at least one element.

    Excludes missing/``null`` and ``[]``. Used when an unscoped "market"
    snapshot must not be pinned to a leftover untagged partial write.
    """
    return {"index_codes.0": {"$exists": True}}


def row_covers_score_scope(index_codes: Any, index_code: IndexScope) -> bool:
    """True when stored ``index_codes`` satisfies the requested scope."""
    if index_code is None:
        return True
    labels = _as_code_list(index_codes)
    if isinstance(index_code, str):
        return index_code.strip() in labels
    wanted = [str(raw).strip() for raw in index_code if str(raw).strip()]
    if not wanted:
        return True
    return all(code in labels for code in wanted)


def _as_code_list(value: Any) -> list:
    if not value:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, Iterable):
        out = []
        for item in value:
            text = str(item).strip()
            if text:
                out.append(text)
        return out
    return []
