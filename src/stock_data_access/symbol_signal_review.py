"""LLM signal review stamps for symbol-level risk/opportunity reviews.

The finding ledgers only store positive discoveries.  This collection records
that a symbol was reviewed even when no risk or opportunity was found, so
enqueue cooldown logic can rely on review coverage instead of active findings.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Sequence

from .mongo_context import get_db

SYMBOL_LLM_SIGNAL_REVIEWS_COL = "symbol_llm_signal_reviews"


def _utcnow() -> datetime:
    return datetime.utcnow()


class SymbolLlmSignalReviewAccess:
    """Read/write access for symbol LLM signal review stamps."""

    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.coll = self.db[SYMBOL_LLM_SIGNAL_REVIEWS_COL]

    def record_symbol_reviews(
        self,
        symbols: Sequence[str],
        *,
        industry: Optional[str] = None,
        reviewed_at: Optional[datetime] = None,
        run_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        task_id: Optional[str] = None,
        user_id: Optional[str] = None,
        dimensions: Sequence[str] = ("risk", "opportunity"),
        result_counts_by_symbol: Optional[Mapping[str, Mapping[str, int]]] = None,
    ) -> int:
        """Upsert one review stamp per symbol and return the number touched."""
        now = reviewed_at or _utcnow()
        dims = sorted({str(dim) for dim in dimensions if dim})
        counts_by_symbol = result_counts_by_symbol or {}
        touched = 0

        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol:
                continue
            counts = dict(counts_by_symbol.get(symbol) or {})
            self.coll.update_one(
                {"symbol": symbol},
                {
                    "$set": {
                        "symbol": symbol,
                        "industry": industry,
                        "reviewed_at": now,
                        "run_id": run_id,
                        "plan_id": plan_id,
                        "task_id": task_id,
                        "user_id": user_id,
                        "dimensions": dims,
                        "result_counts": {
                            "risk": int(counts.get("risk") or 0),
                            "opportunity": int(counts.get("opportunity") or 0),
                        },
                    },
                    "$push": {
                        "review_history": {
                            "$each": [
                                {
                                    "reviewed_at": now,
                                    "run_id": run_id,
                                    "plan_id": plan_id,
                                    "task_id": task_id,
                                    "dimensions": dims,
                                    "result_counts": {
                                        "risk": int(counts.get("risk") or 0),
                                        "opportunity": int(counts.get("opportunity") or 0),
                                    },
                                }
                            ],
                            "$slice": -20,
                        }
                    },
                },
                upsert=True,
            )
            touched += 1

        return touched

    def get_latest_reviews(self, symbols: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        sym_list = [str(symbol).strip() for symbol in symbols if symbol]
        if not sym_list:
            return {}
        rows = self.coll.find({"symbol": {"$in": sym_list}}, {"_id": 0})
        return {str(row.get("symbol") or ""): row for row in rows if row.get("symbol")}


__all__ = [
    "SYMBOL_LLM_SIGNAL_REVIEWS_COL",
    "SymbolLlmSignalReviewAccess",
]
