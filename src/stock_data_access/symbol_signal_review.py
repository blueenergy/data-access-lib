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

    @staticmethod
    def _dimensions(dimensions: Sequence[str]) -> Sequence[str]:
        return sorted({str(dim) for dim in dimensions if dim})

    @staticmethod
    def _counts_for_dimensions(counts: Mapping[str, Any], dimensions: Sequence[str]) -> Dict[str, int]:
        return {str(dim): int(counts.get(str(dim)) or 0) for dim in dimensions}

    def _push_history(self, event: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "review_history": {
                "$each": [event],
                "$slice": -20,
            }
        }

    def record_analysis(
        self,
        symbols: Sequence[str],
        *,
        industry: Optional[str] = None,
        analyzed_at: Optional[datetime] = None,
        run_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        task_id: Optional[str] = None,
        user_id: Optional[str] = None,
        dimensions: Sequence[str] = ("risk", "opportunity"),
        result_counts_by_symbol: Optional[Mapping[str, Mapping[str, int]]] = None,
        fingerprints_by_symbol: Optional[Mapping[str, Mapping[str, Any]]] = None,
        prompt_version: Optional[str] = None,
    ) -> int:
        """Record a successful LLM analysis for each symbol."""
        now = analyzed_at or _utcnow()
        dims = self._dimensions(dimensions)
        counts_by_symbol = result_counts_by_symbol or {}
        fp_by_symbol = fingerprints_by_symbol or {}
        touched = 0

        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol:
                continue
            counts = dict(counts_by_symbol.get(symbol) or {})
            fps = dict(fp_by_symbol.get(symbol) or {})
            result_counts = self._counts_for_dimensions(counts, dims)
            set_doc = {
                "symbol": symbol,
                "industry": industry,
                "reviewed_at": now,  # legacy alias for analyzed_at
                "checked_at": now,
                "analyzed_at": now,
                "run_id": run_id,
                "plan_id": plan_id,
                "task_id": task_id,
                "user_id": user_id,
                "dimensions": list(dims),
                "result_counts": result_counts,
                "last_run_status": "analyzed",
                "last_skip_reason": None,
                "prompt_version": prompt_version,
            }
            for key in (
                "symbol_fingerprint",
                "sector_fingerprint",
                "combined_fingerprint",
                "evidence_count",
                "latest_evidence_at",
                "symbol_evidence_count",
                "sector_evidence_count",
            ):
                if key in fps:
                    set_doc[key] = fps.get(key)
            event = dict(set_doc)
            self.coll.update_one(
                {"symbol": symbol},
                {
                    "$set": set_doc,
                    "$push": self._push_history(event),
                },
                upsert=True,
            )
            touched += 1

        return touched

    def record_skip(
        self,
        symbols: Sequence[str],
        *,
        industry: Optional[str] = None,
        checked_at: Optional[datetime] = None,
        run_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        task_id: Optional[str] = None,
        user_id: Optional[str] = None,
        dimensions: Sequence[str] = ("risk", "opportunity"),
        fingerprints_by_symbol: Optional[Mapping[str, Mapping[str, Any]]] = None,
        reason: str = "unchanged_evidence",
    ) -> int:
        """Record that evidence was checked but LLM analysis was skipped."""
        now = checked_at or _utcnow()
        dims = self._dimensions(dimensions)
        fp_by_symbol = fingerprints_by_symbol or {}
        touched = 0
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol:
                continue
            fps = dict(fp_by_symbol.get(symbol) or {})
            set_doc = {
                "symbol": symbol,
                "industry": industry,
                "checked_at": now,
                "run_id": run_id,
                "plan_id": plan_id,
                "task_id": task_id,
                "user_id": user_id,
                "dimensions": list(dims),
                "last_run_status": "skipped_unchanged",
                "last_skip_reason": reason,
            }
            for key in (
                "evidence_count",
                "latest_evidence_at",
                "symbol_evidence_count",
                "sector_evidence_count",
            ):
                if key in fps:
                    set_doc[key] = fps.get(key)
            event = dict(set_doc)
            event["reviewed_at"] = now
            self.coll.update_one(
                {"symbol": symbol},
                {
                    "$set": set_doc,
                    "$push": self._push_history(event),
                },
                upsert=True,
            )
            touched += 1
        return touched

    def record_parse_error(
        self,
        symbols: Sequence[str],
        *,
        industry: Optional[str] = None,
        checked_at: Optional[datetime] = None,
        run_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        task_id: Optional[str] = None,
        user_id: Optional[str] = None,
        dimensions: Sequence[str] = ("risk", "opportunity"),
        error_detail: Optional[str] = None,
    ) -> int:
        """Record an attempted review that failed before valid ledger writes."""
        return self.record_failure(
            symbols,
            industry=industry,
            checked_at=checked_at,
            run_id=run_id,
            plan_id=plan_id,
            task_id=task_id,
            user_id=user_id,
            dimensions=dimensions,
            status="parse_error",
            error_detail=error_detail,
        )

    def record_failure(
        self,
        symbols: Sequence[str],
        *,
        industry: Optional[str] = None,
        checked_at: Optional[datetime] = None,
        run_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        task_id: Optional[str] = None,
        user_id: Optional[str] = None,
        dimensions: Sequence[str] = ("risk", "opportunity"),
        status: str = "failed",
        error_detail: Optional[str] = None,
    ) -> int:
        """Record an attempted review that failed before valid ledger writes."""
        now = checked_at or _utcnow()
        dims = self._dimensions(dimensions)
        safe_status = status if status in {"failed", "parse_error"} else "failed"
        touched = 0
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol:
                continue
            set_doc = {
                "symbol": symbol,
                "industry": industry,
                "checked_at": now,
                "run_id": run_id,
                "plan_id": plan_id,
                "task_id": task_id,
                "user_id": user_id,
                "dimensions": list(dims),
                "last_run_status": safe_status,
                "last_error_detail": error_detail,
            }
            event = dict(set_doc)
            event["reviewed_at"] = now
            self.coll.update_one(
                {"symbol": symbol},
                {
                    "$set": set_doc,
                    "$push": self._push_history(event),
                },
                upsert=True,
            )
            touched += 1
        return touched

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
        """Backward-compatible alias for a successful analysis stamp."""
        return self.record_analysis(
            symbols,
            industry=industry,
            analyzed_at=reviewed_at,
            run_id=run_id,
            plan_id=plan_id,
            task_id=task_id,
            user_id=user_id,
            dimensions=dimensions,
            result_counts_by_symbol=result_counts_by_symbol,
        )

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
