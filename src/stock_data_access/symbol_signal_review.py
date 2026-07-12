"""Signal review checkpoints for LLM and deterministic rule engines.

Finding ledgers remain the source of truth for lifecycle state. This collection
records per-scope fingerprints, engine metadata, completeness and result counts
needed to decide whether external LLM or internal rule analysis should rerun.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Sequence

from .mongo_context import get_db

SYMBOL_LLM_SIGNAL_REVIEWS_COL = "symbol_llm_signal_reviews"
DEFAULT_REVIEW_SCOPE = "external_event"


def _utcnow() -> datetime:
    return datetime.utcnow()


class SymbolLlmSignalReviewAccess:
    """Backward-compatible access for symbol signal review stamps."""

    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.coll = self.db[SYMBOL_LLM_SIGNAL_REVIEWS_COL]

    @staticmethod
    def _review_scope(review_scope: Optional[str]) -> str:
        return str(review_scope or DEFAULT_REVIEW_SCOPE).strip() or DEFAULT_REVIEW_SCOPE

    @staticmethod
    def _checkpoint_filter(symbol: str, review_scope: str) -> Dict[str, Any]:
        return {"symbol": symbol, "review_scope": review_scope}

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
        review_scope: str = DEFAULT_REVIEW_SCOPE,
        engine_type: str = "llm",
        engine_version: Optional[str] = None,
        rule_config_hash: Optional[str] = None,
        input_completeness: Optional[str] = None,
    ) -> int:
        """Record a successful signal analysis for each symbol."""
        now = analyzed_at or _utcnow()
        fp_by_symbol = fingerprints_by_symbol or {}
        scope = self._review_scope(review_scope)
        touched = 0

        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol:
                continue
            fps = dict(fp_by_symbol.get(symbol) or {})
            set_doc = {
                "symbol": symbol,
                "review_scope": scope,
                "checked_at": now,
                "analyzed_at": now,
                "last_run_status": "analyzed",
                "prompt_version": prompt_version,
                "engine_type": str(engine_type or "llm"),
                "engine_version": engine_version,
                "dimensions_reviewed": [str(value) for value in dimensions],
            }
            counts = dict((result_counts_by_symbol or {}).get(symbol) or {})
            if counts:
                set_doc["result_counts"] = {
                    str(key): int(value) for key, value in counts.items()
                }
            if rule_config_hash is not None:
                set_doc["rule_config_hash"] = rule_config_hash
            if input_completeness is not None:
                set_doc["input_completeness"] = input_completeness
            for key, value in {
                "industry": industry,
                "run_id": run_id,
                "plan_id": plan_id,
                "task_id": task_id,
                "user_id": user_id,
            }.items():
                if value is not None:
                    set_doc[key] = value
            for key in (
                "symbol_fingerprint",
                "sector_fingerprint",
                "combined_fingerprint",
                "internal_fingerprint",
                "evidence_count",
                "latest_evidence_at",
            ):
                if key in fps:
                    set_doc[key] = fps.get(key)
            self.coll.update_one(
                self._checkpoint_filter(symbol, scope),
                {"$set": set_doc},
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
        review_scope: str = DEFAULT_REVIEW_SCOPE,
        engine_type: Optional[str] = None,
        engine_version: Optional[str] = None,
        rule_config_hash: Optional[str] = None,
        input_completeness: Optional[str] = None,
    ) -> int:
        """Record that evidence was checked but LLM analysis was skipped."""
        now = checked_at or _utcnow()
        fp_by_symbol = fingerprints_by_symbol or {}
        scope = self._review_scope(review_scope)
        touched = 0
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol:
                continue
            fps = dict(fp_by_symbol.get(symbol) or {})
            set_doc = {
                "symbol": symbol,
                "review_scope": scope,
                "checked_at": now,
                "last_run_status": "skipped_unchanged",
                "last_skip_reason": reason,
            }
            for key, value in {
                "industry": industry,
                "run_id": run_id,
                "plan_id": plan_id,
                "task_id": task_id,
                "user_id": user_id,
                "engine_type": engine_type,
                "engine_version": engine_version,
                "rule_config_hash": rule_config_hash,
                "input_completeness": input_completeness,
            }.items():
                if value is not None:
                    set_doc[key] = value
            for key in (
                "internal_fingerprint",
                "evidence_count",
                "latest_evidence_at",
            ):
                if key in fps:
                    set_doc[key] = fps.get(key)
            self.coll.update_one(
                self._checkpoint_filter(symbol, scope),
                {"$set": set_doc},
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
        review_scope: str = DEFAULT_REVIEW_SCOPE,
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
            review_scope=review_scope,
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
        review_scope: str = DEFAULT_REVIEW_SCOPE,
    ) -> int:
        """Record an attempted review that failed before valid ledger writes."""
        now = checked_at or _utcnow()
        safe_status = status if status in {"failed", "parse_error"} else "failed"
        scope = self._review_scope(review_scope)
        touched = 0
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").strip()
            if not symbol:
                continue
            set_doc = {
                "symbol": symbol,
                "review_scope": scope,
                "checked_at": now,
                "last_run_status": safe_status,
            }
            if error_detail is not None:
                set_doc["last_error_detail"] = str(error_detail)
            for key, value in {
                "industry": industry,
                "run_id": run_id,
                "plan_id": plan_id,
                "task_id": task_id,
                "user_id": user_id,
                "dimensions_reviewed": [str(value) for value in dimensions],
            }.items():
                if value is not None:
                    set_doc[key] = value
            self.coll.update_one(
                self._checkpoint_filter(symbol, scope),
                {"$set": set_doc},
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
        review_scope: str = DEFAULT_REVIEW_SCOPE,
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
            review_scope=review_scope,
            engine_type="llm",
        )

    def get_latest_reviews(
        self,
        symbols: Sequence[str],
        *,
        review_scope: str = DEFAULT_REVIEW_SCOPE,
    ) -> Dict[str, Dict[str, Any]]:
        sym_list = [str(symbol).strip() for symbol in symbols if symbol]
        if not sym_list:
            return {}
        scope = self._review_scope(review_scope)
        rows = self.coll.find(
            {"symbol": {"$in": sym_list}, "review_scope": scope},
            {"_id": 0},
        )
        return {str(row.get("symbol") or ""): row for row in rows if row.get("symbol")}

    def reset_scope(
        self,
        review_scope: str,
        *,
        rule_config_hash: Optional[str] = None,
        dry_run: bool = True,
    ) -> int:
        """Count or delete checkpoints for one explicit review scope."""

        scope = self._review_scope(review_scope)
        query: Dict[str, Any] = {"review_scope": scope}
        if rule_config_hash is not None:
            config_hash = str(rule_config_hash).strip()
            if not config_hash:
                raise ValueError("rule_config_hash cannot be empty")
            query["rule_config_hash"] = config_hash
        count = int(self.coll.count_documents(query))
        if not dry_run and count:
            self.coll.delete_many(query)
        return count


SymbolSignalReviewAccess = SymbolLlmSignalReviewAccess


__all__ = [
    "DEFAULT_REVIEW_SCOPE",
    "SYMBOL_LLM_SIGNAL_REVIEWS_COL",
    "SymbolLlmSignalReviewAccess",
    "SymbolSignalReviewAccess",
]
