"""Symbol/industry opportunity finding ledger (lifecycle, global shared).

Opportunity findings intentionally live outside the risk ledger.  They share
evidence/provenance patterns with risk findings, but use a separate lifecycle:
active opportunities can be realized, invalidated, or expire.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from .mongo_context import get_db

SYMBOL_OPPORTUNITY_FINDINGS_COL = "symbol_opportunity_findings"
INDUSTRY_OPPORTUNITY_FINDINGS_COL = "industry_opportunity_findings"

OPPORTUNITY_CATEGORIES: Tuple[str, ...] = (
    "policy_tailwind",
    "demand_growth",
    "earnings_catalyst",
    "valuation_upside",
    "industry_momentum",
    "event_catalyst",
    "other",
)

OPPORTUNITY_CATEGORY_ALIASES: Dict[str, str] = {
    "政策": "policy_tailwind",
    "国产替代": "policy_tailwind",
    "补贴": "policy_tailwind",
    "policy": "policy_tailwind",
    "需求": "demand_growth",
    "订单": "demand_growth",
    "算力": "demand_growth",
    "demand": "demand_growth",
    "业绩": "earnings_catalyst",
    "预增": "earnings_catalyst",
    "earnings": "earnings_catalyst",
    "估值": "valuation_upside",
    "修复": "valuation_upside",
    "valuation": "valuation_upside",
    "行业": "industry_momentum",
    "景气": "industry_momentum",
    "主线": "industry_momentum",
    "momentum": "industry_momentum",
    "并购": "event_catalyst",
    "新品": "event_catalyst",
    "事件": "event_catalyst",
    "event": "event_catalyst",
}

_STRENGTH_ORDER = {"none": 0, "low": 1, "medium": 2, "high": 3}
_VALID_MODES = frozenset({"event", "metric", "dated"})
_VALID_OUTCOMES = frozenset({"realized", "invalidated", "expired"})


def _utcnow() -> datetime:
    return datetime.utcnow()


def _today_yyyymmdd() -> str:
    return _utcnow().strftime("%Y%m%d")


def normalize_opportunity_category(raw: Optional[str]) -> str:
    """Map free-text category to a canonical opportunity bucket."""
    if not raw:
        return "other"
    text = str(raw).strip().lower()
    if not text:
        return "other"
    if text in OPPORTUNITY_CATEGORIES:
        return text
    for alias, canonical in OPPORTUNITY_CATEGORY_ALIASES.items():
        if alias.lower() in text or text in alias.lower():
            return canonical
    return "other"


def normalize_opportunity_subject(raw: Optional[str]) -> str:
    if not raw:
        return "unspecified"
    text = str(raw).strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_\u4e00-\u9fff]+", "", text)
    return text[:120] or "unspecified"


def make_opportunity_finding_key(category: Optional[str], subject: Optional[str]) -> str:
    return f"{normalize_opportunity_category(category)}:{normalize_opportunity_subject(subject)}"


def _strength(value: Any, default: str = "none") -> str:
    strength = str(value or default).strip().lower()
    return strength if strength in _STRENGTH_ORDER else default


def _max_strength(values: Iterable[str]) -> str:
    result = "none"
    for value in values:
        strength = _strength(value)
        if _STRENGTH_ORDER[strength] > _STRENGTH_ORDER[result]:
            result = strength
    return result


def _is_active_doc(doc: Dict[str, Any], *, today: Optional[str] = None) -> bool:
    if doc.get("status") != "active":
        return False
    mode = str(doc.get("resolution_mode") or "event")
    if mode == "dated":
        expires = str(doc.get("expires_as_of") or "")
        if expires and expires < (today or _today_yyyymmdd()):
            return False
    return True


def _evidence_digest(evidence: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    digest = []
    for row in evidence[:3]:
        digest.append(
            {
                "source_title": row.get("source_title") or "",
                "source_url": row.get("source_url") or "",
                "source_date": row.get("source_date") or "",
            }
        )
    return digest


def _source_run_entry(source: Dict[str, Any], finding: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    finding = finding or {}
    evidence = finding.get("evidence") or []
    return {
        "discovered_by": source.get("discovered_by"),
        "engine_version": source.get("engine_version"),
        "run_id": source.get("run_id"),
        "plan_id": source.get("plan_id"),
        "task_id": source.get("task_id"),
        "user_id": source.get("user_id"),
        "analyzed_at": source.get("analyzed_at") or _utcnow(),
        "strength": finding.get("strength"),
        "summary": finding.get("summary") or finding.get("title"),
        "evidence_digest": _evidence_digest(evidence) if isinstance(evidence, list) else [],
    }


class SymbolOpportunityLedgerAccess:
    """Read/write access to the global symbol/industry opportunity ledger."""

    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.symbol_coll = self.db[SYMBOL_OPPORTUNITY_FINDINGS_COL]
        self.industry_coll = self.db[INDUSTRY_OPPORTUNITY_FINDINGS_COL]

    normalize_category = staticmethod(normalize_opportunity_category)
    normalize_subject = staticmethod(normalize_opportunity_subject)
    make_finding_key = staticmethod(make_opportunity_finding_key)

    def apply_assessment(
        self,
        symbol: str,
        *,
        new_findings: Sequence[Dict[str, Any]],
        confirmed: Sequence[str],
        suggested_closures: Sequence[Dict[str, Any]],
        assessment: Dict[str, Any],
        source: Dict[str, Any],
    ) -> Dict[str, int]:
        sym = str(symbol or "").strip()
        if not sym:
            raise ValueError("symbol is required")

        as_of = str(assessment.get("as_of_date") or _today_yyyymmdd())
        now = _utcnow()
        touched_keys: Set[str] = set()
        stats = {"inserted": 0, "confirmed": 0, "suggested": 0, "expired": 0}

        for finding in new_findings:
            run_entry = _source_run_entry(source, finding)
            key = self._upsert_new_finding(
                coll=self.symbol_coll,
                filter_base={"symbol": sym},
                scope_field="symbol",
                scope_value=sym,
                finding=finding,
                as_of=as_of,
                source=source,
                run_entry=run_entry,
                now=now,
            )
            touched_keys.add(key)
            stats["inserted"] += 1

        run_entry = _source_run_entry(source)
        for finding_key in confirmed:
            if self._confirm_finding(
                coll=self.symbol_coll,
                scope_filter={"symbol": sym},
                finding_key=str(finding_key),
                as_of=as_of,
                run_entry=run_entry,
                now=now,
            ):
                touched_keys.add(str(finding_key))
                stats["confirmed"] += 1

        for suggestion in suggested_closures:
            key = str(suggestion.get("finding_key") or "")
            if not key:
                continue
            if self._suggest_closure(
                coll=self.symbol_coll,
                scope_filter={"symbol": sym},
                finding_key=key,
                suggestion=suggestion,
                run_entry=run_entry,
                now=now,
            ):
                stats["suggested"] += 1

        stats["expired"] += self._lazy_close_expired(self.symbol_coll, {"symbol": sym}, now=now)
        return stats

    def apply_industry_assessment(
        self,
        industry: str,
        *,
        new_findings: Sequence[Dict[str, Any]],
        confirmed: Sequence[str],
        suggested_closures: Sequence[Dict[str, Any]],
        assessment: Dict[str, Any],
        source: Dict[str, Any],
    ) -> Dict[str, int]:
        ind = str(industry or "").strip()
        if not ind:
            raise ValueError("industry is required")

        as_of = str(assessment.get("as_of_date") or _today_yyyymmdd())
        now = _utcnow()
        stats = {"inserted": 0, "confirmed": 0, "suggested": 0, "expired": 0}

        for finding in new_findings:
            run_entry = _source_run_entry(source, finding)
            self._upsert_new_finding(
                coll=self.industry_coll,
                filter_base={"industry": ind},
                scope_field="industry",
                scope_value=ind,
                finding=finding,
                as_of=as_of,
                source=source,
                run_entry=run_entry,
                now=now,
            )
            stats["inserted"] += 1

        run_entry = _source_run_entry(source)
        for finding_key in confirmed:
            if self._confirm_finding(
                coll=self.industry_coll,
                scope_filter={"industry": ind},
                finding_key=str(finding_key),
                as_of=as_of,
                run_entry=run_entry,
                now=now,
            ):
                stats["confirmed"] += 1

        for suggestion in suggested_closures:
            key = str(suggestion.get("finding_key") or "")
            if not key:
                continue
            if self._suggest_closure(
                coll=self.industry_coll,
                scope_filter={"industry": ind},
                finding_key=key,
                suggestion=suggestion,
                run_entry=run_entry,
                now=now,
            ):
                stats["suggested"] += 1

        stats["expired"] += self._lazy_close_expired(self.industry_coll, {"industry": ind}, now=now)
        return stats

    def add_manual_finding(
        self,
        symbol: str,
        *,
        strength: str,
        summary: str,
        detail: str = "",
        created_by: str,
        category: Optional[str] = None,
        subject: Optional[str] = None,
        resolution_mode: str = "event",
        evidence: Optional[List[Dict[str, Any]]] = None,
        as_of_date: Optional[str] = None,
    ) -> str:
        sym = str(symbol or "").strip()
        if not sym:
            raise ValueError("symbol is required")
        mode = str(resolution_mode or "event")
        if mode not in _VALID_MODES:
            mode = "event"
        cat_raw = category or summary or "manual"
        subj = subject or summary or "manual"
        finding_key = make_opportunity_finding_key(cat_raw, subj)
        now = _utcnow()
        as_of = as_of_date or _today_yyyymmdd()
        strength_s = _strength(strength, "medium")
        doc = {
            "symbol": sym,
            "finding_key": finding_key,
            "discovered_by": "manual",
            "resolution_mode": mode,
            "category": normalize_opportunity_category(cat_raw),
            "category_raw": cat_raw,
            "subject": normalize_opportunity_subject(subj),
            "strength": strength_s,
            "strength_history": [{"strength": strength_s, "at": now, "run_id": None}],
            "summary": summary,
            "detail": detail,
            "evidence": evidence or [],
            "status": "active",
            "outcome": None,
            "first_detected_at": now,
            "first_detected_as_of": as_of,
            "last_confirmed_at": now,
            "last_confirmed_as_of": as_of,
            "created_by": created_by,
            "source_runs": [
                {
                    "discovered_by": "manual",
                    "engine_version": None,
                    "run_id": None,
                    "plan_id": None,
                    "task_id": None,
                    "user_id": created_by,
                    "analyzed_at": now,
                    "strength": strength_s,
                    "summary": summary,
                    "evidence_digest": _evidence_digest(evidence or []),
                }
            ],
        }
        self.symbol_coll.update_one(
            {"symbol": sym, "finding_key": finding_key},
            {"$setOnInsert": doc},
            upsert=True,
        )
        return finding_key

    def realize_finding(
        self,
        symbol: str,
        finding_key: str,
        *,
        reason: str,
        closed_by: str,
    ) -> bool:
        return self._close_finding(
            coll=self.symbol_coll,
            scope_filter={"symbol": str(symbol).strip()},
            finding_key=finding_key,
            outcome="realized",
            reason=reason or "realized",
            closed_by=closed_by,
        )

    def invalidate_finding(
        self,
        symbol: str,
        finding_key: str,
        *,
        reason: str,
        closed_by: str,
        spawn_risk: bool = False,
        risk_ledger: Any = None,
    ) -> bool:
        spawned_key = None
        if spawn_risk and risk_ledger is not None:
            doc = self.symbol_coll.find_one(
                {"symbol": str(symbol).strip(), "finding_key": finding_key},
                {"_id": 0},
            )
            if doc:
                spawned_key = risk_ledger.add_manual_finding(
                    str(symbol).strip(),
                    severity="low",
                    summary=f"机会落空：{doc.get('summary') or finding_key}",
                    detail=reason or doc.get("detail") or "",
                    created_by=closed_by,
                    category="sentiment_incident",
                    subject=f"机会落空:{doc.get('subject') or finding_key}",
                )
                risk_ledger.symbol_coll.update_one(
                    {"symbol": str(symbol).strip(), "finding_key": spawned_key},
                    {
                        "$set": {
                            "discovered_by": "opportunity_invalidation",
                            "origin_opportunity_finding_key": finding_key,
                        }
                    },
                )
        return self._close_finding(
            coll=self.symbol_coll,
            scope_filter={"symbol": str(symbol).strip()},
            finding_key=finding_key,
            outcome="invalidated",
            reason=reason or "invalidated",
            closed_by=closed_by,
            extra_set={"spawned_risk_finding_key": spawned_key} if spawned_key else None,
        )

    def reopen_finding(
        self,
        symbol: str,
        finding_key: str,
        *,
        reopened_by: str,
        reason: str = "",
    ) -> bool:
        now = _utcnow()
        result = self.symbol_coll.update_one(
            {"symbol": str(symbol).strip(), "finding_key": finding_key, "status": "closed"},
            {
                "$set": {
                    "status": "active",
                    "outcome": None,
                    "reopened_at": now,
                    "reopened_by": reopened_by,
                    "reopen_reason": reason,
                    "suggested_closure": None,
                },
                "$unset": {
                    "closed_at": "",
                    "closed_as_of": "",
                    "closed_reason": "",
                    "closed_by": "",
                    "closed_via": "",
                },
            },
        )
        return result.modified_count > 0

    def get_active_for_symbols(
        self,
        symbols: Sequence[str],
        *,
        today: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        sym_list = [str(s).strip() for s in symbols if s]
        if not sym_list:
            return {}
        today_s = today or _today_yyyymmdd()
        self._lazy_close_expired_for_symbols(sym_list, today=today_s)
        rows = list(self.symbol_coll.find({"symbol": {"$in": sym_list}, "status": "active"}, {"_id": 0}))
        grouped: Dict[str, List[Dict[str, Any]]] = {s: [] for s in sym_list}
        for row in rows:
            if not _is_active_doc(row, today=today_s):
                continue
            sym = str(row.get("symbol") or "")
            grouped.setdefault(sym, []).append(row)
        return grouped

    def get_active_for_industries(
        self,
        industries: Sequence[str],
        *,
        today: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        ind_list = [str(i).strip() for i in industries if i]
        if not ind_list:
            return {}
        today_s = today or _today_yyyymmdd()
        rows = list(self.industry_coll.find({"industry": {"$in": ind_list}, "status": "active"}, {"_id": 0}))
        grouped: Dict[str, List[Dict[str, Any]]] = {i: [] for i in ind_list}
        for row in rows:
            if not _is_active_doc(row, today=today_s):
                continue
            ind = str(row.get("industry") or "")
            grouped.setdefault(ind, []).append(row)
        return grouped

    def get_active_snapshot(self, symbols: Sequence[str], *, today: Optional[str] = None) -> List[Dict[str, Any]]:
        grouped = self.get_active_for_symbols(symbols, today=today)
        return [
            {"symbol": sym, "active_findings": findings}
            for sym, findings in grouped.items()
            if findings
        ]

    def _upsert_new_finding(
        self,
        *,
        coll,
        filter_base: Dict[str, Any],
        scope_field: str,
        scope_value: str,
        finding: Dict[str, Any],
        as_of: str,
        source: Dict[str, Any],
        run_entry: Dict[str, Any],
        now: datetime,
    ) -> str:
        cat_raw = finding.get("category_raw") or finding.get("category") or "other"
        subject_raw = finding.get("subject") or finding.get("summary") or finding.get("title") or ""
        category = normalize_opportunity_category(finding.get("category") or cat_raw)
        subject = normalize_opportunity_subject(subject_raw)
        finding_key = str(finding.get("finding_key") or make_opportunity_finding_key(category, subject))
        mode = str(finding.get("resolution_mode") or "event")
        if mode not in _VALID_MODES:
            mode = "event"
        strength_s = _strength(finding.get("strength"), "low")
        filt = {**filter_base, "finding_key": finding_key}
        existing = coll.find_one(filt)
        discovered_by = source.get("discovered_by") or "llm"

        history_entry = {"strength": strength_s, "at": now, "run_id": run_entry.get("run_id")}
        if existing:
            update: Dict[str, Any] = {
                "$set": {
                    "last_confirmed_at": now,
                    "last_confirmed_as_of": as_of,
                    "strength": strength_s,
                    "status": "active",
                    "outcome": None,
                },
                "$addToSet": {"source_runs": run_entry},
                "$push": {
                    "strength_history": {
                        "$each": [history_entry],
                        "$slice": -20,
                    }
                },
                "$unset": {
                    "closed_at": "",
                    "closed_as_of": "",
                    "closed_reason": "",
                    "closed_by": "",
                    "closed_via": "",
                },
            }
            if finding.get("summary"):
                update["$set"]["summary"] = finding.get("summary")
            if finding.get("detail"):
                update["$set"]["detail"] = finding.get("detail")
            if finding.get("evidence"):
                update["$set"]["evidence"] = finding.get("evidence")
            coll.update_one(filt, update)
            return finding_key

        doc = {
            scope_field: scope_value,
            "finding_key": finding_key,
            "discovered_by": discovered_by,
            "resolution_mode": mode,
            "expires_as_of": finding.get("expires_as_of"),
            "category": category,
            "category_raw": str(cat_raw),
            "subject": subject,
            "strength": strength_s,
            "strength_history": [history_entry],
            "summary": str(finding.get("summary") or finding.get("title") or ""),
            "detail": str(finding.get("detail") or ""),
            "evidence": finding.get("evidence") or [],
            "status": "active",
            "outcome": None,
            "first_detected_at": now,
            "first_detected_as_of": as_of,
            "last_confirmed_at": now,
            "last_confirmed_as_of": as_of,
            "source_runs": [run_entry],
        }
        coll.update_one(filt, {"$setOnInsert": doc}, upsert=True)
        return finding_key

    def _confirm_finding(
        self,
        *,
        coll,
        scope_filter: Dict[str, Any],
        finding_key: str,
        as_of: str,
        run_entry: Dict[str, Any],
        now: datetime,
    ) -> bool:
        result = coll.update_one(
            {**scope_filter, "finding_key": finding_key, "status": "active"},
            {
                "$set": {
                    "last_confirmed_at": now,
                    "last_confirmed_as_of": as_of,
                },
                "$addToSet": {"source_runs": run_entry},
            },
        )
        return result.matched_count > 0

    def _suggest_closure(
        self,
        *,
        coll,
        scope_filter: Dict[str, Any],
        finding_key: str,
        suggestion: Dict[str, Any],
        run_entry: Dict[str, Any],
        now: datetime,
    ) -> bool:
        result = coll.update_one(
            {**scope_filter, "finding_key": finding_key, "status": "active"},
            {
                "$set": {
                    "suggested_closure": {
                        "outcome": suggestion.get("outcome") or "invalidated",
                        "reason": suggestion.get("reason") or "",
                        "evidence": suggestion.get("evidence") or [],
                        "by": "llm",
                        "at": now,
                    },
                },
                "$addToSet": {"source_runs": run_entry},
            },
        )
        return result.matched_count > 0

    def _close_finding(
        self,
        *,
        coll,
        scope_filter: Dict[str, Any],
        finding_key: str,
        outcome: str,
        reason: str,
        closed_by: Optional[str],
        extra_set: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if outcome not in _VALID_OUTCOMES:
            raise ValueError(f"invalid outcome: {outcome}")
        now = _utcnow()
        set_fields = {
            "status": "closed",
            "outcome": outcome,
            "closed_at": now,
            "closed_as_of": _today_yyyymmdd(),
            "closed_reason": reason,
            "closed_by": closed_by,
            "closed_via": "human" if closed_by else "auto",
            "suggested_closure": None,
        }
        if extra_set:
            set_fields.update(extra_set)
        result = coll.update_one(
            {**scope_filter, "finding_key": finding_key, "status": "active"},
            {"$set": set_fields},
        )
        return result.modified_count > 0

    def _lazy_close_expired(self, coll, scope_filter: Dict[str, Any], *, now: datetime) -> int:
        today = _today_yyyymmdd()
        result = coll.update_many(
            {
                **scope_filter,
                "status": "active",
                "resolution_mode": "dated",
                "expires_as_of": {"$lt": today},
            },
            {
                "$set": {
                    "status": "closed",
                    "outcome": "expired",
                    "closed_at": now,
                    "closed_as_of": today,
                    "closed_reason": "expired",
                    "closed_by": None,
                    "closed_via": "auto",
                },
            },
        )
        return result.modified_count

    def _lazy_close_expired_for_symbols(self, symbols: Sequence[str], *, today: str) -> None:
        now = _utcnow()
        self.symbol_coll.update_many(
            {
                "symbol": {"$in": list(symbols)},
                "status": "active",
                "resolution_mode": "dated",
                "expires_as_of": {"$lt": today},
            },
            {
                "$set": {
                    "status": "closed",
                    "outcome": "expired",
                    "closed_at": now,
                    "closed_as_of": today,
                    "closed_reason": "expired",
                    "closed_by": None,
                    "closed_via": "auto",
                },
            },
        )


__all__ = [
    "INDUSTRY_OPPORTUNITY_FINDINGS_COL",
    "OPPORTUNITY_CATEGORIES",
    "OPPORTUNITY_CATEGORY_ALIASES",
    "SYMBOL_OPPORTUNITY_FINDINGS_COL",
    "SymbolOpportunityLedgerAccess",
    "make_opportunity_finding_key",
    "normalize_opportunity_category",
    "normalize_opportunity_subject",
]
