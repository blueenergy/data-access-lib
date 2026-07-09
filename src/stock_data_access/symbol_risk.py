"""Symbol/industry risk finding ledger (lifecycle, global shared).

Findings are keyed by ``(symbol, finding_key)`` or ``(industry, finding_key)``
without ``engine_version``.  Discovery provenance lives in ``source_runs``.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from .mongo_context import get_db

SYMBOL_RISK_FINDINGS_COL = "symbol_risk_findings"
INDUSTRY_RISK_FINDINGS_COL = "industry_risk_findings"

ENGINE_VERSION_LLM = "llm-v1"
ENGINE_VERSION_RULES = "rules-v2"

RISK_CATEGORIES: Tuple[str, ...] = (
    "financial_credit",
    "legal_compliance",
    "governance_integrity",
    "share_capital",
    "operating_fundamental",
    "valuation_technical",
    "industry_policy",
    "sentiment_incident",
    "other",
)

RISK_CATEGORY_ALIASES: Dict[str, str] = {
    "财务": "financial_credit",
    "财务信用": "financial_credit",
    "坏账": "financial_credit",
    "信用": "financial_credit",
    "financial": "financial_credit",
    "法律": "legal_compliance",
    "诉讼": "legal_compliance",
    "合规": "legal_compliance",
    "legal": "legal_compliance",
    "治理": "governance_integrity",
    "诚信": "governance_integrity",
    "governance": "governance_integrity",
    "股本": "share_capital",
    "解禁": "share_capital",
    "减持": "share_capital",
    "share": "share_capital",
    "经营": "operating_fundamental",
    "业绩": "operating_fundamental",
    "operating": "operating_fundamental",
    "估值": "valuation_technical",
    "技术面": "valuation_technical",
    "valuation": "valuation_technical",
    "行业": "industry_policy",
    "政策": "industry_policy",
    "industry": "industry_policy",
    "舆情": "sentiment_incident",
    "突发": "sentiment_incident",
    "sentiment": "sentiment_incident",
}

_SEVERITY_ORDER = {"none": 0, "low": 1, "medium": 2, "high": 3}
_VALID_MODES = frozenset({"event", "metric", "dated"})


def _utcnow() -> datetime:
    return datetime.utcnow()


def _today_yyyymmdd() -> str:
    return _utcnow().strftime("%Y%m%d")


def normalize_category(raw: Optional[str]) -> str:
    """Map free-text category to canonical bucket (soft enum)."""
    if not raw:
        return "other"
    text = str(raw).strip().lower()
    if not text:
        return "other"
    if text in RISK_CATEGORIES:
        return text
    for alias, canonical in RISK_CATEGORY_ALIASES.items():
        if alias.lower() in text or text in alias.lower():
            return canonical
    return "other"


def normalize_subject(raw: Optional[str]) -> str:
    """Normalize risk subject for stable finding_key components."""
    if not raw:
        return "unspecified"
    text = str(raw).strip().lower()
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^a-z0-9_\u4e00-\u9fff]+", "", text)
    text = text[:120] or "unspecified"
    return text


def make_finding_key(category: Optional[str], subject: Optional[str]) -> str:
    return f"{normalize_category(category)}:{normalize_subject(subject)}"


def _max_severity(values: Iterable[str]) -> str:
    result = "none"
    for value in values:
        sev = str(value or "none").strip().lower()
        if sev not in _SEVERITY_ORDER:
            sev = "none"
        if _SEVERITY_ORDER[sev] > _SEVERITY_ORDER[result]:
            result = sev
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


def _source_run_entry(source: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "discovered_by": source.get("discovered_by"),
        "engine_version": source.get("engine_version"),
        "run_id": source.get("run_id"),
        "plan_id": source.get("plan_id"),
        "task_id": source.get("task_id"),
        "user_id": source.get("user_id"),
        "analyzed_at": source.get("analyzed_at") or _utcnow(),
    }


class SymbolRiskLedgerAccess:
    """Read/write access to the global symbol/industry risk finding ledger."""

    def __init__(self, db=None):
        self.db = db if db is not None else get_db()
        self.symbol_coll = self.db[SYMBOL_RISK_FINDINGS_COL]
        self.industry_coll = self.db[INDUSTRY_RISK_FINDINGS_COL]

    # ------------------------------------------------------------------
    # Normalization helpers (module-level aliases for tests)
    # ------------------------------------------------------------------
    normalize_category = staticmethod(normalize_category)
    normalize_subject = staticmethod(normalize_subject)
    make_finding_key = staticmethod(make_finding_key)

    # ------------------------------------------------------------------
    # Symbol assessments
    # ------------------------------------------------------------------
    def apply_assessment(
        self,
        symbol: str,
        *,
        new_findings: Sequence[Dict[str, Any]],
        confirmed: Sequence[str],
        suggested_resolutions: Sequence[Dict[str, Any]],
        assessment: Dict[str, Any],
        source: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Apply one symbol assessment batch (lifecycle upsert)."""
        sym = str(symbol or "").strip()
        if not sym:
            raise ValueError("symbol is required")

        as_of = str(assessment.get("as_of_date") or _today_yyyymmdd())
        authoritative_modes = set(assessment.get("authoritative_for") or [])
        run_entry = _source_run_entry(source)
        now = _utcnow()

        touched_keys: Set[str] = set()
        stats = {"inserted": 0, "confirmed": 0, "suggested": 0, "auto_resolved": 0}

        for finding in new_findings:
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

        for suggestion in suggested_resolutions:
            key = str(suggestion.get("finding_key") or "")
            if not key:
                continue
            if self._suggest_resolution(
                coll=self.symbol_coll,
                scope_filter={"symbol": sym},
                finding_key=key,
                suggestion=suggestion,
                run_entry=run_entry,
                now=now,
            ):
                stats["suggested"] += 1

        stats["auto_resolved"] += self._auto_resolve_symbol(
            sym,
            authoritative_modes=authoritative_modes,
            touched_keys=touched_keys,
            as_of=as_of,
            now=now,
        )
        self._lazy_resolve_expired(self.symbol_coll, {"symbol": sym}, now=now)
        return stats

    def apply_industry_assessment(
        self,
        industry: str,
        *,
        new_findings: Sequence[Dict[str, Any]],
        confirmed: Sequence[str],
        suggested_resolutions: Sequence[Dict[str, Any]],
        assessment: Dict[str, Any],
        source: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Apply one industry assessment batch."""
        ind = str(industry or "").strip()
        if not ind:
            raise ValueError("industry is required")

        as_of = str(assessment.get("as_of_date") or _today_yyyymmdd())
        authoritative_modes = set(assessment.get("authoritative_for") or [])
        run_entry = _source_run_entry(source)
        now = _utcnow()
        touched_keys: Set[str] = set()
        stats = {"inserted": 0, "confirmed": 0, "suggested": 0, "auto_resolved": 0}

        for finding in new_findings:
            key = self._upsert_new_finding(
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
            touched_keys.add(key)
            stats["inserted"] += 1

        for finding_key in confirmed:
            if self._confirm_finding(
                coll=self.industry_coll,
                scope_filter={"industry": ind},
                finding_key=str(finding_key),
                as_of=as_of,
                run_entry=run_entry,
                now=now,
            ):
                touched_keys.add(str(finding_key))
                stats["confirmed"] += 1

        for suggestion in suggested_resolutions:
            key = str(suggestion.get("finding_key") or "")
            if not key:
                continue
            if self._suggest_resolution(
                coll=self.industry_coll,
                scope_filter={"industry": ind},
                finding_key=key,
                suggestion=suggestion,
                run_entry=run_entry,
                now=now,
            ):
                stats["suggested"] += 1

        stats["auto_resolved"] += self._auto_resolve_industry(
            ind,
            authoritative_modes=authoritative_modes,
            touched_keys=touched_keys,
            as_of=as_of,
            now=now,
        )
        self._lazy_resolve_expired(self.industry_coll, {"industry": ind}, now=now)
        return stats

    def add_manual_finding(
        self,
        symbol: str,
        *,
        severity: str,
        summary: str,
        detail: str = "",
        created_by: str,
        category: Optional[str] = None,
        subject: Optional[str] = None,
        resolution_mode: str = "event",
        evidence: Optional[List[Dict[str, Any]]] = None,
        as_of_date: Optional[str] = None,
    ) -> str:
        """Insert a manual symbol finding (discovered_by=manual)."""
        sym = str(symbol or "").strip()
        if not sym:
            raise ValueError("symbol is required")
        mode = str(resolution_mode or "event")
        if mode not in _VALID_MODES:
            mode = "event"
        cat_raw = category or summary or "manual"
        subj = subject or summary or "manual"
        finding_key = make_finding_key(cat_raw, subj)
        now = _utcnow()
        as_of = as_of_date or _today_yyyymmdd()
        doc = {
            "symbol": sym,
            "finding_key": finding_key,
            "discovered_by": "manual",
            "resolution_mode": mode,
            "category": normalize_category(cat_raw),
            "category_raw": cat_raw,
            "subject": normalize_subject(subj),
            "severity": severity,
            "summary": summary,
            "detail": detail,
            "evidence": evidence or [],
            "status": "active",
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
                }
            ],
        }
        self.symbol_coll.update_one(
            {"symbol": sym, "finding_key": finding_key},
            {"$setOnInsert": doc},
            upsert=True,
        )
        return finding_key

    def confirm_resolution(
        self,
        symbol: str,
        finding_key: str,
        *,
        confirmed_by: str,
    ) -> bool:
        """Confirm an LLM suggested resolution (event findings only)."""
        return self._resolve_finding(
            coll=self.symbol_coll,
            scope_filter={"symbol": str(symbol).strip()},
            finding_key=finding_key,
            reason="confirmed_llm_suggestion",
            resolved_by=confirmed_by,
            require_suggestion=True,
        )

    def resolve_finding(
        self,
        symbol: str,
        finding_key: str,
        *,
        reason: str,
        resolved_by: str,
    ) -> bool:
        """Manually resolve a symbol finding."""
        return self._resolve_finding(
            coll=self.symbol_coll,
            scope_filter={"symbol": str(symbol).strip()},
            finding_key=finding_key,
            reason=reason,
            resolved_by=resolved_by,
            require_suggestion=False,
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
            {
                "symbol": str(symbol).strip(),
                "finding_key": finding_key,
                "status": "resolved",
            },
            {
                "$set": {
                    "status": "active",
                    "reopened_at": now,
                    "reopened_by": reopened_by,
                    "reopen_reason": reason,
                    "suggested_resolution": None,
                },
                "$unset": {
                    "resolved_at": "",
                    "resolved_as_of": "",
                    "resolved_reason": "",
                    "resolved_by": "",
                    "resolved_via": "",
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
        """Return active findings grouped by symbol."""
        sym_list = [str(s).strip() for s in symbols if s]
        if not sym_list:
            return {}
        today_s = today or _today_yyyymmdd()
        self._lazy_resolve_expired_dated(sym_list, today=today_s)
        rows = list(
            self.symbol_coll.find(
                {"symbol": {"$in": sym_list}, "status": "active"},
                {"_id": 0},
            )
        )
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
        rows = list(
            self.industry_coll.find(
                {"industry": {"$in": ind_list}, "status": "active"},
                {"_id": 0},
            )
        )
        grouped: Dict[str, List[Dict[str, Any]]] = {i: [] for i in ind_list}
        for row in rows:
            if not _is_active_doc(row, today=today_s):
                continue
            ind = str(row.get("industry") or "")
            grouped.setdefault(ind, []).append(row)
        return grouped

    def get_active_snapshot(
        self,
        symbols: Sequence[str],
        *,
        today: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        grouped = self.get_active_for_symbols(symbols, today=today)
        out: List[Dict[str, Any]] = []
        for sym, findings in grouped.items():
            if findings:
                out.append({"symbol": sym, "active_findings": findings})
        return out

    def symbol_has_active_findings(self, symbol: str, *, today: Optional[str] = None) -> bool:
        grouped = self.get_active_for_symbols([symbol], today=today)
        return bool(grouped.get(str(symbol).strip()))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
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
        category = normalize_category(finding.get("category") or cat_raw)
        subject = normalize_subject(subject_raw)
        finding_key = str(finding.get("finding_key") or make_finding_key(category, subject))
        mode = str(finding.get("resolution_mode") or "event")
        if mode not in _VALID_MODES:
            mode = "event"

        filt = {**filter_base, "finding_key": finding_key}
        existing = coll.find_one(filt)
        discovered_by = source.get("discovered_by") or "llm"

        if existing:
            update: Dict[str, Any] = {
                "$set": {
                    "last_confirmed_at": now,
                    "last_confirmed_as_of": as_of,
                    "severity": _max_severity([existing.get("severity"), finding.get("severity")]),
                },
                "$addToSet": {"source_runs": run_entry},
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
            "severity": str(finding.get("severity") or "low"),
            "summary": str(finding.get("summary") or ""),
            "detail": str(finding.get("detail") or ""),
            "evidence": finding.get("evidence") or [],
            "status": "active",
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

    def _suggest_resolution(
        self,
        *,
        coll,
        scope_filter: Dict[str, Any],
        finding_key: str,
        suggestion: Dict[str, Any],
        run_entry: Dict[str, Any],
        now: datetime,
    ) -> bool:
        doc = coll.find_one({**scope_filter, "finding_key": finding_key, "status": "active"})
        if not doc:
            return False
        if str(doc.get("resolution_mode") or "event") != "event":
            return False
        result = coll.update_one(
            {**scope_filter, "finding_key": finding_key, "status": "active"},
            {
                "$set": {
                    "suggested_resolution": {
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

    def _resolve_finding(
        self,
        *,
        coll,
        scope_filter: Dict[str, Any],
        finding_key: str,
        reason: str,
        resolved_by: str,
        require_suggestion: bool,
    ) -> bool:
        filt = {**scope_filter, "finding_key": finding_key, "status": "active"}
        if require_suggestion:
            filt["suggested_resolution"] = {"$ne": None}
        now = _utcnow()
        result = coll.update_one(
            filt,
            {
                "$set": {
                    "status": "resolved",
                    "resolved_at": now,
                    "resolved_as_of": _today_yyyymmdd(),
                    "resolved_reason": reason,
                    "resolved_by": resolved_by,
                    "resolved_via": "human",
                    "suggested_resolution": None,
                },
            },
        )
        return result.modified_count > 0

    def _auto_resolve_symbol(
        self,
        symbol: str,
        *,
        authoritative_modes: Set[str],
        touched_keys: Set[str],
        as_of: str,
        now: datetime,
    ) -> int:
        if not authoritative_modes:
            return 0
        count = 0
        active = list(
            self.symbol_coll.find(
                {"symbol": symbol, "status": "active"},
                {"_id": 0, "finding_key": 1, "resolution_mode": 1},
            )
        )
        for doc in active:
            mode = str(doc.get("resolution_mode") or "event")
            if mode not in authoritative_modes:
                continue
            key = str(doc.get("finding_key") or "")
            if mode == "metric" and key not in touched_keys:
                if self._mark_auto_resolved(self.symbol_coll, {"symbol": symbol}, key, as_of, now):
                    count += 1
        return count

    def _auto_resolve_industry(
        self,
        industry: str,
        *,
        authoritative_modes: Set[str],
        touched_keys: Set[str],
        as_of: str,
        now: datetime,
    ) -> int:
        if not authoritative_modes:
            return 0
        count = 0
        active = list(
            self.industry_coll.find(
                {"industry": industry, "status": "active"},
                {"_id": 0, "finding_key": 1, "resolution_mode": 1},
            )
        )
        for doc in active:
            mode = str(doc.get("resolution_mode") or "event")
            if mode not in authoritative_modes:
                continue
            key = str(doc.get("finding_key") or "")
            if mode == "metric" and key not in touched_keys:
                if self._mark_auto_resolved(self.industry_coll, {"industry": industry}, key, as_of, now):
                    count += 1
        return count

    def _mark_auto_resolved(self, coll, scope_filter: Dict[str, Any], finding_key: str, as_of: str, now: datetime) -> bool:
        result = coll.update_one(
            {**scope_filter, "finding_key": finding_key, "status": "active"},
            {
                "$set": {
                    "status": "resolved",
                    "resolved_at": now,
                    "resolved_as_of": as_of,
                    "resolved_reason": "authoritative_reassessment_absent",
                    "resolved_by": None,
                    "resolved_via": "auto",
                },
            },
        )
        return result.modified_count > 0

    def _lazy_resolve_expired(self, coll, scope_filter: Dict[str, Any], *, now: datetime) -> None:
        today = _today_yyyymmdd()
        coll.update_many(
            {
                **scope_filter,
                "status": "active",
                "resolution_mode": "dated",
                "expires_as_of": {"$lt": today},
            },
            {
                "$set": {
                    "status": "resolved",
                    "resolved_at": now,
                    "resolved_as_of": today,
                    "resolved_reason": "expired",
                    "resolved_by": None,
                    "resolved_via": "auto",
                },
            },
        )

    def _lazy_resolve_expired_dated(self, symbols: Sequence[str], *, today: str) -> None:
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
                    "status": "resolved",
                    "resolved_at": now,
                    "resolved_as_of": today,
                    "resolved_reason": "expired",
                    "resolved_by": None,
                    "resolved_via": "auto",
                },
            },
        )


__all__ = [
    "ENGINE_VERSION_LLM",
    "ENGINE_VERSION_RULES",
    "INDUSTRY_RISK_FINDINGS_COL",
    "RISK_CATEGORIES",
    "RISK_CATEGORY_ALIASES",
    "SYMBOL_RISK_FINDINGS_COL",
    "SymbolRiskLedgerAccess",
    "make_finding_key",
    "normalize_category",
    "normalize_subject",
]
