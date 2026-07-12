"""Shared persistence for deterministic internal S/W findings."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

from .mongo_context import get_db


def _utcnow() -> datetime:
    return datetime.utcnow()


class SymbolInternalFindingLedgerAccess:
    """Base ledger for rule-generated, symbol-scoped internal findings."""

    collection_name = ""
    level_field = ""
    terminal_status = ""

    def __init__(self, db=None):
        if not self.collection_name or not self.level_field or not self.terminal_status:
            raise TypeError("internal finding ledger subclass is not configured")
        self.db = db if db is not None else get_db()
        self.coll = self.db[self.collection_name]

    def apply_rule_assessment(
        self,
        symbol: str,
        *,
        matched_findings: Sequence[Mapping[str, Any]],
        close_rule_ids: Sequence[str],
        evidence_fingerprint: str,
        rule_config_hash: str,
        evidence_version: str,
        source: Optional[Mapping[str, Any]] = None,
        evaluated_at: Optional[datetime] = None,
    ) -> Dict[str, int]:
        """Apply explicit match/close decisions from the deterministic engine.

        Rules omitted from both inputs are intentionally left unchanged. This is
        how missing or not-applicable evidence avoids closing an active finding.
        """

        sym = str(symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol is required")
        fingerprint = str(evidence_fingerprint or "").strip()
        config_hash = str(rule_config_hash or "").strip()
        version = str(evidence_version or "").strip()
        if not fingerprint or not config_hash or not version:
            raise ValueError(
                "evidence_fingerprint, rule_config_hash and evidence_version are required"
            )

        now = evaluated_at or _utcnow()
        run_entry = self._source_run_entry(
            source or {},
            evidence_fingerprint=fingerprint,
            rule_config_hash=config_hash,
            evidence_version=version,
            evaluated_at=now,
        )
        stats = {
            "inserted": 0,
            "confirmed": 0,
            "reopened": 0,
            "closed": 0,
            "manual_conflicts": 0,
        }
        matched_rule_ids = set()

        for raw in matched_findings:
            finding = dict(raw)
            rule_id = str(finding.get("rule_id") or finding.get("finding_key") or "").strip()
            if not rule_id:
                raise ValueError("matched finding requires rule_id")
            matched_rule_ids.add(rule_id)
            current = self.coll.find_one({"symbol": sym, "finding_key": rule_id})
            if current and current.get("discovered_by") not in {None, "rules"}:
                stats["manual_conflicts"] += 1
                continue

            level = str(finding.get(self.level_field) or finding.get("level") or "medium")
            common = {
                "symbol": sym,
                "finding_key": rule_id,
                "rule_id": rule_id,
                "discovered_by": "rules",
                "review_scope": "internal_fundamental",
                "engine_type": "rules",
                "engine_version": (source or {}).get("engine_version"),
                "rule_config_hash": config_hash,
                "evidence_fingerprint": fingerprint,
                "evidence_version": version,
                "last_evaluated_at": now,
                "last_confirmed_at": now,
                "last_confirmed_as_of": version,
                "category": str(finding.get("category") or "other"),
                "summary": str(finding.get("summary") or ""),
                "detail": str(finding.get("detail") or ""),
                "evidence": list(finding.get("evidence") or []),
                "lifecycle_policy": dict(finding.get("lifecycle_policy") or {}),
                self.level_field: level,
                "status": "active",
            }

            if current:
                was_terminal = current.get("status") == self.terminal_status
                same_run_input = (
                    current.get("evidence_fingerprint") == fingerprint
                    and current.get("rule_config_hash") == config_hash
                    and current.get("status") == "active"
                )
                update: Dict[str, Any] = {"$set": common}
                if not same_run_input:
                    update["$push"] = {
                        "source_runs": {"$each": [run_entry], "$slice": -20}
                    }
                if was_terminal:
                    update["$unset"] = {
                        "closed_at": "",
                        "closed_as_of": "",
                        "closed_reason": "",
                        "closed_by": "",
                    }
                    stats["reopened"] += 1
                else:
                    stats["confirmed"] += 1
                self.coll.update_one(
                    {"symbol": sym, "finding_key": rule_id},
                    update,
                )
                continue

            doc = {
                **common,
                "first_detected_at": now,
                "first_detected_as_of": version,
                "source_runs": [run_entry],
            }
            self.coll.update_one(
                {"symbol": sym, "finding_key": rule_id},
                {"$setOnInsert": doc},
                upsert=True,
            )
            stats["inserted"] += 1

        for raw_rule_id in close_rule_ids:
            rule_id = str(raw_rule_id or "").strip()
            if not rule_id or rule_id in matched_rule_ids:
                continue
            result = self.coll.update_one(
                {
                    "symbol": sym,
                    "finding_key": rule_id,
                    "status": "active",
                    "discovered_by": "rules",
                    "evidence_fingerprint": {"$ne": fingerprint},
                },
                {
                    "$set": {
                        "status": self.terminal_status,
                        "last_evaluated_at": now,
                        "evidence_fingerprint": fingerprint,
                        "evidence_version": version,
                        "rule_config_hash": config_hash,
                        "closed_at": now,
                        "closed_as_of": version,
                        "closed_reason": "rule_exit_condition_met",
                        "closed_by": "rules",
                    },
                    "$push": {
                        "source_runs": {"$each": [run_entry], "$slice": -20}
                    },
                },
            )
            stats["closed"] += int(result.modified_count > 0)

        return stats

    def get_active_for_symbols(
        self, symbols: Sequence[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        normalized = [str(symbol or "").strip().upper() for symbol in symbols if symbol]
        result: Dict[str, List[Dict[str, Any]]] = {symbol: [] for symbol in normalized}
        if not normalized:
            return result
        rows = self.coll.find(
            {"symbol": {"$in": normalized}, "status": "active"},
            {"_id": 0},
        )
        for row in rows:
            symbol = str(row.get("symbol") or "")
            result.setdefault(symbol, []).append(row)
        for symbol_rows in result.values():
            symbol_rows.sort(
                key=lambda row: (
                    str(row.get("last_confirmed_at") or ""),
                    str(row.get("finding_key") or ""),
                ),
                reverse=True,
            )
        return result

    def reset_rule_config(
        self,
        rule_config_hash: str,
        *,
        dry_run: bool = True,
    ) -> int:
        """Count or delete only rule-generated findings for one config hash."""

        query = {
            "discovered_by": "rules",
            "review_scope": "internal_fundamental",
            "rule_config_hash": str(rule_config_hash or "").strip(),
        }
        if not query["rule_config_hash"]:
            raise ValueError("rule_config_hash is required")
        count = int(self.coll.count_documents(query))
        if not dry_run and count:
            self.coll.delete_many(query)
        return count

    @staticmethod
    def _source_run_entry(
        source: Mapping[str, Any],
        *,
        evidence_fingerprint: str,
        rule_config_hash: str,
        evidence_version: str,
        evaluated_at: datetime,
    ) -> Dict[str, Any]:
        return {
            "discovered_by": "rules",
            "engine_type": "rules",
            "engine_version": source.get("engine_version"),
            "run_id": source.get("run_id"),
            "task_id": source.get("task_id"),
            "user_id": source.get("user_id"),
            "evaluated_at": evaluated_at,
            "evidence_fingerprint": evidence_fingerprint,
            "evidence_version": evidence_version,
            "rule_config_hash": rule_config_hash,
        }


__all__ = ["SymbolInternalFindingLedgerAccess"]
