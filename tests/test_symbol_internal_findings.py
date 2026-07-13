from datetime import datetime

import mongomock
import pytest

from stock_data_access.symbol_strength import SymbolStrengthLedgerAccess
from stock_data_access.symbol_weakness import SymbolWeaknessLedgerAccess


@pytest.mark.parametrize(
    ("ledger_cls", "level_field", "terminal_status"),
    [
        (SymbolStrengthLedgerAccess, "strength", "deprecated"),
        (SymbolWeaknessLedgerAccess, "severity", "resolved"),
    ],
)
def test_rule_assessment_is_idempotent_and_reopens(
    ledger_cls, level_field, terminal_status
):
    client = mongomock.MongoClient()
    ledger = ledger_cls(db=client["quant_analyzer"])
    finding = {
        "rule_id": "profitability.roe_high",
        "category": "profitability",
        "level": "high",
        "summary": "ROE 行业领先",
        "evidence": [{"metric": "roe_level", "value": 18.2}],
        "lifecycle_policy": {"type": "threshold_band"},
    }
    source = {
        "engine_version": "sw-mvp-v1",
        "run_id": "run-1",
        "task_id": "task-1",
    }

    inserted = ledger.apply_rule_assessment(
        "600519.sh",
        matched_findings=[finding],
        close_rule_ids=[],
        evidence_fingerprint="fp-1",
        rule_config_hash="rules-1",
        evidence_version="20260331:0",
        source=source,
        evaluated_at=datetime(2026, 4, 30),
    )
    repeated = ledger.apply_rule_assessment(
        "600519.SH",
        matched_findings=[finding],
        close_rule_ids=[],
        evidence_fingerprint="fp-1",
        rule_config_hash="rules-1",
        evidence_version="20260331:0",
        source={**source, "run_id": "run-2"},
        evaluated_at=datetime(2026, 5, 1),
    )

    assert inserted["inserted"] == 1
    assert repeated["confirmed"] == 1
    row = ledger.get_active_for_symbols(["600519.SH"])["600519.SH"][0]
    assert row["status"] == "active"
    assert row[level_field] == "high"
    assert row["finding_key"] == "profitability.roe_high"
    assert len(row["source_runs"]) == 1

    closed = ledger.apply_rule_assessment(
        "600519.SH",
        matched_findings=[],
        close_rule_ids=["profitability.roe_high"],
        evidence_fingerprint="fp-2",
        rule_config_hash="rules-1",
        evidence_version="20260630:0",
        source=source,
    )
    assert closed["closed"] == 1
    stored = ledger.coll.find_one({"symbol": "600519.SH"})
    assert stored["status"] == terminal_status

    reopened = ledger.apply_rule_assessment(
        "600519.SH",
        matched_findings=[finding],
        close_rule_ids=[],
        evidence_fingerprint="fp-3",
        rule_config_hash="rules-1",
        evidence_version="20260930:0",
        source=source,
    )
    assert reopened["reopened"] == 1
    assert ledger.coll.find_one({"symbol": "600519.SH"})["status"] == "active"


def test_omitted_rule_is_not_closed_when_input_is_missing():
    client = mongomock.MongoClient()
    ledger = SymbolWeaknessLedgerAccess(db=client["quant_analyzer"])
    ledger.apply_rule_assessment(
        "300196.SZ",
        matched_findings=[
            {
                "rule_id": "growth.revenue_negative",
                "category": "growth",
                "level": "medium",
                "summary": "营收连续下降",
            }
        ],
        close_rule_ids=[],
        evidence_fingerprint="fp-1",
        rule_config_hash="rules-1",
        evidence_version="20260331:0",
    )

    ledger.apply_rule_assessment(
        "300196.SZ",
        matched_findings=[],
        close_rule_ids=[],
        evidence_fingerprint="fp-missing",
        rule_config_hash="rules-1",
        evidence_version="20260630:missing",
    )

    assert ledger.coll.find_one({"symbol": "300196.SZ"})["status"] == "active"


def test_reset_rule_config_never_deletes_manual_findings():
    client = mongomock.MongoClient()
    ledger = SymbolStrengthLedgerAccess(db=client["quant_analyzer"])
    ledger.coll.insert_many(
        [
            {
                "symbol": "600519.SH",
                "finding_key": "profitability.roe_high",
                "discovered_by": "rules",
                "review_scope": "internal_fundamental",
                "rule_config_hash": "rules-1",
                "status": "active",
            },
            {
                "symbol": "600519.SH",
                "finding_key": "manual:brand",
                "discovered_by": "manual",
                "review_scope": "internal_fundamental",
                "rule_config_hash": "rules-1",
                "status": "active",
            },
        ]
    )

    assert ledger.reset_rule_config("rules-1") == 1
    assert ledger.reset_rule_config("rules-1", dry_run=False) == 1
    assert ledger.coll.count_documents({}) == 1
    assert ledger.coll.find_one({})["discovered_by"] == "manual"


def test_close_rule_reason_can_record_rule_removed_from_config():
    client = mongomock.MongoClient()
    ledger = SymbolWeaknessLedgerAccess(db=client["quant_analyzer"])
    ledger.apply_rule_assessment(
        "300196.SZ",
        matched_findings=[
            {
                "rule_id": "growth.revenue_negative",
                "category": "growth",
                "level": "medium",
                "summary": "营收连续下降",
            }
        ],
        close_rule_ids=[],
        evidence_fingerprint="fp-1",
        rule_config_hash="rules-1",
        evidence_version="20260331:0",
    )

    ledger.apply_rule_assessment(
        "300196.SZ",
        matched_findings=[],
        close_rule_ids=["growth.revenue_negative"],
        evidence_fingerprint="fp-2",
        rule_config_hash="rules-2",
        evidence_version="20260630:0",
        close_rule_reasons={
            "growth.revenue_negative": "rule_removed_from_config",
        },
    )

    stored = ledger.coll.find_one({"symbol": "300196.SZ"})
    assert stored["status"] == "resolved"
    assert stored["closed_reason"] == "rule_removed_from_config"
