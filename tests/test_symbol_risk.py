from __future__ import annotations

from datetime import datetime, timedelta

import mongomock
import pytest

from stock_data_access.symbol_risk import (
    SymbolRiskLedgerAccess,
    make_finding_key,
    normalize_category,
    normalize_subject,
)


@pytest.fixture
def ledger():
    client = mongomock.MongoClient()
    return SymbolRiskLedgerAccess(db=client["quant_analyzer"])


def _source(**kwargs):
    base = {
        "discovered_by": "llm",
        "engine_version": "llm-v1",
        "run_id": "run-1",
        "plan_id": "plan-1",
        "task_id": "task-1",
        "user_id": "user-1",
        "analyzed_at": datetime(2026, 1, 9, 10, 0, 0),
    }
    base.update(kwargs)
    return base


def test_normalize_category_maps_aliases_and_other():
    assert normalize_category("诉讼") == "legal_compliance"
    assert normalize_category("financial_credit") == "financial_credit"
    assert normalize_category("完全陌生类别") == "other"


def test_normalize_subject_and_finding_key():
    assert normalize_subject("  大额解禁 2026Q1  ") == "大额解禁_2026q1"
    key = make_finding_key("share_capital", "大额解禁")
    assert key.startswith("share_capital:")


def test_apply_assessment_inserts_new_finding(ledger):
    stats = ledger.apply_assessment(
        "600519.SH",
        new_findings=[
            {
                "category": "legal_compliance",
                "subject": "重大诉讼",
                "severity": "high",
                "summary": "涉诉",
                "resolution_mode": "event",
            }
        ],
        confirmed=[],
        suggested_resolutions=[],
        assessment={"as_of_date": "20260109", "authoritative_for": []},
        source=_source(),
    )
    assert stats["inserted"] == 1
    active = ledger.get_active_for_symbols(["600519.SH"])
    assert len(active["600519.SH"]) == 1
    assert active["600519.SH"][0]["status"] == "active"


def test_event_not_auto_resolved_when_unmentioned(ledger):
    key = make_finding_key("financial_credit", "坏账")
    ledger.symbol_coll.insert_one(
        {
            "symbol": "000001.SZ",
            "finding_key": key,
            "resolution_mode": "event",
            "status": "active",
            "category": "financial_credit",
            "subject": "坏账",
            "severity": "high",
            "summary": "坏账风险",
        }
    )
    ledger.apply_assessment(
        "000001.SZ",
        new_findings=[],
        confirmed=[],
        suggested_resolutions=[],
        assessment={"as_of_date": "20260109", "authoritative_for": []},
        source=_source(run_id="run-2"),
    )
    active = ledger.get_active_for_symbols(["000001.SZ"])
    assert len(active["000001.SZ"]) == 1


def test_suggested_resolution_keeps_active_until_confirmed(ledger):
    key = make_finding_key("financial_credit", "坏账")
    ledger.symbol_coll.insert_one(
        {
            "symbol": "000001.SZ",
            "finding_key": key,
            "resolution_mode": "event",
            "status": "active",
            "category": "financial_credit",
            "subject": "坏账",
            "severity": "high",
            "summary": "坏账风险",
        }
    )
    ledger.apply_assessment(
        "000001.SZ",
        new_findings=[],
        confirmed=[],
        suggested_resolutions=[{"finding_key": key, "reason": "已清偿", "evidence": []}],
        assessment={"as_of_date": "20260109", "authoritative_for": []},
        source=_source(run_id="run-2"),
    )
    doc = ledger.symbol_coll.find_one({"symbol": "000001.SZ", "finding_key": key})
    assert doc["status"] == "active"
    assert doc["suggested_resolution"]["reason"] == "已清偿"
    assert ledger.confirm_resolution("000001.SZ", key, confirmed_by="alice")
    doc = ledger.symbol_coll.find_one({"symbol": "000001.SZ", "finding_key": key})
    assert doc["status"] == "resolved"
    assert ledger.get_active_for_symbols(["000001.SZ"])["000001.SZ"] == []


def test_metric_auto_resolved_under_authoritative_reassessment(ledger):
    key = make_finding_key("valuation_technical", "近期涨幅过大")
    ledger.symbol_coll.insert_one(
        {
            "symbol": "300750.SZ",
            "finding_key": key,
            "resolution_mode": "metric",
            "status": "active",
            "category": "valuation_technical",
            "subject": "近期涨幅过大",
            "severity": "medium",
            "summary": "涨多了",
        }
    )
    stats = ledger.apply_assessment(
        "300750.SZ",
        new_findings=[],
        confirmed=[],
        suggested_resolutions=[],
        assessment={"as_of_date": "20260109", "authoritative_for": ["metric"]},
        source=_source(run_id="run-metric"),
    )
    assert stats["auto_resolved"] == 1
    assert ledger.get_active_for_symbols(["300750.SZ"])["300750.SZ"] == []


def test_dated_expired_filtered_on_read(ledger):
    key = make_finding_key("share_capital", "解禁")
    ledger.symbol_coll.insert_one(
        {
            "symbol": "688001.SH",
            "finding_key": key,
            "resolution_mode": "dated",
            "expires_as_of": "20250101",
            "status": "active",
            "category": "share_capital",
            "subject": "解禁",
            "severity": "medium",
            "summary": "大额解禁",
        }
    )
    active = ledger.get_active_for_symbols(["688001.SH"], today="20260109")
    assert active["688001.SH"] == []
    doc = ledger.symbol_coll.find_one({"symbol": "688001.SH", "finding_key": key})
    assert doc["status"] == "resolved"
    assert doc["resolved_via"] == "auto"


def test_add_manual_finding_and_reopen(ledger):
    key = ledger.add_manual_finding(
        "600519.SH",
        severity="high",
        summary="管理层诚信疑虑",
        created_by="bob",
        category="governance_integrity",
    )
    assert ledger.get_active_for_symbols(["600519.SH"])["600519.SH"]
    assert ledger.resolve_finding("600519.SH", key, reason="误判", resolved_by="bob")
    assert ledger.get_active_for_symbols(["600519.SH"])["600519.SH"] == []
    assert ledger.reopen_finding("600519.SH", key, reopened_by="carol", reason="复核后仍成立")
    assert len(ledger.get_active_for_symbols(["600519.SH"])["600519.SH"]) == 1
