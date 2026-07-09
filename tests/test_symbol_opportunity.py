from __future__ import annotations

from datetime import datetime

import mongomock
import pytest

from stock_data_access.symbol_opportunity import (
    SymbolOpportunityLedgerAccess,
    make_opportunity_finding_key,
    normalize_opportunity_category,
)
from stock_data_access.symbol_risk import SymbolRiskLedgerAccess


@pytest.fixture
def ledger():
    client = mongomock.MongoClient()
    return SymbolOpportunityLedgerAccess(db=client["quant_analyzer"])


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


def test_normalize_opportunity_category_maps_tailwinds():
    assert normalize_opportunity_category("国产替代政策") == "policy_tailwind"
    assert normalize_opportunity_category("demand_growth") == "demand_growth"
    assert normalize_opportunity_category("未知类别") == "other"


def test_apply_assessment_inserts_active_opportunity(ledger):
    stats = ledger.apply_assessment(
        "688001.SH",
        new_findings=[
            {
                "category": "policy_tailwind",
                "subject": "国产替代推进",
                "strength": "medium",
                "summary": "国产替代政策持续推进",
                "resolution_mode": "event",
            }
        ],
        confirmed=[],
        suggested_closures=[],
        assessment={"as_of_date": "20260109"},
        source=_source(),
    )

    assert stats["inserted"] == 1
    active = ledger.get_active_for_symbols(["688001.SH"])["688001.SH"]
    assert len(active) == 1
    assert active[0]["strength"] == "medium"
    assert active[0]["status"] == "active"


def test_strength_latest_wins_and_history_allows_decay(ledger):
    key = make_opportunity_finding_key("demand_growth", "ai算力需求")
    ledger.apply_assessment(
        "300750.SZ",
        new_findings=[
            {
                "finding_key": key,
                "category": "demand_growth",
                "subject": "AI算力需求",
                "strength": "high",
                "summary": "需求高景气",
            }
        ],
        confirmed=[],
        suggested_closures=[],
        assessment={"as_of_date": "20260109"},
        source=_source(run_id="run-high"),
    )
    ledger.apply_assessment(
        "300750.SZ",
        new_findings=[
            {
                "finding_key": key,
                "category": "demand_growth",
                "subject": "AI算力需求",
                "strength": "low",
                "summary": "需求边际降温",
            }
        ],
        confirmed=[],
        suggested_closures=[],
        assessment={"as_of_date": "20260110"},
        source=_source(run_id="run-low"),
    )

    doc = ledger.symbol_coll.find_one({"symbol": "300750.SZ", "finding_key": key})
    assert doc["strength"] == "low"
    assert [row["strength"] for row in doc["strength_history"]] == ["high", "low"]
    assert [row["run_id"] for row in doc["strength_history"]] == ["run-high", "run-low"]


def test_realize_invalidate_and_reopen(ledger):
    key = ledger.add_manual_finding(
        "600519.SH",
        strength="medium",
        summary="新品放量",
        created_by="alice",
        category="event_catalyst",
    )

    assert ledger.realize_finding("600519.SH", key, reason="新品销量兑现", closed_by="alice")
    doc = ledger.symbol_coll.find_one({"symbol": "600519.SH", "finding_key": key})
    assert doc["status"] == "closed"
    assert doc["outcome"] == "realized"
    assert ledger.reopen_finding("600519.SH", key, reopened_by="bob", reason="继续跟踪")
    assert ledger.get_active_for_symbols(["600519.SH"])["600519.SH"]
    assert ledger.invalidate_finding("600519.SH", key, reason="销量不及预期", closed_by="bob")
    doc = ledger.symbol_coll.find_one({"symbol": "600519.SH", "finding_key": key})
    assert doc["outcome"] == "invalidated"


def test_invalidate_spawns_low_risk_only_when_requested():
    client = mongomock.MongoClient()
    db = client["quant_analyzer"]
    opp = SymbolOpportunityLedgerAccess(db=db)
    risk = SymbolRiskLedgerAccess(db=db)
    key = opp.add_manual_finding(
        "688001.SH",
        strength="high",
        summary="AI订单催化",
        created_by="alice",
        category="demand_growth",
    )

    assert opp.invalidate_finding(
        "688001.SH",
        key,
        reason="订单证伪",
        closed_by="alice",
        spawn_risk=True,
        risk_ledger=risk,
    )
    opp_doc = opp.symbol_coll.find_one({"symbol": "688001.SH", "finding_key": key})
    risk_doc = risk.symbol_coll.find_one({"symbol": "688001.SH"})
    assert opp_doc["spawned_risk_finding_key"] == risk_doc["finding_key"]
    assert risk_doc["severity"] == "low"
    assert risk_doc["discovered_by"] == "opportunity_invalidation"
    assert risk_doc["origin_opportunity_finding_key"] == key


def test_dated_opportunity_expires_to_closed(ledger):
    key = make_opportunity_finding_key("event_catalyst", "新品发布")
    ledger.symbol_coll.insert_one(
        {
            "symbol": "688001.SH",
            "finding_key": key,
            "resolution_mode": "dated",
            "expires_as_of": "20250101",
            "status": "active",
            "category": "event_catalyst",
            "subject": "新品发布",
            "strength": "medium",
            "summary": "新品发布催化",
        }
    )

    assert ledger.get_active_for_symbols(["688001.SH"], today="20260109")["688001.SH"] == []
    doc = ledger.symbol_coll.find_one({"symbol": "688001.SH", "finding_key": key})
    assert doc["status"] == "closed"
    assert doc["outcome"] == "expired"
