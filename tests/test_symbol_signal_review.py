from __future__ import annotations

from datetime import datetime

import mongomock

from stock_data_access.symbol_signal_review import SymbolLlmSignalReviewAccess


def test_record_symbol_reviews_covers_symbols_with_no_findings():
    client = mongomock.MongoClient()
    reviews = SymbolLlmSignalReviewAccess(db=client["quant_analyzer"])
    now = datetime(2026, 1, 9, 10, 0, 0)

    touched = reviews.record_symbol_reviews(
        ["600519.SH", "300750.SZ"],
        industry="食品饮料",
        reviewed_at=now,
        run_id="run-1",
        plan_id="plan-1",
        task_id="task-1",
        user_id="user-1",
        result_counts_by_symbol={"600519.SH": {"risk": 0, "opportunity": 0}},
    )

    assert touched == 2
    latest = reviews.get_latest_reviews(["600519.SH", "300750.SZ"])
    assert latest["600519.SH"]["reviewed_at"] == now
    assert latest["600519.SH"]["result_counts"] == {"risk": 0, "opportunity": 0}
    assert latest["300750.SZ"]["dimensions"] == ["opportunity", "risk"]


def test_record_symbol_reviews_updates_latest_and_keeps_history():
    client = mongomock.MongoClient()
    reviews = SymbolLlmSignalReviewAccess(db=client["quant_analyzer"])

    reviews.record_symbol_reviews(["600519.SH"], reviewed_at=datetime(2026, 1, 9), run_id="run-1")
    reviews.record_symbol_reviews(["600519.SH"], reviewed_at=datetime(2026, 1, 10), run_id="run-2")

    row = reviews.get_latest_reviews(["600519.SH"])["600519.SH"]
    assert row["run_id"] == "run-2"
    assert row["reviewed_at"] == datetime(2026, 1, 10)
    assert [entry["run_id"] for entry in row["review_history"]] == ["run-1", "run-2"]
