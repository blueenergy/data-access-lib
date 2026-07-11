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
    assert latest["600519.SH"]["analyzed_at"] == now
    assert latest["600519.SH"]["checked_at"] == now
    assert latest["600519.SH"]["last_run_status"] == "analyzed"
    assert latest["600519.SH"]["review_scope"] == "external_event"
    assert "result_counts" not in latest["600519.SH"]
    assert "dimensions" not in latest["300750.SZ"]
    assert "plan_id" not in latest["600519.SH"]


def test_record_symbol_reviews_updates_latest_without_history():
    client = mongomock.MongoClient()
    reviews = SymbolLlmSignalReviewAccess(db=client["quant_analyzer"])

    reviews.record_symbol_reviews(["600519.SH"], reviewed_at=datetime(2026, 1, 9), run_id="run-1")
    reviews.record_symbol_reviews(["600519.SH"], reviewed_at=datetime(2026, 1, 10), run_id="run-2")

    row = reviews.get_latest_reviews(["600519.SH"])["600519.SH"]
    assert row["analyzed_at"] == datetime(2026, 1, 10)
    assert "run_id" not in row
    assert "review_history" not in row


def test_record_analysis_stores_minimal_checkpoint_fields():
    client = mongomock.MongoClient()
    reviews = SymbolLlmSignalReviewAccess(db=client["quant_analyzer"])
    now = datetime(2026, 1, 9, 10, 0, 0)

    reviews.record_analysis(
        ["600519.SH"],
        analyzed_at=now,
        dimensions=("risk", "opportunity", "strength"),
        result_counts_by_symbol={"600519.SH": {"risk": 1, "opportunity": 2, "strength": 3}},
        fingerprints_by_symbol={
            "600519.SH": {
                "symbol_fingerprint": "sym",
                "sector_fingerprint": "sec",
                "combined_fingerprint": "combo",
                "evidence_count": 4,
                "latest_evidence_at": now,
            }
        },
        prompt_version="v2",
    )

    row = reviews.get_latest_reviews(["600519.SH"])["600519.SH"]
    assert row["combined_fingerprint"] == "combo"
    assert row["sector_fingerprint"] == "sec"
    assert row["latest_evidence_at"] == now
    assert row["prompt_version"] == "v2"
    assert "symbol_fingerprint" not in row
    assert "result_counts" not in row


def test_record_skip_does_not_overwrite_analyzed_at_or_fingerprint():
    client = mongomock.MongoClient()
    reviews = SymbolLlmSignalReviewAccess(db=client["quant_analyzer"])
    analyzed_at = datetime(2026, 1, 9, 10, 0, 0)
    checked_at = datetime(2026, 1, 10, 10, 0, 0)

    reviews.record_analysis(
        ["600519.SH"],
        analyzed_at=analyzed_at,
        fingerprints_by_symbol={"600519.SH": {"combined_fingerprint": "combo"}},
    )
    reviews.record_skip(
        ["600519.SH"],
        checked_at=checked_at,
        reason="unchanged_evidence",
        fingerprints_by_symbol={"600519.SH": {"evidence_count": 2}},
    )

    row = reviews.get_latest_reviews(["600519.SH"])["600519.SH"]
    assert row["checked_at"] == checked_at
    assert row["analyzed_at"] == analyzed_at
    assert row["combined_fingerprint"] == "combo"
    assert row["last_run_status"] == "skipped_unchanged"
    assert "last_skip_reason" not in row


def test_record_parse_error_keeps_previous_analysis_fingerprint():
    client = mongomock.MongoClient()
    reviews = SymbolLlmSignalReviewAccess(db=client["quant_analyzer"])
    analyzed_at = datetime(2026, 1, 9, 10, 0, 0)
    checked_at = datetime(2026, 1, 10, 10, 0, 0)

    reviews.record_analysis(
        ["600519.SH"],
        analyzed_at=analyzed_at,
        fingerprints_by_symbol={"600519.SH": {"combined_fingerprint": "combo"}},
    )
    reviews.record_parse_error(["600519.SH"], checked_at=checked_at, error_detail="bad json")

    row = reviews.get_latest_reviews(["600519.SH"])["600519.SH"]
    assert row["checked_at"] == checked_at
    assert row["analyzed_at"] == analyzed_at
    assert row["combined_fingerprint"] == "combo"
    assert row["last_run_status"] == "parse_error"
    assert "last_error_detail" not in row


def test_record_failure_keeps_previous_analysis_fingerprint():
    client = mongomock.MongoClient()
    reviews = SymbolLlmSignalReviewAccess(db=client["quant_analyzer"])
    analyzed_at = datetime(2026, 1, 9, 10, 0, 0)
    checked_at = datetime(2026, 1, 10, 10, 0, 0)

    reviews.record_analysis(
        ["600519.SH"],
        analyzed_at=analyzed_at,
        fingerprints_by_symbol={"600519.SH": {"combined_fingerprint": "combo"}},
    )
    reviews.record_failure(["600519.SH"], checked_at=checked_at, error_detail="network error")

    row = reviews.get_latest_reviews(["600519.SH"])["600519.SH"]
    assert row["checked_at"] == checked_at
    assert row["analyzed_at"] == analyzed_at
    assert row["combined_fingerprint"] == "combo"
    assert row["last_run_status"] == "failed"
    assert "last_error_detail" not in row


def test_review_scope_keeps_independent_checkpoints():
    client = mongomock.MongoClient()
    reviews = SymbolLlmSignalReviewAccess(db=client["quant_analyzer"])

    reviews.record_analysis(
        ["600519.SH"],
        analyzed_at=datetime(2026, 1, 9),
        fingerprints_by_symbol={"600519.SH": {"combined_fingerprint": "external"}},
    )
    reviews.record_analysis(
        ["600519.SH"],
        analyzed_at=datetime(2026, 1, 10),
        fingerprints_by_symbol={"600519.SH": {"combined_fingerprint": "internal"}},
        review_scope="internal_fundamental",
    )

    external = reviews.get_latest_reviews(["600519.SH"])["600519.SH"]
    internal = reviews.get_latest_reviews(["600519.SH"], review_scope="internal_fundamental")["600519.SH"]
    assert external["combined_fingerprint"] == "external"
    assert internal["combined_fingerprint"] == "internal"
