"""Tests for adjusted price access (raw volume_price + stock_adj_factor)."""

from __future__ import annotations

import math

import mongomock
import pandas as pd
import pytest

from stock_data_access.prices import (
    AdjustedPriceDataAccess,
    apply_adjustment,
)


# ----------------------- pure function -----------------------

def _raw_frame():
    # Two ex-events: factor steps 1.0 -> 1.1 -> 1.21
    return pd.DataFrame(
        {
            "trade_date": ["20240101", "20240102", "20240103"],
            "open": [10.0, 11.0, 12.0],
            "high": [10.5, 11.5, 12.5],
            "low": [9.5, 10.5, 11.5],
            "close": [10.0, 11.0, 12.0],
            "pre_close": [9.8, 10.0, 11.0],
            "volume": [100, 200, 300],
            "adj_factor": [1.0, 1.1, 1.21],
        }
    )


def test_hfq_multiplies_by_factor_and_is_positive():
    out = apply_adjustment(_raw_frame(), adjust="hfq")
    # close * adj_factor
    assert out["close"].tolist() == pytest.approx([10.0, 12.1, 14.52])
    assert (out["close"] > 0).all()
    # volume untouched
    assert out["volume"].tolist() == [100, 200, 300]


def test_qfq_uses_latest_factor_and_preserves_returns():
    raw = _raw_frame()
    hfq = apply_adjustment(raw, adjust="hfq")
    qfq = apply_adjustment(raw, adjust="qfq", latest_factor=1.21)
    # latest bar equals raw close under qfq
    assert qfq["close"].iloc[-1] == pytest.approx(12.0)
    # daily returns identical between hfq and qfq
    r_hfq = hfq["close"].pct_change().dropna().tolist()
    r_qfq = qfq["close"].pct_change().dropna().tolist()
    assert r_hfq == pytest.approx(r_qfq)


def test_none_returns_raw_unchanged():
    raw = _raw_frame()
    out = apply_adjustment(raw, adjust="none")
    assert out["close"].tolist() == raw["close"].tolist()


def test_all_nan_factor_degrades_to_raw():
    raw = _raw_frame()
    raw["adj_factor"] = [math.nan, math.nan, math.nan]
    out = apply_adjustment(raw, adjust="hfq")
    assert out["close"].tolist() == [10.0, 11.0, 12.0]
    assert out.attrs.get("adj_degraded") is True


def test_invalid_adjust_raises():
    with pytest.raises(ValueError):
        apply_adjustment(_raw_frame(), adjust="bogus")


# ----------------------- DAO with mongomock -----------------------

@pytest.fixture()
def dao():
    client = mongomock.MongoClient()
    db = client["quant_data"]
    db["volume_price"].insert_many(
        [
            {"symbol": "600519.SH", "trade_date": "20240101", "open": 10.0,
             "high": 10.5, "low": 9.5, "close": 10.0, "pre_close": 9.8, "volume": 100},
            {"symbol": "600519.SH", "trade_date": "20240102", "open": 11.0,
             "high": 11.5, "low": 10.5, "close": 11.0, "pre_close": 10.0, "volume": 200},
            {"symbol": "600519.SH", "trade_date": "20240103", "open": 12.0,
             "high": 12.5, "low": 11.5, "close": 12.0, "pre_close": 11.0, "volume": 300},
        ]
    )
    db["stock_adj_factor"].insert_many(
        [
            {"symbol": "600519.SH", "trade_date": "20240101", "adj_factor": 1.0},
            {"symbol": "600519.SH", "trade_date": "20240102", "adj_factor": 1.1},
            {"symbol": "600519.SH", "trade_date": "20240103", "adj_factor": 1.21},
        ]
    )
    return AdjustedPriceDataAccess(db=db)


def test_load_hfq_joins_and_adjusts(dao):
    df = dao.load_adjusted_ohlc("600519.SH", "20240101", "20240103", adjust="hfq")
    assert len(df) == 3
    assert df["close"].tolist() == pytest.approx([10.0, 12.1, 14.52])
    assert df.attrs["adjust"] == "hfq"
    assert df.attrs["adj_degraded"] is False
    assert isinstance(df.index, pd.DatetimeIndex)


def test_load_qfq_latest_equals_raw(dao):
    df = dao.load_adjusted_ohlc("600519.SH", "20240101", "20240103", adjust="qfq")
    assert df["close"].iloc[-1] == pytest.approx(12.0)


def test_missing_factor_degrades(dao):
    dao.adj_coll.delete_many({})
    df = dao.load_adjusted_ohlc("600519.SH", "20240101", "20240103", adjust="hfq")
    assert df["close"].tolist() == pytest.approx([10.0, 11.0, 12.0])
    assert df.attrs["adj_degraded"] is True


def test_factor_ffill_within_window(dao):
    # Drop middle-day factor: ffill should carry 20240102's value forward from prior day
    dao.adj_coll.delete_one({"symbol": "600519.SH", "trade_date": "20240102"})
    df = dao.load_adjusted_ohlc("600519.SH", "20240101", "20240103", adjust="hfq")
    # 20240102 factor filled from 20240101 (=1.0): close 11.0 * 1.0
    assert df["close"].tolist() == pytest.approx([10.0, 11.0, 14.52])


def test_qfq_anchors_to_query_end_date(dao):
    # A later factor exists beyond the requested window; qfq must anchor to end_date.
    dao.adj_coll.insert_one(
        {"symbol": "600519.SH", "trade_date": "20250101", "adj_factor": 9.99}
    )
    df = dao.load_adjusted_ohlc("600519.SH", "20240101", "20240103", adjust="qfq")
    assert df["close"].iloc[-1] == pytest.approx(12.0)


def test_empty_when_no_raw(dao):
    df = dao.load_adjusted_ohlc("000001.SZ", "20240101", "20240103", adjust="hfq")
    assert df.empty
