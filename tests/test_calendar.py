from stock_data_access.calendar import get_market_trading_dates


class _Coll:
    def __init__(self, rows):
        self.rows = rows

    def distinct(self, field, query):
        lo = query.get("cal_date", {}).get("$gte", "")
        hi = query.get("cal_date", {}).get("$lte", "99999999")
        open_only = query.get("is_open") == 1
        out = []
        for row in self.rows:
            d = str(row.get(field) or "")
            if d < lo or d > hi:
                continue
            if open_only and row.get("is_open") != 1:
                continue
            out.append(d)
        return out

    def find(self, query, projection=None):
        return []


class _DB(dict):
    pass


def test_get_market_trading_dates_hk(monkeypatch):
    db = _DB()
    db["hk_trade_calendar"] = _Coll(
        [
            {"cal_date": "20260828", "is_open": 1},
            {"cal_date": "20260829", "is_open": 0},
            {"cal_date": "20260831", "is_open": 1},
        ]
    )
    monkeypatch.setattr("stock_data_access.calendar.get_db", lambda: db)
    days = get_market_trading_dates("20260801", "20260831", market="HK")
    assert days == ["20260828", "20260831"]


def test_get_market_trading_dates_rejects_unknown():
    import pytest

    with pytest.raises(ValueError):
        get_market_trading_dates("20260101", "20260131", market="TW")


def test_catalog_includes_hk_us_pipelines():
    from stock_data_access.pipeline_catalog import load_pipeline_catalog

    ids = {p["id"] for p in load_pipeline_catalog()}
    for key in ("hk_basic", "hk_tradecal", "hk_daily", "us_basic", "us_tradecal"):
        assert key in ids
    assert "us_daily" not in ids  # unpaid; not scheduled
