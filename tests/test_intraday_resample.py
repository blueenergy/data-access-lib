from stock_data_access.indicators.intraday_resample import (
    bucket_end_hhmm,
    filter_bars_by_ymd,
    normalize_intraday_tf,
    resample_ashare_intraday,
)


def _hhmm_range(start: int, end: int):
    hour, minute = divmod(start, 100)
    end_hour, end_minute = divmod(end, 100)
    current = hour * 60 + minute
    last = end_hour * 60 + end_minute
    while current <= last:
        yield (current // 60) * 100 + (current % 60)
        current += 1


def _session_hhmm():
    times = list(_hhmm_range(931, 1130))
    times.extend(_hhmm_range(1301, 1500))
    return times


def _day_1m(ymd: str, *, close_start: float = 10.0, incomplete_last: int = 0):
    times = _session_hhmm()
    if incomplete_last:
        times = times[:-incomplete_last]
    rows = []
    for idx, hhmm in enumerate(times):
        close = close_start + idx * 0.01
        rows.append(
            {
                "symbol": "000001.SZ",
                "trade_date": f"{ymd}{hhmm:04d}",
                "open": close,
                "high": close + 0.05,
                "low": close - 0.05,
                "close": close,
                "volume": 100,
            }
        )
    return rows


def test_normalize_intraday_tf_aliases():
    assert normalize_intraday_tf("30min") == "30m"
    assert normalize_intraday_tf("1h") == "60m"
    assert normalize_intraday_tf("minute") == "1m"


def test_bucket_end_skips_lunch():
    assert bucket_end_hhmm(1130, "30m") == 1130
    assert bucket_end_hhmm(1131, "30m") is None
    assert bucket_end_hhmm(1259, "60m") is None
    assert bucket_end_hhmm(1301, "30m") == 1330
    assert bucket_end_hhmm(931, "60m") == 1030
    assert bucket_end_hhmm(1401, "60m") == 1500


def test_full_day_30m_eight_closed_bars():
    bars = resample_ashare_intraday(_day_1m("20260907"), "30m")
    assert [row["trade_date"] for row in bars] == [
        "202609071000",
        "202609071030",
        "202609071100",
        "202609071130",
        "202609071330",
        "202609071400",
        "202609071430",
        "202609071500",
    ]
    assert all(row["is_partial"] is False for row in bars)
    assert all(row["source_bars"] == 30 for row in bars)
    first = bars[0]
    assert first["open"] == 10.0
    assert first["close"] == 10.0 + 29 * 0.01
    assert first["volume"] == 3000


def test_full_day_60m_session_not_clock_hours():
    bars = resample_ashare_intraday(_day_1m("20260907"), "60m")
    assert [row["trade_date"] for row in bars] == [
        "202609071030",
        "202609071130",
        "202609071400",
        "202609071500",
    ]
    assert all(row["is_partial"] is False for row in bars)
    assert all(row["source_bars"] == 60 for row in bars)


def test_lunch_1m_is_not_merged_across_sessions():
    morning = _day_1m("20260907")[:120]
    lunch = [
        {
            "symbol": "000001.SZ",
            "trade_date": "202609071200",
            "open": 99,
            "high": 99,
            "low": 99,
            "close": 99,
            "volume": 1,
        }
    ]
    afternoon = _day_1m("20260907")[120:]
    bars = resample_ashare_intraday(morning + lunch + afternoon, "30m")
    assert "202609071200" not in [row["trade_date"] for row in bars]
    assert bars[3]["trade_date"] == "202609071130"
    assert bars[4]["trade_date"] == "202609071330"
    assert 99 not in (bars[3]["high"], bars[4]["high"], bars[3]["close"], bars[4]["open"])


def test_incomplete_last_bucket_is_partial():
    bars = resample_ashare_intraday(_day_1m("20260907", incomplete_last=10), "30m")
    assert bars[-1]["trade_date"] == "202609071500"
    assert bars[-1]["is_partial"] is True
    assert all(row["is_partial"] is False for row in bars[:-1])


def test_historical_gap_does_not_partial_earlier_bucket():
    rows = _day_1m("20260907")
    # Drop two 1m bars inside the 10:00 30m bucket; later buckets still exist.
    gapped = [row for row in rows if row["trade_date"] not in ("202609070958", "202609070959")]
    bars = resample_ashare_intraday(gapped, "30m")
    assert bars[0]["is_partial"] is False
    assert bars[0]["source_bars"] == 28


def test_filter_bars_by_ymd_uses_day_prefix():
    rows = resample_ashare_intraday(
        _day_1m("20260906") + _day_1m("20260907") + _day_1m("20260908"),
        "60m",
    )
    kept = filter_bars_by_ymd(rows, start_ymd="20260907", end_ymd="20260907")
    assert {row["trade_date"][:8] for row in kept} == {"20260907"}
    assert len(kept) == 4
