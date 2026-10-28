"""Tests for the shared index regime labeler."""

from datetime import datetime, timedelta

import pandas as pd

from stock_data_access.regime import (
    DEFAULT_REGIME_RULE_ID,
    DEFAULT_UNCERTAIN_GROWTH_WEIGHT,
    REGIME_RULE_PRESETS,
    build_regime_cash_schedule,
    build_regime_weight_schedule,
    extract_regime_segments,
    growth_weight_by_date,
    growth_weight_for_label,
    label_as_of,
    label_index_regime,
    load_regime_rule,
    longest_segment_for_label,
)


def _make_index_rows(start: str, days: int, *, close_fn):
    dt = datetime.strptime(start, "%Y%m%d")
    rows = []
    for idx in range(days):
        trade_date = (dt + timedelta(days=idx)).strftime("%Y%m%d")
        rows.append({"trade_date": trade_date, "close": close_fn(idx)})
    return rows


def _warmup_then_flat_bull(days_after_warmup: int = 10):
    def close_fn(idx: int) -> float:
        if idx < 130:
            return 100.0 + idx * 0.01
        return 250.0

    return _make_index_rows("20200101", 130 + days_after_warmup, close_fn=close_fn)


def test_growth_weight_for_label_mapping():
    assert growth_weight_for_label("bull") == 0.6
    assert growth_weight_for_label("uncertain") == 0.2
    assert growth_weight_for_label("bear") == 0.0
    assert growth_weight_for_label("unknown") == DEFAULT_UNCERTAIN_GROWTH_WEIGHT


def test_label_as_of_uses_only_past_rows():
    labels = pd.DataFrame(
        [
            {"trade_date": "20240101", "label": "uncertain"},
            {"trade_date": "20240102", "label": "bull"},
        ]
    )
    assert label_as_of(labels, "20240101") == "uncertain"
    assert label_as_of(labels, "20240102") == "bull"
    assert label_as_of(labels, "20240103") == "bull"


def test_future_close_change_does_not_alter_past_label():
    rows = _warmup_then_flat_bull(days_after_warmup=8)
    labels_before = label_index_regime(rows, confirm_days=5)
    past_label = label_as_of(labels_before, rows[-3]["trade_date"])

    rows[-1]["close"] = 50.0
    rows[-2]["close"] = 60.0
    labels_after = label_index_regime(rows, confirm_days=5)
    assert label_as_of(labels_after, rows[-3]["trade_date"]) == past_label


def test_hysteresis_requires_five_consecutive_bull_days():
    def close_fn(idx: int) -> float:
        if idx < 150:
            return 100.0
        return 100.0 + (idx - 149) * 20.0

    rows = _make_index_rows("20200101", 155, close_fn=close_fn)
    labels = label_index_regime(rows, confirm_days=5)
    fourth_bull_day = rows[153]["trade_date"]
    fifth_bull_day = rows[154]["trade_date"]
    assert label_as_of(labels, fourth_bull_day) != "bull"
    assert label_as_of(labels, fifth_bull_day) == "bull"


def test_growth_weight_by_date_follows_labels():
    rows = _warmup_then_flat_bull(days_after_warmup=8)
    labels = label_index_regime(rows, confirm_days=5)
    weights = growth_weight_by_date(labels, [rows[-1]["trade_date"]])
    assert weights[rows[-1]["trade_date"]] == 0.6


def test_build_regime_cash_schedule_cashes_non_bull():
    rows = _warmup_then_flat_bull(days_after_warmup=8)
    cash_dates, schedule = build_regime_cash_schedule(
        rows, [rows[0]["trade_date"], rows[-1]["trade_date"]]
    )
    assert rows[-1]["trade_date"] not in cash_dates
    assert schedule[-1]["action"] == "invest"
    assert schedule[0]["action"] == "cash"


def test_build_regime_weight_schedule():
    rows = _warmup_then_flat_bull(days_after_warmup=8)
    rebalance_dates = [rows[-1]["trade_date"]]
    weight_by_date, schedule = build_regime_weight_schedule(
        rows, rebalance_dates, confirm_days=5
    )
    assert weight_by_date[rebalance_dates[0]] == 0.6
    assert schedule[0]["label"] == "bull"
    assert schedule[0]["w_cycle"] == 0.4


def test_extract_regime_segments_marks_last_segment_ongoing_at_cutoff():
    labels = pd.DataFrame(
        [
            {"trade_date": "20250101", "label": "bear"},
            {"trade_date": "20250102", "label": "bear"},
            {"trade_date": "20250103", "label": "bull"},
            {"trade_date": "20250104", "label": "bull"},
        ]
    )
    segments = extract_regime_segments(labels, start_date="20250101", end_date="20250104")
    assert segments == [
        {"label": "bear", "start": "20250101", "end": "20250102", "trading_days": 2, "ongoing": False},
        {"label": "bull", "start": "20250103", "end": "20250104", "trading_days": 2, "ongoing": True},
    ]


def test_longest_segment_for_label():
    segments = [
        {"label": "bear", "start": "20230101", "end": "20240101", "trading_days": 100, "ongoing": False},
        {"label": "bear", "start": "20240201", "end": "20240901", "trading_days": 150, "ongoing": False},
        {"label": "bull", "start": "20250101", "end": "20260501", "trading_days": 320, "ongoing": True},
    ]
    bear = longest_segment_for_label(segments, "bear")
    assert bear["start"] == "20240201"
    assert longest_segment_for_label(segments, "bull")["ongoing"] is True


def _price_only_side(close: float, k_fast: float, k_slow: float, *, regime_rule) -> str:
    from stock_data_access.regime import _raw_side_row

    nan = float("nan")
    return _raw_side_row(close, k_fast, k_slow, nan, nan, nan, nan, regime_rule=regime_rule)


def test_k_fast_price_rule_is_stricter_than_k_slow_anchor():
    rule_default = load_regime_rule(DEFAULT_REGIME_RULE_ID)
    rule_k_fast = load_regime_rule("k_fast_price")
    assert _price_only_side(103.0, 105.0, 100.0, regime_rule=rule_default) == "bull"
    assert _price_only_side(103.0, 105.0, 100.0, regime_rule=rule_k_fast) == "uncertain"


def test_load_regime_rule_from_inline_json():
    rule = load_regime_rule(
        '{"rule_id":"custom_v1","bull":{"price_above":"k_fast","k_fast_vs_k_slow":"above"},'
        '"bear":{"price_below":"k_fast","k_fast_vs_k_slow":"below"}}'
    )
    assert rule.rule_id == "custom_v1"
    assert rule.bull.price_ref == "k_fast"
    assert rule.bear.price_ref == "k_fast"


def test_regime_rule_presets_include_defaults():
    assert DEFAULT_REGIME_RULE_ID in REGIME_RULE_PRESETS
    assert load_regime_rule(None).rule_id == DEFAULT_REGIME_RULE_ID
    assert "k_fast_only" in REGIME_RULE_PRESETS


def test_k_fast_only_uses_price_vs_k_fast_without_stack():
    rule = load_regime_rule("k_fast_only")
    assert _price_only_side(115.0, 110.0, 120.0, regime_rule=rule) == "bull"
    assert _price_only_side(85.0, 90.0, 80.0, regime_rule=rule) == "bear"


def test_sticky_hysteresis_keeps_bull_through_uncertain_days():
    def close_fn(idx: int) -> float:
        if idx < 150:
            return 100.0
        if idx < 155:
            return 100.0 + (idx - 149) * 20.0
        if idx < 158:
            return 100.0
        return 200.0

    rows = _make_index_rows("20200101", 160, close_fn=close_fn)
    last_bull_setup = rows[154]["trade_date"]
    first_drop = rows[155]["trade_date"]
    sticky = label_index_regime(rows, confirm_days=5)
    fast = label_index_regime(rows, confirm_days=5, non_bull_exits=True)
    assert label_as_of(sticky, last_bull_setup) == "bull"
    assert label_as_of(fast, last_bull_setup) == "bull"
    assert label_as_of(sticky, first_drop) == "bull"
    assert label_as_of(fast, first_drop) != "bull"


def test_exit_confirm_days_one_is_not_slower_than_five():
    def close_fn(idx: int) -> float:
        if idx < 150:
            return 200.0 - idx * 0.2
        return 50.0

    rows = _make_index_rows("20200101", 160, close_fn=close_fn)
    slow = label_index_regime(rows, confirm_days=5)
    fast_bear = label_index_regime(rows, confirm_days=5, exit_confirm_days=1)
    first_fast_bear = None
    first_slow_bear = None
    for row in rows[120:]:
        date = row["trade_date"]
        if first_fast_bear is None and label_as_of(fast_bear, date) == "bear":
            first_fast_bear = date
        if first_slow_bear is None and label_as_of(slow, date) == "bear":
            first_slow_bear = date
    assert first_fast_bear is not None
    assert first_slow_bear is not None
    assert first_fast_bear <= first_slow_bear


def test_non_bull_exits_cashes_on_first_drop_after_bull():
    def close_fn(idx: int) -> float:
        if idx < 150:
            return 100.0
        if idx < 155:
            return 100.0 + (idx - 149) * 20.0
        return 100.0

    rows = _make_index_rows("20200101", 160, close_fn=close_fn)
    dates = [rows[154]["trade_date"], rows[155]["trade_date"]]
    cash_sticky, _ = build_regime_cash_schedule(rows, dates, confirm_days=5)
    cash_fast, _ = build_regime_cash_schedule(
        rows, dates, confirm_days=5, non_bull_exits=True
    )
    assert rows[154]["trade_date"] not in cash_sticky
    assert rows[155]["trade_date"] not in cash_sticky
    assert rows[154]["trade_date"] not in cash_fast
    assert rows[155]["trade_date"] in cash_fast
