import pandas as pd
import pytest

from stock_data_access.indicators.decision_gs import (
    FORMULA_ID,
    attach_decision_gs,
    compute_decision_gs,
    typical_price,
)


def _n_bars(count: int, closes):
    rows = []
    for idx in range(count):
        close = closes[idx] if idx < len(closes) else closes[-1]
        month_day = idx + 1
        date = f"2024{(month_day // 28) + 1:02d}{(month_day % 28) + 1:02d}"
        rows.append(
            {
                "trade_date": date,
                "open": close,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
            }
        )
    return rows


def test_typical_price_weights_close():
    assert typical_price({"open": 10, "high": 12, "low": 8, "close": 11}) == (12 + 8 + 10 + 22) / 5


def test_ema_matches_pandas_ewm_adjust_false():
    closes = [float(i) for i in range(1, 80)]
    bars = _n_bars(len(closes), closes)
    result = compute_decision_gs(bars)
    jcx = [(row["high"] + row["low"] + row["open"] + 2 * row["close"]) / 5 for row in bars]
    expected = pd.Series(jcx).ewm(span=39, adjust=False).mean().tolist()
    for got, want in zip((row["mj20"] for row in result), expected):
        assert got == pytest.approx(want, rel=1e-9, abs=1e-9)


def test_warmup_suppresses_signals():
    bars = _n_bars(50, [100 - i * 0.2 for i in range(50)])
    result = compute_decision_gs(bars)
    assert all(row["warmup"] for row in result)
    assert all(row["signal"] is None for row in result)


def test_v_bottom_emits_g_as_event_not_state():
    down = [80 - i * 0.4 for i in range(90)]
    up = [44 + i * 0.8 for i in range(40)]
    bars = _n_bars(130, down + up)
    result = compute_decision_gs(bars)
    g_idx = [i for i, row in enumerate(result) if row["signal"] == "g"]
    assert g_idx, "expected at least one G after the slope turn"
    assert all(not result[i]["warmup"] for i in g_idx)
    for prev, cur in zip(g_idx, g_idx[1:]):
        assert cur - prev > 1


def test_partial_bar_never_emits_signal():
    down = [80 - i * 0.4 for i in range(90)]
    up = [44 + i * 0.8 for i in range(40)]
    bars = _n_bars(130, down + up)
    full = compute_decision_gs(bars)
    signal_idx = next(i for i, row in enumerate(full) if row["signal"] == "g")
    bars[signal_idx]["is_partial"] = True
    partial = compute_decision_gs(bars)
    assert partial[signal_idx]["signal"] is None
    assert partial[signal_idx]["mj20"] is not None


def test_attach_preserves_newest_first_order():
    bars = _n_bars(120, [50 + i * 0.1 for i in range(120)])
    newest_first = list(reversed(bars))
    attached = attach_decision_gs(newest_first)
    assert [row["trade_date"] for row in attached] == [row["trade_date"] for row in newest_first]
    assert attached[0]["gs_formula_id"] == FORMULA_ID
    assert "mj20" in attached[0]
    assert "gs_signal" in attached[0]


def test_empty_input():
    assert compute_decision_gs([]) == []
    assert attach_decision_gs([]) == []
