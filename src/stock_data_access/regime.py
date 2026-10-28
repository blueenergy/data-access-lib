"""Look-ahead-free index regime labels for growth/cycle weight switching.

Uses benchmark index closes only (as-of each score_date), smoothed into two
EWMAs (``k_fast`` alpha 0.967 ~= MA60, ``k_slow`` alpha 0.983 ~= MA120) plus
consecutive-day confirmation before entering bull or bear. Uncertain is the
default when either K is not ready or conditions are mixed.

Naming note: the ``ma60``/``ma120`` identifiers below (and the ``price_above``
values accepted in rule JSON) are legacy aliases for ``k_fast``/``k_slow`` --
they are EWMAs, not simple moving averages. Kept for backward compatibility
with stored rule specs; do not read them as SMA windows.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd

from .indicators.k import calculate_k_indicator

REGIME_SCORE_COLUMN = "growth_cycle_regime_score"
REGIME_SCORE_VARIANT = "growth_cycle_regime"
REGIME_MODE_OFF = "off"
REGIME_MODE_BULL_G60_ELSE_CASH = "bull_g60_else_cash"
VALID_REGIME_MODES = (REGIME_MODE_OFF, REGIME_MODE_BULL_G60_ELSE_CASH)

# Sample evidence, not a product gate: CSI1000 2023-2026 helped; CSI500 same rule hurt.
REGIME_CASH_REMINDER = (
    "指数 MA 标成牛市则满仓，否则空仓。"
    "回测里中证1000上有效，同一规则在中证500上伤害收益；是否启用由你决定。"
)
REGIME_CASH_SCOPE_NOTE = REGIME_CASH_REMINDER

# The regime label is path-dependent (hysteresis); a 200-calendar-day window is
# only ~140 trading days, so the initial state can still dominate the label at
# the requested date. 20260812 was labelled cash by a 200-day live window while
# the full-history label (and research) was invest. 400 calendar days matched
# the 2010-onward baseline across 2024-2026 monthly dates.
MA_WARMUP_CALENDAR_DAYS = 400
CONFIRM_DAYS_DEFAULT = 5
EXIT_CONFIRM_DAYS_DEFAULT = 2
K_FAST_ALPHA = float(os.environ.get("REGIME_FAST_ALPHA", "0.967"))
K_SLOW_ALPHA = float(os.environ.get("REGIME_SLOW_ALPHA", "0.983"))
DEFAULT_REGIME_RULE_ID = "k_slow_anchor"

REGIME_GROWTH_WEIGHT: Dict[str, float] = {
    "bull": 0.6,
    "uncertain": 0.2,
    "bear": 0.0,
}

DEFAULT_UNCERTAIN_GROWTH_WEIGHT = REGIME_GROWTH_WEIGHT["uncertain"]

PriceRef = str
MaStackRelation = str


@dataclass(frozen=True)
class RegimeSideCondition:
    """Single-day raw bull/bear predicate before hysteresis."""

    price_compare: str
    price_ref: PriceRef
    k_fast_vs_k_slow: MaStackRelation = "none"
    volume_compare: str = "none"  # "expanding" (vma_fast > vma_slow), "shrinking" (vma_fast < vma_slow), "none"
    volatility_compare: str = "none" # "expanding" (natr > natr_ma), "shrinking" (natr < natr_ma), "none"

    def __post_init__(self) -> None:
        if self.price_compare not in {"above", "below"}:
            raise ValueError(f"price_compare must be above/below, got {self.price_compare!r}")
        if self.price_ref not in {"k_fast", "k_slow", "ma60", "ma120"}:
            raise ValueError(f"price_ref must be ma60/ma120, got {self.price_ref!r}")
        if self.k_fast_vs_k_slow not in {"above", "below", "none"}:
            raise ValueError(f"k_fast_vs_k_slow must be above/below/none, got {self.k_fast_vs_k_slow!r}")
        if self.volume_compare not in {"expanding", "shrinking", "none"}:
            raise ValueError(f"volume_compare must be expanding/shrinking/none, got {self.volume_compare!r}")
        if self.volatility_compare not in {"expanding", "shrinking", "none"}:
            raise ValueError(f"volatility_compare must be expanding/shrinking/none, got {self.volatility_compare!r}")

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any], *, side: str) -> "RegimeSideCondition":
        above_key = "price_above"
        below_key = "price_below"
        if above_key in payload:
            price_compare = "above"
            price_ref = str(payload[above_key]).strip().lower()
        elif below_key in payload:
            price_compare = "below"
            price_ref = str(payload[below_key]).strip().lower()
        else:
            raise ValueError(f"{side} rule requires price_above or price_below")
        if price_ref == "ma60": price_ref = "k_fast"
        if price_ref == "ma120": price_ref = "k_slow"
        k_fast_vs_k_slow = str(payload.get("k_fast_vs_k_slow") or payload.get("ma60_vs_ma120") or "none").strip().lower()
        volume_compare = str(payload.get("volume_compare") or "none").strip().lower()
        volatility_compare = str(payload.get("volatility_compare") or "none").strip().lower()
        return cls(
            price_compare=price_compare, 
            price_ref=price_ref, 
            k_fast_vs_k_slow=k_fast_vs_k_slow,
            volume_compare=volume_compare,
            volatility_compare=volatility_compare
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "k_fast_vs_k_slow": self.k_fast_vs_k_slow,
            "volume_compare": self.volume_compare,
            "volatility_compare": self.volatility_compare
        }
        key = "price_above" if self.price_compare == "above" else "price_below"
        payload[key] = self.price_ref
        return payload


@dataclass(frozen=True)
class RegimeRuleSpec:
    rule_id: str
    bull: RegimeSideCondition
    bear: RegimeSideCondition

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "RegimeRuleSpec":
        rule_id = str(payload.get("rule_id") or "").strip()
        if not rule_id:
            raise ValueError("regime rule requires rule_id")
        bull_raw = payload.get("bull")
        bear_raw = payload.get("bear")
        if not isinstance(bull_raw, Mapping) or not isinstance(bear_raw, Mapping):
            raise ValueError("regime rule requires bull/bear mapping objects")
        return cls(
            rule_id=rule_id,
            bull=RegimeSideCondition.from_mapping(bull_raw, side="bull"),
            bear=RegimeSideCondition.from_mapping(bear_raw, side="bear"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "bull": self.bull.to_dict(),
            "bear": self.bear.to_dict(),
        }


def _preset(name: str, bull: RegimeSideCondition, bear: RegimeSideCondition) -> RegimeRuleSpec:
    return RegimeRuleSpec(rule_id=name, bull=bull, bear=bear)


REGIME_RULE_PRESETS: Dict[str, RegimeRuleSpec] = {
    DEFAULT_REGIME_RULE_ID: _preset(
        DEFAULT_REGIME_RULE_ID,
        RegimeSideCondition("above", "k_slow", "above"),
        RegimeSideCondition("below", "k_slow", "below"),
    ),
    "k_slow_anchor_with_volume": _preset(
        "k_slow_anchor_with_volume",
        RegimeSideCondition("above", "k_slow", "above", volume_compare="expanding"),
        RegimeSideCondition("below", "k_slow", "below"),
    ),
    "k_slow_anchor_with_volatility": _preset(
        "k_slow_anchor_with_volatility",
        RegimeSideCondition("above", "k_slow", "above", volatility_compare="shrinking"),
        RegimeSideCondition("below", "k_slow", "below"),
    ),
    "k_slow_anchor_with_vol_and_atr": _preset(
        "k_slow_anchor_with_vol_and_atr",
        RegimeSideCondition("above", "k_slow", "above", volume_compare="expanding", volatility_compare="shrinking"),
        RegimeSideCondition("below", "k_slow", "below"),
    ),
    "k_fast_price": _preset(
        "k_fast_price",
        RegimeSideCondition("above", "k_fast", "above"),
        RegimeSideCondition("below", "k_slow", "below"),
    ),
    "k_fast_symmetric": _preset(
        "k_fast_symmetric",
        RegimeSideCondition("above", "k_fast", "above"),
        RegimeSideCondition("below", "k_fast", "below"),
    ),
    # Price vs K_fast only. Faster than the K_slow stack; not a product default.
    "k_fast_only": _preset(
        "k_fast_only",
        RegimeSideCondition("above", "k_fast", "none"),
        RegimeSideCondition("below", "k_fast", "none"),
    ),
}


def list_regime_rule_presets() -> List[str]:
    return sorted(REGIME_RULE_PRESETS.keys())


def regime_rule_slug(rule_id: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(rule_id or "").strip())
    return text.strip("_") or DEFAULT_REGIME_RULE_ID


def load_regime_rule(source: Union[str, RegimeRuleSpec, Mapping[str, Any], None]) -> RegimeRuleSpec:
    """Resolve a preset id, JSON path, inline JSON, mapping, or RegimeRuleSpec."""
    if source is None:
        return REGIME_RULE_PRESETS[DEFAULT_REGIME_RULE_ID]
    if isinstance(source, RegimeRuleSpec):
        return source
    if isinstance(source, Mapping):
        return RegimeRuleSpec.from_mapping(source)

    text = str(source).strip()
    if not text:
        return REGIME_RULE_PRESETS[DEFAULT_REGIME_RULE_ID]
    if text in REGIME_RULE_PRESETS:
        return REGIME_RULE_PRESETS[text]

    path = Path(text)
    if path.is_file():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError(f"Regime rule JSON must be an object: {path}")
        return RegimeRuleSpec.from_mapping(payload)

    if text.startswith("{"):
        payload = json.loads(text)
        if not isinstance(payload, Mapping):
            raise ValueError("Inline regime rule JSON must be an object")
        return RegimeRuleSpec.from_mapping(payload)

    raise ValueError(
        f"Unknown regime rule {text!r}; use preset ({', '.join(list_regime_rule_presets())}), "
        "JSON path, or inline JSON object"
    )


def padded_index_start(start_date: str, pad_days: int = MA_WARMUP_CALENDAR_DAYS) -> str:
    """Return a calendar start date padded for MA120 warm-up."""
    text = str(start_date or "").replace("-", "")[:8]
    dt = datetime.strptime(text, "%Y%m%d")
    return (dt - timedelta(days=int(pad_days))).strftime("%Y%m%d")


def _index_frame(index_rows: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(index_rows)
    close_col = next((c for c in ("close", "close_price", "adj_close") if c in frame.columns), None)
    date_col = next((c for c in ("trade_date", "date") if c in frame.columns), None)
    vol_col = next((c for c in ("vol", "volume") if c in frame.columns), None)
    high_col = next((c for c in ("high", "high_price") if c in frame.columns), None)
    low_col = next((c for c in ("low", "low_price") if c in frame.columns), None)
    
    if close_col is None or date_col is None:
        return pd.DataFrame(columns=["trade_date", "close", "k_fast", "k_slow", "vma_fast", "vma_slow", "natr", "natr_ma", "raw_side", "label"])
        
    cols_to_keep = [date_col, close_col]
    if high_col: cols_to_keep.append(high_col)
    if low_col: cols_to_keep.append(low_col)
    
    out = frame[cols_to_keep].copy()
    out = out.rename(columns={date_col: "trade_date", close_col: "close"})
    if high_col: out = out.rename(columns={high_col: "high"})
    if low_col: out = out.rename(columns={low_col: "low"})
    
    out["trade_date"] = out["trade_date"].astype(str)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    if "high" in out.columns: out["high"] = pd.to_numeric(out["high"], errors="coerce")
    if "low" in out.columns: out["low"] = pd.to_numeric(out["low"], errors="coerce")
    
    if vol_col:
        out["vol"] = pd.to_numeric(frame[vol_col], errors="coerce")
    else:
        out["vol"] = float("nan")
        
    out = out.dropna(subset=["trade_date", "close"]).sort_values("trade_date").drop_duplicates("trade_date")
    if out.empty:
        return pd.DataFrame(columns=["trade_date", "close", "k_fast", "k_slow", "vma_fast", "vma_slow", "natr", "natr_ma", "raw_side", "label"])
        
    close_series = out.set_index("trade_date")["close"]
    if len(close_series) >= 6:
        start_date = close_series.index[5]
        k_fast_series = calculate_k_indicator(close_series, start_date=start_date, alpha=K_FAST_ALPHA)
        k_slow_series = calculate_k_indicator(close_series, start_date=start_date, alpha=K_SLOW_ALPHA)
        out["k_fast"] = out["trade_date"].map(k_fast_series)
        out["k_slow"] = out["trade_date"].map(k_slow_series)
    else:
        out["k_fast"] = float("nan")
        out["k_slow"] = float("nan")
        
    if not out["vol"].isna().all():
        vol_series = out.set_index("trade_date")["vol"]
        out["vma_fast"] = out["trade_date"].map(vol_series.rolling(window=5, min_periods=1).mean())
        out["vma_slow"] = out["trade_date"].map(vol_series.rolling(window=60, min_periods=1).mean())
    else:
        out["vma_fast"] = float("nan")
        out["vma_slow"] = float("nan")
        
    # Calculate ATR and NATR if high/low are available
    if "high" in out.columns and "low" in out.columns and not out["high"].isna().all():
        prev_close = out["close"].shift(1)
        tr1 = out["high"] - out["low"]
        tr2 = (out["high"] - prev_close).abs()
        tr3 = (out["low"] - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr_14 = tr.rolling(window=14, min_periods=1).mean()
        out["natr"] = atr_14 / out["close"]
        out["natr_ma"] = out["natr"].rolling(window=60, min_periods=1).mean()
    else:
        out["natr"] = float("nan")
        out["natr_ma"] = float("nan")
        
    return out.reset_index(drop=True)


def _ref_value(price_ref: PriceRef, close: float, k_fast: float, k_slow: float) -> float:
    if price_ref in ("k_fast", "ma60"):
        return k_fast
    if price_ref in ("k_slow", "ma120"):
        return k_slow
    raise ValueError(f"unsupported price_ref {price_ref!r}")


def _side_matches(
    close: float,
    k_fast: float,
    k_slow: float,
    vma_fast: float,
    vma_slow: float,
    natr: float,
    natr_ma: float,
    condition: RegimeSideCondition,
) -> bool:
    # 各条件全部 AND，没有「只看其中一个」的分支：
    #   price_ok  收盘价 vs 参考线（k_fast 或 k_slow）
    #   stack_ok  双 K 线交叉，condition 为 "none" 时跳过
    #   volume/volatility_ok  可选过滤，数据缺失时跳过（退化为价格交叉）
    # 所以默认 k_slow_anchor 要求 close 与 k_fast **同时**站在 k_slow 之上
    # （熊市同时之下），不是只比双 K 线交叉。
    if condition.k_fast_vs_k_slow == "above":
        stack_ok = k_fast > k_slow
    elif condition.k_fast_vs_k_slow == "below":
        stack_ok = k_fast < k_slow
    else:
        stack_ok = True
    
    # 检查价格条件
    if condition.price_compare == "above":
        if condition.price_ref == "k_fast":
            price_ok = close > k_fast
        elif condition.price_ref == "k_slow":
            price_ok = close > k_slow
        else:
            price_ok = True
    elif condition.price_compare == "below":
        if condition.price_ref == "k_fast":
            price_ok = close < k_fast
        elif condition.price_ref == "k_slow":
            price_ok = close < k_slow
        else:
            price_ok = True
    else:
        price_ok = True
        
    # 检查成交量条件 (如果数据缺失则跳过过滤，避免破坏历史回测)
    if condition.volume_compare == "expanding" and not pd.isna(vma_fast) and not pd.isna(vma_slow):
        volume_ok = vma_fast > vma_slow
    elif condition.volume_compare == "shrinking" and not pd.isna(vma_fast) and not pd.isna(vma_slow):
        volume_ok = vma_fast < vma_slow
    else:
        volume_ok = True
        
    # 检查波动率条件 (如果数据缺失则跳过过滤)
    if condition.volatility_compare == "expanding" and not pd.isna(natr) and not pd.isna(natr_ma):
        volatility_ok = natr > natr_ma
    elif condition.volatility_compare == "shrinking" and not pd.isna(natr) and not pd.isna(natr_ma):
        volatility_ok = natr < natr_ma
    else:
        volatility_ok = True
        
    return price_ok and stack_ok and volume_ok and volatility_ok


def _raw_side_row(
    close: float,
    ma60: float,
    ma120: float,
    vma_fast: float,
    vma_slow: float,
    natr: float,
    natr_ma: float,
    *,
    regime_rule: RegimeRuleSpec,
) -> str:
    if pd.isna(ma120) or pd.isna(ma60):
        return "uncertain"
    if _side_matches(close, ma60, ma120, vma_fast, vma_slow, natr, natr_ma, regime_rule.bull):
        return "bull"
    if _side_matches(close, ma60, ma120, vma_fast, vma_slow, natr, natr_ma, regime_rule.bear):
        return "bear"
    return "uncertain"


def label_index_regime(
    index_rows: Sequence[Dict[str, Any]],
    *,
    confirm_days: int = CONFIRM_DAYS_DEFAULT,
    exit_confirm_days: Optional[int] = EXIT_CONFIRM_DAYS_DEFAULT,
    non_bull_exits: bool = False,
    regime_rule: Union[str, RegimeRuleSpec, Mapping[str, Any], None] = None,
) -> pd.DataFrame:
    """Label each index trading day with hysteresis-smoothed bull/bear/uncertain.

    Defaults stay sticky: a confirmed bull/bear holds through mixed days until
    ``exit_confirm_days`` consecutive opposite raw days (default 2). ``non_bull_exits=True`` is the fast-cash switch: the first
    non-bull raw day leaves bull. Product default must keep both off/sticky.
    """
    rule = load_regime_rule(regime_rule)
    enter_n = int(confirm_days)
    if enter_n < 1:
        raise ValueError(f"confirm_days must be >= 1, got {confirm_days!r}")
    exit_n = enter_n if exit_confirm_days is None else int(exit_confirm_days)
    if exit_n < 1:
        raise ValueError(f"exit_confirm_days must be >= 1, got {exit_confirm_days!r}")
    base = _index_frame(index_rows)
    if base.empty:
        return base.assign(raw_side=pd.Series(dtype=str), label=pd.Series(dtype=str))

    raw_sides: List[str] = []
    for _, row in base.iterrows():
        raw_sides.append(
            _raw_side_row(
                float(row["close"]),
                float(row["k_fast"]),
                float(row["k_slow"]),
                float(row["vma_fast"]),
                float(row["vma_slow"]),
                float(row["natr"]),
                float(row["natr_ma"]),
                regime_rule=rule,
            )
        )
    base = base.copy()
    base["raw_side"] = raw_sides

    labels: List[str] = []
    state = "uncertain"
    bull_streak = 0
    bear_streak = 0
    for raw in raw_sides:
        if raw == "bull":
            bull_streak += 1
            bear_streak = 0
        elif raw == "bear":
            bear_streak += 1
            bull_streak = 0
        else:
            bull_streak = 0
            bear_streak = 0

        if non_bull_exits and state == "bull" and raw != "bull":
            state = raw
        elif bull_streak >= enter_n:
            state = "bull"
        elif bear_streak >= exit_n:
            state = "bear"
        labels.append(state)

    base["label"] = labels
    return base


def label_as_of(labels: pd.DataFrame, as_of_date: str) -> str:
    """Return the regime label visible on ``as_of_date`` (last index row <= date)."""
    if labels.empty:
        return "uncertain"
    sub = labels[labels["trade_date"].astype(str) <= str(as_of_date)]
    if sub.empty:
        return "uncertain"
    return str(sub.iloc[-1]["label"])


def growth_weight_for_label(label: str) -> float:
    return float(REGIME_GROWTH_WEIGHT.get(str(label), DEFAULT_UNCERTAIN_GROWTH_WEIGHT))


def growth_weight_by_date(
    labels: pd.DataFrame,
    score_dates: Sequence[str],
) -> Dict[str, float]:
    """Map each score_date to a growth weight using as-of regime labels."""
    out: Dict[str, float] = {}
    for raw_date in score_dates:
        date = str(raw_date)
        label = label_as_of(labels, date)
        out[date] = growth_weight_for_label(label)
    return out


def build_regime_weight_schedule(
    index_rows: Sequence[Dict[str, Any]],
    rebalance_dates: Sequence[str],
    *,
    confirm_days: int = CONFIRM_DAYS_DEFAULT,
    exit_confirm_days: Optional[int] = EXIT_CONFIRM_DAYS_DEFAULT,
    non_bull_exits: bool = False,
    regime_rule: Union[str, RegimeRuleSpec, Mapping[str, Any], None] = None,
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    """Build per-rebalance-date growth weights and an audit schedule."""
    rule = load_regime_rule(regime_rule)
    labels = label_index_regime(
        index_rows,
        confirm_days=confirm_days,
        exit_confirm_days=exit_confirm_days,
        non_bull_exits=non_bull_exits,
        regime_rule=rule,
    )
    weight_by_date = growth_weight_by_date(labels, rebalance_dates)
    schedule: List[Dict[str, Any]] = []
    for date in rebalance_dates:
        date_text = str(date)
        label = label_as_of(labels, date_text)
        w_growth = weight_by_date.get(date_text, DEFAULT_UNCERTAIN_GROWTH_WEIGHT)
        schedule.append(
            {
                "score_date": date_text,
                "label": label,
                "w_growth": w_growth,
                "w_cycle": 1.0 - w_growth,
            }
        )
    return weight_by_date, schedule


def build_regime_cash_schedule(
    index_rows: Sequence[Dict[str, Any]],
    score_dates: Sequence[str],
    *,
    confirm_days: int = CONFIRM_DAYS_DEFAULT,
    exit_confirm_days: Optional[int] = EXIT_CONFIRM_DAYS_DEFAULT,
    non_bull_exits: bool = False,
    regime_rule: Union[str, RegimeRuleSpec, Mapping[str, Any], None] = None,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Map dates to invest/cash using as-of bull vs non-bull labels.

    Bull → invest (caller uses growth 60 / cycle 40). Any other label → cash.
    """
    labels = label_index_regime(
        index_rows,
        confirm_days=confirm_days,
        exit_confirm_days=exit_confirm_days,
        non_bull_exits=non_bull_exits,
        regime_rule=regime_rule,
    )
    cash_dates: List[str] = []
    schedule: List[Dict[str, Any]] = []
    for raw_date in score_dates:
        date_text = str(raw_date)
        label = label_as_of(labels, date_text)
        action = "invest" if label == "bull" else "cash"
        if action == "cash":
            cash_dates.append(date_text)
        schedule.append({"score_date": date_text, "label": label, "action": action})
    return cash_dates, schedule


def coerce_optional_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"", "none", "null"}:
        return None
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"regime_cash must be a boolean, got {value!r}")


def regime_cash_requested(universe_index: Any = None, regime_cash: Any = None) -> bool:
    """Return whether bull-invest / non-bull-cash was explicitly requested.

    Universe is not gated. Unset or false stays off; only explicit true enables.
    ``universe_index`` is kept for call-site compatibility and ignored.
    """
    _ = universe_index
    return coerce_optional_bool(regime_cash) is True


def regime_distribution_in_window(
    labels: pd.DataFrame,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, int]:
    """Count regime labels on index trading days inside an optional window."""
    if labels.empty:
        return {"bull": 0, "bear": 0, "uncertain": 0}
    frame = labels.copy()
    if start_date:
        frame = frame[frame["trade_date"] >= str(start_date)]
    if end_date:
        frame = frame[frame["trade_date"] <= str(end_date)]
    dist = {"bull": 0, "bear": 0, "uncertain": 0}
    for label in frame["label"].astype(str):
        dist[label if label in dist else "uncertain"] += 1
    return dist


def count_regime_flips(
    labels: pd.DataFrame,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    """Count label transitions on index trading days inside the window."""
    if labels.empty:
        return 0
    frame = labels.copy()
    if start_date:
        frame = frame[frame["trade_date"] >= str(start_date)]
    if end_date:
        frame = frame[frame["trade_date"] <= str(end_date)]
    if len(frame) < 2:
        return 0
    series = frame["label"].astype(str).tolist()
    return sum(1 for left, right in zip(series, series[1:]) if left != right)


def longest_regime_run(
    labels: pd.DataFrame,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, int]:
    """Longest consecutive run per label inside the window."""
    if labels.empty:
        return {"bull": 0, "bear": 0, "uncertain": 0}
    frame = labels.copy()
    if start_date:
        frame = frame[frame["trade_date"] >= str(start_date)]
    if end_date:
        frame = frame[frame["trade_date"] <= str(end_date)]
    best = {"bull": 0, "bear": 0, "uncertain": 0}
    if frame.empty:
        return best
    current_label = str(frame.iloc[0]["label"])
    current_run = 1
    for label in frame.iloc[1:]["label"].astype(str):
        if label == current_label:
            current_run += 1
        else:
            key = current_label if current_label in best else "uncertain"
            best[key] = max(best[key], current_run)
            current_label = label
            current_run = 1
    key = current_label if current_label in best else "uncertain"
    best[key] = max(best[key], current_run)
    return best


def extract_regime_segments(
    labels: pd.DataFrame,
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Merge contiguous index labels into segments within an optional window.

    The last segment ending on ``end_date`` is marked ``ongoing=True`` when the
    sample ends while still in that state (cutoff, not a detected regime end).
    """
    if labels.empty:
        return []
    frame = labels.copy()
    if start_date:
        frame = frame[frame["trade_date"] >= str(start_date)]
    if end_date:
        frame = frame[frame["trade_date"] <= str(end_date)]
    if frame.empty:
        return []

    segments: List[Dict[str, Any]] = []
    current_label = str(frame.iloc[0]["label"])
    seg_start = str(frame.iloc[0]["trade_date"])
    prev_date = seg_start
    trading_days = 1

    for _, row in frame.iloc[1:].iterrows():
        trade_date = str(row["trade_date"])
        label = str(row["label"])
        if label == current_label:
            trading_days += 1
            prev_date = trade_date
            continue
        segments.append(
            {
                "label": current_label,
                "start": seg_start,
                "end": prev_date,
                "trading_days": trading_days,
                "ongoing": False,
            }
        )
        current_label = label
        seg_start = trade_date
        prev_date = trade_date
        trading_days = 1

    sample_end = str(end_date) if end_date else str(frame.iloc[-1]["trade_date"])
    last_end = str(frame.iloc[-1]["trade_date"])
    segments.append(
        {
            "label": current_label,
            "start": seg_start,
            "end": last_end,
            "trading_days": trading_days,
            "ongoing": last_end >= sample_end,
        }
    )
    return segments


def longest_segment_for_label(
    segments: Sequence[Dict[str, Any]],
    label: str,
) -> Optional[Dict[str, Any]]:
    """Return the longest contiguous segment for ``label``."""
    matches = [segment for segment in segments if str(segment.get("label")) == label]
    if not matches:
        return None
    return max(matches, key=lambda segment: int(segment.get("trading_days") or 0))
