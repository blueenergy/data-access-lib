from __future__ import annotations

import pytest

from stock_data_access.board_lots import (
    BELOW_LOT_SIZE_BLOCKER,
    BELOW_STAR_MIN_BLOCKER,
    STAR_MIN_ORDER_SHARES,
    board_min_order_shares,
    is_star_market,
    normalize_board_target_shares,
    round_board_lot_shares,
)


# --------------------------------------------------------------------------
# is_star_market
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("688627.SH", True),
        ("688981.SH", True),
        ("689009.SH", True),
        ("688627", True),  # no suffix
        ("600519.SH", False),  # SSE main board
        ("605376.SH", False),  # SSE main board 605
        ("601208.SH", False),
        ("000858.SZ", False),  # SZSE main board
        ("300398.SZ", False),  # ChiNext
        ("301526.SZ", False),  # ChiNext
        ("002859.SZ", False),  # SME
        ("830799.BJ", False),  # BSE
        ("920819.BJ", False),  # BSE
        ("", False),
        (None, False),
        ("  688627.SH  ", True),  # whitespace tolerated
    ],
)
def test_is_star_market(symbol, expected):
    assert is_star_market(symbol) is expected


# --------------------------------------------------------------------------
# board_min_order_shares
# --------------------------------------------------------------------------
def test_board_min_order_shares_star_is_200():
    assert board_min_order_shares("688627.SH") == 200
    assert board_min_order_shares("688627.SH", lot_size=100) == 200


def test_board_min_order_shares_non_star_is_lot():
    assert board_min_order_shares("600519.SH") == 100
    assert board_min_order_shares("300398.SZ", lot_size=100) == 100
    assert board_min_order_shares("000858.SZ", lot_size=200) == 200


# --------------------------------------------------------------------------
# round_board_lot_shares
# --------------------------------------------------------------------------
def test_round_board_lot_shares_non_star_floors_to_lot():
    # 100_000 / 100 = 1000 -> already lot multiple
    assert round_board_lot_shares(100_000, 100.0, 100, "600519.SH") == 1000
    # 12_345 / 100 = 123 -> floor to 100
    assert round_board_lot_shares(12_345, 100.0, 100, "600519.SH") == 100
    # below one lot -> 0
    assert round_board_lot_shares(5_000, 100.0, 100, "600519.SH") == 0


def test_round_board_lot_shares_star_is_one_share_granular():
    # 140_000 / 560 = 250 -> STAR keeps 250 (not floored to 200)
    assert round_board_lot_shares(140_000, 560.0, 100, "688627.SH") == 250
    # 56_000 / 560 = 100 -> STAR keeps raw 100 (order-min enforced later)
    assert round_board_lot_shares(56_000, 560.0, 100, "688627.SH") == 100


@pytest.mark.parametrize("price", [0, None, -1.0])
def test_round_board_lot_shares_bad_price_returns_zero(price):
    assert round_board_lot_shares(100_000, price, 100, "688627.SH") == 0
    assert round_board_lot_shares(100_000, price, 100, "600519.SH") == 0


def test_round_board_lot_shares_zero_amount_returns_zero():
    assert round_board_lot_shares(0, 560.0, 100, "688627.SH") == 0


# --------------------------------------------------------------------------
# normalize_board_target_shares : non-STAR passthrough / legacy lot behavior
# --------------------------------------------------------------------------
def test_normalize_non_star_buy_rounds_delta_to_lot():
    target, blockers = normalize_board_target_shares(0, 300, 10.0, 100, "600519.SH")
    assert target == 300
    assert blockers == []


def test_normalize_non_star_buy_below_lot_blocked():
    target, blockers = normalize_board_target_shares(400, 450, 10.0, 100, "600519.SH")
    assert target == 400
    assert blockers == [BELOW_LOT_SIZE_BLOCKER]


def test_normalize_non_star_full_exit():
    target, blockers = normalize_board_target_shares(500, 0, 10.0, 100, "600519.SH")
    assert target == 0
    assert blockers == []


def test_normalize_non_star_sell_below_lot_blocked():
    target, blockers = normalize_board_target_shares(500, 450, 10.0, 100, "600519.SH")
    assert target == 500
    assert blockers == [BELOW_LOT_SIZE_BLOCKER]


# --------------------------------------------------------------------------
# normalize_board_target_shares : STAR buys
# --------------------------------------------------------------------------
def test_star_fresh_buy_at_or_above_min_kept():
    target, blockers = normalize_board_target_shares(0, 250, 560.0, 100, "688627.SH")
    assert target == 250
    assert blockers == []


def test_star_fresh_buy_exactly_min_kept():
    target, blockers = normalize_board_target_shares(0, 200, 560.0, 100, "688627.SH")
    assert target == 200
    assert blockers == []


def test_star_sub_min_buy_bumped_when_cash_unknown():
    # available_cash None => unknown => allowed to bump to 200
    target, blockers = normalize_board_target_shares(0, 100, 560.0, 100, "688627.SH")
    assert target == 200
    assert blockers == []


def test_star_sub_min_buy_bumped_when_cash_sufficient():
    target, blockers = normalize_board_target_shares(
        0, 100, 560.0, 100, "688627.SH", available_cash=200 * 560.0
    )
    assert target == 200
    assert blockers == []


def test_star_sub_min_buy_skipped_when_cash_insufficient():
    target, blockers = normalize_board_target_shares(
        0, 100, 560.0, 100, "688627.SH", available_cash=200 * 560.0 - 1
    )
    assert target == 0
    assert blockers == [BELOW_STAR_MIN_BLOCKER]


def test_star_sub_min_buy_skipped_without_price():
    target, blockers = normalize_board_target_shares(0, 100, None, 100, "688627.SH")
    assert target == 0
    assert blockers == [BELOW_STAR_MIN_BLOCKER]


def test_star_addon_tiny_delta_below_threshold_skipped():
    # current 300, desired +50 (< 100) -> too small, skip (position kept)
    target, blockers = normalize_board_target_shares(300, 350, 560.0, 100, "688627.SH")
    assert target == 300
    assert blockers == [BELOW_STAR_MIN_BLOCKER]


def test_star_addon_delta_at_threshold_bumps_to_min_order():
    # current 300, desired +100 (>= 100 threshold, < 200) -> bump order to 200
    target, blockers = normalize_board_target_shares(300, 400, 560.0, 100, "688627.SH")
    assert target == 500
    assert blockers == []


def test_star_buy_delta_just_below_threshold_skipped():
    # fresh buy of 99 shares (< 100) -> skip
    target, blockers = normalize_board_target_shares(0, 99, 560.0, 100, "688627.SH")
    assert target == 0
    assert blockers == [BELOW_STAR_MIN_BLOCKER]


def test_star_buy_delta_exactly_threshold_bumps():
    # fresh buy of exactly 100 shares (== threshold) -> bump to 200
    target, blockers = normalize_board_target_shares(0, 100, 560.0, 100, "688627.SH")
    assert target == 200
    assert blockers == []


def test_star_addon_at_min_kept():
    target, blockers = normalize_board_target_shares(300, 500, 560.0, 100, "688627.SH")
    assert target == 500
    assert blockers == []


# --------------------------------------------------------------------------
# normalize_board_target_shares : STAR sells
# --------------------------------------------------------------------------
def test_star_full_exit_allowed():
    target, blockers = normalize_board_target_shares(250, 0, 560.0, 100, "688627.SH")
    assert target == 0
    assert blockers == []


def test_star_sell_leaving_odd_lot_liquidates_fully():
    # current 250, desired 100 (< 200 remaining) -> liquidate all
    target, blockers = normalize_board_target_shares(250, 100, 560.0, 100, "688627.SH")
    assert target == 0
    assert blockers == []


def test_star_sell_odd_holding_below_min_liquidates_fully():
    # holds 150 (an odd lot), wants to trim to 50 -> full exit
    target, blockers = normalize_board_target_shares(150, 50, 560.0, 100, "688627.SH")
    assert target == 0
    assert blockers == []


def test_star_sell_delta_above_min_kept():
    # current 600, desired 300: sell 300 (>= 200), remaining 300 (>= 200)
    target, blockers = normalize_board_target_shares(600, 300, 560.0, 100, "688627.SH")
    assert target == 300
    assert blockers == []


def test_star_sell_delta_below_min_with_valid_remainder_skipped():
    # current 600, desired 500: sell 100 (< 200), remaining 500 (>= 200) -> skip trim
    target, blockers = normalize_board_target_shares(600, 500, 560.0, 100, "688627.SH")
    assert target == 600
    assert blockers == [BELOW_STAR_MIN_BLOCKER]


def test_star_no_change_when_target_equals_current():
    target, blockers = normalize_board_target_shares(300, 300, 560.0, 100, "688627.SH")
    assert target == 300
    assert blockers == []


# --------------------------------------------------------------------------
# Regression: the 688627.SH incident (100-share STAR limit buy was rejected)
# --------------------------------------------------------------------------
def test_incident_688627_100_share_buy_is_corrected():
    # Was: round to 100 shares -> broker 废单. Now: bump to 200 (cash ok) or skip.
    price = 562.0
    target, blockers = normalize_board_target_shares(
        0, 100, price, 100, "688627.SH", available_cash=1_000_000
    )
    assert target == STAR_MIN_ORDER_SHARES
    assert blockers == []
    # And with a tight budget it is skipped rather than sent as an invalid order.
    target2, blockers2 = normalize_board_target_shares(
        0, 100, price, 100, "688627.SH", available_cash=10_000
    )
    assert target2 == 0
    assert blockers2 == [BELOW_STAR_MIN_BLOCKER]
