import pytest

from stock_data_access.score_utils import (
    DEFAULT_WEIGHTED_SCORE_WEIGHTS,
    extract_strategy_score,
    parse_score_weights,
    safe_float,
    safe_round,
    safe_round_or_none,
    weighted_dimension_score,
)


def test_safe_float_dict_balanced():
    assert safe_float({"balanced": 7.5}) == 7.5


def test_safe_float_dict_first_numeric():
    assert safe_float({"x": 3, "y": 9}) == 3.0


def test_safe_float_scalar():
    assert safe_float("4.2") == 4.2
    assert safe_float(None, default=1.0) == 1.0


def test_safe_round():
    assert safe_round({"balanced": 7.556}) == 7.56


def test_safe_round_or_none_preserves_na():
    assert safe_round_or_none(None) is None
    assert safe_round_or_none(7.556) == 7.56


def test_extract_strategy_score():
    assert extract_strategy_score({"balanced": 5, "aggressive": 8}, "balanced") == 5.0
    assert extract_strategy_score({"aggressive": 8}, "balanced") == 8.0
    assert extract_strategy_score(6.5) == 6.5


def test_parse_score_weights_defaults():
    assert parse_score_weights(None) == DEFAULT_WEIGHTED_SCORE_WEIGHTS


def test_parse_score_weights_colon_and_semicolon():
    assert parse_score_weights("growth:0.6;cycle:0.4") == {"growth": 0.6, "cycle": 0.4}
    assert parse_score_weights("growth=0.3,cycle=0.7") == {"growth": 0.3, "cycle": 0.7}


def test_parse_score_weights_errors():
    with pytest.raises(ValueError, match="invalid weight token"):
        parse_score_weights("growth")
    with pytest.raises(ValueError, match="unsupported weight dimension"):
        parse_score_weights("foo:1")
    with pytest.raises(ValueError, match="invalid weight for growth"):
        parse_score_weights("growth:abc")
    with pytest.raises(ValueError, match="must be non-negative"):
        parse_score_weights("growth:-1")
    with pytest.raises(ValueError, match="at least one weight must be positive"):
        parse_score_weights("growth:0")


def test_weighted_dimension_score_normalizes():
    row = {"growth_score": 80.0, "cycle_score": 40.0}
    weights = {"growth": 1.0, "cycle": 1.0}
    assert weighted_dimension_score(row, weights) == 60.0
