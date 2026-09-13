import pytest

from stock_data_access.score_query import (
    apply_score_scope,
    merge_score_scope,
    row_covers_score_scope,
    score_scope_filter,
    tagged_index_codes_filter,
)


def test_score_scope_filter_none_is_explicit_unscoped():
    assert score_scope_filter(None) == {}


def test_score_scope_filter_string_uses_array_contains():
    assert score_scope_filter("csi1000") == {"index_codes": "csi1000"}
    assert score_scope_filter("  hs300  ") == {"index_codes": "hs300"}


def test_score_scope_filter_sequence_uses_in():
    assert score_scope_filter(["csi1000"]) == {"index_codes": "csi1000"}
    assert score_scope_filter(("hs300", "csi1000")) == {"index_codes": {"$in": ["hs300", "csi1000"]}}


def test_score_scope_filter_non_scope_values_are_unscoped():
    class Query:
        default = None

    assert score_scope_filter(Query()) == {}
    assert score_scope_filter(object()) == {}
    with pytest.raises(ValueError, match="non-empty"):
        score_scope_filter("")
    with pytest.raises(ValueError, match="non-empty"):
        score_scope_filter(["", "  "])


def test_merge_and_apply_preserve_caller_query():
    base = {"symbol": "000001.SZ", "score_date": "20260912"}
    merged = merge_score_scope(base, "csi1000")
    assert merged == {
        "symbol": "000001.SZ",
        "score_date": "20260912",
        "index_codes": "csi1000",
    }
    assert base == {"symbol": "000001.SZ", "score_date": "20260912"}
    apply_score_scope(base, None)
    assert base == {"symbol": "000001.SZ", "score_date": "20260912"}


def test_tagged_index_codes_filter_requires_first_element():
    assert tagged_index_codes_filter() == {"index_codes.0": {"$exists": True}}


def test_row_covers_score_scope():
    assert row_covers_score_scope([], None) is True
    assert row_covers_score_scope(["csi1000"], "csi1000") is True
    assert row_covers_score_scope([], "csi1000") is False
    assert row_covers_score_scope(None, "csi1000") is False
    assert row_covers_score_scope(["csi1000"], ["csi1000", "hs300"]) is False
    assert row_covers_score_scope(["csi1000", "hs300"], ["csi1000"]) is True
