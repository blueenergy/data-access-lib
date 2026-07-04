from __future__ import annotations

from stock_data_access.index_constituents import (
    asof_snapshot_symbols,
    latest_snapshot_symbols,
    load_snapshot,
)


class _FakeCollection:
    def __init__(self, docs):
        self.docs = list(docs)

    def find_one(self, query, projection=None, sort=None):
        matched = [d for d in self.docs if _matches(d, query)]
        if not matched:
            return None
        if sort:
            for field, direction in reversed(sort):
                matched.sort(key=lambda row: row.get(field, ""), reverse=direction < 0)
        return dict(matched[0])

    def distinct(self, field, query):
        return sorted(
            {
                row.get(field)
                for row in self.docs
                if _matches(row, query) and row.get(field)
            }
        )


def _matches(doc, query):
    for key, expected in query.items():
        value = doc.get(key)
        if isinstance(expected, dict):
            if "$lte" in expected and not (value is not None and str(value) <= str(expected["$lte"])):
                return False
            continue
        if value != expected:
            return False
    return True


def test_latest_snapshot_excludes_removed_symbols():
    coll = _FakeCollection(
        [
            {
                "index_code": "csi1000",
                "weight_trade_date": "20260101",
                "update_date": "20260102",
                "symbol": "000001.SZ",
            },
            {
                "index_code": "csi1000",
                "weight_trade_date": "20260101",
                "update_date": "20260102",
                "symbol": "000002.SZ",
            },
            {
                "index_code": "csi1000",
                "weight_trade_date": "20251201",
                "update_date": "20251202",
                "symbol": "000003.SZ",
            },
        ]
    )

    assert latest_snapshot_symbols(coll, "csi1000") == ["000001.SZ", "000002.SZ"]


def test_asof_snapshot_uses_latest_snapshot_on_or_before_date():
    coll = _FakeCollection(
        [
            {
                "index_code": "csi1000",
                "weight_trade_date": "20230131",
                "update_date": "20230201",
                "symbol": "000001.SZ",
            },
            {
                "index_code": "csi1000",
                "weight_trade_date": "20230131",
                "update_date": "20230201",
                "symbol": "000002.SZ",
            },
            {
                "index_code": "csi1000",
                "weight_trade_date": "20230630",
                "update_date": "20230701",
                "symbol": "000003.SZ",
            },
        ]
    )

    assert asof_snapshot_symbols(coll, "csi1000", "20230315") == ["000001.SZ", "000002.SZ"]
    assert asof_snapshot_symbols(coll, "csi1000", "20230801") == ["000003.SZ"]


def test_latest_snapshot_supports_legacy_docs_without_weight_trade_date():
    coll = _FakeCollection(
        [
            {"index_code": "csi1000", "update_date": "20251202", "symbol": "000003.SZ"},
            {"index_code": "csi1000", "update_date": "20260102", "symbol": "000001.SZ"},
            {"index_code": "csi1000", "update_date": "20260102", "symbol": "000002.SZ"},
        ]
    )

    assert latest_snapshot_symbols(coll, "csi1000") == ["000001.SZ", "000002.SZ"]


def test_asof_snapshot_supports_legacy_docs_without_weight_trade_date():
    coll = _FakeCollection(
        [
            {"index_code": "csi1000", "update_date": "20230201", "symbol": "000001.SZ"},
            {"index_code": "csi1000", "update_date": "20230701", "symbol": "000003.SZ"},
        ]
    )

    assert asof_snapshot_symbols(coll, "csi1000", "20230315") == ["000001.SZ"]


def test_load_snapshot_returns_metadata():
    coll = _FakeCollection(
        [
            {
                "index_code": "csi1000",
                "weight_trade_date": "20230131",
                "update_date": "20230201",
                "symbol": "000001.SZ",
            }
        ]
    )
    snap = load_snapshot(coll, "csi1000", as_of_date="20230201")
    assert snap is not None
    assert snap.weight_trade_date == "20230131"
    assert snap.update_date == "20230201"
    assert snap.symbols == ("000001.SZ",)
