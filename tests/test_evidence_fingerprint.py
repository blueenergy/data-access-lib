from __future__ import annotations

from datetime import datetime

from stock_data_access.evidence_fingerprint import (
    build_symbol_fingerprints,
    evidence_key,
    fingerprint_from_keys,
    normalize_url,
)


def test_normalize_url_strips_tracking_params_and_fragment():
    assert (
        normalize_url("HTTP://Example.COM/a.pdf?utm_source=x&b=2&a=1#frag")
        == "http://example.com/a.pdf?a=1&b=2"
    )


def test_evidence_key_prefers_cninfo_url_and_date():
    row = {
        "source": "cninfo_announcement",
        "title": "测试公告",
        "datetime": "2026-07-10 09:30:00",
        "url": "http://static.cninfo.com.cn/finalpage.pdf",
    }

    assert evidence_key(row) == "cninfo:http://static.cninfo.com.cn/finalpage.pdf:2026-07-10"


def test_build_symbol_fingerprints_separates_symbol_and_sector_keys():
    snapshot = {
        "sector_news": [
            {
                "source": "sina_search",
                "title": "半导体政策",
                "datetime": "2026-07-09",
                "url": "https://example.com/sector",
            }
        ],
        "symbol_news": {
            "688001.SH": [
                {
                    "source": "cninfo_announcement",
                    "title": "测试股份问询函",
                    "datetime": "2026-07-10 10:00:00",
                    "url": "http://static.cninfo.com.cn/688001.pdf",
                }
            ],
            "300001.SZ": [],
        },
    }

    result = build_symbol_fingerprints(snapshot)

    assert result["688001.SH"]["evidence_count"] == 2
    assert result["300001.SZ"]["evidence_count"] == 1
    assert result["688001.SH"]["combined_fingerprint"] != result["300001.SZ"]["combined_fingerprint"]
    assert result["688001.SH"]["latest_evidence_at"] == datetime(2026, 7, 10, 10, 0, 0)
    assert result["300001.SZ"]["sector_fingerprint"] == fingerprint_from_keys(["url:https://example.com/sector"])
