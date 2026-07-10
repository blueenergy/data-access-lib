"""Evidence fingerprint helpers for signal review short-circuiting.

The fingerprint is intentionally based on the evidence snapshot that is fed to
the LLM, not on the LLM output.  It answers a narrow question: did the input
evidence set change since the last successful analysis?
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_TRACKING_PARAM_PREFIXES = ("utm_",)
_TRACKING_PARAM_NAMES = {
    "spm",
    "from",
    "source",
    "share_token",
    "shareid",
    "isappinstalled",
}


def _sha256_short(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def normalize_title(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"^[【\\[]?(快讯|公告|转发|转载|原创)[】\\]]?", "", text)
    return text


def normalize_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return raw
    query_pairs = []
    for key, val in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered in _TRACKING_PARAM_NAMES or any(lowered.startswith(prefix) for prefix in _TRACKING_PARAM_PREFIXES):
            continue
        query_pairs.append((key, val))
    query = urlencode(sorted(query_pairs))
    return urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            query,
            "",
        )
    )


def normalize_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    if re.match(r"^\d{4}-\d{2}-\d{2}", text):
        return text[:10]
    return text[:10]


def parse_evidence_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("T", " ").replace("Z", "")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d"):
        try:
            return datetime.strptime(re.sub(r"\D", "", normalized)[: len(datetime.now().strftime(fmt))], fmt)
        except ValueError:
            continue
    digits = re.sub(r"\D", "", text)
    if len(digits) >= 8:
        try:
            return datetime.strptime(digits[:8], "%Y%m%d")
        except ValueError:
            return None
    return None


def evidence_key(row: Mapping[str, Any]) -> str:
    source = str(row.get("source") or row.get("evidence_type") or "").strip().lower()
    title = row.get("title") or row.get("source_title") or ""
    url = normalize_url(row.get("url") or row.get("source_url") or "")
    datetime_text = row.get("datetime") or row.get("publish_time") or row.get("date") or row.get("source_date") or ""
    date = normalize_date(datetime_text)

    if source == "cninfo_announcement" and url:
        return f"cninfo:{url}:{date}"
    if url:
        return f"url:{url}"
    normalized_title = normalize_title(title)
    if normalized_title:
        return f"title:{normalized_title}:{date}"
    return f"source:{source}:{date}:{str(title or '')[:40]}"


def evidence_keys(rows: Iterable[Mapping[str, Any]]) -> List[str]:
    keys = {evidence_key(row) for row in rows if isinstance(row, Mapping)}
    return sorted(key for key in keys if key.strip(":"))


def fingerprint_from_keys(keys: Iterable[str]) -> str:
    ordered = sorted({str(key) for key in keys if str(key).strip()})
    return _sha256_short("\n".join(ordered))


def latest_evidence_at(rows: Iterable[Mapping[str, Any]]) -> Optional[datetime]:
    parsed = [
        parse_evidence_datetime(
            row.get("datetime") or row.get("publish_time") or row.get("date") or row.get("source_date")
        )
        for row in rows
        if isinstance(row, Mapping)
    ]
    parsed = [dt for dt in parsed if dt is not None]
    return max(parsed) if parsed else None


def build_symbol_fingerprints(evidence_snapshot: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return symbol/sector/combined fingerprints keyed by symbol."""
    sector_rows = list(evidence_snapshot.get("sector_news") or [])
    sector_keys = evidence_keys(sector_rows)
    sector_fp = fingerprint_from_keys(sector_keys)
    sector_latest = latest_evidence_at(sector_rows)
    results: Dict[str, Dict[str, Any]] = {}
    symbol_news = evidence_snapshot.get("symbol_news") or {}
    for symbol, rows in symbol_news.items():
        symbol_rows = list(rows or [])
        symbol_keys = evidence_keys(symbol_rows)
        symbol_fp = fingerprint_from_keys(symbol_keys)
        combined = fingerprint_from_keys([symbol_fp, sector_fp])
        latest = latest_evidence_at([*symbol_rows, *sector_rows])
        results[str(symbol)] = {
            "symbol_fingerprint": symbol_fp,
            "sector_fingerprint": sector_fp,
            "combined_fingerprint": combined,
            "evidence_count": len(symbol_keys) + len(sector_keys),
            "latest_evidence_at": latest or sector_latest,
            "symbol_evidence_count": len(symbol_keys),
            "sector_evidence_count": len(sector_keys),
        }
    return results


__all__ = [
    "build_symbol_fingerprints",
    "evidence_key",
    "evidence_keys",
    "fingerprint_from_keys",
    "latest_evidence_at",
    "normalize_date",
    "normalize_title",
    "normalize_url",
]
