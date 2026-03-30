from __future__ import annotations

import csv
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import base64
import json
import math
from pathlib import Path
import re
from typing import Any, Callable
from urllib.parse import parse_qs, quote_plus, urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from betbot.dns_guard import urlopen_with_dns_recovery
from betbot.kalshi_nonsports_priors import PRIOR_FIELDNAMES, load_prior_rows
from betbot.kalshi_nonsports_quality import _parse_timestamp, load_history_rows
from betbot.kalshi_nonsports_research_queue import build_research_queue_rows


NewsGetter = Callable[[str, float], str]

GOOGLE_NEWS_RSS_TEMPLATE = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

HIGH_TRUST_DOMAINS = {
    "whitehouse.gov",
    "sec.gov",
    "senate.gov",
    "house.gov",
    "congress.gov",
    "federalreserve.gov",
    "treasury.gov",
    "bls.gov",
    "bea.gov",
    "census.gov",
}
MEDIUM_TRUST_DOMAINS = {
    "reuters.com",
    "apnews.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "nytimes.com",
    "washingtonpost.com",
    "cnbc.com",
}

FAMILY_BASE_PRIOR = {
    "cabinet_exit": 0.07,
    "appointment_confirmation": 0.55,
    "ipo_announcement": 0.03,
    "merger_announcement": 0.05,
    "trailer_release": 0.08,
    "media_release": 0.10,
    "general_event": 0.50,
}

FAMILY_HINTS = {
    "cabinet_exit": "resign OR leaves office OR fired",
    "appointment_confirmation": "confirmed OR nomination OR appointment",
    "ipo_announcement": "IPO OR S-1 OR listing",
    "merger_announcement": "merger agreement OR acquisition announced",
    "trailer_release": "trailer released OR teaser",
    "media_release": "release date OR premiere",
    "general_event": "announcement OR official statement",
}

FAMILY_SIGNAL_TOKENS: dict[str, dict[str, tuple[str, ...]]] = {
    "cabinet_exit": {
        "yes": ("resigns", "resignation", "steps down", "fired", "removed", "out as"),
        "no": ("remains", "stays on", "backs", "no plans to leave", "denies resignation"),
    },
    "appointment_confirmation": {
        "yes": ("confirmed", "approved", "sworn in", "appointed"),
        "no": ("withdraws", "blocked", "fails confirmation", "rejected"),
    },
    "ipo_announcement": {
        "yes": ("files for ipo", "ipo filing", "s-1", "go public", "listing announced"),
        "no": ("no plans to ipo", "stays private", "private funding", "delays ipo"),
    },
    "merger_announcement": {
        "yes": ("merger agreement", "acquisition announced", "deal announced", "combine with"),
        "no": ("talks only", "no deal", "deal collapses", "exploring options"),
    },
    "trailer_release": {
        "yes": ("trailer released", "teaser released", "first trailer"),
        "no": ("trailer delayed", "no trailer yet"),
    },
    "media_release": {
        "yes": ("premieres", "release date announced", "launches"),
        "no": ("delayed", "pushed back", "postponed"),
    },
}

DEFAULT_SIGNAL_TOKENS = {
    "yes": ("announced", "confirmed", "approved", "released", "files"),
    "no": ("denies", "delayed", "postponed", "unlikely", "stays"),
}

AUTO_PRIOR_FIELDNAMES = [
    "market_ticker",
    "fair_yes_probability",
    "fair_yes_probability_low",
    "fair_yes_probability_high",
    "confidence",
    "thesis",
    "source_note",
    "updated_at",
    "evidence_count",
    "high_trust_evidence_count",
    "evidence_quality",
    "source_type",
    "last_evidence_at",
    "market_family",
    "resolution_source_type",
    "research_priority_score",
]


def _normalize_string_set(values: tuple[str, ...] | list[str] | set[str] | None) -> set[str]:
    if not values:
        return set()
    normalized: set[str] = set()
    for value in values:
        text = str(value or "").strip().lower()
        if text:
            normalized.add(text)
    return normalized


def _normalize_market_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _canonical_lookup_keys(value: Any) -> list[str]:
    ticker = _normalize_market_ticker(value)
    if not ticker:
        return []
    keys: list[str] = []

    def _add_key(raw: str) -> None:
        candidate = _normalize_market_ticker(raw)
        if candidate and candidate not in keys:
            keys.append(candidate)

    _add_key(ticker)
    for marker in ("-T", "-B", "-P"):
        if marker in ticker:
            _add_key(ticker.split(marker, 1)[0])
    if ticker.endswith("-PART"):
        _add_key(ticker[: -len("-PART")])
    if "-" in ticker:
        prefix, suffix = ticker.rsplit("-", 1)
        if suffix in {"PART", "YES", "NO"}:
            _add_key(prefix)
        if suffix and suffix[0] in {"T", "B", "P"} and any(ch.isdigit() for ch in suffix):
            _add_key(prefix)
    if "-" in ticker:
        _add_key(ticker.split("-", 1)[0])
    return keys


def _load_mapped_live_ticker_index(
    *,
    canonical_mapping_csv: str | None,
    allowed_canonical_niches: set[str] | None = None,
) -> tuple[set[str], dict[str, str]]:
    if not canonical_mapping_csv:
        return set(), {}
    path = Path(canonical_mapping_csv)
    if not path.exists():
        return set(), {}
    mapped_tickers: set[str] = set()
    niche_choices_by_lookup_key: dict[str, set[str]] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            mapping_status = str(row.get("mapping_status") or "").strip().lower()
            if mapping_status != "mapped":
                continue
            ticker = _normalize_market_ticker(row.get("live_market_ticker"))
            if not ticker:
                continue
            niche = str(row.get("niche") or "").strip().lower()
            if allowed_canonical_niches and niche not in allowed_canonical_niches:
                continue
            for raw_lookup_value in (row.get("live_market_ticker"), row.get("live_event_ticker")):
                for lookup_key in _canonical_lookup_keys(raw_lookup_value):
                    mapped_tickers.add(lookup_key)
                    niche_choices_by_lookup_key.setdefault(lookup_key, set()).add(niche)
    niche_by_ticker: dict[str, str] = {}
    for lookup_key, niche_choices in niche_choices_by_lookup_key.items():
        if len(niche_choices) == 1:
            niche_by_ticker[lookup_key] = next(iter(niche_choices))
    return mapped_tickers, niche_by_ticker


def _http_get_text(url: str, timeout_seconds: float) -> str:
    request = Request(
        url,
        headers={
            "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
            "User-Agent": "betbot-kalshi-auto-priors/1.0",
        },
        method="GET",
    )
    with urlopen_with_dns_recovery(
        request,
        timeout_seconds=timeout_seconds,
        urlopen_fn=urlopen,
    ) as response:
        return response.read().decode("utf-8", errors="replace")


def _clamp_probability(value: float) -> float:
    return round(min(0.999, max(0.001, float(value))), 6)


def _parse_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _latest_market_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)
    latest_rows: dict[str, dict[str, str]] = {}
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest_rows[ticker] = rows_sorted[-1]
    return latest_rows


def _infer_market_family(row: dict[str, Any]) -> str:
    merged = " ".join(
        (
            str(row.get("market_ticker") or ""),
            str(row.get("market_title") or ""),
            str(row.get("event_title") or ""),
            str(row.get("event_sub_title") or ""),
        )
    ).lower()
    if any(token in merged for token in ("ipo", "go public", "public offering", "s-1")):
        return "ipo_announcement"
    if any(token in merged for token in ("merger", "acquisition", "acquire", "buyout")):
        return "merger_announcement"
    if any(token in merged for token in ("trailer",)):
        return "trailer_release"
    if any(token in merged for token in ("release", "premiere", "season")):
        return "media_release"
    if any(token in merged for token in ("resign", "leave office", "fired", "out by", "out as", "cabinet")):
        return "cabinet_exit"
    if any(token in merged for token in ("appoint", "confirmation", "confirm", "nominated")):
        return "appointment_confirmation"
    return "general_event"


def _infer_resolution_source_type(row: dict[str, Any]) -> str:
    rules = str(row.get("rules_primary") or "").strip().lower()
    if not rules:
        return ""
    if any(token in rules for token in (".gov", "official", "department", "ministry", "white house", "sec filing")):
        return "official_source"
    if any(token in rules for token in ("press release", "company announcement", "company filing")):
        return "company_source"
    if any(token in rules for token in ("reuters", "associated press", "news report")):
        return "news_source"
    return "rules_text"


def _build_search_query(*, market_row: dict[str, str], family: str) -> str:
    market_title = str(market_row.get("market_title") or "").strip()
    event_title = str(market_row.get("event_title") or "").strip()
    ticker = str(market_row.get("market_ticker") or "").strip()
    core = event_title or market_title or ticker
    hint = FAMILY_HINTS.get(family, FAMILY_HINTS["general_event"])
    query = f"{core} {hint} official announcement"
    return " ".join(query.split())


def _market_relevance_tokens(market_row: dict[str, str]) -> tuple[str, ...]:
    stopwords = {
        "will",
        "when",
        "before",
        "after",
        "officially",
        "announce",
        "announcement",
        "ipo",
        "out",
        "leaves",
        "leave",
        "director",
        "secretary",
        "national",
        "intelligence",
        "labor",
        "department",
        "office",
    }
    source = " ".join(
        (
            str(market_row.get("event_title") or ""),
            str(market_row.get("market_title") or ""),
        )
    )
    tokens = re.findall(r"[A-Za-z][A-Za-z'-]{2,}", source)
    filtered: list[str] = []
    for token in tokens:
        if not token[0].isupper():
            continue
        normalized = token.lower().strip("-'")
        if not normalized or normalized in stopwords:
            continue
        if normalized not in filtered:
            filtered.append(normalized)
    return tuple(filtered[:4])


def _rss_items_from_xml(xml_text: str) -> list[dict[str, str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    items: list[dict[str, str]] = []
    for item in root.findall("./channel/item"):
        title = str(item.findtext("title") or "").strip()
        link = str(item.findtext("link") or "").strip()
        pub_date = str(item.findtext("pubDate") or "").strip()
        description = str(item.findtext("description") or "").strip()
        source_tag = item.find("source")
        source_name = str(source_tag.text or "").strip() if source_tag is not None and source_tag.text else ""
        source_url = str(source_tag.get("url") or "").strip() if source_tag is not None else ""
        if not title or not link:
            continue
        items.append(
            {
                "title": title,
                "link": link,
                "pub_date": pub_date,
                "description": description,
                "source_name": source_name,
                "source_url": source_url,
            }
        )
    return items


def _domain_from_url(url: str) -> str:
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query or "")
    for key in ("url", "u"):
        for candidate in query_params.get(key, []):
            candidate_domain = _domain_from_url(candidate)
            if candidate_domain and candidate_domain != "news.google.com":
                return candidate_domain
    if str(parsed.netloc or "").lower().endswith("news.google.com"):
        path_segments = [segment for segment in parsed.path.split("/") if segment]
        if path_segments:
            encoded = path_segments[-1]
            padded = encoded + ("=" * ((4 - len(encoded) % 4) % 4))
            try:
                decoded = base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")
            except (ValueError, UnicodeDecodeError):
                decoded = ""
            match = re.search(r"https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+", decoded)
            if match:
                candidate_domain = _domain_from_url(match.group(0))
                if candidate_domain and candidate_domain != "news.google.com":
                    return candidate_domain
    return str(parsed.netloc or "").lower().removeprefix("www.")


def _parse_pub_date(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _source_quality_score(domain: str) -> float:
    if domain in HIGH_TRUST_DOMAINS or domain.endswith(".gov"):
        return 1.0
    if domain in MEDIUM_TRUST_DOMAINS:
        return 0.85
    if domain:
        return 0.65
    return 0.5


def _token_hits(text: str, tokens: tuple[str, ...]) -> int:
    merged = text.lower()
    return sum(1 for token in tokens if token in merged)


def _signal_score(*, family: str, title: str, description: str) -> float:
    token_map = FAMILY_SIGNAL_TOKENS.get(family, DEFAULT_SIGNAL_TOKENS)
    merged = f"{title} {description}"
    yes_hits = _token_hits(merged, token_map["yes"])
    no_hits = _token_hits(merged, token_map["no"])
    raw = float(yes_hits - no_hits)
    return max(-2.0, min(2.0, raw))


def _recency_weight(*, published_at: datetime | None, now: datetime) -> float:
    if published_at is None:
        return 0.4
    age_days = max(0.0, (now - published_at).total_seconds() / 86400.0)
    return max(0.1, math.exp(-age_days / 14.0))


def _build_auto_prior_from_evidence(
    *,
    market_row: dict[str, str],
    family: str,
    evidence_items: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any] | None:
    if not evidence_items:
        return None
    base_prior = FAMILY_BASE_PRIOR.get(family, FAMILY_BASE_PRIOR["general_event"])
    weighted_signal = 0.0
    weighted_abs_signal = 0.0
    weighted_quality = 0.0
    source_quality_total = 0.0
    positive_votes = 0
    negative_votes = 0
    high_trust_evidence_count = 0
    last_evidence_at: datetime | None = None

    for item in evidence_items:
        signal = float(item["signal"])
        quality = float(item["source_quality"])
        recency = float(item["recency_weight"])
        weight = quality * recency
        weighted_signal += signal * weight
        weighted_abs_signal += abs(signal) * weight
        weighted_quality += weight
        source_quality_total += quality
        if signal > 0:
            positive_votes += 1
        if signal < 0:
            negative_votes += 1
        if quality >= 0.85:
            high_trust_evidence_count += 1
        published_at = item.get("published_at")
        if isinstance(published_at, datetime):
            if last_evidence_at is None or published_at > last_evidence_at:
                last_evidence_at = published_at

    if weighted_quality <= 0 or source_quality_total <= 0:
        return None

    evidence_count = len(evidence_items)
    normalized_signal = math.tanh(weighted_signal / max(1.0, weighted_quality))
    midpoint = _clamp_probability(base_prior + 0.22 * normalized_signal)
    market_yes_bid = _parse_float(market_row.get("yes_bid_dollars"))
    market_yes_ask = _parse_float(market_row.get("yes_ask_dollars"))
    market_midpoint: float | None = None
    if isinstance(market_yes_bid, float) and isinstance(market_yes_ask, float):
        market_midpoint = _clamp_probability((market_yes_bid + market_yes_ask) / 2.0)
    if isinstance(market_midpoint, float):
        if family == "general_event":
            max_deviation = 0.12
        else:
            max_deviation = 0.12 + min(0.12, high_trust_evidence_count * 0.04)
        midpoint = _clamp_probability(
            min(max(midpoint, market_midpoint - max_deviation), market_midpoint + max_deviation)
        )
    conflict = 1.0 - min(1.0, abs(weighted_signal) / max(1e-6, weighted_abs_signal))
    average_source_quality = source_quality_total / evidence_count
    recency_adjusted_quality = weighted_quality / evidence_count
    confidence = (
        0.35
        + min(0.4, evidence_count * 0.05)
        + min(0.15, average_source_quality * 0.15)
        + min(0.05, recency_adjusted_quality * 0.1)
        - (0.15 * conflict)
    )
    confidence = round(min(0.95, max(0.15, confidence)), 6)

    interval_width = 0.38 - (0.22 * confidence) + (0.20 * conflict)
    interval_width = max(0.05, min(0.55, interval_width))
    low = _clamp_probability(midpoint - interval_width / 2.0)
    high = _clamp_probability(midpoint + interval_width / 2.0)
    low = min(low, midpoint)
    high = max(high, midpoint)

    top_evidence = sorted(
        evidence_items,
        key=lambda item: float(item["source_quality"]) * float(item["recency_weight"]) * abs(float(item["signal"])),
        reverse=True,
    )[:3]
    evidence_titles = [str(item["title"]) for item in top_evidence]
    source_names = [str(item["source_name"]) for item in top_evidence if str(item.get("source_name") or "").strip()]
    yes_bias = "supports" if positive_votes >= negative_votes else "leans against"
    thesis = (
        f"Auto prior {yes_bias} the event based on {evidence_count} recent headlines across "
        f"{max(1, len(set(source_names or ['news sources'])))} sources."
    )
    source_note = "; ".join(evidence_titles)

    return {
        "market_ticker": str(market_row.get("market_ticker") or "").strip(),
        "fair_yes_probability": midpoint,
        "fair_yes_probability_low": low,
        "fair_yes_probability_high": high,
        "confidence": confidence,
        "thesis": thesis,
        "source_note": source_note,
        "updated_at": now.isoformat(),
        "evidence_count": evidence_count,
        "high_trust_evidence_count": high_trust_evidence_count,
        "evidence_quality": round(min(1.0, max(0.0, average_source_quality)), 6),
        "source_type": "auto",
        "last_evidence_at": last_evidence_at.isoformat() if isinstance(last_evidence_at, datetime) else "",
        "market_family": family,
        "resolution_source_type": _infer_resolution_source_type(market_row),
    }


def _build_market_anchored_fallback_prior(
    *,
    market_row: dict[str, str],
    family: str,
    reason: str,
    now: datetime,
) -> dict[str, Any]:
    yes_bid = _parse_float(market_row.get("yes_bid_dollars"))
    yes_ask = _parse_float(market_row.get("yes_ask_dollars"))
    if isinstance(yes_bid, float) and isinstance(yes_ask, float):
        midpoint = _clamp_probability((yes_bid + yes_ask) / 2.0)
    else:
        midpoint = _clamp_probability(FAMILY_BASE_PRIOR.get(family, FAMILY_BASE_PRIOR["general_event"]))
    interval_half = 0.08
    low = _clamp_probability(midpoint - interval_half)
    high = _clamp_probability(midpoint + interval_half)
    return {
        "market_ticker": str(market_row.get("market_ticker") or "").strip(),
        "fair_yes_probability": midpoint,
        "fair_yes_probability_low": low,
        "fair_yes_probability_high": high,
        "confidence": 0.2,
        "thesis": f"Auto prior fallback anchored to market midpoint; {reason}.",
        "source_note": "No reliable external evidence met thresholds in this refresh cycle.",
        "updated_at": now.isoformat(),
        "evidence_count": 0,
        "high_trust_evidence_count": 0,
        "evidence_quality": 0.0,
        "source_type": "auto",
        "last_evidence_at": "",
        "market_family": family,
        "resolution_source_type": _infer_resolution_source_type(market_row),
    }


def _fetch_evidence_for_market(
    *,
    market_row: dict[str, str],
    family: str,
    timeout_seconds: float,
    max_headlines: int,
    now: datetime,
    news_getter: NewsGetter,
) -> list[dict[str, Any]]:
    query = _build_search_query(market_row=market_row, family=family)
    url = GOOGLE_NEWS_RSS_TEMPLATE.format(query=quote_plus(query))
    xml_text = news_getter(url, timeout_seconds)
    rss_items = _rss_items_from_xml(xml_text)
    relevance_tokens = _market_relevance_tokens(market_row)
    apply_relevance_filter = False
    if relevance_tokens:
        apply_relevance_filter = any(
            any(token in f"{item.get('title') or ''} {item.get('description') or ''}".lower() for token in relevance_tokens)
            for item in rss_items
        )
    deduped: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in rss_items:
        key = str(item.get("title") or "").strip().lower()
        if not key or key in seen_titles:
            continue
        merged_text = f"{item.get('title') or ''} {item.get('description') or ''}".lower()
        if apply_relevance_filter and not any(token in merged_text for token in relevance_tokens):
            continue
        seen_titles.add(key)
        source_url = str(item.get("source_url") or "").strip()
        primary_link = str(item.get("link") or "").strip()
        domain = _domain_from_url(source_url) or _domain_from_url(primary_link)
        source_quality = _source_quality_score(domain)
        published_at = _parse_pub_date(str(item.get("pub_date") or ""))
        signal = _signal_score(
            family=family,
            title=str(item.get("title") or ""),
            description=str(item.get("description") or ""),
        )
        deduped.append(
            {
                "title": str(item.get("title") or ""),
                "source_name": str(item.get("source_name") or ""),
                "source_domain": domain,
                "published_at": published_at,
                "source_quality": source_quality,
                "recency_weight": _recency_weight(published_at=published_at, now=now),
                "signal": signal,
            }
        )
    deduped.sort(
        key=lambda item: (
            float(item["source_quality"]) * float(item["recency_weight"]) * abs(float(item["signal"])),
            item["published_at"] if isinstance(item.get("published_at"), datetime) else datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    return deduped[: max(1, max_headlines)]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _effective_source_type(row: dict[str, str]) -> str:
    source_type = str(row.get("source_type") or "").strip().lower()
    if source_type:
        return source_type
    if any(str(row.get(field) or "").strip() for field in ("thesis", "source_note", "updated_at")):
        return "manual"
    return ""


def _merge_headers(existing_fieldnames: list[str] | None) -> list[str]:
    merged: list[str] = []
    for field in PRIOR_FIELDNAMES + [field for field in AUTO_PRIOR_FIELDNAMES if field not in PRIOR_FIELDNAMES]:
        if field not in merged:
            merged.append(field)
    for field in existing_fieldnames or []:
        if field not in merged:
            merged.append(field)
    return merged


def _is_writeback_safe_auto_row(row: dict[str, Any]) -> bool:
    evidence_count = _parse_float(row.get("evidence_count")) or 0.0
    high_trust_count = _parse_float(row.get("high_trust_evidence_count")) or 0.0
    evidence_quality = _parse_float(row.get("evidence_quality")) or 0.0
    return evidence_count > 0 and high_trust_count > 0 and evidence_quality > 0.0


def _upsert_priors_csv(
    *,
    priors_path: Path,
    auto_rows: list[dict[str, Any]],
    protect_manual: bool,
) -> dict[str, Any]:
    existing_rows: list[dict[str, str]] = []
    existing_fieldnames: list[str] | None = None
    if priors_path.exists():
        with priors_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            existing_fieldnames = list(reader.fieldnames or [])
            existing_rows = [dict(row) for row in reader]

    index_by_ticker: dict[str, int] = {}
    for idx, row in enumerate(existing_rows):
        if not str(row.get("source_type") or "").strip() and any(
            str(row.get(field) or "").strip() for field in ("thesis", "source_note", "updated_at")
        ):
            row["source_type"] = "manual"
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker and ticker not in index_by_ticker:
            index_by_ticker[ticker] = idx

    updated = 0
    inserted = 0
    skipped_manual = 0

    for auto in auto_rows:
        ticker = str(auto.get("market_ticker") or "").strip()
        if not ticker:
            continue
        existing_index = index_by_ticker.get(ticker)
        if existing_index is None:
            existing_rows.append({key: str(value) if not isinstance(value, str) else value for key, value in auto.items()})
            index_by_ticker[ticker] = len(existing_rows) - 1
            inserted += 1
            continue

        existing = existing_rows[existing_index]
        existing_source_type = _effective_source_type(existing)
        if protect_manual and existing_source_type in {"manual", "manual_override"}:
            skipped_manual += 1
            continue
        for key, value in auto.items():
            existing[key] = str(value) if not isinstance(value, str) else value
        updated += 1

    merged_fieldnames = _merge_headers(existing_fieldnames)
    _write_csv(priors_path, existing_rows, merged_fieldnames)
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped_manual": skipped_manual,
        "rows_total": len(existing_rows),
        "fieldnames": merged_fieldnames,
    }


def run_kalshi_nonsports_auto_priors(
    *,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    canonical_mapping_csv: str | None = "data/research/canonical_contract_mapping.csv",
    allowed_canonical_niches: tuple[str, ...] | None = None,
    restrict_to_mapped_live_tickers: bool = False,
    allowed_categories: tuple[str, ...] | None = None,
    disallowed_categories: tuple[str, ...] | None = None,
    top_n: int = 10,
    max_markets: int = 15,
    timeout_seconds: float = 15.0,
    max_headlines_per_market: int = 8,
    min_evidence_count: int = 2,
    min_evidence_quality: float = 0.55,
    min_high_trust_sources: int = 1,
    protect_manual: bool = True,
    write_back_to_priors: bool = True,
    news_getter: NewsGetter = _http_get_text,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    priors_path = Path(priors_csv)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    prior_rows = load_prior_rows(priors_path)
    latest_rows = _latest_market_rows(history_rows)
    queue_rows = build_research_queue_rows(history_rows=history_rows, prior_rows=prior_rows)
    queue_rows_by_ticker = {
        str(row.get("market_ticker") or "").strip(): row
        for row in queue_rows
        if str(row.get("market_ticker") or "").strip()
    }
    allowed_niche_set = _normalize_string_set(allowed_canonical_niches)
    allowed_category_set = _normalize_string_set(allowed_categories)
    disallowed_category_set = _normalize_string_set(disallowed_categories)
    mapped_live_tickers, niche_by_ticker = _load_mapped_live_ticker_index(
        canonical_mapping_csv=canonical_mapping_csv,
        allowed_canonical_niches=(allowed_niche_set or None),
    )

    def _passes_scope_filters(ticker: str, market_row: dict[str, str] | None) -> tuple[bool, str]:
        category = str((market_row or {}).get("category") or "").strip().lower()
        lookup_keys = _canonical_lookup_keys(ticker)
        if allowed_category_set and category not in allowed_category_set:
            return (False, "category_not_allowed")
        if disallowed_category_set and category in disallowed_category_set:
            return (False, "category_disallowed")
        if restrict_to_mapped_live_tickers and not any(key in mapped_live_tickers for key in lookup_keys):
            return (False, "not_mapped_canonical_ticker")
        if allowed_niche_set:
            matched_niche = ""
            for key in lookup_keys:
                candidate_niche = niche_by_ticker.get(key, "")
                if candidate_niche:
                    matched_niche = candidate_niche
                    break
            if matched_niche and matched_niche not in allowed_niche_set:
                return (False, "canonical_niche_not_allowed")
        return (True, "")

    refreshable_rows: list[dict[str, Any]] = []
    filtered_out_rows: list[dict[str, Any]] = []
    for prior_row in prior_rows:
        ticker = str(prior_row.get("market_ticker") or "").strip()
        if not ticker:
            continue
        if _effective_source_type(prior_row) != "auto":
            continue
        market_row = latest_rows.get(ticker)
        if not isinstance(market_row, dict):
            continue
        passes_scope, scope_reason = _passes_scope_filters(ticker, market_row)
        if not passes_scope:
            filtered_out_rows.append({"market_ticker": ticker, "skip_reason": scope_reason})
            continue
        queue_row = dict(queue_rows_by_ticker.get(ticker, {}))
        if not queue_row:
            queue_row = {
                "market_ticker": ticker,
                "research_priority_score": _parse_float(prior_row.get("research_priority_score")) or 0.0,
            }
        queue_row["refresh_mode"] = True
        refreshable_rows.append(queue_row)

    candidate_rows: list[dict[str, Any]] = []
    seen_candidate_tickers: set[str] = set()
    for row in refreshable_rows + queue_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if not ticker or ticker in seen_candidate_tickers:
            continue
        market_row = latest_rows.get(ticker)
        if not isinstance(market_row, dict):
            continue
        passes_scope, scope_reason = _passes_scope_filters(ticker, market_row)
        if not passes_scope:
            filtered_out_rows.append({"market_ticker": ticker, "skip_reason": scope_reason})
            continue
        seen_candidate_tickers.add(ticker)
        candidate_rows.append(row)
        if len(candidate_rows) >= max(1, max_markets):
            break

    generated_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    for queue_row in candidate_rows:
        ticker = str(queue_row.get("market_ticker") or "").strip()
        refresh_mode = bool(queue_row.get("refresh_mode"))
        market_row = latest_rows.get(ticker)
        if not isinstance(market_row, dict):
            skipped_rows.append({"market_ticker": ticker, "skip_reason": "missing_latest_market"})
            continue
        family = _infer_market_family(market_row)
        try:
            evidence_items = _fetch_evidence_for_market(
                market_row=market_row,
                family=family,
                timeout_seconds=timeout_seconds,
                max_headlines=max_headlines_per_market,
                now=captured_at.astimezone(timezone.utc),
                news_getter=news_getter,
            )
        except Exception as exc:
            skipped_rows.append({"market_ticker": ticker, "skip_reason": f"evidence_fetch_error: {exc}"})
            if refresh_mode:
                fallback = _build_market_anchored_fallback_prior(
                    market_row=market_row,
                    family=family,
                    reason="evidence fetch failed",
                    now=captured_at,
                )
                fallback["research_priority_score"] = queue_row.get("research_priority_score", "")
                generated_rows.append(fallback)
            continue
        if len(evidence_items) < min_evidence_count:
            skipped_rows.append({"market_ticker": ticker, "skip_reason": "insufficient_evidence"})
            if refresh_mode:
                fallback = _build_market_anchored_fallback_prior(
                    market_row=market_row,
                    family=family,
                    reason="insufficient evidence",
                    now=captured_at,
                )
                fallback["research_priority_score"] = queue_row.get("research_priority_score", "")
                generated_rows.append(fallback)
            continue
        high_trust_sources = sum(1 for item in evidence_items if float(item["source_quality"]) >= 0.85)
        if high_trust_sources < min_high_trust_sources:
            skipped_rows.append({"market_ticker": ticker, "skip_reason": "insufficient_high_trust_evidence"})
            if refresh_mode:
                fallback = _build_market_anchored_fallback_prior(
                    market_row=market_row,
                    family=family,
                    reason="insufficient high-trust evidence",
                    now=captured_at,
                )
                fallback["research_priority_score"] = queue_row.get("research_priority_score", "")
                generated_rows.append(fallback)
            continue

        auto_prior = _build_auto_prior_from_evidence(
            market_row=market_row,
            family=family,
            evidence_items=evidence_items,
            now=captured_at,
        )
        if auto_prior is None:
            skipped_rows.append({"market_ticker": ticker, "skip_reason": "failed_probability_estimation"})
            if refresh_mode:
                fallback = _build_market_anchored_fallback_prior(
                    market_row=market_row,
                    family=family,
                    reason="failed probability estimation",
                    now=captured_at,
                )
                fallback["research_priority_score"] = queue_row.get("research_priority_score", "")
                generated_rows.append(fallback)
            continue
        evidence_quality = _parse_float(auto_prior.get("evidence_quality"))
        if evidence_quality is None or evidence_quality < min_evidence_quality:
            skipped_rows.append({"market_ticker": ticker, "skip_reason": "low_evidence_quality"})
            if refresh_mode:
                fallback = _build_market_anchored_fallback_prior(
                    market_row=market_row,
                    family=family,
                    reason="low evidence quality",
                    now=captured_at,
                )
                fallback["research_priority_score"] = queue_row.get("research_priority_score", "")
                generated_rows.append(fallback)
            continue
        auto_prior["research_priority_score"] = queue_row.get("research_priority_score", "")
        generated_rows.append(auto_prior)

    generated_rows.sort(
        key=lambda row: (
            _parse_float(row.get("evidence_quality")) or 0.0,
            _parse_float(row.get("research_priority_score")) or 0.0,
            _parse_float(row.get("confidence")) or 0.0,
        ),
        reverse=True,
    )
    writeback_rows = [row for row in generated_rows if _is_writeback_safe_auto_row(row)]

    upsert_summary = {
        "inserted": 0,
        "updated": 0,
        "skipped_manual": 0,
        "rows_total": len(prior_rows),
        "skipped_unsafe_auto": 0,
    }
    if write_back_to_priors and writeback_rows:
        upsert_summary = _upsert_priors_csv(
            priors_path=priors_path,
            auto_rows=writeback_rows,
            protect_manual=protect_manual,
        )
        upsert_summary["skipped_unsafe_auto"] = len(generated_rows) - len(writeback_rows)
    elif generated_rows:
        upsert_summary["skipped_unsafe_auto"] = len(generated_rows) - len(writeback_rows)

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_auto_priors_{stamp}.csv"
    _write_csv(csv_path, generated_rows, AUTO_PRIOR_FIELDNAMES)

    skipped_csv_path = out_dir / f"kalshi_nonsports_auto_priors_skipped_{stamp}.csv"
    all_skipped_rows = list(skipped_rows) + filtered_out_rows
    _write_csv(skipped_csv_path, all_skipped_rows, ["market_ticker", "skip_reason"])

    summary = {
        "captured_at": captured_at.isoformat(),
        "priors_csv": str(priors_path),
        "history_csv": str(history_path),
        "write_back_to_priors": write_back_to_priors,
        "protect_manual": protect_manual,
        "candidate_markets": len(candidate_rows),
        "candidate_markets_from_research_queue": len(queue_rows),
        "candidate_markets_from_auto_refresh": len(refreshable_rows),
        "candidate_markets_filtered_out": len(filtered_out_rows),
        "auto_prior_scope_restrict_to_mapped_live_tickers": restrict_to_mapped_live_tickers,
        "auto_prior_scope_canonical_mapping_csv": canonical_mapping_csv,
        "auto_prior_scope_allowed_canonical_niches": sorted(allowed_niche_set) if allowed_niche_set else None,
        "auto_prior_scope_allowed_categories": sorted(allowed_category_set) if allowed_category_set else None,
        "auto_prior_scope_disallowed_categories": sorted(disallowed_category_set) if disallowed_category_set else None,
        "auto_prior_scope_mapped_live_tickers_count": len(mapped_live_tickers),
        "generated_priors": len(generated_rows),
        "skipped_markets": len(all_skipped_rows),
        "min_evidence_count": min_evidence_count,
        "min_evidence_quality": min_evidence_quality,
        "min_high_trust_sources": min_high_trust_sources,
        "inserted_rows": upsert_summary.get("inserted", 0),
        "updated_rows": upsert_summary.get("updated", 0),
        "manual_rows_protected": upsert_summary.get("skipped_manual", 0),
        "writeback_blocked_rows": upsert_summary.get("skipped_unsafe_auto", 0),
        "prior_rows_total_after_upsert": upsert_summary.get("rows_total", len(prior_rows)),
        "top_market_ticker": generated_rows[0]["market_ticker"] if generated_rows else None,
        "top_market_fair_yes_probability": generated_rows[0]["fair_yes_probability"] if generated_rows else None,
        "top_market_confidence": generated_rows[0]["confidence"] if generated_rows else None,
        "top_markets": generated_rows[:top_n],
        "status": "ready" if generated_rows else "no_auto_priors",
        "output_csv": str(csv_path),
        "skipped_output_csv": str(skipped_csv_path),
    }

    summary_path = out_dir / f"kalshi_nonsports_auto_priors_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
