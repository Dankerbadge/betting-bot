from __future__ import annotations

import hashlib
import re
from typing import Any


_CITY_TIMEZONE_BY_TOKEN = {
    "nyc": "America/New_York",
    "new york": "America/New_York",
    "boston": "America/New_York",
    "dc": "America/New_York",
    "washington": "America/New_York",
    "philadelphia": "America/New_York",
    "philly": "America/New_York",
    "miami": "America/New_York",
    "atlanta": "America/New_York",
    "detroit": "America/New_York",
    "minneapolis": "America/Chicago",
    "chicago": "America/Chicago",
    "dallas": "America/Chicago",
    "houston": "America/Chicago",
    "denver": "America/Denver",
    "phoenix": "America/Phoenix",
    "la": "America/Los_Angeles",
    "los angeles": "America/Los_Angeles",
    "seattle": "America/Los_Angeles",
    "las vegas": "America/Los_Angeles",
    "portland": "America/Los_Angeles",
    "salt lake city": "America/Denver",
    "sf": "America/Los_Angeles",
    "san francisco": "America/Los_Angeles",
}


def normalize_rule_text(rule_text: str) -> str:
    return " ".join(str(rule_text or "").strip().split())


def rule_text_hash_sha256(rule_text: str) -> str:
    normalized = normalize_rule_text(rule_text)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def infer_contract_family(
    *,
    market_ticker: str,
    market_title: str,
    event_title: str,
    rules_primary: str,
) -> str:
    merged = " ".join((market_ticker, market_title, event_title, rules_primary)).lower()
    if any(token in merged for token in ("hurricane", "tropical storm", "landfall")):
        return "hurricane"
    if "snow" in merged:
        return "daily_snow"
    if any(token in merged for token in ("rain", "precip", "precipitation")):
        return "daily_rain"
    if any(
        token in merged
        for token in (
            "high temperature",
            "low temperature",
            "daily temperature",
            "temperature at",
            "temperature in ",
        )
    ):
        return "daily_temperature"
    if any(
        token in merged
        for token in (
            "land ocean-temperature index",
            "temperature increase",
            "hottest month",
            "monthly climate",
            "climate record",
            "temperature anomaly",
        )
    ):
        return "monthly_climate_anomaly"
    if "weather" in merged or "climate" in merged:
        return "weather_other"
    return "non_weather"


def infer_settlement_sources(rules_primary: str) -> tuple[str, str]:
    text = str(rules_primary or "").lower()
    primary = ""
    fallback = ""
    source_positions: list[tuple[int, str]] = []
    for token, source_name in (
        ("monthly climate report", "NWS Monthly Climate Report"),
        ("national weather service", "NWS"),
        ("weather.gov", "NWS"),
        ("nws", "NWS"),
        ("ncei", "NCEI"),
        ("noaa", "NOAA"),
        ("eia", "EIA"),
    ):
        idx = text.find(token)
        if idx >= 0:
            source_positions.append((idx, source_name))
    if source_positions:
        source_positions.sort(key=lambda item: item[0])
        primary = source_positions[0][1]

    fallback_match = re.search(
        r"(?:if|when).{0,80}(?:unavailable|missing|not published).{0,80}(ncei|nws|noaa|monthly climate report)",
        text,
    )
    if fallback_match:
        token = str(fallback_match.group(1) or "").strip().lower()
        if token == "ncei":
            fallback = "NCEI"
        elif token == "nws":
            fallback = "NWS"
        elif token == "noaa":
            fallback = "NOAA"
        elif token == "monthly climate report":
            fallback = "NWS Monthly Climate Report"

    if not primary and fallback:
        primary = fallback
        fallback = ""
    return (primary, fallback)


def extract_threshold_expression(rules_primary: str) -> str:
    text = normalize_rule_text(rules_primary)
    if not text:
        return ""
    lowered = text.lower()
    between = re.search(r"between\s+([-\d.]+)\s*(?:-|to|and)\s*([-\d.]+)", lowered)
    if between:
        return f"between:{between.group(1)}:{between.group(2)}"
    at_most = re.search(
        r"(?:at most|no more than|less than or equal to|<=)\s*([-\d.]+)",
        lowered,
    )
    if at_most:
        return f"at_most:{at_most.group(1)}"
    at_least = re.search(
        r"(?:at least|greater than or equal to|>=)\s*([-\d.]+)",
        lowered,
    )
    if at_least:
        return f"at_least:{at_least.group(1)}"
    above = re.search(r"(?:above|greater than|over)\s+([-\d.]+)", lowered)
    if above:
        return f"above:{above.group(1)}"
    below = re.search(r"(?:below|less than|under)\s+([-\d.]+)", lowered)
    if below:
        return f"below:{below.group(1)}"
    equal = re.search(r"(?:equal to|equals|exactly)\s+([-\d.]+)", lowered)
    if equal:
        return f"equal:{equal.group(1)}"
    return ""


def infer_settlement_station(rules_primary: str, market_title: str, event_title: str) -> str:
    text = " ".join((rules_primary, market_title, event_title))
    icao_match = re.search(r"\b([K][A-Z]{3})\b", text)
    if icao_match:
        return icao_match.group(1)
    station_match = re.search(r"\bstation\s+([A-Z0-9]{3,6})\b", text, flags=re.IGNORECASE)
    if station_match:
        return station_match.group(1).upper()
    return ""


def infer_settlement_timezone(market_ticker: str, market_title: str, event_title: str) -> str:
    merged = " ".join((market_ticker, market_title, event_title)).lower()
    for token, timezone_name in _CITY_TIMEZONE_BY_TOKEN.items():
        if token in merged:
            return timezone_name
    return ""


def infer_local_day_boundary(rules_primary: str) -> str:
    text = str(rules_primary or "").lower()
    if "local" in text and "day" in text:
        return "local_day"
    if "utc" in text:
        return "utc_day"
    return "contract_defined_day"


def _parse_clock_text(value: str) -> tuple[int, int] | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", text)
    if not match:
        return None
    hour_raw = int(match.group(1))
    minute_raw = int(match.group(2) or "0")
    meridiem = str(match.group(3) or "").strip().lower()
    if minute_raw < 0 or minute_raw > 59:
        return None
    if meridiem:
        if hour_raw < 1 or hour_raw > 12:
            return None
        hour = hour_raw % 12
        if meridiem == "pm":
            hour += 12
    else:
        if hour_raw < 0 or hour_raw > 23:
            return None
        hour = hour_raw
    return (hour, minute_raw)


def _format_clock_text(hours: int, minutes: int) -> str:
    return f"{int(hours):02d}:{int(minutes):02d}"


def infer_observation_window_local(rules_primary: str) -> tuple[str, str, str]:
    text = normalize_rule_text(rules_primary)
    if not text:
        return ("", "", "unknown")
    lowered = text.lower()

    explicit_range = re.search(
        r"(?:between|from)\s+([0-9]{1,2}(?::[0-9]{2})?\s*(?:am|pm)?)\s*(?:and|to|-)\s*([0-9]{1,2}(?::[0-9]{2})?\s*(?:am|pm)?)",
        lowered,
    )
    if explicit_range:
        start = _parse_clock_text(str(explicit_range.group(1) or ""))
        end = _parse_clock_text(str(explicit_range.group(2) or ""))
        if start is not None and end is not None:
            return (_format_clock_text(*start), _format_clock_text(*end), "rules_text")

    deadline = re.search(
        r"(?:before|by|until)\s+([0-9]{1,2}(?::[0-9]{2})?\s*(?:am|pm)?)",
        lowered,
    )
    if deadline:
        parsed = _parse_clock_text(str(deadline.group(1) or ""))
        if parsed is not None:
            return ("00:00", _format_clock_text(*parsed), "rules_text")

    if ("local day" in lowered) or ("calendar day" in lowered) or ("all day" in lowered):
        return ("00:00", "23:59", "heuristic_local_day")

    return ("", "", "unknown")


def build_weather_settlement_spec(row: dict[str, Any]) -> dict[str, Any]:
    market_ticker = str(row.get("market_ticker") or "")
    market_title = str(row.get("market_title") or "")
    event_title = str(row.get("event_title") or "")
    rules_primary = str(row.get("rules_primary") or "")
    family = infer_contract_family(
        market_ticker=market_ticker,
        market_title=market_title,
        event_title=event_title,
        rules_primary=rules_primary,
    )
    primary_source, fallback_source = infer_settlement_sources(rules_primary)
    window_start, window_end, window_source = infer_observation_window_local(rules_primary)
    return {
        "contract_family": family,
        "settlement_source_primary": primary_source,
        "settlement_source_fallback": fallback_source,
        "settlement_station": infer_settlement_station(rules_primary, market_title, event_title),
        "settlement_timezone": infer_settlement_timezone(market_ticker, market_title, event_title),
        "local_day_boundary": infer_local_day_boundary(rules_primary),
        "observation_window_local_start": window_start,
        "observation_window_local_end": window_end,
        "observation_window_local_source": window_source,
        "threshold_expression": extract_threshold_expression(rules_primary),
        "rule_text_hash_sha256": rule_text_hash_sha256(rules_primary),
    }
