from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any

from betbot.kalshi_nonsports_scan import KalshiEventsFetchError, _load_open_events_with_diagnostics
from betbot.kalshi_weather_settlement import build_weather_settlement_spec
from betbot.live_smoke import _http_get_json, kalshi_api_root_candidates
from betbot.onboarding import _parse_env_file


KALSHI_TEMPERATURE_SPEC_FIELDNAMES = [
    "captured_at",
    "category",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "event_title",
    "market_title",
    "market_status",
    "close_time",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "floor_strike_fp",
    "cap_strike_fp",
    "settlement_sources",
    "contract_terms_url",
    "contract_family",
    "target_date_local",
    "settlement_confidence_score",
    "settlement_source_primary",
    "settlement_source_fallback",
    "settlement_station",
    "settlement_timezone",
    "local_day_boundary",
    "observation_window_local_start",
    "observation_window_local_end",
    "threshold_expression",
    "rules_primary",
    "rules_secondary",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


_TICKER_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

_TEMPERATURE_TICKER_PREFIXES = ("KXHIGH", "KXLOW", "KXTEMP", "NHIGH", "NLOW", "NTEMP")
_TEMPERATURE_DAY_TOKEN_RE = re.compile(r"-\d{2}[A-Z]{3}\d{2,4}(?:-|$)")


def _has_temperature_day_token(*values: Any) -> bool:
    for value in values:
        token = _normalize_text(value).upper()
        if token and _TEMPERATURE_DAY_TOKEN_RE.search(token):
            return True
    return False


def _is_temperature_market(event: dict[str, Any], market: dict[str, Any]) -> bool:
    category = _normalize_text(event.get("category")).lower()
    series_ticker = _normalize_text(event.get("series_ticker")).upper()
    event_ticker = _normalize_text(event.get("event_ticker")).upper()
    market_ticker = _normalize_text(market.get("ticker")).upper()
    has_day_token = _has_temperature_day_token(series_ticker, event_ticker, market_ticker)

    # Prefer explicit ticker families first: these are stable identifiers for
    # daily high/low temperature contracts even when titles omit "temperature".
    prefix_match = (
        series_ticker.startswith(_TEMPERATURE_TICKER_PREFIXES)
        or event_ticker.startswith(_TEMPERATURE_TICKER_PREFIXES)
        or market_ticker.startswith(_TEMPERATURE_TICKER_PREFIXES)
    )
    if prefix_match and category == "climate and weather" and has_day_token:
        return True

    merged = " ".join(
        (
            _normalize_text(event.get("title")),
            _normalize_text(event.get("sub_title")),
            _normalize_text(market.get("title")),
            _normalize_text(market.get("yes_sub_title")),
            _normalize_text(market.get("rules_primary")),
        )
    ).lower()

    has_temperature_language = any(
        token in merged
        for token in (
            "temperature",
            "high temp",
            "low temp",
            "highest temperature",
            "high temperature",
            "lowest temperature",
            "low temperature",
            "daily temperature",
            "daily temp",
            "degrees fahrenheit",
            "degrees celsius",
            "°f",
            "°c",
        )
    )
    if not has_temperature_language:
        return False

    # Within climate/weather category, explicit temperature phrasing is enough.
    if category == "climate and weather" and has_day_token:
        return True

    # Outside climate/weather, require explicit temperature language.
    return has_temperature_language and has_day_token


def _choose_strike_value(market: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        if key in market:
            text = _normalize_text(market.get(key))
            if text:
                return text
    return ""


def infer_target_date_from_event_ticker(event_ticker: str) -> str:
    token = _normalize_text(event_ticker).upper()
    if not token:
        return ""
    parts = token.split("-")
    if not parts:
        return ""
    suffix = parts[-1]
    if len(suffix) != 7:
        return ""
    year_token = suffix[0:2]
    month_token = suffix[2:5]
    day_token = suffix[5:7]
    if not (year_token.isdigit() and day_token.isdigit()):
        return ""
    month_value = _TICKER_MONTHS.get(month_token)
    if month_value is None:
        return ""
    year_value = 2000 + int(year_token)
    day_value = int(day_token)
    try:
        date_value = datetime(year_value, month_value, day_value)
    except ValueError:
        return ""
    return date_value.strftime("%Y-%m-%d")


def settlement_confidence_score(settlement: dict[str, Any]) -> float:
    score = 0.0
    if _normalize_text(settlement.get("settlement_source_primary")):
        score += 0.35
    if _normalize_text(settlement.get("settlement_station")):
        score += 0.25
    if _normalize_text(settlement.get("settlement_timezone")):
        score += 0.15
    if _normalize_text(settlement.get("threshold_expression")):
        score += 0.10
    boundary = _normalize_text(settlement.get("local_day_boundary"))
    if boundary in {"local_day", "utc_day"}:
        score += 0.15
    return round(min(1.0, score), 3)


def extract_kalshi_temperature_contract_specs(
    *,
    events: list[dict[str, Any]],
    captured_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        markets = event.get("markets")
        if not isinstance(markets, list):
            continue

        for market in markets:
            if not isinstance(market, dict):
                continue
            if not _is_temperature_market(event, market):
                continue

            row_context = {
                "market_ticker": _normalize_text(market.get("ticker")),
                "market_title": _normalize_text(market.get("title")),
                "event_title": _normalize_text(event.get("title")),
                "rules_primary": _normalize_text(market.get("rules_primary")),
            }
            settlement = build_weather_settlement_spec(row_context)
            contract_family = _normalize_text(settlement.get("contract_family"))
            series_ticker = _normalize_text(event.get("series_ticker")).upper()
            event_ticker = _normalize_text(event.get("event_ticker")).upper()
            market_ticker = _normalize_text(market.get("ticker")).upper()
            has_day_token = _has_temperature_day_token(series_ticker, event_ticker, market_ticker)
            target_date_local = infer_target_date_from_event_ticker(_normalize_text(event.get("event_ticker")))
            if contract_family != "daily_temperature":
                category = _normalize_text(event.get("category")).lower()
                daily_prefix_match = (
                    series_ticker.startswith(_TEMPERATURE_TICKER_PREFIXES)
                    or event_ticker.startswith(_TEMPERATURE_TICKER_PREFIXES)
                    or market_ticker.startswith(_TEMPERATURE_TICKER_PREFIXES)
                )
                has_station = bool(_normalize_text(settlement.get("settlement_station")))
                if not (category == "climate and weather" and daily_prefix_match and has_day_token and has_station):
                    continue
                contract_family = "daily_temperature"
            if contract_family == "daily_temperature" and not target_date_local:
                # Prevent non-daily macro/monthly rows from leaking into the
                # daily temperature strategy lane.
                continue

            settlement_sources = event.get("settlement_sources")
            if not isinstance(settlement_sources, list):
                settlement_sources = []
            normalized_sources = [source for source in settlement_sources if isinstance(source, str) and source.strip()]

            rows.append(
                {
                    "captured_at": captured_at.isoformat(),
                    "category": _normalize_text(event.get("category")),
                    "series_ticker": _normalize_text(event.get("series_ticker")),
                    "event_ticker": _normalize_text(event.get("event_ticker")),
                    "market_ticker": _normalize_text(market.get("ticker")),
                    "event_title": _normalize_text(event.get("title")),
                    "market_title": _normalize_text(market.get("title")),
                    "market_status": _normalize_text(market.get("status")),
                    "close_time": _normalize_text(market.get("close_time")),
                    "strike_type": _normalize_text(market.get("strike_type")),
                    "floor_strike": _choose_strike_value(market, ("floor_strike", "floor_strike_dollars")),
                    "cap_strike": _choose_strike_value(market, ("cap_strike", "cap_strike_dollars")),
                    "floor_strike_fp": _choose_strike_value(market, ("floor_strike_fp",)),
                    "cap_strike_fp": _choose_strike_value(market, ("cap_strike_fp",)),
                    "settlement_sources": json.dumps(normalized_sources),
                    "contract_terms_url": _normalize_text(event.get("contract_terms_url")),
                    "contract_family": contract_family,
                    "target_date_local": target_date_local,
                    "settlement_confidence_score": settlement_confidence_score(settlement),
                    "settlement_source_primary": _normalize_text(settlement.get("settlement_source_primary")),
                    "settlement_source_fallback": _normalize_text(settlement.get("settlement_source_fallback")),
                    "settlement_station": _normalize_text(settlement.get("settlement_station")),
                    "settlement_timezone": _normalize_text(settlement.get("settlement_timezone")),
                    "local_day_boundary": _normalize_text(settlement.get("local_day_boundary")),
                    "observation_window_local_start": _normalize_text(settlement.get("observation_window_local_start")),
                    "observation_window_local_end": _normalize_text(settlement.get("observation_window_local_end")),
                    "threshold_expression": _normalize_text(settlement.get("threshold_expression")),
                    "rules_primary": _normalize_text(market.get("rules_primary")),
                    "rules_secondary": _normalize_text(market.get("rules_secondary")),
                }
            )

    rows.sort(
        key=lambda row: (
            row.get("series_ticker", ""),
            row.get("event_ticker", ""),
            row.get("market_ticker", ""),
        )
    )
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=KALSHI_TEMPERATURE_SPEC_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_temperature_contract_specs(
    *,
    env_file: str,
    output_dir: str = "outputs",
    timeout_seconds: float = 15.0,
    page_limit: int = 200,
    max_pages: int = 40,
    top_n: int = 20,
    now: datetime | None = None,
) -> dict[str, Any]:
    env_path = Path(env_file)
    env_data = _parse_env_file(env_path)
    captured_at = now or datetime.now(timezone.utc)

    kalshi_env = _normalize_text(env_data.get("KALSHI_ENV") or "prod").lower()
    api_roots = kalshi_api_root_candidates(kalshi_env)

    diagnostics = {
        "page_requests": 0,
        "rate_limit_retries_used": 0,
        "network_retries_used": 0,
        "transient_http_retries_used": 0,
        "api_root_failovers_used": 0,
    }
    events: list[dict[str, Any]] = []
    status = "ready"
    events_error = ""

    try:
        events, diagnostics = _load_open_events_with_diagnostics(
            api_roots=api_roots,
            timeout_seconds=timeout_seconds,
            page_limit=page_limit,
            max_pages=max_pages,
            http_get_json=_http_get_json,
        )
    except KalshiEventsFetchError as exc:  # pragma: no cover - runtime safety
        diagnostics = dict(exc.diagnostics)
        events_error = str(exc)
        status = "upstream_error"
    except Exception as exc:  # pragma: no cover - runtime safety
        events_error = str(exc)
        status = "upstream_error"

    rows = extract_kalshi_temperature_contract_specs(events=events, captured_at=captured_at)
    if not rows and status == "ready":
        status = "no_markets"

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"kalshi_temperature_contract_specs_{stamp}.csv"
    _write_csv(csv_path, rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "env_file": str(env_path),
        "kalshi_env": kalshi_env,
        "api_roots": list(api_roots),
        "status": status,
        "events_error": events_error,
        "events_fetched": len(events),
        "markets_matched": len(rows),
        "series_count": len({row.get("series_ticker", "") for row in rows if row.get("series_ticker", "")}),
        "page_requests": diagnostics.get("page_requests", 0),
        "rate_limit_retries_used": diagnostics.get("rate_limit_retries_used", 0),
        "network_retries_used": diagnostics.get("network_retries_used", 0),
        "transient_http_retries_used": diagnostics.get("transient_http_retries_used", 0),
        "api_root_failovers_used": diagnostics.get("api_root_failovers_used", 0),
        "top_markets": rows[: max(1, int(top_n))],
        "output_csv": str(csv_path),
    }

    summary_path = out_dir / f"kalshi_temperature_contract_specs_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
