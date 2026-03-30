from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import socket
import time
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from betbot.live_smoke import HttpGetter, KALSHI_API_ROOTS, _http_get_json, kalshi_api_root_candidates
from betbot.onboarding import _parse_env_file

KALSHI_MAX_RATE_LIMIT_RETRIES = 3
KALSHI_RATE_LIMIT_BASE_DELAY_SECONDS = 1.0
KALSHI_NETWORK_MAX_RETRIES = 4
KALSHI_NETWORK_BACKOFF_SECONDS = 0.75
KALSHI_TRANSIENT_HTTP_STATUS_CODES = {408, 425, 500, 502, 503, 504}


class KalshiEventsFetchError(ValueError):
    def __init__(self, message: str, diagnostics: dict[str, int]) -> None:
        super().__init__(message)
        self.diagnostics = diagnostics


def _url_error_reason_text(exc: URLError) -> str:
    reason = exc.reason
    if isinstance(reason, BaseException):
        return str(reason)
    return str(reason or exc)


def _is_dns_resolution_error(exc: URLError) -> bool:
    reason = exc.reason
    if isinstance(reason, socket.gaierror):
        return True
    text = _url_error_reason_text(exc).lower()
    return "nodename nor servname" in text or "name or service not known" in text or "temporary failure in name resolution" in text


def _parse_float(value: Any) -> float | None:
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_category(value: str) -> str:
    return value.strip().lower()


def _kalshi_events_url(*, api_root: str, cursor: str | None, page_limit: int) -> str:
    params = {
        "status": "open",
        "with_nested_markets": "true",
        "limit": str(page_limit),
    }
    if cursor:
        params["cursor"] = cursor
    return f"{api_root}/events?{urlencode(params)}"


def _load_open_events(
    *,
    api_roots: tuple[str, ...],
    timeout_seconds: float,
    page_limit: int,
    max_pages: int,
    http_get_json: HttpGetter,
) -> list[dict[str, Any]]:
    events, _ = _load_open_events_with_diagnostics(
        api_roots=api_roots,
        timeout_seconds=timeout_seconds,
        page_limit=page_limit,
        max_pages=max_pages,
        http_get_json=http_get_json,
    )
    return events


def _load_open_events_with_diagnostics(
    *,
    api_roots: tuple[str, ...],
    timeout_seconds: float,
    page_limit: int,
    max_pages: int,
    http_get_json: HttpGetter,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    collected: list[dict[str, Any]] = []
    cursor: str | None = None
    diagnostics = {
        "page_requests": 0,
        "rate_limit_retries_used": 0,
        "network_retries_used": 0,
        "transient_http_retries_used": 0,
        "api_root_failovers_used": 0,
    }
    effective_api_roots = tuple(root for root in api_roots if str(root or "").strip())
    if not effective_api_roots:
        raise KalshiEventsFetchError("Kalshi api_roots is empty", diagnostics)

    for _ in range(max_pages):
        status_code = 599
        payload: Any = {"error": "request_not_attempted"}
        for api_root_index, api_root in enumerate(effective_api_roots):
            if api_root_index > 0:
                diagnostics["api_root_failovers_used"] += 1
            request_url = _kalshi_events_url(api_root=api_root, cursor=cursor, page_limit=page_limit)
            rate_limit_retries = 0
            network_retries = 0
            while True:
                diagnostics["page_requests"] += 1
                try:
                    status_code, payload = http_get_json(
                        request_url,
                        {
                            "Accept": "application/json",
                            "User-Agent": "betbot-kalshi-nonsports-scan/1.0",
                        },
                        timeout_seconds,
                    )
                except URLError as exc:
                    if _is_dns_resolution_error(exc):
                        diagnostics["network_retries_used"] += 1
                        if api_root_index < len(effective_api_roots) - 1:
                            break
                        raise KalshiEventsFetchError(
                            f"Kalshi events request network_error: {exc.reason}",
                            diagnostics,
                        ) from exc
                    if network_retries >= KALSHI_NETWORK_MAX_RETRIES:
                        if api_root_index < len(effective_api_roots) - 1:
                            break
                        raise KalshiEventsFetchError(
                            f"Kalshi events request network_error: {exc.reason}",
                            diagnostics,
                        ) from exc
                    time.sleep(KALSHI_NETWORK_BACKOFF_SECONDS * (2**network_retries))
                    network_retries += 1
                    diagnostics["network_retries_used"] += 1
                    continue

                if status_code == 429:
                    if rate_limit_retries >= KALSHI_MAX_RATE_LIMIT_RETRIES:
                        break
                    time.sleep(KALSHI_RATE_LIMIT_BASE_DELAY_SECONDS * (2**rate_limit_retries))
                    rate_limit_retries += 1
                    diagnostics["rate_limit_retries_used"] += 1
                    continue

                if status_code in KALSHI_TRANSIENT_HTTP_STATUS_CODES:
                    if network_retries >= KALSHI_NETWORK_MAX_RETRIES:
                        break
                    time.sleep(KALSHI_NETWORK_BACKOFF_SECONDS * (2**network_retries))
                    network_retries += 1
                    diagnostics["transient_http_retries_used"] += 1
                    continue

                break
            if status_code == 200 and isinstance(payload, dict):
                break

        if status_code != 200 or not isinstance(payload, dict):
            raise KalshiEventsFetchError(f"Kalshi events request failed with status {status_code}", diagnostics)
        events = payload.get("events")
        if not isinstance(events, list):
            raise KalshiEventsFetchError("Kalshi events payload did not contain events[]", diagnostics)
        collected.extend(event for event in events if isinstance(event, dict))
        next_cursor = payload.get("cursor")
        if not isinstance(next_cursor, str) or next_cursor.strip() == "":
            break
        cursor = next_cursor

    return collected, diagnostics


def _execution_fit_score(
    *,
    yes_ask: float | None,
    yes_bid: float | None,
    spread: float | None,
    liquidity_dollars: float | None,
    volume_24h_contracts: float | None,
    hours_to_close: float | None,
    ten_dollar_fillable: bool,
    two_sided_book: bool,
) -> float:
    score = 0.0

    if spread is not None:
        score += max(0.0, 0.12 - min(spread, 0.12)) * 200.0
    if liquidity_dollars is not None:
        score += min(liquidity_dollars, 3000.0) / 150.0
    if volume_24h_contracts is not None:
        score += min(volume_24h_contracts, 3000.0) / 300.0
    if hours_to_close is not None and hours_to_close > 0:
        score += max(0.0, 336.0 - min(hours_to_close, 336.0)) / 24.0
    if yes_ask is not None:
        score += max(0.0, 0.35 - min(abs(yes_ask - 0.5), 0.35)) * 20.0
    if yes_bid is not None and yes_bid > 0:
        score += 2.0
    if ten_dollar_fillable:
        score += 8.0
    else:
        score -= 4.0
    if two_sided_book:
        score += 8.0
    else:
        score -= 8.0

    return round(score, 6)


def extract_kalshi_nonsports_rows(
    *,
    events: list[dict[str, Any]],
    captured_at: datetime,
    excluded_categories: tuple[str, ...],
    max_hours_to_close: float | None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}
    excluded = {_normalize_category(category) for category in excluded_categories}

    for event in events:
        category = str(event.get("category") or "").strip() or "Unknown"
        if _normalize_category(category) in excluded:
            continue
        category_counts[category] = category_counts.get(category, 0) + 1

        event_title = str(event.get("title") or "").strip()
        event_sub_title = str(event.get("sub_title") or "").strip()
        event_ticker = str(event.get("event_ticker") or "").strip()
        series_ticker = str(event.get("series_ticker") or "").strip()
        markets = event.get("markets")
        if not isinstance(markets, list):
            continue

        for market in markets:
            if not isinstance(market, dict):
                continue
            if str(market.get("status") or "").strip().lower() != "active":
                continue

            close_time = _parse_timestamp(market.get("close_time"))
            hours_to_close: float | None = None
            if close_time is not None:
                hours_to_close = (close_time - captured_at.astimezone(timezone.utc)).total_seconds() / 3600.0
                if max_hours_to_close is not None and hours_to_close > max_hours_to_close:
                    continue
                if hours_to_close <= 0:
                    continue

            yes_ask = _parse_float(market.get("yes_ask_dollars"))
            yes_bid = _parse_float(market.get("yes_bid_dollars"))
            no_ask = _parse_float(market.get("no_ask_dollars"))
            no_bid = _parse_float(market.get("no_bid_dollars"))
            last_price = _parse_float(market.get("last_price_dollars"))
            liquidity_dollars = _parse_float(market.get("liquidity_dollars"))
            volume_contracts = _parse_float(market.get("volume_fp"))
            volume_24h_contracts = _parse_float(market.get("volume_24h_fp"))
            open_interest_contracts = _parse_float(market.get("open_interest_fp"))
            yes_ask_size = _parse_float(market.get("yes_ask_size_fp"))
            yes_bid_size = _parse_float(market.get("yes_bid_size_fp"))

            if yes_ask is None and yes_bid is None and last_price is None:
                continue

            spread = None
            if yes_ask is not None and yes_bid is not None and yes_ask >= yes_bid:
                spread = round(yes_ask - yes_bid, 6)

            contracts_for_ten_dollars = None
            ten_dollar_cost = None
            ten_dollar_fillable = False
            if yes_ask is not None and yes_ask > 0:
                contracts_for_ten_dollars = max(1, math.floor(10.0 / yes_ask))
                ten_dollar_cost = round(contracts_for_ten_dollars * yes_ask, 4)
                ten_dollar_fillable = yes_ask_size is not None and yes_ask_size >= contracts_for_ten_dollars
            two_sided_book = (
                yes_bid is not None
                and yes_bid > 0
                and yes_bid_size is not None
                and yes_bid_size > 0
                and yes_ask is not None
                and yes_ask > 0
                and yes_ask_size is not None
                and yes_ask_size > 0
            )

            execution_fit_score = _execution_fit_score(
                yes_ask=yes_ask,
                yes_bid=yes_bid,
                spread=spread,
                liquidity_dollars=liquidity_dollars,
                volume_24h_contracts=volume_24h_contracts,
                hours_to_close=hours_to_close,
                ten_dollar_fillable=ten_dollar_fillable,
                two_sided_book=two_sided_book,
            )

            rows.append(
                {
                    "category": category,
                    "series_ticker": series_ticker,
                    "event_ticker": event_ticker,
                    "market_ticker": str(market.get("ticker") or "").strip(),
                    "event_title": event_title,
                    "event_sub_title": event_sub_title,
                    "market_title": str(market.get("title") or "").strip(),
                    "yes_sub_title": str(market.get("yes_sub_title") or "").strip(),
                    "close_time": close_time.isoformat() if close_time is not None else "",
                    "hours_to_close": round(hours_to_close, 4) if hours_to_close is not None else "",
                    "yes_bid_dollars": yes_bid if yes_bid is not None else "",
                    "yes_bid_size_contracts": yes_bid_size if yes_bid_size is not None else "",
                    "yes_ask_dollars": yes_ask if yes_ask is not None else "",
                    "yes_ask_size_contracts": yes_ask_size if yes_ask_size is not None else "",
                    "no_bid_dollars": no_bid if no_bid is not None else "",
                    "no_ask_dollars": no_ask if no_ask is not None else "",
                    "last_price_dollars": last_price if last_price is not None else "",
                    "spread_dollars": spread if spread is not None else "",
                    "liquidity_dollars": liquidity_dollars if liquidity_dollars is not None else "",
                    "volume_contracts": volume_contracts if volume_contracts is not None else "",
                    "volume_24h_contracts": volume_24h_contracts if volume_24h_contracts is not None else "",
                    "open_interest_contracts": open_interest_contracts if open_interest_contracts is not None else "",
                    "contracts_for_ten_dollars": contracts_for_ten_dollars if contracts_for_ten_dollars is not None else "",
                    "ten_dollar_cost": ten_dollar_cost if ten_dollar_cost is not None else "",
                    "ten_dollar_fillable_at_best_ask": ten_dollar_fillable,
                    "two_sided_book": two_sided_book,
                    "execution_fit_score": execution_fit_score,
                    "rules_primary": str(market.get("rules_primary") or "").strip(),
                }
            )

    rows.sort(
        key=lambda row: (
            row["execution_fit_score"],
            row["liquidity_dollars"] if isinstance(row["liquidity_dollars"], int | float) else -1.0,
            row["volume_24h_contracts"] if isinstance(row["volume_24h_contracts"], int | float) else -1.0,
        ),
        reverse=True,
    )
    return rows, dict(sorted(category_counts.items(), key=lambda item: (-item[1], item[0])))


def _write_scan_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "category",
        "series_ticker",
        "event_ticker",
        "market_ticker",
        "event_title",
        "event_sub_title",
        "market_title",
        "yes_sub_title",
        "close_time",
        "hours_to_close",
        "yes_bid_dollars",
        "yes_bid_size_contracts",
        "yes_ask_dollars",
        "yes_ask_size_contracts",
        "no_bid_dollars",
        "no_ask_dollars",
        "last_price_dollars",
        "spread_dollars",
        "liquidity_dollars",
        "volume_contracts",
        "volume_24h_contracts",
        "open_interest_contracts",
        "contracts_for_ten_dollars",
        "ten_dollar_cost",
        "ten_dollar_fillable_at_best_ask",
        "two_sided_book",
        "execution_fit_score",
        "rules_primary",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_scan(
    *,
    env_file: str,
    output_dir: str = "outputs",
    timeout_seconds: float = 15.0,
    excluded_categories: tuple[str, ...] = ("Sports",),
    max_hours_to_close: float | None = 336.0,
    page_limit: int = 200,
    max_pages: int = 5,
    top_n: int = 10,
    http_get_json: HttpGetter = _http_get_json,
    now: datetime | None = None,
) -> dict[str, Any]:
    env_path = Path(env_file)
    env_data = _parse_env_file(env_path)
    captured_at = now or datetime.now(timezone.utc)

    kalshi_env = (env_data.get("KALSHI_ENV") or "prod").strip().lower()
    if kalshi_env not in KALSHI_API_ROOTS:
        raise ValueError(f"Unsupported KALSHI_ENV={kalshi_env!r}")
    api_roots = kalshi_api_root_candidates(kalshi_env)

    stamp = captured_at.astimezone(ZoneInfo("America/New_York")).strftime("%Y%m%d_%H%M%S_%f")[:-3]
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_scan_{stamp}.csv"
    status = "ready"
    events_error = None
    events: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}
    diagnostics = {
        "page_requests": 0,
        "rate_limit_retries_used": 0,
        "network_retries_used": 0,
        "transient_http_retries_used": 0,
    }
    try:
        events, diagnostics = _load_open_events_with_diagnostics(
            api_roots=api_roots,
            timeout_seconds=timeout_seconds,
            page_limit=page_limit,
            max_pages=max_pages,
            http_get_json=http_get_json,
        )
        rows, category_counts = extract_kalshi_nonsports_rows(
            events=events,
            captured_at=captured_at,
            excluded_categories=excluded_categories,
            max_hours_to_close=max_hours_to_close,
        )
    except KalshiEventsFetchError as exc:  # pragma: no cover - defensive runtime path
        diagnostics = dict(exc.diagnostics)
        events_error = str(exc)
        status = "rate_limited" if "status 429" in events_error else "upstream_error"
    except Exception as exc:  # pragma: no cover - defensive runtime path
        events_error = str(exc)
        status = "rate_limited" if "status 429" in events_error else "upstream_error"
    _write_scan_csv(csv_path, rows)

    search_retries_total = (
        diagnostics.get("rate_limit_retries_used", 0)
        + diagnostics.get("network_retries_used", 0)
        + diagnostics.get("transient_http_retries_used", 0)
    )
    search_health_status = "ready"
    if status != "ready":
        search_health_status = "error"
    elif search_retries_total > 0:
        search_health_status = "degraded_retrying"

    output_path = out_dir / f"kalshi_nonsports_scan_summary_{stamp}.json"
    summary = {
        "env_file": str(env_path),
        "captured_at": captured_at.isoformat(),
        "jurisdiction": (env_data.get("BETBOT_JURISDICTION") or "").strip(),
        "kalshi_env": kalshi_env,
        "excluded_categories": list(excluded_categories),
        "max_hours_to_close": max_hours_to_close,
        "events_fetched": len(events),
        "markets_ranked": len(rows),
        "ten_dollar_fillable_markets": sum(1 for row in rows if row["ten_dollar_fillable_at_best_ask"]),
        "categories_considered": category_counts,
        "top_execution_fit_markets": rows[:top_n],
        "events_error": events_error,
        "page_requests": diagnostics.get("page_requests", 0),
        "rate_limit_retries_used": diagnostics.get("rate_limit_retries_used", 0),
        "network_retries_used": diagnostics.get("network_retries_used", 0),
        "transient_http_retries_used": diagnostics.get("transient_http_retries_used", 0),
        "api_root_failovers_used": diagnostics.get("api_root_failovers_used", 0),
        "api_roots": list(api_roots),
        "search_retries_total": search_retries_total,
        "search_health_status": search_health_status,
        "status": status,
        "output_csv": str(csv_path),
        "output_file": str(output_path),
    }
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary
