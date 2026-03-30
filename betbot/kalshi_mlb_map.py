from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import json
import math
from pathlib import Path
import re
import time
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode, urlparse
from zoneinfo import ZoneInfo

from betbot.edge import consensus_rank_score
from betbot.edge import consensus_confidence
from betbot.edge import consensus_stats
from betbot.edge import confidence_adjusted_edge
from betbot.edge import edge_roi_on_cost
from betbot.edge import normalize_implied_probabilities, stale_quote_penalty, stability_adjusted_probability
from betbot.live_candidates import THERUNDOWN_BROWSER_USER_AGENT, american_to_decimal
from betbot.live_smoke import (
    HttpGetter,
    KalshiSigner,
    _http_get_json,
    _kalshi_sign_request,
    kalshi_api_root_candidates,
)
from betbot.onboarding import _is_placeholder, _parse_env_file


KALSHI_MLB_TICKER_PREFIX = "KXMLBGAME-"
ET_ZONE = ZoneInfo("America/New_York")
THERUNDOWN_MAX_RATE_LIMIT_RETRIES = 3
THERUNDOWN_RATE_LIMIT_BASE_DELAY_SECONDS = 1.0
THERUNDOWN_NETWORK_MAX_RETRIES = 4
THERUNDOWN_NETWORK_BACKOFF_SECONDS = 0.75
THERUNDOWN_RETRYABLE_HTTP_STATUSES = {401, 403, 500, 502, 503, 504}
THERUNDOWN_STATUS_MAX_RETRIES = 2
THERUNDOWN_STATUS_BACKOFF_SECONDS = 0.75
KALSHI_MLB_EVENT_RE = re.compile(
    r"^KXMLBGAME-(?P<yy>\d{2})(?P<mon>[A-Z]{3})(?P<day>\d{2})(?P<hour>\d{2})(?P<minute>\d{2})(?P<pair>[A-Z]+)$"
)
MONTH_LOOKUP = {
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
MONTH_CODES = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC",
}


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None
    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_orderable_price(price_dollars: float) -> bool:
    return 0.0 < price_dollars < 1.0


@dataclass(frozen=True)
class TheRundownMlbEvent:
    event_id: str
    event_date_utc: str
    local_start: datetime
    away_abbr: str
    away_team: str
    home_abbr: str
    home_team: str
    away_prob: float
    home_prob: float
    away_mean_prob: float
    home_mean_prob: float
    away_robust_prob: float
    home_robust_prob: float
    away_prob_low: float
    home_prob_low: float
    away_prob_high: float
    home_prob_high: float
    away_prob_range: float
    home_prob_range: float
    away_prob_stddev: float
    home_prob_stddev: float
    away_consensus_stability: float
    home_consensus_stability: float
    away_best_book: str
    away_best_odds: float
    away_best_quote_updated_at: str | None
    away_best_quote_age_seconds: float
    home_best_book: str
    home_best_odds: float
    home_best_quote_updated_at: str | None
    home_best_quote_age_seconds: float
    consensus_book_count: int


def _full_team_name(team: dict[str, Any]) -> str:
    city = str(team.get("name") or "").strip()
    mascot = str(team.get("mascot") or "").strip()
    return " ".join(part for part in [city, mascot] if part).strip()


def estimate_kalshi_taker_fee(price_dollars: float, contracts: int = 1) -> float:
    if contracts <= 0:
        raise ValueError("contracts must be positive")
    if not (0.0 <= price_dollars <= 1.0):
        raise ValueError("price_dollars must be in [0,1]")
    raw_fee = 0.07 * contracts * price_dollars * (1.0 - price_dollars)
    return math.ceil(raw_fee * 100.0) / 100.0


def _find_team_participant_name(team: dict[str, Any], participant_names: list[str]) -> str | None:
    full_name = _full_team_name(team).lower()
    mascot = str(team.get("mascot") or "").strip().lower()
    for participant_name in participant_names:
        lowered = participant_name.lower()
        if lowered == full_name:
            return participant_name
    for participant_name in participant_names:
        lowered = participant_name.lower()
        if mascot and mascot in lowered:
            return participant_name
    return None


def _parse_quote_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None
    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        return None


def _therundown_json_get(
    url: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
) -> tuple[int, Any]:
    headers = {
        "Accept": "application/json",
        "User-Agent": THERUNDOWN_BROWSER_USER_AGENT,
    }
    rate_limit_attempt = 0
    network_attempt = 0
    status_attempt = 0
    while True:
        try:
            status_code, payload = http_get_json(url, headers, timeout_seconds)
        except URLError as exc:
            if network_attempt >= THERUNDOWN_NETWORK_MAX_RETRIES:
                raise ValueError(f"TheRundown request failed: {exc.reason}") from exc
            time.sleep(THERUNDOWN_NETWORK_BACKOFF_SECONDS * (2**network_attempt))
            network_attempt += 1
            continue
        if status_code == 429:
            if rate_limit_attempt >= THERUNDOWN_MAX_RATE_LIMIT_RETRIES:
                return status_code, payload
            time.sleep(THERUNDOWN_RATE_LIMIT_BASE_DELAY_SECONDS * (2**rate_limit_attempt))
            rate_limit_attempt += 1
            continue
        if status_code in THERUNDOWN_RETRYABLE_HTTP_STATUSES:
            if status_attempt >= THERUNDOWN_STATUS_MAX_RETRIES:
                return status_code, payload
            time.sleep(THERUNDOWN_STATUS_BACKOFF_SECONDS * (2**status_attempt))
            status_attempt += 1
            continue
        return status_code, payload


def _load_affiliate_names(
    *,
    base_url: str,
    api_key: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
) -> dict[str, str]:
    url = f"{base_url.rstrip('/')}/affiliates?{urlencode({'key': api_key})}"
    status_code, payload = _therundown_json_get(url, timeout_seconds, http_get_json)
    if status_code != 200 or not isinstance(payload, dict):
        raise ValueError(f"Failed to fetch affiliates from TheRundown (status {status_code})")
    affiliates = payload.get("affiliates")
    if not isinstance(affiliates, list):
        raise ValueError("TheRundown affiliates response is missing affiliates[]")

    names: dict[str, str] = {}
    for affiliate in affiliates:
        if not isinstance(affiliate, dict):
            continue
        affiliate_id = affiliate.get("affiliate_id")
        affiliate_name = affiliate.get("affiliate_name")
        if affiliate_id is None or affiliate_name is None:
            continue
        names[str(affiliate_id)] = str(affiliate_name)
    return names


def _load_therundown_mlb_payload(
    *,
    base_url: str,
    api_key: str,
    event_date: str,
    affiliate_ids: tuple[str, ...],
    timeout_seconds: float,
    http_get_json: HttpGetter,
) -> dict[str, Any]:
    params = {
        "key": api_key,
        "market_ids": "1",
        "affiliate_ids": ",".join(affiliate_ids),
        "main_line": "true",
        "offset": "300",
    }
    url = f"{base_url.rstrip('/')}/sports/3/events/{event_date}?{urlencode(params)}"
    status_code, payload = _therundown_json_get(url, timeout_seconds, http_get_json)
    if status_code != 200 or not isinstance(payload, dict):
        raise ValueError(f"Failed to fetch MLB events from TheRundown (status {status_code})")
    if not isinstance(payload.get("events"), list):
        raise ValueError("TheRundown MLB response is missing events[]")
    return payload


def extract_therundown_mlb_events(
    *,
    events: list[dict[str, Any]],
    affiliate_names: dict[str, str],
    min_books: int,
) -> list[TheRundownMlbEvent]:
    extracted: list[TheRundownMlbEvent] = []

    for event in events:
        teams = event.get("teams")
        markets = event.get("markets")
        if not isinstance(teams, list) or not isinstance(markets, list):
            continue

        away_team_obj = next((team for team in teams if isinstance(team, dict) and team.get("is_away")), None)
        home_team_obj = next((team for team in teams if isinstance(team, dict) and team.get("is_home")), None)
        if away_team_obj is None or home_team_obj is None:
            continue

        moneyline_market = next(
            (
                market
                for market in markets
                if isinstance(market, dict)
                and str(market.get("name") or "").strip().lower() == "moneyline"
            ),
            None,
        )
        if moneyline_market is None:
            continue

        participants = moneyline_market.get("participants")
        if not isinstance(participants, list) or len(participants) != 2:
            continue

        participant_names = [
            str(participant.get("name") or "").strip()
            for participant in participants
            if isinstance(participant, dict)
        ]
        away_participant_name = _find_team_participant_name(away_team_obj, participant_names)
        home_participant_name = _find_team_participant_name(home_team_obj, participant_names)
        if away_participant_name is None or home_participant_name is None:
            continue

        participant_data: dict[str, tuple[float, str, str | None]] = {}
        consensus_probs: dict[str, list[float]] = {}
        latest_complete_quote_update: datetime | None = None

        for participant in participants:
            if not isinstance(participant, dict):
                continue
            participant_name = str(participant.get("name") or "").strip()
            lines = participant.get("lines")
            if participant_name == "" or not isinstance(lines, list) or not lines:
                continue
            prices = lines[0].get("prices")
            if not isinstance(prices, dict):
                continue
            best_decimal: float | None = None
            best_book = ""
            best_updated_at: str | None = None
            for affiliate_id, price_obj in prices.items():
                if not isinstance(price_obj, dict):
                    continue
                price = price_obj.get("price")
                if not isinstance(price, int | float) or price == 0:
                    continue
                decimal_odds = american_to_decimal(float(price))
                if best_decimal is None or decimal_odds > best_decimal:
                    best_decimal = decimal_odds
                    best_book = affiliate_names.get(str(affiliate_id), f"affiliate_{affiliate_id}")
                    best_updated_at = (
                        str(price_obj.get("updated_at")) if price_obj.get("updated_at") is not None else None
                    )
            if best_decimal is None:
                continue
            participant_data[participant_name] = (best_decimal, best_book, best_updated_at)

        if len(participant_data) != 2:
            continue

        book_to_decimals: dict[str, list[tuple[str, float]]] = {}
        for participant in participants:
            if not isinstance(participant, dict):
                continue
            participant_name = str(participant.get("name") or "").strip()
            lines = participant.get("lines")
            if participant_name == "" or not isinstance(lines, list) or not lines:
                continue
            prices = lines[0].get("prices")
            if not isinstance(prices, dict):
                continue
            for affiliate_id, price_obj in prices.items():
                if not isinstance(price_obj, dict):
                    continue
                price = price_obj.get("price")
                if not isinstance(price, int | float) or price == 0:
                    continue
                book_to_decimals.setdefault(str(affiliate_id), []).append(
                    (participant_name, american_to_decimal(float(price)))
                )

        books_used = 0
        ordered_names = [away_participant_name, home_participant_name]
        for affiliate_id, quotes in book_to_decimals.items():
            quote_map = {name: decimal for name, decimal in quotes}
            if any(name not in quote_map for name in ordered_names):
                continue
            for participant in participants:
                if not isinstance(participant, dict):
                    continue
                participant_name = str(participant.get("name") or "").strip()
                if participant_name not in ordered_names:
                    continue
                lines = participant.get("lines")
                if not isinstance(lines, list) or not lines:
                    continue
                prices = lines[0].get("prices")
                if not isinstance(prices, dict):
                    continue
                price_obj = prices.get(affiliate_id)
                if not isinstance(price_obj, dict):
                    continue
                updated_at = _parse_quote_timestamp(
                    str(price_obj.get("updated_at")) if price_obj.get("updated_at") is not None else None
                )
                if updated_at is None:
                    continue
                if latest_complete_quote_update is None or updated_at > latest_complete_quote_update:
                    latest_complete_quote_update = updated_at
            fair_probs = normalize_implied_probabilities([quote_map[name] for name in ordered_names])
            books_used += 1
            consensus_probs.setdefault(ordered_names[0], []).append(fair_probs[0])
            consensus_probs.setdefault(ordered_names[1], []).append(fair_probs[1])

        if books_used < min_books:
            continue

        event_date_utc = str(event.get("event_date") or "")
        if event_date_utc == "":
            continue
        local_start = datetime.fromisoformat(event_date_utc.replace("Z", "+00:00")).astimezone(ET_ZONE)

        away_team = ordered_names[0]
        home_team = ordered_names[1]
        away_best_odds, away_best_book, away_best_updated_at = participant_data[away_team]
        home_best_odds, home_best_book, home_best_updated_at = participant_data[home_team]
        away_best_quote_age_seconds = 0.0
        home_best_quote_age_seconds = 0.0
        away_best_quote_updated = _parse_quote_timestamp(away_best_updated_at)
        home_best_quote_updated = _parse_quote_timestamp(home_best_updated_at)
        if latest_complete_quote_update is not None and away_best_quote_updated is not None:
            away_best_quote_age_seconds = round(
                max(0.0, (latest_complete_quote_update - away_best_quote_updated).total_seconds()),
                3,
            )
        if latest_complete_quote_update is not None and home_best_quote_updated is not None:
            home_best_quote_age_seconds = round(
                max(0.0, (latest_complete_quote_update - home_best_quote_updated).total_seconds()),
                3,
            )

        away_stats = consensus_stats(consensus_probs[away_team])
        home_stats = consensus_stats(consensus_probs[home_team])

        extracted.append(
            TheRundownMlbEvent(
                event_id=str(event.get("event_id") or ""),
                event_date_utc=event_date_utc,
                local_start=local_start,
                away_abbr=str(away_team_obj.get("abbreviation") or "").strip().upper(),
                away_team=away_team,
                home_abbr=str(home_team_obj.get("abbreviation") or "").strip().upper(),
                home_team=home_team,
                away_prob=round(
                    stability_adjusted_probability(away_stats["robust"], away_stats["stability"]),
                    6,
                ),
                home_prob=round(
                    stability_adjusted_probability(home_stats["robust"], home_stats["stability"]),
                    6,
                ),
                away_mean_prob=round(away_stats["mean"], 6),
                home_mean_prob=round(home_stats["mean"], 6),
                away_robust_prob=round(away_stats["robust"], 6),
                home_robust_prob=round(home_stats["robust"], 6),
                away_prob_low=round(away_stats["low"], 6),
                home_prob_low=round(home_stats["low"], 6),
                away_prob_high=round(away_stats["high"], 6),
                home_prob_high=round(home_stats["high"], 6),
                away_prob_range=round(away_stats["range"], 6),
                home_prob_range=round(home_stats["range"], 6),
                away_prob_stddev=round(away_stats["stddev"], 6),
                home_prob_stddev=round(home_stats["stddev"], 6),
                away_consensus_stability=round(away_stats["stability"], 6),
                home_consensus_stability=round(home_stats["stability"], 6),
                away_best_book=away_best_book,
                away_best_odds=round(away_best_odds, 6),
                away_best_quote_updated_at=away_best_updated_at,
                away_best_quote_age_seconds=away_best_quote_age_seconds,
                home_best_book=home_best_book,
                home_best_odds=round(home_best_odds, 6),
                home_best_quote_updated_at=home_best_updated_at,
                home_best_quote_age_seconds=home_best_quote_age_seconds,
                consensus_book_count=books_used,
            )
        )

    extracted.sort(key=lambda item: item.local_start)
    return extracted


def _kalshi_get_json(
    *,
    env_data: dict[str, str],
    path_with_query: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
    sign_request: KalshiSigner,
) -> tuple[int, Any]:
    env_name = (env_data.get("KALSHI_ENV") or "").strip().lower()
    api_roots = kalshi_api_root_candidates(env_name)
    transient_status_codes = {408, 425, 500, 502, 503, 504, 599}
    attempted_roots: list[str] = []
    final_status = 599
    final_payload: Any = {"error": "request_not_attempted", "error_type": "network_error"}

    for index, api_root in enumerate(api_roots):
        attempted_roots.append(api_root)
        request_url = f"{api_root}{path_with_query}"
        timestamp_ms = str(int(time.time() * 1000))
        signature = sign_request(
            env_data["KALSHI_PRIVATE_KEY_PATH"],
            timestamp_ms,
            "GET",
            urlparse(request_url).path,
        )
        try:
            status_code, payload = http_get_json(
                request_url,
                {
                    "Accept": "application/json",
                    "KALSHI-ACCESS-KEY": env_data["KALSHI_ACCESS_KEY_ID"],
                    "KALSHI-ACCESS-SIGNATURE": signature,
                    "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
                    "User-Agent": "betbot-kalshi-mlb-map/1.0",
                },
                timeout_seconds,
            )
        except URLError as exc:
            final_status = 599
            final_payload = {
                "error": str(exc.reason or exc),
                "error_type": "url_error",
                "api_root_used": api_root,
                "api_roots_attempted": list(attempted_roots),
            }
            if index < len(api_roots) - 1:
                continue
            return final_status, final_payload

        final_status = status_code
        final_payload = payload
        if isinstance(final_payload, dict):
            final_payload.setdefault("api_root_used", api_root)
            final_payload.setdefault("api_roots_attempted", list(attempted_roots))
        if status_code in transient_status_codes and index < len(api_roots) - 1:
            continue
        return final_status, final_payload

    if isinstance(final_payload, dict):
        final_payload.setdefault("api_roots_attempted", list(attempted_roots))
    return final_status, final_payload


def _build_kalshi_mlb_event_ticker(local_start: datetime, away_abbr: str, home_abbr: str) -> str:
    return (
        f"{KALSHI_MLB_TICKER_PREFIX}"
        f"{local_start.year % 100:02d}"
        f"{MONTH_CODES[local_start.month]}"
        f"{local_start.day:02d}"
        f"{local_start.hour:02d}"
        f"{local_start.minute:02d}"
        f"{away_abbr}{home_abbr}"
    )


def _load_kalshi_mlb_markets(
    *,
    env_data: dict[str, str],
    therundown_events: list[TheRundownMlbEvent],
    timeout_seconds: float,
    http_get_json: HttpGetter,
    sign_request: KalshiSigner,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []

    for event in therundown_events:
        event_ticker = _build_kalshi_mlb_event_ticker(
            event.local_start,
            event.away_abbr,
            event.home_abbr,
        )
        for selected_code in (event.away_abbr, event.home_abbr):
            market_ticker = f"{event_ticker}-{selected_code}"
            status_code, payload = _kalshi_get_json(
                env_data=env_data,
                path_with_query=f"/markets/{market_ticker}",
                timeout_seconds=timeout_seconds,
                http_get_json=http_get_json,
                sign_request=sign_request,
            )
            if status_code != 200 or not isinstance(payload, dict):
                continue
            market = payload.get("market")
            if isinstance(market, dict):
                collected.append(market)

    return collected


def _parse_kalshi_event_ticker(event_ticker: str) -> tuple[datetime, str] | None:
    match = KALSHI_MLB_EVENT_RE.match(event_ticker)
    if not match:
        return None
    year = 2000 + int(match.group("yy"))
    month = MONTH_LOOKUP[match.group("mon")]
    day = int(match.group("day"))
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    local_start = datetime(year, month, day, hour, minute, tzinfo=ET_ZONE)
    return local_start, match.group("pair")


def extract_kalshi_mlb_rows(
    *,
    therundown_events: list[TheRundownMlbEvent],
    kalshi_markets: list[dict[str, Any]],
    time_tolerance_minutes: int = 15,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for market in kalshi_markets:
        event_ticker = str(market.get("event_ticker") or "")
        parsed = _parse_kalshi_event_ticker(event_ticker)
        ticker = str(market.get("ticker") or "")
        if parsed is None or ticker == "":
            continue
        kalshi_local_start, pair_code = parsed
        matching_events = [
            event
            for event in therundown_events
            if f"{event.away_abbr}{event.home_abbr}" == pair_code
        ]
        if not matching_events:
            continue
        therundown_event = min(
            matching_events,
            key=lambda event: abs(int((kalshi_local_start - event.local_start).total_seconds() // 60)),
        )

        time_diff_minutes = abs(int((kalshi_local_start - therundown_event.local_start).total_seconds() // 60))
        selected_code = ticker.rsplit("-", 1)[-1]
        if selected_code == therundown_event.away_abbr:
            team_name = therundown_event.away_team
            model_prob = therundown_event.away_prob
            best_book = therundown_event.away_best_book
            best_odds = therundown_event.away_best_odds
            best_quote_updated_at = therundown_event.away_best_quote_updated_at
            best_quote_age_seconds = therundown_event.away_best_quote_age_seconds
            consensus_mean_prob = therundown_event.away_mean_prob
            consensus_robust_prob = therundown_event.away_robust_prob
            consensus_prob_low = therundown_event.away_prob_low
            consensus_prob_high = therundown_event.away_prob_high
            consensus_prob_range = therundown_event.away_prob_range
            consensus_prob_stddev = therundown_event.away_prob_stddev
            consensus_stability = therundown_event.away_consensus_stability
        elif selected_code == therundown_event.home_abbr:
            team_name = therundown_event.home_team
            model_prob = therundown_event.home_prob
            best_book = therundown_event.home_best_book
            best_odds = therundown_event.home_best_odds
            best_quote_updated_at = therundown_event.home_best_quote_updated_at
            best_quote_age_seconds = therundown_event.home_best_quote_age_seconds
            consensus_mean_prob = therundown_event.home_mean_prob
            consensus_robust_prob = therundown_event.home_robust_prob
            consensus_prob_low = therundown_event.home_prob_low
            consensus_prob_high = therundown_event.home_prob_high
            consensus_prob_range = therundown_event.home_prob_range
            consensus_prob_stddev = therundown_event.home_prob_stddev
            consensus_stability = therundown_event.home_consensus_stability
        else:
            continue

        yes_ask = float(str(market.get("yes_ask_dollars") or "0"))
        yes_bid = float(str(market.get("yes_bid_dollars") or "0"))
        no_bid = round(1.0 - yes_ask, 6)
        no_ask = round(1.0 - yes_bid, 6)
        fair_no_probability = round(1.0 - model_prob, 6)
        confidence_factor = round(
            consensus_confidence(
                stability=consensus_stability,
                books_used=therundown_event.consensus_book_count,
                min_books=min(2, therundown_event.consensus_book_count),
            ),
            6,
        )
        estimated_taker_fee_yes = estimate_kalshi_taker_fee(yes_ask, contracts=1)
        estimated_taker_fee_no = estimate_kalshi_taker_fee(no_ask, contracts=1)
        gross_edge_buy_yes = round(model_prob - yes_ask, 6)
        gross_edge_sell_yes = round(yes_bid - model_prob, 6)
        gross_edge_buy_no = round(fair_no_probability - no_ask, 6)
        gross_edge_sell_no = round(no_bid - fair_no_probability, 6)
        net_edge_buy_yes = round(gross_edge_buy_yes - estimated_taker_fee_yes, 6)
        net_edge_buy_no = round(gross_edge_buy_no - estimated_taker_fee_no, 6)
        confidence_adjusted_gross_edge_buy_yes = round(
            confidence_adjusted_edge(gross_edge_buy_yes, confidence_factor),
            6,
        )
        confidence_adjusted_gross_edge_buy_no = round(
            confidence_adjusted_edge(gross_edge_buy_no, confidence_factor),
            6,
        )
        confidence_adjusted_net_edge_buy_yes = round(
            confidence_adjusted_edge(net_edge_buy_yes, confidence_factor),
            6,
        )
        confidence_adjusted_net_edge_buy_no = round(
            confidence_adjusted_edge(net_edge_buy_no, confidence_factor),
            6,
        )
        gross_roi_buy_yes: float | str = ""
        gross_roi_buy_no: float | str = ""
        net_roi_buy_yes: float | str = ""
        net_roi_buy_no: float | str = ""
        confidence_adjusted_net_roi_buy_yes: float | str = ""
        confidence_adjusted_net_roi_buy_no: float | str = ""
        entry_candidates: list[tuple[str, float, float, float, float, float, float, float]] = []
        if _is_orderable_price(yes_ask):
            gross_roi_buy_yes = round(edge_roi_on_cost(gross_edge_buy_yes, yes_ask), 6)
            net_roi_buy_yes = round(edge_roi_on_cost(net_edge_buy_yes, yes_ask), 6)
            confidence_adjusted_net_roi_buy_yes = round(
                edge_roi_on_cost(confidence_adjusted_net_edge_buy_yes, yes_ask),
                6,
            )
            entry_candidates.append(
                (
                    "yes",
                    gross_edge_buy_yes,
                    net_edge_buy_yes,
                    confidence_adjusted_net_edge_buy_yes,
                    float(gross_roi_buy_yes),
                    float(net_roi_buy_yes),
                    float(confidence_adjusted_net_roi_buy_yes),
                    yes_ask,
                )
            )
        if _is_orderable_price(no_ask):
            gross_roi_buy_no = round(edge_roi_on_cost(gross_edge_buy_no, no_ask), 6)
            net_roi_buy_no = round(edge_roi_on_cost(net_edge_buy_no, no_ask), 6)
            confidence_adjusted_net_roi_buy_no = round(
                edge_roi_on_cost(confidence_adjusted_net_edge_buy_no, no_ask),
                6,
            )
            entry_candidates.append(
                (
                    "no",
                    gross_edge_buy_no,
                    net_edge_buy_no,
                    confidence_adjusted_net_edge_buy_no,
                    float(gross_roi_buy_no),
                    float(net_roi_buy_no),
                    float(confidence_adjusted_net_roi_buy_no),
                    no_ask,
                )
            )
        best_entry_side = ""
        best_entry_edge: float | str = ""
        best_entry_net_edge: float | str = ""
        best_entry_confidence_adjusted_net_edge: float | str = ""
        best_entry_roi_on_cost: float | str = ""
        best_entry_confidence_adjusted_roi_on_cost: float | str = ""
        best_entry_price_dollars: float | str = ""
        stale_quote_penalty_value = round(stale_quote_penalty(best_quote_age_seconds), 6)
        best_entry_rank_score: float | str = ""
        if entry_candidates:
            (
                best_side,
                best_gross_edge,
                best_net_edge,
                best_adjusted_net_edge,
                _best_gross_roi,
                best_net_roi,
                best_adjusted_net_roi,
                best_price,
            ) = max(
                entry_candidates,
                key=lambda item: (item[6], item[3], item[5], item[2], item[4], item[1], -item[7]),
            )
            best_entry_side = best_side
            best_entry_edge = round(best_gross_edge, 6)
            best_entry_net_edge = round(best_net_edge, 6)
            best_entry_confidence_adjusted_net_edge = round(best_adjusted_net_edge, 6)
            best_entry_roi_on_cost = round(best_net_roi, 6)
            best_entry_confidence_adjusted_roi_on_cost = round(best_adjusted_net_roi, 6)
            best_entry_price_dollars = round(best_price, 6)
            best_entry_rank_score = consensus_rank_score(
                base_edge=best_entry_confidence_adjusted_roi_on_cost,
                stability=consensus_stability,
                books_used=therundown_event.consensus_book_count,
                min_books=min(2, therundown_event.consensus_book_count),
                stale_quote_penalty_value=stale_quote_penalty_value,
            )
        maker_candidates: list[tuple[str, float, float]] = []
        if _is_orderable_price(yes_bid):
            maker_candidates.append(("yes", gross_edge_sell_yes, yes_bid))
        if _is_orderable_price(no_bid):
            maker_candidates.append(("no", gross_edge_sell_no, no_bid))
        best_maker_entry_side = ""
        best_maker_entry_edge: float | str = ""
        best_maker_entry_price_dollars: float | str = ""
        if maker_candidates:
            best_side, best_edge, best_price = max(maker_candidates, key=lambda item: (item[1], -item[2]))
            best_maker_entry_side = best_side
            best_maker_entry_edge = round(best_edge, 6)
            best_maker_entry_price_dollars = round(best_price, 6)
        confidence = "high" if time_diff_minutes <= time_tolerance_minutes else "medium"
        why = "abbr_pair+ticker_time" if confidence == "high" else "abbr_pair_only"
        rows.append(
            {
                "timestamp": therundown_event.local_start.isoformat(),
                "therundown_event_id": therundown_event.event_id,
                "kalshi_event_ticker": event_ticker,
                "kalshi_market_ticker": ticker,
                "selection": team_name,
                "away_team": therundown_event.away_team,
                "home_team": therundown_event.home_team,
                "away_abbr": therundown_event.away_abbr,
                "home_abbr": therundown_event.home_abbr,
                "therundown_model_prob": model_prob,
                "therundown_fair_no_prob": fair_no_probability,
                "therundown_mean_prob": (
                    consensus_mean_prob
                ),
                "therundown_robust_prob": consensus_robust_prob,
                "therundown_prob_low": consensus_prob_low,
                "therundown_prob_high": consensus_prob_high,
                "therundown_prob_range": consensus_prob_range,
                "therundown_prob_stddev": consensus_prob_stddev,
                "therundown_consensus_stability": consensus_stability,
                "therundown_consensus_confidence": confidence_factor,
                "therundown_best_book": best_book,
                "therundown_best_odds": best_odds,
                "therundown_best_quote_updated_at": best_quote_updated_at or "",
                "therundown_best_quote_age_seconds": best_quote_age_seconds,
                "therundown_stale_quote_penalty": stale_quote_penalty_value,
                "therundown_consensus_book_count": therundown_event.consensus_book_count,
                "kalshi_yes_bid_dollars": yes_bid,
                "kalshi_yes_ask_dollars": yes_ask,
                "kalshi_no_bid_dollars": no_bid,
                "kalshi_no_ask_dollars": no_ask,
                "gross_edge_buy_yes": gross_edge_buy_yes,
                "gross_edge_sell_yes": gross_edge_sell_yes,
                "gross_edge_buy_no": gross_edge_buy_no,
                "gross_edge_sell_no": gross_edge_sell_no,
                "gross_roi_buy_yes": gross_roi_buy_yes,
                "gross_roi_buy_no": gross_roi_buy_no,
                "confidence_adjusted_gross_edge_buy_yes": confidence_adjusted_gross_edge_buy_yes,
                "confidence_adjusted_gross_edge_buy_no": confidence_adjusted_gross_edge_buy_no,
                "estimated_taker_fee_buy_yes_1x": round(estimated_taker_fee_yes, 2),
                "estimated_taker_fee_buy_no_1x": round(estimated_taker_fee_no, 2),
                "net_edge_buy_yes_after_fees_1x": net_edge_buy_yes,
                "net_edge_buy_no_after_fees_1x": net_edge_buy_no,
                "net_roi_buy_yes_after_fees_1x": net_roi_buy_yes,
                "net_roi_buy_no_after_fees_1x": net_roi_buy_no,
                "confidence_adjusted_net_edge_buy_yes_after_fees_1x": confidence_adjusted_net_edge_buy_yes,
                "confidence_adjusted_net_edge_buy_no_after_fees_1x": confidence_adjusted_net_edge_buy_no,
                "confidence_adjusted_net_roi_buy_yes_after_fees_1x": confidence_adjusted_net_roi_buy_yes,
                "confidence_adjusted_net_roi_buy_no_after_fees_1x": confidence_adjusted_net_roi_buy_no,
                "best_entry_side": best_entry_side,
                "best_entry_price_dollars": best_entry_price_dollars,
                "best_entry_edge": best_entry_edge,
                "best_entry_net_edge_after_fees_1x": best_entry_net_edge,
                "best_entry_confidence_adjusted_net_edge_after_fees_1x": best_entry_confidence_adjusted_net_edge,
                "best_entry_roi_on_cost_after_fees_1x": best_entry_roi_on_cost,
                "best_entry_confidence_adjusted_roi_on_cost_after_fees_1x": best_entry_confidence_adjusted_roi_on_cost,
                "best_entry_rank_score": best_entry_rank_score,
                "best_maker_entry_side": best_maker_entry_side,
                "best_maker_entry_price_dollars": best_maker_entry_price_dollars,
                "best_maker_entry_edge": best_maker_entry_edge,
                "time_diff_minutes": time_diff_minutes,
                "confidence": confidence,
                "why": why,
                "rules_primary": str(market.get("rules_primary") or ""),
                "rules_secondary": str(market.get("rules_secondary") or ""),
                "kalshi_title": str(market.get("title") or ""),
                "kalshi_yes_sub_title": str(market.get("yes_sub_title") or ""),
            }
        )

    rows.sort(
        key=lambda row: (
            row["best_entry_rank_score"]
            if isinstance(row["best_entry_rank_score"], float)
            else float("-inf"),
            row["best_entry_edge"] if isinstance(row["best_entry_edge"], float) else float("-inf"),
            row["best_maker_entry_edge"] if isinstance(row["best_maker_entry_edge"], float) else float("-inf"),
        ),
        reverse=True,
    )
    return rows


def _write_map_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp",
        "therundown_event_id",
        "kalshi_event_ticker",
        "kalshi_market_ticker",
        "selection",
        "away_team",
        "home_team",
        "away_abbr",
        "home_abbr",
        "therundown_model_prob",
        "therundown_fair_no_prob",
        "therundown_mean_prob",
        "therundown_robust_prob",
        "therundown_prob_low",
        "therundown_prob_high",
        "therundown_prob_range",
        "therundown_prob_stddev",
        "therundown_consensus_stability",
        "therundown_consensus_confidence",
        "therundown_best_book",
        "therundown_best_odds",
        "therundown_best_quote_updated_at",
        "therundown_best_quote_age_seconds",
        "therundown_stale_quote_penalty",
        "therundown_consensus_book_count",
        "kalshi_yes_bid_dollars",
        "kalshi_yes_ask_dollars",
        "kalshi_no_bid_dollars",
        "kalshi_no_ask_dollars",
        "gross_edge_buy_yes",
        "gross_edge_sell_yes",
        "gross_edge_buy_no",
        "gross_edge_sell_no",
        "gross_roi_buy_yes",
        "gross_roi_buy_no",
        "confidence_adjusted_gross_edge_buy_yes",
        "confidence_adjusted_gross_edge_buy_no",
        "estimated_taker_fee_buy_yes_1x",
        "estimated_taker_fee_buy_no_1x",
        "net_edge_buy_yes_after_fees_1x",
        "net_edge_buy_no_after_fees_1x",
        "net_roi_buy_yes_after_fees_1x",
        "net_roi_buy_no_after_fees_1x",
        "confidence_adjusted_net_edge_buy_yes_after_fees_1x",
        "confidence_adjusted_net_edge_buy_no_after_fees_1x",
        "confidence_adjusted_net_roi_buy_yes_after_fees_1x",
        "confidence_adjusted_net_roi_buy_no_after_fees_1x",
        "best_entry_side",
        "best_entry_price_dollars",
        "best_entry_edge",
        "best_entry_net_edge_after_fees_1x",
        "best_entry_confidence_adjusted_net_edge_after_fees_1x",
        "best_entry_roi_on_cost_after_fees_1x",
        "best_entry_confidence_adjusted_roi_on_cost_after_fees_1x",
        "best_entry_rank_score",
        "best_maker_entry_side",
        "best_maker_entry_price_dollars",
        "best_maker_entry_edge",
        "time_diff_minutes",
        "confidence",
        "why",
        "kalshi_title",
        "kalshi_yes_sub_title",
        "rules_primary",
        "rules_secondary",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_map_summary(output_dir: str, event_date: str, summary: dict[str, Any]) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"kalshi_mlb_map_summary_{event_date}_{stamp}.json"
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(json_path)
    return summary


def _load_latest_cached_map_summary(*, output_dir: str, event_date: str) -> dict[str, Any] | None:
    out_dir = Path(output_dir)
    if not out_dir.exists():
        return None

    pattern = f"kalshi_mlb_map_summary_{event_date}_*.json"
    summary_paths = sorted(out_dir.glob(pattern), reverse=True)
    now = datetime.now().astimezone()
    for summary_path in summary_paths:
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(payload, dict):
            continue
        source_status = str(payload.get("status") or "").strip().lower()
        if source_status not in {"ready", "empty"}:
            continue
        output_csv = payload.get("output_csv")
        resolved_output_csv: str | None = None
        if isinstance(output_csv, str) and output_csv.strip() != "":
            csv_path = Path(output_csv)
            if csv_path.exists():
                resolved_output_csv = str(csv_path)
        if source_status == "ready" and resolved_output_csv is None:
            continue
        captured_at = _parse_iso_datetime(payload.get("captured_at"))
        cache_age_seconds: float | None = None
        if captured_at is not None:
            if captured_at.tzinfo is None:
                captured_at = captured_at.replace(tzinfo=now.tzinfo)
            cache_age_seconds = max(0.0, round((now - captured_at).total_seconds(), 3))
        return {
            "summary": payload,
            "summary_file": str(summary_path),
            "source_status": source_status,
            "cache_age_seconds": cache_age_seconds,
            "output_csv": resolved_output_csv,
        }
    return None


def run_kalshi_mlb_map(
    *,
    env_file: str,
    event_date: str,
    output_dir: str = "outputs",
    affiliate_ids: tuple[str, ...] = ("19", "22", "23"),
    min_books: int = 2,
    timeout_seconds: float = 15.0,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
) -> dict[str, Any]:
    env_path = Path(env_file)
    data = _parse_env_file(env_path)
    if _is_placeholder(data.get("THERUNDOWN_API_KEY")):
        raise ValueError("THERUNDOWN_API_KEY is missing")
    if _is_placeholder(data.get("KALSHI_ACCESS_KEY_ID")) or _is_placeholder(data.get("KALSHI_PRIVATE_KEY_PATH")):
        raise ValueError("Kalshi credentials are missing")
    try:
        affiliate_names = _load_affiliate_names(
            base_url=data.get("THERUNDOWN_BASE_URL") or "https://therundown.io/api/v2",
            api_key=data["THERUNDOWN_API_KEY"],
            timeout_seconds=timeout_seconds,
            http_get_json=http_get_json,
        )
        therundown_payload = _load_therundown_mlb_payload(
            base_url=data.get("THERUNDOWN_BASE_URL") or "https://therundown.io/api/v2",
            api_key=data["THERUNDOWN_API_KEY"],
            event_date=event_date,
            affiliate_ids=affiliate_ids,
            timeout_seconds=timeout_seconds,
            http_get_json=http_get_json,
        )
        therundown_events = extract_therundown_mlb_events(
            events=therundown_payload["events"],
            affiliate_names=affiliate_names,
            min_books=min_books,
        )
        kalshi_markets = _load_kalshi_mlb_markets(
            env_data=data,
            therundown_events=therundown_events,
            timeout_seconds=timeout_seconds,
            http_get_json=http_get_json,
            sign_request=sign_request,
        )
        mapped_rows = extract_kalshi_mlb_rows(
            therundown_events=therundown_events,
            kalshi_markets=kalshi_markets,
        )
    except ValueError as exc:
        error_message = str(exc)
        cached = _load_latest_cached_map_summary(output_dir=output_dir, event_date=event_date)
        if cached is not None:
            cached_summary = cached["summary"]
            top_rows = cached_summary.get("top_rows")
            cached_output_csv = cached["output_csv"]
            stale_status = "stale_ready" if cached["source_status"] == "ready" else "stale_empty"
            fallback_summary: dict[str, Any] = {
                "env_file": str(env_path),
                "captured_at": datetime.now().isoformat(),
                "event_date": event_date,
                "affiliate_ids": list(affiliate_ids),
                "therundown_events_considered": int(cached_summary.get("therundown_events_considered") or 0),
                "kalshi_mlb_markets_considered": int(cached_summary.get("kalshi_mlb_markets_considered") or 0),
                "mapped_rows": int(cached_summary.get("mapped_rows") or 0),
                "positive_buy_yes_rows": int(cached_summary.get("positive_buy_yes_rows") or 0),
                "positive_net_buy_yes_rows": int(cached_summary.get("positive_net_buy_yes_rows") or 0),
                "positive_buy_no_rows": int(cached_summary.get("positive_buy_no_rows") or 0),
                "positive_net_buy_no_rows": int(cached_summary.get("positive_net_buy_no_rows") or 0),
                "positive_best_entry_rows": int(cached_summary.get("positive_best_entry_rows") or 0),
                "status": stale_status,
                "top_rows": list(top_rows) if isinstance(top_rows, list) else [],
                "error": error_message,
                "fallback_source_status": cached["source_status"],
                "fallback_summary_file": cached["summary_file"],
                "fallback_cache_age_seconds": cached["cache_age_seconds"],
            }
            if cached_output_csv is not None:
                fallback_summary["output_csv"] = cached_output_csv
            return _write_map_summary(output_dir, event_date, fallback_summary)

        return _write_map_summary(output_dir, event_date, {
            "env_file": str(env_path),
            "captured_at": datetime.now().isoformat(),
            "event_date": event_date,
            "affiliate_ids": list(affiliate_ids),
            "therundown_events_considered": 0,
            "kalshi_mlb_markets_considered": 0,
            "mapped_rows": 0,
            "positive_buy_yes_rows": 0,
            "positive_net_buy_yes_rows": 0,
            "positive_buy_no_rows": 0,
            "positive_net_buy_no_rows": 0,
            "positive_best_entry_rows": 0,
            "status": "error",
            "top_rows": [],
            "error": error_message,
        })

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"kalshi_mlb_map_{event_date}_{stamp}.csv"
    _write_map_csv(csv_path, mapped_rows)

    positive_buy_yes = [row for row in mapped_rows if float(row["gross_edge_buy_yes"]) > 0]
    positive_net_buy_yes = [row for row in mapped_rows if float(row["net_edge_buy_yes_after_fees_1x"]) > 0]
    positive_buy_no = [row for row in mapped_rows if float(row["gross_edge_buy_no"]) > 0]
    positive_net_buy_no = [row for row in mapped_rows if float(row["net_edge_buy_no_after_fees_1x"]) > 0]
    positive_best_entry = [
        row
        for row in mapped_rows
        if isinstance(row["best_entry_net_edge_after_fees_1x"], float)
        and row["best_entry_net_edge_after_fees_1x"] > 0
    ]
    summary: dict[str, Any] = {
        "env_file": str(env_path),
        "captured_at": datetime.now().isoformat(),
        "event_date": event_date,
        "affiliate_ids": list(affiliate_ids),
        "therundown_events_considered": len(therundown_events),
        "kalshi_mlb_markets_considered": len(kalshi_markets),
        "mapped_rows": len(mapped_rows),
        "positive_buy_yes_rows": len(positive_buy_yes),
        "positive_net_buy_yes_rows": len(positive_net_buy_yes),
        "positive_buy_no_rows": len(positive_buy_no),
        "positive_net_buy_no_rows": len(positive_net_buy_no),
        "positive_best_entry_rows": len(positive_best_entry),
        "status": "ready" if mapped_rows else "empty",
        "top_rows": [
            {
                "selection": row["selection"],
                "timestamp": row["timestamp"],
                "kalshi_market_ticker": row["kalshi_market_ticker"],
                "therundown_model_prob": row["therundown_model_prob"],
                "therundown_consensus_stability": row["therundown_consensus_stability"],
                "therundown_consensus_confidence": row["therundown_consensus_confidence"],
                "therundown_prob_range": row["therundown_prob_range"],
                "kalshi_yes_ask_dollars": row["kalshi_yes_ask_dollars"],
                "kalshi_no_ask_dollars": row["kalshi_no_ask_dollars"],
                "gross_edge_buy_yes": row["gross_edge_buy_yes"],
                "gross_edge_buy_no": row["gross_edge_buy_no"],
                "net_edge_buy_yes_after_fees_1x": row["net_edge_buy_yes_after_fees_1x"],
                "net_edge_buy_no_after_fees_1x": row["net_edge_buy_no_after_fees_1x"],
                "best_entry_confidence_adjusted_net_edge_after_fees_1x": row[
                    "best_entry_confidence_adjusted_net_edge_after_fees_1x"
                ],
                "best_entry_roi_on_cost_after_fees_1x": row["best_entry_roi_on_cost_after_fees_1x"],
                "best_entry_confidence_adjusted_roi_on_cost_after_fees_1x": row[
                    "best_entry_confidence_adjusted_roi_on_cost_after_fees_1x"
                ],
                "best_entry_side": row["best_entry_side"],
                "best_entry_edge": row["best_entry_edge"],
                "best_entry_net_edge_after_fees_1x": row["best_entry_net_edge_after_fees_1x"],
                "best_entry_rank_score": row["best_entry_rank_score"],
                "therundown_best_quote_age_seconds": row["therundown_best_quote_age_seconds"],
                "therundown_stale_quote_penalty": row["therundown_stale_quote_penalty"],
                "confidence": row["confidence"],
            }
            for row in mapped_rows[:5]
        ],
        "output_csv": str(csv_path),
    }
    return _write_map_summary(output_dir, event_date, summary)
