from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import time
from typing import Any
from urllib.error import URLError
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from betbot.edge import consensus_rank_score
from betbot.edge import consensus_confidence
from betbot.edge import consensus_stats
from betbot.edge import confidence_adjusted_edge
from betbot.edge import normalize_implied_probabilities
from betbot.edge import expected_value_decimal
from betbot.edge import probability_from_expected_value_decimal
from betbot.edge import stale_quote_penalty
from betbot.edge import stability_adjusted_probability
from betbot.live_smoke import HttpGetter, _http_get_json
from betbot.onboarding import _is_placeholder, _parse_env_file


THERUNDOWN_BROWSER_USER_AGENT = "Mozilla/5.0 betbot/1.0"
THERUNDOWN_MAX_RATE_LIMIT_RETRIES = 3
THERUNDOWN_RATE_LIMIT_BASE_DELAY_SECONDS = 1.0
THERUNDOWN_NETWORK_MAX_RETRIES = 2
THERUNDOWN_NETWORK_BACKOFF_SECONDS = 0.75
THERUNDOWN_RETRYABLE_HTTP_STATUSES = {401, 403, 500, 502, 503, 504}
THERUNDOWN_STATUS_MAX_RETRIES = 2
THERUNDOWN_STATUS_BACKOFF_SECONDS = 0.75
DEFAULT_THERUNDOWN_AFFILIATE_IDS = ("19", "22", "23")
SUPPORTED_MARKET_IDS = {1: "moneyline", 2: "handicap", 3: "totals"}
PREGAME_STATUSES = {"", "STATUS_SCHEDULED", "STATUS_CREATED", "STATUS_DELAYED", "STATUS_TIME_TBD"}


@dataclass(frozen=True)
class SideQuote:
    participant_name: str
    line_value: str
    affiliate_id: str
    affiliate_name: str
    american_price: float
    updated_at: str | None

    @property
    def decimal_odds(self) -> float:
        return american_to_decimal(self.american_price)


def american_to_decimal(price: float) -> float:
    if price == 0:
        raise ValueError("American odds cannot be zero")
    if price > 0:
        return round(1.0 + (price / 100.0), 6)
    return round(1.0 + (100.0 / abs(price)), 6)


def _parse_int_csv(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _format_numeric_string(value: str) -> str:
    if value == "":
        return ""
    numeric = float(value)
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:g}"


def _pair_key(market_name: str, raw_line_value: str) -> str:
    if market_name == "moneyline":
        return "moneyline"
    formatted = _format_numeric_string(raw_line_value)
    if formatted == "":
        return ""
    numeric = float(formatted)
    if market_name == "handicap":
        numeric = abs(numeric)
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:g}"


def _selection_label(market_name: str, participant_name: str, line_value: str) -> str:
    if market_name == "moneyline":
        return f"{participant_name} ML"
    if line_value == "":
        return participant_name
    return f"{participant_name} {_format_numeric_string(line_value)}"


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


def _event_status(event: dict[str, Any]) -> str:
    score = event.get("score")
    if not isinstance(score, dict):
        return ""
    status = score.get("event_status")
    return str(status or "").strip()


def _localize_event_timestamp(event_date: str, timezone_name: str) -> datetime:
    dt = datetime.fromisoformat(event_date.replace("Z", "+00:00"))
    return dt.astimezone(ZoneInfo(timezone_name))


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


def _load_event_payload(
    *,
    base_url: str,
    api_key: str,
    sport_id: int,
    event_date: str,
    market_ids: list[int],
    affiliate_ids: list[str],
    offset_minutes: int,
    timeout_seconds: float,
    http_get_json: HttpGetter,
) -> dict[str, Any]:
    params = {
        "key": api_key,
        "market_ids": ",".join(str(item) for item in market_ids),
        "affiliate_ids": ",".join(affiliate_ids),
        "main_line": "true",
        "offset": str(offset_minutes),
    }
    url = f"{base_url.rstrip('/')}/sports/{sport_id}/events/{event_date}?{urlencode(params)}"
    status_code, payload = _therundown_json_get(url, timeout_seconds, http_get_json)
    if status_code != 200 or not isinstance(payload, dict):
        raise ValueError(f"Failed to fetch events from TheRundown (status {status_code})")
    if not isinstance(payload.get("events"), list):
        raise ValueError("TheRundown events response is missing events[]")
    return payload


def extract_candidate_rows(
    *,
    events: list[dict[str, Any]],
    affiliate_names: dict[str, str],
    min_books: int,
    timezone_name: str,
    include_in_play: bool,
    allowed_market_names: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    counters = {
        "events_total": len(events),
        "events_skipped_in_play": 0,
        "market_pairs_seen": 0,
        "market_pairs_with_consensus": 0,
        "market_pairs_skipped_book_depth": 0,
    }

    for event in events:
        if not include_in_play and _event_status(event) not in PREGAME_STATUSES:
            counters["events_skipped_in_play"] += 1
            continue

        event_date = str(event.get("event_date") or "")
        if event_date == "":
            continue

        teams = event.get("teams") if isinstance(event.get("teams"), list) else []
        away_team = ""
        home_team = ""
        for team in teams:
            if not isinstance(team, dict):
                continue
            name = str(team.get("name") or "").strip()
            mascot = str(team.get("mascot") or "").strip()
            full_name = " ".join(part for part in [name, mascot] if part).strip()
            if team.get("is_away"):
                away_team = full_name or name
            if team.get("is_home"):
                home_team = full_name or name

        for market in event.get("markets", []):
            if not isinstance(market, dict):
                continue
            market_name = str(market.get("name") or "").strip().lower()
            participants = market.get("participants")
            if market_name not in SUPPORTED_MARKET_IDS.values() or not isinstance(participants, list):
                continue
            if allowed_market_names is not None and market_name not in allowed_market_names:
                continue

            participant_order = [
                str(participant.get("name") or "").strip()
                for participant in participants
                if isinstance(participant, dict)
            ]
            grouped_quotes: dict[str, dict[str, dict[str, SideQuote]]] = {}

            for participant in participants:
                if not isinstance(participant, dict):
                    continue
                participant_name = str(participant.get("name") or "").strip()
                if participant_name == "":
                    continue
                lines = participant.get("lines")
                if not isinstance(lines, list):
                    continue
                for line in lines:
                    if not isinstance(line, dict):
                        continue
                    raw_line_value = str(line.get("value") or "").strip()
                    pair_group = _pair_key(market_name, raw_line_value)
                    prices = line.get("prices")
                    if not isinstance(prices, dict):
                        continue
                    for affiliate_id, price_obj in prices.items():
                        if not isinstance(price_obj, dict):
                            continue
                        price = price_obj.get("price")
                        if not isinstance(price, int | float) or price in {0, 0.0001}:
                            continue
                        affiliate_id_str = str(affiliate_id)
                        grouped_quotes.setdefault(pair_group, {}).setdefault(affiliate_id_str, {})[
                            participant_name
                        ] = SideQuote(
                            participant_name=participant_name,
                            line_value=raw_line_value,
                            affiliate_id=affiliate_id_str,
                            affiliate_name=affiliate_names.get(
                                affiliate_id_str, f"affiliate_{affiliate_id_str}"
                            ),
                            american_price=float(price),
                            updated_at=(
                                str(price_obj.get("updated_at"))
                                if price_obj.get("updated_at") is not None
                                else None
                            ),
                        )

            localized_timestamp = _localize_event_timestamp(event_date, timezone_name)
            for pair_group, book_quotes in grouped_quotes.items():
                counters["market_pairs_seen"] += 1
                consensus_probs: dict[str, list[float]] = {}
                best_quotes: dict[str, SideQuote] = {}
                books_used = 0
                latest_complete_quote_update: datetime | None = None

                for affiliate_id, side_map in book_quotes.items():
                    ordered_names = [name for name in participant_order if name in side_map]
                    if len(ordered_names) != 2:
                        continue
                    decimals = [side_map[name].decimal_odds for name in ordered_names]
                    fair_probs = normalize_implied_probabilities(decimals)
                    books_used += 1
                    for participant_name in ordered_names:
                        updated_at = _parse_quote_timestamp(side_map[participant_name].updated_at)
                        if updated_at is None:
                            continue
                        if latest_complete_quote_update is None or updated_at > latest_complete_quote_update:
                            latest_complete_quote_update = updated_at
                    for idx, participant_name in enumerate(ordered_names):
                        consensus_probs.setdefault(participant_name, []).append(fair_probs[idx])
                        existing = best_quotes.get(participant_name)
                        candidate = side_map[participant_name]
                        if existing is None or candidate.decimal_odds > existing.decimal_odds:
                            best_quotes[participant_name] = candidate

                if books_used < min_books:
                    counters["market_pairs_skipped_book_depth"] += 1
                    continue

                counters["market_pairs_with_consensus"] += 1
                pair_key_label = pair_group if pair_group != "moneyline" else ""
                for participant_name, probabilities in consensus_probs.items():
                    best_quote = best_quotes.get(participant_name)
                    if best_quote is None:
                        continue
                    stats = consensus_stats(probabilities)
                    mean_prob = stats["mean"]
                    robust_prob = stats["robust"]
                    prob_low = stats["low"]
                    prob_high = stats["high"]
                    prob_range = stats["range"]
                    stability = stats["stability"]
                    model_prob = round(stability_adjusted_probability(robust_prob, stability), 6)
                    raw_estimated_ev = round(expected_value_decimal(model_prob, best_quote.decimal_odds), 6)
                    confidence = round(
                        consensus_confidence(
                            stability=stability,
                            books_used=books_used,
                            min_books=min_books,
                        ),
                        6,
                    )
                    confidence_adjusted_ev = round(
                        confidence_adjusted_edge(raw_estimated_ev, confidence),
                        6,
                    )
                    decision_prob = round(
                        probability_from_expected_value_decimal(
                            confidence_adjusted_ev,
                            best_quote.decimal_odds,
                        ),
                        6,
                    )
                    decision_ev = round(expected_value_decimal(decision_prob, best_quote.decimal_odds), 6)
                    best_quote_updated_at = _parse_quote_timestamp(best_quote.updated_at)
                    best_quote_age_seconds: float | str = ""
                    stale_quote_penalty_value = 0.0
                    if latest_complete_quote_update is not None and best_quote_updated_at is not None:
                        age_seconds = max(
                            0.0,
                            (latest_complete_quote_update - best_quote_updated_at).total_seconds(),
                        )
                        best_quote_age_seconds = round(age_seconds, 3)
                        stale_quote_penalty_value = round(stale_quote_penalty(age_seconds), 6)
                    rank_score = consensus_rank_score(
                        base_edge=confidence_adjusted_ev,
                        stability=stability,
                        books_used=books_used,
                        min_books=min_books,
                        stale_quote_penalty_value=stale_quote_penalty_value,
                    )
                    rows.append(
                        {
                            "timestamp": localized_timestamp.isoformat(),
                            "event_id": (
                                f"{event.get('event_id')}|{market_name}|"
                                f"{participant_name}|{pair_group}"
                            ),
                            "selection": _selection_label(
                                market_name,
                                participant_name,
                                best_quote.line_value,
                            ),
                            "odds": round(best_quote.decimal_odds, 6),
                            "model_prob": model_prob,
                            "closing_odds": "",
                            "market": market_name,
                            "line_value": _format_numeric_string(best_quote.line_value),
                            "book": best_quote.affiliate_name,
                            "book_affiliate_id": best_quote.affiliate_id,
                            "best_price_american": int(best_quote.american_price),
                            "consensus_book_count": books_used,
                            "consensus_fair_prob": round(mean_prob, 6),
                            "consensus_robust_prob": round(robust_prob, 6),
                            "consensus_prob_low": round(prob_low, 6),
                            "consensus_prob_high": round(prob_high, 6),
                            "consensus_prob_range": round(prob_range, 6),
                            "consensus_prob_stddev": round(stats["stddev"], 6),
                            "consensus_stability": round(stability, 6),
                            "consensus_confidence": confidence,
                            "best_quote_updated_at": best_quote.updated_at or "",
                            "best_quote_age_seconds": best_quote_age_seconds,
                            "stale_quote_penalty": stale_quote_penalty_value,
                            "fair_decimal_odds": round(1.0 / model_prob, 6) if model_prob > 0 else "",
                            "decision_prob": decision_prob,
                            "decision_fair_decimal_odds": round(1.0 / decision_prob, 6) if decision_prob > 0 else "",
                            "estimated_ev": raw_estimated_ev,
                            "confidence_adjusted_ev": confidence_adjusted_ev,
                            "decision_ev": decision_ev,
                            "edge_rank_score": rank_score,
                            "event_date_utc": event_date,
                            "event_status": _event_status(event),
                            "sport_id": event.get("sport_id"),
                            "away_team": away_team,
                            "home_team": home_team,
                            "pair_key": pair_key_label,
                        }
                    )

    rows.sort(
        key=lambda row: (
            row["timestamp"],
            -float(row["edge_rank_score"]),
            row["event_id"],
            row["selection"],
        )
    )
    return rows, counters


def _write_candidate_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp",
        "event_id",
        "selection",
        "odds",
        "closing_odds",
        "model_prob",
        "market",
        "line_value",
        "book",
        "book_affiliate_id",
        "best_price_american",
        "consensus_book_count",
        "consensus_fair_prob",
        "consensus_robust_prob",
        "consensus_prob_low",
        "consensus_prob_high",
        "consensus_prob_range",
        "consensus_prob_stddev",
        "consensus_stability",
        "consensus_confidence",
        "best_quote_updated_at",
        "best_quote_age_seconds",
        "stale_quote_penalty",
        "fair_decimal_odds",
        "decision_prob",
        "decision_fair_decimal_odds",
        "estimated_ev",
        "confidence_adjusted_ev",
        "decision_ev",
        "edge_rank_score",
        "event_date_utc",
        "event_status",
        "sport_id",
        "away_team",
        "home_team",
        "pair_key",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _latest_live_candidates_summary_for_event(
    *,
    output_dir: Path,
    sport_id: int,
    event_date: str,
) -> tuple[dict[str, Any] | None, str]:
    pattern = f"live_candidates_summary_{sport_id}_{event_date}_*.json"
    candidates = sorted(
        output_dir.glob(pattern),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return dict(payload), str(path)
    return None, ""


def _run_provider_artifact_passthrough(
    *,
    provider: str,
    env_file: str,
    sport_id: int,
    event_date: str,
    output_dir: str,
) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = out_dir / f"live_candidates_summary_{sport_id}_{event_date}_{stamp}.json"
    source_payload, source_file = _latest_live_candidates_summary_for_event(
        output_dir=out_dir,
        sport_id=sport_id,
        event_date=event_date,
    )
    if source_payload is None:
        summary = {
            "env_file": env_file,
            "captured_at": datetime.now().isoformat(),
            "status": "error",
            "provider": provider,
            "data_source": "artifact_passthrough",
            "error": f"No live-candidates summary artifact found for ODDS_PROVIDER={provider!r}",
            "sport_id": sport_id,
            "event_date": event_date,
            "source_summary_file": "",
        }
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        summary["output_file"] = str(summary_path)
        return summary

    summary = {
        "env_file": env_file,
        "captured_at": datetime.now().isoformat(),
        "status": str(source_payload.get("status") or "ready"),
        "provider": provider,
        "data_source": "artifact_passthrough",
        "sport_id": sport_id,
        "event_date": event_date,
        "source_summary_file": source_file,
        "source_captured_at": source_payload.get("captured_at"),
        "market_ids": list(source_payload.get("market_ids") or []),
        "affiliate_ids": list(source_payload.get("affiliate_ids") or []),
        "affiliate_names": list(source_payload.get("affiliate_names") or []),
        "events_fetched": int(float(source_payload.get("events_fetched") or 0)),
        "market_pairs_seen": int(float(source_payload.get("market_pairs_seen") or 0)),
        "market_pairs_with_consensus": int(float(source_payload.get("market_pairs_with_consensus") or 0)),
        "candidates_written": int(float(source_payload.get("candidates_written") or 0)),
        "positive_ev_candidates": int(float(source_payload.get("positive_ev_candidates") or 0)),
        "positive_decision_ev_candidates": int(float(source_payload.get("positive_decision_ev_candidates") or 0)),
        "top_candidates": list(source_payload.get("top_candidates") or []),
        "output_csv": str(source_payload.get("output_csv") or ""),
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary


def run_live_candidates(
    *,
    env_file: str,
    sport_id: int,
    event_date: str,
    output_dir: str = "outputs",
    affiliate_ids: tuple[str, ...] = DEFAULT_THERUNDOWN_AFFILIATE_IDS,
    market_ids: tuple[int, ...] = (1, 2, 3),
    min_books: int = 2,
    offset_minutes: int = 300,
    include_in_play: bool = False,
    timeout_seconds: float = 15.0,
    http_get_json: HttpGetter = _http_get_json,
) -> dict[str, Any]:
    env_path = Path(env_file)
    data = _parse_env_file(env_path)
    provider = (data.get("ODDS_PROVIDER") or "therundown").strip().lower()
    if provider != "therundown":
        return _run_provider_artifact_passthrough(
            provider=provider or "unknown",
            env_file=str(env_path),
            sport_id=sport_id,
            event_date=event_date,
            output_dir=output_dir,
        )

    api_key = data.get("THERUNDOWN_API_KEY")
    base_url = data.get("THERUNDOWN_BASE_URL") or "https://therundown.io/api/v2"
    timezone_name = data.get("BETBOT_TIMEZONE") or "America/New_York"
    if _is_placeholder(api_key):
        raise ValueError("THERUNDOWN_API_KEY is missing")
    if min_books <= 0:
        raise ValueError("min_books must be positive")
    allowed_market_names = {SUPPORTED_MARKET_IDS[item] for item in market_ids if item in SUPPORTED_MARKET_IDS}
    if not allowed_market_names:
        raise ValueError(f"market_ids must include one or more of {sorted(SUPPORTED_MARKET_IDS)}")

    affiliate_name_map = _load_affiliate_names(
        base_url=base_url,
        api_key=api_key or "",
        timeout_seconds=timeout_seconds,
        http_get_json=http_get_json,
    )
    payload = _load_event_payload(
        base_url=base_url,
        api_key=api_key or "",
        sport_id=sport_id,
        event_date=event_date,
        market_ids=list(market_ids),
        affiliate_ids=list(affiliate_ids),
        offset_minutes=offset_minutes,
        timeout_seconds=timeout_seconds,
        http_get_json=http_get_json,
    )
    rows, counters = extract_candidate_rows(
        events=payload["events"],
        affiliate_names=affiliate_name_map,
        min_books=min_books,
        timezone_name=timezone_name,
        include_in_play=include_in_play,
        allowed_market_names=allowed_market_names,
    )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"live_candidates_{sport_id}_{event_date}_{stamp}.csv"
    json_path = out_dir / f"live_candidates_summary_{sport_id}_{event_date}_{stamp}.json"
    _write_candidate_csv(csv_path, rows)

    summary: dict[str, Any] = {
        "env_file": str(env_path),
        "captured_at": datetime.now().isoformat(),
        "status": "ready" if rows else "empty",
        "sport_id": sport_id,
        "event_date": event_date,
        "affiliate_ids": list(affiliate_ids),
        "affiliate_names": [affiliate_name_map.get(item, f"affiliate_{item}") for item in affiliate_ids],
        "market_ids": list(market_ids),
        "include_in_play": include_in_play,
        "min_books": min_books,
        "offset_minutes": offset_minutes,
        "events_fetched": counters["events_total"],
        "events_skipped_in_play": counters["events_skipped_in_play"],
        "market_pairs_seen": counters["market_pairs_seen"],
        "market_pairs_with_consensus": counters["market_pairs_with_consensus"],
        "market_pairs_skipped_book_depth": counters["market_pairs_skipped_book_depth"],
        "candidates_written": len(rows),
        "output_csv": str(csv_path),
    }
    market_counts: dict[str, int] = {}
    for row in rows:
        market = str(row["market"])
        market_counts[market] = market_counts.get(market, 0) + 1
    summary["candidates_by_market"] = market_counts
    positive_ev_rows = [row for row in rows if float(row["estimated_ev"]) > 0]
    summary["positive_ev_candidates"] = len(positive_ev_rows)
    positive_decision_ev_rows = [row for row in rows if float(row["decision_ev"]) > 0]
    summary["positive_decision_ev_candidates"] = len(positive_decision_ev_rows)
    ranked_rows = sorted(rows, key=lambda row: float(row["edge_rank_score"]), reverse=True)
    def _summary_candidate(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "selection": row["selection"],
            "market": row["market"],
            "book": row["book"],
            "odds": row["odds"],
            "model_prob": row["model_prob"],
            "decision_prob": row["decision_prob"],
            "estimated_ev": row["estimated_ev"],
            "confidence_adjusted_ev": row["confidence_adjusted_ev"],
            "decision_ev": row["decision_ev"],
            "edge_rank_score": row["edge_rank_score"],
            "consensus_book_count": row["consensus_book_count"],
            "consensus_stability": row["consensus_stability"],
            "consensus_confidence": row["consensus_confidence"],
            "consensus_prob_range": row["consensus_prob_range"],
            "best_quote_updated_at": row["best_quote_updated_at"],
            "best_quote_age_seconds": row["best_quote_age_seconds"],
            "stale_quote_penalty": row["stale_quote_penalty"],
            "timestamp": row["timestamp"],
        }

    summary["top_candidates"] = [_summary_candidate(row) for row in ranked_rows[:5]]
    if positive_ev_rows:
        best_positive_ev_row = max(positive_ev_rows, key=lambda row: float(row["estimated_ev"]))
        summary["top_positive_ev_candidate"] = _summary_candidate(best_positive_ev_row)
    if positive_decision_ev_rows:
        best_positive_decision_ev_row = max(positive_decision_ev_rows, key=lambda row: float(row["decision_ev"]))
        summary["top_positive_decision_ev_candidate"] = _summary_candidate(best_positive_decision_ev_row)
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(json_path)
    return summary
