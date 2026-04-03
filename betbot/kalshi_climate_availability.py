from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Callable
from urllib.parse import urlencode

from betbot.kalshi_book_math import derive_top_of_book
from betbot.kalshi_nonsports_priors import build_prior_rows, load_prior_rows
from betbot.kalshi_nonsports_quality import load_history_rows
from betbot.kalshi_ws_state import run_kalshi_ws_state_collect
from betbot.live_smoke import _http_get_json, kalshi_api_root_candidates
from betbot.onboarding import _parse_env_file
from betbot.runtime_version import build_runtime_version_block, detect_weather_model_tags, weather_priors_version


WsCollectRunner = Callable[..., dict[str, Any]]
MarketHttpGetter = Callable[[str, dict[str, str], float], tuple[int, Any]]


_CLIMATE_FAMILIES = {
    "daily_rain",
    "daily_temperature",
    "daily_snow",
    "monthly_climate_anomaly",
}

_CLIMATE_TICKER_PREFIXES = (
    "KXRAIN",
    "KXTEMP",
    "KXSNOW",
    "KXHMONTH",
    "KXHEAT",
    "KXCOOL",
    "KXWEATH",
)

_CLIMATE_TEXT_HINTS = (
    "climate",
    "weather",
    "temperature",
    "temp",
    "rain",
    "snow",
    "precip",
    "anomaly",
)


def default_climate_availability_db_path(output_dir: str) -> Path:
    return Path(output_dir) / "kalshi_climate_availability.sqlite3"


def _fetch_recently_updated_climate_market_tickers(
    *,
    env_file: str,
    as_of: datetime,
    min_updated_seconds: float,
    max_markets: int,
    timeout_seconds: float = 8.0,
    http_get_json: MarketHttpGetter = _http_get_json,
) -> dict[str, Any]:
    try:
        env_data = _parse_env_file(Path(env_file))
    except Exception as exc:
        return {
            "status": "env_parse_error",
            "reason": str(exc),
            "tickers": [],
            "tickers_count": 0,
        }

    env_name = str(env_data.get("KALSHI_ENV") or "prod").strip().lower() or "prod"
    try:
        api_roots = kalshi_api_root_candidates(env_name)
    except Exception as exc:
        return {
            "status": "invalid_kalshi_env",
            "reason": str(exc),
            "kalshi_env": env_name,
            "tickers": [],
            "tickers_count": 0,
        }

    cutoff_ts_ms = int(
        (as_of - timedelta(seconds=max(1.0, float(min_updated_seconds)))).astimezone(timezone.utc).timestamp() * 1000
    )
    limit = max(50, min(1000, int(max_markets) * 8))
    params = urlencode(
        [
            ("limit", str(limit)),
            ("min_updated_ts", str(cutoff_ts_ms)),
        ]
    )
    headers = {"User-Agent": "betbot-climate-router/1.0"}
    attempted_urls: list[str] = []
    errors: list[str] = []

    for api_root in api_roots:
        url = f"{api_root}/markets?{params}"
        attempted_urls.append(url)
        try:
            status, payload = http_get_json(url, headers, timeout_seconds)
        except Exception as exc:
            errors.append(f"{api_root}: {exc}")
            continue
        if status != 200 or not isinstance(payload, dict):
            errors.append(f"{api_root}: http_{status}")
            continue

        raw_markets = payload.get("markets")
        if not isinstance(raw_markets, list):
            raw_markets = payload.get("data")
        if not isinstance(raw_markets, list):
            errors.append(f"{api_root}: payload_missing_markets")
            continue

        tickers: list[str] = []
        for market in raw_markets:
            if not isinstance(market, dict):
                continue
            market_status = str(market.get("status") or market.get("market_status") or "").strip().lower()
            if market_status and market_status not in {"open", "active", "trading"}:
                continue
            ticker = _normalize_ticker(market.get("ticker") or market.get("market_ticker"))
            if not ticker or ticker in tickers:
                continue
            context_row = {
                "market_ticker": ticker,
                "category": market.get("category"),
                "market_title": market.get("title") or market.get("market_title"),
                "event_title": market.get("event_title") or market.get("event_ticker"),
                "event_sub_title": market.get("subtitle") or market.get("event_sub_title"),
                "rules_primary": market.get("rules_primary") or market.get("rules"),
            }
            if not _is_climate_market_context(context_row):
                continue
            tickers.append(ticker)
            if len(tickers) >= max(1, int(max_markets)):
                break

        return {
            "status": "ready",
            "reason": "recent_markets_loaded",
            "kalshi_env": env_name,
            "api_root": api_root,
            "http_status": status,
            "min_updated_ts_ms": cutoff_ts_ms,
            "tickers": tickers,
            "tickers_count": len(tickers),
            "attempted_urls": attempted_urls,
            "errors": errors,
        }

    return {
        "status": "upstream_error",
        "reason": "failed_to_fetch_recent_markets",
        "kalshi_env": env_name,
        "min_updated_ts_ms": cutoff_ts_ms,
        "tickers": [],
        "tickers_count": 0,
        "attempted_urls": attempted_urls,
        "errors": errors,
    }


def _to_iso_utc(value: datetime | None) -> str:
    dt = value or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y", "t", "on"}:
        return True
    if text in {"0", "false", "no", "n", "f", "off"}:
        return False
    return None


def _normalize_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper()
    if not ticker:
        return ""
    if not ticker.startswith("KX"):
        return ""
    return ticker


def _is_orderable_price(price: float | None) -> bool:
    return isinstance(price, float) and 0.0 < price < 1.0


def _is_endpoint_quote(price: float | None) -> bool:
    return isinstance(price, float) and (price <= 0.0 or price >= 1.0)


def _latest_market_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    grouped: dict[str, dict[str, str]] = {}
    grouped_ts: dict[str, datetime] = {}
    for row in history_rows:
        ticker = _normalize_ticker(row.get("market_ticker"))
        if not ticker:
            continue
        captured_at = _parse_ts(row.get("captured_at")) or datetime.min.replace(tzinfo=timezone.utc)
        previous_ts = grouped_ts.get(ticker)
        if previous_ts is None or captured_at >= previous_ts:
            grouped[ticker] = dict(row)
            grouped_ts[ticker] = captured_at
    return grouped


def _infer_contract_family(row: dict[str, Any]) -> str:
    family = str(row.get("contract_family") or "").strip().lower()
    if family in _CLIMATE_FAMILIES:
        return family

    merged = " ".join(
        [
            str(row.get("market_ticker") or ""),
            str(row.get("market_title") or ""),
            str(row.get("category") or ""),
            str(row.get("event_title") or ""),
            str(row.get("event_sub_title") or ""),
            str(row.get("rules_primary") or ""),
        ]
    ).lower()

    if "kxhmonth" in merged or "anomaly" in merged:
        return "monthly_climate_anomaly"
    if "rain" in merged or "precip" in merged:
        return "daily_rain"
    if "snow" in merged:
        return "daily_snow"
    if "temp" in merged or "temperature" in merged or "high" in merged or "low" in merged:
        return "daily_temperature"
    return ""


def _is_climate_market_context(row: dict[str, Any]) -> bool:
    ticker = _normalize_ticker(row.get("market_ticker"))
    if ticker and ticker.startswith(_CLIMATE_TICKER_PREFIXES):
        return True

    category_text = str(row.get("category") or "").strip().lower()
    if category_text and ("climate" in category_text or "weather" in category_text):
        return True

    merged = " ".join(
        [
            str(row.get("market_title") or ""),
            str(row.get("event_title") or ""),
            str(row.get("event_sub_title") or ""),
            str(row.get("rules_primary") or ""),
        ]
    ).lower()
    return any(token in merged for token in _CLIMATE_TEXT_HINTS)


def _derive_strip_key(row: dict[str, Any], *, contract_family: str) -> str:
    ticker = _normalize_ticker(row.get("market_ticker"))
    if not ticker:
        return ""

    if contract_family == "daily_temperature":
        # Typical Kalshi bucket suffixes include temperature bucket markers.
        ticker = re.sub(r"-(?:T|B|R|S|L|H)[A-Z0-9.+-]+$", "", ticker)
    elif contract_family in {"daily_rain", "daily_snow"}:
        ticker = re.sub(r"-(?:R|S|B|T|P)[A-Z0-9.+-]+$", "", ticker)

    close_time = str(row.get("close_time") or "").strip()
    if close_time:
        return f"{contract_family}|{ticker}|{close_time[:10]}"
    return f"{contract_family}|{ticker}"


def _parse_level_rows(value: Any) -> list[tuple[str, float]]:
    if not isinstance(value, list):
        return []
    levels: list[tuple[str, float]] = []
    for item in value:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        price = _parse_float(item[0])
        size = _parse_float(item[1])
        if price is None or size is None:
            continue
        if price > 1.0:
            price = price / 100.0
        price = max(0.0, min(1.0, price))
        if size <= 0:
            continue
        levels.append((f"{price:.4f}", size))
    return levels


def _levels_to_map(levels: list[tuple[str, float]]) -> dict[str, float]:
    mapped: dict[str, float] = {}
    for price_text, size in levels:
        mapped[price_text] = max(0.0, float(size))
    return mapped


def _sorted_levels(levels: dict[str, float]) -> list[list[str]]:
    return [
        [price_text, f"{float(size):.4f}"]
        for price_text, size in sorted(
            levels.items(),
            key=lambda item: _parse_float(item[0]) or 0.0,
            reverse=True,
        )
        if float(size) > 0.0
    ]


def _extract_event_type(event: dict[str, Any]) -> str:
    return str(
        event.get("event_type")
        or event.get("type")
        or event.get("channel")
        or ""
    ).strip().lower()


def _extract_event_ticker(event: dict[str, Any]) -> str:
    for key in ("market_ticker", "ticker"):
        ticker = _normalize_ticker(event.get(key))
        if ticker:
            return ticker
    payload = event.get("payload")
    if isinstance(payload, dict):
        for key in ("market_ticker", "ticker"):
            ticker = _normalize_ticker(payload.get(key))
            if ticker:
                return ticker
    return ""


def _extract_event_ts(event: dict[str, Any]) -> datetime:
    for key in ("captured_at_utc", "captured_at", "ts", "timestamp"):
        parsed = _parse_ts(event.get(key))
        if isinstance(parsed, datetime):
            return parsed
    payload = event.get("payload")
    if isinstance(payload, dict):
        for key in ("captured_at_utc", "captured_at", "ts", "timestamp"):
            parsed = _parse_ts(payload.get(key))
            if isinstance(parsed, datetime):
                return parsed
    return datetime.now(timezone.utc)


def _snapshot_levels_from_event(event: dict[str, Any]) -> tuple[dict[str, float] | None, dict[str, float] | None]:
    yes_candidates = (
        event.get("yes_dollars_fp"),
        event.get("yes_dollars"),
        event.get("yes"),
        ((event.get("orderbook_fp") or {}).get("yes_dollars") if isinstance(event.get("orderbook_fp"), dict) else None),
    )
    no_candidates = (
        event.get("no_dollars_fp"),
        event.get("no_dollars"),
        event.get("no"),
        ((event.get("orderbook_fp") or {}).get("no_dollars") if isinstance(event.get("orderbook_fp"), dict) else None),
    )
    yes_levels: dict[str, float] | None = None
    no_levels: dict[str, float] | None = None
    for candidate in yes_candidates:
        parsed = _parse_level_rows(candidate)
        if parsed:
            yes_levels = _levels_to_map(parsed)
            break
    for candidate in no_candidates:
        parsed = _parse_level_rows(candidate)
        if parsed:
            no_levels = _levels_to_map(parsed)
            break
    return yes_levels, no_levels


def _delta_levels_from_event(event: dict[str, Any], side: str) -> list[tuple[str, float]]:
    side_key = side.lower()
    candidates = (
        event.get(f"{side_key}_dollars_delta"),
        event.get(f"{side_key}_deltas"),
        event.get(f"{side_key}_updates"),
        event.get(side_key),
    )
    for candidate in candidates:
        parsed = _parse_level_rows(candidate)
        if parsed:
            return parsed
    return []


def _ensure_state_entry(entry: dict[str, Any] | None) -> dict[str, Any]:
    if isinstance(entry, dict):
        state = dict(entry)
    else:
        state = {}
    state.setdefault("first_observed_at_utc", "")
    state.setdefault("last_observed_at_utc", "")
    state.setdefault("last_orderable_at_utc", "")
    state.setdefault("last_non_endpoint_at_utc", "")
    state.setdefault("last_wakeup_at_utc", "")
    state.setdefault("first_wakeup_at_utc", "")
    state.setdefault("last_trade_at_utc", "")
    state.setdefault("observations_total", 0)
    state.setdefault("endpoint_only_observations", 0)
    state.setdefault("non_endpoint_observations", 0)
    state.setdefault("orderable_side_observations", 0)
    state.setdefault("two_sided_observations", 0)
    state.setdefault("wakeup_count", 0)
    state.setdefault("public_trade_events", 0)
    state.setdefault("public_trade_contracts", 0.0)
    state.setdefault("orderable_minutes_observed", 0.0)
    state.setdefault("non_endpoint_minutes_observed", 0.0)
    state.setdefault("max_endpoint_only_streak_count", 0)
    state.setdefault("current_endpoint_only_streak_count", 0)
    state.setdefault("last_has_orderable_side", False)
    state.setdefault("last_has_non_endpoint_quote", False)
    state.setdefault("last_endpoint_only", False)
    state.setdefault("last_yes_bid_dollars", None)
    state.setdefault("last_yes_ask_dollars", None)
    state.setdefault("last_no_bid_dollars", None)
    state.setdefault("last_no_ask_dollars", None)
    state.setdefault("last_spread_dollars", None)
    state.setdefault("contract_family", "")
    state.setdefault("strip_key", "")
    return state


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_climate_availability_schema(path: str | Path) -> Path:
    db_path = Path(path)
    conn = _connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS climate_ticker_state (
                market_ticker TEXT PRIMARY KEY,
                contract_family TEXT,
                strip_key TEXT,
                first_observed_at_utc TEXT,
                last_observed_at_utc TEXT,
                last_orderable_at_utc TEXT,
                last_non_endpoint_at_utc TEXT,
                last_wakeup_at_utc TEXT,
                first_wakeup_at_utc TEXT,
                last_trade_at_utc TEXT,
                observations_total INTEGER NOT NULL DEFAULT 0,
                endpoint_only_observations INTEGER NOT NULL DEFAULT 0,
                non_endpoint_observations INTEGER NOT NULL DEFAULT 0,
                orderable_side_observations INTEGER NOT NULL DEFAULT 0,
                two_sided_observations INTEGER NOT NULL DEFAULT 0,
                wakeup_count INTEGER NOT NULL DEFAULT 0,
                public_trade_events INTEGER NOT NULL DEFAULT 0,
                public_trade_contracts REAL NOT NULL DEFAULT 0,
                orderable_minutes_observed REAL NOT NULL DEFAULT 0,
                non_endpoint_minutes_observed REAL NOT NULL DEFAULT 0,
                max_endpoint_only_streak_count INTEGER NOT NULL DEFAULT 0,
                current_endpoint_only_streak_count INTEGER NOT NULL DEFAULT 0,
                last_has_orderable_side INTEGER NOT NULL DEFAULT 0,
                last_has_non_endpoint_quote INTEGER NOT NULL DEFAULT 0,
                last_endpoint_only INTEGER NOT NULL DEFAULT 0,
                last_yes_bid_dollars REAL,
                last_yes_ask_dollars REAL,
                last_no_bid_dollars REAL,
                last_no_ask_dollars REAL,
                last_spread_dollars REAL,
                updated_at_utc TEXT
            );

            CREATE TABLE IF NOT EXISTS climate_ticker_observations (
                observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                observed_at_utc TEXT NOT NULL,
                market_ticker TEXT NOT NULL,
                contract_family TEXT,
                strip_key TEXT,
                event_type TEXT,
                yes_bid_dollars REAL,
                yes_ask_dollars REAL,
                no_bid_dollars REAL,
                no_ask_dollars REAL,
                spread_dollars REAL,
                has_quotes INTEGER,
                has_orderable_side INTEGER,
                non_endpoint_quote INTEGER,
                endpoint_only INTEGER,
                two_sided_book INTEGER,
                wakeup_transition INTEGER,
                public_trade_event INTEGER,
                public_trade_contracts REAL,
                updated_at_utc TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_climate_obs_ticker_ts
                ON climate_ticker_observations (market_ticker, observed_at_utc);
            CREATE INDEX IF NOT EXISTS idx_climate_obs_run_id
                ON climate_ticker_observations (run_id, observed_at_utc);
            CREATE INDEX IF NOT EXISTS idx_climate_obs_event_type
                ON climate_ticker_observations (event_type, observed_at_utc);
            CREATE INDEX IF NOT EXISTS idx_climate_state_updated
                ON climate_ticker_state (updated_at_utc);
            """
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _load_ticker_states(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute("SELECT * FROM climate_ticker_state").fetchall()
    loaded: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = {key: row[key] for key in row.keys()}
        loaded[str(payload.get("market_ticker") or "").strip().upper()] = payload
    return loaded


def _load_lookback_metrics(
    conn: sqlite3.Connection,
    *,
    lookback_days: float,
    as_of: datetime,
) -> dict[str, dict[str, Any]]:
    cutoff = as_of - timedelta(days=max(0.0, float(lookback_days)))
    rows = conn.execute(
        """
        SELECT
            market_ticker,
            COUNT(*) AS observations_total,
            SUM(CASE WHEN endpoint_only = 1 THEN 1 ELSE 0 END) AS endpoint_only_observations,
            SUM(CASE WHEN non_endpoint_quote = 1 THEN 1 ELSE 0 END) AS non_endpoint_observations,
            SUM(CASE WHEN has_orderable_side = 1 THEN 1 ELSE 0 END) AS orderable_side_observations,
            SUM(CASE WHEN wakeup_transition = 1 THEN 1 ELSE 0 END) AS wakeup_count,
            SUM(CASE WHEN public_trade_event = 1 THEN 1 ELSE 0 END) AS public_trade_events,
            SUM(CASE WHEN public_trade_event = 1 THEN COALESCE(public_trade_contracts, 0) ELSE 0 END)
                AS public_trade_contracts,
            MAX(CASE WHEN has_orderable_side = 1 THEN observed_at_utc ELSE NULL END) AS last_orderable_at_utc,
            MAX(CASE WHEN non_endpoint_quote = 1 THEN observed_at_utc ELSE NULL END) AS last_non_endpoint_at_utc,
            MAX(CASE WHEN public_trade_event = 1 THEN observed_at_utc ELSE NULL END) AS last_trade_at_utc,
            MAX(observed_at_utc) AS last_observed_at_utc
        FROM climate_ticker_observations
        WHERE observed_at_utc >= ?
        GROUP BY market_ticker
        """,
        (cutoff.isoformat(),),
    ).fetchall()
    metrics: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        ticker = _normalize_ticker(item.get("market_ticker"))
        if not ticker:
            continue
        metrics[ticker] = item
    return metrics


def _write_observation_rows(
    conn: sqlite3.Connection,
    *,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0
    written = 0
    for row in rows:
        conn.execute(
            """
            INSERT INTO climate_ticker_observations (
                run_id,
                observed_at_utc,
                market_ticker,
                contract_family,
                strip_key,
                event_type,
                yes_bid_dollars,
                yes_ask_dollars,
                no_bid_dollars,
                no_ask_dollars,
                spread_dollars,
                has_quotes,
                has_orderable_side,
                non_endpoint_quote,
                endpoint_only,
                two_sided_book,
                wakeup_transition,
                public_trade_event,
                public_trade_contracts,
                updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row.get("run_id"),
                row.get("observed_at_utc"),
                row.get("market_ticker"),
                row.get("contract_family"),
                row.get("strip_key"),
                row.get("event_type"),
                row.get("yes_bid_dollars"),
                row.get("yes_ask_dollars"),
                row.get("no_bid_dollars"),
                row.get("no_ask_dollars"),
                row.get("spread_dollars"),
                int(bool(row.get("has_quotes"))),
                int(bool(row.get("has_orderable_side"))),
                int(bool(row.get("non_endpoint_quote"))),
                int(bool(row.get("endpoint_only"))),
                int(bool(row.get("two_sided_book"))),
                int(bool(row.get("wakeup_transition"))),
                int(bool(row.get("public_trade_event"))),
                row.get("public_trade_contracts"),
                row.get("updated_at_utc"),
            ),
        )
        written += 1
    return written


def _upsert_ticker_states(conn: sqlite3.Connection, states: dict[str, dict[str, Any]]) -> int:
    updated = 0
    for ticker, payload in states.items():
        conn.execute(
            """
            INSERT INTO climate_ticker_state (
                market_ticker,
                contract_family,
                strip_key,
                first_observed_at_utc,
                last_observed_at_utc,
                last_orderable_at_utc,
                last_non_endpoint_at_utc,
                last_wakeup_at_utc,
                first_wakeup_at_utc,
                last_trade_at_utc,
                observations_total,
                endpoint_only_observations,
                non_endpoint_observations,
                orderable_side_observations,
                two_sided_observations,
                wakeup_count,
                public_trade_events,
                public_trade_contracts,
                orderable_minutes_observed,
                non_endpoint_minutes_observed,
                max_endpoint_only_streak_count,
                current_endpoint_only_streak_count,
                last_has_orderable_side,
                last_has_non_endpoint_quote,
                last_endpoint_only,
                last_yes_bid_dollars,
                last_yes_ask_dollars,
                last_no_bid_dollars,
                last_no_ask_dollars,
                last_spread_dollars,
                updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market_ticker) DO UPDATE SET
                contract_family=excluded.contract_family,
                strip_key=excluded.strip_key,
                first_observed_at_utc=excluded.first_observed_at_utc,
                last_observed_at_utc=excluded.last_observed_at_utc,
                last_orderable_at_utc=excluded.last_orderable_at_utc,
                last_non_endpoint_at_utc=excluded.last_non_endpoint_at_utc,
                last_wakeup_at_utc=excluded.last_wakeup_at_utc,
                first_wakeup_at_utc=excluded.first_wakeup_at_utc,
                last_trade_at_utc=excluded.last_trade_at_utc,
                observations_total=excluded.observations_total,
                endpoint_only_observations=excluded.endpoint_only_observations,
                non_endpoint_observations=excluded.non_endpoint_observations,
                orderable_side_observations=excluded.orderable_side_observations,
                two_sided_observations=excluded.two_sided_observations,
                wakeup_count=excluded.wakeup_count,
                public_trade_events=excluded.public_trade_events,
                public_trade_contracts=excluded.public_trade_contracts,
                orderable_minutes_observed=excluded.orderable_minutes_observed,
                non_endpoint_minutes_observed=excluded.non_endpoint_minutes_observed,
                max_endpoint_only_streak_count=excluded.max_endpoint_only_streak_count,
                current_endpoint_only_streak_count=excluded.current_endpoint_only_streak_count,
                last_has_orderable_side=excluded.last_has_orderable_side,
                last_has_non_endpoint_quote=excluded.last_has_non_endpoint_quote,
                last_endpoint_only=excluded.last_endpoint_only,
                last_yes_bid_dollars=excluded.last_yes_bid_dollars,
                last_yes_ask_dollars=excluded.last_yes_ask_dollars,
                last_no_bid_dollars=excluded.last_no_bid_dollars,
                last_no_ask_dollars=excluded.last_no_ask_dollars,
                last_spread_dollars=excluded.last_spread_dollars,
                updated_at_utc=excluded.updated_at_utc
            """,
            (
                ticker,
                payload.get("contract_family"),
                payload.get("strip_key"),
                payload.get("first_observed_at_utc"),
                payload.get("last_observed_at_utc"),
                payload.get("last_orderable_at_utc"),
                payload.get("last_non_endpoint_at_utc"),
                payload.get("last_wakeup_at_utc"),
                payload.get("first_wakeup_at_utc"),
                payload.get("last_trade_at_utc"),
                int(payload.get("observations_total") or 0),
                int(payload.get("endpoint_only_observations") or 0),
                int(payload.get("non_endpoint_observations") or 0),
                int(payload.get("orderable_side_observations") or 0),
                int(payload.get("two_sided_observations") or 0),
                int(payload.get("wakeup_count") or 0),
                int(payload.get("public_trade_events") or 0),
                float(payload.get("public_trade_contracts") or 0.0),
                float(payload.get("orderable_minutes_observed") or 0.0),
                float(payload.get("non_endpoint_minutes_observed") or 0.0),
                int(payload.get("max_endpoint_only_streak_count") or 0),
                int(payload.get("current_endpoint_only_streak_count") or 0),
                int(bool(payload.get("last_has_orderable_side"))),
                int(bool(payload.get("last_has_non_endpoint_quote"))),
                int(bool(payload.get("last_endpoint_only"))),
                payload.get("last_yes_bid_dollars"),
                payload.get("last_yes_ask_dollars"),
                payload.get("last_no_bid_dollars"),
                payload.get("last_no_ask_dollars"),
                payload.get("last_spread_dollars"),
                payload.get("updated_at_utc"),
            ),
        )
        updated += 1
    return updated


def _build_climate_enriched_rows(
    *,
    priors_csv: str,
    history_csv: str,
    include_families: set[str] | None,
) -> list[dict[str, Any]]:
    prior_rows = load_prior_rows(Path(priors_csv))
    history_rows = load_history_rows(Path(history_csv))
    enriched = build_prior_rows(
        prior_rows=prior_rows,
        latest_market_rows=_latest_market_rows(history_rows),
    )

    selected: list[dict[str, Any]] = []
    for row in enriched:
        item = dict(row)
        if not _is_climate_market_context(item):
            continue
        family = _infer_contract_family(item)
        if not family:
            continue
        if include_families and family not in include_families:
            continue
        item["contract_family"] = family
        item["strip_key"] = _derive_strip_key(item, contract_family=family)
        selected.append(item)

    selected.sort(
        key=lambda row: (
            _parse_float(row.get("best_entry_edge_net_fees")) or -999.0,
            _parse_float(row.get("best_maker_entry_edge_net_fees")) or -999.0,
            _parse_float(row.get("hours_to_close")) if _parse_float(row.get("hours_to_close")) is not None else 1e9,
        ),
        reverse=True,
    )
    return selected


def _build_ticker_metadata(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = _normalize_ticker(row.get("market_ticker"))
        if not ticker:
            continue
        metadata[ticker] = {
            "market_ticker": ticker,
            "contract_family": str(row.get("contract_family") or "").strip().lower(),
            "strip_key": str(row.get("strip_key") or "").strip(),
            "market_title": str(row.get("market_title") or "").strip(),
            "close_time": str(row.get("close_time") or "").strip(),
            "hours_to_close": _parse_float(row.get("hours_to_close")),
        }
    return metadata


def _merge_book_top(yes_levels: dict[str, float], no_levels: dict[str, float]) -> dict[str, Any]:
    return derive_top_of_book(
        {
            "yes_dollars": _sorted_levels(yes_levels),
            "no_dollars": _sorted_levels(no_levels),
        }
    )


def _extract_public_trade_contracts(event: dict[str, Any]) -> float:
    for key in (
        "count",
        "contracts",
        "size",
        "quantity",
        "volume",
        "delta_fp",
        "count_fp",
    ):
        parsed = _parse_float(event.get(key))
        if isinstance(parsed, float):
            return abs(parsed)
    payload = event.get("payload")
    if isinstance(payload, dict):
        for key in (
            "count",
            "contracts",
            "size",
            "quantity",
            "volume",
            "delta_fp",
            "count_fp",
        ):
            parsed = _parse_float(payload.get(key))
            if isinstance(parsed, float):
                return abs(parsed)
    return 0.0


def _to_price_dollars(value: Any) -> float | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    if parsed > 1.0:
        parsed = parsed / 100.0
    return max(0.0, min(1.0, parsed))


def _event_value(event: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in event and event.get(key) is not None:
            return event.get(key)
    payload = event.get("payload")
    if isinstance(payload, dict):
        for key in keys:
            if key in payload and payload.get(key) is not None:
                return payload.get(key)
    return None


def _extract_ticker_quote_snapshot(event: dict[str, Any]) -> dict[str, float | None]:
    yes_bid = _to_price_dollars(
        _event_value(
            event,
            "yes_bid_dollars",
            "best_yes_bid_dollars",
            "yes_bid",
            "best_yes_bid",
            "yesBid",
        )
    )
    yes_ask = _to_price_dollars(
        _event_value(
            event,
            "yes_ask_dollars",
            "best_yes_ask_dollars",
            "yes_ask",
            "best_yes_ask",
            "yesAsk",
        )
    )
    no_bid = _to_price_dollars(
        _event_value(
            event,
            "no_bid_dollars",
            "best_no_bid_dollars",
            "no_bid",
            "best_no_bid",
            "noBid",
        )
    )
    no_ask = _to_price_dollars(
        _event_value(
            event,
            "no_ask_dollars",
            "best_no_ask_dollars",
            "no_ask",
            "best_no_ask",
            "noAsk",
        )
    )
    spread = _to_price_dollars(
        _event_value(
            event,
            "spread_dollars",
            "yes_spread_dollars",
            "spread",
            "yes_spread",
        )
    )

    if spread is None and isinstance(yes_bid, float) and isinstance(yes_ask, float):
        spread = max(0.0, round(yes_ask - yes_bid, 6))

    return {
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "spread": spread,
    }


def _classify_availability_state(
    *,
    state: dict[str, Any],
    lookback: dict[str, Any] | None,
    as_of: datetime,
    recent_seconds: float,
    hot_trade_window_seconds: float,
) -> tuple[str, dict[str, Any]]:
    last_observed = _parse_ts((lookback or {}).get("last_observed_at_utc") or state.get("last_observed_at_utc"))
    last_orderable = _parse_ts((lookback or {}).get("last_orderable_at_utc") or state.get("last_orderable_at_utc"))
    last_non_endpoint = _parse_ts((lookback or {}).get("last_non_endpoint_at_utc") or state.get("last_non_endpoint_at_utc"))
    last_trade = _parse_ts((lookback or {}).get("last_trade_at_utc") or state.get("last_trade_at_utc"))

    def _age_seconds(ts: datetime | None) -> float | None:
        if ts is None:
            return None
        return max(0.0, (as_of - ts).total_seconds())

    obs_age = _age_seconds(last_observed)
    orderable_age = _age_seconds(last_orderable)
    non_endpoint_age = _age_seconds(last_non_endpoint)
    trade_age = _age_seconds(last_trade)

    orderable_recent = isinstance(orderable_age, float) and orderable_age <= recent_seconds
    non_endpoint_recent = isinstance(non_endpoint_age, float) and non_endpoint_age <= recent_seconds
    trade_recent = isinstance(trade_age, float) and trade_age <= hot_trade_window_seconds

    if orderable_recent and non_endpoint_recent and trade_recent:
        state_name = "hot"
    elif orderable_recent:
        state_name = "tradable"
    elif non_endpoint_recent:
        state_name = "priced_watch_only"
    else:
        state_name = "dead"

    diagnostics = {
        "last_observed_age_seconds": obs_age,
        "last_orderable_age_seconds": orderable_age,
        "last_non_endpoint_age_seconds": non_endpoint_age,
        "last_trade_age_seconds": trade_age,
        "orderable_recent": orderable_recent,
        "non_endpoint_recent": non_endpoint_recent,
        "trade_recent": trade_recent,
    }
    return state_name, diagnostics


def _reference_candidates(row: dict[str, Any]) -> list[dict[str, Any]]:
    fair_yes = _parse_float(row.get("fair_yes_probability"))
    fair_no = _parse_float(row.get("fair_no_probability"))

    def _candidate(*, side: str, source: str, price_key: str, edge_key: str, fallback_edge: float | None) -> dict[str, Any] | None:
        price = _to_price_dollars(row.get(price_key))
        if not isinstance(price, float):
            return None
        edge = _parse_float(row.get(edge_key))
        if edge is None:
            edge = fallback_edge
        return {
            "side": side,
            "reference_source": source,
            "reference_price": price,
            "reference_usable": _is_orderable_price(price),
            "reference_endpoint": _is_endpoint_quote(price),
            "edge_net": edge,
        }

    candidates: list[dict[str, Any]] = []

    yes_fallback = (fair_yes - _to_price_dollars(row.get("latest_yes_ask_dollars"))) if fair_yes is not None and _to_price_dollars(row.get("latest_yes_ask_dollars")) is not None else None
    no_fallback = (fair_no - _to_price_dollars(row.get("latest_no_ask_dollars"))) if fair_no is not None and _to_price_dollars(row.get("latest_no_ask_dollars")) is not None else None
    yes_bid_fallback = (fair_yes - _to_price_dollars(row.get("latest_yes_bid_dollars"))) if fair_yes is not None and _to_price_dollars(row.get("latest_yes_bid_dollars")) is not None else None
    no_bid_fallback = (fair_no - _to_price_dollars(row.get("latest_no_bid_dollars"))) if fair_no is not None and _to_price_dollars(row.get("latest_no_bid_dollars")) is not None else None

    for item in (
        _candidate(
            side="yes",
            source="displayed_yes_ask",
            price_key="latest_yes_ask_dollars",
            edge_key="edge_to_yes_ask_net_fees",
            fallback_edge=yes_fallback,
        ),
        _candidate(
            side="no",
            source="displayed_no_ask",
            price_key="latest_no_ask_dollars",
            edge_key="edge_to_no_ask_net_fees",
            fallback_edge=no_fallback,
        ),
        _candidate(
            side="yes",
            source="displayed_yes_bid",
            price_key="latest_yes_bid_dollars",
            edge_key="edge_to_yes_bid_net_fees",
            fallback_edge=yes_bid_fallback,
        ),
        _candidate(
            side="no",
            source="displayed_no_bid",
            price_key="latest_no_bid_dollars",
            edge_key="edge_to_no_bid_net_fees",
            fallback_edge=no_bid_fallback,
        ),
    ):
        if isinstance(item, dict):
            candidates.append(item)

    mid_yes = _to_price_dollars(row.get("market_mid_probability"))
    mid_no = _to_price_dollars(row.get("market_no_mid_probability"))
    if isinstance(mid_yes, float):
        mid_edge_yes = _parse_float(row.get("edge_to_mid"))
        if mid_edge_yes is None and fair_yes is not None:
            mid_edge_yes = fair_yes - mid_yes
        candidates.append(
            {
                "side": "yes",
                "reference_source": "strip_midpoint",
                "reference_price": mid_yes,
                "reference_usable": _is_orderable_price(mid_yes),
                "reference_endpoint": _is_endpoint_quote(mid_yes),
                "edge_net": mid_edge_yes,
            }
        )
    if isinstance(mid_no, float):
        mid_edge_no = _parse_float(row.get("edge_to_no_mid"))
        if mid_edge_no is None and fair_no is not None:
            mid_edge_no = fair_no - mid_no
        candidates.append(
            {
                "side": "no",
                "reference_source": "strip_no_midpoint",
                "reference_price": mid_no,
                "reference_usable": _is_orderable_price(mid_no),
                "reference_endpoint": _is_endpoint_quote(mid_no),
                "edge_net": mid_edge_no,
            }
        )

    candidates.sort(
        key=lambda item: (
            _parse_float(item.get("edge_net")) or -999.0,
            1 if bool(item.get("reference_usable")) else 0,
        ),
        reverse=True,
    )
    return candidates


def _allocate_tradable_capital(
    *,
    rows: list[dict[str, Any]],
    daily_risk_cap_dollars: float,
    max_risk_per_bet_dollars: float,
) -> dict[str, Any]:
    eligible = [
        row for row in rows
        if str(row.get("opportunity_class") or "").strip() in {"tradable_positive", "hot_positive"}
        and isinstance(_parse_float(row.get("theoretical_edge_net")), float)
        and (_parse_float(row.get("theoretical_edge_net")) or 0.0) > 0
    ]
    if not eligible:
        return {
            "eligible_rows": 0,
            "allocated_rows": 0,
            "total_risk_dollars": 0.0,
            "total_expected_value_dollars": 0.0,
            "allocations": [],
            "family_routed_capital_budget": {},
        }

    total_score = 0.0
    scored: list[tuple[dict[str, Any], float]] = []
    for row in eligible:
        edge = max(0.0, float(_parse_float(row.get("theoretical_edge_net")) or 0.0))
        state = str(row.get("availability_state") or "").strip().lower()
        state_multiplier = 1.2 if state == "hot" else 1.0
        score = edge * state_multiplier
        if score <= 0:
            continue
        total_score += score
        scored.append((row, score))

    if total_score <= 0 or not scored:
        return {
            "eligible_rows": len(eligible),
            "allocated_rows": 0,
            "total_risk_dollars": 0.0,
            "total_expected_value_dollars": 0.0,
            "allocations": [],
            "family_routed_capital_budget": {},
        }

    risk_cap = max(0.0, float(daily_risk_cap_dollars))
    max_per_bet = max(0.01, float(max_risk_per_bet_dollars))
    remaining = risk_cap
    allocations: list[dict[str, Any]] = []
    family_budget: dict[str, float] = {}
    total_ev = 0.0

    for row, score in sorted(scored, key=lambda item: item[1], reverse=True):
        if remaining <= 0:
            break
        proportional = (score / total_score) * risk_cap
        risk = min(max_per_bet, remaining, proportional)
        if risk <= 0:
            continue
        reference_price = _to_price_dollars(row.get("theoretical_reference_price")) or 1.0
        contracts = max(1, int(round(risk / max(reference_price, 0.01))))
        edge = float(_parse_float(row.get("theoretical_edge_net")) or 0.0)
        ev = round(edge * contracts, 6)
        allocation = {
            "market_ticker": row.get("market_ticker"),
            "contract_family": row.get("contract_family"),
            "strip_key": row.get("strip_key"),
            "side": row.get("theoretical_side"),
            "availability_state": row.get("availability_state"),
            "risk_dollars": round(risk, 6),
            "contracts": contracts,
            "reference_price_dollars": reference_price,
            "expected_value_dollars": ev,
            "edge_net": edge,
        }
        allocations.append(allocation)
        remaining = round(max(0.0, remaining - risk), 6)
        total_ev += ev
        family = str(row.get("contract_family") or "unknown").strip().lower() or "unknown"
        family_budget[family] = round(family_budget.get(family, 0.0) + risk, 6)

    total_risk = round(sum(float(item.get("risk_dollars") or 0.0) for item in allocations), 6)
    return {
        "eligible_rows": len(eligible),
        "allocated_rows": len(allocations),
        "total_risk_dollars": total_risk,
        "total_expected_value_dollars": round(total_ev, 6),
        "allocations": allocations,
        "family_routed_capital_budget": family_budget,
    }


def _summarize_strip_availability(
    *,
    rows: list[dict[str, Any]],
    ticker_state: dict[str, dict[str, Any]],
    lookback_metrics: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    strips: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        strip_key = str(row.get("strip_key") or "").strip()
        if not strip_key:
            continue
        strips.setdefault(strip_key, []).append(row)

    summaries: list[dict[str, Any]] = []
    for strip_key, strip_rows in strips.items():
        ticker_items: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for row in strip_rows:
            ticker = _normalize_ticker(row.get("market_ticker"))
            if not ticker:
                continue
            state = dict(ticker_state.get(ticker) or {})
            lookback = dict(lookback_metrics.get(ticker) or {})
            ticker_items.append((state, lookback))
        if not ticker_items:
            continue

        observations_total = sum(int(item[1].get("observations_total") or 0) for item in ticker_items)
        endpoint_only = sum(int(item[1].get("endpoint_only_observations") or 0) for item in ticker_items)
        non_endpoint = sum(int(item[1].get("non_endpoint_observations") or 0) for item in ticker_items)
        orderable = sum(int(item[1].get("orderable_side_observations") or 0) for item in ticker_items)
        wakeups = sum(int(item[1].get("wakeup_count") or 0) for item in ticker_items)
        orderable_minutes = sum(float(item[0].get("orderable_minutes_observed") or 0.0) for item in ticker_items)

        first_wakeup_age_minutes: float | None = None
        for state, _ in ticker_items:
            first_wakeup = _parse_ts(state.get("first_wakeup_at_utc"))
            close_time = _parse_ts(next((row.get("close_time") for row in strip_rows if _normalize_ticker(row.get("market_ticker")) == _normalize_ticker(state.get("market_ticker"))), ""))
            if not isinstance(first_wakeup, datetime) or not isinstance(close_time, datetime):
                continue
            minutes_to_close = (close_time - first_wakeup).total_seconds() / 60.0
            if first_wakeup_age_minutes is None or minutes_to_close < first_wakeup_age_minutes:
                first_wakeup_age_minutes = minutes_to_close

        strip_rows_sorted = sorted(
            strip_rows,
            key=lambda row: _parse_float(row.get("theoretical_edge_net")) or -999.0,
            reverse=True,
        )
        top_row = strip_rows_sorted[0]
        summaries.append(
            {
                "strip_key": strip_key,
                "strip_row_count": len(strip_rows),
                "top_market_ticker": top_row.get("market_ticker"),
                "top_contract_family": top_row.get("contract_family"),
                "top_theoretical_edge_net": _parse_float(top_row.get("theoretical_edge_net")),
                "strip_endpoint_only_rate": (
                    round(endpoint_only / observations_total, 6)
                    if observations_total > 0
                    else None
                ),
                "strip_non_endpoint_rate": (
                    round(non_endpoint / observations_total, 6)
                    if observations_total > 0
                    else None
                ),
                "strip_wakeup_rate": (
                    round(wakeups / observations_total, 6)
                    if observations_total > 0
                    else None
                ),
                "strip_avg_minutes_orderable": round(orderable_minutes / max(1, len(ticker_items)), 6),
                "strip_first_wakeup_minutes_to_close": (
                    round(first_wakeup_age_minutes, 3)
                    if isinstance(first_wakeup_age_minutes, float)
                    else None
                ),
                "strip_priced_watch_only_rate": (
                    round(
                        sum(
                            1
                            for row in strip_rows
                            if str(row.get("opportunity_class") or "").strip() == "priced_watch_only"
                        ) / len(strip_rows),
                        6,
                    )
                    if strip_rows
                    else None
                ),
                "strip_tradable_positive_rate": (
                    round(
                        sum(
                            1
                            for row in strip_rows
                            if str(row.get("opportunity_class") or "").strip() in {"tradable_positive", "hot_positive"}
                        ) / len(strip_rows),
                        6,
                    )
                    if strip_rows
                    else None
                ),
                "strip_observations_total": observations_total,
                "strip_orderable_observations": orderable,
                "strip_non_endpoint_observations": non_endpoint,
                "strip_wakeup_count": wakeups,
            }
        )

    summaries.sort(
        key=lambda row: (
            _parse_float(row.get("strip_wakeup_rate")) or 0.0,
            _parse_float(row.get("strip_non_endpoint_rate")) or 0.0,
            _parse_float(row.get("strip_avg_minutes_orderable")) or 0.0,
            _parse_float(row.get("top_theoretical_edge_net")) or -999.0,
        ),
        reverse=True,
    )
    return summaries


def _write_rows_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_climate_realtime_router(
    *,
    env_file: str = "data/research/account_onboarding.env.template",
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    availability_db_path: str | None = None,
    market_tickers: tuple[str, ...] = (),
    ws_channels: tuple[str, ...] = (
        "orderbook_snapshot",
        "orderbook_delta",
        "ticker",
        "public_trades",
        "user_fills",
        "market_positions",
    ),
    run_seconds: float = 45.0,
    max_markets: int = 40,
    seed_recent_markets: bool = True,
    recent_markets_min_updated_seconds: float = 900.0,
    recent_markets_timeout_seconds: float = 8.0,
    ws_state_max_age_seconds: float = 30.0,
    min_theoretical_edge_net_fees: float = 0.005,
    max_quote_age_seconds: float = 900.0,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    max_risk_per_bet_dollars: float = 1.0,
    availability_lookback_days: float = 7.0,
    availability_recent_seconds: float = 900.0,
    availability_hot_trade_window_seconds: float = 300.0,
    include_contract_families: tuple[str, ...] = ("daily_rain", "daily_temperature", "daily_snow", "monthly_climate_anomaly"),
    ws_collect_runner: WsCollectRunner = run_kalshi_ws_state_collect,
    market_http_get_json: MarketHttpGetter = _http_get_json,
    now: datetime | None = None,
    skip_realtime_collect: bool = False,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    run_id = f"kalshi_climate_realtime_router::{captured_at.strftime('%Y%m%d_%H%M%S_%f')[:-3]}"
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    include_families = {
        str(value).strip().lower()
        for value in include_contract_families
        if str(value).strip()
    }
    climate_rows = _build_climate_enriched_rows(
        priors_csv=priors_csv,
        history_csv=history_csv,
        include_families=include_families or None,
    )
    ticker_metadata = _build_ticker_metadata(climate_rows)

    explicit_tickers = tuple(
        dict.fromkeys(
            ticker
            for ticker in (_normalize_ticker(item) for item in market_tickers)
            if ticker
        )
    )
    recent_market_discovery = {
        "status": "disabled",
        "reason": "explicit_market_tickers_provided",
        "tickers": [],
        "tickers_count": 0,
    }
    seeded_tickers: tuple[str, ...] = ()
    if not explicit_tickers:
        if seed_recent_markets:
            recent_market_discovery = _fetch_recently_updated_climate_market_tickers(
                env_file=env_file,
                as_of=captured_at,
                min_updated_seconds=max(1.0, float(recent_markets_min_updated_seconds)),
                max_markets=max(1, int(max_markets)),
                timeout_seconds=max(1.0, float(recent_markets_timeout_seconds)),
                http_get_json=market_http_get_json,
            )
            seeded_tickers = tuple(
                _normalize_ticker(item)
                for item in recent_market_discovery.get("tickers", [])
                if _normalize_ticker(item)
            )
        else:
            recent_market_discovery = {
                "status": "disabled",
                "reason": "seed_recent_markets_disabled",
                "tickers": [],
                "tickers_count": 0,
            }

    selected_tickers = explicit_tickers or tuple(
        item
        for item in (
            list(seeded_tickers)
            + [
                row.get("market_ticker")
                for row in climate_rows[: max(1, int(max_markets))]
                if _normalize_ticker(row.get("market_ticker"))
            ]
        )
        if _normalize_ticker(item)
    )
    selected_tickers = tuple(dict.fromkeys(_normalize_ticker(item) for item in selected_tickers if _normalize_ticker(item)))

    collect_summary: dict[str, Any] = {
        "status": "skipped_realtime_collect",
        "reason": "skip_realtime_collect_enabled",
        "events_logged": 0,
        "ws_events_ndjson": "",
        "ws_state_json": "",
        "output_file": "",
    }
    if not skip_realtime_collect:
        collect_summary = ws_collect_runner(
            env_file=env_file,
            channels=tuple(channel for channel in ws_channels if str(channel).strip()),
            market_tickers=selected_tickers,
            output_dir=output_dir,
            run_seconds=max(1.0, float(run_seconds)),
            max_staleness_seconds=max(1.0, float(ws_state_max_age_seconds)),
            now=captured_at,
        )

    availability_db = ensure_climate_availability_schema(
        availability_db_path or default_climate_availability_db_path(output_dir)
    )

    events_path = Path(str(collect_summary.get("ws_events_ndjson") or "").strip())
    observation_rows: list[dict[str, Any]] = []
    orderbook_events = 0
    ticker_events = 0
    lifecycle_events = 0
    trade_events = 0
    wakeup_transitions = 0

    conn = _connect(availability_db)
    try:
        ticker_states = {
            ticker: _ensure_state_entry(state)
            for ticker, state in _load_ticker_states(conn).items()
        }
        books: dict[str, dict[str, dict[str, float]]] = {}
        for ticker in ticker_states:
            books[ticker] = {"yes": {}, "no": {}}

        if events_path.exists() and events_path.is_file():
            with events_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        event = json.loads(text)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(event, dict):
                        continue

                    ticker = _extract_event_ticker(event)
                    if not ticker:
                        continue
                    if selected_tickers and ticker not in selected_tickers:
                        continue

                    event_type = _extract_event_type(event)
                    observed_at = _extract_event_ts(event)
                    state = _ensure_state_entry(ticker_states.get(ticker))
                    metadata = ticker_metadata.get(ticker) or {
                        "market_ticker": ticker,
                        "contract_family": str(state.get("contract_family") or "").strip().lower(),
                        "strip_key": str(state.get("strip_key") or "").strip(),
                    }
                    family = str(metadata.get("contract_family") or state.get("contract_family") or "").strip().lower()
                    if family not in include_families:
                        inferred_family = _infer_contract_family(metadata)
                        if inferred_family in include_families:
                            family = inferred_family
                    strip_key = str(metadata.get("strip_key") or state.get("strip_key") or "").strip()

                    if ticker not in books:
                        books[ticker] = {"yes": {}, "no": {}}
                    book = books[ticker]
                    quote_override: dict[str, float | None] | None = None

                    if event_type in {"orderbook_snapshot", "book_snapshot"}:
                        yes_levels, no_levels = _snapshot_levels_from_event(event)
                        if isinstance(yes_levels, dict):
                            book["yes"] = yes_levels
                        if isinstance(no_levels, dict):
                            book["no"] = no_levels
                        orderbook_events += 1
                    elif event_type in {"orderbook_delta", "book_delta"}:
                        yes_updates = _delta_levels_from_event(event, "yes")
                        no_updates = _delta_levels_from_event(event, "no")
                        for price_text, size in yes_updates:
                            if size <= 0:
                                book["yes"].pop(price_text, None)
                            else:
                                book["yes"][price_text] = size
                        for price_text, size in no_updates:
                            if size <= 0:
                                book["no"].pop(price_text, None)
                            else:
                                book["no"][price_text] = size
                        side_text = str(event.get("side") or "").strip().lower()
                        if side_text in {"yes", "no"}:
                            price = _to_price_dollars(event.get("price_dollars") or event.get("price"))
                            delta = _parse_float(event.get("delta_fp") or event.get("delta") or event.get("size_delta"))
                            if isinstance(price, float) and isinstance(delta, float):
                                key = f"{price:.4f}"
                                target = book["yes"] if side_text == "yes" else book["no"]
                                next_size = max(0.0, float(target.get(key) or 0.0) + float(delta))
                                if next_size <= 0:
                                    target.pop(key, None)
                                else:
                                    target[key] = next_size
                        orderbook_events += 1
                    elif event_type in {"ticker", "market_ticker", "market_tickers"}:
                        ticker_events += 1
                        quote_override = _extract_ticker_quote_snapshot(event)
                        yes_bid_tick = quote_override.get("yes_bid")
                        no_bid_tick = quote_override.get("no_bid")
                        if isinstance(yes_bid_tick, float):
                            book["yes"] = {f"{yes_bid_tick:.4f}": max(1.0, float(book["yes"].get(f"{yes_bid_tick:.4f}") or 0.0))}
                        if isinstance(no_bid_tick, float):
                            book["no"] = {f"{no_bid_tick:.4f}": max(1.0, float(book["no"].get(f"{no_bid_tick:.4f}") or 0.0))}
                    elif event_type in {"market_lifecycle", "event_lifecycle", "market_status", "event_status"}:
                        lifecycle_events += 1
                        state["contract_family"] = family
                        state["strip_key"] = strip_key
                        if not str(state.get("first_observed_at_utc") or "").strip():
                            state["first_observed_at_utc"] = observed_at.isoformat()
                        state["last_observed_at_utc"] = observed_at.isoformat()
                        state["observations_total"] = int(state.get("observations_total") or 0) + 1
                        state["updated_at_utc"] = observed_at.isoformat()
                        ticker_states[ticker] = state
                        observation_rows.append(
                            {
                                "run_id": run_id,
                                "observed_at_utc": observed_at.isoformat(),
                                "market_ticker": ticker,
                                "contract_family": family,
                                "strip_key": strip_key,
                                "event_type": event_type,
                                "yes_bid_dollars": None,
                                "yes_ask_dollars": None,
                                "no_bid_dollars": None,
                                "no_ask_dollars": None,
                                "spread_dollars": None,
                                "has_quotes": False,
                                "has_orderable_side": False,
                                "non_endpoint_quote": False,
                                "endpoint_only": False,
                                "two_sided_book": False,
                                "wakeup_transition": False,
                                "public_trade_event": False,
                                "public_trade_contracts": 0.0,
                                "updated_at_utc": _to_iso_utc(captured_at),
                            }
                        )
                        continue
                    elif "trade" in event_type and "orderbook" not in event_type and "subscribed" not in event_type:
                        trade_events += 1
                        contracts = _extract_public_trade_contracts(event)
                        state["public_trade_events"] = int(state.get("public_trade_events") or 0) + 1
                        state["public_trade_contracts"] = round(
                            float(state.get("public_trade_contracts") or 0.0) + contracts,
                            6,
                        )
                        state["last_trade_at_utc"] = observed_at.isoformat()
                        state["contract_family"] = family
                        state["strip_key"] = strip_key
                        state["updated_at_utc"] = observed_at.isoformat()
                        ticker_states[ticker] = state
                        observation_rows.append(
                            {
                                "run_id": run_id,
                                "observed_at_utc": observed_at.isoformat(),
                                "market_ticker": ticker,
                                "contract_family": family,
                                "strip_key": strip_key,
                                "event_type": event_type,
                                "yes_bid_dollars": None,
                                "yes_ask_dollars": None,
                                "no_bid_dollars": None,
                                "no_ask_dollars": None,
                                "spread_dollars": None,
                                "has_quotes": False,
                                "has_orderable_side": False,
                                "non_endpoint_quote": False,
                                "endpoint_only": False,
                                "two_sided_book": False,
                                "wakeup_transition": False,
                                "public_trade_event": True,
                                "public_trade_contracts": contracts,
                                "updated_at_utc": _to_iso_utc(captured_at),
                            }
                        )
                        continue
                    else:
                        continue

                    if quote_override:
                        yes_bid = _to_price_dollars(quote_override.get("yes_bid"))
                        yes_ask = _to_price_dollars(quote_override.get("yes_ask"))
                        no_bid = _to_price_dollars(quote_override.get("no_bid"))
                        no_ask = _to_price_dollars(quote_override.get("no_ask"))
                        spread = _to_price_dollars(quote_override.get("spread"))
                    else:
                        top = _merge_book_top(book["yes"], book["no"])
                        yes_bid = _to_price_dollars(top.get("best_yes_bid_dollars"))
                        yes_ask = _to_price_dollars(top.get("best_yes_ask_dollars"))
                        no_bid = _to_price_dollars(top.get("best_no_bid_dollars"))
                        no_ask = _to_price_dollars(top.get("best_no_ask_dollars"))
                        spread = _to_price_dollars(top.get("yes_spread_dollars"))

                    quotes = [value for value in (yes_bid, yes_ask, no_bid, no_ask) if isinstance(value, float)]
                    has_quotes = bool(quotes)
                    has_orderable = any(_is_orderable_price(value) for value in quotes)
                    non_endpoint_quote = has_quotes and any(
                        isinstance(value, float) and not _is_endpoint_quote(value)
                        for value in quotes
                    )
                    endpoint_only = has_quotes and (not has_orderable) and all(
                        _is_endpoint_quote(value) for value in quotes
                    )
                    two_sided = _is_orderable_price(yes_bid) and _is_orderable_price(no_bid)

                    prev_observed = _parse_ts(state.get("last_observed_at_utc"))
                    if isinstance(prev_observed, datetime):
                        elapsed_minutes = max(0.0, (observed_at - prev_observed).total_seconds() / 60.0)
                        elapsed_minutes = min(elapsed_minutes, 15.0)
                        if bool(state.get("last_has_orderable_side")):
                            state["orderable_minutes_observed"] = round(
                                float(state.get("orderable_minutes_observed") or 0.0) + elapsed_minutes,
                                6,
                            )
                        if bool(state.get("last_has_non_endpoint_quote")):
                            state["non_endpoint_minutes_observed"] = round(
                                float(state.get("non_endpoint_minutes_observed") or 0.0) + elapsed_minutes,
                                6,
                            )

                    wakeup_transition = bool(state.get("last_endpoint_only")) and has_orderable
                    if wakeup_transition:
                        wakeup_transitions += 1
                        state["wakeup_count"] = int(state.get("wakeup_count") or 0) + 1
                        state["last_wakeup_at_utc"] = observed_at.isoformat()
                        if not str(state.get("first_wakeup_at_utc") or "").strip():
                            state["first_wakeup_at_utc"] = observed_at.isoformat()

                    if endpoint_only:
                        state["current_endpoint_only_streak_count"] = int(
                            state.get("current_endpoint_only_streak_count") or 0
                        ) + 1
                    else:
                        state["current_endpoint_only_streak_count"] = 0
                    state["max_endpoint_only_streak_count"] = max(
                        int(state.get("max_endpoint_only_streak_count") or 0),
                        int(state.get("current_endpoint_only_streak_count") or 0),
                    )

                    state["contract_family"] = family
                    state["strip_key"] = strip_key
                    if not str(state.get("first_observed_at_utc") or "").strip():
                        state["first_observed_at_utc"] = observed_at.isoformat()
                    state["last_observed_at_utc"] = observed_at.isoformat()
                    if has_orderable:
                        state["last_orderable_at_utc"] = observed_at.isoformat()
                    if non_endpoint_quote:
                        state["last_non_endpoint_at_utc"] = observed_at.isoformat()

                    state["observations_total"] = int(state.get("observations_total") or 0) + 1
                    if endpoint_only:
                        state["endpoint_only_observations"] = int(state.get("endpoint_only_observations") or 0) + 1
                    if non_endpoint_quote:
                        state["non_endpoint_observations"] = int(state.get("non_endpoint_observations") or 0) + 1
                    if has_orderable:
                        state["orderable_side_observations"] = int(state.get("orderable_side_observations") or 0) + 1
                    if two_sided:
                        state["two_sided_observations"] = int(state.get("two_sided_observations") or 0) + 1

                    state["last_has_orderable_side"] = bool(has_orderable)
                    state["last_has_non_endpoint_quote"] = bool(non_endpoint_quote)
                    state["last_endpoint_only"] = bool(endpoint_only)
                    state["last_yes_bid_dollars"] = yes_bid
                    state["last_yes_ask_dollars"] = yes_ask
                    state["last_no_bid_dollars"] = no_bid
                    state["last_no_ask_dollars"] = no_ask
                    state["last_spread_dollars"] = spread
                    state["updated_at_utc"] = observed_at.isoformat()
                    ticker_states[ticker] = state

                    observation_rows.append(
                        {
                            "run_id": run_id,
                            "observed_at_utc": observed_at.isoformat(),
                            "market_ticker": ticker,
                            "contract_family": family,
                            "strip_key": strip_key,
                            "event_type": event_type,
                            "yes_bid_dollars": yes_bid,
                            "yes_ask_dollars": yes_ask,
                            "no_bid_dollars": no_bid,
                            "no_ask_dollars": no_ask,
                            "spread_dollars": spread,
                            "has_quotes": has_quotes,
                            "has_orderable_side": has_orderable,
                            "non_endpoint_quote": non_endpoint_quote,
                            "endpoint_only": endpoint_only,
                            "two_sided_book": two_sided,
                            "wakeup_transition": wakeup_transition,
                            "public_trade_event": False,
                            "public_trade_contracts": 0.0,
                            "updated_at_utc": _to_iso_utc(captured_at),
                        }
                    )

        observations_written = _write_observation_rows(conn, rows=observation_rows)
        states_updated = _upsert_ticker_states(conn, ticker_states)
        conn.commit()

        lookback_metrics = _load_lookback_metrics(
            conn,
            lookback_days=max(0.0, float(availability_lookback_days)),
            as_of=captured_at,
        )
    finally:
        conn.close()

    rows_with_routing: list[dict[str, Any]] = []
    ticker_state_final_conn = _connect(availability_db)
    try:
        ticker_state_map = {
            ticker: _ensure_state_entry(state)
            for ticker, state in _load_ticker_states(ticker_state_final_conn).items()
        }
    finally:
        ticker_state_final_conn.close()

    family_counts: dict[str, int] = {}
    availability_counts = {
        "dead": 0,
        "priced_watch_only": 0,
        "tradable": 0,
        "hot": 0,
    }
    opportunity_counts = {
        "negative_or_neutral": 0,
        "unpriced_model_view": 0,
        "priced_watch_only": 0,
        "tradable_positive": 0,
        "hot_positive": 0,
    }

    for row in climate_rows:
        ticker = _normalize_ticker(row.get("market_ticker"))
        if not ticker:
            continue
        state = _ensure_state_entry(ticker_state_map.get(ticker))
        lookback = lookback_metrics.get(ticker)
        availability_state, availability_diag = _classify_availability_state(
            state=state,
            lookback=lookback,
            as_of=captured_at,
            recent_seconds=max(1.0, float(availability_recent_seconds)),
            hot_trade_window_seconds=max(1.0, float(availability_hot_trade_window_seconds)),
        )
        availability_counts[availability_state] = availability_counts.get(availability_state, 0) + 1

        candidates = _reference_candidates(row)
        best = candidates[0] if candidates else {}
        best_edge = _parse_float(best.get("edge_net"))
        reference_usable = bool(best.get("reference_usable"))
        modeled_positive = isinstance(best_edge, float) and best_edge > float(min_theoretical_edge_net_fees)

        if modeled_positive and reference_usable and availability_state == "hot":
            opportunity_class = "hot_positive"
        elif modeled_positive and reference_usable and availability_state == "tradable":
            opportunity_class = "tradable_positive"
        elif modeled_positive and reference_usable:
            opportunity_class = "priced_watch_only"
        elif modeled_positive:
            opportunity_class = "unpriced_model_view"
        else:
            opportunity_class = "negative_or_neutral"
        opportunity_counts[opportunity_class] = opportunity_counts.get(opportunity_class, 0) + 1

        family = str(row.get("contract_family") or "").strip().lower() or "unknown"
        family_counts[family] = family_counts.get(family, 0) + 1

        rows_with_routing.append(
            {
                "market_ticker": ticker,
                "market_title": row.get("market_title"),
                "contract_family": family,
                "strip_key": row.get("strip_key"),
                "hours_to_close": _parse_float(row.get("hours_to_close")),
                "fair_yes_probability": _parse_float(row.get("fair_yes_probability")),
                "fair_no_probability": _parse_float(row.get("fair_no_probability")),
                "theoretical_side": best.get("side"),
                "theoretical_reference_source": best.get("reference_source"),
                "theoretical_reference_price": _to_price_dollars(best.get("reference_price")),
                "theoretical_reference_usable": reference_usable,
                "theoretical_reference_endpoint": bool(best.get("reference_endpoint")),
                "theoretical_edge_net": best_edge,
                "modeled_positive": modeled_positive,
                "availability_state": availability_state,
                "opportunity_class": opportunity_class,
                "availability_last_observed_age_seconds": availability_diag.get("last_observed_age_seconds"),
                "availability_last_orderable_age_seconds": availability_diag.get("last_orderable_age_seconds"),
                "availability_last_non_endpoint_age_seconds": availability_diag.get("last_non_endpoint_age_seconds"),
                "availability_last_trade_age_seconds": availability_diag.get("last_trade_age_seconds"),
                "availability_wakeup_count": int((lookback or {}).get("wakeup_count") or state.get("wakeup_count") or 0),
                "availability_orderable_observations": int(
                    (lookback or {}).get("orderable_side_observations")
                    or state.get("orderable_side_observations")
                    or 0
                ),
                "availability_non_endpoint_observations": int(
                    (lookback or {}).get("non_endpoint_observations")
                    or state.get("non_endpoint_observations")
                    or 0
                ),
                "availability_observations_total": int(
                    (lookback or {}).get("observations_total")
                    or state.get("observations_total")
                    or 0
                ),
                "availability_public_trade_events": int(
                    (lookback or {}).get("public_trade_events")
                    or state.get("public_trade_events")
                    or 0
                ),
                "availability_public_trade_contracts": float(
                    (lookback or {}).get("public_trade_contracts")
                    or state.get("public_trade_contracts")
                    or 0.0
                ),
                "availability_orderable_minutes_observed": float(state.get("orderable_minutes_observed") or 0.0),
                "availability_non_endpoint_minutes_observed": float(state.get("non_endpoint_minutes_observed") or 0.0),
            }
        )

    allocation = _allocate_tradable_capital(
        rows=rows_with_routing,
        daily_risk_cap_dollars=max(0.0, float(daily_risk_cap_dollars)),
        max_risk_per_bet_dollars=max(0.01, float(max_risk_per_bet_dollars)),
    )

    strip_summaries = _summarize_strip_availability(
        rows=rows_with_routing,
        ticker_state=ticker_state_map,
        lookback_metrics=lookback_metrics,
    )

    out_csv = out_dir / f"kalshi_climate_router_{stamp}.csv"
    _write_rows_csv(
        out_csv,
        rows_with_routing,
        [
            "market_ticker",
            "market_title",
            "contract_family",
            "strip_key",
            "hours_to_close",
            "fair_yes_probability",
            "fair_no_probability",
            "theoretical_side",
            "theoretical_reference_source",
            "theoretical_reference_price",
            "theoretical_reference_usable",
            "theoretical_reference_endpoint",
            "theoretical_edge_net",
            "modeled_positive",
            "availability_state",
            "opportunity_class",
            "availability_last_observed_age_seconds",
            "availability_last_orderable_age_seconds",
            "availability_last_non_endpoint_age_seconds",
            "availability_last_trade_age_seconds",
            "availability_wakeup_count",
            "availability_orderable_observations",
            "availability_non_endpoint_observations",
            "availability_observations_total",
            "availability_public_trade_events",
            "availability_public_trade_contracts",
            "availability_orderable_minutes_observed",
            "availability_non_endpoint_minutes_observed",
        ],
    )

    top_theoretical = sorted(
        rows_with_routing,
        key=lambda row: _parse_float(row.get("theoretical_edge_net")) or -999.0,
        reverse=True,
    )[:10]
    top_tradable = [
        row for row in rows_with_routing
        if str(row.get("opportunity_class") or "").strip() in {"tradable_positive", "hot_positive"}
    ]
    top_tradable = sorted(
        top_tradable,
        key=lambda row: _parse_float(row.get("theoretical_edge_net")) or -999.0,
        reverse=True,
    )[:10]
    top_watch = [
        row for row in rows_with_routing
        if str(row.get("opportunity_class") or "").strip() in {"priced_watch_only", "unpriced_model_view"}
    ]
    top_watch = sorted(
        top_watch,
        key=lambda row: _parse_float(row.get("theoretical_edge_net")) or -999.0,
        reverse=True,
    )[:10]

    rain_model_tag = detect_weather_model_tags(climate_rows).get("rain_model_tag")
    temperature_model_tag = detect_weather_model_tags(climate_rows).get("temperature_model_tag")
    weather_priors_version_name = weather_priors_version(
        rain_model_tag=rain_model_tag,
        temperature_model_tag=temperature_model_tag,
    )

    summary = {
        "captured_at": captured_at.isoformat(),
        "run_id": run_id,
        "env_file": env_file,
        "priors_csv": str(priors_csv),
        "history_csv": str(history_csv),
        "output_dir": str(out_dir),
        "availability_db_path": str(availability_db),
        "ws_collect_status": collect_summary.get("status"),
        "ws_collect_output_file": collect_summary.get("output_file"),
        "ws_events_ndjson": collect_summary.get("ws_events_ndjson"),
        "ws_state_json": collect_summary.get("ws_state_json"),
        "ws_channels": list(tuple(channel for channel in ws_channels if str(channel).strip())),
        "ws_events_logged": collect_summary.get("events_logged"),
        "ws_market_count": collect_summary.get("market_count"),
        "ws_gate_pass": collect_summary.get("gate_pass"),
        "ws_last_error_kind": collect_summary.get("last_error_kind"),
        "skip_realtime_collect": bool(skip_realtime_collect),
        "seed_recent_markets": bool(seed_recent_markets),
        "recent_markets_min_updated_seconds": max(1.0, float(recent_markets_min_updated_seconds)),
        "recent_market_discovery_status": recent_market_discovery.get("status"),
        "recent_market_discovery_reason": recent_market_discovery.get("reason"),
        "recent_market_discovery_api_root": recent_market_discovery.get("api_root"),
        "recent_market_discovery_http_status": recent_market_discovery.get("http_status"),
        "recent_market_discovery_min_updated_ts_ms": recent_market_discovery.get("min_updated_ts_ms"),
        "recent_market_discovery_tickers": recent_market_discovery.get("tickers"),
        "recent_market_discovery_tickers_count": recent_market_discovery.get("tickers_count"),
        "recent_market_discovery_errors": recent_market_discovery.get("errors"),
        "climate_rows_total": len(rows_with_routing),
        "climate_family_counts": family_counts,
        "climate_availability_state_counts": availability_counts,
        "climate_opportunity_class_counts": opportunity_counts,
        "climate_theoretical_positive_rows": opportunity_counts.get("hot_positive", 0)
        + opportunity_counts.get("tradable_positive", 0)
        + opportunity_counts.get("priced_watch_only", 0)
        + opportunity_counts.get("unpriced_model_view", 0),
        "climate_priced_watch_only_rows": opportunity_counts.get("priced_watch_only", 0),
        "climate_unpriced_model_view_rows": opportunity_counts.get("unpriced_model_view", 0),
        "climate_tradable_rows": availability_counts.get("tradable", 0),
        "climate_hot_rows": availability_counts.get("hot", 0),
        "climate_dead_rows": availability_counts.get("dead", 0),
        "climate_tradable_positive_rows": opportunity_counts.get("tradable_positive", 0),
        "climate_hot_positive_rows": opportunity_counts.get("hot_positive", 0),
        "climate_negative_or_neutral_rows": opportunity_counts.get("negative_or_neutral", 0),
        "market_tickers_selected_count": len(selected_tickers),
        "market_tickers_selected": list(selected_tickers),
        "explicit_market_tickers": list(explicit_tickers),
        "max_markets": int(max_markets),
        "orderbook_events_processed": orderbook_events,
        "ticker_events_processed": ticker_events,
        "lifecycle_events_processed": lifecycle_events,
        "public_trade_events_processed": trade_events,
        "wakeup_transitions_processed": wakeup_transitions,
        "availability_observations_written": observations_written,
        "availability_ticker_states_updated": states_updated,
        "availability_lookback_days": max(0.0, float(availability_lookback_days)),
        "availability_recent_seconds": max(1.0, float(availability_recent_seconds)),
        "availability_hot_trade_window_seconds": max(1.0, float(availability_hot_trade_window_seconds)),
        "top_theoretical_candidates": top_theoretical,
        "top_tradable_candidates": top_tradable,
        "top_watch_only_candidates": top_watch,
        "top_waking_strips": strip_summaries[:10],
        "strip_summaries_count": len(strip_summaries),
        "routing_allocator_eligible_rows": allocation.get("eligible_rows"),
        "routing_allocator_allocated_rows": allocation.get("allocated_rows"),
        "routing_allocator_total_risk_dollars": allocation.get("total_risk_dollars"),
        "routing_allocator_total_expected_value_dollars": allocation.get("total_expected_value_dollars"),
        "routing_allocator_allocations": allocation.get("allocations"),
        "family_routed_capital_budget": allocation.get("family_routed_capital_budget"),
        "output_csv": str(out_csv),
        "status": "ready",
    }

    # Promote realtime ingest health while keeping the command non-fatal for diagnostics.
    ws_status = str(collect_summary.get("status") or "").strip().lower()
    if not skip_realtime_collect and ws_status in {"upstream_error", "invalid", "missing"}:
        summary["status"] = "degraded_realtime_unavailable"

    summary["runtime_version"] = build_runtime_version_block(
        run_started_at=captured_at,
        run_id=run_id,
        git_cwd=Path.cwd(),
        rain_model_tag=rain_model_tag,
        temperature_model_tag=temperature_model_tag,
        weather_priors_version_name=weather_priors_version_name,
        as_of=captured_at,
    )

    summary_path = out_dir / f"kalshi_climate_router_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
