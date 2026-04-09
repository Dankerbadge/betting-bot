from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable

from betbot.kalshi_book import default_book_db_path
from betbot.kalshi_micro_execute import run_kalshi_micro_execute
from betbot.kalshi_temperature_constraints import run_kalshi_temperature_constraint_scan
from betbot.kalshi_ws_state import default_ws_state_path


ConstraintScanRunner = Callable[..., dict[str, Any]]
MicroExecuteRunner = Callable[..., dict[str, Any]]
SleepFn = Callable[[float], None]

_ACTIONABLE_CONSTRAINTS = {"yes_impossible", "yes_likely_locked"}
_CONSTRAINT_PRIORITY = {"yes_impossible": 0, "yes_likely_locked": 1}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> datetime | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _find_latest_csv(output_dir: str, pattern: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob(pattern))
    if not candidates:
        return ""
    return str(candidates[-1])


def _build_spec_hash(spec_row: dict[str, Any]) -> str:
    digest_payload = {
        "market_ticker": _normalize_text(spec_row.get("market_ticker")),
        "rules_primary": _normalize_text(spec_row.get("rules_primary")),
        "rules_secondary": _normalize_text(spec_row.get("rules_secondary")),
        "settlement_station": _normalize_text(spec_row.get("settlement_station")),
        "settlement_timezone": _normalize_text(spec_row.get("settlement_timezone")),
        "local_day_boundary": _normalize_text(spec_row.get("local_day_boundary")),
        "observation_window_local_start": _normalize_text(spec_row.get("observation_window_local_start")),
        "observation_window_local_end": _normalize_text(spec_row.get("observation_window_local_end")),
        "threshold_expression": _normalize_text(spec_row.get("threshold_expression")),
        "contract_terms_url": _normalize_text(spec_row.get("contract_terms_url")),
    }
    encoded = json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _build_underlying_key(
    *,
    series_ticker: Any,
    settlement_station: Any,
    target_date_local: Any,
) -> str:
    return "|".join(
        (
            _normalize_text(series_ticker) or "series_unknown",
            _normalize_text(settlement_station) or "station_unknown",
            _normalize_text(target_date_local) or "date_unknown",
        )
    )


def _resolve_specs_csv(
    *,
    explicit_specs_csv: str | None,
    constraint_rows: list[dict[str, str]],
    output_dir: str,
) -> str:
    if _normalize_text(explicit_specs_csv):
        return _normalize_text(explicit_specs_csv)
    for row in constraint_rows:
        source_specs_csv = _normalize_text(row.get("source_specs_csv"))
        if source_specs_csv:
            return source_specs_csv
    return _find_latest_csv(output_dir, "kalshi_temperature_contract_specs_*.csv")


def _build_specs_by_ticker(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {
        _normalize_text(row.get("market_ticker")): row
        for row in rows
        if _normalize_text(row.get("market_ticker"))
    }


def _ticker_underlying_keys(specs_by_ticker: dict[str, dict[str, str]]) -> dict[str, str]:
    underlying_by_ticker: dict[str, str] = {}
    for ticker, row in specs_by_ticker.items():
        normalized_ticker = _normalize_text(ticker)
        if not normalized_ticker:
            continue
        underlying_by_ticker[normalized_ticker] = _build_underlying_key(
            series_ticker=row.get("series_ticker"),
            settlement_station=row.get("settlement_station"),
            target_date_local=row.get("target_date_local"),
        )
    return underlying_by_ticker


def _load_existing_underlying_netting_snapshot(
    *,
    output_dir: str,
    book_db_path: str | None,
    specs_by_ticker: dict[str, dict[str, str]],
    contracts_per_order: int,
) -> dict[str, Any]:
    resolved_book_path = Path(_normalize_text(book_db_path)) if _normalize_text(book_db_path) else default_book_db_path(output_dir)
    snapshot: dict[str, Any] = {
        "book_db_path": str(resolved_book_path),
        "loaded": False,
        "error": "",
        "position_rows": 0,
        "open_order_rows": 0,
        "unknown_ticker_rows": 0,
        "underlying_slots": {},
        "underlying_position_abs_contracts": {},
        "underlying_open_order_slots": {},
        "underlying_exposure_abs_dollars": {},
    }
    if not resolved_book_path.exists():
        return snapshot

    ticker_to_underlying = _ticker_underlying_keys(specs_by_ticker)
    if not ticker_to_underlying:
        snapshot["loaded"] = True
        return snapshot

    safe_contracts_per_order = max(1, int(contracts_per_order))
    underlying_slots: dict[str, int] = {}
    underlying_position_abs_contracts: dict[str, float] = {}
    underlying_open_order_slots: dict[str, int] = {}
    underlying_exposure_abs_dollars: dict[str, float] = {}

    try:
        connection = sqlite3.connect(str(resolved_book_path))
        connection.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        snapshot["error"] = f"book_connect_failed:{exc}"
        return snapshot

    try:
        try:
            position_rows = connection.execute(
                """
                SELECT ticker, COALESCE(position_fp, 0.0) AS position_fp, COALESCE(market_exposure_dollars, 0.0) AS market_exposure_dollars
                FROM positions
                WHERE ABS(COALESCE(position_fp, 0.0)) > 1e-9
                   OR ABS(COALESCE(market_exposure_dollars, 0.0)) > 1e-9
                """
            ).fetchall()
        except sqlite3.Error:
            position_rows = []
        snapshot["position_rows"] = len(position_rows)
        for row in position_rows:
            ticker = _normalize_text(row["ticker"])
            underlying_key = ticker_to_underlying.get(ticker)
            if not underlying_key:
                snapshot["unknown_ticker_rows"] = int(snapshot.get("unknown_ticker_rows") or 0) + 1
                continue
            position_abs = abs(float(row["position_fp"] or 0.0))
            exposure_abs = abs(float(row["market_exposure_dollars"] or 0.0))
            slots = 0
            if position_abs > 1e-9:
                slots = max(1, int(math.ceil(position_abs / float(safe_contracts_per_order))))
            elif exposure_abs > 1e-9:
                slots = 1
            if slots > 0:
                underlying_slots[underlying_key] = underlying_slots.get(underlying_key, 0) + slots
            if position_abs > 1e-9:
                underlying_position_abs_contracts[underlying_key] = round(
                    underlying_position_abs_contracts.get(underlying_key, 0.0) + position_abs,
                    6,
                )
            if exposure_abs > 1e-9:
                underlying_exposure_abs_dollars[underlying_key] = round(
                    underlying_exposure_abs_dollars.get(underlying_key, 0.0) + exposure_abs,
                    6,
                )

        try:
            open_order_rows = connection.execute(
                """
                SELECT ticker, COUNT(*) AS open_count
                FROM orders
                WHERE LOWER(COALESCE(status, '')) IN ('resting', 'open', 'pending')
                GROUP BY ticker
                """
            ).fetchall()
        except sqlite3.Error:
            open_order_rows = []
        snapshot["open_order_rows"] = len(open_order_rows)
        for row in open_order_rows:
            ticker = _normalize_text(row["ticker"])
            underlying_key = ticker_to_underlying.get(ticker)
            if not underlying_key:
                snapshot["unknown_ticker_rows"] = int(snapshot.get("unknown_ticker_rows") or 0) + 1
                continue
            open_count = max(0, int(row["open_count"] or 0))
            if open_count <= 0:
                continue
            underlying_slots[underlying_key] = underlying_slots.get(underlying_key, 0) + open_count
            underlying_open_order_slots[underlying_key] = underlying_open_order_slots.get(underlying_key, 0) + open_count
    finally:
        connection.close()

    snapshot["loaded"] = True
    snapshot["underlying_slots"] = dict(sorted(underlying_slots.items()))
    snapshot["underlying_position_abs_contracts"] = dict(sorted(underlying_position_abs_contracts.items()))
    snapshot["underlying_open_order_slots"] = dict(sorted(underlying_open_order_slots.items()))
    snapshot["underlying_exposure_abs_dollars"] = dict(sorted(underlying_exposure_abs_dollars.items()))
    snapshot["underlying_count"] = len(underlying_slots)
    return snapshot


def _load_market_sequences(
    *,
    ws_state_json: str | None,
    output_dir: str,
) -> tuple[str, dict[str, int | None], dict[str, Any]]:
    ws_path = Path(ws_state_json) if _normalize_text(ws_state_json) else default_ws_state_path(output_dir)
    payload = _read_json(ws_path)
    market_sequences: dict[str, int | None] = {}
    markets = payload.get("markets")
    if isinstance(markets, dict):
        for market_ticker, market_payload in markets.items():
            if not isinstance(market_payload, dict):
                continue
            market_sequences[str(market_ticker)] = _parse_int(market_payload.get("sequence"))
    return str(ws_path), market_sequences, payload


def _load_metar_context(
    *,
    output_dir: str,
    metar_summary_json: str | None,
    metar_state_json: str | None,
) -> dict[str, Any]:
    summary_path = Path(_normalize_text(metar_summary_json)) if _normalize_text(metar_summary_json) else None
    if summary_path is None:
        latest_summary = _find_latest_csv(output_dir, "kalshi_temperature_metar_summary_*.json")
        if latest_summary:
            summary_path = Path(latest_summary)

    summary_payload: dict[str, Any] = {}
    if summary_path is not None and summary_path.exists():
        summary_payload = _read_json(summary_path)

    state_path_text = _normalize_text(metar_state_json)
    if not state_path_text:
        state_path_text = _normalize_text(summary_payload.get("state_file"))
    if not state_path_text:
        state_path_text = str(Path(output_dir) / "kalshi_temperature_metar_state.json")
    state_path = Path(state_path_text)
    state_payload = _read_json(state_path)
    latest_by_station = state_payload.get("latest_observation_by_station")
    if not isinstance(latest_by_station, dict):
        latest_by_station = {}

    return {
        "summary_path": str(summary_path) if summary_path is not None else "",
        "summary_payload": summary_payload,
        "state_path": str(state_path),
        "state_payload": state_payload,
        "raw_sha256": _normalize_text(summary_payload.get("raw_sha256")),
        "captured_at": _normalize_text(summary_payload.get("captured_at")),
        "latest_by_station": latest_by_station,
    }


def _hours_to_close(*, close_time: Any, now: datetime) -> float | None:
    close_ts = _parse_ts(close_time)
    if close_ts is None:
        return None
    return round((close_ts - now).total_seconds() / 3600.0, 6)


def _metar_observation_age_minutes(*, observation_time_utc: Any, now: datetime) -> float | None:
    observation_ts = _parse_ts(observation_time_utc)
    if observation_ts is None:
        return None
    return round(max(0.0, (now - observation_ts).total_seconds()) / 60.0, 6)


def _side_from_constraint(constraint_status: str) -> str:
    status = _normalize_text(constraint_status).lower()
    if status == "yes_impossible":
        return "no"
    return "yes"


def _find_latest_settlement_state_json(output_dir: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob("kalshi_temperature_settlement_state_*.json"))
    if not candidates:
        return ""
    return str(candidates[-1])


def _derive_settlement_allow_new_orders(*, state: str, finalization_status: str, allow_new_orders: Any) -> bool:
    if isinstance(allow_new_orders, bool):
        return allow_new_orders
    state_text = _normalize_text(state).lower()
    finalization_text = _normalize_text(finalization_status).lower()
    if "review" in state_text or "review" in finalization_text:
        return False
    if state_text in {"final", "final_locked", "settled", "closed_final"}:
        return False
    if finalization_text in {"final", "final_locked", "settled"}:
        return False
    return True


def _load_settlement_state_by_underlying(
    *,
    output_dir: str,
    settlement_state_json: str | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    resolved_path = _normalize_text(settlement_state_json)
    if not resolved_path:
        resolved_path = _find_latest_settlement_state_json(output_dir)
    meta: dict[str, Any] = {
        "settlement_state_json_used": resolved_path,
        "loaded": False,
        "entry_count": 0,
        "error": "",
    }
    if not resolved_path:
        return {}, meta
    payload = _read_json(Path(resolved_path))
    if not payload:
        meta["error"] = "settlement_state_json_unreadable"
        return {}, meta

    raw_underlyings: dict[str, Any] = {}
    for key in ("underlyings", "state_by_underlying", "underlying_states"):
        candidate = payload.get(key)
        if isinstance(candidate, dict):
            raw_underlyings = candidate
            break
    if not raw_underlyings and all(isinstance(key, str) for key in payload.keys()):
        raw_underlyings = payload

    normalized: dict[str, dict[str, Any]] = {}
    for underlying_key, entry in raw_underlyings.items():
        if not isinstance(entry, dict):
            continue
        key = _normalize_text(underlying_key)
        if not key:
            continue
        state = _normalize_text(entry.get("state") or entry.get("finalization_status"))
        finalization_status = _normalize_text(entry.get("finalization_status") or state)
        allow_new_orders = _derive_settlement_allow_new_orders(
            state=state,
            finalization_status=finalization_status,
            allow_new_orders=entry.get("allow_new_orders"),
        )
        normalized[key] = {
            "state": state,
            "finalization_status": finalization_status,
            "allow_new_orders": allow_new_orders,
            "reason": _normalize_text(entry.get("reason")),
            "review_flag": bool(entry.get("review_flag")),
            "updated_at": _normalize_text(entry.get("updated_at")),
            "final_truth_value": entry.get("final_truth_value"),
            "fast_truth_value": entry.get("fast_truth_value"),
            "revision_id": _normalize_text(entry.get("revision_id")),
            "source": _normalize_text(entry.get("source")),
        }

    meta["loaded"] = True
    meta["entry_count"] = len(normalized)
    return normalized, meta


def _settlement_block_reason(entry: dict[str, Any] | None) -> str | None:
    if not isinstance(entry, dict):
        return None
    if bool(entry.get("allow_new_orders", True)):
        return None
    state = _normalize_text(entry.get("state") or entry.get("finalization_status")).lower()
    if "review" in state:
        return "settlement_review_hold"
    if state in {"final", "final_locked", "settled", "closed_final"}:
        return "settlement_final_locked"
    return "settlement_finalization_blocked"


def _build_intent_id(
    *,
    market_ticker: str,
    constraint_status: str,
    spec_hash: str,
    metar_snapshot_sha: str,
    market_snapshot_seq: int | None,
    policy_version: str,
) -> str:
    raw = "|".join(
        (
            market_ticker,
            constraint_status,
            spec_hash,
            metar_snapshot_sha,
            str(market_snapshot_seq if market_snapshot_seq is not None else ""),
            policy_version,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class TemperatureTradeIntent:
    intent_id: str
    captured_at: str
    policy_version: str
    underlying_key: str
    series_ticker: str
    event_ticker: str
    market_ticker: str
    market_title: str
    settlement_station: str
    settlement_timezone: str
    target_date_local: str
    constraint_status: str
    constraint_reason: str
    side: str
    max_entry_price_dollars: float
    intended_contracts: int
    settlement_confidence_score: float
    observed_max_settlement_quantized: float | None
    close_time: str
    hours_to_close: float | None
    spec_hash: str
    metar_snapshot_sha: str
    metar_observation_time_utc: str
    metar_observation_age_minutes: float | None
    market_snapshot_seq: int | None

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TemperaturePolicyDecision:
    intent_id: str
    approved: bool
    decision_reason: str
    decision_notes: str

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TemperaturePolicyGate:
    min_settlement_confidence: float = 0.6
    max_metar_age_minutes: float | None = 20.0
    min_hours_to_close: float | None = 0.0
    max_hours_to_close: float | None = 48.0
    max_intents_per_underlying: int = 1
    require_market_snapshot_seq: bool = True
    require_metar_snapshot_sha: bool = False

    def evaluate(
        self,
        *,
        intents: list[TemperatureTradeIntent],
        existing_underlying_slots: dict[str, int] | None = None,
        settlement_state_by_underlying: dict[str, dict[str, Any]] | None = None,
    ) -> list[TemperaturePolicyDecision]:
        decisions: list[TemperaturePolicyDecision] = []
        approved_by_underlying: dict[str, int] = {}
        if isinstance(existing_underlying_slots, dict):
            for underlying_key, slots in existing_underlying_slots.items():
                key = _normalize_text(underlying_key)
                if not key:
                    continue
                approved_by_underlying[key] = max(0, int(slots))

        for intent in intents:
            blocked: list[str] = []

            if intent.constraint_status not in _ACTIONABLE_CONSTRAINTS:
                blocked.append("constraint_not_actionable")
            if intent.settlement_confidence_score < float(self.min_settlement_confidence):
                blocked.append("settlement_confidence_below_min")
            if self.require_metar_snapshot_sha and not intent.metar_snapshot_sha:
                blocked.append("missing_metar_snapshot_sha")
            if self.require_market_snapshot_seq and intent.market_snapshot_seq is None:
                blocked.append("missing_market_snapshot_seq")
            if not intent.spec_hash:
                blocked.append("missing_spec_hash")

            if self.max_metar_age_minutes is not None:
                if intent.metar_observation_age_minutes is None:
                    blocked.append("metar_observation_age_unknown")
                elif float(intent.metar_observation_age_minutes) > float(self.max_metar_age_minutes):
                    blocked.append("metar_observation_stale")

            if self.min_hours_to_close is not None and intent.hours_to_close is not None:
                if float(intent.hours_to_close) < float(self.min_hours_to_close):
                    blocked.append("inside_cutoff_window")
            if self.max_hours_to_close is not None and intent.hours_to_close is not None:
                if float(intent.hours_to_close) > float(self.max_hours_to_close):
                    blocked.append("outside_active_horizon")

            if isinstance(settlement_state_by_underlying, dict):
                settlement_entry = settlement_state_by_underlying.get(intent.underlying_key)
                settlement_block_reason = _settlement_block_reason(settlement_entry)
                if settlement_block_reason:
                    blocked.append(settlement_block_reason)

            current_underlying = approved_by_underlying.get(intent.underlying_key, 0)
            if current_underlying >= max(1, int(self.max_intents_per_underlying)):
                blocked.append("underlying_exposure_cap_reached")

            if blocked:
                decisions.append(
                    TemperaturePolicyDecision(
                        intent_id=intent.intent_id,
                        approved=False,
                        decision_reason=blocked[0],
                        decision_notes=",".join(blocked),
                    )
                )
                continue

            approved_by_underlying[intent.underlying_key] = current_underlying + 1
            decisions.append(
                TemperaturePolicyDecision(
                    intent_id=intent.intent_id,
                    approved=True,
                    decision_reason="approved",
                    decision_notes="",
                )
            )

        return decisions


@dataclass
class TemperatureExecutionBridge:
    contracts_per_order: int = 1

    def _payload(
        self,
        *,
        intent: TemperatureTradeIntent,
        order_group_id: str,
        client_order_id: str,
    ) -> dict[str, Any]:
        contracts = max(1, int(self.contracts_per_order))
        price = max(0.01, min(0.99, float(intent.max_entry_price_dollars)))
        payload: dict[str, Any] = {
            "ticker": intent.market_ticker,
            "side": intent.side,
            "action": "buy",
            "count": contracts,
            "client_order_id": _normalize_text(client_order_id),
            "time_in_force": "good_till_canceled",
            "post_only": True,
            "cancel_order_on_pause": True,
            "self_trade_prevention_type": "maker",
        }
        if intent.side == "no":
            payload["no_price_dollars"] = f"{price:.4f}"
        else:
            payload["yes_price_dollars"] = f"{price:.4f}"
        if order_group_id:
            payload["order_group_id"] = order_group_id
        return payload

    def to_plan(
        self,
        *,
        intent: TemperatureTradeIntent,
        rank: int,
        order_group_id: str,
    ) -> dict[str, Any]:
        contracts = max(1, int(self.contracts_per_order))
        price = max(0.01, min(0.99, float(intent.max_entry_price_dollars)))
        client_order_id = _build_deterministic_client_order_id(
            intent=intent,
            order_group_id=order_group_id,
        )
        estimated_entry_cost = round(price * contracts, 4)
        if intent.constraint_status == "yes_impossible":
            edge_net = 0.08
            confidence = 0.92
        else:
            edge_net = 0.04
            confidence = 0.78
        return {
            "plan_rank": rank,
            "category": "Climate and Weather",
            "market_ticker": intent.market_ticker,
            "canonical_ticker": intent.underlying_key,
            "canonical_niche": "weather_climate",
            "contract_family": "daily_temperature",
            "source_strategy": "temperature_constraints",
            "side": intent.side,
            "contracts_per_order": contracts,
            "hours_to_close": intent.hours_to_close if intent.hours_to_close is not None else "",
            "confidence": round(max(confidence, intent.settlement_confidence_score), 3),
            "effective_min_evidence_count": 3,
            "maker_entry_price_dollars": round(price, 4),
            "maker_yes_price_dollars": round(price, 4) if intent.side == "yes" else "",
            "yes_ask_dollars": "",
            "maker_entry_edge_conservative_net_total": round(edge_net, 6),
            "estimated_entry_cost_dollars": estimated_entry_cost,
            "estimated_entry_fee_dollars": 0.0,
            "temperature_intent_id": intent.intent_id,
            "temperature_underlying_key": intent.underlying_key,
            "temperature_policy_version": intent.policy_version,
            "temperature_spec_hash": intent.spec_hash,
            "temperature_metar_snapshot_sha": intent.metar_snapshot_sha,
            "temperature_market_snapshot_seq": intent.market_snapshot_seq if intent.market_snapshot_seq is not None else "",
            "temperature_client_order_id": client_order_id,
            "order_payload_preview": self._payload(
                intent=intent,
                order_group_id=order_group_id,
                client_order_id=client_order_id,
            ),
        }


def build_temperature_trade_intents(
    *,
    constraint_rows: list[dict[str, str]],
    specs_by_ticker: dict[str, dict[str, str]],
    metar_context: dict[str, Any],
    market_sequences: dict[str, int | None],
    policy_version: str,
    contracts_per_order: int,
    yes_max_entry_price_dollars: float,
    no_max_entry_price_dollars: float,
    now: datetime,
) -> list[TemperatureTradeIntent]:
    latest_by_station = (
        metar_context.get("latest_by_station") if isinstance(metar_context.get("latest_by_station"), dict) else {}
    )
    metar_snapshot_sha = _normalize_text(metar_context.get("raw_sha256"))

    intents: list[TemperatureTradeIntent] = []
    for row in constraint_rows:
        constraint_status = _normalize_text(row.get("constraint_status")).lower()
        if constraint_status not in _ACTIONABLE_CONSTRAINTS:
            continue

        market_ticker = _normalize_text(row.get("market_ticker"))
        if not market_ticker:
            continue
        spec_row = specs_by_ticker.get(market_ticker, {})
        settlement_station = _normalize_text(row.get("settlement_station")) or _normalize_text(
            spec_row.get("settlement_station")
        )
        target_date_local = _normalize_text(row.get("target_date_local")) or _normalize_text(
            spec_row.get("target_date_local")
        )
        series_ticker = _normalize_text(row.get("series_ticker")) or _normalize_text(spec_row.get("series_ticker"))
        event_ticker = _normalize_text(row.get("event_ticker")) or _normalize_text(spec_row.get("event_ticker"))
        underlying_key = _build_underlying_key(
            series_ticker=series_ticker,
            settlement_station=settlement_station,
            target_date_local=target_date_local,
        )
        side = _side_from_constraint(constraint_status)
        max_entry_price = float(yes_max_entry_price_dollars) if side == "yes" else float(no_max_entry_price_dollars)

        latest_station = latest_by_station.get(settlement_station) if settlement_station else None
        if not isinstance(latest_station, dict):
            latest_station = {}
        metar_observation_time = _normalize_text(latest_station.get("observation_time_utc"))
        metar_age = _metar_observation_age_minutes(observation_time_utc=metar_observation_time, now=now)

        close_time = _normalize_text(spec_row.get("close_time"))
        confidence = _parse_float(row.get("settlement_confidence_score"))
        if confidence is None:
            confidence = _parse_float(spec_row.get("settlement_confidence_score"))
        if confidence is None:
            confidence = 0.0

        observed_max = _parse_float(row.get("observed_max_settlement_quantized"))
        market_seq = market_sequences.get(market_ticker)
        spec_hash = _build_spec_hash(spec_row if spec_row else row)
        intent_id = _build_intent_id(
            market_ticker=market_ticker,
            constraint_status=constraint_status,
            spec_hash=spec_hash,
            metar_snapshot_sha=metar_snapshot_sha,
            market_snapshot_seq=market_seq,
            policy_version=policy_version,
        )

        intents.append(
            TemperatureTradeIntent(
                intent_id=intent_id,
                captured_at=now.isoformat(),
                policy_version=policy_version,
                underlying_key=underlying_key,
                series_ticker=series_ticker,
                event_ticker=event_ticker,
                market_ticker=market_ticker,
                market_title=_normalize_text(row.get("market_title")) or _normalize_text(spec_row.get("market_title")),
                settlement_station=settlement_station,
                settlement_timezone=_normalize_text(row.get("settlement_timezone"))
                or _normalize_text(spec_row.get("settlement_timezone")),
                target_date_local=target_date_local,
                constraint_status=constraint_status,
                constraint_reason=_normalize_text(row.get("constraint_reason")),
                side=side,
                max_entry_price_dollars=round(max(0.01, min(0.99, max_entry_price)), 4),
                intended_contracts=max(1, int(contracts_per_order)),
                settlement_confidence_score=round(max(0.0, min(1.0, confidence)), 6),
                observed_max_settlement_quantized=observed_max,
                close_time=close_time,
                hours_to_close=_hours_to_close(close_time=close_time, now=now),
                spec_hash=spec_hash,
                metar_snapshot_sha=metar_snapshot_sha,
                metar_observation_time_utc=metar_observation_time,
                metar_observation_age_minutes=metar_age,
                market_snapshot_seq=market_seq,
            )
        )

    intents.sort(
        key=lambda intent: (
            _CONSTRAINT_PRIORITY.get(intent.constraint_status, 99),
            intent.hours_to_close if intent.hours_to_close is not None else 9999.0,
            intent.market_ticker,
        )
    )
    return intents


def revalidate_temperature_trade_intents(
    *,
    intents: list[TemperatureTradeIntent],
    output_dir: str,
    specs_csv: str | None,
    metar_summary_json: str | None,
    metar_state_json: str | None,
    ws_state_json: str | None,
    require_market_snapshot_seq: bool,
    require_metar_snapshot_sha: bool,
) -> tuple[list[TemperatureTradeIntent], list[dict[str, Any]], dict[str, Any]]:
    if not intents:
        return [], [], {
            "specs_csv_used": _normalize_text(specs_csv),
            "metar_summary_json_used": _normalize_text(metar_summary_json),
            "metar_state_json_used": _normalize_text(metar_state_json),
            "ws_state_json_used": _normalize_text(ws_state_json),
            "market_count": 0,
        }

    specs_path = Path(_normalize_text(specs_csv)) if _normalize_text(specs_csv) else None
    specs_rows = _read_csv_rows(specs_path) if specs_path is not None else []
    specs_by_ticker = _build_specs_by_ticker(specs_rows)

    metar_context = _load_metar_context(
        output_dir=output_dir,
        metar_summary_json=metar_summary_json,
        metar_state_json=metar_state_json,
    )
    current_metar_snapshot_sha = _normalize_text(metar_context.get("raw_sha256"))
    latest_by_station = metar_context.get("latest_by_station")
    if not isinstance(latest_by_station, dict):
        latest_by_station = {}

    ws_path, market_sequences, _ = _load_market_sequences(
        ws_state_json=ws_state_json,
        output_dir=output_dir,
    )

    valid: list[TemperatureTradeIntent] = []
    invalid: list[dict[str, Any]] = []

    for intent in intents:
        reasons: list[str] = []

        current_spec_row = specs_by_ticker.get(intent.market_ticker, {})
        current_spec_hash = _build_spec_hash(current_spec_row) if current_spec_row else ""
        if not current_spec_hash:
            reasons.append("spec_missing_on_revalidate")
        elif current_spec_hash != intent.spec_hash:
            reasons.append("spec_hash_changed")

        current_seq = market_sequences.get(intent.market_ticker)
        if require_market_snapshot_seq:
            if current_seq is None:
                reasons.append("market_snapshot_seq_missing_on_revalidate")
            elif intent.market_snapshot_seq is None:
                reasons.append("intent_missing_market_snapshot_seq")
            elif int(current_seq) != int(intent.market_snapshot_seq):
                reasons.append("market_snapshot_seq_changed")

        if require_metar_snapshot_sha:
            if not current_metar_snapshot_sha:
                reasons.append("metar_snapshot_sha_missing_on_revalidate")
            elif not intent.metar_snapshot_sha:
                reasons.append("intent_missing_metar_snapshot_sha")
            elif current_metar_snapshot_sha != intent.metar_snapshot_sha:
                reasons.append("metar_snapshot_sha_changed")
        elif current_metar_snapshot_sha and intent.metar_snapshot_sha and current_metar_snapshot_sha != intent.metar_snapshot_sha:
            reasons.append("metar_snapshot_sha_changed")

        current_station_payload = latest_by_station.get(intent.settlement_station)
        if not isinstance(current_station_payload, dict):
            current_station_payload = {}
        current_obs_time = _parse_ts(current_station_payload.get("observation_time_utc"))
        intent_obs_time = _parse_ts(intent.metar_observation_time_utc)
        if current_obs_time is not None and intent_obs_time is not None and current_obs_time > intent_obs_time:
            reasons.append("metar_observation_advanced")

        if reasons:
            invalid.append(
                {
                    "intent_id": intent.intent_id,
                    "market_ticker": intent.market_ticker,
                    "underlying_key": intent.underlying_key,
                    "reason": reasons[0],
                    "reasons": reasons,
                    "intent_market_snapshot_seq": intent.market_snapshot_seq,
                    "current_market_snapshot_seq": current_seq,
                    "intent_metar_snapshot_sha": intent.metar_snapshot_sha,
                    "current_metar_snapshot_sha": current_metar_snapshot_sha,
                    "intent_spec_hash": intent.spec_hash,
                    "current_spec_hash": current_spec_hash,
                }
            )
            continue

        valid.append(intent)

    revalidation_meta = {
        "specs_csv_used": str(specs_path) if specs_path is not None else "",
        "metar_summary_json_used": _normalize_text(metar_context.get("summary_path")),
        "metar_state_json_used": _normalize_text(metar_context.get("state_path")),
        "ws_state_json_used": ws_path,
        "market_count": len(market_sequences),
        "metar_snapshot_sha": current_metar_snapshot_sha,
    }
    return valid, invalid, revalidation_meta


def _build_order_group_id(*, metar_snapshot_sha: str, captured_at: datetime) -> str:
    normalized = _normalize_text(metar_snapshot_sha)
    if normalized:
        return f"temp-{normalized[:20]}"
    return f"temp-{captured_at.strftime('%Y%m%d%H%M%S')}"


def _build_deterministic_client_order_id(
    *,
    intent: TemperatureTradeIntent,
    order_group_id: str,
) -> str:
    seq = intent.market_snapshot_seq if intent.market_snapshot_seq is not None else 0
    raw = "|".join(
        (
            intent.intent_id,
            intent.policy_version,
            intent.market_ticker,
            intent.side,
            str(seq),
            _normalize_text(order_group_id),
        )
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"temp-{intent.intent_id}-{digest}"


def _write_intents_csv(
    *,
    path: Path,
    intents: list[TemperatureTradeIntent],
    decisions_by_id: dict[str, TemperaturePolicyDecision],
    revalidation_by_id: dict[str, dict[str, Any]] | None = None,
) -> None:
    fieldnames = [
        "intent_id",
        "captured_at",
        "policy_version",
        "underlying_key",
        "series_ticker",
        "event_ticker",
        "market_ticker",
        "market_title",
        "settlement_station",
        "settlement_timezone",
        "target_date_local",
        "constraint_status",
        "constraint_reason",
        "side",
        "max_entry_price_dollars",
        "intended_contracts",
        "settlement_confidence_score",
        "observed_max_settlement_quantized",
        "close_time",
        "hours_to_close",
        "spec_hash",
        "metar_snapshot_sha",
        "metar_observation_time_utc",
        "metar_observation_age_minutes",
        "market_snapshot_seq",
        "policy_approved",
        "policy_reason",
        "policy_notes",
        "revalidation_status",
        "revalidation_reason",
        "revalidation_reasons",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for intent in intents:
            row = intent.to_row()
            decision = decisions_by_id.get(intent.intent_id)
            row["policy_approved"] = bool(decision.approved) if decision else False
            row["policy_reason"] = decision.decision_reason if decision else "missing_decision"
            row["policy_notes"] = decision.decision_notes if decision else ""
            revalidation = revalidation_by_id.get(intent.intent_id) if isinstance(revalidation_by_id, dict) else None
            if isinstance(revalidation, dict):
                row["revalidation_status"] = "invalidated"
                row["revalidation_reason"] = _normalize_text(revalidation.get("reason"))
                reasons = revalidation.get("reasons")
                row["revalidation_reasons"] = ",".join([str(item) for item in reasons]) if isinstance(reasons, list) else ""
            else:
                row["revalidation_status"] = "approved"
                row["revalidation_reason"] = ""
                row["revalidation_reasons"] = ""
            writer.writerow(row)


def _write_plan_csv(path: Path, plans: list[dict[str, Any]]) -> None:
    fieldnames = [
        "plan_rank",
        "category",
        "market_ticker",
        "canonical_ticker",
        "canonical_niche",
        "contract_family",
        "source_strategy",
        "side",
        "contracts_per_order",
        "hours_to_close",
        "confidence",
        "maker_entry_price_dollars",
        "maker_yes_price_dollars",
        "maker_entry_edge_conservative_net_total",
        "estimated_entry_cost_dollars",
        "estimated_entry_fee_dollars",
        "temperature_intent_id",
        "temperature_underlying_key",
        "temperature_policy_version",
        "temperature_spec_hash",
        "temperature_metar_snapshot_sha",
        "temperature_market_snapshot_seq",
        "temperature_client_order_id",
        "order_payload_preview",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for plan in plans:
            row = dict(plan)
            row["order_payload_preview"] = json.dumps(plan.get("order_payload_preview", {}), separators=(",", ":"))
            writer.writerow(row)


def _build_settlement_finalization_snapshot(
    *,
    intents: list[TemperatureTradeIntent],
    settlement_state_by_underlying: dict[str, dict[str, Any]],
    captured_at: datetime,
) -> dict[str, Any]:
    by_underlying: dict[str, dict[str, Any]] = {}
    for intent in intents:
        row = by_underlying.setdefault(
            intent.underlying_key,
            {
                "underlying_key": intent.underlying_key,
                "series_ticker": intent.series_ticker,
                "settlement_station": intent.settlement_station,
                "target_date_local": intent.target_date_local,
                "intent_count": 0,
                "market_tickers": [],
                "fast_truth_max_settlement_quantized": None,
                "state": "fast_truth_only",
                "finalization_status": "fast_truth_only",
                "allow_new_orders": True,
                "reason": "",
                "final_truth_value": "",
                "revision_id": "",
                "source": "",
                "updated_at": "",
            },
        )
        row["intent_count"] = int(row.get("intent_count") or 0) + 1
        market_tickers = row.get("market_tickers")
        if isinstance(market_tickers, list) and intent.market_ticker not in market_tickers:
            market_tickers.append(intent.market_ticker)
        observed = intent.observed_max_settlement_quantized
        current_max = _parse_float(row.get("fast_truth_max_settlement_quantized"))
        observed_val = _parse_float(observed)
        if isinstance(observed_val, float) and (current_max is None or observed_val > current_max):
            row["fast_truth_max_settlement_quantized"] = observed_val

    for underlying_key, row in by_underlying.items():
        settlement_entry = settlement_state_by_underlying.get(underlying_key, {})
        if not isinstance(settlement_entry, dict):
            settlement_entry = {}
        if settlement_entry:
            row["state"] = _normalize_text(settlement_entry.get("state")) or "fast_truth_only"
            row["finalization_status"] = _normalize_text(settlement_entry.get("finalization_status")) or row["state"]
            row["allow_new_orders"] = bool(settlement_entry.get("allow_new_orders", True))
            row["reason"] = _normalize_text(settlement_entry.get("reason"))
            final_truth_value = settlement_entry.get("final_truth_value")
            row["final_truth_value"] = final_truth_value if final_truth_value not in (None, "") else ""
            row["revision_id"] = _normalize_text(settlement_entry.get("revision_id"))
            row["source"] = _normalize_text(settlement_entry.get("source"))
            row["updated_at"] = _normalize_text(settlement_entry.get("updated_at"))
        elif isinstance(_parse_float(row.get("fast_truth_max_settlement_quantized")), float):
            row["state"] = "fast_truth_only"
            row["finalization_status"] = "intraday_unfinalized"

    state_counts: dict[str, int] = {}
    blocked_underlyings = 0
    for row in by_underlying.values():
        state = _normalize_text(row.get("state")) or "unknown"
        state_counts[state] = state_counts.get(state, 0) + 1
        if not bool(row.get("allow_new_orders", True)):
            blocked_underlyings += 1

    return {
        "captured_at": captured_at.isoformat(),
        "underlying_count": len(by_underlying),
        "blocked_underlyings": blocked_underlyings,
        "state_counts": dict(sorted(state_counts.items(), key=lambda item: (-item[1], item[0]))),
        "underlyings": list(by_underlying.values()),
    }


def run_kalshi_temperature_trader(
    *,
    env_file: str,
    output_dir: str = "outputs",
    specs_csv: str | None = None,
    constraint_csv: str | None = None,
    metar_summary_json: str | None = None,
    metar_state_json: str | None = None,
    ws_state_json: str | None = None,
    settlement_state_json: str | None = None,
    book_db_path: str | None = None,
    policy_version: str = "temperature_policy_v1",
    contracts_per_order: int = 1,
    max_orders: int = 3,
    max_markets: int = 100,
    timeout_seconds: float = 12.0,
    allow_live_orders: bool = False,
    intents_only: bool = False,
    min_settlement_confidence: float = 0.6,
    max_metar_age_minutes: float | None = 20.0,
    min_hours_to_close: float | None = 0.0,
    max_hours_to_close: float | None = 48.0,
    max_intents_per_underlying: int = 1,
    yes_max_entry_price_dollars: float = 0.95,
    no_max_entry_price_dollars: float = 0.95,
    require_market_snapshot_seq: bool = True,
    require_metar_snapshot_sha: bool = False,
    enforce_underlying_netting: bool = True,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    enforce_trade_gate: bool = False,
    enforce_ws_state_authority: bool = False,
    ws_state_max_age_seconds: float = 30.0,
    constraint_scan_runner: ConstraintScanRunner = run_kalshi_temperature_constraint_scan,
    micro_execute_runner: MicroExecuteRunner = run_kalshi_micro_execute,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    resolved_constraint_csv = _normalize_text(constraint_csv)
    constraint_scan_summary: dict[str, Any] = {}
    if not resolved_constraint_csv:
        constraint_scan_summary = constraint_scan_runner(
            specs_csv=specs_csv,
            output_dir=output_dir,
            timeout_seconds=timeout_seconds,
            max_markets=max_markets,
        )
        resolved_constraint_csv = _normalize_text(constraint_scan_summary.get("output_csv"))

    if not resolved_constraint_csv:
        return {
            "status": "missing_constraint_csv",
            "captured_at": captured_at.isoformat(),
            "constraint_scan_summary": constraint_scan_summary,
            "error": "Constraint scan output CSV unavailable.",
        }

    constraint_path = Path(resolved_constraint_csv)
    constraint_rows = _read_csv_rows(constraint_path)
    if not constraint_rows:
        return {
            "status": "no_constraint_rows",
            "captured_at": captured_at.isoformat(),
            "constraint_csv": str(constraint_path),
            "constraint_scan_summary": constraint_scan_summary,
            "error": "Constraint CSV is empty or missing.",
        }

    resolved_specs_csv = _resolve_specs_csv(
        explicit_specs_csv=specs_csv,
        constraint_rows=constraint_rows,
        output_dir=output_dir,
    )
    specs_path = Path(resolved_specs_csv) if resolved_specs_csv else None
    specs_rows = _read_csv_rows(specs_path) if specs_path is not None else []
    specs_by_ticker = _build_specs_by_ticker(specs_rows)
    netting_snapshot = _load_existing_underlying_netting_snapshot(
        output_dir=output_dir,
        book_db_path=book_db_path,
        specs_by_ticker=specs_by_ticker,
        contracts_per_order=max(1, int(contracts_per_order)),
    )
    existing_underlying_slots = (
        netting_snapshot.get("underlying_slots")
        if enforce_underlying_netting and isinstance(netting_snapshot.get("underlying_slots"), dict)
        else {}
    )
    settlement_state_by_underlying, settlement_state_meta = _load_settlement_state_by_underlying(
        output_dir=output_dir,
        settlement_state_json=settlement_state_json,
    )

    metar_context = _load_metar_context(
        output_dir=output_dir,
        metar_summary_json=metar_summary_json,
        metar_state_json=metar_state_json,
    )
    ws_path, market_sequences, ws_payload = _load_market_sequences(
        ws_state_json=ws_state_json,
        output_dir=output_dir,
    )

    intents = build_temperature_trade_intents(
        constraint_rows=constraint_rows,
        specs_by_ticker=specs_by_ticker,
        metar_context=metar_context,
        market_sequences=market_sequences,
        policy_version=policy_version,
        contracts_per_order=contracts_per_order,
        yes_max_entry_price_dollars=yes_max_entry_price_dollars,
        no_max_entry_price_dollars=no_max_entry_price_dollars,
        now=captured_at,
    )

    gate = TemperaturePolicyGate(
        min_settlement_confidence=float(min_settlement_confidence),
        max_metar_age_minutes=max_metar_age_minutes,
        min_hours_to_close=min_hours_to_close,
        max_hours_to_close=max_hours_to_close,
        max_intents_per_underlying=max(1, int(max_intents_per_underlying)),
        require_market_snapshot_seq=bool(require_market_snapshot_seq),
        require_metar_snapshot_sha=bool(require_metar_snapshot_sha),
    )
    decisions = gate.evaluate(
        intents=intents,
        existing_underlying_slots=existing_underlying_slots,
        settlement_state_by_underlying=settlement_state_by_underlying,
    )
    decisions_by_id = {decision.intent_id: decision for decision in decisions}
    approved_intents = [
        intent for intent in intents if decisions_by_id.get(intent.intent_id, None) and decisions_by_id[intent.intent_id].approved
    ]

    revalidated_intents, revalidation_invalidations, revalidation_meta = revalidate_temperature_trade_intents(
        intents=approved_intents,
        output_dir=output_dir,
        specs_csv=str(specs_path) if specs_path is not None else None,
        metar_summary_json=_normalize_text(metar_context.get("summary_path")) or metar_summary_json,
        metar_state_json=_normalize_text(metar_context.get("state_path")) or metar_state_json,
        ws_state_json=ws_path or ws_state_json,
        require_market_snapshot_seq=bool(require_market_snapshot_seq),
        require_metar_snapshot_sha=bool(require_metar_snapshot_sha),
    )
    revalidation_by_id = {
        _normalize_text(row.get("intent_id")): row
        for row in revalidation_invalidations
        if _normalize_text(row.get("intent_id"))
    }

    order_group_id = _build_order_group_id(
        metar_snapshot_sha=_normalize_text(metar_context.get("raw_sha256")),
        captured_at=captured_at,
    )
    bridge = TemperatureExecutionBridge(contracts_per_order=max(1, int(contracts_per_order)))
    capped_intents = revalidated_intents[: max(0, int(max_orders))]
    plans = [
        bridge.to_plan(intent=intent, rank=index + 1, order_group_id=order_group_id)
        for index, intent in enumerate(capped_intents)
    ]

    stamp = captured_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    intents_csv_path = out_dir / f"kalshi_temperature_trade_intents_{stamp}.csv"
    _write_intents_csv(
        path=intents_csv_path,
        intents=intents,
        decisions_by_id=decisions_by_id,
        revalidation_by_id=revalidation_by_id,
    )

    plan_csv_path = out_dir / f"kalshi_temperature_trade_plan_{stamp}.csv"
    _write_plan_csv(plan_csv_path, plans)
    finalization_snapshot = _build_settlement_finalization_snapshot(
        intents=intents,
        settlement_state_by_underlying=settlement_state_by_underlying,
        captured_at=captured_at,
    )
    finalization_snapshot_path = out_dir / f"kalshi_temperature_finalization_snapshot_{stamp}.json"
    finalization_snapshot_path.write_text(json.dumps(finalization_snapshot, indent=2), encoding="utf-8")

    policy_reason_counts: dict[str, int] = {}
    for decision in decisions:
        key = decision.decision_reason
        policy_reason_counts[key] = policy_reason_counts.get(key, 0) + 1

    bridge_plan_summary = {
        "captured_at": captured_at.isoformat(),
        "status": "ready" if plans else "no_candidates",
        "policy_version": policy_version,
        "constraint_csv": str(constraint_path),
        "specs_csv": str(specs_path) if specs_path is not None else "",
        "metar_summary_json": _normalize_text(metar_context.get("summary_path")),
        "metar_state_json": _normalize_text(metar_context.get("state_path")),
        "ws_state_json": ws_path,
        "settlement_state_json": _normalize_text(settlement_state_meta.get("settlement_state_json_used")),
        "settlement_state_loaded": bool(settlement_state_meta.get("loaded")),
        "settlement_state_entries": int(settlement_state_meta.get("entry_count") or 0),
        "enforce_underlying_netting": bool(enforce_underlying_netting),
        "underlying_netting_book_db_path": _normalize_text(netting_snapshot.get("book_db_path")),
        "underlying_netting_loaded": bool(netting_snapshot.get("loaded")),
        "underlying_netting_error": _normalize_text(netting_snapshot.get("error")),
        "existing_underlying_slots_count": len(existing_underlying_slots) if isinstance(existing_underlying_slots, dict) else 0,
        "existing_underlying_slots": existing_underlying_slots if isinstance(existing_underlying_slots, dict) else {},
        "finalization_snapshot_file": str(finalization_snapshot_path),
        "finalization_state_counts": finalization_snapshot.get("state_counts"),
        "finalization_blocked_underlyings": finalization_snapshot.get("blocked_underlyings"),
        "order_group_id": order_group_id,
        "intents_total": len(intents),
        "intents_approved": len(approved_intents),
        "intents_revalidated": len(revalidated_intents),
        "revalidation_invalidated": len(revalidation_invalidations),
        "revalidation_invalidations": revalidation_invalidations[:100],
        "revalidation_meta": revalidation_meta,
        "intents_selected_for_plan": len(capped_intents),
        "policy_reason_counts": dict(sorted(policy_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
        "planned_orders": len(plans),
        "total_planned_cost_dollars": round(
            sum(_parse_float(plan.get("estimated_entry_cost_dollars")) or 0.0 for plan in plans),
            4,
        ),
        "orders": plans,
        "output_csv": str(plan_csv_path),
    }
    bridge_plan_summary_path = out_dir / f"kalshi_temperature_trade_plan_summary_{stamp}.json"
    bridge_plan_summary_path.write_text(json.dumps(bridge_plan_summary, indent=2), encoding="utf-8")
    bridge_plan_summary["output_file"] = str(bridge_plan_summary_path)

    intents_summary = {
        "captured_at": captured_at.isoformat(),
        "status": "ready",
        "constraint_csv": str(constraint_path),
        "specs_csv": str(specs_path) if specs_path is not None else "",
        "metar_summary_json": _normalize_text(metar_context.get("summary_path")),
        "metar_state_json": _normalize_text(metar_context.get("state_path")),
        "metar_snapshot_sha": _normalize_text(metar_context.get("raw_sha256")),
        "ws_state_json": ws_path,
        "settlement_state_json": _normalize_text(settlement_state_meta.get("settlement_state_json_used")),
        "settlement_state_loaded": bool(settlement_state_meta.get("loaded")),
        "settlement_state_entries": int(settlement_state_meta.get("entry_count") or 0),
        "enforce_underlying_netting": bool(enforce_underlying_netting),
        "underlying_netting_book_db_path": _normalize_text(netting_snapshot.get("book_db_path")),
        "underlying_netting_loaded": bool(netting_snapshot.get("loaded")),
        "underlying_netting_error": _normalize_text(netting_snapshot.get("error")),
        "existing_underlying_slots_count": len(existing_underlying_slots) if isinstance(existing_underlying_slots, dict) else 0,
        "existing_underlying_slots": existing_underlying_slots if isinstance(existing_underlying_slots, dict) else {},
        "ws_market_count": len(market_sequences),
        "intents_total": len(intents),
        "intents_approved": len(approved_intents),
        "intents_revalidated": len(revalidated_intents),
        "revalidation_invalidated": len(revalidation_invalidations),
        "intents_blocked": max(0, len(intents) - len(approved_intents)),
        "policy_reason_counts": bridge_plan_summary["policy_reason_counts"],
        "revalidation_meta": revalidation_meta,
        "revalidation_invalidations": revalidation_invalidations[:100],
        "output_csv": str(intents_csv_path),
        "finalization_snapshot_file": str(finalization_snapshot_path),
        "finalization_state_counts": finalization_snapshot.get("state_counts"),
        "finalization_blocked_underlyings": finalization_snapshot.get("blocked_underlyings"),
        "top_approved": [intent.to_row() for intent in revalidated_intents[:20]],
    }
    intents_summary_path = out_dir / f"kalshi_temperature_trade_intents_summary_{stamp}.json"
    intents_summary_path.write_text(json.dumps(intents_summary, indent=2), encoding="utf-8")
    intents_summary["output_file"] = str(intents_summary_path)

    if intents_only:
        return {
            "status": "intents_only",
            "captured_at": captured_at.isoformat(),
            "constraint_scan_summary": constraint_scan_summary,
            "intent_summary": intents_summary,
            "plan_summary": bridge_plan_summary,
            "ws_state_status": _normalize_text((ws_payload.get("summary") or {}).get("status")),
        }

    synthetic_plan_summary = {
        "status": "ready" if plans else "no_candidates",
        "planned_orders": len(plans),
        "total_planned_cost_dollars": bridge_plan_summary["total_planned_cost_dollars"],
        "actual_live_balance_dollars": None,
        "actual_live_balance_source": "unknown",
        "funding_gap_dollars": None,
        "board_warning": None,
        "output_file": str(bridge_plan_summary_path),
        "output_csv": str(plan_csv_path),
        "orders": plans,
    }

    def _synthetic_plan_runner(**_: Any) -> dict[str, Any]:
        return dict(synthetic_plan_summary)

    execute_summary = micro_execute_runner(
        env_file=env_file,
        output_dir=output_dir,
        planning_bankroll_dollars=planning_bankroll_dollars,
        daily_risk_cap_dollars=daily_risk_cap_dollars,
        contracts_per_order=max(1, int(contracts_per_order)),
        max_orders=max(1, int(max_orders)),
        timeout_seconds=timeout_seconds,
        allow_live_orders=allow_live_orders,
        cancel_resting_immediately=cancel_resting_immediately,
        resting_hold_seconds=resting_hold_seconds,
        max_live_submissions_per_day=max_live_submissions_per_day,
        max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
        enforce_trade_gate=enforce_trade_gate,
        enforce_ws_state_authority=enforce_ws_state_authority,
        ws_state_json=ws_path,
        ws_state_max_age_seconds=ws_state_max_age_seconds,
        order_group_auto_create=bool(allow_live_orders),
        plan_runner=_synthetic_plan_runner,
        now=captured_at,
    )

    return {
        "status": _normalize_text(execute_summary.get("status")) or ("ready" if plans else "no_candidates"),
        "captured_at": captured_at.isoformat(),
        "constraint_scan_summary": constraint_scan_summary,
        "intent_summary": intents_summary,
        "plan_summary": bridge_plan_summary,
        "execute_summary": execute_summary,
    }


def run_kalshi_temperature_shadow_watch(
    *,
    env_file: str,
    output_dir: str = "outputs",
    loops: int = 1,
    sleep_between_loops_seconds: float = 60.0,
    allow_live_orders: bool = False,
    specs_csv: str | None = None,
    constraint_csv: str | None = None,
    metar_summary_json: str | None = None,
    metar_state_json: str | None = None,
    ws_state_json: str | None = None,
    settlement_state_json: str | None = None,
    book_db_path: str | None = None,
    policy_version: str = "temperature_policy_v1",
    contracts_per_order: int = 1,
    max_orders: int = 3,
    max_markets: int = 100,
    timeout_seconds: float = 12.0,
    min_settlement_confidence: float = 0.6,
    max_metar_age_minutes: float | None = 20.0,
    min_hours_to_close: float | None = 0.0,
    max_hours_to_close: float | None = 48.0,
    max_intents_per_underlying: int = 1,
    yes_max_entry_price_dollars: float = 0.95,
    no_max_entry_price_dollars: float = 0.95,
    require_market_snapshot_seq: bool = True,
    require_metar_snapshot_sha: bool = False,
    enforce_underlying_netting: bool = True,
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    cancel_resting_immediately: bool = False,
    resting_hold_seconds: float = 0.0,
    max_live_submissions_per_day: int = 3,
    max_live_cost_per_day_dollars: float = 3.0,
    enforce_trade_gate: bool = False,
    enforce_ws_state_authority: bool = False,
    ws_state_max_age_seconds: float = 30.0,
    trader_runner: Callable[..., dict[str, Any]] = run_kalshi_temperature_trader,
    sleep_fn: SleepFn = time.sleep,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_loops = int(loops)
    run_forever = safe_loops == 0
    if safe_loops < 0:
        safe_loops = 1
    if not run_forever:
        safe_loops = max(1, safe_loops)

    cycle_summaries: list[dict[str, Any]] = []
    run_index = 0
    while run_forever or run_index < safe_loops:
        cycle_now = captured_at if run_index == 0 and now is not None else datetime.now(timezone.utc)
        cycle_summary = trader_runner(
            env_file=env_file,
            output_dir=output_dir,
            specs_csv=specs_csv,
            constraint_csv=constraint_csv,
            metar_summary_json=metar_summary_json,
            metar_state_json=metar_state_json,
            ws_state_json=ws_state_json,
            settlement_state_json=settlement_state_json,
            book_db_path=book_db_path,
            policy_version=policy_version,
            contracts_per_order=contracts_per_order,
            max_orders=max_orders,
            max_markets=max_markets,
            timeout_seconds=timeout_seconds,
            allow_live_orders=allow_live_orders,
            intents_only=False,
            min_settlement_confidence=min_settlement_confidence,
            max_metar_age_minutes=max_metar_age_minutes,
            min_hours_to_close=min_hours_to_close,
            max_hours_to_close=max_hours_to_close,
            max_intents_per_underlying=max_intents_per_underlying,
            yes_max_entry_price_dollars=yes_max_entry_price_dollars,
            no_max_entry_price_dollars=no_max_entry_price_dollars,
            require_market_snapshot_seq=require_market_snapshot_seq,
            require_metar_snapshot_sha=require_metar_snapshot_sha,
            enforce_underlying_netting=enforce_underlying_netting,
            planning_bankroll_dollars=planning_bankroll_dollars,
            daily_risk_cap_dollars=daily_risk_cap_dollars,
            cancel_resting_immediately=cancel_resting_immediately,
            resting_hold_seconds=resting_hold_seconds,
            max_live_submissions_per_day=max_live_submissions_per_day,
            max_live_cost_per_day_dollars=max_live_cost_per_day_dollars,
            enforce_trade_gate=enforce_trade_gate,
            enforce_ws_state_authority=enforce_ws_state_authority,
            ws_state_max_age_seconds=ws_state_max_age_seconds,
            now=cycle_now,
        )
        cycle_summaries.append(
            {
                "cycle_index": run_index + 1,
                "captured_at": cycle_now.isoformat(),
                "status": _normalize_text(cycle_summary.get("status")),
                "execute_status": _normalize_text(
                    (cycle_summary.get("execute_summary") if isinstance(cycle_summary.get("execute_summary"), dict) else {}).get("status")
                ),
                "intents_total": _parse_int(
                    (cycle_summary.get("intent_summary") if isinstance(cycle_summary.get("intent_summary"), dict) else {}).get("intents_total")
                ),
                "intents_approved": _parse_int(
                    (cycle_summary.get("intent_summary") if isinstance(cycle_summary.get("intent_summary"), dict) else {}).get("intents_approved")
                ),
                "intents_revalidated": _parse_int(
                    (cycle_summary.get("intent_summary") if isinstance(cycle_summary.get("intent_summary"), dict) else {}).get("intents_revalidated")
                ),
                "revalidation_invalidated": _parse_int(
                    (cycle_summary.get("intent_summary") if isinstance(cycle_summary.get("intent_summary"), dict) else {}).get("revalidation_invalidated")
                ),
                "planned_orders": _parse_int(
                    (cycle_summary.get("plan_summary") if isinstance(cycle_summary.get("plan_summary"), dict) else {}).get("planned_orders")
                ),
                "summary_file": _normalize_text(
                    (cycle_summary.get("execute_summary") if isinstance(cycle_summary.get("execute_summary"), dict) else {}).get("output_file")
                ),
            }
        )

        run_index += 1
        if run_forever or run_index < safe_loops:
            sleep_fn(max(0.0, float(sleep_between_loops_seconds)))

    status_counts: dict[str, int] = {}
    for cycle in cycle_summaries:
        key = _normalize_text(cycle.get("status")) or "unknown"
        status_counts[key] = status_counts.get(key, 0) + 1

    summary = {
        "captured_at": captured_at.isoformat(),
        "status": "ready",
        "mode": "live" if allow_live_orders else "shadow",
        "loops_requested": loops,
        "loops_run": run_index,
        "sleep_between_loops_seconds": float(sleep_between_loops_seconds),
        "cycle_status_counts": dict(sorted(status_counts.items(), key=lambda item: (-item[1], item[0]))),
        "cycle_summaries": cycle_summaries,
    }
    stamp = captured_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"kalshi_temperature_shadow_watch_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
