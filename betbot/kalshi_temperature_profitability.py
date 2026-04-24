from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
from typing import Any

from betbot.kalshi_execution_journal import default_execution_journal_db_path


_TEMPERATURE_SOURCE_STRATEGY = "temperature_constraints"
_TEMPERATURE_TICKER_PREFIXES = (
    "KXHIGH",
    "KXLOW",
    "KXTEMP",
    "NHIGH",
    "NLOW",
    "NTEMP",
)
_TEMPERATURE_TICKER_DATE_TOKEN_RE = re.compile(r"-\d{2}[A-Z]{3}\d{2,4}(?:-|$)")
_ORDER_EVENT_TYPES = ("order_submitted", "partial_fill", "full_fill", "settlement_outcome")
_EDGE_FIELD_CANDIDATES = (
    "maker_entry_edge_conservative_net_total",
    "maker_entry_edge_net_total",
    "maker_entry_edge_conservative_net_fees",
    "maker_entry_edge_net_fees",
    "maker_entry_edge_conservative",
    "maker_entry_edge",
)
_COST_FIELD_CANDIDATES = ("estimated_entry_cost_dollars", "cost_dollars")
_PLAN_POLICY_VERSION_CANDIDATES = (
    "temperature_policy_version",
    "policy_version",
    "temperature_model_version",
    "model_version",
)
_SIGNAL_BUCKET_FIELD_CANDIDATES = (
    "signal_bucket",
    "constraint_status",
    "signal_type",
    "constraint_bucket",
    "policy_constraint_status",
)
_METAR_OBSERVATION_AGE_FIELD_CANDIDATES = (
    "metar_observation_age_minutes",
    "temperature_metar_observation_age_minutes",
    "policy_metar_observation_age_minutes",
)
_METAR_MAX_AGE_FIELD_CANDIDATES = (
    "policy_metar_max_age_minutes_applied",
    "metar_max_age_minutes_applied",
    "max_metar_age_minutes",
)
_RISK_OFF_NEGATIVE_REGIME_MIN_TRADES = 2
_RISK_OFF_NEGATIVE_REGIME_LOSS_SHARE_THRESHOLD = 0.65
_RISK_OFF_METAR_STRESS_MIN_EVALUABLE_TRADES = 2
_RISK_OFF_METAR_STRESS_MIN_STALE_TRADES = 2
_RISK_OFF_METAR_STRESS_NEGATIVE_SHARE_THRESHOLD = 0.6
_SETTLED_CSV_FIELDNAMES = [
    "order_key",
    "captured_at_utc",
    "market_ticker",
    "client_order_id",
    "realized_pnl_dollars",
    "expected_edge_dollars",
    "expected_cost_dollars",
    "realized_minus_expected_dollars",
    "outcome",
]
_STAMP_SUFFIX_RE = re.compile(r"(?:^|_)(\d{8}_\d{6})$")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _write_text_atomic(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{path.name}.tmp-{os.getpid()}-{datetime.now(timezone.utc).timestamp():.6f}"
    tmp_path = path.with_name(tmp_name)
    try:
        with tmp_path.open("w", encoding=encoding) as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
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


def _artifact_timestamp_utc(path: Path) -> datetime | None:
    match = _STAMP_SUFFIX_RE.search(path.stem)
    if not match:
        return None
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc)


def _artifact_epoch(path: Path) -> float:
    inferred = _artifact_timestamp_utc(path)
    if isinstance(inferred, datetime):
        return inferred.timestamp()
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return 0.0


def _read_json_payload(payload_text: str) -> dict[str, Any]:
    text = _normalize_text(payload_text)
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _extract_row_metric(row: dict[str, str], candidates: tuple[str, ...]) -> float:
    for key in candidates:
        parsed = _parse_float(row.get(key))
        if isinstance(parsed, float):
            return float(parsed)
    return 0.0


def _extract_row_metric_with_field(row: dict[str, str], candidates: tuple[str, ...]) -> tuple[float, str]:
    for key in candidates:
        parsed = _parse_float(row.get(key))
        if isinstance(parsed, float):
            return float(parsed), key
    return 0.0, ""


def _extract_optional_row_metric_with_field(
    row: dict[str, str], candidates: tuple[str, ...]
) -> tuple[float | None, str]:
    for key in candidates:
        parsed = _parse_float(row.get(key))
        if isinstance(parsed, float):
            return float(parsed), key
    return None, ""


def _extract_first_text(row: dict[str, str], candidates: tuple[str, ...]) -> str:
    for key in candidates:
        value = _normalize_text(row.get(key))
        if value:
            return value
    return ""


def _regime_hour_label(value: Any) -> str:
    parsed = _parse_float(value)
    if isinstance(parsed, float):
        hour = int(parsed)
        if 0 <= hour <= 23 and abs(parsed - hour) < 1e-9:
            return str(hour)
    text = _normalize_text(value)
    return text if text else "unknown"


def _regime_text_label(value: Any, *, uppercase: bool = False, lowercase: bool = False) -> str:
    text = _normalize_text(value)
    if not text:
        return "unknown"
    if uppercase:
        return text.upper()
    if lowercase:
        return text.lower()
    return text


def _build_temperature_regime_fields(
    *,
    expected: dict[str, Any] | None,
    event: dict[str, Any] | None,
) -> dict[str, str]:
    expected = expected if isinstance(expected, dict) else {}
    event = event if isinstance(event, dict) else {}
    settlement_station = _extract_first_text(
        expected,  # type: ignore[arg-type]
        (
            "settlement_station",
            "station",
            "policy_settlement_station",
        ),
    )
    side = _extract_first_text(
        event,  # type: ignore[arg-type]
        ("side",),
    ) or _extract_first_text(
        expected,  # type: ignore[arg-type]
        ("side",),
    )
    local_hour = _extract_first_text(
        expected,  # type: ignore[arg-type]
        (
            "policy_metar_local_hour",
            "metar_local_hour",
            "local_hour",
        ),
    )
    signal_bucket = _extract_first_text(
        expected,  # type: ignore[arg-type]
        _SIGNAL_BUCKET_FIELD_CANDIDATES,
    )
    return {
        "settlement_station": _regime_text_label(settlement_station, uppercase=True),
        "side": _regime_text_label(side, lowercase=True),
        "local_hour": _regime_hour_label(local_hour),
        "signal_bucket": _regime_text_label(signal_bucket, lowercase=True),
    }


def _event_market_ticker(event: dict[str, Any]) -> str:
    direct = _normalize_text(event.get("market_ticker"))
    if direct:
        return direct
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return ""

    def _payload_ticker(value: Any) -> str:
        if not isinstance(value, dict):
            return ""
        for key in ("market_ticker", "ticker"):
            candidate = _normalize_text(value.get(key))
            if candidate:
                return candidate
        return ""

    direct_payload = _payload_ticker(payload)
    if direct_payload:
        return direct_payload
    for nested_key in ("payload", "order", "request", "response", "data"):
        nested_payload = _payload_ticker(payload.get(nested_key))
        if nested_payload:
            return nested_payload
    return ""


def _event_client_order_id(event: dict[str, Any]) -> str:
    direct = _normalize_text(event.get("client_order_id"))
    if direct:
        return direct
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return ""

    def _payload_client_order_id(value: Any) -> str:
        if not isinstance(value, dict):
            return ""
        for key in ("client_order_id", "clientOrderId", "client_orderid"):
            candidate = _normalize_text(value.get(key))
            if candidate:
                return candidate
        return ""

    direct_payload = _payload_client_order_id(payload)
    if direct_payload:
        return direct_payload
    for nested_key in ("payload", "order", "request", "response", "data"):
        nested_payload = _payload_client_order_id(payload.get(nested_key))
        if nested_payload:
            return nested_payload
    return ""


def _event_source_strategy(event: dict[str, Any]) -> str:
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return ""

    def _payload_strategy(value: Any) -> str:
        if not isinstance(value, dict):
            return ""
        return _normalize_text(value.get("source_strategy")).lower()

    direct = _payload_strategy(payload)
    if direct:
        return direct
    for nested_key in ("payload", "order", "request", "response", "data"):
        nested = _payload_strategy(payload.get(nested_key))
        if nested:
            return nested
    return ""


def _is_temperature_ticker(ticker: Any) -> bool:
    normalized = _normalize_text(ticker).upper()
    if not normalized.startswith(_TEMPERATURE_TICKER_PREFIXES):
        return False
    # Require an explicit day/hour token in ticker (e.g. 26APR10 or 26APR1514)
    # so macro contracts like KXHIGHINFLATION-26DEC are excluded from
    # temperature execution/profitability accounting.
    return bool(_TEMPERATURE_TICKER_DATE_TOKEN_RE.search(normalized))


def _is_temperature_plan_row(row: dict[str, str]) -> bool:
    ticker_is_temperature = _is_temperature_ticker(row.get("market_ticker"))
    strategy = _normalize_text(row.get("source_strategy")).lower()
    if strategy:
        if strategy != _TEMPERATURE_SOURCE_STRATEGY:
            return False
        # Guard against mislabeled strategy rows leaking non-temperature
        # contracts into temperature profitability accounting.
        if ticker_is_temperature:
            return True
        client_order_id = _normalize_text(row.get("temperature_client_order_id") or row.get("client_order_id"))
        return not _normalize_text(row.get("market_ticker")) and client_order_id.startswith("temp-")
    client_order_id = _normalize_text(row.get("temperature_client_order_id") or row.get("client_order_id"))
    if client_order_id.startswith("temp-"):
        return ticker_is_temperature
    return ticker_is_temperature


def _load_expected_plan_rows(
    *,
    out_dir: Path,
    window_start: datetime,
    window_end: datetime,
) -> tuple[
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    list[Path],
    dict[str, Any],
]:
    plan_rows: list[dict[str, Any]] = []
    expected_by_client_order_id: dict[str, dict[str, Any]] = {}
    expected_by_client_market: dict[str, dict[str, Any]] = {}
    plan_files: list[Path] = []
    policy_version_counts: dict[str, int] = {}
    edge_field_counts: dict[str, int] = {}
    rows_missing_policy_version = 0
    cutoff_epoch = window_start.timestamp()
    window_end_epoch = window_end.timestamp()
    for path in sorted(
        out_dir.glob("kalshi_temperature_trade_plan_*.csv"),
        key=lambda candidate: (_artifact_epoch(candidate), str(candidate)),
    ):
        try:
            stat_mtime = float(path.stat().st_mtime)
        except OSError:
            continue
        inferred_ts = _artifact_timestamp_utc(path)
        inferred_epoch = inferred_ts.timestamp() if isinstance(inferred_ts, datetime) else stat_mtime
        # Trust embedded artifact timestamp for stamped files. Fall back to mtime
        # only for unstamped files.
        if isinstance(inferred_ts, datetime):
            in_window = cutoff_epoch <= inferred_epoch <= window_end_epoch
        else:
            in_window = cutoff_epoch <= stat_mtime <= window_end_epoch
        if not in_window:
            continue
        plan_files.append(path)
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                row_map = dict(row)
                if not _is_temperature_plan_row(row_map):
                    continue
                row_ts = (
                    _parse_ts(row_map.get("captured_at_utc"))
                    or _parse_ts(row_map.get("captured_at"))
                    or _parse_ts(row_map.get("planned_at_utc"))
                    or _parse_ts(row_map.get("planned_at"))
                    or inferred_ts
                )
                if not isinstance(row_ts, datetime):
                    row_ts = datetime.fromtimestamp(stat_mtime, tz=timezone.utc)
                row_epoch = row_ts.timestamp()
                if row_epoch < cutoff_epoch or row_epoch > window_end_epoch:
                    continue
                expected_edge_dollars, edge_field = _extract_row_metric_with_field(row_map, _EDGE_FIELD_CANDIDATES)
                expected_cost_dollars = _extract_row_metric(row_map, _COST_FIELD_CANDIDATES)
                metar_observation_age_minutes, _ = _extract_optional_row_metric_with_field(
                    row_map, _METAR_OBSERVATION_AGE_FIELD_CANDIDATES
                )
                metar_max_age_minutes_applied, _ = _extract_optional_row_metric_with_field(
                    row_map, _METAR_MAX_AGE_FIELD_CANDIDATES
                )
                client_order_id = _normalize_text(
                    row_map.get("temperature_client_order_id") or row_map.get("client_order_id")
                )
                policy_version = _extract_first_text(row_map, _PLAN_POLICY_VERSION_CANDIDATES)
                if policy_version:
                    policy_version_counts[policy_version] = int(policy_version_counts.get(policy_version, 0) + 1)
                else:
                    rows_missing_policy_version += 1
                edge_field_key = edge_field or "none"
                edge_field_counts[edge_field_key] = int(edge_field_counts.get(edge_field_key, 0) + 1)
                record = {
                    "source_file": str(path),
                    "market_ticker": _normalize_text(row_map.get("market_ticker")),
                    "client_order_id": client_order_id,
                    "expected_edge_dollars": expected_edge_dollars,
                    "expected_cost_dollars": expected_cost_dollars,
                    "policy_version": policy_version,
                    "edge_field": edge_field,
                    "settlement_station": _regime_text_label(
                        _extract_first_text(
                            row_map,  # type: ignore[arg-type]
                            (
                                "settlement_station",
                                "station",
                                "policy_settlement_station",
                            ),
                        ),
                        uppercase=True,
                    ),
                    "side": _regime_text_label(
                        _extract_first_text(
                            row_map,  # type: ignore[arg-type]
                            ("side",),
                        ),
                        lowercase=True,
                    ),
                    "local_hour": _regime_hour_label(
                        _extract_first_text(
                            row_map,  # type: ignore[arg-type]
                            (
                                "policy_metar_local_hour",
                                "metar_local_hour",
                                "local_hour",
                            ),
                        )
                    ),
                    "signal_bucket": _regime_text_label(
                        _extract_first_text(
                            row_map,  # type: ignore[arg-type]
                            _SIGNAL_BUCKET_FIELD_CANDIDATES,
                        ),
                        lowercase=True,
                    ),
                    "metar_observation_age_minutes": metar_observation_age_minutes,
                    "metar_max_age_minutes_applied": metar_max_age_minutes_applied,
                    "_record_epoch": row_epoch,
                }
                plan_rows.append(record)
                if client_order_id:
                    existing = expected_by_client_order_id.get(client_order_id)
                    existing_epoch = _parse_float(existing.get("_record_epoch")) if isinstance(existing, dict) else None
                    if not isinstance(existing_epoch, float) or row_epoch >= existing_epoch:
                        expected_by_client_order_id[client_order_id] = record
                    market_ticker = _normalize_text(record.get("market_ticker"))
                    if market_ticker:
                        client_market_key = f"{client_order_id}|{market_ticker.upper()}"
                        existing_market = expected_by_client_market.get(client_market_key)
                        existing_market_epoch = (
                            _parse_float(existing_market.get("_record_epoch")) if isinstance(existing_market, dict) else None
                        )
                        if not isinstance(existing_market_epoch, float) or row_epoch >= existing_market_epoch:
                            expected_by_client_market[client_market_key] = record
    sorted_policy_versions = dict(
        sorted(policy_version_counts.items(), key=lambda item: (-int(item[1]), str(item[0])))
    )
    sorted_edge_fields = dict(
        sorted(edge_field_counts.items(), key=lambda item: (-int(item[1]), str(item[0])))
    )
    warnings: list[str] = []
    if len(sorted_policy_versions) > 1:
        warnings.append("mixed_plan_policy_versions")
    if rows_missing_policy_version > 0:
        warnings.append("plan_rows_missing_policy_version")
    model_lineage = {
        "plan_policy_versions": sorted_policy_versions,
        "plan_rows_missing_policy_version": int(rows_missing_policy_version),
        "edge_field_usage": sorted_edge_fields,
        "mixed_plan_policy_versions": bool(len(sorted_policy_versions) > 1),
        "warnings": warnings,
    }
    return plan_rows, expected_by_client_order_id, expected_by_client_market, plan_files, model_lineage


def _build_expected_by_unique_market_ticker(plan_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in plan_rows:
        ticker_key = _normalize_text(row.get("market_ticker")).upper()
        if not ticker_key:
            continue
        grouped.setdefault(ticker_key, []).append(row)

    output: dict[str, dict[str, Any]] = {}
    for ticker_key, rows in grouped.items():
        unique_clients = {
            _normalize_text(row.get("client_order_id"))
            for row in rows
            if _normalize_text(row.get("client_order_id"))
        }
        if len(unique_clients) > 1:
            # Ambiguous ticker-level attribution: require client-id match.
            continue
        best = max(
            rows,
            key=lambda row: (
                _parse_float(row.get("_record_epoch")) or 0.0,
                _normalize_text(row.get("source_file")),
            ),
        )
        output[ticker_key] = best
    return output


def _order_key(event: dict[str, Any]) -> str:
    order_id = _normalize_text(event.get("exchange_order_id"))
    if order_id:
        return f"order:{order_id}"
    client_order_id = _event_client_order_id(event)
    if client_order_id:
        return f"client:{client_order_id}"
    ticker = _event_market_ticker(event)
    captured = _normalize_text(event.get("captured_at_utc"))
    return f"fallback:{ticker}:{captured}"


def _is_temperature_event(
    *,
    event: dict[str, Any],
    submitted_order_keys: set[str],
) -> bool:
    event_ticker = _event_market_ticker(event)
    ticker_is_temperature = _is_temperature_ticker(event_ticker)
    key = _order_key(event)
    if key in submitted_order_keys:
        return True
    client_order_id = _event_client_order_id(event)
    # Temperature client order IDs are workflow-scoped and remain reliable even
    # when ticker fields are missing from downstream execution events. If ticker
    # is present and explicitly non-temperature, do not include.
    if client_order_id.startswith("temp-") and (not event_ticker or ticker_is_temperature):
        return True
    source_strategy = _event_source_strategy(event)
    if source_strategy == _TEMPERATURE_SOURCE_STRATEGY and ticker_is_temperature:
        return True
    if (
        source_strategy == _TEMPERATURE_SOURCE_STRATEGY
        and not _event_market_ticker(event)
        and client_order_id.startswith("temp-")
    ):
        # Allow source-strategy-tagged temperature events with missing ticker
        # fields to be counted, as long as the client id uses temperature id
        # namespace. This prevents silent undercounting when journal writes
        # lose market_ticker.
        return True
    return ticker_is_temperature


def _load_execution_events(
    *,
    journal_path: Path,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    if not journal_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with sqlite3.connect(journal_path) as conn:
        conn.row_factory = sqlite3.Row
        sql = """
            SELECT
                event_id,
                captured_at_utc,
                event_type,
                market_ticker,
                event_family,
                side,
                client_order_id,
                exchange_order_id,
                realized_pnl_dollars,
                payload_json
            FROM execution_events
            WHERE event_type IN (?, ?, ?, ?)
            ORDER BY event_id ASC
        """
        raw_rows = conn.execute(
            sql,
            (
                _ORDER_EVENT_TYPES[0],
                _ORDER_EVENT_TYPES[1],
                _ORDER_EVENT_TYPES[2],
                _ORDER_EVENT_TYPES[3],
            ),
        ).fetchall()
    window_start_epoch = window_start.timestamp()
    window_end_epoch = window_end.timestamp()
    for row in raw_rows:
        item = dict(row)
        captured_ts = _parse_ts(item.get("captured_at_utc"))
        if not isinstance(captured_ts, datetime):
            continue
        captured_epoch = captured_ts.timestamp()
        if captured_epoch < window_start_epoch or captured_epoch > window_end_epoch:
            continue
        item["payload"] = _read_json_payload(_normalize_text(item.get("payload_json")))
        item["_captured_ts"] = captured_ts
        rows.append(item)
    rows.sort(
        key=lambda row: (
            row.get("_captured_ts").timestamp()
            if isinstance(row.get("_captured_ts"), datetime)
            else 0.0,
            int(row.get("event_id") or 0),
        )
    )
    for row in rows:
        row.pop("_captured_ts", None)
    return rows


def _write_settled_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = f".{path.name}.tmp-{os.getpid()}-{datetime.now(timezone.utc).timestamp():.6f}"
    tmp_path = path.with_name(tmp_name)
    try:
        with tmp_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=_SETTLED_CSV_FIELDNAMES)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in _SETTLED_CSV_FIELDNAMES})
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def _regime_group_seed(labels: dict[str, str]) -> dict[str, Any]:
    return {
        "settlement_station": labels.get("settlement_station", "unknown"),
        "side": labels.get("side", "unknown"),
        "local_hour": labels.get("local_hour", "unknown"),
        "signal_bucket": labels.get("signal_bucket", "unknown"),
        "trades": 0,
        "expected_edge_sum": 0.0,
        "realized_pnl_sum": 0.0,
        "wins": 0,
        "losses": 0,
        "pushes": 0,
    }


def _regime_group_update(group: dict[str, Any], row: dict[str, Any]) -> None:
    realized_pnl = _parse_float(row.get("realized_pnl_dollars"))
    if not isinstance(realized_pnl, float):
        return
    expected_edge = _parse_float(row.get("expected_edge_dollars"))
    group["trades"] = int(group.get("trades") or 0) + 1
    group["expected_edge_sum"] = float(group.get("expected_edge_sum") or 0.0) + float(expected_edge or 0.0)
    group["realized_pnl_sum"] = float(group.get("realized_pnl_sum") or 0.0) + float(realized_pnl)
    outcome = _normalize_text(row.get("outcome")).lower()
    if outcome == "win":
        group["wins"] = int(group.get("wins") or 0) + 1
    elif outcome == "loss":
        group["losses"] = int(group.get("losses") or 0) + 1
    elif outcome == "push":
        group["pushes"] = int(group.get("pushes") or 0) + 1


def _regime_group_finalize(group: dict[str, Any]) -> dict[str, Any]:
    trades = int(group.get("trades") or 0)
    expected_edge_sum = round(float(group.get("expected_edge_sum") or 0.0), 6)
    realized_pnl_sum = round(float(group.get("realized_pnl_sum") or 0.0), 6)
    realized_per_trade = round(realized_pnl_sum / trades, 6) if trades > 0 else None
    edge_realization_ratio = (
        round(realized_pnl_sum / expected_edge_sum, 6)
        if abs(expected_edge_sum) > 1e-12
        else None
    )
    win_rate = round(float(group.get("wins") or 0) / trades, 6) if trades > 0 else None
    return {
        "settlement_station": group.get("settlement_station", "unknown"),
        "side": group.get("side", "unknown"),
        "local_hour": group.get("local_hour", "unknown"),
        "signal_bucket": group.get("signal_bucket", "unknown"),
        "trades": trades,
        "expected_edge_sum": expected_edge_sum,
        "realized_pnl_sum": realized_pnl_sum,
        "realized_per_trade": realized_per_trade,
        "edge_realization_ratio": edge_realization_ratio,
        "win_rate": win_rate,
        "wins": int(group.get("wins") or 0),
        "losses": int(group.get("losses") or 0),
        "pushes": int(group.get("pushes") or 0),
    }


def _regime_group_rank_key(entry: dict[str, Any]) -> tuple[float, int, str]:
    return (
        float(entry.get("realized_pnl_sum") or 0.0),
        -int(entry.get("trades") or 0),
        _normalize_text(
            "|".join(
                [
                    _normalize_text(entry.get("settlement_station")),
                    _normalize_text(entry.get("side")),
                    _normalize_text(entry.get("local_hour")),
                    _normalize_text(entry.get("signal_bucket")),
                ]
            )
        ),
    )


def _aggregate_temperature_regimes(
    settled_rows: list[dict[str, Any]],
    *,
    minimum_trades_for_ranked_regimes: int,
) -> dict[str, Any]:
    dimension_groups: dict[str, dict[str, dict[str, Any]]] = {
        "settlement_station": {},
        "side": {},
        "local_hour": {},
        "signal_bucket": {},
    }
    combined_groups: dict[str, dict[str, Any]] = {}
    for row in settled_rows:
        if not isinstance(_parse_float(row.get("realized_pnl_dollars")), float):
            continue
        labels = {
            "settlement_station": _regime_text_label(row.get("settlement_station"), uppercase=True),
            "side": _regime_text_label(row.get("side"), lowercase=True),
            "local_hour": _regime_hour_label(row.get("local_hour")),
            "signal_bucket": _regime_text_label(row.get("signal_bucket"), lowercase=True),
        }
        for dimension in dimension_groups:
            value = labels[dimension]
            group = dimension_groups[dimension].setdefault(value, _regime_group_seed({dimension: value}))
            group[dimension] = value
            _regime_group_update(group, row)
        combined_key = "|".join(
            [
                labels["settlement_station"],
                labels["side"],
                labels["local_hour"],
                labels["signal_bucket"],
            ]
        )
        combined_group = combined_groups.setdefault(combined_key, _regime_group_seed(labels))
        _regime_group_update(combined_group, row)

    def _finalize_dimension_groups(groups: dict[str, dict[str, Any]]) -> dict[str, Any]:
        finalized: dict[str, Any] = {}
        for key, group in sorted(
            groups.items(),
            key=lambda item: (
                -int(item[1].get("trades") or 0),
                -float(item[1].get("realized_pnl_sum") or 0.0),
                _normalize_text(item[0]),
            ),
        ):
            finalized[key] = _regime_group_finalize(group)
        return finalized

    combined_entries = [
        {
            "regime_key": key,
            **_regime_group_finalize(group),
        }
        for key, group in combined_groups.items()
    ]
    combined_entries.sort(key=lambda entry: _regime_group_rank_key(entry))
    negative_regimes = [
        entry
        for entry in combined_entries
        if int(entry.get("trades") or 0) >= int(minimum_trades_for_ranked_regimes)
        and float(entry.get("realized_pnl_sum") or 0.0) < 0.0
    ]
    positive_regimes = [
        entry
        for entry in combined_entries
        if int(entry.get("trades") or 0) >= int(minimum_trades_for_ranked_regimes)
        and float(entry.get("realized_pnl_sum") or 0.0) > 0.0
    ]
    negative_regimes.sort(key=lambda entry: _regime_group_rank_key(entry))
    positive_regimes.sort(
        key=lambda entry: (-float(entry.get("realized_pnl_sum") or 0.0), -int(entry.get("trades") or 0))
    )

    return {
        "minimum_trades_for_ranked_regimes": int(minimum_trades_for_ranked_regimes),
        "dimension_regimes": {
            "settlement_station": _finalize_dimension_groups(dimension_groups["settlement_station"]),
            "side": _finalize_dimension_groups(dimension_groups["side"]),
            "local_hour": _finalize_dimension_groups(dimension_groups["local_hour"]),
            "signal_bucket": _finalize_dimension_groups(dimension_groups["signal_bucket"]),
        },
        "combined_regimes": {
            entry["regime_key"]: {k: v for k, v in entry.items() if k != "regime_key"}
            for entry in combined_entries
        },
        "top_negative_regimes": negative_regimes[:20],
        "top_positive_regimes": positive_regimes[:20],
    }


def _build_risk_off_diagnostics(
    *,
    regime_breakdown: dict[str, Any],
    settled_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    minimum_trades_for_ranked_regimes = int(regime_breakdown.get("minimum_trades_for_ranked_regimes") or 0)
    combined_regimes_raw = regime_breakdown.get("combined_regimes")
    ranked_negative_regimes: list[dict[str, Any]] = []
    if isinstance(combined_regimes_raw, dict):
        for raw_regime_key, raw_entry in combined_regimes_raw.items():
            if not isinstance(raw_entry, dict):
                continue
            trades = int(raw_entry.get("trades") or 0)
            realized_pnl_sum = round(float(raw_entry.get("realized_pnl_sum") or 0.0), 6)
            if trades < minimum_trades_for_ranked_regimes or realized_pnl_sum >= 0.0:
                continue
            regime_key = _normalize_text(raw_regime_key) or "|".join(
                [
                    _regime_text_label(raw_entry.get("settlement_station"), uppercase=True),
                    _regime_text_label(raw_entry.get("side"), lowercase=True),
                    _regime_hour_label(raw_entry.get("local_hour")),
                    _regime_text_label(raw_entry.get("signal_bucket"), lowercase=True),
                ]
            )
            ranked_negative_regimes.append(
                {
                    "regime_key": regime_key,
                    "settlement_station": _regime_text_label(raw_entry.get("settlement_station"), uppercase=True),
                    "side": _regime_text_label(raw_entry.get("side"), lowercase=True),
                    "local_hour": _regime_hour_label(raw_entry.get("local_hour")),
                    "signal_bucket": _regime_text_label(raw_entry.get("signal_bucket"), lowercase=True),
                    "trades": trades,
                    "realized_pnl_sum": realized_pnl_sum,
                }
            )
    ranked_negative_regimes.sort(
        key=lambda entry: (
            float(entry.get("realized_pnl_sum") or 0.0),
            -int(entry.get("trades") or 0),
            _normalize_text(entry.get("regime_key")),
        )
    )
    ranked_negative_trades = int(sum(int(entry.get("trades") or 0) for entry in ranked_negative_regimes))
    ranked_negative_abs_loss_dollars = round(
        sum(abs(float(entry.get("realized_pnl_sum") or 0.0)) for entry in ranked_negative_regimes),
        6,
    )
    worst_negative_regime = ranked_negative_regimes[0] if ranked_negative_regimes else {}
    worst_negative_trade_share = (
        round(float(worst_negative_regime.get("trades") or 0) / float(ranked_negative_trades), 6)
        if ranked_negative_trades > 0
        else None
    )
    worst_negative_loss_share = (
        round(
            abs(float(worst_negative_regime.get("realized_pnl_sum") or 0.0))
            / float(ranked_negative_abs_loss_dollars),
            6,
        )
        if ranked_negative_abs_loss_dollars > 0.0
        else None
    )
    negative_regime_concentration_triggered = bool(
        ranked_negative_trades >= _RISK_OFF_NEGATIVE_REGIME_MIN_TRADES
        and isinstance(worst_negative_loss_share, float)
        and worst_negative_loss_share >= _RISK_OFF_NEGATIVE_REGIME_LOSS_SHARE_THRESHOLD
    )

    metar_dimensions_available = False
    metar_evaluable_trades = 0
    metar_negative_evaluable_trades = 0
    metar_stale_trade_count = 0
    metar_stale_negative_trade_count = 0
    metar_stale_realized_pnl_sum = 0.0
    metar_fresh_realized_pnl_sum = 0.0
    for row in settled_rows:
        metar_age_minutes = _parse_float(row.get("metar_observation_age_minutes"))
        metar_max_age_minutes = _parse_float(row.get("metar_max_age_minutes_applied"))
        if isinstance(metar_age_minutes, float) or isinstance(metar_max_age_minutes, float):
            metar_dimensions_available = True
        realized_pnl = _parse_float(row.get("realized_pnl_dollars"))
        if (
            not isinstance(realized_pnl, float)
            or not isinstance(metar_age_minutes, float)
            or not isinstance(metar_max_age_minutes, float)
            or metar_max_age_minutes <= 0.0
        ):
            continue
        metar_evaluable_trades += 1
        if realized_pnl < 0.0:
            metar_negative_evaluable_trades += 1
        is_stale = metar_age_minutes > (metar_max_age_minutes + 1e-9)
        if is_stale:
            metar_stale_trade_count += 1
            metar_stale_realized_pnl_sum += realized_pnl
            if realized_pnl < 0.0:
                metar_stale_negative_trade_count += 1
        else:
            metar_fresh_realized_pnl_sum += realized_pnl

    metar_stale_trade_share = (
        round(float(metar_stale_trade_count) / float(metar_evaluable_trades), 6)
        if metar_evaluable_trades > 0
        else None
    )
    metar_stale_negative_share_of_negative_trades = (
        round(float(metar_stale_negative_trade_count) / float(metar_negative_evaluable_trades), 6)
        if metar_negative_evaluable_trades > 0
        else None
    )
    metar_stale_negative_share_of_stale_trades = (
        round(float(metar_stale_negative_trade_count) / float(metar_stale_trade_count), 6)
        if metar_stale_trade_count > 0
        else None
    )
    metar_stale_realized_per_trade = (
        round(float(metar_stale_realized_pnl_sum) / float(metar_stale_trade_count), 6)
        if metar_stale_trade_count > 0
        else None
    )
    metar_fresh_trade_count = max(0, metar_evaluable_trades - metar_stale_trade_count)
    metar_fresh_realized_per_trade = (
        round(float(metar_fresh_realized_pnl_sum) / float(metar_fresh_trade_count), 6)
        if metar_fresh_trade_count > 0
        else None
    )
    stale_metar_regime_stress_triggered = bool(
        metar_dimensions_available
        and metar_evaluable_trades >= _RISK_OFF_METAR_STRESS_MIN_EVALUABLE_TRADES
        and metar_stale_trade_count >= _RISK_OFF_METAR_STRESS_MIN_STALE_TRADES
        and isinstance(metar_stale_negative_share_of_negative_trades, float)
        and metar_stale_negative_share_of_negative_trades >= _RISK_OFF_METAR_STRESS_NEGATIVE_SHARE_THRESHOLD
    )

    temporary_risk_off_reason_codes: list[str] = []
    temporary_risk_off_reasons: list[str] = []
    if negative_regime_concentration_triggered:
        temporary_risk_off_reason_codes.append("negative_regime_concentration")
        temporary_risk_off_reasons.append(
            "Negative regime concentration breached "
            f"loss-share threshold ({worst_negative_loss_share} >= {_RISK_OFF_NEGATIVE_REGIME_LOSS_SHARE_THRESHOLD})."
        )
    if stale_metar_regime_stress_triggered:
        temporary_risk_off_reason_codes.append("stale_metar_regime_stress")
        temporary_risk_off_reasons.append(
            "Stale METAR regime stress breached "
            "negative-share threshold "
            f"({metar_stale_negative_share_of_negative_trades} >= {_RISK_OFF_METAR_STRESS_NEGATIVE_SHARE_THRESHOLD})."
        )

    return {
        "thresholds": {
            "negative_regime_min_trades": _RISK_OFF_NEGATIVE_REGIME_MIN_TRADES,
            "negative_regime_loss_share_threshold": _RISK_OFF_NEGATIVE_REGIME_LOSS_SHARE_THRESHOLD,
            "stale_metar_min_evaluable_trades": _RISK_OFF_METAR_STRESS_MIN_EVALUABLE_TRADES,
            "stale_metar_min_stale_trades": _RISK_OFF_METAR_STRESS_MIN_STALE_TRADES,
            "stale_metar_negative_share_threshold": _RISK_OFF_METAR_STRESS_NEGATIVE_SHARE_THRESHOLD,
        },
        "negative_regime_concentration": {
            "minimum_trades_for_ranked_regimes": minimum_trades_for_ranked_regimes,
            "ranked_negative_regime_count": len(ranked_negative_regimes),
            "ranked_negative_trades": ranked_negative_trades,
            "ranked_negative_abs_loss_dollars": ranked_negative_abs_loss_dollars,
            "worst_negative_regime": worst_negative_regime,
            "worst_negative_trade_share": worst_negative_trade_share,
            "worst_negative_loss_share": worst_negative_loss_share,
            "triggered": negative_regime_concentration_triggered,
        },
        "stale_metar_regime_stress": {
            "dimensions_available": metar_dimensions_available,
            "evaluable_trade_count": metar_evaluable_trades,
            "negative_evaluable_trade_count": metar_negative_evaluable_trades,
            "stale_trade_count": metar_stale_trade_count,
            "stale_negative_trade_count": metar_stale_negative_trade_count,
            "stale_trade_share": metar_stale_trade_share,
            "stale_negative_share_of_negative_trades": metar_stale_negative_share_of_negative_trades,
            "stale_negative_share_of_stale_trades": metar_stale_negative_share_of_stale_trades,
            "stale_realized_pnl_sum": round(metar_stale_realized_pnl_sum, 6),
            "stale_realized_per_trade": metar_stale_realized_per_trade,
            "fresh_realized_pnl_sum": round(metar_fresh_realized_pnl_sum, 6),
            "fresh_realized_per_trade": metar_fresh_realized_per_trade,
            "triggered": stale_metar_regime_stress_triggered,
        },
        "temporary_risk_off_recommended": bool(temporary_risk_off_reason_codes),
        "temporary_risk_off_reason_codes": temporary_risk_off_reason_codes,
        "temporary_risk_off_reason": "; ".join(temporary_risk_off_reasons),
    }


def default_trial_balance_state_path(output_dir: str = "outputs") -> Path:
    return Path(output_dir) / "checkpoints" / "trial_balance_state.json"


def run_kalshi_temperature_refill_trial_balance(
    *,
    output_dir: str = "outputs",
    starting_balance_dollars: float = 1000.0,
    reason: str = "manual_refill",
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)
    balance = float(starting_balance_dollars)
    if balance <= 0:
        raise ValueError("starting_balance_dollars must be > 0")

    state_path = default_trial_balance_state_path(output_dir)
    state_path.parent.mkdir(parents=True, exist_ok=True)

    previous: dict[str, Any] = {}
    if state_path.exists():
        try:
            previous = json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            previous = {}

    state_payload = {
        "starting_balance_dollars": round(balance, 6),
        "reset_epoch": float(captured_at.timestamp()),
        "reset_at_utc": captured_at.isoformat(),
        "reset_reason": _normalize_text(reason) or "manual_refill",
    }
    _write_text_atomic(
        state_path,
        json.dumps(state_payload, indent=2),
        encoding="utf-8",
    )

    summary = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "output_dir": str(Path(output_dir)),
        "state_file": str(state_path),
        "starting_balance_dollars": state_payload["starting_balance_dollars"],
        "reset_epoch": state_payload["reset_epoch"],
        "reset_at_utc": state_payload["reset_at_utc"],
        "reset_reason": state_payload["reset_reason"],
        "previous_state": previous,
        "notes": [
            "Checkpoint profitability windows (1d/7d/30d) now anchor from this reset timestamp.",
            "Use this command whenever you want to refill the trial balance for strategy A/B testing.",
        ],
    }
    return summary


def run_kalshi_temperature_profitability(
    *,
    output_dir: str = "outputs",
    hours: float = 24.0,
    journal_db_path: str | None = None,
    top_n: int = 20,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    captured_at = captured_at.astimezone(timezone.utc)
    safe_hours = max(0.0, float(hours))
    window_start = captured_at - timedelta(hours=safe_hours)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    plan_rows, expected_by_client_order_id, expected_by_client_market, plan_files, model_lineage = _load_expected_plan_rows(
        out_dir=out_dir,
        window_start=window_start,
        window_end=captured_at,
    )
    expected_by_unique_market_ticker = _build_expected_by_unique_market_ticker(plan_rows)
    planned_orders_total = len(plan_rows)
    expected_edge_total_dollars = round(sum(float(row.get("expected_edge_dollars") or 0.0) for row in plan_rows), 6)
    expected_cost_total_dollars = round(sum(float(row.get("expected_cost_dollars") or 0.0) for row in plan_rows), 6)
    expected_edge_per_order_dollars = (
        round(expected_edge_total_dollars / planned_orders_total, 6) if planned_orders_total > 0 else None
    )
    expected_roi_on_cost = (
        round(expected_edge_total_dollars / expected_cost_total_dollars, 6)
        if expected_cost_total_dollars > 0
        else None
    )

    journal_path = Path(journal_db_path) if journal_db_path else default_execution_journal_db_path(str(out_dir))
    events = _load_execution_events(
        journal_path=journal_path,
        window_start=window_start,
        window_end=captured_at,
    )

    submitted_orders: dict[str, dict[str, Any]] = {}
    submitted_by_client: dict[str, dict[str, Any]] = {}
    submitted_by_exchange: dict[str, dict[str, Any]] = {}
    client_by_exchange: dict[str, str] = {}
    exchange_by_client: dict[str, str] = {}
    filled_order_keys: set[str] = set()
    settled_orders: dict[str, dict[str, Any]] = {}
    # Pre-scan crosswalk so out-of-order event ingestion does not drop
    # settlement/fill events that rely on exchange<->client id linkage.
    for event in events:
        pre_client_order_id = _event_client_order_id(event)
        pre_exchange_order_id = _normalize_text(event.get("exchange_order_id"))
        if pre_client_order_id and pre_exchange_order_id:
            client_by_exchange[pre_exchange_order_id] = pre_client_order_id
            exchange_by_client[pre_client_order_id] = pre_exchange_order_id
    for event in events:
        event_client_order_id = _event_client_order_id(event)
        event_exchange_order_id = _normalize_text(event.get("exchange_order_id"))
        if event_exchange_order_id and not event_client_order_id:
            event_client_order_id = client_by_exchange.get(event_exchange_order_id, "")
        if event_client_order_id and not event_exchange_order_id:
            event_exchange_order_id = exchange_by_client.get(event_client_order_id, "")

        event_for_filter = dict(event)
        if event_client_order_id and not _normalize_text(event_for_filter.get("client_order_id")):
            event_for_filter["client_order_id"] = event_client_order_id
        if event_exchange_order_id and not _normalize_text(event_for_filter.get("exchange_order_id")):
            event_for_filter["exchange_order_id"] = event_exchange_order_id

        key = _order_key(event_for_filter)
        event_type = _normalize_text(event.get("event_type"))
        submitted_keys = set(submitted_orders.keys())
        is_temperature = _is_temperature_event(event=event_for_filter, submitted_order_keys=submitted_keys)
        if not is_temperature:
            continue
        if event_exchange_order_id and event_client_order_id:
            client_by_exchange[event_exchange_order_id] = event_client_order_id
            exchange_by_client[event_client_order_id] = event_exchange_order_id
        if event_type == "order_submitted":
            submitted_context = {
                "order_key": key,
                "market_ticker": _event_market_ticker(event_for_filter),
                "client_order_id": event_client_order_id,
                "exchange_order_id": event_exchange_order_id,
                "captured_at_utc": _normalize_text(event.get("captured_at_utc")),
            }
            submitted_orders[key] = submitted_context
            if event_client_order_id:
                submitted_by_client[event_client_order_id] = submitted_context
            if event_exchange_order_id:
                submitted_by_exchange[event_exchange_order_id] = submitted_context
        elif event_type in {"partial_fill", "full_fill"}:
            submitted_for_fill = (
                submitted_by_exchange.get(event_exchange_order_id, {})
                if event_exchange_order_id
                else {}
            )
            if (not isinstance(submitted_for_fill, dict) or not submitted_for_fill) and event_client_order_id:
                submitted_for_fill = submitted_by_client.get(event_client_order_id, {})
            canonical_fill_key = (
                _normalize_text(submitted_for_fill.get("order_key"))
                if isinstance(submitted_for_fill, dict)
                else ""
            )
            filled_order_keys.add(canonical_fill_key or key)
        elif event_type == "settlement_outcome":
            settled_orders[key] = event_for_filter

    settled_rows: list[dict[str, Any]] = []
    settled_with_numeric_pnl = 0
    wins = 0
    losses = 0
    pushes = 0
    realized_pnl_total_dollars = 0.0
    matched_settled_orders = 0
    matched_expected_edge_total_dollars = 0.0
    matched_expected_cost_total_dollars = 0.0
    matched_realized_pnl_total_dollars = 0.0

    for key, event in settled_orders.items():
        submitted = submitted_orders.get(key, {})
        settled_exchange_order_id = _normalize_text(event.get("exchange_order_id"))
        settled_client_order_id = _event_client_order_id(event)
        if settled_exchange_order_id and not settled_client_order_id:
            settled_client_order_id = client_by_exchange.get(settled_exchange_order_id, "")
        if settled_client_order_id and not settled_exchange_order_id:
            settled_exchange_order_id = exchange_by_client.get(settled_client_order_id, "")
        if (not isinstance(submitted, dict) or not submitted) and settled_exchange_order_id:
            submitted = submitted_by_exchange.get(settled_exchange_order_id, {})
        if (not isinstance(submitted, dict) or not submitted) and settled_client_order_id:
            submitted = submitted_by_client.get(settled_client_order_id, {})

        settled_market_ticker = _event_market_ticker(event) or _normalize_text(submitted.get("market_ticker"))
        settled_market_ticker_key = settled_market_ticker.upper()
        client_order_id = settled_client_order_id or _normalize_text(submitted.get("client_order_id"))
        expected: dict[str, Any] = {}
        if client_order_id and settled_market_ticker_key:
            expected = expected_by_client_market.get(f"{client_order_id}|{settled_market_ticker_key}", {})
        if not isinstance(expected, dict) or not expected:
            expected = expected_by_client_order_id.get(client_order_id, {})
        if isinstance(expected, dict) and expected and settled_market_ticker_key:
            expected_market_ticker = _normalize_text(expected.get("market_ticker"))
            if expected_market_ticker and expected_market_ticker.upper() != settled_market_ticker_key:
                expected = {}
        if (not isinstance(expected, dict) or not expected) and settled_market_ticker_key:
            expected = expected_by_unique_market_ticker.get(settled_market_ticker_key, {})
        realized_pnl = _parse_float(event.get("realized_pnl_dollars"))
        expected_edge = _parse_float(expected.get("expected_edge_dollars"))
        expected_cost = _parse_float(expected.get("expected_cost_dollars"))
        if isinstance(realized_pnl, float):
            settled_with_numeric_pnl += 1
            realized_pnl_total_dollars += realized_pnl
            if realized_pnl > 0:
                wins += 1
                outcome = "win"
            elif realized_pnl < 0:
                losses += 1
                outcome = "loss"
            else:
                pushes += 1
                outcome = "push"
        else:
            outcome = "unknown"

        realized_minus_expected = (
            round(realized_pnl - expected_edge, 6)
            if isinstance(realized_pnl, float) and isinstance(expected_edge, float)
            else None
        )
        if isinstance(realized_pnl, float) and isinstance(expected_edge, float):
            matched_settled_orders += 1
            matched_expected_edge_total_dollars += expected_edge
            matched_realized_pnl_total_dollars += realized_pnl
            matched_expected_cost_total_dollars += float(expected_cost or 0.0)

        regime_fields = _build_temperature_regime_fields(expected=expected, event=event)
        metar_observation_age_minutes = _parse_float(expected.get("metar_observation_age_minutes"))
        metar_max_age_minutes_applied = _parse_float(expected.get("metar_max_age_minutes_applied"))

        settled_rows.append(
            {
                "order_key": key,
                "captured_at_utc": _normalize_text(event.get("captured_at_utc")),
                "market_ticker": settled_market_ticker,
                "client_order_id": client_order_id,
                "settlement_station": regime_fields["settlement_station"],
                "side": regime_fields["side"],
                "local_hour": regime_fields["local_hour"],
                "signal_bucket": regime_fields["signal_bucket"],
                "realized_pnl_dollars": realized_pnl if isinstance(realized_pnl, float) else "",
                "expected_edge_dollars": expected_edge if isinstance(expected_edge, float) else "",
                "expected_cost_dollars": expected_cost if isinstance(expected_cost, float) else "",
                "realized_minus_expected_dollars": realized_minus_expected if isinstance(realized_minus_expected, float) else "",
                "metar_observation_age_minutes": (
                    metar_observation_age_minutes if isinstance(metar_observation_age_minutes, float) else ""
                ),
                "metar_max_age_minutes_applied": (
                    metar_max_age_minutes_applied if isinstance(metar_max_age_minutes_applied, float) else ""
                ),
                "outcome": outcome,
            }
        )

    settled_rows_sorted = sorted(
        settled_rows,
        key=lambda row: abs(_parse_float(row.get("realized_pnl_dollars")) or 0.0),
        reverse=True,
    )
    regime_breakdown = _aggregate_temperature_regimes(
        settled_rows,
        minimum_trades_for_ranked_regimes=2,
    )
    risk_off_diagnostics = _build_risk_off_diagnostics(
        regime_breakdown=regime_breakdown,
        settled_rows=settled_rows,
    )

    win_rate = round(wins / settled_with_numeric_pnl, 6) if settled_with_numeric_pnl > 0 else None
    loss_rate = round(losses / settled_with_numeric_pnl, 6) if settled_with_numeric_pnl > 0 else None
    push_rate = round(pushes / settled_with_numeric_pnl, 6) if settled_with_numeric_pnl > 0 else None
    expected_vs_realized_delta = round(
        matched_realized_pnl_total_dollars - matched_expected_edge_total_dollars, 6
    )
    orders_submitted_total = len(submitted_orders)
    orders_with_fills_total = len([key for key in filled_order_keys if key in submitted_orders or key in settled_orders])
    orders_settled_total = len(settled_orders)
    expected_edge_density = expected_edge_per_order_dollars
    submission_conversion = (
        round(orders_submitted_total / planned_orders_total, 6) if planned_orders_total > 0 else None
    )
    fill_conversion = (
        round(orders_with_fills_total / orders_submitted_total, 6) if orders_submitted_total > 0 else None
    )
    if orders_with_fills_total > 0:
        settlement_conversion = round(orders_settled_total / orders_with_fills_total, 6)
    elif orders_submitted_total > 0:
        settlement_conversion = round(orders_settled_total / orders_submitted_total, 6)
    else:
        settlement_conversion = None
    execution_gap = {
        "submission": bool(planned_orders_total > 0 and isinstance(submission_conversion, float) and submission_conversion <= 1e-9),
        "fill": bool(orders_submitted_total > 0 and isinstance(fill_conversion, float) and fill_conversion <= 1e-9),
        "settlement": bool(orders_with_fills_total > 0 and isinstance(settlement_conversion, float) and settlement_conversion <= 1e-9),
    }

    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    settled_csv_path = out_dir / f"kalshi_temperature_profitability_settled_{stamp}.csv"
    _write_settled_csv(settled_csv_path, settled_rows_sorted)

    summary = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "window_hours": safe_hours,
        "window_start_utc": window_start.isoformat(),
        "window_end_utc": captured_at.isoformat(),
        "output_dir": str(out_dir),
        "plan_csv_files_scanned": len(plan_files),
        "planned_orders_total": planned_orders_total,
        "expected_edge_total_dollars": expected_edge_total_dollars,
        "expected_cost_total_dollars": expected_cost_total_dollars,
        "expected_edge_per_order_dollars": expected_edge_per_order_dollars,
        "expected_roi_on_cost": expected_roi_on_cost,
        "prelive_calibration": {
            "planned_orders_total": planned_orders_total,
            "orders_submitted": orders_submitted_total,
            "orders_with_fills": orders_with_fills_total,
            "orders_settled": orders_settled_total,
            "expected_edge_density": expected_edge_density,
            "submission_conversion": submission_conversion,
            "fill_conversion": fill_conversion,
            "settlement_conversion": settlement_conversion,
            "execution_gap": execution_gap,
        },
        "journal_db_path": str(journal_path),
        "journal_events_scanned": len(events),
        "orders_submitted": orders_submitted_total,
        "orders_with_fills": orders_with_fills_total,
        "orders_settled": orders_settled_total,
        "orders_settled_with_numeric_pnl": settled_with_numeric_pnl,
        "realized_pnl_total_dollars": round(realized_pnl_total_dollars, 6),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": win_rate,
        "loss_rate": loss_rate,
        "push_rate": push_rate,
        "expected_vs_realized": {
            "matched_settled_orders": matched_settled_orders,
            "unmatched_settled_orders": max(0, settled_with_numeric_pnl - matched_settled_orders),
            "matched_expected_edge_total_dollars": round(matched_expected_edge_total_dollars, 6),
            "matched_expected_cost_total_dollars": round(matched_expected_cost_total_dollars, 6),
            "matched_realized_pnl_total_dollars": round(matched_realized_pnl_total_dollars, 6),
            "matched_realized_minus_expected_dollars": expected_vs_realized_delta,
            "comparability_warning": (
                "Expected-vs-realized comparison includes mixed plan policy versions."
                if bool(model_lineage.get("mixed_plan_policy_versions"))
                else ""
            ),
        },
        "regime_breakdown": regime_breakdown,
        "top_negative_regimes": regime_breakdown.get("top_negative_regimes", []),
        "top_positive_regimes": regime_breakdown.get("top_positive_regimes", []),
        "risk_off_diagnostics": risk_off_diagnostics,
        "model_lineage": model_lineage,
        "top_settled_orders": settled_rows_sorted[: max(0, int(top_n))],
        "notes": [
            "Expected metrics come from temperature trade-plan CSV rows in the selected window.",
            "Realized metrics come from execution_journal settlement_outcome events tagged to temperature strategy.",
            "Win rate requires settled orders; dry-run windows can show zero settled outcomes.",
            "Model lineage block shows policy-version/edge-field composition to avoid mixed-era misreads.",
            "Risk-off diagnostics summarize concentration and stale-METAR stress for temporary hard gating.",
        ],
        "output_csv": str(settled_csv_path),
    }
    summary_path = out_dir / f"kalshi_temperature_profitability_summary_{stamp}.json"
    _write_text_atomic(
        summary_path,
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    summary["output_file"] = str(summary_path)
    return summary
