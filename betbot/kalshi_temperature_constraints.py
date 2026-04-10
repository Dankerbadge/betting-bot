from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_weather_intraday import build_intraday_temperature_snapshot


CONSTRAINT_SCAN_FIELDNAMES = [
    "scanned_at",
    "source_specs_csv",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "market_title",
    "settlement_station",
    "settlement_timezone",
    "target_date_local",
    "settlement_unit",
    "settlement_precision",
    "threshold_expression",
    "constraint_status",
    "constraint_reason",
    "observed_max_settlement_raw",
    "observed_max_settlement_quantized",
    "observations_for_date",
    "snapshot_status",
    "settlement_confidence_score",
]


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_threshold_expression(value: Any) -> tuple[str, list[float]]:
    text = _normalize_text(value)
    if not text or ":" not in text:
        return ("", [])
    parts = [item.strip() for item in text.split(":")]
    if len(parts) < 2:
        return ("", [])
    kind = parts[0].lower()
    values: list[float] = []
    try:
        values = [float(token) for token in parts[1:] if token]
    except ValueError:
        return ("", [])
    return (kind, values)


def infer_settlement_unit(market_title: str, rules_primary: str) -> str:
    merged = " ".join((_normalize_text(market_title), _normalize_text(rules_primary))).lower()
    if any(token in merged for token in ("°c", " c", "celsius", "deg c")):
        return "celsius"
    return "fahrenheit"


def evaluate_temperature_constraint(
    *,
    threshold_expression: str,
    observed_value: float | None,
) -> tuple[str, str]:
    if observed_value is None:
        return ("no_observation", "No observed maximum for target day yet.")

    kind, values = _normalize_threshold_expression(threshold_expression)
    if not kind:
        return ("unsupported_threshold", "Threshold expression unavailable or unparsable.")

    obs = float(observed_value)
    if kind == "at_most" and len(values) >= 1:
        limit = values[0]
        if obs > limit:
            return ("yes_impossible", f"Observed max {obs:g} exceeds at_most threshold {limit:g}.")
        return ("no_signal", "At-most threshold still feasible.")

    if kind == "below" and len(values) >= 1:
        limit = values[0]
        if obs >= limit:
            return ("yes_impossible", f"Observed max {obs:g} is not below {limit:g}.")
        return ("no_signal", "Below-threshold condition still feasible.")

    if kind == "at_least" and len(values) >= 1:
        floor = values[0]
        if obs >= floor:
            return ("yes_likely_locked", f"Observed max {obs:g} already meets at_least {floor:g}.")
        return ("no_signal", "At-least threshold not reached yet.")

    if kind == "above" and len(values) >= 1:
        floor = values[0]
        if obs > floor:
            return ("yes_likely_locked", f"Observed max {obs:g} already above {floor:g}.")
        return ("no_signal", "Above-threshold condition not reached yet.")

    if kind == "between" and len(values) >= 2:
        low = min(values[0], values[1])
        high = max(values[0], values[1])
        if obs > high:
            return ("yes_impossible", f"Observed max {obs:g} already above between upper bound {high:g}.")
        if low <= obs <= high:
            return ("no_signal", "Between range currently satisfied but can still break later.")
        return ("no_signal", "Between range still feasible.")

    if kind == "equal" and len(values) >= 1:
        target = values[0]
        if obs > target:
            return ("yes_impossible", f"Observed max {obs:g} already above equal target {target:g}.")
        if obs == target:
            return ("no_signal", "Equal target currently matched but can still move.")
        return ("no_signal", "Equal target still feasible.")

    return ("unsupported_threshold", "Threshold kind unsupported by constraint engine.")


def _find_latest_specs_csv(output_dir: str) -> str:
    directory = Path(output_dir)
    candidates = sorted(directory.glob("kalshi_temperature_contract_specs_*.csv"))
    if not candidates:
        return ""
    return str(candidates[-1])


def _read_specs_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_constraints_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CONSTRAINT_SCAN_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_temperature_constraint_scan(
    *,
    specs_csv: str | None = None,
    output_dir: str = "outputs",
    timeout_seconds: float = 12.0,
    max_markets: int = 100,
) -> dict[str, Any]:
    scanned_at = datetime.now(timezone.utc)
    resolved_specs_csv = _normalize_text(specs_csv) or _find_latest_specs_csv(output_dir)
    if not resolved_specs_csv:
        return {
            "status": "missing_specs_csv",
            "error": "No specs CSV provided and none found in output_dir.",
        }

    specs_path = Path(resolved_specs_csv)
    spec_rows = _read_specs_rows(specs_path)
    if not spec_rows:
        return {
            "status": "no_specs_rows",
            "specs_csv": str(specs_path),
            "error": "Specs CSV missing or empty.",
        }

    result_rows: list[dict[str, Any]] = []
    processed = 0
    for row in spec_rows:
        if processed >= max(1, int(max_markets)):
            break
        station_id = _normalize_text(row.get("settlement_station"))
        timezone_name = _normalize_text(row.get("settlement_timezone"))
        target_date_local = _normalize_text(row.get("target_date_local"))
        threshold_expression = _normalize_text(row.get("threshold_expression"))
        if not station_id or not timezone_name or not target_date_local:
            continue
        if not threshold_expression:
            continue

        settlement_unit = infer_settlement_unit(
            _normalize_text(row.get("market_title")),
            _normalize_text(row.get("rules_primary")),
        )
        settlement_precision = "whole_degree"

        snapshot: dict[str, Any]
        try:
            snapshot = build_intraday_temperature_snapshot(
                station_id=station_id,
                target_date_local=target_date_local,
                timezone_name=timezone_name,
                settlement_unit=settlement_unit,
                settlement_precision=settlement_precision,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:  # pragma: no cover - runtime safety
            snapshot = {
                "status": "snapshot_unavailable",
                "error": str(exc),
            }

        snapshot_status = _normalize_text(snapshot.get("status"))
        observed_raw = snapshot.get("max_temperature_settlement_raw") if isinstance(snapshot, dict) else None
        observed_quantized = snapshot.get("max_temperature_settlement_quantized") if isinstance(snapshot, dict) else None
        observations_for_date = int(snapshot.get("observations_for_date") or 0) if isinstance(snapshot, dict) else 0

        if snapshot_status != "ready":
            constraint_status = "snapshot_unavailable"
            constraint_reason = _normalize_text(snapshot.get("error") if isinstance(snapshot, dict) else "") or "Snapshot unavailable."
        else:
            constraint_status, constraint_reason = evaluate_temperature_constraint(
                threshold_expression=threshold_expression,
                observed_value=(float(observed_quantized) if isinstance(observed_quantized, (int, float)) else None),
            )

        result_rows.append(
            {
                "scanned_at": scanned_at.isoformat(),
                "source_specs_csv": str(specs_path),
                "series_ticker": _normalize_text(row.get("series_ticker")),
                "event_ticker": _normalize_text(row.get("event_ticker")),
                "market_ticker": _normalize_text(row.get("market_ticker")),
                "market_title": _normalize_text(row.get("market_title")),
                "settlement_station": station_id,
                "settlement_timezone": timezone_name,
                "target_date_local": target_date_local,
                "settlement_unit": settlement_unit,
                "settlement_precision": settlement_precision,
                "threshold_expression": threshold_expression,
                "constraint_status": constraint_status,
                "constraint_reason": constraint_reason,
                "observed_max_settlement_raw": observed_raw if observed_raw is not None else "",
                "observed_max_settlement_quantized": observed_quantized if observed_quantized is not None else "",
                "observations_for_date": observations_for_date,
                "snapshot_status": snapshot_status,
                "settlement_confidence_score": _normalize_text(row.get("settlement_confidence_score")),
            }
        )
        processed += 1

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = scanned_at.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"kalshi_temperature_constraint_scan_{stamp}.csv"
    _write_constraints_csv(csv_path, result_rows)

    summary = {
        "status": "ready" if result_rows else "no_markets",
        "scanned_at": scanned_at.isoformat(),
        "specs_csv": str(specs_path),
        "markets_processed": processed,
        "markets_emitted": len(result_rows),
        "yes_impossible_count": sum(1 for row in result_rows if row.get("constraint_status") == "yes_impossible"),
        "yes_likely_locked_count": sum(
            1 for row in result_rows if row.get("constraint_status") == "yes_likely_locked"
        ),
        "snapshot_unavailable_count": sum(
            1 for row in result_rows if row.get("constraint_status") == "snapshot_unavailable"
        ),
        "top_candidates": [
            row
            for row in result_rows
            if row.get("constraint_status") in {"yes_impossible", "yes_likely_locked"}
        ][:20],
        "output_csv": str(csv_path),
    }
    summary_path = out_dir / f"kalshi_temperature_constraint_scan_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary
