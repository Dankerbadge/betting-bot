#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_float(value: Any) -> float | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return float(parsed)


def _parse_int(value: Any) -> int | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    if q <= 0.0:
        return float(min(values))
    if q >= 1.0:
        return float(max(values))
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return float(ordered[0])
    pos = q * (len(ordered) - 1)
    lower = int(math.floor(pos))
    upper = int(math.ceil(pos))
    if lower == upper:
        return float(ordered[lower])
    weight = pos - lower
    return float(ordered[lower] * (1.0 - weight) + ordered[upper] * weight)


def _round_minutes(value: float) -> float:
    return round(float(value), 1)


@dataclass
class GroupStats:
    total: int = 0
    stale: int = 0
    approved: int = 0
    taf_ready: int = 0
    forecast_ready: int = 0
    stale_taf_ready: int = 0
    stale_forecast_ready: int = 0
    stale_ages: list[float] = field(default_factory=list)
    approved_ages: list[float] = field(default_factory=list)
    applied_ages: list[float] = field(default_factory=list)

    def add_row(
        self,
        *,
        reason: str,
        metar_age: float | None,
        applied_age: float | None,
        taf_status: str,
        forecast_model_status: str,
    ) -> None:
        self.total += 1
        taf_ready = _normalize_text(taf_status).lower() == "ready"
        forecast_ready = _normalize_text(forecast_model_status).lower() == "ready"
        if taf_ready:
            self.taf_ready += 1
        if forecast_ready:
            self.forecast_ready += 1
        if metar_age is not None:
            if reason == "metar_observation_stale":
                self.stale_ages.append(float(metar_age))
            if reason == "approved":
                self.approved_ages.append(float(metar_age))
        if applied_age is not None:
            self.applied_ages.append(float(applied_age))
        if reason == "metar_observation_stale":
            self.stale += 1
            if taf_ready:
                self.stale_taf_ready += 1
            if forecast_ready:
                self.stale_forecast_ready += 1
        elif reason == "approved":
            self.approved += 1


def _in_window(path: Path, start_epoch: float, end_epoch: float) -> bool:
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return False
    return float(start_epoch) <= mtime < (float(end_epoch) + 1.0)


def _collect_groups(*, out_dir: Path, start_epoch: float, end_epoch: float) -> tuple[dict[str, GroupStats], dict[tuple[str, int], GroupStats], int]:
    station_groups: dict[str, GroupStats] = defaultdict(GroupStats)
    station_hour_groups: dict[tuple[str, int], GroupStats] = defaultdict(GroupStats)
    rows_count = 0

    summary_files = sorted(
        (Path(p) for p in glob.glob(str(out_dir / "kalshi_temperature_trade_intents_summary_*.json"))),
        key=lambda p: p.stat().st_mtime if p.exists() else 0.0,
    )
    for summary_path in summary_files:
        if not _in_window(summary_path, start_epoch, end_epoch):
            continue
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        intents_csv = Path(_normalize_text(payload.get("output_csv")))
        if not intents_csv.exists():
            continue
        try:
            with intents_csv.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    reason = _normalize_text(row.get("policy_reason")).lower()
                    if reason not in {"approved", "metar_observation_stale"}:
                        continue
                    station = _normalize_text(row.get("settlement_station")).upper()
                    if not station:
                        continue
                    hour = _parse_int(row.get("policy_metar_local_hour"))
                    metar_age = _parse_float(row.get("metar_observation_age_minutes"))
                    applied_age = _parse_float(row.get("policy_metar_max_age_minutes_applied"))
                    taf_status = _normalize_text(row.get("taf_status")).lower()
                    forecast_model_status = _normalize_text(row.get("forecast_model_status")).lower()

                    station_groups[station].add_row(
                        reason=reason,
                        metar_age=metar_age,
                        applied_age=applied_age,
                        taf_status=taf_status,
                        forecast_model_status=forecast_model_status,
                    )
                    if hour is not None and 0 <= hour <= 23:
                        station_hour_groups[(station, int(hour))].add_row(
                            reason=reason,
                            metar_age=metar_age,
                            applied_age=applied_age,
                            taf_status=taf_status,
                            forecast_model_status=forecast_model_status,
                        )
                    rows_count += 1
        except OSError:
            continue
    return station_groups, station_hour_groups, rows_count


def _derive_override_minutes(
    station: str,
    group: GroupStats,
    *,
    base_max_age_minutes: float,
    hard_cap_minutes: float,
    min_stale_count: int,
    min_total_count: int,
    min_stale_rate: float,
    min_increment_minutes: float,
    lift_minutes: float,
    approved_slack_minutes: float,
    focus_stations: set[str],
    focus_station_min_total_count: int,
    focus_station_min_stale_count: int,
    focus_station_min_stale_rate: float,
    focus_station_extra_lift_minutes: float,
    taf_ready_bonus_lift_minutes: float,
    taf_ready_min_rate: float,
    forecast_ready_bonus_lift_minutes: float,
    forecast_ready_min_rate: float,
    require_taf_or_forecast_for_bonus: bool,
) -> tuple[float | None, dict[str, Any]]:
    stale_rate = (group.stale / float(group.total)) if group.total > 0 else 0.0
    taf_ready_rate = (group.taf_ready / float(group.total)) if group.total > 0 else 0.0
    forecast_ready_rate = (group.forecast_ready / float(group.total)) if group.total > 0 else 0.0
    stale_taf_ready_rate = (group.stale_taf_ready / float(group.stale)) if group.stale > 0 else 0.0
    stale_forecast_ready_rate = (group.stale_forecast_ready / float(group.stale)) if group.stale > 0 else 0.0
    station_upper = _normalize_text(station).upper()
    is_focus_station = bool(station_upper and station_upper in focus_stations)
    effective_min_total_count = min_total_count
    effective_min_stale_count = min_stale_count
    effective_min_stale_rate = min_stale_rate
    if is_focus_station:
        effective_min_total_count = min(effective_min_total_count, max(1, int(focus_station_min_total_count)))
        effective_min_stale_count = min(effective_min_stale_count, max(1, int(focus_station_min_stale_count)))
        effective_min_stale_rate = min(effective_min_stale_rate, max(0.0, float(focus_station_min_stale_rate)))
    diagnostics: dict[str, Any] = {
        "total": int(group.total),
        "stale": int(group.stale),
        "approved": int(group.approved),
        "stale_rate": round(float(stale_rate), 6),
        "taf_ready_rate": round(float(taf_ready_rate), 6),
        "forecast_ready_rate": round(float(forecast_ready_rate), 6),
        "stale_taf_ready_rate": round(float(stale_taf_ready_rate), 6),
        "stale_forecast_ready_rate": round(float(stale_forecast_ready_rate), 6),
        "is_focus_station": bool(is_focus_station),
        "effective_min_total_count": int(effective_min_total_count),
        "effective_min_stale_count": int(effective_min_stale_count),
        "effective_min_stale_rate": round(float(effective_min_stale_rate), 6),
    }
    if (
        group.total < effective_min_total_count
        or group.stale < effective_min_stale_count
        or stale_rate < effective_min_stale_rate
    ):
        diagnostics["decision"] = "insufficient_signal"
        return None, diagnostics

    stale_q75 = _percentile(group.stale_ages, 0.75)
    stale_q90 = _percentile(group.stale_ages, 0.90)
    approved_q90 = _percentile(group.approved_ages, 0.90)
    applied_q50 = _percentile(group.applied_ages, 0.50)
    diagnostics.update(
        {
            "stale_age_q75": round(stale_q75, 6) if stale_q75 is not None else None,
            "stale_age_q90": round(stale_q90, 6) if stale_q90 is not None else None,
            "approved_age_q90": round(approved_q90, 6) if approved_q90 is not None else None,
            "applied_age_q50": round(applied_q50, 6) if applied_q50 is not None else None,
        }
    )
    if stale_q75 is None:
        diagnostics["decision"] = "missing_stale_age_samples"
        return None, diagnostics

    baseline = max(base_max_age_minutes, float(applied_q50) if applied_q50 is not None else base_max_age_minutes)
    bonus_components: dict[str, float] = {}
    candidate_lift_bonus = 0.0
    if is_focus_station and focus_station_extra_lift_minutes > 0.0:
        candidate_lift_bonus += float(focus_station_extra_lift_minutes)
        bonus_components["focus_station_extra_lift_minutes"] = float(focus_station_extra_lift_minutes)
    allow_taf_or_forecast_bonus = True
    if require_taf_or_forecast_for_bonus:
        allow_taf_or_forecast_bonus = bool(
            taf_ready_rate >= max(0.0, float(taf_ready_min_rate))
            or forecast_ready_rate >= max(0.0, float(forecast_ready_min_rate))
        )
    if allow_taf_or_forecast_bonus:
        if (
            taf_ready_bonus_lift_minutes > 0.0
            and taf_ready_rate >= max(0.0, float(taf_ready_min_rate))
            and stale_taf_ready_rate >= 0.35
        ):
            candidate_lift_bonus += float(taf_ready_bonus_lift_minutes)
            bonus_components["taf_ready_bonus_lift_minutes"] = float(taf_ready_bonus_lift_minutes)
        if (
            forecast_ready_bonus_lift_minutes > 0.0
            and forecast_ready_rate >= max(0.0, float(forecast_ready_min_rate))
            and stale_forecast_ready_rate >= 0.35
        ):
            candidate_lift_bonus += float(forecast_ready_bonus_lift_minutes)
            bonus_components["forecast_ready_bonus_lift_minutes"] = float(forecast_ready_bonus_lift_minutes)
    candidate = stale_q75 + lift_minutes + candidate_lift_bonus
    if approved_q90 is not None:
        candidate = min(candidate, approved_q90 + approved_slack_minutes)
    candidate = max(candidate, baseline)
    candidate = min(candidate, hard_cap_minutes)
    candidate = _round_minutes(candidate)
    diagnostics["candidate"] = candidate
    diagnostics["baseline"] = _round_minutes(baseline)
    diagnostics["candidate_lift_bonus_minutes"] = round(float(candidate_lift_bonus), 6)
    diagnostics["candidate_lift_bonus_components"] = bonus_components

    if candidate < baseline + min_increment_minutes:
        diagnostics["decision"] = "increment_too_small"
        return None, diagnostics
    diagnostics["decision"] = "applied"
    return candidate, diagnostics


def run(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir)
    now = datetime.now(timezone.utc)
    end_epoch = now.timestamp()
    start_epoch = (now - timedelta(hours=float(args.hours))).timestamp()
    station_groups, station_hour_groups, rows_count = _collect_groups(
        out_dir=out_dir,
        start_epoch=start_epoch,
        end_epoch=end_epoch,
    )

    station_overrides: dict[str, float] = {}
    station_hour_overrides: dict[str, dict[int, float]] = defaultdict(dict)
    station_diagnostics: list[dict[str, Any]] = []
    station_hour_diagnostics: list[dict[str, Any]] = []
    focus_stations = {
        _normalize_text(token).upper()
        for token in (args.focus_station or [])
        if _normalize_text(token)
    }

    derive_kwargs = {
        "base_max_age_minutes": float(args.base_max_age_minutes),
        "hard_cap_minutes": float(args.hard_cap_minutes),
        "min_stale_count": int(args.min_stale_count),
        "min_total_count": int(args.min_total_count),
        "min_stale_rate": float(args.min_stale_rate),
        "min_increment_minutes": float(args.min_increment_minutes),
        "lift_minutes": float(args.lift_minutes),
        "approved_slack_minutes": float(args.approved_slack_minutes),
        "focus_stations": focus_stations,
        "focus_station_min_total_count": int(args.focus_station_min_total_count),
        "focus_station_min_stale_count": int(args.focus_station_min_stale_count),
        "focus_station_min_stale_rate": float(args.focus_station_min_stale_rate),
        "focus_station_extra_lift_minutes": float(args.focus_station_extra_lift_minutes),
        "taf_ready_bonus_lift_minutes": float(args.taf_ready_bonus_lift_minutes),
        "taf_ready_min_rate": float(args.taf_ready_min_rate),
        "forecast_ready_bonus_lift_minutes": float(args.forecast_ready_bonus_lift_minutes),
        "forecast_ready_min_rate": float(args.forecast_ready_min_rate),
        "require_taf_or_forecast_for_bonus": bool(args.require_taf_or_forecast_for_bonus),
    }

    for station, group in sorted(station_groups.items()):
        override, diag = _derive_override_minutes(station, group, **derive_kwargs)
        diag["station"] = station
        if override is not None:
            station_overrides[station] = override
        station_diagnostics.append(diag)

    for (station, hour), group in sorted(station_hour_groups.items(), key=lambda item: (item[0][0], item[0][1])):
        override, diag = _derive_override_minutes(station, group, **derive_kwargs)
        diag["station"] = station
        diag["local_hour"] = int(hour)
        if override is not None:
            station_hour_overrides[station][int(hour)] = override
        station_hour_diagnostics.append(diag)

    # Keep the output focused on the strongest stale contributors.
    station_diagnostics.sort(key=lambda row: (int(row.get("stale", 0)), int(row.get("total", 0))), reverse=True)
    station_hour_diagnostics.sort(
        key=lambda row: (int(row.get("stale", 0)), int(row.get("total", 0))),
        reverse=True,
    )
    top_n = max(1, int(args.top_n))

    payload = {
        "generated_at": now.isoformat(),
        "window_hours": float(args.hours),
        "window_start_utc": datetime.fromtimestamp(start_epoch, timezone.utc).isoformat(),
        "window_end_utc": datetime.fromtimestamp(end_epoch, timezone.utc).isoformat(),
        "rows_evaluated": int(rows_count),
        "base_max_age_minutes": float(args.base_max_age_minutes),
        "hard_cap_minutes": float(args.hard_cap_minutes),
        "focus_stations": sorted(focus_stations),
        "taf_path_modeling_lift": {
            "taf_ready_bonus_lift_minutes": float(args.taf_ready_bonus_lift_minutes),
            "taf_ready_min_rate": float(args.taf_ready_min_rate),
            "forecast_ready_bonus_lift_minutes": float(args.forecast_ready_bonus_lift_minutes),
            "forecast_ready_min_rate": float(args.forecast_ready_min_rate),
            "require_taf_or_forecast_for_bonus": bool(args.require_taf_or_forecast_for_bonus),
        },
        "station_max_age_minutes": dict(sorted(station_overrides.items())),
        "station_local_hour_max_age_minutes": {
            station: {str(hour): value for hour, value in sorted(per_hour.items())}
            for station, per_hour in sorted(station_hour_overrides.items())
            if per_hour
        },
        "diagnostics": {
            "station_top": station_diagnostics[:top_n],
            "station_hour_top": station_hour_diagnostics[:top_n],
        },
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "status": "ready",
        "output_file": str(output_path),
        "rows_evaluated": int(rows_count),
        "station_override_count": len(payload["station_max_age_minutes"]),
        "station_hour_override_count": sum(len(v) for v in payload["station_local_hour_max_age_minutes"].values()),
        "generated_at": payload["generated_at"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build station/hour METAR-age policy overrides from recent temperature intents.")
    parser.add_argument("--out-dir", required=True, help="Temperature strategy output directory")
    parser.add_argument("--hours", type=float, default=14.0, help="Lookback window in hours")
    parser.add_argument("--base-max-age-minutes", type=float, default=22.5, help="Global base max METAR age")
    parser.add_argument("--hard-cap-minutes", type=float, default=35.0, help="Hard ceiling for generated overrides")
    parser.add_argument("--min-stale-count", type=int, default=20, help="Minimum stale rows required for override consideration")
    parser.add_argument("--min-total-count", type=int, default=30, help="Minimum total rows required for override consideration")
    parser.add_argument("--min-stale-rate", type=float, default=0.30, help="Minimum stale-rate required for override consideration")
    parser.add_argument("--min-increment-minutes", type=float, default=1.0, help="Minimum incremental lift above baseline required")
    parser.add_argument("--lift-minutes", type=float, default=1.0, help="Lift added to stale age percentile before clamping")
    parser.add_argument("--approved-slack-minutes", type=float, default=1.0, help="Upper slack above approved-age percentile")
    parser.add_argument(
        "--focus-station",
        action="append",
        default=[],
        help="Station to prioritize for policy override generation (repeatable).",
    )
    parser.add_argument(
        "--focus-station-min-total-count",
        type=int,
        default=20,
        help="Minimum total rows required for focus stations.",
    )
    parser.add_argument(
        "--focus-station-min-stale-count",
        type=int,
        default=10,
        help="Minimum stale rows required for focus stations.",
    )
    parser.add_argument(
        "--focus-station-min-stale-rate",
        type=float,
        default=0.2,
        help="Minimum stale rate required for focus stations.",
    )
    parser.add_argument(
        "--focus-station-extra-lift-minutes",
        type=float,
        default=1.0,
        help="Extra candidate lift added for focus stations after baseline stale lift.",
    )
    parser.add_argument(
        "--taf-ready-bonus-lift-minutes",
        type=float,
        default=0.75,
        help="Additional lift when TAF-ready rows dominate stale suppression.",
    )
    parser.add_argument(
        "--taf-ready-min-rate",
        type=float,
        default=0.5,
        help="Minimum TAF-ready rate to activate TAF bonus lift.",
    )
    parser.add_argument(
        "--forecast-ready-bonus-lift-minutes",
        type=float,
        default=0.5,
        help="Additional lift when forecast-modeled rows dominate stale suppression.",
    )
    parser.add_argument(
        "--forecast-ready-min-rate",
        type=float,
        default=0.5,
        help="Minimum forecast-ready rate to activate forecast bonus lift.",
    )
    parser.add_argument(
        "--require-taf-or-forecast-for-bonus",
        action="store_true",
        help="Only apply TAF/forecast lift bonuses when at least one readiness rate threshold is met.",
    )
    parser.add_argument("--top-n", type=int, default=25, help="Top diagnostics rows retained per slice")
    parser.add_argument("--output", required=True, help="Output JSON policy path")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    summary = run(args)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
