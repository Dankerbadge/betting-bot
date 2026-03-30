from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_nonsports_quality import _parse_bool, _parse_float, _parse_timestamp, load_history_rows


def _linear_slope_per_hour(points: list[tuple[datetime, float]]) -> float:
    if len(points) < 2:
        return 0.0
    start = points[0][0]
    xs = [(point[0] - start).total_seconds() / 3600.0 for point in points]
    ys = [point[1] for point in points]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 0.0
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    return numerator / denominator


def _hours_to_target(gap: float, slope_per_hour: float) -> float | None:
    if gap <= 0:
        return 0.0
    if slope_per_hour <= 0:
        return None
    return round(gap / slope_per_hour, 6)


def build_threshold_rows(
    *,
    history_rows: list[dict[str, str]],
    target_yes_bid: float,
    target_spread: float,
    recent_window: int,
    max_hours_to_target: float,
    min_recent_two_sided_ratio: float,
    min_observations: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)

    threshold_rows: list[dict[str, Any]] = []
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        recent_rows = rows_sorted[-recent_window:]
        latest = recent_rows[-1]
        observation_count = len(rows_sorted)
        recent_observation_count = len(recent_rows)
        recent_two_sided = [
            row for row in recent_rows if _parse_bool(str(row.get("two_sided_book") or ""))
        ]
        recent_two_sided_ratio = len(recent_two_sided) / recent_observation_count if recent_observation_count else 0.0

        yes_points = [
            (_parse_timestamp(str(row.get("captured_at") or "")), _parse_float(str(row.get("yes_bid_dollars") or "")))
            for row in recent_rows
        ]
        yes_points = [(timestamp, value) for timestamp, value in yes_points if timestamp and value is not None]
        spread_points = [
            (_parse_timestamp(str(row.get("captured_at") or "")), _parse_float(str(row.get("spread_dollars") or "")))
            for row in recent_rows
        ]
        spread_points = [(timestamp, value) for timestamp, value in spread_points if timestamp and value is not None]

        latest_yes_bid = _parse_float(str(latest.get("yes_bid_dollars") or "")) or 0.0
        latest_spread = _parse_float(str(latest.get("spread_dollars") or "")) or 1.0
        yes_bid_slope_per_hour = round(_linear_slope_per_hour(yes_points), 6)
        spread_slope_per_hour = round(_linear_slope_per_hour(spread_points), 6)

        yes_bid_gap = round(max(0.0, target_yes_bid - latest_yes_bid), 6)
        spread_gap = round(max(0.0, latest_spread - target_spread), 6)
        hours_to_yes_target = _hours_to_target(yes_bid_gap, yes_bid_slope_per_hour)
        hours_to_spread_target = _hours_to_target(spread_gap, -spread_slope_per_hour)
        if hours_to_yes_target is None or hours_to_spread_target is None:
            hours_to_tradeable_target = None
        else:
            hours_to_tradeable_target = round(max(hours_to_yes_target, hours_to_spread_target), 6)

        improvement_events = 0
        for previous, current in zip(recent_rows, recent_rows[1:]):
            prev_yes_bid = _parse_float(str(previous.get("yes_bid_dollars") or "")) or 0.0
            curr_yes_bid = _parse_float(str(current.get("yes_bid_dollars") or "")) or 0.0
            prev_spread = _parse_float(str(previous.get("spread_dollars") or "")) or 1.0
            curr_spread = _parse_float(str(current.get("spread_dollars") or "")) or 1.0
            if curr_yes_bid > prev_yes_bid or curr_spread < prev_spread:
                improvement_events += 1

        threshold_label = "inactive"
        if (
            observation_count >= min_observations
            and recent_two_sided_ratio >= min_recent_two_sided_ratio
            and latest_yes_bid >= target_yes_bid
            and latest_spread <= target_spread
        ):
            threshold_label = "tradeable_now"
        elif (
            observation_count >= min_observations
            and recent_two_sided_ratio >= min_recent_two_sided_ratio
            and hours_to_tradeable_target is not None
            and hours_to_tradeable_target <= max_hours_to_target
            and improvement_events > 0
        ):
            threshold_label = "approaching"
        elif recent_two_sided_ratio > 0 and (improvement_events > 0 or latest_yes_bid > 0):
            threshold_label = "building"
        elif recent_two_sided_ratio > 0:
            threshold_label = "flat_two_sided"

        rank_hours = hours_to_tradeable_target if isinstance(hours_to_tradeable_target, float) else max_hours_to_target * 4.0
        threshold_rank_score = round(
            max(0.0, max_hours_to_target * 2.0 - min(rank_hours, max_hours_to_target * 2.0))
            + recent_two_sided_ratio * 15.0
            + latest_yes_bid * 50.0
            + max(0.0, -spread_gap) * 10.0,
            6,
        )

        threshold_rows.append(
            {
                "market_ticker": ticker,
                "category": str(latest.get("category") or ""),
                "event_title": str(latest.get("event_title") or ""),
                "market_title": str(latest.get("market_title") or ""),
                "observation_count": observation_count,
                "recent_observation_count": recent_observation_count,
                "recent_two_sided_ratio": round(recent_two_sided_ratio, 6),
                "latest_yes_bid_dollars": round(latest_yes_bid, 6),
                "latest_spread_dollars": round(latest_spread, 6),
                "yes_bid_gap_to_0_05": yes_bid_gap,
                "spread_gap_to_0_02": spread_gap,
                "yes_bid_slope_per_hour": yes_bid_slope_per_hour,
                "spread_slope_per_hour": spread_slope_per_hour,
                "hours_to_yes_target": hours_to_yes_target if hours_to_yes_target is not None else "",
                "hours_to_spread_target": hours_to_spread_target if hours_to_spread_target is not None else "",
                "hours_to_tradeable_target": (
                    hours_to_tradeable_target if hours_to_tradeable_target is not None else ""
                ),
                "improvement_events_recent": improvement_events,
                "threshold_label": threshold_label,
                "threshold_rank_score": threshold_rank_score,
            }
        )

    threshold_rows.sort(
        key=lambda row: (
            row["threshold_label"] == "tradeable_now",
            row["threshold_label"] == "approaching",
            row["threshold_label"] == "building",
            row["threshold_label"] == "flat_two_sided",
            row["threshold_rank_score"],
        ),
        reverse=True,
    )
    return threshold_rows


def _write_threshold_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market_ticker",
        "category",
        "event_title",
        "market_title",
        "observation_count",
        "recent_observation_count",
        "recent_two_sided_ratio",
        "latest_yes_bid_dollars",
        "latest_spread_dollars",
        "yes_bid_gap_to_0_05",
        "spread_gap_to_0_02",
        "yes_bid_slope_per_hour",
        "spread_slope_per_hour",
        "hours_to_yes_target",
        "hours_to_spread_target",
        "hours_to_tradeable_target",
        "improvement_events_recent",
        "threshold_label",
        "threshold_rank_score",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_thresholds(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    target_yes_bid: float = 0.05,
    target_spread: float = 0.02,
    recent_window: int = 5,
    max_hours_to_target: float = 6.0,
    min_recent_two_sided_ratio: float = 0.5,
    min_observations: int = 3,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    threshold_rows = build_threshold_rows(
        history_rows=history_rows,
        target_yes_bid=target_yes_bid,
        target_spread=target_spread,
        recent_window=recent_window,
        max_hours_to_target=max_hours_to_target,
        min_recent_two_sided_ratio=min_recent_two_sided_ratio,
        min_observations=min_observations,
    )

    tradeable_now = sum(1 for row in threshold_rows if row["threshold_label"] == "tradeable_now")
    approaching = sum(1 for row in threshold_rows if row["threshold_label"] == "approaching")
    building = sum(1 for row in threshold_rows if row["threshold_label"] == "building")
    flat_two_sided = sum(1 for row in threshold_rows if row["threshold_label"] == "flat_two_sided")
    inactive = sum(1 for row in threshold_rows if row["threshold_label"] == "inactive")
    top_approaching = next((row for row in threshold_rows if row["threshold_label"] == "approaching"), None)

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_thresholds_{stamp}.csv"
    _write_threshold_csv(csv_path, threshold_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "rows_in_history": len(history_rows),
        "distinct_markets": len(threshold_rows),
        "tradeable_now_markets": tradeable_now,
        "approaching_markets": approaching,
        "building_markets": building,
        "flat_two_sided_markets": flat_two_sided,
        "inactive_markets": inactive,
        "top_approaching_market_ticker": (
            top_approaching.get("market_ticker") if isinstance(top_approaching, dict) else None
        ),
        "top_approaching_category": (
            top_approaching.get("category") if isinstance(top_approaching, dict) else None
        ),
        "top_approaching_hours_to_tradeable": (
            top_approaching.get("hours_to_tradeable_target") if isinstance(top_approaching, dict) else None
        ),
        "top_markets": threshold_rows[:top_n],
        "status": "ready",
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_nonsports_thresholds_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
