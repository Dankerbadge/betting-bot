from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any

from betbot.kalshi_nonsports_quality import _parse_bool, _parse_float, _parse_timestamp, load_history_rows


def _safe_mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _safe_stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _safe_mean(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def _pressure_rank_score(
    *,
    latest_yes_bid: float,
    mean_yes_bid: float,
    latest_spread: float,
    recent_yes_bid_change: float,
    recent_spread_change: float,
    two_sided_ratio: float,
    observation_count: int,
    execution_fit_score: float,
) -> float:
    spread_component = max(0.0, 0.05 - min(latest_spread, 0.05)) * 100.0
    bid_change_component = max(0.0, recent_yes_bid_change) * 300.0
    spread_change_component = max(0.0, -recent_spread_change) * 150.0
    bid_vs_mean_component = max(0.0, latest_yes_bid - mean_yes_bid) * 200.0
    return round(
        min(latest_yes_bid, 0.25) * 35.0
        + spread_component
        + bid_change_component
        + spread_change_component
        + bid_vs_mean_component
        + two_sided_ratio * 20.0
        + min(observation_count, 24) * 0.5
        + min(execution_fit_score, 100.0) * 0.1,
        6,
    )


def build_pressure_rows(
    *,
    history_rows: list[dict[str, str]],
    min_observations: int,
    min_latest_yes_bid: float,
    max_latest_spread: float,
    min_two_sided_ratio: float,
    min_recent_bid_change: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)

    pressure_rows: list[dict[str, Any]] = []
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest = rows_sorted[-1]
        previous = rows_sorted[-2] if len(rows_sorted) >= 2 else None
        observation_count = len(rows_sorted)
        two_sided_count = sum(1 for row in rows_sorted if _parse_bool(str(row.get("two_sided_book") or "")))
        two_sided_ratio = two_sided_count / observation_count if observation_count else 0.0

        yes_bids = [_parse_float(str(row.get("yes_bid_dollars") or "")) for row in rows_sorted]
        valid_yes_bids = [value for value in yes_bids if value is not None]
        mean_yes_bid = _safe_mean(valid_yes_bids)
        yes_bid_stddev = _safe_stddev(valid_yes_bids)

        latest_yes_bid = _parse_float(str(latest.get("yes_bid_dollars") or "")) or 0.0
        latest_spread = _parse_float(str(latest.get("spread_dollars") or "")) or 1.0
        latest_execution_fit = _parse_float(str(latest.get("execution_fit_score") or "")) or 0.0
        latest_two_sided = _parse_bool(str(latest.get("two_sided_book") or ""))

        previous_yes_bid = _parse_float(str(previous.get("yes_bid_dollars") or "")) if previous else None
        previous_spread = _parse_float(str(previous.get("spread_dollars") or "")) if previous else None
        recent_yes_bid_change = (
            round(latest_yes_bid - previous_yes_bid, 6)
            if previous_yes_bid is not None
            else 0.0
        )
        recent_spread_change = (
            round(latest_spread - previous_spread, 6)
            if previous_spread is not None
            else 0.0
        )
        if yes_bid_stddev > 0:
            bid_zscore = round((latest_yes_bid - mean_yes_bid) / yes_bid_stddev, 6)
        elif latest_yes_bid > mean_yes_bid:
            bid_zscore = 1.0
        else:
            bid_zscore = 0.0

        recent_improving = latest_two_sided and (
            recent_yes_bid_change >= min_recent_bid_change or recent_spread_change < 0.0
        )

        pressure_label = "idle"
        if (
            observation_count >= min_observations
            and latest_two_sided
            and two_sided_ratio >= min_two_sided_ratio
            and latest_yes_bid >= min_latest_yes_bid
            and latest_spread <= max_latest_spread
            and (recent_improving or latest_yes_bid > mean_yes_bid)
        ):
            pressure_label = "build"
        elif latest_two_sided and (
            latest_yes_bid >= min_latest_yes_bid or recent_improving or bid_zscore > 0.0
        ):
            pressure_label = "watch"
        elif latest_two_sided:
            pressure_label = "thin"

        pressure_rows.append(
            {
                "market_ticker": ticker,
                "category": str(latest.get("category") or ""),
                "event_title": str(latest.get("event_title") or ""),
                "market_title": str(latest.get("market_title") or ""),
                "first_seen": str(rows_sorted[0].get("captured_at") or ""),
                "last_seen": str(latest.get("captured_at") or ""),
                "observation_count": observation_count,
                "two_sided_ratio": round(two_sided_ratio, 6),
                "latest_yes_bid_dollars": round(latest_yes_bid, 6),
                "mean_yes_bid_dollars": round(mean_yes_bid, 6),
                "yes_bid_stddev": round(yes_bid_stddev, 6),
                "latest_spread_dollars": round(latest_spread, 6),
                "recent_yes_bid_change_dollars": round(recent_yes_bid_change, 6),
                "recent_spread_change_dollars": round(recent_spread_change, 6),
                "bid_zscore": bid_zscore,
                "latest_execution_fit_score": round(latest_execution_fit, 6),
                "pressure_label": pressure_label,
                "pressure_rank_score": _pressure_rank_score(
                    latest_yes_bid=latest_yes_bid,
                    mean_yes_bid=mean_yes_bid,
                    latest_spread=latest_spread,
                    recent_yes_bid_change=recent_yes_bid_change,
                    recent_spread_change=recent_spread_change,
                    two_sided_ratio=two_sided_ratio,
                    observation_count=observation_count,
                    execution_fit_score=latest_execution_fit,
                ),
            }
        )

    pressure_rows.sort(
        key=lambda row: (
            row["pressure_label"] == "build",
            row["pressure_label"] == "watch",
            row["pressure_label"] == "thin",
            row["pressure_rank_score"],
        ),
        reverse=True,
    )
    return pressure_rows


def _write_pressure_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market_ticker",
        "category",
        "event_title",
        "market_title",
        "first_seen",
        "last_seen",
        "observation_count",
        "two_sided_ratio",
        "latest_yes_bid_dollars",
        "mean_yes_bid_dollars",
        "yes_bid_stddev",
        "latest_spread_dollars",
        "recent_yes_bid_change_dollars",
        "recent_spread_change_dollars",
        "bid_zscore",
        "latest_execution_fit_score",
        "pressure_label",
        "pressure_rank_score",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_pressure(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    min_observations: int = 3,
    min_latest_yes_bid: float = 0.02,
    max_latest_spread: float = 0.02,
    min_two_sided_ratio: float = 0.5,
    min_recent_bid_change: float = 0.01,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    pressure_rows = build_pressure_rows(
        history_rows=history_rows,
        min_observations=min_observations,
        min_latest_yes_bid=min_latest_yes_bid,
        max_latest_spread=max_latest_spread,
        min_two_sided_ratio=min_two_sided_ratio,
        min_recent_bid_change=min_recent_bid_change,
    )

    build_count = sum(1 for row in pressure_rows if row["pressure_label"] == "build")
    watch_count = sum(1 for row in pressure_rows if row["pressure_label"] == "watch")
    thin_count = sum(1 for row in pressure_rows if row["pressure_label"] == "thin")
    idle_count = sum(1 for row in pressure_rows if row["pressure_label"] == "idle")
    top_build = next((row for row in pressure_rows if row["pressure_label"] == "build"), None)

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_pressure_{stamp}.csv"
    _write_pressure_csv(csv_path, pressure_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "rows_in_history": len(history_rows),
        "distinct_markets": len(pressure_rows),
        "build_markets": build_count,
        "watch_markets": watch_count,
        "thin_markets": thin_count,
        "idle_markets": idle_count,
        "top_build_market_ticker": top_build.get("market_ticker") if isinstance(top_build, dict) else None,
        "top_build_category": top_build.get("category") if isinstance(top_build, dict) else None,
        "top_markets": pressure_rows[:top_n],
        "status": "ready",
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_nonsports_pressure_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
