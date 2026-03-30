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


def _signal_rank_score(
    *,
    stable_ratio: float,
    latest_yes_bid: float,
    mean_yes_bid: float,
    mean_spread: float,
    yes_bid_stddev: float,
    observation_count: int,
) -> float:
    spread_component = max(0.0, 0.05 - min(mean_spread, 0.05)) * 100.0
    stability_component = max(0.0, 0.03 - min(yes_bid_stddev, 0.03)) * 200.0
    return round(
        stable_ratio * 25.0
        + min(latest_yes_bid, 0.25) * 25.0
        + min(mean_yes_bid, 0.25) * 15.0
        + spread_component
        + stability_component
        + min(observation_count, 24) * 0.5,
        6,
    )


def build_signal_rows(
    *,
    history_rows: list[dict[str, str]],
    min_observations: int,
    min_stable_ratio: float,
    min_latest_yes_bid: float,
    min_mean_yes_bid: float,
    max_mean_spread: float,
    max_yes_bid_stddev: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)

    signal_rows: list[dict[str, Any]] = []
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest = rows_sorted[-1]
        observation_count = len(rows_sorted)
        yes_bids = [_parse_float(str(row.get("yes_bid_dollars") or "")) for row in rows_sorted]
        spreads = [_parse_float(str(row.get("spread_dollars") or "")) for row in rows_sorted]

        valid_yes_bids = [value for value in yes_bids if value is not None]
        valid_spreads = [value for value in spreads if value is not None]
        mean_yes_bid = _safe_mean(valid_yes_bids)
        latest_yes_bid = _parse_float(str(latest.get("yes_bid_dollars") or "")) or 0.0
        max_yes_bid = max(valid_yes_bids) if valid_yes_bids else 0.0
        mean_spread = _safe_mean(valid_spreads) if valid_spreads else 1.0
        latest_spread = _parse_float(str(latest.get("spread_dollars") or "")) or 1.0
        yes_bid_stddev = _safe_stddev(valid_yes_bids)

        stable_count = 0
        two_sided_count = 0
        for row in rows_sorted:
            two_sided = _parse_bool(str(row.get("two_sided_book") or ""))
            if two_sided:
                two_sided_count += 1
            row_yes_bid = _parse_float(str(row.get("yes_bid_dollars") or "")) or 0.0
            row_spread = _parse_float(str(row.get("spread_dollars") or "")) or 1.0
            if two_sided and row_yes_bid >= min_latest_yes_bid and row_spread <= max_mean_spread:
                stable_count += 1

        stable_ratio = stable_count / observation_count if observation_count else 0.0
        two_sided_ratio = two_sided_count / observation_count if observation_count else 0.0

        signal_label = "ignore"
        if (
            observation_count >= min_observations
            and stable_ratio >= min_stable_ratio
            and latest_yes_bid >= min_latest_yes_bid
            and mean_yes_bid >= min_mean_yes_bid
            and mean_spread <= max_mean_spread
            and yes_bid_stddev <= max_yes_bid_stddev
        ):
            signal_label = "eligible"
        elif latest_yes_bid >= min_latest_yes_bid and two_sided_ratio > 0:
            signal_label = "watch"
        elif two_sided_ratio > 0:
            signal_label = "thin"

        signal_rows.append(
            {
                "market_ticker": ticker,
                "category": str(latest.get("category") or ""),
                "event_title": str(latest.get("event_title") or ""),
                "market_title": str(latest.get("market_title") or ""),
                "first_seen": str(rows_sorted[0].get("captured_at") or ""),
                "last_seen": str(latest.get("captured_at") or ""),
                "observation_count": observation_count,
                "stable_observations": stable_count,
                "stable_ratio": round(stable_ratio, 6),
                "two_sided_ratio": round(two_sided_ratio, 6),
                "latest_yes_bid_dollars": round(latest_yes_bid, 6),
                "max_yes_bid_dollars": round(max_yes_bid, 6),
                "mean_yes_bid_dollars": round(mean_yes_bid, 6),
                "latest_spread_dollars": round(latest_spread, 6),
                "mean_spread_dollars": round(mean_spread, 6),
                "yes_bid_stddev": round(yes_bid_stddev, 6),
                "hours_to_close_latest": _parse_float(str(latest.get("hours_to_close") or "")) or "",
                "signal_label": signal_label,
                "signal_rank_score": _signal_rank_score(
                    stable_ratio=stable_ratio,
                    latest_yes_bid=latest_yes_bid,
                    mean_yes_bid=mean_yes_bid,
                    mean_spread=mean_spread,
                    yes_bid_stddev=yes_bid_stddev,
                    observation_count=observation_count,
                ),
            }
        )

    signal_rows.sort(
        key=lambda row: (
            row["signal_label"] == "eligible",
            row["signal_label"] == "watch",
            row["signal_label"] == "thin",
            row["signal_rank_score"],
        ),
        reverse=True,
    )
    return signal_rows


def _write_signal_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market_ticker",
        "category",
        "event_title",
        "market_title",
        "first_seen",
        "last_seen",
        "observation_count",
        "stable_observations",
        "stable_ratio",
        "two_sided_ratio",
        "latest_yes_bid_dollars",
        "max_yes_bid_dollars",
        "mean_yes_bid_dollars",
        "latest_spread_dollars",
        "mean_spread_dollars",
        "yes_bid_stddev",
        "hours_to_close_latest",
        "signal_label",
        "signal_rank_score",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_signals(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    min_observations: int = 3,
    min_stable_ratio: float = 0.5,
    min_latest_yes_bid: float = 0.05,
    min_mean_yes_bid: float = 0.05,
    max_mean_spread: float = 0.03,
    max_yes_bid_stddev: float = 0.03,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    signal_rows = build_signal_rows(
        history_rows=history_rows,
        min_observations=min_observations,
        min_stable_ratio=min_stable_ratio,
        min_latest_yes_bid=min_latest_yes_bid,
        min_mean_yes_bid=min_mean_yes_bid,
        max_mean_spread=max_mean_spread,
        max_yes_bid_stddev=max_yes_bid_stddev,
    )

    eligible_count = sum(1 for row in signal_rows if row["signal_label"] == "eligible")
    watch_count = sum(1 for row in signal_rows if row["signal_label"] == "watch")
    thin_count = sum(1 for row in signal_rows if row["signal_label"] == "thin")
    ignore_count = sum(1 for row in signal_rows if row["signal_label"] == "ignore")

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_signals_{stamp}.csv"
    _write_signal_csv(csv_path, signal_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "rows_in_history": len(history_rows),
        "distinct_markets": len(signal_rows),
        "eligible_markets": eligible_count,
        "watch_markets": watch_count,
        "thin_markets": thin_count,
        "ignore_markets": ignore_count,
        "top_markets": signal_rows[:top_n],
        "status": "ready",
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_nonsports_signals_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
