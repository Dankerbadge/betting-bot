from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_nonsports_quality import _parse_bool, _parse_float, _parse_timestamp, load_history_rows


def _tail_streak(snapshot_keys: list[str], active_keys: set[str]) -> int:
    streak = 0
    for snapshot_key in reversed(snapshot_keys):
        if snapshot_key in active_keys:
            streak += 1
        else:
            break
    return streak


def _persistence_rank_score(
    *,
    tradeable_snapshot_ratio: float,
    consecutive_tradeable_snapshots: int,
    latest_yes_bid: float,
    mean_spread: float,
    observation_count: int,
) -> float:
    spread_component = max(0.0, 0.05 - min(mean_spread, 0.05)) * 100.0
    return round(
        tradeable_snapshot_ratio * 30.0
        + min(consecutive_tradeable_snapshots, 12) * 4.0
        + min(latest_yes_bid, 0.25) * 25.0
        + spread_component
        + min(observation_count, 48) * 0.25,
        6,
    )


def build_persistence_rows(
    *,
    history_rows: list[dict[str, str]],
    min_tradeable_yes_bid: float,
    max_tradeable_spread: float,
    min_tradeable_snapshot_count: int,
    min_consecutive_tradeable_snapshots: int,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    snapshot_keys = sorted(
        {
            str(row.get("captured_at") or "").strip()
            for row in history_rows
            if str(row.get("captured_at") or "").strip()
        },
        key=lambda value: _parse_timestamp(value) or datetime.min.replace(tzinfo=timezone.utc),
    )

    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)

    persistence_rows: list[dict[str, Any]] = []
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest = rows_sorted[-1]
        observation_count = len(rows_sorted)
        snapshot_seen_keys = {
            str(row.get("captured_at") or "").strip()
            for row in rows_sorted
            if str(row.get("captured_at") or "").strip()
        }
        two_sided_snapshot_keys: set[str] = set()
        tradeable_snapshot_keys: set[str] = set()
        yes_bids: list[float] = []
        spreads: list[float] = []
        for row in rows_sorted:
            snapshot_key = str(row.get("captured_at") or "").strip()
            yes_bid = _parse_float(str(row.get("yes_bid_dollars") or ""))
            spread = _parse_float(str(row.get("spread_dollars") or ""))
            if yes_bid is not None:
                yes_bids.append(yes_bid)
            if spread is not None:
                spreads.append(spread)
            two_sided = _parse_bool(str(row.get("two_sided_book") or ""))
            if two_sided and snapshot_key:
                two_sided_snapshot_keys.add(snapshot_key)
            if (
                two_sided
                and snapshot_key
                and yes_bid is not None
                and yes_bid >= min_tradeable_yes_bid
                and spread is not None
                and spread <= max_tradeable_spread
            ):
                tradeable_snapshot_keys.add(snapshot_key)

        latest_yes_bid = _parse_float(str(latest.get("yes_bid_dollars") or "")) or 0.0
        latest_spread = _parse_float(str(latest.get("spread_dollars") or "")) or 1.0
        mean_yes_bid = sum(yes_bids) / len(yes_bids) if yes_bids else 0.0
        max_yes_bid = max(yes_bids) if yes_bids else 0.0
        mean_spread = sum(spreads) / len(spreads) if spreads else 1.0

        snapshots_seen = len(snapshot_seen_keys)
        two_sided_snapshots = len(two_sided_snapshot_keys)
        tradeable_snapshots = len(tradeable_snapshot_keys)
        consecutive_seen_snapshots = _tail_streak(snapshot_keys, snapshot_seen_keys)
        consecutive_two_sided_snapshots = _tail_streak(snapshot_keys, two_sided_snapshot_keys)
        consecutive_tradeable_snapshots = _tail_streak(snapshot_keys, tradeable_snapshot_keys)
        tradeable_snapshot_ratio = tradeable_snapshots / snapshots_seen if snapshots_seen else 0.0
        two_sided_snapshot_ratio = two_sided_snapshots / snapshots_seen if snapshots_seen else 0.0

        persistence_label = "one_off"
        if (
            tradeable_snapshots >= min_tradeable_snapshot_count
            and consecutive_tradeable_snapshots >= min_consecutive_tradeable_snapshots
        ):
            persistence_label = "persistent_tradeable"
        elif max_yes_bid >= min_tradeable_yes_bid and two_sided_snapshots >= min_consecutive_tradeable_snapshots:
            persistence_label = "persistent_watch"
        elif (
            two_sided_snapshots >= min_consecutive_tradeable_snapshots
            or consecutive_two_sided_snapshots >= min_consecutive_tradeable_snapshots
        ):
            persistence_label = "recurring"
        elif two_sided_snapshots >= 1:
            persistence_label = "thin"

        persistence_rows.append(
            {
                "market_ticker": ticker,
                "category": str(latest.get("category") or ""),
                "event_title": str(latest.get("event_title") or ""),
                "market_title": str(latest.get("market_title") or ""),
                "first_seen": str(rows_sorted[0].get("captured_at") or ""),
                "last_seen": str(latest.get("captured_at") or ""),
                "observation_count": observation_count,
                "snapshots_seen": snapshots_seen,
                "two_sided_snapshots": two_sided_snapshots,
                "tradeable_snapshots": tradeable_snapshots,
                "consecutive_seen_snapshots": consecutive_seen_snapshots,
                "consecutive_two_sided_snapshots": consecutive_two_sided_snapshots,
                "consecutive_tradeable_snapshots": consecutive_tradeable_snapshots,
                "tradeable_snapshot_ratio": round(tradeable_snapshot_ratio, 6),
                "two_sided_snapshot_ratio": round(two_sided_snapshot_ratio, 6),
                "latest_yes_bid_dollars": round(latest_yes_bid, 6),
                "max_yes_bid_dollars": round(max_yes_bid, 6),
                "mean_yes_bid_dollars": round(mean_yes_bid, 6),
                "latest_spread_dollars": round(latest_spread, 6),
                "mean_spread_dollars": round(mean_spread, 6),
                "hours_to_close_latest": _parse_float(str(latest.get("hours_to_close") or "")) or "",
                "persistence_label": persistence_label,
                "persistence_rank_score": _persistence_rank_score(
                    tradeable_snapshot_ratio=tradeable_snapshot_ratio,
                    consecutive_tradeable_snapshots=consecutive_tradeable_snapshots,
                    latest_yes_bid=latest_yes_bid,
                    mean_spread=mean_spread,
                    observation_count=observation_count,
                ),
            }
        )

    persistence_rows.sort(
        key=lambda row: (
            row["persistence_label"] == "persistent_tradeable",
            row["persistence_label"] == "persistent_watch",
            row["persistence_label"] == "recurring",
            row["persistence_label"] == "thin",
            row["persistence_rank_score"],
        ),
        reverse=True,
    )
    return persistence_rows


def _write_persistence_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market_ticker",
        "category",
        "event_title",
        "market_title",
        "first_seen",
        "last_seen",
        "observation_count",
        "snapshots_seen",
        "two_sided_snapshots",
        "tradeable_snapshots",
        "consecutive_seen_snapshots",
        "consecutive_two_sided_snapshots",
        "consecutive_tradeable_snapshots",
        "tradeable_snapshot_ratio",
        "two_sided_snapshot_ratio",
        "latest_yes_bid_dollars",
        "max_yes_bid_dollars",
        "mean_yes_bid_dollars",
        "latest_spread_dollars",
        "mean_spread_dollars",
        "hours_to_close_latest",
        "persistence_label",
        "persistence_rank_score",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_persistence(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    min_tradeable_yes_bid: float = 0.05,
    max_tradeable_spread: float = 0.03,
    min_tradeable_snapshot_count: int = 2,
    min_consecutive_tradeable_snapshots: int = 2,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    persistence_rows = build_persistence_rows(
        history_rows=history_rows,
        min_tradeable_yes_bid=min_tradeable_yes_bid,
        max_tradeable_spread=max_tradeable_spread,
        min_tradeable_snapshot_count=min_tradeable_snapshot_count,
        min_consecutive_tradeable_snapshots=min_consecutive_tradeable_snapshots,
    )

    persistent_tradeable_count = sum(
        1 for row in persistence_rows if row["persistence_label"] == "persistent_tradeable"
    )
    persistent_watch_count = sum(
        1 for row in persistence_rows if row["persistence_label"] == "persistent_watch"
    )
    recurring_count = sum(1 for row in persistence_rows if row["persistence_label"] == "recurring")
    thin_count = sum(1 for row in persistence_rows if row["persistence_label"] == "thin")
    one_off_count = sum(1 for row in persistence_rows if row["persistence_label"] == "one_off")
    snapshot_count = len(
        {
            str(row.get("captured_at") or "").strip()
            for row in history_rows
            if str(row.get("captured_at") or "").strip()
        }
    )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_persistence_{stamp}.csv"
    _write_persistence_csv(csv_path, persistence_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "rows_in_history": len(history_rows),
        "snapshot_count": snapshot_count,
        "distinct_markets": len(persistence_rows),
        "persistent_tradeable_markets": persistent_tradeable_count,
        "persistent_watch_markets": persistent_watch_count,
        "recurring_markets": recurring_count,
        "thin_markets": thin_count,
        "one_off_markets": one_off_count,
        "top_markets": persistence_rows[:top_n],
        "status": "ready",
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_nonsports_persistence_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
