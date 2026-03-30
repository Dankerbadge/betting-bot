from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_nonsports_quality import _parse_bool, _parse_float, _parse_timestamp, load_history_rows


def _snapshot_groups(history_rows: list[dict[str, str]]) -> list[tuple[str, list[dict[str, str]]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        captured_at = str(row.get("captured_at") or "").strip()
        if captured_at:
            grouped.setdefault(captured_at, []).append(row)
    return sorted(
        grouped.items(),
        key=lambda item: _parse_timestamp(item[0]) or datetime.min.replace(tzinfo=timezone.utc),
    )


def _market_map(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for row in rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            result[ticker] = row
    return result


def _is_tradeable(row: dict[str, str], min_tradeable_yes_bid: float, max_tradeable_spread: float) -> bool:
    yes_bid = _parse_float(str(row.get("yes_bid_dollars") or ""))
    spread = _parse_float(str(row.get("spread_dollars") or ""))
    return (
        _parse_bool(str(row.get("two_sided_book") or ""))
        and yes_bid is not None
        and yes_bid >= min_tradeable_yes_bid
        and spread is not None
        and spread <= max_tradeable_spread
    )


def build_delta_rows(
    *,
    previous_rows: list[dict[str, str]],
    latest_rows: list[dict[str, str]],
    min_tradeable_yes_bid: float,
    max_tradeable_spread: float,
    min_bid_improvement: float,
    min_spread_improvement: float,
) -> list[dict[str, Any]]:
    previous_map = _market_map(previous_rows)
    latest_map = _market_map(latest_rows)
    tickers = sorted(set(previous_map) | set(latest_map))

    delta_rows: list[dict[str, Any]] = []
    for ticker in tickers:
        previous = previous_map.get(ticker)
        latest = latest_map.get(ticker)
        previous_yes_bid = _parse_float(str(previous.get("yes_bid_dollars") or "")) if previous else None
        latest_yes_bid = _parse_float(str(latest.get("yes_bid_dollars") or "")) if latest else None
        previous_spread = _parse_float(str(previous.get("spread_dollars") or "")) if previous else None
        latest_spread = _parse_float(str(latest.get("spread_dollars") or "")) if latest else None
        previous_two_sided = _parse_bool(str(previous.get("two_sided_book") or "")) if previous else False
        latest_two_sided = _parse_bool(str(latest.get("two_sided_book") or "")) if latest else False
        previous_tradeable = (
            _is_tradeable(previous, min_tradeable_yes_bid, max_tradeable_spread) if previous else False
        )
        latest_tradeable = (
            _is_tradeable(latest, min_tradeable_yes_bid, max_tradeable_spread) if latest else False
        )
        yes_bid_delta = (
            round(latest_yes_bid - previous_yes_bid, 6)
            if latest_yes_bid is not None and previous_yes_bid is not None
            else ""
        )
        spread_delta = (
            round(latest_spread - previous_spread, 6)
            if latest_spread is not None and previous_spread is not None
            else ""
        )

        change_label = "unchanged_listing"
        if previous is None and latest is not None:
            change_label = "new_listing"
        elif previous is not None and latest is None:
            change_label = "dropped_listing"
        elif latest_tradeable and not previous_tradeable:
            change_label = "newly_tradeable"
        elif previous_tradeable and not latest_tradeable:
            change_label = "lost_tradeable"
        elif latest_two_sided and not previous_two_sided:
            change_label = "newly_two_sided"
        elif previous_two_sided and not latest_two_sided:
            change_label = "lost_two_sided"
        elif latest_two_sided and previous_two_sided:
            bid_up = (
                isinstance(yes_bid_delta, float) and yes_bid_delta >= min_bid_improvement
            )
            spread_tighter = (
                isinstance(spread_delta, float) and spread_delta <= -min_spread_improvement
            )
            bid_down = (
                isinstance(yes_bid_delta, float) and yes_bid_delta <= -min_bid_improvement
            )
            spread_wider = (
                isinstance(spread_delta, float) and spread_delta >= min_spread_improvement
            )
            if bid_up or spread_tighter:
                change_label = "improved_two_sided"
            elif bid_down or spread_wider:
                change_label = "worsened_two_sided"
            else:
                change_label = "stable_two_sided"

        source = latest or previous or {}
        delta_rows.append(
            {
                "market_ticker": ticker,
                "category": str(source.get("category") or ""),
                "event_title": str(source.get("event_title") or ""),
                "market_title": str(source.get("market_title") or ""),
                "previous_yes_bid_dollars": previous_yes_bid if previous_yes_bid is not None else "",
                "latest_yes_bid_dollars": latest_yes_bid if latest_yes_bid is not None else "",
                "yes_bid_delta_dollars": yes_bid_delta,
                "previous_spread_dollars": previous_spread if previous_spread is not None else "",
                "latest_spread_dollars": latest_spread if latest_spread is not None else "",
                "spread_delta_dollars": spread_delta,
                "previous_two_sided": previous_two_sided,
                "latest_two_sided": latest_two_sided,
                "previous_tradeable": previous_tradeable,
                "latest_tradeable": latest_tradeable,
                "change_label": change_label,
            }
        )

    priority = {
        "newly_tradeable": 7,
        "improved_two_sided": 6,
        "newly_two_sided": 5,
        "stable_two_sided": 4,
        "worsened_two_sided": 3,
        "lost_two_sided": 2,
        "lost_tradeable": 1,
        "new_listing": 0,
        "unchanged_listing": -1,
        "dropped_listing": -2,
    }
    delta_rows.sort(
        key=lambda row: (
            priority.get(str(row["change_label"]), -99),
            row["latest_yes_bid_dollars"] if isinstance(row["latest_yes_bid_dollars"], float) else -1.0,
        ),
        reverse=True,
    )
    return delta_rows


def _write_delta_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market_ticker",
        "category",
        "event_title",
        "market_title",
        "previous_yes_bid_dollars",
        "latest_yes_bid_dollars",
        "yes_bid_delta_dollars",
        "previous_spread_dollars",
        "latest_spread_dollars",
        "spread_delta_dollars",
        "previous_two_sided",
        "latest_two_sided",
        "previous_tradeable",
        "latest_tradeable",
        "change_label",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_deltas(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    min_tradeable_yes_bid: float = 0.05,
    max_tradeable_spread: float = 0.03,
    min_bid_improvement: float = 0.01,
    min_spread_improvement: float = 0.01,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    snapshots = _snapshot_groups(history_rows)

    summary: dict[str, Any] = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "rows_in_history": len(history_rows),
        "snapshot_count": len(snapshots),
        "status": "ready",
    }

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_deltas_{stamp}.csv"

    if len(snapshots) < 2:
        _write_delta_csv(csv_path, [])
        summary.update(
            {
                "board_change_label": "insufficient_snapshots",
                "previous_snapshot_at": None,
                "latest_snapshot_at": snapshots[-1][0] if snapshots else None,
                "newly_tradeable_markets": 0,
                "improved_two_sided_markets": 0,
                "newly_two_sided_markets": 0,
                "stable_two_sided_markets": 0,
                "worsened_two_sided_markets": 0,
                "lost_two_sided_markets": 0,
                "lost_tradeable_markets": 0,
                "new_listings": 0,
                "dropped_listings": 0,
                "top_markets": [],
                "output_csv": str(csv_path),
            }
        )
    else:
        previous_snapshot_at, previous_rows = snapshots[-2]
        latest_snapshot_at, latest_rows = snapshots[-1]
        delta_rows = build_delta_rows(
            previous_rows=previous_rows,
            latest_rows=latest_rows,
            min_tradeable_yes_bid=min_tradeable_yes_bid,
            max_tradeable_spread=max_tradeable_spread,
            min_bid_improvement=min_bid_improvement,
            min_spread_improvement=min_spread_improvement,
        )
        _write_delta_csv(csv_path, delta_rows)

        counts = {
            "newly_tradeable_markets": 0,
            "improved_two_sided_markets": 0,
            "newly_two_sided_markets": 0,
            "stable_two_sided_markets": 0,
            "worsened_two_sided_markets": 0,
            "lost_two_sided_markets": 0,
            "lost_tradeable_markets": 0,
            "new_listings": 0,
            "dropped_listings": 0,
        }
        for row in delta_rows:
            label = str(row.get("change_label") or "")
            if label == "newly_tradeable":
                counts["newly_tradeable_markets"] += 1
            elif label == "improved_two_sided":
                counts["improved_two_sided_markets"] += 1
            elif label == "newly_two_sided":
                counts["newly_two_sided_markets"] += 1
            elif label == "stable_two_sided":
                counts["stable_two_sided_markets"] += 1
            elif label == "worsened_two_sided":
                counts["worsened_two_sided_markets"] += 1
            elif label == "lost_two_sided":
                counts["lost_two_sided_markets"] += 1
            elif label == "lost_tradeable":
                counts["lost_tradeable_markets"] += 1
            elif label == "new_listing":
                counts["new_listings"] += 1
            elif label == "dropped_listing":
                counts["dropped_listings"] += 1

        board_change_label = "stale"
        if counts["newly_tradeable_markets"] > 0 or counts["improved_two_sided_markets"] > 0:
            board_change_label = "improving"
        elif counts["lost_tradeable_markets"] > 0 or counts["worsened_two_sided_markets"] > 0:
            board_change_label = "deteriorating"
        elif counts["newly_two_sided_markets"] > 0 or counts["lost_two_sided_markets"] > 0:
            board_change_label = "mixed"

        summary.update(
            {
                "board_change_label": board_change_label,
                "previous_snapshot_at": previous_snapshot_at,
                "latest_snapshot_at": latest_snapshot_at,
                **counts,
                "top_markets": delta_rows[:top_n],
                "output_csv": str(csv_path),
            }
        )

    output_path = out_dir / f"kalshi_nonsports_deltas_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
