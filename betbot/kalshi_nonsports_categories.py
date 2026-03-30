from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_nonsports_quality import _parse_bool, _parse_float, _parse_timestamp, load_history_rows


def _category_rank_score(
    *,
    recurring_markets: int,
    persistent_tradeable_markets: int,
    improved_two_sided_markets: int,
    two_sided_ratio: float,
    mean_yes_bid: float,
    mean_spread: float,
) -> float:
    spread_component = max(0.0, 0.05 - min(mean_spread, 0.05)) * 100.0
    return round(
        recurring_markets * 8.0
        + persistent_tradeable_markets * 18.0
        + improved_two_sided_markets * 10.0
        + two_sided_ratio * 20.0
        + min(mean_yes_bid, 0.25) * 20.0
        + spread_component,
        6,
    )


def build_category_rows(
    *,
    history_rows: list[dict[str, str]],
    min_tradeable_yes_bid: float,
    max_tradeable_spread: float,
) -> list[dict[str, Any]]:
    snapshots = sorted(
        {
            str(row.get("captured_at") or "").strip()
            for row in history_rows
            if str(row.get("captured_at") or "").strip()
        },
        key=lambda value: _parse_timestamp(value) or datetime.min.replace(tzinfo=timezone.utc),
    )
    latest_snapshot = snapshots[-1] if snapshots else None
    previous_snapshot = snapshots[-2] if len(snapshots) >= 2 else None

    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        category = str(row.get("category") or "").strip() or "Unknown"
        grouped.setdefault(category, []).append(row)

    category_rows: list[dict[str, Any]] = []
    for category, rows in grouped.items():
        market_tickers = {str(row.get("market_ticker") or "").strip() for row in rows if str(row.get("market_ticker") or "").strip()}
        snapshots_seen = {
            str(row.get("captured_at") or "").strip()
            for row in rows
            if str(row.get("captured_at") or "").strip()
        }
        two_sided_rows = [row for row in rows if _parse_bool(str(row.get("two_sided_book") or ""))]
        two_sided_snapshot_keys = {
            str(row.get("captured_at") or "").strip()
            for row in two_sided_rows
            if str(row.get("captured_at") or "").strip()
        }
        recurring_markets = 0
        persistent_tradeable_markets = 0
        improved_two_sided_markets = 0

        per_market: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            ticker = str(row.get("market_ticker") or "").strip()
            if ticker:
                per_market.setdefault(ticker, []).append(row)
        for market_rows in per_market.values():
            snapshot_map = {
                str(row.get("captured_at") or "").strip(): row
                for row in market_rows
                if str(row.get("captured_at") or "").strip()
            }
            two_sided_snapshots = [
                key for key, row in snapshot_map.items()
                if _parse_bool(str(row.get("two_sided_book") or ""))
            ]
            tradeable_snapshots = [
                key for key, row in snapshot_map.items()
                if _parse_bool(str(row.get("two_sided_book") or ""))
                and (_parse_float(str(row.get("yes_bid_dollars") or "")) or 0.0) >= min_tradeable_yes_bid
                and (_parse_float(str(row.get("spread_dollars") or "")) or 1.0) <= max_tradeable_spread
            ]
            if len(two_sided_snapshots) >= 2:
                recurring_markets += 1
            if len(tradeable_snapshots) >= 2:
                persistent_tradeable_markets += 1

            if previous_snapshot and latest_snapshot:
                previous_row = snapshot_map.get(previous_snapshot)
                latest_row = snapshot_map.get(latest_snapshot)
                if previous_row and latest_row:
                    previous_two_sided = _parse_bool(str(previous_row.get("two_sided_book") or ""))
                    latest_two_sided = _parse_bool(str(latest_row.get("two_sided_book") or ""))
                    previous_yes_bid = _parse_float(str(previous_row.get("yes_bid_dollars") or "")) or 0.0
                    latest_yes_bid = _parse_float(str(latest_row.get("yes_bid_dollars") or "")) or 0.0
                    previous_spread = _parse_float(str(previous_row.get("spread_dollars") or "")) or 1.0
                    latest_spread = _parse_float(str(latest_row.get("spread_dollars") or "")) or 1.0
                    if latest_two_sided and previous_two_sided and (
                        latest_yes_bid > previous_yes_bid or latest_spread < previous_spread
                    ):
                        improved_two_sided_markets += 1

        valid_yes_bids = [
            value for value in (_parse_float(str(row.get("yes_bid_dollars") or "")) for row in rows)
            if value is not None
        ]
        valid_spreads = [
            value for value in (_parse_float(str(row.get("spread_dollars") or "")) for row in rows)
            if value is not None
        ]
        mean_yes_bid = sum(valid_yes_bids) / len(valid_yes_bids) if valid_yes_bids else 0.0
        mean_spread = sum(valid_spreads) / len(valid_spreads) if valid_spreads else 1.0
        two_sided_ratio = len(two_sided_rows) / len(rows) if rows else 0.0

        category_label = "dormant"
        if persistent_tradeable_markets > 0:
            category_label = "tradeable"
        elif recurring_markets > 0 or improved_two_sided_markets > 0:
            category_label = "watch"
        elif two_sided_snapshot_keys:
            category_label = "thin"

        category_rows.append(
            {
                "category": category,
                "observation_rows": len(rows),
                "distinct_markets": len(market_tickers),
                "snapshots_seen": len(snapshots_seen),
                "two_sided_rows": len(two_sided_rows),
                "two_sided_snapshots": len(two_sided_snapshot_keys),
                "two_sided_ratio": round(two_sided_ratio, 6),
                "mean_yes_bid_dollars": round(mean_yes_bid, 6),
                "mean_spread_dollars": round(mean_spread, 6),
                "recurring_markets": recurring_markets,
                "persistent_tradeable_markets": persistent_tradeable_markets,
                "improved_two_sided_markets": improved_two_sided_markets,
                "category_label": category_label,
                "category_rank_score": _category_rank_score(
                    recurring_markets=recurring_markets,
                    persistent_tradeable_markets=persistent_tradeable_markets,
                    improved_two_sided_markets=improved_two_sided_markets,
                    two_sided_ratio=two_sided_ratio,
                    mean_yes_bid=mean_yes_bid,
                    mean_spread=mean_spread,
                ),
            }
        )

    category_rows.sort(
        key=lambda row: (
            row["category_label"] == "tradeable",
            row["category_label"] == "watch",
            row["category_label"] == "thin",
            row["category_rank_score"],
        ),
        reverse=True,
    )
    return category_rows


def _write_category_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "category",
        "observation_rows",
        "distinct_markets",
        "snapshots_seen",
        "two_sided_rows",
        "two_sided_snapshots",
        "two_sided_ratio",
        "mean_yes_bid_dollars",
        "mean_spread_dollars",
        "recurring_markets",
        "persistent_tradeable_markets",
        "improved_two_sided_markets",
        "category_label",
        "category_rank_score",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_categories(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    min_tradeable_yes_bid: float = 0.05,
    max_tradeable_spread: float = 0.03,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    category_rows = build_category_rows(
        history_rows=history_rows,
        min_tradeable_yes_bid=min_tradeable_yes_bid,
        max_tradeable_spread=max_tradeable_spread,
    )

    top_category = category_rows[0] if category_rows else None
    total_two_sided_rows = sum(int(row["two_sided_rows"]) for row in category_rows)
    concentration_warning = None
    if top_category and total_two_sided_rows > 0:
        share = float(top_category["two_sided_rows"]) / total_two_sided_rows
        if share >= 0.8:
            concentration_warning = (
                f"Two-sided liquidity is heavily concentrated in {top_category['category']} "
                f"({share:.0%} of observed two-sided rows)."
            )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_categories_{stamp}.csv"
    _write_category_csv(csv_path, category_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "rows_in_history": len(history_rows),
        "categories_observed": len(category_rows),
        "tradeable_categories": sum(1 for row in category_rows if row["category_label"] == "tradeable"),
        "watch_categories": sum(1 for row in category_rows if row["category_label"] == "watch"),
        "thin_categories": sum(1 for row in category_rows if row["category_label"] == "thin"),
        "dormant_categories": sum(1 for row in category_rows if row["category_label"] == "dormant"),
        "top_categories": category_rows[:top_n],
        "concentration_warning": concentration_warning,
        "status": "ready",
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_nonsports_categories_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
