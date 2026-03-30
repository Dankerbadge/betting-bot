from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


def _parse_float(value: str) -> float | None:
    text = str(value or "").strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_bool(value: str) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _parse_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if text == "":
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _quality_rank_score(
    *,
    observation_count: int,
    two_sided_ratio: float,
    mean_yes_bid: float,
    mean_spread: float,
    mean_execution_fit_score: float,
) -> float:
    spread_component = max(0.0, 0.05 - min(mean_spread, 0.05)) * 100.0
    return round(
        min(observation_count, 24) * 0.75
        + two_sided_ratio * 12.0
        + min(mean_yes_bid, 0.25) * 20.0
        + spread_component
        + min(mean_execution_fit_score, 100.0) * 0.25,
        6,
    )


def load_history_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise ValueError(f"History CSV not found: {path}")
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def build_quality_rows(
    *,
    history_rows: list[dict[str, str]],
    min_observations: int,
    min_mean_yes_bid: float,
    min_two_sided_ratio: float,
    max_mean_spread: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker == "":
            continue
        grouped.setdefault(ticker, []).append(row)

    quality_rows: list[dict[str, Any]] = []
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest = rows_sorted[-1]
        observation_count = len(rows_sorted)
        two_sided_count = sum(1 for row in rows_sorted if _parse_bool(str(row.get("two_sided_book") or "")))
        yes_bids = [_parse_float(str(row.get("yes_bid_dollars") or "")) for row in rows_sorted]
        yes_asks = [_parse_float(str(row.get("yes_ask_dollars") or "")) for row in rows_sorted]
        spreads = [_parse_float(str(row.get("spread_dollars") or "")) for row in rows_sorted]
        fit_scores = [_parse_float(str(row.get("execution_fit_score") or "")) for row in rows_sorted]

        valid_yes_bids = [value for value in yes_bids if value is not None]
        valid_yes_asks = [value for value in yes_asks if value is not None]
        valid_spreads = [value for value in spreads if value is not None]
        valid_fit_scores = [value for value in fit_scores if value is not None]

        mean_yes_bid = sum(valid_yes_bids) / len(valid_yes_bids) if valid_yes_bids else 0.0
        max_yes_bid = max(valid_yes_bids) if valid_yes_bids else 0.0
        mean_yes_ask = sum(valid_yes_asks) / len(valid_yes_asks) if valid_yes_asks else 0.0
        mean_spread = sum(valid_spreads) / len(valid_spreads) if valid_spreads else 1.0
        mean_execution_fit_score = (
            sum(valid_fit_scores) / len(valid_fit_scores) if valid_fit_scores else 0.0
        )
        two_sided_ratio = two_sided_count / observation_count if observation_count else 0.0

        quality_label = "penny_noise"
        if (
            observation_count >= min_observations
            and mean_yes_bid >= min_mean_yes_bid
            and two_sided_ratio >= min_two_sided_ratio
            and mean_spread <= max_mean_spread
        ):
            quality_label = "meaningful"
        elif max_yes_bid >= min_mean_yes_bid and two_sided_count > 0:
            quality_label = "watchlist"
        elif two_sided_count > 0:
            quality_label = "thin_two_sided"

        quality_rows.append(
            {
                "market_ticker": ticker,
                "category": str(latest.get("category") or ""),
                "event_title": str(latest.get("event_title") or ""),
                "market_title": str(latest.get("market_title") or ""),
                "first_seen": str(rows_sorted[0].get("captured_at") or ""),
                "last_seen": str(latest.get("captured_at") or ""),
                "observation_count": observation_count,
                "two_sided_observations": two_sided_count,
                "two_sided_ratio": round(two_sided_ratio, 6),
                "mean_yes_bid_dollars": round(mean_yes_bid, 6),
                "max_yes_bid_dollars": round(max_yes_bid, 6),
                "mean_yes_ask_dollars": round(mean_yes_ask, 6),
                "mean_spread_dollars": round(mean_spread, 6),
                "mean_execution_fit_score": round(mean_execution_fit_score, 6),
                "hours_to_close_latest": _parse_float(str(latest.get("hours_to_close") or "")) or "",
                "quality_label": quality_label,
                "quality_rank_score": _quality_rank_score(
                    observation_count=observation_count,
                    two_sided_ratio=two_sided_ratio,
                    mean_yes_bid=mean_yes_bid,
                    mean_spread=mean_spread,
                    mean_execution_fit_score=mean_execution_fit_score,
                ),
            }
        )

    quality_rows.sort(
        key=lambda row: (
            row["quality_label"] == "meaningful",
            row["quality_label"] == "watchlist",
            row["quality_rank_score"],
        ),
        reverse=True,
    )
    return quality_rows


def _write_quality_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market_ticker",
        "category",
        "event_title",
        "market_title",
        "first_seen",
        "last_seen",
        "observation_count",
        "two_sided_observations",
        "two_sided_ratio",
        "mean_yes_bid_dollars",
        "max_yes_bid_dollars",
        "mean_yes_ask_dollars",
        "mean_spread_dollars",
        "mean_execution_fit_score",
        "hours_to_close_latest",
        "quality_label",
        "quality_rank_score",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_quality(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    min_observations: int = 3,
    min_mean_yes_bid: float = 0.05,
    min_two_sided_ratio: float = 0.5,
    max_mean_spread: float = 0.03,
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    history_rows = load_history_rows(history_path)
    quality_rows = build_quality_rows(
        history_rows=history_rows,
        min_observations=min_observations,
        min_mean_yes_bid=min_mean_yes_bid,
        min_two_sided_ratio=min_two_sided_ratio,
        max_mean_spread=max_mean_spread,
    )

    meaningful_count = sum(1 for row in quality_rows if row["quality_label"] == "meaningful")
    watchlist_count = sum(1 for row in quality_rows if row["quality_label"] == "watchlist")
    thin_two_sided_count = sum(1 for row in quality_rows if row["quality_label"] == "thin_two_sided")
    penny_noise_count = sum(1 for row in quality_rows if row["quality_label"] == "penny_noise")

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_quality_{stamp}.csv"
    _write_quality_csv(csv_path, quality_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "history_csv": str(history_path),
        "rows_in_history": len(history_rows),
        "distinct_markets": len(quality_rows),
        "meaningful_markets": meaningful_count,
        "watchlist_markets": watchlist_count,
        "thin_two_sided_markets": thin_two_sided_count,
        "penny_noise_markets": penny_noise_count,
        "top_markets": quality_rows[:top_n],
        "status": "ready",
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_nonsports_quality_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
