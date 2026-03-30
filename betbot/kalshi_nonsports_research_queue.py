from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_nonsports_persistence import build_persistence_rows
from betbot.kalshi_nonsports_pressure import build_pressure_rows
from betbot.kalshi_nonsports_priors import load_prior_rows
from betbot.kalshi_nonsports_quality import (
    _parse_bool,
    _parse_float,
    _parse_timestamp,
    build_quality_rows,
    load_history_rows,
)


def _latest_market_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)

    latest_rows: dict[str, dict[str, str]] = {}
    for ticker, rows in grouped.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest_rows[ticker] = rows_sorted[-1]
    return latest_rows


def _research_priority_score(
    *,
    quality_rank_score: float,
    persistence_rank_score: float,
    pressure_rank_score: float,
    latest_execution_fit_score: float,
    cheapest_ask_dollars: float,
    hours_to_close: float | None,
    category_prior_count: int,
    current_two_sided_book: bool,
) -> float:
    category_bonus = 8.0 if category_prior_count == 0 else max(0.0, 4.0 - category_prior_count)
    capital_bonus = max(0.0, 0.25 - min(cheapest_ask_dollars, 0.25)) * 40.0
    book_bonus = 6.0 if current_two_sided_book else 0.0
    timing_bonus = 0.0
    if hours_to_close is not None and hours_to_close > 0:
        timing_bonus = max(0.0, 168.0 - min(hours_to_close, 168.0)) / 24.0
    return round(
        quality_rank_score * 0.35
        + persistence_rank_score * 0.3
        + pressure_rank_score * 0.15
        + min(latest_execution_fit_score, 100.0) * 0.1
        + capital_bonus
        + category_bonus
        + book_bonus
        + timing_bonus,
        6,
    )


def _research_priority_label(score: float) -> str:
    if score >= 55.0:
        return "high"
    if score >= 35.0:
        return "medium"
    return "low"


def _research_prompt(
    *,
    market_title: str,
    market_ticker: str,
    cheapest_side: str,
    cheapest_ask_dollars: float,
) -> str:
    return (
        f"Estimate the fair yes probability for {market_title} ({market_ticker}). "
        f"Current cheapest side to test is {cheapest_side} at ${cheapest_ask_dollars:.2f}; "
        "decide which side looks overpriced and give a short thesis."
    )


def build_research_queue_rows(
    *,
    history_rows: list[dict[str, str]],
    prior_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    latest_rows = _latest_market_rows(history_rows)
    quality_rows = {
        str(row["market_ticker"]): row
        for row in build_quality_rows(
            history_rows=history_rows,
            min_observations=3,
            min_mean_yes_bid=0.02,
            min_two_sided_ratio=0.4,
            max_mean_spread=0.05,
        )
    }
    persistence_rows = {
        str(row["market_ticker"]): row
        for row in build_persistence_rows(
            history_rows=history_rows,
            min_tradeable_yes_bid=0.03,
            max_tradeable_spread=0.05,
            min_tradeable_snapshot_count=2,
            min_consecutive_tradeable_snapshots=2,
        )
    }
    pressure_rows = {
        str(row["market_ticker"]): row
        for row in build_pressure_rows(
            history_rows=history_rows,
            min_observations=3,
            min_latest_yes_bid=0.01,
            max_latest_spread=0.03,
            min_two_sided_ratio=0.4,
            min_recent_bid_change=0.01,
        )
    }

    prior_tickers = {
        str(row.get("market_ticker") or "").strip()
        for row in prior_rows
        if str(row.get("market_ticker") or "").strip()
    }
    category_prior_counts: dict[str, int] = {}
    for ticker in prior_tickers:
        latest = latest_rows.get(ticker)
        category = str(latest.get("category") or "").strip() if latest else ""
        if category:
            category_prior_counts[category] = category_prior_counts.get(category, 0) + 1

    queue_rows: list[dict[str, Any]] = []
    for ticker, latest in latest_rows.items():
        if ticker in prior_tickers:
            continue
        quality = quality_rows.get(ticker)
        persistence = persistence_rows.get(ticker)
        pressure = pressure_rows.get(ticker)
        if quality is None or persistence is None or pressure is None:
            continue

        current_two_sided = _parse_bool(str(latest.get("two_sided_book") or ""))
        yes_ask = _parse_float(str(latest.get("yes_ask_dollars") or ""))
        yes_bid = _parse_float(str(latest.get("yes_bid_dollars") or ""))
        if yes_ask is None:
            continue
        if yes_bid is None:
            yes_bid = 0.0
        no_ask = round(1.0 - yes_bid, 6)
        no_bid = round(1.0 - yes_ask, 6)
        cheapest_side = "yes" if yes_ask <= no_ask else "no"
        cheapest_ask = round(min(yes_ask, no_ask), 6)
        category = str(latest.get("category") or "")
        hours_to_close = _parse_float(str(latest.get("hours_to_close") or ""))
        latest_execution_fit = _parse_float(str(latest.get("execution_fit_score") or "")) or 0.0
        score = _research_priority_score(
            quality_rank_score=float(quality.get("quality_rank_score") or 0.0),
            persistence_rank_score=float(persistence.get("persistence_rank_score") or 0.0),
            pressure_rank_score=float(pressure.get("pressure_rank_score") or 0.0),
            latest_execution_fit_score=latest_execution_fit,
            cheapest_ask_dollars=cheapest_ask,
            hours_to_close=hours_to_close,
            category_prior_count=category_prior_counts.get(category, 0),
            current_two_sided_book=current_two_sided,
        )
        queue_rows.append(
            {
                "market_ticker": ticker,
                "category": category,
                "event_title": str(latest.get("event_title") or ""),
                "market_title": str(latest.get("market_title") or ""),
                "close_time": str(latest.get("close_time") or ""),
                "hours_to_close": hours_to_close if hours_to_close is not None else "",
                "yes_bid_dollars": round(yes_bid, 6),
                "yes_ask_dollars": round(yes_ask, 6),
                "no_bid_dollars": no_bid,
                "no_ask_dollars": no_ask,
                "current_two_sided_book": current_two_sided,
                "book_state": "two_sided" if current_two_sided else "one_sided",
                "cheapest_side": cheapest_side,
                "cheapest_ask_dollars": cheapest_ask,
                "latest_execution_fit_score": round(latest_execution_fit, 6),
                "quality_label": quality.get("quality_label"),
                "quality_rank_score": quality.get("quality_rank_score"),
                "persistence_label": persistence.get("persistence_label"),
                "persistence_rank_score": persistence.get("persistence_rank_score"),
                "pressure_label": pressure.get("pressure_label"),
                "pressure_rank_score": pressure.get("pressure_rank_score"),
                "category_prior_count": category_prior_counts.get(category, 0),
                "research_priority_score": score,
                "research_priority_label": _research_priority_label(score),
                "research_prompt": _research_prompt(
                    market_title=str(latest.get("market_title") or ""),
                    market_ticker=ticker,
                    cheapest_side=cheapest_side,
                    cheapest_ask_dollars=cheapest_ask,
                ),
            }
        )

    queue_rows.sort(
        key=lambda row: (
            row["research_priority_label"] == "high",
            row["research_priority_label"] == "medium",
            row["research_priority_score"],
        ),
        reverse=True,
    )
    return queue_rows


def _write_research_queue_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "market_ticker",
        "category",
        "event_title",
        "market_title",
        "close_time",
        "hours_to_close",
        "yes_bid_dollars",
        "yes_ask_dollars",
        "no_bid_dollars",
        "no_ask_dollars",
        "current_two_sided_book",
        "book_state",
        "cheapest_side",
        "cheapest_ask_dollars",
        "latest_execution_fit_score",
        "quality_label",
        "quality_rank_score",
        "persistence_label",
        "persistence_rank_score",
        "pressure_label",
        "pressure_rank_score",
        "category_prior_count",
        "research_priority_score",
        "research_priority_label",
        "research_prompt",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_kalshi_nonsports_research_queue(
    *,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    output_dir: str = "outputs",
    top_n: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    priors_path = Path(priors_csv)
    history_path = Path(history_csv)
    prior_rows = load_prior_rows(priors_path)
    history_rows = load_history_rows(history_path)
    queue_rows = build_research_queue_rows(history_rows=history_rows, prior_rows=prior_rows)

    top_row = queue_rows[0] if queue_rows else None
    categories_without_priors = sorted(
        {str(row["category"]) for row in queue_rows if int(row["category_prior_count"]) == 0}
    )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_nonsports_research_queue_{stamp}.csv"
    _write_research_queue_csv(csv_path, queue_rows)

    summary = {
        "captured_at": captured_at.isoformat(),
        "priors_csv": str(priors_path),
        "history_csv": str(history_path),
        "prior_rows": len(prior_rows),
        "uncovered_research_markets": len(queue_rows),
        "high_priority_markets": sum(1 for row in queue_rows if row["research_priority_label"] == "high"),
        "medium_priority_markets": sum(1 for row in queue_rows if row["research_priority_label"] == "medium"),
        "categories_without_priors": categories_without_priors,
        "top_market_ticker": top_row.get("market_ticker") if isinstance(top_row, dict) else None,
        "top_market_category": top_row.get("category") if isinstance(top_row, dict) else None,
        "top_market_cheapest_side": top_row.get("cheapest_side") if isinstance(top_row, dict) else None,
        "top_market_cheapest_ask_dollars": top_row.get("cheapest_ask_dollars") if isinstance(top_row, dict) else None,
        "top_market_research_prompt": top_row.get("research_prompt") if isinstance(top_row, dict) else None,
        "top_markets": queue_rows[:top_n],
        "status": "ready",
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_nonsports_research_queue_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
