from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from betbot.kalshi_micro_watch_history import default_watch_history_path, summarize_watch_history
from betbot.kalshi_nonsports_pressure import build_pressure_rows
from betbot.kalshi_nonsports_priors import build_prior_rows, load_prior_rows
from betbot.kalshi_nonsports_quality import _parse_bool, _parse_float, _parse_timestamp, load_history_rows
from betbot.kalshi_nonsports_thresholds import build_threshold_rows


def _group_history_rows(history_rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in history_rows:
        ticker = str(row.get("market_ticker") or "").strip()
        if ticker:
            grouped.setdefault(ticker, []).append(row)
    return grouped


def _latest_market_rows(history_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    latest_rows: dict[str, dict[str, str]] = {}
    for ticker, rows in _group_history_rows(history_rows).items():
        rows_sorted = sorted(
            rows,
            key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
        )
        latest_rows[ticker] = rows_sorted[-1]
    return latest_rows


def _select_focus_market(
    *,
    watch_history_summary: dict[str, Any],
    threshold_rows: list[dict[str, Any]],
    pressure_rows: list[dict[str, Any]],
    prior_rows: list[dict[str, Any]],
) -> tuple[str | None, str | None, str]:
    watch_ticker = str(watch_history_summary.get("latest_focus_market_ticker") or "").strip()
    watch_mode = str(watch_history_summary.get("latest_focus_market_mode") or "").strip()
    if watch_ticker:
        return (watch_ticker, watch_mode or None, "watch_history")

    threshold_top = next(
        (
            row for row in threshold_rows
            if str(row.get("threshold_label") or "") in {"tradeable_now", "approaching", "building"}
        ),
        None,
    )
    if isinstance(threshold_top, dict):
        ticker = str(threshold_top.get("market_ticker") or "").strip()
        return (ticker or None, "threshold", "thresholds")

    pressure_top = next(
        (
            row for row in pressure_rows
            if str(row.get("pressure_label") or "") in {"build", "watch", "thin"}
        ),
        None,
    )
    if isinstance(pressure_top, dict):
        ticker = str(pressure_top.get("market_ticker") or "").strip()
        return (ticker or None, "pressure", "pressure")

    prior_top = next((row for row in prior_rows if row.get("matched_live_market")), None)
    if isinstance(prior_top, dict):
        ticker = str(prior_top.get("market_ticker") or "").strip()
        return (ticker or None, "prior", "priors")

    return (None, None, "none")


def _find_market_row(rows: list[dict[str, Any]], ticker: str | None) -> dict[str, Any] | None:
    if not ticker:
        return None
    for row in rows:
        if str(row.get("market_ticker") or "").strip() == ticker:
            return row
    return None


def _market_context(
    *,
    market_rows: list[dict[str, str]],
    recent_limit: int,
) -> dict[str, Any]:
    rows_sorted = sorted(
        market_rows,
        key=lambda row: _parse_timestamp(str(row.get("captured_at") or "")) or datetime.min.replace(tzinfo=timezone.utc),
    )
    latest = rows_sorted[-1]
    previous = rows_sorted[-2] if len(rows_sorted) >= 2 else None
    first_seen = _parse_timestamp(str(rows_sorted[0].get("captured_at") or ""))
    last_seen = _parse_timestamp(str(latest.get("captured_at") or ""))
    tracked_hours = None
    if first_seen and last_seen:
        tracked_hours = round((last_seen - first_seen).total_seconds() / 3600.0, 6)

    def _numeric_field(row: dict[str, str] | None, field: str) -> float | None:
        if row is None:
            return None
        return _parse_float(str(row.get(field) or ""))

    observation_count = len(rows_sorted)
    two_sided_observations = sum(1 for row in rows_sorted if _parse_bool(str(row.get("two_sided_book") or "")))
    fillable_observations = sum(
        1 for row in rows_sorted if _parse_bool(str(row.get("ten_dollar_fillable_at_best_ask") or ""))
    )
    yes_bid_values = [
        value
        for value in (_numeric_field(row, "yes_bid_dollars") for row in rows_sorted)
        if value is not None
    ]

    latest_yes_bid = _numeric_field(latest, "yes_bid_dollars")
    latest_yes_ask = _numeric_field(latest, "yes_ask_dollars")
    latest_spread = _numeric_field(latest, "spread_dollars")
    previous_yes_bid = _numeric_field(previous, "yes_bid_dollars")
    previous_yes_ask = _numeric_field(previous, "yes_ask_dollars")
    previous_spread = _numeric_field(previous, "spread_dollars")

    recent_observations: list[dict[str, Any]] = []
    for row in rows_sorted[-recent_limit:]:
        recent_observations.append(
            {
                "captured_at": row.get("captured_at"),
                "yes_bid_dollars": _numeric_field(row, "yes_bid_dollars"),
                "yes_ask_dollars": _numeric_field(row, "yes_ask_dollars"),
                "spread_dollars": _numeric_field(row, "spread_dollars"),
                "two_sided_book": _parse_bool(str(row.get("two_sided_book") or "")),
                "ten_dollar_fillable_at_best_ask": _parse_bool(
                    str(row.get("ten_dollar_fillable_at_best_ask") or "")
                ),
                "execution_fit_score": _numeric_field(row, "execution_fit_score"),
            }
        )

    return {
        "category": str(latest.get("category") or ""),
        "series_ticker": str(latest.get("series_ticker") or ""),
        "event_ticker": str(latest.get("event_ticker") or ""),
        "event_title": str(latest.get("event_title") or ""),
        "market_title": str(latest.get("market_title") or ""),
        "close_time": str(latest.get("close_time") or ""),
        "first_seen": rows_sorted[0].get("captured_at"),
        "last_seen": latest.get("captured_at"),
        "tracked_hours": tracked_hours,
        "observation_count": observation_count,
        "two_sided_observations": two_sided_observations,
        "two_sided_ratio": round(two_sided_observations / observation_count, 6) if observation_count else 0.0,
        "fillable_observations": fillable_observations,
        "fillable_ratio": round(fillable_observations / observation_count, 6) if observation_count else 0.0,
        "latest_yes_bid_dollars": latest_yes_bid,
        "latest_yes_bid_size_contracts": _numeric_field(latest, "yes_bid_size_contracts"),
        "latest_yes_ask_dollars": latest_yes_ask,
        "latest_yes_ask_size_contracts": _numeric_field(latest, "yes_ask_size_contracts"),
        "latest_spread_dollars": latest_spread,
        "latest_liquidity_dollars": _numeric_field(latest, "liquidity_dollars"),
        "latest_volume_24h_contracts": _numeric_field(latest, "volume_24h_contracts"),
        "latest_open_interest_contracts": _numeric_field(latest, "open_interest_contracts"),
        "latest_hours_to_close": _numeric_field(latest, "hours_to_close"),
        "latest_execution_fit_score": _numeric_field(latest, "execution_fit_score"),
        "latest_two_sided_book": _parse_bool(str(latest.get("two_sided_book") or "")),
        "latest_ten_dollar_fillable_at_best_ask": _parse_bool(
            str(latest.get("ten_dollar_fillable_at_best_ask") or "")
        ),
        "previous_yes_bid_dollars": previous_yes_bid,
        "previous_yes_ask_dollars": previous_yes_ask,
        "previous_spread_dollars": previous_spread,
        "yes_bid_change_since_previous": (
            round((latest_yes_bid or 0.0) - previous_yes_bid, 6)
            if previous_yes_bid is not None and latest_yes_bid is not None
            else None
        ),
        "yes_ask_change_since_previous": (
            round((latest_yes_ask or 0.0) - previous_yes_ask, 6)
            if previous_yes_ask is not None and latest_yes_ask is not None
            else None
        ),
        "spread_change_since_previous": (
            round((latest_spread or 0.0) - previous_spread, 6)
            if previous_spread is not None and latest_spread is not None
            else None
        ),
        "min_yes_bid_dollars": round(min(yes_bid_values), 6) if yes_bid_values else None,
        "max_yes_bid_dollars": round(max(yes_bid_values), 6) if yes_bid_values else None,
        "recent_observations": recent_observations,
    }


def _action_plan(
    *,
    focus_ticker: str | None,
    watch_history_summary: dict[str, Any],
    threshold_row: dict[str, Any] | None,
    pressure_row: dict[str, Any] | None,
    prior_row: dict[str, Any] | None,
) -> tuple[str, str, str | None]:
    if not focus_ticker:
        return (
            "no_focus_market",
            "No focus market is active yet, so the bot should keep collecting board history before changing live behavior.",
            None,
        )

    threshold_label = str(threshold_row.get("threshold_label") or "") if isinstance(threshold_row, dict) else ""
    pressure_label = str(pressure_row.get("pressure_label") or "") if isinstance(pressure_row, dict) else ""
    focus_state = str(watch_history_summary.get("focus_market_state") or "")
    edge_to_yes_ask = prior_row.get("edge_to_yes_ask") if isinstance(prior_row, dict) else None
    best_entry_side = str(prior_row.get("best_entry_side") or "") if isinstance(prior_row, dict) else ""
    best_entry_edge = prior_row.get("best_entry_edge") if isinstance(prior_row, dict) else None
    prior_covered = bool(prior_row and prior_row.get("matched_live_market"))

    if best_entry_side == "no" and isinstance(best_entry_edge, float) and best_entry_edge > 0:
        return (
            "review_modeled_no_edge",
            "The model prefers the No side at current prices, and the live prior execution path supports side-aware orders, so this is a candidate for standard prior-backed review.",
            None,
        )
    if isinstance(edge_to_yes_ask, float) and edge_to_yes_ask > 0 and threshold_label in {"tradeable_now", "approaching"}:
        return (
            "review_modeled_edge",
            "The focus market has a positive modeled edge to the Yes ask and is already near the live review thresholds.",
            None,
        )
    if threshold_label == "tradeable_now":
        return (
            "review_live_threshold_cross",
            "The focus market already clears the live structural thresholds, so it is ready for manual review before any automated order flow.",
            None,
        )
    if threshold_label == "approaching":
        return (
            "watch_threshold_focus",
            "The focus market is trending toward the live thresholds, so the right move is tighter monitoring rather than immediate execution.",
            None,
        )
    if focus_state == "stalled_pressure_focus":
        research_prompt = None
        if not prior_covered:
            research_prompt = (
                f"Add a fair_yes_probability row for {focus_ticker} in data/research/kalshi_nonsports_priors.csv "
                "so the bot can compare board movement to a real thesis."
            )
        return (
            "hold_stalled_pressure",
            "The same pressure market has persisted without graduating into threshold approach, so the bot should hold and avoid treating repetition as edge.",
            research_prompt,
        )
    if pressure_label == "build":
        research_prompt = None
        if not prior_covered:
            research_prompt = (
                f"Add a fair_yes_probability row for {focus_ticker} in data/research/kalshi_nonsports_priors.csv "
                "to turn this pressure build into a thesis-backed watch item."
            )
        return (
            "watch_pressure_build",
            "The focus market is building structurally, but it still needs either threshold progress or a modeled thesis before live review.",
            research_prompt,
        )
    if not prior_covered:
        return (
            "add_manual_prior",
            "The board has identified a focus market, but there is still no fair-value prior to judge whether the price is actually good.",
            (
                f"Add a fair_yes_probability row for {focus_ticker} in data/research/kalshi_nonsports_priors.csv "
                "with confidence, thesis, and source_note."
            ),
        )
    return (
        "collect_more_history",
        "The focus market has context now, but it still needs more time-series improvement before the bot should escalate.",
        None,
    )


def build_focus_dossier(
    *,
    history_rows: list[dict[str, str]],
    watch_history_summary: dict[str, Any],
    prior_rows: list[dict[str, str]],
    recent_observation_limit: int = 5,
) -> dict[str, Any]:
    pressure_rows = build_pressure_rows(
        history_rows=history_rows,
        min_observations=3,
        min_latest_yes_bid=0.02,
        max_latest_spread=0.02,
        min_two_sided_ratio=0.5,
        min_recent_bid_change=0.01,
    )
    threshold_rows = build_threshold_rows(
        history_rows=history_rows,
        target_yes_bid=0.05,
        target_spread=0.02,
        recent_window=5,
        max_hours_to_target=6.0,
        min_recent_two_sided_ratio=0.5,
        min_observations=3,
    )
    prior_context_rows = build_prior_rows(
        prior_rows=prior_rows,
        latest_market_rows=_latest_market_rows(history_rows),
    )
    focus_ticker, focus_mode, focus_source = _select_focus_market(
        watch_history_summary=watch_history_summary,
        threshold_rows=threshold_rows,
        pressure_rows=pressure_rows,
        prior_rows=prior_context_rows,
    )

    dossier: dict[str, Any] = {
        "focus_market_ticker": focus_ticker,
        "focus_market_mode": focus_mode,
        "focus_market_source": focus_source,
        "board_regime": watch_history_summary.get("board_regime"),
        "board_regime_reason": watch_history_summary.get("board_regime_reason"),
        "focus_market_state": watch_history_summary.get("focus_market_state"),
        "focus_market_state_reason": watch_history_summary.get("focus_market_state_reason"),
        "status": "ready",
    }

    if not focus_ticker:
        action_hint, action_reason, research_prompt = _action_plan(
            focus_ticker=None,
            watch_history_summary=watch_history_summary,
            threshold_row=None,
            pressure_row=None,
            prior_row=None,
        )
        dossier.update(
            {
                "action_hint": action_hint,
                "action_reason": action_reason,
                "research_prompt": research_prompt,
                "recent_observations": [],
            }
        )
        return dossier

    grouped = _group_history_rows(history_rows)
    market_rows = grouped.get(focus_ticker, [])
    if not market_rows:
        dossier.update(
            {
                "status": "missing_focus_market_history",
                "action_hint": "collect_more_history",
                "action_reason": f"{focus_ticker} is the selected focus market, but it is missing from the saved board history.",
                "research_prompt": None,
                "recent_observations": [],
            }
        )
        return dossier

    market_context = _market_context(
        market_rows=market_rows,
        recent_limit=recent_observation_limit,
    )
    pressure_row = _find_market_row(pressure_rows, focus_ticker)
    threshold_row = _find_market_row(threshold_rows, focus_ticker)
    prior_row = _find_market_row(prior_context_rows, focus_ticker)
    action_hint, action_reason, research_prompt = _action_plan(
        focus_ticker=focus_ticker,
        watch_history_summary=watch_history_summary,
        threshold_row=threshold_row,
        pressure_row=pressure_row,
        prior_row=prior_row,
    )

    dossier.update(
        {
            **market_context,
            "pressure_label": pressure_row.get("pressure_label") if isinstance(pressure_row, dict) else None,
            "pressure_rank_score": pressure_row.get("pressure_rank_score") if isinstance(pressure_row, dict) else None,
            "recent_yes_bid_change_dollars": (
                pressure_row.get("recent_yes_bid_change_dollars") if isinstance(pressure_row, dict) else None
            ),
            "recent_spread_change_dollars": (
                pressure_row.get("recent_spread_change_dollars") if isinstance(pressure_row, dict) else None
            ),
            "threshold_label": threshold_row.get("threshold_label") if isinstance(threshold_row, dict) else None,
            "threshold_rank_score": threshold_row.get("threshold_rank_score") if isinstance(threshold_row, dict) else None,
            "hours_to_tradeable_target": (
                threshold_row.get("hours_to_tradeable_target") if isinstance(threshold_row, dict) else None
            ),
            "yes_bid_gap_to_0_05": threshold_row.get("yes_bid_gap_to_0_05") if isinstance(threshold_row, dict) else None,
            "spread_gap_to_0_02": threshold_row.get("spread_gap_to_0_02") if isinstance(threshold_row, dict) else None,
            "improvement_events_recent": (
                threshold_row.get("improvement_events_recent") if isinstance(threshold_row, dict) else None
            ),
            "prior_covered": bool(prior_row and prior_row.get("matched_live_market")),
            "prior_confidence": prior_row.get("confidence") if isinstance(prior_row, dict) else None,
            "prior_fair_yes_probability": (
                prior_row.get("fair_yes_probability") if isinstance(prior_row, dict) else None
            ),
            "prior_fair_no_probability": (
                prior_row.get("fair_no_probability") if isinstance(prior_row, dict) else None
            ),
            "prior_edge_to_yes_bid": prior_row.get("edge_to_yes_bid") if isinstance(prior_row, dict) else None,
            "prior_edge_to_yes_ask": prior_row.get("edge_to_yes_ask") if isinstance(prior_row, dict) else None,
            "prior_edge_to_mid": prior_row.get("edge_to_mid") if isinstance(prior_row, dict) else None,
            "prior_edge_to_no_bid": prior_row.get("edge_to_no_bid") if isinstance(prior_row, dict) else None,
            "prior_edge_to_no_ask": prior_row.get("edge_to_no_ask") if isinstance(prior_row, dict) else None,
            "prior_edge_to_no_mid": prior_row.get("edge_to_no_mid") if isinstance(prior_row, dict) else None,
            "prior_best_entry_side": prior_row.get("best_entry_side") if isinstance(prior_row, dict) else None,
            "prior_best_entry_edge": prior_row.get("best_entry_edge") if isinstance(prior_row, dict) else None,
            "prior_best_entry_price_dollars": (
                prior_row.get("best_entry_price_dollars") if isinstance(prior_row, dict) else None
            ),
            "prior_thesis": prior_row.get("thesis") if isinstance(prior_row, dict) else None,
            "prior_source_note": prior_row.get("source_note") if isinstance(prior_row, dict) else None,
            "prior_updated_at": prior_row.get("updated_at") if isinstance(prior_row, dict) else None,
            "action_hint": action_hint,
            "action_reason": action_reason,
            "research_prompt": research_prompt,
        }
    )
    return dossier


def run_kalshi_focus_dossier(
    *,
    history_csv: str = "outputs/kalshi_nonsports_history.csv",
    watch_history_csv: str | None = None,
    priors_csv: str = "data/research/kalshi_nonsports_priors.csv",
    output_dir: str = "outputs",
    recent_observation_limit: int = 5,
    watch_history_summary: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    captured_at = now or datetime.now(timezone.utc)
    history_path = Path(history_csv)
    priors_path = Path(priors_csv)
    watch_history_path = Path(watch_history_csv) if watch_history_csv else default_watch_history_path(output_dir)
    effective_watch_history_summary = watch_history_summary or summarize_watch_history(watch_history_path)
    if not history_path.exists():
        dossier = {
            "captured_at": captured_at.isoformat(),
            "history_csv": str(history_path),
            "watch_history_csv": str(watch_history_path),
            "priors_csv": str(priors_path),
            "status": "missing_history",
            "focus_market_ticker": None,
            "focus_market_mode": None,
            "focus_market_source": "none",
            "board_regime": effective_watch_history_summary.get("board_regime"),
            "board_regime_reason": effective_watch_history_summary.get("board_regime_reason"),
            "focus_market_state": effective_watch_history_summary.get("focus_market_state"),
            "focus_market_state_reason": effective_watch_history_summary.get("focus_market_state_reason"),
            "action_hint": "collect_more_history",
            "action_reason": "The focus dossier needs at least one captured board history file before it can summarize a market.",
            "research_prompt": None,
            "recent_observations": [],
        }
        stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        output_path = out_dir / f"kalshi_focus_dossier_{stamp}.json"
        output_path.write_text(json.dumps(dossier, indent=2), encoding="utf-8")
        dossier["output_file"] = str(output_path)
        return dossier

    history_rows = load_history_rows(history_path)
    dossier = build_focus_dossier(
        history_rows=history_rows,
        watch_history_summary=effective_watch_history_summary,
        prior_rows=load_prior_rows(priors_path),
        recent_observation_limit=recent_observation_limit,
    )
    dossier.update(
        {
            "captured_at": captured_at.isoformat(),
            "history_csv": str(history_path),
            "watch_history_csv": str(watch_history_path),
            "priors_csv": str(priors_path),
        }
    )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"kalshi_focus_dossier_{stamp}.json"
    output_path.write_text(json.dumps(dossier, indent=2), encoding="utf-8")
    dossier["output_file"] = str(output_path)
    return dossier
