from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


WATCH_HISTORY_FIELDNAMES = [
    "recorded_at",
    "capture_status",
    "capture_scan_status",
    "status_recommendation",
    "status_trade_gate_status",
    "trade_gate_pass",
    "meaningful_candidates_yes_bid_ge_0_05",
    "persistent_tradeable_markets",
    "improved_two_sided_markets",
    "pressure_build_markets",
    "threshold_approaching_markets",
    "top_pressure_market_ticker",
    "top_threshold_market_ticker",
    "board_change_label",
    "top_category",
    "top_category_label",
    "category_concentration_warning",
]


def default_watch_history_path(output_dir: str) -> Path:
    return Path(output_dir) / "kalshi_micro_watch_history.csv"


def load_watch_history(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _rewrite_watch_history(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=WATCH_HISTORY_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in WATCH_HISTORY_FIELDNAMES})


def append_watch_history(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    if exists:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            existing_fieldnames = list(reader.fieldnames or [])
            existing_rows = [dict(item) for item in reader]
        if existing_fieldnames != WATCH_HISTORY_FIELDNAMES:
            _rewrite_watch_history(path, existing_rows)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=WATCH_HISTORY_FIELDNAMES)
        if not exists:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in WATCH_HISTORY_FIELDNAMES})


def _streak(rows: list[dict[str, str]], key: str) -> int:
    if not rows:
        return 0
    latest_value = str(rows[-1].get(key) or "")
    if latest_value == "":
        return 0
    streak = 0
    for row in reversed(rows):
        if str(row.get(key) or "") != latest_value:
            break
        streak += 1
    return streak


def _parse_int(value: Any) -> int:
    text = str(value or "").strip()
    if text == "":
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _parse_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _focus_identity(row: dict[str, str]) -> tuple[str, str]:
    top_threshold = str(row.get("top_threshold_market_ticker") or "").strip()
    if top_threshold:
        return ("threshold", top_threshold)
    top_pressure = str(row.get("top_pressure_market_ticker") or "").strip()
    if top_pressure:
        return ("pressure", top_pressure)
    return ("", "")


def _focus_market_streak(rows: list[dict[str, str]]) -> tuple[str | None, str | None, int]:
    if not rows:
        return (None, None, 0)
    latest_mode, latest_ticker = _focus_identity(rows[-1])
    if not latest_ticker:
        return (None, None, 0)
    streak = 0
    for row in reversed(rows):
        mode, ticker = _focus_identity(row)
        if (mode, ticker) != (latest_mode, latest_ticker):
            break
        streak += 1
    return (latest_mode, latest_ticker, streak)


def _focus_market_state(
    rows: list[dict[str, str]],
    *,
    recent_window: int,
    recent_threshold_approach_runs: int,
) -> tuple[str, str]:
    latest_mode, latest_ticker, streak = _focus_market_streak(rows)
    if not latest_ticker:
        return ("none", "No focus market is active in the latest run.")
    if latest_mode == "threshold":
        if streak >= 2:
            return ("sustained_threshold_focus", f"{latest_ticker} has remained the top threshold focus across repeated runs.")
        return ("new_threshold_focus", f"{latest_ticker} is the newest threshold focus market.")
    if recent_threshold_approach_runs > 0:
        return ("pressure_with_threshold_context", f"{latest_ticker} remains the top pressure focus while threshold approach signals are active.")
    if streak >= 3:
        return ("stalled_pressure_focus", f"{latest_ticker} has stayed the top pressure focus without reaching the threshold-approach state.")
    if streak >= 2:
        return ("sustained_pressure_focus", f"{latest_ticker} has remained the top pressure focus across repeated runs.")
    return ("new_pressure_focus", f"{latest_ticker} is the newest pressure focus market.")


def _classify_board_regime(rows: list[dict[str, str]], *, recent_window: int) -> tuple[str, str]:
    if not rows:
        return "no_history", "No watch-history runs have been recorded yet."

    recent_rows = rows[-recent_window:]
    latest = rows[-1]
    latest_gate_status = str(latest.get("status_trade_gate_status") or "")
    latest_trade_gate_pass = _parse_bool(latest.get("trade_gate_pass"))
    recent_improving_runs = sum(
        1 for row in recent_rows if str(row.get("board_change_label") or "") == "improving"
    )
    recent_meaningful_candidate_runs = sum(
        1 for row in recent_rows if _parse_int(row.get("meaningful_candidates_yes_bid_ge_0_05")) > 0
    )
    recent_persistent_tradeable_runs = sum(
        1 for row in recent_rows if _parse_int(row.get("persistent_tradeable_markets")) > 0
    )
    recent_pressure_build_runs = sum(
        1 for row in recent_rows if _parse_int(row.get("pressure_build_markets")) > 0
    )
    recent_threshold_approach_runs = sum(
        1 for row in recent_rows if _parse_int(row.get("threshold_approaching_markets")) > 0
    )
    recent_concentration_runs = sum(
        1 for row in recent_rows if str(row.get("category_concentration_warning") or "").strip()
    )

    if latest_trade_gate_pass or latest_gate_status == "pass":
        return "trade_ready", "Latest status run passed the trade gate."
    if latest_gate_status in {"rate_limited", "upstream_error"}:
        return "ops_blocked", "Latest status run was blocked by an upstream or rate-limit issue."
    if recent_meaningful_candidate_runs > 0:
        return "candidate_emerging", "Recent status runs produced at least one meaningful candidate."
    if recent_persistent_tradeable_runs > 0:
        return "persistent_watch", "Recent status runs observed persistent tradeable markets, but the gate still has not passed."
    if recent_threshold_approach_runs > 0:
        return "threshold_approaching", "Recent status runs show a market approaching the live review thresholds."
    if recent_pressure_build_runs > 0:
        return "pressure_building", "Recent status runs show at least one sub-threshold market building pressure."
    if recent_improving_runs >= 2:
        return "improving_but_thin", "Recent runs show board improvement, but not enough depth to clear the gate."
    if (
        latest_gate_status == "no_meaningful_candidates"
        and recent_rows
        and recent_concentration_runs == len(recent_rows)
    ):
        return "concentrated_penny_noise", "Recent runs are still concentrated in one thin category with no meaningful candidates."
    if latest_gate_status == "no_meaningful_candidates" and _streak(rows, "status_trade_gate_status") >= 3:
        return "stalled_penny_board", "Multiple consecutive runs still show no meaningful candidates."
    return "monitor", "Board state is mixed and should keep being monitored."


def summarize_watch_history(path: Path, *, recent_window: int = 5) -> dict[str, Any]:
    rows = load_watch_history(path)
    recent_rows = rows[-recent_window:]
    latest = rows[-1] if rows else {}
    board_regime, board_regime_reason = _classify_board_regime(rows, recent_window=recent_window)
    latest_focus_mode, latest_focus_market_ticker, focus_market_streak = _focus_market_streak(rows)
    recent_threshold_approach_runs = sum(
        1 for row in recent_rows if _parse_int(row.get("threshold_approaching_markets")) > 0
    )
    focus_market_state, focus_market_state_reason = _focus_market_state(
        rows,
        recent_window=recent_window,
        recent_threshold_approach_runs=recent_threshold_approach_runs,
    )
    recent_focus_market_changes = 0
    previous_identity: tuple[str, str] | None = None
    for row in recent_rows:
        identity = _focus_identity(row)
        if previous_identity is not None and identity != previous_identity:
            recent_focus_market_changes += 1
        previous_identity = identity
    return {
        "watch_history_csv": str(path),
        "watch_runs_total": len(rows),
        "latest_recorded_at": latest.get("recorded_at") if rows else None,
        "latest_recommendation": latest.get("status_recommendation") if rows else None,
        "latest_trade_gate_status": latest.get("status_trade_gate_status") if rows else None,
        "latest_trade_gate_pass": _parse_bool(latest.get("trade_gate_pass")) if rows else False,
        "latest_focus_market_mode": latest_focus_mode,
        "latest_focus_market_ticker": latest_focus_market_ticker,
        "focus_market_streak": focus_market_streak,
        "recent_focus_market_changes": recent_focus_market_changes,
        "focus_market_state": focus_market_state,
        "focus_market_state_reason": focus_market_state_reason,
        "recommendation_streak": _streak(rows, "status_recommendation"),
        "trade_gate_status_streak": _streak(rows, "status_trade_gate_status"),
        "recent_improving_runs": sum(
            1 for row in recent_rows if str(row.get("board_change_label") or "") == "improving"
        ),
        "recent_meaningful_candidate_runs": sum(
            1
            for row in recent_rows
            if str(row.get("meaningful_candidates_yes_bid_ge_0_05") or "").strip() not in {"", "0", "0.0"}
        ),
        "recent_persistent_tradeable_runs": sum(
            1 for row in recent_rows if _parse_int(row.get("persistent_tradeable_markets")) > 0
        ),
        "recent_pressure_build_runs": sum(
            1 for row in recent_rows if _parse_int(row.get("pressure_build_markets")) > 0
        ),
        "recent_threshold_approach_runs": recent_threshold_approach_runs,
        "board_regime": board_regime,
        "board_regime_reason": board_regime_reason,
    }
