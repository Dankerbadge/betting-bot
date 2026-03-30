from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


APP_TITLE = "Betting Bot Monitor (Basic)"
DEFAULT_STALE_SECONDS = 60
DEFAULT_ROWS = 20

LATEST_FILE_PATTERNS = {
    "status_json": "kalshi_micro_status_*.json",
    "execute_summary_json": "kalshi_micro_execute_summary_*.json",
    "reconcile_summary_json": "kalshi_micro_reconcile_summary_*.json",
    "execute_csv": "kalshi_micro_execute_*.csv",
    "reconcile_csv": "kalshi_micro_reconcile_*.csv",
}

STATIC_FILES = {
    "watch_history_csv": "kalshi_micro_watch_history.csv",
    "ws_state_json": "kalshi_ws_state_latest.json",
}

WHAT_HAPPENED_LABELS = {
    "dry_run_ready": "Dry run looked okay",
    "orderbook_unavailable": "Could not read the live order book",
    "blocked_trade_gate": "Trade gate blocked trading",
    "blocked_exchange_inactive": "Exchange is not active",
    "blocked_exchange_status_unavailable": "Could not confirm exchange status",
    "blocked_ws_state_missing": "WebSocket state file is missing",
    "blocked_ws_state_stale": "WebSocket state is stale",
    "blocked_ws_state_desynced": "WebSocket state is desynced",
    "blocked_ws_state_empty": "WebSocket state has no market data",
    "blocked_ws_state_invalid": "WebSocket state is invalid",
    "blocked_balance_not_live_verified": "Live balance could not be verified",
    "blocked_balance_insufficient": "Insufficient cash for planned order",
    "submitted": "Order submitted",
    "submitted_then_canceled": "Submitted then canceled",
    "canceled": "Canceled",
    "filled": "Filled",
    "partial_fill": "Partially filled",
    "full_fill": "Filled",
    "no_candidates": "No trade candidates found",
    "skip_policy": "Skipped by execution policy",
}


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _age_seconds(ts: datetime | None) -> float | None:
    if ts is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - ts).total_seconds())


def _format_age(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h"


def _format_dollars(value: Any) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return "n/a"
    return f"${parsed:,.2f}"


def _human_code(value: Any) -> str:
    code = str(value or "").strip().lower()
    if not code:
        return "Unknown"
    if code in WHAT_HAPPENED_LABELS:
        return WHAT_HAPPENED_LABELS[code]
    return code.replace("_", " ").capitalize()


def _latest_file(outputs_dir: Path, pattern: str) -> Path | None:
    matches = list(outputs_dir.glob(pattern))
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


@st.cache_data(show_spinner=False)
def _load_json(path_text: str, mtime_ns: int) -> dict[str, Any]:
    _ = mtime_ns
    payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return {}


@st.cache_data(show_spinner=False)
def _load_csv(path_text: str, mtime_ns: int) -> pd.DataFrame:
    _ = mtime_ns
    return pd.read_csv(path_text)


def _collect_files(outputs_dir: Path) -> dict[str, Path | None]:
    files: dict[str, Path | None] = {}
    for key, pattern in LATEST_FILE_PATTERNS.items():
        files[key] = _latest_file(outputs_dir, pattern)
    for key, name in STATIC_FILES.items():
        candidate = outputs_dir / name
        files[key] = candidate if candidate.exists() else None
    return files


def _json_from(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        return _load_json(str(path), path.stat().st_mtime_ns)
    except Exception:
        return {}


def _csv_from(path: Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    try:
        return _load_csv(str(path), path.stat().st_mtime_ns)
    except Exception:
        return pd.DataFrame()


def _can_trade_now(status_payload: dict[str, Any], execute_payload: dict[str, Any]) -> tuple[str, str]:
    gate = str(status_payload.get("trade_gate_status") or "").strip().lower()
    execute_status = str(execute_payload.get("status") or "").strip().lower()

    if "pass" in gate or gate in {"ready", "tradeable", "approved"}:
        return "Yes", "Trade gate is passing in the latest status snapshot."
    if gate:
        return "No", f"Trade gate currently reports: `{gate}`."

    if any(marker in execute_status for marker in ("blocked", "failed", "degraded", "no_candidates")):
        return "No", f"Latest execute status is `{execute_status}`."
    if execute_status:
        return "Maybe", f"Latest execute status is `{execute_status}`."

    return "Unknown", "No clear gate/execute status found yet."


def _build_attention_items(
    *,
    stale_seconds: int,
    status_payload: dict[str, Any],
    execute_payload: dict[str, Any],
    reconcile_payload: dict[str, Any],
    ws_state_payload: dict[str, Any],
) -> list[str]:
    items: list[str] = []

    status_age = _age_seconds(_parse_ts(status_payload.get("captured_at")))
    if status_age is None or status_age > stale_seconds:
        items.append(
            f"Status data is stale (latest age: {_format_age(status_age)}; threshold: {stale_seconds}s)."
        )

    if status_payload.get("balance_live_verified") is False:
        source = str(status_payload.get("actual_live_balance_source") or "unknown")
        items.append(f"Cash balance is from `{source}`, not live-verified.")

    execute_ts = _parse_ts(execute_payload.get("captured_at"))
    reconcile_ts = _parse_ts(reconcile_payload.get("captured_at"))
    if execute_ts is not None and reconcile_ts is not None and reconcile_ts < execute_ts:
        items.append("Reconcile snapshot is older than the latest execute snapshot.")

    exposure = _safe_float(status_payload.get("total_market_exposure_dollars"))
    if exposure is not None and abs(exposure) > 0.0:
        items.append(f"Money currently at risk is non-zero: {_format_dollars(exposure)}.")

    attempts = execute_payload.get("attempts") if isinstance(execute_payload.get("attempts"), list) else []
    submitted_then_canceled = 0
    queue_waiting_no_fill = 0
    for attempt in attempts:
        if not isinstance(attempt, dict):
            continue
        result_code = str(attempt.get("result") or "").strip().lower()
        if result_code == "submitted_then_canceled":
            submitted_then_canceled += 1
        queue_position = _safe_float(attempt.get("queue_position_contracts"))
        if queue_position is not None and queue_position > 0 and "fill" not in result_code:
            queue_waiting_no_fill += 1
    if submitted_then_canceled >= 2:
        items.append("Repeated `submitted_then_canceled` attempts were detected.")
    if queue_waiting_no_fill >= 2:
        items.append("Orders repeatedly sat in queue without filling.")

    fees = _safe_float(status_payload.get("total_fees_paid_dollars"))
    realized = _safe_float(status_payload.get("total_realized_pnl_dollars"))
    if fees is not None and fees > 0 and (realized is None or realized <= 0):
        items.append("Fees are increasing while realized P/L is not positive.")

    ws_summary = ws_state_payload.get("summary") if isinstance(ws_state_payload.get("summary"), dict) else ws_state_payload
    ws_status = str(ws_summary.get("status") or "").strip().lower()
    if ws_status and ws_status != "ready":
        items.append(f"WebSocket state is `{ws_status}`.")

    return items


def _overall_health(attention_items: list[str], can_trade_label: str) -> tuple[str, str]:
    if len(attention_items) >= 3:
        return "red", "Bot health is RED: immediate attention needed before trusting live behavior."
    if attention_items or can_trade_label in {"No", "Unknown"}:
        return "yellow", "Bot health is YELLOW: running, but with blockers or risk warnings."
    return "green", "Bot health is GREEN: no major warning in the latest snapshots."


def _render_main(outputs_dir: Path, stale_seconds: int, table_rows: int) -> None:
    files = _collect_files(outputs_dir)
    status_payload = _json_from(files.get("status_json"))
    execute_payload = _json_from(files.get("execute_summary_json"))
    reconcile_payload = _json_from(files.get("reconcile_summary_json"))
    ws_state_payload = _json_from(files.get("ws_state_json"))

    can_trade_label, can_trade_reason = _can_trade_now(status_payload, execute_payload)
    attention_items = _build_attention_items(
        stale_seconds=stale_seconds,
        status_payload=status_payload,
        execute_payload=execute_payload,
        reconcile_payload=reconcile_payload,
        ws_state_payload=ws_state_payload,
    )
    health_color, health_message = _overall_health(attention_items, can_trade_label)

    st.caption("Read-only monitor. This dashboard never sends trade orders.")

    if health_color == "red":
        st.error(health_message)
    elif health_color == "yellow":
        st.warning(health_message)
    else:
        st.success(health_message)

    cash = status_payload.get("actual_live_balance_dollars")
    planned_orders = status_payload.get("planned_orders", execute_payload.get("planned_orders"))
    at_risk = status_payload.get("total_market_exposure_dollars")
    pnl = status_payload.get("total_realized_pnl_dollars")

    cols = st.columns(4)
    cols[0].metric("Cash", _format_dollars(cash))
    cols[1].metric("Orders the bot wants to place", str(planned_orders if planned_orders is not None else "n/a"))
    cols[2].metric("Money currently at risk", _format_dollars(at_risk))
    cols[3].metric("Profit / Loss so far", _format_dollars(pnl))

    st.subheader("What this means right now")
    status_age = _format_age(_age_seconds(_parse_ts(status_payload.get("captured_at"))))
    recommendation = status_payload.get("recommendation")
    focus_market = status_payload.get("top_market_ticker") or status_payload.get("top_signal_market_ticker")
    lines = [
        f"Bot status age: **{status_age}**",
        f"Can it trade right now: **{can_trade_label}**",
        can_trade_reason,
        f"Current recommendation: **{recommendation or 'n/a'}**",
        f"Primary market being watched: **{focus_market or 'none'}**",
    ]
    for line in lines:
        st.write(f"- {line}")

    st.subheader("What needs attention")
    if attention_items:
        for item in attention_items:
            st.write(f"- {item}")
    else:
        st.write("- No urgent attention flags in the latest files.")

    st.subheader("Did it try any orders?")
    attempts = execute_payload.get("attempts") if isinstance(execute_payload.get("attempts"), list) else []
    attempts_df = pd.DataFrame(attempts)
    if attempts_df.empty:
        attempts_df = _csv_from(files.get("execute_csv"))

    if attempts_df.empty:
        st.info("No recent attempt rows found.")
    else:
        if "result" in attempts_df.columns:
            attempts_df["What happened"] = attempts_df["result"].map(_human_code)
        elif "status" in attempts_df.columns:
            attempts_df["What happened"] = attempts_df["status"].map(_human_code)
        else:
            attempts_df["What happened"] = "Unknown"

        show_cols = [
            col
            for col in [
                "market_ticker",
                "planned_side",
                "planned_contracts",
                "planned_entry_price_dollars",
                "What happened",
                "execution_policy_reason",
                "order_id",
            ]
            if col in attempts_df.columns
        ]
        st.dataframe(attempts_df[show_cols].head(table_rows), use_container_width=True, hide_index=True)

    st.subheader("Latest reconcile check")
    reconcile_rows = reconcile_payload.get("rows") if isinstance(reconcile_payload.get("rows"), list) else []
    reconcile_df = pd.DataFrame(reconcile_rows)
    if reconcile_df.empty:
        reconcile_df = _csv_from(files.get("reconcile_csv"))

    if reconcile_df.empty:
        st.info("No reconcile rows found yet.")
    else:
        if "status" in reconcile_df.columns:
            reconcile_df["What happened"] = reconcile_df["status"].map(_human_code)
        show_cols = [
            col
            for col in [
                "ticker",
                "planned_side",
                "fill_count_fp",
                "effective_price_dollars",
                "realized_pnl_dollars",
                "fees_paid_dollars",
                "What happened",
            ]
            if col in reconcile_df.columns
        ]
        st.dataframe(reconcile_df[show_cols].head(table_rows), use_container_width=True, hide_index=True)

    watch_df = _csv_from(files.get("watch_history_csv"))
    if not watch_df.empty and "recorded_at" in watch_df.columns:
        st.subheader("What it is watching")
        watch_df["recorded_at"] = pd.to_datetime(watch_df["recorded_at"], errors="coerce", utc=True)
        watch_df = watch_df.sort_values("recorded_at")
        trend_cols = []
        for col in [
            "meaningful_candidates_yes_bid_ge_0_05",
            "persistent_tradeable_markets",
            "pressure_build_markets",
            "threshold_approaching_markets",
        ]:
            if col in watch_df.columns:
                watch_df[col] = pd.to_numeric(watch_df[col], errors="coerce")
                trend_cols.append(col)
        if trend_cols:
            st.line_chart(watch_df.set_index("recorded_at")[trend_cols], use_container_width=True)

    st.subheader("Glossary")
    st.write("- **Cash**: latest available balance snapshot from status output.")
    st.write("- **Orders the bot wants to place**: planned order count from latest status/execute run.")
    st.write("- **Money currently at risk**: current market exposure from reconcile/status data.")
    st.write("- **Profit / Loss so far**: realized P/L from latest status snapshot.")
    st.write("- **What happened**: a plain-English translation of raw result/status codes.")


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    repo_root = Path(__file__).resolve().parent
    default_outputs = repo_root / "outputs"

    outputs_dir_text = st.sidebar.text_input("Outputs directory", str(default_outputs))
    stale_seconds = int(
        st.sidebar.number_input(
            "Stale warning threshold (seconds)",
            min_value=10,
            max_value=3600,
            value=DEFAULT_STALE_SECONDS,
            step=5,
        )
    )
    table_rows = int(
        st.sidebar.slider(
            "Rows shown",
            min_value=5,
            max_value=200,
            value=DEFAULT_ROWS,
            step=5,
        )
    )
    auto_refresh = st.sidebar.checkbox("Auto refresh (fragment)", value=True)
    refresh_seconds = int(
        st.sidebar.slider(
            "Refresh interval (seconds)",
            min_value=5,
            max_value=120,
            value=10,
            step=5,
        )
    )
    if st.sidebar.button("Refresh now"):
        st.cache_data.clear()
        st.rerun()

    outputs_dir = Path(outputs_dir_text).expanduser()
    if not outputs_dir.exists():
        st.error(f"Outputs directory not found: {outputs_dir}")
        return
    if not outputs_dir.is_dir():
        st.error(f"Path is not a directory: {outputs_dir}")
        return

    if auto_refresh and hasattr(st, "fragment"):
        @st.fragment(run_every=f"{refresh_seconds}s")
        def _live() -> None:
            _render_main(outputs_dir, stale_seconds, table_rows)

        _live()
    else:
        if auto_refresh:
            st.info("Your Streamlit version does not expose `st.fragment`; auto refresh is disabled.")
        _render_main(outputs_dir, stale_seconds, table_rows)


if __name__ == "__main__":
    main()
