from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

import pandas as pd
import streamlit as st


APP_TITLE = "Betting Bot Monitor"
DEFAULT_STALE_THRESHOLD_SECONDS = 60
DEFAULT_TABLE_ROWS = 25

LATEST_FILE_PATTERNS = {
    "status_json": "kalshi_micro_status_*.json",
    "execute_summary_json": "kalshi_micro_execute_summary_*.json",
    "execute_csv": "kalshi_micro_execute_*.csv",
    "reconcile_summary_json": "kalshi_micro_reconcile_summary_*.json",
    "reconcile_csv": "kalshi_micro_reconcile_*.csv",
    "prior_trader_summary_json": "kalshi_micro_prior_trader_summary_*.json",
    "prior_execute_summary_json": "kalshi_micro_prior_execute_summary_*.json",
    "frontier_report_json": "execution_frontier_report_*.json",
    "frontier_buckets_csv": "execution_frontier_report_buckets_*.csv",
    "ws_collect_summary_json": "kalshi_ws_state_collect_summary_*.json",
}

STATIC_FILES = {
    "watch_history_csv": "kalshi_micro_watch_history.csv",
    "execution_event_log_csv": "kalshi_execution_event_log.csv",
    "execution_journal_sqlite": "kalshi_execution_journal.sqlite3",
    "ws_state_json": "kalshi_ws_state_latest.json",
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _age_seconds(ts: datetime | None, now: datetime | None = None) -> float | None:
    if ts is None:
        return None
    reference = now or _utc_now()
    return max(0.0, (reference - ts).total_seconds())


def _format_age(seconds: float | None) -> str:
    if seconds is None:
        return "n/a"
    if seconds < 1:
        return f"{seconds * 1000:.0f} ms"
    if seconds < 60:
        return f"{seconds:.1f} s"
    if seconds < 3600:
        return f"{seconds / 60:.1f} min"
    if seconds < 86400:
        return f"{seconds / 3600:.1f} h"
    return f"{seconds / 86400:.1f} d"


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


def _format_dollars(value: Any) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return "n/a"
    return f"${parsed:,.2f}"


def _latest_file(outputs_dir: Path, pattern: str) -> Path | None:
    matches = list(outputs_dir.glob(pattern))
    if not matches:
        return None
    return max(matches, key=lambda item: item.stat().st_mtime)


@st.cache_data(show_spinner=False)
def _load_json_file(path_text: str, mtime_ns: int) -> dict[str, Any]:
    _ = mtime_ns
    payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return {}


@st.cache_data(show_spinner=False)
def _load_csv_file(path_text: str, mtime_ns: int) -> pd.DataFrame:
    _ = mtime_ns
    return pd.read_csv(path_text)


@st.cache_data(show_spinner=False)
def _load_execution_journal_sqlite(path_text: str, mtime_ns: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    _ = mtime_ns
    connection = sqlite3.connect(path_text)
    try:
        by_day = pd.read_sql_query(
            """
            SELECT
                substr(captured_at_utc, 1, 10) AS day,
                SUM(CASE WHEN event_type = 'order_submitted' THEN 1 ELSE 0 END) AS submitted_orders,
                SUM(CASE WHEN event_type = 'partial_fill' THEN 1 ELSE 0 END) AS partial_fills,
                SUM(CASE WHEN event_type = 'full_fill' THEN 1 ELSE 0 END) AS full_fills,
                SUM(CASE WHEN event_type = 'order_terminal' THEN 1 ELSE 0 END) AS terminal_events
            FROM execution_events
            GROUP BY substr(captured_at_utc, 1, 10)
            ORDER BY day
            """,
            connection,
        )
        by_result = pd.read_sql_query(
            """
            SELECT
                CASE
                    WHEN result IS NULL OR trim(result) = '' THEN '(empty)'
                    ELSE result
                END AS result_label,
                COUNT(*) AS count_events
            FROM execution_events
            GROUP BY result_label
            ORDER BY count_events DESC
            LIMIT 25
            """,
            connection,
        )
        return by_day, by_result
    finally:
        connection.close()


def _collect_files(outputs_dir: Path) -> dict[str, Path | None]:
    files: dict[str, Path | None] = {}
    for key, pattern in LATEST_FILE_PATTERNS.items():
        files[key] = _latest_file(outputs_dir, pattern)
    for key, file_name in STATIC_FILES.items():
        candidate = outputs_dir / file_name
        files[key] = candidate if candidate.exists() else None
    return files


def _json_from_path(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        return _load_json_file(str(path), path.stat().st_mtime_ns)
    except Exception:
        return {}


def _csv_from_path(path: Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    try:
        return _load_csv_file(str(path), path.stat().st_mtime_ns)
    except Exception:
        return pd.DataFrame()


def _coalesce_fields(payload: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in payload and payload.get(key) not in (None, ""):
            return payload.get(key)
    return None


def _build_alerts(
    *,
    stale_threshold_seconds: int,
    status_payload: dict[str, Any],
    execute_payload: dict[str, Any],
    reconcile_payload: dict[str, Any],
    ws_state_payload: dict[str, Any],
) -> list[str]:
    alerts: list[str] = []
    now = _utc_now()

    status_age = _age_seconds(_parse_ts(status_payload.get("captured_at")), now)
    if status_age is None or status_age > stale_threshold_seconds:
        alerts.append(
            f"No fresh `kalshi_micro_status` snapshot within {stale_threshold_seconds}s (latest age: {_format_age(status_age)})."
        )

    if status_payload.get("balance_live_verified") is False:
        source = str(status_payload.get("actual_live_balance_source") or "unknown")
        alerts.append(f"Balance source is `{source}`, not live-verified.")

    execute_ts = _parse_ts(execute_payload.get("captured_at"))
    reconcile_ts = _parse_ts(reconcile_payload.get("captured_at"))
    if execute_ts is not None and reconcile_ts is not None and reconcile_ts < execute_ts:
        alerts.append("Latest reconcile snapshot is older than latest execute snapshot.")

    exposure = _safe_float(status_payload.get("total_market_exposure_dollars"))
    if exposure is not None and abs(exposure) > 0.0:
        alerts.append(f"Live exposure is non-zero: {_format_dollars(exposure)}.")

    trade_gate_status = str(status_payload.get("trade_gate_status") or "").strip().lower()
    if "stale" in trade_gate_status:
        alerts.append(f"Trade gate indicates stale board state (`{trade_gate_status}`).")

    attempts = execute_payload.get("attempts")
    if isinstance(attempts, list) and attempts:
        result_counts: dict[str, int] = {}
        repeated_queue_without_fill = 0
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            result = str(attempt.get("result") or "").strip().lower()
            if result:
                result_counts[result] = result_counts.get(result, 0) + 1
            queue_pos = _safe_float(attempt.get("queue_position_contracts"))
            if queue_pos is not None and queue_pos > 0:
                if "fill" not in result:
                    repeated_queue_without_fill += 1

        if result_counts.get("submitted_then_canceled", 0) >= 2:
            alerts.append("Repeated `submitted_then_canceled` attempts detected in latest execute cycle.")
        if repeated_queue_without_fill >= 2:
            alerts.append("Multiple queued orders did not fill in latest execute cycle.")

    fees = _safe_float(status_payload.get("total_fees_paid_dollars"))
    realized = _safe_float(status_payload.get("total_realized_pnl_dollars"))
    if fees is not None and fees > 0 and (realized is None or realized <= 0):
        alerts.append("Fees are rising while realized PnL is non-positive.")

    ws_summary = ws_state_payload.get("summary") if isinstance(ws_state_payload.get("summary"), dict) else ws_state_payload
    ws_status = str(ws_summary.get("status") or "").strip().lower()
    if ws_status and ws_status != "ready":
        alerts.append(f"WebSocket authority state is `{ws_status}`.")

    return alerts


def _freshness_records(files: dict[str, Path | None], stale_threshold_seconds: int) -> pd.DataFrame:
    now = _utc_now()
    rows: list[dict[str, Any]] = []
    for label, path in files.items():
        if path is None:
            rows.append(
                {
                    "artifact": label,
                    "path": "",
                    "exists": False,
                    "modified_at_utc": "",
                    "age": "n/a",
                    "stale": True,
                }
            )
            continue
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        age_seconds = _age_seconds(modified, now)
        rows.append(
            {
                "artifact": label,
                "path": str(path),
                "exists": True,
                "modified_at_utc": modified.isoformat(),
                "age": _format_age(age_seconds),
                "stale": bool(age_seconds is None or age_seconds > stale_threshold_seconds),
            }
        )
    return pd.DataFrame(rows)


def _render_dashboard(
    *,
    outputs_dir: Path,
    stale_threshold_seconds: int,
    table_rows: int,
) -> None:
    files = _collect_files(outputs_dir)
    status_payload = _json_from_path(files.get("status_json"))
    execute_payload = _json_from_path(files.get("execute_summary_json"))
    reconcile_payload = _json_from_path(files.get("reconcile_summary_json"))
    prior_payload = _json_from_path(files.get("prior_trader_summary_json"))
    ws_state_payload = _json_from_path(files.get("ws_state_json"))
    frontier_payload = _json_from_path(files.get("frontier_report_json"))

    st.caption("Read-only console. This dashboard does not place, amend, or cancel orders.")

    status_captured = _parse_ts(status_payload.get("captured_at"))
    status_age = _age_seconds(status_captured)

    balance = _coalesce_fields(
        status_payload,
        ["actual_live_balance_dollars", "live_balance_dollars"],
    )
    trade_gate = _coalesce_fields(
        status_payload,
        ["trade_gate_status", "latest_execute_status", "status"],
    )
    planned_orders = _coalesce_fields(status_payload, ["planned_orders"]) or _coalesce_fields(execute_payload, ["planned_orders"])
    exposure = _coalesce_fields(status_payload, ["total_market_exposure_dollars"])
    realized = _coalesce_fields(status_payload, ["total_realized_pnl_dollars"])
    fees = _coalesce_fields(status_payload, ["total_fees_paid_dollars"])

    metric_columns = st.columns(7)
    metric_columns[0].metric("Balance", _format_dollars(balance))
    metric_columns[1].metric("Trade Gate", str(trade_gate or "n/a"))
    metric_columns[2].metric("Planned Orders", str(planned_orders if planned_orders is not None else "n/a"))
    metric_columns[3].metric("Exposure", _format_dollars(exposure))
    metric_columns[4].metric("Realized PnL", _format_dollars(realized))
    metric_columns[5].metric("Fees", _format_dollars(fees))
    metric_columns[6].metric("Status Age", _format_age(status_age))

    recommendation = _coalesce_fields(status_payload, ["recommendation", "status_recommendation"])
    board_warning = _coalesce_fields(status_payload, ["board_warning"])
    focus_market = _coalesce_fields(
        status_payload,
        ["top_market_ticker", "top_signal_market_ticker", "top_quality_market_ticker"],
    )
    execute_status = _coalesce_fields(execute_payload, ["status"])
    reconcile_status = _coalesce_fields(reconcile_payload, ["status"])
    prior_status = _coalesce_fields(prior_payload, ["status"])
    ws_summary = ws_state_payload.get("summary") if isinstance(ws_state_payload.get("summary"), dict) else ws_state_payload
    ws_status = _coalesce_fields(ws_summary, ["status"])

    left, right = st.columns([3, 4])
    with left:
        st.subheader("Operator State")
        st.write(f"Recommendation: `{recommendation or 'n/a'}`")
        st.write(f"Board Warning: `{board_warning or 'none'}`")
        st.write(f"Focus Market: `{focus_market or 'none'}`")
        st.write(f"Execute Status: `{execute_status or 'n/a'}`")
        st.write(f"Reconcile Status: `{reconcile_status or 'n/a'}`")
        st.write(f"Prior Trader Status: `{prior_status or 'n/a'}`")
        st.write(f"WS State: `{ws_status or 'missing'}`")

    with right:
        st.subheader("Guardrails")
        alerts = _build_alerts(
            stale_threshold_seconds=stale_threshold_seconds,
            status_payload=status_payload,
            execute_payload=execute_payload,
            reconcile_payload=reconcile_payload,
            ws_state_payload=ws_state_payload,
        )
        if alerts:
            for item in alerts:
                st.warning(item)
        else:
            st.success("No active guardrail alerts in current snapshots.")

    attempts = execute_payload.get("attempts") if isinstance(execute_payload.get("attempts"), list) else []
    attempts_df = pd.DataFrame(attempts)
    st.subheader("Latest Execute Attempts")
    if attempts_df.empty:
        st.info("No attempt rows in latest execute summary.")
    else:
        preferred_columns = [
            "plan_rank",
            "market_ticker",
            "planned_side",
            "planned_contracts",
            "planned_entry_price_dollars",
            "result",
            "order_id",
            "queue_position_contracts",
            "execution_policy_decision",
            "execution_policy_reason",
            "execution_ev_submit_dollars",
            "execution_break_even_edge_per_contract_dollars",
            "execution_forecast_edge_net_per_contract_dollars",
        ]
        selected = [name for name in preferred_columns if name in attempts_df.columns]
        st.dataframe(attempts_df[selected].head(table_rows), use_container_width=True, hide_index=True)

    reconcile_rows = reconcile_payload.get("rows") if isinstance(reconcile_payload.get("rows"), list) else []
    reconcile_df = pd.DataFrame(reconcile_rows)
    if reconcile_df.empty:
        reconcile_df = _csv_from_path(files.get("reconcile_csv"))

    st.subheader("Latest Reconcile Rows")
    if reconcile_df.empty:
        st.info("No reconcile rows available.")
    else:
        st.dataframe(reconcile_df.head(table_rows), use_container_width=True, hide_index=True)

    st.subheader("Execution History")
    daily_df = pd.DataFrame()
    result_df = pd.DataFrame()
    journal_path = files.get("execution_journal_sqlite")
    if journal_path is not None:
        try:
            daily_df, result_df = _load_execution_journal_sqlite(str(journal_path), journal_path.stat().st_mtime_ns)
        except Exception:
            daily_df = pd.DataFrame()
            result_df = pd.DataFrame()

    if daily_df.empty and result_df.empty:
        st.info("Execution journal history is unavailable or empty.")
    else:
        chart_left, chart_right = st.columns(2)
        with chart_left:
            st.caption("Submissions and fill lifecycle by day")
            if daily_df.empty:
                st.info("No daily lifecycle rows yet.")
            else:
                daily_plot = daily_df.copy()
                daily_plot["day"] = pd.to_datetime(daily_plot["day"], errors="coerce")
                daily_plot = daily_plot.sort_values("day")
                daily_plot = daily_plot.set_index("day")
                st.bar_chart(daily_plot[["submitted_orders", "partial_fills", "full_fills", "terminal_events"]])
        with chart_right:
            st.caption("Result distribution")
            if result_df.empty:
                st.info("No result distribution rows yet.")
            else:
                result_plot = result_df.copy().set_index("result_label")
                st.bar_chart(result_plot["count_events"])

    watch_df = _csv_from_path(files.get("watch_history_csv"))
    st.subheader("Watch History")
    if watch_df.empty:
        st.info("`kalshi_micro_watch_history.csv` not found or empty.")
    else:
        if "recorded_at" in watch_df.columns:
            watch_df["recorded_at"] = pd.to_datetime(watch_df["recorded_at"], errors="coerce", utc=True)
            watch_df = watch_df.sort_values("recorded_at")
            watch_df = watch_df.set_index("recorded_at")
        numeric_columns: list[str] = []
        for name in [
            "meaningful_candidates_yes_bid_ge_0_05",
            "persistent_tradeable_markets",
            "improved_two_sided_markets",
            "pressure_build_markets",
            "threshold_approaching_markets",
        ]:
            if name in watch_df.columns:
                watch_df[name] = pd.to_numeric(watch_df[name], errors="coerce")
                numeric_columns.append(name)
        if numeric_columns:
            st.line_chart(watch_df[numeric_columns], use_container_width=True)
        st.dataframe(watch_df.reset_index().tail(table_rows), use_container_width=True, hide_index=True)

    st.subheader("Frontier Snapshot")
    if not frontier_payload:
        st.info("No execution frontier report found.")
    else:
        frontier_summary = {
            "status": frontier_payload.get("status"),
            "events_scanned": frontier_payload.get("events_scanned"),
            "submitted_orders": frontier_payload.get("submitted_orders"),
            "filled_orders": frontier_payload.get("filled_orders"),
            "full_filled_orders": frontier_payload.get("full_filled_orders"),
            "fill_samples_with_markout": frontier_payload.get("fill_samples_with_markout"),
        }
        st.json(frontier_summary)
        buckets = frontier_payload.get("bucket_rows")
        if isinstance(buckets, list) and buckets:
            st.dataframe(pd.DataFrame(buckets).head(table_rows), use_container_width=True, hide_index=True)

    st.subheader("File Freshness")
    freshness_df = _freshness_records(files, stale_threshold_seconds)
    st.dataframe(freshness_df, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)

    repo_root = Path(__file__).resolve().parent
    default_outputs = repo_root / "outputs"

    outputs_dir_text = st.sidebar.text_input("Outputs directory", str(default_outputs))
    stale_threshold_seconds = int(
        st.sidebar.number_input(
            "Stale warning threshold (seconds)",
            min_value=10,
            max_value=3600,
            value=DEFAULT_STALE_THRESHOLD_SECONDS,
            step=5,
        )
    )
    table_rows = int(
        st.sidebar.slider(
            "Rows shown in tables",
            min_value=5,
            max_value=200,
            value=DEFAULT_TABLE_ROWS,
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
        def _live_fragment() -> None:
            _render_dashboard(
                outputs_dir=outputs_dir,
                stale_threshold_seconds=stale_threshold_seconds,
                table_rows=table_rows,
            )

        _live_fragment()
    else:
        if auto_refresh:
            st.info("Your Streamlit version does not expose `st.fragment`; auto refresh is disabled.")
        _render_dashboard(
            outputs_dir=outputs_dir,
            stale_threshold_seconds=stale_threshold_seconds,
            table_rows=table_rows,
        )


if __name__ == "__main__":
    main()
