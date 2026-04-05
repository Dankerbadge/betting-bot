#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export BETBOT_REPO_ROOT="$REPO_ROOT"
export BETBOT_OUTPUT_DIR="${BETBOT_OUTPUT_DIR:-$REPO_ROOT/outputs}"
export BETBOT_ENV_FILE="${BETBOT_ENV_FILE:-$REPO_ROOT/data/research/account_onboarding.env.template}"
export BETBOT_HISTORY_CSV="${BETBOT_HISTORY_CSV:-$BETBOT_OUTPUT_DIR/kalshi_nonsports_history.csv}"
export BETBOT_PRIORS_CSV="${BETBOT_PRIORS_CSV:-$REPO_ROOT/data/research/kalshi_nonsports_priors.csv}"
export BETBOT_WEATHER_LOOKBACK_YEARS="${BETBOT_WEATHER_LOOKBACK_YEARS:-15}"
export BETBOT_WEATHER_PREWARM_MAX_KEYS="${BETBOT_WEATHER_PREWARM_MAX_KEYS:-500}"
export BETBOT_WEATHER_CACHE_MAX_AGE_HOURS="${BETBOT_WEATHER_CACHE_MAX_AGE_HOURS:-24}"
export BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS="${BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS:-6}"
export BETBOT_WEATHER_PRIOR_MAX_MARKETS="${BETBOT_WEATHER_PRIOR_MAX_MARKETS:-30}"
export BETBOT_WEATHER_ALLOWED_CONTRACT_FAMILIES="${BETBOT_WEATHER_ALLOWED_CONTRACT_FAMILIES:-daily_rain,daily_temperature}"
export BETBOT_WEATHER_INCLUDE_NWS_GRIDPOINT_DATA="${BETBOT_WEATHER_INCLUDE_NWS_GRIDPOINT_DATA:-1}"
export BETBOT_WEATHER_INCLUDE_NWS_OBSERVATIONS="${BETBOT_WEATHER_INCLUDE_NWS_OBSERVATIONS:-1}"
export BETBOT_WEATHER_INCLUDE_NWS_ALERTS="${BETBOT_WEATHER_INCLUDE_NWS_ALERTS:-1}"
export BETBOT_WEATHER_INCLUDE_NCEI_NORMALS="${BETBOT_WEATHER_INCLUDE_NCEI_NORMALS:-1}"
export BETBOT_WEATHER_INCLUDE_MRMS_QPE="${BETBOT_WEATHER_INCLUDE_MRMS_QPE:-1}"
export BETBOT_WEATHER_INCLUDE_NBM_SNAPSHOT="${BETBOT_WEATHER_INCLUDE_NBM_SNAPSHOT:-1}"
export BETBOT_WEATHER_CDO_TOKEN_FILE="${BETBOT_WEATHER_CDO_TOKEN_FILE:-$REPO_ROOT/.secrets/noaa_cdo_token.txt}"
export BETBOT_TIMEOUT_SECONDS="${BETBOT_TIMEOUT_SECONDS:-15}"
export BETBOT_CAPTURE_MAX_HOURS_TO_CLOSE="${BETBOT_CAPTURE_MAX_HOURS_TO_CLOSE:-4000}"
export BETBOT_CAPTURE_PAGE_LIMIT="${BETBOT_CAPTURE_PAGE_LIMIT:-200}"
export BETBOT_CAPTURE_MAX_PAGES="${BETBOT_CAPTURE_MAX_PAGES:-12}"
export BETBOT_DAILY_WEATHER_RECOVERY_MAX_HOURS_TO_CLOSE="${BETBOT_DAILY_WEATHER_RECOVERY_MAX_HOURS_TO_CLOSE:-6000}"
export BETBOT_DAILY_WEATHER_RECOVERY_PAGE_LIMIT="${BETBOT_DAILY_WEATHER_RECOVERY_PAGE_LIMIT:-300}"
export BETBOT_DAILY_WEATHER_RECOVERY_MAX_PAGES="${BETBOT_DAILY_WEATHER_RECOVERY_MAX_PAGES:-24}"
export BETBOT_FRONTIER_RECENT_ROWS="${BETBOT_FRONTIER_RECENT_ROWS:-20000}"
export BETBOT_FRONTIER_MAX_AGE_SECONDS="${BETBOT_FRONTIER_MAX_AGE_SECONDS:-10800}"
export BETBOT_BALANCE_MAX_AGE_SECONDS="${BETBOT_BALANCE_MAX_AGE_SECONDS:-900}"
export BETBOT_BALANCE_SMOKE_ON_FAILURE="${BETBOT_BALANCE_SMOKE_ON_FAILURE:-1}"
export BETBOT_DAILY_WEATHER_STALE_RECOVERY_ENABLED="${BETBOT_DAILY_WEATHER_STALE_RECOVERY_ENABLED:-1}"
export BETBOT_DAILY_WEATHER_STALE_RECOVERY_MAX_RETRIES="${BETBOT_DAILY_WEATHER_STALE_RECOVERY_MAX_RETRIES:-1}"
export BETBOT_DAILY_WEATHER_STALE_RECOVERY_SLEEP_SECONDS="${BETBOT_DAILY_WEATHER_STALE_RECOVERY_SLEEP_SECONDS:-2}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_ENABLED="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_ENABLED:-1}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_MAX_MARKETS="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_MAX_MARKETS:-20}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_ON_BASE_CAPTURE="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_ON_BASE_CAPTURE:-1}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_MAX_MARKETS="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_MAX_MARKETS:-4}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_INTERVAL_RUNS="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_INTERVAL_RUNS:-3}"
export BETBOT_DAILY_WEATHER_TICKER_REFRESH_STATE_FILE="${BETBOT_DAILY_WEATHER_TICKER_REFRESH_STATE_FILE:-$BETBOT_OUTPUT_DIR/overnight_alpha/daily_weather_ticker_refresh_state.json}"
export BETBOT_DAILY_WEATHER_WAKEUP_BURST_ENABLED="${BETBOT_DAILY_WEATHER_WAKEUP_BURST_ENABLED:-1}"
export BETBOT_DAILY_WEATHER_WAKEUP_BURST_MAX_MARKETS="${BETBOT_DAILY_WEATHER_WAKEUP_BURST_MAX_MARKETS:-4}"
export BETBOT_DAILY_WEATHER_WAKEUP_BURST_SLEEP_SECONDS="${BETBOT_DAILY_WEATHER_WAKEUP_BURST_SLEEP_SECONDS:-1}"
export BETBOT_DAILY_WEATHER_WAKEUP_REPRIORITIZE_ENABLED="${BETBOT_DAILY_WEATHER_WAKEUP_REPRIORITIZE_ENABLED:-1}"
export BETBOT_DAILY_WEATHER_RECOVERY_ALERT_WINDOW_HOURS="${BETBOT_DAILY_WEATHER_RECOVERY_ALERT_WINDOW_HOURS:-6}"
export BETBOT_DAILY_WEATHER_RECOVERY_ALERT_THRESHOLD="${BETBOT_DAILY_WEATHER_RECOVERY_ALERT_THRESHOLD:-3}"
export BETBOT_DAILY_WEATHER_RECOVERY_ALERT_MAX_EVENTS="${BETBOT_DAILY_WEATHER_RECOVERY_ALERT_MAX_EVENTS:-500}"
export BETBOT_DAILY_WEATHER_AVAILABILITY_LOOKBACK_DAYS="${BETBOT_DAILY_WEATHER_AVAILABILITY_LOOKBACK_DAYS:-7}"
export BETBOT_CLIMATE_ROUTER_ENABLED="${BETBOT_CLIMATE_ROUTER_ENABLED:-1}"
export BETBOT_CLIMATE_ROUTER_SKIP_REALTIME_COLLECT="${BETBOT_CLIMATE_ROUTER_SKIP_REALTIME_COLLECT:-0}"
export BETBOT_CLIMATE_ROUTER_RUN_SECONDS="${BETBOT_CLIMATE_ROUTER_RUN_SECONDS:-20}"
export BETBOT_CLIMATE_ROUTER_MAX_MARKETS="${BETBOT_CLIMATE_ROUTER_MAX_MARKETS:-40}"
export BETBOT_CLIMATE_ROUTER_MARKET_TICKERS="${BETBOT_CLIMATE_ROUTER_MARKET_TICKERS:-}"
export BETBOT_CLIMATE_ROUTER_WS_CHANNELS="${BETBOT_CLIMATE_ROUTER_WS_CHANNELS:-orderbook_snapshot,orderbook_delta,ticker,public_trades,user_fills,market_positions}"
export BETBOT_CLIMATE_ROUTER_SEED_RECENT_MARKETS="${BETBOT_CLIMATE_ROUTER_SEED_RECENT_MARKETS:-1}"
export BETBOT_CLIMATE_ROUTER_RECENT_MARKETS_MIN_UPDATED_SECONDS="${BETBOT_CLIMATE_ROUTER_RECENT_MARKETS_MIN_UPDATED_SECONDS:-900}"
export BETBOT_CLIMATE_ROUTER_RECENT_MARKETS_TIMEOUT_SECONDS="${BETBOT_CLIMATE_ROUTER_RECENT_MARKETS_TIMEOUT_SECONDS:-8}"
export BETBOT_CLIMATE_ROUTER_WS_STATE_MAX_AGE_SECONDS="${BETBOT_CLIMATE_ROUTER_WS_STATE_MAX_AGE_SECONDS:-30}"
export BETBOT_CLIMATE_ROUTER_MIN_THEORETICAL_EDGE_NET_FEES="${BETBOT_CLIMATE_ROUTER_MIN_THEORETICAL_EDGE_NET_FEES:-0.005}"
export BETBOT_CLIMATE_ROUTER_MAX_QUOTE_AGE_SECONDS="${BETBOT_CLIMATE_ROUTER_MAX_QUOTE_AGE_SECONDS:-900}"
export BETBOT_CLIMATE_ROUTER_PLANNING_BANKROLL_DOLLARS="${BETBOT_CLIMATE_ROUTER_PLANNING_BANKROLL_DOLLARS:-40}"
export BETBOT_CLIMATE_ROUTER_DAILY_RISK_CAP_DOLLARS="${BETBOT_CLIMATE_ROUTER_DAILY_RISK_CAP_DOLLARS:-3}"
export BETBOT_CLIMATE_ROUTER_MAX_RISK_PER_BET_DOLLARS="${BETBOT_CLIMATE_ROUTER_MAX_RISK_PER_BET_DOLLARS:-1}"
export BETBOT_CLIMATE_ROUTER_AVAILABILITY_LOOKBACK_DAYS="${BETBOT_CLIMATE_ROUTER_AVAILABILITY_LOOKBACK_DAYS:-7}"
export BETBOT_CLIMATE_ROUTER_AVAILABILITY_RECENT_SECONDS="${BETBOT_CLIMATE_ROUTER_AVAILABILITY_RECENT_SECONDS:-900}"
export BETBOT_CLIMATE_ROUTER_AVAILABILITY_HOT_TRADE_WINDOW_SECONDS="${BETBOT_CLIMATE_ROUTER_AVAILABILITY_HOT_TRADE_WINDOW_SECONDS:-300}"
export BETBOT_CLIMATE_ROUTER_INCLUDE_CONTRACT_FAMILIES="${BETBOT_CLIMATE_ROUTER_INCLUDE_CONTRACT_FAMILIES:-daily_rain,daily_temperature,daily_snow,monthly_climate_anomaly}"
export BETBOT_CLIMATE_ROUTER_AVAILABILITY_DB_PATH="${BETBOT_CLIMATE_ROUTER_AVAILABILITY_DB_PATH:-$BETBOT_OUTPUT_DIR/kalshi_climate_availability.sqlite3}"
export BETBOT_CLIMATE_ROUTER_PILOT_ENABLED="${BETBOT_CLIMATE_ROUTER_PILOT_ENABLED:-1}"
export BETBOT_CLIMATE_ROUTER_PILOT_SUMMARY_JSON="${BETBOT_CLIMATE_ROUTER_PILOT_SUMMARY_JSON:-}"
export BETBOT_CLIMATE_ROUTER_PILOT_MAX_ORDERS_PER_RUN="${BETBOT_CLIMATE_ROUTER_PILOT_MAX_ORDERS_PER_RUN:-1}"
export BETBOT_CLIMATE_ROUTER_PILOT_CONTRACTS_CAP="${BETBOT_CLIMATE_ROUTER_PILOT_CONTRACTS_CAP:-1}"
export BETBOT_CLIMATE_ROUTER_PILOT_REQUIRED_EV_DOLLARS="${BETBOT_CLIMATE_ROUTER_PILOT_REQUIRED_EV_DOLLARS:-0.05}"
export BETBOT_CLIMATE_ROUTER_PILOT_ALLOWED_CLASSES="${BETBOT_CLIMATE_ROUTER_PILOT_ALLOWED_CLASSES:-tradable}"
export BETBOT_CLIMATE_ROUTER_PILOT_ALLOWED_FAMILIES="${BETBOT_CLIMATE_ROUTER_PILOT_ALLOWED_FAMILIES:-}"
export BETBOT_CLIMATE_ROUTER_PILOT_EXCLUDED_FAMILIES="${BETBOT_CLIMATE_ROUTER_PILOT_EXCLUDED_FAMILIES:-}"
export BETBOT_CLIMATE_ROUTER_PILOT_DEDUP_TICKERS="${BETBOT_CLIMATE_ROUTER_PILOT_DEDUP_TICKERS:-1}"
export BETBOT_CLIMATE_ROUTER_PILOT_POLICY_SCOPE_OVERRIDE_ENABLED="${BETBOT_CLIMATE_ROUTER_PILOT_POLICY_SCOPE_OVERRIDE_ENABLED:-1}"
export BETBOT_DISABLE_DAILY_WEATHER_LIVE_ONLY="${BETBOT_DISABLE_DAILY_WEATHER_LIVE_ONLY:-0}"
export BETBOT_SHADOW_BANKROLL_ENABLED="${BETBOT_SHADOW_BANKROLL_ENABLED:-1}"
export BETBOT_SHADOW_BANKROLL_START_DOLLARS="${BETBOT_SHADOW_BANKROLL_START_DOLLARS:-1000}"
export BETBOT_SHADOW_BANKROLL_STATE_FILE="${BETBOT_SHADOW_BANKROLL_STATE_FILE:-$BETBOT_OUTPUT_DIR/overnight_alpha/shadow_bankroll_state.json}"
export BETBOT_PAPER_LIVE_ENABLED="${BETBOT_PAPER_LIVE_ENABLED:-1}"
export BETBOT_PAPER_LIVE_START_DOLLARS="${BETBOT_PAPER_LIVE_START_DOLLARS:-$BETBOT_SHADOW_BANKROLL_START_DOLLARS}"
export BETBOT_PAPER_LIVE_STATE_FILE="${BETBOT_PAPER_LIVE_STATE_FILE:-$BETBOT_OUTPUT_DIR/overnight_alpha/paper_live_account_state.json}"
export BETBOT_PAPER_LIVE_RISK_PROFILE="${BETBOT_PAPER_LIVE_RISK_PROFILE:-growth_aggressive}"
export BETBOT_PAPER_LIVE_KELLY_FRACTION="${BETBOT_PAPER_LIVE_KELLY_FRACTION:-0.5}"
export BETBOT_PAPER_LIVE_KELLY_HIGH_CONF_MAX="${BETBOT_PAPER_LIVE_KELLY_HIGH_CONF_MAX:-0.75}"
export BETBOT_PAPER_LIVE_MAX_OPEN_RISK_PCT="${BETBOT_PAPER_LIVE_MAX_OPEN_RISK_PCT:-0.25}"
export BETBOT_PAPER_LIVE_MAX_FAMILY_RISK_PCT="${BETBOT_PAPER_LIVE_MAX_FAMILY_RISK_PCT:-0.15}"
export BETBOT_PAPER_LIVE_MAX_STRIP_RISK_PCT="${BETBOT_PAPER_LIVE_MAX_STRIP_RISK_PCT:-0.08}"
export BETBOT_PAPER_LIVE_MAX_SINGLE_POSITION_RISK_PCT="${BETBOT_PAPER_LIVE_MAX_SINGLE_POSITION_RISK_PCT:-0.06}"
export BETBOT_PAPER_LIVE_MAX_NEW_ATTEMPTS_PER_RUN="${BETBOT_PAPER_LIVE_MAX_NEW_ATTEMPTS_PER_RUN:-8}"
export BETBOT_PAPER_LIVE_FAMILY_ALLOWLIST="${BETBOT_PAPER_LIVE_FAMILY_ALLOWLIST:-monthly_climate_anomaly}"
export BETBOT_PAPER_LIVE_ALLOW_RANDOM_CANCELS="${BETBOT_PAPER_LIVE_ALLOW_RANDOM_CANCELS:-0}"
export BETBOT_PAPER_LIVE_SIZE_FROM_CURRENT_EQUITY="${BETBOT_PAPER_LIVE_SIZE_FROM_CURRENT_EQUITY:-1}"
export BETBOT_PAPER_LIVE_REQUIRE_LIVE_ELIGIBLE_HINT="${BETBOT_PAPER_LIVE_REQUIRE_LIVE_ELIGIBLE_HINT:-0}"
export BETBOT_MIN_SECONDS_BETWEEN_RUNS="${BETBOT_MIN_SECONDS_BETWEEN_RUNS:-2700}"

RUN_ROOT="$BETBOT_OUTPUT_DIR/overnight_alpha"
LOCK_DIR="$RUN_ROOT/.hourly_lock"
mkdir -p "$RUN_ROOT"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  python3 - <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path

try:
    from betbot.runtime_version import build_runtime_version_block
except Exception:  # pragma: no cover - best effort metadata only
    build_runtime_version_block = None  # type: ignore[assignment]

captured_at = datetime.now(timezone.utc).isoformat()
run_started_dt = datetime.now(timezone.utc)
output_dir = Path(os.environ["BETBOT_OUTPUT_DIR"])
run_root = output_dir / "overnight_alpha"
shadow_start_dollars = float(os.environ.get("BETBOT_SHADOW_BANKROLL_START_DOLLARS", "1000") or 1000.0)
shadow_start_text = f"{max(0.0, shadow_start_dollars):.4f}".rstrip("0").rstrip(".") or "0"
paper_live_enabled = str(os.environ.get("BETBOT_PAPER_LIVE_ENABLED", "1")).strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
paper_live_start_dollars = float(
    os.environ.get("BETBOT_PAPER_LIVE_START_DOLLARS", str(shadow_start_dollars)) or shadow_start_dollars
)
paper_live_state_file = str(os.environ.get("BETBOT_PAPER_LIVE_STATE_FILE") or "").strip() or None
paper_live_risk_profile = str(os.environ.get("BETBOT_PAPER_LIVE_RISK_PROFILE", "growth_aggressive") or "growth_aggressive").strip()
paper_live_kelly_fraction = float(os.environ.get("BETBOT_PAPER_LIVE_KELLY_FRACTION", "0.5") or 0.5)
paper_live_kelly_high_conf_max = float(os.environ.get("BETBOT_PAPER_LIVE_KELLY_HIGH_CONF_MAX", "0.75") or 0.75)
paper_live_max_open_risk_pct = float(os.environ.get("BETBOT_PAPER_LIVE_MAX_OPEN_RISK_PCT", "0.25") or 0.25)
paper_live_max_family_risk_pct = float(os.environ.get("BETBOT_PAPER_LIVE_MAX_FAMILY_RISK_PCT", "0.15") or 0.15)
paper_live_max_strip_risk_pct = float(os.environ.get("BETBOT_PAPER_LIVE_MAX_STRIP_RISK_PCT", "0.08") or 0.08)
paper_live_max_single_position_risk_pct = float(
    os.environ.get("BETBOT_PAPER_LIVE_MAX_SINGLE_POSITION_RISK_PCT", "0.06") or 0.06
)
paper_live_max_new_attempts_per_run = int(
    float(os.environ.get("BETBOT_PAPER_LIVE_MAX_NEW_ATTEMPTS_PER_RUN", "8") or 8)
)
paper_live_family_allowlist = [
    value.strip().lower()
    for value in str(os.environ.get("BETBOT_PAPER_LIVE_FAMILY_ALLOWLIST", "monthly_climate_anomaly") or "").split(",")
    if value.strip()
]
paper_live_allow_random_cancels = str(
    os.environ.get("BETBOT_PAPER_LIVE_ALLOW_RANDOM_CANCELS", "0")
).strip().lower() not in {"0", "false", "no", "off"}
paper_live_size_from_current_equity = str(
    os.environ.get("BETBOT_PAPER_LIVE_SIZE_FROM_CURRENT_EQUITY", "1")
).strip().lower() not in {"0", "false", "no", "off"}
paper_live_require_live_eligible_hint = str(
    os.environ.get("BETBOT_PAPER_LIVE_REQUIRE_LIVE_ELIGIBLE_HINT", "0")
).strip().lower() not in {"0", "false", "no", "off"}
payload = {
    "run_id": f"hourly_alpha_overnight::{run_started_dt.strftime('%Y%m%d_%H%M%S_%f')[:-3]}",
    "run_started_at_utc": captured_at,
    "run_finished_at_utc": captured_at,
    "run_stamp_utc": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
    "mode": "research_dry_run_only",
    "sizing_basis": f"shadow_{shadow_start_text}",
    "execution_basis": "live_actual_balance",
    "overall_status": "skipped_locked",
    "pipeline_ready": False,
    "live_ready": False,
    "live_blockers": ["scheduler_lock_held"],
    "shadow_bankroll_enabled": str(os.environ.get("BETBOT_SHADOW_BANKROLL_ENABLED", "1")).strip().lower()
    not in {"0", "false", "no", "off"},
    "shadow_bankroll_state_file": str(os.environ.get("BETBOT_SHADOW_BANKROLL_STATE_FILE") or ""),
    "shadow_bankroll_status": "observer_not_run",
    "shadow_bankroll_reason": "scheduler_lock_held",
    "shadow_bankroll_last_updated_at_utc": None,
    "shadow_bankroll_state_write_error": None,
    "shadow_bankroll_start_dollars": shadow_start_dollars,
    "shadow_theoretical_value_dollars": shadow_start_dollars,
    "shadow_realized_pnl_dollars": 0.0,
    "shadow_theoretical_unrealized_ev_dollars": 0.0,
    "shadow_theoretical_drawdown_pct": 0.0,
    "shadow_allocator_total_risk_dollars": 0.0,
    "shadow_allocator_selected_rows": 0,
    "shadow_expected_value_dollars": 0.0,
    "paper_live_enabled": paper_live_enabled,
    "paper_live_status": "observer_not_run",
    "paper_live_reason": "scheduler_lock_held",
    "paper_live_execution_basis": "paper_live_balance",
    "paper_live_risk_profile": paper_live_risk_profile,
    "paper_live_kelly_fraction": round(max(0.0, paper_live_kelly_fraction), 4),
    "paper_live_kelly_high_conf_max": round(max(0.0, paper_live_kelly_high_conf_max), 4),
    "paper_live_max_open_risk_pct": round(max(0.0, paper_live_max_open_risk_pct), 4),
    "paper_live_max_family_risk_pct": round(max(0.0, paper_live_max_family_risk_pct), 4),
    "paper_live_max_strip_risk_pct": round(max(0.0, paper_live_max_strip_risk_pct), 4),
    "paper_live_max_single_position_risk_pct": round(max(0.0, paper_live_max_single_position_risk_pct), 4),
    "paper_live_max_new_attempts_per_run": max(1, paper_live_max_new_attempts_per_run),
    "paper_live_family_allowlist": paper_live_family_allowlist,
    "paper_live_allow_random_cancels": paper_live_allow_random_cancels,
    "paper_live_size_from_current_equity": paper_live_size_from_current_equity,
    "paper_live_require_live_eligible_hint": paper_live_require_live_eligible_hint,
    "paper_live_state_file": paper_live_state_file,
    "paper_live_balance_start_dollars": paper_live_start_dollars,
    "paper_live_balance_current_dollars": paper_live_start_dollars,
    "paper_live_sizing_balance_dollars": paper_live_start_dollars,
    "paper_live_post_trade_sizing_balance_dollars": paper_live_start_dollars,
    "paper_live_strategy_equity_dollars": paper_live_start_dollars,
    "paper_live_realized_trade_pnl_dollars": 0.0,
    "paper_live_mark_to_market_pnl_dollars": 0.0,
    "paper_live_drawdown_pct": 0.0,
    "paper_live_strategy_drawdown_pct": 0.0,
    "paper_live_positions_open_count": 0,
    "paper_live_positions_closed_count": 0,
    "paper_live_positions_open": [],
    "paper_live_positions_closed": [],
    "paper_live_order_attempts": 0,
    "paper_live_orders_resting": 0,
    "paper_live_orders_filled": 0,
    "paper_live_orders_partial_filled": 0,
    "paper_live_orders_canceled": 0,
    "paper_live_orders_expired": 0,
    "paper_live_fill_time_seconds": None,
    "paper_live_markout_10s_dollars": 0.0,
    "paper_live_markout_10s": 0.0,
    "paper_live_markout_60s_dollars": 0.0,
    "paper_live_markout_60s": 0.0,
    "paper_live_markout_300s_dollars": 0.0,
    "paper_live_markout_300s": 0.0,
    "paper_live_settlement_pnl_dollars": 0.0,
    "paper_live_expected_vs_realized_delta": 0.0,
    "paper_live_family_scorecards": [],
    "paper_live_ticker_scorecards": [],
    "paper_live_top_negative_markout_families": [],
    "paper_live_top_positive_markout_families": [],
    "paper_live_top_expected_vs_realized_deltas": [],
    "paper_live_monthly_climate_anomaly_scorecard": None,
    "paper_live_monthly_climate_anomaly_trend": None,
    "paper_live_monthly_climate_anomaly_trend_band": "anecdotal",
    "paper_live_open_risk_dollars": 0.0,
    "paper_live_open_risk_cap_dollars": 0.0,
    "paper_live_open_risk_remaining_dollars": 0.0,
    "paper_live_family_open_risk_dollars": {},
    "paper_live_family_open_risk_remaining_dollars": {},
    "paper_live_strip_open_risk_dollars": {},
    "paper_live_strip_open_risk_remaining_dollars": {},
    "paper_live_family_execution_state": {},
    "paper_live_ticker_execution_state": {},
    "paper_live_family_mtm_per_risk_pct": {},
    "paper_live_ticker_mtm_per_risk_pct": {},
    "paper_live_family_markout_300s_mean_dollars": {},
    "paper_live_ticker_markout_300s_mean_dollars": {},
    "paper_live_family_markout_300s_mean_per_contract_dollars": {},
    "paper_live_ticker_markout_300s_mean_per_contract_dollars": {},
    "paper_live_family_markout_300s_per_risk_pct": {},
    "paper_live_ticker_markout_300s_per_risk_pct": {},
    "paper_live_family_markout_300s_per_contract": {},
    "paper_live_ticker_markout_300s_per_contract": {},
    "paper_live_family_fill_rate": {},
    "paper_live_ticker_fill_rate": {},
    "paper_live_family_cancel_rate": {},
    "paper_live_ticker_cancel_rate": {},
    "paper_live_family_risk_multiplier": {},
    "paper_live_ticker_risk_multiplier": {},
    "paper_live_drawdown_throttle_state": "full",
    "paper_live_drawdown_risk_scale": 1.0,
    "paper_live_used_kelly_fraction": 0.0,
    "paper_live_avg_kelly_fraction_used": 0.0,
    "paper_live_run_attempt_limit": max(1, paper_live_max_new_attempts_per_run),
    "paper_live_selected_tickers": [],
    "paper_live_equity_curve": [],
    "paper_live_last_updated_at_utc": None,
    "paper_live_accounting_version": 1,
    "paper_live_source": "synthetic_paper_live",
    "balance_heartbeat": {
        "status": "not_run",
        "live_ready": False,
        "source": "unknown",
        "balance_dollars": None,
        "cache_age_seconds": None,
        "freshness_threshold_seconds": None,
        "blockers": [],
        "check_error": None,
        "cache_file": None,
    },
    "execution_frontier": {
        "status": "not_run",
        "trusted_bucket_count": 0,
        "untrusted_bucket_count": 0,
        "submitted_orders": 0,
        "filled_orders": 0,
        "fill_samples_with_markout": 0,
        "bucket_markout_sample_counts_by_horizon": {},
        "output_file": None,
    },
    "top_market_ticker": None,
    "top_market_contract_family": None,
    "fair_yes_probability_raw": None,
    "execution_probability_guarded": None,
    "fill_probability_source": None,
    "empirical_fill_weight": None,
    "heuristic_fill_weight": None,
    "probe_lane_used": None,
    "probe_reason": None,
    "prior_trade_gate_status": None,
    "prior_trade_gate_blockers": None,
    "lane_comparison": {
        "status": "not_run",
        "reason": "skipped_locked",
        "comparison_basis": "same_snapshot_same_filters",
        "executed_lane": "maker_edge",
        "fully_frozen": False,
        "snapshot_inputs": {},
        "maker_edge": {
            "picked_ticker": None,
            "picked_side": None,
            "selected_fair_probability": None,
            "selected_fair_probability_conservative": None,
            "maker_entry_edge": None,
            "maker_entry_edge_net_fees": None,
            "expected_value_dollars": None,
            "expected_value_per_cost": None,
            "estimated_entry_cost_dollars": None,
            "estimated_max_loss_dollars": None,
            "estimated_max_profit_dollars": None,
            "expected_value_per_max_loss": None,
            "gate_status": None,
            "gate_blockers": None,
            "summary_file": None,
        },
        "probability_first": {
            "picked_ticker": None,
            "picked_side": None,
            "selected_fair_probability": None,
            "selected_fair_probability_conservative": None,
            "maker_entry_edge": None,
            "maker_entry_edge_net_fees": None,
            "expected_value_dollars": None,
            "expected_value_per_cost": None,
            "estimated_entry_cost_dollars": None,
            "estimated_max_loss_dollars": None,
            "estimated_max_profit_dollars": None,
            "expected_value_per_max_loss": None,
            "gate_status": None,
            "gate_blockers": None,
            "summary_file": None,
        },
        "delta": {
            "same_pick": None,
            "selected_fair_probability_delta": None,
            "maker_entry_edge_delta": None,
            "expected_value_dollars_delta": None,
            "expected_value_per_cost_delta": None,
            "estimated_max_loss_dollars_delta": None,
            "expected_value_per_max_loss_delta": None,
        },
        "errors": [],
    },
    "no_candidates_diagnostics": None,
    "probe_policy": {
        "enable_untrusted_bucket_probe_exploration": None,
        "untrusted_bucket_probe_exploration_enabled": None,
        "untrusted_bucket_probe_max_orders_per_run": None,
        "untrusted_bucket_probe_required_edge_buffer_dollars": None,
        "untrusted_bucket_probe_contracts_cap": None,
        "untrusted_bucket_probe_submitted_attempts": None,
        "untrusted_bucket_probe_blocked_attempts": None,
        "untrusted_bucket_probe_reason_counts": None,
    },
    "skip_reason": "skipped_locked",
    "steps": [],
    "failed_steps": [],
    "degraded_reasons": [],
}
if callable(build_runtime_version_block):
    payload["runtime_version"] = build_runtime_version_block(
        run_started_at=run_started_dt,
        run_id=f"hourly_alpha_overnight::{run_started_dt.strftime('%Y%m%d_%H%M%S_%f')[:-3]}",
        git_cwd=Path(os.environ.get("BETBOT_REPO_ROOT") or "."),
        as_of=run_started_dt,
    )
(run_root / "last_lock_skip.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
(output_dir / "overnight_alpha_latest.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(
    json.dumps(
        {
            "status": "skipped_locked",
            "latest_report": str(output_dir / "overnight_alpha_latest.json"),
        },
        indent=2,
    )
)
PY
  exit 0
fi

cleanup_lock() {
  rm -f "$LOCK_DIR/owner.json" 2>/dev/null || true
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup_lock EXIT INT TERM
printf '{"pid":%d,"started_at_utc":"%s"}\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$LOCK_DIR/owner.json"

cd "$REPO_ROOT"

python3 - <<'PY'
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

try:
    from betbot.runtime_version import build_runtime_version_block
except Exception:  # pragma: no cover - best effort metadata only
    build_runtime_version_block = None  # type: ignore[assignment]
try:
    from betbot.onboarding import _is_placeholder as _onboarding_is_placeholder
    from betbot.onboarding import _parse_env_file as _onboarding_parse_env_file
except Exception:  # pragma: no cover - fallback parser for control-plane only
    _onboarding_is_placeholder = None  # type: ignore[assignment]
    _onboarding_parse_env_file = None  # type: ignore[assignment]
try:
    from betbot.dns_guard import urlopen_with_dns_recovery as _urlopen_with_dns_recovery
except Exception:  # pragma: no cover - fallback for constrained runtime
    _urlopen_with_dns_recovery = None  # type: ignore[assignment]
try:
    from betbot.kalshi_nonsports_capture import HISTORY_FIELDNAMES as _capture_history_fieldnames
    from betbot.kalshi_nonsports_capture import _append_history as _capture_append_history
except Exception:  # pragma: no cover - fallback when module unavailable
    _capture_history_fieldnames = []
    _capture_append_history = None  # type: ignore[assignment]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: Any) -> datetime | None:
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


def _parse_float(value: Any) -> float | None:
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


def _coerce_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return int(default)
    try:
        return int(float(text))
    except ValueError:
        return int(default)


def _minutes_since_iso(value: Any, *, now_utc: datetime) -> float | None:
    timestamp = _parse_iso(value)
    if not isinstance(timestamp, datetime):
        return None
    return max(0.0, (now_utc - timestamp).total_seconds() / 60.0)


def _daily_weather_watch_priority_score(
    *,
    now_utc: datetime,
    run_counter: int,
    ticker_stats: dict[str, Any] | None,
    prior_hours_to_close: float | None,
) -> tuple[float, dict[str, Any]]:
    stats = dict(ticker_stats or {})
    wakeup_count = max(0, _coerce_int(stats.get("wakeup_count"), 0))
    endpoint_only_streak_count = max(0, _coerce_int(stats.get("endpoint_only_streak_count"), 0))
    minutes_since_last_wakeup = _minutes_since_iso(stats.get("last_wakeup_at_utc"), now_utc=now_utc)
    minutes_since_last_non_endpoint = _minutes_since_iso(stats.get("last_non_endpoint_at_utc"), now_utc=now_utc)
    had_orderable_side_ever = bool(stats.get("had_orderable_side_ever"))
    last_watch_selected_run_counter = max(0, _coerce_int(stats.get("last_watch_selected_run_counter"), 0))
    runs_since_last_watch_selected = (
        max(0, int(run_counter) - int(last_watch_selected_run_counter))
        if last_watch_selected_run_counter > 0
        else int(run_counter)
    )
    hours_to_close = (
        prior_hours_to_close
        if isinstance(prior_hours_to_close, float)
        else _parse_float(stats.get("last_hours_to_close"))
    )

    score = 0.0
    score += min(8.0, float(wakeup_count) * 1.5)
    if isinstance(minutes_since_last_non_endpoint, float):
        # Recent non-endpoint activity is the strongest wake-up predictor.
        score += max(0.0, 10.0 - min(minutes_since_last_non_endpoint, 600.0) / 60.0)
    if had_orderable_side_ever:
        score += 4.0
    if isinstance(hours_to_close, float) and hours_to_close > 0.0:
        score += max(0.0, 4.0 - min(hours_to_close, 96.0) / 24.0)
    score += min(3.0, max(0.0, float(runs_since_last_watch_selected)) / 4.0)
    score -= min(5.0, float(endpoint_only_streak_count) * 0.35)

    components = {
        "wakeup_count": wakeup_count,
        "minutes_since_last_wakeup": (
            round(float(minutes_since_last_wakeup), 3)
            if isinstance(minutes_since_last_wakeup, float)
            else None
        ),
        "minutes_since_last_non_endpoint": (
            round(float(minutes_since_last_non_endpoint), 3)
            if isinstance(minutes_since_last_non_endpoint, float)
            else None
        ),
        "hours_to_close": round(float(hours_to_close), 6) if isinstance(hours_to_close, float) else None,
        "had_orderable_side_ever": had_orderable_side_ever,
        "endpoint_only_streak_count": endpoint_only_streak_count,
        "runs_since_last_watch_selected": runs_since_last_watch_selected,
    }
    return round(score, 6), components


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _parse_env_values(path: Path) -> dict[str, str]:
    if callable(_onboarding_parse_env_file):
        return dict(_onboarding_parse_env_file(path))
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _is_placeholder_value(value: Any) -> bool:
    if callable(_onboarding_is_placeholder):
        return bool(_onboarding_is_placeholder(None if value is None else str(value)))
    raw = str(value or "").strip()
    if not raw:
        return True
    return raw.upper().startswith("TODO")


def _env_has_kalshi_credentials(path: Path) -> tuple[bool, str]:
    if not path.exists():
        return False, "env_file_missing"
    try:
        data = _parse_env_values(path)
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"env_parse_error:{exc}"
    env_name = str(data.get("KALSHI_ENV") or "").strip().lower()
    if env_name not in {"demo", "prod", "production"}:
        return False, "kalshi_env_invalid"
    access_key = data.get("KALSHI_ACCESS_KEY_ID")
    if _is_placeholder_value(access_key):
        return False, "kalshi_access_key_missing"
    private_key_path_raw = str(data.get("KALSHI_PRIVATE_KEY_PATH") or "").strip()
    if _is_placeholder_value(private_key_path_raw):
        return False, "kalshi_private_key_path_missing"
    private_key_path = Path(private_key_path_raw).expanduser()
    if not private_key_path.is_absolute():
        private_key_path = (path.parent / private_key_path).resolve()
    if not private_key_path.exists():
        return False, "kalshi_private_key_file_missing"
    return True, "kalshi_credentials_ready"


def _resolve_env_file(*, requested_path: Path, repo_root: Path) -> dict[str, Any]:
    requested = requested_path.expanduser()
    local_candidate = repo_root / "data" / "research" / "account_onboarding.local.env"
    template_candidate = repo_root / "data" / "research" / "account_onboarding.env.template"
    requested_ready, requested_reason = _env_has_kalshi_credentials(requested)
    effective = requested
    source = "requested"
    resolution_reason = requested_reason
    if (not requested_ready) and local_candidate != requested:
        local_ready, local_reason = _env_has_kalshi_credentials(local_candidate)
        if local_ready:
            effective = local_candidate
            source = "auto_local_override"
            resolution_reason = f"requested_{requested_reason}; using_local_{local_reason}"
    if not effective.exists() and template_candidate.exists():
        effective = template_candidate
        if source == "requested":
            source = "fallback_template"
        resolution_reason = "effective_env_missing_fallback_template"
    effective_ready, effective_reason = _env_has_kalshi_credentials(effective)
    return {
        "env_file_requested": str(requested),
        "env_file_effective": str(effective),
        "env_file_source": source,
        "env_file_resolution_reason": resolution_reason,
        "env_file_requested_kalshi_ready": bool(requested_ready),
        "env_file_requested_kalshi_ready_reason": requested_reason,
        "env_file_kalshi_ready": bool(effective_ready),
        "env_file_kalshi_ready_reason": effective_reason,
    }


def _file_meta(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "mtime_utc": None,
            "age_seconds": None,
            "size_bytes": None,
        }
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    age = max(0.0, (datetime.now(timezone.utc) - mtime).total_seconds())
    return {
        "path": str(path),
        "exists": True,
        "mtime_utc": mtime.isoformat(),
        "age_seconds": round(age, 3),
        "size_bytes": int(path.stat().st_size),
    }


def _file_sha256(path: Path) -> str | None:
    try:
        hasher = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
    except OSError:
        return None


def _snapshot_lane_input_artifact(
    *,
    artifact_name: str,
    source_path: Path | None,
    snapshot_dir: Path,
    snapshot_filename: str,
    required_for_run: bool,
    errors: list[str],
    missing_error_key: str,
    copy_error_prefix: str,
) -> tuple[Path | None, dict[str, Any]]:
    source = source_path if isinstance(source_path, Path) else None
    snapshot_path = snapshot_dir / snapshot_filename
    info: dict[str, Any] = {
        "artifact": artifact_name,
        "required_for_run": bool(required_for_run),
        "used_snapshot": False,
        "frozen": False,
        "source_path": str(source) if isinstance(source, Path) else None,
        "snapshot_path": str(snapshot_path),
        "source_exists": bool(source.exists()) if isinstance(source, Path) else False,
        "snapshot_exists": False,
        "source_size_bytes": int(source.stat().st_size) if isinstance(source, Path) and source.exists() else None,
        "snapshot_size_bytes": None,
        "source_sha256": None,
        "snapshot_sha256": None,
        "sha256": None,
        "error": None,
    }
    if not isinstance(source, Path) or not source.exists():
        if required_for_run:
            errors.append(missing_error_key)
            info["error"] = "missing_source"
        return source if isinstance(source, Path) and source.exists() else None, info

    try:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, snapshot_path)
    except Exception as exc:  # pragma: no cover - observer-only hardening
        errors.append(f"{copy_error_prefix}:{exc}")
        info["error"] = f"copy_failed:{exc}"
        return source, info

    info["snapshot_exists"] = snapshot_path.exists()
    if snapshot_path.exists():
        info["snapshot_size_bytes"] = int(snapshot_path.stat().st_size)
    source_sha = _file_sha256(source)
    snapshot_sha = _file_sha256(snapshot_path) if snapshot_path.exists() else None
    info["source_sha256"] = source_sha
    info["snapshot_sha256"] = snapshot_sha
    info["sha256"] = snapshot_sha or source_sha
    if source_sha and snapshot_sha and source_sha == snapshot_sha:
        info["frozen"] = True
        info["used_snapshot"] = True
        return snapshot_path, info

    if required_for_run:
        errors.append(f"{copy_error_prefix}:hash_mismatch_or_unreadable")
        info["error"] = "hash_mismatch_or_unreadable"
    return source, info


def _choose_betbot_launcher(repo_root: Path) -> list[str]:
    venv_python = repo_root / ".venv" / "bin" / "python"
    if venv_python.exists() and os.access(venv_python, os.X_OK):
        return [str(venv_python), "-m", "betbot.cli"]
    return [sys.executable, "-m", "betbot.cli"]


def _synthetic_step(
    *,
    name: str,
    status: str,
    ok: bool,
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now_iso = _now_iso()
    step = {
        "name": name,
        "command": [],
        "started_at_utc": now_iso,
        "finished_at_utc": now_iso,
        "duration_seconds": 0.0,
        "exit_code": 0 if ok else 1,
        "stdout_json_file": None,
        "stderr_log_file": None,
        "stdout_json_parse_error": None,
        "status": status,
        "output_file": None,
        "ok": ok,
    }
    if reason:
        step["reason"] = reason
    if payload:
        step.update(payload)
    return step


def _is_enabled(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if not text:
        return default
    return text not in {"0", "false", "no", "off"}


def _latest_step_with_prefix(steps: list[dict[str, Any]], prefix: str) -> dict[str, Any] | None:
    for step in reversed(steps):
        if str(step.get("name") or "").startswith(prefix):
            return step
    return None


def _is_daily_weather_board_stale_gate(step: dict[str, Any] | None) -> bool:
    if not isinstance(step, dict):
        return False
    gate_status = str(step.get("prior_trade_gate_status") or "").strip().lower()
    return gate_status == "daily_weather_board_stale"


def _kalshi_api_roots_for_env(env_values: dict[str, str]) -> tuple[str, ...]:
    env_name = str(env_values.get("KALSHI_ENV") or "prod").strip().lower()
    if env_name == "demo":
        return ("https://demo-api.kalshi.co/trade-api/v2",)
    if env_name in {"prod", "production"}:
        return (
            "https://api.elections.kalshi.com/trade-api/v2",
            "https://trading-api.kalshi.com/trade-api/v2",
        )
    return ("https://api.elections.kalshi.com/trade-api/v2",)


def _http_get_json_url(url: str, timeout_seconds: float) -> tuple[int, Any]:
    request = Request(
        url=url,
        headers={
            "Accept": "application/json",
            "User-Agent": "betbot-hourly-overnight/1.0",
        },
        method="GET",
    )
    try:
        if callable(_urlopen_with_dns_recovery):
            with _urlopen_with_dns_recovery(
                request,
                timeout_seconds=max(1.0, float(timeout_seconds)),
                urlopen_fn=urlopen,
            ) as response:
                status = int(getattr(response, "status", 200) or 200)
                payload_text = response.read().decode("utf-8", errors="replace")
        else:
            with urlopen(request, timeout=max(1.0, float(timeout_seconds))) as response:
                status = int(getattr(response, "status", 200) or 200)
                payload_text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        status = int(exc.code or 0)
        payload_text = exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        return 599, {"error": str(exc), "error_type": type(exc).__name__}
    try:
        return status, json.loads(payload_text)
    except json.JSONDecodeError:
        return status, {"raw_text": payload_text[:400]}


def _is_orderable_price(price: float | None) -> bool:
    return isinstance(price, float) and 0.0 < price < 1.0


def _is_endpoint_quote(price: float | None) -> bool:
    return isinstance(price, float) and (price <= 0.0 or price >= 1.0)


def _classify_daily_weather_lane_from_quotes(
    *,
    yes_bid: float | None,
    no_bid: float | None,
    yes_ask: float | None,
    no_ask: float | None,
) -> dict[str, Any]:
    quotes_present = [value for value in (yes_bid, no_bid, yes_ask, no_ask) if isinstance(value, float)]
    has_orderable_bid = _is_orderable_price(yes_bid) or _is_orderable_price(no_bid)
    has_orderable_ask = _is_orderable_price(yes_ask) or _is_orderable_price(no_ask)
    has_orderable_side = has_orderable_bid or has_orderable_ask
    endpoint_only = (
        bool(quotes_present)
        and not has_orderable_side
        and all(_is_endpoint_quote(value) for value in quotes_present)
    )
    lane = "watch_endpoint_only" if endpoint_only else "tradable"
    return {
        "lane": lane,
        "quotes_present_count": len(quotes_present),
        "has_orderable_bid": has_orderable_bid,
        "has_orderable_ask": has_orderable_ask,
        "has_orderable_side": has_orderable_side,
        "endpoint_only": endpoint_only,
    }


def _load_daily_weather_refresh_state(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    if not isinstance(payload, dict):
        return {
            "run_counter": 0,
            "watch_cursor": 0,
            "lane_by_ticker": {},
            "has_orderable_side_by_ticker": {},
            "ticker_stats_by_ticker": {},
        }
    lane_by_ticker = payload.get("lane_by_ticker")
    if not isinstance(lane_by_ticker, dict):
        lane_by_ticker = payload.get("ticker_lane_map")
    if not isinstance(lane_by_ticker, dict):
        lane_by_ticker = {}
    has_orderable_side_by_ticker = payload.get("has_orderable_side_by_ticker")
    if not isinstance(has_orderable_side_by_ticker, dict):
        has_orderable_side_by_ticker = payload.get("ticker_orderable_map")
    if not isinstance(has_orderable_side_by_ticker, dict):
        has_orderable_side_by_ticker = {}
    ticker_stats_by_ticker = payload.get("ticker_stats_by_ticker")
    if not isinstance(ticker_stats_by_ticker, dict):
        ticker_stats_by_ticker = {}
    run_counter_raw = payload.get("run_counter")
    watch_cursor_raw = payload.get("watch_cursor")
    run_counter = int(run_counter_raw) if isinstance(run_counter_raw, (int, float)) else 0
    watch_cursor = int(watch_cursor_raw) if isinstance(watch_cursor_raw, (int, float)) else 0
    return {
        "run_counter": max(0, run_counter),
        "watch_cursor": max(0, watch_cursor),
        "lane_by_ticker": lane_by_ticker,
        "has_orderable_side_by_ticker": has_orderable_side_by_ticker,
        "ticker_stats_by_ticker": ticker_stats_by_ticker,
    }


def _write_daily_weather_refresh_state(path: Path, payload: dict[str, Any]) -> str | None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return None
    except Exception as exc:  # pragma: no cover - best effort persistence
        return str(exc)


def _classify_daily_weather_lane_from_prior_row(row: dict[str, Any]) -> dict[str, Any]:
    yes_bid = _parse_float(row.get("latest_yes_bid_dollars"))
    no_bid = _parse_float(row.get("latest_no_bid_dollars"))
    yes_ask = _parse_float(row.get("latest_yes_ask_dollars"))
    no_ask = _parse_float(row.get("latest_no_ask_dollars"))
    if yes_bid is None:
        yes_bid = _parse_float(row.get("yes_bid_dollars"))
    if no_bid is None:
        no_bid = _parse_float(row.get("no_bid_dollars"))
    if yes_ask is None:
        yes_ask = _parse_float(row.get("yes_ask_dollars"))
    if no_ask is None:
        no_ask = _parse_float(row.get("no_ask_dollars"))
    return _classify_daily_weather_lane_from_quotes(
        yes_bid=yes_bid,
        no_bid=no_bid,
        yes_ask=yes_ask,
        no_ask=no_ask,
    )


def _load_daily_weather_tickers_from_priors(
    *,
    priors_csv: Path,
    allowed_contract_families: list[str],
    max_markets: int,
) -> dict[str, Any]:
    if not priors_csv.exists():
        return {
            "selected_tickers": [],
            "tradable_tickers": [],
            "watch_tickers": [],
            "ranked_watch_tickers": [],
            "watch_hours_to_close_by_ticker": {},
            "watch_tickers_selected": [],
            "lane_by_ticker": {},
            "has_orderable_side_by_ticker": {},
            "ticker_stats_by_ticker": {},
            "lane_counts": {},
            "watch_refresh_due": False,
            "watch_refresh_reason": "priors_missing",
            "watch_refresh_interval_runs": 1,
            "watch_max_markets": 0,
            "watch_cursor_before": 0,
            "watch_cursor_after": 0,
            "state_run_counter": 0,
            "state_write_error": None,
            "endpoint_to_orderable_transition_count": 0,
            "endpoint_to_orderable_transition_tickers": [],
            "watch_priority_candidates": [],
        }
    allowed_set = {str(value or "").strip().lower() for value in allowed_contract_families if str(value or "").strip()}
    max_markets_effective = max(1, int(max_markets))
    max_watch_markets = max(0, int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_MAX_MARKETS", "4")))
    watch_interval_runs = max(1, int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_INTERVAL_RUNS", "3")))
    state_path = Path(
        str(
            os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_STATE_FILE")
            or (Path(os.environ["BETBOT_OUTPUT_DIR"]) / "overnight_alpha" / "daily_weather_ticker_refresh_state.json")
        )
    )
    state_before = _load_daily_weather_refresh_state(state_path)
    state_run_counter = int(state_before.get("run_counter") or 0) + 1
    watch_cursor_before = int(state_before.get("watch_cursor") or 0)
    prev_lane_by_ticker = dict(state_before.get("lane_by_ticker") or {})
    prev_has_orderable_side_by_ticker = dict(state_before.get("has_orderable_side_by_ticker") or {})
    ticker_stats_by_ticker = dict(state_before.get("ticker_stats_by_ticker") or {})
    now_utc = datetime.now(timezone.utc)

    tradable_tickers: list[str] = []
    watch_tickers: list[str] = []
    prior_hours_to_close_by_ticker: dict[str, float | None] = {}
    lane_by_ticker: dict[str, str] = {}
    has_orderable_side_by_ticker: dict[str, bool] = {}
    lane_counts: dict[str, int] = {}
    endpoint_to_orderable_tickers: list[str] = []
    seen: set[str] = set()
    with priors_csv.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            contract_family = str(row.get("contract_family") or "").strip().lower()
            if contract_family not in allowed_set:
                continue
            ticker = str(row.get("market_ticker") or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            lane_info = _classify_daily_weather_lane_from_prior_row(row)
            lane = str(lane_info.get("lane") or "tradable")
            has_orderable_side = bool(lane_info.get("has_orderable_side"))
            quotes_present_count = int(lane_info.get("quotes_present_count") or 0)
            if (
                quotes_present_count == 0
                and not has_orderable_side
                and str(prev_lane_by_ticker.get(ticker) or "").strip()
            ):
                lane = str(prev_lane_by_ticker.get(ticker) or lane)
                has_orderable_side = bool(prev_has_orderable_side_by_ticker.get(ticker))
            lane_by_ticker[ticker] = lane
            has_orderable_side_by_ticker[ticker] = has_orderable_side
            lane_counts[lane] = int(lane_counts.get(lane, 0)) + 1
            prior_hours_to_close_by_ticker[ticker] = _parse_float(row.get("hours_to_close"))
            if lane == "watch_endpoint_only":
                watch_tickers.append(ticker)
            else:
                tradable_tickers.append(ticker)
            if (
                str(prev_lane_by_ticker.get(ticker) or "") == "watch_endpoint_only"
                and bool(prev_has_orderable_side_by_ticker.get(ticker))
                is False
                and has_orderable_side
            ):
                endpoint_to_orderable_tickers.append(ticker)

    watch_refresh_due = (state_run_counter % watch_interval_runs == 0) or not tradable_tickers
    watch_refresh_reason = "interval_due" if (state_run_counter % watch_interval_runs == 0) else "interval_not_due"
    if not tradable_tickers:
        watch_refresh_reason = "no_tradable_tickers"
    watch_priority_candidates: list[dict[str, Any]] = []
    ranked_watch_tickers = list(watch_tickers)
    if ranked_watch_tickers:
        scored: list[tuple[float, str, dict[str, Any]]] = []
        for ticker in ranked_watch_tickers:
            score, components = _daily_weather_watch_priority_score(
                now_utc=now_utc,
                run_counter=state_run_counter,
                ticker_stats=dict(ticker_stats_by_ticker.get(ticker) or {}),
                prior_hours_to_close=prior_hours_to_close_by_ticker.get(ticker),
            )
            scored.append((score, ticker, components))
        scored.sort(
            key=lambda item: (
                -float(item[0]),
                float(item[2].get("minutes_since_last_non_endpoint") or 999999.0),
                float(item[2].get("hours_to_close") or 999999.0),
                item[1],
            )
        )
        ranked_watch_tickers = [ticker for _, ticker, _ in scored]
        watch_priority_candidates = [
            {
                "market_ticker": ticker,
                "priority_score": score,
                **components,
            }
            for score, ticker, components in scored[: min(20, len(scored))]
        ]
    selected_watch_tickers: list[str] = []
    watch_cursor_after = watch_cursor_before
    if watch_refresh_due and ranked_watch_tickers and max_watch_markets > 0:
        watch_take = min(max_watch_markets, max_markets_effective)
        selected_watch_tickers = list(ranked_watch_tickers[:watch_take])
        watch_cursor_after = (watch_cursor_before + len(selected_watch_tickers)) % max(
            1,
            len(ranked_watch_tickers),
        )

    selected_tickers = list(tradable_tickers[:max_markets_effective])
    remaining_slots = max(0, max_markets_effective - len(selected_tickers))
    if remaining_slots > 0 and selected_watch_tickers:
        selected_tickers.extend(selected_watch_tickers[:remaining_slots])
    selected_tickers = selected_tickers[:max_markets_effective]

    state_write_error = None
    return {
        "selected_tickers": selected_tickers,
        "tradable_tickers": tradable_tickers,
        "watch_tickers": watch_tickers,
        "ranked_watch_tickers": ranked_watch_tickers,
        "watch_hours_to_close_by_ticker": {
            ticker: prior_hours_to_close_by_ticker.get(ticker)
            for ticker in ranked_watch_tickers
            if ticker in prior_hours_to_close_by_ticker
        },
        "watch_tickers_selected": selected_watch_tickers,
        "lane_by_ticker": lane_by_ticker,
        "has_orderable_side_by_ticker": has_orderable_side_by_ticker,
        "ticker_stats_by_ticker": ticker_stats_by_ticker,
        "lane_counts": lane_counts,
        "watch_refresh_due": watch_refresh_due,
        "watch_refresh_reason": watch_refresh_reason,
        "watch_refresh_interval_runs": watch_interval_runs,
        "watch_max_markets": max_watch_markets,
        "watch_cursor_before": watch_cursor_before,
        "watch_cursor_after": watch_cursor_after,
        "state_run_counter": state_run_counter,
        "state_file": str(state_path),
        "state_write_error": state_write_error,
        "previous_lane_by_ticker": prev_lane_by_ticker,
        "previous_has_orderable_side_by_ticker": prev_has_orderable_side_by_ticker,
        "endpoint_to_orderable_transition_count": len(endpoint_to_orderable_tickers),
        "endpoint_to_orderable_transition_tickers": endpoint_to_orderable_tickers[:25],
        "watch_priority_candidates": watch_priority_candidates,
        "watch_selection_mode": "priority_score",
    }


def _append_history_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if callable(_capture_append_history):
        _capture_append_history(path, rows)
        return
    fieldnames = list(_capture_history_fieldnames or [])
    if not fieldnames:
        raise RuntimeError("history fieldnames unavailable for ticker refresh append")
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _history_row_from_market_payload(
    *,
    market_ticker: str,
    market_payload: dict[str, Any],
    captured_at: datetime,
) -> dict[str, Any]:
    yes_bid = _parse_float(market_payload.get("yes_bid_dollars"))
    yes_ask = _parse_float(market_payload.get("yes_ask_dollars"))
    no_bid = _parse_float(market_payload.get("no_bid_dollars"))
    no_ask = _parse_float(market_payload.get("no_ask_dollars"))
    last_price = _parse_float(market_payload.get("last_price_dollars"))
    liquidity = _parse_float(market_payload.get("liquidity_dollars"))
    volume_24h = _parse_float(market_payload.get("volume_24h_fp"))
    open_interest = _parse_float(market_payload.get("open_interest_fp"))
    yes_bid_size = _parse_float(market_payload.get("yes_bid_size_fp"))
    yes_ask_size = _parse_float(market_payload.get("yes_ask_size_fp"))
    spread = ""
    if isinstance(yes_ask, float) and isinstance(yes_bid, float) and yes_ask >= yes_bid:
        spread = round(yes_ask - yes_bid, 6)
    contracts_for_ten_dollars = None
    ten_dollar_fillable = False
    if isinstance(yes_ask, float) and yes_ask > 0:
        contracts_for_ten_dollars = max(1, math.floor(10.0 / yes_ask))
        ten_dollar_fillable = isinstance(yes_ask_size, float) and yes_ask_size >= contracts_for_ten_dollars
    two_sided_book = (
        isinstance(yes_bid, float)
        and yes_bid > 0
        and isinstance(yes_ask, float)
        and yes_ask > 0
        and isinstance(yes_bid_size, float)
        and yes_bid_size > 0
        and isinstance(yes_ask_size, float)
        and yes_ask_size > 0
    )
    close_time_text = str(market_payload.get("close_time") or "").strip()
    hours_to_close = ""
    close_time_dt = _parse_iso(close_time_text)
    if isinstance(close_time_dt, datetime):
        hours_to_close = round((close_time_dt - captured_at.astimezone(timezone.utc)).total_seconds() / 3600.0, 4)
    return {
        "captured_at": captured_at.isoformat(),
        "summary_file": "",
        "scan_csv": "",
        "category": "Climate and Weather",
        "market_family": "weather_climate",
        "resolution_source_type": "official_source",
        "series_ticker": str(market_payload.get("series_ticker") or "").strip(),
        "event_ticker": str(market_payload.get("event_ticker") or "").strip(),
        "market_ticker": market_ticker,
        "event_title": str(market_payload.get("event_ticker") or "").strip(),
        "event_sub_title": "",
        "market_title": str(market_payload.get("title") or "").strip(),
        "yes_sub_title": str(market_payload.get("yes_sub_title") or "").strip(),
        "rules_primary": str(market_payload.get("rules_primary") or "").strip(),
        "close_time": close_time_text,
        "hours_to_close": hours_to_close,
        "yes_bid_dollars": yes_bid if yes_bid is not None else "",
        "yes_bid_size_contracts": yes_bid_size if yes_bid_size is not None else "",
        "yes_ask_dollars": yes_ask if yes_ask is not None else "",
        "yes_ask_size_contracts": yes_ask_size if yes_ask_size is not None else "",
        "no_bid_dollars": no_bid if no_bid is not None else "",
        "no_ask_dollars": no_ask if no_ask is not None else "",
        "last_price_dollars": last_price if last_price is not None else "",
        "spread_dollars": spread,
        "liquidity_dollars": liquidity if liquidity is not None else "",
        "volume_24h_contracts": volume_24h if volume_24h is not None else "",
        "open_interest_contracts": open_interest if open_interest is not None else "",
        "ten_dollar_fillable_at_best_ask": ten_dollar_fillable,
        "two_sided_book": two_sided_book,
        "execution_fit_score": "",
    }


def _run_daily_weather_ticker_refresh(
    *,
    env_values: dict[str, str],
    priors_csv: Path,
    history_csv: Path,
    allowed_contract_families: list[str],
    timeout_seconds: float,
    max_markets: int,
    captured_at: datetime,
    explicit_market_tickers: list[str] | None = None,
) -> dict[str, Any]:
    started_at = _now_iso()
    started_monotonic = time.monotonic()
    api_roots = _kalshi_api_roots_for_env(env_values)
    ticker_plan = _load_daily_weather_tickers_from_priors(
        priors_csv=priors_csv,
        allowed_contract_families=allowed_contract_families,
        max_markets=max_markets,
    )
    refresh_mode = "prior_selected"
    tickers = list(ticker_plan.get("selected_tickers") or [])
    if explicit_market_tickers:
        refresh_mode = "explicit_tickers"
        seen_explicit: set[str] = set()
        explicit_clean: list[str] = []
        for value in explicit_market_tickers:
            ticker = str(value or "").strip().upper()
            if not ticker or ticker in seen_explicit:
                continue
            seen_explicit.add(ticker)
            explicit_clean.append(ticker)
        tickers = explicit_clean
    rows: list[dict[str, Any]] = []
    fetch_errors: list[dict[str, Any]] = []
    tickers_succeeded = 0
    requests_total = 0
    for ticker in tickers:
        market_payload: dict[str, Any] | None = None
        last_status: int | None = None
        last_error: str | None = None
        for api_root in api_roots:
            requests_total += 1
            status_code, payload = _http_get_json_url(
                f"{api_root}/markets/{ticker}",
                timeout_seconds=max(1.0, float(timeout_seconds)),
            )
            last_status = int(status_code)
            if status_code == 200 and isinstance(payload, dict) and isinstance(payload.get("market"), dict):
                market_payload = dict(payload.get("market") or {})
                break
            if isinstance(payload, dict):
                error_text = str(payload.get("error") or payload.get("errorMessage") or "").strip()
                if error_text:
                    last_error = error_text
        if not isinstance(market_payload, dict):
            fetch_errors.append(
                {
                    "market_ticker": ticker,
                    "http_status": last_status,
                    "error": last_error or "market_fetch_failed",
                }
            )
            continue
        rows.append(
            _history_row_from_market_payload(
                market_ticker=ticker,
                market_payload=market_payload,
                captured_at=captured_at,
            )
        )
        tickers_succeeded += 1

    append_error = None
    rows_appended = 0
    if rows:
        try:
            _append_history_rows(history_csv, rows)
            rows_appended = len(rows)
        except Exception as exc:
            append_error = str(exc)

    lane_by_ticker = dict(ticker_plan.get("lane_by_ticker") or {})
    has_orderable_side_by_ticker = dict(ticker_plan.get("has_orderable_side_by_ticker") or {})
    ticker_stats_by_ticker = dict(ticker_plan.get("ticker_stats_by_ticker") or {})
    observed_at_utc = _now_iso()
    observed_now_utc = datetime.now(timezone.utc)
    watch_selected_set = {
        str(value or "").strip().upper()
        for value in (ticker_plan.get("watch_tickers_selected") or [])
        if str(value or "").strip()
    }
    for ticker in tickers:
        if ticker not in watch_selected_set:
            continue
        stats = dict(ticker_stats_by_ticker.get(ticker) or {})
        stats["last_watch_selected_at_utc"] = observed_at_utc
        stats["last_watch_selected_run_counter"] = int(ticker_plan.get("state_run_counter") or 0)
        stats["watch_selected_count"] = max(0, _coerce_int(stats.get("watch_selected_count"), 0)) + 1
        ticker_stats_by_ticker[ticker] = stats
    for row in rows:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        if not ticker:
            continue
        observed_lane = _classify_daily_weather_lane_from_quotes(
            yes_bid=_parse_float(row.get("yes_bid_dollars")),
            no_bid=_parse_float(row.get("no_bid_dollars")),
            yes_ask=_parse_float(row.get("yes_ask_dollars")),
            no_ask=_parse_float(row.get("no_ask_dollars")),
        )
        observed_lane_text = str(observed_lane.get("lane") or "tradable")
        observed_has_orderable_side = bool(observed_lane.get("has_orderable_side"))
        lane_by_ticker[ticker] = observed_lane_text
        has_orderable_side_by_ticker[ticker] = observed_has_orderable_side
        stats = dict(ticker_stats_by_ticker.get(ticker) or {})
        stats["last_observed_at_utc"] = observed_at_utc
        stats["last_observed_lane"] = observed_lane_text
        if observed_lane_text == "watch_endpoint_only":
            stats["last_endpoint_only_at_utc"] = observed_at_utc
            stats["endpoint_only_streak_count"] = max(0, _coerce_int(stats.get("endpoint_only_streak_count"), 0)) + 1
        else:
            stats["last_non_endpoint_at_utc"] = observed_at_utc
            stats["endpoint_only_streak_count"] = 0
        if observed_has_orderable_side:
            stats["had_orderable_side_ever"] = True
            stats["last_orderable_side_at_utc"] = observed_at_utc
        hours_to_close = _parse_float(row.get("hours_to_close"))
        if isinstance(hours_to_close, float):
            stats["last_hours_to_close"] = round(hours_to_close, 6)
        ticker_stats_by_ticker[ticker] = stats
    for ticker, stats in list(ticker_stats_by_ticker.items()):
        if not isinstance(stats, dict):
            ticker_stats_by_ticker[ticker] = {}
            continue
        minutes_since_last_wakeup = _minutes_since_iso(stats.get("last_wakeup_at_utc"), now_utc=observed_now_utc)
        if isinstance(minutes_since_last_wakeup, float):
            stats["minutes_since_last_wakeup"] = round(minutes_since_last_wakeup, 3)
        else:
            stats["minutes_since_last_wakeup"] = None
        ticker_stats_by_ticker[ticker] = stats
    lane_counts: dict[str, int] = {}
    for lane in lane_by_ticker.values():
        lane_text = str(lane or "").strip() or "unknown"
        lane_counts[lane_text] = int(lane_counts.get(lane_text, 0)) + 1
    prev_lane_by_ticker = dict(ticker_plan.get("previous_lane_by_ticker") or {})
    prev_has_orderable_side_by_ticker = dict(ticker_plan.get("previous_has_orderable_side_by_ticker") or {})
    endpoint_to_orderable_transition_tickers: list[str] = []
    for ticker, has_orderable_side in has_orderable_side_by_ticker.items():
        if (
            str(prev_lane_by_ticker.get(ticker) or "") == "watch_endpoint_only"
            and bool(prev_has_orderable_side_by_ticker.get(ticker)) is False
            and bool(has_orderable_side)
        ):
            endpoint_to_orderable_transition_tickers.append(ticker)
    endpoint_to_orderable_transition_tickers = sorted(set(endpoint_to_orderable_transition_tickers))
    endpoint_to_orderable_transition_count = len(endpoint_to_orderable_transition_tickers)
    if endpoint_to_orderable_transition_tickers:
        for ticker in endpoint_to_orderable_transition_tickers:
            stats = dict(ticker_stats_by_ticker.get(ticker) or {})
            stats["last_wakeup_at_utc"] = observed_at_utc
            stats["wakeup_count"] = max(0, _coerce_int(stats.get("wakeup_count"), 0)) + 1
            ticker_stats_by_ticker[ticker] = stats

    state_file_text = str(ticker_plan.get("state_file") or "").strip()
    state_write_error = None
    if state_file_text:
        state_tickers = set(lane_by_ticker.keys()) | set(ticker_stats_by_ticker.keys())
        if state_tickers:
            ticker_stats_by_ticker = {
                ticker: dict(ticker_stats_by_ticker.get(ticker) or {})
                for ticker in sorted(state_tickers)
            }
        state_write_error = _write_daily_weather_refresh_state(
            Path(state_file_text),
            {
                "run_counter": int(ticker_plan.get("state_run_counter") or 0),
                "watch_cursor": int(ticker_plan.get("watch_cursor_after") or 0),
                "watch_interval_runs": int(ticker_plan.get("watch_refresh_interval_runs") or 1),
                "watch_max_markets": int(ticker_plan.get("watch_max_markets") or 0),
                "lane_by_ticker": lane_by_ticker,
                "has_orderable_side_by_ticker": has_orderable_side_by_ticker,
                "ticker_stats_by_ticker": ticker_stats_by_ticker,
                "lane_counts": lane_counts,
                "updated_at_utc": _now_iso(),
                "endpoint_to_orderable_transition_count_last": endpoint_to_orderable_transition_count,
                "endpoint_to_orderable_transition_tickers_last": endpoint_to_orderable_transition_tickers[:25],
            },
        )

    if not tickers:
        status = "skipped_no_daily_weather_tickers"
        reason = "no_daily_weather_tickers_in_priors"
    elif rows_appended > 0 and not append_error:
        status = "ready"
        reason = "daily_weather_tickers_refreshed"
    elif append_error:
        status = "append_failed"
        reason = "history_append_failed"
    else:
        status = "upstream_error"
        reason = "market_refresh_failed"

    finished_at = _now_iso()
    duration_seconds = round(time.monotonic() - started_monotonic, 3)
    return {
        "name": "daily_weather_ticker_refresh",
        "command": ["kalshi-market-refresh-by-ticker"],
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration_seconds,
        "exit_code": 0,
        "stdout_json_file": None,
        "stderr_log_file": None,
        "stdout_json_parse_error": None,
        "status": status,
        "output_file": None,
        "ok": True,
        "reason": reason,
        "history_csv": str(history_csv),
        "priors_csv": str(priors_csv),
        "api_roots": list(api_roots),
        "market_tickers_attempted": tickers,
        "market_tickers_attempted_count": len(tickers),
        "market_tickers_succeeded_count": tickers_succeeded,
        "refresh_mode": refresh_mode,
        "watch_selection_mode": ticker_plan.get("watch_selection_mode"),
        "market_tickers_tradable": list(ticker_plan.get("tradable_tickers") or []),
        "market_tickers_tradable_count": len(list(ticker_plan.get("tradable_tickers") or [])),
        "market_tickers_watch": list(ticker_plan.get("watch_tickers") or []),
        "market_tickers_watch_count": len(list(ticker_plan.get("watch_tickers") or [])),
        "market_tickers_watch_selected": list(ticker_plan.get("watch_tickers_selected") or []),
        "market_tickers_watch_selected_count": len(list(ticker_plan.get("watch_tickers_selected") or [])),
        "watch_priority_candidates": list(ticker_plan.get("watch_priority_candidates") or []),
        "ticker_refresh_lane_counts": dict(lane_counts),
        "ticker_refresh_watch_due": bool(ticker_plan.get("watch_refresh_due")),
        "ticker_refresh_watch_reason": str(ticker_plan.get("watch_refresh_reason") or "").strip() or None,
        "ticker_refresh_watch_interval_runs": ticker_plan.get("watch_refresh_interval_runs"),
        "ticker_refresh_watch_max_markets": ticker_plan.get("watch_max_markets"),
        "ticker_refresh_watch_cursor_before": ticker_plan.get("watch_cursor_before"),
        "ticker_refresh_watch_cursor_after": ticker_plan.get("watch_cursor_after"),
        "ticker_refresh_state_run_counter": ticker_plan.get("state_run_counter"),
        "ticker_refresh_state_file": ticker_plan.get("state_file"),
        "ticker_refresh_state_write_error": state_write_error,
        "endpoint_to_orderable_transition_count": endpoint_to_orderable_transition_count,
        "endpoint_to_orderable_transition_tickers": endpoint_to_orderable_transition_tickers[:25],
        "requests_total": requests_total,
        "rows_appended": rows_appended,
        "append_error": append_error,
        "fetch_errors": fetch_errors[:20],
    }


def _run_daily_weather_micro_watch(
    *,
    env_values: dict[str, str],
    priors_csv: Path,
    history_csv: Path,
    allowed_contract_families: list[str],
    timeout_seconds: float,
    max_markets: int,
    captured_at: datetime,
    poll_interval_seconds: float,
    max_polls: int,
    active_hours_to_close: float,
    include_unknown_hours_to_close: bool,
) -> dict[str, Any]:
    watch_plan = _load_daily_weather_tickers_from_priors(
        priors_csv=priors_csv,
        allowed_contract_families=allowed_contract_families,
        max_markets=max_markets,
    )
    ranked_watch_tickers = list(watch_plan.get("ranked_watch_tickers") or watch_plan.get("watch_tickers") or [])
    watch_hours_to_close_by_ticker_raw = dict(watch_plan.get("watch_hours_to_close_by_ticker") or {})
    ticker_stats_by_ticker = dict(watch_plan.get("ticker_stats_by_ticker") or {})
    watch_hours_to_close_by_ticker = {
        str(ticker).strip().upper(): _parse_float(value)
        for ticker, value in watch_hours_to_close_by_ticker_raw.items()
        if str(ticker).strip()
    }
    watch_hours_effective_by_ticker: dict[str, float | None] = {}
    for ticker in ranked_watch_tickers:
        ticker_key = str(ticker or "").strip().upper()
        if not ticker_key:
            continue
        ticker_hours = watch_hours_to_close_by_ticker.get(ticker_key)
        if not isinstance(ticker_hours, float):
            ticker_hours = _parse_float(
                dict(ticker_stats_by_ticker.get(ticker_key) or {}).get("last_hours_to_close")
            )
        watch_hours_effective_by_ticker[ticker_key] = ticker_hours
    watch_total = len(ranked_watch_tickers)
    active_watch_tickers: list[str] = []
    for ticker in ranked_watch_tickers:
        ticker_hours = watch_hours_effective_by_ticker.get(ticker)
        if isinstance(ticker_hours, float):
            if ticker_hours < 0.0:
                continue
            if ticker_hours <= max(0.0, float(active_hours_to_close)):
                active_watch_tickers.append(ticker)
            continue
        if include_unknown_hours_to_close:
            active_watch_tickers.append(ticker)

    selected_watch_tickers = active_watch_tickers[: max(1, int(max_markets))]
    polls_planned = max(1, int(max_polls))
    poll_steps: list[dict[str, Any]] = []
    wakeup_transition_tickers: list[str] = []
    wakeup_transition_seen: set[str] = set()
    wakeup_detected_at_poll: int | None = None

    for poll_index, _ in enumerate(range(polls_planned), start=1):
        if not selected_watch_tickers:
            break
        poll_step = _run_daily_weather_ticker_refresh(
            env_values=env_values,
            priors_csv=priors_csv,
            history_csv=history_csv,
            allowed_contract_families=allowed_contract_families,
            timeout_seconds=timeout_seconds,
            max_markets=max(1, len(selected_watch_tickers)),
            captured_at=captured_at,
            explicit_market_tickers=selected_watch_tickers,
        )
        poll_step["name"] = f"daily_weather_ticker_refresh_micro_watch_{poll_index}"
        poll_step["micro_watch_poll_index"] = poll_index
        poll_step["micro_watch_poll_total"] = polls_planned
        poll_step["micro_watch_selected_tickers"] = selected_watch_tickers
        poll_steps.append(poll_step)

        for value in poll_step.get("endpoint_to_orderable_transition_tickers") or []:
            ticker = str(value or "").strip().upper()
            if not ticker or ticker in wakeup_transition_seen:
                continue
            wakeup_transition_seen.add(ticker)
            wakeup_transition_tickers.append(ticker)
        if wakeup_transition_tickers and wakeup_detected_at_poll is None:
            wakeup_detected_at_poll = poll_index
            break

        if poll_index < polls_planned and float(poll_interval_seconds) > 0.0:
            time.sleep(float(poll_interval_seconds))

    status = "ready_no_wakeup"
    reason = "micro_watch_completed_without_wakeup"
    if watch_total == 0:
        status = "skipped_no_watch_tickers"
        reason = "no_watch_tickers_available"
    elif not active_watch_tickers:
        status = "skipped_no_active_window"
        reason = "watch_tickers_outside_active_window"
    elif wakeup_transition_tickers:
        status = "wakeup_detected"
        reason = "endpoint_to_orderable_transition_detected"
    elif poll_steps and all(str(step.get("status") or "").strip().lower() == "upstream_error" for step in poll_steps):
        status = "upstream_error"
        reason = "micro_watch_all_polls_upstream_error"

    summary_step = _synthetic_step(
        name="daily_weather_micro_watch",
        status=status,
        ok=True,
        reason=reason,
        payload={
            "watch_total": watch_total,
            "active_watch_tickers_count": len(active_watch_tickers),
            "active_watch_tickers": active_watch_tickers[:30],
            "selected_watch_tickers_count": len(selected_watch_tickers),
            "selected_watch_tickers": selected_watch_tickers,
            "polls_planned": polls_planned,
            "polls_completed": len(poll_steps),
            "poll_interval_seconds": round(float(poll_interval_seconds), 3),
            "active_hours_to_close": round(float(active_hours_to_close), 6),
            "include_unknown_hours_to_close": bool(include_unknown_hours_to_close),
            "watch_hours_to_close_by_ticker": {
                ticker: watch_hours_effective_by_ticker.get(ticker)
                for ticker in selected_watch_tickers
            },
            "watch_priority_candidates": list(watch_plan.get("watch_priority_candidates") or []),
            "wakeup_transition_count": len(wakeup_transition_tickers),
            "wakeup_transition_tickers": wakeup_transition_tickers[:25],
            "wakeup_detected_at_poll": wakeup_detected_at_poll,
        },
    )
    return {
        "poll_steps": poll_steps,
        "summary_step": summary_step,
        "wakeup_transition_tickers": wakeup_transition_tickers,
        "watch_plan": watch_plan,
    }


def _weather_cache_state(*, output_dir: Path, max_age_hours: float) -> dict[str, Any]:
    cache_dir = output_dir / "weather_station_history_cache"
    threshold_seconds = max(0.0, float(max_age_hours)) * 3600.0
    state: dict[str, Any] = {
        "cache_dir": str(cache_dir),
        "cache_exists": cache_dir.exists(),
        "cache_file_count": 0,
        "cache_newest_age_seconds": None,
        "cache_freshness_threshold_seconds": round(threshold_seconds, 3),
        "cache_stale": True,
        "cache_reason": "cache_missing_or_empty",
    }
    if not cache_dir.exists():
        return state
    entries = [path for path in cache_dir.glob("*.json") if path.is_file()]
    state["cache_file_count"] = len(entries)
    if not entries:
        return state
    newest_mtime = max(path.stat().st_mtime for path in entries)
    newest_dt = datetime.fromtimestamp(newest_mtime, tz=timezone.utc)
    newest_age_seconds = max(0.0, (datetime.now(timezone.utc) - newest_dt).total_seconds())
    fresh = newest_age_seconds <= threshold_seconds
    state.update(
        {
            "cache_newest_mtime_utc": newest_dt.isoformat(),
            "cache_newest_age_seconds": round(newest_age_seconds, 3),
            "cache_stale": not fresh,
            "cache_reason": "cache_fresh" if fresh else "cache_stale",
        }
    )
    return state


def _parse_csv_list(value: str | None) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    items = [part.strip() for part in text.split(",")]
    return [item for item in items if item]


def _weather_prior_state(
    *,
    priors_csv: Path,
    max_age_hours: float,
    allowed_contract_families: list[str],
) -> dict[str, Any]:
    threshold_seconds = max(0.0, float(max_age_hours)) * 3600.0
    meta = _file_meta(priors_csv)
    state: dict[str, Any] = {
        "priors_csv": str(priors_csv),
        "priors_exists": bool(meta.get("exists")),
        "priors_mtime_utc": meta.get("mtime_utc"),
        "priors_age_seconds": meta.get("age_seconds"),
        "priors_freshness_threshold_seconds": round(threshold_seconds, 3),
        "allowed_contract_families": list(allowed_contract_families),
        "contract_family_counts": {},
        "allowed_family_counts": {},
        "allowed_rows_total": 0,
        "stale": True,
        "reason": "priors_missing",
        "parse_error": None,
    }
    if not bool(meta.get("exists")):
        return state
    age_seconds = _parse_float(meta.get("age_seconds"))
    if age_seconds is not None and age_seconds > threshold_seconds:
        state["reason"] = "priors_stale"
    else:
        state["reason"] = "no_allowed_weather_priors"

    counts: dict[str, int] = {}
    try:
        with priors_csv.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                family = str((row or {}).get("contract_family") or "").strip()
                if not family:
                    continue
                counts[family] = counts.get(family, 0) + 1
    except Exception as exc:  # pragma: no cover - best effort diagnostics
        state["parse_error"] = str(exc)
        state["reason"] = "priors_parse_error"
        return state

    state["contract_family_counts"] = counts
    allowed_counts = {family: int(counts.get(family, 0)) for family in allowed_contract_families}
    state["allowed_family_counts"] = allowed_counts
    allowed_rows_total = sum(allowed_counts.values())
    state["allowed_rows_total"] = int(allowed_rows_total)

    if state["reason"] == "priors_stale":
        state["stale"] = True
    elif allowed_rows_total <= 0:
        state["stale"] = True
        state["reason"] = "no_allowed_weather_priors"
    else:
        state["stale"] = False
        state["reason"] = "weather_priors_fresh"
    return state


def _as_bool(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def _weather_history_token_state(env_values: dict[str, str]) -> dict[str, Any]:
    keys = ["BETBOT_NOAA_CDO_TOKEN", "NOAA_CDO_TOKEN", "NCEI_CDO_TOKEN"]
    for key in keys:
        value = env_values.get(key)
        if value is None:
            continue
        if _is_placeholder_value(value):
            continue
        if str(value).strip():
            return {
                "weather_history_token_present": True,
                "weather_history_token_env_key": key,
                "weather_history_token_source": f"env:{key}",
                "weather_history_token_file_used": None,
            }
    return {
        "weather_history_token_present": False,
        "weather_history_token_env_key": None,
        "weather_history_token_source": "missing",
        "weather_history_token_file_used": None,
    }


def _resolve_weather_history_token(
    *,
    env_values: dict[str, str],
    repo_root: Path,
) -> dict[str, Any]:
    direct_state = _weather_history_token_state(env_values)
    if bool(direct_state.get("weather_history_token_present")):
        return direct_state

    file_candidates: list[Path] = []
    explicit_file = str(env_values.get("BETBOT_WEATHER_CDO_TOKEN_FILE") or "").strip()
    if explicit_file:
        explicit_path = Path(explicit_file).expanduser()
        if not explicit_path.is_absolute():
            explicit_path = (repo_root / explicit_path).resolve()
        file_candidates.append(explicit_path)
    else:
        default_from_env = str(os.environ.get("BETBOT_WEATHER_CDO_TOKEN_FILE") or "").strip()
        if default_from_env:
            default_path = Path(default_from_env).expanduser()
            if not default_path.is_absolute():
                default_path = (repo_root / default_path).resolve()
            file_candidates.append(default_path)
    fallback_paths = [
        repo_root / ".secrets" / "noaa_cdo_token.txt",
        repo_root / ".secrets" / "ncei_cdo_token.txt",
        repo_root / ".secrets" / "cdo_token.txt",
    ]
    for fallback in fallback_paths:
        if fallback not in file_candidates:
            file_candidates.append(fallback)

    for candidate in file_candidates:
        if not candidate.exists() or not candidate.is_file():
            continue
        try:
            token_text = candidate.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if _is_placeholder_value(token_text):
            continue
        if not token_text:
            continue
        env_values["BETBOT_NOAA_CDO_TOKEN"] = token_text
        return {
            "weather_history_token_present": True,
            "weather_history_token_env_key": "BETBOT_NOAA_CDO_TOKEN",
            "weather_history_token_source": "token_file",
            "weather_history_token_file_used": str(candidate),
        }

    direct_state["weather_history_token_source"] = "missing"
    direct_state["weather_history_token_file_used"] = None
    return direct_state


def _weather_history_readiness_state(
    *,
    priors_csv: Path,
    allowed_contract_families: list[str],
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "weather_history_rows_total": 0,
        "weather_history_live_ready_rows": 0,
        "weather_history_unhealthy_rows": 0,
        "weather_history_status_counts": {},
        "weather_history_live_ready_reason_counts": {},
        "weather_history_missing_token_count": 0,
        "weather_history_station_mapping_missing_count": 0,
        "weather_history_sample_depth_block_count": 0,
        "weather_history_parse_error": None,
    }
    if not priors_csv.exists():
        state["weather_history_parse_error"] = "priors_missing"
        return state
    try:
        with priors_csv.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                family = str((row or {}).get("contract_family") or "").strip()
                if family not in allowed_contract_families:
                    continue
                state["weather_history_rows_total"] += 1
                status_text = str((row or {}).get("weather_station_history_status") or "").strip()
                reason_text = str((row or {}).get("weather_station_history_live_ready_reason") or "").strip()
                live_ready = _as_bool((row or {}).get("weather_station_history_live_ready"))
                if status_text:
                    counts = state["weather_history_status_counts"]
                    counts[status_text] = int(counts.get(status_text, 0)) + 1
                if live_ready:
                    state["weather_history_live_ready_rows"] += 1
                else:
                    state["weather_history_unhealthy_rows"] += 1
                    if reason_text:
                        reason_counts = state["weather_history_live_ready_reason_counts"]
                        reason_counts[reason_text] = int(reason_counts.get(reason_text, 0)) + 1
                    if status_text == "disabled_missing_token" or reason_text.startswith(
                        "status_disabled_missing_token"
                    ):
                        state["weather_history_missing_token_count"] += 1
                    if status_text == "station_mapping_missing" or reason_text.startswith(
                        "status_station_mapping_missing"
                    ):
                        state["weather_history_station_mapping_missing_count"] += 1
                    if "sample_years_below_min" in reason_text:
                        state["weather_history_sample_depth_block_count"] += 1
    except Exception as exc:  # pragma: no cover - best effort diagnostics
        state["weather_history_parse_error"] = str(exc)
    return state


def _classify_balance_smoke_failure(*, message: Any, http_status: Any) -> str:
    text = str(message or "").strip().lower()
    status_text = str(http_status or "").strip()
    if "missing" in text and ("credential" in text or "environment" in text):
        return "missing_credentials"
    if "dns" in text:
        return "dns_failure"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    if status_text in {"401", "403"} or "unauthorized" in text or "forbidden" in text:
        return "auth_failed"
    if "connection" in text or "network" in text:
        return "network_failure"
    if status_text:
        return f"http_{status_text}"
    return "unknown"


def _plan_skip_diagnostics_from_summary(
    *,
    plan_summary_file: Any,
    top_n: int = 5,
) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {
        "plan_summary_file": None,
        "plan_summary_status": "missing",
        "plan_summary_error": None,
        "plan_summary_planned_orders": None,
        "plan_skip_counts_total": 0,
        "plan_skip_counts_nonzero_total": 0,
        "plan_skip_counts_top": [],
        "plan_skip_reason_dominant": None,
        "plan_skip_reason_dominant_count": None,
        "plan_skip_reason_dominant_share": None,
        "allowed_universe_candidate_pool_size": None,
        "allowed_universe_skip_counts_total": 0,
        "allowed_universe_skip_counts_nonzero_total": 0,
        "allowed_universe_skip_counts_top": [],
        "allowed_universe_skip_reason_dominant": None,
        "allowed_universe_skip_reason_dominant_count": None,
        "allowed_universe_skip_reason_dominant_share": None,
        "daily_weather_candidate_pool_size": None,
        "daily_weather_rows_total": None,
        "daily_weather_skip_counts_total": 0,
        "daily_weather_skip_counts_nonzero_total": 0,
        "daily_weather_skip_counts_top": [],
        "daily_weather_skip_reason_dominant": None,
        "daily_weather_skip_reason_dominant_count": None,
        "daily_weather_skip_reason_dominant_share": None,
        "daily_weather_rows_with_conservative_candidate": None,
        "daily_weather_rows_with_both_sides_candidate": None,
        "daily_weather_rows_with_one_side_failed": None,
        "daily_weather_rows_with_both_sides_failed": None,
        "daily_weather_orderable_bid_rows": None,
        "daily_weather_rows_with_any_orderable_bid": None,
        "daily_weather_rows_with_any_orderable_ask": None,
        "daily_weather_rows_with_fair_probabilities": None,
        "daily_weather_rows_with_both_quote_and_fair_value": None,
        "daily_weather_quote_orderability_counts": None,
        "daily_weather_quote_age_rows_with_timestamp": None,
        "daily_weather_quote_stale_max_age_seconds": None,
        "daily_weather_shadow_taker_rows_total": None,
        "daily_weather_shadow_taker_rows_with_orderable_yes_ask": None,
        "daily_weather_shadow_taker_rows_with_orderable_no_ask": None,
        "daily_weather_shadow_taker_rows_with_any_orderable_ask": None,
        "daily_weather_shadow_taker_edge_above_min_count": None,
        "daily_weather_shadow_taker_edge_net_fees_above_min_count": None,
        "daily_weather_shadow_taker_endpoint_orderbook_rows": None,
        "daily_weather_best_shadow_taker_candidate": None,
        "daily_weather_allowed_universe_rows_with_conservative_candidate": None,
        "daily_weather_endpoint_orderbook_filtered": None,
        "daily_weather_conservative_candidate_failure_counts": None,
        "daily_weather_planned_orders": None,
    }
    path_text = str(plan_summary_file or "").strip()
    diagnostics["plan_summary_file"] = path_text or None
    if not path_text:
        diagnostics["plan_summary_error"] = "plan_summary_file_missing"
        return diagnostics

    path = Path(path_text)
    if not path.exists():
        diagnostics["plan_summary_error"] = "plan_summary_file_not_found"
        return diagnostics

    payload = _load_json(path)
    if not isinstance(payload, dict):
        diagnostics["plan_summary_status"] = "invalid"
        diagnostics["plan_summary_error"] = "plan_summary_payload_invalid"
        return diagnostics

    diagnostics["plan_summary_status"] = str(payload.get("status") or "").strip() or "unknown"
    planned_orders_raw = payload.get("planned_orders")
    if isinstance(planned_orders_raw, (int, float)):
        diagnostics["plan_summary_planned_orders"] = int(planned_orders_raw)

    def _apply_skip_profile(
        *,
        counts_key: str,
        output_prefix: str,
    ) -> None:
        raw_counts = payload.get(counts_key)
        if not isinstance(raw_counts, dict):
            return
        normalized_counts: list[tuple[str, int]] = []
        for raw_reason, raw_count in raw_counts.items():
            reason = str(raw_reason or "").strip()
            if not reason:
                continue
            count_value: int | None = None
            if isinstance(raw_count, bool):
                count_value = int(raw_count)
            elif isinstance(raw_count, (int, float)):
                count_value = int(raw_count)
            else:
                count_parsed = _parse_float(raw_count)
                if isinstance(count_parsed, float):
                    count_value = int(count_parsed)
            if count_value is None:
                continue
            normalized_counts.append((reason, max(0, count_value)))

        total = int(sum(count for _, count in normalized_counts))
        nonzero = [(reason, count) for reason, count in normalized_counts if count > 0]
        nonzero.sort(key=lambda item: (-item[1], item[0]))
        top_limit = max(1, int(top_n))
        diagnostics[f"{output_prefix}_skip_counts_total"] = total
        diagnostics[f"{output_prefix}_skip_counts_nonzero_total"] = len(nonzero)
        diagnostics[f"{output_prefix}_skip_counts_top"] = [
            {"reason": reason, "count": count}
            for reason, count in nonzero[:top_limit]
        ]
        if nonzero:
            dominant_reason, dominant_count = nonzero[0]
            diagnostics[f"{output_prefix}_skip_reason_dominant"] = dominant_reason
            diagnostics[f"{output_prefix}_skip_reason_dominant_count"] = dominant_count
            if total > 0:
                diagnostics[f"{output_prefix}_skip_reason_dominant_share"] = round(
                    float(dominant_count) / float(total),
                    4,
                )

    _apply_skip_profile(counts_key="skip_counts", output_prefix="plan")
    if diagnostics["plan_skip_counts_total"] == 0 and not isinstance(payload.get("skip_counts"), dict):
        diagnostics["plan_summary_error"] = "skip_counts_missing"
        return diagnostics

    allowed_pool = payload.get("allowed_universe_candidate_pool_size")
    if isinstance(allowed_pool, (int, float)):
        diagnostics["allowed_universe_candidate_pool_size"] = int(allowed_pool)
    _apply_skip_profile(counts_key="allowed_universe_skip_counts", output_prefix="allowed_universe")

    daily_pool = payload.get("daily_weather_candidate_pool_size")
    if isinstance(daily_pool, (int, float)):
        diagnostics["daily_weather_candidate_pool_size"] = int(daily_pool)
    daily_rows_total = payload.get("daily_weather_rows_total")
    if isinstance(daily_rows_total, (int, float)):
        diagnostics["daily_weather_rows_total"] = int(daily_rows_total)
    _apply_skip_profile(counts_key="daily_weather_skip_counts", output_prefix="daily_weather")
    daily_conservative_rows = payload.get("daily_weather_rows_with_conservative_candidate")
    if isinstance(daily_conservative_rows, (int, float)):
        diagnostics["daily_weather_rows_with_conservative_candidate"] = int(daily_conservative_rows)
    daily_both_sides_rows = payload.get("daily_weather_rows_with_both_sides_candidate")
    if isinstance(daily_both_sides_rows, (int, float)):
        diagnostics["daily_weather_rows_with_both_sides_candidate"] = int(daily_both_sides_rows)
    daily_one_side_failed_rows = payload.get("daily_weather_rows_with_one_side_failed")
    if isinstance(daily_one_side_failed_rows, (int, float)):
        diagnostics["daily_weather_rows_with_one_side_failed"] = int(daily_one_side_failed_rows)
    daily_both_sides_failed_rows = payload.get("daily_weather_rows_with_both_sides_failed")
    if isinstance(daily_both_sides_failed_rows, (int, float)):
        diagnostics["daily_weather_rows_with_both_sides_failed"] = int(daily_both_sides_failed_rows)
    daily_orderable_bid_rows = payload.get("daily_weather_orderable_bid_rows")
    if isinstance(daily_orderable_bid_rows, (int, float)):
        diagnostics["daily_weather_orderable_bid_rows"] = int(daily_orderable_bid_rows)
    daily_rows_with_any_orderable_bid = payload.get("daily_weather_rows_with_any_orderable_bid")
    if isinstance(daily_rows_with_any_orderable_bid, (int, float)):
        diagnostics["daily_weather_rows_with_any_orderable_bid"] = int(daily_rows_with_any_orderable_bid)
    daily_rows_with_any_orderable_ask = payload.get("daily_weather_rows_with_any_orderable_ask")
    if isinstance(daily_rows_with_any_orderable_ask, (int, float)):
        diagnostics["daily_weather_rows_with_any_orderable_ask"] = int(daily_rows_with_any_orderable_ask)
    daily_rows_with_fair = payload.get("daily_weather_rows_with_fair_probabilities")
    if isinstance(daily_rows_with_fair, (int, float)):
        diagnostics["daily_weather_rows_with_fair_probabilities"] = int(daily_rows_with_fair)
    daily_rows_with_quote_and_fair = payload.get("daily_weather_rows_with_both_quote_and_fair_value")
    if isinstance(daily_rows_with_quote_and_fair, (int, float)):
        diagnostics["daily_weather_rows_with_both_quote_and_fair_value"] = int(daily_rows_with_quote_and_fair)
    daily_quote_orderability_raw = payload.get("daily_weather_quote_orderability_counts")
    if isinstance(daily_quote_orderability_raw, dict):
        normalized_daily_quote_orderability: dict[str, int] = {}
        for raw_reason, raw_count in daily_quote_orderability_raw.items():
            reason_text = str(raw_reason or "").strip()
            if not reason_text:
                continue
            count_value: int | None = None
            if isinstance(raw_count, bool):
                count_value = int(raw_count)
            elif isinstance(raw_count, (int, float)):
                count_value = int(raw_count)
            else:
                parsed_count = _parse_float(raw_count)
                if isinstance(parsed_count, float):
                    count_value = int(parsed_count)
            if count_value is None:
                continue
            normalized_daily_quote_orderability[reason_text] = max(0, count_value)
        diagnostics["daily_weather_quote_orderability_counts"] = normalized_daily_quote_orderability
    daily_quote_age_rows = payload.get("daily_weather_quote_age_rows_with_timestamp")
    if isinstance(daily_quote_age_rows, (int, float)):
        diagnostics["daily_weather_quote_age_rows_with_timestamp"] = int(daily_quote_age_rows)
    daily_quote_stale_max_age = payload.get("daily_weather_quote_stale_max_age_seconds")
    if isinstance(daily_quote_stale_max_age, (int, float)):
        diagnostics["daily_weather_quote_stale_max_age_seconds"] = round(float(daily_quote_stale_max_age), 3)
    daily_shadow_rows_total = payload.get("daily_weather_shadow_taker_rows_total")
    if isinstance(daily_shadow_rows_total, (int, float)):
        diagnostics["daily_weather_shadow_taker_rows_total"] = int(daily_shadow_rows_total)
    daily_shadow_rows_yes_ask = payload.get("daily_weather_shadow_taker_rows_with_orderable_yes_ask")
    if isinstance(daily_shadow_rows_yes_ask, (int, float)):
        diagnostics["daily_weather_shadow_taker_rows_with_orderable_yes_ask"] = int(daily_shadow_rows_yes_ask)
    daily_shadow_rows_no_ask = payload.get("daily_weather_shadow_taker_rows_with_orderable_no_ask")
    if isinstance(daily_shadow_rows_no_ask, (int, float)):
        diagnostics["daily_weather_shadow_taker_rows_with_orderable_no_ask"] = int(daily_shadow_rows_no_ask)
    daily_shadow_rows_any_ask = payload.get("daily_weather_shadow_taker_rows_with_any_orderable_ask")
    if isinstance(daily_shadow_rows_any_ask, (int, float)):
        diagnostics["daily_weather_shadow_taker_rows_with_any_orderable_ask"] = int(daily_shadow_rows_any_ask)
    daily_shadow_edge_count = payload.get("daily_weather_shadow_taker_edge_above_min_count")
    if isinstance(daily_shadow_edge_count, (int, float)):
        diagnostics["daily_weather_shadow_taker_edge_above_min_count"] = int(daily_shadow_edge_count)
    daily_shadow_edge_net_count = payload.get("daily_weather_shadow_taker_edge_net_fees_above_min_count")
    if isinstance(daily_shadow_edge_net_count, (int, float)):
        diagnostics["daily_weather_shadow_taker_edge_net_fees_above_min_count"] = int(daily_shadow_edge_net_count)
    daily_shadow_endpoint_rows = payload.get("daily_weather_shadow_taker_endpoint_orderbook_rows")
    if isinstance(daily_shadow_endpoint_rows, (int, float)):
        diagnostics["daily_weather_shadow_taker_endpoint_orderbook_rows"] = int(daily_shadow_endpoint_rows)
    daily_shadow_best_candidate = payload.get("daily_weather_best_shadow_taker_candidate")
    if isinstance(daily_shadow_best_candidate, dict):
        diagnostics["daily_weather_best_shadow_taker_candidate"] = daily_shadow_best_candidate
    daily_allowed_conservative_rows = payload.get("daily_weather_allowed_universe_rows_with_conservative_candidate")
    if isinstance(daily_allowed_conservative_rows, (int, float)):
        diagnostics["daily_weather_allowed_universe_rows_with_conservative_candidate"] = int(
            daily_allowed_conservative_rows
        )
    daily_endpoint_filtered = payload.get("daily_weather_endpoint_orderbook_filtered")
    if isinstance(daily_endpoint_filtered, (int, float)):
        diagnostics["daily_weather_endpoint_orderbook_filtered"] = int(daily_endpoint_filtered)
    daily_failure_counts_raw = payload.get("daily_weather_conservative_candidate_failure_counts")
    if isinstance(daily_failure_counts_raw, dict):
        normalized_daily_failure_counts: dict[str, int] = {}
        for raw_reason, raw_count in daily_failure_counts_raw.items():
            reason_text = str(raw_reason or "").strip()
            if not reason_text:
                continue
            count_value: int | None = None
            if isinstance(raw_count, bool):
                count_value = int(raw_count)
            elif isinstance(raw_count, (int, float)):
                count_value = int(raw_count)
            else:
                parsed_count = _parse_float(raw_count)
                if isinstance(parsed_count, float):
                    count_value = int(parsed_count)
            if count_value is None:
                continue
            normalized_daily_failure_counts[reason_text] = max(0, count_value)
        diagnostics["daily_weather_conservative_candidate_failure_counts"] = normalized_daily_failure_counts
    daily_planned_orders = payload.get("daily_weather_planned_orders")
    if isinstance(daily_planned_orders, (int, float)):
        diagnostics["daily_weather_planned_orders"] = int(daily_planned_orders)
    return diagnostics


def _run_step(
    *,
    name: str,
    launcher: list[str],
    args: list[str],
    cwd: Path,
    run_dir: Path,
    env_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    started_at = _now_iso()
    started_monotonic = time.monotonic()
    stdout_file = run_dir / f"{name}.stdout.json"
    stderr_file = run_dir / f"{name}.stderr.log"

    cmd = launcher + args
    merged_env = os.environ.copy()
    if isinstance(env_overrides, dict):
        for key, value in env_overrides.items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            merged_env[key_text] = str(value or "")

    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
        env=merged_env,
    )

    stdout_file.write_text(proc.stdout or "", encoding="utf-8")
    stderr_file.write_text(proc.stderr or "", encoding="utf-8")

    parsed: dict[str, Any] | None = None
    parse_error = None
    if (proc.stdout or "").strip():
        try:
            payload = json.loads(proc.stdout)
            if isinstance(payload, dict):
                parsed = payload
            else:
                parse_error = "stdout_json_not_object"
        except json.JSONDecodeError as exc:
            parse_error = f"stdout_json_decode_error:{exc}"

    finished_at = _now_iso()
    duration_seconds = round(time.monotonic() - started_monotonic, 3)

    step = {
        "name": name,
        "command": cmd,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration_seconds,
        "exit_code": int(proc.returncode),
        "stdout_json_file": str(stdout_file),
        "stderr_log_file": str(stderr_file),
        "stdout_json_parse_error": parse_error,
        "status": (parsed or {}).get("status") if isinstance(parsed, dict) else None,
        "output_file": (parsed or {}).get("output_file") if isinstance(parsed, dict) else None,
        "reason": (parsed or {}).get("reason") if isinstance(parsed, dict) else None,
        "ok": proc.returncode == 0,
    }

    if isinstance(parsed, dict):
        if name == "micro_status":
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["top_market_edge_net_fees"] = parsed.get("top_market_edge_net_fees")
            step["status_reason"] = parsed.get("status_reason")
            step["actual_live_balance_dollars"] = parsed.get("actual_live_balance_dollars")
            step["actual_live_balance_source"] = parsed.get("actual_live_balance_source")
            step["balance_live_verified"] = parsed.get("balance_live_verified")
            step["balance_cache_age_seconds"] = parsed.get("balance_cache_age_seconds")
            step["balance_cache_file"] = parsed.get("balance_cache_file")
            step["balance_check_error"] = parsed.get("balance_check_error")
        elif name.startswith("capture"):
            step["scan_status"] = parsed.get("scan_status")
            step["rows_appended"] = parsed.get("rows_appended")
            step["scan_summary_file"] = parsed.get("scan_summary_file")
        elif name == "weather_prewarm":
            step["ready_station_day_keys"] = parsed.get("ready_station_day_keys")
            step["refreshed_station_day_keys"] = parsed.get("refreshed_station_day_keys")
            step["failed_station_day_keys"] = parsed.get("failed_station_day_keys")
            step["status_counts"] = parsed.get("status_counts")
            step["station_history_cache_dir"] = parsed.get("station_history_cache_dir")
        elif name == "weather_prior_refresh":
            step["generated_priors"] = parsed.get("generated_priors")
            step["candidate_markets"] = parsed.get("candidate_markets")
            step["inserted_rows"] = parsed.get("inserted_rows")
            step["updated_rows"] = parsed.get("updated_rows")
            step["manual_rows_protected"] = parsed.get("manual_rows_protected")
            step["contract_family_generated_counts"] = parsed.get("contract_family_generated_counts")
            step["station_history_status_counts"] = parsed.get("station_history_status_counts")
            step["station_normals_cache_entries"] = parsed.get("station_normals_cache_entries")
            step["mrms_snapshot_status"] = parsed.get("mrms_snapshot_status")
            step["mrms_snapshot_age_seconds"] = parsed.get("mrms_snapshot_age_seconds")
            step["nbm_snapshot_status"] = parsed.get("nbm_snapshot_status")
            step["nbm_snapshot_cycle_age_seconds"] = parsed.get("nbm_snapshot_cycle_age_seconds")
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["top_market_confidence"] = parsed.get("top_market_confidence")
        elif name == "prior_plan_wakeup_reprioritize":
            step["planned_orders"] = parsed.get("planned_orders")
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["daily_weather_candidate_pool_size"] = parsed.get("daily_weather_candidate_pool_size")
            step["daily_weather_rows_with_conservative_candidate"] = parsed.get(
                "daily_weather_rows_with_conservative_candidate"
            )
            step["daily_weather_planned_orders"] = parsed.get("daily_weather_planned_orders")
            top_plans = parsed.get("top_plans")
            if isinstance(top_plans, list):
                step["top_plans_count"] = len(top_plans)
                step["top_plans_tickers"] = [
                    str(item.get("market_ticker") or "").strip().upper()
                    for item in top_plans
                    if isinstance(item, dict) and str(item.get("market_ticker") or "").strip()
                ][:20]
        elif name == "balance_smoke":
            checks = parsed.get("checks")
            step["checks_total"] = parsed.get("checks_total")
            step["checks_failed"] = parsed.get("checks_failed")
            step["smoke_status"] = parsed.get("status")
            step["kalshi_ok"] = None
            step["kalshi_message"] = None
            step["kalshi_http_status"] = None
            step["kalshi_failure_kind"] = None
            if isinstance(checks, list):
                kalshi_check = next(
                    (
                        item
                        for item in checks
                        if isinstance(item, dict) and str(item.get("component") or "").strip().lower() == "kalshi"
                    ),
                    None,
                )
                if isinstance(kalshi_check, dict):
                    step["kalshi_ok"] = bool(kalshi_check.get("ok"))
                    step["kalshi_message"] = kalshi_check.get("message")
                    step["kalshi_http_status"] = kalshi_check.get("http_status")
                    step["kalshi_failure_kind"] = _classify_balance_smoke_failure(
                        message=kalshi_check.get("message"),
                        http_status=kalshi_check.get("http_status"),
                    )
        elif name == "execution_frontier_refresh":
            trusted = parsed.get("trusted_break_even_edge_by_bucket")
            trust_map = parsed.get("bucket_markout_trust_by_bucket")
            trusted_bucket_count = len(trusted) if isinstance(trusted, dict) else 0
            untrusted_bucket_count = 0
            if isinstance(trust_map, dict):
                untrusted_bucket_count = sum(1 for item in trust_map.values() if not bool((item or {}).get("trusted")))
            step["submitted_orders"] = parsed.get("submitted_orders")
            step["filled_orders"] = parsed.get("filled_orders")
            step["fill_samples_with_markout"] = parsed.get("fill_samples_with_markout")
            step["trusted_bucket_count"] = trusted_bucket_count
            step["untrusted_bucket_count"] = untrusted_bucket_count
            step["bucket_markout_sample_counts_by_horizon"] = parsed.get("bucket_markout_sample_counts_by_horizon")
            step["recommendations"] = parsed.get("recommendations")
            step["frontier_artifact_path"] = parsed.get("frontier_artifact_path")
            step["frontier_artifact_sha256"] = parsed.get("frontier_artifact_sha256")
            step["frontier_artifact_file_sha256"] = parsed.get("frontier_artifact_file_sha256")
            step["frontier_artifact_payload_sha256"] = parsed.get("frontier_artifact_payload_sha256")
            step["frontier_artifact_as_of_utc"] = parsed.get("frontier_artifact_as_of_utc")
            step["frontier_artifact_age_seconds"] = parsed.get("frontier_artifact_age_seconds")
            step["frontier_selection_mode"] = parsed.get("frontier_selection_mode")
        elif name == "climate_realtime_router":
            step["ws_collect_status"] = parsed.get("ws_collect_status")
            step["ws_channels"] = parsed.get("ws_channels")
            step["ws_events_logged"] = parsed.get("ws_events_logged")
            step["orderbook_events_processed"] = parsed.get("orderbook_events_processed")
            step["ticker_events_processed"] = parsed.get("ticker_events_processed")
            step["lifecycle_events_processed"] = parsed.get("lifecycle_events_processed")
            step["public_trade_events_processed"] = parsed.get("public_trade_events_processed")
            step["wakeup_transitions_processed"] = parsed.get("wakeup_transitions_processed")
            step["availability_observations_written"] = parsed.get("availability_observations_written")
            step["availability_ticker_states_updated"] = parsed.get("availability_ticker_states_updated")
            step["availability_db_path"] = parsed.get("availability_db_path")
            step["output_csv"] = parsed.get("output_csv")
            step["climate_rows_total"] = parsed.get("climate_rows_total")
            step["climate_family_counts"] = parsed.get("climate_family_counts")
            step["climate_availability_state_counts"] = parsed.get("climate_availability_state_counts")
            step["climate_opportunity_class_counts"] = parsed.get("climate_opportunity_class_counts")
            step["climate_theoretical_positive_rows"] = parsed.get("climate_theoretical_positive_rows")
            step["climate_priced_watch_only_rows"] = parsed.get("climate_priced_watch_only_rows")
            step["climate_unpriced_model_view_rows"] = parsed.get("climate_unpriced_model_view_rows")
            step["climate_tradable_rows"] = parsed.get("climate_tradable_rows")
            step["climate_hot_rows"] = parsed.get("climate_hot_rows")
            step["climate_dead_rows"] = parsed.get("climate_dead_rows")
            step["climate_tradable_positive_rows"] = parsed.get("climate_tradable_positive_rows")
            step["climate_hot_positive_rows"] = parsed.get("climate_hot_positive_rows")
            step["climate_negative_or_neutral_rows"] = parsed.get("climate_negative_or_neutral_rows")
            step["top_theoretical_candidates"] = parsed.get("top_theoretical_candidates")
            step["top_tradable_candidates"] = parsed.get("top_tradable_candidates")
            step["top_watch_only_candidates"] = parsed.get("top_watch_only_candidates")
            step["top_waking_strips"] = parsed.get("top_waking_strips")
            step["strip_summaries_count"] = parsed.get("strip_summaries_count")
            step["routing_allocator_eligible_rows"] = parsed.get("routing_allocator_eligible_rows")
            step["routing_allocator_allocated_rows"] = parsed.get("routing_allocator_allocated_rows")
            step["routing_allocator_total_risk_dollars"] = parsed.get("routing_allocator_total_risk_dollars")
            step["routing_allocator_total_expected_value_dollars"] = parsed.get(
                "routing_allocator_total_expected_value_dollars"
            )
            step["routing_allocator_allocations"] = parsed.get("routing_allocator_allocations")
            step["family_routed_capital_budget"] = parsed.get("family_routed_capital_budget")
            step["market_tickers_selected_count"] = parsed.get("market_tickers_selected_count")
            step["market_tickers_selected"] = parsed.get("market_tickers_selected")
            step["seed_recent_markets"] = parsed.get("seed_recent_markets")
            step["recent_markets_min_updated_seconds"] = parsed.get("recent_markets_min_updated_seconds")
            step["recent_market_discovery_status"] = parsed.get("recent_market_discovery_status")
            step["recent_market_discovery_reason"] = parsed.get("recent_market_discovery_reason")
            step["recent_market_discovery_api_root"] = parsed.get("recent_market_discovery_api_root")
            step["recent_market_discovery_http_status"] = parsed.get("recent_market_discovery_http_status")
            step["recent_market_discovery_min_updated_ts_ms"] = parsed.get(
                "recent_market_discovery_min_updated_ts_ms"
            )
            step["recent_market_discovery_tickers"] = parsed.get("recent_market_discovery_tickers")
            step["recent_market_discovery_tickers_count"] = parsed.get(
                "recent_market_discovery_tickers_count"
            )
            step["recent_market_discovery_errors"] = parsed.get("recent_market_discovery_errors")
        elif name.startswith("prior_trader_dry_run"):
            step["allow_live_orders_effective"] = parsed.get("allow_live_orders_effective")
            step["prior_execute_status"] = parsed.get("prior_execute_status")
            step["execution_frontier_status"] = parsed.get("execution_frontier_status")
            step["execution_frontier_report_reference_file"] = (
                parsed.get("execution_frontier_report_reference_file")
                or parsed.get("execution_frontier_break_even_reference_file")
            )
            step["execution_frontier_report_selection_mode"] = (
                parsed.get("execution_frontier_report_selection_mode")
                or parsed.get("execution_frontier_selection_mode")
            )
            step["execution_frontier_report_stale"] = parsed.get("execution_frontier_report_stale")
            step["execution_frontier_report_stale_reason"] = parsed.get("execution_frontier_report_stale_reason")
            step["capture_status"] = parsed.get("capture_status")
            step["prior_trade_gate_status"] = parsed.get("prior_trade_gate_status")
            step["prior_trade_gate_blockers"] = parsed.get("prior_trade_gate_blockers")
            step["prior_plan_summary_file"] = parsed.get("prior_plan_summary_file")
            step["top_market_ticker"] = parsed.get("top_market_ticker")
            step["top_market_contract_family"] = parsed.get("top_market_contract_family")
            step["top_market_weather_history_status"] = parsed.get("top_market_weather_history_status")
            step["top_market_weather_history_live_ready"] = parsed.get("top_market_weather_history_live_ready")
            step["top_market_weather_history_live_ready_reason"] = parsed.get("top_market_weather_history_live_ready_reason")
            step["daily_weather_board_fresh"] = parsed.get("daily_weather_board_fresh")
            step["daily_weather_board_age_seconds"] = parsed.get("daily_weather_board_age_seconds")
            step["daily_weather_markets"] = parsed.get("daily_weather_markets")
            step["daily_weather_family_counts"] = parsed.get("daily_weather_family_counts")
            step["daily_weather_markets_with_fresh_snapshot"] = parsed.get("daily_weather_markets_with_fresh_snapshot")
            step["daily_weather_board_freshness_threshold_seconds"] = parsed.get(
                "daily_weather_board_freshness_threshold_seconds"
            )
            step["rain_model_tag"] = parsed.get("rain_model_tag")
            step["temperature_model_tag"] = parsed.get("temperature_model_tag")
            step["weather_priors_version"] = parsed.get("weather_priors_version")
            step["fill_model_mode"] = parsed.get("fill_model_mode")
            step["execution_empirical_fill_model_prefer_empirical"] = parsed.get(
                "execution_empirical_fill_model_prefer_empirical"
            )
            step["history_csv_path"] = parsed.get("history_csv_path")
            step["history_csv_mtime_utc"] = parsed.get("history_csv_mtime_utc")
            step["weather_station_history_cache_age_seconds"] = parsed.get("weather_station_history_cache_age_seconds")
            step["balance_heartbeat_age_seconds"] = parsed.get("balance_heartbeat_age_seconds")
            step["fair_yes_probability_raw"] = (
                parsed.get("fair_yes_probability_raw")
                or parsed.get("top_market_fair_probability_raw")
            )
            step["execution_probability_guarded"] = (
                parsed.get("execution_probability_guarded")
                or parsed.get("top_market_execution_probability_guarded")
            )
            step["fill_probability_source"] = parsed.get("fill_probability_source")
            step["empirical_fill_weight"] = parsed.get("empirical_fill_weight")
            step["heuristic_fill_weight"] = parsed.get("heuristic_fill_weight")
            step["probe_lane_used"] = parsed.get("probe_lane_used")
            step["probe_reason"] = parsed.get("probe_reason")
            step["climate_router_pilot_enabled"] = parsed.get("climate_router_pilot_enabled")
            step["climate_router_pilot_status"] = parsed.get("climate_router_pilot_status")
            step["climate_router_pilot_reason"] = parsed.get("climate_router_pilot_reason")
            step["climate_router_pilot_summary_file"] = parsed.get("climate_router_pilot_summary_file")
            step["climate_router_pilot_selection_mode"] = parsed.get("climate_router_pilot_selection_mode")
            step["climate_router_pilot_summary_status"] = parsed.get("climate_router_pilot_summary_status")
            step["climate_router_pilot_allowed_classes"] = parsed.get("climate_router_pilot_allowed_classes")
            step["climate_router_pilot_allowed_families"] = parsed.get("climate_router_pilot_allowed_families")
            step["climate_router_pilot_excluded_families"] = parsed.get("climate_router_pilot_excluded_families")
            step["climate_router_pilot_allowed_families_effective"] = parsed.get(
                "climate_router_pilot_allowed_families_effective"
            )
            step["climate_router_pilot_excluded_families_effective"] = parsed.get(
                "climate_router_pilot_excluded_families_effective"
            )
            step["climate_router_pilot_max_orders_per_run"] = parsed.get("climate_router_pilot_max_orders_per_run")
            step["climate_router_pilot_contracts_cap"] = parsed.get("climate_router_pilot_contracts_cap")
            step["climate_router_pilot_required_ev_dollars"] = parsed.get("climate_router_pilot_required_ev_dollars")
            step["climate_router_pilot_policy_scope_override_enabled"] = parsed.get(
                "climate_router_pilot_policy_scope_override_enabled"
            )
            step["climate_router_pilot_policy_scope_override_active"] = parsed.get(
                "climate_router_pilot_policy_scope_override_active"
            )
            step["climate_router_pilot_policy_scope_override_status"] = parsed.get(
                "climate_router_pilot_policy_scope_override_status"
            )
            step["climate_router_pilot_policy_scope_override_gate_active"] = parsed.get(
                "climate_router_pilot_policy_scope_override_gate_active"
            )
            step["climate_router_pilot_policy_scope_override_applicable"] = parsed.get(
                "climate_router_pilot_policy_scope_override_applicable"
            )
            step["climate_router_pilot_policy_scope_override_attempts"] = parsed.get(
                "climate_router_pilot_policy_scope_override_attempts"
            )
            step["climate_router_pilot_policy_scope_override_submissions"] = parsed.get(
                "climate_router_pilot_policy_scope_override_submissions"
            )
            step["climate_router_pilot_policy_scope_override_blocked_reason_counts"] = parsed.get(
                "climate_router_pilot_policy_scope_override_blocked_reason_counts"
            )
            step["climate_router_pilot_considered_rows"] = parsed.get("climate_router_pilot_considered_rows")
            step["climate_router_pilot_promoted_rows"] = parsed.get("climate_router_pilot_promoted_rows")
            step["climate_router_pilot_submitted_rows"] = parsed.get("climate_router_pilot_submitted_rows")
            step["climate_router_pilot_expected_value_dollars"] = parsed.get(
                "climate_router_pilot_expected_value_dollars"
            )
            step["climate_router_pilot_blocked_reason_counts"] = parsed.get(
                "climate_router_pilot_blocked_reason_counts"
            )
            step["climate_router_pilot_selected_tickers"] = parsed.get("climate_router_pilot_selected_tickers")
            step["climate_router_pilot_execute_considered_rows"] = parsed.get(
                "climate_router_pilot_execute_considered_rows"
            )
            step["climate_router_pilot_live_mode_enabled"] = parsed.get(
                "climate_router_pilot_live_mode_enabled"
            )
            step["climate_router_pilot_live_eligible_rows"] = parsed.get(
                "climate_router_pilot_live_eligible_rows"
            )
            step["climate_router_pilot_would_attempt_live_if_enabled"] = parsed.get(
                "climate_router_pilot_would_attempt_live_if_enabled"
            )
            step["climate_router_pilot_blocked_dry_run_only_rows"] = parsed.get(
                "climate_router_pilot_blocked_dry_run_only_rows"
            )
            step["climate_router_pilot_blocked_research_dry_run_only_reason_counts"] = parsed.get(
                "climate_router_pilot_blocked_research_dry_run_only_reason_counts"
            )
            step["climate_router_pilot_non_policy_gates_passed_rows"] = parsed.get(
                "climate_router_pilot_non_policy_gates_passed_rows"
            )
            step["climate_router_pilot_attempted_orders"] = parsed.get("climate_router_pilot_attempted_orders")
            step["climate_router_pilot_acked_orders"] = parsed.get("climate_router_pilot_acked_orders")
            step["climate_router_pilot_resting_orders"] = parsed.get("climate_router_pilot_resting_orders")
            step["climate_router_pilot_filled_orders"] = parsed.get("climate_router_pilot_filled_orders")
            step["climate_router_pilot_partial_fills"] = parsed.get("climate_router_pilot_partial_fills")
            step["climate_router_pilot_blocked_post_promotion_reason_counts"] = parsed.get(
                "climate_router_pilot_blocked_post_promotion_reason_counts"
            )
            step["climate_router_pilot_blocked_frontier_insufficient_data"] = parsed.get(
                "climate_router_pilot_blocked_frontier_insufficient_data"
            )
            step["climate_router_pilot_blocked_balance"] = parsed.get("climate_router_pilot_blocked_balance")
            step["climate_router_pilot_blocked_board_stale"] = parsed.get("climate_router_pilot_blocked_board_stale")
            step["climate_router_pilot_blocked_weather_history"] = parsed.get(
                "climate_router_pilot_blocked_weather_history"
            )
            step["climate_router_pilot_blocked_duplicate_ticker"] = parsed.get(
                "climate_router_pilot_blocked_duplicate_ticker"
            )
            step["climate_router_pilot_blocked_no_orderable_side_on_recheck"] = parsed.get(
                "climate_router_pilot_blocked_no_orderable_side_on_recheck"
            )
            step["climate_router_pilot_blocked_ev_below_threshold"] = parsed.get(
                "climate_router_pilot_blocked_ev_below_threshold"
            )
            step["climate_router_pilot_blocked_research_dry_run_only"] = parsed.get(
                "climate_router_pilot_blocked_research_dry_run_only"
            )
            step["climate_router_pilot_blocked_live_disabled"] = parsed.get(
                "climate_router_pilot_blocked_live_disabled"
            )
            step["climate_router_pilot_blocked_policy_scope"] = parsed.get(
                "climate_router_pilot_blocked_policy_scope"
            )
            step["climate_router_pilot_blocked_family_filter"] = parsed.get(
                "climate_router_pilot_blocked_family_filter"
            )
            step["climate_router_pilot_blocked_contract_cap"] = parsed.get(
                "climate_router_pilot_blocked_contract_cap"
            )
            step["climate_router_pilot_frontier_bootstrap_submitted_attempts"] = parsed.get(
                "climate_router_pilot_frontier_bootstrap_submitted_attempts"
            )
            step["climate_router_pilot_frontier_bootstrap_blocked_attempts"] = parsed.get(
                "climate_router_pilot_frontier_bootstrap_blocked_attempts"
            )
            step["climate_router_pilot_markout_10s_dollars"] = parsed.get("climate_router_pilot_markout_10s_dollars")
            step["climate_router_pilot_markout_60s_dollars"] = parsed.get("climate_router_pilot_markout_60s_dollars")
            step["climate_router_pilot_markout_300s_dollars"] = parsed.get("climate_router_pilot_markout_300s_dollars")
            step["climate_router_pilot_realized_pnl_dollars"] = parsed.get("climate_router_pilot_realized_pnl_dollars")
            step["climate_router_pilot_expected_vs_realized_delta"] = parsed.get(
                "climate_router_pilot_expected_vs_realized_delta"
            )
            step["enable_untrusted_bucket_probe_exploration"] = parsed.get(
                "enable_untrusted_bucket_probe_exploration"
            )
            step["untrusted_bucket_probe_exploration_enabled"] = parsed.get(
                "untrusted_bucket_probe_exploration_enabled"
            )
            step["untrusted_bucket_probe_max_orders_per_run"] = parsed.get(
                "untrusted_bucket_probe_max_orders_per_run"
            )
            step["untrusted_bucket_probe_required_edge_buffer_dollars"] = parsed.get(
                "untrusted_bucket_probe_required_edge_buffer_dollars"
            )
            step["untrusted_bucket_probe_contracts_cap"] = parsed.get(
                "untrusted_bucket_probe_contracts_cap"
            )
            step["untrusted_bucket_probe_submitted_attempts"] = parsed.get(
                "untrusted_bucket_probe_submitted_attempts"
            )
            step["untrusted_bucket_probe_blocked_attempts"] = parsed.get(
                "untrusted_bucket_probe_blocked_attempts"
            )
            step["untrusted_bucket_probe_reason_counts"] = parsed.get(
                "untrusted_bucket_probe_reason_counts"
            )
            step["frontier_artifact_path"] = parsed.get("frontier_artifact_path")
            step["frontier_artifact_sha256"] = parsed.get("frontier_artifact_sha256")
            step["frontier_artifact_file_sha256"] = parsed.get("frontier_artifact_file_sha256")
            step["frontier_artifact_payload_sha256"] = parsed.get("frontier_artifact_payload_sha256")
            step["frontier_artifact_as_of_utc"] = parsed.get("frontier_artifact_as_of_utc")
            step["frontier_artifact_age_seconds"] = parsed.get("frontier_artifact_age_seconds")
            skip_diagnostics = _plan_skip_diagnostics_from_summary(
                plan_summary_file=parsed.get("prior_plan_summary_file")
            )
            step["plan_summary_status"] = skip_diagnostics.get("plan_summary_status")
            step["plan_summary_error"] = skip_diagnostics.get("plan_summary_error")
            step["plan_summary_planned_orders"] = skip_diagnostics.get("plan_summary_planned_orders")
            step["plan_skip_counts_total"] = skip_diagnostics.get("plan_skip_counts_total")
            step["plan_skip_counts_nonzero_total"] = skip_diagnostics.get("plan_skip_counts_nonzero_total")
            step["plan_skip_counts_top"] = skip_diagnostics.get("plan_skip_counts_top")
            step["plan_skip_reason_dominant"] = skip_diagnostics.get("plan_skip_reason_dominant")
            step["plan_skip_reason_dominant_count"] = skip_diagnostics.get("plan_skip_reason_dominant_count")
            step["plan_skip_reason_dominant_share"] = skip_diagnostics.get("plan_skip_reason_dominant_share")
            step["allowed_universe_candidate_pool_size"] = skip_diagnostics.get("allowed_universe_candidate_pool_size")
            step["allowed_universe_skip_counts_total"] = skip_diagnostics.get("allowed_universe_skip_counts_total")
            step["allowed_universe_skip_counts_nonzero_total"] = skip_diagnostics.get(
                "allowed_universe_skip_counts_nonzero_total"
            )
            step["allowed_universe_skip_counts_top"] = skip_diagnostics.get("allowed_universe_skip_counts_top")
            step["allowed_universe_skip_reason_dominant"] = skip_diagnostics.get(
                "allowed_universe_skip_reason_dominant"
            )
            step["allowed_universe_skip_reason_dominant_count"] = skip_diagnostics.get(
                "allowed_universe_skip_reason_dominant_count"
            )
            step["allowed_universe_skip_reason_dominant_share"] = skip_diagnostics.get(
                "allowed_universe_skip_reason_dominant_share"
            )
            step["daily_weather_candidate_pool_size"] = skip_diagnostics.get("daily_weather_candidate_pool_size")
            step["daily_weather_rows_total"] = skip_diagnostics.get("daily_weather_rows_total")
            step["daily_weather_skip_counts_total"] = skip_diagnostics.get("daily_weather_skip_counts_total")
            step["daily_weather_skip_counts_nonzero_total"] = skip_diagnostics.get(
                "daily_weather_skip_counts_nonzero_total"
            )
            step["daily_weather_skip_counts_top"] = skip_diagnostics.get("daily_weather_skip_counts_top")
            step["daily_weather_skip_reason_dominant"] = skip_diagnostics.get(
                "daily_weather_skip_reason_dominant"
            )
            step["daily_weather_skip_reason_dominant_count"] = skip_diagnostics.get(
                "daily_weather_skip_reason_dominant_count"
            )
            step["daily_weather_skip_reason_dominant_share"] = skip_diagnostics.get(
                "daily_weather_skip_reason_dominant_share"
            )
            step["daily_weather_rows_with_conservative_candidate"] = skip_diagnostics.get(
                "daily_weather_rows_with_conservative_candidate"
            )
            step["daily_weather_rows_with_both_sides_candidate"] = skip_diagnostics.get(
                "daily_weather_rows_with_both_sides_candidate"
            )
            step["daily_weather_rows_with_one_side_failed"] = skip_diagnostics.get(
                "daily_weather_rows_with_one_side_failed"
            )
            step["daily_weather_rows_with_both_sides_failed"] = skip_diagnostics.get(
                "daily_weather_rows_with_both_sides_failed"
            )
            step["daily_weather_orderable_bid_rows"] = skip_diagnostics.get("daily_weather_orderable_bid_rows")
            step["daily_weather_rows_with_any_orderable_bid"] = skip_diagnostics.get(
                "daily_weather_rows_with_any_orderable_bid"
            )
            step["daily_weather_rows_with_any_orderable_ask"] = skip_diagnostics.get(
                "daily_weather_rows_with_any_orderable_ask"
            )
            step["daily_weather_rows_with_fair_probabilities"] = skip_diagnostics.get(
                "daily_weather_rows_with_fair_probabilities"
            )
            step["daily_weather_rows_with_both_quote_and_fair_value"] = skip_diagnostics.get(
                "daily_weather_rows_with_both_quote_and_fair_value"
            )
            step["daily_weather_quote_orderability_counts"] = skip_diagnostics.get(
                "daily_weather_quote_orderability_counts"
            )
            step["daily_weather_quote_age_rows_with_timestamp"] = skip_diagnostics.get(
                "daily_weather_quote_age_rows_with_timestamp"
            )
            step["daily_weather_quote_stale_max_age_seconds"] = skip_diagnostics.get(
                "daily_weather_quote_stale_max_age_seconds"
            )
            step["daily_weather_shadow_taker_rows_total"] = skip_diagnostics.get("daily_weather_shadow_taker_rows_total")
            step["daily_weather_shadow_taker_rows_with_orderable_yes_ask"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_rows_with_orderable_yes_ask"
            )
            step["daily_weather_shadow_taker_rows_with_orderable_no_ask"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_rows_with_orderable_no_ask"
            )
            step["daily_weather_shadow_taker_rows_with_any_orderable_ask"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_rows_with_any_orderable_ask"
            )
            step["daily_weather_shadow_taker_edge_above_min_count"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_edge_above_min_count"
            )
            step["daily_weather_shadow_taker_edge_net_fees_above_min_count"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_edge_net_fees_above_min_count"
            )
            step["daily_weather_shadow_taker_endpoint_orderbook_rows"] = skip_diagnostics.get(
                "daily_weather_shadow_taker_endpoint_orderbook_rows"
            )
            step["daily_weather_best_shadow_taker_candidate"] = skip_diagnostics.get(
                "daily_weather_best_shadow_taker_candidate"
            )
            step["daily_weather_allowed_universe_rows_with_conservative_candidate"] = skip_diagnostics.get(
                "daily_weather_allowed_universe_rows_with_conservative_candidate"
            )
            step["daily_weather_endpoint_orderbook_filtered"] = skip_diagnostics.get(
                "daily_weather_endpoint_orderbook_filtered"
            )
            step["daily_weather_conservative_candidate_failure_counts"] = skip_diagnostics.get(
                "daily_weather_conservative_candidate_failure_counts"
            )
            step["daily_weather_planned_orders"] = skip_diagnostics.get("daily_weather_planned_orders")

    return step


def _run_preflight(
    *,
    launcher: list[str],
    cwd: Path,
    run_dir: Path,
) -> dict[str, Any]:
    started_at = _now_iso()
    started_monotonic = time.monotonic()
    stdout_file = run_dir / "preflight.stdout.json"
    stderr_file = run_dir / "preflight.stderr.log"
    stderr_lines: list[str] = []
    checks: list[dict[str, Any]] = []

    required_commands = [
        "kalshi-micro-status",
        "kalshi-nonsports-capture",
        "live-smoke",
        "kalshi-weather-priors",
        "kalshi-weather-prewarm",
        "kalshi-climate-realtime-router",
        "kalshi-execution-frontier",
        "kalshi-micro-prior-trader",
    ]
    help_proc = subprocess.run(
        launcher + ["--help"],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    help_text = f"{help_proc.stdout}\n{help_proc.stderr}".lower()
    missing_commands = [command for command in required_commands if command.lower() not in help_text]
    command_ok = help_proc.returncode == 0 and not missing_commands
    checks.append(
        {
            "name": "cli_commands",
            "ok": command_ok,
            "missing_commands": missing_commands,
            "return_code": int(help_proc.returncode),
        }
    )
    if not command_ok:
        stderr_lines.append(
            "cli_commands failed: "
            + (
                f"missing={','.join(missing_commands)}"
                if missing_commands
                else f"help_return_code={help_proc.returncode}"
            )
        )

    python_exe = launcher[0]
    import_proc = subprocess.run(
        [
            python_exe,
            "-c",
            (
                "import betbot.kalshi_micro_execute; "
                "import betbot.kalshi_nonsports_priors; "
                "import betbot.kalshi_weather_priors"
            ),
        ],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    import_ok = import_proc.returncode == 0
    checks.append({"name": "module_imports", "ok": import_ok, "return_code": int(import_proc.returncode)})
    if not import_ok:
        stderr_lines.append(f"module_imports failed: {import_proc.stderr.strip() or import_proc.stdout.strip()}")

    schema_proc = subprocess.run(
        [
            python_exe,
            "-c",
            (
                "from pathlib import Path\n"
                "import tempfile\n"
                "from betbot.kalshi_nonsports_priors import _write_prior_csv\n"
                "tmp = Path(tempfile.mkdtemp()) / 'preflight_priors.csv'\n"
                "_write_prior_csv(tmp, [{'market_ticker': 'PREFLIGHT-TEST', 'contract_family': 'daily_rain'}])\n"
                "assert tmp.exists()\n"
            ),
        ],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    schema_ok = schema_proc.returncode == 0
    checks.append({"name": "priors_csv_schema", "ok": schema_ok, "return_code": int(schema_proc.returncode)})
    if not schema_ok:
        stderr_lines.append(f"priors_csv_schema failed: {schema_proc.stderr.strip() or schema_proc.stdout.strip()}")

    ok = all(bool(check.get("ok")) for check in checks)
    status = "ready" if ok else "failed"

    stdout_file.write_text(json.dumps({"status": status, "checks": checks}, indent=2), encoding="utf-8")
    stderr_file.write_text("\n".join(stderr_lines), encoding="utf-8")

    finished_at = _now_iso()
    duration_seconds = round(time.monotonic() - started_monotonic, 3)
    return {
        "name": "preflight",
        "command": ["preflight:self_test"],
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration_seconds,
        "exit_code": 0 if ok else 1,
        "stdout_json_file": str(stdout_file),
        "stderr_log_file": str(stderr_file),
        "stdout_json_parse_error": None,
        "status": status,
        "output_file": None,
        "ok": ok,
        "checks": checks,
    }


def _build_balance_heartbeat(
    *,
    micro_status_step: dict[str, Any] | None,
    output_dir: Path,
    max_age_seconds: float,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if isinstance(micro_status_step, dict):
        payload = dict(micro_status_step)
        output_file = Path(str(micro_status_step.get("output_file") or ""))
        if output_file.exists():
            summary_payload = _load_json(output_file)
            if isinstance(summary_payload, dict):
                payload = {**summary_payload, **payload}

    balance_dollars = _parse_float(payload.get("actual_live_balance_dollars"))
    source = str(payload.get("actual_live_balance_source") or "unknown").strip().lower() or "unknown"
    live_verified = bool(payload.get("balance_live_verified"))
    cache_age_seconds = _parse_float(payload.get("balance_cache_age_seconds"))
    check_error = str(payload.get("balance_check_error") or "").strip()
    cache_file = Path(str(payload.get("balance_cache_file") or output_dir / "kalshi_live_balance_cache.json"))
    cache_meta = _file_meta(cache_file)
    if cache_age_seconds is None:
        cache_age_seconds = _parse_float(cache_meta.get("age_seconds"))

    freshness_ok = False
    if source == "live":
        freshness_ok = live_verified and not check_error
    elif source in {"cache", "cached", "fallback"}:
        freshness_ok = (cache_age_seconds is not None and cache_age_seconds <= max_age_seconds) and not check_error

    live_ready = bool(
        isinstance(balance_dollars, float)
        and balance_dollars > 0.0
        and freshness_ok
    )

    blockers: list[str] = []
    if check_error:
        blockers.append(f"balance_error:{check_error}")
    if not isinstance(balance_dollars, float):
        blockers.append("balance_unavailable")
    elif balance_dollars <= 0.0:
        blockers.append("balance_nonpositive")
    if not freshness_ok:
        blockers.append("balance_stale_or_unverified")

    status = "ready" if live_ready else "unavailable"
    return _synthetic_step(
        name="balance_heartbeat",
        status=status,
        ok=True,
        payload={
            "balance_dollars": balance_dollars,
            "balance_source": source,
            "balance_live_verified": live_verified,
            "balance_cache_age_seconds": cache_age_seconds,
            "balance_freshness_threshold_seconds": max_age_seconds,
            "balance_cache_file": str(cache_file),
            "balance_cache_file_meta": cache_meta,
            "balance_check_error": check_error or None,
            "balance_live_ready": live_ready,
            "balance_blockers": blockers,
        },
    )


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = str(value).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _daily_weather_recovery_failure_kind(
    *,
    stale_recovery_triggered: bool,
    stale_recovery_resolved: bool,
    recovery_capture_step: dict[str, Any] | None,
    prior_trader_step: dict[str, Any] | None,
) -> str | None:
    if not stale_recovery_triggered or stale_recovery_resolved:
        return None
    if isinstance(recovery_capture_step, dict):
        status = str(recovery_capture_step.get("status") or "").strip().lower()
        if status and status not in {"ready", "ok"}:
            return f"capture_{status}"
        scan_status = str(recovery_capture_step.get("scan_status") or "").strip().lower()
        if scan_status and scan_status not in {"ready", "ok"}:
            return f"capture_scan_{scan_status}"
    if isinstance(prior_trader_step, dict):
        gate_status = str(prior_trader_step.get("prior_trade_gate_status") or "").strip().lower()
        if gate_status:
            return f"gate_{gate_status}"
    return "unknown"


def _daily_weather_recovery_alert_state(
    *,
    run_root: Path,
    run_id: str,
    run_started_at_utc: str,
    stale_recovery_triggered: bool,
    stale_recovery_resolved: bool,
    failure_kind: str | None,
    recovery_capture_status: str | None,
    recovery_capture_scan_status: str | None,
    window_hours: float,
    threshold: int,
    max_events: int,
) -> dict[str, Any]:
    now_dt = _parse_iso(run_started_at_utc) or datetime.now(timezone.utc)
    window_seconds = max(0.0, float(window_hours)) * 3600.0
    threshold_effective = max(1, int(threshold))
    max_events_effective = max(25, int(max_events))
    state_path = run_root / "daily_weather_recovery_health.json"
    existing_events: list[dict[str, Any]] = []
    parse_error: str | None = None
    write_error: str | None = None

    existing_payload = _load_json(state_path)
    if isinstance(existing_payload, dict):
        raw_events = existing_payload.get("events")
        if isinstance(raw_events, list):
            for item in raw_events:
                if isinstance(item, dict):
                    existing_events.append(dict(item))

    cutoff = now_dt.timestamp() - window_seconds
    pruned_events: list[dict[str, Any]] = []
    for event in existing_events:
        occurred_text = str(event.get("occurred_at_utc") or "").strip()
        occurred_dt = _parse_iso(occurred_text)
        if occurred_dt is None:
            continue
        if occurred_dt.timestamp() < cutoff:
            continue
        pruned_events.append(event)

    if stale_recovery_triggered and not stale_recovery_resolved:
        pruned_events.append(
            {
                "run_id": run_id,
                "occurred_at_utc": now_dt.isoformat(),
                "failure_kind": str(failure_kind or "unknown"),
                "capture_status": str(recovery_capture_status or "") or None,
                "capture_scan_status": str(recovery_capture_scan_status or "") or None,
            }
        )

    if len(pruned_events) > max_events_effective:
        pruned_events = pruned_events[-max_events_effective:]

    failure_counts_by_kind: dict[str, int] = {}
    for event in pruned_events:
        kind = str(event.get("failure_kind") or "unknown").strip().lower() or "unknown"
        failure_counts_by_kind[kind] = failure_counts_by_kind.get(kind, 0) + 1
    failure_count_window = len(pruned_events)
    alert_triggered = failure_count_window >= threshold_effective

    payload = {
        "updated_at_utc": now_dt.isoformat(),
        "window_hours": max(0.0, float(window_hours)),
        "threshold": threshold_effective,
        "max_events": max_events_effective,
        "events": pruned_events,
    }
    try:
        state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError as exc:
        write_error = str(exc)

    return {
        "state_file": str(state_path),
        "window_hours": max(0.0, float(window_hours)),
        "threshold": threshold_effective,
        "max_events": max_events_effective,
        "failure_count_window": failure_count_window,
        "failure_counts_by_kind": dict(sorted(failure_counts_by_kind.items())),
        "alert_triggered": alert_triggered,
        "last_failure_kind": str(failure_kind or "") or None,
        "parse_error": parse_error,
        "write_error": write_error,
    }


def _top_level_balance_heartbeat(step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {
            "status": "not_run",
            "live_ready": False,
            "source": "unknown",
            "balance_dollars": None,
            "cache_age_seconds": None,
            "freshness_threshold_seconds": None,
            "blockers": [],
            "check_error": None,
            "cache_file": None,
        }
    return {
        "status": str(step.get("status") or "").strip() or "unknown",
        "live_ready": bool(step.get("balance_live_ready")),
        "source": str(step.get("balance_source") or "unknown"),
        "balance_dollars": _parse_float(step.get("balance_dollars")),
        "cache_age_seconds": _parse_float(step.get("balance_cache_age_seconds")),
        "freshness_threshold_seconds": _parse_float(step.get("balance_freshness_threshold_seconds")),
        "blockers": list(step.get("balance_blockers") or []),
        "check_error": step.get("balance_check_error"),
        "cache_file": step.get("balance_cache_file"),
    }


def _top_level_execution_frontier(step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {
            "status": "not_run",
            "trusted_bucket_count": 0,
            "untrusted_bucket_count": 0,
            "submitted_orders": 0,
            "filled_orders": 0,
            "fill_samples_with_markout": 0,
            "bucket_markout_sample_counts_by_horizon": {},
            "output_file": None,
        }
    return {
        "status": str(step.get("status") or "").strip() or "unknown",
        "trusted_bucket_count": int(step.get("trusted_bucket_count") or 0),
        "untrusted_bucket_count": int(step.get("untrusted_bucket_count") or 0),
        "submitted_orders": int(step.get("submitted_orders") or 0),
        "filled_orders": int(step.get("filled_orders") or 0),
        "fill_samples_with_markout": int(step.get("fill_samples_with_markout") or 0),
        "bucket_markout_sample_counts_by_horizon": dict(step.get("bucket_markout_sample_counts_by_horizon") or {}),
        "output_file": step.get("output_file"),
    }


def _shadow_bankroll_defaults(
    *,
    enabled: bool,
    start_dollars: float,
    state_file: Path | None,
    status: str,
    reason: str | None,
) -> dict[str, Any]:
    start = round(max(0.0, float(start_dollars)), 4)
    state_file_text = str(state_file) if isinstance(state_file, Path) else ""
    return {
        "enabled": bool(enabled),
        "state_file": state_file_text or None,
        "status": str(status or "observer_not_run"),
        "reason": str(reason or "").strip() or None,
        "last_updated_at_utc": None,
        "state_write_error": None,
        "start_dollars": start,
        "theoretical_value_dollars": start,
        "realized_pnl_dollars": 0.0,
        "theoretical_unrealized_ev_dollars": 0.0,
        "theoretical_drawdown_pct": 0.0,
        "allocator_total_risk_dollars": 0.0,
        "allocator_selected_rows": 0,
        "expected_value_dollars": 0.0,
        "strategy_equity_dollars": start,
        "strategy_drawdown_pct": 0.0,
        "strategy_peak_equity_dollars": start,
        "mark_to_model_pnl_dollars": 0.0,
        "realized_trade_pnl_dollars": 0.0,
        "positions_open": [],
        "positions_closed": [],
        "positions_open_count": 0,
        "positions_closed_count": 0,
        "equity_curve": [],
        "strategy_accounting_version": 2,
        "entry_price": None,
        "entry_time_utc": None,
        "side": None,
        "contracts": None,
        "notional_risk_dollars": None,
        "mark_price": None,
    }


def _shadow_bankroll_report_fields(shadow_bankroll: dict[str, Any] | None) -> dict[str, Any]:
    payload = shadow_bankroll if isinstance(shadow_bankroll, dict) else {}
    start_dollars = _parse_float(payload.get("start_dollars"))
    start_text = (
        f"{max(0.0, start_dollars):.4f}".rstrip("0").rstrip(".")
        if isinstance(start_dollars, float)
        else ""
    )
    sizing_basis = f"shadow_{start_text}" if start_text else "shadow_unknown"
    return {
        "sizing_basis": sizing_basis,
        "execution_basis": "live_actual_balance",
        "shadow_bankroll_enabled": bool(payload.get("enabled")),
        "shadow_bankroll_state_file": payload.get("state_file"),
        "shadow_bankroll_status": payload.get("status"),
        "shadow_bankroll_reason": payload.get("reason"),
        "shadow_bankroll_last_updated_at_utc": payload.get("last_updated_at_utc"),
        "shadow_bankroll_state_write_error": payload.get("state_write_error"),
        "shadow_bankroll_start_dollars": payload.get("start_dollars"),
        "shadow_theoretical_value_dollars": payload.get("theoretical_value_dollars"),
        "shadow_realized_pnl_dollars": payload.get("realized_pnl_dollars"),
        "shadow_theoretical_unrealized_ev_dollars": payload.get("theoretical_unrealized_ev_dollars"),
        "shadow_theoretical_drawdown_pct": payload.get("theoretical_drawdown_pct"),
        "shadow_allocator_total_risk_dollars": payload.get("allocator_total_risk_dollars"),
        "shadow_allocator_selected_rows": payload.get("allocator_selected_rows"),
        "shadow_expected_value_dollars": payload.get("expected_value_dollars"),
        "shadow_strategy_equity_dollars": payload.get("strategy_equity_dollars"),
        "shadow_strategy_drawdown_pct": payload.get("strategy_drawdown_pct"),
        "shadow_mark_to_model_pnl_dollars": payload.get("mark_to_model_pnl_dollars"),
        "shadow_realized_trade_pnl_dollars": payload.get("realized_trade_pnl_dollars"),
        "shadow_positions_open_count": payload.get("positions_open_count"),
        "shadow_positions_closed_count": payload.get("positions_closed_count"),
        "shadow_positions_open": payload.get("positions_open"),
        "shadow_positions_closed": payload.get("positions_closed"),
        "shadow_equity_curve": payload.get("equity_curve"),
        "shadow_strategy_accounting_version": payload.get("strategy_accounting_version"),
        "shadow_entry_price": payload.get("entry_price"),
        "shadow_entry_time": payload.get("entry_time_utc"),
        "shadow_side": payload.get("side"),
        "shadow_contracts": payload.get("contracts"),
        "shadow_notional_risk_dollars": payload.get("notional_risk_dollars"),
        "shadow_mark_price": payload.get("mark_price"),
    }


def _paper_live_defaults(
    *,
    enabled: bool,
    start_dollars: float,
    state_file: Path | None,
    status: str,
    reason: str | None,
    risk_profile: str = "growth_aggressive",
    kelly_fraction: float = 0.5,
    kelly_high_conf_max: float = 0.75,
    max_open_risk_pct: float = 0.25,
    max_family_risk_pct: float = 0.15,
    max_strip_risk_pct: float = 0.08,
    max_single_position_risk_pct: float = 0.06,
    max_new_attempts_per_run: int = 8,
    family_allowlist: list[str] | None = None,
    allow_random_cancels: bool = False,
    size_from_current_equity: bool = True,
    require_live_eligible_hint: bool = False,
) -> dict[str, Any]:
    start = round(max(0.0, float(start_dollars)), 4)
    normalized_allowlist = [
        str(token or "").strip().lower()
        for token in (family_allowlist or ["monthly_climate_anomaly"])
        if str(token or "").strip()
    ]
    if not normalized_allowlist:
        normalized_allowlist = ["monthly_climate_anomaly"]
    return {
        "enabled": bool(enabled),
        "status": str(status or "observer_not_run"),
        "reason": str(reason or "").strip() or None,
        "execution_basis": "paper_live_balance",
        "risk_profile": str(risk_profile or "growth_aggressive").strip() or "growth_aggressive",
        "kelly_fraction": round(max(0.0, float(kelly_fraction)), 6),
        "kelly_high_conf_max": round(max(0.0, float(kelly_high_conf_max)), 6),
        "max_open_risk_pct": round(max(0.0, float(max_open_risk_pct)), 6),
        "max_family_risk_pct": round(max(0.0, float(max_family_risk_pct)), 6),
        "max_strip_risk_pct": round(max(0.0, float(max_strip_risk_pct)), 6),
        "max_single_position_risk_pct": round(max(0.0, float(max_single_position_risk_pct)), 6),
        "max_new_attempts_per_run": max(1, int(max_new_attempts_per_run)),
        "family_allowlist": normalized_allowlist,
        "allow_random_cancels": bool(allow_random_cancels),
        "size_from_current_equity": bool(size_from_current_equity),
        "require_live_eligible_hint": bool(require_live_eligible_hint),
        "state_file": str(state_file) if isinstance(state_file, Path) else None,
        "start_dollars": start,
        "current_dollars": start,
        "sizing_balance_dollars": start,
        "post_trade_sizing_balance_dollars": start,
        "realized_trade_pnl_dollars": 0.0,
        "mark_to_market_pnl_dollars": 0.0,
        "drawdown_pct": 0.0,
        "positions_open": [],
        "positions_closed": [],
        "positions_open_count": 0,
        "positions_closed_count": 0,
        "order_attempts": 0,
        "orders_partial_filled": 0,
        "orders_resting": 0,
        "orders_filled": 0,
        "orders_canceled": 0,
        "orders_expired": 0,
        "fill_time_seconds": None,
        "run_attempt_limit": max(1, int(max_new_attempts_per_run)),
        "used_kelly_fraction": 0.0,
        "avg_kelly_fraction_used": 0.0,
        "markout_10s_dollars": 0.0,
        "markout_60s_dollars": 0.0,
        "markout_300s_dollars": 0.0,
        "settlement_pnl_dollars": 0.0,
        "expected_value_dollars": 0.0,
        "expected_vs_realized_delta": 0.0,
        "open_risk_dollars": 0.0,
        "open_risk_cap_dollars": 0.0,
        "open_risk_remaining_dollars": 0.0,
        "family_open_risk_dollars": {},
        "family_open_risk_remaining_dollars": {},
        "strip_open_risk_dollars": {},
        "strip_open_risk_remaining_dollars": {},
        "family_execution_state": {},
        "ticker_execution_state": {},
        "family_mtm_per_risk_pct": {},
        "ticker_mtm_per_risk_pct": {},
        "family_markout_300s_mean_dollars": {},
        "ticker_markout_300s_mean_dollars": {},
        "family_markout_300s_mean_per_contract_dollars": {},
        "ticker_markout_300s_mean_per_contract_dollars": {},
        "family_markout_300s_per_risk_pct": {},
        "ticker_markout_300s_per_risk_pct": {},
        "family_markout_300s_per_contract": {},
        "ticker_markout_300s_per_contract": {},
        "family_fill_rate": {},
        "ticker_fill_rate": {},
        "family_cancel_rate": {},
        "ticker_cancel_rate": {},
        "family_risk_multiplier": {},
        "ticker_risk_multiplier": {},
        "drawdown_throttle_state": "full",
        "drawdown_risk_scale": 1.0,
        "selected_tickers": [],
        "attempt_events": [],
        "equity_curve": [],
        "last_updated_at_utc": None,
        "accounting_version": 2,
        "source": "synthetic_paper_live",
        "state_write_error": None,
    }


def _paper_live_deterministic_score(*parts: str) -> float:
    seed = "|".join(str(part or "").strip() for part in parts)
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    numerator = int(digest[:8], 16)
    denominator = float(0xFFFFFFFF)
    return max(0.0, min(1.0, numerator / denominator))


def _paper_live_markout_pnl(
    *,
    position_key: str,
    side: str,
    entry_price: float,
    edge_net: float,
    fair_yes_probability: float | None,
    horizon_seconds: int,
) -> float:
    baseline_yes = fair_yes_probability if isinstance(fair_yes_probability, float) else 0.5
    baseline_yes = max(0.01, min(0.99, baseline_yes))
    drift = float(edge_net) * min(1.0, max(0.0, float(horizon_seconds)) / 300.0)
    noise = (_paper_live_deterministic_score(position_key, str(horizon_seconds), "markout_noise") - 0.5) * 0.04
    mark_yes = max(0.01, min(0.99, baseline_yes + drift + noise))
    if str(side or "").strip().lower() == "no":
        mark_price = 1.0 - mark_yes
    else:
        mark_price = mark_yes
    return round(float(mark_price - float(entry_price)), 6)


def _paper_live_settlement_pnl(
    *,
    position_key: str,
    side: str,
    entry_price: float,
    fair_yes_probability: float | None,
    contracts: int,
) -> tuple[float, str]:
    yes_prob = fair_yes_probability if isinstance(fair_yes_probability, float) else 0.5
    yes_prob = max(0.01, min(0.99, yes_prob))
    yes_outcome = _paper_live_deterministic_score(position_key, "settlement_yes") < yes_prob
    side_lower = str(side or "").strip().lower()
    payout = 0.0
    if side_lower == "no":
        payout = 1.0 if not yes_outcome else 0.0
    else:
        payout = 1.0 if yes_outcome else 0.0
    pnl_per_contract = payout - float(entry_price)
    return round(float(pnl_per_contract) * float(max(1, int(contracts))), 6), ("yes" if yes_outcome else "no")


def _paper_live_candidate_pool(
    *,
    climate_router_pilot: dict[str, Any] | None,
    climate_router_shadow_plan: dict[str, Any] | None,
    allowed_families: list[str] | None = None,
) -> list[dict[str, Any]]:
    pilot = climate_router_pilot if isinstance(climate_router_pilot, dict) else {}
    shadow_plan = climate_router_shadow_plan if isinstance(climate_router_shadow_plan, dict) else {}
    family_allowset = {
        str(item or "").strip().lower() for item in (allowed_families or ["monthly_climate_anomaly"]) if str(item or "").strip()
    }
    if not family_allowset:
        family_allowset = {"monthly_climate_anomaly"}

    selected_tickers_raw = pilot.get("selected_tickers")
    selected_tickers = [
        str(item or "").strip().upper()
        for item in selected_tickers_raw
        if str(item or "").strip()
    ] if isinstance(selected_tickers_raw, list) else []

    pool_by_ticker: dict[str, dict[str, Any]] = {}

    # Candidate rows from pilot diagnostics.
    top_candidates_raw = pilot.get("top_candidates")
    top_candidates = [item for item in top_candidates_raw if isinstance(item, dict)] if isinstance(top_candidates_raw, list) else []
    for row in top_candidates:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        if not ticker:
            continue
        family = str(row.get("contract_family") or "").strip().lower()
        if family and family not in family_allowset:
            continue
        candidate = {
            "market_ticker": ticker,
            "market_title": str(row.get("market_title") or "").strip() or None,
            "contract_family": family or "unknown_family",
            "strip_key": str(row.get("strip_key") or "").strip() or None,
            "availability_state": str(row.get("availability_state") or "").strip().lower() or None,
            "opportunity_class": str(row.get("opportunity_class") or "").strip().lower() or None,
            "theoretical_side": str(row.get("theoretical_side") or "").strip().lower() or "yes",
            "theoretical_reference_price": _parse_float(row.get("theoretical_reference_price")),
            "theoretical_edge_net": _parse_float(row.get("theoretical_edge_net")),
            "fair_yes_probability": _parse_float(row.get("fair_yes_probability")),
            "hours_to_close": _parse_float(row.get("hours_to_close")),
            "source_strategy": str(row.get("source_strategy") or "").strip() or "climate_router_pilot",
            "expected_value_dollars": _parse_float(row.get("expected_value_dollars")),
            "suggested_risk_dollars": _parse_float(row.get("suggested_risk_dollars")),
        }
        pool_by_ticker[ticker] = candidate

    # Enrich / add candidates from shadow allocations so paper-live is not constrained to 1x1 pilot picks.
    allocations_raw = shadow_plan.get("top_shadow_allocations")
    allocations = [item for item in allocations_raw if isinstance(item, dict)] if isinstance(allocations_raw, list) else []
    for row in allocations:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        if not ticker:
            continue
        family = str(row.get("contract_family") or "").strip().lower()
        if family and family not in family_allowset:
            continue
        existing = dict(pool_by_ticker.get(ticker) or {})
        side = str(row.get("side") or "").strip().lower()
        reference_price = _parse_float(row.get("reference_price_dollars"))
        if not isinstance(reference_price, float):
            reference_price = _parse_float(existing.get("theoretical_reference_price"))
        edge_net = _parse_float(row.get("edge_net"))
        if not isinstance(edge_net, float):
            edge_net = _parse_float(existing.get("theoretical_edge_net"))
        expected_value = _parse_float(row.get("expected_value_dollars"))
        if not isinstance(expected_value, float):
            expected_value = _parse_float(existing.get("expected_value_dollars"))
        suggested_risk = _parse_float(row.get("risk_dollars"))
        if not isinstance(suggested_risk, float):
            suggested_risk = _parse_float(existing.get("suggested_risk_dollars"))
        candidate = {
            **existing,
            "market_ticker": ticker,
            "contract_family": family or str(existing.get("contract_family") or "unknown_family"),
            "strip_key": str(row.get("strip_key") or existing.get("strip_key") or "").strip() or None,
            "availability_state": str(row.get("availability_state") or existing.get("availability_state") or "").strip().lower() or None,
            "opportunity_class": str(row.get("opportunity_class") or existing.get("opportunity_class") or "").strip().lower() or None,
            "theoretical_side": side if side in {"yes", "no"} else str(existing.get("theoretical_side") or "yes"),
            "theoretical_reference_price": reference_price,
            "theoretical_edge_net": edge_net,
            "expected_value_dollars": expected_value,
            "suggested_risk_dollars": suggested_risk,
            "source_strategy": str(existing.get("source_strategy") or "climate_router_shadow_plan"),
            "contracts": _coerce_int(row.get("contracts"), 0),
        }
        pool_by_ticker[ticker] = candidate

    def _candidate_rank(row: dict[str, Any]) -> tuple[int, float, float, int, str]:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        expected_value = float(_parse_float(row.get("expected_value_dollars")) or 0.0)
        edge_net = float(_parse_float(row.get("theoretical_edge_net")) or 0.0)
        suggested_risk = float(_parse_float(row.get("suggested_risk_dollars")) or 0.0)
        selected_rank = 1 if ticker in set(selected_tickers) else 0
        return (
            selected_rank,
            expected_value,
            edge_net,
            suggested_risk,
            ticker,
        )

    ranked = sorted(pool_by_ticker.values(), key=_candidate_rank, reverse=True)
    normalized: list[dict[str, Any]] = []
    for row in ranked:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        family = str(row.get("contract_family") or "").strip().lower()
        if not ticker or not family or family not in family_allowset:
            continue
        side = str(row.get("theoretical_side") or "").strip().lower()
        if side not in {"yes", "no"}:
            side = "yes"
        normalized.append({**row, "market_ticker": ticker, "contract_family": family, "theoretical_side": side})
    return normalized


def _paper_live_drawdown_scale(drawdown_pct: float) -> tuple[float, str]:
    drawdown = max(0.0, float(drawdown_pct))
    if drawdown > 18.0:
        return 0.35, "max_throttle"
    if drawdown >= 12.0:
        return 0.5, "half_risk"
    if drawdown >= 8.0:
        return 0.75, "reduced_risk"
    return 1.0, "full"


def _paper_live_tier_targets(edge_net: float) -> tuple[str, float]:
    edge = float(edge_net)
    if edge >= 0.20:
        return "A+", 0.06
    if edge >= 0.12:
        return "A", 0.04
    if edge >= 0.05:
        return "B", 0.025
    return "C", 0.0


def _paper_live_update(
    *,
    run_id: str,
    run_finished_at_utc: str,
    enabled: bool,
    start_dollars: float,
    state_file: Path | None,
    climate_router_pilot: dict[str, Any] | None,
    climate_router_shadow_plan: dict[str, Any] | None,
    risk_profile: str = "growth_aggressive",
    kelly_fraction: float = 0.5,
    kelly_high_conf_max: float = 0.75,
    max_open_risk_pct: float = 0.25,
    max_family_risk_pct: float = 0.15,
    max_strip_risk_pct: float = 0.08,
    max_single_position_risk_pct: float = 0.06,
    max_new_attempts_per_run: int = 8,
    family_allowlist: list[str] | None = None,
    allow_random_cancels: bool = False,
    size_from_current_equity: bool = True,
    require_live_eligible_hint: bool = False,
) -> dict[str, Any]:
    snapshot = _paper_live_defaults(
        enabled=enabled,
        start_dollars=start_dollars,
        state_file=state_file,
        status="observer_not_run",
        reason=None,
        risk_profile=risk_profile,
        kelly_fraction=kelly_fraction,
        kelly_high_conf_max=kelly_high_conf_max,
        max_open_risk_pct=max_open_risk_pct,
        max_family_risk_pct=max_family_risk_pct,
        max_strip_risk_pct=max_strip_risk_pct,
        max_single_position_risk_pct=max_single_position_risk_pct,
        max_new_attempts_per_run=max_new_attempts_per_run,
        family_allowlist=family_allowlist,
        allow_random_cancels=allow_random_cancels,
        size_from_current_equity=size_from_current_equity,
        require_live_eligible_hint=require_live_eligible_hint,
    )
    if not enabled:
        snapshot["status"] = "disabled"
        snapshot["reason"] = "paper_live_disabled"
        return snapshot
    if not isinstance(state_file, Path):
        snapshot["status"] = "observer_degraded"
        snapshot["reason"] = "paper_live_state_file_missing"
        return snapshot

    run_finished_at_dt = _parse_iso(run_finished_at_utc)
    if not isinstance(run_finished_at_dt, datetime):
        run_finished_at_dt = datetime.now(timezone.utc)

    start = max(0.0, float(start_dollars))
    realized_trade_total = 0.0
    settlement_pnl_total = 0.0
    expected_value_total = 0.0
    settlement_expected_value_total = 0.0
    fill_time_seconds_total = 0.0
    fill_events_count = 0
    peak_balance = start
    order_attempts_total = 0
    orders_filled_total = 0
    orders_partial_filled_total = 0
    orders_canceled_total = 0
    orders_expired_total = 0
    last_run_id = ""
    positions_open: list[dict[str, Any]] = []
    positions_closed: list[dict[str, Any]] = []
    resting_orders: list[dict[str, Any]] = []
    attempt_events: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []

    existing_payload = _load_json(state_file)
    if isinstance(existing_payload, dict):
        previous_start = _parse_float(existing_payload.get("paper_live_balance_start_dollars"))
        if isinstance(previous_start, float) and previous_start > 0.0 and abs(previous_start - start) < 1e-9:
            start = previous_start
            realized_trade_total = float(
                _parse_float(existing_payload.get("paper_live_realized_trade_pnl_dollars")) or 0.0
            )
            settlement_pnl_total = float(
                _parse_float(existing_payload.get("paper_live_settlement_pnl_dollars")) or realized_trade_total
            )
            expected_value_total = float(
                _parse_float(existing_payload.get("paper_live_expected_value_dollars")) or 0.0
            )
            settlement_expected_value_total = float(
                _parse_float(existing_payload.get("paper_live_settlement_expected_value_dollars")) or 0.0
            )
            peak_balance = max(
                start,
                float(_parse_float(existing_payload.get("paper_live_peak_balance_dollars")) or start),
            )
            order_attempts_total = max(0, _coerce_int(existing_payload.get("paper_live_order_attempts"), 0))
            orders_filled_total = max(0, _coerce_int(existing_payload.get("paper_live_orders_filled"), 0))
            orders_partial_filled_total = max(
                0,
                _coerce_int(existing_payload.get("paper_live_orders_partial_filled"), 0),
            )
            orders_canceled_total = max(0, _coerce_int(existing_payload.get("paper_live_orders_canceled"), 0))
            orders_expired_total = max(0, _coerce_int(existing_payload.get("paper_live_orders_expired"), 0))
            fill_events_count = max(0, _coerce_int(existing_payload.get("paper_live_fill_events_count"), 0))
            fill_time_seconds_total = float(
                _parse_float(existing_payload.get("paper_live_fill_time_seconds_total")) or 0.0
            )
            if fill_events_count <= 0 and orders_filled_total > 0:
                fill_time_avg = _parse_float(existing_payload.get("paper_live_fill_time_seconds"))
                if isinstance(fill_time_avg, float) and fill_time_avg >= 0.0:
                    fill_events_count = orders_filled_total
                    fill_time_seconds_total = float(fill_time_avg) * float(fill_events_count)
            last_run_id = str(existing_payload.get("last_run_id") or "").strip()
            raw_open = existing_payload.get("paper_live_positions_open")
            if isinstance(raw_open, list):
                positions_open = [item for item in raw_open if isinstance(item, dict)]
            raw_closed = existing_payload.get("paper_live_positions_closed")
            if isinstance(raw_closed, list):
                positions_closed = [item for item in raw_closed if isinstance(item, dict)]
            raw_resting = existing_payload.get("paper_live_resting_orders")
            if isinstance(raw_resting, list):
                resting_orders = [item for item in raw_resting if isinstance(item, dict)]
            raw_attempt_events = existing_payload.get("paper_live_attempt_events")
            if isinstance(raw_attempt_events, list):
                attempt_events = [item for item in raw_attempt_events if isinstance(item, dict)]
            raw_curve = existing_payload.get("paper_live_equity_curve")
            if isinstance(raw_curve, list):
                equity_curve = [item for item in raw_curve if isinstance(item, dict)]

    run_is_new = bool(run_id and run_id != last_run_id)
    pilot = climate_router_pilot if isinstance(climate_router_pilot, dict) else {}
    normalized_family_allowlist = [
        str(token or "").strip().lower()
        for token in (family_allowlist or ["monthly_climate_anomaly"])
        if str(token or "").strip()
    ]
    if not normalized_family_allowlist:
        normalized_family_allowlist = ["monthly_climate_anomaly"]
    normalized_risk_profile = str(risk_profile or "growth_aggressive").strip().lower() or "growth_aggressive"
    normalized_kelly_fraction = max(0.0, min(1.0, float(kelly_fraction)))
    normalized_kelly_high_conf_max = max(normalized_kelly_fraction, min(1.0, float(kelly_high_conf_max)))
    normalized_max_open_risk_pct = max(0.01, min(0.9, float(max_open_risk_pct)))
    normalized_max_family_risk_pct = max(0.01, min(normalized_max_open_risk_pct, float(max_family_risk_pct)))
    normalized_max_strip_risk_pct = max(0.005, min(normalized_max_family_risk_pct, float(max_strip_risk_pct)))
    normalized_max_single_position_risk_pct = max(
        0.005,
        min(normalized_max_strip_risk_pct, float(max_single_position_risk_pct)),
    )
    normalized_max_new_attempts_per_run = max(1, int(max_new_attempts_per_run))
    normalized_allow_random_cancels = bool(allow_random_cancels)
    normalized_size_from_current_equity = bool(size_from_current_equity)
    normalized_require_live_eligible_hint = bool(require_live_eligible_hint)

    candidate_pool = _paper_live_candidate_pool(
        climate_router_pilot=pilot,
        climate_router_shadow_plan=climate_router_shadow_plan,
        allowed_families=normalized_family_allowlist,
    )
    selected_candidate = candidate_pool[0] if candidate_pool else None
    selected_ticker = (
        str(selected_candidate.get("market_ticker") or "").strip().upper()
        if isinstance(selected_candidate, dict)
        else ""
    )

    open_by_key: dict[str, dict[str, Any]] = {}
    for position in positions_open:
        key = _shadow_position_key(
            market_ticker=str(position.get("market_ticker") or ""),
            side=str(position.get("side") or ""),
        )
        if key and key not in open_by_key:
            open_by_key[key] = dict(position)

    resting_by_key: dict[str, dict[str, Any]] = {}
    for order in resting_orders:
        key = _shadow_position_key(
            market_ticker=str(order.get("market_ticker") or ""),
            side=str(order.get("side") or ""),
        )
        if key and key not in resting_by_key:
            resting_by_key[key] = dict(order)

    def _risk_dollars_from_payload(item: dict[str, Any]) -> float:
        risk = _parse_float(item.get("notional_risk_dollars"))
        if isinstance(risk, float) and risk >= 0.0:
            return float(risk)
        price = float(_parse_float(item.get("entry_price_dollars")) or 0.0)
        contracts = max(1, _coerce_int(item.get("contracts"), 1))
        return max(0.0, price * float(contracts))

    open_risk_by_family: dict[str, float] = {}
    open_risk_by_strip: dict[str, float] = {}
    open_risk_total = 0.0
    for position in open_by_key.values():
        if not isinstance(position, dict):
            continue
        risk = _risk_dollars_from_payload(position)
        if risk <= 0.0:
            continue
        open_risk_total += risk
        family = str(position.get("contract_family") or "unknown_family").strip().lower() or "unknown_family"
        strip = str(position.get("strip_key") or "").strip() or "__no_strip__"
        open_risk_by_family[family] = float(open_risk_by_family.get(family, 0.0)) + risk
        open_risk_by_strip[strip] = float(open_risk_by_strip.get(strip, 0.0)) + risk

    open_mark_to_model_pre = 0.0
    for position in open_by_key.values():
        if not isinstance(position, dict):
            continue
        open_mark_to_model_pre += float(_parse_float(position.get("mark_to_model_pnl_dollars")) or 0.0)
    pre_trade_balance = float(start) + float(realized_trade_total) + float(open_mark_to_model_pre)
    sizing_balance_dollars = float(pre_trade_balance) if normalized_size_from_current_equity else float(start)
    if sizing_balance_dollars <= 0.0:
        sizing_balance_dollars = float(start)
    sizing_balance_dollars = max(0.0, float(sizing_balance_dollars))
    peak_balance_for_risk = max(float(peak_balance), float(start), float(pre_trade_balance))
    pre_trade_drawdown_pct = (
        ((peak_balance_for_risk - pre_trade_balance) / peak_balance_for_risk * 100.0)
        if peak_balance_for_risk > 0.0
        else 0.0
    )
    drawdown_risk_scale, drawdown_throttle_state = _paper_live_drawdown_scale(pre_trade_drawdown_pct)

    run_attempt_limit = normalized_max_new_attempts_per_run
    if normalized_risk_profile != "growth_aggressive":
        run_attempt_limit = min(run_attempt_limit, 1)

    open_risk_cap_dollars = max(0.0, float(sizing_balance_dollars) * normalized_max_open_risk_pct * drawdown_risk_scale)
    family_risk_cap_dollars = max(0.0, float(sizing_balance_dollars) * normalized_max_family_risk_pct * drawdown_risk_scale)
    strip_risk_cap_dollars = max(0.0, float(sizing_balance_dollars) * normalized_max_strip_risk_pct * drawdown_risk_scale)
    single_position_risk_cap_dollars = max(
        0.0,
        float(sizing_balance_dollars) * normalized_max_single_position_risk_pct * drawdown_risk_scale,
    )

    per_attempt_kelly_used: list[float] = []
    # Include resting order notional in open risk budget to prevent over-commit during illiquid phases.
    for order in resting_by_key.values():
        if not isinstance(order, dict):
            continue
        risk = _risk_dollars_from_payload(order)
        if risk <= 0.0:
            continue
        family = str(order.get("contract_family") or "unknown_family").strip().lower() or "unknown_family"
        strip = str(order.get("strip_key") or "").strip() or "__no_strip__"
        open_risk_total += risk
        open_risk_by_family[family] = float(open_risk_by_family.get(family, 0.0)) + risk
        open_risk_by_strip[strip] = float(open_risk_by_strip.get(strip, 0.0)) + risk

    attempt_events_by_key: dict[str, int] = {}
    for idx, event in enumerate(attempt_events):
        event_key = str(event.get("attempt_event_key") or "").strip()
        if event_key:
            attempt_events_by_key[event_key] = idx

    def _ensure_attempt_event(
        *,
        attempt_event_key: str,
        run_id_value: str,
        attempted_at_utc: str,
        market_ticker: str,
        contract_family: str | None,
        strip_key: str | None,
        side: str,
        contracts: int,
        notional_risk_dollars: float,
        expected_value_dollars: float,
        source_strategy: str | None,
        opportunity_class: str | None,
        order_id: str | None,
        status: str,
    ) -> None:
        key = str(attempt_event_key or "").strip()
        if not key:
            return
        payload = {
            "attempt_event_key": key,
            "run_id": run_id_value,
            "attempted_at_utc": attempted_at_utc,
            "market_ticker": str(market_ticker or "").strip().upper() or None,
            "contract_family": str(contract_family or "").strip().lower() or None,
            "strip_key": str(strip_key or "").strip() or None,
            "side": str(side or "").strip().lower() or "yes",
            "contracts": max(1, int(contracts)),
            "notional_risk_dollars": round(float(notional_risk_dollars), 6),
            "expected_value_dollars": round(float(expected_value_dollars), 6),
            "source_strategy": str(source_strategy or "").strip() or None,
            "opportunity_class": str(opportunity_class or "").strip() or None,
            "order_id": str(order_id or "").strip() or None,
            "status": str(status or "").strip().lower() or "unknown",
            "partial_fill": False,
            "fill_time_seconds": None,
            "filled_at_utc": None,
            "canceled_at_utc": None,
            "expired_at_utc": None,
        }
        existing_idx = attempt_events_by_key.get(key)
        if existing_idx is None:
            attempt_events.append(payload)
            attempt_events_by_key[key] = len(attempt_events) - 1
            return
        existing = attempt_events[existing_idx]
        if not isinstance(existing, dict):
            attempt_events[existing_idx] = payload
            return
        merged = dict(existing)
        for field, value in payload.items():
            if merged.get(field) in (None, "", []) and value not in (None, "", []):
                merged[field] = value
        merged["status"] = str(status or merged.get("status") or "unknown").strip().lower()
        if payload["order_id"] and not merged.get("order_id"):
            merged["order_id"] = payload["order_id"]
        attempt_events[existing_idx] = merged

    def _update_attempt_event_status(
        *,
        attempt_event_key: str | None,
        status: str,
        fill_time_seconds: float | None = None,
        partial_fill: bool | None = None,
        event_time_utc: str | None = None,
    ) -> None:
        key = str(attempt_event_key or "").strip()
        if not key:
            return
        idx = attempt_events_by_key.get(key)
        if idx is None:
            return
        event = attempt_events[idx]
        if not isinstance(event, dict):
            return
        normalized_status = str(status or "").strip().lower()
        if normalized_status:
            event["status"] = normalized_status
        if isinstance(fill_time_seconds, float):
            event["fill_time_seconds"] = round(max(0.0, float(fill_time_seconds)), 6)
        if isinstance(partial_fill, bool):
            event["partial_fill"] = bool(partial_fill)
        event_time_text = str(event_time_utc or "").strip() or run_finished_at_dt.astimezone(timezone.utc).isoformat()
        if normalized_status == "filled":
            event["filled_at_utc"] = event_time_text
        elif normalized_status == "canceled":
            event["canceled_at_utc"] = event_time_text
        elif normalized_status == "expired":
            event["expired_at_utc"] = event_time_text
        attempt_events[idx] = event

    def _build_execution_quality_rows(
        *,
        attempts_data: list[dict[str, Any]],
        open_positions_data: list[dict[str, Any]],
        closed_positions_data: list[dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        def _row(*, family: str, ticker: str | None) -> dict[str, Any]:
            return {
                "family": family,
                "ticker": ticker,
                "attempts": 0,
                "fills": 0,
                "canceled": 0,
                "markout_300s_sum_dollars": 0.0,
                "markout_300s_sum_per_contract_dollars": 0.0,
                "markout_300s_count": 0,
                "mtm_sum_dollars": 0.0,
                "risk_sum_dollars": 0.0,
                "state": "warmup",
                "risk_multiplier": 1.0,
            }

        def _touch(
            store: dict[str, dict[str, Any]],
            key: str,
            *,
            family: str,
            ticker: str | None,
        ) -> dict[str, Any]:
            existing = store.get(key)
            if isinstance(existing, dict):
                return existing
            created = _row(family=family, ticker=ticker)
            store[key] = created
            return created

        by_family: dict[str, dict[str, Any]] = {}
        by_ticker: dict[str, dict[str, Any]] = {}

        for event in attempts_data:
            if not isinstance(event, dict):
                continue
            family = str(event.get("contract_family") or "").strip().lower() or "unknown_family"
            ticker = str(event.get("market_ticker") or "").strip().upper() or "UNKNOWN_TICKER"
            status = str(event.get("status") or "").strip().lower()
            family_row = _touch(by_family, family, family=family, ticker=None)
            ticker_row = _touch(by_ticker, ticker, family=family, ticker=ticker)
            for row in (family_row, ticker_row):
                row["attempts"] = int(row.get("attempts") or 0) + 1
                if status == "filled":
                    row["fills"] = int(row.get("fills") or 0) + 1
                elif status == "canceled":
                    row["canceled"] = int(row.get("canceled") or 0) + 1

        for position in [*open_positions_data, *closed_positions_data]:
            if not isinstance(position, dict):
                continue
            family = str(position.get("contract_family") or "").strip().lower() or "unknown_family"
            ticker = str(position.get("market_ticker") or "").strip().upper() or "UNKNOWN_TICKER"
            contracts = max(1, _coerce_int(position.get("contracts"), 1))
            markout_300 = _parse_float(position.get("markout_300s_dollars"))
            mtm_value = _parse_float(position.get("mark_to_model_pnl_dollars"))
            if not isinstance(mtm_value, float):
                mtm_value = _parse_float(position.get("settlement_pnl_dollars"))
            if not isinstance(mtm_value, float):
                mtm_value = _parse_float(position.get("realized_trade_pnl_dollars"))
            risk_value = _risk_dollars_from_payload(position)

            family_row = _touch(by_family, family, family=family, ticker=None)
            ticker_row = _touch(by_ticker, ticker, family=family, ticker=ticker)
            for row in (family_row, ticker_row):
                if isinstance(markout_300, float):
                    row["markout_300s_sum_dollars"] = float(row.get("markout_300s_sum_dollars") or 0.0) + float(markout_300)
                    row["markout_300s_sum_per_contract_dollars"] = (
                        float(row.get("markout_300s_sum_per_contract_dollars") or 0.0)
                        + (float(markout_300) / float(max(1, contracts)))
                    )
                    row["markout_300s_count"] = int(row.get("markout_300s_count") or 0) + 1
                row["mtm_sum_dollars"] = float(row.get("mtm_sum_dollars") or 0.0) + float(mtm_value or 0.0)
                row["risk_sum_dollars"] = float(row.get("risk_sum_dollars") or 0.0) + float(max(0.0, risk_value))

        def _finalize(row: dict[str, Any]) -> dict[str, Any]:
            fills = max(0, _coerce_int(row.get("fills"), 0))
            attempts = max(0, _coerce_int(row.get("attempts"), 0))
            canceled = max(0, _coerce_int(row.get("canceled"), 0))
            markout_count = max(0, _coerce_int(row.get("markout_300s_count"), 0))
            markout_300_mean_dollars = (
                round(float(row.get("markout_300s_sum_dollars") or 0.0) / float(markout_count), 6)
                if markout_count > 0
                else None
            )
            markout_300_mean_per_contract_dollars = (
                round(float(row.get("markout_300s_sum_per_contract_dollars") or 0.0) / float(markout_count), 6)
                if markout_count > 0
                else None
            )
            risk_sum = float(row.get("risk_sum_dollars") or 0.0)
            markout_300_per_risk_pct = (
                round((float(row.get("markout_300s_sum_dollars") or 0.0) / risk_sum) * 100.0, 6)
                if risk_sum > 0.0
                else None
            )
            mtm_sum = float(row.get("mtm_sum_dollars") or 0.0)
            mtm_per_risk_pct = round((mtm_sum / risk_sum) * 100.0, 6) if risk_sum > 0.0 else None
            fill_rate = round(float(fills) / float(attempts), 6) if attempts > 0 else None
            cancel_rate = round(float(canceled) / float(attempts), 6) if attempts > 0 else None

            poor_quality = False
            if isinstance(markout_300_mean_per_contract_dollars, float) and markout_300_mean_per_contract_dollars < -0.05:
                poor_quality = True
            if isinstance(mtm_per_risk_pct, float) and mtm_per_risk_pct < -15.0:
                poor_quality = True

            state = "warmup"
            risk_multiplier = 1.0
            if fills < 5:
                state = "warmup"
                risk_multiplier = 1.0
            elif fills >= 10 and poor_quality:
                state = "paused"
                risk_multiplier = 0.0
            elif poor_quality:
                state = "degraded"
                risk_multiplier = 0.5
            else:
                state = "healthy"
                risk_multiplier = 1.0

            return {
                **row,
                "state": state,
                "risk_multiplier": round(float(risk_multiplier), 6),
                "fill_rate": fill_rate,
                "cancel_rate": cancel_rate,
                "markout_300s_mean_dollars": markout_300_mean_dollars,
                "markout_300s_mean_per_contract_dollars": markout_300_mean_per_contract_dollars,
                "markout_300s_per_risk_pct": markout_300_per_risk_pct,
                "markout_300s_per_contract": markout_300_mean_per_contract_dollars,
                "mtm_per_risk_pct": mtm_per_risk_pct,
            }

        return (
            {key: _finalize(value) for key, value in by_family.items()},
            {key: _finalize(value) for key, value in by_ticker.items()},
        )

    family_quality_rows, ticker_quality_rows = _build_execution_quality_rows(
        attempts_data=attempt_events,
        open_positions_data=[dict(item) for item in open_by_key.values() if isinstance(item, dict)],
        closed_positions_data=[dict(item) for item in positions_closed if isinstance(item, dict)],
    )
    family_execution_state_map = {
        key: str(value.get("state") or "warmup")
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_execution_state_map = {
        key: str(value.get("state") or "warmup")
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }
    family_risk_multiplier_map = {
        key: round(float(_parse_float(value.get("risk_multiplier")) or 1.0), 6)
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_risk_multiplier_map = {
        key: round(float(_parse_float(value.get("risk_multiplier")) or 1.0), 6)
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }
    family_fill_rate_map = {
        key: _parse_float(value.get("fill_rate"))
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_fill_rate_map = {
        key: _parse_float(value.get("fill_rate"))
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }
    family_cancel_rate_map = {
        key: _parse_float(value.get("cancel_rate"))
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_cancel_rate_map = {
        key: _parse_float(value.get("cancel_rate"))
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }
    family_mtm_per_risk_pct_map = {
        key: _parse_float(value.get("mtm_per_risk_pct"))
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_mtm_per_risk_pct_map = {
        key: _parse_float(value.get("mtm_per_risk_pct"))
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }
    family_markout_300_mean_map = {
        key: _parse_float(value.get("markout_300s_mean_dollars"))
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_markout_300_mean_map = {
        key: _parse_float(value.get("markout_300s_mean_dollars"))
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }
    family_markout_300_per_contract_mean_map = {
        key: _parse_float(value.get("markout_300s_mean_per_contract_dollars"))
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_markout_300_per_contract_mean_map = {
        key: _parse_float(value.get("markout_300s_mean_per_contract_dollars"))
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }
    family_markout_300_per_risk_pct_map = {
        key: _parse_float(value.get("markout_300s_per_risk_pct"))
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_markout_300_per_risk_pct_map = {
        key: _parse_float(value.get("markout_300s_per_risk_pct"))
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }
    family_markout_300_per_contract_map = {
        key: _parse_float(value.get("markout_300s_per_contract"))
        for key, value in family_quality_rows.items()
        if isinstance(value, dict)
    }
    ticker_markout_300_per_contract_map = {
        key: _parse_float(value.get("markout_300s_per_contract"))
        for key, value in ticker_quality_rows.items()
        if isinstance(value, dict)
    }

    run_attempts = 0
    run_filled = 0
    run_partial_filled = 0
    run_canceled = 0
    run_expired = 0

    def _append_filled_position(
        *,
        key: str,
        attempt_event_key: str | None,
        ticker: str,
        side: str,
        contract_family: str | None,
        strip_key: str | None,
        entry_price: float,
        contracts: int,
        expected_value_dollars: float,
        fill_source: str,
        fill_time_seconds: float,
        hours_to_close: float | None,
        close_time_estimate_utc: str | None,
        availability_state: str | None,
        fair_yes_probability: float | None,
        edge_net: float,
        source_strategy: str | None,
        opportunity_class: str | None,
        partial_fill: bool,
    ) -> None:
        nonlocal fill_events_count
        nonlocal fill_time_seconds_total
        nonlocal expected_value_total
        nonlocal orders_filled_total
        nonlocal orders_partial_filled_total
        nonlocal run_filled
        nonlocal run_partial_filled

        normalized_side = str(side or "").strip().lower()
        if normalized_side not in {"yes", "no"}:
            normalized_side = "yes"
        contracts_effective = max(1, int(contracts))
        entry_price_effective = round(max(0.0, min(1.0, float(entry_price))), 6)
        expected_value_effective = round(float(expected_value_dollars), 6)
        fill_seconds_effective = max(0.0, float(fill_time_seconds))
        fair_yes_effective = (
            max(0.01, min(0.99, float(fair_yes_probability)))
            if isinstance(fair_yes_probability, float)
            else None
        )

        open_by_key[key] = {
            "position_id": f"{key}|{run_finished_at_dt.strftime('%Y%m%d%H%M%S')}",
            "position_key": key,
            "attempt_event_key": str(attempt_event_key or "").strip() or None,
            "market_ticker": ticker,
            "contract_family": contract_family,
            "strip_key": strip_key,
            "side": normalized_side,
            "contracts": contracts_effective,
            "entry_price_dollars": entry_price_effective,
            "entry_time_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
            "notional_risk_dollars": round(entry_price_effective * float(contracts_effective), 6),
            "hours_to_close": hours_to_close,
            "close_time_estimate_utc": close_time_estimate_utc,
            "availability_state": availability_state,
            "mark_price_dollars": entry_price_effective,
            "mark_time_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
            "mark_to_model_pnl_dollars": 0.0,
            "markout_10s_dollars": 0.0,
            "markout_60s_dollars": 0.0,
            "markout_300s_dollars": 0.0,
            "expected_value_dollars": expected_value_effective,
            "fair_yes_probability": fair_yes_effective,
            "edge_net": round(float(edge_net), 6),
            "source_strategy": str(source_strategy or "").strip() or None,
            "opportunity_class": str(opportunity_class or "").strip() or None,
            "fill_source": fill_source,
            "partial_fill": bool(partial_fill),
            "fill_time_seconds": round(fill_seconds_effective, 6),
        }
        orders_filled_total += 1
        run_filled += 1
        if partial_fill:
            orders_partial_filled_total += 1
            run_partial_filled += 1
        expected_value_total += expected_value_effective
        fill_events_count += 1
        fill_time_seconds_total += fill_seconds_effective
        _update_attempt_event_status(
            attempt_event_key=attempt_event_key,
            status="filled",
            fill_time_seconds=fill_seconds_effective,
            partial_fill=bool(partial_fill),
            event_time_utc=run_finished_at_dt.astimezone(timezone.utc).isoformat(),
        )

    if run_is_new:
        # Advance existing resting synthetic orders first.
        remaining_resting: list[dict[str, Any]] = []
        for key, order in resting_by_key.items():
            ticker = str(order.get("market_ticker") or "").strip().upper()
            side = str(order.get("side") or "yes").strip().lower()
            if side not in {"yes", "no"}:
                side = "yes"
            contracts = max(1, _coerce_int(order.get("contracts"), 1))
            reference_price = float(_parse_float(order.get("entry_price_dollars")) or 0.0)
            expected_value = float(_parse_float(order.get("expected_value_dollars")) or 0.0)
            fair_yes_probability = _parse_float(order.get("fair_yes_probability"))
            edge_net = float(_parse_float(order.get("edge_net")) or 0.0)
            submitted_at_dt = _parse_iso(order.get("submitted_at_utc"))
            close_estimate_dt = _parse_iso(order.get("close_time_estimate_utc"))
            attempt_event_key = str(order.get("attempt_event_key") or "").strip()
            if not attempt_event_key:
                fallback_stamp = (
                    submitted_at_dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S")
                    if isinstance(submitted_at_dt, datetime)
                    else run_finished_at_dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S")
                )
                attempt_event_key = f"paper_live_attempt::{key}::{fallback_stamp}"
            _ensure_attempt_event(
                attempt_event_key=attempt_event_key,
                run_id_value=str(order.get("run_id") or run_id).strip() or run_id,
                attempted_at_utc=(
                    submitted_at_dt.astimezone(timezone.utc).isoformat()
                    if isinstance(submitted_at_dt, datetime)
                    else run_finished_at_dt.astimezone(timezone.utc).isoformat()
                ),
                market_ticker=ticker,
                contract_family=str(order.get("contract_family") or "").strip().lower() or None,
                strip_key=str(order.get("strip_key") or "").strip() or None,
                side=side,
                contracts=contracts,
                notional_risk_dollars=float(reference_price) * float(contracts),
                expected_value_dollars=expected_value,
                source_strategy=str(order.get("source_strategy") or "").strip() or "climate_router_pilot",
                opportunity_class=str(order.get("opportunity_class") or "").strip() or None,
                order_id=str(order.get("order_id") or "").strip() or None,
                status="resting",
            )
            if isinstance(close_estimate_dt, datetime) and run_finished_at_dt >= close_estimate_dt:
                orders_expired_total += 1
                run_expired += 1
                _update_attempt_event_status(
                    attempt_event_key=attempt_event_key,
                    status="expired",
                    event_time_utc=run_finished_at_dt.astimezone(timezone.utc).isoformat(),
                )
                continue
            score = _paper_live_deterministic_score(run_id, key, "resting_progress")
            partial_prob = 0.15
            fill_prob = 0.35
            cancel_prob = 0.2 if normalized_allow_random_cancels else 0.0
            fill_time_seconds = (
                max(0.0, (run_finished_at_dt - submitted_at_dt).total_seconds())
                if isinstance(submitted_at_dt, datetime)
                else 0.0
            )
            if score < partial_prob:
                _append_filled_position(
                    key=key,
                    attempt_event_key=attempt_event_key,
                    ticker=ticker,
                    side=side,
                    contract_family=str(order.get("contract_family") or "").strip() or None,
                    strip_key=str(order.get("strip_key") or "").strip() or None,
                    entry_price=reference_price,
                    contracts=contracts,
                    expected_value_dollars=expected_value,
                    fill_source="resting_partial_fill",
                    fill_time_seconds=fill_time_seconds,
                    hours_to_close=_parse_float(order.get("hours_to_close")),
                    close_time_estimate_utc=str(order.get("close_time_estimate_utc") or "").strip() or None,
                    availability_state=str(order.get("availability_state") or "").strip() or None,
                    fair_yes_probability=fair_yes_probability,
                    edge_net=edge_net,
                    source_strategy=str(order.get("source_strategy") or "").strip() or "climate_router_pilot",
                    opportunity_class=str(order.get("opportunity_class") or "").strip() or None,
                    partial_fill=True,
                )
            elif score < partial_prob + fill_prob:
                _append_filled_position(
                    key=key,
                    attempt_event_key=attempt_event_key,
                    ticker=ticker,
                    side=side,
                    contract_family=str(order.get("contract_family") or "").strip() or None,
                    strip_key=str(order.get("strip_key") or "").strip() or None,
                    entry_price=reference_price,
                    contracts=contracts,
                    expected_value_dollars=expected_value,
                    fill_source="resting_progress",
                    fill_time_seconds=fill_time_seconds,
                    hours_to_close=_parse_float(order.get("hours_to_close")),
                    close_time_estimate_utc=str(order.get("close_time_estimate_utc") or "").strip() or None,
                    availability_state=str(order.get("availability_state") or "").strip() or None,
                    fair_yes_probability=fair_yes_probability,
                    edge_net=edge_net,
                    source_strategy=str(order.get("source_strategy") or "").strip() or "climate_router_pilot",
                    opportunity_class=str(order.get("opportunity_class") or "").strip() or None,
                    partial_fill=False,
                )
            elif normalized_allow_random_cancels and score < partial_prob + fill_prob + cancel_prob:
                canceled_order = dict(order)
                canceled_order["canceled_at_utc"] = run_finished_at_dt.astimezone(timezone.utc).isoformat()
                canceled_order["cancel_reason"] = "resting_timeout_simulated"
                orders_canceled_total += 1
                run_canceled += 1
                _update_attempt_event_status(
                    attempt_event_key=attempt_event_key,
                    status="canceled",
                    event_time_utc=run_finished_at_dt.astimezone(timezone.utc).isoformat(),
                )
            else:
                order["attempt_event_key"] = attempt_event_key
                remaining_resting.append(order)
        resting_by_key = {
            _shadow_position_key(market_ticker=str(item.get("market_ticker") or ""), side=str(item.get("side") or "")): item
            for item in remaining_resting
            if _shadow_position_key(market_ticker=str(item.get("market_ticker") or ""), side=str(item.get("side") or ""))
        }

        # Recompute execution quality state after resting-order lifecycle progression.
        family_quality_rows, ticker_quality_rows = _build_execution_quality_rows(
            attempts_data=attempt_events,
            open_positions_data=[dict(item) for item in open_by_key.values() if isinstance(item, dict)],
            closed_positions_data=[dict(item) for item in positions_closed if isinstance(item, dict)],
        )
        family_execution_state_map = {
            key: str(value.get("state") or "warmup")
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_execution_state_map = {
            key: str(value.get("state") or "warmup")
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }
        family_risk_multiplier_map = {
            key: round(float(_parse_float(value.get("risk_multiplier")) or 1.0), 6)
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_risk_multiplier_map = {
            key: round(float(_parse_float(value.get("risk_multiplier")) or 1.0), 6)
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }
        family_fill_rate_map = {
            key: _parse_float(value.get("fill_rate"))
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_fill_rate_map = {
            key: _parse_float(value.get("fill_rate"))
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }
        family_cancel_rate_map = {
            key: _parse_float(value.get("cancel_rate"))
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_cancel_rate_map = {
            key: _parse_float(value.get("cancel_rate"))
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }
        family_mtm_per_risk_pct_map = {
            key: _parse_float(value.get("mtm_per_risk_pct"))
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_mtm_per_risk_pct_map = {
            key: _parse_float(value.get("mtm_per_risk_pct"))
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }
        family_markout_300_mean_map = {
            key: _parse_float(value.get("markout_300s_mean_dollars"))
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_markout_300_mean_map = {
            key: _parse_float(value.get("markout_300s_mean_dollars"))
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }
        family_markout_300_per_contract_mean_map = {
            key: _parse_float(value.get("markout_300s_mean_per_contract_dollars"))
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_markout_300_per_contract_mean_map = {
            key: _parse_float(value.get("markout_300s_mean_per_contract_dollars"))
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }
        family_markout_300_per_risk_pct_map = {
            key: _parse_float(value.get("markout_300s_per_risk_pct"))
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_markout_300_per_risk_pct_map = {
            key: _parse_float(value.get("markout_300s_per_risk_pct"))
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }
        family_markout_300_per_contract_map = {
            key: _parse_float(value.get("markout_300s_per_contract"))
            for key, value in family_quality_rows.items()
            if isinstance(value, dict)
        }
        ticker_markout_300_per_contract_map = {
            key: _parse_float(value.get("markout_300s_per_contract"))
            for key, value in ticker_quality_rows.items()
            if isinstance(value, dict)
        }

        # Growth-mode paper-live submission: risk-budget limited, family/strip capped, liquidity-aware.
        live_eligible_hint = _coerce_int(pilot.get("would_attempt_live_if_enabled"), 0) > 0
        can_attempt_live_path = bool(candidate_pool) and (
            live_eligible_hint or not normalized_require_live_eligible_hint
        )
        for candidate in candidate_pool:
            if run_attempts >= run_attempt_limit:
                break
            if not can_attempt_live_path:
                break

            ticker = str(candidate.get("market_ticker") or "").strip().upper()
            family = str(candidate.get("contract_family") or "").strip().lower()
            side = str(candidate.get("theoretical_side") or "yes").strip().lower()
            if side not in {"yes", "no"}:
                side = "yes"
            key = _shadow_position_key(market_ticker=ticker, side=side) if ticker else ""
            if not key or family not in set(normalized_family_allowlist):
                continue
            if key in open_by_key or key in resting_by_key:
                continue

            family_multiplier = float(family_risk_multiplier_map.get(family, 1.0))
            ticker_multiplier = float(ticker_risk_multiplier_map.get(ticker, 1.0))
            execution_risk_multiplier = max(0.0, min(family_multiplier, ticker_multiplier))
            if execution_risk_multiplier <= 0.0:
                continue

            reference_price = float(_parse_float(candidate.get("theoretical_reference_price")) or 0.0)
            reference_price = round(max(0.01, min(0.99, reference_price)), 6)
            edge_net = float(_parse_float(candidate.get("theoretical_edge_net")) or 0.0)
            tier_label, tier_risk_pct = _paper_live_tier_targets(edge_net)
            if tier_risk_pct <= 0.0:
                continue

            availability_state = str(candidate.get("availability_state") or "").strip().lower() or None
            strip_key = str(candidate.get("strip_key") or "").strip() or None
            strip_budget_key = strip_key or "__no_strip__"
            fair_yes_probability = _parse_float(candidate.get("fair_yes_probability"))
            hours_to_close = _parse_float(candidate.get("hours_to_close"))
            close_estimate = _shadow_close_time_estimate(as_of=run_finished_at_dt, hours_to_close=hours_to_close)
            source_strategy = str(candidate.get("source_strategy") or "").strip() or "climate_router_pilot"
            opportunity_class = str(candidate.get("opportunity_class") or "").strip() or None
            suggested_risk_dollars = float(_parse_float(candidate.get("suggested_risk_dollars")) or 0.0)

            kelly_used = normalized_kelly_fraction
            if tier_label == "A+" and availability_state in {"tradable", "hot"}:
                kelly_used = normalized_kelly_high_conf_max
            kelly_used = float(kelly_used) * execution_risk_multiplier
            kelly_used = max(0.0, min(1.0, float(kelly_used)))
            if kelly_used <= 0.0:
                continue
            per_attempt_kelly_used.append(kelly_used)

            target_risk_pct = tier_risk_pct * (kelly_used / 0.5 if 0.5 > 0 else 1.0) * drawdown_risk_scale
            target_risk_dollars = float(sizing_balance_dollars) * max(0.0, target_risk_pct)

            remaining_total_risk = max(0.0, open_risk_cap_dollars - open_risk_total)
            effective_family_risk_cap_dollars = max(0.0, family_risk_cap_dollars * family_multiplier)
            remaining_family_risk = max(
                0.0,
                effective_family_risk_cap_dollars - float(open_risk_by_family.get(family, 0.0)),
            )
            remaining_strip_risk = max(0.0, strip_risk_cap_dollars - float(open_risk_by_strip.get(strip_budget_key, 0.0)))

            liquidity_cap_pct = 0.08
            if availability_state in {"hot"}:
                liquidity_cap_pct = 0.10
            elif availability_state in {"priced_watch_only"}:
                liquidity_cap_pct = 0.035
            elif availability_state in {"watch_only", "watch", "endpoint_only"}:
                liquidity_cap_pct = 0.02
            state_liquidity_cap = float(sizing_balance_dollars) * liquidity_cap_pct
            baseline_liquidity = max(reference_price, suggested_risk_dollars)
            liquidity_cap_dollars = max(
                reference_price,
                min(state_liquidity_cap, max(state_liquidity_cap * 0.6, baseline_liquidity * 8.0)),
            )

            allowed_risk_dollars = min(
                target_risk_dollars,
                single_position_risk_cap_dollars,
                remaining_total_risk,
                remaining_family_risk,
                remaining_strip_risk,
                liquidity_cap_dollars,
            )
            if allowed_risk_dollars < reference_price:
                continue

            contracts = max(1, int(allowed_risk_dollars / reference_price))
            notional_risk_dollars = round(reference_price * float(contracts), 6)
            if notional_risk_dollars <= 0.0:
                continue

            expected_value_per_contract = float(_parse_float(candidate.get("expected_value_dollars")) or 0.0)
            if expected_value_per_contract <= 0.0:
                expected_value_per_contract = max(0.0, edge_net * reference_price)
            expected_value_dollars = round(float(expected_value_per_contract) * float(contracts), 6)

            order_attempts_total += 1
            run_attempts += 1
            attempted_at_utc = run_finished_at_dt.astimezone(timezone.utc).isoformat()
            score = _paper_live_deterministic_score(run_id, key, "initial_attempt_growth")
            size_pressure = max(0.0, min(1.0, notional_risk_dollars / max(1.0, liquidity_cap_dollars)))
            partial_prob = max(0.05, min(0.35, 0.08 + max(0.0, edge_net) * 0.4 + size_pressure * 0.05))
            fill_prob = max(0.10, min(0.85, 0.55 + max(0.0, edge_net) * 1.5 - size_pressure * 0.35))
            cancel_prob = (
                max(0.05, min(0.60, 0.12 + size_pressure * 0.25))
                if normalized_allow_random_cancels
                else 0.0
            )
            total_prob = partial_prob + fill_prob + cancel_prob
            if total_prob > 0.95:
                scale = 0.95 / total_prob
                partial_prob *= scale
                fill_prob *= scale
                cancel_prob *= scale
            attempt_event_key = f"paper_live_attempt::{key}::{run_id}"

            _ensure_attempt_event(
                attempt_event_key=attempt_event_key,
                run_id_value=run_id,
                attempted_at_utc=attempted_at_utc,
                market_ticker=ticker,
                contract_family=family or None,
                strip_key=strip_key,
                side=side,
                contracts=contracts,
                notional_risk_dollars=notional_risk_dollars,
                expected_value_dollars=expected_value_dollars,
                source_strategy=source_strategy,
                opportunity_class=opportunity_class,
                order_id=f"paper_live::{key}::{run_finished_at_dt.strftime('%Y%m%d%H%M%S')}",
                status="attempted",
            )

            if score < partial_prob:
                _append_filled_position(
                    key=key,
                    attempt_event_key=attempt_event_key,
                    ticker=ticker,
                    side=side,
                    contract_family=family or None,
                    strip_key=strip_key,
                    entry_price=reference_price,
                    contracts=contracts,
                    expected_value_dollars=expected_value_dollars,
                    fill_source="initial_partial_fill",
                    fill_time_seconds=0.0,
                    hours_to_close=hours_to_close,
                    close_time_estimate_utc=close_estimate,
                    availability_state=availability_state,
                    fair_yes_probability=fair_yes_probability,
                    edge_net=edge_net,
                    source_strategy=source_strategy,
                    opportunity_class=opportunity_class,
                    partial_fill=True,
                )
                open_risk_total += notional_risk_dollars
                open_risk_by_family[family] = float(open_risk_by_family.get(family, 0.0)) + notional_risk_dollars
                open_risk_by_strip[strip_budget_key] = float(open_risk_by_strip.get(strip_budget_key, 0.0)) + notional_risk_dollars
            elif score < partial_prob + fill_prob:
                _append_filled_position(
                    key=key,
                    attempt_event_key=attempt_event_key,
                    ticker=ticker,
                    side=side,
                    contract_family=family or None,
                    strip_key=strip_key,
                    entry_price=reference_price,
                    contracts=contracts,
                    expected_value_dollars=expected_value_dollars,
                    fill_source="initial_attempt",
                    fill_time_seconds=0.0,
                    hours_to_close=hours_to_close,
                    close_time_estimate_utc=close_estimate,
                    availability_state=availability_state,
                    fair_yes_probability=fair_yes_probability,
                    edge_net=edge_net,
                    source_strategy=source_strategy,
                    opportunity_class=opportunity_class,
                    partial_fill=False,
                )
                open_risk_total += notional_risk_dollars
                open_risk_by_family[family] = float(open_risk_by_family.get(family, 0.0)) + notional_risk_dollars
                open_risk_by_strip[strip_budget_key] = float(open_risk_by_strip.get(strip_budget_key, 0.0)) + notional_risk_dollars
            elif normalized_allow_random_cancels and score < partial_prob + fill_prob + cancel_prob:
                orders_canceled_total += 1
                run_canceled += 1
                _update_attempt_event_status(
                    attempt_event_key=attempt_event_key,
                    status="canceled",
                    event_time_utc=attempted_at_utc,
                )
            else:
                resting_by_key[key] = {
                    "attempt_event_key": attempt_event_key,
                    "run_id": run_id,
                    "order_id": f"paper_live::{key}::{run_finished_at_dt.strftime('%Y%m%d%H%M%S')}",
                    "market_ticker": ticker,
                    "contract_family": family,
                    "strip_key": strip_key,
                    "side": side,
                    "contracts": contracts,
                    "entry_price_dollars": round(reference_price, 6),
                    "submitted_at_utc": attempted_at_utc,
                    "hours_to_close": hours_to_close,
                    "close_time_estimate_utc": close_estimate,
                    "availability_state": availability_state,
                    "expected_value_dollars": expected_value_dollars,
                    "fair_yes_probability": fair_yes_probability,
                    "edge_net": round(edge_net, 6),
                    "source_strategy": source_strategy,
                    "opportunity_class": opportunity_class,
                    "risk_tier": tier_label,
                    "kelly_fraction_used": round(float(kelly_used), 6),
                }
                _update_attempt_event_status(
                    attempt_event_key=attempt_event_key,
                    status="resting",
                    event_time_utc=attempted_at_utc,
                )
                open_risk_total += notional_risk_dollars
                open_risk_by_family[family] = float(open_risk_by_family.get(family, 0.0)) + notional_risk_dollars
                open_risk_by_strip[strip_budget_key] = float(open_risk_by_strip.get(strip_budget_key, 0.0)) + notional_risk_dollars

    # Mark-to-model / synthetic close handling on open positions.
    updated_open_positions: list[dict[str, Any]] = []
    closed_positions_history: list[dict[str, Any]] = [dict(item) for item in positions_closed if isinstance(item, dict)]
    markout_10s_total = 0.0
    markout_60s_total = 0.0
    markout_300s_total = 0.0
    for key, position in open_by_key.items():
        entry_price = float(_parse_float(position.get("entry_price_dollars")) or 0.0)
        contracts = max(1, _coerce_int(position.get("contracts"), 1))
        side = str(position.get("side") or "yes").strip().lower()
        if side not in {"yes", "no"}:
            side = "yes"
        edge_net = float(_parse_float(position.get("edge_net")) or 0.0)
        fair_yes_probability = _parse_float(position.get("fair_yes_probability"))
        expected_value_position = float(_parse_float(position.get("expected_value_dollars")) or 0.0)
        close_estimate = _parse_iso(position.get("close_time_estimate_utc"))
        markout_10s = _paper_live_markout_pnl(
            position_key=key,
            side=side,
            entry_price=entry_price,
            edge_net=edge_net,
            fair_yes_probability=fair_yes_probability,
            horizon_seconds=10,
        ) * float(contracts)
        markout_60s = _paper_live_markout_pnl(
            position_key=key,
            side=side,
            entry_price=entry_price,
            edge_net=edge_net,
            fair_yes_probability=fair_yes_probability,
            horizon_seconds=60,
        ) * float(contracts)
        markout_300s = _paper_live_markout_pnl(
            position_key=key,
            side=side,
            entry_price=entry_price,
            edge_net=edge_net,
            fair_yes_probability=fair_yes_probability,
            horizon_seconds=300,
        ) * float(contracts)
        markout_10s_total += float(markout_10s)
        markout_60s_total += float(markout_60s)
        markout_300s_total += float(markout_300s)

        mark_price = round(entry_price + (float(markout_300s) / float(max(1, contracts))), 6)
        mark_to_model = float(markout_300s)

        if isinstance(close_estimate, datetime) and run_finished_at_dt >= close_estimate:
            settlement_pnl, settlement_outcome = _paper_live_settlement_pnl(
                position_key=key,
                side=side,
                entry_price=entry_price,
                fair_yes_probability=fair_yes_probability,
                contracts=contracts,
            )
            settlement_price = 1.0 if (
                (side == "yes" and settlement_outcome == "yes")
                or (side == "no" and settlement_outcome == "no")
            ) else 0.0
            closed = dict(position)
            closed["exit_time_utc"] = run_finished_at_dt.astimezone(timezone.utc).isoformat()
            closed["exit_price_dollars"] = round(float(settlement_price), 6)
            closed["close_reason"] = "market_settlement_estimate"
            closed["settlement_outcome"] = settlement_outcome
            closed["settlement_pnl_dollars"] = round(float(settlement_pnl), 6)
            closed["realized_trade_pnl_dollars"] = round(float(settlement_pnl), 6)
            closed["markout_10s_dollars"] = round(float(markout_10s), 6)
            closed["markout_60s_dollars"] = round(float(markout_60s), 6)
            closed["markout_300s_dollars"] = round(float(markout_300s), 6)
            realized_trade_total += float(settlement_pnl)
            settlement_pnl_total += float(settlement_pnl)
            settlement_expected_value_total += float(expected_value_position)
            closed_positions_history.append(closed)
            continue

        updated = dict(position)
        updated["mark_price_dollars"] = mark_price
        updated["mark_time_utc"] = run_finished_at_dt.astimezone(timezone.utc).isoformat()
        updated["mark_to_model_pnl_dollars"] = round(float(mark_to_model), 6)
        updated["markout_10s_dollars"] = round(float(markout_10s), 6)
        updated["markout_60s_dollars"] = round(float(markout_60s), 6)
        updated["markout_300s_dollars"] = round(float(markout_300s), 6)
        updated_open_positions.append(updated)

    max_closed_positions = 250
    if len(closed_positions_history) > max_closed_positions:
        closed_positions_history = closed_positions_history[-max_closed_positions:]

    mark_to_market_total = 0.0
    for position in updated_open_positions:
        mark_to_market_total += float(_parse_float(position.get("mark_to_model_pnl_dollars")) or 0.0)

    current_balance = float(start) + float(realized_trade_total) + float(mark_to_market_total)
    post_trade_sizing_balance_dollars = (
        float(current_balance) if normalized_size_from_current_equity else float(start)
    )
    if post_trade_sizing_balance_dollars <= 0.0:
        post_trade_sizing_balance_dollars = float(start)
    post_trade_sizing_balance_dollars = max(0.0, float(post_trade_sizing_balance_dollars))
    peak_balance = max(float(peak_balance), float(start), float(current_balance))
    drawdown_pct = ((peak_balance - current_balance) / peak_balance * 100.0) if peak_balance > 0.0 else 0.0
    drawdown_risk_scale, drawdown_throttle_state = _paper_live_drawdown_scale(drawdown_pct)

    open_risk_by_family_current: dict[str, float] = {}
    open_risk_by_strip_current: dict[str, float] = {}
    open_risk_total_current = 0.0
    for position in updated_open_positions:
        if not isinstance(position, dict):
            continue
        risk = _risk_dollars_from_payload(position)
        if risk <= 0.0:
            continue
        family = str(position.get("contract_family") or "unknown_family").strip().lower() or "unknown_family"
        strip_key = str(position.get("strip_key") or "").strip() or "__no_strip__"
        open_risk_total_current += risk
        open_risk_by_family_current[family] = float(open_risk_by_family_current.get(family, 0.0)) + risk
        open_risk_by_strip_current[strip_key] = float(open_risk_by_strip_current.get(strip_key, 0.0)) + risk
    for order in resting_by_key.values():
        if not isinstance(order, dict):
            continue
        risk = _risk_dollars_from_payload(order)
        if risk <= 0.0:
            continue
        family = str(order.get("contract_family") or "unknown_family").strip().lower() or "unknown_family"
        strip_key = str(order.get("strip_key") or "").strip() or "__no_strip__"
        open_risk_total_current += risk
        open_risk_by_family_current[family] = float(open_risk_by_family_current.get(family, 0.0)) + risk
        open_risk_by_strip_current[strip_key] = float(open_risk_by_strip_current.get(strip_key, 0.0)) + risk

    open_risk_cap_dollars = max(
        0.0, float(post_trade_sizing_balance_dollars) * normalized_max_open_risk_pct * drawdown_risk_scale
    )
    family_risk_cap_dollars = max(
        0.0, float(post_trade_sizing_balance_dollars) * normalized_max_family_risk_pct * drawdown_risk_scale
    )
    strip_risk_cap_dollars = max(
        0.0, float(post_trade_sizing_balance_dollars) * normalized_max_strip_risk_pct * drawdown_risk_scale
    )
    open_risk_remaining_dollars = max(0.0, open_risk_cap_dollars - open_risk_total_current)

    family_open_risk_remaining_dollars: dict[str, float] = {}
    for family in set([*normalized_family_allowlist, *open_risk_by_family_current.keys()]):
        family_open_risk_remaining_dollars[family] = round(
            max(0.0, family_risk_cap_dollars - float(open_risk_by_family_current.get(family, 0.0))),
            6,
        )
    strip_open_risk_remaining_dollars: dict[str, float] = {}
    for strip_key, risk_value in open_risk_by_strip_current.items():
        strip_open_risk_remaining_dollars[strip_key] = round(
            max(0.0, strip_risk_cap_dollars - float(risk_value)),
            6,
        )
    fill_time_seconds = (
        round(float(fill_time_seconds_total) / float(fill_events_count), 6)
        if fill_events_count > 0
        else None
    )
    expected_vs_realized_delta = float(settlement_pnl_total) - float(settlement_expected_value_total)

    equity_point = {
        "run_id": run_id,
        "as_of_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
        "equity_dollars": round(float(current_balance), 6),
        "realized_trade_pnl_dollars": round(float(realized_trade_total), 6),
        "mark_to_market_pnl_dollars": round(float(mark_to_market_total), 6),
        "positions_open": len(updated_open_positions),
        "positions_closed": len(closed_positions_history),
    }
    if run_is_new:
        equity_curve.append(equity_point)
    elif equity_curve:
        equity_curve[-1] = equity_point
    else:
        equity_curve.append(equity_point)
    if len(equity_curve) > 500:
        equity_curve = equity_curve[-500:]

    selected_tickers: list[str] = []
    if selected_ticker:
        selected_tickers.append(selected_ticker)
    for candidate in candidate_pool:
        ticker = str(candidate.get("market_ticker") or "").strip().upper()
        if ticker and ticker not in selected_tickers:
            selected_tickers.append(ticker)
    raw_selected_tickers = pilot.get("selected_tickers")
    if isinstance(raw_selected_tickers, list):
        for raw_ticker in raw_selected_tickers:
            ticker = str(raw_ticker or "").strip().upper()
            if ticker and ticker not in selected_tickers:
                selected_tickers.append(ticker)

    max_attempt_events = 2000
    if len(attempt_events) > max_attempt_events:
        attempt_events = attempt_events[-max_attempt_events:]

    snapshot.update(
        {
            "status": "observer_ready",
            "reason": "paper_live_simulated",
            "execution_basis": "paper_live_balance",
            "risk_profile": normalized_risk_profile,
            "family_allowlist": normalized_family_allowlist,
            "allow_random_cancels": normalized_allow_random_cancels,
            "size_from_current_equity": normalized_size_from_current_equity,
            "require_live_eligible_hint": normalized_require_live_eligible_hint,
            "kelly_fraction": round(float(normalized_kelly_fraction), 6),
            "kelly_high_conf_max": round(float(normalized_kelly_high_conf_max), 6),
            "max_open_risk_pct": round(float(normalized_max_open_risk_pct), 6),
            "max_family_risk_pct": round(float(normalized_max_family_risk_pct), 6),
            "max_strip_risk_pct": round(float(normalized_max_strip_risk_pct), 6),
            "max_single_position_risk_pct": round(float(normalized_max_single_position_risk_pct), 6),
            "max_new_attempts_per_run": int(normalized_max_new_attempts_per_run),
            "run_attempt_limit": int(run_attempt_limit),
            "used_kelly_fraction": round(float(per_attempt_kelly_used[-1]), 6) if per_attempt_kelly_used else 0.0,
            "avg_kelly_fraction_used": (
                round(sum(float(value) for value in per_attempt_kelly_used) / float(len(per_attempt_kelly_used)), 6)
                if per_attempt_kelly_used
                else 0.0
            ),
            "start_dollars": round(float(start), 4),
            "current_dollars": round(float(current_balance), 4),
            "sizing_balance_dollars": round(float(sizing_balance_dollars), 4),
            "post_trade_sizing_balance_dollars": round(float(post_trade_sizing_balance_dollars), 4),
            "realized_trade_pnl_dollars": round(float(realized_trade_total), 4),
            "mark_to_market_pnl_dollars": round(float(mark_to_market_total), 4),
            "drawdown_pct": round(max(0.0, float(drawdown_pct)), 4),
            "drawdown_throttle_state": drawdown_throttle_state,
            "drawdown_risk_scale": round(float(drawdown_risk_scale), 6),
            "positions_open": updated_open_positions,
            "positions_closed": closed_positions_history,
            "positions_open_count": len(updated_open_positions),
            "positions_closed_count": len(closed_positions_history),
            "open_risk_dollars": round(float(open_risk_total_current), 6),
            "open_risk_cap_dollars": round(float(open_risk_cap_dollars), 6),
            "open_risk_remaining_dollars": round(float(open_risk_remaining_dollars), 6),
            "family_open_risk_dollars": {
                key: round(float(value), 6)
                for key, value in sorted(open_risk_by_family_current.items(), key=lambda item: item[0])
            },
            "family_open_risk_remaining_dollars": family_open_risk_remaining_dollars,
            "strip_open_risk_dollars": {
                key: round(float(value), 6)
                for key, value in sorted(open_risk_by_strip_current.items(), key=lambda item: item[0])
            },
            "strip_open_risk_remaining_dollars": strip_open_risk_remaining_dollars,
            "family_execution_state": {
                key: str(value)
                for key, value in sorted(family_execution_state_map.items(), key=lambda item: item[0])
            },
            "ticker_execution_state": {
                key: str(value)
                for key, value in sorted(ticker_execution_state_map.items(), key=lambda item: item[0])
            },
            "family_mtm_per_risk_pct": {
                key: _parse_float(value)
                for key, value in sorted(family_mtm_per_risk_pct_map.items(), key=lambda item: item[0])
            },
            "ticker_mtm_per_risk_pct": {
                key: _parse_float(value)
                for key, value in sorted(ticker_mtm_per_risk_pct_map.items(), key=lambda item: item[0])
            },
            "family_markout_300s_mean_dollars": {
                key: _parse_float(value)
                for key, value in sorted(family_markout_300_mean_map.items(), key=lambda item: item[0])
            },
            "ticker_markout_300s_mean_dollars": {
                key: _parse_float(value)
                for key, value in sorted(ticker_markout_300_mean_map.items(), key=lambda item: item[0])
            },
            "family_markout_300s_mean_per_contract_dollars": {
                key: _parse_float(value)
                for key, value in sorted(family_markout_300_per_contract_mean_map.items(), key=lambda item: item[0])
            },
            "ticker_markout_300s_mean_per_contract_dollars": {
                key: _parse_float(value)
                for key, value in sorted(ticker_markout_300_per_contract_mean_map.items(), key=lambda item: item[0])
            },
            "family_markout_300s_per_risk_pct": {
                key: _parse_float(value)
                for key, value in sorted(family_markout_300_per_risk_pct_map.items(), key=lambda item: item[0])
            },
            "ticker_markout_300s_per_risk_pct": {
                key: _parse_float(value)
                for key, value in sorted(ticker_markout_300_per_risk_pct_map.items(), key=lambda item: item[0])
            },
            "family_markout_300s_per_contract": {
                key: _parse_float(value)
                for key, value in sorted(family_markout_300_per_contract_map.items(), key=lambda item: item[0])
            },
            "ticker_markout_300s_per_contract": {
                key: _parse_float(value)
                for key, value in sorted(ticker_markout_300_per_contract_map.items(), key=lambda item: item[0])
            },
            "family_fill_rate": {
                key: _parse_float(value)
                for key, value in sorted(family_fill_rate_map.items(), key=lambda item: item[0])
            },
            "ticker_fill_rate": {
                key: _parse_float(value)
                for key, value in sorted(ticker_fill_rate_map.items(), key=lambda item: item[0])
            },
            "family_cancel_rate": {
                key: _parse_float(value)
                for key, value in sorted(family_cancel_rate_map.items(), key=lambda item: item[0])
            },
            "ticker_cancel_rate": {
                key: _parse_float(value)
                for key, value in sorted(ticker_cancel_rate_map.items(), key=lambda item: item[0])
            },
            "family_risk_multiplier": {
                key: round(float(value), 6)
                for key, value in sorted(family_risk_multiplier_map.items(), key=lambda item: item[0])
            },
            "ticker_risk_multiplier": {
                key: round(float(value), 6)
                for key, value in sorted(ticker_risk_multiplier_map.items(), key=lambda item: item[0])
            },
            "order_attempts": int(order_attempts_total),
            "orders_partial_filled": int(orders_partial_filled_total),
            "orders_resting": len(resting_by_key),
            "orders_filled": int(orders_filled_total),
            "orders_canceled": int(orders_canceled_total),
            "orders_expired": int(orders_expired_total),
            "fill_time_seconds": fill_time_seconds,
            "markout_10s_dollars": round(float(markout_10s_total), 6),
            "markout_60s_dollars": round(float(markout_60s_total), 6),
            "markout_300s_dollars": round(float(markout_300s_total), 6),
            "settlement_pnl_dollars": round(float(settlement_pnl_total), 6),
            "expected_value_dollars": round(float(expected_value_total), 6),
            "expected_vs_realized_delta": round(float(expected_vs_realized_delta), 6),
            "order_attempts_run": int(run_attempts),
            "orders_filled_run": int(run_filled),
            "orders_partial_filled_run": int(run_partial_filled),
            "orders_canceled_run": int(run_canceled),
            "orders_expired_run": int(run_expired),
            "selected_tickers": selected_tickers,
            "attempt_events": attempt_events,
            "equity_curve": equity_curve,
            "last_updated_at_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
            "accounting_version": 2,
            "source": "synthetic_paper_live",
        }
    )

    state_payload = {
        "last_run_id": run_id,
        "updated_at_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
        "paper_live_execution_basis": snapshot.get("execution_basis"),
        "paper_live_risk_profile": snapshot.get("risk_profile"),
        "paper_live_kelly_fraction": snapshot.get("kelly_fraction"),
        "paper_live_kelly_high_conf_max": snapshot.get("kelly_high_conf_max"),
        "paper_live_max_open_risk_pct": snapshot.get("max_open_risk_pct"),
        "paper_live_max_family_risk_pct": snapshot.get("max_family_risk_pct"),
        "paper_live_max_strip_risk_pct": snapshot.get("max_strip_risk_pct"),
        "paper_live_max_single_position_risk_pct": snapshot.get("max_single_position_risk_pct"),
        "paper_live_max_new_attempts_per_run": snapshot.get("max_new_attempts_per_run"),
        "paper_live_run_attempt_limit": snapshot.get("run_attempt_limit"),
        "paper_live_family_allowlist": snapshot.get("family_allowlist"),
        "paper_live_allow_random_cancels": snapshot.get("allow_random_cancels"),
        "paper_live_size_from_current_equity": snapshot.get("size_from_current_equity"),
        "paper_live_require_live_eligible_hint": snapshot.get("require_live_eligible_hint"),
        "paper_live_used_kelly_fraction": snapshot.get("used_kelly_fraction"),
        "paper_live_avg_kelly_fraction_used": snapshot.get("avg_kelly_fraction_used"),
        "paper_live_sizing_balance_dollars": snapshot.get("sizing_balance_dollars"),
        "paper_live_post_trade_sizing_balance_dollars": snapshot.get("post_trade_sizing_balance_dollars"),
        "paper_live_open_risk_dollars": snapshot.get("open_risk_dollars"),
        "paper_live_open_risk_cap_dollars": snapshot.get("open_risk_cap_dollars"),
        "paper_live_open_risk_remaining_dollars": snapshot.get("open_risk_remaining_dollars"),
        "paper_live_family_open_risk_dollars": snapshot.get("family_open_risk_dollars"),
        "paper_live_family_open_risk_remaining_dollars": snapshot.get("family_open_risk_remaining_dollars"),
        "paper_live_strip_open_risk_dollars": snapshot.get("strip_open_risk_dollars"),
        "paper_live_strip_open_risk_remaining_dollars": snapshot.get("strip_open_risk_remaining_dollars"),
        "paper_live_family_execution_state": snapshot.get("family_execution_state"),
        "paper_live_ticker_execution_state": snapshot.get("ticker_execution_state"),
        "paper_live_family_mtm_per_risk_pct": snapshot.get("family_mtm_per_risk_pct"),
        "paper_live_ticker_mtm_per_risk_pct": snapshot.get("ticker_mtm_per_risk_pct"),
        "paper_live_family_markout_300s_mean_dollars": snapshot.get("family_markout_300s_mean_dollars"),
        "paper_live_ticker_markout_300s_mean_dollars": snapshot.get("ticker_markout_300s_mean_dollars"),
        "paper_live_family_markout_300s_mean_per_contract_dollars": snapshot.get(
            "family_markout_300s_mean_per_contract_dollars"
        ),
        "paper_live_ticker_markout_300s_mean_per_contract_dollars": snapshot.get(
            "ticker_markout_300s_mean_per_contract_dollars"
        ),
        "paper_live_family_markout_300s_per_risk_pct": snapshot.get("family_markout_300s_per_risk_pct"),
        "paper_live_ticker_markout_300s_per_risk_pct": snapshot.get("ticker_markout_300s_per_risk_pct"),
        "paper_live_family_markout_300s_per_contract": snapshot.get("family_markout_300s_per_contract"),
        "paper_live_ticker_markout_300s_per_contract": snapshot.get("ticker_markout_300s_per_contract"),
        "paper_live_family_fill_rate": snapshot.get("family_fill_rate"),
        "paper_live_ticker_fill_rate": snapshot.get("ticker_fill_rate"),
        "paper_live_family_cancel_rate": snapshot.get("family_cancel_rate"),
        "paper_live_ticker_cancel_rate": snapshot.get("ticker_cancel_rate"),
        "paper_live_family_risk_multiplier": snapshot.get("family_risk_multiplier"),
        "paper_live_ticker_risk_multiplier": snapshot.get("ticker_risk_multiplier"),
        "paper_live_drawdown_throttle_state": snapshot.get("drawdown_throttle_state"),
        "paper_live_drawdown_risk_scale": snapshot.get("drawdown_risk_scale"),
        "paper_live_balance_start_dollars": snapshot.get("start_dollars"),
        "paper_live_balance_current_dollars": snapshot.get("current_dollars"),
        "paper_live_strategy_equity_dollars": snapshot.get("current_dollars"),
        "paper_live_peak_balance_dollars": round(float(peak_balance), 4),
        "paper_live_realized_trade_pnl_dollars": snapshot.get("realized_trade_pnl_dollars"),
        "paper_live_mark_to_market_pnl_dollars": snapshot.get("mark_to_market_pnl_dollars"),
        "paper_live_drawdown_pct": snapshot.get("drawdown_pct"),
        "paper_live_strategy_drawdown_pct": snapshot.get("drawdown_pct"),
        "paper_live_positions_open": snapshot.get("positions_open"),
        "paper_live_positions_closed": snapshot.get("positions_closed"),
        "paper_live_positions_open_count": snapshot.get("positions_open_count"),
        "paper_live_positions_closed_count": snapshot.get("positions_closed_count"),
        "paper_live_resting_orders": [item for item in resting_by_key.values()],
        "paper_live_order_attempts": snapshot.get("order_attempts"),
        "paper_live_orders_filled": snapshot.get("orders_filled"),
        "paper_live_orders_partial_filled": snapshot.get("orders_partial_filled"),
        "paper_live_orders_canceled": snapshot.get("orders_canceled"),
        "paper_live_orders_expired": snapshot.get("orders_expired"),
        "paper_live_fill_time_seconds": snapshot.get("fill_time_seconds"),
        "paper_live_fill_time_seconds_total": round(float(fill_time_seconds_total), 6),
        "paper_live_fill_events_count": int(fill_events_count),
        "paper_live_markout_10s_dollars": snapshot.get("markout_10s_dollars"),
        "paper_live_markout_60s_dollars": snapshot.get("markout_60s_dollars"),
        "paper_live_markout_300s_dollars": snapshot.get("markout_300s_dollars"),
        "paper_live_settlement_pnl_dollars": snapshot.get("settlement_pnl_dollars"),
        "paper_live_expected_value_dollars": snapshot.get("expected_value_dollars"),
        "paper_live_settlement_expected_value_dollars": round(float(settlement_expected_value_total), 6),
        "paper_live_expected_vs_realized_delta": snapshot.get("expected_vs_realized_delta"),
        "paper_live_selected_tickers": snapshot.get("selected_tickers"),
        "paper_live_attempt_events": snapshot.get("attempt_events"),
        "paper_live_equity_curve": snapshot.get("equity_curve"),
        "paper_live_accounting_version": snapshot.get("accounting_version"),
    }
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps(state_payload, indent=2), encoding="utf-8")
    except Exception as exc:  # pragma: no cover - observer lane should never fail overnight
        snapshot["status"] = "observer_degraded"
        snapshot["reason"] = "paper_live_state_write_failed"
        snapshot["state_write_error"] = str(exc)

    return snapshot


def _paper_live_report_fields(paper_live: dict[str, Any] | None) -> dict[str, Any]:
    payload = paper_live if isinstance(paper_live, dict) else {}
    return {
        "paper_live_enabled": bool(payload.get("enabled")),
        "paper_live_status": payload.get("status"),
        "paper_live_reason": payload.get("reason"),
        "paper_live_execution_basis": payload.get("execution_basis") or "paper_live_balance",
        "paper_live_risk_profile": payload.get("risk_profile"),
        "paper_live_kelly_fraction": payload.get("kelly_fraction"),
        "paper_live_kelly_high_conf_max": payload.get("kelly_high_conf_max"),
        "paper_live_max_open_risk_pct": payload.get("max_open_risk_pct"),
        "paper_live_max_family_risk_pct": payload.get("max_family_risk_pct"),
        "paper_live_max_strip_risk_pct": payload.get("max_strip_risk_pct"),
        "paper_live_max_single_position_risk_pct": payload.get("max_single_position_risk_pct"),
        "paper_live_max_new_attempts_per_run": payload.get("max_new_attempts_per_run"),
        "paper_live_run_attempt_limit": payload.get("run_attempt_limit"),
        "paper_live_family_allowlist": payload.get("family_allowlist"),
        "paper_live_allow_random_cancels": payload.get("allow_random_cancels"),
        "paper_live_size_from_current_equity": payload.get("size_from_current_equity"),
        "paper_live_require_live_eligible_hint": payload.get("require_live_eligible_hint"),
        "paper_live_used_kelly_fraction": payload.get("used_kelly_fraction"),
        "paper_live_avg_kelly_fraction_used": payload.get("avg_kelly_fraction_used"),
        "paper_live_sizing_balance_dollars": payload.get("sizing_balance_dollars"),
        "paper_live_post_trade_sizing_balance_dollars": payload.get("post_trade_sizing_balance_dollars"),
        "paper_live_open_risk_dollars": payload.get("open_risk_dollars"),
        "paper_live_open_risk_cap_dollars": payload.get("open_risk_cap_dollars"),
        "paper_live_open_risk_remaining_dollars": payload.get("open_risk_remaining_dollars"),
        "paper_live_family_open_risk_dollars": payload.get("family_open_risk_dollars"),
        "paper_live_family_open_risk_remaining_dollars": payload.get("family_open_risk_remaining_dollars"),
        "paper_live_strip_open_risk_dollars": payload.get("strip_open_risk_dollars"),
        "paper_live_strip_open_risk_remaining_dollars": payload.get("strip_open_risk_remaining_dollars"),
        "paper_live_family_execution_state": payload.get("family_execution_state"),
        "paper_live_ticker_execution_state": payload.get("ticker_execution_state"),
        "paper_live_family_mtm_per_risk_pct": payload.get("family_mtm_per_risk_pct"),
        "paper_live_ticker_mtm_per_risk_pct": payload.get("ticker_mtm_per_risk_pct"),
        "paper_live_family_markout_300s_mean_dollars": payload.get("family_markout_300s_mean_dollars"),
        "paper_live_ticker_markout_300s_mean_dollars": payload.get("ticker_markout_300s_mean_dollars"),
        "paper_live_family_markout_300s_mean_per_contract_dollars": payload.get(
            "family_markout_300s_mean_per_contract_dollars"
        ),
        "paper_live_ticker_markout_300s_mean_per_contract_dollars": payload.get(
            "ticker_markout_300s_mean_per_contract_dollars"
        ),
        "paper_live_family_markout_300s_per_risk_pct": payload.get("family_markout_300s_per_risk_pct"),
        "paper_live_ticker_markout_300s_per_risk_pct": payload.get("ticker_markout_300s_per_risk_pct"),
        "paper_live_family_markout_300s_per_contract": payload.get("family_markout_300s_per_contract"),
        "paper_live_ticker_markout_300s_per_contract": payload.get("ticker_markout_300s_per_contract"),
        "paper_live_family_fill_rate": payload.get("family_fill_rate"),
        "paper_live_ticker_fill_rate": payload.get("ticker_fill_rate"),
        "paper_live_family_cancel_rate": payload.get("family_cancel_rate"),
        "paper_live_ticker_cancel_rate": payload.get("ticker_cancel_rate"),
        "paper_live_family_risk_multiplier": payload.get("family_risk_multiplier"),
        "paper_live_ticker_risk_multiplier": payload.get("ticker_risk_multiplier"),
        "paper_live_drawdown_throttle_state": payload.get("drawdown_throttle_state"),
        "paper_live_drawdown_risk_scale": payload.get("drawdown_risk_scale"),
        "paper_live_state_file": payload.get("state_file"),
        "paper_live_state_write_error": payload.get("state_write_error"),
        "paper_live_balance_start_dollars": payload.get("start_dollars"),
        "paper_live_balance_current_dollars": payload.get("current_dollars"),
        "paper_live_strategy_equity_dollars": payload.get("current_dollars"),
        "paper_live_realized_trade_pnl_dollars": payload.get("realized_trade_pnl_dollars"),
        "paper_live_mark_to_market_pnl_dollars": payload.get("mark_to_market_pnl_dollars"),
        "paper_live_drawdown_pct": payload.get("drawdown_pct"),
        "paper_live_strategy_drawdown_pct": payload.get("drawdown_pct"),
        "paper_live_positions_open_count": payload.get("positions_open_count"),
        "paper_live_positions_closed_count": payload.get("positions_closed_count"),
        "paper_live_open_positions": payload.get("positions_open_count"),
        "paper_live_closed_positions": payload.get("positions_closed_count"),
        "paper_live_positions_open": payload.get("positions_open"),
        "paper_live_positions_closed": payload.get("positions_closed"),
        "paper_live_order_attempts": payload.get("order_attempts"),
        "paper_live_attempted_orders": payload.get("order_attempts"),
        "paper_live_orders_resting": payload.get("orders_resting"),
        "paper_live_orders_filled": payload.get("orders_filled"),
        "paper_live_filled_orders": payload.get("orders_filled"),
        "paper_live_orders_partial_filled": payload.get("orders_partial_filled"),
        "paper_live_orders_canceled": payload.get("orders_canceled"),
        "paper_live_orders_expired": payload.get("orders_expired"),
        "paper_live_fill_time_seconds": payload.get("fill_time_seconds"),
        "paper_live_markout_10s_dollars": payload.get("markout_10s_dollars"),
        "paper_live_markout_10s": payload.get("markout_10s_dollars"),
        "paper_live_markout_60s_dollars": payload.get("markout_60s_dollars"),
        "paper_live_markout_60s": payload.get("markout_60s_dollars"),
        "paper_live_markout_300s_dollars": payload.get("markout_300s_dollars"),
        "paper_live_markout_300s": payload.get("markout_300s_dollars"),
        "paper_live_settlement_pnl_dollars": payload.get("settlement_pnl_dollars"),
        "paper_live_expected_value_dollars": payload.get("expected_value_dollars"),
        "paper_live_expected_vs_realized_delta": payload.get("expected_vs_realized_delta"),
        "paper_live_order_attempts_run": payload.get("order_attempts_run"),
        "paper_live_orders_filled_run": payload.get("orders_filled_run"),
        "paper_live_orders_partial_filled_run": payload.get("orders_partial_filled_run"),
        "paper_live_orders_canceled_run": payload.get("orders_canceled_run"),
        "paper_live_orders_expired_run": payload.get("orders_expired_run"),
        "paper_live_selected_tickers": payload.get("selected_tickers"),
        "paper_live_equity_curve": payload.get("equity_curve"),
        "paper_live_last_updated_at_utc": payload.get("last_updated_at_utc"),
        "paper_live_accounting_version": payload.get("accounting_version"),
        "paper_live_source": payload.get("source"),
    }


def _paper_live_scorecard_fields(paper_live: dict[str, Any] | None) -> dict[str, Any]:
    payload = paper_live if isinstance(paper_live, dict) else {}
    attempts_raw = payload.get("attempt_events")
    attempts = [item for item in attempts_raw if isinstance(item, dict)] if isinstance(attempts_raw, list) else []
    open_positions_raw = payload.get("positions_open")
    open_positions = [item for item in open_positions_raw if isinstance(item, dict)] if isinstance(open_positions_raw, list) else []
    closed_positions_raw = payload.get("positions_closed")
    closed_positions = [item for item in closed_positions_raw if isinstance(item, dict)] if isinstance(closed_positions_raw, list) else []

    if not attempts:
        # Backfill minimal attempt records from known positions so scorecards remain populated on older state files.
        synthesized_attempts: list[dict[str, Any]] = []
        for position in [*open_positions, *closed_positions]:
            ticker = str(position.get("market_ticker") or "").strip().upper()
            if not ticker:
                continue
            side = str(position.get("side") or "").strip().lower()
            if side not in {"yes", "no"}:
                side = "yes"
            status = "filled"
            synthesized_attempts.append(
                {
                    "attempt_event_key": str(position.get("attempt_event_key") or _shadow_position_key(market_ticker=ticker, side=side)),
                    "run_id": None,
                    "attempted_at_utc": position.get("entry_time_utc"),
                    "market_ticker": ticker,
                    "contract_family": str(position.get("contract_family") or "").strip().lower() or None,
                    "strip_key": str(position.get("strip_key") or "").strip() or None,
                    "side": side,
                    "contracts": _coerce_int(position.get("contracts"), 1),
                    "notional_risk_dollars": _parse_float(position.get("notional_risk_dollars")) or 0.0,
                    "expected_value_dollars": _parse_float(position.get("expected_value_dollars")) or 0.0,
                    "source_strategy": str(position.get("source_strategy") or "").strip() or None,
                    "opportunity_class": str(position.get("opportunity_class") or "").strip() or None,
                    "status": status,
                    "partial_fill": bool(position.get("partial_fill")),
                    "fill_time_seconds": _parse_float(position.get("fill_time_seconds")),
                }
            )
        attempts = synthesized_attempts

    def _new_group(*, family: str | None, ticker: str | None) -> dict[str, Any]:
        return {
            "family": family,
            "ticker": ticker,
            "strip_id": None,
            "source_strategy": None,
            "opportunity_class": None,
            "attempts": 0,
            "fills": 0,
            "partial_fills": 0,
            "canceled": 0,
            "expired": 0,
            "risk_sum_dollars": 0.0,
            "contracts_sum": 0.0,
            "expected_ev_entry_total_dollars": 0.0,
            "fill_time_samples": [],
            "markout_10s_sum": 0.0,
            "markout_60s_sum": 0.0,
            "markout_300s_sum": 0.0,
            "markout_count": 0,
            "realized_settlement_pnl_dollars": 0.0,
            "open_position_count": 0,
            "close_settlement_count": 0,
        }

    def _touch_group(store: dict[str, dict[str, Any]], key: str, *, family: str | None, ticker: str | None) -> dict[str, Any]:
        existing = store.get(key)
        if isinstance(existing, dict):
            return existing
        created = _new_group(family=family, ticker=ticker)
        store[key] = created
        return created

    by_family: dict[str, dict[str, Any]] = {}
    by_ticker: dict[str, dict[str, Any]] = {}

    for attempt in attempts:
        ticker = str(attempt.get("market_ticker") or "").strip().upper()
        family = str(attempt.get("contract_family") or "").strip().lower()
        if not family:
            family = "unknown_family"
        ticker_key = ticker or "UNKNOWN_TICKER"
        family_group = _touch_group(by_family, family, family=family, ticker=None)
        ticker_group = _touch_group(by_ticker, ticker_key, family=family, ticker=ticker_key)
        for group in (family_group, ticker_group):
            group["attempts"] = int(group.get("attempts") or 0) + 1
            status = str(attempt.get("status") or "").strip().lower()
            if status == "filled":
                group["fills"] = int(group.get("fills") or 0) + 1
            elif status == "canceled":
                group["canceled"] = int(group.get("canceled") or 0) + 1
            elif status == "expired":
                group["expired"] = int(group.get("expired") or 0) + 1
            if bool(attempt.get("partial_fill")):
                group["partial_fills"] = int(group.get("partial_fills") or 0) + 1
            risk = float(_parse_float(attempt.get("notional_risk_dollars")) or 0.0)
            contracts = float(max(1, _coerce_int(attempt.get("contracts"), 1)))
            expected_value = float(_parse_float(attempt.get("expected_value_dollars")) or 0.0)
            group["risk_sum_dollars"] = float(group.get("risk_sum_dollars") or 0.0) + risk
            group["contracts_sum"] = float(group.get("contracts_sum") or 0.0) + contracts
            group["expected_ev_entry_total_dollars"] = float(group.get("expected_ev_entry_total_dollars") or 0.0) + expected_value
            fill_time = _parse_float(attempt.get("fill_time_seconds"))
            if isinstance(fill_time, float) and fill_time >= 0.0 and status == "filled":
                samples = group.get("fill_time_samples")
                if isinstance(samples, list):
                    samples.append(float(fill_time))
            strip_id = str(attempt.get("strip_key") or "").strip()
            if strip_id and not group.get("strip_id"):
                group["strip_id"] = strip_id
            source_strategy = str(attempt.get("source_strategy") or "").strip()
            if source_strategy and not group.get("source_strategy"):
                group["source_strategy"] = source_strategy
            opportunity_class = str(attempt.get("opportunity_class") or "").strip()
            if opportunity_class and not group.get("opportunity_class"):
                group["opportunity_class"] = opportunity_class

    for position, position_is_open in [*[(item, True) for item in open_positions], *[(item, False) for item in closed_positions]]:
        ticker = str(position.get("market_ticker") or "").strip().upper()
        family = str(position.get("contract_family") or "").strip().lower()
        if not family:
            family = "unknown_family"
        ticker_key = ticker or "UNKNOWN_TICKER"
        family_group = _touch_group(by_family, family, family=family, ticker=None)
        ticker_group = _touch_group(by_ticker, ticker_key, family=family, ticker=ticker_key)
        for group in (family_group, ticker_group):
            markout_10s = _parse_float(position.get("markout_10s_dollars"))
            markout_60s = _parse_float(position.get("markout_60s_dollars"))
            markout_300s = _parse_float(position.get("markout_300s_dollars"))
            if isinstance(markout_300s, float):
                group["markout_count"] = int(group.get("markout_count") or 0) + 1
                group["markout_10s_sum"] = float(group.get("markout_10s_sum") or 0.0) + float(markout_10s or 0.0)
                group["markout_60s_sum"] = float(group.get("markout_60s_sum") or 0.0) + float(markout_60s or 0.0)
                group["markout_300s_sum"] = float(group.get("markout_300s_sum") or 0.0) + float(markout_300s or 0.0)
            if position_is_open:
                group["open_position_count"] = int(group.get("open_position_count") or 0) + 1
            else:
                group["close_settlement_count"] = int(group.get("close_settlement_count") or 0) + 1
                realized = _parse_float(position.get("settlement_pnl_dollars"))
                if not isinstance(realized, float):
                    realized = _parse_float(position.get("realized_trade_pnl_dollars"))
                group["realized_settlement_pnl_dollars"] = float(group.get("realized_settlement_pnl_dollars") or 0.0) + float(realized or 0.0)
            strip_id = str(position.get("strip_key") or "").strip()
            if strip_id and not group.get("strip_id"):
                group["strip_id"] = strip_id
            source_strategy = str(position.get("source_strategy") or "").strip()
            if source_strategy and not group.get("source_strategy"):
                group["source_strategy"] = source_strategy
            opportunity_class = str(position.get("opportunity_class") or "").strip()
            if opportunity_class and not group.get("opportunity_class"):
                group["opportunity_class"] = opportunity_class

    def _median(values: list[float]) -> float | None:
        if not values:
            return None
        sorted_values = sorted(float(v) for v in values)
        mid = len(sorted_values) // 2
        if len(sorted_values) % 2 == 1:
            return round(float(sorted_values[mid]), 6)
        return round((float(sorted_values[mid - 1]) + float(sorted_values[mid])) / 2.0, 6)

    def _finalize_group(group: dict[str, Any]) -> dict[str, Any]:
        attempts_count = max(0, _coerce_int(group.get("attempts"), 0))
        fills_count = max(0, _coerce_int(group.get("fills"), 0))
        markout_count = max(0, _coerce_int(group.get("markout_count"), 0))
        fill_time_samples = [float(v) for v in (group.get("fill_time_samples") or []) if isinstance(v, (int, float))]
        fill_time_mean = round(sum(fill_time_samples) / float(len(fill_time_samples)), 6) if fill_time_samples else None
        fill_time_median = _median(fill_time_samples)
        markout_10s_mean = (
            round(float(group.get("markout_10s_sum") or 0.0) / float(markout_count), 6)
            if markout_count > 0
            else None
        )
        markout_60s_mean = (
            round(float(group.get("markout_60s_sum") or 0.0) / float(markout_count), 6)
            if markout_count > 0
            else None
        )
        markout_300s_mean = (
            round(float(group.get("markout_300s_sum") or 0.0) / float(markout_count), 6)
            if markout_count > 0
            else None
        )
        expected_ev_total = round(float(group.get("expected_ev_entry_total_dollars") or 0.0), 6)
        realized_settlement = round(float(group.get("realized_settlement_pnl_dollars") or 0.0), 6)
        expected_vs_realized_delta = round(realized_settlement - expected_ev_total, 6)
        return {
            "family": group.get("family"),
            "ticker": group.get("ticker"),
            "strip_id": group.get("strip_id"),
            "source_strategy": group.get("source_strategy"),
            "opportunity_class": group.get("opportunity_class"),
            "attempts": attempts_count,
            "fills": fills_count,
            "fill_rate": round(float(fills_count) / float(attempts_count), 6) if attempts_count > 0 else None,
            "partial_fills": max(0, _coerce_int(group.get("partial_fills"), 0)),
            "canceled": max(0, _coerce_int(group.get("canceled"), 0)),
            "expired": max(0, _coerce_int(group.get("expired"), 0)),
            "fill_time_mean_seconds": fill_time_mean,
            "fill_time_median_seconds": fill_time_median,
            "markout_10s_mean_dollars": markout_10s_mean,
            "markout_60s_mean_dollars": markout_60s_mean,
            "markout_300s_mean_dollars": markout_300s_mean,
            "realized_settlement_pnl_dollars": realized_settlement,
            "expected_ev_at_entry_total_dollars": expected_ev_total,
            "expected_vs_realized_delta_dollars": expected_vs_realized_delta,
            "average_risk_dollars": (
                round(float(group.get("risk_sum_dollars") or 0.0) / float(attempts_count), 6)
                if attempts_count > 0
                else None
            ),
            "average_contracts": (
                round(float(group.get("contracts_sum") or 0.0) / float(attempts_count), 6)
                if attempts_count > 0
                else None
            ),
            "open_position_count": max(0, _coerce_int(group.get("open_position_count"), 0)),
            "close_settlement_count": max(0, _coerce_int(group.get("close_settlement_count"), 0)),
        }

    family_scorecards = [_finalize_group(group) for group in by_family.values()]
    ticker_scorecards = [_finalize_group(group) for group in by_ticker.values()]
    family_scorecards.sort(
        key=lambda row: (
            _coerce_int(row.get("attempts"), 0),
            _coerce_int(row.get("fills"), 0),
            str(row.get("family") or ""),
        ),
        reverse=True,
    )
    ticker_scorecards.sort(
        key=lambda row: (
            _coerce_int(row.get("attempts"), 0),
            _coerce_int(row.get("fills"), 0),
            str(row.get("ticker") or ""),
        ),
        reverse=True,
    )

    markout_rows = [row for row in family_scorecards if isinstance(_parse_float(row.get("markout_300s_mean_dollars")), float)]
    top_negative_markout = sorted(
        markout_rows,
        key=lambda row: float(_parse_float(row.get("markout_300s_mean_dollars")) or 0.0),
    )[:5]
    top_positive_markout = sorted(
        markout_rows,
        key=lambda row: float(_parse_float(row.get("markout_300s_mean_dollars")) or 0.0),
        reverse=True,
    )[:5]
    delta_ranked = sorted(
        family_scorecards,
        key=lambda row: abs(float(_parse_float(row.get("expected_vs_realized_delta_dollars")) or 0.0)),
        reverse=True,
    )[:10]
    monthly_row = next(
        (row for row in family_scorecards if str(row.get("family") or "").strip().lower() == "monthly_climate_anomaly"),
        None,
    )

    monthly_family = "monthly_climate_anomaly"

    def _parse_iso_timestamp(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _mean(values: list[float]) -> float | None:
        samples = [float(v) for v in values if isinstance(v, (int, float))]
        if not samples:
            return None
        return round(sum(samples) / float(len(samples)), 6)

    def _sample_band(sample_size: int) -> str:
        if sample_size < 5:
            return "anecdotal"
        if sample_size < 20:
            return "early_directional"
        if sample_size < 50:
            return "real_family_signal"
        return "policy_change_ready"

    monthly_position_samples: list[dict[str, Any]] = []
    for idx, (position, position_is_open) in enumerate(
        [*[(item, True) for item in open_positions], *[(item, False) for item in closed_positions]]
    ):
        family = str(position.get("contract_family") or "").strip().lower()
        if family != monthly_family:
            continue
        entry_time_utc = str(position.get("entry_time_utc") or "").strip() or None
        entry_dt = _parse_iso_timestamp(entry_time_utc)
        settlement_pnl: float | None = None
        if not position_is_open:
            settlement_pnl = _parse_float(position.get("settlement_pnl_dollars"))
            if not isinstance(settlement_pnl, float):
                settlement_pnl = _parse_float(position.get("realized_trade_pnl_dollars"))
        monthly_position_samples.append(
            {
                "entry_time_utc": entry_time_utc,
                "entry_epoch": float(entry_dt.timestamp()) if isinstance(entry_dt, datetime) else float(idx),
                "expected_value_dollars": float(_parse_float(position.get("expected_value_dollars")) or 0.0),
                "fill_time_seconds": _parse_float(position.get("fill_time_seconds")),
                "markout_10s_dollars": _parse_float(position.get("markout_10s_dollars")),
                "markout_60s_dollars": _parse_float(position.get("markout_60s_dollars")),
                "markout_300s_dollars": _parse_float(position.get("markout_300s_dollars")),
                "settlement_pnl_dollars": settlement_pnl if isinstance(settlement_pnl, float) else None,
            }
        )

    monthly_position_samples.sort(key=lambda row: (float(row.get("entry_epoch") or 0.0), str(row.get("entry_time_utc") or "")))

    def _rolling_summary(samples: list[dict[str, Any]], window_size: int) -> dict[str, Any]:
        if window_size <= 0:
            window_size = 1
        subset = samples[-window_size:]
        attempts_n = len(subset)
        fills_n = attempts_n
        settled_rows = [row for row in subset if isinstance(row.get("settlement_pnl_dollars"), float)]
        settled_n = len(settled_rows)
        positive_settlements = sum(1 for row in settled_rows if float(row.get("settlement_pnl_dollars") or 0.0) > 0.0)
        realized_settlement = (
            round(sum(float(row.get("settlement_pnl_dollars") or 0.0) for row in settled_rows), 6)
            if settled_n > 0
            else None
        )
        expected_settlement = (
            round(sum(float(row.get("expected_value_dollars") or 0.0) for row in settled_rows), 6)
            if settled_n > 0
            else None
        )
        expected_vs_realized = (
            round(float(realized_settlement) - float(expected_settlement), 6)
            if isinstance(realized_settlement, float) and isinstance(expected_settlement, float)
            else None
        )
        return {
            "window_size": int(window_size),
            "attempts": int(attempts_n),
            "fills": int(fills_n),
            "fill_rate": round(float(fills_n) / float(attempts_n), 6) if attempts_n > 0 else None,
            "fill_time_mean_seconds": _mean(
                [float(row.get("fill_time_seconds")) for row in subset if isinstance(row.get("fill_time_seconds"), (int, float))]
            ),
            "markout_10s_mean_dollars": _mean(
                [float(row.get("markout_10s_dollars")) for row in subset if isinstance(row.get("markout_10s_dollars"), (int, float))]
            ),
            "markout_60s_mean_dollars": _mean(
                [float(row.get("markout_60s_dollars")) for row in subset if isinstance(row.get("markout_60s_dollars"), (int, float))]
            ),
            "markout_300s_mean_dollars": _mean(
                [float(row.get("markout_300s_dollars")) for row in subset if isinstance(row.get("markout_300s_dollars"), (int, float))]
            ),
            "settled_count": int(settled_n),
            "positive_settlement_count": int(positive_settlements),
            "positive_settlement_rate": (
                round(float(positive_settlements) / float(settled_n), 6) if settled_n > 0 else None
            ),
            "realized_settlement_pnl_dollars": realized_settlement,
            "expected_vs_realized_delta_dollars": expected_vs_realized,
        }

    rolling_windows = [_rolling_summary(monthly_position_samples, size) for size in [5, 20, 50]]
    latest_window = next((row for row in rolling_windows if _coerce_int(row.get("window_size"), 0) == 20), None)
    if latest_window is None and rolling_windows:
        latest_window = rolling_windows[0]

    attempts_total = (
        max(0, _coerce_int(monthly_row.get("attempts"), 0)) if isinstance(monthly_row, dict) else len(monthly_position_samples)
    )
    fills_total = (
        max(0, _coerce_int(monthly_row.get("fills"), 0)) if isinstance(monthly_row, dict) else len(monthly_position_samples)
    )
    settled_total = sum(
        1 for row in monthly_position_samples if isinstance(row.get("settlement_pnl_dollars"), float)
    )
    positive_settlements_total = sum(
        1
        for row in monthly_position_samples
        if isinstance(row.get("settlement_pnl_dollars"), float) and float(row.get("settlement_pnl_dollars") or 0.0) > 0.0
    )
    monthly_trend = {
        "family": monthly_family,
        "sample_band": _sample_band(attempts_total),
        "attempts_total": int(attempts_total),
        "fills_total": int(fills_total),
        "settled_total": int(settled_total),
        "positive_settlements_total": int(positive_settlements_total),
        "positive_settlement_rate_total": (
            round(float(positive_settlements_total) / float(settled_total), 6) if settled_total > 0 else None
        ),
        "rolling_n": int(min(20, attempts_total)) if attempts_total > 0 else 0,
        "rolling_windows": rolling_windows,
        "latest_window": latest_window,
        "updated_at_utc": payload.get("last_updated_at_utc"),
    }

    return {
        "paper_live_family_scorecards": family_scorecards[:20],
        "paper_live_ticker_scorecards": ticker_scorecards[:50],
        "paper_live_top_negative_markout_families": top_negative_markout,
        "paper_live_top_positive_markout_families": top_positive_markout,
        "paper_live_top_expected_vs_realized_deltas": delta_ranked,
        "paper_live_monthly_climate_anomaly_scorecard": monthly_row,
        "paper_live_monthly_climate_anomaly_trend": monthly_trend,
        "paper_live_monthly_climate_anomaly_trend_band": monthly_trend.get("sample_band"),
    }


def _shadow_position_key(*, market_ticker: str, side: str) -> str:
    ticker = str(market_ticker or "").strip().upper()
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"yes", "no"}:
        normalized_side = "yes"
    return f"{ticker}|{normalized_side}" if ticker else ""


def _shadow_close_time_estimate(*, as_of: datetime | None, hours_to_close: float | None) -> str | None:
    if not isinstance(as_of, datetime):
        return None
    if not isinstance(hours_to_close, float):
        return None
    close_dt = as_of + timedelta(hours=float(hours_to_close))
    return close_dt.astimezone(timezone.utc).isoformat()


def _shadow_primary_position(
    positions_open: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not positions_open:
        return None
    ranked = sorted(
        [item for item in positions_open if isinstance(item, dict)],
        key=lambda item: (
            float(_parse_float(item.get("notional_risk_dollars")) or 0.0),
            float(_parse_float(item.get("mark_to_model_pnl_dollars")) or 0.0),
            str(item.get("market_ticker") or ""),
        ),
        reverse=True,
    )
    return ranked[0] if ranked else None


def _shadow_current_targets(
    *,
    climate_router_step: dict[str, Any] | None,
    climate_router_shadow_plan: dict[str, Any] | None,
    run_finished_at: datetime | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, float]]:
    targets: dict[str, dict[str, Any]] = {}
    marks_by_key: dict[str, float] = {}

    allocations_raw: Any = None
    if isinstance(climate_router_step, dict):
        allocations_raw = climate_router_step.get("routing_allocator_allocations")
    if not isinstance(allocations_raw, list):
        allocations_raw = (
            climate_router_shadow_plan.get("top_shadow_allocations")
            if isinstance(climate_router_shadow_plan, dict)
            else None
        )
    allocations = [item for item in allocations_raw if isinstance(item, dict)] if isinstance(allocations_raw, list) else []

    tradable_rows = _load_router_tradable_rows(climate_router_step)
    tradable_by_key: dict[str, dict[str, Any]] = {}
    for row in tradable_rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("market_ticker") or "").strip().upper()
        side = str(row.get("theoretical_side") or "").strip().lower()
        key = _shadow_position_key(market_ticker=ticker, side=side)
        if not key:
            continue
        tradable_by_key[key] = row
        price = _parse_float(row.get("theoretical_reference_price"))
        if isinstance(price, float):
            marks_by_key[key] = float(price)

    for row in allocations:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        side = str(row.get("side") or row.get("theoretical_side") or "").strip().lower()
        key = _shadow_position_key(market_ticker=ticker, side=side)
        if not key:
            continue

        tradable_row = tradable_by_key.get(key) or {}
        reference_price = _parse_float(row.get("reference_price_dollars"))
        if not isinstance(reference_price, float):
            reference_price = _parse_float(row.get("theoretical_reference_price"))
        if not isinstance(reference_price, float):
            reference_price = _parse_float(tradable_row.get("theoretical_reference_price"))
        if not isinstance(reference_price, float):
            reference_price = 0.0
        marks_by_key[key] = float(reference_price)

        contracts = max(
            1,
            _coerce_int(
                row.get("contracts"),
                _coerce_int(tradable_row.get("contracts"), 1),
            ),
        )
        risk_dollars = _parse_float(row.get("risk_dollars"))
        if not isinstance(risk_dollars, float):
            risk_dollars = max(0.0, float(reference_price) * float(contracts))

        hours_to_close = _parse_float(row.get("hours_to_close"))
        if not isinstance(hours_to_close, float):
            hours_to_close = _parse_float(tradable_row.get("hours_to_close"))

        target = {
            "position_key": key,
            "market_ticker": ticker,
            "contract_family": (
                str(row.get("contract_family") or tradable_row.get("contract_family") or "").strip().lower() or None
            ),
            "strip_key": str(row.get("strip_key") or tradable_row.get("strip_key") or "").strip() or None,
            "side": str(side or "yes").strip().lower(),
            "contracts": int(contracts),
            "entry_price_dollars": round(float(reference_price), 6),
            "mark_price_dollars": round(float(reference_price), 6),
            "notional_risk_dollars": round(float(risk_dollars), 6),
            "expected_value_dollars": round(float(_parse_float(row.get("expected_value_dollars")) or 0.0), 6),
            "availability_state": (
                str(row.get("availability_state") or tradable_row.get("availability_state") or "").strip().lower() or None
            ),
            "hours_to_close": round(float(hours_to_close), 6) if isinstance(hours_to_close, float) else None,
            "close_time_estimate_utc": _shadow_close_time_estimate(as_of=run_finished_at, hours_to_close=hours_to_close),
        }
        targets[key] = target

    return targets, marks_by_key


def _update_shadow_bankroll(
    *,
    run_id: str,
    run_finished_at_utc: str,
    enabled: bool,
    start_dollars: float,
    state_file: Path | None,
    climate_router_step: dict[str, Any] | None,
    climate_router_shadow_plan: dict[str, Any] | None,
    climate_router_pilot: dict[str, Any] | None,
) -> dict[str, Any]:
    snapshot = _shadow_bankroll_defaults(
        enabled=enabled,
        start_dollars=start_dollars,
        state_file=state_file,
        status="observer_not_run",
        reason=None,
    )
    if not enabled:
        snapshot["status"] = "disabled"
        snapshot["reason"] = "shadow_bankroll_disabled"
        return snapshot
    if not isinstance(state_file, Path):
        snapshot["status"] = "observer_degraded"
        snapshot["reason"] = "shadow_bankroll_state_file_missing"
        return snapshot

    start = max(0.0, float(start_dollars))
    total_realized = 0.0
    total_realized_trade = 0.0
    previous_peak = start
    previous_strategy_peak = start
    previous_last_run_id = ""
    strategy_accounting_version = 0
    existing_open_positions: list[dict[str, Any]] = []
    existing_closed_positions: list[dict[str, Any]] = []
    existing_equity_curve: list[dict[str, Any]] = []

    existing_payload = _load_json(state_file)
    if isinstance(existing_payload, dict):
        strategy_accounting_version = max(
            0,
            _coerce_int(existing_payload.get("shadow_strategy_accounting_version"), 0),
        )
        previous_start = _parse_float(existing_payload.get("shadow_bankroll_start_dollars"))
        if isinstance(previous_start, float) and previous_start > 0.0:
            if abs(previous_start - start) > 1e-9:
                total_realized = 0.0
                previous_peak = start
                previous_last_run_id = ""
                snapshot["reason"] = "shadow_bankroll_start_changed_reset"
            else:
                start = previous_start
                total_realized = float(_parse_float(existing_payload.get("shadow_realized_pnl_dollars")) or 0.0)
                total_realized_trade = float(
                    _parse_float(existing_payload.get("shadow_realized_trade_pnl_dollars"))
                    or _parse_float(existing_payload.get("shadow_realized_pnl_dollars"))
                    or 0.0
                )
                previous_peak = max(
                    start,
                    float(
                        _parse_float(existing_payload.get("shadow_theoretical_peak_value_dollars"))
                        or _parse_float(existing_payload.get("shadow_bankroll_peak_dollars"))
                        or start
                    ),
                    float(
                        _parse_float(existing_payload.get("shadow_theoretical_value_dollars"))
                        or _parse_float(existing_payload.get("shadow_bankroll_current_dollars"))
                        or start
                    ),
                )
                previous_strategy_peak = max(
                    start,
                    float(
                        _parse_float(existing_payload.get("shadow_strategy_peak_equity_dollars"))
                        or start
                    ),
                )
                previous_last_run_id = str(existing_payload.get("last_run_id") or "").strip()
                raw_open = existing_payload.get("shadow_positions_open")
                if isinstance(raw_open, list):
                    existing_open_positions = [item for item in raw_open if isinstance(item, dict)]
                raw_closed = existing_payload.get("shadow_positions_closed")
                if isinstance(raw_closed, list):
                    existing_closed_positions = [item for item in raw_closed if isinstance(item, dict)]
                raw_curve = existing_payload.get("shadow_equity_curve")
                if isinstance(raw_curve, list):
                    existing_equity_curve = [item for item in raw_curve if isinstance(item, dict)]
                if strategy_accounting_version < 2:
                    previous_strategy_peak = max(
                        start,
                        float(_parse_float(existing_payload.get("shadow_strategy_equity_dollars")) or start),
                    )

    expected_value_dollars = 0.0
    allocator_total_risk_dollars = 0.0
    allocator_selected_rows = 0
    if isinstance(climate_router_shadow_plan, dict):
        expected_value_dollars = float(
            _parse_float(climate_router_shadow_plan.get("total_expected_value_dollars")) or 0.0
        )
        allocator_total_risk_dollars = float(
            _parse_float(climate_router_shadow_plan.get("total_risk_dollars")) or 0.0
        )
        allocator_selected_rows = max(
            0,
            _coerce_int(
                climate_router_shadow_plan.get("would_trade_rows"),
                _coerce_int(climate_router_shadow_plan.get("eligible_rows"), 0),
            ),
        )

    run_finished_at_dt = _parse_iso(run_finished_at_utc)
    if not isinstance(run_finished_at_dt, datetime):
        run_finished_at_dt = datetime.now(timezone.utc)
    run_is_new = bool(run_id and run_id != previous_last_run_id)

    current_targets, marks_by_key = _shadow_current_targets(
        climate_router_step=climate_router_step,
        climate_router_shadow_plan=climate_router_shadow_plan,
        run_finished_at=run_finished_at_dt,
    )

    updated_open_positions: list[dict[str, Any]] = []
    closed_positions_history: list[dict[str, Any]] = list(existing_closed_positions)
    realized_trade_delta_dollars = 0.0

    open_by_key: dict[str, dict[str, Any]] = {}
    for raw_position in existing_open_positions:
        key = _shadow_position_key(
            market_ticker=str(raw_position.get("market_ticker") or ""),
            side=str(raw_position.get("side") or ""),
        )
        if key and key not in open_by_key:
            open_by_key[key] = dict(raw_position)

    if run_is_new:
        for key, current_position in open_by_key.items():
            target = current_targets.get(key)
            current_mark = _parse_float(marks_by_key.get(key))
            if not isinstance(current_mark, float):
                current_mark = _parse_float(current_position.get("mark_price_dollars"))
            if not isinstance(current_mark, float):
                current_mark = _parse_float(current_position.get("entry_price_dollars"))
            if not isinstance(current_mark, float):
                current_mark = 0.0

            entry_price = float(_parse_float(current_position.get("entry_price_dollars")) or 0.0)
            contracts = max(1, _coerce_int(current_position.get("contracts"), 1))
            mark_to_model = (float(current_mark) - float(entry_price)) * float(contracts)

            should_close = target is None
            close_reason = "deallocated"
            close_time_estimate_text = (
                str(target.get("close_time_estimate_utc") or "").strip()
                if isinstance(target, dict)
                else ""
            ) or str(current_position.get("close_time_estimate_utc") or "").strip()
            close_time_estimate_dt = _parse_iso(close_time_estimate_text)
            if isinstance(close_time_estimate_dt, datetime):
                close_time_estimate_dt = close_time_estimate_dt.astimezone(timezone.utc)
            if isinstance(close_time_estimate_dt, datetime) and run_finished_at_dt >= close_time_estimate_dt:
                should_close = True
                close_reason = "market_closed_estimate"
            if not should_close:
                target_contracts = max(1, _coerce_int(target.get("contracts"), 1))
                if target_contracts != contracts:
                    should_close = True
                    close_reason = "rebalance_contracts"

            if should_close:
                closed_position = dict(current_position)
                closed_position["position_key"] = key
                closed_position["exit_price_dollars"] = round(float(current_mark), 6)
                closed_position["exit_time_utc"] = run_finished_at_dt.astimezone(timezone.utc).isoformat()
                closed_position["close_reason"] = close_reason
                closed_position["realized_trade_pnl_dollars"] = round(float(mark_to_model), 6)
                closed_positions_history.append(closed_position)
                realized_trade_delta_dollars += float(mark_to_model)
                continue

            merged = dict(current_position)
            merged.update(
                {
                    "position_key": key,
                    "contract_family": target.get("contract_family"),
                    "strip_key": target.get("strip_key"),
                    "availability_state": target.get("availability_state"),
                    "notional_risk_dollars": round(float(_parse_float(target.get("notional_risk_dollars")) or 0.0), 6),
                    "hours_to_close": target.get("hours_to_close"),
                    "close_time_estimate_utc": target.get("close_time_estimate_utc"),
                    "mark_price_dollars": round(float(current_mark), 6),
                    "mark_time_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
                    "mark_to_model_pnl_dollars": round(float(mark_to_model), 6),
                    "expected_value_dollars": round(float(_parse_float(target.get("expected_value_dollars")) or 0.0), 6),
                }
            )
            updated_open_positions.append(merged)

        existing_keys = {
            _shadow_position_key(
                market_ticker=str(item.get("market_ticker") or ""),
                side=str(item.get("side") or ""),
            )
            for item in updated_open_positions
            if isinstance(item, dict)
        }
        for key, target in current_targets.items():
            if key in existing_keys:
                continue
            entry_price = float(_parse_float(target.get("entry_price_dollars")) or 0.0)
            contracts = max(1, _coerce_int(target.get("contracts"), 1))
            opened = {
                "position_id": f"{key}|{run_finished_at_dt.strftime('%Y%m%d%H%M%S')}",
                "position_key": key,
                "market_ticker": target.get("market_ticker"),
                "contract_family": target.get("contract_family"),
                "strip_key": target.get("strip_key"),
                "side": target.get("side"),
                "contracts": int(contracts),
                "entry_price_dollars": round(float(entry_price), 6),
                "entry_time_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
                "notional_risk_dollars": round(float(_parse_float(target.get("notional_risk_dollars")) or 0.0), 6),
                "hours_to_close": target.get("hours_to_close"),
                "close_time_estimate_utc": target.get("close_time_estimate_utc"),
                "availability_state": target.get("availability_state"),
                "mark_price_dollars": round(float(entry_price), 6),
                "mark_time_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
                "mark_to_model_pnl_dollars": 0.0,
                "expected_value_dollars": round(float(_parse_float(target.get("expected_value_dollars")) or 0.0), 6),
            }
            updated_open_positions.append(opened)
    else:
        updated_open_positions = [dict(item) for item in existing_open_positions if isinstance(item, dict)]
        closed_positions_history = [dict(item) for item in existing_closed_positions if isinstance(item, dict)]

    # Keep position history bounded for payload size control.
    max_closed_positions = 250
    if len(closed_positions_history) > max_closed_positions:
        closed_positions_history = closed_positions_history[-max_closed_positions:]

    if run_is_new:
        total_realized_trade = float(total_realized_trade) + float(realized_trade_delta_dollars)

    mark_to_model_pnl_dollars = 0.0
    normalized_open_positions: list[dict[str, Any]] = []
    for raw_position in updated_open_positions:
        key = _shadow_position_key(
            market_ticker=str(raw_position.get("market_ticker") or ""),
            side=str(raw_position.get("side") or ""),
        )
        if not key:
            continue
        mark_price = _parse_float(marks_by_key.get(key))
        if not isinstance(mark_price, float):
            mark_price = _parse_float(raw_position.get("mark_price_dollars"))
        if not isinstance(mark_price, float):
            mark_price = _parse_float(raw_position.get("entry_price_dollars"))
        if not isinstance(mark_price, float):
            mark_price = 0.0

        entry_price = float(_parse_float(raw_position.get("entry_price_dollars")) or 0.0)
        contracts = max(1, _coerce_int(raw_position.get("contracts"), 1))
        mark_to_model = (float(mark_price) - float(entry_price)) * float(contracts)
        mark_to_model_pnl_dollars += float(mark_to_model)

        normalized = dict(raw_position)
        normalized["position_key"] = key
        normalized["mark_price_dollars"] = round(float(mark_price), 6)
        normalized["mark_time_utc"] = run_finished_at_dt.astimezone(timezone.utc).isoformat()
        normalized["mark_to_model_pnl_dollars"] = round(float(mark_to_model), 6)
        normalized["contracts"] = int(contracts)
        normalized_open_positions.append(normalized)

    strategy_equity_dollars = float(start) + float(total_realized_trade) + float(mark_to_model_pnl_dollars)
    strategy_peak_dollars = max(float(previous_strategy_peak), float(start), float(strategy_equity_dollars))
    strategy_drawdown_pct = (
        ((strategy_peak_dollars - strategy_equity_dollars) / strategy_peak_dollars) * 100.0
        if strategy_peak_dollars > 0.0
        else 0.0
    )

    total_realized = float(total_realized_trade)
    theoretical_unrealized_ev_dollars = float(expected_value_dollars)
    theoretical_value_dollars = float(start) + float(total_realized) + float(theoretical_unrealized_ev_dollars)
    peak_dollars = max(float(previous_peak), float(start), float(theoretical_value_dollars))
    theoretical_drawdown_pct = (
        ((peak_dollars - theoretical_value_dollars) / peak_dollars) * 100.0
        if peak_dollars > 0.0
        else 0.0
    )

    equity_curve = [dict(item) for item in existing_equity_curve if isinstance(item, dict)]
    equity_point = {
        "run_id": run_id,
        "as_of_utc": run_finished_at_dt.astimezone(timezone.utc).isoformat(),
        "equity_dollars": round(float(strategy_equity_dollars), 6),
        "realized_trade_pnl_dollars": round(float(total_realized_trade), 6),
        "mark_to_model_pnl_dollars": round(float(mark_to_model_pnl_dollars), 6),
        "positions_open": len(normalized_open_positions),
        "positions_closed": len(closed_positions_history),
    }
    if run_is_new:
        equity_curve.append(equity_point)
    elif equity_curve:
        equity_curve[-1] = equity_point
    else:
        equity_curve.append(equity_point)
    max_equity_points = 500
    if len(equity_curve) > max_equity_points:
        equity_curve = equity_curve[-max_equity_points:]

    primary_position = _shadow_primary_position(normalized_open_positions)
    primary_entry_price = None
    primary_entry_time = None
    primary_side = None
    primary_contracts = None
    primary_notional_risk = None
    primary_mark_price = None
    if isinstance(primary_position, dict):
        primary_entry_price = _parse_float(primary_position.get("entry_price_dollars"))
        primary_entry_time = str(primary_position.get("entry_time_utc") or "").strip() or None
        primary_side = str(primary_position.get("side") or "").strip().lower() or None
        primary_contracts = _coerce_int(primary_position.get("contracts"), 0)
        primary_notional_risk = _parse_float(primary_position.get("notional_risk_dollars"))
        primary_mark_price = _parse_float(primary_position.get("mark_price_dollars"))

    snapshot.update(
        {
            "enabled": True,
            "status": "observer_ready",
            "reason": snapshot.get("reason") or "shadow_bankroll_updated",
            "last_updated_at_utc": run_finished_at_utc,
            "start_dollars": round(float(start), 4),
            "theoretical_value_dollars": round(float(theoretical_value_dollars), 4),
            "realized_pnl_dollars": round(float(total_realized), 4),
            "theoretical_unrealized_ev_dollars": round(float(theoretical_unrealized_ev_dollars), 4),
            "theoretical_drawdown_pct": round(max(0.0, float(theoretical_drawdown_pct)), 4),
            "allocator_total_risk_dollars": round(float(allocator_total_risk_dollars), 4),
            "allocator_selected_rows": int(allocator_selected_rows),
            "expected_value_dollars": round(float(expected_value_dollars), 4),
            "strategy_equity_dollars": round(float(strategy_equity_dollars), 4),
            "strategy_drawdown_pct": round(max(0.0, float(strategy_drawdown_pct)), 4),
            "strategy_peak_equity_dollars": round(float(strategy_peak_dollars), 4),
            "mark_to_model_pnl_dollars": round(float(mark_to_model_pnl_dollars), 4),
            "realized_trade_pnl_dollars": round(float(total_realized_trade), 4),
            "positions_open": normalized_open_positions,
            "positions_closed": closed_positions_history,
            "positions_open_count": len(normalized_open_positions),
            "positions_closed_count": len(closed_positions_history),
            "equity_curve": equity_curve,
            "strategy_accounting_version": 2,
            "entry_price": round(float(primary_entry_price), 6) if isinstance(primary_entry_price, float) else None,
            "entry_time_utc": primary_entry_time,
            "side": primary_side,
            "contracts": int(primary_contracts) if isinstance(primary_contracts, int) and primary_contracts > 0 else None,
            "notional_risk_dollars": (
                round(float(primary_notional_risk), 6) if isinstance(primary_notional_risk, float) else None
            ),
            "mark_price": round(float(primary_mark_price), 6) if isinstance(primary_mark_price, float) else None,
        }
    )

    state_payload = {
        "updated_at_utc": run_finished_at_utc,
        "last_run_id": run_id,
        "shadow_bankroll_start_dollars": round(float(start), 4),
        "shadow_theoretical_value_dollars": round(float(theoretical_value_dollars), 4),
        "shadow_theoretical_peak_value_dollars": round(float(peak_dollars), 4),
        "shadow_realized_pnl_dollars": round(float(total_realized), 4),
        "shadow_theoretical_unrealized_ev_dollars": round(float(theoretical_unrealized_ev_dollars), 4),
        "shadow_theoretical_drawdown_pct": round(max(0.0, float(theoretical_drawdown_pct)), 4),
        "shadow_allocator_total_risk_dollars": round(float(allocator_total_risk_dollars), 4),
        "shadow_allocator_selected_rows": int(allocator_selected_rows),
        "shadow_expected_value_dollars": round(float(expected_value_dollars), 4),
        "shadow_strategy_equity_dollars": round(float(strategy_equity_dollars), 4),
        "shadow_strategy_drawdown_pct": round(max(0.0, float(strategy_drawdown_pct)), 4),
        "shadow_strategy_peak_equity_dollars": round(float(strategy_peak_dollars), 4),
        "shadow_mark_to_model_pnl_dollars": round(float(mark_to_model_pnl_dollars), 4),
        "shadow_realized_trade_pnl_dollars": round(float(total_realized_trade), 4),
        "shadow_positions_open": normalized_open_positions,
        "shadow_positions_closed": closed_positions_history,
        "shadow_positions_open_count": len(normalized_open_positions),
        "shadow_positions_closed_count": len(closed_positions_history),
        "shadow_equity_curve": equity_curve,
        "shadow_strategy_accounting_version": 2,
    }
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text(json.dumps(state_payload, indent=2), encoding="utf-8")
    except Exception as exc:  # pragma: no cover - observer lane should never fail run
        snapshot["status"] = "observer_degraded"
        snapshot["state_write_error"] = str(exc)
        snapshot["reason"] = "shadow_bankroll_state_write_failed"
    return snapshot


def _runtime_version_for_report(
    *,
    run_started_at: str,
    run_id: str,
    repo_root: Path,
    prior_trader_step: dict[str, Any] | None,
    frontier_step: dict[str, Any] | None,
    as_of: datetime,
) -> dict[str, Any]:
    if not callable(build_runtime_version_block):
        return {
            "git_sha": "unknown",
            "git_branch": "unknown",
            "git_dirty": None,
            "run_id": run_id,
            "run_started_at_utc": run_started_at,
            "rain_model_tag": None,
            "temperature_model_tag": None,
            "fill_model_mode": None,
            "prefer_empirical_fill_model": None,
            "weather_priors_version": None,
            "frontier_artifact_path": None,
            "frontier_artifact_sha256": None,
            "frontier_artifact_file_sha256": None,
            "frontier_artifact_payload_sha256": None,
            "frontier_artifact_as_of_utc": None,
            "frontier_artifact_age_seconds": None,
            "frontier_selection_mode": None,
            "frontier_trusted_bucket_count": 0,
            "frontier_untrusted_bucket_count": 0,
        }
    frontier_path = None
    frontier_selection_mode = None
    if isinstance(prior_trader_step, dict):
        frontier_path = (
            prior_trader_step.get("execution_frontier_report_reference_file")
            or prior_trader_step.get("execution_frontier_break_even_reference_file")
            or prior_trader_step.get("frontier_artifact_path")
        )
        frontier_selection_mode = (
            prior_trader_step.get("execution_frontier_report_selection_mode")
            or prior_trader_step.get("execution_frontier_selection_mode")
        )
    if not frontier_path and isinstance(frontier_step, dict):
        frontier_path = frontier_step.get("output_file") or frontier_step.get("frontier_artifact_path")
    if not frontier_selection_mode and isinstance(frontier_step, dict):
        frontier_selection_mode = frontier_step.get("frontier_selection_mode") or "self_generated"
    return build_runtime_version_block(
        run_started_at=run_started_at,
        run_id=run_id,
        git_cwd=repo_root,
        rain_model_tag=(
            prior_trader_step.get("rain_model_tag")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        temperature_model_tag=(
            prior_trader_step.get("temperature_model_tag")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        fill_model_mode=(
            prior_trader_step.get("fill_model_mode")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        prefer_empirical_fill_model=(
            prior_trader_step.get("execution_empirical_fill_model_prefer_empirical")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        weather_priors_version_name=(
            prior_trader_step.get("weather_priors_version")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        frontier_artifact_path=frontier_path,
        frontier_selection_mode=frontier_selection_mode,
        as_of=as_of,
    )


def _top_level_decision_identity(step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {
            "top_market_ticker": None,
            "top_market_contract_family": None,
            "fair_yes_probability_raw": None,
            "execution_probability_guarded": None,
            "fill_probability_source": None,
            "empirical_fill_weight": None,
            "heuristic_fill_weight": None,
            "probe_lane_used": None,
            "probe_reason": None,
        }
    return {
        "top_market_ticker": step.get("top_market_ticker"),
        "top_market_contract_family": step.get("top_market_contract_family"),
        "fair_yes_probability_raw": _parse_float(step.get("fair_yes_probability_raw")),
        "execution_probability_guarded": _parse_float(step.get("execution_probability_guarded")),
        "fill_probability_source": step.get("fill_probability_source"),
        "empirical_fill_weight": _parse_float(step.get("empirical_fill_weight")),
        "heuristic_fill_weight": _parse_float(step.get("heuristic_fill_weight")),
        "probe_lane_used": step.get("probe_lane_used"),
        "probe_reason": step.get("probe_reason"),
    }


def _top_level_probe_policy(step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {
            "enable_untrusted_bucket_probe_exploration": None,
            "untrusted_bucket_probe_exploration_enabled": None,
            "untrusted_bucket_probe_max_orders_per_run": None,
            "untrusted_bucket_probe_required_edge_buffer_dollars": None,
            "untrusted_bucket_probe_contracts_cap": None,
            "untrusted_bucket_probe_submitted_attempts": None,
            "untrusted_bucket_probe_blocked_attempts": None,
            "untrusted_bucket_probe_reason_counts": None,
        }
    return {
        "enable_untrusted_bucket_probe_exploration": step.get("enable_untrusted_bucket_probe_exploration"),
        "untrusted_bucket_probe_exploration_enabled": step.get("untrusted_bucket_probe_exploration_enabled"),
        "untrusted_bucket_probe_max_orders_per_run": step.get("untrusted_bucket_probe_max_orders_per_run"),
        "untrusted_bucket_probe_required_edge_buffer_dollars": _parse_float(
            step.get("untrusted_bucket_probe_required_edge_buffer_dollars")
        ),
        "untrusted_bucket_probe_contracts_cap": step.get("untrusted_bucket_probe_contracts_cap"),
        "untrusted_bucket_probe_submitted_attempts": step.get("untrusted_bucket_probe_submitted_attempts"),
        "untrusted_bucket_probe_blocked_attempts": step.get("untrusted_bucket_probe_blocked_attempts"),
        "untrusted_bucket_probe_reason_counts": step.get("untrusted_bucket_probe_reason_counts"),
    }


def _top_level_climate_router_pilot(
    *,
    prior_trader_step: dict[str, Any] | None,
    climate_router_step: dict[str, Any] | None,
) -> dict[str, Any]:
    selected_tickers_raw = (
        prior_trader_step.get("climate_router_pilot_selected_tickers")
        if isinstance(prior_trader_step, dict)
        else []
    )
    selected_tickers: list[str] = []
    if isinstance(selected_tickers_raw, list):
        for raw_value in selected_tickers_raw:
            ticker = str(raw_value or "").strip().upper()
            if ticker and ticker not in selected_tickers:
                selected_tickers.append(ticker)

    top_router_rows_raw = (
        climate_router_step.get("top_tradable_candidates")
        if isinstance(climate_router_step, dict)
        else None
    )
    top_router_rows: list[dict[str, Any]] = []
    if isinstance(top_router_rows_raw, list):
        top_router_rows = [row for row in top_router_rows_raw if isinstance(row, dict)]
    router_rows_by_ticker: dict[str, dict[str, Any]] = {}
    for row in top_router_rows:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        if ticker and ticker not in router_rows_by_ticker:
            router_rows_by_ticker[ticker] = row

    selected_candidates: list[dict[str, Any]] = []
    for ticker in selected_tickers:
        row = router_rows_by_ticker.get(ticker)
        if not isinstance(row, dict):
            continue
        selected_candidates.append(
            {
                "market_ticker": ticker,
                "market_title": row.get("market_title"),
                "contract_family": row.get("contract_family"),
                "strip_key": row.get("strip_key"),
                "hours_to_close": _parse_float(row.get("hours_to_close")),
                "availability_state": row.get("availability_state"),
                "opportunity_class": row.get("opportunity_class"),
                "theoretical_side": row.get("theoretical_side"),
                "theoretical_reference_price": _parse_float(row.get("theoretical_reference_price")),
                "theoretical_edge_net": _parse_float(row.get("theoretical_edge_net")),
                "fair_yes_probability": _parse_float(row.get("fair_yes_probability")),
                "expected_value_dollars": _parse_float(row.get("expected_value_dollars")),
            }
        )

    if not selected_candidates:
        for row in top_router_rows[:5]:
            ticker = str(row.get("market_ticker") or "").strip().upper()
            selected_candidates.append(
                {
                    "market_ticker": ticker or None,
                    "market_title": row.get("market_title"),
                    "contract_family": row.get("contract_family"),
                    "strip_key": row.get("strip_key"),
                    "hours_to_close": _parse_float(row.get("hours_to_close")),
                    "availability_state": row.get("availability_state"),
                    "opportunity_class": row.get("opportunity_class"),
                    "theoretical_side": row.get("theoretical_side"),
                    "theoretical_reference_price": _parse_float(row.get("theoretical_reference_price")),
                    "theoretical_edge_net": _parse_float(row.get("theoretical_edge_net")),
                    "fair_yes_probability": _parse_float(row.get("fair_yes_probability")),
                    "expected_value_dollars": _parse_float(row.get("expected_value_dollars")),
                }
            )

    contracts_cap = max(
        1,
        _coerce_int(
            prior_trader_step.get("climate_router_pilot_contracts_cap")
            if isinstance(prior_trader_step, dict)
            else 1,
            1,
        ),
    )
    total_risk_dollars = 0.0
    for candidate in selected_candidates:
        reference_price = _parse_float(candidate.get("theoretical_reference_price"))
        if isinstance(reference_price, float) and reference_price > 0.0:
            total_risk_dollars += reference_price * float(contracts_cap)

    return {
        "enabled": bool(
            prior_trader_step.get("climate_router_pilot_enabled")
            if isinstance(prior_trader_step, dict)
            else False
        ),
        "status": (
            prior_trader_step.get("climate_router_pilot_status")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "reason": (
            prior_trader_step.get("climate_router_pilot_reason")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "summary_file": (
            prior_trader_step.get("climate_router_pilot_summary_file")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "selection_mode": (
            prior_trader_step.get("climate_router_pilot_selection_mode")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "summary_status": (
            prior_trader_step.get("climate_router_pilot_summary_status")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_classes": (
            prior_trader_step.get("climate_router_pilot_allowed_classes")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_families": (
            prior_trader_step.get("climate_router_pilot_allowed_families")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "excluded_families": (
            prior_trader_step.get("climate_router_pilot_excluded_families")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_families_effective": (
            prior_trader_step.get("climate_router_pilot_allowed_families_effective")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "excluded_families_effective": (
            prior_trader_step.get("climate_router_pilot_excluded_families_effective")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "max_orders_per_run": (
            prior_trader_step.get("climate_router_pilot_max_orders_per_run")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "contracts_cap": (
            prior_trader_step.get("climate_router_pilot_contracts_cap")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "required_ev_dollars": _parse_float(
            prior_trader_step.get("climate_router_pilot_required_ev_dollars")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "policy_scope_override_enabled": (
            prior_trader_step.get("climate_router_pilot_policy_scope_override_enabled")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "policy_scope_override_active": (
            prior_trader_step.get("climate_router_pilot_policy_scope_override_active")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "policy_scope_override_status": (
            prior_trader_step.get("climate_router_pilot_policy_scope_override_status")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "policy_scope_override_gate_active": (
            prior_trader_step.get("climate_router_pilot_policy_scope_override_gate_active")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "policy_scope_override_applicable": (
            prior_trader_step.get("climate_router_pilot_policy_scope_override_applicable")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "policy_scope_override_attempts": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_policy_scope_override_attempts"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "policy_scope_override_submissions": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_policy_scope_override_submissions"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "policy_scope_override_blocked_reason_counts": (
            prior_trader_step.get("climate_router_pilot_policy_scope_override_blocked_reason_counts")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "considered_rows": (
            prior_trader_step.get("climate_router_pilot_considered_rows")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "promoted_rows": (
            prior_trader_step.get("climate_router_pilot_promoted_rows")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "submitted_rows": (
            prior_trader_step.get("climate_router_pilot_submitted_rows")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "execute_considered_rows": (
            prior_trader_step.get("climate_router_pilot_execute_considered_rows")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "live_mode_enabled": (
            prior_trader_step.get("climate_router_pilot_live_mode_enabled")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "live_eligible_rows": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_live_eligible_rows"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "would_attempt_live_if_enabled": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_would_attempt_live_if_enabled"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_dry_run_only_rows": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_dry_run_only_rows"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_research_dry_run_only_reason_counts": (
            prior_trader_step.get("climate_router_pilot_blocked_research_dry_run_only_reason_counts")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "non_policy_gates_passed_rows": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_non_policy_gates_passed_rows"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "expected_value_dollars": _parse_float(
            prior_trader_step.get("climate_router_pilot_expected_value_dollars")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "blocked_reason_counts": (
            prior_trader_step.get("climate_router_pilot_blocked_reason_counts")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "selected_tickers": selected_tickers,
        "top_candidates": selected_candidates[:5],
        "promoted_from_router_count": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_submitted_rows"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "total_risk_dollars": round(total_risk_dollars, 6),
        "attempted_orders": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_attempted_orders"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "acked_orders": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_acked_orders"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "resting_orders": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_resting_orders"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "filled_orders": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_filled_orders"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "partial_fills": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_partial_fills"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "markout_10s_dollars": _parse_float(
            prior_trader_step.get("climate_router_pilot_markout_10s_dollars")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "markout_60s_dollars": _parse_float(
            prior_trader_step.get("climate_router_pilot_markout_60s_dollars")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "markout_300s_dollars": _parse_float(
            prior_trader_step.get("climate_router_pilot_markout_300s_dollars")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "realized_pnl_dollars": _parse_float(
            prior_trader_step.get("climate_router_pilot_realized_pnl_dollars")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "expected_vs_realized_delta": _parse_float(
            prior_trader_step.get("climate_router_pilot_expected_vs_realized_delta")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "blocked_post_promotion_reason_counts": (
            prior_trader_step.get("climate_router_pilot_blocked_post_promotion_reason_counts")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "blocked_frontier_insufficient_data": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_frontier_insufficient_data"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_balance": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_balance"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_board_stale": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_board_stale"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_weather_history": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_weather_history"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_duplicate_ticker": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_duplicate_ticker"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_no_orderable_side_on_recheck": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_no_orderable_side_on_recheck"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_ev_below_threshold": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_ev_below_threshold"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_research_dry_run_only": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_research_dry_run_only"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_live_disabled": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_live_disabled"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_policy_scope": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_policy_scope"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_family_filter": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_family_filter"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "blocked_contract_cap": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_blocked_contract_cap"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "frontier_bootstrap_submitted_attempts": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_frontier_bootstrap_submitted_attempts"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
        "frontier_bootstrap_blocked_attempts": (
            _coerce_int(prior_trader_step.get("climate_router_pilot_frontier_bootstrap_blocked_attempts"), 0)
            if isinstance(prior_trader_step, dict)
            else 0
        ),
    }


def _top_level_pilot_execution_evidence(
    *,
    step: dict[str, Any] | None,
    climate_router_pilot: dict[str, Any] | None,
) -> dict[str, Any]:
    summary_file = (
        str(step.get("pilot_execution_summary_file") or step.get("output_file") or "").strip()
        if isinstance(step, dict)
        else ""
    )
    payload: dict[str, Any] = {}
    if summary_file and not (isinstance(step, dict) and bool(step.get("observer_failure"))):
        payload = _load_json(Path(summary_file))
        if not isinstance(payload, dict):
            payload = {}

    core_state = payload.get("core_state")
    if not isinstance(core_state, dict):
        core_state = {}

    pilot_funnel = payload.get("pilot_funnel")
    if not isinstance(pilot_funnel, dict):
        pilot_funnel = {}

    first_attempt_evidence = payload.get("first_attempt_evidence")
    if not isinstance(first_attempt_evidence, dict):
        first_attempt_evidence = {}

    attempt_snapshot = first_attempt_evidence.get("attempt_snapshot")
    if not isinstance(attempt_snapshot, dict):
        attempt_snapshot = {}

    selected_ticker = str(
        pilot_funnel.get("selected_ticker")
        or attempt_snapshot.get("market_ticker")
        or ""
    ).strip()
    if not selected_ticker and isinstance(climate_router_pilot, dict):
        selected_tickers = climate_router_pilot.get("selected_tickers")
        if isinstance(selected_tickers, list):
            for raw in selected_tickers:
                ticker = str(raw or "").strip()
                if ticker:
                    selected_ticker = ticker
                    break

    would_attempt_live_if_enabled = _coerce_int(
        pilot_funnel.get("would_attempt_live_if_enabled"),
        _coerce_int(
            climate_router_pilot.get("would_attempt_live_if_enabled") if isinstance(climate_router_pilot, dict) else 0,
            0,
        ),
    )
    attempted_orders = _coerce_int(
        pilot_funnel.get("attempted_orders"),
        _coerce_int(climate_router_pilot.get("attempted_orders") if isinstance(climate_router_pilot, dict) else 0, 0),
    )
    filled_orders = _coerce_int(
        pilot_funnel.get("filled_orders"),
        _coerce_int(climate_router_pilot.get("filled_orders") if isinstance(climate_router_pilot, dict) else 0, 0),
    )

    frontier_status_raw = core_state.get("frontier_status")
    frontier_status = str(frontier_status_raw or "").strip() or None

    selected_family: str | None = None
    expected_value_dollars = None
    total_risk_dollars = None
    blocked_reason_counts = None

    if isinstance(climate_router_pilot, dict):
        expected_value_dollars = _parse_float(climate_router_pilot.get("expected_value_dollars"))
        total_risk_dollars = _parse_float(climate_router_pilot.get("total_risk_dollars"))
        blocked_reason_counts = climate_router_pilot.get("blocked_reason_counts")
        top_candidates = climate_router_pilot.get("top_candidates")
        if isinstance(top_candidates, list):
            normalized_selected = selected_ticker.upper()
            selected_candidate: dict[str, Any] | None = None
            for raw_candidate in top_candidates:
                if not isinstance(raw_candidate, dict):
                    continue
                candidate_ticker = str(raw_candidate.get("market_ticker") or "").strip().upper()
                if normalized_selected and candidate_ticker == normalized_selected:
                    selected_candidate = raw_candidate
                    break
            if selected_candidate is None:
                for raw_candidate in top_candidates:
                    if isinstance(raw_candidate, dict):
                        selected_candidate = raw_candidate
                        break
            if isinstance(selected_candidate, dict):
                selected_family = str(selected_candidate.get("contract_family") or "").strip() or None
                if expected_value_dollars is None:
                    expected_value_dollars = _parse_float(selected_candidate.get("expected_value_dollars"))

    status = str(first_attempt_evidence.get("status") or "").strip() or None
    if status is None:
        if filled_orders > 0:
            status = "filled"
        elif attempted_orders > 0:
            status = "attempted_no_fill"
        elif would_attempt_live_if_enabled > 0:
            status = "blocked_before_submit"
        elif selected_ticker:
            status = "selected_no_attempt_signal"

    recommended_next_action = str(payload.get("recommended_next_action") or "").strip() or None
    if recommended_next_action is None:
        if filled_orders > 0:
            recommended_next_action = "collect_markout_and_roll_forward"
        elif attempted_orders > 0:
            recommended_next_action = "collect_no_fill_diagnostics_and_retry_single_shot"
        elif would_attempt_live_if_enabled > 0:
            recommended_next_action = "enable_single_shot_live_for_1x1_evidence"
        elif str(frontier_status or "").strip().lower() == "insufficient_data":
            recommended_next_action = "collect_more_frontier_samples_before_scaling"

    return {
        "status": status,
        "selected_ticker": selected_ticker or None,
        "would_attempt_live_if_enabled": would_attempt_live_if_enabled,
        "attempted_orders": attempted_orders,
        "filled_orders": filled_orders,
        "frontier_status": frontier_status,
        "recommended_next_action": recommended_next_action,
        "summary_file": summary_file or None,
        "blocked_reason_counts": blocked_reason_counts,
        "selected_family": selected_family,
        "expected_value_dollars": expected_value_dollars,
        "total_risk_dollars": total_risk_dollars,
    }


def _pilot_execution_report_fields(pilot_execution_evidence: dict[str, Any] | None) -> dict[str, Any]:
    evidence = pilot_execution_evidence if isinstance(pilot_execution_evidence, dict) else {}
    return {
        "pilot_execution_evidence_status": evidence.get("status"),
        "pilot_execution_selected_ticker": evidence.get("selected_ticker"),
        "pilot_execution_would_attempt_live_if_enabled": evidence.get("would_attempt_live_if_enabled"),
        "pilot_execution_attempted_orders": evidence.get("attempted_orders"),
        "pilot_execution_filled_orders": evidence.get("filled_orders"),
        "pilot_execution_frontier_status": evidence.get("frontier_status"),
        "pilot_execution_recommended_next_action": evidence.get("recommended_next_action"),
        "pilot_execution_summary_file": evidence.get("summary_file"),
        "pilot_execution_blocked_reason_counts": evidence.get("blocked_reason_counts"),
        "pilot_execution_selected_family": evidence.get("selected_family"),
        "pilot_execution_expected_value_dollars": evidence.get("expected_value_dollars"),
        "pilot_execution_total_risk_dollars": evidence.get("total_risk_dollars"),
    }


def _default_lane_comparison_lane() -> dict[str, Any]:
    return {
        "picked_ticker": None,
        "picked_side": None,
        "selected_fair_probability": None,
        "selected_fair_probability_conservative": None,
        "maker_entry_edge": None,
        "maker_entry_edge_net_fees": None,
        "expected_value_dollars": None,
        "expected_value_per_cost": None,
        "estimated_entry_cost_dollars": None,
        "estimated_max_loss_dollars": None,
        "estimated_max_profit_dollars": None,
        "expected_value_per_max_loss": None,
        "gate_status": None,
        "gate_blockers": None,
        "summary_file": None,
    }


def _default_lane_comparison(
    *,
    status: str = "not_run",
    reason: str | None = None,
    executed_lane: str = "maker_edge",
    comparison_basis: str = "same_snapshot_same_filters",
) -> dict[str, Any]:
    return {
        "status": status,
        "reason": reason,
        "comparison_basis": comparison_basis,
        "executed_lane": executed_lane,
        "fully_frozen": False,
        "snapshot_inputs": {},
        "maker_edge": _default_lane_comparison_lane(),
        "probability_first": _default_lane_comparison_lane(),
        "delta": {
            "same_pick": None,
            "selected_fair_probability_delta": None,
            "maker_entry_edge_delta": None,
            "expected_value_dollars_delta": None,
            "expected_value_per_cost_delta": None,
            "estimated_max_loss_dollars_delta": None,
            "expected_value_per_max_loss_delta": None,
        },
        "errors": [],
    }


def _safe_lane_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    return _parse_float(value)


def _lane_value_delta(probability_first_value: Any, maker_edge_value: Any) -> float | None:
    probability_first_number = _safe_lane_float(probability_first_value)
    maker_edge_number = _safe_lane_float(maker_edge_value)
    if not isinstance(probability_first_number, float) or not isinstance(maker_edge_number, float):
        return None
    return round(float(probability_first_number) - float(maker_edge_number), 6)


def _lane_snapshot_from_execute_summary(summary: dict[str, Any] | None) -> dict[str, Any]:
    lane = _default_lane_comparison_lane()
    if not isinstance(summary, dict):
        return lane

    prior_gate = summary.get("prior_trade_gate_summary")
    if not isinstance(prior_gate, dict):
        prior_gate = {}

    expected_value_dollars = _safe_lane_float(summary.get("top_market_expected_value_dollars"))
    estimated_entry_cost_dollars = _safe_lane_float(summary.get("top_market_estimated_entry_cost_dollars"))
    expected_value_per_cost = _safe_lane_float(summary.get("top_market_expected_roi_on_cost"))
    if not isinstance(expected_value_per_cost, float):
        if isinstance(expected_value_dollars, float) and isinstance(estimated_entry_cost_dollars, float) and (
            estimated_entry_cost_dollars > 0.0
        ):
            expected_value_per_cost = round(expected_value_dollars / estimated_entry_cost_dollars, 6)
        else:
            expected_value_per_cost = None

    estimated_max_loss_dollars = _safe_lane_float(summary.get("top_market_estimated_max_loss_dollars"))
    expected_value_per_max_loss = None
    if isinstance(expected_value_dollars, float) and isinstance(estimated_max_loss_dollars, float) and (
        estimated_max_loss_dollars > 0.0
    ):
        expected_value_per_max_loss = round(expected_value_dollars / estimated_max_loss_dollars, 6)

    lane.update(
        {
            "picked_ticker": str(summary.get("top_market_ticker") or "").strip() or None,
            "picked_side": str(summary.get("top_market_side") or "").strip().lower() or None,
            "selected_fair_probability": _safe_lane_float(summary.get("top_market_fair_probability")),
            "selected_fair_probability_conservative": _safe_lane_float(
                summary.get("top_market_fair_probability_conservative")
            ),
            "maker_entry_edge": _safe_lane_float(summary.get("top_market_maker_entry_edge")),
            "maker_entry_edge_net_fees": _safe_lane_float(summary.get("top_market_maker_entry_edge_net_fees")),
            "expected_value_dollars": expected_value_dollars,
            "expected_value_per_cost": expected_value_per_cost,
            "estimated_entry_cost_dollars": estimated_entry_cost_dollars,
            "estimated_max_loss_dollars": estimated_max_loss_dollars,
            "estimated_max_profit_dollars": _safe_lane_float(summary.get("top_market_estimated_max_profit_dollars")),
            "expected_value_per_max_loss": expected_value_per_max_loss,
            "gate_status": (
                str(prior_gate.get("gate_status") or "").strip()
                or str(summary.get("prior_trade_gate_status") or "").strip()
                or None
            ),
            "gate_blockers": (
                prior_gate.get("gate_blockers")
                if isinstance(prior_gate.get("gate_blockers"), list)
                else summary.get("prior_trade_gate_blockers")
            ),
            "summary_file": str(summary.get("output_file") or "").strip() or None,
        }
    )
    return lane


def _build_lane_comparison(
    *,
    maker_edge_summary: dict[str, Any] | None,
    probability_first_summary: dict[str, Any] | None,
    executed_lane: str = "maker_edge",
    comparison_basis: str = "same_snapshot_same_filters",
    reason: str | None = None,
    errors: list[str] | None = None,
    fully_frozen: bool = False,
    snapshot_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = _default_lane_comparison(
        status="ready",
        reason=reason,
        executed_lane=executed_lane,
        comparison_basis=comparison_basis,
    )
    payload["fully_frozen"] = bool(fully_frozen)
    payload["snapshot_inputs"] = dict(snapshot_inputs or {})
    payload["maker_edge"] = _lane_snapshot_from_execute_summary(maker_edge_summary)
    payload["probability_first"] = _lane_snapshot_from_execute_summary(probability_first_summary)
    payload["delta"] = {
        "same_pick": (
            bool(payload["maker_edge"].get("picked_ticker"))
            and bool(payload["probability_first"].get("picked_ticker"))
            and str(payload["maker_edge"].get("picked_ticker") or "").strip().upper()
            == str(payload["probability_first"].get("picked_ticker") or "").strip().upper()
            and str(payload["maker_edge"].get("picked_side") or "").strip().lower()
            == str(payload["probability_first"].get("picked_side") or "").strip().lower()
        ),
        "selected_fair_probability_delta": _lane_value_delta(
            payload["probability_first"].get("selected_fair_probability"),
            payload["maker_edge"].get("selected_fair_probability"),
        ),
        "maker_entry_edge_delta": _lane_value_delta(
            payload["probability_first"].get("maker_entry_edge"),
            payload["maker_edge"].get("maker_entry_edge"),
        ),
        "expected_value_dollars_delta": _lane_value_delta(
            payload["probability_first"].get("expected_value_dollars"),
            payload["maker_edge"].get("expected_value_dollars"),
        ),
        "expected_value_per_cost_delta": _lane_value_delta(
            payload["probability_first"].get("expected_value_per_cost"),
            payload["maker_edge"].get("expected_value_per_cost"),
        ),
        "estimated_max_loss_dollars_delta": _lane_value_delta(
            payload["probability_first"].get("estimated_max_loss_dollars"),
            payload["maker_edge"].get("estimated_max_loss_dollars"),
        ),
        "expected_value_per_max_loss_delta": _lane_value_delta(
            payload["probability_first"].get("expected_value_per_max_loss"),
            payload["maker_edge"].get("expected_value_per_max_loss"),
        ),
    }
    payload_errors = [str(item).strip() for item in (errors or []) if str(item).strip()]
    payload["errors"] = payload_errors
    if payload_errors:
        payload["status"] = "observer_degraded"
        if not payload.get("reason"):
            payload["reason"] = "lane_comparison_observer_degraded"
    return payload


def _top_level_no_candidates_diagnostics(step: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(step, dict):
        return None
    gate_status = str(step.get("prior_trade_gate_status") or "").strip().lower()
    if gate_status != "no_candidates":
        return None
    return {
        "prior_trade_gate_status": gate_status,
        "prior_trade_gate_blockers": step.get("prior_trade_gate_blockers"),
        "plan_summary_file": step.get("prior_plan_summary_file"),
        "plan_summary_status": step.get("plan_summary_status"),
        "plan_summary_error": step.get("plan_summary_error"),
        "plan_summary_planned_orders": step.get("plan_summary_planned_orders"),
        "dominant_skip_reason": step.get("plan_skip_reason_dominant"),
        "dominant_skip_count": step.get("plan_skip_reason_dominant_count"),
        "dominant_skip_share": step.get("plan_skip_reason_dominant_share"),
        "skip_counts_total": step.get("plan_skip_counts_total"),
        "skip_counts_nonzero_total": step.get("plan_skip_counts_nonzero_total"),
        "top_skip_counts": step.get("plan_skip_counts_top"),
        "allowed_universe_candidate_pool_size": step.get("allowed_universe_candidate_pool_size"),
        "allowed_universe_dominant_skip_reason": step.get("allowed_universe_skip_reason_dominant"),
        "allowed_universe_dominant_skip_count": step.get("allowed_universe_skip_reason_dominant_count"),
        "allowed_universe_dominant_skip_share": step.get("allowed_universe_skip_reason_dominant_share"),
        "allowed_universe_skip_counts_total": step.get("allowed_universe_skip_counts_total"),
        "allowed_universe_skip_counts_nonzero_total": step.get("allowed_universe_skip_counts_nonzero_total"),
        "allowed_universe_top_skip_counts": step.get("allowed_universe_skip_counts_top"),
        "daily_weather_candidate_pool_size": step.get("daily_weather_candidate_pool_size"),
        "daily_weather_rows_total": step.get("daily_weather_rows_total"),
        "daily_weather_dominant_skip_reason": step.get("daily_weather_skip_reason_dominant"),
        "daily_weather_dominant_skip_count": step.get("daily_weather_skip_reason_dominant_count"),
        "daily_weather_dominant_skip_share": step.get("daily_weather_skip_reason_dominant_share"),
        "daily_weather_skip_counts_total": step.get("daily_weather_skip_counts_total"),
        "daily_weather_skip_counts_nonzero_total": step.get("daily_weather_skip_counts_nonzero_total"),
        "daily_weather_top_skip_counts": step.get("daily_weather_skip_counts_top"),
        "daily_weather_rows_with_conservative_candidate": step.get("daily_weather_rows_with_conservative_candidate"),
        "daily_weather_rows_with_both_sides_candidate": step.get("daily_weather_rows_with_both_sides_candidate"),
        "daily_weather_rows_with_one_side_failed": step.get("daily_weather_rows_with_one_side_failed"),
        "daily_weather_rows_with_both_sides_failed": step.get("daily_weather_rows_with_both_sides_failed"),
        "daily_weather_orderable_bid_rows": step.get("daily_weather_orderable_bid_rows"),
        "daily_weather_rows_with_any_orderable_bid": step.get("daily_weather_rows_with_any_orderable_bid"),
        "daily_weather_rows_with_any_orderable_ask": step.get("daily_weather_rows_with_any_orderable_ask"),
        "daily_weather_rows_with_fair_probabilities": step.get("daily_weather_rows_with_fair_probabilities"),
        "daily_weather_rows_with_both_quote_and_fair_value": step.get(
            "daily_weather_rows_with_both_quote_and_fair_value"
        ),
        "daily_weather_quote_orderability_counts": step.get("daily_weather_quote_orderability_counts"),
        "daily_weather_quote_age_rows_with_timestamp": step.get("daily_weather_quote_age_rows_with_timestamp"),
        "daily_weather_quote_stale_max_age_seconds": step.get("daily_weather_quote_stale_max_age_seconds"),
        "daily_weather_shadow_taker_rows_total": step.get("daily_weather_shadow_taker_rows_total"),
        "daily_weather_shadow_taker_rows_with_orderable_yes_ask": step.get(
            "daily_weather_shadow_taker_rows_with_orderable_yes_ask"
        ),
        "daily_weather_shadow_taker_rows_with_orderable_no_ask": step.get(
            "daily_weather_shadow_taker_rows_with_orderable_no_ask"
        ),
        "daily_weather_shadow_taker_rows_with_any_orderable_ask": step.get(
            "daily_weather_shadow_taker_rows_with_any_orderable_ask"
        ),
        "daily_weather_shadow_taker_edge_above_min_count": step.get(
            "daily_weather_shadow_taker_edge_above_min_count"
        ),
        "daily_weather_shadow_taker_edge_net_fees_above_min_count": step.get(
            "daily_weather_shadow_taker_edge_net_fees_above_min_count"
        ),
        "daily_weather_shadow_taker_endpoint_orderbook_rows": step.get(
            "daily_weather_shadow_taker_endpoint_orderbook_rows"
        ),
        "daily_weather_best_shadow_taker_candidate": step.get("daily_weather_best_shadow_taker_candidate"),
        "daily_weather_allowed_universe_rows_with_conservative_candidate": step.get(
            "daily_weather_allowed_universe_rows_with_conservative_candidate"
        ),
        "daily_weather_endpoint_orderbook_filtered": step.get("daily_weather_endpoint_orderbook_filtered"),
        "daily_weather_conservative_candidate_failure_counts": step.get(
            "daily_weather_conservative_candidate_failure_counts"
        ),
        "daily_weather_planned_orders": step.get("daily_weather_planned_orders"),
    }


def _top_level_daily_weather_funnel(
    *,
    prior_trader_step: dict[str, Any] | None,
    weather_prior_state_after: dict[str, Any],
    weather_history_state: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(prior_trader_step, dict):
        return {
            "captured_daily_weather_markets_total": None,
            "captured_daily_weather_markets_with_fresh_snapshot": None,
            "captured_daily_weather_family_counts": None,
            "daily_weather_board_age_seconds": None,
            "priors_allowed_daily_weather_rows_total": weather_prior_state_after.get("allowed_rows_total"),
            "priors_allowed_daily_weather_family_counts": weather_prior_state_after.get("allowed_family_counts"),
            "priors_weather_history_live_ready_rows": weather_history_state.get("weather_history_live_ready_rows"),
            "priors_weather_history_unhealthy_rows": weather_history_state.get("weather_history_unhealthy_rows"),
            "allowed_universe_candidate_pool_size": None,
            "daily_weather_candidate_pool_size": None,
            "daily_weather_rows_total": None,
            "daily_weather_rows_with_conservative_candidate": None,
            "daily_weather_rows_with_both_sides_candidate": None,
            "daily_weather_rows_with_one_side_failed": None,
            "daily_weather_rows_with_both_sides_failed": None,
            "daily_weather_orderable_bid_rows": None,
            "daily_weather_rows_with_any_orderable_bid": None,
            "daily_weather_rows_with_any_orderable_ask": None,
            "daily_weather_rows_with_fair_probabilities": None,
            "daily_weather_rows_with_both_quote_and_fair_value": None,
            "daily_weather_quote_orderability_counts": None,
            "daily_weather_quote_age_rows_with_timestamp": None,
            "daily_weather_quote_stale_max_age_seconds": None,
            "daily_weather_shadow_taker_rows_total": None,
            "daily_weather_shadow_taker_rows_with_orderable_yes_ask": None,
            "daily_weather_shadow_taker_rows_with_orderable_no_ask": None,
            "daily_weather_shadow_taker_rows_with_any_orderable_ask": None,
            "daily_weather_shadow_taker_edge_above_min_count": None,
            "daily_weather_shadow_taker_edge_net_fees_above_min_count": None,
            "daily_weather_shadow_taker_endpoint_orderbook_rows": None,
            "daily_weather_best_shadow_taker_candidate": None,
            "daily_weather_allowed_universe_rows_with_conservative_candidate": None,
            "daily_weather_endpoint_orderbook_filtered": None,
            "daily_weather_conservative_candidate_failure_counts": None,
            "daily_weather_skip_reason_dominant": None,
            "daily_weather_skip_counts_top": None,
            "daily_weather_planned_orders": None,
        }
    return {
        "captured_daily_weather_markets_total": prior_trader_step.get("daily_weather_markets"),
        "captured_daily_weather_markets_with_fresh_snapshot": prior_trader_step.get(
            "daily_weather_markets_with_fresh_snapshot"
        ),
        "captured_daily_weather_family_counts": prior_trader_step.get("daily_weather_family_counts"),
        "daily_weather_board_age_seconds": prior_trader_step.get("daily_weather_board_age_seconds"),
        "priors_allowed_daily_weather_rows_total": weather_prior_state_after.get("allowed_rows_total"),
        "priors_allowed_daily_weather_family_counts": weather_prior_state_after.get("allowed_family_counts"),
        "priors_weather_history_live_ready_rows": weather_history_state.get("weather_history_live_ready_rows"),
        "priors_weather_history_unhealthy_rows": weather_history_state.get("weather_history_unhealthy_rows"),
        "allowed_universe_candidate_pool_size": prior_trader_step.get("allowed_universe_candidate_pool_size"),
        "daily_weather_candidate_pool_size": prior_trader_step.get("daily_weather_candidate_pool_size"),
        "daily_weather_rows_total": prior_trader_step.get("daily_weather_rows_total"),
        "daily_weather_rows_with_conservative_candidate": prior_trader_step.get(
            "daily_weather_rows_with_conservative_candidate"
        ),
        "daily_weather_rows_with_both_sides_candidate": prior_trader_step.get(
            "daily_weather_rows_with_both_sides_candidate"
        ),
        "daily_weather_rows_with_one_side_failed": prior_trader_step.get("daily_weather_rows_with_one_side_failed"),
        "daily_weather_rows_with_both_sides_failed": prior_trader_step.get(
            "daily_weather_rows_with_both_sides_failed"
        ),
        "daily_weather_orderable_bid_rows": prior_trader_step.get("daily_weather_orderable_bid_rows"),
        "daily_weather_rows_with_any_orderable_bid": prior_trader_step.get(
            "daily_weather_rows_with_any_orderable_bid"
        ),
        "daily_weather_rows_with_any_orderable_ask": prior_trader_step.get(
            "daily_weather_rows_with_any_orderable_ask"
        ),
        "daily_weather_rows_with_fair_probabilities": prior_trader_step.get(
            "daily_weather_rows_with_fair_probabilities"
        ),
        "daily_weather_rows_with_both_quote_and_fair_value": prior_trader_step.get(
            "daily_weather_rows_with_both_quote_and_fair_value"
        ),
        "daily_weather_quote_orderability_counts": prior_trader_step.get("daily_weather_quote_orderability_counts"),
        "daily_weather_quote_age_rows_with_timestamp": prior_trader_step.get(
            "daily_weather_quote_age_rows_with_timestamp"
        ),
        "daily_weather_quote_stale_max_age_seconds": prior_trader_step.get(
            "daily_weather_quote_stale_max_age_seconds"
        ),
        "daily_weather_shadow_taker_rows_total": prior_trader_step.get("daily_weather_shadow_taker_rows_total"),
        "daily_weather_shadow_taker_rows_with_orderable_yes_ask": prior_trader_step.get(
            "daily_weather_shadow_taker_rows_with_orderable_yes_ask"
        ),
        "daily_weather_shadow_taker_rows_with_orderable_no_ask": prior_trader_step.get(
            "daily_weather_shadow_taker_rows_with_orderable_no_ask"
        ),
        "daily_weather_shadow_taker_rows_with_any_orderable_ask": prior_trader_step.get(
            "daily_weather_shadow_taker_rows_with_any_orderable_ask"
        ),
        "daily_weather_shadow_taker_edge_above_min_count": prior_trader_step.get(
            "daily_weather_shadow_taker_edge_above_min_count"
        ),
        "daily_weather_shadow_taker_edge_net_fees_above_min_count": prior_trader_step.get(
            "daily_weather_shadow_taker_edge_net_fees_above_min_count"
        ),
        "daily_weather_shadow_taker_endpoint_orderbook_rows": prior_trader_step.get(
            "daily_weather_shadow_taker_endpoint_orderbook_rows"
        ),
        "daily_weather_best_shadow_taker_candidate": prior_trader_step.get(
            "daily_weather_best_shadow_taker_candidate"
        ),
        "daily_weather_allowed_universe_rows_with_conservative_candidate": prior_trader_step.get(
            "daily_weather_allowed_universe_rows_with_conservative_candidate"
        ),
        "daily_weather_endpoint_orderbook_filtered": prior_trader_step.get("daily_weather_endpoint_orderbook_filtered"),
        "daily_weather_conservative_candidate_failure_counts": prior_trader_step.get(
            "daily_weather_conservative_candidate_failure_counts"
        ),
        "daily_weather_skip_reason_dominant": prior_trader_step.get("daily_weather_skip_reason_dominant"),
        "daily_weather_skip_counts_top": prior_trader_step.get("daily_weather_skip_counts_top"),
        "daily_weather_planned_orders": prior_trader_step.get("daily_weather_planned_orders"),
    }


def _daily_weather_strip_key_from_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper()
    if not ticker:
        return ""
    if "-" in ticker:
        return ticker.rsplit("-", 1)[0]
    return ticker


def _daily_weather_market_availability_regime(
    *,
    tradable_positive_rows: int,
    priced_watch_only_rows: int,
    wakeup_count_total: int,
    non_endpoint_observations_total: int,
    orderable_side_observations_total: int,
) -> tuple[str, str]:
    if tradable_positive_rows > 0 or orderable_side_observations_total > 0:
        return "tradable", "tradable_positive_or_orderable_observations_present"
    if priced_watch_only_rows > 0:
        return "priced_watch_only", "priced_watch_only_rows_present_without_tradable_rows"
    if wakeup_count_total > 0 or non_endpoint_observations_total > 0:
        return "wakeups_rare", "wakeups_or_non_endpoint_observations_present_without_priced_rows"
    return "dead", "endpoint_only_without_wakeups_or_priced_rows"


def _build_daily_weather_market_availability_study(
    *,
    prior_trader_step: dict[str, Any] | None,
    state_file_path: Any,
    lookback_days: float,
    top_n: int = 20,
) -> dict[str, Any]:
    study: dict[str, Any] = {
        "regime": "unknown",
        "regime_reason": "availability_state_missing",
        "lookback_days_requested": round(float(lookback_days), 3),
        "state_file": None,
        "state_file_status": "missing",
        "state_updated_at_utc": None,
        "run_counter": 0,
        "ticker_count": 0,
        "strip_count": 0,
        "sessions_watched_total": 0,
        "observations_total": 0,
        "endpoint_only_observations_total": 0,
        "non_endpoint_observations_total": 0,
        "orderable_side_observations_total": 0,
        "non_endpoint_quote_observations_total": 0,
        "wakeup_count_total": 0,
        "first_observed_at_utc": None,
        "last_observed_at_utc": None,
        "duration_observed_hours": None,
        "priced_watch_only_rows_latest": 0,
        "tradable_positive_rows_latest": 0,
        "unpriced_model_view_rows_latest": 0,
        "ticker_summaries_top": [],
        "strip_summaries_top": [],
    }
    if isinstance(prior_trader_step, dict):
        study["priced_watch_only_rows_latest"] = max(
            0,
            _coerce_int(prior_trader_step.get("daily_weather_priced_watch_only_rows"), 0),
        )
        study["tradable_positive_rows_latest"] = max(
            0,
            _coerce_int(prior_trader_step.get("daily_weather_tradable_positive_rows"), 0),
        )
        study["unpriced_model_view_rows_latest"] = max(
            0,
            _coerce_int(prior_trader_step.get("daily_weather_unpriced_model_view_rows"), 0),
        )

    state_path_text = str(state_file_path or "").strip()
    study["state_file"] = state_path_text or None
    if not state_path_text:
        regime, reason = _daily_weather_market_availability_regime(
            tradable_positive_rows=study["tradable_positive_rows_latest"],
            priced_watch_only_rows=study["priced_watch_only_rows_latest"],
            wakeup_count_total=0,
            non_endpoint_observations_total=0,
            orderable_side_observations_total=0,
        )
        study["regime"] = regime
        study["regime_reason"] = reason
        return study

    payload = _load_json(Path(state_path_text))
    if not isinstance(payload, dict):
        study["state_file_status"] = "invalid_or_unreadable"
        regime, reason = _daily_weather_market_availability_regime(
            tradable_positive_rows=study["tradable_positive_rows_latest"],
            priced_watch_only_rows=study["priced_watch_only_rows_latest"],
            wakeup_count_total=0,
            non_endpoint_observations_total=0,
            orderable_side_observations_total=0,
        )
        study["regime"] = regime
        study["regime_reason"] = reason
        return study

    study["state_file_status"] = "loaded"
    study["state_updated_at_utc"] = str(payload.get("updated_at_utc") or "").strip() or None
    study["run_counter"] = max(0, _coerce_int(payload.get("run_counter"), 0))
    ticker_stats_by_ticker_raw = payload.get("ticker_stats_by_ticker")
    ticker_stats_by_ticker = (
        dict(ticker_stats_by_ticker_raw)
        if isinstance(ticker_stats_by_ticker_raw, dict)
        else {}
    )
    opportunity_class_by_ticker = {}
    if isinstance(prior_trader_step, dict):
        raw_opportunity_class_by_ticker = prior_trader_step.get("daily_weather_opportunity_class_by_ticker")
        if isinstance(raw_opportunity_class_by_ticker, dict):
            opportunity_class_by_ticker = {
                str(ticker or "").strip().upper(): str(opportunity_class or "").strip().lower()
                for ticker, opportunity_class in raw_opportunity_class_by_ticker.items()
                if str(ticker or "").strip()
            }

    ticker_summaries: list[dict[str, Any]] = []
    strip_rollup: dict[str, dict[str, Any]] = {}
    first_observed_values: list[datetime] = []
    last_observed_values: list[datetime] = []

    for raw_ticker, raw_stats in ticker_stats_by_ticker.items():
        ticker = str(raw_ticker or "").strip().upper()
        if not ticker:
            continue
        stats = dict(raw_stats or {}) if isinstance(raw_stats, dict) else {}
        strip_key = _daily_weather_strip_key_from_ticker(ticker)
        sessions_watched = max(0, _coerce_int(stats.get("watch_selected_count"), 0))
        observations_total = max(0, _coerce_int(stats.get("observations_total"), 0))
        endpoint_only_observations = max(0, _coerce_int(stats.get("endpoint_only_observations"), 0))
        non_endpoint_observations = max(0, _coerce_int(stats.get("non_endpoint_observations"), 0))
        orderable_side_observations = max(0, _coerce_int(stats.get("orderable_side_observations"), 0))
        non_endpoint_quote_observations = max(0, _coerce_int(stats.get("non_endpoint_quote_observations"), 0))
        wakeup_count = max(0, _coerce_int(stats.get("wakeup_count"), 0))
        max_endpoint_only_streak = max(
            max(0, _coerce_int(stats.get("max_endpoint_only_streak_count"), 0)),
            max(0, _coerce_int(stats.get("endpoint_only_streak_count"), 0)),
        )
        orderable_side_observed_minutes = round(
            float(_parse_float(stats.get("orderable_side_observed_minutes")) or 0.0),
            4,
        )
        non_endpoint_quote_observed_minutes = round(
            float(_parse_float(stats.get("non_endpoint_quote_observed_minutes")) or 0.0),
            4,
        )
        first_wakeup_hours_to_close = _parse_float(stats.get("first_wakeup_hours_to_close"))
        first_wakeup_minutes_to_close = (
            round(float(first_wakeup_hours_to_close) * 60.0, 3)
            if isinstance(first_wakeup_hours_to_close, float)
            else None
        )
        first_observed = _parse_iso(stats.get("first_observed_at_utc"))
        last_observed = _parse_iso(stats.get("last_observed_at_utc"))
        if isinstance(first_observed, datetime):
            first_observed_values.append(first_observed)
        if isinstance(last_observed, datetime):
            last_observed_values.append(last_observed)
        observations_for_rate = max(1, observations_total)
        endpoint_only_rate = round(float(endpoint_only_observations) / float(observations_for_rate), 4)
        non_endpoint_rate = round(float(non_endpoint_observations) / float(observations_for_rate), 4)
        wakeup_rate = round(float(wakeup_count) / float(max(1, sessions_watched)), 4)
        opportunity_class = str(opportunity_class_by_ticker.get(ticker) or "").strip().lower()
        ticker_summary = {
            "market_ticker": ticker,
            "strip_key": strip_key,
            "sessions_watched": sessions_watched,
            "observations_total": observations_total,
            "endpoint_only_observations": endpoint_only_observations,
            "non_endpoint_observations": non_endpoint_observations,
            "orderable_side_observations": orderable_side_observations,
            "non_endpoint_quote_observations": non_endpoint_quote_observations,
            "endpoint_only_rate": endpoint_only_rate,
            "non_endpoint_rate": non_endpoint_rate,
            "wakeup_count": wakeup_count,
            "wakeup_rate": wakeup_rate,
            "orderable_side_observed_minutes": orderable_side_observed_minutes,
            "non_endpoint_quote_observed_minutes": non_endpoint_quote_observed_minutes,
            "avg_minutes_orderable": round(orderable_side_observed_minutes / float(observations_for_rate), 4),
            "avg_minutes_non_endpoint_quote": round(
                non_endpoint_quote_observed_minutes / float(observations_for_rate),
                4,
            ),
            "first_wakeup_minutes_to_close": first_wakeup_minutes_to_close,
            "max_endpoint_only_streak": max_endpoint_only_streak,
            "last_observed_lane": str(stats.get("last_observed_lane") or "").strip(),
            "had_orderable_side_ever": bool(stats.get("had_orderable_side_ever")),
            "opportunity_class_latest": opportunity_class or None,
        }
        ticker_summaries.append(ticker_summary)

        strip_stats = strip_rollup.setdefault(
            strip_key or ticker,
            {
                "strip_key": strip_key or ticker,
                "ticker_count": 0,
                "sessions_watched": 0,
                "observations_total": 0,
                "endpoint_only_observations": 0,
                "non_endpoint_observations": 0,
                "orderable_side_observations": 0,
                "non_endpoint_quote_observations": 0,
                "wakeup_count": 0,
                "orderable_side_observed_minutes": 0.0,
                "non_endpoint_quote_observed_minutes": 0.0,
                "first_wakeup_minutes_to_close_values": [],
                "priced_watch_only_rows": 0,
                "tradable_positive_rows": 0,
            },
        )
        strip_stats["ticker_count"] += 1
        strip_stats["sessions_watched"] += sessions_watched
        strip_stats["observations_total"] += observations_total
        strip_stats["endpoint_only_observations"] += endpoint_only_observations
        strip_stats["non_endpoint_observations"] += non_endpoint_observations
        strip_stats["orderable_side_observations"] += orderable_side_observations
        strip_stats["non_endpoint_quote_observations"] += non_endpoint_quote_observations
        strip_stats["wakeup_count"] += wakeup_count
        strip_stats["orderable_side_observed_minutes"] += orderable_side_observed_minutes
        strip_stats["non_endpoint_quote_observed_minutes"] += non_endpoint_quote_observed_minutes
        if isinstance(first_wakeup_minutes_to_close, float):
            strip_stats["first_wakeup_minutes_to_close_values"].append(first_wakeup_minutes_to_close)
        if opportunity_class == "priced_watch_only":
            strip_stats["priced_watch_only_rows"] += 1
        elif opportunity_class == "tradable_positive":
            strip_stats["tradable_positive_rows"] += 1

    ticker_summaries.sort(
        key=lambda item: (
            float(item.get("wakeup_count") or 0.0),
            float(item.get("non_endpoint_observations") or 0.0),
            float(item.get("orderable_side_observations") or 0.0),
            str(item.get("market_ticker") or ""),
        ),
        reverse=True,
    )
    strip_summaries: list[dict[str, Any]] = []
    for strip_key, raw_strip_stats in strip_rollup.items():
        strip_stats = dict(raw_strip_stats or {})
        ticker_count = max(1, int(strip_stats.get("ticker_count") or 1))
        observations_total = max(1, int(strip_stats.get("observations_total") or 1))
        endpoint_only_observations = int(strip_stats.get("endpoint_only_observations") or 0)
        non_endpoint_observations = int(strip_stats.get("non_endpoint_observations") or 0)
        wakeup_count = int(strip_stats.get("wakeup_count") or 0)
        orderable_side_observed_minutes = float(strip_stats.get("orderable_side_observed_minutes") or 0.0)
        first_wakeup_values = list(strip_stats.get("first_wakeup_minutes_to_close_values") or [])
        first_wakeup_minutes_to_close = (
            round(min(float(value) for value in first_wakeup_values), 3)
            if first_wakeup_values
            else None
        )
        strip_summary = {
            "strip_key": strip_key,
            "strip_ticker_count": ticker_count,
            "strip_endpoint_only_rate": round(float(endpoint_only_observations) / float(observations_total), 4),
            "strip_non_endpoint_rate": round(float(non_endpoint_observations) / float(observations_total), 4),
            "strip_wakeup_rate": round(float(wakeup_count) / float(max(1, observations_total)), 4),
            "strip_avg_minutes_orderable": round(orderable_side_observed_minutes / float(max(1, observations_total)), 4),
            "strip_first_wakeup_minutes_to_close": first_wakeup_minutes_to_close,
            "strip_priced_watch_only_rate": round(
                float(strip_stats.get("priced_watch_only_rows") or 0) / float(ticker_count),
                4,
            ),
            "strip_tradable_positive_rate": round(
                float(strip_stats.get("tradable_positive_rows") or 0) / float(ticker_count),
                4,
            ),
            "strip_sessions_watched": int(strip_stats.get("sessions_watched") or 0),
            "strip_observations_total": int(strip_stats.get("observations_total") or 0),
        }
        strip_summaries.append(strip_summary)
    strip_summaries.sort(
        key=lambda item: (
            float(item.get("strip_tradable_positive_rate") or 0.0),
            float(item.get("strip_priced_watch_only_rate") or 0.0),
            float(item.get("strip_wakeup_rate") or 0.0),
            -float(item.get("strip_endpoint_only_rate") or 0.0),
            str(item.get("strip_key") or ""),
        ),
        reverse=True,
    )

    study["ticker_count"] = len(ticker_summaries)
    study["strip_count"] = len(strip_summaries)
    study["ticker_summaries_top"] = ticker_summaries[: max(1, int(top_n))]
    study["strip_summaries_top"] = strip_summaries[: max(1, int(top_n))]
    study["sessions_watched_total"] = sum(int(item.get("sessions_watched") or 0) for item in ticker_summaries)
    study["observations_total"] = sum(int(item.get("observations_total") or 0) for item in ticker_summaries)
    study["endpoint_only_observations_total"] = sum(
        int(item.get("endpoint_only_observations") or 0)
        for item in ticker_summaries
    )
    study["non_endpoint_observations_total"] = sum(
        int(item.get("non_endpoint_observations") or 0)
        for item in ticker_summaries
    )
    study["orderable_side_observations_total"] = sum(
        int(item.get("orderable_side_observations") or 0)
        for item in ticker_summaries
    )
    study["non_endpoint_quote_observations_total"] = sum(
        int(item.get("non_endpoint_quote_observations") or 0)
        for item in ticker_summaries
    )
    study["wakeup_count_total"] = sum(int(item.get("wakeup_count") or 0) for item in ticker_summaries)
    if first_observed_values:
        first_observed = min(first_observed_values)
        study["first_observed_at_utc"] = first_observed.isoformat()
    if last_observed_values:
        last_observed = max(last_observed_values)
        study["last_observed_at_utc"] = last_observed.isoformat()
    if first_observed_values and last_observed_values:
        duration_hours = max(0.0, (max(last_observed_values) - min(first_observed_values)).total_seconds() / 3600.0)
        study["duration_observed_hours"] = round(duration_hours, 4)

    regime, reason = _daily_weather_market_availability_regime(
        tradable_positive_rows=study["tradable_positive_rows_latest"],
        priced_watch_only_rows=study["priced_watch_only_rows_latest"],
        wakeup_count_total=study["wakeup_count_total"],
        non_endpoint_observations_total=study["non_endpoint_observations_total"],
        orderable_side_observations_total=study["orderable_side_observations_total"],
    )
    study["regime"] = regime
    study["regime_reason"] = reason
    return study


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                rows.append(dict(row))
    except Exception:
        return []
    return rows


def _normalized_counts_map(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    normalized: dict[str, int] = {}
    for raw_key, raw_value in raw.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        count: int | None = None
        if isinstance(raw_value, bool):
            count = int(raw_value)
        elif isinstance(raw_value, (int, float)):
            count = int(raw_value)
        else:
            parsed = _parse_float(raw_value)
            if isinstance(parsed, float):
                count = int(parsed)
        if count is None:
            continue
        normalized[key] = max(0, int(count))
    return normalized


def _nonzero_counts_map(raw: Any) -> dict[str, int]:
    counts = _normalized_counts_map(raw)
    return {key: value for key, value in counts.items() if int(value) > 0}


def _count_from_map(raw: Any, key: str) -> int:
    counts = _normalized_counts_map(raw)
    return max(0, int(counts.get(str(key or "").strip(), 0)))


def _bool_from_any(value: Any) -> bool:
    return bool(_as_bool(value))


def _load_prior_trader_payload(prior_trader_step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(prior_trader_step, dict):
        return {}
    output_file_text = str(prior_trader_step.get("output_file") or "").strip()
    if not output_file_text:
        return {}
    payload = _load_json(Path(output_file_text))
    return payload if isinstance(payload, dict) else {}


def _load_plan_summary_payload(prior_trader_step: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(prior_trader_step, dict):
        return {}
    plan_summary_text = str(prior_trader_step.get("prior_plan_summary_file") or "").strip()
    if not plan_summary_text:
        return {}
    payload = _load_json(Path(plan_summary_text))
    return payload if isinstance(payload, dict) else {}


def _planned_tickers_from_plan_summary(payload: dict[str, Any]) -> list[str]:
    planned: list[str] = []
    seen: set[str] = set()

    def _add_ticker(raw_value: Any) -> None:
        ticker = str(raw_value or "").strip().upper()
        if not ticker:
            return
        if ticker in seen:
            return
        seen.add(ticker)
        planned.append(ticker)

    canonical_tickers = payload.get("canonical_covered_planned_tickers")
    if isinstance(canonical_tickers, list):
        for value in canonical_tickers:
            _add_ticker(value)

    top_plans = payload.get("top_plans")
    if isinstance(top_plans, list):
        for item in top_plans:
            if isinstance(item, dict):
                _add_ticker(item.get("market_ticker"))

    for key in ("top_market_ticker",):
        _add_ticker(payload.get(key))

    for key in ("plans", "planned_rows", "planned_entries", "planned_orders_rows"):
        values = payload.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            if isinstance(item, dict):
                _add_ticker(item.get("market_ticker"))

    return planned


def _normalize_climate_router_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    return {
        "market_ticker": str(raw_row.get("market_ticker") or "").strip().upper(),
        "market_title": str(raw_row.get("market_title") or "").strip(),
        "contract_family": str(raw_row.get("contract_family") or "").strip().lower(),
        "strip_key": str(raw_row.get("strip_key") or "").strip(),
        "availability_state": str(raw_row.get("availability_state") or "").strip().lower(),
        "opportunity_class": str(raw_row.get("opportunity_class") or "").strip().lower(),
        "theoretical_side": str(raw_row.get("theoretical_side") or "").strip().lower(),
        "theoretical_reference_source": str(raw_row.get("theoretical_reference_source") or "").strip(),
        "theoretical_reference_price": _parse_float(raw_row.get("theoretical_reference_price")),
        "theoretical_reference_usable": _bool_from_any(raw_row.get("theoretical_reference_usable")),
        "theoretical_reference_endpoint": _bool_from_any(raw_row.get("theoretical_reference_endpoint")),
        "theoretical_edge_net": _parse_float(raw_row.get("theoretical_edge_net")),
        "hours_to_close": _parse_float(raw_row.get("hours_to_close")),
    }


def _load_router_tradable_rows(climate_router_step: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(climate_router_step, dict):
        return []

    output_csv_text = str(climate_router_step.get("output_csv") or "").strip()
    csv_rows: list[dict[str, Any]] = []
    if output_csv_text:
        csv_rows = _load_csv_rows(Path(output_csv_text))

    tradable: list[dict[str, Any]] = []
    for row in csv_rows:
        normalized = _normalize_climate_router_row(row)
        if not normalized.get("market_ticker"):
            continue
        if normalized.get("opportunity_class") not in {"tradable_positive", "hot_positive"}:
            continue
        tradable.append(normalized)

    if tradable:
        return tradable

    top_rows = climate_router_step.get("top_tradable_candidates")
    if isinstance(top_rows, list):
        for row in top_rows:
            if not isinstance(row, dict):
                continue
            normalized = _normalize_climate_router_row(row)
            if not normalized.get("market_ticker"):
                continue
            if normalized.get("opportunity_class") not in {"tradable_positive", "hot_positive"}:
                continue
            tradable.append(normalized)
    return tradable


def _build_climate_router_shadow_plan(
    *,
    climate_router_step: dict[str, Any] | None,
    top_n: int = 5,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "status": "not_run",
        "reason": "climate_router_not_run",
        "eligible_rows": 0,
        "would_trade_rows": 0,
        "total_risk_dollars": 0.0,
        "total_expected_value_dollars": 0.0,
        "family_routed_capital_budget": {},
        "strip_routed_capital_budget": {},
        "top_shadow_allocations": [],
    }
    if not isinstance(climate_router_step, dict):
        return summary

    router_status = str(climate_router_step.get("status") or "").strip().lower()
    summary["status"] = router_status or "unknown"
    summary["reason"] = climate_router_step.get("reason")
    summary["eligible_rows"] = _coerce_int(climate_router_step.get("routing_allocator_eligible_rows"), 0)
    summary["would_trade_rows"] = _coerce_int(climate_router_step.get("routing_allocator_allocated_rows"), 0)
    summary["total_risk_dollars"] = round(float(_parse_float(climate_router_step.get("routing_allocator_total_risk_dollars")) or 0.0), 4)
    summary["total_expected_value_dollars"] = round(
        float(_parse_float(climate_router_step.get("routing_allocator_total_expected_value_dollars")) or 0.0),
        4,
    )

    family_budget = climate_router_step.get("family_routed_capital_budget")
    if isinstance(family_budget, dict):
        normalized_family_budget: dict[str, float] = {}
        for raw_family, raw_value in family_budget.items():
            family = str(raw_family or "").strip()
            if not family:
                continue
            value = _parse_float(raw_value)
            if value is None:
                continue
            normalized_family_budget[family] = round(float(value), 4)
        summary["family_routed_capital_budget"] = normalized_family_budget

    allocations_raw = climate_router_step.get("routing_allocator_allocations")
    normalized_allocations: list[dict[str, Any]] = []
    strip_budget: dict[str, float] = {}
    if isinstance(allocations_raw, list):
        for row in allocations_raw:
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("market_ticker") or "").strip().upper()
            if not ticker:
                continue
            risk = float(_parse_float(row.get("risk_dollars")) or 0.0)
            expected_value = float(_parse_float(row.get("expected_value_dollars")) or 0.0)
            strip_key = str(row.get("strip_key") or "").strip()
            normalized = {
                "market_ticker": ticker,
                "contract_family": str(row.get("contract_family") or "").strip().lower(),
                "strip_key": strip_key,
                "side": str(row.get("side") or "").strip().lower(),
                "availability_state": str(row.get("availability_state") or "").strip().lower(),
                "risk_dollars": round(risk, 4),
                "expected_value_dollars": round(expected_value, 4),
                "edge_net": round(float(_parse_float(row.get("edge_net")) or 0.0), 6),
                "reference_price_dollars": _parse_float(row.get("reference_price_dollars")),
                "contracts": _coerce_int(row.get("contracts"), 0),
            }
            normalized_allocations.append(normalized)
            if strip_key:
                strip_budget[strip_key] = round(float(strip_budget.get(strip_key, 0.0)) + risk, 4)

    normalized_allocations.sort(
        key=lambda item: (
            float(item.get("expected_value_dollars") or 0.0),
            float(item.get("risk_dollars") or 0.0),
            str(item.get("market_ticker") or ""),
        ),
        reverse=True,
    )
    summary["top_shadow_allocations"] = normalized_allocations[: max(1, int(top_n))]
    summary["strip_routed_capital_budget"] = dict(
        sorted(
            ((strip, round(float(risk), 4)) for strip, risk in strip_budget.items()),
            key=lambda item: (-item[1], item[0]),
        )
    )

    if router_status in {"ready", "ok"}:
        summary["status"] = "ready"
        summary["reason"] = "shadow_plan_derived_from_climate_router_allocations"
    elif not str(summary.get("reason") or "").strip():
        summary["reason"] = "climate_router_not_ready"
    return summary


def _build_router_vs_planner_gap(
    *,
    climate_router_step: dict[str, Any] | None,
    prior_trader_step: dict[str, Any] | None,
    balance_step: dict[str, Any] | None,
    balance_smoke_step: dict[str, Any] | None,
    top_n: int = 10,
) -> dict[str, Any]:
    gap: dict[str, Any] = {
        "status": "not_run",
        "reason": "climate_router_not_run",
        "climate_router_output_csv": None,
        "plan_summary_file": None,
        "router_tradable_rows": 0,
        "planner_planned_rows": 0,
        "router_tradable_not_planned_count": 0,
        "router_tradable_not_planned_tickers": [],
        "router_vs_planner_gap_reason_counts": {},
        "router_vs_planner_gap_top_rows": [],
        "planner_skip_counts_nonzero": {},
        "enforce_daily_weather_live_only": None,
        "routine_max_hours_to_close": None,
    }
    if not isinstance(climate_router_step, dict):
        return gap

    gap["climate_router_output_csv"] = climate_router_step.get("output_csv")
    tradable_rows = _load_router_tradable_rows(climate_router_step)
    gap["router_tradable_rows"] = len(tradable_rows)
    if not tradable_rows:
        gap["status"] = "no_router_tradable_rows"
        gap["reason"] = "router_has_no_tradable_positive_rows"
        return gap

    prior_payload = _load_prior_trader_payload(prior_trader_step)
    plan_payload = _load_plan_summary_payload(prior_trader_step)
    gap["plan_summary_file"] = (
        str(prior_trader_step.get("prior_plan_summary_file") or "").strip()
        if isinstance(prior_trader_step, dict)
        else None
    ) or None
    planned_tickers = _planned_tickers_from_plan_summary(plan_payload)
    planned_set = {ticker for ticker in planned_tickers if ticker}
    gap["planner_planned_rows"] = len(planned_set)

    skip_counts_nonzero = _nonzero_counts_map(plan_payload.get("skip_counts"))
    gap["planner_skip_counts_nonzero"] = dict(sorted(skip_counts_nonzero.items()))

    enforce_daily_weather_live_only = _bool_from_any(prior_payload.get("enforce_daily_weather_live_only"))
    gap["enforce_daily_weather_live_only"] = enforce_daily_weather_live_only
    routine_max_hours_to_close = _parse_float(plan_payload.get("routine_max_hours_to_close"))
    gap["routine_max_hours_to_close"] = routine_max_hours_to_close

    balance_blocked = False
    if isinstance(balance_step, dict) and not bool(balance_step.get("balance_live_ready")):
        balance_blocked = True
    if isinstance(balance_smoke_step, dict) and not bool(balance_smoke_step.get("kalshi_ok")):
        balance_blocked = True

    risk_cap_signal = False
    for key, value in skip_counts_nonzero.items():
        lower_key = str(key).strip().lower()
        if "risk" in lower_key or "budget" in lower_key:
            if int(value) > 0:
                risk_cap_signal = True
                break
    if not risk_cap_signal:
        remaining_risk = _parse_float(plan_payload.get("daily_weather_allocator_remaining_unallocated_risk_dollars"))
        if isinstance(remaining_risk, float) and remaining_risk <= 0.0:
            risk_cap_signal = True

    weather_history_unhealthy_signal = (
        _coerce_int(plan_payload.get("weather_history_unhealthy_filtered"), 0) > 0
    )

    reason_priority = [
        "gap_out_of_scope_family",
        "gap_daily_weather_only_mode",
        "gap_no_orderable_side_on_recheck",
        "gap_maker_only_requirement",
        "gap_edge_below_min",
        "gap_net_edge_below_min",
        "gap_risk_cap",
        "gap_balance",
        "gap_hours_to_close",
        "gap_weather_history_unhealthy",
        "gap_unknown",
    ]
    weather_daily_families = {"daily_rain", "daily_temperature", "daily_snow"}
    climate_families = weather_daily_families | {"monthly_climate_anomaly"}

    unmatched_rows: list[dict[str, Any]] = []
    reason_counts: dict[str, int] = {}
    for row in tradable_rows:
        ticker = str(row.get("market_ticker") or "").strip().upper()
        if not ticker:
            continue
        if ticker in planned_set:
            continue

        family = str(row.get("contract_family") or "").strip().lower()
        hours_to_close = _parse_float(row.get("hours_to_close"))
        reference_source = str(row.get("theoretical_reference_source") or "").strip().lower()
        reference_usable = _bool_from_any(row.get("theoretical_reference_usable"))
        availability_state = str(row.get("availability_state") or "").strip().lower()

        reason_candidates: list[str] = []
        if family not in climate_families:
            reason_candidates.append("gap_out_of_scope_family")
        if enforce_daily_weather_live_only and family not in weather_daily_families:
            reason_candidates.append("gap_daily_weather_only_mode")
        if (not reference_usable) or availability_state not in {"tradable", "hot"}:
            reason_candidates.append("gap_no_orderable_side_on_recheck")
        if "ask" in reference_source:
            reason_candidates.append("gap_maker_only_requirement")
        if _count_from_map(skip_counts_nonzero, "maker_edge_below_min") > 0:
            reason_candidates.append("gap_edge_below_min")
        if _count_from_map(skip_counts_nonzero, "maker_edge_net_fees_below_min") > 0:
            reason_candidates.append("gap_net_edge_below_min")
        if risk_cap_signal:
            reason_candidates.append("gap_risk_cap")
        if balance_blocked:
            reason_candidates.append("gap_balance")
        if isinstance(hours_to_close, float) and isinstance(routine_max_hours_to_close, float):
            if hours_to_close > routine_max_hours_to_close:
                reason_candidates.append("gap_hours_to_close")
        if weather_history_unhealthy_signal and family in weather_daily_families:
            reason_candidates.append("gap_weather_history_unhealthy")
        if not reason_candidates:
            reason_candidates.append("gap_unknown")

        primary_reason = "gap_unknown"
        for reason in reason_priority:
            if reason in reason_candidates:
                primary_reason = reason
                break

        reason_counts[primary_reason] = int(reason_counts.get(primary_reason, 0)) + 1
        unmatched_rows.append(
            {
                **row,
                "gap_primary_reason": primary_reason,
                "gap_reason_candidates": reason_candidates,
            }
        )

    unmatched_rows.sort(
        key=lambda item: (
            float(_parse_float(item.get("theoretical_edge_net")) or 0.0),
            str(item.get("market_ticker") or ""),
        ),
        reverse=True,
    )

    gap["router_tradable_not_planned_count"] = len(unmatched_rows)
    gap["router_tradable_not_planned_tickers"] = [
        str(item.get("market_ticker") or "").strip().upper()
        for item in unmatched_rows[:50]
        if str(item.get("market_ticker") or "").strip()
    ]
    gap["router_vs_planner_gap_reason_counts"] = dict(
        sorted(reason_counts.items(), key=lambda item: (-int(item[1]), item[0]))
    )
    gap["router_vs_planner_gap_top_rows"] = unmatched_rows[: max(1, int(top_n))]

    if not unmatched_rows:
        gap["status"] = "reconciled"
        gap["reason"] = "all_router_tradable_rows_present_in_planner_output"
        return gap

    gap["status"] = "gap_detected"
    gap["reason"] = "router_tradable_rows_missing_from_planner_output"
    return gap


def _collect_wakeup_transition_tickers(steps: list[dict[str, Any]]) -> list[str]:
    collected: list[str] = []
    seen: set[str] = set()
    for step in steps:
        name = str(step.get("name") or "").strip()
        if not name.startswith("daily_weather_ticker_refresh"):
            continue
        values = step.get("endpoint_to_orderable_transition_tickers")
        if not isinstance(values, list):
            continue
        for value in values:
            ticker = str(value or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            collected.append(ticker)
    return collected


def _estimate_wakeup_to_candidate_count(
    *,
    wakeup_tickers: list[str],
    priors_csv: Path,
    history_csv: Path,
) -> int:
    if not wakeup_tickers:
        return 0
    wakeup_set = {str(value or "").strip().upper() for value in wakeup_tickers if str(value or "").strip()}
    if not wakeup_set:
        return 0

    prior_by_ticker: dict[str, dict[str, Any]] = {}
    if priors_csv.exists():
        try:
            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    ticker = str(row.get("market_ticker") or "").strip().upper()
                    if ticker and ticker in wakeup_set:
                        prior_by_ticker[ticker] = row
        except Exception:
            prior_by_ticker = {}

    latest_by_ticker: dict[str, dict[str, Any]] = {}
    latest_ts_by_ticker: dict[str, datetime] = {}
    if history_csv.exists():
        try:
            with history_csv.open("r", newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    ticker = str(row.get("market_ticker") or "").strip().upper()
                    if not ticker or ticker not in wakeup_set:
                        continue
                    captured = _parse_iso(row.get("captured_at"))
                    if not isinstance(captured, datetime):
                        captured = datetime.fromtimestamp(0, tz=timezone.utc)
                    prev_ts = latest_ts_by_ticker.get(ticker)
                    if prev_ts is None or captured > prev_ts:
                        latest_ts_by_ticker[ticker] = captured
                        latest_by_ticker[ticker] = row
        except Exception:
            latest_by_ticker = {}

    candidate_count = 0
    for ticker in wakeup_set:
        prior_row = prior_by_ticker.get(ticker) or {}
        history_row = latest_by_ticker.get(ticker) or {}
        if not prior_row or not history_row:
            continue
        yes_bid = _parse_float(history_row.get("yes_bid_dollars"))
        no_bid = _parse_float(history_row.get("no_bid_dollars"))
        has_orderable_bid = _is_orderable_price(yes_bid) or _is_orderable_price(no_bid)
        fair_yes = _parse_float(prior_row.get("fair_yes_probability"))
        fair_no = _parse_float(prior_row.get("fair_no_probability"))
        if has_orderable_bid and (isinstance(fair_yes, float) or isinstance(fair_no, float)):
            candidate_count += 1
    return candidate_count


def _estimate_wakeup_to_planned_order_count(
    *,
    wakeup_tickers: list[str],
    wakeup_plan_step: dict[str, Any] | None,
) -> int:
    if not wakeup_tickers or not isinstance(wakeup_plan_step, dict):
        return 0
    wakeup_set = {str(value or "").strip().upper() for value in wakeup_tickers if str(value or "").strip()}
    if not wakeup_set:
        return 0
    top_plan_tickers = wakeup_plan_step.get("top_plans_tickers")
    if not isinstance(top_plan_tickers, list):
        return 0
    count = 0
    for value in top_plan_tickers:
        ticker = str(value or "").strip().upper()
        if ticker and ticker in wakeup_set:
            count += 1
    return count


def _write_report(
    *,
    run_report_path: Path,
    latest_report_path: Path,
    report: dict[str, Any],
) -> None:
    run_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    latest_report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def main() -> int:
    repo_root = Path(os.environ["BETBOT_REPO_ROOT"])
    output_dir = Path(os.environ["BETBOT_OUTPUT_DIR"])
    requested_env_file = Path(os.environ["BETBOT_ENV_FILE"])
    env_resolution = _resolve_env_file(requested_path=requested_env_file, repo_root=repo_root)
    env_file = Path(str(env_resolution.get("env_file_effective") or requested_env_file))
    env_file_values: dict[str, str] = {}
    env_file_parse_error: str | None = None
    try:
        env_file_values = _parse_env_values(env_file)
    except Exception as exc:  # pragma: no cover - defensive fallback
        env_file_values = {}
        env_file_parse_error = str(exc)
    weather_history_token_state = _resolve_weather_history_token(
        env_values=env_file_values,
        repo_root=repo_root,
    )
    history_csv = Path(os.environ["BETBOT_HISTORY_CSV"])
    priors_csv = Path(os.environ["BETBOT_PRIORS_CSV"])

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_id = f"hourly_alpha_overnight::{run_stamp}"
    run_root = output_dir / "overnight_alpha"
    run_logs = run_root / "logs" / run_stamp
    run_reports = run_root / "reports"
    run_logs.mkdir(parents=True, exist_ok=True)
    run_reports.mkdir(parents=True, exist_ok=True)

    run_report_path = run_reports / f"overnight_alpha_{run_stamp}.json"
    latest_report_path = output_dir / "overnight_alpha_latest.json"
    shadow_bankroll_enabled = _is_enabled(os.environ.get("BETBOT_SHADOW_BANKROLL_ENABLED"), default=True)
    shadow_bankroll_start_dollars = max(
        0.0,
        float(os.environ.get("BETBOT_SHADOW_BANKROLL_START_DOLLARS", "1000") or 1000.0),
    )
    paper_live_enabled = _is_enabled(os.environ.get("BETBOT_PAPER_LIVE_ENABLED"), default=True)
    paper_live_start_dollars = max(
        0.0,
        float(os.environ.get("BETBOT_PAPER_LIVE_START_DOLLARS", str(shadow_bankroll_start_dollars)) or shadow_bankroll_start_dollars),
    )
    paper_live_state_file_raw = str(
        os.environ.get("BETBOT_PAPER_LIVE_STATE_FILE")
        or (output_dir / "overnight_alpha" / "paper_live_account_state.json")
    ).strip()
    paper_live_state_file = Path(paper_live_state_file_raw).expanduser()
    if not paper_live_state_file.is_absolute():
        paper_live_state_file = (repo_root / paper_live_state_file).resolve()
    paper_live_risk_profile = str(
        os.environ.get("BETBOT_PAPER_LIVE_RISK_PROFILE", "growth_aggressive") or "growth_aggressive"
    ).strip() or "growth_aggressive"
    paper_live_kelly_fraction = max(
        0.0,
        float(os.environ.get("BETBOT_PAPER_LIVE_KELLY_FRACTION", "0.5") or 0.5),
    )
    paper_live_kelly_high_conf_max = max(
        0.0,
        float(os.environ.get("BETBOT_PAPER_LIVE_KELLY_HIGH_CONF_MAX", "0.75") or 0.75),
    )
    paper_live_max_open_risk_pct = max(
        0.01,
        float(os.environ.get("BETBOT_PAPER_LIVE_MAX_OPEN_RISK_PCT", "0.25") or 0.25),
    )
    paper_live_max_family_risk_pct = max(
        0.01,
        float(os.environ.get("BETBOT_PAPER_LIVE_MAX_FAMILY_RISK_PCT", "0.15") or 0.15),
    )
    paper_live_max_strip_risk_pct = max(
        0.01,
        float(os.environ.get("BETBOT_PAPER_LIVE_MAX_STRIP_RISK_PCT", "0.08") or 0.08),
    )
    paper_live_max_single_position_risk_pct = max(
        0.005,
        float(os.environ.get("BETBOT_PAPER_LIVE_MAX_SINGLE_POSITION_RISK_PCT", "0.06") or 0.06),
    )
    paper_live_max_new_attempts_per_run = max(
        1,
        _coerce_int(os.environ.get("BETBOT_PAPER_LIVE_MAX_NEW_ATTEMPTS_PER_RUN"), 8),
    )
    paper_live_family_allowlist = [
        token.strip().lower()
        for token in str(os.environ.get("BETBOT_PAPER_LIVE_FAMILY_ALLOWLIST", "monthly_climate_anomaly") or "").split(",")
        if token.strip()
    ]
    if not paper_live_family_allowlist:
        paper_live_family_allowlist = ["monthly_climate_anomaly"]
    paper_live_allow_random_cancels = _is_enabled(
        os.environ.get("BETBOT_PAPER_LIVE_ALLOW_RANDOM_CANCELS"),
        default=False,
    )
    paper_live_size_from_current_equity = _is_enabled(
        os.environ.get("BETBOT_PAPER_LIVE_SIZE_FROM_CURRENT_EQUITY"),
        default=True,
    )
    paper_live_require_live_eligible_hint = _is_enabled(
        os.environ.get("BETBOT_PAPER_LIVE_REQUIRE_LIVE_ELIGIBLE_HINT"),
        default=False,
    )
    shadow_bankroll_state_file_raw = str(
        os.environ.get("BETBOT_SHADOW_BANKROLL_STATE_FILE")
        or (output_dir / "overnight_alpha" / "shadow_bankroll_state.json")
    ).strip()
    shadow_bankroll_state_file = Path(shadow_bankroll_state_file_raw).expanduser()
    if not shadow_bankroll_state_file.is_absolute():
        shadow_bankroll_state_file = (repo_root / shadow_bankroll_state_file).resolve()
    launcher = _choose_betbot_launcher(repo_root)
    started_at = _now_iso()

    min_seconds_between_runs = max(0.0, float(os.environ.get("BETBOT_MIN_SECONDS_BETWEEN_RUNS", "2700")))
    latest_existing = _load_json(latest_report_path)
    if isinstance(latest_existing, dict):
        previous_started = _parse_iso(latest_existing.get("run_started_at_utc"))
        if previous_started is not None:
            age_seconds = max(0.0, (datetime.now(timezone.utc) - previous_started).total_seconds())
            if age_seconds < min_seconds_between_runs:
                pilot_execution_evidence = _top_level_pilot_execution_evidence(
                    step=None,
                    climate_router_pilot=None,
                )
                report = {
                    "run_id": run_id,
                    "run_started_at_utc": started_at,
                    "run_finished_at_utc": _now_iso(),
                    "run_stamp_utc": run_stamp,
                    "repo_root": str(repo_root),
                    "mode": "research_dry_run_only",
                    "live_orders_allowed": False,
                    "env_file_requested": env_resolution.get("env_file_requested"),
                    "env_file_effective": env_resolution.get("env_file_effective"),
                    "env_file_source": env_resolution.get("env_file_source"),
                    "env_file_resolution_reason": env_resolution.get("env_file_resolution_reason"),
                    "env_file_kalshi_ready": env_resolution.get("env_file_kalshi_ready"),
                    "env_file_kalshi_ready_reason": env_resolution.get("env_file_kalshi_ready_reason"),
                    "env_file_loaded_key_count": len(env_file_values),
                    "env_file_parse_error": env_file_parse_error,
                    "weather_history_token_present": weather_history_token_state.get("weather_history_token_present"),
                    "weather_history_token_env_key": weather_history_token_state.get("weather_history_token_env_key"),
                    "weather_history_token_source": weather_history_token_state.get("weather_history_token_source"),
                    "weather_history_token_file_used": weather_history_token_state.get("weather_history_token_file_used"),
                    "betbot_launcher": launcher,
                    "steps": [
                        _synthetic_step(
                            name="run_interval_guard",
                            status="skipped_recent_run",
                            ok=True,
                            reason="previous_run_too_recent",
                            payload={
                                "previous_run_started_at_utc": previous_started.isoformat(),
                                "previous_run_age_seconds": round(age_seconds, 3),
                                "min_seconds_between_runs": round(min_seconds_between_runs, 3),
                            },
                        )
                    ],
                    "overall_status": "skipped_recent_run",
                    "failed_steps": [],
                    "degraded_reasons": [],
                    "pipeline_ready": False,
                    "live_ready": False,
                    "live_blockers": ["run_skipped_recent_run"],
                    **_pilot_execution_report_fields(pilot_execution_evidence),
                    **_shadow_bankroll_report_fields(
                        _shadow_bankroll_defaults(
                            enabled=shadow_bankroll_enabled,
                            start_dollars=shadow_bankroll_start_dollars,
                            state_file=shadow_bankroll_state_file,
                            status="observer_not_run",
                            reason="shadow_bankroll_skipped_recent_run",
                        )
                    ),
                    **_paper_live_report_fields(
                        _paper_live_defaults(
                            enabled=paper_live_enabled,
                            start_dollars=paper_live_start_dollars,
                            state_file=paper_live_state_file,
                            status="observer_not_run",
                            reason="paper_live_skipped_recent_run",
                        )
                    ),
                    **_paper_live_scorecard_fields(
                        _paper_live_defaults(
                            enabled=paper_live_enabled,
                            start_dollars=paper_live_start_dollars,
                            state_file=paper_live_state_file,
                            status="observer_not_run",
                            reason="paper_live_skipped_recent_run",
                        )
                    ),
                    "balance_heartbeat": _top_level_balance_heartbeat(None),
                    "execution_frontier": _top_level_execution_frontier(None),
                    "decision_identity": _top_level_decision_identity(None),
                    "probe_policy": _top_level_probe_policy(None),
                    "lane_comparison": _default_lane_comparison(
                        status="not_run",
                        reason="skipped_recent_run",
                    ),
                    "no_candidates_diagnostics": None,
                    "daily_weather_funnel": _top_level_daily_weather_funnel(
                        prior_trader_step=None,
                        weather_prior_state_after={},
                        weather_history_state={},
                    ),
                    "top_market_ticker": None,
                    "top_market_contract_family": None,
                    "fair_yes_probability_raw": None,
                    "execution_probability_guarded": None,
                    "fill_probability_source": None,
                    "empirical_fill_weight": None,
                    "heuristic_fill_weight": None,
                    "probe_lane_used": None,
                    "probe_reason": None,
                    "history_csv_path": str(history_csv),
                    "history_csv_mtime_utc": _file_meta(history_csv).get("mtime_utc"),
                    "daily_weather_board_age_seconds": None,
                    "weather_station_history_cache_age_seconds": None,
                    "balance_heartbeat_age_seconds": None,
                    "skip_reason": "skipped_recent_run",
                    "freshness": {
                        "history_csv": _file_meta(history_csv),
                        "priors_csv": _file_meta(priors_csv),
                        "ws_state_latest_json": _file_meta(output_dir / "kalshi_ws_state_latest.json"),
                        "execution_journal_db": _file_meta(output_dir / "kalshi_execution_journal.sqlite3"),
                        "latest_prior_trader_summary": {
                            "path": None,
                            "exists": False,
                            "mtime_utc": None,
                            "age_seconds": None,
                            "size_bytes": None,
                        },
                    },
                }
                report["runtime_version"] = _runtime_version_for_report(
                    run_started_at=started_at,
                    run_id=run_id,
                    repo_root=repo_root,
                    prior_trader_step=None,
                    frontier_step=None,
                    as_of=datetime.now(timezone.utc),
                )
                _write_report(run_report_path=run_report_path, latest_report_path=latest_report_path, report=report)
                print(
                    json.dumps(
                        {
                            "status": report["overall_status"],
                            "run_report": str(run_report_path),
                            "latest_report": str(latest_report_path),
                            "failed_steps": [],
                            "degraded_reasons": [],
                        },
                        indent=2,
                    )
                )
                return 0

    steps: list[dict[str, Any]] = []
    preflight_step = _run_preflight(launcher=launcher, cwd=repo_root, run_dir=run_logs)
    steps.append(preflight_step)
    if not bool(preflight_step.get("ok")):
        failed_steps = [step["name"] for step in steps if not bool(step.get("ok"))]
        pilot_execution_evidence = _top_level_pilot_execution_evidence(
            step=None,
            climate_router_pilot=None,
        )
        report = {
            "run_id": run_id,
            "run_started_at_utc": started_at,
            "run_finished_at_utc": _now_iso(),
            "run_stamp_utc": run_stamp,
            "repo_root": str(repo_root),
            "mode": "research_dry_run_only",
            "live_orders_allowed": False,
            "env_file_requested": env_resolution.get("env_file_requested"),
            "env_file_effective": env_resolution.get("env_file_effective"),
            "env_file_source": env_resolution.get("env_file_source"),
            "env_file_resolution_reason": env_resolution.get("env_file_resolution_reason"),
            "env_file_kalshi_ready": env_resolution.get("env_file_kalshi_ready"),
            "env_file_kalshi_ready_reason": env_resolution.get("env_file_kalshi_ready_reason"),
            "env_file_loaded_key_count": len(env_file_values),
            "env_file_parse_error": env_file_parse_error,
            "weather_history_token_present": weather_history_token_state.get("weather_history_token_present"),
            "weather_history_token_env_key": weather_history_token_state.get("weather_history_token_env_key"),
            "weather_history_token_source": weather_history_token_state.get("weather_history_token_source"),
            "weather_history_token_file_used": weather_history_token_state.get("weather_history_token_file_used"),
            "betbot_launcher": launcher,
            "steps": steps,
            "overall_status": "failed",
            "failed_steps": failed_steps,
            "degraded_reasons": [],
            "pipeline_ready": False,
            "live_ready": False,
            "live_blockers": ["preflight_failed"],
            **_pilot_execution_report_fields(pilot_execution_evidence),
            **_shadow_bankroll_report_fields(
                _shadow_bankroll_defaults(
                    enabled=shadow_bankroll_enabled,
                    start_dollars=shadow_bankroll_start_dollars,
                    state_file=shadow_bankroll_state_file,
                    status="observer_not_run",
                    reason="shadow_bankroll_preflight_failed",
                )
            ),
            **_paper_live_report_fields(
                _paper_live_defaults(
                    enabled=paper_live_enabled,
                    start_dollars=paper_live_start_dollars,
                    state_file=paper_live_state_file,
                    status="observer_not_run",
                    reason="paper_live_preflight_failed",
                )
            ),
            **_paper_live_scorecard_fields(
                _paper_live_defaults(
                    enabled=paper_live_enabled,
                    start_dollars=paper_live_start_dollars,
                    state_file=paper_live_state_file,
                    status="observer_not_run",
                    reason="paper_live_preflight_failed",
                )
            ),
            "balance_heartbeat": _top_level_balance_heartbeat(None),
            "execution_frontier": _top_level_execution_frontier(None),
            "decision_identity": _top_level_decision_identity(None),
            "probe_policy": _top_level_probe_policy(None),
            "lane_comparison": _default_lane_comparison(
                status="not_run",
                reason="preflight_failed",
            ),
            "no_candidates_diagnostics": None,
            "daily_weather_funnel": _top_level_daily_weather_funnel(
                prior_trader_step=None,
                weather_prior_state_after={},
                weather_history_state={},
            ),
            "top_market_ticker": None,
            "top_market_contract_family": None,
            "fair_yes_probability_raw": None,
            "execution_probability_guarded": None,
            "fill_probability_source": None,
            "empirical_fill_weight": None,
            "heuristic_fill_weight": None,
            "probe_lane_used": None,
            "probe_reason": None,
            "history_csv_path": str(history_csv),
            "history_csv_mtime_utc": _file_meta(history_csv).get("mtime_utc"),
            "daily_weather_board_age_seconds": None,
            "weather_station_history_cache_age_seconds": None,
            "balance_heartbeat_age_seconds": None,
            "freshness": {
                "history_csv": _file_meta(history_csv),
                "priors_csv": _file_meta(priors_csv),
                "ws_state_latest_json": _file_meta(output_dir / "kalshi_ws_state_latest.json"),
                "execution_journal_db": _file_meta(output_dir / "kalshi_execution_journal.sqlite3"),
                "latest_prior_trader_summary": {
                    "path": None,
                    "exists": False,
                    "mtime_utc": None,
                    "age_seconds": None,
                    "size_bytes": None,
                },
            },
        }
        report["runtime_version"] = _runtime_version_for_report(
            run_started_at=started_at,
            run_id=run_id,
            repo_root=repo_root,
            prior_trader_step=None,
            frontier_step=None,
            as_of=datetime.now(timezone.utc),
        )
        _write_report(run_report_path=run_report_path, latest_report_path=latest_report_path, report=report)
        print(
            json.dumps(
                {
                    "status": report["overall_status"],
                    "run_report": str(run_report_path),
                    "latest_report": str(latest_report_path),
                    "failed_steps": failed_steps,
                    "degraded_reasons": [],
                },
                indent=2,
            )
        )
        return 1

    steps.append(
        _run_step(
            name="micro_status",
            launcher=launcher,
            args=[
                "kalshi-micro-status",
                "--env-file",
                str(env_file),
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
    )

    capture_max_hours_to_close = max(
        1.0,
        float(os.environ.get("BETBOT_CAPTURE_MAX_HOURS_TO_CLOSE", "4000")),
    )
    capture_page_limit = max(
        1,
        int(os.environ.get("BETBOT_CAPTURE_PAGE_LIMIT", "200")),
    )
    capture_max_pages = max(
        1,
        int(os.environ.get("BETBOT_CAPTURE_MAX_PAGES", "12")),
    )
    steps.append(
        _run_step(
            name="capture",
            launcher=launcher,
            args=[
                "kalshi-nonsports-capture",
                "--env-file",
                str(env_file),
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
                "--max-hours-to-close",
                str(capture_max_hours_to_close),
                "--page-limit",
                str(capture_page_limit),
                "--max-pages",
                str(capture_max_pages),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
    )

    allowed_weather_contract_families = _parse_csv_list(
        os.environ.get("BETBOT_WEATHER_ALLOWED_CONTRACT_FAMILIES")
    )
    if not allowed_weather_contract_families:
        allowed_weather_contract_families = ["daily_rain", "daily_temperature"]
    daily_weather_ticker_refresh_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_ENABLED"),
        default=True,
    )
    daily_weather_ticker_refresh_max_markets = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_MAX_MARKETS", "20")),
    )
    daily_weather_ticker_refresh_watch_max_markets = max(
        0,
        int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_MAX_MARKETS", "4")),
    )
    daily_weather_ticker_refresh_watch_interval_runs = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_WATCH_INTERVAL_RUNS", "3")),
    )
    daily_weather_ticker_refresh_state_file = str(
        os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_STATE_FILE")
        or (output_dir / "overnight_alpha" / "daily_weather_ticker_refresh_state.json")
    )
    daily_weather_wakeup_burst_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_WAKEUP_BURST_ENABLED"),
        default=True,
    )
    daily_weather_wakeup_burst_max_markets = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_WAKEUP_BURST_MAX_MARKETS", "4")),
    )
    daily_weather_wakeup_burst_sleep_seconds = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_WAKEUP_BURST_SLEEP_SECONDS", "1")),
    )
    daily_weather_wakeup_reprioritize_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_WAKEUP_REPRIORITIZE_ENABLED"),
        default=True,
    )
    daily_weather_micro_watch_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_ENABLED"),
        default=True,
    )
    daily_weather_micro_watch_interval_seconds = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_INTERVAL_SECONDS", "180")),
    )
    daily_weather_micro_watch_max_polls = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_MAX_POLLS", "4")),
    )
    daily_weather_micro_watch_active_hours_to_close = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_ACTIVE_HOURS_TO_CLOSE", "2")),
    )
    daily_weather_micro_watch_max_markets = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_MAX_MARKETS", "12")),
    )
    daily_weather_micro_watch_include_unknown_hours_to_close = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_MICRO_WATCH_INCLUDE_UNKNOWN_HOURS_TO_CLOSE"),
        default=False,
    )
    daily_weather_ticker_refresh_on_base_capture = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_TICKER_REFRESH_ON_BASE_CAPTURE"),
        default=True,
    )
    base_refresh_step: dict[str, Any] | None = None
    micro_watch_step: dict[str, Any] | None = None
    micro_watch_poll_steps: list[dict[str, Any]] = []
    wakeup_burst_step: dict[str, Any] | None = None
    if daily_weather_ticker_refresh_enabled and daily_weather_ticker_refresh_on_base_capture:
        base_refresh_step = _run_daily_weather_ticker_refresh(
            env_values=env_file_values,
            priors_csv=priors_csv,
            history_csv=history_csv,
            allowed_contract_families=allowed_weather_contract_families,
            timeout_seconds=max(1.0, float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            max_markets=daily_weather_ticker_refresh_max_markets,
            captured_at=datetime.now(timezone.utc),
        )
        base_refresh_step["name"] = "daily_weather_ticker_refresh_base"
        base_refresh_step["base_capture_refresh"] = True
        steps.append(base_refresh_step)
    elif daily_weather_ticker_refresh_enabled:
        steps.append(
            _synthetic_step(
                name="daily_weather_ticker_refresh_base",
                status="skipped_base_refresh_disabled",
                ok=True,
                reason="daily_weather_ticker_refresh_on_base_capture_disabled",
            )
        )
    else:
        steps.append(
            _synthetic_step(
                name="daily_weather_ticker_refresh_base",
                status="skipped_disabled",
                ok=True,
                reason="daily_weather_ticker_refresh_disabled",
            )
        )

    if daily_weather_micro_watch_enabled and daily_weather_ticker_refresh_enabled:
        micro_watch_result = _run_daily_weather_micro_watch(
            env_values=env_file_values,
            priors_csv=priors_csv,
            history_csv=history_csv,
            allowed_contract_families=allowed_weather_contract_families,
            timeout_seconds=max(1.0, float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            max_markets=daily_weather_micro_watch_max_markets,
            captured_at=datetime.now(timezone.utc),
            poll_interval_seconds=daily_weather_micro_watch_interval_seconds,
            max_polls=daily_weather_micro_watch_max_polls,
            active_hours_to_close=daily_weather_micro_watch_active_hours_to_close,
            include_unknown_hours_to_close=daily_weather_micro_watch_include_unknown_hours_to_close,
        )
        micro_watch_poll_steps = list(micro_watch_result.get("poll_steps") or [])
        for poll_step in micro_watch_poll_steps:
            steps.append(poll_step)
        micro_watch_step = (
            micro_watch_result.get("summary_step")
            if isinstance(micro_watch_result.get("summary_step"), dict)
            else None
        )
        if isinstance(micro_watch_step, dict):
            steps.append(micro_watch_step)
    elif daily_weather_micro_watch_enabled:
        micro_watch_step = _synthetic_step(
            name="daily_weather_micro_watch",
            status="skipped_daily_weather_ticker_refresh_disabled",
            ok=True,
            reason="daily_weather_ticker_refresh_disabled",
        )
        steps.append(micro_watch_step)
    else:
        micro_watch_step = _synthetic_step(
            name="daily_weather_micro_watch",
            status="skipped_disabled",
            ok=True,
            reason="daily_weather_micro_watch_disabled",
        )
        steps.append(micro_watch_step)

    wakeup_transition_tickers_before_burst = _collect_wakeup_transition_tickers(steps)
    if (
        daily_weather_wakeup_burst_enabled
        and daily_weather_ticker_refresh_enabled
        and wakeup_transition_tickers_before_burst
    ):
        if daily_weather_wakeup_burst_sleep_seconds > 0:
            time.sleep(daily_weather_wakeup_burst_sleep_seconds)
        wakeup_burst_tickers = wakeup_transition_tickers_before_burst[:daily_weather_wakeup_burst_max_markets]
        wakeup_burst_step = _run_daily_weather_ticker_refresh(
            env_values=env_file_values,
            priors_csv=priors_csv,
            history_csv=history_csv,
            allowed_contract_families=allowed_weather_contract_families,
            timeout_seconds=max(1.0, float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            max_markets=max(1, len(wakeup_burst_tickers)),
            captured_at=datetime.now(timezone.utc),
            explicit_market_tickers=wakeup_burst_tickers,
        )
        wakeup_burst_step["name"] = "daily_weather_ticker_refresh_wakeup_burst"
        wakeup_burst_step["wakeup_transition_source"] = "base_refresh"
        wakeup_burst_step["wakeup_transition_tickers"] = wakeup_burst_tickers
        steps.append(wakeup_burst_step)
    elif daily_weather_wakeup_burst_enabled:
        steps.append(
            _synthetic_step(
                name="daily_weather_ticker_refresh_wakeup_burst",
                status="skipped_no_wakeup_transition",
                ok=True,
                reason="no_endpoint_to_orderable_transition_detected",
            )
        )
    else:
        steps.append(
            _synthetic_step(
                name="daily_weather_ticker_refresh_wakeup_burst",
                status="skipped_disabled",
                ok=True,
                reason="daily_weather_wakeup_burst_disabled",
            )
        )

    weather_cache = _weather_cache_state(
        output_dir=output_dir,
        max_age_hours=float(os.environ.get("BETBOT_WEATHER_CACHE_MAX_AGE_HOURS", "24")),
    )
    if bool(weather_cache.get("cache_stale")):
        weather_step = _run_step(
            name="weather_prewarm",
            launcher=launcher,
            args=[
                "kalshi-weather-prewarm",
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
                "--historical-lookback-years",
                str(int(os.environ.get("BETBOT_WEATHER_LOOKBACK_YEARS", "15"))),
                "--max-station-day-keys",
                str(int(os.environ.get("BETBOT_WEATHER_PREWARM_MAX_KEYS", "500"))),
                "--station-history-cache-max-age-hours",
                str(float(os.environ.get("BETBOT_WEATHER_CACHE_MAX_AGE_HOURS", "24"))),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        weather_step["cache_state_before"] = weather_cache
        steps.append(weather_step)
    else:
        steps.append(
            _synthetic_step(
                name="weather_prewarm",
                status="skipped_fresh_cache",
                ok=True,
                reason="weather_station_history_cache_fresh",
                payload={"cache_state_before": weather_cache},
            )
        )

    weather_prior_state_before = _weather_prior_state(
        priors_csv=priors_csv,
        max_age_hours=float(os.environ.get("BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS", "6")),
        allowed_contract_families=allowed_weather_contract_families,
    )
    force_weather_prior_refresh_reasons: list[str] = []
    if isinstance(base_refresh_step, dict):
        base_rows_appended = base_refresh_step.get("rows_appended")
        if isinstance(base_rows_appended, (int, float)) and int(base_rows_appended) > 0:
            force_weather_prior_refresh_reasons.append("base_daily_weather_refresh_appended_rows")
    if isinstance(wakeup_burst_step, dict):
        burst_rows_appended = wakeup_burst_step.get("rows_appended")
        if isinstance(burst_rows_appended, (int, float)) and int(burst_rows_appended) > 0:
            force_weather_prior_refresh_reasons.append("wakeup_transition_burst_refresh_appended_rows")
        burst_transition_count = wakeup_burst_step.get("endpoint_to_orderable_transition_count")
        if isinstance(burst_transition_count, (int, float)) and int(burst_transition_count) > 0:
            force_weather_prior_refresh_reasons.append("wakeup_transition_detected")
    force_weather_prior_refresh_reasons = list(dict.fromkeys(force_weather_prior_refresh_reasons))
    force_weather_prior_refresh_reason = (
        ",".join(force_weather_prior_refresh_reasons)
        if force_weather_prior_refresh_reasons
        else None
    )

    if bool(weather_prior_state_before.get("stale")) or force_weather_prior_refresh_reason is not None:
        weather_priors_args = [
            "kalshi-weather-priors",
            "--priors-csv",
            str(priors_csv),
            "--history-csv",
            str(history_csv),
            "--allowed-contract-families",
            ",".join(allowed_weather_contract_families),
            "--max-markets",
            str(max(1, int(os.environ.get("BETBOT_WEATHER_PRIOR_MAX_MARKETS", "30")))),
            "--timeout-seconds",
            str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            "--historical-lookback-years",
            str(int(os.environ.get("BETBOT_WEATHER_LOOKBACK_YEARS", "15"))),
            "--station-history-cache-max-age-hours",
            str(float(os.environ.get("BETBOT_WEATHER_CACHE_MAX_AGE_HOURS", "24"))),
            "--output-dir",
            str(output_dir),
        ]
        if not _is_enabled(os.environ.get("BETBOT_WEATHER_INCLUDE_NWS_GRIDPOINT_DATA"), default=True):
            weather_priors_args.append("--disable-nws-gridpoint-data")
        if not _is_enabled(os.environ.get("BETBOT_WEATHER_INCLUDE_NWS_OBSERVATIONS"), default=True):
            weather_priors_args.append("--disable-nws-observations")
        if not _is_enabled(os.environ.get("BETBOT_WEATHER_INCLUDE_NWS_ALERTS"), default=True):
            weather_priors_args.append("--disable-nws-alerts")
        if not _is_enabled(os.environ.get("BETBOT_WEATHER_INCLUDE_NCEI_NORMALS"), default=True):
            weather_priors_args.append("--disable-ncei-normals")
        if not _is_enabled(os.environ.get("BETBOT_WEATHER_INCLUDE_MRMS_QPE"), default=True):
            weather_priors_args.append("--disable-mrms-qpe")
        if not _is_enabled(os.environ.get("BETBOT_WEATHER_INCLUDE_NBM_SNAPSHOT"), default=True):
            weather_priors_args.append("--disable-nbm-snapshot")

        weather_prior_step = _run_step(
            name="weather_prior_refresh",
            launcher=launcher,
            args=weather_priors_args,
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        weather_prior_step["prior_state_before"] = weather_prior_state_before
        weather_prior_step["forced_refresh_reason"] = force_weather_prior_refresh_reason
        steps.append(weather_prior_step)
    else:
        steps.append(
            _synthetic_step(
                name="weather_prior_refresh",
                status="skipped_fresh_weather_priors",
                ok=True,
                reason=str(weather_prior_state_before.get("reason") or "weather_priors_fresh"),
                payload={"prior_state_before": weather_prior_state_before},
            )
        )

    wakeup_reprioritize_step: dict[str, Any] | None = None
    wakeup_transition_tickers_for_reprioritize = _collect_wakeup_transition_tickers(steps)
    if daily_weather_wakeup_reprioritize_enabled and wakeup_transition_tickers_for_reprioritize:
        wakeup_reprioritize_step = _run_step(
            name="prior_plan_wakeup_reprioritize",
            launcher=launcher,
            args=[
                "kalshi-micro-prior-plan",
                "--env-file",
                str(env_file),
                "--priors-csv",
                str(priors_csv),
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        wakeup_reprioritize_step["wakeup_transition_tickers"] = wakeup_transition_tickers_for_reprioritize
        wakeup_reprioritize_step["wakeup_transition_count"] = len(wakeup_transition_tickers_for_reprioritize)
        steps.append(wakeup_reprioritize_step)
    elif daily_weather_wakeup_reprioritize_enabled:
        steps.append(
            _synthetic_step(
                name="prior_plan_wakeup_reprioritize",
                status="skipped_no_wakeup_transition",
                ok=True,
                reason="no_endpoint_to_orderable_transition_detected",
            )
        )
    else:
        steps.append(
            _synthetic_step(
                name="prior_plan_wakeup_reprioritize",
                status="skipped_disabled",
                ok=True,
                reason="daily_weather_wakeup_reprioritize_disabled",
            )
        )

    frontier_refresh_step = _run_step(
        name="execution_frontier_refresh",
        launcher=launcher,
        args=[
            "kalshi-execution-frontier",
            "--output-dir",
            str(output_dir),
            "--journal-db-path",
            str(output_dir / "kalshi_execution_journal.sqlite3"),
            "--recent-rows",
            str(max(1, int(os.environ.get("BETBOT_FRONTIER_RECENT_ROWS", "20000")))),
        ],
        cwd=repo_root,
        run_dir=run_logs,
        env_overrides=env_file_values,
    )
    steps.append(frontier_refresh_step)

    micro_status_step = next((step for step in steps if step.get("name") == "micro_status"), None)
    balance_step_initial = _build_balance_heartbeat(
        micro_status_step=micro_status_step if isinstance(micro_status_step, dict) else None,
        output_dir=output_dir,
        max_age_seconds=max(0.0, float(os.environ.get("BETBOT_BALANCE_MAX_AGE_SECONDS", "900"))),
    )
    steps.append(balance_step_initial)

    balance_smoke_enabled = str(os.environ.get("BETBOT_BALANCE_SMOKE_ON_FAILURE", "1")).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }
    if balance_smoke_enabled and not bool(balance_step_initial.get("balance_live_ready")):
        balance_smoke_step = _run_step(
            name="balance_smoke",
            launcher=launcher,
            args=[
                "live-smoke",
                "--env-file",
                str(env_file),
                "--skip-odds-provider-check",
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
                "--output-dir",
                str(output_dir),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        # Balance smoke is diagnostic: a failing smoke should enrich blockers,
        # not mark the whole orchestration step as failed.
        balance_smoke_step["ok"] = True
        balance_smoke_step["diagnostic_only"] = True
        steps.append(balance_smoke_step)
    elif balance_smoke_enabled:
        steps.append(
            _synthetic_step(
                name="balance_smoke",
                status="skipped_balance_ready",
                ok=True,
                reason="balance_live_ready",
            )
        )
    else:
        steps.append(
            _synthetic_step(
                name="balance_smoke",
                status="skipped_disabled",
                ok=True,
                reason="balance_smoke_disabled",
            )
        )

    climate_router_enabled = _is_enabled(
        os.environ.get("BETBOT_CLIMATE_ROUTER_ENABLED"),
        default=True,
    )
    climate_router_step: dict[str, Any] | None = None
    climate_router_skip_realtime_collect = _is_enabled(
        os.environ.get("BETBOT_CLIMATE_ROUTER_SKIP_REALTIME_COLLECT"),
        default=False,
    )
    climate_router_market_tickers = _parse_csv_list(os.environ.get("BETBOT_CLIMATE_ROUTER_MARKET_TICKERS"))
    climate_router_ws_channels = _parse_csv_list(os.environ.get("BETBOT_CLIMATE_ROUTER_WS_CHANNELS"))
    if not climate_router_ws_channels:
        climate_router_ws_channels = [
            "orderbook_snapshot",
            "orderbook_delta",
            "ticker",
            "public_trades",
            "user_fills",
            "market_positions",
        ]
    climate_router_seed_recent_markets = _is_enabled(
        os.environ.get("BETBOT_CLIMATE_ROUTER_SEED_RECENT_MARKETS"),
        default=True,
    )
    climate_router_include_contract_families = _parse_csv_list(
        os.environ.get("BETBOT_CLIMATE_ROUTER_INCLUDE_CONTRACT_FAMILIES")
    )
    if not climate_router_include_contract_families:
        climate_router_include_contract_families = [
            "daily_rain",
            "daily_temperature",
            "daily_snow",
            "monthly_climate_anomaly",
        ]
    if climate_router_enabled:
        climate_router_args = [
            "kalshi-climate-realtime-router",
            "--env-file",
            str(env_file),
            "--priors-csv",
            str(priors_csv),
            "--history-csv",
            str(history_csv),
            "--output-dir",
            str(output_dir),
            "--availability-db-path",
            str(
                os.environ.get(
                    "BETBOT_CLIMATE_ROUTER_AVAILABILITY_DB_PATH",
                    str(output_dir / "kalshi_climate_availability.sqlite3"),
                )
            ),
            "--ws-channels",
            ",".join(climate_router_ws_channels),
            "--run-seconds",
            str(max(1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_RUN_SECONDS", "20")))),
            "--max-markets",
            str(max(1, int(os.environ.get("BETBOT_CLIMATE_ROUTER_MAX_MARKETS", "40")))),
            "--recent-markets-min-updated-seconds",
            str(max(1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_RECENT_MARKETS_MIN_UPDATED_SECONDS", "900")))),
            "--recent-markets-timeout-seconds",
            str(max(1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_RECENT_MARKETS_TIMEOUT_SECONDS", "8")))),
            "--ws-state-max-age-seconds",
            str(max(1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_WS_STATE_MAX_AGE_SECONDS", "30")))),
            "--min-theoretical-edge-net-fees",
            str(float(os.environ.get("BETBOT_CLIMATE_ROUTER_MIN_THEORETICAL_EDGE_NET_FEES", "0.005"))),
            "--max-quote-age-seconds",
            str(max(0.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_MAX_QUOTE_AGE_SECONDS", "900")))),
            "--planning-bankroll",
            str(max(0.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_PLANNING_BANKROLL_DOLLARS", "40")))),
            "--daily-risk-cap",
            str(max(0.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_DAILY_RISK_CAP_DOLLARS", "3")))),
            "--max-risk-per-bet",
            str(max(0.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_MAX_RISK_PER_BET_DOLLARS", "1")))),
            "--availability-lookback-days",
            str(max(1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_AVAILABILITY_LOOKBACK_DAYS", "7")))),
            "--availability-recent-seconds",
            str(max(1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_AVAILABILITY_RECENT_SECONDS", "900")))),
            "--availability-hot-trade-window-seconds",
            str(max(1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_AVAILABILITY_HOT_TRADE_WINDOW_SECONDS", "300")))),
            "--include-contract-families",
            ",".join(climate_router_include_contract_families),
        ]
        if climate_router_market_tickers:
            climate_router_args.extend(["--market-tickers", ",".join(climate_router_market_tickers)])
        if not climate_router_seed_recent_markets:
            climate_router_args.append("--no-seed-recent-markets")
        if climate_router_skip_realtime_collect:
            climate_router_args.append("--skip-realtime-collect")
        climate_router_step = _run_step(
            name="climate_realtime_router",
            launcher=launcher,
            args=climate_router_args,
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        # Availability routing is additive control-plane intelligence and should
        # not hard-fail overnight orchestration when upstream WS/API is flaky.
        climate_router_step["ok"] = True
        climate_router_step["diagnostic_only"] = True
        steps.append(climate_router_step)
    else:
        climate_router_step = _synthetic_step(
            name="climate_realtime_router",
            status="skipped_disabled",
            ok=True,
            reason="climate_router_disabled",
        )
        steps.append(climate_router_step)

    climate_router_pilot_enabled = _is_enabled(
        os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_ENABLED"),
        default=False,
    )
    climate_router_pilot_allowed_classes = ",".join(
        _parse_csv_list(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_ALLOWED_CLASSES"))
    )
    climate_router_pilot_allowed_families = ",".join(
        _parse_csv_list(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_ALLOWED_FAMILIES"))
    )
    climate_router_pilot_excluded_families = ",".join(
        _parse_csv_list(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_EXCLUDED_FAMILIES"))
    )
    climate_router_pilot_policy_scope_override_enabled = _is_enabled(
        os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_POLICY_SCOPE_OVERRIDE_ENABLED"),
        default=True,
    )
    climate_router_pilot_summary_json = str(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_SUMMARY_JSON") or "").strip()
    if not climate_router_pilot_summary_json and isinstance(climate_router_step, dict):
        routed_summary_path = str(climate_router_step.get("output_file") or "").strip()
        if routed_summary_path and Path(routed_summary_path).exists():
            climate_router_pilot_summary_json = routed_summary_path
    disable_daily_weather_live_only = _is_enabled(
        os.environ.get("BETBOT_DISABLE_DAILY_WEATHER_LIVE_ONLY"),
        default=False,
    )

    prior_trader_args = [
        "kalshi-micro-prior-trader",
        "--env-file",
        str(env_file),
        "--priors-csv",
        str(priors_csv),
        "--history-csv",
        str(history_csv),
        "--output-dir",
        str(output_dir),
        "--timeout-seconds",
        str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
        "--enforce-ws-state-authority",
        "--disable-auto-refresh-weather-priors",
        "--disable-auto-refresh-priors",
    ]
    if disable_daily_weather_live_only:
        prior_trader_args.append("--disable-daily-weather-live-only")
    if climate_router_pilot_enabled:
        prior_trader_args.append("--climate-router-pilot-enabled")
    if climate_router_pilot_enabled and climate_router_pilot_policy_scope_override_enabled:
        prior_trader_args.append("--climate-router-pilot-policy-scope-override-enabled")
    if climate_router_pilot_summary_json:
        prior_trader_args.extend(["--climate-router-summary-json", climate_router_pilot_summary_json])
    prior_trader_args.extend(
        [
            "--climate-router-pilot-max-orders-per-run",
            str(max(0, int(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_MAX_ORDERS_PER_RUN", "1")))),
            "--climate-router-pilot-contracts-cap",
            str(max(1, int(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_CONTRACTS_CAP", "1")))),
            "--climate-router-pilot-required-ev-dollars",
            str(max(0.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_REQUIRED_EV_DOLLARS", "0.05")))),
        ]
    )
    if climate_router_pilot_allowed_classes:
        prior_trader_args.extend(["--climate-router-pilot-allowed-classes", climate_router_pilot_allowed_classes])
    if climate_router_pilot_allowed_families:
        prior_trader_args.extend(["--climate-router-pilot-allowed-families", climate_router_pilot_allowed_families])
    if climate_router_pilot_excluded_families:
        prior_trader_args.extend(["--climate-router-pilot-excluded-families", climate_router_pilot_excluded_families])
    frontier_report_path = str(frontier_refresh_step.get("output_file") or "").strip()
    if bool(frontier_refresh_step.get("ok")) and frontier_report_path and Path(frontier_report_path).exists():
        prior_trader_args.extend(
            [
                "--execution-frontier-report-json",
                frontier_report_path,
                "--execution-frontier-max-report-age-seconds",
                str(max(0.0, float(os.environ.get("BETBOT_FRONTIER_MAX_AGE_SECONDS", "10800")))),
            ]
        )

    stale_recovery_enabled = _is_enabled(
        os.environ.get("BETBOT_DAILY_WEATHER_STALE_RECOVERY_ENABLED"),
        default=True,
    )
    stale_recovery_max_retries = max(
        0,
        int(os.environ.get("BETBOT_DAILY_WEATHER_STALE_RECOVERY_MAX_RETRIES", "1")),
    )
    stale_recovery_sleep_seconds = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_STALE_RECOVERY_SLEEP_SECONDS", "2")),
    )
    stale_recovery_attempts = 0
    stale_recovery_triggered = False
    stale_recovery_resolved = False
    stale_recovery_capture_max_hours_to_close = max(
        capture_max_hours_to_close,
        float(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_MAX_HOURS_TO_CLOSE", "6000")),
    )
    stale_recovery_capture_page_limit = max(
        capture_page_limit,
        int(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_PAGE_LIMIT", "300")),
    )
    stale_recovery_capture_max_pages = max(
        capture_max_pages,
        int(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_MAX_PAGES", "24")),
    )

    prior_trader_step = _run_step(
        name="prior_trader_dry_run",
        launcher=launcher,
        args=prior_trader_args,
        cwd=repo_root,
        run_dir=run_logs,
        env_overrides=env_file_values,
    )
    steps.append(prior_trader_step)

    while (
        stale_recovery_enabled
        and stale_recovery_attempts < stale_recovery_max_retries
        and _is_daily_weather_board_stale_gate(prior_trader_step)
    ):
        stale_recovery_triggered = True
        stale_recovery_attempts += 1
        if stale_recovery_sleep_seconds > 0:
            time.sleep(stale_recovery_sleep_seconds)
        capture_retry_step = _run_step(
            name=f"capture_recovery_daily_weather_{stale_recovery_attempts}",
            launcher=launcher,
            args=[
                "kalshi-nonsports-capture",
                "--env-file",
                str(env_file),
                "--history-csv",
                str(history_csv),
                "--output-dir",
                str(output_dir),
                "--timeout-seconds",
                str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
                "--max-hours-to-close",
                str(stale_recovery_capture_max_hours_to_close),
                "--page-limit",
                str(stale_recovery_capture_page_limit),
                "--max-pages",
                str(stale_recovery_capture_max_pages),
            ],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        capture_retry_step["stale_recovery_attempt"] = stale_recovery_attempts
        capture_retry_step["stale_recovery_trigger_gate_status"] = "daily_weather_board_stale"
        capture_retry_step["stale_recovery_capture_max_hours_to_close"] = stale_recovery_capture_max_hours_to_close
        capture_retry_step["stale_recovery_capture_page_limit"] = stale_recovery_capture_page_limit
        capture_retry_step["stale_recovery_capture_max_pages"] = stale_recovery_capture_max_pages
        steps.append(capture_retry_step)

        if daily_weather_ticker_refresh_enabled:
            ticker_refresh_step = _run_daily_weather_ticker_refresh(
                env_values=env_file_values,
                priors_csv=priors_csv,
                history_csv=history_csv,
                allowed_contract_families=allowed_weather_contract_families,
                timeout_seconds=float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15")),
                max_markets=daily_weather_ticker_refresh_max_markets,
                captured_at=datetime.now(timezone.utc),
            )
            ticker_refresh_step["stale_recovery_attempt"] = stale_recovery_attempts
            ticker_refresh_step["stale_recovery_trigger_gate_status"] = "daily_weather_board_stale"
            steps.append(ticker_refresh_step)
        else:
            steps.append(
                _synthetic_step(
                    name=f"daily_weather_ticker_refresh_{stale_recovery_attempts}",
                    status="skipped_disabled",
                    ok=True,
                    reason="daily_weather_ticker_refresh_disabled",
                    payload={
                        "stale_recovery_attempt": stale_recovery_attempts,
                        "stale_recovery_trigger_gate_status": "daily_weather_board_stale",
                    },
                )
            )

        prior_trader_step = _run_step(
            name=f"prior_trader_dry_run_retry_{stale_recovery_attempts}",
            launcher=launcher,
            args=prior_trader_args,
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        prior_trader_step["stale_recovery_attempt"] = stale_recovery_attempts
        prior_trader_step["stale_recovery_trigger_gate_status"] = "daily_weather_board_stale"
        steps.append(prior_trader_step)

    if stale_recovery_triggered and not _is_daily_weather_board_stale_gate(prior_trader_step):
        stale_recovery_resolved = True

    recovery_capture_step = _latest_step_with_prefix(steps, "capture_recovery_daily_weather")
    daily_weather_recovery_failure_kind = _daily_weather_recovery_failure_kind(
        stale_recovery_triggered=stale_recovery_triggered,
        stale_recovery_resolved=stale_recovery_resolved,
        recovery_capture_step=recovery_capture_step if isinstance(recovery_capture_step, dict) else None,
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
    )
    daily_weather_recovery_alert_window_hours = max(
        0.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_ALERT_WINDOW_HOURS", "6")),
    )
    daily_weather_recovery_alert_threshold = max(
        1,
        int(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_ALERT_THRESHOLD", "3")),
    )
    daily_weather_recovery_alert_max_events = max(
        25,
        int(os.environ.get("BETBOT_DAILY_WEATHER_RECOVERY_ALERT_MAX_EVENTS", "500")),
    )
    daily_weather_recovery_alert_state = _daily_weather_recovery_alert_state(
        run_root=run_root,
        run_id=run_id,
        run_started_at_utc=started_at,
        stale_recovery_triggered=stale_recovery_triggered,
        stale_recovery_resolved=stale_recovery_resolved,
        failure_kind=daily_weather_recovery_failure_kind,
        recovery_capture_status=(
            str(recovery_capture_step.get("status") or "").strip()
            if isinstance(recovery_capture_step, dict)
            else None
        ),
        recovery_capture_scan_status=(
            str(recovery_capture_step.get("scan_status") or "").strip()
            if isinstance(recovery_capture_step, dict)
            else None
        ),
        window_hours=daily_weather_recovery_alert_window_hours,
        threshold=daily_weather_recovery_alert_threshold,
        max_events=daily_weather_recovery_alert_max_events,
    )
    steps.append(
        _synthetic_step(
            name="daily_weather_stale_recovery_health",
            status="alert" if bool(daily_weather_recovery_alert_state.get("alert_triggered")) else "ready",
            ok=True,
            reason=(
                f"failure_burst:{daily_weather_recovery_failure_kind or 'none'}"
                if bool(daily_weather_recovery_alert_state.get("alert_triggered"))
                else "within_threshold"
            ),
            payload=dict(daily_weather_recovery_alert_state),
        )
    )
    # Router executes before prior-trader so the same-run summary can seed pilot promotion.
    climate_router_step = next(
        (step for step in steps if str(step.get("name") or "").strip() == "climate_realtime_router"),
        None,
    )
    daily_weather_ticker_refresh_step = next(
        (
            step
            for step in reversed(steps)
            if str(step.get("name") or "").startswith("daily_weather_ticker_refresh")
            and str(step.get("name") or "").strip()
            not in {"daily_weather_ticker_refresh_base", "daily_weather_ticker_refresh_wakeup_burst"}
        ),
        None,
    )
    daily_weather_ticker_refresh_base_step = next(
        (step for step in steps if str(step.get("name") or "").strip() == "daily_weather_ticker_refresh_base"),
        None,
    )
    micro_watch_step = next(
        (step for step in reversed(steps) if str(step.get("name") or "").strip() == "daily_weather_micro_watch"),
        None,
    )
    wakeup_burst_step = next(
        (step for step in reversed(steps) if str(step.get("name") or "").strip() == "daily_weather_ticker_refresh_wakeup_burst"),
        None,
    )
    balance_step_for_gap = next((step for step in steps if step.get("name") == "balance_heartbeat"), None)
    balance_smoke_step_for_gap = next((step for step in steps if step.get("name") == "balance_smoke"), None)
    climate_router_shadow_plan = _build_climate_router_shadow_plan(
        climate_router_step=climate_router_step if isinstance(climate_router_step, dict) else None,
    )
    steps.append(
        _synthetic_step(
            name="climate_router_shadow_plan",
            status=str(climate_router_shadow_plan.get("status") or "not_run"),
            ok=True,
            reason=(
                str(climate_router_shadow_plan.get("reason") or "").strip()
                or None
            ),
            payload=dict(climate_router_shadow_plan),
        )
    )
    router_vs_planner_gap = _build_router_vs_planner_gap(
        climate_router_step=climate_router_step if isinstance(climate_router_step, dict) else None,
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
        balance_step=balance_step_for_gap if isinstance(balance_step_for_gap, dict) else None,
        balance_smoke_step=balance_smoke_step_for_gap if isinstance(balance_smoke_step_for_gap, dict) else None,
    )
    steps.append(
        _synthetic_step(
            name="router_vs_planner_gap",
            status=str(router_vs_planner_gap.get("status") or "not_run"),
            ok=True,
            reason=(
                str(router_vs_planner_gap.get("reason") or "").strip()
                or None
            ),
            payload=dict(router_vs_planner_gap),
        )
    )
    pilot_execution_summary_path = output_dir / "pilot_execution_evidence_latest.json"
    pilot_execution_evidence_step = _run_step(
        name="pilot_execution_evidence",
        launcher=[launcher[0]],
        args=[
            str(repo_root / "scripts" / "pilot_execution_evidence.py"),
            "--outputs-dir",
            str(output_dir),
            "--output-json",
            str(pilot_execution_summary_path),
        ],
        cwd=repo_root,
        run_dir=run_logs,
        env_overrides=env_file_values,
    )
    pilot_execution_evidence_step["pilot_execution_summary_file"] = str(pilot_execution_summary_path)
    pilot_execution_evidence_step["output_file"] = str(pilot_execution_summary_path)
    pilot_execution_evidence_step["diagnostic_only"] = True
    if not bool(pilot_execution_evidence_step.get("ok")):
        pilot_execution_evidence_step["status"] = (
            str(pilot_execution_evidence_step.get("status") or "").strip() or "observer_failed"
        )
        pilot_execution_evidence_step["reason"] = (
            str(pilot_execution_evidence_step.get("reason") or "").strip() or "pilot_execution_evidence_observer_failed"
        )
        pilot_execution_evidence_step["observer_failure"] = True
        pilot_execution_evidence_step["ok"] = True
    else:
        observer_payload = _load_json(pilot_execution_summary_path)
        first_attempt_evidence = observer_payload.get("first_attempt_evidence") if isinstance(observer_payload, dict) else {}
        first_attempt_status = (
            str(first_attempt_evidence.get("status") or "").strip()
            if isinstance(first_attempt_evidence, dict)
            else ""
        )
        pilot_execution_evidence_step["status"] = first_attempt_status or "observer_ready"
    steps.append(pilot_execution_evidence_step)

    # Keep pilot scorecard freshness current on every overnight run.
    alpha_scoreboard_step = _run_step(
        name="alpha_scoreboard_refresh",
        launcher=launcher,
        args=[
            "alpha-scoreboard",
            "--output-dir",
            str(output_dir),
            "--planning-bankroll",
            str(shadow_bankroll_start_dollars),
        ],
        cwd=repo_root,
        run_dir=run_logs,
        env_overrides=env_file_values,
    )
    alpha_scoreboard_step["diagnostic_only"] = True
    if not bool(alpha_scoreboard_step.get("ok")):
        alpha_scoreboard_step["status"] = (
            str(alpha_scoreboard_step.get("status") or "").strip() or "observer_failed"
        )
        alpha_scoreboard_step["reason"] = (
            str(alpha_scoreboard_step.get("reason") or "").strip() or "alpha_scoreboard_refresh_failed"
        )
        alpha_scoreboard_step["observer_failure"] = True
        alpha_scoreboard_step["ok"] = True
    else:
        alpha_scoreboard_step["status"] = (
            str(alpha_scoreboard_step.get("status") or "").strip() or "observer_ready"
        )
    steps.append(alpha_scoreboard_step)

    lane_comparison_errors: list[str] = []
    lane_comparison = _default_lane_comparison(
        status="not_run",
        reason="lane_comparison_observer_not_run",
    )
    lane_comparison_enabled = _is_enabled(
        os.environ.get("BETBOT_LANE_COMPARISON_ENABLED"),
        default=True,
    )
    if lane_comparison_enabled:
        lane_compare_snapshot_dir = run_logs / "lane_comparison_snapshot"
        lane_snapshot_inputs: dict[str, Any] = {}
        lane_compare_priors_csv, priors_snapshot_info = _snapshot_lane_input_artifact(
            artifact_name="priors_csv",
            source_path=priors_csv,
            snapshot_dir=lane_compare_snapshot_dir,
            snapshot_filename="kalshi_nonsports_priors.snapshot.csv",
            required_for_run=True,
            errors=lane_comparison_errors,
            missing_error_key="lane_compare_priors_snapshot_missing_source",
            copy_error_prefix="lane_compare_priors_snapshot_copy_failed",
        )
        lane_snapshot_inputs["priors_csv"] = priors_snapshot_info

        lane_compare_history_csv, history_snapshot_info = _snapshot_lane_input_artifact(
            artifact_name="history_csv",
            source_path=history_csv,
            snapshot_dir=lane_compare_snapshot_dir,
            snapshot_filename="kalshi_nonsports_history.snapshot.csv",
            required_for_run=True,
            errors=lane_comparison_errors,
            missing_error_key="lane_compare_history_snapshot_missing_source",
            copy_error_prefix="lane_compare_history_snapshot_copy_failed",
        )
        lane_snapshot_inputs["history_csv"] = history_snapshot_info

        lane_compare_frontier_source_path: Path | None = None
        if bool(frontier_refresh_step.get("ok")) and frontier_report_path:
            frontier_candidate = Path(frontier_report_path)
            if not frontier_candidate.is_absolute():
                frontier_candidate = (repo_root / frontier_candidate).resolve()
            if frontier_candidate.exists():
                lane_compare_frontier_source_path = frontier_candidate
        lane_compare_frontier_report_json, frontier_snapshot_info = _snapshot_lane_input_artifact(
            artifact_name="execution_frontier_report_json",
            source_path=lane_compare_frontier_source_path,
            snapshot_dir=lane_compare_snapshot_dir,
            snapshot_filename="execution_frontier_report.snapshot.json",
            required_for_run=lane_compare_frontier_source_path is not None,
            errors=lane_comparison_errors,
            missing_error_key="lane_compare_frontier_snapshot_missing_source",
            copy_error_prefix="lane_compare_frontier_snapshot_copy_failed",
        )
        lane_snapshot_inputs["execution_frontier_report_json"] = frontier_snapshot_info

        lane_compare_climate_summary_source_path: Path | None = None
        if climate_router_pilot_summary_json:
            lane_compare_climate_summary_source_path = Path(climate_router_pilot_summary_json)
            if not lane_compare_climate_summary_source_path.is_absolute():
                lane_compare_climate_summary_source_path = (repo_root / lane_compare_climate_summary_source_path).resolve()
        lane_compare_climate_summary_json, climate_summary_snapshot_info = _snapshot_lane_input_artifact(
            artifact_name="climate_router_summary_json",
            source_path=lane_compare_climate_summary_source_path,
            snapshot_dir=lane_compare_snapshot_dir,
            snapshot_filename="climate_router_summary.snapshot.json",
            required_for_run=lane_compare_climate_summary_source_path is not None,
            errors=lane_comparison_errors,
            missing_error_key="lane_compare_climate_summary_snapshot_missing_source",
            copy_error_prefix="lane_compare_climate_summary_snapshot_copy_failed",
        )
        lane_snapshot_inputs["climate_router_summary_json"] = climate_summary_snapshot_info

        lane_compare_ws_state_source_text = str(
            os.environ.get("BETBOT_WS_STATE_JSON") or (output_dir / "kalshi_ws_state_latest.json")
        ).strip()
        lane_compare_ws_state_source_path = Path(lane_compare_ws_state_source_text).expanduser()
        if not lane_compare_ws_state_source_path.is_absolute():
            lane_compare_ws_state_source_path = (repo_root / lane_compare_ws_state_source_path).resolve()
        lane_compare_ws_state_json, ws_state_snapshot_info = _snapshot_lane_input_artifact(
            artifact_name="ws_state_json",
            source_path=lane_compare_ws_state_source_path,
            snapshot_dir=lane_compare_snapshot_dir,
            snapshot_filename="kalshi_ws_state_latest.snapshot.json",
            required_for_run=True,
            errors=lane_comparison_errors,
            missing_error_key="lane_compare_ws_state_snapshot_missing_source",
            copy_error_prefix="lane_compare_ws_state_snapshot_copy_failed",
        )
        lane_snapshot_inputs["ws_state_json"] = ws_state_snapshot_info

        required_lane_snapshot_inputs = [
            item for item in lane_snapshot_inputs.values() if isinstance(item, dict) and bool(item.get("required_for_run"))
        ]
        lane_compare_fully_frozen = bool(required_lane_snapshot_inputs) and all(
            bool(item.get("frozen")) and bool(item.get("used_snapshot")) for item in required_lane_snapshot_inputs
        )
        lane_compare_comparison_basis = (
            "same_snapshot_same_filters_frozen_artifacts"
            if lane_compare_fully_frozen
            else "same_snapshot_same_filters_partial_freeze"
        )

        lane_compare_execute_common_args = [
            "kalshi-micro-prior-execute",
            "--env-file",
            str(env_file),
            "--priors-csv",
            str(lane_compare_priors_csv or priors_csv),
            "--history-csv",
            str(lane_compare_history_csv or history_csv),
            "--output-dir",
            str(output_dir),
            "--timeout-seconds",
            str(float(os.environ.get("BETBOT_TIMEOUT_SECONDS", "15"))),
            "--enforce-ws-state-authority",
            "--daily-weather-board-max-age-seconds",
            str(max(0.0, float(os.environ.get("BETBOT_DAILY_WEATHER_BOARD_MAX_AGE_SECONDS", "900")))),
        ]
        if isinstance(lane_compare_ws_state_json, Path):
            lane_compare_execute_common_args.extend(["--ws-state-json", str(lane_compare_ws_state_json)])
        if disable_daily_weather_live_only:
            lane_compare_execute_common_args.append("--disable-daily-weather-live-only")
        if climate_router_pilot_enabled:
            lane_compare_execute_common_args.append("--climate-router-pilot-enabled")
        if climate_router_pilot_enabled and climate_router_pilot_policy_scope_override_enabled:
            lane_compare_execute_common_args.append("--climate-router-pilot-policy-scope-override-enabled")
        if isinstance(lane_compare_climate_summary_json, Path):
            lane_compare_execute_common_args.extend(["--climate-router-summary-json", str(lane_compare_climate_summary_json)])
        lane_compare_execute_common_args.extend(
            [
                "--climate-router-pilot-max-orders-per-run",
                str(max(0, int(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_MAX_ORDERS_PER_RUN", "1")))),
                "--climate-router-pilot-contracts-cap",
                str(max(1, int(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_CONTRACTS_CAP", "1")))),
                "--climate-router-pilot-required-ev-dollars",
                str(max(0.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_REQUIRED_EV_DOLLARS", "0.05")))),
            ]
        )
        if climate_router_pilot_allowed_classes:
            lane_compare_execute_common_args.extend(
                ["--climate-router-pilot-allowed-classes", climate_router_pilot_allowed_classes]
            )
        if climate_router_pilot_allowed_families:
            lane_compare_execute_common_args.extend(
                ["--climate-router-pilot-allowed-families", climate_router_pilot_allowed_families]
            )
        if climate_router_pilot_excluded_families:
            lane_compare_execute_common_args.extend(
                ["--climate-router-pilot-excluded-families", climate_router_pilot_excluded_families]
            )
        if isinstance(lane_compare_frontier_report_json, Path):
            lane_compare_execute_common_args.extend(
                [
                    "--execution-frontier-report-json",
                    str(lane_compare_frontier_report_json),
                    "--execution-frontier-max-report-age-seconds",
                    str(max(0.0, float(os.environ.get("BETBOT_FRONTIER_MAX_AGE_SECONDS", "10800")))),
                ]
            )

        configured_min_selected_probability = _parse_float(
            os.environ.get("BETBOT_MIN_SELECTED_FAIR_PROBABILITY")
        )
        configured_min_live_selected_probability = _parse_float(
            os.environ.get("BETBOT_MIN_LIVE_SELECTED_FAIR_PROBABILITY")
        )
        if isinstance(configured_min_selected_probability, float):
            lane_compare_execute_common_args.extend(
                ["--min-selected-fair-probability", str(configured_min_selected_probability)]
            )
        if isinstance(configured_min_live_selected_probability, float):
            lane_compare_execute_common_args.extend(
                ["--min-live-selected-fair-probability", str(configured_min_live_selected_probability)]
            )

        lane_compare_maker_step = _run_step(
            name="lane_compare_execute_maker_edge",
            launcher=launcher,
            args=lane_compare_execute_common_args + ["--selection-lane", "maker_edge"],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        lane_compare_maker_step["snapshot_frozen"] = lane_compare_fully_frozen
        lane_compare_maker_step["snapshot_priors_csv"] = str(lane_compare_priors_csv or priors_csv)
        lane_compare_maker_step["snapshot_history_csv"] = str(lane_compare_history_csv or history_csv)
        lane_compare_maker_step["snapshot_inputs"] = lane_snapshot_inputs
        lane_compare_maker_step["diagnostic_only"] = True
        if not bool(lane_compare_maker_step.get("ok")):
            lane_compare_maker_step["status"] = (
                str(lane_compare_maker_step.get("status") or "").strip() or "observer_failed"
            )
            lane_compare_maker_step["reason"] = (
                str(lane_compare_maker_step.get("reason") or "").strip() or "lane_compare_maker_edge_failed"
            )
            lane_compare_maker_step["observer_failure"] = True
            lane_compare_maker_step["ok"] = True
            lane_comparison_errors.append("maker_edge_lane_compare_execute_failed")
        else:
            lane_compare_maker_step["status"] = (
                str(lane_compare_maker_step.get("status") or "").strip() or "observer_ready"
            )
        steps.append(lane_compare_maker_step)

        lane_compare_probability_step = _run_step(
            name="lane_compare_execute_probability_first",
            launcher=launcher,
            args=lane_compare_execute_common_args + ["--selection-lane", "probability_first"],
            cwd=repo_root,
            run_dir=run_logs,
            env_overrides=env_file_values,
        )
        lane_compare_probability_step["snapshot_frozen"] = lane_compare_fully_frozen
        lane_compare_probability_step["snapshot_priors_csv"] = str(lane_compare_priors_csv or priors_csv)
        lane_compare_probability_step["snapshot_history_csv"] = str(lane_compare_history_csv or history_csv)
        lane_compare_probability_step["snapshot_inputs"] = lane_snapshot_inputs
        lane_compare_probability_step["diagnostic_only"] = True
        if not bool(lane_compare_probability_step.get("ok")):
            lane_compare_probability_step["status"] = (
                str(lane_compare_probability_step.get("status") or "").strip() or "observer_failed"
            )
            lane_compare_probability_step["reason"] = (
                str(lane_compare_probability_step.get("reason") or "").strip()
                or "lane_compare_probability_first_execute_failed"
            )
            lane_compare_probability_step["observer_failure"] = True
            lane_compare_probability_step["ok"] = True
            lane_comparison_errors.append("probability_first_lane_compare_execute_failed")
        else:
            lane_compare_probability_step["status"] = (
                str(lane_compare_probability_step.get("status") or "").strip() or "observer_ready"
            )
        steps.append(lane_compare_probability_step)

        lane_compare_maker_summary: dict[str, Any] | None = None
        maker_summary_file = str(lane_compare_maker_step.get("output_file") or "").strip()
        if maker_summary_file:
            lane_compare_maker_summary = _load_json(Path(maker_summary_file))
            if not isinstance(lane_compare_maker_summary, dict):
                lane_compare_maker_summary = None
                lane_comparison_errors.append("maker_edge_lane_compare_summary_parse_failed")
        else:
            lane_comparison_errors.append("maker_edge_lane_compare_missing_output_file")

        lane_compare_probability_summary: dict[str, Any] | None = None
        probability_summary_file = str(lane_compare_probability_step.get("output_file") or "").strip()
        if probability_summary_file:
            lane_compare_probability_summary = _load_json(Path(probability_summary_file))
            if not isinstance(lane_compare_probability_summary, dict):
                lane_compare_probability_summary = None
                lane_comparison_errors.append("probability_first_lane_compare_summary_parse_failed")
        else:
            lane_comparison_errors.append("probability_first_lane_compare_missing_output_file")

        executed_lane = "maker_edge"
        if isinstance(lane_compare_maker_summary, dict):
            executed_lane_candidate = str(lane_compare_maker_summary.get("selection_lane") or "").strip().lower()
            if executed_lane_candidate:
                executed_lane = executed_lane_candidate
        lane_comparison = _build_lane_comparison(
            maker_edge_summary=lane_compare_maker_summary,
            probability_first_summary=lane_compare_probability_summary,
            executed_lane=executed_lane,
            comparison_basis=lane_compare_comparison_basis,
            errors=lane_comparison_errors,
            fully_frozen=lane_compare_fully_frozen,
            snapshot_inputs=lane_snapshot_inputs,
        )
    else:
        lane_comparison = _default_lane_comparison(
            status="skipped_disabled",
            reason="lane_comparison_disabled",
        )

    failed_steps = [step["name"] for step in steps if not bool(step.get("ok"))]
    degraded_reasons: list[str] = []

    prior_trader_step = _latest_step_with_prefix(steps, "prior_trader_dry_run")
    if isinstance(prior_trader_step, dict) and bool(prior_trader_step.get("allow_live_orders_effective")):
        degraded_reasons.append("prior_trader_reported_allow_live_orders_effective=true_in_dry_run_setup")

    overall_status = "ok"
    if failed_steps:
        overall_status = "failed"
    elif degraded_reasons:
        overall_status = "degraded"

    balance_step = next((step for step in steps if step.get("name") == "balance_heartbeat"), None)
    balance_smoke_step = next((step for step in steps if step.get("name") == "balance_smoke"), None)
    frontier_step = next((step for step in steps if step.get("name") == "execution_frontier_refresh"), None)
    climate_router_step = next((step for step in steps if step.get("name") == "climate_realtime_router"), None)
    weather_prior_step = next((step for step in steps if step.get("name") == "weather_prior_refresh"), None)
    weather_prior_state_after = _weather_prior_state(
        priors_csv=priors_csv,
        max_age_hours=float(os.environ.get("BETBOT_WEATHER_PRIOR_MAX_AGE_HOURS", "6")),
        allowed_contract_families=allowed_weather_contract_families,
    )
    weather_history_state = _weather_history_readiness_state(
        priors_csv=priors_csv,
        allowed_contract_families=allowed_weather_contract_families,
    )
    steps.append(
        _synthetic_step(
            name="weather_history_health",
            status="ready" if not weather_history_state.get("weather_history_parse_error") else "degraded",
            ok=True,
            reason=(
                None
                if not weather_history_state.get("weather_history_parse_error")
                else f"weather_history_parse_error:{weather_history_state.get('weather_history_parse_error')}"
            ),
            payload=dict(weather_history_state),
        )
    )

    live_blockers: list[str] = []
    if isinstance(balance_step, dict) and not bool(balance_step.get("balance_live_ready")):
        live_blockers.extend(list(balance_step.get("balance_blockers") or []))
    if isinstance(balance_smoke_step, dict) and not bool(balance_smoke_step.get("kalshi_ok")):
        failure_kind = str(balance_smoke_step.get("kalshi_failure_kind") or "").strip()
        if failure_kind:
            live_blockers.append(f"balance_smoke_{failure_kind}")
    if not bool(env_resolution.get("env_file_kalshi_ready")):
        live_blockers.append("env_file_missing_kalshi_credentials")
    if isinstance(frontier_step, dict):
        frontier_status = str(frontier_step.get("status") or "").strip().lower()
        if frontier_status and frontier_status != "ready":
            live_blockers.append(f"execution_frontier_{frontier_status}")
    if isinstance(prior_trader_step, dict):
        gate_status = str(prior_trader_step.get("prior_trade_gate_status") or "").strip().lower()
        if gate_status and gate_status not in {"gate_pass", "pass", "ok"}:
            live_blockers.append(f"prior_trade_gate_{gate_status}")
        capture_status = str(prior_trader_step.get("capture_status") or "").strip().lower()
        if capture_status and capture_status not in {"ready", "ok"}:
            live_blockers.append(f"capture_{capture_status}")
    if bool(weather_prior_state_after.get("stale")):
        live_blockers.append("weather_priors_stale_or_empty")
    if not bool(weather_history_token_state.get("weather_history_token_present")) and int(
        weather_history_state.get("weather_history_missing_token_count") or 0
    ) > 0:
        live_blockers.append("weather_history_missing_noaa_token")
    if int(weather_history_state.get("weather_history_station_mapping_missing_count") or 0) > 0:
        live_blockers.append("weather_history_station_mapping_missing")
    if int(weather_history_state.get("weather_history_sample_depth_block_count") or 0) > 0:
        live_blockers.append("weather_history_sample_depth_insufficient")
    if (
        bool(daily_weather_recovery_alert_state.get("alert_triggered"))
        and stale_recovery_triggered
        and not stale_recovery_resolved
    ):
        live_blockers.append("daily_weather_recovery_failure_burst")
        if str(daily_weather_recovery_failure_kind or "").strip().lower().startswith("capture_upstream_error"):
            live_blockers.append("daily_weather_recovery_upstream_error_burst")
    live_blockers = _dedupe(live_blockers)
    pipeline_ready = overall_status == "ok"
    live_ready = pipeline_ready and not live_blockers
    top_level_balance = _top_level_balance_heartbeat(balance_step if isinstance(balance_step, dict) else None)
    top_level_frontier = _top_level_execution_frontier(frontier_step if isinstance(frontier_step, dict) else None)
    decision_identity = _top_level_decision_identity(prior_trader_step if isinstance(prior_trader_step, dict) else None)
    probe_policy = _top_level_probe_policy(prior_trader_step if isinstance(prior_trader_step, dict) else None)
    climate_router_pilot = _top_level_climate_router_pilot(
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
        climate_router_step=climate_router_step if isinstance(climate_router_step, dict) else None,
    )
    shadow_bankroll = _update_shadow_bankroll(
        run_id=run_id,
        run_finished_at_utc=_now_iso(),
        enabled=shadow_bankroll_enabled,
        start_dollars=shadow_bankroll_start_dollars,
        state_file=shadow_bankroll_state_file,
        climate_router_step=climate_router_step if isinstance(climate_router_step, dict) else None,
        climate_router_shadow_plan=climate_router_shadow_plan,
        climate_router_pilot=climate_router_pilot,
    )
    paper_live_account = _paper_live_update(
        run_id=run_id,
        run_finished_at_utc=_now_iso(),
        climate_router_pilot=climate_router_pilot,
        climate_router_shadow_plan=climate_router_shadow_plan,
        enabled=paper_live_enabled,
        start_dollars=paper_live_start_dollars,
        state_file=paper_live_state_file,
        risk_profile=paper_live_risk_profile,
        kelly_fraction=paper_live_kelly_fraction,
        kelly_high_conf_max=paper_live_kelly_high_conf_max,
        max_open_risk_pct=paper_live_max_open_risk_pct,
        max_family_risk_pct=paper_live_max_family_risk_pct,
        max_strip_risk_pct=paper_live_max_strip_risk_pct,
        max_single_position_risk_pct=paper_live_max_single_position_risk_pct,
        max_new_attempts_per_run=paper_live_max_new_attempts_per_run,
        family_allowlist=paper_live_family_allowlist,
        allow_random_cancels=paper_live_allow_random_cancels,
        size_from_current_equity=paper_live_size_from_current_equity,
        require_live_eligible_hint=paper_live_require_live_eligible_hint,
    )
    pilot_execution_evidence_step = next((step for step in steps if step.get("name") == "pilot_execution_evidence"), None)
    pilot_execution_evidence = _top_level_pilot_execution_evidence(
        step=pilot_execution_evidence_step if isinstance(pilot_execution_evidence_step, dict) else None,
        climate_router_pilot=climate_router_pilot,
    )
    no_candidates_diagnostics = _top_level_no_candidates_diagnostics(
        prior_trader_step if isinstance(prior_trader_step, dict) else None
    )
    wakeup_transition_tickers = _collect_wakeup_transition_tickers(steps)
    wakeup_plan_step = next(
        (step for step in reversed(steps) if str(step.get("name") or "").strip() == "prior_plan_wakeup_reprioritize"),
        None,
    )
    wakeup_to_candidate_count = _estimate_wakeup_to_candidate_count(
        wakeup_tickers=wakeup_transition_tickers,
        priors_csv=priors_csv,
        history_csv=history_csv,
    )
    wakeup_to_planned_order_count = _estimate_wakeup_to_planned_order_count(
        wakeup_tickers=wakeup_transition_tickers,
        wakeup_plan_step=wakeup_plan_step if isinstance(wakeup_plan_step, dict) else None,
    )
    wakeup_funnel = {
        "daily_weather_watch_wakeup_count": len(wakeup_transition_tickers),
        "daily_weather_watch_wakeup_tickers": wakeup_transition_tickers[:25],
        "daily_weather_watch_wakeup_to_candidate_count": wakeup_to_candidate_count,
        "daily_weather_watch_wakeup_to_planned_order_count": wakeup_to_planned_order_count,
    }
    daily_weather_funnel = _top_level_daily_weather_funnel(
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
        weather_prior_state_after=weather_prior_state_after,
        weather_history_state=weather_history_state,
    )
    daily_weather_availability_lookback_days = max(
        1.0,
        float(os.environ.get("BETBOT_DAILY_WEATHER_AVAILABILITY_LOOKBACK_DAYS", "7") or 7.0),
    )
    daily_weather_market_availability = _build_daily_weather_market_availability_study(
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
        state_file_path=daily_weather_ticker_refresh_state_file,
        lookback_days=daily_weather_availability_lookback_days,
        top_n=20,
    )
    top_level_climate_router = {
        "status": (
            str(climate_router_step.get("status") or "").strip()
            if isinstance(climate_router_step, dict)
            else "not_run"
        ),
        "reason": (
            climate_router_step.get("reason")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "output_file": (
            climate_router_step.get("output_file")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "output_csv": (
            climate_router_step.get("output_csv")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "availability_db_path": (
            climate_router_step.get("availability_db_path")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "ws_collect_status": (
            climate_router_step.get("ws_collect_status")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "ws_events_logged": (
            climate_router_step.get("ws_events_logged")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "ticker_events_processed": (
            climate_router_step.get("ticker_events_processed")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "lifecycle_events_processed": (
            climate_router_step.get("lifecycle_events_processed")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "wakeup_transitions_processed": (
            climate_router_step.get("wakeup_transitions_processed")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "recent_market_discovery_status": (
            climate_router_step.get("recent_market_discovery_status")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "recent_market_discovery_reason": (
            climate_router_step.get("recent_market_discovery_reason")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "recent_market_discovery_tickers_count": (
            climate_router_step.get("recent_market_discovery_tickers_count")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_rows_total": (
            climate_router_step.get("climate_rows_total")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_family_counts": (
            climate_router_step.get("climate_family_counts")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_availability_state_counts": (
            climate_router_step.get("climate_availability_state_counts")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_opportunity_class_counts": (
            climate_router_step.get("climate_opportunity_class_counts")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_theoretical_positive_rows": (
            climate_router_step.get("climate_theoretical_positive_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_priced_watch_only_rows": (
            climate_router_step.get("climate_priced_watch_only_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_unpriced_model_view_rows": (
            climate_router_step.get("climate_unpriced_model_view_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_tradable_rows": (
            climate_router_step.get("climate_tradable_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_hot_rows": (
            climate_router_step.get("climate_hot_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_dead_rows": (
            climate_router_step.get("climate_dead_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_tradable_positive_rows": (
            climate_router_step.get("climate_tradable_positive_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_hot_positive_rows": (
            climate_router_step.get("climate_hot_positive_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "climate_negative_or_neutral_rows": (
            climate_router_step.get("climate_negative_or_neutral_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "top_theoretical_candidates": (
            climate_router_step.get("top_theoretical_candidates")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "top_tradable_candidates": (
            climate_router_step.get("top_tradable_candidates")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "top_watch_only_candidates": (
            climate_router_step.get("top_watch_only_candidates")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "top_waking_strips": (
            climate_router_step.get("top_waking_strips")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "strip_summaries_count": (
            climate_router_step.get("strip_summaries_count")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "routing_allocator_eligible_rows": (
            climate_router_step.get("routing_allocator_eligible_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "routing_allocator_allocated_rows": (
            climate_router_step.get("routing_allocator_allocated_rows")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "routing_allocator_total_risk_dollars": (
            climate_router_step.get("routing_allocator_total_risk_dollars")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "routing_allocator_total_expected_value_dollars": (
            climate_router_step.get("routing_allocator_total_expected_value_dollars")
            if isinstance(climate_router_step, dict)
            else None
        ),
        "family_routed_capital_budget": (
            climate_router_step.get("family_routed_capital_budget")
            if isinstance(climate_router_step, dict)
            else None
        ),
    }
    runtime_version = _runtime_version_for_report(
        run_started_at=started_at,
        run_id=run_id,
        repo_root=repo_root,
        prior_trader_step=prior_trader_step if isinstance(prior_trader_step, dict) else None,
        frontier_step=frontier_step if isinstance(frontier_step, dict) else None,
        as_of=datetime.now(timezone.utc),
    )

    latest_prior_summary_file = ""
    if isinstance(prior_trader_step, dict):
        latest_prior_summary_file = str(prior_trader_step.get("output_file") or "").strip()

    report = {
        "run_id": run_id,
        "run_started_at_utc": started_at,
        "run_finished_at_utc": _now_iso(),
        "run_stamp_utc": run_stamp,
        "repo_root": str(repo_root),
        "mode": "research_dry_run_only",
        "live_orders_allowed": False,
        "env_file_requested": env_resolution.get("env_file_requested"),
        "env_file_effective": env_resolution.get("env_file_effective"),
        "env_file_source": env_resolution.get("env_file_source"),
        "env_file_resolution_reason": env_resolution.get("env_file_resolution_reason"),
        "env_file_kalshi_ready": env_resolution.get("env_file_kalshi_ready"),
        "env_file_kalshi_ready_reason": env_resolution.get("env_file_kalshi_ready_reason"),
        "env_file_loaded_key_count": len(env_file_values),
        "env_file_parse_error": env_file_parse_error,
        "weather_history_token_present": weather_history_token_state.get("weather_history_token_present"),
        "weather_history_token_env_key": weather_history_token_state.get("weather_history_token_env_key"),
        "weather_history_token_source": weather_history_token_state.get("weather_history_token_source"),
        "weather_history_token_file_used": weather_history_token_state.get("weather_history_token_file_used"),
        "betbot_launcher": launcher,
        "steps": steps,
        "overall_status": overall_status,
        "failed_steps": failed_steps,
        "degraded_reasons": degraded_reasons,
        "pipeline_ready": pipeline_ready,
        "live_ready": live_ready,
        "live_blockers": live_blockers,
        **_pilot_execution_report_fields(pilot_execution_evidence),
        **_shadow_bankroll_report_fields(shadow_bankroll),
        **_paper_live_report_fields(paper_live_account),
        **_paper_live_scorecard_fields(paper_live_account),
        "balance_heartbeat": top_level_balance,
        "balance_smoke_status": (
            balance_smoke_step.get("smoke_status")
            if isinstance(balance_smoke_step, dict)
            else None
        ),
        "balance_smoke_failure_kind": (
            balance_smoke_step.get("kalshi_failure_kind")
            if isinstance(balance_smoke_step, dict)
            else None
        ),
        "balance_smoke_http_status": (
            balance_smoke_step.get("kalshi_http_status")
            if isinstance(balance_smoke_step, dict)
            else None
        ),
        "balance_smoke_message": (
            balance_smoke_step.get("kalshi_message")
            if isinstance(balance_smoke_step, dict)
            else None
        ),
        "execution_frontier": top_level_frontier,
        "lane_comparison": lane_comparison,
        "climate_router_enabled": climate_router_enabled,
        "climate_router_skip_realtime_collect": climate_router_skip_realtime_collect,
        "climate_router_run_seconds": max(1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_RUN_SECONDS", "20"))),
        "climate_router_max_markets": max(1, int(os.environ.get("BETBOT_CLIMATE_ROUTER_MAX_MARKETS", "40"))),
        "climate_router_market_tickers": climate_router_market_tickers,
        "climate_router_ws_channels": climate_router_ws_channels,
        "climate_router_seed_recent_markets": _is_enabled(
            os.environ.get("BETBOT_CLIMATE_ROUTER_SEED_RECENT_MARKETS"),
            default=True,
        ),
        "climate_router_recent_markets_min_updated_seconds": max(
            1.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_RECENT_MARKETS_MIN_UPDATED_SECONDS", "900"))
        ),
        "climate_router_include_contract_families": climate_router_include_contract_families,
        "disable_daily_weather_live_only": disable_daily_weather_live_only,
        "climate_router_pilot_enabled": climate_router_pilot_enabled,
        "climate_router_pilot_summary_json": climate_router_pilot_summary_json or None,
        "climate_router_pilot_max_orders_per_run": max(
            0, int(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_MAX_ORDERS_PER_RUN", "1"))
        ),
        "climate_router_pilot_contracts_cap": max(
            1, int(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_CONTRACTS_CAP", "1"))
        ),
        "climate_router_pilot_required_ev_dollars": max(
            0.0, float(os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_REQUIRED_EV_DOLLARS", "0.05"))
        ),
        "climate_router_pilot_policy_scope_override_enabled": climate_router_pilot_policy_scope_override_enabled,
        "climate_router_pilot_allowed_classes": _parse_csv_list(
            os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_ALLOWED_CLASSES")
        )
        or ["tradable"],
        "climate_router_pilot_allowed_families": _parse_csv_list(
            os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_ALLOWED_FAMILIES")
        ),
        "climate_router_pilot_excluded_families": _parse_csv_list(
            os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_EXCLUDED_FAMILIES")
        ),
        "climate_router_pilot_dedup_tickers": _is_enabled(
            os.environ.get("BETBOT_CLIMATE_ROUTER_PILOT_DEDUP_TICKERS"),
            default=True,
        ),
        "climate_router_summary": top_level_climate_router,
        "climate_router_status": top_level_climate_router.get("status"),
        "climate_router_reason": top_level_climate_router.get("reason"),
        "climate_router_output_file": top_level_climate_router.get("output_file"),
        "climate_router_output_csv": top_level_climate_router.get("output_csv"),
        "climate_router_availability_db_path": top_level_climate_router.get("availability_db_path"),
        "climate_ws_collect_status": top_level_climate_router.get("ws_collect_status"),
        "climate_ws_events_logged": top_level_climate_router.get("ws_events_logged"),
        "climate_ticker_events_processed": top_level_climate_router.get("ticker_events_processed"),
        "climate_lifecycle_events_processed": top_level_climate_router.get("lifecycle_events_processed"),
        "climate_wakeup_transitions_processed": top_level_climate_router.get("wakeup_transitions_processed"),
        "climate_recent_market_discovery_status": top_level_climate_router.get("recent_market_discovery_status"),
        "climate_recent_market_discovery_reason": top_level_climate_router.get("recent_market_discovery_reason"),
        "climate_recent_market_discovery_tickers_count": top_level_climate_router.get(
            "recent_market_discovery_tickers_count"
        ),
        "climate_rows_total": top_level_climate_router.get("climate_rows_total"),
        "climate_family_counts": top_level_climate_router.get("climate_family_counts"),
        "climate_availability_state_counts": top_level_climate_router.get("climate_availability_state_counts"),
        "climate_opportunity_class_counts": top_level_climate_router.get("climate_opportunity_class_counts"),
        "climate_theoretical_positive_rows": top_level_climate_router.get("climate_theoretical_positive_rows"),
        "climate_priced_watch_only_rows": top_level_climate_router.get("climate_priced_watch_only_rows"),
        "climate_unpriced_model_view_rows": top_level_climate_router.get("climate_unpriced_model_view_rows"),
        "climate_tradable_rows": top_level_climate_router.get("climate_tradable_rows"),
        "climate_hot_rows": top_level_climate_router.get("climate_hot_rows"),
        "climate_dead_rows": top_level_climate_router.get("climate_dead_rows"),
        "climate_tradable_positive_rows": top_level_climate_router.get("climate_tradable_positive_rows"),
        "climate_hot_positive_rows": top_level_climate_router.get("climate_hot_positive_rows"),
        "climate_negative_or_neutral_rows": top_level_climate_router.get("climate_negative_or_neutral_rows"),
        "climate_top_theoretical_candidates": top_level_climate_router.get("top_theoretical_candidates"),
        "climate_top_tradable_candidates": top_level_climate_router.get("top_tradable_candidates"),
        "climate_top_watch_only_candidates": top_level_climate_router.get("top_watch_only_candidates"),
        "climate_top_waking_strips": top_level_climate_router.get("top_waking_strips"),
        "climate_strip_summaries_count": top_level_climate_router.get("strip_summaries_count"),
        "climate_routing_allocator_eligible_rows": top_level_climate_router.get("routing_allocator_eligible_rows"),
        "climate_routing_allocator_allocated_rows": top_level_climate_router.get("routing_allocator_allocated_rows"),
        "climate_routing_allocator_total_risk_dollars": top_level_climate_router.get(
            "routing_allocator_total_risk_dollars"
        ),
        "climate_routing_allocator_total_expected_value_dollars": top_level_climate_router.get(
            "routing_allocator_total_expected_value_dollars"
        ),
        "family_routed_capital_budget": top_level_climate_router.get("family_routed_capital_budget"),
        "climate_router_shadow_plan": climate_router_shadow_plan,
        "climate_router_shadow_plan_status": climate_router_shadow_plan.get("status"),
        "climate_router_shadow_plan_reason": climate_router_shadow_plan.get("reason"),
        "climate_router_shadow_plan_eligible_rows": climate_router_shadow_plan.get("eligible_rows"),
        "climate_router_shadow_plan_would_trade_rows": climate_router_shadow_plan.get("would_trade_rows"),
        "climate_router_shadow_plan_total_risk_dollars": climate_router_shadow_plan.get("total_risk_dollars"),
        "climate_router_shadow_plan_total_expected_value_dollars": climate_router_shadow_plan.get(
            "total_expected_value_dollars"
        ),
        "climate_router_shadow_plan_top_allocations": climate_router_shadow_plan.get("top_shadow_allocations"),
        "climate_router_shadow_plan_family_routed_capital_budget": climate_router_shadow_plan.get(
            "family_routed_capital_budget"
        ),
        "climate_router_shadow_plan_strip_routed_capital_budget": climate_router_shadow_plan.get(
            "strip_routed_capital_budget"
        ),
        "climate_router_pilot": climate_router_pilot,
        "climate_router_pilot_status": climate_router_pilot.get("status"),
        "climate_router_pilot_reason": climate_router_pilot.get("reason"),
        "climate_router_pilot_selection_mode": climate_router_pilot.get("selection_mode"),
        "climate_router_pilot_summary_status": climate_router_pilot.get("summary_status"),
        "climate_router_pilot_considered_rows": climate_router_pilot.get("considered_rows"),
        "climate_router_pilot_promoted_rows": climate_router_pilot.get("promoted_rows"),
        "climate_router_pilot_submitted_rows": climate_router_pilot.get("submitted_rows"),
        "climate_router_pilot_execute_considered_rows": climate_router_pilot.get("execute_considered_rows"),
        "climate_router_pilot_expected_value_dollars": climate_router_pilot.get("expected_value_dollars"),
        "climate_router_pilot_total_risk_dollars": climate_router_pilot.get("total_risk_dollars"),
        "climate_router_pilot_policy_scope_override_enabled": climate_router_pilot.get(
            "policy_scope_override_enabled"
        ),
        "climate_router_pilot_policy_scope_override_active": climate_router_pilot.get(
            "policy_scope_override_active"
        ),
        "climate_router_pilot_policy_scope_override_attempts": climate_router_pilot.get(
            "policy_scope_override_attempts"
        ),
        "climate_router_pilot_policy_scope_override_submissions": climate_router_pilot.get(
            "policy_scope_override_submissions"
        ),
        "climate_router_pilot_policy_scope_override_blocked_reason_counts": climate_router_pilot.get(
            "policy_scope_override_blocked_reason_counts"
        ),
        "climate_router_pilot_allowed_families_effective": climate_router_pilot.get("allowed_families_effective"),
        "climate_router_pilot_excluded_families_effective": climate_router_pilot.get("excluded_families_effective"),
        "climate_router_pilot_blocked_reason_counts": climate_router_pilot.get("blocked_reason_counts"),
        "climate_router_pilot_selected_tickers": climate_router_pilot.get("selected_tickers"),
        "climate_router_pilot_top_candidates": climate_router_pilot.get("top_candidates"),
        "climate_router_pilot_promoted_from_router_count": climate_router_pilot.get(
            "promoted_from_router_count"
        ),
        "climate_router_pilot_attempted_orders": climate_router_pilot.get("attempted_orders"),
        "climate_router_pilot_acked_orders": climate_router_pilot.get("acked_orders"),
        "climate_router_pilot_resting_orders": climate_router_pilot.get("resting_orders"),
        "climate_router_pilot_filled_orders": climate_router_pilot.get("filled_orders"),
        "climate_router_pilot_partial_fills": climate_router_pilot.get("partial_fills"),
        "climate_router_pilot_blocked_post_promotion_reason_counts": climate_router_pilot.get(
            "blocked_post_promotion_reason_counts"
        ),
        "climate_router_pilot_blocked_frontier_insufficient_data": climate_router_pilot.get(
            "blocked_frontier_insufficient_data"
        ),
        "climate_router_pilot_blocked_balance": climate_router_pilot.get("blocked_balance"),
        "climate_router_pilot_blocked_board_stale": climate_router_pilot.get("blocked_board_stale"),
        "climate_router_pilot_blocked_weather_history": climate_router_pilot.get("blocked_weather_history"),
        "climate_router_pilot_blocked_duplicate_ticker": climate_router_pilot.get("blocked_duplicate_ticker"),
        "climate_router_pilot_blocked_no_orderable_side_on_recheck": climate_router_pilot.get(
            "blocked_no_orderable_side_on_recheck"
        ),
        "climate_router_pilot_blocked_ev_below_threshold": climate_router_pilot.get("blocked_ev_below_threshold"),
        "climate_router_pilot_blocked_policy_scope": climate_router_pilot.get("blocked_policy_scope"),
        "climate_router_pilot_blocked_family_filter": climate_router_pilot.get("blocked_family_filter"),
        "climate_router_pilot_blocked_contract_cap": climate_router_pilot.get("blocked_contract_cap"),
        "climate_router_pilot_frontier_bootstrap_submitted_attempts": climate_router_pilot.get(
            "frontier_bootstrap_submitted_attempts"
        ),
        "climate_router_pilot_frontier_bootstrap_blocked_attempts": climate_router_pilot.get(
            "frontier_bootstrap_blocked_attempts"
        ),
        "climate_router_pilot_markout_10s_dollars": climate_router_pilot.get("markout_10s_dollars"),
        "climate_router_pilot_markout_60s_dollars": climate_router_pilot.get("markout_60s_dollars"),
        "climate_router_pilot_markout_300s_dollars": climate_router_pilot.get("markout_300s_dollars"),
        "climate_router_pilot_realized_pnl_dollars": climate_router_pilot.get("realized_pnl_dollars"),
        "climate_router_pilot_expected_vs_realized_delta": climate_router_pilot.get("expected_vs_realized_delta"),
        "router_vs_planner_gap": router_vs_planner_gap,
        "router_vs_planner_gap_status": router_vs_planner_gap.get("status"),
        "router_vs_planner_gap_reason": router_vs_planner_gap.get("reason"),
        "router_tradable_rows": router_vs_planner_gap.get("router_tradable_rows"),
        "planner_planned_rows": router_vs_planner_gap.get("planner_planned_rows"),
        "router_tradable_not_planned_count": router_vs_planner_gap.get("router_tradable_not_planned_count"),
        "router_tradable_not_planned_tickers": router_vs_planner_gap.get("router_tradable_not_planned_tickers"),
        "router_vs_planner_gap_reason_counts": router_vs_planner_gap.get("router_vs_planner_gap_reason_counts"),
        "climate_router_pilot_gap_reason_counts": router_vs_planner_gap.get("router_vs_planner_gap_reason_counts"),
        "router_vs_planner_gap_top_rows": router_vs_planner_gap.get("router_vs_planner_gap_top_rows"),
        "router_vs_planner_skip_counts_nonzero": router_vs_planner_gap.get("planner_skip_counts_nonzero"),
        "router_vs_planner_enforce_daily_weather_live_only": router_vs_planner_gap.get(
            "enforce_daily_weather_live_only"
        ),
        "router_vs_planner_routine_max_hours_to_close": router_vs_planner_gap.get("routine_max_hours_to_close"),
        "decision_identity": decision_identity,
        "probe_policy": probe_policy,
        "no_candidates_diagnostics": no_candidates_diagnostics,
        "daily_weather_funnel": daily_weather_funnel,
        "wakeup_funnel": wakeup_funnel,
        "prior_trade_gate_status": (
            prior_trader_step.get("prior_trade_gate_status")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "prior_trade_gate_blockers": (
            prior_trader_step.get("prior_trade_gate_blockers")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "prior_plan_summary_file": (
            prior_trader_step.get("prior_plan_summary_file")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_summary_status": (
            prior_trader_step.get("plan_summary_status")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_summary_error": (
            prior_trader_step.get("plan_summary_error")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_summary_planned_orders": (
            prior_trader_step.get("plan_summary_planned_orders")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_reason_dominant": (
            prior_trader_step.get("plan_skip_reason_dominant")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_reason_dominant_count": (
            prior_trader_step.get("plan_skip_reason_dominant_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_reason_dominant_share": (
            prior_trader_step.get("plan_skip_reason_dominant_share")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_counts_total": (
            prior_trader_step.get("plan_skip_counts_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_counts_nonzero_total": (
            prior_trader_step.get("plan_skip_counts_nonzero_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "plan_skip_counts_top": (
            prior_trader_step.get("plan_skip_counts_top")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_candidate_pool_size": (
            prior_trader_step.get("allowed_universe_candidate_pool_size")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_reason_dominant": (
            prior_trader_step.get("allowed_universe_skip_reason_dominant")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_reason_dominant_count": (
            prior_trader_step.get("allowed_universe_skip_reason_dominant_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_reason_dominant_share": (
            prior_trader_step.get("allowed_universe_skip_reason_dominant_share")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_counts_total": (
            prior_trader_step.get("allowed_universe_skip_counts_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_counts_nonzero_total": (
            prior_trader_step.get("allowed_universe_skip_counts_nonzero_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "allowed_universe_skip_counts_top": (
            prior_trader_step.get("allowed_universe_skip_counts_top")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_candidate_pool_size": (
            prior_trader_step.get("daily_weather_candidate_pool_size")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_total": (
            prior_trader_step.get("daily_weather_rows_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_reason_dominant": (
            prior_trader_step.get("daily_weather_skip_reason_dominant")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_reason_dominant_count": (
            prior_trader_step.get("daily_weather_skip_reason_dominant_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_reason_dominant_share": (
            prior_trader_step.get("daily_weather_skip_reason_dominant_share")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_counts_total": (
            prior_trader_step.get("daily_weather_skip_counts_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_counts_nonzero_total": (
            prior_trader_step.get("daily_weather_skip_counts_nonzero_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_skip_counts_top": (
            prior_trader_step.get("daily_weather_skip_counts_top")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_conservative_candidate": (
            prior_trader_step.get("daily_weather_rows_with_conservative_candidate")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_both_sides_candidate": (
            prior_trader_step.get("daily_weather_rows_with_both_sides_candidate")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_one_side_failed": (
            prior_trader_step.get("daily_weather_rows_with_one_side_failed")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_both_sides_failed": (
            prior_trader_step.get("daily_weather_rows_with_both_sides_failed")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_orderable_bid_rows": (
            prior_trader_step.get("daily_weather_orderable_bid_rows")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_any_orderable_bid": (
            prior_trader_step.get("daily_weather_rows_with_any_orderable_bid")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_any_orderable_ask": (
            prior_trader_step.get("daily_weather_rows_with_any_orderable_ask")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_fair_probabilities": (
            prior_trader_step.get("daily_weather_rows_with_fair_probabilities")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_rows_with_both_quote_and_fair_value": (
            prior_trader_step.get("daily_weather_rows_with_both_quote_and_fair_value")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_quote_orderability_counts": (
            prior_trader_step.get("daily_weather_quote_orderability_counts")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_quote_age_rows_with_timestamp": (
            prior_trader_step.get("daily_weather_quote_age_rows_with_timestamp")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_quote_stale_max_age_seconds": (
            prior_trader_step.get("daily_weather_quote_stale_max_age_seconds")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_rows_total": (
            prior_trader_step.get("daily_weather_shadow_taker_rows_total")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_rows_with_orderable_yes_ask": (
            prior_trader_step.get("daily_weather_shadow_taker_rows_with_orderable_yes_ask")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_rows_with_orderable_no_ask": (
            prior_trader_step.get("daily_weather_shadow_taker_rows_with_orderable_no_ask")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_rows_with_any_orderable_ask": (
            prior_trader_step.get("daily_weather_shadow_taker_rows_with_any_orderable_ask")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_edge_above_min_count": (
            prior_trader_step.get("daily_weather_shadow_taker_edge_above_min_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_edge_net_fees_above_min_count": (
            prior_trader_step.get("daily_weather_shadow_taker_edge_net_fees_above_min_count")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_shadow_taker_endpoint_orderbook_rows": (
            prior_trader_step.get("daily_weather_shadow_taker_endpoint_orderbook_rows")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_best_shadow_taker_candidate": (
            prior_trader_step.get("daily_weather_best_shadow_taker_candidate")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_allowed_universe_rows_with_conservative_candidate": (
            prior_trader_step.get("daily_weather_allowed_universe_rows_with_conservative_candidate")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_endpoint_orderbook_filtered": (
            prior_trader_step.get("daily_weather_endpoint_orderbook_filtered")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_conservative_candidate_failure_counts": (
            prior_trader_step.get("daily_weather_conservative_candidate_failure_counts")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "daily_weather_planned_orders": (
            prior_trader_step.get("daily_weather_planned_orders")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "top_market_ticker": decision_identity.get("top_market_ticker"),
        "top_market_contract_family": decision_identity.get("top_market_contract_family"),
        "fair_yes_probability_raw": decision_identity.get("fair_yes_probability_raw"),
        "execution_probability_guarded": decision_identity.get("execution_probability_guarded"),
        "fill_probability_source": decision_identity.get("fill_probability_source"),
        "empirical_fill_weight": decision_identity.get("empirical_fill_weight"),
        "heuristic_fill_weight": decision_identity.get("heuristic_fill_weight"),
        "probe_lane_used": decision_identity.get("probe_lane_used"),
        "probe_reason": decision_identity.get("probe_reason"),
        "rain_model_tag": (
            prior_trader_step.get("rain_model_tag")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "temperature_model_tag": (
            prior_trader_step.get("temperature_model_tag")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "fill_model_mode": (
            prior_trader_step.get("fill_model_mode")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "weather_prior_refresh_status": (
            weather_prior_step.get("status")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_refresh_reason": (
            weather_prior_step.get("reason")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_refresh_forced_reason": (
            weather_prior_step.get("forced_refresh_reason")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_mrms_snapshot_status": (
            weather_prior_step.get("mrms_snapshot_status")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_mrms_snapshot_age_seconds": (
            weather_prior_step.get("mrms_snapshot_age_seconds")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_nbm_snapshot_status": (
            weather_prior_step.get("nbm_snapshot_status")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_nbm_snapshot_cycle_age_seconds": (
            weather_prior_step.get("nbm_snapshot_cycle_age_seconds")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_prior_station_normals_cache_entries": (
            weather_prior_step.get("station_normals_cache_entries")
            if isinstance(weather_prior_step, dict)
            else None
        ),
        "weather_include_nws_gridpoint_data": _is_enabled(
            os.environ.get("BETBOT_WEATHER_INCLUDE_NWS_GRIDPOINT_DATA"),
            default=True,
        ),
        "weather_include_nws_observations": _is_enabled(
            os.environ.get("BETBOT_WEATHER_INCLUDE_NWS_OBSERVATIONS"),
            default=True,
        ),
        "weather_include_nws_alerts": _is_enabled(
            os.environ.get("BETBOT_WEATHER_INCLUDE_NWS_ALERTS"),
            default=True,
        ),
        "weather_include_ncei_normals": _is_enabled(
            os.environ.get("BETBOT_WEATHER_INCLUDE_NCEI_NORMALS"),
            default=True,
        ),
        "weather_include_mrms_qpe": _is_enabled(
            os.environ.get("BETBOT_WEATHER_INCLUDE_MRMS_QPE"),
            default=True,
        ),
        "weather_include_nbm_snapshot": _is_enabled(
            os.environ.get("BETBOT_WEATHER_INCLUDE_NBM_SNAPSHOT"),
            default=True,
        ),
        "daily_weather_stale_recovery_enabled": stale_recovery_enabled,
        "daily_weather_stale_recovery_max_retries": stale_recovery_max_retries,
        "daily_weather_stale_recovery_sleep_seconds": stale_recovery_sleep_seconds,
        "daily_weather_ticker_refresh_enabled": daily_weather_ticker_refresh_enabled,
        "daily_weather_ticker_refresh_on_base_capture": daily_weather_ticker_refresh_on_base_capture,
        "daily_weather_ticker_refresh_max_markets": daily_weather_ticker_refresh_max_markets,
        "daily_weather_ticker_refresh_watch_max_markets": daily_weather_ticker_refresh_watch_max_markets,
        "daily_weather_ticker_refresh_watch_interval_runs": daily_weather_ticker_refresh_watch_interval_runs,
        "daily_weather_ticker_refresh_state_file": daily_weather_ticker_refresh_state_file,
        "daily_weather_wakeup_burst_enabled": daily_weather_wakeup_burst_enabled,
        "daily_weather_wakeup_burst_max_markets": daily_weather_wakeup_burst_max_markets,
        "daily_weather_wakeup_burst_sleep_seconds": daily_weather_wakeup_burst_sleep_seconds,
        "daily_weather_wakeup_reprioritize_enabled": daily_weather_wakeup_reprioritize_enabled,
        "daily_weather_micro_watch_enabled": daily_weather_micro_watch_enabled,
        "daily_weather_micro_watch_interval_seconds": daily_weather_micro_watch_interval_seconds,
        "daily_weather_micro_watch_max_polls": daily_weather_micro_watch_max_polls,
        "daily_weather_micro_watch_active_hours_to_close": daily_weather_micro_watch_active_hours_to_close,
        "daily_weather_micro_watch_max_markets": daily_weather_micro_watch_max_markets,
        "daily_weather_micro_watch_include_unknown_hours_to_close": (
            daily_weather_micro_watch_include_unknown_hours_to_close
        ),
        "daily_weather_watch_wakeup_count": len(wakeup_transition_tickers),
        "daily_weather_watch_wakeup_tickers": wakeup_transition_tickers[:25],
        "daily_weather_watch_wakeup_to_candidate_count": wakeup_to_candidate_count,
        "daily_weather_watch_wakeup_to_planned_order_count": wakeup_to_planned_order_count,
        "daily_weather_micro_watch_status": (
            micro_watch_step.get("status")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_reason": (
            micro_watch_step.get("reason")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_watch_total": (
            micro_watch_step.get("watch_total")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_active_watch_tickers_count": (
            micro_watch_step.get("active_watch_tickers_count")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_selected_watch_tickers_count": (
            micro_watch_step.get("selected_watch_tickers_count")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_selected_watch_tickers": (
            micro_watch_step.get("selected_watch_tickers")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_polls_planned": (
            micro_watch_step.get("polls_planned")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_polls_completed": (
            micro_watch_step.get("polls_completed")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_wakeup_transition_count": (
            micro_watch_step.get("wakeup_transition_count")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_micro_watch_wakeup_transition_tickers": (
            micro_watch_step.get("wakeup_transition_tickers")
            if isinstance(micro_watch_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_status": (
            daily_weather_ticker_refresh_base_step.get("status")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_rows_appended": (
            daily_weather_ticker_refresh_base_step.get("rows_appended")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_refresh_mode": (
            daily_weather_ticker_refresh_base_step.get("refresh_mode")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_selection_mode": (
            daily_weather_ticker_refresh_base_step.get("watch_selection_mode")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_priority_candidates": (
            daily_weather_ticker_refresh_base_step.get("watch_priority_candidates")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_tickers_attempted_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_attempted_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_tickers_succeeded_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_succeeded_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_tradable_tickers_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_tradable_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_tickers_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_watch_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_selected_count": (
            daily_weather_ticker_refresh_base_step.get("market_tickers_watch_selected_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_due": (
            daily_weather_ticker_refresh_base_step.get("ticker_refresh_watch_due")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_watch_reason": (
            daily_weather_ticker_refresh_base_step.get("ticker_refresh_watch_reason")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_endpoint_to_orderable_transition_count": (
            daily_weather_ticker_refresh_base_step.get("endpoint_to_orderable_transition_count")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_base_endpoint_to_orderable_transition_tickers": (
            daily_weather_ticker_refresh_base_step.get("endpoint_to_orderable_transition_tickers")
            if isinstance(daily_weather_ticker_refresh_base_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_status": (
            wakeup_burst_step.get("status")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_rows_appended": (
            wakeup_burst_step.get("rows_appended")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_tickers_attempted_count": (
            wakeup_burst_step.get("market_tickers_attempted_count")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_tickers_attempted": (
            wakeup_burst_step.get("market_tickers_attempted")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_endpoint_to_orderable_transition_count": (
            wakeup_burst_step.get("endpoint_to_orderable_transition_count")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_wakeup_burst_endpoint_to_orderable_transition_tickers": (
            wakeup_burst_step.get("endpoint_to_orderable_transition_tickers")
            if isinstance(wakeup_burst_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_status": (
            wakeup_plan_step.get("status")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_output_file": (
            wakeup_plan_step.get("output_file")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_planned_orders": (
            wakeup_plan_step.get("planned_orders")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_top_plans_count": (
            wakeup_plan_step.get("top_plans_count")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "prior_plan_wakeup_reprioritize_top_plans_tickers": (
            wakeup_plan_step.get("top_plans_tickers")
            if isinstance(wakeup_plan_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_status": (
            daily_weather_ticker_refresh_step.get("status")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_rows_appended": (
            daily_weather_ticker_refresh_step.get("rows_appended")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_refresh_mode": (
            daily_weather_ticker_refresh_step.get("refresh_mode")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_selection_mode": (
            daily_weather_ticker_refresh_step.get("watch_selection_mode")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_priority_candidates": (
            daily_weather_ticker_refresh_step.get("watch_priority_candidates")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_tickers_attempted_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_attempted_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_tickers_succeeded_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_succeeded_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_tradable_tickers_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_tradable_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_tickers_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_watch_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_selected_count": (
            daily_weather_ticker_refresh_step.get("market_tickers_watch_selected_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_due": (
            daily_weather_ticker_refresh_step.get("ticker_refresh_watch_due")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_watch_reason": (
            daily_weather_ticker_refresh_step.get("ticker_refresh_watch_reason")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_endpoint_to_orderable_transition_count": (
            daily_weather_ticker_refresh_step.get("endpoint_to_orderable_transition_count")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_ticker_refresh_endpoint_to_orderable_transition_tickers": (
            daily_weather_ticker_refresh_step.get("endpoint_to_orderable_transition_tickers")
            if isinstance(daily_weather_ticker_refresh_step, dict)
            else None
        ),
        "daily_weather_stale_recovery_triggered": stale_recovery_triggered,
        "daily_weather_stale_recovery_attempts": stale_recovery_attempts,
        "daily_weather_stale_recovery_resolved": stale_recovery_resolved,
        "daily_weather_stale_recovery_failure_kind": daily_weather_recovery_failure_kind,
        "daily_weather_recovery_alert_window_hours": daily_weather_recovery_alert_state.get("window_hours"),
        "daily_weather_recovery_alert_threshold": daily_weather_recovery_alert_state.get("threshold"),
        "daily_weather_recovery_alert_max_events": daily_weather_recovery_alert_state.get("max_events"),
        "daily_weather_recovery_failure_count_window": daily_weather_recovery_alert_state.get("failure_count_window"),
        "daily_weather_recovery_failure_counts_by_kind": daily_weather_recovery_alert_state.get("failure_counts_by_kind"),
        "daily_weather_recovery_alert_triggered": daily_weather_recovery_alert_state.get("alert_triggered"),
        "daily_weather_recovery_health_file": daily_weather_recovery_alert_state.get("state_file"),
        "daily_weather_recovery_alert_parse_error": daily_weather_recovery_alert_state.get("parse_error"),
        "daily_weather_recovery_alert_write_error": daily_weather_recovery_alert_state.get("write_error"),
        "capture_max_hours_to_close": capture_max_hours_to_close,
        "capture_page_limit": capture_page_limit,
        "capture_max_pages": capture_max_pages,
        "daily_weather_recovery_capture_max_hours_to_close": stale_recovery_capture_max_hours_to_close,
        "daily_weather_recovery_capture_page_limit": stale_recovery_capture_page_limit,
        "daily_weather_recovery_capture_max_pages": stale_recovery_capture_max_pages,
        "weather_prior_allowed_contract_families": list(allowed_weather_contract_families),
        "weather_prior_allowed_family_counts": weather_prior_state_after.get("allowed_family_counts"),
        "weather_prior_allowed_rows_total": weather_prior_state_after.get("allowed_rows_total"),
        "weather_prior_state_reason": weather_prior_state_after.get("reason"),
        "weather_prior_stale": bool(weather_prior_state_after.get("stale")),
        "weather_prior_age_seconds": weather_prior_state_after.get("priors_age_seconds"),
        "weather_history_rows_total": weather_history_state.get("weather_history_rows_total"),
        "weather_history_live_ready_rows": weather_history_state.get("weather_history_live_ready_rows"),
        "weather_history_unhealthy_rows": weather_history_state.get("weather_history_unhealthy_rows"),
        "weather_history_status_counts": weather_history_state.get("weather_history_status_counts"),
        "weather_history_live_ready_reason_counts": weather_history_state.get(
            "weather_history_live_ready_reason_counts"
        ),
        "weather_history_missing_token_count": weather_history_state.get("weather_history_missing_token_count"),
        "weather_history_station_mapping_missing_count": weather_history_state.get(
            "weather_history_station_mapping_missing_count"
        ),
        "weather_history_sample_depth_block_count": weather_history_state.get(
            "weather_history_sample_depth_block_count"
        ),
        "weather_history_parse_error": weather_history_state.get("weather_history_parse_error"),
        "prefer_empirical_fill_model": (
            prior_trader_step.get("execution_empirical_fill_model_prefer_empirical")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "weather_priors_version": (
            prior_trader_step.get("weather_priors_version")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "frontier_artifact_path": runtime_version.get("frontier_artifact_path"),
        "frontier_artifact_sha256": runtime_version.get("frontier_artifact_sha256"),
        "frontier_artifact_file_sha256": runtime_version.get("frontier_artifact_file_sha256"),
        "frontier_artifact_payload_sha256": runtime_version.get("frontier_artifact_payload_sha256"),
        "frontier_artifact_as_of_utc": runtime_version.get("frontier_artifact_as_of_utc"),
        "frontier_artifact_age_seconds": runtime_version.get("frontier_artifact_age_seconds"),
        "frontier_selection_mode": runtime_version.get("frontier_selection_mode"),
        "frontier_trusted_bucket_count": runtime_version.get("frontier_trusted_bucket_count"),
        "frontier_untrusted_bucket_count": runtime_version.get("frontier_untrusted_bucket_count"),
        "history_csv_path": str(history_csv),
        "history_csv_mtime_utc": _file_meta(history_csv).get("mtime_utc"),
        "daily_weather_board_age_seconds": (
            prior_trader_step.get("daily_weather_board_age_seconds")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "weather_station_history_cache_age_seconds": (
            prior_trader_step.get("weather_station_history_cache_age_seconds")
            if isinstance(prior_trader_step, dict)
            else None
        ),
        "balance_heartbeat_age_seconds": (
            top_level_balance.get("cache_age_seconds")
            if isinstance(top_level_balance, dict)
            else (
                prior_trader_step.get("balance_heartbeat_age_seconds")
                if isinstance(prior_trader_step, dict)
                else None
            )
        ),
        "freshness": {
            "history_csv": _file_meta(history_csv),
            "priors_csv": _file_meta(priors_csv),
            "ws_state_latest_json": _file_meta(output_dir / "kalshi_ws_state_latest.json"),
            "execution_journal_db": _file_meta(output_dir / "kalshi_execution_journal.sqlite3"),
            "latest_prior_trader_summary": _file_meta(Path(latest_prior_summary_file))
            if latest_prior_summary_file
            else {
                "path": None,
                "exists": False,
                "mtime_utc": None,
                "age_seconds": None,
                "size_bytes": None,
                },
        },
        "runtime_version": runtime_version,
    }

    _write_report(run_report_path=run_report_path, latest_report_path=latest_report_path, report=report)

    print(
        json.dumps(
            {
                "status": overall_status,
                "pipeline_ready": pipeline_ready,
                "live_ready": live_ready,
                "live_blockers": live_blockers,
                "run_report": str(run_report_path),
                "latest_report": str(latest_report_path),
                "failed_steps": failed_steps,
                "degraded_reasons": degraded_reasons,
            },
            indent=2,
        )
    )

    return 0 if overall_status == "ok" else 1


raise SystemExit(main())
PY
