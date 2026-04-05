#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BETBOT_OUTPUT_DIR="${BETBOT_OUTPUT_DIR:-$REPO_ROOT/outputs}"
BETBOT_ENV_FILE="${BETBOT_ENV_FILE:-$REPO_ROOT/data/research/account_onboarding.local.env}"
BETBOT_PRIORS_CSV="${BETBOT_PRIORS_CSV:-$REPO_ROOT/data/research/kalshi_nonsports_priors.csv}"
BETBOT_HISTORY_CSV="${BETBOT_HISTORY_CSV:-$BETBOT_OUTPUT_DIR/kalshi_nonsports_history.csv}"
BETBOT_WATCH_HISTORY_CSV="${BETBOT_WATCH_HISTORY_CSV:-$BETBOT_OUTPUT_DIR/kalshi_micro_watch_history.csv}"
MONTHLY_ATTEMPT_MODE="${MONTHLY_ATTEMPT_MODE:-pre_submit_smoke}"

MONTHLY_FAMILY="${MONTHLY_FAMILY:-monthly_climate_anomaly}"
MONTHLY_RUN_SECONDS="${MONTHLY_RUN_SECONDS:-20}"
MONTHLY_MAX_MARKETS="${MONTHLY_MAX_MARKETS:-40}"
MONTHLY_MIN_THEORETICAL_EDGE_NET_FEES="${MONTHLY_MIN_THEORETICAL_EDGE_NET_FEES:-0.005}"
MONTHLY_ROUTER_DAILY_RISK_CAP_DOLLARS="${MONTHLY_ROUTER_DAILY_RISK_CAP_DOLLARS:-3}"
MONTHLY_ROUTER_MAX_RISK_PER_BET_DOLLARS="${MONTHLY_ROUTER_MAX_RISK_PER_BET_DOLLARS:-1}"

PILOT_MAX_ORDERS="${PILOT_MAX_ORDERS:-1}"
PILOT_CONTRACTS_PER_ORDER="${PILOT_CONTRACTS_PER_ORDER:-1}"
PILOT_MAX_ORDERS_PER_RUN="${PILOT_MAX_ORDERS_PER_RUN:-1}"
PILOT_CONTRACTS_CAP="${PILOT_CONTRACTS_CAP:-1}"
PILOT_REQUIRED_EV_DOLLARS="${PILOT_REQUIRED_EV_DOLLARS:-0.05}"
PILOT_MAX_LIVE_SUBMISSIONS_PER_DAY="${PILOT_MAX_LIVE_SUBMISSIONS_PER_DAY:-3}"
PILOT_MAX_LIVE_COST_PER_DAY="${PILOT_MAX_LIVE_COST_PER_DAY:-3}"
PILOT_RESTING_HOLD_SECONDS="${PILOT_RESTING_HOLD_SECONDS:-0}"

mkdir -p "$BETBOT_OUTPUT_DIR"
if [[ ! -f "$BETBOT_ENV_FILE" ]]; then
  echo "Missing env file: $BETBOT_ENV_FILE" >&2
  exit 1
fi

if [[ "$MONTHLY_ATTEMPT_MODE" != "live" && "$MONTHLY_ATTEMPT_MODE" != "pre_submit_smoke" ]]; then
  echo "Invalid MONTHLY_ATTEMPT_MODE=$MONTHLY_ATTEMPT_MODE (expected: live|pre_submit_smoke)" >&2
  exit 1
fi

STAMP="$(date -u +%Y%m%d_%H%M%S)"
RUN_DIR="$BETBOT_OUTPUT_DIR/monthly_climate_live_attempt/$STAMP"
mkdir -p "$RUN_DIR"

ROUTER_STDOUT_JSON="$RUN_DIR/step1_climate_router_stdout.json"
PRIOR_TRADER_STDOUT_JSON="$RUN_DIR/step2_prior_trader_stdout.json"
EVIDENCE_STDOUT_JSON="$RUN_DIR/step3_pilot_execution_evidence_stdout.json"

echo "[monthly-live-attempt] Step 1/3: refreshing climate router (family=$MONTHLY_FAMILY)"
python3 -m betbot.cli kalshi-climate-realtime-router \
  --env-file "$BETBOT_ENV_FILE" \
  --priors-csv "$BETBOT_PRIORS_CSV" \
  --history-csv "$BETBOT_HISTORY_CSV" \
  --output-dir "$BETBOT_OUTPUT_DIR" \
  --include-contract-families "$MONTHLY_FAMILY" \
  --run-seconds "$MONTHLY_RUN_SECONDS" \
  --max-markets "$MONTHLY_MAX_MARKETS" \
  --min-theoretical-edge-net-fees "$MONTHLY_MIN_THEORETICAL_EDGE_NET_FEES" \
  --daily-risk-cap "$MONTHLY_ROUTER_DAILY_RISK_CAP_DOLLARS" \
  --max-risk-per-bet "$MONTHLY_ROUTER_MAX_RISK_PER_BET_DOLLARS" \
  >"$ROUTER_STDOUT_JSON"

ROUTER_SUMMARY_JSON="$(
  python3 - "$ROUTER_STDOUT_JSON" <<'PY'
from __future__ import annotations
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
output_file = str(payload.get("output_file") or "").strip()
if not output_file:
    raise SystemExit("Router output_file missing.")
print(output_file)
PY
)"

echo "[monthly-live-attempt] Step 2/3: running prior-trader mode=$MONTHLY_ATTEMPT_MODE (1x1 monthly-only pilot lane)"
if [[ "$MONTHLY_ATTEMPT_MODE" == "pre_submit_smoke" ]]; then
  export BETBOT_PRE_SUBMIT_SMOKE_MODE=1
else
  unset BETBOT_PRE_SUBMIT_SMOKE_MODE >/dev/null 2>&1 || true
fi
python3 -m betbot.cli kalshi-micro-prior-trader \
  --env-file "$BETBOT_ENV_FILE" \
  --priors-csv "$BETBOT_PRIORS_CSV" \
  --history-csv "$BETBOT_HISTORY_CSV" \
  --watch-history-csv "$BETBOT_WATCH_HISTORY_CSV" \
  --output-dir "$BETBOT_OUTPUT_DIR" \
  --allow-live-orders \
  --use-temp-live-env \
  --max-orders "$PILOT_MAX_ORDERS" \
  --contracts-per-order "$PILOT_CONTRACTS_PER_ORDER" \
  --max-live-submissions-per-day "$PILOT_MAX_LIVE_SUBMISSIONS_PER_DAY" \
  --max-live-cost-per-day "$PILOT_MAX_LIVE_COST_PER_DAY" \
  --resting-hold-seconds "$PILOT_RESTING_HOLD_SECONDS" \
  --climate-router-pilot-enabled \
  --climate-router-summary-json "$ROUTER_SUMMARY_JSON" \
  --climate-router-pilot-max-orders-per-run "$PILOT_MAX_ORDERS_PER_RUN" \
  --climate-router-pilot-contracts-cap "$PILOT_CONTRACTS_CAP" \
  --climate-router-pilot-required-ev-dollars "$PILOT_REQUIRED_EV_DOLLARS" \
  --climate-router-pilot-allowed-families "$MONTHLY_FAMILY" \
  --climate-router-pilot-policy-scope-override-enabled \
  >"$PRIOR_TRADER_STDOUT_JSON"
unset BETBOT_PRE_SUBMIT_SMOKE_MODE >/dev/null 2>&1 || true

PRIOR_TRADER_SUMMARY_JSON="$(
  python3 - "$PRIOR_TRADER_STDOUT_JSON" <<'PY'
from __future__ import annotations
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
payload = json.loads(path.read_text(encoding="utf-8"))
output_file = str(payload.get("output_file") or "").strip()
if not output_file:
    raise SystemExit("Prior-trader output_file missing.")
print(output_file)
PY
)"

echo "[monthly-live-attempt] Step 3/3: generating pilot execution evidence snapshot"
EVIDENCE_JSON="$BETBOT_OUTPUT_DIR/pilot_execution_evidence_latest.json"
python3 "$SCRIPT_DIR/pilot_execution_evidence.py" \
  --outputs-dir "$BETBOT_OUTPUT_DIR" \
  --output-json "$EVIDENCE_JSON" \
  >"$EVIDENCE_STDOUT_JSON"

HARNESS_SUMMARY_JSON="$BETBOT_OUTPUT_DIR/monthly_climate_live_attempt_summary_${STAMP}.json"
HARNESS_SUMMARY_LATEST_JSON="$BETBOT_OUTPUT_DIR/monthly_climate_live_attempt_summary_latest.json"

python3 - "$MONTHLY_ATTEMPT_MODE" "$ROUTER_SUMMARY_JSON" "$PRIOR_TRADER_SUMMARY_JSON" "$EVIDENCE_JSON" "$HARNESS_SUMMARY_JSON" "$HARNESS_SUMMARY_LATEST_JSON" <<'PY'
from __future__ import annotations
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


attempt_mode = str(sys.argv[1]).strip().lower() or "pre_submit_smoke"
router_path = Path(sys.argv[2])
prior_path = Path(sys.argv[3])
evidence_path = Path(sys.argv[4])
summary_path = Path(sys.argv[5])
latest_path = Path(sys.argv[6])

router = _load(router_path)
prior = _load(prior_path)
evidence = _load(evidence_path)
pilot_funnel = evidence.get("pilot_funnel")
if not isinstance(pilot_funnel, dict):
    pilot_funnel = {}
core_state = evidence.get("core_state")
if not isinstance(core_state, dict):
    core_state = {}
pass_fail = evidence.get("pass_fail")
if not isinstance(pass_fail, dict):
    pass_fail = {}

summary = {
    "generated_at_utc": datetime.now(UTC).isoformat(),
    "status": "ready",
    "attempt_mode": attempt_mode,
    "family_scope": "monthly_climate_anomaly",
    "source_files": {
        "router_summary_file": str(router_path),
        "prior_trader_summary_file": str(prior_path),
        "pilot_execution_evidence_file": str(evidence_path),
        "prior_execute_summary_file": prior.get("prior_execute_summary_file"),
        "micro_execute_summary_file": prior.get("prior_execute_summary_file"),
        "reconcile_summary_file": prior.get("reconcile_summary_file"),
    },
    # Flat compatibility fields used for operator copy/paste checks.
    "allow_live_orders_requested": prior.get("allow_live_orders_requested"),
    "allow_live_orders_effective": prior.get("allow_live_orders_effective"),
    "sizing_basis": prior.get("sizing_basis"),
    "execution_basis": prior.get("execution_basis"),
    "prior_trade_gate_status": prior.get("prior_trade_gate_status"),
    "prior_trade_gate_blockers": prior.get("prior_trade_gate_blockers"),
    "actual_live_balance_dollars": prior.get("actual_live_balance_dollars"),
    "climate_router_pilot_considered_rows": prior.get("climate_router_pilot_considered_rows"),
    "climate_router_pilot_promoted_rows": prior.get("climate_router_pilot_promoted_rows"),
    "climate_router_pilot_execute_considered_rows": prior.get("climate_router_pilot_execute_considered_rows"),
    "climate_router_pilot_attempted_orders": prior.get("climate_router_pilot_attempted_orders"),
    "climate_router_pilot_filled_orders": prior.get("climate_router_pilot_filled_orders"),
    "climate_router_pilot_blocked_post_promotion_reason_counts": prior.get(
        "climate_router_pilot_blocked_post_promotion_reason_counts"
    ),
    "climate_router_pilot_policy_scope_override_status": prior.get(
        "climate_router_pilot_policy_scope_override_status"
    ),
    "climate_router_pilot_selected_tickers": prior.get("climate_router_pilot_selected_tickers"),
    "climate_router_pilot_expected_value_dollars": prior.get("climate_router_pilot_expected_value_dollars"),
    "climate_router_pilot_total_risk_dollars": prior.get("climate_router_pilot_total_risk_dollars"),
    "pilot_execution_evidence_status": evidence.get("pilot_execution_evidence_status"),
    "pilot_execution_selected_ticker": evidence.get("pilot_execution_selected_ticker"),
    "pilot_execution_selected_family": evidence.get("pilot_execution_selected_family"),
    "pilot_execution_would_attempt_live_if_enabled": evidence.get(
        "pilot_execution_would_attempt_live_if_enabled"
    ),
    "pilot_execution_attempted_orders": evidence.get("pilot_execution_attempted_orders"),
    "pilot_execution_filled_orders": evidence.get("pilot_execution_filled_orders"),
    "pilot_execution_frontier_status": evidence.get("pilot_execution_frontier_status"),
    "pilot_execution_recommended_next_action": evidence.get("pilot_execution_recommended_next_action"),
    "pilot_execution_blocked_reason_counts": evidence.get("pilot_execution_blocked_reason_counts"),
    "router": {
        "status": router.get("status"),
        "ws_collect_status": router.get("ws_collect_status"),
        "climate_opportunity_class_counts": router.get("climate_opportunity_class_counts"),
        "climate_availability_state_counts": router.get("climate_availability_state_counts"),
        "climate_tradable_positive_rows": router.get("climate_tradable_positive_rows"),
        "top_tradable_candidate_ticker": (
            (router.get("top_tradable_candidates") or [{}])[0].get("market_ticker")
            if isinstance(router.get("top_tradable_candidates"), list) and router.get("top_tradable_candidates")
            else None
        ),
    },
    "prior_trader": {
        "status": prior.get("status"),
        "allow_live_orders_requested": prior.get("allow_live_orders_requested"),
        "allow_live_orders_effective": prior.get("allow_live_orders_effective"),
        "prior_trade_gate_pass": prior.get("prior_trade_gate_pass"),
        "prior_trade_gate_status": prior.get("prior_trade_gate_status"),
        "prior_execute_status": prior.get("prior_execute_status"),
        "reconcile_status": prior.get("reconcile_status"),
        "climate_router_pilot_selected_tickers": prior.get("climate_router_pilot_selected_tickers"),
        "climate_router_pilot_live_eligible_rows": prior.get("climate_router_pilot_live_eligible_rows"),
        "climate_router_pilot_would_attempt_live_if_enabled": prior.get(
            "climate_router_pilot_would_attempt_live_if_enabled"
        ),
        "climate_router_pilot_attempted_orders": prior.get("climate_router_pilot_attempted_orders"),
        "climate_router_pilot_filled_orders": prior.get("climate_router_pilot_filled_orders"),
        "climate_router_pilot_blocked_post_promotion_reason_counts": prior.get(
            "climate_router_pilot_blocked_post_promotion_reason_counts"
        ),
        "climate_router_pilot_policy_scope_override_status": prior.get(
            "climate_router_pilot_policy_scope_override_status"
        ),
    },
    "pilot_execution_evidence": {
        "mode": core_state.get("mode"),
        "live_ready": core_state.get("live_ready"),
        "live_blockers": core_state.get("live_blockers"),
        "frontier_status": core_state.get("frontier_status"),
        "status": evidence.get("first_attempt_evidence", {}).get("status")
        if isinstance(evidence.get("first_attempt_evidence"), dict)
        else None,
        "selected_ticker": pilot_funnel.get("selected_ticker"),
        "selected_family": prior.get("pilot_execution_selected_family"),
        "would_attempt_live_if_enabled": pilot_funnel.get("would_attempt_live_if_enabled"),
        "attempted_orders": pilot_funnel.get("attempted_orders"),
        "filled_orders": pilot_funnel.get("filled_orders"),
        "dominant_dry_run_blocker_reason": pilot_funnel.get("dominant_dry_run_blocker_reason"),
        "recommended_next_action": evidence.get("recommended_next_action"),
    },
    "pass_fail": {
        "has_selected_ticker": bool(pass_fail.get("has_selected_ticker")),
        "has_attempt": bool(pass_fail.get("has_attempt")),
        "has_fill": bool(pass_fail.get("has_fill")),
        "frontier_not_insufficient_data": bool(pass_fail.get("frontier_not_insufficient_data")),
    },
}

summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
latest_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
print(json.dumps(summary, indent=2))
PY

echo "[monthly-live-attempt] Completed."
echo "[monthly-live-attempt] Harness summary: $HARNESS_SUMMARY_JSON"
echo "[monthly-live-attempt] Latest summary:  $HARNESS_SUMMARY_LATEST_JSON"
