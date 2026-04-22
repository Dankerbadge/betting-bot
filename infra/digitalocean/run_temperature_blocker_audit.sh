#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing runtime env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" ]]; then
  echo "runtime env file is not readable (check owner/group/perms): $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

: "${BETBOT_ROOT:?BETBOT_ROOT is required}"
: "${OUTPUT_DIR:?OUTPUT_DIR is required}"

PYTHON_BIN="$BETBOT_ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "missing python venv executable: $PYTHON_BIN" >&2
  exit 1
fi

WINDOW_SUMMARIZE_SCRIPT="${WINDOW_SUMMARIZE_SCRIPT:-$BETBOT_ROOT/infra/digitalocean/summarize_window.py}"
if [[ ! -f "$WINDOW_SUMMARIZE_SCRIPT" ]]; then
  echo "missing summarize script: $WINDOW_SUMMARIZE_SCRIPT" >&2
  exit 1
fi

BLOCKER_AUDIT_HOURS="${BLOCKER_AUDIT_HOURS:-168}"
BLOCKER_AUDIT_TOP_N="${BLOCKER_AUDIT_TOP_N:-20}"
BLOCKER_AUDIT_DISCORD_MODE="${BLOCKER_AUDIT_DISCORD_MODE:-concise}"
BLOCKER_AUDIT_SEND_WEBHOOK="${BLOCKER_AUDIT_SEND_WEBHOOK:-1}"
BLOCKER_AUDIT_WEBHOOK_URL="${BLOCKER_AUDIT_WEBHOOK_URL:-${ALPHA_SUMMARY_WEBHOOK_URL:-${ALERT_WEBHOOK_URL:-}}}"
BLOCKER_AUDIT_WEBHOOK_THREAD_ID="${BLOCKER_AUDIT_WEBHOOK_THREAD_ID:-${ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID:-${ALPHA_SUMMARY_WEBHOOK_THREAD_ID:-${ALERT_WEBHOOK_THREAD_ID:-}}}}"
BLOCKER_AUDIT_WEBHOOK_TIMEOUT_SECONDS="${BLOCKER_AUDIT_WEBHOOK_TIMEOUT_SECONDS:-5}"
BLOCKER_AUDIT_WEBHOOK_USERNAME="${BLOCKER_AUDIT_WEBHOOK_USERNAME:-BetBot Ops}"
BLOCKER_AUDIT_STRICT_FAIL_ON_WINDOW_SUMMARY_PARSE_ERROR="${BLOCKER_AUDIT_STRICT_FAIL_ON_WINDOW_SUMMARY_PARSE_ERROR:-0}"
export BLOCKER_AUDIT_DISCORD_MODE BLOCKER_AUDIT_STRICT_FAIL_ON_WINDOW_SUMMARY_PARSE_ERROR

build_discord_target_url() {
  local base_url="${1:-}"
  local thread_id="${2:-}"
  if [[ -z "$base_url" || -z "$thread_id" ]]; then
    echo "$base_url"
    return
  fi
  if [[ "$base_url" == *"thread_id="* ]]; then
    echo "$base_url"
    return
  fi
  if [[ "$base_url" == *\?* ]]; then
    echo "${base_url}&thread_id=${thread_id}"
  else
    echo "${base_url}?thread_id=${thread_id}"
  fi
}

BLOCKER_AUDIT_WEBHOOK_TARGET_URL="$(build_discord_target_url "$BLOCKER_AUDIT_WEBHOOK_URL" "$BLOCKER_AUDIT_WEBHOOK_THREAD_ID")"

mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" "$OUTPUT_DIR/checkpoints"
LOG_FILE="$OUTPUT_DIR/logs/blocker_audit.log"
CHECKPOINTS_DIR="$OUTPUT_DIR/checkpoints"
LOCK_FILE="$OUTPUT_DIR/.blocker_audit.lock"

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] blocker audit skipped: lock busy" >> "$LOG_FILE"
  exit 0
fi

cd "$BETBOT_ROOT"

window_end_epoch="$(date +%s)"
window_meta="$("$PYTHON_BIN" - "$BLOCKER_AUDIT_HOURS" "$window_end_epoch" <<'PY'
from __future__ import annotations

import sys

hours = max(0.0, float(sys.argv[1]))
end_epoch = int(float(sys.argv[2]))
start_epoch = max(0, int(end_epoch - (hours * 3600.0)))
if abs(hours - round(hours)) < 1e-9:
    label = f"{int(round(hours))}h"
else:
    label = f"{hours:g}h"
print(f"{start_epoch}|{label}|{hours:g}")
PY
)"
IFS='|' read -r window_start_epoch window_label window_hours_safe <<<"$window_meta"
window_ts="$(date -u +"%Y%m%d_%H%M%S")"

window_summary_file="$CHECKPOINTS_DIR/blocker_audit_window_${window_label}_${window_ts}.json"
audit_file="$CHECKPOINTS_DIR/blocker_audit_${window_label}_${window_ts}.json"
audit_latest_file="$CHECKPOINTS_DIR/blocker_audit_${window_label}_latest.json"

echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") blocker audit cycle start (window=${window_label}) ===" >> "$LOG_FILE"

"$PYTHON_BIN" "$WINDOW_SUMMARIZE_SCRIPT" \
  --out-dir "$OUTPUT_DIR" \
  --start-epoch "$window_start_epoch" \
  --end-epoch "$window_end_epoch" \
  --label "blocker_audit_${window_label}" \
  --output "$window_summary_file" >> "$LOG_FILE" 2>&1

"$PYTHON_BIN" - "$window_summary_file" "$audit_file" "$audit_latest_file" "$BLOCKER_AUDIT_TOP_N" "$window_label" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import glob
import json
import os
from pathlib import Path
import sys
from typing import Any


def _normalize(value: Any) -> str:
    return str(value or "").strip()


def _parse_int(value: Any) -> int:
    try:
        return int(float(value))
    except Exception:
        return 0


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _humanize_reason(reason: str) -> str:
    key = _normalize(reason).lower()
    mapping = {
        "metar_observation_stale": "METAR observation stale",
        "metar_freshness_boundary_quality_insufficient": "METAR near-stale quality insufficient",
        "settlement_finalization_blocked": "Settlement finalization blocked",
        "inside_cutoff_window": "Inside cutoff window",
        "no_side_interval_overlap_still_possible": "Range still possible",
        "yes_side_not_impossible": "YES side still possible",
        "underlying_exposure_cap_reached": "Underlying exposure cap reached",
        "underlying_submission_cap_reached": "Underlying submission cap reached",
        "alpha_strength_below_min": "Alpha strength below min",
        "probability_confidence_below_min": "Probability confidence below min",
        "expected_edge_below_min": "Expected edge below min",
        "historical_quality_global_only_pressure": "Historical quality global-only pressure",
        "below_min_alpha_strength": "Below minimum alpha strength",
    }
    return mapping.get(key, key.replace("_", " "))


def _reason_action(reason: str) -> str:
    key = _normalize(reason).lower()
    action_map = {
        "metar_observation_stale": "Tighten station/hour freshness policy and allow grace only in validated volatile pockets.",
        "metar_freshness_boundary_quality_insufficient": "Tune near-stale quality thresholds by station/hour using settled reference outcomes.",
        "settlement_finalization_blocked": "Run settlement pressure mode faster and lower final-report cache TTL during backlog.",
        "inside_cutoff_window": "Shift scans earlier near close and shorten loop sleep in local close windows.",
        "no_side_interval_overlap_still_possible": "Expand family/hour breadth in overlap-heavy pockets before changing global thresholds.",
        "yes_side_not_impossible": "Strengthen path/monotonic checks so impossibility upgrades happen sooner.",
        "underlying_exposure_cap_reached": "Rebalance per-family and per-station caps before stacking repeated entries.",
        "underlying_submission_cap_reached": "Raise submission caps only after independent breadth rises.",
        "alpha_strength_below_min": "Calibrate alpha thresholds by station/hour so high-confidence pockets pass cleanly.",
        "probability_confidence_below_min": "Recalibrate probability confidence by station/hour from settled outcomes.",
        "expected_edge_below_min": "Re-tune edge model terms (friction, urgency, consensus) to realistic fill economics.",
        "historical_quality_global_only_pressure": "Reduce global-only adjustments: expand bucket-backed evidence and tighten weak-evidence approvals before throughput changes.",
        "below_min_alpha_strength": "Tune alpha thresholds by station/hour; avoid global over-suppression.",
    }
    return action_map.get(key, "Audit sample rows for this reason and close the largest data-quality or gating bottleneck before threshold changes.")


def _load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


window_summary_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
latest_path = Path(sys.argv[3])
top_n = max(1, int(float(sys.argv[4])))
window_label = _normalize(sys.argv[5]) or "168h"
discord_mode = _normalize(os.environ.get("BLOCKER_AUDIT_DISCORD_MODE")).lower() or "concise"
if discord_mode not in {"concise", "detailed"}:
    discord_mode = "concise"
strict_fail_on_window_summary_parse_error = _normalize(
    os.environ.get("BLOCKER_AUDIT_STRICT_FAIL_ON_WINDOW_SUMMARY_PARSE_ERROR")
).lower() in {"1", "true", "yes", "on"}

window_summary_parse_error = ""
window_summary_loaded = False
try:
    payload = json.loads(window_summary_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        window_summary_loaded = True
    else:
        payload = {}
        window_summary_parse_error = "root payload is not an object"
except Exception:
    payload = {}
    window_summary_parse_error = "invalid JSON"

if window_summary_parse_error and strict_fail_on_window_summary_parse_error:
    raise SystemExit(
        "STRICT CHECK FAILED: blocker audit window summary malformed "
        f"({window_summary_parse_error})"
    )

totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
reason_counts = payload.get("policy_reason_counts") if isinstance(payload.get("policy_reason_counts"), dict) else {}
intents_total = max(0, _parse_int(totals.get("intents_total")))
approved_count = max(0, _parse_int(reason_counts.get("approved")))

# Read current settlement backlog directly from latest settlement-state snapshot so
# rolling blocker counts do not get interpreted as active unresolved pressure.
current_settlement_blocked_underlyings = 0
current_settlement_pending_final_report = 0
out_dir_text = _normalize(payload.get("out_dir"))
if out_dir_text:
    out_dir = Path(out_dir_text)
else:
    out_dir = window_summary_path.parent.parent

settlement_candidates = glob.glob(str(out_dir / "kalshi_temperature_settlement_state_*.json"))
if settlement_candidates:
    settlement_candidates.sort(key=lambda p: Path(p).stat().st_mtime)
    latest_settlement_path = Path(settlement_candidates[-1])
    try:
        settlement_payload = json.loads(latest_settlement_path.read_text(encoding="utf-8"))
    except Exception:
        settlement_payload = {}
    if isinstance(settlement_payload, dict):
        current_settlement_blocked_underlyings = max(
            0, _parse_int(settlement_payload.get("blocked_underlyings"))
        )
        state_counts = (
            settlement_payload.get("state_counts")
            if isinstance(settlement_payload.get("state_counts"), dict)
            else {}
        )
        pending_from_state = max(0, _parse_int(state_counts.get("pending_final_report")))
        pending_from_top = max(0, _parse_int(settlement_payload.get("final_report_pending_count")))
        current_settlement_pending_final_report = max(pending_from_state, pending_from_top)

current_settlement_unresolved = (
    current_settlement_blocked_underlyings + current_settlement_pending_final_report
)

health_dir = out_dir / "health"
coldmath_hardening_path = health_dir / "coldmath_hardening_latest.json"
decision_matrix_path = health_dir / "decision_matrix_hardening_latest.json"
lane_alert_state_path = health_dir / ".decision_matrix_lane_alert_state.json"
coldmath_hardening_payload = _load_json_dict(coldmath_hardening_path)
decision_matrix_payload = _load_json_dict(decision_matrix_path)
lane_alert_state = _load_json_dict(lane_alert_state_path)

targeted_trading_support = (
    coldmath_hardening_payload.get("targeted_trading_support")
    if isinstance(coldmath_hardening_payload.get("targeted_trading_support"), dict)
    else {}
)
lane_checks = (
    targeted_trading_support.get("checks")
    if isinstance(targeted_trading_support.get("checks"), dict)
    else {}
)
lane_observed = (
    targeted_trading_support.get("observed")
    if isinstance(targeted_trading_support.get("observed"), dict)
    else {}
)
lane_thresholds = (
    targeted_trading_support.get("thresholds")
    if isinstance(targeted_trading_support.get("thresholds"), dict)
    else {}
)
decision_matrix_strict_signal = bool(lane_checks.get("decision_matrix_strict_signal") is True)
decision_matrix_bootstrap_signal_raw = bool(lane_checks.get("decision_matrix_bootstrap_signal_raw") is True)
decision_matrix_bootstrap_signal = bool(lane_checks.get("decision_matrix_bootstrap_signal") is True)
decision_matrix_signal = bool(lane_checks.get("decision_matrix_signal") is True)
decision_matrix_supports_consistency = bool(
    lane_observed.get("decision_matrix_supports_consistency_and_profitability") is True
)
decision_matrix_bootstrap_guard_status = _normalize(
    lane_observed.get("decision_matrix_bootstrap_guard_status")
).lower()
decision_matrix_bootstrap_guard_reasons_raw = (
    lane_observed.get("decision_matrix_bootstrap_guard_reasons")
    if isinstance(lane_observed.get("decision_matrix_bootstrap_guard_reasons"), list)
    else []
)
decision_matrix_bootstrap_guard_reasons = [
    _normalize(item).lower().replace("_", " ")
    for item in decision_matrix_bootstrap_guard_reasons_raw
    if _normalize(item)
]
decision_matrix_bootstrap_elapsed_hours = _parse_float(
    lane_observed.get("decision_matrix_bootstrap_guard_elapsed_hours")
)
decision_matrix_bootstrap_max_hours = _parse_float(
    lane_thresholds.get("matrix_bootstrap_max_hours")
)
decision_matrix_bootstrap_hours_to_expiry = None
if (
    isinstance(decision_matrix_bootstrap_elapsed_hours, float)
    and isinstance(decision_matrix_bootstrap_max_hours, float)
    and decision_matrix_bootstrap_max_hours > 0
):
    decision_matrix_bootstrap_hours_to_expiry = max(
        0.0,
        float(decision_matrix_bootstrap_max_hours - decision_matrix_bootstrap_elapsed_hours),
    )

decision_matrix_lane_status = "unknown"
if decision_matrix_strict_signal:
    decision_matrix_lane_status = "strict"
elif decision_matrix_bootstrap_signal:
    decision_matrix_lane_status = "bootstrap"
elif decision_matrix_bootstrap_signal_raw and not decision_matrix_bootstrap_signal:
    decision_matrix_lane_status = "bootstrap_blocked"
elif lane_checks:
    decision_matrix_lane_status = "matrix_failed"
elif isinstance(decision_matrix_payload, dict) and decision_matrix_payload:
    if decision_matrix_payload.get("supports_consistency_and_profitability") is True:
        decision_matrix_lane_status = "strict"
    elif decision_matrix_payload.get("supports_bootstrap_progression") is True:
        decision_matrix_lane_status = "bootstrap"
    else:
        decision_matrix_lane_status = "matrix_failed"

if decision_matrix_lane_status == "strict":
    decision_matrix_lane_line = "Decision matrix lane: strict pass (bootstrap not used)."
elif decision_matrix_lane_status == "bootstrap":
    if isinstance(decision_matrix_bootstrap_hours_to_expiry, float):
        decision_matrix_lane_line = (
            "Decision matrix lane: bootstrap pass "
            f"({decision_matrix_bootstrap_elapsed_hours:.1f}h elapsed, "
            f"~{decision_matrix_bootstrap_hours_to_expiry:.1f}h to expiry)."
        )
    elif isinstance(decision_matrix_bootstrap_elapsed_hours, float):
        decision_matrix_lane_line = (
            "Decision matrix lane: bootstrap pass "
            f"({decision_matrix_bootstrap_elapsed_hours:.1f}h elapsed)."
        )
    else:
        decision_matrix_lane_line = "Decision matrix lane: bootstrap pass."
elif decision_matrix_lane_status == "bootstrap_blocked":
    blocker_reason = (
        ", ".join(decision_matrix_bootstrap_guard_reasons[:2])
        if decision_matrix_bootstrap_guard_reasons
        else (decision_matrix_bootstrap_guard_status or "guard blocked")
    )
    decision_matrix_lane_line = f"Decision matrix lane: bootstrap blocked ({blocker_reason})."
elif decision_matrix_lane_status == "matrix_failed":
    decision_matrix_lane_line = "Decision matrix lane: failed (strict and bootstrap both off)."
else:
    decision_matrix_lane_line = "Decision matrix lane: unavailable."

lane_alert_degraded_statuses_raw = (
    lane_alert_state.get("degraded_statuses")
    if isinstance(lane_alert_state.get("degraded_statuses"), list)
    else []
)
lane_alert_degraded_statuses = {
    _normalize(item).lower()
    for item in lane_alert_degraded_statuses_raw
    if _normalize(item)
}
lane_alert_streak_count = max(0, _parse_int(lane_alert_state.get("degraded_streak_count")))
lane_alert_streak_threshold = max(0, _parse_int(lane_alert_state.get("degraded_streak_threshold")))
lane_alert_streak_notify_every = max(0, _parse_int(lane_alert_state.get("degraded_streak_notify_every")))
lane_alert_status = _normalize(lane_alert_state.get("last_lane_status")).lower()
lane_alert_notify_reason = _normalize(lane_alert_state.get("last_notify_reason")).lower()
decision_matrix_lane_streak_line = ""
lane_status_is_degraded = bool(
    lane_alert_status
    and (
        (lane_alert_status in lane_alert_degraded_statuses)
        if lane_alert_degraded_statuses
        else (lane_alert_status in {"matrix_failed", "bootstrap_blocked"})
    )
)
if lane_alert_streak_count > 0 and lane_status_is_degraded:
    threshold_label = f"{lane_alert_streak_threshold:,}" if lane_alert_streak_threshold > 0 else "off"
    every_label = f"{lane_alert_streak_notify_every:,}" if lane_alert_streak_notify_every > 0 else "n/a"
    decision_matrix_lane_streak_line = (
        "Decision matrix degraded streak: "
        f"{lane_alert_streak_count:,} run(s) "
        f"({lane_alert_status.replace('_', ' ')}, threshold {threshold_label}, every {every_label})"
    )
    if lane_alert_notify_reason == "degraded_streak":
        decision_matrix_lane_streak_line += " [streak alert fired]"
    decision_matrix_lane_streak_line += "."

non_approved_items: list[tuple[str, int]] = []
for key, raw_count in reason_counts.items():
    reason = _normalize(key)
    if not reason or reason.lower() == "approved":
        continue
    count = max(0, _parse_int(raw_count))
    if count <= 0:
        continue
    non_approved_items.append((reason, count))
non_approved_items.sort(key=lambda item: item[1], reverse=True)
blocked_total = sum(count for _, count in non_approved_items)
flow_totals_consistent = (
    (intents_total == (approved_count + blocked_total))
    if intents_total > 0
    else True
)
largest_blocker_present_if_blocked = (
    (len(non_approved_items) > 0)
    if blocked_total > 0
    else True
)

top_blockers: list[dict[str, Any]] = []
for rank, (reason, count) in enumerate(non_approved_items[:top_n], start=1):
    share_blocked = (count / float(blocked_total)) if blocked_total > 0 else 0.0
    share_intents = (count / float(intents_total)) if intents_total > 0 else 0.0
    top_blockers.append(
        {
            "rank": int(rank),
            "reason": reason,
            "reason_human": _humanize_reason(reason),
            "count": int(count),
            "share_of_blocked": round(float(share_blocked), 6),
            "share_of_intents": round(float(share_intents), 6),
            "recommended_action": _reason_action(reason),
        }
    )

# Avoid misleading action guidance when settlement backlog is currently zero.
if current_settlement_unresolved <= 0:
    for row in top_blockers:
        if _normalize(row.get("reason")).lower() == "settlement_finalization_blocked":
            row["recommended_action"] = (
                "Current settlement backlog is zero (blocked underlyings and pending final reports are clear); "
                "keep pressure mode enabled and prioritize other active blockers unless backlog reappears."
            )

display_top_blockers = top_blockers
if current_settlement_unresolved <= 0 and top_blockers:
    filtered = [
        row
        for row in top_blockers
        if _normalize(row.get("reason")).lower() != "settlement_finalization_blocked"
    ]
    if filtered:
        display_top_blockers = filtered

largest_blocker = display_top_blockers[0] if display_top_blockers else {}
largest_blocker_reason = _normalize(largest_blocker.get("reason"))
largest_blocker_count = _parse_int(largest_blocker.get("count"))
largest_blocker_share_blocked = float(largest_blocker.get("share_of_blocked") or 0.0)
largest_blocker_raw = top_blockers[0] if top_blockers else {}
largest_blocker_reason_raw = _normalize(largest_blocker_raw.get("reason"))
largest_blocker_count_raw = _parse_int(largest_blocker_raw.get("count"))
largest_blocker_share_blocked_raw = float(largest_blocker_raw.get("share_of_blocked") or 0.0)

recommendations: list[str] = []
recommendations_short: list[str] = []
for row in display_top_blockers[:5]:
    share_pct = (float(row["share_of_blocked"]) * 100.0) if blocked_total > 0 else 0.0
    reason_text = f"{row['reason_human']} ({row['count']:,}, {share_pct:.1f}%)"
    action_text = str(row["recommended_action"] or "").strip()
    recommendations.append(f"{reason_text}: {action_text}")
    clipped_action = action_text
    if len(clipped_action) > 112:
        clipped_action = clipped_action[:109].rstrip() + "..."
    recommendations_short.append(f"{reason_text}: {clipped_action}")

summary_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
blocked_rate = (float(blocked_total) / float(intents_total)) if intents_total > 0 else 0.0
approved_rate = (float(approved_count) / float(intents_total)) if intents_total > 0 else 0.0
base_lines = [
    f"Weekly Win-Rate Blocker Check ({window_label}, rolling) — {summary_time}",
    (
        "What happened: "
        f"{blocked_total:,}/{intents_total:,} signals were blocked ({blocked_rate*100.0:.2f}%), "
        f"{approved_count:,} were approved ({approved_rate*100.0:.2f}%)."
    ),
    "Performance basis: shadow non-approved reasons (not live losses).",
]
if not flow_totals_consistent:
    base_lines.append(
        "Data quality check: totals do not add up (signals != approved + blocked); review the window data before acting."
    )
if window_summary_parse_error:
    base_lines.append(
        "Data quality check: blocker window summary malformed; review summarize_window output before trusting blocker guidance."
    )
if not largest_blocker_present_if_blocked:
    base_lines.append(
        "Data quality check: blocked flow exists but no blocker rows were produced; review reason parsing."
    )
if display_top_blockers:
    base_lines.append(
        "Biggest blocker this week: "
        f"{display_top_blockers[0]['reason_human']} "
        f"({display_top_blockers[0]['count']:,}, {display_top_blockers[0]['share_of_blocked']*100.0:.2f}% of blocked)."
    )
else:
    base_lines.append("Biggest blocker this week: none.")
base_lines.append(
    "Settlement backlog now: "
    f"blocked underlyings {current_settlement_blocked_underlyings:,} | "
    f"pending final reports {current_settlement_pending_final_report:,}"
)
base_lines.append(decision_matrix_lane_line)
if decision_matrix_lane_streak_line:
    base_lines.append(decision_matrix_lane_streak_line)
base_lines.append("Why this matters: this is a shadow-quality blocker view, not realized live PnL.")

detailed_lines = base_lines[:] + ["", "Next focus actions:"]
for idx, item in enumerate(recommendations[:5], start=1):
    detailed_lines.append(f"{idx}. {item}")
discord_message_detailed = "\n".join(detailed_lines)
if len(discord_message_detailed) > 1900:
    detailed_compact = base_lines[:] + ["", "Next focus actions:"]
    for idx, item in enumerate(recommendations_short[:4], start=1):
        detailed_compact.append(f"{idx}. {item}")
    discord_message_detailed = "\n".join(detailed_compact)
if len(discord_message_detailed) > 1900:
    discord_message_detailed = discord_message_detailed[:1897].rstrip() + "..."

concise_lines = base_lines[:] + ["", "Top 3 fixes this week:"]
for idx, item in enumerate(recommendations_short[:3], start=1):
    concise_lines.append(f"{idx}. {item}")
discord_message_concise = "\n".join(concise_lines)
if len(discord_message_concise) > 1900:
    concise_lines = base_lines[:] + ["", "Top 2 fixes this week:"]
    for idx, item in enumerate(recommendations_short[:2], start=1):
        concise_lines.append(f"{idx}. {item}")
    discord_message_concise = "\n".join(concise_lines)
if len(discord_message_concise) > 1900:
    discord_message_concise = discord_message_concise[:1897].rstrip() + "..."

discord_message = discord_message_concise if discord_mode == "concise" else discord_message_detailed
message_quality_checks = {
    "selected_mode": discord_mode,
    "selected_message_length_ok": len(discord_message) <= 1900,
    "concise_message_length_ok": len(discord_message_concise) <= 1900,
    "detailed_message_length_ok": len(discord_message_detailed) <= 1900,
    "contains_performance_basis_line": ("Performance basis:" in discord_message),
    "window_summary_loaded": bool(window_summary_loaded),
    "window_summary_parse_error_present": bool(window_summary_parse_error),
    "window_summary_parse_error": window_summary_parse_error,
    "flow_totals_consistent": bool(flow_totals_consistent),
    "largest_blocker_present_if_blocked": bool(largest_blocker_present_if_blocked),
}
message_quality_checks["overall_pass"] = bool(
    message_quality_checks["selected_message_length_ok"]
    and message_quality_checks["concise_message_length_ok"]
    and message_quality_checks["detailed_message_length_ok"]
    and message_quality_checks["contains_performance_basis_line"]
    and message_quality_checks["flow_totals_consistent"]
    and message_quality_checks["largest_blocker_present_if_blocked"]
)

result = {
    "status": "ready",
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "window_label": window_label,
    "discord_mode": discord_mode,
    "window_hours": float(window_label.replace("h", "")) if window_label.endswith("h") else None,
    "source_files": {
        "window_summary_file": str(window_summary_path),
        "coldmath_hardening_latest": str(coldmath_hardening_path),
        "decision_matrix_hardening_latest": str(decision_matrix_path),
        "decision_matrix_lane_alert_state": str(lane_alert_state_path),
    },
    "data_quality": {
        "window_summary_loaded": bool(window_summary_loaded),
        "window_summary_parse_error_present": bool(window_summary_parse_error),
        "window_summary_parse_error": window_summary_parse_error,
    },
    "headline": {
        "prediction_quality_basis": "unique_market_side",
        "largest_blocker_reason": largest_blocker_reason,
        "largest_blocker_count": int(largest_blocker_count),
        "largest_blocker_share_of_blocked": round(float(largest_blocker_share_blocked), 6),
        "largest_blocker_reason_raw": largest_blocker_reason_raw,
        "largest_blocker_count_raw": int(largest_blocker_count_raw),
        "largest_blocker_share_of_blocked_raw": round(float(largest_blocker_share_blocked_raw), 6),
        "blocked_total": int(blocked_total),
        "approved_count": int(approved_count),
        "intents_total": int(intents_total),
        "current_settlement_blocked_underlyings": int(current_settlement_blocked_underlyings),
        "current_settlement_pending_final_report": int(current_settlement_pending_final_report),
        "current_settlement_unresolved": int(current_settlement_unresolved),
    },
    "top_blockers": display_top_blockers,
    "top_blockers_raw": top_blockers,
    "decision_matrix_lane": {
        "status": decision_matrix_lane_status,
        "summary_line": decision_matrix_lane_line,
        "decision_matrix_signal": decision_matrix_signal,
        "decision_matrix_strict_signal": decision_matrix_strict_signal,
        "decision_matrix_bootstrap_signal_raw": decision_matrix_bootstrap_signal_raw,
        "decision_matrix_bootstrap_signal": decision_matrix_bootstrap_signal,
        "decision_matrix_supports_consistency_and_profitability": decision_matrix_supports_consistency,
        "decision_matrix_bootstrap_guard_status": decision_matrix_bootstrap_guard_status,
        "decision_matrix_bootstrap_guard_reasons": decision_matrix_bootstrap_guard_reasons,
        "decision_matrix_bootstrap_elapsed_hours": (
            round(float(decision_matrix_bootstrap_elapsed_hours), 6)
            if isinstance(decision_matrix_bootstrap_elapsed_hours, float)
            else None
        ),
        "decision_matrix_bootstrap_max_hours": (
            round(float(decision_matrix_bootstrap_max_hours), 6)
            if isinstance(decision_matrix_bootstrap_max_hours, float)
            else None
        ),
        "decision_matrix_bootstrap_hours_to_expiry": (
            round(float(decision_matrix_bootstrap_hours_to_expiry), 6)
            if isinstance(decision_matrix_bootstrap_hours_to_expiry, float)
            else None
        ),
        "degraded_streak_count": int(lane_alert_streak_count),
        "degraded_streak_threshold": int(lane_alert_streak_threshold),
        "degraded_streak_notify_every": int(lane_alert_streak_notify_every),
        "degraded_statuses": sorted(lane_alert_degraded_statuses),
        "degraded_streak_summary_line": decision_matrix_lane_streak_line,
        "last_notify_reason": lane_alert_notify_reason,
    },
    "recommendations": recommendations,
    "discord_message": discord_message,
    "discord_message_detailed": discord_message_detailed,
    "discord_message_concise": discord_message_concise,
    "message_quality_checks": message_quality_checks,
}

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
latest_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
print(str(output_path))
PY

if [[ "$BLOCKER_AUDIT_SEND_WEBHOOK" == "1" && -n "$BLOCKER_AUDIT_WEBHOOK_TARGET_URL" ]]; then
  webhook_payload="$("$PYTHON_BIN" - "$audit_file" "$BLOCKER_AUDIT_WEBHOOK_USERNAME" <<'PY'
from __future__ import annotations

import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
username = str(sys.argv[2] or "").strip() or "BetBot Ops"
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    payload = {}
text = str(payload.get("discord_message") or "").strip()
if not text:
    text = "BetBot blocker audit failed to render."
print(json.dumps({"text": text, "content": text, "username": username}))
PY
)"
  curl --silent --show-error --fail \
    --max-time "$BLOCKER_AUDIT_WEBHOOK_TIMEOUT_SECONDS" \
    --header "Content-Type: application/json" \
    --data-binary "$webhook_payload" \
    "$BLOCKER_AUDIT_WEBHOOK_TARGET_URL" >/dev/null 2>&1 || true
fi

echo "blocker_audit file=$audit_file window=$window_label webhook_sent=$BLOCKER_AUDIT_SEND_WEBHOOK" >> "$LOG_FILE"
echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") blocker audit cycle end ===" >> "$LOG_FILE"
