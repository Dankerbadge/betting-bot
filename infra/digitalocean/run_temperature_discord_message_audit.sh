#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" ]]; then
  echo "env file is not readable (check owner/group/perms): $ENV_FILE" >&2
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

AUDIT_DIR="$OUTPUT_DIR/health/discord_message_audit"
mkdir -p "$AUDIT_DIR"

STAMP="$(date -u +"%Y%m%d_%H%M%S")"
OUT_FILE="$AUDIT_DIR/discord_message_audit_${STAMP}.json"
LATEST_FILE="$AUDIT_DIR/discord_message_audit_latest.json"

"$PYTHON_BIN" - "$OUTPUT_DIR" "$OUT_FILE" "$LATEST_FILE" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
from typing import Any


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists() and path.is_file():
            return path
    return None


def _latest_glob(path: Path, pattern: str) -> Path | None:
    candidates = sorted(path.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _extract_message_alpha(payload: dict[str, Any]) -> str:
    message_summary = payload.get("message_summary")
    if isinstance(message_summary, dict):
        selected = _normalize(message_summary.get("selected"))
        if selected:
            return selected
    for key in ("discord_message", "discord_summary_text", "discord_summary"):
        text = _normalize(payload.get(key))
        if text:
            return text
    return ""


def _extract_message_blocker(payload: dict[str, Any]) -> str:
    for key in ("discord_message", "discord_message_concise", "discord_message_detailed"):
        text = _normalize(payload.get(key))
        if text:
            return text
    return ""


def _extract_message_route_guard(payload: dict[str, Any]) -> str:
    for key in ("discord_message_preview", "discord_message", "discord_message_concise"):
        text = _normalize(payload.get(key))
        if text:
            return text
    return ""


def _extract_message_recovery(payload: dict[str, Any], output_dir: Path) -> str:
    direct = _normalize(payload.get("discord_message_preview"))
    if direct:
        return direct
    recovery_dir = output_dir / "health" / "recovery"
    latest_event = _latest_glob(recovery_dir, "recovery_event_*.json")
    if latest_event:
        event_payload = _load_json(latest_event)
        preview = _normalize(event_payload.get("discord_message_preview"))
        if preview:
            return preview
    health = payload.get("health") if isinstance(payload.get("health"), dict) else {}
    pipeline = payload.get("pipeline") if isinstance(payload.get("pipeline"), dict) else {}
    services = payload.get("service_states") if isinstance(payload.get("service_states"), dict) else {}
    health_status = _normalize(health.get("status")).upper() or "UNKNOWN"
    pipeline_status = _normalize(pipeline.get("status")).upper() or "UNKNOWN"
    shadow = _normalize(services.get("shadow_service_state")) or "unknown"
    return (
        "BetBot Recovery Action\n"
        f"State: STABLE | health {health_status} | pipeline {pipeline_status}\n"
        f"Issue: none\n"
        f"Action taken: none\n"
        f"Services: shadow {shadow}"
    )


def _extract_message_stale_drill(payload: dict[str, Any]) -> str:
    preview = _normalize(payload.get("discord_message_preview"))
    if preview:
        return preview
    status = _normalize(payload.get("status")).upper() or "UNKNOWN"
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    cycle_count = int(float(metrics.get("cycle_count") or 0))
    stale_hits = int(float(metrics.get("blocker_metrics_stale_cycle_count") or 0))
    return (
        "BetBot Stale Metrics Drill\n"
        f"State: {status}\n"
        f"Coverage: cycles {cycle_count} | stale-cycle hits {stale_hits}"
    )


def _score_message(text: str) -> tuple[int, list[str], dict[str, Any]]:
    lines = [line.rstrip() for line in text.splitlines() if _normalize(line)]
    chars = len(text)
    max_line_len = max((len(line) for line in lines), default=0)
    long_line_count = sum(1 for line in lines if len(line) > 140)
    ellipsis_count = text.count("...")
    jargon_tokens = re.findall(r"\b[a-z]+_[a-z0-9_]+\b", text)
    jargon_tokens = [
        token for token in jargon_tokens
        if token not in {"n_a", "top_n"}
    ]
    flags: list[str] = []
    score = 100
    if not text:
        score -= 70
        flags.append("missing_message")
    if chars > 1500:
        score -= 15
        flags.append("too_long")
    elif chars > 1000:
        score -= 8
        flags.append("long_message")
    if long_line_count > 0:
        score -= min(15, long_line_count * 3)
        flags.append("long_lines")
    if ellipsis_count > 0:
        score -= min(8, ellipsis_count * 2)
        flags.append("ellipsis_present")
    if len(jargon_tokens) > 2:
        score -= min(12, len(jargon_tokens))
        flags.append("jargon_tokens_present")
    has_action_signal = any(
        marker in text
        for marker in (
            "Action:",
            "Operator action:",
            "Action taken:",
            "Best next action:",
            "Top 3 optimization moves:",
            "Top 3 blocker fixes this week:",
            "Run:",
        )
    )
    if not has_action_signal:
        score -= 6
        flags.append("missing_action_line")
    score = max(0, min(100, score))
    details = {
        "line_count": len(lines),
        "char_count": chars,
        "max_line_length": max_line_len,
        "long_line_count": long_line_count,
        "ellipsis_count": ellipsis_count,
        "jargon_token_count": len(jargon_tokens),
        "jargon_tokens_sample": jargon_tokens[:8],
    }
    return score, flags, details


output_dir = Path(sys.argv[1])
out_file = Path(sys.argv[2])
latest_file = Path(sys.argv[3])

alpha_file = output_dir / "health" / "alpha_summary_latest.json"
blocker_file = output_dir / "checkpoints" / "blocker_audit_168h_latest.json"
route_guard_file = output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json"
recovery_file = output_dir / "health" / "recovery" / "recovery_latest.json"
stale_drill_file = output_dir / "recovery_chaos" / "stale_metrics_drill" / "stale_metrics_drill_latest.json"

streams: list[dict[str, Any]] = []

stream_specs = [
    ("alpha_summary", alpha_file, _extract_message_alpha),
    ("blocker_audit", blocker_file, _extract_message_blocker),
    ("discord_route_guard", route_guard_file, _extract_message_route_guard),
    ("pipeline_recovery", recovery_file, lambda payload: _extract_message_recovery(payload, output_dir)),
    ("stale_metrics_drill", stale_drill_file, _extract_message_stale_drill),
]

for name, path, extractor in stream_specs:
    payload = _load_json(path) if path.exists() else {}
    message = extractor(payload) if payload else ""
    score, flags, details = _score_message(message)
    stream_row = {
        "stream": name,
        "source_file": str(path),
        "source_exists": bool(path.exists()),
        "score": score,
        "flags": flags,
        "details": details,
        "message_preview": message,
    }
    streams.append(stream_row)

available_scores = [int(row["score"]) for row in streams if row.get("source_exists")]
overall_score = round(sum(available_scores) / len(available_scores), 1) if available_scores else 0.0
worst_streams = sorted(streams, key=lambda row: int(row.get("score", 0)))[:3]

recommendations: list[str] = []
if any("too_long" in row.get("flags", []) or "long_message" in row.get("flags", []) for row in streams):
    recommendations.append("Trim verbose sections to keep each message under ~1000 chars.")
if any("long_lines" in row.get("flags", []) for row in streams):
    recommendations.append("Wrap long lines at 120 chars for mobile Discord readability.")
if any("ellipsis_present" in row.get("flags", []) for row in streams):
    recommendations.append("Replace hard truncation with word-safe clipping to avoid broken phrases.")
if any("jargon_tokens_present" in row.get("flags", []) for row in streams):
    recommendations.append("Replace internal token names with plain-English labels.")
if any("missing_action_line" in row.get("flags", []) for row in streams):
    recommendations.append("Ensure every alert includes one explicit next action line.")

route_guard_payload = _load_json(route_guard_file) if route_guard_file.exists() else {}
route_remediations = (
    route_guard_payload.get("route_remediations")
    if isinstance(route_guard_payload.get("route_remediations"), list)
    else []
)
missing_thread_keys: list[str] = []
for row in route_remediations:
    if not isinstance(row, dict):
        continue
    for key in row.get("required_thread_env_keys") or []:
        text = _normalize(key)
        if text and text not in missing_thread_keys:
            missing_thread_keys.append(text)

payload = {
    "status": "ready",
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "output_dir": str(output_dir),
    "overall_score": overall_score,
    "streams_total": len(streams),
    "streams": streams,
    "worst_streams": [
        {
            "stream": row.get("stream"),
            "score": row.get("score"),
            "flags": row.get("flags"),
        }
        for row in worst_streams
    ],
    "recommendations": recommendations[:5],
    "route_guard": {
        "status": _normalize(route_guard_payload.get("guard_status")) or "unknown",
        "shared_route_group_count": int(float(route_guard_payload.get("shared_route_group_count") or 0)),
        "missing_thread_key_count": len(missing_thread_keys),
        "missing_thread_keys": missing_thread_keys,
    },
}

out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
latest_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(str(out_file))
print(f"overall_score={overall_score}")
for row in streams:
    print(f"{row['stream']}: score={row['score']} flags={','.join(row['flags']) or 'none'}")
PY

echo "discord_message_audit latest=$LATEST_FILE"
