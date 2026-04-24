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

MAINT_ENABLED="${ARTIFACT_MAINTENANCE_ENABLED:-1}"

PRUNE_LIVE_READINESS_HOURS="${ARTIFACT_PRUNE_LIVE_READINESS_HOURS:-48}"
PRUNE_GO_LIVE_GATE_HOURS="${ARTIFACT_PRUNE_GO_LIVE_GATE_HOURS:-72}"
PRUNE_METAR_OBSERVATIONS_HOURS="${ARTIFACT_PRUNE_METAR_OBSERVATIONS_HOURS:-72}"
PRUNE_METAR_RAW_HOURS="${ARTIFACT_PRUNE_METAR_RAW_HOURS:-72}"
PRUNE_ALPHA_WORKER_HOURS="${ARTIFACT_PRUNE_ALPHA_WORKER_HOURS:-72}"
PRUNE_BREADTH_WORKER_HOURS="${ARTIFACT_PRUNE_BREADTH_WORKER_HOURS:-72}"
PRUNE_CHECKPOINTS_HOURS="${ARTIFACT_PRUNE_CHECKPOINTS_HOURS:-336}"

KEEP_LIVE_READINESS_LATEST="${ARTIFACT_KEEP_LIVE_READINESS_LATEST:-48}"
KEEP_GO_LIVE_GATE_LATEST="${ARTIFACT_KEEP_GO_LIVE_GATE_LATEST:-48}"
KEEP_METAR_OBSERVATIONS_LATEST="${ARTIFACT_KEEP_METAR_OBSERVATIONS_LATEST:-2880}"
KEEP_METAR_RAW_LATEST="${ARTIFACT_KEEP_METAR_RAW_LATEST:-2880}"
KEEP_CHECKPOINTS_LATEST="${ARTIFACT_KEEP_CHECKPOINTS_LATEST:-2000}"

HEALTH_DIR="$OUTPUT_DIR/health/artifact_maintenance"
mkdir -p "$OUTPUT_DIR/logs" "$HEALTH_DIR"
LOG_FILE="$OUTPUT_DIR/logs/artifact_maintenance.log"
LOCK_FILE="$OUTPUT_DIR/.artifact_maintenance.lock"
LATEST_FILE="$HEALTH_DIR/artifact_maintenance_latest.json"
EVENT_FILE="$HEALTH_DIR/artifact_maintenance_$(date -u +"%Y%m%d_%H%M%S").json"

exec 9>"$LOCK_FILE"
if command -v flock >/dev/null 2>&1; then
  if ! flock -n 9; then
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] artifact maintenance skipped: lock busy" >> "$LOG_FILE"
    exit 0
  fi
fi

echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") artifact maintenance cycle start ===" >> "$LOG_FILE"

tmp_summary="$(mktemp)"
trap 'rm -f "$tmp_summary"' EXIT

python3 - "$OUTPUT_DIR" "$LATEST_FILE" "$EVENT_FILE" "$tmp_summary" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any


def _normalize(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_stat_mtime(path: Path) -> float | None:
    try:
        return float(path.stat().st_mtime)
    except Exception:
        return None


def _safe_stat_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except Exception:
        return 0


def _iter_files(base_dir: Path, mode: str, pattern: str) -> list[Path]:
    if not base_dir.exists():
        return []
    if mode == "glob":
        return [p for p in base_dir.glob(pattern) if p.is_file()]
    if mode == "rglob":
        return [p for p in base_dir.rglob(pattern) if p.is_file()]
    return []


def _prune_rule(output_dir: Path, now_epoch: float, rule: dict[str, Any]) -> dict[str, Any]:
    label = _normalize(rule.get("label")) or "rule"
    subdir = _normalize(rule.get("subdir"))
    mode = _normalize(rule.get("mode")) or "glob"
    pattern = _normalize(rule.get("pattern")) or "*"
    max_age_h = max(0.0, _to_float(rule.get("max_age_hours"), 0.0))
    keep_latest = max(0, _to_int(rule.get("keep_latest"), 0))
    keep_latest_suffix = _normalize(rule.get("keep_latest_suffix"))
    skip_dotfiles = bool(rule.get("skip_dotfiles"))

    base_dir = output_dir / subdir if subdir else output_dir
    files = _iter_files(base_dir, mode, pattern)

    file_rows: list[tuple[Path, float]] = []
    for path in files:
        mtime = _safe_stat_mtime(path)
        if isinstance(mtime, float):
            file_rows.append((path, mtime))
    file_rows.sort(key=lambda item: item[1], reverse=True)

    deleted_count = 0
    deleted_bytes = 0
    errors: list[str] = []
    retained_recent_count = min(len(file_rows), keep_latest)
    cutoff_epoch = now_epoch - (max_age_h * 3600.0)

    for idx, (path, _) in enumerate(file_rows):
        if idx < keep_latest:
            continue
        if skip_dotfiles and path.name.startswith("."):
            continue
        if keep_latest_suffix and path.name.endswith(keep_latest_suffix):
            continue
        mtime = _safe_stat_mtime(path)
        if not isinstance(mtime, float):
            continue
        if mtime >= cutoff_epoch:
            continue
        size = _safe_stat_size(path)
        try:
            path.unlink()
            deleted_count += 1
            deleted_bytes += size
        except FileNotFoundError:
            continue
        except Exception as exc:
            errors.append(f"{path}: {exc}")

    # Best-effort cleanup of empty directories for recursive rules.
    if mode == "rglob" and base_dir.exists():
        dirs = sorted(
            [p for p in base_dir.rglob("*") if p.is_dir()],
            key=lambda p: len(p.parts),
            reverse=True,
        )
        for d in dirs:
            try:
                d.rmdir()
            except Exception:
                pass

    return {
        "label": label,
        "subdir": subdir or None,
        "mode": mode,
        "pattern": pattern,
        "max_age_hours": max_age_h,
        "keep_latest": keep_latest,
        "keep_latest_suffix": keep_latest_suffix or None,
        "input_file_count": len(file_rows),
        "retained_recent_count": retained_recent_count,
        "deleted_count": deleted_count,
        "deleted_bytes": deleted_bytes,
        "deleted_gib": round(deleted_bytes / (1024.0**3), 6),
        "errors": errors[:10],
        "error_count": len(errors),
    }


output_dir = Path(sys.argv[1])
latest_file = Path(sys.argv[2])
event_file = Path(sys.argv[3])
summary_file = Path(sys.argv[4])

maint_enabled = _normalize(os.environ.get("ARTIFACT_MAINTENANCE_ENABLED", "1")) in {"1", "true", "yes", "y", "on"}
now_dt = datetime.now(timezone.utc)
now_epoch = now_dt.timestamp()

disk_before = shutil.disk_usage(output_dir)

rules = [
    {
        "label": "live_readiness",
        "mode": "glob",
        "pattern": "kalshi_temperature_live_readiness_*.json",
        "max_age_hours": _to_float(os.environ.get("ARTIFACT_PRUNE_LIVE_READINESS_HOURS"), 48.0),
        "keep_latest": _to_int(os.environ.get("ARTIFACT_KEEP_LIVE_READINESS_LATEST"), 48),
    },
    {
        "label": "go_live_gate",
        "mode": "glob",
        "pattern": "kalshi_temperature_go_live_gate_*.json",
        "max_age_hours": _to_float(os.environ.get("ARTIFACT_PRUNE_GO_LIVE_GATE_HOURS"), 72.0),
        "keep_latest": _to_int(os.environ.get("ARTIFACT_KEEP_GO_LIVE_GATE_LATEST"), 48),
    },
    {
        "label": "metar_observations_csv",
        "mode": "glob",
        "pattern": "kalshi_temperature_metar_observations_*.csv",
        "max_age_hours": _to_float(os.environ.get("ARTIFACT_PRUNE_METAR_OBSERVATIONS_HOURS"), 72.0),
        "keep_latest": _to_int(os.environ.get("ARTIFACT_KEEP_METAR_OBSERVATIONS_LATEST"), 2880),
    },
    {
        "label": "metar_raw_gz",
        "subdir": "kalshi_temperature_metar_raw",
        "mode": "glob",
        "pattern": "*.gz",
        "max_age_hours": _to_float(os.environ.get("ARTIFACT_PRUNE_METAR_RAW_HOURS"), 72.0),
        "keep_latest": _to_int(os.environ.get("ARTIFACT_KEEP_METAR_RAW_LATEST"), 2880),
    },
    {
        "label": "alpha_workers_recursive",
        "subdir": "alpha_workers",
        "mode": "rglob",
        "pattern": "*",
        "max_age_hours": _to_float(os.environ.get("ARTIFACT_PRUNE_ALPHA_WORKER_HOURS"), 72.0),
        "keep_latest": 0,
        "keep_latest_suffix": "_latest.json",
        "skip_dotfiles": True,
    },
    {
        "label": "breadth_workers_recursive",
        "subdir": "breadth_worker",
        "mode": "rglob",
        "pattern": "*",
        "max_age_hours": _to_float(os.environ.get("ARTIFACT_PRUNE_BREADTH_WORKER_HOURS"), 72.0),
        "keep_latest": 0,
        "keep_latest_suffix": "_latest.json",
        "skip_dotfiles": True,
    },
    {
        "label": "checkpoints_json",
        "subdir": "checkpoints",
        "mode": "glob",
        "pattern": "*.json",
        "max_age_hours": _to_float(os.environ.get("ARTIFACT_PRUNE_CHECKPOINTS_HOURS"), 336.0),
        "keep_latest": _to_int(os.environ.get("ARTIFACT_KEEP_CHECKPOINTS_LATEST"), 2000),
        "keep_latest_suffix": "_latest.json",
    },
]

categories: list[dict[str, Any]] = []
if maint_enabled:
    for rule in rules:
        categories.append(_prune_rule(output_dir, now_epoch, rule))
else:
    for rule in rules:
        categories.append(
            {
                "label": _normalize(rule.get("label")) or "rule",
                "deleted_count": 0,
                "deleted_bytes": 0,
                "deleted_gib": 0.0,
                "error_count": 0,
                "errors": [],
                "max_age_hours": _to_float(rule.get("max_age_hours"), 0.0),
                "keep_latest": _to_int(rule.get("keep_latest"), 0),
                "pattern": _normalize(rule.get("pattern")) or "*",
                "mode": _normalize(rule.get("mode")) or "glob",
                "subdir": _normalize(rule.get("subdir")) or None,
                "maintenance_disabled": True,
            }
        )

disk_after = shutil.disk_usage(output_dir)

total_deleted_count = int(sum(int(item.get("deleted_count") or 0) for item in categories))
total_deleted_bytes = int(sum(int(item.get("deleted_bytes") or 0) for item in categories))
total_error_count = int(sum(int(item.get("error_count") or 0) for item in categories))

payload = {
    "status": "ready",
    "captured_at": now_dt.isoformat(),
    "output_dir": str(output_dir),
    "maintenance_enabled": bool(maint_enabled),
    "summary": {
        "total_deleted_count": total_deleted_count,
        "total_deleted_bytes": total_deleted_bytes,
        "total_deleted_gib": round(total_deleted_bytes / (1024.0**3), 6),
        "total_error_count": total_error_count,
    },
    "disk_usage": {
        "before": {
            "total_bytes": int(disk_before.total),
            "used_bytes": int(disk_before.used),
            "free_bytes": int(disk_before.free),
            "used_percent": round((disk_before.used / max(1, disk_before.total)) * 100.0, 4),
        },
        "after": {
            "total_bytes": int(disk_after.total),
            "used_bytes": int(disk_after.used),
            "free_bytes": int(disk_after.free),
            "used_percent": round((disk_after.used / max(1, disk_after.total)) * 100.0, 4),
        },
    },
    "categories": categories,
}

latest_file.parent.mkdir(parents=True, exist_ok=True)
event_file.parent.mkdir(parents=True, exist_ok=True)
summary_file.parent.mkdir(parents=True, exist_ok=True)

encoded = json.dumps(payload, indent=2)
latest_file.write_text(encoded, encoding="utf-8")
event_file.write_text(encoded, encoding="utf-8")
summary_file.write_text(encoded, encoding="utf-8")
PY

deleted_count="$(
  python3 - <<'PY' "$tmp_summary"
from __future__ import annotations
import json
import sys
payload = json.loads(open(sys.argv[1], "r", encoding="utf-8").read())
print(int(((payload.get("summary") or {}).get("total_deleted_count") or 0)))
PY
)"

deleted_gib="$(
  python3 - <<'PY' "$tmp_summary"
from __future__ import annotations
import json
import sys
payload = json.loads(open(sys.argv[1], "r", encoding="utf-8").read())
value = float(((payload.get("summary") or {}).get("total_deleted_gib") or 0.0))
print(f"{value:.2f}")
PY
)"

used_percent_after="$(
  python3 - <<'PY' "$tmp_summary"
from __future__ import annotations
import json
import sys
payload = json.loads(open(sys.argv[1], "r", encoding="utf-8").read())
value = float((((payload.get("disk_usage") or {}).get("after") or {}).get("used_percent") or 0.0))
print(f"{value:.2f}")
PY
)"

echo "artifact maintenance deleted_count=$deleted_count deleted_gib=$deleted_gib disk_used_percent_after=$used_percent_after" >> "$LOG_FILE"
echo "=== $(date -u +"%Y-%m-%dT%H:%M:%SZ") artifact maintenance cycle end ===" >> "$LOG_FILE"
