#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

ENV_FILE="/etc/betbot/temperature-shadow.env"
MAP_FILE="$SCRIPT_DIR/discord_thread_map.template.env"
LATEST_FILE=""
STRICT=0
APPLY=0
JSON_OUTPUT=0
NO_RESTART=0
NO_AUDIT=0

if [[ -f "/etc/betbot/discord-thread-map.env" ]]; then
  MAP_FILE="/etc/betbot/discord-thread-map.env"
fi

usage() {
  cat <<'EOF'
usage:
  check_discord_thread_map.sh [--env <path>] [--map <path>] [--latest <path>] [--strict] [--json] [--apply] [--no-restart] [--no-audit]

defaults:
  --env /etc/betbot/temperature-shadow.env
  --map /etc/betbot/discord-thread-map.env (preferred if present), else infra/digitalocean/discord_thread_map.template.env
  --latest <OUTPUT_DIR>/health/discord_route_guard/discord_route_guard_latest.json (auto-derived from env)

flags:
  --strict      exit non-zero when required thread IDs are missing in map or env is out-of-sync
  --json        emit machine-readable JSON summary
  --apply       when map is complete for required keys, run apply_discord_thread_map.sh
  --no-restart  pass through to apply_discord_thread_map.sh
  --no-audit    pass through to apply_discord_thread_map.sh
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      [[ $# -ge 2 ]] || { echo "missing value for --env" >&2; exit 1; }
      ENV_FILE="$2"
      shift 2
      ;;
    --map)
      [[ $# -ge 2 ]] || { echo "missing value for --map" >&2; exit 1; }
      MAP_FILE="$2"
      shift 2
      ;;
    --latest)
      [[ $# -ge 2 ]] || { echo "missing value for --latest" >&2; exit 1; }
      LATEST_FILE="$2"
      shift 2
      ;;
    --strict)
      STRICT=1
      shift
      ;;
    --json)
      JSON_OUTPUT=1
      shift
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    --no-restart)
      NO_RESTART=1
      shift
      ;;
    --no-audit)
      NO_AUDIT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -f "$MAP_FILE" ]]; then
  echo "missing map file: $MAP_FILE" >&2
  exit 1
fi

if [[ -z "$LATEST_FILE" ]]; then
  derived_output_dir="$(awk -F= '$1=="OUTPUT_DIR"{print substr($0, index($0, "=")+1)}' "$ENV_FILE" | tail -n 1 | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | sed 's/^["'"'"']//; s/["'"'"']$//')"
  if [[ -n "$derived_output_dir" && -f "$derived_output_dir/health/discord_route_guard/discord_route_guard_latest.json" ]]; then
    LATEST_FILE="$derived_output_dir/health/discord_route_guard/discord_route_guard_latest.json"
  fi
fi

tmp_json="$(mktemp)"
trap 'rm -f "$tmp_json"' EXIT

python3 - "$ENV_FILE" "$MAP_FILE" "$LATEST_FILE" >"$tmp_json" <<'PY'
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
map_path = Path(sys.argv[2])
latest_path = Path(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else None

default_keys = [
    "SHADOW_ALERT_WEBHOOK_THREAD_ID",
    "ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID",
    "BLOCKER_AUDIT_WEBHOOK_THREAD_ID",
    "RECOVERY_WEBHOOK_THREAD_ID",
    "PIPELINE_ALERT_WEBHOOK_THREAD_ID",
    "RECOVERY_CHAOS_WEBHOOK_THREAD_ID",
    "STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID",
    "LOG_MAINT_ALERT_WEBHOOK_THREAD_ID",
]


def _normalize_scalar(value: str) -> str:
    raw = (value or "").strip()
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {"'", '"'}:
        raw = raw[1:-1].strip()
    return raw


def _parse_kv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        out[key] = _normalize_scalar(value)
    return out


def _extract_thread_id(value: str) -> str:
    text = _normalize_scalar(value)
    if not text:
        return ""
    mention = re.fullmatch(r"<#(\d+)>", text)
    if mention:
        return mention.group(1)
    direct = re.fullmatch(r"\d{8,32}", text)
    if direct:
        return direct.group(0)
    m = re.search(r"[?&]thread_id=(\d{8,32})", text)
    if m:
        return m.group(1)
    m = re.search(r"/channels/\d+/\d+/(\d{8,32})", text)
    if m:
        return m.group(1)
    m = re.search(r"(\d{8,32})", text)
    return m.group(1) if m else text


def _mask_id(thread_id: str) -> str:
    if not thread_id:
        return ""
    if len(thread_id) <= 8:
        return thread_id
    return f"{thread_id[:4]}...{thread_id[-4:]}"


env_map = _parse_kv(env_path)
map_map = _parse_kv(map_path)

route_payload: dict[str, object] = {}
if latest_path and latest_path.exists():
    try:
        maybe = json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        maybe = {}
    if isinstance(maybe, dict):
        route_payload = maybe

required_keys: list[str] = []
for remediation in route_payload.get("route_remediations", []) if isinstance(route_payload.get("route_remediations"), list) else []:
    if not isinstance(remediation, dict):
        continue
    keys = remediation.get("required_thread_env_keys")
    if isinstance(keys, list):
        for key in keys:
            key_text = str(key or "").strip()
            if key_text:
                required_keys.append(key_text)
if not required_keys:
    required_keys = default_keys[:]
required_keys = sorted(set(required_keys))

all_known_keys = sorted(set(default_keys) | set(required_keys))
rows: list[dict[str, object]] = []

missing_required_in_map: list[str] = []
missing_required_in_env: list[str] = []
map_env_mismatch_required: list[str] = []

for key in all_known_keys:
    required = key in required_keys
    map_raw = map_map.get(key, "")
    env_raw = env_map.get(key, "")
    map_tid = _extract_thread_id(map_raw)
    env_tid = _extract_thread_id(env_raw)
    map_set = bool(map_tid)
    env_set = bool(env_tid)
    sync = (not map_set and not env_set) or (map_set and env_set and map_tid == env_tid)
    if required and not map_set:
        missing_required_in_map.append(key)
    if required and not env_set:
        missing_required_in_env.append(key)
    if required and map_set and env_set and map_tid != env_tid:
        map_env_mismatch_required.append(key)
    rows.append(
        {
            "key": key,
            "required": required,
            "map_set": map_set,
            "env_set": env_set,
            "in_sync": sync,
            "map_thread_id_masked": _mask_id(map_tid),
            "env_thread_id_masked": _mask_id(env_tid),
        }
    )

can_apply = len(missing_required_in_map) == 0 and len(required_keys) > 0
required_env_in_sync = (
    len(missing_required_in_env) == 0 and len(map_env_mismatch_required) == 0 and can_apply
)

out = {
    "status": "ready",
    "env_file": str(env_path),
    "map_file": str(map_path),
    "latest_route_guard_file": str(latest_path) if latest_path else "",
    "route_guard_status": str(route_payload.get("guard_status") or "unknown"),
    "route_guard_shared_route_group_count": int(route_payload.get("shared_route_group_count") or 0),
    "required_keys": required_keys,
    "rows": rows,
    "missing_required_in_map": missing_required_in_map,
    "missing_required_in_env": missing_required_in_env,
    "map_env_mismatch_required": map_env_mismatch_required,
    "can_apply": can_apply,
    "required_env_in_sync": required_env_in_sync,
}
print(json.dumps(out))
PY

if [[ "$JSON_OUTPUT" == "1" ]]; then
  cat "$tmp_json"
else
  python3 - "$tmp_json" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []

print("Discord Thread Map Check")
print(f"env_file: {payload.get('env_file')}")
print(f"map_file: {payload.get('map_file')}")
latest = str(payload.get("latest_route_guard_file") or "")
if latest:
    print(
        "route_guard: "
        f"{payload.get('route_guard_status')} "
        f"(shared_route_groups={payload.get('route_guard_shared_route_group_count')})"
    )
else:
    print("route_guard: unavailable")
print(f"required_keys: {len(payload.get('required_keys') or [])}")

missing_map = payload.get("missing_required_in_map") or []
missing_env = payload.get("missing_required_in_env") or []
mismatch = payload.get("map_env_mismatch_required") or []

if missing_map:
    print("missing_required_in_map: " + ", ".join(str(x) for x in missing_map))
if missing_env:
    print("missing_required_in_env: " + ", ".join(str(x) for x in missing_env))
if mismatch:
    print("map_env_mismatch_required: " + ", ".join(str(x) for x in mismatch))

print("")
for row in rows:
    if not isinstance(row, dict):
        continue
    key = str(row.get("key") or "")
    required = bool(row.get("required"))
    map_set = bool(row.get("map_set"))
    env_set = bool(row.get("env_set"))
    sync = bool(row.get("in_sync"))
    map_id = str(row.get("map_thread_id_masked") or "")
    env_id = str(row.get("env_thread_id_masked") or "")
    print(
        f"- {key}: "
        f"required={'yes' if required else 'no'} "
        f"map={'set' if map_set else 'missing'} "
        f"env={'set' if env_set else 'missing'} "
        f"sync={'yes' if sync else 'no'} "
        f"map_id={map_id or 'n/a'} env_id={env_id or 'n/a'}"
    )

print("")
if payload.get("required_env_in_sync"):
    print("result: required thread IDs are present and in sync.")
elif payload.get("can_apply"):
    print("result: map is complete for required keys; apply needed to sync env/services.")
else:
    print("result: map is incomplete for required keys.")
PY
fi

missing_required_in_map_count="$(python3 - "$tmp_json" <<'PY'
from __future__ import annotations
import json
import sys
payload = json.loads(open(sys.argv[1], "r", encoding="utf-8").read())
print(len(payload.get("missing_required_in_map") or []))
PY
)"
required_env_in_sync="$(python3 - "$tmp_json" <<'PY'
from __future__ import annotations
import json
import sys
payload = json.loads(open(sys.argv[1], "r", encoding="utf-8").read())
print("1" if payload.get("required_env_in_sync") else "0")
PY
)"
can_apply="$(python3 - "$tmp_json" <<'PY'
from __future__ import annotations
import json
import sys
payload = json.loads(open(sys.argv[1], "r", encoding="utf-8").read())
print("1" if payload.get("can_apply") else "0")
PY
)"

if [[ "$JSON_OUTPUT" != "1" && "$missing_required_in_map_count" =~ ^[0-9]+$ ]] && (( missing_required_in_map_count > 0 )); then
  echo ""
  echo "next_step:"
  echo "1) set missing thread IDs in map: $MAP_FILE"
  echo "2) preflight+apply once IDs are set:"
  echo "   sudo bash $SCRIPT_DIR/check_discord_thread_map.sh --env $ENV_FILE --map $MAP_FILE --strict --apply"
  echo ""
  echo "paste-ready (replace <thread_id_or_url>):"
  echo "  sudo bash $SCRIPT_DIR/set_discord_thread_ids.sh --restart --audit $ENV_FILE \\"
  python3 - "$tmp_json" <<'PY'
from __future__ import annotations
import json
import sys
payload = json.loads(open(sys.argv[1], "r", encoding="utf-8").read())
missing = [str(item) for item in (payload.get("missing_required_in_map") or []) if str(item)]
for idx, key in enumerate(missing):
    trailer = " \\" if idx < len(missing) - 1 else ""
    print(f"    {key}=<thread_id_or_url>{trailer}")
PY
fi

if [[ "$APPLY" == "1" ]]; then
  if [[ "$can_apply" != "1" ]]; then
    echo "apply skipped: map does not contain all required thread IDs" >&2
    exit 2
  fi
  apply_cmd=("$SCRIPT_DIR/apply_discord_thread_map.sh" --env "$ENV_FILE" --map "$MAP_FILE")
  if [[ "$NO_RESTART" == "1" ]]; then
    apply_cmd+=(--no-restart)
  fi
  if [[ "$NO_AUDIT" == "1" ]]; then
    apply_cmd+=(--no-audit)
  fi
  echo ""
  echo "applying thread map..."
  "${apply_cmd[@]}"
fi

if [[ "$STRICT" == "1" ]]; then
  if (( missing_required_in_map_count > 0 )); then
    exit 2
  fi
  if [[ "$required_env_in_sync" != "1" ]]; then
    exit 2
  fi
fi

exit 0
