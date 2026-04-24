#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
ENV_FILE="/etc/betbot/temperature-shadow.env"
GLOBAL_SECONDS=""
SNAPSHOT_SECONDS=""
MARKET_INGEST_SECONDS=""
ADVISOR_SECONDS=""
LOOP_SECONDS=""
CAMPAIGN_SECONDS=""
DISABLE_ALL=0
POSITIONAL_ENV_SET=0

usage() {
  cat <<'EOF'
usage:
  set_coldmath_stage_timeout_guardrails.sh [flags] [/etc/betbot/temperature-shadow.env]

behavior:
  - upserts the ColdMath timeout guardrail keys in the env file
  - creates a timestamped backup next to the env file before writing
  - validates every timeout value as a non-negative integer
  - prints a summary of the applied values

flags:
  --global-seconds <int>        base value used for any unset stage-specific timeout
  --snapshot-seconds <int>      override COLDMATH_SNAPSHOT_TIMEOUT_SECONDS
  --market-ingest-seconds <int> override COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS
  --advisor-seconds <int>       override COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS
  --loop-seconds <int>          override COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS
  --campaign-seconds <int>      override COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS
  --disable-all                 set all timeout guardrails to 0
  -h, --help                    show this help
EOF
}

is_nonnegative_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --global-seconds)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --global-seconds" >&2
        usage >&2
        exit 1
      fi
      GLOBAL_SECONDS="$2"
      shift 2
      ;;
    --snapshot-seconds)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --snapshot-seconds" >&2
        usage >&2
        exit 1
      fi
      SNAPSHOT_SECONDS="$2"
      shift 2
      ;;
    --market-ingest-seconds)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --market-ingest-seconds" >&2
        usage >&2
        exit 1
      fi
      MARKET_INGEST_SECONDS="$2"
      shift 2
      ;;
    --advisor-seconds)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --advisor-seconds" >&2
        usage >&2
        exit 1
      fi
      ADVISOR_SECONDS="$2"
      shift 2
      ;;
    --loop-seconds)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --loop-seconds" >&2
        usage >&2
        exit 1
      fi
      LOOP_SECONDS="$2"
      shift 2
      ;;
    --campaign-seconds)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --campaign-seconds" >&2
        usage >&2
        exit 1
      fi
      CAMPAIGN_SECONDS="$2"
      shift 2
      ;;
    --disable-all)
      DISABLE_ALL=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      if (( POSITIONAL_ENV_SET == 1 )); then
        echo "unexpected extra positional argument: $1" >&2
        usage >&2
        exit 1
      fi
      ENV_FILE="$1"
      POSITIONAL_ENV_SET=1
      shift
      ;;
  esac
done

if (( DISABLE_ALL == 1 )); then
  GLOBAL_SECONDS="0"
  SNAPSHOT_SECONDS="0"
  MARKET_INGEST_SECONDS="0"
  ADVISOR_SECONDS="0"
  LOOP_SECONDS="0"
  CAMPAIGN_SECONDS="0"
else
  if [[ -z "$GLOBAL_SECONDS" ]]; then
    GLOBAL_SECONDS="0"
  fi
  if [[ -z "$SNAPSHOT_SECONDS" ]]; then
    SNAPSHOT_SECONDS="$GLOBAL_SECONDS"
  fi
  if [[ -z "$MARKET_INGEST_SECONDS" ]]; then
    MARKET_INGEST_SECONDS="$GLOBAL_SECONDS"
  fi
  if [[ -z "$ADVISOR_SECONDS" ]]; then
    ADVISOR_SECONDS="$GLOBAL_SECONDS"
  fi
  if [[ -z "$LOOP_SECONDS" ]]; then
    LOOP_SECONDS="$GLOBAL_SECONDS"
  fi
  if [[ -z "$CAMPAIGN_SECONDS" ]]; then
    CAMPAIGN_SECONDS="$GLOBAL_SECONDS"
  fi
fi

for value in \
  "$GLOBAL_SECONDS" \
  "$SNAPSHOT_SECONDS" \
  "$MARKET_INGEST_SECONDS" \
  "$ADVISOR_SECONDS" \
  "$LOOP_SECONDS" \
  "$CAMPAIGN_SECONDS"
do
  if ! is_nonnegative_int "$value"; then
    echo "invalid timeout value: $value" >&2
    exit 1
  fi
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" ]]; then
  echo "env file is not readable: $ENV_FILE" >&2
  exit 1
fi

update_cmd=(python3 - "$ENV_FILE" "$GLOBAL_SECONDS" "$SNAPSHOT_SECONDS" "$MARKET_INGEST_SECONDS" "$ADVISOR_SECONDS" "$LOOP_SECONDS" "$CAMPAIGN_SECONDS")
if [[ ! -w "$ENV_FILE" ]]; then
  if [[ "$(id -u)" -ne 0 ]] && command -v sudo >/dev/null 2>&1; then
    update_cmd=(sudo "${update_cmd[@]}")
  else
    echo "env file is not writable and sudo is unavailable: $ENV_FILE" >&2
    exit 1
  fi
fi

"${update_cmd[@]}" <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import sys


KEY_RE = re.compile(r"^[A-Z0-9_]+$")
TARGET_KEYS = [
    "COLDMATH_STAGE_TIMEOUT_SECONDS",
    "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS",
    "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS",
    "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS",
    "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS",
    "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS",
]


def _parse_assignment_key(line: str) -> str:
    text = line.strip()
    if not text or text.startswith("#"):
        return ""
    if text.startswith("export "):
        text = text[7:].lstrip()
    if "=" not in text:
        return ""
    key_raw, _ = text.split("=", 1)
    key = key_raw.strip()
    if not KEY_RE.fullmatch(key):
        return ""
    return key


env_path = Path(sys.argv[1])
values = {
    "COLDMATH_STAGE_TIMEOUT_SECONDS": str(sys.argv[2] or "").strip(),
    "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS": str(sys.argv[3] or "").strip(),
    "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS": str(sys.argv[4] or "").strip(),
    "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS": str(sys.argv[5] or "").strip(),
    "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS": str(sys.argv[6] or "").strip(),
    "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS": str(sys.argv[7] or "").strip(),
}

for key, value in values.items():
    if not KEY_RE.fullmatch(key):
        raise SystemExit(f"invalid env key: {key!r}")
    if not re.fullmatch(r"^[0-9]+$", value):
        raise SystemExit(f"invalid timeout value for {key}: {value!r}")

original_text = env_path.read_text(encoding="utf-8")
original_norm = original_text if (original_text.endswith("\n") or original_text == "") else original_text + "\n"
lines = original_norm.splitlines(keepends=True)

rendered_lines: list[str] = []
seen_keys: set[str] = set()
for line in lines:
    key = _parse_assignment_key(line)
    if key in values:
      if key not in seen_keys:
        rendered_lines.append(f"{key}={values[key]}\n")
        seen_keys.add(key)
      continue
    rendered_lines.append(line)

if rendered_lines and rendered_lines[-1].strip():
    rendered_lines.append("\n")
for key in TARGET_KEYS:
    if key not in seen_keys:
        rendered_lines.append(f"{key}={values[key]}\n")

rendered_text = "".join(rendered_lines)
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
backup_path = env_path.with_name(env_path.name + f".bak_{timestamp}")
shutil.copy2(env_path, backup_path)
env_path.write_text(rendered_text, encoding="utf-8")

print(f"updated: {env_path}")
print(f"backup: {backup_path}")
for key in TARGET_KEYS:
    print(f"{key}={values[key]}")
PY
