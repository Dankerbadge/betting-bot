#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
ENV_FILE="/etc/betbot/temperature-shadow.env"
SETTING_VALUE="1"
RUN_STRICT_CHECKS=1
POSITIONAL_ENV_SET=0

usage() {
  cat <<'EOF'
usage:
  set_coldmath_recovery_env_persistence_gate.sh [--enable|--disable] [--skip-checks] [--run-checks] [/etc/betbot/temperature-shadow.env]

behavior:
  - upserts both keys in the env file:
      COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR
      RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY
  - creates a timestamped backup next to the env file before writing
  - runs strict health checks by default:
      check_temperature_shadow.sh --strict --env <env>
      check_temperature_shadow_quick.sh --strict --env <env>

flags:
  --enable       set value to 1 (default)
  --disable      set value to 0
  --skip-checks  update env only
  --run-checks   force strict checks on
  -h, --help     show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --enable)
      SETTING_VALUE="1"
      shift
      ;;
    --disable)
      SETTING_VALUE="0"
      shift
      ;;
    --skip-checks)
      RUN_STRICT_CHECKS=0
      shift
      ;;
    --run-checks)
      RUN_STRICT_CHECKS=1
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

if [[ ! -f "$ENV_FILE" ]]; then
  echo "missing env file: $ENV_FILE" >&2
  exit 1
fi
if [[ ! -r "$ENV_FILE" ]]; then
  echo "env file is not readable: $ENV_FILE" >&2
  exit 1
fi

update_cmd=(python3 - "$ENV_FILE" "$SETTING_VALUE")
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
target_keys = [
    "COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR",
    "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY",
]
target_value = str(sys.argv[2] or "").strip()

for key in target_keys:
    if not KEY_RE.fullmatch(key):
        raise SystemExit(f"invalid env key: {key!r}")
if target_value not in {"0", "1"}:
    raise SystemExit(f"invalid env value for strict gate keys: {target_value!r}")

original_text = env_path.read_text(encoding="utf-8")
original_norm = original_text if (original_text.endswith("\n") or original_text == "") else original_text + "\n"
lines = original_norm.splitlines(keepends=True)

seen_targets: dict[str, bool] = {key: False for key in target_keys}
rendered_lines: list[str] = []
for line in lines:
    key = _parse_assignment_key(line)
    if key in seen_targets:
        if not seen_targets[key]:
            rendered_lines.append(f"{key}={target_value}\n")
            seen_targets[key] = True
        continue
    rendered_lines.append(line)

missing_keys = [key for key in target_keys if not seen_targets[key]]
if missing_keys:
    if rendered_lines and rendered_lines[-1].strip():
        rendered_lines.append("\n")
    for key in missing_keys:
        rendered_lines.append(f"{key}={target_value}\n")

rendered_text = "".join(rendered_lines)
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
backup_path = env_path.with_name(env_path.name + f".bak_{timestamp}")
shutil.copy2(env_path, backup_path)
env_path.write_text(rendered_text, encoding="utf-8")

print(f"updated: {env_path}")
print(f"backup: {backup_path}")
for key in target_keys:
    print(f"{key}={target_value}")
PY

if (( RUN_STRICT_CHECKS == 0 )); then
  echo "strict checks skipped (--skip-checks)"
  exit 0
fi

shadow_check_script="$SCRIPT_DIR/check_temperature_shadow.sh"
quick_check_script="$SCRIPT_DIR/check_temperature_shadow_quick.sh"
if [[ ! -f "$shadow_check_script" ]]; then
  echo "missing script: $shadow_check_script" >&2
  exit 1
fi
if [[ ! -f "$quick_check_script" ]]; then
  echo "missing script: $quick_check_script" >&2
  exit 1
fi

echo "running strict checks against: $ENV_FILE"
strict_failed=0
if ! bash "$shadow_check_script" --strict --env "$ENV_FILE"; then
  strict_failed=1
fi
if ! bash "$quick_check_script" --strict --env "$ENV_FILE"; then
  strict_failed=1
fi

if (( strict_failed == 1 )); then
  echo "strict checks failed after env update" >&2
  exit 2
fi

echo "strict checks passed"
