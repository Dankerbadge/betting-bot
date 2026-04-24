#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
STRICT_FAIL_KEY="COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR"
REQUIRE_SUMMARY_KEY="RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY"
RECOVERY_GATE_REMEDIATION_SCRIPT="$REPO_DIR/infra/digitalocean/set_coldmath_recovery_env_persistence_gate.sh"

failures=0
warnings=0

pass() {
  echo "PASS: $*"
}

warn() {
  warnings=$((warnings + 1))
  echo "WARN: $*" >&2
}

fail() {
  failures=$((failures + 1))
  echo "FAIL: $*" >&2
}

normalize_gate_toggle() {
  local raw="${1:-}"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]')"
  case "$lowered" in
    1|true|yes|on)
      echo "enabled"
      ;;
    0|false|no|off)
      echo "disabled"
      ;;
    *)
      echo "invalid"
      ;;
  esac
}

require_file() {
  local path="$1"
  local label="$2"
  if [[ -f "$path" ]]; then
    pass "$label exists: $path"
  else
    fail "$label missing: $path"
  fi
}

require_dir() {
  local path="$1"
  local label="$2"
  if [[ -d "$path" ]]; then
    pass "$label exists: $path"
  else
    fail "$label missing: $path"
  fi
}

check_url() {
  local url="$1"
  local label="$2"
  if curl -fsSIL --max-time 15 "$url" >/dev/null 2>&1; then
    pass "$label reachable: $url"
  else
    warn "$label unreachable right now: $url"
  fi
}

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing runtime env file: $ENV_FILE" >&2
  exit 1
fi
pass "env file exists: $ENV_FILE"

# shellcheck disable=SC1090
source "$ENV_FILE"

required_vars=(
  BETBOT_ROOT
  OUTPUT_DIR
  BETBOT_ENV_FILE
)
for key in "${required_vars[@]}"; do
  if [[ -z "${!key:-}" ]]; then
    fail "$key is required and empty in $ENV_FILE"
  else
    pass "$key set"
  fi
done

strict_gate_raw=""
strict_gate_state="missing"
if [[ -n "${COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR+x}" ]]; then
  strict_gate_raw="${COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR:-}"
  strict_gate_state="$(normalize_gate_toggle "$strict_gate_raw")"
fi

effectiveness_gate_raw=""
effectiveness_gate_state="missing"
if [[ -n "${RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY+x}" ]]; then
  effectiveness_gate_raw="${RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY:-}"
  effectiveness_gate_state="$(normalize_gate_toggle "$effectiveness_gate_raw")"
fi

if [[ "$strict_gate_state" == "enabled" && "$effectiveness_gate_state" == "enabled" ]]; then
  pass "strict recovery gates enabled: $STRICT_FAIL_KEY and $REQUIRE_SUMMARY_KEY"
else
  if [[ "$strict_gate_state" == "missing" ]]; then
    warn "$STRICT_FAIL_KEY is missing in $ENV_FILE (treated as disabled)"
  elif [[ "$strict_gate_state" == "disabled" ]]; then
    warn "$STRICT_FAIL_KEY is disabled in $ENV_FILE (value='$strict_gate_raw')"
  elif [[ "$strict_gate_state" == "invalid" ]]; then
    warn "$STRICT_FAIL_KEY has invalid value '$strict_gate_raw' in $ENV_FILE (treated as disabled)"
  fi

  if [[ "$effectiveness_gate_state" == "missing" ]]; then
    warn "$REQUIRE_SUMMARY_KEY is missing in $ENV_FILE (treated as disabled)"
  elif [[ "$effectiveness_gate_state" == "disabled" ]]; then
    warn "$REQUIRE_SUMMARY_KEY is disabled in $ENV_FILE (value='$effectiveness_gate_raw')"
  elif [[ "$effectiveness_gate_state" == "invalid" ]]; then
    warn "$REQUIRE_SUMMARY_KEY has invalid value '$effectiveness_gate_raw' in $ENV_FILE (treated as disabled)"
  fi

  warn "strict recovery gate remediation: bash $RECOVERY_GATE_REMEDIATION_SCRIPT --enable $ENV_FILE"
fi

BETBOT_ROOT="${BETBOT_ROOT:-}"
OUTPUT_DIR="${OUTPUT_DIR:-}"
BETBOT_ENV_FILE="${BETBOT_ENV_FILE:-}"
METAR_AGE_POLICY_JSON="${METAR_AGE_POLICY_JSON:-}"
SPECI_CALIBRATION_JSON="${SPECI_CALIBRATION_JSON:-}"

if [[ -n "$BETBOT_ROOT" ]]; then
  require_dir "$BETBOT_ROOT" "BETBOT_ROOT"
  require_dir "$BETBOT_ROOT/betbot" "betbot package directory"
  require_file "$BETBOT_ROOT/infra/digitalocean/run_temperature_shadow_loop.sh" "loop runner script"
  ALPHA_WORKER_ENABLED="${ALPHA_WORKER_ENABLED:-0}"
  if [[ "$ALPHA_WORKER_ENABLED" == "1" ]]; then
    require_file "$BETBOT_ROOT/infra/digitalocean/run_temperature_alpha_workers.sh" "alpha workers runner script"
    require_file "$BETBOT_ROOT/infra/digitalocean/build_metar_age_policy.py" "metar policy builder script"
  fi
  require_file "$BETBOT_ROOT/infra/digitalocean/run_temperature_coldmath_hardening.sh" "coldmath hardening runner script"
fi

if [[ -n "$OUTPUT_DIR" ]]; then
  mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/logs" 2>/dev/null || true
  if [[ -d "$OUTPUT_DIR" && -w "$OUTPUT_DIR" ]]; then
    pass "OUTPUT_DIR writable: $OUTPUT_DIR"
  else
    fail "OUTPUT_DIR is not writable: $OUTPUT_DIR"
  fi
fi

if [[ -n "$BETBOT_ENV_FILE" ]]; then
  require_file "$BETBOT_ENV_FILE" "BETBOT_ENV_FILE"
fi

if [[ -n "$METAR_AGE_POLICY_JSON" ]]; then
  require_file "$METAR_AGE_POLICY_JSON" "METAR_AGE_POLICY_JSON"
fi
if [[ -n "$SPECI_CALIBRATION_JSON" ]]; then
  require_file "$SPECI_CALIBRATION_JSON" "SPECI_CALIBRATION_JSON"
fi

PYTHON_BIN="${BETBOT_ROOT:-}/.venv/bin/python"
if [[ -x "$PYTHON_BIN" ]]; then
  pass "python venv binary exists: $PYTHON_BIN"
else
  fail "missing python venv binary: $PYTHON_BIN"
fi

RECOVERY_SCRIPT="$BETBOT_ROOT/infra/digitalocean/run_temperature_pipeline_recovery.sh"
if [[ -x "$RECOVERY_SCRIPT" ]]; then
  pass "pipeline recovery script is executable"
else
  warn "pipeline recovery script missing or not executable ($RECOVERY_SCRIPT)"
fi

RECOVERY_CHAOS_SCRIPT="$BETBOT_ROOT/infra/digitalocean/run_temperature_recovery_chaos_check.sh"
if [[ -x "$RECOVERY_CHAOS_SCRIPT" ]]; then
  pass "recovery chaos script is executable"
else
  warn "recovery chaos script missing or not executable ($RECOVERY_CHAOS_SCRIPT)"
fi

STALE_METRICS_DRILL_SCRIPT="$BETBOT_ROOT/infra/digitalocean/run_temperature_stale_metrics_drill.sh"
if [[ -x "$STALE_METRICS_DRILL_SCRIPT" ]]; then
  pass "stale metrics drill script is executable"
else
  warn "stale metrics drill script missing or not executable ($STALE_METRICS_DRILL_SCRIPT)"
fi

COLDMATH_HARDENING_SCRIPT="$BETBOT_ROOT/infra/digitalocean/run_temperature_coldmath_hardening.sh"
if [[ -x "$COLDMATH_HARDENING_SCRIPT" ]]; then
  pass "coldmath hardening script is executable"
else
  warn "coldmath hardening script missing or not executable ($COLDMATH_HARDENING_SCRIPT)"
fi

COLDMATH_HARDENING_ENABLED="${COLDMATH_HARDENING_ENABLED:-1}"
COLDMATH_WALLET_ADDRESS="${COLDMATH_WALLET_ADDRESS:-}"
if [[ "$COLDMATH_HARDENING_ENABLED" == "1" ]]; then
  if [[ -n "$COLDMATH_WALLET_ADDRESS" ]]; then
    pass "COLDMATH_WALLET_ADDRESS is set"
  else
    warn "COLDMATH_HARDENING_ENABLED=1 but COLDMATH_WALLET_ADDRESS is empty"
  fi
fi

if [[ -x "$PYTHON_BIN" ]]; then
  if "$PYTHON_BIN" -m betbot.cli --help >/dev/null 2>&1; then
    pass "betbot CLI import/execution works"
  else
    fail "betbot CLI failed to execute"
  fi
fi

ALLOW_LIVE_ORDERS="${ALLOW_LIVE_ORDERS:-0}"
if [[ "$ALLOW_LIVE_ORDERS" == "1" ]]; then
  warn "ALLOW_LIVE_ORDERS=1 (live enabled). For shadow bring-up this should be 0."
else
  pass "ALLOW_LIVE_ORDERS is shadow-safe ($ALLOW_LIVE_ORDERS)"
fi

ADAPTIVE_MAX_MARKETS_ENABLED="${ADAPTIVE_MAX_MARKETS_ENABLED:-1}"
if [[ "$ADAPTIVE_MAX_MARKETS_ENABLED" == "1" ]]; then
  pass "ADAPTIVE_MAX_MARKETS_ENABLED is on"
else
  warn "ADAPTIVE_MAX_MARKETS_ENABLED is off (fixed scan breadth may leave VM headroom unused)"
fi

check_url "https://aviationweather.gov/data/cache/metars.cache.csv.gz" "AviationWeather METAR cache"
check_url "https://api.weather.gov/" "NWS API"
check_url "https://www.ncei.noaa.gov/" "NCEI web"

SERVICE_FILE="/etc/systemd/system/betbot-temperature-shadow.service"
if [[ -f "$SERVICE_FILE" ]]; then
  pass "systemd service file present: $SERVICE_FILE"
  svc_enabled="$(systemctl is-enabled betbot-temperature-shadow 2>/dev/null || true)"
  svc_active="$(systemctl is-active betbot-temperature-shadow 2>/dev/null || true)"
  echo "INFO: service enabled=$svc_enabled active=$svc_active"
else
  warn "systemd service file not installed yet ($SERVICE_FILE)"
fi

RECOVERY_TIMER_FILE="/etc/systemd/system/betbot-temperature-recovery.timer"
if [[ -f "$RECOVERY_TIMER_FILE" ]]; then
  pass "recovery timer file present: $RECOVERY_TIMER_FILE"
  recovery_enabled="$(systemctl is-enabled betbot-temperature-recovery.timer 2>/dev/null || true)"
  recovery_active="$(systemctl is-active betbot-temperature-recovery.timer 2>/dev/null || true)"
  echo "INFO: recovery timer enabled=$recovery_enabled active=$recovery_active"
else
  warn "recovery timer not installed yet (recommended): $RECOVERY_TIMER_FILE"
fi

RECOVERY_CHAOS_TIMER_FILE="/etc/systemd/system/betbot-temperature-recovery-chaos.timer"
if [[ -f "$RECOVERY_CHAOS_TIMER_FILE" ]]; then
  pass "recovery chaos timer file present: $RECOVERY_CHAOS_TIMER_FILE"
  recovery_chaos_enabled="$(systemctl is-enabled betbot-temperature-recovery-chaos.timer 2>/dev/null || true)"
  recovery_chaos_active="$(systemctl is-active betbot-temperature-recovery-chaos.timer 2>/dev/null || true)"
  echo "INFO: recovery chaos timer enabled=$recovery_chaos_enabled active=$recovery_chaos_active"
else
  warn "recovery chaos timer not installed yet (recommended): $RECOVERY_CHAOS_TIMER_FILE"
fi

STALE_METRICS_DRILL_TIMER_FILE="/etc/systemd/system/betbot-temperature-stale-metrics-drill.timer"
if [[ -f "$STALE_METRICS_DRILL_TIMER_FILE" ]]; then
  pass "stale metrics drill timer file present: $STALE_METRICS_DRILL_TIMER_FILE"
  stale_drill_timer_enabled="$(systemctl is-enabled betbot-temperature-stale-metrics-drill.timer 2>/dev/null || true)"
  stale_drill_timer_active="$(systemctl is-active betbot-temperature-stale-metrics-drill.timer 2>/dev/null || true)"
  echo "INFO: stale metrics drill timer enabled=$stale_drill_timer_enabled active=$stale_drill_timer_active"
else
  warn "stale metrics drill timer not installed yet (recommended): $STALE_METRICS_DRILL_TIMER_FILE"
fi

COLDMATH_HARDENING_TIMER_FILE="/etc/systemd/system/betbot-temperature-coldmath-hardening.timer"
if [[ -f "$COLDMATH_HARDENING_TIMER_FILE" ]]; then
  pass "coldmath hardening timer file present: $COLDMATH_HARDENING_TIMER_FILE"
  coldmath_hardening_enabled="$(systemctl is-enabled betbot-temperature-coldmath-hardening.timer 2>/dev/null || true)"
  coldmath_hardening_active="$(systemctl is-active betbot-temperature-coldmath-hardening.timer 2>/dev/null || true)"
  echo "INFO: coldmath hardening timer enabled=$coldmath_hardening_enabled active=$coldmath_hardening_active"
else
  if [[ "$COLDMATH_HARDENING_ENABLED" == "1" ]]; then
    warn "coldmath hardening timer not installed yet (recommended): $COLDMATH_HARDENING_TIMER_FILE"
  fi
fi

echo
echo "Preflight summary: failures=$failures warnings=$warnings"
if (( failures > 0 )); then
  exit 1
fi
exit 0
