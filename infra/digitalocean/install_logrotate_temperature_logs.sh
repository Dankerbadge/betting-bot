#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-/etc/betbot/temperature-shadow.env}"
ROTATE_FILE="/etc/logrotate.d/betbot-temperature"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

: "${OUTPUT_DIR:?OUTPUT_DIR is required in $ENV_FILE}"

LOG_DIR="${BETBOT_LOG_DIR:-$OUTPUT_DIR/logs}"
ROTATE_SIZE="${BETBOT_LOG_ROTATE_SIZE:-100M}"
ROTATE_COUNT="${BETBOT_LOG_ROTATE_COUNT:-14}"
ROTATE_DAILY="${BETBOT_LOG_ROTATE_DAILY:-1}"
LOGROTATE_FORCE_NOW="${BETBOT_LOGROTATE_FORCE_NOW:-0}"
LOGROTATE_SU_USER="${BETBOT_LOGROTATE_SU_USER:-root}"
LOGROTATE_SU_GROUP="${BETBOT_LOGROTATE_SU_GROUP:-root}"

mkdir -p "$LOG_DIR"

log_owner="betbot"
log_group="betbot"
if command -v stat >/dev/null 2>&1; then
  detected_owner="$(stat -c '%U' "$LOG_DIR" 2>/dev/null || true)"
  detected_group="$(stat -c '%G' "$LOG_DIR" 2>/dev/null || true)"
  if [[ -n "$detected_owner" && "$detected_owner" != "UNKNOWN" ]]; then
    log_owner="$detected_owner"
  fi
  if [[ -n "$detected_group" && "$detected_group" != "UNKNOWN" ]]; then
    log_group="$detected_group"
  fi
fi

if [[ "$ROTATE_DAILY" == "1" ]]; then
  cadence_line="daily"
else
  cadence_line="# daily disabled (size-based rotation still active)"
fi

if [[ -z "$LOGROTATE_SU_USER" ]]; then
  LOGROTATE_SU_USER="root"
fi
if [[ -z "$LOGROTATE_SU_GROUP" ]]; then
  LOGROTATE_SU_GROUP="root"
fi
su_line="su $LOGROTATE_SU_USER $LOGROTATE_SU_GROUP"

sudo tee "$ROTATE_FILE" >/dev/null <<EOF
$LOG_DIR/*.log {
  $su_line
  $cadence_line
  size $ROTATE_SIZE
  rotate $ROTATE_COUNT
  missingok
  notifempty
  compress
  delaycompress
  copytruncate
  create 0640 $log_owner $log_group
  dateext
  # Include clock time so multiple rotations in the same day do not collide.
  dateformat -%Y%m%d-%H%M%S
}
EOF

echo "Installed logrotate rule: $ROTATE_FILE"
echo "  log_dir=$LOG_DIR"
echo "  size=$ROTATE_SIZE"
echo "  rotate=$ROTATE_COUNT"
echo "  owner_group=$log_owner:$log_group"
echo "  su=$LOGROTATE_SU_USER:$LOGROTATE_SU_GROUP"

if [[ "$LOGROTATE_FORCE_NOW" == "1" ]]; then
  sudo logrotate -f "$ROTATE_FILE"
  echo "Forced one-time logrotate run completed."
fi

echo
echo "Current log directory usage:"
du -sh "$LOG_DIR" || true
echo
echo "Largest log files:"
find "$LOG_DIR" -maxdepth 1 -type f -name '*.log*' -printf '%10s %p\n' 2>/dev/null | sort -nr | head -n 15 || true
