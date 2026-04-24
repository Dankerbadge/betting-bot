#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
  echo "Run this as your non-root deploy user (with sudo), not as root." >&2
  exit 1
fi

REPO_DIR="${1:-$HOME/betting-bot}"
BETBOT_ETC_DIR="${BETBOT_ETC_DIR:-/etc/betbot}"
TEMPERATURE_ENV_PATH="$BETBOT_ETC_DIR/temperature-shadow.env"
ACCOUNT_ENV_PATH="$BETBOT_ETC_DIR/account_onboarding.local.env"

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  git curl ca-certificates jq tmux htop \
  build-essential pkg-config \
  python3 python3-venv python3-pip

if [[ ! -d "$REPO_DIR/.git" ]]; then
  echo "Missing git repo at $REPO_DIR" >&2
  echo "Clone your repo first, e.g.:" >&2
  echo "  git clone <repo-url> \"$REPO_DIR\"" >&2
  exit 1
fi

cd "$REPO_DIR"
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip wheel
pip install -r requirements.txt
pip install .

sudo mkdir -p "$BETBOT_ETC_DIR"
sudo chmod 755 "$BETBOT_ETC_DIR"

if [[ ! -f "$TEMPERATURE_ENV_PATH" ]]; then
  sudo cp infra/digitalocean/temperature-shadow.env.example "$TEMPERATURE_ENV_PATH"
  sudo chown root:root "$TEMPERATURE_ENV_PATH"
  sudo chmod 640 "$TEMPERATURE_ENV_PATH"
  sudo python3 - "$REPO_DIR" "$TEMPERATURE_ENV_PATH" "$ACCOUNT_ENV_PATH" <<'PY'
from pathlib import Path
import sys

repo_dir = Path(sys.argv[1]).resolve().as_posix()
env_path = Path(sys.argv[2])
account_env_file = Path(sys.argv[3]).as_posix()

updates = {
    "BETBOT_ROOT": repo_dir,
    "OUTPUT_DIR": f"{repo_dir}/outputs/pilot_candidate_do",
    "BETBOT_ENV_FILE": account_env_file,
    "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY": "1",
}

lines = env_path.read_text(encoding="utf-8").splitlines()
new_lines = []
seen = set()
for line in lines:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        new_lines.append(line)
        continue
    key, _ = line.split("=", 1)
    key = key.strip()
    if key in updates:
        new_lines.append(f"{key}={updates[key]}")
        seen.add(key)
    else:
        new_lines.append(line)
for key, value in updates.items():
    if key not in seen:
        new_lines.append(f"{key}={value}")
env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
PY
  echo "Created $TEMPERATURE_ENV_PATH from example. Edit before starting service."
fi

echo "Bootstrap complete for $REPO_DIR"
echo "Next:"
echo "  1) Copy credential env to $ACCOUNT_ENV_PATH (chmod 600)"
echo "  2) Edit $TEMPERATURE_ENV_PATH"
echo "  3) Run infra/digitalocean/preflight_temperature_shadow.sh"
echo "  4) Run infra/digitalocean/install_systemd_temperature_shadow.sh"
echo "  5) Optional: infra/digitalocean/install_systemd_temperature_reporting.sh"
echo "  6) Recommended: infra/digitalocean/install_systemd_temperature_recovery.sh"
echo "  7) Recommended: infra/digitalocean/install_systemd_temperature_recovery_chaos.sh"
echo "  8) Recommended for ColdMath pivot: infra/digitalocean/install_systemd_temperature_coldmath_hardening.sh"
