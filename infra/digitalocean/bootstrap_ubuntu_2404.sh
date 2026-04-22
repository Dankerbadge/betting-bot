#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
  echo "Run this as your non-root deploy user (with sudo), not as root." >&2
  exit 1
fi

REPO_DIR="${1:-$HOME/betting-bot}"

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

sudo mkdir -p /etc/betbot
sudo chmod 755 /etc/betbot

if [[ ! -f /etc/betbot/temperature-shadow.env ]]; then
  sudo cp infra/digitalocean/temperature-shadow.env.example /etc/betbot/temperature-shadow.env
  sudo chown root:root /etc/betbot/temperature-shadow.env
  sudo chmod 640 /etc/betbot/temperature-shadow.env
  sudo python3 - "$REPO_DIR" <<'PY'
from pathlib import Path
import sys

repo_dir = Path(sys.argv[1]).resolve().as_posix()
env_path = Path("/etc/betbot/temperature-shadow.env")

updates = {
    "BETBOT_ROOT": repo_dir,
    "OUTPUT_DIR": f"{repo_dir}/outputs/pilot_candidate_do",
    "BETBOT_ENV_FILE": "/etc/betbot/account_onboarding.local.env",
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
  echo "Created /etc/betbot/temperature-shadow.env from example. Edit before starting service."
fi

echo "Bootstrap complete for $REPO_DIR"
echo "Next:"
echo "  1) Copy credential env to /etc/betbot/account_onboarding.local.env (chmod 600)"
echo "  2) Edit /etc/betbot/temperature-shadow.env"
echo "  3) Run infra/digitalocean/preflight_temperature_shadow.sh"
echo "  4) Run infra/digitalocean/install_systemd_temperature_shadow.sh"
echo "  5) Optional: infra/digitalocean/install_systemd_temperature_reporting.sh"
echo "  6) Recommended: infra/digitalocean/install_systemd_temperature_recovery.sh"
echo "  7) Recommended: infra/digitalocean/install_systemd_temperature_recovery_chaos.sh"
echo "  8) Recommended for ColdMath pivot: infra/digitalocean/install_systemd_temperature_coldmath_hardening.sh"
