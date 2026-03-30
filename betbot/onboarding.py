from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path


@dataclass(frozen=True)
class OnboardingCheck:
    component: str
    key: str
    ok: bool
    message: str


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise ValueError(f"Env file not found: {path}")

    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _is_placeholder(value: str | None) -> bool:
    raw = (value or "").strip()
    if raw == "":
        return True
    return raw.upper().startswith("TODO")


def run_onboarding_check(
    *,
    env_file: str,
    output_dir: str = "outputs",
) -> dict:
    env_path = Path(env_file)
    data = _parse_env_file(env_path)
    checks: list[OnboardingCheck] = []

    kalshi_access_key_id = data.get("KALSHI_ACCESS_KEY_ID")
    kalshi_key_path = data.get("KALSHI_PRIVATE_KEY_PATH")
    kalshi_env = data.get("KALSHI_ENV")

    checks.append(
        OnboardingCheck(
            component="kalshi",
            key="KALSHI_ACCESS_KEY_ID",
            ok=not _is_placeholder(kalshi_access_key_id),
            message="Present"
            if not _is_placeholder(kalshi_access_key_id)
            else "Missing or TODO placeholder",
        )
    )
    key_path_ok = not _is_placeholder(kalshi_key_path)
    if key_path_ok:
        key_path_ok = Path(kalshi_key_path).exists()
    checks.append(
        OnboardingCheck(
            component="kalshi",
            key="KALSHI_PRIVATE_KEY_PATH",
            ok=key_path_ok,
            message="Private key file found" if key_path_ok else "Private key path missing or file not found",
        )
    )
    checks.append(
        OnboardingCheck(
            component="kalshi",
            key="KALSHI_ENV",
            ok=(kalshi_env or "").strip().lower() in {"demo", "prod", "production"},
            message="Valid environment value"
            if (kalshi_env or "").strip().lower() in {"demo", "prod", "production"}
            else "Expected demo/prod/production",
        )
    )

    provider = (data.get("ODDS_PROVIDER") or "therundown").strip().lower()
    checks.append(
        OnboardingCheck(
            component="odds_provider",
            key="ODDS_PROVIDER",
            ok=provider in {"therundown", "opticodds"},
            message="Supported provider selected"
            if provider in {"therundown", "opticodds"}
            else "Expected 'therundown' or 'opticodds'",
        )
    )

    if provider == "therundown":
        tr_api_key = data.get("THERUNDOWN_API_KEY")
        tr_base_url = data.get("THERUNDOWN_BASE_URL")
        checks.append(
            OnboardingCheck(
                component="therundown",
                key="THERUNDOWN_API_KEY",
                ok=not _is_placeholder(tr_api_key),
                message="Present"
                if not _is_placeholder(tr_api_key)
                else "Missing or TODO placeholder (get API key from TheRundown dashboard)",
            )
        )
        tr_url_ok = not _is_placeholder(tr_base_url) and (tr_base_url or "").startswith("http")
        checks.append(
            OnboardingCheck(
                component="therundown",
                key="THERUNDOWN_BASE_URL",
                ok=tr_url_ok,
                message="Valid URL" if tr_url_ok else "Missing or invalid URL",
            )
        )
    else:
        optic_api_key = data.get("OPTICODDS_API_KEY")
        optic_base_url = data.get("OPTICODDS_BASE_URL")
        checks.append(
            OnboardingCheck(
                component="opticodds",
                key="OPTICODDS_API_KEY",
                ok=not _is_placeholder(optic_api_key),
                message="Present"
                if not _is_placeholder(optic_api_key)
                else "Missing or TODO placeholder",
            )
        )
        base_url_ok = not _is_placeholder(optic_base_url) and (optic_base_url or "").startswith("http")
        checks.append(
            OnboardingCheck(
                component="opticodds",
                key="OPTICODDS_BASE_URL",
                ok=base_url_ok,
                message="Valid URL" if base_url_ok else "Missing or invalid URL",
            )
        )

    timezone = data.get("BETBOT_TIMEZONE")
    jurisdiction = data.get("BETBOT_JURISDICTION")
    checks.append(
        OnboardingCheck(
            component="runtime",
            key="BETBOT_TIMEZONE",
            ok=not _is_placeholder(timezone),
            message="Present" if not _is_placeholder(timezone) else "Missing or TODO placeholder",
        )
    )
    checks.append(
        OnboardingCheck(
            component="runtime",
            key="BETBOT_JURISDICTION",
            ok=not _is_placeholder(jurisdiction),
            message="Present" if not _is_placeholder(jurisdiction) else "Missing or TODO placeholder",
        )
    )

    failed = [c for c in checks if not c.ok]
    summary = {
        "env_file": str(env_path),
        "status": "ready" if not failed else "blocked",
        "checks_total": len(checks),
        "checks_failed": len(failed),
        "failed": [
            {
                "component": c.component,
                "key": c.key,
                "message": c.message,
            }
            for c in failed
        ],
        "checks": [
            {
                "component": c.component,
                "key": c.key,
                "ok": c.ok,
                "message": c.message,
            }
            for c in checks
        ],
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"onboarding_check_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
