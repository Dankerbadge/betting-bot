#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import ipaddress
import os
import socket
from pathlib import Path
import subprocess
import sys
from typing import Any
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from betbot.dns_guard import resolve_host_with_public_dns
from betbot.live_smoke import kalshi_api_root_candidates
from betbot.onboarding import _is_placeholder as _onboarding_is_placeholder


PLACEHOLDER_SUBSTRINGS = (
    "todo",
    "your-new-",
    "replace-me",
    "replace_with",
    "changeme",
)

DNS_CACHE_PATH = Path(
    os.getenv(
        "BETBOT_AUTOMATION_PREFLIGHT_DNS_CACHE_FILE",
        str(REPO_ROOT / "outputs" / "automation_preflight_dns_cache.json"),
    )
).expanduser()
DNS_CACHE_TTL_SECONDS = max(
    0.0,
    float(os.getenv("BETBOT_AUTOMATION_PREFLIGHT_DNS_CACHE_TTL_SECONDS", str(12 * 60 * 60))),
)

WEATHER_HOSTS = (
    "api.weather.gov",
    "www.ncei.noaa.gov",
    "storage.googleapis.com",
    "noaa-mrms-pds.s3.amazonaws.com",
    "noaa-nbm-grib2-pds.s3.amazonaws.com",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_value(raw: str) -> str:
    text = str(raw or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"\"", "'"}:
        return text[1:-1].strip()
    return text


def _load_key_value_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("export "):
            raw = raw[len("export ") :].strip()
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        values[key.strip()] = _clean_value(value)
    return values


def _is_placeholder(value: str | None) -> bool:
    text = _clean_value(str(value or ""))
    if not text:
        return True
    if bool(_onboarding_is_placeholder(text)):
        return True
    lowered = text.lower()
    return any(token in lowered for token in PLACEHOLDER_SUBSTRINGS)


def _host_from_url(url: str) -> str | None:
    text = _clean_value(url)
    if not text:
        return None
    parsed = urlparse(text)
    host = (parsed.hostname or "").strip().lower()
    return host or None


def _load_dns_cache() -> dict[str, Any]:
    try:
        payload = json.loads(DNS_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"hosts": {}}
    if not isinstance(payload, dict):
        return {"hosts": {}}
    hosts = payload.get("hosts")
    if not isinstance(hosts, dict):
        payload["hosts"] = {}
    return payload


def _store_dns_cache(host: str, addresses: list[str]) -> None:
    host_key = str(host or "").strip().lower()
    if not host_key:
        return
    unique_ips = _extract_ip_tokens(" ".join(addresses))
    if not unique_ips:
        return
    payload = _load_dns_cache()
    hosts = payload.get("hosts")
    if not isinstance(hosts, dict):
        hosts = {}
        payload["hosts"] = hosts
    hosts[host_key] = {
        "ips": unique_ips,
        "updated_at_epoch": datetime.now(timezone.utc).timestamp(),
    }
    DNS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = DNS_CACHE_PATH.with_suffix(DNS_CACHE_PATH.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(DNS_CACHE_PATH)


def _load_cached_dns_ips(host: str) -> tuple[str, ...]:
    host_key = str(host or "").strip().lower()
    if not host_key:
        return ()
    payload = _load_dns_cache()
    hosts = payload.get("hosts")
    if not isinstance(hosts, dict):
        return ()
    row = hosts.get(host_key)
    if not isinstance(row, dict):
        return ()
    try:
        updated_at_epoch = float(row.get("updated_at_epoch") or 0.0)
    except (TypeError, ValueError):
        updated_at_epoch = 0.0
    if DNS_CACHE_TTL_SECONDS > 0 and updated_at_epoch > 0:
        age_seconds = max(0.0, datetime.now(timezone.utc).timestamp() - updated_at_epoch)
        if age_seconds > DNS_CACHE_TTL_SECONDS:
            return ()
    ips = _extract_ip_tokens(" ".join(str(item) for item in (row.get("ips") or [])))
    return tuple(ips)


def _extract_ip_tokens(text: str) -> list[str]:
    ips: list[str] = []
    for token in str(text or "").replace(",", " ").split():
        candidate = token.strip().strip("[]()")
        if not candidate:
            continue
        try:
            normalized = str(ipaddress.ip_address(candidate))
        except ValueError:
            continue
        if normalized not in ips:
            ips.append(normalized)
    return ips


def _resolve_host_with_cli_dns(host: str, timeout_seconds: float) -> tuple[str, ...]:
    commands = (
        ["dig", "+short", host, "A"],
        ["dig", "+short", "@1.1.1.1", host, "A"],
        ["nslookup", host, "1.1.1.1"],
    )
    resolved: list[str] = []

    for command in commands:
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=max(0.2, float(timeout_seconds)),
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            continue
        combined = f"{completed.stdout or ''}\n{completed.stderr or ''}"
        for ip_value in _extract_ip_tokens(combined):
            if ip_value not in resolved:
                resolved.append(ip_value)
        if resolved:
            break
    return tuple(resolved)


def _dns_check_host(host: str, timeout_seconds: float) -> dict[str, Any]:
    result: dict[str, Any] = {
        "host": host,
        "status": "failed",
        "system_ips": [],
        "public_dns_ips": [],
        "cli_dns_ips": [],
        "cached_dns_ips": [],
        "error": None,
    }

    try:
        infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        system_ips: list[str] = []
        for info in infos:
            ip = str(info[4][0] or "").strip()
            if ip and ip not in system_ips:
                system_ips.append(ip)
        if system_ips:
            result["status"] = "system_ok"
            result["system_ips"] = system_ips
            _store_dns_cache(host, system_ips)
            return result
    except OSError as exc:
        result["error"] = str(exc)

    public_ips = list(resolve_host_with_public_dns(host, timeout_seconds=max(0.2, float(timeout_seconds))))
    if public_ips:
        result["status"] = "recovered_public_dns"
        result["public_dns_ips"] = public_ips
        result["error"] = None
        _store_dns_cache(host, public_ips)
        return result

    cli_ips = list(_resolve_host_with_cli_dns(host, timeout_seconds=max(0.2, float(timeout_seconds))))
    if cli_ips:
        result["status"] = "recovered_cli_dns"
        result["cli_dns_ips"] = cli_ips
        result["error"] = None
        _store_dns_cache(host, cli_ips)
        return result

    cached_ips = list(_load_cached_dns_ips(host))
    if cached_ips:
        result["status"] = "recovered_cached_dns"
        result["cached_dns_ips"] = cached_ips
        result["error"] = None
    return result


def _resolve_trading_env_file(*, requested: Path, repo_root: Path) -> tuple[Path, str]:
    requested = requested.expanduser()
    local_candidate = repo_root / "data" / "research" / "account_onboarding.local.env"

    if requested.exists():
        try:
            requested_data = _load_key_value_file(requested)
        except Exception:
            requested_data = {}
        if not _is_placeholder(requested_data.get("KALSHI_ACCESS_KEY_ID")) and not _is_placeholder(
            requested_data.get("KALSHI_PRIVATE_KEY_PATH")
        ):
            return requested, "requested"

    if local_candidate != requested and local_candidate.exists():
        try:
            local_data = _load_key_value_file(local_candidate)
        except Exception:
            local_data = {}
        if not _is_placeholder(local_data.get("KALSHI_ACCESS_KEY_ID")) and not _is_placeholder(
            local_data.get("KALSHI_PRIVATE_KEY_PATH")
        ):
            return local_candidate, "auto_local_override"

    return requested, "requested_unready"


def _key_reference(*, key: str, required: bool, present: bool, source: str, note: str | None = None) -> dict[str, Any]:
    payload = {
        "key": key,
        "required": required,
        "present": present,
        "source": source,
    }
    if note:
        payload["note"] = note
    return payload


def _run_trading_profile(*, profile: str, repo_root: Path, env_file: Path, timeout_seconds: float) -> dict[str, Any]:
    effective_env_file, env_resolution = _resolve_trading_env_file(requested=env_file, repo_root=repo_root)
    errors: list[str] = []
    warnings: list[str] = []
    key_references: list[dict[str, Any]] = []

    try:
        data = _load_key_value_file(effective_env_file)
    except Exception as exc:
        return {
            "profile": profile,
            "status": "blocked",
            "checked_at_utc": _now_iso(),
            "env_file_requested": str(env_file),
            "env_file_effective": str(effective_env_file),
            "env_resolution": env_resolution,
            "errors": [f"env_file_load_failed:{exc}"],
            "warnings": [],
            "key_references": [],
            "dns_checks": [],
            "hosts_checked": [],
            "hosts_failed": [],
        }

    def require_key(key: str, *, source: str = "env_file") -> str:
        value = _clean_value(data.get(key, ""))
        present = not _is_placeholder(value)
        key_references.append(_key_reference(key=key, required=True, present=present, source=source))
        if not present:
            errors.append(f"missing_or_placeholder:{key}")
        return value

    def optional_key(key: str, *, note: str | None = None, source: str = "env_file") -> str:
        value = _clean_value(data.get(key, ""))
        present = not _is_placeholder(value)
        key_references.append(_key_reference(key=key, required=False, present=present, source=source, note=note))
        return value

    _ = require_key("KALSHI_ACCESS_KEY_ID")
    kalshi_private_key = require_key("KALSHI_PRIVATE_KEY_PATH")
    kalshi_env = require_key("KALSHI_ENV")

    kalshi_env_normalized = kalshi_env.strip().lower()
    if kalshi_env_normalized not in {"demo", "prod", "production"}:
        errors.append(f"invalid_value:KALSHI_ENV={kalshi_env}")

    private_key_path = Path(kalshi_private_key).expanduser() if kalshi_private_key else Path(".")
    if kalshi_private_key:
        if not private_key_path.is_absolute():
            private_key_path = (effective_env_file.parent / private_key_path).resolve()
        if not private_key_path.exists():
            errors.append(f"missing_private_key_file:{private_key_path}")
        else:
            key_references.append(
                _key_reference(
                    key="KALSHI_PRIVATE_KEY_FILE",
                    required=True,
                    present=True,
                    source="filesystem",
                    note=str(private_key_path),
                )
            )

    provider = optional_key("ODDS_PROVIDER", note="defaults to therundown when omitted")
    provider_normalized = provider.strip().lower() or "therundown"
    if provider_normalized not in {"therundown", "opticodds"}:
        errors.append(f"invalid_value:ODDS_PROVIDER={provider}")

    hosts: list[str] = []
    try:
        for root in kalshi_api_root_candidates(kalshi_env_normalized or "prod"):
            host = _host_from_url(root)
            if host and host not in hosts:
                hosts.append(host)
    except Exception:
        for fallback in (
            "https://api.elections.kalshi.com/trade-api/v2",
            "https://trading-api.kalshi.com/trade-api/v2",
            "https://demo-api.kalshi.co/trade-api/v2",
        ):
            host = _host_from_url(fallback)
            if host and host not in hosts:
                hosts.append(host)

    if provider_normalized == "therundown":
        therundown_key = require_key("THERUNDOWN_API_KEY")
        therundown_base_url = require_key("THERUNDOWN_BASE_URL")
        _ = therundown_key
        odds_host = _host_from_url(therundown_base_url)
        if odds_host:
            hosts.append(odds_host)
        else:
            errors.append("invalid_url:THERUNDOWN_BASE_URL")
        optional_key(
            "THERUNDOWN_LOGIN_EMAIL",
            note="template-only credential; currently not required by automation runtime",
        )
        optional_key(
            "THERUNDOWN_LOGIN_PASSWORD",
            note="template-only credential; currently not required by automation runtime",
        )
    elif provider_normalized == "opticodds":
        opticodds_key = require_key("OPTICODDS_API_KEY")
        opticodds_base_url = require_key("OPTICODDS_BASE_URL")
        _ = opticodds_key
        odds_host = _host_from_url(opticodds_base_url)
        if odds_host:
            hosts.append(odds_host)
        else:
            errors.append("invalid_url:OPTICODDS_BASE_URL")

    weather_token = optional_key(
        "BETBOT_NOAA_CDO_TOKEN",
        note="optional direct token; token file fallback is supported",
    )
    if _is_placeholder(weather_token):
        optional_key("NOAA_CDO_TOKEN", note="optional alias")
        optional_key("NCEI_CDO_TOKEN", note="optional alias")
    optional_key(
        "BETBOT_WEATHER_CDO_TOKEN_FILE",
        note="optional token file path fallback",
        source="env_file_or_shell",
    )

    for host in WEATHER_HOSTS:
        if host not in hosts:
            hosts.append(host)

    dns_checks = [_dns_check_host(host, timeout_seconds) for host in hosts]
    hosts_failed = [check["host"] for check in dns_checks if check.get("status") == "failed"]
    if hosts_failed:
        errors.append("dns_unresolved_hosts:" + ",".join(hosts_failed))

    if env_resolution != "requested":
        warnings.append(f"env_file_auto_resolution:{env_resolution}")

    return {
        "profile": profile,
        "status": "ready" if not errors else "blocked",
        "checked_at_utc": _now_iso(),
        "env_file_requested": str(env_file),
        "env_file_effective": str(effective_env_file),
        "env_resolution": env_resolution,
        "errors": errors,
        "warnings": warnings,
        "key_references": key_references,
        "dns_checks": dns_checks,
        "hosts_checked": hosts,
        "hosts_failed": hosts_failed,
    }


def _run_supabase_sync_profile(*, repo_root: Path, secrets_file: Path, timeout_seconds: float) -> dict[str, Any]:
    _ = repo_root
    errors: list[str] = []
    warnings: list[str] = []
    key_references: list[dict[str, Any]] = []

    try:
        data = _load_key_value_file(secrets_file)
    except Exception as exc:
        return {
            "profile": "supabase_sync",
            "status": "blocked",
            "checked_at_utc": _now_iso(),
            "secrets_file": str(secrets_file),
            "errors": [f"secrets_file_load_failed:{exc}"],
            "warnings": [],
            "key_references": [],
            "dns_checks": [],
            "hosts_checked": [],
            "hosts_failed": [],
        }

    def require_key(key: str) -> str:
        value = _clean_value(data.get(key, ""))
        present = not _is_placeholder(value)
        key_references.append(_key_reference(key=key, required=True, present=present, source="secrets_file"))
        if not present:
            errors.append(f"missing_or_placeholder:{key}")
        return value

    def optional_key(key: str, *, note: str | None = None) -> str:
        value = _clean_value(data.get(key, ""))
        present = not _is_placeholder(value)
        key_references.append(_key_reference(key=key, required=False, present=present, source="secrets_file", note=note))
        return value

    supabase_url = require_key("OPSBOT_SUPABASE_URL")
    service_role_key = require_key("OPSBOT_SUPABASE_SERVICE_ROLE_KEY")
    project_ref = require_key("OPSBOT_SUPABASE_PROJECT_REF")
    optional_key(
        "OPSBOT_SUPABASE_ANON_KEY",
        note="not required for DB sync ingestion; required for dashboard runtime",
    )
    forbidden_hint = optional_key(
        "OPSBOT_FORBIDDEN_PROJECT_HINT",
        note="defaults to 'legacy_external_project' when omitted",
    )

    if service_role_key and len(service_role_key) < 20:
        errors.append("invalid_value:OPSBOT_SUPABASE_SERVICE_ROLE_KEY_too_short")

    host = _host_from_url(supabase_url)
    hosts = [host] if host else []
    if not host:
        errors.append("invalid_url:OPSBOT_SUPABASE_URL")
    else:
        host_project_ref = host.split(".")[0]
        if project_ref and host_project_ref and project_ref != host_project_ref:
            errors.append(
                "supabase_url_project_ref_mismatch:"
                f"url_ref={host_project_ref},project_ref={project_ref}"
            )

    effective_forbidden_hint = forbidden_hint or "legacy_external_project"
    if effective_forbidden_hint and (
        effective_forbidden_hint.lower() in supabase_url.lower()
        or effective_forbidden_hint.lower() in project_ref.lower()
    ):
        errors.append(f"forbidden_project_hint_detected:{effective_forbidden_hint}")

    dns_checks = [_dns_check_host(entry, timeout_seconds) for entry in hosts if entry]
    hosts_failed = [check["host"] for check in dns_checks if check.get("status") == "failed"]
    if hosts_failed:
        errors.append("dns_unresolved_hosts:" + ",".join(hosts_failed))

    return {
        "profile": "supabase_sync",
        "status": "ready" if not errors else "blocked",
        "checked_at_utc": _now_iso(),
        "secrets_file": str(secrets_file),
        "errors": errors,
        "warnings": warnings,
        "key_references": key_references,
        "dns_checks": dns_checks,
        "hosts_checked": hosts,
        "hosts_failed": hosts_failed,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Automation preflight: secret and DNS readiness checks")
    parser.add_argument(
        "--profile",
        required=True,
        choices=("hourly", "monthly", "supabase_sync"),
        help="Automation profile to validate",
    )
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root",
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Path to trading env file (for hourly/monthly profiles)",
    )
    parser.add_argument(
        "--secrets-file",
        default="",
        help="Path to Supabase secrets env file (for supabase_sync profile)",
    )
    parser.add_argument(
        "--dns-timeout-seconds",
        type=float,
        default=2.0,
        help="Timeout seconds for each DNS fallback query",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional output file path for summary JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()

    if args.profile in {"hourly", "monthly"}:
        default_env = (
            repo_root / "data" / "research" / "account_onboarding.env.template"
            if args.profile == "hourly"
            else repo_root / "data" / "research" / "account_onboarding.local.env"
        )
        env_file = Path(args.env_file).expanduser().resolve() if args.env_file else default_env
        summary = _run_trading_profile(
            profile=args.profile,
            repo_root=repo_root,
            env_file=env_file,
            timeout_seconds=args.dns_timeout_seconds,
        )
    else:
        default_secrets = Path.home() / ".codex" / "secrets" / "betting-bot-supabase.env"
        secrets_file = Path(args.secrets_file).expanduser().resolve() if args.secrets_file else default_secrets
        summary = _run_supabase_sync_profile(
            repo_root=repo_root,
            secrets_file=secrets_file,
            timeout_seconds=args.dns_timeout_seconds,
        )

    if args.output_json:
        output_path = Path(args.output_json).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        summary["output_file"] = str(output_path)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary.get("status") == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
