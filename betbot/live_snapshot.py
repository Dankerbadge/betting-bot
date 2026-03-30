from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import time
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import urlencode, urlparse

from betbot.live_smoke import (
    HttpGetter,
    KalshiSigner,
    KALSHI_API_ROOTS,
    _http_get_json,
    _kalshi_sign_request,
    kalshi_api_root_candidates,
)
from betbot.onboarding import _is_placeholder, _parse_env_file

LIVE_SNAPSHOT_NETWORK_MAX_RETRIES = 2
LIVE_SNAPSHOT_NETWORK_BACKOFF_SECONDS = 0.75
LIVE_SNAPSHOT_RETRYABLE_STATUS_CODES = {401, 403, 500, 502, 503, 504}
LIVE_SNAPSHOT_STATUS_MAX_RETRIES = 2
LIVE_SNAPSHOT_STATUS_BACKOFF_SECONDS = 0.75


def _http_get_json_with_retries(
    *,
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    http_get_json: HttpGetter,
    retryable_status_codes: set[int] = LIVE_SNAPSHOT_RETRYABLE_STATUS_CODES,
    max_network_retries: int = LIVE_SNAPSHOT_NETWORK_MAX_RETRIES,
    network_backoff_seconds: float = LIVE_SNAPSHOT_NETWORK_BACKOFF_SECONDS,
    max_status_retries: int = LIVE_SNAPSHOT_STATUS_MAX_RETRIES,
    status_backoff_seconds: float = LIVE_SNAPSHOT_STATUS_BACKOFF_SECONDS,
    sleep_fn: Callable[[float], None] | None = None,
) -> tuple[int, Any]:
    effective_sleep = sleep_fn or time.sleep
    network_attempt = 0
    status_attempt = 0
    while True:
        try:
            status_code, payload = http_get_json(url, headers, timeout_seconds)
        except URLError:
            if network_attempt >= max(0, max_network_retries):
                raise
            effective_sleep(network_backoff_seconds * (2**network_attempt))
            network_attempt += 1
            continue
        if status_code in retryable_status_codes and status_attempt < max(0, max_status_retries):
            effective_sleep(status_backoff_seconds * (2**status_attempt))
            status_attempt += 1
            continue
        return status_code, payload


def _therundown_sports_url(base_url: str, api_key: str) -> str:
    query = urlencode({"key": api_key})
    return f"{base_url.rstrip('/')}/sports?{query}"


def _kalshi_balance_snapshot(
    *,
    access_key_id: str,
    private_key_path: str,
    env_name: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
    sign_request: KalshiSigner,
) -> dict[str, Any]:
    api_roots = kalshi_api_root_candidates(env_name.strip().lower())
    endpoint_path = "/portfolio/balance"
    attempted_roots: list[str] = []
    final_status: int | None = None
    final_payload: Any = None

    for index, api_root in enumerate(api_roots):
        attempted_roots.append(api_root)
        request_url = f"{api_root}{endpoint_path}"
        timestamp_ms = str(int(datetime.now().timestamp() * 1000))
        signature = sign_request(
            private_key_path,
            timestamp_ms,
            "GET",
            urlparse(request_url).path,
        )
        try:
            status_code, payload = _http_get_json_with_retries(
                url=request_url,
                headers={
                    "Accept": "application/json",
                    "KALSHI-ACCESS-KEY": access_key_id,
                    "KALSHI-ACCESS-SIGNATURE": signature,
                    "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
                    "User-Agent": "betbot-live-snapshot/1.0",
                },
                timeout_seconds=timeout_seconds,
                http_get_json=http_get_json,
            )
        except URLError:
            if index < len(api_roots) - 1:
                continue
            raise

        final_status = status_code
        final_payload = payload
        if status_code == 200 and isinstance(payload, dict):
            snapshot = {
                "http_status": status_code,
                "balance_cents": payload.get("balance"),
                "portfolio_value_cents": payload.get("portfolio_value"),
                "updated_ts": payload.get("updated_ts"),
                "api_root_used": api_root,
            }
            if len(attempted_roots) > 1:
                snapshot["api_roots_attempted"] = attempted_roots
            return snapshot

        transient_status_codes = LIVE_SNAPSHOT_RETRYABLE_STATUS_CODES | {408, 425, 599}
        if status_code in transient_status_codes and index < len(api_roots) - 1:
            continue
        break

    if final_status is None:
        raise ValueError("Kalshi balance request was not attempted")
    details = ""
    if isinstance(final_payload, dict):
        details = str(final_payload.get("error") or final_payload.get("raw_text_excerpt") or "").strip()
    if details:
        raise ValueError(f"Kalshi balance request failed with status {final_status}: {details}")
    raise ValueError(f"Kalshi balance request failed with status {final_status}")


def _therundown_sports_snapshot(
    *,
    api_key: str,
    base_url: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
    preview_limit: int,
) -> dict[str, Any]:
    request_url = _therundown_sports_url(base_url, api_key)
    status_code, payload = _http_get_json_with_retries(
        url=request_url,
        headers={"User-Agent": "betbot-live-snapshot/1.0"},
        timeout_seconds=timeout_seconds,
        http_get_json=http_get_json,
    )
    if status_code != 200 or not isinstance(payload, dict):
        raise ValueError(f"TheRundown sports request failed with status {status_code}")
    sports = payload.get("sports")
    if not isinstance(sports, list):
        raise ValueError("TheRundown sports payload did not contain a sports list")
    return {
        "http_status": status_code,
        "sports_count": len(sports),
        "sports_preview": sports[:preview_limit],
    }


def run_live_snapshot(
    *,
    env_file: str,
    output_dir: str = "outputs",
    timeout_seconds: float = 10.0,
    sports_preview_limit: int = 5,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
) -> dict[str, Any]:
    env_path = Path(env_file)
    data = _parse_env_file(env_path)

    failures: list[dict[str, str]] = []
    summary: dict[str, Any] = {
        "env_file": str(env_path),
        "captured_at": datetime.now().isoformat(),
    }

    kalshi_env = (data.get("KALSHI_ENV") or "").strip().lower()
    access_key_id = data.get("KALSHI_ACCESS_KEY_ID")
    private_key_path = data.get("KALSHI_PRIVATE_KEY_PATH")
    if (
        _is_placeholder(access_key_id)
        or _is_placeholder(private_key_path)
        or kalshi_env not in KALSHI_API_ROOTS
    ):
        failures.append(
            {
                "component": "kalshi",
                "message": "Kalshi credentials or environment are missing",
            }
        )
    else:
        try:
            summary["kalshi"] = _kalshi_balance_snapshot(
                access_key_id=access_key_id or "",
                private_key_path=private_key_path or "",
                env_name=kalshi_env,
                timeout_seconds=timeout_seconds,
                http_get_json=http_get_json,
                sign_request=sign_request,
            )
            summary["kalshi"]["env"] = kalshi_env
        except (URLError, ValueError, RuntimeError) as exc:
            failures.append({"component": "kalshi", "message": str(exc)})

    provider = (data.get("ODDS_PROVIDER") or "therundown").strip().lower()
    summary["odds_provider"] = provider
    if provider != "therundown":
        failures.append(
            {
                "component": "odds_provider",
                "message": f"live-snapshot does not support ODDS_PROVIDER={provider!r} yet",
            }
        )
    else:
        api_key = data.get("THERUNDOWN_API_KEY")
        base_url = data.get("THERUNDOWN_BASE_URL") or "https://therundown.io/api/v2"
        if _is_placeholder(api_key):
            failures.append(
                {
                    "component": "therundown",
                    "message": "TheRundown API key is missing",
                }
            )
        else:
            try:
                summary["therundown"] = _therundown_sports_snapshot(
                    api_key=api_key or "",
                    base_url=base_url,
                    timeout_seconds=timeout_seconds,
                    http_get_json=http_get_json,
                    preview_limit=sports_preview_limit,
                )
            except (URLError, ValueError) as exc:
                failures.append({"component": "therundown", "message": str(exc)})

    summary["status"] = "passed" if not failures else "failed"
    summary["failed"] = failures
    summary["checks_failed"] = len(failures)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"live_snapshot_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
