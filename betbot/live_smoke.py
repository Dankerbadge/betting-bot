from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import socket
import subprocess
import time
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from betbot.dns_guard import urlopen_with_dns_recovery
from betbot.onboarding import _is_placeholder, _parse_env_file


KALSHI_API_ROOTS = {
    "demo": "https://demo-api.kalshi.co/trade-api/v2",
    "prod": "https://api.elections.kalshi.com/trade-api/v2",
    "production": "https://api.elections.kalshi.com/trade-api/v2",
}
KALSHI_API_ROOT_FAILOVERS = {
    "demo": (),
    "prod": ("https://trading-api.kalshi.com/trade-api/v2",),
    "production": ("https://trading-api.kalshi.com/trade-api/v2",),
}
LIVE_SMOKE_NETWORK_MAX_RETRIES = 2
LIVE_SMOKE_NETWORK_BACKOFF_SECONDS = 0.75
HTTP_GET_NETWORK_MAX_RETRIES = 2
HTTP_GET_NETWORK_BACKOFF_SECONDS = 0.35
DNS_ERROR_MARKERS = (
    "nodename nor servname",
    "name or service not known",
    "temporary failure in name resolution",
    "no address associated with hostname",
)


@dataclass(frozen=True)
class LiveSmokeCheck:
    component: str
    target: str
    ok: bool
    message: str
    http_status: int | None = None
    latency_ms: int | None = None
    details: dict[str, Any] | None = None


HttpGetter = Callable[[str, dict[str, str], float], tuple[int, Any]]
KalshiSigner = Callable[[str, str, str, str], str]


def kalshi_api_root_candidates(env_name: str) -> tuple[str, ...]:
    normalized = env_name.strip().lower()
    primary = KALSHI_API_ROOTS.get(normalized)
    if not primary:
        raise ValueError(f"Unsupported KALSHI_ENV={env_name!r}")
    candidates = [primary]
    for value in KALSHI_API_ROOT_FAILOVERS.get(normalized, ()):
        candidate = str(value or "").strip()
        if not candidate or candidate in candidates:
            continue
        candidates.append(candidate)
    return tuple(candidates)


def _json_excerpt(payload: Any) -> str:
    if isinstance(payload, dict):
        keys = ", ".join(sorted(str(key) for key in payload.keys())[:5])
        return f"JSON object keys: {keys}" if keys else "JSON object"
    if isinstance(payload, list):
        return f"JSON array with {len(payload)} item(s)"
    return f"Response type: {type(payload).__name__}"


def _extract_error_details(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    error = payload.get("error")
    if isinstance(error, dict):
        extracted = {
            "error_code": error.get("code"),
            "error_message": error.get("message"),
            "error_details": error.get("details"),
        }
        return {key: value for key, value in extracted.items() if value is not None}
    return None


def _decode_response_body(raw_body: bytes) -> Any:
    text = raw_body.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw_text_excerpt": text[:200]}


def _url_error_reason_text(exc: URLError) -> str:
    reason = exc.reason
    if isinstance(reason, BaseException):
        return str(reason)
    return str(reason or exc)


def _is_transient_network_error(exc: URLError | TimeoutError) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    reason = exc.reason
    if isinstance(reason, (TimeoutError, socket.timeout)):
        return True
    if isinstance(reason, socket.gaierror):
        return True
    text = _url_error_reason_text(exc).lower()
    if any(marker in text for marker in DNS_ERROR_MARKERS):
        return True
    transient_markers = (
        "timed out",
        "temporarily unavailable",
        "network is unreachable",
        "connection reset",
        "connection refused",
    )
    return any(marker in text for marker in transient_markers)


def _http_get_json(
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
) -> tuple[int, Any]:
    request = Request(url=url, headers=headers, method="GET")
    for attempt in range(HTTP_GET_NETWORK_MAX_RETRIES + 1):
        try:
            with urlopen_with_dns_recovery(
                request,
                timeout_seconds=timeout_seconds,
                urlopen_fn=urlopen,
            ) as response:
                return response.getcode(), _decode_response_body(response.read())
        except HTTPError as exc:
            return exc.code, _decode_response_body(exc.read())
        except (URLError, TimeoutError) as exc:
            if isinstance(exc, URLError) and not _is_transient_network_error(exc):
                raise
            if attempt >= HTTP_GET_NETWORK_MAX_RETRIES:
                raise
            time.sleep(HTTP_GET_NETWORK_BACKOFF_SECONDS * (2**attempt))
    raise RuntimeError("unreachable")


def _http_get_json_with_retries(
    *,
    url: str,
    headers: dict[str, str],
    timeout_seconds: float,
    http_get_json: HttpGetter,
    max_retries: int = LIVE_SMOKE_NETWORK_MAX_RETRIES,
    backoff_seconds: float = LIVE_SMOKE_NETWORK_BACKOFF_SECONDS,
    sleep_fn: Callable[[float], None] | None = None,
) -> tuple[int, Any]:
    effective_sleep = sleep_fn or time.sleep
    for attempt in range(max(0, max_retries) + 1):
        try:
            return http_get_json(url, headers, timeout_seconds)
        except URLError:
            if attempt >= max_retries:
                raise
            effective_sleep(backoff_seconds * (2**attempt))
    raise RuntimeError("unreachable")


def _kalshi_sign_request(
    private_key_path: str,
    timestamp_ms: str,
    method: str,
    path: str,
) -> str:
    path_without_query = path.split("?", 1)[0]
    message = f"{timestamp_ms}{method.upper()}{path_without_query}".encode("utf-8")
    try:
        result = subprocess.run(
            [
                "openssl",
                "dgst",
                "-sha256",
                "-sign",
                private_key_path,
                "-sigopt",
                "rsa_padding_mode:pss",
                "-sigopt",
                "rsa_pss_saltlen:-1",
                "-binary",
            ],
            input=message,
            capture_output=True,
            check=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("OpenSSL CLI is required for Kalshi request signing") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"OpenSSL signing failed: {stderr or 'unknown error'}") from exc
    return base64.b64encode(result.stdout).decode("utf-8")


def _build_therundown_url(base_url: str, api_key: str) -> str:
    query = urlencode({"key": api_key})
    return f"{base_url.rstrip('/')}/sports?{query}"


def _run_therundown_smoke(
    *,
    api_key: str,
    base_url: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
) -> LiveSmokeCheck:
    started = time.perf_counter()
    url = _build_therundown_url(base_url, api_key)
    headers = {"User-Agent": "betbot-live-smoke/1.0"}

    try:
        status_code, payload = _http_get_json_with_retries(
            url=url,
            headers=headers,
            timeout_seconds=timeout_seconds,
            http_get_json=http_get_json,
        )
    except URLError as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return LiveSmokeCheck(
            component="therundown",
            target="/sports",
            ok=False,
            message=f"Network error: {exc.reason}",
            latency_ms=latency_ms,
        )

    latency_ms = int((time.perf_counter() - started) * 1000)
    sports = payload.get("sports") if isinstance(payload, dict) else None
    if status_code == 200 and isinstance(sports, list) and sports:
        return LiveSmokeCheck(
            component="therundown",
            target="/sports",
            ok=True,
            message=f"Fetched {len(sports)} sports from TheRundown",
            http_status=status_code,
            latency_ms=latency_ms,
            details={"sports_count": len(sports)},
        )

    return LiveSmokeCheck(
        component="therundown",
        target="/sports",
        ok=False,
        message=f"Unexpected response from TheRundown: {_json_excerpt(payload)}",
        http_status=status_code,
        latency_ms=latency_ms,
    )


def _run_kalshi_smoke(
    *,
    access_key_id: str,
    private_key_path: str,
    env_name: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
    sign_request: KalshiSigner,
) -> LiveSmokeCheck:
    return _run_kalshi_smoke_with_probe(
        access_key_id=access_key_id,
        private_key_path=private_key_path,
        env_name=env_name,
        timeout_seconds=timeout_seconds,
        http_get_json=http_get_json,
        sign_request=sign_request,
        allow_alt_env_probe=True,
    )


def _run_kalshi_smoke_with_probe(
    *,
    access_key_id: str,
    private_key_path: str,
    env_name: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
    sign_request: KalshiSigner,
    allow_alt_env_probe: bool,
) -> LiveSmokeCheck:
    endpoint_path = "/portfolio/balance"
    transient_http_status_codes = {408, 425, 500, 502, 503, 504}
    api_roots = kalshi_api_root_candidates(env_name)
    attempted_roots: list[str] = []
    network_errors: list[str] = []
    status_code: int | None = None
    payload: Any = {}
    latency_ms = 0

    for index, api_root in enumerate(api_roots):
        request_url = f"{api_root}{endpoint_path}"
        signature_path = urlparse(request_url).path
        timestamp_ms = str(int(time.time() * 1000))
        attempted_roots.append(api_root)

        try:
            signature = sign_request(private_key_path, timestamp_ms, "GET", signature_path)
        except RuntimeError as exc:
            return LiveSmokeCheck(
                component="kalshi",
                target=endpoint_path,
                ok=False,
                message=str(exc),
            )

        headers = {
            "Accept": "application/json",
            "KALSHI-ACCESS-KEY": access_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "User-Agent": "betbot-live-smoke/1.0",
        }

        started = time.perf_counter()
        try:
            status_code, payload = _http_get_json_with_retries(
                url=request_url,
                headers=headers,
                timeout_seconds=timeout_seconds,
                http_get_json=http_get_json,
            )
        except URLError as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            network_errors.append(f"{api_root}: {exc.reason}")
            if index < len(api_roots) - 1:
                continue
            return LiveSmokeCheck(
                component="kalshi",
                target=endpoint_path,
                ok=False,
                message=f"Network error: {exc.reason}",
                latency_ms=latency_ms,
                details={
                    "attempted_api_roots": attempted_roots,
                    "network_errors": network_errors,
                },
            )

        latency_ms = int((time.perf_counter() - started) * 1000)
        if status_code in transient_http_status_codes and index < len(api_roots) - 1:
            continue
        break

    balance = payload.get("balance") if isinstance(payload, dict) else None
    if status_code == 200 and isinstance(balance, int | float):
        details: dict[str, Any] = {
            "balance_cents": int(balance),
            "api_root_used": attempted_roots[-1] if attempted_roots else "",
        }
        if len(attempted_roots) > 1:
            details["attempted_api_roots"] = attempted_roots
        if network_errors:
            details["network_errors"] = network_errors
        return LiveSmokeCheck(
            component="kalshi",
            target=endpoint_path,
            ok=True,
            message="Authenticated Kalshi balance request succeeded",
            http_status=status_code,
            latency_ms=latency_ms,
            details=details,
        )

    error_details = _extract_error_details(payload) or {}
    if attempted_roots:
        error_details["attempted_api_roots"] = attempted_roots
        error_details["api_root_used"] = attempted_roots[-1]
    if network_errors:
        error_details["network_errors"] = network_errors
    if (
        allow_alt_env_probe
        and status_code == 401
        and error_details.get("error_details") == "NOT_FOUND"
    ):
        alternate_env = "prod" if env_name == "demo" else "demo"
        alternate_check = _run_kalshi_smoke_with_probe(
            access_key_id=access_key_id,
            private_key_path=private_key_path,
            env_name=alternate_env,
            timeout_seconds=timeout_seconds,
            http_get_json=http_get_json,
            sign_request=sign_request,
            allow_alt_env_probe=False,
        )
        if alternate_check.ok:
            alt_details = dict(error_details)
            alt_details["suggested_env"] = alternate_env
            if alternate_check.details:
                alt_details["alternate_env_details"] = alternate_check.details
            return LiveSmokeCheck(
                component="kalshi",
                target=endpoint_path,
                ok=False,
                message=(
                    f"Configured Kalshi env {env_name!r} failed, "
                    f"but {alternate_env!r} succeeded"
                ),
                http_status=status_code,
                latency_ms=latency_ms,
                details=alt_details,
            )

    return LiveSmokeCheck(
        component="kalshi",
        target=endpoint_path,
        ok=False,
        message=f"Unexpected response from Kalshi: {_json_excerpt(payload)}",
        http_status=status_code,
        latency_ms=latency_ms,
        details=error_details or None,
    )


def run_live_smoke(
    *,
    env_file: str,
    output_dir: str = "outputs",
    timeout_seconds: float = 10.0,
    include_odds_provider_check: bool = True,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
) -> dict[str, Any]:
    env_path = Path(env_file)
    data = _parse_env_file(env_path)
    checks: list[LiveSmokeCheck] = []

    kalshi_env = (data.get("KALSHI_ENV") or "").strip().lower()
    kalshi_access_key_id = data.get("KALSHI_ACCESS_KEY_ID")
    kalshi_private_key_path = data.get("KALSHI_PRIVATE_KEY_PATH")
    if (
        not _is_placeholder(kalshi_access_key_id)
        and not _is_placeholder(kalshi_private_key_path)
        and kalshi_env in KALSHI_API_ROOTS
    ):
        checks.append(
            _run_kalshi_smoke(
                access_key_id=kalshi_access_key_id or "",
                private_key_path=kalshi_private_key_path or "",
                env_name=kalshi_env,
                timeout_seconds=timeout_seconds,
                http_get_json=http_get_json,
                sign_request=sign_request,
            )
        )
    else:
        checks.append(
            LiveSmokeCheck(
                component="kalshi",
                target="/portfolio/balance",
                ok=False,
                message="Kalshi credentials or environment are missing",
            )
        )

    provider = (data.get("ODDS_PROVIDER") or "therundown").strip().lower()
    if include_odds_provider_check:
        if provider == "therundown":
            api_key = data.get("THERUNDOWN_API_KEY")
            base_url = data.get("THERUNDOWN_BASE_URL") or "https://therundown.io/api/v2"
            if not _is_placeholder(api_key):
                checks.append(
                    _run_therundown_smoke(
                        api_key=api_key or "",
                        base_url=base_url,
                        timeout_seconds=timeout_seconds,
                        http_get_json=http_get_json,
                    )
                )
            else:
                checks.append(
                    LiveSmokeCheck(
                        component="therundown",
                        target="/sports",
                        ok=False,
                        message="TheRundown API key is missing",
                    )
                )
        else:
            checks.append(
                LiveSmokeCheck(
                    component="odds_provider",
                    target=provider,
                    ok=False,
                    message=f"Live smoke does not support ODDS_PROVIDER={provider!r} yet",
                )
            )
    else:
        checks.append(
            LiveSmokeCheck(
                component="odds_provider",
                target=provider or "unknown",
                ok=True,
                message="Odds-provider smoke was skipped by request",
            )
        )

    failed = [check for check in checks if not check.ok]
    summary: dict[str, Any] = {
        "env_file": str(env_path),
        "checked_at": datetime.now().isoformat(),
        "status": "passed" if not failed else "failed",
        "checks_total": len(checks),
        "checks_failed": len(failed),
        "failed": [
            {
                "component": check.component,
                "target": check.target,
                "message": check.message,
                "http_status": check.http_status,
            }
            for check in failed
        ],
        "checks": [
            {
                "component": check.component,
                "target": check.target,
                "ok": check.ok,
                "message": check.message,
                "http_status": check.http_status,
                "latency_ms": check.latency_ms,
                "details": check.details,
            }
            for check in checks
        ],
    }

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"live_smoke_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
