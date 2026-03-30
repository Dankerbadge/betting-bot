from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import ipaddress
import json
import os
from pathlib import Path
import random
import socket
import struct
from typing import Any, Callable, Iterator
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from betbot.onboarding import _parse_env_file

DnsOpenFn = Callable[..., Any]

DNS_ERROR_MARKERS = (
    "nodename nor servname",
    "name or service not known",
    "temporary failure in name resolution",
    "no address associated with hostname",
)
DNS_RECOVERY_HOST_SUFFIXES = (
    "kalshi.com",
    "kalshi.co",
    "therundown.io",
    "weather.gov",
    "googleapis.com",
)
PUBLIC_DNS_RESOLVERS = (
    "1.1.1.1",
    "8.8.8.8",
    "9.9.9.9",
)
DOH_ENDPOINTS = (
    "https://cloudflare-dns.com/dns-query",
    "https://dns.google/resolve",
)


def _exc_text(exc: BaseException) -> str:
    if isinstance(exc, URLError):
        reason = exc.reason
        if isinstance(reason, BaseException):
            return str(reason)
        return str(reason or exc)
    return str(exc)


def is_dns_resolution_error(exc: BaseException) -> bool:
    if isinstance(exc, URLError):
        reason = exc.reason
        if isinstance(reason, socket.gaierror):
            return True
        text = _exc_text(exc).strip().lower()
        return any(marker in text for marker in DNS_ERROR_MARKERS)
    if isinstance(exc, socket.gaierror):
        return True
    text = _exc_text(exc).strip().lower()
    return any(marker in text for marker in DNS_ERROR_MARKERS)


def _normalize_host(host: str) -> str:
    return str(host or "").strip().lower().rstrip(".")


def should_attempt_dns_recovery(host: str) -> bool:
    if os.getenv("BETBOT_DISABLE_DNS_RECOVERY", "").strip() in {"1", "true", "TRUE"}:
        return False
    normalized = _normalize_host(host)
    if not normalized:
        return False
    if os.getenv("BETBOT_DNS_RECOVERY_ALL_HOSTS", "").strip() in {"1", "true", "TRUE"}:
        return True
    return any(
        normalized == suffix or normalized.endswith(f".{suffix}")
        for suffix in DNS_RECOVERY_HOST_SUFFIXES
    )


def _unique_ip_values(values: list[str]) -> tuple[str, ...]:
    unique: list[str] = []
    for value in values:
        candidate = str(value or "").strip()
        if not candidate:
            continue
        try:
            ipaddress.ip_address(candidate)
        except ValueError:
            continue
        if candidate not in unique:
            unique.append(candidate)
    return tuple(unique)


def _skip_dns_name(payload: bytes, offset: int) -> int:
    cursor = max(0, int(offset))
    while cursor < len(payload):
        length = payload[cursor]
        if length == 0:
            return cursor + 1
        if (length & 0xC0) == 0xC0:
            if cursor + 1 >= len(payload):
                return len(payload)
            return cursor + 2
        cursor += 1 + length
    return len(payload)


def _build_dns_question(host: str, qtype: int) -> bytes:
    labels = [label for label in host.strip(".").split(".") if label]
    encoded_labels = b"".join(
        len(label.encode("idna")).to_bytes(1, "big") + label.encode("idna")
        for label in labels
    )
    return encoded_labels + b"\x00" + struct.pack("!HH", qtype, 1)


def _query_public_dns_udp(
    *,
    host: str,
    qtype: int,
    resolver_ip: str,
    timeout_seconds: float,
) -> tuple[str, ...]:
    txid = random.randint(0, 0xFFFF)
    header = struct.pack("!HHHHHH", txid, 0x0100, 1, 0, 0, 0)
    question = _build_dns_question(host, qtype)
    request_payload = header + question

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.settimeout(max(0.05, float(timeout_seconds)))
        sock.sendto(request_payload, (resolver_ip, 53))
        response_payload, _ = sock.recvfrom(2048)
    except OSError:
        return ()
    finally:
        try:
            sock.close()
        except OSError:
            pass

    if len(response_payload) < 12:
        return ()
    response_txid, response_flags, qd_count, an_count, _, _ = struct.unpack("!HHHHHH", response_payload[:12])
    if response_txid != txid:
        return ()
    if (response_flags & 0x000F) != 0:
        return ()

    offset = 12
    for _ in range(max(0, qd_count)):
        offset = _skip_dns_name(response_payload, offset)
        offset += 4
        if offset > len(response_payload):
            return ()

    answers: list[str] = []
    for _ in range(max(0, an_count)):
        offset = _skip_dns_name(response_payload, offset)
        if offset + 10 > len(response_payload):
            break
        record_type, record_class, _, rdlength = struct.unpack("!HHIH", response_payload[offset:offset + 10])
        offset += 10
        if offset + rdlength > len(response_payload):
            break
        rdata = response_payload[offset:offset + rdlength]
        offset += rdlength
        if record_class != 1:
            continue
        if record_type == 1 and len(rdata) == 4:
            answers.append(socket.inet_ntoa(rdata))
        elif record_type == 28 and len(rdata) == 16:
            try:
                answers.append(socket.inet_ntop(socket.AF_INET6, rdata))
            except OSError:
                continue

    return _unique_ip_values(answers)


def _resolve_host_via_doh(
    *,
    host: str,
    timeout_seconds: float,
    open_fn: DnsOpenFn,
) -> tuple[str, ...]:
    addresses: list[str] = []
    for endpoint in DOH_ENDPOINTS:
        query = f"{endpoint}?name={host}&type=A"
        request = Request(
            query,
            headers={
                "Accept": "application/dns-json, application/json",
                "User-Agent": "betbot-dns-guard/1.0",
            },
            method="GET",
        )
        try:
            with open_fn(request, timeout=max(0.1, float(timeout_seconds))) as response:
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        answer_rows = payload.get("Answer")
        if not isinstance(answer_rows, list):
            continue
        for row in answer_rows:
            if not isinstance(row, dict):
                continue
            if int(row.get("type") or 0) != 1:
                continue
            data = str(row.get("data") or "").strip()
            if data:
                addresses.append(data)
    return _unique_ip_values(addresses)


def resolve_host_with_public_dns(
    host: str,
    *,
    timeout_seconds: float = 1.5,
    open_fn: DnsOpenFn = urlopen,
) -> tuple[str, ...]:
    normalized = _normalize_host(host)
    if not normalized:
        return ()

    resolved: list[str] = []
    per_resolver_timeout = max(0.1, float(timeout_seconds) / max(1, len(PUBLIC_DNS_RESOLVERS)))
    for resolver in PUBLIC_DNS_RESOLVERS:
        for qtype in (1, 28):
            answers = _query_public_dns_udp(
                host=normalized,
                qtype=qtype,
                resolver_ip=resolver,
                timeout_seconds=per_resolver_timeout,
            )
            for answer in answers:
                if answer not in resolved:
                    resolved.append(answer)
        if resolved:
            return tuple(resolved)

    doh_answers = _resolve_host_via_doh(
        host=normalized,
        timeout_seconds=timeout_seconds,
        open_fn=open_fn,
    )
    for answer in doh_answers:
        if answer not in resolved:
            resolved.append(answer)
    return tuple(resolved)


@contextmanager
def _patched_getaddrinfo(host: str, addresses: tuple[str, ...]) -> Iterator[None]:
    normalized_host = _normalize_host(host)
    original_getaddrinfo = socket.getaddrinfo

    def patched(
        target_host: str,
        port: Any,
        family: int = 0,
        socktype: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> Any:
        if _normalize_host(str(target_host or "")) != normalized_host:
            return original_getaddrinfo(target_host, port, family, socktype, proto, flags)

        selected: list[Any] = []
        for raw_address in addresses:
            try:
                parsed = ipaddress.ip_address(raw_address)
            except ValueError:
                continue
            address_family = socket.AF_INET6 if parsed.version == 6 else socket.AF_INET
            if family not in {0, socket.AF_UNSPEC, address_family}:
                continue
            effective_socktype = socktype if socktype else socket.SOCK_STREAM
            effective_proto = proto if proto else socket.IPPROTO_TCP
            if address_family == socket.AF_INET6:
                sockaddr = (raw_address, int(port or 0), 0, 0)
            else:
                sockaddr = (raw_address, int(port or 0))
            selected.append((address_family, effective_socktype, effective_proto, "", sockaddr))

        if selected:
            return selected
        return original_getaddrinfo(target_host, port, family, socktype, proto, flags)

    socket.getaddrinfo = patched
    try:
        yield
    finally:
        socket.getaddrinfo = original_getaddrinfo


def urlopen_with_dns_recovery(
    request: Request,
    *,
    timeout_seconds: float,
    urlopen_fn: DnsOpenFn = urlopen,
) -> Any:
    try:
        return urlopen_fn(request, timeout=timeout_seconds)
    except URLError as exc:
        host = _normalize_host(urlparse(str(request.full_url)).hostname or "")
        if not host or not should_attempt_dns_recovery(host) or not is_dns_resolution_error(exc):
            raise
        recovered = resolve_host_with_public_dns(
            host,
            timeout_seconds=min(2.0, max(0.25, float(timeout_seconds) / 2.0)),
        )
        if not recovered:
            raise
        with _patched_getaddrinfo(host, recovered):
            return urlopen_fn(request, timeout=timeout_seconds)


def create_connection_with_dns_recovery(
    *,
    host: str,
    port: int,
    timeout_seconds: float,
) -> socket.socket:
    try:
        return socket.create_connection((host, port), timeout_seconds)
    except OSError as exc:
        if not should_attempt_dns_recovery(host) or not is_dns_resolution_error(exc):
            raise
        recovered = resolve_host_with_public_dns(
            host,
            timeout_seconds=min(2.0, max(0.25, float(timeout_seconds) / 2.0)),
        )
        if not recovered:
            raise
        last_error: OSError | None = None
        for address in recovered:
            try:
                return socket.create_connection((address, port), timeout_seconds)
            except OSError as candidate_exc:
                last_error = candidate_exc
                continue
        if last_error is not None:
            raise last_error
        raise


def _system_resolve(host: str) -> tuple[tuple[str, ...], str]:
    try:
        rows = socket.getaddrinfo(host, None)
    except OSError as exc:
        return (), _exc_text(exc)
    addresses: list[str] = []
    for row in rows:
        if not isinstance(row, tuple) or len(row) < 5:
            continue
        sockaddr = row[4]
        if not isinstance(sockaddr, tuple) or not sockaddr:
            continue
        candidate = str(sockaddr[0] or "").strip()
        if candidate and candidate not in addresses:
            addresses.append(candidate)
    return tuple(addresses), ""


def run_dns_doctor(
    *,
    env_file: str = "data/research/account_onboarding.env.template",
    hosts: tuple[str, ...] = (),
    output_dir: str = "outputs",
    timeout_seconds: float = 1.5,
) -> dict[str, Any]:
    env_data = _parse_env_file(Path(env_file))
    configured_hosts: list[str] = [str(item or "").strip() for item in hosts if str(item or "").strip()]
    if not configured_hosts:
        kalshi_env = str(env_data.get("KALSHI_ENV") or "").strip().lower()
        if kalshi_env in {"demo"}:
            configured_hosts.append("demo-api.kalshi.co")
        else:
            configured_hosts.extend(["api.elections.kalshi.com", "trading-api.kalshi.com"])
        therundown_base_url = str(env_data.get("THERUNDOWN_BASE_URL") or "https://therundown.io/api/v2").strip()
        therundown_host = _normalize_host(urlparse(therundown_base_url).hostname or "therundown.io")
        if therundown_host:
            configured_hosts.append(therundown_host)
        configured_hosts.append("api.therundown.io")

    unique_hosts: list[str] = []
    for host in configured_hosts:
        normalized = _normalize_host(host)
        if normalized and normalized not in unique_hosts:
            unique_hosts.append(normalized)

    checks: list[dict[str, Any]] = []
    healthy_count = 0
    for host in unique_hosts:
        system_ips, system_error = _system_resolve(host)
        recovered_ips = resolve_host_with_public_dns(host, timeout_seconds=timeout_seconds)
        healthy = bool(system_ips or recovered_ips)
        if healthy:
            healthy_count += 1
        checks.append(
            {
                "host": host,
                "healthy": healthy,
                "system_ips": list(system_ips),
                "public_dns_ips": list(recovered_ips),
                "system_error": system_error,
            }
        )

    status = "healthy" if healthy_count == len(unique_hosts) else ("degraded" if healthy_count > 0 else "failed")
    summary: dict[str, Any] = {
        "captured_at": datetime.now().isoformat(),
        "env_file": env_file,
        "status": status,
        "hosts_checked": len(unique_hosts),
        "hosts_healthy": healthy_count,
        "checks": checks,
    }
    if status != "healthy":
        summary["recommendation"] = (
            "Keep live mode gated; run live-smoke and kalshi-ws-state-collect after network recovers."
        )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = out_dir / f"dns_doctor_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
