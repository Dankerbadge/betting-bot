from __future__ import annotations

import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import struct
import time
from typing import Any, Callable
from urllib.parse import urlparse

from betbot.dns_guard import create_connection_with_dns_recovery
from betbot.kalshi_book_math import derive_top_of_book
from betbot.live_smoke import KalshiSigner, _kalshi_sign_request, kalshi_api_root_candidates
from betbot.onboarding import _parse_env_file


def default_ws_state_path(output_dir: str) -> Path:
    return Path(output_dir) / "kalshi_ws_state_latest.json"


KALSHI_WS_PATH = "/trade-api/ws/v2"
DEFAULT_WS_CHANNELS = (
    "orderbook_snapshot",
    "orderbook_delta",
    "user_orders",
    "user_fills",
    "market_positions",
)
MARKET_FILTER_CHANNELS = {
    "orderbook_snapshot",
    "orderbook_delta",
    "public_trades",
    "ticker",
}
_WS_MAGIC_KEY = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


_WS_TICKER_DISCOVERY_PATTERNS = (
    "kalshi_micro_prior_plan_summary_*.json",
    "kalshi_micro_plan_summary_*.json",
    "kalshi_micro_prior_execute_summary_*.json",
    "kalshi_micro_execute_summary_*.json",
)
_WS_TICKER_DISCOVERY_MAX = 20


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _parse_ts(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_market_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper()
    if not ticker:
        return ""
    if not ticker.startswith("KX"):
        return ""
    return ticker


def _append_ticker_if_valid(tickers: list[str], value: Any) -> None:
    ticker = _normalize_market_ticker(value)
    if not ticker or ticker in tickers:
        return
    tickers.append(ticker)


def _extract_tickers_from_summary_payload(payload: dict[str, Any]) -> list[str]:
    tickers: list[str] = []
    _append_ticker_if_valid(tickers, payload.get("top_market_ticker"))

    list_keys = (
        "top_plans",
        "planned_orders_preview",
        "eligible_orders_preview",
        "attempts",
        "prior_execute_attempts",
    )
    for key in list_keys:
        raw_rows = payload.get(key)
        if not isinstance(raw_rows, list):
            continue
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            _append_ticker_if_valid(tickers, row.get("market_ticker"))
            _append_ticker_if_valid(tickers, row.get("ticker"))
            preview = row.get("order_payload_preview")
            if isinstance(preview, dict):
                _append_ticker_if_valid(tickers, preview.get("ticker"))
            if len(tickers) >= _WS_TICKER_DISCOVERY_MAX:
                return tickers
    return tickers


def _discover_market_tickers_from_outputs(output_dir: Path) -> tuple[str, ...]:
    seen: list[str] = []
    for pattern in _WS_TICKER_DISCOVERY_PATTERNS:
        candidates = sorted(output_dir.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
        for candidate in candidates:
            try:
                payload = json.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            for ticker in _extract_tickers_from_summary_payload(payload):
                if ticker not in seen:
                    seen.append(ticker)
                    if len(seen) >= _WS_TICKER_DISCOVERY_MAX:
                        return tuple(seen)
            if seen:
                break
        if seen:
            break
    return tuple(seen)


def _to_utc_iso(ts: datetime | None = None) -> str:
    return (ts or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()


def _normalize_price_dollars(value: Any) -> str:
    parsed = _parse_float(value)
    if parsed is None:
        return ""
    if parsed > 1.0:
        parsed = parsed / 100.0
    parsed = max(0.0, min(1.0, parsed))
    return f"{parsed:.4f}"


def _normalize_size_contracts(value: Any) -> str:
    parsed = _parse_float(value)
    if parsed is None:
        return ""
    return f"{max(0.0, parsed):.4f}"


def _host_header(parsed_url: Any) -> str:
    host = str(parsed_url.hostname or "").strip()
    if not host:
        return ""
    port = parsed_url.port
    default_port = 443 if parsed_url.scheme == "wss" else 80
    if port is None or port == default_port:
        return host
    return f"{host}:{port}"


def _ws_root_from_api_root(api_root: str) -> str:
    if "/trade-api/v2" in api_root:
        base = api_root.split("/trade-api/v2", 1)[0]
    else:
        base = api_root.rstrip("/")
    if base.startswith("https://"):
        base = f"wss://{base[len('https://'):]}"
    elif base.startswith("http://"):
        base = f"ws://{base[len('http://'):]}"
    elif not (base.startswith("ws://") or base.startswith("wss://")):
        raise ValueError(f"Unsupported Kalshi API root for websocket conversion: {api_root!r}")
    return f"{base}{KALSHI_WS_PATH}"


def _ws_roots_for_env(env_name: str) -> tuple[str, ...]:
    ws_candidates: list[str] = []
    for api_root in kalshi_api_root_candidates(env_name):
        candidate = _ws_root_from_api_root(api_root)
        if candidate not in ws_candidates:
            ws_candidates.append(candidate)
    return tuple(ws_candidates)


def _ws_root_for_env(env_name: str) -> str:
    return _ws_roots_for_env(env_name)[0]


def _normalize_levels(levels: Any) -> list[list[str]]:
    normalized: list[list[str]] = []
    if not isinstance(levels, list):
        return normalized
    for level in levels:
        if not isinstance(level, list) or len(level) < 2:
            continue
        price = _parse_float(level[0])
        size = _parse_float(level[1])
        if price is None:
            continue
        normalized.append([f"{price:.4f}", f"{max(0.0, size or 0.0):.4f}"])
    return normalized


def _levels_to_map(levels: Any) -> dict[str, float]:
    as_map: dict[str, float] = {}
    for level in _normalize_levels(levels):
        price_key = level[0]
        size_value = _parse_float(level[1]) or 0.0
        if size_value <= 0:
            continue
        as_map[price_key] = size_value
    return as_map


def _sorted_levels(level_map: dict[str, float]) -> list[list[str]]:
    ranked = sorted(((float(price), size) for price, size in level_map.items()), reverse=True)
    return [[f"{price:.4f}", f"{size:.4f}"] for price, size in ranked if size > 0]


def _extract_sequence(payload: dict[str, Any]) -> int | None:
    for key in ("sequence", "seq", "sequence_id", "sequence_number"):
        value = _parse_int(payload.get(key))
        if value is not None:
            return value
    return None


def _extract_ticker(payload: dict[str, Any]) -> str:
    for key in ("market_ticker", "ticker", "market"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _extract_event_type(event: dict[str, Any]) -> str:
    event_type = str(event.get("event_type") or event.get("type") or "").strip().lower()
    channel = str(event.get("channel") or "").strip().lower()
    message_type = str(event.get("message_type") or event.get("msg_type") or "").strip().lower()
    for candidate in (event_type, channel, message_type):
        if candidate:
            return candidate
    return ""


def _extract_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    if isinstance(payload, dict):
        merged = dict(payload)
        for key in ("event_type", "type", "channel", "message_type", "msg_type"):
            if key in event and key not in merged:
                merged[key] = event[key]
        return merged
    return dict(event)


class KalshiRawWebSocketClient:
    def __init__(
        self,
        *,
        url: str,
        headers: dict[str, str] | None = None,
        connect_timeout_seconds: float = 10.0,
        read_timeout_seconds: float = 1.0,
    ) -> None:
        self.url = url
        self.headers = dict(headers or {})
        self.connect_timeout_seconds = max(1.0, float(connect_timeout_seconds))
        self.read_timeout_seconds = max(0.05, float(read_timeout_seconds))
        self._sock: socket.socket | None = None
        self._buffer = bytearray()
        self._closed = True

    def _ensure_socket(self) -> socket.socket:
        if self._sock is None:
            raise RuntimeError("websocket is not connected")
        return self._sock

    def _read_exact(self, size: int) -> bytes:
        sock = self._ensure_socket()
        while len(self._buffer) < size:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("websocket closed by peer")
            self._buffer.extend(chunk)
        output = bytes(self._buffer[:size])
        del self._buffer[:size]
        return output

    def _read_until(self, separator: bytes, *, max_bytes: int = 262_144) -> bytes:
        sock = self._ensure_socket()
        while True:
            marker = self._buffer.find(separator)
            if marker >= 0:
                end = marker + len(separator)
                output = bytes(self._buffer[:end])
                del self._buffer[:end]
                return output
            if len(self._buffer) >= max_bytes:
                raise ValueError("websocket handshake exceeded max header size")
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("websocket closed before handshake completed")
            self._buffer.extend(chunk)

    def connect(self) -> None:
        parsed = urlparse(self.url)
        scheme = parsed.scheme.lower()
        if scheme not in {"ws", "wss"}:
            raise ValueError(f"unsupported websocket scheme: {parsed.scheme!r}")
        if not parsed.hostname:
            raise ValueError("websocket URL missing hostname")
        port = parsed.port or (443 if scheme == "wss" else 80)
        raw_sock = create_connection_with_dns_recovery(
            host=parsed.hostname,
            port=port,
            timeout_seconds=self.connect_timeout_seconds,
        )
        raw_sock.settimeout(self.read_timeout_seconds)
        if scheme == "wss":
            context = ssl.create_default_context()
            sock: socket.socket = context.wrap_socket(raw_sock, server_hostname=parsed.hostname)
        else:
            sock = raw_sock
        self._sock = sock
        self._closed = False
        self._buffer.clear()

        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request_headers = {
            "Host": _host_header(parsed),
            "Upgrade": "websocket",
            "Connection": "Upgrade",
            "Sec-WebSocket-Key": key,
            "Sec-WebSocket-Version": "13",
            "User-Agent": "betbot-kalshi-ws-state/1.0",
        }
        request_headers.update(self.headers)
        header_lines = "".join(f"{name}: {value}\r\n" for name, value in request_headers.items())
        request_bytes = f"GET {path} HTTP/1.1\r\n{header_lines}\r\n".encode("utf-8")
        sock.sendall(request_bytes)

        header_block = self._read_until(b"\r\n\r\n")
        header_text = header_block.decode("utf-8", errors="replace")
        lines = [line.strip() for line in header_text.split("\r\n") if line.strip()]
        if not lines:
            raise ConnectionError("websocket handshake returned empty response")
        status_line = lines[0]
        if " 101 " not in f" {status_line} ":
            raise ConnectionError(f"websocket handshake failed: {status_line}")

        response_headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" not in line:
                continue
            name, value = line.split(":", 1)
            response_headers[name.strip().lower()] = value.strip()
        accept_expected = base64.b64encode(hashlib.sha1(f"{key}{_WS_MAGIC_KEY}".encode("utf-8")).digest()).decode("ascii")
        accept_actual = response_headers.get("sec-websocket-accept", "")
        if accept_actual != accept_expected:
            raise ConnectionError("websocket handshake returned invalid Sec-WebSocket-Accept")

    def _send_frame(self, opcode: int, payload: bytes = b"") -> None:
        sock = self._ensure_socket()
        first_byte = 0x80 | (opcode & 0x0F)
        payload_len = len(payload)
        if payload_len < 126:
            header = bytes([first_byte, 0x80 | payload_len])
        elif payload_len <= 0xFFFF:
            header = bytes([first_byte, 0x80 | 126]) + struct.pack("!H", payload_len)
        else:
            header = bytes([first_byte, 0x80 | 127]) + struct.pack("!Q", payload_len)
        mask = os.urandom(4)
        masked_payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        sock.sendall(header + mask + masked_payload)

    def send_text(self, text: str) -> None:
        self._send_frame(0x1, text.encode("utf-8"))

    def send_json(self, payload: dict[str, Any]) -> None:
        self.send_text(json.dumps(payload, separators=(",", ":")))

    def send_ping(self, payload: bytes = b"ping") -> None:
        self._send_frame(0x9, payload[:125])

    def send_pong(self, payload: bytes = b"") -> None:
        self._send_frame(0xA, payload[:125])

    def _recv_frame(self) -> tuple[int, bool, bytes]:
        first_two = self._read_exact(2)
        first = first_two[0]
        second = first_two[1]
        fin = bool(first & 0x80)
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        payload_len = second & 0x7F
        if payload_len == 126:
            payload_len = struct.unpack("!H", self._read_exact(2))[0]
        elif payload_len == 127:
            payload_len = struct.unpack("!Q", self._read_exact(8))[0]
        mask = self._read_exact(4) if masked else b""
        payload = self._read_exact(payload_len) if payload_len else b""
        if masked:
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        return opcode, fin, payload

    def read_message(self) -> tuple[int, bytes] | None:
        message_opcode: int | None = None
        chunks: list[bytes] = []
        while True:
            opcode, fin, payload = self._recv_frame()
            if opcode == 0x8:
                close_code = 1000
                if len(payload) >= 2:
                    close_code = struct.unpack("!H", payload[:2])[0]
                self.close()
                raise ConnectionError(f"websocket close frame received (code={close_code})")
            if opcode == 0x9:
                self.send_pong(payload)
                continue
            if opcode == 0xA:
                continue

            if opcode in {0x1, 0x2}:
                message_opcode = opcode
                chunks = [payload]
                if fin:
                    return message_opcode, b"".join(chunks)
                continue
            if opcode == 0x0:
                if message_opcode is None:
                    continue
                chunks.append(payload)
                if fin:
                    return message_opcode, b"".join(chunks)
                continue

    def read_text_message(self, *, timeout_seconds: float | None = None) -> str | None:
        sock = self._ensure_socket()
        previous_timeout = sock.gettimeout()
        if timeout_seconds is not None:
            sock.settimeout(max(0.01, float(timeout_seconds)))
        try:
            message = self.read_message()
        except socket.timeout:
            return None
        finally:
            if timeout_seconds is not None:
                sock.settimeout(previous_timeout)
        if message is None:
            return None
        opcode, payload = message
        if opcode == 0x1:
            return payload.decode("utf-8", errors="replace")
        if opcode == 0x2:
            return payload.decode("utf-8", errors="replace")
        return None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        sock = self._sock
        if sock is None:
            return
        try:
            self._send_frame(0x8, struct.pack("!H", 1000))
        except Exception:
            pass
        self._sock = None
        self._buffer.clear()
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            sock.close()
        except OSError:
            pass


def _normalize_side_levels(raw_levels: Any) -> list[list[str]]:
    normalized: list[list[str]] = []
    if not isinstance(raw_levels, list):
        return normalized
    for level in raw_levels:
        if isinstance(level, list) and len(level) >= 2:
            price = _normalize_price_dollars(level[0])
            size = _normalize_size_contracts(level[1])
            if price and size:
                normalized.append([price, size])
            continue
        if isinstance(level, dict):
            price = _normalize_price_dollars(level.get("price") or level.get("price_fp") or level.get("yes_price") or level.get("no_price"))
            size = _normalize_size_contracts(
                level.get("count")
                or level.get("contracts")
                or level.get("size")
                or level.get("quantity")
                or level.get("count_fp")
            )
            if price and size:
                normalized.append([price, size])
    return normalized


def _normalize_orderbook_message(
    *,
    event_type: str,
    message: dict[str, Any],
    envelope: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(message)
    payload["event_type"] = event_type
    market_ticker = (
        str(message.get("market_ticker") or message.get("ticker") or envelope.get("market_ticker") or "").strip()
    )
    if market_ticker:
        payload["market_ticker"] = market_ticker
    if "captured_at_utc" not in payload:
        payload["captured_at_utc"] = _to_utc_iso()

    sequence = _parse_int(message.get("sequence"))
    if sequence is None:
        sequence = _parse_int(envelope.get("seq") or envelope.get("sequence"))
    if sequence is not None:
        payload["sequence"] = sequence

    orderbook_fp = payload.get("orderbook_fp") if isinstance(payload.get("orderbook_fp"), dict) else {}
    yes_levels = _normalize_side_levels(orderbook_fp.get("yes_dollars"))
    no_levels = _normalize_side_levels(orderbook_fp.get("no_dollars"))
    if not yes_levels and "yes_dollars" in payload:
        yes_levels = _normalize_side_levels(payload.get("yes_dollars"))
    if not no_levels and "no_dollars" in payload:
        no_levels = _normalize_side_levels(payload.get("no_dollars"))
    if not yes_levels and "yes" in payload:
        yes_levels = _normalize_side_levels(payload.get("yes"))
    if not no_levels and "no" in payload:
        no_levels = _normalize_side_levels(payload.get("no"))

    if event_type == "orderbook_snapshot":
        payload["orderbook_fp"] = {
            "yes_dollars": yes_levels,
            "no_dollars": no_levels,
        }
        return payload

    if yes_levels:
        payload["yes_dollars_delta"] = yes_levels
    if no_levels:
        payload["no_dollars_delta"] = no_levels
    side = str(message.get("side") or "").strip().lower()
    if side in {"yes", "no"}:
        level_price = _normalize_price_dollars(message.get("price") or message.get("price_cents") or message.get("price_fp"))
        level_size = _normalize_size_contracts(
            message.get("size")
            or message.get("contracts")
            or message.get("count")
            or message.get("count_fp")
            or message.get("delta")
        )
        if level_price and level_size:
            payload[f"{side}_dollars_delta"] = [[level_price, level_size]]
    return payload


def normalize_ws_envelope(
    *,
    envelope: dict[str, Any],
    captured_at: datetime | None = None,
) -> list[dict[str, Any]]:
    event_time = captured_at or datetime.now(timezone.utc)
    event_type = str(envelope.get("type") or envelope.get("event_type") or envelope.get("channel") or "").strip().lower()
    if not event_type:
        return []

    payload = envelope.get("msg")
    if payload is None:
        payload = envelope

    if isinstance(payload, list):
        items = payload
    else:
        items = [payload]

    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, dict):
            item_payload = dict(item)
        else:
            item_payload = {"raw_msg": item}
        item_payload.setdefault("captured_at_utc", event_time.isoformat())
        item_payload.setdefault("event_type", event_type)
        if "sid" in envelope and "sid" not in item_payload:
            item_payload["sid"] = envelope["sid"]
        if "subscription_id" in envelope and "subscription_id" not in item_payload:
            item_payload["subscription_id"] = envelope["subscription_id"]
        if event_type in {"orderbook_snapshot", "orderbook_delta"}:
            normalized.append(_normalize_orderbook_message(event_type=event_type, message=item_payload, envelope=envelope))
        else:
            normalized.append(item_payload)
    return normalized


def _parse_ws_text_message(
    *,
    text: str,
    captured_at: datetime | None = None,
) -> tuple[list[dict[str, Any]], int]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return [], 1
    if isinstance(payload, dict):
        return normalize_ws_envelope(envelope=payload, captured_at=captured_at), 0
    if isinstance(payload, list):
        all_events: list[dict[str, Any]] = []
        parse_errors = 0
        for item in payload:
            if not isinstance(item, dict):
                parse_errors += 1
                continue
            normalized = normalize_ws_envelope(envelope=item, captured_at=captured_at)
            all_events.extend(normalized)
        return all_events, parse_errors
    return [], 1


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temporary.replace(path)


class KalshiWsStateEngine:
    def __init__(self, *, max_staleness_seconds: float = 30.0) -> None:
        self.max_staleness_seconds = max(1.0, float(max_staleness_seconds))
        self.market_books: dict[str, dict[str, Any]] = {}
        self.user_orders: dict[str, dict[str, Any]] = {}
        self.user_fills: list[dict[str, Any]] = []
        self.market_positions: dict[str, dict[str, Any]] = {}
        self.desynced_markets: dict[str, str] = {}
        self.events_processed = 0
        self.last_event_at: datetime | None = None

    def _touch(self, captured_at: datetime | None) -> None:
        self.events_processed += 1
        if captured_at is None:
            return
        if self.last_event_at is None or captured_at > self.last_event_at:
            self.last_event_at = captured_at

    def _mark_desync(self, ticker: str, reason: str) -> None:
        if not ticker:
            return
        self.desynced_markets[ticker] = reason

    def ingest_event(self, event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        payload = _extract_payload(event)
        event_type = _extract_event_type(payload)
        captured_at = _parse_ts(payload.get("captured_at_utc") or payload.get("captured_at"))
        self._touch(captured_at)

        if event_type in {"orderbook_snapshot", "book_snapshot"}:
            self._apply_orderbook_snapshot(payload, captured_at)
        elif event_type in {"orderbook_delta", "book_delta"}:
            self._apply_orderbook_delta(payload, captured_at)
        elif event_type in {"user_orders", "user_order"}:
            self._apply_user_order(payload, captured_at)
        elif event_type in {"user_fills", "user_fill"}:
            self._apply_user_fill(payload, captured_at)
        elif event_type in {"market_positions", "market_position"}:
            self._apply_market_position(payload, captured_at)

    def _ensure_market(self, ticker: str) -> dict[str, Any]:
        state = self.market_books.get(ticker)
        if state is None:
            state = {
                "sequence": None,
                "yes_levels": {},
                "no_levels": {},
                "top_of_book": {},
                "updated_at_utc": "",
            }
            self.market_books[ticker] = state
        return state

    def _apply_orderbook_snapshot(self, payload: dict[str, Any], captured_at: datetime | None) -> None:
        ticker = _extract_ticker(payload)
        if not ticker:
            return
        orderbook = payload.get("orderbook_fp")
        if not isinstance(orderbook, dict):
            orderbook = payload
        sequence = _extract_sequence(payload)
        state = self._ensure_market(ticker)
        state["yes_levels"] = _levels_to_map(orderbook.get("yes_dollars"))
        state["no_levels"] = _levels_to_map(orderbook.get("no_dollars"))
        state["sequence"] = sequence
        state["top_of_book"] = derive_top_of_book(
            {
                "yes_dollars": _sorted_levels(state["yes_levels"]),
                "no_dollars": _sorted_levels(state["no_levels"]),
            }
        )
        state["updated_at_utc"] = (captured_at or datetime.now(timezone.utc)).isoformat()
        self.desynced_markets.pop(ticker, None)

    def _extract_delta_levels(self, payload: dict[str, Any], side: str) -> list[list[str]]:
        candidates = (
            f"{side}_dollars_delta",
            f"{side}_deltas",
            f"{side}_levels_delta",
            f"{side}_updates",
            f"{side}_dollars",
        )
        for key in candidates:
            if key in payload and isinstance(payload.get(key), list):
                return _normalize_levels(payload.get(key))
        orderbook = payload.get("orderbook_fp")
        if isinstance(orderbook, dict):
            value = orderbook.get(f"{side}_dollars")
            if isinstance(value, list):
                return _normalize_levels(value)
        return []

    def _apply_orderbook_delta(self, payload: dict[str, Any], captured_at: datetime | None) -> None:
        ticker = _extract_ticker(payload)
        if not ticker:
            return
        state = self.market_books.get(ticker)
        if not isinstance(state, dict):
            return
        current_sequence = _parse_int(state.get("sequence"))
        incoming_sequence = _extract_sequence(payload)
        if current_sequence is None:
            # Deltas can arrive before the first snapshot immediately after subscribe.
            # Ignore these until we have a baseline snapshot for this ticker.
            return
        if incoming_sequence is None:
            self._mark_desync(ticker, "delta_missing_sequence")
            return
        if incoming_sequence != current_sequence + 1:
            self._mark_desync(ticker, f"sequence_gap_expected_{current_sequence + 1}_got_{incoming_sequence}")
            return

        yes_levels = state.get("yes_levels")
        no_levels = state.get("no_levels")
        if not isinstance(yes_levels, dict) or not isinstance(no_levels, dict):
            self._mark_desync(ticker, "missing_book_levels")
            return
        for price_text, size_text in self._extract_delta_levels(payload, "yes"):
            size_value = _parse_float(size_text) or 0.0
            if size_value <= 0:
                yes_levels.pop(price_text, None)
            else:
                yes_levels[price_text] = size_value
        for price_text, size_text in self._extract_delta_levels(payload, "no"):
            size_value = _parse_float(size_text) or 0.0
            if size_value <= 0:
                no_levels.pop(price_text, None)
            else:
                no_levels[price_text] = size_value
        state["sequence"] = incoming_sequence
        state["top_of_book"] = derive_top_of_book(
            {
                "yes_dollars": _sorted_levels(yes_levels),
                "no_dollars": _sorted_levels(no_levels),
            }
        )
        state["updated_at_utc"] = (captured_at or datetime.now(timezone.utc)).isoformat()

    def _apply_user_order(self, payload: dict[str, Any], captured_at: datetime | None) -> None:
        order = payload.get("order") if isinstance(payload.get("order"), dict) else payload
        order_id = str(order.get("order_id") or order.get("exchange_order_id") or "").strip()
        client_order_id = str(order.get("client_order_id") or "").strip()
        key = order_id or client_order_id
        if not key:
            return
        self.user_orders[key] = {
            "order_id": order_id,
            "client_order_id": client_order_id,
            "status": str(order.get("status") or "").strip().lower(),
            "ticker": str(order.get("ticker") or order.get("market_ticker") or "").strip(),
            "updated_at_utc": (captured_at or datetime.now(timezone.utc)).isoformat(),
        }

    def _apply_user_fill(self, payload: dict[str, Any], captured_at: datetime | None) -> None:
        fill = payload.get("fill") if isinstance(payload.get("fill"), dict) else payload
        self.user_fills.append(
            {
                "order_id": str(fill.get("order_id") or "").strip(),
                "client_order_id": str(fill.get("client_order_id") or "").strip(),
                "ticker": str(fill.get("ticker") or fill.get("market_ticker") or "").strip(),
                "side": str(fill.get("side") or "").strip().lower(),
                "count": _parse_float(fill.get("count") or fill.get("contracts") or fill.get("fill_count_fp")),
                "price_dollars": _parse_float(fill.get("price_dollars") or fill.get("yes_price_dollars")),
                "captured_at_utc": (captured_at or datetime.now(timezone.utc)).isoformat(),
            }
        )

    def _apply_market_position(self, payload: dict[str, Any], captured_at: datetime | None) -> None:
        position = payload.get("position") if isinstance(payload.get("position"), dict) else payload
        ticker = str(position.get("ticker") or position.get("market_ticker") or "").strip()
        if not ticker:
            return
        self.market_positions[ticker] = {
            "position_fp": _parse_float(position.get("position_fp")),
            "market_exposure_dollars": _parse_float(position.get("market_exposure_dollars")),
            "realized_pnl_dollars": _parse_float(position.get("realized_pnl_dollars")),
            "fees_paid_dollars": _parse_float(position.get("fees_paid_dollars")),
            "updated_at_utc": (captured_at or datetime.now(timezone.utc)).isoformat(),
        }

    def health_summary(self, *, now: datetime | None = None) -> dict[str, Any]:
        captured_at = now or datetime.now(timezone.utc)
        desync_count = len(self.desynced_markets)
        market_count = len(self.market_books)
        age_seconds = None
        if self.last_event_at is not None:
            age_seconds = max(0.0, (captured_at - self.last_event_at).total_seconds())

        status = "ready"
        if market_count <= 0:
            status = "empty"
        elif desync_count > 0:
            status = "desynced"
        elif age_seconds is None or age_seconds > self.max_staleness_seconds:
            status = "stale"

        ws_lag_ms = None
        if isinstance(age_seconds, float):
            ws_lag_ms = round(age_seconds * 1000.0, 3)

        return {
            "captured_at": captured_at.isoformat(),
            "status": status,
            "gate_pass": status == "ready",
            "max_staleness_seconds": self.max_staleness_seconds,
            "events_processed": self.events_processed,
            "market_count": market_count,
            "desynced_market_count": desync_count,
            "desynced_markets": dict(self.desynced_markets),
            "last_event_at": self.last_event_at.isoformat() if self.last_event_at is not None else "",
            "last_event_age_seconds": round(age_seconds, 6) if isinstance(age_seconds, float) else "",
            "websocket_lag_ms": ws_lag_ms if isinstance(ws_lag_ms, float) else "",
            "user_orders_tracked": len(self.user_orders),
            "user_fills_tracked": len(self.user_fills),
            "market_positions_tracked": len(self.market_positions),
        }

    def serialize(self, *, now: datetime | None = None) -> dict[str, Any]:
        summary = self.health_summary(now=now)
        return {
            "summary": summary,
            "markets": self.market_books,
            "user_orders": self.user_orders,
            "user_fills": self.user_fills,
            "market_positions": self.market_positions,
        }


def run_kalshi_ws_state_replay(
    *,
    events_ndjson: str,
    output_dir: str = "outputs",
    ws_state_json: str | None = None,
    max_staleness_seconds: float = 30.0,
    now: datetime | None = None,
) -> dict[str, Any]:
    events_path = Path(events_ndjson)
    if not events_path.exists():
        raise ValueError(f"events_ndjson not found: {events_path}")

    captured_at = now or datetime.now(timezone.utc)
    engine = KalshiWsStateEngine(max_staleness_seconds=max_staleness_seconds)
    processed_lines = 0
    parse_errors = 0
    with events_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            processed_lines += 1
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                parse_errors += 1
                continue
            if not isinstance(payload, dict):
                continue
            engine.ingest_event(payload)

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = Path(ws_state_json) if ws_state_json else default_ws_state_path(output_dir)
    state_payload = engine.serialize(now=captured_at)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state_payload, indent=2), encoding="utf-8")

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    summary_path = out_dir / f"kalshi_ws_state_summary_{stamp}.json"
    summary = {
        "captured_at": captured_at.isoformat(),
        "events_ndjson": str(events_path),
        "events_lines_processed": processed_lines,
        "events_parse_errors": parse_errors,
        **state_payload.get("summary", {}),
        "ws_state_json": str(state_path),
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary


def run_kalshi_ws_state_collect(
    *,
    env_file: str = "data/research/account_onboarding.env.template",
    channels: tuple[str, ...] = DEFAULT_WS_CHANNELS,
    market_tickers: tuple[str, ...] = (),
    output_dir: str = "outputs",
    ws_events_ndjson: str | None = None,
    ws_state_json: str | None = None,
    max_staleness_seconds: float = 30.0,
    run_seconds: float = 120.0,
    max_events: int = 0,
    connect_timeout_seconds: float = 10.0,
    read_timeout_seconds: float = 1.0,
    ping_interval_seconds: float = 15.0,
    flush_state_every_seconds: float = 2.0,
    reconnect_max_attempts: int = 8,
    reconnect_backoff_seconds: float = 1.0,
    sign_request: KalshiSigner = _kalshi_sign_request,
    websocket_client_factory: Callable[..., Any] = KalshiRawWebSocketClient,
    now: datetime | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    env_data = _parse_env_file(Path(env_file))
    env_name = str(env_data.get("KALSHI_ENV") or "").strip().lower()
    access_key = str(env_data.get("KALSHI_ACCESS_KEY_ID") or "").strip()
    private_key_path = str(env_data.get("KALSHI_PRIVATE_KEY_PATH") or "").strip()
    if not env_name:
        raise ValueError("KALSHI_ENV is required in env_file")
    if not access_key:
        raise ValueError("KALSHI_ACCESS_KEY_ID is required in env_file")
    if not private_key_path:
        raise ValueError("KALSHI_PRIVATE_KEY_PATH is required in env_file")
    ws_urls = _ws_roots_for_env(env_name)
    ws_url = ws_urls[0]

    effective_channels = tuple(
        dict.fromkeys(
            channel.strip().lower()
            for channel in channels
            if channel and channel.strip()
        )
    )
    if not effective_channels:
        effective_channels = DEFAULT_WS_CHANNELS
    explicit_tickers = tuple(
        dict.fromkeys(
            ticker.strip().upper()
            for ticker in market_tickers
            if ticker and ticker.strip()
        )
    )
    effective_tickers = explicit_tickers
    max_events_limit = max(0, int(max_events))
    max_reconnects = max(0, int(reconnect_max_attempts))
    reconnect_backoff = max(0.1, float(reconnect_backoff_seconds))
    ping_every = max(1.0, float(ping_interval_seconds))
    flush_every = max(0.25, float(flush_state_every_seconds))
    read_timeout = max(0.05, float(read_timeout_seconds))
    connect_timeout = max(1.0, float(connect_timeout_seconds))
    run_budget_seconds = max(1.0, float(run_seconds))

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    auto_discovered_tickers = ()
    if not effective_tickers:
        auto_discovered_tickers = _discover_market_tickers_from_outputs(out_dir)
        if auto_discovered_tickers:
            effective_tickers = auto_discovered_tickers
    started_at = now or datetime.now(timezone.utc)
    stamp = started_at.astimezone().strftime("%Y%m%d_%H%M%S_%f")[:-3]
    events_path = Path(ws_events_ndjson) if ws_events_ndjson else out_dir / f"kalshi_ws_events_{stamp}.ndjson"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    state_path = Path(ws_state_json) if ws_state_json else default_ws_state_path(output_dir)

    engine = KalshiWsStateEngine(max_staleness_seconds=max_staleness_seconds)
    capture_started = time.monotonic()
    capture_deadline = capture_started + run_budget_seconds
    connection_attempts = 0
    reconnects = 0
    messages_received = 0
    events_logged = 0
    parse_errors = 0
    subscription_requests = 0
    subscription_errors = 0
    last_error = ""
    ws_urls_attempted: list[str] = []
    ws_url_used = ""
    ws_url_failover_errors: list[str] = []

    with events_path.open("a", encoding="utf-8") as events_handle:
        while time.monotonic() < capture_deadline:
            if max_events_limit > 0 and events_logged >= max_events_limit:
                break
            if reconnects > max_reconnects:
                break

            timestamp_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
            signature = sign_request(private_key_path, timestamp_ms, "GET", KALSHI_WS_PATH)
            ws_headers = {
                "KALSHI-ACCESS-KEY": access_key,
                "KALSHI-ACCESS-SIGNATURE": signature,
                "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
                "User-Agent": "betbot-kalshi-ws-state/1.0",
            }

            client: Any | None = None
            for candidate_ws_url in ws_urls:
                if candidate_ws_url not in ws_urls_attempted:
                    ws_urls_attempted.append(candidate_ws_url)
                connection_attempts += 1
                candidate_client = websocket_client_factory(
                    url=candidate_ws_url,
                    headers=ws_headers,
                    connect_timeout_seconds=connect_timeout,
                    read_timeout_seconds=read_timeout,
                )
                try:
                    candidate_client.connect()
                    client = candidate_client
                    ws_url_used = candidate_ws_url
                    break
                except (ConnectionError, OSError, socket.timeout, ValueError, RuntimeError) as exc:
                    last_error = str(exc)
                    ws_url_failover_errors.append(f"{candidate_ws_url}: {last_error}")
                    try:
                        candidate_client.close()
                    except Exception:
                        pass
                    continue

            if client is None:
                reconnects += 1
                if time.monotonic() >= capture_deadline:
                    break
                if reconnects > max_reconnects:
                    break
                sleep_seconds = reconnect_backoff * (2 ** max(0, min(reconnects - 1, 6)))
                sleep_fn(sleep_seconds)
                continue

            try:
                request_id = 1
                for channel in effective_channels:
                    params: dict[str, Any] = {"channels": [channel]}
                    if effective_tickers and channel in MARKET_FILTER_CHANNELS:
                        params["market_tickers"] = list(effective_tickers)
                    subscription = {
                        "id": request_id,
                        "cmd": "subscribe",
                        "params": params,
                    }
                    request_id += 1
                    subscription_requests += 1
                    try:
                        client.send_json(subscription)
                    except Exception:
                        subscription_errors += 1
                        raise

                next_ping = time.monotonic() + ping_every
                next_flush = time.monotonic() + flush_every
                while time.monotonic() < capture_deadline:
                    if max_events_limit > 0 and events_logged >= max_events_limit:
                        break
                    now_mono = time.monotonic()
                    if now_mono >= next_ping:
                        client.send_ping()
                        next_ping = now_mono + ping_every

                    remaining = max(0.01, capture_deadline - now_mono)
                    text_message = client.read_text_message(timeout_seconds=min(read_timeout, remaining))
                    if text_message is None:
                        if now_mono >= next_flush:
                            _write_json_atomic(state_path, engine.serialize())
                            next_flush = now_mono + flush_every
                        continue

                    messages_received += 1
                    normalized_events, message_parse_errors = _parse_ws_text_message(
                        text=text_message,
                        captured_at=datetime.now(timezone.utc),
                    )
                    parse_errors += message_parse_errors
                    for event in normalized_events:
                        events_handle.write(json.dumps(event) + "\n")
                        events_logged += 1
                        engine.ingest_event(event)
                        if max_events_limit > 0 and events_logged >= max_events_limit:
                            break
                    if normalized_events:
                        events_handle.flush()
                    if now_mono >= next_flush:
                        _write_json_atomic(state_path, engine.serialize())
                        next_flush = now_mono + flush_every
                _write_json_atomic(state_path, engine.serialize())
                break
            except (ConnectionError, OSError, socket.timeout, ValueError, RuntimeError) as exc:
                last_error = str(exc)
                # Count failed handshakes toward the reconnect budget so upstream
                # DNS/TLS outages do not spin until the full run window expires.
                reconnects += 1
                if time.monotonic() >= capture_deadline:
                    break
                if reconnects > max_reconnects:
                    break
                sleep_seconds = reconnect_backoff * (2 ** max(0, min(reconnects - 1, 6)))
                sleep_fn(sleep_seconds)
            finally:
                try:
                    client.close()
                except Exception:
                    pass

    finished_at = datetime.now(timezone.utc)
    state_payload = engine.serialize(now=finished_at)
    if last_error:
        summary_payload = state_payload.get("summary")
        if isinstance(summary_payload, dict):
            summary_payload["last_error"] = last_error
            if int(summary_payload.get("market_count") or 0) <= 0 and int(summary_payload.get("events_processed") or 0) <= 0:
                summary_payload["status"] = "upstream_error"
                summary_payload["gate_pass"] = False
    _write_json_atomic(state_path, state_payload)
    summary = {
        "captured_at": finished_at.isoformat(),
        "started_at": started_at.isoformat(),
        "duration_seconds": round(max(0.0, time.monotonic() - capture_started), 6),
        "kalshi_env": env_name,
        "ws_url": ws_url,
        "ws_urls": list(ws_urls),
        "ws_urls_attempted": list(ws_urls_attempted),
        "ws_url_used": ws_url_used,
        "channels": list(effective_channels),
        "market_tickers": list(effective_tickers),
        "market_tickers_explicit": list(explicit_tickers),
        "market_tickers_auto_discovered": list(auto_discovered_tickers),
        "connection_attempts": connection_attempts,
        "reconnects": reconnects,
        "messages_received": messages_received,
        "events_logged": events_logged,
        "events_parse_errors": parse_errors,
        "subscription_requests": subscription_requests,
        "subscription_errors": subscription_errors,
        "max_events": max_events_limit,
        "max_events_reached": max_events_limit > 0 and events_logged >= max_events_limit,
        "ws_events_ndjson": str(events_path),
        "ws_state_json": str(state_path),
        "last_error": last_error,
        **state_payload.get("summary", {}),
    }
    if ws_url_failover_errors:
        summary["ws_url_failover_errors"] = ws_url_failover_errors
    summary_path = out_dir / f"kalshi_ws_state_collect_summary_{stamp}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(summary_path)
    return summary


def load_ws_state_authority(
    *,
    ws_state_json: str | Path,
    captured_at: datetime | None = None,
    max_staleness_seconds: float = 30.0,
) -> dict[str, Any]:
    now = captured_at or datetime.now(timezone.utc)
    path = Path(ws_state_json)
    base = {
        "checked": True,
        "path": str(path),
        "status": "missing",
        "gate_pass": False,
        "reason": "ws_state_file_missing",
        "market_count": 0,
        "desynced_market_count": 0,
        "last_event_at": "",
        "last_event_age_seconds": "",
        "websocket_lag_ms": "",
        "max_staleness_seconds": max(1.0, float(max_staleness_seconds)),
    }
    if not path.exists():
        return base

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        base.update({"status": "invalid", "reason": "ws_state_file_invalid"})
        return base
    if not isinstance(payload, dict):
        base.update({"status": "invalid", "reason": "ws_state_file_invalid"})
        return base

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else payload
    status = str(summary.get("status") or "").strip().lower()
    last_error = str(payload.get("last_error") or summary.get("last_error") or "").strip()
    market_count = int(_parse_int(summary.get("market_count")) or 0)
    desynced_count = int(_parse_int(summary.get("desynced_market_count")) or 0)
    last_event_at = _parse_ts(summary.get("last_event_at"))
    age_seconds = None
    if last_event_at is not None:
        age_seconds = max(0.0, (now - last_event_at).total_seconds())
    stale_limit = max(1.0, float(max_staleness_seconds))

    if not status:
        if market_count <= 0:
            status = "empty"
        elif desynced_count > 0:
            status = "desynced"
        elif age_seconds is None or age_seconds > stale_limit:
            status = "stale"
        else:
            status = "ready"
    if status == "ready" and (age_seconds is None or age_seconds > stale_limit):
        status = "stale"
    if status == "empty" and last_error and market_count <= 0:
        status = "upstream_error"

    reason_by_status = {
        "ready": "ws_state_ready",
        "empty": "ws_state_empty",
        "stale": "ws_state_stale",
        "desynced": "ws_state_desynced",
        "invalid": "ws_state_invalid",
        "missing": "ws_state_missing",
        "upstream_error": "ws_state_upstream_error",
    }
    gate_pass = status == "ready"
    return {
        "checked": True,
        "path": str(path),
        "status": status,
        "gate_pass": gate_pass,
        "reason": reason_by_status.get(status, f"ws_state_{status}"),
        "market_count": market_count,
        "desynced_market_count": desynced_count,
        "last_event_at": last_event_at.isoformat() if last_event_at is not None else "",
        "last_event_age_seconds": round(age_seconds, 6) if isinstance(age_seconds, float) else "",
        "websocket_lag_ms": (round(age_seconds * 1000.0, 3) if isinstance(age_seconds, float) else ""),
        "max_staleness_seconds": stale_limit,
    }
