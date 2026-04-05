from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import uuid
from urllib.parse import urlencode
from typing import Any

from betbot.execution.live_executor import LiveReconciliationResult, LiveVenueAck
from betbot.execution.ticket import TicketProposal
from betbot.kalshi_micro_execute import (
    AuthenticatedRequester,
    _http_request_json,
    _signed_kalshi_request,
)
from betbot.live_smoke import KalshiSigner, _kalshi_sign_request
from betbot.onboarding import _parse_env_file

_TRANSIENT_HTTP_STATUSES = {408, 425, 500, 502, 503, 504, 599}
_TERMINAL_FILLED_STATUSES = {"executed", "filled", "completed", "closed"}
_TERMINAL_CANCELED_STATUSES = {"canceled", "cancelled", "expired", "rejected", "voided"}


def _to_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _payload_reason(payload: Any, *, fallback: str) -> str:
    if isinstance(payload, dict):
        error_type = str(payload.get("error_type") or "").strip()
        if error_type:
            return error_type
        error = str(payload.get("error") or "").strip()
        if error:
            return error
    return fallback


def _ticket_price_dollars(ticket: TicketProposal) -> float:
    raw = _to_float(ticket.max_cost)
    if raw is None:
        return 0.5
    return min(0.99, max(0.01, raw))


@dataclass(frozen=True)
class KalshiLiveVenueAdapter:
    env_data: dict[str, str]
    timeout_seconds: float = 10.0
    max_historical_pages: int = 3
    http_request_json: AuthenticatedRequester = _http_request_json
    sign_request: KalshiSigner = _kalshi_sign_request

    @classmethod
    def from_env_file(
        cls,
        *,
        env_file: str,
        timeout_seconds: float = 10.0,
        max_historical_pages: int = 3,
        http_request_json: AuthenticatedRequester = _http_request_json,
        sign_request: KalshiSigner = _kalshi_sign_request,
    ) -> "KalshiLiveVenueAdapter":
        env_path = Path(env_file)
        env_data = _parse_env_file(env_path)
        return cls(
            env_data=env_data,
            timeout_seconds=timeout_seconds,
            max_historical_pages=max_historical_pages,
            http_request_json=http_request_json,
            sign_request=sign_request,
        )

    def _signed_request(
        self,
        *,
        method: str,
        path_with_query: str,
        body: Any | None,
    ) -> tuple[int, Any]:
        return _signed_kalshi_request(
            env_data=self.env_data,
            method=method,
            path_with_query=path_with_query,
            body=body,
            timeout_seconds=self.timeout_seconds,
            http_request_json=self.http_request_json,
            sign_request=self.sign_request,
        )

    def _build_order_payload(self, ticket: TicketProposal) -> dict[str, Any]:
        side = str(ticket.side or "").strip().lower()
        if side not in {"yes", "no"}:
            side = "yes"
        payload: dict[str, Any] = {
            "ticker": ticket.market,
            "side": side,
            "action": "buy",
            "count": 1,
            "time_in_force": "good_till_canceled",
            "post_only": True,
            "cancel_order_on_pause": True,
            "self_trade_prevention_type": "maker",
            "client_order_id": f"runtime-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}-{uuid.uuid4().hex[:8]}",
        }
        price_key = "yes_price_dollars" if side == "yes" else "no_price_dollars"
        payload[price_key] = f"{_ticket_price_dollars(ticket):.4f}"
        return payload

    def submit_order(self, *, lane: str, ticket: TicketProposal) -> LiveVenueAck:
        status_code, payload = self._signed_request(
            method="POST",
            path_with_query="/portfolio/orders",
            body=self._build_order_payload(ticket),
        )
        order = payload.get("order") if isinstance(payload, dict) else None
        if status_code == 201 and isinstance(order, dict):
            order_id = str(order.get("order_id") or "").strip()
            if order_id:
                return LiveVenueAck(
                    accepted=True,
                    ack_status="accepted",
                    external_order_id=order_id,
                    reason="accepted",
                )
            return LiveVenueAck(
                accepted=False,
                ack_status="rejected",
                external_order_id=None,
                reason="submission_missing_order_id",
            )
        if status_code in _TRANSIENT_HTTP_STATUSES:
            return LiveVenueAck(
                accepted=False,
                ack_status="timeout",
                external_order_id=None,
                reason=_payload_reason(payload, fallback="submission_timeout"),
            )
        return LiveVenueAck(
            accepted=False,
            ack_status="rejected",
            external_order_id=None,
            reason=_payload_reason(payload, fallback=f"submission_rejected_http_{status_code}"),
        )

    def _historical_order_by_id(self, *, order_id: str) -> dict[str, Any] | None:
        cursor: str | None = None
        for _ in range(max(1, int(self.max_historical_pages))):
            query: dict[str, str] = {"limit": "200"}
            if cursor:
                query["cursor"] = cursor
            status_code, payload = self._signed_request(
                method="GET",
                path_with_query=f"/historical/orders?{urlencode(query)}",
                body=None,
            )
            if status_code != 200 or not isinstance(payload, dict):
                return None
            orders = payload.get("orders")
            if isinstance(orders, list):
                for row in orders:
                    if isinstance(row, dict) and str(row.get("order_id") or "").strip() == order_id:
                        return row
            next_cursor = str(payload.get("cursor") or "").strip()
            if not next_cursor:
                break
            cursor = next_cursor
        return None

    @staticmethod
    def _map_order_to_reconciliation(
        *,
        order: dict[str, Any],
        external_order_id: str,
    ) -> LiveReconciliationResult:
        status_raw = str(order.get("status") or "").strip().lower()
        filled = (
            _to_float(order.get("fill_count_fp"))
            or _to_float(order.get("fill_count"))
            or 0.0
        )
        remaining = _to_float(order.get("remaining_count_fp"))
        if remaining is None:
            remaining = _to_float(order.get("remaining_count"))
        if remaining is None:
            initial = (
                _to_float(order.get("initial_count_fp"))
                or _to_float(order.get("initial_count"))
                or _to_float(order.get("count"))
                or 1.0
            )
            remaining = max(0.0, float(initial) - float(filled))

        if filled > 0.0 and remaining > 0.0:
            return LiveReconciliationResult(
                status="partially_filled",
                reason="reconciled_partially_filled",
                filled_quantity=float(filled),
                remaining_quantity=float(remaining),
                mismatches=0,
                position_status="open",
                external_order_id=external_order_id,
            )
        if status_raw in _TERMINAL_FILLED_STATUSES or (filled > 0.0 and remaining <= 0.0):
            return LiveReconciliationResult(
                status="filled",
                reason="reconciled_filled",
                filled_quantity=float(max(filled, 1.0 if remaining <= 0 else filled)),
                remaining_quantity=max(0.0, float(remaining)),
                mismatches=0,
                position_status="open",
                external_order_id=external_order_id,
            )
        if status_raw in _TERMINAL_CANCELED_STATUSES:
            return LiveReconciliationResult(
                status="canceled",
                reason="reconciled_canceled",
                filled_quantity=float(filled),
                remaining_quantity=max(0.0, float(remaining)),
                mismatches=0,
                position_status="none",
                external_order_id=external_order_id,
            )
        if status_raw == "resting":
            return LiveReconciliationResult(
                status="resting",
                reason="reconciled_resting",
                filled_quantity=float(filled),
                remaining_quantity=max(0.0, float(remaining)),
                mismatches=0,
                position_status="none",
                external_order_id=external_order_id,
            )
        return LiveReconciliationResult(
            status="mismatch",
            reason="reconcile_mismatch",
            filled_quantity=float(filled),
            remaining_quantity=max(0.0, float(remaining)),
            mismatches=1,
            position_status="unknown",
            external_order_id=external_order_id,
        )

    def reconcile_order(
        self,
        *,
        lane: str,
        ticket: TicketProposal,
        external_order_id: str,
    ) -> LiveReconciliationResult:
        status_code, payload = self._signed_request(
            method="GET",
            path_with_query=f"/portfolio/orders/{external_order_id}",
            body=None,
        )
        order = payload.get("order") if isinstance(payload, dict) else None
        if status_code == 200 and isinstance(order, dict):
            return self._map_order_to_reconciliation(order=order, external_order_id=external_order_id)

        if status_code in {404, 410}:
            historical = self._historical_order_by_id(order_id=external_order_id)
            if isinstance(historical, dict):
                return self._map_order_to_reconciliation(order=historical, external_order_id=external_order_id)
            return LiveReconciliationResult(
                status="mismatch",
                reason="reconcile_order_missing",
                filled_quantity=0.0,
                remaining_quantity=0.0,
                mismatches=1,
                position_status="unknown",
                external_order_id=external_order_id,
            )

        if status_code in _TRANSIENT_HTTP_STATUSES:
            return LiveReconciliationResult(
                status="mismatch",
                reason=_payload_reason(payload, fallback="reconcile_timeout"),
                filled_quantity=0.0,
                remaining_quantity=0.0,
                mismatches=1,
                position_status="unknown",
                external_order_id=external_order_id,
            )

        return LiveReconciliationResult(
            status="mismatch",
            reason=_payload_reason(payload, fallback=f"reconcile_http_{status_code}"),
            filled_quantity=0.0,
            remaining_quantity=0.0,
            mismatches=1,
            position_status="unknown",
            external_order_id=external_order_id,
        )
