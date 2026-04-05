from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from urllib.parse import urlparse

from betbot.execution.kalshi_live_venue_adapter import KalshiLiveVenueAdapter
from betbot.execution.ticket import create_ticket_proposal


class _FakeRequester:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self._responses: dict[tuple[str, str], tuple[int, object]] = {}

    def set_response(self, method: str, path: str, status_code: int, payload: object) -> None:
        self._responses[(method.upper(), path)] = (status_code, payload)

    def __call__(
        self,
        url: str,
        method: str,
        headers: dict[str, str],
        body: object | None,
        timeout_seconds: float,
    ) -> tuple[int, object]:
        path = urlparse(url).path
        query = urlparse(url).query
        path_with_query = f"{path}?{query}" if query else path
        key = (method.upper(), path_with_query)
        self.calls.append(key)
        return self._responses.get(key, (404, {"error_type": "not_found", "error": "missing"}))


def _fake_signer(private_key_path: str, timestamp_ms: str, method: str, path: str) -> str:
    return "fake-signature"


def _build_adapter(requester: _FakeRequester) -> KalshiLiveVenueAdapter:
    return KalshiLiveVenueAdapter(
        env_data={
            "KALSHI_ENV": "demo",
            "KALSHI_ACCESS_KEY_ID": "abc",
            "KALSHI_PRIVATE_KEY_PATH": "/tmp/fake.pem",
        },
        timeout_seconds=5.0,
        http_request_json=requester,
        sign_request=_fake_signer,
    )


class KalshiLiveVenueAdapterTests(unittest.TestCase):
    def test_submit_order_accepted_returns_external_order_id(self) -> None:
        requester = _FakeRequester()
        requester.set_response(
            "POST",
            "/trade-api/v2/portfolio/orders",
            201,
            {"order": {"order_id": "ord-accepted", "status": "resting"}},
        )
        adapter = _build_adapter(requester)
        ticket = create_ticket_proposal(
            market="MKT-ACCEPT",
            side="yes",
            max_cost=0.42,
            lane="live_execute",
            source_run_id="run-1",
        )
        ack = adapter.submit_order(lane="live_execute", ticket=ticket)
        self.assertTrue(ack.accepted)
        self.assertEqual(ack.ack_status, "accepted")
        self.assertEqual(ack.external_order_id, "ord-accepted")

    def test_submit_order_rejected_maps_to_rejected_ack(self) -> None:
        requester = _FakeRequester()
        requester.set_response(
            "POST",
            "/trade-api/v2/portfolio/orders",
            422,
            {"error_type": "invalid_order", "error": "price_out_of_bounds"},
        )
        adapter = _build_adapter(requester)
        ticket = create_ticket_proposal(
            market="MKT-REJECT",
            side="no",
            max_cost=0.95,
            lane="live_execute",
            source_run_id="run-2",
        )
        ack = adapter.submit_order(lane="live_execute", ticket=ticket)
        self.assertFalse(ack.accepted)
        self.assertEqual(ack.ack_status, "rejected")
        self.assertEqual(ack.reason, "invalid_order")
        self.assertIsNone(ack.external_order_id)

    def test_submit_order_timeout_maps_to_timeout_ack(self) -> None:
        requester = _FakeRequester()
        requester.set_response(
            "POST",
            "/trade-api/v2/portfolio/orders",
            599,
            {"error_type": "timeout_error", "error": "timed out"},
        )
        adapter = _build_adapter(requester)
        ticket = create_ticket_proposal(
            market="MKT-TIMEOUT",
            side="yes",
            max_cost=0.5,
            lane="live_execute",
            source_run_id="run-3",
        )
        ack = adapter.submit_order(lane="live_execute", ticket=ticket)
        self.assertFalse(ack.accepted)
        self.assertEqual(ack.ack_status, "timeout")
        self.assertEqual(ack.reason, "timeout_error")
        self.assertIsNone(ack.external_order_id)

    def test_reconcile_order_from_current_endpoint(self) -> None:
        requester = _FakeRequester()
        requester.set_response(
            "GET",
            "/trade-api/v2/portfolio/orders/ord-partial",
            200,
            {
                "order": {
                    "order_id": "ord-partial",
                    "status": "resting",
                    "fill_count_fp": "0.4",
                    "remaining_count_fp": "0.6",
                }
            },
        )
        adapter = _build_adapter(requester)
        ticket = create_ticket_proposal(
            market="MKT-PARTIAL",
            side="yes",
            max_cost=0.5,
            lane="live_execute",
            source_run_id="run-4",
        )
        result = adapter.reconcile_order(
            lane="live_execute",
            ticket=ticket,
            external_order_id="ord-partial",
        )
        self.assertEqual(result.status, "partially_filled")
        self.assertEqual(result.reason, "reconciled_partially_filled")
        self.assertEqual(result.filled_quantity, 0.4)
        self.assertEqual(result.remaining_quantity, 0.6)

    def test_reconcile_order_uses_historical_fallback(self) -> None:
        requester = _FakeRequester()
        requester.set_response(
            "GET",
            "/trade-api/v2/portfolio/orders/ord-hist",
            404,
            {"error_type": "not_found"},
        )
        requester.set_response(
            "GET",
            "/trade-api/v2/historical/orders?limit=200",
            200,
            {
                "orders": [
                    {
                        "order_id": "ord-hist",
                        "status": "executed",
                        "fill_count_fp": "1.0",
                        "remaining_count_fp": "0.0",
                    }
                ]
            },
        )
        adapter = _build_adapter(requester)
        ticket = create_ticket_proposal(
            market="MKT-HIST",
            side="no",
            max_cost=0.5,
            lane="live_execute",
            source_run_id="run-5",
        )
        result = adapter.reconcile_order(
            lane="live_execute",
            ticket=ticket,
            external_order_id="ord-hist",
        )
        self.assertEqual(result.status, "filled")
        self.assertEqual(result.reason, "reconciled_filled")
        self.assertEqual(result.filled_quantity, 1.0)
        self.assertEqual(result.remaining_quantity, 0.0)

    def test_from_env_file_loads_env_values(self) -> None:
        requester = _FakeRequester()
        with tempfile.TemporaryDirectory() as tmp:
            env_path = Path(tmp) / "live.env"
            env_path.write_text(
                "\n".join(
                    [
                        "KALSHI_ENV=demo",
                        "KALSHI_ACCESS_KEY_ID=abc",
                        "KALSHI_PRIVATE_KEY_PATH=/tmp/test.pem",
                    ]
                ),
                encoding="utf-8",
            )
            adapter = KalshiLiveVenueAdapter.from_env_file(
                env_file=str(env_path),
                http_request_json=requester,
                sign_request=_fake_signer,
            )
        self.assertEqual(adapter.env_data.get("KALSHI_ENV"), "demo")
        self.assertEqual(adapter.env_data.get("KALSHI_ACCESS_KEY_ID"), "abc")


if __name__ == "__main__":
    unittest.main()
