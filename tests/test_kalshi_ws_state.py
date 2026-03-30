import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest

from betbot.kalshi_ws_state import (
    KalshiWsStateEngine,
    load_ws_state_authority,
    normalize_ws_envelope,
    run_kalshi_ws_state_collect,
    run_kalshi_ws_state_replay,
)


class _FakeWebSocketClient:
    def __init__(
        self,
        *,
        url: str,
        headers: dict[str, str] | None = None,
        connect_timeout_seconds: float = 10.0,
        read_timeout_seconds: float = 1.0,
        scripted_messages: list[str] | None = None,
    ) -> None:
        self.url = url
        self.headers = dict(headers or {})
        self.connect_timeout_seconds = connect_timeout_seconds
        self.read_timeout_seconds = read_timeout_seconds
        self.scripted_messages = list(scripted_messages or [])
        self.sent_subscriptions: list[dict[str, object]] = []
        self.pings_sent = 0
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def send_json(self, payload: dict[str, object]) -> None:
        self.sent_subscriptions.append(dict(payload))

    def send_ping(self, payload: bytes = b"ping") -> None:
        _ = payload
        self.pings_sent += 1

    def read_text_message(self, *, timeout_seconds: float | None = None) -> str | None:
        _ = timeout_seconds
        if self.scripted_messages:
            return self.scripted_messages.pop(0)
        return None

    def close(self) -> None:
        self.connected = False


class _MaybeFailingWebSocketClient(_FakeWebSocketClient):
    def __init__(
        self,
        *,
        url: str,
        headers: dict[str, str] | None = None,
        connect_timeout_seconds: float = 10.0,
        read_timeout_seconds: float = 1.0,
        scripted_messages: list[str] | None = None,
        fail_connect: bool = False,
    ) -> None:
        super().__init__(
            url=url,
            headers=headers,
            connect_timeout_seconds=connect_timeout_seconds,
            read_timeout_seconds=read_timeout_seconds,
            scripted_messages=scripted_messages,
        )
        self.fail_connect = fail_connect

    def connect(self) -> None:
        if self.fail_connect:
            raise OSError("[Errno 8] nodename nor servname provided, or not known")
        super().connect()


class _FakeWebSocketFactory:
    def __init__(self, scripted_messages: list[str]) -> None:
        self.scripted_messages = list(scripted_messages)
        self.clients: list[_FakeWebSocketClient] = []

    def __call__(
        self,
        *,
        url: str,
        headers: dict[str, str] | None = None,
        connect_timeout_seconds: float = 10.0,
        read_timeout_seconds: float = 1.0,
    ) -> _FakeWebSocketClient:
        client = _FakeWebSocketClient(
            url=url,
            headers=headers,
            connect_timeout_seconds=connect_timeout_seconds,
            read_timeout_seconds=read_timeout_seconds,
            scripted_messages=self.scripted_messages,
        )
        self.scripted_messages = []
        self.clients.append(client)
        return client


class _FailoverWebSocketFactory:
    def __init__(self, *, scripted_messages: list[str], fail_connect_urls: set[str]) -> None:
        self.scripted_messages = list(scripted_messages)
        self.fail_connect_urls = set(fail_connect_urls)
        self.clients: list[_MaybeFailingWebSocketClient] = []

    def __call__(
        self,
        *,
        url: str,
        headers: dict[str, str] | None = None,
        connect_timeout_seconds: float = 10.0,
        read_timeout_seconds: float = 1.0,
    ) -> _MaybeFailingWebSocketClient:
        client = _MaybeFailingWebSocketClient(
            url=url,
            headers=headers,
            connect_timeout_seconds=connect_timeout_seconds,
            read_timeout_seconds=read_timeout_seconds,
            scripted_messages=self.scripted_messages,
            fail_connect=url in self.fail_connect_urls,
        )
        self.clients.append(client)
        return client


class _FailingWebSocketClient:
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
        self.connect_timeout_seconds = connect_timeout_seconds
        self.read_timeout_seconds = read_timeout_seconds

    def connect(self) -> None:
        raise OSError("[Errno 8] nodename nor servname provided, or not known")

    def close(self) -> None:
        return None


class KalshiWsStateTests(unittest.TestCase):
    def test_engine_applies_snapshot_and_delta_with_sequence_integrity(self) -> None:
        engine = KalshiWsStateEngine(max_staleness_seconds=60.0)
        start = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        engine.ingest_event(
            {
                "event_type": "orderbook_snapshot",
                "captured_at_utc": start.isoformat(),
                "ticker": "KXTEST-1",
                "sequence": 10,
                "orderbook_fp": {
                    "yes_dollars": [["0.4200", "100.00"]],
                    "no_dollars": [["0.5600", "120.00"]],
                },
            }
        )
        engine.ingest_event(
            {
                "event_type": "orderbook_delta",
                "captured_at_utc": (start + timedelta(seconds=1)).isoformat(),
                "ticker": "KXTEST-1",
                "sequence": 11,
                "yes_dollars_delta": [["0.4200", "80.00"], ["0.4300", "40.00"]],
                "no_dollars_delta": [["0.5600", "100.00"]],
            }
        )
        summary = engine.health_summary(now=start + timedelta(seconds=2))
        self.assertEqual(summary["status"], "ready")
        self.assertEqual(summary["desynced_market_count"], 0)
        self.assertEqual(summary["market_count"], 1)
        top = engine.market_books["KXTEST-1"]["top_of_book"]
        self.assertEqual(top["best_yes_bid_dollars"], 0.43)

    def test_engine_marks_desync_on_sequence_gap(self) -> None:
        engine = KalshiWsStateEngine(max_staleness_seconds=60.0)
        start = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        engine.ingest_event(
            {
                "event_type": "orderbook_snapshot",
                "captured_at_utc": start.isoformat(),
                "ticker": "KXTEST-1",
                "sequence": 4,
                "orderbook_fp": {
                    "yes_dollars": [["0.4200", "100.00"]],
                    "no_dollars": [["0.5600", "120.00"]],
                },
            }
        )
        engine.ingest_event(
            {
                "event_type": "orderbook_delta",
                "captured_at_utc": (start + timedelta(seconds=1)).isoformat(),
                "ticker": "KXTEST-1",
                "sequence": 7,
                "yes_dollars_delta": [["0.4200", "80.00"]],
            }
        )
        summary = engine.health_summary(now=start + timedelta(seconds=2))
        self.assertEqual(summary["status"], "desynced")
        self.assertEqual(summary["desynced_market_count"], 1)

    def test_engine_ignores_delta_before_snapshot(self) -> None:
        engine = KalshiWsStateEngine(max_staleness_seconds=60.0)
        start = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
        engine.ingest_event(
            {
                "event_type": "orderbook_delta",
                "captured_at_utc": start.isoformat(),
                "ticker": "KXTEST-1",
                "sequence": 7,
                "yes_dollars_delta": [["0.4200", "80.00"]],
            }
        )
        summary = engine.health_summary(now=start + timedelta(seconds=1))
        self.assertEqual(summary["status"], "empty")
        self.assertEqual(summary["market_count"], 0)
        self.assertEqual(summary["desynced_market_count"], 0)

    def test_run_ws_state_replay_writes_state_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            events_path = base / "events.ndjson"
            start = datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc)
            events_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "event_type": "orderbook_snapshot",
                                "captured_at_utc": start.isoformat(),
                                "ticker": "KXTEST-1",
                                "sequence": 1,
                                "orderbook_fp": {
                                    "yes_dollars": [["0.4200", "100.00"]],
                                    "no_dollars": [["0.5600", "120.00"]],
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "event_type": "orderbook_delta",
                                "captured_at_utc": (start + timedelta(seconds=1)).isoformat(),
                                "ticker": "KXTEST-1",
                                "sequence": 2,
                                "yes_dollars_delta": [["0.4300", "25.00"]],
                            }
                        ),
                    ]
                ),
                encoding="utf-8",
            )
            summary = run_kalshi_ws_state_replay(
                events_ndjson=str(events_path),
                output_dir=str(base),
                now=start + timedelta(seconds=2),
            )
            self.assertEqual(summary["status"], "ready")
            self.assertTrue(Path(summary["ws_state_json"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_normalize_ws_envelope_maps_orderbook_cents_to_dollars(self) -> None:
        normalized = normalize_ws_envelope(
            envelope={
                "type": "orderbook_snapshot",
                "seq": 42,
                "msg": {
                    "market_ticker": "KXTEST-1",
                    "yes": [[42, 100], [41, 80]],
                    "no": [[58, 120]],
                },
            },
            captured_at=datetime(2026, 3, 29, 12, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(len(normalized), 1)
        payload = normalized[0]
        self.assertEqual(payload["event_type"], "orderbook_snapshot")
        self.assertEqual(payload["sequence"], 42)
        orderbook = payload["orderbook_fp"]
        self.assertEqual(orderbook["yes_dollars"][0][0], "0.4200")
        self.assertEqual(orderbook["no_dollars"][0][0], "0.5800")

    def test_run_ws_state_collect_captures_events_with_fake_socket_client(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_path = base / "kalshi.env"
            env_path.write_text(
                "\n".join(
                    [
                        "KALSHI_ENV=demo",
                        "KALSHI_ACCESS_KEY_ID=test-access-key",
                        "KALSHI_PRIVATE_KEY_PATH=/tmp/test-private-key.pem",
                    ]
                ),
                encoding="utf-8",
            )
            scripted_messages = [
                json.dumps(
                    {
                        "type": "orderbook_snapshot",
                        "seq": 1,
                        "msg": {
                            "market_ticker": "KXTEST-1",
                            "yes": [[42, 100]],
                            "no": [[58, 80]],
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "orderbook_delta",
                        "seq": 2,
                        "msg": {
                            "market_ticker": "KXTEST-1",
                            "yes": [[43, 60]],
                            "no": [[57, 90]],
                        },
                    }
                ),
            ]
            factory = _FakeWebSocketFactory(scripted_messages)
            summary = run_kalshi_ws_state_collect(
                env_file=str(env_path),
                channels=("orderbook_snapshot", "orderbook_delta"),
                market_tickers=("KXTEST-1",),
                output_dir=str(base),
                run_seconds=1.0,
                max_events=2,
                max_staleness_seconds=60.0,
                websocket_client_factory=factory,
                sign_request=lambda private_key_path, timestamp_ms, method, path: "fake-signature",
                sleep_fn=lambda seconds: None,
            )
            self.assertEqual(summary["events_logged"], 2)
            self.assertEqual(summary["status"], "ready")
            self.assertTrue(Path(summary["ws_state_json"]).exists())
            self.assertTrue(Path(summary["ws_events_ndjson"]).exists())
            self.assertEqual(len(factory.clients), 1)
            first_client = factory.clients[0]
            self.assertEqual(first_client.url, "wss://demo-api.kalshi.co/trade-api/ws/v2")
            self.assertGreaterEqual(len(first_client.sent_subscriptions), 2)
            ws_state_payload = json.loads(Path(summary["ws_state_json"]).read_text(encoding="utf-8"))
            market = ws_state_payload["markets"]["KXTEST-1"]
            self.assertEqual(market["top_of_book"]["best_yes_bid_dollars"], 0.43)

    def test_run_ws_state_collect_fails_over_ws_url_on_dns_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_path = base / "kalshi.env"
            env_path.write_text(
                "\n".join(
                    [
                        "KALSHI_ENV=prod",
                        "KALSHI_ACCESS_KEY_ID=test-access-key",
                        "KALSHI_PRIVATE_KEY_PATH=/tmp/test-private-key.pem",
                    ]
                ),
                encoding="utf-8",
            )
            factory = _FailoverWebSocketFactory(
                scripted_messages=[
                    json.dumps(
                        {
                            "type": "orderbook_snapshot",
                            "seq": 1,
                            "msg": {
                                "market_ticker": "KXTEST-FAILOVER-1",
                                "yes": [[42, 80]],
                                "no": [[58, 70]],
                            },
                        }
                    ),
                ],
                fail_connect_urls={"wss://api.elections.kalshi.com/trade-api/ws/v2"},
            )
            summary = run_kalshi_ws_state_collect(
                env_file=str(env_path),
                channels=("orderbook_snapshot",),
                market_tickers=("KXTEST-FAILOVER-1",),
                output_dir=str(base),
                run_seconds=1.0,
                max_events=1,
                max_staleness_seconds=60.0,
                reconnect_max_attempts=0,
                websocket_client_factory=factory,
                sign_request=lambda *_: "fake-signature",
                sleep_fn=lambda seconds: None,
            )
            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["connection_attempts"], 2)
            self.assertEqual(summary["reconnects"], 0)
            self.assertEqual(summary["ws_url_used"], "wss://trading-api.kalshi.com/trade-api/ws/v2")
            self.assertEqual(
                summary["ws_urls_attempted"],
                [
                    "wss://api.elections.kalshi.com/trade-api/ws/v2",
                    "wss://trading-api.kalshi.com/trade-api/ws/v2",
                ],
            )

    def test_load_ws_state_authority_flags_stale_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            state_path = base / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "status": "ready",
                            "market_count": 1,
                            "desynced_market_count": 0,
                            "last_event_at": "2026-03-29T12:00:00+00:00",
                        }
                    }
                ),
                encoding="utf-8",
            )
            authority = load_ws_state_authority(
                ws_state_json=state_path,
                captured_at=datetime(2026, 3, 29, 12, 10, tzinfo=timezone.utc),
                max_staleness_seconds=30.0,
            )
            self.assertEqual(authority["status"], "stale")
            self.assertFalse(authority["gate_pass"])

    def test_run_ws_state_collect_reports_upstream_error_on_connect_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_path = base / "kalshi.env"
            env_path.write_text(
                "\n".join(
                    [
                        "KALSHI_ENV=demo",
                        "KALSHI_ACCESS_KEY_ID=test-access-key",
                        "KALSHI_PRIVATE_KEY_PATH=/tmp/test-private-key.pem",
                    ]
                ),
                encoding="utf-8",
            )
            summary = run_kalshi_ws_state_collect(
                env_file=str(env_path),
                channels=("orderbook_snapshot",),
                output_dir=str(base),
                run_seconds=1.0,
                reconnect_max_attempts=0,
                websocket_client_factory=_FailingWebSocketClient,
                sign_request=lambda *_: "fake-signature",
                sleep_fn=lambda seconds: None,
            )
            self.assertEqual(summary["status"], "upstream_error")
            self.assertFalse(summary["gate_pass"])
            self.assertIn("nodename nor servname", summary["last_error"])
            self.assertEqual(summary["last_error_kind"], "dns_resolution_error")
            self.assertEqual(summary["ws_url_failover_error_kind_counts"], {"dns_resolution_error": 1})
            self.assertEqual(summary["ws_url_failover_error_counts_by_url"], {"wss://demo-api.kalshi.co/trade-api/ws/v2": 1})

            authority = load_ws_state_authority(
                ws_state_json=summary["ws_state_json"],
                captured_at=datetime(2026, 3, 29, 12, 10, tzinfo=timezone.utc),
                max_staleness_seconds=30.0,
            )
            self.assertEqual(authority["status"], "upstream_error")
            self.assertEqual(authority["reason"], "ws_state_upstream_error")
            self.assertEqual(authority["last_error_kind"], "dns_resolution_error")
            self.assertFalse(authority["gate_pass"])

    def test_run_ws_state_collect_preserves_previous_ready_state_on_upstream_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_path = base / "kalshi.env"
            env_path.write_text(
                "\n".join(
                    [
                        "KALSHI_ENV=demo",
                        "KALSHI_ACCESS_KEY_ID=test-access-key",
                        "KALSHI_PRIVATE_KEY_PATH=/tmp/test-private-key.pem",
                    ]
                ),
                encoding="utf-8",
            )
            ws_state_path = base / "kalshi_ws_state_latest.json"
            ws_state_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "status": "ready",
                            "gate_pass": True,
                            "market_count": 1,
                            "desynced_market_count": 0,
                            "events_processed": 3,
                            "last_event_at": "2026-03-29T12:04:55+00:00",
                        },
                        "markets": {
                            "KXTEST-PREVIOUS-1": {
                                "ticker": "KXTEST-PREVIOUS-1",
                                "top_of_book": {
                                    "best_yes_bid_dollars": 0.42,
                                    "best_no_bid_dollars": 0.57,
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_ws_state_collect(
                env_file=str(env_path),
                channels=("orderbook_snapshot",),
                output_dir=str(base),
                ws_state_json=str(ws_state_path),
                max_staleness_seconds=30.0,
                run_seconds=1.0,
                reconnect_max_attempts=0,
                websocket_client_factory=_FailingWebSocketClient,
                sign_request=lambda *_: "fake-signature",
                sleep_fn=lambda seconds: None,
                now=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            )
            self.assertTrue(summary["fallback_state_used"])
            self.assertEqual(summary["fallback_state_reason"], "preserved_previous_ready_state")
            self.assertEqual(summary["status_before_fallback"], "upstream_error")
            self.assertEqual(summary["status"], "ready")
            self.assertTrue(summary["gate_pass"])
            self.assertIn("nodename nor servname", summary["last_error"])
            self.assertEqual(summary["last_error_kind"], "dns_resolution_error")
            self.assertEqual(summary["ws_url_failover_error_kind_counts"], {"dns_resolution_error": 1})
            self.assertEqual(summary["ws_url_failover_error_counts_by_url"], {"wss://demo-api.kalshi.co/trade-api/ws/v2": 1})

            authority = load_ws_state_authority(
                ws_state_json=summary["ws_state_json"],
                captured_at=datetime(2026, 3, 29, 12, 5, 10, tzinfo=timezone.utc),
                max_staleness_seconds=30.0,
            )
            self.assertEqual(authority["status"], "ready")
            self.assertTrue(authority["gate_pass"])

    def test_run_ws_state_collect_caps_failed_connect_attempts_by_reconnect_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_path = base / "kalshi.env"
            env_path.write_text(
                "\n".join(
                    [
                        "KALSHI_ENV=demo",
                        "KALSHI_ACCESS_KEY_ID=test-access-key",
                        "KALSHI_PRIVATE_KEY_PATH=/tmp/test-private-key.pem",
                    ]
                ),
                encoding="utf-8",
            )
            summary = run_kalshi_ws_state_collect(
                env_file=str(env_path),
                channels=("orderbook_snapshot",),
                output_dir=str(base),
                run_seconds=60.0,
                reconnect_max_attempts=2,
                websocket_client_factory=_FailingWebSocketClient,
                sign_request=lambda *_: "fake-signature",
                sleep_fn=lambda seconds: None,
            )
            self.assertEqual(summary["status"], "upstream_error")
            self.assertEqual(summary["connection_attempts"], 3)
            self.assertEqual(summary["reconnects"], 3)
            self.assertEqual(summary["last_error_kind"], "dns_resolution_error")
            self.assertEqual(summary["ws_url_failover_error_kind_counts"], {"dns_resolution_error": 3})
            self.assertEqual(summary["ws_url_failover_error_counts_by_url"], {"wss://demo-api.kalshi.co/trade-api/ws/v2": 3})

    def test_run_ws_state_collect_auto_discovers_market_tickers_from_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_path = base / "kalshi.env"
            env_path.write_text(
                "\n".join(
                    [
                        "KALSHI_ENV=demo",
                        "KALSHI_ACCESS_KEY_ID=test-access-key",
                        "KALSHI_PRIVATE_KEY_PATH=/tmp/test-private-key.pem",
                    ]
                ),
                encoding="utf-8",
            )
            (base / "kalshi_micro_prior_plan_summary_20260329_120000.json").write_text(
                json.dumps(
                    {
                        "top_market_ticker": "KXAUTO-1",
                        "top_plans": [{"market_ticker": "KXAUTO-1"}],
                    }
                ),
                encoding="utf-8",
            )
            scripted_messages = [
                json.dumps(
                    {
                        "type": "orderbook_snapshot",
                        "seq": 1,
                        "msg": {
                            "market_ticker": "KXAUTO-1",
                            "yes": [[45, 80]],
                            "no": [[55, 90]],
                        },
                    }
                ),
            ]
            factory = _FakeWebSocketFactory(scripted_messages)
            summary = run_kalshi_ws_state_collect(
                env_file=str(env_path),
                channels=("orderbook_snapshot",),
                output_dir=str(base),
                run_seconds=1.0,
                max_events=1,
                max_staleness_seconds=60.0,
                websocket_client_factory=factory,
                sign_request=lambda private_key_path, timestamp_ms, method, path: "fake-signature",
                sleep_fn=lambda seconds: None,
            )
            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["market_tickers_explicit"], [])
            self.assertEqual(summary["market_tickers_auto_discovered"], ["KXAUTO-1"])
            self.assertEqual(summary["market_tickers"], ["KXAUTO-1"])
            self.assertEqual(len(factory.clients), 1)
            first_client = factory.clients[0]
            self.assertEqual(len(first_client.sent_subscriptions), 1)
            subscribe_payload = first_client.sent_subscriptions[0]
            params = subscribe_payload.get("params")
            self.assertIsInstance(params, dict)
            if isinstance(params, dict):
                self.assertEqual(params.get("market_tickers"), ["KXAUTO-1"])


if __name__ == "__main__":
    unittest.main()
