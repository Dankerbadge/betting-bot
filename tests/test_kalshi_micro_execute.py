import csv
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import URLError

from betbot.kalshi_book import ensure_book_schema
from betbot.kalshi_micro_execute import _http_request_json, _read_exchange_status, _signed_kalshi_request, run_kalshi_micro_execute
from betbot.kalshi_micro_ledger import LEDGER_FIELDNAMES


class KalshiMicroExecuteTests(unittest.TestCase):
    def test_http_request_json_returns_structured_error_on_url_error(self) -> None:
        with patch("betbot.kalshi_micro_execute.urlopen", side_effect=URLError("dns failed")):
            status_code, payload = _http_request_json(
                "https://example.com",
                "GET",
                {},
                None,
                5.0,
            )

        self.assertEqual(status_code, 599)
        self.assertEqual(payload["error_type"], "url_error")
        self.assertIn("dns failed", payload["error"])

    def test_http_request_json_retries_transient_dns_then_succeeds(self) -> None:
        class _FakeResponse:
            def __enter__(self) -> "_FakeResponse":
                return self

            def __exit__(self, exc_type, exc, tb) -> bool:
                return False

            def getcode(self) -> int:
                return 200

            def read(self) -> bytes:
                return b'{"ok": true}'

        with patch(
            "betbot.kalshi_micro_execute.urlopen",
            side_effect=[URLError("[Errno 8] nodename nor servname provided, or not known"), _FakeResponse()],
        ) as mock_urlopen, patch("betbot.kalshi_micro_execute.time.sleep") as mock_sleep:
            status_code, payload = _http_request_json(
                "https://example.com",
                "GET",
                {},
                None,
                5.0,
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload, {"ok": True})
        self.assertEqual(mock_urlopen.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)

    def test_http_request_json_limits_persistent_dns_retry_budget(self) -> None:
        with patch(
            "betbot.kalshi_micro_execute.urlopen",
            side_effect=[
                URLError("[Errno 8] nodename nor servname provided, or not known"),
                URLError("[Errno 8] nodename nor servname provided, or not known"),
                URLError("[Errno 8] nodename nor servname provided, or not known"),
            ],
        ) as mock_urlopen, patch("betbot.kalshi_micro_execute.time.sleep") as mock_sleep:
            status_code, payload = _http_request_json(
                "https://example.com",
                "GET",
                {},
                None,
                5.0,
            )

        self.assertEqual(status_code, 599)
        self.assertEqual(payload["error_type"], "url_error")
        self.assertIn("nodename nor servname", payload["error"])
        self.assertEqual(mock_urlopen.call_count, 2)
        self.assertEqual(mock_sleep.call_count, 1)

    def test_signed_kalshi_request_fails_over_api_root_on_retryable_network_error(self) -> None:
        requested_urls: list[str] = []

        def fake_http_request_json(
            url: str,
            method: str,
            headers: dict[str, str],
            body: object | None,
            timeout_seconds: float,
        ) -> tuple[int, object]:
            requested_urls.append(url)
            if "api.elections.kalshi.com" in url:
                return 599, {"error": "dns failed", "error_type": "url_error"}
            return 200, {"ok": True}

        status_code, payload = _signed_kalshi_request(
            env_data={
                "KALSHI_ENV": "prod",
                "KALSHI_ACCESS_KEY_ID": "key123",
                "KALSHI_PRIVATE_KEY_PATH": "/tmp/key.pem",
            },
            method="GET",
            path_with_query="/markets/KXTEST-1/orderbook?depth=1",
            body=None,
            timeout_seconds=5.0,
            http_request_json=fake_http_request_json,
            sign_request=lambda *_: "signed",
        )

        self.assertEqual(status_code, 200)
        self.assertTrue(requested_urls[0].startswith("https://api.elections.kalshi.com/"))
        self.assertTrue(any("https://trading-api.kalshi.com/" in value for value in requested_urls))
        self.assertEqual(payload["api_root_used"], "https://trading-api.kalshi.com/trade-api/v2")

    def test_read_exchange_status_fails_over_api_root_on_dns_error(self) -> None:
        requested_urls: list[str] = []

        def fake_http_request_json(
            url: str,
            method: str,
            headers: dict[str, str],
            body: object | None,
            timeout_seconds: float,
        ) -> tuple[int, object]:
            _ = method
            _ = headers
            _ = body
            _ = timeout_seconds
            requested_urls.append(url)
            if "api.elections.kalshi.com" in url:
                return 599, {"error": "dns failed", "error_type": "url_error"}
            return 200, {"trading_active": True, "exchange_active": True}

        result = _read_exchange_status(
            env_data={"KALSHI_ENV": "prod"},
            timeout_seconds=5.0,
            http_request_json=fake_http_request_json,
        )

        self.assertTrue(result["status_ok"])
        self.assertTrue(result["trading_active"])
        self.assertEqual(result["api_root_used"], "https://trading-api.kalshi.com/trade-api/v2")
        self.assertTrue(any("api.elections.kalshi.com" in value for value in requested_urls))
        self.assertTrue(any("trading-api.kalshi.com" in value for value in requested_urls))

    def test_run_kalshi_micro_execute_dry_run_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            calls: list[tuple[str, str]] = []

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                calls.append((method, url))
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.0200", "50.00"]],
                        "no_dollars": [["0.9700", "10.00"]],
                    }
                }

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 0.0,
                    "funding_gap_dollars": 0.02,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                }

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                http_request_json=fake_http_request_json,
                plan_runner=fake_plan_runner,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "dry_run")
            self.assertEqual(len(summary["attempts"]), 1)
            self.assertEqual(summary["attempts"][0]["result"], "dry_run_ready")
            self.assertEqual(calls, [("GET", "https://api.elections.kalshi.com/trade-api/v2/markets/KXTEST-1/orderbook?depth=1")])
            self.assertEqual(summary["ledger_summary_after"]["live_submissions_today"], 0)
            self.assertEqual(summary["ledger_summary_after"]["ledger_rows_total"], 0)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_execute_writes_execution_frontier_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                http_request_json=lambda *args, **kwargs: (
                    200,
                    {
                        "orderbook_fp": {
                            "yes_dollars": [["0.4200", "120.00"]],
                            "no_dollars": [["0.5600", "120.00"]],
                        }
                    },
                ),
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.42,
                    "actual_live_balance_dollars": 40.0,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Economics",
                            "market_ticker": "KXTEST-EDGE",
                            "side": "yes",
                            "contracts_per_order": 1,
                            "hours_to_close": 12.0,
                            "confidence": 0.72,
                            "maker_entry_price_dollars": 0.42,
                            "maker_yes_price_dollars": 0.42,
                            "yes_ask_dollars": 0.43,
                            "maker_entry_edge_conservative_net_total": 0.03,
                            "estimated_entry_cost_dollars": 0.42,
                            "order_payload_preview": {
                                "ticker": "KXTEST-EDGE",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.4200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["execution_frontier_status"], "insufficient_data")
            self.assertGreaterEqual(summary["execution_event_rows_written"], 3)
            self.assertEqual(summary["execution_policy_active_attempts"], 1)
            self.assertEqual(summary["execution_policy_submit_attempts"], 1)
            self.assertEqual(summary["blocked_execution_policy_attempts"], 0)
            self.assertTrue(Path(summary["execution_event_log_csv"]).exists())
            self.assertTrue(Path(summary["execution_frontier_summary_file"]).exists())
            self.assertTrue(Path(summary["execution_frontier_bucket_csv"]).exists())

    def test_run_kalshi_micro_execute_blocks_negative_ev_submit_policy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                http_request_json=lambda *args, **kwargs: (
                    200,
                    {
                        "orderbook_fp": {
                            "yes_dollars": [["0.5000", "1.00"]],
                            "no_dollars": [["0.4900", "1.00"]],
                        }
                    },
                ),
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.5,
                    "actual_live_balance_dollars": 40.0,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Economics",
                            "market_ticker": "KXTEST-LOWEDGE",
                            "side": "yes",
                            "contracts_per_order": 1,
                            "hours_to_close": 72.0,
                            "confidence": 0.55,
                            "maker_entry_price_dollars": 0.5,
                            "maker_yes_price_dollars": 0.5,
                            "yes_ask_dollars": 0.58,
                            "maker_entry_edge_conservative_net_total": 0.0001,
                            "estimated_entry_cost_dollars": 0.5,
                            "order_payload_preview": {
                                "ticker": "KXTEST-LOWEDGE",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.5000",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "dry_run_policy_blocked")
            self.assertEqual(summary["blocked_execution_policy_attempts"], 1)
            self.assertEqual(summary["attempts"][0]["result"], "blocked_execution_policy")
            self.assertEqual(summary["attempts"][0]["execution_policy_decision"], "skip")

    def test_run_kalshi_micro_execute_dry_run_marks_transient_orderbook_error_as_degraded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 599, {
                    "error": "[Errno 8] nodename nor servname provided, or not known",
                    "error_type": "url_error",
                }

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                }

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                http_request_json=fake_http_request_json,
                plan_runner=fake_plan_runner,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "dry_run_orderbook_degraded")
            self.assertEqual(len(summary["attempts"]), 1)
            self.assertEqual(summary["attempts"][0]["result"], "orderbook_unavailable")
            self.assertEqual(summary["attempts"][0]["orderbook_error_type"], "url_error")

    def test_run_kalshi_micro_execute_short_circuits_after_transient_orderbook_outage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            requested_tickers: list[str] = []

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                requested_tickers.append(url.rsplit("/", 1)[-1])
                return 599, {
                    "error": "[Errno 8] nodename nor servname provided, or not known",
                    "error_type": "url_error",
                }

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "planned_orders": 3,
                    "total_planned_cost_dollars": 0.06,
                    "actual_live_balance_dollars": 40.0,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "order_payload_preview": {"ticker": "KXTEST-1", "side": "yes", "count": 1},
                        },
                        {
                            "plan_rank": 2,
                            "category": "Politics",
                            "market_ticker": "KXTEST-2",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "order_payload_preview": {"ticker": "KXTEST-2", "side": "yes", "count": 1},
                        },
                        {
                            "plan_rank": 3,
                            "category": "Politics",
                            "market_ticker": "KXTEST-3",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "order_payload_preview": {"ticker": "KXTEST-3", "side": "yes", "count": 1},
                        },
                    ],
                }

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                http_request_json=fake_http_request_json,
                plan_runner=fake_plan_runner,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "dry_run_orderbook_degraded")
            self.assertGreaterEqual(len(requested_tickers), 1)
            self.assertLessEqual(len(requested_tickers), 2)
            self.assertEqual(len(summary["attempts"]), 1)
            self.assertTrue(summary["orderbook_outage_short_circuit_triggered"])
            self.assertEqual(summary["orderbook_outage_short_circuit_trigger_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["orderbook_outage_short_circuit_skipped_orders"], 2)

    def test_run_kalshi_micro_execute_uses_max_bid_level_when_orderbook_levels_are_sorted_ascending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.0100", "5.00"], ["0.0200", "50.00"]],
                        "no_dollars": [["0.9700", "5.00"], ["0.9800", "25.00"]],
                    }
                }

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 0.0,
                    "funding_gap_dollars": 0.02,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                }

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                http_request_json=fake_http_request_json,
                plan_runner=fake_plan_runner,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["attempts"][0]["current_best_yes_bid_dollars"], 0.02)
            self.assertEqual(summary["attempts"][0]["current_best_no_bid_dollars"], 0.98)

    def test_run_kalshi_micro_execute_live_submit_and_cancel(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            methods: list[str] = []

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                methods.append(method)
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    }
                if method == "POST" and url.endswith("/portfolio/orders"):
                    return 201, {
                        "order": {
                            "order_id": "order-123",
                            "status": "resting",
                        }
                    }
                if method == "GET" and url.endswith("/portfolio/orders/order-123/queue_position"):
                    return 200, {"queue_position_fp": "12.00"}
                if method == "DELETE" and url.endswith("/portfolio/orders/order-123"):
                    return 200, {"reduced_by_fp": "1.00", "order": {"order_id": "order-123"}}
                return 404, {"error": "not found"}

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                }

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                cancel_resting_immediately=True,
                http_request_json=fake_http_request_json,
                plan_runner=fake_plan_runner,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "live_submitted_and_canceled")
            self.assertEqual(summary["attempts"][0]["result"], "submitted_then_canceled")
            self.assertEqual(summary["attempts"][0]["queue_position_contracts"], 12.0)
            self.assertEqual(summary["ledger_summary_after"]["live_submissions_today"], 0)
            self.assertEqual(methods, ["GET", "POST", "GET", "DELETE"])

    def test_run_kalshi_micro_execute_supports_no_side_plans(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.0300", "50.00"]],
                        "no_dollars": [["0.9600", "10.00"]],
                    }
                }

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.96,
                    "actual_live_balance_dollars": 40.0,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "side": "no",
                            "maker_entry_price_dollars": 0.96,
                            "estimated_entry_cost_dollars": 0.96,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "no",
                                "action": "buy",
                                "count": 1,
                                "no_price_dollars": "0.9600",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                }

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                http_request_json=fake_http_request_json,
                plan_runner=fake_plan_runner,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "dry_run")
            self.assertEqual(summary["attempts"][0]["planned_side"], "no")
            self.assertEqual(summary["attempts"][0]["planned_entry_price_dollars"], 0.96)
            self.assertEqual(summary["attempts"][0]["current_best_same_side_bid_dollars"], 0.96)
            self.assertEqual(summary["attempts"][0]["result"], "dry_run_ready")

    def test_run_kalshi_micro_execute_blocks_submission_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            ledger_csv = base / "ledger.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with ledger_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=LEDGER_FIELDNAMES)
                writer.writeheader()
                writer.writerow(
                    {
                        "recorded_at": "2026-03-27T20:55:00+00:00",
                        "trading_day": "2026-03-27",
                        "run_mode": "live",
                        "live_write_allowed": "true",
                        "market_ticker": "KXOLD",
                        "plan_rank": "1",
                        "planned_yes_bid_dollars": "0.02",
                        "planned_yes_ask_dollars": "0.03",
                        "estimated_entry_cost_dollars": "0.02",
                        "result": "submitted",
                        "submission_http_status": "201",
                        "order_id": "order-1",
                        "order_status": "resting",
                        "queue_position_contracts": "",
                        "cancel_http_status": "",
                        "cancel_reduced_by_contracts": "",
                        "resting_hold_seconds": "0.0",
                        "counts_toward_live_submission": "true",
                    }
                )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.0200", "50.00"]],
                        "no_dollars": [["0.9700", "10.00"]],
                    }
                }

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                }

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                http_request_json=fake_http_request_json,
                plan_runner=fake_plan_runner,
                sign_request=lambda *_: "signed",
                ledger_csv=str(ledger_csv),
                max_live_submissions_per_day=1,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_submission_budget")
            self.assertEqual(summary["attempts"][0]["result"], "blocked_submission_budget")

    def test_run_kalshi_micro_execute_blocks_duplicate_open_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            book_db_path = base / "book.sqlite3"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            ensure_book_schema(book_db_path)
            with sqlite3.connect(book_db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, client_order_id, ticker, side, action, limit_price_dollars,
                        post_only, status, created_time, last_update_time, last_seen_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "existing-order-1",
                        "bot-existing-1",
                        "KXTEST-1",
                        "yes",
                        "buy",
                        0.02,
                        1,
                        "resting",
                        "2026-03-27T20:00:00Z",
                        "2026-03-27T20:00:00Z",
                        "2026-03-27T20:00:00+00:00",
                        "{}",
                    ),
                )
                conn.commit()

            methods: list[str] = []

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                methods.append(method)
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    }
                if method == "POST" and url.endswith("/portfolio/orders"):
                    return 201, {"order": {"order_id": "new-order-1", "status": "resting"}}
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                book_db_path=str(book_db_path),
                http_request_json=fake_http_request_json,
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "live_blocked_duplicate_open_orders")
            self.assertEqual(summary["blocked_duplicate_open_order_attempts"], 1)
            self.assertEqual(len(summary["duplicate_open_order_markets"]), 1)
            self.assertEqual(summary["duplicate_open_order_markets"][0]["market_ticker"], "KXTEST-1")
            self.assertEqual(summary["duplicate_open_order_markets"][0]["planned_side"], "yes")
            self.assertEqual(summary["duplicate_open_order_markets"][0]["duplicate_open_orders_count"], 1)
            self.assertEqual(summary["attempts"][0]["result"], "blocked_duplicate_open_order")
            self.assertEqual(summary["attempts"][0]["duplicate_open_orders_count"], 1)
            self.assertEqual(methods, ["GET"])

    def test_run_kalshi_micro_execute_janitor_cancels_excess_duplicates_then_submits(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            book_db_path = base / "book.sqlite3"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            ensure_book_schema(book_db_path)
            with sqlite3.connect(book_db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO orders (
                        order_id, client_order_id, ticker, side, action, limit_price_dollars,
                        post_only, status, created_time, last_update_time, last_seen_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            "existing-order-1",
                            "bot-existing-1",
                            "KXTEST-1",
                            "yes",
                            "buy",
                            0.02,
                            1,
                            "resting",
                            "2026-03-27T20:00:00Z",
                            "2026-03-27T20:00:00Z",
                            "2026-03-27T20:00:00+00:00",
                            "{}",
                        ),
                        (
                            "existing-order-2",
                            "bot-existing-2",
                            "KXTEST-1",
                            "yes",
                            "buy",
                            0.02,
                            1,
                            "resting",
                            "2026-03-27T20:01:00Z",
                            "2026-03-27T20:01:00Z",
                            "2026-03-27T20:01:00+00:00",
                            "{}",
                        ),
                    ],
                )
                conn.commit()

            methods: list[tuple[str, str]] = []

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                methods.append((method, url))
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    }
                if method == "DELETE" and url.endswith("/portfolio/orders/existing-order-1"):
                    return 200, {"reduced_by_fp": "0.00"}
                if method == "DELETE" and url.endswith("/portfolio/orders/existing-order-2"):
                    return 200, {"reduced_by_fp": "0.00"}
                if method == "POST" and url.endswith("/portfolio/orders"):
                    return 201, {"order": {"order_id": "new-order-1", "status": "booked"}}
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                book_db_path=str(book_db_path),
                http_request_json=fake_http_request_json,
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "live_submitted")
            self.assertEqual(summary["janitor_attempts"], 1)
            self.assertEqual(summary["janitor_canceled_open_orders_count"], 2)
            self.assertEqual(summary["janitor_cancel_failed_attempts"], 0)
            self.assertEqual(summary["attempts"][0]["janitor_attempted"], True)
            self.assertEqual(summary["attempts"][0]["result"], "submitted")
            self.assertEqual(summary["attempts"][0]["duplicate_open_orders_count"], 2)
            self.assertEqual(summary["attempts"][0]["duplicate_open_orders_count_after_janitor"], 0)
            self.assertEqual(
                methods,
                [
                    ("GET", "https://api.elections.kalshi.com/trade-api/v2/markets/KXTEST-1/orderbook?depth=1"),
                    ("DELETE", "https://api.elections.kalshi.com/trade-api/v2/portfolio/orders/existing-order-1"),
                    ("DELETE", "https://api.elections.kalshi.com/trade-api/v2/portfolio/orders/existing-order-2"),
                    ("POST", "https://api.elections.kalshi.com/trade-api/v2/portfolio/orders"),
                ],
            )

            with sqlite3.connect(book_db_path) as conn:
                statuses = dict(
                    conn.execute(
                        "SELECT order_id, status FROM orders WHERE order_id IN ('existing-order-1', 'existing-order-2')"
                    ).fetchall()
                )
            self.assertEqual(statuses["existing-order-1"], "canceled")
            self.assertEqual(statuses["existing-order-2"], "canceled")

    def test_run_kalshi_micro_execute_janitor_failure_sets_live_janitor_failed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            book_db_path = base / "book.sqlite3"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            ensure_book_schema(book_db_path)
            with sqlite3.connect(book_db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO orders (
                        order_id, client_order_id, ticker, side, action, limit_price_dollars,
                        post_only, status, created_time, last_update_time, last_seen_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            "existing-order-1",
                            "bot-existing-1",
                            "KXTEST-1",
                            "yes",
                            "buy",
                            0.02,
                            1,
                            "resting",
                            "2026-03-27T20:00:00Z",
                            "2026-03-27T20:00:00Z",
                            "2026-03-27T20:00:00+00:00",
                            "{}",
                        ),
                        (
                            "existing-order-2",
                            "bot-existing-2",
                            "KXTEST-1",
                            "yes",
                            "buy",
                            0.02,
                            1,
                            "resting",
                            "2026-03-27T20:01:00Z",
                            "2026-03-27T20:01:00Z",
                            "2026-03-27T20:01:00+00:00",
                            "{}",
                        ),
                    ],
                )
                conn.commit()

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    }
                if method == "DELETE" and url.endswith("/portfolio/orders/existing-order-1"):
                    return 200, {"reduced_by_fp": "0.00"}
                if method == "DELETE" and url.endswith("/portfolio/orders/existing-order-2"):
                    return 503, {"error": "service unavailable"}
                if method == "POST" and url.endswith("/portfolio/orders"):
                    return 201, {"order": {"order_id": "new-order-1", "status": "booked"}}
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                book_db_path=str(book_db_path),
                http_request_json=fake_http_request_json,
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "live_janitor_cancel_failed")
            self.assertEqual(summary["janitor_attempts"], 1)
            self.assertEqual(summary["janitor_cancel_failed_attempts"], 1)
            self.assertEqual(summary["attempts"][0]["result"], "janitor_cancel_failed")
            self.assertIn("existing-order-2", summary["attempts"][0]["janitor_cancel_error"])

    def test_run_kalshi_micro_execute_reports_cost_cap_when_mixed_with_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            book_db_path = base / "book.sqlite3"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            ensure_book_schema(book_db_path)
            with sqlite3.connect(book_db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, client_order_id, ticker, side, action, limit_price_dollars,
                        post_only, status, created_time, last_update_time, last_seen_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "existing-dup-1",
                        "bot-dup-1",
                        "KXDUP-1",
                        "yes",
                        "buy",
                        0.02,
                        1,
                        "resting",
                        "2026-03-27T20:00:00Z",
                        "2026-03-27T20:00:00Z",
                        "2026-03-27T20:00:00+00:00",
                        "{}",
                    ),
                )
                conn.commit()

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    }
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                max_live_cost_per_day_dollars=0.05,
                book_db_path=str(book_db_path),
                http_request_json=fake_http_request_json,
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 2,
                    "total_planned_cost_dollars": 0.12,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXCOST-1",
                            "maker_yes_price_dollars": 0.10,
                            "yes_ask_dollars": 0.11,
                            "estimated_entry_cost_dollars": 0.10,
                            "order_payload_preview": {
                                "ticker": "KXCOST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.1000",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        },
                        {
                            "plan_rank": 2,
                            "category": "Politics",
                            "market_ticker": "KXDUP-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXDUP-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        },
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_live_cost_cap")
            self.assertEqual(summary["blocked_live_cost_cap_attempts"], 1)
            self.assertEqual(summary["blocked_duplicate_open_order_attempts"], 1)
            self.assertEqual(summary["attempts"][0]["result"], "blocked_live_cost_cap")
            self.assertEqual(summary["attempts"][1]["result"], "blocked_duplicate_open_order")

    def test_run_kalshi_micro_execute_blocks_concurrent_live_execution_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            with patch("betbot.kalshi_micro_execute._acquire_live_execution_lock") as mock_lock:
                mock_lock.return_value = (None, False, "resource busy")
                summary = run_kalshi_micro_execute(
                    env_file=str(env_file),
                    output_dir=str(base),
                    allow_live_orders=True,
                    http_request_json=lambda *args, **kwargs: (
                        200,
                        {"orderbook_fp": {"yes_dollars": [["0.0200", "50.00"]], "no_dollars": [["0.9700", "10.00"]]}},
                    ),
                    plan_runner=lambda **kwargs: {
                        "status": "ready",
                        "planned_orders": 1,
                        "total_planned_cost_dollars": 0.02,
                        "actual_live_balance_dollars": 40.0,
                        "actual_live_balance_source": "live",
                        "balance_live_verified": True,
                        "funding_gap_dollars": 0.0,
                        "board_warning": None,
                        "output_file": str(base / "plan.json"),
                        "output_csv": str(base / "plan.csv"),
                        "orders": [
                            {
                                "plan_rank": 1,
                                "category": "Politics",
                                "market_ticker": "KXTEST-1",
                                "maker_yes_price_dollars": 0.02,
                                "yes_ask_dollars": 0.03,
                                "estimated_entry_cost_dollars": 0.02,
                                "order_payload_preview": {
                                    "ticker": "KXTEST-1",
                                    "side": "yes",
                                    "action": "buy",
                                    "count": 1,
                                    "yes_price_dollars": "0.0200",
                                    "time_in_force": "good_till_canceled",
                                    "post_only": True,
                                    "cancel_order_on_pause": True,
                                    "self_trade_prevention_type": "maker",
                                },
                            }
                        ],
                    },
                    sign_request=lambda *_: "signed",
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "blocked_concurrent_live_execution")
            self.assertFalse(summary["live_execution_lock_acquired"])
            self.assertEqual(summary["attempts"][0]["result"], "blocked_concurrent_live_execution")

    def test_run_kalshi_micro_execute_live_orderbook_unavailable_sets_explicit_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                http_request_json=lambda *args, **kwargs: (503, {"error": "upstream down"}),
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "live_orderbook_unavailable")
            self.assertEqual(summary["attempts"][0]["result"], "orderbook_unavailable")

    def test_run_kalshi_micro_execute_dry_run_missing_credentials_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=False,
                http_request_json=lambda *args, **kwargs: (
                    200,
                    {"orderbook_fp": {"yes_dollars": [["0.0200", "50.00"]], "no_dollars": [["0.9700", "10.00"]]}},
                ),
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "cache",
                    "balance_live_verified": False,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "upstream_error")
            self.assertEqual(summary["attempts"][0]["result"], "orderbook_unavailable")
            self.assertEqual(summary["attempts"][0]["orderbook_error_type"], "config_error")
            self.assertEqual(summary["attempts"][0]["orderbook_error"], "missing_kalshi_credentials")

    def test_run_kalshi_micro_execute_blocks_trade_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.0200", "50.00"]],
                        "no_dollars": [["0.9700", "10.00"]],
                    }
                }

            def fake_plan_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                }

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                enforce_trade_gate=True,
                history_csv=str(base / "history.csv"),
                http_request_json=fake_http_request_json,
                plan_runner=fake_plan_runner,
                quality_runner=lambda **kwargs: {"meaningful_markets": 0},
                signal_runner=lambda **kwargs: {"eligible_markets": 0},
                persistence_runner=lambda **kwargs: {"persistent_tradeable_markets": 0},
                delta_runner=lambda **kwargs: {
                    "board_change_label": "stale",
                    "improved_two_sided_markets": 0,
                    "newly_tradeable_markets": 0,
                },
                category_runner=lambda **kwargs: {
                    "tradeable_categories": 0,
                    "watch_categories": 1,
                    "top_categories": [{"category": "Politics", "category_label": "watch"}],
                    "concentration_warning": "Two-sided liquidity is heavily concentrated in Politics.",
                },
                pressure_runner=lambda **kwargs: {
                    "build_markets": 1,
                    "watch_markets": 0,
                    "top_build_market_ticker": "KXTEST-1",
                    "top_build_category": "Politics",
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_trade_gate")
            self.assertEqual(summary["attempts"][0]["result"], "blocked_trade_gate")
            self.assertFalse(summary["trade_gate_summary"]["gate_pass"])

    def test_run_kalshi_micro_execute_blocks_live_orders_without_live_verified_balance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                history_csv=str(base / "history.csv"),
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.96,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "cache",
                    "balance_live_verified": False,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                                "time_in_force": "good_till_canceled",
                                "post_only": True,
                                "cancel_order_on_pause": True,
                                "self_trade_prevention_type": "maker",
                            },
                        }
                    ],
                },
                http_request_json=lambda *args, **kwargs: (200, {}),
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_balance_unverified")
            self.assertEqual(summary["actual_live_balance_source"], "cache")

    def test_run_kalshi_micro_execute_blocks_exchange_inactive_when_trade_gate_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if method == "GET" and url.endswith("/exchange/status"):
                    return 200, {"trading_active": False, "exchange_active": False}
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    }
                return 404, {"error": "not found"}

            with patch(
                "betbot.kalshi_micro_execute.build_trade_gate_decision",
                return_value={
                    "gate_pass": True,
                    "gate_status": "pass",
                    "gate_score": 100.0,
                    "gate_blockers": [],
                },
            ):
                summary = run_kalshi_micro_execute(
                    env_file=str(env_file),
                    output_dir=str(base),
                    allow_live_orders=True,
                    enforce_trade_gate=True,
                    history_csv=str(base / "history.csv"),
                    http_request_json=fake_http_request_json,
                    plan_runner=lambda **kwargs: {
                        "status": "ready",
                        "planned_orders": 1,
                        "total_planned_cost_dollars": 0.02,
                        "actual_live_balance_dollars": 40.0,
                        "actual_live_balance_source": "live",
                        "balance_live_verified": True,
                        "funding_gap_dollars": 0.0,
                        "board_warning": None,
                        "output_file": str(base / "plan.json"),
                        "output_csv": str(base / "plan.csv"),
                        "orders": [
                            {
                                "plan_rank": 1,
                                "category": "Politics",
                                "market_ticker": "KXTEST-1",
                                "maker_yes_price_dollars": 0.02,
                                "yes_ask_dollars": 0.03,
                                "estimated_entry_cost_dollars": 0.02,
                                "order_payload_preview": {
                                    "ticker": "KXTEST-1",
                                    "side": "yes",
                                    "action": "buy",
                                    "count": 1,
                                    "yes_price_dollars": "0.0200",
                                },
                            }
                        ],
                    },
                    quality_runner=lambda **kwargs: {"meaningful_markets": 2},
                    signal_runner=lambda **kwargs: {"eligible_markets": 2},
                    persistence_runner=lambda **kwargs: {"persistent_tradeable_markets": 2},
                    delta_runner=lambda **kwargs: {
                        "board_change_label": "improving",
                        "improved_two_sided_markets": 2,
                        "newly_tradeable_markets": 1,
                    },
                    category_runner=lambda **kwargs: {
                        "tradeable_categories": 2,
                        "watch_categories": 0,
                        "top_categories": [{"category": "Politics", "category_label": "tradeable"}],
                        "concentration_warning": "",
                    },
                    pressure_runner=lambda **kwargs: {
                        "build_markets": 2,
                        "watch_markets": 0,
                        "top_build_market_ticker": "KXTEST-1",
                        "top_build_category": "Politics",
                    },
                    sign_request=lambda *_: "signed",
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "blocked_exchange_inactive")
            self.assertEqual(summary["attempts"][0]["result"], "blocked_exchange_inactive")
            self.assertTrue(summary["exchange_status"]["checked"])
            self.assertFalse(summary["exchange_status"]["trading_active"])

    def test_run_kalshi_micro_execute_blocks_when_ws_state_authority_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                enforce_ws_state_authority=True,
                history_csv=str(base / "history.csv"),
                http_request_json=lambda *args, **kwargs: (
                    200,
                    {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    },
                ),
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_ws_state_missing")
            self.assertEqual(summary["attempts"][0]["result"], "blocked_ws_state_missing")
            self.assertTrue(summary["ws_state_authority"]["checked"])
            self.assertEqual(summary["ws_state_authority"]["status"], "missing")

    def test_run_kalshi_micro_execute_blocks_when_ws_state_authority_is_upstream_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            ws_state_path = base / "kalshi_ws_state_latest.json"
            ws_state_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "status": "upstream_error",
                            "gate_pass": False,
                            "market_count": 0,
                            "desynced_market_count": 0,
                            "last_error": "[Errno 8] nodename nor servname provided, or not known",
                        }
                    }
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                enforce_ws_state_authority=True,
                ws_state_json=str(ws_state_path),
                history_csv=str(base / "history.csv"),
                http_request_json=lambda *args, **kwargs: (
                    200,
                    {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    },
                ),
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_ws_state_upstream_error")
            self.assertEqual(summary["attempts"][0]["result"], "blocked_ws_state_upstream_error")
            self.assertTrue(summary["ws_state_authority"]["checked"])
            self.assertEqual(summary["ws_state_authority"]["status"], "upstream_error")

    def test_run_kalshi_micro_execute_dry_run_reports_ws_state_authority_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            ws_state_path = base / "kalshi_ws_state_latest.json"
            ws_state_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "status": "upstream_error",
                            "gate_pass": False,
                            "market_count": 0,
                            "desynced_market_count": 0,
                            "last_error": "[Errno 8] nodename nor servname provided, or not known",
                        }
                    }
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=False,
                enforce_ws_state_authority=True,
                ws_state_json=str(ws_state_path),
                history_csv=str(base / "history.csv"),
                http_request_json=lambda *args, **kwargs: (
                    200,
                    {
                        "orderbook_fp": {
                            "yes_dollars": [["0.0200", "50.00"]],
                            "no_dollars": [["0.9700", "10.00"]],
                        }
                    },
                ),
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.02,
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "funding_gap_dollars": 0.0,
                    "board_warning": None,
                    "output_file": str(base / "plan.json"),
                    "output_csv": str(base / "plan.csv"),
                    "orders": [
                        {
                            "plan_rank": 1,
                            "category": "Politics",
                            "market_ticker": "KXTEST-1",
                            "maker_yes_price_dollars": 0.02,
                            "yes_ask_dollars": 0.03,
                            "estimated_entry_cost_dollars": 0.02,
                            "order_payload_preview": {
                                "ticker": "KXTEST-1",
                                "side": "yes",
                                "action": "buy",
                                "count": 1,
                                "yes_price_dollars": "0.0200",
                            },
                        }
                    ],
                },
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "dry_run")
            self.assertTrue(summary["ws_state_authority"]["checked"])
            self.assertEqual(summary["ws_state_authority"]["status"], "upstream_error")
            self.assertFalse(summary["ws_state_authority"]["gate_pass"])


if __name__ == "__main__":
    unittest.main()
