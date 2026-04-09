import csv
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import URLError

from betbot.kalshi_execution_journal import append_execution_events
from betbot.kalshi_book import ensure_book_schema
from betbot.kalshi_micro_execute import (
    _build_climate_router_pilot_execute_funnel,
    _execution_policy_metrics,
    _build_empirical_fill_training_rows,
    _http_request_json,
    _estimate_empirical_fill_probabilities_from_rows,
    _load_latest_break_even_edges_by_bucket,
    _read_exchange_status,
    _should_allow_untrusted_bucket_probe,
    _signed_kalshi_request,
    run_kalshi_micro_execute,
)
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

    def test_run_kalshi_micro_execute_preserves_explicit_client_order_id(self) -> None:
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

            submitted_payloads: list[dict[str, object]] = []

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                _ = headers
                _ = timeout_seconds
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.4200", "120.00"]],
                            "no_dollars": [["0.5600", "120.00"]],
                        }
                    }
                if method == "POST" and url.endswith("/portfolio/orders"):
                    if isinstance(body, dict):
                        submitted_payloads.append(dict(body))
                    return 201, {"order": {"order_id": "order-temp-1", "status": "executed"}}
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                http_request_json=fake_http_request_json,
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.42,
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
                            "category": "Climate",
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
                                "client_order_id": "temp-fixed-client-id",
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

            self.assertIn(summary["status"], {"live_submitted", "live_submitted_and_canceled"})
            self.assertEqual(len(submitted_payloads), 1)
            self.assertEqual(submitted_payloads[0].get("client_order_id"), "temp-fixed-client-id")
            self.assertEqual(summary["attempts"][0]["client_order_id"], "temp-fixed-client-id")

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

    def test_run_kalshi_micro_execute_blends_empirical_fill_probabilities_from_frontier_bucket(self) -> None:
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
            frontier_bucket = "aggr_mid|spread_mid|ttc_short"
            (base / "execution_frontier_report_20260327_205959_000.json").write_text(
                json.dumps(
                    {
                        "trusted_break_even_edge_by_bucket": {frontier_bucket: 0.02},
                        "bucket_markout_trust_by_bucket": {
                            frontier_bucket: {
                                "trusted": True,
                                "reason": "ready",
                                "markout_10s_samples": 8,
                                "markout_60s_samples": 9,
                                "markout_300s_samples": 6,
                            }
                        },
                        "bucket_rows": [
                            {
                                "bucket": frontier_bucket,
                                "orders_submitted": 24,
                                "fill_rate": 0.84,
                                "full_fill_rate": 0.62,
                                "median_time_to_fill_seconds": 40.0,
                                "p90_time_to_fill_seconds": 120.0,
                            }
                        ],
                    }
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

            attempt = summary["attempts"][0]
            self.assertEqual(attempt["execution_fill_probability_source"], "blended_empirical")
            self.assertGreater(float(attempt["execution_fill_probability_model_weight_empirical"]), 0.0)
            self.assertEqual(attempt["execution_empirical_orders_submitted_bucket"], 24)
            self.assertEqual(attempt["execution_empirical_fill_rate_bucket"], 0.84)

    def test_load_latest_break_even_edges_by_bucket_marks_stale_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            report_path = base / "execution_frontier_report_20260327_205959_000.json"
            report_path.write_text(
                json.dumps(
                    {
                        "trusted_break_even_edge_by_bucket": {"aggr_mid|spread_mid|ttc_short": 0.02},
                        "bucket_markout_trust_by_bucket": {},
                        "bucket_rows": [],
                    }
                ),
                encoding="utf-8",
            )
            old_ts = datetime(2026, 3, 27, 20, 0, tzinfo=timezone.utc).timestamp()
            os.utime(report_path, (old_ts, old_ts))

            edges, reference_file, trust_map, fill_profiles, meta = _load_latest_break_even_edges_by_bucket(
                str(base),
                max_report_age_seconds=60.0,
                as_of=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(edges, {})
            self.assertEqual(trust_map, {})
            self.assertEqual(fill_profiles, {})
            self.assertEqual(reference_file, str(report_path))
            self.assertTrue(meta.get("stale"))
            self.assertTrue(str(meta.get("stale_reason") or "").startswith("frontier_report_stale:"))

    def test_estimate_empirical_fill_probabilities_from_rows_returns_learned_profile(self) -> None:
        training_rows = [
            {
                "frontier_bucket": "aggr_mid|spread_mid|ttc_short",
                "age_seconds": 1200.0,
                "quote_aggressiveness": 0.55,
                "order_size_depth_ratio": 0.15,
                "queue_ahead_estimate_contracts": 6.0,
                "market_spread_dollars": 0.02,
                "market_hours_to_close": 14.0,
                "first_fill_seconds": 22.0,
                "full_fill_seconds": 46.0,
            },
            {
                "frontier_bucket": "aggr_mid|spread_mid|ttc_short",
                "age_seconds": 2400.0,
                "quote_aggressiveness": 0.53,
                "order_size_depth_ratio": 0.18,
                "queue_ahead_estimate_contracts": 8.0,
                "market_spread_dollars": 0.018,
                "market_hours_to_close": 13.0,
                "first_fill_seconds": 80.0,
                "full_fill_seconds": None,
            },
            {
                "frontier_bucket": "aggr_mid|spread_mid|ttc_short",
                "age_seconds": 3600.0,
                "quote_aggressiveness": 0.56,
                "order_size_depth_ratio": 0.2,
                "queue_ahead_estimate_contracts": 5.0,
                "market_spread_dollars": 0.019,
                "market_hours_to_close": 11.0,
                "first_fill_seconds": None,
                "full_fill_seconds": None,
            },
        ]
        profile = _estimate_empirical_fill_probabilities_from_rows(
            training_rows=training_rows,
            frontier_bucket="aggr_mid|spread_mid|ttc_short",
            attempt={
                "quote_aggressiveness": 0.54,
                "order_size_depth_ratio": 0.17,
                "queue_ahead_estimate_contracts": 7.0,
                "market_spread_dollars": 0.019,
                "market_hours_to_close": 12.0,
            },
            horizon_seconds=180.0,
            min_effective_samples=2.0,
        )
        self.assertIsInstance(profile, dict)
        assert isinstance(profile, dict)
        self.assertEqual(profile.get("source"), "learned_hazard")
        self.assertGreater(float(profile.get("effective_samples") or 0.0), 1.0)
        self.assertGreater(float(profile.get("fill_prob_60s") or 0.0), 0.0)
        self.assertGreaterEqual(float(profile.get("fill_prob_horizon") or 0.0), float(profile.get("full_fill_prob_horizon") or 0.0))

    def test_build_empirical_fill_training_rows_parses_journal_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            journal_path = base / "kalshi_execution_journal.sqlite3"
            append_execution_events(
                journal_db_path=journal_path,
                events=[
                    {
                        "run_id": "test-run",
                        "captured_at_utc": "2026-03-27T21:00:00+00:00",
                        "event_type": "order_submitted",
                        "market_ticker": "KXTEST-EDGE",
                        "client_order_id": "client-1",
                        "execution_frontier_bucket": "aggr_mid|spread_mid|ttc_short",
                        "quote_aggressiveness": 0.55,
                        "order_size_depth_ratio": 0.2,
                        "queue_ahead_estimate_contracts": 8.0,
                        "spread_dollars": 0.02,
                        "time_to_close_seconds": 7200,
                    },
                    {
                        "run_id": "test-run",
                        "captured_at_utc": "2026-03-27T21:00:45+00:00",
                        "event_type": "full_fill",
                        "market_ticker": "KXTEST-EDGE",
                        "client_order_id": "client-1",
                    },
                ],
            )

            rows = _build_empirical_fill_training_rows(
                journal_db_path=journal_path,
                as_of=datetime(2026, 3, 27, 22, 0, tzinfo=timezone.utc),
                lookback_days=7.0,
                recent_events=100,
            )

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["frontier_bucket"], "aggr_mid|spread_mid|ttc_short")
            self.assertAlmostEqual(float(rows[0]["first_fill_seconds"]), 45.0, places=3)

    def test_run_kalshi_micro_execute_allows_guarded_untrusted_bucket_probe_submission(self) -> None:
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
            (base / "execution_frontier_report_20260327_205959_000.json").write_text(
                json.dumps(
                    {
                        "trusted_break_even_edge_by_bucket": {
                            "aggr_passive|spread_mid|ttc_short": 0.02
                        },
                        "bucket_markout_trust_by_bucket": {
                            "aggr_mid|spread_mid|ttc_short": {
                                "trusted": False,
                                "reason": "markout_10s_samples_below_min:1<3;markout_60s_samples_below_min:1<3",
                                "markout_10s_samples": 1,
                                "markout_60s_samples": 1,
                                "markout_300s_samples": 1,
                            }
                        },
                        "bucket_rows": [
                            {
                                "bucket": "aggr_mid|spread_mid|ttc_short",
                                "orders_submitted": 9,
                                "fill_rate": 0.55,
                                "full_fill_rate": 0.4,
                                "median_time_to_fill_seconds": 65.0,
                                "p90_time_to_fill_seconds": 160.0,
                            }
                        ],
                    }
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
                _ = headers
                _ = body
                _ = timeout_seconds
                if method == "GET" and url.endswith("/exchange/status"):
                    return 200, {"trading_active": True, "exchange_active": True}
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.4200", "120.00"]],
                            "no_dollars": [["0.5600", "120.00"]],
                        }
                    }
                if method == "POST" and url.endswith("/portfolio/orders"):
                    return 201, {"order": {"order_id": "order-probe", "status": "executed"}}
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                enforce_trade_gate=True,
                http_request_json=fake_http_request_json,
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.42,
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
                            "category": "Climate",
                            "canonical_niche": "weather_climate",
                            "market_ticker": "KXTEST-RAIN",
                            "side": "yes",
                            "contracts_per_order": 1,
                            "hours_to_close": 12.0,
                            "confidence": 0.7,
                            "maker_entry_price_dollars": 0.42,
                            "maker_yes_price_dollars": 0.42,
                            "yes_ask_dollars": 0.43,
                            "maker_entry_edge_conservative_net_total": 0.08,
                            "estimated_entry_cost_dollars": 0.42,
                            "order_payload_preview": {
                                "ticker": "KXTEST-RAIN",
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
                quality_runner=lambda **kwargs: {"meaningful_markets": 2},
                signal_runner=lambda **kwargs: {"eligible_markets": 2},
                persistence_runner=lambda **kwargs: {"persistent_tradeable_markets": 1},
                delta_runner=lambda **kwargs: {
                    "improved_two_sided_markets": 1,
                    "newly_tradeable_markets": 0,
                    "board_change_label": "improving",
                },
                category_runner=lambda **kwargs: {
                    "tradeable_categories": 1,
                    "watch_categories": 0,
                    "top_categories": [{"category": "Climate", "category_label": "Climate"}],
                    "concentration_warning": "",
                },
                pressure_runner=lambda **kwargs: {"build_markets": 1, "watch_markets": 0},
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertIn(summary["status"], {"live_submitted", "live_submitted_and_canceled"})
            self.assertEqual(summary["untrusted_bucket_probe_submitted_attempts"], 1)
            self.assertEqual(
                summary["untrusted_bucket_probe_reason_counts"].get("probe_submit_untrusted_bucket_sample_depth"),
                1,
            )
            self.assertTrue(summary["attempts"][0]["execution_untrusted_bucket_probe"])
            self.assertEqual(summary["attempts"][0]["execution_policy_reason"], "submit_probe_untrusted_bucket")

    def test_run_kalshi_micro_execute_allows_climate_router_pilot_frontier_bootstrap_probe(self) -> None:
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
            (base / "execution_frontier_report_20260327_205959_000.json").write_text(
                json.dumps(
                    {
                        "trusted_break_even_edge_by_bucket": {},
                        "bucket_markout_trust_by_bucket": {},
                        "bucket_rows": [],
                    }
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
                _ = headers
                _ = body
                _ = timeout_seconds
                if method == "GET" and url.endswith("/exchange/status"):
                    return 200, {"trading_active": True, "exchange_active": True}
                if method == "GET" and url.endswith("/orderbook?depth=1"):
                    return 200, {
                        "orderbook_fp": {
                            "yes_dollars": [["0.4200", "120.00"]],
                            "no_dollars": [["0.5600", "120.00"]],
                        }
                    }
                if method == "POST" and url.endswith("/portfolio/orders"):
                    return 201, {"order": {"order_id": "order-bootstrap", "status": "executed"}}
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_execute(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                enforce_trade_gate=True,
                http_request_json=fake_http_request_json,
                plan_runner=lambda **kwargs: {
                    "status": "ready",
                    "planned_orders": 1,
                    "total_planned_cost_dollars": 0.69,
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
                            "category": "Climate",
                            "canonical_niche": "weather_climate",
                            "contract_family": "daily_rain",
                            "market_ticker": "KXTEST-RAIN",
                            "side": "yes",
                            "contracts_per_order": 1,
                            "hours_to_close": 12.0,
                            "confidence": 0.7,
                            "maker_entry_price_dollars": 0.42,
                            "maker_yes_price_dollars": 0.42,
                            "yes_ask_dollars": 0.43,
                            "maker_entry_edge_conservative_net_total": 0.08,
                            "estimated_entry_cost_dollars": 0.42,
                            "source_strategy": "climate_router_pilot",
                            "router_opportunity_class": "tradable_positive",
                            "router_expected_value_dollars": 0.1708,
                            "order_payload_preview": {
                                "ticker": "KXTEST-RAIN",
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
                quality_runner=lambda **kwargs: {"meaningful_markets": 2},
                signal_runner=lambda **kwargs: {"eligible_markets": 2},
                persistence_runner=lambda **kwargs: {"persistent_tradeable_markets": 1},
                delta_runner=lambda **kwargs: {
                    "improved_two_sided_markets": 1,
                    "newly_tradeable_markets": 0,
                    "board_change_label": "improving",
                },
                category_runner=lambda **kwargs: {
                    "tradeable_categories": 1,
                    "watch_categories": 0,
                    "top_categories": [{"category": "Climate", "category_label": "Climate"}],
                    "concentration_warning": "",
                },
                pressure_runner=lambda **kwargs: {"build_markets": 1, "watch_markets": 0},
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertIn(summary["status"], {"live_submitted", "live_submitted_and_canceled"})
            self.assertEqual(summary["climate_router_pilot_frontier_bootstrap_submitted_attempts"], 1)
            self.assertEqual(summary["attempts"][0]["execution_frontier_bootstrap_probe"], True)
            self.assertEqual(
                summary["attempts"][0]["execution_policy_reason"],
                "submit_pilot_frontier_bootstrap_probe_frontier_insufficient_data",
            )

    def test_execution_policy_metrics_prefers_learned_hazard_when_trusted(self) -> None:
        metrics = _execution_policy_metrics(
            plan={
                "contracts_per_order": 1,
                "hours_to_close": 12.0,
                "maker_entry_edge_conservative_net_total": 0.03,
                "confidence": 0.7,
            },
            attempt={
                "planned_side": "yes",
                "planned_contracts": 1,
                "planned_entry_price_dollars": 0.42,
                "current_best_same_side_bid_dollars": 0.41,
                "current_best_same_side_bid_size_contracts": 120.0,
                "current_best_yes_bid_dollars": 0.41,
                "market_hours_to_close": 12.0,
            },
            orderbook={"best_yes_ask_dollars": 0.43, "http_status": 200},
            resting_hold_seconds=60.0,
            empirical_fill_profile={
                "source": "learned_hazard",
                "effective_samples": 12.0,
                "rows_used": 24,
                "fill_prob_10s": 0.25,
                "fill_prob_60s": 0.72,
                "fill_prob_300s": 0.90,
                "fill_prob_horizon": 0.78,
                "full_fill_prob_horizon": 0.62,
                "partial_fill_prob_horizon": 0.16,
                "markout_trusted": True,
            },
            empirical_fill_min_effective_samples=6.0,
            prefer_empirical_fill_model=True,
        )

        self.assertEqual(metrics["execution_fill_probability_source"], "empirical_primary_learned_hazard")
        self.assertEqual(float(metrics["execution_fill_probability_model_weight_empirical"]), 1.0)
        self.assertEqual(float(metrics["execution_fill_probability_model_weight_heuristic"]), 0.0)

    def test_should_allow_untrusted_bucket_probe_requires_edge_buffer_and_budget(self) -> None:
        attempt = {
            "canonical_niche": "weather_climate",
            "planned_contracts": 1,
            "execution_policy_decision": "submit",
            "execution_forecast_edge_net_per_contract_dollars": 0.08,
            "execution_break_even_edge_per_contract_dollars": 0.05,
        }
        bucket_trust = {
            "trusted": False,
            "reason": "markout_10s_samples_below_min:1<3;markout_60s_samples_below_min:1<3",
        }
        allowed, reason = _should_allow_untrusted_bucket_probe(
            attempt=attempt,
            bucket_trust=bucket_trust,
            probe_enabled=True,
            probe_budget_remaining=1,
            required_edge_buffer_dollars=0.01,
            contracts_cap=1,
        )
        self.assertTrue(allowed)
        self.assertEqual(reason, "probe_submit_untrusted_bucket_sample_depth")

        blocked, blocked_reason = _should_allow_untrusted_bucket_probe(
            attempt={**attempt, "execution_forecast_edge_net_per_contract_dollars": 0.055},
            bucket_trust=bucket_trust,
            probe_enabled=True,
            probe_budget_remaining=1,
            required_edge_buffer_dollars=0.01,
            contracts_cap=1,
        )
        self.assertFalse(blocked)
        self.assertTrue(blocked_reason.startswith("probe_edge_buffer_not_met:"))

    def test_build_climate_router_pilot_execute_funnel_splits_dry_run_from_policy_scope(self) -> None:
        funnel = _build_climate_router_pilot_execute_funnel(
            [
                {
                    "source_strategy": "climate_router_pilot",
                    "result": "dry_run_ready",
                    "run_mode": "dry_run",
                    "allow_live_orders_requested": False,
                    "execution_policy_decision": "submit",
                    "execution_policy_reason": "positive_ev_submit",
                }
            ]
        )

        self.assertEqual(funnel["climate_router_pilot_execute_considered_rows"], 1)
        self.assertFalse(funnel["climate_router_pilot_live_mode_enabled"])
        self.assertEqual(funnel["climate_router_pilot_live_eligible_rows"], 0)
        self.assertEqual(funnel["climate_router_pilot_would_attempt_live_if_enabled"], 1)
        self.assertEqual(funnel["climate_router_pilot_blocked_dry_run_only_rows"], 1)
        self.assertEqual(funnel["climate_router_pilot_non_policy_gates_passed_rows"], 1)
        self.assertEqual(funnel["climate_router_pilot_attempted_orders"], 0)
        self.assertEqual(funnel["climate_router_pilot_blocked_research_dry_run_only"], 1)
        self.assertEqual(funnel["climate_router_pilot_blocked_policy_scope"], 0)
        self.assertEqual(
            funnel["climate_router_pilot_blocked_research_dry_run_only_reason_counts"],
            {"would_attempt_live_if_enabled": 1},
        )
        self.assertEqual(
            funnel["climate_router_pilot_blocked_post_promotion_reason_counts"].get(
                "blocked_research_dry_run_only"
            ),
            1,
        )

    def test_build_climate_router_pilot_execute_funnel_keeps_policy_scope_for_live_mode(self) -> None:
        funnel = _build_climate_router_pilot_execute_funnel(
            [
                {
                    "source_strategy": "climate_router_pilot",
                    "result": "blocked_trade_gate",
                    "run_mode": "live",
                    "allow_live_orders_requested": True,
                    "execution_policy_decision": "submit",
                    "execution_policy_reason": "",
                }
            ]
        )

        self.assertTrue(funnel["climate_router_pilot_live_mode_enabled"])
        self.assertEqual(funnel["climate_router_pilot_live_eligible_rows"], 0)
        self.assertEqual(funnel["climate_router_pilot_would_attempt_live_if_enabled"], 0)
        self.assertEqual(funnel["climate_router_pilot_blocked_dry_run_only_rows"], 0)
        self.assertEqual(funnel["climate_router_pilot_blocked_research_dry_run_only"], 0)
        self.assertEqual(funnel["climate_router_pilot_blocked_policy_scope"], 1)

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
