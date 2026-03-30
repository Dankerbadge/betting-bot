from datetime import datetime, timezone
import json
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_prior_trader import run_kalshi_micro_prior_trader


class KalshiMicroPriorTraderTests(unittest.TestCase):
    def test_run_kalshi_micro_prior_trader_holds_when_capture_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "upstream_error",
                    "scan_error": "<urlopen error [Errno 8] nodename nor servname provided, or not known>",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                    "scan_search_retries_total": 2,
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "upstream_error")
            self.assertEqual(summary["capture_error_kind"], "dns_resolution_error")
            self.assertIn("prior execution was skipped", summary["status_reason"])
            self.assertIn("dns_resolution_error", summary["status_reason"])
            self.assertIn("Scan retries used: 2.", summary["status_reason"])
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_prior_trader_disables_live_orders_without_fresh_capture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            allow_live_orders_seen: list[bool] = []

            def fake_prior_execute_runner(**kwargs):
                allow_live_orders_seen.append(bool(kwargs.get("allow_live_orders")))
                return {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 88.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXTEST-1",
                        "top_market_title": "Test Market",
                        "top_market_close_time": "2026-03-28T12:00:00Z",
                        "top_market_hours_to_close": 15.0,
                        "top_market_side": "no",
                        "top_market_maker_entry_price_dollars": 0.96,
                        "top_market_maker_entry_edge": 0.02,
                        "top_market_estimated_entry_cost_dollars": 0.96,
                        "top_market_expected_value_dollars": 0.02,
                        "top_market_expected_roi_on_cost": 0.020833,
                        "top_market_expected_value_per_day_dollars": 0.032,
                        "top_market_expected_roi_per_day": 0.033333,
                        "top_market_estimated_max_profit_dollars": 0.04,
                        "top_market_estimated_max_loss_dollars": 0.96,
                        "top_market_max_profit_roi_on_cost": 0.041667,
                        "top_market_fair_probability": 0.98,
                        "top_market_confidence": 0.7,
                        "top_market_thesis": "Test thesis",
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                }

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                allow_live_orders=True,
                capture_before_execute=False,
                prior_execute_runner=fake_prior_execute_runner,
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(allow_live_orders_seen, [False])
            self.assertTrue(summary["allow_live_orders_requested"])
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertTrue(summary["capture_required_for_live_orders"])
            self.assertIn("downgraded to dry-run", str(summary["live_orders_downgraded_reason"]))
            self.assertEqual(summary["status"], "dry_run")
            self.assertFalse(summary["ready_for_live_order"])
            self.assertIn("downgraded to dry-run", str(summary["ready_for_live_order_reason"]))

    def test_run_kalshi_micro_prior_trader_dry_run_reports_top_no_market(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            watch_history = base / "watch_history.csv"
            watch_history.write_text(
                (
                    "recorded_at,capture_status,capture_scan_status,status_recommendation,status_trade_gate_status,"
                    "trade_gate_pass,meaningful_candidates_yes_bid_ge_0_05,persistent_tradeable_markets,"
                    "improved_two_sided_markets,pressure_build_markets,threshold_approaching_markets,"
                    "top_pressure_market_ticker,top_threshold_market_ticker,board_change_label,top_category,"
                    "top_category_label,category_concentration_warning\n"
                    "2026-03-27T20:00:00+00:00,status_only,dry_run,review_prior_edge,no_meaningful_candidates,"
                    "false,0,0,0,1,0,KXTEST-1,,stale,Politics,watch,\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                watch_history_csv=str(watch_history),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                    "scan_page_requests": 4,
                    "scan_rate_limit_retries_used": 0,
                    "scan_network_retries_used": 1,
                    "scan_transient_http_retries_used": 0,
                    "scan_search_retries_total": 1,
                    "scan_search_health_status": "degraded_retrying",
                    "scan_events_fetched": 20,
                    "scan_markets_ranked": 7,
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "orderbook_outage_short_circuit_triggered": True,
                    "orderbook_outage_short_circuit_trigger_market_ticker": "KXTEST-1",
                    "orderbook_outage_short_circuit_skipped_orders": 2,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 88.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXTEST-1",
                        "top_market_title": "Test Market",
                        "top_market_close_time": "2026-03-28T12:00:00Z",
                        "top_market_hours_to_close": 15.0,
                        "top_market_side": "no",
                        "top_market_maker_entry_price_dollars": 0.96,
                        "top_market_maker_entry_edge": 0.02,
                        "top_market_estimated_entry_cost_dollars": 0.96,
                        "top_market_expected_value_dollars": 0.02,
                        "top_market_expected_roi_on_cost": 0.020833,
                        "top_market_expected_value_per_day_dollars": 0.032,
                        "top_market_expected_roi_per_day": 0.033333,
                        "top_market_estimated_max_profit_dollars": 0.04,
                        "top_market_estimated_max_loss_dollars": 0.96,
                        "top_market_max_profit_roi_on_cost": 0.041667,
                        "top_market_fair_probability": 0.98,
                        "top_market_confidence": 0.7,
                        "top_market_thesis": "Test thesis",
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "dry_run")
            self.assertEqual(summary["action_taken"], "dry_run_execute_reconcile")
            self.assertEqual(summary["capture_scan_page_requests"], 4)
            self.assertEqual(summary["capture_scan_network_retries_used"], 1)
            self.assertEqual(summary["capture_scan_search_retries_total"], 1)
            self.assertEqual(summary["capture_scan_search_health_status"], "degraded_retrying")
            self.assertEqual(summary["capture_scan_events_fetched"], 20)
            self.assertEqual(summary["capture_scan_markets_ranked"], 7)
            self.assertIn("janitor_attempts", summary)
            self.assertIn("janitor_canceled_open_orders_count", summary)
            self.assertIn("janitor_cancel_failed_attempts", summary)
            self.assertTrue(summary["orderbook_outage_short_circuit_triggered"])
            self.assertEqual(summary["orderbook_outage_short_circuit_trigger_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["orderbook_outage_short_circuit_skipped_orders"], 2)
            self.assertEqual(summary["top_market_side"], "no")
            self.assertEqual(summary["top_market_maker_entry_edge"], 0.02)
            self.assertEqual(summary["top_market_hours_to_close"], 15.0)
            self.assertEqual(summary["top_market_maker_entry_price_dollars"], 0.96)
            self.assertEqual(summary["top_market_estimated_entry_cost_dollars"], 0.96)
            self.assertEqual(summary["top_market_expected_value_dollars"], 0.02)
            self.assertAlmostEqual(summary["top_market_expected_roi_on_cost"], 0.020833, places=6)
            self.assertEqual(summary["top_market_estimated_max_profit_dollars"], 0.04)
            self.assertAlmostEqual(summary["top_market_max_profit_roi_on_cost"], 0.041667, places=6)
            self.assertEqual(summary["top_market_fair_probability"], 0.98)
            self.assertEqual(summary["reconcile_status"], "no_order_ids")
            self.assertTrue(summary["ready_for_live_order"])
            self.assertFalse(summary["ready_for_auto_live_order"])
            self.assertTrue(Path(summary["output_file"]).exists())
            persisted = json.loads(Path(summary["output_file"]).read_text(encoding="utf-8"))
            self.assertEqual(persisted["output_file"], summary["output_file"])

    def test_run_kalshi_micro_prior_trader_requires_live_verified_balance_for_live_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "cache",
                    "balance_live_verified": False,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 88.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXTEST-1",
                        "top_market_title": "Test Market",
                        "top_market_close_time": "2026-03-28T12:00:00Z",
                        "top_market_hours_to_close": 15.0,
                        "top_market_side": "no",
                        "top_market_maker_entry_price_dollars": 0.96,
                        "top_market_maker_entry_edge": 0.02,
                        "top_market_estimated_entry_cost_dollars": 0.96,
                        "top_market_expected_value_dollars": 0.02,
                        "top_market_expected_roi_on_cost": 0.020833,
                        "top_market_expected_value_per_day_dollars": 0.032,
                        "top_market_expected_roi_per_day": 0.033333,
                        "top_market_estimated_max_profit_dollars": 0.04,
                        "top_market_estimated_max_loss_dollars": 0.96,
                        "top_market_max_profit_roi_on_cost": 0.041667,
                        "top_market_fair_probability": 0.98,
                        "top_market_confidence": 0.7,
                        "top_market_thesis": "Test thesis",
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertFalse(summary["ready_for_live_order"])
            self.assertIn("live-verified", summary["ready_for_live_order_reason"])

    def test_run_kalshi_micro_prior_trader_blocks_live_readiness_when_ws_state_authority_is_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                enforce_ws_state_authority=True,
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "enforce_ws_state_authority": True,
                    "ws_state_authority": {
                        "checked": True,
                        "status": "upstream_error",
                        "gate_pass": False,
                    },
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 88.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXTEST-1",
                        "top_market_title": "Test Market",
                        "top_market_close_time": "2026-03-28T12:00:00Z",
                        "top_market_hours_to_close": 15.0,
                        "top_market_side": "no",
                        "top_market_maker_entry_price_dollars": 0.96,
                        "top_market_maker_entry_edge": 0.02,
                        "top_market_estimated_entry_cost_dollars": 0.96,
                        "top_market_expected_value_dollars": 0.02,
                        "top_market_expected_roi_on_cost": 0.020833,
                        "top_market_expected_value_per_day_dollars": 0.032,
                        "top_market_expected_roi_per_day": 0.033333,
                        "top_market_estimated_max_profit_dollars": 0.04,
                        "top_market_estimated_max_loss_dollars": 0.96,
                        "top_market_max_profit_roi_on_cost": 0.041667,
                        "top_market_fair_probability": 0.98,
                        "top_market_confidence": 0.7,
                        "top_market_thesis": "Test thesis",
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertFalse(summary["ready_for_live_order"])
            self.assertEqual(
                summary["ready_for_live_order_reason"],
                "Websocket-state authority is not healthy yet (upstream_error).",
            )

    def test_run_kalshi_micro_prior_trader_allows_macro_probe_auto_live_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 88.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXMACRO-1",
                        "top_market_title": "Macro Test Market",
                        "top_market_close_time": "2026-04-05T12:00:00Z",
                        "top_market_hours_to_close": 120.0,
                        "top_market_side": "no",
                        "top_market_canonical_niche": "macro_release",
                        "top_market_maker_entry_price_dollars": 0.45,
                        "top_market_maker_entry_edge": 0.03,
                        "top_market_estimated_entry_cost_dollars": 0.45,
                        "top_market_expected_value_dollars": 0.03,
                        "top_market_expected_roi_on_cost": 0.066667,
                        "top_market_expected_value_per_day_dollars": 0.01,
                        "top_market_expected_roi_per_day": 0.016667,
                        "top_market_estimated_max_profit_dollars": 0.55,
                        "top_market_estimated_max_loss_dollars": 0.45,
                        "top_market_max_profit_roi_on_cost": 1.222222,
                        "top_market_fair_probability": 0.48,
                        "top_market_confidence": 0.7,
                        "top_market_thesis": "Macro probe thesis",
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertTrue(summary["ready_for_live_order"])
            self.assertTrue(summary["ready_for_auto_live_order"])
            self.assertTrue(
                "unattended auto-live" in summary["ready_for_auto_live_order_reason"]
                or "probe-size live mode" in summary["ready_for_auto_live_order_reason"]
            )

    def test_run_kalshi_micro_prior_trader_surfaces_prior_execute_failure_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "upstream_error",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "cache",
                    "balance_live_verified": False,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 100.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXTEST-1",
                        "top_market_title": "Test Market",
                        "top_market_close_time": "2026-03-28T12:00:00Z",
                        "top_market_hours_to_close": 15.0,
                        "top_market_side": "no",
                        "top_market_maker_entry_price_dollars": 0.96,
                        "top_market_maker_entry_edge": 0.02,
                        "top_market_estimated_entry_cost_dollars": 0.96,
                        "top_market_expected_value_dollars": 0.02,
                        "top_market_expected_roi_on_cost": 0.020833,
                        "top_market_expected_value_per_day_dollars": 0.032,
                        "top_market_expected_roi_per_day": 0.033333,
                        "top_market_estimated_max_profit_dollars": 0.04,
                        "top_market_estimated_max_loss_dollars": 0.96,
                        "top_market_max_profit_roi_on_cost": 0.041667,
                        "top_market_fair_probability": 0.98,
                        "top_market_confidence": 0.7,
                        "top_market_thesis": "Test thesis",
                    },
                    "attempts": [
                        {
                            "market_ticker": "KXTEST-1",
                            "result": "orderbook_unavailable",
                            "orderbook_http_status": 599,
                            "orderbook_error_type": "url_error",
                            "orderbook_error": "timed out",
                        }
                    ],
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "upstream_error")
            self.assertEqual(summary["prior_execute_error_kind"], "timeout")
            self.assertEqual(summary["prior_execute_failure_attempts_count"], 1)
            self.assertEqual(summary["prior_execute_failure_retryable_attempts_count"], 1)
            self.assertEqual(summary["prior_execute_failure_market_tickers"], ["KXTEST-1"])
            self.assertEqual(summary["prior_execute_failure_result_counts"], {"orderbook_unavailable": 1})
            self.assertEqual(summary["prior_execute_failure_http_status_counts"], {"orderbook:599": 1})
            self.assertEqual(summary["prior_execute_failure_error_type_counts"], {"orderbook:url_error": 1})
            self.assertIn("First failing attempt result: orderbook_unavailable on KXTEST-1.", summary["status_reason"])
            self.assertIn("Orderbook HTTP status: 599.", summary["status_reason"])
            self.assertIn("Orderbook error type: url_error.", summary["status_reason"])
            self.assertIn("Orderbook error: timed out.", summary["status_reason"])
            self.assertIn("Failure error types: orderbook:url_errorx1.", summary["status_reason"])

    def test_run_kalshi_micro_prior_trader_skips_non_failure_attempts_in_failure_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "upstream_error",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "cache",
                    "balance_live_verified": False,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 100.0,
                        "gate_blockers": [],
                    },
                    "attempts": [
                        {
                            "market_ticker": "KXHEALTHY-1",
                            "result": "dry_run_ready",
                            "orderbook_http_status": 200,
                        },
                        {
                            "market_ticker": "KXFAIL-1",
                            "result": "orderbook_unavailable",
                            "orderbook_http_status": 599,
                            "orderbook_error_type": "url_error",
                            "orderbook_error": "timed out",
                        },
                    ],
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertNotIn("dry_run_ready", summary["status_reason"])
            self.assertEqual(summary["prior_execute_failure_attempts_count"], 1)
            self.assertIn("First failing attempt result: orderbook_unavailable on KXFAIL-1.", summary["status_reason"])

    def test_run_kalshi_micro_prior_trader_classifies_submit_failed_http_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "live_submit_failed",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 100.0,
                        "gate_blockers": [],
                    },
                    "attempts": [
                        {
                            "market_ticker": "KXSUBMIT-1",
                            "result": "submit_failed",
                            "submission_http_status": 429,
                        }
                    ],
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["prior_execute_error_kind"], "rate_limited")
            self.assertIn("First failing attempt result: submit_failed on KXSUBMIT-1.", summary["status_reason"])
            self.assertIn("Submission HTTP status: 429.", summary["status_reason"])

    def test_run_kalshi_micro_prior_trader_summarizes_multiple_failure_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "upstream_error",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "cache",
                    "balance_live_verified": False,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 100.0,
                        "gate_blockers": [],
                    },
                    "attempts": [
                        {
                            "market_ticker": "KXFAIL-1",
                            "result": "orderbook_unavailable",
                            "orderbook_http_status": 599,
                            "orderbook_error_type": "url_error",
                            "orderbook_error": "timed out",
                        },
                        {
                            "market_ticker": "KXFAIL-2",
                            "result": "orderbook_unavailable",
                            "orderbook_http_status": 599,
                            "orderbook_error_type": "url_error",
                            "orderbook_error": "timed out",
                        },
                    ],
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["prior_execute_failure_attempts_count"], 2)
            self.assertEqual(summary["prior_execute_failure_retryable_attempts_count"], 2)
            self.assertEqual(summary["prior_execute_failure_market_tickers"], ["KXFAIL-1", "KXFAIL-2"])
            self.assertEqual(summary["prior_execute_failure_result_counts"], {"orderbook_unavailable": 2})
            self.assertEqual(summary["prior_execute_failure_http_status_counts"], {"orderbook:599": 2})
            self.assertEqual(summary["prior_execute_failure_error_type_counts"], {"orderbook:url_error": 2})
            self.assertIn("Total failing attempts: 2.", summary["status_reason"])
            self.assertIn("Affected markets: KXFAIL-1, KXFAIL-2.", summary["status_reason"])
            self.assertIn("Failure HTTP statuses: orderbook:599x2.", summary["status_reason"])
            self.assertIn("Failure error types: orderbook:url_errorx2.", summary["status_reason"])

    def test_run_kalshi_micro_prior_trader_classifies_config_error_from_orderbook_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=lambda **kwargs: {
                    "status": "upstream_error",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "cache",
                    "balance_live_verified": False,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 100.0,
                        "gate_blockers": [],
                    },
                    "attempts": [
                        {
                            "market_ticker": "KXCFG-1",
                            "result": "orderbook_unavailable",
                            "orderbook_http_status": 599,
                            "orderbook_error_type": "config_error",
                            "orderbook_error": "missing_kalshi_credentials",
                        }
                    ],
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["prior_execute_error_kind"], "config_error")
            self.assertIn("Orderbook error type: config_error.", summary["status_reason"])
            self.assertIn("Orderbook error: missing_kalshi_credentials.", summary["status_reason"])

    def test_run_kalshi_micro_prior_trader_uses_temporary_live_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            seen_env_files: list[str] = []

            def fake_prior_execute_runner(**kwargs):
                current_env_file = str(kwargs["env_file"])
                seen_env_files.append(current_env_file)
                self.assertNotEqual(current_env_file, str(env_file))
                self.assertTrue(Path(current_env_file).exists())
                self.assertIn("BETBOT_ENABLE_LIVE_ORDERS=1", Path(current_env_file).read_text(encoding="utf-8"))
                return {
                    "status": "live_submitted_and_canceled",
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 91.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXTEST-2",
                        "top_market_title": "Live Test Market",
                        "top_market_close_time": "2026-03-28T14:00:00Z",
                        "top_market_hours_to_close": 17.0,
                        "top_market_side": "no",
                        "top_market_maker_entry_price_dollars": 0.97,
                        "top_market_maker_entry_edge": 0.03,
                        "top_market_estimated_entry_cost_dollars": 0.97,
                        "top_market_expected_value_dollars": 0.03,
                        "top_market_expected_roi_on_cost": 0.030928,
                        "top_market_expected_value_per_day_dollars": 0.042353,
                        "top_market_expected_roi_per_day": 0.043663,
                        "top_market_estimated_max_profit_dollars": 0.03,
                        "top_market_estimated_max_loss_dollars": 0.97,
                        "top_market_max_profit_roi_on_cost": 0.030928,
                        "top_market_fair_probability": 0.99,
                        "top_market_confidence": 0.8,
                        "top_market_thesis": "Live test thesis",
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                }

            def fake_reconcile_runner(**kwargs):
                current_env_file = str(kwargs["env_file"])
                self.assertEqual(current_env_file, seen_env_files[0])
                self.assertTrue(Path(current_env_file).exists())
                return {
                    "status": "ready",
                    "output_file": str(base / "reconcile.json"),
                }

            summary = run_kalshi_micro_prior_trader(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                use_temporary_live_env=True,
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=fake_prior_execute_runner,
                reconcile_runner=fake_reconcile_runner,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["live_env_mode"], "temporary_copy")
            self.assertEqual(summary["status"], "live_submitted_and_canceled")
            self.assertEqual(summary["action_taken"], "live_execute_reconcile")
            self.assertEqual(summary["top_market_estimated_entry_cost_dollars"], 0.97)
            self.assertEqual(summary["top_market_expected_value_dollars"], 0.03)
            self.assertEqual(summary["reconcile_status"], "ready")
            self.assertFalse(summary["ready_for_live_order"])
            self.assertFalse(summary["ready_for_auto_live_order"])
            self.assertFalse(Path(seen_env_files[0]).exists())

    def test_run_kalshi_micro_prior_trader_runs_weather_prior_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            history_csv.write_text("captured_at,market_ticker\n", encoding="utf-8")
            weather_called: list[bool] = []
            auto_called: list[bool] = []

            def fake_weather_runner(**kwargs):
                weather_called.append(True)
                self.assertEqual(kwargs["history_csv"], str(history_csv))
                self.assertEqual(kwargs["allowed_contract_families"], ("daily_rain",))
                return {
                    "status": "ready",
                    "generated_priors": 1,
                    "inserted_rows": 1,
                    "updated_rows": 0,
                    "manual_rows_protected": 0,
                    "output_file": str(base / "weather_summary.json"),
                    "output_csv": str(base / "weather.csv"),
                    "skipped_output_csv": str(base / "weather_skipped.csv"),
                }

            def fake_auto_prior_runner(**kwargs):
                auto_called.append(True)
                return {
                    "status": "ready",
                    "generated_priors": 0,
                    "candidate_markets_filtered_out": 0,
                    "inserted_rows": 0,
                    "updated_rows": 0,
                    "manual_rows_protected": 0,
                    "output_file": str(base / "auto_prior_summary.json"),
                    "output_csv": str(base / "auto_priors.csv"),
                    "skipped_output_csv": str(base / "auto_priors_skipped.csv"),
                }

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(history_csv),
                capture_before_execute=False,
                auto_refresh_weather_priors=True,
                auto_weather_allowed_contract_families=("daily_rain",),
                weather_prior_runner=fake_weather_runner,
                auto_refresh_priors=True,
                auto_prior_runner=fake_auto_prior_runner,
                prior_execute_runner=lambda **kwargs: {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 85.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXTEST-1",
                        "top_market_title": "Test Market",
                        "top_market_close_time": "2026-03-28T12:00:00Z",
                        "top_market_hours_to_close": 15.0,
                        "top_market_side": "yes",
                        "top_market_maker_entry_price_dollars": 0.45,
                        "top_market_maker_entry_edge": 0.03,
                        "top_market_estimated_entry_cost_dollars": 0.45,
                        "top_market_expected_value_dollars": 0.03,
                        "top_market_expected_roi_on_cost": 0.066667,
                        "top_market_expected_value_per_day_dollars": 0.04,
                        "top_market_expected_roi_per_day": 0.088889,
                        "top_market_estimated_max_profit_dollars": 0.55,
                        "top_market_estimated_max_loss_dollars": 0.45,
                        "top_market_max_profit_roi_on_cost": 1.222222,
                        "top_market_fair_probability": 0.48,
                        "top_market_confidence": 0.7,
                        "top_market_thesis": "Test thesis",
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(weather_called, [True])
            self.assertEqual(auto_called, [True])
            self.assertEqual(summary["weather_priors_status"], "ready")
            self.assertEqual(summary["weather_priors_generated"], 1)
            self.assertEqual(summary["weather_priors_inserted_rows"], 1)
            self.assertEqual(summary["status"], "dry_run")

    def test_run_kalshi_micro_prior_trader_surfaces_weather_refresh_upstream_error_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            history_csv.write_text("captured_at,market_ticker\n", encoding="utf-8")

            def fake_weather_runner(**kwargs):
                return {
                    "status": "upstream_error",
                    "generated_priors": 0,
                    "inserted_rows": 0,
                    "updated_rows": 0,
                    "manual_rows_protected": 0,
                    "fetch_errors_count": 1,
                    "fetch_error_kind_counts": {"dns_resolution_error": 1},
                    "error_kind": "dns_resolution_error",
                    "error": "<urlopen error [Errno 8] nodename nor servname provided, or not known>",
                    "output_file": str(base / "weather_summary.json"),
                    "output_csv": str(base / "weather.csv"),
                    "skipped_output_csv": str(base / "weather_skipped.csv"),
                }

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(history_csv),
                capture_before_execute=False,
                auto_refresh_weather_priors=True,
                auto_weather_allowed_contract_families=("daily_rain",),
                weather_prior_runner=fake_weather_runner,
                auto_refresh_priors=False,
                prior_execute_runner=lambda **kwargs: {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "prior_trade_gate_summary": {
                        "gate_pass": False,
                        "gate_status": "no_candidates",
                        "gate_score": 0.0,
                        "gate_blockers": ["No prior-backed maker plans are available."],
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["weather_priors_status"], "upstream_error")
            self.assertEqual(summary["weather_priors_error_kind"], "dns_resolution_error")
            self.assertEqual(summary["weather_priors_fetch_errors_count"], 1)
            self.assertEqual(summary["weather_priors_fetch_error_kind_counts"], {"dns_resolution_error": 1})
            self.assertIn("nodename nor servname", summary["weather_priors_error"])


if __name__ == "__main__":
    unittest.main()
