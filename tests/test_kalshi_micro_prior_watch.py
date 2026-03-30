from datetime import datetime, timezone
import json
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_prior_watch import run_kalshi_micro_prior_watch


class KalshiMicroPriorWatchTests(unittest.TestCase):
    def test_run_kalshi_micro_prior_watch_uses_single_capture_then_prior_trader(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            status_kwargs: dict[str, object] = {}
            prior_trader_kwargs: dict[str, object] = {}
            prior_summary_kwargs: dict[str, object] = {}

            summary = run_kalshi_micro_prior_watch(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(base / "history.csv"),
                watch_history_csv=str(base / "watch_history.csv"),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "scan_status": "ready",
                    "scan_error": None,
                    "scan_summary_file": str(base / "capture.json"),
                    "scan_output_csv": str(base / "scan.csv"),
                    "scan_page_requests": 5,
                    "scan_rate_limit_retries_used": 0,
                    "scan_network_retries_used": 0,
                    "scan_transient_http_retries_used": 0,
                    "scan_search_retries_total": 0,
                    "scan_search_health_status": "ready",
                    "scan_events_fetched": 120,
                    "scan_markets_ranked": 42,
                    "history_csv": str(base / "history.csv"),
                },
                status_runner=lambda **kwargs: (
                    status_kwargs.update(kwargs) or {
                        "recommendation": "review_prior_edge",
                        "trade_gate_status": "no_meaningful_candidates",
                        "board_regime": "pressure_building",
                        "board_regime_reason": "A repeated pressure market is forming.",
                        "output_file": str(base / "status.json"),
                    }
                ),
                prior_summary_runner=lambda **kwargs: (
                    prior_summary_kwargs.update(kwargs) or {
                        "positive_best_entry_markets": 2,
                        "positive_edge_yes_ask_markets": 1,
                        "positive_edge_no_ask_markets": 1,
                        "top_market_ticker": "KXTAKER-1",
                        "top_market_hours_to_close": 10.0,
                        "top_market_best_entry_side": "yes",
                        "top_market_best_entry_edge": 0.01,
                        "top_market_best_maker_entry_side": "no",
                        "top_market_best_maker_entry_edge": 0.005,
                        "output_file": str(base / "prior_summary.json"),
                    }
                ),
                prior_trader_runner=lambda **kwargs: (
                    prior_trader_kwargs.update(kwargs) or {
                        "status": "dry_run",
                        "action_taken": "dry_run_execute_reconcile",
                        "prior_execute_failure_attempts_count": 0,
                        "prior_execute_failure_retryable_attempts_count": 0,
                        "prior_execute_failure_market_tickers": [],
                        "prior_execute_failure_result_counts": {},
                        "prior_execute_failure_http_status_counts": {},
                        "prior_trade_gate_pass": True,
                        "prior_trade_gate_status": "pass",
                        "prior_trade_gate_score": 86.0,
                        "watch_runs_total": 12,
                        "watch_latest_recorded_at": "2026-03-27T20:55:00+00:00",
                        "watch_focus_market_mode": "pressure",
                        "watch_focus_market_ticker": "KXWATCH-1",
                        "watch_focus_market_streak": 8,
                        "watch_focus_market_state": "threshold_approaching",
                        "watch_focus_market_state_reason": "The repeated pressure market is nearing the threshold.",
                        "watch_recent_focus_market_changes": 2,
                        "watch_recommendation_streak": 3,
                        "watch_trade_gate_status_streak": 4,
                        "ready_for_live_order": True,
                        "ready_for_live_order_reason": "Dry-run path is clear.",
                        "ready_for_auto_live_order": False,
                        "ready_for_auto_live_order_reason": "Capital efficiency is still too weak for unattended auto-live.",
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
                        "reconcile_status": "no_order_ids",
                        "reconcile_summary_file": str(base / "reconcile.json"),
                        "output_file": str(base / "prior_trader.json"),
                    }
                ),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(status_kwargs["scan_csv"], str(base / "scan.csv"))
            self.assertEqual(prior_summary_kwargs["history_csv"], str(base / "history.csv"))
            self.assertEqual(prior_trader_kwargs["capture_before_execute"], False)
            self.assertEqual(prior_trader_kwargs["watch_history_csv"], str(base / "watch_history.csv"))
            self.assertEqual(summary["status_recommendation"], "review_prior_edge")
            self.assertEqual(summary["recommendation"], "review_prior_edge")
            self.assertEqual(summary["board_regime"], "pressure_building")
            self.assertFalse(summary["capture_degraded"])
            self.assertEqual(summary["capture_scan_page_requests"], 5)
            self.assertEqual(summary["capture_scan_search_health_status"], "ready")
            self.assertEqual(summary["capture_scan_events_fetched"], 120)
            self.assertEqual(summary["best_entry_top_market_ticker"], "KXTAKER-1")
            self.assertEqual(summary["best_entry_top_market_side"], "yes")
            self.assertEqual(summary["best_entry_top_market_edge"], 0.01)
            self.assertEqual(summary["prior_gate_result"], "pass")
            self.assertEqual(summary["prior_trader_failure_attempts_count"], 0)
            self.assertEqual(summary["prior_trader_failure_http_status_counts"], {})
            self.assertIn("janitor_attempts", summary)
            self.assertIn("janitor_canceled_open_orders_count", summary)
            self.assertIn("janitor_cancel_failed_attempts", summary)
            self.assertEqual(summary["watch_runs_total"], 12)
            self.assertEqual(summary["watch_latest_recorded_at"], "2026-03-27T20:55:00+00:00")
            self.assertEqual(summary["top_market_side"], "no")
            self.assertEqual(summary["watch_focus_market_mode"], "pressure")
            self.assertEqual(summary["watch_focus_market_ticker"], "KXWATCH-1")
            self.assertEqual(summary["watch_focus_market_streak"], 8)
            self.assertEqual(summary["watch_focus_market_state"], "threshold_approaching")
            self.assertEqual(summary["watch_recent_focus_market_changes"], 2)
            self.assertEqual(summary["watch_recommendation_streak"], 3)
            self.assertEqual(summary["watch_trade_gate_status_streak"], 4)
            self.assertEqual(summary["top_market_estimated_entry_cost_dollars"], 0.96)
            self.assertEqual(summary["estimated_entry_cost"], 0.96)
            self.assertEqual(summary["top_market_expected_value_dollars"], 0.02)
            self.assertEqual(summary["expected_value"], 0.02)
            self.assertAlmostEqual(summary["top_market_expected_roi_on_cost"], 0.020833, places=6)
            self.assertAlmostEqual(summary["expected_roi_on_cost"], 0.020833, places=6)
            self.assertAlmostEqual(summary["expected_value_per_day"], 0.032, places=6)
            self.assertTrue(summary["ready_for_live_order"])
            self.assertTrue(summary["ready_for_manual_live_order"])
            self.assertTrue(summary["manual_live_ready"])
            self.assertEqual(summary["ready_for_manual_live_order_reason"], "Dry-run path is clear.")
            self.assertFalse(summary["ready_for_auto_live_order"])
            self.assertFalse(summary["auto_live_ready"])
            self.assertEqual(summary["maker_price"], 0.96)
            self.assertEqual(summary["maker_edge"], 0.02)
            self.assertEqual(summary["hours_to_close"], 15.0)
            self.assertAlmostEqual(summary["expected_roi_per_day"], 0.033333, places=6)
            self.assertEqual(summary["expected_max_profit"], 0.04)
            self.assertEqual(summary["prior_trader_status"], "dry_run")
            self.assertTrue(Path(summary["output_file"]).exists())
            persisted = json.loads(Path(summary["output_file"]).read_text(encoding="utf-8"))
            self.assertEqual(persisted["output_file"], summary["output_file"])

    def test_run_kalshi_micro_prior_watch_marks_degraded_ready_when_capture_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_watch(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(base / "history.csv"),
                watch_history_csv=str(base / "watch_history.csv"),
                capture_runner=lambda **kwargs: {
                    "status": "upstream_error",
                    "scan_status": "upstream_error",
                    "scan_error": "network down",
                    "scan_summary_file": str(base / "capture.json"),
                    "scan_output_csv": None,
                    "scan_page_requests": 2,
                    "scan_rate_limit_retries_used": 0,
                    "scan_network_retries_used": 2,
                    "scan_transient_http_retries_used": 0,
                    "scan_search_retries_total": 2,
                    "scan_search_health_status": "error",
                    "scan_events_fetched": 0,
                    "scan_markets_ranked": 0,
                    "history_csv": str(base / "history.csv"),
                },
                status_runner=lambda **kwargs: {
                    "recommendation": "review_prior_edge",
                    "trade_gate_status": "no_candidates",
                    "board_regime": "pressure_building",
                    "board_regime_reason": "A repeated pressure market is forming.",
                    "output_file": str(base / "status.json"),
                },
                prior_summary_runner=lambda **kwargs: {
                    "positive_best_entry_markets": 1,
                    "positive_edge_yes_ask_markets": 1,
                    "positive_edge_no_ask_markets": 0,
                    "top_market_ticker": "KXTAKER-1",
                    "top_market_hours_to_close": 10.0,
                    "top_market_best_entry_side": "yes",
                    "top_market_best_entry_edge": 0.01,
                    "top_market_best_maker_entry_side": "yes",
                    "top_market_best_maker_entry_edge": 0.01,
                    "output_file": str(base / "prior_summary.json"),
                },
                prior_trader_runner=lambda **kwargs: {
                    "status": "dry_run",
                    "action_taken": "dry_run_execute_reconcile",
                    "prior_execute_failure_attempts_count": 0,
                    "prior_execute_failure_retryable_attempts_count": 0,
                    "prior_execute_failure_market_tickers": [],
                    "prior_execute_failure_result_counts": {},
                    "prior_execute_failure_http_status_counts": {},
                    "prior_trade_gate_pass": True,
                    "prior_trade_gate_status": "pass",
                    "prior_trade_gate_score": 86.0,
                    "watch_focus_market_mode": "pressure",
                    "watch_focus_market_ticker": "KXWATCH-1",
                    "watch_focus_market_streak": 8,
                    "watch_focus_market_state": "stalled_pressure_focus",
                    "watch_focus_market_state_reason": "The same pressure focus has stalled.",
                    "ready_for_live_order": False,
                    "ready_for_live_order_reason": "Live balance requires reverification.",
                    "ready_for_auto_live_order": False,
                    "ready_for_auto_live_order_reason": "Manual live readiness is not clear yet.",
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
                    "reconcile_status": "no_order_ids",
                    "reconcile_summary_file": str(base / "reconcile.json"),
                    "output_file": str(base / "prior_trader.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "degraded_ready")
            self.assertTrue(summary["capture_degraded"])
            self.assertEqual(summary["capture_scan_search_health_status"], "error")
            self.assertEqual(summary["capture_scan_network_retries_used"], 2)
            self.assertEqual(summary["capture_error_kind"], "network_error")
            self.assertIn("existing history", summary["status_reason"])
            self.assertIn("network down", summary["status_reason"])
            self.assertEqual(summary["prior_trader_failure_attempts_count"], 0)
            self.assertEqual(summary["watch_focus_market_state"], "stalled_pressure_focus")
            self.assertFalse(summary["ready_for_manual_live_order"])
            self.assertFalse(summary["manual_live_ready"])
            self.assertEqual(summary["ready_for_manual_live_order_reason"], "Live balance requires reverification.")
            self.assertEqual(summary["manual_live_ready_reason"], "Live balance requires reverification.")
            persisted = json.loads(Path(summary["output_file"]).read_text(encoding="utf-8"))
            self.assertEqual(persisted["status"], "degraded_ready")

    def test_run_kalshi_micro_prior_watch_surfaces_prior_trader_failure_reason(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_prior_watch(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(base / "history.csv"),
                watch_history_csv=str(base / "watch_history.csv"),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "scan_status": "ready",
                    "scan_error": None,
                    "scan_summary_file": str(base / "capture.json"),
                    "scan_output_csv": str(base / "scan.csv"),
                    "history_csv": str(base / "history.csv"),
                },
                status_runner=lambda **kwargs: {
                    "recommendation": "review_prior_edge",
                    "trade_gate_status": "no_candidates",
                    "board_regime": "monitor",
                    "board_regime_reason": "No clear board signal.",
                    "output_file": str(base / "status.json"),
                },
                prior_summary_runner=lambda **kwargs: {
                    "positive_best_entry_markets": 0,
                    "positive_edge_yes_ask_markets": 0,
                    "positive_edge_no_ask_markets": 0,
                    "output_file": str(base / "prior_summary.json"),
                },
                prior_trader_runner=lambda **kwargs: {
                    "status": "upstream_error",
                    "prior_execute_failure_attempts_count": 2,
                    "prior_execute_failure_retryable_attempts_count": 2,
                    "prior_execute_failure_market_tickers": ["KXTEST-1", "KXTEST-2"],
                    "prior_execute_failure_result_counts": {"orderbook_unavailable": 2},
                    "prior_execute_failure_http_status_counts": {"orderbook:599": 2},
                    "prior_execute_error_kind": "network_error",
                    "status_reason": (
                        "Prior execution finished with upstream_error. "
                        "Failure kind: network_error. "
                        "First failing attempt result: orderbook_unavailable on KXTEST-1. "
                        "Orderbook HTTP status: 599."
                    ),
                    "ready_for_live_order": False,
                    "ready_for_live_order_reason": "Live balance requires reverification.",
                    "ready_for_auto_live_order": False,
                    "ready_for_auto_live_order_reason": "Manual live readiness is not clear yet.",
                    "output_file": str(base / "prior_trader.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["prior_trader_status"], "upstream_error")
            self.assertEqual(summary["prior_trader_error_kind"], "network_error")
            self.assertEqual(summary["prior_trader_failure_attempts_count"], 2)
            self.assertEqual(summary["prior_trader_failure_retryable_attempts_count"], 2)
            self.assertEqual(summary["prior_trader_failure_market_tickers"], ["KXTEST-1", "KXTEST-2"])
            self.assertEqual(summary["prior_trader_failure_result_counts"], {"orderbook_unavailable": 2})
            self.assertEqual(summary["prior_trader_failure_http_status_counts"], {"orderbook:599": 2})
            self.assertIn("orderbook_unavailable on KXTEST-1", summary["prior_trader_status_reason"])


if __name__ == "__main__":
    unittest.main()
