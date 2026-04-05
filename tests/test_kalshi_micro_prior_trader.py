from datetime import datetime, timezone
import json
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_prior_trader import run_kalshi_micro_prior_trader


class KalshiMicroPriorTraderTests(unittest.TestCase):
    def test_run_kalshi_micro_prior_trader_defaults_ws_authority_true(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            seen_ws_flags: list[bool] = []

            def fake_prior_execute_runner(**kwargs):
                seen_ws_flags.append(bool(kwargs.get("enforce_ws_state_authority")))
                return {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "enforce_ws_state_authority": bool(kwargs.get("enforce_ws_state_authority")),
                    "ws_state_authority": {
                        "checked": True,
                        "status": "ready",
                        "gate_pass": True,
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
                }

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_before_execute=False,
                prior_execute_runner=fake_prior_execute_runner,
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(seen_ws_flags, [True])
            self.assertTrue(summary["enforce_ws_state_authority"])

    def test_run_kalshi_micro_prior_trader_passes_climate_router_pilot_controls(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            seen_kwargs: list[dict[str, object]] = []

            def fake_prior_execute_runner(**kwargs):
                seen_kwargs.append(dict(kwargs))
                return {
                    "status": "dry_run",
                    "actual_live_balance_dollars": 40.0,
                    "actual_live_balance_source": "live",
                    "balance_live_verified": True,
                    "climate_router_pilot_status": "ready",
                    "climate_router_pilot_submitted_rows": 1,
                    "climate_router_pilot_promoted_rows": 1,
                    "climate_router_pilot_considered_rows": 2,
                    "climate_router_pilot_execute_considered_rows": 1,
                    "climate_router_pilot_live_mode_enabled": True,
                    "climate_router_pilot_live_eligible_rows": 1,
                    "climate_router_pilot_would_attempt_live_if_enabled": 0,
                    "climate_router_pilot_blocked_dry_run_only_rows": 0,
                    "climate_router_pilot_blocked_research_dry_run_only_reason_counts": {},
                    "climate_router_pilot_non_policy_gates_passed_rows": 1,
                    "climate_router_pilot_attempted_orders": 1,
                    "climate_router_pilot_acked_orders": 1,
                    "climate_router_pilot_resting_orders": 1,
                    "climate_router_pilot_filled_orders": 0,
                    "climate_router_pilot_blocked_post_promotion_reason_counts": {},
                    "climate_router_pilot_blocked_research_dry_run_only": 0,
                    "climate_router_pilot_blocked_live_disabled": 0,
                    "climate_router_pilot_expected_value_dollars": 0.08,
                    "climate_router_pilot_blocked_reason_counts": {"pilot_limit_reached": 1},
                    "climate_router_pilot_selected_tickers": ["KXTEST-1"],
                    "climate_router_pilot_allowed_families_effective": ["monthly_climate_anomaly"],
                    "climate_router_pilot_excluded_families_effective": ["daily_rain"],
                    "climate_router_pilot_policy_scope_override_enabled": True,
                    "climate_router_pilot_policy_scope_override_active": True,
                    "climate_router_pilot_policy_scope_override_status": "active",
                    "climate_router_pilot_policy_scope_override_attempts": 1,
                    "climate_router_pilot_policy_scope_override_submissions": 1,
                    "climate_router_pilot_policy_scope_override_blocked_reason_counts": {},
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 88.0,
                        "gate_blockers": [],
                        "top_market_ticker": "KXTEST-1",
                        "top_market_title": "Test Market",
                        "top_market_close_time": "2026-03-28T12:00:00Z",
                        "top_market_hours_to_close": 15.0,
                        "top_market_side": "yes",
                        "top_market_maker_entry_price_dollars": 0.42,
                        "top_market_maker_entry_edge": 0.08,
                        "top_market_estimated_entry_cost_dollars": 0.42,
                        "top_market_expected_value_dollars": 0.08,
                        "top_market_expected_roi_on_cost": 0.190476,
                        "top_market_expected_value_per_day_dollars": 0.128,
                        "top_market_expected_roi_per_day": 0.304761,
                        "top_market_estimated_max_profit_dollars": 0.58,
                        "top_market_estimated_max_loss_dollars": 0.42,
                        "top_market_max_profit_roi_on_cost": 1.380952,
                        "top_market_fair_probability": 0.61,
                        "top_market_confidence": 0.8,
                        "top_market_thesis": "Pilot test",
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                }

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_before_execute=False,
                climate_router_pilot_enabled=True,
                climate_router_summary_json=str(base / "router_summary.json"),
                climate_router_pilot_max_orders_per_run=2,
                climate_router_pilot_contracts_cap=1,
                climate_router_pilot_required_ev_dollars=0.02,
                climate_router_pilot_allowed_classes=("tradable", "hot_positive"),
                climate_router_pilot_allowed_families=("monthly_climate_anomaly",),
                climate_router_pilot_excluded_families=("daily_rain",),
                climate_router_pilot_policy_scope_override_enabled=True,
                prior_execute_runner=fake_prior_execute_runner,
                reconcile_runner=lambda **kwargs: {
                    "status": "ready",
                    "output_file": str(base / "reconcile.json"),
                    "pilot_attempted_orders": 1,
                    "pilot_filled_orders": 1,
                    "pilot_partial_fills": 0,
                    "pilot_markout_10s_dollars": 0.01,
                    "pilot_markout_60s_dollars": 0.02,
                    "pilot_markout_300s_dollars": -0.01,
                    "pilot_realized_pnl_dollars": 0.05,
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(len(seen_kwargs), 1)
            self.assertTrue(seen_kwargs[0]["climate_router_pilot_enabled"])
            self.assertEqual(seen_kwargs[0]["climate_router_pilot_max_orders_per_run"], 2)
            self.assertEqual(seen_kwargs[0]["climate_router_pilot_contracts_cap"], 1)
            self.assertEqual(seen_kwargs[0]["climate_router_pilot_required_ev_dollars"], 0.02)
            self.assertEqual(
                tuple(seen_kwargs[0]["climate_router_pilot_allowed_classes"]),
                ("tradable", "hot_positive"),
            )
            self.assertEqual(
                tuple(seen_kwargs[0]["climate_router_pilot_allowed_families"]),
                ("monthly_climate_anomaly",),
            )
            self.assertEqual(
                tuple(seen_kwargs[0]["climate_router_pilot_excluded_families"]),
                ("daily_rain",),
            )
            self.assertTrue(seen_kwargs[0]["climate_router_pilot_policy_scope_override_enabled"])
            self.assertEqual(summary["climate_router_pilot_status"], "ready")
            self.assertEqual(summary["climate_router_pilot_submitted_rows"], 1)
            self.assertEqual(summary["climate_router_pilot_considered_rows"], 2)
            self.assertEqual(summary["climate_router_pilot_allowed_families_effective"], ["monthly_climate_anomaly"])
            self.assertEqual(summary["climate_router_pilot_excluded_families_effective"], ["daily_rain"])
            self.assertTrue(summary["climate_router_pilot_policy_scope_override_enabled"])
            self.assertTrue(summary["climate_router_pilot_policy_scope_override_active"])
            self.assertEqual(summary["climate_router_pilot_policy_scope_override_status"], "active")
            self.assertEqual(summary["climate_router_pilot_policy_scope_override_attempts"], 1)
            self.assertEqual(summary["climate_router_pilot_policy_scope_override_submissions"], 1)
            self.assertTrue(summary["climate_router_pilot_live_mode_enabled"])
            self.assertEqual(summary["climate_router_pilot_live_eligible_rows"], 1)
            self.assertEqual(summary["climate_router_pilot_attempted_orders"], 1)
            self.assertEqual(summary["climate_router_pilot_acked_orders"], 1)
            self.assertEqual(summary["climate_router_pilot_resting_orders"], 1)
            self.assertEqual(summary["climate_router_pilot_filled_orders"], 0)
            self.assertEqual(summary["climate_router_pilot_reconcile_filled_orders"], 1)
            self.assertEqual(summary["climate_router_pilot_partial_fills"], 0)
            self.assertAlmostEqual(float(summary["climate_router_pilot_realized_pnl_dollars"]), 0.05, places=6)
            self.assertAlmostEqual(float(summary["climate_router_pilot_expected_vs_realized_delta"]), -0.03, places=6)

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
                auto_refresh_weather_priors=False,
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                prior_execute_runner=fake_prior_execute_runner,
                reconcile_runner=fake_reconcile_runner,
                sleep_fn=lambda *_: None,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["live_env_mode"], "temporary_copy")
            self.assertEqual(summary["status"], "live_submitted_and_canceled")
            self.assertEqual(summary["action_taken"], "live_execute_reconcile")
            self.assertEqual(summary["top_market_estimated_entry_cost_dollars"], 0.97)
            self.assertEqual(summary["top_market_expected_value_dollars"], 0.03)
            self.assertEqual(summary["reconcile_status"], "ready")
            self.assertTrue(summary["post_live_markout_capture_attempted"])
            self.assertEqual(summary["post_live_markout_capture_status"], "ready")
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

    def test_run_kalshi_micro_prior_trader_downgrades_live_when_weather_prior_refresh_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            history_csv.write_text("captured_at,market_ticker\n2026-03-27T20:55:00+00:00,KXRAIN-1\n", encoding="utf-8")
            allow_live_orders_seen: list[bool] = []

            def fake_prior_execute_runner(**kwargs):
                allow_live_orders_seen.append(bool(kwargs.get("allow_live_orders")))
                return {
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
                }

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(history_csv),
                allow_live_orders=True,
                capture_before_execute=True,
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(history_csv),
                    "scan_summary_file": str(base / "capture.json"),
                },
                auto_refresh_weather_priors=True,
                weather_prior_runner=lambda **kwargs: {
                    "status": "upstream_error",
                    "error_kind": "dns_resolution_error",
                    "error": "<urlopen error [Errno 8] nodename nor servname provided, or not known>",
                },
                auto_refresh_priors=False,
                prior_execute_runner=fake_prior_execute_runner,
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(allow_live_orders_seen, [False])
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertFalse(summary["weather_refresh_live_ready"])
            self.assertIn("weather prior refresh did not complete cleanly", str(summary["live_orders_downgraded_reason"]))

    def test_run_kalshi_micro_prior_trader_downgrades_live_when_prewarm_has_zero_live_ready_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            history_csv.write_text("captured_at,market_ticker\n2026-03-27T20:55:00+00:00,KXRAIN-1\n", encoding="utf-8")
            allow_live_orders_seen: list[bool] = []
            weather_refresh_call_count = 0

            def fake_prior_execute_runner(**kwargs):
                allow_live_orders_seen.append(bool(kwargs.get("allow_live_orders")))
                return {
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
                }

            def fake_weather_prior_runner(**kwargs):
                nonlocal weather_refresh_call_count
                weather_refresh_call_count += 1
                if weather_refresh_call_count == 1:
                    return {"status": "no_weather_priors"}
                return {"status": "ready"}

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(history_csv),
                allow_live_orders=True,
                capture_before_execute=True,
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(history_csv),
                    "scan_summary_file": str(base / "capture.json"),
                },
                auto_refresh_weather_priors=True,
                auto_prewarm_weather_station_history=True,
                weather_prewarm_runner=lambda **kwargs: {
                    "status": "ready",
                    "prewarm_keys_attempted": 2,
                    "live_ready_counts": {"live_ready": 0, "not_live_ready": 2},
                    "status_counts": {"rate_limited": 2},
                },
                weather_prior_runner=fake_weather_prior_runner,
                auto_refresh_priors=False,
                prior_execute_runner=fake_prior_execute_runner,
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(allow_live_orders_seen, [False])
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertFalse(summary["weather_refresh_live_ready"])
            self.assertIn("zero live-ready station/day keys", str(summary["live_orders_downgraded_reason"]))
            self.assertTrue(summary["weather_prewarm_fallback_triggered"])
            self.assertEqual(summary["weather_prior_refresh_attempts"], 2)
            self.assertEqual(weather_refresh_call_count, 2)

    def test_run_kalshi_micro_prior_trader_skips_prewarm_when_weather_priors_are_healthy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            history_csv.write_text("captured_at,market_ticker\n2026-03-27T20:55:00+00:00,KXRAIN-1\n", encoding="utf-8")
            prewarm_called: list[bool] = []

            summary = run_kalshi_micro_prior_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                history_csv=str(history_csv),
                capture_before_execute=False,
                auto_refresh_weather_priors=True,
                auto_prewarm_weather_station_history=True,
                weather_prior_runner=lambda **kwargs: {
                    "status": "ready",
                    "station_history_status_counts": {"ready": 2},
                    "contract_family_generated_counts": {"daily_rain": 1, "daily_temperature": 1},
                },
                weather_prewarm_runner=lambda **kwargs: (
                    prewarm_called.append(True) or {"status": "ready", "prewarm_keys_attempted": 1}
                ),
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

            self.assertEqual(prewarm_called, [])
            self.assertFalse(summary["weather_prewarm_fallback_triggered"])
            self.assertEqual(summary["weather_prior_refresh_attempts"], 1)

    def test_run_kalshi_micro_prior_trader_runs_continuous_post_live_markout_capture_loop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")
            capture_calls: list[dict[str, object]] = []
            sleep_calls: list[float] = []

            def fake_capture_runner(**kwargs):
                capture_calls.append(kwargs)
                return {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / f"capture_{len(capture_calls)}.json"),
                    "rows_appended": 1,
                    "scan_page_requests": 2,
                    "scan_search_health_status": "ready",
                }

            summary = run_kalshi_micro_prior_trader(
                env_file=str(env_file),
                output_dir=str(base),
                allow_live_orders=True,
                auto_refresh_weather_priors=False,
                capture_before_execute=True,
                capture_runner=fake_capture_runner,
                prior_execute_runner=lambda **kwargs: {
                    "status": "live_submitted",
                    "prior_trade_gate_summary": {
                        "gate_pass": True,
                        "gate_status": "pass",
                        "gate_score": 90.0,
                        "gate_blockers": [],
                    },
                    "output_file": str(base / "prior_execute.json"),
                    "execute_summary_file": str(base / "execute_summary.json"),
                    "execute_output_csv": str(base / "execute.csv"),
                    "plan_summary_file": str(base / "plan.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "ready",
                    "output_file": str(base / "reconcile.json"),
                },
                post_live_markout_capture_delay_seconds=10.0,
                post_live_markout_capture_window_seconds=120.0,
                post_live_markout_capture_interval_seconds=50.0,
                post_live_markout_capture_max_runs=10,
                sleep_fn=lambda seconds: sleep_calls.append(round(float(seconds), 3)),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            # One pre-execute capture + 4 post-live capture runs.
            self.assertEqual(len(capture_calls), 5)
            self.assertEqual(sleep_calls, [10.0, 50.0, 50.0, 20.0])
            self.assertTrue(summary["post_live_markout_capture_attempted"])
            self.assertEqual(summary["post_live_markout_capture_status"], "ready")
            self.assertEqual(summary["post_live_markout_capture_runs_total"], 4)
            self.assertEqual(summary["post_live_markout_capture_successful_runs"], 4)
            self.assertEqual(summary["post_live_markout_capture_failed_runs"], 0)
            self.assertEqual(summary["post_live_markout_capture_rows_appended_total"], 4)


if __name__ == "__main__":
    unittest.main()
