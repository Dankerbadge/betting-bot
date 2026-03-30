from datetime import datetime, timezone
import sqlite3
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_book import default_book_db_path, ensure_book_schema
from betbot.kalshi_micro_status import run_kalshi_micro_status


class KalshiMicroStatusTests(unittest.TestCase):
    def test_run_kalshi_micro_status_builds_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            def fake_execute_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "actual_live_balance_dollars": 40.0,
                    "planned_orders": 1,
                    "status": "dry_run",
                    "board_warning": "thin board",
                    "attempts": [
                        {
                            "market_ticker": "KXTEST-1",
                            "planned_yes_bid_dollars": 0.02,
                        }
                    ],
                    "output_file": str(base / "execute.json"),
                    "ledger_csv": str(base / "ledger.csv"),
                }

            def fake_reconcile_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "no_order_ids",
                    "status_counts": {},
                    "total_market_exposure_dollars": 0.0,
                    "total_realized_pnl_dollars": 0.0,
                    "total_fees_paid_dollars": 0.0,
                    "output_file": str(base / "reconcile.json"),
                }

            def fake_quality_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "meaningful_markets": 0,
                    "watchlist_markets": 1,
                    "top_markets": [{"market_ticker": "KXTEST-1"}],
                    "output_file": str(base / "quality.json"),
                }

            def fake_signal_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "eligible_markets": 0,
                    "watch_markets": 1,
                    "top_markets": [{"market_ticker": "KXTEST-1"}],
                    "output_file": str(base / "signals.json"),
                }

            def fake_persistence_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "persistent_tradeable_markets": 0,
                    "persistent_watch_markets": 1,
                    "recurring_markets": 1,
                    "top_markets": [{"market_ticker": "KXTEST-1"}],
                    "output_file": str(base / "persistence.json"),
                }

            def fake_delta_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "board_change_label": "stale",
                    "improved_two_sided_markets": 0,
                    "newly_tradeable_markets": 0,
                    "top_markets": [{"market_ticker": "KXTEST-1"}],
                    "output_file": str(base / "deltas.json"),
                }

            def fake_category_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "tradeable_categories": 0,
                    "watch_categories": 1,
                    "thin_categories": 0,
                    "top_categories": [
                        {
                            "category": "Politics",
                            "category_label": "watch",
                            "category_rank_score": 12.5,
                        }
                    ],
                    "concentration_warning": "Two-sided liquidity is heavily concentrated in Politics.",
                    "output_file": str(base / "categories.json"),
                }

            def fake_pressure_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "build_markets": 0,
                    "watch_markets": 1,
                    "top_build_market_ticker": "KXTEST-1",
                    "top_build_category": "Politics",
                    "output_file": str(base / "pressure.json"),
                }

            def fake_threshold_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "approaching_markets": 1,
                    "building_markets": 0,
                    "top_approaching_market_ticker": "KXTEST-1",
                    "top_approaching_category": "Politics",
                    "top_approaching_hours_to_tradeable": 2.0,
                    "output_file": str(base / "thresholds.json"),
                }

            def fake_prior_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "matched_live_markets": 1,
                    "positive_edge_yes_bid_markets": 1,
                    "positive_edge_yes_ask_markets": 1,
                    "top_market_ticker": "KXTEST-1",
                    "top_market_edge_to_yes_ask": 0.04,
                    "output_file": str(base / "priors.json"),
                }

            summary = run_kalshi_micro_status(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                ledger_csv=str(base / "ledger.csv"),
                watch_history_csv=str(base / "watch_history.csv"),
                execute_runner=fake_execute_runner,
                reconcile_runner=fake_reconcile_runner,
                quality_runner=fake_quality_runner,
                signal_runner=fake_signal_runner,
                persistence_runner=fake_persistence_runner,
                delta_runner=fake_delta_runner,
                category_runner=fake_category_runner,
                pressure_runner=fake_pressure_runner,
                threshold_runner=fake_threshold_runner,
                prior_runner=fake_prior_runner,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["actual_live_balance_dollars"], 40.0)
            self.assertEqual(summary["planned_orders"], 1)
            self.assertEqual(summary["meaningful_candidates_yes_bid_ge_0_05"], 0)
            self.assertEqual(summary["watchlist_markets_observed"], 1)
            self.assertEqual(summary["top_quality_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["watch_signal_markets"], 1)
            self.assertEqual(summary["top_signal_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["persistent_watch_markets"], 1)
            self.assertEqual(summary["top_persistence_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["board_change_label"], "stale")
            self.assertEqual(summary["top_delta_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["watch_categories_observed"], 1)
            self.assertEqual(summary["pressure_watch_markets"], 1)
            self.assertEqual(summary["threshold_approaching_markets"], 1)
            self.assertEqual(summary["top_threshold_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["prior_positive_yes_ask_markets"], 1)
            self.assertEqual(summary["top_prior_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["focus_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["focus_market_mode"], "threshold")
            self.assertIsNotNone(summary["focus_dossier_action_hint"])
            self.assertEqual(summary["top_category"], "Politics")
            self.assertEqual(summary["top_category_label"], "watch")
            self.assertIsNotNone(summary["category_concentration_warning"])
            self.assertEqual(summary["watch_history_summary"]["watch_runs_total"], 1)
            self.assertEqual(summary["board_regime"], "threshold_approaching")
            self.assertFalse(summary["trade_gate_pass"])
            self.assertEqual(summary["trade_gate_status"], "no_meaningful_candidates")
            self.assertEqual(summary["recommendation"], "review_prior_edge")
            self.assertEqual(summary["ledger_summary"]["live_submissions_today"], 0)
            self.assertTrue(Path(summary["focus_dossier_file"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_status_reuses_recent_scan_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            scan_csv = base / "kalshi_nonsports_scan_recent.csv"
            scan_csv.write_text("category\nEconomics\n", encoding="utf-8")
            target_time = datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc)
            epoch = target_time.timestamp()
            import os
            os.utime(scan_csv, (epoch, epoch))

            execute_kwargs: dict[str, object] = {}

            def fake_execute_runner(**kwargs: object) -> dict[str, object]:
                execute_kwargs.update(kwargs)
                return {
                    "actual_live_balance_dollars": 40.0,
                    "planned_orders": 0,
                    "status": "dry_run",
                    "board_warning": None,
                    "attempts": [],
                    "output_file": str(base / "execute.json"),
                    "ledger_csv": str(base / "ledger.csv"),
                }

            summary = run_kalshi_micro_status(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                ledger_csv=str(base / "ledger.csv"),
                watch_history_csv=str(base / "watch_history.csv"),
                history_csv=str(base / "history.csv"),
                execute_runner=fake_execute_runner,
                reconcile_runner=lambda **kwargs: {"status": "no_order_ids", "output_file": str(base / "reconcile.json")},
                quality_runner=lambda **kwargs: {"meaningful_markets": 0, "watchlist_markets": 0, "top_markets": [], "output_file": str(base / "quality.json")},
                signal_runner=lambda **kwargs: {"eligible_markets": 0, "watch_markets": 0, "top_markets": [], "output_file": str(base / "signals.json")},
                persistence_runner=lambda **kwargs: {"persistent_tradeable_markets": 0, "persistent_watch_markets": 0, "recurring_markets": 0, "top_markets": [], "output_file": str(base / "persistence.json")},
                delta_runner=lambda **kwargs: {"board_change_label": "stale", "improved_two_sided_markets": 0, "newly_tradeable_markets": 0, "top_markets": [], "output_file": str(base / "deltas.json")},
                category_runner=lambda **kwargs: {"tradeable_categories": 0, "watch_categories": 0, "thin_categories": 0, "top_categories": [], "concentration_warning": None, "output_file": str(base / "categories.json")},
                pressure_runner=lambda **kwargs: {"build_markets": 0, "watch_markets": 0, "top_build_market_ticker": None, "top_build_category": None, "output_file": str(base / "pressure.json")},
                threshold_runner=lambda **kwargs: {"approaching_markets": 0, "building_markets": 0, "top_approaching_market_ticker": None, "top_approaching_category": None, "top_approaching_hours_to_tradeable": None, "output_file": str(base / "thresholds.json")},
                prior_runner=lambda **kwargs: {"matched_live_markets": 0, "positive_edge_yes_bid_markets": 0, "positive_edge_yes_ask_markets": 0, "top_market_ticker": None, "top_market_edge_to_yes_ask": None, "output_file": str(base / "priors.json")},
                now=target_time,
            )

            self.assertEqual(execute_kwargs["scan_csv"], str(scan_csv))
            self.assertEqual(summary["reused_scan_csv"], str(scan_csv))

    def test_run_kalshi_micro_status_downgrades_to_monitor_focus_market(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            watch_history_csv = base / "watch_history.csv"
            watch_history_csv.write_text(
                (
                    "recorded_at,capture_status,capture_scan_status,status_recommendation,status_trade_gate_status,"
                    "trade_gate_pass,meaningful_candidates_yes_bid_ge_0_05,persistent_tradeable_markets,"
                    "improved_two_sided_markets,pressure_build_markets,threshold_approaching_markets,"
                    "top_pressure_market_ticker,top_threshold_market_ticker,board_change_label,top_category,"
                    "top_category_label,category_concentration_warning\n"
                    "2026-03-27T20:00:00+00:00,status_only,dry_run,review_pressure_build,no_meaningful_candidates,"
                    "false,0,0,0,1,0,KXTEST-1,,stale,Politics,watch,\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_status(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                ledger_csv=str(base / "ledger.csv"),
                watch_history_csv=str(watch_history_csv),
                execute_runner=lambda **kwargs: {
                    "actual_live_balance_dollars": 40.0,
                    "planned_orders": 1,
                    "status": "dry_run",
                    "board_warning": None,
                    "attempts": [{"market_ticker": "KXTEST-1", "planned_yes_bid_dollars": 0.02}],
                    "output_file": str(base / "execute.json"),
                    "ledger_csv": str(base / "ledger.csv"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "status_counts": {},
                    "total_market_exposure_dollars": 0.0,
                    "total_realized_pnl_dollars": 0.0,
                    "total_fees_paid_dollars": 0.0,
                    "output_file": str(base / "reconcile.json"),
                },
                quality_runner=lambda **kwargs: {"meaningful_markets": 0, "watchlist_markets": 0, "top_markets": [], "output_file": str(base / "quality.json")},
                signal_runner=lambda **kwargs: {"eligible_markets": 0, "watch_markets": 0, "top_markets": [], "output_file": str(base / "signals.json")},
                persistence_runner=lambda **kwargs: {"persistent_tradeable_markets": 0, "persistent_watch_markets": 0, "recurring_markets": 0, "top_markets": [], "output_file": str(base / "persistence.json")},
                delta_runner=lambda **kwargs: {"board_change_label": "stale", "improved_two_sided_markets": 0, "newly_tradeable_markets": 0, "top_markets": [], "output_file": str(base / "deltas.json")},
                category_runner=lambda **kwargs: {"tradeable_categories": 0, "watch_categories": 1, "thin_categories": 0, "top_categories": [{"category": "Politics", "category_label": "watch"}], "concentration_warning": None, "output_file": str(base / "categories.json")},
                pressure_runner=lambda **kwargs: {"build_markets": 1, "watch_markets": 0, "top_build_market_ticker": "KXTEST-1", "top_build_category": "Politics", "output_file": str(base / "pressure.json")},
                threshold_runner=lambda **kwargs: {"approaching_markets": 0, "building_markets": 1, "top_approaching_market_ticker": None, "top_approaching_category": None, "top_approaching_hours_to_tradeable": None, "output_file": str(base / "thresholds.json")},
                prior_runner=lambda **kwargs: {"matched_live_markets": 0, "positive_edge_yes_bid_markets": 0, "positive_edge_yes_ask_markets": 0, "top_market_ticker": None, "top_market_edge_to_yes_ask": None, "output_file": str(base / "priors.json")},
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["focus_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["focus_market_state"], "sustained_pressure_focus")
            self.assertEqual(summary["recommendation"], "monitor_focus_market")

    def test_run_kalshi_micro_status_flags_balance_connection_issue_separately_from_funding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_status(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                ledger_csv=str(base / "ledger.csv"),
                watch_history_csv=str(base / "watch_history.csv"),
                execute_runner=lambda **kwargs: {
                    "actual_live_balance_dollars": None,
                    "actual_live_balance_source": "unknown",
                    "balance_live_verified": False,
                    "balance_check_error": "temporary dns failure",
                    "planned_orders": 1,
                    "status": "dry_run",
                    "board_warning": None,
                    "attempts": [{"market_ticker": "KXTEST-1", "planned_yes_bid_dollars": 0.02}],
                    "output_file": str(base / "execute.json"),
                    "ledger_csv": str(base / "ledger.csv"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "status_counts": {},
                    "total_market_exposure_dollars": 0.0,
                    "total_realized_pnl_dollars": 0.0,
                    "total_fees_paid_dollars": 0.0,
                    "output_file": str(base / "reconcile.json"),
                },
                quality_runner=lambda **kwargs: {"meaningful_markets": 0, "watchlist_markets": 0, "top_markets": [], "output_file": str(base / "quality.json")},
                signal_runner=lambda **kwargs: {"eligible_markets": 0, "watch_markets": 0, "top_markets": [], "output_file": str(base / "signals.json")},
                persistence_runner=lambda **kwargs: {"persistent_tradeable_markets": 0, "persistent_watch_markets": 0, "recurring_markets": 0, "top_markets": [], "output_file": str(base / "persistence.json")},
                delta_runner=lambda **kwargs: {"board_change_label": "stale", "improved_two_sided_markets": 0, "newly_tradeable_markets": 0, "top_markets": [], "output_file": str(base / "deltas.json")},
                category_runner=lambda **kwargs: {"tradeable_categories": 0, "watch_categories": 0, "thin_categories": 0, "top_categories": [], "concentration_warning": None, "output_file": str(base / "categories.json")},
                pressure_runner=lambda **kwargs: {"build_markets": 0, "watch_markets": 0, "top_build_market_ticker": None, "top_build_category": None, "output_file": str(base / "pressure.json")},
                threshold_runner=lambda **kwargs: {"approaching_markets": 0, "building_markets": 0, "top_approaching_market_ticker": None, "top_approaching_category": None, "top_approaching_hours_to_tradeable": None, "output_file": str(base / "thresholds.json")},
                prior_runner=lambda **kwargs: {"matched_live_markets": 0, "positive_edge_yes_bid_markets": 0, "positive_edge_yes_ask_markets": 0, "positive_edge_no_ask_markets": 0, "top_market_ticker": None, "top_market_edge_to_yes_ask": None, "output_file": str(base / "priors.json")},
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["trade_gate_status"], "balance_unavailable")
            self.assertEqual(summary["recommendation"], "check_balance_connection")

    def test_run_kalshi_micro_status_flags_open_order_state_inconsistency_under_upstream_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            book_db_path = ensure_book_schema(default_book_db_path(str(base)))
            with sqlite3.connect(book_db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO positions (
                        ticker, position_fp, market_exposure_dollars, realized_pnl_dollars,
                        fees_paid_dollars, resting_orders_count, updated_at, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "KXTEST-1",
                        2.0,
                        1.0,
                        0.0,
                        0.0,
                        3,
                        "2026-03-27T21:00:00+00:00",
                        "{}",
                    ),
                )
                conn.commit()

            summary = run_kalshi_micro_status(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                ledger_csv=str(base / "ledger.csv"),
                watch_history_csv=str(base / "watch_history.csv"),
                execute_runner=lambda **kwargs: {
                    "actual_live_balance_dollars": 40.0,
                    "planned_orders": 0,
                    "status": "upstream_error",
                    "board_warning": None,
                    "attempts": [],
                    "output_file": str(base / "execute.json"),
                    "ledger_csv": str(base / "ledger.csv"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "status_counts": {},
                    "total_market_exposure_dollars": 0.0,
                    "total_realized_pnl_dollars": 0.0,
                    "total_fees_paid_dollars": 0.0,
                    "output_file": str(base / "reconcile.json"),
                },
                quality_runner=lambda **kwargs: {"meaningful_markets": 0, "watchlist_markets": 0, "top_markets": [], "output_file": str(base / "quality.json")},
                signal_runner=lambda **kwargs: {"eligible_markets": 0, "watch_markets": 0, "top_markets": [], "output_file": str(base / "signals.json")},
                persistence_runner=lambda **kwargs: {"persistent_tradeable_markets": 0, "persistent_watch_markets": 0, "recurring_markets": 0, "top_markets": [], "output_file": str(base / "persistence.json")},
                delta_runner=lambda **kwargs: {"board_change_label": "stale", "improved_two_sided_markets": 0, "newly_tradeable_markets": 0, "top_markets": [], "output_file": str(base / "deltas.json")},
                category_runner=lambda **kwargs: {"tradeable_categories": 0, "watch_categories": 0, "thin_categories": 0, "top_categories": [], "concentration_warning": None, "output_file": str(base / "categories.json")},
                pressure_runner=lambda **kwargs: {"build_markets": 0, "watch_markets": 0, "top_build_market_ticker": None, "top_build_category": None, "output_file": str(base / "pressure.json")},
                threshold_runner=lambda **kwargs: {"approaching_markets": 0, "building_markets": 0, "top_approaching_market_ticker": None, "top_approaching_category": None, "top_approaching_hours_to_tradeable": None, "output_file": str(base / "thresholds.json")},
                prior_runner=lambda **kwargs: {"matched_live_markets": 0, "positive_edge_yes_bid_markets": 0, "positive_edge_yes_ask_markets": 0, "positive_edge_no_ask_markets": 0, "top_market_ticker": None, "top_market_edge_to_yes_ask": None, "output_file": str(base / "priors.json")},
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["open_order_state_consistency"], "inconsistent")
            self.assertIn("positions table indicates resting orders", summary["open_order_state_warning"])
            self.assertEqual(summary["recommendation"], "restore_connectivity_then_reconcile_open_orders")
            self.assertEqual(summary["trade_gate_status"], "open_order_state_inconsistent")


if __name__ == "__main__":
    unittest.main()
