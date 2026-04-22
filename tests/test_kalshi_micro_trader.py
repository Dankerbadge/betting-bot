from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_trader import run_kalshi_micro_trader


class KalshiMicroTraderTests(unittest.TestCase):
    def test_run_kalshi_micro_trader_holds_when_gate_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            watch_history = base / "watch_history.csv"
            watch_history.write_text(
                (
                    "recorded_at,capture_status,capture_scan_status,status_recommendation,status_trade_gate_status,"
                    "trade_gate_pass,meaningful_candidates_yes_bid_ge_0_05,persistent_tradeable_markets,"
                    "improved_two_sided_markets,board_change_label,top_category,top_category_label,"
                    "category_concentration_warning\n"
                    "2026-03-27T20:00:00+00:00,status_only,dry_run,hold_penny_markets_only,no_meaningful_candidates,"
                    "false,0,0,0,stale,Politics,watch,Two-sided liquidity is heavily concentrated in Politics.\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                watch_history_csv=str(watch_history),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                gate_runner=lambda **kwargs: {
                    "gate_pass": False,
                    "gate_status": "no_meaningful_candidates",
                    "gate_score": 6.0,
                    "gate_blockers": ["No candidate clears the $0.05 Yes-bid floor."],
                    "pressure_build_markets": 1,
                    "pressure_watch_markets": 0,
                    "top_pressure_market_ticker": "KXTEST-1",
                    "top_pressure_category": "Politics",
                    "top_category": "Politics",
                    "top_category_label": "watch",
                    "category_concentration_warning": "Two-sided liquidity is heavily concentrated in Politics.",
                    "output_file": str(base / "gate.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "hold")
            self.assertEqual(summary["action_taken"], "hold")
            self.assertEqual(summary["gate_top_category"], "Politics")
            self.assertEqual(summary["gate_top_pressure_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["watch_board_regime"], "concentrated_penny_noise")
            self.assertEqual(summary["watch_focus_market_state"], "none")
            self.assertIsNotNone(summary["focus_dossier_action_hint"])
            self.assertTrue(Path(summary["focus_dossier_file"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_trader_executes_when_gate_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                allow_live_orders=False,
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                gate_runner=lambda **kwargs: {
                    "gate_pass": True,
                    "gate_status": "pass",
                    "gate_score": 42.0,
                    "gate_blockers": [],
                    "output_file": str(base / "gate.json"),
                },
                execute_runner=lambda **kwargs: {
                    "status": "dry_run",
                    "output_file": str(base / "execute.json"),
                },
                reconcile_runner=lambda **kwargs: {
                    "status": "no_order_ids",
                    "output_file": str(base / "reconcile.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "executed")
            self.assertEqual(summary["action_taken"], "dry_run_execute_reconcile")
            self.assertEqual(summary["execute_status"], "dry_run")
            self.assertEqual(summary["reconcile_status"], "no_order_ids")
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_trader_holds_when_capture_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            summary = run_kalshi_micro_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "rate_limited",
                    "scan_error": "Kalshi events request failed with status 429",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                gate_runner=lambda **kwargs: {
                    "gate_pass": True,
                    "gate_status": "pass",
                    "gate_score": 42.0,
                    "gate_blockers": [],
                    "output_file": str(base / "gate.json"),
                },
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "hold")
            self.assertEqual(summary["gate_status"], "rate_limited")
            self.assertFalse(summary["gate_pass"])

    def test_run_kalshi_micro_trader_holds_when_gate_pass_is_false_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            def fail_execute(**kwargs):
                raise AssertionError("execute_runner should not be called when gate_pass is false-like")

            summary = run_kalshi_micro_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                gate_runner=lambda **kwargs: {
                    "gate_pass": "false",
                    "gate_status": "pass",
                    "gate_score": 42.0,
                    "gate_blockers": [],
                    "output_file": str(base / "gate.json"),
                },
                execute_runner=fail_execute,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "hold")
            self.assertFalse(summary["gate_pass"])
            self.assertEqual(summary["gate_status"], "pass")

    def test_run_kalshi_micro_trader_holds_when_gate_pass_is_invalid_string(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            def fail_execute(**kwargs):
                raise AssertionError("execute_runner should not be called when gate_pass is invalid")

            summary = run_kalshi_micro_trader(
                env_file="data/research/account_onboarding.local.env",
                output_dir=str(base),
                capture_runner=lambda **kwargs: {
                    "status": "ready",
                    "history_csv": str(base / "history.csv"),
                    "scan_summary_file": str(base / "capture.json"),
                },
                gate_runner=lambda **kwargs: {
                    "gate_pass": "MAYBE",
                    "gate_status": "pass",
                    "gate_score": 42.0,
                    "gate_blockers": [],
                    "output_file": str(base / "gate.json"),
                },
                execute_runner=fail_execute,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "hold")
            self.assertFalse(summary["gate_pass"])
            self.assertEqual(summary["gate_status"], "invalid_gate_pass")
            self.assertIn("Invalid gate_pass value", summary["gate_blockers"][0])


if __name__ == "__main__":
    unittest.main()
