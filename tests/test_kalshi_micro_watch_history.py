import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_watch_history import append_watch_history, summarize_watch_history


class KalshiMicroWatchHistoryTests(unittest.TestCase):
    def test_summarize_watch_history_tracks_streaks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watch_history.csv"
            append_watch_history(
                path,
                {
                    "recorded_at": "2026-03-27T21:00:00+00:00",
                    "status_recommendation": "hold_penny_markets_only",
                    "status_trade_gate_status": "no_meaningful_candidates",
                    "board_change_label": "stale",
                    "meaningful_candidates_yes_bid_ge_0_05": 0,
                    "pressure_build_markets": 0,
                    "threshold_approaching_markets": 0,
                    "top_pressure_market_ticker": "",
                    "top_threshold_market_ticker": "",
                },
            )
            append_watch_history(
                path,
                {
                    "recorded_at": "2026-03-27T22:00:00+00:00",
                    "status_recommendation": "hold_penny_markets_only",
                    "status_trade_gate_status": "no_meaningful_candidates",
                    "board_change_label": "improving",
                    "meaningful_candidates_yes_bid_ge_0_05": 0,
                    "pressure_build_markets": 0,
                    "threshold_approaching_markets": 0,
                    "top_pressure_market_ticker": "",
                    "top_threshold_market_ticker": "",
                },
            )
            append_watch_history(
                path,
                {
                    "recorded_at": "2026-03-27T23:00:00+00:00",
                    "status_recommendation": "review_board_improvement",
                    "status_trade_gate_status": "no_meaningful_candidates",
                    "board_change_label": "improving",
                    "meaningful_candidates_yes_bid_ge_0_05": 0,
                    "pressure_build_markets": 0,
                    "threshold_approaching_markets": 0,
                    "top_pressure_market_ticker": "",
                    "top_threshold_market_ticker": "",
                },
            )

            summary = summarize_watch_history(path)

            self.assertEqual(summary["watch_runs_total"], 3)
            self.assertEqual(summary["latest_recommendation"], "review_board_improvement")
            self.assertEqual(summary["recommendation_streak"], 1)
            self.assertEqual(summary["trade_gate_status_streak"], 3)
            self.assertEqual(summary["recent_improving_runs"], 2)
            self.assertEqual(summary["board_regime"], "improving_but_thin")

    def test_summarize_watch_history_flags_concentrated_penny_noise(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watch_history.csv"
            for hour in range(3):
                append_watch_history(
                    path,
                    {
                        "recorded_at": f"2026-03-27T2{hour}:00:00+00:00",
                        "status_recommendation": "hold_penny_markets_only",
                        "status_trade_gate_status": "no_meaningful_candidates",
                        "trade_gate_pass": "false",
                        "meaningful_candidates_yes_bid_ge_0_05": 0,
                        "persistent_tradeable_markets": 0,
                        "pressure_build_markets": 0,
                        "threshold_approaching_markets": 0,
                        "top_pressure_market_ticker": "",
                        "top_threshold_market_ticker": "",
                        "board_change_label": "stale",
                        "category_concentration_warning": "Two-sided liquidity is heavily concentrated in Politics.",
                    },
                )

            summary = summarize_watch_history(path)

            self.assertEqual(summary["board_regime"], "concentrated_penny_noise")

    def test_summarize_watch_history_flags_pressure_building(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watch_history.csv"
            append_watch_history(
                path,
                {
                    "recorded_at": "2026-03-27T21:00:00+00:00",
                    "status_recommendation": "review_pressure_build",
                    "status_trade_gate_status": "no_meaningful_candidates",
                    "trade_gate_pass": "false",
                    "meaningful_candidates_yes_bid_ge_0_05": 0,
                    "persistent_tradeable_markets": 0,
                    "pressure_build_markets": 1,
                    "threshold_approaching_markets": 0,
                    "top_pressure_market_ticker": "KXTEST-1",
                    "top_threshold_market_ticker": "",
                    "board_change_label": "stale",
                },
            )

            summary = summarize_watch_history(path)

            self.assertEqual(summary["board_regime"], "pressure_building")
            self.assertEqual(summary["latest_focus_market_ticker"], "KXTEST-1")

    def test_summarize_watch_history_flags_threshold_approaching(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watch_history.csv"
            append_watch_history(
                path,
                {
                    "recorded_at": "2026-03-27T21:00:00+00:00",
                    "status_recommendation": "review_threshold_approach",
                    "status_trade_gate_status": "no_meaningful_candidates",
                    "trade_gate_pass": "false",
                    "meaningful_candidates_yes_bid_ge_0_05": 0,
                    "persistent_tradeable_markets": 0,
                    "pressure_build_markets": 1,
                    "threshold_approaching_markets": 1,
                    "top_pressure_market_ticker": "KXTEST-1",
                    "top_threshold_market_ticker": "KXTEST-1",
                    "board_change_label": "stale",
                },
            )

            summary = summarize_watch_history(path)

            self.assertEqual(summary["board_regime"], "threshold_approaching")
            self.assertEqual(summary["latest_focus_market_mode"], "threshold")

    def test_summarize_watch_history_tracks_sustained_focus_market(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watch_history.csv"
            for hour in range(3):
                append_watch_history(
                    path,
                    {
                        "recorded_at": f"2026-03-27T2{hour}:00:00+00:00",
                        "status_recommendation": "review_pressure_build",
                        "status_trade_gate_status": "no_meaningful_candidates",
                        "trade_gate_pass": "false",
                        "meaningful_candidates_yes_bid_ge_0_05": 0,
                        "persistent_tradeable_markets": 0,
                        "pressure_build_markets": 1,
                        "threshold_approaching_markets": 0,
                        "top_pressure_market_ticker": "KXTEST-1",
                        "top_threshold_market_ticker": "",
                        "board_change_label": "stale",
                    },
                )

            summary = summarize_watch_history(path)

            self.assertEqual(summary["focus_market_streak"], 3)
            self.assertEqual(summary["focus_market_state"], "stalled_pressure_focus")

    def test_append_watch_history_migrates_older_header(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "watch_history.csv"
            path.write_text(
                (
                    "recorded_at,capture_status,capture_scan_status,status_recommendation,status_trade_gate_status,"
                    "trade_gate_pass,meaningful_candidates_yes_bid_ge_0_05,persistent_tradeable_markets,"
                    "improved_two_sided_markets,board_change_label,top_category,top_category_label,"
                    "category_concentration_warning\n"
                    "2026-03-27T21:00:00+00:00,status_only,dry_run,hold_penny_markets_only,no_meaningful_candidates,"
                    "false,0,0,0,stale,Politics,watch,\n"
                ),
                encoding="utf-8",
            )

            append_watch_history(
                path,
                {
                    "recorded_at": "2026-03-27T22:00:00+00:00",
                    "status_recommendation": "review_pressure_build",
                    "status_trade_gate_status": "no_meaningful_candidates",
                    "trade_gate_pass": "false",
                    "pressure_build_markets": 1,
                },
            )

            summary = summarize_watch_history(path)

            self.assertEqual(summary["watch_runs_total"], 2)
            self.assertEqual(summary["recent_pressure_build_runs"], 1)


if __name__ == "__main__":
    unittest.main()
