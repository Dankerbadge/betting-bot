from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_focus_dossier import build_focus_dossier, run_kalshi_focus_dossier


HISTORY_FIELDNAMES = [
    "captured_at",
    "category",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "event_title",
    "market_title",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_bid_size_contracts",
    "yes_ask_dollars",
    "yes_ask_size_contracts",
    "spread_dollars",
    "liquidity_dollars",
    "volume_24h_contracts",
    "open_interest_contracts",
    "ten_dollar_fillable_at_best_ask",
    "two_sided_book",
    "execution_fit_score",
]

PRIOR_FIELDNAMES = [
    "market_ticker",
    "fair_yes_probability",
    "confidence",
    "thesis",
    "source_note",
    "updated_at",
]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class KalshiFocusDossierTests(unittest.TestCase):
    def test_build_focus_dossier_uses_watch_focus_and_prior_context(self) -> None:
        history_rows = [
            {
                "captured_at": "2026-03-27T20:00:00+00:00",
                "category": "Politics",
                "series_ticker": "KXTEST",
                "event_ticker": "KXTEST-26",
                "market_ticker": "KXTEST-1",
                "event_title": "Test Event?",
                "market_title": "Will test event happen?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "100",
                "yes_bid_dollars": "0.02",
                "yes_bid_size_contracts": "10",
                "yes_ask_dollars": "0.04",
                "yes_ask_size_contracts": "12",
                "spread_dollars": "0.02",
                "liquidity_dollars": "0",
                "volume_24h_contracts": "100",
                "open_interest_contracts": "1000",
                "ten_dollar_fillable_at_best_ask": "false",
                "two_sided_book": "true",
                "execution_fit_score": "40",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "category": "Politics",
                "series_ticker": "KXTEST",
                "event_ticker": "KXTEST-26",
                "market_ticker": "KXTEST-1",
                "event_title": "Test Event?",
                "market_title": "Will test event happen?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "99",
                "yes_bid_dollars": "0.03",
                "yes_bid_size_contracts": "11",
                "yes_ask_dollars": "0.04",
                "yes_ask_size_contracts": "13",
                "spread_dollars": "0.01",
                "liquidity_dollars": "0",
                "volume_24h_contracts": "150",
                "open_interest_contracts": "1010",
                "ten_dollar_fillable_at_best_ask": "false",
                "two_sided_book": "true",
                "execution_fit_score": "45",
            },
            {
                "captured_at": "2026-03-27T22:00:00+00:00",
                "category": "Politics",
                "series_ticker": "KXTEST",
                "event_ticker": "KXTEST-26",
                "market_ticker": "KXTEST-1",
                "event_title": "Test Event?",
                "market_title": "Will test event happen?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "98",
                "yes_bid_dollars": "0.04",
                "yes_bid_size_contracts": "12",
                "yes_ask_dollars": "0.05",
                "yes_ask_size_contracts": "14",
                "spread_dollars": "0.01",
                "liquidity_dollars": "0",
                "volume_24h_contracts": "175",
                "open_interest_contracts": "1020",
                "ten_dollar_fillable_at_best_ask": "true",
                "two_sided_book": "true",
                "execution_fit_score": "47",
            },
        ]
        watch_history_summary = {
            "board_regime": "pressure_building",
            "board_regime_reason": "A market is building.",
            "latest_focus_market_mode": "pressure",
            "latest_focus_market_ticker": "KXTEST-1",
            "focus_market_state": "stalled_pressure_focus",
            "focus_market_state_reason": "Still not approaching the threshold.",
        }
        prior_rows = [
            {
                "market_ticker": "KXTEST-1",
                "fair_yes_probability": "0.08",
                "confidence": "0.7",
                "thesis": "Test thesis",
                "source_note": "Test note",
                "updated_at": "2026-03-27T22:00:00+00:00",
            }
        ]

        dossier = build_focus_dossier(
            history_rows=history_rows,
            watch_history_summary=watch_history_summary,
            prior_rows=prior_rows,
        )

        self.assertEqual(dossier["focus_market_ticker"], "KXTEST-1")
        self.assertEqual(dossier["focus_market_source"], "watch_history")
        self.assertEqual(dossier["pressure_label"], "build")
        self.assertEqual(dossier["threshold_label"], "approaching")
        self.assertEqual(dossier["prior_edge_to_yes_ask"], 0.03)
        self.assertEqual(dossier["prior_edge_to_no_ask"], -0.04)
        self.assertEqual(dossier["prior_best_entry_side"], "yes")
        self.assertEqual(dossier["action_hint"], "review_modeled_edge")
        self.assertIsNone(dossier["research_prompt"])
        self.assertEqual(len(dossier["recent_observations"]), 3)

    def test_run_kalshi_focus_dossier_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            watch_history_csv = base / "watch_history.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-27T20:00:00+00:00",
                        "category": "Politics",
                        "series_ticker": "KXTEST",
                        "event_ticker": "KXTEST-26",
                        "market_ticker": "KXTEST-1",
                        "event_title": "Test Event?",
                        "market_title": "Will test event happen?",
                        "close_time": "2026-04-01T03:59:00+00:00",
                        "hours_to_close": "100",
                        "yes_bid_dollars": "0.02",
                        "yes_bid_size_contracts": "10",
                        "yes_ask_dollars": "0.04",
                        "yes_ask_size_contracts": "12",
                        "spread_dollars": "0.02",
                        "liquidity_dollars": "0",
                        "volume_24h_contracts": "100",
                        "open_interest_contracts": "1000",
                        "ten_dollar_fillable_at_best_ask": "false",
                        "two_sided_book": "true",
                        "execution_fit_score": "40",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "series_ticker": "KXTEST",
                        "event_ticker": "KXTEST-26",
                        "market_ticker": "KXTEST-1",
                        "event_title": "Test Event?",
                        "market_title": "Will test event happen?",
                        "close_time": "2026-04-01T03:59:00+00:00",
                        "hours_to_close": "99",
                        "yes_bid_dollars": "0.03",
                        "yes_bid_size_contracts": "11",
                        "yes_ask_dollars": "0.04",
                        "yes_ask_size_contracts": "13",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "0",
                        "volume_24h_contracts": "150",
                        "open_interest_contracts": "1010",
                        "ten_dollar_fillable_at_best_ask": "false",
                        "two_sided_book": "true",
                        "execution_fit_score": "45",
                    },
                    {
                        "captured_at": "2026-03-27T22:00:00+00:00",
                        "category": "Politics",
                        "series_ticker": "KXTEST",
                        "event_ticker": "KXTEST-26",
                        "market_ticker": "KXTEST-1",
                        "event_title": "Test Event?",
                        "market_title": "Will test event happen?",
                        "close_time": "2026-04-01T03:59:00+00:00",
                        "hours_to_close": "98",
                        "yes_bid_dollars": "0.04",
                        "yes_bid_size_contracts": "12",
                        "yes_ask_dollars": "0.05",
                        "yes_ask_size_contracts": "14",
                        "spread_dollars": "0.01",
                        "liquidity_dollars": "0",
                        "volume_24h_contracts": "175",
                        "open_interest_contracts": "1020",
                        "ten_dollar_fillable_at_best_ask": "true",
                        "two_sided_book": "true",
                        "execution_fit_score": "47",
                    },
                ],
            )
            _write_csv(
                priors_csv,
                PRIOR_FIELDNAMES,
                [
                    {
                        "market_ticker": "KXTEST-1",
                        "fair_yes_probability": "0.08",
                        "confidence": "0.7",
                        "thesis": "Test thesis",
                        "source_note": "Test note",
                        "updated_at": "2026-03-27T22:00:00+00:00",
                    }
                ],
            )
            watch_history_csv.write_text(
                (
                    "recorded_at,capture_status,capture_scan_status,status_recommendation,status_trade_gate_status,"
                    "trade_gate_pass,meaningful_candidates_yes_bid_ge_0_05,persistent_tradeable_markets,"
                    "improved_two_sided_markets,pressure_build_markets,threshold_approaching_markets,"
                    "top_pressure_market_ticker,top_threshold_market_ticker,board_change_label,top_category,"
                    "top_category_label,category_concentration_warning\n"
                    "2026-03-27T22:00:00+00:00,status_only,dry_run,review_pressure_build,no_meaningful_candidates,"
                    "false,0,0,0,1,0,KXTEST-1,,stale,Politics,watch,\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_focus_dossier(
                history_csv=str(history_csv),
                watch_history_csv=str(watch_history_csv),
                priors_csv=str(priors_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 23, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["focus_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["action_hint"], "review_modeled_edge")
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_build_focus_dossier_flags_modeled_no_edge(self) -> None:
        history_rows = [
            {
                "captured_at": "2026-03-27T20:00:00+00:00",
                "category": "Politics",
                "series_ticker": "KXTEST",
                "event_ticker": "KXTEST-26",
                "market_ticker": "KXTEST-2",
                "event_title": "Test Event?",
                "market_title": "Will test event happen?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "100",
                "yes_bid_dollars": "0.03",
                "yes_bid_size_contracts": "10",
                "yes_ask_dollars": "0.04",
                "yes_ask_size_contracts": "12",
                "spread_dollars": "0.01",
                "liquidity_dollars": "0",
                "volume_24h_contracts": "100",
                "open_interest_contracts": "1000",
                "ten_dollar_fillable_at_best_ask": "true",
                "two_sided_book": "true",
                "execution_fit_score": "40",
            },
            {
                "captured_at": "2026-03-27T21:00:00+00:00",
                "category": "Politics",
                "series_ticker": "KXTEST",
                "event_ticker": "KXTEST-26",
                "market_ticker": "KXTEST-2",
                "event_title": "Test Event?",
                "market_title": "Will test event happen?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "99",
                "yes_bid_dollars": "0.03",
                "yes_bid_size_contracts": "10",
                "yes_ask_dollars": "0.04",
                "yes_ask_size_contracts": "12",
                "spread_dollars": "0.01",
                "liquidity_dollars": "0",
                "volume_24h_contracts": "100",
                "open_interest_contracts": "1000",
                "ten_dollar_fillable_at_best_ask": "true",
                "two_sided_book": "true",
                "execution_fit_score": "40",
            },
            {
                "captured_at": "2026-03-27T22:00:00+00:00",
                "category": "Politics",
                "series_ticker": "KXTEST",
                "event_ticker": "KXTEST-26",
                "market_ticker": "KXTEST-2",
                "event_title": "Test Event?",
                "market_title": "Will test event happen?",
                "close_time": "2026-04-01T03:59:00+00:00",
                "hours_to_close": "98",
                "yes_bid_dollars": "0.03",
                "yes_bid_size_contracts": "10",
                "yes_ask_dollars": "0.04",
                "yes_ask_size_contracts": "12",
                "spread_dollars": "0.01",
                "liquidity_dollars": "0",
                "volume_24h_contracts": "100",
                "open_interest_contracts": "1000",
                "ten_dollar_fillable_at_best_ask": "true",
                "two_sided_book": "true",
                "execution_fit_score": "40",
            },
        ]
        watch_history_summary = {
            "board_regime": "pressure_building",
            "board_regime_reason": "A market is building.",
            "latest_focus_market_mode": "pressure",
            "latest_focus_market_ticker": "KXTEST-2",
            "focus_market_state": "stalled_pressure_focus",
            "focus_market_state_reason": "Still not approaching the threshold.",
        }
        prior_rows = [
            {
                "market_ticker": "KXTEST-2",
                "fair_yes_probability": "0.02",
                "confidence": "0.7",
                "thesis": "Test thesis",
                "source_note": "Test note",
                "updated_at": "2026-03-27T22:00:00+00:00",
            }
        ]

        dossier = build_focus_dossier(
            history_rows=history_rows,
            watch_history_summary=watch_history_summary,
            prior_rows=prior_rows,
        )

        self.assertEqual(dossier["prior_best_entry_side"], "no")
        self.assertEqual(dossier["prior_best_entry_edge"], 0.01)
        self.assertEqual(dossier["action_hint"], "review_modeled_no_edge")


if __name__ == "__main__":
    unittest.main()
