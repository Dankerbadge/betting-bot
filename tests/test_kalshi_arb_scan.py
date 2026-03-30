from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_arb_scan import build_mutually_exclusive_arb_rows, run_kalshi_arb_scan


class KalshiArbScanTests(unittest.TestCase):
    def test_build_mutually_exclusive_arb_rows_finds_positive_margin(self) -> None:
        rows = build_mutually_exclusive_arb_rows(
            events=[
                {
                    "category": "Economics",
                    "series_ticker": "KXECON",
                    "event_ticker": "KXECON-1",
                    "title": "Test event",
                    "mutually_exclusive": True,
                    "markets": [
                        {"status": "active", "ticker": "A", "title": "A", "yes_ask_dollars": "0.31"},
                        {"status": "active", "ticker": "B", "title": "B", "yes_ask_dollars": "0.32"},
                        {"status": "active", "ticker": "C", "title": "C", "yes_ask_dollars": "0.33"},
                    ],
                }
            ],
            fee_buffer_per_contract_dollars=0.01,
            min_margin_dollars=0.0,
        )

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["bundle_cost_dollars"], 0.96, places=6)
        self.assertAlmostEqual(rows[0]["expected_margin_dollars"], 0.01, places=6)
        self.assertTrue(rows[0]["is_opportunity"])

    def test_run_kalshi_arb_scan_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            def fake_http_get_json(url: str, headers: dict[str, str], timeout_seconds: float):
                return 200, {
                    "events": [
                        {
                            "category": "Economics",
                            "series_ticker": "KXECON",
                            "event_ticker": "KXECON-1",
                            "title": "Test event",
                            "mutually_exclusive": True,
                            "markets": [
                                {"status": "active", "ticker": "A", "title": "A", "yes_ask_dollars": "0.31"},
                                {"status": "active", "ticker": "B", "title": "B", "yes_ask_dollars": "0.32"},
                                {"status": "active", "ticker": "C", "title": "C", "yes_ask_dollars": "0.33"},
                            ],
                        }
                    ]
                }

            summary = run_kalshi_arb_scan(
                env_file=str(env_file),
                output_dir=str(base),
                fee_buffer_per_contract_dollars=0.01,
                http_get_json=fake_http_get_json,
                now=datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["mutually_exclusive_opportunities"], 1)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()

