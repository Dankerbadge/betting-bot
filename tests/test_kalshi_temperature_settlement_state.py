from __future__ import annotations

import csv
from datetime import datetime, timezone
import unittest

from betbot.kalshi_temperature_settlement_state import run_kalshi_temperature_settlement_state


class KalshiTemperatureSettlementStateTests(unittest.TestCase):
    def test_builds_underlyings_and_blocks_past_dates(self) -> None:
        with self.subTest("build settlement state"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmp:
                specs_csv = f"{tmp}/kalshi_temperature_contract_specs_20260410_000000.csv"
                with open(specs_csv, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "series_ticker",
                            "event_ticker",
                            "market_ticker",
                            "contract_family",
                            "settlement_station",
                            "settlement_timezone",
                            "target_date_local",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "series_ticker": "KXHIGHAUS",
                            "event_ticker": "KXHIGHAUS-26APR10",
                            "market_ticker": "KXHIGHAUS-26APR10-T84",
                            "contract_family": "daily_temperature",
                            "settlement_station": "KAUS",
                            "settlement_timezone": "UTC",
                            "target_date_local": "2026-04-10",
                        }
                    )
                    writer.writerow(
                        {
                            "series_ticker": "KXHIGHCHI",
                            "event_ticker": "KXHIGHCHI-26APR09",
                            "market_ticker": "KXHIGHCHI-26APR09-B65.5",
                            "contract_family": "daily_temperature",
                            "settlement_station": "KMDW",
                            "settlement_timezone": "UTC",
                            "target_date_local": "2026-04-09",
                        }
                    )

                constraint_csv = f"{tmp}/kalshi_temperature_constraint_scan_20260410_000000.csv"
                with open(constraint_csv, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "market_ticker",
                            "observed_max_settlement_quantized",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "market_ticker": "KXHIGHAUS-26APR10-T84",
                            "observed_max_settlement_quantized": "82",
                        }
                    )

                summary = run_kalshi_temperature_settlement_state(
                    specs_csv=specs_csv,
                    constraint_csv=constraint_csv,
                    output_dir=tmp,
                    now=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
                )

                self.assertEqual(summary["status"], "ready")
                self.assertEqual(summary["underlying_count"], 2)
                self.assertEqual(summary["blocked_underlyings"], 1)
                self.assertTrue(summary.get("output_file"))

                underlyings = summary["underlyings"]
                key_today = "KXHIGHAUS|KAUS|2026-04-10"
                key_past = "KXHIGHCHI|KMDW|2026-04-09"

                self.assertIn(key_today, underlyings)
                self.assertIn(key_past, underlyings)

                self.assertEqual(underlyings[key_today]["state"], "intraday_unfinalized")
                self.assertTrue(underlyings[key_today]["allow_new_orders"])
                self.assertEqual(underlyings[key_today]["fast_truth_value"], 82.0)

                self.assertEqual(underlyings[key_past]["state"], "pending_final_report")
                self.assertFalse(underlyings[key_past]["allow_new_orders"])


if __name__ == "__main__":
    unittest.main()

