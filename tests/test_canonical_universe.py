from datetime import datetime, timezone
import csv
from pathlib import Path
import tempfile
import unittest

from betbot.canonical_universe import (
    MAPPING_FIELDNAMES,
    THRESHOLD_FIELDNAMES,
    run_canonical_universe,
)


class CanonicalUniverseTests(unittest.TestCase):
    def _read_csv(self, path: Path) -> list[dict[str, str]]:
        with path.open("r", newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    def test_run_canonical_universe_writes_mapping_and_threshold_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            summary = run_canonical_universe(
                output_dir=tmp,
                now=datetime(2026, 3, 29, 16, 45, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["canonical_ticker_count"], 40)
            self.assertEqual(summary["macro_release_count"], 20)
            self.assertEqual(summary["weather_energy_count"], 20)

            mapping_path = Path(summary["mapping_csv"])
            threshold_path = Path(summary["threshold_csv"])
            self.assertTrue(mapping_path.exists())
            self.assertTrue(threshold_path.exists())
            self.assertTrue(Path(summary["output_file"]).exists())

            mapping_rows = self._read_csv(mapping_path)
            threshold_rows = self._read_csv(threshold_path)
            self.assertEqual(len(mapping_rows), 40)
            self.assertEqual(len(threshold_rows), 40)
            self.assertEqual(list(mapping_rows[0].keys()), MAPPING_FIELDNAMES)
            self.assertEqual(list(threshold_rows[0].keys()), THRESHOLD_FIELDNAMES)

            tickers = {row["canonical_ticker"] for row in mapping_rows}
            self.assertIn("MX01_CPI_HEADLINE_MOM", tickers)
            self.assertIn("EN20_MIDWEST_RES_PROPANE_PRICE", tickers)

            fomc_row = next(row for row in threshold_rows if row["canonical_ticker"] == "MX20_FOMC_TARGET_RANGE")
            self.assertEqual(fomc_row["entry_min_edge_net"], "0.015")

    def test_run_canonical_universe_preserves_existing_mapping_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mapping_path = Path(tmp) / "canonical_contract_mapping.csv"
            with mapping_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=MAPPING_FIELDNAMES)
                writer.writeheader()
                writer.writerow(
                    {
                        "canonical_ticker": "MX01_CPI_HEADLINE_MOM",
                        "niche": "macro_release",
                        "execution_phase": "phase1_live",
                        "market_description": "",
                        "settlement_source": "",
                        "settlement_source_url": "",
                        "release_time_et": "",
                        "schedule_source_url": "",
                        "schedule_needs_nightly_poll": "true",
                        "schedule_holiday_shift_risk": "true",
                        "source_timestamp_rule": "",
                        "mispricing_hypothesis": "",
                        "confounders": "",
                        "mapping_status": "mapped",
                        "live_event_ticker": "KXINFLATIONCPI",
                        "live_market_ticker": "KXINFLATIONCPI-26APR",
                        "mapping_confidence": "0.86",
                        "mapping_notes": "manual map retained",
                        "last_mapped_at": "2026-03-29T12:00:00-04:00",
                    }
                )

            summary = run_canonical_universe(
                output_dir=tmp,
                now=datetime(2026, 3, 29, 17, 0, tzinfo=timezone.utc),
            )
            mapping_rows = self._read_csv(Path(summary["mapping_csv"]))

            mx01 = next(row for row in mapping_rows if row["canonical_ticker"] == "MX01_CPI_HEADLINE_MOM")
            self.assertEqual(mx01["mapping_status"], "mapped")
            self.assertEqual(mx01["live_event_ticker"], "KXINFLATIONCPI")
            self.assertEqual(mx01["live_market_ticker"], "KXINFLATIONCPI-26APR")
            self.assertEqual(mx01["mapping_confidence"], "0.86")
            self.assertEqual(mx01["mapping_notes"], "manual map retained")


if __name__ == "__main__":
    unittest.main()
