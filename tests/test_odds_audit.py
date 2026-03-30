import tempfile
from pathlib import Path
import unittest

from betbot.odds_audit import run_odds_audit


class OddsAuditTests(unittest.TestCase):
    def test_odds_audit_flags_invalid_odds_and_missing_prestart_quote(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            csv_path = base / "odds.csv"
            csv_path.write_text(
                (
                    "timestamp,event_id,market,book,odds,commence_time\n"
                    "2026-03-27T10:00:00,e1,mh,b1,1.95,2026-03-27T12:00:00\n"
                    "2026-03-27T13:00:00,e2,mh,b1,1.90,2026-03-27T12:00:00\n"
                    "2026-03-27T10:05:00,e3,mh,b2,1.00,2026-03-27T12:00:00\n"
                ),
                encoding="utf-8",
            )
            summary = run_odds_audit(
                input_csv=str(csv_path),
                output_dir=str(base),
                max_gap_minutes=30.0,
            )
            self.assertEqual(summary["status"], "blocked")
            self.assertGreater(summary["metrics"]["invalid_odds_count"], 0)
            self.assertGreater(summary["metrics"]["missing_prestart_quote_count"], 0)
            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertTrue(Path(summary["issues_file"]).exists())

    def test_odds_audit_ready_on_clean_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            csv_path = base / "odds.csv"
            csv_path.write_text(
                (
                    "timestamp,event_id,market,book,odds,commence_time\n"
                    "2026-03-27T10:00:00,e1,mh,b1,1.95,2026-03-27T12:00:00\n"
                    "2026-03-27T10:10:00,e1,mh,b1,1.93,2026-03-27T12:00:00\n"
                    "2026-03-27T11:50:00,e1,mh,b1,1.90,2026-03-27T12:00:00\n"
                ),
                encoding="utf-8",
            )
            summary = run_odds_audit(
                input_csv=str(csv_path),
                output_dir=str(base),
                max_gap_minutes=180.0,
            )
            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["metrics"]["invalid_odds_count"], 0)
            self.assertEqual(summary["metrics"]["missing_prestart_quote_count"], 0)


if __name__ == "__main__":
    unittest.main()

