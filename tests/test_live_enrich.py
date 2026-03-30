import csv
from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.live_enrich import run_live_candidate_enrichment


class LiveEnrichTests(unittest.TestCase):
    def _write_candidate_csv(self, path: Path) -> None:
        rows = [
            {
                "timestamp": "2026-03-28T19:00:00-04:00",
                "event_id": "evt1|moneyline|Boston Celtics|moneyline",
                "selection": "Boston Celtics ML",
                "odds": "1.95",
                "model_prob": "0.500000",
                "decision_prob": "0.500000",
                "market": "moneyline",
                "sport_id": "4",
            },
            {
                "timestamp": "2026-03-28T19:00:00-04:00",
                "event_id": "evt1|handicap|Boston Celtics|3.5",
                "selection": "Boston Celtics -3.5",
                "odds": "1.91",
                "model_prob": "0.520000",
                "decision_prob": "0.520000",
                "market": "handicap",
                "sport_id": "4",
            },
        ]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    def test_run_live_candidate_enrichment_adjusts_fresh_moneyline_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            candidate_csv = base / "candidates.csv"
            evidence_csv = base / "evidence.csv"
            self._write_candidate_csv(candidate_csv)

            observed_at = datetime(2026, 3, 28, 22, 0, tzinfo=timezone.utc).isoformat()
            with evidence_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "selection",
                        "observed_at",
                        "availability_signal",
                        "lineup_signal",
                        "news_signal",
                        "source_confidence",
                        "source_count",
                        "conflict_flag",
                        "source_note",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "selection": "Boston Celtics ML",
                        "observed_at": observed_at,
                        "availability_signal": "0.20",
                        "lineup_signal": "0.10",
                        "news_signal": "0.00",
                        "source_confidence": "1.0",
                        "source_count": "3",
                        "conflict_flag": "false",
                        "source_note": "starter available",
                    }
                )

            summary = run_live_candidate_enrichment(
                candidate_csv=str(candidate_csv),
                output_dir=str(base),
                evidence_csv=str(evidence_csv),
                freshness_hours=12.0,
                max_logit_shift=0.35,
                moneyline_only=True,
                now=datetime(2026, 3, 28, 23, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["rows_adjusted"], 1)
            self.assertEqual(summary["rows_moneyline_filtered"], 1)
            output_csv = Path(summary["output_csv"])
            self.assertTrue(output_csv.exists())

            with output_csv.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            adjusted = rows[0]
            filtered = rows[1]
            self.assertEqual(adjusted["enrichment_applied"], "true")
            self.assertEqual(adjusted["enrichment_reason"], "adjusted")
            self.assertGreater(float(adjusted["model_prob"]), float(adjusted["model_prob_base"]))
            self.assertEqual(adjusted["model_prob"], adjusted["decision_prob"])
            self.assertEqual(filtered["enrichment_reason"], "market_not_moneyline")
            self.assertEqual(filtered["enrichment_applied"], "false")

    def test_run_live_candidate_enrichment_skips_stale_and_conflicting_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            candidate_csv = base / "candidates.csv"
            evidence_csv = base / "evidence.csv"
            self._write_candidate_csv(candidate_csv)

            stale_observed_at = datetime(2026, 3, 27, 22, 0, tzinfo=timezone.utc).isoformat()
            with evidence_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "selection",
                        "observed_at",
                        "availability_signal",
                        "lineup_signal",
                        "news_signal",
                        "source_confidence",
                        "source_count",
                        "conflict_flag",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "selection": "Boston Celtics ML",
                        "observed_at": stale_observed_at,
                        "availability_signal": "0.2",
                        "lineup_signal": "0.0",
                        "news_signal": "0.0",
                        "source_confidence": "1.0",
                        "source_count": "2",
                        "conflict_flag": "false",
                    }
                )

            summary = run_live_candidate_enrichment(
                candidate_csv=str(candidate_csv),
                output_dir=str(base),
                evidence_csv=str(evidence_csv),
                freshness_hours=4.0,
                now=datetime(2026, 3, 28, 23, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "no_adjustments")
            self.assertEqual(summary["rows_adjusted"], 0)
            self.assertEqual(summary["rows_stale_evidence"], 1)

            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["enrichment_reason"], "stale_evidence")
            self.assertEqual(rows[0]["enrichment_applied"], "false")
            self.assertEqual(rows[0]["model_prob"], rows[0]["model_prob_base"])

    def test_run_live_candidate_enrichment_handles_missing_evidence_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            candidate_csv = base / "candidates.csv"
            self._write_candidate_csv(candidate_csv)

            summary = run_live_candidate_enrichment(
                candidate_csv=str(candidate_csv),
                output_dir=str(base),
                evidence_csv=str(base / "missing.csv"),
            )

            self.assertEqual(summary["status"], "missing_evidence")
            self.assertEqual(summary["rows_adjusted"], 0)
            self.assertTrue(Path(summary["output_csv"]).exists())


if __name__ == "__main__":
    unittest.main()
