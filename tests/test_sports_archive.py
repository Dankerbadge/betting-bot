import csv
from datetime import datetime, timezone
import tempfile
from pathlib import Path
import unittest

from betbot.sports_archive import run_sports_archive


class SportsArchiveTests(unittest.TestCase):
    def test_run_sports_archive_upgrades_stale_archive_header_before_append(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            archive_csv = base / "archive.csv"
            archive_csv.write_text(
                (
                    "recorded_at,sport_id,event_date,status,error,candidate_status,events_fetched,"
                    "candidates_written,positive_ev_candidates,accepted,rejected,avg_ev_accepted,"
                    "top_candidate_selection,top_candidate_market,top_candidate_book,"
                    "top_candidate_estimated_ev,live_paper_summary_file,candidate_summary_file,paper_summary_file\n"
                    "2026-03-28T07:25:43.931734+00:00,4,2026-03-28,ready,,ready,6,22,0,0,22,0.0,"
                    "Sacramento Kings ML,moneyline,BetMGM,-0.008376,/tmp/old_live_paper.json,"
                    "/tmp/old_candidates.json,/tmp/old_paper.json\n"
                ),
                encoding="utf-8",
            )

            def fake_live_paper_runner(**_: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "candidate_pull": {
                        "status": "ready",
                        "events_fetched": 5,
                        "candidates_written": 8,
                        "positive_ev_candidates": 2,
                        "top_candidates": [
                            {
                                "selection": "Boston Celtics ML",
                                "market": "moneyline",
                                "book": "DraftKings",
                                "estimated_ev": 0.031,
                                "edge_rank_score": 0.029,
                                "consensus_book_count": 3,
                                "consensus_stability": 0.972,
                                "consensus_prob_range": 0.008,
                            }
                        ],
                        "output_file": str(base / "candidates_ready.json"),
                    },
                    "paper_run": {
                        "accepted": 1,
                        "rejected": 7,
                        "avg_ev_accepted": 0.031,
                        "output_file": str(base / "paper_ready.json"),
                    },
                    "output_file": str(base / "live_paper_ready.json"),
                }

            run_sports_archive(
                env_file="data/research/account_onboarding.local.env",
                sport_id=4,
                event_dates=("2026-03-29",),
                starting_bankroll=1000.0,
                output_dir=str(base),
                archive_csv=str(archive_csv),
                live_paper_runner=fake_live_paper_runner,  # type: ignore[arg-type]
                now=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
            )

            with archive_csv.open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["live_paper_summary_file"], "/tmp/old_live_paper.json")
            self.assertEqual(rows[0]["candidate_summary_file"], "/tmp/old_candidates.json")
            self.assertEqual(rows[0]["paper_summary_file"], "/tmp/old_paper.json")
            self.assertEqual(rows[0]["top_candidate_edge_rank_score"], "")
            self.assertEqual(rows[1]["live_paper_summary_file"], str(base / "live_paper_ready.json"))
            self.assertEqual(rows[1]["top_candidate_edge_rank_score"], "0.029")
            self.assertEqual(rows[1]["top_candidate_consensus_book_count"], "3")
            self.assertEqual(rows[1]["top_candidate_consensus_stability"], "0.972")
            self.assertEqual(rows[1]["top_candidate_consensus_prob_range"], "0.008")

    def test_run_sports_archive_appends_ready_and_empty_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            archive_csv = base / "archive.csv"

            def fake_live_paper_runner(**kwargs: object) -> dict[str, object]:
                event_date = str(kwargs["event_date"])
                if event_date == "2026-03-27":
                    return {
                        "status": "ready",
                        "candidate_pull": {
                            "status": "ready",
                            "events_fetched": 5,
                            "candidates_written": 8,
                            "positive_ev_candidates": 2,
                            "top_candidates": [
                                {
                                    "selection": "Boston Celtics ML",
                                    "market": "moneyline",
                                    "book": "DraftKings",
                                    "estimated_ev": 0.031,
                                    "edge_rank_score": 0.029,
                                    "consensus_book_count": 3,
                                    "consensus_stability": 0.972,
                                    "consensus_prob_range": 0.008,
                                }
                            ],
                            "output_file": str(base / "candidates_ready.json"),
                        },
                        "paper_run": {
                            "accepted": 1,
                            "rejected": 7,
                            "avg_ev_accepted": 0.031,
                            "output_file": str(base / "paper_ready.json"),
                        },
                        "output_file": str(base / "live_paper_ready.json"),
                    }
                return {
                    "status": "empty",
                    "candidate_pull": {
                        "status": "empty",
                        "events_fetched": 0,
                        "candidates_written": 0,
                        "positive_ev_candidates": 0,
                        "top_candidates": [],
                        "output_file": str(base / "candidates_empty.json"),
                    },
                    "paper_run": None,
                    "output_file": str(base / "live_paper_empty.json"),
                }

            summary = run_sports_archive(
                env_file="data/research/account_onboarding.local.env",
                sport_id=4,
                event_dates=("2026-03-27", "2026-03-28"),
                starting_bankroll=1000.0,
                output_dir=str(base),
                archive_csv=str(archive_csv),
                live_paper_runner=fake_live_paper_runner,  # type: ignore[arg-type]
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["rows_appended"], 2)
            self.assertEqual(summary["archive_rows_total"], 2)
            self.assertEqual(summary["dates_ready"], 1)
            self.assertEqual(summary["dates_empty"], 1)
            self.assertEqual(summary["total_positive_ev_candidates"], 2)
            self.assertEqual(summary["total_paper_accepts"], 1)
            self.assertEqual(summary["top_ready_event_date"], "2026-03-27")
            self.assertEqual(summary["top_ready_candidate_edge_rank_score"], 0.029)
            self.assertEqual(summary["top_ready_candidate_consensus_book_count"], 3)
            self.assertAlmostEqual(summary["top_ready_candidate_consensus_stability"], 0.972)
            self.assertAlmostEqual(summary["top_ready_candidate_consensus_prob_range"], 0.008)
            self.assertEqual(summary["recent_history"]["lookback_rows"], 0)
            self.assertEqual(summary["recent_history"]["ready_rows"], 0)
            self.assertIsNone(summary["runs"][0]["previous_status"])
            self.assertFalse(summary["runs"][0]["status_changed"])
            self.assertIsNone(summary["runs"][0]["accepted_delta"])
            self.assertAlmostEqual(summary["runs"][0]["top_candidate_consensus_prob_range"], 0.008)
            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertTrue(archive_csv.exists())

    def test_run_sports_archive_reports_deltas_against_prior_same_date_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            archive_csv = base / "archive.csv"
            archive_csv.write_text(
                (
                    "recorded_at,sport_id,event_date,status,error,candidate_status,events_fetched,"
                    "candidates_written,positive_ev_candidates,accepted,rejected,avg_ev_accepted,"
                    "top_candidate_selection,top_candidate_market,top_candidate_book,"
                    "top_candidate_estimated_ev,top_candidate_edge_rank_score,"
                    "top_candidate_consensus_book_count,top_candidate_consensus_stability,"
                    "top_candidate_consensus_prob_range,live_paper_summary_file,"
                    "candidate_summary_file,paper_summary_file\n"
                    "2026-03-28T07:25:43.931734+00:00,4,2026-03-28,ready,,ready,6,22,1,1,21,0.02,"
                    "Sacramento Kings ML,moneyline,BetMGM,0.015,0.014,2,0.95,0.01,/tmp/old_live_paper.json,"
                    "/tmp/old_candidates.json,/tmp/old_paper.json\n"
                ),
                encoding="utf-8",
            )

            def fake_live_paper_runner(**_: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "candidate_pull": {
                        "status": "ready",
                        "events_fetched": 5,
                        "candidates_written": 24,
                        "positive_ev_candidates": 3,
                        "top_candidates": [
                            {
                                "selection": "Boston Celtics ML",
                                "market": "moneyline",
                                "book": "DraftKings",
                                "estimated_ev": 0.031,
                                "edge_rank_score": 0.029,
                                "consensus_book_count": 3,
                                "consensus_stability": 0.972,
                                "consensus_prob_range": 0.008,
                            }
                        ],
                        "output_file": str(base / "candidates_ready.json"),
                    },
                    "paper_run": {
                        "accepted": 2,
                        "rejected": 22,
                        "avg_ev_accepted": 0.028,
                        "output_file": str(base / "paper_ready.json"),
                    },
                    "output_file": str(base / "live_paper_ready.json"),
                }

            summary = run_sports_archive(
                env_file="data/research/account_onboarding.local.env",
                sport_id=4,
                event_dates=("2026-03-28",),
                starting_bankroll=1000.0,
                output_dir=str(base),
                archive_csv=str(archive_csv),
                live_paper_runner=fake_live_paper_runner,  # type: ignore[arg-type]
                now=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["recent_history"]["lookback_rows"], 1)
            self.assertEqual(summary["recent_history"]["ready_rows"], 1)
            self.assertEqual(summary["recent_history"]["latest_ready_event_date"], "2026-03-28")
            self.assertEqual(summary["runs"][0]["previous_status"], "ready")
            self.assertFalse(summary["runs"][0]["status_changed"])
            self.assertEqual(summary["runs"][0]["candidates_written_delta"], 2)
            self.assertEqual(summary["runs"][0]["positive_ev_candidates_delta"], 2)
            self.assertEqual(summary["runs"][0]["accepted_delta"], 1)
            self.assertAlmostEqual(summary["runs"][0]["top_candidate_estimated_ev_delta"], 0.016)
            self.assertEqual(summary["runs"][0]["top_candidate_consensus_book_count_delta"], 1)
            self.assertAlmostEqual(summary["runs"][0]["top_candidate_consensus_stability_delta"], 0.022)
            self.assertAlmostEqual(summary["runs"][0]["top_candidate_consensus_prob_range_delta"], -0.002)
            self.assertTrue(summary["runs"][0]["top_candidate_book_changed"])

    def test_run_sports_archive_records_errors_without_stopping(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            def fake_live_paper_runner(**kwargs: object) -> dict[str, object]:
                raise RuntimeError("temporary sports upstream failure")

            summary = run_sports_archive(
                env_file="data/research/account_onboarding.local.env",
                sport_id=4,
                event_dates=("2026-03-27",),
                starting_bankroll=1000.0,
                output_dir=str(base),
                live_paper_runner=fake_live_paper_runner,  # type: ignore[arg-type]
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "error")
            self.assertEqual(summary["dates_error"], 1)
            self.assertEqual(summary["rows_appended"], 1)
            self.assertEqual(summary["runs"][0]["status"], "error")

    def test_run_sports_archive_prefers_positive_ev_candidate_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            def fake_live_paper_runner(**_: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "candidate_pull": {
                        "status": "ready",
                        "events_fetched": 5,
                        "candidates_written": 8,
                        "positive_ev_candidates": 1,
                        "top_candidates": [
                            {
                                "selection": "Negative EV leader",
                                "market": "moneyline",
                                "book": "DraftKings",
                                "estimated_ev": -0.015,
                                "edge_rank_score": 0.02,
                                "consensus_book_count": 2,
                                "consensus_stability": 0.91,
                                "consensus_prob_range": 0.017,
                            }
                        ],
                        "top_positive_ev_candidate": {
                            "selection": "Positive EV fallback",
                            "market": "handicap",
                            "book": "FanDuel",
                            "estimated_ev": 0.014,
                            "edge_rank_score": 0.01,
                            "consensus_book_count": 2,
                            "consensus_stability": 0.85,
                            "consensus_prob_range": 0.03,
                        },
                        "output_file": str(base / "candidates_ready.json"),
                    },
                    "paper_run": {
                        "accepted": 0,
                        "rejected": 8,
                        "avg_ev_accepted": 0.0,
                        "output_file": str(base / "paper_ready.json"),
                    },
                    "output_file": str(base / "live_paper_ready.json"),
                }

            summary = run_sports_archive(
                env_file="data/research/account_onboarding.local.env",
                sport_id=4,
                event_dates=("2026-03-29",),
                starting_bankroll=1000.0,
                output_dir=str(base),
                live_paper_runner=fake_live_paper_runner,  # type: ignore[arg-type]
                now=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["top_ready_candidate_selection"], "Positive EV fallback")
            self.assertAlmostEqual(summary["top_ready_candidate_estimated_ev"], 0.014)
            self.assertEqual(summary["runs"][0]["top_candidate_selection"], "Positive EV fallback")
            self.assertAlmostEqual(summary["runs"][0]["top_candidate_estimated_ev"], 0.014)

    def test_run_sports_archive_preserves_structured_live_paper_errors(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)

            def fake_live_paper_runner(**kwargs: object) -> dict[str, object]:
                return {
                    "status": "error",
                    "error": "TheRundown request failed: upstream unavailable",
                    "candidate_pull": None,
                    "paper_run": None,
                    "output_file": str(base / "live_paper_error.json"),
                }

            summary = run_sports_archive(
                env_file="data/research/account_onboarding.local.env",
                sport_id=4,
                event_dates=("2026-03-28",),
                starting_bankroll=1000.0,
                output_dir=str(base),
                live_paper_runner=fake_live_paper_runner,  # type: ignore[arg-type]
                now=datetime(2026, 3, 28, 12, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "error")
            self.assertEqual(summary["runs"][0]["status"], "error")
            self.assertEqual(summary["runs"][0]["error"], "TheRundown request failed: upstream unavailable")

            with Path(summary["archive_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["status"], "error")
            self.assertEqual(rows[0]["error"], "TheRundown request failed: upstream unavailable")
            self.assertEqual(rows[0]["live_paper_summary_file"], str(base / "live_paper_error.json"))


if __name__ == "__main__":
    unittest.main()
