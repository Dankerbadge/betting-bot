import csv
import json
import tempfile
from pathlib import Path
import unittest
from urllib.error import URLError
from unittest.mock import patch

from betbot.live_paper import run_live_paper


class LivePaperTests(unittest.TestCase):
    def test_run_live_paper_combines_candidate_pull_and_paper(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                ),
                encoding="utf-8",
            )
            config_path = base / "cfg.json"
            config_path.write_text('{"min_stake": 0.1, "min_ev": 0.001}', encoding="utf-8")

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if "/affiliates?" in url:
                    return 200, {
                        "affiliates": [
                            {"affiliate_id": 19, "affiliate_name": "DraftKings"},
                            {"affiliate_id": 23, "affiliate_name": "FanDuel"},
                        ]
                    }
                return 200, {
                    "events": [
                        {
                            "event_id": "evt1",
                            "sport_id": 4,
                            "event_date": "2026-03-28T00:00:00Z",
                            "score": {"event_status": "STATUS_SCHEDULED"},
                            "teams": [
                                {
                                    "name": "Boston",
                                    "mascot": "Celtics",
                                    "is_away": True,
                                    "is_home": False,
                                },
                                {
                                    "name": "New York",
                                    "mascot": "Knicks",
                                    "is_away": False,
                                    "is_home": True,
                                },
                            ],
                            "markets": [
                                {
                                    "market_id": 1,
                                    "name": "moneyline",
                                    "participants": [
                                        {
                                            "name": "Boston Celtics",
                                            "lines": [
                                                {
                                                    "prices": {
                                                        "19": {"price": -110},
                                                        "23": {"price": 110},
                                                    }
                                                }
                                            ],
                                        },
                                        {
                                            "name": "New York Knicks",
                                            "lines": [
                                                {
                                                    "prices": {
                                                        "19": {"price": -110},
                                                        "23": {"price": -130},
                                                    }
                                                }
                                            ],
                                        },
                                    ],
                                }
                            ],
                        }
                    ]
                }

            summary = run_live_paper(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-27",
                starting_bankroll=1000,
                config_path=str(config_path),
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                http_get_json=fake_http_get_json,  # type: ignore[arg-type]
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["sport_id"], 4)
            self.assertEqual(summary["event_date"], "2026-03-27")
            self.assertEqual(summary["candidate_pull"]["candidates_written"], 2)
            self.assertEqual(summary["paper_run"]["accepted"], 1)
            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertTrue(Path(summary["paper_run"]["output_file"]).exists())
            self.assertTrue(Path(summary["paper_run"]["output_decisions_csv"]).exists())

    def test_run_live_paper_returns_empty_when_no_candidates_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if "/affiliates?" in url:
                    return 200, {
                        "affiliates": [
                            {"affiliate_id": 19, "affiliate_name": "DraftKings"},
                            {"affiliate_id": 23, "affiliate_name": "FanDuel"},
                        ]
                    }
                return 200, {"events": []}

            summary = run_live_paper(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-27",
                starting_bankroll=1000,
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                http_get_json=fake_http_get_json,  # type: ignore[arg-type]
            )

            self.assertEqual(summary["status"], "empty")
            self.assertEqual(summary["sport_id"], 4)
            self.assertEqual(summary["event_date"], "2026-03-27")
            self.assertIsNone(summary["paper_run"])
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_live_paper_writes_error_summary_when_candidate_pull_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            summary = run_live_paper(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-27",
                starting_bankroll=1000,
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                http_get_json=fake_http_get_json,  # type: ignore[arg-type]
            )

            self.assertEqual(summary["status"], "error")
            self.assertIsNone(summary["candidate_pull"])
            self.assertIsNone(summary["paper_run"])
            self.assertEqual(summary["error"], "TheRundown request failed: dns failed")
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_live_paper_falls_back_to_cached_candidates_when_live_pull_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                ),
                encoding="utf-8",
            )

            cached_csv = base / "live_candidates_4_2026-03-27_20260329_070000.csv"
            with cached_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "timestamp",
                        "event_id",
                        "selection",
                        "odds",
                        "closing_odds",
                        "model_prob",
                        "decision_prob",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "timestamp": "2026-03-27T12:00:00-04:00",
                        "event_id": "evt-cached-1",
                        "selection": "Cached Team ML",
                        "odds": "2.2",
                        "closing_odds": "",
                        "model_prob": "0.6",
                        "decision_prob": "0.6",
                    }
                )

            cached_summary = base / "live_candidates_summary_4_2026-03-27_20260329_070000.json"
            cached_summary.write_text(
                json.dumps(
                    {
                        "captured_at": "2026-03-29T07:00:00+00:00",
                        "status": "ready",
                        "sport_id": 4,
                        "event_date": "2026-03-27",
                        "output_csv": str(cached_csv),
                        "candidates_written": 1,
                    }
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            summary = run_live_paper(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-27",
                starting_bankroll=1000,
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                http_get_json=fake_http_get_json,  # type: ignore[arg-type]
            )

            self.assertEqual(summary["status"], "stale_ready")
            self.assertTrue(summary["used_cached_candidates"])
            self.assertEqual(summary["candidate_csv_used"], str(cached_csv))
            self.assertIsNotNone(summary["paper_run"])
            self.assertEqual(summary["error"], "TheRundown request failed: dns failed")
            self.assertEqual(summary["candidate_pull"]["status"], "cached_fallback")
            self.assertEqual(summary["candidate_pull"]["source_status"], "ready")
            self.assertEqual(summary["candidate_pull"]["fallback_error"], "TheRundown request failed: dns failed")
            self.assertEqual(summary["candidate_pull"]["output_file"], str(cached_summary))
            self.assertTrue(Path(summary["paper_run"]["output_file"]).exists())

    def test_run_live_paper_returns_stale_empty_when_only_cached_empty_exists(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                ),
                encoding="utf-8",
            )

            cached_csv = base / "live_candidates_4_2026-03-27_20260329_070000.csv"
            with cached_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "timestamp",
                        "event_id",
                        "selection",
                        "odds",
                        "closing_odds",
                        "model_prob",
                    ],
                )
                writer.writeheader()

            cached_summary = base / "live_candidates_summary_4_2026-03-27_20260329_070000.json"
            cached_summary.write_text(
                json.dumps(
                    {
                        "captured_at": "2026-03-29T07:00:00+00:00",
                        "status": "empty",
                        "sport_id": 4,
                        "event_date": "2026-03-27",
                        "output_csv": str(cached_csv),
                        "candidates_written": 0,
                    }
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            summary = run_live_paper(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-27",
                starting_bankroll=1000,
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                http_get_json=fake_http_get_json,  # type: ignore[arg-type]
            )

            self.assertEqual(summary["status"], "stale_empty")
            self.assertTrue(summary["used_cached_candidates"])
            self.assertEqual(summary["candidate_csv_used"], str(cached_csv))
            self.assertEqual(summary["error"], "TheRundown request failed: dns failed")
            self.assertEqual(summary["candidate_pull"]["source_status"], "empty")
            self.assertIsNone(summary["paper_run"])
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_live_paper_summary_filenames_do_not_collide_across_event_dates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            frozen_now = __import__("datetime").datetime(2026, 3, 28, 11, 5, 0)
            with patch("betbot.live_paper.datetime") as mock_datetime:
                mock_datetime.now.return_value = frozen_now
                mock_datetime.strftime = __import__("datetime").datetime.strftime

                first = run_live_paper(
                    env_file=str(env_file),
                    sport_id=4,
                    event_date="2026-03-28",
                    starting_bankroll=1000,
                    output_dir=str(base),
                    affiliate_ids=("19", "23"),
                    http_get_json=fake_http_get_json,  # type: ignore[arg-type]
                )
                second = run_live_paper(
                    env_file=str(env_file),
                    sport_id=4,
                    event_date="2026-03-29",
                    starting_bankroll=1000,
                    output_dir=str(base),
                    affiliate_ids=("19", "23"),
                    http_get_json=fake_http_get_json,  # type: ignore[arg-type]
                )

            self.assertNotEqual(first["output_file"], second["output_file"])
            self.assertIn("2026-03-28", Path(first["output_file"]).name)
            self.assertIn("2026-03-29", Path(second["output_file"]).name)
            self.assertTrue(Path(first["output_file"]).exists())
            self.assertTrue(Path(second["output_file"]).exists())

    def test_run_live_paper_summary_filenames_do_not_collide_across_sports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            frozen_now = __import__("datetime").datetime(2026, 3, 28, 11, 6, 27)
            with patch("betbot.live_paper.datetime") as mock_datetime:
                mock_datetime.now.return_value = frozen_now
                mock_datetime.strftime = __import__("datetime").datetime.strftime

                first = run_live_paper(
                    env_file=str(env_file),
                    sport_id=3,
                    event_date="2026-03-28",
                    starting_bankroll=1000,
                    output_dir=str(base),
                    affiliate_ids=("19", "23"),
                    http_get_json=fake_http_get_json,  # type: ignore[arg-type]
                )
                second = run_live_paper(
                    env_file=str(env_file),
                    sport_id=4,
                    event_date="2026-03-28",
                    starting_bankroll=1000,
                    output_dir=str(base),
                    affiliate_ids=("19", "23"),
                    http_get_json=fake_http_get_json,  # type: ignore[arg-type]
                )

            self.assertNotEqual(first["output_file"], second["output_file"])
            self.assertIn("live_paper_summary_3_", Path(first["output_file"]).name)
            self.assertIn("live_paper_summary_4_", Path(second["output_file"]).name)
            self.assertTrue(Path(first["output_file"]).exists())
            self.assertTrue(Path(second["output_file"]).exists())

    def test_run_live_paper_applies_candidate_enrichment_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "BETBOT_TIMEZONE=America/New_York\n"
                ),
                encoding="utf-8",
            )
            config_path = base / "cfg.json"
            config_path.write_text('{"min_stake": 0.1, "min_ev": 0.001}', encoding="utf-8")
            evidence_csv = base / "evidence.csv"
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
                        "observed_at": "2026-03-28T20:00:00+00:00",
                        "availability_signal": "0.2",
                        "lineup_signal": "0.05",
                        "news_signal": "0.0",
                        "source_confidence": "1.0",
                        "source_count": "2",
                        "conflict_flag": "false",
                    }
                )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if "/affiliates?" in url:
                    return 200, {
                        "affiliates": [
                            {"affiliate_id": 19, "affiliate_name": "DraftKings"},
                            {"affiliate_id": 23, "affiliate_name": "FanDuel"},
                        ]
                    }
                return 200, {
                    "events": [
                        {
                            "event_id": "evt1",
                            "sport_id": 4,
                            "event_date": "2026-03-28T00:00:00Z",
                            "score": {"event_status": "STATUS_SCHEDULED"},
                            "teams": [
                                {
                                    "name": "Boston",
                                    "mascot": "Celtics",
                                    "is_away": True,
                                    "is_home": False,
                                },
                                {
                                    "name": "New York",
                                    "mascot": "Knicks",
                                    "is_away": False,
                                    "is_home": True,
                                },
                            ],
                            "markets": [
                                {
                                    "market_id": 1,
                                    "name": "moneyline",
                                    "participants": [
                                        {
                                            "name": "Boston Celtics",
                                            "lines": [
                                                {
                                                    "prices": {
                                                        "19": {"price": -110},
                                                        "23": {"price": 110},
                                                    }
                                                }
                                            ],
                                        },
                                        {
                                            "name": "New York Knicks",
                                            "lines": [
                                                {
                                                    "prices": {
                                                        "19": {"price": -110},
                                                        "23": {"price": -130},
                                                    }
                                                }
                                            ],
                                        },
                                    ],
                                }
                            ],
                        }
                    ]
                }

            summary = run_live_paper(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-27",
                starting_bankroll=1000,
                config_path=str(config_path),
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                enrich_candidates=True,
                enrichment_csv=str(evidence_csv),
                enrichment_freshness_hours=2400.0,
                http_get_json=fake_http_get_json,  # type: ignore[arg-type]
            )

            self.assertEqual(summary["status"], "ready")
            enrichment = summary["candidate_enrichment"]
            self.assertIsInstance(enrichment, dict)
            assert isinstance(enrichment, dict)
            self.assertEqual(enrichment["status"], "ready")
            self.assertGreaterEqual(enrichment["rows_adjusted"], 1)
            self.assertEqual(summary["candidate_csv_used"], enrichment["output_csv"])
            self.assertTrue(Path(summary["candidate_csv_used"]).exists())


if __name__ == "__main__":
    unittest.main()
