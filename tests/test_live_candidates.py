import csv
from datetime import datetime
import json
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import URLError

from betbot.edge import expected_value_decimal
from betbot.live_candidates import (
    _therundown_json_get,
    american_to_decimal,
    extract_candidate_rows,
    run_live_candidates,
)
from betbot.io import load_candidates
from betbot.kalshi_mlb_map import run_kalshi_mlb_map


class LiveCandidatesTests(unittest.TestCase):
    def test_american_to_decimal(self) -> None:
        self.assertAlmostEqual(american_to_decimal(-110), 1.909091, places=6)
        self.assertAlmostEqual(american_to_decimal(150), 2.5, places=6)

    def test_extract_candidate_rows_uses_consensus_and_best_book(self) -> None:
        events = [
            {
                "event_id": "evt1",
                "sport_id": 4,
                "event_date": "2026-03-28T00:00:00Z",
                "score": {"event_status": "STATUS_SCHEDULED"},
                "teams": [
                    {"name": "Boston", "mascot": "Celtics", "is_away": True, "is_home": False},
                    {"name": "New York", "mascot": "Knicks", "is_away": False, "is_home": True},
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
                                            "19": {"price": -110, "updated_at": "2026-03-27T18:00:00Z"},
                                            "23": {"price": -105, "updated_at": "2026-03-27T18:01:00Z"},
                                        }
                                    }
                                ],
                            },
                            {
                                "name": "New York Knicks",
                                "lines": [
                                    {
                                        "prices": {
                                            "19": {"price": -110, "updated_at": "2026-03-27T18:00:00Z"},
                                            "23": {"price": -115, "updated_at": "2026-03-27T18:01:00Z"},
                                        }
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ]
        affiliate_names = {"19": "DraftKings", "23": "FanDuel"}

        rows, counters = extract_candidate_rows(
            events=events,
            affiliate_names=affiliate_names,
            min_books=2,
            timezone_name="America/New_York",
            include_in_play=False,
        )

        self.assertEqual(counters["events_total"], 1)
        self.assertEqual(counters["market_pairs_with_consensus"], 1)
        self.assertEqual(len(rows), 2)
        first, second = rows
        self.assertEqual(first["selection"], "Boston Celtics ML")
        self.assertEqual(first["book"], "FanDuel")
        self.assertAlmostEqual(first["odds"], 1.952381, places=6)
        self.assertEqual(first["timestamp"], "2026-03-27T20:00:00-04:00")
        self.assertAlmostEqual(first["model_prob"], 0.494534, places=6)
        self.assertAlmostEqual(first["consensus_fair_prob"], 0.494198, places=6)
        self.assertAlmostEqual(first["consensus_prob_range"], 0.011604, places=6)
        self.assertAlmostEqual(first["consensus_stability"], 0.941978, places=6)
        self.assertAlmostEqual(first["consensus_confidence"], 0.627985, places=6)
        self.assertAlmostEqual(first["confidence_adjusted_ev"], -0.021654, places=6)
        self.assertAlmostEqual(first["decision_ev"], first["confidence_adjusted_ev"], places=6)
        self.assertAlmostEqual(first["decision_prob"], 0.501104, places=6)
        self.assertAlmostEqual(
            expected_value_decimal(first["decision_prob"], first["odds"]),
            first["decision_ev"],
            places=6,
        )
        self.assertAlmostEqual(first["edge_rank_score"], -0.022814, places=6)
        self.assertEqual(second["selection"], "New York Knicks ML")

    def test_extract_candidate_rows_shrinks_disputed_consensus_and_penalizes_rank(self) -> None:
        events = [
            {
                "event_id": "evt1",
                "sport_id": 4,
                "event_date": "2026-03-28T00:00:00Z",
                "score": {"event_status": "STATUS_SCHEDULED"},
                "teams": [
                    {"name": "Boston", "mascot": "Celtics", "is_away": True, "is_home": False},
                    {"name": "New York", "mascot": "Knicks", "is_away": False, "is_home": True},
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
                                            "22": {"price": 130},
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
                                            "22": {"price": -150},
                                        }
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ]
        affiliate_names = {"19": "DraftKings", "22": "BetMGM"}

        rows, _ = extract_candidate_rows(
            events=events,
            affiliate_names=affiliate_names,
            min_books=2,
            timezone_name="America/New_York",
            include_in_play=False,
        )

        boston = rows[0]
        self.assertGreater(boston["model_prob"], boston["consensus_fair_prob"])
        self.assertLess(boston["model_prob"], 0.5)
        self.assertGreater(boston["consensus_prob_range"], 0.05)
        self.assertLess(boston["consensus_stability"], 0.75)
        self.assertLess(boston["edge_rank_score"], boston["estimated_ev"])

    def test_extract_candidate_rows_sorts_same_timestamp_by_edge_rank_score(self) -> None:
        events = [
            {
                "event_id": "evt1",
                "sport_id": 4,
                "event_date": "2026-03-28T00:00:00Z",
                "score": {"event_status": "STATUS_SCHEDULED"},
                "teams": [
                    {"name": "Boston", "mascot": "Celtics", "is_away": True, "is_home": False},
                    {"name": "New York", "mascot": "Knicks", "is_away": False, "is_home": True},
                ],
                "markets": [
                    {
                        "market_id": 1,
                        "name": "moneyline",
                        "participants": [
                            {
                                "name": "Boston Celtics",
                                "lines": [{"prices": {"19": {"price": -110}, "23": {"price": 110}}}],
                            },
                            {
                                "name": "New York Knicks",
                                "lines": [{"prices": {"19": {"price": -110}, "23": {"price": -130}}}],
                            },
                        ],
                    }
                ],
            }
        ]
        rows, _ = extract_candidate_rows(
            events=events,
            affiliate_names={"19": "DraftKings", "23": "FanDuel"},
            min_books=2,
            timezone_name="America/New_York",
            include_in_play=False,
        )

        self.assertGreater(rows[0]["edge_rank_score"], rows[1]["edge_rank_score"])
        self.assertEqual(rows[0]["selection"], "Boston Celtics ML")

    def test_extract_candidate_rows_rewards_deeper_stable_consensus(self) -> None:
        events = [
            {
                "event_id": "evt1",
                "sport_id": 4,
                "event_date": "2026-03-28T00:00:00Z",
                "score": {"event_status": "STATUS_SCHEDULED"},
                "teams": [
                    {"name": "Boston", "mascot": "Celtics", "is_away": True, "is_home": False},
                    {"name": "New York", "mascot": "Knicks", "is_away": False, "is_home": True},
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
                                            "19": {"price": 110},
                                            "22": {"price": 110},
                                            "23": {"price": 112},
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
                                            "22": {"price": -110},
                                            "23": {"price": -112},
                                        }
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ]

        rows, _ = extract_candidate_rows(
            events=events,
            affiliate_names={"19": "DraftKings", "22": "BetMGM", "23": "FanDuel"},
            min_books=2,
            timezone_name="America/New_York",
            include_in_play=False,
        )

        boston = rows[0]
        self.assertEqual(boston["consensus_book_count"], 3)
        self.assertGreater(boston["consensus_stability"], 0.95)
        self.assertGreater(boston["edge_rank_score"], boston["estimated_ev"])

    def test_extract_candidate_rows_penalizes_stale_best_quote_in_rank_only(self) -> None:
        events = [
            {
                "event_id": "evt1",
                "sport_id": 4,
                "event_date": "2026-03-28T00:00:00Z",
                "score": {"event_status": "STATUS_SCHEDULED"},
                "teams": [
                    {"name": "Boston", "mascot": "Celtics", "is_away": True, "is_home": False},
                    {"name": "New York", "mascot": "Knicks", "is_away": False, "is_home": True},
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
                                            "19": {"price": 130, "updated_at": "2026-03-27T18:00:00Z"},
                                            "23": {"price": 120, "updated_at": "2026-03-27T19:00:00Z"},
                                        }
                                    }
                                ],
                            },
                            {
                                "name": "New York Knicks",
                                "lines": [
                                    {
                                        "prices": {
                                            "19": {"price": -140, "updated_at": "2026-03-27T18:00:00Z"},
                                            "23": {"price": -130, "updated_at": "2026-03-27T19:00:00Z"},
                                        }
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ]

        rows, _ = extract_candidate_rows(
            events=events,
            affiliate_names={"19": "DraftKings", "23": "FanDuel"},
            min_books=2,
            timezone_name="America/New_York",
            include_in_play=False,
        )

        boston = next(row for row in rows if row["selection"] == "Boston Celtics ML")
        self.assertEqual(boston["book"], "DraftKings")
        self.assertEqual(boston["best_quote_updated_at"], "2026-03-27T18:00:00Z")
        self.assertEqual(boston["best_quote_age_seconds"], 3600.0)
        self.assertAlmostEqual(boston["stale_quote_penalty"], 0.02, places=6)
        self.assertLess(boston["edge_rank_score"], boston["estimated_ev"])
        self.assertLess(boston["confidence_adjusted_ev"], boston["estimated_ev"])

    def test_extract_candidate_rows_uses_robust_consensus_against_three_book_outlier(self) -> None:
        events = [
            {
                "event_id": "evt1",
                "sport_id": 4,
                "event_date": "2026-03-28T00:00:00Z",
                "score": {"event_status": "STATUS_SCHEDULED"},
                "teams": [
                    {"name": "Boston", "mascot": "Celtics", "is_away": True, "is_home": False},
                    {"name": "New York", "mascot": "Knicks", "is_away": False, "is_home": True},
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
                                            "19": {"price": 110},
                                            "22": {"price": 112},
                                            "23": {"price": 180},
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
                                            "22": {"price": -112},
                                            "23": {"price": -220},
                                        }
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ]

        rows, _ = extract_candidate_rows(
            events=events,
            affiliate_names={"19": "DraftKings", "22": "BetMGM", "23": "FanDuel"},
            min_books=2,
            timezone_name="America/New_York",
            include_in_play=False,
        )

        boston = rows[0]
        self.assertAlmostEqual(boston["consensus_fair_prob"], 0.426346, places=6)
        self.assertAlmostEqual(boston["consensus_robust_prob"], 0.471698, places=6)
        self.assertGreater(boston["model_prob"], boston["consensus_fair_prob"])

    def test_load_candidates_keeps_decision_prob_edge_rank_score_and_ordering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "candidates.csv"
            csv_path.write_text(
                (
                    "timestamp,event_id,selection,odds,model_prob,decision_prob,edge_rank_score\n"
                    "2026-03-28T19:30:00,evt_b,Lower edge,2.0,0.55,0.52,0.010000\n"
                    "2026-03-28T19:30:00,evt_a,Higher edge,2.0,0.55,0.53,0.020000\n"
                ),
                encoding="utf-8",
            )

            candidates = load_candidates(str(csv_path))

            self.assertEqual([c.selection for c in candidates], ["Higher edge", "Lower edge"])
            self.assertEqual(candidates[0].decision_prob, 0.53)
            self.assertEqual(candidates[0].edge_rank_score, 0.02)

    def test_run_live_candidates_writes_csv(self) -> None:
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
                                    "market_id": 2,
                                    "name": "handicap",
                                    "participants": [
                                        {
                                            "name": "Boston Celtics",
                                            "lines": [
                                                {
                                                    "value": "-3.5",
                                                    "prices": {
                                                        "19": {"price": -110},
                                                        "23": {"price": -105},
                                                    },
                                                }
                                            ],
                                        },
                                        {
                                            "name": "New York Knicks",
                                            "lines": [
                                                {
                                                    "value": "+3.5",
                                                    "prices": {
                                                        "19": {"price": -110},
                                                        "23": {"price": -115},
                                                    },
                                                }
                                            ],
                                        },
                                    ],
                                }
                            ],
                        }
                    ]
                }

            summary = run_live_candidates(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-27",
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                market_ids=(1, 2, 3),
                http_get_json=fake_http_get_json,
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["candidates_written"], 2)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

            with Path(summary["output_csv"]).open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["market"], "handicap")
            self.assertEqual(rows[0]["book"], "FanDuel")
            self.assertIn("edge_rank_score", rows[0])
            self.assertIn("consensus_confidence", rows[0])
            self.assertIn("confidence_adjusted_ev", rows[0])
            self.assertIn("decision_prob", rows[0])
            self.assertIn("decision_ev", rows[0])
            self.assertIn("best_quote_updated_at", rows[0])
            self.assertIn("best_quote_age_seconds", rows[0])
            self.assertIn("stale_quote_penalty", rows[0])
            self.assertGreater(float(rows[0]["edge_rank_score"]), float(rows[1]["edge_rank_score"]))
            self.assertIn("consensus_book_count", summary["top_candidates"][0])
            self.assertIn("consensus_confidence", summary["top_candidates"][0])
            self.assertIn("confidence_adjusted_ev", summary["top_candidates"][0])
            self.assertIn("decision_prob", summary["top_candidates"][0])
            self.assertIn("decision_ev", summary["top_candidates"][0])
            self.assertIn("consensus_prob_range", summary["top_candidates"][0])
            self.assertIn("best_quote_age_seconds", summary["top_candidates"][0])
            self.assertIn("positive_decision_ev_candidates", summary)
            self.assertNotIn("top_positive_ev_candidate", summary)
            self.assertNotIn("top_positive_decision_ev_candidate", summary)

    def test_run_live_candidates_respects_market_ids_filter(self) -> None:
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
                return 200, {
                    "events": [
                        {
                            "event_id": "evt1",
                            "sport_id": 4,
                            "event_date": "2026-03-28T00:00:00Z",
                            "score": {"event_status": "STATUS_SCHEDULED"},
                            "markets": [
                                {
                                    "market_id": 1,
                                    "name": "moneyline",
                                    "participants": [
                                        {
                                            "name": "Boston Celtics",
                                            "lines": [{"prices": {"19": {"price": -110}, "23": {"price": -105}}}],
                                        },
                                        {
                                            "name": "New York Knicks",
                                            "lines": [{"prices": {"19": {"price": -110}, "23": {"price": -115}}}],
                                        },
                                    ],
                                },
                                {
                                    "market_id": 2,
                                    "name": "handicap",
                                    "participants": [
                                        {
                                            "name": "Boston Celtics",
                                            "lines": [
                                                {
                                                    "value": "-3.5",
                                                    "prices": {"19": {"price": -110}, "23": {"price": -105}},
                                                }
                                            ],
                                        },
                                        {
                                            "name": "New York Knicks",
                                            "lines": [
                                                {
                                                    "value": "+3.5",
                                                    "prices": {"19": {"price": -110}, "23": {"price": -115}},
                                                }
                                            ],
                                        },
                                    ],
                                },
                            ],
                        }
                    ]
                }

            summary = run_live_candidates(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-28",
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                market_ids=(1,),
                http_get_json=fake_http_get_json,
            )

            self.assertEqual(summary["status"], "ready")
            with Path(summary["output_csv"]).open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertTrue(rows)
            self.assertTrue(all(row["market"] == "moneyline" for row in rows))

    def test_run_live_candidates_exposes_best_positive_ev_candidate_separately(self) -> None:
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

            with patch("betbot.live_candidates._load_affiliate_names", return_value={"19": "DraftKings"}):
                with patch(
                    "betbot.live_candidates._load_event_payload",
                    return_value={"events": []},
                ):
                    with patch(
                        "betbot.live_candidates.extract_candidate_rows",
                        return_value=(
                            [
                                {
                                    "selection": "High rank negative EV",
                                    "market": "moneyline",
                                    "book": "DraftKings",
                                    "odds": 2.59,
                                    "model_prob": 0.38,
                                    "decision_prob": 0.382,
                                    "estimated_ev": -0.015,
                                    "confidence_adjusted_ev": -0.009,
                                    "decision_ev": -0.009,
                                    "edge_rank_score": 0.02,
                                    "consensus_book_count": 2,
                                    "consensus_stability": 0.91,
                                    "consensus_confidence": 0.61,
                                    "consensus_prob_range": 0.017,
                                    "best_quote_updated_at": "2026-03-28T20:47:03Z",
                                    "best_quote_age_seconds": 0.0,
                                    "stale_quote_penalty": 0.0,
                                    "timestamp": "2026-03-29T13:40:00-04:00",
                                },
                                {
                                    "selection": "Lower rank positive EV",
                                    "market": "handicap",
                                    "book": "DraftKings",
                                    "odds": 2.46,
                                    "model_prob": 0.412,
                                    "decision_prob": 0.409,
                                    "estimated_ev": 0.014,
                                    "confidence_adjusted_ev": 0.008,
                                    "decision_ev": 0.008,
                                    "edge_rank_score": 0.01,
                                    "consensus_book_count": 2,
                                    "consensus_stability": 0.85,
                                    "consensus_confidence": 0.57,
                                    "consensus_prob_range": 0.03,
                                    "best_quote_updated_at": "2026-03-28T19:58:24Z",
                                    "best_quote_age_seconds": 10335.0,
                                    "stale_quote_penalty": 0.02,
                                    "timestamp": "2026-03-29T19:20:00-04:00",
                                },
                            ],
                            {
                                "events_total": 0,
                                "events_skipped_in_play": 0,
                                "market_pairs_seen": 0,
                                "market_pairs_with_consensus": 0,
                                "market_pairs_skipped_book_depth": 0,
                            },
                        ),
                    ):
                        summary = run_live_candidates(
                            env_file=str(env_file),
                            sport_id=4,
                            event_date="2026-03-27",
                            output_dir=str(base),
                            affiliate_ids=("19",),
                            market_ids=(1, 2, 3),
                        )

            self.assertEqual(summary["top_candidates"][0]["selection"], "High rank negative EV")
            self.assertEqual(summary["top_positive_ev_candidate"]["selection"], "Lower rank positive EV")
            self.assertEqual(summary["top_positive_decision_ev_candidate"]["selection"], "Lower rank positive EV")

    def test_run_live_candidates_surfaces_network_error_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            with self.assertRaisesRegex(ValueError, "TheRundown request failed: dns failed"):
                run_live_candidates(
                    env_file=str(env_file),
                    sport_id=4,
                    event_date="2026-03-28",
                    output_dir=str(base),
                    http_get_json=fake_http_get_json,
                )

    def test_run_live_candidates_non_therundown_uses_artifact_passthrough(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=opticodds\n"
                    "OPTICODDS_API_KEY=abc123\n"
                ),
                encoding="utf-8",
            )
            source_summary = base / "live_candidates_summary_4_2026-03-28_20260328_010101.json"
            source_summary.write_text(
                json.dumps(
                    {
                        "captured_at": datetime.now().isoformat(),
                        "status": "ready",
                        "market_ids": [1, 2, 3],
                        "affiliate_ids": ["19"],
                        "affiliate_names": ["DraftKings"],
                        "events_fetched": 3,
                        "market_pairs_seen": 6,
                        "market_pairs_with_consensus": 4,
                        "candidates_written": 5,
                        "positive_ev_candidates": 2,
                        "positive_decision_ev_candidates": 2,
                        "top_candidates": [{"selection": "Example candidate"}],
                        "output_csv": str(base / "live_candidates_4_2026-03-28_20260328_010101.csv"),
                    }
                ),
                encoding="utf-8",
            )

            summary = run_live_candidates(
                env_file=str(env_file),
                sport_id=4,
                event_date="2026-03-28",
                output_dir=str(base),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["provider"], "opticodds")
            self.assertEqual(summary["data_source"], "artifact_passthrough")
            self.assertEqual(summary["candidates_written"], 5)
            self.assertTrue(str(summary["source_summary_file"]).endswith(source_summary.name))
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_therundown_json_get_retries_rate_limited_calls(self) -> None:
        attempts = 0

        def fake_http_get_json(
            url: str,
            headers: dict[str, str],
            timeout_seconds: float,
        ) -> tuple[int, object]:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                return 429, {"error": "rate limited"}
            return 200, {"events": []}

        with patch("betbot.live_candidates.time.sleep") as mock_sleep:
            status_code, payload = _therundown_json_get(
                "https://therundown.example/events",
                15.0,
                fake_http_get_json,  # type: ignore[arg-type]
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload, {"events": []})
        self.assertEqual(attempts, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_therundown_json_get_retries_retryable_http_statuses(self) -> None:
        attempts = 0

        def fake_http_get_json(
            url: str,
            headers: dict[str, str],
            timeout_seconds: float,
        ) -> tuple[int, object]:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                return 401, {"error": "temporary auth failure"}
            return 200, {"events": []}

        with patch("betbot.live_candidates.time.sleep") as mock_sleep:
            status_code, payload = _therundown_json_get(
                "https://therundown.example/events",
                15.0,
                fake_http_get_json,  # type: ignore[arg-type]
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload, {"events": []})
        self.assertEqual(attempts, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_therundown_json_get_retries_transient_network_errors(self) -> None:
        attempts = 0

        def fake_http_get_json(
            url: str,
            headers: dict[str, str],
            timeout_seconds: float,
        ) -> tuple[int, object]:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise URLError("dns failed")
            return 200, {"events": []}

        with patch("betbot.live_candidates.time.sleep") as mock_sleep:
            status_code, payload = _therundown_json_get(
                "https://therundown.example/events",
                15.0,
                fake_http_get_json,  # type: ignore[arg-type]
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload, {"events": []})
        self.assertEqual(attempts, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_therundown_json_get_raises_after_network_retry_budget_exhausted(self) -> None:
        attempts = 0

        def fake_http_get_json(
            url: str,
            headers: dict[str, str],
            timeout_seconds: float,
        ) -> tuple[int, object]:
            nonlocal attempts
            attempts += 1
            raise URLError("dns failed")

        with patch("betbot.live_candidates.time.sleep") as mock_sleep:
            with self.assertRaisesRegex(ValueError, "TheRundown request failed: dns failed"):
                _therundown_json_get(
                    "https://therundown.example/events",
                    15.0,
                    fake_http_get_json,  # type: ignore[arg-type]
                )

        self.assertEqual(attempts, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_run_kalshi_mlb_map_writes_error_summary_when_network_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            key_file = base / "kalshi.pem"
            key_file.write_text("dummy", encoding="utf-8")
            env_file.write_text(
                (
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    f"KALSHI_PRIVATE_KEY_PATH={key_file}\n"
                    "KALSHI_ENV=prod\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            summary = run_kalshi_mlb_map(
                env_file=str(env_file),
                event_date="2026-03-28",
                output_dir=str(base),
                http_get_json=fake_http_get_json,
            )
            self.assertEqual(summary["status"], "error")
            self.assertEqual(summary["error"], "TheRundown request failed: dns failed")
            self.assertTrue(Path(summary["output_file"]).exists())
            written = json.loads(Path(summary["output_file"]).read_text(encoding="utf-8"))
            self.assertEqual(written["status"], "error")
            self.assertEqual(written["event_date"], "2026-03-28")

    def test_cli_surfaces_valueerror_without_traceback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "ODDS_PROVIDER=therundown\n"
                    "THERUNDOWN_API_KEY=abc123\n"
                    "THERUNDOWN_BASE_URL=https://therundown.io/api/v2\n"
                ),
                encoding="utf-8",
            )

            repo_root = Path(__file__).resolve().parents[1]
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "betbot.cli",
                    "live-candidates",
                    "--env-file",
                    str(env_file),
                    "--sport-id",
                    "4",
                    "--event-date",
                    "2026-03-28",
                ],
                cwd=repo_root,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertTrue(
                (
                    "TheRundown request failed:" in result.stderr
                    or "Failed to fetch events from TheRundown" in result.stderr
                ),
                msg=result.stderr,
            )
            self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
