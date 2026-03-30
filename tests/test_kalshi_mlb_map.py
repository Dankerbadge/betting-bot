import tempfile
from datetime import datetime
import json
from pathlib import Path
import unittest
from urllib.error import URLError
from unittest.mock import patch
from zoneinfo import ZoneInfo

from betbot.kalshi_mlb_map import (
    _kalshi_get_json,
    _therundown_json_get,
    TheRundownMlbEvent,
    extract_kalshi_mlb_rows,
    extract_therundown_mlb_events,
    run_kalshi_mlb_map,
)


class KalshiMlbMapTests(unittest.TestCase):
    def test_kalshi_get_json_fails_over_api_root_on_dns_error(self) -> None:
        requested_urls: list[str] = []

        def fake_http_get_json(
            url: str,
            headers: dict[str, str],
            timeout_seconds: float,
        ) -> tuple[int, object]:
            requested_urls.append(url)
            _ = headers
            _ = timeout_seconds
            if "api.elections.kalshi.com" in url:
                raise URLError("dns failed")
            return 200, {"market": {"ticker": "KXTEST-1"}}

        status_code, payload = _kalshi_get_json(
            env_data={
                "KALSHI_ENV": "prod",
                "KALSHI_ACCESS_KEY_ID": "key123",
                "KALSHI_PRIVATE_KEY_PATH": "/tmp/key.pem",
            },
            path_with_query="/markets/KXTEST-1",
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,  # type: ignore[arg-type]
            sign_request=lambda *_: "signed",
        )

        self.assertEqual(status_code, 200)
        self.assertTrue(any("api.elections.kalshi.com" in url for url in requested_urls))
        self.assertTrue(any("trading-api.kalshi.com" in url for url in requested_urls))
        self.assertIsInstance(payload, dict)
        if isinstance(payload, dict):
            self.assertEqual(payload.get("api_root_used"), "https://trading-api.kalshi.com/trade-api/v2")

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

        with patch("betbot.kalshi_mlb_map.time.sleep") as mock_sleep:
            status_code, payload = _therundown_json_get(
                "https://therundown.example/events",
                15.0,
                fake_http_get_json,  # type: ignore[arg-type]
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload, {"events": []})
        self.assertEqual(attempts, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_therundown_json_get_retries_retryable_status_then_succeeds(self) -> None:
        attempts = 0

        def fake_http_get_json(
            url: str,
            headers: dict[str, str],
            timeout_seconds: float,
        ) -> tuple[int, object]:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                return 401, {"error": "temporary auth upstream"}
            return 200, {"events": []}

        with patch("betbot.kalshi_mlb_map.time.sleep") as mock_sleep:
            status_code, payload = _therundown_json_get(
                "https://therundown.example/events",
                15.0,
                fake_http_get_json,  # type: ignore[arg-type]
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload, {"events": []})
        self.assertEqual(attempts, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_therundown_json_get_retries_transient_network_error_then_succeeds(self) -> None:
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

        with patch("betbot.kalshi_mlb_map.time.sleep") as mock_sleep:
            status_code, payload = _therundown_json_get(
                "https://therundown.example/events",
                15.0,
                fake_http_get_json,  # type: ignore[arg-type]
            )

        self.assertEqual(status_code, 200)
        self.assertEqual(payload, {"events": []})
        self.assertEqual(attempts, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_extract_therundown_mlb_events(self) -> None:
        events = [
            {
                "event_id": "evt1",
                "event_date": "2026-03-27T23:07:00Z",
                "teams": [
                    {"abbreviation": "OAK", "name": "Oakland", "mascot": "Athletics", "is_away": True, "is_home": False},
                    {"abbreviation": "TOR", "name": "Toronto", "mascot": "Blue Jays", "is_away": False, "is_home": True},
                ],
                "markets": [
                    {
                        "name": "moneyline",
                        "participants": [
                            {
                                "name": "Oakland Athletics",
                                "lines": [
                                    {
                                        "prices": {
                                            "19": {"price": 145, "updated_at": "2026-03-27T18:00:00Z"},
                                            "23": {"price": 150, "updated_at": "2026-03-27T19:00:00Z"},
                                        }
                                    }
                                ],
                            },
                            {
                                "name": "Toronto Blue Jays",
                                "lines": [
                                    {
                                        "prices": {
                                            "19": {"price": -155, "updated_at": "2026-03-27T18:00:00Z"},
                                            "23": {"price": -165, "updated_at": "2026-03-27T19:00:00Z"},
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
        extracted = extract_therundown_mlb_events(events=events, affiliate_names=affiliate_names, min_books=2)
        self.assertEqual(len(extracted), 1)
        event = extracted[0]
        self.assertEqual(event.away_abbr, "OAK")
        self.assertEqual(event.home_abbr, "TOR")
        self.assertEqual(event.away_best_book, "FanDuel")
        self.assertEqual(event.home_best_book, "DraftKings")
        self.assertEqual(event.away_best_quote_age_seconds, 0.0)
        self.assertEqual(event.home_best_quote_age_seconds, 3600.0)

    def test_extract_kalshi_mlb_rows(self) -> None:
        therundown_events = extract_therundown_mlb_events(
            events=[
                {
                    "event_id": "evt1",
                    "event_date": "2026-03-27T23:07:00Z",
                    "teams": [
                        {"abbreviation": "OAK", "name": "Oakland", "mascot": "Athletics", "is_away": True, "is_home": False},
                        {"abbreviation": "TOR", "name": "Toronto", "mascot": "Blue Jays", "is_away": False, "is_home": True},
                    ],
                    "markets": [
                        {
                            "name": "moneyline",
                            "participants": [
                                {
                                    "name": "Oakland Athletics",
                                    "lines": [
                                        {
                                            "prices": {
                                                "19": {"price": 145, "updated_at": "2026-03-27T18:00:00Z"},
                                                "23": {"price": 150, "updated_at": "2026-03-27T19:00:00Z"},
                                            }
                                        }
                                    ],
                                },
                                {
                                    "name": "Toronto Blue Jays",
                                    "lines": [
                                        {
                                            "prices": {
                                                "19": {"price": -155, "updated_at": "2026-03-27T18:00:00Z"},
                                                "23": {"price": -165, "updated_at": "2026-03-27T19:00:00Z"},
                                            }
                                        }
                                    ],
                                },
                            ],
                        }
                    ],
                }
            ],
            affiliate_names={"19": "DraftKings", "23": "FanDuel"},
            min_books=2,
        )
        kalshi_markets = [
            {
                "ticker": "KXMLBGAME-26MAR271907OAKTOR-OAK",
                "event_ticker": "KXMLBGAME-26MAR271907OAKTOR",
                "title": "Oakland Athletics vs Toronto Blue Jays Winner?",
                "yes_sub_title": "Oakland Athletics",
                "rules_primary": "If Oakland Athletics wins...",
                "rules_secondary": "If postponed within two days...",
                "yes_ask_dollars": "0.39",
                "yes_bid_dollars": "0.38",
            },
            {
                "ticker": "KXMLBGAME-26MAR271907OAKTOR-TOR",
                "event_ticker": "KXMLBGAME-26MAR271907OAKTOR",
                "title": "Oakland Athletics vs Toronto Blue Jays Winner?",
                "yes_sub_title": "Toronto Blue Jays",
                "rules_primary": "If Toronto Blue Jays wins...",
                "rules_secondary": "If postponed within two days...",
                "yes_ask_dollars": "0.63",
                "yes_bid_dollars": "0.62",
            },
        ]
        rows = extract_kalshi_mlb_rows(therundown_events=therundown_events, kalshi_markets=kalshi_markets)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["selection"], "Oakland Athletics")
        self.assertEqual(rows[0]["confidence"], "high")
        toronto = next(row for row in rows if row["selection"] == "Toronto Blue Jays")
        self.assertGreater(toronto["gross_edge_buy_no"], 0)
        self.assertEqual(toronto["best_entry_side"], "no")
        self.assertEqual(toronto["best_maker_entry_side"], "yes")
        self.assertIn("therundown_consensus_confidence", rows[0])
        self.assertIn("best_entry_confidence_adjusted_net_edge_after_fees_1x", rows[0])
        self.assertIn("best_entry_confidence_adjusted_roi_on_cost_after_fees_1x", rows[0])
        self.assertIn("best_entry_rank_score", rows[0])

    def test_extract_kalshi_mlb_rows_can_prefer_no_side(self) -> None:
        therundown_events = extract_therundown_mlb_events(
            events=[
                {
                    "event_id": "evt1",
                    "event_date": "2026-03-27T23:07:00Z",
                    "teams": [
                        {"abbreviation": "OAK", "name": "Oakland", "mascot": "Athletics", "is_away": True, "is_home": False},
                        {"abbreviation": "TOR", "name": "Toronto", "mascot": "Blue Jays", "is_away": False, "is_home": True},
                    ],
                    "markets": [
                        {
                            "name": "moneyline",
                            "participants": [
                                {
                                    "name": "Oakland Athletics",
                                    "lines": [
                                        {
                                            "prices": {
                                                "19": {"price": 145, "updated_at": "2026-03-27T18:00:00Z"},
                                                "23": {"price": 150, "updated_at": "2026-03-27T19:00:00Z"},
                                            }
                                        }
                                    ],
                                },
                                {
                                    "name": "Toronto Blue Jays",
                                    "lines": [
                                        {
                                            "prices": {
                                                "19": {"price": -155, "updated_at": "2026-03-27T18:00:00Z"},
                                                "23": {"price": -165, "updated_at": "2026-03-27T19:00:00Z"},
                                            }
                                        }
                                    ],
                                },
                            ],
                        }
                    ],
                }
            ],
            affiliate_names={"19": "DraftKings", "23": "FanDuel"},
            min_books=2,
        )
        kalshi_markets = [
            {
                "ticker": "KXMLBGAME-26MAR271907OAKTOR-OAK",
                "event_ticker": "KXMLBGAME-26MAR271907OAKTOR",
                "title": "Oakland Athletics vs Toronto Blue Jays Winner?",
                "yes_sub_title": "Oakland Athletics",
                "rules_primary": "If Oakland Athletics wins...",
                "rules_secondary": "If postponed within two days...",
                "yes_ask_dollars": "0.46",
                "yes_bid_dollars": "0.45",
            }
        ]

        rows = extract_kalshi_mlb_rows(therundown_events=therundown_events, kalshi_markets=kalshi_markets)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["best_entry_side"], "no")
        self.assertGreater(rows[0]["gross_edge_buy_no"], 0)
        self.assertGreater(rows[0]["net_edge_buy_no_after_fees_1x"], 0)
        self.assertGreater(rows[0]["best_maker_entry_edge"], 0)
        self.assertEqual(rows[0]["best_maker_entry_side"], "yes")

    def test_extract_kalshi_mlb_rows_penalizes_stale_therundown_best_quote_in_rank(self) -> None:
        therundown_events = extract_therundown_mlb_events(
            events=[
                {
                    "event_id": "evt1",
                    "event_date": "2026-03-27T23:07:00Z",
                    "teams": [
                        {"abbreviation": "OAK", "name": "Oakland", "mascot": "Athletics", "is_away": True, "is_home": False},
                        {"abbreviation": "TOR", "name": "Toronto", "mascot": "Blue Jays", "is_away": False, "is_home": True},
                    ],
                    "markets": [
                        {
                            "name": "moneyline",
                            "participants": [
                                {
                                    "name": "Oakland Athletics",
                                    "lines": [
                                        {
                                            "prices": {
                                                "19": {"price": 155, "updated_at": "2026-03-27T18:00:00Z"},
                                                "23": {"price": 145, "updated_at": "2026-03-27T19:00:00Z"},
                                            }
                                        }
                                    ],
                                },
                                {
                                    "name": "Toronto Blue Jays",
                                    "lines": [
                                        {
                                            "prices": {
                                                "19": {"price": -165, "updated_at": "2026-03-27T18:00:00Z"},
                                                "23": {"price": -155, "updated_at": "2026-03-27T19:00:00Z"},
                                            }
                                        }
                                    ],
                                },
                            ],
                        }
                    ],
                }
            ],
            affiliate_names={"19": "DraftKings", "23": "FanDuel"},
            min_books=2,
        )
        kalshi_markets = [
            {
                "ticker": "KXMLBGAME-26MAR271907OAKTOR-OAK",
                "event_ticker": "KXMLBGAME-26MAR271907OAKTOR",
                "title": "Oakland Athletics vs Toronto Blue Jays Winner?",
                "yes_sub_title": "Oakland Athletics",
                "rules_primary": "If Oakland Athletics wins...",
                "rules_secondary": "If postponed within two days...",
                "yes_ask_dollars": "0.39",
                "yes_bid_dollars": "0.38",
            },
            {
                "ticker": "KXMLBGAME-26MAR271907OAKTOR-TOR",
                "event_ticker": "KXMLBGAME-26MAR271907OAKTOR",
                "title": "Oakland Athletics vs Toronto Blue Jays Winner?",
                "yes_sub_title": "Toronto Blue Jays",
                "rules_primary": "If Toronto Blue Jays wins...",
                "rules_secondary": "If postponed within two days...",
                "yes_ask_dollars": "0.61",
                "yes_bid_dollars": "0.60",
            },
        ]

        rows = extract_kalshi_mlb_rows(therundown_events=therundown_events, kalshi_markets=kalshi_markets)

        self.assertEqual(rows[0]["selection"], "Toronto Blue Jays")
        oakland = next(row for row in rows if row["selection"] == "Oakland Athletics")
        self.assertEqual(oakland["therundown_best_quote_age_seconds"], 3600.0)
        self.assertAlmostEqual(oakland["therundown_stale_quote_penalty"], 0.02, places=6)
        self.assertLess(
            oakland["best_entry_rank_score"],
            oakland["best_entry_confidence_adjusted_roi_on_cost_after_fees_1x"],
        )

    def test_extract_therundown_mlb_events_uses_robust_consensus(self) -> None:
        extracted = extract_therundown_mlb_events(
            events=[
                {
                    "event_id": "evt1",
                    "event_date": "2026-03-27T23:07:00Z",
                    "teams": [
                        {"abbreviation": "OAK", "name": "Oakland", "mascot": "Athletics", "is_away": True, "is_home": False},
                        {"abbreviation": "TOR", "name": "Toronto", "mascot": "Blue Jays", "is_away": False, "is_home": True},
                    ],
                    "markets": [
                        {
                            "name": "moneyline",
                            "participants": [
                                {
                                    "name": "Oakland Athletics",
                                    "lines": [{"prices": {"19": {"price": 145}, "22": {"price": 150}, "23": {"price": 250}}}],
                                },
                                {
                                    "name": "Toronto Blue Jays",
                                    "lines": [{"prices": {"19": {"price": -155}, "22": {"price": -165}, "23": {"price": -300}}}],
                                },
                            ],
                        }
                    ],
                }
            ],
            affiliate_names={"19": "DraftKings", "22": "BetMGM", "23": "FanDuel"},
            min_books=2,
        )

        self.assertEqual(len(extracted), 1)
        event = extracted[0]
        self.assertGreater(event.away_prob, event.away_mean_prob)
        self.assertLess(event.home_prob, event.home_mean_prob)
        self.assertAlmostEqual(event.away_robust_prob, 0.387498, places=6)
        self.assertAlmostEqual(event.away_prob_range, 0.135733, places=6)
        self.assertAlmostEqual(event.away_consensus_stability, 0.5, places=6)

    def test_extract_kalshi_mlb_rows_penalizes_disputed_consensus_in_rank(self) -> None:
        aligned_events = extract_therundown_mlb_events(
            events=[
                {
                    "event_id": "evt1",
                    "event_date": "2026-03-27T23:07:00Z",
                    "teams": [
                        {"abbreviation": "OAK", "name": "Oakland", "mascot": "Athletics", "is_away": True, "is_home": False},
                        {"abbreviation": "TOR", "name": "Toronto", "mascot": "Blue Jays", "is_away": False, "is_home": True},
                    ],
                    "markets": [
                        {
                            "name": "moneyline",
                            "participants": [
                                {
                                    "name": "Oakland Athletics",
                                    "lines": [{"prices": {"19": {"price": 145}, "22": {"price": 146}, "23": {"price": 147}}}],
                                },
                                {
                                    "name": "Toronto Blue Jays",
                                    "lines": [{"prices": {"19": {"price": -155}, "22": {"price": -156}, "23": {"price": -157}}}],
                                },
                            ],
                        }
                    ],
                }
            ],
            affiliate_names={"19": "DraftKings", "22": "BetMGM", "23": "FanDuel"},
            min_books=2,
        )
        disputed_events = extract_therundown_mlb_events(
            events=[
                {
                    "event_id": "evt1",
                    "event_date": "2026-03-27T23:07:00Z",
                    "teams": [
                        {"abbreviation": "OAK", "name": "Oakland", "mascot": "Athletics", "is_away": True, "is_home": False},
                        {"abbreviation": "TOR", "name": "Toronto", "mascot": "Blue Jays", "is_away": False, "is_home": True},
                    ],
                    "markets": [
                        {
                            "name": "moneyline",
                            "participants": [
                                {
                                    "name": "Oakland Athletics",
                                    "lines": [{"prices": {"19": {"price": 120}, "22": {"price": 145}, "23": {"price": 250}}}],
                                },
                                {
                                    "name": "Toronto Blue Jays",
                                    "lines": [{"prices": {"19": {"price": -130}, "22": {"price": -155}, "23": {"price": -300}}}],
                                },
                            ],
                        }
                    ],
                }
            ],
            affiliate_names={"19": "DraftKings", "22": "BetMGM", "23": "FanDuel"},
            min_books=2,
        )
        kalshi_markets = [
            {
                "ticker": "KXMLBGAME-26MAR271907OAKTOR-OAK",
                "event_ticker": "KXMLBGAME-26MAR271907OAKTOR",
                "title": "Oakland Athletics vs Toronto Blue Jays Winner?",
                "yes_sub_title": "Oakland Athletics",
                "rules_primary": "If Oakland Athletics wins...",
                "rules_secondary": "If postponed within two days...",
                "yes_ask_dollars": "0.42",
                "yes_bid_dollars": "0.41",
            }
        ]

        aligned_row = extract_kalshi_mlb_rows(
            therundown_events=aligned_events,
            kalshi_markets=kalshi_markets,
        )[0]
        disputed_row = extract_kalshi_mlb_rows(
            therundown_events=disputed_events,
            kalshi_markets=kalshi_markets,
        )[0]

        self.assertGreater(aligned_row["therundown_consensus_stability"], disputed_row["therundown_consensus_stability"])
        self.assertGreater(
            aligned_row["best_entry_rank_score"],
            aligned_row["best_entry_confidence_adjusted_roi_on_cost_after_fees_1x"],
        )
        self.assertLess(
            disputed_row["best_entry_rank_score"],
            disputed_row["best_entry_confidence_adjusted_roi_on_cost_after_fees_1x"],
        )
        self.assertGreater(
            aligned_row["therundown_consensus_confidence"],
            disputed_row["therundown_consensus_confidence"],
        )
        self.assertGreater(disputed_row["therundown_model_prob"], aligned_row["therundown_model_prob"])

    def test_extract_kalshi_mlb_rows_ranks_capital_efficient_entries_higher(self) -> None:
        therundown_events = [
            TheRundownMlbEvent(
                event_id="evt-favorite",
                event_date_utc="2026-03-27T23:07:00Z",
                local_start=datetime(2026, 3, 27, 19, 7, tzinfo=ZoneInfo("America/New_York")),
                away_abbr="AAA",
                away_team="Alpha Away",
                home_abbr="BBB",
                home_team="Beta Home",
                away_prob=0.8,
                home_prob=0.2,
                away_mean_prob=0.8,
                home_mean_prob=0.2,
                away_robust_prob=0.8,
                home_robust_prob=0.2,
                away_prob_low=0.79,
                home_prob_low=0.19,
                away_prob_high=0.81,
                home_prob_high=0.21,
                away_prob_range=0.02,
                home_prob_range=0.02,
                away_prob_stddev=0.01,
                home_prob_stddev=0.01,
                away_consensus_stability=0.95,
                home_consensus_stability=0.95,
                away_best_book="DraftKings",
                away_best_odds=1.25,
                away_best_quote_updated_at="2026-03-27T19:00:00Z",
                away_best_quote_age_seconds=0.0,
                home_best_book="DraftKings",
                home_best_odds=4.0,
                home_best_quote_updated_at="2026-03-27T19:00:00Z",
                home_best_quote_age_seconds=0.0,
                consensus_book_count=3,
            ),
            TheRundownMlbEvent(
                event_id="evt-dog",
                event_date_utc="2026-03-27T23:10:00Z",
                local_start=datetime(2026, 3, 27, 19, 10, tzinfo=ZoneInfo("America/New_York")),
                away_abbr="CCC",
                away_team="Gamma Away",
                home_abbr="DDD",
                home_team="Delta Home",
                away_prob=0.23,
                home_prob=0.77,
                away_mean_prob=0.23,
                home_mean_prob=0.77,
                away_robust_prob=0.23,
                home_robust_prob=0.77,
                away_prob_low=0.22,
                home_prob_low=0.76,
                away_prob_high=0.24,
                home_prob_high=0.78,
                away_prob_range=0.02,
                home_prob_range=0.02,
                away_prob_stddev=0.01,
                home_prob_stddev=0.01,
                away_consensus_stability=0.95,
                home_consensus_stability=0.95,
                away_best_book="FanDuel",
                away_best_odds=4.35,
                away_best_quote_updated_at="2026-03-27T19:00:00Z",
                away_best_quote_age_seconds=0.0,
                home_best_book="FanDuel",
                home_best_odds=1.3,
                home_best_quote_updated_at="2026-03-27T19:00:00Z",
                home_best_quote_age_seconds=0.0,
                consensus_book_count=3,
            ),
        ]
        kalshi_markets = [
            {
                "ticker": "KXMLBGAME-26MAR271907AAABBB-AAA",
                "event_ticker": "KXMLBGAME-26MAR271907AAABBB",
                "title": "Alpha Away vs Beta Home Winner?",
                "yes_sub_title": "Alpha Away",
                "rules_primary": "If Alpha Away wins...",
                "rules_secondary": "",
                "yes_ask_dollars": "0.75",
                "yes_bid_dollars": "0.74",
            },
            {
                "ticker": "KXMLBGAME-26MAR271910CCCDDD-CCC",
                "event_ticker": "KXMLBGAME-26MAR271910CCCDDD",
                "title": "Gamma Away vs Delta Home Winner?",
                "yes_sub_title": "Gamma Away",
                "rules_primary": "If Gamma Away wins...",
                "rules_secondary": "",
                "yes_ask_dollars": "0.19",
                "yes_bid_dollars": "0.18",
            },
        ]

        rows = extract_kalshi_mlb_rows(therundown_events=therundown_events, kalshi_markets=kalshi_markets)

        self.assertEqual(rows[0]["selection"], "Gamma Away")
        self.assertGreater(
            rows[0]["best_entry_confidence_adjusted_roi_on_cost_after_fees_1x"],
            rows[1]["best_entry_confidence_adjusted_roi_on_cost_after_fees_1x"],
        )
        self.assertLess(
            rows[0]["best_entry_net_edge_after_fees_1x"],
            rows[1]["best_entry_net_edge_after_fees_1x"],
        )

    def test_run_kalshi_mlb_map_writes_outputs(self) -> None:
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
                if "/affiliates?" in url:
                    return 200, {
                        "affiliates": [
                            {"affiliate_id": 19, "affiliate_name": "DraftKings"},
                            {"affiliate_id": 23, "affiliate_name": "FanDuel"},
                        ]
                    }
                if "/sports/3/events/" in url:
                    return 200, {
                        "events": [
                            {
                                "event_id": "evt1",
                                "event_date": "2026-03-27T23:07:00Z",
                                "teams": [
                                    {
                                        "abbreviation": "OAK",
                                        "name": "Oakland",
                                        "mascot": "Athletics",
                                        "is_away": True,
                                        "is_home": False,
                                    },
                                    {
                                        "abbreviation": "TOR",
                                        "name": "Toronto",
                                        "mascot": "Blue Jays",
                                        "is_away": False,
                                        "is_home": True,
                                    },
                                ],
                                "markets": [
                                    {
                                        "name": "moneyline",
                                        "participants": [
                                            {
                                                "name": "Oakland Athletics",
                                                "lines": [
                                                    {
                                                        "prices": {
                                                            "19": {"price": 145, "updated_at": "2026-03-27T18:00:00Z"},
                                                            "23": {"price": 150, "updated_at": "2026-03-27T19:00:00Z"},
                                                        }
                                                    }
                                                ],
                                            },
                                            {
                                                "name": "Toronto Blue Jays",
                                                "lines": [
                                                    {
                                                        "prices": {
                                                            "19": {"price": -155, "updated_at": "2026-03-27T18:00:00Z"},
                                                            "23": {"price": -165, "updated_at": "2026-03-27T19:00:00Z"},
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
                if url.endswith("/markets/KXMLBGAME-26MAR271907OAKTOR-OAK"):
                    return 200, {
                        "market": {
                            "ticker": "KXMLBGAME-26MAR271907OAKTOR-OAK",
                            "event_ticker": "KXMLBGAME-26MAR271907OAKTOR",
                            "title": "Oakland Athletics vs Toronto Blue Jays Winner?",
                            "yes_sub_title": "Oakland Athletics",
                            "rules_primary": "If Oakland Athletics wins...",
                            "rules_secondary": "If postponed within two days...",
                            "yes_ask_dollars": "0.39",
                            "yes_bid_dollars": "0.38",
                        }
                    }
                if url.endswith("/markets/KXMLBGAME-26MAR271907OAKTOR-TOR"):
                    return 200, {
                        "market": {
                            "ticker": "KXMLBGAME-26MAR271907OAKTOR-TOR",
                            "event_ticker": "KXMLBGAME-26MAR271907OAKTOR",
                            "title": "Oakland Athletics vs Toronto Blue Jays Winner?",
                            "yes_sub_title": "Toronto Blue Jays",
                            "rules_primary": "If Toronto Blue Jays wins...",
                            "rules_secondary": "If postponed within two days...",
                            "yes_ask_dollars": "0.63",
                            "yes_bid_dollars": "0.62",
                        }
                    }
                return 404, {"error": "not found"}

            summary = run_kalshi_mlb_map(
                env_file=str(env_file),
                event_date="2026-03-27",
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                http_get_json=fake_http_get_json,
                sign_request=lambda *_: "signed",
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["mapped_rows"], 2)
            self.assertEqual(summary["positive_buy_yes_rows"], 1)
            self.assertEqual(summary["positive_buy_no_rows"], 1)
            self.assertEqual(summary["positive_best_entry_rows"], 0)
            self.assertEqual(summary["top_rows"][0]["best_entry_side"], "yes")
            self.assertIn("best_entry_rank_score", summary["top_rows"][0])
            self.assertIn("best_entry_confidence_adjusted_net_edge_after_fees_1x", summary["top_rows"][0])
            self.assertIn("best_entry_confidence_adjusted_roi_on_cost_after_fees_1x", summary["top_rows"][0])
            self.assertIn("therundown_stale_quote_penalty", summary["top_rows"][0])
            self.assertIn("therundown_consensus_stability", summary["top_rows"][0])
            self.assertIn("therundown_consensus_confidence", summary["top_rows"][0])
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_mlb_map_falls_back_to_cached_summary_on_dns_failure(self) -> None:
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

            cached_csv = base / "kalshi_mlb_map_2026-03-27_20260329_070000.csv"
            cached_csv.write_text("selection\nCached Row\n", encoding="utf-8")
            cached_summary = base / "kalshi_mlb_map_summary_2026-03-27_20260329_070000.json"
            cached_summary.write_text(
                json.dumps(
                    {
                        "captured_at": "2026-03-29T07:00:00+00:00",
                        "event_date": "2026-03-27",
                        "affiliate_ids": ["19", "23"],
                        "therundown_events_considered": 2,
                        "kalshi_mlb_markets_considered": 4,
                        "mapped_rows": 2,
                        "positive_buy_yes_rows": 1,
                        "positive_net_buy_yes_rows": 1,
                        "positive_buy_no_rows": 1,
                        "positive_net_buy_no_rows": 1,
                        "positive_best_entry_rows": 1,
                        "status": "ready",
                        "top_rows": [{"selection": "Cached Row"}],
                        "output_csv": str(cached_csv),
                    }
                ),
                encoding="utf-8",
            )

            def failing_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            summary = run_kalshi_mlb_map(
                env_file=str(env_file),
                event_date="2026-03-27",
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                http_get_json=failing_http_get_json,  # type: ignore[arg-type]
                sign_request=lambda *_: "signed",
            )

            self.assertEqual(summary["status"], "stale_ready")
            self.assertEqual(summary["mapped_rows"], 2)
            self.assertEqual(summary["output_csv"], str(cached_csv))
            self.assertEqual(summary["fallback_source_status"], "ready")
            self.assertEqual(summary["fallback_summary_file"], str(cached_summary))
            self.assertEqual(summary["error"], "TheRundown request failed: dns failed")
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_mlb_map_returns_error_when_dns_fails_without_cache(self) -> None:
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

            def failing_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                raise URLError("dns failed")

            summary = run_kalshi_mlb_map(
                env_file=str(env_file),
                event_date="2026-03-27",
                output_dir=str(base),
                affiliate_ids=("19", "23"),
                http_get_json=failing_http_get_json,  # type: ignore[arg-type]
                sign_request=lambda *_: "signed",
            )

            self.assertEqual(summary["status"], "error")
            self.assertEqual(summary["mapped_rows"], 0)
            self.assertEqual(summary["error"], "TheRundown request failed: dns failed")
            self.assertTrue(Path(summary["output_file"]).exists())


if __name__ == "__main__":
    unittest.main()
