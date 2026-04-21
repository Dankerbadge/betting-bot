from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from betbot.coldmath_replication import run_coldmath_replication_plan


class ColdMathReplicationTests(unittest.TestCase):
    def test_run_plan_builds_no_side_candidates_from_latest_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            (out_dir / "coldmath_snapshot_summary_latest.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "family_behavior": {
                            "behavior_tags": ["multi_strike_clustering", "high_price_no_inventory"],
                            "no_outcome_ratio": 0.64,
                            "positions_with_high_price_no": 12,
                            "families": [
                                {
                                    "family_key": "highest-temperature-in-new-york-on-april-21-2026",
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "polymarket_temperature_markets_summary_20260421_000001.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "coldmath_temperature_alignment": {
                            "matched_ratio": 0.42,
                            "top_matched_positions": [
                                {
                                    "market_slug": "highest-temp-nyc",
                                    "question": "Highest temperature in NYC on April 21, 2026?",
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_collect_summary_20260421_000001.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "market_tickers": [
                            "KXHIGHNYC-26APR21-B80",
                            "KXHIGHNYC-26APR21-B81",
                            "KXRAINNYCM-26APR-1",
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_latest.json").write_text(
                json.dumps(
                    {
                        "markets": {
                            "KXHIGHNYC-26APR21-B80": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.72,
                                    "best_no_ask_dollars": 0.74,
                                    "best_yes_bid_dollars": 0.24,
                                    "best_yes_ask_dollars": 0.26,
                                    "yes_spread_dollars": 0.02,
                                }
                            },
                            "KXHIGHNYC-26APR21-B81": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.68,
                                    "best_no_ask_dollars": 0.71,
                                    "best_yes_bid_dollars": 0.28,
                                    "best_yes_ask_dollars": 0.31,
                                    "yes_spread_dollars": 0.03,
                                }
                            },
                            "KXRAINNYCM-26APR-1": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.42,
                                    "best_no_ask_dollars": 0.49,
                                    "best_yes_bid_dollars": 0.51,
                                    "best_yes_ask_dollars": 0.58,
                                    "yes_spread_dollars": 0.07,
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            payload = run_coldmath_replication_plan(output_dir=str(out_dir), top_n=5)

            self.assertEqual(payload["status"], "ready")
            self.assertEqual(payload["preferred_side"], "no")
            self.assertGreaterEqual(float(payload["preferred_max_cost"]), 0.94)
            self.assertGreaterEqual(int(payload["candidate_count"]), 1)
            first = payload["candidates"][0]
            self.assertEqual(first["side"], "no")
            self.assertTrue(first["market"].startswith("KXHIGH"))
            self.assertIn("liquidity", first)
            self.assertTrue(bool(first["liquidity"]["is_tradable"]))
            self.assertTrue(Path(str(payload["output_file"])).exists())
            self.assertTrue(Path(str(payload["latest_file"])).exists())

    def test_run_plan_prefers_rain_theme_when_alignment_signals_rain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            (out_dir / "coldmath_snapshot_summary_latest.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "family_behavior": {
                            "behavior_tags": [],
                            "no_outcome_ratio": 0.48,
                            "positions_with_high_price_no": 0,
                            "families": [
                                {
                                    "family_key": "rainfall-in-nyc-in-april-2026",
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "polymarket_temperature_markets_summary_20260421_000001.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "coldmath_temperature_alignment": {
                            "matched_ratio": 0.25,
                            "top_matched_positions": [
                                {
                                    "market_slug": "rainfall-in-nyc",
                                    "question": "Will rainfall in NYC exceed 1 inch?",
                                }
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_collect_summary_20260421_000001.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "market_tickers": [
                            "KXHMONTHRANGE-26APR-B1.200",
                            "KXRAINNYCM-26APR-1",
                            "KXRAINNYCM-26APR-2",
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_latest.json").write_text(
                json.dumps(
                    {
                        "markets": {
                            "KXHMONTHRANGE-26APR-B1.200": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.38,
                                    "best_no_ask_dollars": 0.46,
                                    "best_yes_bid_dollars": 0.54,
                                    "best_yes_ask_dollars": 0.62,
                                    "yes_spread_dollars": 0.08,
                                }
                            },
                            "KXRAINNYCM-26APR-1": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.55,
                                    "best_no_ask_dollars": 0.6,
                                    "best_yes_bid_dollars": 0.4,
                                    "best_yes_ask_dollars": 0.45,
                                    "yes_spread_dollars": 0.05,
                                }
                            },
                            "KXRAINNYCM-26APR-2": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.5,
                                    "best_no_ask_dollars": 0.54,
                                    "best_yes_bid_dollars": 0.46,
                                    "best_yes_ask_dollars": 0.5,
                                    "yes_spread_dollars": 0.04,
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            payload = run_coldmath_replication_plan(output_dir=str(out_dir), top_n=3)

            self.assertEqual(payload["status"], "ready")
            self.assertEqual(payload["theme"], "rain")
            first = payload["candidates"][0]
            self.assertIn("RAIN", str(first["market"]))

    def test_run_plan_liquidity_filter_blocks_unquoted_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            (out_dir / "coldmath_snapshot_summary_latest.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "family_behavior": {
                            "behavior_tags": ["no_side_bias"],
                            "no_outcome_ratio": 0.6,
                            "positions_with_high_price_no": 6,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "polymarket_temperature_markets_summary_20260421_000001.json").write_text(
                json.dumps({"status": "ready", "coldmath_temperature_alignment": {"matched_ratio": 0.1}}),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_collect_summary_20260421_000001.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "market_tickers": ["KXHIGHA-26APR21-B80", "KXHIGHA-26APR21-B81"],
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_latest.json").write_text(
                json.dumps(
                    {
                        "markets": {
                            "KXHIGHA-26APR21-B80": {"top_of_book": {}},
                            "KXHIGHA-26APR21-B81": {"top_of_book": {}},
                        }
                    }
                ),
                encoding="utf-8",
            )

            payload = run_coldmath_replication_plan(output_dir=str(out_dir), top_n=3)

            self.assertEqual(payload["status"], "no_candidates_after_filters")
            self.assertEqual(int(payload["candidate_count"]), 0)
            liquidity_filter = dict(payload.get("liquidity_filter") or {})
            self.assertTrue(bool(liquidity_filter.get("enabled")))
            self.assertGreaterEqual(int(liquidity_filter.get("filtered_out") or 0), 2)

    def test_run_plan_family_cap_diversifies_when_one_family_dominates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            (out_dir / "coldmath_snapshot_summary_latest.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "family_behavior": {
                            "behavior_tags": ["multi_strike_clustering", "no_side_bias"],
                            "no_outcome_ratio": 0.58,
                            "positions_with_high_price_no": 4,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "polymarket_temperature_markets_summary_20260421_000001.json").write_text(
                json.dumps({"status": "ready", "coldmath_temperature_alignment": {"matched_ratio": 0.2}}),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_collect_summary_20260421_000001.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "market_tickers": [
                            "KXHFA-26APR21-B80",
                            "KXHFA-26APR21-B81",
                            "KXHFA-26APR21-B82",
                            "KXHFA-26APR21-B83",
                            "KXHFB-26APR21-B80",
                            "KXHFB-26APR21-B81",
                        ],
                    }
                ),
                encoding="utf-8",
            )
            markets = {}
            for ticker in [
                "KXHFA-26APR21-B80",
                "KXHFA-26APR21-B81",
                "KXHFA-26APR21-B82",
                "KXHFA-26APR21-B83",
                "KXHFB-26APR21-B80",
                "KXHFB-26APR21-B81",
            ]:
                markets[ticker] = {
                    "top_of_book": {
                        "best_no_bid_dollars": 0.5,
                        "best_no_ask_dollars": 0.54,
                        "best_yes_bid_dollars": 0.46,
                        "best_yes_ask_dollars": 0.5,
                        "yes_spread_dollars": 0.04,
                    }
                }
            (out_dir / "kalshi_ws_state_latest.json").write_text(
                json.dumps({"markets": markets}),
                encoding="utf-8",
            )

            payload = run_coldmath_replication_plan(
                output_dir=str(out_dir),
                top_n=4,
                max_family_candidates=2,
                max_family_share=0.5,
            )

            self.assertEqual(payload["status"], "ready")
            self.assertEqual(int(payload["candidate_count"]), 4)
            family_counts: dict[str, int] = {}
            for row in payload.get("candidates") or []:
                family_key = str(row.get("family_key") or "")
                family_counts[family_key] = family_counts.get(family_key, 0) + 1
            self.assertTrue(all(count <= 2 for count in family_counts.values()))
            self.assertGreaterEqual(len(family_counts), 2)

    def test_run_plan_emits_center_yes_adjacent_no_roles_for_clustered_family(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            (out_dir / "coldmath_snapshot_summary_latest.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "family_behavior": {
                            "behavior_tags": ["no_side_bias", "multi_strike_clustering"],
                            "no_outcome_ratio": 0.61,
                            "positions_with_high_price_no": 7,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "polymarket_temperature_markets_summary_20260421_000001.json").write_text(
                json.dumps({"status": "ready", "coldmath_temperature_alignment": {"matched_ratio": 0.3}}),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_collect_summary_20260421_000001.json").write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "market_tickers": [
                            "KXHIGHNYC-26APR21-B80",
                            "KXHIGHNYC-26APR21-B81",
                            "KXHIGHNYC-26APR21-B82",
                        ],
                    }
                ),
                encoding="utf-8",
            )
            (out_dir / "kalshi_ws_state_latest.json").write_text(
                json.dumps(
                    {
                        "markets": {
                            "KXHIGHNYC-26APR21-B80": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.58,
                                    "best_no_ask_dollars": 0.61,
                                    "best_yes_bid_dollars": 0.39,
                                    "best_yes_ask_dollars": 0.42,
                                    "yes_spread_dollars": 0.03,
                                }
                            },
                            "KXHIGHNYC-26APR21-B81": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.55,
                                    "best_no_ask_dollars": 0.58,
                                    "best_yes_bid_dollars": 0.42,
                                    "best_yes_ask_dollars": 0.45,
                                    "yes_spread_dollars": 0.03,
                                }
                            },
                            "KXHIGHNYC-26APR21-B82": {
                                "top_of_book": {
                                    "best_no_bid_dollars": 0.57,
                                    "best_no_ask_dollars": 0.6,
                                    "best_yes_bid_dollars": 0.4,
                                    "best_yes_ask_dollars": 0.43,
                                    "yes_spread_dollars": 0.03,
                                }
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            payload = run_coldmath_replication_plan(
                output_dir=str(out_dir),
                top_n=3,
                max_family_candidates=3,
                max_family_share=1.0,
            )

            self.assertEqual(payload["status"], "ready")
            rows = list(payload.get("candidates") or [])
            self.assertEqual(len(rows), 3)
            roles = {str(row.get("strategy_role") or "") for row in rows}
            sides = {str(row.get("side") or "") for row in rows}
            self.assertIn("center_bracket_yes", roles)
            self.assertIn("adjacent_bracket_no", roles)
            self.assertIn("yes", sides)
            self.assertIn("no", sides)

    def test_run_plan_handles_missing_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            payload = run_coldmath_replication_plan(output_dir=str(out_dir), top_n=3)
            self.assertEqual(payload["status"], "missing_snapshot_summary")
            self.assertEqual(payload["candidate_count"], 0)
            self.assertIn("coldmath_snapshot_summary_missing", payload["errors"])


if __name__ == "__main__":
    unittest.main()
