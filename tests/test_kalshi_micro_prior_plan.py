from datetime import datetime, timezone
import csv
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_prior_plan import build_micro_prior_plans, run_kalshi_micro_prior_plan
from betbot.kalshi_nonsports_priors import build_prior_rows


HISTORY_FIELDNAMES = [
    "captured_at",
    "category",
    "market_ticker",
    "market_title",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_ask_dollars",
]

PRIOR_FIELDNAMES = [
    "market_ticker",
    "fair_yes_probability",
    "confidence",
    "thesis",
    "source_note",
    "updated_at",
]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class KalshiMicroPriorPlanTests(unittest.TestCase):
    def test_build_micro_prior_plans_supports_no_side(self) -> None:
        enriched_rows = build_prior_rows(
            prior_rows=[
                {
                    "market_ticker": "KXTEST-1",
                    "fair_yes_probability": "0.02",
                    "confidence": "0.7",
                    "thesis": "Test",
                    "source_note": "Note",
                    "updated_at": "2026-03-27T21:00:00+00:00",
                }
            ],
            latest_market_rows={
                "KXTEST-1": {
                    "category": "Politics",
                    "market_title": "Test Market",
                    "close_time": "2026-03-28T12:00:00Z",
                    "hours_to_close": "15",
                    "yes_bid_dollars": "0.03",
                    "yes_ask_dollars": "0.04",
                }
            },
        )

        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=enriched_rows,
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=3,
            min_maker_edge=0.005,
            max_entry_price_dollars=0.99,
        )

        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["side"], "no")
        self.assertEqual(plans[0]["maker_entry_price_dollars"], 0.96)
        self.assertEqual(plans[0]["maker_entry_edge"], 0.02)
        self.assertEqual(plans[0]["maker_entry_edge_conservative"], 0.02)
        self.assertEqual(plans[0]["hours_to_close"], 15.0)
        self.assertEqual(plans[0]["expected_value_dollars"], 0.02)
        self.assertEqual(plans[0]["expected_value_conservative_dollars"], 0.02)
        self.assertAlmostEqual(plans[0]["expected_roi_on_cost"], 0.020833, places=6)
        self.assertAlmostEqual(plans[0]["max_profit_roi_on_cost"], 0.041667, places=6)
        self.assertIn("no_price_dollars", plans[0]["order_payload_preview"])
        self.assertEqual(skip_counts["not_live_matched"], 0)

    def test_run_kalshi_micro_prior_plan_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "market_title": "Test Market",
                        "close_time": "2026-03-28T12:00:00Z",
                        "hours_to_close": "15",
                        "yes_bid_dollars": "0.03",
                        "yes_ask_dollars": "0.04",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-2",
                        "market_title": "Second Market",
                        "close_time": "2026-03-29T12:00:00Z",
                        "hours_to_close": "39",
                        "yes_bid_dollars": "0.01",
                        "yes_ask_dollars": "0.02",
                    },
                ],
            )
            _write_csv(
                priors_csv,
                PRIOR_FIELDNAMES,
                [
                    {
                        "market_ticker": "KXTEST-1",
                        "fair_yes_probability": "0.02",
                        "confidence": "0.7",
                        "thesis": "Test",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    },
                    {
                        "market_ticker": "KXTEST-2",
                        "fair_yes_probability": "0.01",
                        "confidence": "0.6",
                        "thesis": "Test 2",
                        "source_note": "Note 2",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    },
                ],
            )

            summary = run_kalshi_micro_prior_plan(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 22, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["positive_maker_entry_markets"], 2)
            self.assertEqual(summary["planned_orders"], 2)
            self.assertEqual(summary["top_market_ticker"], "KXTEST-1")
            self.assertEqual(summary["top_market_side"], "no")
            self.assertEqual(summary["top_market_hours_to_close"], 15.0)
            self.assertEqual(summary["top_market_maker_entry_price_dollars"], 0.96)
            self.assertEqual(summary["top_market_estimated_entry_cost_dollars"], 0.96)
            self.assertEqual(summary["top_market_expected_value_dollars"], 0.02)
            self.assertAlmostEqual(summary["top_market_expected_roi_on_cost"], 0.020833, places=6)
            self.assertEqual(summary["top_market_estimated_max_profit_dollars"], 0.04)
            self.assertAlmostEqual(summary["top_market_max_profit_roi_on_cost"], 0.041667, places=6)
            self.assertEqual(summary["top_market_fair_probability"], 0.98)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_prior_plan_surfaces_top_weather_history_health(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-30T21:00:00+00:00",
                        "category": "Climate and Weather",
                        "market_ticker": "KXRAINNYC-26MAR30",
                        "market_title": "Will it rain in NYC tonight?",
                        "close_time": "2026-03-31T03:59:00Z",
                        "hours_to_close": "7",
                        "yes_bid_dollars": "0.35",
                        "yes_ask_dollars": "0.36",
                    }
                ],
            )
            prior_fieldnames = PRIOR_FIELDNAMES + [
                "contract_family",
                "weather_station_history_status",
                "weather_station_history_cache_hit",
                "weather_station_history_cache_fallback_used",
                "weather_station_history_cache_fresh",
                "weather_station_history_cache_age_seconds",
                "weather_station_history_live_ready",
                "weather_station_history_live_ready_reason",
            ]
            _write_csv(
                priors_csv,
                prior_fieldnames,
                [
                    {
                        "market_ticker": "KXRAINNYC-26MAR30",
                        "fair_yes_probability": "0.75",
                        "confidence": "0.8",
                        "thesis": "Weather edge",
                        "source_note": "Synthetic test",
                        "updated_at": "2026-03-30T21:00:00+00:00",
                        "contract_family": "daily_rain",
                        "weather_station_history_status": "rate_limited",
                        "weather_station_history_cache_hit": "True",
                        "weather_station_history_cache_fallback_used": "False",
                        "weather_station_history_cache_fresh": "False",
                        "weather_station_history_cache_age_seconds": "90000",
                        "weather_station_history_live_ready": "False",
                        "weather_station_history_live_ready_reason": "status_rate_limited",
                    }
                ],
            )

            summary = run_kalshi_micro_prior_plan(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 30, 21, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["top_market_ticker"], "KXRAINNYC-26MAR30")
            self.assertEqual(summary["top_market_contract_family"], "daily_rain")
            self.assertEqual(summary["top_market_weather_station_history_status"], "rate_limited")
            self.assertFalse(summary["top_market_weather_station_history_live_ready"])
            self.assertEqual(
                summary["top_market_weather_station_history_live_ready_reason"],
                "status_rate_limited",
            )
            self.assertEqual(summary["weather_history_daily_candidates_total"], 1)
            self.assertEqual(summary["weather_history_daily_candidates_live_ready"], 0)
            self.assertEqual(summary["weather_history_daily_candidates_unhealthy"], 1)

    def test_run_kalshi_micro_prior_plan_reports_daily_weather_pre_edge_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-30T21:00:00+00:00",
                        "category": "Climate and Weather",
                        "market_ticker": "KXRAINNYC-26MAR30A",
                        "market_title": "Will it rain in NYC tonight? A",
                        "close_time": "2026-03-31T03:59:00Z",
                        "hours_to_close": "7",
                        "yes_bid_dollars": "0.35",
                        "yes_ask_dollars": "0.36",
                    },
                    {
                        "captured_at": "2026-03-30T21:00:00+00:00",
                        "category": "Climate and Weather",
                        "market_ticker": "KXRAINNYC-26MAR30B",
                        "market_title": "Will it rain in NYC tonight? B",
                        "close_time": "2026-03-31T03:59:00Z",
                        "hours_to_close": "7",
                        "yes_bid_dollars": "",
                        "yes_ask_dollars": "",
                    },
                ],
            )
            prior_fieldnames = PRIOR_FIELDNAMES + [
                "contract_family",
                "fair_yes_probability_conservative",
                "fair_no_probability_conservative",
            ]
            _write_csv(
                priors_csv,
                prior_fieldnames,
                [
                    {
                        "market_ticker": "KXRAINNYC-26MAR30A",
                        "fair_yes_probability": "0.55",
                        "fair_yes_probability_conservative": "0.55",
                        "fair_no_probability_conservative": "0.45",
                        "confidence": "0.8",
                        "thesis": "Weather edge",
                        "source_note": "Synthetic test",
                        "updated_at": "2026-03-30T21:00:00+00:00",
                        "contract_family": "daily_rain",
                    },
                    {
                        "market_ticker": "KXRAINNYC-26MAR30B",
                        "fair_yes_probability": "",
                        "fair_yes_probability_conservative": "",
                        "fair_no_probability_conservative": "",
                        "confidence": "0.7",
                        "thesis": "Missing quote/fair coverage",
                        "source_note": "Synthetic test",
                        "updated_at": "2026-03-30T21:00:00+00:00",
                        "contract_family": "daily_rain",
                    },
                ],
            )

            summary = run_kalshi_micro_prior_plan(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                now=datetime(2026, 3, 30, 21, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["daily_weather_rows_total"], 2)
            self.assertEqual(summary["daily_weather_rows_with_conservative_candidate"], 1)
            self.assertEqual(summary["daily_weather_rows_with_both_sides_candidate"], 1)
            self.assertEqual(summary["daily_weather_rows_with_one_side_failed"], 0)
            self.assertEqual(summary["daily_weather_rows_with_both_sides_failed"], 1)
            self.assertEqual(summary["daily_weather_orderable_bid_rows"], 1)
            self.assertEqual(summary["daily_weather_rows_with_fair_probabilities"], 1)
            self.assertEqual(summary["daily_weather_rows_with_both_quote_and_fair_value"], 1)
            self.assertEqual(summary["daily_weather_allowed_universe_rows_with_conservative_candidate"], 0)
            self.assertEqual(
                summary["daily_weather_conservative_candidate_failure_counts"]["missing_yes_bid"],
                1,
            )
            self.assertEqual(
                summary["daily_weather_conservative_candidate_failure_counts"]["missing_no_bid"],
                1,
            )
            self.assertEqual(
                summary["daily_weather_conservative_candidate_failure_counts"]["missing_fair_yes_probability"],
                1,
            )

    def test_build_micro_prior_plans_skips_zero_cost_endpoint_quotes(self) -> None:
        enriched_rows = build_prior_rows(
            prior_rows=[
                {
                    "market_ticker": "KXTEST-EDGE",
                    "fair_yes_probability": "0.03",
                    "confidence": "0.57",
                    "thesis": "Test",
                    "source_note": "Note",
                    "updated_at": "2026-03-28T01:00:00+00:00",
                }
            ],
            latest_market_rows={
                "KXTEST-EDGE": {
                    "category": "Politics",
                    "market_title": "Endpoint Market",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": "94",
                    "yes_bid_dollars": "0.00",
                    "yes_ask_dollars": "0.02",
                }
            },
        )

        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=enriched_rows,
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=3,
            min_maker_edge=0.005,
            max_entry_price_dollars=0.99,
        )

        self.assertEqual(plans, [])
        self.assertEqual(skip_counts["maker_edge_below_min"], 1)

    def test_build_micro_prior_plans_filters_unhealthy_daily_weather_before_top_pick(self) -> None:
        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=[
                {
                    "market_ticker": "KXRAIN-UNHEALTHY",
                    "market_title": "Unhealthy Daily Rain",
                    "category": "Climate and Weather",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": 8.0,
                    "contract_family": "daily_rain",
                    "weather_station_history_live_ready": False,
                    "weather_station_history_live_ready_reason": "status_rate_limited",
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.38,
                    "latest_yes_ask_dollars": 0.39,
                    "latest_no_bid_dollars": 0.61,
                    "latest_no_ask_dollars": 0.62,
                    "fair_yes_probability": 0.50,
                    "fair_yes_probability_conservative": 0.50,
                    "fair_no_probability": 0.50,
                    "fair_no_probability_conservative": 0.50,
                    "confidence": 0.74,
                    "evidence_count": 5,
                    "thesis": "Would be top without weather-history filter.",
                },
                {
                    "market_ticker": "KXRAIN-HEALTHY",
                    "market_title": "Healthy Daily Rain",
                    "category": "Climate and Weather",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": 8.0,
                    "contract_family": "daily_rain",
                    "weather_station_history_live_ready": True,
                    "weather_station_history_live_ready_reason": "ready",
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.36,
                    "latest_yes_ask_dollars": 0.37,
                    "latest_no_bid_dollars": 0.63,
                    "latest_no_ask_dollars": 0.64,
                    "fair_yes_probability": 0.45,
                    "fair_yes_probability_conservative": 0.45,
                    "fair_no_probability": 0.55,
                    "fair_no_probability_conservative": 0.55,
                    "confidence": 0.72,
                    "evidence_count": 5,
                    "thesis": "Healthy backup should become top.",
                },
            ],
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=2,
            min_maker_edge=0.005,
            max_entry_price_dollars=0.99,
            require_weather_history_live_ready_for_daily_weather=True,
        )

        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["market_ticker"], "KXRAIN-HEALTHY")
        self.assertEqual(skip_counts["weather_history_unhealthy"], 1)

    def test_build_micro_prior_plans_ranks_by_maker_edge_not_taker_edge(self) -> None:
        plans, _ = build_micro_prior_plans(
            enriched_rows=[
                {
                    "market_ticker": "KXTAKER-FIRST",
                    "market_title": "Taker-first market",
                    "category": "Politics",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": 48.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.2,
                    "latest_no_bid_dollars": 0.8,
                    "best_maker_entry_side": "yes",
                    "best_maker_entry_edge": 0.01,
                    "best_maker_entry_price_dollars": 0.2,
                    "fair_yes_probability": 0.21,
                    "fair_yes_probability_conservative": 0.21,
                    "fair_no_probability": 0.79,
                    "fair_no_probability_conservative": 0.79,
                    "confidence": 0.6,
                    "thesis": "A",
                },
                {
                    "market_ticker": "KXMAKER-FIRST",
                    "market_title": "Maker-first market",
                    "category": "Politics",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": 48.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.12,
                    "latest_no_bid_dollars": 0.85,
                    "best_maker_entry_side": "no",
                    "best_maker_entry_edge": 0.03,
                    "best_maker_entry_price_dollars": 0.85,
                    "fair_yes_probability": 0.12,
                    "fair_yes_probability_conservative": 0.12,
                    "fair_no_probability": 0.88,
                    "fair_no_probability_conservative": 0.88,
                    "confidence": 0.6,
                    "thesis": "B",
                },
            ],
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=2,
            min_maker_edge=0.005,
            max_entry_price_dollars=0.99,
        )

        self.assertEqual(len(plans), 2)
        self.assertEqual(plans[0]["market_ticker"], "KXMAKER-FIRST")
        self.assertEqual(plans[0]["side"], "no")

    def test_build_micro_prior_plans_gates_on_conservative_probability(self) -> None:
        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=[
                {
                    "market_ticker": "KXCONS-1",
                    "market_title": "Conservative Gate",
                    "category": "Politics",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": 36.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.6,
                    "latest_no_bid_dollars": 0.4,
                    "fair_yes_probability": 0.62,
                    "fair_yes_probability_conservative": 0.58,
                    "fair_no_probability": 0.38,
                    "fair_no_probability_conservative": 0.35,
                    "confidence": 0.7,
                    "thesis": "Midpoint says edge, conservative says no edge.",
                }
            ],
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=2,
            min_maker_edge=0.01,
            max_entry_price_dollars=0.99,
        )

        self.assertEqual(plans, [])
        self.assertEqual(skip_counts["maker_edge_below_min"], 1)

    def test_build_micro_prior_plans_can_require_canonical_mapping(self) -> None:
        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=[
                {
                    "market_ticker": "KXUNMAPPED-1",
                    "market_title": "Unmapped Market",
                    "category": "Economics",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": 24.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.45,
                    "latest_yes_ask_dollars": 0.46,
                    "latest_no_bid_dollars": 0.54,
                    "latest_no_ask_dollars": 0.55,
                    "fair_yes_probability": 0.55,
                    "fair_yes_probability_conservative": 0.55,
                    "fair_no_probability": 0.45,
                    "fair_no_probability_conservative": 0.45,
                    "confidence": 0.7,
                    "evidence_count": 4,
                    "thesis": "Mapped gating test",
                }
            ],
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=2,
            min_maker_edge=0.005,
            max_entry_price_dollars=0.99,
            canonical_policy_by_live_ticker={},
            require_canonical_mapping=True,
        )

        self.assertEqual(plans, [])
        self.assertEqual(skip_counts["canonical_unmapped"], 1)

    def test_build_micro_prior_plans_applies_canonical_threshold_policy(self) -> None:
        canonical_policy = {
            "KXMAPPED-1": {
                "canonical_ticker": "MX01_CPI_HEADLINE_MOM",
                "niche": "macro_release",
                "release_cluster": "BLS_0830",
                "entry_min_edge_net": 0.02,
                "entry_min_confidence": 0.65,
                "entry_min_evidence_count": 3,
                "entry_max_price_dollars": 0.72,
                "entry_max_spread_dollars": 0.02,
                "per_market_risk_cap_fraction_nav": 0.02,
                "release_cluster_risk_cap_fraction_nav": 0.03,
                "same_day_correlated_risk_cap_fraction_nav": 0.04,
            }
        }
        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=[
                {
                    "market_ticker": "KXMAPPED-1",
                    "market_title": "Mapped Market",
                    "category": "Economics",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": 18.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.5,
                    "latest_yes_ask_dollars": 0.51,
                    "latest_no_bid_dollars": 0.49,
                    "latest_no_ask_dollars": 0.5,
                    "fair_yes_probability": 0.56,
                    "fair_yes_probability_conservative": 0.56,
                    "fair_no_probability": 0.44,
                    "fair_no_probability_conservative": 0.44,
                    "confidence": 0.72,
                    "evidence_count": 4,
                    "thesis": "Canonical policy test",
                }
            ],
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=2,
            min_maker_edge=0.005,
            min_maker_edge_net_fees=0.0,
            max_entry_price_dollars=0.99,
            canonical_policy_by_live_ticker=canonical_policy,
            require_canonical_mapping=True,
        )

        self.assertEqual(skip_counts["canonical_unmapped"], 0)
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["canonical_ticker"], "MX01_CPI_HEADLINE_MOM")
        self.assertTrue(plans[0]["canonical_policy_applied"])
        self.assertEqual(plans[0]["effective_max_entry_price_dollars"], 0.72)
        self.assertEqual(plans[0]["effective_min_evidence_count"], 3)

    def test_build_micro_prior_plans_blocks_disallowed_canonical_niche(self) -> None:
        canonical_policy = {
            "KXDISALLOWED-1": {
                "canonical_ticker": "PX01_POLITICS_TEST",
                "niche": "politics",
                "release_cluster": "POL_0000",
                "entry_min_edge_net": 0.01,
            }
        }
        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=[
                {
                    "market_ticker": "KXDISALLOWED-1",
                    "market_title": "Disallowed Niche",
                    "category": "Politics",
                    "close_time": "2026-04-01T03:59:00Z",
                    "hours_to_close": 18.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.40,
                    "latest_yes_ask_dollars": 0.41,
                    "latest_no_bid_dollars": 0.59,
                    "latest_no_ask_dollars": 0.60,
                    "fair_yes_probability": 0.52,
                    "fair_yes_probability_conservative": 0.52,
                    "fair_no_probability": 0.48,
                    "fair_no_probability_conservative": 0.48,
                    "confidence": 0.72,
                    "evidence_count": 4,
                    "thesis": "Should not pass live niche allowlist.",
                }
            ],
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=2,
            min_maker_edge=0.005,
            max_entry_price_dollars=0.99,
            canonical_policy_by_live_ticker=canonical_policy,
            require_canonical_mapping=True,
            allowed_canonical_niches={"macro_release", "weather_energy_transmission"},
        )

        self.assertEqual(plans, [])
        self.assertEqual(skip_counts["canonical_unmapped"], 0)
        self.assertEqual(skip_counts["canonical_niche_disallowed"], 1)

    def test_build_micro_prior_plans_can_filter_longdated_routine_markets(self) -> None:
        canonical_policy = {
            "KXMACRO-1": {
                "canonical_ticker": "MX01_TEST",
                "niche": "macro_release",
            },
            "KXPOL-1": {
                "canonical_ticker": "PX01_TEST",
                "niche": "politics",
            },
        }
        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=[
                {
                    "market_ticker": "KXMACRO-1",
                    "market_title": "Macro Market",
                    "category": "Economics",
                    "close_time": "2026-04-10T03:59:00Z",
                    "hours_to_close": 120.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.40,
                    "latest_yes_ask_dollars": 0.41,
                    "latest_no_bid_dollars": 0.59,
                    "latest_no_ask_dollars": 0.60,
                    "fair_yes_probability": 0.50,
                    "fair_yes_probability_conservative": 0.50,
                    "fair_no_probability": 0.50,
                    "fair_no_probability_conservative": 0.50,
                    "confidence": 0.72,
                    "evidence_count": 4,
                    "thesis": "Macro should pass longdated allowlist.",
                },
                {
                    "market_ticker": "KXPOL-1",
                    "market_title": "Politics Market",
                    "category": "Politics",
                    "close_time": "2026-04-10T03:59:00Z",
                    "hours_to_close": 120.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.40,
                    "latest_yes_ask_dollars": 0.41,
                    "latest_no_bid_dollars": 0.59,
                    "latest_no_ask_dollars": 0.60,
                    "fair_yes_probability": 0.50,
                    "fair_yes_probability_conservative": 0.50,
                    "fair_no_probability": 0.50,
                    "fair_no_probability_conservative": 0.50,
                    "confidence": 0.72,
                    "evidence_count": 4,
                    "thesis": "Politics should fail routine longdated filter.",
                },
            ],
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=2,
            min_maker_edge=0.005,
            min_entry_price_dollars=0.03,
            max_entry_price_dollars=0.95,
            routine_max_hours_to_close=72.0,
            routine_longdated_allowed_niches={"macro_release", "weather_energy_transmission"},
            canonical_policy_by_live_ticker=canonical_policy,
            require_canonical_mapping=True,
        )

        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["market_ticker"], "KXMACRO-1")
        self.assertEqual(skip_counts["routine_hours_to_close_above_max"], 1)

    def test_build_micro_prior_plans_can_apply_alias_mapping_keys(self) -> None:
        plans, skip_counts = build_micro_prior_plans(
            enriched_rows=[
                {
                    "market_ticker": "KXECONSTATCPIYOY-26JUN-T3.4",
                    "market_title": "CPI year-over-year in Jun 2026?",
                    "category": "Economics",
                    "close_time": "2026-07-10T12:29:00+00:00",
                    "hours_to_close": 300.0,
                    "matched_live_market": True,
                    "latest_yes_bid_dollars": 0.15,
                    "latest_yes_ask_dollars": 0.16,
                    "latest_no_bid_dollars": 0.84,
                    "latest_no_ask_dollars": 0.85,
                    "fair_yes_probability": 0.24,
                    "fair_yes_probability_conservative": 0.22,
                    "fair_no_probability": 0.76,
                    "fair_no_probability_conservative": 0.74,
                    "confidence": 0.72,
                    "evidence_count": 4,
                    "thesis": "Alias mapping should connect this month variant.",
                }
            ],
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=1,
            min_maker_edge=0.005,
            max_entry_price_dollars=0.99,
            canonical_policy_by_live_ticker={},
            canonical_policy_alias_by_lookup_key={
                "KXECONSTATCPIYOY": {
                    "canonical_ticker": "MX03_CPI_HEADLINE_YOY",
                    "niche": "macro_release",
                    "release_cluster": "BLS_0830",
                    "entry_min_edge_net": 0.012,
                    "entry_min_confidence": 0.58,
                    "entry_min_evidence_count": 3,
                }
            },
            require_canonical_mapping=True,
        )

        self.assertEqual(skip_counts["canonical_unmapped"], 0)
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["canonical_ticker"], "MX03_CPI_HEADLINE_YOY")
        self.assertEqual(plans[0]["canonical_mapping_match_type"], "alias_lookup_key")
        self.assertEqual(plans[0]["canonical_mapping_match_key"], "KXECONSTATCPIYOY")

    def test_run_kalshi_micro_prior_plan_reports_unmapped_scope_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            mapping_csv = base / "canonical_contract_mapping.csv"
            threshold_csv = base / "canonical_threshold_library.csv"

            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Economics",
                        "market_ticker": "KXECONSTATCPIYOY-26JUN-T3.3",
                        "market_title": "CPI year-over-year in Jun 2026?",
                        "close_time": "2026-07-10T12:29:00+00:00",
                        "hours_to_close": "300",
                        "yes_bid_dollars": "0.10",
                        "yes_ask_dollars": "0.11",
                    },
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Companies",
                        "market_ticker": "KXIPOALPHA-26APR30",
                        "market_title": "When will Alpha IPO?",
                        "close_time": "2026-05-01T03:59:00+00:00",
                        "hours_to_close": "120",
                        "yes_bid_dollars": "0.03",
                        "yes_ask_dollars": "0.04",
                    },
                ],
            )
            _write_csv(
                priors_csv,
                PRIOR_FIELDNAMES,
                [
                    {
                        "market_ticker": "KXECONSTATCPIYOY-26JUN-T3.3",
                        "fair_yes_probability": "0.20",
                        "confidence": "0.75",
                        "thesis": "Macro prior",
                        "source_note": "note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    },
                    {
                        "market_ticker": "KXIPOALPHA-26APR30",
                        "fair_yes_probability": "0.06",
                        "confidence": "0.65",
                        "thesis": "IPO prior",
                        "source_note": "note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    },
                ],
            )
            _write_csv(
                threshold_csv,
                [
                    "canonical_ticker",
                    "niche",
                    "execution_phase",
                    "release_cluster",
                    "entry_min_edge_net",
                    "entry_min_confidence",
                    "entry_min_evidence_count",
                    "entry_max_price_dollars",
                    "entry_max_spread_dollars",
                    "per_market_risk_cap_fraction_nav",
                    "release_cluster_risk_cap_fraction_nav",
                    "same_day_correlated_risk_cap_fraction_nav",
                    "notes",
                ],
                [
                    {
                        "canonical_ticker": "MX03_CPI_HEADLINE_YOY",
                        "niche": "macro_release",
                        "execution_phase": "phase1_live",
                        "release_cluster": "BLS_0830",
                        "entry_min_edge_net": "0.012",
                        "entry_min_confidence": "0.58",
                        "entry_min_evidence_count": "3",
                        "entry_max_price_dollars": "0.72",
                        "entry_max_spread_dollars": "0.05",
                        "per_market_risk_cap_fraction_nav": "0.003",
                        "release_cluster_risk_cap_fraction_nav": "0.0035",
                        "same_day_correlated_risk_cap_fraction_nav": "0.0075",
                        "notes": "macro test",
                    }
                ],
            )
            _write_csv(
                mapping_csv,
                [
                    "canonical_ticker",
                    "niche",
                    "execution_phase",
                    "market_description",
                    "settlement_source",
                    "settlement_source_url",
                    "release_time_et",
                    "schedule_source_url",
                    "schedule_needs_nightly_poll",
                    "schedule_holiday_shift_risk",
                    "source_timestamp_rule",
                    "mispricing_hypothesis",
                    "confounders",
                    "mapping_status",
                    "live_event_ticker",
                    "live_market_ticker",
                    "mapping_confidence",
                    "mapping_notes",
                    "last_mapped_at",
                ],
                [
                    {
                        "canonical_ticker": "MX03_CPI_HEADLINE_YOY",
                        "niche": "macro_release",
                        "execution_phase": "phase1_live",
                        "market_description": "macro test",
                        "settlement_source": "BLS",
                        "settlement_source_url": "https://www.bls.gov",
                        "release_time_et": "08:30",
                        "schedule_source_url": "https://www.bls.gov",
                        "schedule_needs_nightly_poll": "true",
                        "schedule_holiday_shift_risk": "true",
                        "source_timestamp_rule": "first official print",
                        "mispricing_hypothesis": "test",
                        "confounders": "test",
                        "mapping_status": "mapped",
                        "live_event_ticker": "KXECONSTATCPIYOY-26MAY",
                        "live_market_ticker": "KXECONSTATCPIYOY-26MAY-T3.3",
                        "mapping_confidence": "0.9",
                        "mapping_notes": "test",
                        "last_mapped_at": "2026-03-29T00:00:00+00:00",
                    }
                ],
            )

            summary = run_kalshi_micro_prior_plan(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                canonical_mapping_csv=str(mapping_csv),
                canonical_threshold_csv=str(threshold_csv),
                prefer_canonical_thresholds=True,
                require_canonical_mapping=True,
                allowed_canonical_niches=("macro_release", "weather_energy_transmission"),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 22, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["matched_live_markets"], 2)
            self.assertEqual(summary["matched_live_markets_with_canonical_policy"], 1)
            self.assertEqual(summary["canonical_unmapped_total"], 1)
            self.assertEqual(summary["canonical_unmapped_in_allowed_niche_guess"], 0)
            self.assertEqual(summary["canonical_unmapped_outside_allowed_niche_guess"], 1)
            self.assertEqual(summary["canonical_unmapped_counts_by_niche_guess"].get("companies_ipo"), 1)
            self.assertEqual(summary["skip_counts"]["canonical_unmapped"], 1)
            self.assertEqual(summary["skip_counts"]["canonical_unmapped_outside_allowed_niche_guess"], 1)
            self.assertEqual(summary["production_live_allowed_canonical_niches"], [
                "macro_release",
                "weather_climate",
                "weather_energy_transmission",
            ])
            self.assertEqual(summary["production_daily_weather_contract_families"], ["daily_rain", "daily_temperature"])
            self.assertEqual(summary["allowed_universe_candidate_pool_size"], 1)
            self.assertEqual(summary["daily_weather_candidate_pool_size"], 0)
            self.assertEqual(summary["daily_weather_rows_with_conservative_candidate"], 0)
            self.assertEqual(summary["daily_weather_planned_orders"], 0)
            self.assertEqual(summary["allowed_universe_skip_reason_dominant"], "canonical_evidence_below_min")
            self.assertEqual(summary["allowed_universe_skip_reason_dominant_count"], 1)
            self.assertEqual(summary["daily_weather_skip_counts_total"], 0)
            self.assertEqual(summary["daily_weather_skip_counts_nonzero_total"], 0)
            self.assertEqual(summary["daily_weather_skip_counts_top"], [])


if __name__ == "__main__":
    unittest.main()
