import tempfile
from datetime import datetime, timezone
from pathlib import Path
import unittest

from betbot.kalshi_micro_plan import build_micro_order_plans, run_kalshi_micro_plan


class KalshiMicroPlanTests(unittest.TestCase):
    def test_build_micro_order_plans_prefers_two_sided_markets(self) -> None:
        ranked_rows = [
            {
                "category": "Economics",
                "market_ticker": "KXONE",
                "market_title": "Market one",
                "event_title": "Event one",
                "yes_bid_dollars": 0.02,
                "yes_ask_dollars": 0.03,
                "spread_dollars": 0.01,
                "hours_to_close": 48.0,
                "yes_bid_size_contracts": 100.0,
                "two_sided_book": True,
                "execution_fit_score": 9.0,
            },
            {
                "category": "Economics",
                "market_ticker": "KXTWO",
                "market_title": "Market two",
                "event_title": "Event two",
                "yes_bid_dollars": 0.01,
                "yes_ask_dollars": 0.02,
                "spread_dollars": 0.01,
                "hours_to_close": 72.0,
                "yes_bid_size_contracts": 50.0,
                "two_sided_book": False,
                "execution_fit_score": 8.0,
            },
        ]

        plans, skip_counts = build_micro_order_plans(
            ranked_rows=ranked_rows,
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=3,
            min_yes_bid_dollars=0.01,
            max_yes_ask_dollars=0.10,
            max_spread_dollars=0.02,
            require_two_sided_book=True,
        )

        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["market_ticker"], "KXONE")
        self.assertEqual(plans[0]["order_payload_preview"]["post_only"], True)
        self.assertEqual(plans[0]["order_payload_preview"]["self_trade_prevention_type"], "maker")
        self.assertEqual(skip_counts["not_two_sided"], 1)

    def test_run_kalshi_micro_plan_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "events": [
                        {
                            "category": "Economics",
                            "event_ticker": "KXECON-1",
                            "series_ticker": "KXECON",
                            "title": "Will CPI beat expectations?",
                            "sub_title": "March release",
                            "markets": [
                                {
                                    "ticker": "KXECON-1-Y",
                                    "title": "Will CPI beat expectations?",
                                    "yes_sub_title": "Yes",
                                    "status": "active",
                                    "close_time": "2026-03-28T12:00:00Z",
                                    "yes_ask_dollars": "0.03",
                                    "yes_bid_dollars": "0.02",
                                    "yes_ask_size_fp": "50.00",
                                    "yes_bid_size_fp": "20.00",
                                    "no_ask_dollars": "0.98",
                                    "no_bid_dollars": "0.97",
                                    "liquidity_dollars": "500.00",
                                    "volume_fp": "900.00",
                                    "volume_24h_fp": "120.00",
                                    "open_interest_fp": "700.00",
                                    "rules_primary": "Resolves to official CPI print.",
                                }
                            ],
                        }
                    ]
                }

            summary = run_kalshi_micro_plan(
                env_file=str(env_file),
                output_dir=str(base),
                planning_bankroll_dollars=40.0,
                daily_risk_cap_dollars=3.0,
                max_orders=3,
                http_get_json=fake_http_get_json,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["planned_orders"], 1)
            self.assertEqual(summary["total_planned_cost_dollars"], 0.02)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_plan_handles_rate_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 429, {"error": "rate limited"}

            summary = run_kalshi_micro_plan(
                env_file=str(env_file),
                output_dir=str(base),
                http_get_json=fake_http_get_json,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "rate_limited")
            self.assertEqual(summary["planned_orders"], 0)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_build_micro_order_plans_parses_two_sided_book_from_csv_text(self) -> None:
        ranked_rows = [
            {
                "category": "Economics",
                "market_ticker": "KXONE",
                "market_title": "Market one",
                "event_title": "Event one",
                "yes_bid_dollars": "0.02",
                "yes_ask_dollars": "0.03",
                "spread_dollars": "0.01",
                "hours_to_close": "48.0",
                "yes_bid_size_contracts": "100.0",
                "two_sided_book": "False",
                "execution_fit_score": "9.0",
            }
        ]

        plans, skip_counts = build_micro_order_plans(
            ranked_rows=ranked_rows,
            planning_bankroll_dollars=40.0,
            daily_risk_cap_dollars=3.0,
            contracts_per_order=1,
            max_orders=3,
            min_yes_bid_dollars=0.01,
            max_yes_ask_dollars=0.10,
            max_spread_dollars=0.02,
            require_two_sided_book=True,
        )

        self.assertEqual(plans, [])
        self.assertEqual(skip_counts["not_two_sided"], 1)

    def test_run_kalshi_micro_plan_can_reuse_scan_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            scan_csv = base / "scan.csv"
            scan_csv.write_text(
                (
                    "category,series_ticker,event_ticker,market_ticker,event_title,event_sub_title,market_title,yes_sub_title,close_time,hours_to_close,yes_bid_dollars,yes_bid_size_contracts,yes_ask_dollars,yes_ask_size_contracts,no_bid_dollars,no_ask_dollars,last_price_dollars,spread_dollars,liquidity_dollars,volume_contracts,volume_24h_contracts,open_interest_contracts,contracts_for_ten_dollars,ten_dollar_cost,ten_dollar_fillable_at_best_ask,two_sided_book,execution_fit_score,rules_primary\n"
                    "Economics,KXECON,KXECON-1,KXECON-1-Y,Event one,Sub,Market one,Yes,2026-03-28T12:00:00+00:00,12,0.02,20,0.03,50,0.97,0.98,0.025,0.01,500,900,120,700,333,9.99,True,True,9.5,Rules\n"
                ),
                encoding="utf-8",
            )

            summary = run_kalshi_micro_plan(
                env_file=str(env_file),
                output_dir=str(base),
                scan_csv=str(scan_csv),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["data_source"], "scan_csv")
            self.assertEqual(summary["planned_orders"], 1)
            self.assertEqual(summary["events_fetched"], 0)

    def test_run_kalshi_micro_plan_uses_recent_cached_balance_when_live_check_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            cache_file = base / "balance_cache.json"
            cache_file.write_text(
                (
                    "{\n"
                    '  "balance_cents": 4000,\n'
                    '  "captured_at": "2026-03-27T20:00:00+00:00",\n'
                    '  "kalshi_env": "prod"\n'
                    "}\n"
                ),
                encoding="utf-8",
            )

            def fake_http_get_json(
                url: str,
                headers: dict[str, str],
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "events": [
                        {
                            "category": "Economics",
                            "event_ticker": "KXECON-1",
                            "series_ticker": "KXECON",
                            "title": "Will CPI beat expectations?",
                            "sub_title": "March release",
                            "markets": [
                                {
                                    "ticker": "KXECON-1-Y",
                                    "title": "Will CPI beat expectations?",
                                    "yes_sub_title": "Yes",
                                    "status": "active",
                                    "close_time": "2026-03-28T12:00:00Z",
                                    "yes_ask_dollars": "0.03",
                                    "yes_bid_dollars": "0.02",
                                    "yes_ask_size_fp": "50.00",
                                    "yes_bid_size_fp": "20.00",
                                    "no_ask_dollars": "0.98",
                                    "no_bid_dollars": "0.97",
                                    "liquidity_dollars": "500.00",
                                    "volume_fp": "900.00",
                                    "volume_24h_fp": "120.00",
                                    "open_interest_fp": "700.00",
                                    "rules_primary": "Resolves to official CPI print.",
                                }
                            ],
                        }
                    ]
                }

            def failing_balance_fetcher(*args: object, **kwargs: object) -> dict[str, object]:
                raise OSError("temporary dns failure")

            summary = run_kalshi_micro_plan(
                env_file=str(env_file),
                output_dir=str(base),
                balance_cache_file=str(cache_file),
                balance_fetcher=failing_balance_fetcher,
                http_get_json=fake_http_get_json,
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["actual_live_balance_dollars"], 40.0)
            self.assertEqual(summary["actual_live_balance_source"], "cache")
            self.assertFalse(summary["balance_live_verified"])
            self.assertIsNotNone(summary["balance_cache_age_seconds"])
            self.assertEqual(summary["balance_check_error"], "temporary dns failure")


if __name__ == "__main__":
    unittest.main()
