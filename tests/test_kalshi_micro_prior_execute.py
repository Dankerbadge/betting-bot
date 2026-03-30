import tempfile
from datetime import datetime, timezone
import csv
from pathlib import Path
import unittest
from unittest.mock import patch

from betbot.kalshi_micro_prior_execute import run_kalshi_micro_prior_execute


class KalshiMicroPriorExecuteTests(unittest.TestCase):
    def test_run_kalshi_micro_prior_execute_blocks_live_when_daily_weather_board_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            priors_csv = base / "priors.csv"
            history_csv = base / "history.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with priors_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["market_ticker", "fair_yes_probability", "confidence", "thesis", "source_note", "updated_at"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXMONTHLY-1",
                        "fair_yes_probability": "0.62",
                        "confidence": "0.85",
                        "thesis": "Monthly anomaly edge",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                )
            with history_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["captured_at", "category", "market_ticker", "market_title", "yes_bid_dollars", "yes_ask_dollars"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Weather",
                        "market_ticker": "KXMONTHLY-1",
                        "market_title": "Monthly climate anomaly in 2026",
                        "yes_bid_dollars": "0.49",
                        "yes_ask_dollars": "0.50",
                    }
                )

            plan_summary = {
                "status": "ready",
                "planned_orders": 1,
                "positive_maker_entry_markets": 1,
                "positive_maker_entry_markets_with_canonical_policy": 1,
                "top_market_ticker": "KXMONTHLY-1",
                "top_market_title": "Monthly climate anomaly in 2026",
                "top_market_side": "yes",
                "top_market_canonical_ticker": "WX-MONTHLY",
                "top_market_canonical_niche": "weather_climate",
                "top_market_canonical_policy_applied": True,
                "top_market_maker_entry_price_dollars": 0.50,
                "top_market_maker_entry_edge": 0.02,
                "top_market_maker_entry_edge_net_fees": 0.015,
                "top_market_estimated_entry_cost_dollars": 0.50,
                "top_market_estimated_entry_fee_dollars": 0.0,
                "top_market_expected_value_dollars": 0.02,
                "top_market_expected_value_net_dollars": 0.015,
                "top_market_expected_roi_on_cost": 0.04,
                "top_market_expected_roi_on_cost_net": 0.03,
                "top_market_expected_value_per_day_dollars": 0.03,
                "top_market_expected_value_per_day_net_dollars": 0.02,
                "top_market_expected_roi_per_day": 0.05,
                "top_market_expected_roi_per_day_net": 0.04,
                "top_market_estimated_max_profit_dollars": 0.5,
                "top_market_estimated_max_loss_dollars": 0.5,
                "top_market_max_profit_roi_on_cost": 1.0,
                "top_market_fair_probability": 0.62,
                "top_market_confidence": 0.85,
                "top_market_thesis": "Monthly anomaly edge",
                "actual_live_balance_dollars": 100.0,
                "funding_gap_dollars": 0.0,
                "output_file": str(base / "plan.json"),
            }
            execute_summary = {
                "status": "dry_run",
                "planned_orders": 1,
                "total_planned_cost_dollars": 0.50,
                "actual_live_balance_dollars": 100.0,
                "actual_live_balance_source": "live",
                "balance_live_verified": True,
                "output_file": str(base / "execute.json"),
                "output_csv": str(base / "execute.csv"),
                "attempts": [],
            }

            with (
                patch("betbot.kalshi_micro_prior_execute.run_kalshi_micro_prior_plan", return_value=plan_summary),
                patch("betbot.kalshi_micro_prior_execute.run_kalshi_micro_execute", return_value=execute_summary),
            ):
                summary = run_kalshi_micro_prior_execute(
                    env_file=str(env_file),
                    priors_csv=str(priors_csv),
                    history_csv=str(history_csv),
                    output_dir=str(base),
                    allow_live_orders=True,
                    enforce_canonical_dataset=False,
                    enforce_daily_weather_live_only=True,
                    require_daily_weather_board_coverage_for_live=True,
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "blocked_prior_trade_gate")
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertEqual(summary["prior_trade_gate_summary"]["gate_status"], "daily_weather_board_missing")
            self.assertEqual(summary["weather_board_summary"]["daily_weather_markets_total"], 0)
            self.assertIn(
                "No daily weather markets are present in the captured board snapshot",
                " ".join(summary["prior_trade_gate_summary"]["gate_blockers"]),
            )

    def test_run_kalshi_micro_prior_execute_blocks_live_when_top_contract_is_not_daily_weather(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            priors_csv = base / "priors.csv"
            history_csv = base / "history.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with priors_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["market_ticker", "fair_yes_probability", "confidence", "thesis", "source_note", "updated_at"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXMONTHLY-1",
                        "fair_yes_probability": "0.62",
                        "confidence": "0.85",
                        "thesis": "Monthly anomaly edge",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                )
            with history_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["captured_at", "category", "market_ticker", "market_title", "yes_bid_dollars", "yes_ask_dollars"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Weather",
                        "market_ticker": "KXMONTHLY-1",
                        "market_title": "Monthly climate anomaly in 2026",
                        "yes_bid_dollars": "0.49",
                        "yes_ask_dollars": "0.50",
                    }
                )
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Weather",
                        "market_ticker": "KXRAIN-1",
                        "market_title": "Will it rain in New York tomorrow?",
                        "yes_bid_dollars": "0.41",
                        "yes_ask_dollars": "0.42",
                    }
                )

            plan_summary = {
                "status": "ready",
                "planned_orders": 1,
                "positive_maker_entry_markets": 1,
                "positive_maker_entry_markets_with_canonical_policy": 1,
                "top_market_ticker": "KXMONTHLY-1",
                "top_market_title": "Monthly climate anomaly in 2026",
                "top_market_side": "yes",
                "top_market_canonical_ticker": "WX-MONTHLY",
                "top_market_canonical_niche": "weather_climate",
                "top_market_canonical_policy_applied": True,
                "top_market_maker_entry_price_dollars": 0.50,
                "top_market_maker_entry_edge": 0.02,
                "top_market_maker_entry_edge_net_fees": 0.015,
                "top_market_estimated_entry_cost_dollars": 0.50,
                "top_market_estimated_entry_fee_dollars": 0.0,
                "top_market_expected_value_dollars": 0.02,
                "top_market_expected_value_net_dollars": 0.015,
                "top_market_expected_roi_on_cost": 0.04,
                "top_market_expected_roi_on_cost_net": 0.03,
                "top_market_expected_value_per_day_dollars": 0.03,
                "top_market_expected_value_per_day_net_dollars": 0.02,
                "top_market_expected_roi_per_day": 0.05,
                "top_market_expected_roi_per_day_net": 0.04,
                "top_market_estimated_max_profit_dollars": 0.5,
                "top_market_estimated_max_loss_dollars": 0.5,
                "top_market_max_profit_roi_on_cost": 1.0,
                "top_market_fair_probability": 0.62,
                "top_market_confidence": 0.85,
                "top_market_thesis": "Monthly anomaly edge",
                "actual_live_balance_dollars": 100.0,
                "funding_gap_dollars": 0.0,
                "output_file": str(base / "plan.json"),
            }
            execute_summary = {
                "status": "dry_run",
                "planned_orders": 1,
                "total_planned_cost_dollars": 0.50,
                "actual_live_balance_dollars": 100.0,
                "actual_live_balance_source": "live",
                "balance_live_verified": True,
                "output_file": str(base / "execute.json"),
                "output_csv": str(base / "execute.csv"),
                "attempts": [],
            }

            with (
                patch("betbot.kalshi_micro_prior_execute.run_kalshi_micro_prior_plan", return_value=plan_summary),
                patch("betbot.kalshi_micro_prior_execute.run_kalshi_micro_execute", return_value=execute_summary),
            ):
                summary = run_kalshi_micro_prior_execute(
                    env_file=str(env_file),
                    priors_csv=str(priors_csv),
                    history_csv=str(history_csv),
                    output_dir=str(base),
                    allow_live_orders=True,
                    enforce_canonical_dataset=False,
                    enforce_daily_weather_live_only=True,
                    require_daily_weather_board_coverage_for_live=True,
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "blocked_prior_trade_gate")
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertGreater(summary["weather_board_summary"]["daily_weather_markets_total"], 0)
            self.assertEqual(summary["prior_trade_gate_summary"]["gate_status"], "daily_weather_only")
            self.assertIn(
                "Daily-weather-only live mode is enabled",
                " ".join(summary["prior_trade_gate_summary"]["gate_blockers"]),
            )

    def test_run_kalshi_micro_prior_execute_blocks_live_when_weather_history_unhealthy(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            priors_csv = base / "priors.csv"
            history_csv = base / "history.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with priors_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["market_ticker", "fair_yes_probability", "confidence", "thesis", "source_note", "updated_at"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXRAIN-1",
                        "fair_yes_probability": "0.62",
                        "confidence": "0.85",
                        "thesis": "Daily rain edge",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                )
            with history_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["captured_at", "category", "market_ticker", "market_title", "yes_bid_dollars", "yes_ask_dollars"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Weather",
                        "market_ticker": "KXRAIN-1",
                        "market_title": "Will it rain in New York tomorrow?",
                        "yes_bid_dollars": "0.49",
                        "yes_ask_dollars": "0.50",
                    }
                )

            plan_summary = {
                "status": "ready",
                "planned_orders": 1,
                "positive_maker_entry_markets": 1,
                "positive_maker_entry_markets_with_canonical_policy": 1,
                "top_market_ticker": "KXRAIN-1",
                "top_market_title": "Will it rain in New York tomorrow?",
                "top_market_side": "yes",
                "top_market_canonical_ticker": "WX-DAILY-RAIN",
                "top_market_canonical_niche": "weather_climate",
                "top_market_canonical_policy_applied": True,
                "top_market_maker_entry_price_dollars": 0.50,
                "top_market_maker_entry_edge": 0.02,
                "top_market_maker_entry_edge_net_fees": 0.015,
                "top_market_estimated_entry_cost_dollars": 0.50,
                "top_market_estimated_entry_fee_dollars": 0.0,
                "top_market_expected_value_dollars": 0.02,
                "top_market_expected_value_net_dollars": 0.015,
                "top_market_expected_roi_on_cost": 0.04,
                "top_market_expected_roi_on_cost_net": 0.03,
                "top_market_expected_value_per_day_dollars": 0.03,
                "top_market_expected_value_per_day_net_dollars": 0.02,
                "top_market_expected_roi_per_day": 0.05,
                "top_market_expected_roi_per_day_net": 0.04,
                "top_market_estimated_max_profit_dollars": 0.5,
                "top_market_estimated_max_loss_dollars": 0.5,
                "top_market_max_profit_roi_on_cost": 1.0,
                "top_market_fair_probability": 0.62,
                "top_market_confidence": 0.85,
                "top_market_thesis": "Daily rain edge",
                "top_market_contract_family": "daily_rain",
                "top_market_weather_station_history_status": "rate_limited",
                "top_market_weather_station_history_live_ready": False,
                "top_market_weather_station_history_live_ready_reason": "status_rate_limited",
                "actual_live_balance_dollars": 100.0,
                "funding_gap_dollars": 0.0,
                "output_file": str(base / "plan.json"),
            }
            execute_summary = {
                "status": "dry_run",
                "planned_orders": 1,
                "total_planned_cost_dollars": 0.50,
                "actual_live_balance_dollars": 100.0,
                "actual_live_balance_source": "live",
                "balance_live_verified": True,
                "output_file": str(base / "execute.json"),
                "output_csv": str(base / "execute.csv"),
                "attempts": [],
            }

            with (
                patch("betbot.kalshi_micro_prior_execute.run_kalshi_micro_prior_plan", return_value=plan_summary),
                patch("betbot.kalshi_micro_prior_execute.run_kalshi_micro_execute", return_value=execute_summary),
            ):
                summary = run_kalshi_micro_prior_execute(
                    env_file=str(env_file),
                    priors_csv=str(priors_csv),
                    history_csv=str(history_csv),
                    output_dir=str(base),
                    allow_live_orders=True,
                    enforce_canonical_dataset=False,
                    enforce_daily_weather_live_only=True,
                    require_daily_weather_board_coverage_for_live=True,
                    now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
                )

            self.assertEqual(summary["status"], "blocked_prior_trade_gate")
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertEqual(summary["prior_trade_gate_summary"]["gate_status"], "weather_history_unhealthy")
            self.assertIn(
                "station-history readiness",
                " ".join(summary["prior_trade_gate_summary"]["gate_blockers"]),
            )
            self.assertEqual(summary["prior_trade_gate_summary"]["top_market_weather_history_status"], "rate_limited")
            self.assertFalse(summary["prior_trade_gate_summary"]["top_market_weather_history_live_ready"])
            self.assertEqual(
                summary["prior_trade_gate_summary"]["top_market_weather_history_live_ready_reason"],
                "status_rate_limited",
            )

    def test_run_kalshi_micro_prior_execute_default_enforces_canonical_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            priors_csv = base / "priors.csv"
            history_csv = base / "history.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with priors_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["market_ticker", "fair_yes_probability", "confidence", "thesis", "source_note", "updated_at"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXUNMAPPED-ONLY",
                        "fair_yes_probability": "0.60",
                        "confidence": "0.8",
                        "thesis": "Unmapped high-edge market",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                )
            with history_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["captured_at", "category", "market_ticker", "market_title", "yes_bid_dollars", "yes_ask_dollars"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXUNMAPPED-ONLY",
                        "market_title": "Unmapped Market",
                        "yes_bid_dollars": "0.40",
                        "yes_ask_dollars": "0.41",
                    }
                )

            summary = run_kalshi_micro_prior_execute(
                env_file=str(env_file),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                http_get_json=lambda *args, **kwargs: (200, {"balance_cents": 4000}),
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "no_candidates")
            self.assertEqual(summary["planned_orders"], 0)
            self.assertTrue(summary["enforce_canonical_dataset_effective"])
            self.assertEqual(summary["prior_trade_gate_summary"]["planned_orders"], 0)

    def test_run_kalshi_micro_prior_execute_dry_run_supports_no_side(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            priors_csv = base / "priors.csv"
            history_csv = base / "history.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with priors_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["market_ticker", "fair_yes_probability", "confidence", "thesis", "source_note", "updated_at"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXTEST-1",
                        "fair_yes_probability": "0.02",
                        "confidence": "0.7",
                        "thesis": "Test",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                )
            with history_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["captured_at", "category", "market_ticker", "market_title", "yes_bid_dollars", "yes_ask_dollars"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "market_title": "Test Market",
                        "yes_bid_dollars": "0.03",
                        "yes_ask_dollars": "0.04",
                    }
                )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.0300", "50.00"]],
                        "no_dollars": [["0.9600", "10.00"]],
                    }
                }

            summary = run_kalshi_micro_prior_execute(
                env_file=str(env_file),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                enforce_canonical_dataset=False,
                http_request_json=fake_http_request_json,
                http_get_json=lambda *args, **kwargs: (200, {"balance_cents": 4000}),
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "dry_run")
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertEqual(summary["attempts"][0]["planned_side"], "no")
            self.assertIn("janitor_attempts", summary)
            self.assertIn("janitor_canceled_open_orders_count", summary)
            self.assertIn("janitor_cancel_failed_attempts", summary)
            self.assertIn("execution_frontier_status", summary)
            self.assertIn("execution_frontier_summary_file", summary)
            self.assertTrue(Path(summary["execution_frontier_summary_file"]).exists())
            self.assertTrue(Path(summary["execute_output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_prior_execute_blocks_live_when_edge_too_small(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            priors_csv = base / "priors.csv"
            history_csv = base / "history.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with priors_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["market_ticker", "fair_yes_probability", "confidence", "thesis", "source_note", "updated_at"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXTEST-1",
                        "fair_yes_probability": "0.01",
                        "confidence": "0.7",
                        "thesis": "Test",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                )
            with history_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["captured_at", "category", "market_ticker", "market_title", "yes_bid_dollars", "yes_ask_dollars"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-1",
                        "market_title": "Test Market",
                        "yes_bid_dollars": "0.01",
                        "yes_ask_dollars": "0.02",
                    }
                )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.0100", "50.00"]],
                        "no_dollars": [["0.9800", "10.00"]],
                    }
                }

            summary = run_kalshi_micro_prior_execute(
                env_file=str(env_file),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                allow_live_orders=True,
                enforce_canonical_dataset=False,
                http_request_json=fake_http_request_json,
                http_get_json=lambda *args, **kwargs: (200, {"balance_cents": 4000}),
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_prior_trade_gate")
            self.assertFalse(summary["prior_trade_gate_summary"]["gate_pass"])
            self.assertFalse(summary["allow_live_orders_effective"])

    def test_run_kalshi_micro_prior_execute_requires_canonical_mapping_for_live(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            priors_csv = base / "priors.csv"
            history_csv = base / "history.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with priors_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["market_ticker", "fair_yes_probability", "confidence", "thesis", "source_note", "updated_at"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXTEST-HIGHEDGE",
                        "fair_yes_probability": "0.60",
                        "confidence": "0.8",
                        "thesis": "Would pass edge but is unmapped.",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                )
            with history_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["captured_at", "category", "market_ticker", "market_title", "yes_bid_dollars", "yes_ask_dollars"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Economics",
                        "market_ticker": "KXTEST-HIGHEDGE",
                        "market_title": "High Edge Test",
                        "yes_bid_dollars": "0.50",
                        "yes_ask_dollars": "0.51",
                    }
                )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.5000", "50.00"]],
                        "no_dollars": [["0.4900", "10.00"]],
                    }
                }

            summary = run_kalshi_micro_prior_execute(
                env_file=str(env_file),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                allow_live_orders=True,
                http_request_json=fake_http_request_json,
                http_get_json=lambda *args, **kwargs: (200, {"balance_cents": 4000}),
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_prior_trade_gate")
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertFalse(summary["prior_trade_gate_summary"]["gate_pass"])
            self.assertIn(
                "No prior-backed maker plans are available.",
                summary["prior_trade_gate_summary"]["gate_blockers"],
            )

    def test_run_kalshi_micro_prior_execute_live_overrides_permissive_canonical_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            priors_csv = base / "priors.csv"
            history_csv = base / "history.csv"
            env_file.write_text(
                (
                    "KALSHI_ENV=prod\n"
                    "BETBOT_JURISDICTION=new_jersey\n"
                    "BETBOT_ENABLE_LIVE_ORDERS=1\n"
                    "KALSHI_ACCESS_KEY_ID=key123\n"
                    "KALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n"
                ),
                encoding="utf-8",
            )
            with priors_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["market_ticker", "fair_yes_probability", "confidence", "thesis", "source_note", "updated_at"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXTEST-HIGHEDGE",
                        "fair_yes_probability": "0.60",
                        "confidence": "0.8",
                        "thesis": "Would pass edge but should fail canonical enforcement.",
                        "source_note": "Note",
                        "updated_at": "2026-03-27T21:00:00+00:00",
                    }
                )
            with history_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["captured_at", "category", "market_ticker", "market_title", "yes_bid_dollars", "yes_ask_dollars"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": "2026-03-27T21:00:00+00:00",
                        "category": "Politics",
                        "market_ticker": "KXTEST-HIGHEDGE",
                        "market_title": "High Edge Test",
                        "yes_bid_dollars": "0.50",
                        "yes_ask_dollars": "0.51",
                    }
                )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                return 200, {
                    "orderbook_fp": {
                        "yes_dollars": [["0.5000", "50.00"]],
                        "no_dollars": [["0.4900", "10.00"]],
                    }
                }

            summary = run_kalshi_micro_prior_execute(
                env_file=str(env_file),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                allow_live_orders=True,
                prefer_canonical_thresholds=False,
                require_canonical_mapping_for_live=False,
                http_request_json=fake_http_request_json,
                http_get_json=lambda *args, **kwargs: (200, {"balance_cents": 4000}),
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "blocked_prior_trade_gate")
            self.assertFalse(summary["allow_live_orders_effective"])
            self.assertTrue(summary["prefer_canonical_thresholds_effective"])
            self.assertTrue(summary["require_canonical_mapping_for_live_effective"])
            self.assertEqual(
                summary["allowed_live_canonical_niches"],
                ["macro_release", "weather_energy_transmission", "weather_climate"],
            )
            self.assertIn(
                "No prior-backed maker plans are available.",
                summary["prior_trade_gate_summary"]["gate_blockers"],
            )


if __name__ == "__main__":
    unittest.main()
