from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from betbot.kalshi_climate_availability import run_kalshi_climate_realtime_router


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class KalshiClimateAvailabilityTests(unittest.TestCase):
    def test_run_kalshi_climate_realtime_router_records_wakeup_and_routes_tradable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            output_dir = base / "outputs"
            output_dir.mkdir(parents=True, exist_ok=True)

            history_csv = output_dir / "history.csv"
            priors_csv = base / "priors.csv"

            _write_csv(
                history_csv,
                [
                    "captured_at",
                    "category",
                    "market_ticker",
                    "market_title",
                    "close_time",
                    "hours_to_close",
                    "yes_bid_dollars",
                    "yes_bid_size_contracts",
                    "yes_ask_dollars",
                    "yes_ask_size_contracts",
                    "no_bid_dollars",
                    "no_ask_dollars",
                    "spread_dollars",
                    "two_sided_book",
                    "ten_dollar_fillable_at_best_ask",
                ],
                [
                    {
                        "captured_at": "2026-04-01T00:00:00+00:00",
                        "category": "Climate and Weather",
                        "market_ticker": "KXRAINTEST-26APR01",
                        "market_title": "Will it rain in NYC on Apr 1?",
                        "close_time": "2026-04-02T00:00:00Z",
                        "hours_to_close": "24",
                        "yes_bid_dollars": "0.45",
                        "yes_bid_size_contracts": "20",
                        "yes_ask_dollars": "0.47",
                        "yes_ask_size_contracts": "20",
                        "no_bid_dollars": "0.53",
                        "no_ask_dollars": "0.55",
                        "spread_dollars": "0.02",
                        "two_sided_book": "True",
                        "ten_dollar_fillable_at_best_ask": "True",
                    }
                ],
            )

            _write_csv(
                priors_csv,
                [
                    "market_ticker",
                    "fair_yes_probability",
                    "confidence",
                    "evidence_count",
                    "source_type",
                    "thesis",
                    "source_note",
                    "updated_at",
                    "contract_family",
                ],
                [
                    {
                        "market_ticker": "KXRAINTEST-26APR01",
                        "fair_yes_probability": "0.70",
                        "confidence": "0.8",
                        "evidence_count": "5",
                        "source_type": "weather_model",
                        "thesis": "Synthetic test",
                        "source_note": "Synthetic test note",
                        "updated_at": "2026-04-01T00:00:00+00:00",
                        "contract_family": "daily_rain",
                    }
                ],
            )

            events_path = output_dir / "ws_events_test.ndjson"
            events = [
                {
                    "event_type": "orderbook_snapshot",
                    "market_ticker": "KXRAINTEST-26APR01",
                    "yes_dollars_fp": [["0.0000", "100.0"]],
                    "no_dollars_fp": [["1.0000", "100.0"]],
                    "captured_at_utc": "2026-04-01T00:00:00+00:00",
                },
                {
                    "event_type": "orderbook_snapshot",
                    "market_ticker": "KXRAINTEST-26APR01",
                    "yes_dollars_fp": [["0.4800", "120.0"]],
                    "no_dollars_fp": [["0.5200", "110.0"]],
                    "captured_at_utc": "2026-04-01T00:00:10+00:00",
                },
                {
                    "event_type": "public_trades",
                    "market_ticker": "KXRAINTEST-26APR01",
                    "count": "3",
                    "captured_at_utc": "2026-04-01T00:00:20+00:00",
                },
            ]
            events_path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")

            def _fake_collect_runner(**_: object) -> dict[str, object]:
                return {
                    "status": "ready",
                    "events_logged": len(events),
                    "ws_events_ndjson": str(events_path),
                    "ws_state_json": str(output_dir / "kalshi_ws_state_latest.json"),
                    "output_file": str(output_dir / "kalshi_ws_state_collect_summary_test.json"),
                    "market_count": 1,
                    "gate_pass": True,
                    "last_error_kind": "",
                }

            summary = run_kalshi_climate_realtime_router(
                env_file=str(base / "fake.env"),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(output_dir),
                ws_collect_runner=_fake_collect_runner,
                run_seconds=5.0,
                max_markets=10,
                now=datetime(2026, 4, 1, 0, 1, tzinfo=timezone.utc),
            )

            self.assertIn(summary["status"], {"ready", "degraded_realtime_unavailable"})
            self.assertEqual(summary["market_tickers_selected_count"], 1)
            self.assertGreaterEqual(int(summary["wakeup_transitions_processed"]), 1)
            self.assertGreaterEqual(int(summary["availability_observations_written"]), 2)
            self.assertGreaterEqual(int(summary["availability_ticker_states_updated"]), 1)
            self.assertEqual(summary["climate_rows_total"], 1)
            self.assertEqual(summary["climate_family_counts"].get("daily_rain"), 1)
            self.assertTrue(Path(summary["availability_db_path"]).exists())
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_climate_realtime_router_skip_collect_supports_unpriced_model_view(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            output_dir = base / "outputs"
            output_dir.mkdir(parents=True, exist_ok=True)

            history_csv = output_dir / "history.csv"
            priors_csv = base / "priors.csv"

            _write_csv(
                history_csv,
                [
                    "captured_at",
                    "category",
                    "market_ticker",
                    "market_title",
                    "close_time",
                    "hours_to_close",
                    "yes_bid_dollars",
                    "yes_ask_dollars",
                    "no_bid_dollars",
                    "no_ask_dollars",
                ],
                [
                    {
                        "captured_at": "2026-04-01T00:00:00+00:00",
                        "category": "Climate and Weather",
                        "market_ticker": "KXHMONTHTEST-26APR",
                        "market_title": "Monthly anomaly synthetic",
                        "close_time": "2026-05-01T00:00:00Z",
                        "hours_to_close": "720",
                        "yes_bid_dollars": "0",
                        "yes_ask_dollars": "1",
                        "no_bid_dollars": "1",
                        "no_ask_dollars": "1",
                    }
                ],
            )

            _write_csv(
                priors_csv,
                [
                    "market_ticker",
                    "fair_yes_probability",
                    "confidence",
                    "source_type",
                    "thesis",
                    "source_note",
                    "updated_at",
                    "contract_family",
                ],
                [
                    {
                        "market_ticker": "KXHMONTHTEST-26APR",
                        "fair_yes_probability": "0.65",
                        "confidence": "0.8",
                        "source_type": "weather_model",
                        "thesis": "Synthetic monthly",
                        "source_note": "Synthetic note",
                        "updated_at": "2026-04-01T00:00:00+00:00",
                        "contract_family": "monthly_climate_anomaly",
                    }
                ],
            )

            summary = run_kalshi_climate_realtime_router(
                env_file=str(base / "fake.env"),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(output_dir),
                skip_realtime_collect=True,
                now=datetime(2026, 4, 1, 1, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["ws_collect_status"], "skipped_realtime_collect")
            self.assertEqual(summary["climate_rows_total"], 1)
            self.assertGreaterEqual(int(summary["climate_unpriced_model_view_rows"]), 1)
            self.assertTrue(Path(summary["availability_db_path"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_climate_realtime_router_excludes_non_climate_context_even_with_family_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            output_dir = base / "outputs"
            output_dir.mkdir(parents=True, exist_ok=True)

            history_csv = output_dir / "history.csv"
            priors_csv = base / "priors.csv"

            _write_csv(
                history_csv,
                [
                    "captured_at",
                    "category",
                    "market_ticker",
                    "market_title",
                    "close_time",
                    "hours_to_close",
                    "yes_bid_dollars",
                    "yes_ask_dollars",
                    "no_bid_dollars",
                    "no_ask_dollars",
                ],
                [
                    {
                        "captured_at": "2026-04-01T00:00:00+00:00",
                        "category": "Economics",
                        "market_ticker": "KXFED-26JUN-T3.50",
                        "market_title": "Will the fed funds upper bound be above 3.50%?",
                        "close_time": "2026-06-17T19:00:00Z",
                        "hours_to_close": "1800",
                        "yes_bid_dollars": "0.82",
                        "yes_ask_dollars": "0.84",
                        "no_bid_dollars": "0.16",
                        "no_ask_dollars": "0.18",
                    }
                ],
            )

            _write_csv(
                priors_csv,
                [
                    "market_ticker",
                    "fair_yes_probability",
                    "confidence",
                    "source_type",
                    "thesis",
                    "source_note",
                    "updated_at",
                    "contract_family",
                ],
                [
                    {
                        "market_ticker": "KXFED-26JUN-T3.50",
                        "fair_yes_probability": "0.65",
                        "confidence": "0.8",
                        "source_type": "weather_model",
                        "thesis": "Synthetic mislabeled non-climate",
                        "source_note": "Synthetic note",
                        "updated_at": "2026-04-01T00:00:00+00:00",
                        "contract_family": "daily_temperature",
                    }
                ],
            )

            summary = run_kalshi_climate_realtime_router(
                env_file=str(base / "fake.env"),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(output_dir),
                skip_realtime_collect=True,
                now=datetime(2026, 4, 1, 1, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["climate_rows_total"], 0)
            self.assertEqual(summary["market_tickers_selected_count"], 0)
            self.assertEqual(summary["climate_tradable_rows"], 0)
            self.assertEqual(summary["climate_priced_watch_only_rows"], 0)

    def test_run_kalshi_climate_realtime_router_seeds_recent_market_tickers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            output_dir = base / "outputs"
            output_dir.mkdir(parents=True, exist_ok=True)

            env_file = base / "env.local"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            history_csv = output_dir / "history.csv"
            priors_csv = base / "priors.csv"

            _write_csv(
                history_csv,
                [
                    "captured_at",
                    "category",
                    "market_ticker",
                    "market_title",
                    "close_time",
                    "hours_to_close",
                    "yes_bid_dollars",
                    "yes_ask_dollars",
                    "no_bid_dollars",
                    "no_ask_dollars",
                ],
                [],
            )
            _write_csv(
                priors_csv,
                [
                    "market_ticker",
                    "fair_yes_probability",
                    "confidence",
                    "source_type",
                    "thesis",
                    "source_note",
                    "updated_at",
                    "contract_family",
                ],
                [],
            )

            def _fake_market_http_get_json(url: str, headers: dict[str, str], timeout_seconds: float):
                self.assertIn("/markets?", url)
                return (
                    200,
                    {
                        "markets": [
                            {
                                "ticker": "KXHMONTHRANGE-26APR-B1.200",
                                "category": "Climate and Weather",
                                "title": "Apr 2026 temperature increase?",
                            },
                            {
                                "ticker": "KXFED-26JUN-T3.50",
                                "category": "Economics",
                                "title": "Fed funds upper bound?",
                            },
                        ]
                    },
                )

            summary = run_kalshi_climate_realtime_router(
                env_file=str(env_file),
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(output_dir),
                max_markets=10,
                seed_recent_markets=True,
                skip_realtime_collect=True,
                market_http_get_json=_fake_market_http_get_json,
                now=datetime(2026, 4, 1, 2, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["recent_market_discovery_status"], "ready")
            self.assertEqual(summary["recent_market_discovery_tickers_count"], 1)
            self.assertIn("KXHMONTHRANGE-26APR-B1.200", summary["market_tickers_selected"])
            self.assertNotIn("KXFED-26JUN-T3.50", summary["market_tickers_selected"])


if __name__ == "__main__":
    unittest.main()
