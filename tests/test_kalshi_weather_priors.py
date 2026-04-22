from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from urllib.error import URLError

from betbot.kalshi_nonsports_priors import PRIOR_FIELDNAMES
from betbot.kalshi_weather_priors import (
    _infer_station_id,
    _settlement_timezone_name,
    run_kalshi_weather_priors,
    run_kalshi_weather_station_history_prewarm,
)


HISTORY_FIELDNAMES = [
    "captured_at",
    "category",
    "series_ticker",
    "event_ticker",
    "market_ticker",
    "event_title",
    "market_title",
    "rules_primary",
    "close_time",
    "hours_to_close",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "spread_dollars",
]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _source_note_value(source_note: str, key: str) -> float | None:
    for part in source_note.split(";"):
        token = part.strip()
        prefix = f"{key}="
        if not token.startswith(prefix):
            continue
        try:
            return float(token[len(prefix):])
        except ValueError:
            return None
    return None


class KalshiWeatherPriorsTests(unittest.TestCase):
    def test_infer_station_id_avoids_la_substring_false_positive(self) -> None:
        row = {
            "market_ticker": "KXHIGHOKC-26APR10-T82",
            "event_title": "Oklahoma City high temperature",
            "market_title": "Oklahoma City above 82F?",
            "rules_primary": "",
        }
        self.assertEqual(_infer_station_id(row, ""), "KOKC")

    def test_settlement_timezone_name_falls_back_to_station_timezone(self) -> None:
        row = {
            "market_ticker": "KXHIGHTSATX-26APR10-T83",
            "event_title": "San Antonio high temperature",
            "market_title": "San Antonio above 83F?",
            "rules_primary": "",
        }
        settlement = {"settlement_station": "KSAT", "settlement_timezone": ""}
        self.assertEqual(
            _settlement_timezone_name(settlement, row),
            "America/Chicago",
        )

    def test_settlement_timezone_name_avoids_la_substring_false_positive(self) -> None:
        row = {
            "market_ticker": "KXHIGHNOLA-26APR10-B83.5",
            "event_title": "New Orleans high temperature",
            "market_title": "New Orleans 83.5 or below?",
            "rules_primary": "",
        }
        settlement = {"settlement_station": "", "settlement_timezone": ""}
        self.assertEqual(
            _settlement_timezone_name(settlement, row),
            "America/Chicago",
        )

    def test_run_kalshi_weather_priors_generates_and_upserts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-29T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR29",
                        "market_ticker": "KXRAINNYC-26MAR29",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": "If measurable rain is recorded at station KJFK, this market resolves to Yes.",
                        "close_time": "2026-03-30T03:59:00+00:00",
                        "hours_to_close": "10",
                        "yes_bid_dollars": "0.30",
                        "yes_ask_dollars": "0.40",
                        "spread_dollars": "0.10",
                    },
                    {
                        "captured_at": "2026-03-29T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXHMONTHRANGE",
                        "event_ticker": "KXHMONTHRANGE-26APR",
                        "market_ticker": "KXHMONTHRANGE-26APR-B1.200",
                        "event_title": "Apr 2026 temperature increase?",
                        "market_title": "Apr 2026 temperature increase?",
                        "rules_primary": "If the Land Ocean-Temperature Index for Apr 2026 is between 1.17-1.23, then the market resolves to Yes.",
                        "close_time": "2026-05-01T00:00:00+00:00",
                        "hours_to_close": "790",
                        "yes_bid_dollars": "0.20",
                        "yes_ask_dollars": "0.30",
                        "spread_dollars": "0.10",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            def fake_station_forecast_fetcher(*, station_id: str, timeout_seconds: float):
                self.assertEqual(station_id, "KJFK")
                return {
                    "status": "ready",
                    "station_id": station_id,
                    "forecast_updated_at": "2026-03-29T12:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-03-29T13:00:00+00:00",
                            "temperature": 53,
                            "probabilityOfPrecipitation": {"value": 35},
                        },
                        {
                            "startTime": "2026-03-29T14:00:00+00:00",
                            "temperature": 55,
                            "probabilityOfPrecipitation": {"value": 40},
                        },
                        {
                            "startTime": "2026-03-29T15:00:00+00:00",
                            "temperature": 57,
                            "probabilityOfPrecipitation": {"value": 30},
                        },
                    ],
                }

            def fake_noaa_series_fetcher(*, timeout_seconds: float):
                values = [0.8 + (index * 0.001) for index in range(300)]
                return {
                    "status": "ready",
                    "series_url": "https://example.test/noaa-series",
                    "start_year": 1850,
                    "start_month": 1,
                    "end_year": 2026,
                    "end_month": 3,
                    "values": values,
                }

            def fake_station_history_fetcher(
                *,
                station_id: str,
                month: int,
                day: int,
                lookback_years: int,
                timeout_seconds: float,
                now: datetime,
            ):
                self.assertEqual(station_id, "KJFK")
                return {
                    "status": "ready",
                    "sample_years": 8,
                    "rain_day_frequency": 0.41,
                    "tmax_values_f": [55.0, 58.0, 60.0, 63.0, 61.0, 59.0, 57.0, 62.0],
                    "tmin_values_f": [43.0, 45.0, 47.0, 49.0, 46.0, 44.0, 42.0, 48.0],
                    "daily_mean_values_f": [49.0, 51.0, 53.5, 56.0, 53.5, 51.5, 49.5, 55.0],
                }

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=fake_station_forecast_fetcher,
                station_history_fetcher=fake_station_history_fetcher,
                anomaly_series_fetcher=fake_noaa_series_fetcher,
                now=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["generated_priors"], 2)
            self.assertEqual(summary["inserted_rows"], 2)
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            tickers = {row["market_ticker"] for row in rows}
            self.assertEqual(tickers, {"KXRAINNYC-26MAR29", "KXHMONTHRANGE-26APR-B1.200"})
            for row in rows:
                self.assertEqual(row["source_type"], "auto_weather")
                probability = float(row["fair_yes_probability"])
                self.assertGreaterEqual(probability, 0.001)
                self.assertLessEqual(probability, 0.999)
            rain_row = next(row for row in rows if row["market_ticker"] == "KXRAINNYC-26MAR29")
            self.assertEqual(rain_row["weather_station_history_status"], "ready")
            self.assertEqual(rain_row["weather_station_history_live_ready"], "True")
            self.assertEqual(rain_row["weather_station_history_live_ready_reason"], "ready")

    def test_run_kalshi_weather_priors_protects_manual_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-29T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR29",
                        "market_ticker": "KXRAINNYC-26MAR29",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": "If measurable rain is recorded at station KJFK, this market resolves to Yes.",
                        "close_time": "2026-03-30T03:59:00+00:00",
                        "hours_to_close": "10",
                        "yes_bid_dollars": "0.30",
                        "yes_ask_dollars": "0.40",
                        "spread_dollars": "0.10",
                    },
                ],
            )
            _write_csv(
                priors_csv,
                PRIOR_FIELDNAMES,
                [
                    {
                        "market_ticker": "KXRAINNYC-26MAR29",
                        "fair_yes_probability": "0.11",
                        "fair_yes_probability_low": "0.08",
                        "fair_yes_probability_high": "0.14",
                        "confidence": "0.5",
                        "thesis": "Manual thesis",
                        "source_note": "Manual note",
                        "updated_at": "2026-03-29T10:00:00+00:00",
                        "evidence_count": "3",
                        "evidence_quality": "0.9",
                        "source_type": "manual",
                        "last_evidence_at": "2026-03-29T10:00:00+00:00",
                    }
                ],
            )

            def fake_station_forecast_fetcher(*, station_id: str, timeout_seconds: float):
                return {
                    "status": "ready",
                    "station_id": station_id,
                    "forecast_updated_at": "2026-03-29T12:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-03-29T13:00:00+00:00",
                            "temperature": 53,
                            "probabilityOfPrecipitation": {"value": 75},
                        },
                    ],
                }

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=fake_station_forecast_fetcher,
                station_history_fetcher=lambda **kwargs: {"status": "disabled_missing_token"},
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["generated_priors"], 1)
            self.assertEqual(summary["manual_rows_protected"], 1)
            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["source_type"], "manual")
            self.assertEqual(rows[0]["fair_yes_probability"], "0.11")

    def test_run_kalshi_weather_priors_writes_upstream_error_summary_on_dns_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-29T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR29",
                        "market_ticker": "KXRAINNYC-26MAR29",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": "If measurable rain is recorded at station KJFK, this market resolves to Yes.",
                        "close_time": "2026-03-30T03:59:00+00:00",
                        "hours_to_close": "10",
                        "yes_bid_dollars": "0.30",
                        "yes_ask_dollars": "0.40",
                        "spread_dollars": "0.10",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            def failing_station_forecast_fetcher(*, station_id: str, timeout_seconds: float):
                raise URLError("[Errno 8] nodename nor servname provided, or not known")

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=failing_station_forecast_fetcher,
                station_history_fetcher=lambda **kwargs: {"status": "disabled_missing_token"},
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "upstream_error")
            self.assertEqual(summary["generated_priors"], 0)
            self.assertEqual(summary["fetch_errors_count"], 1)
            self.assertEqual(summary["fetch_error_kind_counts"], {"dns_resolution_error": 1})
            self.assertEqual(summary["error_kind"], "dns_resolution_error")
            self.assertIn("nodename nor servname", summary["error"])
            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertTrue(Path(summary["skipped_output_csv"]).exists())

    def test_run_kalshi_weather_priors_passes_station_history_cache_settings_when_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-29T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR29",
                        "market_ticker": "KXRAINNYC-26MAR29",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": "If measurable rain is recorded at station KJFK, this market resolves to Yes.",
                        "close_time": "2026-03-30T03:59:00+00:00",
                        "hours_to_close": "10",
                        "yes_bid_dollars": "0.30",
                        "yes_ask_dollars": "0.40",
                        "spread_dollars": "0.10",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            seen_cache_dir: list[str] = []
            seen_cache_age_hours: list[float] = []

            def fake_station_history_fetcher(
                *,
                station_id: str,
                month: int,
                day: int,
                lookback_years: int,
                timeout_seconds: float,
                now: datetime,
                cache_dir: str,
                cache_max_age_hours: float,
            ):
                seen_cache_dir.append(cache_dir)
                seen_cache_age_hours.append(float(cache_max_age_hours))
                return {
                    "status": "ready",
                    "sample_years": 4,
                    "rain_day_frequency": 0.35,
                    "tmax_values_f": [55.0, 58.0, 60.0, 63.0],
                    "tmin_values_f": [43.0, 45.0, 47.0, 49.0],
                    "daily_mean_values_f": [49.0, 51.5, 53.5, 56.0],
                }

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=lambda **kwargs: {
                    "status": "ready",
                    "station_id": "KJFK",
                    "forecast_updated_at": "2026-03-29T12:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-03-29T13:00:00+00:00",
                            "temperature": 53,
                            "probabilityOfPrecipitation": {"value": 35},
                        }
                    ],
                },
                station_history_fetcher=fake_station_history_fetcher,
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                station_history_cache_max_age_hours=18.0,
                now=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertTrue(seen_cache_dir)
            self.assertEqual(str(Path(seen_cache_dir[0]).name), "weather_station_history_cache")
            self.assertEqual(seen_cache_age_hours, [18.0])
            self.assertEqual(summary["station_history_cache_max_age_hours"], 18.0)
            self.assertTrue(str(summary["station_history_cache_dir"]).endswith("weather_station_history_cache"))

    def test_run_kalshi_weather_priors_uses_settlement_window_for_daily_rain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-30T00:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR30",
                        "market_ticker": "KXRAINNYC-26MAR30",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": (
                            "If measurable rain is recorded at station KJFK between 6:00 AM and 6:59 AM local time, "
                            "this market resolves to Yes."
                        ),
                        "close_time": "2026-03-30T14:00:00+00:00",
                        "hours_to_close": "12",
                        "yes_bid_dollars": "0.09",
                        "yes_ask_dollars": "0.11",
                        "spread_dollars": "0.02",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            def fake_station_forecast_fetcher(*, station_id: str, timeout_seconds: float):
                self.assertEqual(station_id, "KJFK")
                return {
                    "status": "ready",
                    "station_id": station_id,
                    "forecast_updated_at": "2026-03-30T00:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-03-30T09:00:00+00:00",  # 05:00 local
                            "temperature": 51,
                            "probabilityOfPrecipitation": {"value": 90},
                        },
                        {
                            "startTime": "2026-03-30T10:00:00+00:00",  # 06:00 local
                            "temperature": 52,
                            "probabilityOfPrecipitation": {"value": 10},
                        },
                        {
                            "startTime": "2026-03-30T14:00:00+00:00",  # 10:00 local
                            "temperature": 56,
                            "probabilityOfPrecipitation": {"value": 90},
                        },
                    ],
                }

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=fake_station_forecast_fetcher,
                station_history_fetcher=lambda **kwargs: {"status": "disabled_missing_token"},
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 3, 30, 0, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["generated_priors"], 1)

            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(len(rows), 1)
            generated = rows[0]
            # Window filter should include the single 06:00 local period (10% PoP), not the surrounding 90% periods.
            self.assertAlmostEqual(float(generated["model_probability_raw"]), 0.10, places=3)
            self.assertAlmostEqual(float(generated["fair_yes_probability"]), 0.10, places=3)
            self.assertEqual(generated["observation_window_local_start"], "06:00")
            self.assertEqual(generated["observation_window_local_end"], "06:59")
            self.assertIn("settlement_window_local=06:00-06:59", generated["source_note"])

    def test_run_kalshi_weather_priors_handles_overnight_settlement_windows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-31T00:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR31",
                        "market_ticker": "KXRAINNYC-26MAR31",
                        "event_title": "Will it rain in NYC overnight?",
                        "market_title": "Will it rain in NYC overnight?",
                        "rules_primary": (
                            "If measurable rain is recorded at station KJFK between 6:00 PM and 6:00 AM local time, "
                            "this market resolves to Yes."
                        ),
                        "close_time": "2026-03-31T12:00:00+00:00",
                        "hours_to_close": "12",
                        "yes_bid_dollars": "0.40",
                        "yes_ask_dollars": "0.42",
                        "spread_dollars": "0.02",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            def fake_station_forecast_fetcher(*, station_id: str, timeout_seconds: float):
                self.assertEqual(station_id, "KJFK")
                return {
                    "status": "ready",
                    "station_id": station_id,
                    "forecast_updated_at": "2026-03-31T00:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-03-31T16:00:00+00:00",  # 12:00 local (exclude)
                            "temperature": 58,
                            "probabilityOfPrecipitation": {"value": 90},
                        },
                        {
                            "startTime": "2026-03-31T22:00:00+00:00",  # 18:00 local (include)
                            "temperature": 55,
                            "probabilityOfPrecipitation": {"value": 20},
                        },
                        {
                            "startTime": "2026-04-01T06:00:00+00:00",  # 02:00 local next day (include)
                            "temperature": 50,
                            "probabilityOfPrecipitation": {"value": 30},
                        },
                    ],
                }

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=fake_station_forecast_fetcher,
                station_history_fetcher=lambda **kwargs: {"status": "disabled_missing_token"},
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 3, 31, 0, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["generated_priors"], 1)

            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(len(rows), 1)
            generated = rows[0]
            # Overnight window should include 18:00 local same-day + early next-day periods only.
            self.assertAlmostEqual(float(generated["model_probability_raw"]), 0.44, places=3)
            self.assertEqual(generated["observation_window_local_start"], "18:00")
            self.assertEqual(generated["observation_window_local_end"], "06:00")
            self.assertIn("settlement_window_local=18:00-06:00", generated["source_note"])

    def test_run_kalshi_weather_station_history_prewarm_deduplicates_station_day_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-30T00:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR30",
                        "market_ticker": "KXRAINNYC-26MAR30",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": "If measurable rain is recorded at station KJFK, this market resolves to Yes.",
                        "close_time": "2026-03-30T18:00:00+00:00",
                        "hours_to_close": "12",
                        "yes_bid_dollars": "0.40",
                        "yes_ask_dollars": "0.42",
                        "spread_dollars": "0.02",
                    },
                    {
                        "captured_at": "2026-03-30T00:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXTEMPNYC",
                        "event_ticker": "KXTEMPNYC-26MAR30",
                        "market_ticker": "KXTEMPNYC-26MAR30",
                        "event_title": "NYC high temperature today",
                        "market_title": "Will NYC high temperature be above 65?",
                        "rules_primary": "If high temperature is measured at station KJFK, resolves Yes above 65.",
                        "close_time": "2026-03-30T20:00:00+00:00",
                        "hours_to_close": "14",
                        "yes_bid_dollars": "0.30",
                        "yes_ask_dollars": "0.31",
                        "spread_dollars": "0.01",
                    },
                ],
            )

            fetch_calls: list[tuple[str, int, int]] = []

            def fake_station_history_fetcher(
                *,
                station_id: str,
                month: int,
                day: int,
                lookback_years: int,
                timeout_seconds: float,
                now: datetime,
                cache_dir: str,
                cache_max_age_hours: float,
            ):
                fetch_calls.append((station_id, month, day))
                return {
                    "status": "ready",
                    "cache_hit": True,
                    "cache_fallback_used": False,
                    "cache_fresh": True,
                    "cache_age_seconds": 120.0,
                    "sample_years": 10,
                }

            summary = run_kalshi_weather_station_history_prewarm(
                history_csv=str(history_csv),
                output_dir=str(base),
                station_history_fetcher=fake_station_history_fetcher,
                max_station_day_keys=50,
                now=datetime(2026, 3, 30, 0, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["daily_weather_station_day_keys_total"], 1)
            self.assertEqual(summary["prewarm_keys_attempted"], 1)
            self.assertEqual(fetch_calls, [("KJFK", 3, 30)])
            self.assertEqual(summary["status_counts"], {"ready": 1})
            self.assertEqual(summary["live_ready_counts"], {"live_ready": 1})
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_weather_priors_marks_insufficient_sample_years_not_live_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-30T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXTEMPNYC",
                        "event_ticker": "KXTEMPNYC-26MAR30",
                        "market_ticker": "KXTEMPNYC-26MAR30",
                        "event_title": "NYC high temperature today",
                        "market_title": "Will NYC high temperature be at least 60?",
                        "rules_primary": "If high temperature at station KJFK is at least 60, resolves Yes.",
                        "close_time": "2026-03-30T20:00:00+00:00",
                        "hours_to_close": "8",
                        "yes_bid_dollars": "0.44",
                        "yes_ask_dollars": "0.46",
                        "spread_dollars": "0.02",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            def fake_station_forecast_fetcher(*, station_id: str, timeout_seconds: float):
                return {
                    "status": "ready",
                    "station_id": station_id,
                    "forecast_updated_at": "2026-03-30T12:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-03-30T13:00:00+00:00",
                            "temperature": 62,
                            "probabilityOfPrecipitation": {"value": 20},
                        },
                        {
                            "startTime": "2026-03-30T14:00:00+00:00",
                            "temperature": 64,
                            "probabilityOfPrecipitation": {"value": 25},
                        },
                    ],
                }

            def fake_station_history_fetcher(**kwargs):
                return {
                    "status": "ready",
                    "sample_years": 6,
                    "rain_day_frequency": 0.30,
                    "tmax_values_f": [60.0, 62.0, 64.0, 63.0, 61.0, 65.0],
                    "tmin_values_f": [45.0, 46.0, 47.0, 44.0, 43.0, 48.0],
                    "daily_mean_values_f": [52.5, 54.0, 55.5, 53.5, 52.0, 56.5],
                    "cache_hit": True,
                    "cache_fallback_used": False,
                    "cache_fresh": True,
                    "cache_age_seconds": 30.0,
                }

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=fake_station_forecast_fetcher,
                station_history_fetcher=fake_station_history_fetcher,
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 3, 30, 12, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["contract_family"], "daily_temperature")
            self.assertEqual(row["weather_station_history_sample_years"], "6")
            self.assertEqual(row["weather_station_history_min_sample_years_required"], "10")
            self.assertEqual(row["weather_station_history_live_ready"], "False")
            self.assertEqual(row["weather_station_history_live_ready_reason"], "insufficient_sample_years")

    def test_run_kalshi_weather_priors_uses_variable_specific_sample_depth_for_rain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-30T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR30",
                        "market_ticker": "KXRAINNYC-26MAR30",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": "If measurable rain at KJFK, resolves Yes.",
                        "close_time": "2026-03-30T20:00:00+00:00",
                        "hours_to_close": "8",
                        "yes_bid_dollars": "0.44",
                        "yes_ask_dollars": "0.46",
                        "spread_dollars": "0.02",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=lambda **kwargs: {
                    "status": "ready",
                    "station_id": "KJFK",
                    "forecast_updated_at": "2026-03-30T12:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-03-30T13:00:00+00:00",
                            "temperature": 62,
                            "probabilityOfPrecipitation": {"value": 40},
                        },
                    ],
                },
                station_history_fetcher=lambda **kwargs: {
                    "status": "ready",
                    "sample_years": 20,
                    "sample_years_precip": 5,
                    "sample_years_tmax": 20,
                    "sample_years_tmin": 20,
                    "sample_years_mean": 20,
                    "rain_day_frequency": 0.35,
                    "prcp_values_in": [0.0, 0.1, 0.2, 0.0, 0.05],
                    "tmax_values_f": [60.0] * 20,
                    "tmin_values_f": [45.0] * 20,
                    "daily_mean_values_f": [52.5] * 20,
                    "cache_hit": True,
                    "cache_fallback_used": False,
                    "cache_fresh": True,
                    "cache_age_seconds": 30.0,
                },
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 3, 30, 12, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["contract_family"], "daily_rain")
            self.assertEqual(row["weather_station_history_sample_metric"], "precip")
            self.assertEqual(row["weather_station_history_sample_years"], "5")
            self.assertEqual(row["weather_station_history_sample_years_total"], "20")
            self.assertEqual(row["evidence_count"], "6")
            self.assertGreater(float(row["confidence"]), 0.43)
            self.assertLess(float(row["confidence"]), 0.50)
            self.assertEqual(row["weather_station_history_live_ready"], "False")
            self.assertEqual(row["weather_station_history_live_ready_reason"], "insufficient_sample_years")

    def test_run_kalshi_weather_priors_adapts_rain_climatology_weight_by_regime(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-03-29T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26MAR29",
                        "market_ticker": "KXRAINNYC-26MAR29",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": "If measurable rain is recorded at station KJFK, this market resolves to Yes.",
                        "close_time": "2026-03-30T03:59:00+00:00",
                        "hours_to_close": "10",
                        "yes_bid_dollars": "0.30",
                        "yes_ask_dollars": "0.40",
                        "spread_dollars": "0.10",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            history_payload = {
                "status": "ready",
                "sample_years": 16,
                "sample_years_precip": 16,
                "rain_day_frequency": 0.32,
                "tmax_values_f": [55.0, 58.0, 60.0, 63.0, 61.0, 59.0, 57.0, 62.0],
                "tmin_values_f": [43.0, 45.0, 47.0, 49.0, 46.0, 44.0, 42.0, 48.0],
                "daily_mean_values_f": [49.0, 51.0, 53.5, 56.0, 53.5, 51.5, 49.5, 55.0],
                "cache_hit": True,
                "cache_fallback_used": False,
                "cache_fresh": True,
                "cache_age_seconds": 120.0,
            }

            run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=lambda **kwargs: {
                    "status": "ready",
                    "station_id": "KJFK",
                    "forecast_updated_at": "2026-03-26T12:00:00+00:00",
                    "periods": [
                        {"startTime": "2026-03-29T13:00:00+00:00", "temperature": 53, "probabilityOfPrecipitation": {"value": 5}},
                        {"startTime": "2026-03-29T14:00:00+00:00", "temperature": 54, "probabilityOfPrecipitation": {"value": 15}},
                        {"startTime": "2026-03-29T15:00:00+00:00", "temperature": 55, "probabilityOfPrecipitation": {"value": 10}},
                        {"startTime": "2026-03-29T16:00:00+00:00", "temperature": 56, "probabilityOfPrecipitation": {"value": 25}},
                        {"startTime": "2026-03-29T17:00:00+00:00", "temperature": 56, "probabilityOfPrecipitation": {"value": 5}},
                        {"startTime": "2026-03-29T18:00:00+00:00", "temperature": 55, "probabilityOfPrecipitation": {"value": 20}},
                        {"startTime": "2026-03-29T19:00:00+00:00", "temperature": 54, "probabilityOfPrecipitation": {"value": 10}},
                        {"startTime": "2026-03-29T20:00:00+00:00", "temperature": 53, "probabilityOfPrecipitation": {"value": 15}},
                    ],
                },
                station_history_fetcher=lambda **kwargs: dict(history_payload),
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            )
            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                stale_row = next(dict(row) for row in csv.DictReader(handle) if row["market_ticker"] == "KXRAINNYC-26MAR29")
            stale_weight = _source_note_value(stale_row.get("source_note", ""), "rain_climatology_blend_weight")

            run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=lambda **kwargs: {
                    "status": "ready",
                    "station_id": "KJFK",
                    "forecast_updated_at": "2026-03-29T12:00:00+00:00",
                    "periods": [
                        {"startTime": "2026-03-29T13:00:00+00:00", "temperature": 53, "probabilityOfPrecipitation": {"value": 85}},
                        {"startTime": "2026-03-29T14:00:00+00:00", "temperature": 54, "probabilityOfPrecipitation": {"value": 92}},
                        {"startTime": "2026-03-29T15:00:00+00:00", "temperature": 55, "probabilityOfPrecipitation": {"value": 88}},
                    ],
                },
                station_history_fetcher=lambda **kwargs: dict(history_payload),
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 3, 29, 12, 5, tzinfo=timezone.utc),
            )
            with priors_csv.open("r", newline="", encoding="utf-8") as handle:
                fresh_row = next(dict(row) for row in csv.DictReader(handle) if row["market_ticker"] == "KXRAINNYC-26MAR29")
            fresh_weight = _source_note_value(fresh_row.get("source_note", ""), "rain_climatology_blend_weight")

            self.assertIsNotNone(stale_weight)
            self.assertIsNotNone(fresh_weight)
            assert isinstance(stale_weight, float)
            assert isinstance(fresh_weight, float)
            self.assertGreater(stale_weight, fresh_weight)
            self.assertGreaterEqual(stale_weight, 0.06)
            self.assertLessEqual(stale_weight, 0.45)
            self.assertGreaterEqual(fresh_weight, 0.06)
            self.assertLessEqual(fresh_weight, 0.45)

    def test_run_kalshi_weather_priors_ignores_non_finite_rain_pop_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-04-01T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXRAINNYC",
                        "event_ticker": "KXRAINNYC-26APR01",
                        "market_ticker": "KXRAINNYC-26APR01",
                        "event_title": "Will it rain in NYC today?",
                        "market_title": "Will it rain in NYC today?",
                        "rules_primary": "If measurable rain is recorded at station KJFK, this market resolves to Yes.",
                        "close_time": "2026-04-01T23:59:00+00:00",
                        "hours_to_close": "10",
                        "yes_bid_dollars": "0.09",
                        "yes_ask_dollars": "0.11",
                        "spread_dollars": "0.02",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=lambda **kwargs: {
                    "status": "ready",
                    "station_id": "KJFK",
                    "forecast_updated_at": "2026-04-01T12:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-04-01T13:00:00+00:00",
                            "temperature": 58,
                            "probabilityOfPrecipitation": {"value": "Infinity"},
                        },
                        {
                            "startTime": "2026-04-01T14:00:00+00:00",
                            "temperature": 60,
                            "probabilityOfPrecipitation": {"value": 10},
                        },
                    ],
                },
                station_history_fetcher=lambda **kwargs: {"status": "disabled_missing_token"},
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 4, 1, 12, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["contract_family"], "daily_rain")
            self.assertAlmostEqual(float(row["model_probability_raw"]), 0.1, places=3)
            self.assertLess(float(row["fair_yes_probability"]), 0.2)

    def test_run_kalshi_weather_priors_ignores_non_finite_temperature_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            history_csv = base / "history.csv"
            priors_csv = base / "priors.csv"
            _write_csv(
                history_csv,
                HISTORY_FIELDNAMES,
                [
                    {
                        "captured_at": "2026-04-01T12:00:00+00:00",
                        "category": "Climate and Weather",
                        "series_ticker": "KXTEMPNYC",
                        "event_ticker": "KXTEMPNYC-26APR01",
                        "market_ticker": "KXTEMPNYC-26APR01",
                        "event_title": "NYC high temperature today",
                        "market_title": "Will NYC high temperature be at least 70?",
                        "rules_primary": "If high temperature at station KJFK is at least 70, resolves Yes.",
                        "close_time": "2026-04-01T23:59:00+00:00",
                        "hours_to_close": "10",
                        "yes_bid_dollars": "0.40",
                        "yes_ask_dollars": "0.42",
                        "spread_dollars": "0.02",
                    },
                ],
            )
            _write_csv(priors_csv, PRIOR_FIELDNAMES, [])

            summary = run_kalshi_weather_priors(
                priors_csv=str(priors_csv),
                history_csv=str(history_csv),
                output_dir=str(base),
                station_forecast_fetcher=lambda **kwargs: {
                    "status": "ready",
                    "station_id": "KJFK",
                    "forecast_updated_at": "2026-04-01T12:00:00+00:00",
                    "periods": [
                        {
                            "startTime": "2026-04-01T13:00:00+00:00",
                            "temperature": "NaN",
                            "probabilityOfPrecipitation": {"value": 10},
                        },
                        {
                            "startTime": "2026-04-01T14:00:00+00:00",
                            "temperature": 80,
                            "probabilityOfPrecipitation": {"value": 10},
                        },
                    ],
                },
                station_history_fetcher=lambda **kwargs: {"status": "disabled_missing_token"},
                anomaly_series_fetcher=lambda **kwargs: {"status": "ready", "values": [0.0] * 24},
                now=datetime(2026, 4, 1, 12, 5, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            with Path(summary["output_csv"]).open("r", newline="", encoding="utf-8") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["contract_family"], "daily_temperature")
            self.assertGreater(float(row["model_probability_raw"]), 0.99)
            self.assertGreater(float(row["fair_yes_probability"]), 0.85)


if __name__ == "__main__":
    unittest.main()
