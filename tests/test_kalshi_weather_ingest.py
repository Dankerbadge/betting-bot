from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
from urllib.parse import parse_qs, urlparse
import unittest
from unittest.mock import patch

from betbot.kalshi_weather_ingest import (
    fetch_ncei_cdo_station_daily_history,
    fetch_noaa_global_land_ocean_anomaly_series,
    fetch_nws_station_hourly_forecast,
)


class KalshiWeatherIngestTests(unittest.TestCase):
    def test_fetch_nws_station_hourly_forecast_ready(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            if url.endswith("/stations/KJFK"):
                return (
                    200,
                    {
                        "properties": {"timeZone": "America/New_York"},
                        "geometry": {"coordinates": [-73.7800, 40.6400]},
                    },
                )
            if url.endswith("/points/40.6400,-73.7800"):
                return (200, {"properties": {"forecastHourly": "https://example.test/hourly"}})
            if url == "https://example.test/hourly":
                return (
                    200,
                    {
                        "properties": {
                            "updateTime": "2026-03-29T14:00:00+00:00",
                            "periods": [
                                {
                                    "startTime": "2026-03-29T15:00:00+00:00",
                                    "temperature": 52,
                                    "probabilityOfPrecipitation": {"value": 40},
                                }
                            ],
                        }
                    },
                )
            return (404, {})

        payload = fetch_nws_station_hourly_forecast(
            station_id="kjfk",
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["station_id"], "KJFK")
        self.assertEqual(payload["station_timezone"], "America/New_York")
        self.assertEqual(len(payload["periods"]), 1)
        self.assertEqual(payload["http_status_forecast"], 200)

    def test_fetch_noaa_global_land_ocean_anomaly_series_ready(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("anomaly_globe-land_ocean.json", url)
            return (200, [0.1, 0.2, 0.3])

        payload = fetch_noaa_global_land_ocean_anomaly_series(
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["start_year"], 1850)
        self.assertEqual(payload["end_year"], 1850)
        self.assertEqual(payload["end_month"], 3)
        self.assertEqual(payload["values"], [0.1, 0.2, 0.3])

    def test_fetch_ncei_cdo_station_daily_history_ready(self) -> None:
        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            self.assertEqual(headers, {"token": "demo-token"})
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            startdate = params.get("startdate", [""])[0]
            if startdate == "2024-03-29":
                return (
                    200,
                    {
                        "results": [
                            {"datatype": "TMAX", "value": 68.0},
                            {"datatype": "TMIN", "value": 53.0},
                            {"datatype": "PRCP", "value": 0.12},
                        ]
                    },
                )
            if startdate == "2025-03-29":
                return (
                    200,
                    {
                        "results": [
                            {"datatype": "TMAX", "value": 71.0},
                            {"datatype": "TMIN", "value": 49.0},
                            {"datatype": "PRCP", "value": 0.0},
                        ]
                    },
                )
            return (200, {"results": []})

        payload = fetch_ncei_cdo_station_daily_history(
            station_id="KJFK",
            month=3,
            day=29,
            lookback_years=2,
            timeout_seconds=5.0,
            cdo_token="demo-token",
            now=datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc),
            http_get_json_with_headers=fake_http_get_json_with_headers,
        )

        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["sample_years"], 2)
        self.assertEqual(payload["tmax_values_f"], [68.0, 71.0])
        self.assertEqual(payload["tmin_values_f"], [53.0, 49.0])
        self.assertAlmostEqual(payload["rain_day_frequency"], 0.5)

    def test_fetch_ncei_cdo_station_daily_history_missing_token(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "BETBOT_NOAA_CDO_TOKEN": "",
                "NOAA_CDO_TOKEN": "",
                "NCEI_CDO_TOKEN": "",
            },
            clear=False,
        ):
            payload = fetch_ncei_cdo_station_daily_history(
                station_id="KJFK",
                month=3,
                day=29,
                cdo_token="",
            )
        self.assertEqual(payload["status"], "disabled_missing_token")

    def test_fetch_ncei_cdo_station_daily_history_uses_cache_when_fresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cdo_cache"
            request_calls: list[str] = []

            def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
                request_calls.append(url)
                parsed = urlparse(url)
                params = parse_qs(parsed.query)
                startdate = params.get("startdate", [""])[0]
                if startdate == "2025-03-29":
                    return (
                        200,
                        {
                            "results": [
                                {"datatype": "TMAX", "value": 66.0},
                                {"datatype": "TMIN", "value": 51.0},
                                {"datatype": "PRCP", "value": 0.08},
                            ]
                        },
                    )
                return (200, {"results": []})

            first = fetch_ncei_cdo_station_daily_history(
                station_id="KJFK",
                month=3,
                day=29,
                lookback_years=3,
                timeout_seconds=5.0,
                cdo_token="demo-token",
                cache_dir=str(cache_dir),
                cache_max_age_hours=24.0,
                now=datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc),
                http_get_json_with_headers=fake_http_get_json_with_headers,
            )
            self.assertEqual(first["status"], "ready")
            self.assertFalse(first["cache_hit"])
            self.assertGreater(len(request_calls), 0)

            second = fetch_ncei_cdo_station_daily_history(
                station_id="KJFK",
                month=3,
                day=29,
                lookback_years=3,
                timeout_seconds=5.0,
                cdo_token="demo-token",
                cache_dir=str(cache_dir),
                cache_max_age_hours=24.0,
                now=datetime(2026, 3, 30, 0, 10, tzinfo=timezone.utc),
                http_get_json_with_headers=lambda *_: (_ for _ in ()).throw(AssertionError("network should not be called")),
            )
            self.assertEqual(second["status"], "ready")
            self.assertTrue(second["cache_hit"])
            self.assertFalse(second["cache_fallback_used"])
            self.assertTrue(second["cache_fresh"])


if __name__ == "__main__":
    unittest.main()
