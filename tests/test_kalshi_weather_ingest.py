from __future__ import annotations

import unittest

from betbot.kalshi_weather_ingest import (
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


if __name__ == "__main__":
    unittest.main()
