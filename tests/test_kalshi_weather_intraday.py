from __future__ import annotations

import unittest

from betbot.kalshi_weather_intraday import (
    build_intraday_temperature_snapshot,
    classify_temperature_outcomes,
    quantize_temperature,
)


class KalshiWeatherIntradayTests(unittest.TestCase):
    def test_build_intraday_temperature_snapshot_filters_by_local_day(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-04-08T03:50:00+00:00",
                                "temperature": {"value": 10.0},
                            }
                        },
                        {
                            "properties": {
                                "timestamp": "2026-04-08T14:10:00+00:00",
                                "temperature": {"value": 18.2},
                            }
                        },
                        {
                            "properties": {
                                "timestamp": "2026-04-08T18:40:00+00:00",
                                "temperature": {"value": 20.0},
                            }
                        },
                    ]
                },
            )

        snapshot = build_intraday_temperature_snapshot(
            station_id="KJFK",
            target_date_local="2026-04-08",
            timezone_name="America/New_York",
            settlement_unit="fahrenheit",
            settlement_precision="whole_degree",
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(snapshot["status"], "ready")
        self.assertEqual(snapshot["observations_total"], 3)
        self.assertEqual(snapshot["observations_for_date"], 2)
        self.assertEqual(snapshot["max_temperature_c"], 20.0)
        self.assertEqual(snapshot["max_temperature_settlement_quantized"], 68.0)

    def test_quantize_temperature_nearest_half_away_from_zero(self) -> None:
        self.assertEqual(quantize_temperature(21.5), 22.0)
        self.assertEqual(quantize_temperature(-1.5), -2.0)

    def test_classify_temperature_outcomes_applies_bounds(self) -> None:
        classification = classify_temperature_outcomes(
            candidate_values=[65, 66, 67, 68, 69, 70],
            observed_max_value=68,
            forecast_upper_bound=69,
        )
        self.assertEqual(classification["status"], "ready")
        self.assertEqual(classification["impossible_values"], [65, 66, 67, 70])
        self.assertEqual(classification["feasible_values"], [68, 69])
        self.assertEqual(classification["locked_values"], [])


if __name__ == "__main__":
    unittest.main()
