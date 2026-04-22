from __future__ import annotations

from datetime import datetime, timezone
import unittest

from betbot.kalshi_weather_intraday import (
    build_intraday_temperature_snapshot,
    classify_temperature_outcomes,
    quantize_temperature,
)


class KalshiWeatherIntradayTests(unittest.TestCase):
    def test_build_intraday_temperature_snapshot_prefers_metar_state(self) -> None:
        metar_state = {
            "latest_observation_by_station": {
                "KJFK": {
                    "observation_time_utc": "2026-04-08T18:40:00+00:00",
                    "temp_c": 20.0,
                }
            },
            "max_temp_c_by_station_local_day": {
                "KJFK|2026-04-08": 20.0,
            },
            "min_temp_c_by_station_local_day": {
                "KJFK|2026-04-08": 12.0,
            },
        }

        def should_not_fetch(_url: str, _timeout_seconds: float):
            raise AssertionError("NWS fetch should not run when METAR state already has same-day max")

        snapshot = build_intraday_temperature_snapshot(
            station_id="KJFK",
            target_date_local="2026-04-08",
            timezone_name="America/New_York",
            settlement_unit="fahrenheit",
            settlement_precision="whole_degree",
            metar_state=metar_state,
            http_get_json=should_not_fetch,
        )
        self.assertEqual(snapshot["status"], "ready")
        self.assertEqual(snapshot["snapshot_source"], "metar_state")
        self.assertEqual(snapshot["observations_for_date"], 1)
        self.assertEqual(snapshot["max_temperature_c"], 20.0)
        self.assertEqual(snapshot["max_temperature_settlement_quantized"], 68.0)
        self.assertEqual(snapshot["min_temperature_c"], 12.0)
        self.assertEqual(snapshot["min_temperature_settlement_quantized"], 54.0)

    def test_build_intraday_temperature_snapshot_falls_back_to_nws_when_metar_missing(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-04-08T14:10:00+00:00",
                                "temperature": {"value": 18.2},
                            }
                        }
                    ]
                },
            )

        snapshot = build_intraday_temperature_snapshot(
            station_id="KJFK",
            target_date_local="2026-04-08",
            timezone_name="America/New_York",
            settlement_unit="fahrenheit",
            settlement_precision="whole_degree",
            metar_state={
                "latest_observation_by_station": {},
                "max_temp_c_by_station_local_day": {},
                "min_temp_c_by_station_local_day": {},
            },
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(snapshot["status"], "ready")
        self.assertEqual(snapshot["snapshot_source"], "nws_station_observations")
        self.assertEqual(snapshot["observations_for_date"], 1)

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
        self.assertEqual(snapshot["min_temperature_c"], 18.2)
        self.assertEqual(snapshot["min_temperature_settlement_quantized"], 65.0)

    def test_build_intraday_temperature_snapshot_orders_observations_by_timestamp(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-04-08T18:40:00+00:00",
                                "temperature": {"value": 20.0},
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
                                "timestamp": "2026-04-08T17:05:00+00:00",
                                "temperature": {"value": 19.1},
                            }
                        },
                    ]
                },
            )

        snapshot = build_intraday_temperature_snapshot(
            station_id="KJFK",
            target_date_local="2026-04-08",
            timezone_name="America/New_York",
            http_get_json=fake_http_get_json,
        )

        self.assertEqual(snapshot["status"], "ready")
        ordered_timestamps = [item["timestamp"] for item in snapshot["observations"]]
        self.assertEqual(
            ordered_timestamps,
            [
                "2026-04-08T14:10:00+00:00",
                "2026-04-08T17:05:00+00:00",
                "2026-04-08T18:40:00+00:00",
            ],
        )

    def test_build_intraday_temperature_snapshot_drops_future_timestamp_observations(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
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
            http_get_json=fake_http_get_json,
            now_utc=datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(snapshot["status"], "ready")
        self.assertEqual(snapshot["observations_for_date"], 1)
        self.assertEqual(
            [item["timestamp"] for item in snapshot["observations"]],
            ["2026-04-08T14:10:00+00:00"],
        )
        self.assertIn("future_timestamp_at_index_1", snapshot["parse_warnings"])

    def test_build_intraday_temperature_snapshot_filters_invalid_temperature_values(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-04-08T13:10:00+00:00",
                                "temperature": {"value": "bad"},
                            }
                        },
                        {
                            "properties": {
                                "timestamp": "2026-04-08T14:10:00+00:00",
                                "temperature": {"value": float("nan")},
                            }
                        },
                        {
                            "properties": {
                                "timestamp": "2026-04-08T15:10:00+00:00",
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
            http_get_json=fake_http_get_json,
        )

        self.assertEqual(snapshot["status"], "ready")
        self.assertEqual(snapshot["observations_for_date"], 1)
        self.assertEqual(snapshot["max_temperature_c"], 20.0)
        self.assertEqual(snapshot["min_temperature_c"], 20.0)
        self.assertEqual(
            [item["timestamp"] for item in snapshot["observations"]],
            ["2026-04-08T15:10:00+00:00"],
        )
        self.assertIn("invalid_temperature_c_at_index_0", snapshot["parse_warnings"])
        self.assertIn("invalid_temperature_c_at_index_1", snapshot["parse_warnings"])

    def test_build_intraday_temperature_snapshot_marks_freshness_unchecked_by_default(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-04-08T15:10:00+00:00",
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
            http_get_json=fake_http_get_json,
            now_utc=datetime(2026, 4, 8, 16, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(snapshot["status"], "ready")
        self.assertEqual(snapshot["freshness_status"], "unchecked")
        self.assertEqual(snapshot["freshness_reason"], "freshness_guard_disabled")
        self.assertAlmostEqual(float(snapshot["latest_observation_age_seconds"]), 3000.0, places=3)

    def test_build_intraday_temperature_snapshot_strict_freshness_rejects_stale_observation(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-04-08T14:10:00+00:00",
                                "temperature": {"value": 18.2},
                            }
                        },
                    ]
                },
            )

        snapshot = build_intraday_temperature_snapshot(
            station_id="KJFK",
            target_date_local="2026-04-08",
            timezone_name="America/New_York",
            http_get_json=fake_http_get_json,
            now_utc=datetime(2026, 4, 8, 16, 0, tzinfo=timezone.utc),
            max_observation_age_seconds=3600.0,
            require_fresh_observation=True,
        )

        self.assertEqual(snapshot["status"], "observations_stale")
        self.assertEqual(snapshot["freshness_status"], "stale")
        self.assertEqual(snapshot["freshness_reason"], "max_age_exceeded")
        self.assertEqual(snapshot["latest_observation_time_utc"], "2026-04-08T14:10:00+00:00")
        self.assertIn("freshness_guard_failed:max_age_exceeded", snapshot["parse_warnings"])

    def test_build_intraday_temperature_snapshot_strict_freshness_falls_back_from_stale_metar(self) -> None:
        metar_state = {
            "latest_observation_by_station": {
                "KJFK": {
                    "observation_time_utc": "2026-04-08T10:00:00+00:00",
                    "temp_c": 20.0,
                }
            },
            "max_temp_c_by_station_local_day": {
                "KJFK|2026-04-08": 20.0,
            },
            "min_temp_c_by_station_local_day": {
                "KJFK|2026-04-08": 12.0,
            },
        }

        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-04-08T15:30:00+00:00",
                                "temperature": {"value": 17.0},
                            }
                        },
                    ]
                },
            )

        snapshot = build_intraday_temperature_snapshot(
            station_id="KJFK",
            target_date_local="2026-04-08",
            timezone_name="America/New_York",
            metar_state=metar_state,
            http_get_json=fake_http_get_json,
            now_utc=datetime(2026, 4, 8, 16, 0, tzinfo=timezone.utc),
            max_observation_age_seconds=3600.0,
            require_fresh_observation=True,
            allow_nws_fallback=True,
        )

        self.assertEqual(snapshot["status"], "ready")
        self.assertEqual(snapshot["snapshot_source"], "nws_station_observations")
        self.assertEqual(snapshot["freshness_status"], "fresh")
        self.assertEqual(snapshot["freshness_reason"], "max_age_within_limit")

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
