from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
from urllib.parse import parse_qs, urlparse
import unittest
from unittest.mock import patch

from betbot.kalshi_weather_ingest import (
    fetch_aviationweather_taf_temperature_envelopes,
    fetch_nws_active_alerts_for_point,
    fetch_nws_cli_station_daily_summary_for_date,
    fetch_ncei_cdo_station_daily_history,
    fetch_ncei_station_daily_summary_for_date,
    fetch_ncei_normals_station_day,
    fetch_noaa_mrms_qpe_latest_metadata,
    fetch_noaa_nbm_latest_snapshot,
    fetch_noaa_global_land_ocean_anomaly_series,
    fetch_nws_station_hourly_forecast,
    fetch_nws_station_recent_observations,
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

    def test_fetch_nws_station_hourly_forecast_with_gridpoint_data(self) -> None:
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
                return (
                    200,
                    {
                        "properties": {
                            "forecastHourly": "https://example.test/hourly",
                            "forecastGridData": "https://example.test/gridpoint",
                        }
                    },
                )
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
                                }
                            ],
                        }
                    },
                )
            if url == "https://example.test/gridpoint":
                return (
                    200,
                    {
                        "properties": {
                            "updateTime": "2026-03-29T14:05:00+00:00",
                            "maxTemperature": {
                                "values": [
                                    {"validTime": "2026-03-29T00:00:00+00:00/PT1H", "value": 13.0}
                                ]
                            },
                            "probabilityOfPrecipitation": {
                                "values": [
                                    {"validTime": "2026-03-29T00:00:00+00:00/PT1H", "value": 35.0}
                                ]
                            },
                        }
                    },
                )
            return (404, {})

        payload = fetch_nws_station_hourly_forecast(
            station_id="kjfk",
            include_gridpoint_data=True,
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["gridpoint_status"], "ready")
        self.assertEqual(payload["http_status_gridpoint"], 200)
        self.assertIn("maxTemperature", payload["gridpoint_layers"])
        self.assertIn("probabilityOfPrecipitation", payload["gridpoint_layers"])

    def test_fetch_nws_station_recent_observations_ready(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/stations/KJFK/observations", url)
            return (
                200,
                {
                    "features": [
                        {
                            "properties": {
                                "timestamp": "2026-03-29T14:00:00+00:00",
                                "textDescription": "Light rain",
                                "temperature": {"value": 11.2},
                                "dewpoint": {"value": 9.3},
                                "relativeHumidity": {"value": 82.0},
                                "precipitationLastHour": {"value": 2.1},
                                "windSpeed": {"value": 5.4},
                            }
                        }
                    ]
                },
            )

        payload = fetch_nws_station_recent_observations(
            station_id="KJFK",
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["observations_count"], 1)
        self.assertEqual(payload["observations"][0]["text_description"], "Light rain")
        self.assertEqual(payload["observations"][0]["temperature_c"], 11.2)

    def test_fetch_aviationweather_taf_temperature_envelopes_ready(self) -> None:
        taf_xml = """
        <response>
          <data num_results=\"2\">
            <TAF>
              <station_id>KJFK</station_id>
              <issue_time>2026-04-16T10:00:00Z</issue_time>
              <raw_text>TAF KJFK 161000Z 1610/1712 21010KT P6SM FEW040 TX18/1619Z TN07/1709Z TEMPO 1618/1622 4SM TSRA</raw_text>
            </TAF>
            <TAF>
              <station_id>KJFK</station_id>
              <issue_time>2026-04-16T06:00:00Z</issue_time>
              <raw_text>TAF KJFK 160600Z 1606/1712 22008KT P6SM FEW040 TX14/1618Z TN06/1708Z</raw_text>
            </TAF>
          </data>
        </response>
        """.strip().encode("utf-8")

        def fake_http_get_bytes(url: str, timeout_seconds: float):
            self.assertIn("tafs.cache.xml.gz", url)
            import gzip

            return (200, gzip.compress(taf_xml), {"etag": "taf-demo"})

        payload = fetch_aviationweather_taf_temperature_envelopes(
            station_ids=["KJFK", "KMDW"],
            timeout_seconds=5.0,
            http_get_bytes=fake_http_get_bytes,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["taf_station_ready_count"], 1)
        station = payload["station_envelopes"]["KJFK"]
        self.assertEqual(station["taf_status"], "ready")
        self.assertAlmostEqual(float(station["taf_upper_bound_f"]), 64.4, places=1)
        self.assertAlmostEqual(float(station["taf_lower_bound_f"]), 44.6, places=1)
        self.assertGreater(float(station["taf_volatility_score"]), 0.0)
        self.assertEqual(payload["station_envelopes"]["KMDW"]["taf_status"], "missing_station")

    def test_fetch_nws_active_alerts_for_point_ready(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("/alerts/active?point=40.6400,-73.7800", url)
            return (
                200,
                {
                    "features": [
                        {
                            "id": "https://api.weather.gov/alerts/abc123",
                            "properties": {
                                "event": "Flood Watch",
                                "severity": "Moderate",
                                "urgency": "Future",
                                "headline": "Flood Watch remains in effect",
                                "effective": "2026-03-29T14:00:00+00:00",
                                "expires": "2026-03-30T02:00:00+00:00",
                                "areaDesc": "Queens; Nassau",
                            },
                        }
                    ]
                },
            )

        payload = fetch_nws_active_alerts_for_point(
            latitude=40.64,
            longitude=-73.78,
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["alerts_count"], 1)
        self.assertEqual(payload["alerts"][0]["event"], "Flood Watch")

    def test_fetch_noaa_mrms_qpe_latest_metadata_ready(self) -> None:
        def fake_http_get_text(url: str, timeout_seconds: float):
            self.assertIn("prefix=CONUS%2FMultiSensor_QPE_01H_Pass2_00.00%2F20260401%2F", url)
            return (
                200,
                """
                <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                  <Name>noaa-mrms-pds</Name>
                  <Prefix>CONUS/MultiSensor_QPE_01H_Pass2_00.00/20260401/</Prefix>
                  <IsTruncated>false</IsTruncated>
                  <Contents>
                    <Key>CONUS/MultiSensor_QPE_01H_Pass2_00.00/20260401/MRMS_MultiSensor_QPE_01H_Pass2_00.00_20260401-120000.grib2.gz</Key>
                  </Contents>
                  <Contents>
                    <Key>CONUS/MultiSensor_QPE_01H_Pass2_00.00/20260401/MRMS_MultiSensor_QPE_01H_Pass2_00.00_20260401-130000.grib2.gz</Key>
                  </Contents>
                </ListBucketResult>
                """,
            )

        payload = fetch_noaa_mrms_qpe_latest_metadata(
            now=datetime(2026, 4, 1, 13, 10, tzinfo=timezone.utc),
            timeout_seconds=5.0,
            lookback_days=0,
            http_get_text=fake_http_get_text,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(
            payload["latest_key"],
            "CONUS/MultiSensor_QPE_01H_Pass2_00.00/20260401/MRMS_MultiSensor_QPE_01H_Pass2_00.00_20260401-130000.grib2.gz",
        )
        self.assertLessEqual(float(payload["age_seconds"]), 900.0)

    def test_fetch_noaa_nbm_latest_snapshot_ready(self) -> None:
        idx_payload = "\n".join(
            [
                "1:0:d=2026040100:TMP:2 m above ground:1 hour fcst:",
                "2:123:d=2026040100:TMAX:2 m above ground:1 hour fcst:",
                "3:456:d=2026040100:TMIN:2 m above ground:1 hour fcst:",
                "4:789:d=2026040100:APCP:surface:1 hour fcst:",
                "5:999:d=2026040100:POP:surface:1 hour fcst:",
            ]
        )

        def fake_http_get_text(url: str, timeout_seconds: float):
            if "prefix=blend.20260401%2F23%2Fcore%2F" in url:
                return (
                    200,
                    """
                    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                      <Name>noaa-nbm-grib2-pds</Name>
                      <Prefix>blend.20260401/23/core/</Prefix>
                      <IsTruncated>false</IsTruncated>
                      <Contents><Key>blend.20260401/23/core/blend.t23z.core.f001.co.grib2.idx</Key></Contents>
                      <Contents><Key>blend.20260401/23/core/blend.t23z.core.f002.co.grib2.idx</Key></Contents>
                      <Contents><Key>blend.20260401/23/core/blend.t23z.core.f003.co.grib2.idx</Key></Contents>
                    </ListBucketResult>
                    """,
                )
            if url.endswith("blend.20260401/23/core/blend.t23z.core.f001.co.grib2.idx"):
                return (200, idx_payload)
            self.fail(f"unexpected url: {url}")

        payload = fetch_noaa_nbm_latest_snapshot(
            now=datetime(2026, 4, 1, 23, 40, tzinfo=timezone.utc),
            timeout_seconds=5.0,
            lookback_days=0,
            region="co",
            http_get_text=fake_http_get_text,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["forecast_hours_count"], 3)
        self.assertEqual(payload["max_forecast_hour"], 3)
        self.assertEqual(payload["idx_variable_counts"].get("TMP"), 1)
        self.assertEqual(payload["idx_variable_counts"].get("APCP"), 1)
        self.assertEqual(payload["idx_variable_counts"].get("POP"), 1)

    def test_fetch_ncei_normals_station_day_ready(self) -> None:
        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            self.assertEqual(headers, {"token": "demo-token"})
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            self.assertEqual(params.get("datasetid"), ["NORMAL_DLY"])
            self.assertEqual(params.get("startdate"), ["2010-03-29"])
            return (
                200,
                {
                    "results": [
                        {"datatype": "DLY-TMAX-NORMAL", "value": 53.0},
                        {"datatype": "DLY-TMAX-STDDEV", "value": 8.1},
                        {"datatype": "DLY-TMIN-NORMAL", "value": 37.9},
                        {"datatype": "DLY-TMIN-STDDEV", "value": 6.0},
                        {"datatype": "DLY-PRCP-PCTALL-GE001HI", "value": 377},
                    ]
                },
            )

        payload = fetch_ncei_normals_station_day(
            station_id="KJFK",
            month=3,
            day=29,
            timeout_seconds=5.0,
            cdo_token="demo-token",
            http_get_json_with_headers=fake_http_get_json_with_headers,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["tmax_normal_f"], 53.0)
        self.assertEqual(payload["tmin_normal_f"], 37.9)
        self.assertAlmostEqual(float(payload["rain_day_frequency"]), 0.377)

    def test_fetch_ncei_normals_station_day_retries_rate_limited_exception(self) -> None:
        calls = 0

        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            nonlocal calls
            calls += 1
            if calls <= 2:
                raise RuntimeError("HTTP Error 429: ")
            return (
                200,
                {
                    "results": [
                        {"datatype": "DLY-TMAX-NORMAL", "value": 53.0},
                        {"datatype": "DLY-TMIN-NORMAL", "value": 37.9},
                        {"datatype": "DLY-PRCP-PCTALL-GE001HI", "value": 377},
                    ]
                },
            )

        payload = fetch_ncei_normals_station_day(
            station_id="KJFK",
            month=3,
            day=29,
            timeout_seconds=5.0,
            cdo_token="demo-token",
            rate_limit_retries=2,
            rate_limit_backoff_seconds=0.0,
            rate_limit_backoff_cap_seconds=0.0,
            sleep_fn=lambda _: None,
            http_get_json_with_headers=fake_http_get_json_with_headers,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(calls, 3)

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

    def test_fetch_noaa_global_land_ocean_anomaly_series_filters_non_finite_values(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("anomaly_globe-land_ocean.json", url)
            return (200, [0.1, float("nan"), "Infinity", "-inf", 0.3])

        payload = fetch_noaa_global_land_ocean_anomaly_series(
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["end_year"], 1850)
        self.assertEqual(payload["end_month"], 2)
        self.assertEqual(payload["values"], [0.1, 0.3])

    def test_fetch_noaa_global_land_ocean_anomaly_series_non_finite_only_returns_empty(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            self.assertIn("anomaly_globe-land_ocean.json", url)
            return (200, [float("nan"), "inf", "-inf"])

        payload = fetch_noaa_global_land_ocean_anomaly_series(
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,
        )
        self.assertEqual(payload["status"], "noaa_series_empty")
        self.assertEqual(payload["http_status"], 200)

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

    def test_fetch_ncei_cdo_station_daily_history_filters_invalid_temperature_range(self) -> None:
        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            self.assertEqual(headers, {"token": "demo-token"})
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            startdate = params.get("startdate", [""])[0]
            if startdate == "2023-03-29":
                return (
                    200,
                    {
                        "results": [
                            {"datatype": "TMAX", "value": 67.0},
                            {"datatype": "TMIN", "value": 51.0},
                            {"datatype": "PRCP", "value": 0.0},
                        ]
                    },
                )
            if startdate == "2024-03-29":
                return (
                    200,
                    {
                        "results": [
                            {"datatype": "TMAX", "value": 49.0},
                            {"datatype": "TMIN", "value": 61.0},
                            {"datatype": "PRCP", "value": 0.04},
                        ]
                    },
                )
            if startdate == "2025-03-29":
                return (
                    200,
                    {
                        "results": [
                            {"datatype": "TMAX", "value": 72.0},
                            {"datatype": "TMIN", "value": 54.0},
                            {"datatype": "PRCP", "value": 0.1},
                        ]
                    },
                )
            return (200, {"results": []})

        payload = fetch_ncei_cdo_station_daily_history(
            station_id="KJFK",
            month=3,
            day=29,
            lookback_years=3,
            timeout_seconds=5.0,
            cdo_token="demo-token",
            now=datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc),
            http_get_json_with_headers=fake_http_get_json_with_headers,
        )

        self.assertEqual(payload["status"], "ready_partial")
        self.assertEqual(payload["sample_years"], 3)
        self.assertEqual(payload["sample_years_tmax"], 2)
        self.assertEqual(payload["sample_years_tmin"], 2)
        self.assertEqual(payload["tmax_values_f"], [67.0, 72.0])
        self.assertEqual(payload["tmin_values_f"], [51.0, 54.0])
        self.assertTrue(any(str(err.get("error")) == "invalid_temperature_range" for err in payload["errors"]))
        sample_2024 = next(
            sample for sample in payload["daily_samples"] if int(sample.get("year", 0)) == 2024
        )
        self.assertNotIn("tmax_f", sample_2024)
        self.assertNotIn("tmin_f", sample_2024)

    def test_fetch_ncei_cdo_station_daily_history_retries_rate_limited_exception(self) -> None:
        calls_by_date: dict[str, int] = {}

        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            startdate = params.get("startdate", [""])[0]
            calls_by_date[startdate] = calls_by_date.get(startdate, 0) + 1
            if startdate == "2025-03-29" and calls_by_date[startdate] <= 2:
                raise RuntimeError("HTTP Error 429: ")
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
            lookback_years=3,
            timeout_seconds=5.0,
            cdo_token="demo-token",
            now=datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc),
            rate_limit_retries=2,
            rate_limit_backoff_seconds=0.0,
            rate_limit_backoff_cap_seconds=0.0,
            sleep_fn=lambda _: None,
            http_get_json_with_headers=fake_http_get_json_with_headers,
        )

        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["sample_years"], 1)
        self.assertEqual(payload["rate_limit_retries_used"], 2)
        self.assertEqual(payload["request_count"], 5)

    def test_fetch_ncei_cdo_station_daily_history_missing_token(self) -> None:
        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            self.assertIsNone(headers)
            return (503, {"errorMessage": "Service unavailable"})

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
                http_get_json_with_headers=fake_http_get_json_with_headers,
            )
        self.assertEqual(payload["status"], "disabled_missing_token")

    def test_fetch_ncei_cdo_station_daily_history_missing_token_uses_ads_fallback(self) -> None:
        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            self.assertIsNone(headers)
            parsed = urlparse(url)
            self.assertIn("/access/services/data/v1", parsed.path)
            return (
                200,
                [
                    {"DATE": "2024-03-29", "TMAX": "68", "TMIN": "53", "PRCP": "0.12"},
                    {"DATE": "2025-03-29", "TMAX": "71", "TMIN": "49", "PRCP": "0.00"},
                ],
            )

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
                lookback_years=2,
                now=datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc),
                http_get_json_with_headers=fake_http_get_json_with_headers,
            )

        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["sample_years"], 2)
        self.assertEqual(payload["sample_years_precip"], 2)
        self.assertEqual(payload["data_source"], "access_data_service_v1")
        self.assertAlmostEqual(payload["rain_day_frequency"], 0.5)

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

    def test_fetch_ncei_cdo_station_daily_history_ignores_cache_without_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cdo_cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file = cache_dir / "KJFK_03_29_3_2025.json"
            cache_file.write_text(
                (
                    '{\n'
                    '  "payload": {\n'
                    '    "status": "ready",\n'
                    '    "station_id": "KJFK",\n'
                    '    "daily_samples": [{"year": 2025, "date": "2025-03-29", "tmax_f": 66.0, "tmin_f": 51.0}],\n'
                    '    "sample_years": 1\n'
                    "  }\n"
                    "}\n"
                ),
                encoding="utf-8",
            )

            payload = fetch_ncei_cdo_station_daily_history(
                station_id="KJFK",
                month=3,
                day=29,
                lookback_years=3,
                timeout_seconds=5.0,
                cdo_token="",
                cache_dir=str(cache_dir),
                cache_max_age_hours=24.0,
                enable_access_data_service_fallback=False,
                now=datetime(2026, 3, 30, 0, 10, tzinfo=timezone.utc),
                http_get_json_with_headers=lambda *_: (_ for _ in ()).throw(
                    AssertionError("network should not be called in this path")
                ),
            )

            self.assertEqual(payload["status"], "disabled_missing_token")
            self.assertFalse(payload["cache_hit"])
            self.assertFalse(payload["cache_fallback_used"])

    def test_fetch_ncei_cdo_station_daily_history_surfaces_cache_write_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / "cdo_cache"

            def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
                parsed = urlparse(url)
                self.assertIn("/cdo-web/api/v2/data", parsed.path)
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

            with patch("betbot.kalshi_weather_ingest._write_cdo_cache_entry", side_effect=OSError("disk full")):
                payload = fetch_ncei_cdo_station_daily_history(
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

            self.assertEqual(payload["status"], "ready")
            self.assertEqual(payload.get("cache_write_error"), "disk full")

    def test_fetch_ncei_station_daily_summary_for_date_uses_cdo_when_token_present(self) -> None:
        def should_not_fetch_nws(_url: str, _timeout_seconds: float):
            raise AssertionError("NWS lookup should be disabled in this CDO-only test")

        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            parsed = urlparse(url)
            self.assertIn("/cdo-web/api/v2/data", parsed.path)
            self.assertIsNotNone(headers)
            self.assertEqual(headers.get("token"), "demo-token")
            return (
                200,
                {
                    "results": [
                        {"datatype": "TMAX", "value": 87.0},
                        {"datatype": "TMIN", "value": 61.0},
                        {"datatype": "PRCP", "value": 0.12},
                    ]
                },
            )

        payload = fetch_ncei_station_daily_summary_for_date(
            station_id="KJFK",
            target_date="2026-04-10",
            cdo_token="demo-token",
            enable_nws_cli_lookup=False,
            http_get_json=should_not_fetch_nws,
            http_get_json_with_headers=fake_http_get_json_with_headers,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["data_source"], "cdo_api_v2")
        self.assertEqual(payload["daily_sample"]["tmax_f"], 87.0)

    def test_fetch_ncei_station_daily_summary_for_date_falls_back_to_ads_without_token(self) -> None:
        def should_not_fetch_nws(_url: str, _timeout_seconds: float):
            raise AssertionError("NWS lookup should be disabled in this ADS-only test")

        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            parsed = urlparse(url)
            self.assertIn("/access/services/data/v1", parsed.path)
            self.assertIsNone(headers)
            return (
                200,
                [
                    {"DATE": "2026-04-10", "TMAX": "86", "TMIN": "58", "PRCP": "0.00"},
                ],
            )

        with patch.dict(
            "os.environ",
            {
                "BETBOT_NOAA_CDO_TOKEN": "",
                "NOAA_CDO_TOKEN": "",
                "NCEI_CDO_TOKEN": "",
            },
            clear=False,
        ):
            payload = fetch_ncei_station_daily_summary_for_date(
                station_id="KJFK",
                target_date="2026-04-10",
                cdo_token="",
                enable_nws_cli_lookup=False,
                http_get_json=should_not_fetch_nws,
                http_get_json_with_headers=fake_http_get_json_with_headers,
            )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["data_source"], "access_data_service_v1")
        self.assertEqual(payload["daily_sample"]["tmax_f"], 86.0)

    def test_fetch_ncei_station_daily_summary_for_date_prefers_nws_cli_report(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            parsed = urlparse(url)
            if parsed.path.endswith("/products/types/CLI/locations/AUS"):
                return (
                    200,
                    {
                        "@graph": [
                            {"id": "demo-product", "issuanceTime": "2026-04-11T22:40:00+00:00"},
                        ]
                    },
                )
            if parsed.path.endswith("/products/demo-product"):
                return (
                    200,
                    {
                        "productText": (
                            "000\n"
                            "CDUS44 KEWX 112240\n"
                            "CLIAUS\n\n"
                            "...THE AUSTIN CLIMATE SUMMARY FOR APRIL 10 2026...\n"
                            "TEMPERATURE (F)\n"
                            "  MAXIMUM         88   3:31 PM  95  1907\n"
                            "  MINIMUM         61   6:43 AM  40  1988\n"
                        )
                    },
                )
            return (404, {})

        def should_not_fetch_cdo_or_ads(_url: str, _timeout_seconds: float, _headers: dict[str, str] | None):
            raise AssertionError("CDO/ADS fallback should not run when NWS CLI report is ready")

        payload = fetch_ncei_station_daily_summary_for_date(
            station_id="KAUS",
            target_date="2026-04-10",
            cdo_token="",
            enable_nws_cli_lookup=True,
            http_get_json=fake_http_get_json,
            http_get_json_with_headers=should_not_fetch_cdo_or_ads,
        )
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["data_source"], "nws_cli_daily_climate_report")
        self.assertEqual(payload["daily_sample"]["tmax_f"], 88.0)
        self.assertEqual(payload["daily_sample"]["tmin_f"], 61.0)

    def test_fetch_nws_cli_station_daily_summary_for_date_rejects_station_code_mismatch(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            parsed = urlparse(url)
            if parsed.path.endswith("/products/types/CLI/locations/KJFK"):
                return (200, {"@graph": []})
            if parsed.path.endswith("/products/types/CLI/locations/JFK"):
                return (
                    200,
                    {
                        "@graph": [
                            {"id": "mismatch-product", "issuanceTime": "2026-04-11T22:40:00+00:00"},
                        ]
                    },
                )
            if parsed.path.endswith("/products/mismatch-product"):
                return (
                    200,
                    {
                        "productText": (
                            "000\n"
                            "CDUS41 KOKX 112240\n"
                            "CLINEWR\n\n"
                            "...THE NEWARK CLIMATE SUMMARY FOR APRIL 10 2026...\n"
                            "TEMPERATURE (F)\n"
                            "  MAXIMUM         76   4:11 PM  95  1988\n"
                            "  MINIMUM         52   5:05 AM  40  1899\n"
                        )
                    },
                )
            return (404, {})

        payload = fetch_nws_cli_station_daily_summary_for_date(
            station_id="KJFK",
            target_date="2026-04-10",
            timeout_seconds=5.0,
            http_get_json=fake_http_get_json,
        )

        self.assertEqual(payload["status"], "no_final_report")
        self.assertTrue(
            any(str(item).startswith("station_code_mismatch:mismatch-product:") for item in payload.get("errors", []))
        )

    def test_fetch_ncei_station_daily_summary_for_date_supports_nws_cli_location_override(self) -> None:
        def fake_http_get_json(url: str, timeout_seconds: float):
            parsed = urlparse(url)
            if parsed.path.endswith("/products/types/CLI/locations/KDAL"):
                return (200, {"@graph": []})
            if parsed.path.endswith("/products/types/CLI/locations/DAL"):
                return (200, {"@graph": []})
            if parsed.path.endswith("/products/types/CLI/locations/DFW"):
                return (
                    200,
                    {
                        "@graph": [
                            {"id": "dfw-product", "issuanceTime": "2026-04-11T22:40:00+00:00"},
                        ]
                    },
                )
            if parsed.path.endswith("/products/dfw-product"):
                return (
                    200,
                    {
                        "productText": (
                            "000\n"
                            "CDUS44 KFWD 112240\n"
                            "CLIDFW\n\n"
                            "...THE DALLAS CLIMATE SUMMARY FOR APRIL 10 2026...\n"
                            "TEMPERATURE (F)\n"
                            "  MAXIMUM         86   4:11 PM  95  1988\n"
                            "  MINIMUM         66   5:05 AM  40  1899\n"
                        )
                    },
                )
            return (404, {})

        def should_not_fetch_cdo_or_ads(_url: str, _timeout_seconds: float, _headers: dict[str, str] | None):
            raise AssertionError("CDO/ADS fallback should not run when CLI location override resolves a report")

        with patch.dict(
            "os.environ",
            {
                "BETBOT_WEATHER_NWS_CLI_LOCATION_MAP": "KDAL=DFW",
                "BETBOT_NOAA_CDO_TOKEN": "",
                "NOAA_CDO_TOKEN": "",
                "NCEI_CDO_TOKEN": "",
            },
            clear=False,
        ):
            payload = fetch_ncei_station_daily_summary_for_date(
                station_id="KDAL",
                target_date="2026-04-10",
                cdo_token="",
                enable_nws_cli_lookup=True,
                http_get_json=fake_http_get_json,
                http_get_json_with_headers=should_not_fetch_cdo_or_ads,
            )

        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["data_source"], "nws_cli_daily_climate_report")
        self.assertEqual(payload["nws_cli_station_code"], "DFW")
        self.assertIn("DFW", payload.get("location_candidates", []))
        self.assertEqual(payload["daily_sample"]["tmax_f"], 86.0)
        self.assertEqual(payload["daily_sample"]["tmin_f"], 66.0)

    def test_fetch_ncei_station_daily_summary_for_date_continues_when_nws_cli_lookup_errors(self) -> None:
        def failing_http_get_json(_url: str, _timeout_seconds: float):
            raise TimeoutError("read operation timed out")

        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            parsed = urlparse(url)
            self.assertIn("/access/services/data/v1", parsed.path)
            self.assertIsNone(headers)
            return (
                200,
                [
                    {"DATE": "2026-04-10", "TMAX": "86", "TMIN": "58", "PRCP": "0.00"},
                ],
            )

        with patch.dict(
            "os.environ",
            {
                "BETBOT_NOAA_CDO_TOKEN": "",
                "NOAA_CDO_TOKEN": "",
                "NCEI_CDO_TOKEN": "",
            },
            clear=False,
        ):
            payload = fetch_ncei_station_daily_summary_for_date(
                station_id="KJFK",
                target_date="2026-04-10",
                cdo_token="",
                enable_nws_cli_lookup=True,
                http_get_json=failing_http_get_json,
                http_get_json_with_headers=fake_http_get_json_with_headers,
            )

        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["data_source"], "access_data_service_v1")
        self.assertEqual(payload["daily_sample"]["tmax_f"], 86.0)
        self.assertEqual(payload["nws_cli_lookup_status"], "request_failed")
        self.assertIn("timed out", str(payload["nws_cli_lookup_error"]).lower())

    def test_fetch_ncei_station_daily_summary_for_date_uses_ads_when_cdo_mapping_missing(self) -> None:
        def should_not_fetch_nws(_url: str, _timeout_seconds: float):
            raise AssertionError("NWS lookup should be disabled in this ADS fallback test")

        def fake_http_get_json_with_headers(url: str, timeout_seconds: float, headers: dict[str, str] | None):
            parsed = urlparse(url)
            self.assertIn("/access/services/data/v1", parsed.path)
            self.assertIsNone(headers)
            query = parse_qs(parsed.query)
            self.assertEqual(query.get("stations"), ["KAUS"])
            return (
                200,
                [
                    {"DATE": "2026-04-10", "TMAX": "89", "TMIN": "64", "PRCP": "0.00"},
                ],
            )

        with patch.dict(
            "os.environ",
            {
                "BETBOT_NOAA_CDO_TOKEN": "",
                "NOAA_CDO_TOKEN": "",
                "NCEI_CDO_TOKEN": "",
            },
            clear=False,
        ):
            payload = fetch_ncei_station_daily_summary_for_date(
                station_id="KAUS",
                target_date="2026-04-10",
                cdo_token="",
                enable_nws_cli_lookup=False,
                http_get_json=should_not_fetch_nws,
                http_get_json_with_headers=fake_http_get_json_with_headers,
            )

        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["data_source"], "access_data_service_v1")
        self.assertEqual(payload["daily_sample"]["tmax_f"], 89.0)
        self.assertEqual(payload.get("cdo_station_id"), "")
        self.assertEqual(payload.get("ads_station_id"), "KAUS")


if __name__ == "__main__":
    unittest.main()
