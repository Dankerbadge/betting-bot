from __future__ import annotations

import csv
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import tempfile
import unittest

from betbot.kalshi_temperature_metar_ingest import (
    parse_metar_cache_csv_gz,
    run_kalshi_temperature_metar_ingest,
)


def _build_metar_cache_blob(rows: list[dict[str, str]]) -> bytes:
    fieldnames = ["station_id", "report_type", "observation_time", "temp_c", "raw_text"]
    import io

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return gzip.compress(buffer.getvalue().encode("utf-8"))


class KalshiTemperatureMetarIngestTests(unittest.TestCase):
    def test_parse_metar_cache_csv_gz_ready(self) -> None:
        blob = _build_metar_cache_blob(
            [
                {
                    "station_id": "KJFK",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T14:00:00Z",
                    "temp_c": "19.0",
                    "raw_text": "KJFK 081400Z ...",
                },
                {
                    "station_id": "KLAX",
                    "observation_time": "2026-04-08T14:00:00Z",
                    "temp_c": "17.0",
                    "raw_text": "SPECI KLAX 081400Z ...",
                },
            ]
        )
        parsed = parse_metar_cache_csv_gz(blob)
        self.assertEqual(parsed["status"], "ready")
        self.assertEqual(len(parsed["rows"]), 2)
        self.assertEqual(len(parsed["errors"]), 0)
        self.assertEqual(parsed["rows"][0]["station_id"], "KJFK")
        self.assertEqual(parsed["rows"][0]["report_type"], "METAR")
        self.assertEqual(parsed["rows"][1]["report_type"], "SPECI")

    def test_parse_metar_cache_csv_gz_tracks_missing_station_and_observation_time_diagnostics(self) -> None:
        blob = _build_metar_cache_blob(
            [
                {
                    "station_id": "",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T14:00:00Z",
                    "temp_c": "19.0",
                    "raw_text": "XXXX 081400Z ...",
                },
                {
                    "station_id": "KJFK",
                    "report_type": "METAR",
                    "observation_time": "",
                    "temp_c": "18.0",
                    "raw_text": "KJFK 081410Z ...",
                },
                {
                    "station_id": "",
                    "report_type": "SPECI",
                    "observation_time": "",
                    "temp_c": "17.0",
                    "raw_text": "XXXX 081420Z ...",
                },
                {
                    "station_id": "KLAX",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T14:30:00Z",
                    "temp_c": "17.0",
                    "raw_text": "KLAX 081430Z ...",
                },
            ]
        )
        parsed = parse_metar_cache_csv_gz(blob)
        self.assertEqual(parsed["status"], "ready_partial")
        self.assertEqual(parsed["source_row_count"], 4)
        diagnostics = parsed["diagnostics"]
        self.assertEqual(diagnostics["source_row_count"], 4)
        self.assertEqual(diagnostics["dropped_row_count"], 3)
        self.assertEqual(diagnostics["missing_station_count"], 2)
        self.assertEqual(diagnostics["missing_observation_time_count"], 2)
        self.assertEqual(diagnostics["missing_station_or_observation_time_count"], 3)
        self.assertEqual(len(parsed["rows"]), 1)

    def test_run_ingest_updates_station_local_day_max(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\nKJFK,America/New_York\n",
                encoding="utf-8",
            )

            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T03:00:00Z",
                        "temp_c": "10.0",
                        "raw_text": "KJFK 080300Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T14:00:00Z",
                        "temp_c": "20.0",
                        "raw_text": "KJFK 081400Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:00:00Z",
                        "temp_c": "18.0",
                        "raw_text": "KJFK 081600Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "abc123"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 16, 30, tzinfo=timezone.utc),
            )
            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["rows_parsed"], 3)
            self.assertEqual(summary["station_timezone_mappings"], 1)
            self.assertEqual(summary["station_local_day_max_count"], 2)
            self.assertEqual(summary["station_local_day_min_count"], 2)

            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            max_by_day = state_payload["max_temp_c_by_station_local_day"]
            min_by_day = state_payload["min_temp_c_by_station_local_day"]
            interval_stats = state_payload.get("station_observation_interval_stats", {})
            self.assertEqual(max_by_day["KJFK|2026-04-07"], 10.0)
            self.assertEqual(max_by_day["KJFK|2026-04-08"], 20.0)
            self.assertEqual(min_by_day["KJFK|2026-04-07"], 10.0)
            self.assertEqual(min_by_day["KJFK|2026-04-08"], 18.0)
            self.assertEqual(
                state_payload["latest_observation_by_station"]["KJFK"]["report_type"],
                "METAR",
            )
            self.assertEqual(
                state_payload["latest_observation_by_station"]["KJFK"]["previous_report_type"],
                "SPECI",
            )
            self.assertEqual(
                state_payload["latest_observation_by_station"]["KJFK"]["previous_observation_time_utc"],
                "2026-04-08T14:00:00+00:00",
            )
            self.assertEqual(
                state_payload["latest_observation_by_station"]["KJFK"]["previous_temp_c"],
                20.0,
            )
            self.assertIn("KJFK", interval_stats)
            self.assertEqual(interval_stats["KJFK"]["sample_count"], 1)
            self.assertEqual(interval_stats["KJFK"]["latest_interval_minutes"], 120.0)

    def test_run_ingest_uses_local_standard_day_key_during_dst(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\nKJFK,America/New_York\n",
                encoding="utf-8",
            )

            # 2026-06-01 04:30Z is 00:30 local daylight time in New York.
            # Local standard-time bucket should be previous day (2026-05-31).
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "SPECI",
                        "observation_time": "2026-06-01T04:30:00Z",
                        "temp_c": "26.0",
                        "raw_text": "KJFK 010430Z ...",
                    }
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "dst-key"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 6, 1, 4, 31, tzinfo=timezone.utc),
            )
            self.assertEqual(summary["status"], "ready")

            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            max_by_day = state_payload["max_temp_c_by_station_local_day"]
            min_by_day = state_payload["min_temp_c_by_station_local_day"]
            self.assertEqual(max_by_day["KJFK|2026-05-31"], 26.0)
            self.assertNotIn("KJFK|2026-06-01", max_by_day)
            self.assertEqual(min_by_day["KJFK|2026-05-31"], 26.0)

    def test_run_ingest_reuses_station_timezone_from_existing_state_when_specs_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            state_path = output_dir / "kalshi_temperature_metar_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KJFK": {
                                "observation_time_utc": "2026-05-31T22:00:00+00:00",
                                "temp_c": 24.0,
                                "report_type": "METAR",
                                "payload_hash": "prior",
                                "timezone_name": "America/New_York",
                                "local_date": "2026-05-31",
                                "captured_at": "2026-05-31T22:05:00+00:00",
                            }
                        },
                        "max_temp_c_by_station_local_day": {},
                        "min_temp_c_by_station_local_day": {},
                    }
                ),
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-06-01T04:30:00Z",
                        "temp_c": "26.0",
                        "raw_text": "KJFK 010430Z ...",
                    }
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "reuse-state-tz"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=None,
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 6, 1, 4, 31, tzinfo=timezone.utc),
            )
            self.assertEqual(summary["status"], "ready")

            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            max_by_day = state_payload["max_temp_c_by_station_local_day"]
            min_by_day = state_payload["min_temp_c_by_station_local_day"]
            self.assertEqual(max_by_day["KJFK|2026-05-31"], 26.0)
            self.assertNotIn("KJFK|2026-06-01", max_by_day)
            self.assertEqual(min_by_day["KJFK|2026-05-31"], 26.0)

    def test_parse_metar_cache_csv_gz_rejects_non_finite_temperature_values(self) -> None:
        blob = _build_metar_cache_blob(
            [
                {
                    "station_id": "KJFK",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T14:00:00Z",
                    "temp_c": "NaN",
                    "raw_text": "KJFK 081400Z ...",
                },
                {
                    "station_id": "KJFK",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T15:00:00Z",
                    "temp_c": "Infinity",
                    "raw_text": "KJFK 081500Z ...",
                },
                {
                    "station_id": "KJFK",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T16:00:00Z",
                    "temp_c": "21.0",
                    "raw_text": "KJFK 081600Z ...",
                },
            ]
        )
        parsed = parse_metar_cache_csv_gz(blob)
        self.assertEqual(parsed["status"], "ready_partial")
        self.assertEqual(len(parsed["rows"]), 3)
        self.assertEqual(len(parsed["errors"]), 2)
        self.assertIn("row_0:invalid_temp_c", parsed["errors"])
        self.assertIn("row_1:invalid_temp_c", parsed["errors"])
        self.assertIsNone(parsed["rows"][0]["temp_c"])
        self.assertIsNone(parsed["rows"][1]["temp_c"])
        self.assertEqual(parsed["rows"][2]["temp_c"], 21.0)

    def test_parse_metar_cache_csv_gz_rejects_out_of_range_temperature_values(self) -> None:
        blob = _build_metar_cache_blob(
            [
                {
                    "station_id": "KJFK",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T14:00:00Z",
                    "temp_c": "-120.0",
                    "raw_text": "KJFK 081400Z ...",
                },
                {
                    "station_id": "KJFK",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T15:00:00Z",
                    "temp_c": "85.0",
                    "raw_text": "KJFK 081500Z ...",
                },
                {
                    "station_id": "KJFK",
                    "report_type": "METAR",
                    "observation_time": "2026-04-08T16:00:00Z",
                    "temp_c": "21.0",
                    "raw_text": "KJFK 081600Z ...",
                },
            ]
        )
        parsed = parse_metar_cache_csv_gz(blob)
        self.assertEqual(parsed["status"], "ready_partial")
        self.assertEqual(len(parsed["rows"]), 3)
        self.assertEqual(len(parsed["errors"]), 2)
        self.assertIn("row_0:invalid_temp_c", parsed["errors"])
        self.assertIn("row_1:invalid_temp_c", parsed["errors"])
        self.assertIsNone(parsed["rows"][0]["temp_c"])
        self.assertIsNone(parsed["rows"][1]["temp_c"])
        self.assertEqual(parsed["rows"][2]["temp_c"], 21.0)

    def test_run_ingest_ignores_non_finite_temperature_for_station_day_extremes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\nKJFK,America/New_York\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T14:00:00Z",
                        "temp_c": "NaN",
                        "raw_text": "KJFK 081400Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T16:00:00Z",
                        "temp_c": "20.0",
                        "raw_text": "KJFK 081600Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "finite-only"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 16, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready_partial")
            self.assertEqual(summary["parse_errors_count"], 1)
            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            max_by_day = state_payload["max_temp_c_by_station_local_day"]
            min_by_day = state_payload["min_temp_c_by_station_local_day"]
            self.assertEqual(max_by_day["KJFK|2026-04-08"], 20.0)
            self.assertEqual(min_by_day["KJFK|2026-04-08"], 20.0)

    def test_run_ingest_ignores_out_of_range_temperature_for_station_day_extremes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\nKJFK,America/New_York\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T14:00:00Z",
                        "temp_c": "20.0",
                        "raw_text": "KJFK 081400Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T16:00:00Z",
                        "temp_c": "85.0",
                        "raw_text": "KJFK 081600Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "finite-range-only"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 16, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready_partial")
            self.assertEqual(summary["parse_errors_count"], 2)
            self.assertIn("row_1:latest_update_skipped_missing_temp_c", summary["parse_errors"])
            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            max_by_day = state_payload["max_temp_c_by_station_local_day"]
            min_by_day = state_payload["min_temp_c_by_station_local_day"]
            self.assertEqual(max_by_day["KJFK|2026-04-08"], 20.0)
            self.assertEqual(min_by_day["KJFK|2026-04-08"], 20.0)

    def test_run_ingest_duplicate_timestamp_invalid_temp_does_not_clobber_latest_valid_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\nKJFK,America/New_York\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T14:00:00Z",
                        "temp_c": "20.0",
                        "raw_text": "KJFK 081400Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T14:00:00Z",
                        "temp_c": "NaN",
                        "raw_text": "SPECI KJFK 081400Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "same-ts-invalid-dup"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 14, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready_partial")
            self.assertEqual(summary["parse_errors_count"], 1)
            self.assertEqual(summary["station_updates"], 1)

            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            latest = state_payload["latest_observation_by_station"]["KJFK"]
            self.assertEqual(latest["observation_time_utc"], "2026-04-08T14:00:00+00:00")
            self.assertEqual(latest["temp_c"], 20.0)
            self.assertEqual(latest["report_type"], "METAR")
            self.assertNotIn("previous_observation_time_utc", latest)

    def test_run_ingest_newer_invalid_temp_does_not_refresh_latest_observation_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\nKJFK,America/New_York\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T14:00:00Z",
                        "temp_c": "20.0",
                        "raw_text": "KJFK 081400Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T14:30:00Z",
                        "temp_c": "NaN",
                        "raw_text": "SPECI KJFK 081430Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "newer-invalid-temp-no-refresh"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 15, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready_partial")
            self.assertIn("row_1:invalid_temp_c", summary["parse_errors"])
            self.assertIn("row_1:latest_update_skipped_missing_temp_c", summary["parse_errors"])
            self.assertEqual(summary["station_updates"], 1)

            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            latest = state_payload["latest_observation_by_station"]["KJFK"]
            self.assertEqual(latest["observation_time_utc"], "2026-04-08T14:00:00+00:00")
            self.assertEqual(latest["temp_c"], 20.0)
            self.assertEqual(latest["report_type"], "METAR")
            self.assertNotIn("previous_observation_time_utc", latest)

    def test_run_ingest_skips_stale_and_future_observations_for_state_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\nKJFK,America/New_York\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-04T16:00:00Z",
                        "temp_c": "9.0",
                        "raw_text": "KJFK 041600Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T14:00:00Z",
                        "temp_c": "20.0",
                        "raw_text": "KJFK 081400Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T17:00:00Z",
                        "temp_c": "27.0",
                        "raw_text": "SPECI KJFK 081700Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "stale-future-guard"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 16, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready_partial")
            self.assertEqual(summary["parse_errors_count"], 2)
            self.assertIn("row_0:stale_observation_time", summary["parse_errors"])
            self.assertIn("row_2:future_observation_time", summary["parse_errors"])
            self.assertEqual(int(summary["stale_or_future_row_count"]), 2)
            self.assertEqual(int(summary["timestamp_leakage_row_count"]), 2)
            self.assertTrue(bool(summary["timestamp_leakage_detected"]))
            self.assertEqual(int(summary["stale_observation_row_count"]), 1)
            self.assertEqual(int(summary["future_observation_row_count"]), 1)
            self.assertTrue(bool(summary["timestamp_leakage_critical"]))
            self.assertEqual(summary["timestamp_leakage_status"], "critical")
            self.assertEqual(summary["leakage_status"], "critical")
            self.assertIn("timestamp_leakage_ratio_critical", summary["leakage_flags"])

            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            latest = state_payload["latest_observation_by_station"]["KJFK"]
            self.assertEqual(latest["observation_time_utc"], "2026-04-08T14:00:00+00:00")
            self.assertEqual(latest["temp_c"], 20.0)

            max_by_day = state_payload["max_temp_c_by_station_local_day"]
            min_by_day = state_payload["min_temp_c_by_station_local_day"]
            self.assertNotIn("KJFK|2026-04-04", max_by_day)
            self.assertEqual(max_by_day["KJFK|2026-04-08"], 20.0)
            self.assertEqual(min_by_day["KJFK|2026-04-08"], 20.0)

    def test_run_ingest_quality_ready_for_healthy_feed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\n"
                "KJFK,America/New_York\n"
                "KLAX,America/Los_Angeles\n"
                "KORD,America/Chicago\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:10:00Z",
                        "temp_c": "20.0",
                        "raw_text": "KJFK 081610Z ...",
                    },
                    {
                        "station_id": "KLAX",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T16:08:00Z",
                        "temp_c": "17.5",
                        "raw_text": "SPECI KLAX 081608Z ...",
                    },
                    {
                        "station_id": "KORD",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:05:00Z",
                        "temp_c": "14.0",
                        "raw_text": "KORD 081605Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "quality-healthy"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 16, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["quality_status"], "ready")
            self.assertIn(summary["quality_grade"], {"excellent", "good"})
            self.assertGreaterEqual(float(summary["quality_score"]), 0.9)
            self.assertEqual(summary["usable_latest_station_count"], 3)
            self.assertAlmostEqual(float(summary["fresh_station_coverage_ratio"]), 1.0, places=6)
            self.assertAlmostEqual(float(summary["stale_or_future_row_ratio"]), 0.0, places=6)
            self.assertLessEqual(int(summary["quality_signal_count"]), 20)
            self.assertEqual(int(summary["quality_signal_count"]), len(summary["quality_signals"]))

    def test_run_ingest_quality_degraded_from_stale_and_invalid_temp_pressure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\n"
                "KJFK,America/New_York\n"
                "KLAX,America/Los_Angeles\n"
                "KSEA,America/Los_Angeles\n"
                "KORD,America/Chicago\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:00:00Z",
                        "temp_c": "19.0",
                        "raw_text": "KJFK 081600Z ...",
                    },
                    {
                        "station_id": "KLAX",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:05:00Z",
                        "temp_c": "18.0",
                        "raw_text": "KLAX 081605Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-01T15:00:00Z",
                        "temp_c": "17.0",
                        "raw_text": "SPECI KJFK 011500Z ...",
                    },
                    {
                        "station_id": "KLAX",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-01T16:00:00Z",
                        "temp_c": "16.5",
                        "raw_text": "SPECI KLAX 011600Z ...",
                    },
                    {
                        "station_id": "KSEA",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:07:00Z",
                        "temp_c": "NaN",
                        "raw_text": "KSEA 081607Z ...",
                    },
                    {
                        "station_id": "KSEA",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T16:09:00Z",
                        "temp_c": "Infinity",
                        "raw_text": "SPECI KSEA 081609Z ...",
                    },
                    {
                        "station_id": "KORD",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:15:00Z",
                        "temp_c": "15.5",
                        "raw_text": "KORD 081615Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "quality-degraded"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 16, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["quality_status"], "degraded")
            self.assertEqual(summary["quality_grade"], "degraded")
            self.assertLess(float(summary["quality_score"]), 0.8)
            self.assertGreaterEqual(float(summary["stale_or_future_row_ratio"]), 0.2)
            self.assertIn("parse_error_rate_elevated", summary["quality_signals"])
            self.assertIn("stale_or_future_ratio_elevated", summary["quality_signals"])
            self.assertIn("fresh_station_coverage_low", summary["quality_signals"])
            self.assertEqual(int(summary["quality_signal_count"]), len(summary["quality_signals"]))
            self.assertTrue(bool(summary["fresh_station_coverage_low"]))
            self.assertFalse(bool(summary["fresh_station_coverage_critical"]))
            self.assertTrue(bool(summary["fresh_station_coverage_degraded"]))
            self.assertEqual(summary["fresh_station_coverage_status"], "low")
            self.assertEqual(summary["leakage_status"], "elevated")

    def test_run_ingest_quality_blocked_when_no_usable_latest_stations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\n"
                "KJFK,America/New_York\n"
                "KLAX,America/Los_Angeles\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "KJFK",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:00:00Z",
                        "temp_c": "NaN",
                        "raw_text": "KJFK 081600Z ...",
                    },
                    {
                        "station_id": "KLAX",
                        "report_type": "SPECI",
                        "observation_time": "2026-04-08T16:02:00Z",
                        "temp_c": "Infinity",
                        "raw_text": "SPECI KLAX 081602Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "quality-blocked-no-usable"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 16, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["quality_status"], "blocked")
            self.assertEqual(summary["quality_grade"], "critical")
            self.assertEqual(summary["usable_latest_station_count"], 0)
            self.assertAlmostEqual(float(summary["fresh_station_coverage_ratio"]), 0.0, places=6)
            self.assertIn("no_usable_latest_stations", summary["quality_signals"])
            self.assertIn("no_fresh_usable_stations", summary["quality_signals"])

    def test_run_ingest_emits_missing_station_leakage_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir)
            specs_csv = output_dir / "specs.csv"
            specs_csv.write_text(
                "settlement_station,settlement_timezone\nKLAX,America/Los_Angeles\n",
                encoding="utf-8",
            )
            blob = _build_metar_cache_blob(
                [
                    {
                        "station_id": "",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:00:00Z",
                        "temp_c": "19.0",
                        "raw_text": "XXXX 081600Z ...",
                    },
                    {
                        "station_id": "KLAX",
                        "report_type": "METAR",
                        "observation_time": "",
                        "temp_c": "18.0",
                        "raw_text": "KLAX 081605Z ...",
                    },
                    {
                        "station_id": "",
                        "report_type": "SPECI",
                        "observation_time": "",
                        "temp_c": "17.0",
                        "raw_text": "XXXX 081610Z ...",
                    },
                    {
                        "station_id": "KLAX",
                        "report_type": "METAR",
                        "observation_time": "2026-04-08T16:12:00Z",
                        "temp_c": "17.5",
                        "raw_text": "KLAX 081612Z ...",
                    },
                ]
            )

            def fake_http_get_bytes(url: str, timeout_seconds: float):
                self.assertIn("metars.cache.csv.gz", url)
                return (200, blob, {"etag": "missing-station-diagnostics"})

            summary = run_kalshi_temperature_metar_ingest(
                output_dir=str(output_dir),
                specs_csv=str(specs_csv),
                http_get_bytes=fake_http_get_bytes,
                now=datetime(2026, 4, 8, 16, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["source_row_count"], 4)
            self.assertEqual(summary["missing_station_count"], 2)
            self.assertEqual(summary["missing_observation_time_count"], 2)
            self.assertEqual(summary["raw_rows_missing_station_or_observation_time_count"], 3)
            self.assertEqual(summary["missing_station_or_observation_time_count"], 3)
            self.assertAlmostEqual(
                float(summary["missing_station_or_observation_time_ratio"]),
                0.75,
                places=6,
            )
            self.assertTrue(bool(summary["missing_station_or_observation_time_detected"]))
            self.assertTrue(bool(summary["missing_station_or_observation_time_warn"]))
            self.assertTrue(bool(summary["missing_station_or_observation_time_critical"]))
            self.assertEqual(summary["missing_station_or_observation_time_status"], "critical")
            self.assertFalse(bool(summary["timestamp_leakage_detected"]))
            self.assertIn(
                "missing_station_or_observation_time_ratio_critical",
                summary["quality_signals"],
            )
            self.assertIn(
                "missing_station_or_observation_time_ratio_critical",
                summary["leakage_flags"],
            )


if __name__ == "__main__":
    unittest.main()
