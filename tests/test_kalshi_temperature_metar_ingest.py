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
            self.assertEqual(summary["parse_errors_count"], 1)
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

            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            latest = state_payload["latest_observation_by_station"]["KJFK"]
            self.assertEqual(latest["observation_time_utc"], "2026-04-08T14:00:00+00:00")
            self.assertEqual(latest["temp_c"], 20.0)

            max_by_day = state_payload["max_temp_c_by_station_local_day"]
            min_by_day = state_payload["min_temp_c_by_station_local_day"]
            self.assertNotIn("KJFK|2026-04-04", max_by_day)
            self.assertEqual(max_by_day["KJFK|2026-04-08"], 20.0)
            self.assertEqual(min_by_day["KJFK|2026-04-08"], 20.0)


if __name__ == "__main__":
    unittest.main()
