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
    fieldnames = ["station_id", "observation_time", "temp_c", "raw_text"]
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
                    "observation_time": "2026-04-08T14:00:00Z",
                    "temp_c": "19.0",
                    "raw_text": "KJFK 081400Z ...",
                },
                {
                    "station_id": "KLAX",
                    "observation_time": "2026-04-08T14:00:00Z",
                    "temp_c": "17.0",
                    "raw_text": "KLAX 081400Z ...",
                },
            ]
        )
        parsed = parse_metar_cache_csv_gz(blob)
        self.assertEqual(parsed["status"], "ready")
        self.assertEqual(len(parsed["rows"]), 2)
        self.assertEqual(len(parsed["errors"]), 0)
        self.assertEqual(parsed["rows"][0]["station_id"], "KJFK")

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
                        "observation_time": "2026-04-08T03:00:00Z",
                        "temp_c": "10.0",
                        "raw_text": "KJFK 080300Z ...",
                    },
                    {
                        "station_id": "KJFK",
                        "observation_time": "2026-04-08T14:00:00Z",
                        "temp_c": "20.0",
                        "raw_text": "KJFK 081400Z ...",
                    },
                    {
                        "station_id": "KJFK",
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

            state_payload = json.loads(Path(summary["state_file"]).read_text(encoding="utf-8"))
            max_by_day = state_payload["max_temp_c_by_station_local_day"]
            self.assertEqual(max_by_day["KJFK|2026-04-07"], 10.0)
            self.assertEqual(max_by_day["KJFK|2026-04-08"], 20.0)


if __name__ == "__main__":
    unittest.main()
