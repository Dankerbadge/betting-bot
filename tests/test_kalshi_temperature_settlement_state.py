from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import unittest

from betbot.kalshi_temperature_settlement_state import run_kalshi_temperature_settlement_state


class KalshiTemperatureSettlementStateTests(unittest.TestCase):
    def test_builds_underlyings_and_blocks_past_dates(self) -> None:
        with self.subTest("build settlement state"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmp:
                specs_csv = f"{tmp}/kalshi_temperature_contract_specs_20260410_000000.csv"
                with open(specs_csv, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "series_ticker",
                            "event_ticker",
                            "market_ticker",
                            "contract_family",
                            "settlement_station",
                            "settlement_timezone",
                            "target_date_local",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "series_ticker": "KXHIGHAUS",
                            "event_ticker": "KXHIGHAUS-26APR10",
                            "market_ticker": "KXHIGHAUS-26APR10-T84",
                            "contract_family": "daily_temperature",
                            "settlement_station": "KAUS",
                            "settlement_timezone": "UTC",
                            "target_date_local": "2026-04-10",
                        }
                    )
                    writer.writerow(
                        {
                            "series_ticker": "KXHIGHCHI",
                            "event_ticker": "KXHIGHCHI-26APR09",
                            "market_ticker": "KXHIGHCHI-26APR09-B65.5",
                            "contract_family": "daily_temperature",
                            "settlement_station": "KMDW",
                            "settlement_timezone": "UTC",
                            "target_date_local": "2026-04-09",
                        }
                    )

                constraint_csv = f"{tmp}/kalshi_temperature_constraint_scan_20260410_000000.csv"
                with open(constraint_csv, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "market_ticker",
                            "observed_max_settlement_quantized",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "market_ticker": "KXHIGHAUS-26APR10-T84",
                            "observed_max_settlement_quantized": "82",
                        }
                    )

                summary = run_kalshi_temperature_settlement_state(
                    specs_csv=specs_csv,
                    constraint_csv=constraint_csv,
                    output_dir=tmp,
                    now=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
                    final_report_lookup_enabled=False,
                )

                self.assertEqual(summary["status"], "ready")
                self.assertEqual(summary["underlying_count"], 2)
                self.assertEqual(summary["blocked_underlyings"], 1)
                self.assertTrue(summary.get("output_file"))

                underlyings = summary["underlyings"]
                key_today = "KXHIGHAUS|KAUS|2026-04-10"
                key_past = "KXHIGHCHI|KMDW|2026-04-09"

                self.assertIn(key_today, underlyings)
                self.assertIn(key_past, underlyings)

                self.assertEqual(underlyings[key_today]["state"], "intraday_unfinalized")
                self.assertTrue(underlyings[key_today]["allow_new_orders"])
                self.assertEqual(underlyings[key_today]["fast_truth_value"], 82.0)

                self.assertEqual(underlyings[key_past]["state"], "pending_final_report")
                self.assertFalse(underlyings[key_past]["allow_new_orders"])

    def test_uses_final_report_lookup_for_past_underlying(self) -> None:
        with self.subTest("final report lookup"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmp:
                specs_csv = f"{tmp}/kalshi_temperature_contract_specs_20260410_000000.csv"
                with open(specs_csv, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "series_ticker",
                            "event_ticker",
                            "market_ticker",
                            "contract_family",
                            "settlement_station",
                            "settlement_timezone",
                            "target_date_local",
                            "settlement_source_primary",
                            "settlement_source_fallback",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "series_ticker": "KXHIGHAUS",
                            "event_ticker": "KXHIGHAUS-26APR09",
                            "market_ticker": "KXHIGHAUS-26APR09-T84",
                            "contract_family": "daily_temperature",
                            "settlement_station": "KAUS",
                            "settlement_timezone": "UTC",
                            "target_date_local": "2026-04-09",
                            "settlement_source_primary": "NWS",
                            "settlement_source_fallback": "NCEI",
                        }
                    )

                def fake_lookup_runner(**kwargs):
                    self.assertEqual(kwargs["station_id"], "KAUS")
                    self.assertEqual(kwargs["target_date"], "2026-04-09")
                    return {
                        "status": "ready",
                        "data_source": "cdo_api_v2",
                        "http_status": 200,
                        "daily_sample": {
                            "date": "2026-04-09",
                            "tmax_f": 88.0,
                            "tmin_f": 61.0,
                        },
                    }

                summary = run_kalshi_temperature_settlement_state(
                    specs_csv=specs_csv,
                    output_dir=tmp,
                    now=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
                    final_report_lookup_enabled=True,
                    final_report_cache_ttl_minutes=60.0,
                    final_report_lookup_runner=fake_lookup_runner,
                )

                self.assertEqual(summary["status"], "ready")
                self.assertEqual(summary["final_report_lookup_attempted"], 1)
                self.assertEqual(summary["final_report_ready_count"], 1)
                key = "KXHIGHAUS|KAUS|2026-04-09"
                entry = summary["underlyings"][key]
                self.assertEqual(entry["state"], "final_report_available")
                self.assertEqual(entry["finalization_status"], "final_report_available")
                self.assertFalse(entry["allow_new_orders"])
                self.assertEqual(entry["final_truth_value"], 88.0)
                self.assertEqual(entry["final_report_lookup_status"], "ready")

    def test_falls_back_timezone_from_station_when_specs_timezone_blank(self) -> None:
        with self.subTest("settlement timezone fallback"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmp:
                specs_csv = f"{tmp}/kalshi_temperature_contract_specs_20260410_000000.csv"
                with open(specs_csv, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "series_ticker",
                            "event_ticker",
                            "market_ticker",
                            "contract_family",
                            "settlement_station",
                            "settlement_timezone",
                            "target_date_local",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "series_ticker": "KXHIGHAUS",
                            "event_ticker": "KXHIGHAUS-26APR10",
                            "market_ticker": "KXHIGHAUS-26APR10-T84",
                            "contract_family": "daily_temperature",
                            "settlement_station": "KAUS",
                            "settlement_timezone": "",
                            "target_date_local": "2026-04-10",
                        }
                    )

                summary = run_kalshi_temperature_settlement_state(
                    specs_csv=specs_csv,
                    output_dir=tmp,
                    # 03:30Z is still previous local day in America/Chicago.
                    now=datetime(2026, 4, 10, 3, 30, tzinfo=timezone.utc),
                    final_report_lookup_enabled=False,
                )

                key = "KXHIGHAUS|KAUS|2026-04-10"
                entry = summary["underlyings"][key]
                self.assertEqual(entry["settlement_timezone"], "America/Chicago")
                self.assertEqual(entry["state"], "pre_target_day")
                self.assertTrue(entry["allow_new_orders"])

    def test_final_report_lookup_timeout_degrades_without_crashing(self) -> None:
        with self.subTest("final report timeout handling"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmp:
                specs_csv = f"{tmp}/kalshi_temperature_contract_specs_20260410_000000.csv"
                with open(specs_csv, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "series_ticker",
                            "event_ticker",
                            "market_ticker",
                            "contract_family",
                            "settlement_station",
                            "settlement_timezone",
                            "target_date_local",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "series_ticker": "KXHIGHAUS",
                            "event_ticker": "KXHIGHAUS-26APR09",
                            "market_ticker": "KXHIGHAUS-26APR09-T84",
                            "contract_family": "daily_temperature",
                            "settlement_station": "KAUS",
                            "settlement_timezone": "America/Chicago",
                            "target_date_local": "2026-04-09",
                        }
                    )

                def timeout_lookup_runner(**_: object) -> dict[str, object]:
                    raise TimeoutError("simulated timeout")

                summary = run_kalshi_temperature_settlement_state(
                    specs_csv=specs_csv,
                    output_dir=tmp,
                    now=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
                    final_report_lookup_enabled=True,
                    final_report_lookup_runner=timeout_lookup_runner,
                )

                self.assertEqual(summary["status"], "ready")
                self.assertEqual(summary["final_report_lookup_attempted"], 1)
                self.assertEqual(summary["final_report_error_count"], 1)
                key = "KXHIGHAUS|KAUS|2026-04-09"
                entry = summary["underlyings"][key]
                self.assertEqual(entry["state"], "pending_final_report")
                self.assertEqual(entry["final_report_lookup_status"], "lookup_error")
                self.assertEqual(entry["reason"], "target_date_elapsed_final_report_lookup_error")

    def test_cached_station_mapping_missing_is_forced_to_refresh(self) -> None:
        with self.subTest("station_mapping_missing cache migration"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as tmp:
                specs_csv = f"{tmp}/kalshi_temperature_contract_specs_20260410_000000.csv"
                with open(specs_csv, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(
                        handle,
                        fieldnames=[
                            "series_ticker",
                            "event_ticker",
                            "market_ticker",
                            "contract_family",
                            "settlement_station",
                            "settlement_timezone",
                            "target_date_local",
                        ],
                    )
                    writer.writeheader()
                    writer.writerow(
                        {
                            "series_ticker": "KXHIGHAUS",
                            "event_ticker": "KXHIGHAUS-26APR09",
                            "market_ticker": "KXHIGHAUS-26APR09-T84",
                            "contract_family": "daily_temperature",
                            "settlement_station": "KAUS",
                            "settlement_timezone": "America/Chicago",
                            "target_date_local": "2026-04-09",
                        }
                    )

                cache_path = f"{tmp}/kalshi_temperature_settlement_final_reports_cache.json"
                cache_payload = {
                    "entries": {
                        "KXHIGHAUS|KAUS|2026-04-09": {
                            "looked_up_at": "2026-04-10T11:58:00+00:00",
                            "lookup": {
                                "status": "station_mapping_missing",
                                "error": "legacy behavior",
                            },
                        }
                    }
                }
                with open(cache_path, "w", encoding="utf-8") as handle:
                    json.dump(cache_payload, handle)

                calls = {"count": 0}

                def refreshed_lookup_runner(**kwargs: object) -> dict[str, object]:
                    calls["count"] += 1
                    return {
                        "status": "no_final_report",
                        "data_source": "access_data_service_v1",
                        "http_status": 200,
                        "daily_sample": {},
                    }

                summary = run_kalshi_temperature_settlement_state(
                    specs_csv=specs_csv,
                    output_dir=tmp,
                    now=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
                    final_report_lookup_enabled=True,
                    final_report_cache_ttl_minutes=60.0,
                    final_report_lookup_runner=refreshed_lookup_runner,
                )

                self.assertEqual(calls["count"], 1)
                self.assertEqual(summary["final_report_lookup_attempted"], 1)
                self.assertEqual(summary["final_report_lookup_cache_hit"], 0)
                key = "KXHIGHAUS|KAUS|2026-04-09"
                entry = summary["underlyings"][key]
                self.assertEqual(entry["final_report_lookup_status"], "no_final_report")
                self.assertEqual(entry["state"], "pending_final_report")


if __name__ == "__main__":
    unittest.main()
