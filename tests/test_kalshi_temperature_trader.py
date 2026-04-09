from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest

from betbot.kalshi_temperature_trader import (
    TemperaturePolicyGate,
    build_temperature_trade_intents,
    revalidate_temperature_trade_intents,
    run_kalshi_temperature_trader,
    run_kalshi_temperature_shadow_watch,
)


class KalshiTemperatureTraderTests(unittest.TestCase):
    def test_build_temperature_trade_intents_and_gate_approve(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_max_settlement_quantized": "74",
                "settlement_confidence_score": "0.92",
            }
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "close_time": "2026-04-09T00:00:00Z",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "rules_primary": "Highest temperature in local day at KNYC.",
            }
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T11:50:00Z",
                    "temp_c": 24.2,
                }
            },
        }
        market_sequences = {"KXHIGHNY-26APR08-B72": 41}

        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences=market_sequences,
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.96,
            now=now,
        )
        self.assertEqual(len(intents), 1)
        intent = intents[0]
        self.assertEqual(intent.side, "no")
        self.assertEqual(intent.market_snapshot_seq, 41)
        self.assertEqual(intent.metar_snapshot_sha, "abc123def456")
        self.assertEqual(intent.max_entry_price_dollars, 0.96)

        decisions = TemperaturePolicyGate(
            min_settlement_confidence=0.7,
            max_metar_age_minutes=20.0,
            max_intents_per_underlying=1,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=intents)
        self.assertEqual(len(decisions), 1)
        self.assertTrue(decisions[0].approved)
        self.assertEqual(decisions[0].decision_reason, "approved")

    def test_policy_gate_blocks_stale_metar(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_likely_locked",
                "constraint_reason": "Observed max already meets floor",
                "observed_max_settlement_quantized": "72",
                "settlement_confidence_score": "0.95",
            }
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "close_time": "2026-04-09T00:00:00Z",
                "rules_primary": "Highest temperature in local day at KNYC.",
            }
        }
        metar_context = {
            "raw_sha256": "abc123def456",
            "latest_by_station": {
                "KNYC": {
                    "observation_time_utc": "2026-04-08T09:30:00Z",
                    "temp_c": 22.0,
                }
            },
        }

        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context=metar_context,
            market_sequences={"KXHIGHNY-26APR08-B72": 10},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.9,
            no_max_entry_price_dollars=0.9,
            now=now,
        )
        decisions = TemperaturePolicyGate(
            min_settlement_confidence=0.5,
            max_metar_age_minutes=20.0,
            require_market_snapshot_seq=True,
            require_metar_snapshot_sha=True,
        ).evaluate(intents=intents)
        self.assertEqual(len(decisions), 1)
        self.assertFalse(decisions[0].approved)
        self.assertEqual(decisions[0].decision_reason, "metar_observation_stale")
        self.assertIn("metar_observation_stale", decisions[0].decision_notes)

    def test_run_kalshi_temperature_trader_builds_plan_and_calls_execute(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text("KALSHI_ENV=prod\n", encoding="utf-8")

            specs_csv = base / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "close_time",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "rules_primary",
                        "rules_secondary",
                        "local_day_boundary",
                        "observation_window_local_start",
                        "observation_window_local_end",
                        "threshold_expression",
                        "contract_terms_url",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "series_ticker": "KXHIGHNY",
                        "event_ticker": "KXHIGHNY-26APR08",
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "market_title": "72F or above",
                        "close_time": "2026-04-09T00:00:00Z",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "target_date_local": "2026-04-08",
                        "rules_primary": "Highest temperature in local day at KNYC.",
                        "rules_secondary": "",
                        "local_day_boundary": "local_day",
                        "observation_window_local_start": "00:00",
                        "observation_window_local_end": "23:59",
                        "threshold_expression": "at_most:72",
                        "contract_terms_url": "https://example.test/terms",
                        "settlement_confidence_score": "0.92",
                    }
                )

            constraint_csv = base / "constraints.csv"
            with constraint_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "scanned_at",
                        "source_specs_csv",
                        "series_ticker",
                        "event_ticker",
                        "market_ticker",
                        "market_title",
                        "settlement_station",
                        "settlement_timezone",
                        "target_date_local",
                        "settlement_unit",
                        "settlement_precision",
                        "threshold_expression",
                        "constraint_status",
                        "constraint_reason",
                        "observed_max_settlement_raw",
                        "observed_max_settlement_quantized",
                        "observations_for_date",
                        "snapshot_status",
                        "settlement_confidence_score",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "scanned_at": now.isoformat(),
                        "source_specs_csv": str(specs_csv),
                        "series_ticker": "KXHIGHNY",
                        "event_ticker": "KXHIGHNY-26APR08",
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "market_title": "72F or above",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "target_date_local": "2026-04-08",
                        "settlement_unit": "fahrenheit",
                        "settlement_precision": "whole_degree",
                        "threshold_expression": "at_most:72",
                        "constraint_status": "yes_impossible",
                        "constraint_reason": "Observed max 74 exceeds at_most threshold 72.",
                        "observed_max_settlement_raw": "74",
                        "observed_max_settlement_quantized": "74",
                        "observations_for_date": "12",
                        "snapshot_status": "ready",
                        "settlement_confidence_score": "0.92",
                    }
                )

            metar_state = base / "metar_state.json"
            metar_state.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KNYC": {
                                "observation_time_utc": "2026-04-08T11:55:00Z",
                                "temp_c": 24.5,
                            }
                        },
                        "max_temp_c_by_station_local_day": {},
                    }
                ),
                encoding="utf-8",
            )
            metar_summary = base / "metar_summary.json"
            metar_summary.write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "captured_at": now.isoformat(),
                        "raw_sha256": "feedbead1234",
                        "state_file": str(metar_state),
                    }
                ),
                encoding="utf-8",
            )

            ws_state = base / "ws_state.json"
            ws_state.write_text(
                json.dumps(
                    {
                        "summary": {
                            "status": "ready",
                            "market_count": 1,
                            "desynced_market_count": 0,
                            "last_event_at": now.isoformat(),
                        },
                        "markets": {
                            "KXHIGHNY-26APR08-B72": {
                                "sequence": 22,
                                "updated_at_utc": now.isoformat(),
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            execute_capture: dict[str, object] = {}

            def fake_micro_execute_runner(**kwargs: object) -> dict[str, object]:
                plan_runner = kwargs.get("plan_runner")
                self.assertTrue(callable(plan_runner))
                plan_summary = plan_runner()
                execute_capture["plan_summary"] = plan_summary
                return {
                    "status": "dry_run",
                    "attempts": [],
                    "output_csv": str(base / "execute.csv"),
                    "output_file": str(base / "execute_summary.json"),
                }

            summary = run_kalshi_temperature_trader(
                env_file=str(env_file),
                output_dir=str(base),
                specs_csv=str(specs_csv),
                constraint_csv=str(constraint_csv),
                metar_summary_json=str(metar_summary),
                ws_state_json=str(ws_state),
                micro_execute_runner=fake_micro_execute_runner,
                now=now,
            )

            self.assertEqual(summary["status"], "dry_run")
            self.assertEqual(summary["intent_summary"]["intents_total"], 1)
            self.assertEqual(summary["intent_summary"]["intents_approved"], 1)
            self.assertTrue(Path(summary["intent_summary"]["output_csv"]).exists())
            self.assertTrue(Path(summary["plan_summary"]["output_csv"]).exists())

            plan_summary = execute_capture["plan_summary"]
            self.assertEqual(plan_summary["planned_orders"], 1)
            order = plan_summary["orders"][0]
            self.assertEqual(order["side"], "no")
            payload = order["order_payload_preview"]
            self.assertEqual(payload["side"], "no")
            self.assertIn("order_group_id", payload)
            self.assertTrue(str(payload.get("client_order_id", "")).startswith("temp-"))

    def test_revalidate_temperature_trade_intents_detects_sequence_and_metar_changes(self) -> None:
        now = datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)
        constraint_rows = [
            {
                "series_ticker": "KXHIGHNY",
                "event_ticker": "KXHIGHNY-26APR08",
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "market_title": "72F or above",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "constraint_status": "yes_impossible",
                "constraint_reason": "Observed max already above threshold",
                "observed_max_settlement_quantized": "74",
                "settlement_confidence_score": "0.92",
            }
        ]
        specs_by_ticker = {
            "KXHIGHNY-26APR08-B72": {
                "market_ticker": "KXHIGHNY-26APR08-B72",
                "close_time": "2026-04-09T00:00:00Z",
                "settlement_station": "KNYC",
                "settlement_timezone": "America/New_York",
                "target_date_local": "2026-04-08",
                "rules_primary": "Highest temperature in local day at KNYC.",
            }
        }
        intents = build_temperature_trade_intents(
            constraint_rows=constraint_rows,
            specs_by_ticker=specs_by_ticker,
            metar_context={
                "raw_sha256": "sha-old",
                "latest_by_station": {"KNYC": {"observation_time_utc": "2026-04-08T11:50:00Z", "temp_c": 24.2}},
            },
            market_sequences={"KXHIGHNY-26APR08-B72": 10},
            policy_version="temperature_policy_v1",
            contracts_per_order=1,
            yes_max_entry_price_dollars=0.95,
            no_max_entry_price_dollars=0.95,
            now=now,
        )
        self.assertEqual(len(intents), 1)

        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            specs_csv = base / "specs.csv"
            with specs_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_ticker",
                        "rules_primary",
                        "rules_secondary",
                        "settlement_station",
                        "settlement_timezone",
                        "local_day_boundary",
                        "observation_window_local_start",
                        "observation_window_local_end",
                        "threshold_expression",
                        "contract_terms_url",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_ticker": "KXHIGHNY-26APR08-B72",
                        "rules_primary": "Updated rule text",
                        "rules_secondary": "",
                        "settlement_station": "KNYC",
                        "settlement_timezone": "America/New_York",
                        "local_day_boundary": "local_day",
                        "observation_window_local_start": "00:00",
                        "observation_window_local_end": "23:59",
                        "threshold_expression": "at_most:72",
                        "contract_terms_url": "https://example.test/terms",
                    }
                )
            metar_state = base / "metar_state.json"
            metar_state.write_text(
                json.dumps(
                    {
                        "latest_observation_by_station": {
                            "KNYC": {"observation_time_utc": "2026-04-08T11:58:00Z", "temp_c": 24.5}
                        }
                    }
                ),
                encoding="utf-8",
            )
            metar_summary = base / "metar_summary.json"
            metar_summary.write_text(
                json.dumps({"raw_sha256": "sha-new", "state_file": str(metar_state)}),
                encoding="utf-8",
            )
            ws_state = base / "ws_state.json"
            ws_state.write_text(
                json.dumps({"markets": {"KXHIGHNY-26APR08-B72": {"sequence": 11}}}),
                encoding="utf-8",
            )

            valid, invalid, meta = revalidate_temperature_trade_intents(
                intents=intents,
                output_dir=str(base),
                specs_csv=str(specs_csv),
                metar_summary_json=str(metar_summary),
                metar_state_json=str(metar_state),
                ws_state_json=str(ws_state),
                require_market_snapshot_seq=True,
                require_metar_snapshot_sha=False,
            )

            self.assertEqual(len(valid), 0)
            self.assertEqual(len(invalid), 1)
            reasons = set(invalid[0]["reasons"])
            self.assertIn("market_snapshot_seq_changed", reasons)
            self.assertIn("metar_observation_advanced", reasons)
            self.assertIn("metar_snapshot_sha_changed", reasons)
            self.assertEqual(meta["metar_snapshot_sha"], "sha-new")

    def test_run_kalshi_temperature_shadow_watch_runs_multiple_cycles(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_trader_runner(**kwargs: object) -> dict[str, object]:
            calls.append(dict(kwargs))
            loop_index = len(calls)
            return {
                "status": "dry_run",
                "intent_summary": {
                    "intents_total": 2,
                    "intents_approved": 1,
                    "intents_revalidated": 1,
                    "revalidation_invalidated": 0,
                },
                "plan_summary": {"planned_orders": 1},
                "execute_summary": {"status": "dry_run", "output_file": f"/tmp/execute_{loop_index}.json"},
            }

        sleeps: list[float] = []
        summary = run_kalshi_temperature_shadow_watch(
            env_file=".env",
            output_dir="outputs",
            loops=2,
            sleep_between_loops_seconds=3.5,
            trader_runner=fake_trader_runner,
            sleep_fn=lambda seconds: sleeps.append(seconds),
            now=datetime(2026, 4, 9, 13, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(summary["loops_run"], 2)
        self.assertEqual(summary["mode"], "shadow")
        self.assertEqual(len(summary["cycle_summaries"]), 2)
        self.assertEqual(summary["cycle_status_counts"].get("dry_run"), 2)
        self.assertEqual(len(calls), 2)
        self.assertEqual(sleeps, [3.5])


if __name__ == "__main__":
    unittest.main()
