import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import unittest

from betbot.kalshi_execution_journal import append_execution_events, load_execution_events
from betbot.kalshi_micro_reconcile import run_kalshi_micro_reconcile


class KalshiMicroReconcileTests(unittest.TestCase):
    def test_run_kalshi_micro_reconcile_handles_no_order_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                "KALSHI_ENV=prod\nBETBOT_JURISDICTION=new_jersey\nKALSHI_ACCESS_KEY_ID=key123\nKALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n",
                encoding="utf-8",
            )
            execute_summary = base / "kalshi_micro_execute_summary_test.json"
            execute_summary.write_text(json.dumps({"attempts": []}), encoding="utf-8")

            summary = run_kalshi_micro_reconcile(
                env_file=str(env_file),
                execute_summary_file=str(execute_summary),
                output_dir=str(base),
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "no_order_ids")
            self.assertTrue(Path(summary["output_csv"]).exists())
            self.assertTrue(Path(summary["output_file"]).exists())

    def test_run_kalshi_micro_reconcile_collects_order_and_position_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                "KALSHI_ENV=prod\nBETBOT_JURISDICTION=new_jersey\nKALSHI_ACCESS_KEY_ID=key123\nKALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n",
                encoding="utf-8",
            )
            execute_summary = base / "kalshi_micro_execute_summary_test.json"
            execute_summary.write_text(
                json.dumps({"attempts": [{"order_id": "order-123"}]}),
                encoding="utf-8",
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if method == "GET" and url.endswith("/portfolio/orders/order-123"):
                    return 200, {
                        "order": {
                            "order_id": "order-123",
                            "ticker": "KXTEST-1",
                            "client_order_id": "betbot-1",
                            "status": "resting",
                            "yes_price_dollars": "0.0200",
                            "fill_count_fp": "0.00",
                            "remaining_count_fp": "1.00",
                            "initial_count_fp": "1.00",
                            "maker_fill_cost_dollars": "0.0000",
                            "maker_fees_dollars": "0.0000",
                            "taker_fill_cost_dollars": "0.0000",
                            "taker_fees_dollars": "0.0000",
                            "created_time": "2026-03-27T21:00:00Z",
                            "last_update_time": "2026-03-27T21:00:05Z",
                        }
                    }
                if method == "GET" and url.endswith("/portfolio/orders/order-123/queue_position"):
                    return 200, {"queue_position_fp": "12.00"}
                if method == "GET" and "/portfolio/positions?" in url:
                    return 200, {
                        "market_positions": [
                            {
                                "ticker": "KXTEST-1",
                                "position_fp": "1.00",
                                "market_exposure_dollars": "0.0200",
                                "realized_pnl_dollars": "0.0000",
                                "fees_paid_dollars": "0.0000",
                                "resting_orders_count": 1,
                            }
                        ],
                        "event_positions": [],
                    }
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_reconcile(
                env_file=str(env_file),
                execute_summary_file=str(execute_summary),
                output_dir=str(base),
                http_request_json=fake_http_request_json,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["orders_requested"], 1)
            self.assertEqual(summary["orders_found"], 1)
            self.assertEqual(summary["status_counts"], {"resting": 1})
            self.assertEqual(summary["rows"][0]["queue_position_contracts"], 12.0)

    def test_run_kalshi_micro_reconcile_preserves_no_side_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            env_file.write_text(
                "KALSHI_ENV=prod\nBETBOT_JURISDICTION=new_jersey\nKALSHI_ACCESS_KEY_ID=key123\nKALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n",
                encoding="utf-8",
            )
            execute_summary = base / "kalshi_micro_execute_summary_test.json"
            execute_summary.write_text(
                json.dumps(
                    {
                        "attempts": [
                            {
                                "order_id": "order-no-1",
                                "planned_side": "no",
                                "planned_entry_price_dollars": 0.96,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if method == "GET" and url.endswith("/portfolio/orders/order-no-1"):
                    return 200, {
                        "order": {
                            "order_id": "order-no-1",
                            "ticker": "KXTEST-2",
                            "client_order_id": "betbot-2",
                            "status": "canceled",
                            "yes_price_dollars": "0.0400",
                            "fill_count_fp": "0.00",
                            "remaining_count_fp": "1.00",
                            "initial_count_fp": "1.00",
                            "created_time": "2026-03-27T21:00:00Z",
                            "last_update_time": "2026-03-27T21:00:05Z",
                        }
                    }
                if method == "GET" and "/portfolio/positions?" in url:
                    return 200, {"market_positions": [], "event_positions": []}
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_reconcile(
                env_file=str(env_file),
                execute_summary_file=str(execute_summary),
                output_dir=str(base),
                http_request_json=fake_http_request_json,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["rows"][0]["planned_side"], "no")
            self.assertEqual(summary["rows"][0]["yes_price_dollars"], 0.04)
            self.assertEqual(summary["rows"][0]["no_price_dollars"], 0.96)
            self.assertEqual(summary["rows"][0]["effective_price_dollars"], 0.96)
            self.assertEqual(summary["rows"][0]["planned_entry_price_dollars"], 0.96)

    def test_run_kalshi_micro_reconcile_backfills_partial_and_terminal_events_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            journal_path = base / "execution.sqlite3"
            env_file.write_text(
                "KALSHI_ENV=prod\nBETBOT_JURISDICTION=new_jersey\nKALSHI_ACCESS_KEY_ID=key123\nKALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n",
                encoding="utf-8",
            )
            execute_summary = base / "kalshi_micro_execute_summary_test.json"
            execute_summary.write_text(
                json.dumps(
                    {
                        "attempts": [
                            {
                                "order_id": "order-123",
                                "planned_side": "yes",
                                "planned_contracts": 1.0,
                                "planned_entry_price_dollars": 0.50,
                                "category": "Economics",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            append_execution_events(
                journal_db_path=journal_path,
                events=[
                    {
                        "run_id": "preseed",
                        "captured_at_utc": "2026-03-27T21:00:00+00:00",
                        "event_type": "partial_fill",
                        "market_ticker": "KXTEST-1",
                        "side": "yes",
                        "limit_price_dollars": 0.50,
                        "contracts_fp": 0.4,
                        "fee_dollars": 0.004,
                        "exchange_order_id": "order-123",
                    }
                ],
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if method == "GET" and url.endswith("/portfolio/orders/order-123"):
                    return 200, {
                        "order": {
                            "order_id": "order-123",
                            "ticker": "KXTEST-1",
                            "client_order_id": "betbot-1",
                            "status": "canceled",
                            "yes_price_dollars": "0.5000",
                            "fill_count_fp": "0.70",
                            "remaining_count_fp": "0.30",
                            "initial_count_fp": "1.00",
                            "maker_fees_dollars": "0.0070",
                            "taker_fees_dollars": "0.0000",
                            "created_time": "2026-03-27T21:00:00Z",
                            "last_update_time": "2026-03-27T21:03:00Z",
                        }
                    }
                if method == "GET" and "/portfolio/positions?" in url:
                    return 200, {
                        "market_positions": [
                            {
                                "ticker": "KXTEST-1",
                                "position_fp": "0.00",
                                "market_exposure_dollars": "0.0000",
                                "realized_pnl_dollars": "0.0500",
                                "fees_paid_dollars": "0.0070",
                                "resting_orders_count": 0,
                            }
                        ],
                        "event_positions": [],
                    }
                return 404, {"error": "not found"}

            first = run_kalshi_micro_reconcile(
                env_file=str(env_file),
                execute_summary_file=str(execute_summary),
                output_dir=str(base),
                execution_journal_db_path=str(journal_path),
                http_request_json=fake_http_request_json,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 5, tzinfo=timezone.utc),
            )
            second = run_kalshi_micro_reconcile(
                env_file=str(env_file),
                execute_summary_file=str(execute_summary),
                output_dir=str(base),
                execution_journal_db_path=str(journal_path),
                http_request_json=fake_http_request_json,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 6, tzinfo=timezone.utc),
            )

            self.assertEqual(first["status"], "ready")
            self.assertGreaterEqual(first["execution_journal_rows_written"], 4)
            self.assertEqual(second["execution_journal_rows_written"], 0)

            events = load_execution_events(
                journal_db_path=journal_path,
                exchange_order_id="order-123",
                limit=50,
            )
            event_types = [str(event.get("event_type")) for event in events]
            self.assertIn("partial_fill", event_types)
            self.assertIn("cancel_confirmed", event_types)
            self.assertIn("order_terminal", event_types)
            self.assertIn("settlement_outcome", event_types)

            partial_fills = [event for event in events if event.get("event_type") == "partial_fill"]
            # Existing 0.4 fill plus reconcile delta 0.3.
            total_logged_fill = sum(float(event.get("contracts_fp") or 0.0) for event in partial_fills)
            self.assertAlmostEqual(total_logged_fill, 0.7, places=6)

    def test_run_kalshi_micro_reconcile_writes_markout_snapshot_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            journal_path = base / "execution.sqlite3"
            history_csv = base / "kalshi_nonsports_history.csv"
            env_file.write_text(
                "KALSHI_ENV=prod\nBETBOT_JURISDICTION=new_jersey\nKALSHI_ACCESS_KEY_ID=key123\nKALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n",
                encoding="utf-8",
            )
            history_csv.write_text(
                (
                    "captured_at,market_ticker,yes_bid_dollars,yes_ask_dollars\n"
                    "2026-03-27T21:00:12+00:00,KXTEST-2,0.55,0.57\n"
                    "2026-03-27T21:01:05+00:00,KXTEST-2,0.57,0.59\n"
                    "2026-03-27T21:05:10+00:00,KXTEST-2,0.59,0.61\n"
                ),
                encoding="utf-8",
            )
            execute_summary = base / "kalshi_micro_execute_summary_test.json"
            execute_summary.write_text(
                json.dumps(
                    {
                        "history_csv": str(history_csv),
                        "attempts": [
                            {
                                "order_id": "order-222",
                                "planned_side": "yes",
                                "planned_contracts": 1.0,
                                "planned_entry_price_dollars": 0.50,
                                "category": "Economics",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            append_execution_events(
                journal_db_path=journal_path,
                events=[
                    {
                        "run_id": "preseed",
                        "captured_at_utc": "2026-03-27T21:00:00+00:00",
                        "event_type": "full_fill",
                        "market_ticker": "KXTEST-2",
                        "side": "yes",
                        "limit_price_dollars": 0.50,
                        "contracts_fp": 1.0,
                        "fee_dollars": 0.01,
                        "exchange_order_id": "order-222",
                    }
                ],
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if method == "GET" and url.endswith("/portfolio/orders/order-222"):
                    return 200, {
                        "order": {
                            "order_id": "order-222",
                            "ticker": "KXTEST-2",
                            "client_order_id": "betbot-2",
                            "status": "executed",
                            "yes_price_dollars": "0.5000",
                            "fill_count_fp": "1.00",
                            "remaining_count_fp": "0.00",
                            "initial_count_fp": "1.00",
                            "maker_fees_dollars": "0.0100",
                            "taker_fees_dollars": "0.0000",
                            "created_time": "2026-03-27T21:00:00Z",
                            "last_update_time": "2026-03-27T21:00:05Z",
                        }
                    }
                if method == "GET" and "/portfolio/positions?" in url:
                    return 200, {
                        "market_positions": [
                            {
                                "ticker": "KXTEST-2",
                                "position_fp": "0.00",
                                "market_exposure_dollars": "0.0000",
                                "realized_pnl_dollars": "0.0200",
                                "fees_paid_dollars": "0.0100",
                                "resting_orders_count": 0,
                            }
                        ],
                        "event_positions": [],
                    }
                return 404, {"error": "not found"}

            first = run_kalshi_micro_reconcile(
                env_file=str(env_file),
                execute_summary_file=str(execute_summary),
                output_dir=str(base),
                execution_journal_db_path=str(journal_path),
                http_request_json=fake_http_request_json,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 10, tzinfo=timezone.utc),
            )
            second = run_kalshi_micro_reconcile(
                env_file=str(env_file),
                execute_summary_file=str(execute_summary),
                output_dir=str(base),
                execution_journal_db_path=str(journal_path),
                http_request_json=fake_http_request_json,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 11, tzinfo=timezone.utc),
            )

            self.assertEqual(first["status"], "ready")
            self.assertEqual(first["markout_snapshot_events_generated"], 3)
            self.assertEqual(first["markout_samples_scored"], 3)
            self.assertIsInstance(first["markout_observation_lag_seconds_avg"], float)
            self.assertIsInstance(first["markout_observation_lag_seconds_max"], float)
            self.assertEqual(second["markout_snapshot_events_generated"], 0)
            events = load_execution_events(
                journal_db_path=journal_path,
                exchange_order_id="order-222",
                limit=100,
            )
            markout_events = [event for event in events if event.get("event_type") == "markout_snapshot"]
            self.assertEqual(len(markout_events), 3)
            horizons = sorted(
                int((event.get("payload") or {}).get("horizon_seconds"))
                for event in markout_events
                if isinstance(event.get("payload"), dict)
            )
            self.assertEqual(horizons, [10, 60, 300])
            self.assertTrue(any(float(event.get("markout_10s_dollars") or 0.0) > 0.0 for event in markout_events))
            self.assertTrue(any(float(event.get("markout_60s_dollars") or 0.0) > 0.0 for event in markout_events))
            self.assertTrue(any(float(event.get("markout_300s_dollars") or 0.0) > 0.0 for event in markout_events))

    def test_run_kalshi_micro_reconcile_skips_markouts_when_history_is_too_sparse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            env_file = base / "env.txt"
            journal_path = base / "execution.sqlite3"
            history_csv = base / "kalshi_nonsports_history.csv"
            env_file.write_text(
                "KALSHI_ENV=prod\nBETBOT_JURISDICTION=new_jersey\nKALSHI_ACCESS_KEY_ID=key123\nKALSHI_PRIVATE_KEY_PATH=/tmp/key.pem\n",
                encoding="utf-8",
            )
            history_csv.write_text(
                (
                    "captured_at,market_ticker,yes_bid_dollars,yes_ask_dollars\n"
                    "2026-03-27T21:10:00+00:00,KXTEST-3,0.55,0.57\n"
                ),
                encoding="utf-8",
            )
            execute_summary = base / "kalshi_micro_execute_summary_test.json"
            execute_summary.write_text(
                json.dumps(
                    {
                        "history_csv": str(history_csv),
                        "attempts": [
                            {
                                "order_id": "order-333",
                                "planned_side": "yes",
                                "planned_contracts": 1.0,
                                "planned_entry_price_dollars": 0.50,
                                "category": "Economics",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            append_execution_events(
                journal_db_path=journal_path,
                events=[
                    {
                        "run_id": "preseed",
                        "captured_at_utc": "2026-03-27T21:00:00+00:00",
                        "event_type": "full_fill",
                        "market_ticker": "KXTEST-3",
                        "side": "yes",
                        "limit_price_dollars": 0.50,
                        "contracts_fp": 1.0,
                        "fee_dollars": 0.01,
                        "exchange_order_id": "order-333",
                    }
                ],
            )

            def fake_http_request_json(
                url: str,
                method: str,
                headers: dict[str, str],
                body: object | None,
                timeout_seconds: float,
            ) -> tuple[int, object]:
                if method == "GET" and url.endswith("/portfolio/orders/order-333"):
                    return 200, {
                        "order": {
                            "order_id": "order-333",
                            "ticker": "KXTEST-3",
                            "client_order_id": "betbot-3",
                            "status": "executed",
                            "yes_price_dollars": "0.5000",
                            "fill_count_fp": "1.00",
                            "remaining_count_fp": "0.00",
                            "initial_count_fp": "1.00",
                            "maker_fees_dollars": "0.0100",
                            "taker_fees_dollars": "0.0000",
                            "created_time": "2026-03-27T21:00:00Z",
                            "last_update_time": "2026-03-27T21:00:05Z",
                        }
                    }
                if method == "GET" and "/portfolio/positions?" in url:
                    return 200, {
                        "market_positions": [
                            {
                                "ticker": "KXTEST-3",
                                "position_fp": "0.00",
                                "market_exposure_dollars": "0.0000",
                                "realized_pnl_dollars": "0.0200",
                                "fees_paid_dollars": "0.0100",
                                "resting_orders_count": 0,
                            }
                        ],
                        "event_positions": [],
                    }
                return 404, {"error": "not found"}

            summary = run_kalshi_micro_reconcile(
                env_file=str(env_file),
                execute_summary_file=str(execute_summary),
                output_dir=str(base),
                execution_journal_db_path=str(journal_path),
                http_request_json=fake_http_request_json,
                sign_request=lambda *_: "signed",
                now=datetime(2026, 3, 27, 21, 11, tzinfo=timezone.utc),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["markout_snapshot_events_generated"], 0)
            self.assertEqual(summary["markout_samples_scored"], 0)
            self.assertGreaterEqual(summary["markout_samples_skipped_due_to_lag"], 1)
            events = load_execution_events(
                journal_db_path=journal_path,
                exchange_order_id="order-333",
                limit=100,
            )
            self.assertFalse(any(event.get("event_type") == "markout_snapshot" for event in events))


if __name__ == "__main__":
    unittest.main()
