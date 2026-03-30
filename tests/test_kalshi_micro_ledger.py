from datetime import datetime, timezone
import sqlite3
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_micro_ledger import (
    append_trade_ledger,
    ledger_rows_from_attempts,
    summarize_trade_ledger,
    trading_day_for_timestamp,
)


class KalshiMicroLedgerTests(unittest.TestCase):
    def test_ledger_rows_and_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.csv"
            captured_at = datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc)
            trading_day = trading_day_for_timestamp(captured_at, "America/New_York")
            rows = ledger_rows_from_attempts(
                attempts=[
                    {
                        "market_ticker": "KXTEST-1",
                        "plan_rank": 1,
                        "planned_yes_bid_dollars": 0.02,
                        "planned_yes_ask_dollars": 0.03,
                        "estimated_entry_cost_dollars": 0.02,
                        "result": "submitted_then_canceled",
                        "live_write_allowed": True,
                        "submission_http_status": 201,
                        "order_id": "order-1",
                        "order_status": "canceled",
                        "cancel_http_status": 200,
                        "cancel_reduced_by_contracts": 1.0,
                    }
                ],
                captured_at=captured_at,
                trading_day=trading_day,
                run_mode="live",
                resting_hold_seconds=0.0,
            )
            append_trade_ledger(path, rows)
            summary = summarize_trade_ledger(
                path=path,
                timezone_name="America/New_York",
                trading_day=trading_day,
                max_live_submissions_per_day=3,
            )
            self.assertEqual(summary["live_submissions_today"], 0)
            self.assertEqual(summary["live_submitted_cost_today"], 0.0)
            self.assertEqual(summary["canceled_submissions_today"], 1)
            self.assertEqual(summary["live_submission_budget_remaining"], 3)

    def test_dry_run_attempts_do_not_emit_trade_ledger_rows(self) -> None:
        captured_at = datetime(2026, 3, 27, 21, 0, tzinfo=timezone.utc)
        trading_day = trading_day_for_timestamp(captured_at, "America/New_York")
        rows = ledger_rows_from_attempts(
            attempts=[
                {
                    "market_ticker": "KXTEST-1",
                    "plan_rank": 1,
                    "planned_yes_bid_dollars": 0.02,
                    "planned_yes_ask_dollars": 0.03,
                    "estimated_entry_cost_dollars": 0.02,
                    "result": "dry_run_ready",
                    "live_write_allowed": False,
                }
            ],
            captured_at=captured_at,
            trading_day=trading_day,
            run_mode="dry_run",
            resting_hold_seconds=0.0,
        )
        self.assertEqual(rows, [])

    def test_submission_budget_accumulates_by_trading_day(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.csv"
            append_trade_ledger(
                path,
                [
                    {
                        "recorded_at": "2026-03-27T20:30:00+00:00",
                        "trading_day": "2026-03-27",
                        "run_mode": "live",
                        "live_write_allowed": "true",
                        "market_ticker": "KXTEST-1",
                        "plan_rank": 1,
                        "planned_yes_bid_dollars": 0.02,
                        "planned_yes_ask_dollars": 0.03,
                        "estimated_entry_cost_dollars": 0.02,
                        "result": "submitted",
                        "submission_http_status": 201,
                        "order_id": "order-1",
                        "order_status": "resting",
                        "queue_position_contracts": "",
                        "cancel_http_status": "",
                        "cancel_reduced_by_contracts": "",
                        "resting_hold_seconds": 0.0,
                        "counts_toward_live_submission": "true",
                    }
                ],
            )
            summary = summarize_trade_ledger(
                path=path,
                timezone_name="America/New_York",
                trading_day=datetime(2026, 3, 28, 21, 0, tzinfo=timezone.utc).date(),
                max_live_submissions_per_day=3,
            )
            self.assertEqual(summary["live_submissions_to_date"], 1)
            self.assertEqual(summary["live_submission_days_elapsed"], 2)
            self.assertEqual(summary["live_submission_budget_total"], 6)
            self.assertEqual(summary["live_submission_budget_remaining"], 5)
            self.assertEqual(summary["live_submissions_remaining_today"], 3)

    def test_budget_accrues_from_first_live_activity_even_when_submissions_are_released(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.csv"
            append_trade_ledger(
                path,
                [
                    {
                        "recorded_at": "2026-03-28T04:51:54+00:00",
                        "trading_day": "2026-03-28",
                        "run_mode": "live",
                        "live_write_allowed": "true",
                        "market_ticker": "KXTEST-1",
                        "plan_rank": 1,
                        "planned_yes_bid_dollars": 0.02,
                        "planned_yes_ask_dollars": 0.03,
                        "estimated_entry_cost_dollars": 0.96,
                        "result": "submitted_then_canceled",
                        "submission_http_status": 201,
                        "order_id": "order-1",
                        "order_status": "canceled",
                        "queue_position_contracts": "",
                        "cancel_http_status": 200,
                        "cancel_reduced_by_contracts": 1.0,
                        "resting_hold_seconds": 0.0,
                        "counts_toward_live_submission": "false",
                    }
                ],
            )
            summary = summarize_trade_ledger(
                path=path,
                timezone_name="America/New_York",
                trading_day=datetime(2026, 3, 29, 21, 0, tzinfo=timezone.utc).date(),
                max_live_submissions_per_day=3,
                max_live_cost_per_day_dollars=3.0,
            )
            self.assertEqual(summary["first_live_activity_day"], "2026-03-28")
            self.assertEqual(summary["live_submission_days_elapsed"], 2)
            self.assertEqual(summary["live_submission_budget_total"], 6)
            self.assertEqual(summary["live_submission_budget_remaining"], 6)
            self.assertEqual(summary["live_cost_budget_total"], 6.0)
            self.assertEqual(summary["live_cost_budget_remaining"], 6.0)

    def test_cost_budget_tracks_accrual_and_remaining(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.csv"
            append_trade_ledger(
                path,
                [
                    {
                        "recorded_at": "2026-03-28T16:00:00+00:00",
                        "trading_day": "2026-03-28",
                        "run_mode": "live",
                        "live_write_allowed": "true",
                        "market_ticker": "KXTEST-1",
                        "plan_rank": 1,
                        "planned_yes_bid_dollars": 0.60,
                        "planned_yes_ask_dollars": 0.61,
                        "estimated_entry_cost_dollars": 0.6,
                        "result": "submitted",
                        "submission_http_status": 201,
                        "order_id": "order-1",
                        "order_status": "resting",
                        "queue_position_contracts": "",
                        "cancel_http_status": "",
                        "cancel_reduced_by_contracts": "",
                        "resting_hold_seconds": 0.0,
                        "counts_toward_live_submission": "true",
                    },
                    {
                        "recorded_at": "2026-03-29T16:00:00+00:00",
                        "trading_day": "2026-03-29",
                        "run_mode": "live",
                        "live_write_allowed": "true",
                        "market_ticker": "KXTEST-2",
                        "plan_rank": 2,
                        "planned_yes_bid_dollars": 0.40,
                        "planned_yes_ask_dollars": 0.41,
                        "estimated_entry_cost_dollars": 0.4,
                        "result": "submitted",
                        "submission_http_status": 201,
                        "order_id": "order-2",
                        "order_status": "resting",
                        "queue_position_contracts": "",
                        "cancel_http_status": "",
                        "cancel_reduced_by_contracts": "",
                        "resting_hold_seconds": 0.0,
                        "counts_toward_live_submission": "true",
                    },
                ],
            )
            summary = summarize_trade_ledger(
                path=path,
                timezone_name="America/New_York",
                trading_day=datetime(2026, 3, 29, 21, 0, tzinfo=timezone.utc).date(),
                max_live_submissions_per_day=3,
                max_live_cost_per_day_dollars=3.0,
            )
            self.assertEqual(summary["live_submitted_cost_to_date"], 1.0)
            self.assertEqual(summary["live_cost_budget_total"], 6.0)
            self.assertEqual(summary["live_cost_budget_remaining"], 5.0)
            self.assertEqual(summary["live_cost_remaining_today"], 2.6)

    def test_summary_releases_submitted_rows_for_terminal_canceled_orders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            ledger_path = base / "ledger.csv"
            book_db_path = base / "book.sqlite3"

            append_trade_ledger(
                ledger_path,
                [
                    {
                        "recorded_at": "2026-03-29T15:30:00+00:00",
                        "trading_day": "2026-03-29",
                        "run_mode": "live",
                        "live_write_allowed": "true",
                        "market_ticker": "KXTEST-1",
                        "plan_rank": 1,
                        "planned_yes_bid_dollars": 0.02,
                        "planned_yes_ask_dollars": 0.03,
                        "estimated_entry_cost_dollars": 0.97,
                        "result": "submitted",
                        "submission_http_status": 201,
                        "order_id": "order-1",
                        "order_status": "resting",
                        "queue_position_contracts": "",
                        "cancel_http_status": "",
                        "cancel_reduced_by_contracts": "",
                        "resting_hold_seconds": 0.0,
                        "counts_toward_live_submission": "true",
                    }
                ],
            )

            with sqlite3.connect(book_db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE orders (
                        order_id TEXT PRIMARY KEY,
                        client_order_id TEXT,
                        ticker TEXT NOT NULL,
                        side TEXT,
                        action TEXT,
                        limit_price_dollars REAL,
                        post_only INTEGER,
                        status TEXT,
                        created_time TEXT,
                        last_update_time TEXT,
                        last_seen_at TEXT NOT NULL,
                        raw_json TEXT
                    )
                    """
                )
                conn.execute(
                    """
                    INSERT INTO orders (
                        order_id, ticker, status, last_seen_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    ("order-1", "KXTEST-1", "canceled", "2026-03-29T16:00:00+00:00"),
                )
                conn.commit()

            summary = summarize_trade_ledger(
                path=ledger_path,
                timezone_name="America/New_York",
                trading_day=datetime(2026, 3, 29, 21, 0, tzinfo=timezone.utc).date(),
                max_live_submissions_per_day=3,
                book_db_path=book_db_path,
            )
            self.assertEqual(summary["live_submissions_today"], 0)
            self.assertEqual(summary["live_submitted_cost_today"], 0.0)
            self.assertEqual(summary["live_submissions_to_date"], 0)
            self.assertEqual(summary["released_counted_submission_rows"], 1)


if __name__ == "__main__":
    unittest.main()
