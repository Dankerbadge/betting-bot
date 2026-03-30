from datetime import datetime, timezone
import sqlite3
import tempfile
from pathlib import Path
import unittest

from betbot.kalshi_book import (
    ensure_book_schema,
    record_decisions,
    record_order_attempts,
    record_reconcile_snapshot,
)


class KalshiBookTests(unittest.TestCase):
    def test_record_decisions_and_reconcile_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "book.sqlite3"
            ensure_book_schema(db_path)
            captured_at = datetime(2026, 3, 28, 0, 0, tzinfo=timezone.utc)

            record_decisions(
                book_db_path=db_path,
                source="test",
                captured_at=captured_at,
                plans=[
                    {
                        "market_ticker": "KXTEST-1",
                        "category": "Politics",
                        "side": "no",
                        "contracts_per_order": 1,
                        "maker_entry_edge": 0.02,
                        "maker_entry_edge_net_fees": 0.01,
                        "expected_value_dollars": 0.02,
                        "expected_value_net_dollars": 0.01,
                        "expected_roi_on_cost_net": 0.01,
                    }
                ],
            )
            record_order_attempts(
                book_db_path=db_path,
                captured_at=captured_at,
                attempts=[
                    {
                        "order_id": "order-1",
                        "market_ticker": "KXTEST-1",
                        "planned_side": "no",
                        "planned_entry_price_dollars": 0.96,
                        "result": "dry_run_ready",
                        "order_status": "resting",
                    }
                ],
            )
            record_reconcile_snapshot(
                book_db_path=db_path,
                captured_at=captured_at,
                rows=[
                    {
                        "order_id": "order-1",
                        "client_order_id": "client-1",
                        "ticker": "KXTEST-1",
                        "planned_side": "no",
                        "effective_price_dollars": 0.96,
                        "status": "resting",
                        "position_fp": 1.0,
                        "market_exposure_dollars": 0.96,
                        "realized_pnl_dollars": 0.0,
                        "fees_paid_dollars": 0.01,
                        "resting_orders_count": 1,
                    }
                ],
            )

            with sqlite3.connect(db_path) as conn:
                decisions = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
                orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
                positions = conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0]

            self.assertEqual(decisions, 1)
            self.assertEqual(orders, 1)
            self.assertEqual(positions, 1)


if __name__ == "__main__":
    unittest.main()

