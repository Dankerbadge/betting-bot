import tempfile
from datetime import datetime, timezone
from pathlib import Path
import unittest

from betbot.kalshi_execution_journal import append_execution_events, load_execution_events


class KalshiExecutionJournalTests(unittest.TestCase):
    def test_append_and_load_execution_events_with_order_filters(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            db_path = base / "execution.sqlite3"

            written = append_execution_events(
                journal_db_path=db_path,
                events=[
                    {
                        "run_id": "run-1",
                        "captured_at_utc": datetime(2026, 3, 29, 6, 0, tzinfo=timezone.utc).isoformat(),
                        "event_type": "order_submitted",
                        "market_ticker": "KXTEST-1",
                        "side": "yes",
                        "contracts_fp": 1.0,
                        "client_order_id": "client-1",
                        "exchange_order_id": "exchange-1",
                        "payload": {"snapshot_phase": "pre_submit"},
                    },
                    {
                        "run_id": "run-1",
                        "captured_at_utc": datetime(2026, 3, 29, 6, 0, 5, tzinfo=timezone.utc).isoformat(),
                        "event_type": "full_fill",
                        "market_ticker": "KXTEST-1",
                        "side": "yes",
                        "contracts_fp": 1.0,
                        "fee_dollars": 0.01,
                        "client_order_id": "client-1",
                        "exchange_order_id": "exchange-1",
                        "payload": {"source": "unit_test"},
                    },
                ],
            )

            self.assertEqual(written, 2)
            all_events = load_execution_events(journal_db_path=db_path, limit=10)
            self.assertEqual(len(all_events), 2)

            by_exchange = load_execution_events(
                journal_db_path=db_path,
                exchange_order_id="exchange-1",
                limit=10,
            )
            self.assertEqual(len(by_exchange), 2)
            self.assertEqual(str(by_exchange[0]["exchange_order_id"]), "exchange-1")
            self.assertIn("payload", by_exchange[0])

            by_client = load_execution_events(
                journal_db_path=db_path,
                client_order_id="client-1",
                limit=10,
            )
            self.assertEqual(len(by_client), 2)


if __name__ == "__main__":
    unittest.main()
