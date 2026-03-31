import csv
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import unittest

from betbot.kalshi_execution_frontier import run_kalshi_execution_frontier
from betbot.kalshi_execution_journal import append_execution_events


class KalshiExecutionFrontierTests(unittest.TestCase):
    def test_execution_frontier_computes_markouts_and_break_even_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            journal_path = base / "execution.sqlite3"
            history_path = base / "history.csv"
            start = datetime(2026, 3, 29, 6, 0, tzinfo=timezone.utc)

            with history_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "captured_at",
                        "market_ticker",
                        "yes_bid_dollars",
                        "yes_ask_dollars",
                        "last_price_dollars",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": start.isoformat(),
                        "market_ticker": "KXTEST-1",
                        "yes_bid_dollars": "0.49",
                        "yes_ask_dollars": "0.51",
                        "last_price_dollars": "",
                    }
                )
                writer.writerow(
                    {
                        "captured_at": (start + timedelta(seconds=10)).isoformat(),
                        "market_ticker": "KXTEST-1",
                        "yes_bid_dollars": "0.51",
                        "yes_ask_dollars": "0.53",
                        "last_price_dollars": "",
                    }
                )
                writer.writerow(
                    {
                        "captured_at": (start + timedelta(seconds=60)).isoformat(),
                        "market_ticker": "KXTEST-1",
                        "yes_bid_dollars": "0.53",
                        "yes_ask_dollars": "0.55",
                        "last_price_dollars": "",
                    }
                )
                writer.writerow(
                    {
                        "captured_at": (start + timedelta(seconds=300)).isoformat(),
                        "market_ticker": "KXTEST-1",
                        "yes_bid_dollars": "0.57",
                        "yes_ask_dollars": "0.59",
                        "last_price_dollars": "",
                    }
                )
                writer.writerow(
                    {
                        "captured_at": (start + timedelta(seconds=301)).isoformat(),
                        "market_ticker": "KXTEST-1",
                        "yes_bid_dollars": "0.57",
                        "yes_ask_dollars": "0.59",
                        "last_price_dollars": "",
                    }
                )

            append_execution_events(
                journal_db_path=journal_path,
                events=[
                    {
                        "run_id": "frontier-run",
                        "captured_at_utc": start.isoformat(),
                        "event_type": "order_submitted",
                        "market_ticker": "KXTEST-1",
                        "event_family": "Economics",
                        "side": "yes",
                        "limit_price_dollars": 0.50,
                        "contracts_fp": 1.0,
                        "client_order_id": "client-1",
                        "exchange_order_id": "order-1",
                        "spread_dollars": 0.02,
                        "time_to_close_seconds": 7200.0,
                        "payload": {
                            "quote_aggressiveness": 0.75,
                            "execution_forecast_edge_net_per_contract_dollars": 0.04,
                        },
                    },
                    {
                        "run_id": "frontier-run",
                        "captured_at_utc": (start + timedelta(seconds=1)).isoformat(),
                        "event_type": "full_fill",
                        "market_ticker": "KXTEST-1",
                        "event_family": "Economics",
                        "side": "yes",
                        "limit_price_dollars": 0.50,
                        "contracts_fp": 1.0,
                        "fee_dollars": 0.002,
                        "client_order_id": "client-1",
                        "exchange_order_id": "order-1",
                    },
                    {
                        "run_id": "frontier-run",
                        "captured_at_utc": (start + timedelta(seconds=2)).isoformat(),
                        "event_type": "order_terminal",
                        "market_ticker": "KXTEST-1",
                        "event_family": "Economics",
                        "side": "yes",
                        "client_order_id": "client-1",
                        "exchange_order_id": "order-1",
                        "status": "executed",
                        "result": "executed",
                    },
                ],
            )

            summary = run_kalshi_execution_frontier(
                output_dir=str(base),
                journal_db_path=str(journal_path),
                history_csv=str(history_path),
                recent_events=100,
                min_markout_samples_10s=1,
                min_markout_samples_60s=1,
                min_markout_samples_300s=1,
                now=start + timedelta(minutes=6),
            )

            self.assertEqual(summary["status"], "ready")
            self.assertEqual(summary["submitted_orders"], 1)
            self.assertEqual(summary["filled_orders"], 1)
            self.assertEqual(summary["full_filled_orders"], 1)
            self.assertTrue(Path(summary["output_file"]).exists())
            self.assertTrue(Path(summary["bucket_csv"]).exists())
            self.assertEqual(len(summary["bucket_rows"]), 1)
            row = summary["bucket_rows"][0]
            self.assertEqual(row["fill_rate"], 1.0)
            self.assertEqual(row["full_fill_rate"], 1.0)
            self.assertAlmostEqual(row["median_time_to_fill_seconds"], 1.0, places=6)
            self.assertGreater(float(row["markout_10s_side_adjusted"]), 0.0)
            self.assertGreater(float(row["markout_60s_side_adjusted"]), 0.0)
            self.assertGreater(float(row["markout_300s_side_adjusted"]), 0.0)
            self.assertGreater(float(row["break_even_edge_per_contract"]), 0.0)
            self.assertTrue(row["markout_horizons_trusted"])

    def test_execution_frontier_requires_per_horizon_markout_samples_for_trusted_break_even(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            journal_path = base / "execution.sqlite3"
            history_path = base / "history.csv"
            start = datetime(2026, 3, 29, 6, 0, tzinfo=timezone.utc)

            with history_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "captured_at",
                        "market_ticker",
                        "yes_bid_dollars",
                        "yes_ask_dollars",
                        "last_price_dollars",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "captured_at": (start + timedelta(seconds=10)).isoformat(),
                        "market_ticker": "KXTEST-2",
                        "yes_bid_dollars": "0.51",
                        "yes_ask_dollars": "0.53",
                        "last_price_dollars": "",
                    }
                )
                writer.writerow(
                    {
                        "captured_at": (start + timedelta(seconds=60)).isoformat(),
                        "market_ticker": "KXTEST-2",
                        "yes_bid_dollars": "0.53",
                        "yes_ask_dollars": "0.55",
                        "last_price_dollars": "",
                    }
                )
                writer.writerow(
                    {
                        "captured_at": (start + timedelta(seconds=300)).isoformat(),
                        "market_ticker": "KXTEST-2",
                        "yes_bid_dollars": "0.57",
                        "yes_ask_dollars": "0.59",
                        "last_price_dollars": "",
                    }
                )

            append_execution_events(
                journal_db_path=journal_path,
                events=[
                    {
                        "run_id": "frontier-run",
                        "captured_at_utc": start.isoformat(),
                        "event_type": "order_submitted",
                        "market_ticker": "KXTEST-2",
                        "event_family": "Economics",
                        "side": "yes",
                        "limit_price_dollars": 0.50,
                        "contracts_fp": 1.0,
                        "client_order_id": "client-2",
                        "exchange_order_id": "order-2",
                        "spread_dollars": 0.02,
                        "time_to_close_seconds": 7200.0,
                        "payload": {
                            "quote_aggressiveness": 0.75,
                            "execution_forecast_edge_net_per_contract_dollars": 0.04,
                        },
                    },
                    {
                        "run_id": "frontier-run",
                        "captured_at_utc": (start + timedelta(seconds=1)).isoformat(),
                        "event_type": "full_fill",
                        "market_ticker": "KXTEST-2",
                        "event_family": "Economics",
                        "side": "yes",
                        "limit_price_dollars": 0.50,
                        "contracts_fp": 1.0,
                        "fee_dollars": 0.002,
                        "client_order_id": "client-2",
                        "exchange_order_id": "order-2",
                    },
                ],
            )

            summary = run_kalshi_execution_frontier(
                output_dir=str(base),
                journal_db_path=str(journal_path),
                history_csv=str(history_path),
                recent_events=100,
                min_markout_samples_10s=2,
                min_markout_samples_60s=2,
                min_markout_samples_300s=2,
                now=start + timedelta(minutes=6),
            )

            self.assertEqual(summary["status"], "insufficient_data")
            bucket_name = str(summary["bucket_rows"][0]["bucket"])
            self.assertIn(bucket_name, summary["break_even_edge_by_bucket"])
            self.assertEqual(summary["trusted_break_even_edge_by_bucket"], {})
            self.assertEqual(summary["bucket_rows"][0]["markout_10s_samples"], 1)
            self.assertEqual(summary["bucket_rows"][0]["markout_60s_samples"], 1)
            self.assertEqual(summary["bucket_rows"][0]["markout_300s_samples"], 0)
            self.assertFalse(summary["bucket_rows"][0]["markout_horizons_trusted"])
            self.assertIn("samples_below_min", str(summary["bucket_rows"][0]["markout_horizons_untrusted_reason"]))


if __name__ == "__main__":
    unittest.main()
