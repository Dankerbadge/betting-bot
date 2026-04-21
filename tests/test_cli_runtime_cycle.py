from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from betbot.cli import main as cli_main


class CliRuntimeCycleTests(unittest.TestCase):
    def test_runtime_cycle_uses_default_adapters(self) -> None:
        with patch("betbot.cli.CycleRunner") as mock_runner_cls, patch.object(
            sys,
            "argv",
            ["betbot", "runtime-cycle", "--output-dir", "outputs"],
        ):
            mock_runner = mock_runner_cls.return_value
            mock_runner.run.return_value = {"overall_status": "ok"}

            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        adapters = mock_runner_cls.call_args.kwargs.get("adapters")
        providers = [getattr(adapter, "provider", "") for adapter in adapters]
        self.assertEqual(
            providers,
            ["kalshi_market_data", "curated_news", "opticodds_consensus"],
        )
        cfg = mock_runner.run.call_args.args[0]
        self.assertEqual(cfg.lane, "research")
        self.assertEqual(cfg.output_dir, "outputs")
        self.assertIsNone(cfg.hard_required_sources)
        self.assertIn('"overall_status": "ok"', stdout.getvalue())

    def test_runtime_cycle_supports_mapping_adapter_and_required_sources_override(self) -> None:
        with patch("betbot.cli.CycleRunner") as mock_runner_cls, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "runtime-cycle",
                "--output-dir",
                "outputs",
                "--include-therundown-mapping",
                "--hard-required-sources",
                "kalshi_market_data,curated_news",
            ],
        ):
            mock_runner = mock_runner_cls.return_value
            mock_runner.run.return_value = {"overall_status": "ok"}

            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        adapters = mock_runner_cls.call_args.kwargs.get("adapters")
        providers = [getattr(adapter, "provider", "") for adapter in adapters]
        self.assertEqual(
            providers,
            ["kalshi_market_data", "curated_news", "opticodds_consensus", "therundown_mapping"],
        )
        cfg = mock_runner.run.call_args.args[0]
        self.assertEqual(cfg.hard_required_sources, ("kalshi_market_data", "curated_news"))
        self.assertIn('"overall_status": "ok"', stdout.getvalue())

    def test_runtime_cycle_can_refresh_coldmath_snapshot_before_run(self) -> None:
        with patch(
            "betbot.cli.run_coldmath_snapshot_summary",
            return_value={"status": "ready", "output_file": "outputs/coldmath_snapshot_summary.json"},
        ) as mock_snapshot, patch("betbot.cli.CycleRunner") as mock_runner_cls, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "runtime-cycle",
                "--output-dir",
                "outputs",
                "--coldmath-refresh-from-api",
                "--coldmath-wallet-address",
                "0x594edb9112f526fa6a80b8f858a6379c8a2c1c11",
            ],
        ):
            mock_runner = mock_runner_cls.return_value
            mock_runner.run.return_value = {"overall_status": "ok"}

            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_snapshot.call_args.kwargs
        self.assertTrue(kwargs["refresh_from_api"])
        self.assertEqual(
            kwargs["wallet_address"],
            "0x594edb9112f526fa6a80b8f858a6379c8a2c1c11",
        )
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertTrue(kwargs["refresh_trades_from_api"])
        self.assertTrue(kwargs["refresh_activity_from_api"])
        self.assertTrue(kwargs["include_taker_only_trades"])
        self.assertTrue(kwargs["include_all_trade_roles"])
        self.assertIn('"coldmath_snapshot"', stdout.getvalue())

    def test_runtime_cycle_can_build_replication_plan_before_run(self) -> None:
        with patch(
            "betbot.cli.run_coldmath_replication_plan",
            return_value={"status": "ready", "output_file": "outputs/coldmath_replication_plan_latest.json"},
        ) as mock_plan, patch("betbot.cli.CycleRunner") as mock_runner_cls, patch.object(
            sys,
            "argv",
            [
                "betbot",
                "runtime-cycle",
                "--output-dir",
                "outputs",
                "--coldmath-build-replication-plan",
                "--coldmath-replication-top-n",
                "5",
                "--coldmath-replication-market-tickers",
                "MKT-A,MKT-B",
                "--coldmath-replication-max-spread-dollars",
                "0.14",
                "--coldmath-replication-min-liquidity-score",
                "0.55",
                "--coldmath-replication-max-family-candidates",
                "2",
                "--coldmath-replication-max-family-share",
                "0.5",
            ],
        ):
            mock_runner = mock_runner_cls.return_value
            mock_runner.run.return_value = {"overall_status": "ok"}

            stdout = StringIO()
            with redirect_stdout(stdout):
                cli_main()

        kwargs = mock_plan.call_args.kwargs
        self.assertEqual(kwargs["output_dir"], "outputs")
        self.assertEqual(kwargs["top_n"], 5)
        self.assertEqual(kwargs["market_tickers"], ["MKT-A", "MKT-B"])
        self.assertAlmostEqual(float(kwargs["max_spread_dollars"]), 0.14, places=9)
        self.assertAlmostEqual(float(kwargs["min_liquidity_score"]), 0.55, places=9)
        self.assertEqual(int(kwargs["max_family_candidates"]), 2)
        self.assertAlmostEqual(float(kwargs["max_family_share"]), 0.5, places=9)
        self.assertIn('"coldmath_replication_plan"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
