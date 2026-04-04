from __future__ import annotations

import unittest

from betbot.policy.degraded_mode import summarize_source_results
from betbot.runtime.source_result import SourceResult


class DegradedModeTests(unittest.TestCase):
    def test_required_source_failure_blocks(self) -> None:
        source_results = {
            "kalshi_market_data": SourceResult(
                provider="kalshi_market_data",
                status="failed",
                payload=None,
                errors=["down"],
            ),
            "opticodds_consensus": SourceResult(
                provider="opticodds_consensus",
                status="ok",
                payload={"books": 3},
            ),
        }
        summary = summarize_source_results(
            source_results=source_results,
            phase="sources.ready",
            hard_required_sources=("kalshi_market_data",),
        )
        self.assertEqual(summary.overall_status, "blocked")
        self.assertEqual(summary.blocker_type, "required_source_failure")

    def test_partial_source_marks_degraded(self) -> None:
        source_results = {
            "kalshi_market_data": SourceResult(provider="kalshi_market_data", status="ok", payload={}),
            "opticodds_consensus": SourceResult(provider="opticodds_consensus", status="partial", payload={}),
        }
        summary = summarize_source_results(
            source_results=source_results,
            phase="sources.partial",
            hard_required_sources=("kalshi_market_data",),
        )
        self.assertEqual(summary.overall_status, "degraded")

    def test_missing_required_source_blocks(self) -> None:
        source_results = {
            "opticodds_consensus": SourceResult(provider="opticodds_consensus", status="ok", payload={}),
        }
        summary = summarize_source_results(
            source_results=source_results,
            phase="sources.ready",
            hard_required_sources=("kalshi_market_data",),
        )
        self.assertEqual(summary.overall_status, "blocked")
        self.assertEqual(summary.blocker_type, "required_source_missing")
        self.assertIn("kalshi_market_data", summary.missing_required_sources)


if __name__ == "__main__":
    unittest.main()
