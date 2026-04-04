from __future__ import annotations

import unittest

from betbot.policy.compliance import (
    ComplianceScorecard,
    ComplianceThresholds,
    evaluate_readiness,
)


class CompliancePolicyTests(unittest.TestCase):
    def test_evaluate_readiness_pass(self) -> None:
        thresholds = ComplianceThresholds(
            settlement_min=0.7,
            execution_min=0.7,
            compliance_min=0.7,
            overall_min=0.75,
        )
        scorecard = ComplianceScorecard(
            settlement=0.9,
            execution=0.88,
            compliance=0.85,
            overall=0.86,
            high_blockers=0,
            critical_blockers=0,
        )
        ready, reasons = evaluate_readiness(scorecard, thresholds)
        self.assertTrue(ready)
        self.assertEqual(reasons, [])

    def test_evaluate_readiness_fails_on_thresholds(self) -> None:
        thresholds = ComplianceThresholds()
        scorecard = ComplianceScorecard(
            settlement=0.5,
            execution=0.6,
            compliance=0.4,
            overall=0.5,
            high_blockers=1,
            critical_blockers=0,
        )
        ready, reasons = evaluate_readiness(scorecard, thresholds)
        self.assertFalse(ready)
        self.assertIn("high_blockers_present", reasons)
        self.assertIn("settlement_below_threshold", reasons)
        self.assertIn("execution_below_threshold", reasons)
        self.assertIn("compliance_below_threshold", reasons)
        self.assertIn("overall_below_threshold", reasons)


if __name__ == "__main__":
    unittest.main()
