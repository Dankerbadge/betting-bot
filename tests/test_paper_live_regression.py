from __future__ import annotations

import unittest

from betbot.paper_live_regression import evaluate_paper_live_regression


class PaperLiveRegressionTests(unittest.TestCase):
    def _base_run(self) -> dict[str, object]:
        return {
            "paper_live_allow_random_cancels": False,
            "paper_live_size_from_current_equity": True,
            "paper_live_require_live_eligible_hint": False,
            "paper_live_order_attempts_run": 4,
            "paper_live_orders_filled_run": 2,
            "paper_live_orders_canceled_run": 0,
            "paper_live_order_attempts": 4,
            "paper_live_post_trade_sizing_balance_dollars": 1191.2056,
            "paper_live_sizing_balance_dollars": 1000.0,
        }

    def test_regression_passes_for_expected_carryover(self) -> None:
        run1 = self._base_run()
        run2 = {
            **self._base_run(),
            "paper_live_order_attempts_run": 0,
            "paper_live_orders_filled_run": 2,
            "paper_live_order_attempts": 4,
            "paper_live_sizing_balance_dollars": 1191.2056,
            "paper_live_post_trade_sizing_balance_dollars": 1160.0037,
        }
        summary = evaluate_paper_live_regression(run1=run1, run2=run2, require_attempts=True)
        self.assertEqual(summary["status"], "pass")

    def test_regression_fails_when_random_cancels_enabled(self) -> None:
        run1 = self._base_run()
        run1["paper_live_allow_random_cancels"] = True
        run2 = {
            **self._base_run(),
            "paper_live_sizing_balance_dollars": 1191.2056,
        }
        summary = evaluate_paper_live_regression(run1=run1, run2=run2)
        self.assertEqual(summary["status"], "fail")
        self.assertTrue(
            any(
                check.get("name") == "defaults.random_cancels_disabled" and not check.get("passed")
                for check in summary["checks"]  # type: ignore[index]
            )
        )

    def test_regression_fails_when_carryover_mismatch(self) -> None:
        run1 = self._base_run()
        run2 = {
            **self._base_run(),
            "paper_live_sizing_balance_dollars": 900.0,
            "paper_live_post_trade_sizing_balance_dollars": 905.0,
        }
        summary = evaluate_paper_live_regression(run1=run1, run2=run2)
        self.assertEqual(summary["status"], "fail")
        self.assertTrue(
            any(
                check.get("name") == "carryover.pre_trade_equals_previous_post_trade" and not check.get("passed")
                for check in summary["checks"]  # type: ignore[index]
            )
        )


if __name__ == "__main__":
    unittest.main()
