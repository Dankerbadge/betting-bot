import unittest

from betbot.ladder import build_policy, choose_withdrawal


class LadderTests(unittest.TestCase):
    def test_withdrawal_selected_when_probability_constraint_allows(self) -> None:
        policy = build_policy(
            enabled=True,
            rungs=[100, 200, 400],
            min_success_prob=0.70,
            withdraw_step=10.0,
            min_risk_wallet=10.0,
            risk_per_effort=10.0,
            planning_p=0.55,
        )
        amount, probability = choose_withdrawal(
            current_total_wealth=100.0,
            locked_vault=0.0,
            next_target=200.0,
            risk_wallet=100.0,
            policy=policy,
        )
        self.assertGreaterEqual(probability, policy.min_success_prob)
        self.assertGreater(amount, 0.0)

    def test_no_withdrawal_when_constraint_too_strict(self) -> None:
        policy = build_policy(
            enabled=True,
            rungs=[100, 200, 400],
            min_success_prob=0.99,
            withdraw_step=10.0,
            min_risk_wallet=10.0,
            risk_per_effort=10.0,
            planning_p=0.55,
        )
        amount, probability = choose_withdrawal(
            current_total_wealth=100.0,
            locked_vault=0.0,
            next_target=200.0,
            risk_wallet=100.0,
            policy=policy,
        )
        self.assertEqual(amount, 0.0)
        self.assertLess(probability, policy.min_success_prob)


if __name__ == "__main__":
    unittest.main()

