import unittest

from betbot.bayes import build_beta_posterior, conservative_planning_p


class BayesTests(unittest.TestCase):
    def test_posterior_mean(self) -> None:
        posterior = build_beta_posterior(wins=55, trials=100, prior_alpha=0.5, prior_beta=0.5)
        self.assertAlmostEqual(posterior.posterior_mean, 0.55, places=3)

    def test_conservative_planning_bounds(self) -> None:
        result = conservative_planning_p(wins=110, trials=200, confidence=0.95)
        self.assertLessEqual(result["credible_low"], result["posterior_mean"])
        self.assertLessEqual(result["posterior_mean"], result["credible_high"])
        self.assertLess(result["credible_low"], 0.55)


if __name__ == "__main__":
    unittest.main()

