import unittest
from datetime import date

from betbot.config import StrategyConfig
from betbot.guardrails import (
    GuardrailState,
    apply_settlement,
    apply_transfer_out,
    check_pre_trade_limits,
)
from betbot.risk import propose_stake


class RiskGuardrailTests(unittest.TestCase):
    def test_propose_stake_respects_cap(self) -> None:
        cfg = StrategyConfig(max_bet_fraction=0.02, kelly_fraction=0.25, min_stake=1.0)
        result = propose_stake(bankroll=100.0, model_prob=0.60, odds=2.00, cfg=cfg)
        self.assertLessEqual(result.stake, 2.0)

    def test_daily_loss_limit_blocks_trade(self) -> None:
        cfg = StrategyConfig(max_daily_loss_fraction=0.05)
        state = GuardrailState.initialize(date(2026, 3, 27), 100.0)
        state = apply_settlement(state, -5.0)
        allowed, reason = check_pre_trade_limits(state, stake=1.0, cfg=cfg)
        self.assertFalse(allowed)
        self.assertEqual(reason, "daily_loss_limit")

    def test_drawdown_limit_blocks_trade(self) -> None:
        cfg = StrategyConfig(max_drawdown_fraction=0.20)
        state = GuardrailState.initialize(date(2026, 3, 27), 100.0)
        state = apply_settlement(state, +50.0)
        state = apply_settlement(state, -40.0)  # drawdown now 26.67%
        allowed, reason = check_pre_trade_limits(state, stake=1.0, cfg=cfg)
        self.assertFalse(allowed)
        self.assertEqual(reason, "drawdown_limit")

    def test_transfer_out_does_not_count_as_loss(self) -> None:
        cfg = StrategyConfig(max_daily_loss_fraction=0.05)
        state = GuardrailState.initialize(date(2026, 3, 27), 100.0)
        state = apply_transfer_out(state, 40.0)
        self.assertEqual(state.current_bankroll, 60.0)
        self.assertEqual(state.day_realized_pnl, 0.0)
        allowed, _ = check_pre_trade_limits(state, stake=1.0, cfg=cfg)
        self.assertTrue(allowed)

    def test_dynamic_kelly_increases_with_confidence(self) -> None:
        cfg = StrategyConfig(
            kelly_fraction=0.25,
            max_bet_fraction=0.2,
            min_stake=0.0,
            dynamic_kelly_enabled=True,
            dynamic_kelly_confidence_floor=0.5,
            dynamic_kelly_confidence_ceiling=0.9,
            dynamic_kelly_min_fraction=0.1,
            dynamic_kelly_max_fraction=0.3,
        )
        low_conf = propose_stake(bankroll=100.0, model_prob=0.6, odds=2.0, cfg=cfg, confidence=0.5)
        high_conf = propose_stake(bankroll=100.0, model_prob=0.6, odds=2.0, cfg=cfg, confidence=0.9)
        self.assertGreater(high_conf.stake, low_conf.stake)


if __name__ == "__main__":
    unittest.main()
