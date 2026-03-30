from __future__ import annotations

from dataclasses import dataclass, fields
import json
from pathlib import Path


@dataclass
class StrategyConfig:
    min_ev: float = 0.01
    kelly_fraction: float = 0.25
    dynamic_kelly_enabled: bool = False
    dynamic_kelly_confidence_floor: float = 0.50
    dynamic_kelly_confidence_ceiling: float = 0.90
    dynamic_kelly_min_fraction: float = 0.10
    dynamic_kelly_max_fraction: float = 0.35
    max_bet_fraction: float = 0.03
    min_stake: float = 1.0
    max_daily_loss_fraction: float = 0.10
    max_drawdown_fraction: float = 0.20
    planning_prob_floor: float | None = None
    ladder_enabled: bool = False
    ladder_rungs: list[float] | None = None
    ladder_min_success_prob: float = 0.70
    ladder_withdraw_step: float = 10.0
    ladder_min_risk_wallet: float = 10.0
    ladder_risk_per_effort: float = 10.0
    ladder_planning_p: float | None = None


def load_config(config_path: str | None) -> StrategyConfig:
    if not config_path:
        return StrategyConfig()

    payload = json.loads(Path(config_path).read_text(encoding="utf-8"))
    allowed = {f.name for f in fields(StrategyConfig)}
    filtered = {k: v for k, v in payload.items() if k in allowed}
    return StrategyConfig(**filtered)
