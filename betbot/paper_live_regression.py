from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RegressionCheck:
    name: str
    passed: bool
    detail: str


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def evaluate_paper_live_regression(
    *,
    run1: dict[str, Any],
    run2: dict[str, Any],
    require_attempts: bool = False,
    tolerance: float = 1e-3,
) -> dict[str, Any]:
    checks: list[RegressionCheck] = []

    run1_attempts = _as_int(run1.get("paper_live_order_attempts_run"))
    run1_fills = _as_int(run1.get("paper_live_orders_filled_run"))
    run1_cancels = _as_int(run1.get("paper_live_orders_canceled_run"))

    run1_total_attempts = _as_int(run1.get("paper_live_order_attempts"))
    run2_total_attempts = _as_int(run2.get("paper_live_order_attempts"))

    run1_post_trade = _as_float(run1.get("paper_live_post_trade_sizing_balance_dollars"))
    run2_pre_trade = _as_float(run2.get("paper_live_sizing_balance_dollars"))

    checks.append(
        RegressionCheck(
            name="defaults.random_cancels_disabled",
            passed=not _as_bool(run1.get("paper_live_allow_random_cancels"), default=False),
            detail=f"paper_live_allow_random_cancels={run1.get('paper_live_allow_random_cancels')!r}",
        )
    )
    checks.append(
        RegressionCheck(
            name="defaults.size_from_current_equity_enabled",
            passed=_as_bool(run1.get("paper_live_size_from_current_equity"), default=False),
            detail=f"paper_live_size_from_current_equity={run1.get('paper_live_size_from_current_equity')!r}",
        )
    )
    checks.append(
        RegressionCheck(
            name="defaults.require_live_eligible_hint_disabled",
            passed=not _as_bool(run1.get("paper_live_require_live_eligible_hint"), default=False),
            detail=(
                "paper_live_require_live_eligible_hint="
                f"{run1.get('paper_live_require_live_eligible_hint')!r}"
            ),
        )
    )

    if require_attempts:
        checks.append(
            RegressionCheck(
                name="run1.has_order_attempts",
                passed=run1_attempts > 0,
                detail=f"paper_live_order_attempts_run={run1_attempts}",
            )
        )
    else:
        checks.append(
            RegressionCheck(
                name="run1.has_order_activity",
                passed=(run1_attempts > 0) or (run1_fills > 0),
                detail=(
                    f"paper_live_order_attempts_run={run1_attempts}, "
                    f"paper_live_orders_filled_run={run1_fills}"
                ),
            )
        )

    checks.append(
        RegressionCheck(
            name="run1.no_random_cancels",
            passed=run1_cancels == 0,
            detail=f"paper_live_orders_canceled_run={run1_cancels}",
        )
    )
    checks.append(
        RegressionCheck(
            name="carryover.pre_trade_equals_previous_post_trade",
            passed=abs(run2_pre_trade - run1_post_trade) <= float(tolerance),
            detail=(
                f"run1_post_trade={run1_post_trade:.6f}, "
                f"run2_pre_trade={run2_pre_trade:.6f}, tolerance={tolerance}"
            ),
        )
    )
    checks.append(
        RegressionCheck(
            name="totals.non_reset_attempt_count",
            passed=run2_total_attempts >= run1_total_attempts,
            detail=(
                f"run1_total_attempts={run1_total_attempts}, "
                f"run2_total_attempts={run2_total_attempts}"
            ),
        )
    )

    passed = all(item.passed for item in checks)
    return {
        "status": "pass" if passed else "fail",
        "checks": [
            {"name": item.name, "passed": item.passed, "detail": item.detail}
            for item in checks
        ],
    }
