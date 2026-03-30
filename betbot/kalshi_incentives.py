from __future__ import annotations

from typing import Any

from betbot.kalshi_micro_execute import _signed_kalshi_request
from betbot.kalshi_nonsports_scan import _parse_float
from betbot.live_smoke import KalshiSigner


def _extract_reward_per_contract(candidate: dict[str, Any]) -> float | None:
    for key in (
        "reward_per_contract_dollars",
        "maker_reward_per_contract_dollars",
        "maker_rebate_per_contract_dollars",
        "rebate_per_contract_dollars",
        "reward_dollars_per_contract",
    ):
        parsed = _parse_float(candidate.get(key))
        if isinstance(parsed, float):
            return parsed
    return None


def parse_incentive_map(payload: dict[str, Any]) -> dict[str, float]:
    programs = payload.get("incentive_programs")
    if not isinstance(programs, list):
        programs = payload.get("programs")
    if not isinstance(programs, list):
        return {}
    result: dict[str, float] = {}
    for program in programs:
        if not isinstance(program, dict):
            continue
        reward = _extract_reward_per_contract(program) or 0.0
        markets = program.get("markets")
        if isinstance(markets, list):
            for market in markets:
                if not isinstance(market, dict):
                    continue
                ticker = str(market.get("ticker") or market.get("market_ticker") or "").strip()
                if not ticker:
                    continue
                market_reward = _extract_reward_per_contract(market)
                result[ticker] = float(market_reward if market_reward is not None else reward)
            continue
        ticker = str(program.get("ticker") or program.get("market_ticker") or "").strip()
        if ticker:
            result[ticker] = float(reward)
    return result


def fetch_incentive_map(
    *,
    env_data: dict[str, str],
    timeout_seconds: float,
    http_request_json,
    sign_request: KalshiSigner,
) -> dict[str, float]:
    status_code, payload = _signed_kalshi_request(
        env_data=env_data,
        method="GET",
        path_with_query="/incentive_programs",
        body=None,
        timeout_seconds=timeout_seconds,
        http_request_json=http_request_json,
        sign_request=sign_request,
    )
    if status_code != 200 or not isinstance(payload, dict):
        return {}
    return parse_incentive_map(payload)

