from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable

from betbot.kalshi_nonsports_scan import _load_open_events, _parse_float, extract_kalshi_nonsports_rows
from betbot.live_smoke import (
    HttpGetter,
    KALSHI_API_ROOTS,
    KalshiSigner,
    _http_get_json,
    _kalshi_sign_request,
    kalshi_api_root_candidates,
)
from betbot.live_snapshot import _kalshi_balance_snapshot
from betbot.onboarding import _is_placeholder, _parse_env_file


BalanceFetcher = Callable[
    [str, str, str, float, HttpGetter, KalshiSigner],
    dict[str, Any],
]


def default_balance_cache_path(output_dir: str) -> Path:
    return Path(output_dir) / "kalshi_live_balance_cache.json"


def _write_balance_cache(
    path: Path,
    *,
    balance_cents: int,
    captured_at: datetime,
    kalshi_env: str,
) -> None:
    payload = {
        "balance_cents": balance_cents,
        "captured_at": captured_at.isoformat(),
        "kalshi_env": kalshi_env,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_balance_cache(
    path: Path,
    *,
    captured_at: datetime,
    kalshi_env: str,
    max_age_seconds: float,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("kalshi_env") or "").strip().lower() != kalshi_env:
        return None
    balance_cents = payload.get("balance_cents")
    captured_at_text = str(payload.get("captured_at") or "").strip()
    if not isinstance(balance_cents, (int, float)) or not captured_at_text:
        return None
    try:
        cached_at = datetime.fromisoformat(captured_at_text.replace("Z", "+00:00"))
    except ValueError:
        return None
    age_seconds = max(0.0, (captured_at - cached_at).total_seconds())
    if age_seconds > max_age_seconds:
        return None
    return {
        "balance_cents": int(balance_cents),
        "captured_at": cached_at.isoformat(),
        "age_seconds": round(age_seconds, 4),
    }


def _parse_bool_like(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes"}


def _default_balance_fetcher(
    access_key_id: str,
    private_key_path: str,
    env_name: str,
    timeout_seconds: float,
    http_get_json: HttpGetter,
    sign_request: KalshiSigner,
) -> dict[str, Any]:
    return _kalshi_balance_snapshot(
        access_key_id=access_key_id,
        private_key_path=private_key_path,
        env_name=env_name,
        timeout_seconds=timeout_seconds,
        http_get_json=http_get_json,
        sign_request=sign_request,
    )


def _build_order_payload_preview(
    *,
    ticker: str,
    count: int,
    yes_price_dollars: float,
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "side": "yes",
        "action": "buy",
        "count": count,
        "yes_price_dollars": f"{yes_price_dollars:.4f}",
        "time_in_force": "good_till_canceled",
        "post_only": True,
        "cancel_order_on_pause": True,
        "self_trade_prevention_type": "maker",
    }


def _load_ranked_rows_from_scan_csv(path: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    ranked_rows: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            normalized = dict(row)
            category = str(normalized.get("category") or "").strip() or "Unknown"
            category_counts[category] = category_counts.get(category, 0) + 1
            ranked_rows.append(normalized)
    return ranked_rows, category_counts


def build_micro_order_plans(
    *,
    ranked_rows: list[dict[str, Any]],
    planning_bankroll_dollars: float,
    daily_risk_cap_dollars: float,
    contracts_per_order: int,
    max_orders: int,
    min_yes_bid_dollars: float,
    max_yes_ask_dollars: float,
    max_spread_dollars: float,
    require_two_sided_book: bool,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if planning_bankroll_dollars <= 0:
        raise ValueError("planning_bankroll_dollars must be positive")
    if daily_risk_cap_dollars <= 0:
        raise ValueError("daily_risk_cap_dollars must be positive")
    if contracts_per_order <= 0:
        raise ValueError("contracts_per_order must be positive")
    if max_orders <= 0:
        raise ValueError("max_orders must be positive")

    remaining_risk = round(daily_risk_cap_dollars, 4)
    plans: list[dict[str, Any]] = []
    skip_counts = {
        "not_two_sided": 0,
        "missing_yes_bid": 0,
        "yes_bid_below_min": 0,
        "yes_ask_above_max": 0,
        "spread_above_max": 0,
        "budget_too_small": 0,
    }

    for row in ranked_rows:
        if len(plans) >= max_orders:
            break

        if require_two_sided_book and not _parse_bool_like(row.get("two_sided_book")):
            skip_counts["not_two_sided"] += 1
            continue

        yes_bid = _parse_float(row.get("yes_bid_dollars"))
        yes_ask = _parse_float(row.get("yes_ask_dollars"))
        spread = _parse_float(row.get("spread_dollars"))
        if yes_bid is None:
            skip_counts["missing_yes_bid"] += 1
            continue
        if yes_bid < min_yes_bid_dollars:
            skip_counts["yes_bid_below_min"] += 1
            continue
        if yes_ask is None or yes_ask > max_yes_ask_dollars:
            skip_counts["yes_ask_above_max"] += 1
            continue
        if spread is None or spread > max_spread_dollars:
            skip_counts["spread_above_max"] += 1
            continue

        estimated_entry_cost = round(yes_bid * contracts_per_order, 4)
        if estimated_entry_cost > remaining_risk:
            skip_counts["budget_too_small"] += 1
            continue

        estimated_max_profit = round((1.0 - yes_bid) * contracts_per_order, 4)
        queue_ahead = _parse_float(row.get("yes_bid_size_contracts"))
        payload_preview = _build_order_payload_preview(
            ticker=str(row.get("market_ticker") or ""),
            count=contracts_per_order,
            yes_price_dollars=yes_bid,
        )
        plans.append(
            {
                "plan_rank": len(plans) + 1,
                "category": row["category"],
                "market_ticker": row["market_ticker"],
                "market_title": row["market_title"],
                "event_title": row["event_title"],
                "yes_bid_dollars": yes_bid,
                "yes_ask_dollars": yes_ask,
                "spread_dollars": spread,
                "hours_to_close": row["hours_to_close"],
                "contracts_per_order": contracts_per_order,
                "maker_yes_price_dollars": yes_bid,
                "estimated_entry_cost_dollars": estimated_entry_cost,
                "estimated_max_loss_dollars": estimated_entry_cost,
                "estimated_max_profit_dollars": estimated_max_profit,
                "queue_ahead_contracts_estimate": queue_ahead if queue_ahead is not None else "",
                "planning_bankroll_fraction": round(estimated_entry_cost / planning_bankroll_dollars, 6),
                "execution_fit_score": row["execution_fit_score"],
                "order_payload_preview": payload_preview,
            }
        )
        remaining_risk = round(remaining_risk - estimated_entry_cost, 4)

    return plans, skip_counts


def _write_plan_csv(path: Path, plans: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "plan_rank",
        "category",
        "market_ticker",
        "market_title",
        "event_title",
        "yes_bid_dollars",
        "yes_ask_dollars",
        "spread_dollars",
        "hours_to_close",
        "contracts_per_order",
        "maker_yes_price_dollars",
        "estimated_entry_cost_dollars",
        "estimated_max_loss_dollars",
        "estimated_max_profit_dollars",
        "queue_ahead_contracts_estimate",
        "planning_bankroll_fraction",
        "execution_fit_score",
        "order_payload_preview",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for plan in plans:
            serializable = dict(plan)
            serializable["order_payload_preview"] = json.dumps(plan["order_payload_preview"], separators=(",", ":"))
            writer.writerow(serializable)


def run_kalshi_micro_plan(
    *,
    env_file: str,
    output_dir: str = "outputs",
    planning_bankroll_dollars: float = 40.0,
    daily_risk_cap_dollars: float = 3.0,
    contracts_per_order: int = 1,
    max_orders: int = 3,
    min_yes_bid_dollars: float = 0.01,
    max_yes_ask_dollars: float = 0.10,
    max_spread_dollars: float = 0.02,
    max_hours_to_close: float = 336.0,
    excluded_categories: tuple[str, ...] = ("Sports",),
    require_two_sided_book: bool = True,
    page_limit: int = 200,
    max_pages: int = 5,
    timeout_seconds: float = 15.0,
    balance_cache_file: str | None = None,
    max_balance_cache_age_seconds: float = 86400.0,
    http_get_json: HttpGetter = _http_get_json,
    sign_request: KalshiSigner = _kalshi_sign_request,
    balance_fetcher: BalanceFetcher = _default_balance_fetcher,
    scan_csv: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    env_path = Path(env_file)
    env_data = _parse_env_file(env_path)
    captured_at = now or datetime.now(timezone.utc)

    kalshi_env = (env_data.get("KALSHI_ENV") or "prod").strip().lower()
    if kalshi_env not in KALSHI_API_ROOTS:
        raise ValueError(f"Unsupported KALSHI_ENV={kalshi_env!r}")
    api_roots = kalshi_api_root_candidates(kalshi_env)

    events: list[dict[str, Any]] = []
    ranked_rows: list[dict[str, Any]] = []
    category_counts: dict[str, int] = {}
    skip_counts = {
        "not_two_sided": 0,
        "missing_yes_bid": 0,
        "yes_bid_below_min": 0,
        "yes_ask_above_max": 0,
        "spread_above_max": 0,
        "budget_too_small": 0,
    }
    plans: list[dict[str, Any]] = []
    events_error: str | None = None
    status = "ready"
    data_source = "kalshi_api"
    try:
        if scan_csv:
            ranked_rows, category_counts = _load_ranked_rows_from_scan_csv(Path(scan_csv))
            data_source = "scan_csv"
        else:
            events = _load_open_events(
                api_roots=api_roots,
                timeout_seconds=timeout_seconds,
                page_limit=page_limit,
                max_pages=max_pages,
                http_get_json=http_get_json,
            )
            ranked_rows, category_counts = extract_kalshi_nonsports_rows(
                events=events,
                captured_at=captured_at,
                excluded_categories=excluded_categories,
                max_hours_to_close=max_hours_to_close,
            )
        plans, skip_counts = build_micro_order_plans(
            ranked_rows=ranked_rows,
            planning_bankroll_dollars=planning_bankroll_dollars,
            daily_risk_cap_dollars=daily_risk_cap_dollars,
            contracts_per_order=contracts_per_order,
            max_orders=max_orders,
            min_yes_bid_dollars=min_yes_bid_dollars,
            max_yes_ask_dollars=max_yes_ask_dollars,
            max_spread_dollars=max_spread_dollars,
            require_two_sided_book=require_two_sided_book,
        )
    except Exception as exc:  # pragma: no cover - defensive runtime path
        events_error = str(exc)
        status = "rate_limited" if "status 429" in events_error else "upstream_error"

    live_balance_cents: int | None = None
    balance_error: str | None = None
    balance_source = "unknown"
    balance_cache_age_seconds: float | None = None
    balance_cache_path = Path(balance_cache_file) if balance_cache_file else default_balance_cache_path(output_dir)
    access_key_id = env_data.get("KALSHI_ACCESS_KEY_ID")
    private_key_path = env_data.get("KALSHI_PRIVATE_KEY_PATH")
    if not _is_placeholder(access_key_id) and not _is_placeholder(private_key_path):
        try:
            balance_snapshot = balance_fetcher(
                access_key_id or "",
                private_key_path or "",
                kalshi_env,
                timeout_seconds,
                http_get_json,
                sign_request,
            )
            if isinstance(balance_snapshot.get("balance_cents"), int | float):
                live_balance_cents = int(balance_snapshot["balance_cents"])
                balance_source = "live"
                _write_balance_cache(
                    balance_cache_path,
                    balance_cents=live_balance_cents,
                    captured_at=captured_at,
                    kalshi_env=kalshi_env,
                )
        except Exception as exc:  # pragma: no cover - defensive summary path
            balance_error = str(exc)

    if live_balance_cents is None:
        cached_balance = _load_balance_cache(
            balance_cache_path,
            captured_at=captured_at,
            kalshi_env=kalshi_env,
            max_age_seconds=max_balance_cache_age_seconds,
        )
        if cached_balance is not None:
            live_balance_cents = int(cached_balance["balance_cents"])
            balance_source = "cache"
            balance_cache_age_seconds = float(cached_balance["age_seconds"])

    total_planned_cost = round(sum(plan["estimated_entry_cost_dollars"] for plan in plans), 4)
    live_balance_dollars = round(live_balance_cents / 100.0, 2) if live_balance_cents is not None else None
    funding_gap_dollars = None
    if live_balance_dollars is not None:
        funding_gap_dollars = round(max(0.0, total_planned_cost - live_balance_dollars), 4)
    penny_price_only = bool(plans) and all(plan["maker_yes_price_dollars"] < 0.05 for plan in plans)
    board_warning = None
    if penny_price_only:
        board_warning = (
            "Current two-sided candidates are all below $0.05 on the Yes bid, "
            "so this plan is suitable for execution smoke-testing but not for a strong profitability read."
        )

    stamp = captured_at.astimezone().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"kalshi_micro_plan_{stamp}.csv"
    _write_plan_csv(csv_path, plans)

    if status == "ready" and not plans:
        status = "no_candidates"
    elif status == "ready" and live_balance_dollars is not None and funding_gap_dollars is not None and funding_gap_dollars > 0:
        status = "needs_funding"

    summary = {
        "env_file": str(env_path),
        "captured_at": captured_at.isoformat(),
        "jurisdiction": (env_data.get("BETBOT_JURISDICTION") or "").strip(),
        "kalshi_env": kalshi_env,
        "planning_bankroll_dollars": planning_bankroll_dollars,
        "daily_risk_cap_dollars": daily_risk_cap_dollars,
        "contracts_per_order": contracts_per_order,
        "max_orders": max_orders,
        "max_yes_ask_dollars": max_yes_ask_dollars,
        "max_spread_dollars": max_spread_dollars,
        "max_hours_to_close": max_hours_to_close,
        "excluded_categories": list(excluded_categories),
        "data_source": data_source,
        "scan_csv": scan_csv,
        "events_fetched": len(events),
        "ranked_markets": len(ranked_rows),
        "category_counts": category_counts,
        "skip_counts": skip_counts,
        "planned_orders": len(plans),
        "total_planned_cost_dollars": total_planned_cost,
        "recommended_testing_bankroll_dollars": planning_bankroll_dollars,
        "actual_live_balance_dollars": live_balance_dollars,
        "actual_live_balance_source": balance_source,
        "balance_live_verified": balance_source == "live",
        "funding_gap_dollars": funding_gap_dollars,
        "funding_gap_for_current_plan_dollars": funding_gap_dollars,
        "balance_check_error": balance_error,
        "balance_cache_file": str(balance_cache_path),
        "balance_cache_age_seconds": balance_cache_age_seconds,
        "events_error": events_error,
        "board_warning": board_warning,
        "status": status,
        "orders": plans,
        "output_csv": str(csv_path),
    }

    output_path = out_dir / f"kalshi_micro_plan_summary_{stamp}.json"
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["output_file"] = str(output_path)
    return summary
