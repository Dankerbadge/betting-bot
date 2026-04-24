from __future__ import annotations

import argparse
import csv
import inspect
import json
from datetime import datetime
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any

from betbot.adapters import (
    CuratedNewsAdapter,
    KalshiMarketDataAdapter,
    OpticOddsConsensusAdapter,
    TheRundownMappingAdapter,
)
from betbot.alpha_scoreboard import run_alpha_scoreboard
from betbot.backtest import run_backtest
from betbot.bayes import conservative_planning_p
from betbot.canonical_universe import run_canonical_universe
from betbot.decision_matrix_hardening import run_decision_matrix_hardening
from betbot.coldmath_snapshot import run_coldmath_snapshot_summary
from betbot.coldmath_replication import run_coldmath_replication_plan
from betbot.commands.runtime_ops import run_effective_config, run_policy_check, run_render_board
from betbot.config import load_config
from betbot.dns_guard import run_dns_doctor
from betbot.io import load_candidates
from betbot.kalshi_focus_dossier import run_kalshi_focus_dossier
from betbot.kalshi_execution_frontier import run_kalshi_execution_frontier
from betbot.kalshi_autopilot import run_kalshi_autopilot
from betbot.kalshi_watchdog import run_kalshi_watchdog
from betbot.kalshi_micro_execute import run_kalshi_micro_execute
from betbot.kalshi_micro_gate import run_kalshi_micro_gate
from betbot.kalshi_micro_prior_execute import run_kalshi_micro_prior_execute
from betbot.kalshi_micro_prior_plan import run_kalshi_micro_prior_plan
from betbot.kalshi_micro_prior_trader import run_kalshi_micro_prior_trader
from betbot.kalshi_micro_prior_watch import run_kalshi_micro_prior_watch
from betbot.kalshi_micro_reconcile import run_kalshi_micro_reconcile
from betbot.kalshi_micro_status import run_kalshi_micro_status
from betbot.kalshi_micro_trader import run_kalshi_micro_trader
from betbot.kalshi_micro_watch import run_kalshi_micro_watch
from betbot.kalshi_mlb_map import run_kalshi_mlb_map
from betbot.kalshi_micro_plan import run_kalshi_micro_plan
from betbot.kalshi_arb_scan import run_kalshi_arb_scan
from betbot.kalshi_supervisor import run_kalshi_supervisor
from betbot.kalshi_ws_state import run_kalshi_ws_state_collect, run_kalshi_ws_state_replay
from betbot.kalshi_nonsports_categories import run_kalshi_nonsports_categories
from betbot.kalshi_nonsports_auto_priors import run_kalshi_nonsports_auto_priors
from betbot.kalshi_nonsports_capture import run_kalshi_nonsports_capture
from betbot.kalshi_nonsports_deltas import run_kalshi_nonsports_deltas
from betbot.kalshi_nonsports_persistence import run_kalshi_nonsports_persistence
from betbot.kalshi_nonsports_pressure import run_kalshi_nonsports_pressure
from betbot.kalshi_nonsports_priors import run_kalshi_nonsports_priors
from betbot.kalshi_nonsports_quality import run_kalshi_nonsports_quality
from betbot.kalshi_nonsports_research_queue import run_kalshi_nonsports_research_queue
from betbot.kalshi_nonsports_signals import run_kalshi_nonsports_signals
from betbot.kalshi_nonsports_scan import run_kalshi_nonsports_scan
from betbot.kalshi_nonsports_thresholds import run_kalshi_nonsports_thresholds
from betbot.kalshi_temperature_constraints import run_kalshi_temperature_constraint_scan
from betbot.kalshi_temperature_contract_specs import run_kalshi_temperature_contract_specs
from betbot.kalshi_temperature_metar_ingest import run_kalshi_temperature_metar_ingest
from betbot.kalshi_temperature_profitability import (
    run_kalshi_temperature_profitability,
    run_kalshi_temperature_refill_trial_balance,
)
from betbot.kalshi_temperature_execution_cost_tape import run_kalshi_temperature_execution_cost_tape
from betbot.kalshi_temperature_selection_quality import run_kalshi_temperature_selection_quality
from betbot.kalshi_temperature_bankroll_validation import (
    run_kalshi_temperature_alpha_gap_report,
    run_kalshi_temperature_bankroll_validation,
    run_kalshi_temperature_go_live_gate,
    run_kalshi_temperature_live_readiness,
)
from betbot.kalshi_temperature_settlement_state import run_kalshi_temperature_settlement_state
from betbot.kalshi_temperature_coverage_velocity_report import (
    run_kalshi_temperature_coverage_velocity_report,
    summarize_kalshi_temperature_coverage_velocity_report,
)
try:
    from betbot.kalshi_temperature_settled_outcome_throughput import (
        run_kalshi_temperature_settled_outcome_throughput,
        summarize_kalshi_temperature_settled_outcome_throughput,
    )
except ImportError:
    run_kalshi_temperature_settled_outcome_throughput = None
    summarize_kalshi_temperature_settled_outcome_throughput = None
from betbot.kalshi_temperature_trader import run_kalshi_temperature_shadow_watch, run_kalshi_temperature_trader
from betbot.kalshi_weather_catalog import run_kalshi_weather_catalog
from betbot.kalshi_weather_priors import run_kalshi_weather_priors, run_kalshi_weather_station_history_prewarm
from betbot.kalshi_climate_availability import run_kalshi_climate_realtime_router
from betbot.live_candidates import run_live_candidates
from betbot.live_paper import run_live_paper
from betbot.sports_archive import run_sports_archive
from betbot.ladder_grid import parse_float_list, parse_int_list, run_ladder_grid
from betbot.live_snapshot import run_live_snapshot
from betbot.live_smoke import run_live_smoke
from betbot.odds_audit import run_odds_audit
from betbot.onboarding import run_onboarding_check
from betbot.paper import run_paper
from betbot.polymarket_market_ingest import run_polymarket_market_data_ingest
from betbot.probability_path import (
    eventual_success_probability,
    hitting_probability,
    required_starting_units,
    units_from_dollars,
)
from betbot.research_audit import run_research_audit
from betbot.runtime.cycle_runner import CycleRunner, CycleRunnerConfig


def _load_json_dict(path_text: str | None) -> dict[str, Any] | None:
    if not path_text:
        return None
    try:
        payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_boolish(value: Any) -> bool | None:
    text = _normalize_text(value).lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _coerce_optimizer_profile_settings(payload: dict[str, Any]) -> dict[str, Any]:
    candidate: dict[str, Any] = payload
    for key in ("optimizer_profile", "profile", "selection_quality_profile", "settings"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidate = nested
            break
    settings: dict[str, Any] = {}
    aliases: dict[str, tuple[str, ...]] = {
        "historical_selection_quality_enabled": ("historical_selection_quality_enabled", "enabled"),
        "historical_selection_quality_lookback_hours": ("historical_selection_quality_lookback_hours", "lookback_hours"),
        "historical_selection_quality_min_resolved_market_sides": (
            "historical_selection_quality_min_resolved_market_sides",
            "min_resolved_market_sides",
        ),
        "historical_selection_quality_min_bucket_samples": (
            "historical_selection_quality_min_bucket_samples",
            "min_bucket_samples",
        ),
        "historical_selection_quality_probability_penalty_max": (
            "historical_selection_quality_probability_penalty_max",
            "probability_penalty_max",
        ),
        "historical_selection_quality_expected_edge_penalty_max": (
            "historical_selection_quality_expected_edge_penalty_max",
            "expected_edge_penalty_max",
        ),
        "historical_selection_quality_score_adjust_scale": (
            "historical_selection_quality_score_adjust_scale",
            "score_adjust_scale",
        ),
        "historical_selection_quality_profile_max_age_hours": (
            "historical_selection_quality_profile_max_age_hours",
            "max_profile_age_hours",
        ),
        "historical_selection_quality_preferred_model": (
            "historical_selection_quality_preferred_model",
            "preferred_attribution_model",
            "preferred_model",
        ),
    }
    for dest_key, source_keys in aliases.items():
        for source_key in source_keys:
            if source_key in candidate:
                settings[dest_key] = candidate[source_key]
                break
    return settings


def _load_optimizer_profile_reference(profile_json_path: str | None) -> dict[str, Any]:
    resolved_path = _normalize_text(profile_json_path)
    if not resolved_path:
        return {"status": "not_provided", "source_file": "", "applied": False, "settings": {}}
    path = Path(resolved_path)
    payload = _load_json_dict(str(path))
    if payload is None:
        return {
            "status": "optimizer_profile_parse_failed",
            "source_file": str(path),
            "applied": False,
            "settings": {},
        }
    settings = _coerce_optimizer_profile_settings(payload)
    if not settings:
        return {
            "status": "optimizer_profile_no_settings",
            "source_file": str(path),
            "applied": False,
            "settings": {},
        }
    return {"status": "applied", "source_file": str(path), "applied": True, "settings": settings}


def _load_weather_pattern_profile_reference(profile_json_path: str | None) -> dict[str, Any]:
    resolved_path = _normalize_text(profile_json_path)
    if not resolved_path:
        return {"status": "not_provided", "source_file": "", "applied": False, "profile": {}}
    path = Path(resolved_path)
    payload = _load_json_dict(str(path))
    if payload is None:
        return {
            "status": "weather_pattern_profile_parse_failed",
            "source_file": str(path),
            "applied": False,
            "profile": {},
        }
    return {"status": "applied", "source_file": str(path), "applied": True, "profile": payload}


def _invoke_runner_with_supported_kwargs(
    runner: Any,
    kwargs: dict[str, Any],
) -> tuple[dict[str, Any], tuple[str, ...]]:
    filtered_kwargs = dict(kwargs)
    ignored_keys: list[str] = []
    try:
        signature = inspect.signature(runner)
    except (TypeError, ValueError):
        signature = None
    if signature is not None:
        accepts_var_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
        )
        if not accepts_var_kwargs:
            supported_names = set(signature.parameters.keys())
            filtered_kwargs = {}
            for key, value in kwargs.items():
                if key in supported_names:
                    filtered_kwargs[key] = value
                else:
                    ignored_keys.append(key)
    return runner(**filtered_kwargs), tuple(ignored_keys)


def _apply_micro_live_50_temperature_profile(args: argparse.Namespace) -> dict[str, Any] | None:
    if not bool(getattr(args, "micro_live_50", False)):
        return None

    # Enforce strict micro-live caps for a $50 pilot and only allow equal-or-more
    # conservative overrides from CLI.
    args.planning_bankroll = 50.0
    args.daily_risk_cap = min(float(args.daily_risk_cap), 3.0)
    args.max_total_deployed_pct = min(float(args.max_total_deployed_pct), 0.2)
    args.max_live_cost_per_day_dollars = min(float(args.max_live_cost_per_day_dollars), 3.0)
    args.max_live_submissions_per_day = min(int(args.max_live_submissions_per_day), 3)
    args.max_orders = min(int(args.max_orders), 3)
    args.contracts_per_order = 1
    # Keep micro-live entries away from expensive strikes where edge is most
    # likely to get siphoned by slippage/fees.
    args.yes_max_entry_price = min(float(args.yes_max_entry_price), 0.85)
    args.no_max_entry_price = min(float(args.no_max_entry_price), 0.85)
    micro_min_metar_ingest_quality_score = 0.85
    micro_min_metar_fresh_station_coverage_ratio = 0.75
    if hasattr(args, "min_metar_ingest_quality_score"):
        if args.min_metar_ingest_quality_score is None:
            args.min_metar_ingest_quality_score = micro_min_metar_ingest_quality_score
        else:
            args.min_metar_ingest_quality_score = max(
                float(args.min_metar_ingest_quality_score),
                micro_min_metar_ingest_quality_score,
            )
    if hasattr(args, "min_metar_fresh_station_coverage_ratio"):
        if args.min_metar_fresh_station_coverage_ratio is None:
            args.min_metar_fresh_station_coverage_ratio = micro_min_metar_fresh_station_coverage_ratio
        else:
            args.min_metar_fresh_station_coverage_ratio = max(
                float(args.min_metar_fresh_station_coverage_ratio),
                micro_min_metar_fresh_station_coverage_ratio,
            )
    if hasattr(args, "require_metar_ingest_status_ready"):
        args.require_metar_ingest_status_ready = True
    if hasattr(args, "high_price_edge_guard_enabled"):
        args.high_price_edge_guard_enabled = True
    if hasattr(args, "high_price_edge_guard_min_entry_price_dollars"):
        current_entry_floor = _parse_float(getattr(args, "high_price_edge_guard_min_entry_price_dollars", None))
        if current_entry_floor is None:
            args.high_price_edge_guard_min_entry_price_dollars = 0.85
        else:
            # Lower trigger floor is stricter because more entries are screened.
            args.high_price_edge_guard_min_entry_price_dollars = min(float(current_entry_floor), 0.85)
    if hasattr(args, "high_price_edge_guard_min_expected_edge_net"):
        current_min_edge = _parse_float(getattr(args, "high_price_edge_guard_min_expected_edge_net", None))
        if current_min_edge is None:
            args.high_price_edge_guard_min_expected_edge_net = 0.0
        else:
            args.high_price_edge_guard_min_expected_edge_net = max(float(current_min_edge), 0.0)
    if hasattr(args, "high_price_edge_guard_min_edge_to_risk_ratio"):
        current_min_ratio = _parse_float(getattr(args, "high_price_edge_guard_min_edge_to_risk_ratio", None))
        if current_min_ratio is None:
            args.high_price_edge_guard_min_edge_to_risk_ratio = 0.02
        else:
            args.high_price_edge_guard_min_edge_to_risk_ratio = max(float(current_min_ratio), 0.02)

    max_open_exposure_dollars = round(float(args.planning_bankroll) * float(args.max_total_deployed_pct), 6)
    return {
        "planning_bankroll_dollars": round(float(args.planning_bankroll), 6),
        "max_daily_loss_dollars": round(float(args.daily_risk_cap), 6),
        "max_total_open_exposure_dollars": max_open_exposure_dollars,
        "max_total_deployed_pct": round(float(args.max_total_deployed_pct), 6),
        "max_live_cost_per_day_dollars": round(float(args.max_live_cost_per_day_dollars), 6),
        "max_live_submissions_per_day": int(args.max_live_submissions_per_day),
        "max_orders_per_loop": int(args.max_orders),
        "contracts_per_order": int(args.contracts_per_order),
        "yes_max_entry_price_dollars": round(float(args.yes_max_entry_price), 6),
        "no_max_entry_price_dollars": round(float(args.no_max_entry_price), 6),
        "min_metar_ingest_quality_score": round(float(args.min_metar_ingest_quality_score), 6)
        if getattr(args, "min_metar_ingest_quality_score", None) is not None
        else None,
        "min_metar_fresh_station_coverage_ratio": round(float(args.min_metar_fresh_station_coverage_ratio), 6)
        if getattr(args, "min_metar_fresh_station_coverage_ratio", None) is not None
        else None,
        "require_metar_ingest_status_ready": bool(getattr(args, "require_metar_ingest_status_ready", False)),
        "high_price_edge_guard_enabled": bool(getattr(args, "high_price_edge_guard_enabled", False)),
        "high_price_edge_guard_min_entry_price_dollars": round(
            float(args.high_price_edge_guard_min_entry_price_dollars),
            6,
        )
        if getattr(args, "high_price_edge_guard_min_entry_price_dollars", None) is not None
        else None,
        "high_price_edge_guard_min_expected_edge_net": round(
            float(args.high_price_edge_guard_min_expected_edge_net),
            6,
        )
        if getattr(args, "high_price_edge_guard_min_expected_edge_net", None) is not None
        else None,
        "high_price_edge_guard_min_edge_to_risk_ratio": round(
            float(args.high_price_edge_guard_min_edge_to_risk_ratio),
            6,
        )
        if getattr(args, "high_price_edge_guard_min_edge_to_risk_ratio", None) is not None
        else None,
    }


def _run_settled_outcome_throughput_cli(
    *,
    output_dir: str,
    summarize_only: bool,
) -> dict[str, Any]:
    kwargs = {"output_dir": output_dir}

    def _invoke_optional_runner(runner: Any) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
        if not callable(runner):
            return None, ()
        payload, ignored = _invoke_runner_with_supported_kwargs(runner, kwargs)
        if isinstance(payload, dict):
            return payload, ignored
        if isinstance(payload, str):
            try:
                decoded = json.loads(payload)
            except json.JSONDecodeError:
                decoded = None
            if isinstance(decoded, dict):
                return decoded, ignored
        return {"status": "invalid_runner_payload", "payload_type": type(payload).__name__}, ignored

    if summarize_only:
        summary, ignored = _invoke_optional_runner(summarize_kalshi_temperature_settled_outcome_throughput)
        if summary is None:
            summary, ignored = _invoke_optional_runner(run_kalshi_temperature_settled_outcome_throughput)
            if summary is None:
                return {
                    "status": "runner_unavailable",
                    "mode": "summarize_only",
                    "runner_module": "betbot.kalshi_temperature_settled_outcome_throughput",
                    "message": "Settled-outcome throughput runner is unavailable in this environment.",
                }
            summary.setdefault("mode", "summarize_only_fallback_run")
        else:
            summary.setdefault("mode", "summarize_only")
    else:
        summary, ignored = _invoke_optional_runner(run_kalshi_temperature_settled_outcome_throughput)
        if summary is None:
            summary, ignored = _invoke_optional_runner(summarize_kalshi_temperature_settled_outcome_throughput)
            if summary is None:
                return {
                    "status": "runner_unavailable",
                    "mode": "run",
                    "runner_module": "betbot.kalshi_temperature_settled_outcome_throughput",
                    "message": "Settled-outcome throughput runner is unavailable in this environment.",
                }
            summary.setdefault("mode", "run_fallback_summary")
        else:
            summary.setdefault("mode", "run")

    if ignored:
        summary["runner_ignored_cli_kwargs"] = sorted(ignored)
    return summary


def _run_coverage_velocity_report_cli(
    *,
    output_dir: str,
    history_limit: int,
    summarize_only: bool,
) -> dict[str, Any]:
    kwargs = {"output_dir": output_dir, "history_limit": history_limit}

    def _invoke_optional_runner(runner: Any) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
        if not callable(runner):
            return None, ()
        payload, ignored = _invoke_runner_with_supported_kwargs(runner, kwargs)
        if isinstance(payload, dict):
            return payload, ignored
        if isinstance(payload, str):
            try:
                decoded = json.loads(payload)
            except json.JSONDecodeError:
                decoded = None
            if isinstance(decoded, dict):
                return decoded, ignored
        return {"status": "invalid_runner_payload", "payload_type": type(payload).__name__}, ignored

    if summarize_only:
        summary, ignored = _invoke_optional_runner(summarize_kalshi_temperature_coverage_velocity_report)
        if summary is None:
            summary, ignored = _invoke_optional_runner(run_kalshi_temperature_coverage_velocity_report)
            if summary is None:
                return {
                    "status": "runner_unavailable",
                    "mode": "summarize_only",
                    "runner_module": "betbot.kalshi_temperature_coverage_velocity_report",
                    "message": "Coverage-velocity report runner is unavailable in this environment.",
                }
            summary.setdefault("mode", "summarize_only_fallback_run")
        else:
            summary.setdefault("mode", "summarize_only")
    else:
        summary, ignored = _invoke_optional_runner(run_kalshi_temperature_coverage_velocity_report)
        if summary is None:
            summary, ignored = _invoke_optional_runner(summarize_kalshi_temperature_coverage_velocity_report)
            if summary is None:
                return {
                    "status": "runner_unavailable",
                    "mode": "run",
                    "runner_module": "betbot.kalshi_temperature_coverage_velocity_report",
                    "message": "Coverage-velocity report runner is unavailable in this environment.",
                }
            summary.setdefault("mode", "run_fallback_summary")
        else:
            summary.setdefault("mode", "run")

    if ignored:
        summary["runner_ignored_cli_kwargs"] = sorted(ignored)
    return summary


def _promote_weather_pattern_risk_off_summary(summary: dict[str, Any]) -> None:
    sources: list[dict[str, Any]] = [summary]
    for key in ("bridge_plan_summary", "plan_summary", "intent_summary", "weather_pattern_summary"):
        candidate = summary.get(key)
        if isinstance(candidate, dict):
            sources.append(candidate)
    for source in list(sources):
        for container_key in ("weather_pattern_risk_off", "weather_pattern_risk_off_summary", "risk_off"):
            candidate = source.get(container_key)
            if isinstance(candidate, dict):
                sources.append(candidate)
    aliases: dict[str, tuple[str, ...]] = {
        "weather_pattern_risk_off_enabled": ("weather_pattern_risk_off_enabled", "enabled"),
        "weather_pattern_risk_off_applied": ("weather_pattern_risk_off_applied", "applied"),
        "weather_pattern_risk_off_status": ("weather_pattern_risk_off_status", "status"),
        "weather_pattern_risk_off_application_status": (
            "weather_pattern_risk_off_application_status",
            "application_status",
        ),
        "weather_pattern_risk_off_triggered": ("weather_pattern_risk_off_triggered", "triggered"),
        "weather_pattern_risk_off_concentration_threshold": (
            "weather_pattern_risk_off_concentration_threshold",
            "concentration_threshold",
        ),
        "weather_pattern_risk_off_min_attempts": ("weather_pattern_risk_off_min_attempts", "min_attempts"),
        "weather_pattern_risk_off_stale_metar_share_threshold": (
            "weather_pattern_risk_off_stale_metar_share_threshold",
            "stale_metar_share_threshold",
        ),
    }
    for destination_key, source_keys in aliases.items():
        if summary.get(destination_key) is not None:
            continue
        direct_value = summary.get(destination_key)
        if direct_value is not None:
            summary[destination_key] = direct_value
            continue
        for source in sources[1:]:
            direct_source_value = source.get(destination_key)
            if direct_source_value is not None:
                summary[destination_key] = direct_source_value
                break
        if summary.get(destination_key) is not None:
            continue
        fallback_keys = source_keys[1:]
        for source in sources[1:]:
            for source_key in fallback_keys:
                value = source.get(source_key)
                if value is not None:
                    summary[destination_key] = value
                    break
            if summary.get(destination_key) is not None:
                break


def run_kalshi_temperature_weather_pattern(
    **kwargs: Any,
) -> dict[str, Any]:
    from betbot.kalshi_temperature_weather_pattern import run_kalshi_temperature_weather_pattern as _runner

    return _runner(**kwargs)


def run_kalshi_temperature_recovery_advisor(
    **kwargs: Any,
) -> dict[str, Any]:
    from betbot.kalshi_temperature_recovery_advisor import run_kalshi_temperature_recovery_advisor as _runner

    return _runner(**kwargs)


def run_kalshi_temperature_recovery_loop(
    **kwargs: Any,
) -> dict[str, Any]:
    from betbot.kalshi_temperature_recovery_loop import run_kalshi_temperature_recovery_loop as _runner

    return _runner(**kwargs)


def run_kalshi_temperature_recovery_campaign(
    **kwargs: Any,
) -> dict[str, Any]:
    from betbot.kalshi_temperature_recovery_campaign import run_kalshi_temperature_recovery_campaign as _runner

    return _runner(**kwargs)


def _stage_optimizer_intent_files(intent_files: list[str], staging_dir: Path) -> tuple[list[str], list[str]]:
    staged_files: list[str] = []
    warnings: list[str] = []
    for index, raw_path in enumerate(intent_files, start=1):
        source_path = Path(str(raw_path))
        if not source_path.exists():
            warnings.append(f"missing:{source_path}")
            continue
        if source_path.is_dir():
            warnings.append(f"directory_skipped:{source_path}")
            continue
        suffix = source_path.suffix or ".csv"
        staged_name = f"kalshi_temperature_trade_intents_{index:02d}_{source_path.stem}{suffix}"
        staged_path = staging_dir / staged_name
        try:
            shutil.copy2(source_path, staged_path)
        except OSError as exc:
            warnings.append(f"copy_failed:{source_path}:{exc}")
            continue
        staged_files.append(str(staged_path))
    return staged_files, warnings


def _optimizer_metric_key(summary: dict[str, Any]) -> tuple[float, float, float, int]:
    intent_window = summary.get("intent_window") if isinstance(summary.get("intent_window"), dict) else {}
    profile = summary.get("profile") if isinstance(summary.get("profile"), dict) else {}
    return (
        float(_parse_float(intent_window.get("approved_adjusted_rate")) or 0.0),
        float(_parse_float(intent_window.get("adjusted_rate")) or 0.0),
        float(_parse_float(profile.get("evidence_confidence")) or 0.0),
        int(_parse_int(intent_window.get("rows_total")) or 0),
    )


def _optimizer_candidate_configs(
    *,
    lookback_hours_min: float,
    lookback_hours_max: float,
    lookback_hours_step: float,
    intent_hours_min: float,
    intent_hours_max: float,
    intent_hours_step: float,
    min_resolved_market_sides_min: int,
    min_resolved_market_sides_max: int,
    min_bucket_samples_min: int,
    min_bucket_samples_max: int,
    probability_penalty_max_min: float,
    probability_penalty_max_max: float,
    expected_edge_penalty_max_min: float,
    expected_edge_penalty_max_max: float,
    score_adjust_scale_min: float,
    score_adjust_scale_max: float,
    score_adjust_scale_step: float,
    preferred_attribution_model: str,
    top_n: int,
) -> list[dict[str, Any]]:
    def _clamp_bounds(lo: float, hi: float) -> tuple[float, float]:
        lo = float(lo)
        hi = float(hi)
        if hi < lo:
            lo, hi = hi, lo
        return lo, hi

    def _mid(lo: float, hi: float) -> float:
        return lo + ((hi - lo) / 2.0)

    lb_min, lb_max = _clamp_bounds(lookback_hours_min, lookback_hours_max)
    ih_min, ih_max = _clamp_bounds(intent_hours_min, intent_hours_max)
    pp_min, pp_max = _clamp_bounds(probability_penalty_max_min, probability_penalty_max_max)
    ee_min, ee_max = _clamp_bounds(expected_edge_penalty_max_min, expected_edge_penalty_max_max)
    sa_min, sa_max = _clamp_bounds(score_adjust_scale_min, score_adjust_scale_max)

    configs = [
        {
            "lookback_hours": round(lb_min, 3),
            "intent_hours": round(ih_min, 3),
            "min_resolved_market_sides": max(1, int(min_resolved_market_sides_min)),
            "min_bucket_samples": max(1, int(min_bucket_samples_min)),
            "probability_penalty_max": round(pp_min, 6),
            "expected_edge_penalty_max": round(ee_min, 6),
            "score_adjust_scale": round(sa_min, 6),
            "preferred_attribution_model": preferred_attribution_model,
            "top_n": int(top_n),
        },
        {
            "lookback_hours": round(_mid(lb_min, lb_max), 3),
            "intent_hours": round(_mid(ih_min, ih_max), 3),
            "min_resolved_market_sides": max(
                1, int(_mid(float(min_resolved_market_sides_min), float(min_resolved_market_sides_max)))
            ),
            "min_bucket_samples": max(1, int(_mid(float(min_bucket_samples_min), float(min_bucket_samples_max)))),
            "probability_penalty_max": round(_mid(pp_min, pp_max), 6),
            "expected_edge_penalty_max": round(_mid(ee_min, ee_max), 6),
            "score_adjust_scale": round(_mid(sa_min, sa_max), 6),
            "preferred_attribution_model": preferred_attribution_model,
            "top_n": int(top_n),
        },
        {
            "lookback_hours": round(lb_max, 3),
            "intent_hours": round(ih_max, 3),
            "min_resolved_market_sides": max(1, int(min_resolved_market_sides_max)),
            "min_bucket_samples": max(1, int(min_bucket_samples_max)),
            "probability_penalty_max": round(pp_max, 6),
            "expected_edge_penalty_max": round(ee_max, 6),
            "score_adjust_scale": round(sa_max, 6),
            "preferred_attribution_model": preferred_attribution_model,
            "top_n": int(top_n),
        },
    ]
    if lb_min != lb_max or ih_min != ih_max:
        configs.append(
            {
                "lookback_hours": round(lb_max, 3),
                "intent_hours": round(ih_min, 3),
                "min_resolved_market_sides": max(1, int(min_resolved_market_sides_max)),
                "min_bucket_samples": max(1, int(min_bucket_samples_min)),
                "probability_penalty_max": round(pp_min, 6),
                "expected_edge_penalty_max": round(ee_max, 6),
                "score_adjust_scale": round(_mid(sa_min, sa_max), 6),
                "preferred_attribution_model": preferred_attribution_model,
                "top_n": int(top_n),
            }
        )

    unique_configs: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for config in configs:
        signature = tuple(sorted(config.items()))
        if signature in seen:
            continue
        seen.add(signature)
        unique_configs.append(config)
    return unique_configs


def run_temperature_growth_optimizer(
    *,
    output_dir: str,
    intent_files: list[str],
    lookback_hours_min: float,
    lookback_hours_max: float,
    lookback_hours_step: float,
    intent_hours_min: float,
    intent_hours_max: float,
    intent_hours_step: float,
    min_resolved_market_sides_min: int,
    min_resolved_market_sides_max: int,
    min_bucket_samples_min: int,
    min_bucket_samples_max: int,
    probability_penalty_max_min: float,
    probability_penalty_max_max: float,
    expected_edge_penalty_max_min: float,
    expected_edge_penalty_max_max: float,
    score_adjust_scale_min: float,
    score_adjust_scale_max: float,
    score_adjust_scale_step: float,
    preferred_attribution_model: str,
    top_n: int = 10,
    search_bounds_json: str | None = None,
) -> dict[str, Any]:
    captured_at = datetime.now().astimezone()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bounds_payload = _load_json_dict(search_bounds_json) if _normalize_text(search_bounds_json) else None
    if isinstance(bounds_payload, dict):
        bounds_source = bounds_payload.get("bounds") if isinstance(bounds_payload.get("bounds"), dict) else bounds_payload
        lookback_hours_min = _parse_float(bounds_source.get("lookback_hours_min")) or lookback_hours_min
        lookback_hours_max = _parse_float(bounds_source.get("lookback_hours_max")) or lookback_hours_max
        lookback_hours_step = _parse_float(bounds_source.get("lookback_hours_step")) or lookback_hours_step
        intent_hours_min = _parse_float(bounds_source.get("intent_hours_min")) or intent_hours_min
        intent_hours_max = _parse_float(bounds_source.get("intent_hours_max")) or intent_hours_max
        intent_hours_step = _parse_float(bounds_source.get("intent_hours_step")) or intent_hours_step
        min_resolved_market_sides_min = (
            _parse_int(bounds_source.get("min_resolved_market_sides_min")) or min_resolved_market_sides_min
        )
        min_resolved_market_sides_max = (
            _parse_int(bounds_source.get("min_resolved_market_sides_max")) or min_resolved_market_sides_max
        )
        min_bucket_samples_min = _parse_int(bounds_source.get("min_bucket_samples_min")) or min_bucket_samples_min
        min_bucket_samples_max = _parse_int(bounds_source.get("min_bucket_samples_max")) or min_bucket_samples_max
        probability_penalty_max_min = (
            _parse_float(bounds_source.get("probability_penalty_max_min")) or probability_penalty_max_min
        )
        probability_penalty_max_max = (
            _parse_float(bounds_source.get("probability_penalty_max_max")) or probability_penalty_max_max
        )
        expected_edge_penalty_max_min = (
            _parse_float(bounds_source.get("expected_edge_penalty_max_min")) or expected_edge_penalty_max_min
        )
        expected_edge_penalty_max_max = (
            _parse_float(bounds_source.get("expected_edge_penalty_max_max")) or expected_edge_penalty_max_max
        )
        score_adjust_scale_min = _parse_float(bounds_source.get("score_adjust_scale_min")) or score_adjust_scale_min
        score_adjust_scale_max = _parse_float(bounds_source.get("score_adjust_scale_max")) or score_adjust_scale_max
        score_adjust_scale_step = (
            _parse_float(bounds_source.get("score_adjust_scale_step")) or score_adjust_scale_step
        )
        preferred_attribution_model = _normalize_text(
            bounds_source.get("preferred_attribution_model") or bounds_source.get("preferred_model")
        ) or preferred_attribution_model
        top_n = _parse_int(bounds_source.get("top_n")) or top_n

    with tempfile.TemporaryDirectory(prefix=".temperature_growth_optimizer_", dir=str(out_dir)) as temp_dir:
        staging_dir = Path(temp_dir) / "intents"
        staging_dir.mkdir(parents=True, exist_ok=True)
        staged_files, stage_warnings = _stage_optimizer_intent_files(intent_files, staging_dir)
        if not staged_files:
            return {
                "status": "no_intent_files",
                "captured_at": captured_at.isoformat(),
                "output_dir": str(out_dir),
                "profile_application_status": "not_applied",
                "profile_json": "",
                "warnings": stage_warnings,
                "intent_files": [str(Path(item)) for item in intent_files],
            }

        candidate_configs = _optimizer_candidate_configs(
            lookback_hours_min=lookback_hours_min,
            lookback_hours_max=lookback_hours_max,
            lookback_hours_step=lookback_hours_step,
            intent_hours_min=intent_hours_min,
            intent_hours_max=intent_hours_max,
            intent_hours_step=intent_hours_step,
            min_resolved_market_sides_min=min_resolved_market_sides_min,
            min_resolved_market_sides_max=min_resolved_market_sides_max,
            min_bucket_samples_min=min_bucket_samples_min,
            min_bucket_samples_max=min_bucket_samples_max,
            probability_penalty_max_min=probability_penalty_max_min,
            probability_penalty_max_max=probability_penalty_max_max,
            expected_edge_penalty_max_min=expected_edge_penalty_max_min,
            expected_edge_penalty_max_max=expected_edge_penalty_max_max,
            score_adjust_scale_min=score_adjust_scale_min,
            score_adjust_scale_max=score_adjust_scale_max,
            score_adjust_scale_step=score_adjust_scale_step,
            preferred_attribution_model=preferred_attribution_model,
            top_n=top_n,
        )

        evaluations: list[dict[str, Any]] = []
        best_summary: dict[str, Any] | None = None
        best_config: dict[str, Any] | None = None
        best_score: tuple[float, float, float, int] | None = None
        for index, config in enumerate(candidate_configs, start=1):
            summary = run_kalshi_temperature_selection_quality(
                output_dir=str(staging_dir),
                lookback_hours=config["lookback_hours"],
                min_resolved_market_sides=config["min_resolved_market_sides"],
                min_bucket_samples=config["min_bucket_samples"],
                preferred_attribution_model=config["preferred_attribution_model"],
                max_profile_age_hours=max(0.0, float(max(lookback_hours_max, intent_hours_max))),
                probability_penalty_max=config["probability_penalty_max"],
                expected_edge_penalty_max=config["expected_edge_penalty_max"],
                score_adjust_scale=config["score_adjust_scale"],
                intent_hours=config["intent_hours"],
                top_n=max(1, int(config["top_n"])),
            )
            metric = _optimizer_metric_key(summary)
            evaluations.append(
                {
                    "index": index,
                    "config": config,
                    "metric": {
                        "approved_adjusted_rate": metric[0],
                        "adjusted_rate": metric[1],
                        "evidence_confidence": metric[2],
                        "rows_total": metric[3],
                    },
                    "selection_quality_output_file": summary.get("output_file"),
                }
            )
            if best_score is None or metric > best_score:
                best_score = metric
                best_summary = summary
                best_config = config

        assert best_summary is not None
        assert best_config is not None

        profile_payload = {
            "status": "ready",
            "captured_at": captured_at.isoformat(),
            "source": "kalshi-temperature-growth-optimizer",
            "profile_application_status": "applied" if bool(best_summary.get("profile")) else "not_applied",
            "search_bounds": {
                "lookback_hours_min": float(lookback_hours_min),
                "lookback_hours_max": float(lookback_hours_max),
                "lookback_hours_step": float(lookback_hours_step),
                "intent_hours_min": float(intent_hours_min),
                "intent_hours_max": float(intent_hours_max),
                "intent_hours_step": float(intent_hours_step),
                "min_resolved_market_sides_min": int(min_resolved_market_sides_min),
                "min_resolved_market_sides_max": int(min_resolved_market_sides_max),
                "min_bucket_samples_min": int(min_bucket_samples_min),
                "min_bucket_samples_max": int(min_bucket_samples_max),
                "probability_penalty_max_min": float(probability_penalty_max_min),
                "probability_penalty_max_max": float(probability_penalty_max_max),
                "expected_edge_penalty_max_min": float(expected_edge_penalty_max_min),
                "expected_edge_penalty_max_max": float(expected_edge_penalty_max_max),
                "score_adjust_scale_min": float(score_adjust_scale_min),
                "score_adjust_scale_max": float(score_adjust_scale_max),
                "score_adjust_scale_step": float(score_adjust_scale_step),
                "preferred_attribution_model": preferred_attribution_model,
                "top_n": int(top_n),
            },
            "selected_profile": best_summary.get("profile") if isinstance(best_summary.get("profile"), dict) else {},
            "optimizer_profile": {
                "historical_selection_quality_enabled": True,
                "historical_selection_quality_lookback_hours": float(best_config["lookback_hours"]),
                "historical_selection_quality_min_resolved_market_sides": int(best_config["min_resolved_market_sides"]),
                "historical_selection_quality_min_bucket_samples": int(best_config["min_bucket_samples"]),
                "historical_selection_quality_probability_penalty_max": float(best_config["probability_penalty_max"]),
                "historical_selection_quality_expected_edge_penalty_max": float(best_config["expected_edge_penalty_max"]),
                "historical_selection_quality_score_adjust_scale": float(best_config["score_adjust_scale"]),
                "historical_selection_quality_profile_max_age_hours": max(
                    0.0,
                    float(max(lookback_hours_max, intent_hours_max)),
                ),
                "historical_selection_quality_preferred_model": preferred_attribution_model,
            },
            "best_candidate": best_config,
            "best_metric": {
                "approved_adjusted_rate": best_score[0] if best_score else 0.0,
                "adjusted_rate": best_score[1] if best_score else 0.0,
                "evidence_confidence": best_score[2] if best_score else 0.0,
                "rows_total": best_score[3] if best_score else 0,
            },
            "candidate_count": len(candidate_configs),
            "evaluations": evaluations[: max(1, int(top_n))],
            "intent_files": staged_files,
            "warnings": stage_warnings,
        }
        profile_stamp = captured_at.strftime("%Y%m%d_%H%M%S")
        profile_path = out_dir / f"kalshi_temperature_growth_optimizer_profile_{profile_stamp}.json"
        profile_path.write_text(json.dumps(profile_payload, indent=2, sort_keys=True), encoding="utf-8")

        summary = {
            "status": "ready",
            "captured_at": captured_at.isoformat(),
            "output_dir": str(out_dir),
            "profile_application_status": profile_payload["profile_application_status"],
            "profile_json": str(profile_path),
            "profile": profile_payload,
            "search_bounds": profile_payload["search_bounds"],
            "best_candidate": best_config,
            "best_metric": profile_payload["best_metric"],
            "candidate_count": len(candidate_configs),
            "intent_files": staged_files,
            "warnings": stage_warnings,
            "evaluations": evaluations[: max(1, int(top_n))],
            "selection_quality_output_file": best_summary.get("output_file"),
        }
        summary_path = out_dir / f"kalshi_temperature_growth_optimizer_summary_{profile_stamp}.json"
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        summary["output_file"] = str(summary_path)
        summary["profile_json"] = str(profile_path)
        return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Strict-statistics betting bot CLI")

    subparsers = parser.add_subparsers(dest="command", required=True)

    backtest = subparsers.add_parser("backtest", help="Run historical backtest")
    backtest.add_argument("--config", help="Optional JSON config path", default=None)
    backtest.add_argument("--input", required=True, help="Input CSV path")
    backtest.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    backtest.add_argument("--output-dir", default="outputs", help="Output directory")

    paper = subparsers.add_parser("paper", help="Run paper decision engine")
    paper.add_argument("--config", help="Optional JSON config path", default=None)
    paper.add_argument("--input", required=True, help="Input CSV path")
    paper.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    paper.add_argument("--output-dir", default="outputs", help="Output directory")
    paper.add_argument(
        "--simulate-with-outcomes",
        action="store_true",
        help="If outcome exists, settle paper bets for simulation",
    )

    analyze = subparsers.add_parser(
        "analyze",
        help="Compute probability-path and conservative planning stats",
    )
    analyze.add_argument(
        "--starting-bankroll",
        type=float,
        default=10.0,
        help="Starting bankroll in dollars",
    )
    analyze.add_argument(
        "--risk-per-effort",
        type=float,
        default=10.0,
        help="Fixed risk per attempt in dollars",
    )
    analyze.add_argument(
        "--targets",
        default="20,50,100,250,1000,10000",
        help="Comma-separated target bankrolls in dollars",
    )
    analyze.add_argument(
        "--p-values",
        default="0.50,0.51,0.52,0.55,0.60",
        help="Comma-separated p values to evaluate",
    )
    analyze.add_argument(
        "--history-input",
        default=None,
        help="Optional CSV with outcome column to estimate Bayesian planning p",
    )
    analyze.add_argument(
        "--confidence",
        type=float,
        default=0.95,
        help="Credible interval confidence for Bayesian estimate",
    )
    analyze.add_argument("--output-dir", default="outputs", help="Output directory")

    effective_config = subparsers.add_parser(
        "effective-config",
        help="Render merged runtime config with deterministic fingerprints",
    )
    effective_config.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root override for config resolution",
    )

    policy_check = subparsers.add_parser(
        "policy-check",
        help="Validate lane permissions and emit policy snapshot",
    )
    policy_check.add_argument(
        "--lane",
        default="research",
        help="Permission lane to evaluate",
    )
    policy_check.add_argument(
        "--lane-policy-path",
        default=None,
        help="Optional path to lane policy YAML",
    )

    render_board = subparsers.add_parser(
        "render-board",
        help="Render board projection from runtime cycle/board JSON",
    )
    render_board.add_argument(
        "--board-json",
        default=None,
        help="Optional explicit board JSON path",
    )
    render_board.add_argument(
        "--cycle-json",
        default=None,
        help="Optional explicit cycle JSON path",
    )
    render_board.add_argument("--output-dir", default="outputs", help="Output directory")

    runtime_cycle = subparsers.add_parser(
        "runtime-cycle",
        help="Run one runtime cycle with adapter-backed source health and ticket selection",
    )
    runtime_cycle.add_argument("--lane", default="research", help="Permission lane to evaluate")
    runtime_cycle.add_argument("--output-dir", default="outputs", help="Output directory")
    runtime_cycle.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root override for config resolution",
    )
    runtime_cycle.add_argument(
        "--lane-policy-path",
        default=None,
        help="Optional path to lane policy YAML",
    )
    runtime_cycle.add_argument(
        "--hard-required-sources",
        default="",
        help="Optional comma-separated override for hard required providers",
    )
    runtime_cycle.add_argument(
        "--request-live-submit",
        action="store_true",
        help="Attempt live order submit when lane policy allows",
    )
    runtime_cycle.add_argument(
        "--live-env-file",
        default=None,
        help="Optional env file for live venue adapter initialization",
    )
    runtime_cycle.add_argument(
        "--live-timeout-seconds",
        type=float,
        default=10.0,
        help="Live venue HTTP timeout budget",
    )
    runtime_cycle.add_argument(
        "--allow-simulated-live-adapter",
        action="store_true",
        help="Allow local simulated live adapter in live_execute lane",
    )
    runtime_cycle.add_argument(
        "--approval-json-path",
        default=None,
        help="Optional approval JSON path for live submit lanes",
    )
    runtime_cycle.add_argument(
        "--ticket-market",
        default="SIM-MARKET",
        help="Explicit market ticker override (default resolves from scored candidates)",
    )
    runtime_cycle.add_argument(
        "--ticket-side",
        default="yes",
        choices=("yes", "no"),
        help="Explicit side override when --ticket-market is provided",
    )
    runtime_cycle.add_argument(
        "--ticket-max-cost",
        type=float,
        default=1.0,
        help="Explicit max cost override when --ticket-market is provided",
    )
    runtime_cycle.add_argument(
        "--ticket-expires-at",
        default=None,
        help="Optional explicit ISO timestamp for ticket expiry",
    )
    runtime_cycle.add_argument(
        "--include-therundown-mapping",
        action="store_true",
        help="Include TheRundown mapping adapter in source health checks",
    )
    runtime_cycle.add_argument(
        "--coldmath-snapshot-dir",
        default="tmp/coldmath_snapshot",
        help="Directory containing ColdMath equity/positions snapshot CSVs",
    )
    runtime_cycle.add_argument(
        "--coldmath-wallet-address",
        default="",
        help="Wallet address label used for optional ColdMath snapshot refresh",
    )
    runtime_cycle.add_argument(
        "--coldmath-stale-hours",
        type=float,
        default=48.0,
        help="Staleness threshold for ColdMath snapshot summary",
    )
    runtime_cycle.add_argument(
        "--coldmath-refresh-from-api",
        action="store_true",
        help="Refresh ColdMath snapshot CSVs from Polymarket data API before cycle execution",
    )
    runtime_cycle.add_argument(
        "--coldmath-data-api-base-url",
        default="https://data-api.polymarket.com",
        help="Polymarket data API base URL for ColdMath snapshot refresh",
    )
    runtime_cycle.add_argument(
        "--coldmath-api-timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout for ColdMath snapshot refresh calls",
    )
    runtime_cycle.add_argument(
        "--coldmath-positions-page-size",
        type=int,
        default=500,
        help="Page size used for Polymarket positions pagination",
    )
    runtime_cycle.add_argument(
        "--coldmath-positions-max-pages",
        type=int,
        default=20,
        help="Max positions pages pulled during ColdMath refresh",
    )
    runtime_cycle.add_argument(
        "--coldmath-disable-closed-positions-refresh",
        action="store_true",
        help="Skip /closed-positions API pulls during ColdMath snapshot refresh",
    )
    runtime_cycle.add_argument(
        "--coldmath-closed-positions-page-size",
        type=int,
        default=50,
        help="Page size used for paginated ColdMath closed-positions API pulls",
    )
    runtime_cycle.add_argument(
        "--coldmath-closed-positions-max-pages",
        type=int,
        default=20,
        help="Maximum closed-positions pages to fetch when ColdMath API refresh is enabled",
    )
    runtime_cycle.add_argument(
        "--coldmath-disable-trades-refresh",
        action="store_true",
        help="Skip /trades API pulls during ColdMath snapshot refresh",
    )
    runtime_cycle.add_argument(
        "--coldmath-disable-activity-refresh",
        action="store_true",
        help="Skip /activity API pulls during ColdMath snapshot refresh",
    )
    runtime_cycle.add_argument(
        "--coldmath-disable-taker-only-trades",
        action="store_true",
        help="Skip takerOnly=true trade query during ColdMath snapshot refresh",
    )
    runtime_cycle.add_argument(
        "--coldmath-disable-all-trade-roles",
        action="store_true",
        help="Skip takerOnly=false trade query during ColdMath snapshot refresh",
    )
    runtime_cycle.add_argument(
        "--coldmath-trades-page-size",
        type=int,
        default=500,
        help="Page size used for paginated ColdMath trades API pulls",
    )
    runtime_cycle.add_argument(
        "--coldmath-trades-max-pages",
        type=int,
        default=20,
        help="Maximum trades pages to fetch when ColdMath API refresh is enabled",
    )
    runtime_cycle.add_argument(
        "--coldmath-activity-page-size",
        type=int,
        default=500,
        help="Page size used for paginated ColdMath activity API pulls",
    )
    runtime_cycle.add_argument(
        "--coldmath-activity-max-pages",
        type=int,
        default=20,
        help="Maximum activity pages to fetch when ColdMath API refresh is enabled",
    )
    runtime_cycle.add_argument(
        "--coldmath-build-replication-plan",
        action="store_true",
        help="Build coldmath-replication-plan artifact before running runtime cycle",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-top-n",
        type=int,
        default=12,
        help="Candidate depth when building replication plan during runtime-cycle",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-market-tickers",
        default="",
        help="Optional comma-separated ticker override for runtime-cycle replication plan build",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-excluded-market-tickers",
        default="",
        help="Optional comma-separated ticker exclusions for runtime-cycle replication plan build",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-excluded-market-tickers-file",
        default="",
        help="Optional JSON/text exclusion file for runtime-cycle replication plan build",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-disable-liquidity-filter",
        action="store_true",
        help="Disable liquidity gating when building runtime-cycle replication plan",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-disable-require-two-sided-quotes",
        action="store_true",
        help="Allow one-sided quotes during runtime-cycle replication plan build",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-max-spread-dollars",
        type=float,
        default=0.18,
        help="Liquidity gate spread cap used by runtime-cycle replication plan build",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-min-liquidity-score",
        type=float,
        default=0.45,
        help="Liquidity gate minimum score used by runtime-cycle replication plan build",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-max-family-candidates",
        type=int,
        default=3,
        help="Per-family cap for runtime-cycle replication plan candidates",
    )
    runtime_cycle.add_argument(
        "--coldmath-replication-max-family-share",
        type=float,
        default=0.6,
        help="Max share of selected slots allocated to one family in runtime-cycle plan build",
    )

    alpha_scoreboard = subparsers.add_parser(
        "alpha-scoreboard",
        help="Score current projected edge versus a benchmark and output prioritized research targets",
    )
    alpha_scoreboard.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll used to compute deployed fraction and bankroll-level compounding",
    )
    alpha_scoreboard.add_argument(
        "--benchmark-annual-return",
        type=float,
        default=0.10,
        help="Annual benchmark return target as a decimal (0.10 = 10 percent)",
    )
    alpha_scoreboard.add_argument(
        "--plan-summary-file",
        default=None,
        help="Optional explicit kalshi_micro_prior_plan_summary JSON file path",
    )
    alpha_scoreboard.add_argument(
        "--daily-ops-file",
        default=None,
        help="Optional explicit daily_ops_report JSON file path",
    )
    alpha_scoreboard.add_argument(
        "--research-queue-csv",
        default=None,
        help="Optional explicit kalshi_nonsports_research_queue CSV path",
    )
    alpha_scoreboard.add_argument(
        "--top-research-targets",
        type=int,
        default=5,
        help="Maximum research targets embedded in the output summary",
    )
    alpha_scoreboard.add_argument("--output-dir", default="outputs", help="Output directory")

    ladder_grid = subparsers.add_parser(
        "ladder-grid",
        help="Sweep ladder-policy parameters and rank results",
    )
    ladder_grid.add_argument("--config", help="Optional JSON config path", default=None)
    ladder_grid.add_argument("--input", required=True, help="Input CSV path")
    ladder_grid.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    ladder_grid.add_argument(
        "--first-rung-offsets",
        default="5,10,20",
        help="Comma-separated first rung offsets in dollars",
    )
    ladder_grid.add_argument(
        "--rung-step-offsets",
        default="20,30",
        help="Comma-separated rung step offsets in dollars",
    )
    ladder_grid.add_argument(
        "--rung-count-values",
        default="3,4",
        help="Comma-separated rung counts",
    )
    ladder_grid.add_argument(
        "--min-success-probs",
        default="0.60,0.70,0.80",
        help="Comma-separated ladder minimum success probabilities",
    )
    ladder_grid.add_argument(
        "--planning-ps",
        default="0.52,0.55,0.58",
        help="Comma-separated ladder planning p values",
    )
    ladder_grid.add_argument(
        "--withdraw-steps",
        default="10",
        help="Comma-separated withdrawal step sizes",
    )
    ladder_grid.add_argument(
        "--min-risk-wallet-values",
        default="10",
        help="Comma-separated minimum risk wallet values",
    )
    ladder_grid.add_argument(
        "--drawdown-penalty",
        type=float,
        default=0.0,
        help="Score penalty multiplier for drawdown (higher penalizes volatility)",
    )
    ladder_grid.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of top scenarios returned in summary",
    )
    ladder_grid.add_argument(
        "--pareto-k",
        type=int,
        default=20,
        help="Maximum number of Pareto-front scenarios returned in summary",
    )
    ladder_grid.add_argument("--output-dir", default="outputs", help="Output directory")

    research_audit = subparsers.add_parser(
        "research-audit",
        help="Audit settlement/execution/compliance research completeness",
    )
    research_audit.add_argument(
        "--research-dir",
        default="data/research",
        help="Directory containing research matrix CSVs",
    )
    research_audit.add_argument(
        "--venues",
        default="kalshi,therundown",
        help="Comma-separated venues to audit",
    )
    research_audit.add_argument(
        "--jurisdictions",
        default="new_york",
        help="Comma-separated jurisdictions to audit",
    )
    research_audit.add_argument("--output-dir", default="outputs", help="Output directory")

    canonical_universe = subparsers.add_parser(
        "canonical-universe",
        help="Build canonical ticker contract-mapping and threshold libraries for macro + energy release research",
    )
    canonical_universe.add_argument(
        "--output-dir",
        default="data/research",
        help="Directory where canonical_contract_mapping.csv and canonical_threshold_library.csv are written",
    )

    odds_audit = subparsers.add_parser(
        "odds-audit",
        help="Audit historical odds data quality for backtest safety",
    )
    odds_audit.add_argument("--input", required=True, help="Input CSV path")
    odds_audit.add_argument(
        "--max-gap-minutes",
        type=float,
        default=60.0,
        help="Maximum acceptable quote gap per event/market/book group",
    )
    odds_audit.add_argument("--output-dir", default="outputs", help="Output directory")

    onboarding = subparsers.add_parser(
        "onboarding-check",
        help="Validate Kalshi + OpticOdds onboarding env prerequisites",
    )
    onboarding.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    onboarding.add_argument("--output-dir", default="outputs", help="Output directory")

    live_smoke = subparsers.add_parser(
        "live-smoke",
        help="Run authenticated Kalshi and odds-provider smoke tests",
    )
    live_smoke.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    live_smoke.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="HTTP timeout per request",
    )
    live_smoke.add_argument(
        "--skip-odds-provider-check",
        action="store_true",
        help="Skip TheRundown/odds-provider smoke and verify Kalshi only",
    )
    live_smoke.add_argument("--output-dir", default="outputs", help="Output directory")

    dns_doctor = subparsers.add_parser(
        "dns-doctor",
        help="Diagnose DNS readiness using system and public resolvers",
    )
    dns_doctor.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    dns_doctor.add_argument(
        "--hosts",
        default="",
        help="Optional comma-separated hostnames to test (defaults derive from env)",
    )
    dns_doctor.add_argument(
        "--timeout-seconds",
        type=float,
        default=1.5,
        help="DNS query timeout budget per host",
    )
    dns_doctor.add_argument("--output-dir", default="outputs", help="Output directory")

    live_snapshot = subparsers.add_parser(
        "live-snapshot",
        help="Capture a read-only snapshot from Kalshi and the odds provider",
    )
    live_snapshot.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    live_snapshot.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="HTTP timeout per request",
    )
    live_snapshot.add_argument(
        "--sports-preview-limit",
        type=int,
        default=5,
        help="Number of TheRundown sports records to include in the snapshot",
    )
    live_snapshot.add_argument("--output-dir", default="outputs", help="Output directory")

    live_candidates = subparsers.add_parser(
        "live-candidates",
        help="Fetch TheRundown odds and write a candidate CSV for the paper engine",
    )
    live_candidates.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    live_candidates.add_argument(
        "--sport-id",
        required=True,
        type=int,
        help="TheRundown sport ID, for example 4 for NBA",
    )
    live_candidates.add_argument(
        "--event-date",
        required=True,
        help="Event date in YYYY-MM-DD format as interpreted by TheRundown offset rule",
    )
    live_candidates.add_argument(
        "--affiliate-ids",
        default="19,22,23",
        help="Comma-separated sportsbook affiliate IDs, default DraftKings/BetMGM/FanDuel",
    )
    live_candidates.add_argument(
        "--market-ids",
        default="1,2,3",
        help="Comma-separated market IDs, default moneyline/spread/total",
    )
    live_candidates.add_argument(
        "--min-books",
        type=int,
        default=2,
        help="Minimum number of books required to form consensus fair probabilities",
    )
    live_candidates.add_argument(
        "--offset-minutes",
        type=int,
        default=300,
        help="TheRundown date offset in minutes, default 300 per docs",
    )
    live_candidates.add_argument(
        "--include-in-play",
        action="store_true",
        help="Keep in-play events instead of only pregame events",
    )
    live_candidates.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    live_candidates.add_argument("--output-dir", default="outputs", help="Output directory")

    live_paper = subparsers.add_parser(
        "live-paper",
        help="Fetch live candidates from TheRundown and run the paper engine in one step",
    )
    live_paper.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    live_paper.add_argument(
        "--config",
        default=None,
        help="Optional JSON config path for paper decisions",
    )
    live_paper.add_argument(
        "--sport-id",
        required=True,
        type=int,
        help="TheRundown sport ID, for example 4 for NBA",
    )
    live_paper.add_argument(
        "--event-date",
        required=True,
        help="Event date in YYYY-MM-DD format as interpreted by TheRundown offset rule",
    )
    live_paper.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    live_paper.add_argument(
        "--affiliate-ids",
        default="19,22,23",
        help="Comma-separated sportsbook affiliate IDs, default DraftKings/BetMGM/FanDuel",
    )
    live_paper.add_argument(
        "--market-ids",
        default="1,2,3",
        help="Comma-separated market IDs, default moneyline/spread/total",
    )
    live_paper.add_argument(
        "--min-books",
        type=int,
        default=2,
        help="Minimum number of books required to form consensus fair probabilities",
    )
    live_paper.add_argument(
        "--offset-minutes",
        type=int,
        default=300,
        help="TheRundown date offset in minutes, default 300 per docs",
    )
    live_paper.add_argument(
        "--include-in-play",
        action="store_true",
        help="Keep in-play events instead of only pregame events",
    )
    live_paper.add_argument(
        "--enrich-candidates",
        action="store_true",
        help="Apply optional sports evidence enrichment before paper decisions",
    )
    live_paper.add_argument(
        "--enrichment-csv",
        default=None,
        help="Optional CSV of external evidence used to adjust model_prob",
    )
    live_paper.add_argument(
        "--enrichment-freshness-hours",
        type=float,
        default=12.0,
        help="Maximum age for evidence rows before enrichment is skipped as stale",
    )
    live_paper.add_argument(
        "--enrichment-max-logit-shift",
        type=float,
        default=0.35,
        help="Maximum absolute logit shift applied to model_prob per candidate",
    )
    live_paper.add_argument(
        "--enrichment-include-non-moneyline",
        action="store_true",
        help="Allow enrichment on non-moneyline markets (default enriches moneyline only)",
    )
    live_paper.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    live_paper.add_argument("--output-dir", default="outputs", help="Output directory")

    sports_archive = subparsers.add_parser(
        "sports-archive",
        help="Run live sports paper flows across one or more dates and append a rolling archive",
    )
    sports_archive.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    sports_archive.add_argument(
        "--config",
        default=None,
        help="Optional JSON config path for paper decisions",
    )
    sports_archive.add_argument(
        "--sport-id",
        required=True,
        type=int,
        help="TheRundown sport ID, for example 4 for NBA",
    )
    sports_archive.add_argument(
        "--event-dates",
        required=True,
        help="Comma-separated event dates in YYYY-MM-DD format",
    )
    sports_archive.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    sports_archive.add_argument(
        "--archive-csv",
        default=None,
        help="Optional rolling archive CSV path, default outputs/live_paper_archive.csv",
    )
    sports_archive.add_argument(
        "--affiliate-ids",
        default="19,22,23",
        help="Comma-separated sportsbook affiliate IDs, default DraftKings/BetMGM/FanDuel",
    )
    sports_archive.add_argument(
        "--market-ids",
        default="1,2,3",
        help="Comma-separated market IDs, default moneyline/spread/total",
    )
    sports_archive.add_argument(
        "--min-books",
        type=int,
        default=2,
        help="Minimum number of books required to form consensus fair probabilities",
    )
    sports_archive.add_argument(
        "--offset-minutes",
        type=int,
        default=300,
        help="TheRundown date offset in minutes, default 300 per docs",
    )
    sports_archive.add_argument(
        "--include-in-play",
        action="store_true",
        help="Keep in-play events instead of only pregame events",
    )
    sports_archive.add_argument(
        "--enrich-candidates",
        action="store_true",
        help="Apply optional sports evidence enrichment before paper decisions",
    )
    sports_archive.add_argument(
        "--enrichment-csv",
        default=None,
        help="Optional CSV of external evidence used to adjust model_prob",
    )
    sports_archive.add_argument(
        "--enrichment-freshness-hours",
        type=float,
        default=12.0,
        help="Maximum age for evidence rows before enrichment is skipped as stale",
    )
    sports_archive.add_argument(
        "--enrichment-max-logit-shift",
        type=float,
        default=0.35,
        help="Maximum absolute logit shift applied to model_prob per candidate",
    )
    sports_archive.add_argument(
        "--enrichment-include-non-moneyline",
        action="store_true",
        help="Allow enrichment on non-moneyline markets (default enriches moneyline only)",
    )
    sports_archive.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    sports_archive.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_mlb_map = subparsers.add_parser(
        "kalshi-mlb-map",
        help="Map TheRundown MLB moneylines to Kalshi winner markets and score gross edge",
    )
    kalshi_mlb_map.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    kalshi_mlb_map.add_argument(
        "--event-date",
        required=True,
        help="Event date in YYYY-MM-DD format",
    )
    kalshi_mlb_map.add_argument(
        "--affiliate-ids",
        default="19,22,23",
        help="Comma-separated sportsbook affiliate IDs, default DraftKings/BetMGM/FanDuel",
    )
    kalshi_mlb_map.add_argument(
        "--min-books",
        type=int,
        default=2,
        help="Minimum number of books required to form consensus fair probabilities",
    )
    kalshi_mlb_map.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_mlb_map.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_scan = subparsers.add_parser(
        "kalshi-nonsports-scan",
        help="Rank near-term non-sports Kalshi markets for a small-risk execution workflow",
    )
    kalshi_nonsports_scan.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_nonsports_scan.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_nonsports_scan.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_nonsports_scan.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_nonsports_scan.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_nonsports_scan.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of ranked markets embedded in the JSON summary",
    )
    kalshi_nonsports_scan.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_nonsports_scan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_capture = subparsers.add_parser(
        "kalshi-nonsports-capture",
        help="Append the latest non-sports Kalshi board scan to a stable history CSV",
    )
    kalshi_nonsports_capture.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_nonsports_capture.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_nonsports_capture.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_nonsports_capture.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_nonsports_capture.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_nonsports_capture.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of ranked markets embedded in the scan summary",
    )
    kalshi_nonsports_capture.add_argument(
        "--history-csv",
        default=None,
        help="Optional stable history CSV path; defaults to outputs/kalshi_nonsports_history.csv",
    )
    kalshi_nonsports_capture.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_nonsports_capture.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_quality = subparsers.add_parser(
        "kalshi-nonsports-quality",
        help="Aggregate captured non-sports history into persistent board-quality scores",
    )
    kalshi_nonsports_quality.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_quality.add_argument(
        "--min-observations",
        type=int,
        default=3,
        help="Minimum observations for a market to qualify as meaningful",
    )
    kalshi_nonsports_quality.add_argument(
        "--min-mean-yes-bid",
        type=float,
        default=0.05,
        help="Minimum average Yes bid for a market to qualify as meaningful",
    )
    kalshi_nonsports_quality.add_argument(
        "--min-two-sided-ratio",
        type=float,
        default=0.5,
        help="Minimum two-sided observation ratio for a market to qualify as meaningful",
    )
    kalshi_nonsports_quality.add_argument(
        "--max-mean-spread",
        type=float,
        default=0.03,
        help="Maximum average spread for a market to qualify as meaningful",
    )
    kalshi_nonsports_quality.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-quality markets embedded in the summary",
    )
    kalshi_nonsports_quality.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_signals = subparsers.add_parser(
        "kalshi-nonsports-signals",
        help="Convert captured non-sports history into stability-backed trade signals",
    )
    kalshi_nonsports_signals.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_signals.add_argument(
        "--min-observations",
        type=int,
        default=3,
        help="Minimum observations for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--min-stable-ratio",
        type=float,
        default=0.5,
        help="Minimum stable-observation ratio for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--min-latest-yes-bid",
        type=float,
        default=0.05,
        help="Minimum latest Yes bid for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--min-mean-yes-bid",
        type=float,
        default=0.05,
        help="Minimum average Yes bid for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--max-mean-spread",
        type=float,
        default=0.03,
        help="Maximum average spread for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--max-yes-bid-stddev",
        type=float,
        default=0.03,
        help="Maximum Yes-bid standard deviation for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-signal markets embedded in the summary",
    )
    kalshi_nonsports_signals.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_persistence = subparsers.add_parser(
        "kalshi-nonsports-persistence",
        help="Measure whether non-sports markets stay tradeable across repeated snapshots",
    )
    kalshi_nonsports_persistence.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_persistence.add_argument(
        "--min-tradeable-yes-bid",
        type=float,
        default=0.05,
        help="Minimum latest Yes bid for a snapshot to count as tradeable",
    )
    kalshi_nonsports_persistence.add_argument(
        "--max-tradeable-spread",
        type=float,
        default=0.03,
        help="Maximum spread for a snapshot to count as tradeable",
    )
    kalshi_nonsports_persistence.add_argument(
        "--min-tradeable-snapshot-count",
        type=int,
        default=2,
        help="Minimum tradeable snapshots required for persistent-tradeable status",
    )
    kalshi_nonsports_persistence.add_argument(
        "--min-consecutive-tradeable-snapshots",
        type=int,
        default=2,
        help="Minimum consecutive tradeable snapshots required for persistent-tradeable status",
    )
    kalshi_nonsports_persistence.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-persistence markets embedded in the summary",
    )
    kalshi_nonsports_persistence.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_deltas = subparsers.add_parser(
        "kalshi-nonsports-deltas",
        help="Compare the latest two non-sports captures to detect board improvement or decay",
    )
    kalshi_nonsports_deltas.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_deltas.add_argument(
        "--min-tradeable-yes-bid",
        type=float,
        default=0.05,
        help="Minimum Yes bid for a snapshot to count as tradeable",
    )
    kalshi_nonsports_deltas.add_argument(
        "--max-tradeable-spread",
        type=float,
        default=0.03,
        help="Maximum spread for a snapshot to count as tradeable",
    )
    kalshi_nonsports_deltas.add_argument(
        "--min-bid-improvement",
        type=float,
        default=0.01,
        help="Minimum Yes-bid increase to mark a two-sided market as improved",
    )
    kalshi_nonsports_deltas.add_argument(
        "--min-spread-improvement",
        type=float,
        default=0.01,
        help="Minimum spread tightening to mark a two-sided market as improved",
    )
    kalshi_nonsports_deltas.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top delta markets embedded in the summary",
    )
    kalshi_nonsports_deltas.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_categories = subparsers.add_parser(
        "kalshi-nonsports-categories",
        help="Aggregate non-sports board health by category across captured history",
    )
    kalshi_nonsports_categories.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_categories.add_argument(
        "--min-tradeable-yes-bid",
        type=float,
        default=0.05,
        help="Minimum Yes bid for a category observation to count as tradeable",
    )
    kalshi_nonsports_categories.add_argument(
        "--max-tradeable-spread",
        type=float,
        default=0.03,
        help="Maximum spread for a category observation to count as tradeable",
    )
    kalshi_nonsports_categories.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top categories embedded in the summary",
    )
    kalshi_nonsports_categories.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_pressure = subparsers.add_parser(
        "kalshi-nonsports-pressure",
        help="Spot non-sports markets that are building pressure toward a more tradeable state",
    )
    kalshi_nonsports_pressure.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_pressure.add_argument(
        "--min-observations",
        type=int,
        default=3,
        help="Minimum observations required before a market can be labeled as build",
    )
    kalshi_nonsports_pressure.add_argument(
        "--min-latest-yes-bid",
        type=float,
        default=0.02,
        help="Minimum latest Yes bid for a market to be considered pressure-building",
    )
    kalshi_nonsports_pressure.add_argument(
        "--max-latest-spread",
        type=float,
        default=0.02,
        help="Maximum latest spread for a market to be considered pressure-building",
    )
    kalshi_nonsports_pressure.add_argument(
        "--min-two-sided-ratio",
        type=float,
        default=0.5,
        help="Minimum two-sided observation ratio required for pressure-build status",
    )
    kalshi_nonsports_pressure.add_argument(
        "--min-recent-bid-change",
        type=float,
        default=0.01,
        help="Minimum latest Yes-bid increase to count as recent pressure",
    )
    kalshi_nonsports_pressure.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-pressure markets embedded in the summary",
    )
    kalshi_nonsports_pressure.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_thresholds = subparsers.add_parser(
        "kalshi-nonsports-thresholds",
        help="Forecast which non-sports markets are approaching the live review thresholds",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--target-yes-bid",
        type=float,
        default=0.05,
        help="Target Yes bid used for tradeability forecasting",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--target-spread",
        type=float,
        default=0.02,
        help="Target spread used for tradeability forecasting",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--recent-window",
        type=int,
        default=5,
        help="Number of most recent observations to use for the trend forecast",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--max-hours-to-target",
        type=float,
        default=6.0,
        help="Maximum forecast hours to count as approaching the threshold",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--min-recent-two-sided-ratio",
        type=float,
        default=0.5,
        help="Minimum recent two-sided ratio required for threshold approach status",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--min-observations",
        type=int,
        default=3,
        help="Minimum observations required before threshold forecasting applies",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-threshold markets embedded in the summary",
    )
    kalshi_nonsports_thresholds.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_priors = subparsers.add_parser(
        "kalshi-nonsports-priors",
        help="Compare user-supplied non-sports fair-value priors against the latest captured board",
    )
    kalshi_nonsports_priors.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_nonsports_priors.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_priors.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top prior-backed markets embedded in the summary",
    )
    kalshi_nonsports_priors.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts used for fee-aware per-contract edge estimates",
    )
    kalshi_nonsports_priors.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_research_queue = subparsers.add_parser(
        "kalshi-nonsports-research-queue",
        help="Rank uncovered non-sports markets that look worth researching next",
    )
    kalshi_nonsports_research_queue.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_nonsports_research_queue.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_research_queue.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top research candidates embedded in the summary",
    )
    kalshi_nonsports_research_queue.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_auto_priors = subparsers.add_parser(
        "kalshi-nonsports-auto-priors",
        help="Generate thesis-backed auto priors for uncovered non-sports markets using external news evidence",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of fair probabilities that will be upserted with auto-generated rows",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--max-markets",
        type=int,
        default=15,
        help="Maximum uncovered markets to evaluate per run",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--max-headlines-per-market",
        type=int,
        default=8,
        help="Maximum evidence headlines to score per market",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--min-evidence-count",
        type=int,
        default=2,
        help="Minimum evidence items required before writing an auto prior",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--min-evidence-quality",
        type=float,
        default=0.55,
        help="Minimum average source-quality score required before writing an auto prior",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--min-high-trust-sources",
        type=int,
        default=1,
        help="Minimum number of high-trust evidence sources required before writing an auto prior",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--disable-protect-manual",
        action="store_true",
        help="Allow auto rows to overwrite manual rows (disabled by default for safety)",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate auto priors without writing back into the priors CSV",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical mapping CSV used for optional mapped-ticker scoping",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--restrict-to-mapped-live-tickers",
        action="store_true",
        help="Only generate auto priors for live market tickers currently mapped in canonical_contract_mapping.csv",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--allowed-canonical-niches",
        default="",
        help="Optional comma-separated canonical niches (for example: macro_release,weather_energy_transmission,weather_climate)",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--allowed-categories",
        default="",
        help="Optional comma-separated category allow-list from history.csv",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--disallowed-categories",
        default="Sports",
        help="Optional comma-separated category block-list from history.csv",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per evidence request",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top auto priors embedded in the summary",
    )
    kalshi_nonsports_auto_priors.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_weather_catalog = subparsers.add_parser(
        "kalshi-weather-catalog",
        help="Build a weather-market catalog with settlement-spec metadata from captured non-sports history",
    )
    kalshi_weather_catalog.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_weather_catalog.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of top weather markets embedded in the summary",
    )
    kalshi_weather_catalog.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_weather_priors = subparsers.add_parser(
        "kalshi-weather-priors",
        help="Generate weather-contract priors (daily rain, daily temperature, monthly anomaly) from weather-specific data sources",
    )
    kalshi_weather_priors.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV where generated weather priors are upserted",
    )
    kalshi_weather_priors.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_weather_priors.add_argument(
        "--allowed-contract-families",
        default="daily_rain,daily_temperature",
        help="Comma-separated weather contract families to process",
    )
    kalshi_weather_priors.add_argument(
        "--max-markets",
        type=int,
        default=30,
        help="Maximum weather markets to process per run",
    )
    kalshi_weather_priors.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout per weather data request",
    )
    kalshi_weather_priors.add_argument(
        "--historical-lookback-years",
        type=int,
        default=15,
        help="Years of station-level historical day-of-year samples to use when NOAA CDO token is available",
    )
    kalshi_weather_priors.add_argument(
        "--station-history-cache-max-age-hours",
        type=float,
        default=24.0,
        help="Max age for cached station-history snapshots before forcing a fresh CDO pull",
    )
    kalshi_weather_priors.add_argument(
        "--disable-nws-gridpoint-data",
        action="store_true",
        help="Disable NWS forecastGridData enrichment for rain/temperature priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-nws-observations",
        action="store_true",
        help="Disable NWS station observations enrichment for rain/temperature priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-nws-alerts",
        action="store_true",
        help="Disable NWS active alerts enrichment for rain/temperature priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-ncei-normals",
        action="store_true",
        help="Disable NCEI daily normals enrichment for station/day priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-mrms-qpe",
        action="store_true",
        help="Disable NOAA MRMS QPE metadata enrichment for rain priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-nbm-snapshot",
        action="store_true",
        help="Disable NOAA NBM snapshot metadata enrichment for weather priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-protect-manual",
        action="store_true",
        help="Allow generated weather priors to overwrite manual rows (disabled by default for safety)",
    )
    kalshi_weather_priors.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate weather priors without writing back to the priors CSV",
    )
    kalshi_weather_priors.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top weather priors embedded in the summary",
    )
    kalshi_weather_priors.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_weather_prewarm = subparsers.add_parser(
        "kalshi-weather-prewarm",
        help="Prewarm NOAA CDO station/day climatology cache for daily weather markets",
    )
    kalshi_weather_prewarm.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_weather_prewarm.add_argument(
        "--historical-lookback-years",
        type=int,
        default=15,
        help="Years of station-level historical day-of-year samples to cache",
    )
    kalshi_weather_prewarm.add_argument(
        "--station-history-cache-max-age-hours",
        type=float,
        default=24.0,
        help="Max age for cached station-history snapshots before forcing refresh",
    )
    kalshi_weather_prewarm.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout per station/day prewarm fetch",
    )
    kalshi_weather_prewarm.add_argument(
        "--max-station-day-keys",
        type=int,
        default=500,
        help="Maximum unique station/day keys to prewarm per run",
    )
    kalshi_weather_prewarm.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_contract_specs = subparsers.add_parser(
        "kalshi-temperature-contract-specs",
        help="Build a canonical contract-spec snapshot for Kalshi temperature markets from live event/market metadata",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--env-file",
        default=".env",
        help="Env-style file used to resolve Kalshi environment and credentials",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per Kalshi events request",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Kalshi events page size",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--max-pages",
        type=int,
        default=40,
        help="Maximum events pages to scan per run",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of top contract specs embedded in the summary",
    )
    kalshi_temperature_contract_specs.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_constraint_scan = subparsers.add_parser(
        "kalshi-temperature-constraint-scan",
        help="Scan Kalshi temperature contract specs against intraday station observations for hard constraint opportunities",
    )
    kalshi_temperature_constraint_scan.add_argument(
        "--specs-csv",
        default=None,
        help="Optional explicit kalshi_temperature_contract_specs CSV path (latest in output-dir used when omitted)",
    )
    kalshi_temperature_constraint_scan.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout per station observations request",
    )
    kalshi_temperature_constraint_scan.add_argument(
        "--max-markets",
        type=int,
        default=100,
        help="Maximum markets to evaluate per run",
    )
    kalshi_temperature_constraint_scan.add_argument(
        "--speci-calibration-json",
        default=None,
        help=(
            "Optional JSON file with calibrated SPECI shock thresholds/weights. "
            "Used to tune shock activation, cooldown, and persistence gates."
        ),
    )
    kalshi_temperature_constraint_scan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_settlement_state = subparsers.add_parser(
        "kalshi-temperature-settlement-state",
        help="Build settlement-finalization state keyed by temperature underlying (series|station|date)",
    )
    kalshi_temperature_settlement_state.add_argument(
        "--specs-csv",
        default=None,
        help="Optional explicit kalshi_temperature_contract_specs CSV path (latest in output-dir used when omitted)",
    )
    kalshi_temperature_settlement_state.add_argument(
        "--constraint-csv",
        default=None,
        help="Optional explicit kalshi_temperature_constraint_scan CSV path (latest in output-dir used when omitted)",
    )
    kalshi_temperature_settlement_state.add_argument(
        "--top-n",
        type=int,
        default=25,
        help="Number of underlyings embedded in top_underlyings summary",
    )
    kalshi_temperature_settlement_state.add_argument(
        "--final-report-cache-ttl-minutes",
        type=float,
        default=30.0,
        help="Cache TTL for station/day final report lookups",
    )
    kalshi_temperature_settlement_state.add_argument(
        "--final-report-timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout per final report lookup",
    )
    kalshi_temperature_settlement_state.add_argument(
        "--disable-final-report-lookup",
        action="store_true",
        help="Disable authoritative station/day final report lookup enrichment",
    )
    kalshi_temperature_settlement_state.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_coverage_velocity_report = subparsers.add_parser(
        "kalshi-temperature-coverage-velocity-report",
        aliases=("temperature-coverage-velocity-report",),
        help="Build or summarize coverage-velocity trend artifacts for recovery hardening",
    )
    kalshi_temperature_coverage_velocity_report.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory",
    )
    kalshi_temperature_coverage_velocity_report.add_argument(
        "--history-limit",
        type=int,
        default=24,
        help="Maximum decision-matrix hardening history files to include in the trend report",
    )
    kalshi_temperature_coverage_velocity_report.add_argument(
        "--summarize-only",
        action="store_true",
        help="Summarize latest coverage-velocity artifacts without requesting a fresh rebuild",
    )

    kalshi_temperature_settled_outcome_throughput = subparsers.add_parser(
        "kalshi-temperature-settled-outcome-throughput",
        aliases=("temperature-settled-outcome-throughput",),
        help="Build or summarize settled-outcome throughput coverage artifacts for recovery hardening",
    )
    kalshi_temperature_settled_outcome_throughput.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory",
    )
    kalshi_temperature_settled_outcome_throughput.add_argument(
        "--summarize-only",
        action="store_true",
        help="Summarize latest throughput artifacts without requesting a fresh rebuild",
    )

    kalshi_temperature_metar_ingest = subparsers.add_parser(
        "kalshi-temperature-metar-ingest",
        help="Ingest AviationWeather METAR cache files and update per-station local-day maxima for Kalshi temperature workflows",
    )
    kalshi_temperature_metar_ingest.add_argument(
        "--specs-csv",
        default=None,
        help="Optional explicit kalshi_temperature_contract_specs CSV path used for station-timezone mapping",
    )
    kalshi_temperature_metar_ingest.add_argument(
        "--cache-url",
        default="https://aviationweather.gov/data/cache/metars.cache.csv.gz",
        help="METAR cache CSV GZ URL",
    )
    kalshi_temperature_metar_ingest.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout per METAR cache request",
    )
    kalshi_temperature_metar_ingest.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_weather_pattern = subparsers.add_parser(
        "kalshi-temperature-weather-pattern",
        aliases=["temperature-weather-pattern"],
        help="Build weather-pattern diagnostics for temperature pipeline readiness",
    )
    kalshi_temperature_weather_pattern.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory",
    )
    kalshi_temperature_weather_pattern.add_argument(
        "--window-hours",
        type=float,
        default=24.0,
        help="Rolling lookback window in hours",
    )
    kalshi_temperature_weather_pattern.add_argument(
        "--min-samples",
        "--min-bucket-samples",
        dest="min_bucket_samples",
        type=int,
        default=25,
        help="Minimum sample count required for pattern summaries",
    )
    kalshi_temperature_weather_pattern.add_argument(
        "--max-age-hours",
        "--max-profile-age-hours",
        dest="max_profile_age_hours",
        type=float,
        default=72.0,
        help="Maximum allowed age in hours for source weather-pattern inputs",
    )

    kalshi_temperature_trader = subparsers.add_parser(
        "kalshi-temperature-trader",
        help="Deterministic temperature intent -> policy gate -> execution bridge on top of kalshi-micro-execute",
    )
    kalshi_temperature_trader.add_argument(
        "--env-file",
        default=".env",
        help="Env-style file used to resolve Kalshi environment and credentials",
    )
    kalshi_temperature_trader.add_argument(
        "--specs-csv",
        default=None,
        help="Optional explicit contract-spec CSV; uses constraint source or latest snapshot when omitted",
    )
    kalshi_temperature_trader.add_argument(
        "--constraint-csv",
        default=None,
        help="Optional explicit constraint-scan CSV; runs fresh scan when omitted",
    )
    kalshi_temperature_trader.add_argument(
        "--metar-summary-json",
        default=None,
        help="Optional explicit METAR summary JSON; latest summary in output-dir used when omitted",
    )
    kalshi_temperature_trader.add_argument(
        "--metar-state-json",
        default=None,
        help="Optional explicit METAR state JSON; inferred from summary or output-dir when omitted",
    )
    kalshi_temperature_trader.add_argument(
        "--min-metar-ingest-quality-score",
        type=float,
        default=None,
        help="Optional minimum METAR ingest quality score required for approvals",
    )
    kalshi_temperature_trader.add_argument(
        "--min-metar-fresh-station-coverage-ratio",
        type=float,
        default=None,
        help="Optional minimum fresh-station coverage ratio required from METAR ingest",
    )
    kalshi_temperature_trader.add_argument(
        "--require-metar-ingest-status-ready",
        action="store_true",
        help="Require METAR ingest summary status=ready before approvals",
    )
    kalshi_temperature_trader.add_argument(
        "--high-price-edge-guard-enabled",
        action="store_true",
        help="Enable high-entry-price fail-safe gate even when broader edge thresholds are disabled",
    )
    kalshi_temperature_trader.add_argument(
        "--high-price-edge-guard-min-entry-price-dollars",
        type=float,
        default=0.85,
        help="Entry-price trigger where high-price fail-safe checks activate",
    )
    kalshi_temperature_trader.add_argument(
        "--high-price-edge-guard-min-expected-edge-net",
        type=float,
        default=0.0,
        help="Minimum expected edge required once high-price fail-safe checks activate",
    )
    kalshi_temperature_trader.add_argument(
        "--high-price-edge-guard-min-edge-to-risk-ratio",
        type=float,
        default=0.02,
        help="Minimum edge-to-risk ratio required once high-price fail-safe checks activate",
    )
    kalshi_temperature_trader.add_argument(
        "--ws-state-json",
        default=None,
        help="Optional explicit websocket state JSON; defaults to kalshi_ws_state_latest.json in output-dir",
    )
    kalshi_temperature_trader.add_argument(
        "--alpha-consensus-json",
        default=None,
        help=(
            "Optional fused breadth-worker consensus JSON. "
            "Defaults to output-dir/breadth_worker/breadth_worker_consensus_latest.json when present."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--settlement-state-json",
        default=None,
        help="Optional settlement-finalization state JSON keyed by underlying (series|station|date)",
    )
    kalshi_temperature_trader.add_argument(
        "--book-db-path",
        default=None,
        help="Optional explicit portfolio book SQLite path for underlying-family netting",
    )
    kalshi_temperature_trader.add_argument(
        "--policy-version",
        default="temperature_policy_v1",
        help="Policy version tag attached to intents",
    )
    kalshi_temperature_trader.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per order for approved intents",
    )
    kalshi_temperature_trader.add_argument(
        "--max-orders",
        type=int,
        default=8,
        help="Maximum approved intents to convert into executable plans",
    )
    kalshi_temperature_trader.add_argument(
        "--max-markets",
        type=int,
        default=100,
        help="Maximum markets evaluated when running an implicit constraint scan",
    )
    kalshi_temperature_trader.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout used by scan and execution calls",
    )
    kalshi_temperature_trader.add_argument(
        "--yes-max-entry-price",
        type=float,
        default=0.95,
        help="Maximum entry price for YES-side intents",
    )
    kalshi_temperature_trader.add_argument(
        "--no-max-entry-price",
        type=float,
        default=0.95,
        help="Maximum entry price for NO-side intents",
    )
    kalshi_temperature_trader.add_argument(
        "--min-settlement-confidence",
        type=float,
        default=0.6,
        help="Minimum settlement confidence score required for intent approval",
    )
    kalshi_temperature_trader.add_argument(
        "--max-metar-age-minutes",
        type=float,
        default=20.0,
        help="Maximum allowed METAR observation age in minutes",
    )
    kalshi_temperature_trader.add_argument(
        "--metar-age-policy-json",
        default=None,
        help=(
            "Optional JSON file with station/hour-specific METAR age overrides. "
            "Schema keys: station_max_age_minutes and station_local_hour_max_age_minutes."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--speci-calibration-json",
        default=None,
        help=(
            "Optional JSON file with calibrated SPECI shock thresholds/weights forwarded to implicit constraint scans."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--min-alpha-strength",
        type=float,
        default=0.0,
        help="Minimum alpha-strength score required for intent approval",
    )
    kalshi_temperature_trader.add_argument(
        "--min-probability-confidence",
        type=float,
        default=None,
        help="Optional minimum modeled probability confidence required for intent approval",
    )
    kalshi_temperature_trader.add_argument(
        "--min-expected-edge-net",
        type=float,
        default=None,
        help="Optional minimum modeled expected edge (net) required for intent approval",
    )
    kalshi_temperature_trader.add_argument(
        "--min-edge-to-risk-ratio",
        type=float,
        default=None,
        help=(
            "Optional minimum base edge-to-risk ratio required for intent approval. "
            "Computed as (probability - entry_price) / entry_price."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--min-base-edge-net",
        type=float,
        default=0.0,
        help="Minimum base edge (probability - entry_price) required before additive bonuses",
    )
    kalshi_temperature_trader.add_argument(
        "--min-probability-breakeven-gap",
        type=float,
        default=0.0,
        help="Minimum probability minus breakeven-price gap required for approval",
    )
    kalshi_temperature_trader.add_argument(
        "--disable-enforce-probability-edge-thresholds",
        action="store_true",
        help=(
            "Disable probability/edge threshold enforcement when explicit thresholds are omitted. "
            "By default, omitted thresholds are backfilled by configured fallback values."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--enforce-entry-price-probability-floor",
        action="store_true",
        help=(
            "When fallback probability thresholds are active, raise minimum probability based on entry price "
            "to avoid high-price low-conviction approvals."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--fallback-min-probability-confidence",
        type=float,
        default=None,
        help=(
            "Fallback minimum probability confidence when --min-probability-confidence is omitted "
            "(defaults to min-settlement-confidence if unset)."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--fallback-min-expected-edge-net",
        type=float,
        default=0.005,
        help=(
            "Fallback minimum expected edge (net) when --min-expected-edge-net is omitted "
            "(default: 0.005)."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--fallback-min-edge-to-risk-ratio",
        type=float,
        default=0.02,
        help=(
            "Fallback minimum base edge-to-risk ratio when --min-edge-to-risk-ratio is omitted "
            "(default: 0.02)."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--disable-interval-consistency-gate",
        action="store_true",
        help="Disable interval-consistency checks (yes/no feasibility overlap + gap checks)",
    )
    kalshi_temperature_trader.add_argument(
        "--max-yes-possible-gap-for-yes-side",
        type=float,
        default=0.0,
        help="Maximum allowed yes_possible_gap for YES-side intents",
    )
    kalshi_temperature_trader.add_argument(
        "--min-hours-to-close",
        type=float,
        default=0.0,
        help="Minimum hours-to-close required for approval (cutoff window)",
    )
    kalshi_temperature_trader.add_argument(
        "--max-hours-to-close",
        type=float,
        default=48.0,
        help="Maximum hours-to-close for active horizon filtering",
    )
    kalshi_temperature_trader.add_argument(
        "--max-intents-per-underlying",
        type=int,
        default=6,
        help="Maximum approved intents per underlying key (series|station|date)",
    )
    kalshi_temperature_trader.add_argument(
        "--taf-stale-grace-minutes",
        type=float,
        default=0.0,
        help="Optional METAR-age grace when forecast/TAF path modeling is ready",
    )
    kalshi_temperature_trader.add_argument(
        "--taf-stale-grace-max-volatility-score",
        type=float,
        default=1.0,
        help="Maximum TAF volatility score allowed for stale-grace activation",
    )
    kalshi_temperature_trader.add_argument(
        "--taf-stale-grace-max-range-width",
        type=float,
        default=10.0,
        help="Maximum forecast range width allowed for stale-grace activation",
    )
    kalshi_temperature_trader.add_argument(
        "--metar-freshness-quality-boundary-ratio",
        type=float,
        default=0.92,
        help=(
            "Age-ratio boundary (age/max_age) where near-stale quality tightening activates. "
            "Set <=0 to disable."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--metar-freshness-quality-probability-margin",
        type=float,
        default=0.03,
        help="Additional probability-confidence margin required near stale boundary",
    )
    kalshi_temperature_trader.add_argument(
        "--metar-freshness-quality-expected-edge-margin",
        type=float,
        default=0.005,
        help="Additional expected-edge margin required near stale boundary",
    )
    kalshi_temperature_trader.add_argument(
        "--disable-require-market-snapshot-seq",
        action="store_true",
        help="Allow intents when market snapshot sequence is missing from ws-state",
    )
    kalshi_temperature_trader.add_argument(
        "--require-metar-snapshot-sha",
        action="store_true",
        help="Require METAR raw snapshot SHA in intent evidence",
    )
    kalshi_temperature_trader.add_argument(
        "--disable-underlying-netting",
        action="store_true",
        help="Disable family-level netting against existing book positions/open orders",
    )
    kalshi_temperature_trader.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Allow live order writes (still subject to existing micro-execute safety gates)",
    )
    kalshi_temperature_trader.add_argument(
        "--micro-live-50",
        action="store_true",
        help=(
            "Apply strict $50 micro-live caps (bankroll=50, daily risk<=3, "
            "max deployed<=20%%, live cost/day<=3, max submissions/day<=3, "
            "max orders/loop<=3, contracts/order=1, max entry<=0.85)."
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--intents-only",
        action="store_true",
        help="Build intents and bridge plans only; skip micro-execution",
    )
    kalshi_temperature_trader.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Planning bankroll forwarded to micro-execute",
    )
    kalshi_temperature_trader.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Daily risk cap forwarded to micro-execute",
    )
    kalshi_temperature_trader.add_argument(
        "--max-total-deployed-pct",
        type=float,
        default=0.35,
        help="Maximum fraction of reference bankroll deployable per loop before plan truncation",
    )
    kalshi_temperature_trader.add_argument(
        "--max-same-station-exposure-pct",
        type=float,
        default=0.6,
        help="Maximum per-station share of deployed loop budget during portfolio selection",
    )
    kalshi_temperature_trader.add_argument(
        "--max-same-hour-cluster-exposure-pct",
        type=float,
        default=0.6,
        help="Maximum per-local-hour share of deployed loop budget during portfolio selection",
    )
    kalshi_temperature_trader.add_argument(
        "--max-same-underlying-exposure-pct",
        type=float,
        default=0.5,
        help="Maximum per-underlying share of deployed loop budget during portfolio selection",
    )
    kalshi_temperature_trader.add_argument(
        "--max-orders-per-station",
        type=int,
        default=2,
        help="Maximum planned orders per station per loop (count cap)",
    )
    kalshi_temperature_trader.add_argument(
        "--max-orders-per-underlying",
        type=int,
        default=2,
        help="Maximum planned orders per underlying per loop (count cap)",
    )
    kalshi_temperature_trader.add_argument(
        "--min-unique-stations-per-loop",
        type=int,
        default=3,
        help="Breadth quota: minimum distinct stations targeted per loop",
    )
    kalshi_temperature_trader.add_argument(
        "--min-unique-underlyings-per-loop",
        type=int,
        default=4,
        help="Breadth quota: minimum distinct underlying families targeted per loop",
    )
    kalshi_temperature_trader.add_argument(
        "--min-unique-local-hours-per-loop",
        type=int,
        default=2,
        help="Breadth quota: minimum distinct local-hour buckets targeted per loop",
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-cooldown-minutes",
        type=float,
        default=20.0,
        help="Cooldown window that blocks re-planning the same market-side unless a material signal override is present",
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-price-change-override-dollars",
        type=float,
        default=0.02,
        help="Allow cooldown override when maker entry price changes by at least this amount",
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-alpha-change-override",
        type=float,
        default=0.2,
        help="Allow cooldown override when alpha-strength improves by at least this amount",
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-confidence-change-override",
        type=float,
        default=0.03,
        help="Allow cooldown override when settlement confidence improves by at least this amount",
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-min-observation-advance-minutes",
        type=float,
        default=2.0,
        help="Allow cooldown override when METAR observation timestamp advances by at least this many minutes",
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-repeat-window-minutes",
        type=float,
        default=1440.0,
        help=(
            "Rolling window used to cap repeated planning of the same market-side "
            "(independent of cooldown elapsed minutes)"
        ),
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-max-plans-per-window",
        type=int,
        default=8,
        help="Maximum recent plans allowed per market-side within repeat window (0 disables cap)",
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-history-files",
        type=int,
        default=180,
        help="Maximum recent plan CSV files scanned for market-side cooldown enforcement",
    )
    kalshi_temperature_trader.add_argument(
        "--replan-market-side-min-orders-backstop",
        type=int,
        default=1,
        help="Minimum plans kept after cooldown filtering (0 disables throughput backstop)",
    )
    kalshi_temperature_trader.add_argument(
        "--disable-historical-selection-quality",
        action="store_true",
        help="Disable settled-history quality calibration overlays during policy gating and ranking",
    )
    kalshi_temperature_trader.add_argument(
        "--historical-selection-quality-lookback-hours",
        type=float,
        default=336.0,
        help="Lookback window (hours) for settled-history selection quality profile",
    )
    kalshi_temperature_trader.add_argument(
        "--historical-selection-quality-min-resolved-market-sides",
        type=int,
        default=12,
        help="Minimum resolved unique market-side outcomes required before profile is considered fully ready",
    )
    kalshi_temperature_trader.add_argument(
        "--historical-selection-quality-min-bucket-samples",
        type=int,
        default=4,
        help="Minimum settled samples required per station/hour/signal bucket in quality profile",
    )
    kalshi_temperature_trader.add_argument(
        "--historical-selection-quality-probability-penalty-max",
        type=float,
        default=0.05,
        help="Maximum additional probability-confidence requirement from historical quality penalties",
    )
    kalshi_temperature_trader.add_argument(
        "--historical-selection-quality-expected-edge-penalty-max",
        type=float,
        default=0.006,
        help="Maximum additional expected-edge requirement from historical quality penalties",
    )
    kalshi_temperature_trader.add_argument(
        "--historical-selection-quality-score-adjust-scale",
        type=float,
        default=0.35,
        help="Score adjustment scale applied by historical quality calibration in portfolio ranking",
    )
    kalshi_temperature_trader.add_argument(
        "--historical-selection-quality-profile-max-age-hours",
        type=float,
        default=96.0,
        help="Maximum allowed age of bankroll-validation artifact used for historical quality profile",
    )
    kalshi_temperature_trader.add_argument(
        "--historical-selection-quality-preferred-model",
        default="fixed_fraction_per_underlying_family",
        help="Preferred bankroll-validation attribution model used for historical quality bucket penalties/boosts",
    )
    kalshi_temperature_trader.add_argument(
        "--optimizer-profile-json",
        "--optimizer-profile-json-path",
        dest="optimizer_profile_json",
        default=None,
        help="Optional optimizer profile JSON path used to seed historical selection-quality settings",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-profile-json",
        dest="weather_pattern_profile_json",
        default=None,
        help="Optional weather-pattern profile JSON path used to seed temperature trader weather-pattern settings",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-hardening-enabled",
        dest="weather_pattern_hardening_enabled",
        action="store_true",
        default=None,
        help="Enable weather-pattern hardening controls when supported by the trader runner",
    )
    kalshi_temperature_trader.add_argument(
        "--no-weather-pattern-hardening-enabled",
        "--disable-weather-pattern-hardening",
        dest="weather_pattern_hardening_enabled",
        action="store_false",
        help="Disable weather-pattern hardening controls when supported by the trader runner",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-risk-off-enabled",
        dest="weather_pattern_risk_off_enabled",
        action="store_true",
        default=None,
        help="Enable weather-pattern risk-off controls when supported by the trader runner",
    )
    kalshi_temperature_trader.add_argument(
        "--no-weather-pattern-risk-off-enabled",
        dest="weather_pattern_risk_off_enabled",
        action="store_false",
        help="Disable weather-pattern risk-off controls when supported by the trader runner",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-risk-off-concentration-threshold",
        type=float,
        default=None,
        help="Optional concentration threshold used by weather-pattern risk-off controls",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-risk-off-min-attempts",
        type=int,
        default=None,
        help="Optional minimum attempts threshold used by weather-pattern risk-off controls",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-risk-off-stale-metar-share-threshold",
        type=float,
        default=None,
        help="Optional stale-METAR share threshold used by weather-pattern risk-off controls",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-negative-bucket-suppression-enabled",
        dest="weather_pattern_negative_bucket_suppression_enabled",
        action="store_true",
        default=None,
        help="Enable weather-pattern negative bucket suppression controls when supported by the trader runner",
    )
    kalshi_temperature_trader.add_argument(
        "--no-weather-pattern-negative-bucket-suppression-enabled",
        dest="weather_pattern_negative_bucket_suppression_enabled",
        action="store_false",
        help="Disable weather-pattern negative bucket suppression controls when supported by the trader runner",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-negative-bucket-suppression-top-n",
        type=int,
        default=None,
        help="Optional cap for strongest negative weather-pattern buckets used for suppression",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-negative-bucket-suppression-min-samples",
        type=int,
        default=None,
        help="Optional minimum sample count for weather-pattern negative bucket suppression",
    )
    kalshi_temperature_trader.add_argument(
        "--weather-pattern-negative-bucket-suppression-negative-expectancy-threshold",
        type=float,
        default=None,
        help="Optional negative expectancy threshold used by weather-pattern negative bucket suppression",
    )
    kalshi_temperature_trader.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Forwarded to micro-execute cancel behavior",
    )
    kalshi_temperature_trader.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Forwarded to micro-execute resting hold duration",
    )
    kalshi_temperature_trader.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="Forwarded live submission cap",
    )
    kalshi_temperature_trader.add_argument(
        "--max-live-cost-per-day-dollars",
        type=float,
        default=3.0,
        help="Forwarded live cost cap",
    )
    kalshi_temperature_trader.add_argument(
        "--enforce-trade-gate",
        action="store_true",
        help="Enable micro-execute trade gate checks",
    )
    kalshi_temperature_trader.add_argument(
        "--enforce-ws-state-authority",
        action="store_true",
        help="Enable micro-execute websocket state authority gate",
    )
    kalshi_temperature_trader.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum websocket state staleness before gate blocks",
    )
    kalshi_temperature_trader.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_shadow_watch = subparsers.add_parser(
        "kalshi-temperature-shadow-watch",
        help="Run supervised shadow loops for kalshi-temperature-trader (live data, gated execution path, live writes optional)",
    )
    kalshi_temperature_shadow_watch.add_argument("--env-file", default=".env", help="Env-style credentials file")
    kalshi_temperature_shadow_watch.add_argument("--specs-csv", default=None, help="Optional explicit specs CSV")
    kalshi_temperature_shadow_watch.add_argument("--constraint-csv", default=None, help="Optional explicit constraint CSV")
    kalshi_temperature_shadow_watch.add_argument(
        "--metar-summary-json",
        default=None,
        help="Optional explicit METAR summary JSON",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--metar-state-json",
        default=None,
        help="Optional explicit METAR state JSON",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-metar-ingest-quality-score",
        type=float,
        default=None,
        help="Optional minimum METAR ingest quality score required for approvals",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-metar-fresh-station-coverage-ratio",
        type=float,
        default=None,
        help="Optional minimum fresh-station coverage ratio required from METAR ingest",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--require-metar-ingest-status-ready",
        action="store_true",
        help="Require METAR ingest summary status=ready before approvals",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--high-price-edge-guard-enabled",
        action="store_true",
        help="Enable high-entry-price fail-safe gate even when broader edge thresholds are disabled",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--high-price-edge-guard-min-entry-price-dollars",
        type=float,
        default=0.85,
        help="Entry-price trigger where high-price fail-safe checks activate",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--high-price-edge-guard-min-expected-edge-net",
        type=float,
        default=0.0,
        help="Minimum expected edge required once high-price fail-safe checks activate",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--high-price-edge-guard-min-edge-to-risk-ratio",
        type=float,
        default=0.02,
        help="Minimum edge-to-risk ratio required once high-price fail-safe checks activate",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--ws-state-json",
        default=None,
        help="Optional explicit websocket state JSON",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--alpha-consensus-json",
        default=None,
        help=(
            "Optional fused breadth-worker consensus JSON. "
            "Defaults to output-dir/breadth_worker/breadth_worker_consensus_latest.json when present."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--settlement-state-json",
        default=None,
        help="Optional settlement-finalization state JSON keyed by underlying",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--book-db-path",
        default=None,
        help="Optional explicit portfolio book SQLite path for family-level netting",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--loops",
        type=int,
        default=1,
        help="Number of shadow loops; set 0 to run continuously",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--sleep-between-loops-seconds",
        type=float,
        default=60.0,
        help="Sleep between loops",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--policy-version",
        default="temperature_policy_v1",
        help="Policy version tag attached to intents",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per order for approved intents",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-orders",
        type=int,
        default=8,
        help="Maximum approved intents converted to executable plans per loop",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-markets",
        type=int,
        default=100,
        help="Maximum markets evaluated per loop when constraint scan is implicit",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout for scan + execution operations",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--yes-max-entry-price",
        type=float,
        default=0.95,
        help="Maximum entry price for YES-side intents",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--no-max-entry-price",
        type=float,
        default=0.95,
        help="Maximum entry price for NO-side intents",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-settlement-confidence",
        type=float,
        default=0.6,
        help="Minimum settlement confidence score required for approval",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-metar-age-minutes",
        type=float,
        default=20.0,
        help="Maximum METAR observation age in minutes",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--metar-age-policy-json",
        default=None,
        help=(
            "Optional JSON file with station/hour-specific METAR age overrides. "
            "Schema keys: station_max_age_minutes and station_local_hour_max_age_minutes."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--speci-calibration-json",
        default=None,
        help=(
            "Optional JSON file with calibrated SPECI shock thresholds/weights forwarded to implicit constraint scans."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-alpha-strength",
        type=float,
        default=0.0,
        help="Minimum alpha-strength score required for intent approval",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-probability-confidence",
        type=float,
        default=None,
        help="Optional minimum modeled probability confidence required for approval",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-expected-edge-net",
        type=float,
        default=None,
        help="Optional minimum modeled expected edge (net) required for approval",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-edge-to-risk-ratio",
        type=float,
        default=None,
        help=(
            "Optional minimum base edge-to-risk ratio required for approval. "
            "Computed as (probability - entry_price) / entry_price."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-base-edge-net",
        type=float,
        default=0.0,
        help="Minimum base edge (probability - entry_price) required before additive bonuses",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-probability-breakeven-gap",
        type=float,
        default=0.0,
        help="Minimum probability minus breakeven-price gap required for approval",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--disable-enforce-probability-edge-thresholds",
        action="store_true",
        help=(
            "Disable probability/edge threshold enforcement when explicit thresholds are omitted. "
            "By default, omitted thresholds are backfilled by configured fallback values."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--enforce-entry-price-probability-floor",
        action="store_true",
        help=(
            "When fallback probability thresholds are active, raise minimum probability based on entry price "
            "to avoid high-price low-conviction approvals."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--fallback-min-probability-confidence",
        type=float,
        default=None,
        help=(
            "Fallback minimum probability confidence when --min-probability-confidence is omitted "
            "(defaults to min-settlement-confidence if unset)."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--fallback-min-expected-edge-net",
        type=float,
        default=0.005,
        help=(
            "Fallback minimum expected edge (net) when --min-expected-edge-net is omitted "
            "(default: 0.005)."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--fallback-min-edge-to-risk-ratio",
        type=float,
        default=0.02,
        help=(
            "Fallback minimum base edge-to-risk ratio when --min-edge-to-risk-ratio is omitted "
            "(default: 0.02)."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--disable-interval-consistency-gate",
        action="store_true",
        help="Disable interval-consistency checks (yes/no feasibility overlap + gap checks)",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-yes-possible-gap-for-yes-side",
        type=float,
        default=0.0,
        help="Maximum allowed yes_possible_gap for YES-side intents",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-hours-to-close",
        type=float,
        default=0.0,
        help="Minimum hours-to-close required for approval",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-hours-to-close",
        type=float,
        default=48.0,
        help="Maximum hours-to-close allowed for active horizon",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-intents-per-underlying",
        type=int,
        default=6,
        help="Maximum approved intents per station/date underlying",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--taf-stale-grace-minutes",
        type=float,
        default=0.0,
        help="Optional METAR-age grace when forecast/TAF path modeling is ready",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--taf-stale-grace-max-volatility-score",
        type=float,
        default=1.0,
        help="Maximum TAF volatility score allowed for stale-grace activation",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--taf-stale-grace-max-range-width",
        type=float,
        default=10.0,
        help="Maximum forecast range width allowed for stale-grace activation",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--metar-freshness-quality-boundary-ratio",
        type=float,
        default=0.92,
        help=(
            "Age-ratio boundary (age/max_age) where near-stale quality tightening activates. "
            "Set <=0 to disable."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--metar-freshness-quality-probability-margin",
        type=float,
        default=0.03,
        help="Additional probability-confidence margin required near stale boundary",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--metar-freshness-quality-expected-edge-margin",
        type=float,
        default=0.005,
        help="Additional expected-edge margin required near stale boundary",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--disable-require-market-snapshot-seq",
        action="store_true",
        help="Disable market-sequence gating (shadow mode already defaults to this; live mode can opt out with this flag)",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--require-metar-snapshot-sha",
        action="store_true",
        help="Require METAR snapshot SHA for approval",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--disable-underlying-netting",
        action="store_true",
        help="Disable family-level netting against existing inventory",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Allow live order writes (default is shadow mode)",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--micro-live-50",
        action="store_true",
        help=(
            "Apply strict $50 micro-live caps (bankroll=50, daily risk<=3, "
            "max deployed<=20%%, live cost/day<=3, max submissions/day<=3, "
            "max orders/loop<=3, contracts/order=1, max entry<=0.85)."
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Planning bankroll forwarded to micro-execute",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Daily risk cap forwarded to micro-execute",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-total-deployed-pct",
        type=float,
        default=0.35,
        help="Maximum fraction of reference bankroll deployable per loop before plan truncation",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-same-station-exposure-pct",
        type=float,
        default=0.6,
        help="Maximum per-station share of deployed loop budget during portfolio selection",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-same-hour-cluster-exposure-pct",
        type=float,
        default=0.6,
        help="Maximum per-local-hour share of deployed loop budget during portfolio selection",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-same-underlying-exposure-pct",
        type=float,
        default=0.5,
        help="Maximum per-underlying share of deployed loop budget during portfolio selection",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-orders-per-station",
        type=int,
        default=2,
        help="Maximum planned orders per station per loop (count cap)",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-orders-per-underlying",
        type=int,
        default=2,
        help="Maximum planned orders per underlying per loop (count cap)",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-unique-stations-per-loop",
        type=int,
        default=3,
        help="Breadth quota: minimum distinct stations targeted per loop",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-unique-underlyings-per-loop",
        type=int,
        default=4,
        help="Breadth quota: minimum distinct underlying families targeted per loop",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--min-unique-local-hours-per-loop",
        type=int,
        default=2,
        help="Breadth quota: minimum distinct local-hour buckets targeted per loop",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-cooldown-minutes",
        type=float,
        default=20.0,
        help="Cooldown window that blocks re-planning the same market-side unless a material signal override is present",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-price-change-override-dollars",
        type=float,
        default=0.02,
        help="Allow cooldown override when maker entry price changes by at least this amount",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-alpha-change-override",
        type=float,
        default=0.2,
        help="Allow cooldown override when alpha-strength improves by at least this amount",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-confidence-change-override",
        type=float,
        default=0.03,
        help="Allow cooldown override when settlement confidence improves by at least this amount",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-min-observation-advance-minutes",
        type=float,
        default=2.0,
        help="Allow cooldown override when METAR observation timestamp advances by at least this many minutes",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-repeat-window-minutes",
        type=float,
        default=1440.0,
        help=(
            "Rolling window used to cap repeated planning of the same market-side "
            "(independent of cooldown elapsed minutes)"
        ),
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-max-plans-per-window",
        type=int,
        default=8,
        help="Maximum recent plans allowed per market-side within repeat window (0 disables cap)",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-history-files",
        type=int,
        default=180,
        help="Maximum recent plan CSV files scanned for market-side cooldown enforcement",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--replan-market-side-min-orders-backstop",
        type=int,
        default=1,
        help="Minimum plans kept after cooldown filtering (0 disables throughput backstop)",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--disable-historical-selection-quality",
        action="store_true",
        help="Disable settled-history quality calibration overlays during policy gating and ranking",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--historical-selection-quality-lookback-hours",
        type=float,
        default=336.0,
        help="Lookback window (hours) for settled-history selection quality profile",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--historical-selection-quality-min-resolved-market-sides",
        type=int,
        default=12,
        help="Minimum resolved unique market-side outcomes required before profile is considered fully ready",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--historical-selection-quality-min-bucket-samples",
        type=int,
        default=4,
        help="Minimum settled samples required per station/hour/signal bucket in quality profile",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--historical-selection-quality-probability-penalty-max",
        type=float,
        default=0.05,
        help="Maximum additional probability-confidence requirement from historical quality penalties",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--historical-selection-quality-expected-edge-penalty-max",
        type=float,
        default=0.006,
        help="Maximum additional expected-edge requirement from historical quality penalties",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--historical-selection-quality-score-adjust-scale",
        type=float,
        default=0.35,
        help="Score adjustment scale applied by historical quality calibration in portfolio ranking",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--historical-selection-quality-profile-max-age-hours",
        type=float,
        default=96.0,
        help="Maximum allowed age of bankroll-validation artifact used for historical quality profile",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--historical-selection-quality-preferred-model",
        default="fixed_fraction_per_underlying_family",
        help="Preferred bankroll-validation attribution model used for historical quality bucket penalties/boosts",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--weather-pattern-hardening-enabled",
        dest="weather_pattern_hardening_enabled",
        action="store_true",
        default=None,
        help="Enable weather-pattern hardening controls when supported by the trader runner",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--no-weather-pattern-hardening-enabled",
        "--disable-weather-pattern-hardening",
        dest="weather_pattern_hardening_enabled",
        action="store_false",
        help="Disable weather-pattern hardening controls when supported by the trader runner",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Forwarded cancel behavior",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Forwarded resting hold seconds",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="Forwarded live submission cap",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--max-live-cost-per-day-dollars",
        type=float,
        default=3.0,
        help="Forwarded live cost cap",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--enforce-trade-gate",
        action="store_true",
        help="Enable micro-execute trade gate checks",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--enforce-ws-state-authority",
        action="store_true",
        help="Enable ws-state authority gate",
    )
    kalshi_temperature_shadow_watch.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum ws-state staleness before gating blocks",
    )
    kalshi_temperature_shadow_watch.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_profitability = subparsers.add_parser(
        "kalshi-temperature-profitability",
        help="Summarize temperature strategy expected edge vs realized PnL and win rate over a time window",
    )
    kalshi_temperature_profitability.add_argument(
        "--hours",
        type=float,
        default=24.0,
        help="Lookback window in hours",
    )
    kalshi_temperature_profitability.add_argument(
        "--journal-db-path",
        default=None,
        help="Optional explicit execution journal SQLite path",
    )
    kalshi_temperature_profitability.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of top settled orders by absolute realized PnL in summary",
    )
    kalshi_temperature_profitability.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_recovery_advisor = subparsers.add_parser(
        "kalshi-temperature-recovery-advisor",
        aliases=("temperature-recovery-advisor",),
        help="recovery hardening pass for weather risk-off blockers",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--weather-window-hours",
        type=float,
        default=720.0,
        help="Lookback window in hours for weather-pattern profile refresh",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--weather-min-bucket-samples",
        type=int,
        default=10,
        help="Minimum bucket samples for weather-pattern profile usage",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--weather-max-profile-age-hours",
        type=float,
        default=336.0,
        help="Maximum age in hours for weather-pattern profile inputs",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--weather-negative-expectancy-attempt-share-target",
        type=float,
        default=0.50,
        help="Target ceiling for negative expectancy attempt share",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--weather-stale-metar-negative-attempt-share-target",
        type=float,
        default=0.60,
        help="Target ceiling for stale-METAR negative expectancy attempt share",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--weather-stale-metar-attempt-share-target",
        type=float,
        default=0.65,
        help="Target ceiling for stale-METAR attempt share",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--weather-min-attempts-target",
        type=int,
        default=200,
        help="Minimum attempt count target for recovery readiness",
    )
    kalshi_temperature_recovery_advisor.add_argument(
        "--optimizer-top-n",
        type=int,
        default=5,
        help="Top-N optimizer candidates considered for recovery recommendations",
    )

    kalshi_temperature_recovery_loop = subparsers.add_parser(
        "kalshi-temperature-recovery-loop",
        aliases=("temperature-recovery-loop",),
        help="Run automated weather-risk remediation loop until gaps clear or progress stalls",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--trader-env-file",
        default="data/research/account_onboarding.env.template",
        help="Env file path used for trader remediation action",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--max-iterations",
        type=int,
        default=4,
        help="Maximum number of remediation loop iterations",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--stall-iterations",
        type=int,
        default=2,
        help="Maximum number of consecutive non-improving iterations before stop",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--min-gap-improvement",
        type=float,
        default=0.01,
        help="Minimum aggregate gap improvement required to reset stall counter",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--weather-window-hours",
        type=float,
        default=720.0,
        help="Lookback window in hours for weather-pattern profile refresh",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--weather-min-bucket-samples",
        type=int,
        default=10,
        help="Minimum bucket samples for weather-pattern profile usage",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--weather-max-profile-age-hours",
        type=float,
        default=336.0,
        help="Maximum age in hours for weather-pattern profile inputs",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--weather-negative-expectancy-attempt-share-target",
        type=float,
        default=0.50,
        help="Target ceiling for negative expectancy attempt share",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--weather-stale-metar-negative-attempt-share-target",
        type=float,
        default=0.60,
        help="Target ceiling for stale-METAR negative expectancy attempt share",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--weather-stale-metar-attempt-share-target",
        type=float,
        default=0.65,
        help="Target ceiling for stale-METAR attempt share",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--weather-min-attempts-target",
        type=int,
        default=200,
        help="Minimum attempt count target for recovery readiness",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--optimizer-top-n",
        type=int,
        default=5,
        help="Top-N optimizer candidates considered for recovery recommendations",
    )
    plateau_negative_regime_suppression_group = kalshi_temperature_recovery_loop.add_mutually_exclusive_group()
    plateau_negative_regime_suppression_group.add_argument(
        "--plateau-negative-regime-suppression-enabled",
        dest="plateau_negative_regime_suppression_enabled",
        action="store_true",
        help="Enable negative-regime suppression during plateau break remediation",
    )
    plateau_negative_regime_suppression_group.add_argument(
        "--no-plateau-negative-regime-suppression-enabled",
        dest="plateau_negative_regime_suppression_enabled",
        action="store_false",
        help="Disable negative-regime suppression during plateau break remediation",
    )
    kalshi_temperature_recovery_loop.set_defaults(plateau_negative_regime_suppression_enabled=True)
    kalshi_temperature_recovery_loop.add_argument(
        "--plateau-negative-regime-suppression-min-bucket-samples",
        type=int,
        default=18,
        help="Minimum bucket samples required for negative-regime suppression candidates",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--plateau-negative-regime-suppression-expectancy-threshold",
        type=float,
        default=-0.06,
        help="Expectancy threshold used to identify negative-regime suppression buckets",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--plateau-negative-regime-suppression-top-n",
        type=int,
        default=10,
        help="Top-N negative-regime suppression buckets to enforce",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--retune-weather-window-hours-cap",
        type=float,
        default=336.0,
        help="Maximum weather-pattern lookback window used during retune remediation actions",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--retune-overblocking-blocked-share-threshold",
        type=float,
        default=0.25,
        help="Blocked-share threshold above which retune actions classify suppression as overblocking",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--retune-underblocking-min-top-n",
        type=int,
        default=16,
        help="Minimum suppression top-N used when retune actions classify suppression as underblocking",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--retune-overblocking-max-top-n",
        type=int,
        default=4,
        help="Maximum suppression top-N used when retune actions classify suppression as overblocking",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--retune-min-bucket-samples-target",
        type=int,
        default=14,
        help="Retune target minimum bucket samples for suppression profile adjustments",
    )
    kalshi_temperature_recovery_loop.add_argument(
        "--retune-expectancy-threshold-target",
        type=float,
        default=-0.045,
        help="Retune target expectancy threshold for suppression profile adjustments",
    )
    execute_actions_group = kalshi_temperature_recovery_loop.add_mutually_exclusive_group()
    execute_actions_group.add_argument(
        "--execute-actions",
        dest="execute_actions",
        action="store_true",
        help="Execute remediation actions each iteration",
    )
    execute_actions_group.add_argument(
        "--no-execute-actions",
        dest="execute_actions",
        action="store_false",
        help="Do not execute remediation actions",
    )
    kalshi_temperature_recovery_loop.set_defaults(execute_actions=True)

    kalshi_temperature_recovery_campaign = subparsers.add_parser(
        "kalshi-temperature-recovery-campaign",
        aliases=("temperature-recovery-campaign",),
        help="Run multi-profile recovery campaign and recommend best convergence settings",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--trader-env-file",
        default="data/research/account_onboarding.env.template",
        help="Env file path used for trader remediation actions",
    )
    campaign_execute_actions_group = kalshi_temperature_recovery_campaign.add_mutually_exclusive_group()
    campaign_execute_actions_group.add_argument(
        "--execute-actions",
        dest="execute_actions",
        action="store_true",
        help="Execute remediation actions in campaign profiles",
    )
    campaign_execute_actions_group.add_argument(
        "--no-execute-actions",
        dest="execute_actions",
        action="store_false",
        help="Do not execute remediation actions in campaign profiles",
    )
    kalshi_temperature_recovery_campaign.set_defaults(execute_actions=True)
    kalshi_temperature_recovery_campaign.add_argument(
        "--profiles-json",
        default=None,
        help="Optional JSON file containing a top-level profiles list or a direct profiles list payload",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--weather-window-hours",
        type=float,
        default=720.0,
        help="Lookback window in hours for weather-pattern profile refresh",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--weather-min-bucket-samples",
        type=int,
        default=10,
        help="Minimum bucket samples for weather-pattern profile usage",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--weather-max-profile-age-hours",
        type=float,
        default=336.0,
        help="Maximum age in hours for weather-pattern profile inputs",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--weather-negative-expectancy-attempt-share-target",
        type=float,
        default=0.50,
        help="Target ceiling for negative expectancy attempt share",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--weather-stale-metar-negative-attempt-share-target",
        type=float,
        default=0.60,
        help="Target ceiling for stale-METAR negative expectancy attempt share",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--weather-stale-metar-attempt-share-target",
        type=float,
        default=0.65,
        help="Target ceiling for stale-METAR attempt share",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--weather-min-attempts-target",
        type=int,
        default=200,
        help="Minimum attempt count target for recovery readiness",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--optimizer-top-n",
        type=int,
        default=5,
        help="Top-N optimizer candidates considered for recovery recommendations",
    )
    campaign_plateau_negative_regime_suppression_group = (
        kalshi_temperature_recovery_campaign.add_mutually_exclusive_group()
    )
    campaign_plateau_negative_regime_suppression_group.add_argument(
        "--plateau-negative-regime-suppression-enabled",
        dest="plateau_negative_regime_suppression_enabled",
        action="store_true",
        help="Enable negative-regime suppression during plateau break remediation",
    )
    campaign_plateau_negative_regime_suppression_group.add_argument(
        "--no-plateau-negative-regime-suppression-enabled",
        dest="plateau_negative_regime_suppression_enabled",
        action="store_false",
        help="Disable negative-regime suppression during plateau break remediation",
    )
    kalshi_temperature_recovery_campaign.set_defaults(plateau_negative_regime_suppression_enabled=True)
    kalshi_temperature_recovery_campaign.add_argument(
        "--plateau-negative-regime-suppression-min-bucket-samples",
        type=int,
        default=18,
        help="Minimum bucket samples required for negative-regime suppression candidates",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--plateau-negative-regime-suppression-expectancy-threshold",
        type=float,
        default=-0.06,
        help="Expectancy threshold used to identify negative-regime suppression buckets",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--plateau-negative-regime-suppression-top-n",
        type=int,
        default=10,
        help="Top-N negative-regime suppression buckets to enforce",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--retune-weather-window-hours-cap",
        type=float,
        default=336.0,
        help="Maximum weather-pattern lookback window used during retune remediation actions",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--retune-overblocking-blocked-share-threshold",
        type=float,
        default=0.25,
        help="Blocked-share threshold above which retune actions classify suppression as overblocking",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--retune-underblocking-min-top-n",
        type=int,
        default=16,
        help="Minimum suppression top-N used when retune actions classify suppression as underblocking",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--retune-overblocking-max-top-n",
        type=int,
        default=4,
        help="Maximum suppression top-N used when retune actions classify suppression as overblocking",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--retune-min-bucket-samples-target",
        type=int,
        default=14,
        help="Retune target minimum bucket samples for suppression profile adjustments",
    )
    kalshi_temperature_recovery_campaign.add_argument(
        "--retune-expectancy-threshold-target",
        type=float,
        default=-0.045,
        help="Retune target expectancy threshold for suppression profile adjustments",
    )

    kalshi_temperature_growth_optimizer = subparsers.add_parser(
        "kalshi-temperature-growth-optimizer",
        aliases=("temperature-growth-optimizer",),
        help="Search a temperature selection-quality profile from supplied intent files",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory for optimizer artifacts",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--intent-files",
        "--intent-file",
        dest="intent_files",
        nargs="+",
        required=True,
        help="One or more intent CSV files to use as optimizer input",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--search-bounds-json",
        "--search-bounds",
        dest="search_bounds_json",
        default=None,
        help="Optional JSON file that overrides the optimizer search bounds",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--lookback-hours-min",
        type=float,
        default=7.0 * 24.0,
        help="Minimum lookback-hours bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--lookback-hours-max",
        type=float,
        default=21.0 * 24.0,
        help="Maximum lookback-hours bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--lookback-hours-step",
        type=float,
        default=24.0,
        help="Lookback-hours search step",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--intent-hours-min",
        type=float,
        default=12.0,
        help="Minimum intent-hours bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--intent-hours-max",
        type=float,
        default=72.0,
        help="Maximum intent-hours bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--intent-hours-step",
        type=float,
        default=12.0,
        help="Intent-hours search step",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--min-resolved-market-sides-min",
        type=int,
        default=12,
        help="Minimum resolved-market-sides bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--min-resolved-market-sides-max",
        type=int,
        default=24,
        help="Maximum resolved-market-sides bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--min-bucket-samples-min",
        type=int,
        default=4,
        help="Minimum bucket-samples bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--min-bucket-samples-max",
        type=int,
        default=8,
        help="Maximum bucket-samples bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--probability-penalty-max-min",
        type=float,
        default=0.03,
        help="Minimum probability-penalty bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--probability-penalty-max-max",
        type=float,
        default=0.08,
        help="Maximum probability-penalty bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--expected-edge-penalty-max-min",
        type=float,
        default=0.004,
        help="Minimum expected-edge-penalty bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--expected-edge-penalty-max-max",
        type=float,
        default=0.01,
        help="Maximum expected-edge-penalty bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--score-adjust-scale-min",
        type=float,
        default=0.25,
        help="Minimum score-adjust-scale bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--score-adjust-scale-max",
        type=float,
        default=0.5,
        help="Maximum score-adjust-scale bound",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--score-adjust-scale-step",
        type=float,
        default=0.05,
        help="Score-adjust-scale search step",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--preferred-attribution-model",
        default="fixed_fraction_per_underlying_family",
        help="Preferred attribution model to seed the optimizer",
    )
    kalshi_temperature_growth_optimizer.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of candidate evaluations retained in the summary",
    )

    kalshi_temperature_selection_quality = subparsers.add_parser(
        "kalshi-temperature-selection-quality",
        help="Audit historical selection-quality profile and quantify current intent-level adjustment pressure",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory containing temperature artifacts",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--lookback-hours",
        type=float,
        default=14.0 * 24.0,
        help="Lookback window (hours) used to build historical selection-quality profile",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--intent-hours",
        type=float,
        default=24.0,
        help="Lookback window (hours) for recent intent CSV rows used to compute adjustment pressure",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--min-resolved-market-sides",
        type=int,
        default=12,
        help="Minimum resolved unique market-side outcomes required for profile readiness",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--min-bucket-samples",
        type=int,
        default=4,
        help="Minimum bucket sample size used for per-dimension profile penalties/boosts",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--preferred-attribution-model",
        default="fixed_fraction_per_underlying_family",
        help="Preferred bankroll-validation attribution model to source bucket overrides",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--max-profile-age-hours",
        type=float,
        default=96.0,
        help="Maximum allowed age of bankroll/profitability artifacts before profile is marked stale",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--probability-penalty-max",
        type=float,
        default=0.05,
        help="Maximum probability-confidence uplift applied by historical-quality penalties",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--expected-edge-penalty-max",
        type=float,
        default=0.006,
        help="Maximum expected-edge uplift applied by historical-quality penalties",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--score-adjust-scale",
        type=float,
        default=0.35,
        help="Scale factor for historical-quality score adjustment term",
    )
    kalshi_temperature_selection_quality.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Top source count to include in adjustment attribution diagnostics",
    )

    kalshi_temperature_execution_cost_tape = subparsers.add_parser(
        "kalshi-temperature-execution-cost-tape",
        help="Build execution-cost calibration tape from blocker, intents, ws-state, and journal telemetry",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory containing temperature artifacts",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--window-hours",
        type=float,
        default=168.0,
        help="Rolling window in hours for execution-journal cost telemetry",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--min-candidate-samples",
        type=int,
        default=200,
        help="Minimum candidate rows required before calibration status can pass",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--min-quote-coverage-ratio",
        type=float,
        default=0.60,
        help="Minimum two-sided quote coverage ratio (0-1) required for calibration readiness",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--max-tickers",
        type=int,
        default=25,
        help="Maximum top tickers included in execution-cost concentration diagnostics",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--min-global-expected-edge-share-for-exclusion",
        type=float,
        default=0.45,
        help="Minimum global expected-edge blocker share required before ticker exclusions activate",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--min-ticker-rows-for-exclusion",
        type=int,
        default=200,
        help="Minimum ticker rows required before ticker can be excluded",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--exclusion-max-quote-coverage-ratio",
        type=float,
        default=0.20,
        help="Maximum ticker quote-coverage ratio to qualify for exclusion recommendation",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--max-ticker-mean-spread-for-exclusion",
        type=float,
        default=0.10,
        help="Maximum ticker mean spread threshold used by exclusion recommendation logic",
    )
    kalshi_temperature_execution_cost_tape.add_argument(
        "--max-excluded-tickers",
        type=int,
        default=12,
        help="Maximum exclusion recommendation list size",
    )

    kalshi_temperature_refill_trial_balance = subparsers.add_parser(
        "kalshi-temperature-refill-trial-balance",
        help="Reset/refill the persistent trial balance used by timed profitability checkpoints",
    )
    kalshi_temperature_refill_trial_balance.add_argument(
        "--starting-balance-dollars",
        type=float,
        default=1000.0,
        help="Starting balance to persist after refill",
    )
    kalshi_temperature_refill_trial_balance.add_argument(
        "--reason",
        default="manual_refill",
        help="Reset reason label stored in trial_balance_state.json",
    )
    kalshi_temperature_refill_trial_balance.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory containing checkpoints/trial_balance_state.json",
    )

    kalshi_temperature_bankroll_validation = subparsers.add_parser(
        "kalshi-temperature-bankroll-validation",
        help="Truth-first alpha breadth and bankroll viability validation across row/order/market-side/family layers",
    )
    kalshi_temperature_bankroll_validation.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory containing temperature artifacts",
    )
    kalshi_temperature_bankroll_validation.add_argument(
        "--hours",
        type=float,
        default=24.0,
        help="Rolling lookback window in hours",
    )
    kalshi_temperature_bankroll_validation.add_argument(
        "--reference-bankroll-dollars",
        type=float,
        default=1000.0,
        help="Reference bankroll used for sizing and viability simulation",
    )
    kalshi_temperature_bankroll_validation.add_argument(
        "--sizing-models-json",
        default=None,
        help="JSON string or JSON file path overriding sizing model parameters",
    )
    kalshi_temperature_bankroll_validation.add_argument(
        "--slippage-bps-list",
        default="0,5,10",
        help="Comma-separated slippage basis points or JSON file path",
    )
    kalshi_temperature_bankroll_validation.add_argument(
        "--fee-model-json",
        default=None,
        help="JSON string or JSON file path for fee model and HYSA comparison assumptions",
    )
    kalshi_temperature_bankroll_validation.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Top and bottom contributor count per attribution group",
    )

    kalshi_temperature_alpha_gap_report = subparsers.add_parser(
        "kalshi-temperature-alpha-gap-report",
        help="Quantify current signal ceiling and missing layers vs broader ColdMath-style weather engine",
    )
    kalshi_temperature_alpha_gap_report.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory containing temperature artifacts",
    )
    kalshi_temperature_alpha_gap_report.add_argument(
        "--hours",
        type=float,
        default=24.0,
        help="Rolling lookback window in hours",
    )
    kalshi_temperature_alpha_gap_report.add_argument(
        "--reference-bankroll-dollars",
        type=float,
        default=1000.0,
        help="Reference bankroll used in embedded validation context",
    )
    kalshi_temperature_alpha_gap_report.add_argument(
        "--sizing-models-json",
        default=None,
        help="JSON string or JSON file path overriding sizing model parameters",
    )
    kalshi_temperature_alpha_gap_report.add_argument(
        "--slippage-bps-list",
        default="0,5,10",
        help="Comma-separated slippage basis points or JSON file path",
    )
    kalshi_temperature_alpha_gap_report.add_argument(
        "--fee-model-json",
        default=None,
        help="JSON string or JSON file path for fee model and HYSA comparison assumptions",
    )
    kalshi_temperature_alpha_gap_report.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Top and bottom contributor count per attribution group",
    )
    kalshi_temperature_alpha_gap_report.add_argument(
        "--source-bankroll-validation-file",
        default=None,
        help="Optional precomputed kalshi_temperature_bankroll_validation JSON to reuse instead of recomputing",
    )

    kalshi_temperature_live_readiness = subparsers.add_parser(
        "kalshi-temperature-live-readiness",
        help="Evaluate shadow-settled readiness for risking real money across rolling horizons (1d to 1yr)",
    )
    kalshi_temperature_live_readiness.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory containing temperature artifacts",
    )
    kalshi_temperature_live_readiness.add_argument(
        "--horizons",
        default="1d,7d,14d,21d,28d,3mo,6mo,1yr",
        help="Comma-separated horizons (presets: 1d,7d,14d,21d,28d,3mo,6mo,1yr; or custom 48h/30d)",
    )
    kalshi_temperature_live_readiness.add_argument(
        "--reference-bankroll-dollars",
        type=float,
        default=1000.0,
        help="Reference bankroll used for projected and simulated returns",
    )
    kalshi_temperature_live_readiness.add_argument(
        "--sizing-models-json",
        default=None,
        help="JSON string or JSON file path overriding sizing model parameters",
    )
    kalshi_temperature_live_readiness.add_argument(
        "--slippage-bps-list",
        default="0,5,10",
        help="Comma-separated slippage basis points or JSON file path",
    )
    kalshi_temperature_live_readiness.add_argument(
        "--fee-model-json",
        default=None,
        help="JSON string or JSON file path for fee model and HYSA comparison assumptions",
    )
    kalshi_temperature_live_readiness.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Top and bottom contributor count per attribution group",
    )

    kalshi_temperature_go_live_gate = subparsers.add_parser(
        "kalshi-temperature-go-live-gate",
        help="One-shot PASS/FAIL live promotion gate built from rolling readiness horizons",
    )
    kalshi_temperature_go_live_gate.add_argument(
        "--output-dir",
        default="outputs",
        help="Output directory containing temperature artifacts",
    )
    kalshi_temperature_go_live_gate.add_argument(
        "--horizons",
        default="1d,7d,14d,21d,28d,3mo,6mo,1yr",
        help="Comma-separated horizons (presets: 1d,7d,14d,21d,28d,3mo,6mo,1yr; or custom 48h/30d)",
    )
    kalshi_temperature_go_live_gate.add_argument(
        "--reference-bankroll-dollars",
        type=float,
        default=1000.0,
        help="Reference bankroll used for projected and simulated returns",
    )
    kalshi_temperature_go_live_gate.add_argument(
        "--sizing-models-json",
        default=None,
        help="JSON string or JSON file path overriding sizing model parameters",
    )
    kalshi_temperature_go_live_gate.add_argument(
        "--slippage-bps-list",
        default="0,5,10",
        help="Comma-separated slippage basis points or JSON file path",
    )
    kalshi_temperature_go_live_gate.add_argument(
        "--fee-model-json",
        default=None,
        help="JSON string or JSON file path for fee model and HYSA comparison assumptions",
    )
    kalshi_temperature_go_live_gate.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Top and bottom contributor count per attribution group",
    )
    kalshi_temperature_go_live_gate.add_argument(
        "--source-live-readiness-file",
        default=None,
        help="Optional precomputed kalshi_temperature_live_readiness JSON to reuse instead of recomputing",
    )

    polymarket_market_ingest = subparsers.add_parser(
        "polymarket-market-ingest",
        help="Optional market-data ingest adapter for Polymarket temperature markets (no execution)",
    )
    polymarket_market_ingest.add_argument(
        "--max-markets",
        type=int,
        default=500,
        help="Maximum normalized markets to keep per run",
    )
    polymarket_market_ingest.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Gamma API page size per request",
    )
    polymarket_market_ingest.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum pages to scan per run",
    )
    polymarket_market_ingest.add_argument(
        "--gamma-base-url",
        default="https://gamma-api.polymarket.com",
        help="Polymarket Gamma API base URL",
    )
    polymarket_market_ingest.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per Gamma request",
    )
    polymarket_market_ingest.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive/closed markets when fetching pages",
    )
    polymarket_market_ingest.add_argument("--output-dir", default="outputs", help="Output directory")
    polymarket_market_ingest.add_argument(
        "--coldmath-snapshot-dir",
        default=None,
        help="Optional directory containing ColdMath-style equity.csv and positions.csv snapshots",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-equity-csv",
        default=None,
        help="Optional explicit ColdMath equity CSV path",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-positions-csv",
        default=None,
        help="Optional explicit ColdMath positions CSV path",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-wallet-address",
        default="",
        help="Optional wallet address label for snapshot metadata",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-stale-hours",
        type=float,
        default=48.0,
        help="Staleness threshold for optional ColdMath snapshot summary",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-refresh-from-api",
        action="store_true",
        help="Refresh ColdMath snapshot CSVs from Polymarket data API before ingest alignment",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-data-api-base-url",
        default="https://data-api.polymarket.com",
        help="Polymarket data API base URL used for ColdMath snapshot refresh",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-api-timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout for ColdMath data API refresh",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-positions-page-size",
        type=int,
        default=500,
        help="Page size used for paginated ColdMath positions API pulls",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-positions-max-pages",
        type=int,
        default=20,
        help="Maximum positions pages to fetch when ColdMath API refresh is enabled",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-disable-closed-positions-refresh",
        action="store_true",
        help="Skip /closed-positions API pulls during optional ColdMath snapshot refresh",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-closed-positions-page-size",
        type=int,
        default=50,
        help="Page size used for paginated ColdMath closed-positions API pulls",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-closed-positions-max-pages",
        type=int,
        default=20,
        help="Maximum closed-positions pages to fetch when ColdMath API refresh is enabled",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-disable-trades-refresh",
        action="store_true",
        help="Skip /trades API pulls during optional ColdMath snapshot refresh",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-disable-activity-refresh",
        action="store_true",
        help="Skip /activity API pulls during optional ColdMath snapshot refresh",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-disable-taker-only-trades",
        action="store_true",
        help="Skip takerOnly=true trade query during optional ColdMath snapshot refresh",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-disable-all-trade-roles",
        action="store_true",
        help="Skip takerOnly=false trade query during optional ColdMath snapshot refresh",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-trades-page-size",
        type=int,
        default=500,
        help="Page size used for paginated ColdMath trades API pulls",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-trades-max-pages",
        type=int,
        default=20,
        help="Maximum trades pages to fetch when ColdMath API refresh is enabled",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-activity-page-size",
        type=int,
        default=500,
        help="Page size used for paginated ColdMath activity API pulls",
    )
    polymarket_market_ingest.add_argument(
        "--coldmath-activity-max-pages",
        type=int,
        default=20,
        help="Maximum activity pages to fetch when ColdMath API refresh is enabled",
    )

    coldmath_snapshot_summary = subparsers.add_parser(
        "coldmath-snapshot-summary",
        help="Summarize ColdMath-style equity/positions snapshots and write normalized diagnostics",
    )
    coldmath_snapshot_summary.add_argument(
        "--snapshot-dir",
        default="tmp/coldmath_snapshot",
        help="Directory containing equity.csv and positions.csv",
    )
    coldmath_snapshot_summary.add_argument(
        "--equity-csv",
        default=None,
        help="Optional explicit equity CSV path (overrides --snapshot-dir/equity.csv)",
    )
    coldmath_snapshot_summary.add_argument(
        "--positions-csv",
        default=None,
        help="Optional explicit positions CSV path (overrides --snapshot-dir/positions.csv)",
    )
    coldmath_snapshot_summary.add_argument(
        "--wallet-address",
        default="",
        help="Optional wallet address label stored in summary output",
    )
    coldmath_snapshot_summary.add_argument(
        "--stale-hours",
        type=float,
        default=48.0,
        help="Staleness threshold in hours for valuation timestamp",
    )
    coldmath_snapshot_summary.add_argument(
        "--refresh-from-api",
        action="store_true",
        help="Refresh snapshot CSVs from Polymarket data API before summarizing",
    )
    coldmath_snapshot_summary.add_argument(
        "--data-api-base-url",
        default="https://data-api.polymarket.com",
        help="Polymarket data API base URL used for wallet snapshot refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--api-timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout (seconds) for Polymarket data API snapshot refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--positions-page-size",
        type=int,
        default=500,
        help="Page size used when fetching paginated wallet positions",
    )
    coldmath_snapshot_summary.add_argument(
        "--positions-max-pages",
        type=int,
        default=20,
        help="Maximum paginated positions pages to fetch during API refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--disable-closed-positions-refresh",
        action="store_true",
        help="Skip /closed-positions API pulls during snapshot refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--closed-positions-page-size",
        type=int,
        default=50,
        help="Page size used when fetching paginated wallet closed-positions",
    )
    coldmath_snapshot_summary.add_argument(
        "--closed-positions-max-pages",
        type=int,
        default=20,
        help="Maximum paginated closed-positions pages to fetch during API refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--disable-trades-refresh",
        action="store_true",
        help="Skip /trades API pulls during snapshot refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--disable-activity-refresh",
        action="store_true",
        help="Skip /activity API pulls during snapshot refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--disable-taker-only-trades",
        action="store_true",
        help="Skip takerOnly=true trade query during snapshot refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--disable-all-trade-roles",
        action="store_true",
        help="Skip takerOnly=false trade query during snapshot refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--trades-page-size",
        type=int,
        default=500,
        help="Page size used when fetching paginated wallet trades",
    )
    coldmath_snapshot_summary.add_argument(
        "--trades-max-pages",
        type=int,
        default=20,
        help="Maximum paginated trades pages to fetch during API refresh",
    )
    coldmath_snapshot_summary.add_argument(
        "--activity-page-size",
        type=int,
        default=500,
        help="Page size used when fetching paginated wallet activity",
    )
    coldmath_snapshot_summary.add_argument(
        "--activity-max-pages",
        type=int,
        default=20,
        help="Maximum paginated activity pages to fetch during API refresh",
    )
    coldmath_snapshot_summary.add_argument("--output-dir", default="outputs", help="Output directory")

    coldmath_replication_plan = subparsers.add_parser(
        "coldmath-replication-plan",
        help="Build a ranked ColdMath weather replication candidate plan from latest artifacts",
    )
    coldmath_replication_plan.add_argument("--output-dir", default="outputs", help="Output directory")
    coldmath_replication_plan.add_argument(
        "--top-n",
        type=int,
        default=12,
        help="Maximum replication candidates to include",
    )
    coldmath_replication_plan.add_argument(
        "--market-tickers",
        default="",
        help="Optional comma-separated market ticker override (defaults to latest ws-state summary)",
    )
    coldmath_replication_plan.add_argument(
        "--excluded-market-tickers",
        default="",
        help="Optional comma-separated market tickers to drop before ranking",
    )
    coldmath_replication_plan.add_argument(
        "--excluded-market-tickers-file",
        default="",
        help="Optional JSON/text file containing market tickers to drop (supports execution_cost_tape_latest.json)",
    )
    coldmath_replication_plan.add_argument(
        "--disable-liquidity-filter",
        action="store_true",
        help="Disable top-of-book liquidity gating for replication candidates",
    )
    coldmath_replication_plan.add_argument(
        "--disable-require-two-sided-quotes",
        action="store_true",
        help="Allow one-sided books when evaluating replication candidate tradability",
    )
    coldmath_replication_plan.add_argument(
        "--max-spread-dollars",
        type=float,
        default=0.18,
        help="Maximum acceptable spread per candidate side for liquidity gating",
    )
    coldmath_replication_plan.add_argument(
        "--min-liquidity-score",
        type=float,
        default=0.45,
        help="Minimum liquidity score threshold (0-1) for candidate eligibility",
    )
    coldmath_replication_plan.add_argument(
        "--max-family-candidates",
        type=int,
        default=3,
        help="Hard cap on selected candidates per family before backfill",
    )
    coldmath_replication_plan.add_argument(
        "--max-family-share",
        type=float,
        default=0.6,
        help="Maximum share of selected slots allocated to one family (0.1-1.0)",
    )

    decision_matrix_hardening = subparsers.add_parser(
        "decision-matrix-hardening",
        help="Diagnose consistency/profitability blockers and emit hardening backlog signals",
    )
    decision_matrix_hardening.add_argument("--output-dir", default="outputs", help="Output directory")
    decision_matrix_hardening.add_argument(
        "--window-hours",
        type=float,
        default=168.0,
        help="Window size used to align blocker-audit artifact selection",
    )
    decision_matrix_hardening.add_argument(
        "--min-settled-outcomes",
        type=int,
        default=25,
        help="Minimum settled independent outcomes required for confidence",
    )
    decision_matrix_hardening.add_argument(
        "--max-top-blocker-share",
        type=float,
        default=0.55,
        help="Maximum allowed share of blocked flow captured by one blocker",
    )
    decision_matrix_hardening.add_argument(
        "--min-approval-rate",
        type=float,
        default=0.03,
        help="Minimum approval-rate floor before throughput is considered too constrained",
    )
    decision_matrix_hardening.add_argument(
        "--min-intents-sample",
        type=int,
        default=1000,
        help="Minimum intents sample required before approval/PnL blocker checks apply",
    )
    decision_matrix_hardening.add_argument(
        "--max-sparse-edge-block-share",
        type=float,
        default=0.80,
        help="Maximum allowed sparse hardening expected-edge block share",
    )
    decision_matrix_hardening.add_argument(
        "--min-execution-cost-candidate-samples",
        type=int,
        default=200,
        help="Minimum execution-cost tape candidate rows required for expected-edge recalibration readiness",
    )
    decision_matrix_hardening.add_argument(
        "--min-execution-cost-quote-coverage-ratio",
        type=float,
        default=0.60,
        help="Minimum execution-cost tape two-sided quote coverage ratio required for expected-edge recalibration readiness",
    )

    kalshi_focus_dossier = subparsers.add_parser(
        "kalshi-focus-dossier",
        help="Build a compact dossier for the current top-focus non-sports market",
    )
    kalshi_focus_dossier.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_focus_dossier.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional watch-history CSV used to choose the current focus market",
    )
    kalshi_focus_dossier.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_focus_dossier.add_argument(
        "--recent-observation-limit",
        type=int,
        default=5,
        help="Number of recent observations embedded in the dossier",
    )
    kalshi_focus_dossier.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_prior_plan = subparsers.add_parser(
        "kalshi-micro-prior-plan",
        help="Build a read-only side-aware maker plan from non-sports priors",
    )
    kalshi_micro_prior_plan.add_argument(
        "--env-file",
        default=None,
        help="Optional env-style file used for live balance and incentive lookups",
    )
    kalshi_micro_prior_plan.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_micro_prior_plan.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_micro_prior_plan.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_micro_prior_plan.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_micro_prior_plan.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_prior_plan.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_micro_prior_plan.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_micro_prior_plan.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_micro_prior_plan.add_argument(
        "--selection-lane",
        choices=["maker_edge", "probability_first", "kelly_unified"],
        default="maker_edge",
        help="Plan ranking lane: maker-edge, probability-first compounding, or Kelly-unified",
    )
    kalshi_micro_prior_plan.add_argument(
        "--min-selected-fair-probability",
        type=float,
        default=None,
        help="Optional minimum selected-side fair probability gate (0-1)",
    )
    kalshi_micro_prior_plan.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_micro_prior_plan.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical-to-live mapping CSV path",
    )
    kalshi_micro_prior_plan.add_argument(
        "--canonical-threshold-csv",
        default="data/research/canonical_threshold_library.csv",
        help="Canonical threshold-library CSV path",
    )
    kalshi_micro_prior_plan.add_argument(
        "--disable-canonical-thresholds",
        action="store_true",
        help="Disable canonical threshold overlays and use global maker filters only",
    )
    kalshi_micro_prior_plan.add_argument(
        "--require-canonical-mapping",
        action="store_true",
        help="Only allow plans for live markets that are mapped to a canonical ticker",
    )
    kalshi_micro_prior_plan.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top plans embedded in the summary",
    )
    kalshi_micro_prior_plan.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_prior_plan.add_argument(
        "--include-incentives",
        action="store_true",
        help="Include incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_micro_prior_plan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_prior_execute = subparsers.add_parser(
        "kalshi-micro-prior-execute",
        help="Run a read-only or explicit live micro execution cycle from prior-backed side-aware plans",
    )
    kalshi_micro_prior_execute.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_prior_execute.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_micro_prior_execute.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_micro_prior_execute.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_micro_prior_execute.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_micro_prior_execute.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_prior_execute.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_micro_prior_execute.add_argument(
        "--selection-lane",
        choices=["maker_edge", "probability_first", "kelly_unified"],
        default="maker_edge",
        help="Plan ranking lane: maker-edge, probability-first compounding, or Kelly-unified",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-selected-fair-probability",
        type=float,
        default=None,
        help="Optional minimum selected-side fair probability gate for planning (0-1)",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-live-selected-fair-probability",
        type=float,
        default=None,
        help="Optional minimum selected-side fair probability gate for live trade admission (0-1)",
    )
    kalshi_micro_prior_execute.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_micro_prior_execute.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical-to-live mapping CSV path",
    )
    kalshi_micro_prior_execute.add_argument(
        "--canonical-threshold-csv",
        default="data/research/canonical_threshold_library.csv",
        help="Canonical threshold-library CSV path",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-canonical-thresholds",
        action="store_true",
        help="Disable canonical threshold overlays for dry-run analysis; live execution still enforces canonical niche policy",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-require-canonical-for-live",
        action="store_true",
        help="Disable canonical mapping requirement in dry-run reports only; live execution still requires canonical mapping",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-daily-weather-live-only",
        action="store_true",
        help="Allow non-daily-weather contracts to pass live gating (default enforces daily weather only)",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-daily-weather-board-coverage",
        action="store_true",
        help="Allow live mode even when captured history is missing daily weather board coverage",
    )
    kalshi_micro_prior_execute.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Actually submit orders if all other live safety checks pass",
    )
    kalshi_micro_prior_execute.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Cancel resting orders immediately after submission",
    )
    kalshi_micro_prior_execute.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Seconds to leave a resting order on the book before canceling it",
    )
    kalshi_micro_prior_execute.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_micro_prior_execute.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-live-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker edge net estimated fees required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_micro_prior_execute.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for live reads or writes",
    )
    kalshi_micro_prior_execute.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_micro_prior_execute.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-event-log-csv",
        default=None,
        help="Optional persistent execution-event log CSV path; defaults to outputs/kalshi_execution_event_log.csv",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-frontier-recent-rows",
        type=int,
        default=5000,
        help="Recent execution-event rows to scan when building each frontier snapshot",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-frontier-report-json",
        default=None,
        help="Optional explicit execution-frontier report JSON path; if omitted, latest report by mtime is used",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-frontier-max-report-age-seconds",
        type=float,
        default=10800.0,
        help="Maximum accepted age for the selected execution-frontier report before gating treats it as stale",
    )
    kalshi_micro_prior_execute.add_argument(
        "--enforce-ws-state-authority",
        dest="enforce_ws_state_authority",
        action="store_true",
        help="Fail closed for live orders unless websocket state is ready (not missing/stale/desynced). Default: enabled.",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-enforce-ws-state-authority",
        dest="enforce_ws_state_authority",
        action="store_false",
        help="Disable websocket-state authority gating (not recommended for unattended live mode)",
    )
    kalshi_micro_prior_execute.set_defaults(enforce_ws_state_authority=True)
    kalshi_micro_prior_execute.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_micro_prior_execute.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_micro_prior_execute.add_argument(
        "--include-incentives",
        action="store_true",
        help="Include incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_micro_prior_execute.add_argument(
        "--daily-weather-board-max-age-seconds",
        type=float,
        default=900.0,
        help="Maximum allowed age for daily-weather board snapshot before live daily-weather gating blocks",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-enabled",
        action="store_true",
        help="Promote climate-router tradable rows into a capped pilot lane before execution",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-summary-json",
        default=None,
        help="Optional explicit climate-router summary JSON path; defaults to latest router summary in output-dir",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-max-orders-per-run",
        type=int,
        default=1,
        help="Maximum promoted router pilot orders per run",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-contracts-cap",
        type=int,
        default=1,
        help="Maximum contracts per promoted router pilot order",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-required-ev-dollars",
        type=float,
        default=0.01,
        help="Minimum expected value dollars per promoted router pilot order",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-allowed-classes",
        default="tradable",
        help=(
            "Comma-separated climate opportunity classes eligible for pilot promotion "
            "(e.g. tradable,hot_positive)"
        ),
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-allowed-families",
        default="",
        help="Optional comma-separated contract-family allowlist for pilot promotion",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-excluded-families",
        default="",
        help="Optional comma-separated contract-family denylist for pilot promotion",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-policy-scope-override-enabled",
        dest="climate_router_pilot_policy_scope_override_enabled",
        action="store_true",
        help=(
            "Allow climate-router pilot rows to bypass daily-weather-only scope under pilot safety caps "
            "(max 1 order/run, contracts cap 1)"
        ),
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-climate-router-pilot-policy-scope-override",
        dest="climate_router_pilot_policy_scope_override_enabled",
        action="store_false",
        help="Disable pilot-only policy-scope override and keep strict daily-weather-only gating",
    )
    kalshi_micro_prior_execute.set_defaults(climate_router_pilot_policy_scope_override_enabled=False)
    kalshi_micro_prior_execute.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_prior_trader = subparsers.add_parser(
        "kalshi-micro-prior-trader",
        help="Run the unattended-safe prior-backed trader loop with optional capture",
    )
    kalshi_micro_prior_trader.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_prior_trader.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_micro_prior_trader.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_micro_prior_trader.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional watch-history CSV used for regime and focus-market context",
    )
    kalshi_micro_prior_trader.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_micro_prior_trader.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_micro_prior_trader.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_micro_prior_trader.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_micro_prior_trader.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_micro_prior_trader.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical-to-live mapping CSV path",
    )
    kalshi_micro_prior_trader.add_argument(
        "--canonical-threshold-csv",
        default="data/research/canonical_threshold_library.csv",
        help="Canonical threshold-library CSV path",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-canonical-thresholds",
        action="store_true",
        help="Disable canonical threshold overlays for dry-run analysis; live execution still enforces canonical niche policy",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-require-canonical-for-live",
        action="store_true",
        help="Disable canonical mapping requirement in dry-run reports only; live execution still requires canonical mapping",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-daily-weather-live-only",
        action="store_true",
        help="Allow non-daily-weather contracts to pass live gating (default enforces daily weather only)",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-daily-weather-board-coverage",
        action="store_true",
        help="Allow live mode even when captured history is missing daily weather board coverage",
    )
    kalshi_micro_prior_trader.add_argument(
        "--daily-weather-board-max-age-seconds",
        type=float,
        default=900.0,
        help="Maximum allowed age for daily-weather board snapshot before live daily-weather gating blocks",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-enabled",
        action="store_true",
        help="Promote climate-router tradable rows into a capped pilot lane before execution",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-summary-json",
        default=None,
        help="Optional explicit climate-router summary JSON path; defaults to latest router summary in output-dir",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-max-orders-per-run",
        type=int,
        default=1,
        help="Maximum promoted router pilot orders per run",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-contracts-cap",
        type=int,
        default=1,
        help="Maximum contracts per promoted router pilot order",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-required-ev-dollars",
        type=float,
        default=0.01,
        help="Minimum expected value dollars per promoted router pilot order",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-allowed-classes",
        default="tradable",
        help=(
            "Comma-separated climate opportunity classes eligible for pilot promotion "
            "(e.g. tradable,hot_positive)"
        ),
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-allowed-families",
        default="",
        help="Optional comma-separated contract-family allowlist for pilot promotion",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-excluded-families",
        default="",
        help="Optional comma-separated contract-family denylist for pilot promotion",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-policy-scope-override-enabled",
        dest="climate_router_pilot_policy_scope_override_enabled",
        action="store_true",
        help=(
            "Allow climate-router pilot rows to bypass daily-weather-only scope under pilot safety caps "
            "(max 1 order/run, contracts cap 1)"
        ),
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-climate-router-pilot-policy-scope-override",
        dest="climate_router_pilot_policy_scope_override_enabled",
        action="store_false",
        help="Disable pilot-only policy-scope override and keep strict daily-weather-only gating",
    )
    kalshi_micro_prior_trader.set_defaults(climate_router_pilot_policy_scope_override_enabled=False)
    kalshi_micro_prior_trader.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Actually submit orders if all prior-trade safety checks pass",
    )
    kalshi_micro_prior_trader.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Cancel resting orders immediately after submission",
    )
    kalshi_micro_prior_trader.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Seconds to leave a resting order on the book before canceling it",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_micro_prior_trader.add_argument(
        "--min-live-maker-edge",
        type=float,
        default=0.01,
        help="Minimum maker edge required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_trader.add_argument(
        "--min-live-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker edge net estimated fees required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_micro_prior_trader.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for live reads or writes",
    )
    kalshi_micro_prior_trader.add_argument(
        "--skip-capture",
        action="store_true",
        help="Reuse existing history instead of capturing a fresh board snapshot first",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-hours-to-close",
        type=float,
        default=4000.0,
        help="Capture markets closing within this many hours before prior execution",
    )
    kalshi_micro_prior_trader.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page during fresh capture",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-pages",
        type=int,
        default=12,
        help="Maximum Kalshi event pages to scan during fresh capture",
    )
    kalshi_micro_prior_trader.add_argument(
        "--use-temp-live-env",
        action="store_true",
        help="When live orders are enabled, create a temporary env copy with BETBOT_ENABLE_LIVE_ORDERS=1 for execute and reconcile",
    )
    kalshi_micro_prior_trader.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_micro_prior_trader.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-event-log-csv",
        default=None,
        help="Optional persistent execution-event log CSV path; defaults to outputs/kalshi_execution_event_log.csv",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-frontier-recent-rows",
        type=int,
        default=5000,
        help="Recent execution-event rows to scan when building each frontier snapshot",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-frontier-report-json",
        default=None,
        help="Optional explicit execution-frontier report JSON path; if omitted, latest report by mtime is used",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-frontier-max-report-age-seconds",
        type=float,
        default=10800.0,
        help="Maximum accepted age for the selected execution-frontier report before gating treats it as stale",
    )
    kalshi_micro_prior_trader.add_argument(
        "--enforce-ws-state-authority",
        dest="enforce_ws_state_authority",
        action="store_true",
        help="Fail closed for live orders unless websocket state is ready (not missing/stale/desynced). Default: enabled.",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-enforce-ws-state-authority",
        dest="enforce_ws_state_authority",
        action="store_false",
        help="Disable websocket-state authority gating (not recommended for unattended live mode)",
    )
    kalshi_micro_prior_trader.set_defaults(enforce_ws_state_authority=True)
    kalshi_micro_prior_trader.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_micro_prior_trader.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_micro_prior_trader.add_argument(
        "--include-incentives",
        action="store_true",
        help="Include incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-auto-refresh-weather-priors",
        action="store_true",
        help="Skip weather-specific prior refresh before news auto-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-auto-prewarm-weather-station-history",
        action="store_true",
        help="Skip station-day climatology prewarm before weather-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-prior-max-markets",
        type=int,
        default=30,
        help="Maximum weather markets to process during weather-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-allowed-contract-families",
        default="daily_rain,daily_temperature",
        help="Comma-separated weather contract families for weather-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-prewarm-max-station-day-keys",
        type=int,
        default=500,
        help="Maximum unique station/day keys to prewarm each unattended trader cycle",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-historical-lookback-years",
        type=int,
        default=15,
        help="Station-history lookback years used by weather prewarm and weather priors",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-station-history-cache-max-age-hours",
        type=float,
        default=24.0,
        help="Maximum cache age for station-history snapshots before refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-auto-refresh-priors",
        action="store_true",
        help="Skip auto-prior refresh before prior-trader execution",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-prior-max-markets",
        type=int,
        default=15,
        help="Maximum uncovered markets to process during auto-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-prior-min-evidence-count",
        type=int,
        default=2,
        help="Minimum evidence items required to write an auto prior",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-prior-min-evidence-quality",
        type=float,
        default=0.55,
        help="Minimum evidence-quality score required to write an auto prior",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-prior-min-high-trust-sources",
        type=int,
        default=1,
        help="Minimum high-trust sources required to write an auto prior",
    )
    kalshi_micro_prior_trader.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_prior_watch = subparsers.add_parser(
        "kalshi-micro-prior-watch",
        help="Run one prior-aware watch cycle: capture, status, and prior-trader dry-run from the same loop",
    )
    kalshi_micro_prior_watch.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_prior_watch.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_micro_prior_watch.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_micro_prior_watch.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional watch-history CSV used for regime and focus-market context",
    )
    kalshi_micro_prior_watch.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_micro_prior_watch.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_micro_prior_watch.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required for the generic status planning path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for the generic status planning path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for the generic status planning path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_micro_prior_watch.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical-to-live mapping CSV path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--canonical-threshold-csv",
        default="data/research/canonical_threshold_library.csv",
        help="Canonical threshold-library CSV path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--disable-canonical-thresholds",
        action="store_true",
        help="Disable canonical threshold overlays for dry-run analysis; live execution still enforces canonical niche policy",
    )
    kalshi_micro_prior_watch.add_argument(
        "--disable-require-canonical-for-live",
        action="store_true",
        help="Disable canonical mapping requirement in dry-run reports only; live execution still requires canonical mapping",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_prior_watch.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_prior_watch.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-live-maker-edge",
        type=float,
        default=0.01,
        help="Minimum maker edge required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-live-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker edge net estimated fees required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_watch.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_micro_prior_watch.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for live reads or writes",
    )
    kalshi_micro_prior_watch.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_micro_prior_watch.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_prior_watch.add_argument(
        "--include-incentives",
        action="store_true",
        help="Include incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_micro_prior_watch.add_argument(
        "--disable-auto-refresh-priors",
        action="store_true",
        help="Skip auto-prior refresh before prior-trader execution",
    )
    kalshi_micro_prior_watch.add_argument(
        "--auto-prior-max-markets",
        type=int,
        default=15,
        help="Maximum uncovered markets to process during auto-prior refresh",
    )
    kalshi_micro_prior_watch.add_argument(
        "--auto-prior-min-evidence-count",
        type=int,
        default=2,
        help="Minimum evidence items required to write an auto prior",
    )
    kalshi_micro_prior_watch.add_argument(
        "--auto-prior-min-evidence-quality",
        type=float,
        default=0.55,
        help="Minimum evidence-quality score required to write an auto prior",
    )
    kalshi_micro_prior_watch.add_argument(
        "--auto-prior-min-high-trust-sources",
        type=int,
        default=1,
        help="Minimum high-trust sources required to write an auto prior",
    )
    kalshi_micro_prior_watch.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_arb_scan = subparsers.add_parser(
        "kalshi-arb-scan",
        help="Scan mutually-exclusive Kalshi events for fee-buffered partition arbitrage opportunities",
    )
    kalshi_arb_scan.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_arb_scan.add_argument(
        "--fee-buffer-per-contract",
        type=float,
        default=0.01,
        help="Fee/slippage buffer per market leg in dollars",
    )
    kalshi_arb_scan.add_argument(
        "--min-margin-dollars",
        type=float,
        default=0.0,
        help="Only keep opportunities with at least this expected margin",
    )
    kalshi_arb_scan.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_arb_scan.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_arb_scan.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of opportunities embedded in the summary",
    )
    kalshi_arb_scan.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_arb_scan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_supervisor = subparsers.add_parser(
        "kalshi-supervisor",
        help="Run an operational Kalshi loop with rate-limited cycles, status checks, prior-trader execution, and arb scanning",
    )
    kalshi_supervisor.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_supervisor.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_supervisor.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_supervisor.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional ledger CSV path override",
    )
    kalshi_supervisor.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_supervisor.add_argument(
        "--cycles",
        type=int,
        default=1,
        help="Number of supervisor cycles to run",
    )
    kalshi_supervisor.add_argument(
        "--sleep-between-cycles-seconds",
        type=float,
        default=20.0,
        help="Delay between cycles",
    )
    kalshi_supervisor.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Allow live orders when exchange status indicates trading is active",
    )
    kalshi_supervisor.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Cancel resting orders immediately after submission",
    )
    kalshi_supervisor.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Seconds to leave a resting order on the book before canceling it",
    )
    kalshi_supervisor.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_supervisor.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_supervisor.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_supervisor.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_supervisor.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_supervisor.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_supervisor.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_supervisor.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_supervisor.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_supervisor.add_argument(
        "--min-live-maker-edge",
        type=float,
        default=0.01,
        help="Minimum maker edge required before a prior-backed live order can pass the gate",
    )
    kalshi_supervisor.add_argument(
        "--min-live-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker edge net estimated fees required before a prior-backed live order can pass the gate",
    )
    kalshi_supervisor.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_supervisor.add_argument(
        "--read-requests-per-minute",
        type=float,
        default=120.0,
        help="Read request throttle for the supervisor",
    )
    kalshi_supervisor.add_argument(
        "--write-requests-per-minute",
        type=float,
        default=30.0,
        help="Write request throttle for the supervisor",
    )
    kalshi_supervisor.add_argument(
        "--disable-failure-remediation",
        action="store_true",
        help="Disable supervisor-level retries/remediation on transient failure states",
    )
    kalshi_supervisor.add_argument(
        "--failure-remediation-max-retries",
        type=int,
        default=2,
        help="Maximum remediation retries per cycle when transient failures occur",
    )
    kalshi_supervisor.add_argument(
        "--failure-remediation-backoff-seconds",
        type=float,
        default=5.0,
        help="Base exponential backoff for remediation retries",
    )
    kalshi_supervisor.add_argument(
        "--failure-remediation-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to timeout on each supervisor remediation retry",
    )
    kalshi_supervisor.add_argument(
        "--failure-remediation-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by supervisor remediation retries",
    )
    kalshi_supervisor.add_argument(
        "--disable-arb-scan",
        action="store_true",
        help="Disable partition-arb scanning in each cycle",
    )
    kalshi_supervisor.add_argument(
        "--disable-incentives",
        action="store_true",
        help="Disable incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_supervisor.add_argument(
        "--disable-auto-refresh-priors",
        action="store_true",
        help="Skip auto-prior refresh before each prior-trader cycle",
    )
    kalshi_supervisor.add_argument(
        "--auto-prior-max-markets",
        type=int,
        default=15,
        help="Maximum uncovered markets to process during auto-prior refresh",
    )
    kalshi_supervisor.add_argument(
        "--auto-prior-min-evidence-count",
        type=int,
        default=2,
        help="Minimum evidence items required to write an auto prior",
    )
    kalshi_supervisor.add_argument(
        "--auto-prior-min-evidence-quality",
        type=float,
        default=0.55,
        help="Minimum evidence-quality score required to write an auto prior",
    )
    kalshi_supervisor.add_argument(
        "--auto-prior-min-high-trust-sources",
        type=int,
        default=1,
        help="Minimum high-trust sources required to write an auto prior",
    )
    kalshi_supervisor.add_argument(
        "--disable-enforce-ws-state-authority",
        action="store_true",
        help="Allow live supervisor cycles to proceed without websocket-state authority gating",
    )
    kalshi_supervisor.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_supervisor.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_supervisor.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for reads or writes",
    )
    kalshi_supervisor.add_argument(
        "--exchange-status-self-heal-attempts",
        type=int,
        default=2,
        help="Remediation retries when exchange status is unavailable due to upstream issues",
    )
    kalshi_supervisor.add_argument(
        "--exchange-status-self-heal-pause-seconds",
        type=float,
        default=10.0,
        help="Pause between exchange-status remediation retries",
    )
    kalshi_supervisor.add_argument(
        "--disable-exchange-status-dns-remediation",
        action="store_true",
        help="Disable DNS-doctor remediation before exchange-status retry attempts",
    )
    kalshi_supervisor.add_argument(
        "--exchange-status-self-heal-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to exchange-status timeout on each remediation retry",
    )
    kalshi_supervisor.add_argument(
        "--exchange-status-self-heal-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used during exchange-status remediation retries",
    )
    kalshi_supervisor.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_autopilot = subparsers.add_parser(
        "kalshi-autopilot",
        help="Run fully-guarded autonomous live development loop with preflight gates and progressive scaling",
    )
    kalshi_autopilot.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_autopilot.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_autopilot.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_autopilot.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_autopilot.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_autopilot.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Request live mode (still subject to automated safety gates)",
    )
    kalshi_autopilot.add_argument("--cycles", type=int, default=1, help="Number of supervisor cycles to run")
    kalshi_autopilot.add_argument(
        "--sleep-between-cycles-seconds",
        type=float,
        default=20.0,
        help="Delay between supervisor cycles",
    )
    kalshi_autopilot.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for reads or writes",
    )
    kalshi_autopilot.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_autopilot.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_autopilot.add_argument("--contracts-per-order", type=int, default=1, help="Contracts per planned order")
    kalshi_autopilot.add_argument("--max-orders", type=int, default=3, help="Maximum number of planned orders")
    kalshi_autopilot.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_autopilot.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_autopilot.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_autopilot.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_autopilot.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_autopilot.add_argument(
        "--failure-remediation-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to timeout on each supervisor remediation retry",
    )
    kalshi_autopilot.add_argument(
        "--failure-remediation-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by supervisor remediation retries",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-dns-doctor",
        action="store_true",
        help="Skip DNS preflight gate",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-live-smoke",
        action="store_true",
        help="Skip live-smoke preflight gate",
    )
    kalshi_autopilot.add_argument(
        "--preflight-live-smoke-include-odds-provider",
        action="store_true",
        help="Include odds-provider smoke check in autopilot preflight (Kalshi-only by default)",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-ws-state-collect",
        action="store_true",
        help="Skip websocket-state preflight gate",
    )
    kalshi_autopilot.add_argument(
        "--ws-collect-run-seconds",
        type=float,
        default=45.0,
        help="Seconds to collect websocket state during preflight",
    )
    kalshi_autopilot.add_argument(
        "--ws-collect-max-events",
        type=int,
        default=250,
        help="Maximum websocket events to collect during preflight",
    )
    kalshi_autopilot.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_autopilot.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_autopilot.add_argument(
        "--preflight-self-heal-attempts",
        type=int,
        default=2,
        help="In-run preflight remediation retries before forcing dry-run",
    )
    kalshi_autopilot.add_argument(
        "--preflight-self-heal-pause-seconds",
        type=float,
        default=10.0,
        help="Pause between preflight remediation retries",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-self-heal-upstream-only",
        action="store_true",
        help="Allow preflight remediation retries for non-upstream failures too",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-self-heal-retry-ws-state-gate-failures",
        action="store_true",
        help="Disable retries for websocket-state stale/empty/desynced gate failures in upstream-only mode",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-self-heal-dns-remediation",
        action="store_true",
        help="Disable remediation DNS-doctor runs between preflight retries",
    )
    kalshi_autopilot.add_argument(
        "--preflight-retry-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to preflight timeout on each retry attempt",
    )
    kalshi_autopilot.add_argument(
        "--preflight-retry-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by adaptive preflight retries",
    )
    kalshi_autopilot.add_argument(
        "--preflight-retry-ws-collect-increment-seconds",
        type=float,
        default=15.0,
        help="Additional websocket collection window added per preflight retry",
    )
    kalshi_autopilot.add_argument(
        "--preflight-retry-ws-collect-max-seconds",
        type=float,
        default=180.0,
        help="Maximum websocket collection window used by adaptive preflight retries",
    )
    kalshi_autopilot.add_argument(
        "--disable-progressive-scaling",
        action="store_true",
        help="Disable adaptive scale-up logic based on consecutive green autopilot runs",
    )
    kalshi_autopilot.add_argument(
        "--scaling-lookback-runs",
        type=int,
        default=20,
        help="How many recent autopilot summaries to inspect for scaling signal",
    )
    kalshi_autopilot.add_argument(
        "--scaling-green-runs-per-step",
        type=int,
        default=3,
        help="Consecutive green runs required before each scaling step",
    )
    kalshi_autopilot.add_argument(
        "--scaling-step-live-submissions",
        type=int,
        default=1,
        help="Additional live submissions per day per scaling step",
    )
    kalshi_autopilot.add_argument(
        "--scaling-step-live-cost-dollars",
        type=float,
        default=1.0,
        help="Additional live cost cap per day per scaling step",
    )
    kalshi_autopilot.add_argument(
        "--scaling-step-daily-risk-cap-dollars",
        type=float,
        default=1.0,
        help="Additional daily risk cap per scaling step",
    )
    kalshi_autopilot.add_argument(
        "--scaling-hard-max-live-submissions-per-day",
        type=int,
        default=12,
        help="Absolute upper bound for live submissions per day after scaling",
    )
    kalshi_autopilot.add_argument(
        "--scaling-hard-max-live-cost-per-day-dollars",
        type=float,
        default=12.0,
        help="Absolute upper bound for live cost per day after scaling",
    )
    kalshi_autopilot.add_argument(
        "--scaling-hard-max-daily-risk-cap-dollars",
        type=float,
        default=12.0,
        help="Absolute upper bound for daily risk cap after scaling",
    )
    kalshi_autopilot.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_watchdog = subparsers.add_parser(
        "kalshi-watchdog",
        help="Run continuous guarded-autopilot loop with upstream remediation and persistent live kill-switch",
    )
    kalshi_watchdog.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_watchdog.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_watchdog.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_watchdog.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_watchdog.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_watchdog.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Request live mode (still subject to automated safety gates and kill-switch)",
    )
    kalshi_watchdog.add_argument(
        "--loops",
        type=int,
        default=0,
        help="Watchdog loop count; set 0 to run continuously",
    )
    kalshi_watchdog.add_argument(
        "--sleep-between-loops-seconds",
        type=float,
        default=60.0,
        help="Sleep between healthy watchdog loops",
    )
    kalshi_watchdog.add_argument(
        "--autopilot-cycles",
        type=int,
        default=1,
        help="Number of supervisor cycles to run per watchdog loop",
    )
    kalshi_watchdog.add_argument(
        "--autopilot-sleep-between-cycles-seconds",
        type=float,
        default=20.0,
        help="Delay between supervisor cycles inside each watchdog loop",
    )
    kalshi_watchdog.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for reads or writes",
    )
    kalshi_watchdog.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_watchdog.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one loop",
    )
    kalshi_watchdog.add_argument("--contracts-per-order", type=int, default=1, help="Contracts per planned order")
    kalshi_watchdog.add_argument("--max-orders", type=int, default=3, help="Maximum number of planned orders")
    kalshi_watchdog.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_watchdog.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_watchdog.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_watchdog.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_watchdog.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_watchdog.add_argument(
        "--failure-remediation-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to timeout on each supervisor remediation retry",
    )
    kalshi_watchdog.add_argument(
        "--failure-remediation-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by supervisor remediation retries",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-dns-doctor",
        action="store_true",
        help="Skip DNS preflight gate inside autopilot",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-live-smoke",
        action="store_true",
        help="Skip live-smoke preflight gate inside autopilot",
    )
    kalshi_watchdog.add_argument(
        "--preflight-live-smoke-include-odds-provider",
        action="store_true",
        help="Include odds-provider smoke check in watchdog/autopilot preflight (Kalshi-only by default)",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-ws-state-collect",
        action="store_true",
        help="Skip websocket-state preflight gate inside autopilot",
    )
    kalshi_watchdog.add_argument(
        "--ws-collect-run-seconds",
        type=float,
        default=45.0,
        help="Seconds to collect websocket state during preflight",
    )
    kalshi_watchdog.add_argument(
        "--ws-collect-max-events",
        type=int,
        default=250,
        help="Maximum websocket events to collect during preflight",
    )
    kalshi_watchdog.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_watchdog.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_watchdog.add_argument(
        "--preflight-self-heal-attempts",
        type=int,
        default=2,
        help="In-run preflight remediation retries inside autopilot before forcing dry-run",
    )
    kalshi_watchdog.add_argument(
        "--preflight-self-heal-pause-seconds",
        type=float,
        default=10.0,
        help="Pause between preflight remediation retries inside autopilot",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-self-heal-upstream-only",
        action="store_true",
        help="Allow preflight remediation retries for non-upstream failures too",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-self-heal-retry-ws-state-gate-failures",
        action="store_true",
        help="Disable retries for websocket-state stale/empty/desynced gate failures in upstream-only mode",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-self-heal-dns-remediation",
        action="store_true",
        help="Disable remediation DNS-doctor runs between preflight retries",
    )
    kalshi_watchdog.add_argument(
        "--preflight-retry-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to preflight timeout on each retry attempt",
    )
    kalshi_watchdog.add_argument(
        "--preflight-retry-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by adaptive preflight retries",
    )
    kalshi_watchdog.add_argument(
        "--preflight-retry-ws-collect-increment-seconds",
        type=float,
        default=15.0,
        help="Additional websocket collection window added per preflight retry",
    )
    kalshi_watchdog.add_argument(
        "--preflight-retry-ws-collect-max-seconds",
        type=float,
        default=180.0,
        help="Maximum websocket collection window used by adaptive preflight retries",
    )
    kalshi_watchdog.add_argument(
        "--disable-progressive-scaling",
        action="store_true",
        help="Disable adaptive scale-up logic based on consecutive green autopilot runs",
    )
    kalshi_watchdog.add_argument(
        "--scaling-lookback-runs",
        type=int,
        default=20,
        help="How many recent autopilot summaries to inspect for scaling signal",
    )
    kalshi_watchdog.add_argument(
        "--scaling-green-runs-per-step",
        type=int,
        default=3,
        help="Consecutive green runs required before each scaling step",
    )
    kalshi_watchdog.add_argument(
        "--scaling-step-live-submissions",
        type=int,
        default=1,
        help="Additional live submissions per day per scaling step",
    )
    kalshi_watchdog.add_argument(
        "--scaling-step-live-cost-dollars",
        type=float,
        default=1.0,
        help="Additional live cost cap per day per scaling step",
    )
    kalshi_watchdog.add_argument(
        "--scaling-step-daily-risk-cap-dollars",
        type=float,
        default=1.0,
        help="Additional daily risk cap per scaling step",
    )
    kalshi_watchdog.add_argument(
        "--scaling-hard-max-live-submissions-per-day",
        type=int,
        default=12,
        help="Absolute upper bound for live submissions per day after scaling",
    )
    kalshi_watchdog.add_argument(
        "--scaling-hard-max-live-cost-per-day-dollars",
        type=float,
        default=12.0,
        help="Absolute upper bound for live cost per day after scaling",
    )
    kalshi_watchdog.add_argument(
        "--scaling-hard-max-daily-risk-cap-dollars",
        type=float,
        default=12.0,
        help="Absolute upper bound for daily risk cap after scaling",
    )
    kalshi_watchdog.add_argument(
        "--upstream-incident-threshold",
        type=int,
        default=3,
        help="Consecutive upstream incidents before kill-switch engages",
    )
    kalshi_watchdog.add_argument(
        "--kill-switch-cooldown-seconds",
        type=float,
        default=1800.0,
        help="Kill-switch hold period after escalation",
    )
    kalshi_watchdog.add_argument(
        "--healthy-runs-to-clear-kill-switch",
        type=int,
        default=1,
        help="Healthy autopilot runs required to clear an active kill-switch early",
    )
    kalshi_watchdog.add_argument(
        "--upstream-retry-backoff-base-seconds",
        type=float,
        default=15.0,
        help="Base backoff used after upstream incidents",
    )
    kalshi_watchdog.add_argument(
        "--upstream-retry-backoff-max-seconds",
        type=float,
        default=300.0,
        help="Maximum backoff used after upstream incidents",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-attempts-per-loop",
        type=int,
        default=2,
        help="In-loop remediation retries before deferring to next watchdog loop",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-pause-seconds",
        type=float,
        default=10.0,
        help="Pause between in-loop remediation retry attempts",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-retry-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to watchdog in-loop retry timeout on each autopilot re-attempt",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-retry-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by watchdog in-loop autopilot re-attempts",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-retry-ws-collect-increment-seconds",
        type=float,
        default=15.0,
        help="Additional websocket collect window added per watchdog in-loop autopilot re-attempt",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-retry-ws-collect-max-seconds",
        type=float,
        default=180.0,
        help="Maximum websocket collect window used by watchdog in-loop autopilot re-attempts",
    )
    kalshi_watchdog.add_argument(
        "--disable-remediation-dns-doctor",
        action="store_true",
        help="Disable remediation DNS doctor runs after upstream incidents",
    )
    kalshi_watchdog.add_argument(
        "--kill-switch-state-json",
        default=None,
        help="Optional path for persistent kill-switch state JSON",
    )
    kalshi_watchdog.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_plan = subparsers.add_parser(
        "kalshi-micro-plan",
        help="Build a tiny read-only Kalshi order plan for a small bankroll",
    )
    kalshi_micro_plan.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_plan.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the order-planning budget",
    )
    kalshi_micro_plan.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_plan.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_plan.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_plan.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_plan.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_plan.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_plan.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_plan.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_plan.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_plan.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_plan.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_plan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_execute = subparsers.add_parser(
        "kalshi-micro-execute",
        help="Dry-run or execute a tiny non-sports Kalshi maker-order workflow",
    )
    kalshi_micro_execute.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_execute.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the order-planning budget",
    )
    kalshi_micro_execute.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_execute.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_execute.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_execute.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_execute.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_execute.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_execute.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_execute.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_execute.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_execute.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_execute.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Actually submit live orders. Also requires BETBOT_ENABLE_LIVE_ORDERS in the env file.",
    )
    kalshi_micro_execute.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="If positive, let a resting maker order sit for this many seconds before canceling it.",
    )
    kalshi_micro_execute.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="After a successful submit, immediately cancel any resting order to smoke-test submit/cancel flow.",
    )
    kalshi_micro_execute.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_execute.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_execute.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_micro_execute.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_execute.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_execute.add_argument(
        "--execution-event-log-csv",
        default=None,
        help="Optional persistent execution-event log CSV path; defaults to outputs/kalshi_execution_event_log.csv",
    )
    kalshi_micro_execute.add_argument(
        "--execution-journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_micro_execute.add_argument(
        "--execution-frontier-recent-rows",
        type=int,
        default=5000,
        help="Recent execution-event rows to scan when building each frontier snapshot",
    )
    kalshi_micro_execute.add_argument(
        "--enforce-ws-state-authority",
        action="store_true",
        help="Fail closed for live orders unless websocket state is ready (not missing/stale/desynced)",
    )
    kalshi_micro_execute.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_micro_execute.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_micro_execute.add_argument(
        "--history-csv",
        default=None,
        help="Optional persistent non-sports history CSV path used for the trade gate (defaults to <output-dir>/kalshi_nonsports_history.csv)",
    )
    kalshi_micro_execute.add_argument(
        "--enforce-trade-gate",
        action="store_true",
        help="Require the multi-snapshot trade gate to pass before any live write is allowed",
    )
    kalshi_micro_execute.add_argument(
        "--order-group-auto-create",
        action="store_true",
        help="Create a Kalshi order group for this run and attach submitted orders to it",
    )
    kalshi_micro_execute.add_argument(
        "--order-group-contract-limit",
        type=int,
        default=None,
        help="Optional rolling-15s contracts limit used when auto-creating an order group",
    )
    kalshi_micro_execute.add_argument(
        "--disable-order-group-fetch-after-run",
        action="store_true",
        help="Skip fetching final order-group state telemetry at the end of the run",
    )
    kalshi_micro_execute.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_execute.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_gate = subparsers.add_parser(
        "kalshi-micro-gate",
        help="Evaluate whether the current non-sports board is strong enough for tiny live automation",
    )
    kalshi_micro_gate.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_gate.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the gate planning budget",
    )
    kalshi_micro_gate.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_gate.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_gate.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_gate.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_gate.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_gate.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_gate.add_argument(
        "--history-csv",
        default=None,
        help="Optional persistent non-sports history CSV path used for the gate (defaults to <output-dir>/kalshi_nonsports_history.csv)",
    )
    kalshi_micro_gate.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_gate.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_trader = subparsers.add_parser(
        "kalshi-micro-trader",
        help="Run the gated micro trader: gate first, then execute and reconcile only if allowed",
    )
    kalshi_micro_trader.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_trader.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the trader planning budget",
    )
    kalshi_micro_trader.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_trader.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_trader.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_trader.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_trader.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_trader.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_trader.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_trader.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_trader.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_trader.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_trader.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Actually submit live orders after the gate passes. Also requires BETBOT_ENABLE_LIVE_ORDERS in the env file.",
    )
    kalshi_micro_trader.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="If positive, let a resting maker order sit for this many seconds before canceling it.",
    )
    kalshi_micro_trader.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="After a successful submit, immediately cancel any resting order.",
    )
    kalshi_micro_trader.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_trader.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_trader.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_trader.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_trader.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional stable CSV for status/watch history; defaults to outputs/kalshi_micro_watch_history.csv",
    )
    kalshi_micro_trader.add_argument(
        "--history-csv",
        default=None,
        help="Optional persistent non-sports history CSV path used for the trade gate (defaults to <output-dir>/kalshi_nonsports_history.csv)",
    )
    kalshi_micro_trader.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_trader.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_watch = subparsers.add_parser(
        "kalshi-micro-watch",
        help="Run a sequential read-only watch cycle: capture first, then status from that same snapshot",
    )
    kalshi_micro_watch.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_watch.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the watch planning budget",
    )
    kalshi_micro_watch.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_watch.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_watch.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_watch.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_watch.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_watch.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_watch.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_watch.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_watch.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_watch.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_watch.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_watch.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_watch.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_watch.add_argument(
        "--history-csv",
        default=None,
        help="Optional persistent non-sports history CSV path used for the watch cycle (defaults to <output-dir>/kalshi_nonsports_history.csv)",
    )
    kalshi_micro_watch.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_watch.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional stable CSV for watch-run summaries; defaults to outputs/kalshi_micro_watch_history.csv",
    )
    kalshi_micro_watch.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_reconcile = subparsers.add_parser(
        "kalshi-micro-reconcile",
        help="Audit orders, queue positions, fees, and exposure after a micro execution run",
    )
    kalshi_micro_reconcile.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_reconcile.add_argument(
        "--execute-summary-file",
        default=None,
        help="Optional kalshi_micro_execute_summary JSON path; defaults to the newest one in outputs/",
    )
    kalshi_micro_reconcile.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_reconcile.add_argument(
        "--execution-journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_micro_reconcile.add_argument(
        "--max-historical-pages",
        type=int,
        default=5,
        help="Maximum historical-order pages to scan when an order is no longer in current orders",
    )
    kalshi_micro_reconcile.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_reconcile.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_execution_frontier = subparsers.add_parser(
        "kalshi-execution-frontier",
        help="Summarize the execution frontier from the persistent execution journal",
    )
    kalshi_execution_frontier.add_argument(
        "--journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_execution_frontier.add_argument(
        "--event-log-csv",
        default=None,
        help="Legacy alias for journal path. If this ends with .csv, .sqlite3 will be used beside it.",
    )
    kalshi_execution_frontier.add_argument(
        "--recent-rows",
        type=int,
        default=5000,
        help="Recent execution events to scan",
    )
    kalshi_execution_frontier.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_ws_state_replay = subparsers.add_parser(
        "kalshi-ws-state-replay",
        help="Replay websocket NDJSON events into authoritative ws-state JSON and health summary",
    )
    kalshi_ws_state_replay.add_argument(
        "--events-ndjson",
        required=True,
        help="Path to NDJSON websocket events (orderbook snapshot/delta, user orders/fills, positions)",
    )
    kalshi_ws_state_replay.add_argument(
        "--ws-state-json",
        default=None,
        help="Output websocket-state JSON path; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_ws_state_replay.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Max staleness threshold included in the generated authority summary",
    )
    kalshi_ws_state_replay.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_ws_state_collect = subparsers.add_parser(
        "kalshi-ws-state-collect",
        help="Connect to Kalshi websocket channels directly and maintain authoritative ws-state JSON",
    )
    kalshi_ws_state_collect.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file containing KALSHI credentials and environment",
    )
    kalshi_ws_state_collect.add_argument(
        "--channels",
        default="orderbook_snapshot,orderbook_delta,user_orders,user_fills,market_positions",
        help="Comma-separated websocket channels to subscribe",
    )
    kalshi_ws_state_collect.add_argument(
        "--market-tickers",
        default="",
        help="Optional comma-separated market tickers for market-scoped channels",
    )
    kalshi_ws_state_collect.add_argument(
        "--run-seconds",
        type=float,
        default=120.0,
        help="Wall-clock runtime budget for this collector pass",
    )
    kalshi_ws_state_collect.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after this many normalized events (0 means no explicit limit)",
    )
    kalshi_ws_state_collect.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=10.0,
        help="Socket connect timeout for each websocket handshake",
    )
    kalshi_ws_state_collect.add_argument(
        "--read-timeout-seconds",
        type=float,
        default=1.0,
        help="Read timeout for websocket receive loop",
    )
    kalshi_ws_state_collect.add_argument(
        "--ping-interval-seconds",
        type=float,
        default=15.0,
        help="Client ping cadence to keep the websocket session active",
    )
    kalshi_ws_state_collect.add_argument(
        "--flush-state-every-seconds",
        type=float,
        default=2.0,
        help="How often to persist current ws-state JSON during collection",
    )
    kalshi_ws_state_collect.add_argument(
        "--reconnect-max-attempts",
        type=int,
        default=8,
        help="Maximum reconnects after websocket disconnects within one run",
    )
    kalshi_ws_state_collect.add_argument(
        "--reconnect-backoff-seconds",
        type=float,
        default=1.0,
        help="Base reconnect backoff (exponential by reconnect count)",
    )
    kalshi_ws_state_collect.add_argument(
        "--ws-events-ndjson",
        default=None,
        help="Optional NDJSON event log path; defaults to outputs/kalshi_ws_events_<stamp>.ndjson",
    )
    kalshi_ws_state_collect.add_argument(
        "--ws-state-json",
        default=None,
        help="Output websocket-state JSON path; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_ws_state_collect.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Max staleness threshold embedded in state health summaries",
    )
    kalshi_ws_state_collect.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_climate_realtime_router = subparsers.add_parser(
        "kalshi-climate-realtime-router",
        help="Ingest real-time climate availability and route climate opportunities by modeled edge + market availability",
    )
    kalshi_climate_realtime_router.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file containing KALSHI credentials and environment",
    )
    kalshi_climate_realtime_router.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="Path to priors CSV (includes weather priors rows)",
    )
    kalshi_climate_realtime_router.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Path to persistent non-sports history CSV",
    )
    kalshi_climate_realtime_router.add_argument(
        "--availability-db-path",
        default=None,
        help="Optional explicit SQLite path for climate availability store (default: outputs/kalshi_climate_availability.sqlite3)",
    )
    kalshi_climate_realtime_router.add_argument(
        "--market-tickers",
        default="",
        help="Optional comma-separated market tickers to monitor; defaults to top climate rows by edge",
    )
    kalshi_climate_realtime_router.add_argument(
        "--ws-channels",
        default="orderbook_snapshot,orderbook_delta,ticker,public_trades,user_fills,market_positions",
        help="Comma-separated websocket channels to subscribe during realtime ingest",
    )
    kalshi_climate_realtime_router.add_argument(
        "--run-seconds",
        type=float,
        default=45.0,
        help="Realtime websocket ingest duration in seconds",
    )
    kalshi_climate_realtime_router.add_argument(
        "--max-markets",
        type=int,
        default=40,
        help="Maximum climate market tickers to monitor when market-tickers is not provided",
    )
    kalshi_climate_realtime_router.add_argument(
        "--seed-recent-markets",
        dest="seed_recent_markets",
        action="store_true",
        default=True,
        help="Seed monitored tickers with recently updated open markets using GET /markets?min_updated_ts",
    )
    kalshi_climate_realtime_router.add_argument(
        "--no-seed-recent-markets",
        dest="seed_recent_markets",
        action="store_false",
        help="Disable recent-market discovery seeding and rely on priors-ranked climate rows only",
    )
    kalshi_climate_realtime_router.add_argument(
        "--recent-markets-min-updated-seconds",
        type=float,
        default=900.0,
        help="Recency window (seconds) forwarded to Kalshi recent-market discovery min_updated_ts",
    )
    kalshi_climate_realtime_router.add_argument(
        "--recent-markets-timeout-seconds",
        type=float,
        default=8.0,
        help="HTTP timeout for recent-market discovery requests",
    )
    kalshi_climate_realtime_router.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Staleness threshold forwarded to websocket collector health checks",
    )
    kalshi_climate_realtime_router.add_argument(
        "--min-theoretical-edge-net-fees",
        type=float,
        default=0.005,
        help="Minimum net edge used to classify modeled-positive opportunities",
    )
    kalshi_climate_realtime_router.add_argument(
        "--max-quote-age-seconds",
        type=float,
        default=900.0,
        help="Reserved for downstream quote-freshness routing guardrail",
    )
    kalshi_climate_realtime_router.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Planning bankroll context for routing summaries",
    )
    kalshi_climate_realtime_router.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Total routed risk cap for tradable climate opportunities",
    )
    kalshi_climate_realtime_router.add_argument(
        "--max-risk-per-bet",
        type=float,
        default=1.0,
        help="Maximum routed risk dollars per climate opportunity",
    )
    kalshi_climate_realtime_router.add_argument(
        "--availability-lookback-days",
        type=float,
        default=7.0,
        help="Lookback horizon for availability-rate metrics",
    )
    kalshi_climate_realtime_router.add_argument(
        "--availability-recent-seconds",
        type=float,
        default=900.0,
        help="Recency window for tradable/priced classification",
    )
    kalshi_climate_realtime_router.add_argument(
        "--availability-hot-trade-window-seconds",
        type=float,
        default=300.0,
        help="Recency window for public-trade activity to classify hot strips",
    )
    kalshi_climate_realtime_router.add_argument(
        "--include-contract-families",
        default="daily_rain,daily_temperature,daily_snow,monthly_climate_anomaly",
        help="Comma-separated climate contract families to include in routing",
    )
    kalshi_climate_realtime_router.add_argument(
        "--skip-realtime-collect",
        action="store_true",
        help="Skip websocket ingest and run routing from persisted availability DB + priors only",
    )
    kalshi_climate_realtime_router.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_status = subparsers.add_parser(
        "kalshi-micro-status",
        help="Run a fresh read-only micro status cycle and summarize whether to hold, watch, or act",
    )
    kalshi_micro_status.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_status.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the status planning budget",
    )
    kalshi_micro_status.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_status.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_status.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_status.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_status.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_status.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_status.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_status.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_status.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_status.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_status.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_status.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_status.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_status.add_argument(
        "--history-csv",
        default=None,
        help="Optional persistent non-sports history CSV path; defaults to outputs/kalshi_nonsports_history.csv",
    )
    kalshi_micro_status.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_status.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional stable CSV for status-run history; defaults to outputs/kalshi_micro_watch_history.csv",
    )
    kalshi_micro_status.add_argument("--output-dir", default="outputs", help="Output directory")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = 0
    optimizer_profile_meta = _load_optimizer_profile_reference(getattr(args, "optimizer_profile_json", None))
    optimizer_profile_settings = (
        optimizer_profile_meta.get("settings") if isinstance(optimizer_profile_meta.get("settings"), dict) else {}
    )
    weather_pattern_profile_meta = _load_weather_pattern_profile_reference(
        getattr(args, "weather_pattern_profile_json", None)
    )
    weather_pattern_profile = (
        weather_pattern_profile_meta.get("profile")
        if isinstance(weather_pattern_profile_meta.get("profile"), dict)
        else {}
    )

    if args.command == "backtest":
        cfg = load_config(args.config)
        candidates = load_candidates(args.input)
        summary = run_backtest(
            candidates=candidates,
            cfg=cfg,
            starting_bankroll=args.starting_bankroll,
            output_dir=args.output_dir,
        )
    elif args.command == "paper":
        cfg = load_config(args.config)
        candidates = load_candidates(args.input)
        summary = run_paper(
            candidates=candidates,
            cfg=cfg,
            starting_bankroll=args.starting_bankroll,
            output_dir=args.output_dir,
            simulate_with_outcomes=args.simulate_with_outcomes,
        )
    elif args.command == "analyze":
        start_units = units_from_dollars(args.starting_bankroll, args.risk_per_effort)
        targets = [float(x.strip()) for x in args.targets.split(",") if x.strip()]
        p_values = [float(x.strip()) for x in args.p_values.split(",") if x.strip()]

        target_rows = []
        for target_dollars in targets:
            target_units = units_from_dollars(target_dollars, args.risk_per_effort)
            row = {
                "target_bankroll": target_dollars,
                "target_units": target_units,
                "start_units": start_units,
            }
            for p in p_values:
                if target_units <= start_units:
                    row[f"p_{p:.2f}"] = 1.0
                else:
                    row[f"p_{p:.2f}"] = round(
                        hitting_probability(start_units, target_units, p), 6
                    )
            target_rows.append(row)

        rung_rows = []
        rung_levels = [args.starting_bankroll] + targets
        for idx in range(len(rung_levels) - 1):
            current = rung_levels[idx]
            nxt = rung_levels[idx + 1]
            current_units = units_from_dollars(current, args.risk_per_effort)
            next_units = units_from_dollars(nxt, args.risk_per_effort)
            row = {
                "from_bankroll": current,
                "to_bankroll": nxt,
                "from_units": current_units,
                "to_units": next_units,
            }
            for p in p_values:
                if next_units <= current_units:
                    row[f"p_{p:.2f}"] = 1.0
                else:
                    row[f"p_{p:.2f}"] = round(
                        hitting_probability(current_units, next_units, p), 6
                    )
            rung_rows.append(row)

        survivability = []
        for p in p_values:
            survivability.append(
                {
                    "p": p,
                    "eventual_success_prob": round(
                        eventual_success_probability(start_units, p), 6
                    ),
                    "units_for_90pct_success": required_starting_units(0.90, p),
                    "units_for_95pct_success": required_starting_units(0.95, p),
                }
            )

        summary = {
            "analysis_timestamp": datetime.now().isoformat(),
            "starting_bankroll": args.starting_bankroll,
            "risk_per_effort": args.risk_per_effort,
            "start_units": start_units,
            "targets": targets,
            "p_values": p_values,
            "hitting_probabilities": target_rows,
            "rung_transitions": rung_rows,
            "survivability": survivability,
        }

        if args.history_input:
            history_candidates = load_candidates(args.history_input)
            outcomes = [c.outcome for c in history_candidates if c.outcome in (0, 1)]
            wins = sum(outcomes)
            trials = len(outcomes)
            if trials > 0:
                summary["bayesian_planning"] = conservative_planning_p(
                    wins=wins,
                    trials=trials,
                    confidence=args.confidence,
                )
            else:
                summary["bayesian_planning"] = {
                    "warning": "No settled outcomes (0/1) found in history input"
                }

        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = out_dir / f"probability_analysis_{stamp}.json"
        output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        summary["output_file"] = str(output_path)
    elif args.command == "effective-config":
        summary = run_effective_config(repo_root=args.repo_root)
    elif args.command == "policy-check":
        summary = run_policy_check(
            lane=args.lane,
            lane_policy_path=args.lane_policy_path,
        )
    elif args.command == "render-board":
        summary = run_render_board(
            board_json=args.board_json,
            cycle_json=args.cycle_json,
            output_dir=args.output_dir,
        )
    elif args.command == "runtime-cycle":
        hard_required_sources = tuple(
            item.strip() for item in str(args.hard_required_sources or "").split(",") if item.strip()
        )
        coldmath_snapshot_summary: dict[str, object] | None = None
        coldmath_replication_plan_summary: dict[str, object] | None = None
        if args.coldmath_refresh_from_api:
            coldmath_snapshot_summary = run_coldmath_snapshot_summary(
                snapshot_dir=args.coldmath_snapshot_dir,
                wallet_address=args.coldmath_wallet_address,
                stale_hours=args.coldmath_stale_hours,
                output_dir=args.output_dir,
                refresh_from_api=True,
                data_api_base_url=args.coldmath_data_api_base_url,
                api_timeout_seconds=args.coldmath_api_timeout_seconds,
                positions_page_size=args.coldmath_positions_page_size,
                positions_max_pages=args.coldmath_positions_max_pages,
                refresh_closed_positions_from_api=not args.coldmath_disable_closed_positions_refresh,
                closed_positions_page_size=args.coldmath_closed_positions_page_size,
                closed_positions_max_pages=args.coldmath_closed_positions_max_pages,
                refresh_trades_from_api=not args.coldmath_disable_trades_refresh,
                refresh_activity_from_api=not args.coldmath_disable_activity_refresh,
                include_taker_only_trades=not args.coldmath_disable_taker_only_trades,
                include_all_trade_roles=not args.coldmath_disable_all_trade_roles,
                trades_page_size=args.coldmath_trades_page_size,
                trades_max_pages=args.coldmath_trades_max_pages,
                activity_page_size=args.coldmath_activity_page_size,
                activity_max_pages=args.coldmath_activity_max_pages,
            )
        if args.coldmath_build_replication_plan:
            coldmath_replication_plan_summary = run_coldmath_replication_plan(
                output_dir=args.output_dir,
                top_n=args.coldmath_replication_top_n,
                market_tickers=[
                    item.strip()
                    for item in str(args.coldmath_replication_market_tickers or "").split(",")
                    if item.strip()
                ],
                excluded_market_tickers=[
                    item.strip()
                    for item in str(args.coldmath_replication_excluded_market_tickers or "").split(",")
                    if item.strip()
                ],
                excluded_market_tickers_file=(
                    str(args.coldmath_replication_excluded_market_tickers_file or "").strip() or None
                ),
                require_liquidity_filter=not args.coldmath_replication_disable_liquidity_filter,
                require_two_sided_quotes=not args.coldmath_replication_disable_require_two_sided_quotes,
                max_spread_dollars=args.coldmath_replication_max_spread_dollars,
                min_liquidity_score=args.coldmath_replication_min_liquidity_score,
                max_family_candidates=args.coldmath_replication_max_family_candidates,
                max_family_share=args.coldmath_replication_max_family_share,
            )

        adapters = [
            KalshiMarketDataAdapter(),
            CuratedNewsAdapter(),
            OpticOddsConsensusAdapter(),
        ]
        if args.include_therundown_mapping:
            adapters.append(TheRundownMappingAdapter())

        runner = CycleRunner(adapters=adapters)
        summary = runner.run(
            CycleRunnerConfig(
                lane=args.lane,
                output_dir=args.output_dir,
                repo_root=args.repo_root,
                lane_policy_path=args.lane_policy_path,
                request_live_submit=args.request_live_submit,
                live_env_file=args.live_env_file,
                live_timeout_seconds=args.live_timeout_seconds,
                allow_simulated_live_adapter=args.allow_simulated_live_adapter,
                hard_required_sources=(hard_required_sources or None),
                approval_json_path=args.approval_json_path,
                ticket_market=args.ticket_market,
                ticket_side=args.ticket_side,
                ticket_max_cost=args.ticket_max_cost,
                ticket_expires_at=args.ticket_expires_at,
            )
        )
        if coldmath_snapshot_summary is not None:
            summary["coldmath_snapshot"] = coldmath_snapshot_summary
        if coldmath_replication_plan_summary is not None:
            summary["coldmath_replication_plan"] = coldmath_replication_plan_summary
    elif args.command == "alpha-scoreboard":
        summary = run_alpha_scoreboard(
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            benchmark_annual_return=args.benchmark_annual_return,
            plan_summary_file=args.plan_summary_file,
            daily_ops_report_file=args.daily_ops_file,
            research_queue_csv=args.research_queue_csv,
            top_research_targets=args.top_research_targets,
        )
    elif args.command == "ladder-grid":
        cfg = load_config(args.config)
        candidates = load_candidates(args.input)
        summary = run_ladder_grid(
            candidates=candidates,
            base_cfg=cfg,
            starting_bankroll=args.starting_bankroll,
            output_dir=args.output_dir,
            first_rung_offsets=parse_float_list(args.first_rung_offsets),
            rung_step_offsets=parse_float_list(args.rung_step_offsets),
            rung_counts=parse_int_list(args.rung_count_values),
            min_success_probs=parse_float_list(args.min_success_probs),
            planning_ps=parse_float_list(args.planning_ps),
            withdraw_steps=parse_float_list(args.withdraw_steps),
            min_risk_wallets=parse_float_list(args.min_risk_wallet_values),
            drawdown_penalty=args.drawdown_penalty,
            top_k=args.top_k,
            pareto_k=args.pareto_k,
        )
    elif args.command == "research-audit":
        venues = [x.strip() for x in args.venues.split(",") if x.strip()]
        jurisdictions = [x.strip() for x in args.jurisdictions.split(",") if x.strip()]
        summary = run_research_audit(
            research_dir=args.research_dir,
            venues=venues,
            jurisdictions=jurisdictions,
            output_dir=args.output_dir,
        )
    elif args.command == "canonical-universe":
        summary = run_canonical_universe(output_dir=args.output_dir)
    elif args.command == "odds-audit":
        summary = run_odds_audit(
            input_csv=args.input,
            output_dir=args.output_dir,
            max_gap_minutes=args.max_gap_minutes,
        )
    elif args.command == "onboarding-check":
        summary = run_onboarding_check(
            env_file=args.env_file,
            output_dir=args.output_dir,
        )
    elif args.command == "live-smoke":
        summary = run_live_smoke(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            include_odds_provider_check=not args.skip_odds_provider_check,
        )
        if summary.get("status") == "failed":
            exit_code = 1
    elif args.command == "dns-doctor":
        summary = run_dns_doctor(
            env_file=args.env_file,
            hosts=tuple(item.strip() for item in args.hosts.split(",") if item.strip()),
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "live-snapshot":
        summary = run_live_snapshot(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            sports_preview_limit=args.sports_preview_limit,
        )
    elif args.command == "live-candidates":
        summary = run_live_candidates(
            env_file=args.env_file,
            sport_id=args.sport_id,
            event_date=args.event_date,
            output_dir=args.output_dir,
            affiliate_ids=tuple(item.strip() for item in args.affiliate_ids.split(",") if item.strip()),
            market_ids=tuple(parse_int_list(args.market_ids)),
            min_books=args.min_books,
            offset_minutes=args.offset_minutes,
            include_in_play=args.include_in_play,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "live-paper":
        summary = run_live_paper(
            env_file=args.env_file,
            sport_id=args.sport_id,
            event_date=args.event_date,
            starting_bankroll=args.starting_bankroll,
            config_path=args.config,
            output_dir=args.output_dir,
            affiliate_ids=tuple(item.strip() for item in args.affiliate_ids.split(",") if item.strip()),
            market_ids=tuple(parse_int_list(args.market_ids)),
            min_books=args.min_books,
            offset_minutes=args.offset_minutes,
            include_in_play=args.include_in_play,
            enrich_candidates=args.enrich_candidates,
            enrichment_csv=args.enrichment_csv,
            enrichment_freshness_hours=args.enrichment_freshness_hours,
            enrichment_max_logit_shift=args.enrichment_max_logit_shift,
            enrichment_moneyline_only=not args.enrichment_include_non_moneyline,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "sports-archive":
        summary = run_sports_archive(
            env_file=args.env_file,
            sport_id=args.sport_id,
            event_dates=tuple(item.strip() for item in args.event_dates.split(",") if item.strip()),
            starting_bankroll=args.starting_bankroll,
            config_path=args.config,
            output_dir=args.output_dir,
            archive_csv=args.archive_csv,
            affiliate_ids=tuple(item.strip() for item in args.affiliate_ids.split(",") if item.strip()),
            market_ids=tuple(parse_int_list(args.market_ids)),
            min_books=args.min_books,
            offset_minutes=args.offset_minutes,
            include_in_play=args.include_in_play,
            enrich_candidates=args.enrich_candidates,
            enrichment_csv=args.enrichment_csv,
            enrichment_freshness_hours=args.enrichment_freshness_hours,
            enrichment_max_logit_shift=args.enrichment_max_logit_shift,
            enrichment_moneyline_only=not args.enrichment_include_non_moneyline,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-mlb-map":
        summary = run_kalshi_mlb_map(
            env_file=args.env_file,
            event_date=args.event_date,
            output_dir=args.output_dir,
            affiliate_ids=tuple(item.strip() for item in args.affiliate_ids.split(",") if item.strip()),
            min_books=args.min_books,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-nonsports-scan":
        summary = run_kalshi_nonsports_scan(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            max_hours_to_close=args.max_hours_to_close,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-capture":
        summary = run_kalshi_nonsports_capture(
            env_file=args.env_file,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            timeout_seconds=args.timeout_seconds,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            max_hours_to_close=args.max_hours_to_close,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-quality":
        summary = run_kalshi_nonsports_quality(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_observations=args.min_observations,
            min_mean_yes_bid=args.min_mean_yes_bid,
            min_two_sided_ratio=args.min_two_sided_ratio,
            max_mean_spread=args.max_mean_spread,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-signals":
        summary = run_kalshi_nonsports_signals(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_observations=args.min_observations,
            min_stable_ratio=args.min_stable_ratio,
            min_latest_yes_bid=args.min_latest_yes_bid,
            min_mean_yes_bid=args.min_mean_yes_bid,
            max_mean_spread=args.max_mean_spread,
            max_yes_bid_stddev=args.max_yes_bid_stddev,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-persistence":
        summary = run_kalshi_nonsports_persistence(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_tradeable_yes_bid=args.min_tradeable_yes_bid,
            max_tradeable_spread=args.max_tradeable_spread,
            min_tradeable_snapshot_count=args.min_tradeable_snapshot_count,
            min_consecutive_tradeable_snapshots=args.min_consecutive_tradeable_snapshots,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-deltas":
        summary = run_kalshi_nonsports_deltas(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_tradeable_yes_bid=args.min_tradeable_yes_bid,
            max_tradeable_spread=args.max_tradeable_spread,
            min_bid_improvement=args.min_bid_improvement,
            min_spread_improvement=args.min_spread_improvement,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-categories":
        summary = run_kalshi_nonsports_categories(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_tradeable_yes_bid=args.min_tradeable_yes_bid,
            max_tradeable_spread=args.max_tradeable_spread,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-pressure":
        summary = run_kalshi_nonsports_pressure(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_observations=args.min_observations,
            min_latest_yes_bid=args.min_latest_yes_bid,
            max_latest_spread=args.max_latest_spread,
            min_two_sided_ratio=args.min_two_sided_ratio,
            min_recent_bid_change=args.min_recent_bid_change,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-thresholds":
        summary = run_kalshi_nonsports_thresholds(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            target_yes_bid=args.target_yes_bid,
            target_spread=args.target_spread,
            recent_window=args.recent_window,
            max_hours_to_target=args.max_hours_to_target,
            min_recent_two_sided_ratio=args.min_recent_two_sided_ratio,
            min_observations=args.min_observations,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-priors":
        summary = run_kalshi_nonsports_priors(
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            top_n=args.top_n,
            contracts_per_order=args.contracts_per_order,
        )
    elif args.command == "kalshi-nonsports-research-queue":
        summary = run_kalshi_nonsports_research_queue(
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-auto-priors":
        allowed_canonical_niches = tuple(
            value.strip()
            for value in str(args.allowed_canonical_niches or "").split(",")
            if value.strip()
        )
        allowed_categories = tuple(
            value.strip()
            for value in str(args.allowed_categories or "").split(",")
            if value.strip()
        )
        disallowed_categories = tuple(
            value.strip()
            for value in str(args.disallowed_categories or "").split(",")
            if value.strip()
        )
        summary = run_kalshi_nonsports_auto_priors(
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            canonical_mapping_csv=args.canonical_mapping_csv,
            allowed_canonical_niches=allowed_canonical_niches,
            restrict_to_mapped_live_tickers=args.restrict_to_mapped_live_tickers,
            allowed_categories=(allowed_categories or None),
            disallowed_categories=(disallowed_categories or None),
            top_n=args.top_n,
            max_markets=args.max_markets,
            timeout_seconds=args.timeout_seconds,
            max_headlines_per_market=args.max_headlines_per_market,
            min_evidence_count=args.min_evidence_count,
            min_evidence_quality=args.min_evidence_quality,
            min_high_trust_sources=args.min_high_trust_sources,
            protect_manual=not args.disable_protect_manual,
            write_back_to_priors=not args.dry_run,
        )
    elif args.command == "kalshi-weather-catalog":
        summary = run_kalshi_weather_catalog(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-weather-priors":
        allowed_contract_families = tuple(
            value.strip()
            for value in str(args.allowed_contract_families or "").split(",")
            if value.strip()
        )
        summary = run_kalshi_weather_priors(
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            allowed_contract_families=allowed_contract_families,
            max_markets=args.max_markets,
            timeout_seconds=args.timeout_seconds,
            historical_lookback_years=args.historical_lookback_years,
            station_history_cache_max_age_hours=args.station_history_cache_max_age_hours,
            include_nws_gridpoint_data=not args.disable_nws_gridpoint_data,
            include_nws_observations=not args.disable_nws_observations,
            include_nws_alerts=not args.disable_nws_alerts,
            include_ncei_normals=not args.disable_ncei_normals,
            include_mrms_qpe=not args.disable_mrms_qpe,
            include_nbm_snapshot=not args.disable_nbm_snapshot,
            protect_manual=not args.disable_protect_manual,
            write_back_to_priors=not args.dry_run,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-weather-prewarm":
        summary = run_kalshi_weather_station_history_prewarm(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            historical_lookback_years=args.historical_lookback_years,
            station_history_cache_max_age_hours=args.station_history_cache_max_age_hours,
            timeout_seconds=args.timeout_seconds,
            max_station_day_keys=args.max_station_day_keys,
        )
    elif args.command == "kalshi-temperature-contract-specs":
        summary = run_kalshi_temperature_contract_specs(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-temperature-constraint-scan":
        summary = run_kalshi_temperature_constraint_scan(
            specs_csv=args.specs_csv,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            max_markets=args.max_markets,
            speci_calibration_json=args.speci_calibration_json,
        )
    elif args.command == "kalshi-temperature-settlement-state":
        summary = run_kalshi_temperature_settlement_state(
            specs_csv=args.specs_csv,
            constraint_csv=args.constraint_csv,
            output_dir=args.output_dir,
            top_n=args.top_n,
            final_report_lookup_enabled=not bool(args.disable_final_report_lookup),
            final_report_cache_ttl_minutes=args.final_report_cache_ttl_minutes,
            final_report_timeout_seconds=args.final_report_timeout_seconds,
        )
    elif args.command in {
        "kalshi-temperature-settled-outcome-throughput",
        "temperature-settled-outcome-throughput",
    }:
        summary = _run_settled_outcome_throughput_cli(
            output_dir=args.output_dir,
            summarize_only=bool(args.summarize_only),
        )
    elif args.command in {
        "kalshi-temperature-coverage-velocity-report",
        "temperature-coverage-velocity-report",
    }:
        summary = _run_coverage_velocity_report_cli(
            output_dir=args.output_dir,
            history_limit=args.history_limit,
            summarize_only=bool(args.summarize_only),
        )
    elif args.command == "kalshi-temperature-metar-ingest":
        summary = run_kalshi_temperature_metar_ingest(
            output_dir=args.output_dir,
            specs_csv=args.specs_csv,
            cache_url=args.cache_url,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command in {"kalshi-temperature-weather-pattern", "temperature-weather-pattern"}:
        summary = run_kalshi_temperature_weather_pattern(
            output_dir=args.output_dir,
            window_hours=args.window_hours,
            min_bucket_samples=args.min_bucket_samples,
            max_profile_age_hours=args.max_profile_age_hours,
        )
    elif args.command == "kalshi-temperature-trader":
        micro_live_profile_caps = _apply_micro_live_50_temperature_profile(args)
        historical_selection_quality_enabled = not args.disable_historical_selection_quality
        historical_selection_quality_lookback_hours = args.historical_selection_quality_lookback_hours
        historical_selection_quality_min_resolved_market_sides = args.historical_selection_quality_min_resolved_market_sides
        historical_selection_quality_min_bucket_samples = args.historical_selection_quality_min_bucket_samples
        historical_selection_quality_probability_penalty_max = args.historical_selection_quality_probability_penalty_max
        historical_selection_quality_expected_edge_penalty_max = args.historical_selection_quality_expected_edge_penalty_max
        historical_selection_quality_score_adjust_scale = args.historical_selection_quality_score_adjust_scale
        historical_selection_quality_profile_max_age_hours = args.historical_selection_quality_profile_max_age_hours
        historical_selection_quality_preferred_model = args.historical_selection_quality_preferred_model
        if optimizer_profile_meta.get("applied"):
            enabled_override = _parse_boolish(
                optimizer_profile_settings.get("historical_selection_quality_enabled")
            )
            if enabled_override is not None:
                historical_selection_quality_enabled = bool(enabled_override)
            historical_selection_quality_lookback_hours = (
                _parse_float(optimizer_profile_settings.get("historical_selection_quality_lookback_hours"))
                or historical_selection_quality_lookback_hours
            )
            historical_selection_quality_min_resolved_market_sides = (
                _parse_int(optimizer_profile_settings.get("historical_selection_quality_min_resolved_market_sides"))
                or historical_selection_quality_min_resolved_market_sides
            )
            historical_selection_quality_min_bucket_samples = (
                _parse_int(optimizer_profile_settings.get("historical_selection_quality_min_bucket_samples"))
                or historical_selection_quality_min_bucket_samples
            )
            historical_selection_quality_probability_penalty_max = (
                _parse_float(optimizer_profile_settings.get("historical_selection_quality_probability_penalty_max"))
                or historical_selection_quality_probability_penalty_max
            )
            historical_selection_quality_expected_edge_penalty_max = (
                _parse_float(optimizer_profile_settings.get("historical_selection_quality_expected_edge_penalty_max"))
                or historical_selection_quality_expected_edge_penalty_max
            )
            historical_selection_quality_score_adjust_scale = (
                _parse_float(optimizer_profile_settings.get("historical_selection_quality_score_adjust_scale"))
                or historical_selection_quality_score_adjust_scale
            )
            historical_selection_quality_profile_max_age_hours = (
                _parse_float(optimizer_profile_settings.get("historical_selection_quality_profile_max_age_hours"))
                or historical_selection_quality_profile_max_age_hours
            )
            historical_selection_quality_preferred_model = (
                _normalize_text(optimizer_profile_settings.get("historical_selection_quality_preferred_model"))
                or historical_selection_quality_preferred_model
            )
        trader_kwargs: dict[str, Any] = {
            "env_file": args.env_file,
            "output_dir": args.output_dir,
            "specs_csv": args.specs_csv,
            "constraint_csv": args.constraint_csv,
            "metar_summary_json": args.metar_summary_json,
            "metar_state_json": args.metar_state_json,
            "metar_ingest_min_quality_score": args.min_metar_ingest_quality_score,
            "metar_ingest_min_fresh_station_coverage_ratio": args.min_metar_fresh_station_coverage_ratio,
            "metar_ingest_require_ready_status": args.require_metar_ingest_status_ready,
            "high_price_edge_guard_enabled": args.high_price_edge_guard_enabled,
            "high_price_edge_guard_min_entry_price_dollars": args.high_price_edge_guard_min_entry_price_dollars,
            "high_price_edge_guard_min_expected_edge_net": args.high_price_edge_guard_min_expected_edge_net,
            "high_price_edge_guard_min_edge_to_risk_ratio": args.high_price_edge_guard_min_edge_to_risk_ratio,
            "ws_state_json": args.ws_state_json,
            "alpha_consensus_json": args.alpha_consensus_json,
            "settlement_state_json": args.settlement_state_json,
            "book_db_path": args.book_db_path,
            "policy_version": args.policy_version,
            "contracts_per_order": args.contracts_per_order,
            "max_orders": args.max_orders,
            "max_markets": args.max_markets,
            "timeout_seconds": args.timeout_seconds,
            "allow_live_orders": args.allow_live_orders,
            "intents_only": args.intents_only,
            "min_settlement_confidence": args.min_settlement_confidence,
            "max_metar_age_minutes": args.max_metar_age_minutes,
            "metar_age_policy_json": args.metar_age_policy_json,
            "speci_calibration_json": args.speci_calibration_json,
            "min_alpha_strength": args.min_alpha_strength,
            "min_probability_confidence": args.min_probability_confidence,
            "min_expected_edge_net": args.min_expected_edge_net,
            "min_edge_to_risk_ratio": args.min_edge_to_risk_ratio,
            "min_base_edge_net": args.min_base_edge_net,
            "min_probability_breakeven_gap": args.min_probability_breakeven_gap,
            "enforce_probability_edge_thresholds": not args.disable_enforce_probability_edge_thresholds,
            "enforce_entry_price_probability_floor": args.enforce_entry_price_probability_floor,
            "fallback_min_probability_confidence": args.fallback_min_probability_confidence,
            "fallback_min_expected_edge_net": args.fallback_min_expected_edge_net,
            "fallback_min_edge_to_risk_ratio": args.fallback_min_edge_to_risk_ratio,
            "enforce_interval_consistency": not args.disable_interval_consistency_gate,
            "max_yes_possible_gap_for_yes_side": args.max_yes_possible_gap_for_yes_side,
            "min_hours_to_close": args.min_hours_to_close,
            "max_hours_to_close": args.max_hours_to_close,
            "max_intents_per_underlying": args.max_intents_per_underlying,
            "taf_stale_grace_minutes": args.taf_stale_grace_minutes,
            "taf_stale_grace_max_volatility_score": args.taf_stale_grace_max_volatility_score,
            "taf_stale_grace_max_range_width": args.taf_stale_grace_max_range_width,
            "metar_freshness_quality_boundary_ratio": args.metar_freshness_quality_boundary_ratio,
            "metar_freshness_quality_probability_margin": args.metar_freshness_quality_probability_margin,
            "metar_freshness_quality_expected_edge_margin": args.metar_freshness_quality_expected_edge_margin,
            "yes_max_entry_price_dollars": args.yes_max_entry_price,
            "no_max_entry_price_dollars": args.no_max_entry_price,
            "require_market_snapshot_seq": not args.disable_require_market_snapshot_seq,
            "require_metar_snapshot_sha": args.require_metar_snapshot_sha,
            "enforce_underlying_netting": not args.disable_underlying_netting,
            "planning_bankroll_dollars": args.planning_bankroll,
            "daily_risk_cap_dollars": args.daily_risk_cap,
            "cancel_resting_immediately": args.cancel_resting_immediately,
            "resting_hold_seconds": args.resting_hold_seconds,
            "max_live_submissions_per_day": args.max_live_submissions_per_day,
            "max_live_cost_per_day_dollars": args.max_live_cost_per_day_dollars,
            "enforce_trade_gate": args.enforce_trade_gate,
            "enforce_ws_state_authority": args.enforce_ws_state_authority,
            "ws_state_max_age_seconds": args.ws_state_max_age_seconds,
            "max_total_deployed_pct": args.max_total_deployed_pct,
            "max_same_station_exposure_pct": args.max_same_station_exposure_pct,
            "max_same_hour_cluster_exposure_pct": args.max_same_hour_cluster_exposure_pct,
            "max_same_underlying_exposure_pct": args.max_same_underlying_exposure_pct,
            "max_orders_per_station": args.max_orders_per_station,
            "max_orders_per_underlying": args.max_orders_per_underlying,
            "min_unique_stations_per_loop": args.min_unique_stations_per_loop,
            "min_unique_underlyings_per_loop": args.min_unique_underlyings_per_loop,
            "min_unique_local_hours_per_loop": args.min_unique_local_hours_per_loop,
            "replan_market_side_cooldown_minutes": args.replan_market_side_cooldown_minutes,
            "replan_market_side_price_change_override_dollars": args.replan_market_side_price_change_override_dollars,
            "replan_market_side_alpha_change_override": args.replan_market_side_alpha_change_override,
            "replan_market_side_confidence_change_override": args.replan_market_side_confidence_change_override,
            "replan_market_side_min_observation_advance_minutes": args.replan_market_side_min_observation_advance_minutes,
            "replan_market_side_repeat_window_minutes": args.replan_market_side_repeat_window_minutes,
            "replan_market_side_max_plans_per_window": args.replan_market_side_max_plans_per_window,
            "replan_market_side_history_files": args.replan_market_side_history_files,
            "replan_market_side_min_orders_backstop": args.replan_market_side_min_orders_backstop,
            "historical_selection_quality_enabled": historical_selection_quality_enabled,
            "historical_selection_quality_lookback_hours": historical_selection_quality_lookback_hours,
            "historical_selection_quality_min_resolved_market_sides": historical_selection_quality_min_resolved_market_sides,
            "historical_selection_quality_min_bucket_samples": historical_selection_quality_min_bucket_samples,
            "historical_selection_quality_probability_penalty_max": historical_selection_quality_probability_penalty_max,
            "historical_selection_quality_expected_edge_penalty_max": historical_selection_quality_expected_edge_penalty_max,
            "historical_selection_quality_score_adjust_scale": historical_selection_quality_score_adjust_scale,
            "historical_selection_quality_profile_max_age_hours": historical_selection_quality_profile_max_age_hours,
            "historical_selection_quality_preferred_model": historical_selection_quality_preferred_model,
            "weather_pattern_profile": weather_pattern_profile if weather_pattern_profile_meta.get("applied") else None,
        }
        if args.weather_pattern_risk_off_enabled is not None:
            trader_kwargs["weather_pattern_risk_off_enabled"] = bool(args.weather_pattern_risk_off_enabled)
        if args.weather_pattern_risk_off_concentration_threshold is not None:
            trader_kwargs["weather_pattern_risk_off_concentration_threshold"] = (
                args.weather_pattern_risk_off_concentration_threshold
            )
        if args.weather_pattern_risk_off_min_attempts is not None:
            trader_kwargs["weather_pattern_risk_off_min_attempts"] = args.weather_pattern_risk_off_min_attempts
        if args.weather_pattern_risk_off_stale_metar_share_threshold is not None:
            trader_kwargs["weather_pattern_risk_off_stale_metar_share_threshold"] = (
                args.weather_pattern_risk_off_stale_metar_share_threshold
            )
        if args.weather_pattern_negative_bucket_suppression_enabled is not None:
            trader_kwargs["weather_pattern_negative_regime_suppression_enabled"] = bool(
                args.weather_pattern_negative_bucket_suppression_enabled
            )
        if args.weather_pattern_negative_bucket_suppression_top_n is not None:
            trader_kwargs["weather_pattern_negative_regime_suppression_top_n"] = (
                args.weather_pattern_negative_bucket_suppression_top_n
            )
        if args.weather_pattern_negative_bucket_suppression_min_samples is not None:
            trader_kwargs["weather_pattern_negative_regime_suppression_min_bucket_samples"] = (
                args.weather_pattern_negative_bucket_suppression_min_samples
            )
        if args.weather_pattern_negative_bucket_suppression_negative_expectancy_threshold is not None:
            trader_kwargs["weather_pattern_negative_regime_suppression_expectancy_threshold"] = (
                args.weather_pattern_negative_bucket_suppression_negative_expectancy_threshold
            )
        if args.weather_pattern_hardening_enabled is not None:
            trader_kwargs["weather_pattern_hardening_enabled"] = bool(args.weather_pattern_hardening_enabled)
        summary, ignored_trader_kwargs = _invoke_runner_with_supported_kwargs(run_kalshi_temperature_trader, trader_kwargs)
        if ignored_trader_kwargs:
            summary["runner_ignored_cli_kwargs"] = sorted(ignored_trader_kwargs)
        summary["optimizer_profile_application_status"] = optimizer_profile_meta.get("status")
        summary["optimizer_profile_source_file"] = optimizer_profile_meta.get("source_file")
        summary["optimizer_profile_applied"] = bool(optimizer_profile_meta.get("applied"))
        summary["weather_pattern_profile_application_status"] = weather_pattern_profile_meta.get("status")
        summary["weather_pattern_profile_source_file"] = weather_pattern_profile_meta.get("source_file")
        summary["weather_pattern_profile_applied"] = bool(weather_pattern_profile_meta.get("applied"))
        if micro_live_profile_caps is not None:
            summary["risk_profile"] = "micro_live_50"
            summary["risk_profile_applied"] = True
            summary["risk_profile_caps"] = micro_live_profile_caps
        _promote_weather_pattern_risk_off_summary(summary)
    elif args.command == "kalshi-temperature-shadow-watch":
        micro_live_profile_caps = _apply_micro_live_50_temperature_profile(args)
        # In shadow mode we prefer discovery over strict websocket-sequence
        # gating. Live mode keeps sequence gating on by default.
        shadow_require_market_snapshot_seq = bool(args.allow_live_orders) and not args.disable_require_market_snapshot_seq
        trader_kwargs: dict[str, Any] = {
            "env_file": args.env_file,
            "output_dir": args.output_dir,
            "loops": args.loops,
            "sleep_between_loops_seconds": args.sleep_between_loops_seconds,
            "allow_live_orders": args.allow_live_orders,
            "specs_csv": args.specs_csv,
            "constraint_csv": args.constraint_csv,
            "metar_summary_json": args.metar_summary_json,
            "metar_state_json": args.metar_state_json,
            "metar_ingest_min_quality_score": args.min_metar_ingest_quality_score,
            "metar_ingest_min_fresh_station_coverage_ratio": args.min_metar_fresh_station_coverage_ratio,
            "metar_ingest_require_ready_status": args.require_metar_ingest_status_ready,
            "high_price_edge_guard_enabled": args.high_price_edge_guard_enabled,
            "high_price_edge_guard_min_entry_price_dollars": args.high_price_edge_guard_min_entry_price_dollars,
            "high_price_edge_guard_min_expected_edge_net": args.high_price_edge_guard_min_expected_edge_net,
            "high_price_edge_guard_min_edge_to_risk_ratio": args.high_price_edge_guard_min_edge_to_risk_ratio,
            "ws_state_json": args.ws_state_json,
            "alpha_consensus_json": args.alpha_consensus_json,
            "settlement_state_json": args.settlement_state_json,
            "book_db_path": args.book_db_path,
            "policy_version": args.policy_version,
            "contracts_per_order": args.contracts_per_order,
            "max_orders": args.max_orders,
            "max_markets": args.max_markets,
            "timeout_seconds": args.timeout_seconds,
            "min_settlement_confidence": args.min_settlement_confidence,
            "max_metar_age_minutes": args.max_metar_age_minutes,
            "metar_age_policy_json": args.metar_age_policy_json,
            "speci_calibration_json": args.speci_calibration_json,
            "min_alpha_strength": args.min_alpha_strength,
            "min_probability_confidence": args.min_probability_confidence,
            "min_expected_edge_net": args.min_expected_edge_net,
            "min_edge_to_risk_ratio": args.min_edge_to_risk_ratio,
            "min_base_edge_net": args.min_base_edge_net,
            "min_probability_breakeven_gap": args.min_probability_breakeven_gap,
            "enforce_probability_edge_thresholds": not args.disable_enforce_probability_edge_thresholds,
            "enforce_entry_price_probability_floor": args.enforce_entry_price_probability_floor,
            "fallback_min_probability_confidence": args.fallback_min_probability_confidence,
            "fallback_min_expected_edge_net": args.fallback_min_expected_edge_net,
            "fallback_min_edge_to_risk_ratio": args.fallback_min_edge_to_risk_ratio,
            "enforce_interval_consistency": not args.disable_interval_consistency_gate,
            "max_yes_possible_gap_for_yes_side": args.max_yes_possible_gap_for_yes_side,
            "min_hours_to_close": args.min_hours_to_close,
            "max_hours_to_close": args.max_hours_to_close,
            "max_intents_per_underlying": args.max_intents_per_underlying,
            "taf_stale_grace_minutes": args.taf_stale_grace_minutes,
            "taf_stale_grace_max_volatility_score": args.taf_stale_grace_max_volatility_score,
            "taf_stale_grace_max_range_width": args.taf_stale_grace_max_range_width,
            "metar_freshness_quality_boundary_ratio": args.metar_freshness_quality_boundary_ratio,
            "metar_freshness_quality_probability_margin": args.metar_freshness_quality_probability_margin,
            "metar_freshness_quality_expected_edge_margin": args.metar_freshness_quality_expected_edge_margin,
            "yes_max_entry_price_dollars": args.yes_max_entry_price,
            "no_max_entry_price_dollars": args.no_max_entry_price,
            "require_market_snapshot_seq": shadow_require_market_snapshot_seq,
            "require_metar_snapshot_sha": args.require_metar_snapshot_sha,
            "enforce_underlying_netting": not args.disable_underlying_netting,
            "planning_bankroll_dollars": args.planning_bankroll,
            "daily_risk_cap_dollars": args.daily_risk_cap,
            "cancel_resting_immediately": args.cancel_resting_immediately,
            "resting_hold_seconds": args.resting_hold_seconds,
            "max_live_submissions_per_day": args.max_live_submissions_per_day,
            "max_live_cost_per_day_dollars": args.max_live_cost_per_day_dollars,
            "enforce_trade_gate": args.enforce_trade_gate,
            "enforce_ws_state_authority": args.enforce_ws_state_authority,
            "ws_state_max_age_seconds": args.ws_state_max_age_seconds,
            "max_total_deployed_pct": args.max_total_deployed_pct,
            "max_same_station_exposure_pct": args.max_same_station_exposure_pct,
            "max_same_hour_cluster_exposure_pct": args.max_same_hour_cluster_exposure_pct,
            "max_same_underlying_exposure_pct": args.max_same_underlying_exposure_pct,
            "max_orders_per_station": args.max_orders_per_station,
            "max_orders_per_underlying": args.max_orders_per_underlying,
            "min_unique_stations_per_loop": args.min_unique_stations_per_loop,
            "min_unique_underlyings_per_loop": args.min_unique_underlyings_per_loop,
            "min_unique_local_hours_per_loop": args.min_unique_local_hours_per_loop,
            "replan_market_side_cooldown_minutes": args.replan_market_side_cooldown_minutes,
            "replan_market_side_price_change_override_dollars": args.replan_market_side_price_change_override_dollars,
            "replan_market_side_alpha_change_override": args.replan_market_side_alpha_change_override,
            "replan_market_side_confidence_change_override": args.replan_market_side_confidence_change_override,
            "replan_market_side_min_observation_advance_minutes": args.replan_market_side_min_observation_advance_minutes,
            "replan_market_side_repeat_window_minutes": args.replan_market_side_repeat_window_minutes,
            "replan_market_side_max_plans_per_window": args.replan_market_side_max_plans_per_window,
            "replan_market_side_history_files": args.replan_market_side_history_files,
            "replan_market_side_min_orders_backstop": args.replan_market_side_min_orders_backstop,
            "historical_selection_quality_enabled": not args.disable_historical_selection_quality,
            "historical_selection_quality_lookback_hours": args.historical_selection_quality_lookback_hours,
            "historical_selection_quality_min_resolved_market_sides": args.historical_selection_quality_min_resolved_market_sides,
            "historical_selection_quality_min_bucket_samples": args.historical_selection_quality_min_bucket_samples,
            "historical_selection_quality_probability_penalty_max": args.historical_selection_quality_probability_penalty_max,
            "historical_selection_quality_expected_edge_penalty_max": args.historical_selection_quality_expected_edge_penalty_max,
            "historical_selection_quality_score_adjust_scale": args.historical_selection_quality_score_adjust_scale,
            "historical_selection_quality_profile_max_age_hours": args.historical_selection_quality_profile_max_age_hours,
            "historical_selection_quality_preferred_model": args.historical_selection_quality_preferred_model,
        }
        if args.weather_pattern_hardening_enabled is not None:
            trader_kwargs["weather_pattern_hardening_enabled"] = bool(args.weather_pattern_hardening_enabled)
        summary, ignored_trader_kwargs = _invoke_runner_with_supported_kwargs(
            run_kalshi_temperature_shadow_watch,
            trader_kwargs,
        )
        if ignored_trader_kwargs:
            summary["runner_ignored_cli_kwargs"] = sorted(ignored_trader_kwargs)
        if micro_live_profile_caps is not None:
            summary["risk_profile"] = "micro_live_50"
            summary["risk_profile_applied"] = True
            summary["risk_profile_caps"] = micro_live_profile_caps
    elif args.command == "kalshi-temperature-profitability":
        summary = run_kalshi_temperature_profitability(
            output_dir=args.output_dir,
            hours=args.hours,
            journal_db_path=args.journal_db_path,
            top_n=args.top_n,
        )
    elif args.command in {"kalshi-temperature-recovery-advisor", "temperature-recovery-advisor"}:
        summary = run_kalshi_temperature_recovery_advisor(
            output_dir=args.output_dir,
            weather_window_hours=args.weather_window_hours,
            weather_min_bucket_samples=args.weather_min_bucket_samples,
            weather_max_profile_age_hours=args.weather_max_profile_age_hours,
            weather_negative_expectancy_attempt_share_target=args.weather_negative_expectancy_attempt_share_target,
            weather_stale_metar_negative_attempt_share_target=args.weather_stale_metar_negative_attempt_share_target,
            weather_stale_metar_attempt_share_target=args.weather_stale_metar_attempt_share_target,
            weather_min_attempts_target=args.weather_min_attempts_target,
            optimizer_top_n=args.optimizer_top_n,
        )
    elif args.command in {"kalshi-temperature-recovery-loop", "temperature-recovery-loop"}:
        recovery_loop_kwargs = {
            "output_dir": args.output_dir,
            "trader_env_file": args.trader_env_file,
            "max_iterations": args.max_iterations,
            "stall_iterations": args.stall_iterations,
            "min_gap_improvement": args.min_gap_improvement,
            "weather_window_hours": args.weather_window_hours,
            "weather_min_bucket_samples": args.weather_min_bucket_samples,
            "weather_max_profile_age_hours": args.weather_max_profile_age_hours,
            "weather_negative_expectancy_attempt_share_target": args.weather_negative_expectancy_attempt_share_target,
            "weather_stale_metar_negative_attempt_share_target": args.weather_stale_metar_negative_attempt_share_target,
            "weather_stale_metar_attempt_share_target": args.weather_stale_metar_attempt_share_target,
            "weather_min_attempts_target": args.weather_min_attempts_target,
            "optimizer_top_n": args.optimizer_top_n,
            "plateau_negative_regime_suppression_enabled": args.plateau_negative_regime_suppression_enabled,
            "plateau_negative_regime_suppression_min_bucket_samples": (
                args.plateau_negative_regime_suppression_min_bucket_samples
            ),
            "plateau_negative_regime_suppression_expectancy_threshold": (
                args.plateau_negative_regime_suppression_expectancy_threshold
            ),
            "plateau_negative_regime_suppression_top_n": args.plateau_negative_regime_suppression_top_n,
            "retune_weather_window_hours_cap": args.retune_weather_window_hours_cap,
            "retune_overblocking_blocked_share_threshold": args.retune_overblocking_blocked_share_threshold,
            "retune_underblocking_min_top_n": args.retune_underblocking_min_top_n,
            "retune_overblocking_max_top_n": args.retune_overblocking_max_top_n,
            "retune_min_bucket_samples_target": args.retune_min_bucket_samples_target,
            "retune_expectancy_threshold_target": args.retune_expectancy_threshold_target,
            "execute_actions": args.execute_actions,
        }
        summary, ignored_recovery_loop_kwargs = _invoke_runner_with_supported_kwargs(
            run_kalshi_temperature_recovery_loop,
            recovery_loop_kwargs,
        )
        if ignored_recovery_loop_kwargs:
            summary["runner_ignored_cli_kwargs"] = sorted(ignored_recovery_loop_kwargs)
    elif args.command in {"kalshi-temperature-recovery-campaign", "temperature-recovery-campaign"}:
        advisor_targets = {
            "weather_window_hours": args.weather_window_hours,
            "weather_min_bucket_samples": args.weather_min_bucket_samples,
            "weather_max_profile_age_hours": args.weather_max_profile_age_hours,
            "weather_negative_expectancy_attempt_share_target": args.weather_negative_expectancy_attempt_share_target,
            "weather_stale_metar_negative_attempt_share_target": args.weather_stale_metar_negative_attempt_share_target,
            "weather_stale_metar_attempt_share_target": args.weather_stale_metar_attempt_share_target,
            "weather_min_attempts_target": args.weather_min_attempts_target,
            "optimizer_top_n": args.optimizer_top_n,
            "plateau_negative_regime_suppression_enabled": args.plateau_negative_regime_suppression_enabled,
            "plateau_negative_regime_suppression_min_bucket_samples": (
                args.plateau_negative_regime_suppression_min_bucket_samples
            ),
            "plateau_negative_regime_suppression_expectancy_threshold": (
                args.plateau_negative_regime_suppression_expectancy_threshold
            ),
            "plateau_negative_regime_suppression_top_n": args.plateau_negative_regime_suppression_top_n,
            "retune_weather_window_hours_cap": args.retune_weather_window_hours_cap,
            "retune_overblocking_blocked_share_threshold": args.retune_overblocking_blocked_share_threshold,
            "retune_underblocking_min_top_n": args.retune_underblocking_min_top_n,
            "retune_overblocking_max_top_n": args.retune_overblocking_max_top_n,
            "retune_min_bucket_samples_target": args.retune_min_bucket_samples_target,
            "retune_expectancy_threshold_target": args.retune_expectancy_threshold_target,
        }
        profiles: list[dict[str, Any]] | None = None
        if args.profiles_json:
            profiles_payload: Any = _load_json_dict(args.profiles_json)
            if profiles_payload is None:
                try:
                    profiles_payload = json.loads(Path(args.profiles_json).read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    profiles_payload = None
            if isinstance(profiles_payload, dict):
                raw_profiles = profiles_payload.get("profiles")
            elif isinstance(profiles_payload, list):
                raw_profiles = profiles_payload
            else:
                raw_profiles = None
            if isinstance(raw_profiles, list):
                profiles = [item for item in raw_profiles if isinstance(item, dict)]
        summary = run_kalshi_temperature_recovery_campaign(
            output_dir=args.output_dir,
            trader_env_file=args.trader_env_file,
            execute_actions=args.execute_actions,
            profiles=profiles,
            advisor_targets=advisor_targets,
        )
    elif args.command == "kalshi-temperature-selection-quality":
        summary = run_kalshi_temperature_selection_quality(
            output_dir=args.output_dir,
            lookback_hours=args.lookback_hours,
            min_resolved_market_sides=args.min_resolved_market_sides,
            min_bucket_samples=args.min_bucket_samples,
            preferred_attribution_model=args.preferred_attribution_model,
            max_profile_age_hours=args.max_profile_age_hours,
            probability_penalty_max=args.probability_penalty_max,
            expected_edge_penalty_max=args.expected_edge_penalty_max,
            score_adjust_scale=args.score_adjust_scale,
            intent_hours=args.intent_hours,
            top_n=args.top_n,
        )
    elif args.command in {"kalshi-temperature-growth-optimizer", "temperature-growth-optimizer"}:
        summary = run_temperature_growth_optimizer(
            output_dir=args.output_dir,
            intent_files=args.intent_files,
            lookback_hours_min=args.lookback_hours_min,
            lookback_hours_max=args.lookback_hours_max,
            lookback_hours_step=args.lookback_hours_step,
            intent_hours_min=args.intent_hours_min,
            intent_hours_max=args.intent_hours_max,
            intent_hours_step=args.intent_hours_step,
            min_resolved_market_sides_min=args.min_resolved_market_sides_min,
            min_resolved_market_sides_max=args.min_resolved_market_sides_max,
            min_bucket_samples_min=args.min_bucket_samples_min,
            min_bucket_samples_max=args.min_bucket_samples_max,
            probability_penalty_max_min=args.probability_penalty_max_min,
            probability_penalty_max_max=args.probability_penalty_max_max,
            expected_edge_penalty_max_min=args.expected_edge_penalty_max_min,
            expected_edge_penalty_max_max=args.expected_edge_penalty_max_max,
            score_adjust_scale_min=args.score_adjust_scale_min,
            score_adjust_scale_max=args.score_adjust_scale_max,
            score_adjust_scale_step=args.score_adjust_scale_step,
            preferred_attribution_model=args.preferred_attribution_model,
            top_n=args.top_n,
            search_bounds_json=args.search_bounds_json,
        )
    elif args.command == "kalshi-temperature-execution-cost-tape":
        summary = run_kalshi_temperature_execution_cost_tape(
            output_dir=args.output_dir,
            window_hours=args.window_hours,
            min_candidate_samples=args.min_candidate_samples,
            min_quote_coverage_ratio=args.min_quote_coverage_ratio,
            journal_db_path=args.journal_db_path,
            max_tickers=args.max_tickers,
            min_global_expected_edge_share_for_exclusion=args.min_global_expected_edge_share_for_exclusion,
            min_ticker_rows_for_exclusion=args.min_ticker_rows_for_exclusion,
            exclusion_max_quote_coverage_ratio=args.exclusion_max_quote_coverage_ratio,
            max_ticker_mean_spread_for_exclusion=args.max_ticker_mean_spread_for_exclusion,
            max_excluded_tickers=args.max_excluded_tickers,
        )
    elif args.command == "kalshi-temperature-refill-trial-balance":
        summary = run_kalshi_temperature_refill_trial_balance(
            output_dir=args.output_dir,
            starting_balance_dollars=args.starting_balance_dollars,
            reason=args.reason,
        )
    elif args.command == "kalshi-temperature-bankroll-validation":
        summary = run_kalshi_temperature_bankroll_validation(
            output_dir=args.output_dir,
            hours=args.hours,
            reference_bankroll_dollars=args.reference_bankroll_dollars,
            sizing_models_json=args.sizing_models_json,
            slippage_bps_list=args.slippage_bps_list,
            fee_model_json=args.fee_model_json,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-temperature-alpha-gap-report":
        summary = run_kalshi_temperature_alpha_gap_report(
            output_dir=args.output_dir,
            hours=args.hours,
            reference_bankroll_dollars=args.reference_bankroll_dollars,
            sizing_models_json=args.sizing_models_json,
            slippage_bps_list=args.slippage_bps_list,
            fee_model_json=args.fee_model_json,
            top_n=args.top_n,
            source_bankroll_validation_file=args.source_bankroll_validation_file,
        )
    elif args.command == "kalshi-temperature-live-readiness":
        summary = run_kalshi_temperature_live_readiness(
            output_dir=args.output_dir,
            horizons=args.horizons,
            reference_bankroll_dollars=args.reference_bankroll_dollars,
            sizing_models_json=args.sizing_models_json,
            slippage_bps_list=args.slippage_bps_list,
            fee_model_json=args.fee_model_json,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-temperature-go-live-gate":
        summary = run_kalshi_temperature_go_live_gate(
            output_dir=args.output_dir,
            horizons=args.horizons,
            reference_bankroll_dollars=args.reference_bankroll_dollars,
            sizing_models_json=args.sizing_models_json,
            slippage_bps_list=args.slippage_bps_list,
            fee_model_json=args.fee_model_json,
            top_n=args.top_n,
            source_live_readiness_file=args.source_live_readiness_file,
        )
    elif args.command == "polymarket-market-ingest":
        summary = run_polymarket_market_data_ingest(
            output_dir=args.output_dir,
            max_markets=args.max_markets,
            page_size=args.page_size,
            max_pages=args.max_pages,
            only_active=not args.include_inactive,
            gamma_base_url=args.gamma_base_url,
            timeout_seconds=args.timeout_seconds,
            coldmath_snapshot_dir=args.coldmath_snapshot_dir,
            coldmath_equity_csv=args.coldmath_equity_csv,
            coldmath_positions_csv=args.coldmath_positions_csv,
            coldmath_wallet_address=args.coldmath_wallet_address,
            coldmath_stale_hours=args.coldmath_stale_hours,
            coldmath_refresh_from_api=args.coldmath_refresh_from_api,
            coldmath_data_api_base_url=args.coldmath_data_api_base_url,
            coldmath_api_timeout_seconds=args.coldmath_api_timeout_seconds,
            coldmath_positions_page_size=args.coldmath_positions_page_size,
            coldmath_positions_max_pages=args.coldmath_positions_max_pages,
            coldmath_refresh_closed_positions_from_api=not args.coldmath_disable_closed_positions_refresh,
            coldmath_closed_positions_page_size=args.coldmath_closed_positions_page_size,
            coldmath_closed_positions_max_pages=args.coldmath_closed_positions_max_pages,
            coldmath_refresh_trades_from_api=not args.coldmath_disable_trades_refresh,
            coldmath_refresh_activity_from_api=not args.coldmath_disable_activity_refresh,
            coldmath_include_taker_only_trades=not args.coldmath_disable_taker_only_trades,
            coldmath_include_all_trade_roles=not args.coldmath_disable_all_trade_roles,
            coldmath_trades_page_size=args.coldmath_trades_page_size,
            coldmath_trades_max_pages=args.coldmath_trades_max_pages,
            coldmath_activity_page_size=args.coldmath_activity_page_size,
            coldmath_activity_max_pages=args.coldmath_activity_max_pages,
        )
    elif args.command == "coldmath-snapshot-summary":
        summary = run_coldmath_snapshot_summary(
            snapshot_dir=args.snapshot_dir,
            equity_csv=args.equity_csv,
            positions_csv=args.positions_csv,
            wallet_address=args.wallet_address,
            stale_hours=args.stale_hours,
            output_dir=args.output_dir,
            refresh_from_api=args.refresh_from_api,
            data_api_base_url=args.data_api_base_url,
            api_timeout_seconds=args.api_timeout_seconds,
            positions_page_size=args.positions_page_size,
            positions_max_pages=args.positions_max_pages,
            refresh_closed_positions_from_api=not args.disable_closed_positions_refresh,
            closed_positions_page_size=args.closed_positions_page_size,
            closed_positions_max_pages=args.closed_positions_max_pages,
            refresh_trades_from_api=not args.disable_trades_refresh,
            refresh_activity_from_api=not args.disable_activity_refresh,
            include_taker_only_trades=not args.disable_taker_only_trades,
            include_all_trade_roles=not args.disable_all_trade_roles,
            trades_page_size=args.trades_page_size,
            trades_max_pages=args.trades_max_pages,
            activity_page_size=args.activity_page_size,
            activity_max_pages=args.activity_max_pages,
        )
    elif args.command == "coldmath-replication-plan":
        summary = run_coldmath_replication_plan(
            output_dir=args.output_dir,
            top_n=args.top_n,
            market_tickers=[item.strip() for item in str(args.market_tickers or "").split(",") if item.strip()],
            excluded_market_tickers=[
                item.strip() for item in str(args.excluded_market_tickers or "").split(",") if item.strip()
            ],
            excluded_market_tickers_file=str(args.excluded_market_tickers_file or "").strip() or None,
            require_liquidity_filter=not args.disable_liquidity_filter,
            require_two_sided_quotes=not args.disable_require_two_sided_quotes,
            max_spread_dollars=args.max_spread_dollars,
            min_liquidity_score=args.min_liquidity_score,
            max_family_candidates=args.max_family_candidates,
            max_family_share=args.max_family_share,
        )
    elif args.command == "decision-matrix-hardening":
        summary = run_decision_matrix_hardening(
            output_dir=args.output_dir,
            window_hours=args.window_hours,
            min_settled_outcomes=args.min_settled_outcomes,
            max_top_blocker_share=args.max_top_blocker_share,
            min_approval_rate=args.min_approval_rate,
            min_intents_sample=args.min_intents_sample,
            max_sparse_edge_block_share=args.max_sparse_edge_block_share,
            min_execution_cost_candidate_samples=args.min_execution_cost_candidate_samples,
            min_execution_cost_quote_coverage_ratio=args.min_execution_cost_quote_coverage_ratio,
        )
    elif args.command == "kalshi-focus-dossier":
        summary = run_kalshi_focus_dossier(
            history_csv=args.history_csv,
            watch_history_csv=args.watch_history_csv,
            priors_csv=args.priors_csv,
            output_dir=args.output_dir,
            recent_observation_limit=args.recent_observation_limit,
        )
    elif args.command == "kalshi-micro-prior-plan":
        summary = run_kalshi_micro_prior_plan(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            selection_lane=args.selection_lane,
            min_selected_fair_probability=args.min_selected_fair_probability,
            max_entry_price_dollars=args.max_entry_price,
            canonical_mapping_csv=args.canonical_mapping_csv,
            canonical_threshold_csv=args.canonical_threshold_csv,
            prefer_canonical_thresholds=not args.disable_canonical_thresholds,
            require_canonical_mapping=args.require_canonical_mapping,
            top_n=args.top_n,
            book_db_path=args.book_db_path,
            include_incentives=args.include_incentives,
        )
    elif args.command == "kalshi-micro-prior-execute":
        climate_router_pilot_allowed_classes = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_allowed_classes or "").split(",")
            if value.strip()
        )
        climate_router_pilot_allowed_families = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_allowed_families or "").split(",")
            if value.strip()
        )
        climate_router_pilot_excluded_families = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_excluded_families or "").split(",")
            if value.strip()
        )
        summary = run_kalshi_micro_prior_execute(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            selection_lane=args.selection_lane,
            min_selected_fair_probability=args.min_selected_fair_probability,
            max_entry_price_dollars=args.max_entry_price,
            canonical_mapping_csv=args.canonical_mapping_csv,
            canonical_threshold_csv=args.canonical_threshold_csv,
            prefer_canonical_thresholds=not args.disable_canonical_thresholds,
            require_canonical_mapping_for_live=not args.disable_require_canonical_for_live,
            allow_live_orders=args.allow_live_orders,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            min_live_maker_edge_net_fees=args.min_live_maker_edge_net_fees,
            min_live_selected_fair_probability=args.min_live_selected_fair_probability,
            timeout_seconds=args.timeout_seconds,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            execution_event_log_csv=args.execution_event_log_csv,
            execution_journal_db_path=args.execution_journal_db_path,
            execution_frontier_recent_rows=args.execution_frontier_recent_rows,
            execution_frontier_report_json=args.execution_frontier_report_json,
            execution_frontier_max_report_age_seconds=args.execution_frontier_max_report_age_seconds,
            enforce_ws_state_authority=args.enforce_ws_state_authority,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            enforce_daily_weather_live_only=not args.disable_daily_weather_live_only,
            require_daily_weather_board_coverage_for_live=not args.disable_daily_weather_board_coverage,
            daily_weather_board_max_age_seconds=args.daily_weather_board_max_age_seconds,
            climate_router_pilot_enabled=args.climate_router_pilot_enabled,
            climate_router_summary_json=args.climate_router_summary_json,
            climate_router_pilot_max_orders_per_run=args.climate_router_pilot_max_orders_per_run,
            climate_router_pilot_contracts_cap=args.climate_router_pilot_contracts_cap,
            climate_router_pilot_required_ev_dollars=args.climate_router_pilot_required_ev_dollars,
            climate_router_pilot_allowed_classes=climate_router_pilot_allowed_classes,
            climate_router_pilot_allowed_families=climate_router_pilot_allowed_families,
            climate_router_pilot_excluded_families=climate_router_pilot_excluded_families,
            climate_router_pilot_policy_scope_override_enabled=(
                args.climate_router_pilot_policy_scope_override_enabled
            ),
            include_incentives=args.include_incentives,
        )
    elif args.command == "kalshi-micro-prior-trader":
        auto_weather_allowed_contract_families = tuple(
            value.strip()
            for value in str(args.auto_weather_allowed_contract_families or "").split(",")
            if value.strip()
        )
        climate_router_pilot_allowed_classes = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_allowed_classes or "").split(",")
            if value.strip()
        )
        climate_router_pilot_allowed_families = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_allowed_families or "").split(",")
            if value.strip()
        )
        climate_router_pilot_excluded_families = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_excluded_families or "").split(",")
            if value.strip()
        )
        summary = run_kalshi_micro_prior_trader(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            watch_history_csv=args.watch_history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            canonical_mapping_csv=args.canonical_mapping_csv,
            canonical_threshold_csv=args.canonical_threshold_csv,
            prefer_canonical_thresholds=not args.disable_canonical_thresholds,
            require_canonical_mapping_for_live=not args.disable_require_canonical_for_live,
            timeout_seconds=args.timeout_seconds,
            allow_live_orders=args.allow_live_orders,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            min_live_maker_edge=args.min_live_maker_edge,
            min_live_maker_edge_net_fees=args.min_live_maker_edge_net_fees,
            include_incentives=args.include_incentives,
            auto_refresh_priors=not args.disable_auto_refresh_priors,
            auto_prior_max_markets=args.auto_prior_max_markets,
            auto_prior_min_evidence_count=args.auto_prior_min_evidence_count,
            auto_prior_min_evidence_quality=args.auto_prior_min_evidence_quality,
            auto_prior_min_high_trust_sources=args.auto_prior_min_high_trust_sources,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            execution_event_log_csv=args.execution_event_log_csv,
            execution_journal_db_path=args.execution_journal_db_path,
            execution_frontier_recent_rows=args.execution_frontier_recent_rows,
            execution_frontier_report_json=args.execution_frontier_report_json,
            execution_frontier_max_report_age_seconds=args.execution_frontier_max_report_age_seconds,
            enforce_ws_state_authority=args.enforce_ws_state_authority,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            enforce_daily_weather_live_only=not args.disable_daily_weather_live_only,
            require_daily_weather_board_coverage_for_live=not args.disable_daily_weather_board_coverage,
            daily_weather_board_max_age_seconds=args.daily_weather_board_max_age_seconds,
            climate_router_pilot_enabled=args.climate_router_pilot_enabled,
            climate_router_summary_json=args.climate_router_summary_json,
            climate_router_pilot_max_orders_per_run=args.climate_router_pilot_max_orders_per_run,
            climate_router_pilot_contracts_cap=args.climate_router_pilot_contracts_cap,
            climate_router_pilot_required_ev_dollars=args.climate_router_pilot_required_ev_dollars,
            climate_router_pilot_allowed_classes=climate_router_pilot_allowed_classes,
            climate_router_pilot_allowed_families=climate_router_pilot_allowed_families,
            climate_router_pilot_excluded_families=climate_router_pilot_excluded_families,
            climate_router_pilot_policy_scope_override_enabled=(
                args.climate_router_pilot_policy_scope_override_enabled
            ),
            capture_before_execute=not args.skip_capture,
            capture_max_hours_to_close=args.max_hours_to_close,
            capture_page_limit=args.page_limit,
            capture_max_pages=args.max_pages,
            use_temporary_live_env=args.use_temp_live_env,
            auto_refresh_weather_priors=not args.disable_auto_refresh_weather_priors,
            auto_prewarm_weather_station_history=not args.disable_auto_prewarm_weather_station_history,
            auto_weather_prior_max_markets=args.auto_weather_prior_max_markets,
            auto_weather_allowed_contract_families=auto_weather_allowed_contract_families,
            auto_weather_prewarm_max_station_day_keys=args.auto_weather_prewarm_max_station_day_keys,
            auto_weather_historical_lookback_years=args.auto_weather_historical_lookback_years,
            auto_weather_station_history_cache_max_age_hours=args.auto_weather_station_history_cache_max_age_hours,
        )
    elif args.command == "kalshi-micro-prior-watch":
        summary = run_kalshi_micro_prior_watch(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            watch_history_csv=args.watch_history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            canonical_mapping_csv=args.canonical_mapping_csv,
            canonical_threshold_csv=args.canonical_threshold_csv,
            prefer_canonical_thresholds=not args.disable_canonical_thresholds,
            require_canonical_mapping_for_live=not args.disable_require_canonical_for_live,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            timeout_seconds=args.timeout_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            min_live_maker_edge=args.min_live_maker_edge,
            min_live_maker_edge_net_fees=args.min_live_maker_edge_net_fees,
            include_incentives=args.include_incentives,
            auto_refresh_priors=not args.disable_auto_refresh_priors,
            auto_prior_max_markets=args.auto_prior_max_markets,
            auto_prior_min_evidence_count=args.auto_prior_min_evidence_count,
            auto_prior_min_evidence_quality=args.auto_prior_min_evidence_quality,
            auto_prior_min_high_trust_sources=args.auto_prior_min_high_trust_sources,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
        )
    elif args.command == "kalshi-arb-scan":
        summary = run_kalshi_arb_scan(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            fee_buffer_per_contract_dollars=args.fee_buffer_per_contract,
            min_margin_dollars=args.min_margin_dollars,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-supervisor":
        summary = run_kalshi_supervisor(
            env_file=args.env_file,
            output_dir=args.output_dir,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            cycles=args.cycles,
            sleep_between_cycles_seconds=args.sleep_between_cycles_seconds,
            timeout_seconds=args.timeout_seconds,
            allow_live_orders=args.allow_live_orders,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            min_live_maker_edge=args.min_live_maker_edge,
            min_live_maker_edge_net_fees=args.min_live_maker_edge_net_fees,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            read_requests_per_minute=args.read_requests_per_minute,
            write_requests_per_minute=args.write_requests_per_minute,
            failure_remediation_enabled=not args.disable_failure_remediation,
            failure_remediation_max_retries=args.failure_remediation_max_retries,
            failure_remediation_backoff_seconds=args.failure_remediation_backoff_seconds,
            failure_remediation_timeout_multiplier=args.failure_remediation_timeout_multiplier,
            failure_remediation_timeout_cap_seconds=args.failure_remediation_timeout_cap_seconds,
            exchange_status_self_heal_attempts=args.exchange_status_self_heal_attempts,
            exchange_status_self_heal_pause_seconds=args.exchange_status_self_heal_pause_seconds,
            exchange_status_run_dns_doctor=not args.disable_exchange_status_dns_remediation,
            exchange_status_self_heal_timeout_multiplier=args.exchange_status_self_heal_timeout_multiplier,
            exchange_status_self_heal_timeout_cap_seconds=args.exchange_status_self_heal_timeout_cap_seconds,
            run_arb_scan_each_cycle=not args.disable_arb_scan,
            include_incentives=not args.disable_incentives,
            auto_refresh_priors=not args.disable_auto_refresh_priors,
            auto_prior_max_markets=args.auto_prior_max_markets,
            auto_prior_min_evidence_count=args.auto_prior_min_evidence_count,
            auto_prior_min_evidence_quality=args.auto_prior_min_evidence_quality,
            auto_prior_min_high_trust_sources=args.auto_prior_min_high_trust_sources,
            enforce_ws_state_authority=not args.disable_enforce_ws_state_authority,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
        )
    elif args.command == "kalshi-autopilot":
        summary = run_kalshi_autopilot(
            env_file=args.env_file,
            output_dir=args.output_dir,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            allow_live_orders=args.allow_live_orders,
            cycles=args.cycles,
            sleep_between_cycles_seconds=args.sleep_between_cycles_seconds,
            timeout_seconds=args.timeout_seconds,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            failure_remediation_timeout_multiplier=args.failure_remediation_timeout_multiplier,
            failure_remediation_timeout_cap_seconds=args.failure_remediation_timeout_cap_seconds,
            preflight_run_dns_doctor=not args.disable_preflight_dns_doctor,
            preflight_run_live_smoke=not args.disable_preflight_live_smoke,
            preflight_live_smoke_include_odds_provider_check=args.preflight_live_smoke_include_odds_provider,
            preflight_run_ws_state_collect=not args.disable_preflight_ws_state_collect,
            ws_collect_run_seconds=args.ws_collect_run_seconds,
            ws_collect_max_events=args.ws_collect_max_events,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            preflight_self_heal_attempts=args.preflight_self_heal_attempts,
            preflight_self_heal_pause_seconds=args.preflight_self_heal_pause_seconds,
            preflight_self_heal_upstream_only=not args.disable_preflight_self_heal_upstream_only,
            preflight_self_heal_retry_ws_state_gate_failures=not args.disable_preflight_self_heal_retry_ws_state_gate_failures,
            preflight_self_heal_run_dns_doctor=not args.disable_preflight_self_heal_dns_remediation,
            preflight_retry_timeout_multiplier=args.preflight_retry_timeout_multiplier,
            preflight_retry_timeout_cap_seconds=args.preflight_retry_timeout_cap_seconds,
            preflight_retry_ws_collect_increment_seconds=args.preflight_retry_ws_collect_increment_seconds,
            preflight_retry_ws_collect_max_seconds=args.preflight_retry_ws_collect_max_seconds,
            enable_progressive_scaling=not args.disable_progressive_scaling,
            scaling_lookback_runs=args.scaling_lookback_runs,
            scaling_green_runs_per_step=args.scaling_green_runs_per_step,
            scaling_step_live_submissions=args.scaling_step_live_submissions,
            scaling_step_live_cost_dollars=args.scaling_step_live_cost_dollars,
            scaling_step_daily_risk_cap_dollars=args.scaling_step_daily_risk_cap_dollars,
            scaling_hard_max_live_submissions_per_day=args.scaling_hard_max_live_submissions_per_day,
            scaling_hard_max_live_cost_per_day_dollars=args.scaling_hard_max_live_cost_per_day_dollars,
            scaling_hard_max_daily_risk_cap_dollars=args.scaling_hard_max_daily_risk_cap_dollars,
        )
    elif args.command == "kalshi-watchdog":
        summary = run_kalshi_watchdog(
            env_file=args.env_file,
            output_dir=args.output_dir,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            allow_live_orders=args.allow_live_orders,
            loops=args.loops,
            sleep_between_loops_seconds=args.sleep_between_loops_seconds,
            autopilot_cycles=args.autopilot_cycles,
            autopilot_sleep_between_cycles_seconds=args.autopilot_sleep_between_cycles_seconds,
            timeout_seconds=args.timeout_seconds,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            failure_remediation_timeout_multiplier=args.failure_remediation_timeout_multiplier,
            failure_remediation_timeout_cap_seconds=args.failure_remediation_timeout_cap_seconds,
            preflight_run_dns_doctor=not args.disable_preflight_dns_doctor,
            preflight_run_live_smoke=not args.disable_preflight_live_smoke,
            preflight_live_smoke_include_odds_provider_check=args.preflight_live_smoke_include_odds_provider,
            preflight_run_ws_state_collect=not args.disable_preflight_ws_state_collect,
            ws_collect_run_seconds=args.ws_collect_run_seconds,
            ws_collect_max_events=args.ws_collect_max_events,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            preflight_self_heal_attempts=args.preflight_self_heal_attempts,
            preflight_self_heal_pause_seconds=args.preflight_self_heal_pause_seconds,
            preflight_self_heal_upstream_only=not args.disable_preflight_self_heal_upstream_only,
            preflight_self_heal_retry_ws_state_gate_failures=not args.disable_preflight_self_heal_retry_ws_state_gate_failures,
            preflight_self_heal_run_dns_doctor=not args.disable_preflight_self_heal_dns_remediation,
            preflight_retry_timeout_multiplier=args.preflight_retry_timeout_multiplier,
            preflight_retry_timeout_cap_seconds=args.preflight_retry_timeout_cap_seconds,
            preflight_retry_ws_collect_increment_seconds=args.preflight_retry_ws_collect_increment_seconds,
            preflight_retry_ws_collect_max_seconds=args.preflight_retry_ws_collect_max_seconds,
            enable_progressive_scaling=not args.disable_progressive_scaling,
            scaling_lookback_runs=args.scaling_lookback_runs,
            scaling_green_runs_per_step=args.scaling_green_runs_per_step,
            scaling_step_live_submissions=args.scaling_step_live_submissions,
            scaling_step_live_cost_dollars=args.scaling_step_live_cost_dollars,
            scaling_step_daily_risk_cap_dollars=args.scaling_step_daily_risk_cap_dollars,
            scaling_hard_max_live_submissions_per_day=args.scaling_hard_max_live_submissions_per_day,
            scaling_hard_max_live_cost_per_day_dollars=args.scaling_hard_max_live_cost_per_day_dollars,
            scaling_hard_max_daily_risk_cap_dollars=args.scaling_hard_max_daily_risk_cap_dollars,
            upstream_incident_threshold=args.upstream_incident_threshold,
            kill_switch_cooldown_seconds=args.kill_switch_cooldown_seconds,
            healthy_runs_to_clear_kill_switch=args.healthy_runs_to_clear_kill_switch,
            upstream_retry_backoff_base_seconds=args.upstream_retry_backoff_base_seconds,
            upstream_retry_backoff_max_seconds=args.upstream_retry_backoff_max_seconds,
            self_heal_attempts_per_run=args.self_heal_attempts_per_loop,
            self_heal_pause_seconds=args.self_heal_pause_seconds,
            self_heal_retry_timeout_multiplier=args.self_heal_retry_timeout_multiplier,
            self_heal_retry_timeout_cap_seconds=args.self_heal_retry_timeout_cap_seconds,
            self_heal_retry_ws_collect_increment_seconds=args.self_heal_retry_ws_collect_increment_seconds,
            self_heal_retry_ws_collect_max_seconds=args.self_heal_retry_ws_collect_max_seconds,
            run_dns_doctor_on_upstream=not args.disable_remediation_dns_doctor,
            kill_switch_state_json=args.kill_switch_state_json,
        )
    elif args.command == "kalshi-micro-plan":
        summary = run_kalshi_micro_plan(
            env_file=args.env_file,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-micro-gate":
        summary = run_kalshi_micro_gate(
            env_file=args.env_file,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-micro-trader":
        summary = run_kalshi_micro_trader(
            env_file=args.env_file,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            allow_live_orders=args.allow_live_orders,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            watch_history_csv=args.watch_history_csv,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-micro-watch":
        summary = run_kalshi_micro_watch(
            env_file=args.env_file,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-micro-execute":
        summary = run_kalshi_micro_execute(
            env_file=args.env_file,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            timeout_seconds=args.timeout_seconds,
            allow_live_orders=args.allow_live_orders,
            resting_hold_seconds=args.resting_hold_seconds,
            cancel_resting_immediately=args.cancel_resting_immediately,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            execution_event_log_csv=args.execution_event_log_csv,
            execution_journal_db_path=args.execution_journal_db_path,
            execution_frontier_recent_rows=args.execution_frontier_recent_rows,
            history_csv=args.history_csv,
            enforce_trade_gate=args.enforce_trade_gate,
            enforce_ws_state_authority=args.enforce_ws_state_authority,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            order_group_auto_create=args.order_group_auto_create,
            order_group_contract_limit=args.order_group_contract_limit,
            order_group_fetch_after_run=not args.disable_order_group_fetch_after_run,
        )
    elif args.command == "kalshi-micro-reconcile":
        summary = run_kalshi_micro_reconcile(
            env_file=args.env_file,
            execute_summary_file=args.execute_summary_file,
            output_dir=args.output_dir,
            book_db_path=args.book_db_path,
            execution_journal_db_path=args.execution_journal_db_path,
            timeout_seconds=args.timeout_seconds,
            max_historical_pages=args.max_historical_pages,
        )
    elif args.command == "kalshi-execution-frontier":
        frontier_journal_db_path = args.journal_db_path
        if frontier_journal_db_path is None and args.event_log_csv:
            event_log_path = Path(args.event_log_csv)
            frontier_journal_db_path = (
                str(event_log_path.with_suffix(".sqlite3"))
                if event_log_path.suffix.lower() == ".csv"
                else str(event_log_path)
            )
        summary = run_kalshi_execution_frontier(
            output_dir=args.output_dir,
            journal_db_path=frontier_journal_db_path,
            recent_events=args.recent_rows,
        )
    elif args.command == "kalshi-ws-state-replay":
        summary = run_kalshi_ws_state_replay(
            events_ndjson=args.events_ndjson,
            output_dir=args.output_dir,
            ws_state_json=args.ws_state_json,
            max_staleness_seconds=args.ws_state_max_age_seconds,
        )
    elif args.command == "kalshi-ws-state-collect":
        summary = run_kalshi_ws_state_collect(
            env_file=args.env_file,
            channels=tuple(item.strip() for item in args.channels.split(",") if item.strip()),
            market_tickers=tuple(item.strip() for item in args.market_tickers.split(",") if item.strip()),
            output_dir=args.output_dir,
            ws_events_ndjson=args.ws_events_ndjson,
            ws_state_json=args.ws_state_json,
            max_staleness_seconds=args.ws_state_max_age_seconds,
            run_seconds=args.run_seconds,
            max_events=args.max_events,
            connect_timeout_seconds=args.connect_timeout_seconds,
            read_timeout_seconds=args.read_timeout_seconds,
            ping_interval_seconds=args.ping_interval_seconds,
            flush_state_every_seconds=args.flush_state_every_seconds,
            reconnect_max_attempts=args.reconnect_max_attempts,
            reconnect_backoff_seconds=args.reconnect_backoff_seconds,
        )
    elif args.command == "kalshi-climate-realtime-router":
        summary = run_kalshi_climate_realtime_router(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            availability_db_path=args.availability_db_path,
            market_tickers=tuple(item.strip() for item in args.market_tickers.split(",") if item.strip()),
            ws_channels=tuple(item.strip() for item in args.ws_channels.split(",") if item.strip()),
            run_seconds=args.run_seconds,
            max_markets=args.max_markets,
            seed_recent_markets=args.seed_recent_markets,
            recent_markets_min_updated_seconds=args.recent_markets_min_updated_seconds,
            recent_markets_timeout_seconds=args.recent_markets_timeout_seconds,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            min_theoretical_edge_net_fees=args.min_theoretical_edge_net_fees,
            max_quote_age_seconds=args.max_quote_age_seconds,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            max_risk_per_bet_dollars=args.max_risk_per_bet,
            availability_lookback_days=args.availability_lookback_days,
            availability_recent_seconds=args.availability_recent_seconds,
            availability_hot_trade_window_seconds=args.availability_hot_trade_window_seconds,
            include_contract_families=tuple(
                item.strip() for item in args.include_contract_families.split(",") if item.strip()
            ),
            skip_realtime_collect=args.skip_realtime_collect,
        )
    elif args.command == "kalshi-micro-status":
        summary = run_kalshi_micro_status(
            env_file=args.env_file,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            watch_history_csv=args.watch_history_csv,
            history_csv=args.history_csv,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        raise ValueError(f"Unsupported command: {args.command}")

    print(json.dumps(summary, indent=2))
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc))
