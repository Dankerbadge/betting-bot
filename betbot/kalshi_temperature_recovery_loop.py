from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
from typing import Any

from betbot.decision_matrix_hardening import run_decision_matrix_hardening
from betbot.kalshi_temperature_constraints import run_kalshi_temperature_constraint_scan
from betbot.kalshi_temperature_contract_specs import run_kalshi_temperature_contract_specs
from betbot.kalshi_temperature_execution_cost_tape import run_kalshi_temperature_execution_cost_tape
from betbot.kalshi_temperature_growth_optimizer import run_kalshi_temperature_growth_optimizer
from betbot.kalshi_temperature_metar_ingest import run_kalshi_temperature_metar_ingest
from betbot.kalshi_temperature_profitability import run_kalshi_temperature_profitability
from betbot.kalshi_temperature_recovery_advisor import run_kalshi_temperature_recovery_advisor
from betbot.kalshi_temperature_settlement_state import run_kalshi_temperature_settlement_state
from betbot.kalshi_temperature_settled_outcome_throughput import run_kalshi_temperature_settled_outcome_throughput
from betbot.kalshi_temperature_trader import run_kalshi_temperature_trader
from betbot.kalshi_temperature_weather_pattern import run_kalshi_temperature_weather_pattern
from betbot.kalshi_ws_state import run_kalshi_ws_state_collect

DEFAULT_TRADER_ENV_FILE = "data/research/account_onboarding.env.template"
NEGATIVE_SHARE_WORSENING_EPSILON = 1e-6
NEGATIVE_SHARE_WORSENING_STREAK_THRESHOLD = 2
EFFECTIVENESS_DEMOTION_MIN_EXECUTIONS = 3
EFFECTIVENESS_DEMOTION_MIN_WORSENING_RATIO = 0.8
EFFECTIVENESS_DEMOTION_MIN_AVERAGE_DELTA = 0.0
REPLACEMENT_ROUTE_ARBITRATION_SCORE_EPSILON = 1e-6
REPLACEMENT_ROUTE_UNKNOWN_WORSENING_RATIO = 0.5
REPLACEMENT_ROUTE_AVERAGE_DELTA_SCALE = 50.0
REPLACEMENT_ROUTE_WEIGHT_WORSENING_RATIO = 0.55
REPLACEMENT_ROUTE_WEIGHT_AVERAGE_DELTA = 0.35
REPLACEMENT_ROUTE_WEIGHT_EXECUTION_UNCERTAINTY = 0.10
CORE_COVERAGE_ACTION_COOLDOWN_ITERATIONS = 1
ALPHA_LEVER_ACTION_COOLDOWN_ITERATIONS = 2
RETUNE_SEVERE_OVERBLOCKING_ABSOLUTE_THRESHOLD = 0.75
RETUNE_SEVERE_OVERBLOCKING_RELATIVE_BONUS = 0.35
TERTIARY_RESERVE_PROTECTION_MAX_ITERATIONS = 6
FINAL_FALLBACK_RESERVE_PROTECTION_MAX_ITERATIONS = 6
DEFENSIVE_PIVOT_ACTION_KEY = "auto_defensive_pivot_shadow_profile"
REPLACEMENT_INCREASE_WEATHER_SAMPLE_COVERAGE_ACTION_KEY = (
    "replacement_increase_weather_sample_coverage_strict_shadow"
)
REPLACEMENT_SETTLED_OUTCOME_COVERAGE_ACTION_KEY = "replacement_settled_outcome_coverage_strict_shadow"
REPLACEMENT_REDUCE_NEGATIVE_EXPECTANCY_ACTION_KEY = (
    "replacement_reduce_negative_expectancy_regimes_strict_shadow"
)
REPLACEMENT_PLATEAU_BREAK_NEGATIVE_EXPECTANCY_ACTION_KEY = (
    "replacement_plateau_break_negative_expectancy_share_strict_shadow"
)

CORE_COVERAGE_ACTION_KEYS = {
    "bootstrap_shadow_trade_intents",
    "repair_metar_ingest_quality_pipeline",
    "repair_taf_station_mapping_pipeline",
    "rebalance_weather_pattern_hard_block_pressure",
    "increase_settled_outcome_coverage",
    "recover_settled_outcome_velocity",
    "increase_weather_sample_coverage",
    "refresh_market_horizon_inputs",
    "reduce_stale_metar_pressure",
    "reduce_stale_station_concentration",
    "repair_execution_telemetry_pipeline",
    "improve_execution_quote_coverage_shadow",
    "reduce_execution_friction_pressure",
}

ALPHA_LEVER_ACTION_KEYS = {
    "reduce_negative_expectancy_regimes",
    "plateau_break_negative_expectancy_share",
}

TRADER_SHADOW_EFFECT_ACTION_KEYS = {
    "bootstrap_shadow_trade_intents",
    "repair_metar_ingest_quality_pipeline",
    "repair_taf_station_mapping_pipeline",
    "rebalance_weather_pattern_hard_block_pressure",
    "reduce_negative_expectancy_regimes",
    "reduce_stale_station_concentration",
    "repair_execution_telemetry_pipeline",
    "improve_execution_quote_coverage_shadow",
    "reduce_execution_friction_pressure",
    "probe_expected_edge_floor_with_hardening_disabled",
    "apply_expected_edge_relief_shadow_profile",
    DEFENSIVE_PIVOT_ACTION_KEY,
    REPLACEMENT_REDUCE_NEGATIVE_EXPECTANCY_ACTION_KEY,
    REPLACEMENT_PLATEAU_BREAK_NEGATIVE_EXPECTANCY_ACTION_KEY,
    "plateau_break_negative_expectancy_share",
    "retune_negative_regime_suppression",
    "increase_settled_outcome_coverage",
    "recover_settled_outcome_velocity",
    REPLACEMENT_SETTLED_OUTCOME_COVERAGE_ACTION_KEY,
    REPLACEMENT_INCREASE_WEATHER_SAMPLE_COVERAGE_ACTION_KEY,
}

TRADER_PAYLOAD_HARDENING_FLAG_KEYS = (
    "intents_only",
    "allow_live_orders",
    "weather_pattern_hardening_enabled",
    "weather_pattern_risk_off_enabled",
    "weather_pattern_negative_regime_suppression_enabled",
    "historical_selection_quality_enabled",
    "metar_ingest_quality_gate_enabled",
    "enforce_probability_edge_thresholds",
    "enforce_interval_consistency",
    "require_market_snapshot_seq",
    "require_market_snapshot_seq_applied",
)

TELEMETRY_HARDENING_PRESSURE_POLICY_REASONS = {
    "weather_pattern_multi_bucket_hard_block",
    "weather_pattern_negative_regime_bucket_suppressed",
    "historical_quality_signal_type_hard_block",
    "historical_quality_station_hour_hard_block",
    "historical_expectancy_hard_block",
    "historical_quality_global_only_pressure",
    "metar_ingest_quality_insufficient",
    "probability_confidence_below_min",
    "expected_edge_below_min",
    "edge_to_risk_ratio_below_min",
    "base_edge_below_min",
    "probability_breakeven_gap_below_min",
    "high_price_expected_edge_nonpositive",
    "high_price_edge_to_risk_ratio_below_min",
}

RISKY_EDGE_RELAX_ACTION_KEYS = {
    "probe_expected_edge_floor_with_hardening_disabled",
    "apply_expected_edge_relief_shadow_profile",
}

SAFE_HARD_DISABLE_REPLACEABLE_ACTION_KEYS = {
    "clear_weather_risk_off_state",
    "resolve_decision_matrix_weather_blockers",
    "retune_negative_regime_suppression",
}

REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY = {
    "increase_weather_sample_coverage": REPLACEMENT_INCREASE_WEATHER_SAMPLE_COVERAGE_ACTION_KEY,
    "increase_settled_outcome_coverage": REPLACEMENT_SETTLED_OUTCOME_COVERAGE_ACTION_KEY,
    "recover_settled_outcome_velocity": REPLACEMENT_SETTLED_OUTCOME_COVERAGE_ACTION_KEY,
    "reduce_negative_expectancy_regimes": REPLACEMENT_REDUCE_NEGATIVE_EXPECTANCY_ACTION_KEY,
    "plateau_break_negative_expectancy_share": REPLACEMENT_PLATEAU_BREAK_NEGATIVE_EXPECTANCY_ACTION_KEY,
}

TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY = {
    "increase_weather_sample_coverage": "clear_weather_risk_off_state",
    "increase_settled_outcome_coverage": "resolve_decision_matrix_weather_blockers",
    "recover_settled_outcome_velocity": "resolve_decision_matrix_weather_blockers",
    "reduce_negative_expectancy_regimes": "retune_negative_regime_suppression",
    "plateau_break_negative_expectancy_share": "retune_negative_regime_suppression",
    "clear_weather_risk_off_state": REPLACEMENT_REDUCE_NEGATIVE_EXPECTANCY_ACTION_KEY,
    "resolve_decision_matrix_weather_blockers": REPLACEMENT_INCREASE_WEATHER_SAMPLE_COVERAGE_ACTION_KEY,
    "retune_negative_regime_suppression": REPLACEMENT_PLATEAU_BREAK_NEGATIVE_EXPECTANCY_ACTION_KEY,
}

TERTIARY_REPLACEMENT_ACTION_BY_POLICY_CLASS = {
    "core_coverage": "resolve_decision_matrix_weather_blockers",
    "alpha_lever": "retune_negative_regime_suppression",
}

FINAL_FALLBACK_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY = {
    "increase_weather_sample_coverage": "reduce_stale_metar_pressure",
    "increase_settled_outcome_coverage": "refresh_market_horizon_inputs",
    "recover_settled_outcome_velocity": "refresh_market_horizon_inputs",
    "reduce_negative_expectancy_regimes": "reduce_stale_metar_pressure",
    "plateau_break_negative_expectancy_share": "refresh_market_horizon_inputs",
    "clear_weather_risk_off_state": "refresh_market_horizon_inputs",
    "resolve_decision_matrix_weather_blockers": "reduce_stale_metar_pressure",
    "retune_negative_regime_suppression": "refresh_market_horizon_inputs",
    "reduce_stale_metar_pressure": "refresh_market_horizon_inputs",
    "refresh_market_horizon_inputs": "reduce_stale_metar_pressure",
}

FINAL_FALLBACK_REPLACEMENT_ACTION_BY_POLICY_CLASS = {
    "core_coverage": "reduce_stale_metar_pressure",
    "alpha_lever": "refresh_market_horizon_inputs",
    "hard_disable": "reduce_stale_metar_pressure",
}

DEFAULT_FINAL_FALLBACK_REPLACEMENT_ACTION_KEY = "reduce_stale_metar_pressure"
RECOVERY_RISK_OFF_STALE_SHARE_THRESHOLD_MIN = 0.45
RESERVE_RELEASE_OVERRIDE_EPSILON = 1e-6
EXECUTION_COST_EXCLUSIONS_STATE_FILENAME = "execution_cost_exclusions_state_latest.json"
EXECUTION_COST_EXCLUSIONS_STATE_MAX_ACTIVE = 20
EXECUTION_COST_EXCLUSIONS_STATE_MAX_ACTIVE_MARKET_SIDE_TARGETS = 64
EXECUTION_COST_EXCLUSIONS_STATE_ACTIVATION_RUNS = 2
EXECUTION_COST_EXCLUSIONS_STATE_DEACTIVATION_MISSING_RUNS = 3
EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_NEAR_CAP_RATIO = 0.80
EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_MIN_ACTIVE = 8
EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_DROP_FRACTION = 0.25
EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_HOLD_RUNS = 2
EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_COOLDOWN_RUNS = 2
EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_COVERAGE_EPSILON = 0.002
EXECUTION_COST_SIDE_TARGET_ACCELERATOR_DOMINANT_SHARE_THRESHOLD = 0.72
EXECUTION_COST_SIDE_TARGET_ACCELERATOR_IMBALANCE_THRESHOLD = 0.60
EXECUTION_COST_SIDE_TARGET_ACCELERATOR_PRESSURE_SCORE_THRESHOLD = 0.55
EXECUTION_COST_SIDE_TARGET_ACCELERATOR_COVERAGE_GAP_THRESHOLD = 0.10
EXECUTION_COST_SIDE_TARGET_ACCELERATOR_TREND_PRESSURE_DELTA_THRESHOLD = 0.01
EXECUTION_COST_SIDE_TARGET_ACCELERATOR_TREND_TOP5_SHARE_DELTA_THRESHOLD = 0.01
EXECUTION_COST_SIDE_TARGET_ACCELERATOR_TREND_CANDIDATE_ROWS_DELTA_THRESHOLD = 24
EXECUTION_COST_SIDE_TARGET_ACCELERATOR_MAX_TARGETS = 6
QUOTE_COVERAGE_SIDE_PRESSURE_SCORE_THRESHOLD = 0.70
QUOTE_COVERAGE_SIDE_PRESSURE_DOMINANT_SHARE_THRESHOLD = 0.65
QUOTE_COVERAGE_SIDE_PRESSURE_MIN_DOMINANT_ROWS = 6
QUOTE_COVERAGE_SIDE_BIAS_MIN_FALLBACK_TARGETS = 1

REPLACEMENT_PROFILE_CONFIG = {
    "stabilize": {
        "weather_window_hours_floor": 336.0,
        "weather_profile_max_age_hours": 48.0,
        "weather_max_profile_age_cap": 192.0,
        "weather_min_bucket_samples_floor": 12,
        "weather_negative_expectancy_threshold": -0.025,
        "risk_off_concentration_threshold": 0.60,
        "risk_off_stale_metar_share_threshold": 0.33,
        "suppression_min_bucket_samples_floor": 12,
        "suppression_expectancy_threshold_floor": -0.032,
        "suppression_top_n_floor": 11,
        "min_probability_confidence": 0.84,
        "min_expected_edge_net": 0.02,
        "min_edge_to_risk_ratio": 0.10,
    },
    "balanced": {
        "weather_window_hours_floor": 540.0,
        "weather_profile_max_age_hours": 30.0,
        "weather_max_profile_age_cap": 180.0,
        "weather_min_bucket_samples_floor": 13,
        "weather_negative_expectancy_threshold": -0.022,
        "risk_off_concentration_threshold": 0.58,
        "risk_off_stale_metar_share_threshold": 0.32,
        "suppression_min_bucket_samples_floor": 13,
        "suppression_expectancy_threshold_floor": -0.033,
        "suppression_top_n_floor": 10,
        "min_probability_confidence": 0.86,
        "min_expected_edge_net": 0.025,
        "min_edge_to_risk_ratio": 0.11,
    },
    "strict": {
        "weather_window_hours_floor": 720.0,
        "weather_profile_max_age_hours": 24.0,
        "weather_max_profile_age_cap": 168.0,
        "weather_min_bucket_samples_floor": 14,
        "weather_negative_expectancy_threshold": -0.02,
        "risk_off_concentration_threshold": 0.55,
        "risk_off_stale_metar_share_threshold": 0.30,
        "suppression_min_bucket_samples_floor": 14,
        "suppression_expectancy_threshold_floor": -0.035,
        "suppression_top_n_floor": 10,
        "min_probability_confidence": 0.88,
        "min_expected_edge_net": 0.03,
        "min_edge_to_risk_ratio": 0.12,
    },
}

DEFAULT_REPLACEMENT_PROFILE_KEY = "strict"


def _policy_class_and_cooldown(action_key: str) -> tuple[str, int | None]:
    if action_key in CORE_COVERAGE_ACTION_KEYS:
        return ("core_coverage", CORE_COVERAGE_ACTION_COOLDOWN_ITERATIONS)
    if action_key in ALPHA_LEVER_ACTION_KEYS:
        return ("alpha_lever", ALPHA_LEVER_ACTION_COOLDOWN_ITERATIONS)
    if action_key in RISKY_EDGE_RELAX_ACTION_KEYS:
        return ("risky_edge_relax", None)
    return ("hard_disable", None)


def _is_source_action_replaceable(action_key: str, policy_class: str) -> bool:
    if action_key in RISKY_EDGE_RELAX_ACTION_KEYS:
        return False
    if policy_class in {"core_coverage", "alpha_lever"}:
        return True
    if policy_class == "hard_disable" and action_key in SAFE_HARD_DISABLE_REPLACEABLE_ACTION_KEYS:
        return True
    return False


def _replacement_profile_config_for_key(profile_key: str) -> tuple[str, dict[str, float | int]]:
    normalized = _text(profile_key).lower() or DEFAULT_REPLACEMENT_PROFILE_KEY
    if normalized not in REPLACEMENT_PROFILE_CONFIG:
        normalized = DEFAULT_REPLACEMENT_PROFILE_KEY
    profile = REPLACEMENT_PROFILE_CONFIG[normalized]
    return normalized, dict(profile)


def _compute_adaptive_replacement_profile(
    thresholds: dict[str, int | float],
) -> dict[str, int | float | str]:
    severity = max(
        0.0,
        min(
            1.0,
            float(_safe_float(thresholds.get("severity")) or 0.5),
        ),
    )
    worsening_velocity = max(0.0, float(_safe_float(thresholds.get("worsening_velocity")) or 0.0))
    if severity >= 0.9 or worsening_velocity >= 0.02:
        selected_profile = "strict"
        selection_reason = "high_severity_or_velocity"
    elif severity >= 0.7 or worsening_velocity >= 0.008:
        selected_profile = "balanced"
        selection_reason = "moderate_severity_or_velocity"
    else:
        selected_profile = "stabilize"
        selection_reason = "low_severity_and_velocity"
    return {
        "selected_profile": selected_profile,
        "selection_reason": selection_reason,
        "severity": round(float(severity), 6),
        "worsening_velocity": round(float(worsening_velocity), 6),
    }


def _compute_reserve_release_override(
    *,
    prior_gap_improvement: float | None,
    prior_negative_share_delta: float | None,
    min_gap_improvement: float,
) -> dict[str, Any]:
    reasons: list[str] = []
    gap_threshold = max(float(min_gap_improvement), float(RESERVE_RELEASE_OVERRIDE_EPSILON))
    if isinstance(prior_gap_improvement, float) and prior_gap_improvement > gap_threshold:
        reasons.append("gap_score_improved")
    if (
        isinstance(prior_negative_share_delta, float)
        and prior_negative_share_delta < (-1.0 * float(NEGATIVE_SHARE_WORSENING_EPSILON))
    ):
        reasons.append("negative_expectancy_attempt_share_improved")
    return {
        "active": bool(reasons),
        "reasons": list(reasons),
        "prior_gap_improvement": (
            round(float(prior_gap_improvement), 6) if isinstance(prior_gap_improvement, float) else None
        ),
        "prior_negative_expectancy_attempt_share_delta": (
            round(float(prior_negative_share_delta), 6)
            if isinstance(prior_negative_share_delta, float)
            else None
        ),
    }


def _increment_counter(counter: dict[str, int], key: str, *, amount: int = 1) -> None:
    normalized = _text(key)
    if not normalized:
        return
    counter[normalized] = max(0, int(counter.get(normalized, 0))) + max(0, int(amount))


def _collect_tertiary_reserve_candidates(
    *,
    demoted_actions: set[str],
    auto_disabled_actions: set[str],
) -> dict[str, dict[str, list[str]]]:
    candidates: dict[str, dict[str, list[str]]] = {}
    for source_action_key in sorted(demoted_actions):
        policy_class, _ = _policy_class_and_cooldown(source_action_key)
        source_replaceable = _is_source_action_replaceable(source_action_key, policy_class)
        if not source_replaceable:
            continue
        strict_replacement_action_key = _text(REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(source_action_key))
        tertiary_replacement_action_key = _text(
            TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(source_action_key)
            or TERTIARY_REPLACEMENT_ACTION_BY_POLICY_CLASS.get(policy_class)
        )
        if not tertiary_replacement_action_key:
            continue
        if tertiary_replacement_action_key in RISKY_EDGE_RELAX_ACTION_KEYS:
            continue
        strict_replacement_viable = bool(
            strict_replacement_action_key
            and strict_replacement_action_key not in demoted_actions
            and strict_replacement_action_key not in auto_disabled_actions
        )
        if not strict_replacement_viable:
            continue
        row = candidates.setdefault(
            tertiary_replacement_action_key,
            {
                "source_actions": [],
                "strict_replacement_actions": [],
            },
        )
        row["source_actions"].append(source_action_key)
        if strict_replacement_action_key:
            row["strict_replacement_actions"].append(strict_replacement_action_key)
    normalized_candidates: dict[str, dict[str, list[str]]] = {}
    for action_key in sorted(candidates):
        row = candidates[action_key]
        normalized_candidates[action_key] = {
            "source_actions": sorted(set(_text(value) for value in row.get("source_actions", []) if _text(value))),
            "strict_replacement_actions": sorted(
                set(_text(value) for value in row.get("strict_replacement_actions", []) if _text(value))
            ),
            }
    return normalized_candidates


def _collect_final_fallback_reserve_candidates(
    *,
    demoted_actions: set[str],
    auto_disabled_actions: set[str],
) -> dict[str, dict[str, list[str]]]:
    candidates: dict[str, dict[str, list[str]]] = {}
    for source_action_key in sorted(demoted_actions):
        policy_class, _ = _policy_class_and_cooldown(source_action_key)
        source_replaceable = _is_source_action_replaceable(source_action_key, policy_class)
        if not source_replaceable:
            continue
        strict_replacement_action_key = _text(REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(source_action_key))
        tertiary_replacement_action_key = _text(
            TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(source_action_key)
            or TERTIARY_REPLACEMENT_ACTION_BY_POLICY_CLASS.get(policy_class)
        )
        final_fallback_replacement_action_key = _text(
            FINAL_FALLBACK_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(source_action_key)
            or FINAL_FALLBACK_REPLACEMENT_ACTION_BY_POLICY_CLASS.get(policy_class)
            or DEFAULT_FINAL_FALLBACK_REPLACEMENT_ACTION_KEY
        )
        if not final_fallback_replacement_action_key:
            continue
        if final_fallback_replacement_action_key in RISKY_EDGE_RELAX_ACTION_KEYS:
            continue
        strict_replacement_viable = bool(
            strict_replacement_action_key
            and strict_replacement_action_key not in demoted_actions
            and strict_replacement_action_key not in auto_disabled_actions
        )
        tertiary_replacement_viable = bool(
            tertiary_replacement_action_key
            and tertiary_replacement_action_key not in demoted_actions
            and tertiary_replacement_action_key not in auto_disabled_actions
        )
        if not (strict_replacement_viable or tertiary_replacement_viable):
            continue
        row = candidates.setdefault(
            final_fallback_replacement_action_key,
            {
                "source_actions": [],
                "strict_replacement_actions": [],
                "tertiary_replacement_actions": [],
            },
        )
        row["source_actions"].append(source_action_key)
        if strict_replacement_action_key:
            row["strict_replacement_actions"].append(strict_replacement_action_key)
        if tertiary_replacement_action_key:
            row["tertiary_replacement_actions"].append(tertiary_replacement_action_key)
    normalized_candidates: dict[str, dict[str, list[str]]] = {}
    for action_key in sorted(candidates):
        row = candidates[action_key]
        normalized_candidates[action_key] = {
            "source_actions": sorted(set(_text(value) for value in row.get("source_actions", []) if _text(value))),
            "strict_replacement_actions": sorted(
                set(_text(value) for value in row.get("strict_replacement_actions", []) if _text(value))
            ),
            "tertiary_replacement_actions": sorted(
                set(_text(value) for value in row.get("tertiary_replacement_actions", []) if _text(value))
            ),
        }
    return normalized_candidates


def _collect_final_fallback_candidate_action_keys(
    *,
    demoted_actions: set[str],
) -> set[str]:
    candidates: set[str] = set()
    for source_action_key in sorted(demoted_actions):
        policy_class, _ = _policy_class_and_cooldown(source_action_key)
        if not _is_source_action_replaceable(source_action_key, policy_class):
            continue
        final_fallback_replacement_action_key = _text(
            FINAL_FALLBACK_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(source_action_key)
            or FINAL_FALLBACK_REPLACEMENT_ACTION_BY_POLICY_CLASS.get(policy_class)
            or DEFAULT_FINAL_FALLBACK_REPLACEMENT_ACTION_KEY
        )
        if not final_fallback_replacement_action_key:
            continue
        if final_fallback_replacement_action_key in RISKY_EDGE_RELAX_ACTION_KEYS:
            continue
        candidates.add(final_fallback_replacement_action_key)
    return candidates


def _new_action_effectiveness_metrics() -> dict[str, int | float]:
    return {
        "executed_count": 0,
        "worsening_count": 0,
        "non_worsening_count": 0,
        "cumulative_negative_share_delta": 0.0,
        "average_negative_share_delta": 0.0,
    }


def _action_effectiveness_snapshot(
    action_effectiveness: dict[str, dict[str, int | float]],
) -> dict[str, dict[str, int | float]]:
    snapshot: dict[str, dict[str, int | float]] = {}
    for action_key in sorted(action_effectiveness):
        row = action_effectiveness[action_key]
        executed_count = max(0, _safe_int(row.get("executed_count")))
        worsening_count = max(0, _safe_int(row.get("worsening_count")))
        non_worsening_count = max(0, _safe_int(row.get("non_worsening_count")))
        cumulative_delta = _safe_float(row.get("cumulative_negative_share_delta")) or 0.0
        average_delta = _safe_float(row.get("average_negative_share_delta")) or 0.0
        snapshot[action_key] = {
            "executed_count": executed_count,
            "worsening_count": worsening_count,
            "non_worsening_count": non_worsening_count,
            "cumulative_negative_share_delta": round(float(cumulative_delta), 6),
            "average_negative_share_delta": round(float(average_delta), 6),
        }
    return snapshot


def _replacement_route_candidate_score(
    *,
    action_key: str,
    action_effectiveness: dict[str, dict[str, int | float]],
) -> dict[str, int | float | str]:
    normalized_action_key = _text(action_key)
    metrics = _as_dict(action_effectiveness.get(normalized_action_key))
    executed_count = max(0, _safe_int(metrics.get("executed_count")))
    worsening_count = max(0, _safe_int(metrics.get("worsening_count")))
    average_delta = float(_safe_float(metrics.get("average_negative_share_delta")) or 0.0)
    worsening_ratio = (
        float(worsening_count) / float(executed_count)
        if executed_count > 0
        else float(REPLACEMENT_ROUTE_UNKNOWN_WORSENING_RATIO)
    )
    average_delta_harm = max(0.0, min(1.0, float(average_delta) * float(REPLACEMENT_ROUTE_AVERAGE_DELTA_SCALE)))
    execution_uncertainty = 1.0 / (1.0 + float(executed_count))
    harm_score = (
        float(REPLACEMENT_ROUTE_WEIGHT_WORSENING_RATIO) * float(worsening_ratio)
        + float(REPLACEMENT_ROUTE_WEIGHT_AVERAGE_DELTA) * float(average_delta_harm)
        + float(REPLACEMENT_ROUTE_WEIGHT_EXECUTION_UNCERTAINTY) * float(execution_uncertainty)
    )
    return {
        "action_key": normalized_action_key,
        "executed_count": int(executed_count),
        "worsening_count": int(worsening_count),
        "worsening_ratio": round(float(worsening_ratio), 6),
        "average_negative_share_delta": round(float(average_delta), 6),
        "average_delta_harm_component": round(float(average_delta_harm), 6),
        "execution_uncertainty_component": round(float(execution_uncertainty), 6),
        "harm_score": round(float(harm_score), 6),
    }


def _compute_adaptive_effectiveness_thresholds(
    *,
    negative_share_before: float | None,
    negative_share_after: float | None,
    negative_share_delta_history: list[float],
) -> dict[str, int | float]:
    severity_source = (
        negative_share_after
        if isinstance(negative_share_after, float)
        else negative_share_before
    )
    severity = max(0.0, min(1.0, float(severity_source))) if isinstance(severity_source, float) else 0.5
    worsening_deltas = [float(delta) for delta in negative_share_delta_history if float(delta) > 0.0]
    if worsening_deltas:
        worsening_velocity = float(sum(worsening_deltas)) / float(len(worsening_deltas))
    else:
        worsening_velocity = 0.0

    min_executions = int(EFFECTIVENESS_DEMOTION_MIN_EXECUTIONS)
    min_worsening_ratio = float(EFFECTIVENESS_DEMOTION_MIN_WORSENING_RATIO)
    min_average_delta = float(EFFECTIVENESS_DEMOTION_MIN_AVERAGE_DELTA)

    if severity >= 0.85:
        min_executions = max(2, min_executions - 1)
        min_worsening_ratio = max(0.6, min_worsening_ratio - 0.15)
    elif severity <= 0.40:
        min_executions += 1
        min_worsening_ratio = min(0.95, min_worsening_ratio + 0.05)
        min_average_delta = round(min_average_delta + 0.002, 6)

    if worsening_velocity >= 0.02:
        min_executions = max(2, min_executions - 1)
        min_worsening_ratio = max(0.6, min_worsening_ratio - 0.1)
    elif worsening_velocity <= 0.002:
        min_worsening_ratio = min(0.95, min_worsening_ratio + 0.05)

    return {
        "severity": round(float(severity), 6),
        "worsening_velocity": round(float(worsening_velocity), 6),
        "min_executions": int(min_executions),
        "min_worsening_ratio": round(float(min_worsening_ratio), 6),
        "min_average_negative_share_delta": round(float(min_average_delta), 6),
    }


def _should_demote_action_for_effectiveness(
    metrics: dict[str, int | float],
    *,
    thresholds: dict[str, int | float],
) -> bool:
    executed_count = max(0, _safe_int(metrics.get("executed_count")))
    min_executions = max(1, _safe_int(thresholds.get("min_executions")))
    if executed_count < min_executions:
        return False
    worsening_count = max(0, _safe_int(metrics.get("worsening_count")))
    worsening_ratio = float(worsening_count) / float(executed_count) if executed_count > 0 else 0.0
    average_delta = _safe_float(metrics.get("average_negative_share_delta")) or 0.0
    min_worsening_ratio = max(0.0, min(1.0, float(_safe_float(thresholds.get("min_worsening_ratio")) or 0.0)))
    min_average_delta = float(_safe_float(thresholds.get("min_average_negative_share_delta")) or 0.0)
    return bool(
        worsening_ratio >= min_worsening_ratio
        and float(average_delta) > min_average_delta
    )


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return float(parsed)


def _safe_int(value: Any) -> int:
    parsed = _safe_float(value)
    if parsed is None:
        return 0
    return int(round(parsed))


def _safe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return bool(value)
    parsed = _safe_float(value)
    if isinstance(parsed, float):
        return bool(parsed)
    normalized = _text(value).lower()
    if normalized in {"true", "yes", "y", "on", "active", "enabled", "high"}:
        return True
    if normalized in {"false", "no", "n", "off", "inactive", "disabled", "low"}:
        return False
    return None


def _normalize_recovery_risk_off_stale_share_threshold(value: Any) -> float | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return max(
        float(RECOVERY_RISK_OFF_STALE_SHARE_THRESHOLD_MIN),
        min(1.0, float(parsed)),
    )


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _coerce_policy_reason_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, int] = {}
    for raw_reason, raw_count in value.items():
        reason = _text(raw_reason)
        if not reason:
            continue
        count = max(0, _safe_int(raw_count))
        if count <= 0:
            continue
        normalized[reason] = int(count)
    return dict(
        sorted(
            normalized.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )
    )


def _extract_policy_reason_counts_from_trader_payload(payload: dict[str, Any]) -> dict[str, int]:
    payload_dict = _as_dict(payload)
    intent_summary = _as_dict(payload_dict.get("intent_summary"))
    counts = _coerce_policy_reason_counts(intent_summary.get("policy_reason_counts"))
    if counts:
        return counts
    return _coerce_policy_reason_counts(payload_dict.get("policy_reason_counts"))


def _policy_reason_counts_subset(*, counts: dict[str, int], allowed_reasons: set[str]) -> dict[str, int]:
    subset: dict[str, int] = {}
    for reason, count in counts.items():
        if reason in allowed_reasons and int(count) > 0:
            subset[reason] = int(count)
    return dict(
        sorted(
            subset.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )
    )


def _resolve_trader_diagnostic_total(payload: dict[str, Any], key: str) -> int:
    payload_dict = _as_dict(payload)
    intent_summary = _as_dict(payload_dict.get("intent_summary"))
    if key in intent_summary:
        return max(0, _safe_int(intent_summary.get(key)))
    if key in payload_dict:
        return max(0, _safe_int(payload_dict.get(key)))
    return 0


def _resolve_trader_hardening_flag(
    *,
    payload: dict[str, Any],
    call_kwargs: dict[str, Any],
    key: str,
) -> bool | None:
    payload_dict = _as_dict(payload)
    intent_summary = _as_dict(payload_dict.get("intent_summary"))
    for source in (intent_summary, payload_dict, call_kwargs):
        if key not in source:
            continue
        value = source.get(key)
        if isinstance(value, bool):
            return bool(value)
        parsed = _safe_float(value)
        if parsed is not None:
            return bool(parsed)
    return None


def _build_trader_payload_diagnostics(
    *,
    payload: dict[str, Any],
    call_kwargs: dict[str, Any],
    probe_index: int,
) -> dict[str, Any]:
    payload_dict = _as_dict(payload)
    intent_summary = _as_dict(payload_dict.get("intent_summary"))
    plan_summary = _as_dict(payload_dict.get("plan_summary"))
    policy_reason_counts = _extract_policy_reason_counts_from_trader_payload(payload_dict)
    hardening_flags: dict[str, bool | None] = {}
    for key in TRADER_PAYLOAD_HARDENING_FLAG_KEYS:
        hardening_flags[key] = _resolve_trader_hardening_flag(
            payload=payload_dict,
            call_kwargs=call_kwargs,
            key=key,
        )
    return {
        "probe_index": int(probe_index),
        "status": _text(payload_dict.get("status")).lower() or "unknown",
        "applied_hardening_flags": hardening_flags,
        "policy_reason_counts": policy_reason_counts,
        "policy_reason_counts_top": list(policy_reason_counts.items())[:5],
        "key_totals": {
            "intents_total": _resolve_trader_diagnostic_total(payload_dict, "intents_total"),
            "intents_approved": _resolve_trader_diagnostic_total(payload_dict, "intents_approved"),
            "intents_blocked": _resolve_trader_diagnostic_total(payload_dict, "intents_blocked"),
            "planned_orders": max(0, _safe_int(plan_summary.get("planned_orders"))),
            "policy_reason_count_total": sum(int(count) for count in policy_reason_counts.values()),
        },
        "intent_summary_status": _text(intent_summary.get("status")).lower() or "",
        "plan_summary_status": _text(plan_summary.get("status")).lower() or "",
    }


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name = f".{path.name}.tmp-{os.getpid()}-{datetime.now(timezone.utc).timestamp():.6f}"
    temp_path = path.with_name(temp_name)
    try:
        temp_path.write_text(text, encoding="utf-8")
        temp_path.replace(path)
    finally:
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass


def _load_json_file(path: Path) -> dict[str, Any]:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(decoded) if isinstance(decoded, dict) else {}


def _candidate_artifact_roots(output_dir: Path) -> list[Path]:
    candidates = [output_dir]
    name = _text(output_dir.name).lower()
    parent = output_dir.parent
    if name == "output":
        candidates.append(parent / "outputs")
    elif name == "outputs":
        candidates.append(parent / "output")
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _artifact_search_roots(*, output_dir: Path, include_health: bool = False) -> list[Path]:
    roots: list[Path] = []
    for candidate in _candidate_artifact_roots(output_dir):
        roots.append(candidate)
        if include_health:
            roots.append(candidate / "health")
    deduped: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root.resolve()) if root.exists() else str(root)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(root)
    return deduped


def _latest_artifact_by_patterns(
    *,
    output_dir: Path,
    patterns: list[str],
    include_health: bool = False,
) -> Path | None:
    matches: list[Path] = []
    for root in _artifact_search_roots(output_dir=output_dir, include_health=include_health):
        for pattern in patterns:
            matches.extend(path for path in root.glob(pattern) if path.is_file())
    if not matches:
        return None
    return max(
        matches,
        key=lambda path: (_artifact_mtime_epoch(path) or 0.0, path.name),
    )


def _latest_trade_summary_artifact(output_dir: Path) -> Path | None:
    search_roots = _artifact_search_roots(output_dir=output_dir, include_health=True)
    preferred_exact = [
        "kalshi_temperature_trade_intents_summary_latest.json",
        "kalshi_temperature_trade_plan_summary_latest.json",
    ]
    for name in preferred_exact:
        for root in search_roots:
            path = root / name
            if path.is_file():
                return path

    return _latest_artifact_by_patterns(
        output_dir=output_dir,
        patterns=[
            "kalshi_temperature_trade_intents_summary_*.json",
            "kalshi_temperature_trade_plan_summary_*.json",
        ],
        include_health=True,
    )


def _artifact_mtime_epoch(path: Path | None) -> float | None:
    if not isinstance(path, Path):
        return None
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return None


def _summary_activity_count(summary_payload: dict[str, Any]) -> int:
    if not isinstance(summary_payload, dict):
        return 0
    candidates = [
        summary_payload.get("intents_total"),
        summary_payload.get("candidate_count"),
        summary_payload.get("planned_orders"),
        summary_payload.get("planned_order_count"),
    ]
    parsed = [max(0, _safe_int(value)) for value in candidates]
    return max(parsed) if parsed else 0


def _latest_blocker_audit_artifact(output_dir: Path) -> Path | None:
    return _latest_artifact_by_patterns(
        output_dir=output_dir,
        patterns=[
            "checkpoints/blocker_audit_168h_latest.json",
            "checkpoints/blocker_audit_168h_*.json",
            "checkpoints/blocker_audit_*_latest.json",
            "checkpoints/blocker_audit_*.json",
        ],
    )


def _latest_execution_cost_tape_artifact(output_dir: Path) -> Path | None:
    search_roots = _artifact_search_roots(output_dir=output_dir, include_health=True)
    preferred_exact = [
        "execution_cost_tape_latest.json",
        "kalshi_temperature_execution_cost_tape_latest.json",
    ]
    for name in preferred_exact:
        for root in search_roots:
            path = root / name
            if path.is_file():
                return path
    return _latest_artifact_by_patterns(
        output_dir=output_dir,
        patterns=[
            "execution_cost_tape_*.json",
            "kalshi_temperature_execution_cost_tape_*.json",
        ],
        include_health=True,
    )


def _load_latest_execution_cost_tape_payload(output_dir: Path) -> dict[str, Any]:
    artifact_path = _latest_execution_cost_tape_artifact(output_dir)
    if not isinstance(artifact_path, Path):
        return {}
    return _as_dict(_load_json_file(artifact_path))


def _humanize_policy_reason(reason: str) -> str:
    text = _text(reason).replace("_", " ")
    text = " ".join(part for part in text.split() if part)
    return text or "unknown"


def _ensure_blocker_audit_fallback_from_trade_summary(output_dir: Path) -> dict[str, Any]:
    existing = _latest_blocker_audit_artifact(output_dir)
    if isinstance(existing, Path) and existing.is_file():
        return {
            "status": "existing",
            "source_file": str(existing),
            "written": False,
        }

    summary_path = _latest_trade_summary_artifact(output_dir)
    if not isinstance(summary_path, Path):
        return {
            "status": "missing_trade_summary",
            "source_file": "",
            "written": False,
        }

    summary_payload = _load_json_file(summary_path)
    intent_summary = _as_dict(summary_payload.get("intent_summary"))
    policy_reason_counts_raw = summary_payload.get("policy_reason_counts")
    if not isinstance(policy_reason_counts_raw, dict):
        policy_reason_counts_raw = intent_summary.get("policy_reason_counts")

    policy_reason_counts: dict[str, int] = {}
    if isinstance(policy_reason_counts_raw, dict):
        for reason_raw, count_raw in policy_reason_counts_raw.items():
            reason = _text(reason_raw).lower()
            if not reason or reason == "approved":
                continue
            count = max(0, _safe_int(count_raw))
            if count <= 0:
                continue
            policy_reason_counts[reason] = count

    intents_total = max(0, _safe_int(summary_payload.get("intents_total")))
    intents_approved = max(0, _safe_int(summary_payload.get("intents_approved")))
    if "intents_total" not in summary_payload:
        intents_total = max(0, _safe_int(intent_summary.get("intents_total")))
    if "intents_approved" not in summary_payload:
        intents_approved = max(0, _safe_int(intent_summary.get("intents_approved")))

    intents_blocked = max(0, _safe_int(summary_payload.get("intents_blocked")))
    if "intents_blocked" not in summary_payload:
        intents_blocked = max(0, _safe_int(intent_summary.get("intents_blocked")))

    blocked_total = max(
        intents_blocked,
        max(0, intents_total - intents_approved),
        sum(policy_reason_counts.values()),
    )
    if blocked_total <= 0:
        return {
            "status": "insufficient_blocked_flow",
            "source_file": str(summary_path),
            "written": False,
        }

    sorted_reasons = sorted(
        policy_reason_counts.items(),
        key=lambda item: (-int(item[1]), str(item[0])),
    )
    largest_reason = ""
    largest_count = 0
    if sorted_reasons:
        largest_reason, largest_count = sorted_reasons[0]
    largest_share = float(largest_count) / float(blocked_total) if blocked_total > 0 else 0.0

    top_blockers: list[dict[str, Any]] = []
    for reason, count in sorted_reasons[:5]:
        share = float(count) / float(blocked_total) if blocked_total > 0 else 0.0
        top_blockers.append(
            {
                "reason_raw": reason,
                "reason_human": _humanize_policy_reason(reason),
                "count": int(count),
                "share_of_blocked": round(float(share), 6),
                "recommended_action": "Prioritize recovery-loop remediation for this dominant blocker.",
            }
        )

    captured_at = datetime.now(timezone.utc)
    payload = {
        "status": "ready",
        "captured_at": captured_at.isoformat(),
        "window_label": "168h",
        "source": "recovery_loop_trade_summary_fallback",
        "headline": {
            "largest_blocker_reason": _humanize_policy_reason(largest_reason),
            "largest_blocker_reason_raw": largest_reason,
            "largest_blocker_count": int(largest_count),
            "largest_blocker_count_raw": int(largest_count),
            "largest_blocker_share_of_blocked": round(float(largest_share), 6),
            "largest_blocker_share_of_blocked_raw": round(float(largest_share), 6),
            "blocked_total": int(blocked_total),
            "blocked_total_raw": int(blocked_total),
        },
        "top_blockers": top_blockers,
        "source_files": {
            "trade_summary_file": str(summary_path),
        },
    }

    checkpoints_dir = output_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    stamp = captured_at.strftime("%Y%m%d_%H%M%S")
    output_path = checkpoints_dir / f"blocker_audit_168h_fallback_{stamp}.json"
    latest_path = checkpoints_dir / "blocker_audit_168h_latest.json"
    encoded = json.dumps(payload, indent=2, sort_keys=True)
    _write_text_atomic(output_path, encoded)
    _write_text_atomic(latest_path, encoded)
    return {
        "status": "written",
        "source_file": str(summary_path),
        "output_file": str(output_path),
        "latest_file": str(latest_path),
        "written": True,
    }


def _verify_trader_shadow_action_effect(
    *,
    action_key: str,
    output_dir: Path,
    before_summary_path: Path | None,
    before_summary_mtime_epoch: float | None,
) -> tuple[str, str, dict[str, Any]]:
    if action_key not in TRADER_SHADOW_EFFECT_ACTION_KEYS:
        return ("not_checked", "", {})

    after_summary_path = _latest_trade_summary_artifact(output_dir)
    after_summary_mtime_epoch = _artifact_mtime_epoch(after_summary_path)
    evidence: dict[str, Any] = {
        "before_trade_summary_path": str(before_summary_path) if isinstance(before_summary_path, Path) else "",
        "after_trade_summary_path": str(after_summary_path) if isinstance(after_summary_path, Path) else "",
        "before_trade_summary_mtime_epoch": (
            round(float(before_summary_mtime_epoch), 6)
            if isinstance(before_summary_mtime_epoch, float)
            else None
        ),
        "after_trade_summary_mtime_epoch": (
            round(float(after_summary_mtime_epoch), 6)
            if isinstance(after_summary_mtime_epoch, float)
            else None
        ),
    }
    if not isinstance(after_summary_path, Path):
        return ("no_effect", "missing_trade_summary_artifact", evidence)

    summary_payload = _load_json_file(after_summary_path)
    activity_count = _summary_activity_count(summary_payload)
    evidence["summary_activity_count"] = int(activity_count)
    if (
        isinstance(before_summary_mtime_epoch, float)
        and isinstance(after_summary_mtime_epoch, float)
        and float(after_summary_mtime_epoch) <= float(before_summary_mtime_epoch)
    ):
        return ("no_effect", "trade_summary_not_updated", evidence)
    if activity_count <= 0:
        return ("no_effect", "trade_summary_zero_activity", evidence)
    return ("verified", "", evidence)


def _resolve_trader_shadow_fallback_inputs(*, output_dir: str) -> dict[str, str]:
    out_path = Path(output_dir)
    resolved: dict[str, str] = {}
    constraint_csv = _latest_artifact_by_patterns(
        output_dir=out_path,
        patterns=["kalshi_temperature_constraint_scan_*.csv"],
    )
    specs_csv = _latest_artifact_by_patterns(
        output_dir=out_path,
        patterns=["kalshi_temperature_contract_specs_*.csv"],
    )
    metar_summary_json = _latest_artifact_by_patterns(
        output_dir=out_path,
        patterns=["kalshi_temperature_metar_summary_*.json"],
    )
    settlement_state_json = _latest_artifact_by_patterns(
        output_dir=out_path,
        patterns=["kalshi_temperature_settlement_state_*.json"],
    )
    if isinstance(constraint_csv, Path):
        resolved["constraint_csv"] = str(constraint_csv)
    if isinstance(specs_csv, Path):
        resolved["specs_csv"] = str(specs_csv)
    if isinstance(metar_summary_json, Path):
        resolved["metar_summary_json"] = str(metar_summary_json)
    if isinstance(settlement_state_json, Path):
        resolved["settlement_state_json"] = str(settlement_state_json)
    return resolved


def _normalize_shadow_quote_probe_target(raw_target: Any) -> str:
    if isinstance(raw_target, dict):
        bucket = _text(
            raw_target.get("bucket")
            or raw_target.get("market_side")
            or raw_target.get("market_ticker")
            or raw_target.get("ticker")
        )
        if not bucket:
            return ""
        if "|" in bucket:
            ticker, side = bucket.rsplit("|", 1)
            ticker = _text(ticker).upper()
            side = _text(side).lower()
            if ticker and side in {"yes", "no"}:
                return f"{ticker}|{side}"
            return ticker
        bucket = bucket.upper()
        side = _text(raw_target.get("side")).lower()
        if side in {"yes", "no"}:
            return f"{bucket}|{side}"
        return bucket
    text = _text(raw_target)
    if not text:
        return ""
    if "|" not in text:
        return text.upper()
    ticker, side = text.rsplit("|", 1)
    ticker = _text(ticker).upper()
    side = _text(side).lower()
    if ticker and side in {"yes", "no"}:
        return f"{ticker}|{side}"
    return ticker


def _normalize_execution_cost_market_side_target(raw_target: Any) -> str:
    if isinstance(raw_target, dict):
        bucket = _text(
            raw_target.get("bucket")
            or raw_target.get("market_side")
            or raw_target.get("market_side_key")
            or raw_target.get("target")
            or raw_target.get("market_ticker")
            or raw_target.get("ticker")
        )
        side = _normalize_market_side(raw_target.get("side"))
        if bucket and "|" not in bucket and side in {"yes", "no"}:
            bucket = f"{bucket}|{side}"
        normalized = _normalize_shadow_quote_probe_target(bucket)
    else:
        normalized = _normalize_shadow_quote_probe_target(raw_target)
    if "|" not in normalized:
        return ""
    ticker, side = normalized.rsplit("|", 1)
    ticker = _normalize_execution_cost_exclusion_ticker(ticker)
    side = _normalize_market_side(side)
    if not ticker or side not in {"yes", "no"}:
        return ""
    return f"{ticker}|{side}"


def _normalize_market_side(value: Any) -> str:
    side = _text(value).lower()
    return side if side in {"yes", "no"} else ""


def _load_shadow_quote_probe_side_pressure(output_dir: Path) -> dict[str, Any]:
    payload = _load_latest_execution_cost_tape_payload(output_dir)
    execution_siphon_pressure = _as_dict(payload.get("execution_siphon_pressure"))
    execution_cost_observations = _as_dict(payload.get("execution_cost_observations"))
    recommended_shadow_quote_probe_targets = _as_dict(payload.get("recommended_shadow_quote_probe_targets"))
    side_pressure_payload = _as_dict(execution_siphon_pressure.get("side_pressure"))
    if not side_pressure_payload:
        side_pressure_payload = _as_dict(payload.get("side_pressure"))
    if not side_pressure_payload:
        side_pressure_payload = _as_dict(execution_cost_observations.get("side_pressure"))
    if not side_pressure_payload:
        side_pressure_payload = _as_dict(recommended_shadow_quote_probe_targets.get("side_pressure"))

    dominant_side = _normalize_market_side(
        side_pressure_payload.get("dominant_side")
        or side_pressure_payload.get("dominant_market_side")
        or side_pressure_payload.get("side")
    )
    dominant_side_share = _safe_float(
        side_pressure_payload.get("dominant_side_share")
        or side_pressure_payload.get("dominance_share")
        or side_pressure_payload.get("dominant_share")
    )
    dominant_side_rows = max(
        0,
        _safe_int(
            side_pressure_payload.get("dominant_side_rows")
            or side_pressure_payload.get("dominant_rows")
        ),
    )
    total_rows = max(
        0,
        _safe_int(
            side_pressure_payload.get("total_rows")
            or side_pressure_payload.get("rows_total")
            or side_pressure_payload.get("side_rows_total")
        ),
    )
    pressure_score = _safe_float(
        side_pressure_payload.get("side_pressure_score")
        or side_pressure_payload.get("pressure_score")
        or side_pressure_payload.get("score")
    )
    side_pressure_active_flag: bool | None = None
    for key in ("side_pressure_active", "active", "is_active", "high_pressure"):
        if key not in side_pressure_payload:
            continue
        side_pressure_active_flag = _safe_bool(side_pressure_payload.get(key))
        if isinstance(side_pressure_active_flag, bool):
            break

    top_missing_coverage_buckets = _as_dict(execution_cost_observations.get("top_missing_coverage_buckets"))
    by_side_rows = top_missing_coverage_buckets.get("by_side")
    side_rows: dict[str, int] = {"yes": 0, "no": 0}
    if isinstance(by_side_rows, list):
        for row in by_side_rows:
            row_dict = _as_dict(row)
            side = _normalize_market_side(row_dict.get("bucket") or row_dict.get("side"))
            if not side:
                continue
            missing_rows = _safe_int(
                row_dict.get("rows_without_two_sided_quote")
                or row_dict.get("rows")
                or row_dict.get("count")
            )
            if missing_rows <= 0:
                continue
            side_rows[side] = side_rows.get(side, 0) + int(missing_rows)

    derived_yes_rows = max(0, _safe_int(side_rows.get("yes")))
    derived_no_rows = max(0, _safe_int(side_rows.get("no")))
    derived_total_rows = int(derived_yes_rows + derived_no_rows)
    derived_dominant_side = ""
    if derived_yes_rows > derived_no_rows:
        derived_dominant_side = "yes"
    elif derived_no_rows > derived_yes_rows:
        derived_dominant_side = "no"
    derived_dominant_rows = 0
    if derived_dominant_side:
        derived_dominant_rows = int(derived_yes_rows if derived_dominant_side == "yes" else derived_no_rows)
    derived_dominant_share = (
        float(derived_dominant_rows) / float(derived_total_rows)
        if derived_dominant_side and derived_total_rows > 0
        else None
    )

    if not dominant_side:
        dominant_side = derived_dominant_side
    if dominant_side_rows <= 0:
        dominant_side_rows = int(derived_dominant_rows)
    if total_rows <= 0:
        total_rows = int(derived_total_rows)
    if not isinstance(dominant_side_share, float) and isinstance(derived_dominant_share, float):
        dominant_side_share = float(derived_dominant_share)
    if not isinstance(pressure_score, float) and isinstance(dominant_side_share, float):
        pressure_score = float(dominant_side_share)

    high_side_pressure_by_score = bool(
        dominant_side in {"yes", "no"}
        and isinstance(pressure_score, float)
        and pressure_score >= float(QUOTE_COVERAGE_SIDE_PRESSURE_SCORE_THRESHOLD)
    )
    high_side_pressure_by_share = bool(
        dominant_side in {"yes", "no"}
        and isinstance(dominant_side_share, float)
        and dominant_side_rows >= int(QUOTE_COVERAGE_SIDE_PRESSURE_MIN_DOMINANT_ROWS)
        and dominant_side_share >= float(QUOTE_COVERAGE_SIDE_PRESSURE_DOMINANT_SHARE_THRESHOLD)
    )
    if isinstance(side_pressure_active_flag, bool):
        side_pressure_active = bool(side_pressure_active_flag and dominant_side in {"yes", "no"})
    else:
        side_pressure_active = bool(high_side_pressure_by_score or high_side_pressure_by_share)

    return {
        "active": bool(side_pressure_active),
        "dominant_side": dominant_side,
        "dominant_side_rows": int(dominant_side_rows),
        "total_rows": int(total_rows),
        "dominant_side_share": (
            round(float(dominant_side_share), 6)
            if isinstance(dominant_side_share, float)
            else None
        ),
        "pressure_score": round(float(pressure_score), 6) if isinstance(pressure_score, float) else None,
    }


def _apply_shadow_quote_probe_side_bias(
    *,
    targets: list[str],
    side_pressure_active: bool,
    dominant_side: str,
) -> list[str]:
    ordered_targets = [_text(target) for target in targets if _text(target)]
    normalized_dominant_side = _normalize_market_side(dominant_side)
    if not ordered_targets:
        return []
    if not side_pressure_active or normalized_dominant_side not in {"yes", "no"}:
        return ordered_targets

    dominant_targets: list[str] = []
    fallback_targets: list[str] = []
    for target in ordered_targets:
        target_side = ""
        if "|" in target:
            target_side = _normalize_market_side(target.rsplit("|", 1)[1])
        if target_side == normalized_dominant_side:
            dominant_targets.append(target)
        else:
            fallback_targets.append(target)
    if not dominant_targets:
        return ordered_targets

    fallback_keep_count = 0
    if fallback_targets:
        fallback_keep_count = max(
            int(QUOTE_COVERAGE_SIDE_BIAS_MIN_FALLBACK_TARGETS),
            min(len(fallback_targets), max(1, len(dominant_targets) // 2)),
        )

    biased_targets = list(dominant_targets)
    if fallback_keep_count > 0:
        biased_targets.extend(fallback_targets[:fallback_keep_count])
    return biased_targets if biased_targets else ordered_targets


def _normalize_execution_cost_exclusion_ticker(raw_ticker: Any) -> str:
    if isinstance(raw_ticker, dict):
        ticker = _text(
            raw_ticker.get("ticker")
            or raw_ticker.get("market_ticker")
            or raw_ticker.get("bucket")
            or raw_ticker.get("market")
        )
    else:
        ticker = _text(raw_ticker)
    if not ticker:
        return ""
    if "|" in ticker:
        ticker = ticker.rsplit("|", 1)[0]
    return ticker.upper()


def _load_shadow_quote_probe_targets(output_dir: Path) -> list[str]:
    artifact_path = _latest_execution_cost_tape_artifact(output_dir)
    payload = _load_json_file(artifact_path) if isinstance(artifact_path, Path) else {}
    execution_cost_observations = _as_dict(payload.get("execution_cost_observations"))
    nested_buckets = _as_dict(execution_cost_observations.get("top_missing_coverage_buckets"))
    top_level_buckets = _as_dict(payload.get("top_missing_coverage_buckets"))

    ordered_targets: list[str] = []
    seen: set[str] = set()

    def _extend(raw_rows: Any, *, market_side_only: bool) -> None:
        if not isinstance(raw_rows, list):
            return
        for row in raw_rows:
            target = _normalize_shadow_quote_probe_target(row)
            if not target:
                continue
            if market_side_only:
                if "|" not in target:
                    continue
                side = _text(target.rsplit("|", 1)[1]).lower()
                if side not in {"yes", "no"}:
                    continue
            if target in seen:
                continue
            seen.add(target)
            ordered_targets.append(target)

    _extend(nested_buckets.get("by_market_side"), market_side_only=True)
    _extend(top_level_buckets.get("by_market_side"), market_side_only=True)
    if not ordered_targets:
        _extend(nested_buckets.get("by_market"), market_side_only=False)
        _extend(top_level_buckets.get("by_market"), market_side_only=False)
    return ordered_targets[:64]


def _load_execution_cost_exclusion_market_side_targets(output_dir: Path) -> list[str]:
    payload = _load_latest_execution_cost_tape_payload(output_dir)
    if not payload:
        return []
    execution_siphon_pressure = _as_dict(payload.get("execution_siphon_pressure"))
    dominant_side = _normalize_market_side(
        execution_siphon_pressure.get("dominant_uncovered_side")
        or execution_siphon_pressure.get("dominant_side")
        or execution_siphon_pressure.get("dominant_market_side")
    )
    if dominant_side not in {"yes", "no"}:
        return []

    ordered_targets: list[str] = []
    seen_targets: set[str] = set()

    def _extend(raw_rows: Any) -> None:
        if not isinstance(raw_rows, list):
            return
        for row in raw_rows:
            target = _normalize_execution_cost_market_side_target(row)
            if not target or target in seen_targets:
                continue
            _, side = target.rsplit("|", 1)
            if side != dominant_side:
                continue
            seen_targets.add(target)
            ordered_targets.append(target)

    recommended_shadow_quote_probe_targets = _as_dict(payload.get("recommended_shadow_quote_probe_targets"))
    recommended_exclusions = _as_dict(payload.get("recommended_exclusions"))
    execution_cost_observations = _as_dict(payload.get("execution_cost_observations"))
    nested_buckets = _as_dict(execution_cost_observations.get("top_missing_coverage_buckets"))
    top_level_buckets = _as_dict(payload.get("top_missing_coverage_buckets"))

    _extend(recommended_shadow_quote_probe_targets.get("target_keys"))
    _extend(recommended_shadow_quote_probe_targets.get("market_side_targets"))
    _extend(recommended_exclusions.get("market_side_targets"))
    _extend(nested_buckets.get("by_market_side"))
    _extend(top_level_buckets.get("by_market_side"))
    return ordered_targets[:64]


def _filter_execution_cost_exclusion_market_side_targets(
    *,
    market_side_targets: list[str],
    allowed_tickers: list[str],
) -> list[str]:
    allowed_ticker_set = {
        ticker
        for ticker in (
            _normalize_execution_cost_exclusion_ticker(raw_ticker)
            for raw_ticker in allowed_tickers
        )
        if ticker
    }
    if not allowed_ticker_set:
        return []
    filtered: list[str] = []
    seen_targets: set[str] = set()
    for raw_target in market_side_targets:
        target = _normalize_execution_cost_market_side_target(raw_target)
        if not target or target in seen_targets:
            continue
        ticker, _ = target.rsplit("|", 1)
        if ticker not in allowed_ticker_set:
            continue
        seen_targets.add(target)
        filtered.append(target)
    return filtered


def _execution_cost_exclusions_state_artifact(output_dir: Path) -> Path:
    return output_dir / "health" / EXECUTION_COST_EXCLUSIONS_STATE_FILENAME


def _load_execution_cost_exclusions_state(output_dir: Path) -> dict[str, Any]:
    state_path = _execution_cost_exclusions_state_artifact(output_dir)
    payload = _load_json_file(state_path)
    if not payload:
        return {
            "run_count": 0,
            "tracked_tickers": {},
            "tracked_market_side_targets": {},
            "adaptive_downshift": {
                "last_evaluated_run": 0,
                "last_downshift_run": 0,
                "last_decision": "not_evaluated",
                "last_reason": "",
                "suppressed_tickers": {},
                "suppressed_market_side_targets": {},
                "last_probe_metrics": {},
                "last_coverage_metrics": {},
                "last_active_count_before": 0,
                "last_active_count_after": 0,
                "last_drop_count": 0,
                "last_drop_tickers": [],
                "last_active_market_side_target_count_before": 0,
                "last_active_market_side_target_count_after": 0,
                "last_drop_market_side_target_count": 0,
                "last_drop_market_side_targets": [],
            },
        }
    payload["run_count"] = max(0, _safe_int(payload.get("run_count")))
    tracked_tickers = _as_dict(payload.get("tracked_tickers"))
    payload["tracked_tickers"] = dict(tracked_tickers)
    tracked_market_side_targets_raw = _as_dict(payload.get("tracked_market_side_targets"))
    normalized_tracked_market_side_targets: dict[str, dict[str, Any]] = {}
    for raw_target, raw_entry in tracked_market_side_targets_raw.items():
        target = _normalize_execution_cost_market_side_target(raw_target)
        if not target:
            continue
        normalized_tracked_market_side_targets[target] = dict(_as_dict(raw_entry))
    payload["tracked_market_side_targets"] = dict(
        sorted(normalized_tracked_market_side_targets.items(), key=lambda item: item[0])
    )
    adaptive_downshift = _as_dict(payload.get("adaptive_downshift"))
    suppressed_tickers_raw = _as_dict(adaptive_downshift.get("suppressed_tickers"))
    suppressed_market_side_targets_raw = _as_dict(adaptive_downshift.get("suppressed_market_side_targets"))
    raw_last_drop_tickers = adaptive_downshift.get("last_drop_tickers")
    if not isinstance(raw_last_drop_tickers, list):
        raw_last_drop_tickers = []
    raw_last_drop_market_side_targets = adaptive_downshift.get("last_drop_market_side_targets")
    if not isinstance(raw_last_drop_market_side_targets, list):
        raw_last_drop_market_side_targets = []
    normalized_suppressed_tickers: dict[str, dict[str, Any]] = {}
    for raw_ticker, raw_entry in suppressed_tickers_raw.items():
        ticker = _normalize_execution_cost_exclusion_ticker(raw_ticker)
        if not ticker:
            continue
        entry = _as_dict(raw_entry)
        suppressed_until_run = max(0, _safe_int(entry.get("suppressed_until_run")))
        if suppressed_until_run <= 0:
            continue
        normalized_suppressed_tickers[ticker] = {
            "suppressed_until_run": int(suppressed_until_run),
            "applied_run": max(0, _safe_int(entry.get("applied_run"))),
            "reason": _text(entry.get("reason")),
        }
    normalized_suppressed_market_side_targets: dict[str, dict[str, Any]] = {}
    for raw_target, raw_entry in suppressed_market_side_targets_raw.items():
        target = _normalize_execution_cost_market_side_target(raw_target)
        if not target:
            continue
        entry = _as_dict(raw_entry)
        suppressed_until_run = max(0, _safe_int(entry.get("suppressed_until_run")))
        if suppressed_until_run <= 0:
            continue
        normalized_suppressed_market_side_targets[target] = {
            "suppressed_until_run": int(suppressed_until_run),
            "applied_run": max(0, _safe_int(entry.get("applied_run"))),
            "reason": _text(entry.get("reason")),
        }
    payload["adaptive_downshift"] = {
        "last_evaluated_run": max(0, _safe_int(adaptive_downshift.get("last_evaluated_run"))),
        "last_downshift_run": max(0, _safe_int(adaptive_downshift.get("last_downshift_run"))),
        "last_decision": _text(adaptive_downshift.get("last_decision")) or "not_evaluated",
        "last_reason": _text(adaptive_downshift.get("last_reason")),
        "suppressed_tickers": dict(sorted(normalized_suppressed_tickers.items(), key=lambda item: item[0])),
        "suppressed_market_side_targets": dict(
            sorted(normalized_suppressed_market_side_targets.items(), key=lambda item: item[0])
        ),
        "last_probe_metrics": _as_dict(adaptive_downshift.get("last_probe_metrics")),
        "last_coverage_metrics": _as_dict(adaptive_downshift.get("last_coverage_metrics")),
        "last_active_count_before": max(0, _safe_int(adaptive_downshift.get("last_active_count_before"))),
        "last_active_count_after": max(0, _safe_int(adaptive_downshift.get("last_active_count_after"))),
        "last_drop_count": max(0, _safe_int(adaptive_downshift.get("last_drop_count"))),
        "last_drop_tickers": [
            ticker
            for ticker in (
                _normalize_execution_cost_exclusion_ticker(value)
                for value in raw_last_drop_tickers
            )
            if ticker
        ],
        "last_active_market_side_target_count_before": max(
            0, _safe_int(adaptive_downshift.get("last_active_market_side_target_count_before"))
        ),
        "last_active_market_side_target_count_after": max(
            0, _safe_int(adaptive_downshift.get("last_active_market_side_target_count_after"))
        ),
        "last_drop_market_side_target_count": max(
            0, _safe_int(adaptive_downshift.get("last_drop_market_side_target_count"))
        ),
        "last_drop_market_side_targets": [
            target
            for target in (
                _normalize_execution_cost_market_side_target(value)
                for value in raw_last_drop_market_side_targets
            )
            if target
        ],
    }
    return payload


def _load_execution_cost_exclusion_tickers(output_dir: Path) -> list[str]:
    artifact_path = _latest_execution_cost_tape_artifact(output_dir)
    payload = _load_json_file(artifact_path) if isinstance(artifact_path, Path) else {}
    recommended_exclusions = _as_dict(payload.get("recommended_exclusions"))

    ordered_tickers: list[str] = []
    seen: set[str] = set()

    def _extend(raw_rows: Any) -> None:
        if not isinstance(raw_rows, list):
            return
        for row in raw_rows:
            ticker = _normalize_execution_cost_exclusion_ticker(row)
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            ordered_tickers.append(ticker)

    _extend(recommended_exclusions.get("market_tickers"))
    if ordered_tickers:
        return ordered_tickers[:20]

    calibration_readiness = _as_dict(payload.get("calibration_readiness"))
    quote_coverage_ratio = _safe_float(calibration_readiness.get("quote_coverage_ratio"))
    min_quote_coverage_ratio = _safe_float(calibration_readiness.get("min_quote_coverage_ratio"))
    if (
        not isinstance(quote_coverage_ratio, float)
        or not isinstance(min_quote_coverage_ratio, float)
        or quote_coverage_ratio >= min_quote_coverage_ratio
    ):
        return []

    execution_cost_observations = _as_dict(payload.get("execution_cost_observations"))
    nested_buckets = _as_dict(execution_cost_observations.get("top_missing_coverage_buckets"))
    top_level_buckets = _as_dict(payload.get("top_missing_coverage_buckets"))
    _extend(nested_buckets.get("by_market"))
    _extend(top_level_buckets.get("by_market"))
    return ordered_tickers[:20]


def _load_execution_cost_tape_quality_metrics(output_dir: Path) -> dict[str, Any]:
    artifact_path = _latest_execution_cost_tape_artifact(output_dir)
    payload = _load_json_file(artifact_path) if isinstance(artifact_path, Path) else {}
    calibration_readiness = _as_dict(payload.get("calibration_readiness"))
    quote_coverage_ratio = _safe_float(calibration_readiness.get("quote_coverage_ratio"))
    min_quote_coverage_ratio = _safe_float(calibration_readiness.get("min_quote_coverage_ratio"))
    coverage_gap = None
    if isinstance(quote_coverage_ratio, float) and isinstance(min_quote_coverage_ratio, float):
        coverage_gap = round(float(min_quote_coverage_ratio - quote_coverage_ratio), 6)
    return {
        "quote_coverage_ratio": round(float(quote_coverage_ratio), 6)
        if isinstance(quote_coverage_ratio, float)
        else None,
        "min_quote_coverage_ratio": round(float(min_quote_coverage_ratio), 6)
        if isinstance(min_quote_coverage_ratio, float)
        else None,
        "quote_coverage_gap_to_min": coverage_gap,
    }


def _build_execution_cost_side_target_accelerator_context(
    *,
    payload: dict[str, Any],
    side_target_candidates: list[str],
) -> dict[str, Any]:
    execution_siphon_pressure = _as_dict(payload.get("execution_siphon_pressure"))
    execution_siphon_trend = _as_dict(payload.get("execution_siphon_trend"))
    recommended_exclusions = _as_dict(payload.get("recommended_exclusions"))
    calibration_readiness = _as_dict(payload.get("calibration_readiness"))

    dominant_side = _normalize_market_side(
        execution_siphon_pressure.get("dominant_uncovered_side")
        or execution_siphon_pressure.get("dominant_side")
        or execution_siphon_pressure.get("dominant_market_side")
    )
    dominant_side_share = _safe_float(
        execution_siphon_pressure.get("dominant_uncovered_side_share")
        or execution_siphon_pressure.get("dominant_side_share")
        or execution_siphon_pressure.get("dominant_share")
    )
    side_imbalance_magnitude = _safe_float(
        execution_siphon_pressure.get("side_imbalance_magnitude")
        or execution_siphon_pressure.get("imbalance_magnitude")
    )
    side_pressure_score = _safe_float(
        execution_siphon_pressure.get("side_pressure_score_contribution")
        or execution_siphon_pressure.get("pressure_score")
    )

    side_pressure_materially_high = bool(
        dominant_side in {"yes", "no"}
        and (
            (
                isinstance(dominant_side_share, float)
                and float(dominant_side_share)
                >= float(EXECUTION_COST_SIDE_TARGET_ACCELERATOR_DOMINANT_SHARE_THRESHOLD)
            )
            or (
                isinstance(side_imbalance_magnitude, float)
                and float(side_imbalance_magnitude)
                >= float(EXECUTION_COST_SIDE_TARGET_ACCELERATOR_IMBALANCE_THRESHOLD)
            )
            or (
                isinstance(side_pressure_score, float)
                and float(side_pressure_score)
                >= float(EXECUTION_COST_SIDE_TARGET_ACCELERATOR_PRESSURE_SCORE_THRESHOLD)
            )
        )
    )

    quote_coverage_ratio = _safe_float(calibration_readiness.get("quote_coverage_ratio"))
    min_quote_coverage_ratio = _safe_float(calibration_readiness.get("min_quote_coverage_ratio"))
    quote_coverage_gap_to_min = None
    if isinstance(quote_coverage_ratio, float) and isinstance(min_quote_coverage_ratio, float):
        quote_coverage_gap_to_min = float(min_quote_coverage_ratio) - float(quote_coverage_ratio)
    quote_coverage_significantly_below_min = bool(
        isinstance(quote_coverage_gap_to_min, float)
        and float(quote_coverage_gap_to_min)
        >= float(EXECUTION_COST_SIDE_TARGET_ACCELERATOR_COVERAGE_GAP_THRESHOLD)
    )

    trend_worsening_raw = _safe_bool(execution_siphon_trend.get("worsening"))
    trend_worsening = bool(trend_worsening_raw) if isinstance(trend_worsening_raw, bool) else bool(
        max(0, _safe_int(execution_siphon_trend.get("worsening_component_count"))) > 0
    )
    trend_label = _text(execution_siphon_trend.get("trend_label")).lower()
    siphon_pressure_score_delta = _safe_float(execution_siphon_trend.get("siphon_pressure_score_delta"))
    uncovered_market_top5_share_delta = _safe_float(execution_siphon_trend.get("uncovered_market_top5_share_delta"))
    candidate_rows_delta = max(0, _safe_int(execution_siphon_trend.get("candidate_rows_delta")))
    worsening_component_count = max(0, _safe_int(execution_siphon_trend.get("worsening_component_count")))
    worsening_components_raw = execution_siphon_trend.get("worsening_components")
    worsening_components: set[str] = set()
    if isinstance(worsening_components_raw, list):
        worsening_components = {_text(value).lower() for value in worsening_components_raw if _text(value)}

    trend_worsening_material = bool(
        trend_worsening
        and (
            (
                isinstance(siphon_pressure_score_delta, float)
                and float(siphon_pressure_score_delta)
                >= float(EXECUTION_COST_SIDE_TARGET_ACCELERATOR_TREND_PRESSURE_DELTA_THRESHOLD)
            )
            or (
                isinstance(uncovered_market_top5_share_delta, float)
                and float(uncovered_market_top5_share_delta)
                >= float(EXECUTION_COST_SIDE_TARGET_ACCELERATOR_TREND_TOP5_SHARE_DELTA_THRESHOLD)
            )
            or (
                int(candidate_rows_delta)
                >= int(EXECUTION_COST_SIDE_TARGET_ACCELERATOR_TREND_CANDIDATE_ROWS_DELTA_THRESHOLD)
            )
            or (
                int(worsening_component_count) >= 2
                and (
                    "siphon_pressure_score" in worsening_components
                    or "uncovered_market_top5_share" in worsening_components
                )
            )
        )
    )

    side_target_candidate_set = {
        target
        for target in (
            _normalize_execution_cost_market_side_target(value) for value in list(side_target_candidates or [])
        )
        if target
    }
    side_target_row_rank: dict[str, tuple[float, int, int, int, int]] = {}
    diagnostics_rows = recommended_exclusions.get("market_side_diagnostics")
    if isinstance(diagnostics_rows, list):
        for index, raw_row in enumerate(diagnostics_rows):
            row = _as_dict(raw_row)
            target = _normalize_execution_cost_market_side_target(
                row.get("market_side_target")
                or row.get("bucket")
                or row.get("target")
                or row
            )
            if not target or target not in side_target_candidate_set:
                continue
            _, side = target.rsplit("|", 1)
            if dominant_side in {"yes", "no"} and side != dominant_side:
                continue
            share_of_uncovered_rows = _safe_float(row.get("share_of_uncovered_rows"))
            rows_without_two_sided_quote = max(0, _safe_int(row.get("rows_without_two_sided_quote")))
            low_quote_evidence = _safe_bool(row.get("low_quote_coverage_evidence"))
            wide_spread_evidence = _safe_bool(row.get("wide_spread_evidence"))
            rank_tuple = (
                float(share_of_uncovered_rows) if isinstance(share_of_uncovered_rows, float) else 0.0,
                int(rows_without_two_sided_quote),
                1 if bool(low_quote_evidence) else 0,
                1 if bool(wide_spread_evidence) else 0,
                -1 * int(index),
            )
            existing = side_target_row_rank.get(target)
            if existing is None or rank_tuple > existing:
                side_target_row_rank[target] = rank_tuple

    ranked_side_targets = sorted(
        side_target_row_rank,
        key=lambda target: (
            -float(side_target_row_rank[target][0]),
            -int(side_target_row_rank[target][1]),
            -int(side_target_row_rank[target][2]),
            -int(side_target_row_rank[target][3]),
            -int(side_target_row_rank[target][4]),
            target,
        ),
    )
    for candidate in list(side_target_candidates or []):
        normalized_candidate = _normalize_execution_cost_market_side_target(candidate)
        if not normalized_candidate:
            continue
        if normalized_candidate in ranked_side_targets:
            continue
        if normalized_candidate not in side_target_candidate_set:
            continue
        ranked_side_targets.append(normalized_candidate)

    accelerator_trigger_reasons: list[str] = []
    if quote_coverage_significantly_below_min:
        accelerator_trigger_reasons.append("quote_coverage_significantly_below_min")
    if trend_worsening_material:
        accelerator_trigger_reasons.append("execution_siphon_trend_worsening_material")

    accelerator_eligible = bool(
        dominant_side in {"yes", "no"}
        and side_pressure_materially_high
        and bool(accelerator_trigger_reasons)
        and bool(ranked_side_targets)
    )
    target_limit = min(
        int(EXECUTION_COST_SIDE_TARGET_ACCELERATOR_MAX_TARGETS),
        max(0, len(ranked_side_targets)),
    )

    return {
        "eligible": bool(accelerator_eligible),
        "dominant_side": dominant_side,
        "dominant_side_share": (
            round(float(dominant_side_share), 6) if isinstance(dominant_side_share, float) else None
        ),
        "side_imbalance_magnitude": (
            round(float(side_imbalance_magnitude), 6)
            if isinstance(side_imbalance_magnitude, float)
            else None
        ),
        "side_pressure_score": round(float(side_pressure_score), 6) if isinstance(side_pressure_score, float) else None,
        "side_pressure_materially_high": bool(side_pressure_materially_high),
        "quote_coverage_ratio": (
            round(float(quote_coverage_ratio), 6) if isinstance(quote_coverage_ratio, float) else None
        ),
        "min_quote_coverage_ratio": (
            round(float(min_quote_coverage_ratio), 6) if isinstance(min_quote_coverage_ratio, float) else None
        ),
        "quote_coverage_gap_to_min": (
            round(float(quote_coverage_gap_to_min), 6)
            if isinstance(quote_coverage_gap_to_min, float)
            else None
        ),
        "quote_coverage_significantly_below_min": bool(quote_coverage_significantly_below_min),
        "execution_siphon_trend_label": trend_label,
        "execution_siphon_trend_worsening": bool(trend_worsening),
        "execution_siphon_trend_worsening_material": bool(trend_worsening_material),
        "execution_siphon_trend_worsening_component_count": int(worsening_component_count),
        "execution_siphon_trend_worsening_components": sorted(worsening_components),
        "execution_siphon_trend_siphon_pressure_score_delta": (
            round(float(siphon_pressure_score_delta), 6)
            if isinstance(siphon_pressure_score_delta, float)
            else None
        ),
        "execution_siphon_trend_uncovered_market_top5_share_delta": (
            round(float(uncovered_market_top5_share_delta), 6)
            if isinstance(uncovered_market_top5_share_delta, float)
            else None
        ),
        "execution_siphon_trend_candidate_rows_delta": int(candidate_rows_delta),
        "trigger_reasons": list(accelerator_trigger_reasons),
        "target_limit": int(target_limit),
        "ranked_side_target_count": int(len(ranked_side_targets)),
        "ranked_side_targets": list(ranked_side_targets),
    }


def _resolve_stateful_execution_cost_exclusion_tickers(
    output_dir: Path,
) -> tuple[list[str], list[str], dict[str, Any], list[str], list[str]]:
    candidates = _load_execution_cost_exclusion_tickers(output_dir)
    side_target_candidates_raw = _load_execution_cost_exclusion_market_side_targets(output_dir)
    side_target_candidates: list[str] = []
    side_target_seen: set[str] = set()
    candidate_ticker_set = {
        ticker for ticker in (_normalize_execution_cost_exclusion_ticker(value) for value in candidates) if ticker
    }
    for raw_target in side_target_candidates_raw:
        target = _normalize_execution_cost_market_side_target(raw_target)
        if not target or target in side_target_seen:
            continue
        ticker, _ = target.rsplit("|", 1)
        if ticker not in candidate_ticker_set:
            continue
        side_target_seen.add(target)
        side_target_candidates.append(target)
    execution_cost_tape_payload = _load_latest_execution_cost_tape_payload(output_dir)
    side_target_accelerator_context = _build_execution_cost_side_target_accelerator_context(
        payload=execution_cost_tape_payload,
        side_target_candidates=side_target_candidates,
    )

    state = _load_execution_cost_exclusions_state(output_dir)
    state_path = _execution_cost_exclusions_state_artifact(output_dir)
    current_run = max(1, int(_safe_int(state.get("run_count")) or 0) + 1)
    previous_candidate_count = max(0, _safe_int(state.get("candidate_count")))
    previous_active_count = max(0, _safe_int(state.get("active_count")))
    previous_market_side_candidate_count = max(0, _safe_int(state.get("candidate_market_side_target_count")))
    previous_market_side_active_count = max(0, _safe_int(state.get("active_market_side_target_count")))
    previous_quote_coverage_ratio = _safe_float(state.get("last_quote_coverage_ratio"))
    previous_min_quote_coverage_ratio = _safe_float(state.get("last_min_quote_coverage_ratio"))
    tracked_tickers = _as_dict(state.get("tracked_tickers"))
    tracked_market_side_targets = _as_dict(state.get("tracked_market_side_targets"))
    adaptive_downshift_state = _as_dict(state.get("adaptive_downshift"))
    suppressed_tickers_raw = _as_dict(adaptive_downshift_state.get("suppressed_tickers"))
    suppressed_market_side_targets_raw = _as_dict(adaptive_downshift_state.get("suppressed_market_side_targets"))
    active_suppressed_tickers: dict[str, dict[str, Any]] = {}
    active_suppressed_market_side_targets: dict[str, dict[str, Any]] = {}
    for raw_ticker, raw_entry in suppressed_tickers_raw.items():
        ticker = _normalize_execution_cost_exclusion_ticker(raw_ticker)
        if not ticker:
            continue
        entry = _as_dict(raw_entry)
        suppressed_until_run = max(0, _safe_int(entry.get("suppressed_until_run")))
        if suppressed_until_run < current_run:
            continue
        active_suppressed_tickers[ticker] = {
            "suppressed_until_run": int(suppressed_until_run),
            "applied_run": max(0, _safe_int(entry.get("applied_run"))),
            "reason": _text(entry.get("reason")),
        }
    for raw_target, raw_entry in suppressed_market_side_targets_raw.items():
        target = _normalize_execution_cost_market_side_target(raw_target)
        if not target:
            continue
        entry = _as_dict(raw_entry)
        suppressed_until_run = max(0, _safe_int(entry.get("suppressed_until_run")))
        if suppressed_until_run < current_run:
            continue
        active_suppressed_market_side_targets[target] = {
            "suppressed_until_run": int(suppressed_until_run),
            "applied_run": max(0, _safe_int(entry.get("applied_run"))),
            "reason": _text(entry.get("reason")),
        }
    raw_last_drop_tickers = adaptive_downshift_state.get("last_drop_tickers")
    if not isinstance(raw_last_drop_tickers, list):
        raw_last_drop_tickers = []
    raw_last_drop_market_side_targets = adaptive_downshift_state.get("last_drop_market_side_targets")
    if not isinstance(raw_last_drop_market_side_targets, list):
        raw_last_drop_market_side_targets = []
    candidate_positions = {ticker: index for index, ticker in enumerate(candidates)}
    all_tickers = sorted(set(tracked_tickers) | set(candidates))
    updated_tracked_tickers: dict[str, dict[str, Any]] = {}

    for ticker in all_tickers:
        entry = _as_dict(tracked_tickers.get(ticker))
        consecutive_seen_runs = max(0, _safe_int(entry.get("consecutive_seen_runs")))
        consecutive_missing_runs = max(0, _safe_int(entry.get("consecutive_missing_runs")))
        first_seen_run = max(0, _safe_int(entry.get("first_seen_run")))
        last_seen_run = max(0, _safe_int(entry.get("last_seen_run")))
        last_active_run = max(0, _safe_int(entry.get("last_active_run")))
        active = bool(entry.get("active"))
        seen_now = ticker in candidate_positions

        if seen_now:
            if consecutive_missing_runs > 0:
                consecutive_seen_runs = 1
            else:
                consecutive_seen_runs = max(1, consecutive_seen_runs + 1)
            consecutive_missing_runs = 0
            if first_seen_run <= 0:
                first_seen_run = current_run
            last_seen_run = current_run
            if consecutive_seen_runs >= EXECUTION_COST_EXCLUSIONS_STATE_ACTIVATION_RUNS:
                active = True
                last_active_run = current_run
            elif not bool(entry):
                active = False
        else:
            consecutive_missing_runs = max(1, consecutive_missing_runs + 1)
            consecutive_seen_runs = 0
            if active and consecutive_missing_runs >= EXECUTION_COST_EXCLUSIONS_STATE_DEACTIVATION_MISSING_RUNS:
                active = False

        updated_tracked_tickers[ticker] = {
            "active": bool(active),
            "first_seen_run": int(first_seen_run),
            "last_seen_run": int(last_seen_run),
            "last_active_run": int(last_active_run),
            "consecutive_seen_runs": int(consecutive_seen_runs),
            "consecutive_missing_runs": int(consecutive_missing_runs),
            "current_candidate_rank": int(candidate_positions.get(ticker, -1)),
        }

    active_tickers_sorted = sorted(
        (ticker for ticker, entry in updated_tracked_tickers.items() if bool(entry.get("active"))),
        key=lambda ticker: (
            0 if ticker in candidate_positions else 1,
            candidate_positions.get(ticker, 10**9),
            -int(_safe_int(updated_tracked_tickers[ticker].get("last_seen_run"))),
            -int(_safe_int(updated_tracked_tickers[ticker].get("last_active_run"))),
            ticker,
        ),
    )
    active_tickers_capped = list(active_tickers_sorted[:EXECUTION_COST_EXCLUSIONS_STATE_MAX_ACTIVE])
    suppressed_active_tickers_current_run: list[str] = []
    active_tickers: list[str] = []
    for ticker in active_tickers_capped:
        suppressed_entry = _as_dict(active_suppressed_tickers.get(ticker))
        suppressed_until_run = max(0, _safe_int(suppressed_entry.get("suppressed_until_run")))
        if suppressed_until_run >= current_run:
            suppressed_active_tickers_current_run.append(ticker)
            continue
        active_tickers.append(ticker)

    for ticker in list(updated_tracked_tickers):
        updated_tracked_tickers[ticker]["active"] = ticker in active_tickers

    side_target_accelerator_eligible = bool(side_target_accelerator_context.get("eligible"))
    side_target_accelerator_ranked_targets_raw = side_target_accelerator_context.get("ranked_side_targets")
    side_target_accelerator_ranked_targets: list[str] = []
    side_target_accelerator_ranked_targets_seen: set[str] = set()
    if isinstance(side_target_accelerator_ranked_targets_raw, list):
        for raw_target in side_target_accelerator_ranked_targets_raw:
            target = _normalize_execution_cost_market_side_target(raw_target)
            if not target or target in side_target_accelerator_ranked_targets_seen:
                continue
            if target not in side_target_seen:
                continue
            side_target_accelerator_ranked_targets_seen.add(target)
            side_target_accelerator_ranked_targets.append(target)
    if not side_target_accelerator_ranked_targets:
        side_target_accelerator_ranked_targets = list(side_target_candidates)
    side_target_accelerator_target_limit = max(
        0,
        min(
            max(0, _safe_int(side_target_accelerator_context.get("target_limit"))),
            len(side_target_accelerator_ranked_targets),
        ),
    )
    side_target_accelerator_candidate_targets = list(
        side_target_accelerator_ranked_targets[:side_target_accelerator_target_limit]
    )
    side_target_accelerator_candidate_targets = [
        target
        for target in side_target_accelerator_candidate_targets
        if target not in active_suppressed_market_side_targets
    ]
    side_target_accelerator_candidate_target_set = set(side_target_accelerator_candidate_targets)

    side_target_candidate_positions = {target: index for index, target in enumerate(side_target_candidates)}
    all_market_side_targets = sorted(set(tracked_market_side_targets) | set(side_target_candidates))
    updated_tracked_market_side_targets: dict[str, dict[str, Any]] = {}
    accelerated_market_side_targets: list[str] = []
    for target in all_market_side_targets:
        entry = _as_dict(tracked_market_side_targets.get(target))
        consecutive_seen_runs = max(0, _safe_int(entry.get("consecutive_seen_runs")))
        consecutive_missing_runs = max(0, _safe_int(entry.get("consecutive_missing_runs")))
        first_seen_run = max(0, _safe_int(entry.get("first_seen_run")))
        last_seen_run = max(0, _safe_int(entry.get("last_seen_run")))
        last_active_run = max(0, _safe_int(entry.get("last_active_run")))
        active = bool(entry.get("active"))
        seen_now = target in side_target_candidate_positions

        if seen_now:
            if consecutive_missing_runs > 0:
                consecutive_seen_runs = 1
            else:
                consecutive_seen_runs = max(1, consecutive_seen_runs + 1)
            consecutive_missing_runs = 0
            if first_seen_run <= 0:
                first_seen_run = current_run
            last_seen_run = current_run
            if consecutive_seen_runs >= EXECUTION_COST_EXCLUSIONS_STATE_ACTIVATION_RUNS:
                active = True
                last_active_run = current_run
            elif not bool(entry):
                active = False
        else:
            consecutive_missing_runs = max(1, consecutive_missing_runs + 1)
            consecutive_seen_runs = 0
            if active and consecutive_missing_runs >= EXECUTION_COST_EXCLUSIONS_STATE_DEACTIVATION_MISSING_RUNS:
                active = False

        accelerated_now = False
        if (
            side_target_accelerator_eligible
            and seen_now
            and target in side_target_accelerator_candidate_target_set
            and not active
            and consecutive_seen_runs > 0
            and consecutive_seen_runs < EXECUTION_COST_EXCLUSIONS_STATE_ACTIVATION_RUNS
        ):
            active = True
            last_active_run = current_run
            accelerated_now = True
            accelerated_market_side_targets.append(target)

        updated_tracked_market_side_targets[target] = {
            "active": bool(active),
            "first_seen_run": int(first_seen_run),
            "last_seen_run": int(last_seen_run),
            "last_active_run": int(last_active_run),
            "consecutive_seen_runs": int(consecutive_seen_runs),
            "consecutive_missing_runs": int(consecutive_missing_runs),
            "current_candidate_rank": int(side_target_candidate_positions.get(target, -1)),
            "accelerated_this_run": bool(accelerated_now),
        }

    active_market_side_targets_sorted = sorted(
        (
            target
            for target, entry in updated_tracked_market_side_targets.items()
            if bool(entry.get("active"))
        ),
        key=lambda target: (
            0 if target in side_target_candidate_positions else 1,
            side_target_candidate_positions.get(target, 10**9),
            -int(_safe_int(updated_tracked_market_side_targets[target].get("last_seen_run"))),
            -int(_safe_int(updated_tracked_market_side_targets[target].get("last_active_run"))),
            target,
        ),
    )
    active_market_side_targets_capped = list(
        active_market_side_targets_sorted[:EXECUTION_COST_EXCLUSIONS_STATE_MAX_ACTIVE_MARKET_SIDE_TARGETS]
    )
    suppressed_active_market_side_targets_current_run: list[str] = []
    active_market_side_targets: list[str] = []
    for target in active_market_side_targets_capped:
        suppressed_entry = _as_dict(active_suppressed_market_side_targets.get(target))
        suppressed_until_run = max(0, _safe_int(suppressed_entry.get("suppressed_until_run")))
        if suppressed_until_run >= current_run:
            suppressed_active_market_side_targets_current_run.append(target)
            continue
        active_market_side_targets.append(target)

    for target in list(updated_tracked_market_side_targets):
        updated_tracked_market_side_targets[target]["active"] = target in active_market_side_targets
    accelerated_market_side_targets_set = set(accelerated_market_side_targets)
    for target in list(updated_tracked_market_side_targets):
        if target not in accelerated_market_side_targets_set:
            updated_tracked_market_side_targets[target]["accelerated_this_run"] = False

    state_payload = {
        "status": "ready",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "run_count": current_run,
        "candidate_count": int(len(candidates)),
        "active_count": int(len(active_tickers)),
        "candidate_tickers": list(candidates),
        "active_tickers": list(active_tickers),
        "tracked_tickers": dict(sorted(updated_tracked_tickers.items(), key=lambda item: item[0])),
        "candidate_market_side_target_count": int(len(side_target_candidates)),
        "active_market_side_target_count": int(len(active_market_side_targets)),
        "candidate_market_side_targets": list(side_target_candidates),
        "active_market_side_targets": list(active_market_side_targets),
        "tracked_market_side_targets": dict(
            sorted(updated_tracked_market_side_targets.items(), key=lambda item: item[0])
        ),
        "previous_candidate_count": int(previous_candidate_count),
        "previous_active_count": int(previous_active_count),
        "previous_market_side_candidate_count": int(previous_market_side_candidate_count),
        "previous_market_side_active_count": int(previous_market_side_active_count),
        "previous_quote_coverage_ratio": previous_quote_coverage_ratio,
        "previous_min_quote_coverage_ratio": previous_min_quote_coverage_ratio,
        "market_side_activation_accelerator": {
            "engaged": bool(side_target_accelerator_eligible),
            "dominant_side": _text(side_target_accelerator_context.get("dominant_side")),
            "side_pressure_materially_high": bool(side_target_accelerator_context.get("side_pressure_materially_high")),
            "quote_coverage_significantly_below_min": bool(
                side_target_accelerator_context.get("quote_coverage_significantly_below_min")
            ),
            "execution_siphon_trend_worsening_material": bool(
                side_target_accelerator_context.get("execution_siphon_trend_worsening_material")
            ),
            "trigger_reasons": [
                _text(value)
                for value in list(side_target_accelerator_context.get("trigger_reasons") or [])
                if _text(value)
            ],
            "target_limit": int(side_target_accelerator_target_limit),
            "candidate_ranked_target_count": int(len(side_target_accelerator_ranked_targets)),
            "candidate_ranked_targets": list(side_target_accelerator_ranked_targets[:20]),
            "candidate_selected_target_count": int(len(side_target_accelerator_candidate_targets)),
            "candidate_selected_targets": list(side_target_accelerator_candidate_targets[:20]),
            "accelerated_market_side_target_count": int(len(accelerated_market_side_targets)),
            "accelerated_market_side_targets": list(accelerated_market_side_targets[:20]),
        },
        "adaptive_downshift": {
            "last_evaluated_run": max(0, _safe_int(adaptive_downshift_state.get("last_evaluated_run"))),
            "last_downshift_run": max(0, _safe_int(adaptive_downshift_state.get("last_downshift_run"))),
            "last_decision": _text(adaptive_downshift_state.get("last_decision")) or "not_evaluated",
            "last_reason": _text(adaptive_downshift_state.get("last_reason")),
            "suppressed_tickers": dict(sorted(active_suppressed_tickers.items(), key=lambda item: item[0])),
            "suppressed_market_side_targets": dict(
                sorted(active_suppressed_market_side_targets.items(), key=lambda item: item[0])
            ),
            "last_probe_metrics": _as_dict(adaptive_downshift_state.get("last_probe_metrics")),
            "last_coverage_metrics": _as_dict(adaptive_downshift_state.get("last_coverage_metrics")),
            "last_active_count_before": max(0, _safe_int(adaptive_downshift_state.get("last_active_count_before"))),
            "last_active_count_after": max(0, _safe_int(adaptive_downshift_state.get("last_active_count_after"))),
            "last_drop_count": max(0, _safe_int(adaptive_downshift_state.get("last_drop_count"))),
            "last_drop_tickers": [
                ticker
                for ticker in (_normalize_execution_cost_exclusion_ticker(value) for value in raw_last_drop_tickers)
                if ticker
            ],
            "last_active_market_side_target_count_before": max(
                0,
                _safe_int(adaptive_downshift_state.get("last_active_market_side_target_count_before")),
            ),
            "last_active_market_side_target_count_after": max(
                0,
                _safe_int(adaptive_downshift_state.get("last_active_market_side_target_count_after")),
            ),
            "last_drop_market_side_target_count": max(
                0,
                _safe_int(adaptive_downshift_state.get("last_drop_market_side_target_count")),
            ),
            "last_drop_market_side_targets": [
                target
                for target in (
                    _normalize_execution_cost_market_side_target(value)
                    for value in raw_last_drop_market_side_targets
                )
                if target
            ],
            "suppressed_ticker_count": int(len(active_suppressed_tickers)),
            "suppressed_active_tickers_current_run": list(suppressed_active_tickers_current_run),
            "suppressed_market_side_target_count": int(len(active_suppressed_market_side_targets)),
            "suppressed_active_market_side_targets_current_run": list(
                suppressed_active_market_side_targets_current_run
            ),
        },
    }
    _write_text_atomic(state_path, json.dumps(state_payload, indent=2, sort_keys=True))
    return active_tickers, active_market_side_targets, state_payload, candidates, side_target_candidates


def _apply_execution_cost_exclusion_downshift(
    *,
    output_dir: Path,
    state_payload: dict[str, Any],
    active_tickers: list[str],
    active_market_side_targets: list[str],
    first_probe_status: str,
    first_probe_intents_total: int,
    first_probe_intents_approved: int,
    second_probe_triggered: bool,
    second_probe_status: str,
    second_probe_intents_total: int,
    second_probe_intents_approved: int,
) -> tuple[list[str], list[str], dict[str, Any], dict[str, Any]]:
    state_path = _execution_cost_exclusions_state_artifact(output_dir)
    run_count = max(1, _safe_int(state_payload.get("run_count")))
    active_count_before = int(len(active_tickers))
    active_market_side_target_count_before = int(len(active_market_side_targets))
    near_cap_threshold = max(
        int(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_MIN_ACTIVE),
        int(math.ceil(float(EXECUTION_COST_EXCLUSIONS_STATE_MAX_ACTIVE) * float(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_NEAR_CAP_RATIO))),
    )
    pressure_near_cap = active_count_before >= near_cap_threshold
    first_probe_blocked = first_probe_status == "no_candidates" or int(first_probe_intents_approved) <= 0
    second_probe_improved = bool(
        second_probe_triggered
        and (
            int(second_probe_intents_approved) > int(first_probe_intents_approved)
            or int(second_probe_intents_total) > int(first_probe_intents_total)
            or (
                first_probe_status == "no_candidates"
                and _text(second_probe_status).lower() not in {"", "no_candidates"}
            )
        )
    )
    throughput_stalled = bool(first_probe_blocked and not second_probe_improved)

    adaptive_downshift_state = _as_dict(state_payload.get("adaptive_downshift"))
    last_downshift_run = max(0, _safe_int(adaptive_downshift_state.get("last_downshift_run")))
    runs_since_last_downshift = run_count - last_downshift_run if last_downshift_run > 0 else 10**9
    cooldown_active = bool(last_downshift_run > 0 and runs_since_last_downshift < EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_COOLDOWN_RUNS)
    cooldown_runs_remaining = (
        int(max(0, int(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_COOLDOWN_RUNS) - int(runs_since_last_downshift)))
        if cooldown_active
        else 0
    )

    coverage_metrics = _load_execution_cost_tape_quality_metrics(output_dir)
    quote_coverage_ratio = _safe_float(coverage_metrics.get("quote_coverage_ratio"))
    min_quote_coverage_ratio = _safe_float(coverage_metrics.get("min_quote_coverage_ratio"))
    prior_coverage_metrics = _as_dict(adaptive_downshift_state.get("last_coverage_metrics"))
    previous_quote_coverage_ratio = _safe_float(prior_coverage_metrics.get("quote_coverage_ratio"))
    coverage_signal_available = isinstance(quote_coverage_ratio, float) and isinstance(min_quote_coverage_ratio, float)
    coverage_below_min = bool(
        coverage_signal_available and float(quote_coverage_ratio) + float(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_COVERAGE_EPSILON) < float(min_quote_coverage_ratio)
    )
    coverage_improved_vs_previous = bool(
        coverage_signal_available
        and isinstance(previous_quote_coverage_ratio, float)
        and float(quote_coverage_ratio) > float(previous_quote_coverage_ratio) + float(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_COVERAGE_EPSILON)
    )
    coverage_stalled = bool(
        (not coverage_signal_available and first_probe_blocked)
        or (coverage_below_min and not coverage_improved_vs_previous)
    )

    should_downshift = bool(
        active_count_before > 1
        and pressure_near_cap
        and throughput_stalled
        and coverage_stalled
        and not cooldown_active
    )

    decision_reason = ""
    if active_count_before <= 1:
        decision_reason = "insufficient_active_exclusions"
    elif not pressure_near_cap:
        decision_reason = "pressure_below_near_cap_threshold"
    elif cooldown_active:
        decision_reason = "downshift_cooldown_active"
    elif not throughput_stalled:
        decision_reason = "throughput_improving_or_not_blocked"
    elif not coverage_stalled:
        decision_reason = "coverage_improving_or_not_under_min"
    else:
        decision_reason = "high_pressure_without_throughput_or_coverage_improvement"

    drop_count = 0
    dropped_tickers: list[str] = []
    dropped_market_side_targets: list[str] = []
    next_active_tickers = list(active_tickers)
    next_active_market_side_targets = list(active_market_side_targets)
    if should_downshift:
        drop_count = max(1, int(math.ceil(float(active_count_before) * float(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_DROP_FRACTION))))
        drop_count = min(drop_count, max(0, active_count_before - 1))
        if drop_count > 0:
            keep_count = max(1, active_count_before - drop_count)
            next_active_tickers = list(active_tickers[:keep_count])
            dropped_tickers = list(active_tickers[keep_count:])
            drop_count = int(len(dropped_tickers))
        if active_market_side_target_count_before > 1:
            dropped_ticker_set = set(dropped_tickers)
            retained_market_side_targets: list[str] = []
            for target in active_market_side_targets:
                ticker = _normalize_execution_cost_exclusion_ticker(target.rsplit("|", 1)[0])
                if ticker and ticker in dropped_ticker_set:
                    dropped_market_side_targets.append(target)
                else:
                    retained_market_side_targets.append(target)
            desired_market_side_target_drop_count = max(
                1,
                int(
                    math.ceil(
                        float(active_market_side_target_count_before)
                        * float(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_DROP_FRACTION)
                    )
                ),
            )
            desired_market_side_target_drop_count = min(
                desired_market_side_target_drop_count,
                max(0, active_market_side_target_count_before - 1),
            )
            additional_drop_count = max(0, desired_market_side_target_drop_count - len(dropped_market_side_targets))
            if additional_drop_count > 0 and retained_market_side_targets:
                additional_drop = retained_market_side_targets[-additional_drop_count:]
                retained_market_side_targets = retained_market_side_targets[:-additional_drop_count]
                for target in additional_drop:
                    if target not in dropped_market_side_targets:
                        dropped_market_side_targets.append(target)
            next_active_market_side_targets = retained_market_side_targets

    raw_suppressed_tickers = _as_dict(adaptive_downshift_state.get("suppressed_tickers"))
    raw_suppressed_market_side_targets = _as_dict(adaptive_downshift_state.get("suppressed_market_side_targets"))
    suppressed_tickers: dict[str, dict[str, Any]] = {}
    suppressed_market_side_targets: dict[str, dict[str, Any]] = {}
    for raw_ticker, raw_entry in raw_suppressed_tickers.items():
        ticker = _normalize_execution_cost_exclusion_ticker(raw_ticker)
        if not ticker:
            continue
        entry = _as_dict(raw_entry)
        suppressed_until_run = max(0, _safe_int(entry.get("suppressed_until_run")))
        if suppressed_until_run < run_count:
            continue
        suppressed_tickers[ticker] = {
            "suppressed_until_run": int(suppressed_until_run),
            "applied_run": max(0, _safe_int(entry.get("applied_run"))),
            "reason": _text(entry.get("reason")),
        }
    for raw_target, raw_entry in raw_suppressed_market_side_targets.items():
        target = _normalize_execution_cost_market_side_target(raw_target)
        if not target:
            continue
        entry = _as_dict(raw_entry)
        suppressed_until_run = max(0, _safe_int(entry.get("suppressed_until_run")))
        if suppressed_until_run < run_count:
            continue
        suppressed_market_side_targets[target] = {
            "suppressed_until_run": int(suppressed_until_run),
            "applied_run": max(0, _safe_int(entry.get("applied_run"))),
            "reason": _text(entry.get("reason")),
        }

    if should_downshift and dropped_tickers:
        suppressed_until_run = run_count + int(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_HOLD_RUNS)
        for ticker in dropped_tickers:
            suppressed_tickers[ticker] = {
                "suppressed_until_run": int(suppressed_until_run),
                "applied_run": int(run_count),
                "reason": "execution_friction_pressure_downshift",
            }
    if should_downshift and dropped_market_side_targets:
        suppressed_until_run = run_count + int(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_HOLD_RUNS)
        for target in dropped_market_side_targets:
            suppressed_market_side_targets[target] = {
                "suppressed_until_run": int(suppressed_until_run),
                "applied_run": int(run_count),
                "reason": "execution_friction_pressure_downshift",
            }

    tracked_tickers = {
        ticker: _as_dict(entry)
        for ticker, entry in _as_dict(state_payload.get("tracked_tickers")).items()
    }
    for ticker in list(tracked_tickers):
        tracked_tickers[ticker]["active"] = ticker in next_active_tickers
    tracked_market_side_targets = {
        target: _as_dict(entry)
        for target, entry in _as_dict(state_payload.get("tracked_market_side_targets")).items()
    }
    for target in list(tracked_market_side_targets):
        tracked_market_side_targets[target]["active"] = target in next_active_market_side_targets

    probe_metrics = {
        "first_probe_status": _text(first_probe_status).lower(),
        "first_probe_intents_total": int(max(0, first_probe_intents_total)),
        "first_probe_intents_approved": int(max(0, first_probe_intents_approved)),
        "second_probe_triggered": bool(second_probe_triggered),
        "second_probe_status": _text(second_probe_status).lower(),
        "second_probe_intents_total": int(max(0, second_probe_intents_total)),
        "second_probe_intents_approved": int(max(0, second_probe_intents_approved)),
    }
    persisted_coverage_metrics = {
        "quote_coverage_ratio": round(float(quote_coverage_ratio), 6) if isinstance(quote_coverage_ratio, float) else None,
        "min_quote_coverage_ratio": round(float(min_quote_coverage_ratio), 6)
        if isinstance(min_quote_coverage_ratio, float)
        else None,
        "previous_quote_coverage_ratio": round(float(previous_quote_coverage_ratio), 6)
        if isinstance(previous_quote_coverage_ratio, float)
        else None,
        "coverage_signal_available": bool(coverage_signal_available),
        "coverage_below_min": bool(coverage_below_min),
        "coverage_improved_vs_previous": bool(coverage_improved_vs_previous),
        "coverage_stalled": bool(coverage_stalled),
    }

    updated_state_payload = dict(state_payload)
    updated_state_payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    updated_state_payload["active_tickers"] = list(next_active_tickers)
    updated_state_payload["active_count"] = int(len(next_active_tickers))
    updated_state_payload["tracked_tickers"] = dict(sorted(tracked_tickers.items(), key=lambda item: item[0]))
    updated_state_payload["active_market_side_targets"] = list(next_active_market_side_targets)
    updated_state_payload["active_market_side_target_count"] = int(len(next_active_market_side_targets))
    updated_state_payload["tracked_market_side_targets"] = dict(
        sorted(tracked_market_side_targets.items(), key=lambda item: item[0])
    )
    updated_state_payload["last_quote_coverage_ratio"] = (
        round(float(quote_coverage_ratio), 6) if isinstance(quote_coverage_ratio, float) else None
    )
    updated_state_payload["last_min_quote_coverage_ratio"] = (
        round(float(min_quote_coverage_ratio), 6) if isinstance(min_quote_coverage_ratio, float) else None
    )
    updated_state_payload["adaptive_downshift"] = {
        "last_evaluated_run": int(run_count),
        "last_downshift_run": int(run_count) if should_downshift and drop_count > 0 else int(last_downshift_run),
        "last_decision": "downshift_applied" if should_downshift and drop_count > 0 else "no_downshift",
        "last_reason": _text(decision_reason),
        "suppressed_tickers": dict(sorted(suppressed_tickers.items(), key=lambda item: item[0])),
        "suppressed_market_side_targets": dict(
            sorted(suppressed_market_side_targets.items(), key=lambda item: item[0])
        ),
        "last_probe_metrics": probe_metrics,
        "last_coverage_metrics": persisted_coverage_metrics,
        "last_active_count_before": int(active_count_before),
        "last_active_count_after": int(len(next_active_tickers)),
        "last_drop_count": int(drop_count),
        "last_drop_tickers": list(dropped_tickers),
        "last_active_market_side_target_count_before": int(active_market_side_target_count_before),
        "last_active_market_side_target_count_after": int(len(next_active_market_side_targets)),
        "last_drop_market_side_target_count": int(len(dropped_market_side_targets)),
        "last_drop_market_side_targets": list(dropped_market_side_targets),
        "suppressed_ticker_count": int(len(suppressed_tickers)),
        "suppressed_active_tickers_current_run": [ticker for ticker in active_tickers if ticker in suppressed_tickers],
        "suppressed_market_side_target_count": int(len(suppressed_market_side_targets)),
        "suppressed_active_market_side_targets_current_run": [
            target for target in active_market_side_targets if target in suppressed_market_side_targets
        ],
        "downshift_hold_runs": int(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_HOLD_RUNS),
        "downshift_cooldown_runs": int(EXECUTION_COST_EXCLUSIONS_DOWNSHIFT_COOLDOWN_RUNS),
    }
    _write_text_atomic(state_path, json.dumps(updated_state_payload, indent=2, sort_keys=True))

    downshift_evidence = {
        "decision": "downshift_applied" if should_downshift and drop_count > 0 else "no_downshift",
        "reason": _text(decision_reason),
        "triggered": bool(should_downshift and drop_count > 0),
        "active_count_before": int(active_count_before),
        "active_count_after": int(len(next_active_tickers)),
        "drop_count": int(drop_count),
        "dropped_tickers": list(dropped_tickers),
        "near_cap_threshold": int(near_cap_threshold),
        "pressure_near_cap": bool(pressure_near_cap),
        "throughput_stalled": bool(throughput_stalled),
        "second_probe_improved": bool(second_probe_improved),
        "first_probe_blocked": bool(first_probe_blocked),
        "coverage_stalled": bool(coverage_stalled),
        "coverage_signal_available": bool(coverage_signal_available),
        "coverage_below_min": bool(coverage_below_min),
        "coverage_improved_vs_previous": bool(coverage_improved_vs_previous),
        "quote_coverage_ratio": round(float(quote_coverage_ratio), 6) if isinstance(quote_coverage_ratio, float) else None,
        "min_quote_coverage_ratio": round(float(min_quote_coverage_ratio), 6)
        if isinstance(min_quote_coverage_ratio, float)
        else None,
        "previous_quote_coverage_ratio": round(float(previous_quote_coverage_ratio), 6)
        if isinstance(previous_quote_coverage_ratio, float)
        else None,
        "cooldown_active": bool(cooldown_active),
        "cooldown_runs_remaining": int(cooldown_runs_remaining),
        "suppressed_ticker_count": int(len(suppressed_tickers)),
        "active_market_side_target_count_before": int(active_market_side_target_count_before),
        "active_market_side_target_count_after": int(len(next_active_market_side_targets)),
        "drop_market_side_target_count": int(len(dropped_market_side_targets)),
        "dropped_market_side_targets": list(dropped_market_side_targets),
        "suppressed_market_side_target_count": int(len(suppressed_market_side_targets)),
    }
    return next_active_tickers, next_active_market_side_targets, updated_state_payload, downshift_evidence


def _run_shadow_trader_with_fallback(
    *,
    output_dir: str,
    env_file: str,
    **kwargs: Any,
) -> dict[str, Any]:
    trader_kwargs = dict(kwargs)
    trader_kwargs["output_dir"] = _text(trader_kwargs.get("output_dir")) or output_dir
    trader_kwargs["env_file"] = _text(trader_kwargs.get("env_file")) or env_file
    fallback_inputs = _resolve_trader_shadow_fallback_inputs(output_dir=trader_kwargs["output_dir"])
    for key, value in fallback_inputs.items():
        if not _text(trader_kwargs.get(key)):
            trader_kwargs[key] = value
    payload = run_kalshi_temperature_trader(**trader_kwargs)
    return dict(payload) if isinstance(payload, dict) else {"status": "unknown"}


def _load_suppression_snapshot(output_dir: Path) -> dict[str, Any]:
    summary_path = _latest_trade_summary_artifact(output_dir)
    summary_payload = _load_json_file(summary_path) if isinstance(summary_path, Path) else {}
    candidate_count = max(
        0,
        _safe_int(summary_payload.get("weather_pattern_negative_regime_suppression_candidate_count")),
    )
    blocked_count = max(
        0,
        _safe_int(summary_payload.get("weather_pattern_negative_regime_suppression_blocked_count")),
    )
    blocked_share = None
    denominator = max(candidate_count, blocked_count)
    if denominator > 0:
        blocked_share = round(float(blocked_count) / float(denominator), 6)
    return {
        "summary_file_used": str(summary_path) if isinstance(summary_path, Path) else "",
        "summary_available": bool(summary_payload),
        "candidate_count": candidate_count,
        "blocked_count": blocked_count,
        "blocked_share": blocked_share,
    }


def _extract_advisor_status(payload: dict[str, Any]) -> str:
    remediation = _as_dict(payload.get("remediation_plan"))
    return _text(remediation.get("status")).lower() or "unknown"


def _extract_action_keys(payload: dict[str, Any]) -> list[str]:
    remediation = _as_dict(payload.get("remediation_plan"))
    rows = remediation.get("prioritized_actions")
    keys: list[str] = []
    if not isinstance(rows, list):
        return keys
    for row in rows:
        row_dict = _as_dict(row)
        key = _text(row_dict.get("key"))
        if key:
            keys.append(key)
    return keys


def _compute_gap_score(payload: dict[str, Any]) -> float:
    remediation = _as_dict(payload.get("remediation_plan"))
    gap = remediation.get("gap_to_clear")
    if not isinstance(gap, dict):
        return 0.0

    total = 0.0
    for value in gap.values():
        parsed = _safe_float(value)
        if not isinstance(parsed, float):
            continue
        total += max(0.0, parsed)
    return round(float(total), 6)


def _extract_negative_expectancy_attempt_share(payload: dict[str, Any]) -> float | None:
    metrics = _as_dict(payload.get("metrics"))
    weather_metrics = _as_dict(metrics.get("weather"))

    direct = _safe_float(weather_metrics.get("negative_expectancy_attempt_share"))
    if isinstance(direct, float):
        return round(float(direct), 6)

    fallback_values = [
        weather_metrics.get("negative_expectancy_attempt_share_observed"),
        weather_metrics.get("negative_expectancy_attempt_share_confidence_adjusted"),
        _as_dict(_as_dict(payload.get("profile")).get("regime_risk")).get("negative_expectancy_attempt_share"),
        _as_dict(_as_dict(payload.get("weather_pattern_profile")).get("regime_risk")).get(
            "negative_expectancy_attempt_share"
        ),
        _as_dict(_as_dict(payload.get("remediation_plan")).get("gap_to_clear")).get(
            "weather_negative_expectancy_attempt_share"
        ),
    ]
    for value in fallback_values:
        parsed = _safe_float(value)
        if isinstance(parsed, float):
            return round(float(parsed), 6)
    return None


def _resolve_contract_specs_retry_env_file(primary_env_file: str) -> str | None:
    fallback = Path(DEFAULT_TRADER_ENV_FILE)
    primary = Path(_text(primary_env_file))
    if primary == fallback:
        return None
    if not fallback.is_file():
        return None
    return str(fallback)


def _resolve_demoted_source_replacement_route(
    *,
    action_key: str,
    policy_class: str,
    demoted_actions: set[str],
    auto_disabled_actions: set[str],
    replacement_sources_executed: set[str],
    action_effectiveness: dict[str, dict[str, int | float]],
) -> dict[str, Any]:
    strict_replacement_action_key = _text(REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(action_key))
    tertiary_replacement_action_key = _text(
        TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(action_key)
        or TERTIARY_REPLACEMENT_ACTION_BY_POLICY_CLASS.get(policy_class)
    )
    source_replaceable = _is_source_action_replaceable(action_key, policy_class)
    final_fallback_action_key = (
        _text(
            FINAL_FALLBACK_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.get(action_key)
            or FINAL_FALLBACK_REPLACEMENT_ACTION_BY_POLICY_CLASS.get(policy_class)
            or DEFAULT_FINAL_FALLBACK_REPLACEMENT_ACTION_KEY
        )
        if source_replaceable
        else ""
    )
    already_routed = action_key in replacement_sources_executed

    strict_replacement_action_viable = bool(
        strict_replacement_action_key
        and strict_replacement_action_key not in demoted_actions
        and strict_replacement_action_key not in auto_disabled_actions
    )
    tertiary_replacement_action_viable = bool(
        tertiary_replacement_action_key
        and tertiary_replacement_action_key not in demoted_actions
        and tertiary_replacement_action_key not in auto_disabled_actions
    )
    final_fallback_action_viable = bool(
        final_fallback_action_key
        and final_fallback_action_key not in demoted_actions
        and final_fallback_action_key not in auto_disabled_actions
    )

    strict_unavailable_reason = ""
    if strict_replacement_action_key and not strict_replacement_action_viable:
        if strict_replacement_action_key in demoted_actions:
            strict_unavailable_reason = "replacement_action_demoted"
        elif strict_replacement_action_key in auto_disabled_actions:
            strict_unavailable_reason = "replacement_action_auto_disabled"
        else:
            strict_unavailable_reason = "replacement_action_unavailable"
    elif strict_replacement_action_key and already_routed:
        strict_unavailable_reason = "replacement_already_executed_for_source"
    elif strict_replacement_action_key and not source_replaceable:
        strict_unavailable_reason = (
            "source_action_not_replaceable"
            if action_key in RISKY_EDGE_RELAX_ACTION_KEYS
            else "source_policy_not_replaceable"
        )
    elif not strict_replacement_action_key:
        strict_unavailable_reason = "no_replacement_mapping"

    tertiary_unavailable_reason = ""
    if tertiary_replacement_action_key and not tertiary_replacement_action_viable:
        if tertiary_replacement_action_key in demoted_actions:
            tertiary_unavailable_reason = "tertiary_replacement_action_demoted"
        elif tertiary_replacement_action_key in auto_disabled_actions:
            tertiary_unavailable_reason = "tertiary_replacement_action_auto_disabled"
        else:
            tertiary_unavailable_reason = "tertiary_replacement_action_unavailable"
    elif tertiary_replacement_action_key and already_routed:
        tertiary_unavailable_reason = "tertiary_replacement_already_executed_for_source"
    elif tertiary_replacement_action_key and not source_replaceable:
        tertiary_unavailable_reason = (
            "source_action_not_replaceable"
            if action_key in RISKY_EDGE_RELAX_ACTION_KEYS
            else "source_policy_not_replaceable"
        )
    elif not tertiary_replacement_action_key:
        tertiary_unavailable_reason = "no_tertiary_replacement_mapping"

    final_fallback_unavailable_reason = ""
    if final_fallback_action_key and not final_fallback_action_viable:
        if final_fallback_action_key in demoted_actions:
            final_fallback_unavailable_reason = "final_fallback_action_demoted"
        elif final_fallback_action_key in auto_disabled_actions:
            final_fallback_unavailable_reason = "final_fallback_action_auto_disabled"
        else:
            final_fallback_unavailable_reason = "final_fallback_action_unavailable"
    elif final_fallback_action_key and already_routed:
        final_fallback_unavailable_reason = "final_fallback_already_executed_for_source"
    elif final_fallback_action_key and not source_replaceable:
        final_fallback_unavailable_reason = (
            "source_action_not_replaceable"
            if action_key in RISKY_EDGE_RELAX_ACTION_KEYS
            else "source_policy_not_replaceable"
        )
    elif not final_fallback_action_key:
        final_fallback_unavailable_reason = "no_final_fallback_mapping"

    replacement_route_stage = "none"
    selected_replacement_action_key = ""
    arbitration_decision_reason = ""
    strict_candidate_score: dict[str, int | float | str] = {}
    tertiary_candidate_score: dict[str, int | float | str] = {}
    final_fallback_candidate_score: dict[str, int | float | str] = {}
    if strict_replacement_action_key:
        strict_candidate_score = _replacement_route_candidate_score(
            action_key=strict_replacement_action_key,
            action_effectiveness=action_effectiveness,
        )
        strict_candidate_score["viable"] = bool(strict_replacement_action_viable)
        strict_candidate_score["unavailable_reason"] = _text(strict_unavailable_reason)
    if tertiary_replacement_action_key:
        tertiary_candidate_score = _replacement_route_candidate_score(
            action_key=tertiary_replacement_action_key,
            action_effectiveness=action_effectiveness,
        )
        tertiary_candidate_score["viable"] = bool(tertiary_replacement_action_viable)
        tertiary_candidate_score["unavailable_reason"] = _text(tertiary_unavailable_reason)
    if final_fallback_action_key:
        final_fallback_candidate_score = _replacement_route_candidate_score(
            action_key=final_fallback_action_key,
            action_effectiveness=action_effectiveness,
        )
        final_fallback_candidate_score["viable"] = bool(final_fallback_action_viable)
        final_fallback_candidate_score["unavailable_reason"] = _text(final_fallback_unavailable_reason)

    if source_replaceable and already_routed:
        arbitration_decision_reason = "source_already_routed"
    elif source_replaceable and strict_replacement_action_viable and tertiary_replacement_action_viable:
        strict_harm_score = float(_safe_float(strict_candidate_score.get("harm_score")) or 0.0)
        tertiary_harm_score = float(_safe_float(tertiary_candidate_score.get("harm_score")) or 0.0)
        if strict_harm_score > tertiary_harm_score + float(REPLACEMENT_ROUTE_ARBITRATION_SCORE_EPSILON):
            replacement_route_stage = "tertiary"
            selected_replacement_action_key = tertiary_replacement_action_key
            arbitration_decision_reason = "tertiary_lower_harm_score"
        elif tertiary_harm_score > strict_harm_score + float(REPLACEMENT_ROUTE_ARBITRATION_SCORE_EPSILON):
            replacement_route_stage = "strict"
            selected_replacement_action_key = strict_replacement_action_key
            arbitration_decision_reason = "strict_lower_harm_score"
        else:
            replacement_route_stage = "strict"
            selected_replacement_action_key = strict_replacement_action_key
            arbitration_decision_reason = "strict_tie_break_deterministic"
    elif source_replaceable and strict_replacement_action_viable:
        replacement_route_stage = "strict"
        selected_replacement_action_key = strict_replacement_action_key
        arbitration_decision_reason = "strict_only_viable"
    elif source_replaceable and tertiary_replacement_action_viable:
        replacement_route_stage = "tertiary"
        selected_replacement_action_key = tertiary_replacement_action_key
        arbitration_decision_reason = "tertiary_only_viable"
    elif source_replaceable and final_fallback_action_viable:
        replacement_route_stage = "final_fallback"
        selected_replacement_action_key = final_fallback_action_key
        arbitration_decision_reason = "final_fallback_only_viable"
    elif not source_replaceable and action_key in RISKY_EDGE_RELAX_ACTION_KEYS:
        arbitration_decision_reason = "source_action_not_replaceable"
    elif not source_replaceable:
        arbitration_decision_reason = "source_policy_not_replaceable"
    elif source_replaceable and final_fallback_action_key:
        arbitration_decision_reason = "no_viable_replacement_route_final_fallback_unavailable"
    else:
        arbitration_decision_reason = "no_viable_replacement_route"

    if replacement_route_stage == "strict":
        source_reason = "demoted_source_routed_to_strict_replacement"
        replacement_routing_status = "routed"
    elif replacement_route_stage == "tertiary":
        source_reason = "demoted_source_routed_to_tertiary_replacement"
        replacement_routing_status = "routed"
    elif replacement_route_stage == "final_fallback":
        source_reason = "demoted_source_routed_to_final_fallback"
        replacement_routing_status = "routed"
    elif source_replaceable and (
        strict_replacement_action_key or tertiary_replacement_action_key or final_fallback_action_key
    ):
        source_reason = "demoted_source_replacement_unavailable"
        replacement_routing_status = "unavailable"
    else:
        source_reason = "auto_disabled_effectiveness_demotion_persistently_harmful"
        replacement_routing_status = "not_mapped"

    replacement_route_arbitration = {
        "policy": "minimize_harm_score",
        "selected_route_stage": replacement_route_stage,
        "selected_action_key": selected_replacement_action_key,
        "decision_reason": arbitration_decision_reason,
        "score_epsilon": round(float(REPLACEMENT_ROUTE_ARBITRATION_SCORE_EPSILON), 6),
        "strict_candidate": dict(strict_candidate_score),
        "tertiary_candidate": dict(tertiary_candidate_score),
        "final_fallback_candidate": dict(final_fallback_candidate_score),
    }

    return {
        "strict_replacement_action_key": strict_replacement_action_key,
        "strict_replacement_action_viable": strict_replacement_action_viable,
        "strict_replacement_unavailable_reason": strict_unavailable_reason,
        "tertiary_replacement_action_key": tertiary_replacement_action_key,
        "tertiary_replacement_action_viable": tertiary_replacement_action_viable,
        "tertiary_replacement_unavailable_reason": tertiary_unavailable_reason,
        "final_fallback_action_key": final_fallback_action_key,
        "final_fallback_action_viable": final_fallback_action_viable,
        "final_fallback_unavailable_reason": final_fallback_unavailable_reason,
        "replacement_route_stage": replacement_route_stage,
        "selected_replacement_action_key": selected_replacement_action_key,
        "source_reason": source_reason,
        "replacement_routing_status": replacement_routing_status,
        "replacement_route_arbitration": replacement_route_arbitration,
    }


def _execute_action(
    *,
    action_key: str,
    output_dir: str,
    trader_env_file: str,
    weather_window_hours: float,
    weather_min_bucket_samples: int,
    weather_max_profile_age_hours: float,
    optimizer_top_n: int,
    plateau_negative_regime_suppression_enabled: bool,
    plateau_negative_regime_suppression_min_bucket_samples: int,
    plateau_negative_regime_suppression_expectancy_threshold: float,
    plateau_negative_regime_suppression_top_n: int,
    retune_weather_window_hours_cap: float,
    retune_overblocking_blocked_share_threshold: float,
    retune_underblocking_min_top_n: int,
    retune_overblocking_max_top_n: int,
    retune_min_bucket_samples_target: int,
    retune_expectancy_threshold_target: float,
    replacement_profile_key: str = DEFAULT_REPLACEMENT_PROFILE_KEY,
) -> dict[str, Any]:
    selected_replacement_profile_key, replacement_profile = _replacement_profile_config_for_key(
        replacement_profile_key
    )
    output_path = Path(output_dir)
    before_trade_summary_path = _latest_trade_summary_artifact(output_path)
    before_trade_summary_mtime_epoch = _artifact_mtime_epoch(before_trade_summary_path)
    trader_payloads: list[dict[str, Any]] = []
    trader_call_kwargs: list[dict[str, Any]] = []
    telemetry_probe_diagnostics: dict[str, Any] = {}
    shadow_quote_probe_targets = _load_shadow_quote_probe_targets(output_path)
    execution_cost_exclusion_candidates: list[str] = []
    execution_cost_exclusion_tickers: list[str] = []
    execution_cost_exclusion_market_side_candidates: list[str] = []
    execution_cost_exclusion_market_side_targets: list[str] = []
    execution_cost_exclusion_second_probe_market_side_targets: list[str] = []
    execution_cost_exclusion_state_file = str(_execution_cost_exclusions_state_artifact(output_path))

    def _run_trader(**kwargs: Any) -> dict[str, Any]:
        trader_kwargs = dict(kwargs)
        trader_kwargs.pop("output_dir", None)
        trader_kwargs.pop("env_file", None)
        normalized_stale_share_threshold = _normalize_recovery_risk_off_stale_share_threshold(
            trader_kwargs.get("weather_pattern_risk_off_stale_metar_share_threshold")
        )
        if isinstance(normalized_stale_share_threshold, float):
            trader_kwargs["weather_pattern_risk_off_stale_metar_share_threshold"] = round(
                float(normalized_stale_share_threshold),
                6,
            )
        trader_call_kwargs.append(dict(trader_kwargs))
        payload = _run_shadow_trader_with_fallback(
            output_dir=output_dir,
            env_file=trader_env_file,
            **trader_kwargs,
        )
        trader_payloads.append(dict(payload) if isinstance(payload, dict) else {"status": "unknown"})
        return payload

    try:
        if action_key == "clear_weather_risk_off_state":
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=weather_window_hours,
                min_bucket_samples=weather_min_bucket_samples,
                max_profile_age_hours=weather_max_profile_age_hours,
            )
        elif action_key == "reduce_negative_expectancy_regimes":
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    -0.03,
                ),
                6,
            )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=48.0,
                weather_pattern_min_bucket_samples=max(12, int(weather_min_bucket_samples)),
                weather_pattern_negative_expectancy_threshold=-0.03,
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=0.65,
                weather_pattern_risk_off_min_attempts=max(12, int(weather_min_bucket_samples)),
                weather_pattern_risk_off_stale_metar_share_threshold=0.35,
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    12,
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    12,
                    int(plateau_negative_regime_suppression_top_n),
                ),
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=0.75,
                min_expected_edge_net=0.015,
                min_edge_to_risk_ratio=0.06,
            )
        elif action_key == "bootstrap_shadow_trade_intents":
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=False,
                weather_pattern_risk_off_enabled=False,
                weather_pattern_negative_regime_suppression_enabled=False,
                historical_selection_quality_enabled=False,
                enforce_probability_edge_thresholds=False,
            )
        elif action_key == "probe_expected_edge_floor_with_hardening_disabled":
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=False,
                weather_pattern_risk_off_enabled=False,
                weather_pattern_negative_regime_suppression_enabled=False,
                historical_selection_quality_enabled=False,
                enforce_probability_edge_thresholds=False,
            )
        elif action_key == "apply_expected_edge_relief_shadow_profile":
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=False,
                weather_pattern_risk_off_enabled=True,
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=12,
                weather_pattern_negative_regime_suppression_expectancy_threshold=-0.03,
                weather_pattern_negative_regime_suppression_top_n=12,
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=0.55,
                min_expected_edge_net=0.0,
                min_edge_to_risk_ratio=0.02,
            )
        elif action_key == DEFENSIVE_PIVOT_ACTION_KEY:
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    -0.035,
                ),
                6,
            )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=24.0,
                weather_pattern_min_bucket_samples=max(14, int(weather_min_bucket_samples)),
                weather_pattern_negative_expectancy_threshold=-0.02,
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=0.55,
                weather_pattern_risk_off_min_attempts=max(14, int(weather_min_bucket_samples)),
                weather_pattern_risk_off_stale_metar_share_threshold=0.30,
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    14,
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    10,
                    int(plateau_negative_regime_suppression_top_n),
                ),
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=0.85,
                min_expected_edge_net=0.025,
                min_edge_to_risk_ratio=0.10,
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == REPLACEMENT_REDUCE_NEGATIVE_EXPECTANCY_ACTION_KEY:
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    float(replacement_profile["suppression_expectancy_threshold_floor"]),
                ),
                6,
            )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=float(replacement_profile["weather_profile_max_age_hours"]),
                weather_pattern_min_bucket_samples=max(
                    int(replacement_profile["weather_min_bucket_samples_floor"]),
                    int(weather_min_bucket_samples),
                ),
                weather_pattern_negative_expectancy_threshold=float(
                    replacement_profile["weather_negative_expectancy_threshold"]
                ),
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=float(
                    replacement_profile["risk_off_concentration_threshold"]
                ),
                weather_pattern_risk_off_min_attempts=max(
                    int(replacement_profile["weather_min_bucket_samples_floor"]),
                    int(weather_min_bucket_samples),
                ),
                weather_pattern_risk_off_stale_metar_share_threshold=float(
                    replacement_profile["risk_off_stale_metar_share_threshold"]
                ),
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    int(replacement_profile["suppression_min_bucket_samples_floor"]),
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    int(replacement_profile["suppression_top_n_floor"]),
                    int(plateau_negative_regime_suppression_top_n),
                ),
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=float(replacement_profile["min_probability_confidence"]),
                min_expected_edge_net=float(replacement_profile["min_expected_edge_net"]),
                min_edge_to_risk_ratio=float(replacement_profile["min_edge_to_risk_ratio"]),
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == REPLACEMENT_PLATEAU_BREAK_NEGATIVE_EXPECTANCY_ACTION_KEY:
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    float(replacement_profile["suppression_expectancy_threshold_floor"]),
                ),
                6,
            )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=float(replacement_profile["weather_profile_max_age_hours"]),
                weather_pattern_min_bucket_samples=max(
                    int(replacement_profile["weather_min_bucket_samples_floor"]),
                    int(weather_min_bucket_samples),
                ),
                weather_pattern_negative_expectancy_threshold=float(
                    replacement_profile["weather_negative_expectancy_threshold"]
                ),
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    int(replacement_profile["suppression_min_bucket_samples_floor"]),
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    int(replacement_profile["suppression_top_n_floor"]),
                    int(plateau_negative_regime_suppression_top_n),
                ),
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=float(
                    replacement_profile["risk_off_concentration_threshold"]
                ),
                weather_pattern_risk_off_min_attempts=max(
                    int(replacement_profile["weather_min_bucket_samples_floor"]),
                    int(weather_min_bucket_samples),
                ),
                weather_pattern_risk_off_stale_metar_share_threshold=float(
                    replacement_profile["risk_off_stale_metar_share_threshold"]
                ),
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=float(replacement_profile["min_probability_confidence"]),
                min_expected_edge_net=float(replacement_profile["min_expected_edge_net"]),
                min_edge_to_risk_ratio=float(replacement_profile["min_edge_to_risk_ratio"]),
            )
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=max(336.0, weather_window_hours),
                min_bucket_samples=max(
                    int(replacement_profile["weather_min_bucket_samples_floor"]),
                    int(weather_min_bucket_samples),
                ),
                max_profile_age_hours=min(
                    float(weather_max_profile_age_hours),
                    float(replacement_profile["weather_max_profile_age_cap"]),
                ),
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "plateau_break_negative_expectancy_share":
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=48.0,
                weather_pattern_min_bucket_samples=12,
                weather_pattern_negative_expectancy_threshold=-0.03,
                weather_pattern_negative_regime_suppression_enabled=bool(
                    plateau_negative_regime_suppression_enabled
                ),
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    1, int(plateau_negative_regime_suppression_min_bucket_samples)
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=float(
                    plateau_negative_regime_suppression_expectancy_threshold
                ),
                weather_pattern_negative_regime_suppression_top_n=max(
                    1, int(plateau_negative_regime_suppression_top_n)
                ),
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=0.70,
                weather_pattern_risk_off_min_attempts=18,
                weather_pattern_risk_off_stale_metar_share_threshold=0.40,
                historical_selection_quality_enabled=True,
                min_probability_confidence=0.80,
                min_expected_edge_net=0.02,
                min_edge_to_risk_ratio=0.08,
            )
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=weather_window_hours,
                min_bucket_samples=weather_min_bucket_samples,
                max_profile_age_hours=weather_max_profile_age_hours,
            )
        elif action_key == "retune_negative_regime_suppression":
            suppression_snapshot = _load_suppression_snapshot(Path(output_dir))
            blocked_share = _safe_float(suppression_snapshot.get("blocked_share"))
            is_overblocking = (
                int(suppression_snapshot.get("candidate_count") or 0) > 0
                and isinstance(blocked_share, float)
                and blocked_share >= float(retune_overblocking_blocked_share_threshold)
            )
            severe_overblocking_threshold = max(
                float(RETUNE_SEVERE_OVERBLOCKING_ABSOLUTE_THRESHOLD),
                min(
                    0.99,
                    float(retune_overblocking_blocked_share_threshold)
                    + float(RETUNE_SEVERE_OVERBLOCKING_RELATIVE_BONUS),
                ),
            )
            is_severe_overblocking = bool(
                is_overblocking
                and isinstance(blocked_share, float)
                and blocked_share >= severe_overblocking_threshold
            )
            retuned_weather_window_hours = max(
                1.0,
                min(float(weather_window_hours), float(retune_weather_window_hours_cap)),
            )
            if is_overblocking:
                retuned_min_bucket_samples = max(
                    10,
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                )
                retuned_top_n = max(
                    1,
                    min(int(retune_overblocking_max_top_n), int(plateau_negative_regime_suppression_top_n)),
                )
                if is_severe_overblocking:
                    retuned_top_n = max(1, min(2, int(retuned_top_n)))
            else:
                retuned_min_bucket_samples = max(
                    10,
                    min(
                        int(retune_min_bucket_samples_target),
                        int(plateau_negative_regime_suppression_min_bucket_samples),
                    ),
                )
                retuned_top_n = max(
                    int(retune_underblocking_min_top_n),
                    int(plateau_negative_regime_suppression_top_n),
                )
            retune_expectancy_floor = float(retune_expectancy_threshold_target) - 0.005
            retuned_expectancy_threshold = round(
                max(
                    retune_expectancy_floor,
                    min(
                        float(retune_expectancy_threshold_target),
                        float(plateau_negative_regime_suppression_expectancy_threshold) + 0.015,
                    ),
                ),
                6,
            )
            if is_severe_overblocking:
                severe_overblocking_expectancy_threshold = min(
                    -0.055,
                    float(retune_expectancy_threshold_target) - 0.015,
                )
                retuned_expectancy_threshold = round(
                    min(float(retuned_expectancy_threshold), float(severe_overblocking_expectancy_threshold)),
                    6,
                )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=48.0,
                weather_pattern_min_bucket_samples=12,
                weather_pattern_negative_expectancy_threshold=-0.03,
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=retuned_min_bucket_samples,
                weather_pattern_negative_regime_suppression_expectancy_threshold=retuned_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=retuned_top_n,
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=0.70,
                weather_pattern_risk_off_min_attempts=18,
                weather_pattern_risk_off_stale_metar_share_threshold=0.40,
                historical_selection_quality_enabled=True,
                min_probability_confidence=0.80,
                min_expected_edge_net=0.02,
                min_edge_to_risk_ratio=0.08,
            )
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=retuned_weather_window_hours,
                min_bucket_samples=weather_min_bucket_samples,
                max_profile_age_hours=weather_max_profile_age_hours,
            )
        elif action_key == "rebalance_weather_pattern_hard_block_pressure":
            suppression_expectancy_threshold = round(
                min(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    -0.055,
                ),
                6,
            )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=24.0,
                weather_pattern_min_bucket_samples=max(10, int(weather_min_bucket_samples)),
                weather_pattern_negative_expectancy_threshold=-0.03,
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    10,
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    2,
                    min(6, int(plateau_negative_regime_suppression_top_n)),
                ),
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=0.75,
                weather_pattern_risk_off_min_attempts=max(10, int(weather_min_bucket_samples)),
                weather_pattern_risk_off_stale_metar_share_threshold=0.45,
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=0.82,
                min_expected_edge_net=0.02,
                min_edge_to_risk_ratio=0.08,
            )
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=max(1.0, min(float(weather_window_hours), float(retune_weather_window_hours_cap))),
                min_bucket_samples=max(10, int(weather_min_bucket_samples)),
                max_profile_age_hours=weather_max_profile_age_hours,
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "repair_metar_ingest_quality_pipeline":
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    -0.03,
                ),
                6,
            )
            run_kalshi_temperature_metar_ingest(output_dir=output_dir)
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=max(weather_window_hours, 336.0),
                min_bucket_samples=max(12, int(weather_min_bucket_samples)),
                max_profile_age_hours=weather_max_profile_age_hours,
            )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=24.0,
                weather_pattern_min_bucket_samples=max(12, int(weather_min_bucket_samples)),
                weather_pattern_negative_expectancy_threshold=-0.025,
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=0.60,
                weather_pattern_risk_off_min_attempts=max(12, int(weather_min_bucket_samples)),
                weather_pattern_risk_off_stale_metar_share_threshold=0.30,
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    12,
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    10,
                    int(plateau_negative_regime_suppression_top_n),
                ),
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=0.82,
                min_expected_edge_net=0.02,
                min_edge_to_risk_ratio=0.08,
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "reduce_stale_station_concentration":
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    -0.03,
                ),
                6,
            )
            run_kalshi_temperature_metar_ingest(output_dir=output_dir)
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=24.0,
                weather_pattern_min_bucket_samples=max(12, int(weather_min_bucket_samples)),
                weather_pattern_negative_expectancy_threshold=-0.025,
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=0.60,
                weather_pattern_risk_off_min_attempts=max(12, int(weather_min_bucket_samples)),
                weather_pattern_risk_off_stale_metar_share_threshold=0.30,
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    12,
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    10,
                    int(plateau_negative_regime_suppression_top_n),
                ),
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=0.82,
                min_expected_edge_net=0.02,
                min_edge_to_risk_ratio=0.08,
            )
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=max(weather_window_hours, 336.0),
                min_bucket_samples=weather_min_bucket_samples,
                max_profile_age_hours=weather_max_profile_age_hours,
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "reduce_stale_metar_pressure":
            run_kalshi_temperature_metar_ingest(output_dir=output_dir)
        elif action_key == "refresh_market_horizon_inputs":
            try:
                run_kalshi_temperature_contract_specs(
                    env_file=trader_env_file,
                    output_dir=output_dir,
                )
            except ValueError as exc:
                fallback_env_file = _resolve_contract_specs_retry_env_file(trader_env_file)
                if "Env file not found" not in _text(exc) or not fallback_env_file:
                    raise
                run_kalshi_temperature_contract_specs(
                    env_file=fallback_env_file,
                    output_dir=output_dir,
                )
            run_kalshi_temperature_constraint_scan(output_dir=output_dir)
            run_kalshi_temperature_settlement_state(output_dir=output_dir)
        elif action_key == "repair_taf_station_mapping_pipeline":
            try:
                run_kalshi_temperature_contract_specs(
                    env_file=trader_env_file,
                    output_dir=output_dir,
                )
            except ValueError as exc:
                fallback_env_file = _resolve_contract_specs_retry_env_file(trader_env_file)
                if "Env file not found" not in _text(exc) or not fallback_env_file:
                    raise
                run_kalshi_temperature_contract_specs(
                    env_file=fallback_env_file,
                    output_dir=output_dir,
                )
            run_kalshi_temperature_constraint_scan(output_dir=output_dir)
            run_kalshi_temperature_settlement_state(output_dir=output_dir)
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    -0.03,
                ),
                6,
            )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=24.0,
                weather_pattern_min_bucket_samples=max(12, int(weather_min_bucket_samples)),
                weather_pattern_negative_expectancy_threshold=-0.025,
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=0.60,
                weather_pattern_risk_off_min_attempts=max(12, int(weather_min_bucket_samples)),
                weather_pattern_risk_off_stale_metar_share_threshold=0.30,
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    12,
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    10,
                    int(plateau_negative_regime_suppression_top_n),
                ),
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=0.82,
                min_expected_edge_net=0.02,
                min_edge_to_risk_ratio=0.08,
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "reduce_execution_friction_pressure":
            run_kalshi_temperature_execution_cost_tape(output_dir=output_dir)
            (
                execution_cost_exclusion_tickers,
                execution_cost_exclusion_market_side_targets,
                execution_cost_exclusion_state_payload,
                execution_cost_exclusion_candidates,
                execution_cost_exclusion_market_side_candidates,
            ) = _resolve_stateful_execution_cost_exclusion_tickers(output_path)
            execution_cost_exclusion_active_target_count = (
                int(len(execution_cost_exclusion_tickers))
                + int(len(execution_cost_exclusion_market_side_targets))
            )
            execution_cost_exclusion_second_probe_market_side_targets = []
            execution_cost_exclusion_state_file = str(_execution_cost_exclusions_state_artifact(output_path))
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    -0.03,
                ),
                6,
            )
            execution_cost_exclusion_downshift_evidence: dict[str, Any] = {}
            active_exclusion_count_before_downshift = int(len(execution_cost_exclusion_tickers))
            first_probe_status = "no_candidates"
            first_probe_intents_total = 0
            first_probe_intents_approved = 0
            second_probe_triggered = False
            second_probe_reason = ""
            second_probe_status = ""
            second_probe_exclusion_count = 0
            second_probe_intents_total = 0
            second_probe_intents_approved = 0
            if _text(trader_env_file):
                first_probe_exclusion_targets = list(execution_cost_exclusion_tickers)
                if execution_cost_exclusion_market_side_targets:
                    first_probe_exclusion_targets.extend(execution_cost_exclusion_market_side_targets)
                first_probe_kwargs = {
                    "env_file": trader_env_file,
                    "output_dir": output_dir,
                    "intents_only": True,
                    "allow_live_orders": False,
                    "weather_pattern_hardening_enabled": True,
                    "weather_pattern_profile_max_age_hours": 24.0,
                    "weather_pattern_min_bucket_samples": max(12, int(weather_min_bucket_samples)),
                    "weather_pattern_negative_expectancy_threshold": -0.025,
                    "weather_pattern_risk_off_enabled": True,
                    "weather_pattern_risk_off_concentration_threshold": 0.60,
                    "weather_pattern_risk_off_min_attempts": max(12, int(weather_min_bucket_samples)),
                    "weather_pattern_risk_off_stale_metar_share_threshold": 0.30,
                    "weather_pattern_negative_regime_suppression_enabled": True,
                    "weather_pattern_negative_regime_suppression_min_bucket_samples": max(
                        12,
                        int(plateau_negative_regime_suppression_min_bucket_samples),
                    ),
                    "weather_pattern_negative_regime_suppression_expectancy_threshold": suppression_expectancy_threshold,
                    "weather_pattern_negative_regime_suppression_top_n": max(
                        10,
                        int(plateau_negative_regime_suppression_top_n),
                    ),
                    "historical_selection_quality_enabled": True,
                    "enforce_probability_edge_thresholds": True,
                    "min_probability_confidence": 0.86,
                    "min_expected_edge_net": 0.025,
                    "min_edge_to_risk_ratio": 0.10,
                    "exclude_market_tickers": list(first_probe_exclusion_targets),
                }
                first_probe_payload = _run_trader(**first_probe_kwargs)
                first_probe_status = _text(first_probe_payload.get("status")).lower() or "unknown"
                first_probe_intents_total = _resolve_trader_diagnostic_total(first_probe_payload, "intents_total")
                first_probe_intents_approved = _resolve_trader_diagnostic_total(first_probe_payload, "intents_approved")
                reduced_execution_cost_exclusion_tickers = list(
                    execution_cost_exclusion_tickers[: max(0, len(execution_cost_exclusion_tickers) // 2)]
                )
                reduced_execution_cost_exclusion_market_side_targets = (
                    _filter_execution_cost_exclusion_market_side_targets(
                        market_side_targets=execution_cost_exclusion_market_side_targets,
                        allowed_tickers=reduced_execution_cost_exclusion_tickers,
                    )
                )
                if (first_probe_status == "no_candidates" or int(first_probe_intents_approved) <= 0) and execution_cost_exclusion_tickers:
                    second_probe_kwargs = {
                        "env_file": trader_env_file,
                        "output_dir": output_dir,
                        "intents_only": True,
                        "allow_live_orders": False,
                        "weather_pattern_hardening_enabled": True,
                        "weather_pattern_profile_max_age_hours": 24.0,
                        "weather_pattern_min_bucket_samples": max(12, int(weather_min_bucket_samples)),
                        "weather_pattern_negative_expectancy_threshold": -0.025,
                        "weather_pattern_risk_off_enabled": True,
                        "weather_pattern_risk_off_concentration_threshold": 0.60,
                        "weather_pattern_risk_off_min_attempts": max(12, int(weather_min_bucket_samples)),
                        "weather_pattern_risk_off_stale_metar_share_threshold": 0.30,
                        "weather_pattern_negative_regime_suppression_enabled": True,
                        "weather_pattern_negative_regime_suppression_min_bucket_samples": max(
                            12,
                            int(plateau_negative_regime_suppression_min_bucket_samples),
                        ),
                        "weather_pattern_negative_regime_suppression_expectancy_threshold": suppression_expectancy_threshold,
                        "weather_pattern_negative_regime_suppression_top_n": max(
                            10,
                            int(plateau_negative_regime_suppression_top_n),
                        ),
                        "historical_selection_quality_enabled": True,
                        "enforce_probability_edge_thresholds": True,
                        "min_probability_confidence": 0.86,
                        "min_expected_edge_net": 0.025,
                        "min_edge_to_risk_ratio": 0.10,
                        "exclude_market_tickers": list(reduced_execution_cost_exclusion_tickers),
                    }
                    if reduced_execution_cost_exclusion_market_side_targets:
                        second_probe_kwargs["exclude_market_tickers"] = list(
                            reduced_execution_cost_exclusion_tickers
                        ) + list(reduced_execution_cost_exclusion_market_side_targets)
                    second_probe_payload = _run_trader(**second_probe_kwargs)
                    second_probe_triggered = True
                    second_probe_reason = "first_probe_no_candidates_or_zero_approved_with_active_exclusions"
                    second_probe_status = _text(second_probe_payload.get("status")).lower() or "unknown"
                    second_probe_exclusion_count = int(len(reduced_execution_cost_exclusion_tickers))
                    second_probe_intents_total = _resolve_trader_diagnostic_total(second_probe_payload, "intents_total")
                    second_probe_intents_approved = _resolve_trader_diagnostic_total(second_probe_payload, "intents_approved")
                    execution_cost_exclusion_second_probe_market_side_targets = list(
                        reduced_execution_cost_exclusion_market_side_targets
                    )
                elif first_probe_status == "no_candidates" or int(first_probe_intents_approved) <= 0:
                    second_probe_reason = "no_active_exclusions"
                else:
                    second_probe_reason = "first_probe_returned_approved_intents"
                (
                    execution_cost_exclusion_tickers,
                    execution_cost_exclusion_market_side_targets,
                    execution_cost_exclusion_state_payload,
                    execution_cost_exclusion_downshift_evidence,
                ) = _apply_execution_cost_exclusion_downshift(
                    output_dir=output_path,
                    state_payload=execution_cost_exclusion_state_payload,
                    active_tickers=execution_cost_exclusion_tickers,
                    active_market_side_targets=execution_cost_exclusion_market_side_targets,
                    first_probe_status=first_probe_status,
                    first_probe_intents_total=first_probe_intents_total,
                    first_probe_intents_approved=first_probe_intents_approved,
                    second_probe_triggered=second_probe_triggered,
                    second_probe_status=second_probe_status,
                    second_probe_intents_total=second_probe_intents_total,
                    second_probe_intents_approved=second_probe_intents_approved,
                )
            else:
                first_probe_status = "skipped_missing_env_file"
                second_probe_reason = "missing_trader_env_file"
                execution_cost_exclusion_downshift_evidence = {
                    "decision": "no_downshift",
                    "reason": "missing_trader_env_file",
                    "triggered": False,
                    "active_count_before": int(active_exclusion_count_before_downshift),
                    "active_count_after": int(active_exclusion_count_before_downshift),
                    "drop_count": 0,
                    "dropped_tickers": [],
                    "active_market_side_target_count_before": int(len(execution_cost_exclusion_market_side_targets)),
                    "active_market_side_target_count_after": int(len(execution_cost_exclusion_market_side_targets)),
                    "drop_market_side_target_count": 0,
                    "dropped_market_side_targets": [],
                }
            downshift_triggered = bool(execution_cost_exclusion_downshift_evidence.get("triggered"))
            downshift_reason = _text(execution_cost_exclusion_downshift_evidence.get("reason"))
            downshift_removed_tickers = list(execution_cost_exclusion_downshift_evidence.get("dropped_tickers") or [])
            downshift_removed_market_side_targets = list(
                execution_cost_exclusion_downshift_evidence.get("dropped_market_side_targets") or []
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "repair_execution_telemetry_pipeline":
            _ensure_blocker_audit_fallback_from_trade_summary(Path(output_dir))
            if _text(trader_env_file):
                try:
                    run_kalshi_ws_state_collect(
                        env_file=trader_env_file,
                        output_dir=output_dir,
                        run_seconds=20.0,
                    )
                except Exception:
                    pass
                # Run a safe shadow execute probe so candidate/book telemetry
                # is recorded in execution journal artifacts used by cost tape.
                probe_kwargs = {
                    "env_file": trader_env_file,
                    "output_dir": output_dir,
                    "intents_only": False,
                    "allow_live_orders": False,
                    "contracts_per_order": 1,
                    "max_orders": 24,
                    "max_markets": 250,
                    "planning_bankroll_dollars": 250.0,
                    "daily_risk_cap_dollars": 75.0,
                    "weather_pattern_hardening_enabled": False,
                    "weather_pattern_risk_off_enabled": False,
                    "weather_pattern_negative_regime_suppression_enabled": False,
                    "historical_selection_quality_enabled": False,
                    "metar_ingest_quality_gate_enabled": False,
                    "enforce_probability_edge_thresholds": False,
                    "enforce_interval_consistency": False,
                    "require_market_snapshot_seq": False,
                    "max_total_deployed_pct": 1.0,
                    "max_same_station_exposure_pct": 1.0,
                    "max_same_hour_cluster_exposure_pct": 1.0,
                    "max_same_underlying_exposure_pct": 1.0,
                    "max_intents_per_underlying": 12,
                    "replan_market_side_cooldown_minutes": 0.0,
                    "replan_market_side_max_plans_per_window": 999,
                    "min_hours_to_close": 0.0,
                    "max_hours_to_close": 72.0,
                    "max_metar_age_minutes": None,
                    "min_settlement_confidence": 0.0,
                    "min_expected_edge_net": 0.0,
                    "min_edge_to_risk_ratio": 0.0,
                }
                first_probe_payload = _run_trader(**probe_kwargs)
                first_probe_status = _text(first_probe_payload.get("status")).lower() or "unknown"
                first_probe_reason_counts = _extract_policy_reason_counts_from_trader_payload(first_probe_payload)
                hardening_pressure_reason_counts = _policy_reason_counts_subset(
                    counts=first_probe_reason_counts,
                    allowed_reasons=TELEMETRY_HARDENING_PRESSURE_POLICY_REASONS,
                )
                hardening_pressure_total = int(sum(hardening_pressure_reason_counts.values()))
                telemetry_probe_diagnostics = {
                    "first_probe_status": first_probe_status,
                    "first_probe_policy_reason_counts": first_probe_reason_counts,
                    "hardening_pressure_policy_reason_counts": hardening_pressure_reason_counts,
                    "hardening_pressure_total": int(hardening_pressure_total),
                    "second_probe_triggered": False,
                    "second_probe_reason": "",
                    "second_probe_status": "",
                }
                if first_probe_status == "no_candidates" and hardening_pressure_total > 0:
                    second_probe_kwargs = dict(probe_kwargs)
                    second_probe_kwargs.update(
                        shadow_quote_probe_on_no_candidates=True,
                        min_alpha_strength=None,
                        min_probability_confidence=None,
                        min_expected_edge_net=None,
                        min_edge_to_risk_ratio=None,
                        min_base_edge_net=None,
                        min_probability_breakeven_gap=None,
                        max_yes_possible_gap_for_yes_side=1.0,
                        yes_max_entry_price_dollars=0.99,
                        no_max_entry_price_dollars=0.99,
                        max_orders=32,
                        max_markets=300,
                    )
                    second_probe_payload = _run_trader(**second_probe_kwargs)
                    telemetry_probe_diagnostics["second_probe_triggered"] = True
                    telemetry_probe_diagnostics["second_probe_reason"] = (
                        "first_probe_no_candidates_with_hardening_pressure"
                    )
                    telemetry_probe_diagnostics["second_probe_status"] = (
                        _text(second_probe_payload.get("status")).lower() or "unknown"
                    )
                elif first_probe_status != "no_candidates":
                    telemetry_probe_diagnostics["second_probe_reason"] = "first_probe_not_no_candidates"
                else:
                    telemetry_probe_diagnostics["second_probe_reason"] = "no_hardening_pressure_detected"
            else:
                telemetry_probe_diagnostics = {
                    "first_probe_status": "skipped_missing_env_file",
                    "first_probe_policy_reason_counts": {},
                    "hardening_pressure_policy_reason_counts": {},
                    "hardening_pressure_total": 0,
                    "second_probe_triggered": False,
                    "second_probe_reason": "missing_trader_env_file",
                    "second_probe_status": "",
                }
            run_kalshi_temperature_execution_cost_tape(output_dir=output_dir)
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "improve_execution_quote_coverage_shadow":
            side_pressure_metadata = _load_shadow_quote_probe_side_pressure(output_path)
            side_pressure_active = bool(side_pressure_metadata.get("active"))
            dominant_side = _normalize_market_side(side_pressure_metadata.get("dominant_side"))
            quote_probe_targets_before_side_bias = list(shadow_quote_probe_targets)
            quote_probe_targets_after_side_bias = _apply_shadow_quote_probe_side_bias(
                targets=quote_probe_targets_before_side_bias,
                side_pressure_active=side_pressure_active,
                dominant_side=dominant_side,
            )
            if _text(trader_env_file):
                try:
                    run_kalshi_ws_state_collect(
                        env_file=trader_env_file,
                        output_dir=output_dir,
                        run_seconds=35.0,
                    )
                except Exception:
                    pass
                quote_probe_kwargs = {
                    "env_file": trader_env_file,
                    "output_dir": output_dir,
                    "intents_only": False,
                    "allow_live_orders": False,
                    "shadow_quote_probe_on_no_candidates": True,
                    "shadow_quote_probe_market_side_targets": list(quote_probe_targets_after_side_bias),
                    "contracts_per_order": 1,
                    "max_orders": 36,
                    "max_markets": 360,
                    "planning_bankroll_dollars": 300.0,
                    "daily_risk_cap_dollars": 90.0,
                    "weather_pattern_hardening_enabled": False,
                    "weather_pattern_risk_off_enabled": False,
                    "weather_pattern_negative_regime_suppression_enabled": False,
                    "historical_selection_quality_enabled": False,
                    "metar_ingest_quality_gate_enabled": False,
                    "enforce_probability_edge_thresholds": False,
                    "enforce_interval_consistency": False,
                    "require_market_snapshot_seq": False,
                    "max_total_deployed_pct": 1.0,
                    "max_same_station_exposure_pct": 1.0,
                    "max_same_hour_cluster_exposure_pct": 1.0,
                    "max_same_underlying_exposure_pct": 1.0,
                    "max_intents_per_underlying": 18,
                    "replan_market_side_cooldown_minutes": 0.0,
                    "replan_market_side_max_plans_per_window": 999,
                    "min_hours_to_close": 0.0,
                    "max_hours_to_close": 96.0,
                    "max_metar_age_minutes": None,
                    "min_settlement_confidence": 0.0,
                    "min_alpha_strength": None,
                    "min_probability_confidence": None,
                    "min_expected_edge_net": None,
                    "min_edge_to_risk_ratio": None,
                    "min_base_edge_net": None,
                    "min_probability_breakeven_gap": None,
                }
                first_probe_payload = _run_trader(**quote_probe_kwargs)
                first_probe_status = _text(first_probe_payload.get("status")).lower() or "unknown"
                first_probe_reason_counts = _extract_policy_reason_counts_from_trader_payload(first_probe_payload)
                quote_coverage_pressure_policy_reasons = set(TELEMETRY_HARDENING_PRESSURE_POLICY_REASONS)
                quote_coverage_pressure_policy_reasons.add("missing_market_snapshot_seq")
                quote_coverage_pressure_reason_counts = _policy_reason_counts_subset(
                    counts=first_probe_reason_counts,
                    allowed_reasons=quote_coverage_pressure_policy_reasons,
                )
                quote_coverage_pressure_total = int(sum(quote_coverage_pressure_reason_counts.values()))
                first_probe_intents_total = _resolve_trader_diagnostic_total(first_probe_payload, "intents_total")
                first_probe_intents_approved = _resolve_trader_diagnostic_total(first_probe_payload, "intents_approved")
                telemetry_probe_diagnostics = {
                    "probe_kind": "quote_coverage_shadow",
                    "first_probe_status": first_probe_status,
                    "first_probe_policy_reason_counts": first_probe_reason_counts,
                    "quote_coverage_pressure_policy_reason_counts": quote_coverage_pressure_reason_counts,
                    "quote_coverage_pressure_total": int(quote_coverage_pressure_total),
                    "first_probe_intents_total": int(first_probe_intents_total),
                    "first_probe_intents_approved": int(first_probe_intents_approved),
                    "side_pressure_active": bool(side_pressure_active),
                    "dominant_side": dominant_side,
                    "shadow_quote_probe_target_count_before_side_bias": int(
                        len(quote_probe_targets_before_side_bias)
                    ),
                    "shadow_quote_probe_target_count_after_side_bias": int(
                        len(quote_probe_targets_after_side_bias)
                    ),
                    "shadow_quote_probe_target_count": int(len(quote_probe_targets_after_side_bias)),
                    "shadow_quote_probe_targets": list(quote_probe_targets_after_side_bias[:20]),
                    "second_probe_triggered": False,
                    "second_probe_reason": "",
                    "second_probe_status": "",
                }
                should_run_second_probe = bool(
                    first_probe_status == "no_candidates"
                    and (
                        quote_coverage_pressure_total > 0
                        or (
                            int(first_probe_intents_total) >= 12
                            and int(first_probe_intents_approved) <= 0
                        )
                    )
                )
                if should_run_second_probe:
                    second_probe_kwargs = dict(quote_probe_kwargs)
                    second_probe_kwargs.update(
                        max_orders=48,
                        max_markets=500,
                        max_intents_per_underlying=24,
                        max_yes_possible_gap_for_yes_side=1.0,
                        yes_max_entry_price_dollars=0.99,
                        no_max_entry_price_dollars=0.99,
                        shadow_quote_probe_market_side_targets=list(quote_probe_targets_after_side_bias),
                    )
                    second_probe_payload = _run_trader(**second_probe_kwargs)
                    telemetry_probe_diagnostics["second_probe_triggered"] = True
                    telemetry_probe_diagnostics["second_probe_reason"] = (
                        "first_probe_no_candidates_low_quote_coverage_pressure"
                    )
                    telemetry_probe_diagnostics["second_probe_status"] = (
                        _text(second_probe_payload.get("status")).lower() or "unknown"
                    )
                elif first_probe_status != "no_candidates":
                    telemetry_probe_diagnostics["second_probe_reason"] = "first_probe_not_no_candidates"
                else:
                    telemetry_probe_diagnostics["second_probe_reason"] = "no_quote_coverage_pressure_detected"
            else:
                telemetry_probe_diagnostics = {
                    "probe_kind": "quote_coverage_shadow",
                    "first_probe_status": "skipped_missing_env_file",
                    "first_probe_policy_reason_counts": {},
                    "quote_coverage_pressure_policy_reason_counts": {},
                    "quote_coverage_pressure_total": 0,
                    "first_probe_intents_total": 0,
                    "first_probe_intents_approved": 0,
                    "side_pressure_active": bool(side_pressure_active),
                    "dominant_side": dominant_side,
                    "shadow_quote_probe_target_count_before_side_bias": int(
                        len(quote_probe_targets_before_side_bias)
                    ),
                    "shadow_quote_probe_target_count_after_side_bias": int(
                        len(quote_probe_targets_after_side_bias)
                    ),
                    "shadow_quote_probe_target_count": int(len(quote_probe_targets_after_side_bias)),
                    "shadow_quote_probe_targets": list(quote_probe_targets_after_side_bias[:20]),
                    "second_probe_triggered": False,
                    "second_probe_reason": "missing_trader_env_file",
                    "second_probe_status": "",
                }
            run_kalshi_temperature_execution_cost_tape(output_dir=output_dir)
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key in {
            "increase_settled_outcome_coverage",
            "recover_settled_outcome_velocity",
        }:
            throughput_payload = run_kalshi_temperature_settled_outcome_throughput(
                output_dir=output_dir,
            )
            targeted_constraint_csv = _text(throughput_payload.get("targeted_constraint_csv"))
            if targeted_constraint_csv:
                suppression_expectancy_threshold = round(
                    max(
                        float(plateau_negative_regime_suppression_expectancy_threshold),
                        -0.03,
                    ),
                    6,
                )
                _run_trader(
                    env_file=trader_env_file,
                    output_dir=output_dir,
                    constraint_csv=targeted_constraint_csv,
                    intents_only=True,
                    allow_live_orders=False,
                    weather_pattern_hardening_enabled=True,
                    weather_pattern_profile_max_age_hours=48.0,
                    weather_pattern_min_bucket_samples=max(12, int(weather_min_bucket_samples)),
                    weather_pattern_negative_expectancy_threshold=-0.03,
                    weather_pattern_risk_off_enabled=True,
                    weather_pattern_risk_off_concentration_threshold=0.65,
                    weather_pattern_risk_off_min_attempts=max(12, int(weather_min_bucket_samples)),
                    weather_pattern_risk_off_stale_metar_share_threshold=0.35,
                    weather_pattern_negative_regime_suppression_enabled=True,
                    weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                        12,
                        int(plateau_negative_regime_suppression_min_bucket_samples),
                    ),
                    weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                    weather_pattern_negative_regime_suppression_top_n=max(
                        12,
                        int(plateau_negative_regime_suppression_top_n),
                    ),
                    historical_selection_quality_enabled=True,
                    enforce_probability_edge_thresholds=True,
                    min_probability_confidence=0.75,
                    min_expected_edge_net=0.015,
                    min_edge_to_risk_ratio=0.06,
                )
            run_kalshi_temperature_settlement_state(output_dir=output_dir)
            run_kalshi_temperature_profitability(
                output_dir=output_dir,
                hours=max(float(weather_window_hours), 168.0),
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == REPLACEMENT_SETTLED_OUTCOME_COVERAGE_ACTION_KEY:
            throughput_payload = run_kalshi_temperature_settled_outcome_throughput(
                output_dir=output_dir,
            )
            targeted_constraint_csv = _text(throughput_payload.get("targeted_constraint_csv"))
            if targeted_constraint_csv:
                suppression_expectancy_threshold = round(
                    max(
                        float(plateau_negative_regime_suppression_expectancy_threshold),
                        float(replacement_profile["suppression_expectancy_threshold_floor"]),
                    ),
                    6,
                )
                _run_trader(
                    env_file=trader_env_file,
                    output_dir=output_dir,
                    constraint_csv=targeted_constraint_csv,
                    intents_only=True,
                    allow_live_orders=False,
                    weather_pattern_hardening_enabled=True,
                    weather_pattern_profile_max_age_hours=float(replacement_profile["weather_profile_max_age_hours"]),
                    weather_pattern_min_bucket_samples=max(
                        int(replacement_profile["weather_min_bucket_samples_floor"]),
                        int(weather_min_bucket_samples),
                    ),
                    weather_pattern_negative_expectancy_threshold=float(
                        replacement_profile["weather_negative_expectancy_threshold"]
                    ),
                    weather_pattern_risk_off_enabled=True,
                    weather_pattern_risk_off_concentration_threshold=float(
                        replacement_profile["risk_off_concentration_threshold"]
                    ),
                    weather_pattern_risk_off_min_attempts=max(
                        int(replacement_profile["weather_min_bucket_samples_floor"]),
                        int(weather_min_bucket_samples),
                    ),
                    weather_pattern_risk_off_stale_metar_share_threshold=float(
                        replacement_profile["risk_off_stale_metar_share_threshold"]
                    ),
                    weather_pattern_negative_regime_suppression_enabled=True,
                    weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                        int(replacement_profile["suppression_min_bucket_samples_floor"]),
                        int(plateau_negative_regime_suppression_min_bucket_samples),
                    ),
                    weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                    weather_pattern_negative_regime_suppression_top_n=max(
                        int(replacement_profile["suppression_top_n_floor"]),
                        int(plateau_negative_regime_suppression_top_n),
                    ),
                    historical_selection_quality_enabled=True,
                    enforce_probability_edge_thresholds=True,
                    min_probability_confidence=float(replacement_profile["min_probability_confidence"]),
                    min_expected_edge_net=float(replacement_profile["min_expected_edge_net"]),
                    min_edge_to_risk_ratio=float(replacement_profile["min_edge_to_risk_ratio"]),
                )
            run_kalshi_temperature_settlement_state(output_dir=output_dir)
            run_kalshi_temperature_profitability(
                output_dir=output_dir,
                hours=max(float(weather_window_hours), 168.0),
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "resolve_decision_matrix_weather_blockers":
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "repair_weather_confidence_adjusted_signal_pipeline":
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=weather_window_hours,
                min_bucket_samples=weather_min_bucket_samples,
                max_profile_age_hours=weather_max_profile_age_hours,
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "clear_optimizer_weather_hard_block":
            run_kalshi_temperature_growth_optimizer(
                input_paths=[output_dir],
                top_n=max(10, int(optimizer_top_n)),
            )
        elif action_key == "increase_weather_sample_coverage":
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=max(weather_window_hours, 1440.0),
                min_bucket_samples=weather_min_bucket_samples,
                max_profile_age_hours=weather_max_profile_age_hours,
            )
        elif action_key == REPLACEMENT_INCREASE_WEATHER_SAMPLE_COVERAGE_ACTION_KEY:
            run_kalshi_temperature_weather_pattern(
                output_dir=output_dir,
                window_hours=max(weather_window_hours, float(replacement_profile["weather_window_hours_floor"])),
                min_bucket_samples=max(
                    int(replacement_profile["weather_min_bucket_samples_floor"]),
                    int(weather_min_bucket_samples),
                ),
                max_profile_age_hours=min(
                    float(weather_max_profile_age_hours),
                    float(replacement_profile["weather_max_profile_age_cap"]),
                ),
            )
            suppression_expectancy_threshold = round(
                max(
                    float(plateau_negative_regime_suppression_expectancy_threshold),
                    float(replacement_profile["suppression_expectancy_threshold_floor"]),
                ),
                6,
            )
            _run_trader(
                env_file=trader_env_file,
                output_dir=output_dir,
                intents_only=True,
                allow_live_orders=False,
                weather_pattern_hardening_enabled=True,
                weather_pattern_profile_max_age_hours=float(replacement_profile["weather_profile_max_age_hours"]),
                weather_pattern_min_bucket_samples=max(
                    int(replacement_profile["weather_min_bucket_samples_floor"]),
                    int(weather_min_bucket_samples),
                ),
                weather_pattern_negative_expectancy_threshold=float(
                    replacement_profile["weather_negative_expectancy_threshold"]
                ),
                weather_pattern_risk_off_enabled=True,
                weather_pattern_risk_off_concentration_threshold=float(
                    replacement_profile["risk_off_concentration_threshold"]
                ),
                weather_pattern_risk_off_min_attempts=max(
                    int(replacement_profile["weather_min_bucket_samples_floor"]),
                    int(weather_min_bucket_samples),
                ),
                weather_pattern_risk_off_stale_metar_share_threshold=float(
                    replacement_profile["risk_off_stale_metar_share_threshold"]
                ),
                weather_pattern_negative_regime_suppression_enabled=True,
                weather_pattern_negative_regime_suppression_min_bucket_samples=max(
                    int(replacement_profile["suppression_min_bucket_samples_floor"]),
                    int(plateau_negative_regime_suppression_min_bucket_samples),
                ),
                weather_pattern_negative_regime_suppression_expectancy_threshold=suppression_expectancy_threshold,
                weather_pattern_negative_regime_suppression_top_n=max(
                    int(replacement_profile["suppression_top_n_floor"]),
                    int(plateau_negative_regime_suppression_top_n),
                ),
                historical_selection_quality_enabled=True,
                enforce_probability_edge_thresholds=True,
                min_probability_confidence=float(replacement_profile["min_probability_confidence"]),
                min_expected_edge_net=float(replacement_profile["min_expected_edge_net"]),
                min_edge_to_risk_ratio=float(replacement_profile["min_edge_to_risk_ratio"]),
            )
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "refresh_decision_matrix_weather_signals":
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "restore_stage_timeout_guardrail_script":
            # Keep this executable in-loop: refresh matrix context immediately
            # after operator-led guardrail script restoration.
            run_decision_matrix_hardening(output_dir=output_dir)
        elif action_key == "rerun_stage_timeout_guardrail_hardening":
            # Rebuild advisor context after timeout guardrail rerun so follow-up
            # actions in the same loop observe updated status.
            run_decision_matrix_hardening(output_dir=output_dir)
            run_kalshi_temperature_recovery_advisor(
                output_dir=output_dir,
                weather_window_hours=weather_window_hours,
                weather_min_bucket_samples=weather_min_bucket_samples,
                weather_max_profile_age_hours=weather_max_profile_age_hours,
                weather_negative_expectancy_attempt_share_target=0.50,
                weather_stale_metar_negative_attempt_share_target=0.60,
                weather_stale_metar_attempt_share_target=0.65,
                weather_min_attempts_target=200,
                optimizer_top_n=optimizer_top_n,
            )
        elif action_key == "refresh_recovery_stack":
            run_decision_matrix_hardening(output_dir=output_dir)
        else:
            return {
                "key": action_key,
                "status": "unknown_action",
                "error": f"Unsupported action key: {action_key}",
                "effect_status": "not_checked",
                "effect_reason": "",
                "effect_evidence": {},
                "counts_toward_effectiveness": False,
            }
    except Exception as exc:  # pragma: no cover - exercised via tests with synthetic failures only.
        return {
            "key": action_key,
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "effect_status": "not_checked",
            "effect_reason": "",
            "effect_evidence": {},
            "counts_toward_effectiveness": False,
        }

    effect_status, effect_reason, effect_evidence = _verify_trader_shadow_action_effect(
        action_key=action_key,
        output_dir=output_path,
        before_summary_path=before_trade_summary_path,
        before_summary_mtime_epoch=before_trade_summary_mtime_epoch,
    )
    trader_statuses = [
        _text(payload.get("status")).lower()
        for payload in trader_payloads
        if isinstance(payload, dict)
    ]
    if trader_statuses:
        effect_evidence["trader_statuses"] = trader_statuses
    if trader_payloads:
        trader_payload_diagnostics: list[dict[str, Any]] = []
        for probe_index, payload in enumerate(trader_payloads):
            call_kwargs = trader_call_kwargs[probe_index] if probe_index < len(trader_call_kwargs) else {}
            trader_payload_diagnostics.append(
                _build_trader_payload_diagnostics(
                    payload=payload,
                    call_kwargs=_as_dict(call_kwargs),
                    probe_index=probe_index,
                )
            )
        effect_evidence["trader_payload_diagnostics"] = trader_payload_diagnostics
    if telemetry_probe_diagnostics:
        effect_evidence["telemetry_probe_diagnostics"] = dict(telemetry_probe_diagnostics)
    if action_key == "reduce_execution_friction_pressure":
        effect_evidence["execution_cost_exclusion_candidate_count"] = int(len(execution_cost_exclusion_candidates))
        effect_evidence["execution_cost_exclusion_active_count"] = int(len(execution_cost_exclusion_tickers))
        effect_evidence["execution_cost_exclusion_market_side_candidate_count"] = int(
            len(execution_cost_exclusion_market_side_candidates)
        )
        effect_evidence["execution_cost_exclusion_market_side_active_count"] = int(
            len(execution_cost_exclusion_market_side_targets)
        )
        effect_evidence["execution_cost_exclusion_market_side_state_tracked_count"] = int(
            len(_as_dict(execution_cost_exclusion_state_payload.get("tracked_market_side_targets")))
        )
        effect_evidence["execution_cost_exclusion_market_side_target_count"] = int(
            len(execution_cost_exclusion_market_side_targets)
        )
        effect_evidence["execution_cost_exclusion_market_side_targets"] = list(
            execution_cost_exclusion_market_side_targets[:20]
        )
        effect_evidence["execution_cost_exclusion_active_target_count"] = int(
            execution_cost_exclusion_active_target_count
        )
        effect_evidence["execution_cost_exclusion_second_probe_market_side_target_count"] = int(
            len(execution_cost_exclusion_second_probe_market_side_targets)
        )
        market_side_activation_accelerator = _as_dict(
            execution_cost_exclusion_state_payload.get("market_side_activation_accelerator")
        )
        effect_evidence["execution_cost_exclusion_market_side_accelerator_engaged"] = bool(
            market_side_activation_accelerator.get("engaged")
        )
        effect_evidence["execution_cost_exclusion_market_side_accelerator_accelerated_count"] = int(
            _safe_int(market_side_activation_accelerator.get("accelerated_market_side_target_count"))
        )
        effect_evidence["execution_cost_exclusion_market_side_accelerator_accelerated_targets"] = list(
            market_side_activation_accelerator.get("accelerated_market_side_targets") or []
        )[:20]
        effect_evidence["execution_cost_exclusion_market_side_accelerator_trigger_reasons"] = [
            _text(value)
            for value in list(market_side_activation_accelerator.get("trigger_reasons") or [])
            if _text(value)
        ][:10]
        effect_evidence["execution_cost_exclusion_state_file"] = execution_cost_exclusion_state_file
        effect_evidence["execution_cost_exclusion_ticker_count"] = int(len(execution_cost_exclusion_tickers))
        effect_evidence["execution_cost_exclusion_tickers"] = list(execution_cost_exclusion_tickers[:20])
        effect_evidence["execution_cost_exclusion_downshift_triggered"] = bool(downshift_triggered)
        effect_evidence["execution_cost_exclusion_downshift_reason"] = downshift_reason
        effect_evidence["execution_cost_exclusion_downshift_active_count_before"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("active_count_before"))
        )
        effect_evidence["execution_cost_exclusion_downshift_active_count_after"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("active_count_after"))
        )
        effect_evidence["execution_cost_exclusion_downshift_removed_count"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("drop_count"))
        )
        effect_evidence["execution_cost_exclusion_downshift_removed_tickers"] = list(downshift_removed_tickers[:20])
        effect_evidence["execution_cost_exclusion_downshift_market_side_target_count_before"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("active_market_side_target_count_before"))
        )
        effect_evidence["execution_cost_exclusion_downshift_market_side_target_count_after"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("active_market_side_target_count_after"))
        )
        effect_evidence["execution_cost_exclusion_downshift_removed_market_side_target_count"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("drop_market_side_target_count"))
        )
        effect_evidence["execution_cost_exclusion_downshift_removed_market_side_targets"] = list(
            downshift_removed_market_side_targets[:20]
        )
        effect_evidence["execution_cost_exclusion_downshift_removed_market_side_targets_any"] = bool(
            downshift_removed_market_side_targets
        )
        effect_evidence["execution_cost_exclusion_downshift_near_cap_threshold"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("near_cap_threshold"))
        )
        effect_evidence["execution_cost_exclusion_downshift_pressure_near_cap"] = bool(
            execution_cost_exclusion_downshift_evidence.get("pressure_near_cap")
        )
        effect_evidence["execution_cost_exclusion_downshift_first_probe_blocked"] = bool(
            execution_cost_exclusion_downshift_evidence.get("first_probe_blocked")
        )
        effect_evidence["execution_cost_exclusion_downshift_throughput_stalled"] = bool(
            execution_cost_exclusion_downshift_evidence.get("throughput_stalled")
        )
        effect_evidence["execution_cost_exclusion_downshift_second_probe_improved"] = bool(
            execution_cost_exclusion_downshift_evidence.get("second_probe_improved")
        )
        effect_evidence["execution_cost_exclusion_downshift_coverage_signal_available"] = bool(
            execution_cost_exclusion_downshift_evidence.get("coverage_signal_available")
        )
        effect_evidence["execution_cost_exclusion_downshift_coverage_below_min"] = bool(
            execution_cost_exclusion_downshift_evidence.get("coverage_below_min")
        )
        effect_evidence["execution_cost_exclusion_downshift_coverage_improved_vs_previous"] = bool(
            execution_cost_exclusion_downshift_evidence.get("coverage_improved_vs_previous")
        )
        effect_evidence["execution_cost_exclusion_downshift_coverage_stalled"] = bool(
            execution_cost_exclusion_downshift_evidence.get("coverage_stalled")
        )
        effect_evidence["execution_cost_exclusion_downshift_quote_coverage_ratio"] = (
            execution_cost_exclusion_downshift_evidence.get("quote_coverage_ratio")
        )
        effect_evidence["execution_cost_exclusion_downshift_min_quote_coverage_ratio"] = (
            execution_cost_exclusion_downshift_evidence.get("min_quote_coverage_ratio")
        )
        effect_evidence["execution_cost_exclusion_downshift_previous_quote_coverage_ratio"] = (
            execution_cost_exclusion_downshift_evidence.get("previous_quote_coverage_ratio")
        )
        effect_evidence["execution_cost_exclusion_downshift_cooldown_active"] = bool(
            execution_cost_exclusion_downshift_evidence.get("cooldown_active")
        )
        effect_evidence["execution_cost_exclusion_downshift_cooldown_runs_remaining"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("cooldown_runs_remaining"))
        )
        effect_evidence["execution_cost_exclusion_downshift_suppressed_ticker_count"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("suppressed_ticker_count"))
        )
        effect_evidence["execution_cost_exclusion_downshift_suppressed_market_side_target_count"] = int(
            _safe_int(execution_cost_exclusion_downshift_evidence.get("suppressed_market_side_target_count"))
        )
        effect_evidence["execution_cost_exclusion_downshift_retained_tickers"] = list(execution_cost_exclusion_tickers[:20])
        effect_evidence["execution_cost_exclusion_downshift_retained_market_side_targets"] = list(
            execution_cost_exclusion_market_side_targets[:20]
        )
        effect_evidence["execution_cost_exclusion_adaptive_second_probe_triggered"] = bool(second_probe_triggered)
        effect_evidence["execution_cost_exclusion_adaptive_second_probe_reason"] = second_probe_reason
        effect_evidence["execution_cost_exclusion_second_probe_exclusion_count"] = int(second_probe_exclusion_count)
    if action_key == "improve_execution_quote_coverage_shadow":
        effect_evidence["quote_coverage_side_pressure_active"] = bool(
            telemetry_probe_diagnostics.get("side_pressure_active")
        )
        effect_evidence["quote_coverage_side_pressure_dominant_side"] = _text(
            telemetry_probe_diagnostics.get("dominant_side")
        )
        effect_evidence["quote_coverage_target_count_before_side_bias"] = int(
            _safe_int(telemetry_probe_diagnostics.get("shadow_quote_probe_target_count_before_side_bias"))
        )
        effect_evidence["quote_coverage_target_count_after_side_bias"] = int(
            _safe_int(telemetry_probe_diagnostics.get("shadow_quote_probe_target_count_after_side_bias"))
        )
    if action_key in TRADER_SHADOW_EFFECT_ACTION_KEYS:
        allowed_statuses = {
            "ready",
            "intents_only",
            "no_candidates",
            "dry_run",
            "dry_run_policy_blocked",
        }
        invalid_status = next((status for status in trader_statuses if status and status not in allowed_statuses), "")
        if invalid_status:
            effect_status = "no_effect"
            effect_reason = f"trader_status_{invalid_status}"
            counts_toward_effectiveness = False
        elif not trader_statuses and effect_status == "not_checked":
            effect_status = "no_effect"
            effect_reason = "trader_not_invoked"
            counts_toward_effectiveness = False
        else:
            counts_toward_effectiveness = True
    else:
        counts_toward_effectiveness = True
    return {
        "key": action_key,
        "status": "executed",
        "error": None,
        "replacement_profile_key": selected_replacement_profile_key,
        "effect_status": effect_status,
        "effect_reason": effect_reason,
        "effect_evidence": effect_evidence,
        "counts_toward_effectiveness": counts_toward_effectiveness,
    }


def run_kalshi_temperature_recovery_loop(
    *,
    output_dir: str,
    trader_env_file: str = "data/research/account_onboarding.env.template",
    max_iterations: int = 4,
    stall_iterations: int = 2,
    min_gap_improvement: float = 0.01,
    weather_window_hours: float = 720.0,
    weather_min_bucket_samples: int = 10,
    weather_max_profile_age_hours: float = 336.0,
    weather_negative_expectancy_attempt_share_target: float = 0.50,
    weather_stale_metar_negative_attempt_share_target: float = 0.60,
    weather_stale_metar_attempt_share_target: float = 0.65,
    weather_min_attempts_target: int = 200,
    optimizer_top_n: int = 5,
    plateau_negative_regime_suppression_enabled: bool = True,
    plateau_negative_regime_suppression_min_bucket_samples: int = 18,
    plateau_negative_regime_suppression_expectancy_threshold: float = -0.06,
    plateau_negative_regime_suppression_top_n: int = 10,
    retune_weather_window_hours_cap: float = 336.0,
    retune_overblocking_blocked_share_threshold: float = 0.25,
    retune_underblocking_min_top_n: int = 16,
    retune_overblocking_max_top_n: int = 4,
    retune_min_bucket_samples_target: int = 14,
    retune_expectancy_threshold_target: float = -0.045,
    execute_actions: bool = True,
) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    safe_max_iterations = max(0, int(max_iterations))
    safe_stall_iterations = max(1, int(stall_iterations))
    safe_min_gap_improvement = max(0.0, float(min_gap_improvement))
    safe_trader_env_file = _text(trader_env_file) or "data/research/account_onboarding.env.template"

    safe_weather_window_hours = max(1.0, float(weather_window_hours))
    safe_weather_min_bucket_samples = max(1, int(weather_min_bucket_samples))
    safe_weather_max_profile_age_hours = max(0.0, float(weather_max_profile_age_hours))
    safe_negative_target = max(0.0, min(1.0, float(weather_negative_expectancy_attempt_share_target)))
    safe_stale_negative_target = max(0.0, min(1.0, float(weather_stale_metar_negative_attempt_share_target)))
    safe_stale_target = max(0.0, min(1.0, float(weather_stale_metar_attempt_share_target)))
    safe_weather_min_attempts_target = max(1, int(weather_min_attempts_target))
    safe_optimizer_top_n = max(1, int(optimizer_top_n))
    safe_plateau_negative_regime_suppression_enabled = bool(plateau_negative_regime_suppression_enabled)
    safe_plateau_negative_regime_suppression_min_bucket_samples = max(
        1,
        int(plateau_negative_regime_suppression_min_bucket_samples),
    )
    safe_plateau_negative_regime_suppression_expectancy_threshold = float(
        plateau_negative_regime_suppression_expectancy_threshold
    )
    safe_plateau_negative_regime_suppression_top_n = max(
        1,
        int(plateau_negative_regime_suppression_top_n),
    )
    safe_retune_weather_window_hours_cap = max(1.0, float(retune_weather_window_hours_cap))
    safe_retune_overblocking_blocked_share_threshold = max(
        0.0,
        min(1.0, float(retune_overblocking_blocked_share_threshold)),
    )
    safe_retune_underblocking_min_top_n = max(1, int(retune_underblocking_min_top_n))
    safe_retune_overblocking_max_top_n = max(1, int(retune_overblocking_max_top_n))
    safe_retune_min_bucket_samples_target = max(1, int(retune_min_bucket_samples_target))
    safe_retune_expectancy_threshold_target = float(retune_expectancy_threshold_target)

    started_at = datetime.now(timezone.utc)
    initial_advisor_payload = run_kalshi_temperature_recovery_advisor(
        output_dir=str(out_dir),
        weather_window_hours=safe_weather_window_hours,
        weather_min_bucket_samples=safe_weather_min_bucket_samples,
        weather_max_profile_age_hours=safe_weather_max_profile_age_hours,
        weather_negative_expectancy_attempt_share_target=safe_negative_target,
        weather_stale_metar_negative_attempt_share_target=safe_stale_negative_target,
        weather_stale_metar_attempt_share_target=safe_stale_target,
        weather_min_attempts_target=safe_weather_min_attempts_target,
        optimizer_top_n=safe_optimizer_top_n,
    )

    current_payload = initial_advisor_payload
    current_status = _extract_advisor_status(current_payload)
    current_gap_score = _compute_gap_score(current_payload)
    current_negative_share = _extract_negative_expectancy_attempt_share(current_payload)

    iteration_logs: list[dict[str, Any]] = []
    stall_counter = 0
    termination_reason = "max_iterations"
    action_negative_share_worsening_streaks: dict[str, int] = {}
    action_effectiveness: dict[str, dict[str, int | float]] = {}
    auto_disabled_actions: set[str] = set()
    demoted_actions: set[str] = set()
    throttled_actions: set[str] = set()
    action_cooldowns: dict[str, int] = {}
    negative_share_delta_history: list[float] = []
    adaptive_effectiveness_thresholds = _compute_adaptive_effectiveness_thresholds(
        negative_share_before=current_negative_share,
        negative_share_after=current_negative_share,
        negative_share_delta_history=negative_share_delta_history,
    )
    adaptive_replacement_profile = _compute_adaptive_replacement_profile(adaptive_effectiveness_thresholds)
    replacement_usage_by_source: dict[str, int] = {}
    replacement_usage_by_action: dict[str, int] = {}
    tertiary_replacement_usage_by_source: dict[str, int] = {}
    tertiary_replacement_usage_by_action: dict[str, int] = {}
    final_fallback_replacement_usage_by_source: dict[str, int] = {}
    final_fallback_replacement_usage_by_action: dict[str, int] = {}
    final_fallback_decision_reason_counts: dict[str, int] = {}
    final_fallback_unavailable_reason_counts: dict[str, int] = {}
    replacement_route_arbitration_counts: dict[str, int] = {}
    replacement_route_selection_counts: dict[str, int] = {}
    tertiary_reserve_protection_progress: dict[str, int] = {}
    tertiary_reserve_protection_reason_counters: dict[str, int] = {}
    tertiary_reserve_suppressed_demotion_counts: dict[str, int] = {}
    tertiary_reserve_suppressed_auto_disable_counts: dict[str, int] = {}
    latest_tertiary_reserve_protected_actions: list[str] = []
    latest_tertiary_reserve_protection_state: dict[str, dict[str, Any]] = {}
    final_fallback_reserve_protection_progress: dict[str, int] = {}
    final_fallback_reserve_protection_reason_counters: dict[str, int] = {}
    final_fallback_reserve_suppressed_demotion_counts: dict[str, int] = {}
    final_fallback_reserve_suppressed_auto_disable_counts: dict[str, int] = {}
    latest_final_fallback_reserve_protected_actions: list[str] = []
    latest_final_fallback_reserve_protection_state: dict[str, dict[str, Any]] = {}
    last_iteration_gap_improvement: float | None = None
    last_iteration_negative_share_delta: float | None = None

    if current_status == "risk_off_cleared":
        termination_reason = "cleared"
    elif not bool(execute_actions):
        termination_reason = "actions_disabled"
    elif current_status == "insufficient_data" and safe_max_iterations == 0:
        termination_reason = "insufficient_data"
    else:
        for iteration in range(1, safe_max_iterations + 1):
            advisor_status_before = current_status
            gap_score_before = current_gap_score
            negative_share_before = current_negative_share
            adaptive_replacement_profile = _compute_adaptive_replacement_profile(adaptive_effectiveness_thresholds)
            selected_replacement_profile_key = (
                _text(adaptive_replacement_profile.get("selected_profile")) or DEFAULT_REPLACEMENT_PROFILE_KEY
            )
            reserve_release_override = _compute_reserve_release_override(
                prior_gap_improvement=last_iteration_gap_improvement,
                prior_negative_share_delta=last_iteration_negative_share_delta,
                min_gap_improvement=safe_min_gap_improvement,
            )
            reserve_release_override_active = bool(reserve_release_override.get("active"))
            reserve_release_override_reasons = [
                _text(value) for value in list(reserve_release_override.get("reasons", [])) if _text(value)
            ]
            action_keys = _extract_action_keys(current_payload)
            executed_actions: list[dict[str, Any]] = []
            replacement_sources_executed: set[str] = set()
            replacement_rows: list[dict[str, Any]] = []
            iteration_replacement_route_arbitration_counts: dict[str, int] = {}
            iteration_replacement_route_selection_counts: dict[str, int] = {}
            iteration_final_fallback_decision_reason_counts: dict[str, int] = {}
            iteration_final_fallback_unavailable_reason_counts: dict[str, int] = {}
            tertiary_reserve_candidates = _collect_tertiary_reserve_candidates(
                demoted_actions=demoted_actions,
                auto_disabled_actions=auto_disabled_actions,
            )
            final_fallback_reserve_candidates = _collect_final_fallback_reserve_candidates(
                demoted_actions=demoted_actions,
                auto_disabled_actions=auto_disabled_actions,
            )
            final_fallback_candidate_action_keys = _collect_final_fallback_candidate_action_keys(
                demoted_actions=demoted_actions,
            )
            for tracked_action_key in list(tertiary_reserve_protection_progress):
                if tracked_action_key not in tertiary_reserve_candidates:
                    tertiary_reserve_protection_progress.pop(tracked_action_key, None)
                    _increment_counter(
                        tertiary_reserve_protection_reason_counters,
                        "released_no_longer_needed",
                    )
            for tracked_action_key in list(final_fallback_reserve_protection_progress):
                if (
                    tracked_action_key not in final_fallback_reserve_candidates
                    and tracked_action_key not in final_fallback_candidate_action_keys
                ):
                    final_fallback_reserve_protection_progress.pop(tracked_action_key, None)
                    _increment_counter(
                        final_fallback_reserve_protection_reason_counters,
                        "released_no_longer_needed",
                    )
            protected_tertiary_reserve_actions: set[str] = set()
            tertiary_reserve_protection_state: dict[str, dict[str, Any]] = {}
            for reserve_action_key in sorted(tertiary_reserve_candidates):
                reserve_row = tertiary_reserve_candidates[reserve_action_key]
                iterations_used_before = int(tertiary_reserve_protection_progress.get(reserve_action_key, 0))
                should_protect = iterations_used_before < TERTIARY_RESERVE_PROTECTION_MAX_ITERATIONS
                reactivated_from_demoted = False
                reactivated_from_auto_disabled = False
                reactivation_blocked = False
                if should_protect:
                    iterations_used_after = iterations_used_before + 1
                    tertiary_reserve_protection_progress[reserve_action_key] = iterations_used_after
                    protected_tertiary_reserve_actions.add(reserve_action_key)
                    _increment_counter(
                        tertiary_reserve_protection_reason_counters,
                        "protected_strict_replacement_viable",
                    )
                    needs_reactivation = bool(
                        reserve_action_key in demoted_actions or reserve_action_key in auto_disabled_actions
                    )
                    if needs_reactivation and not reserve_release_override_active:
                        reactivation_blocked = True
                        _increment_counter(
                            tertiary_reserve_protection_reason_counters,
                            "release_blocked_no_quality_override",
                        )
                    else:
                        if needs_reactivation:
                            _increment_counter(
                                tertiary_reserve_protection_reason_counters,
                                "release_override_reactivation_allowed",
                            )
                        if reserve_action_key in demoted_actions:
                            demoted_actions.discard(reserve_action_key)
                            reactivated_from_demoted = True
                            _increment_counter(
                                tertiary_reserve_protection_reason_counters,
                                "reactivated_from_demoted",
                            )
                        if reserve_action_key in auto_disabled_actions:
                            auto_disabled_actions.discard(reserve_action_key)
                            reactivated_from_auto_disabled = True
                            _increment_counter(
                                tertiary_reserve_protection_reason_counters,
                                "reactivated_from_auto_disabled",
                            )
                    protection_status = "protected"
                else:
                    iterations_used_after = iterations_used_before
                    _increment_counter(
                        tertiary_reserve_protection_reason_counters,
                        "skipped_iteration_cap_reached",
                    )
                    protection_status = "cap_reached"
                tertiary_reserve_protection_state[reserve_action_key] = {
                    "source_actions": list(reserve_row.get("source_actions", [])),
                    "strict_replacement_actions": list(reserve_row.get("strict_replacement_actions", [])),
                    "protection_status": protection_status,
                    "iterations_used_before": int(iterations_used_before),
                    "iterations_used_after": int(iterations_used_after),
                    "remaining_iteration_budget": max(
                        0,
                        int(TERTIARY_RESERVE_PROTECTION_MAX_ITERATIONS - iterations_used_after),
                    ),
                    "reactivated_from_demoted": bool(reactivated_from_demoted),
                    "reactivated_from_auto_disabled": bool(reactivated_from_auto_disabled),
                    "reactivation_blocked": bool(reactivation_blocked),
                    "release_override_active": bool(reserve_release_override_active),
                    "release_override_reasons": list(reserve_release_override_reasons),
                }
            latest_tertiary_reserve_protected_actions = sorted(protected_tertiary_reserve_actions)
            latest_tertiary_reserve_protection_state = dict(
                sorted(tertiary_reserve_protection_state.items(), key=lambda item: item[0])
            )
            protected_final_fallback_reserve_actions: set[str] = set()
            final_fallback_reserve_protection_state: dict[str, dict[str, Any]] = {}
            for reserve_action_key in sorted(final_fallback_reserve_candidates):
                reserve_row = final_fallback_reserve_candidates[reserve_action_key]
                iterations_used_before = int(final_fallback_reserve_protection_progress.get(reserve_action_key, 0))
                should_protect = iterations_used_before < FINAL_FALLBACK_RESERVE_PROTECTION_MAX_ITERATIONS
                reactivated_from_demoted = False
                reactivated_from_auto_disabled = False
                reactivation_blocked = False
                if should_protect:
                    iterations_used_after = iterations_used_before + 1
                    final_fallback_reserve_protection_progress[reserve_action_key] = iterations_used_after
                    protected_final_fallback_reserve_actions.add(reserve_action_key)
                    _increment_counter(
                        final_fallback_reserve_protection_reason_counters,
                        "protected_strict_or_tertiary_replacement_viable",
                    )
                    needs_reactivation = bool(
                        reserve_action_key in demoted_actions or reserve_action_key in auto_disabled_actions
                    )
                    if needs_reactivation and not reserve_release_override_active:
                        reactivation_blocked = True
                        _increment_counter(
                            final_fallback_reserve_protection_reason_counters,
                            "release_blocked_no_quality_override",
                        )
                    else:
                        if needs_reactivation:
                            _increment_counter(
                                final_fallback_reserve_protection_reason_counters,
                                "release_override_reactivation_allowed",
                            )
                        if reserve_action_key in demoted_actions:
                            demoted_actions.discard(reserve_action_key)
                            reactivated_from_demoted = True
                            _increment_counter(
                                final_fallback_reserve_protection_reason_counters,
                                "reactivated_from_demoted",
                            )
                        if reserve_action_key in auto_disabled_actions:
                            auto_disabled_actions.discard(reserve_action_key)
                            reactivated_from_auto_disabled = True
                            _increment_counter(
                                final_fallback_reserve_protection_reason_counters,
                                "reactivated_from_auto_disabled",
                            )
                    protection_status = "protected"
                else:
                    iterations_used_after = iterations_used_before
                    _increment_counter(
                        final_fallback_reserve_protection_reason_counters,
                        "skipped_iteration_cap_reached",
                    )
                    protection_status = "cap_reached"
                final_fallback_reserve_protection_state[reserve_action_key] = {
                    "source_actions": list(reserve_row.get("source_actions", [])),
                    "strict_replacement_actions": list(reserve_row.get("strict_replacement_actions", [])),
                    "tertiary_replacement_actions": list(reserve_row.get("tertiary_replacement_actions", [])),
                    "protection_status": protection_status,
                    "iterations_used_before": int(iterations_used_before),
                    "iterations_used_after": int(iterations_used_after),
                    "remaining_iteration_budget": max(
                        0,
                        int(FINAL_FALLBACK_RESERVE_PROTECTION_MAX_ITERATIONS - iterations_used_after),
                    ),
                    "reactivated_from_demoted": bool(reactivated_from_demoted),
                    "reactivated_from_auto_disabled": bool(reactivated_from_auto_disabled),
                    "reactivation_blocked": bool(reactivation_blocked),
                    "release_override_active": bool(reserve_release_override_active),
                    "release_override_reasons": list(reserve_release_override_reasons),
                }
            latest_final_fallback_reserve_protected_actions = sorted(protected_final_fallback_reserve_actions)
            latest_final_fallback_reserve_protection_state = dict(
                sorted(final_fallback_reserve_protection_state.items(), key=lambda item: item[0])
            )
            for action_key in action_keys:
                policy_class, cooldown_iterations = _policy_class_and_cooldown(action_key)
                if action_key in demoted_actions:
                    action_cooldowns.pop(action_key, None)
                    route = _resolve_demoted_source_replacement_route(
                        action_key=action_key,
                        policy_class=policy_class,
                        demoted_actions=demoted_actions,
                        auto_disabled_actions=auto_disabled_actions,
                        replacement_sources_executed=replacement_sources_executed,
                        action_effectiveness=action_effectiveness,
                    )
                    strict_replacement_action_key = _text(route.get("strict_replacement_action_key"))
                    strict_replacement_action_viable = bool(route.get("strict_replacement_action_viable"))
                    strict_replacement_unavailable_reason = _text(route.get("strict_replacement_unavailable_reason"))
                    tertiary_replacement_action_key = _text(route.get("tertiary_replacement_action_key"))
                    tertiary_replacement_action_viable = bool(route.get("tertiary_replacement_action_viable"))
                    tertiary_replacement_unavailable_reason = _text(route.get("tertiary_replacement_unavailable_reason"))
                    final_fallback_action_key = _text(route.get("final_fallback_action_key"))
                    final_fallback_action_viable = bool(route.get("final_fallback_action_viable"))
                    final_fallback_unavailable_reason = _text(route.get("final_fallback_unavailable_reason"))
                    replacement_route_stage = _text(route.get("replacement_route_stage"))
                    selected_replacement_action_key = _text(route.get("selected_replacement_action_key"))
                    replacement_route_arbitration = _as_dict(route.get("replacement_route_arbitration"))
                    arbitration_decision_reason = _text(replacement_route_arbitration.get("decision_reason"))
                    arbitration_selected_route_stage = _text(
                        replacement_route_arbitration.get("selected_route_stage")
                    )
                    _increment_counter(
                        iteration_replacement_route_selection_counts,
                        arbitration_selected_route_stage or "none",
                    )
                    _increment_counter(
                        replacement_route_selection_counts,
                        arbitration_selected_route_stage or "none",
                    )
                    if arbitration_decision_reason:
                        _increment_counter(
                            iteration_replacement_route_arbitration_counts,
                            arbitration_decision_reason,
                        )
                        _increment_counter(
                            replacement_route_arbitration_counts,
                            arbitration_decision_reason,
                        )
                    if replacement_route_stage == "final_fallback":
                        _increment_counter(
                            iteration_final_fallback_decision_reason_counts,
                            arbitration_decision_reason or "final_fallback_selected",
                        )
                        _increment_counter(
                            final_fallback_decision_reason_counts,
                            arbitration_decision_reason or "final_fallback_selected",
                        )
                    if final_fallback_unavailable_reason:
                        _increment_counter(
                            iteration_final_fallback_unavailable_reason_counts,
                            final_fallback_unavailable_reason,
                        )
                        _increment_counter(
                            final_fallback_unavailable_reason_counts,
                            final_fallback_unavailable_reason,
                        )
                    if replacement_route_stage == "final_fallback" and selected_replacement_action_key:
                        selected_reactivated_from_demoted = False
                        selected_reactivated_from_auto_disabled = False
                        selected_iterations_used_before = int(
                            final_fallback_reserve_protection_progress.get(selected_replacement_action_key, 0)
                        )
                        selected_should_protect = (
                            selected_iterations_used_before < FINAL_FALLBACK_RESERVE_PROTECTION_MAX_ITERATIONS
                        )
                        if selected_should_protect:
                            selected_iterations_used_after = selected_iterations_used_before + 1
                            final_fallback_reserve_protection_progress[selected_replacement_action_key] = (
                                selected_iterations_used_after
                            )
                            protected_final_fallback_reserve_actions.add(selected_replacement_action_key)
                            _increment_counter(
                                final_fallback_reserve_protection_reason_counters,
                                "protected_selected_final_fallback_route",
                            )
                            if selected_replacement_action_key in demoted_actions:
                                demoted_actions.discard(selected_replacement_action_key)
                                selected_reactivated_from_demoted = True
                                _increment_counter(
                                    final_fallback_reserve_protection_reason_counters,
                                    "reactivated_from_demoted",
                                )
                            if selected_replacement_action_key in auto_disabled_actions:
                                auto_disabled_actions.discard(selected_replacement_action_key)
                                selected_reactivated_from_auto_disabled = True
                                _increment_counter(
                                    final_fallback_reserve_protection_reason_counters,
                                    "reactivated_from_auto_disabled",
                                )
                            existing_state = _as_dict(
                                final_fallback_reserve_protection_state.get(selected_replacement_action_key)
                            )
                            source_actions = sorted(
                                set(
                                    [
                                        _text(value)
                                        for value in (
                                            list(existing_state.get("source_actions", [])) + [action_key]
                                        )
                                        if _text(value)
                                    ]
                                )
                            )
                            strict_replacement_actions = sorted(
                                set(
                                    [
                                        _text(value)
                                        for value in (
                                            list(existing_state.get("strict_replacement_actions", []))
                                            + [strict_replacement_action_key]
                                        )
                                        if _text(value)
                                    ]
                                )
                            )
                            tertiary_replacement_actions = sorted(
                                set(
                                    [
                                        _text(value)
                                        for value in (
                                            list(existing_state.get("tertiary_replacement_actions", []))
                                            + [tertiary_replacement_action_key]
                                        )
                                        if _text(value)
                                    ]
                                )
                            )
                            final_fallback_reserve_protection_state[selected_replacement_action_key] = {
                                "source_actions": source_actions,
                                "strict_replacement_actions": strict_replacement_actions,
                                "tertiary_replacement_actions": tertiary_replacement_actions,
                                "protection_status": "protected",
                                "iterations_used_before": int(selected_iterations_used_before),
                                "iterations_used_after": int(selected_iterations_used_after),
                                "remaining_iteration_budget": max(
                                    0,
                                    int(
                                        FINAL_FALLBACK_RESERVE_PROTECTION_MAX_ITERATIONS
                                        - selected_iterations_used_after
                                    ),
                                ),
                                "reactivated_from_demoted": bool(selected_reactivated_from_demoted),
                                "reactivated_from_auto_disabled": bool(selected_reactivated_from_auto_disabled),
                                "selection_protection": True,
                            }
                        else:
                            _increment_counter(
                                final_fallback_reserve_protection_reason_counters,
                                "skipped_selected_route_cap_reached",
                            )
                    can_replace = bool(
                        selected_replacement_action_key
                        and replacement_route_stage in {"strict", "tertiary", "final_fallback"}
                    )
                    source_row = {
                        "key": action_key,
                        "status": "auto_disabled",
                        "error": None,
                        "negative_share_worsening_streak": int(
                            action_negative_share_worsening_streaks.get(action_key, 0)
                        ),
                        "policy_class": policy_class,
                    }
                    if strict_replacement_action_key:
                        source_row["replacement_action_key"] = strict_replacement_action_key
                        source_row["replacement_action_viable"] = bool(strict_replacement_action_viable)
                    if tertiary_replacement_action_key:
                        source_row["tertiary_replacement_action_key"] = tertiary_replacement_action_key
                        source_row["tertiary_replacement_action_viable"] = bool(tertiary_replacement_action_viable)
                        source_row["tertiary_reserve_protected"] = bool(
                            tertiary_replacement_action_key in protected_tertiary_reserve_actions
                        )
                    if final_fallback_action_key:
                        source_row["final_fallback_action_key"] = final_fallback_action_key
                        source_row["final_fallback_action_viable"] = bool(final_fallback_action_viable)
                        source_row["final_fallback_reserve_protected"] = bool(
                            final_fallback_action_key in protected_final_fallback_reserve_actions
                        )
                    source_row["replacement_route_stage"] = replacement_route_stage
                    source_row["adaptive_replacement_profile"] = dict(adaptive_replacement_profile)
                    source_row["replacement_route_arbitration"] = dict(replacement_route_arbitration)
                    if can_replace:
                        source_row["reason"] = _text(route.get("source_reason")) or "demoted_source_replacement_unavailable"
                        source_row["replacement_routing_status"] = "routed"
                        source_row["selected_replacement_action_key"] = selected_replacement_action_key
                    else:
                        source_row["reason"] = (
                            _text(route.get("source_reason"))
                            or "auto_disabled_effectiveness_demotion_persistently_harmful"
                        )
                        source_row["replacement_routing_status"] = _text(route.get("replacement_routing_status")) or "not_mapped"
                    if strict_replacement_unavailable_reason:
                        source_row["replacement_unavailable_reason"] = strict_replacement_unavailable_reason
                    if tertiary_replacement_unavailable_reason:
                        source_row["tertiary_replacement_unavailable_reason"] = tertiary_replacement_unavailable_reason
                    if final_fallback_unavailable_reason:
                        source_row["final_fallback_unavailable_reason"] = final_fallback_unavailable_reason
                    executed_actions.append(source_row)
                    if can_replace:
                        replacement_sources_executed.add(action_key)
                        replacement_row = _execute_action(
                            action_key=selected_replacement_action_key,
                            output_dir=str(out_dir),
                            trader_env_file=safe_trader_env_file,
                            weather_window_hours=safe_weather_window_hours,
                            weather_min_bucket_samples=safe_weather_min_bucket_samples,
                            weather_max_profile_age_hours=safe_weather_max_profile_age_hours,
                            optimizer_top_n=safe_optimizer_top_n,
                            plateau_negative_regime_suppression_enabled=safe_plateau_negative_regime_suppression_enabled,
                            plateau_negative_regime_suppression_min_bucket_samples=(
                                safe_plateau_negative_regime_suppression_min_bucket_samples
                            ),
                            plateau_negative_regime_suppression_expectancy_threshold=(
                                safe_plateau_negative_regime_suppression_expectancy_threshold
                            ),
                            plateau_negative_regime_suppression_top_n=safe_plateau_negative_regime_suppression_top_n,
                            retune_weather_window_hours_cap=safe_retune_weather_window_hours_cap,
                            retune_overblocking_blocked_share_threshold=safe_retune_overblocking_blocked_share_threshold,
                            retune_underblocking_min_top_n=safe_retune_underblocking_min_top_n,
                            retune_overblocking_max_top_n=safe_retune_overblocking_max_top_n,
                            retune_min_bucket_samples_target=safe_retune_min_bucket_samples_target,
                            retune_expectancy_threshold_target=safe_retune_expectancy_threshold_target,
                            replacement_profile_key=selected_replacement_profile_key,
                        )
                        replacement_row["reason"] = "replacement_for_demoted_action_effectiveness"
                        replacement_row["replacement_for_action_key"] = action_key
                        replacement_row["source_action_policy_class"] = policy_class
                        replacement_row["replacement_routing_status"] = "executed"
                        replacement_row["replacement_route_stage"] = replacement_route_stage
                        replacement_row["adaptive_replacement_profile"] = dict(adaptive_replacement_profile)
                        replacement_row["replacement_route_arbitration"] = dict(replacement_route_arbitration)
                        replacement_row["final_fallback_reserve_protected"] = bool(
                            selected_replacement_action_key in protected_final_fallback_reserve_actions
                        )
                        replacement_usage_by_source[action_key] = int(
                            replacement_usage_by_source.get(action_key, 0)
                        ) + 1
                        replacement_usage_by_action[selected_replacement_action_key] = int(
                            replacement_usage_by_action.get(selected_replacement_action_key, 0)
                        ) + 1
                        if replacement_route_stage == "tertiary":
                            tertiary_replacement_usage_by_source[action_key] = int(
                                tertiary_replacement_usage_by_source.get(action_key, 0)
                            ) + 1
                            tertiary_replacement_usage_by_action[selected_replacement_action_key] = int(
                                tertiary_replacement_usage_by_action.get(selected_replacement_action_key, 0)
                            ) + 1
                        if replacement_route_stage == "final_fallback":
                            final_fallback_replacement_usage_by_source[action_key] = int(
                                final_fallback_replacement_usage_by_source.get(action_key, 0)
                            ) + 1
                            final_fallback_replacement_usage_by_action[selected_replacement_action_key] = int(
                                final_fallback_replacement_usage_by_action.get(selected_replacement_action_key, 0)
                            ) + 1
                        replacement_rows.append(
                            {
                                "source_action_key": action_key,
                                "replacement_action_key": selected_replacement_action_key,
                                "strict_replacement_action_key": strict_replacement_action_key,
                                "tertiary_replacement_action_key": tertiary_replacement_action_key,
                                "final_fallback_action_key": final_fallback_action_key,
                                "replacement_route_stage": replacement_route_stage,
                                "status": _text(replacement_row.get("status")).lower(),
                                "reason": _text(replacement_row.get("reason")),
                                "adaptive_replacement_profile": dict(adaptive_replacement_profile),
                                "replacement_route_arbitration": dict(replacement_route_arbitration),
                                "tertiary_reserve_protected": bool(
                                    selected_replacement_action_key in protected_tertiary_reserve_actions
                                ),
                                "final_fallback_reserve_protected": bool(
                                    selected_replacement_action_key in protected_final_fallback_reserve_actions
                                ),
                            }
                        )
                        executed_actions.append(replacement_row)
                    else:
                        replacement_row_status = (
                            "unavailable"
                            if strict_replacement_action_key
                            or tertiary_replacement_action_key
                            or final_fallback_action_key
                            else "not_routed"
                        )
                        replacement_unavailable_reason = (
                            strict_replacement_unavailable_reason
                            or tertiary_replacement_unavailable_reason
                            or final_fallback_unavailable_reason
                        )
                        replacement_rows.append(
                            {
                                "source_action_key": action_key,
                                "replacement_action_key": (
                                    strict_replacement_action_key
                                    or tertiary_replacement_action_key
                                    or final_fallback_action_key
                                ),
                                "strict_replacement_action_key": strict_replacement_action_key,
                                "tertiary_replacement_action_key": tertiary_replacement_action_key,
                                "final_fallback_action_key": final_fallback_action_key,
                                "replacement_route_stage": "none",
                                "status": replacement_row_status,
                                "reason": replacement_unavailable_reason,
                                "tertiary_reason": tertiary_replacement_unavailable_reason,
                                "final_fallback_reason": final_fallback_unavailable_reason,
                                "adaptive_replacement_profile": dict(adaptive_replacement_profile),
                                "replacement_route_arbitration": dict(replacement_route_arbitration),
                                "tertiary_reserve_protected": bool(
                                    tertiary_replacement_action_key
                                    and tertiary_replacement_action_key in protected_tertiary_reserve_actions
                                ),
                                "final_fallback_reserve_protected": bool(
                                    final_fallback_action_key
                                    and final_fallback_action_key in protected_final_fallback_reserve_actions
                                ),
                            }
                        )
                    continue
                if action_key in auto_disabled_actions:
                    executed_actions.append(
                        {
                            "key": action_key,
                            "status": "auto_disabled",
                            "error": None,
                            "reason": "auto_disabled_after_consecutive_negative_share_worsening",
                            "negative_share_worsening_streak": int(
                                action_negative_share_worsening_streaks.get(action_key, 0)
                            ),
                            "policy_class": policy_class,
                        }
                    )
                    continue

                cooldown_remaining_before = int(action_cooldowns.get(action_key, 0))
                if cooldown_remaining_before > 0:
                    cooldown_remaining_after = max(0, cooldown_remaining_before - 1)
                    if cooldown_remaining_after > 0:
                        action_cooldowns[action_key] = cooldown_remaining_after
                    else:
                        action_cooldowns.pop(action_key, None)
                    executed_actions.append(
                        {
                            "key": action_key,
                            "status": "cooldown_skip",
                            "error": None,
                            "reason": "throttled_after_consecutive_negative_share_worsening",
                            "negative_share_worsening_streak": int(
                                action_negative_share_worsening_streaks.get(action_key, 0)
                            ),
                            "cooldown_remaining_before": cooldown_remaining_before,
                            "cooldown_remaining_after": cooldown_remaining_after,
                            "policy_class": policy_class,
                        }
                    )
                    continue

                executed_actions.append(
                    _execute_action(
                        action_key=action_key,
                        output_dir=str(out_dir),
                        trader_env_file=safe_trader_env_file,
                        weather_window_hours=safe_weather_window_hours,
                        weather_min_bucket_samples=safe_weather_min_bucket_samples,
                        weather_max_profile_age_hours=safe_weather_max_profile_age_hours,
                        optimizer_top_n=safe_optimizer_top_n,
                        plateau_negative_regime_suppression_enabled=safe_plateau_negative_regime_suppression_enabled,
                        plateau_negative_regime_suppression_min_bucket_samples=(
                            safe_plateau_negative_regime_suppression_min_bucket_samples
                        ),
                        plateau_negative_regime_suppression_expectancy_threshold=(
                            safe_plateau_negative_regime_suppression_expectancy_threshold
                        ),
                        plateau_negative_regime_suppression_top_n=safe_plateau_negative_regime_suppression_top_n,
                        retune_weather_window_hours_cap=safe_retune_weather_window_hours_cap,
                        retune_overblocking_blocked_share_threshold=safe_retune_overblocking_blocked_share_threshold,
                        retune_underblocking_min_top_n=safe_retune_underblocking_min_top_n,
                        retune_overblocking_max_top_n=safe_retune_overblocking_max_top_n,
                        retune_min_bucket_samples_target=safe_retune_min_bucket_samples_target,
                        retune_expectancy_threshold_target=safe_retune_expectancy_threshold_target,
                        replacement_profile_key=selected_replacement_profile_key,
                    )
                )

            primary_action_rows = [
                row for row in executed_actions if _text(row.get("key")) in set(action_keys)
            ]
            if primary_action_rows:
                primary_statuses = {_text(row.get("status")).lower() for row in primary_action_rows}
                primary_actions_unavailable = primary_statuses.issubset({"auto_disabled", "cooldown_skip"})
            else:
                primary_actions_unavailable = False
            if primary_actions_unavailable:
                defensive_row = _execute_action(
                    action_key=DEFENSIVE_PIVOT_ACTION_KEY,
                    output_dir=str(out_dir),
                    trader_env_file=safe_trader_env_file,
                    weather_window_hours=safe_weather_window_hours,
                    weather_min_bucket_samples=safe_weather_min_bucket_samples,
                    weather_max_profile_age_hours=safe_weather_max_profile_age_hours,
                    optimizer_top_n=safe_optimizer_top_n,
                    plateau_negative_regime_suppression_enabled=safe_plateau_negative_regime_suppression_enabled,
                    plateau_negative_regime_suppression_min_bucket_samples=(
                        safe_plateau_negative_regime_suppression_min_bucket_samples
                    ),
                    plateau_negative_regime_suppression_expectancy_threshold=(
                        safe_plateau_negative_regime_suppression_expectancy_threshold
                    ),
                    plateau_negative_regime_suppression_top_n=safe_plateau_negative_regime_suppression_top_n,
                    retune_weather_window_hours_cap=safe_retune_weather_window_hours_cap,
                    retune_overblocking_blocked_share_threshold=safe_retune_overblocking_blocked_share_threshold,
                    retune_underblocking_min_top_n=safe_retune_underblocking_min_top_n,
                    retune_overblocking_max_top_n=safe_retune_overblocking_max_top_n,
                    retune_min_bucket_samples_target=safe_retune_min_bucket_samples_target,
                    retune_expectancy_threshold_target=safe_retune_expectancy_threshold_target,
                    replacement_profile_key=selected_replacement_profile_key,
                )
                defensive_row["reason"] = "auto_defensive_pivot_all_primary_actions_unavailable"
                defensive_row["unavailable_primary_actions"] = [
                    {
                        "key": _text(row.get("key")),
                        "status": _text(row.get("status")).lower(),
                    }
                    for row in primary_action_rows
                ]
                executed_actions.append(defensive_row)

            next_payload = run_kalshi_temperature_recovery_advisor(
                output_dir=str(out_dir),
                weather_window_hours=safe_weather_window_hours,
                weather_min_bucket_samples=safe_weather_min_bucket_samples,
                weather_max_profile_age_hours=safe_weather_max_profile_age_hours,
                weather_negative_expectancy_attempt_share_target=safe_negative_target,
                weather_stale_metar_negative_attempt_share_target=safe_stale_negative_target,
                weather_stale_metar_attempt_share_target=safe_stale_target,
                weather_min_attempts_target=safe_weather_min_attempts_target,
                optimizer_top_n=safe_optimizer_top_n,
            )
            advisor_status_after = _extract_advisor_status(next_payload)
            gap_score_after = _compute_gap_score(next_payload)
            negative_share_after = _extract_negative_expectancy_attempt_share(next_payload)
            improvement = round(float(gap_score_before - gap_score_after), 6)
            negative_share_delta = None
            if isinstance(negative_share_before, float) and isinstance(negative_share_after, float):
                negative_share_delta = round(float(negative_share_after - negative_share_before), 6)
            negative_share_worsened = bool(
                isinstance(negative_share_delta, float) and negative_share_delta > NEGATIVE_SHARE_WORSENING_EPSILON
            )
            if isinstance(negative_share_delta, float):
                negative_share_delta_history.append(float(negative_share_delta))
                if len(negative_share_delta_history) > 12:
                    negative_share_delta_history.pop(0)
            adaptive_effectiveness_thresholds = _compute_adaptive_effectiveness_thresholds(
                negative_share_before=negative_share_before,
                negative_share_after=negative_share_after,
                negative_share_delta_history=negative_share_delta_history,
            )
            adaptive_replacement_profile = _compute_adaptive_replacement_profile(adaptive_effectiveness_thresholds)

            for action_row in executed_actions:
                action_key = _text(action_row.get("key"))
                action_status = _text(action_row.get("status")).lower()
                if action_key == DEFENSIVE_PIVOT_ACTION_KEY:
                    continue
                if action_row.get("counts_toward_effectiveness") is False:
                    continue
                if not action_key or action_status != "executed":
                    continue

                reserve_protected = action_key in protected_tertiary_reserve_actions or (
                    action_key in protected_final_fallback_reserve_actions
                )
                if action_key in protected_tertiary_reserve_actions:
                    action_row["tertiary_reserve_protected"] = True
                if action_key in protected_final_fallback_reserve_actions:
                    action_row["final_fallback_reserve_protected"] = True
                metrics = action_effectiveness.setdefault(action_key, _new_action_effectiveness_metrics())
                metrics["executed_count"] = max(0, _safe_int(metrics.get("executed_count"))) + 1
                if negative_share_worsened:
                    metrics["worsening_count"] = max(0, _safe_int(metrics.get("worsening_count"))) + 1
                else:
                    metrics["non_worsening_count"] = max(0, _safe_int(metrics.get("non_worsening_count"))) + 1
                negative_share_delta_for_metrics = (
                    float(negative_share_delta) if isinstance(negative_share_delta, float) else 0.0
                )
                metrics["cumulative_negative_share_delta"] = round(
                    float(_safe_float(metrics.get("cumulative_negative_share_delta")) or 0.0)
                    + float(negative_share_delta_for_metrics),
                    6,
                )
                executed_count = max(1, _safe_int(metrics.get("executed_count")))
                metrics["average_negative_share_delta"] = round(
                    float(_safe_float(metrics.get("cumulative_negative_share_delta")) or 0.0)
                    / float(executed_count),
                    6,
                )

                if negative_share_worsened:
                    next_streak = int(action_negative_share_worsening_streaks.get(action_key, 0)) + 1
                    action_negative_share_worsening_streaks[action_key] = next_streak
                    if next_streak >= NEGATIVE_SHARE_WORSENING_STREAK_THRESHOLD:
                        if reserve_protected:
                            action_negative_share_worsening_streaks[action_key] = min(
                                next_streak,
                                max(0, int(NEGATIVE_SHARE_WORSENING_STREAK_THRESHOLD - 1)),
                            )
                            if action_key in protected_tertiary_reserve_actions:
                                _increment_counter(
                                    tertiary_reserve_protection_reason_counters,
                                    "suppressed_consecutive_worsening_disable",
                                )
                                _increment_counter(
                                    tertiary_reserve_suppressed_auto_disable_counts,
                                    action_key,
                                )
                            if action_key in protected_final_fallback_reserve_actions:
                                _increment_counter(
                                    final_fallback_reserve_protection_reason_counters,
                                    "suppressed_consecutive_worsening_disable",
                                )
                                _increment_counter(
                                    final_fallback_reserve_suppressed_auto_disable_counts,
                                    action_key,
                                )
                        else:
                            policy_class, cooldown_iterations = _policy_class_and_cooldown(action_key)
                            if isinstance(cooldown_iterations, int) and cooldown_iterations > 0:
                                action_cooldowns[action_key] = max(
                                    cooldown_iterations,
                                    int(action_cooldowns.get(action_key, 0)),
                                )
                                throttled_actions.add(action_key)
                            else:
                                auto_disabled_actions.add(action_key)
                else:
                    action_negative_share_worsening_streaks[action_key] = 0

                if _should_demote_action_for_effectiveness(
                    metrics,
                    thresholds=adaptive_effectiveness_thresholds,
                ):
                    if reserve_protected:
                        if action_key in protected_tertiary_reserve_actions:
                            _increment_counter(
                                tertiary_reserve_protection_reason_counters,
                                "suppressed_effectiveness_demotion",
                            )
                            _increment_counter(
                                tertiary_reserve_suppressed_demotion_counts,
                                action_key,
                            )
                        if action_key in protected_final_fallback_reserve_actions:
                            _increment_counter(
                                final_fallback_reserve_protection_reason_counters,
                                "suppressed_effectiveness_demotion",
                            )
                            _increment_counter(
                                final_fallback_reserve_suppressed_demotion_counts,
                                action_key,
                            )
                    else:
                        demoted_actions.add(action_key)
                        auto_disabled_actions.add(action_key)
                        action_cooldowns.pop(action_key, None)

            latest_tertiary_reserve_protected_actions = sorted(protected_tertiary_reserve_actions)
            latest_tertiary_reserve_protection_state = dict(
                sorted(tertiary_reserve_protection_state.items(), key=lambda item: item[0])
            )
            latest_final_fallback_reserve_protected_actions = sorted(protected_final_fallback_reserve_actions)
            latest_final_fallback_reserve_protection_state = dict(
                sorted(final_fallback_reserve_protection_state.items(), key=lambda item: item[0])
            )

            if improvement < safe_min_gap_improvement:
                stall_counter += 1
            else:
                stall_counter = 0

            iteration_logs.append(
                {
                    "iteration": iteration,
                    "advisor_status_before": advisor_status_before,
                    "advisor_status_after": advisor_status_after,
                    "reserve_release_override": dict(reserve_release_override),
                    "gap_score_before": gap_score_before,
                    "gap_score_after": gap_score_after,
                    "improvement": improvement,
                    "negative_expectancy_attempt_share_before": negative_share_before,
                    "negative_expectancy_attempt_share_after": negative_share_after,
                    "negative_expectancy_attempt_share_delta": negative_share_delta,
                    "negative_expectancy_attempt_share_worsened": negative_share_worsened,
                    "executed_actions": executed_actions,
                    "negative_share_worsening_streaks": dict(
                        sorted(action_negative_share_worsening_streaks.items(), key=lambda item: item[0])
                    ),
                    "adaptive_effectiveness_thresholds": dict(adaptive_effectiveness_thresholds),
                    "adaptive_replacement_profile": dict(adaptive_replacement_profile),
                    "action_effectiveness": _action_effectiveness_snapshot(action_effectiveness),
                    "auto_disabled_actions": sorted(auto_disabled_actions),
                    "demoted_actions": sorted(demoted_actions),
                    "throttled_actions": sorted(throttled_actions),
                    "action_cooldowns": dict(sorted(action_cooldowns.items(), key=lambda item: item[0])),
                    "replacement_actions": replacement_rows,
                    "replacement_usage_by_source": dict(
                        sorted(replacement_usage_by_source.items(), key=lambda item: item[0])
                    ),
                    "replacement_usage_by_action": dict(
                        sorted(replacement_usage_by_action.items(), key=lambda item: item[0])
                    ),
                    "replacement_route_arbitration_counts": dict(
                        sorted(iteration_replacement_route_arbitration_counts.items(), key=lambda item: item[0])
                    ),
                    "replacement_route_selection_counts": dict(
                        sorted(iteration_replacement_route_selection_counts.items(), key=lambda item: item[0])
                    ),
                    "tertiary_replacement_usage_by_source": dict(
                        sorted(tertiary_replacement_usage_by_source.items(), key=lambda item: item[0])
                    ),
                    "tertiary_replacement_usage_by_action": dict(
                        sorted(tertiary_replacement_usage_by_action.items(), key=lambda item: item[0])
                    ),
                    "final_fallback_replacement_usage_by_source": dict(
                        sorted(final_fallback_replacement_usage_by_source.items(), key=lambda item: item[0])
                    ),
                    "final_fallback_replacement_usage_by_action": dict(
                        sorted(final_fallback_replacement_usage_by_action.items(), key=lambda item: item[0])
                    ),
                    "final_fallback_decision_reason_counts": dict(
                        sorted(iteration_final_fallback_decision_reason_counts.items(), key=lambda item: item[0])
                    ),
                    "final_fallback_unavailable_reason_counts": dict(
                        sorted(
                            iteration_final_fallback_unavailable_reason_counts.items(),
                            key=lambda item: item[0],
                        )
                    ),
                    "tertiary_reserve_protected_actions": list(latest_tertiary_reserve_protected_actions),
                    "tertiary_reserve_protection_state": dict(latest_tertiary_reserve_protection_state),
                    "tertiary_reserve_protection_progress": dict(
                        sorted(tertiary_reserve_protection_progress.items(), key=lambda item: item[0])
                    ),
                    "tertiary_reserve_protection_reason_counters": dict(
                        sorted(tertiary_reserve_protection_reason_counters.items(), key=lambda item: item[0])
                    ),
                    "tertiary_reserve_suppressed_demotion_counts": dict(
                        sorted(tertiary_reserve_suppressed_demotion_counts.items(), key=lambda item: item[0])
                    ),
                    "tertiary_reserve_suppressed_auto_disable_counts": dict(
                        sorted(tertiary_reserve_suppressed_auto_disable_counts.items(), key=lambda item: item[0])
                    ),
                    "final_fallback_reserve_protected_actions": list(latest_final_fallback_reserve_protected_actions),
                    "final_fallback_reserve_protection_state": dict(latest_final_fallback_reserve_protection_state),
                    "final_fallback_reserve_protection_progress": dict(
                        sorted(final_fallback_reserve_protection_progress.items(), key=lambda item: item[0])
                    ),
                    "final_fallback_reserve_protection_reason_counters": dict(
                        sorted(final_fallback_reserve_protection_reason_counters.items(), key=lambda item: item[0])
                    ),
                    "final_fallback_reserve_suppressed_demotion_counts": dict(
                        sorted(final_fallback_reserve_suppressed_demotion_counts.items(), key=lambda item: item[0])
                    ),
                    "final_fallback_reserve_suppressed_auto_disable_counts": dict(
                        sorted(final_fallback_reserve_suppressed_auto_disable_counts.items(), key=lambda item: item[0])
                    ),
                    "stall_counter": stall_counter,
                }
            )

            current_payload = next_payload
            current_status = advisor_status_after
            current_gap_score = gap_score_after
            current_negative_share = negative_share_after
            last_iteration_gap_improvement = improvement
            last_iteration_negative_share_delta = (
                float(negative_share_delta) if isinstance(negative_share_delta, float) else None
            )

            if advisor_status_after == "risk_off_cleared":
                termination_reason = "cleared"
                break
            if stall_counter >= safe_stall_iterations:
                termination_reason = "stalled"
                break
        else:
            termination_reason = "max_iterations"

    completed_at = datetime.now(timezone.utc)
    payload: dict[str, Any] = {
        "status": "ready",
        "started_at": started_at.isoformat(),
        "captured_at": completed_at.isoformat(),
        "output_dir": str(out_dir),
        "health_dir": str(health_dir),
        "inputs": {
            "trader_env_file": safe_trader_env_file,
            "max_iterations": safe_max_iterations,
            "stall_iterations": safe_stall_iterations,
            "min_gap_improvement": round(float(safe_min_gap_improvement), 6),
            "weather_window_hours": round(float(safe_weather_window_hours), 6),
            "weather_min_bucket_samples": safe_weather_min_bucket_samples,
            "weather_max_profile_age_hours": round(float(safe_weather_max_profile_age_hours), 6),
            "weather_negative_expectancy_attempt_share_target": round(float(safe_negative_target), 6),
            "weather_stale_metar_negative_attempt_share_target": round(float(safe_stale_negative_target), 6),
            "weather_stale_metar_attempt_share_target": round(float(safe_stale_target), 6),
            "weather_min_attempts_target": safe_weather_min_attempts_target,
            "optimizer_top_n": safe_optimizer_top_n,
            "plateau_negative_regime_suppression_enabled": safe_plateau_negative_regime_suppression_enabled,
            "plateau_negative_regime_suppression_min_bucket_samples": (
                safe_plateau_negative_regime_suppression_min_bucket_samples
            ),
            "plateau_negative_regime_suppression_expectancy_threshold": round(
                float(safe_plateau_negative_regime_suppression_expectancy_threshold),
                6,
            ),
            "plateau_negative_regime_suppression_top_n": safe_plateau_negative_regime_suppression_top_n,
            "retune_weather_window_hours_cap": round(float(safe_retune_weather_window_hours_cap), 6),
            "retune_overblocking_blocked_share_threshold": round(
                float(safe_retune_overblocking_blocked_share_threshold),
                6,
            ),
            "retune_underblocking_min_top_n": safe_retune_underblocking_min_top_n,
            "retune_overblocking_max_top_n": safe_retune_overblocking_max_top_n,
            "retune_min_bucket_samples_target": safe_retune_min_bucket_samples_target,
            "retune_expectancy_threshold_target": round(float(safe_retune_expectancy_threshold_target), 6),
            "execute_actions": bool(execute_actions),
        },
        "termination_reason": termination_reason,
        "iterations_executed": len(iteration_logs),
        "final_stall_counter": stall_counter,
        "initial_advisor_status": _extract_advisor_status(initial_advisor_payload),
        "final_advisor_status": current_status,
        "initial_gap_score": _compute_gap_score(initial_advisor_payload),
        "final_gap_score": current_gap_score,
        "initial_negative_expectancy_attempt_share": _extract_negative_expectancy_attempt_share(initial_advisor_payload),
        "final_negative_expectancy_attempt_share": current_negative_share,
        "negative_share_worsening_epsilon": NEGATIVE_SHARE_WORSENING_EPSILON,
        "effectiveness_demotion_min_executions": EFFECTIVENESS_DEMOTION_MIN_EXECUTIONS,
        "effectiveness_demotion_min_worsening_ratio": EFFECTIVENESS_DEMOTION_MIN_WORSENING_RATIO,
        "effectiveness_demotion_min_average_delta": EFFECTIVENESS_DEMOTION_MIN_AVERAGE_DELTA,
        "adaptive_effectiveness_thresholds": dict(adaptive_effectiveness_thresholds),
        "adaptive_replacement_profile": dict(adaptive_replacement_profile),
        "tertiary_reserve_protection_max_iterations": int(TERTIARY_RESERVE_PROTECTION_MAX_ITERATIONS),
        "final_fallback_reserve_protection_max_iterations": int(FINAL_FALLBACK_RESERVE_PROTECTION_MAX_ITERATIONS),
        "cooldown_iterations_by_policy_class": {
            "core_coverage": CORE_COVERAGE_ACTION_COOLDOWN_ITERATIONS,
            "alpha_lever": ALPHA_LEVER_ACTION_COOLDOWN_ITERATIONS,
        },
        "replacement_action_map": dict(sorted(REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.items(), key=lambda item: item[0])),
        "tertiary_replacement_action_map": dict(
            sorted(TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.items(), key=lambda item: item[0])
        ),
        "final_fallback_replacement_action_map": dict(
            sorted(FINAL_FALLBACK_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY.items(), key=lambda item: item[0])
        ),
        "final_fallback_replacement_action_map_by_policy_class": dict(
            sorted(FINAL_FALLBACK_REPLACEMENT_ACTION_BY_POLICY_CLASS.items(), key=lambda item: item[0])
        ),
        "final_fallback_default_action_key": DEFAULT_FINAL_FALLBACK_REPLACEMENT_ACTION_KEY,
        "replacement_route_arbitration_config": {
            "policy": "minimize_harm_score",
            "route_order": ["strict", "tertiary", "final_fallback", "unavailable"],
            "score_epsilon": round(float(REPLACEMENT_ROUTE_ARBITRATION_SCORE_EPSILON), 6),
            "unknown_worsening_ratio": round(float(REPLACEMENT_ROUTE_UNKNOWN_WORSENING_RATIO), 6),
            "average_delta_scale": round(float(REPLACEMENT_ROUTE_AVERAGE_DELTA_SCALE), 6),
            "weights": {
                "worsening_ratio": round(float(REPLACEMENT_ROUTE_WEIGHT_WORSENING_RATIO), 6),
                "average_delta_harm": round(float(REPLACEMENT_ROUTE_WEIGHT_AVERAGE_DELTA), 6),
                "execution_uncertainty": round(float(REPLACEMENT_ROUTE_WEIGHT_EXECUTION_UNCERTAINTY), 6),
            },
        },
        "negative_share_worsening_streaks": dict(
            sorted(action_negative_share_worsening_streaks.items(), key=lambda item: item[0])
        ),
        "action_effectiveness": _action_effectiveness_snapshot(action_effectiveness),
        "auto_disabled_actions": sorted(auto_disabled_actions),
        "demoted_actions": sorted(demoted_actions),
        "throttled_actions": sorted(throttled_actions),
        "action_cooldowns": dict(sorted(action_cooldowns.items(), key=lambda item: item[0])),
        "replacement_usage_by_source": dict(sorted(replacement_usage_by_source.items(), key=lambda item: item[0])),
        "replacement_usage_by_action": dict(sorted(replacement_usage_by_action.items(), key=lambda item: item[0])),
        "replacement_route_arbitration_counts": dict(
            sorted(replacement_route_arbitration_counts.items(), key=lambda item: item[0])
        ),
        "replacement_route_selection_counts": dict(
            sorted(replacement_route_selection_counts.items(), key=lambda item: item[0])
        ),
        "tertiary_replacement_usage_by_source": dict(
            sorted(tertiary_replacement_usage_by_source.items(), key=lambda item: item[0])
        ),
        "tertiary_replacement_usage_by_action": dict(
            sorted(tertiary_replacement_usage_by_action.items(), key=lambda item: item[0])
        ),
        "final_fallback_replacement_usage_by_source": dict(
            sorted(final_fallback_replacement_usage_by_source.items(), key=lambda item: item[0])
        ),
        "final_fallback_replacement_usage_by_action": dict(
            sorted(final_fallback_replacement_usage_by_action.items(), key=lambda item: item[0])
        ),
        "final_fallback_decision_reason_counts": dict(
            sorted(final_fallback_decision_reason_counts.items(), key=lambda item: item[0])
        ),
        "final_fallback_unavailable_reason_counts": dict(
            sorted(final_fallback_unavailable_reason_counts.items(), key=lambda item: item[0])
        ),
        "tertiary_reserve_protected_actions": list(latest_tertiary_reserve_protected_actions),
        "tertiary_reserve_protection_state": dict(latest_tertiary_reserve_protection_state),
        "tertiary_reserve_protection_progress": dict(
            sorted(tertiary_reserve_protection_progress.items(), key=lambda item: item[0])
        ),
        "tertiary_reserve_protection_reason_counters": dict(
            sorted(tertiary_reserve_protection_reason_counters.items(), key=lambda item: item[0])
        ),
        "tertiary_reserve_suppressed_demotion_counts": dict(
            sorted(tertiary_reserve_suppressed_demotion_counts.items(), key=lambda item: item[0])
        ),
        "tertiary_reserve_suppressed_auto_disable_counts": dict(
            sorted(tertiary_reserve_suppressed_auto_disable_counts.items(), key=lambda item: item[0])
        ),
        "final_fallback_reserve_protected_actions": list(latest_final_fallback_reserve_protected_actions),
        "final_fallback_reserve_protection_state": dict(latest_final_fallback_reserve_protection_state),
        "final_fallback_reserve_protection_progress": dict(
            sorted(final_fallback_reserve_protection_progress.items(), key=lambda item: item[0])
        ),
        "final_fallback_reserve_protection_reason_counters": dict(
            sorted(final_fallback_reserve_protection_reason_counters.items(), key=lambda item: item[0])
        ),
        "final_fallback_reserve_suppressed_demotion_counts": dict(
            sorted(final_fallback_reserve_suppressed_demotion_counts.items(), key=lambda item: item[0])
        ),
        "final_fallback_reserve_suppressed_auto_disable_counts": dict(
            sorted(final_fallback_reserve_suppressed_auto_disable_counts.items(), key=lambda item: item[0])
        ),
        "iteration_logs": iteration_logs,
        "initial_advisor": initial_advisor_payload,
        "final_advisor": current_payload,
    }

    stamp = completed_at.strftime("%Y%m%d_%H%M%S")
    output_path = health_dir / f"kalshi_temperature_recovery_loop_{stamp}.json"
    latest_path = health_dir / "kalshi_temperature_recovery_loop_latest.json"
    payload["output_file"] = str(output_path)
    payload["latest_file"] = str(latest_path)

    encoded = json.dumps(payload, indent=2, sort_keys=True)
    _write_text_atomic(output_path, encoded)
    _write_text_atomic(latest_path, encoded)
    return payload


def summarize_kalshi_temperature_recovery_loop(
    *,
    output_dir: str,
    trader_env_file: str = "data/research/account_onboarding.env.template",
    max_iterations: int = 4,
    stall_iterations: int = 2,
    min_gap_improvement: float = 0.01,
    weather_window_hours: float = 720.0,
    weather_min_bucket_samples: int = 10,
    weather_max_profile_age_hours: float = 336.0,
    weather_negative_expectancy_attempt_share_target: float = 0.50,
    weather_stale_metar_negative_attempt_share_target: float = 0.60,
    weather_stale_metar_attempt_share_target: float = 0.65,
    weather_min_attempts_target: int = 200,
    optimizer_top_n: int = 5,
    plateau_negative_regime_suppression_enabled: bool = True,
    plateau_negative_regime_suppression_min_bucket_samples: int = 18,
    plateau_negative_regime_suppression_expectancy_threshold: float = -0.06,
    plateau_negative_regime_suppression_top_n: int = 10,
    retune_weather_window_hours_cap: float = 336.0,
    retune_overblocking_blocked_share_threshold: float = 0.25,
    retune_underblocking_min_top_n: int = 16,
    retune_overblocking_max_top_n: int = 4,
    retune_min_bucket_samples_target: int = 14,
    retune_expectancy_threshold_target: float = -0.045,
    execute_actions: bool = True,
) -> str:
    payload = run_kalshi_temperature_recovery_loop(
        output_dir=output_dir,
        trader_env_file=trader_env_file,
        max_iterations=max_iterations,
        stall_iterations=stall_iterations,
        min_gap_improvement=min_gap_improvement,
        weather_window_hours=weather_window_hours,
        weather_min_bucket_samples=weather_min_bucket_samples,
        weather_max_profile_age_hours=weather_max_profile_age_hours,
        weather_negative_expectancy_attempt_share_target=weather_negative_expectancy_attempt_share_target,
        weather_stale_metar_negative_attempt_share_target=weather_stale_metar_negative_attempt_share_target,
        weather_stale_metar_attempt_share_target=weather_stale_metar_attempt_share_target,
        weather_min_attempts_target=weather_min_attempts_target,
        optimizer_top_n=optimizer_top_n,
        plateau_negative_regime_suppression_enabled=plateau_negative_regime_suppression_enabled,
        plateau_negative_regime_suppression_min_bucket_samples=plateau_negative_regime_suppression_min_bucket_samples,
        plateau_negative_regime_suppression_expectancy_threshold=plateau_negative_regime_suppression_expectancy_threshold,
        plateau_negative_regime_suppression_top_n=plateau_negative_regime_suppression_top_n,
        retune_weather_window_hours_cap=retune_weather_window_hours_cap,
        retune_overblocking_blocked_share_threshold=retune_overblocking_blocked_share_threshold,
        retune_underblocking_min_top_n=retune_underblocking_min_top_n,
        retune_overblocking_max_top_n=retune_overblocking_max_top_n,
        retune_min_bucket_samples_target=retune_min_bucket_samples_target,
        retune_expectancy_threshold_target=retune_expectancy_threshold_target,
        execute_actions=execute_actions,
    )
    return json.dumps(payload, indent=2, sort_keys=True)
