from __future__ import annotations

from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any

from betbot.kalshi_temperature_recovery_loop import run_kalshi_temperature_recovery_loop

_DEFAULT_PROFILE_ROWS: list[dict[str, Any]] = [
    {
        "name": "steady_4x2",
        "max_iterations": 4,
        "stall_iterations": 2,
        "min_gap_improvement": 0.005,
    },
    {
        "name": "extended_6x3",
        "max_iterations": 6,
        "stall_iterations": 3,
        "min_gap_improvement": 0.0025,
    },
    {
        "name": "focused_3x2",
        "max_iterations": 3,
        "stall_iterations": 2,
        "min_gap_improvement": 0.0075,
    },
]

_DEFAULT_ADVISOR_TARGETS: dict[str, Any] = {
    "weather_window_hours": 720.0,
    "weather_min_bucket_samples": 10,
    "weather_max_profile_age_hours": 336.0,
    "weather_negative_expectancy_attempt_share_target": 0.50,
    "weather_stale_metar_negative_attempt_share_target": 0.60,
    "weather_stale_metar_attempt_share_target": 0.65,
    "weather_min_attempts_target": 200,
    "optimizer_top_n": 5,
    "plateau_negative_regime_suppression_enabled": True,
    "plateau_negative_regime_suppression_min_bucket_samples": 18,
    "plateau_negative_regime_suppression_expectancy_threshold": -0.06,
    "plateau_negative_regime_suppression_top_n": 10,
    "retune_weather_window_hours_cap": 336.0,
    "retune_overblocking_blocked_share_threshold": 0.25,
    "retune_underblocking_min_top_n": 16,
    "retune_overblocking_max_top_n": 4,
    "retune_min_bucket_samples_target": 14,
    "retune_expectancy_threshold_target": -0.045,
}

_DEFAULT_TRADER_ENV_FILE = "data/research/account_onboarding.env.template"


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


def _safe_int(value: Any) -> int | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    return int(round(parsed))


def _safe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = _text(value).lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    return None


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(text, encoding="utf-8")
    temp_path.replace(path)


def _format_number(value: float | int) -> str:
    if isinstance(value, int):
        return str(value)
    rendered = f"{float(value):.6f}".rstrip("0").rstrip(".")
    return rendered or "0"


def _sorted_env_mapping(value: Any) -> dict[str, str]:
    source = _as_dict(value)
    normalized: dict[str, str] = {}
    for raw_key, raw_value in source.items():
        key = _text(raw_key)
        if not key:
            continue
        normalized[key] = _text(raw_value)
    return {key: normalized[key] for key in sorted(normalized)}


def _shell_double_quote(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("$", "\\$")
        .replace("`", "\\`")
    )


def _render_env_export_text(env_mapping: dict[str, str]) -> str:
    if not env_mapping:
        return ""
    lines = [f'export {key}="{_shell_double_quote(value)}"' for key, value in env_mapping.items()]
    return "\n".join(lines) + "\n"


def _render_env_patch_text(env_mapping: dict[str, str]) -> str:
    if not env_mapping:
        return ""
    lines = [f"{key}={value}" for key, value in env_mapping.items()]
    return "\n".join(lines) + "\n"


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    return dict(payload) if isinstance(payload, dict) else None


def _find_latest_decision_matrix_hardening_artifact(health_dir: Path) -> tuple[Path | None, dict[str, Any] | None]:
    latest_path = health_dir / "decision_matrix_hardening_latest.json"
    if latest_path.exists():
        payload = _read_json_object(latest_path)
        if payload is not None:
            return latest_path, payload

    timestamped_paths = sorted(
        (
            path
            for path in health_dir.glob("decision_matrix_hardening_*.json")
            if path.name != "decision_matrix_hardening_latest.json"
        ),
        reverse=True,
    )
    for candidate_path in timestamped_paths:
        payload = _read_json_object(candidate_path)
        if payload is not None:
            return candidate_path, payload
    return None, None


def _safe_profile_float(value: Any, default: float = 0.0) -> float:
    parsed = _safe_float(value)
    return float(parsed if parsed is not None else default)


def _safe_profile_int(value: Any, default: int = 0) -> int:
    parsed = _safe_int(value)
    return int(parsed if parsed is not None else default)


def _derive_profile_adaptation_signals(
    decision_matrix_artifact: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(decision_matrix_artifact, dict):
        return {
            "available": False,
            "status": "",
            "source": "",
            "matrix_health_status": "",
            "critical_blockers_count": None,
            "settled_outcomes_delta_24h": None,
            "settled_outcomes_delta_7d": None,
            "combined_bucket_count_delta_24h": None,
            "combined_bucket_count_delta_7d": None,
            "targeted_constraint_rows": None,
            "top_bottlenecks_count": None,
            "bottleneck_source": "",
            "settled_outcome_growth_stalled": False,
            "guardrail_active": False,
            "guardrail_cleared": False,
            "positive_streak": False,
            "momentum_score": 0,
        }

    status = _text(decision_matrix_artifact.get("status")).lower()
    matrix_health_status = _text(decision_matrix_artifact.get("matrix_health_status")).lower()
    source = _text(decision_matrix_artifact.get("source"))
    critical_blockers_count = _safe_int(decision_matrix_artifact.get("critical_blockers_count"))
    observed_metrics = _as_dict(decision_matrix_artifact.get("observed_metrics"))
    data_sources = _as_dict(decision_matrix_artifact.get("data_sources"))
    blocking_factors = decision_matrix_artifact.get("blocking_factors")
    blocker_rows = blocking_factors if isinstance(blocking_factors, list) else []
    blocker_keys = {
        _text(row.get("key")).lower()
        for row in blocker_rows
        if isinstance(row, dict) and _text(row.get("key"))
    }
    settled_outcome_growth_stalled = bool(
        _safe_bool(observed_metrics.get("settled_outcome_growth_stalled"))
        or "settled_outcome_growth_stalled" in blocker_keys
    )
    settled_outcomes_delta_24h = _safe_float(
        observed_metrics.get("settled_outcome_throughput_growth_delta_24h")
    )
    if settled_outcomes_delta_24h is None:
        settled_outcomes_delta_24h = _safe_float(observed_metrics.get("settled_outcome_growth_delta_24h"))
    if settled_outcomes_delta_24h is None:
        settled_outcomes_delta_24h = _safe_float(data_sources.get("settled_outcome_throughput", {}).get("growth_deltas_settled_outcomes_delta_24h"))

    settled_outcomes_delta_7d = _safe_float(
        observed_metrics.get("settled_outcome_throughput_growth_delta_7d")
    )
    if settled_outcomes_delta_7d is None:
        settled_outcomes_delta_7d = _safe_float(observed_metrics.get("settled_outcome_growth_delta_7d"))
    if settled_outcomes_delta_7d is None:
        settled_outcomes_delta_7d = _safe_float(data_sources.get("settled_outcome_throughput", {}).get("growth_deltas_settled_outcomes_delta_7d"))

    combined_bucket_count_delta_24h = _safe_int(
        observed_metrics.get("settled_outcome_throughput_combined_bucket_count_delta_24h")
    )
    if combined_bucket_count_delta_24h is None:
        combined_bucket_count_delta_24h = _safe_int(
            observed_metrics.get("settled_outcome_growth_combined_bucket_count_delta_24h")
        )
    if combined_bucket_count_delta_24h is None:
        combined_bucket_count_delta_24h = _safe_int(
            data_sources.get("settled_outcome_throughput", {}).get("growth_deltas_combined_bucket_count_delta_24h")
        )

    combined_bucket_count_delta_7d = _safe_int(
        observed_metrics.get("settled_outcome_throughput_combined_bucket_count_delta_7d")
    )
    if combined_bucket_count_delta_7d is None:
        combined_bucket_count_delta_7d = _safe_int(
            observed_metrics.get("settled_outcome_growth_combined_bucket_count_delta_7d")
        )
    if combined_bucket_count_delta_7d is None:
        combined_bucket_count_delta_7d = _safe_int(
            data_sources.get("settled_outcome_throughput", {}).get("growth_deltas_combined_bucket_count_delta_7d")
        )

    targeted_constraint_rows = _safe_int(
        observed_metrics.get("settled_outcome_throughput_targeted_constraint_rows")
    )
    if targeted_constraint_rows is None:
        targeted_constraint_rows = _safe_int(data_sources.get("settled_outcome_throughput", {}).get("targeting_targeted_constraint_rows"))

    top_bottlenecks_count = _safe_int(observed_metrics.get("settled_outcome_throughput_top_bottlenecks_count"))
    if top_bottlenecks_count is None:
        top_bottlenecks_count = _safe_int(data_sources.get("settled_outcome_throughput", {}).get("top_bottlenecks_count"))

    bottleneck_source = _text(observed_metrics.get("settled_outcome_throughput_bottleneck_source"))
    if not bottleneck_source:
        bottleneck_source = _text(data_sources.get("settled_outcome_throughput", {}).get("bottleneck_source"))

    guardrail_active = bool(
        matrix_health_status in {"red", "blocked"}
        or _safe_bool(decision_matrix_artifact.get("supports_consistency_and_profitability")) is False
        or (critical_blockers_count is not None and critical_blockers_count > 0)
        or settled_outcome_growth_stalled
    )
    guardrail_cleared = bool(
        matrix_health_status == "green"
        or (
            _safe_bool(decision_matrix_artifact.get("supports_consistency_and_profitability")) is True
            and (critical_blockers_count or 0) == 0
            and not settled_outcome_growth_stalled
        )
    )

    momentum_score = 0
    for delta, positive_weight, negative_weight in (
        (settled_outcomes_delta_24h, 4, -4),
        (settled_outcomes_delta_7d, 2, -2),
    ):
        if isinstance(delta, float):
            if delta > 0:
                momentum_score += positive_weight
            elif delta < 0:
                momentum_score += negative_weight
    for delta, positive_weight, negative_weight in (
        (combined_bucket_count_delta_24h, 3, -3),
        (combined_bucket_count_delta_7d, 1, -1),
    ):
        if isinstance(delta, int):
            if delta > 0:
                momentum_score += positive_weight
            elif delta < 0:
                momentum_score += negative_weight
    if isinstance(targeted_constraint_rows, int):
        momentum_score += 2 if targeted_constraint_rows > 0 else -1
    if isinstance(top_bottlenecks_count, int):
        momentum_score += 1 if top_bottlenecks_count > 0 else 0
    if bottleneck_source:
        momentum_score += 1 if "bootstrap" in bottleneck_source else 0
    if settled_outcome_growth_stalled:
        momentum_score -= 5
    if guardrail_active:
        momentum_score -= 2
    if guardrail_cleared:
        momentum_score += 2

    positive_streak = bool(
        isinstance(settled_outcomes_delta_24h, float)
        and settled_outcomes_delta_24h > 0
        and isinstance(settled_outcomes_delta_7d, float)
        and settled_outcomes_delta_7d > 0
        and isinstance(combined_bucket_count_delta_24h, int)
        and combined_bucket_count_delta_24h > 0
        and isinstance(targeted_constraint_rows, int)
        and targeted_constraint_rows > 0
        and not settled_outcome_growth_stalled
    )

    return {
        "available": True,
        "status": status,
        "source": source,
        "matrix_health_status": matrix_health_status,
        "critical_blockers_count": critical_blockers_count,
        "settled_outcomes_delta_24h": settled_outcomes_delta_24h,
        "settled_outcomes_delta_7d": settled_outcomes_delta_7d,
        "combined_bucket_count_delta_24h": combined_bucket_count_delta_24h,
        "combined_bucket_count_delta_7d": combined_bucket_count_delta_7d,
        "targeted_constraint_rows": targeted_constraint_rows,
        "top_bottlenecks_count": top_bottlenecks_count,
        "bottleneck_source": bottleneck_source,
        "settled_outcome_growth_stalled": settled_outcome_growth_stalled,
        "guardrail_active": guardrail_active,
        "guardrail_cleared": guardrail_cleared,
        "positive_streak": positive_streak,
        "momentum_score": momentum_score,
    }


def _adapt_default_profile_rows(
    profiles: list[dict[str, Any]],
    signals: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    if not signals.get("available"):
        return [dict(row) for row in profiles], "baseline"

    guardrail_active = bool(signals.get("guardrail_active"))
    guardrail_cleared = bool(signals.get("guardrail_cleared"))
    positive_streak = bool(signals.get("positive_streak"))
    momentum_score = _safe_int(signals.get("momentum_score")) or 0

    if guardrail_active or momentum_score <= 0 or not positive_streak:
        mode = "acceleration"
        max_iterations_delta = 2
        stall_iterations_delta = 1
        min_gap_multiplier = 0.70
    elif guardrail_cleared or momentum_score >= 6 or positive_streak:
        mode = "confirmation"
        max_iterations_delta = -1
        stall_iterations_delta = -1
        min_gap_multiplier = 1.25
    else:
        return [dict(row) for row in profiles], "baseline"

    adapted_profiles: list[dict[str, Any]] = []
    for row in profiles:
        max_iterations = max(1, _safe_profile_int(row.get("max_iterations"), 1) + max_iterations_delta)
        stall_iterations = max(1, _safe_profile_int(row.get("stall_iterations"), 1) + stall_iterations_delta)
        min_gap_improvement = max(0.0, round(_safe_profile_float(row.get("min_gap_improvement"), 0.0) * min_gap_multiplier, 6))
        adapted_profiles.append(
            {
                "name": _text(row.get("name")),
                "max_iterations": max_iterations,
                "stall_iterations": stall_iterations,
                "min_gap_improvement": min_gap_improvement,
            }
        )
    return adapted_profiles, mode


def _sanitize_profiles(profiles: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    source: list[dict[str, Any]]
    if isinstance(profiles, list) and profiles:
        source = [dict(row) if isinstance(row, dict) else {} for row in profiles]
    else:
        source = [dict(row) for row in _DEFAULT_PROFILE_ROWS]

    normalized: list[dict[str, Any]] = []
    seen_names: dict[str, int] = {}
    fallback = _DEFAULT_PROFILE_ROWS[0]
    for index, raw_row in enumerate(source, start=1):
        base_name = _text(raw_row.get("name")) or f"profile_{index}"
        seen_count = seen_names.get(base_name, 0)
        seen_names[base_name] = seen_count + 1
        name = base_name if seen_count == 0 else f"{base_name}_{seen_count + 1}"

        max_iterations = _safe_int(raw_row.get("max_iterations"))
        stall_iterations = _safe_int(raw_row.get("stall_iterations"))
        min_gap_improvement = _safe_float(raw_row.get("min_gap_improvement"))

        normalized.append(
            {
                "name": name,
                "max_iterations": max(0, max_iterations if max_iterations is not None else int(fallback["max_iterations"])),
                "stall_iterations": max(
                    1,
                    stall_iterations if stall_iterations is not None else int(fallback["stall_iterations"]),
                ),
                "min_gap_improvement": max(
                    0.0,
                    float(min_gap_improvement if min_gap_improvement is not None else float(fallback["min_gap_improvement"])),
                ),
            }
        )
    return normalized


def _sanitize_advisor_targets(advisor_targets: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(_DEFAULT_ADVISOR_TARGETS)
    overrides = _as_dict(advisor_targets)
    if not overrides:
        return merged

    float_keys = {
        "weather_window_hours": 1.0,
        "weather_max_profile_age_hours": 0.0,
        "weather_negative_expectancy_attempt_share_target": 0.0,
        "weather_stale_metar_negative_attempt_share_target": 0.0,
        "weather_stale_metar_attempt_share_target": 0.0,
        "retune_weather_window_hours_cap": 1.0,
    }
    int_keys = {
        "weather_min_bucket_samples": 1,
        "weather_min_attempts_target": 1,
        "optimizer_top_n": 1,
        "plateau_negative_regime_suppression_min_bucket_samples": 1,
        "plateau_negative_regime_suppression_top_n": 1,
        "retune_underblocking_min_top_n": 1,
        "retune_overblocking_max_top_n": 1,
        "retune_min_bucket_samples_target": 1,
    }
    bool_keys = {
        "plateau_negative_regime_suppression_enabled",
    }
    bounded_float_keys: dict[str, tuple[float, float]] = {
        "plateau_negative_regime_suppression_expectancy_threshold": (-1.0, 0.0),
        "retune_overblocking_blocked_share_threshold": (0.0, 1.0),
        "retune_expectancy_threshold_target": (-1.0, 0.0),
    }

    for key, min_value in float_keys.items():
        parsed = _safe_float(overrides.get(key))
        if parsed is None:
            continue
        if key.endswith("_target"):
            merged[key] = max(0.0, min(1.0, float(parsed)))
        else:
            merged[key] = max(float(min_value), float(parsed))

    for key, min_value in int_keys.items():
        parsed = _safe_int(overrides.get(key))
        if parsed is None:
            continue
        merged[key] = max(int(min_value), int(parsed))

    for key in bool_keys:
        parsed = _safe_bool(overrides.get(key))
        if parsed is None:
            continue
        merged[key] = bool(parsed)

    for key, (lower, upper) in bounded_float_keys.items():
        parsed = _safe_float(overrides.get(key))
        if parsed is None:
            continue
        merged[key] = max(float(lower), min(float(upper), float(parsed)))

    return merged


def _extract_final_weather_metric(loop_payload: dict[str, Any], metric_key: str) -> float | None:
    final_advisor = _as_dict(loop_payload.get("final_advisor"))
    metrics = _as_dict(final_advisor.get("metrics"))
    weather_metrics = _as_dict(metrics.get("weather"))
    parsed = _safe_float(weather_metrics.get(metric_key))
    if parsed is not None:
        return round(float(parsed), 6)

    remediation = _as_dict(final_advisor.get("remediation_plan"))
    targets = _as_dict(remediation.get("targets"))
    target_row = _as_dict(targets.get(metric_key))
    fallback = _safe_float(target_row.get("current"))
    if fallback is not None:
        return round(float(fallback), 6)
    return None


def _extract_final_decision_matrix_context(loop_payload: dict[str, Any]) -> dict[str, Any]:
    final_advisor = _as_dict(loop_payload.get("final_advisor"))
    metrics = _as_dict(final_advisor.get("metrics"))
    source_candidates: list[tuple[str, dict[str, Any]]] = [
        ("final_advisor.metrics.decision_matrix", _as_dict(metrics.get("decision_matrix"))),
        ("final_advisor.decision_matrix", _as_dict(final_advisor.get("decision_matrix"))),
        ("loop_payload.final_decision_matrix", _as_dict(loop_payload.get("final_decision_matrix"))),
        ("loop_payload.decision_matrix", _as_dict(loop_payload.get("decision_matrix"))),
    ]
    source = ""
    decision_context: dict[str, Any] = {}
    for candidate_source, candidate_context in source_candidates:
        if candidate_context:
            source = candidate_source
            decision_context = candidate_context
            break

    blocker_rows: list[dict[str, Any]] = []
    for key in ("blockers", "blocking_factors"):
        rows = decision_context.get(key)
        if isinstance(rows, list):
            for row in rows:
                row_dict = _as_dict(row)
                if row_dict:
                    blocker_rows.append(row_dict)
    blocker_keys = {
        _text(row.get("key")).lower()
        for row in blocker_rows
        if _text(row.get("key"))
    }

    settled_insufficient_flag = _safe_bool(decision_context.get("settled_outcomes_insufficient"))
    settled_outcomes_insufficient = bool(
        settled_insufficient_flag
        or "insufficient_settled_outcomes" in blocker_keys
        or "settled_outcomes_insufficient" in blocker_keys
    )
    growth_stalled_flag = _safe_bool(decision_context.get("settled_outcome_growth_stalled"))
    settled_outcome_growth_stalled = bool(
        growth_stalled_flag
        or "settled_outcome_growth_stalled" in blocker_keys
    )

    critical_blockers_count = _safe_int(
        decision_context.get("critical_blockers_count")
        if decision_context.get("critical_blockers_count") is not None
        else decision_context.get("critical_blockers")
    )
    if critical_blockers_count is None and blocker_rows:
        critical_blockers_count = sum(
            1
            for row in blocker_rows
            if _text(row.get("severity")).lower() == "critical"
        )

    settled_outcomes_delta_24h = _safe_float(decision_context.get("settled_outcomes_delta_24h"))
    settled_outcomes_delta_7d = _safe_float(decision_context.get("settled_outcomes_delta_7d"))
    combined_bucket_count_delta_24h = _safe_int(decision_context.get("combined_bucket_count_delta_24h"))
    combined_bucket_count_delta_7d = _safe_int(decision_context.get("combined_bucket_count_delta_7d"))
    targeted_constraint_rows = _safe_int(decision_context.get("targeted_constraint_rows"))
    top_bottlenecks_count = _safe_int(decision_context.get("top_bottlenecks_count"))
    bottleneck_source = _text(decision_context.get("bottleneck_source"))

    return {
        "source": source,
        "settled_outcomes_insufficient": settled_outcomes_insufficient,
        "settled_outcome_growth_stalled": settled_outcome_growth_stalled,
        "critical_blockers_count": (
            max(0, int(critical_blockers_count)) if isinstance(critical_blockers_count, int) else None
        ),
        "settled_outcomes_delta_24h": settled_outcomes_delta_24h,
        "settled_outcomes_delta_7d": settled_outcomes_delta_7d,
        "combined_bucket_count_delta_24h": combined_bucket_count_delta_24h,
        "combined_bucket_count_delta_7d": combined_bucket_count_delta_7d,
        "targeted_constraint_rows": (
            max(0, int(targeted_constraint_rows)) if isinstance(targeted_constraint_rows, int) else None
        ),
        "top_bottlenecks_count": max(0, int(top_bottlenecks_count)) if isinstance(top_bottlenecks_count, int) else None,
        "bottleneck_source": bottleneck_source,
    }


def _extract_final_recovery_watchdog_context(loop_payload: dict[str, Any]) -> dict[str, Any]:
    final_advisor = _as_dict(loop_payload.get("final_advisor"))
    metrics = _as_dict(final_advisor.get("metrics"))
    source_candidates: list[tuple[str, dict[str, Any]]] = [
        ("final_advisor.metrics.recovery_watchdog", _as_dict(metrics.get("recovery_watchdog"))),
        ("final_advisor.recovery_watchdog", _as_dict(final_advisor.get("recovery_watchdog"))),
        ("loop_payload.recovery_watchdog", _as_dict(loop_payload.get("recovery_watchdog"))),
    ]
    source = ""
    recovery_watchdog_context: dict[str, Any] = {}
    for candidate_source, candidate_context in source_candidates:
        if candidate_context:
            source = candidate_source
            recovery_watchdog_context = candidate_context
            break

    stage_timeout_repair_action = _text(
        recovery_watchdog_context.get("stage_timeout_repair_action")
        or recovery_watchdog_context.get("latest_stage_timeout_repair_action")
    ).lower()
    stage_timeout_repair_status = _text(
        recovery_watchdog_context.get("stage_timeout_repair_status")
        or recovery_watchdog_context.get("latest_stage_timeout_repair_status")
    ).lower()

    actions_attempted = recovery_watchdog_context.get("actions_attempted")
    if isinstance(actions_attempted, list):
        for raw_action in reversed(actions_attempted):
            action_code = _text(raw_action).lower()
            if not action_code.startswith("repair_coldmath_stage_timeout_guardrails:"):
                continue
            stage_timeout_repair_action = action_code
            _, _, parsed_status = action_code.partition(":")
            if parsed_status:
                stage_timeout_repair_status = parsed_status
            break

    if not stage_timeout_repair_status and stage_timeout_repair_action:
        _, _, parsed_status = stage_timeout_repair_action.partition(":")
        if parsed_status:
            stage_timeout_repair_status = parsed_status

    severe_stage_timeout_repair = bool(
        stage_timeout_repair_status in {"missing_script", "failed"}
        or _safe_bool(recovery_watchdog_context.get("severe_stage_timeout_repair"))
        or _safe_bool(recovery_watchdog_context.get("severe_issue"))
    )
    return {
        "source": source,
        "stage_timeout_repair_action": stage_timeout_repair_action,
        "stage_timeout_repair_status": stage_timeout_repair_status,
        "severe_stage_timeout_repair": severe_stage_timeout_repair,
    }


def _normalized_action_code_list(value: Any) -> list[str]:
    rows: list[Any] = []
    if isinstance(value, list):
        rows = list(value)
    elif isinstance(value, dict):
        for raw_action, raw_count in value.items():
            action_code = _text(raw_action).lower()
            if not action_code:
                continue
            count = _safe_int(raw_count)
            if isinstance(count, int):
                if count <= 0:
                    continue
            else:
                enabled = _safe_bool(raw_count)
                if enabled is False:
                    continue
            rows.append(action_code)

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_action in rows:
        action_code = _text(raw_action).lower()
        if not action_code or action_code in seen:
            continue
        seen.add(action_code)
        normalized.append(action_code)
    return normalized


def _extract_final_recovery_effectiveness_context(loop_payload: dict[str, Any]) -> dict[str, Any]:
    final_advisor = _as_dict(loop_payload.get("final_advisor"))
    metrics = _as_dict(final_advisor.get("metrics"))
    source_candidates: list[tuple[str, dict[str, Any]]] = [
        ("final_advisor.metrics.recovery_effectiveness", _as_dict(metrics.get("recovery_effectiveness"))),
        ("final_advisor.recovery_effectiveness", _as_dict(final_advisor.get("recovery_effectiveness"))),
        ("loop_payload.recovery_effectiveness", _as_dict(loop_payload.get("recovery_effectiveness"))),
    ]
    source = ""
    recovery_effectiveness_context: dict[str, Any] = {}
    for candidate_source, candidate_context in source_candidates:
        if candidate_context:
            source = candidate_source
            recovery_effectiveness_context = candidate_context
            break

    demoted_actions: list[str] = []
    for key in (
        "persistently_harmful_actions",
        "persistently_harmful_action_keys",
        "demoted_actions",
        "demoted_action_keys",
        "harmful_actions",
    ):
        normalized = _normalized_action_code_list(recovery_effectiveness_context.get(key))
        if normalized:
            demoted_actions = normalized
            break

    harmful_action_count = _safe_int(recovery_effectiveness_context.get("harmful_action_count"))
    if harmful_action_count is None:
        for key in (
            "persistently_harmful_count",
            "persistently_harmful_action_count",
            "demoted_actions_count",
            "demoted_action_count",
            "harmful_count",
        ):
            harmful_action_count = _safe_int(recovery_effectiveness_context.get(key))
            if harmful_action_count is not None:
                break
    if harmful_action_count is None and demoted_actions:
        harmful_action_count = len(demoted_actions)

    return {
        "source": source,
        "demoted_actions": demoted_actions,
        "harmful_action_count": (
            max(0, int(harmful_action_count)) if isinstance(harmful_action_count, int) else None
        ),
    }


def _build_profile_result(profile: dict[str, Any], loop_payload: dict[str, Any]) -> dict[str, Any]:
    initial_gap = _safe_float(loop_payload.get("initial_gap_score"))
    final_gap = _safe_float(loop_payload.get("final_gap_score"))
    safe_initial_gap = round(float(initial_gap), 6) if isinstance(initial_gap, float) else 0.0
    safe_final_gap = round(float(final_gap), 6) if isinstance(final_gap, float) else safe_initial_gap
    gap_improvement_abs = round(float(safe_initial_gap - safe_final_gap), 6)
    if safe_initial_gap > 0.0:
        gap_improvement_pct = round((gap_improvement_abs / safe_initial_gap) * 100.0, 6)
    else:
        gap_improvement_pct = 0.0

    final_status = _text(loop_payload.get("final_advisor_status")).lower()
    if not final_status:
        final_advisor = _as_dict(loop_payload.get("final_advisor"))
        remediation = _as_dict(final_advisor.get("remediation_plan"))
        final_status = _text(remediation.get("status")).lower() or "unknown"
    final_advisor = _as_dict(loop_payload.get("final_advisor"))
    metrics = _as_dict(final_advisor.get("metrics"))
    trade_plan_metrics = _as_dict(metrics.get("trade_plan_blockers"))
    policy_reason_counts = (
        trade_plan_metrics.get("policy_reason_counts")
        if isinstance(trade_plan_metrics.get("policy_reason_counts"), dict)
        else {}
    )
    final_intents_total = max(0, int(_safe_int(trade_plan_metrics.get("intents_total")) or 0))
    final_intents_approved = max(0, int(_safe_int(trade_plan_metrics.get("intents_approved")) or 0))
    final_expected_edge_blocked_count = max(
        0,
        int(_safe_int(policy_reason_counts.get("expected_edge_below_min")) or 0),
    ) + max(
        0,
        int(_safe_int(policy_reason_counts.get("historical_profitability_expected_edge_below_min")) or 0),
    )
    decision_matrix_context = _extract_final_decision_matrix_context(loop_payload)
    recovery_watchdog_context = _extract_final_recovery_watchdog_context(loop_payload)
    recovery_effectiveness_context = _extract_final_recovery_effectiveness_context(loop_payload)
    effectiveness_demoted_actions = _normalized_action_code_list(
        recovery_effectiveness_context.get("demoted_actions")
    )
    effectiveness_harmful_count = _safe_int(recovery_effectiveness_context.get("harmful_action_count"))

    return {
        "name": profile["name"],
        "max_iterations": int(profile["max_iterations"]),
        "stall_iterations": int(profile["stall_iterations"]),
        "min_gap_improvement": round(float(profile["min_gap_improvement"]), 6),
        "termination_reason": _text(loop_payload.get("termination_reason")).lower() or "unknown",
        "iterations_executed": max(0, int(_safe_int(loop_payload.get("iterations_executed")) or 0)),
        "initial_gap_score": safe_initial_gap,
        "final_gap_score": safe_final_gap,
        "gap_improvement_abs": gap_improvement_abs,
        "gap_improvement_pct": gap_improvement_pct,
        "final_advisor_status": final_status,
        "final_weather_negative_expectancy_attempt_share": _extract_final_weather_metric(
            loop_payload,
            "negative_expectancy_attempt_share",
        ),
        "final_weather_stale_metar_negative_attempt_share": _extract_final_weather_metric(
            loop_payload,
            "stale_metar_negative_attempt_share",
        ),
        "final_weather_stale_metar_attempt_share": _extract_final_weather_metric(
            loop_payload,
            "stale_metar_attempt_share",
        ),
        "final_intents_total": final_intents_total,
        "final_intents_approved": final_intents_approved,
        "final_expected_edge_blocked_count": final_expected_edge_blocked_count,
        "final_decision_matrix_blocker_context": decision_matrix_context,
        "final_recovery_watchdog_context": recovery_watchdog_context,
        "final_recovery_effectiveness_context": recovery_effectiveness_context,
        "final_effectiveness_demoted_actions": effectiveness_demoted_actions,
        "final_effectiveness_harmful_count": (
            max(0, int(effectiveness_harmful_count)) if isinstance(effectiveness_harmful_count, int) else None
        ),
        "final_stage_timeout_repair_action": _text(
            recovery_watchdog_context.get("stage_timeout_repair_action")
        ).lower(),
        "final_stage_timeout_repair_status": _text(
            recovery_watchdog_context.get("stage_timeout_repair_status")
        ).lower(),
        "final_stage_timeout_repair_severe": bool(
            recovery_watchdog_context.get("severe_stage_timeout_repair")
        ),
        "final_settled_outcomes_insufficient": bool(decision_matrix_context.get("settled_outcomes_insufficient")),
        "final_settled_outcome_growth_stalled": bool(decision_matrix_context.get("settled_outcome_growth_stalled")),
        "final_settled_outcomes_delta_24h": _safe_float(decision_matrix_context.get("settled_outcomes_delta_24h")),
        "final_settled_outcomes_delta_7d": _safe_float(decision_matrix_context.get("settled_outcomes_delta_7d")),
        "final_combined_bucket_count_delta_24h": _safe_int(
            decision_matrix_context.get("combined_bucket_count_delta_24h")
        ),
        "final_combined_bucket_count_delta_7d": _safe_int(
            decision_matrix_context.get("combined_bucket_count_delta_7d")
        ),
        "final_targeted_constraint_rows": _safe_int(decision_matrix_context.get("targeted_constraint_rows")),
        "final_top_bottlenecks_count": _safe_int(decision_matrix_context.get("top_bottlenecks_count")),
        "final_bottleneck_source": _text(decision_matrix_context.get("bottleneck_source")),
        "final_critical_blockers_count": (
            int(decision_matrix_context["critical_blockers_count"])
            if isinstance(decision_matrix_context.get("critical_blockers_count"), int)
            else None
        ),
        "output_file": _text(loop_payload.get("output_file")),
    }


def _throughput_momentum_score(row: dict[str, Any]) -> int:
    if _text(row.get("final_advisor_status")).lower() != "insufficient_data":
        return 0

    decision_context = _as_dict(row.get("final_decision_matrix_blocker_context"))
    score = 0

    settled_outcomes_delta_24h = _safe_float(decision_context.get("settled_outcomes_delta_24h"))
    if isinstance(settled_outcomes_delta_24h, float):
        score += int(round(settled_outcomes_delta_24h * 1000.0))

    settled_outcomes_delta_7d = _safe_float(decision_context.get("settled_outcomes_delta_7d"))
    if isinstance(settled_outcomes_delta_7d, float):
        score += int(round(settled_outcomes_delta_7d * 100.0))

    combined_bucket_count_delta_24h = _safe_int(decision_context.get("combined_bucket_count_delta_24h"))
    if isinstance(combined_bucket_count_delta_24h, int):
        score += combined_bucket_count_delta_24h * 10

    combined_bucket_count_delta_7d = _safe_int(decision_context.get("combined_bucket_count_delta_7d"))
    if isinstance(combined_bucket_count_delta_7d, int):
        score += combined_bucket_count_delta_7d * 2

    targeted_constraint_rows = _safe_int(decision_context.get("targeted_constraint_rows"))
    if isinstance(targeted_constraint_rows, int):
        score += max(0, targeted_constraint_rows)

    top_bottlenecks_count = _safe_int(decision_context.get("top_bottlenecks_count"))
    if isinstance(top_bottlenecks_count, int):
        score += max(0, top_bottlenecks_count)

    bottleneck_source = _text(decision_context.get("bottleneck_source")).lower()
    if bottleneck_source:
        score += 1
        if "bootstrap" in bottleneck_source:
            score += 1

    if bool(
        _safe_bool(decision_context.get("settled_outcome_growth_stalled"))
        or _safe_bool(row.get("final_settled_outcome_growth_stalled"))
    ):
        score -= 50

    return score


def _best_profile_sort_key(
    row: dict[str, Any],
) -> tuple[int, int, int, int, int, int, int, int, int, float, float]:
    final_status = _text(row.get("final_advisor_status")).lower()
    cleared_rank = 1 if final_status == "risk_off_cleared" else 0
    decision_context = _as_dict(row.get("final_decision_matrix_blocker_context"))
    settled_outcomes_insufficient = bool(
        _safe_bool(decision_context.get("settled_outcomes_insufficient"))
        or _safe_bool(row.get("final_settled_outcomes_insufficient"))
    )
    settled_outcome_growth_stalled = bool(
        _safe_bool(decision_context.get("settled_outcome_growth_stalled"))
        or _safe_bool(row.get("final_settled_outcome_growth_stalled"))
    )
    critical_blockers_count = _safe_int(
        decision_context.get("critical_blockers_count")
        if decision_context.get("critical_blockers_count") is not None
        else row.get("final_critical_blockers_count")
    )
    throughput_momentum_rank = _throughput_momentum_score(row)
    if final_status == "insufficient_data":
        settled_outcomes_rank = 1 if not settled_outcomes_insufficient else 0
        growth_stalled_rank = 1 if not settled_outcome_growth_stalled else 0
        critical_blockers_rank = -max(0, int(critical_blockers_count or 0))
    else:
        settled_outcomes_rank = 0
        growth_stalled_rank = 0
        critical_blockers_rank = 0
    recovery_watchdog_context = _as_dict(row.get("final_recovery_watchdog_context"))
    stage_timeout_repair_status = _text(
        recovery_watchdog_context.get("stage_timeout_repair_status")
        if recovery_watchdog_context.get("stage_timeout_repair_status") is not None
        else row.get("final_stage_timeout_repair_status")
    ).lower()
    severe_stage_timeout_repair = stage_timeout_repair_status in {"missing_script", "failed"}
    stage_timeout_repair_rank = 0 if severe_stage_timeout_repair else 1
    recovery_effectiveness_context = _as_dict(row.get("final_recovery_effectiveness_context"))
    effectiveness_harmful_count = _safe_int(
        recovery_effectiveness_context.get("harmful_action_count")
        if recovery_effectiveness_context.get("harmful_action_count") is not None
        else row.get("final_effectiveness_harmful_count")
    )
    effectiveness_harmful_rank = (
        -max(0, int(effectiveness_harmful_count)) if isinstance(effectiveness_harmful_count, int) else 0
    )
    final_intents_approved = max(0, int(_safe_int(row.get("final_intents_approved")) or 0))
    final_expected_edge_blocked_count = max(0, int(_safe_int(row.get("final_expected_edge_blocked_count")) or 0))
    improvement = _safe_float(row.get("gap_improvement_abs")) or 0.0
    final_gap = _safe_float(row.get("final_gap_score"))
    safe_final_gap = float(final_gap) if isinstance(final_gap, float) else float("inf")
    return (
        cleared_rank,
        throughput_momentum_rank,
        settled_outcomes_rank,
        growth_stalled_rank,
        critical_blockers_rank,
        stage_timeout_repair_rank,
        effectiveness_harmful_rank,
        final_intents_approved,
        -final_expected_edge_blocked_count,
        float(improvement),
        -safe_final_gap,
    )


def _build_env_overrides(
    *,
    best_profile: dict[str, Any] | None,
    advisor_targets: dict[str, Any],
    execute_actions: bool,
    profile_adaptation_mode: str = "",
    adapted_profiles_used: bool = False,
    momentum_score: int = 0,
) -> dict[str, Any]:
    if not isinstance(best_profile, dict):
        return {
            "profile_name": "",
            "execute_actions": bool(execute_actions),
            "profile": {},
            "advisor_targets": dict(advisor_targets),
            "env": {
                "COLDMATH_RECOVERY_LOOP_EXECUTE_ACTIONS": "1" if execute_actions else "0",
                "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MODE": _text(profile_adaptation_mode),
                "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTED_PROFILES_USED": "1"
                if bool(adapted_profiles_used)
                else "0",
                "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MOMENTUM_SCORE": str(int(momentum_score)),
            },
        }

    profile_values = {
        "max_iterations": int(best_profile["max_iterations"]),
        "stall_iterations": int(best_profile["stall_iterations"]),
        "min_gap_improvement": round(float(best_profile["min_gap_improvement"]), 6),
    }
    env_overrides = {
        "COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS": str(profile_values["max_iterations"]),
        "COLDMATH_RECOVERY_LOOP_STALL_ITERATIONS": str(profile_values["stall_iterations"]),
        "COLDMATH_RECOVERY_LOOP_MIN_GAP_IMPROVEMENT": _format_number(profile_values["min_gap_improvement"]),
        "COLDMATH_RECOVERY_LOOP_EXECUTE_ACTIONS": "1" if execute_actions else "0",
        "COLDMATH_RECOVERY_ADVISOR_WEATHER_WINDOW_HOURS": _format_number(advisor_targets["weather_window_hours"]),
        "COLDMATH_RECOVERY_ADVISOR_WEATHER_MIN_BUCKET_SAMPLES": str(advisor_targets["weather_min_bucket_samples"]),
        "COLDMATH_RECOVERY_ADVISOR_WEATHER_MAX_PROFILE_AGE_HOURS": _format_number(
            advisor_targets["weather_max_profile_age_hours"]
        ),
        "COLDMATH_RECOVERY_ADVISOR_WEATHER_NEGATIVE_EXPECTANCY_ATTEMPT_SHARE_TARGET": _format_number(
            advisor_targets["weather_negative_expectancy_attempt_share_target"]
        ),
        "COLDMATH_RECOVERY_ADVISOR_WEATHER_STALE_METAR_NEGATIVE_ATTEMPT_SHARE_TARGET": _format_number(
            advisor_targets["weather_stale_metar_negative_attempt_share_target"]
        ),
        "COLDMATH_RECOVERY_ADVISOR_WEATHER_STALE_METAR_ATTEMPT_SHARE_TARGET": _format_number(
            advisor_targets["weather_stale_metar_attempt_share_target"]
        ),
        "COLDMATH_RECOVERY_ADVISOR_WEATHER_MIN_ATTEMPTS_TARGET": str(advisor_targets["weather_min_attempts_target"]),
        "COLDMATH_RECOVERY_ADVISOR_OPTIMIZER_TOP_N": str(advisor_targets["optimizer_top_n"]),
        "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_ENABLED": (
            "1" if bool(advisor_targets["plateau_negative_regime_suppression_enabled"]) else "0"
        ),
        "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_MIN_BUCKET_SAMPLES": str(
            advisor_targets["plateau_negative_regime_suppression_min_bucket_samples"]
        ),
        "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_EXPECTANCY_THRESHOLD": _format_number(
            advisor_targets["plateau_negative_regime_suppression_expectancy_threshold"]
        ),
        "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_TOP_N": str(
            advisor_targets["plateau_negative_regime_suppression_top_n"]
        ),
        "COLDMATH_RECOVERY_LOOP_RETUNE_WEATHER_WINDOW_HOURS_CAP": _format_number(
            advisor_targets["retune_weather_window_hours_cap"]
        ),
        "COLDMATH_RECOVERY_LOOP_RETUNE_OVERBLOCKING_BLOCKED_SHARE_THRESHOLD": _format_number(
            advisor_targets["retune_overblocking_blocked_share_threshold"]
        ),
        "COLDMATH_RECOVERY_LOOP_RETUNE_UNDERBLOCKING_MIN_TOP_N": str(
            advisor_targets["retune_underblocking_min_top_n"]
        ),
        "COLDMATH_RECOVERY_LOOP_RETUNE_OVERBLOCKING_MAX_TOP_N": str(
            advisor_targets["retune_overblocking_max_top_n"]
        ),
        "COLDMATH_RECOVERY_LOOP_RETUNE_MIN_BUCKET_SAMPLES_TARGET": str(
            advisor_targets["retune_min_bucket_samples_target"]
        ),
        "COLDMATH_RECOVERY_LOOP_RETUNE_EXPECTANCY_THRESHOLD_TARGET": _format_number(
            advisor_targets["retune_expectancy_threshold_target"]
        ),
        "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MODE": _text(profile_adaptation_mode),
        "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTED_PROFILES_USED": "1"
        if bool(adapted_profiles_used)
        else "0",
        "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MOMENTUM_SCORE": str(int(momentum_score)),
    }
    return {
        "profile_name": _text(best_profile.get("name")),
        "execute_actions": bool(execute_actions),
        "profile": profile_values,
        "advisor_targets": dict(advisor_targets),
        "env": env_overrides,
    }


def run_kalshi_temperature_recovery_campaign(
    *,
    output_dir: str,
    trader_env_file: str = _DEFAULT_TRADER_ENV_FILE,
    execute_actions: bool = True,
    profiles: list[dict[str, Any]] | None = None,
    advisor_targets: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    safe_trader_env_file = _text(trader_env_file) or _DEFAULT_TRADER_ENV_FILE
    normalized_targets = _sanitize_advisor_targets(advisor_targets)
    started_at = datetime.now(timezone.utc)
    decision_matrix_artifact_path, decision_matrix_artifact = _find_latest_decision_matrix_hardening_artifact(health_dir)
    adaptation_signals = _derive_profile_adaptation_signals(decision_matrix_artifact)

    if profiles is None:
        base_profiles = _sanitize_profiles(None)
        normalized_profiles, profile_mode = _adapt_default_profile_rows(base_profiles, adaptation_signals)
        adapted_profiles_used = profile_mode in {"acceleration", "confirmation"}
    else:
        normalized_profiles = _sanitize_profiles(profiles)
        profile_mode = "explicit_profiles"
        adapted_profiles_used = False

    runs: list[dict[str, Any]] = []
    for profile in normalized_profiles:
        loop_payload = run_kalshi_temperature_recovery_loop(
            output_dir=str(out_dir),
            trader_env_file=safe_trader_env_file,
            max_iterations=int(profile["max_iterations"]),
            stall_iterations=int(profile["stall_iterations"]),
            min_gap_improvement=float(profile["min_gap_improvement"]),
            weather_window_hours=float(normalized_targets["weather_window_hours"]),
            weather_min_bucket_samples=int(normalized_targets["weather_min_bucket_samples"]),
            weather_max_profile_age_hours=float(normalized_targets["weather_max_profile_age_hours"]),
            weather_negative_expectancy_attempt_share_target=float(
                normalized_targets["weather_negative_expectancy_attempt_share_target"]
            ),
            weather_stale_metar_negative_attempt_share_target=float(
                normalized_targets["weather_stale_metar_negative_attempt_share_target"]
            ),
            weather_stale_metar_attempt_share_target=float(
                normalized_targets["weather_stale_metar_attempt_share_target"]
            ),
            weather_min_attempts_target=int(normalized_targets["weather_min_attempts_target"]),
            optimizer_top_n=int(normalized_targets["optimizer_top_n"]),
            plateau_negative_regime_suppression_enabled=bool(
                normalized_targets["plateau_negative_regime_suppression_enabled"]
            ),
            plateau_negative_regime_suppression_min_bucket_samples=int(
                normalized_targets["plateau_negative_regime_suppression_min_bucket_samples"]
            ),
            plateau_negative_regime_suppression_expectancy_threshold=float(
                normalized_targets["plateau_negative_regime_suppression_expectancy_threshold"]
            ),
            plateau_negative_regime_suppression_top_n=int(
                normalized_targets["plateau_negative_regime_suppression_top_n"]
            ),
            retune_weather_window_hours_cap=float(normalized_targets["retune_weather_window_hours_cap"]),
            retune_overblocking_blocked_share_threshold=float(
                normalized_targets["retune_overblocking_blocked_share_threshold"]
            ),
            retune_underblocking_min_top_n=int(normalized_targets["retune_underblocking_min_top_n"]),
            retune_overblocking_max_top_n=int(normalized_targets["retune_overblocking_max_top_n"]),
            retune_min_bucket_samples_target=int(normalized_targets["retune_min_bucket_samples_target"]),
            retune_expectancy_threshold_target=float(normalized_targets["retune_expectancy_threshold_target"]),
            execute_actions=bool(execute_actions),
        )
        runs.append(_build_profile_result(profile, loop_payload))

    best_profile = max(runs, key=_best_profile_sort_key) if runs else None
    completed_at = datetime.now(timezone.utc)
    recommended_env_overrides = _build_env_overrides(
        best_profile=best_profile,
        advisor_targets=normalized_targets,
        execute_actions=bool(execute_actions),
        profile_adaptation_mode=profile_mode,
        adapted_profiles_used=bool(adapted_profiles_used),
        momentum_score=_safe_int(adaptation_signals.get("momentum_score")) or 0,
    )
    recommended_env_overrides["env"] = _sorted_env_mapping(recommended_env_overrides.get("env"))

    payload: dict[str, Any] = {
        "status": "ready",
        "started_at": started_at.isoformat(),
        "captured_at": completed_at.isoformat(),
        "output_dir": str(out_dir),
        "health_dir": str(health_dir),
        "profile_adaptation": {
            "mode": profile_mode,
            "source_artifact": str(decision_matrix_artifact_path) if decision_matrix_artifact_path else "",
            "source_status": _text(decision_matrix_artifact.get("status")) if isinstance(decision_matrix_artifact, dict) else "",
            "derived_signals": adaptation_signals,
            "adapted_profiles_used": bool(adapted_profiles_used),
        },
        "inputs": {
            "trader_env_file": safe_trader_env_file,
            "execute_actions": bool(execute_actions),
            "profiles": normalized_profiles,
            "advisor_targets": normalized_targets,
        },
        "profiles_evaluated": len(runs),
        "runs": runs,
        "best_profile": best_profile,
        "recommended_env_overrides": recommended_env_overrides,
    }

    stamp = completed_at.strftime("%Y%m%d_%H%M%S")
    output_path = health_dir / f"kalshi_temperature_recovery_campaign_{stamp}.json"
    latest_path = health_dir / "kalshi_temperature_recovery_campaign_latest.json"
    env_export_path = health_dir / "kalshi_temperature_recovery_recommended_env.sh"
    env_patch_path = health_dir / "kalshi_temperature_recovery_recommended.env"
    payload["output_file"] = str(output_path)
    payload["latest_file"] = str(latest_path)
    payload["recommended_env_export_file"] = str(env_export_path)
    payload["recommended_env_patch_file"] = str(env_patch_path)

    recommended_env = _sorted_env_mapping(_as_dict(_as_dict(payload.get("recommended_env_overrides")).get("env")))
    payload["recommended_env_overrides"]["env"] = recommended_env

    encoded = json.dumps(payload, indent=2, sort_keys=True)
    _write_text_atomic(env_export_path, _render_env_export_text(recommended_env))
    _write_text_atomic(env_patch_path, _render_env_patch_text(recommended_env))
    _write_text_atomic(output_path, encoded)
    _write_text_atomic(latest_path, encoded)
    return payload


def summarize_kalshi_temperature_recovery_campaign(
    *,
    output_dir: str,
    trader_env_file: str = _DEFAULT_TRADER_ENV_FILE,
    execute_actions: bool = True,
    profiles: list[dict[str, Any]] | None = None,
    advisor_targets: dict[str, Any] | None = None,
) -> str:
    payload = run_kalshi_temperature_recovery_campaign(
        output_dir=output_dir,
        trader_env_file=trader_env_file,
        execute_actions=execute_actions,
        profiles=profiles,
        advisor_targets=advisor_targets,
    )
    return json.dumps(payload, indent=2, sort_keys=True)
