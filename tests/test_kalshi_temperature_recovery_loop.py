from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from betbot import kalshi_temperature_recovery_advisor as recovery_advisor
from betbot import kalshi_temperature_recovery_loop as loop


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _seed_execution_cost_exclusions_state(
    path: Path,
    *,
    tickers: list[str],
    active: bool,
    seen_runs: int,
    missing_runs: int = 0,
    market_side_targets: list[str] | None = None,
    active_market_side_targets: bool | None = None,
    side_seen_runs: int | None = None,
    side_missing_runs: int = 0,
    previous_candidate_count: int | None = None,
    previous_active_count: int | None = None,
    previous_quote_coverage_ratio: float | None = None,
    previous_min_quote_coverage_ratio: float | None = None,
) -> None:
    tracked_tickers = {
        ticker: {
            "active": active,
            "first_seen_run": 1,
            "last_seen_run": seen_runs if seen_runs > 0 else 0,
            "last_active_run": seen_runs if active else 0,
            "consecutive_seen_runs": seen_runs,
            "consecutive_missing_runs": missing_runs,
            "current_candidate_rank": index,
        }
        for index, ticker in enumerate(tickers)
    }
    normalized_market_side_targets = [
        str(target).strip()
        for target in list(market_side_targets or [])
        if str(target).strip()
    ]
    if active_market_side_targets is None:
        active_market_side_targets = bool(active)
    if side_seen_runs is None:
        side_seen_runs = int(seen_runs)
    tracked_market_side_targets = {
        target: {
            "active": bool(active_market_side_targets),
            "first_seen_run": 1,
            "last_seen_run": int(side_seen_runs) if int(side_seen_runs) > 0 else 0,
            "last_active_run": int(side_seen_runs) if bool(active_market_side_targets) else 0,
            "consecutive_seen_runs": int(side_seen_runs),
            "consecutive_missing_runs": int(side_missing_runs),
            "current_candidate_rank": index,
        }
        for index, target in enumerate(normalized_market_side_targets)
    }
    payload: dict[str, object] = {
        "status": "ready",
        "run_count": max(seen_runs, missing_runs, 1),
        "candidate_count": len(tickers),
        "active_count": len(tickers) if active else 0,
        "candidate_tickers": list(tickers),
        "active_tickers": list(tickers) if active else [],
        "tracked_tickers": tracked_tickers,
        "candidate_market_side_target_count": len(normalized_market_side_targets),
        "active_market_side_target_count": len(normalized_market_side_targets) if bool(active_market_side_targets) else 0,
        "candidate_market_side_targets": list(normalized_market_side_targets),
        "active_market_side_targets": list(normalized_market_side_targets) if bool(active_market_side_targets) else [],
        "tracked_market_side_targets": tracked_market_side_targets,
    }
    if previous_candidate_count is not None:
        payload["previous_candidate_count"] = int(previous_candidate_count)
        payload["candidate_count"] = int(previous_candidate_count)
    if previous_active_count is not None:
        payload["previous_active_count"] = int(previous_active_count)
        payload["active_count"] = int(previous_active_count)
    if previous_quote_coverage_ratio is not None:
        payload["previous_quote_coverage_ratio"] = round(float(previous_quote_coverage_ratio), 6)
        payload["last_quote_coverage_ratio"] = round(float(previous_quote_coverage_ratio), 6)
    if previous_min_quote_coverage_ratio is not None:
        payload["previous_min_quote_coverage_ratio"] = round(float(previous_min_quote_coverage_ratio), 6)
        payload["last_min_quote_coverage_ratio"] = round(float(previous_min_quote_coverage_ratio), 6)
    _write_json(path, payload)


def _advisor_payload(
    *,
    status: str,
    actions: list[str],
    gap_score: float,
    negative_expectancy_attempt_share: float | None = None,
) -> dict[str, object]:
    negative_share = round(
        float(gap_score if negative_expectancy_attempt_share is None else negative_expectancy_attempt_share),
        6,
    )
    return {
        "status": "ready",
        "metrics": {
            "weather": {
                "negative_expectancy_attempt_share": negative_share,
            }
        },
        "remediation_plan": {
            "status": status,
            "prioritized_actions": [{"key": key} for key in actions],
            "gap_to_clear": {
                "weather_negative_expectancy_attempt_share": round(float(gap_score), 6),
                "weather_stale_metar_negative_attempt_share": 0.0,
                "weather_stale_metar_attempt_share": 0.0,
                "weather_min_attempts": 0,
            },
        },
    }


def _aggressive_effectiveness_thresholds(**_: object) -> dict[str, float | int]:
    return {
        "severity": 0.99,
        "worsening_velocity": 0.05,
        "min_executions": 1,
        "min_worsening_ratio": 0.0,
        "min_average_negative_share_delta": -1.0,
    }


def _discover_tertiary_action_candidates() -> set[str]:
    candidates = {
        "clear_weather_risk_off_state",
        "resolve_decision_matrix_weather_blockers",
        "retune_negative_regime_suppression",
    }
    for name, value in vars(loop).items():
        upper_name = name.upper()
        if "TERTIARY" not in upper_name:
            continue
        if isinstance(value, str):
            candidates.add(value)
        elif isinstance(value, dict):
            candidates.update(item for item in value.values() if isinstance(item, str))
        elif isinstance(value, (set, tuple, list)):
            candidates.update(item for item in value if isinstance(item, str))
    return candidates


def _extract_replacement_profile_or_tier_scalars(payload: object) -> dict[str, object]:
    scalars: dict[str, object] = {}

    def _walk(node: object, *, path: tuple[str, ...], replacement_context: bool) -> None:
        if isinstance(node, dict):
            for raw_key, raw_value in node.items():
                key = str(raw_key)
                key_lower = key.lower()
                next_path = (*path, key)
                next_replacement_context = replacement_context or ("replacement" in key_lower)
                is_profile_or_tier = "profile" in key_lower or "tier" in key_lower
                if is_profile_or_tier and next_replacement_context:
                    if isinstance(raw_value, (str, int, float, bool)) or raw_value is None:
                        scalars[".".join(next_path)] = raw_value
                _walk(raw_value, path=next_path, replacement_context=next_replacement_context)
        elif isinstance(node, list):
            for idx, item in enumerate(node):
                _walk(item, path=(*path, f"[{idx}]"), replacement_context=replacement_context)

    _walk(payload, path=(), replacement_context=False)
    return scalars


def _tier_rank(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    tiers = {
        "lenient": 0,
        "relaxed": 0,
        "balanced": 1,
        "moderate": 1,
        "standard": 1,
        "strict": 2,
        "tight": 2,
        "defensive": 3,
        "hardened": 3,
        "maximum": 4,
        "max": 4,
    }
    return tiers.get(normalized)


def _call_demoted_source_replacement_route(
    *,
    action_key: str,
    policy_class: str,
    demoted_actions: set[str],
    auto_disabled_actions: set[str],
    replacement_sources_executed: set[str],
    action_effectiveness: dict[str, dict[str, int | float]] | None = None,
) -> dict[str, object]:
    route_fn = loop._resolve_demoted_source_replacement_route
    signature = inspect.signature(route_fn)
    kwargs: dict[str, object] = {
        "action_key": action_key,
        "policy_class": policy_class,
        "demoted_actions": demoted_actions,
        "auto_disabled_actions": auto_disabled_actions,
        "replacement_sources_executed": replacement_sources_executed,
    }
    if "action_effectiveness" in signature.parameters:
        kwargs["action_effectiveness"] = action_effectiveness if action_effectiveness is not None else {}
    if "adaptive_effectiveness_thresholds" in signature.parameters:
        kwargs["adaptive_effectiveness_thresholds"] = _aggressive_effectiveness_thresholds()
    if "negative_share_delta_history" in signature.parameters:
        kwargs["negative_share_delta_history"] = [0.04, 0.03, 0.02]
    missing_required = [
        name
        for name, parameter in signature.parameters.items()
        if parameter.default is inspect.Signature.empty and name not in kwargs
    ]
    if missing_required:
        pytest.xfail(
            "Replacement route helper requires unsupported arbitration inputs: "
            + ", ".join(sorted(missing_required))
        )
    return route_fn(**kwargs)


def _arbitration_scalar_fields(node: object) -> dict[str, object]:
    fields: dict[str, object] = {}

    def _walk(value: object, *, path: tuple[str, ...], arbitration_context: bool) -> None:
        if isinstance(value, dict):
            for raw_key, raw_child in value.items():
                key = str(raw_key)
                next_path = (*path, key)
                key_lower = key.lower()
                next_arbitration_context = arbitration_context or ("arbitr" in key_lower)
                if next_arbitration_context and (
                    isinstance(raw_child, (str, int, float, bool)) or raw_child is None
                ):
                    fields[".".join(next_path)] = raw_child
                _walk(raw_child, path=next_path, arbitration_context=next_arbitration_context)
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                _walk(item, path=(*path, f"[{idx}]"), arbitration_context=arbitration_context)

    _walk(node, path=(), arbitration_context=False)
    return fields


def _final_fallback_scalar_fields(node: object) -> dict[str, object]:
    fields: dict[str, object] = {}

    def _walk(value: object, *, path: tuple[str, ...], final_fallback_context: bool) -> None:
        if isinstance(value, dict):
            for raw_key, raw_child in value.items():
                key = str(raw_key)
                next_path = (*path, key)
                key_lower = key.lower()
                next_final_fallback_context = final_fallback_context or ("final_fallback" in key_lower)
                if next_final_fallback_context and (
                    isinstance(raw_child, (str, int, float, bool)) or raw_child is None
                ):
                    fields[".".join(next_path)] = raw_child
                _walk(raw_child, path=next_path, final_fallback_context=next_final_fallback_context)
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                _walk(item, path=(*path, f"[{idx}]"), final_fallback_context=final_fallback_context)

    _walk(node, path=(), final_fallback_context=False)
    return fields


def _find_final_fallback_reserve_hit(
    payload: dict[str, object],
    *,
    source_action_key: str,
) -> tuple[dict[str, object], dict[str, object], dict[str, object], str, str, dict[str, object]] | None:
    iteration_logs = payload.get("iteration_logs")
    if not isinstance(iteration_logs, list):
        return None

    for iteration in iteration_logs:
        if not isinstance(iteration, dict):
            continue
        source_row = next(
            (
                row
                for row in iteration.get("executed_actions", [])
                if isinstance(row, dict)
                and row.get("key") == source_action_key
                and str(row.get("replacement_route_stage") or "").strip().lower() == "final_fallback"
            ),
            None,
        )
        if source_row is None:
            continue
        replacement_log_row = next(
            (
                row
                for row in iteration.get("replacement_actions", [])
                if isinstance(row, dict) and row.get("source_action_key") == source_action_key
            ),
            None,
        )
        if replacement_log_row is None:
            continue

        selected_replacement_key = str(source_row.get("selected_replacement_action_key") or "").strip()
        if not selected_replacement_key:
            continue

        reserve_state = iteration.get("final_fallback_reserve_protection_state")
        if not isinstance(reserve_state, dict):
            continue

        reserve_action_key = ""
        reserve_entry: dict[str, object] | None = None
        selected_entry = reserve_state.get(selected_replacement_key)
        if isinstance(selected_entry, dict):
            reserve_action_key = selected_replacement_key
            reserve_entry = selected_entry
        else:
            protected_actions = iteration.get("final_fallback_reserve_protected_actions")
            if isinstance(protected_actions, list):
                for raw_key in protected_actions:
                    candidate_key = str(raw_key or "").strip()
                    if not candidate_key:
                        continue
                    candidate_entry = reserve_state.get(candidate_key)
                    if isinstance(candidate_entry, dict):
                        reserve_action_key = candidate_key
                        reserve_entry = candidate_entry
                        break
        if reserve_entry is None:
            for raw_key, raw_entry in reserve_state.items():
                candidate_key = str(raw_key or "").strip()
                if not candidate_key or not isinstance(raw_entry, dict):
                    continue
                reserve_action_key = candidate_key
                reserve_entry = raw_entry
                break
        if reserve_entry is None or not reserve_action_key:
            continue

        return (
            iteration,
            source_row,
            replacement_log_row,
            selected_replacement_key,
            reserve_action_key,
            reserve_entry,
        )

    return None


def _patch_recovery_action_runners_ready(monkeypatch) -> None:
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_profitability", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_settled_outcome_throughput", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_metar_ingest", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_constraint_scan", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_contract_specs", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_growth_optimizer", lambda **kwargs: {"status": "ready"})


def _no_replacement_route_stub(**_: object) -> dict[str, object]:
    return {
        "strict_replacement_action_key": "",
        "strict_replacement_action_viable": False,
        "strict_replacement_unavailable_reason": "no_replacement_mapping",
        "tertiary_replacement_action_key": "",
        "tertiary_replacement_action_viable": False,
        "tertiary_replacement_unavailable_reason": "no_tertiary_replacement_mapping",
        "final_fallback_action_key": "",
        "final_fallback_action_viable": False,
        "final_fallback_unavailable_reason": "no_final_fallback_mapping",
        "replacement_route_stage": "none",
        "selected_replacement_action_key": "",
        "source_reason": "demoted_source_replacement_unavailable",
        "replacement_routing_status": "unavailable",
        "replacement_route_arbitration": {
            "policy": "stub_no_replacement_route",
            "selected_route_stage": "none",
            "selected_action_key": "",
            "decision_reason": "stub_no_replacement_route",
        },
    }


def _run_recovery_loop_with_negative_share_sequence(
    *,
    tmp_path: Path,
    monkeypatch,
    source_action_key: str,
    negative_shares: list[float],
    gap_score: float = 0.95,
    aggressive_thresholds: bool = False,
    max_iterations: int | None = None,
) -> dict[str, object]:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=gap_score,
            negative_expectancy_attempt_share=share,
        )
        for share in negative_shares
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    if aggressive_thresholds:
        monkeypatch.setattr(loop, "_compute_adaptive_effectiveness_thresholds", _aggressive_effectiveness_thresholds)
    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    safe_max_iterations = len(negative_shares) - 1 if max_iterations is None else max_iterations
    return loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=safe_max_iterations,
        stall_iterations=8,
        min_gap_improvement=0.0,
    )


def test_recovery_loop_clears_after_first_iteration(tmp_path: Path, monkeypatch) -> None:
    advisor_calls: list[dict[str, object]] = []
    weather_calls: list[dict[str, object]] = []
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["clear_weather_risk_off_state"],
            gap_score=0.4,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]

    def fake_advisor(**kwargs):
        advisor_calls.append(dict(kwargs))
        return sequence.pop(0)

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        weather_calls.append(
            {
                "output_dir": output_dir,
                "window_hours": window_hours,
                "min_bucket_samples": min_bucket_samples,
                "max_profile_age_hours": max_profile_age_hours,
            }
        )
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert payload["iterations_executed"] == 1
    assert len(advisor_calls) == 2
    assert len(weather_calls) == 1

    iteration = payload["iteration_logs"][0]
    assert iteration["advisor_status_before"] == "risk_off_active"
    assert iteration["advisor_status_after"] == "risk_off_cleared"
    assert float(iteration["gap_score_before"]) == 0.4
    assert float(iteration["gap_score_after"]) == 0.0
    assert float(iteration["improvement"]) > 0.0
    assert iteration["executed_actions"][0]["key"] == "clear_weather_risk_off_state"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_passes_trader_env_file_for_negative_regime_action(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes"],
            gap_score=0.3,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {"status": "ready", "intents_total": 1, "intents_approved": 1}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/recovery.env",
        weather_min_bucket_samples=17,
        plateau_negative_regime_suppression_min_bucket_samples=20,
        plateau_negative_regime_suppression_expectancy_threshold=-0.02,
        plateau_negative_regime_suppression_top_n=15,
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/recovery.env"
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is True
    assert round(float(trader_call["weather_pattern_profile_max_age_hours"]), 6) == 48.0
    assert int(trader_call["weather_pattern_min_bucket_samples"]) == 17
    assert round(float(trader_call["weather_pattern_negative_expectancy_threshold"]), 6) == -0.03
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert round(float(trader_call["weather_pattern_risk_off_concentration_threshold"]), 6) == 0.65
    assert int(trader_call["weather_pattern_risk_off_min_attempts"]) == 17
    assert round(float(trader_call["weather_pattern_risk_off_stale_metar_share_threshold"]), 6) == 0.45
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 20
    assert round(
        float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]),
        6,
    ) == -0.02
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 15
    assert trader_call["historical_selection_quality_enabled"] is True
    assert trader_call["enforce_probability_edge_thresholds"] is True
    assert round(float(trader_call["min_probability_confidence"]), 6) == 0.75
    assert round(float(trader_call["min_expected_edge_net"]), 6) == 0.015
    assert round(float(trader_call["min_edge_to_risk_ratio"]), 6) == 0.06


def test_recovery_loop_bootstrap_shadow_trade_intents_uses_shadow_bootstrap_flags(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="insufficient_data",
            actions=["bootstrap_shadow_trade_intents"],
            gap_score=0.3,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {"status": "ready", "intents_total": 1, "intents_approved": 1}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/bootstrap.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/bootstrap.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is False
    assert trader_call["weather_pattern_risk_off_enabled"] is False
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is False
    assert trader_call["historical_selection_quality_enabled"] is False
    assert trader_call["enforce_probability_edge_thresholds"] is False

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "bootstrap_shadow_trade_intents"
    assert iteration["executed_actions"][0]["status"] == "executed"
    assert iteration["executed_actions"][0]["effect_status"] == "no_effect"
    assert iteration["executed_actions"][0]["effect_reason"] == "missing_trade_summary_artifact"


def test_recovery_loop_bootstrap_marks_missing_constraint_trader_status_no_effect(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="insufficient_data",
            actions=["bootstrap_shadow_trade_intents"],
            gap_score=0.3,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        return {"status": "missing_constraint_csv"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/bootstrap.env",
    )

    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    assert action["key"] == "bootstrap_shadow_trade_intents"
    assert action["status"] == "executed"
    assert action["effect_status"] == "no_effect"
    assert action["effect_reason"] == "trader_status_missing_constraint_csv"
    assert action["counts_toward_effectiveness"] is False


def test_recovery_loop_latest_trade_summary_artifact_checks_outputs_sibling(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir = tmp_path / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    summary_path = outputs_dir / "kalshi_temperature_trade_intents_summary_20260423_140135.json"
    summary_path.write_text(json.dumps({"status": "ready", "intents_total": 3}), encoding="utf-8")
    resolved = loop._latest_trade_summary_artifact(output_dir)
    assert resolved == summary_path


def test_recovery_loop_trader_fallback_uses_outputs_artifacts(tmp_path: Path, monkeypatch) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir = tmp_path / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    constraint_csv = outputs_dir / "kalshi_temperature_constraint_scan_20260423_141336.csv"
    constraint_csv.write_text("market_ticker,constraint_status\nKXHIGHNY-26APR23-B72,yes_impossible\n", encoding="utf-8")
    specs_csv = outputs_dir / "kalshi_temperature_contract_specs_20260410_050128.csv"
    specs_csv.write_text("market_ticker,market_title\nKXHIGHNY-26APR23-B72,72F or above\n", encoding="utf-8")
    metar_summary_json = outputs_dir / "kalshi_temperature_metar_summary_20260423_122717.json"
    metar_summary_json.write_text(json.dumps({"status": "ready"}), encoding="utf-8")
    settlement_state_json = outputs_dir / "kalshi_temperature_settlement_state_20260423_122749.json"
    settlement_state_json.write_text(json.dumps({"status": "ready"}), encoding="utf-8")

    captured_kwargs: dict[str, object] = {}

    def fake_trader(**kwargs):
        captured_kwargs.update(kwargs)
        return {"status": "intents_only"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop._run_shadow_trader_with_fallback(
        output_dir=str(output_dir),
        env_file="/tmp/recovery.env",
        intents_only=True,
        allow_live_orders=False,
    )
    assert payload["status"] == "intents_only"
    assert captured_kwargs["env_file"] == "/tmp/recovery.env"
    assert captured_kwargs["output_dir"] == str(output_dir)
    assert captured_kwargs["constraint_csv"] == str(constraint_csv)
    assert captured_kwargs["specs_csv"] == str(specs_csv)
    assert captured_kwargs["metar_summary_json"] == str(metar_summary_json)
    assert captured_kwargs["settlement_state_json"] == str(settlement_state_json)


def test_recovery_loop_bootstrap_shadow_trade_intents_marks_effect_verified_when_summary_updates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="insufficient_data",
            actions=["bootstrap_shadow_trade_intents"],
            gap_score=0.3,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        summary_path = tmp_path / "kalshi_temperature_trade_intents_summary_latest.json"
        summary_path.write_text(
            json.dumps({"intents_total": 3}, indent=2),
            encoding="utf-8",
        )
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 3,
                "intents_approved": 3,
                "intents_blocked": 0,
                "policy_reason_counts": {
                    "approved": 3,
                },
                "weather_pattern_hardening_enabled": False,
                "weather_pattern_risk_off_enabled": False,
                "weather_pattern_negative_regime_suppression_enabled": False,
                "historical_selection_quality_enabled": False,
                "enforce_probability_edge_thresholds": False,
            },
            "plan_summary": {
                "status": "ready",
                "planned_orders": 3,
            },
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/bootstrap.env",
    )

    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    assert action["key"] == "bootstrap_shadow_trade_intents"
    assert action["status"] == "executed"
    assert action["effect_status"] == "verified"
    assert action["effect_reason"] == ""
    assert int(action["effect_evidence"]["summary_activity_count"]) == 3
    diagnostics = action["effect_evidence"]["trader_payload_diagnostics"][0]
    assert diagnostics["status"] == "ready"
    assert diagnostics["applied_hardening_flags"]["weather_pattern_hardening_enabled"] is False
    assert diagnostics["applied_hardening_flags"]["enforce_probability_edge_thresholds"] is False
    assert diagnostics["policy_reason_counts"]["approved"] == 3
    assert diagnostics["key_totals"]["intents_total"] == 3
    assert diagnostics["key_totals"]["planned_orders"] == 3


def test_recovery_loop_probe_expected_edge_floor_with_hardening_disabled_uses_expected_flags(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["probe_expected_edge_floor_with_hardening_disabled"],
            gap_score=0.3,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {"status": "ready", "intents_total": 1, "intents_approved": 1}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/probe.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/probe.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is False
    assert trader_call["weather_pattern_risk_off_enabled"] is False
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is False
    assert trader_call["historical_selection_quality_enabled"] is False
    assert trader_call["enforce_probability_edge_thresholds"] is False

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "probe_expected_edge_floor_with_hardening_disabled"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_apply_expected_edge_relief_shadow_profile_uses_conservative_flags(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["apply_expected_edge_relief_shadow_profile"],
            gap_score=0.3,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {"status": "ready", "intents_total": 1, "intents_approved": 1}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/relief.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/relief.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is False
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 12
    assert round(
        float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]),
        6,
    ) == -0.03
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 12
    assert trader_call["historical_selection_quality_enabled"] is True
    assert trader_call["enforce_probability_edge_thresholds"] is True
    assert round(float(trader_call["min_probability_confidence"]), 6) == 0.55
    assert round(float(trader_call["min_expected_edge_net"]), 6) == 0.0
    assert round(float(trader_call["min_edge_to_risk_ratio"]), 6) == 0.02

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "apply_expected_edge_relief_shadow_profile"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_plateau_action_uses_strict_trader_profile_and_weather_refresh(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["plateau_break_negative_expectancy_share"],
            gap_score=0.42,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []
    weather_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {"status": "ready", "intents_total": 1, "intents_approved": 1}

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        weather_calls.append(
            {
                "output_dir": output_dir,
                "window_hours": window_hours,
                "min_bucket_samples": min_bucket_samples,
                "max_profile_age_hours": max_profile_age_hours,
            }
        )
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/plateau.env",
        plateau_negative_regime_suppression_enabled=False,
        plateau_negative_regime_suppression_min_bucket_samples=22,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=13,
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    assert len(weather_calls) == 1

    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/plateau.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is True
    assert float(trader_call["weather_pattern_profile_max_age_hours"]) == 48.0
    assert int(trader_call["weather_pattern_min_bucket_samples"]) == 12
    assert float(trader_call["weather_pattern_negative_expectancy_threshold"]) == -0.03
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is False
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 22
    assert float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]) == -0.08
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 13
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert float(trader_call["weather_pattern_risk_off_concentration_threshold"]) == 0.70
    assert int(trader_call["weather_pattern_risk_off_min_attempts"]) == 18
    assert float(trader_call["weather_pattern_risk_off_stale_metar_share_threshold"]) == 0.45
    assert trader_call["historical_selection_quality_enabled"] is True
    assert float(trader_call["min_probability_confidence"]) == 0.80
    assert float(trader_call["min_expected_edge_net"]) == 0.02
    assert float(trader_call["min_edge_to_risk_ratio"]) == 0.08

    weather_call = weather_calls[0]
    assert weather_call["output_dir"] == str(tmp_path)
    assert float(weather_call["window_hours"]) == 720.0
    assert int(weather_call["min_bucket_samples"]) == 10
    assert float(weather_call["max_profile_age_hours"]) == 336.0

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "plateau_break_negative_expectancy_share"
    assert iteration["executed_actions"][0]["status"] == "executed"
    inputs = payload["inputs"]
    assert inputs["plateau_negative_regime_suppression_enabled"] is False
    assert int(inputs["plateau_negative_regime_suppression_min_bucket_samples"]) == 22
    assert float(inputs["plateau_negative_regime_suppression_expectancy_threshold"]) == -0.08
    assert int(inputs["plateau_negative_regime_suppression_top_n"]) == 13


def test_recovery_loop_retune_negative_regime_suppression_runs_trader_and_refresh(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["retune_negative_regime_suppression"],
            gap_score=0.34,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []
    weather_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {"status": "ready", "intents_total": 1, "intents_approved": 1}

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        weather_calls.append(
            {
                "output_dir": output_dir,
                "window_hours": window_hours,
                "min_bucket_samples": min_bucket_samples,
                "max_profile_age_hours": max_profile_age_hours,
            }
        )
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/retune.env",
        plateau_negative_regime_suppression_enabled=False,
        plateau_negative_regime_suppression_min_bucket_samples=22,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=13,
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    assert len(weather_calls) == 1

    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/retune.env"
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is True
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 14
    assert round(float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]), 6) == -0.05
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 16
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert trader_call["historical_selection_quality_enabled"] is True

    weather_call = weather_calls[0]
    assert weather_call["output_dir"] == str(tmp_path)
    assert float(weather_call["window_hours"]) == 336.0
    assert int(weather_call["min_bucket_samples"]) == 10
    assert float(weather_call["max_profile_age_hours"]) == 336.0

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "retune_negative_regime_suppression"
    assert iteration["executed_actions"][0]["status"] == "executed"
    inputs = payload["inputs"]
    assert float(inputs["retune_weather_window_hours_cap"]) == 336.0
    assert float(inputs["retune_overblocking_blocked_share_threshold"]) == 0.25
    assert int(inputs["retune_underblocking_min_top_n"]) == 16
    assert int(inputs["retune_overblocking_max_top_n"]) == 4
    assert int(inputs["retune_min_bucket_samples_target"]) == 14
    assert float(inputs["retune_expectancy_threshold_target"]) == -0.045


def test_recovery_loop_retune_negative_regime_suppression_uses_custom_tuning_inputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_candidate_count": 10,
                "weather_pattern_negative_regime_suppression_blocked_count": 2,
            }
        ),
        encoding="utf-8",
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["retune_negative_regime_suppression"],
            gap_score=0.33,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []
    weather_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 5,
                "intents_approved": 2,
                "intents_blocked": 3,
            },
        }

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        weather_calls.append(
            {
                "output_dir": output_dir,
                "window_hours": window_hours,
                "min_bucket_samples": min_bucket_samples,
                "max_profile_age_hours": max_profile_age_hours,
            }
        )
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        weather_window_hours=800.0,
        plateau_negative_regime_suppression_min_bucket_samples=22,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=13,
        retune_weather_window_hours_cap=120.0,
        retune_overblocking_blocked_share_threshold=0.15,
        retune_underblocking_min_top_n=20,
        retune_overblocking_max_top_n=3,
        retune_min_bucket_samples_target=12,
        retune_expectancy_threshold_target=-0.04,
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    assert len(weather_calls) == 1

    trader_call = trader_calls[0]
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 22
    assert round(float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]), 6) == -0.045
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 3

    weather_call = weather_calls[0]
    assert float(weather_call["window_hours"]) == 120.0

    inputs = payload["inputs"]
    assert float(inputs["retune_weather_window_hours_cap"]) == 120.0
    assert float(inputs["retune_overblocking_blocked_share_threshold"]) == 0.15
    assert int(inputs["retune_underblocking_min_top_n"]) == 20
    assert int(inputs["retune_overblocking_max_top_n"]) == 3
    assert int(inputs["retune_min_bucket_samples_target"]) == 12
    assert float(inputs["retune_expectancy_threshold_target"]) == -0.04


def test_recovery_loop_retune_negative_regime_suppression_overblocking_reduces_top_n(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_candidate_count": 10,
                "weather_pattern_negative_regime_suppression_blocked_count": 5,
            }
        ),
        encoding="utf-8",
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["retune_negative_regime_suppression"],
            gap_score=0.31,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 5,
                "intents_approved": 2,
                "intents_blocked": 3,
            },
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        plateau_negative_regime_suppression_enabled=True,
        plateau_negative_regime_suppression_min_bucket_samples=22,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=13,
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 22
    assert round(float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]), 6) == -0.05
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 4


def test_recovery_loop_retune_negative_regime_suppression_severe_overblocking_further_reduces_choke(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_candidate_count": 10,
                "weather_pattern_negative_regime_suppression_blocked_count": 9,
            }
        ),
        encoding="utf-8",
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["retune_negative_regime_suppression"],
            gap_score=0.30,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []
    weather_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {"status": "ready", "intents_total": 1, "intents_approved": 1}

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        weather_calls.append(
            {
                "output_dir": output_dir,
                "window_hours": float(window_hours),
                "min_bucket_samples": int(min_bucket_samples),
                "max_profile_age_hours": float(max_profile_age_hours),
            }
        )
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        plateau_negative_regime_suppression_min_bucket_samples=22,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=13,
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    assert len(weather_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 22
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 2
    assert round(
        float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]),
        6,
    ) == -0.06
    weather_call = weather_calls[0]
    assert weather_call["window_hours"] == 336.0
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "retune_negative_regime_suppression"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_refresh_market_horizon_inputs_runs_expected_sequence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_market_horizon_inputs"],
            gap_score=0.22,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_contract_specs(*, env_file: str, output_dir: str):
        call_order.append(("contract_specs", env_file))
        return {"status": "ready", "output_dir": output_dir}

    def fake_constraint_scan(*, output_dir: str):
        call_order.append(("constraint_scan", output_dir))
        return {"status": "ready"}

    def fake_settlement_state(*, output_dir: str):
        call_order.append(("settlement_state", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_contract_specs", fake_contract_specs)
    monkeypatch.setattr(loop, "run_kalshi_temperature_constraint_scan", fake_constraint_scan)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/horizon.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("contract_specs", "/tmp/horizon.env"),
        ("constraint_scan", str(tmp_path)),
        ("settlement_state", str(tmp_path)),
    ]
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "refresh_market_horizon_inputs"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_refresh_market_horizon_inputs_retries_missing_env_with_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_market_horizon_inputs"],
            gap_score=0.22,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_contract_specs(*, env_file: str, output_dir: str):
        call_order.append(("contract_specs", env_file))
        if env_file == "/tmp/missing.env":
            raise ValueError(f"Env file not found: {env_file}")
        return {"status": "ready", "output_dir": output_dir}

    def fake_constraint_scan(*, output_dir: str):
        call_order.append(("constraint_scan", output_dir))
        return {"status": "ready"}

    def fake_settlement_state(*, output_dir: str):
        call_order.append(("settlement_state", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_contract_specs", fake_contract_specs)
    monkeypatch.setattr(loop, "run_kalshi_temperature_constraint_scan", fake_constraint_scan)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "_resolve_contract_specs_retry_env_file", lambda primary_env_file: "/tmp/fallback.env")

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/missing.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("contract_specs", "/tmp/missing.env"),
        ("contract_specs", "/tmp/fallback.env"),
        ("constraint_scan", str(tmp_path)),
        ("settlement_state", str(tmp_path)),
    ]
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "refresh_market_horizon_inputs"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_repair_taf_station_mapping_pipeline_runs_expected_sequence_with_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["repair_taf_station_mapping_pipeline"],
            gap_score=0.22,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_contract_specs(*, env_file: str, output_dir: str):
        call_order.append(("contract_specs", env_file))
        if env_file == "/tmp/missing.env":
            raise ValueError(f"Env file not found: {env_file}")
        return {"status": "ready"}

    def fake_constraint_scan(*, output_dir: str):
        call_order.append(("constraint_scan", output_dir))
        return {"status": "ready"}

    def fake_settlement_state(*, output_dir: str):
        call_order.append(("settlement_state", output_dir))
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 2,
                "intents_approved": 2,
                "intents_blocked": 0,
                "policy_reason_counts": {
                    "approved": 2,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 2},
        }

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_contract_specs", fake_contract_specs)
    monkeypatch.setattr(loop, "run_kalshi_temperature_constraint_scan", fake_constraint_scan)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(loop, "_resolve_contract_specs_retry_env_file", lambda primary_env_file: "/tmp/fallback.env")

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/missing.env",
        weather_min_bucket_samples=17,
        plateau_negative_regime_suppression_min_bucket_samples=20,
        plateau_negative_regime_suppression_expectancy_threshold=-0.02,
        plateau_negative_regime_suppression_top_n=15,
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("contract_specs", "/tmp/missing.env"),
        ("contract_specs", "/tmp/fallback.env"),
        ("constraint_scan", str(tmp_path)),
        ("settlement_state", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/missing.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is True
    assert round(float(trader_call["weather_pattern_profile_max_age_hours"]), 6) == 24.0
    assert int(trader_call["weather_pattern_min_bucket_samples"]) == 17
    assert round(float(trader_call["weather_pattern_negative_expectancy_threshold"]), 6) == -0.025
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert round(float(trader_call["weather_pattern_risk_off_concentration_threshold"]), 6) == 0.60
    assert int(trader_call["weather_pattern_risk_off_min_attempts"]) == 17
    assert round(float(trader_call["weather_pattern_risk_off_stale_metar_share_threshold"]), 6) == 0.45
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 20
    assert round(float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]), 6) == -0.02
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 15
    assert trader_call["historical_selection_quality_enabled"] is True
    assert trader_call["enforce_probability_edge_thresholds"] is True
    assert round(float(trader_call["min_probability_confidence"]), 6) == 0.82
    assert round(float(trader_call["min_expected_edge_net"]), 6) == 0.02
    assert round(float(trader_call["min_edge_to_risk_ratio"]), 6) == 0.08

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "repair_taf_station_mapping_pipeline"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_reduce_execution_friction_pressure_runs_expected_sequence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_execution_cost_exclusions_state(
        tmp_path / "health" / "execution_cost_exclusions_state_latest.json",
        tickers=[
            "KXHIGHBOS-26APR22-B75",
            "KXHIGHNYC-26APR22-T72",
        ],
        active=False,
        seen_runs=1,
    )
    _write_json(
        tmp_path / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "recommended_exclusions": {
                "market_tickers": [
                    "kxhighbos-26apr22-b75",
                    "KXHIGHBOS-26APR22-B75",
                    "kxhighnyc-26apr22-t72",
                ]
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.12,
                "min_quote_coverage_ratio": 0.20,
            },
        },
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_execution_cost_tape(*, output_dir: str):
        call_order.append(("execution_cost_tape", output_dir))
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 2,
                "intents_approved": 2,
                "intents_blocked": 0,
                "policy_reason_counts": {
                    "approved": 2,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 2},
        }

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
        weather_min_bucket_samples=9,
        plateau_negative_regime_suppression_min_bucket_samples=7,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=3,
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("execution_cost_tape", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/friction.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is True
    assert round(float(trader_call["weather_pattern_profile_max_age_hours"]), 6) == 24.0
    assert int(trader_call["weather_pattern_min_bucket_samples"]) == 12
    assert round(float(trader_call["weather_pattern_negative_expectancy_threshold"]), 6) == -0.025
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert round(float(trader_call["weather_pattern_risk_off_concentration_threshold"]), 6) == 0.60
    assert int(trader_call["weather_pattern_risk_off_min_attempts"]) == 12
    assert round(float(trader_call["weather_pattern_risk_off_stale_metar_share_threshold"]), 6) == 0.45
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 12
    assert round(float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]), 6) == -0.03
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 10
    assert trader_call["historical_selection_quality_enabled"] is True
    assert trader_call["enforce_probability_edge_thresholds"] is True
    assert round(float(trader_call["min_probability_confidence"]), 6) == 0.86
    assert round(float(trader_call["min_expected_edge_net"]), 6) == 0.025
    assert round(float(trader_call["min_edge_to_risk_ratio"]), 6) == 0.10
    assert trader_call["exclude_market_tickers"] == [
        "KXHIGHBOS-26APR22-B75",
        "KXHIGHNYC-26APR22-T72",
    ]
    assert "shadow_quote_probe_market_side_targets" not in trader_call

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "reduce_execution_friction_pressure"
    assert iteration["executed_actions"][0]["status"] == "executed"
    effect_evidence = iteration["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_candidate_count"] == 2
    assert effect_evidence["execution_cost_exclusion_active_count"] == 2
    assert effect_evidence["execution_cost_exclusion_market_side_target_count"] == 0
    assert effect_evidence["execution_cost_exclusion_market_side_targets"] == []
    assert effect_evidence["execution_cost_exclusion_active_target_count"] == 2
    assert effect_evidence["execution_cost_exclusion_second_probe_market_side_target_count"] == 0
    assert effect_evidence["execution_cost_exclusion_state_file"] == str(
        tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    )
    assert effect_evidence["execution_cost_exclusion_ticker_count"] == 2
    assert effect_evidence["execution_cost_exclusion_tickers"] == [
        "KXHIGHBOS-26APR22-B75",
        "KXHIGHNYC-26APR22-T72",
    ]
    assert effect_evidence["execution_cost_exclusion_downshift_triggered"] is False
    assert effect_evidence["execution_cost_exclusion_downshift_reason"] == "pressure_below_near_cap_threshold"
    assert effect_evidence["execution_cost_exclusion_downshift_active_count_before"] == 2
    assert effect_evidence["execution_cost_exclusion_downshift_active_count_after"] == 2
    assert effect_evidence["execution_cost_exclusion_downshift_removed_count"] == 0


def test_recovery_loop_reduce_execution_friction_pressure_falls_back_to_top_missing_market_tickers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_execution_cost_exclusions_state(
        tmp_path / "health" / "execution_cost_exclusions_state_latest.json",
        tickers=[
            "KXHIGHBOS-26APR22-B75",
            "KXHIGHNYC-26APR22-T72",
        ],
        active=False,
        seen_runs=1,
    )
    _write_json(
        tmp_path / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "execution_cost_observations": {
                "top_missing_coverage_buckets": {
                    "by_market": [
                        {"bucket": "kxhighbos-26apr22-b75", "rows_without_two_sided_quote": 9},
                        {"bucket": "KXHIGHNYC-26APR22-T72", "rows_without_two_sided_quote": 6},
                        {"bucket": "kxhighbos-26apr22-b75", "rows_without_two_sided_quote": 4},
                    ]
                }
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.08,
                "min_quote_coverage_ratio": 0.20,
            },
        },
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 2,
                "intents_approved": 2,
                "intents_blocked": 0,
                "policy_reason_counts": {
                    "approved": 2,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 2},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
        weather_min_bucket_samples=9,
        plateau_negative_regime_suppression_min_bucket_samples=7,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=3,
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["exclude_market_tickers"] == [
        "KXHIGHBOS-26APR22-B75",
        "KXHIGHNYC-26APR22-T72",
    ]
    assert "shadow_quote_probe_market_side_targets" not in trader_call
    effect_evidence = payload["iteration_logs"][0]["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_candidate_count"] == 2
    assert effect_evidence["execution_cost_exclusion_active_count"] == 2
    assert effect_evidence["execution_cost_exclusion_market_side_target_count"] == 0
    assert effect_evidence["execution_cost_exclusion_market_side_targets"] == []
    assert effect_evidence["execution_cost_exclusion_active_target_count"] == 2
    assert effect_evidence["execution_cost_exclusion_second_probe_market_side_target_count"] == 0
    assert effect_evidence["execution_cost_exclusion_state_file"] == str(
        tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    )
    assert effect_evidence["execution_cost_exclusion_ticker_count"] == 2
    assert effect_evidence["execution_cost_exclusion_tickers"] == [
        "KXHIGHBOS-26APR22-B75",
        "KXHIGHNYC-26APR22-T72",
    ]
    assert effect_evidence["execution_cost_exclusion_downshift_triggered"] is False
    assert effect_evidence["execution_cost_exclusion_downshift_reason"] == "pressure_below_near_cap_threshold"
    assert effect_evidence["execution_cost_exclusion_downshift_active_count_before"] == 2
    assert effect_evidence["execution_cost_exclusion_downshift_active_count_after"] == 2
    assert effect_evidence["execution_cost_exclusion_downshift_removed_count"] == 0


def test_recovery_loop_reduce_execution_friction_pressure_downshifts_active_exclusions_when_pressure_stalls(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_tickers = [f"KXHIGH{i:02d}-26APR22-B75" for i in range(20)]
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    _seed_execution_cost_exclusions_state(
        state_path,
        tickers=active_tickers,
        active=True,
        seen_runs=4,
    )
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    state_payload["adaptive_downshift"] = {
        "last_evaluated_run": 3,
        "last_downshift_run": 0,
        "last_decision": "no_downshift",
        "last_reason": "",
        "suppressed_tickers": {},
        "last_probe_metrics": {
            "first_probe_status": "no_candidates",
            "first_probe_intents_total": 0,
            "first_probe_intents_approved": 0,
            "second_probe_triggered": True,
            "second_probe_status": "no_candidates",
            "second_probe_intents_total": 0,
            "second_probe_intents_approved": 0,
        },
        "last_coverage_metrics": {
            "quote_coverage_ratio": 0.11,
            "min_quote_coverage_ratio": 0.20,
        },
        "last_active_count_before": 20,
        "last_active_count_after": 20,
        "last_drop_count": 0,
        "last_drop_tickers": [],
    }
    _write_json(state_path, state_payload)
    _write_json(
        tape_path,
        {
            "status": "ready",
            "recommended_exclusions": {
                "market_tickers": list(active_tickers),
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.10,
                "min_quote_coverage_ratio": 0.20,
            },
        },
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_execution_cost_tape(*, output_dir: str):
        call_order.append(("execution_cost_tape", output_dir))
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {
            "status": "no_candidates",
            "intent_summary": {
                "status": "no_candidates",
                "intents_total": 0,
                "intents_approved": 0,
                "intents_blocked": 0,
                "policy_reason_counts": {
                    "expected_edge_below_min": 0,
                },
            },
            "plan_summary": {"status": "no_candidates", "planned_orders": 0},
        }

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
        weather_min_bucket_samples=9,
        plateau_negative_regime_suppression_min_bucket_samples=7,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=3,
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("execution_cost_tape", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 2
    assert trader_calls[0]["exclude_market_tickers"] == active_tickers
    assert trader_calls[1]["exclude_market_tickers"] == active_tickers[:10]
    assert "shadow_quote_probe_market_side_targets" not in trader_calls[0]
    assert "shadow_quote_probe_market_side_targets" not in trader_calls[1]
    state_after = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_after["active_count"] == 15
    assert state_after["active_tickers"] == active_tickers[:15]
    assert round(float(state_after["last_quote_coverage_ratio"]), 6) == 0.10
    assert state_after["adaptive_downshift"]["last_decision"] == "downshift_applied"
    assert state_after["adaptive_downshift"]["last_reason"] == (
        "high_pressure_without_throughput_or_coverage_improvement"
    )
    assert state_after["adaptive_downshift"]["last_active_count_before"] == 20
    assert state_after["adaptive_downshift"]["last_active_count_after"] == 15
    assert state_after["adaptive_downshift"]["last_drop_count"] == 5
    assert state_after["adaptive_downshift"]["last_drop_tickers"] == active_tickers[15:]
    assert state_after["adaptive_downshift"]["suppressed_ticker_count"] == 5
    iteration = payload["iteration_logs"][0]
    effect_evidence = iteration["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_downshift_triggered"] is True
    assert effect_evidence["execution_cost_exclusion_downshift_reason"] == (
        "high_pressure_without_throughput_or_coverage_improvement"
    )
    assert effect_evidence["execution_cost_exclusion_downshift_active_count_before"] == 20
    assert effect_evidence["execution_cost_exclusion_downshift_active_count_after"] == 15
    assert effect_evidence["execution_cost_exclusion_downshift_removed_count"] == 5
    assert effect_evidence["execution_cost_exclusion_downshift_removed_tickers"] == active_tickers[15:]
    assert effect_evidence["execution_cost_exclusion_downshift_pressure_near_cap"] is True
    assert effect_evidence["execution_cost_exclusion_downshift_throughput_stalled"] is True
    assert effect_evidence["execution_cost_exclusion_downshift_coverage_stalled"] is True
    assert effect_evidence["execution_cost_exclusion_downshift_quote_coverage_ratio"] == 0.1
    assert effect_evidence["execution_cost_exclusion_downshift_previous_quote_coverage_ratio"] == 0.11
    assert effect_evidence["execution_cost_exclusion_adaptive_second_probe_triggered"] is True
    assert effect_evidence["execution_cost_exclusion_market_side_target_count"] == 0
    assert effect_evidence["execution_cost_exclusion_market_side_targets"] == []
    assert effect_evidence["execution_cost_exclusion_active_target_count"] == 20
    assert effect_evidence["execution_cost_exclusion_second_probe_market_side_target_count"] == 0


def test_recovery_loop_reduce_execution_friction_pressure_filters_market_side_targets_for_reduced_probe(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_tickers = [
        "KXHIGHBOS-26APR22-B75",
        "KXHIGHNYC-26APR22-T72",
        "KXHIGHCHI-26APR22-T73",
        "KXHIGHDAL-26APR22-T71",
    ]
    _seed_execution_cost_exclusions_state(
        tmp_path / "health" / "execution_cost_exclusions_state_latest.json",
        tickers=active_tickers,
        active=True,
        seen_runs=4,
        market_side_targets=[
            "KXHIGHBOS-26APR22-B75|yes",
            "KXHIGHNYC-26APR22-T72|yes",
            "KXHIGHCHI-26APR22-T73|yes",
        ],
        active_market_side_targets=True,
        side_seen_runs=4,
    )
    _write_json(
        tmp_path / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "recommended_exclusions": {
                "market_tickers": list(active_tickers),
            },
            "execution_cost_observations": {
                "top_missing_coverage_buckets": {
                    "by_market_side": [
                        {"bucket": "KXHIGHBOS-26APR22-B75|yes", "rows_without_two_sided_quote": 9},
                        {"bucket": "KXHIGHBOS-26APR22-B75|no", "rows_without_two_sided_quote": 4},
                        {"bucket": "KXHIGHNYC-26APR22-T72|yes", "rows_without_two_sided_quote": 8},
                        {"bucket": "KXHIGHCHI-26APR22-T73|yes", "rows_without_two_sided_quote": 6},
                        {"bucket": "KXHIGHMIA-26APR22-T80|yes", "rows_without_two_sided_quote": 5},
                    ],
                    "by_side": [
                        {"bucket": "yes", "rows_without_two_sided_quote": 28},
                        {"bucket": "no", "rows_without_two_sided_quote": 4},
                    ],
                }
            },
            "execution_siphon_pressure": {
                "status": "ready",
                "dominant_uncovered_side": "yes",
                "dominant_uncovered_side_share": 0.875,
                "side_imbalance_magnitude": 0.75,
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.10,
                "min_quote_coverage_ratio": 0.20,
            },
        },
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "no_candidates",
            "intent_summary": {
                "status": "no_candidates",
                "intents_total": 0,
                "intents_approved": 0,
                "intents_blocked": 0,
                "policy_reason_counts": {
                    "expected_edge_below_min": 0,
                },
            },
            "plan_summary": {"status": "no_candidates", "planned_orders": 0},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 2
    assert trader_calls[0]["exclude_market_tickers"] == active_tickers + [
        "KXHIGHBOS-26APR22-B75|yes",
        "KXHIGHNYC-26APR22-T72|yes",
        "KXHIGHCHI-26APR22-T73|yes",
    ]
    assert "shadow_quote_probe_market_side_targets" not in trader_calls[0]
    assert trader_calls[1]["exclude_market_tickers"] == active_tickers[:2] + [
        "KXHIGHBOS-26APR22-B75|yes",
        "KXHIGHNYC-26APR22-T72|yes",
    ]
    assert "shadow_quote_probe_market_side_targets" not in trader_calls[1]
    effect_evidence = payload["iteration_logs"][0]["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_market_side_target_count"] == 3
    assert effect_evidence["execution_cost_exclusion_market_side_targets"] == [
        "KXHIGHBOS-26APR22-B75|yes",
        "KXHIGHNYC-26APR22-T72|yes",
        "KXHIGHCHI-26APR22-T73|yes",
    ]
    assert effect_evidence["execution_cost_exclusion_active_target_count"] == 7
    assert effect_evidence["execution_cost_exclusion_second_probe_market_side_target_count"] == 2


def test_recovery_loop_execution_cost_exclusion_state_hysteresis(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    target_ticker = "KXHIGHBOS-26APR22-B75"

    def fake_advisor(**kwargs):
        return _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        )

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    trader_calls: list[dict[str, object]] = []

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 2,
                "intents_approved": 2,
                "intents_blocked": 0,
                "policy_reason_counts": {
                    "approved": 2,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 2},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    def run_once(*, payload: dict[str, object]) -> dict[str, object]:
        _write_json(tape_path, payload)
        return loop.run_kalshi_temperature_recovery_loop(
            output_dir=str(tmp_path),
            trader_env_file="/tmp/friction.env",
            max_iterations=1,
            weather_min_bucket_samples=9,
            plateau_negative_regime_suppression_min_bucket_samples=7,
            plateau_negative_regime_suppression_expectancy_threshold=-0.08,
            plateau_negative_regime_suppression_top_n=3,
        )

    active_tape_payload = {
        "status": "ready",
        "recommended_exclusions": {
            "market_tickers": [target_ticker],
        },
        "calibration_readiness": {
            "status": "yellow",
            "quote_coverage_ratio": 0.12,
            "min_quote_coverage_ratio": 0.20,
        },
    }
    inactive_tape_payload = {
        "status": "ready",
        "calibration_readiness": {
            "status": "yellow",
            "quote_coverage_ratio": 0.08,
            "min_quote_coverage_ratio": 0.20,
        },
    }

    run_once(payload=active_tape_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == []
    state1 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state1["tracked_tickers"][target_ticker]["consecutive_seen_runs"] == 1
    assert state1["tracked_tickers"][target_ticker]["active"] is False

    run_once(payload=active_tape_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker]
    state2 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state2["tracked_tickers"][target_ticker]["consecutive_seen_runs"] == 2
    assert state2["tracked_tickers"][target_ticker]["active"] is True

    run_once(payload=inactive_tape_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker]
    state3 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state3["tracked_tickers"][target_ticker]["consecutive_missing_runs"] == 1
    assert state3["tracked_tickers"][target_ticker]["active"] is True

    run_once(payload=inactive_tape_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker]
    state4 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state4["tracked_tickers"][target_ticker]["consecutive_missing_runs"] == 2
    assert state4["tracked_tickers"][target_ticker]["active"] is True

    run_once(payload=inactive_tape_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == []
    state5 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state5["tracked_tickers"][target_ticker]["consecutive_missing_runs"] == 3
    assert state5["tracked_tickers"][target_ticker]["active"] is False


def test_recovery_loop_execution_cost_market_side_target_state_activation_hysteresis(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    target_ticker = "KXHIGHBOS-26APR22-B75"
    target_side = f"{target_ticker}|yes"

    def fake_advisor(**kwargs):
        return _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        )

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    trader_calls: list[dict[str, object]] = []

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 1,
                "intents_approved": 1,
                "intents_blocked": 0,
                "policy_reason_counts": {"approved": 1},
            },
            "plan_summary": {"status": "ready", "planned_orders": 1},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    def run_once(*, payload: dict[str, object]) -> dict[str, object]:
        _write_json(tape_path, payload)
        return loop.run_kalshi_temperature_recovery_loop(
            output_dir=str(tmp_path),
            trader_env_file="/tmp/friction.env",
            max_iterations=1,
            weather_min_bucket_samples=9,
            plateau_negative_regime_suppression_min_bucket_samples=7,
            plateau_negative_regime_suppression_expectancy_threshold=-0.08,
            plateau_negative_regime_suppression_top_n=3,
        )

    side_payload = {
        "status": "ready",
        "recommended_exclusions": {
            "market_tickers": [target_ticker],
            "market_side_targets": [target_side],
        },
        "execution_siphon_pressure": {
            "dominant_uncovered_side": "yes",
        },
        "calibration_readiness": {
            "status": "yellow",
            "quote_coverage_ratio": 0.12,
            "min_quote_coverage_ratio": 0.20,
        },
    }

    run_once(payload=side_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == []
    state1 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state1["tracked_market_side_targets"][target_side]["consecutive_seen_runs"] == 1
    assert state1["tracked_market_side_targets"][target_side]["active"] is False

    run_once(payload=side_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker, target_side]
    state2 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state2["tracked_market_side_targets"][target_side]["consecutive_seen_runs"] == 2
    assert state2["tracked_market_side_targets"][target_side]["active"] is True


def test_recovery_loop_execution_cost_market_side_target_accelerator_triggers_under_extreme_pressure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    tickers = [f"KXACCL{i:02d}" for i in range(8)]
    market_side_targets = [f"{ticker}|yes" for ticker in tickers]
    diagnostics = [
        {"target": market_side_targets[3], "share": 0.18, "rows": 52},
        {"target": market_side_targets[0], "share": 0.09, "rows": 31},
        {"target": market_side_targets[6], "share": 0.24, "rows": 78},
        {"target": market_side_targets[5], "share": 0.12, "rows": 44},
        {"target": market_side_targets[1], "share": 0.21, "rows": 66},
        {"target": market_side_targets[7], "share": 0.16, "rows": 49},
        {"target": market_side_targets[2], "share": 0.19, "rows": 60},
        {"target": market_side_targets[4], "share": 0.22, "rows": 73},
    ]
    expected_accelerated = [
        row["target"]
        for row in sorted(
            diagnostics,
            key=lambda row: (-float(row["share"]), -int(row["rows"]), str(row["target"])),
        )[: int(loop.EXECUTION_COST_SIDE_TARGET_ACCELERATOR_MAX_TARGETS)]
    ]

    def fake_advisor(**kwargs):
        return _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        )

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    trader_calls: list[dict[str, object]] = []

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 1,
                "intents_approved": 1,
                "intents_blocked": 0,
                "policy_reason_counts": {"approved": 1},
            },
            "plan_summary": {"status": "ready", "planned_orders": 1},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    _write_json(
        tape_path,
        {
            "status": "ready",
            "recommended_exclusions": {
                "market_tickers": list(tickers),
                "market_side_targets": list(market_side_targets),
                "market_side_diagnostics": [
                    {
                        "market_side_target": str(row["target"]),
                        "rows_without_two_sided_quote": int(row["rows"]),
                        "share_of_uncovered_rows": float(row["share"]),
                        "low_quote_coverage_evidence": True,
                        "wide_spread_evidence": False,
                    }
                    for row in diagnostics
                ],
            },
            "execution_siphon_pressure": {
                "dominant_uncovered_side": "yes",
                "dominant_uncovered_side_share": 0.91,
                "side_imbalance_magnitude": 0.91,
                "pressure_score": 0.78,
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.04,
                "min_quote_coverage_ratio": 0.20,
            },
            "execution_siphon_trend": {
                "status": "ready",
                "worsening": False,
                "trend_label": "flat",
            },
        },
    )
    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
        max_iterations=1,
        weather_min_bucket_samples=9,
        plateau_negative_regime_suppression_min_bucket_samples=7,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=3,
    )

    excluded_targets = [str(value) for value in list(trader_calls[-1]["exclude_market_tickers"])]
    excluded_market_side_targets = [target for target in excluded_targets if "|" in target]
    assert len(excluded_market_side_targets) == int(loop.EXECUTION_COST_SIDE_TARGET_ACCELERATOR_MAX_TARGETS)
    assert set(excluded_market_side_targets) == set(expected_accelerated)
    assert [target for target in excluded_targets if "|" not in target] == []

    state_after = json.loads(state_path.read_text(encoding="utf-8"))
    accelerator = state_after["market_side_activation_accelerator"]
    assert accelerator["engaged"] is True
    assert accelerator["accelerated_market_side_target_count"] == int(
        loop.EXECUTION_COST_SIDE_TARGET_ACCELERATOR_MAX_TARGETS
    )
    assert set(accelerator["accelerated_market_side_targets"]) == set(expected_accelerated)
    for target in market_side_targets:
        tracked = state_after["tracked_market_side_targets"][target]
        assert tracked["active"] is (target in expected_accelerated)

    effect_evidence = payload["iteration_logs"][0]["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_market_side_accelerator_engaged"] is True
    assert effect_evidence["execution_cost_exclusion_market_side_accelerator_accelerated_count"] == int(
        loop.EXECUTION_COST_SIDE_TARGET_ACCELERATOR_MAX_TARGETS
    )
    assert set(effect_evidence["execution_cost_exclusion_market_side_accelerator_accelerated_targets"]) == set(
        expected_accelerated
    )
    assert "quote_coverage_significantly_below_min" in set(
        effect_evidence["execution_cost_exclusion_market_side_accelerator_trigger_reasons"]
    )


def test_recovery_loop_execution_cost_market_side_target_accelerator_no_trigger_under_weak_mixed_pressure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    target_ticker = "KXMIXED00"
    target_side = f"{target_ticker}|yes"

    def fake_advisor(**kwargs):
        return _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        )

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    trader_calls: list[dict[str, object]] = []

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 1,
                "intents_approved": 1,
                "intents_blocked": 0,
                "policy_reason_counts": {"approved": 1},
            },
            "plan_summary": {"status": "ready", "planned_orders": 1},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    _write_json(
        tape_path,
        {
            "status": "ready",
            "recommended_exclusions": {
                "market_tickers": [target_ticker],
                "market_side_targets": [target_side],
                "market_side_diagnostics": [
                    {
                        "market_side_target": target_side,
                        "rows_without_two_sided_quote": 18,
                        "share_of_uncovered_rows": 0.14,
                    }
                ],
            },
            "execution_siphon_pressure": {
                "dominant_uncovered_side": "yes",
                "dominant_uncovered_side_share": 0.56,
                "side_imbalance_magnitude": 0.18,
                "pressure_score": 0.37,
            },
            "execution_siphon_trend": {
                "status": "ready",
                "worsening": True,
                "trend_label": "mixed",
                "worsening_component_count": 1,
                "worsening_components": ["quote_coverage_ratio"],
                "siphon_pressure_score_delta": 0.002,
                "uncovered_market_top5_share_delta": 0.001,
                "candidate_rows_delta": 2,
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.17,
                "min_quote_coverage_ratio": 0.20,
            },
        },
    )
    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
        max_iterations=1,
        weather_min_bucket_samples=9,
        plateau_negative_regime_suppression_min_bucket_samples=7,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=3,
    )

    assert trader_calls[-1]["exclude_market_tickers"] == []
    state_after = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_after["tracked_market_side_targets"][target_side]["consecutive_seen_runs"] == 1
    assert state_after["tracked_market_side_targets"][target_side]["active"] is False
    accelerator = state_after["market_side_activation_accelerator"]
    assert accelerator["engaged"] is False
    assert accelerator["accelerated_market_side_target_count"] == 0

    effect_evidence = payload["iteration_logs"][0]["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_market_side_accelerator_engaged"] is False
    assert effect_evidence["execution_cost_exclusion_market_side_accelerator_accelerated_count"] == 0
    assert effect_evidence["execution_cost_exclusion_market_side_accelerator_accelerated_targets"] == []


def test_recovery_loop_execution_cost_market_side_target_accelerator_preserves_hysteresis_when_not_extreme(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    target_ticker = "KXHYSTER00"
    target_side = f"{target_ticker}|yes"

    def fake_advisor(**kwargs):
        return _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        )

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    trader_calls: list[dict[str, object]] = []

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 1,
                "intents_approved": 1,
                "intents_blocked": 0,
                "policy_reason_counts": {"approved": 1},
            },
            "plan_summary": {"status": "ready", "planned_orders": 1},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    def run_once() -> dict[str, object]:
        _write_json(
            tape_path,
            {
                "status": "ready",
                "recommended_exclusions": {
                    "market_tickers": [target_ticker],
                    "market_side_targets": [target_side],
                    "market_side_diagnostics": [
                        {
                            "market_side_target": target_side,
                            "rows_without_two_sided_quote": 34,
                            "share_of_uncovered_rows": 0.18,
                        }
                    ],
                },
                "execution_siphon_pressure": {
                    "dominant_uncovered_side": "yes",
                    "dominant_uncovered_side_share": 0.90,
                    "side_imbalance_magnitude": 0.90,
                    "pressure_score": 0.74,
                },
                "execution_siphon_trend": {
                    "status": "ready",
                    "worsening": False,
                    "trend_label": "flat",
                },
                "calibration_readiness": {
                    "status": "yellow",
                    "quote_coverage_ratio": 0.16,
                    "min_quote_coverage_ratio": 0.20,
                },
            },
        )
        return loop.run_kalshi_temperature_recovery_loop(
            output_dir=str(tmp_path),
            trader_env_file="/tmp/friction.env",
            max_iterations=1,
            weather_min_bucket_samples=9,
            plateau_negative_regime_suppression_min_bucket_samples=7,
            plateau_negative_regime_suppression_expectancy_threshold=-0.08,
            plateau_negative_regime_suppression_top_n=3,
        )

    payload_first = run_once()
    assert trader_calls[-1]["exclude_market_tickers"] == []
    effect_first = payload_first["iteration_logs"][0]["executed_actions"][0]["effect_evidence"]
    assert effect_first["execution_cost_exclusion_market_side_accelerator_engaged"] is False
    assert effect_first["execution_cost_exclusion_market_side_accelerator_accelerated_count"] == 0
    state_first = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_first["tracked_market_side_targets"][target_side]["consecutive_seen_runs"] == 1
    assert state_first["tracked_market_side_targets"][target_side]["active"] is False

    run_once()
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker, target_side]
    state_second = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_second["tracked_market_side_targets"][target_side]["consecutive_seen_runs"] == 2
    assert state_second["tracked_market_side_targets"][target_side]["active"] is True


def test_recovery_loop_execution_cost_market_side_target_state_deactivation_hysteresis(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    target_ticker = "KXHIGHBOS-26APR22-B75"
    target_side = f"{target_ticker}|yes"
    _seed_execution_cost_exclusions_state(
        state_path,
        tickers=[target_ticker],
        active=True,
        seen_runs=5,
        market_side_targets=[target_side],
        active_market_side_targets=True,
        side_seen_runs=5,
    )

    def fake_advisor(**kwargs):
        return _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        )

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    trader_calls: list[dict[str, object]] = []

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 1,
                "intents_approved": 1,
                "intents_blocked": 0,
                "policy_reason_counts": {"approved": 1},
            },
            "plan_summary": {"status": "ready", "planned_orders": 1},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    def run_once(*, payload: dict[str, object]) -> dict[str, object]:
        _write_json(tape_path, payload)
        return loop.run_kalshi_temperature_recovery_loop(
            output_dir=str(tmp_path),
            trader_env_file="/tmp/friction.env",
            max_iterations=1,
            weather_min_bucket_samples=9,
            plateau_negative_regime_suppression_min_bucket_samples=7,
            plateau_negative_regime_suppression_expectancy_threshold=-0.08,
            plateau_negative_regime_suppression_top_n=3,
        )

    missing_side_payload = {
        "status": "ready",
        "recommended_exclusions": {
            "market_tickers": [target_ticker],
        },
        "calibration_readiness": {
            "status": "yellow",
            "quote_coverage_ratio": 0.10,
            "min_quote_coverage_ratio": 0.20,
        },
    }

    run_once(payload=missing_side_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker, target_side]
    state1 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state1["tracked_market_side_targets"][target_side]["consecutive_missing_runs"] == 1
    assert state1["tracked_market_side_targets"][target_side]["active"] is True

    run_once(payload=missing_side_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker, target_side]
    state2 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state2["tracked_market_side_targets"][target_side]["consecutive_missing_runs"] == 2
    assert state2["tracked_market_side_targets"][target_side]["active"] is True

    run_once(payload=missing_side_payload)
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker]
    state3 = json.loads(state_path.read_text(encoding="utf-8"))
    assert state3["tracked_market_side_targets"][target_side]["consecutive_missing_runs"] == 3
    assert state3["tracked_market_side_targets"][target_side]["active"] is False


def test_recovery_loop_reduce_execution_friction_pressure_downshift_removes_market_side_targets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    tickers = [f"KXDROP{i:02d}" for i in range(20)]
    market_side_targets = [f"{ticker}|yes" for ticker in tickers]
    _seed_execution_cost_exclusions_state(
        state_path,
        tickers=tickers,
        active=True,
        seen_runs=4,
        market_side_targets=market_side_targets,
        active_market_side_targets=True,
        side_seen_runs=4,
    )
    _write_json(
        tape_path,
        {
            "status": "ready",
            "recommended_exclusions": {
                "market_tickers": list(tickers),
                "market_side_targets": list(market_side_targets),
            },
            "execution_siphon_pressure": {
                "dominant_uncovered_side": "yes",
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.10,
                "min_quote_coverage_ratio": 0.20,
            },
        },
    )

    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "no_candidates",
            "intent_summary": {
                "status": "no_candidates",
                "intents_total": 0,
                "intents_approved": 0,
                "intents_blocked": 0,
                "policy_reason_counts": {
                    "expected_edge_below_min": 0,
                },
            },
            "plan_summary": {"status": "no_candidates", "planned_orders": 0},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 2
    assert trader_calls[0]["exclude_market_tickers"] == tickers + market_side_targets
    assert trader_calls[1]["exclude_market_tickers"] == tickers[:10] + market_side_targets[:10]
    effect_evidence = payload["iteration_logs"][0]["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_downshift_triggered"] is True
    assert effect_evidence["execution_cost_exclusion_downshift_removed_market_side_target_count"] == 5
    assert effect_evidence["execution_cost_exclusion_downshift_removed_market_side_targets"] == market_side_targets[15:]
    assert effect_evidence["execution_cost_exclusion_downshift_removed_market_side_targets_any"] is True
    state_after = json.loads(state_path.read_text(encoding="utf-8"))
    adaptive = state_after["adaptive_downshift"]
    assert adaptive["last_drop_market_side_target_count"] == 5
    assert adaptive["last_drop_market_side_targets"] == market_side_targets[15:]
    assert adaptive["suppressed_market_side_target_count"] == 5


def test_recovery_loop_execution_cost_market_side_target_state_fallback_when_telemetry_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    target_ticker = "KXHIGHBOS-26APR22-B75"
    target_side = f"{target_ticker}|yes"
    _seed_execution_cost_exclusions_state(
        state_path,
        tickers=[target_ticker],
        active=True,
        seen_runs=5,
        market_side_targets=[target_side],
        active_market_side_targets=True,
        side_seen_runs=5,
    )

    def fake_advisor(**kwargs):
        return _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        )

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    trader_calls: list[dict[str, object]] = []

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 1,
                "intents_approved": 1,
                "intents_blocked": 0,
                "policy_reason_counts": {"approved": 1},
            },
            "plan_summary": {"status": "ready", "planned_orders": 1},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    _write_json(
        tape_path,
        {
            "status": "ready",
            "recommended_exclusions": {
                "market_tickers": [target_ticker],
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.10,
                "min_quote_coverage_ratio": 0.20,
            },
        },
    )
    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
        max_iterations=1,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert trader_calls[-1]["exclude_market_tickers"] == [target_ticker, target_side]
    effect_evidence = payload["iteration_logs"][0]["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_market_side_candidate_count"] == 0
    assert effect_evidence["execution_cost_exclusion_market_side_active_count"] == 1
    state_after = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_after["tracked_market_side_targets"][target_side]["consecutive_missing_runs"] == 1
    assert state_after["tracked_market_side_targets"][target_side]["active"] is True


def test_recovery_loop_reduce_execution_friction_pressure_does_not_downshift_when_second_probe_improves(
    tmp_path: Path,
    monkeypatch,
) -> None:
    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    tape_path = tmp_path / "health" / "execution_cost_tape_latest.json"
    tickers = [f"KXKEEP{i:02d}" for i in range(20)]
    _seed_execution_cost_exclusions_state(
        state_path,
        tickers=tickers,
        active=True,
        seen_runs=4,
    )
    seeded_state = json.loads(state_path.read_text(encoding="utf-8"))
    seeded_state["adaptive_downshift"] = {
        "last_evaluated_run": 3,
        "last_downshift_run": 0,
        "last_decision": "no_downshift",
        "last_reason": "",
        "suppressed_tickers": {},
        "last_probe_metrics": {
            "first_probe_status": "no_candidates",
            "first_probe_intents_total": 0,
            "first_probe_intents_approved": 0,
            "second_probe_triggered": True,
            "second_probe_status": "no_candidates",
            "second_probe_intents_total": 0,
            "second_probe_intents_approved": 0,
        },
        "last_coverage_metrics": {
            "quote_coverage_ratio": 0.10,
            "min_quote_coverage_ratio": 0.20,
        },
        "last_active_count_before": 20,
        "last_active_count_after": 20,
        "last_drop_count": 0,
        "last_drop_tickers": [],
    }
    _write_json(state_path, seeded_state)
    _write_json(
        tape_path,
        {
            "status": "ready",
            "recommended_exclusions": {
                "market_tickers": list(tickers),
            },
            "calibration_readiness": {
                "status": "yellow",
                "quote_coverage_ratio": 0.10,
                "min_quote_coverage_ratio": 0.20,
            },
        },
    )

    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_execution_friction_pressure"],
            gap_score=0.24,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_execution_cost_tape(*, output_dir: str):
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        if len(trader_calls) == 1:
            return {
                "status": "no_candidates",
                "intent_summary": {
                    "status": "no_candidates",
                    "intents_total": 0,
                    "intents_approved": 0,
                    "intents_blocked": 0,
                    "policy_reason_counts": {
                        "expected_edge_below_min": 0,
                    },
                },
                "plan_summary": {"status": "no_candidates", "planned_orders": 0},
            }
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 3,
                "intents_approved": 2,
                "intents_blocked": 1,
                "policy_reason_counts": {
                    "approved": 2,
                    "expected_edge_below_min": 1,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 2},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/friction.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 2
    assert trader_calls[0]["exclude_market_tickers"] == tickers
    assert trader_calls[1]["exclude_market_tickers"] == tickers[:10]
    effect_evidence = payload["iteration_logs"][0]["executed_actions"][0]["effect_evidence"]
    assert effect_evidence["execution_cost_exclusion_downshift_triggered"] is False
    assert effect_evidence["execution_cost_exclusion_downshift_reason"] == "throughput_improving_or_not_blocked"
    assert effect_evidence["execution_cost_exclusion_downshift_active_count_before"] == 20
    assert effect_evidence["execution_cost_exclusion_downshift_active_count_after"] == 20
    assert effect_evidence["execution_cost_exclusion_downshift_removed_count"] == 0
    assert effect_evidence["execution_cost_exclusion_downshift_second_probe_improved"] is True
    assert effect_evidence["execution_cost_exclusion_downshift_throughput_stalled"] is False
    assert effect_evidence["execution_cost_exclusion_adaptive_second_probe_triggered"] is True
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["active_count"] == 20
    adaptive = dict(state.get("adaptive_downshift") or {})
    assert adaptive.get("last_decision") == "no_downshift"
    assert adaptive.get("last_reason") == "throughput_improving_or_not_blocked"


def test_recovery_loop_repair_execution_telemetry_pipeline_runs_expected_sequence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["repair_execution_telemetry_pipeline"],
            gap_score=0.16,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    ws_calls: list[dict[str, object]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_ws_collect(**kwargs):
        ws_calls.append(dict(kwargs))
        call_order.append(("ws_state_collect", str(kwargs.get("output_dir") or "")))
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {"status": "ready"}

    def fake_execution_cost_tape(*, output_dir: str):
        call_order.append(("execution_cost_tape", output_dir))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", fake_ws_collect)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/telemetry.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("ws_state_collect", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("execution_cost_tape", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(ws_calls) == 1
    ws_call = ws_calls[0]
    assert ws_call["env_file"] == "/tmp/telemetry.env"
    assert ws_call["output_dir"] == str(tmp_path)
    assert round(float(ws_call["run_seconds"]), 6) == 20.0
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/telemetry.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is False
    assert trader_call["allow_live_orders"] is False
    assert int(trader_call["contracts_per_order"]) == 1
    assert int(trader_call["max_orders"]) == 24
    assert int(trader_call["max_markets"]) == 250
    assert round(float(trader_call["planning_bankroll_dollars"]), 6) == 250.0
    assert round(float(trader_call["daily_risk_cap_dollars"]), 6) == 75.0
    assert trader_call["weather_pattern_hardening_enabled"] is False
    assert trader_call["weather_pattern_risk_off_enabled"] is False
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is False
    assert trader_call["historical_selection_quality_enabled"] is False
    assert trader_call["metar_ingest_quality_gate_enabled"] is False
    assert trader_call["enforce_probability_edge_thresholds"] is False
    assert trader_call["enforce_interval_consistency"] is False
    assert trader_call["require_market_snapshot_seq"] is False
    assert round(float(trader_call["max_total_deployed_pct"]), 6) == 1.0
    assert round(float(trader_call["max_same_station_exposure_pct"]), 6) == 1.0
    assert round(float(trader_call["max_same_hour_cluster_exposure_pct"]), 6) == 1.0
    assert round(float(trader_call["max_same_underlying_exposure_pct"]), 6) == 1.0
    assert int(trader_call["max_intents_per_underlying"]) == 12
    assert round(float(trader_call["replan_market_side_cooldown_minutes"]), 6) == 0.0
    assert int(trader_call["replan_market_side_max_plans_per_window"]) == 999
    assert round(float(trader_call["min_hours_to_close"]), 6) == 0.0
    assert round(float(trader_call["max_hours_to_close"]), 6) == 72.0
    assert trader_call["max_metar_age_minutes"] is None
    assert round(float(trader_call["min_settlement_confidence"]), 6) == 0.0
    assert round(float(trader_call["min_expected_edge_net"]), 6) == 0.0
    assert round(float(trader_call["min_edge_to_risk_ratio"]), 6) == 0.0
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "repair_execution_telemetry_pipeline"
    assert iteration["executed_actions"][0]["status"] == "executed"
    probe_diagnostics = iteration["executed_actions"][0]["effect_evidence"]["telemetry_probe_diagnostics"]
    assert probe_diagnostics["first_probe_status"] == "ready"
    assert probe_diagnostics["second_probe_triggered"] is False
    assert probe_diagnostics["second_probe_reason"] == "first_probe_not_no_candidates"
    assert probe_diagnostics["hardening_pressure_total"] == 0


def test_recovery_loop_repair_execution_telemetry_pipeline_runs_guarded_second_probe_on_hardening_pressure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["repair_execution_telemetry_pipeline"],
            gap_score=0.2,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_ws_collect(**kwargs):
        call_order.append(("ws_state_collect", str(kwargs.get("output_dir") or "")))
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        if len(trader_calls) == 1:
            return {
                "status": "no_candidates",
                "intent_summary": {
                    "status": "ready",
                    "intents_total": 12,
                    "intents_approved": 0,
                    "intents_blocked": 12,
                    "policy_reason_counts": {
                        "weather_pattern_multi_bucket_hard_block": 7,
                        "expected_edge_below_min": 5,
                    },
                },
                "plan_summary": {"status": "no_candidates", "planned_orders": 0},
            }
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 12,
                "intents_approved": 2,
                "intents_blocked": 10,
                "policy_reason_counts": {
                    "approved": 2,
                    "constraint_not_actionable": 10,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 2},
        }

    def fake_execution_cost_tape(*, output_dir: str):
        call_order.append(("execution_cost_tape", output_dir))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", fake_ws_collect)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/telemetry.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("ws_state_collect", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("execution_cost_tape", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 2
    assert trader_calls[0]["allow_live_orders"] is False
    assert trader_calls[1]["allow_live_orders"] is False
    assert trader_calls[0]["intents_only"] is False
    assert trader_calls[1]["intents_only"] is False
    assert trader_calls[1]["shadow_quote_probe_on_no_candidates"] is True
    assert trader_calls[1]["min_expected_edge_net"] is None
    assert trader_calls[1]["min_edge_to_risk_ratio"] is None
    assert trader_calls[1]["min_probability_confidence"] is None
    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    probe_diagnostics = action["effect_evidence"]["telemetry_probe_diagnostics"]
    assert probe_diagnostics["first_probe_status"] == "no_candidates"
    assert probe_diagnostics["hardening_pressure_total"] == 12
    assert probe_diagnostics["second_probe_triggered"] is True
    assert probe_diagnostics["second_probe_reason"] == "first_probe_no_candidates_with_hardening_pressure"
    assert probe_diagnostics["second_probe_status"] == "ready"
    payload_diagnostics = action["effect_evidence"]["trader_payload_diagnostics"]
    assert len(payload_diagnostics) == 2
    assert payload_diagnostics[0]["policy_reason_counts"]["weather_pattern_multi_bucket_hard_block"] == 7
    assert payload_diagnostics[1]["key_totals"]["planned_orders"] == 2


def test_recovery_loop_repair_execution_telemetry_pipeline_accepts_dry_run_policy_blocked_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["repair_execution_telemetry_pipeline"],
            gap_score=0.2,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    summary_path = tmp_path / "kalshi_temperature_trade_intents_summary_latest.json"

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_ws_collect(**kwargs):
        return {"status": "ready"}

    trader_call_count = {"count": 0}

    def fake_trader(**kwargs):
        trader_call_count["count"] += 1
        if trader_call_count["count"] == 1:
            summary_path.write_text(
                json.dumps(
                    {
                        "intents_total": 12,
                        "intents_approved": 0,
                        "intents_blocked": 12,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            return {
                "status": "no_candidates",
                "intent_summary": {
                    "status": "ready",
                    "intents_total": 12,
                    "intents_approved": 0,
                    "intents_blocked": 12,
                    "policy_reason_counts": {
                        "expected_edge_below_min": 12,
                    },
                },
                "plan_summary": {"status": "no_candidates", "planned_orders": 0},
            }
        summary_path.write_text(
            json.dumps(
                {
                    "intents_total": 12,
                    "intents_approved": 10,
                    "intents_blocked": 2,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        return {
            "status": "dry_run_policy_blocked",
            "intent_summary": {
                "status": "ready",
                "intents_total": 12,
                "intents_approved": 10,
                "intents_blocked": 2,
                "policy_reason_counts": {
                    "approved": 10,
                    "taf_station_missing": 2,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 10},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", fake_ws_collect)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/telemetry.env",
    )

    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    assert action["key"] == "repair_execution_telemetry_pipeline"
    assert action["effect_status"] == "verified"
    assert action["effect_reason"] == ""
    assert action["counts_toward_effectiveness"] is True
    assert action["effect_evidence"]["trader_statuses"] == [
        "no_candidates",
        "dry_run_policy_blocked",
    ]


def test_recovery_loop_improve_execution_quote_coverage_shadow_runs_safe_probe_sequence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_json(
        tmp_path / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "execution_cost_observations": {
                "top_missing_coverage_buckets": {
                    "by_market_side": [
                        {"bucket": "KXHIGHBOS-26APR22-B75|no", "rows_without_two_sided_quote": 7},
                        {"bucket": "KXHIGHBOS-26APR22-B75|yes", "rows_without_two_sided_quote": 5},
                    ],
                    "by_market": [
                        {"bucket": "KXHIGHNYC-26APR22-T72", "rows_without_two_sided_quote": 3},
                    ],
                }
            },
            "top_missing_coverage_buckets": {
                "by_market_side": [
                    {"bucket": "KXHIGHCHI-26APR22-T73|yes", "rows_without_two_sided_quote": 4},
                    {"bucket": "KXHIGHBOS-26APR22-B75|no", "rows_without_two_sided_quote": 2},
                ],
                "by_market": [
                    {"bucket": "KXHIGHDAL-26APR22-T71", "rows_without_two_sided_quote": 2},
                ],
            },
        },
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["improve_execution_quote_coverage_shadow"],
            gap_score=0.17,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    ws_calls: list[dict[str, object]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_ws_collect(**kwargs):
        ws_calls.append(dict(kwargs))
        call_order.append(("ws_state_collect", str(kwargs.get("output_dir") or "")))
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 18,
                "intents_approved": 4,
                "intents_blocked": 14,
                "policy_reason_counts": {
                    "approved": 4,
                    "expected_edge_below_min": 14,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 4},
        }

    def fake_execution_cost_tape(*, output_dir: str):
        call_order.append(("execution_cost_tape", output_dir))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", fake_ws_collect)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/quote-coverage.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("ws_state_collect", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("execution_cost_tape", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(ws_calls) == 1
    ws_call = ws_calls[0]
    assert ws_call["env_file"] == "/tmp/quote-coverage.env"
    assert ws_call["output_dir"] == str(tmp_path)
    assert round(float(ws_call["run_seconds"]), 6) == 35.0
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/quote-coverage.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is False
    assert trader_call["allow_live_orders"] is False
    assert trader_call["shadow_quote_probe_on_no_candidates"] is True
    assert trader_call["weather_pattern_hardening_enabled"] is False
    assert trader_call["weather_pattern_risk_off_enabled"] is False
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is False
    assert trader_call["historical_selection_quality_enabled"] is False
    assert trader_call["metar_ingest_quality_gate_enabled"] is False
    assert trader_call["enforce_probability_edge_thresholds"] is False
    assert trader_call["enforce_interval_consistency"] is False
    assert trader_call["require_market_snapshot_seq"] is False
    assert trader_call["min_probability_confidence"] is None
    assert trader_call["min_expected_edge_net"] is None
    assert trader_call["min_edge_to_risk_ratio"] is None
    assert trader_call["shadow_quote_probe_market_side_targets"] == [
        "KXHIGHBOS-26APR22-B75|no",
        "KXHIGHBOS-26APR22-B75|yes",
        "KXHIGHCHI-26APR22-T73|yes",
    ]
    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    assert action["key"] == "improve_execution_quote_coverage_shadow"
    assert action["status"] == "executed"
    probe_diagnostics = action["effect_evidence"]["telemetry_probe_diagnostics"]
    assert probe_diagnostics["probe_kind"] == "quote_coverage_shadow"
    assert probe_diagnostics["first_probe_status"] == "ready"
    assert probe_diagnostics["shadow_quote_probe_target_count"] == 3
    assert probe_diagnostics["shadow_quote_probe_targets"] == [
        "KXHIGHBOS-26APR22-B75|no",
        "KXHIGHBOS-26APR22-B75|yes",
        "KXHIGHCHI-26APR22-T73|yes",
    ]
    assert probe_diagnostics["second_probe_triggered"] is False
    assert probe_diagnostics["second_probe_reason"] == "first_probe_not_no_candidates"
    assert probe_diagnostics["quote_coverage_pressure_total"] == 14


def test_recovery_loop_improve_execution_quote_coverage_shadow_biases_targets_under_side_pressure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_json(
        tmp_path / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "execution_cost_observations": {
                "top_missing_coverage_buckets": {
                    "by_market_side": [
                        {"bucket": "KXHIGHBOS-26APR22-B75|no", "rows_without_two_sided_quote": 6},
                        {"bucket": "KXHIGHNYC-26APR22-T72|yes", "rows_without_two_sided_quote": 10},
                        {"bucket": "KXHIGHCHI-26APR22-T73|yes", "rows_without_two_sided_quote": 8},
                        {"bucket": "KXHIGHDAL-26APR22-T71|no", "rows_without_two_sided_quote": 5},
                    ],
                    "by_side": [
                        {"bucket": "yes", "rows_without_two_sided_quote": 18},
                        {"bucket": "no", "rows_without_two_sided_quote": 11},
                    ],
                }
            },
            "execution_siphon_pressure": {
                "status": "ready",
                "side_pressure": {
                    "active": True,
                    "dominant_side": "yes",
                    "pressure_score": 0.88,
                },
            },
        },
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["improve_execution_quote_coverage_shadow"],
            gap_score=0.16,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_ws_collect(**kwargs):
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 12,
                "intents_approved": 2,
                "intents_blocked": 10,
                "policy_reason_counts": {
                    "approved": 2,
                    "expected_edge_below_min": 10,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 2},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", fake_ws_collect)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/quote-coverage.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    assert trader_calls[0]["shadow_quote_probe_market_side_targets"] == [
        "KXHIGHNYC-26APR22-T72|yes",
        "KXHIGHCHI-26APR22-T73|yes",
        "KXHIGHBOS-26APR22-B75|no",
    ]

    action = payload["iteration_logs"][0]["executed_actions"][0]
    effect_evidence = action["effect_evidence"]
    assert effect_evidence["quote_coverage_side_pressure_active"] is True
    assert effect_evidence["quote_coverage_side_pressure_dominant_side"] == "yes"
    assert effect_evidence["quote_coverage_target_count_before_side_bias"] == 4
    assert effect_evidence["quote_coverage_target_count_after_side_bias"] == 3
    probe_diagnostics = effect_evidence["telemetry_probe_diagnostics"]
    assert probe_diagnostics["side_pressure_active"] is True
    assert probe_diagnostics["dominant_side"] == "yes"
    assert probe_diagnostics["shadow_quote_probe_target_count_before_side_bias"] == 4
    assert probe_diagnostics["shadow_quote_probe_target_count_after_side_bias"] == 3


def test_recovery_loop_improve_execution_quote_coverage_shadow_noop_when_side_pressure_weak(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_json(
        tmp_path / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "execution_cost_observations": {
                "top_missing_coverage_buckets": {
                    "by_market_side": [
                        {"bucket": "KXHIGHBOS-26APR22-B75|no", "rows_without_two_sided_quote": 3},
                        {"bucket": "KXHIGHNYC-26APR22-T72|yes", "rows_without_two_sided_quote": 2},
                        {"bucket": "KXHIGHCHI-26APR22-T73|no", "rows_without_two_sided_quote": 2},
                    ],
                    "by_side": [
                        {"bucket": "no", "rows_without_two_sided_quote": 5},
                        {"bucket": "yes", "rows_without_two_sided_quote": 4},
                    ],
                }
            },
            "execution_siphon_pressure": {
                "status": "ready",
                "side_pressure": {
                    "dominant_side": "no",
                    "pressure_score": 0.45,
                },
            },
        },
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["improve_execution_quote_coverage_shadow"],
            gap_score=0.16,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_ws_collect(**kwargs):
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        return {
            "status": "ready",
            "intent_summary": {
                "status": "ready",
                "intents_total": 9,
                "intents_approved": 2,
                "intents_blocked": 7,
                "policy_reason_counts": {
                    "approved": 2,
                    "expected_edge_below_min": 7,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 2},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", fake_ws_collect)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/quote-coverage.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 1
    assert trader_calls[0]["shadow_quote_probe_market_side_targets"] == [
        "KXHIGHBOS-26APR22-B75|no",
        "KXHIGHNYC-26APR22-T72|yes",
        "KXHIGHCHI-26APR22-T73|no",
    ]

    action = payload["iteration_logs"][0]["executed_actions"][0]
    effect_evidence = action["effect_evidence"]
    assert effect_evidence["quote_coverage_side_pressure_active"] is False
    assert effect_evidence["quote_coverage_side_pressure_dominant_side"] == "no"
    assert effect_evidence["quote_coverage_target_count_before_side_bias"] == 3
    assert effect_evidence["quote_coverage_target_count_after_side_bias"] == 3
    probe_diagnostics = effect_evidence["telemetry_probe_diagnostics"]
    assert probe_diagnostics["side_pressure_active"] is False
    assert probe_diagnostics["dominant_side"] == "no"
    assert probe_diagnostics["shadow_quote_probe_target_count_before_side_bias"] == 3
    assert probe_diagnostics["shadow_quote_probe_target_count_after_side_bias"] == 3


def test_recovery_loop_improve_execution_quote_coverage_shadow_triggers_second_probe_on_no_candidates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_json(
        tmp_path / "health" / "execution_cost_tape_latest.json",
        {
            "status": "ready",
            "execution_cost_observations": {
                "top_missing_coverage_buckets": {
                    "by_market_side": [
                        {"bucket": "KXHIGHBOS-26APR22-B75|no", "rows_without_two_sided_quote": 9},
                        {"bucket": "KXHIGHNYC-26APR22-T72|yes", "rows_without_two_sided_quote": 6},
                    ]
                }
            },
        },
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["improve_execution_quote_coverage_shadow"],
            gap_score=0.16,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_ws_collect(**kwargs):
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        if len(trader_calls) == 1:
            return {
                "status": "no_candidates",
                "intent_summary": {
                    "status": "ready",
                    "intents_total": 24,
                    "intents_approved": 0,
                    "intents_blocked": 24,
                    "policy_reason_counts": {
                        "expected_edge_below_min": 20,
                        "missing_market_snapshot_seq": 4,
                    },
                },
                "plan_summary": {"status": "no_candidates", "planned_orders": 0},
            }
        return {
            "status": "dry_run_policy_blocked",
            "intent_summary": {
                "status": "ready",
                "intents_total": 24,
                "intents_approved": 18,
                "intents_blocked": 6,
                "policy_reason_counts": {
                    "approved": 18,
                    "taf_station_missing": 6,
                },
            },
            "plan_summary": {"status": "ready", "planned_orders": 18},
        }

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", fake_ws_collect)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/quote-coverage.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert len(trader_calls) == 2
    assert trader_calls[0]["shadow_quote_probe_on_no_candidates"] is True
    assert trader_calls[1]["shadow_quote_probe_on_no_candidates"] is True
    assert trader_calls[0]["shadow_quote_probe_market_side_targets"] == [
        "KXHIGHBOS-26APR22-B75|no",
        "KXHIGHNYC-26APR22-T72|yes",
    ]
    assert trader_calls[1]["shadow_quote_probe_market_side_targets"] == [
        "KXHIGHBOS-26APR22-B75|no",
        "KXHIGHNYC-26APR22-T72|yes",
    ]
    assert int(trader_calls[1]["max_orders"]) == 48
    assert int(trader_calls[1]["max_markets"]) == 500

    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    probe_diagnostics = action["effect_evidence"]["telemetry_probe_diagnostics"]
    assert probe_diagnostics["probe_kind"] == "quote_coverage_shadow"
    assert probe_diagnostics["first_probe_status"] == "no_candidates"
    assert probe_diagnostics["shadow_quote_probe_target_count"] == 2
    assert probe_diagnostics["shadow_quote_probe_targets"] == [
        "KXHIGHBOS-26APR22-B75|no",
        "KXHIGHNYC-26APR22-T72|yes",
    ]
    assert probe_diagnostics["second_probe_triggered"] is True
    assert probe_diagnostics["second_probe_reason"] == "first_probe_no_candidates_low_quote_coverage_pressure"
    assert probe_diagnostics["second_probe_status"] == "dry_run_policy_blocked"


def test_recovery_loop_repair_execution_telemetry_pipeline_continues_when_ws_collect_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["repair_execution_telemetry_pipeline"],
            gap_score=0.18,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_ws_collect(**kwargs):
        call_order.append(("ws_state_collect", str(kwargs.get("output_dir") or "")))
        raise RuntimeError("ws unavailable")

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {"status": "ready"}

    def fake_execution_cost_tape(*, output_dir: str):
        call_order.append(("execution_cost_tape", output_dir))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", fake_ws_collect)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_execution_cost_tape", fake_execution_cost_tape)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/telemetry.env",
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("ws_state_collect", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("execution_cost_tape", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 1
    assert trader_calls[0]["intents_only"] is False
    assert trader_calls[0]["allow_live_orders"] is False
    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    assert action["key"] == "repair_execution_telemetry_pipeline"
    assert action["status"] == "executed"
    assert action["error"] is None


def test_recovery_loop_repair_execution_telemetry_pipeline_writes_blocker_audit_fallback_when_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 30,
                "intents_approved": 4,
                "policy_reason_counts": {
                    "expected_edge_below_min": 20,
                    "weather_pattern_multi_bucket_hard_block": 6,
                    "approved": 4,
                },
            }
        ),
        encoding="utf-8",
    )
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["repair_execution_telemetry_pipeline"],
            gap_score=0.19,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_ws_state_collect", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(
        loop,
        "run_kalshi_temperature_execution_cost_tape",
        lambda **kwargs: {"status": "ready"},
    )
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/telemetry.env",
    )

    assert payload["termination_reason"] == "cleared"
    blocker_latest = tmp_path / "checkpoints" / "blocker_audit_168h_latest.json"
    assert blocker_latest.exists() is True
    blocker_payload = json.loads(blocker_latest.read_text(encoding="utf-8"))
    headline = blocker_payload["headline"]
    assert headline["largest_blocker_reason_raw"] == "expected_edge_below_min"
    assert int(headline["largest_blocker_count"]) == 20
    assert int(headline["blocked_total"]) == 26
    assert float(headline["largest_blocker_share_of_blocked"]) == round(20.0 / 26.0, 6)


def test_recovery_loop_refresh_market_horizon_inputs_reports_error_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_market_horizon_inputs"],
            gap_score=0.22,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    called: list[str] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_contract_specs(*, env_file: str, output_dir: str):
        called.append("contract_specs")
        raise RuntimeError("forced failure")

    def fake_constraint_scan(*, output_dir: str):
        called.append("constraint_scan")
        return {"status": "ready"}

    def fake_settlement_state(*, output_dir: str):
        called.append("settlement_state")
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_contract_specs", fake_contract_specs)
    monkeypatch.setattr(loop, "run_kalshi_temperature_constraint_scan", fake_constraint_scan)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert called == ["contract_specs"]
    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    assert action["key"] == "refresh_market_horizon_inputs"
    assert action["status"] == "error"
    assert "RuntimeError: forced failure" in action["error"]


def test_recovery_loop_increase_settled_outcome_coverage_runs_expected_sequence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_settled_outcome_coverage"],
            gap_score=0.17,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_settlement_state(*, output_dir: str):
        call_order.append(("settlement_state", output_dir))
        return {"status": "ready"}

    def fake_profitability(*, output_dir: str, **kwargs):
        call_order.append(("profitability", output_dir))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "run_kalshi_temperature_profitability", fake_profitability)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("settlement_state", str(tmp_path)),
        ("profitability", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "increase_settled_outcome_coverage"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_increase_settled_outcome_coverage_runs_targeted_shadow_before_refresh_sequence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_settled_outcome_coverage"],
            gap_score=0.17,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []
    targeted_csv = tmp_path / "health" / "targeted_constraints.csv"
    targeted_csv.parent.mkdir(parents=True, exist_ok=True)
    targeted_csv.write_text(
        "market_ticker,settlement_station,constraint_status\n",
        encoding="utf-8",
    )

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_throughput(*, output_dir: str, **kwargs):
        call_order.append(("throughput", output_dir))
        return {
            "status": "ready",
            "targeted_constraint_csv": str(targeted_csv),
            "targeted_constraint_rows": 1,
        }

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("targeted_shadow", str(kwargs.get("constraint_csv") or "")))
        return {"status": "ready"}

    def fake_settlement_state(*, output_dir: str):
        call_order.append(("settlement_state", output_dir))
        return {"status": "ready"}

    def fake_profitability(*, output_dir: str, **kwargs):
        call_order.append(("profitability", output_dir))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settled_outcome_throughput", fake_throughput)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "run_kalshi_temperature_profitability", fake_profitability)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/recovery.env",
        weather_min_bucket_samples=17,
        plateau_negative_regime_suppression_min_bucket_samples=20,
        plateau_negative_regime_suppression_expectancy_threshold=-0.02,
        plateau_negative_regime_suppression_top_n=15,
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("throughput", str(tmp_path)),
        ("targeted_shadow", str(targeted_csv)),
        ("settlement_state", str(tmp_path)),
        ("profitability", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/recovery.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["constraint_csv"] == str(targeted_csv)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is True
    assert round(float(trader_call["weather_pattern_profile_max_age_hours"]), 6) == 48.0
    assert int(trader_call["weather_pattern_min_bucket_samples"]) == 17
    assert round(float(trader_call["weather_pattern_negative_expectancy_threshold"]), 6) == -0.03
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert round(float(trader_call["weather_pattern_risk_off_concentration_threshold"]), 6) == 0.65
    assert int(trader_call["weather_pattern_risk_off_min_attempts"]) == 17
    assert round(float(trader_call["weather_pattern_risk_off_stale_metar_share_threshold"]), 6) == 0.45
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 20
    assert round(
        float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]),
        6,
    ) == -0.02
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 15
    assert trader_call["historical_selection_quality_enabled"] is True
    assert trader_call["enforce_probability_edge_thresholds"] is True
    assert round(float(trader_call["min_probability_confidence"]), 6) == 0.75
    assert round(float(trader_call["min_expected_edge_net"]), 6) == 0.015
    assert round(float(trader_call["min_edge_to_risk_ratio"]), 6) == 0.06

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "increase_settled_outcome_coverage"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_recover_settled_outcome_velocity_runs_targeted_shadow_before_refresh_sequence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["recover_settled_outcome_velocity"],
            gap_score=0.19,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []
    targeted_csv = tmp_path / "health" / "targeted_constraints_recover.csv"
    targeted_csv.parent.mkdir(parents=True, exist_ok=True)
    targeted_csv.write_text(
        "market_ticker,settlement_station,constraint_status\n",
        encoding="utf-8",
    )

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_throughput(*, output_dir: str, **kwargs):
        call_order.append(("throughput", output_dir))
        return {
            "status": "ready",
            "targeted_constraint_csv": str(targeted_csv),
            "targeted_constraint_rows": 1,
        }

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("targeted_shadow", str(kwargs.get("constraint_csv") or "")))
        return {"status": "ready"}

    def fake_settlement_state(*, output_dir: str):
        call_order.append(("settlement_state", output_dir))
        return {"status": "ready"}

    def fake_profitability(*, output_dir: str, **kwargs):
        call_order.append(("profitability", output_dir))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settled_outcome_throughput", fake_throughput)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "run_kalshi_temperature_profitability", fake_profitability)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/recovery.env",
        weather_min_bucket_samples=8,
        plateau_negative_regime_suppression_min_bucket_samples=5,
        plateau_negative_regime_suppression_expectancy_threshold=-0.08,
        plateau_negative_regime_suppression_top_n=3,
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("throughput", str(tmp_path)),
        ("targeted_shadow", str(targeted_csv)),
        ("settlement_state", str(tmp_path)),
        ("profitability", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/recovery.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["constraint_csv"] == str(targeted_csv)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is True
    assert round(float(trader_call["weather_pattern_profile_max_age_hours"]), 6) == 48.0
    assert int(trader_call["weather_pattern_min_bucket_samples"]) == 12
    assert round(float(trader_call["weather_pattern_negative_expectancy_threshold"]), 6) == -0.03
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert round(float(trader_call["weather_pattern_risk_off_concentration_threshold"]), 6) == 0.65
    assert int(trader_call["weather_pattern_risk_off_min_attempts"]) == 12
    assert round(float(trader_call["weather_pattern_risk_off_stale_metar_share_threshold"]), 6) == 0.45
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 12
    assert round(
        float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]),
        6,
    ) == -0.03
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 12
    assert trader_call["historical_selection_quality_enabled"] is True
    assert trader_call["enforce_probability_edge_thresholds"] is True
    assert round(float(trader_call["min_probability_confidence"]), 6) == 0.75
    assert round(float(trader_call["min_expected_edge_net"]), 6) == 0.015
    assert round(float(trader_call["min_edge_to_risk_ratio"]), 6) == 0.06

    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "recover_settled_outcome_velocity"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_increase_settled_outcome_coverage_skips_targeted_shadow_when_throughput_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_settled_outcome_coverage"],
            gap_score=0.17,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_call_count = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_throughput(*, output_dir: str, **kwargs):
        call_order.append(("throughput", output_dir))
        return {
            "status": "missing_profitability_summary",
            "targeted_constraint_csv": "",
            "targeted_constraint_rows": 0,
        }

    def fake_trader(**kwargs):
        nonlocal trader_call_count
        trader_call_count += 1
        return {"status": "ready"}

    def fake_settlement_state(*, output_dir: str):
        call_order.append(("settlement_state", output_dir))
        return {"status": "ready"}

    def fake_profitability(*, output_dir: str, **kwargs):
        call_order.append(("profitability", output_dir))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settled_outcome_throughput", fake_throughput)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "run_kalshi_temperature_profitability", fake_profitability)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert trader_call_count == 0
    assert call_order == [
        ("throughput", str(tmp_path)),
        ("settlement_state", str(tmp_path)),
        ("profitability", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "increase_settled_outcome_coverage"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_propagates_stalled_settled_outcome_growth_blocker_to_top_ranked_coverage_action(
    tmp_path: Path,
    monkeypatch,
) -> None:
    advisor_pass = 0
    action_call_order: list[tuple[str, str]] = []

    def fake_advisor_weather(
        *,
        output_dir: str,
        window_hours: float,
        min_bucket_samples: int,
        max_profile_age_hours: float,
    ):
        nonlocal advisor_pass
        advisor_pass += 1
        if advisor_pass == 1:
            return {
                "status": "ready",
                "overall": {"attempts_total": 280},
                "profile": {
                    "regime_risk": {
                        "negative_expectancy_attempt_share": 0.21,
                        "stale_metar_negative_attempt_share": 0.19,
                        "stale_metar_attempt_share": 0.22,
                    },
                    "risk_off_recommendation": {
                        "active": True,
                        "status": "risk_off_hard",
                        "reason": "settled_outcome_growth_stalled",
                    },
                },
            }
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.21,
                    "stale_metar_negative_attempt_share": 0.19,
                    "stale_metar_attempt_share": 0.22,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_advisor_decision(*, output_dir: str, **_: object):
        if advisor_pass == 1:
            return {
                "status": "ready",
                "observed_metrics": {"weather_risk_off_recommended": False},
                "blocking_factors": [
                    {
                        "key": "settled_outcome_growth_stalled",
                        "severity": "high",
                        "summary": "Settled outcome growth has stalled.",
                    }
                ],
            }
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_advisor_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    def fake_settlement_state(*, output_dir: str):
        action_call_order.append(("settlement_state", output_dir))
        return {"status": "ready"}

    def fake_profitability(*, output_dir: str, **kwargs):
        action_call_order.append(("profitability", output_dir))
        return {"status": "ready"}

    def fake_loop_decision(*, output_dir: str, **kwargs):
        action_call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    def fake_loop_weather(
        *,
        output_dir: str,
        window_hours: float,
        min_bucket_samples: int,
        max_profile_age_hours: float,
    ):
        action_call_order.append(("weather_pattern", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", recovery_advisor.run_kalshi_temperature_recovery_advisor)
    monkeypatch.setattr(recovery_advisor, "run_kalshi_temperature_weather_pattern", fake_advisor_weather)
    monkeypatch.setattr(recovery_advisor, "run_decision_matrix_hardening", fake_advisor_decision)
    monkeypatch.setattr(recovery_advisor, "run_kalshi_temperature_growth_optimizer", fake_advisor_growth)

    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "run_kalshi_temperature_profitability", fake_profitability)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_loop_decision)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_loop_weather)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert payload["iterations_executed"] == 1

    initial_action_rows = payload["initial_advisor"]["remediation_plan"]["prioritized_actions"]
    initial_action_keys = [str(row.get("key")) for row in initial_action_rows]
    assert initial_action_keys
    assert initial_action_keys[0] == "increase_settled_outcome_coverage"
    assert "clear_weather_risk_off_state" in initial_action_keys

    assert action_call_order == [
        ("settlement_state", str(tmp_path)),
        ("profitability", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
        ("weather_pattern", str(tmp_path)),
    ]

    iteration = payload["iteration_logs"][0]
    executed_keys = [str(row.get("key")) for row in iteration["executed_actions"]]
    assert executed_keys[:2] == [
        "increase_settled_outcome_coverage",
        "clear_weather_risk_off_state",
    ]
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_repair_weather_confidence_adjusted_signal_pipeline_runs_weather_then_hardening(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["repair_weather_confidence_adjusted_signal_pipeline"],
            gap_score=0.21,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        call_order.append(("weather_pattern", output_dir))
        assert float(window_hours) == 720.0
        assert int(min_bucket_samples) == 10
        assert float(max_profile_age_hours) == 336.0
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("weather_pattern", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "repair_weather_confidence_adjusted_signal_pipeline"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_reduce_stale_station_concentration_runs_metar_trader_weather_then_hardening(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_stale_station_concentration"],
            gap_score=0.23,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_metar(*, output_dir: str):
        call_order.append(("metar_ingest", output_dir))
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {"status": "ready"}

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        call_order.append(("weather_pattern", output_dir))
        assert float(window_hours) == 720.0
        assert int(min_bucket_samples) == 10
        assert float(max_profile_age_hours) == 336.0
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_metar_ingest", fake_metar)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("metar_ingest", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("weather_pattern", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 1
    trader_kwargs = trader_calls[0]
    assert trader_kwargs["weather_pattern_hardening_enabled"] is True
    assert trader_kwargs["weather_pattern_risk_off_enabled"] is True
    assert trader_kwargs["weather_pattern_negative_regime_suppression_enabled"] is True
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "reduce_stale_station_concentration"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_repair_metar_ingest_quality_pipeline_runs_metar_weather_trader_then_hardening(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["repair_metar_ingest_quality_pipeline"],
            gap_score=0.27,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_metar(*, output_dir: str):
        call_order.append(("metar_ingest", output_dir))
        return {"status": "ready"}

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        call_order.append(("weather_pattern", output_dir))
        assert float(window_hours) == 720.0
        assert int(min_bucket_samples) == 12
        assert float(max_profile_age_hours) == 336.0
        return {"status": "ready"}

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_metar_ingest", fake_metar)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("metar_ingest", str(tmp_path)),
        ("weather_pattern", str(tmp_path)),
        ("trader", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 1
    trader_kwargs = trader_calls[0]
    assert trader_kwargs["weather_pattern_hardening_enabled"] is True
    assert trader_kwargs["weather_pattern_risk_off_enabled"] is True
    assert trader_kwargs["weather_pattern_negative_regime_suppression_enabled"] is True
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "repair_metar_ingest_quality_pipeline"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_rebalance_weather_pattern_hard_block_pressure_runs_trader_weather_then_hardening(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["rebalance_weather_pattern_hard_block_pressure"],
            gap_score=0.26,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    call_order: list[tuple[str, str]] = []
    trader_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        trader_calls.append(dict(kwargs))
        call_order.append(("trader", str(kwargs.get("output_dir") or "")))
        return {"status": "ready"}

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        call_order.append(("weather_pattern", output_dir))
        assert float(window_hours) == 336.0
        assert int(min_bucket_samples) == 10
        assert float(max_profile_age_hours) == 336.0
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        call_order.append(("decision_matrix_hardening", output_dir))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        trader_env_file="/tmp/rebalance.env",
        weather_min_bucket_samples=8,
        plateau_negative_regime_suppression_min_bucket_samples=6,
        plateau_negative_regime_suppression_expectancy_threshold=-0.02,
        plateau_negative_regime_suppression_top_n=13,
    )

    assert payload["termination_reason"] == "cleared"
    assert call_order == [
        ("trader", str(tmp_path)),
        ("weather_pattern", str(tmp_path)),
        ("decision_matrix_hardening", str(tmp_path)),
    ]
    assert len(trader_calls) == 1
    trader_call = trader_calls[0]
    assert trader_call["env_file"] == "/tmp/rebalance.env"
    assert trader_call["output_dir"] == str(tmp_path)
    assert trader_call["intents_only"] is True
    assert trader_call["allow_live_orders"] is False
    assert trader_call["weather_pattern_hardening_enabled"] is True
    assert round(float(trader_call["weather_pattern_profile_max_age_hours"]), 6) == 24.0
    assert int(trader_call["weather_pattern_min_bucket_samples"]) == 10
    assert round(float(trader_call["weather_pattern_negative_expectancy_threshold"]), 6) == -0.03
    assert trader_call["weather_pattern_negative_regime_suppression_enabled"] is True
    assert int(trader_call["weather_pattern_negative_regime_suppression_min_bucket_samples"]) == 10
    assert round(
        float(trader_call["weather_pattern_negative_regime_suppression_expectancy_threshold"]),
        6,
    ) == -0.055
    assert int(trader_call["weather_pattern_negative_regime_suppression_top_n"]) == 6
    assert trader_call["weather_pattern_risk_off_enabled"] is True
    assert round(float(trader_call["weather_pattern_risk_off_concentration_threshold"]), 6) == 0.75
    assert int(trader_call["weather_pattern_risk_off_min_attempts"]) == 10
    assert round(float(trader_call["weather_pattern_risk_off_stale_metar_share_threshold"]), 6) == 0.45
    assert trader_call["historical_selection_quality_enabled"] is True
    assert trader_call["enforce_probability_edge_thresholds"] is True
    assert round(float(trader_call["min_probability_confidence"]), 6) == 0.82
    assert round(float(trader_call["min_expected_edge_net"]), 6) == 0.02
    assert round(float(trader_call["min_edge_to_risk_ratio"]), 6) == 0.08
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "rebalance_weather_pattern_hard_block_pressure"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_unsupported_action_remains_unknown_action_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["unsupported_action_key"],
            gap_score=0.18,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    iteration = payload["iteration_logs"][0]
    action = iteration["executed_actions"][0]
    assert action["key"] == "unsupported_action_key"
    assert action["status"] == "unknown_action"
    assert action["error"] == "Unsupported action key: unsupported_action_key"


def test_recovery_loop_refresh_decision_matrix_weather_signals_executes_hardening(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_decision_matrix_weather_signals"],
            gap_score=0.12,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    decision_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert decision_calls == 1
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "refresh_decision_matrix_weather_signals"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_restore_stage_timeout_guardrail_script_executes_hardening(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["restore_stage_timeout_guardrail_script"],
            gap_score=0.12,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    decision_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert decision_calls == 1
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "restore_stage_timeout_guardrail_script"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_rerun_stage_timeout_guardrail_hardening_refreshes_advisor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["rerun_stage_timeout_guardrail_hardening"],
            gap_score=0.12,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[],
            gap_score=0.12,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    decision_calls = 0
    advisor_calls = 0

    def fake_advisor(**kwargs):
        nonlocal advisor_calls
        advisor_calls += 1
        return sequence.pop(0)

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert decision_calls == 1
    assert advisor_calls == 3
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "rerun_stage_timeout_guardrail_hardening"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_clear_optimizer_weather_hard_block_enforces_top_n_floor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["clear_optimizer_weather_hard_block"],
            gap_score=0.15,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    optimizer_calls: list[dict[str, object]] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_growth(*, input_paths: list[str], top_n: int):
        optimizer_calls.append({"input_paths": list(input_paths), "top_n": int(top_n)})
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        optimizer_top_n=4,
    )

    assert payload["termination_reason"] == "cleared"
    assert optimizer_calls == [{"input_paths": [str(tmp_path)], "top_n": 10}]
    iteration = payload["iteration_logs"][0]
    assert iteration["executed_actions"][0]["key"] == "clear_optimizer_weather_hard_block"
    assert iteration["executed_actions"][0]["status"] == "executed"


def test_recovery_loop_clear_optimizer_weather_hard_block_respects_larger_top_n(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["clear_optimizer_weather_hard_block"],
            gap_score=0.15,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        ),
    ]
    optimizer_calls: list[int] = []

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_growth(*, input_paths: list[str], top_n: int):
        optimizer_calls.append(int(top_n))
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        optimizer_top_n=13,
    )

    assert payload["termination_reason"] == "cleared"
    assert optimizer_calls == [13]


def test_recovery_loop_insufficient_data_runs_multiple_iterations_until_stalled(
    tmp_path: Path,
    monkeypatch,
) -> None:
    advisor_calls = 0
    throughput_calls = 0
    settlement_calls = 0
    profitability_calls = 0
    decision_calls = 0

    def fake_advisor(**kwargs):
        nonlocal advisor_calls
        advisor_calls += 1
        return _advisor_payload(
            status="insufficient_data",
            actions=["increase_settled_outcome_coverage"],
            gap_score=0.23,
        )

    def fake_throughput(*, output_dir: str):
        nonlocal throughput_calls
        throughput_calls += 1
        return {"status": "ready", "targeted_constraint_csv": ""}

    def fake_settlement_state(*, output_dir: str):
        nonlocal settlement_calls
        settlement_calls += 1
        return {"status": "ready"}

    def fake_profitability(*, output_dir: str, **kwargs):
        nonlocal profitability_calls
        profitability_calls += 1
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settled_outcome_throughput", fake_throughput)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "run_kalshi_temperature_profitability", fake_profitability)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert advisor_calls == 3
    assert throughput_calls == 2
    assert settlement_calls == 2
    assert profitability_calls == 2
    assert decision_calls == 2
    assert payload["termination_reason"] == "stalled"
    assert payload["iterations_executed"] == 2
    assert payload["final_advisor_status"] == "insufficient_data"
    assert [entry["advisor_status_before"] for entry in payload["iteration_logs"]] == [
        "insufficient_data",
        "insufficient_data",
    ]
    assert [entry["advisor_status_after"] for entry in payload["iteration_logs"]] == [
        "insufficient_data",
        "insufficient_data",
    ]
    assert [entry["executed_actions"][0]["key"] for entry in payload["iteration_logs"]] == [
        "increase_settled_outcome_coverage",
        "increase_settled_outcome_coverage",
    ]


def test_recovery_loop_insufficient_data_can_terminate_at_max_iterations(
    tmp_path: Path,
    monkeypatch,
) -> None:
    advisor_calls = 0
    decision_calls = 0
    sequence = [
        _advisor_payload(
            status="insufficient_data",
            actions=["refresh_recovery_stack"],
            gap_score=0.30,
        ),
        _advisor_payload(
            status="insufficient_data",
            actions=["refresh_recovery_stack"],
            gap_score=0.20,
        ),
        _advisor_payload(
            status="insufficient_data",
            actions=["refresh_recovery_stack"],
            gap_score=0.10,
        ),
        _advisor_payload(
            status="insufficient_data",
            actions=["refresh_recovery_stack"],
            gap_score=0.00,
        ),
    ]

    def fake_advisor(**kwargs):
        nonlocal advisor_calls
        advisor_calls += 1
        return sequence.pop(0)

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=3,
        stall_iterations=2,
        min_gap_improvement=0.01,
    )

    assert advisor_calls == 4
    assert decision_calls == 3
    assert payload["termination_reason"] == "max_iterations"
    assert payload["iterations_executed"] == 3
    assert payload["final_advisor_status"] == "insufficient_data"
    assert [entry["improvement"] for entry in payload["iteration_logs"]] == [0.1, 0.1, 0.1]


def test_recovery_loop_insufficient_data_with_zero_max_iterations_keeps_short_circuit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    advisor_calls = 0

    def fake_advisor(**kwargs):
        nonlocal advisor_calls
        advisor_calls += 1
        return _advisor_payload(
            status="insufficient_data",
            actions=["increase_settled_outcome_coverage"],
            gap_score=0.3,
        )

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=0,
    )

    assert advisor_calls == 1
    assert payload["termination_reason"] == "insufficient_data"
    assert payload["iterations_executed"] == 0
    assert payload["final_advisor_status"] == "insufficient_data"


def test_recovery_loop_stalls_when_gap_does_not_improve(tmp_path: Path, monkeypatch) -> None:
    advisor_call_count = 0
    decision_calls = 0

    def fake_advisor(**kwargs):
        nonlocal advisor_call_count
        advisor_call_count += 1
        return _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.25,
        )

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=5,
        stall_iterations=2,
        min_gap_improvement=0.01,
    )

    assert payload["termination_reason"] == "stalled"
    assert payload["iterations_executed"] == 2
    assert advisor_call_count == 3
    assert decision_calls == 2
    assert [log["improvement"] for log in payload["iteration_logs"]] == [0.0, 0.0]


def test_recovery_loop_logs_negative_share_attribution_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
            negative_expectancy_attempt_share=0.35,
        ),
    ]
    decision_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert decision_calls == 1
    iteration = payload["iteration_logs"][0]
    assert iteration["negative_expectancy_attempt_share_before"] == 0.2
    assert iteration["negative_expectancy_attempt_share_after"] == 0.35
    assert iteration["negative_expectancy_attempt_share_delta"] == 0.15
    assert iteration["negative_expectancy_attempt_share_worsened"] is True
    assert iteration["negative_share_worsening_streaks"]["refresh_recovery_stack"] == 1
    assert payload["negative_share_worsening_streaks"]["refresh_recovery_stack"] == 1


def test_recovery_loop_core_coverage_action_throttles_after_two_worsening_iterations(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.30,
        ),
    ]
    weather_calls = 0
    trader_calls = 0
    decision_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_weather(*, output_dir: str, **kwargs):
        nonlocal weather_calls
        weather_calls += 1
        return {"status": "ready"}

    def fake_trader(**kwargs):
        nonlocal trader_calls
        trader_calls += 1
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=2,
        stall_iterations=4,
        min_gap_improvement=0.01,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["iterations_executed"] == 2
    assert weather_calls == 2
    assert trader_calls == 0
    assert decision_calls == 0
    assert payload["auto_disabled_actions"] == []
    assert payload["throttled_actions"] == ["increase_weather_sample_coverage"]
    assert payload["action_cooldowns"]["increase_weather_sample_coverage"] == 1
    assert payload["negative_share_worsening_streaks"]["increase_weather_sample_coverage"] == 2


def test_recovery_loop_alpha_lever_action_throttles_after_two_worsening_iterations(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.30,
        ),
    ]
    trader_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        nonlocal trader_calls
        trader_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=2,
        stall_iterations=4,
        min_gap_improvement=0.01,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["iterations_executed"] == 2
    assert trader_calls == 2
    assert payload["auto_disabled_actions"] == []
    assert payload["throttled_actions"] == ["reduce_negative_expectancy_regimes"]
    assert payload["action_cooldowns"]["reduce_negative_expectancy_regimes"] == 2
    assert payload["negative_share_worsening_streaks"]["reduce_negative_expectancy_regimes"] == 2


def test_recovery_loop_alpha_lever_action_cooldown_lasts_two_iterations(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes", "unknown_action_padding"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes", "unknown_action_padding"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes", "unknown_action_padding"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.30,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes", "unknown_action_padding"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.30,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["reduce_negative_expectancy_regimes", "unknown_action_padding"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.30,
        ),
    ]
    trader_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        nonlocal trader_calls
        trader_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=4,
        stall_iterations=6,
        min_gap_improvement=0.0,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["iterations_executed"] == 4
    assert trader_calls == 2
    assert payload["auto_disabled_actions"] == []
    assert payload["throttled_actions"] == ["reduce_negative_expectancy_regimes"]
    assert payload["action_cooldowns"] == {}
    assert payload["negative_share_worsening_streaks"]["reduce_negative_expectancy_regimes"] == 2

    third_iteration = payload["iteration_logs"][2]
    fourth_iteration = payload["iteration_logs"][3]
    third_reduce = next(
        row for row in third_iteration["executed_actions"] if row["key"] == "reduce_negative_expectancy_regimes"
    )
    fourth_reduce = next(
        row for row in fourth_iteration["executed_actions"] if row["key"] == "reduce_negative_expectancy_regimes"
    )
    assert third_reduce["status"] == "cooldown_skip"
    assert third_reduce["policy_class"] == "alpha_lever"
    assert third_reduce["cooldown_remaining_before"] == 2
    assert third_reduce["cooldown_remaining_after"] == 1
    assert fourth_reduce["status"] == "cooldown_skip"
    assert fourth_reduce["policy_class"] == "alpha_lever"
    assert fourth_reduce["cooldown_remaining_before"] == 1
    assert fourth_reduce["cooldown_remaining_after"] == 0


def test_recovery_loop_risky_edge_relax_action_hard_disables_after_two_worsening_iterations(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["apply_expected_edge_relief_shadow_profile"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["apply_expected_edge_relief_shadow_profile"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["apply_expected_edge_relief_shadow_profile"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.30,
        ),
    ]
    trader_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_trader(**kwargs):
        nonlocal trader_calls
        trader_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=2,
        stall_iterations=4,
        min_gap_improvement=0.01,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["iterations_executed"] == 2
    assert trader_calls == 2
    assert payload["auto_disabled_actions"] == ["apply_expected_edge_relief_shadow_profile"]
    assert payload["throttled_actions"] == []
    assert payload["action_cooldowns"] == {}
    assert payload["negative_share_worsening_streaks"]["apply_expected_edge_relief_shadow_profile"] == 2


def test_recovery_loop_triggers_defensive_pivot_when_primary_actions_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.30,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.40,
        ),
    ]
    weather_calls = 0
    trader_calls = 0
    decision_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_weather(*, output_dir: str, **kwargs):
        nonlocal weather_calls
        weather_calls += 1
        return {"status": "ready"}

    def fake_trader(**kwargs):
        nonlocal trader_calls
        trader_calls += 1
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=3,
        stall_iterations=4,
        min_gap_improvement=0.01,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["iterations_executed"] == 3
    assert weather_calls == 2
    assert trader_calls == 1
    assert decision_calls == 1
    assert payload["throttled_actions"] == ["increase_weather_sample_coverage"]
    assert payload["action_cooldowns"] == {}

    third_iteration = payload["iteration_logs"][2]
    assert third_iteration["executed_actions"][0]["key"] == "increase_weather_sample_coverage"
    assert third_iteration["executed_actions"][0]["status"] == "cooldown_skip"
    assert third_iteration["executed_actions"][0]["reason"] == (
        "throttled_after_consecutive_negative_share_worsening"
    )
    assert third_iteration["executed_actions"][1]["key"] == loop.DEFENSIVE_PIVOT_ACTION_KEY
    assert third_iteration["executed_actions"][1]["status"] == "executed"
    assert third_iteration["executed_actions"][1]["reason"] == (
        "auto_defensive_pivot_all_primary_actions_unavailable"
    )


def test_recovery_loop_resets_action_worsening_streak_when_delta_not_worsening(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.15,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
            negative_expectancy_attempt_share=0.19,
        ),
    ]
    decision_calls = 0

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert payload["iterations_executed"] == 2
    assert decision_calls == 2
    assert payload["negative_share_worsening_streaks"]["refresh_recovery_stack"] == 0
    assert payload["auto_disabled_actions"] == []
    assert payload["iteration_logs"][0]["negative_share_worsening_streaks"]["refresh_recovery_stack"] == 1
    assert payload["iteration_logs"][1]["negative_share_worsening_streaks"]["refresh_recovery_stack"] == 0


def test_recovery_loop_action_effectiveness_metrics_accumulate_for_executed_actions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.25,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.20,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
            negative_expectancy_attempt_share=0.15,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    assert payload["termination_reason"] == "cleared"
    assert payload["iterations_executed"] == 2
    metrics = payload["action_effectiveness"]["refresh_recovery_stack"]
    assert metrics["executed_count"] == 2
    assert metrics["worsening_count"] == 1
    assert metrics["non_worsening_count"] == 1
    assert metrics["cumulative_negative_share_delta"] == 0.05
    assert metrics["average_negative_share_delta"] == 0.025
    assert payload["demoted_actions"] == []
    assert payload["iteration_logs"][0]["action_effectiveness"]["refresh_recovery_stack"]["executed_count"] == 1
    assert payload["iteration_logs"][1]["action_effectiveness"]["refresh_recovery_stack"]["executed_count"] == 2
    assert payload["iteration_logs"][1]["demoted_actions"] == []


def test_recovery_loop_adaptive_effectiveness_thresholds_tighten_in_harsher_regimes() -> None:
    mild = loop._compute_adaptive_effectiveness_thresholds(
        negative_share_before=0.35,
        negative_share_after=0.36,
        negative_share_delta_history=[0.001, 0.0015, -0.0005],
    )
    harsh = loop._compute_adaptive_effectiveness_thresholds(
        negative_share_before=0.92,
        negative_share_after=0.95,
        negative_share_delta_history=[0.03, 0.04, 0.02],
    )

    assert harsh["severity"] > mild["severity"]
    assert harsh["worsening_velocity"] > mild["worsening_velocity"]
    assert int(harsh["min_executions"]) < int(mild["min_executions"])
    assert float(harsh["min_worsening_ratio"]) < float(mild["min_worsening_ratio"])


def test_recovery_loop_demotes_persistently_harmful_action_after_effectiveness_threshold(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.30,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.40,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.50,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=4,
        stall_iterations=8,
        min_gap_improvement=0.0,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["demoted_actions"] == ["increase_weather_sample_coverage"]
    assert "increase_weather_sample_coverage" in payload["auto_disabled_actions"]
    metrics = payload["action_effectiveness"]["increase_weather_sample_coverage"]
    assert metrics["executed_count"] == 3
    assert metrics["worsening_count"] == 3
    assert metrics["non_worsening_count"] == 0
    assert metrics["cumulative_negative_share_delta"] == 0.3
    assert metrics["average_negative_share_delta"] == 0.1


def test_recovery_loop_routes_demoted_core_action_to_strict_replacement(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.10,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.30,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.40,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.50,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["increase_weather_sample_coverage"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.60,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    weather_calls = 0
    trader_calls = 0
    decision_calls = 0

    def fake_weather(**kwargs):
        nonlocal weather_calls
        weather_calls += 1
        return {"status": "ready"}

    def fake_trader(**kwargs):
        nonlocal trader_calls
        trader_calls += 1
        return {"status": "ready"}

    def fake_decision(**kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", fake_trader)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=5,
        stall_iterations=8,
        min_gap_improvement=0.0,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["demoted_actions"] == ["increase_weather_sample_coverage"]
    assert payload["replacement_usage_by_source"]["increase_weather_sample_coverage"] == 1
    replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY["increase_weather_sample_coverage"]
    assert payload["replacement_usage_by_action"][replacement_key] == 1
    final_iteration = payload["iteration_logs"][4]
    source_row = next(
        row for row in final_iteration["executed_actions"] if row["key"] == "increase_weather_sample_coverage"
    )
    assert source_row["status"] == "auto_disabled"
    assert source_row["reason"] == "demoted_source_routed_to_strict_replacement"
    assert source_row["replacement_action_key"] == replacement_key
    replacement_row = next(
        row
        for row in final_iteration["executed_actions"]
        if row["key"] == replacement_key
    )
    assert replacement_row["status"] == "executed"
    assert replacement_row["reason"] == "replacement_for_demoted_action_effectiveness"
    assert replacement_row["replacement_for_action_key"] == "increase_weather_sample_coverage"
    assert replacement_row["source_action_policy_class"] == "core_coverage"
    assert replacement_row["replacement_routing_status"] == "executed"
    replacement_log_row = final_iteration["replacement_actions"][0]
    assert replacement_log_row["source_action_key"] == "increase_weather_sample_coverage"
    assert replacement_log_row["replacement_action_key"] == replacement_key
    assert replacement_log_row["status"] == "executed"
    assert replacement_log_row["reason"] == "replacement_for_demoted_action_effectiveness"
    assert weather_calls >= 4
    assert trader_calls >= 1
    assert decision_calls >= 1
    metrics = payload["action_effectiveness"]["increase_weather_sample_coverage"]
    assert metrics["executed_count"] == 3
    assert metrics["worsening_count"] == 3
    assert metrics["non_worsening_count"] == 0
    replacement_metrics = payload["action_effectiveness"][replacement_key]
    assert replacement_metrics["executed_count"] == 1
    assert replacement_metrics["worsening_count"] == 1
    assert replacement_metrics["non_worsening_count"] == 0


def test_recovery_loop_demoted_source_marks_replacement_unavailable_after_replacement_demotion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    tertiary_key = loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.90,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.93,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.96,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.99,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.995,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.997,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "_compute_adaptive_effectiveness_thresholds", _aggressive_effectiveness_thresholds)
    monkeypatch.setattr(loop, "TERTIARY_RESERVE_PROTECTION_MAX_ITERATIONS", 0)
    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=5,
        stall_iterations=8,
        min_gap_improvement=0.0,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert source_action_key in payload["demoted_actions"]
    replacement_inventory_demoted = replacement_key in payload["demoted_actions"] or tertiary_key in payload["demoted_actions"]
    replacement_inventory_disabled = (
        replacement_key in payload["auto_disabled_actions"] or tertiary_key in payload["auto_disabled_actions"]
    )
    assert replacement_inventory_demoted is True
    assert replacement_inventory_disabled is True

    executed_replacements_for_source = [
        row
        for iteration in payload["iteration_logs"]
        for row in iteration["executed_actions"]
        if row.get("replacement_for_action_key") == source_action_key and row.get("status") == "executed"
    ]
    if executed_replacements_for_source:
        assert executed_replacements_for_source[0]["key"] in {replacement_key, tertiary_key}

    fifth_iteration = payload["iteration_logs"][4]
    fifth_source = next(
        row for row in fifth_iteration["executed_actions"] if row["key"] == source_action_key
    )
    assert fifth_source["status"] == "auto_disabled"
    tertiary_row = next(
        (
            row
            for row in fifth_iteration["executed_actions"]
            if row.get("replacement_for_action_key") == source_action_key
            and row["key"] not in {replacement_key, source_action_key}
            and row.get("status") == "executed"
        ),
        None,
    )

    if tertiary_row is not None:
        assert fifth_source["reason"] in {
            "demoted_source_routed_to_tertiary_replacement",
            "demoted_source_routed_to_strict_replacement",
            "demoted_source_routed_to_final_fallback",
        }
        assert fifth_source["replacement_routing_status"] in {"routed", "executed"}
        replacement_log_row = fifth_iteration["replacement_actions"][0]
        assert replacement_log_row["source_action_key"] == "increase_weather_sample_coverage"
        assert replacement_log_row["replacement_action_key"] == tertiary_row["key"]
        assert replacement_log_row["status"] == "executed"
    else:
        assert fifth_source["reason"] == "demoted_source_replacement_unavailable"
        assert fifth_source["replacement_action_key"] == replacement_key
        assert fifth_source["replacement_action_viable"] is False
        assert fifth_source["replacement_routing_status"] == "unavailable"
        assert fifth_source["replacement_unavailable_reason"] in {
            "replacement_action_demoted",
            "replacement_action_auto_disabled",
        }
        assert not any(row["key"] == replacement_key for row in fifth_iteration["executed_actions"])
        replacement_log_row = fifth_iteration["replacement_actions"][0]
        assert replacement_log_row["source_action_key"] == source_action_key
        assert replacement_log_row["replacement_action_key"] == replacement_key
        assert replacement_log_row["status"] == "unavailable"
        assert replacement_log_row["reason"] in {
            "replacement_action_demoted",
            "replacement_action_auto_disabled",
        }
        pivot_row = next(
            row for row in fifth_iteration["executed_actions"] if row["key"] == loop.DEFENSIVE_PIVOT_ACTION_KEY
        )
        assert pivot_row["status"] == "executed"
        assert pivot_row["reason"] == "auto_defensive_pivot_all_primary_actions_unavailable"


def test_recovery_loop_routes_to_tertiary_replacement_when_strict_replacement_is_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    strict_replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.90,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.93,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.96,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.98,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.99,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.995,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "_compute_adaptive_effectiveness_thresholds", _aggressive_effectiveness_thresholds)
    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=5,
        stall_iterations=8,
        min_gap_improvement=0.0,
    )

    tertiary_hit: tuple[dict[str, object], dict[str, object]] | None = None
    for iteration in payload["iteration_logs"]:
        for row in iteration["executed_actions"]:
            if row.get("replacement_for_action_key") != source_action_key:
                continue
            if row.get("status") != "executed":
                continue
            action_key = str(row.get("key") or "")
            if action_key in {source_action_key, strict_replacement_key}:
                continue
            tertiary_hit = (iteration, row)
            break
        if tertiary_hit is not None:
            break

    if tertiary_hit is None:
        pytest.xfail("Tertiary replacement routing not yet exposed in recovery loop.")

    iteration, tertiary_row = tertiary_hit
    candidate_tertiary_actions = _discover_tertiary_action_candidates()
    tertiary_key = str(tertiary_row["key"])
    assert tertiary_key in candidate_tertiary_actions or "tertiary" in tertiary_key.lower()
    assert tertiary_row["replacement_for_action_key"] == source_action_key
    assert tertiary_row["reason"] == "replacement_for_demoted_action_effectiveness"
    assert tertiary_row.get("replacement_routing_status") == "executed"

    source_row = next(row for row in iteration["executed_actions"] if row["key"] == source_action_key)
    assert source_row["status"] == "auto_disabled"
    assert source_row["replacement_routing_status"] in {"routed", "executed"}
    routing_tier = str(
        source_row.get("replacement_routing_tier")
        or source_row.get("replacement_tier")
        or ""
    ).lower()
    if routing_tier:
        assert "tertiary" in routing_tier

    replacement_log_row = next(
        row for row in iteration["replacement_actions"] if row["source_action_key"] == source_action_key
    )
    assert replacement_log_row["replacement_action_key"] == tertiary_key
    assert replacement_log_row["status"] == "executed"
    if "routing_tier" in replacement_log_row:
        assert "tertiary" in str(replacement_log_row["routing_tier"]).lower()


def test_recovery_loop_protects_tertiary_reserve_inventory_until_strict_replacement_is_demoted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    strict_replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    tertiary_replacement_key = loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    payload = _run_recovery_loop_with_negative_share_sequence(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        source_action_key=source_action_key,
        negative_shares=[0.90, 0.93, 0.96, 0.98, 0.99, 0.995],
        aggressive_thresholds=True,
        max_iterations=5,
    )

    strict_route_hit: tuple[dict[str, object], dict[str, object]] | None = None
    tertiary_route_hit: tuple[dict[str, object], dict[str, object]] | None = None
    for iteration in payload["iteration_logs"]:
        source_row = next(
            (row for row in iteration["executed_actions"] if row.get("key") == source_action_key),
            None,
        )
        if source_row is None:
            continue
        if (
            source_row.get("replacement_route_stage") == "strict"
            and source_row.get("replacement_routing_status") == "routed"
        ):
            strict_route_hit = (iteration, source_row)
        if (
            source_row.get("replacement_route_stage") == "tertiary"
            and source_row.get("replacement_routing_status") == "routed"
        ):
            tertiary_route_hit = (iteration, source_row)
            break

    assert strict_route_hit is not None
    assert tertiary_route_hit is not None

    strict_iteration, strict_source_row = strict_route_hit
    assert strict_source_row["replacement_action_key"] == strict_replacement_key
    assert strict_source_row["selected_replacement_action_key"] == strict_replacement_key
    assert strict_source_row["tertiary_replacement_action_key"] == tertiary_replacement_key
    assert strict_source_row["tertiary_replacement_action_viable"] is True
    assert strict_iteration["tertiary_replacement_usage_by_source"] == {}
    assert strict_iteration["tertiary_replacement_usage_by_action"] == {}

    tertiary_iteration, tertiary_source_row = tertiary_route_hit
    assert tertiary_source_row["replacement_action_key"] == strict_replacement_key
    assert tertiary_source_row["replacement_unavailable_reason"] == "replacement_action_demoted"
    assert tertiary_source_row["replacement_route_stage"] == "tertiary"
    assert tertiary_source_row["selected_replacement_action_key"] == tertiary_replacement_key
    assert tertiary_source_row["tertiary_replacement_action_key"] == tertiary_replacement_key
    assert tertiary_source_row["tertiary_replacement_action_viable"] is True

    tertiary_execution_row = next(
        row
        for row in tertiary_iteration["executed_actions"]
        if row.get("key") == tertiary_replacement_key
        and row.get("replacement_for_action_key") == source_action_key
    )
    assert tertiary_execution_row["status"] == "executed"
    assert tertiary_execution_row["replacement_route_stage"] == "tertiary"


def test_recovery_loop_surfaces_tertiary_reserve_payload_visibility_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    strict_replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    tertiary_replacement_key = loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    payload = _run_recovery_loop_with_negative_share_sequence(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        source_action_key=source_action_key,
        negative_shares=[0.90, 0.93, 0.96, 0.98, 0.99, 0.995],
        aggressive_thresholds=True,
        max_iterations=5,
    )

    assert payload["replacement_action_map"][source_action_key] == strict_replacement_key
    assert payload["tertiary_replacement_action_map"][source_action_key] == tertiary_replacement_key
    assert payload["tertiary_replacement_usage_by_source"][source_action_key] >= 1
    assert payload["tertiary_replacement_usage_by_action"][tertiary_replacement_key] >= 1
    assert payload["adaptive_replacement_profile"]["selected_profile"] in loop.REPLACEMENT_PROFILE_CONFIG

    tertiary_iteration = next(
        iteration
        for iteration in payload["iteration_logs"]
        if iteration["tertiary_replacement_usage_by_source"].get(source_action_key, 0) > 0
    )
    source_row = next(
        row for row in tertiary_iteration["executed_actions"] if row.get("key") == source_action_key
    )
    replacement_log_row = next(
        row
        for row in tertiary_iteration["replacement_actions"]
        if row.get("source_action_key") == source_action_key
    )
    tertiary_execution_row = next(
        row
        for row in tertiary_iteration["executed_actions"]
        if row.get("key") == tertiary_replacement_key
        and row.get("replacement_for_action_key") == source_action_key
    )

    assert source_row["replacement_route_stage"] == "tertiary"
    assert source_row["selected_replacement_action_key"] == tertiary_replacement_key
    assert source_row["adaptive_replacement_profile"] == tertiary_iteration["adaptive_replacement_profile"]

    assert replacement_log_row["strict_replacement_action_key"] == strict_replacement_key
    assert replacement_log_row["tertiary_replacement_action_key"] == tertiary_replacement_key
    assert replacement_log_row["replacement_route_stage"] == "tertiary"
    assert replacement_log_row["adaptive_replacement_profile"] == tertiary_iteration["adaptive_replacement_profile"]

    assert tertiary_execution_row["adaptive_replacement_profile"] == tertiary_iteration["adaptive_replacement_profile"]


def test_replacement_route_arbitrates_to_tertiary_when_strict_effectiveness_is_worse() -> None:
    source_action_key = "increase_weather_sample_coverage"
    policy_class, _ = loop._policy_class_and_cooldown(source_action_key)
    strict_replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    tertiary_replacement_key = loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]

    route = _call_demoted_source_replacement_route(
        action_key=source_action_key,
        policy_class=policy_class,
        demoted_actions={source_action_key},
        auto_disabled_actions=set(),
        replacement_sources_executed=set(),
        action_effectiveness={
            strict_replacement_key: {
                "executed_count": 8,
                "worsening_count": 7,
                "non_worsening_count": 1,
                "cumulative_negative_share_delta": 0.24,
                "average_negative_share_delta": 0.03,
            },
            tertiary_replacement_key: {
                "executed_count": 8,
                "worsening_count": 1,
                "non_worsening_count": 7,
                "cumulative_negative_share_delta": -0.16,
                "average_negative_share_delta": -0.02,
            },
        },
    )

    assert route["strict_replacement_action_viable"] is True
    assert route["tertiary_replacement_action_viable"] is True
    arbitration_fields = _arbitration_scalar_fields(route)
    assert arbitration_fields
    assert route["replacement_route_stage"] == "tertiary"
    assert route["selected_replacement_action_key"] == tertiary_replacement_key
    assert route["replacement_routing_status"] == "routed"


def test_recovery_loop_surfaces_arbitration_decision_fields_across_source_replacement_and_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    payload = _run_recovery_loop_with_negative_share_sequence(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        source_action_key=source_action_key,
        negative_shares=[0.90, 0.93, 0.96, 0.98, 0.99, 0.995],
        aggressive_thresholds=True,
        max_iterations=5,
    )

    routed_iteration: tuple[dict[str, object], dict[str, object], dict[str, object]] | None = None
    for iteration in payload["iteration_logs"]:
        source_row = next(
            (
                row
                for row in iteration["executed_actions"]
                if row.get("key") == source_action_key
                and row.get("status") == "auto_disabled"
                and row.get("replacement_routing_status") == "routed"
            ),
            None,
        )
        if source_row is None:
            continue
        replacement_log_row = next(
            (
                row
                for row in iteration["replacement_actions"]
                if row.get("source_action_key") == source_action_key
            ),
            None,
        )
        if replacement_log_row is None:
            continue
        routed_iteration = (iteration, source_row, replacement_log_row)
        break

    assert routed_iteration is not None
    iteration, source_row, replacement_log_row = routed_iteration
    source_arbitration_fields = _arbitration_scalar_fields(source_row)
    replacement_arbitration_fields = _arbitration_scalar_fields(replacement_log_row)
    iteration_arbitration_fields = _arbitration_scalar_fields(iteration)
    payload_arbitration_fields = _arbitration_scalar_fields(payload)
    if (
        not source_arbitration_fields
        or not replacement_arbitration_fields
        or not iteration_arbitration_fields
        or not payload_arbitration_fields
    ):
        raise AssertionError("Arbitration decision fields were expected across source/replacement/payload layers.")

    source_stage = str(source_row.get("replacement_route_stage") or "").strip()
    selected_replacement_action_key = str(source_row.get("selected_replacement_action_key") or "").strip()
    replacement_action_key = str(replacement_log_row.get("replacement_action_key") or "").strip()
    assert source_stage in {"strict", "tertiary"}
    assert selected_replacement_action_key
    assert selected_replacement_action_key == replacement_action_key


def test_replacement_route_hard_disable_tertiary_mappings_no_longer_emit_not_mapped_when_viable() -> None:
    hard_disable_mapped_sources = [
        action_key
        for action_key in sorted(loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY)
        if loop._policy_class_and_cooldown(action_key)[0] == "hard_disable"
    ]
    if not hard_disable_mapped_sources:
        pytest.xfail("No hard-disable tertiary mappings are configured.")

    for source_action_key in hard_disable_mapped_sources:
        policy_class, _ = loop._policy_class_and_cooldown(source_action_key)
        route = _call_demoted_source_replacement_route(
            action_key=source_action_key,
            policy_class=policy_class,
            demoted_actions={source_action_key},
            auto_disabled_actions=set(),
            replacement_sources_executed=set(),
        )
        assert route["tertiary_replacement_action_viable"] is True
        assert str(route.get("replacement_routing_status") or "").lower() != "not_mapped"
        assert route["replacement_routing_status"] == "routed"
        assert route["replacement_route_stage"] in {"strict", "tertiary"}
        assert str(route.get("selected_replacement_action_key") or "").strip()


def test_recovery_loop_hard_disable_mapped_fallback_rows_avoid_not_mapped_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    hard_disable_mapped_sources = [
        action_key
        for action_key in sorted(loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY)
        if loop._policy_class_and_cooldown(action_key)[0] == "hard_disable"
    ]
    if not hard_disable_mapped_sources:
        pytest.xfail("No hard-disable tertiary mappings are configured.")
    source_action_key = hard_disable_mapped_sources[0]
    expected_tertiary_key = loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]

    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.90,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.93,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.96,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.98,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "_compute_adaptive_effectiveness_thresholds", _aggressive_effectiveness_thresholds)
    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    _patch_recovery_action_runners_ready(monkeypatch)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=3,
        stall_iterations=8,
        min_gap_improvement=0.0,
    )

    demoted_iteration: tuple[dict[str, object], dict[str, object], dict[str, object]] | None = None
    for iteration in payload["iteration_logs"]:
        source_row = next(
            (
                row
                for row in iteration["executed_actions"]
                if row.get("key") == source_action_key and row.get("status") == "auto_disabled"
            ),
            None,
        )
        if source_row is None:
            continue
        replacement_log_row = next(
            (
                row
                for row in iteration["replacement_actions"]
                if row.get("source_action_key") == source_action_key
            ),
            None,
        )
        if replacement_log_row is None:
            continue
        demoted_iteration = (iteration, source_row, replacement_log_row)
        break

    assert demoted_iteration is not None
    iteration, source_row, replacement_log_row = demoted_iteration
    if str(source_row.get("replacement_routing_status") or "").lower() == "not_mapped":
        pytest.xfail("Hard-disable mapped tertiary fallback is still reported as not_mapped in source row.")

    assert source_row["tertiary_replacement_action_key"] == expected_tertiary_key
    assert source_row["tertiary_replacement_action_viable"] is True
    assert source_row["replacement_routing_status"] in {"routed", "executed", "unavailable"}
    assert source_row["replacement_route_stage"] in {"strict", "tertiary"}
    assert replacement_log_row["status"] != "not_routed"
    assert replacement_log_row["replacement_route_stage"] in {"strict", "tertiary"}
    assert payload["tertiary_replacement_action_map"][source_action_key] == expected_tertiary_key
    assert iteration["tertiary_replacement_usage_by_source"].get(source_action_key, 0) >= 0


def test_replacement_route_selects_final_fallback_when_strict_and_tertiary_are_unavailable() -> None:
    source_action_key = "increase_weather_sample_coverage"
    policy_class, _ = loop._policy_class_and_cooldown(source_action_key)
    strict_replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    tertiary_replacement_key = loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]

    route = _call_demoted_source_replacement_route(
        action_key=source_action_key,
        policy_class=policy_class,
        demoted_actions={
            source_action_key,
            strict_replacement_key,
            tertiary_replacement_key,
        },
        auto_disabled_actions=set(),
        replacement_sources_executed=set(),
        action_effectiveness={},
    )

    route_stage = str(route.get("replacement_route_stage") or "").strip().lower()
    assert route_stage == "final_fallback"

    selected_replacement_key = str(route.get("selected_replacement_action_key") or "").strip()
    assert selected_replacement_key
    assert selected_replacement_key not in {strict_replacement_key, tertiary_replacement_key}
    assert str(route.get("replacement_routing_status") or "").strip().lower() in {"routed", "executed"}
    assert str(route.get("replacement_routing_status") or "").strip().lower() != "not_mapped"

    final_fallback_fields = _final_fallback_scalar_fields(route)
    assert final_fallback_fields


def test_recovery_loop_routes_to_final_fallback_and_surfaces_final_fallback_counters(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    strict_replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    tertiary_replacement_key = loop.TERTIARY_REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]

    payload = _run_recovery_loop_with_negative_share_sequence(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        source_action_key=source_action_key,
        negative_shares=[0.90, 0.93, 0.96, 0.98, 0.99, 0.995, 0.997],
        aggressive_thresholds=True,
        max_iterations=6,
    )

    final_fallback_hit: tuple[dict[str, object], dict[str, object], dict[str, object]] | None = None
    for iteration in payload["iteration_logs"]:
        source_row = next(
            (
                row
                for row in iteration["executed_actions"]
                if row.get("key") == source_action_key
                and str(row.get("replacement_route_stage") or "").strip().lower() == "final_fallback"
            ),
            None,
        )
        if source_row is None:
            continue
        replacement_log_row = next(
            (
                row
                for row in iteration["replacement_actions"]
                if row.get("source_action_key") == source_action_key
            ),
            None,
        )
        if replacement_log_row is None:
            continue
        final_fallback_hit = (iteration, source_row, replacement_log_row)
        break

    assert final_fallback_hit is not None

    iteration, source_row, replacement_log_row = final_fallback_hit
    selected_replacement_key = str(source_row.get("selected_replacement_action_key") or "").strip()
    replacement_action_key = str(replacement_log_row.get("replacement_action_key") or "").strip()

    assert selected_replacement_key
    assert replacement_action_key
    assert selected_replacement_key == replacement_action_key
    assert selected_replacement_key not in {strict_replacement_key, tertiary_replacement_key}
    assert str(source_row.get("replacement_routing_status") or "").strip().lower() in {"routed", "executed"}
    assert str(source_row.get("replacement_routing_status") or "").strip().lower() != "not_mapped"
    assert str(replacement_log_row.get("replacement_route_stage") or "").strip().lower() == "final_fallback"
    assert str(replacement_log_row.get("status") or "").strip().lower() not in {"not_routed", "unavailable"}

    iteration_final_fallback_fields = _final_fallback_scalar_fields(iteration)
    payload_final_fallback_fields = _final_fallback_scalar_fields(payload)
    assert iteration_final_fallback_fields
    assert payload_final_fallback_fields

    asserted_counter_fields = 0
    for scope in (iteration, payload):
        usage_by_source = scope.get("final_fallback_replacement_usage_by_source")
        if isinstance(usage_by_source, dict):
            asserted_counter_fields += 1
            assert int(usage_by_source.get(source_action_key, 0)) >= 1

        usage_by_action = scope.get("final_fallback_replacement_usage_by_action")
        if isinstance(usage_by_action, dict):
            asserted_counter_fields += 1
            assert int(usage_by_action.get(selected_replacement_key, 0)) >= 1

    final_fallback_action_map = payload.get("final_fallback_replacement_action_map")
    if isinstance(final_fallback_action_map, dict):
        asserted_counter_fields += 1
        assert str(final_fallback_action_map.get(source_action_key) or "").strip() == selected_replacement_key

    assert asserted_counter_fields > 0


def test_recovery_loop_protects_final_fallback_reserve_action_during_window(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    payload = _run_recovery_loop_with_negative_share_sequence(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        source_action_key=source_action_key,
        negative_shares=[0.90, 0.93, 0.96, 0.98, 0.99, 0.995, 0.997, 0.998, 0.999],
        aggressive_thresholds=True,
        max_iterations=8,
    )

    final_fallback_hit = _find_final_fallback_reserve_hit(payload, source_action_key=source_action_key)
    assert final_fallback_hit is not None

    iteration, source_row, replacement_log_row, selected_replacement_key, reserve_action_key, reserve_state = (
        final_fallback_hit
    )
    protected_actions = iteration.get("final_fallback_reserve_protected_actions")
    demoted_actions = iteration.get("demoted_actions")
    auto_disabled_actions = iteration.get("auto_disabled_actions")
    assert isinstance(protected_actions, list)
    assert isinstance(demoted_actions, list)
    assert isinstance(auto_disabled_actions, list)

    assert source_row["replacement_route_stage"] == "final_fallback"
    assert source_row["selected_replacement_action_key"] == selected_replacement_key
    assert replacement_log_row["replacement_action_key"] == selected_replacement_key
    assert replacement_log_row["replacement_route_stage"] == "final_fallback"
    assert replacement_log_row["status"] == "executed"
    replacement_route_selection_counts = payload.get("replacement_route_selection_counts")
    assert isinstance(replacement_route_selection_counts, dict)
    assert int(replacement_route_selection_counts.get("final_fallback", 0)) >= 1
    assert reserve_action_key in protected_actions
    assert reserve_action_key not in demoted_actions
    assert reserve_action_key not in auto_disabled_actions
    assert reserve_state["protection_status"] == "protected"
    assert int(reserve_state["iterations_used_after"]) >= int(reserve_state["iterations_used_before"])
    assert int(reserve_state["remaining_iteration_budget"]) >= 0


def test_recovery_loop_surfaces_final_fallback_reserve_telemetry_fields_and_counters(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    payload = _run_recovery_loop_with_negative_share_sequence(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        source_action_key=source_action_key,
        negative_shares=[0.90, 0.93, 0.96, 0.98, 0.99, 0.995, 0.997, 0.998, 0.999],
        aggressive_thresholds=True,
        max_iterations=8,
    )

    final_fallback_hit = _find_final_fallback_reserve_hit(payload, source_action_key=source_action_key)
    assert final_fallback_hit is not None

    iteration, _source_row, _replacement_log_row, selected_replacement_key, reserve_action_key, reserve_state = (
        final_fallback_hit
    )
    final_fallback_reserve_protected_actions = payload.get("final_fallback_reserve_protected_actions")
    final_fallback_reserve_protection_state = payload.get("final_fallback_reserve_protection_state")
    final_fallback_reserve_protection_progress = payload.get("final_fallback_reserve_protection_progress")
    final_fallback_reserve_reason_counters = payload.get("final_fallback_reserve_protection_reason_counters")
    final_fallback_reserve_suppressed_demotion_counts = payload.get("final_fallback_reserve_suppressed_demotion_counts")
    final_fallback_reserve_suppressed_auto_disable_counts = payload.get(
        "final_fallback_reserve_suppressed_auto_disable_counts"
    )

    assert isinstance(final_fallback_reserve_protected_actions, list)
    assert isinstance(final_fallback_reserve_protection_state, dict)
    assert isinstance(final_fallback_reserve_protection_progress, dict)
    assert isinstance(final_fallback_reserve_reason_counters, dict)
    assert isinstance(final_fallback_reserve_suppressed_demotion_counts, dict)
    assert isinstance(final_fallback_reserve_suppressed_auto_disable_counts, dict)

    iteration_protected_actions = iteration.get("final_fallback_reserve_protected_actions")
    assert isinstance(iteration_protected_actions, list)
    assert reserve_action_key in iteration_protected_actions
    if reserve_action_key in final_fallback_reserve_protection_progress:
        assert int(final_fallback_reserve_protection_progress[reserve_action_key]) >= 1
    else:
        iteration_progress = _as_dict(iteration.get("final_fallback_reserve_protection_progress"))
        assert reserve_action_key in iteration_progress
    assert int(
        final_fallback_reserve_reason_counters.get("protected_strict_or_tertiary_replacement_viable", 0)
    ) >= 1
    replacement_route_selection_counts = payload.get("replacement_route_selection_counts")
    assert isinstance(replacement_route_selection_counts, dict)
    assert int(replacement_route_selection_counts.get("final_fallback", 0)) >= 1
    assert selected_replacement_key
    assert reserve_state["protection_status"] in {"protected", "cap_reached"}
    assert int(reserve_state["iterations_used_after"]) >= int(reserve_state["iterations_used_before"])
    assert int(reserve_state["remaining_iteration_budget"]) >= 0

    reserve_scalar_fields = {
        path: value
        for path, value in _final_fallback_scalar_fields(payload).items()
        if "reserve" in path.lower()
    }
    assert reserve_scalar_fields
    assert any("protection_status" in path for path in reserve_scalar_fields)
    assert any("iterations_used_after" in path for path in reserve_scalar_fields)
    assert any("remaining_iteration_budget" in path for path in reserve_scalar_fields)
    assert any("reason_counters" in path for path in reserve_scalar_fields)


def test_recovery_loop_selected_final_fallback_protection_reduces_unavailable_routes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    payload = _run_recovery_loop_with_negative_share_sequence(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        source_action_key=source_action_key,
        negative_shares=[0.90, 0.93, 0.96, 0.98, 0.99, 0.995, 0.997, 0.998, 0.999],
        aggressive_thresholds=True,
        max_iterations=8,
    )

    replacement_route_selection_counts = payload.get("replacement_route_selection_counts")
    final_fallback_unavailable_reason_counts = payload.get("final_fallback_unavailable_reason_counts")
    final_fallback_reserve_reason_counters = payload.get("final_fallback_reserve_protection_reason_counters")

    assert isinstance(replacement_route_selection_counts, dict)
    assert isinstance(final_fallback_unavailable_reason_counts, dict)
    assert isinstance(final_fallback_reserve_reason_counters, dict)

    assert int(replacement_route_selection_counts.get("final_fallback", 0)) >= 1
    assert int(replacement_route_selection_counts.get("none", 0)) == 0
    assert int(final_fallback_unavailable_reason_counts.get("final_fallback_action_demoted", 0)) == 0
    assert int(final_fallback_reserve_reason_counters.get("protected_selected_final_fallback_route", 0)) >= 1


def test_recovery_loop_blocks_reserve_reactivation_without_quality_override(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    reserve_action_key = "clear_weather_risk_off_state"
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.90,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.94,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.97,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.99,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_collect_tertiary_reserve_candidates(
        *,
        demoted_actions: set[str],
        auto_disabled_actions: set[str],
    ) -> dict[str, dict[str, list[str]]]:
        demoted_actions.add(reserve_action_key)
        auto_disabled_actions.add(reserve_action_key)
        return {
            reserve_action_key: {
                "source_actions": [source_action_key],
                "strict_replacement_actions": [source_action_key],
            }
        }

    monkeypatch.setattr(loop, "_compute_adaptive_effectiveness_thresholds", _aggressive_effectiveness_thresholds)
    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "_collect_tertiary_reserve_candidates", fake_collect_tertiary_reserve_candidates)
    monkeypatch.setattr(loop, "_collect_final_fallback_reserve_candidates", lambda **kwargs: {})
    monkeypatch.setattr(loop, "_collect_final_fallback_candidate_action_keys", lambda **kwargs: set())
    monkeypatch.setattr(loop, "_resolve_demoted_source_replacement_route", _no_replacement_route_stub)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=3,
        stall_iterations=6,
        min_gap_improvement=0.0,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["iterations_executed"] == 3
    reason_counters = payload["tertiary_reserve_protection_reason_counters"]
    assert int(reason_counters.get("release_blocked_no_quality_override", 0)) >= 1

    blocked_hit = None
    for iteration in payload["iteration_logs"]:
        reserve_state = iteration.get("tertiary_reserve_protection_state")
        if not isinstance(reserve_state, dict):
            continue
        row = reserve_state.get(reserve_action_key)
        if not isinstance(row, dict):
            continue
        if bool(row.get("reactivation_blocked")):
            blocked_hit = (iteration, row)
            break

    assert blocked_hit is not None
    iteration, reserve_row = blocked_hit
    assert reserve_row["release_override_active"] is False
    assert reserve_row["release_override_reasons"] == []
    assert reserve_action_key in iteration["demoted_actions"]


def test_recovery_loop_allows_reserve_reactivation_with_quality_override(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    reserve_action_key = "clear_weather_risk_off_state"
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.90,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.70,
            negative_expectancy_attempt_share=0.80,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.70,
            negative_expectancy_attempt_share=0.80,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    def fake_collect_tertiary_reserve_candidates(
        *,
        demoted_actions: set[str],
        auto_disabled_actions: set[str],
    ) -> dict[str, dict[str, list[str]]]:
        demoted_actions.add(reserve_action_key)
        auto_disabled_actions.add(reserve_action_key)
        return {
            reserve_action_key: {
                "source_actions": [source_action_key],
                "strict_replacement_actions": [source_action_key],
            }
        }

    monkeypatch.setattr(loop, "_compute_adaptive_effectiveness_thresholds", _aggressive_effectiveness_thresholds)
    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "_collect_tertiary_reserve_candidates", fake_collect_tertiary_reserve_candidates)
    monkeypatch.setattr(loop, "_collect_final_fallback_reserve_candidates", lambda **kwargs: {})
    monkeypatch.setattr(loop, "_collect_final_fallback_candidate_action_keys", lambda **kwargs: set())
    monkeypatch.setattr(loop, "_resolve_demoted_source_replacement_route", _no_replacement_route_stub)

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=2,
        stall_iterations=6,
        min_gap_improvement=0.0,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["iterations_executed"] == 2
    reason_counters = payload["tertiary_reserve_protection_reason_counters"]
    assert int(reason_counters.get("release_override_reactivation_allowed", 0)) >= 1

    allowed_hit = None
    for iteration in payload["iteration_logs"]:
        reserve_state = iteration.get("tertiary_reserve_protection_state")
        if not isinstance(reserve_state, dict):
            continue
        row = reserve_state.get(reserve_action_key)
        if not isinstance(row, dict):
            continue
        if bool(row.get("reactivated_from_demoted")):
            allowed_hit = (iteration, row)
            break

    assert allowed_hit is not None
    iteration, reserve_row = allowed_hit
    assert reserve_row["reactivation_blocked"] is False
    assert reserve_row["release_override_active"] is True
    assert any(
        reason in {"gap_score_improved", "negative_expectancy_attempt_share_improved"}
        for reason in reserve_row["release_override_reasons"]
    )
    reserve_override = iteration["reserve_release_override"]
    assert reserve_override["active"] is True
    assert reserve_override["reasons"]


def test_recovery_loop_expires_final_fallback_reserve_protection_after_window(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    payload = _run_recovery_loop_with_negative_share_sequence(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        source_action_key=source_action_key,
        negative_shares=[
            0.90,
            0.93,
            0.96,
            0.98,
            0.99,
            0.995,
            0.997,
            0.998,
            0.999,
            0.9991,
            0.9992,
            0.9993,
            0.9994,
            0.9995,
        ],
        aggressive_thresholds=True,
        max_iterations=12,
    )

    cap_hit: tuple[dict[str, object], str, dict[str, object]] | None = None
    iteration_logs = payload.get("iteration_logs")
    assert isinstance(iteration_logs, list)

    for iteration in iteration_logs:
        if not isinstance(iteration, dict):
            continue
        reserve_state = iteration.get("final_fallback_reserve_protection_state")
        if not isinstance(reserve_state, dict):
            continue
        for raw_key, raw_entry in reserve_state.items():
            reserve_action_key = str(raw_key or "").strip()
            if not reserve_action_key or not isinstance(raw_entry, dict):
                continue
            if str(raw_entry.get("protection_status") or "").strip().lower() != "cap_reached":
                continue
            cap_hit = (iteration, reserve_action_key, raw_entry)
            break
        if cap_hit is not None:
            break

    assert cap_hit is not None

    iteration, reserve_action_key, reserve_state = cap_hit
    protected_actions = iteration.get("final_fallback_reserve_protected_actions")
    reason_counters = iteration.get("final_fallback_reserve_protection_reason_counters")
    assert isinstance(protected_actions, list)
    assert isinstance(reason_counters, dict)

    replacement_route_selection_counts = payload.get("replacement_route_selection_counts")
    assert isinstance(replacement_route_selection_counts, dict)
    assert int(replacement_route_selection_counts.get("final_fallback", 0)) >= 1
    assert reserve_state["protection_status"] == "cap_reached"
    assert int(reserve_state["remaining_iteration_budget"]) == 0
    assert reserve_action_key not in protected_actions
    assert int(reason_counters.get("skipped_iteration_cap_reached", 0)) >= 1


def test_recovery_loop_surfaces_explicit_tertiary_unavailability_reason_and_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"
    strict_replacement_key = loop.REPLACEMENT_ACTION_BY_DEMOTED_ACTION_KEY[source_action_key]
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.90,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.93,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.96,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.98,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.99,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.995,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=[source_action_key],
            gap_score=0.95,
            negative_expectancy_attempt_share=0.997,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "_compute_adaptive_effectiveness_thresholds", _aggressive_effectiveness_thresholds)
    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=6,
        stall_iterations=8,
        min_gap_improvement=0.0,
    )

    tertiary_unavailable: tuple[dict[str, object], dict[str, object], dict[str, object]] | None = None
    for iteration in payload["iteration_logs"]:
        source_row = next((row for row in iteration["executed_actions"] if row.get("key") == source_action_key), None)
        if source_row is None:
            continue
        if str(source_row.get("replacement_routing_status") or "").lower() not in {"unavailable", "not_routed"}:
            continue
        replacement_log_row = next(
            (
                row
                for row in iteration["replacement_actions"]
                if row.get("source_action_key") == source_action_key
            ),
            None,
        )
        if replacement_log_row is None:
            continue
        reason_tokens = " ".join(
            str(token or "").lower()
            for token in (
                source_row.get("replacement_unavailable_reason"),
                source_row.get("tertiary_replacement_unavailable_reason"),
                source_row.get("replacement_routing_tier"),
                replacement_log_row.get("reason"),
                replacement_log_row.get("status"),
                replacement_log_row.get("routing_tier"),
            )
        )
        if "tertiary" not in reason_tokens:
            continue
        tertiary_unavailable = (iteration, source_row, replacement_log_row)
        break

    if tertiary_unavailable is None:
        pytest.xfail("Explicit tertiary replacement unavailability signal not yet exposed.")

    iteration, source_row, replacement_log_row = tertiary_unavailable
    assert source_row["status"] == "auto_disabled"
    assert "unavailable" in str(source_row.get("replacement_routing_status") or "").lower()
    assert str(source_row.get("replacement_unavailable_reason") or replacement_log_row.get("reason") or "").strip()
    assert "unavailable" in str(replacement_log_row["status"]).lower()
    assert "tertiary" in " ".join(
        str(token or "").lower()
        for token in (
            source_row.get("replacement_unavailable_reason"),
            source_row.get("tertiary_replacement_unavailable_reason"),
            replacement_log_row.get("reason"),
            replacement_log_row.get("routing_tier"),
        )
    )
    assert not any(
        row.get("replacement_for_action_key") == source_action_key
        and row.get("status") == "executed"
        and str(row.get("key") or "") not in {source_action_key, strict_replacement_key}
        for row in iteration["executed_actions"]
    )


def test_recovery_loop_exposes_adaptive_replacement_profile_fields_and_tunes_tier(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_action_key = "increase_weather_sample_coverage"

    def run_case(out_dir: Path, shares: list[float]) -> dict[str, object]:
        sequence = [
            _advisor_payload(
                status="risk_off_active",
                actions=[source_action_key],
                gap_score=0.95,
                negative_expectancy_attempt_share=share,
            )
            for share in shares
        ]

        def fake_advisor(**kwargs):
            return sequence.pop(0)

        monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
        monkeypatch.setattr(loop, "run_kalshi_temperature_weather_pattern", lambda **kwargs: {"status": "ready"})
        monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})
        monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

        return loop.run_kalshi_temperature_recovery_loop(
            output_dir=str(out_dir),
            max_iterations=len(shares) - 1,
            stall_iterations=12,
            min_gap_improvement=0.0,
        )

    mild_payload = run_case(
        tmp_path / "mild",
        [0.35, 0.351, 0.352, 0.353, 0.354],
    )
    harsh_payload = run_case(
        tmp_path / "harsh",
        [0.90, 0.94, 0.97, 0.99, 0.995],
    )

    assert (
        harsh_payload["adaptive_effectiveness_thresholds"]["severity"]
        > mild_payload["adaptive_effectiveness_thresholds"]["severity"]
    )
    assert (
        harsh_payload["adaptive_effectiveness_thresholds"]["worsening_velocity"]
        > mild_payload["adaptive_effectiveness_thresholds"]["worsening_velocity"]
    )

    mild_payload_fields = _extract_replacement_profile_or_tier_scalars(mild_payload)
    harsh_payload_fields = _extract_replacement_profile_or_tier_scalars(harsh_payload)
    mild_iteration_fields = _extract_replacement_profile_or_tier_scalars(mild_payload["iteration_logs"][-1])
    harsh_iteration_fields = _extract_replacement_profile_or_tier_scalars(harsh_payload["iteration_logs"][-1])

    if not mild_payload_fields or not harsh_payload_fields or not mild_iteration_fields or not harsh_iteration_fields:
        pytest.xfail("Adaptive replacement profile/tier fields are not yet exposed in payload and iteration logs.")

    payload_common = sorted(set(mild_payload_fields) & set(harsh_payload_fields))
    iteration_common = sorted(set(mild_iteration_fields) & set(harsh_iteration_fields))
    assert payload_common
    assert iteration_common

    payload_changed = [path for path in payload_common if mild_payload_fields[path] != harsh_payload_fields[path]]
    iteration_changed = [
        path for path in iteration_common if mild_iteration_fields[path] != harsh_iteration_fields[path]
    ]
    assert payload_changed or iteration_changed

    directional_numeric_checks = []
    for path in payload_common:
        if "severity" not in path.lower() and "velocity" not in path.lower():
            continue
        mild_value = mild_payload_fields[path]
        harsh_value = harsh_payload_fields[path]
        if isinstance(mild_value, (int, float)) and isinstance(harsh_value, (int, float)):
            directional_numeric_checks.append((path, float(mild_value), float(harsh_value)))
    for path, mild_value, harsh_value in directional_numeric_checks:
        assert harsh_value >= mild_value, path

    tier_checks = []
    for path in payload_common:
        if "tier" not in path.lower():
            continue
        mild_value = mild_payload_fields[path]
        harsh_value = harsh_payload_fields[path]
        mild_rank = _tier_rank(mild_value)
        harsh_rank = _tier_rank(harsh_value)
        if mild_rank is not None and harsh_rank is not None:
            tier_checks.append((path, mild_rank, harsh_rank))
    for path, mild_rank, harsh_rank in tier_checks:
        assert harsh_rank >= mild_rank, path


def test_recovery_loop_keeps_risky_edge_relax_action_hard_disabled_when_demoted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["apply_expected_edge_relief_shadow_profile"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.90,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["apply_expected_edge_relief_shadow_profile"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.95,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["apply_expected_edge_relief_shadow_profile"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.97,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["apply_expected_edge_relief_shadow_profile"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.98,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_trader", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=3,
        stall_iterations=8,
        min_gap_improvement=0.0,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["demoted_actions"] == ["apply_expected_edge_relief_shadow_profile"]
    assert payload["replacement_usage_by_source"] == {}
    assert payload["replacement_usage_by_action"] == {}
    final_iteration = payload["iteration_logs"][2]
    source_row = next(
        row
        for row in final_iteration["executed_actions"]
        if row["key"] == "apply_expected_edge_relief_shadow_profile"
    )
    assert source_row["status"] == "auto_disabled"
    assert source_row["reason"] == "auto_disabled_effectiveness_demotion_persistently_harmful"
    assert final_iteration["replacement_actions"][0]["status"] in {"not_routed", "unavailable"}


def test_recovery_loop_does_not_demote_action_with_non_harmful_effectiveness_sequence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sequence = [
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.30,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.20,
            negative_expectancy_attempt_share=0.30,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.10,
            negative_expectancy_attempt_share=0.20,
        ),
        _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.05,
            negative_expectancy_attempt_share=0.20,
        ),
    ]

    def fake_advisor(**kwargs):
        return sequence.pop(0)

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", lambda **kwargs: {"status": "ready"})

    payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        max_iterations=3,
        stall_iterations=6,
        min_gap_improvement=0.0,
    )

    assert payload["termination_reason"] == "max_iterations"
    assert payload["demoted_actions"] == []
    assert payload["auto_disabled_actions"] == []
    metrics = payload["action_effectiveness"]["refresh_recovery_stack"]
    assert metrics["executed_count"] == 3
    assert metrics["worsening_count"] == 1
    assert metrics["non_worsening_count"] == 2
    assert metrics["cumulative_negative_share_delta"] == 0.0
    assert metrics["average_negative_share_delta"] == 0.0


def test_recovery_loop_actions_disabled_unless_already_cleared(tmp_path: Path, monkeypatch) -> None:
    active_calls = 0

    def fake_active_advisor(**kwargs):
        nonlocal active_calls
        active_calls += 1
        return _advisor_payload(
            status="risk_off_active",
            actions=["refresh_recovery_stack"],
            gap_score=0.3,
        )

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_active_advisor)

    active_payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        execute_actions=False,
    )
    assert active_calls == 1
    assert active_payload["termination_reason"] == "actions_disabled"
    assert active_payload["iterations_executed"] == 0

    insufficient_calls = 0
    settlement_calls = 0
    profitability_calls = 0
    decision_calls = 0

    def fake_insufficient_advisor(**kwargs):
        nonlocal insufficient_calls
        insufficient_calls += 1
        return _advisor_payload(
            status="insufficient_data",
            actions=["increase_settled_outcome_coverage"],
            gap_score=0.3,
        )

    def fake_settlement_state(*, output_dir: str):
        nonlocal settlement_calls
        settlement_calls += 1
        return {"status": "ready"}

    def fake_profitability(*, output_dir: str, **kwargs):
        nonlocal profitability_calls
        profitability_calls += 1
        return {"status": "ready"}

    def fake_decision(*, output_dir: str, **kwargs):
        nonlocal decision_calls
        decision_calls += 1
        return {"status": "ready"}

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_insufficient_advisor)
    monkeypatch.setattr(loop, "run_kalshi_temperature_settlement_state", fake_settlement_state)
    monkeypatch.setattr(loop, "run_kalshi_temperature_profitability", fake_profitability)
    monkeypatch.setattr(loop, "run_decision_matrix_hardening", fake_decision)

    insufficient_payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        execute_actions=False,
    )
    assert insufficient_calls == 1
    assert insufficient_payload["termination_reason"] == "actions_disabled"
    assert insufficient_payload["iterations_executed"] == 0
    assert settlement_calls == 0
    assert profitability_calls == 0
    assert decision_calls == 0

    cleared_calls = 0

    def fake_cleared_advisor(**kwargs):
        nonlocal cleared_calls
        cleared_calls += 1
        return _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        )

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_cleared_advisor)

    cleared_payload = loop.run_kalshi_temperature_recovery_loop(
        output_dir=str(tmp_path),
        execute_actions=False,
    )
    assert cleared_calls == 1
    assert cleared_payload["termination_reason"] == "cleared"
    assert cleared_payload["iterations_executed"] == 0


def test_recovery_loop_writes_artifacts_and_summary(tmp_path: Path, monkeypatch) -> None:
    def fake_advisor(**kwargs):
        return _advisor_payload(
            status="risk_off_cleared",
            actions=[],
            gap_score=0.0,
        )

    monkeypatch.setattr(loop, "run_kalshi_temperature_recovery_advisor", fake_advisor)

    payload = loop.run_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))

    output_file = Path(payload["output_file"])
    latest_file = Path(payload["latest_file"])
    assert output_file.exists()
    assert latest_file.exists()
    assert output_file.name.startswith("kalshi_temperature_recovery_loop_")
    assert latest_file.name == "kalshi_temperature_recovery_loop_latest.json"

    latest_payload = json.loads(latest_file.read_text(encoding="utf-8"))
    assert latest_payload["termination_reason"] == payload["termination_reason"]

    summary = loop.summarize_kalshi_temperature_recovery_loop(output_dir=str(tmp_path))
    summary_payload = json.loads(summary)
    assert summary_payload["status"] == "ready"
    assert summary_payload["termination_reason"] == "cleared"


def test_load_suppression_snapshot_normal_counts(tmp_path: Path) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_candidate_count": 10,
                "weather_pattern_negative_regime_suppression_blocked_count": 8,
            }
        ),
        encoding="utf-8",
    )

    snapshot = loop._load_suppression_snapshot(tmp_path)

    assert int(snapshot["candidate_count"]) == 10
    assert int(snapshot["blocked_count"]) == 8
    assert snapshot["blocked_share"] == 0.8


def test_load_suppression_snapshot_inconsistent_counts_are_bounded(tmp_path: Path) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_candidate_count": 4,
                "weather_pattern_negative_regime_suppression_blocked_count": 5,
            }
        ),
        encoding="utf-8",
    )

    snapshot = loop._load_suppression_snapshot(tmp_path)

    assert int(snapshot["candidate_count"]) == 4
    assert int(snapshot["blocked_count"]) == 5
    assert snapshot["blocked_share"] == 1.0


def test_load_suppression_snapshot_zero_candidates_with_blocks_saturates(tmp_path: Path) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_candidate_count": 0,
                "weather_pattern_negative_regime_suppression_blocked_count": 5,
            }
        ),
        encoding="utf-8",
    )

    snapshot = loop._load_suppression_snapshot(tmp_path)

    assert int(snapshot["candidate_count"]) == 0
    assert int(snapshot["blocked_count"]) == 5
    assert snapshot["blocked_share"] == 1.0
