from __future__ import annotations

import json
from pathlib import Path

from betbot import kalshi_temperature_recovery_campaign as campaign


def _write_decision_matrix_hardening_artifact(
    tmp_path: Path,
    *,
    payload: dict[str, object],
    latest: bool = True,
    filename: str = "decision_matrix_hardening_20260423_120000.json",
) -> Path:
    health_dir = tmp_path / "health"
    health_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = health_dir / filename
    artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    if latest:
        (health_dir / "decision_matrix_hardening_latest.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    return artifact_path


def _loop_payload(
    *,
    termination_reason: str,
    iterations_executed: int,
    initial_gap_score: float,
    final_gap_score: float,
    final_advisor_status: str,
    negative_share: float,
    stale_negative_share: float,
    stale_share: float,
    output_file: str,
    final_intents_total: int = 0,
    final_intents_approved: int = 0,
    policy_reason_counts: dict[str, int] | None = None,
    final_actions: list[str] | None = None,
    decision_matrix_metrics: dict[str, object] | None = None,
    recovery_watchdog_metrics: dict[str, object] | None = None,
    recovery_effectiveness_metrics: dict[str, object] | None = None,
) -> dict[str, object]:
    safe_policy_reason_counts = (
        {str(key): int(value) for key, value in policy_reason_counts.items()}
        if isinstance(policy_reason_counts, dict)
        else {}
    )
    safe_decision_matrix_metrics = (
        dict(decision_matrix_metrics)
        if isinstance(decision_matrix_metrics, dict)
        else {}
    )
    safe_recovery_watchdog_metrics = (
        dict(recovery_watchdog_metrics)
        if isinstance(recovery_watchdog_metrics, dict)
        else {}
    )
    safe_recovery_effectiveness_metrics = (
        dict(recovery_effectiveness_metrics)
        if isinstance(recovery_effectiveness_metrics, dict)
        else {}
    )
    return {
        "termination_reason": termination_reason,
        "iterations_executed": iterations_executed,
        "initial_gap_score": initial_gap_score,
        "final_gap_score": final_gap_score,
        "final_advisor_status": final_advisor_status,
        "output_file": output_file,
        "final_advisor": {
            "remediation_plan": {
                "status": final_advisor_status,
                "prioritized_actions": [{"key": str(key)} for key in (final_actions or [])],
            },
            "metrics": {
                "weather": {
                    "negative_expectancy_attempt_share": negative_share,
                    "stale_metar_negative_attempt_share": stale_negative_share,
                    "stale_metar_attempt_share": stale_share,
                },
                "trade_plan_blockers": {
                    "intents_total": int(final_intents_total),
                    "intents_approved": int(final_intents_approved),
                    "policy_reason_counts": safe_policy_reason_counts,
                },
                "decision_matrix": safe_decision_matrix_metrics,
                "recovery_watchdog": safe_recovery_watchdog_metrics,
                "recovery_effectiveness": safe_recovery_effectiveness_metrics,
            },
        },
    }


def test_recovery_campaign_selects_profile_with_best_gap_improvement(
    tmp_path: Path,
    monkeypatch,
) -> None:
    by_name = {
        "steady_4x2": _loop_payload(
            termination_reason="stalled",
            iterations_executed=4,
            initial_gap_score=1.0,
            final_gap_score=0.70,
            final_advisor_status="risk_off_active",
            negative_share=0.65,
            stale_negative_share=0.71,
            stale_share=0.73,
            output_file=str(tmp_path / "steady.json"),
        ),
        "extended_6x3": _loop_payload(
            termination_reason="max_iterations",
            iterations_executed=6,
            initial_gap_score=1.0,
            final_gap_score=0.35,
            final_advisor_status="risk_off_active",
            negative_share=0.58,
            stale_negative_share=0.63,
            stale_share=0.67,
            output_file=str(tmp_path / "extended.json"),
        ),
        "focused_3x2": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.55,
            final_advisor_status="risk_off_active",
            negative_share=0.62,
            stale_negative_share=0.69,
            stale_share=0.70,
            output_file=str(tmp_path / "focused.json"),
        ),
    }

    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        if max_iterations == 4:
            return by_name["steady_4x2"]
        if max_iterations == 6:
            return by_name["extended_6x3"]
        return by_name["focused_3x2"]

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)

    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))

    best_profile = payload["best_profile"]
    assert best_profile["name"] == "extended_6x3"
    assert float(best_profile["gap_improvement_abs"]) == 0.65
    assert float(best_profile["final_gap_score"]) == 0.35
    assert best_profile["final_weather_negative_expectancy_attempt_share"] == 0.58


def test_recovery_campaign_adapts_default_profiles_when_decision_matrix_shows_weak_momentum_and_guardrail_active(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_decision_matrix_hardening_artifact(
        tmp_path,
        payload={
            "status": "ready",
            "matrix_health_status": "red",
            "critical_blockers_count": 1,
            "supports_consistency_and_profitability": False,
            "blocking_factors": [
                {"key": "insufficient_settled_outcomes", "severity": "critical"},
            ],
            "observed_metrics": {
                "settled_outcome_throughput_status": "ready",
                "settled_outcome_throughput_source": "outputs/health/kalshi_temperature_settled_outcome_throughput_latest.json",
                "settled_outcome_throughput_growth_delta_24h": -2,
                "settled_outcome_throughput_growth_delta_7d": -1,
                "settled_outcome_throughput_combined_bucket_count_delta_24h": 0,
                "settled_outcome_throughput_combined_bucket_count_delta_7d": 0,
                "settled_outcome_throughput_targeted_constraint_rows": 88,
                "settled_outcome_throughput_top_bottlenecks_count": 12,
                "settled_outcome_throughput_bottleneck_source": "constraint_station_bootstrap",
                "settled_outcome_growth_stalled": True,
            },
            "data_sources": {
                "settled_outcome_throughput": {
                    "status": "ready",
                    "source": "outputs/health/kalshi_temperature_settled_outcome_throughput_latest.json",
                    "growth_deltas_settled_outcomes_delta_24h": -2,
                    "growth_deltas_settled_outcomes_delta_7d": -1,
                    "growth_deltas_combined_bucket_count_delta_24h": 0,
                    "growth_deltas_combined_bucket_count_delta_7d": 0,
                    "targeting_targeted_constraint_rows": 88,
                    "top_bottlenecks_count": 12,
                    "bottleneck_source": "constraint_station_bootstrap",
                },
            },
        },
    )

    loop_calls: list[dict[str, object]] = []

    def fake_loop(**kwargs):
        loop_calls.append(dict(kwargs))
        run_index = len(loop_calls)
        return _loop_payload(
            termination_reason="stalled",
            iterations_executed=int(kwargs["max_iterations"]),
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.66,
            output_file=str(tmp_path / f"adaptive_run_{run_index}.json"),
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)

    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))

    assert [int(call["max_iterations"]) for call in loop_calls] == [6, 8, 5]
    assert [int(call["stall_iterations"]) for call in loop_calls] == [3, 4, 3]
    assert [round(float(call["min_gap_improvement"]), 6) for call in loop_calls] == [0.0035, 0.00175, 0.00525]
    adaptation = payload["profile_adaptation"]
    assert adaptation["mode"] == "acceleration"
    assert adaptation["adapted_profiles_used"] is True
    assert adaptation["source_artifact"].endswith("decision_matrix_hardening_latest.json")
    assert adaptation["derived_signals"]["guardrail_active"] is True
    assert adaptation["derived_signals"]["positive_streak"] is False
    env = payload["recommended_env_overrides"]["env"]
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MODE"] == "acceleration"
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTED_PROFILES_USED"] == "1"
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MOMENTUM_SCORE"] == "-9"


def test_recovery_campaign_bypasses_profile_adaptation_when_explicit_profiles_are_passed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_decision_matrix_hardening_artifact(
        tmp_path,
        payload={
            "status": "ready",
            "matrix_health_status": "green",
            "critical_blockers_count": 0,
            "supports_consistency_and_profitability": True,
            "blocking_factors": [],
            "observed_metrics": {
                "settled_outcome_throughput_status": "ready",
                "settled_outcome_throughput_source": "outputs/health/kalshi_temperature_settled_outcome_throughput_latest.json",
                "settled_outcome_throughput_growth_delta_24h": 4,
                "settled_outcome_throughput_growth_delta_7d": 9,
                "settled_outcome_throughput_combined_bucket_count_delta_24h": 3,
                "settled_outcome_throughput_combined_bucket_count_delta_7d": 5,
                "settled_outcome_throughput_targeted_constraint_rows": 144,
                "settled_outcome_throughput_top_bottlenecks_count": 2,
                "settled_outcome_throughput_bottleneck_source": "constraint_station_bootstrap",
                "settled_outcome_growth_stalled": False,
            },
            "data_sources": {
                "settled_outcome_throughput": {
                    "status": "ready",
                    "source": "outputs/health/kalshi_temperature_settled_outcome_throughput_latest.json",
                    "growth_deltas_settled_outcomes_delta_24h": 4,
                    "growth_deltas_settled_outcomes_delta_7d": 9,
                    "growth_deltas_combined_bucket_count_delta_24h": 3,
                    "growth_deltas_combined_bucket_count_delta_7d": 5,
                    "targeting_targeted_constraint_rows": 144,
                    "top_bottlenecks_count": 2,
                    "bottleneck_source": "constraint_station_bootstrap",
                },
            },
        },
    )

    profiles = [
        {
            "name": "explicit_acceleration",
            "max_iterations": 9,
            "stall_iterations": 4,
            "min_gap_improvement": 0.004,
        },
        {
            "name": "explicit_confirmation",
            "max_iterations": 2,
            "stall_iterations": 1,
            "min_gap_improvement": 0.02,
        },
    ]
    loop_calls: list[dict[str, object]] = []

    def fake_loop(**kwargs):
        loop_calls.append(dict(kwargs))
        run_index = len(loop_calls)
        return _loop_payload(
            termination_reason="stalled",
            iterations_executed=int(kwargs["max_iterations"]),
            initial_gap_score=1.0,
            final_gap_score=0.55,
            final_advisor_status="risk_off_active",
            negative_share=0.50,
            stale_negative_share=0.55,
            stale_share=0.57,
            output_file=str(tmp_path / f"explicit_run_{run_index}.json"),
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)

    payload = campaign.run_kalshi_temperature_recovery_campaign(
        output_dir=str(tmp_path),
        profiles=profiles,
    )

    assert [int(call["max_iterations"]) for call in loop_calls] == [9, 2]
    assert [int(call["stall_iterations"]) for call in loop_calls] == [4, 1]
    assert [round(float(call["min_gap_improvement"]), 6) for call in loop_calls] == [0.004, 0.02]
    adaptation = payload["profile_adaptation"]
    assert adaptation["mode"] == "explicit_profiles"
    assert adaptation["adapted_profiles_used"] is False
    assert adaptation["derived_signals"]["guardrail_cleared"] is True
    assert payload["inputs"]["profiles"] == profiles
    env = payload["recommended_env_overrides"]["env"]
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MODE"] == "explicit_profiles"
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTED_PROFILES_USED"] == "0"
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MOMENTUM_SCORE"] == "16"


def test_recovery_campaign_prefers_cleared_status_over_bigger_raw_improvement(
    tmp_path: Path,
    monkeypatch,
) -> None:
    by_name = {
        "steady_4x2": _loop_payload(
            termination_reason="max_iterations",
            iterations_executed=4,
            initial_gap_score=1.0,
            final_gap_score=0.20,
            final_advisor_status="risk_off_active",
            negative_share=0.57,
            stale_negative_share=0.61,
            stale_share=0.64,
            output_file=str(tmp_path / "steady.json"),
        ),
        "extended_6x3": _loop_payload(
            termination_reason="cleared",
            iterations_executed=2,
            initial_gap_score=0.50,
            final_gap_score=0.40,
            final_advisor_status="risk_off_cleared",
            negative_share=0.40,
            stale_negative_share=0.42,
            stale_share=0.44,
            output_file=str(tmp_path / "extended.json"),
        ),
        "focused_3x2": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=0.9,
            final_gap_score=0.5,
            final_advisor_status="risk_off_active",
            negative_share=0.56,
            stale_negative_share=0.63,
            stale_share=0.66,
            output_file=str(tmp_path / "focused.json"),
        ),
    }

    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        if max_iterations == 4:
            return by_name["steady_4x2"]
        if max_iterations == 6:
            return by_name["extended_6x3"]
        return by_name["focused_3x2"]

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)

    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))

    best_profile = payload["best_profile"]
    assert best_profile["name"] == "extended_6x3"
    assert best_profile["final_advisor_status"] == "risk_off_cleared"
    overrides = payload["recommended_env_overrides"]
    assert overrides["profile_name"] == "extended_6x3"
    assert overrides["env"]["COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS"] == "6"


def test_recovery_campaign_handles_loop_payloads_with_stalled_settled_outcome_growth_actions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = [
        {
            "name": "growth_recovery_focus",
            "max_iterations": 2,
            "stall_iterations": 1,
            "min_gap_improvement": 0.001,
        }
    ]
    loop_calls: list[dict[str, object]] = []

    def fake_loop(**kwargs):
        loop_calls.append(dict(kwargs))
        return _loop_payload(
            termination_reason="max_iterations",
            iterations_executed=2,
            initial_gap_score=0.70,
            final_gap_score=0.45,
            final_advisor_status="risk_off_active",
            negative_share=0.21,
            stale_negative_share=0.19,
            stale_share=0.22,
            final_actions=[
                "increase_settled_outcome_coverage",
                "clear_weather_risk_off_state",
            ],
            output_file=str(tmp_path / "growth_recovery_focus_loop.json"),
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)

    payload = campaign.run_kalshi_temperature_recovery_campaign(
        output_dir=str(tmp_path),
        profiles=profiles,
    )

    assert len(loop_calls) == 1
    assert payload["profiles_evaluated"] == 1
    assert payload["best_profile"]["name"] == "growth_recovery_focus"
    assert payload["best_profile"]["final_advisor_status"] == "risk_off_active"
    assert float(payload["best_profile"]["gap_improvement_abs"]) == 0.25


def test_recovery_campaign_extracts_final_decision_matrix_blocker_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = [
        {
            "name": "decision_matrix_context_probe",
            "max_iterations": 2,
            "stall_iterations": 1,
            "min_gap_improvement": 0.001,
        }
    ]

    def fake_loop(**kwargs):
        return _loop_payload(
            termination_reason="max_iterations",
            iterations_executed=2,
            initial_gap_score=0.70,
            final_gap_score=0.45,
            final_advisor_status="risk_off_active",
            negative_share=0.30,
            stale_negative_share=0.32,
            stale_share=0.34,
            output_file=str(tmp_path / "decision_matrix_context_probe_loop.json"),
            decision_matrix_metrics={
                "settled_outcomes_insufficient": True,
                "critical_blockers_count": 2,
                "blockers": [
                    {"key": "settled_outcome_growth_stalled", "severity": "high"},
                    {"key": "weather_global_risk_off_recommended", "severity": "critical"},
                ],
            },
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(
        output_dir=str(tmp_path),
        profiles=profiles,
    )

    best_profile = payload["best_profile"]
    context = best_profile["final_decision_matrix_blocker_context"]
    assert context["source"] == "final_advisor.metrics.decision_matrix"
    assert context["settled_outcomes_insufficient"] is True
    assert context["settled_outcome_growth_stalled"] is True
    assert context["critical_blockers_count"] == 2
    assert best_profile["final_settled_outcomes_insufficient"] is True
    assert best_profile["final_settled_outcome_growth_stalled"] is True
    assert best_profile["final_critical_blockers_count"] == 2


def test_recovery_campaign_extracts_final_recovery_watchdog_stage_timeout_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = [
        {
            "name": "watchdog_context_probe",
            "max_iterations": 2,
            "stall_iterations": 1,
            "min_gap_improvement": 0.001,
        }
    ]

    def fake_loop(**kwargs):
        return _loop_payload(
            termination_reason="max_iterations",
            iterations_executed=2,
            initial_gap_score=0.70,
            final_gap_score=0.45,
            final_advisor_status="risk_off_active",
            negative_share=0.30,
            stale_negative_share=0.32,
            stale_share=0.34,
            output_file=str(tmp_path / "watchdog_context_probe_loop.json"),
            recovery_watchdog_metrics={
                "actions_attempted": [
                    "repair_coldmath_stage_timeout_guardrails:ok",
                    "repair_coldmath_stage_timeout_guardrails:missing_script",
                ],
            },
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(
        output_dir=str(tmp_path),
        profiles=profiles,
    )

    best_profile = payload["best_profile"]
    context = best_profile["final_recovery_watchdog_context"]
    assert context["source"] == "final_advisor.metrics.recovery_watchdog"
    assert (
        context["stage_timeout_repair_action"]
        == "repair_coldmath_stage_timeout_guardrails:missing_script"
    )
    assert context["stage_timeout_repair_status"] == "missing_script"
    assert context["severe_stage_timeout_repair"] is True
    assert (
        best_profile["final_stage_timeout_repair_action"]
        == "repair_coldmath_stage_timeout_guardrails:missing_script"
    )
    assert best_profile["final_stage_timeout_repair_status"] == "missing_script"
    assert best_profile["final_stage_timeout_repair_severe"] is True


def test_recovery_campaign_extracts_advisor_style_recovery_watchdog_stage_timeout_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = [
        {
            "name": "watchdog_context_advisor_style_probe",
            "max_iterations": 2,
            "stall_iterations": 1,
            "min_gap_improvement": 0.001,
        }
    ]

    def fake_loop(**kwargs):
        return _loop_payload(
            termination_reason="max_iterations",
            iterations_executed=2,
            initial_gap_score=0.70,
            final_gap_score=0.45,
            final_advisor_status="risk_off_active",
            negative_share=0.30,
            stale_negative_share=0.32,
            stale_share=0.34,
            output_file=str(tmp_path / "watchdog_context_advisor_style_probe_loop.json"),
            recovery_watchdog_metrics={
                "latest_stage_timeout_repair_action": "repair_coldmath_stage_timeout_guardrails:failed",
                "latest_stage_timeout_repair_status": "failed",
                "severe_issue": True,
            },
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(
        output_dir=str(tmp_path),
        profiles=profiles,
    )

    best_profile = payload["best_profile"]
    context = best_profile["final_recovery_watchdog_context"]
    assert context["source"] == "final_advisor.metrics.recovery_watchdog"
    assert (
        context["stage_timeout_repair_action"]
        == "repair_coldmath_stage_timeout_guardrails:failed"
    )
    assert context["stage_timeout_repair_status"] == "failed"
    assert context["severe_stage_timeout_repair"] is True
    assert (
        best_profile["final_stage_timeout_repair_action"]
        == "repair_coldmath_stage_timeout_guardrails:failed"
    )
    assert best_profile["final_stage_timeout_repair_status"] == "failed"
    assert best_profile["final_stage_timeout_repair_severe"] is True


def test_recovery_campaign_extracts_final_recovery_effectiveness_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = [
        {
            "name": "effectiveness_context_probe",
            "max_iterations": 2,
            "stall_iterations": 1,
            "min_gap_improvement": 0.001,
        }
    ]

    def fake_loop(**kwargs):
        return _loop_payload(
            termination_reason="max_iterations",
            iterations_executed=2,
            initial_gap_score=0.70,
            final_gap_score=0.45,
            final_advisor_status="risk_off_active",
            negative_share=0.30,
            stale_negative_share=0.32,
            stale_share=0.34,
            output_file=str(tmp_path / "effectiveness_context_probe_loop.json"),
            recovery_effectiveness_metrics={
                "demoted_actions": [
                    "retune_weather_targets",
                    "clear_weather_risk_off_state",
                ],
                "persistently_harmful_action_count": 2,
            },
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(
        output_dir=str(tmp_path),
        profiles=profiles,
    )

    best_profile = payload["best_profile"]
    context = best_profile["final_recovery_effectiveness_context"]
    assert context["source"] == "final_advisor.metrics.recovery_effectiveness"
    assert context["demoted_actions"] == [
        "retune_weather_targets",
        "clear_weather_risk_off_state",
    ]
    assert context["harmful_action_count"] == 2
    assert best_profile["final_effectiveness_demoted_actions"] == [
        "retune_weather_targets",
        "clear_weather_risk_off_state",
    ]
    assert best_profile["final_effectiveness_harmful_count"] == 2


def test_recovery_campaign_prefers_non_severe_stage_timeout_repair_status_under_tied_conditions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = [
        {
            "name": "failed_profile",
            "max_iterations": 4,
            "stall_iterations": 2,
            "min_gap_improvement": 0.005,
        },
        {
            "name": "missing_script_profile",
            "max_iterations": 6,
            "stall_iterations": 2,
            "min_gap_improvement": 0.005,
        },
        {
            "name": "healthy_profile",
            "max_iterations": 8,
            "stall_iterations": 2,
            "min_gap_improvement": 0.005,
        },
    ]
    by_name = {
        "failed_profile": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.67,
            final_intents_total=25,
            final_intents_approved=8,
            policy_reason_counts={"expected_edge_below_min": 1, "approved": 8},
            output_file=str(tmp_path / "failed_profile.json"),
            recovery_watchdog_metrics={
                "stage_timeout_repair_status": "failed",
            },
        ),
        "missing_script_profile": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.67,
            final_intents_total=25,
            final_intents_approved=8,
            policy_reason_counts={"expected_edge_below_min": 1, "approved": 8},
            output_file=str(tmp_path / "missing_script_profile.json"),
            recovery_watchdog_metrics={
                "stage_timeout_repair_status": "missing_script",
            },
        ),
        "healthy_profile": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.67,
            final_intents_total=25,
            final_intents_approved=8,
            policy_reason_counts={"expected_edge_below_min": 1, "approved": 8},
            output_file=str(tmp_path / "healthy_profile.json"),
            recovery_watchdog_metrics={
                "stage_timeout_repair_status": "ok",
            },
        ),
    }

    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        if max_iterations == 4:
            return by_name["failed_profile"]
        if max_iterations == 6:
            return by_name["missing_script_profile"]
        return by_name["healthy_profile"]

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(
        output_dir=str(tmp_path),
        profiles=profiles,
    )

    best_profile = payload["best_profile"]
    assert best_profile["name"] == "healthy_profile"
    assert best_profile["final_stage_timeout_repair_status"] == "ok"


def test_recovery_campaign_prefers_lower_effectiveness_harmful_count_under_tied_conditions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = [
        {
            "name": "high_harmful_profile",
            "max_iterations": 4,
            "stall_iterations": 2,
            "min_gap_improvement": 0.005,
        },
        {
            "name": "low_harmful_profile",
            "max_iterations": 6,
            "stall_iterations": 2,
            "min_gap_improvement": 0.005,
        },
    ]
    by_name = {
        "high_harmful_profile": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.67,
            final_intents_total=25,
            final_intents_approved=8,
            policy_reason_counts={"expected_edge_below_min": 1, "approved": 8},
            output_file=str(tmp_path / "high_harmful_profile.json"),
            recovery_watchdog_metrics={
                "stage_timeout_repair_status": "ok",
            },
            recovery_effectiveness_metrics={
                "persistently_harmful_actions": [
                    "increase_settled_outcome_coverage",
                    "clear_weather_risk_off_state",
                    "retune_weather_targets",
                ],
                "persistently_harmful_action_count": 3,
            },
        ),
        "low_harmful_profile": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.67,
            final_intents_total=25,
            final_intents_approved=8,
            policy_reason_counts={"expected_edge_below_min": 1, "approved": 8},
            output_file=str(tmp_path / "low_harmful_profile.json"),
            recovery_watchdog_metrics={
                "stage_timeout_repair_status": "ok",
            },
            recovery_effectiveness_metrics={
                "persistently_harmful_actions": [
                    "retune_weather_targets",
                ],
                "persistently_harmful_action_count": 1,
            },
        ),
    }

    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        if max_iterations == 4:
            return by_name["high_harmful_profile"]
        return by_name["low_harmful_profile"]

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(
        output_dir=str(tmp_path),
        profiles=profiles,
    )

    best_profile = payload["best_profile"]
    assert best_profile["name"] == "low_harmful_profile"
    assert best_profile["final_effectiveness_harmful_count"] == 1


def test_recovery_campaign_prefers_lower_decision_matrix_hard_block_pressure_under_insufficient_data_ties(
    tmp_path: Path,
    monkeypatch,
) -> None:
    by_name = {
        "steady_4x2": _loop_payload(
            termination_reason="insufficient_data",
            iterations_executed=4,
            initial_gap_score=1.0,
            final_gap_score=0.70,
            final_advisor_status="insufficient_data",
            negative_share=0.70,
            stale_negative_share=0.72,
            stale_share=0.74,
            final_intents_total=50,
            final_intents_approved=5,
            policy_reason_counts={"expected_edge_below_min": 5},
            output_file=str(tmp_path / "steady.json"),
            decision_matrix_metrics={
                "settled_outcomes_insufficient": True,
                "critical_blockers_count": 3,
                "blockers": [
                    {"key": "insufficient_settled_outcomes", "severity": "critical"},
                    {"key": "settled_outcome_growth_stalled", "severity": "high"},
                ],
            },
        ),
        "extended_6x3": _loop_payload(
            termination_reason="insufficient_data",
            iterations_executed=6,
            initial_gap_score=1.0,
            final_gap_score=0.70,
            final_advisor_status="insufficient_data",
            negative_share=0.70,
            stale_negative_share=0.72,
            stale_share=0.74,
            final_intents_total=50,
            final_intents_approved=5,
            policy_reason_counts={"expected_edge_below_min": 5},
            output_file=str(tmp_path / "extended.json"),
            decision_matrix_metrics={
                "settled_outcomes_insufficient": True,
                "critical_blockers_count": 4,
                "blockers": [
                    {"key": "insufficient_settled_outcomes", "severity": "critical"},
                ],
            },
        ),
        "focused_3x2": _loop_payload(
            termination_reason="insufficient_data",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.70,
            final_advisor_status="insufficient_data",
            negative_share=0.70,
            stale_negative_share=0.72,
            stale_share=0.74,
            final_intents_total=50,
            final_intents_approved=5,
            policy_reason_counts={"expected_edge_below_min": 5},
            output_file=str(tmp_path / "focused.json"),
            decision_matrix_metrics={
                "settled_outcomes_insufficient": True,
                "critical_blockers_count": 1,
                "blockers": [
                    {"key": "insufficient_settled_outcomes", "severity": "critical"},
                ],
            },
        ),
    }

    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        if max_iterations == 4:
            return by_name["steady_4x2"]
        if max_iterations == 6:
            return by_name["extended_6x3"]
        return by_name["focused_3x2"]

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))

    best_profile = payload["best_profile"]
    assert best_profile["name"] == "focused_3x2"
    assert best_profile["final_advisor_status"] == "insufficient_data"
    assert best_profile["final_settled_outcome_growth_stalled"] is False
    assert best_profile["final_critical_blockers_count"] == 1


def test_recovery_campaign_prefers_stronger_settled_throughput_momentum_under_insufficient_data_ties(
    tmp_path: Path,
    monkeypatch,
) -> None:
    profiles = [
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
    ]

    by_name = {
        "steady_4x2": _loop_payload(
            termination_reason="insufficient_data",
            iterations_executed=4,
            initial_gap_score=1.0,
            final_gap_score=0.70,
            final_advisor_status="insufficient_data",
            negative_share=0.70,
            stale_negative_share=0.72,
            stale_share=0.74,
            final_intents_total=50,
            final_intents_approved=5,
            policy_reason_counts={"expected_edge_below_min": 5},
            output_file=str(tmp_path / "steady.json"),
            decision_matrix_metrics={
                "settled_outcomes_insufficient": True,
                "critical_blockers_count": 1,
                "settled_outcomes_delta_24h": -0.5,
                "settled_outcomes_delta_7d": 1.0,
                "combined_bucket_count_delta_24h": 0,
                "combined_bucket_count_delta_7d": 1,
                "targeted_constraint_rows": 2,
                "top_bottlenecks_count": 1,
                "bottleneck_source": "constraint_station_bootstrap",
                "blockers": [
                    {"key": "insufficient_settled_outcomes", "severity": "critical"},
                ],
            },
        ),
        "extended_6x3": _loop_payload(
            termination_reason="insufficient_data",
            iterations_executed=6,
            initial_gap_score=1.0,
            final_gap_score=0.70,
            final_advisor_status="insufficient_data",
            negative_share=0.70,
            stale_negative_share=0.72,
            stale_share=0.74,
            final_intents_total=50,
            final_intents_approved=5,
            policy_reason_counts={"expected_edge_below_min": 5},
            output_file=str(tmp_path / "extended.json"),
            decision_matrix_metrics={
                "settled_outcomes_insufficient": True,
                "critical_blockers_count": 1,
                "settled_outcomes_delta_24h": 4.0,
                "settled_outcomes_delta_7d": 12.0,
                "combined_bucket_count_delta_24h": 3,
                "combined_bucket_count_delta_7d": 8,
                "targeted_constraint_rows": 18,
                "top_bottlenecks_count": 4,
                "bottleneck_source": "constraint_station_bootstrap",
                "blockers": [
                    {"key": "insufficient_settled_outcomes", "severity": "critical"},
                ],
            },
        ),
    }

    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        if max_iterations == 4:
            return by_name["steady_4x2"]
        return by_name["extended_6x3"]

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path), profiles=profiles)

    best_profile = payload["best_profile"]
    assert best_profile["name"] == "extended_6x3"
    assert best_profile["final_advisor_status"] == "insufficient_data"
    assert float(best_profile["final_settled_outcomes_delta_24h"]) == 4.0
    assert float(best_profile["final_settled_outcomes_delta_7d"]) == 12.0
    assert int(best_profile["final_combined_bucket_count_delta_24h"]) == 3
    assert int(best_profile["final_combined_bucket_count_delta_7d"]) == 8
    assert int(best_profile["final_targeted_constraint_rows"]) == 18
    assert int(best_profile["final_top_bottlenecks_count"]) == 4
    assert best_profile["final_bottleneck_source"] == "constraint_station_bootstrap"


def test_recovery_campaign_prefers_higher_approved_intents_when_status_and_gap_tie(
    tmp_path: Path,
    monkeypatch,
) -> None:
    by_name = {
        "steady_4x2": _loop_payload(
            termination_reason="stalled",
            iterations_executed=4,
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.66,
            final_intents_total=43,
            final_intents_approved=0,
            policy_reason_counts={"expected_edge_below_min": 25},
            output_file=str(tmp_path / "steady.json"),
        ),
        "extended_6x3": _loop_payload(
            termination_reason="stalled",
            iterations_executed=6,
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.66,
            final_intents_total=43,
            final_intents_approved=25,
            policy_reason_counts={"approved": 25},
            output_file=str(tmp_path / "extended.json"),
        ),
        "focused_3x2": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.50,
            final_advisor_status="risk_off_active",
            negative_share=0.60,
            stale_negative_share=0.65,
            stale_share=0.66,
            final_intents_total=43,
            final_intents_approved=5,
            policy_reason_counts={"expected_edge_below_min": 20, "approved": 5},
            output_file=str(tmp_path / "focused.json"),
        ),
    }

    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        if max_iterations == 4:
            return by_name["steady_4x2"]
        if max_iterations == 6:
            return by_name["extended_6x3"]
        return by_name["focused_3x2"]

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))

    best_profile = payload["best_profile"]
    assert best_profile["name"] == "extended_6x3"
    assert int(best_profile["final_intents_approved"]) == 25
    assert int(best_profile["final_expected_edge_blocked_count"]) == 0


def test_recovery_campaign_breaks_approval_ties_with_lower_expected_edge_pressure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    by_name = {
        "steady_4x2": _loop_payload(
            termination_reason="stalled",
            iterations_executed=4,
            initial_gap_score=1.0,
            final_gap_score=0.60,
            final_advisor_status="risk_off_active",
            negative_share=0.61,
            stale_negative_share=0.66,
            stale_share=0.67,
            final_intents_total=40,
            final_intents_approved=10,
            policy_reason_counts={"expected_edge_below_min": 8, "approved": 10},
            output_file=str(tmp_path / "steady.json"),
        ),
        "extended_6x3": _loop_payload(
            termination_reason="stalled",
            iterations_executed=6,
            initial_gap_score=1.0,
            final_gap_score=0.60,
            final_advisor_status="risk_off_active",
            negative_share=0.61,
            stale_negative_share=0.66,
            stale_share=0.67,
            final_intents_total=40,
            final_intents_approved=10,
            policy_reason_counts={"expected_edge_below_min": 2, "approved": 10},
            output_file=str(tmp_path / "extended.json"),
        ),
        "focused_3x2": _loop_payload(
            termination_reason="stalled",
            iterations_executed=3,
            initial_gap_score=1.0,
            final_gap_score=0.60,
            final_advisor_status="risk_off_active",
            negative_share=0.61,
            stale_negative_share=0.66,
            stale_share=0.67,
            final_intents_total=40,
            final_intents_approved=10,
            policy_reason_counts={
                "expected_edge_below_min": 1,
                "historical_profitability_expected_edge_below_min": 0,
                "approved": 10,
            },
            output_file=str(tmp_path / "focused.json"),
        ),
    }

    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        if max_iterations == 4:
            return by_name["steady_4x2"]
        if max_iterations == 6:
            return by_name["extended_6x3"]
        return by_name["focused_3x2"]

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))

    best_profile = payload["best_profile"]
    assert best_profile["name"] == "focused_3x2"
    assert int(best_profile["final_intents_approved"]) == 10
    assert int(best_profile["final_expected_edge_blocked_count"]) == 1


def test_recovery_campaign_writes_artifacts_and_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_loop(**kwargs):
        max_iterations = int(kwargs["max_iterations"])
        return _loop_payload(
            termination_reason="stalled",
            iterations_executed=max_iterations,
            initial_gap_score=1.0,
            final_gap_score=0.9,
            final_advisor_status="risk_off_active",
            negative_share=0.71,
            stale_negative_share=0.74,
            stale_share=0.76,
            output_file=str(tmp_path / f"loop_{max_iterations}.json"),
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)

    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))
    output_file = Path(payload["output_file"])
    latest_file = Path(payload["latest_file"])
    export_file = Path(payload["recommended_env_export_file"])
    patch_file = Path(payload["recommended_env_patch_file"])

    assert output_file.exists()
    assert latest_file.exists()
    assert export_file.exists()
    assert patch_file.exists()
    assert output_file.name.startswith("kalshi_temperature_recovery_campaign_")
    assert latest_file.name == "kalshi_temperature_recovery_campaign_latest.json"
    assert export_file.name == "kalshi_temperature_recovery_recommended_env.sh"
    assert patch_file.name == "kalshi_temperature_recovery_recommended.env"

    latest_payload = json.loads(latest_file.read_text(encoding="utf-8"))
    assert latest_payload["status"] == "ready"
    assert latest_payload["profiles_evaluated"] == 3
    assert latest_payload["recommended_env_export_file"] == str(export_file)
    assert latest_payload["recommended_env_patch_file"] == str(patch_file)

    export_text = export_file.read_text(encoding="utf-8")
    patch_text = patch_file.read_text(encoding="utf-8")
    assert "COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS" in export_text
    assert "COLDMATH_RECOVERY_LOOP_MIN_GAP_IMPROVEMENT" in export_text
    assert "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_ENABLED" in export_text
    assert "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_MIN_BUCKET_SAMPLES" in export_text
    assert "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_EXPECTANCY_THRESHOLD" in export_text
    assert "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_TOP_N" in export_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_WEATHER_WINDOW_HOURS_CAP" in export_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_OVERBLOCKING_BLOCKED_SHARE_THRESHOLD" in export_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_UNDERBLOCKING_MIN_TOP_N" in export_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_OVERBLOCKING_MAX_TOP_N" in export_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_MIN_BUCKET_SAMPLES_TARGET" in export_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_EXPECTANCY_THRESHOLD_TARGET" in export_text
    assert "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MODE" in export_text
    assert "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTED_PROFILES_USED" in export_text
    assert "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MOMENTUM_SCORE" in export_text
    assert "COLDMATH_RECOVERY_LOOP_MAX_ITERATIONS" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_MIN_GAP_IMPROVEMENT" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_ENABLED" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_MIN_BUCKET_SAMPLES" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_EXPECTANCY_THRESHOLD" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_TOP_N" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_WEATHER_WINDOW_HOURS_CAP" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_OVERBLOCKING_BLOCKED_SHARE_THRESHOLD" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_UNDERBLOCKING_MIN_TOP_N" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_OVERBLOCKING_MAX_TOP_N" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_MIN_BUCKET_SAMPLES_TARGET" in patch_text
    assert "COLDMATH_RECOVERY_LOOP_RETUNE_EXPECTANCY_THRESHOLD_TARGET" in patch_text
    assert "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MODE" in patch_text
    assert "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTED_PROFILES_USED" in patch_text
    assert "COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MOMENTUM_SCORE" in patch_text

    env = payload["recommended_env_overrides"]["env"]
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MODE"] == "baseline"
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTED_PROFILES_USED"] == "0"
    assert env["COLDMATH_RECOVERY_CAMPAIGN_PROFILE_ADAPTATION_MOMENTUM_SCORE"] == "0"

    summary = campaign.summarize_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))
    summary_payload = json.loads(summary)
    assert summary_payload["status"] == "ready"
    assert isinstance(summary, str)


def test_recovery_campaign_passes_and_exports_default_suppression_targets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    loop_calls: list[dict[str, object]] = []

    def fake_loop(**kwargs):
        loop_calls.append(dict(kwargs))
        max_iterations = int(kwargs["max_iterations"])
        return _loop_payload(
            termination_reason="stalled",
            iterations_executed=max_iterations,
            initial_gap_score=1.0,
            final_gap_score=0.85,
            final_advisor_status="risk_off_active",
            negative_share=0.66,
            stale_negative_share=0.69,
            stale_share=0.70,
            output_file=str(tmp_path / f"loop_{max_iterations}.json"),
        )

    monkeypatch.setattr(campaign, "run_kalshi_temperature_recovery_loop", fake_loop)
    payload = campaign.run_kalshi_temperature_recovery_campaign(output_dir=str(tmp_path))

    assert len(loop_calls) == 3
    for call in loop_calls:
        assert call["plateau_negative_regime_suppression_enabled"] is True
        assert int(call["plateau_negative_regime_suppression_min_bucket_samples"]) == 18
        assert float(call["plateau_negative_regime_suppression_expectancy_threshold"]) == -0.06
        assert int(call["plateau_negative_regime_suppression_top_n"]) == 10
        assert float(call["retune_weather_window_hours_cap"]) == 336.0
        assert float(call["retune_overblocking_blocked_share_threshold"]) == 0.25
        assert int(call["retune_underblocking_min_top_n"]) == 16
        assert int(call["retune_overblocking_max_top_n"]) == 4
        assert int(call["retune_min_bucket_samples_target"]) == 14
        assert float(call["retune_expectancy_threshold_target"]) == -0.045

    advisor_targets = payload["inputs"]["advisor_targets"]
    assert advisor_targets["plateau_negative_regime_suppression_enabled"] is True
    assert int(advisor_targets["plateau_negative_regime_suppression_min_bucket_samples"]) == 18
    assert float(advisor_targets["plateau_negative_regime_suppression_expectancy_threshold"]) == -0.06
    assert int(advisor_targets["plateau_negative_regime_suppression_top_n"]) == 10
    assert float(advisor_targets["retune_weather_window_hours_cap"]) == 336.0
    assert float(advisor_targets["retune_overblocking_blocked_share_threshold"]) == 0.25
    assert int(advisor_targets["retune_underblocking_min_top_n"]) == 16
    assert int(advisor_targets["retune_overblocking_max_top_n"]) == 4
    assert int(advisor_targets["retune_min_bucket_samples_target"]) == 14
    assert float(advisor_targets["retune_expectancy_threshold_target"]) == -0.045

    env = payload["recommended_env_overrides"]["env"]
    assert env["COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_ENABLED"] == "1"
    assert env["COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_MIN_BUCKET_SAMPLES"] == "18"
    assert env["COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_EXPECTANCY_THRESHOLD"] == "-0.06"
    assert env["COLDMATH_RECOVERY_LOOP_PLATEAU_NEGATIVE_REGIME_SUPPRESSION_TOP_N"] == "10"
    assert env["COLDMATH_RECOVERY_LOOP_RETUNE_WEATHER_WINDOW_HOURS_CAP"] == "336"
    assert env["COLDMATH_RECOVERY_LOOP_RETUNE_OVERBLOCKING_BLOCKED_SHARE_THRESHOLD"] == "0.25"
    assert env["COLDMATH_RECOVERY_LOOP_RETUNE_UNDERBLOCKING_MIN_TOP_N"] == "16"
    assert env["COLDMATH_RECOVERY_LOOP_RETUNE_OVERBLOCKING_MAX_TOP_N"] == "4"
    assert env["COLDMATH_RECOVERY_LOOP_RETUNE_MIN_BUCKET_SAMPLES_TARGET"] == "14"
    assert env["COLDMATH_RECOVERY_LOOP_RETUNE_EXPECTANCY_THRESHOLD_TARGET"] == "-0.045"
