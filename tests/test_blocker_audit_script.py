from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest


def _run_blocker_audit_with_lane_payload(
    *,
    tmp_path: Path,
    targeted_trading_support: dict[str, object],
    recovery_env_persistence: dict[str, object] | None = None,
    lane_alert_state: dict[str, object] | None = None,
    coldmath_hardening_overrides: dict[str, object] | None = None,
    recovery_actions_attempted: list[str] | None = None,
    recovery_latest_overrides: dict[str, object] | None = None,
    recovery_advisor_payload: dict[str, object] | None = None,
    recovery_advisor_raw_text: str | None = None,
    extra_env_lines: list[str] | None = None,
) -> dict[str, object]:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    coldmath_hardening_payload: dict[str, object] = {
        "status": "ready",
        "targeted_trading_support": targeted_trading_support,
    }
    if isinstance(recovery_env_persistence, dict):
        coldmath_hardening_payload["recovery_env_persistence"] = recovery_env_persistence
    if isinstance(coldmath_hardening_overrides, dict):
        coldmath_hardening_payload.update(coldmath_hardening_overrides)
    (health_dir / "coldmath_hardening_latest.json").write_text(
        json.dumps(coldmath_hardening_payload, indent=2),
        encoding="utf-8",
    )
    (health_dir / "decision_matrix_hardening_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "matrix_health_status": "red",
                "matrix_score": 0,
                "supports_consistency_and_profitability": False,
                "supports_bootstrap_progression": True,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    if isinstance(lane_alert_state, dict):
        (health_dir / ".decision_matrix_lane_alert_state.json").write_text(
            json.dumps(lane_alert_state, indent=2),
            encoding="utf-8",
        )
    if isinstance(recovery_actions_attempted, list) or isinstance(
        recovery_latest_overrides, dict
    ):
        recovery_dir = health_dir / "recovery"
        recovery_dir.mkdir(parents=True, exist_ok=True)
        recovery_latest_payload: dict[str, object] = {}
        if isinstance(recovery_actions_attempted, list):
            recovery_latest_payload["actions_attempted"] = recovery_actions_attempted
        if isinstance(recovery_latest_overrides, dict):
            recovery_latest_payload.update(recovery_latest_overrides)
        (recovery_dir / "recovery_latest.json").write_text(
            json.dumps(recovery_latest_payload, indent=2),
            encoding="utf-8",
        )
    if isinstance(recovery_advisor_payload, dict) or isinstance(recovery_advisor_raw_text, str):
        recovery_dir = health_dir / "recovery"
        recovery_dir.mkdir(parents=True, exist_ok=True)
        advisor_latest_path = recovery_dir / "kalshi_temperature_recovery_advisor_latest.json"
        if isinstance(recovery_advisor_raw_text, str):
            advisor_latest_path.write_text(recovery_advisor_raw_text, encoding="utf-8")
        else:
            advisor_latest_path.write_text(
                json.dumps(recovery_advisor_payload, indent=2),
                encoding="utf-8",
            )

    env_file = tmp_path / "blocker_audit.env"
    env_lines = [
        f'BETBOT_ROOT="{root}"',
        f'OUTPUT_DIR="{out_dir}"',
        "BLOCKER_AUDIT_SEND_WEBHOOK=0",
        "BLOCKER_AUDIT_DISCORD_MODE=concise",
        "COLDMATH_HARDENING_ENABLED=1",
        "COLDMATH_MARKET_INGEST_ENABLED=1",
        "COLDMATH_RECOVERY_ADVISOR_ENABLED=1",
        "COLDMATH_RECOVERY_LOOP_ENABLED=1",
        "COLDMATH_RECOVERY_CAMPAIGN_ENABLED=1",
        "COLDMATH_STAGE_TIMEOUT_STRICT_REQUIRED=1",
        "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
        "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=900",
        "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS=900",
        "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS=600",
        "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS=900",
        "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS=1200",
    ]
    if isinstance(extra_env_lines, list):
        env_lines.extend(extra_env_lines)
    env_file.write_text("\n".join(env_lines) + "\n", encoding="utf-8")

    script = root / "infra" / "digitalocean" / "run_temperature_blocker_audit.sh"
    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)
    flock_shim = tool_dir / "flock"
    flock_shim.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    flock_shim.chmod(0o755)
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    subprocess.run(
        ["/bin/bash", str(script), str(env_file)],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    latest_path = out_dir / "checkpoints" / "blocker_audit_168h_latest.json"
    assert latest_path.exists()
    payload = json.loads(latest_path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_malformed_window_summarize_script(tmp_path: Path) -> Path:
    script_path = tmp_path / "malformed_window_summarize.py"
    script_path.write_text(
        (
            "from __future__ import annotations\n"
            "import argparse\n"
            "from pathlib import Path\n"
            "\n"
            "parser = argparse.ArgumentParser()\n"
            "parser.add_argument('--output', required=True)\n"
            "args, _ = parser.parse_known_args()\n"
            "Path(args.output).write_text('{broken_json', encoding='utf-8')\n"
        ),
        encoding="utf-8",
    )
    return script_path


def test_blocker_audit_includes_bootstrap_lane_and_expiry_window(tmp_path: Path) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": False,
                "decision_matrix_bootstrap_signal_raw": True,
                "decision_matrix_bootstrap_signal": True,
            },
            "observed": {
                "decision_matrix_bootstrap_guard_status": "active",
                "decision_matrix_bootstrap_guard_reasons": [],
                "decision_matrix_bootstrap_guard_elapsed_hours": 12.5,
                "decision_matrix_supports_consistency_and_profitability": False,
            },
            "thresholds": {
                "matrix_bootstrap_max_hours": 336,
            },
        },
    )

    lane = payload.get("decision_matrix_lane") or {}
    assert lane.get("status") == "bootstrap"
    assert lane.get("decision_matrix_bootstrap_hours_to_expiry") == 323.5
    assert "bootstrap pass" in str(lane.get("summary_line") or "").lower()
    discord_message = str(payload.get("discord_message") or "")
    assert "Decision matrix lane: bootstrap pass" in discord_message


def test_blocker_audit_includes_bootstrap_blocked_reason(tmp_path: Path) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": False,
                "decision_matrix_strict_signal": False,
                "decision_matrix_bootstrap_signal_raw": True,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_bootstrap_guard_status": "blocked",
                "decision_matrix_bootstrap_guard_reasons": ["bootstrap_window_expired"],
                "decision_matrix_supports_consistency_and_profitability": False,
            },
            "thresholds": {
                "matrix_bootstrap_max_hours": 336,
            },
        },
    )

    lane = payload.get("decision_matrix_lane") or {}
    assert lane.get("status") == "bootstrap_blocked"
    summary_line = str(lane.get("summary_line") or "").lower()
    assert "bootstrap blocked" in summary_line
    assert "window expired" in summary_line
    discord_message = str(payload.get("discord_message") or "").lower()
    assert "decision matrix lane: bootstrap blocked" in discord_message


def test_blocker_audit_includes_decision_matrix_degraded_streak_context(tmp_path: Path) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": False,
                "decision_matrix_strict_signal": False,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_bootstrap_guard_status": "inactive",
                "decision_matrix_bootstrap_guard_reasons": [],
                "decision_matrix_supports_consistency_and_profitability": False,
            },
            "thresholds": {
                "matrix_bootstrap_max_hours": 336,
            },
        },
        lane_alert_state={
            "last_lane_status": "matrix_failed",
            "degraded_statuses": ["matrix_failed", "bootstrap_blocked"],
            "degraded_streak_count": 5,
            "degraded_streak_threshold": 3,
            "degraded_streak_notify_every": 3,
            "last_notify_reason": "degraded_streak",
        },
    )

    lane = payload.get("decision_matrix_lane") or {}
    assert lane.get("status") == "matrix_failed"
    assert lane.get("degraded_streak_count") == 5
    assert lane.get("degraded_streak_threshold") == 3
    assert lane.get("degraded_streak_notify_every") == 3
    assert lane.get("last_notify_reason") == "degraded_streak"
    streak_line = str(lane.get("degraded_streak_summary_line") or "")
    assert "Decision matrix degraded streak: 5 run(s)" in streak_line
    assert "[streak alert fired]" in streak_line
    discord_message = str(payload.get("discord_message") or "")
    assert "Decision matrix degraded streak: 5 run(s)" in discord_message


def test_blocker_audit_surfaces_window_summary_parse_error_without_strict_failure(
    tmp_path: Path,
) -> None:
    summarize_script = _write_malformed_window_summarize_script(tmp_path)
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        extra_env_lines=[f'WINDOW_SUMMARIZE_SCRIPT="{summarize_script}"'],
    )

    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("window_summary_loaded") is False
    assert data_quality.get("window_summary_parse_error_present") is True
    assert data_quality.get("window_summary_parse_error") == "invalid JSON"
    discord_message = str(payload.get("discord_message") or "")
    assert "Data quality check: blocker window summary malformed" in discord_message


def test_blocker_audit_surfaces_recovery_env_persistence_error(tmp_path: Path) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        recovery_env_persistence={
            "status": "error",
            "changed": False,
            "target_file": "/etc/betbot/temperature-shadow.env",
            "error": "Target env file not found",
        },
    )

    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("recovery_env_persistence_has_error") is True
    assert data_quality.get("recovery_env_persistence_status") == "error"
    persistence = payload.get("recovery_env_persistence") or {}
    assert persistence.get("status") == "error"
    assert persistence.get("has_error") is True
    assert persistence.get("target_file") == "/etc/betbot/temperature-shadow.env"
    message_quality = payload.get("message_quality_checks") or {}
    assert message_quality.get("recovery_env_persistence_ok") is False
    assert message_quality.get("overall_pass") is False
    discord_message = str(payload.get("discord_message") or "").lower()
    assert "data quality check: recovery env persistence failed" in discord_message
    assert "status=error" in discord_message


def test_blocker_audit_surfaces_recovery_advisor_effectiveness_demotion_line(
    tmp_path: Path,
) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        recovery_advisor_payload={
            "metrics": {
                "recovery_effectiveness": {
                    "summary_available": True,
                    "summary_source": "exact",
                    "summary_file_used": "/tmp/health/kalshi_temperature_recovery_loop_latest.json",
                    "persistently_harmful_actions": ["reduce_negative_expectancy_regimes"],
                }
            },
            "remediation_plan": {
                "demoted_actions_for_effectiveness": ["reduce_negative_expectancy_regimes"]
            },
        },
        recovery_latest_overrides={
            "recovery_effectiveness": {
                "gap_detected": True,
                "gap_reason": "stale_effectiveness_evidence",
                "stale": True,
                "file_age_seconds": 7200,
                "stale_threshold_seconds": 1800,
                "strict_required": True,
            }
        },
    )

    advisor_effectiveness = payload.get("recovery_advisor_effectiveness") or {}
    assert advisor_effectiveness.get("status") == "demoted_for_effectiveness"
    assert advisor_effectiveness.get("artifact_status") == "ok"
    assert advisor_effectiveness.get("summary_available") is True
    assert advisor_effectiveness.get("route_demotion_active") is True
    assert advisor_effectiveness.get("demoted_actions_for_effectiveness_count") == 1
    assert advisor_effectiveness.get("persistently_harmful_actions_count") == 1
    assert advisor_effectiveness.get("demoted_actions_for_effectiveness") == [
        "reduce_negative_expectancy_regimes"
    ]
    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("recovery_advisor_effectiveness_artifact_status") == "ok"
    assert data_quality.get("recovery_advisor_effectiveness_route_demotion_active") is True
    assert data_quality.get("recovery_advisor_effectiveness_gap_detected") is True
    assert data_quality.get("recovery_advisor_effectiveness_stale") is True
    assert data_quality.get("recovery_advisor_effectiveness_strict_required") is True
    discord_message = str(payload.get("discord_message") or "")
    assert "Recovery advisor effectiveness: demoted 1 action(s) for effectiveness." in discord_message


def test_blocker_audit_surfaces_recovery_latest_effectiveness_stale_gap(
    tmp_path: Path,
) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        recovery_latest_overrides={
            "recovery_effectiveness": {
                "gap_detected": True,
                "gap_reason": "stale_effectiveness_evidence",
                "stale": True,
                "file_age_seconds": 7200,
                "stale_threshold_seconds": 1800,
                "strict_required": True,
            }
        },
    )

    advisor_effectiveness = payload.get("recovery_advisor_effectiveness") or {}
    assert advisor_effectiveness.get("status") == "missing_or_stale_effectiveness_evidence"
    assert advisor_effectiveness.get("gap_detected") is True
    assert advisor_effectiveness.get("gap_reason") == "stale_effectiveness_evidence"
    assert advisor_effectiveness.get("stale") is True
    assert advisor_effectiveness.get("file_age_seconds") == 7200.0
    assert advisor_effectiveness.get("stale_threshold_seconds") == 1800.0
    assert advisor_effectiveness.get("strict_required") is True

    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("recovery_advisor_effectiveness_gap_detected") is True
    assert data_quality.get("recovery_advisor_effectiveness_gap_reason") == "stale_effectiveness_evidence"
    assert data_quality.get("recovery_advisor_effectiveness_stale") is True
    assert data_quality.get("recovery_advisor_effectiveness_file_age_seconds") == 7200.0
    assert data_quality.get("recovery_advisor_effectiveness_stale_threshold_seconds") == 1800.0
    assert data_quality.get("recovery_advisor_effectiveness_strict_required") is True

    discord_message = str(payload.get("discord_message") or "").lower()
    assert "missing/stale effectiveness evidence" in discord_message
    assert "rerun coldmath hardening" in discord_message


def test_blocker_audit_surfaces_recovery_latest_effectiveness_missing_gap(
    tmp_path: Path,
) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        recovery_latest_overrides={
            "recovery_effectiveness": {
                "gap_detected": True,
                "gap_reason": "missing_effectiveness_summary",
                "stale": False,
                "file_age_seconds": 0,
                "stale_threshold_seconds": 1800,
                "strict_required": True,
            }
        },
    )

    advisor_effectiveness = payload.get("recovery_advisor_effectiveness") or {}
    assert advisor_effectiveness.get("status") == "missing_or_stale_effectiveness_evidence"
    assert advisor_effectiveness.get("gap_detected") is True
    assert advisor_effectiveness.get("gap_reason") == "missing_effectiveness_summary"
    assert advisor_effectiveness.get("stale") is False
    assert advisor_effectiveness.get("file_age_seconds") == 0.0
    assert advisor_effectiveness.get("stale_threshold_seconds") == 1800.0
    assert advisor_effectiveness.get("strict_required") is True

    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("recovery_advisor_effectiveness_gap_detected") is True
    assert data_quality.get("recovery_advisor_effectiveness_gap_reason") == "missing_effectiveness_summary"
    assert data_quality.get("recovery_advisor_effectiveness_stale") is False
    assert data_quality.get("recovery_advisor_effectiveness_file_age_seconds") == 0.0
    assert data_quality.get("recovery_advisor_effectiveness_stale_threshold_seconds") == 1800.0
    assert data_quality.get("recovery_advisor_effectiveness_strict_required") is True

    discord_message = str(payload.get("discord_message") or "").lower()
    assert "missing/stale effectiveness evidence" in discord_message
    assert "missing effectiveness summary" in discord_message


def test_blocker_audit_surfaces_recovery_advisor_effectiveness_missing_fallback(
    tmp_path: Path,
) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
    )

    advisor_effectiveness = payload.get("recovery_advisor_effectiveness") or {}
    assert advisor_effectiveness.get("status") == "unavailable"
    assert advisor_effectiveness.get("artifact_status") == "missing"
    assert advisor_effectiveness.get("summary_available") is False
    assert advisor_effectiveness.get("route_demotion_active") is False
    assert advisor_effectiveness.get("demoted_actions_for_effectiveness_count") == 0
    assert advisor_effectiveness.get("persistently_harmful_actions_count") == 0
    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("recovery_advisor_effectiveness_artifact_status") == "missing"
    assert data_quality.get("recovery_advisor_effectiveness_route_demotion_active") is False
    discord_message = str(payload.get("discord_message") or "")
    assert "Recovery advisor effectiveness: unavailable." in discord_message


def test_blocker_audit_uses_recovery_latest_effectiveness_when_advisor_missing(
    tmp_path: Path,
) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        recovery_latest_overrides={
            "recovery_effectiveness": {
                "summary_available": True,
                "summary_source": "exact",
                "summary_file_used": "/tmp/health/kalshi_temperature_recovery_loop_latest.json",
                "persistently_harmful_actions": ["reduce_negative_expectancy_regimes"],
                "demoted_actions_for_effectiveness": ["reduce_negative_expectancy_regimes"],
            }
        },
    )

    advisor_effectiveness = payload.get("recovery_advisor_effectiveness") or {}
    assert advisor_effectiveness.get("status") == "demoted_for_effectiveness"
    assert advisor_effectiveness.get("artifact_status") == "missing"
    assert advisor_effectiveness.get("source_used") == "recovery_latest"
    assert advisor_effectiveness.get("summary_available") is True
    assert advisor_effectiveness.get("route_demotion_active") is True
    assert advisor_effectiveness.get("demoted_actions_for_effectiveness_count") == 1
    assert advisor_effectiveness.get("persistently_harmful_actions_count") == 1
    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("recovery_advisor_effectiveness_artifact_status") == "missing"
    assert data_quality.get("recovery_advisor_effectiveness_source_used") == "recovery_latest"
    assert data_quality.get("recovery_advisor_effectiveness_route_demotion_active") is True
    discord_message = str(payload.get("discord_message") or "")
    assert "Recovery advisor effectiveness: demoted 1 action(s) for effectiveness." in discord_message


def test_blocker_audit_prioritizes_timeout_guardrail_drift_as_top_blocker(tmp_path: Path) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        extra_env_lines=[
            "COLDMATH_STAGE_TIMEOUT_SECONDS=0",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
            "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS=0",
        ],
    )

    top_blockers = payload.get("top_blockers") or []
    assert isinstance(top_blockers, list)
    assert top_blockers
    top_blocker = top_blockers[0]
    assert isinstance(top_blocker, dict)
    assert top_blocker.get("reason") == "coldmath_stage_timeout_guardrail_drift"
    headline = payload.get("headline") or {}
    assert headline.get("largest_blocker_reason") == "coldmath_stage_timeout_guardrail_drift"
    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("coldmath_stage_timeout_guardrails_issue_present") is True
    assert data_quality.get("coldmath_stage_timeout_guardrails_config_issue") is True
    discord_message = str(payload.get("discord_message") or "").lower()
    assert "coldmath timeout issue detected" in discord_message
    assert "set_coldmath_stage_timeout_guardrails.sh" in discord_message


def test_blocker_audit_prioritizes_timeout_guardrail_missing_script_as_top_blocker(
    tmp_path: Path,
) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        recovery_actions_attempted=[
            "repair_coldmath_stage_timeout_guardrails:failed",
            "repair_coldmath_stage_timeout_guardrails:missing_script",
        ],
        extra_env_lines=[
            "COLDMATH_STAGE_TIMEOUT_SECONDS=0",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
            "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS=0",
        ],
    )

    top_blockers = payload.get("top_blockers") or []
    assert isinstance(top_blockers, list)
    assert top_blockers
    top_blocker = top_blockers[0]
    assert isinstance(top_blocker, dict)
    assert (
        top_blocker.get("reason")
        == "coldmath_stage_timeout_guardrail_repair_script_missing"
    )
    assert "script missing" in str(top_blocker.get("reason_human") or "").lower()
    headline = payload.get("headline") or {}
    assert (
        headline.get("largest_blocker_reason")
        == "coldmath_stage_timeout_guardrail_repair_script_missing"
    )
    data_quality = payload.get("data_quality") or {}
    assert (
        data_quality.get("coldmath_stage_timeout_guardrails_latest_repair_action")
        == "repair_coldmath_stage_timeout_guardrails:missing_script"
    )
    assert (
        data_quality.get("coldmath_stage_timeout_guardrails_latest_repair_status")
        == "missing_script"
    )


def test_blocker_audit_prioritizes_timeout_guardrail_failed_as_top_blocker(
    tmp_path: Path,
) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        recovery_actions_attempted=[
            "repair_coldmath_stage_timeout_guardrails:ok",
            "repair_coldmath_stage_timeout_guardrails:failed",
        ],
        extra_env_lines=[
            "COLDMATH_STAGE_TIMEOUT_SECONDS=0",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
            "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS=0",
            "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS=0",
        ],
    )

    top_blockers = payload.get("top_blockers") or []
    assert isinstance(top_blockers, list)
    assert top_blockers
    top_blocker = top_blockers[0]
    assert isinstance(top_blocker, dict)
    assert top_blocker.get("reason") == "coldmath_stage_timeout_guardrail_repair_failed"
    assert "repair failed" in str(top_blocker.get("reason_human") or "").lower()
    headline = payload.get("headline") or {}
    assert (
        headline.get("largest_blocker_reason")
        == "coldmath_stage_timeout_guardrail_repair_failed"
    )
    timeout_signal = payload.get("coldmath_stage_timeout_guardrails") or {}
    assert (
        timeout_signal.get("latest_repair_action")
        == "repair_coldmath_stage_timeout_guardrails:failed"
    )
    assert timeout_signal.get("latest_repair_status") == "failed"


def test_blocker_audit_includes_required_stage_timeout_signal_in_payload(tmp_path: Path) -> None:
    payload = _run_blocker_audit_with_lane_payload(
        tmp_path=tmp_path,
        targeted_trading_support={
            "checks": {
                "decision_matrix_signal": True,
                "decision_matrix_strict_signal": True,
                "decision_matrix_bootstrap_signal_raw": False,
                "decision_matrix_bootstrap_signal": False,
            },
            "observed": {
                "decision_matrix_supports_consistency_and_profitability": True,
            },
            "thresholds": {},
        },
        coldmath_hardening_overrides={
            "stages": [
                {"stage": "coldmath_snapshot_summary", "status": "ok"},
                {"stage": "polymarket_market_ingest", "status": "ok"},
                {"stage": "kalshi_temperature_recovery_advisor", "status": "ok"},
                {"stage": "kalshi_temperature_recovery_loop", "status": "timeout"},
                {"stage": "kalshi_temperature_recovery_campaign", "status": "ok"},
            ]
        },
    )

    timeout_signal = payload.get("coldmath_stage_timeout_guardrails") or {}
    assert timeout_signal.get("issue_present") is True
    assert timeout_signal.get("required_stage_timeout_present") is True
    assert timeout_signal.get("status") == "required_stage_timeout"
    assert "kalshi_temperature_recovery_loop" in (
        timeout_signal.get("required_stage_timeout_stages") or []
    )
    data_quality = payload.get("data_quality") or {}
    assert data_quality.get("coldmath_stage_timeout_guardrails_issue_present") is True
    assert (
        data_quality.get("coldmath_stage_timeout_guardrails_required_stage_timeout_present")
        is True
    )
    top_blockers = payload.get("top_blockers") or []
    assert isinstance(top_blockers, list)
    assert top_blockers
    assert isinstance(top_blockers[0], dict)
    assert top_blockers[0].get("reason") == "coldmath_stage_timeout_guardrail_drift"


def test_blocker_audit_strict_fails_when_window_summary_is_malformed(tmp_path: Path) -> None:
    summarize_script = _write_malformed_window_summarize_script(tmp_path)
    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        _run_blocker_audit_with_lane_payload(
            tmp_path=tmp_path,
            targeted_trading_support={
                "checks": {
                    "decision_matrix_signal": True,
                    "decision_matrix_strict_signal": True,
                    "decision_matrix_bootstrap_signal_raw": False,
                    "decision_matrix_bootstrap_signal": False,
                },
                "observed": {
                    "decision_matrix_supports_consistency_and_profitability": True,
                },
                "thresholds": {},
            },
            extra_env_lines=[
                f'WINDOW_SUMMARIZE_SCRIPT="{summarize_script}"',
                "BLOCKER_AUDIT_STRICT_FAIL_ON_WINDOW_SUMMARY_PARSE_ERROR=1",
            ],
        )

    strict_message = f"{exc_info.value.stdout}\n{exc_info.value.stderr}"
    assert "STRICT CHECK FAILED: blocker audit window summary malformed" in strict_message
