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
    lane_alert_state: dict[str, object] | None = None,
    extra_env_lines: list[str] | None = None,
) -> dict[str, object]:
    root = Path(__file__).resolve().parents[1]
    out_dir = tmp_path / "out"
    health_dir = out_dir / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    (health_dir / "coldmath_hardening_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "targeted_trading_support": targeted_trading_support,
            },
            indent=2,
        ),
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

    env_file = tmp_path / "blocker_audit.env"
    env_lines = [
        f'BETBOT_ROOT="{root}"',
        f'OUTPUT_DIR="{out_dir}"',
        "BLOCKER_AUDIT_SEND_WEBHOOK=0",
        "BLOCKER_AUDIT_DISCORD_MODE=concise",
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
