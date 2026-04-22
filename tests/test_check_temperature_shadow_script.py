from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
from typing import Iterable


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _prepare_script_bundle(*, tmp_path: Path, root: Path) -> tuple[Path, Path]:
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    src_script = root / "infra" / "digitalocean" / "check_temperature_shadow.sh"
    script_path = bundle_dir / "check_temperature_shadow.sh"
    shutil.copy2(src_script, script_path)
    script_path.chmod(0o755)

    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)

    sudo_shim = tool_dir / "sudo"
    sudo_shim.write_text("#!/bin/sh\nexec \"$@\"\n", encoding="utf-8")
    sudo_shim.chmod(0o755)

    systemctl_shim = tool_dir / "systemctl"
    systemctl_shim.write_text(
        """#!/bin/sh
while [ "$#" -gt 0 ]; do
  case "$1" in
    -*) shift ;;
    *) break ;;
  esac
done
cmd="${1:-}"
unit="${2:-}"
case "$cmd" in
  is-active)
    default_active="${MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT:-active}"
    case "$unit" in
      betbot-temperature-alpha-workers) echo "${MOCK_ALPHA_WORKER_ACTIVE:-$default_active}"; exit 0 ;;
      betbot-temperature-breadth-worker) echo "${MOCK_BREADTH_WORKER_ACTIVE:-$default_active}"; exit 0 ;;
      betbot-temperature-discord-route-guard.timer) echo "${MOCK_DISCORD_ROUTE_GUARD_TIMER_ACTIVE:-$default_active}"; exit 0 ;;
      betbot-temperature-stale-metrics-drill.timer) echo "${MOCK_STALE_METRICS_DRILL_TIMER_ACTIVE:-$default_active}"; exit 0 ;;
      *) echo "$default_active"; exit 0 ;;
    esac
    ;;
  is-enabled)
    default_enabled="${MOCK_SYSTEMCTL_IS_ENABLED_DEFAULT:-enabled}"
    case "$unit" in
      betbot-temperature-discord-route-guard.timer) echo "${MOCK_DISCORD_ROUTE_GUARD_TIMER_ENABLED:-$default_enabled}"; exit 0 ;;
      betbot-temperature-stale-metrics-drill.timer) echo "${MOCK_STALE_METRICS_DRILL_TIMER_ENABLED:-$default_enabled}"; exit 0 ;;
      *) echo "$default_enabled"; exit 0 ;;
    esac
    ;;
  cat)
    case ",${MOCK_SYSTEMCTL_CAT_MISSING_UNITS:-}," in
      *,"$unit",*) exit 1 ;;
    esac
    echo "# stub unit"
    exit 0
    ;;
  status) echo "stub status"; exit 0 ;;
  *) echo active; exit 0 ;;
esac
""",
        encoding="utf-8",
    )
    systemctl_shim.chmod(0o755)

    journalctl_shim = tool_dir / "journalctl"
    journalctl_shim.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    journalctl_shim.chmod(0o755)

    return script_path, tool_dir


def _write_env_file(
    *,
    env_file: Path,
    output_dir: Path,
    extra_lines: Iterable[str] = (),
) -> None:
    lines = [
        f'OUTPUT_DIR="{output_dir}"',
        "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=0",
        "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
        "LIVE_STATUS_STRICT_MAX_AGE_SECONDS=300",
        "ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS=54000",
        "DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS=10800",
    ]
    lines.extend(extra_lines)
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _seed_required_artifacts(output_dir: Path) -> None:
    _write_json(
        output_dir / "health" / "live_status_latest.json",
        {
            "status": "green",
            "trigger_flags": {
                "approvals_resumed": False,
                "planned_orders_resumed": False,
                "shadow_resolved_first": False,
                "resolved_shadow_basis": "none",
                "resolved_shadow_basis_value_14h": 0,
            },
            "freshness_plan": {
                "approval_rate_guardrail_status": "within_band",
                "approval_rate_guardrail_evaluated": True,
                "approval_rate": 0.1,
                "metar_observation_stale_rate": 0.0,
            },
            "scan_budget": {
                "effective_max_markets": 100,
                "next_max_markets": 100,
                "adaptive_decision_action": "hold",
                "adaptive_decision_reason": "within_guardrails",
                "scan_cap_bound_with_headroom": False,
                "load_per_vcpu": 0.5,
                "intents_total_hint": 10,
            },
            "latest_cycle_metrics": {
                "intents_approved": 1,
                "intents_total": 10,
                "planned_orders": 1,
            },
            "red_reasons": [],
            "yellow_reasons": [],
        },
    )
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "GREEN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )


def _run_shadow_check(
    *,
    script_path: Path,
    env_file: Path,
    tool_dir: Path,
    strict: bool = True,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    if extra_env:
        env.update(extra_env)
    cmd = ["/bin/bash", str(script_path)]
    if strict:
        cmd.append("--strict")
    cmd.extend(["--env", str(env_file)])
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_shadow_check_strict_fails_when_lane_degraded_streak_hits_threshold(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / ".decision_matrix_lane_alert_state.json",
        {
            "last_lane_status": "matrix_failed",
            "degraded_streak_count": 1,
            "degraded_streak_threshold": 3,
            "degraded_streak_notify_every": 3,
            "last_notify_reason": "none",
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "decision_matrix_lane_alert_state status=matrix_failed" in result.stdout
    assert "STRICT CHECK FAILED: decision-matrix lane degraded streak active" in result.stderr


def test_shadow_check_strict_passes_when_streak_below_default_threshold(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / ".decision_matrix_lane_alert_state.json",
        {
            "last_lane_status": "matrix_failed",
            "degraded_streak_count": 1,
            "degraded_streak_threshold": 3,
            "degraded_streak_notify_every": 3,
            "last_notify_reason": "none",
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "decision_matrix_lane_alert_state status=matrix_failed" in result.stdout
    assert "STRICT CHECK FAILED: decision-matrix lane degraded streak active" not in result.stderr


def test_shadow_check_strict_passes_when_lane_status_not_in_strict_set_even_with_high_streak(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / ".decision_matrix_lane_alert_state.json",
        {
            "last_lane_status": "healthy",
            "degraded_streak_count": 99,
            "degraded_streak_threshold": 1,
            "degraded_streak_notify_every": 1,
            "last_notify_reason": "none",
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "decision_matrix_lane_alert_state status=healthy" in result.stdout
    assert "STRICT CHECK FAILED: decision-matrix lane degraded streak active" not in result.stderr


def test_shadow_check_strict_fails_when_lane_status_matches_custom_list_with_spaces(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / ".decision_matrix_lane_alert_state.json",
        {
            "last_lane_status": "bootstrap_blocked",
            "degraded_streak_count": 2,
            "degraded_streak_threshold": 1,
            "degraded_streak_notify_every": 1,
            "last_notify_reason": "none",
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD=1",
            'DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES="matrix_failed, bootstrap_blocked"',
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "decision_matrix_lane_alert_state status=bootstrap_blocked" in result.stdout
    assert "STRICT CHECK FAILED: decision-matrix lane degraded streak active" in result.stderr


def test_shadow_check_strict_passes_when_lane_threshold_is_non_numeric(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / ".decision_matrix_lane_alert_state.json",
        {
            "last_lane_status": "matrix_failed",
            "degraded_streak_count": 99,
            "degraded_streak_threshold": 1,
            "degraded_streak_notify_every": 1,
            "last_notify_reason": "none",
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD=not-a-number",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "decision_matrix_lane_alert_state status=matrix_failed" in result.stdout
    assert "STRICT CHECK FAILED: decision-matrix lane degraded streak active" not in result.stderr


def test_shadow_check_strict_falls_back_to_default_lane_statuses_when_config_empty(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / ".decision_matrix_lane_alert_state.json",
        {
            "last_lane_status": "matrix_failed",
            "degraded_streak_count": 2,
            "degraded_streak_threshold": 1,
            "degraded_streak_notify_every": 1,
            "last_notify_reason": "none",
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD=1",
            'DECISION_MATRIX_LANE_STRICT_DEGRADED_STATUSES=""',
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "strict_statuses=matrix_failed,bootstrap_blocked" in result.stdout
    assert "STRICT CHECK FAILED: decision-matrix lane degraded streak active" in result.stderr


def test_shadow_check_strict_fails_when_lane_state_required_but_missing(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "decision_matrix_lane_alert_state -> MISSING" in result.stdout
    assert "STRICT CHECK FAILED: decision-matrix lane state file required but missing" in result.stderr


def test_shadow_check_strict_fails_on_non_green_route_guard_when_collision_gate_enabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "yellow",
            "shared_route_group_count": 2,
            "route_remediations": [
                {
                    "route_hint": "shadow_alert",
                    "required_thread_env_keys": ["SHADOW_ALERT_WEBHOOK_THREAD_ID"],
                }
            ],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "discord_route_guard_latest status=yellow" in result.stdout or "discord_route_guard_latest status=unknown" in result.stdout
    assert "STRICT CHECK FAILED: discord-route-guard indicates non-green route separation" in result.stderr


def test_shadow_check_strict_route_guard_failure_prints_remediation_commands(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "yellow",
            "shared_route_group_count": 2,
            "route_remediations": [
                {
                    "route_hint": "shadow_alert",
                    "required_thread_env_keys": ["SHADOW_ALERT_WEBHOOK_THREAD_ID"],
                }
            ],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK REMEDIATION: route_hint=shadow_alert required_thread_env_keys=SHADOW_ALERT_WEBHOOK_THREAD_ID" in result.stderr
    assert "STRICT CHECK REMEDIATION: edit /etc/betbot/discord-thread-map.env" in result.stderr
    assert "STRICT CHECK REMEDIATION: preflight map with sudo bash" in result.stderr
    assert "STRICT CHECK REMEDIATION: preflight+apply with sudo bash" in result.stderr
    assert "STRICT CHECK REMEDIATION: run sudo bash" in result.stderr


def test_shadow_check_strict_allows_non_green_route_guard_when_collision_gate_disabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "yellow",
            "shared_route_group_count": 2,
            "route_remediations": [
                {
                    "route_hint": "shadow_alert",
                    "required_thread_env_keys": ["SHADOW_ALERT_WEBHOOK_THREAD_ID"],
                }
            ],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "STRICT CHECK FAILED: discord-route-guard indicates non-green route separation" not in result.stderr


def test_shadow_check_strict_fails_on_non_green_route_guard_with_legacy_collision_gate(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "yellow",
            "shared_route_group_count": 2,
            "route_remediations": [
                {
                    "route_hint": "shadow_alert",
                    "required_thread_env_keys": [
                        "SHADOW_ALERT_WEBHOOK_THREAD_ID",
                        "HEALTH_ALERTS_WEBHOOK_THREAD_ID",
                    ],
                }
            ],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "unset DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION",
            "DISCORD_ROUTE_GUARD_FAIL_ON_COLLISION=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: discord-route-guard indicates non-green route separation" in result.stderr
    assert "STRICT CHECK REMEDIATION: route_hint=shadow_alert required_thread_env_keys=HEALTH_ALERTS_WEBHOOK_THREAD_ID,SHADOW_ALERT_WEBHOOK_THREAD_ID" in result.stderr


def test_shadow_check_strict_fails_on_non_green_route_guard_when_collision_gate_uses_expected_default(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "yellow",
            "shared_route_group_count": 1,
            "route_remediations": [
                {
                    "route_hint": "ops_summary",
                    "required_thread_env_keys": ["ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID"],
                }
            ],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "unset DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION",
            "unset DISCORD_ROUTE_GUARD_FAIL_ON_COLLISION",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: discord-route-guard indicates non-green route separation" in result.stderr
    assert "STRICT CHECK REMEDIATION: route_hint=ops_summary required_thread_env_keys=ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID" in result.stderr


def test_shadow_check_strict_fails_when_route_guard_artifact_is_stale_and_expected(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    route_guard_path = output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json"
    _write_json(
        route_guard_path,
        {
            "guard_status": "green",
            "shared_route_group_count": 0,
            "route_remediations": [],
        },
    )
    # Force artifact age far beyond strict max-age threshold.
    os.utime(route_guard_path, (1, 1))

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
            "DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: discord-route-guard artifact stale" in result.stderr


def test_shadow_check_strict_fails_when_route_guard_artifact_is_stale_with_non_numeric_max_age(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    route_guard_path = output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json"
    _write_json(
        route_guard_path,
        {
            "guard_status": "green",
            "shared_route_group_count": 0,
            "route_remediations": [],
        },
    )
    os.utime(route_guard_path, (1, 1))

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
            "DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS=not-a-number",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: discord-route-guard artifact stale" in result.stderr


def test_shadow_check_strict_fails_when_route_guard_expected_defaults_to_installed_timer(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "unset DISCORD_ROUTE_GUARD_TIMER_EXPECTED",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_DISCORD_ROUTE_GUARD_TIMER_ACTIVE": "inactive",
        },
    )

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: discord-route-guard timer expected but not active" in result.stderr


def test_shadow_check_strict_ignores_stale_route_guard_when_not_expected(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    route_guard_path = output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json"
    _write_json(
        route_guard_path,
        {
            "guard_status": "green",
            "shared_route_group_count": 0,
            "route_remediations": [],
        },
    )
    os.utime(route_guard_path, (1, 1))

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=0",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
            "DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "STRICT CHECK FAILED: discord-route-guard artifact stale" not in result.stderr


def test_shadow_check_strict_warns_when_alpha_summary_health_is_yellow(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "YELLOW",
                "reason_text": "quality gate caution",
            },
            "headline_metrics": {
                "health_status": "YELLOW",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 1
    assert "STRICT CHECK WARNING: alpha summary health is yellow" in result.stderr


def test_shadow_check_strict_warns_when_alpha_summary_health_is_yellow_without_reason_text(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "YELLOW",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "YELLOW",
                "health_reason_text": "",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 1
    assert "STRICT CHECK WARNING: alpha summary health is yellow" in result.stderr
    assert "(reason:" not in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_health_is_red(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "RED",
                "reason_text": "critical signal mismatch",
            },
            "headline_metrics": {
                "health_status": "RED",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary health is red" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_payload_consistency_is_false(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "GREEN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": False,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary payload consistency failed" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_message_quality_is_false(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "GREEN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": False,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary message quality checks failed" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_trader_payload_consistency_is_false(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "GREEN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": False,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary trader_view payload consistency failed" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_trader_view_consistency_check_fails(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "GREEN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.2,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary trader_view consistency check failed" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_trader_view_block_is_missing(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "GREEN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary trader_view block missing" in result.stderr


def test_shadow_check_strict_fails_when_auto_profile_required_but_missing(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "GREEN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "auto_profile",
                "quality_gate_auto_applied": True,
                "quality_gate_min_probability_confidence": 0.62,
                "quality_gate_min_expected_edge_net": 0.01,
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": True,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: auto profile required but missing" in result.stderr


def test_shadow_check_strict_passes_when_auto_profile_required_and_valid(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(
        auto_profile_path,
        {
            "status": "ready",
            "min_probability_confidence": 0.62,
            "min_expected_edge_net": 0.01,
        },
    )
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "GREEN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "auto_profile",
                "quality_gate_auto_applied": True,
                "quality_gate_min_probability_confidence": 0.62,
                "quality_gate_min_expected_edge_net": 0.01,
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": True,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "STRICT CHECK FAILED: auto profile required" not in result.stderr


def test_shadow_check_strict_fails_when_auto_profile_is_stale(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(
        auto_profile_path,
        {
            "status": "ready",
            "min_probability_confidence": 0.62,
            "min_expected_edge_net": 0.01,
        },
    )
    os.utime(auto_profile_path, (1, 1))
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "auto_profile",
                "quality_gate_auto_applied": True,
                "quality_gate_min_probability_confidence": 0.62,
                "quality_gate_min_expected_edge_net": 0.01,
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": True,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            "AUTO_PROFILE_STRICT_MAX_AGE_SECONDS=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: auto profile stale" in result.stderr


def test_shadow_check_strict_fails_when_auto_profile_is_stale_with_non_numeric_max_age(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(
        auto_profile_path,
        {
            "status": "ready",
            "min_probability_confidence": 0.62,
            "min_expected_edge_net": 0.01,
        },
    )
    os.utime(auto_profile_path, (1, 1))
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "auto_profile",
                "quality_gate_auto_applied": True,
                "quality_gate_min_probability_confidence": 0.62,
                "quality_gate_min_expected_edge_net": 0.01,
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": True,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            "AUTO_PROFILE_STRICT_MAX_AGE_SECONDS=not-a-number",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: auto profile stale" in result.stderr


def test_shadow_check_strict_fails_when_auto_profile_source_is_not_auto_profile(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(auto_profile_path, {"status": "ready"})
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "manual",
                "quality_gate_auto_applied": True,
                "quality_gate_min_probability_confidence": 0.62,
                "quality_gate_min_expected_edge_net": 0.01,
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": True,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: auto profile required but alpha summary source is" in result.stderr


def test_shadow_check_strict_fails_when_auto_profile_not_marked_applied(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(auto_profile_path, {"status": "ready"})
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "auto_profile",
                "quality_gate_auto_applied": False,
                "quality_gate_min_probability_confidence": 0.62,
                "quality_gate_min_expected_edge_net": 0.01,
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": True,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: auto profile required but alpha summary reports auto_applied=false" in result.stderr


def test_shadow_check_strict_fails_when_auto_profile_min_probability_confidence_is_non_numeric(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(auto_profile_path, {"status": "ready"})
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "auto_profile",
                "quality_gate_auto_applied": True,
                "quality_gate_min_probability_confidence": "n/a",
                "quality_gate_min_expected_edge_net": 0.01,
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": True,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: auto profile required but min probability confidence missing/non-numeric" in result.stderr


def test_shadow_check_strict_fails_when_auto_profile_min_expected_edge_is_non_numeric(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(auto_profile_path, {"status": "ready"})
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "auto_profile",
                "quality_gate_auto_applied": True,
                "quality_gate_min_probability_confidence": 0.62,
                "quality_gate_min_expected_edge_net": "n/a",
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": True,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: auto profile required but min expected edge missing/non-numeric" in result.stderr


def test_shadow_check_strict_passes_when_auto_profile_released_in_this_run_and_file_missing(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "manual",
                "quality_gate_auto_applied": False,
                "quality_gate_min_probability_confidence": "",
                "quality_gate_min_expected_edge_net": "",
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": True,
                "apply_reason": "released_profile_after_quality_recovery",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "STRICT CHECK FAILED: auto profile required" not in result.stderr


def test_shadow_check_strict_passes_when_auto_profile_released_via_apply_reason_prefix_and_file_missing(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "manual",
                "quality_gate_auto_applied": False,
                "quality_gate_min_probability_confidence": "",
                "quality_gate_min_expected_edge_net": "",
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "released_profile_manual_override",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "STRICT CHECK FAILED: auto profile required" not in result.stderr


def test_shadow_check_strict_fails_when_auto_profile_applied_in_run_but_file_missing(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "manual",
                "quality_gate_auto_applied": False,
                "quality_gate_min_probability_confidence": 0.62,
                "quality_gate_min_expected_edge_net": 0.01,
            },
            "approval_auto_apply": {
                "enabled": True,
                "should_apply": False,
                "applied_in_this_run": True,
                "released_in_this_run": False,
                "apply_reason": "auto_profile_enforced",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: auto profile required but missing" in result.stderr


def test_shadow_check_strict_passes_when_auto_profile_expected_but_not_currently_required(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    auto_profile_path = tmp_path / "approval_gate_profile_auto.json"
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {"status": "GREEN", "reason_text": ""},
            "headline_metrics": {
                "health_status": "GREEN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
                "quality_gate_source": "manual",
                "quality_gate_auto_applied": False,
                "quality_gate_min_probability_confidence": "",
                "quality_gate_min_expected_edge_net": "",
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "APPROVAL_GATE_PROFILE_AUTO_ENABLED=1",
            f"APPROVAL_GATE_PROFILE_AUTO_PATH={auto_profile_path}",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "STRICT CHECK FAILED: auto profile required" not in result.stderr


def test_shadow_check_strict_fails_when_live_status_is_red(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "live_status_latest.json",
        {
            "status": "red",
            "trigger_flags": {
                "approvals_resumed": False,
                "planned_orders_resumed": False,
            },
            "freshness_plan": {
                "approval_rate_guardrail_status": "critical_high",
                "approval_rate_guardrail_evaluated": True,
                "approval_rate": 0.9,
                "metar_observation_stale_rate": 0.8,
            },
            "scan_budget": {
                "effective_max_markets": 100,
            },
            "latest_cycle_metrics": {
                "intents_approved": 0,
                "intents_total": 10,
                "planned_orders": 0,
            },
            "red_reasons": ["simulated_red"],
            "yellow_reasons": [],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: live_status is red" in result.stderr


def test_shadow_check_non_strict_ignores_red_live_status_failure(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "live_status_latest.json",
        {
            "status": "red",
            "trigger_flags": {
                "approvals_resumed": False,
                "planned_orders_resumed": False,
            },
            "freshness_plan": {
                "approval_rate_guardrail_status": "critical_high",
                "approval_rate_guardrail_evaluated": True,
                "approval_rate": 0.9,
                "metar_observation_stale_rate": 0.8,
            },
            "scan_budget": {
                "effective_max_markets": 100,
            },
            "latest_cycle_metrics": {
                "intents_approved": 0,
                "intents_total": 10,
                "planned_orders": 0,
            },
            "red_reasons": ["simulated_red"],
            "yellow_reasons": [],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir, strict=False)

    assert result.returncode == 0
    assert "STRICT CHECK FAILED" not in result.stderr


def test_shadow_check_strict_fails_when_live_status_artifact_is_missing(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    (output_dir / "health" / "live_status_latest.json").unlink(missing_ok=True)

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "live_status -> MISSING" in result.stdout
    assert "STRICT CHECK FAILED: live_status is unknown/non-green" in result.stderr


def test_shadow_check_non_strict_allows_missing_live_status_artifact(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    (output_dir / "health" / "live_status_latest.json").unlink(missing_ok=True)

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        strict=False,
    )

    assert result.returncode == 0
    assert "live_status -> MISSING" in result.stdout
    assert "STRICT CHECK FAILED: live_status is unknown/non-green" not in result.stderr


def test_shadow_check_non_strict_ignores_route_guard_timer_failure(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        strict=False,
        extra_env={
            "MOCK_DISCORD_ROUTE_GUARD_TIMER_ACTIVE": "inactive",
        },
    )

    assert result.returncode == 0
    assert "STRICT CHECK FAILED: discord-route-guard timer expected but not active" not in result.stderr


def test_shadow_check_help_flag_exits_zero_and_prints_usage() -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow.sh"
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "Usage:" in result.stdout


def test_shadow_check_unknown_option_fails_with_usage() -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow.sh"
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--bad-option"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Unknown option: --bad-option" in result.stderr
    assert "Usage:" in result.stderr


def test_shadow_check_missing_env_flag_value_fails() -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow.sh"
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--env"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Missing value for --env" in result.stderr


def test_shadow_check_missing_env_file_fails(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow.sh"
    missing_env = tmp_path / "missing.env"
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--env", str(missing_env)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert f"Missing {missing_env}" in result.stderr


def test_shadow_check_env_file_without_output_dir_fails(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow.sh"
    env_file = tmp_path / "shadow.env"
    env_file.write_text("DISCORD_ROUTE_GUARD_TIMER_EXPECTED=0\n", encoding="utf-8")
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--env", str(env_file)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "OUTPUT_DIR not set in" in result.stderr


def test_shadow_check_accepts_positional_env_path(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    result = subprocess.run(
        ["/bin/bash", str(script_path), str(env_file)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )

    assert result.returncode == 0
    assert "=== systemd ===" in result.stdout


def test_shadow_check_strict_skips_route_guard_timer_gate_when_timer_not_installed_and_expectation_unset(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "yellow",
            "shared_route_group_count": 2,
            "route_remediations": [
                {
                    "route_hint": "shadow_alert",
                    "required_thread_env_keys": ["SHADOW_ALERT_WEBHOOK_THREAD_ID"],
                }
            ],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "unset DISCORD_ROUTE_GUARD_TIMER_EXPECTED",
            "unset DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION",
            "unset DISCORD_ROUTE_GUARD_FAIL_ON_COLLISION",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_SYSTEMCTL_CAT_MISSING_UNITS": "betbot-temperature-discord-route-guard.timer",
            "MOCK_DISCORD_ROUTE_GUARD_TIMER_ACTIVE": "inactive",
        },
    )

    assert result.returncode == 0
    assert "STRICT CHECK FAILED: discord-route-guard timer expected but not active" not in result.stderr
    assert "STRICT CHECK FAILED: discord-route-guard indicates non-green route separation" not in result.stderr


def test_shadow_check_strict_enforces_route_guard_timer_gate_when_expectation_explicit_even_if_timer_not_installed(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_SYSTEMCTL_CAT_MISSING_UNITS": "betbot-temperature-discord-route-guard.timer",
            "MOCK_DISCORD_ROUTE_GUARD_TIMER_ACTIVE": "inactive",
        },
    )

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: discord-route-guard timer expected but not active" in result.stderr


def test_shadow_check_strict_warns_when_live_status_is_yellow(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "live_status_latest.json",
        {
            "status": "yellow",
            "trigger_flags": {
                "approvals_resumed": False,
                "planned_orders_resumed": False,
            },
            "freshness_plan": {
                "approval_rate_guardrail_status": "above_band",
                "approval_rate_guardrail_evaluated": True,
                "approval_rate": 0.25,
                "metar_observation_stale_rate": 0.2,
            },
            "scan_budget": {
                "effective_max_markets": 100,
            },
            "latest_cycle_metrics": {
                "intents_approved": 1,
                "intents_total": 10,
                "planned_orders": 1,
            },
            "red_reasons": [],
            "yellow_reasons": ["simulated_yellow"],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 1
    assert "STRICT CHECK WARNING: live_status is yellow" in result.stderr


def test_shadow_check_strict_fails_when_live_status_artifact_is_stale(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    live_status_path = output_dir / "health" / "live_status_latest.json"
    os.utime(live_status_path, (1, 1))

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "LIVE_STATUS_STRICT_MAX_AGE_SECONDS=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: live_status artifact stale" in result.stderr


def test_shadow_check_strict_fails_when_live_status_artifact_is_stale_with_non_numeric_max_age(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    live_status_path = output_dir / "health" / "live_status_latest.json"
    os.utime(live_status_path, (1, 1))

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "LIVE_STATUS_STRICT_MAX_AGE_SECONDS=not-a-number",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: live_status artifact stale" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_artifact_is_missing(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    alpha_path = output_dir / "health" / "alpha_summary_latest.json"
    alpha_path.unlink(missing_ok=True)

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary artifact unavailable" in result.stderr


def test_shadow_check_strict_fails_when_alpha_worker_expected_but_not_active(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "ALPHA_WORKER_ENABLED=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_ALPHA_WORKER_ACTIVE": "inactive",
        },
    )

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha worker service expected but not active" in result.stderr


def test_shadow_check_strict_fails_when_breadth_worker_expected_but_not_active(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "BREADTH_WORKER_ENABLED=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_BREADTH_WORKER_ACTIVE": "inactive",
        },
    )

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: breadth worker service expected but not active" in result.stderr


def test_shadow_check_strict_fails_when_discord_route_guard_timer_expected_but_not_active(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_DISCORD_ROUTE_GUARD_TIMER_ACTIVE": "inactive",
        },
    )

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: discord-route-guard timer expected but not active" in result.stderr


def test_shadow_check_strict_fails_when_discord_route_guard_timer_expected_but_not_enabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1",
            "DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_DISCORD_ROUTE_GUARD_TIMER_ACTIVE": "active",
            "MOCK_DISCORD_ROUTE_GUARD_TIMER_ENABLED": "disabled",
        },
    )

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: discord-route-guard timer expected but not enabled" in result.stderr


def test_shadow_check_strict_fails_when_live_status_is_unknown_non_green(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "live_status_latest.json",
        {
            "status": "orange",
            "trigger_flags": {
                "approvals_resumed": False,
                "planned_orders_resumed": False,
            },
            "freshness_plan": {
                "approval_rate_guardrail_status": "within_band",
                "approval_rate_guardrail_evaluated": True,
                "approval_rate": 0.1,
                "metar_observation_stale_rate": 0.0,
            },
            "scan_budget": {
                "effective_max_markets": 100,
            },
            "latest_cycle_metrics": {
                "intents_approved": 1,
                "intents_total": 10,
                "planned_orders": 1,
            },
            "red_reasons": [],
            "yellow_reasons": [],
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: live_status is unknown/non-green" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_artifact_is_stale(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    alpha_path = output_dir / "health" / "alpha_summary_latest.json"
    os.utime(alpha_path, (1, 1))

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary artifact stale" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_artifact_is_stale_with_non_numeric_max_age(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    alpha_path = output_dir / "health" / "alpha_summary_latest.json"
    os.utime(alpha_path, (1, 1))

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS=not-a-number",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary artifact stale" in result.stderr


def test_shadow_check_strict_fails_when_alpha_summary_health_status_is_unknown(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "health": {
                "status": "UNKNOWN",
                "reason_text": "",
            },
            "headline_metrics": {
                "health_status": "UNKNOWN",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "deployment_confidence_score": 10.0,
                "approval_auto_apply_payload_consistent": True,
                "message_quality_overall_pass": True,
                "trader_view_payload_consistent": True,
            },
            "approval_auto_apply": {
                "enabled": False,
                "should_apply": False,
                "applied_in_this_run": False,
                "released_in_this_run": False,
                "apply_reason": "none",
            },
            "trader_view": {
                "mode": "shadow_only",
                "decision_now": "stay_shadow_only",
                "live_recommendation": "no_go_shadow_only",
                "approval_rate": 0.1,
                "confidence_score": 10.0,
            },
        },
    )

    env_file = tmp_path / "shadow.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: alpha summary health status unknown" in result.stderr


def test_shadow_check_strict_fails_when_stale_metrics_timer_expected_but_not_active(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "STALE_METRICS_DRILL_TIMER_EXPECTED=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_STALE_METRICS_DRILL_TIMER_ACTIVE": "inactive",
        },
    )

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: stale-metrics drill timer expected but not active" in result.stderr


def test_shadow_check_strict_fails_when_stale_metrics_timer_expected_but_not_enabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "shadow.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "STALE_METRICS_DRILL_TIMER_EXPECTED=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_shadow_check(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={
            "MOCK_STALE_METRICS_DRILL_TIMER_ACTIVE": "active",
            "MOCK_STALE_METRICS_DRILL_TIMER_ENABLED": "disabled",
        },
    )

    assert result.returncode == 2
    assert "STRICT CHECK FAILED: stale-metrics drill timer expected but not enabled" in result.stderr
