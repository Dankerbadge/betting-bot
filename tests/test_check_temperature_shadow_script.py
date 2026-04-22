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
case "$cmd" in
  is-active) echo active; exit 0 ;;
  is-enabled) echo enabled; exit 0 ;;
  cat) echo "# stub unit"; exit 0 ;;
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
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
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
