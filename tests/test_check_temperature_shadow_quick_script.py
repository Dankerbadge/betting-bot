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


def _prepare_script_bundle(
    *,
    tmp_path: Path,
    root: Path,
    thread_map_json: str | None = None,
) -> tuple[Path, Path]:
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    src_script = root / "infra" / "digitalocean" / "check_temperature_shadow_quick.sh"
    quick_script = bundle_dir / "check_temperature_shadow_quick.sh"
    shutil.copy2(src_script, quick_script)
    quick_script.chmod(0o755)

    # Keep thread-map checks deterministic so tests only exercise quick-health logic.
    if not thread_map_json:
        thread_map_json = (
            '{"ready_for_apply": true, "route_guard_shared_route_group_count": 0, '
            '"missing_required_in_map": [], "missing_required_in_env": []}'
        )
    fake_thread_map = bundle_dir / "check_discord_thread_map.sh"
    fake_thread_map.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "--env" ]]; then
  shift 2 || true
fi
if [[ "${1:-}" == "--json" ]]; then
  echo '{thread_map_json}'
  exit 0
fi
echo "ok"
""",
        encoding="utf-8",
    )
    fake_thread_map.chmod(0o755)

    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)

    sudo_shim = tool_dir / "sudo"
    sudo_shim.write_text("#!/bin/sh\nexec \"$@\"\n", encoding="utf-8")
    sudo_shim.chmod(0o755)

    # The quick check only asks `systemctl is-active ...`.
    systemctl_shim = tool_dir / "systemctl"
    systemctl_shim.write_text(
        """#!/bin/sh
if [ "$1" = "is-active" ]; then
  echo active
  exit 0
fi
echo active
exit 0
""",
        encoding="utf-8",
    )
    systemctl_shim.chmod(0o755)

    return quick_script, tool_dir


def _write_env_file(
    *,
    env_file: Path,
    output_dir: Path,
    extra_lines: Iterable[str] = (),
) -> None:
    lines = [
        f'OUTPUT_DIR="{output_dir}"',
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
        },
    )
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "headline_metrics": {
                "health_status": "GREEN",
                "confidence_level": "LOW",
                "approval_rate": 0.1,
                "intents_total": 10,
                "intents_approved": 1,
                "planned_orders": 1,
                "top_blocker_reason": "none",
                "suggestion_impact_pool_basis_label": "settled_projection",
                "settled_unique_market_side_total": 1,
                "projected_pnl_on_reference_bankroll_dollars": 1.0,
            },
            "trader_view": {
                "confidence_score": 10.0,
                "selection_confidence_score": 10.0,
            },
        },
    )
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "green",
            "shared_route_group_count": 0,
            "route_remediations": [],
        },
    )
    _write_json(
        output_dir / "health" / "discord_message_audit" / "discord_message_audit_latest.json",
        {
            "overall_score": 98.0,
            "streams": [
                {"score": 94},
            ],
        },
    )


def _run_quick_script(
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


def test_quick_check_strict_fails_when_lane_degraded_streak_hits_threshold(tmp_path: Path) -> None:
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

    env_file = tmp_path / "quick.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DECISION_MATRIX_LANE_STRICT_DEGRADED_THRESHOLD=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "decision_matrix_lane_degraded_streak" in result.stdout
    assert "strict_blocked=true" in result.stdout


def test_quick_check_default_threshold_keeps_green_when_streak_below_threshold(tmp_path: Path) -> None:
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

    env_file = tmp_path / "quick.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert "quick_result: GREEN" in result.stdout
    assert "strict_threshold=6" in result.stdout


def test_quick_check_strict_fails_when_lane_state_required_but_missing(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "quick.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DECISION_MATRIX_LANE_STRICT_REQUIRE_STATE_FILE=1",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "decision_matrix_lane_state_missing" in result.stdout


def test_quick_check_strict_fails_when_thread_map_is_incomplete(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)

    env_file = tmp_path / "quick.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        thread_map_json=(
            '{"ready_for_apply": false, "route_guard_shared_route_group_count": 2, '
            '"missing_required_in_map": ["SHADOW_ALERT_WEBHOOK_THREAD_ID"], '
            '"missing_required_in_env": ["ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID"]}'
        ),
    )
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "thread_map_incomplete" in result.stdout
    assert "next_action: fill /etc/betbot/discord-thread-map.env then run:" in result.stdout


def test_quick_check_strict_fails_when_route_guard_status_is_not_green(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "yellow",
            "shared_route_group_count": 2,
            "route_remediations": [],
        },
    )

    env_file = tmp_path / "quick.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "discord_route_guard_not_green" in result.stdout


def test_quick_check_strict_fails_on_discord_message_readability_regression(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_message_audit" / "discord_message_audit_latest.json",
        {
            "overall_score": 82.0,
            "streams": [
                {"score": 80},
            ],
        },
    )

    env_file = tmp_path / "quick.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "discord_message_readability_regression" in result.stdout


def test_quick_check_strict_fails_on_confidence_pnl_divergence(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "headline_metrics": {
                "health_status": "GREEN",
                "confidence_level": "HIGH",
                "approval_rate": 0.1,
                "intents_total": 10,
                "intents_approved": 1,
                "planned_orders": 1,
                "top_blocker_reason": "none",
                "suggestion_impact_pool_basis_label": "settled_projection",
                "settled_unique_market_side_total": 1,
                "projected_pnl_on_reference_bankroll_dollars": -5.0,
            },
            "trader_view": {
                "confidence_score": 60.0,
                "selection_confidence_score": 60.0,
            },
        },
    )

    env_file = tmp_path / "quick.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "confidence_pnl_divergence" in result.stdout


def test_quick_check_strict_fails_when_live_status_artifact_is_missing(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    (output_dir / "health" / "live_status_latest.json").unlink(missing_ok=True)

    env_file = tmp_path / "quick.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "live_status_missing" in result.stdout


def test_quick_check_strict_fails_when_live_status_artifact_is_stale(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    live_status_file = output_dir / "health" / "live_status_latest.json"
    os.utime(live_status_file, (1, 1))

    env_file = tmp_path / "quick.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "LIVE_STATUS_STRICT_MAX_AGE_SECONDS=5",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "live_status_stale" in result.stdout


def test_quick_check_strict_fails_when_alpha_summary_artifact_is_stale(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    alpha_summary_file = output_dir / "health" / "alpha_summary_latest.json"
    os.utime(alpha_summary_file, (1, 1))

    env_file = tmp_path / "quick.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "ALPHA_SUMMARY_STRICT_MAX_AGE_SECONDS=5",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "alpha_summary_stale" in result.stdout


def test_quick_check_strict_fails_when_route_guard_artifact_is_stale(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    route_guard_file = output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json"
    os.utime(route_guard_file, (1, 1))

    env_file = tmp_path / "quick.env"
    _write_env_file(
        env_file=env_file,
        output_dir=output_dir,
        extra_lines=(
            "DISCORD_ROUTE_GUARD_STRICT_MAX_AGE_SECONDS=5",
        ),
    )
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 2
    assert "route_guard_stale" in result.stdout


def test_quick_check_non_strict_reports_flags_but_exits_zero(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_required_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "discord_route_guard" / "discord_route_guard_latest.json",
        {
            "guard_status": "yellow",
            "shared_route_group_count": 3,
            "route_remediations": [],
        },
    )

    env_file = tmp_path / "quick.env"
    _write_env_file(env_file=env_file, output_dir=output_dir)
    script_path, tool_dir = _prepare_script_bundle(tmp_path=tmp_path, root=root)
    result = _run_quick_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir, strict=False)

    assert result.returncode == 0
    assert "quick_result: YELLOW" in result.stdout
    assert "discord_route_guard_not_green" in result.stdout


def test_quick_check_help_flag_exits_zero_and_prints_usage() -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow_quick.sh"
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "Usage:" in result.stdout


def test_quick_check_unknown_option_fails_with_usage() -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow_quick.sh"
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--bad-option"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Unknown option: --bad-option" in result.stderr
    assert "Usage:" in result.stderr


def test_quick_check_missing_env_flag_value_fails() -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow_quick.sh"
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--env"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert "Missing value for --env" in result.stderr


def test_quick_check_missing_env_file_fails(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "check_temperature_shadow_quick.sh"
    missing_env = tmp_path / "missing.env"
    result = subprocess.run(
        ["/bin/bash", str(script_path), "--env", str(missing_env)],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert f"Missing {missing_env}" in result.stderr
