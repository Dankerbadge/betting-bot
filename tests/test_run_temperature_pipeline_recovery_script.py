from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _prepare_fake_betbot_root(tmp_path: Path) -> Path:
    fake_root = tmp_path / "fake_betbot_root"
    python_bin = fake_root / ".venv" / "bin" / "python"
    python_bin.parent.mkdir(parents=True, exist_ok=True)
    python_bin.symlink_to(Path(sys.executable))
    return fake_root


def _write_systemctl_shim(path: Path, *, invocation_log: Path) -> None:
    template = """#!/usr/bin/env bash
set -euo pipefail

args=("$@")
while [[ ${#args[@]} -gt 0 && "${args[0]}" == -* ]]; do
  args=("${args[@]:1}")
done

cmd="${args[0]:-}"
unit="${args[1]:-}"
echo "$*" >> "__INVOCATION_LOG__"

case "$cmd" in
  is-active)
    echo "${MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT:-active}"
    exit 0
    ;;
  is-enabled)
    echo "${MOCK_SYSTEMCTL_IS_ENABLED_DEFAULT:-enabled}"
    exit 0
    ;;
  cat)
    exit 0
    ;;
  show)
    property=""
    if [[ "${args[1]:-}" == "-p" ]]; then
      property="${args[2]:-}"
    fi
    case "$property" in
      SubState)
        echo "${MOCK_SYSTEMCTL_SHOW_SUBSTATE:-running}"
        ;;
      *)
        echo "0"
        ;;
    esac
    exit 0
    ;;
  restart|start|enable|reset-failed)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
"""
    path.write_text(
        template.replace("__INVOCATION_LOG__", str(invocation_log)),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _prepare_script_bundle(
    *,
    tmp_path: Path,
    root: Path,
    include_remediation_script: bool,
    remediation_exit_code: int = 0,
) -> tuple[Path, Path, Path, Path]:
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    src_script = root / "infra" / "digitalocean" / "run_temperature_pipeline_recovery.sh"
    script_path = bundle_dir / src_script.name
    shutil.copy2(src_script, script_path)
    _patch_script_for_legacy_bash(script_path)
    script_path.chmod(0o755)

    remediation_invocations = bundle_dir / "remediation_invocations.log"
    if include_remediation_script:
        remediation_script = bundle_dir / "set_coldmath_recovery_env_persistence_gate.sh"
        remediation_script.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    f'echo "$*" >> "{remediation_invocations}"',
                    f"exit {remediation_exit_code}",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        remediation_script.chmod(0o755)

    tool_dir = tmp_path / "tools"
    tool_dir.mkdir(parents=True, exist_ok=True)

    sudo_shim = tool_dir / "sudo"
    sudo_shim.write_text("#!/bin/sh\nexec \"$@\"\n", encoding="utf-8")
    sudo_shim.chmod(0o755)

    curl_shim = tool_dir / "curl"
    curl_shim.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    curl_shim.chmod(0o755)

    systemctl_invocations = bundle_dir / "systemctl_invocations.log"
    _write_systemctl_shim(tool_dir / "systemctl", invocation_log=systemctl_invocations)
    return script_path, tool_dir, remediation_invocations, systemctl_invocations


def _write_timeout_guardrail_repair_script(
    *,
    bundle_dir: Path,
    exit_code: int = 0,
) -> Path:
    invocation_log = bundle_dir / "timeout_guardrail_repair_invocations.log"
    timeout_script = bundle_dir / "set_coldmath_stage_timeout_guardrails.sh"
    timeout_script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f'echo "$*" >> "{invocation_log}"',
                f"exit {exit_code}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    timeout_script.chmod(0o755)
    return invocation_log


def _patch_script_for_legacy_bash(script_path: Path) -> None:
    text = script_path.read_text(encoding="utf-8")
    old = (
        "parse_extra_args() {\n"
        "  local raw=\"${1:-}\"\n"
        "  local -n dest_ref=\"$2\"\n"
        "  dest_ref=()\n"
        "  if [[ -n \"$raw\" ]]; then\n"
        "    read -r -a dest_ref <<<\"$raw\"\n"
        "  fi\n"
        "}\n"
    )
    new = (
        "parse_extra_args() {\n"
        "  local raw=\"${1:-}\"\n"
        "  local dest_name=\"${2:-}\"\n"
        "  eval \"$dest_name=()\"\n"
        "  if [[ -n \"$raw\" ]]; then\n"
        "    # shellcheck disable=SC2206\n"
        "    local parsed=( $raw )\n"
        "    eval \"$dest_name=(\\\"${parsed[@]}\\\")\"\n"
        "  fi\n"
        "}\n"
    )
    if old in text:
        text = text.replace(old, new, 1)
    text = text.replace(
        'for action in "${action_records[@]}"; do',
        'for action in "${action_records[@]-}"; do',
    )
    script_path.write_text(text, encoding="utf-8")


def _seed_healthy_artifacts(output_dir: Path) -> None:
    _write_json(
        output_dir / "health" / "live_status_latest.json",
        {
            "status": "green",
            "red_reasons": [],
            "yellow_reasons": [],
            "freshness_plan": {
                "pressure_active": False,
                "metar_observation_stale_rate": 0.0,
                "metar_observation_stale_count": 0,
                "approval_rate": 1.0,
            },
            "scan_budget": {
                "effective_max_markets": 200,
                "next_max_markets": 200,
                "adaptive_decision_action": "hold",
                "adaptive_decision_reason": "within_guardrails",
            },
            "command_execution": {
                "metar_attempts": 1,
                "settlement_attempts": 1,
                "shadow_attempts": 1,
            },
            "latest_cycle_metrics": {
                "intents_total": 12,
            },
        },
    )
    _write_json(
        output_dir / "kalshi_temperature_live_readiness_20260101_000000.json",
        {
            "status": "ready",
            "executive_summary": {
                "shortest_horizon_pipeline_status": "green",
                "shortest_horizon_pipeline_reason": "healthy",
            },
        },
    )
    _write_json(
        output_dir / "health" / "alpha_summary_latest.json",
        {
            "status": "ready",
            "health": {
                "status": "GREEN",
            },
        },
    )
    _write_json(
        output_dir / "health" / "log_maintenance" / "log_maintenance_latest.json",
        {
            "status": "ready",
            "health_status": "green",
            "usage": {
                "log_dir_bytes": 100,
            },
        },
    )
    _write_json(
        output_dir / "health" / "readiness_runner_latest.json",
        {
            "run_status": "idle",
            "stage": "idle",
        },
    )


def _write_hardening_status(
    path: Path,
    *,
    status: str,
    stage_statuses: dict[str, str] | None = None,
) -> None:
    payload: dict[str, object] = {
        "status": "ready",
        "recovery_env_persistence": {
            "status": status,
            "changed": False,
            "target_file": "/etc/betbot/temperature-shadow.env",
            "error": "Target env file not found",
        },
    }
    if stage_statuses is not None:
        ordered_stage_names = [
            "coldmath_snapshot_summary",
            "polymarket_market_ingest",
            "kalshi_temperature_recovery_advisor",
            "kalshi_temperature_recovery_loop",
            "kalshi_temperature_recovery_campaign",
        ]
        stage_rows: list[dict[str, object]] = []
        for stage_name in ordered_stage_names:
            if stage_name not in stage_statuses:
                continue
            stage_rows.append(
                {
                    "stage": stage_name,
                    "status": str(stage_statuses.get(stage_name) or "").strip().lower() or "unknown",
                    "exit_code": 0,
                    "duration_seconds": 1,
                    "required": "required" if stage_name == "coldmath_snapshot_summary" else "optional",
                }
            )
        for stage_name, stage_status in stage_statuses.items():
            if stage_name in ordered_stage_names:
                continue
            stage_rows.append(
                {
                    "stage": stage_name,
                    "status": str(stage_status or "").strip().lower() or "unknown",
                    "exit_code": 0,
                    "duration_seconds": 1,
                    "required": "optional",
                }
            )
        payload["stages"] = stage_rows
    _write_json(path, payload)


def _write_timeout_guardrail_hardening_status(path: Path, *, status: str) -> None:
    _write_hardening_status(
        path,
        status=status,
        stage_statuses={
            "coldmath_snapshot_summary": "ok",
            "polymarket_market_ingest": "ok",
            "kalshi_temperature_recovery_advisor": "ok",
            "kalshi_temperature_recovery_loop": "timeout",
            "kalshi_temperature_recovery_campaign": "ok",
        },
    )


def _write_env_file(
    *,
    env_file: Path,
    betbot_root: Path,
    output_dir: Path,
    hardening_status_file: Path,
    strict_gate_enabled: bool,
    extra_lines: tuple[str, ...] = (),
) -> None:
    lines = [
        f'BETBOT_ROOT="{betbot_root}"',
        f'OUTPUT_DIR="{output_dir}"',
        f'COLDMATH_HARDENING_STATUS_FILE="{hardening_status_file}"',
        f"COLDMATH_RECOVERY_ENV_PERSISTENCE_STRICT_FAIL_ON_ERROR={'1' if strict_gate_enabled else '0'}",
        "RECOVERY_REQUIRE_ALPHA_SUMMARY_TIMER=0",
        "RECOVERY_REQUIRE_ALPHA_WORKER=0",
        "RECOVERY_REQUIRE_BREADTH_WORKER=0",
        "RECOVERY_REQUIRE_LOG_MAINTENANCE_TIMER=0",
        "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
        "RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR=0",
    ]
    lines.extend(extra_lines)
    lines.append("")
    env_file.write_text("\n".join(lines), encoding="utf-8")


def _run_recovery_script(
    *,
    script_path: Path,
    env_file: Path,
    tool_dir: Path,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["PATH"] = f"{tool_dir}:{env.get('PATH', '')}"
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        ["/bin/bash", str(script_path), str(env_file)],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        cwd=str(script_path.parent),
    )


def _load_recovery_latest(output_dir: Path) -> dict[str, object]:
    latest = output_dir / "health" / "recovery" / "recovery_latest.json"
    assert latest.exists(), f"missing recovery latest payload: {latest}"
    loaded = json.loads(latest.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _contains_systemctl_start(lines: list[str], unit_name: str) -> bool:
    return any(
        (" start " in f" {line} " or line.startswith("start ")) and unit_name in line
        for line in lines
    )


def _write_curl_capture_shim(
    path: Path,
    *,
    payload_capture: Path,
    invocation_log: Path | None = None,
) -> None:
    invocation_log_line = ""
    if invocation_log is not None:
        invocation_log_line = 'echo "$*" >> "__INVOCATION_LOG__"\n'
    template = """#!/usr/bin/env bash
set -euo pipefail

payload=""
args=("$@")
__INVOCATION_LOG_LINE__
for ((idx=0; idx<${#args[@]}; idx++)); do
  if [[ "${args[$idx]}" == "--data-binary" ]]; then
    next_idx=$((idx + 1))
    if (( next_idx < ${#args[@]} )); then
      payload="${args[$next_idx]}"
    fi
    break
  fi
done

printf '%s' "$payload" > "__PAYLOAD_CAPTURE__"
exit 0
"""
    path.write_text(
        template.replace("__PAYLOAD_CAPTURE__", str(payload_capture))
        .replace("__INVOCATION_LOG_LINE__", invocation_log_line)
        .replace("__INVOCATION_LOG__", str(invocation_log) if invocation_log is not None else ""),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _read_webhook_payload_lines(payload_capture: Path) -> list[str]:
    assert payload_capture.exists(), "expected webhook payload capture file"
    captured_payload = json.loads(payload_capture.read_text(encoding="utf-8"))
    assert isinstance(captured_payload, dict)
    webhook_content = captured_payload.get("content")
    assert isinstance(webhook_content, str)
    lines = [line.strip() for line in webhook_content.splitlines() if line.strip()]
    assert lines
    return lines


def _project_webhook_line_prefixes(lines: list[str]) -> list[str]:
    known_prefixes = (
        "State:",
        "Status:",
        "What happened:",
        "Auto actions:",
        "Recovery:",
        "Pressure:",
        "Why it matters:",
        "Services:",
        "Freshness and scan:",
        "Log maintenance:",
        "Event:",
        "Event file:",
        "Next step:",
    )
    projected: list[str] = []
    for idx, line in enumerate(lines):
        if idx == 0:
            projected.append("HEADER")
            continue
        matched = next((prefix for prefix in known_prefixes if line.startswith(prefix)), None)
        assert matched is not None, f"unexpected webhook line format: {line!r}"
        projected.append(matched)
    return projected


def _prepare_status_change_test_context(
    *,
    tmp_path: Path,
    hardening_status: str,
    webhook_payload_capture_name: str,
    curl_invocation_log_name: str,
    alert_state_file_name: str,
    extra_env_lines: tuple[str, ...] = (),
) -> tuple[Path, Path, Path, Path, Path, Path]:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status=hardening_status)

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / webhook_payload_capture_name
    curl_invocation_log = tmp_path / curl_invocation_log_name
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    alert_state_file = tmp_path / alert_state_file_name
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    default_status_change_env_lines = (
        'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
        "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
        "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
        f'RECOVERY_ALERT_STATE_FILE="{alert_state_file}"',
    )
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=default_status_change_env_lines + extra_env_lines,
    )

    return output_dir, script_path, tool_dir, env_file, alert_state_file, curl_invocation_log


def _run_status_change_step(
    *,
    script_path: Path,
    env_file: Path,
    tool_dir: Path,
    output_dir: Path,
    alert_state_file: Path,
    curl_invocation_log: Path,
    expected_curl_invocations: int,
    extra_env: dict[str, str] | None = None,
) -> tuple[dict[str, object], dict[str, object], str]:
    result = _run_recovery_script(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env=extra_env,
    )
    assert result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == expected_curl_invocations

    payload = _load_recovery_latest(output_dir)
    state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(state, dict)
    fingerprint = state.get("last_fingerprint")
    assert isinstance(fingerprint, str)
    assert fingerprint
    fingerprint_payload = json.loads(fingerprint)
    assert isinstance(fingerprint_payload, dict)
    return payload, fingerprint_payload, fingerprint


def test_recovery_auto_remediation_runs_when_persistence_error_and_strict_gate_enabled(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, remediation_invocations, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert any("recovery_env_persistence" in str(action) for action in actions)
    persistence = payload.get("recovery_env_persistence")
    assert isinstance(persistence, dict)
    assert persistence.get("status") == "error"
    assert persistence.get("strict_blocked") is True
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)
    assert "coldmath_stage_timeout_stage_timeouts" in decision_reasons
    guardrails = payload.get("coldmath_stage_timeout_guardrails")
    assert isinstance(guardrails, dict)
    assert guardrails.get("status") == "disabled"
    assert guardrails.get("stage_telemetry_status") == "timeout"
    assert guardrails.get("timeout_stages") == ["recovery_loop"]
    state = payload.get("state")
    assert isinstance(state, dict)
    repair_epoch = state.get("last_recovery_env_persistence_repair_epoch")
    assert isinstance(repair_epoch, int)
    assert repair_epoch > 0
    assert _read_lines(remediation_invocations), "expected remediation script invocation"


def test_recovery_auto_remediation_does_not_run_when_strict_gate_disabled(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, remediation_invocations, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=False,
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert not any("recovery_env_persistence" in str(action) for action in actions)
    assert _read_lines(remediation_invocations) == []


def test_recovery_auto_remediation_records_disabled_when_env_switch_off(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, remediation_invocations, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "RECOVERY_ENABLE_ENV_PERSISTENCE_REPAIR=0",
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert any(
        "repair_recovery_env_persistence_gate:disabled" == str(action)
        for action in actions
    )
    assert "coldmath_stage_timeout_stage_timeouts" in (payload.get("decision_reasons") or [])
    guardrails = payload.get("coldmath_stage_timeout_guardrails")
    assert isinstance(guardrails, dict)
    assert guardrails.get("status") == "disabled"
    assert guardrails.get("stage_telemetry_status") == "timeout"
    assert _read_lines(remediation_invocations) == []


def test_recovery_auto_remediation_records_missing_script_and_keeps_issue_flagged(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="execution_failed")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert any(
        "recovery_env_persistence" in str(action) and "missing" in str(action)
        for action in actions
    )
    assert "coldmath_stage_timeout_stage_timeouts" in (payload.get("decision_reasons") or [])
    guardrails = payload.get("coldmath_stage_timeout_guardrails")
    assert isinstance(guardrails, dict)
    assert guardrails.get("status") == "disabled"
    assert guardrails.get("stage_telemetry_status") == "timeout"
    assert payload.get("issue_remaining") is True


def test_recovery_auto_remediation_respects_cooldown_and_skips_reinvocation(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="error")

    # Seed state as if remediation just ran, so the cooldown guard should block re-run.
    _write_json(
        output_dir / "health" / "recovery" / ".recovery_state.json",
        {
            "last_recovery_env_persistence_repair_epoch": int(time.time()),
        },
    )

    script_path, tool_dir, remediation_invocations, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "RECOVERY_ENV_PERSISTENCE_REPAIR_COOLDOWN_SECONDS=3600",
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert any(
        "repair_recovery_env_persistence_gate:cooldown" == str(action)
        for action in actions
    )
    assert "coldmath_stage_timeout_stage_timeouts" in (payload.get("decision_reasons") or [])
    guardrails = payload.get("coldmath_stage_timeout_guardrails")
    assert isinstance(guardrails, dict)
    assert guardrails.get("status") == "disabled"
    assert guardrails.get("stage_telemetry_status") == "timeout"
    assert _read_lines(remediation_invocations) == []


def test_env_repair_success_triggers_coldmath_hardening_service(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, remediation_invocations, systemctl_invocations = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "repair_recovery_env_persistence_gate:ok" in actions
    assert "trigger_coldmath_hardening_after_env_repair:ok" in actions
    assert _read_lines(remediation_invocations), "expected remediation script invocation"
    systemctl_lines = _read_lines(systemctl_invocations)
    assert _contains_systemctl_start(
        systemctl_lines,
        "betbot-temperature-coldmath-hardening.service",
    )


def test_stage_timeout_guardrail_repair_runs_when_stage_timeout_detected(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="ok")

    script_path, tool_dir, _, systemctl_invocations = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    timeout_repair_invocations = _write_timeout_guardrail_repair_script(
        bundle_dir=script_path.parent
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=False,
        extra_lines=(
            "RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR=1",
            "RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_GLOBAL_SECONDS=777",
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)
    assert "coldmath_stage_timeout_stage_timeouts" in decision_reasons
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "repair_coldmath_stage_timeout_guardrails:ok" in actions
    assert "trigger_coldmath_hardening_after_stage_timeout_repair:ok" in actions
    state = payload.get("state")
    assert isinstance(state, dict)
    repair_epoch = state.get("last_coldmath_stage_timeout_guardrail_repair_epoch")
    assert isinstance(repair_epoch, int)
    assert repair_epoch > 0

    invocation_lines = _read_lines(timeout_repair_invocations)
    assert invocation_lines, "expected timeout guardrail repair script invocation"
    assert any("--global-seconds 777" in line and str(env_file) in line for line in invocation_lines)
    systemctl_lines = _read_lines(systemctl_invocations)
    assert _contains_systemctl_start(
        systemctl_lines,
        "betbot-temperature-coldmath-hardening.service",
    )


def test_stage_timeout_guardrail_repair_records_disabled_when_switch_off(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="ok")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    timeout_repair_invocations = _write_timeout_guardrail_repair_script(
        bundle_dir=script_path.parent
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=False,
        extra_lines=(
            "RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR=0",
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "repair_coldmath_stage_timeout_guardrails:disabled" in actions
    assert _read_lines(timeout_repair_invocations) == []


def test_stage_timeout_guardrail_repair_records_missing_script(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="ok")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=False,
        extra_lines=(
            "RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR=1",
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "repair_coldmath_stage_timeout_guardrails:missing_script" in actions
    assert payload.get("issue_remaining") is True


def test_stage_timeout_guardrail_repair_respects_cooldown(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)
    _write_json(
        output_dir / "health" / "recovery" / ".recovery_state.json",
        {
            "last_coldmath_stage_timeout_guardrail_repair_epoch": int(time.time()),
        },
    )

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="ok")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    timeout_repair_invocations = _write_timeout_guardrail_repair_script(
        bundle_dir=script_path.parent
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=False,
        extra_lines=(
            "RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR=1",
            "RECOVERY_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR_COOLDOWN_SECONDS=3600",
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "repair_coldmath_stage_timeout_guardrails:cooldown" in actions
    assert _read_lines(timeout_repair_invocations) == []


def test_stage_timeout_repair_hardening_trigger_records_disabled_when_switch_off(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="ok")

    script_path, tool_dir, _, systemctl_invocations = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    timeout_repair_invocations = _write_timeout_guardrail_repair_script(
        bundle_dir=script_path.parent
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=False,
        extra_lines=(
            "RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR=1",
            "RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_STAGE_TIMEOUT_REPAIR=0",
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "repair_coldmath_stage_timeout_guardrails:ok" in actions
    assert "trigger_coldmath_hardening_after_stage_timeout_repair:disabled" in actions
    assert _read_lines(timeout_repair_invocations), "expected timeout guardrail repair invocation"
    systemctl_lines = _read_lines(systemctl_invocations)
    assert not _contains_systemctl_start(
        systemctl_lines,
        "betbot-temperature-coldmath-hardening.service",
    )


def test_action_records_have_humanize_mapping_coverage() -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "run_temperature_pipeline_recovery.sh"
    script_text = script_path.read_text(encoding="utf-8")

    action_codes = {
        match.group(1)
        for match in re.finditer(r'action_records\+=\("([^"]+)"\)', script_text)
    }
    assert action_codes, "no action_records codes found in recovery script"

    humanize_idx = script_text.find("def humanize(value: str) -> str:")
    assert humanize_idx >= 0, "humanize() function not found in recovery script"

    mapping_idx = script_text.find("mapping = {", humanize_idx)
    assert mapping_idx >= 0, "humanize() mapping block not found in recovery script"

    mapping_open = script_text.find("{", mapping_idx)
    assert mapping_open >= 0, "mapping opening brace not found"

    depth = 0
    mapping_close = -1
    for idx in range(mapping_open, len(script_text)):
        ch = script_text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                mapping_close = idx
                break
    assert mapping_close > mapping_open, "mapping closing brace not found"

    mapping_block = script_text[mapping_open : mapping_close + 1]
    mapping_keys = {
        match.group(2)
        for match in re.finditer(r'^\s*([\'"])([^\'"]+)\1\s*:', mapping_block, re.MULTILINE)
    }
    assert mapping_keys, "no mapping keys found in humanize() mapping block"

    def normalize_action_code(code: str) -> str:
        normalized = code.strip()
        dynamic_idx = normalized.find("${")
        if dynamic_idx >= 0:
            normalized = normalized[:dynamic_idx].rstrip(":")
        return normalized

    normalized_action_codes = {normalize_action_code(code) for code in action_codes}
    missing_codes = sorted(code for code in normalized_action_codes if code not in mapping_keys)
    assert not missing_codes, (
        "missing humanize() mapping entries for action codes: "
        + ", ".join(missing_codes)
    )


def test_decision_reasons_have_humanize_mapping_coverage() -> None:
    root = Path(__file__).resolve().parents[1]
    script_path = root / "infra" / "digitalocean" / "run_temperature_pipeline_recovery.sh"
    script_text = script_path.read_text(encoding="utf-8")

    decision_reason_codes = {
        match.group(1)
        for match in re.finditer(r'decision_reasons\+=\("([^"]+)"\)', script_text)
    }
    assert decision_reason_codes, "no decision_reasons codes found in recovery script"

    humanize_idx = script_text.find("def humanize(value: str) -> str:")
    assert humanize_idx >= 0, "humanize() function not found in recovery script"

    mapping_idx = script_text.find("mapping = {", humanize_idx)
    assert mapping_idx >= 0, "humanize() mapping block not found in recovery script"

    mapping_open = script_text.find("{", mapping_idx)
    assert mapping_open >= 0, "mapping opening brace not found"

    depth = 0
    mapping_close = -1
    for idx in range(mapping_open, len(script_text)):
        ch = script_text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                mapping_close = idx
                break
    assert mapping_close > mapping_open, "mapping closing brace not found"

    mapping_block = script_text[mapping_open : mapping_close + 1]
    mapping_keys = {
        match.group(2)
        for match in re.finditer(r'^\s*([\'"])([^\'"]+)\1\s*:', mapping_block, re.MULTILINE)
    }
    assert mapping_keys, "no mapping keys found in humanize() mapping block"

    def normalize_reason_code(code: str) -> set[str]:
        normalized = code.strip()
        dynamic_idx = normalized.find("${")
        if dynamic_idx < 0:
            return {normalized}
        dynamic_prefix = normalized[:dynamic_idx]
        return {
            f"{dynamic_prefix}{suffix}"
            for suffix in ("inactive", "failed", "unknown")
        }

    normalized_reason_codes = {
        variant
        for code in decision_reason_codes
        for variant in normalize_reason_code(code)
    }
    missing_codes = sorted(code for code in normalized_reason_codes if code not in mapping_keys)
    assert not missing_codes, (
        "missing humanize() mapping entries for decision reason codes: "
        + ", ".join(missing_codes)
    )


def test_coldmath_stage_timeout_guardrails_unavailable_without_stage_telemetry(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="ready")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)
    assert "coldmath_stage_timeout_guardrails_invalid" not in decision_reasons
    assert "coldmath_stage_timeout_guardrails_disabled" not in decision_reasons
    assert "coldmath_stage_timeout_stage_timeouts" not in decision_reasons

    guardrails = payload.get("coldmath_stage_timeout_guardrails")
    assert isinstance(guardrails, dict)
    assert guardrails.get("status") == "unavailable"
    assert guardrails.get("stage_telemetry_status") == "unavailable"
    assert guardrails.get("strict_blocked") is False
    assert guardrails.get("required_keys") == [
        "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS",
        "COLDMATH_MARKET_INGEST_TIMEOUT_SECONDS",
        "COLDMATH_RECOVERY_ADVISOR_TIMEOUT_SECONDS",
        "COLDMATH_RECOVERY_LOOP_TIMEOUT_SECONDS",
        "COLDMATH_RECOVERY_CAMPAIGN_TIMEOUT_SECONDS",
    ]
    assert guardrails.get("required_stages") == ["snapshot", "market_ingest", "recovery_advisor", "recovery_loop", "recovery_campaign"]
    assert guardrails.get("timeout_stages") == []


def test_recovery_emits_invalid_stage_timeout_guardrail_reason_when_strict_required(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(
        hardening_status_file,
        status="ready",
        stage_statuses={
            "coldmath_snapshot_summary": "ok",
        },
    )

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=invalid",
            "COLDMATH_MARKET_INGEST_ENABLED=0",
            "COLDMATH_RECOVERY_ADVISOR_ENABLED=0",
            "COLDMATH_RECOVERY_LOOP_ENABLED=0",
            "COLDMATH_RECOVERY_CAMPAIGN_ENABLED=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    assert payload.get("issue_detected") is True
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)
    assert "coldmath_stage_timeout_guardrails_invalid" in decision_reasons
    assert "coldmath_stage_timeout_guardrails_disabled" not in decision_reasons
    assert "coldmath_stage_timeout_stage_timeouts" not in decision_reasons

    guardrails = payload.get("coldmath_stage_timeout_guardrails")
    assert isinstance(guardrails, dict)
    assert guardrails.get("status") == "invalid"
    assert guardrails.get("strict_required") is True
    assert guardrails.get("strict_blocked") is True
    assert guardrails.get("required_keys") == ["COLDMATH_SNAPSHOT_TIMEOUT_SECONDS"]
    assert guardrails.get("invalid_keys") == ["COLDMATH_SNAPSHOT_TIMEOUT_SECONDS"]
    assert guardrails.get("disabled_keys") == []
    assert guardrails.get("required_stages") == ["snapshot"]
    assert guardrails.get("timeout_stages") == []


def test_recovery_emits_disabled_stage_timeout_guardrail_reason_when_strict_required(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(
        hardening_status_file,
        status="ready",
        stage_statuses={
            "coldmath_snapshot_summary": "ok",
        },
    )

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
            "COLDMATH_SNAPSHOT_TIMEOUT_SECONDS=0",
            "COLDMATH_MARKET_INGEST_ENABLED=0",
            "COLDMATH_RECOVERY_ADVISOR_ENABLED=0",
            "COLDMATH_RECOVERY_LOOP_ENABLED=0",
            "COLDMATH_RECOVERY_CAMPAIGN_ENABLED=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    assert payload.get("issue_detected") is True
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)
    assert "coldmath_stage_timeout_guardrails_invalid" not in decision_reasons
    assert "coldmath_stage_timeout_guardrails_disabled" in decision_reasons
    assert "coldmath_stage_timeout_stage_timeouts" not in decision_reasons

    guardrails = payload.get("coldmath_stage_timeout_guardrails")
    assert isinstance(guardrails, dict)
    assert guardrails.get("status") == "disabled"
    assert guardrails.get("strict_required") is True
    assert guardrails.get("strict_blocked") is True
    assert guardrails.get("required_keys") == ["COLDMATH_SNAPSHOT_TIMEOUT_SECONDS"]
    assert guardrails.get("invalid_keys") == []
    assert guardrails.get("disabled_keys") == ["COLDMATH_SNAPSHOT_TIMEOUT_SECONDS"]
    assert guardrails.get("required_stages") == ["snapshot"]
    assert guardrails.get("timeout_stages") == []


def test_recovery_emits_stage_timeout_reason_when_required_stage_times_out(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(
        hardening_status_file,
        status="ready",
        stage_statuses={
            "coldmath_snapshot_summary": "ok",
            "polymarket_market_ingest": "ok",
            "kalshi_temperature_recovery_advisor": "ok",
            "kalshi_temperature_recovery_loop": "timeout",
            "kalshi_temperature_recovery_campaign": "ok",
        },
    )

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "COLDMATH_STAGE_TIMEOUT_SECONDS=900",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    assert payload.get("issue_detected") is True
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)
    assert "coldmath_stage_timeout_stage_timeouts" in decision_reasons
    assert "coldmath_stage_timeout_guardrails_invalid" not in decision_reasons
    assert "coldmath_stage_timeout_guardrails_disabled" not in decision_reasons

    guardrails = payload.get("coldmath_stage_timeout_guardrails")
    assert isinstance(guardrails, dict)
    assert guardrails.get("status") == "ok"
    assert guardrails.get("strict_required") is True
    assert guardrails.get("strict_blocked") is False
    assert guardrails.get("stage_telemetry_status") == "timeout"
    assert guardrails.get("timeout_stages") == ["recovery_loop"]

def test_activating_state_normalizes_to_inactive_reason_codes(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
    )

    result = _run_recovery_script(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={"MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT": "activating"},
    )

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)

    reason_codes = {str(reason) for reason in decision_reasons}
    assert all("activating" not in reason for reason in reason_codes), reason_codes
    assert "shadow_service_inactive" in reason_codes
    assert "reporting_timer_inactive" in reason_codes


def test_concise_webhook_payload_prefix_template_regression(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    live_status_file = output_dir / "health" / "live_status_latest.json"
    live_status_payload = json.loads(live_status_file.read_text(encoding="utf-8"))
    assert isinstance(live_status_payload, dict)
    command_execution = live_status_payload.get("command_execution")
    assert isinstance(command_execution, dict)
    command_execution["settlement_attempts"] = 2
    _write_json(live_status_file, live_status_payload)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="execution_failed")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_prefix_concise.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=webhook_payload_capture)

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    lines = _read_webhook_payload_lines(webhook_payload_capture)

    payload = _load_recovery_latest(output_dir)
    assert payload.get("issue_detected") is True
    assert payload.get("issue_remaining") is True
    retries = payload.get("execution_attempts")
    assert isinstance(retries, dict)
    assert retries.get("metar") == 1
    assert retries.get("settlement") == 2
    assert retries.get("shadow") == 1

    projected_prefixes = _project_webhook_line_prefixes(lines)
    assert projected_prefixes == [
        "HEADER",
        "State:",
        "What happened:",
        "Auto actions:",
        "Pressure:",
        "Why it matters:",
        "Next step:",
    ]
    assert not any(
        line.startswith(("Status:", "Recovery:", "Services:", "Freshness and scan:", "Log maintenance:", "Event file:"))
        for line in lines
    )


def test_non_concise_webhook_payload_prefix_template_regression(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_prefix_non_concise.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=webhook_payload_capture)

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=verbose",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    lines = _read_webhook_payload_lines(webhook_payload_capture)

    payload = _load_recovery_latest(output_dir)
    assert payload.get("issue_detected") is True
    actions_attempted = payload.get("actions_attempted") or []
    assert isinstance(actions_attempted, list)
    assert actions_attempted

    projected_prefixes = _project_webhook_line_prefixes(lines)
    assert projected_prefixes == [
        "HEADER",
        "Status:",
        "What happened:",
        "Auto actions:",
        "Recovery:",
        "Why it matters:",
        "Services:",
        "Freshness and scan:",
        "Log maintenance:",
        "Event file:",
        "Next step:",
    ]
    assert not any(line.startswith(("State:", "Pressure:", "Event:", "Issue:")) for line in lines)


def test_status_change_only_suppresses_webhook_for_high_churn_telemetry_changes(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_status_change_only.json"
    curl_invocation_log = tmp_path / "curl_invocations.log"
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
        ),
    )

    first_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert first_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    live_status_file = output_dir / "health" / "live_status_latest.json"
    live_status_payload = json.loads(live_status_file.read_text(encoding="utf-8"))
    assert isinstance(live_status_payload, dict)
    freshness_plan = live_status_payload.get("freshness_plan")
    assert isinstance(freshness_plan, dict)
    freshness_plan["metar_observation_stale_rate"] = 0.05
    freshness_plan["approval_rate"] = 0.91
    scan_budget = live_status_payload.get("scan_budget")
    assert isinstance(scan_budget, dict)
    scan_budget["effective_max_markets"] = 150
    scan_budget["next_max_markets"] = 140
    scan_budget["adaptive_decision_action"] = "trim"
    scan_budget["adaptive_decision_reason"] = "volatility_guardrail"
    latest_cycle_metrics = live_status_payload.get("latest_cycle_metrics")
    assert isinstance(latest_cycle_metrics, dict)
    latest_cycle_metrics["intents_total"] = 50
    _write_json(live_status_file, live_status_payload)

    second_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert second_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1


def test_status_change_only_suppresses_webhook_when_only_trim_scan_budget_detail_changes(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    live_status_file = output_dir / "health" / "live_status_latest.json"
    live_status_payload = json.loads(live_status_file.read_text(encoding="utf-8"))
    assert isinstance(live_status_payload, dict)
    freshness_plan = live_status_payload.get("freshness_plan")
    assert isinstance(freshness_plan, dict)
    freshness_plan["pressure_active"] = True
    latest_cycle_metrics = live_status_payload.get("latest_cycle_metrics")
    assert isinstance(latest_cycle_metrics, dict)
    latest_cycle_metrics["intents_total"] = 200
    scan_budget = live_status_payload.get("scan_budget")
    assert isinstance(scan_budget, dict)
    scan_budget["effective_max_markets"] = 600
    scan_budget["next_max_markets"] = 600
    _write_json(live_status_file, live_status_payload)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="ready")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_status_change_only_trim_details.json"
    curl_invocation_log = tmp_path / "curl_invocations_status_change_only_trim_details.log"
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
            "RECOVERY_FRESHNESS_PRESSURE_CONSECUTIVE_THRESHOLD=1",
            "RECOVERY_SCAN_BUDGET_TRIM_COOLDOWN_SECONDS=0",
            "RECOVERY_ENABLE_METAR_REFRESH=0",
            "RECOVERY_ENABLE_REPORTING_TRIGGER=0",
            "RECOVERY_ENABLE_SERVICE_RESTARTS=0",
            "RECOVERY_ENABLE_SCAN_BUDGET_TRIM=1",
        ),
    )

    first_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert first_result.returncode == 0
    first_payload = _load_recovery_latest(output_dir)
    assert first_payload.get("issue_detected") is True
    first_actions = first_payload.get("actions_attempted") or []
    assert isinstance(first_actions, list)
    assert "trim_scan_budget:ok:600->510" in first_actions
    assert len(_read_lines(curl_invocation_log)) == 1

    live_status_payload = json.loads(live_status_file.read_text(encoding="utf-8"))
    assert isinstance(live_status_payload, dict)
    scan_budget = live_status_payload.get("scan_budget")
    assert isinstance(scan_budget, dict)
    scan_budget["effective_max_markets"] = 620
    scan_budget["next_max_markets"] = 620
    _write_json(live_status_file, live_status_payload)

    second_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert second_result.returncode == 0
    second_payload = _load_recovery_latest(output_dir)
    assert second_payload.get("issue_detected") is True
    second_actions = second_payload.get("actions_attempted") or []
    assert isinstance(second_actions, list)
    assert "trim_scan_budget:ok:620->527" in second_actions
    assert len(_read_lines(curl_invocation_log)) == 1


def test_status_change_only_emits_when_recovery_effectiveness_route_set_changes(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    advisor_latest = output_dir / "health" / "kalshi_temperature_recovery_advisor_latest.json"
    _write_json(
        advisor_latest,
        {
            "metrics": {
                "recovery_effectiveness": {
                    "summary_available": True,
                    "persistently_harmful_actions": ["reduce_negative_expectancy_regimes"],
                }
            },
            "remediation_plan": {
                "demoted_actions_for_effectiveness": ["reduce_negative_expectancy_regimes"],
            },
        },
    )

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_status_change_only_effectiveness.json"
    curl_invocation_log = tmp_path / "curl_invocations_status_change_only_effectiveness.log"
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
        ),
    )

    first_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert first_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    _write_json(
        advisor_latest,
        {
            "metrics": {
                "recovery_effectiveness": {
                    "summary_available": True,
                    "persistently_harmful_actions": ["tighten_weather_pattern_confidence_floor"],
                }
            },
            "remediation_plan": {
                "demoted_actions_for_effectiveness": ["tighten_weather_pattern_confidence_floor"],
            },
        },
    )

    second_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert second_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 2

    lines = _read_webhook_payload_lines(webhook_payload_capture)
    why_line = next((line for line in lines if line.startswith("Why it matters:")), "")
    assert "top route tighten weather pattern" in why_line


def test_status_change_only_suppresses_webhook_when_event_filename_changes_between_runs(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_status_change_only_event_file.json"
    curl_invocation_log = tmp_path / "curl_invocations_status_change_only_event_file.log"
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
        ),
    )

    first_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert first_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    recovery_dir = output_dir / "health" / "recovery"
    first_event_files = sorted(recovery_dir.glob("recovery_event_*.json"))
    assert len(first_event_files) == 1
    first_event_name = first_event_files[0].name

    first_payload = _load_recovery_latest(output_dir)
    assert first_payload.get("issue_detected") is True
    first_decision_reasons = first_payload.get("decision_reasons")
    first_actions_attempted = first_payload.get("actions_attempted")
    assert isinstance(first_decision_reasons, list)
    assert isinstance(first_actions_attempted, list)

    time.sleep(1.1)

    second_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert second_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    second_event_files = sorted(recovery_dir.glob("recovery_event_*.json"))
    assert len(second_event_files) == 2
    second_event_name = second_event_files[-1].name
    assert second_event_name != first_event_name

    second_payload = _load_recovery_latest(output_dir)
    assert second_payload.get("issue_detected") is True
    second_decision_reasons = second_payload.get("decision_reasons")
    second_actions_attempted = second_payload.get("actions_attempted")
    assert isinstance(second_decision_reasons, list)
    assert isinstance(second_actions_attempted, list)
    assert second_decision_reasons == first_decision_reasons
    assert second_actions_attempted == first_actions_attempted


def test_status_change_only_recovers_from_malformed_alert_state_file_and_suppresses_repeat(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_malformed_state.json"
    curl_invocation_log = tmp_path / "curl_invocations_malformed_state.log"
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    alert_state_file = tmp_path / "recovery_alert_state_malformed.json"
    alert_state_file.write_text("{this-is-not-valid-json", encoding="utf-8")

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
            f'RECOVERY_ALERT_STATE_FILE="{alert_state_file}"',
        ),
    )

    first_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert first_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    first_payload = _load_recovery_latest(output_dir)
    assert first_payload.get("issue_detected") is True

    rewritten_state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(rewritten_state, dict)
    first_fingerprint = rewritten_state.get("last_fingerprint")
    assert isinstance(first_fingerprint, str)
    assert first_fingerprint

    second_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert second_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    second_rewritten_state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(second_rewritten_state, dict)
    assert second_rewritten_state.get("last_fingerprint") == first_fingerprint


def test_status_change_only_recovers_from_partial_alert_state_file_and_suppresses_repeat(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_partial_state.json"
    curl_invocation_log = tmp_path / "curl_invocations_partial_state.log"
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    alert_state_file = tmp_path / "recovery_alert_state_partial.json"
    alert_state_file.write_text(
        json.dumps({"schema_version": 1, "previous_status": "open"}, indent=2),
        encoding="utf-8",
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
            f'RECOVERY_ALERT_STATE_FILE="{alert_state_file}"',
        ),
    )

    first_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert first_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    first_payload = _load_recovery_latest(output_dir)
    assert first_payload.get("issue_detected") is True

    rewritten_state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(rewritten_state, dict)
    first_fingerprint = rewritten_state.get("last_fingerprint")
    assert isinstance(first_fingerprint, str)
    assert first_fingerprint

    second_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert second_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    second_rewritten_state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(second_rewritten_state, dict)
    assert second_rewritten_state.get("last_fingerprint") == first_fingerprint


def test_status_change_only_recovers_from_list_alert_state_file_and_suppresses_repeat(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_list_state.json"
    curl_invocation_log = tmp_path / "curl_invocations_list_state.log"
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    alert_state_file = tmp_path / "recovery_alert_state_list.json"
    alert_state_file.write_text(json.dumps(["stale", "state"], indent=2), encoding="utf-8")

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
            f'RECOVERY_ALERT_STATE_FILE="{alert_state_file}"',
        ),
    )

    first_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert first_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    first_payload = _load_recovery_latest(output_dir)
    assert first_payload.get("issue_detected") is True

    rewritten_state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(rewritten_state, dict)
    first_fingerprint = rewritten_state.get("last_fingerprint")
    assert isinstance(first_fingerprint, str)
    assert first_fingerprint

    second_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert second_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    second_rewritten_state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(second_rewritten_state, dict)
    assert second_rewritten_state.get("last_fingerprint") == first_fingerprint


def test_status_change_only_recovers_from_scalar_alert_state_file_and_suppresses_repeat(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_scalar_state.json"
    curl_invocation_log = tmp_path / "curl_invocations_scalar_state.log"
    _write_curl_capture_shim(
        tool_dir / "curl",
        payload_capture=webhook_payload_capture,
        invocation_log=curl_invocation_log,
    )

    alert_state_file = tmp_path / "recovery_alert_state_scalar.json"
    alert_state_file.write_text(json.dumps("stale-state"), encoding="utf-8")

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=1",
            "RECOVERY_ENABLE_SERVICE_RESTARTS=0",
            f'RECOVERY_ALERT_STATE_FILE="{alert_state_file}"',
        ),
    )

    first_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert first_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    first_payload = _load_recovery_latest(output_dir)
    assert first_payload.get("issue_detected") is True

    rewritten_state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(rewritten_state, dict)
    first_fingerprint = rewritten_state.get("last_fingerprint")
    assert isinstance(first_fingerprint, str)
    assert first_fingerprint

    second_result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert second_result.returncode == 0
    assert len(_read_lines(curl_invocation_log)) == 1

    second_rewritten_state = json.loads(alert_state_file.read_text(encoding="utf-8"))
    assert isinstance(second_rewritten_state, dict)
    assert second_rewritten_state.get("last_fingerprint") == first_fingerprint


def test_status_change_only_resends_when_fingerprint_relevant_service_state_changes(
    tmp_path: Path,
) -> None:
    (
        output_dir,
        script_path,
        tool_dir,
        env_file,
        alert_state_file,
        curl_invocation_log,
    ) = _prepare_status_change_test_context(
        tmp_path=tmp_path,
        hardening_status="error",
        webhook_payload_capture_name="captured_webhook_payload_fingerprint_change.json",
        curl_invocation_log_name="curl_invocations_fingerprint_change.log",
        alert_state_file_name="recovery_alert_state_fingerprint_change.json",
    )

    first_payload, _, first_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
    )
    assert first_payload.get("issue_detected") is True
    first_services = first_payload.get("service_states")
    assert isinstance(first_services, dict)
    assert first_services.get("shadow_service") == "active"

    _, second_fingerprint_payload, second_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
    )
    assert second_fingerprint == first_fingerprint
    assert second_fingerprint_payload.get("shadow_service") == "active"

    third_payload, _, third_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=2,
        extra_env={"MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT": "failed"},
    )

    assert third_payload.get("issue_detected") is True
    third_services = third_payload.get("service_states")
    assert isinstance(third_services, dict)
    assert third_services.get("shadow_service") == "failed"
    assert third_fingerprint != first_fingerprint


def test_status_change_only_resends_when_fingerprint_relevant_log_maintenance_health_changes(
    tmp_path: Path,
) -> None:
    (
        output_dir,
        script_path,
        tool_dir,
        env_file,
        alert_state_file,
        curl_invocation_log,
    ) = _prepare_status_change_test_context(
        tmp_path=tmp_path,
        hardening_status="error",
        webhook_payload_capture_name="captured_webhook_payload_log_maintenance_change.json",
        curl_invocation_log_name="curl_invocations_log_maintenance_change.log",
        alert_state_file_name="recovery_alert_state_log_maintenance_change.json",
    )

    first_payload, first_fingerprint_payload, first_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
    )
    assert first_payload.get("issue_detected") is True
    assert first_fingerprint_payload.get("log_maintenance_health_status") == "green"

    _, second_fingerprint_payload, second_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
    )
    assert second_fingerprint == first_fingerprint
    assert second_fingerprint_payload == first_fingerprint_payload

    log_maintenance_file = output_dir / "health" / "log_maintenance" / "log_maintenance_latest.json"
    log_maintenance_payload = json.loads(log_maintenance_file.read_text(encoding="utf-8"))
    assert isinstance(log_maintenance_payload, dict)
    log_maintenance_payload["health_status"] = "red"
    _write_json(log_maintenance_file, log_maintenance_payload)

    third_payload, third_fingerprint_payload, third_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=2,
    )
    assert third_payload.get("issue_detected") is True
    assert third_fingerprint_payload.get("log_maintenance_health_status") == "red"
    assert third_fingerprint != first_fingerprint


def test_status_change_only_resends_when_issue_remaining_transitions_open_to_cleared(
    tmp_path: Path,
) -> None:
    (
        output_dir,
        script_path,
        tool_dir,
        env_file,
        alert_state_file,
        curl_invocation_log,
    ) = _prepare_status_change_test_context(
        tmp_path=tmp_path,
        hardening_status="ready",
        webhook_payload_capture_name="captured_webhook_payload_issue_remaining_transition.json",
        curl_invocation_log_name="curl_invocations_issue_remaining_transition.log",
        alert_state_file_name="recovery_alert_state_issue_remaining_transition.json",
        extra_env_lines=("RECOVERY_LOG_MAINTENANCE_TRIGGER_COOLDOWN_SECONDS=0",),
    )

    log_maintenance_file = output_dir / "health" / "log_maintenance" / "log_maintenance_latest.json"
    log_maintenance_payload = json.loads(log_maintenance_file.read_text(encoding="utf-8"))
    assert isinstance(log_maintenance_payload, dict)
    log_maintenance_payload["health_status"] = "red"
    _write_json(log_maintenance_file, log_maintenance_payload)

    first_payload, first_fingerprint_payload, first_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
        extra_env={"RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "0"},
    )

    assert first_payload.get("issue_detected") is True
    assert first_payload.get("issue_remaining") is True
    first_actions = first_payload.get("actions_attempted") or []
    assert isinstance(first_actions, list)
    assert "trigger_log_maintenance:disabled" in first_actions
    assert first_fingerprint_payload.get("issue_remaining") is True

    _, second_fingerprint_payload, second_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
        extra_env={"RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "0"},
    )
    assert second_fingerprint == first_fingerprint
    assert second_fingerprint_payload == first_fingerprint_payload

    third_payload, third_fingerprint_payload, third_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=2,
        extra_env={"RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "1"},
    )

    assert third_payload.get("issue_detected") is True
    assert third_payload.get("issue_remaining") is False
    third_actions = third_payload.get("actions_attempted") or []
    assert isinstance(third_actions, list)
    assert "trigger_log_maintenance:ok" in third_actions
    assert third_fingerprint_payload.get("issue_remaining") is False
    assert third_fingerprint != first_fingerprint

    _, fourth_fingerprint_payload, fourth_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=2,
        extra_env={"RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "1"},
    )
    assert fourth_fingerprint == third_fingerprint
    assert fourth_fingerprint_payload == third_fingerprint_payload


def test_status_change_only_resends_when_issue_remaining_transitions_cleared_to_open(
    tmp_path: Path,
) -> None:
    (
        output_dir,
        script_path,
        tool_dir,
        env_file,
        alert_state_file,
        curl_invocation_log,
    ) = _prepare_status_change_test_context(
        tmp_path=tmp_path,
        hardening_status="ready",
        webhook_payload_capture_name="captured_webhook_payload_issue_remaining_reopen.json",
        curl_invocation_log_name="curl_invocations_issue_remaining_reopen.log",
        alert_state_file_name="recovery_alert_state_issue_remaining_reopen.json",
        extra_env_lines=("RECOVERY_LOG_MAINTENANCE_TRIGGER_COOLDOWN_SECONDS=0",),
    )

    log_maintenance_file = output_dir / "health" / "log_maintenance" / "log_maintenance_latest.json"
    log_maintenance_payload = json.loads(log_maintenance_file.read_text(encoding="utf-8"))
    assert isinstance(log_maintenance_payload, dict)
    log_maintenance_payload["health_status"] = "red"
    _write_json(log_maintenance_file, log_maintenance_payload)

    first_payload, first_fingerprint_payload, first_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
        extra_env={"RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "1"},
    )

    assert first_payload.get("issue_detected") is True
    assert first_payload.get("issue_remaining") is False
    first_actions = first_payload.get("actions_attempted") or []
    assert isinstance(first_actions, list)
    assert "trigger_log_maintenance:ok" in first_actions
    assert first_fingerprint_payload.get("issue_remaining") is False

    _, second_fingerprint_payload, second_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
        extra_env={"RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "1"},
    )
    assert second_fingerprint == first_fingerprint
    assert second_fingerprint_payload == first_fingerprint_payload

    third_payload, third_fingerprint_payload, third_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=2,
        extra_env={"RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "0"},
    )

    assert third_payload.get("issue_detected") is True
    assert third_payload.get("issue_remaining") is True
    third_actions = third_payload.get("actions_attempted") or []
    assert isinstance(third_actions, list)
    assert "trigger_log_maintenance:disabled" in third_actions
    assert third_fingerprint_payload.get("issue_remaining") is True
    assert third_fingerprint != first_fingerprint

    _, fourth_fingerprint_payload, fourth_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=2,
        extra_env={"RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "0"},
    )
    assert fourth_fingerprint == third_fingerprint
    assert fourth_fingerprint_payload == third_fingerprint_payload


def test_status_change_only_matrix_resends_only_on_fingerprint_changes(
    tmp_path: Path,
) -> None:
    (
        output_dir,
        script_path,
        tool_dir,
        env_file,
        alert_state_file,
        curl_invocation_log,
    ) = _prepare_status_change_test_context(
        tmp_path=tmp_path,
        hardening_status="error",
        webhook_payload_capture_name="captured_webhook_payload_status_change_matrix.json",
        curl_invocation_log_name="curl_invocations_status_change_matrix.log",
        alert_state_file_name="recovery_alert_state_status_change_matrix.json",
        extra_env_lines=("RECOVERY_ENABLE_SERVICE_RESTARTS=0",),
    )

    # 1) Baseline fingerprint A sends.
    _, first_fingerprint_payload, fingerprint_a = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
    )
    assert first_fingerprint_payload.get("shadow_service") == "active"
    assert first_fingerprint_payload.get("log_maintenance_health_status") == "green"

    # 2) Repeat A suppresses.
    _, second_fingerprint_payload, second_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
    )
    assert second_fingerprint == fingerprint_a
    assert second_fingerprint_payload == first_fingerprint_payload

    # 3) Non-fingerprint telemetry churn still suppresses.
    live_status_file = output_dir / "health" / "live_status_latest.json"
    live_status_payload = json.loads(live_status_file.read_text(encoding="utf-8"))
    assert isinstance(live_status_payload, dict)
    freshness_plan = live_status_payload.get("freshness_plan")
    assert isinstance(freshness_plan, dict)
    freshness_plan["metar_observation_stale_rate"] = 0.05
    freshness_plan["approval_rate"] = 0.91
    scan_budget = live_status_payload.get("scan_budget")
    assert isinstance(scan_budget, dict)
    scan_budget["effective_max_markets"] = 150
    scan_budget["next_max_markets"] = 140
    scan_budget["adaptive_decision_action"] = "trim"
    scan_budget["adaptive_decision_reason"] = "volatility_guardrail"
    latest_cycle_metrics = live_status_payload.get("latest_cycle_metrics")
    assert isinstance(latest_cycle_metrics, dict)
    latest_cycle_metrics["intents_total"] = 50
    _write_json(live_status_file, live_status_payload)

    _, third_fingerprint_payload, third_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=1,
    )
    assert third_fingerprint == fingerprint_a
    assert third_fingerprint_payload == first_fingerprint_payload

    # 4) Fingerprint field B change sends.
    _, fingerprint_b_payload, fingerprint_b = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=2,
        extra_env={"MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT": "failed"},
    )
    assert fingerprint_b != fingerprint_a
    assert fingerprint_b_payload.get("shadow_service") == "failed"
    assert fingerprint_b_payload.get("log_maintenance_health_status") == "green"

    # 5) Repeat B suppresses.
    _, fifth_fingerprint_payload, fifth_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=2,
        extra_env={"MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT": "failed"},
    )
    assert fifth_fingerprint == fingerprint_b
    assert fifth_fingerprint_payload == fingerprint_b_payload

    # 6) Fingerprint field C change (different from B) sends.
    log_maintenance_file = output_dir / "health" / "log_maintenance" / "log_maintenance_latest.json"
    log_maintenance_payload = json.loads(log_maintenance_file.read_text(encoding="utf-8"))
    assert isinstance(log_maintenance_payload, dict)
    log_maintenance_payload["health_status"] = "red"
    _write_json(log_maintenance_file, log_maintenance_payload)

    _, fingerprint_c_payload, fingerprint_c = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=3,
        extra_env={
            "MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT": "failed",
            "RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "0",
        },
    )
    assert fingerprint_c != fingerprint_b
    assert fingerprint_c_payload.get("shadow_service") == "failed"
    assert fingerprint_c_payload.get("log_maintenance_health_status") == "red"

    # 7) Repeat C suppresses.
    _, seventh_fingerprint_payload, seventh_fingerprint = _run_status_change_step(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        output_dir=output_dir,
        alert_state_file=alert_state_file,
        curl_invocation_log=curl_invocation_log,
        expected_curl_invocations=3,
        extra_env={
            "MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT": "failed",
            "RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER": "0",
        },
    )
    assert seventh_fingerprint == fingerprint_c
    assert seventh_fingerprint_payload == fingerprint_c_payload


def test_concise_webhook_payload_prioritizes_stage_timeout_repair_missing_script(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_timeout_guardrail_hardening_status(hardening_status_file, status="ready")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_timeout_priority.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=webhook_payload_capture)

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
            "RECOVERY_ENABLE_COLDMATH_STAGE_TIMEOUT_GUARDRAIL_REPAIR=1",
        ),
    )

    result = _run_recovery_script(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={"MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT": "failed"},
    )

    assert result.returncode == 0
    lines = _read_webhook_payload_lines(webhook_payload_capture)
    auto_actions_line = next((line for line in lines if line.startswith("Auto actions:")), "")
    assert auto_actions_line
    assert "enabled reporting timer" in auto_actions_line
    assert auto_actions_line.startswith(
        "Auto actions: coldmath stage-timeout guardrail repair script missing"
    )


def test_recovery_effectiveness_strict_gap_missing_triggers_hardening_action(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="ready")

    script_path, tool_dir, _, systemctl_invocations = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=False,
        extra_lines=(
            "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY=1",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert result.returncode == 0

    payload = _load_recovery_latest(output_dir)
    assert payload.get("issue_detected") is True
    assert payload.get("issue_remaining") is True
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)
    assert "recovery_effectiveness_summary_missing" in decision_reasons

    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "trigger_coldmath_hardening_on_effectiveness_gap:ok" in actions

    effectiveness = payload.get("recovery_effectiveness")
    assert isinstance(effectiveness, dict)
    assert effectiveness.get("summary_available") is False
    assert effectiveness.get("file_age_seconds") == -1
    assert effectiveness.get("stale") is False
    assert effectiveness.get("gap_detected") is True
    assert effectiveness.get("gap_reason") == "summary_missing"
    assert effectiveness.get("strict_required") is True

    systemctl_lines = _read_lines(systemctl_invocations)
    assert _contains_systemctl_start(
        systemctl_lines,
        "betbot-temperature-coldmath-hardening.service",
    )


def test_recovery_effectiveness_strict_gap_stale_records_disabled_trigger_action(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    advisor_latest = output_dir / "health" / "kalshi_temperature_recovery_advisor_latest.json"
    _write_json(
        advisor_latest,
        {
            "metrics": {
                "recovery_effectiveness": {
                    "summary_available": True,
                    "persistently_harmful_actions": ["reduce_negative_expectancy_regimes"],
                }
            },
            "remediation_plan": {
                "demoted_actions_for_effectiveness": ["reduce_negative_expectancy_regimes"],
            },
        },
    )
    stale_epoch = time.time() - 7200
    os.utime(advisor_latest, (stale_epoch, stale_epoch))

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="ready")

    script_path, tool_dir, _, systemctl_invocations = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=False,
        extra_lines=(
            "RECOVERY_REQUIRE_EFFECTIVENESS_SUMMARY=1",
            "RECOVERY_EFFECTIVENESS_STALE_CRIT_SECONDS=60",
            "RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_EFFECTIVENESS_GAP=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert result.returncode == 0

    payload = _load_recovery_latest(output_dir)
    assert payload.get("issue_detected") is True
    assert payload.get("issue_remaining") is True
    decision_reasons = payload.get("decision_reasons") or []
    assert isinstance(decision_reasons, list)
    assert "recovery_effectiveness_summary_stale" in decision_reasons

    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "trigger_coldmath_hardening_on_effectiveness_gap:disabled" in actions

    effectiveness = payload.get("recovery_effectiveness")
    assert isinstance(effectiveness, dict)
    assert effectiveness.get("summary_available") is True
    file_age_seconds = effectiveness.get("file_age_seconds")
    assert isinstance(file_age_seconds, int)
    assert file_age_seconds >= 60
    assert effectiveness.get("stale_threshold_seconds") == 60
    assert effectiveness.get("stale") is True
    assert effectiveness.get("gap_detected") is True
    assert effectiveness.get("gap_reason") == "summary_stale"
    assert effectiveness.get("strict_required") is True

    systemctl_lines = _read_lines(systemctl_invocations)
    assert not _contains_systemctl_start(
        systemctl_lines,
        "betbot-temperature-coldmath-hardening.service",
    )


def test_recovery_effectiveness_summary_populates_payload_and_concise_webhook(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    _write_json(
        output_dir / "health" / "kalshi_temperature_recovery_advisor_latest.json",
        {
            "metrics": {
                "recovery_effectiveness": {
                    "summary_available": True,
                    "persistently_harmful_actions": ["reduce_negative_expectancy_regimes"],
                }
            },
            "remediation_plan": {
                "demoted_actions_for_effectiveness": ["reduce_negative_expectancy_regimes"],
            },
        },
    )

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_effectiveness.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=webhook_payload_capture)

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)
    assert result.returncode == 0

    payload = _load_recovery_latest(output_dir)
    effectiveness = payload.get("recovery_effectiveness")
    assert isinstance(effectiveness, dict)
    assert effectiveness.get("summary_available") is True
    assert effectiveness.get("summary_source") == "advisor_latest"
    assert effectiveness.get("persistently_harmful_actions") == ["reduce_negative_expectancy_regimes"]
    assert effectiveness.get("demoted_actions_for_effectiveness") == ["reduce_negative_expectancy_regimes"]
    assert effectiveness.get("harmful_action_count") == 1
    assert effectiveness.get("demoted_action_count") == 1
    summary_text = effectiveness.get("summary")
    assert isinstance(summary_text, str)
    assert "effectiveness harmful routes 1" in summary_text
    assert "demoted routes 1" in summary_text

    lines = _read_webhook_payload_lines(webhook_payload_capture)
    _project_webhook_line_prefixes(lines)
    why_line = next((line for line in lines if line.startswith("Why it matters:")), "")
    assert why_line
    assert "effectiveness harmful routes 1" in why_line
    assert "demoted routes 1" in why_line
    assert "top route reduce negative" in why_line


def test_concise_webhook_payload_avoids_raw_reason_and_action_tokens(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=webhook_payload_capture)

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    assert webhook_payload_capture.exists(), "expected webhook payload capture file"

    captured_payload = json.loads(webhook_payload_capture.read_text(encoding="utf-8"))
    assert isinstance(captured_payload, dict)
    webhook_content = captured_payload.get("content")
    assert isinstance(webhook_content, str)
    assert webhook_content.strip()

    recovery_payload = _load_recovery_latest(output_dir)
    decision_reasons = recovery_payload.get("decision_reasons") or []
    actions_attempted = recovery_payload.get("actions_attempted") or []
    assert isinstance(decision_reasons, list)
    assert isinstance(actions_attempted, list)

    raw_tokens = [str(token) for token in [*decision_reasons, *actions_attempted] if str(token).strip()]
    assert raw_tokens, "expected at least one raw recovery token for verification"

    for token in raw_tokens:
        assert token not in webhook_content, f"raw internal token leaked into webhook content: {token}"

    assert "coldmath recovery env persistence has error" in webhook_content
    assert "repaired coldmath recovery env persistence strict gate" in webhook_content


def test_concise_webhook_payload_has_no_machine_style_tokens(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    live_status_file = output_dir / "health" / "live_status_latest.json"
    live_status_payload = json.loads(live_status_file.read_text(encoding="utf-8"))
    assert isinstance(live_status_payload, dict)
    command_execution = live_status_payload.get("command_execution")
    assert isinstance(command_execution, dict)
    command_execution["settlement_attempts"] = 2
    _write_json(live_status_file, live_status_payload)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="execution_failed")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_machine_tokens.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=webhook_payload_capture)

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    lines = _read_webhook_payload_lines(webhook_payload_capture)

    recovery_payload = _load_recovery_latest(output_dir)
    assert recovery_payload.get("issue_detected") is True
    assert recovery_payload.get("issue_remaining") is True
    actions_attempted = recovery_payload.get("actions_attempted") or []
    assert isinstance(actions_attempted, list)
    assert actions_attempted, "expected at least one recovery action in issue_remaining scenario"

    projected_prefixes = _project_webhook_line_prefixes(lines)
    assert projected_prefixes == [
        "HEADER",
        "State:",
        "What happened:",
        "Auto actions:",
        "Pressure:",
        "Why it matters:",
        "Next step:",
    ]
    assert not any(
        line.startswith(("Status:", "Recovery:", "Services:", "Freshness and scan:", "Log maintenance:", "Event file:"))
        for line in lines
    )

    semantic_prefixes = ("State:", "What happened:", "Auto actions:", "Pressure:", "Why it matters:")
    concise_line_bodies: list[str] = []
    for prefix in semantic_prefixes:
        matching_lines = [line for line in lines if line.startswith(prefix)]
        assert len(matching_lines) == 1, lines
        line = matching_lines[0]
        body = line.split(":", 1)[1].strip() if ":" in line else line
        body = re.sub(r"https?://\S+", "", body).strip()
        if body:
            concise_line_bodies.append(body)

    assert concise_line_bodies
    concise_text = "\n".join(concise_line_bodies)
    assert re.search(r"\b[a-z0-9]+_[a-z0-9_]+\b", concise_text) is None, concise_text
    assert re.search(r"\b[a-z0-9_]+:[a-z0-9_]+\b", concise_text) is None, concise_text


def test_non_concise_webhook_payload_has_no_machine_style_tokens(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_non_concise_machine_tokens.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=webhook_payload_capture)

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=verbose",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    lines = _read_webhook_payload_lines(webhook_payload_capture)

    recovery_payload = _load_recovery_latest(output_dir)
    assert recovery_payload.get("issue_detected") is True
    actions_attempted = recovery_payload.get("actions_attempted") or []
    assert isinstance(actions_attempted, list)
    assert actions_attempted, "expected at least one recovery action in non-concise issue scenario"

    projected_prefixes = _project_webhook_line_prefixes(lines)
    assert projected_prefixes == [
        "HEADER",
        "Status:",
        "What happened:",
        "Auto actions:",
        "Recovery:",
        "Why it matters:",
        "Services:",
        "Freshness and scan:",
        "Log maintenance:",
        "Event file:",
        "Next step:",
    ]
    assert not any(line.startswith(("State:", "Pressure:", "Event:", "Issue:")) for line in lines)

    semantic_prefixes = (
        "Status:",
        "What happened:",
        "Auto actions:",
        "Recovery:",
        "Why it matters:",
        "Services:",
        "Freshness and scan:",
        "Log maintenance:",
    )
    token_guard_lines: list[str] = []
    for prefix in semantic_prefixes:
        matching_lines = [line for line in lines if line.startswith(prefix)]
        assert len(matching_lines) == 1, lines
        token_guard_lines.append(matching_lines[0])

    cleaned_token_guard_lines = [re.sub(r"https?://\S+", "", line).strip() for line in token_guard_lines]
    token_guard_text = "\n".join(line for line in cleaned_token_guard_lines if line)
    assert token_guard_text
    assert re.search(r"\b[a-z0-9]+_[a-z0-9_]+\b", token_guard_text) is None, token_guard_text
    assert re.search(r"\b[a-z0-9_]+:[a-z0-9_]+\b", token_guard_text) is None, token_guard_text


def test_concise_webhook_payload_line_order_and_length_caps(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="ready")

    script_path, tool_dir, _, _ = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=False,
    )
    webhook_payload_capture = tmp_path / "captured_webhook_payload_ordered.json"
    _write_curl_capture_shim(tool_dir / "curl", payload_capture=webhook_payload_capture)

    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            'RECOVERY_WEBHOOK_URL="https://discord.example/webhook"',
            "RECOVERY_WEBHOOK_MESSAGE_MODE=concise",
            "RECOVERY_NOTIFY_STATUS_CHANGE_ONLY=0",
        ),
    )

    result = _run_recovery_script(
        script_path=script_path,
        env_file=env_file,
        tool_dir=tool_dir,
        extra_env={"MOCK_SYSTEMCTL_IS_ACTIVE_DEFAULT": "activating"},
    )

    assert result.returncode == 0
    assert webhook_payload_capture.exists(), "expected webhook payload capture file"

    captured_payload = json.loads(webhook_payload_capture.read_text(encoding="utf-8"))
    assert isinstance(captured_payload, dict)
    webhook_content = captured_payload.get("content")
    assert isinstance(webhook_content, str)
    assert webhook_content.strip()

    recovery_payload = _load_recovery_latest(output_dir)
    assert recovery_payload.get("issue_remaining") is True

    lines = webhook_content.splitlines()
    assert lines
    assert all(line.strip() for line in lines), f"unexpected blank line in concise webhook payload: {lines}"
    assert len(lines) <= 7
    assert len(lines) == 7
    assert all(len(line) <= 170 for line in lines), lines
    assert not any(line.startswith("Pressure:") for line in lines), lines

    ordered_prefixes = [
        "State:",
        "What happened:",
        "Auto actions:",
        "Why it matters:",
        "Next step:",
        "Event:",
    ]
    assert not any(lines[0].startswith(prefix) for prefix in ordered_prefixes)

    prefix_positions: dict[str, int] = {}
    for idx, line in enumerate(lines):
        for prefix in ordered_prefixes:
            if line.startswith(prefix):
                assert prefix not in prefix_positions, f"duplicate prefix {prefix!r} in payload: {lines}"
                prefix_positions[prefix] = idx
                break

    for required_prefix in ordered_prefixes:
        assert required_prefix in prefix_positions, f"missing prefix {required_prefix!r} in payload: {lines}"

    positions = [prefix_positions[prefix] for prefix in ordered_prefixes]
    assert positions == sorted(positions), f"concise payload prefix order regressed: {lines}"


def test_env_repair_hardening_trigger_records_disabled_and_skips_invocation(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    script_path, tool_dir, remediation_invocations, systemctl_invocations = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "RECOVERY_ENABLE_COLDMATH_HARDENING_TRIGGER_ON_ENV_REPAIR=0",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "repair_recovery_env_persistence_gate:ok" in actions
    assert "trigger_coldmath_hardening_after_env_repair:disabled" in actions
    assert _read_lines(remediation_invocations), "expected remediation script invocation"
    systemctl_lines = _read_lines(systemctl_invocations)
    assert not _contains_systemctl_start(
        systemctl_lines,
        "betbot-temperature-coldmath-hardening.service",
    )


def test_env_repair_hardening_trigger_respects_cooldown(
    tmp_path: Path,
) -> None:
    root = Path(__file__).resolve().parents[1]
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    _seed_healthy_artifacts(output_dir)

    hardening_status_file = output_dir / "health" / "coldmath_hardening_latest.json"
    _write_hardening_status(hardening_status_file, status="error")

    _write_json(
        output_dir / "health" / "recovery" / ".recovery_state.json",
        {
            "last_coldmath_hardening_trigger_epoch": int(time.time()),
        },
    )

    script_path, tool_dir, remediation_invocations, systemctl_invocations = _prepare_script_bundle(
        tmp_path=tmp_path,
        root=root,
        include_remediation_script=True,
    )
    env_file = tmp_path / "recovery.env"
    fake_betbot_root = _prepare_fake_betbot_root(tmp_path)
    _write_env_file(
        env_file=env_file,
        betbot_root=fake_betbot_root,
        output_dir=output_dir,
        hardening_status_file=hardening_status_file,
        strict_gate_enabled=True,
        extra_lines=(
            "RECOVERY_COLDMATH_HARDENING_TRIGGER_COOLDOWN_SECONDS=3600",
        ),
    )

    result = _run_recovery_script(script_path=script_path, env_file=env_file, tool_dir=tool_dir)

    assert result.returncode == 0
    payload = _load_recovery_latest(output_dir)
    actions = payload.get("actions_attempted") or []
    assert isinstance(actions, list)
    assert "repair_recovery_env_persistence_gate:ok" in actions
    assert "trigger_coldmath_hardening_after_env_repair:cooldown" in actions
    assert _read_lines(remediation_invocations), "expected remediation script invocation"
    systemctl_lines = _read_lines(systemctl_invocations)
    assert not _contains_systemctl_start(
        systemctl_lines,
        "betbot-temperature-coldmath-hardening.service",
    )
